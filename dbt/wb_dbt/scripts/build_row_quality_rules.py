import csv
import json
import re
from collections import defaultdict
from pathlib import Path

import psycopg2


PROJECT = Path("/opt/wb_pipeline/dbt/wb_dbt")
ENV_PATH = Path("/opt/wb_pipeline/.env")

OUT_CSV = PROJECT / "metadata" / "row_quality_rules.csv"
OUT_JSON = PROJECT / "metadata" / "row_quality_rules.json"
OUT_SUMMARY_CSV = PROJECT / "metadata" / "row_quality_rules_summary.csv"


ORDERS_CURRENT_DATASETS = [
    "orders_fbs_new",
    "orders_fbs_current",
    "orders_fbs_archive",
    "orders_dbs_new",
    "orders_dbs_completed",
    "orders_dbw_new",
    "orders_dbw_completed",
    "orders_pickup_new",
]


def load_env(path):
    env = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def model_to_dataset(model_name):
    if model_name.startswith("stg_"):
        return model_name[4:]
    return model_name


def clean_table_name(model_name):
    name = model_to_dataset(model_name)
    if model_name == "stg_orders_current":
        return "orders"
    return name


def is_text_type(data_type):
    return data_type in {
        "text",
        "character varying",
        "character",
        "varchar",
        "char",
        "json",
        "jsonb",
    }


def load_staging_columns(conn):
    rows = []

    with conn.cursor() as cur:
        cur.execute("""
            select
                table_name,
                column_name,
                data_type,
                ordinal_position
            from information_schema.columns
            where table_schema = 'staging'
              and table_name like 'stg_%'
            order by table_name, ordinal_position;
        """)
        rows = cur.fetchall()

    tables = defaultdict(dict)

    for table_name, column_name, data_type, ordinal_position in rows:
        tables[table_name][column_name] = {
            "data_type": data_type,
            "ordinal_position": ordinal_position,
        }

    return dict(tables)


def add_rule(rules, *, model_name, cleaned_table, rule_group, rule_type, severity,
             column_name, sql_condition, issue_code, issue_message,
             enabled=True, source="generated", notes=""):
    rules.append({
        "rule_id": f"{model_name}__{issue_code}",
        "model_name": model_name,
        "cleaned_table": cleaned_table,
        "rule_group": rule_group,
        "rule_type": rule_type,
        "severity": severity,
        "column_name": column_name or "",
        "sql_condition": sql_condition or "",
        "issue_code": issue_code,
        "issue_message": issue_message,
        "enabled": "true" if enabled else "false",
        "source": source,
        "notes": notes,
    })


def add_required_rule(rules, model_name, cleaned_table, columns, col, *,
                      severity="bad", group="domain_required", code=None, message=None):
    if col not in columns:
        return

    data_type = columns[col]["data_type"]

    if is_text_type(data_type):
        condition = f"{col} is null or nullif({col}::text, '') is null"
    else:
        condition = f"{col} is null"

    issue_code = code or f"{col}_missing"
    issue_message = message or f"Required field `{col}` is missing."

    add_rule(
        rules,
        model_name=model_name,
        cleaned_table=cleaned_table,
        rule_group=group,
        rule_type="required",
        severity=severity,
        column_name=col,
        sql_condition=condition,
        issue_code=issue_code,
        issue_message=issue_message,
    )


def add_nonnegative_rule(rules, model_name, cleaned_table, columns, col, *,
                         severity="bad", group="numeric_quality"):
    if col not in columns:
        return

    data_type = columns[col]["data_type"]

    if data_type not in {
        "bigint",
        "integer",
        "numeric",
        "double precision",
        "real",
        "smallint",
    }:
        return

    add_rule(
        rules,
        model_name=model_name,
        cleaned_table=cleaned_table,
        rule_group=group,
        rule_type="non_negative",
        severity=severity,
        column_name=col,
        sql_condition=f"{col} < 0",
        issue_code=f"{col}_negative",
        issue_message=f"Numeric field `{col}` is negative.",
    )


def add_any_required_rule(rules, model_name, cleaned_table, columns, candidates, *,
                          severity="bad", group="domain_required", code, message):
    existing = [c for c in candidates if c in columns]

    if not existing:
        return

    parts = []

    for col in existing:
        data_type = columns[col]["data_type"]
        if is_text_type(data_type):
            parts.append(f"({col} is null or nullif({col}::text, '') is null)")
        else:
            parts.append(f"({col} is null)")

    condition = " and ".join(parts)

    add_rule(
        rules,
        model_name=model_name,
        cleaned_table=cleaned_table,
        rule_group=group,
        rule_type="at_least_one_required",
        severity=severity,
        column_name=",".join(existing),
        sql_condition=condition,
        issue_code=code,
        issue_message=message,
    )


def add_universal_rules(rules, model_name, cleaned_table, columns):
    for col in ["raw_payload_id", "record_index", "dataset_name"]:
        add_required_rule(
            rules,
            model_name,
            cleaned_table,
            columns,
            col,
            severity="bad",
            group="technical_lineage",
            code=f"technical_{col}_missing",
            message=f"Technical lineage field `{col}` is missing.",
        )

    # raw_record есть почти везде. Для promotion_fullstats есть raw_record = raw_nm.
    if "raw_record" in columns:
        add_required_rule(
            rules,
            model_name,
            cleaned_table,
            columns,
            "raw_record",
            severity="bad",
            group="technical_lineage",
            code="technical_raw_record_missing",
            message="Technical lineage field `raw_record` is missing.",
        )

    if "raw_payload_id" in columns and "record_index" in columns:
        add_rule(
            rules,
            model_name=model_name,
            cleaned_table=cleaned_table,
            rule_group="technical_lineage",
            rule_type="unique_combination",
            severity="bad",
            column_name="raw_payload_id,record_index",
            sql_condition="count(*) over (partition by raw_payload_id, record_index) > 1",
            issue_code="duplicate_raw_payload_record_index",
            issue_message="Duplicate row identity by raw_payload_id + record_index.",
        )

    if "dataset_name" in columns:
        if model_name == "stg_orders_current":
            allowed = ", ".join("'" + x + "'" for x in ORDERS_CURRENT_DATASETS)
            condition = f"dataset_name not in ({allowed})"
            message = "dataset_name is not one of the datasets covered by stg_orders_current."
        else:
            expected = model_to_dataset(model_name)
            condition = f"dataset_name <> '{expected}'"
            message = f"dataset_name is not `{expected}`."

        add_rule(
            rules,
            model_name=model_name,
            cleaned_table=cleaned_table,
            rule_group="technical_lineage",
            rule_type="accepted_dataset",
            severity="bad",
            column_name="dataset_name",
            sql_condition=condition,
            issue_code="unexpected_dataset_name",
            issue_message=message,
        )


def add_metric_nonnegative_rules(rules, model_name, cleaned_table, columns):
    metric_patterns = [
        r"(^|_)views?$",
        r"(^|_)clicks?$",
        r"(^|_)orders?$",
        r"(^|_)canceled$",
        r"(^|_)atbs?$",
        r"(^|_)shks?$",
        r"(^|_)quantity$",
        r"(^|_)qty$",
        r"(^|_)stock$",
        r"(^|_)stocks?$",
        r"(^|_)balance$",
        r"(^|_)count$",
    ]

    money_patterns = [
        r"(^|_)sum$",
        r"(^|_)sum_price$",
        r"(^|_)price$",
        r"(^|_)amount$",
        r"(^|_)cost$",
        r"(^|_)rate$",
        r"(^|_)commission$",
    ]

    for col in columns:
        is_metric = any(re.search(p, col) for p in metric_patterns)
        is_money = any(re.search(p, col) for p in money_patterns)

        if not is_metric and not is_money:
            continue

        # Dynamic / delta / change fields are changes versus another period.
        # They may legitimately be negative, so non-negative checks are invalid.
        if any(token in col for token in ["dynamic", "delta", "change", "diff", "growth"]):
            continue

        # В finance/reports отрицательные суммы могут быть возвратами/корректировками,
        # поэтому пока warning, а не bad.
        if model_name.startswith("stg_finance_") or model_name.startswith("stg_report_"):
            severity = "warning"
        else:
            severity = "bad"

        add_nonnegative_rule(
            rules,
            model_name,
            cleaned_table,
            columns,
            col,
            severity=severity,
            group="numeric_quality",
        )


def add_domain_rules(rules, model_name, cleaned_table, columns):
    # orders_current участвует в общем row-quality framework.
    # Правила для него curated, а не просто generic.
    if model_name == "stg_orders_current":
        add_any_required_rule(
            rules, model_name, cleaned_table, columns,
            ["order_id", "rid", "srid", "order_uid", "order_code"],
            severity="bad",
            code="order_identity_missing",
            message="Order row has no stable order identity field.",
        )

        add_any_required_rule(
            rules, model_name, cleaned_table, columns,
            ["created_at", "ddate", "seller_date"],
            severity="warning",
            code="order_date_missing",
            message="Order row has no usable order/date field.",
        )

        add_any_required_rule(
            rules, model_name, cleaned_table, columns,
            ["nm_id", "chrt_id", "article", "barcode"],
            severity="warning",
            code="order_product_identity_missing",
            message="Order row has no product identity field.",
        )

        add_any_required_rule(
            rules, model_name, cleaned_table, columns,
            ["currency_code", "converted_currency_code"],
            severity="warning",
            code="order_currency_missing",
            message="Order row has price-like fields but no currency code.",
        )

        add_any_required_rule(
            rules, model_name, cleaned_table, columns,
            ["warehouse_id", "warehouse_address", "office_id"],
            severity="warning",
            code="order_fulfillment_context_missing",
            message="Order row has no warehouse/office fulfillment context.",
        )

        return

    # order statuses
    if model_name.startswith("stg_orders_") and model_name.endswith("_statuses"):
        add_any_required_rule(
            rules, model_name, cleaned_table, columns,
            ["order_id", "rid", "srid"],
            severity="bad",
            code="order_status_identity_missing",
            message="Order status row has no order identity field.",
        )
        for c in ["wb_status", "supplier_status", "status"]:
            add_required_rule(rules, model_name, cleaned_table, columns, c, severity="warning")

    # items
    if model_name == "stg_items_cards":
        add_required_rule(rules, model_name, cleaned_table, columns, "nm_id", severity="bad")
        add_any_required_rule(
            rules, model_name, cleaned_table, columns,
            ["article", "vendor_code", "supplier_article"],
            severity="warning",
            code="product_article_missing",
            message="Product row has no seller article/vendor code.",
        )
        add_any_required_rule(
            rules, model_name, cleaned_table, columns,
            ["barcode", "skus"],
            severity="warning",
            code="product_barcode_or_skus_missing",
            message="Product row has no barcode/skus field.",
        )

    if model_name == "stg_items_stocks":
        add_required_rule(rules, model_name, cleaned_table, columns, "nm_id", severity="bad")
        add_required_rule(rules, model_name, cleaned_table, columns, "warehouse_id", severity="bad")
        add_any_required_rule(
            rules, model_name, cleaned_table, columns,
            ["quantity", "qty", "amount", "stock", "stocks"],
            severity="bad",
            code="stock_quantity_missing",
            message="Stock row has no quantity-like field.",
        )

    if model_name == "stg_items_warehouses":
        add_any_required_rule(
            rules, model_name, cleaned_table, columns,
            ["warehouse_id", "id"],
            severity="bad",
            code="warehouse_identity_missing",
            message="Warehouse row has no warehouse identity field.",
        )
        add_any_required_rule(
            rules, model_name, cleaned_table, columns,
            ["name", "warehouse_name", "warehouse"],
            severity="warning",
            code="warehouse_name_missing",
            message="Warehouse row has no warehouse name field.",
        )

    # finance / reports
    if model_name in {
        "stg_finance_sales_reports",
        "stg_finance_sales_reports_detailed",
        "stg_report_orders",
        "stg_report_sales",
    }:
        add_required_rule(rules, model_name, cleaned_table, columns, "operation_date", severity="bad")
        add_any_required_rule(
            rules, model_name, cleaned_table, columns,
            ["nm_id", "order_id", "rid", "srid", "rrd_id"],
            severity="warning",
            code="operation_business_identity_missing",
            message="Operation/report row has no product/order/report identity field.",
        )

    if model_name == "stg_finance_balance":
        add_any_required_rule(
            rules, model_name, cleaned_table, columns,
            ["balance", "amount", "sum"],
            severity="warning",
            code="finance_balance_amount_missing",
            message="Finance balance row has no amount/balance field.",
        )

    # tariffs
    if model_name.startswith("stg_tariffs_"):
        add_any_required_rule(
            rules, model_name, cleaned_table, columns,
            ["warehouse_name", "warehouse_id", "box_type_name", "subject_name", "category"],
            severity="warning",
            code="tariff_context_missing",
            message="Tariff row has no warehouse/category/context field.",
        )
        add_any_required_rule(
            rules, model_name, cleaned_table, columns,
            ["date", "dt", "period"],
            severity="warning",
            code="tariff_date_missing",
            message="Tariff row has no date/period field.",
        )

    # analytics
    if model_name.startswith("stg_analytics_"):
        add_any_required_rule(
            rules, model_name, cleaned_table, columns,
            ["nm_id", "product_nm_id"],
            severity="bad",
            code="analytics_nm_id_missing",
            message="Analytics row has no nm_id/product_nm_id.",
        )

    # promotion
    if model_name == "stg_promotion_campaigns":
        add_any_required_rule(
            rules, model_name, cleaned_table, columns,
            ["campaign_id", "advert_id"],
            severity="bad",
            code="promotion_campaign_identity_missing",
            message="Promotion campaign row has no campaign/advert identity.",
        )

    if model_name == "stg_promotion_fullstats":
        add_required_rule(rules, model_name, cleaned_table, columns, "root_campaign_id", severity="bad")
        add_required_rule(rules, model_name, cleaned_table, columns, "root_advert_id", severity="bad")
        add_required_rule(rules, model_name, cleaned_table, columns, "nm_nm_id", severity="bad")

    # communications
    if model_name == "stg_communications_feedbacks":
        add_any_required_rule(
            rules, model_name, cleaned_table, columns,
            ["feedback_id", "id"],
            severity="bad",
            code="feedback_identity_missing",
            message="Feedback row has no feedback identity.",
        )
        add_any_required_rule(
            rules, model_name, cleaned_table, columns,
            ["created_at", "created_date", "date"],
            severity="warning",
            code="feedback_date_missing",
            message="Feedback row has no created/date field.",
        )

    if model_name == "stg_communications_chats":
        add_any_required_rule(
            rules, model_name, cleaned_table, columns,
            ["chat_id", "id"],
            severity="bad",
            code="chat_identity_missing",
            message="Chat row has no chat identity.",
        )

    # fbw
    if model_name == "stg_fbw_supplies":
        add_any_required_rule(
            rules, model_name, cleaned_table, columns,
            ["supply_id", "id"],
            severity="bad",
            code="fbw_supply_identity_missing",
            message="FBW supply row has no supply identity.",
        )

    # general seller info
    if model_name == "stg_general_seller_info":
        add_any_required_rule(
            rules, model_name, cleaned_table, columns,
            ["seller_id", "supplier_id", "id", "inn", "name"],
            severity="warning",
            code="seller_identity_missing",
            message="Seller info row has no seller identity-like field.",
        )


def main():
    env = load_env(ENV_PATH)

    conn = psycopg2.connect(
        host=env.get("DB_HOST", "localhost"),
        port=env.get("DB_PORT", "5432"),
        dbname=env.get("DB_NAME", "wb_pipeline"),
        user=env.get("DB_USER", "wb_user"),
        password=env["DB_PASSWORD"],
    )

    tables = load_staging_columns(conn)
    conn.close()

    rules = []

    for model_name in sorted(tables):
        columns = tables[model_name]
        cleaned_table = clean_table_name(model_name)

        add_universal_rules(rules, model_name, cleaned_table, columns)
        add_domain_rules(rules, model_name, cleaned_table, columns)
        add_metric_nonnegative_rules(rules, model_name, cleaned_table, columns)

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "rule_id",
        "model_name",
        "cleaned_table",
        "rule_group",
        "rule_type",
        "severity",
        "column_name",
        "sql_condition",
        "issue_code",
        "issue_message",
        "enabled",
        "source",
        "notes",
    ]

    with OUT_CSV.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rules)

    OUT_JSON.write_text(
        json.dumps(rules, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    summary = defaultdict(lambda: defaultdict(int))

    for r in rules:
        summary[r["model_name"]]["total"] += 1
        summary[r["model_name"]][r["severity"]] += 1

    with OUT_SUMMARY_CSV.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["model_name", "total_rules", "bad", "warning", "info"],
        )
        writer.writeheader()

        for model_name in sorted(summary):
            writer.writerow({
                "model_name": model_name,
                "total_rules": summary[model_name]["total"],
                "bad": summary[model_name]["bad"],
                "warning": summary[model_name]["warning"],
                "info": summary[model_name]["info"],
            })

    print("OK: row quality rules generated")
    print("staging models:", len(tables))
    print("rules:", len(rules))
    print()
    print("files:")
    print(OUT_CSV)
    print(OUT_JSON)
    print(OUT_SUMMARY_CSV)
    print()

    print("========== rules by model ==========")
    for model_name in sorted(summary):
        s = summary[model_name]
        print(
            f"{model_name:40s} "
            f"total={s['total']:3d} "
            f"bad={s['bad']:3d} "
            f"warning={s['warning']:3d} "
            f"info={s['info']:3d}"
        )


if __name__ == "__main__":
    main()
