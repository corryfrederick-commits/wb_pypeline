import csv
from pathlib import Path

import psycopg2
import yaml


PROJECT = Path("/opt/wb_pipeline/dbt/wb_dbt")
CORE_DIR = PROJECT / "models" / "core"
ENV_PATH = Path("/opt/wb_pipeline/.env")
REPORT_PATH = PROJECT / "metadata" / "core_key_relationship_test_plan.csv"


FOLDER_YML = {
    "reference": "core_reference.yml",
    "operations": "core_operations.yml",
    "reports_finance": "core_reports_finance.yml",
    "tariffs": "core_tariffs.yml",
    "analytics_promotion": "core_analytics_promotion.yml",
    "communications_supplies": "core_communications_supplies.yml",
}


PRIMARY_KEYS = {
    "sellers": ["seller_key"],
    "products": ["product_id"],
    "product_variants": ["product_variant_id"],
    "product_barcodes": ["product_barcode_key"],
    "warehouses": ["warehouse_key"],

    "stock_balances": ["stock_balance_key"],
    "orders": ["order_key"],
    "order_items": ["order_item_key"],
    "order_status_snapshots": ["order_status_snapshot_key"],

    "report_order_events": ["report_order_event_key"],
    "report_sale_events": ["report_sale_event_key"],
    "finance_balances": ["finance_balance_key"],
    "finance_report_summaries": ["finance_report_summary_key"],
    "finance_transactions": ["finance_transaction_key"],

    "tariff_commissions": ["tariff_commission_key"],
    "tariff_box_prices": ["tariff_box_price_key"],
    "tariff_acceptance_prices": ["tariff_acceptance_price_key"],

    "sales_funnel_metrics": ["sales_funnel_metric_key"],
    "stock_analytics_metrics": ["stock_analytics_metric_key"],
    "promotion_campaigns": ["promotion_campaign_key"],
    "promotion_product_daily_stats": ["promotion_product_daily_stat_key"],

    "feedbacks": ["feedback_key"],
    "chats": ["chat_key"],
    "fbw_supplies": ["fbw_supply_key"],
}


RELATIONSHIP_CANDIDATES = [
    ("product_variants", "product_id", "products", "product_id"),
    ("product_barcodes", "product_id", "products", "product_id"),
    ("product_barcodes", "product_variant_id", "product_variants", "product_variant_id"),

    ("stock_balances", "product_id", "products", "product_id"),
    ("stock_balances", "product_variant_id", "product_variants", "product_variant_id"),
    ("stock_balances", "warehouse_key", "warehouses", "warehouse_key"),

    ("order_items", "order_key", "orders", "order_key"),
    ("order_items", "product_id", "products", "product_id"),
    ("order_items", "product_variant_id", "product_variants", "product_variant_id"),
    ("order_status_snapshots", "order_key", "orders", "order_key"),

    ("report_order_events", "order_key", "orders", "order_key"),
    ("report_order_events", "product_id", "products", "product_id"),
    ("report_order_events", "product_variant_id", "product_variants", "product_variant_id"),
    ("report_order_events", "warehouse_key", "warehouses", "warehouse_key"),

    ("report_sale_events", "order_key", "orders", "order_key"),
    ("report_sale_events", "product_id", "products", "product_id"),
    ("report_sale_events", "product_variant_id", "product_variants", "product_variant_id"),
    ("report_sale_events", "warehouse_key", "warehouses", "warehouse_key"),

    ("finance_report_summaries", "order_key", "orders", "order_key"),
    ("finance_report_summaries", "product_id", "products", "product_id"),
    ("finance_report_summaries", "product_variant_id", "product_variants", "product_variant_id"),
    ("finance_report_summaries", "warehouse_key", "warehouses", "warehouse_key"),

    ("finance_transactions", "order_key", "orders", "order_key"),
    ("finance_transactions", "product_id", "products", "product_id"),
    ("finance_transactions", "product_variant_id", "product_variants", "product_variant_id"),
    ("finance_transactions", "warehouse_key", "warehouses", "warehouse_key"),

    ("tariff_acceptance_prices", "warehouse_key", "warehouses", "warehouse_key"),

    ("sales_funnel_metrics", "product_id", "products", "product_id"),

    ("stock_analytics_metrics", "product_id", "products", "product_id"),
    ("stock_analytics_metrics", "product_variant_id", "product_variants", "product_variant_id"),
    ("stock_analytics_metrics", "warehouse_key", "warehouses", "warehouse_key"),

    ("promotion_campaigns", "product_id", "products", "product_id"),
    ("promotion_campaigns", "product_variant_id", "product_variants", "product_variant_id"),

    ("promotion_product_daily_stats", "promotion_campaign_key", "promotion_campaigns", "promotion_campaign_key"),
    ("promotion_product_daily_stats", "product_id", "products", "product_id"),
    ("promotion_product_daily_stats", "product_variant_id", "product_variants", "product_variant_id"),

    ("feedbacks", "product_id", "products", "product_id"),

    ("chats", "order_key", "orders", "order_key"),
    ("chats", "product_id", "products", "product_id"),
    ("chats", "product_variant_id", "product_variants", "product_variant_id"),
]


def load_env():
    env = {}
    for line in ENV_PATH.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def connect():
    env = load_env()
    return psycopg2.connect(
        host=env.get("DB_HOST", "localhost"),
        port=env.get("DB_PORT", "5432"),
        dbname=env.get("DB_NAME", "wb_pipeline"),
        user=env.get("DB_USER", "wb_user"),
        password=env["DB_PASSWORD"],
    )


def table_columns(conn, table_name):
    with conn.cursor() as cur:
        cur.execute(
            """
            select column_name
            from information_schema.columns
            where table_schema = 'core'
              and table_name = %s
            order by ordinal_position;
            """,
            (table_name,),
        )
        return {r[0] for r in cur.fetchall()}


def scalar(conn, sql):
    with conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchone()[0]


def qident(name):
    return '"' + name.replace('"', '""') + '"'


def count_nulls(conn, table_name, column_name):
    return scalar(
        conn,
        f"""
        select count(*)
        from core.{qident(table_name)}
        where {qident(column_name)} is null;
        """,
    )


def count_duplicate_non_nulls(conn, table_name, column_name):
    return scalar(
        conn,
        f"""
        select count(*)
        from (
            select {qident(column_name)}
            from core.{qident(table_name)}
            where {qident(column_name)} is not null
            group by {qident(column_name)}
            having count(*) > 1
        ) x;
        """,
    )


def count_relationship_candidates(conn, source_table, source_column):
    return scalar(
        conn,
        f"""
        select count(*)
        from core.{qident(source_table)}
        where {qident(source_column)} is not null;
        """,
    )


def count_relationship_bad_rows(conn, source_table, source_column, target_table, target_column):
    return scalar(
        conn,
        f"""
        select count(*)
        from core.{qident(source_table)} s
        left join core.{qident(target_table)} t
            on s.{qident(source_column)} = t.{qident(target_column)}
        where s.{qident(source_column)} is not null
          and t.{qident(target_column)} is null;
        """,
    )


def load_yaml(path):
    if not path.exists():
        return {"version": 2, "models": []}

    doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not doc:
        return {"version": 2, "models": []}

    doc.setdefault("version", 2)
    doc.setdefault("models", [])
    return doc


def get_model_doc(doc, model_name):
    for model in doc["models"]:
        if model.get("name") == model_name:
            model.setdefault("columns", [])
            return model
    model = {"name": model_name, "description": f"Core-модель `{model_name}`.", "columns": []}
    doc["models"].append(model)
    return model


def get_column_doc(model_doc, column_name):
    for col in model_doc["columns"]:
        if col.get("name") == column_name:
            col.setdefault("description", f"Поле core-модели `{column_name}`.")
            return col
    col = {"name": column_name, "description": f"Поле core-модели `{column_name}`."}
    model_doc["columns"].append(col)
    return col


def has_simple_test(col_doc, test_name):
    for test in col_doc.get("tests", []):
        if isinstance(test, str) and test == test_name:
            return True
        if isinstance(test, dict) and test_name in test:
            return True
    return False


def has_relationship_test(col_doc, target_model, target_column):
    for test in col_doc.get("tests", []):
        if not isinstance(test, dict):
            continue
        rel = test.get("relationships")
        if not rel:
            continue
        if rel.get("to") == f"ref('{target_model}')" and rel.get("field") == target_column:
            return True
    return False


def add_simple_test(col_doc, test_name):
    col_doc.setdefault("tests", [])
    if not has_simple_test(col_doc, test_name):
        col_doc["tests"].append(test_name)
        return True
    return False


def add_relationship_test(col_doc, source_column, target_model, target_column):
    col_doc.setdefault("tests", [])

    if has_relationship_test(col_doc, target_model, target_column):
        return False

    col_doc["tests"].append(
        {
            "relationships": {
                "to": f"ref('{target_model}')",
                "field": target_column,
                "config": {
                    "where": f"{source_column} is not null"
                },
            }
        }
    )
    return True


def model_to_yml_path():
    result = {}

    for folder, yml_name in FOLDER_YML.items():
        folder_path = CORE_DIR / folder
        if not folder_path.exists():
            continue

        yml_path = folder_path / yml_name

        for sql_path in folder_path.glob("*.sql"):
            result[sql_path.stem] = yml_path

    return result


def main():
    conn = connect()
    model_yml = model_to_yml_path()

    docs_by_path = {}
    report_rows = []

    def get_doc(path):
        if path not in docs_by_path:
            docs_by_path[path] = load_yaml(path)
        return docs_by_path[path]

    # Primary key tests
    for model_name, pk_cols in sorted(PRIMARY_KEYS.items()):
        if model_name not in model_yml:
            continue

        cols = table_columns(conn, model_name)
        yml_path = model_yml[model_name]
        doc = get_doc(yml_path)
        model_doc = get_model_doc(doc, model_name)

        for col_name in pk_cols:
            if col_name not in cols:
                report_rows.append({
                    "test_type": "primary_key",
                    "model_name": model_name,
                    "column_name": col_name,
                    "target_model": "",
                    "target_column": "",
                    "candidate_rows": "",
                    "bad_rows": "",
                    "status": "missing_column",
                    "added": "false",
                })
                continue

            nulls = count_nulls(conn, model_name, col_name)
            duplicates = count_duplicate_non_nulls(conn, model_name, col_name)

            col_doc = get_column_doc(model_doc, col_name)

            added_not_null = False
            added_unique = False

            if nulls == 0:
                added_not_null = add_simple_test(col_doc, "not_null")

            if duplicates == 0:
                added_unique = add_simple_test(col_doc, "unique")

            report_rows.append({
                "test_type": "primary_key",
                "model_name": model_name,
                "column_name": col_name,
                "target_model": "",
                "target_column": "",
                "candidate_rows": "",
                "bad_rows": f"nulls={nulls};duplicates={duplicates}",
                "status": "pass" if nulls == 0 and duplicates == 0 else "fail_not_added_or_partial",
                "added": str(added_not_null or added_unique).lower(),
            })

    # Relationship tests
    for source_model, source_col, target_model, target_col in RELATIONSHIP_CANDIDATES:
        if source_model not in model_yml or target_model not in model_yml:
            continue

        source_cols = table_columns(conn, source_model)
        target_cols = table_columns(conn, target_model)

        if source_col not in source_cols or target_col not in target_cols:
            report_rows.append({
                "test_type": "relationship",
                "model_name": source_model,
                "column_name": source_col,
                "target_model": target_model,
                "target_column": target_col,
                "candidate_rows": "",
                "bad_rows": "",
                "status": "missing_column",
                "added": "false",
            })
            continue

        candidate_rows = count_relationship_candidates(conn, source_model, source_col)
        bad_rows = count_relationship_bad_rows(conn, source_model, source_col, target_model, target_col)

        yml_path = model_yml[source_model]
        doc = get_doc(yml_path)
        model_doc = get_model_doc(doc, source_model)
        col_doc = get_column_doc(model_doc, source_col)

        added = False
        status = "skipped_no_non_null_values"

        if candidate_rows > 0 and bad_rows == 0:
            added = add_relationship_test(col_doc, source_col, target_model, target_col)
            status = "pass"
        elif bad_rows > 0:
            status = "fail_not_added"

        report_rows.append({
            "test_type": "relationship",
            "model_name": source_model,
            "column_name": source_col,
            "target_model": target_model,
            "target_column": target_col,
            "candidate_rows": candidate_rows,
            "bad_rows": bad_rows,
            "status": status,
            "added": str(added).lower(),
        })

    for path, doc in docs_by_path.items():
        path.write_text(
            yaml.safe_dump(doc, allow_unicode=True, sort_keys=False, width=140),
            encoding="utf-8",
        )

    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)

    with REPORT_PATH.open("w", encoding="utf-8", newline="") as f:
        fieldnames = [
            "test_type",
            "model_name",
            "column_name",
            "target_model",
            "target_column",
            "candidate_rows",
            "bad_rows",
            "status",
            "added",
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(report_rows)

    added_count = sum(1 for r in report_rows if r["added"] == "true")
    failed_relationships = sum(1 for r in report_rows if r["status"] == "fail_not_added")

    print("report:", REPORT_PATH)
    print("tests added:", added_count)
    print("failed relationships not added:", failed_relationships)

    conn.close()


if __name__ == "__main__":
    main()
