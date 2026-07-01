from pathlib import Path
import yaml

PROJECT = Path("/opt/wb_pipeline/dbt/wb_dbt")

overrides = {
    ("stg_finance_sales_reports", "operation_date"):
        "Дата и время операции в финансовом отчёте. Исходное поле WB JSON: `operationDate`. Описание добавлено вручную, потому что в исходном WB YAML для этого поля не было description.",

    ("stg_finance_sales_reports_detailed", "operation_date"):
        "Дата и время операции в детализированном финансовом отчёте. Исходное поле WB JSON: `operationDate`. Описание добавлено вручную, потому что в исходном WB YAML для этого поля не было description.",

    ("stg_report_orders", "operation_date"):
        "Дата и время операции в отчёте по заказам. Исходное поле WB JSON: `operationDate`. Описание добавлено вручную, потому что в исходном WB YAML для этого поля не было description.",

    ("stg_report_sales", "operation_date"):
        "Дата и время операции в отчёте по продажам. Исходное поле WB JSON: `operationDate`. Описание добавлено вручную, потому что в исходном WB YAML для этого поля не было description.",

    ("stg_promotion_campaigns", "campaign_id"):
        "Идентификатор рекламной кампании. Исходное поле WB JSON: `campaignId`. Описание добавлено вручную, потому что в исходном WB YAML для этого поля не было description.",

    ("stg_promotion_fullstats", "root_campaign_id"):
        "Идентификатор рекламной кампании на корневом уровне fullstats. Исходное поле WB JSON: `campaignId`. Описание добавлено вручную, потому что в исходном WB YAML для этого поля не было description.",
}

changed_files = 0
changed_columns = 0

for path in sorted((PROJECT / "models" / "staging").glob("**/*.yml")):
    doc = yaml.safe_load(path.read_text(encoding="utf-8"))

    if not doc or "models" not in doc:
        continue

    changed = False

    for model in doc.get("models", []):
        model_name = model.get("name")

        for col in model.get("columns", []):
            key = (model_name, col.get("name"))

            if key in overrides:
                old = col.get("description", "")
                new = overrides[key]

                if old != new:
                    col["description"] = new
                    changed = True
                    changed_columns += 1
                    print(f"UPDATED {model_name}.{col.get('name')}")

    if changed:
        path.write_text(
            yaml.safe_dump(
                doc,
                allow_unicode=True,
                sort_keys=False,
                width=140,
            ),
            encoding="utf-8",
        )
        changed_files += 1

print()
print("changed_files:", changed_files)
print("changed_columns:", changed_columns)
