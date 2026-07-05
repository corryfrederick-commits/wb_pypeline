import os
import csv
from pathlib import Path

import psycopg2


PROJECT_ROOT = Path("/opt/wb_pipeline")
DBT_MODELS_DIR = PROJECT_ROOT / "dbt" / "wb_dbt" / "models"

CORE_FACT_OBJECTS = [
    "orders",
    "order_items",
    "report_order_events",
    "report_sale_events",
    "orders_current",
    "order_items_current",
    "report_order_events_current",
    "report_sale_events_current",
]

TARGET_SCHEMAS = [
    "marts",
    "client_exports",
    "client_demo",
]

TECHNICAL_PATTERNS = [
    "_version_key",
    "_row_hash",
    "row_hash",
    "version_number",
    "valid_from",
    "valid_to",
    "is_current",
    "loaded_at",
    "ingested_at",
    "raw_payload",
    "raw_payload_id",
    "source_file",
    "source_dataset",
    "source_system",
    "dbt_",
]


def connect():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("DB_NAME", "wb_pipeline"),
        user=os.getenv("DB_USER", "wb_user"),
        password=os.getenv("DB_PASSWORD"),
    )


def is_technical(column_name: str) -> bool:
    name = column_name.lower()
    return any(pattern in name for pattern in TECHNICAL_PATTERNS)


def read_sql_files():
    sql_texts = {}

    for folder in [
        DBT_MODELS_DIR / "marts",
        DBT_MODELS_DIR / "client_exports",
    ]:
        if not folder.exists():
            continue

        for path in folder.rglob("*.sql"):
            try:
                sql_texts[str(path.relative_to(PROJECT_ROOT))] = path.read_text(
                    encoding="utf-8"
                ).lower()
            except UnicodeDecodeError:
                sql_texts[str(path.relative_to(PROJECT_ROOT))] = path.read_text(
                    encoding="latin-1"
                ).lower()

    return sql_texts


def main():
    out_dir = PROJECT_ROOT / "docs"
    out_dir.mkdir(exist_ok=True)

    csv_path = out_dir / "core_fact_to_marts_field_coverage.csv"
    md_path = out_dir / "core_fact_to_marts_field_coverage.md"

    sql_texts = read_sql_files()

    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select table_schema, table_name, column_name, data_type
                from information_schema.columns
                where table_schema = 'core'
                  and table_name = any(%s)
                order by table_name, ordinal_position;
                """,
                (CORE_FACT_OBJECTS,),
            )
            core_columns = cur.fetchall()

            cur.execute(
                """
                select table_schema, table_name, column_name, data_type
                from information_schema.columns
                where table_schema = any(%s)
                order by table_schema, table_name, ordinal_position;
                """,
                (TARGET_SCHEMAS,),
            )
            mart_columns = cur.fetchall()

    mart_column_index = {}
    for schema, table, column, data_type in mart_columns:
        mart_column_index.setdefault(column.lower(), []).append(
            f"{schema}.{table}.{column}"
        )

    rows = []

    for core_schema, core_table, column, data_type in core_columns:
        col_lower = column.lower()

        output_locations = mart_column_index.get(col_lower, [])

        sql_usage_locations = [
            file_path
            for file_path, text in sql_texts.items()
            if col_lower in text
        ]

        if output_locations:
            status = "present_as_output_column"
        elif sql_usage_locations:
            status = "used_in_mart_sql_but_not_output_column"
        else:
            status = "not_found_by_name"

        rows.append(
            {
                "core_object": f"{core_schema}.{core_table}",
                "core_column": column,
                "core_data_type": data_type,
                "is_likely_technical": "yes" if is_technical(column) else "no",
                "coverage_status": status,
                "marts_output_locations": "; ".join(output_locations),
                "dbt_sql_usage_locations": "; ".join(sql_usage_locations),
            }
        )

    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "core_object",
                "core_column",
                "core_data_type",
                "is_likely_technical",
                "coverage_status",
                "marts_output_locations",
                "dbt_sql_usage_locations",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)

    total = len(rows)
    present = sum(1 for r in rows if r["coverage_status"] == "present_as_output_column")
    used = sum(1 for r in rows if r["coverage_status"] == "used_in_mart_sql_but_not_output_column")
    missing = sum(1 for r in rows if r["coverage_status"] == "not_found_by_name")
    technical_missing = sum(
        1
        for r in rows
        if r["coverage_status"] == "not_found_by_name"
        and r["is_likely_technical"] == "yes"
    )
    business_missing = sum(
        1
        for r in rows
        if r["coverage_status"] == "not_found_by_name"
        and r["is_likely_technical"] == "no"
    )

    with md_path.open("w", encoding="utf-8") as f:
        f.write("# Core fact fields coverage in marts\n\n")
        f.write("Проверка показывает, какие поля из core fact/current tables присутствуют в marts/client exports.\n\n")

        f.write("## Summary\n\n")
        f.write(f"- Total core fact columns checked: {total}\n")
        f.write(f"- Present as output columns in marts/client exports: {present}\n")
        f.write(f"- Used in mart SQL, but not exposed as output columns: {used}\n")
        f.write(f"- Not found by column name: {missing}\n")
        f.write(f"- Not found and likely technical: {technical_missing}\n")
        f.write(f"- Not found and likely business fields: {business_missing}\n\n")

        f.write("## Important interpretation\n\n")
        f.write("Не каждое поле из core должно напрямую попадать в marts.\n\n")
        f.write("Обычно в marts не выводятся:\n\n")
        f.write("- SCD2 technical fields;\n")
        f.write("- hash fields;\n")
        f.write("- service metadata;\n")
        f.write("- raw payload ids;\n")
        f.write("- internal source tracking fields.\n\n")

        f.write("Но если business-поле не найдено ни как output column, ни в SQL marts, это надо проверить отдельно.\n\n")

        f.write("## Fields not found by name\n\n")
        f.write("| core_object | core_column | data_type | technical? |\n")
        f.write("|---|---|---|---|\n")

        for r in rows:
            if r["coverage_status"] == "not_found_by_name":
                f.write(
                    f"| {r['core_object']} | {r['core_column']} | "
                    f"{r['core_data_type']} | {r['is_likely_technical']} |\n"
                )

    print(f"[OK] CSV report: {csv_path}")
    print(f"[OK] Markdown report: {md_path}")
    print()
    print("Summary:")
    print(f"  total checked: {total}")
    print(f"  present as output column: {present}")
    print(f"  used in SQL only: {used}")
    print(f"  not found by name: {missing}")
    print(f"  not found technical: {technical_missing}")
    print(f"  not found business: {business_missing}")


if __name__ == "__main__":
    main()
