import csv
from collections import defaultdict
from pathlib import Path

import yaml


PROJECT = Path("/opt/wb_pipeline/dbt/wb_dbt")
STAGING_DIR = PROJECT / "models" / "staging"
RULES_PATH = PROJECT / "metadata" / "row_quality_rules.csv"
OUT_DIR = PROJECT / "models" / "staging_cleaned"


QUALITY_COLUMNS = [
    {
        "name": "issue_count",
        "description": "Общее количество найденных row-quality нарушений по строке.",
    },
    {
        "name": "bad_issue_count",
        "description": "Количество критических bad-нарушений по строке.",
    },
    {
        "name": "warning_issue_count",
        "description": "Количество warning-предупреждений по строке.",
    },
    {
        "name": "quality_status",
        "description": "Итоговый статус качества строки: good, partial или bad.",
    },
    {
        "name": "quality_issues",
        "description": "Массив кодов критических bad-нарушений.",
    },
    {
        "name": "warning_issues",
        "description": "Массив кодов warning-предупреждений.",
    },
    {
        "name": "can_load_to_cleaned",
        "description": "Флаг, показывающий, что строка прошла row quarantine и разрешена к загрузке в staging_cleaned.",
    },
]


def clean_name_from_staging_model(model_name: str) -> str:
    if model_name == "stg_orders_current":
        return "orders"

    if model_name.startswith("stg_"):
        return model_name[4:]

    return model_name


def cleaned_model_name(cleaned_table: str) -> str:
    return f"{cleaned_table}_cleaned"


def cleaned_view_name(cleaned_table: str) -> str:
    return f"v_{cleaned_table}_for_cleaned"


def load_staging_yml_models():
    result = {}

    for yml_path in sorted(STAGING_DIR.glob("**/*.yml")):
        doc = yaml.safe_load(yml_path.read_text(encoding="utf-8"))

        if not doc or "models" not in doc:
            continue

        rel = yml_path.relative_to(STAGING_DIR)
        domain = rel.parts[0] if len(rel.parts) > 1 else "misc"

        for model in doc.get("models", []):
            name = model.get("name")
            if not name:
                continue

            result[name] = {
                "domain": domain,
                "description": model.get("description", ""),
                "columns": model.get("columns", []),
                "source_yml": str(yml_path),
            }

    return result


def load_row_quality_models():
    rows = list(csv.DictReader(RULES_PATH.open("r", encoding="utf-8")))

    models = {}

    for r in rows:
        if r.get("enabled") != "true":
            continue

        if r.get("severity") == "info":
            continue

        model_name = r["model_name"]
        cleaned_table = r["cleaned_table"]

        models[model_name] = cleaned_table

    return models


def dedupe_columns(columns):
    seen = set()
    out = []

    for col in columns:
        name = col.get("name")
        if not name or name in seen:
            continue
        seen.add(name)
        out.append(col)

    return out


def main():
    staging_models = load_staging_yml_models()
    row_quality_models = load_row_quality_models()

    generated = 0

    for staging_model_name, cleaned_table in sorted(row_quality_models.items()):
        if staging_model_name not in staging_models:
            raise RuntimeError(f"Staging model not found in YAML: {staging_model_name}")

        meta = staging_models[staging_model_name]

        domain = meta["domain"]
        model_name = cleaned_model_name(cleaned_table)
        view_name = cleaned_view_name(cleaned_table)

        domain_dir = OUT_DIR / domain
        domain_dir.mkdir(parents=True, exist_ok=True)

        sql_path = domain_dir / f"{model_name}.sql"
        yml_path = domain_dir / f"{model_name}.yml"

        sql = f"""{{{{ config(materialized='table', schema='staging_cleaned', alias='{cleaned_table}', tags=['staging_cleaned']) }}}}

select *
from {{{{ ref('{view_name}') }}}}
"""

        description = (
            f"Очищенная staging-модель `{cleaned_table}`. "
            f"Данные берутся из `quarantine.{view_name}` после единого row quarantine. "
            f"Сохраняет endpoint-level структуру и technical/quality поля. "
            f"Это trusted staging слой, а не финальная нормализованная core-сущность."
        )

        columns = dedupe_columns(list(meta["columns"]) + QUALITY_COLUMNS)

        yml_doc = {
            "version": 2,
            "models": [
                {
                    "name": model_name,
                    "description": description,
                    "columns": columns,
                }
            ],
        }

        sql_path.write_text(sql, encoding="utf-8")
        yml_path.write_text(
            yaml.safe_dump(yml_doc, allow_unicode=True, sort_keys=False, width=140),
            encoding="utf-8",
        )

        generated += 1

        print(
            f"GENERATED {staging_model_name:40s} "
            f"-> {model_name:40s} "
            f"alias staging_cleaned.{cleaned_table}"
        )

    print()
    print("generated staging_cleaned models:", generated)


if __name__ == "__main__":
    main()
