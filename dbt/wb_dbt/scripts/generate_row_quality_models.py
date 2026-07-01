import csv
from collections import defaultdict
from pathlib import Path

PROJECT = Path("/opt/wb_pipeline/dbt/wb_dbt")
RULES_PATH = PROJECT / "metadata" / "row_quality_rules.csv"
OUT_DIR = PROJECT / "models" / "quarantine" / "row_quality"


def sql_literal(value: str) -> str:
    if value is None:
        value = ""
    return "'" + str(value).replace("'", "''") + "'"


def load_rules():
    rows = list(csv.DictReader(RULES_PATH.open("r", encoding="utf-8")))

    grouped = defaultdict(list)

    for r in rows:
        if r.get("enabled") != "true":
            continue

        if r.get("severity") == "info":
            continue

        condition = (r.get("sql_condition") or "").strip()
        if not condition:
            continue

        grouped[r["model_name"]].append(r)

    return grouped


def issue_model_name(cleaned_table):
    return f"rq_{cleaned_table}_issues"


def quality_model_name(cleaned_table):
    return f"rq_{cleaned_table}_quality"


def cleaned_view_name(cleaned_table):
    return f"v_{cleaned_table}_for_cleaned"


def generate_issues_sql(model_name, cleaned_table, rules):
    model = issue_model_name(cleaned_table)

    parts = []

    for r in rules:
        condition = r["sql_condition"]

        part = f"""
select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    {sql_literal(r["rule_id"])}::text as rule_id,
    {sql_literal(r["rule_group"])}::text as rule_group,
    {sql_literal(r["rule_type"])}::text as rule_type,
    {sql_literal(r["severity"])}::text as issue_severity,
    {sql_literal(r["column_name"])}::text as column_name,
    {sql_literal(r["issue_code"])}::text as issue_code,
    {sql_literal(r["issue_message"])}::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        ({condition}) as is_issue
    from base
) q
where q.is_issue is true
""".strip()

        parts.append(part)

    if not parts:
        union_sql = """
select
    null as raw_payload_id,
    null::integer as record_index,
    null::text as dataset_name,
    null::text as rule_id,
    null::text as rule_group,
    null::text as rule_type,
    null::text as issue_severity,
    null::text as column_name,
    null::text as issue_code,
    null::text as issue_message,
    current_timestamp as detected_at
where false
""".strip()
    else:
        union_sql = "\n\nunion all\n\n".join(parts)

    return f"""{{{{ config(materialized='table', schema='quarantine', alias='{model}', tags=['row_quality']) }}}}

with base as (
    select *
    from {{{{ ref('{model_name}') }}}}
),

issues as (
{union_sql}
)

select *
from issues
"""


def generate_quality_sql(model_name, cleaned_table):
    issues_model = issue_model_name(cleaned_table)
    quality_model = quality_model_name(cleaned_table)

    return f"""{{{{ config(materialized='table', schema='quarantine', alias='{quality_model}', tags=['row_quality']) }}}}

with base as (
    select *
    from {{{{ ref('{model_name}') }}}}
),

issues as (
    select *
    from {{{{ ref('{issues_model}') }}}}
),

issues_agg as (
    select
        raw_payload_id,
        record_index,

        count(*) as issue_count,

        count(*) filter (
            where issue_severity = 'bad'
        ) as bad_issue_count,

        count(*) filter (
            where issue_severity = 'warning'
        ) as warning_issue_count,

        array_agg(issue_code order by issue_code) filter (
            where issue_severity = 'bad'
        ) as quality_issues,

        array_agg(issue_code order by issue_code) filter (
            where issue_severity = 'warning'
        ) as warning_issues

    from issues
    group by
        raw_payload_id,
        record_index
)

select
    base.*,

    coalesce(issues_agg.issue_count, 0) as issue_count,
    coalesce(issues_agg.bad_issue_count, 0) as bad_issue_count,
    coalesce(issues_agg.warning_issue_count, 0) as warning_issue_count,

    case
        when coalesce(issues_agg.bad_issue_count, 0) > 0 then 'bad'
        when coalesce(issues_agg.warning_issue_count, 0) > 0 then 'partial'
        else 'good'
    end as quality_status,

    coalesce(issues_agg.quality_issues, array[]::text[]) as quality_issues,
    coalesce(issues_agg.warning_issues, array[]::text[]) as warning_issues,

    coalesce(issues_agg.bad_issue_count, 0) = 0 as can_load_to_cleaned

from base
left join issues_agg
    on base.raw_payload_id = issues_agg.raw_payload_id
   and base.record_index = issues_agg.record_index
"""


def generate_view_sql(cleaned_table):
    quality_model = quality_model_name(cleaned_table)
    view_model = cleaned_view_name(cleaned_table)

    return f"""{{{{ config(materialized='view', schema='quarantine', alias='{view_model}', tags=['row_quality']) }}}}

select *
from {{{{ ref('{quality_model}') }}}}
where can_load_to_cleaned = true
"""


def generate_yml(model_name, cleaned_table):
    issues_model = issue_model_name(cleaned_table)
    quality_model = quality_model_name(cleaned_table)
    view_model = cleaned_view_name(cleaned_table)

    return f"""version: 2

models:
  - name: {issues_model}
    description: "Единая row-quality таблица нарушений для `{model_name}`. Одна строка = одно нарушение одного правила одной staging-строки."
    columns:
      - name: raw_payload_id
        description: "Идентификатор исходного payload из landing.raw_payloads."
      - name: record_index
        description: "Порядковый номер записи внутри распарсенного payload."
      - name: dataset_name
        description: "Имя исходного mock/WB dataset."
      - name: rule_id
        description: "Технический идентификатор row-quality правила."
      - name: rule_group
        description: "Группа правила качества."
      - name: rule_type
        description: "Тип правила качества."
      - name: issue_severity
        description: "Критичность нарушения: bad или warning."
      - name: column_name
        description: "Колонка или набор колонок, к которым относится правило."
      - name: issue_code
        description: "Код найденной проблемы качества."
      - name: issue_message
        description: "Человекочитаемое описание проблемы качества."
      - name: detected_at
        description: "Время вычисления нарушения."

  - name: {quality_model}
    description: "Единая row-quality таблица для `{model_name}`. Содержит исходные staging-поля и агрегированные quality-флаги."
    columns:
      - name: issue_count
        description: "Общее количество найденных нарушений по строке."
      - name: bad_issue_count
        description: "Количество критических нарушений bad по строке."
      - name: warning_issue_count
        description: "Количество предупреждений warning по строке."
      - name: quality_status
        description: "Итоговый статус качества строки: good, partial или bad."
      - name: quality_issues
        description: "Массив кодов критических bad-нарушений."
      - name: warning_issues
        description: "Массив кодов warning-предупреждений."
      - name: can_load_to_cleaned
        description: "Флаг, показывающий, можно ли загружать строку в staging_cleaned."

  - name: {view_model}
    description: "View строк `{model_name}`, разрешённых к загрузке в staging_cleaned. Фильтр: can_load_to_cleaned = true."
"""


def main():
    grouped = load_rules()

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    generated = 0

    for model_name in sorted(grouped):
        rules = grouped[model_name]
        cleaned_table = rules[0]["cleaned_table"]

        issues = issue_model_name(cleaned_table)
        quality = quality_model_name(cleaned_table)
        view = cleaned_view_name(cleaned_table)

        (OUT_DIR / f"{issues}.sql").write_text(
            generate_issues_sql(model_name, cleaned_table, rules),
            encoding="utf-8",
        )

        (OUT_DIR / f"{quality}.sql").write_text(
            generate_quality_sql(model_name, cleaned_table),
            encoding="utf-8",
        )

        (OUT_DIR / f"{view}.sql").write_text(
            generate_view_sql(cleaned_table),
            encoding="utf-8",
        )

        (OUT_DIR / f"{cleaned_table}_row_quality.yml").write_text(
            generate_yml(model_name, cleaned_table),
            encoding="utf-8",
        )

        generated += 3

        print(
            f"GENERATED {model_name:40s} "
            f"-> {issues}, {quality}, {view}"
        )

    print()
    print("staging models:", len(grouped))
    print("dbt models generated:", generated)


if __name__ == "__main__":
    main()
