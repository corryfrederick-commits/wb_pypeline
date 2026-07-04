#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="/opt/wb_pipeline"
DAYS="${1:-60}"
END_DATE="${2:-2026-07-03}"

cd "$PROJECT_DIR"

set -a
source "${PROJECT_DIR}/.env"
set +a

source "${PROJECT_DIR}/venv/bin/activate"

ORIGINAL_MOCK_BASE_URL="${WB_MOCK_BASE_URL%/}"

echo "WB one-time backfill load started"
echo "days: $DAYS"
echo "end date: $END_DATE"
echo "base mock url: $ORIGINAL_MOCK_BASE_URL"

PGPASSWORD="$DB_PASSWORD" psql -v ON_ERROR_STOP=1 -P pager=off \
  -h "${DB_HOST:-localhost}" \
  -p "${DB_PORT:-5432}" \
  -U "${DB_USER:-wb_user}" \
  -d "${DB_NAME:-wb_pipeline}" <<'SQL'
create schema if not exists control;

create table if not exists control.backfill_loaded_days (
    day date primary key,
    source_url text not null,
    loaded_at timestamptz not null default now()
);
SQL

for OFFSET in $(seq "$((DAYS - 1))" -1 0); do
    DAY="$(python - <<PY
from datetime import date, timedelta
end = date.fromisoformat("$END_DATE")
print((end - timedelta(days=$OFFSET)).isoformat())
PY
)"

    export WB_MOCK_BASE_URL="${ORIGINAL_MOCK_BASE_URL}/backfill/${DAY}"

    ALREADY_LOADED="$(
      PGPASSWORD="$DB_PASSWORD" psql -At \
        -h "${DB_HOST:-localhost}" \
        -p "${DB_PORT:-5432}" \
        -U "${DB_USER:-wb_user}" \
        -d "${DB_NAME:-wb_pipeline}" \
        -c "select 1 from control.backfill_loaded_days where day = date '${DAY}';"
    )"

    if [ "$ALREADY_LOADED" = "1" ]; then
        echo
        echo "=== skipping already loaded day: ${DAY} ==="
        continue
    fi

    echo
    echo "=== loading backfill day: ${DAY} ==="
    echo "WB_MOCK_BASE_URL=${WB_MOCK_BASE_URL}"

    python "${PROJECT_DIR}/loaders/download_mock_json.py"

    DOWNLOADED_COUNT="$(find "${PROJECT_DIR}/data/tmp_downloads" -maxdepth 1 -name "*.json" | wc -l)"
    echo "downloaded json files: ${DOWNLOADED_COUNT}"

    if [ "$DOWNLOADED_COUNT" -ne 31 ]; then
        echo "ERROR: expected 31 downloaded json files, got ${DOWNLOADED_COUNT}" >&2
        exit 1
    fi

    BEFORE_COUNT="$(
      PGPASSWORD="$DB_PASSWORD" psql -At \
        -h "${DB_HOST:-localhost}" \
        -p "${DB_PORT:-5432}" \
        -U "${DB_USER:-wb_user}" \
        -d "${DB_NAME:-wb_pipeline}" \
        -c "select count(*) from landing.raw_payloads;"
    )"

    python "${PROJECT_DIR}/loaders/load_raw_json_to_postgres.py"

    AFTER_COUNT="$(
      PGPASSWORD="$DB_PASSWORD" psql -At \
        -h "${DB_HOST:-localhost}" \
        -p "${DB_PORT:-5432}" \
        -U "${DB_USER:-wb_user}" \
        -d "${DB_NAME:-wb_pipeline}" \
        -c "select count(*) from landing.raw_payloads;"
    )"

    echo "raw_payloads before: ${BEFORE_COUNT}"
    echo "raw_payloads after:  ${AFTER_COUNT}"

    PGPASSWORD="$DB_PASSWORD" psql -v ON_ERROR_STOP=1 \
      -h "${DB_HOST:-localhost}" \
      -p "${DB_PORT:-5432}" \
      -U "${DB_USER:-wb_user}" \
      -d "${DB_NAME:-wb_pipeline}" \
      -c "insert into control.backfill_loaded_days(day, source_url) values (date '${DAY}', '${WB_MOCK_BASE_URL}') on conflict (day) do nothing;"
done

echo
echo "=== dbt build internal DWH ==="

cd "${PROJECT_DIR}/dbt/wb_dbt"

DBT_PROFILES_DIR="${PROJECT_DIR}/dbt" dbt build \
  --select models/staging models/staging_cleaned models/core models/marts \
  --threads 2

echo
echo "=== dbt build client exports ==="

DBT_PROFILES_DIR="${PROJECT_DIR}/dbt" dbt build \
  --select models/client_exports \
  --threads 2

echo
echo "=== final checks ==="

cd "$PROJECT_DIR"

PGPASSWORD="$DB_PASSWORD" psql -P pager=off \
  -h "${DB_HOST:-localhost}" \
  -p "${DB_PORT:-5432}" \
  -U "${DB_USER:-wb_user}" \
  -d "${DB_NAME:-wb_pipeline}" <<'SQL'
select count(*) as loaded_backfill_days
from control.backfill_loaded_days;

select count(*) as raw_payloads_count
from landing.raw_payloads;

select
    schemaname,
    count(*) as materialized_views_count
from pg_matviews
where schemaname = 'client_demo'
group by schemaname;

select count(*) as business_daily_rows
from client_demo.marts__mart_business_daily;
SQL

echo
echo "WB one-time backfill load finished"
