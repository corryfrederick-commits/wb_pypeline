from __future__ import annotations

from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator


DEFAULT_ARGS = {
    "owner": "wb_pipeline",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def bash(cmd: str) -> str:
    return dedent(cmd).strip()


def audit_cmd(action: str, extra_args: str = "") -> str:
    return bash(f"""
        set -euo pipefail

        cd /opt/wb_pipeline

        set -a
        source /opt/wb_pipeline/.env
        set +a

        source /opt/wb_pipeline/venv/bin/activate

        python /opt/wb_pipeline/scripts/audit_airflow_run.py {action} --orchestrator-run-id "{{{{ run_id }}}}" {extra_args}
    """)


with DAG(
    dag_id="wb_daily_pipeline",
    description="Daily WB mock API load, dbt build and client materialized exports",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 7, 3),
    schedule_interval="0 4 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["wb", "dwh", "daily"],
) as dag:

    audit_start_run = BashOperator(
        task_id="audit_start_run",
        bash_command=audit_cmd("start", "--run-mode scheduled"),
    )

    audit_collect_dataset_runs = BashOperator(
        task_id="audit_collect_dataset_runs",
        bash_command=audit_cmd("collect"),
    )

    audit_finish_success = BashOperator(
        task_id="audit_finish_success",
        bash_command=audit_cmd("success"),
    )

    audit_finish_failed = BashOperator(
        task_id="audit_finish_failed",
        trigger_rule="one_failed",
        bash_command=audit_cmd("failed"),
    )

    check_mock_api = BashOperator(
        task_id="check_mock_api",
        bash_command=bash("""
            set -euo pipefail

            cd /opt/wb_pipeline

            set -a
            source /opt/wb_pipeline/.env
            set +a

            URL="${WB_MOCK_BASE_URL%/}/general_seller_info.json"

            echo "Checking mock API: $URL"
            curl -fsS "$URL" >/tmp/wb_mock_api_check.json

            test -s /tmp/wb_mock_api_check.json
            echo "Mock API is available"
        """),
    )

    download_mock_json = BashOperator(
        task_id="download_mock_json",
        bash_command=bash("""
            set -euo pipefail

            cd /opt/wb_pipeline

            set -a
            source /opt/wb_pipeline/.env
            set +a

            source /opt/wb_pipeline/venv/bin/activate

            python /opt/wb_pipeline/loaders/download_mock_json.py
        """),
    )

    load_raw_payloads = BashOperator(
        task_id="load_raw_payloads",
        bash_command=bash("""
            set -euo pipefail

            cd /opt/wb_pipeline

            set -a
            source /opt/wb_pipeline/.env
            set +a

            source /opt/wb_pipeline/venv/bin/activate

            AUDIT_RUN_ID="$(
              python /opt/wb_pipeline/scripts/audit_airflow_run.py get-run-id --orchestrator-run-id "{{ run_id }}"
            )"

            export AUDIT_RUN_ID

            echo "AUDIT_RUN_ID=$AUDIT_RUN_ID"

            python /opt/wb_pipeline/loaders/load_raw_json_to_postgres.py
        """),
    )

    detect_json_schema_drift = BashOperator(
        task_id="detect_json_schema_drift",
        bash_command=bash("""
            set -euo pipefail

            cd /opt/wb_pipeline

            set -a
            source /opt/wb_pipeline/.env
            set +a

            source /opt/wb_pipeline/venv/bin/activate

            python /opt/wb_pipeline/loaders/discover_json_fields.py --mode check
        """),
    )

    dbt_build_internal_dwh = BashOperator(
        task_id="dbt_build_internal_dwh",
        bash_command=bash("""
            set -euo pipefail

            cd /opt/wb_pipeline/dbt/wb_dbt

            set -a
            source /opt/wb_pipeline/.env
            set +a

            source /opt/wb_pipeline/venv/bin/activate

            DBT_PROFILES_DIR=/opt/wb_pipeline/dbt dbt build \
              --select models/staging models/staging_cleaned models/core models/marts \
              --threads 2
        """),
    )

    dbt_build_client_exports = BashOperator(
        task_id="dbt_build_client_exports",
        bash_command=bash("""
            set -euo pipefail

            cd /opt/wb_pipeline/dbt/wb_dbt

            set -a
            source /opt/wb_pipeline/.env
            set +a

            source /opt/wb_pipeline/venv/bin/activate

            DBT_PROFILES_DIR=/opt/wb_pipeline/dbt dbt build \
              --select models/client_exports \
              --threads 2
        """),
    )

    check_client_demo = BashOperator(
        task_id="check_client_demo",
        bash_command=bash("""
            set -euo pipefail

            cd /opt/wb_pipeline

            set -a
            source /opt/wb_pipeline/.env
            set +a

            MATVIEWS_COUNT="$(
              PGPASSWORD="$DB_PASSWORD" psql -At \
                -h "${DB_HOST:-localhost}" \
                -p "${DB_PORT:-5432}" \
                -U "${DB_USER:-wb_user}" \
                -d "${DB_NAME:-wb_pipeline}" \
                -c "select count(*) from pg_matviews where schemaname = 'client_demo';"
            )"

            BUSINESS_ROWS="$(
              PGPASSWORD="$DB_PASSWORD" psql -At \
                -h "${DB_HOST:-localhost}" \
                -p "${DB_PORT:-5432}" \
                -U "${DB_USER:-wb_user}" \
                -d "${DB_NAME:-wb_pipeline}" \
                -c "select count(*) from client_demo.marts__mart_business_daily;"
            )"

            echo "client_demo materialized views: $MATVIEWS_COUNT"
            echo "client_demo.marts__mart_business_daily rows: $BUSINESS_ROWS"

            test "$MATVIEWS_COUNT" -eq 54
            test "$BUSINESS_ROWS" -gt 0
        """),
    )

    (
        audit_start_run
        >> check_mock_api
        >> download_mock_json
        >> load_raw_payloads
        >> audit_collect_dataset_runs
        >> detect_json_schema_drift
        >> dbt_build_internal_dwh
        >> dbt_build_client_exports
        >> check_client_demo
        >> audit_finish_success
    )

    [
        check_mock_api,
        download_mock_json,
        load_raw_payloads,
        audit_collect_dataset_runs,
        detect_json_schema_drift,
        dbt_build_internal_dwh,
        dbt_build_client_exports,
        check_client_demo,
    ] >> audit_finish_failed
