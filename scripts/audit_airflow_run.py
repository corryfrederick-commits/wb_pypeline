#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys

import psycopg2


def conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("DB_NAME", "wb_pipeline"),
        user=os.getenv("DB_USER", "wb_user"),
        password=os.getenv("DB_PASSWORD"),
    )


def run_id(cur, pipeline_name: str, orchestrator_run_id: str) -> int:
    cur.execute(
        """
        select run_id
        from audit.load_runs
        where pipeline_name = %s
          and orchestrator_run_id = %s
        order by run_id desc
        limit 1
        """,
        (pipeline_name, orchestrator_run_id),
    )
    row = cur.fetchone()
    if not row:
        raise RuntimeError("audit.load_runs row not found")
    return int(row[0])


def start(cur, pipeline_name: str, orchestrator_run_id: str, run_mode: str) -> int:
    cur.execute(
        """
        insert into audit.load_runs (
            pipeline_name,
            orchestrator_run_id,
            run_mode,
            status,
            started_at,
            finished_at,
            error_message
        )
        values (%s, %s, %s, 'running', now(), null, null)
        on conflict (pipeline_name, orchestrator_run_id)
        where orchestrator_run_id is not null
        do update set
            run_mode = excluded.run_mode,
            status = 'running',
            started_at = now(),
            finished_at = null,
            error_message = null
        returning run_id
        """,
        (pipeline_name, orchestrator_run_id, run_mode),
    )
    return int(cur.fetchone()[0])


def collect(cur, pipeline_name: str, orchestrator_run_id: str) -> int:
    rid = run_id(cur, pipeline_name, orchestrator_run_id)

    cur.execute(
        """
        insert into audit.dataset_runs (
            run_id,
            client_id,
            wb_account_id,
            source_system,
            dataset_name,
            source_file,
            status,
            started_at,
            finished_at,
            raw_payloads_loaded,
            raw_records_loaded,
            duplicate_payloads,
            skipped_duplicate_payloads,
            duplicate_payloads_in_raw
        )
        select
            %s::bigint,
            e.client_id,
            e.wb_account_id,
            coalesce(e.source_system, 'wb'),
            e.dataset_name,
            coalesce(e.source_file, 'unknown'),
            case
                when count(*) filter (where e.status = 'failed') > 0 then 'failed'
                else 'success'
            end,
            min(e.event_at),
            max(e.event_at),
            count(*) filter (where e.status = 'inserted'),
            coalesce(
                sum(
                    case
                        when e.status = 'inserted'
                        then coalesce(e.top_level_count, 1)
                        else 0
                    end
                ),
                0
            )::bigint,
            (
                count(*) filter (where e.status = 'inserted')
                - count(distinct e.raw_payload_id) filter (where e.status = 'inserted')
            )::bigint,
            count(*) filter (where e.status = 'skipped_duplicate'),
            (
                count(*) filter (where e.status = 'inserted')
                - count(distinct e.raw_payload_id) filter (where e.status = 'inserted')
            )::bigint
        from audit.raw_payload_load_events e
        where e.run_id = %s::bigint
        group by
            e.client_id,
            e.wb_account_id,
            coalesce(e.source_system, 'wb'),
            e.dataset_name,
            coalesce(e.source_file, 'unknown')
        on conflict (
            run_id,
            client_id,
            wb_account_id,
            source_system,
            dataset_name,
            source_file
        )
        do update set
            status = excluded.status,
            started_at = excluded.started_at,
            finished_at = excluded.finished_at,
            raw_payloads_loaded = excluded.raw_payloads_loaded,
            raw_records_loaded = excluded.raw_records_loaded,
            duplicate_payloads = excluded.duplicate_payloads,
            skipped_duplicate_payloads = excluded.skipped_duplicate_payloads,
            duplicate_payloads_in_raw = excluded.duplicate_payloads_in_raw,
            error_message = null
        """,
        (rid, rid),
    )
    return rid


def refresh_table_freshness(cur, run_id: int, run_status: str) -> None:
    freshness_status = "fresh" if run_status == "success" else "fresh_with_warnings"

    cur.execute(
        """
        select
            count(*)::bigint as row_count,
            max(loaded_at) as last_refreshed_at
        from landing.raw_payloads
        """
    )
    row_count, last_refreshed_at = cur.fetchone()

    cur.execute(
        """
        update audit.table_freshness
        set
            last_refreshed_at = %s,
            last_run_id = %s::bigint,
            last_run_status = %s,
            last_successful_run_id = case
                when %s = 'success' then %s::bigint
                else last_successful_run_id
            end,
            row_count = %s::bigint,
            status = %s,
            checked_at = now()
        where schema_name = 'landing'
          and table_name = 'raw_payloads'
          and client_id = '__all__'
          and wb_account_id = '__all__'
        """,
        (
            last_refreshed_at,
            run_id,
            run_status,
            run_status,
            run_id,
            row_count,
            freshness_status,
        ),
    )

    if cur.rowcount == 0:
        cur.execute(
            """
            insert into audit.table_freshness (
                schema_name,
                table_name,
                client_id,
                wb_account_id,
                last_refreshed_at,
                last_run_id,
                last_run_status,
                last_successful_run_id,
                row_count,
                status
            )
            values (
                'landing',
                'raw_payloads',
                '__all__',
                '__all__',
                %s,
                %s::bigint,
                %s,
                case when %s = 'success' then %s::bigint else null end,
                %s::bigint,
                %s
            )
            """,
            (
                last_refreshed_at,
                run_id,
                run_status,
                run_status,
                run_id,
                row_count,
                freshness_status,
            ),
        )



def finish(cur, pipeline_name: str, orchestrator_run_id: str, status: str, error_message: str | None) -> int:
    rid = run_id(cur, pipeline_name, orchestrator_run_id)

    final_status = status
    final_error_message = error_message

    if status == "success":
        cur.execute(
            """
            select count(*)::bigint
            from audit.dataset_runs
            where run_id = %s::bigint
              and status = 'failed'
            """,
            (rid,),
        )
        failed_dataset_runs = cur.fetchone()[0]

        if failed_dataset_runs > 0:
            final_status = "partial_success"
            final_error_message = f"{failed_dataset_runs} dataset run(s) failed"

    cur.execute(
        """
        update audit.load_runs
        set
            status = %s,
            finished_at = now(),
            error_message = %s
        where run_id = %s::bigint
        """,
        (final_status, final_error_message, rid),
    )

    if final_status in ("success", "partial_success"):
        refresh_table_freshness(cur, rid, final_status)

    return rid


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["start", "collect", "success", "failed", "get-run-id"])
    parser.add_argument("--pipeline-name", default="wb_daily_pipeline")
    parser.add_argument("--orchestrator-run-id", required=True)
    parser.add_argument("--run-mode", default="scheduled")
    parser.add_argument("--error-message")
    args = parser.parse_args()

    with conn() as c:
        with c.cursor() as cur:
            if args.action == "get-run-id":
                rid = run_id(cur, args.pipeline_name, args.orchestrator_run_id)
                print(rid)
                return 0
            elif args.action == "start":
                rid = start(cur, args.pipeline_name, args.orchestrator_run_id, args.run_mode)
            elif args.action == "collect":
                rid = collect(cur, args.pipeline_name, args.orchestrator_run_id)
            elif args.action == "success":
                rid = finish(cur, args.pipeline_name, args.orchestrator_run_id, "success", None)
            else:
                rid = finish(
                    cur,
                    args.pipeline_name,
                    args.orchestrator_run_id,
                    "failed",
                    args.error_message or "Airflow DAG failed; inspect task logs",
                )

    print(f"audit {args.action}: run_id={rid}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
