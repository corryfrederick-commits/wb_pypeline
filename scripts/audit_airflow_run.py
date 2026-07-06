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
            duplicate_payloads
        )
        with run_scope as (
            select
                run_id,
                started_at,
                coalesce(finished_at, now()) as finished_at
            from audit.load_runs
            where run_id = %s::bigint
        )
        select
            rs.run_id,
            rp.client_id,
            rp.wb_account_id,
            coalesce(rp.source_system, 'wb'),
            rp.dataset_name,
            coalesce(rp.source_file, 'unknown'),
            'success',
            min(rp.loaded_at),
            max(rp.loaded_at),
            count(*)::bigint,
            sum(coalesce(rp.top_level_count, 1))::bigint,
            (count(*) - count(distinct md5(rp.payload::text)))::bigint
        from landing.raw_payloads rp
        join run_scope rs
          on rp.loaded_at >= rs.started_at
         and rp.loaded_at <= rs.finished_at
        group by
            rs.run_id,
            rp.client_id,
            rp.wb_account_id,
            coalesce(rp.source_system, 'wb'),
            rp.dataset_name,
            coalesce(rp.source_file, 'unknown')
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
            error_message = null
        """,
        (rid,),
    )
    return rid


def finish(cur, pipeline_name: str, orchestrator_run_id: str, status: str, error: str | None) -> int:
    rid = run_id(cur, pipeline_name, orchestrator_run_id)

    if status == "success":
        cur.execute(
            """
            insert into audit.table_freshness (
                schema_name,
                table_name,
                client_id,
                wb_account_id,
                last_refreshed_at,
                last_successful_run_id,
                row_count,
                max_data_date,
                status,
                checked_at
            )
            select
                'landing',
                'raw_payloads',
                '__all__',
                '__all__',
                max(loaded_at),
                %s::bigint,
                count(*)::bigint,
                null::date,
                case
                    when count(*) = 0 then 'empty'
                    when max(loaded_at) < now() - interval '36 hours' then 'stale'
                    else 'fresh'
                end,
                now()
            from landing.raw_payloads
            on conflict (schema_name, table_name, client_id, wb_account_id)
            do update set
                last_refreshed_at = excluded.last_refreshed_at,
                last_successful_run_id = excluded.last_successful_run_id,
                row_count = excluded.row_count,
                max_data_date = excluded.max_data_date,
                status = excluded.status,
                checked_at = now()
            """,
            (rid,),
        )

    cur.execute(
        """
        update audit.load_runs
        set status = %s,
            finished_at = now(),
            error_message = %s
        where run_id = %s
        """,
        (status, error, rid),
    )
    return rid


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["start", "collect", "success", "failed"])
    parser.add_argument("--pipeline-name", default="wb_daily_pipeline")
    parser.add_argument("--orchestrator-run-id", required=True)
    parser.add_argument("--run-mode", default="scheduled")
    parser.add_argument("--error-message")
    args = parser.parse_args()

    with conn() as c:
        with c.cursor() as cur:
            if args.action == "start":
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
