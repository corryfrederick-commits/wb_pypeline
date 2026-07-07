DROP VIEW IF EXISTS audit.v_pipeline_failures;
DROP VIEW IF EXISTS audit.v_table_freshness;

CREATE VIEW audit.v_table_freshness AS
SELECT
    schema_name,
    table_name,
    client_id,
    wb_account_id,
    last_refreshed_at,
    max_data_date,
    row_count,
    status,
    checked_at,
    last_run_id,
    last_run_status,
    last_successful_run_id
FROM audit.table_freshness;

CREATE VIEW audit.v_pipeline_failures AS
SELECT
    'pipeline'::text AS failure_level,
    lr.run_id,
    NULL::bigint AS dataset_run_id,
    lr.pipeline_name,
    NULL::text AS client_id,
    NULL::text AS wb_account_id,
    NULL::text AS dataset_name,
    lr.status,
    lr.started_at,
    lr.finished_at,
    lr.error_message
FROM audit.load_runs lr
WHERE lr.status IN ('failed', 'partial_success')
UNION ALL
SELECT
    'dataset'::text AS failure_level,
    dr.run_id,
    dr.dataset_run_id,
    lr.pipeline_name,
    dr.client_id,
    dr.wb_account_id,
    dr.dataset_name,
    dr.status,
    dr.started_at,
    dr.finished_at,
    dr.error_message
FROM audit.dataset_runs dr
JOIN audit.load_runs lr
  ON lr.run_id = dr.run_id
WHERE dr.status = 'failed';
