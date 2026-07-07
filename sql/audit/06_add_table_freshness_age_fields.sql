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
    now() - checked_at AS checked_age,
    CASE
        WHEN last_refreshed_at IS NULL THEN NULL::interval
        ELSE now() - last_refreshed_at
    END AS freshness_age,
    last_run_id,
    last_run_status,
    last_successful_run_id
FROM audit.table_freshness;
