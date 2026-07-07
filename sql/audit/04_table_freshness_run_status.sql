ALTER TABLE audit.table_freshness
    ADD COLUMN IF NOT EXISTS last_run_id BIGINT;

ALTER TABLE audit.table_freshness
    ADD COLUMN IF NOT EXISTS last_run_status TEXT;

UPDATE audit.table_freshness
SET
    last_run_id = COALESCE(last_run_id, last_successful_run_id),
    last_run_status = COALESCE(
        last_run_status,
        CASE
            WHEN last_successful_run_id IS NOT NULL THEN 'success'
            ELSE NULL
        END
    );
