ALTER TABLE audit.load_runs
    DROP CONSTRAINT IF EXISTS ck_load_runs_status;

ALTER TABLE audit.load_runs
    ADD CONSTRAINT ck_load_runs_status
    CHECK (status IN ('running', 'success', 'partial_success', 'failed'));
