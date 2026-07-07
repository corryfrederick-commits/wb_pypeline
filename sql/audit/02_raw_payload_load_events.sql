CREATE SCHEMA IF NOT EXISTS audit;

CREATE TABLE IF NOT EXISTS audit.raw_payload_load_events (
    event_id BIGSERIAL PRIMARY KEY,
    run_id BIGINT NOT NULL REFERENCES audit.load_runs(run_id) ON DELETE CASCADE,
    raw_payload_id BIGINT REFERENCES landing.raw_payloads(id) ON DELETE SET NULL,
    client_id TEXT NOT NULL,
    wb_account_id TEXT NOT NULL,
    source_system TEXT NOT NULL,
    dataset_name TEXT NOT NULL,
    source_file TEXT NOT NULL,
    source_url TEXT,
    file_hash TEXT NOT NULL,
    payload_type TEXT,
    top_level_count INTEGER,
    status TEXT NOT NULL,
    event_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    error_message TEXT,
    CONSTRAINT ck_raw_payload_load_events_status
        CHECK (status IN ('inserted', 'skipped_duplicate', 'failed'))
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_raw_payload_load_events_run_payload
    ON audit.raw_payload_load_events (
        run_id,
        client_id,
        wb_account_id,
        source_system,
        source_file,
        file_hash
    );

CREATE INDEX IF NOT EXISTS ix_raw_payload_load_events_run_id
    ON audit.raw_payload_load_events(run_id);

CREATE INDEX IF NOT EXISTS ix_raw_payload_load_events_dataset
    ON audit.raw_payload_load_events(
        client_id,
        wb_account_id,
        source_system,
        dataset_name,
        source_file
    );

ALTER TABLE audit.dataset_runs
    ADD COLUMN IF NOT EXISTS skipped_duplicate_payloads BIGINT;

ALTER TABLE audit.dataset_runs
    ADD COLUMN IF NOT EXISTS duplicate_payloads_in_raw BIGINT;
