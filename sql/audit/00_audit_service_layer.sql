--
-- PostgreSQL database dump
--

\restrict 7U8h9R8eT1IeyuAnC5IlWupkh5S6rGMmcIxNxf4npwzOTgvqKDDGkvea7sVDrl6

-- Dumped from database version 16.14 (Ubuntu 16.14-0ubuntu0.24.04.1)
-- Dumped by pg_dump version 16.14 (Ubuntu 16.14-0ubuntu0.24.04.1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: dataset_runs; Type: TABLE; Schema: audit; Owner: -
--

CREATE TABLE audit.dataset_runs (
    dataset_run_id bigint NOT NULL,
    run_id bigint,
    client_id text NOT NULL,
    wb_account_id text NOT NULL,
    source_system text DEFAULT 'wb'::text NOT NULL,
    dataset_name text NOT NULL,
    source_file text DEFAULT 'unknown'::text NOT NULL,
    status text DEFAULT 'running'::text NOT NULL,
    started_at timestamp with time zone DEFAULT now() NOT NULL,
    finished_at timestamp with time zone,
    raw_payloads_loaded bigint DEFAULT 0,
    raw_records_loaded bigint DEFAULT 0,
    duplicate_payloads bigint DEFAULT 0,
    staging_rows bigint,
    cleaned_rows bigint,
    quarantined_rows bigint,
    bad_issue_count bigint,
    warning_issue_count bigint,
    error_message text
);


--
-- Name: dataset_runs_dataset_run_id_seq; Type: SEQUENCE; Schema: audit; Owner: -
--

CREATE SEQUENCE audit.dataset_runs_dataset_run_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: dataset_runs_dataset_run_id_seq; Type: SEQUENCE OWNED BY; Schema: audit; Owner: -
--

ALTER SEQUENCE audit.dataset_runs_dataset_run_id_seq OWNED BY audit.dataset_runs.dataset_run_id;


--
-- Name: load_runs; Type: TABLE; Schema: audit; Owner: -
--

CREATE TABLE audit.load_runs (
    run_id bigint NOT NULL,
    pipeline_name text NOT NULL,
    client_id text,
    wb_account_id text,
    run_mode text DEFAULT 'scheduled'::text NOT NULL,
    status text DEFAULT 'running'::text NOT NULL,
    started_at timestamp with time zone DEFAULT now() NOT NULL,
    finished_at timestamp with time zone,
    error_message text,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: load_runs_run_id_seq; Type: SEQUENCE; Schema: audit; Owner: -
--

CREATE SEQUENCE audit.load_runs_run_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: load_runs_run_id_seq; Type: SEQUENCE OWNED BY; Schema: audit; Owner: -
--

ALTER SEQUENCE audit.load_runs_run_id_seq OWNED BY audit.load_runs.run_id;


--
-- Name: table_freshness; Type: TABLE; Schema: audit; Owner: -
--

CREATE TABLE audit.table_freshness (
    schema_name text NOT NULL,
    table_name text NOT NULL,
    client_id text DEFAULT '__all__'::text NOT NULL,
    wb_account_id text DEFAULT '__all__'::text NOT NULL,
    last_refreshed_at timestamp with time zone,
    last_successful_run_id bigint,
    row_count bigint,
    max_data_date date,
    status text DEFAULT 'unknown'::text NOT NULL,
    checked_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: v_latest_loads; Type: VIEW; Schema: audit; Owner: -
--

CREATE VIEW audit.v_latest_loads AS
 WITH ranked AS (
         SELECT dr.dataset_run_id,
            dr.run_id,
            dr.client_id,
            dr.wb_account_id,
            dr.source_system,
            dr.dataset_name,
            dr.source_file,
            dr.status,
            dr.started_at,
            dr.finished_at,
            dr.raw_payloads_loaded,
            dr.raw_records_loaded,
            dr.duplicate_payloads,
            dr.staging_rows,
            dr.cleaned_rows,
            dr.quarantined_rows,
            dr.bad_issue_count,
            dr.warning_issue_count,
            dr.error_message,
            row_number() OVER (PARTITION BY dr.client_id, dr.wb_account_id, dr.source_system, dr.dataset_name ORDER BY dr.finished_at DESC NULLS LAST, dr.started_at DESC, dr.dataset_run_id DESC) AS rn
           FROM audit.dataset_runs dr
        )
 SELECT client_id,
    wb_account_id,
    source_system,
    dataset_name,
    status,
    started_at,
    finished_at AS last_loaded_at,
    raw_payloads_loaded,
    raw_records_loaded,
    duplicate_payloads,
    staging_rows,
    cleaned_rows,
    quarantined_rows,
    bad_issue_count,
    warning_issue_count,
    error_message
   FROM ranked
  WHERE (rn = 1);


--
-- Name: v_pipeline_failures; Type: VIEW; Schema: audit; Owner: -
--

CREATE VIEW audit.v_pipeline_failures AS
 SELECT 'load_run'::text AS failure_level,
    load_runs.run_id,
    NULL::bigint AS dataset_run_id,
    load_runs.pipeline_name,
    load_runs.client_id,
    load_runs.wb_account_id,
    NULL::text AS dataset_name,
    load_runs.status,
    load_runs.started_at,
    load_runs.finished_at,
    load_runs.error_message
   FROM audit.load_runs
  WHERE (load_runs.status = ANY (ARRAY['failed'::text, 'success_with_warnings'::text]))
UNION ALL
 SELECT 'dataset_run'::text AS failure_level,
    lr.run_id,
    dr.dataset_run_id,
    lr.pipeline_name,
    dr.client_id,
    dr.wb_account_id,
    dr.dataset_name,
    dr.status,
    dr.started_at,
    dr.finished_at,
    dr.error_message
   FROM (audit.dataset_runs dr
     LEFT JOIN audit.load_runs lr ON ((lr.run_id = dr.run_id)))
  WHERE (dr.status = ANY (ARRAY['failed'::text, 'success_with_warnings'::text]));


--
-- Name: v_quarantine_summary; Type: VIEW; Schema: audit; Owner: -
--

CREATE VIEW audit.v_quarantine_summary AS
 SELECT COALESCE((row_payload ->> 'client_id'::text), '__unknown__'::text) AS client_id,
    COALESCE((row_payload ->> 'wb_account_id'::text), '__unknown__'::text) AS wb_account_id,
    source_model,
    issue_code,
    COALESCE(issue_severity, 'warning'::text) AS issue_severity,
    count(*) AS issue_count,
    count(DISTINCT md5(COALESCE((row_payload)::text, ((source_model || ':'::text) || COALESCE(issue_code, ''::text))))) AS affected_rows,
    min(detected_at) AS first_seen_at,
    max(detected_at) AS last_seen_at
   FROM public.rq_cleaned_required_null_decisions
  GROUP BY COALESCE((row_payload ->> 'client_id'::text), '__unknown__'::text), COALESCE((row_payload ->> 'wb_account_id'::text), '__unknown__'::text), source_model, issue_code, COALESCE(issue_severity, 'warning'::text);


--
-- Name: v_schema_drift_summary; Type: VIEW; Schema: audit; Owner: -
--

CREATE VIEW audit.v_schema_drift_summary AS
 SELECT COALESCE(client_id, '__unknown__'::text) AS client_id,
    COALESCE(wb_account_id, '__unknown__'::text) AS wb_account_id,
    dataset_name,
    source_file,
    json_path,
    check_status AS issue_type,
    expected_type,
    actual_types,
    count(*) AS event_count,
    min(run_at) AS first_seen_at,
    max(run_at) AS last_seen_at
   FROM quarantine.json_schema_drift_events
  GROUP BY COALESCE(client_id, '__unknown__'::text), COALESCE(wb_account_id, '__unknown__'::text), dataset_name, source_file, json_path, check_status, expected_type, actual_types;


--
-- Name: v_table_freshness; Type: VIEW; Schema: audit; Owner: -
--

CREATE VIEW audit.v_table_freshness AS
 SELECT schema_name,
    table_name,
    client_id,
    wb_account_id,
    last_refreshed_at,
    last_successful_run_id,
    row_count,
    max_data_date,
    status,
    checked_at,
    (now() - checked_at) AS checked_age,
        CASE
            WHEN (last_refreshed_at IS NULL) THEN NULL::interval
            ELSE (now() - last_refreshed_at)
        END AS freshness_age
   FROM audit.table_freshness;


--
-- Name: dataset_runs dataset_run_id; Type: DEFAULT; Schema: audit; Owner: -
--

ALTER TABLE ONLY audit.dataset_runs ALTER COLUMN dataset_run_id SET DEFAULT nextval('audit.dataset_runs_dataset_run_id_seq'::regclass);


--
-- Name: load_runs run_id; Type: DEFAULT; Schema: audit; Owner: -
--

ALTER TABLE ONLY audit.load_runs ALTER COLUMN run_id SET DEFAULT nextval('audit.load_runs_run_id_seq'::regclass);


--
-- Name: dataset_runs dataset_runs_pkey; Type: CONSTRAINT; Schema: audit; Owner: -
--

ALTER TABLE ONLY audit.dataset_runs
    ADD CONSTRAINT dataset_runs_pkey PRIMARY KEY (dataset_run_id);


--
-- Name: load_runs load_runs_pkey; Type: CONSTRAINT; Schema: audit; Owner: -
--

ALTER TABLE ONLY audit.load_runs
    ADD CONSTRAINT load_runs_pkey PRIMARY KEY (run_id);


--
-- Name: table_freshness table_freshness_pkey; Type: CONSTRAINT; Schema: audit; Owner: -
--

ALTER TABLE ONLY audit.table_freshness
    ADD CONSTRAINT table_freshness_pkey PRIMARY KEY (schema_name, table_name, client_id, wb_account_id);


--
-- Name: ix_dataset_runs_dataset_finished; Type: INDEX; Schema: audit; Owner: -
--

CREATE INDEX ix_dataset_runs_dataset_finished ON audit.dataset_runs USING btree (client_id, wb_account_id, source_system, dataset_name, finished_at DESC);


--
-- Name: ix_load_runs_pipeline_started; Type: INDEX; Schema: audit; Owner: -
--

CREATE INDEX ix_load_runs_pipeline_started ON audit.load_runs USING btree (pipeline_name, started_at DESC);


--
-- Name: ix_load_runs_status_started; Type: INDEX; Schema: audit; Owner: -
--

CREATE INDEX ix_load_runs_status_started ON audit.load_runs USING btree (status, started_at DESC);


--
-- Name: ux_dataset_runs_run_dataset_source; Type: INDEX; Schema: audit; Owner: -
--

CREATE UNIQUE INDEX ux_dataset_runs_run_dataset_source ON audit.dataset_runs USING btree (run_id, client_id, wb_account_id, source_system, dataset_name, source_file);


--
-- Name: dataset_runs dataset_runs_run_id_fkey; Type: FK CONSTRAINT; Schema: audit; Owner: -
--

ALTER TABLE ONLY audit.dataset_runs
    ADD CONSTRAINT dataset_runs_run_id_fkey FOREIGN KEY (run_id) REFERENCES audit.load_runs(run_id);


--
-- PostgreSQL database dump complete
--

\unrestrict 7U8h9R8eT1IeyuAnC5IlWupkh5S6rGMmcIxNxf4npwzOTgvqKDDGkvea7sVDrl6

