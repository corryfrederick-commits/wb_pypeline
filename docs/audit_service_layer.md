# Audit service layer

Operational SQL layer for checking pipeline runs, dataset loads, quarantine, schema drift, table freshness and failures.

Main tables:
- audit.load_runs
- audit.dataset_runs
- audit.table_freshness

Main views:
- audit.v_latest_loads
- audit.v_quarantine_summary
- audit.v_schema_drift_summary
- audit.v_table_freshness
- audit.v_pipeline_failures

Example checks:

```sql
select * from audit.v_latest_loads;
select * from audit.v_quarantine_summary;
select * from audit.v_schema_drift_summary;
select * from audit.v_table_freshness;
select * from audit.v_pipeline_failures;
```
