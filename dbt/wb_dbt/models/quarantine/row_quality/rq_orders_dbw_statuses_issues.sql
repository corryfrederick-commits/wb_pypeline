{{ config(materialized='table', schema='quarantine', alias='rq_orders_dbw_statuses_issues', tags=['row_quality']) }}

with base as (
    select *
    from {{ ref('stg_orders_dbw_statuses') }}
),

issues as (
select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_orders_dbw_statuses__technical_raw_payload_id_missing'::text as rule_id,
    'technical_lineage'::text as rule_group,
    'required'::text as rule_type,
    'bad'::text as issue_severity,
    'raw_payload_id'::text as column_name,
    'technical_raw_payload_id_missing'::text as issue_code,
    'Technical lineage field `raw_payload_id` is missing.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (raw_payload_id is null) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_orders_dbw_statuses__technical_record_index_missing'::text as rule_id,
    'technical_lineage'::text as rule_group,
    'required'::text as rule_type,
    'bad'::text as issue_severity,
    'record_index'::text as column_name,
    'technical_record_index_missing'::text as issue_code,
    'Technical lineage field `record_index` is missing.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (record_index is null) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_orders_dbw_statuses__technical_dataset_name_missing'::text as rule_id,
    'technical_lineage'::text as rule_group,
    'required'::text as rule_type,
    'bad'::text as issue_severity,
    'dataset_name'::text as column_name,
    'technical_dataset_name_missing'::text as issue_code,
    'Technical lineage field `dataset_name` is missing.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (dataset_name is null or nullif(dataset_name::text, '') is null) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_orders_dbw_statuses__technical_raw_record_missing'::text as rule_id,
    'technical_lineage'::text as rule_group,
    'required'::text as rule_type,
    'bad'::text as issue_severity,
    'raw_record'::text as column_name,
    'technical_raw_record_missing'::text as issue_code,
    'Technical lineage field `raw_record` is missing.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (raw_record is null or nullif(raw_record::text, '') is null) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_orders_dbw_statuses__duplicate_raw_payload_record_index'::text as rule_id,
    'technical_lineage'::text as rule_group,
    'unique_combination'::text as rule_type,
    'bad'::text as issue_severity,
    'raw_payload_id,record_index'::text as column_name,
    'duplicate_raw_payload_record_index'::text as issue_code,
    'Duplicate row identity by raw_payload_id + record_index.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (count(*) over (partition by raw_payload_id, record_index) > 1) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_orders_dbw_statuses__unexpected_dataset_name'::text as rule_id,
    'technical_lineage'::text as rule_group,
    'accepted_dataset'::text as rule_type,
    'bad'::text as issue_severity,
    'dataset_name'::text as column_name,
    'unexpected_dataset_name'::text as issue_code,
    'dataset_name is not `orders_dbw_statuses`.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (dataset_name <> 'orders_dbw_statuses') as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_orders_dbw_statuses__order_status_identity_missing'::text as rule_id,
    'domain_required'::text as rule_group,
    'at_least_one_required'::text as rule_type,
    'bad'::text as issue_severity,
    'order_id,rid,srid'::text as column_name,
    'order_status_identity_missing'::text as issue_code,
    'Order status row has no order identity field.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        ((order_id is null) and (rid is null or nullif(rid::text, '') is null) and (srid is null or nullif(srid::text, '') is null)) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_orders_dbw_statuses__wb_status_missing'::text as rule_id,
    'domain_required'::text as rule_group,
    'required'::text as rule_type,
    'warning'::text as issue_severity,
    'wb_status'::text as column_name,
    'wb_status_missing'::text as issue_code,
    'Required field `wb_status` is missing.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (wb_status is null or nullif(wb_status::text, '') is null) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_orders_dbw_statuses__supplier_status_missing'::text as rule_id,
    'domain_required'::text as rule_group,
    'required'::text as rule_type,
    'warning'::text as issue_severity,
    'supplier_status'::text as column_name,
    'supplier_status_missing'::text as issue_code,
    'Required field `supplier_status` is missing.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (supplier_status is null or nullif(supplier_status::text, '') is null) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_orders_dbw_statuses__status_missing'::text as rule_id,
    'domain_required'::text as rule_group,
    'required'::text as rule_type,
    'warning'::text as issue_severity,
    'status'::text as column_name,
    'status_missing'::text as issue_code,
    'Required field `status` is missing.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (status is null or nullif(status::text, '') is null) as is_issue
    from base
) q
where q.is_issue is true
)

select *
from issues
