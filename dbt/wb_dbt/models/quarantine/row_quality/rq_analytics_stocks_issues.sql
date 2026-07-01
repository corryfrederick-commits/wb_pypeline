{{ config(materialized='table', schema='quarantine', alias='rq_analytics_stocks_issues', tags=['row_quality']) }}

with base as (
    select *
    from {{ ref('stg_analytics_stocks') }}
),

issues as (
select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_analytics_stocks__technical_raw_payload_id_missing'::text as rule_id,
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
    'stg_analytics_stocks__technical_record_index_missing'::text as rule_id,
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
    'stg_analytics_stocks__technical_dataset_name_missing'::text as rule_id,
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
    'stg_analytics_stocks__technical_raw_record_missing'::text as rule_id,
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
    'stg_analytics_stocks__duplicate_raw_payload_record_index'::text as rule_id,
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
    'stg_analytics_stocks__unexpected_dataset_name'::text as rule_id,
    'technical_lineage'::text as rule_group,
    'accepted_dataset'::text as rule_type,
    'bad'::text as issue_severity,
    'dataset_name'::text as column_name,
    'unexpected_dataset_name'::text as issue_code,
    'dataset_name is not `analytics_stocks`.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (dataset_name <> 'analytics_stocks') as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_analytics_stocks__analytics_nm_id_missing'::text as rule_id,
    'domain_required'::text as rule_group,
    'at_least_one_required'::text as rule_type,
    'bad'::text as issue_severity,
    'nm_id'::text as column_name,
    'analytics_nm_id_missing'::text as issue_code,
    'Analytics row has no nm_id/product_nm_id.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        ((nm_id is null)) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_analytics_stocks__quantity_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'bad'::text as issue_severity,
    'quantity'::text as column_name,
    'quantity_negative'::text as issue_code,
    'Numeric field `quantity` is negative.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (quantity < 0) as is_issue
    from base
) q
where q.is_issue is true
)

select *
from issues
