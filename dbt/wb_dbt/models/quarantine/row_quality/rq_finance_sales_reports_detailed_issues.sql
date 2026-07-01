{{ config(materialized='table', schema='quarantine', alias='rq_finance_sales_reports_detailed_issues', tags=['row_quality']) }}

with base as (
    select *
    from {{ ref('stg_finance_sales_reports_detailed') }}
),

issues as (
select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_finance_sales_reports_detailed__technical_raw_payload_id_missing'::text as rule_id,
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
    'stg_finance_sales_reports_detailed__technical_record_index_missing'::text as rule_id,
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
    'stg_finance_sales_reports_detailed__technical_dataset_name_missing'::text as rule_id,
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
    'stg_finance_sales_reports_detailed__technical_raw_record_missing'::text as rule_id,
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
    'stg_finance_sales_reports_detailed__duplicate_raw_payload_record_index'::text as rule_id,
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
    'stg_finance_sales_reports_detailed__unexpected_dataset_name'::text as rule_id,
    'technical_lineage'::text as rule_group,
    'accepted_dataset'::text as rule_type,
    'bad'::text as issue_severity,
    'dataset_name'::text as column_name,
    'unexpected_dataset_name'::text as issue_code,
    'dataset_name is not `finance_sales_reports_detailed`.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (dataset_name <> 'finance_sales_reports_detailed') as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_finance_sales_reports_detailed__operation_date_missing'::text as rule_id,
    'domain_required'::text as rule_group,
    'required'::text as rule_type,
    'bad'::text as issue_severity,
    'operation_date'::text as column_name,
    'operation_date_missing'::text as issue_code,
    'Required field `operation_date` is missing.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (operation_date is null) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_finance_sales_reports_detailed__operation_business_identity_missing'::text as rule_id,
    'domain_required'::text as rule_group,
    'at_least_one_required'::text as rule_type,
    'warning'::text as issue_severity,
    'nm_id,order_id,rid,srid,rrd_id'::text as column_name,
    'operation_business_identity_missing'::text as issue_code,
    'Operation/report row has no product/order/report identity field.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        ((nm_id is null) and (order_id is null) and (rid is null or nullif(rid::text, '') is null) and (srid is null or nullif(srid::text, '') is null) and (rrd_id is null)) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_finance_sales_reports_detailed__converted_final_price_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'warning'::text as issue_severity,
    'converted_final_price'::text as column_name,
    'converted_final_price_negative'::text as issue_code,
    'Numeric field `converted_final_price` is negative.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (converted_final_price < 0) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_finance_sales_reports_detailed__converted_price_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'warning'::text as issue_severity,
    'converted_price'::text as column_name,
    'converted_price_negative'::text as issue_code,
    'Numeric field `converted_price` is negative.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (converted_price < 0) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_finance_sales_reports_detailed__delivery_amount_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'warning'::text as issue_severity,
    'delivery_amount'::text as column_name,
    'delivery_amount_negative'::text as issue_code,
    'Numeric field `delivery_amount` is negative.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (delivery_amount < 0) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_finance_sales_reports_detailed__final_price_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'warning'::text as issue_severity,
    'final_price'::text as column_name,
    'final_price_negative'::text as issue_code,
    'Numeric field `final_price` is negative.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (final_price < 0) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_finance_sales_reports_detailed__price_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'warning'::text as issue_severity,
    'price'::text as column_name,
    'price_negative'::text as issue_code,
    'Numeric field `price` is negative.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (price < 0) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_finance_sales_reports_detailed__quantity_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'warning'::text as issue_severity,
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

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_finance_sales_reports_detailed__return_amount_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'warning'::text as issue_severity,
    'return_amount'::text as column_name,
    'return_amount_negative'::text as issue_code,
    'Numeric field `return_amount` is negative.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (return_amount < 0) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_finance_sales_reports_detailed__sale_price_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'warning'::text as issue_severity,
    'sale_price'::text as column_name,
    'sale_price_negative'::text as issue_code,
    'Numeric field `sale_price` is negative.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (sale_price < 0) as is_issue
    from base
) q
where q.is_issue is true
)

select *
from issues
