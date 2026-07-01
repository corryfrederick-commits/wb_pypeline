{{ config(materialized='table', schema='quarantine', alias='rq_orders_issues', tags=['row_quality']) }}

with base as (
    select *
    from {{ ref('stg_orders_current') }}
),

issues as (
select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_orders_current__technical_raw_payload_id_missing'::text as rule_id,
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
    'stg_orders_current__technical_record_index_missing'::text as rule_id,
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
    'stg_orders_current__technical_dataset_name_missing'::text as rule_id,
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
    'stg_orders_current__technical_raw_record_missing'::text as rule_id,
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
    'stg_orders_current__duplicate_raw_payload_record_index'::text as rule_id,
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
    'stg_orders_current__unexpected_dataset_name'::text as rule_id,
    'technical_lineage'::text as rule_group,
    'accepted_dataset'::text as rule_type,
    'bad'::text as issue_severity,
    'dataset_name'::text as column_name,
    'unexpected_dataset_name'::text as issue_code,
    'dataset_name is not one of the datasets covered by stg_orders_current.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (dataset_name not in ('orders_fbs_new', 'orders_fbs_current', 'orders_fbs_archive', 'orders_dbs_new', 'orders_dbs_completed', 'orders_dbw_new', 'orders_dbw_completed', 'orders_pickup_new')) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_orders_current__order_identity_missing'::text as rule_id,
    'domain_required'::text as rule_group,
    'at_least_one_required'::text as rule_type,
    'bad'::text as issue_severity,
    'order_id,rid,srid,order_uid,order_code'::text as column_name,
    'order_identity_missing'::text as issue_code,
    'Order row has no stable order identity field.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        ((order_id is null) and (rid is null or nullif(rid::text, '') is null) and (srid is null or nullif(srid::text, '') is null) and (order_uid is null or nullif(order_uid::text, '') is null) and (order_code is null or nullif(order_code::text, '') is null)) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_orders_current__order_date_missing'::text as rule_id,
    'domain_required'::text as rule_group,
    'at_least_one_required'::text as rule_type,
    'warning'::text as issue_severity,
    'created_at,ddate,seller_date'::text as column_name,
    'order_date_missing'::text as issue_code,
    'Order row has no usable order/date field.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        ((created_at is null) and (ddate is null) and (seller_date is null)) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_orders_current__order_product_identity_missing'::text as rule_id,
    'domain_required'::text as rule_group,
    'at_least_one_required'::text as rule_type,
    'warning'::text as issue_severity,
    'nm_id,chrt_id,article,barcode'::text as column_name,
    'order_product_identity_missing'::text as issue_code,
    'Order row has no product identity field.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        ((nm_id is null) and (chrt_id is null) and (article is null or nullif(article::text, '') is null) and (barcode is null or nullif(barcode::text, '') is null)) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_orders_current__order_currency_missing'::text as rule_id,
    'domain_required'::text as rule_group,
    'at_least_one_required'::text as rule_type,
    'warning'::text as issue_severity,
    'currency_code,converted_currency_code'::text as column_name,
    'order_currency_missing'::text as issue_code,
    'Order row has price-like fields but no currency code.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        ((currency_code is null) and (converted_currency_code is null)) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_orders_current__order_fulfillment_context_missing'::text as rule_id,
    'domain_required'::text as rule_group,
    'at_least_one_required'::text as rule_type,
    'warning'::text as issue_severity,
    'warehouse_id,warehouse_address,office_id'::text as column_name,
    'order_fulfillment_context_missing'::text as issue_code,
    'Order row has no warehouse/office fulfillment context.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        ((warehouse_id is null) and (warehouse_address is null or nullif(warehouse_address::text, '') is null) and (office_id is null)) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_orders_current__price_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'bad'::text as issue_severity,
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
    'stg_orders_current__sale_price_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'bad'::text as issue_severity,
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

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_orders_current__final_price_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'bad'::text as issue_severity,
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
    'stg_orders_current__converted_price_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'bad'::text as issue_severity,
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
    'stg_orders_current__converted_final_price_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'bad'::text as issue_severity,
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
    'stg_orders_current__scan_price_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'bad'::text as issue_severity,
    'scan_price'::text as column_name,
    'scan_price_negative'::text as issue_code,
    'Numeric field `scan_price` is negative.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (scan_price < 0) as is_issue
    from base
) q
where q.is_issue is true
)

select *
from issues
