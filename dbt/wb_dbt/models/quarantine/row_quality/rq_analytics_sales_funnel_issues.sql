{{ config(materialized='table', schema='quarantine', alias='rq_analytics_sales_funnel_issues', tags=['row_quality']) }}

with base as (
    select *
    from {{ ref('stg_analytics_sales_funnel') }}
),

issues as (
select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_analytics_sales_funnel__technical_raw_payload_id_missing'::text as rule_id,
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
    'stg_analytics_sales_funnel__technical_record_index_missing'::text as rule_id,
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
    'stg_analytics_sales_funnel__technical_dataset_name_missing'::text as rule_id,
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
    'stg_analytics_sales_funnel__technical_raw_record_missing'::text as rule_id,
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
    'stg_analytics_sales_funnel__duplicate_raw_payload_record_index'::text as rule_id,
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
    'stg_analytics_sales_funnel__unexpected_dataset_name'::text as rule_id,
    'technical_lineage'::text as rule_group,
    'accepted_dataset'::text as rule_type,
    'bad'::text as issue_severity,
    'dataset_name'::text as column_name,
    'unexpected_dataset_name'::text as issue_code,
    'dataset_name is not `analytics_sales_funnel`.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (dataset_name <> 'analytics_sales_funnel') as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_analytics_sales_funnel__analytics_nm_id_missing'::text as rule_id,
    'domain_required'::text as rule_group,
    'at_least_one_required'::text as rule_type,
    'bad'::text as issue_severity,
    'product_nm_id'::text as column_name,
    'analytics_nm_id_missing'::text as issue_code,
    'Analytics row has no nm_id/product_nm_id.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        ((product_nm_id is null)) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_analytics_sales_funnel__product_stocks_balance_sum_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'bad'::text as issue_severity,
    'product_stocks_balance_sum'::text as column_name,
    'product_stocks_balance_sum_negative'::text as issue_code,
    'Numeric field `product_stocks_balance_sum` is negative.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (product_stocks_balance_sum < 0) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_analytics_sales_funnel__statistic_past_avg_price_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'bad'::text as issue_severity,
    'statistic_past_avg_price'::text as column_name,
    'statistic_past_avg_price_negative'::text as issue_code,
    'Numeric field `statistic_past_avg_price` is negative.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (statistic_past_avg_price < 0) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_analytics_sales_funnel__statistic_past_buyout_count_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'bad'::text as issue_severity,
    'statistic_past_buyout_count'::text as column_name,
    'statistic_past_buyout_count_negative'::text as issue_code,
    'Numeric field `statistic_past_buyout_count` is negative.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (statistic_past_buyout_count < 0) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_analytics_sales_funnel__statistic_past_buyout_sum_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'bad'::text as issue_severity,
    'statistic_past_buyout_sum'::text as column_name,
    'statistic_past_buyout_sum_negative'::text as issue_code,
    'Numeric field `statistic_past_buyout_sum` is negative.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (statistic_past_buyout_sum < 0) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_analytics_sales_funnel__statistic_past_cancel_count_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'bad'::text as issue_severity,
    'statistic_past_cancel_count'::text as column_name,
    'statistic_past_cancel_count_negative'::text as issue_code,
    'Numeric field `statistic_past_cancel_count` is negative.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (statistic_past_cancel_count < 0) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_analytics_sales_funnel__statistic_past_cancel_sum_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'bad'::text as issue_severity,
    'statistic_past_cancel_sum'::text as column_name,
    'statistic_past_cancel_sum_negative'::text as issue_code,
    'Numeric field `statistic_past_cancel_sum` is negative.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (statistic_past_cancel_sum < 0) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_analytics_sales_funnel__statistic_past_cart_count_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'bad'::text as issue_severity,
    'statistic_past_cart_count'::text as column_name,
    'statistic_past_cart_count_negative'::text as issue_code,
    'Numeric field `statistic_past_cart_count` is negative.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (statistic_past_cart_count < 0) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_analytics_sales_funnel__statistic_past_open_count_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'bad'::text as issue_severity,
    'statistic_past_open_count'::text as column_name,
    'statistic_past_open_count_negative'::text as issue_code,
    'Numeric field `statistic_past_open_count` is negative.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (statistic_past_open_count < 0) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_analytics_sales_funnel__statistic_past_order_count_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'bad'::text as issue_severity,
    'statistic_past_order_count'::text as column_name,
    'statistic_past_order_count_negative'::text as issue_code,
    'Numeric field `statistic_past_order_count` is negative.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (statistic_past_order_count < 0) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_analytics_sales_funnel__statistic_past_order_sum_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'bad'::text as issue_severity,
    'statistic_past_order_sum'::text as column_name,
    'statistic_past_order_sum_negative'::text as issue_code,
    'Numeric field `statistic_past_order_sum` is negative.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (statistic_past_order_sum < 0) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_analytics_sales_funnel__statistic_past_wb_club_avg_price_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'bad'::text as issue_severity,
    'statistic_past_wb_club_avg_price'::text as column_name,
    'statistic_past_wb_club_avg_price_negative'::text as issue_code,
    'Numeric field `statistic_past_wb_club_avg_price` is negative.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (statistic_past_wb_club_avg_price < 0) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_analytics_sales_funnel__statistic_past_wb_club_buyout_count_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'bad'::text as issue_severity,
    'statistic_past_wb_club_buyout_count'::text as column_name,
    'statistic_past_wb_club_buyout_count_negative'::text as issue_code,
    'Numeric field `statistic_past_wb_club_buyout_count` is negative.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (statistic_past_wb_club_buyout_count < 0) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_analytics_sales_funnel__statistic_past_wb_club_buyout_sum_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'bad'::text as issue_severity,
    'statistic_past_wb_club_buyout_sum'::text as column_name,
    'statistic_past_wb_club_buyout_sum_negative'::text as issue_code,
    'Numeric field `statistic_past_wb_club_buyout_sum` is negative.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (statistic_past_wb_club_buyout_sum < 0) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_analytics_sales_funnel__statistic_past_wb_club_cancel_count_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'bad'::text as issue_severity,
    'statistic_past_wb_club_cancel_count'::text as column_name,
    'statistic_past_wb_club_cancel_count_negative'::text as issue_code,
    'Numeric field `statistic_past_wb_club_cancel_count` is negative.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (statistic_past_wb_club_cancel_count < 0) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_analytics_sales_funnel__statistic_past_wb_club_cancel_sum_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'bad'::text as issue_severity,
    'statistic_past_wb_club_cancel_sum'::text as column_name,
    'statistic_past_wb_club_cancel_sum_negative'::text as issue_code,
    'Numeric field `statistic_past_wb_club_cancel_sum` is negative.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (statistic_past_wb_club_cancel_sum < 0) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_analytics_sales_funnel__statistic_past_wb_club_order_count_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'bad'::text as issue_severity,
    'statistic_past_wb_club_order_count'::text as column_name,
    'statistic_past_wb_club_order_count_negative'::text as issue_code,
    'Numeric field `statistic_past_wb_club_order_count` is negative.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (statistic_past_wb_club_order_count < 0) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_analytics_sales_funnel__statistic_past_wb_club_order_sum_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'bad'::text as issue_severity,
    'statistic_past_wb_club_order_sum'::text as column_name,
    'statistic_past_wb_club_order_sum_negative'::text as issue_code,
    'Numeric field `statistic_past_wb_club_order_sum` is negative.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (statistic_past_wb_club_order_sum < 0) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_analytics_sales_funnel__statistic_selected_avg_price_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'bad'::text as issue_severity,
    'statistic_selected_avg_price'::text as column_name,
    'statistic_selected_avg_price_negative'::text as issue_code,
    'Numeric field `statistic_selected_avg_price` is negative.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (statistic_selected_avg_price < 0) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_analytics_sales_funnel__statistic_selected_buyout_count_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'bad'::text as issue_severity,
    'statistic_selected_buyout_count'::text as column_name,
    'statistic_selected_buyout_count_negative'::text as issue_code,
    'Numeric field `statistic_selected_buyout_count` is negative.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (statistic_selected_buyout_count < 0) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_analytics_sales_funnel__statistic_selected_buyout_sum_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'bad'::text as issue_severity,
    'statistic_selected_buyout_sum'::text as column_name,
    'statistic_selected_buyout_sum_negative'::text as issue_code,
    'Numeric field `statistic_selected_buyout_sum` is negative.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (statistic_selected_buyout_sum < 0) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_analytics_sales_funnel__statistic_selected_cancel_count_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'bad'::text as issue_severity,
    'statistic_selected_cancel_count'::text as column_name,
    'statistic_selected_cancel_count_negative'::text as issue_code,
    'Numeric field `statistic_selected_cancel_count` is negative.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (statistic_selected_cancel_count < 0) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_analytics_sales_funnel__statistic_selected_cancel_sum_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'bad'::text as issue_severity,
    'statistic_selected_cancel_sum'::text as column_name,
    'statistic_selected_cancel_sum_negative'::text as issue_code,
    'Numeric field `statistic_selected_cancel_sum` is negative.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (statistic_selected_cancel_sum < 0) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_analytics_sales_funnel__statistic_selected_cart_count_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'bad'::text as issue_severity,
    'statistic_selected_cart_count'::text as column_name,
    'statistic_selected_cart_count_negative'::text as issue_code,
    'Numeric field `statistic_selected_cart_count` is negative.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (statistic_selected_cart_count < 0) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_analytics_sales_funnel__statistic_selected_open_count_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'bad'::text as issue_severity,
    'statistic_selected_open_count'::text as column_name,
    'statistic_selected_open_count_negative'::text as issue_code,
    'Numeric field `statistic_selected_open_count` is negative.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (statistic_selected_open_count < 0) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_analytics_sales_funnel__statistic_selected_order_count_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'bad'::text as issue_severity,
    'statistic_selected_order_count'::text as column_name,
    'statistic_selected_order_count_negative'::text as issue_code,
    'Numeric field `statistic_selected_order_count` is negative.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (statistic_selected_order_count < 0) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_analytics_sales_funnel__statistic_selected_order_sum_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'bad'::text as issue_severity,
    'statistic_selected_order_sum'::text as column_name,
    'statistic_selected_order_sum_negative'::text as issue_code,
    'Numeric field `statistic_selected_order_sum` is negative.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (statistic_selected_order_sum < 0) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_analytics_sales_funnel__statistic_selected_wb_club_avg_price_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'bad'::text as issue_severity,
    'statistic_selected_wb_club_avg_price'::text as column_name,
    'statistic_selected_wb_club_avg_price_negative'::text as issue_code,
    'Numeric field `statistic_selected_wb_club_avg_price` is negative.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (statistic_selected_wb_club_avg_price < 0) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_analytics_sales_funnel__statistic_selected_wb_club_buyout_count_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'bad'::text as issue_severity,
    'statistic_selected_wb_club_buyout_count'::text as column_name,
    'statistic_selected_wb_club_buyout_count_negative'::text as issue_code,
    'Numeric field `statistic_selected_wb_club_buyout_count` is negative.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (statistic_selected_wb_club_buyout_count < 0) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_analytics_sales_funnel__statistic_selected_wb_club_buyout_sum_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'bad'::text as issue_severity,
    'statistic_selected_wb_club_buyout_sum'::text as column_name,
    'statistic_selected_wb_club_buyout_sum_negative'::text as issue_code,
    'Numeric field `statistic_selected_wb_club_buyout_sum` is negative.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (statistic_selected_wb_club_buyout_sum < 0) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_analytics_sales_funnel__statistic_selected_wb_club_cancel_count_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'bad'::text as issue_severity,
    'statistic_selected_wb_club_cancel_count'::text as column_name,
    'statistic_selected_wb_club_cancel_count_negative'::text as issue_code,
    'Numeric field `statistic_selected_wb_club_cancel_count` is negative.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (statistic_selected_wb_club_cancel_count < 0) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_analytics_sales_funnel__statistic_selected_wb_club_cancel_sum_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'bad'::text as issue_severity,
    'statistic_selected_wb_club_cancel_sum'::text as column_name,
    'statistic_selected_wb_club_cancel_sum_negative'::text as issue_code,
    'Numeric field `statistic_selected_wb_club_cancel_sum` is negative.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (statistic_selected_wb_club_cancel_sum < 0) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_analytics_sales_funnel__statistic_selected_wb_club_order_count_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'bad'::text as issue_severity,
    'statistic_selected_wb_club_order_count'::text as column_name,
    'statistic_selected_wb_club_order_count_negative'::text as issue_code,
    'Numeric field `statistic_selected_wb_club_order_count` is negative.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (statistic_selected_wb_club_order_count < 0) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_analytics_sales_funnel__statistic_selected_wb_club_order_sum_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'bad'::text as issue_severity,
    'statistic_selected_wb_club_order_sum'::text as column_name,
    'statistic_selected_wb_club_order_sum_negative'::text as issue_code,
    'Numeric field `statistic_selected_wb_club_order_sum` is negative.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (statistic_selected_wb_club_order_sum < 0) as is_issue
    from base
) q
where q.is_issue is true
)

select *
from issues
