{{ config(materialized='table', schema='quarantine', alias='rq_promotion_fullstats_issues', tags=['row_quality']) }}

with base as (
    select *
    from {{ ref('stg_promotion_fullstats') }}
),

issues as (
select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_promotion_fullstats__technical_raw_payload_id_missing'::text as rule_id,
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
    'stg_promotion_fullstats__technical_record_index_missing'::text as rule_id,
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
    'stg_promotion_fullstats__technical_dataset_name_missing'::text as rule_id,
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
    'stg_promotion_fullstats__technical_raw_record_missing'::text as rule_id,
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
    'stg_promotion_fullstats__duplicate_raw_payload_record_index'::text as rule_id,
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
    'stg_promotion_fullstats__unexpected_dataset_name'::text as rule_id,
    'technical_lineage'::text as rule_group,
    'accepted_dataset'::text as rule_type,
    'bad'::text as issue_severity,
    'dataset_name'::text as column_name,
    'unexpected_dataset_name'::text as issue_code,
    'dataset_name is not `promotion_fullstats`.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (dataset_name <> 'promotion_fullstats') as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_promotion_fullstats__root_campaign_id_missing'::text as rule_id,
    'domain_required'::text as rule_group,
    'required'::text as rule_type,
    'bad'::text as issue_severity,
    'root_campaign_id'::text as column_name,
    'root_campaign_id_missing'::text as issue_code,
    'Required field `root_campaign_id` is missing.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (root_campaign_id is null) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_promotion_fullstats__root_advert_id_missing'::text as rule_id,
    'domain_required'::text as rule_group,
    'required'::text as rule_type,
    'bad'::text as issue_severity,
    'root_advert_id'::text as column_name,
    'root_advert_id_missing'::text as issue_code,
    'Required field `root_advert_id` is missing.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (root_advert_id is null) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_promotion_fullstats__nm_nm_id_missing'::text as rule_id,
    'domain_required'::text as rule_group,
    'required'::text as rule_type,
    'bad'::text as issue_severity,
    'nm_nm_id'::text as column_name,
    'nm_nm_id_missing'::text as issue_code,
    'Required field `nm_nm_id` is missing.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (nm_nm_id is null) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_promotion_fullstats__root_atbs_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'bad'::text as issue_severity,
    'root_atbs'::text as column_name,
    'root_atbs_negative'::text as issue_code,
    'Numeric field `root_atbs` is negative.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (root_atbs < 0) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_promotion_fullstats__root_canceled_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'bad'::text as issue_severity,
    'root_canceled'::text as column_name,
    'root_canceled_negative'::text as issue_code,
    'Numeric field `root_canceled` is negative.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (root_canceled < 0) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_promotion_fullstats__root_clicks_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'bad'::text as issue_severity,
    'root_clicks'::text as column_name,
    'root_clicks_negative'::text as issue_code,
    'Numeric field `root_clicks` is negative.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (root_clicks < 0) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_promotion_fullstats__root_orders_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'bad'::text as issue_severity,
    'root_orders'::text as column_name,
    'root_orders_negative'::text as issue_code,
    'Numeric field `root_orders` is negative.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (root_orders < 0) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_promotion_fullstats__root_shks_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'bad'::text as issue_severity,
    'root_shks'::text as column_name,
    'root_shks_negative'::text as issue_code,
    'Numeric field `root_shks` is negative.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (root_shks < 0) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_promotion_fullstats__root_sum_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'bad'::text as issue_severity,
    'root_sum'::text as column_name,
    'root_sum_negative'::text as issue_code,
    'Numeric field `root_sum` is negative.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (root_sum < 0) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_promotion_fullstats__root_sum_price_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'bad'::text as issue_severity,
    'root_sum_price'::text as column_name,
    'root_sum_price_negative'::text as issue_code,
    'Numeric field `root_sum_price` is negative.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (root_sum_price < 0) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_promotion_fullstats__root_views_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'bad'::text as issue_severity,
    'root_views'::text as column_name,
    'root_views_negative'::text as issue_code,
    'Numeric field `root_views` is negative.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (root_views < 0) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_promotion_fullstats__day_atbs_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'bad'::text as issue_severity,
    'day_atbs'::text as column_name,
    'day_atbs_negative'::text as issue_code,
    'Numeric field `day_atbs` is negative.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (day_atbs < 0) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_promotion_fullstats__day_canceled_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'bad'::text as issue_severity,
    'day_canceled'::text as column_name,
    'day_canceled_negative'::text as issue_code,
    'Numeric field `day_canceled` is negative.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (day_canceled < 0) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_promotion_fullstats__day_clicks_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'bad'::text as issue_severity,
    'day_clicks'::text as column_name,
    'day_clicks_negative'::text as issue_code,
    'Numeric field `day_clicks` is negative.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (day_clicks < 0) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_promotion_fullstats__day_orders_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'bad'::text as issue_severity,
    'day_orders'::text as column_name,
    'day_orders_negative'::text as issue_code,
    'Numeric field `day_orders` is negative.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (day_orders < 0) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_promotion_fullstats__day_shks_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'bad'::text as issue_severity,
    'day_shks'::text as column_name,
    'day_shks_negative'::text as issue_code,
    'Numeric field `day_shks` is negative.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (day_shks < 0) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_promotion_fullstats__day_sum_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'bad'::text as issue_severity,
    'day_sum'::text as column_name,
    'day_sum_negative'::text as issue_code,
    'Numeric field `day_sum` is negative.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (day_sum < 0) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_promotion_fullstats__day_sum_price_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'bad'::text as issue_severity,
    'day_sum_price'::text as column_name,
    'day_sum_price_negative'::text as issue_code,
    'Numeric field `day_sum_price` is negative.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (day_sum_price < 0) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_promotion_fullstats__day_views_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'bad'::text as issue_severity,
    'day_views'::text as column_name,
    'day_views_negative'::text as issue_code,
    'Numeric field `day_views` is negative.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (day_views < 0) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_promotion_fullstats__app_atbs_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'bad'::text as issue_severity,
    'app_atbs'::text as column_name,
    'app_atbs_negative'::text as issue_code,
    'Numeric field `app_atbs` is negative.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (app_atbs < 0) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_promotion_fullstats__app_canceled_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'bad'::text as issue_severity,
    'app_canceled'::text as column_name,
    'app_canceled_negative'::text as issue_code,
    'Numeric field `app_canceled` is negative.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (app_canceled < 0) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_promotion_fullstats__app_clicks_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'bad'::text as issue_severity,
    'app_clicks'::text as column_name,
    'app_clicks_negative'::text as issue_code,
    'Numeric field `app_clicks` is negative.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (app_clicks < 0) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_promotion_fullstats__app_orders_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'bad'::text as issue_severity,
    'app_orders'::text as column_name,
    'app_orders_negative'::text as issue_code,
    'Numeric field `app_orders` is negative.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (app_orders < 0) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_promotion_fullstats__app_shks_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'bad'::text as issue_severity,
    'app_shks'::text as column_name,
    'app_shks_negative'::text as issue_code,
    'Numeric field `app_shks` is negative.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (app_shks < 0) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_promotion_fullstats__app_sum_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'bad'::text as issue_severity,
    'app_sum'::text as column_name,
    'app_sum_negative'::text as issue_code,
    'Numeric field `app_sum` is negative.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (app_sum < 0) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_promotion_fullstats__app_sum_price_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'bad'::text as issue_severity,
    'app_sum_price'::text as column_name,
    'app_sum_price_negative'::text as issue_code,
    'Numeric field `app_sum_price` is negative.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (app_sum_price < 0) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_promotion_fullstats__app_views_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'bad'::text as issue_severity,
    'app_views'::text as column_name,
    'app_views_negative'::text as issue_code,
    'Numeric field `app_views` is negative.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (app_views < 0) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_promotion_fullstats__nm_atbs_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'bad'::text as issue_severity,
    'nm_atbs'::text as column_name,
    'nm_atbs_negative'::text as issue_code,
    'Numeric field `nm_atbs` is negative.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (nm_atbs < 0) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_promotion_fullstats__nm_canceled_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'bad'::text as issue_severity,
    'nm_canceled'::text as column_name,
    'nm_canceled_negative'::text as issue_code,
    'Numeric field `nm_canceled` is negative.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (nm_canceled < 0) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_promotion_fullstats__nm_clicks_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'bad'::text as issue_severity,
    'nm_clicks'::text as column_name,
    'nm_clicks_negative'::text as issue_code,
    'Numeric field `nm_clicks` is negative.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (nm_clicks < 0) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_promotion_fullstats__nm_orders_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'bad'::text as issue_severity,
    'nm_orders'::text as column_name,
    'nm_orders_negative'::text as issue_code,
    'Numeric field `nm_orders` is negative.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (nm_orders < 0) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_promotion_fullstats__nm_shks_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'bad'::text as issue_severity,
    'nm_shks'::text as column_name,
    'nm_shks_negative'::text as issue_code,
    'Numeric field `nm_shks` is negative.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (nm_shks < 0) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_promotion_fullstats__nm_sum_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'bad'::text as issue_severity,
    'nm_sum'::text as column_name,
    'nm_sum_negative'::text as issue_code,
    'Numeric field `nm_sum` is negative.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (nm_sum < 0) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_promotion_fullstats__nm_sum_price_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'bad'::text as issue_severity,
    'nm_sum_price'::text as column_name,
    'nm_sum_price_negative'::text as issue_code,
    'Numeric field `nm_sum_price` is negative.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (nm_sum_price < 0) as is_issue
    from base
) q
where q.is_issue is true

union all

select
    q.raw_payload_id,
    q.record_index,
    q.dataset_name,
    'stg_promotion_fullstats__nm_views_negative'::text as rule_id,
    'numeric_quality'::text as rule_group,
    'non_negative'::text as rule_type,
    'bad'::text as issue_severity,
    'nm_views'::text as column_name,
    'nm_views_negative'::text as issue_code,
    'Numeric field `nm_views` is negative.'::text as issue_message,
    current_timestamp as detected_at
from (
    select
        base.*,
        (nm_views < 0) as is_issue
    from base
) q
where q.is_issue is true
)

select *
from issues
