-- generated canonical dbt row-quality decision layer
-- Replaces manual SQL row quarantine views such as sql/quarantine/01_quarantine_orders_rows.sql.

{{ config(materialized='table') }}

with issues as (

select
    source_model::text as source_model,
    column_name::text as column_name,
    issue_code::text as issue_code,
    issue_message::text as issue_message,
    row_payload::jsonb as row_payload,
    detected_at::timestamptz as detected_at
from {{ ref('rq_analytics_sales_funnel_cleaned_required_null_issues') }}

union all

select
    source_model::text as source_model,
    column_name::text as column_name,
    issue_code::text as issue_code,
    issue_message::text as issue_message,
    row_payload::jsonb as row_payload,
    detected_at::timestamptz as detected_at
from {{ ref('rq_analytics_stocks_cleaned_required_null_issues') }}

union all

select
    source_model::text as source_model,
    column_name::text as column_name,
    issue_code::text as issue_code,
    issue_message::text as issue_message,
    row_payload::jsonb as row_payload,
    detected_at::timestamptz as detected_at
from {{ ref('rq_communications_chats_cleaned_required_null_issues') }}

union all

select
    source_model::text as source_model,
    column_name::text as column_name,
    issue_code::text as issue_code,
    issue_message::text as issue_message,
    row_payload::jsonb as row_payload,
    detected_at::timestamptz as detected_at
from {{ ref('rq_communications_feedbacks_cleaned_required_null_issues') }}

union all

select
    source_model::text as source_model,
    column_name::text as column_name,
    issue_code::text as issue_code,
    issue_message::text as issue_message,
    row_payload::jsonb as row_payload,
    detected_at::timestamptz as detected_at
from {{ ref('rq_fbw_supplies_cleaned_required_null_issues') }}

union all

select
    source_model::text as source_model,
    column_name::text as column_name,
    issue_code::text as issue_code,
    issue_message::text as issue_message,
    row_payload::jsonb as row_payload,
    detected_at::timestamptz as detected_at
from {{ ref('rq_finance_balance_cleaned_required_null_issues') }}

union all

select
    source_model::text as source_model,
    column_name::text as column_name,
    issue_code::text as issue_code,
    issue_message::text as issue_message,
    row_payload::jsonb as row_payload,
    detected_at::timestamptz as detected_at
from {{ ref('rq_finance_sales_reports_cleaned_required_null_issues') }}

union all

select
    source_model::text as source_model,
    column_name::text as column_name,
    issue_code::text as issue_code,
    issue_message::text as issue_message,
    row_payload::jsonb as row_payload,
    detected_at::timestamptz as detected_at
from {{ ref('rq_finance_sales_reports_detailed_cleaned_required_null_issues') }}

union all

select
    source_model::text as source_model,
    column_name::text as column_name,
    issue_code::text as issue_code,
    issue_message::text as issue_message,
    row_payload::jsonb as row_payload,
    detected_at::timestamptz as detected_at
from {{ ref('rq_general_seller_info_cleaned_required_null_issues') }}

union all

select
    source_model::text as source_model,
    column_name::text as column_name,
    issue_code::text as issue_code,
    issue_message::text as issue_message,
    row_payload::jsonb as row_payload,
    detected_at::timestamptz as detected_at
from {{ ref('rq_items_cards_cleaned_required_null_issues') }}

union all

select
    source_model::text as source_model,
    column_name::text as column_name,
    issue_code::text as issue_code,
    issue_message::text as issue_message,
    row_payload::jsonb as row_payload,
    detected_at::timestamptz as detected_at
from {{ ref('rq_items_stocks_cleaned_required_null_issues') }}

union all

select
    source_model::text as source_model,
    column_name::text as column_name,
    issue_code::text as issue_code,
    issue_message::text as issue_message,
    row_payload::jsonb as row_payload,
    detected_at::timestamptz as detected_at
from {{ ref('rq_items_warehouses_cleaned_required_null_issues') }}

union all

select
    source_model::text as source_model,
    column_name::text as column_name,
    issue_code::text as issue_code,
    issue_message::text as issue_message,
    row_payload::jsonb as row_payload,
    detected_at::timestamptz as detected_at
from {{ ref('rq_orders_cleaned_required_null_issues') }}

union all

select
    source_model::text as source_model,
    column_name::text as column_name,
    issue_code::text as issue_code,
    issue_message::text as issue_message,
    row_payload::jsonb as row_payload,
    detected_at::timestamptz as detected_at
from {{ ref('rq_orders_dbs_statuses_cleaned_required_null_issues') }}

union all

select
    source_model::text as source_model,
    column_name::text as column_name,
    issue_code::text as issue_code,
    issue_message::text as issue_message,
    row_payload::jsonb as row_payload,
    detected_at::timestamptz as detected_at
from {{ ref('rq_orders_dbw_statuses_cleaned_required_null_issues') }}

union all

select
    source_model::text as source_model,
    column_name::text as column_name,
    issue_code::text as issue_code,
    issue_message::text as issue_message,
    row_payload::jsonb as row_payload,
    detected_at::timestamptz as detected_at
from {{ ref('rq_orders_fbs_statuses_cleaned_required_null_issues') }}

union all

select
    source_model::text as source_model,
    column_name::text as column_name,
    issue_code::text as issue_code,
    issue_message::text as issue_message,
    row_payload::jsonb as row_payload,
    detected_at::timestamptz as detected_at
from {{ ref('rq_orders_pickup_statuses_cleaned_required_null_issues') }}

union all

select
    source_model::text as source_model,
    column_name::text as column_name,
    issue_code::text as issue_code,
    issue_message::text as issue_message,
    row_payload::jsonb as row_payload,
    detected_at::timestamptz as detected_at
from {{ ref('rq_promotion_campaigns_cleaned_required_null_issues') }}

union all

select
    source_model::text as source_model,
    column_name::text as column_name,
    issue_code::text as issue_code,
    issue_message::text as issue_message,
    row_payload::jsonb as row_payload,
    detected_at::timestamptz as detected_at
from {{ ref('rq_promotion_fullstats_cleaned_required_null_issues') }}

union all

select
    source_model::text as source_model,
    column_name::text as column_name,
    issue_code::text as issue_code,
    issue_message::text as issue_message,
    row_payload::jsonb as row_payload,
    detected_at::timestamptz as detected_at
from {{ ref('rq_report_orders_cleaned_required_null_issues') }}

union all

select
    source_model::text as source_model,
    column_name::text as column_name,
    issue_code::text as issue_code,
    issue_message::text as issue_message,
    row_payload::jsonb as row_payload,
    detected_at::timestamptz as detected_at
from {{ ref('rq_report_sales_cleaned_required_null_issues') }}

union all

select
    source_model::text as source_model,
    column_name::text as column_name,
    issue_code::text as issue_code,
    issue_message::text as issue_message,
    row_payload::jsonb as row_payload,
    detected_at::timestamptz as detected_at
from {{ ref('rq_tariffs_acceptance_cleaned_required_null_issues') }}

union all

select
    source_model::text as source_model,
    column_name::text as column_name,
    issue_code::text as issue_code,
    issue_message::text as issue_message,
    row_payload::jsonb as row_payload,
    detected_at::timestamptz as detected_at
from {{ ref('rq_tariffs_box_cleaned_required_null_issues') }}

union all

select
    source_model::text as source_model,
    column_name::text as column_name,
    issue_code::text as issue_code,
    issue_message::text as issue_message,
    row_payload::jsonb as row_payload,
    detected_at::timestamptz as detected_at
from {{ ref('rq_tariffs_commission_cleaned_required_null_issues') }}

),

classified as (
    select
        source_model,
        column_name,
        issue_code,
        issue_message,
        row_payload,
        detected_at,

        case
            when column_name in (
                'client_id',
                'wb_account_id'
            )
                then 'bad'

            when column_name like '%\_key' escape '\'
              or column_name like '%\_id' escape '\'
              or column_name in (
                    'order_id',
                    'sale_id',
                    'product_id',
                    'product_variant_id',
                    'warehouse_id',
                    'warehouse_key',
                    'nm_id',
                    'chrt_id',
                    'barcode'
                )
                then 'bad'

            when column_name in (
                'order_date',
                'sale_date',
                'business_date',
                'created_at',
                'updated_at',
                'date'
            )
                then 'partial'

            when column_name in (
                'price',
                'quantity',
                'amount',
                'revenue',
                'total_price',
                'for_pay',
                'finished_price',
                'discount_percent'
            )
                then 'partial'

            else 'warning'
        end as issue_severity
    from issues
),

decisions as (
    select
        *,
        case
            when issue_severity = 'bad' then false
            else true
        end as can_load_to_core,

        case
            when column_name in (
                'price',
                'quantity',
                'amount',
                'revenue',
                'total_price',
                'for_pay',
                'finished_price'
            ) then false
            else true
        end as can_count_revenue,

        case
            when column_name in (
                'order_date',
                'sale_date',
                'business_date',
                'created_at',
                'date'
            ) then false
            else true
        end as can_use_order_date
    from classified
)

select *
from decisions
