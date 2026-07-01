{{ config(materialized='view', schema='quarantine', alias='v_orders_pickup_statuses_for_cleaned', tags=['row_quality']) }}

select *
from {{ ref('rq_orders_pickup_statuses_quality') }}
where can_load_to_cleaned = true
