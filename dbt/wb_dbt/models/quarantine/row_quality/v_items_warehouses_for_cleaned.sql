{{ config(materialized='view', schema='quarantine', alias='v_items_warehouses_for_cleaned', tags=['row_quality']) }}

select *
from {{ ref('rq_items_warehouses_quality') }}
where can_load_to_cleaned = true
