{{ config(materialized='view', schema='quarantine', alias='v_items_stocks_for_cleaned', tags=['row_quality']) }}

select *
from {{ ref('rq_items_stocks_quality') }}
where can_load_to_cleaned = true
