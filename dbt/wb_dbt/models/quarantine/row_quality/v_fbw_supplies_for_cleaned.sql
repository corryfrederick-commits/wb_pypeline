{{ config(materialized='view', schema='quarantine', alias='v_fbw_supplies_for_cleaned', tags=['row_quality']) }}

select *
from {{ ref('rq_fbw_supplies_quality') }}
where can_load_to_cleaned = true
