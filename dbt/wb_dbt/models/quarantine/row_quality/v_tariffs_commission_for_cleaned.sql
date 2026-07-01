{{ config(materialized='view', schema='quarantine', alias='v_tariffs_commission_for_cleaned', tags=['row_quality']) }}

select *
from {{ ref('rq_tariffs_commission_quality') }}
where can_load_to_cleaned = true
