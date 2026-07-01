{{ config(materialized='view', schema='quarantine', alias='v_promotion_fullstats_for_cleaned', tags=['row_quality']) }}

select *
from {{ ref('rq_promotion_fullstats_quality') }}
where can_load_to_cleaned = true
