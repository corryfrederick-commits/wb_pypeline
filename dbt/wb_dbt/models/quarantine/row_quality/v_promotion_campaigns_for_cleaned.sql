{{ config(materialized='view', schema='quarantine', alias='v_promotion_campaigns_for_cleaned', tags=['row_quality']) }}

select *
from {{ ref('rq_promotion_campaigns_quality') }}
where can_load_to_cleaned = true
