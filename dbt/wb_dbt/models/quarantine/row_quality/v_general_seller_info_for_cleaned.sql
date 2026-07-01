{{ config(materialized='view', schema='quarantine', alias='v_general_seller_info_for_cleaned', tags=['row_quality']) }}

select *
from {{ ref('rq_general_seller_info_quality') }}
where can_load_to_cleaned = true
