{{ config(materialized='view', schema='quarantine', alias='v_communications_feedbacks_for_cleaned', tags=['row_quality']) }}

select *
from {{ ref('rq_communications_feedbacks_quality') }}
where can_load_to_cleaned = true
