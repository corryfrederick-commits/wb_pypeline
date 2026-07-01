{{ config(materialized='table', schema='staging_cleaned', alias='communications_feedbacks', tags=['staging_cleaned']) }}

select *
from {{ ref('v_communications_feedbacks_for_cleaned') }}
