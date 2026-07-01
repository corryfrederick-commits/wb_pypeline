{{ config(materialized='table', schema='staging_cleaned', alias='tariffs_acceptance', tags=['staging_cleaned']) }}

select *
from {{ ref('v_tariffs_acceptance_for_cleaned') }}
