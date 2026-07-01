{{ config(materialized='table', schema='staging_cleaned', alias='general_seller_info', tags=['staging_cleaned']) }}

select *
from {{ ref('v_general_seller_info_for_cleaned') }}
