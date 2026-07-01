{{ config(materialized='table', schema='staging_cleaned', alias='communications_chats', tags=['staging_cleaned']) }}

select *
from {{ ref('v_communications_chats_for_cleaned') }}
