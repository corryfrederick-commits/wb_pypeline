{{ config(materialized='table', schema='staging_cleaned', alias='items_cards', tags=['staging_cleaned']) }}

select *
from {{ ref('v_items_cards_for_cleaned') }}
