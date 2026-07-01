{{ config(materialized='view', schema='quarantine', alias='v_items_cards_for_cleaned', tags=['row_quality']) }}

select *
from {{ ref('rq_items_cards_quality') }}
where can_load_to_cleaned = true
