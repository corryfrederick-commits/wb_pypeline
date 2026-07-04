{{ config(materialized='view', schema='core', alias='order_items_current', tags=['core', 'core_operations', 'current']) }}

select *
from {{ ref('order_items') }}
where is_current = true
