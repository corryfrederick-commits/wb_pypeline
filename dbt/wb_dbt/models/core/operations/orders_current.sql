{{ config(materialized='view', schema='core', alias='orders_current', tags=['core', 'core_operations', 'current']) }}

select *
from {{ ref('orders') }}
where is_current = true
