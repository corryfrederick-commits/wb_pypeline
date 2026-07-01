{{ config(materialized='view', schema='quarantine') }}

select *
from {{ ref('v_orders_current_for_core') }}
where quality_status = 'good'
