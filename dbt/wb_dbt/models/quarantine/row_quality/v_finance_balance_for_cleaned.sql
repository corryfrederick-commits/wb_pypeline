{{ config(materialized='view', schema='quarantine', alias='v_finance_balance_for_cleaned', tags=['row_quality']) }}

select *
from {{ ref('rq_finance_balance_quality') }}
where can_load_to_cleaned = true
