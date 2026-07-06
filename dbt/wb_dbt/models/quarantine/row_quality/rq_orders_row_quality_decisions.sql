-- canonical dbt orders row-quality decision view

{{ config(materialized='view') }}

select *
from {{ ref('rq_row_quality_decisions') }}
where source_model ilike '%order%'
   or source_model ilike '%sale%'
