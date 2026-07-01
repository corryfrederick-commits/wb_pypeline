{{ config(materialized='table', schema='quarantine', alias='rq_promotion_fullstats_quality', tags=['row_quality']) }}

with base as (
    select *
    from {{ ref('stg_promotion_fullstats') }}
),

issues as (
    select *
    from {{ ref('rq_promotion_fullstats_issues') }}
),

issues_agg as (
    select
        raw_payload_id,
        record_index,

        count(*) as issue_count,

        count(*) filter (
            where issue_severity = 'bad'
        ) as bad_issue_count,

        count(*) filter (
            where issue_severity = 'warning'
        ) as warning_issue_count,

        array_agg(issue_code order by issue_code) filter (
            where issue_severity = 'bad'
        ) as quality_issues,

        array_agg(issue_code order by issue_code) filter (
            where issue_severity = 'warning'
        ) as warning_issues

    from issues
    group by
        raw_payload_id,
        record_index
)

select
    base.*,

    coalesce(issues_agg.issue_count, 0) as issue_count,
    coalesce(issues_agg.bad_issue_count, 0) as bad_issue_count,
    coalesce(issues_agg.warning_issue_count, 0) as warning_issue_count,

    case
        when coalesce(issues_agg.bad_issue_count, 0) > 0 then 'bad'
        when coalesce(issues_agg.warning_issue_count, 0) > 0 then 'partial'
        else 'good'
    end as quality_status,

    coalesce(issues_agg.quality_issues, array[]::text[]) as quality_issues,
    coalesce(issues_agg.warning_issues, array[]::text[]) as warning_issues,

    coalesce(issues_agg.bad_issue_count, 0) = 0 as can_load_to_cleaned

from base
left join issues_agg
    on base.raw_payload_id = issues_agg.raw_payload_id
   and base.record_index = issues_agg.record_index
