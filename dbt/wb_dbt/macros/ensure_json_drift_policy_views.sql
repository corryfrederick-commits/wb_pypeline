{% macro ensure_json_drift_policy_views() %}

create schema if not exists audit;

create table if not exists audit.json_extra_field_decisions (
    decision_id bigserial primary key,
    dataset_name text not null,
    source_file text not null,
    json_path text not null,
    actual_type text,
    action text not null,
    decision_reason text,
    decided_at timestamptz not null default now(),
    unique (dataset_name, source_file, json_path)
);

do $$
begin
    if to_regclass('audit.v_json_schema_check') is not null
       and to_regclass('audit.expected_json_fields') is not null then

        execute $sql$
            create or replace view audit.v_json_extra_fields_pending as
            select
                c.dataset_name,
                c.source_file,
                c.json_path,
                c.actual_types as actual_type,
                c.expected_type,
                c.check_status,
                d.action as decision_action,
                d.decided_at
            from audit.v_json_schema_check c
            left join audit.json_extra_field_decisions d
              on d.dataset_name = c.dataset_name
             and d.source_file = c.source_file
             and d.json_path = c.json_path
            where c.check_status = 'extra_in_actual'
              and d.decision_id is null
        $sql$;

        execute $sql$
            create or replace view audit.v_json_missing_fields_current as
            select
                c.dataset_name,
                c.source_file,
                c.json_path,
                c.expected_type,
                coalesce(e.is_required, false) as is_required,
                c.check_status
            from audit.v_json_schema_check c
            left join audit.expected_json_fields e
              on e.dataset_name = c.dataset_name
             and e.source_file = c.source_file
             and e.json_path = c.json_path
            where c.check_status = 'missing_in_actual'
        $sql$;

        execute $sql$
            create or replace view audit.v_json_missing_required_fields_current as
            select *
            from audit.v_json_missing_fields_current
            where is_required = true
        $sql$;

        execute $sql$
            create or replace view audit.v_json_missing_optional_fields_current as
            select *
            from audit.v_json_missing_fields_current
            where is_required = false
        $sql$;

    else
        raise notice 'Skipping JSON drift policy views: audit.v_json_schema_check or audit.expected_json_fields is missing';
    end if;
end $$;

{% endmacro %}
