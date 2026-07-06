{% macro ensure_json_drift_policy_views() %}

create schema if not exists audit;

create table if not exists audit.json_extra_field_decisions (
    decision_id bigserial primary key,
    client_id text not null default 'demo_client',
    wb_account_id text not null default 'demo_wb_account',
    dataset_name text not null,
    source_file text not null,
    json_path text not null,
    actual_type text,
    action text not null,
    decision_reason text,
    decided_at timestamptz not null default now(),
    unique (client_id, wb_account_id, dataset_name, source_file, json_path)
);

alter table audit.json_extra_field_decisions add column if not exists client_id text;
alter table audit.json_extra_field_decisions add column if not exists wb_account_id text;
update audit.json_extra_field_decisions set client_id = 'demo_client' where client_id is null;
update audit.json_extra_field_decisions set wb_account_id = 'demo_wb_account' where wb_account_id is null;
alter table audit.json_extra_field_decisions alter column client_id set default 'demo_client';
alter table audit.json_extra_field_decisions alter column wb_account_id set default 'demo_wb_account';
alter table audit.json_extra_field_decisions alter column client_id set not null;
alter table audit.json_extra_field_decisions alter column wb_account_id set not null;
drop index if exists audit.json_extra_field_decisions_dataset_name_source_file_json_path_key;
create unique index if not exists ux_json_extra_field_decisions_client_account_path
    on audit.json_extra_field_decisions (client_id, wb_account_id, dataset_name, source_file, json_path);

do $$
begin
    if to_regclass('audit.v_json_schema_check') is not null
       and to_regclass('audit.expected_json_fields') is not null then

        execute $sql$
            create or replace view audit.v_json_extra_fields_pending as
            select
                c.client_id,
                c.wb_account_id,
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
              on d.client_id = c.client_id
             and d.wb_account_id = c.wb_account_id
             and d.dataset_name = c.dataset_name
             and d.source_file = c.source_file
             and d.json_path = c.json_path
            where c.check_status = 'extra_in_actual'
              and d.decision_id is null
        $sql$;

        execute $sql$
            create or replace view audit.v_json_missing_fields_current as
            select
                c.client_id,
                c.wb_account_id,
                c.dataset_name,
                c.source_file,
                c.json_path,
                c.expected_type,
                coalesce(e.is_required, false) as is_required,
                c.check_status
            from audit.v_json_schema_check c
            left join audit.expected_json_fields e
              on e.client_id = c.client_id
             and e.wb_account_id = c.wb_account_id
             and e.dataset_name = c.dataset_name
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
