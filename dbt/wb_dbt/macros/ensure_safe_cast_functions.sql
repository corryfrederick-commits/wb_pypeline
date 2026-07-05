{% macro ensure_safe_cast_functions() %}

{% set sql %}
create schema if not exists audit;

create or replace function audit.try_cast_date(value text)
returns date
language plpgsql
stable
as $$
begin
    if value is null or btrim(value) = '' then
        return null;
    end if;
    return value::date;
exception when others then
    return null;
end;
$$;

create or replace function audit.try_cast_timestamp(value text)
returns timestamp
language plpgsql
stable
as $$
begin
    if value is null or btrim(value) = '' then
        return null;
    end if;
    return value::timestamp;
exception when others then
    return null;
end;
$$;

create or replace function audit.try_cast_timestamptz(value text)
returns timestamptz
language plpgsql
stable
as $$
begin
    if value is null or btrim(value) = '' then
        return null;
    end if;
    return value::timestamptz;
exception when others then
    return null;
end;
$$;

create or replace function audit.try_cast_jsonb(value text)
returns jsonb
language plpgsql
stable
as $$
begin
    if value is null or btrim(value) = '' then
        return null;
    end if;
    return value::jsonb;
exception when others then
    return null;
end;
$$;
{% endset %}

{% if execute %}
    {% do run_query(sql) %}
{% endif %}

{{ return('select 1 as safe_cast_functions_ready') }}

{% endmacro %}
