{% macro safe_cast(expr, target_type) -%}
    {%- set t = target_type | lower -%}

    {%- if t in ['text', 'varchar', 'character varying', 'string'] -%}
        {{ safe_cast_text(expr) }}
    {%- elif t in ['int', 'integer'] -%}
        {{ safe_cast_integer(expr) }}
    {%- elif t in ['bigint'] -%}
        {{ safe_cast_bigint(expr) }}
    {%- elif t in ['numeric', 'decimal', 'double precision', 'real', 'float'] -%}
        {{ safe_cast_numeric(expr) }}
    {%- elif t in ['boolean', 'bool'] -%}
        {{ safe_cast_boolean(expr) }}
    {%- elif t in ['date'] -%}
        audit.try_cast_date({{ expr }}::text)
    {%- elif t in ['timestamp', 'timestamp without time zone'] -%}
        audit.try_cast_timestamp({{ expr }}::text)
    {%- elif t in ['timestamptz', 'timestamp with time zone'] -%}
        audit.try_cast_timestamptz({{ expr }}::text)
    {%- elif t in ['json', 'jsonb'] -%}
        audit.try_cast_jsonb({{ expr }}::text)
    {%- else -%}
        {{ expr }}::{{ target_type }}
    {%- endif -%}
{%- endmacro %}

{% macro safe_cast_text(expr) -%}
    case when {{ expr }} is null then null else {{ expr }}::text end
{%- endmacro %}

{% macro safe_cast_integer(expr) -%}
    case
        when {{ expr }} is null then null
        when trim({{ expr }}::text) ~ '^[+-]?[0-9]+$' then trim({{ expr }}::text)::integer
        else null
    end
{%- endmacro %}

{% macro safe_cast_bigint(expr) -%}
    case
        when {{ expr }} is null then null
        when trim({{ expr }}::text) ~ '^[+-]?[0-9]+$' then trim({{ expr }}::text)::bigint
        else null
    end
{%- endmacro %}

{% macro safe_cast_numeric(expr) -%}
    case
        when {{ expr }} is null then null
        when trim({{ expr }}::text) ~ '^[+-]?([0-9]+([.][0-9]*)?|[.][0-9]+)$' then trim({{ expr }}::text)::numeric
        else null
    end
{%- endmacro %}

{% macro safe_cast_boolean(expr) -%}
    case
        when {{ expr }} is null then null
        when lower(trim({{ expr }}::text)) in ('true', 't', '1', 'yes', 'y', 'да') then true
        when lower(trim({{ expr }}::text)) in ('false', 'f', '0', 'no', 'n', 'нет') then false
        else null
    end
{%- endmacro %}
