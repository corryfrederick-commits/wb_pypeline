\set ON_ERROR_STOP on

do $$
begin
    if not exists (
        select 1
        from pg_catalog.pg_roles
        where rolname = 'client_demo_readonly'
    ) then
        create role client_demo_readonly login;
    end if;
end
$$;

alter role client_demo_readonly with login password :'readonly_password';
alter role client_demo_readonly set search_path = client_demo;

grant connect on database :"db_name" to client_demo_readonly;

do $$
declare
    schema_name text;
begin
    for schema_name in
        select nspname
        from pg_catalog.pg_namespace
        where nspname not in ('client_demo', 'information_schema', 'pg_catalog')
          and nspname not like 'pg\_%' escape '\'
    loop
        execute format(
            'revoke all privileges on schema %I from client_demo_readonly',
            schema_name
        );

        execute format(
            'revoke all privileges on all tables in schema %I from client_demo_readonly',
            schema_name
        );

        execute format(
            'revoke all privileges on all sequences in schema %I from client_demo_readonly',
            schema_name
        );
    end loop;
end
$$;

grant usage on schema client_demo to client_demo_readonly;
grant select on all tables in schema client_demo to client_demo_readonly;

alter default privileges for role :"owner_role" in schema client_demo
grant select on tables to client_demo_readonly;
