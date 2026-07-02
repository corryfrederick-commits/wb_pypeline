begin;

alter table landing.raw_payloads
    add column if not exists client_id text;

alter table landing.raw_payloads
    add column if not exists wb_account_id text;

update landing.raw_payloads
set
    client_id = coalesce(nullif(client_id, ''), 'demo_client'),
    wb_account_id = coalesce(nullif(wb_account_id, ''), 'demo_wb_account')
where client_id is null
   or client_id = ''
   or wb_account_id is null
   or wb_account_id = '';

alter table landing.raw_payloads
    alter column client_id set default 'demo_client',
    alter column client_id set not null;

alter table landing.raw_payloads
    alter column wb_account_id set default 'demo_wb_account',
    alter column wb_account_id set not null;

create schema if not exists control;

create table if not exists control.clients (
    client_id text primary key,
    client_name text not null,
    is_active boolean not null default true,
    created_at timestamp not null default now()
);

create table if not exists control.client_wb_accounts (
    wb_account_id text primary key,
    client_id text not null references control.clients(client_id),
    account_name text not null,
    is_active boolean not null default true,
    created_at timestamp not null default now()
);

insert into control.clients (client_id, client_name)
values ('demo_client', 'Demo WB client')
on conflict (client_id) do nothing;

insert into control.client_wb_accounts (wb_account_id, client_id, account_name)
values ('demo_wb_account', 'demo_client', 'Demo WB account')
on conflict (wb_account_id) do nothing;

create index if not exists ix_raw_payloads_client_account_dataset_loaded
on landing.raw_payloads (
    client_id,
    wb_account_id,
    dataset_name,
    loaded_at desc
);

create or replace view quarantine.v_raw_payloads_schema_passed as
select
    rp.id,
    rp.source_system,
    rp.dataset_name,
    rp.source_file,
    rp.source_url,
    rp.file_hash,
    rp.loaded_at,
    rp.payload_type,
    rp.top_level_count,
    rp.payload,
    rp.client_id,
    rp.wb_account_id
from landing.raw_payloads rp
left join quarantine.raw_payloads_schema_failed f
    on f.raw_payload_id = rp.id
where f.raw_payload_id is null;

commit;
