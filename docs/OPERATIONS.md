# WB Pipeline Operations

## Servers

### Server 1 — Mock API

Role:

- generates mock Wildberries JSON files
- publishes JSON through nginx from `/var/www/html`
- runs daily generation through cron

Project path:

/opt/wb_mock_api

Public JSON directory:

/var/www/html

Daily generation script:

/opt/wb_mock_api/scripts/run_daily_generation.sh

Cron schedule:

0 3 * * * /opt/wb_mock_api/scripts/run_daily_generation.sh

Check cron:

crontab -l

Run generation manually:

/opt/wb_mock_api/scripts/run_daily_generation.sh

Check generation log:

tail -200 /var/log/wb_mock_api/daily_generation.log

Check published JSON files:

find /var/www/html -maxdepth 1 -name "*.json" -printf "%f\n" | sort
find /var/www/html -maxdepth 1 -name "*.json" | wc -l


## Server 2 — DWH / dbt / Airflow

Role:

- loads JSON from Server 1
- stores raw payloads in PostgreSQL
- builds dbt layers
- publishes client-facing materialized views
- runs full pipeline through Airflow

Project path:

/opt/wb_pipeline

Main database:

wb_pipeline

Main dbt project:

/opt/wb_pipeline/dbt/wb_dbt

dbt profiles directory:

/opt/wb_pipeline/dbt

Python venv for loaders/dbt:

/opt/wb_pipeline/venv

Airflow venv:

/opt/wb_pipeline/airflow_venv

Airflow home:

/opt/wb_pipeline/airflow

Airflow env file:

/etc/wb_airflow.env


## Data flow

Current pipeline:

landing.raw_payloads
→ quarantine
→ staging
→ staging_cleaned
→ core
→ marts
→ client_demo

Internal schemas:

- landing
- quarantine
- staging
- staging_cleaned
- core
- marts
- control

Client-facing schema:

- client_demo

The client receives materialized views from:

- staging_cleaned
- core
- marts

The client does not receive:

- landing
- quarantine
- raw_payloads
- internal control tables


## Client schema

Client schema name:

client_demo

Expected materialized views:

- 24 staging_cleaned materialized views
- 24 core materialized views
- 6 marts materialized views

Total:

54 materialized views

Check client schema:

cd /opt/wb_pipeline

set -a
source /opt/wb_pipeline/.env
set +a

PGPASSWORD="$DB_PASSWORD" psql -P pager=off \
  -h "${DB_HOST:-localhost}" \
  -p "${DB_PORT:-5432}" \
  -U "${DB_USER:-wb_user}" \
  -d "${DB_NAME:-wb_pipeline}" <<'SQL'
select
    schemaname,
    count(*) as materialized_views_count
from pg_matviews
where schemaname = 'client_demo'
group by schemaname;

select
    case
        when matviewname like 'staging_cleaned__%' then 'staging_cleaned'
        when matviewname like 'core__%' then 'core'
        when matviewname like 'marts__%' then 'marts'
        else 'other'
    end as layer,
    count(*) as objects_count
from pg_matviews
where schemaname = 'client_demo'
group by 1
order by 1;

select count(*) as business_daily_rows
from client_demo.marts__mart_business_daily;
SQL


## Client readonly user

Readonly role:

client_demo_readonly

Password file on server:

/root/client_demo_readonly.password

Do not commit this password.

Check readonly access:

cd /opt/wb_pipeline

set -a
source /opt/wb_pipeline/.env
set +a

READONLY_PASSWORD="$(cat /root/client_demo_readonly.password)"

PGPASSWORD="$READONLY_PASSWORD" psql -P pager=off \
  -h "${DB_HOST:-localhost}" \
  -p "${DB_PORT:-5432}" \
  -U client_demo_readonly \
  -d "${DB_NAME:-wb_pipeline}" \
  -c "select count(*) from client_demo.marts__mart_business_daily;"

The readonly user must not be able to read internal schemas:

PGPASSWORD="$READONLY_PASSWORD" psql -P pager=off \
  -h "${DB_HOST:-localhost}" \
  -p "${DB_PORT:-5432}" \
  -U client_demo_readonly \
  -d "${DB_NAME:-wb_pipeline}" \
  -c "select count(*) from core.orders;"


## Airflow

Airflow version:

2.10.5

Airflow executor:

LocalExecutor

Airflow webserver:

http://94.232.40.124:8081

Admin username:

admin

Admin password file on server:

/root/airflow_admin.password

Do not commit this password.

Check Airflow services:

systemctl status airflow-webserver --no-pager -l
systemctl status airflow-scheduler --no-pager -l

Restart Airflow services:

systemctl restart airflow-webserver airflow-scheduler

Check Airflow DAGs:

cd /opt/wb_pipeline

set -a
source /etc/wb_airflow.env
set +a

source /opt/wb_pipeline/airflow_venv/bin/activate

airflow dags list

Main DAG:

wb_daily_pipeline

DAG schedule:

0 4 * * *

Check DAG status:

airflow dags list | grep wb_daily_pipeline

List DAG tasks:

airflow tasks list wb_daily_pipeline

Run DAG manually:

airflow dags trigger wb_daily_pipeline

Run DAG as local test:

airflow dags test wb_daily_pipeline 2026-07-03

Unpause DAG:

airflow dags unpause wb_daily_pipeline

Pause DAG:

airflow dags pause wb_daily_pipeline


## Main DAG tasks

DAG:

wb_daily_pipeline

Tasks:

1. check_mock_api
2. load_raw_payloads
3. dbt_build_internal_dwh
4. dbt_build_client_exports
5. check_client_demo

Task logic:

check_mock_api:
- checks that Server 1 JSON endpoint is available

load_raw_payloads:
- runs loader
- loads JSON into landing.raw_payloads

dbt_build_internal_dwh:
- builds staging
- builds staging_cleaned
- builds core
- builds marts

dbt_build_client_exports:
- builds client_demo materialized views

check_client_demo:
- checks that client_demo has 54 materialized views
- checks that marts__mart_business_daily has rows


## Manual dbt commands

Activate project venv:

source /opt/wb_pipeline/venv/bin/activate

Build internal DWH:

cd /opt/wb_pipeline/dbt/wb_dbt

DBT_PROFILES_DIR=/opt/wb_pipeline/dbt dbt build \
  --select models/staging models/staging_cleaned models/core models/marts \
  --threads 2

Build client exports:

cd /opt/wb_pipeline/dbt/wb_dbt

DBT_PROFILES_DIR=/opt/wb_pipeline/dbt dbt build \
  --select models/client_exports \
  --threads 2

Build everything important:

cd /opt/wb_pipeline/dbt/wb_dbt

DBT_PROFILES_DIR=/opt/wb_pipeline/dbt dbt build \
  --select models/staging models/staging_cleaned models/core models/marts models/client_exports \
  --threads 2


## Manual loader command

cd /opt/wb_pipeline

set -a
source /opt/wb_pipeline/.env
set +a

source /opt/wb_pipeline/venv/bin/activate

python /opt/wb_pipeline/loaders/load_raw_json_to_postgres.py


## Useful PostgreSQL checks

Check raw payload count:

cd /opt/wb_pipeline

set -a
source /opt/wb_pipeline/.env
set +a

PGPASSWORD="$DB_PASSWORD" psql -P pager=off \
  -h "${DB_HOST:-localhost}" \
  -p "${DB_PORT:-5432}" \
  -U "${DB_USER:-wb_user}" \
  -d "${DB_NAME:-wb_pipeline}" \
  -c "select count(*) from landing.raw_payloads;"

Check marts row counts:

PGPASSWORD="$DB_PASSWORD" psql -P pager=off \
  -h "${DB_HOST:-localhost}" \
  -p "${DB_PORT:-5432}" \
  -U "${DB_USER:-wb_user}" \
  -d "${DB_NAME:-wb_pipeline}" <<'SQL'
select 'mart_products_catalog' as table_name, count(*) from marts.mart_products_catalog
union all select 'mart_stock_current', count(*) from marts.mart_stock_current
union all select 'mart_orders_daily', count(*) from marts.mart_orders_daily
union all select 'mart_sales_daily', count(*) from marts.mart_sales_daily
union all select 'mart_promotion_daily', count(*) from marts.mart_promotion_daily
union all select 'mart_business_daily', count(*) from marts.mart_business_daily
order by table_name;
SQL


## Logs

Airflow service logs:

journalctl -u airflow-webserver -n 200 --no-pager
journalctl -u airflow-scheduler -n 200 --no-pager

Airflow task logs:

/opt/wb_pipeline/airflow/logs

dbt logs:

/opt/wb_pipeline/dbt/wb_dbt/logs

Server 1 generation log:

/var/log/wb_mock_api/daily_generation.log


## Git repositories

Server 1 repo:

/opt/wb_mock_api

Server 2 repo:

/opt/wb_pipeline

Check repo status:

git status --short
git log --oneline -6

Do not commit:

- .env
- dbt/profiles.yml
- airflow_venv/
- venv/
- passwords
- Airflow logs
- dbt target/
- dbt logs/
- __pycache__/
- generated JSON files
