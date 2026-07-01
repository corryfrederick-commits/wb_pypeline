{{ config(materialized='table', schema='quarantine') }}

with source_orders as (

    select *
    from {{ ref('stg_orders_current') }}

),

base_issues as (

    select
        raw_payload_id, record_index, dataset_name, source_file,
        order_flow, order_kind, order_id,
        'bad'::text as issue_level,
        'missing_any_order_identifier'::text as issue_code,
        'order_id/order_uid/rid/order_code'::text as problem_field,
        null::text as problem_value,
        'Нет ни order_id, ни order_uid, ни rid, ни order_code. Нельзя безопасно идентифицировать заказ.'::text as details,
        raw_record
    from source_orders
    where order_id is null
      and nullif(order_uid, '') is null
      and nullif(rid, '') is null
      and nullif(order_code, '') is null

    union all

    select
        raw_payload_id, record_index, dataset_name, source_file,
        order_flow, order_kind, order_id,
        'bad',
        'invalid_order_flow',
        'order_flow',
        order_flow,
        'order_flow должен быть одним из: fbs, dbs, dbw, pickup.',
        raw_record
    from source_orders
    where order_flow is null
       or order_flow not in ('fbs', 'dbs', 'edbs', 'dbspickuppoint', 'dbw', 'pickup', 'pickuppoint', 'selfpickup')

    union all

    select
        raw_payload_id, record_index, dataset_name, source_file,
        order_flow, order_kind, order_id,
        'bad',
        'invalid_order_kind',
        'order_kind',
        order_kind,
        'order_kind должен быть одним из: new, current, completed, archive.',
        raw_record
    from source_orders
    where order_kind is null
       or order_kind not in ('new', 'current', 'completed', 'archive')

    union all

    select
        raw_payload_id, record_index, dataset_name, source_file,
        order_flow, order_kind, order_id,
        'bad',
        'raw_record_not_object',
        'raw_record',
        jsonb_typeof(raw_record),
        'raw_record должен быть JSON object.',
        raw_record
    from source_orders
    where jsonb_typeof(raw_record) <> 'object'

),

duplicated_identity as (

    select
        dataset_name,
        coalesce(order_id::text, nullif(order_uid, ''), nullif(rid, ''), nullif(order_code, '')) as order_identity,
        count(*) as rows_count
    from source_orders
    where coalesce(order_id::text, nullif(order_uid, ''), nullif(rid, ''), nullif(order_code, '')) is not null
    group by dataset_name, coalesce(order_id::text, nullif(order_uid, ''), nullif(rid, ''), nullif(order_code, ''))
    having count(*) > 1

),

duplicate_issues as (

    select
        s.raw_payload_id, s.record_index, s.dataset_name, s.source_file,
        s.order_flow, s.order_kind, s.order_id,
        'bad'::text as issue_level,
        'duplicate_order_identity_in_dataset'::text as issue_code,
        'order_identity'::text as problem_field,
        d.order_identity::text as problem_value,
        'Один идентификатор заказа несколько раз встретился внутри одного dataset_name.'::text as details,
        s.raw_record
    from source_orders s
    join duplicated_identity d
      on d.dataset_name = s.dataset_name
     and d.order_identity = coalesce(s.order_id::text, nullif(s.order_uid, ''), nullif(s.rid, ''), nullif(s.order_code, ''))

),

cross_flow as (

    select
        order_id,
        string_agg(distinct order_flow, ', ' order by order_flow) as flows
    from source_orders
    where order_id is not null
    group by order_id
    having count(distinct order_flow) > 1

),

cross_flow_issues as (

    select
        s.raw_payload_id, s.record_index, s.dataset_name, s.source_file,
        s.order_flow, s.order_kind, s.order_id,
        'bad'::text as issue_level,
        'order_id_in_multiple_flows'::text as issue_code,
        'order_id'::text as problem_field,
        s.order_id::text as problem_value,
        'Один order_id встретился в нескольких order_flow: ' || c.flows as details,
        s.raw_record
    from source_orders s
    join cross_flow c
      on c.order_id = s.order_id

),

partial_issues as (

    select
        raw_payload_id, record_index, dataset_name, source_file,
        order_flow, order_kind, order_id,
        'partial'::text as issue_level,
        'missing_order_id_using_alternate_identifier'::text as issue_code,
        'order_id'::text as problem_field,
        coalesce(nullif(order_uid, ''), nullif(rid, ''), nullif(order_code, '')) as problem_value,
        'order_id отсутствует, но есть order_uid/rid/order_code.'::text as details,
        raw_record
    from source_orders
    where order_id is null
      and coalesce(nullif(order_uid, ''), nullif(rid, ''), nullif(order_code, '')) is not null

    union all

    select
        raw_payload_id, record_index, dataset_name, source_file,
        order_flow, order_kind, order_id,
        'partial',
        'missing_created_at',
        'created_at',
        null,
        'created_at отсутствует. Заказ можно хранить, но нельзя корректно использовать в аналитике по дате заказа.',
        raw_record
    from source_orders
    where created_at is null

    union all

    select
        raw_payload_id, record_index, dataset_name, source_file,
        order_flow, order_kind, order_id,
        'partial',
        'missing_price',
        'price',
        null,
        'price отсутствует. Заказ можно хранить, но нельзя использовать для расчёта выручки.',
        raw_record
    from source_orders
    where price is null

    union all

    select
        raw_payload_id, record_index, dataset_name, source_file,
        order_flow, order_kind, order_id,
        'partial',
        'negative_price',
        'price',
        price::text,
        'price отрицательная. Заказ можно хранить, но нельзя безопасно использовать для revenue-аналитики.',
        raw_record
    from source_orders
    where price is not null
      and price < 0

    union all

    select
        raw_payload_id, record_index, dataset_name, source_file,
        order_flow, order_kind, order_id,
        'partial',
        'missing_nm_id',
        'nm_id',
        null,
        'nm_id отсутствует. Заказ можно хранить, но товарная аналитика будет неполной.',
        raw_record
    from source_orders
    where nm_id is null

    union all

    select
        raw_payload_id, record_index, dataset_name, source_file,
        order_flow, order_kind, order_id,
        'partial',
        'missing_article',
        'article',
        null,
        'article отсутствует. Заказ можно хранить, но аналитика по артикулу будет неполной.',
        raw_record
    from source_orders
    where article is null

),

warning_issues as (

    select
        raw_payload_id, record_index, dataset_name, source_file,
        order_flow, order_kind, order_id,
        'warning'::text as issue_level,
        'final_price_greater_than_price'::text as issue_code,
        'final_price'::text as problem_field,
        final_price::text as problem_value,
        'final_price больше price. Это не блокирует заказ.'::text as details,
        raw_record
    from source_orders
    where price is not null
      and final_price is not null
      and final_price > price

    union all

    select
        raw_payload_id, record_index, dataset_name, source_file,
        order_flow, order_kind, order_id,
        'warning',
        'suspicious_delivery_type',
        'delivery_type',
        delivery_type,
        'delivery_type не совпадает с order_flow. Сейчас order_flow считается более надёжным.',
        raw_record
    from source_orders
    where delivery_type is not null
      and lower(delivery_type) not like '%' || lower(order_flow) || '%'

    union all

    select
        raw_payload_id, record_index, dataset_name, source_file,
        order_flow, order_kind, order_id,
        'warning',
        'invalid_address_latitude',
        'address_latitude',
        address_latitude::text,
        'Широта должна быть в диапазоне от -90 до 90.',
        raw_record
    from source_orders
    where address_latitude is not null
      and (address_latitude < -90 or address_latitude > 90)

    union all

    select
        raw_payload_id, record_index, dataset_name, source_file,
        order_flow, order_kind, order_id,
        'warning',
        'invalid_address_longitude',
        'address_longitude',
        address_longitude::text,
        'Долгота должна быть в диапазоне от -180 до 180.',
        raw_record
    from source_orders
    where address_longitude is not null
      and (address_longitude < -180 or address_longitude > 180)

    union all

    select
        raw_payload_id, record_index, dataset_name, source_file,
        order_flow, order_kind, order_id,
        'warning',
        'skus_not_array',
        'skus',
        jsonb_typeof(skus),
        'skus ожидался как array. Строка не блокируется.',
        raw_record
    from source_orders
    where skus is not null
      and jsonb_typeof(skus) <> 'array'

),

all_issues as (

    select * from base_issues
    union all
    select * from duplicate_issues
    union all
    select * from cross_flow_issues
    union all
    select * from partial_issues
    union all
    select * from warning_issues

)

select
    row_number() over (
        order by
            raw_payload_id,
            record_index,
            issue_level,
            issue_code,
            problem_field,
            problem_value
    )::bigint as id,
    raw_payload_id,
    record_index,
    dataset_name,
    source_file,
    order_flow,
    order_kind,
    order_id,
    issue_level,
    issue_code,
    problem_field,
    problem_value,
    details,
    raw_record,
    now() as detected_at
from all_issues
