# Business core fact fields not found in marts

Этот отчёт показывает бизнес-поля из core fact/current таблиц, которые не найдены по имени в marts/client_exports/client_demo и не найдены в SQL marts.

Важно: если поле было переименовано, агрегировано или объединено с другим полем, такой скрипт может считать его отсутствующим. Поэтому это список для ручной проверки, а не автоматический приговор.

Total potentially missing business fields: 168

## core.order_items

| column | data_type |
|---|---|
| order_item_natural_id | text |
| order_natural_id | text |
| order_id | bigint |
| rid | text |
| srid | text |
| order_uid | text |
| order_code | text |
| skus | jsonb |
| currency_code | integer |
| converted_currency_code | integer |
| scan_price | numeric |
| delivery_deadline_at | timestamp with time zone |
| seller_date | timestamp with time zone |
| source_row_id | text |

## core.order_items_current

| column | data_type |
|---|---|
| order_item_natural_id | text |
| order_natural_id | text |
| order_id | bigint |
| rid | text |
| srid | text |
| order_uid | text |
| order_code | text |
| skus | jsonb |
| currency_code | integer |
| converted_currency_code | integer |
| scan_price | numeric |
| delivery_deadline_at | timestamp with time zone |
| seller_date | timestamp with time zone |
| source_row_id | text |

## core.orders

| column | data_type |
|---|---|
| order_natural_id | text |
| order_id | bigint |
| rid | text |
| srid | text |
| order_uid | text |
| order_code | text |
| delivery_deadline_at | timestamp with time zone |
| seller_date | timestamp with time zone |
| inserted_at | timestamp with time zone |
| supply_id | text |
| group_id | text |
| cargo_type | text |
| cross_border_type | text |
| color_code | text |
| comment | text |
| is_zero_order | boolean |
| is_b2b | boolean |
| is_archive | boolean |
| address_full | text |
| address_latitude | numeric |
| address_longitude | numeric |
| source_row_id | text |

## core.orders_current

| column | data_type |
|---|---|
| order_natural_id | text |
| order_id | bigint |
| rid | text |
| srid | text |
| order_uid | text |
| order_code | text |
| delivery_deadline_at | timestamp with time zone |
| seller_date | timestamp with time zone |
| inserted_at | timestamp with time zone |
| supply_id | text |
| group_id | text |
| cargo_type | text |
| cross_border_type | text |
| color_code | text |
| comment | text |
| is_zero_order | boolean |
| is_b2b | boolean |
| is_archive | boolean |
| address_full | text |
| address_latitude | numeric |
| address_longitude | numeric |
| source_row_id | text |

## core.report_order_events

| column | data_type |
|---|---|
| report_order_event_key | text |
| report_order_event_natural_id | text |
| source_report_order_id | bigint |
| order_natural_id | text |
| order_id | bigint |
| order_uid | text |
| rid | text |
| srid | text |
| g_number | text |
| supplier_article | text |
| skus | jsonb |
| tech_size | text |
| warehouse_type | text |
| date_value | timestamp with time zone |
| last_change_date | timestamp with time zone |
| sale_dt | timestamp with time zone |
| cancel_date | timestamp with time zone |
| is_cancel | boolean |
| is_realization | boolean |
| is_supply | boolean |
| currency_code | bigint |
| converted_currency_code | bigint |
| income_id | bigint |
| sticker | text |
| source_row_id | text |

## core.report_order_events_current

| column | data_type |
|---|---|
| report_order_event_key | text |
| report_order_event_natural_id | text |
| source_report_order_id | bigint |
| order_natural_id | text |
| order_id | bigint |
| order_uid | text |
| rid | text |
| srid | text |
| g_number | text |
| supplier_article | text |
| skus | jsonb |
| tech_size | text |
| warehouse_type | text |
| date_value | timestamp with time zone |
| last_change_date | timestamp with time zone |
| sale_dt | timestamp with time zone |
| cancel_date | timestamp with time zone |
| is_cancel | boolean |
| is_realization | boolean |
| is_supply | boolean |
| currency_code | bigint |
| converted_currency_code | bigint |
| income_id | bigint |
| sticker | text |
| source_row_id | text |

## core.report_sale_events

| column | data_type |
|---|---|
| report_sale_event_key | text |
| sale_event_natural_id | text |
| source_report_sale_id | bigint |
| order_natural_id | text |
| order_id | bigint |
| order_uid | text |
| rid | text |
| srid | text |
| g_number | text |
| supplier_article | text |
| skus | jsonb |
| tech_size | text |
| warehouse_type | text |
| date_value | timestamp with time zone |
| last_change_date | timestamp with time zone |
| sale_dt | timestamp with time zone |
| is_realization | boolean |
| is_supply | boolean |
| currency_code | bigint |
| converted_currency_code | bigint |
| income_id | bigint |
| sticker | text |
| source_row_id | text |

## core.report_sale_events_current

| column | data_type |
|---|---|
| report_sale_event_key | text |
| sale_event_natural_id | text |
| source_report_sale_id | bigint |
| order_natural_id | text |
| order_id | bigint |
| order_uid | text |
| rid | text |
| srid | text |
| g_number | text |
| supplier_article | text |
| skus | jsonb |
| tech_size | text |
| warehouse_type | text |
| date_value | timestamp with time zone |
| last_change_date | timestamp with time zone |
| sale_dt | timestamp with time zone |
| is_realization | boolean |
| is_supply | boolean |
| currency_code | bigint |
| converted_currency_code | bigint |
| income_id | bigint |
| sticker | text |
| source_row_id | text |

