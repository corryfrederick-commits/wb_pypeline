# Core fact fields coverage in marts

Проверка показывает, какие поля из core fact/current tables присутствуют в marts/client exports.

## Summary

- Total core fact columns checked: 476
- Present as output columns in marts/client exports: 142
- Used in mart SQL, but not exposed as output columns: 94
- Not found by column name: 240
- Not found and likely technical: 72
- Not found and likely business fields: 168

## Important interpretation

Не каждое поле из core должно напрямую попадать в marts.

Обычно в marts не выводятся:

- SCD2 technical fields;
- hash fields;
- service metadata;
- raw payload ids;
- internal source tracking fields.

Но если business-поле не найдено ни как output column, ни в SQL marts, это надо проверить отдельно.

## Fields not found by name

| core_object | core_column | data_type | technical? |
|---|---|---|---|
| core.order_items | order_item_version_key | text | yes |
| core.order_items | order_item_natural_id | text | no |
| core.order_items | order_item_row_hash | text | yes |
| core.order_items | version_number | bigint | yes |
| core.order_items | valid_from | timestamp with time zone | yes |
| core.order_items | valid_to | timestamp with time zone | yes |
| core.order_items | is_current | boolean | yes |
| core.order_items | order_natural_id | text | no |
| core.order_items | order_id | bigint | no |
| core.order_items | rid | text | no |
| core.order_items | srid | text | no |
| core.order_items | order_uid | text | no |
| core.order_items | order_code | text | no |
| core.order_items | skus | jsonb | no |
| core.order_items | currency_code | integer | no |
| core.order_items | converted_currency_code | integer | no |
| core.order_items | scan_price | numeric | no |
| core.order_items | delivery_deadline_at | timestamp with time zone | no |
| core.order_items | seller_date | timestamp with time zone | no |
| core.order_items | source_system | text | yes |
| core.order_items | source_dataset | text | yes |
| core.order_items | source_row_id | text | no |
| core.order_items | core_loaded_at | timestamp with time zone | yes |
| core.order_items_current | order_item_version_key | text | yes |
| core.order_items_current | order_item_natural_id | text | no |
| core.order_items_current | order_item_row_hash | text | yes |
| core.order_items_current | version_number | bigint | yes |
| core.order_items_current | valid_from | timestamp with time zone | yes |
| core.order_items_current | valid_to | timestamp with time zone | yes |
| core.order_items_current | is_current | boolean | yes |
| core.order_items_current | order_natural_id | text | no |
| core.order_items_current | order_id | bigint | no |
| core.order_items_current | rid | text | no |
| core.order_items_current | srid | text | no |
| core.order_items_current | order_uid | text | no |
| core.order_items_current | order_code | text | no |
| core.order_items_current | skus | jsonb | no |
| core.order_items_current | currency_code | integer | no |
| core.order_items_current | converted_currency_code | integer | no |
| core.order_items_current | scan_price | numeric | no |
| core.order_items_current | delivery_deadline_at | timestamp with time zone | no |
| core.order_items_current | seller_date | timestamp with time zone | no |
| core.order_items_current | source_system | text | yes |
| core.order_items_current | source_dataset | text | yes |
| core.order_items_current | source_row_id | text | no |
| core.order_items_current | core_loaded_at | timestamp with time zone | yes |
| core.orders | order_version_key | text | yes |
| core.orders | order_natural_id | text | no |
| core.orders | order_row_hash | text | yes |
| core.orders | version_number | bigint | yes |
| core.orders | valid_from | timestamp with time zone | yes |
| core.orders | valid_to | timestamp with time zone | yes |
| core.orders | is_current | boolean | yes |
| core.orders | order_id | bigint | no |
| core.orders | rid | text | no |
| core.orders | srid | text | no |
| core.orders | order_uid | text | no |
| core.orders | order_code | text | no |
| core.orders | delivery_deadline_at | timestamp with time zone | no |
| core.orders | seller_date | timestamp with time zone | no |
| core.orders | inserted_at | timestamp with time zone | no |
| core.orders | supply_id | text | no |
| core.orders | group_id | text | no |
| core.orders | cargo_type | text | no |
| core.orders | cross_border_type | text | no |
| core.orders | color_code | text | no |
| core.orders | comment | text | no |
| core.orders | is_zero_order | boolean | no |
| core.orders | is_b2b | boolean | no |
| core.orders | is_archive | boolean | no |
| core.orders | address_full | text | no |
| core.orders | address_latitude | numeric | no |
| core.orders | address_longitude | numeric | no |
| core.orders | source_system | text | yes |
| core.orders | source_dataset | text | yes |
| core.orders | source_row_id | text | no |
| core.orders | core_loaded_at | timestamp with time zone | yes |
| core.orders_current | order_version_key | text | yes |
| core.orders_current | order_natural_id | text | no |
| core.orders_current | order_row_hash | text | yes |
| core.orders_current | version_number | bigint | yes |
| core.orders_current | valid_from | timestamp with time zone | yes |
| core.orders_current | valid_to | timestamp with time zone | yes |
| core.orders_current | is_current | boolean | yes |
| core.orders_current | order_id | bigint | no |
| core.orders_current | rid | text | no |
| core.orders_current | srid | text | no |
| core.orders_current | order_uid | text | no |
| core.orders_current | order_code | text | no |
| core.orders_current | delivery_deadline_at | timestamp with time zone | no |
| core.orders_current | seller_date | timestamp with time zone | no |
| core.orders_current | inserted_at | timestamp with time zone | no |
| core.orders_current | supply_id | text | no |
| core.orders_current | group_id | text | no |
| core.orders_current | cargo_type | text | no |
| core.orders_current | cross_border_type | text | no |
| core.orders_current | color_code | text | no |
| core.orders_current | comment | text | no |
| core.orders_current | is_zero_order | boolean | no |
| core.orders_current | is_b2b | boolean | no |
| core.orders_current | is_archive | boolean | no |
| core.orders_current | address_full | text | no |
| core.orders_current | address_latitude | numeric | no |
| core.orders_current | address_longitude | numeric | no |
| core.orders_current | source_system | text | yes |
| core.orders_current | source_dataset | text | yes |
| core.orders_current | source_row_id | text | no |
| core.orders_current | core_loaded_at | timestamp with time zone | yes |
| core.report_order_events | report_order_event_key | text | no |
| core.report_order_events | report_order_event_version_key | text | yes |
| core.report_order_events | report_order_event_natural_id | text | no |
| core.report_order_events | report_order_event_row_hash | text | yes |
| core.report_order_events | version_number | bigint | yes |
| core.report_order_events | valid_from | timestamp with time zone | yes |
| core.report_order_events | valid_to | timestamp with time zone | yes |
| core.report_order_events | is_current | boolean | yes |
| core.report_order_events | source_report_order_id | bigint | no |
| core.report_order_events | order_natural_id | text | no |
| core.report_order_events | order_id | bigint | no |
| core.report_order_events | order_uid | text | no |
| core.report_order_events | rid | text | no |
| core.report_order_events | srid | text | no |
| core.report_order_events | g_number | text | no |
| core.report_order_events | supplier_article | text | no |
| core.report_order_events | skus | jsonb | no |
| core.report_order_events | tech_size | text | no |
| core.report_order_events | warehouse_type | text | no |
| core.report_order_events | date_value | timestamp with time zone | no |
| core.report_order_events | last_change_date | timestamp with time zone | no |
| core.report_order_events | sale_dt | timestamp with time zone | no |
| core.report_order_events | cancel_date | timestamp with time zone | no |
| core.report_order_events | is_cancel | boolean | no |
| core.report_order_events | is_realization | boolean | no |
| core.report_order_events | is_supply | boolean | no |
| core.report_order_events | currency_code | bigint | no |
| core.report_order_events | converted_currency_code | bigint | no |
| core.report_order_events | income_id | bigint | no |
| core.report_order_events | sticker | text | no |
| core.report_order_events | source_system | text | yes |
| core.report_order_events | source_dataset | text | yes |
| core.report_order_events | source_row_id | text | no |
| core.report_order_events | core_loaded_at | timestamp with time zone | yes |
| core.report_order_events_current | report_order_event_key | text | no |
| core.report_order_events_current | report_order_event_version_key | text | yes |
| core.report_order_events_current | report_order_event_natural_id | text | no |
| core.report_order_events_current | report_order_event_row_hash | text | yes |
| core.report_order_events_current | version_number | bigint | yes |
| core.report_order_events_current | valid_from | timestamp with time zone | yes |
| core.report_order_events_current | valid_to | timestamp with time zone | yes |
| core.report_order_events_current | is_current | boolean | yes |
| core.report_order_events_current | source_report_order_id | bigint | no |
| core.report_order_events_current | order_natural_id | text | no |
| core.report_order_events_current | order_id | bigint | no |
| core.report_order_events_current | order_uid | text | no |
| core.report_order_events_current | rid | text | no |
| core.report_order_events_current | srid | text | no |
| core.report_order_events_current | g_number | text | no |
| core.report_order_events_current | supplier_article | text | no |
| core.report_order_events_current | skus | jsonb | no |
| core.report_order_events_current | tech_size | text | no |
| core.report_order_events_current | warehouse_type | text | no |
| core.report_order_events_current | date_value | timestamp with time zone | no |
| core.report_order_events_current | last_change_date | timestamp with time zone | no |
| core.report_order_events_current | sale_dt | timestamp with time zone | no |
| core.report_order_events_current | cancel_date | timestamp with time zone | no |
| core.report_order_events_current | is_cancel | boolean | no |
| core.report_order_events_current | is_realization | boolean | no |
| core.report_order_events_current | is_supply | boolean | no |
| core.report_order_events_current | currency_code | bigint | no |
| core.report_order_events_current | converted_currency_code | bigint | no |
| core.report_order_events_current | income_id | bigint | no |
| core.report_order_events_current | sticker | text | no |
| core.report_order_events_current | source_system | text | yes |
| core.report_order_events_current | source_dataset | text | yes |
| core.report_order_events_current | source_row_id | text | no |
| core.report_order_events_current | core_loaded_at | timestamp with time zone | yes |
| core.report_sale_events | report_sale_event_key | text | no |
| core.report_sale_events | report_sale_event_version_key | text | yes |
| core.report_sale_events | sale_event_natural_id | text | no |
| core.report_sale_events | sale_event_row_hash | text | yes |
| core.report_sale_events | version_number | bigint | yes |
| core.report_sale_events | valid_from | timestamp with time zone | yes |
| core.report_sale_events | valid_to | timestamp with time zone | yes |
| core.report_sale_events | is_current | boolean | yes |
| core.report_sale_events | source_report_sale_id | bigint | no |
| core.report_sale_events | order_natural_id | text | no |
| core.report_sale_events | order_id | bigint | no |
| core.report_sale_events | order_uid | text | no |
| core.report_sale_events | rid | text | no |
| core.report_sale_events | srid | text | no |
| core.report_sale_events | g_number | text | no |
| core.report_sale_events | supplier_article | text | no |
| core.report_sale_events | skus | jsonb | no |
| core.report_sale_events | tech_size | text | no |
| core.report_sale_events | warehouse_type | text | no |
| core.report_sale_events | date_value | timestamp with time zone | no |
| core.report_sale_events | last_change_date | timestamp with time zone | no |
| core.report_sale_events | sale_dt | timestamp with time zone | no |
| core.report_sale_events | is_realization | boolean | no |
| core.report_sale_events | is_supply | boolean | no |
| core.report_sale_events | currency_code | bigint | no |
| core.report_sale_events | converted_currency_code | bigint | no |
| core.report_sale_events | income_id | bigint | no |
| core.report_sale_events | sticker | text | no |
| core.report_sale_events | source_system | text | yes |
| core.report_sale_events | source_dataset | text | yes |
| core.report_sale_events | source_row_id | text | no |
| core.report_sale_events | core_loaded_at | timestamp with time zone | yes |
| core.report_sale_events_current | report_sale_event_key | text | no |
| core.report_sale_events_current | report_sale_event_version_key | text | yes |
| core.report_sale_events_current | sale_event_natural_id | text | no |
| core.report_sale_events_current | sale_event_row_hash | text | yes |
| core.report_sale_events_current | version_number | bigint | yes |
| core.report_sale_events_current | valid_from | timestamp with time zone | yes |
| core.report_sale_events_current | valid_to | timestamp with time zone | yes |
| core.report_sale_events_current | is_current | boolean | yes |
| core.report_sale_events_current | source_report_sale_id | bigint | no |
| core.report_sale_events_current | order_natural_id | text | no |
| core.report_sale_events_current | order_id | bigint | no |
| core.report_sale_events_current | order_uid | text | no |
| core.report_sale_events_current | rid | text | no |
| core.report_sale_events_current | srid | text | no |
| core.report_sale_events_current | g_number | text | no |
| core.report_sale_events_current | supplier_article | text | no |
| core.report_sale_events_current | skus | jsonb | no |
| core.report_sale_events_current | tech_size | text | no |
| core.report_sale_events_current | warehouse_type | text | no |
| core.report_sale_events_current | date_value | timestamp with time zone | no |
| core.report_sale_events_current | last_change_date | timestamp with time zone | no |
| core.report_sale_events_current | sale_dt | timestamp with time zone | no |
| core.report_sale_events_current | is_realization | boolean | no |
| core.report_sale_events_current | is_supply | boolean | no |
| core.report_sale_events_current | currency_code | bigint | no |
| core.report_sale_events_current | converted_currency_code | bigint | no |
| core.report_sale_events_current | income_id | bigint | no |
| core.report_sale_events_current | sticker | text | no |
| core.report_sale_events_current | source_system | text | yes |
| core.report_sale_events_current | source_dataset | text | yes |
| core.report_sale_events_current | source_row_id | text | no |
| core.report_sale_events_current | core_loaded_at | timestamp with time zone | yes |
