import os
import re
from pathlib import Path

import psycopg2
import yaml


PROJECT = Path("/opt/wb_pipeline/dbt/wb_dbt")
CORE_DIR = PROJECT / "models" / "core"
ENV_PATH = Path("/opt/wb_pipeline/.env")


FOLDER_YML = {
    "reference": "core_reference.yml",
    "operations": "core_operations.yml",
    "reports_finance": "core_reports_finance.yml",
    "tariffs": "core_tariffs.yml",
    "analytics_promotion": "core_analytics_promotion.yml",
    "communications_supplies": "core_communications_supplies.yml",
}


MODEL_DESCRIPTIONS = {
    "sellers": "Core-справочник продавцов / кабинетов WB. Одна строка соответствует одному продавцу или кабинету продавца.",
    "products": "Core-справочник товаров WB. Одна строка соответствует одному товару WB по nm_id.",
    "product_variants": "Core-справочник вариантов товаров WB. Одна строка соответствует одному варианту товара по chrt_id.",
    "product_barcodes": "Core bridge-таблица barcode/SKU товаров. Одна строка соответствует одному barcode/SKU для товара или варианта товара.",
    "warehouses": "Core-справочник складов / офисов / точек исполнения WB.",
    "stock_balances": "Core факт-снимок остатков товаров на складах. Одна строка соответствует остатку товара/варианта на складе на момент загрузки данных.",
    "orders": "Core таблица бизнес-заказов. Одна строка соответствует одному заказу.",
    "order_items": "Core таблица товарных строк заказов. Одна строка соответствует одной товарной строке заказа.",
    "order_status_snapshots": "Core факт-снимок статусов заказов из status endpoint'ов.",
    "report_order_events": "Core таблица отчётных событий по заказам WB.",
    "report_sale_events": "Core таблица отчётных событий по продажам WB.",
    "finance_balances": "Core факт-снимок финансового баланса продавца.",
    "finance_report_summaries": "Core таблица сводных строк финансовых отчётов WB.",
    "finance_transactions": "Core таблица детальных финансовых операций WB.",
    "tariff_commissions": "Core периодический справочник комиссий WB по категориям и предметам.",
    "tariff_box_prices": "Core периодический справочник тарифов коробов WB.",
    "tariff_acceptance_prices": "Core периодический справочник тарифов приёмки WB.",
    "sales_funnel_metrics": "Core агрегированные метрики воронки продаж WB.",
    "stock_analytics_metrics": "Core агрегированные аналитические метрики остатков WB.",
    "promotion_campaigns": "Core справочник рекламных кампаний WB.",
    "promotion_product_daily_stats": "Core дневная статистика рекламы по кампании, приложению и товару.",
    "feedbacks": "Core таблица отзывов покупателей.",
    "chats": "Core таблица чатов / коммуникаций с покупателями.",
    "fbw_supplies": "Core таблица поставок FBW.",
}


EXACT_COLUMN_DESCRIPTIONS = {
    "source_system": "Источник данных.",
    "source_dataset": "Название исходного cleaned-набора данных.",
    "source_row_id": "Технический идентификатор исходной строки.",
    "raw_payload_id": "Идентификатор исходного raw payload.",
    "record_index": "Номер записи внутри исходного payload.",
    "source_loaded_at": "Время загрузки исходных данных.",
    "core_loaded_at": "Время построения core-записи.",
    "loaded_at": "Время загрузки исходной записи.",
    "nm_id": "Идентификатор номенклатуры WB.",
    "product_id": "Ссылка на товар. Соответствует nm_id.",
    "chrt_id": "Идентификатор характеристики / варианта товара WB.",
    "product_variant_id": "Ссылка на вариант товара. Соответствует chrt_id.",
    "warehouse_id": "Идентификатор склада WB.",
    "office_id": "Идентификатор офиса WB.",
    "warehouse_key": "Суррогатный ключ склада.",
    "warehouse_name": "Название склада.",
    "warehouse_address": "Адрес склада.",
    "warehouse_natural_id": "Единый натуральный идентификатор склада.",
    "order_key": "Суррогатный ключ заказа.",
    "order_natural_id": "Натуральный идентификатор заказа, собранный из order_uid, srid, rid или order_id.",
    "order_id": "Числовой идентификатор заказа WB.",
    "order_uid": "UID заказа.",
    "rid": "RID заказа.",
    "srid": "SRID заказа.",
    "order_code": "Код заказа.",
    "order_flow": "Схема / поток заказа.",
    "order_kind": "Тип заказа.",
    "order_created_at": "Дата и время создания заказа.",
    "delivery_deadline_at": "Плановая дата / дедлайн доставки.",
    "seller_date": "Дата продавца из источника.",
    "delivery_type": "Тип доставки.",
    "delivery_method": "Метод доставки.",
    "delivery_service": "Служба доставки.",
    "pay_mode": "Способ оплаты.",
    "currency": "Валюта.",
    "currency_code": "Код валюты.",
    "converted_currency_code": "Код валюты после конвертации.",
    "price": "Цена.",
    "sale_price": "Цена продажи.",
    "final_price": "Финальная цена.",
    "converted_price": "Цена после конвертации.",
    "converted_final_price": "Финальная цена после конвертации.",
    "barcode_value": "Значение barcode или SKU.",
    "skus": "JSON-массив SKU / barcode.",
    "article": "Артикул товара.",
    "vendor_code": "Артикул продавца.",
    "brand": "Бренд.",
    "subject_id": "Идентификатор предмета / категории WB.",
    "subject_name": "Название предмета / категории WB.",
    "category": "Категория товара.",
    "title": "Название товара.",
    "description": "Описание.",
    "created_at": "Дата и время создания записи в источнике.",
    "updated_at": "Дата и время обновления записи в источнике.",
    "operation_date": "Дата операции.",
    "date_from": "Начало периода.",
    "date_to": "Конец периода.",
    "report_id": "Идентификатор отчёта WB.",
    "report_type": "Тип отчёта WB.",
    "payment_schedule": "График платежей.",
    "status": "Статус.",
    "supplier_status": "Статус у поставщика.",
    "wb_status": "Статус на стороне WB.",
    "is_b2b": "Признак B2B.",
    "is_archive": "Признак архивной записи.",
    "quantity": "Количество.",
    "amount": "Количество или сумма из источника.",
    "phone": "Телефон из источника.",
}


def load_env():
    env = {}
    if ENV_PATH.exists():
        for line in ENV_PATH.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def humanize(name: str) -> str:
    return name.replace("_", " ")


def describe_column(col: str) -> str:
    if col in EXACT_COLUMN_DESCRIPTIONS:
        return EXACT_COLUMN_DESCRIPTIONS[col]

    if col.endswith("_key"):
        return f"Суррогатный ключ: {humanize(col)}."
    if col.endswith("_id"):
        return f"Идентификатор: {humanize(col)}."
    if col.endswith("_at"):
        return f"Дата и время: {humanize(col)}."
    if col.endswith("_date"):
        return f"Дата: {humanize(col)}."
    if col.endswith("_name"):
        return f"Название: {humanize(col)}."
    if col.endswith("_text"):
        return f"Текстовое поле: {humanize(col)}."
    if col.startswith("is_"):
        return f"Булевый признак: {humanize(col)}."
    if col.startswith("has_"):
        return f"Булевый признак наличия: {humanize(col)}."
    if col.startswith("statistic_"):
        return f"Метрика воронки продаж WB: {humanize(col)}."
    if col.startswith("root_"):
        return f"Метрика или атрибут на root-уровне рекламной статистики: {humanize(col)}."
    if col.startswith("day_"):
        return f"Дневная метрика рекламной статистики: {humanize(col)}."
    if col.startswith("app_"):
        return f"Метрика рекламной статистики на уровне приложения: {humanize(col)}."
    if col.startswith("nm_"):
        return f"Метрика рекламной статистики на уровне товара: {humanize(col)}."
    if "price" in col:
        return f"Ценовой показатель: {humanize(col)}."
    if "sum" in col:
        return f"Суммовой показатель: {humanize(col)}."
    if "count" in col:
        return f"Количественный показатель: {humanize(col)}."
    if "percent" in col or col.endswith("_prc"):
        return f"Процентный показатель: {humanize(col)}."
    if "discount" in col:
        return f"Показатель скидки: {humanize(col)}."
    if "commission" in col:
        return f"Показатель комиссии: {humanize(col)}."

    return f"Поле core-модели: {humanize(col)}."


def read_columns(conn, table_name):
    with conn.cursor() as cur:
        cur.execute(
            """
            select column_name
            from information_schema.columns
            where table_schema = 'core'
              and table_name = %s
            order by ordinal_position;
            """,
            (table_name,),
        )
        return [r[0] for r in cur.fetchall()]


def existing_tests_by_column(yml_path):
    if not yml_path.exists():
        return {}

    doc = yaml.safe_load(yml_path.read_text(encoding="utf-8"))
    if not doc:
        return {}

    result = {}

    for model in doc.get("models", []):
        model_name = model.get("name")
        for col in model.get("columns", []):
            col_name = col.get("name")
            if not model_name or not col_name:
                continue
            tests = col.get("tests")
            if tests:
                result[(model_name, col_name)] = tests

    return result


def main():
    env = load_env()

    conn = psycopg2.connect(
        host=env.get("DB_HOST", "localhost"),
        port=env.get("DB_PORT", "5432"),
        dbname=env.get("DB_NAME", "wb_pipeline"),
        user=env.get("DB_USER", "wb_user"),
        password=env["DB_PASSWORD"],
    )

    written = []

    for folder, yml_name in FOLDER_YML.items():
        folder_path = CORE_DIR / folder
        if not folder_path.exists():
            continue

        sql_models = sorted(p.stem for p in folder_path.glob("*.sql"))
        if not sql_models:
            continue

        yml_path = folder_path / yml_name
        tests_map = existing_tests_by_column(yml_path)

        models = []

        for model_name in sql_models:
            columns = read_columns(conn, model_name)

            if not columns:
                raise RuntimeError(f"No columns found in core.{model_name}. Run dbt model first.")

            yml_columns = []

            for col in columns:
                col_doc = {
                    "name": col,
                    "description": describe_column(col),
                }

                tests = tests_map.get((model_name, col))
                if tests:
                    col_doc["tests"] = tests

                yml_columns.append(col_doc)

            models.append(
                {
                    "name": model_name,
                    "description": MODEL_DESCRIPTIONS.get(
                        model_name,
                        f"Core-модель `{model_name}`.",
                    ),
                    "columns": yml_columns,
                }
            )

        doc = {
            "version": 2,
            "models": models,
        }

        yml_path.write_text(
            yaml.safe_dump(doc, allow_unicode=True, sort_keys=False, width=140),
            encoding="utf-8",
        )

        written.append(str(yml_path.relative_to(PROJECT)))

    conn.close()

    print("written yaml files:")
    for p in written:
        print(" -", p)


if __name__ == "__main__":
    main()
