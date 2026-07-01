import argparse
import csv
import json
import re
from collections import defaultdict
from pathlib import Path

import yaml


PROJECT = Path("/opt/wb_pipeline/dbt/wb_dbt")
DICT_PATH = PROJECT / "metadata" / "wb_yaml" / "dataset_field_dictionary.json"
REPORT_PATH = PROJECT / "metadata" / "staging_description_enrichment_report.csv"


DATASET_KEYS = [
    "dataset_name",
    "dataset",
    "actual_dataset",
    "source_dataset",
    "mock_dataset",
    "json_dataset",
    "file_dataset",
    "source_file",
    "file_name",
    "json_file",
    "mock_file",
]

FIELD_PATH_KEYS = [
    "field_path",
    "actual_field_path",
    "json_path",
    "path",
    "full_path",
    "field_full_path",
    "source_field_path",
    "yaml_path",
    "property_path",
]

FIELD_NAME_KEYS = [
    "field_name",
    "name",
    "actual_field_name",
    "property_name",
    "yaml_field_name",
    "source_field_name",
]

DESCRIPTION_KEYS = [
    "description",
    "desc",
    "yaml_description",
    "field_description",
    "description_ru",
    "description_en",
    "text",
]


def norm_dataset(value):
    if value is None:
        return ""

    s = str(value).strip()
    s = s.split("/")[-1]
    s = re.sub(r"\.json$", "", s, flags=re.IGNORECASE)
    s = s.replace("-", "_")
    return s.lower()


def norm_path(value):
    if value is None:
        return ""

    s = str(value).strip()
    s = s.replace("$.", "")
    s = s.replace("$", "")
    s = s.replace("[]", "")
    s = s.replace("/", ".")
    s = s.strip(".")
    s = re.sub(r"\.+", ".", s)
    return s.lower()


def norm_name(value):
    if value is None:
        return ""

    s = str(value).strip()
    s = s.split(".")[-1]
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s)
    s = re.sub(r"[^a-zA-Z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_").lower()
    return s


def first_present(row, keys):
    for k in keys:
        if k in row and row[k] not in (None, ""):
            return row[k]
    return None


def load_dictionary():
    if not DICT_PATH.exists():
        raise FileNotFoundError(f"Dictionary not found: {DICT_PATH}")

    data = json.loads(DICT_PATH.read_text(encoding="utf-8"))

    exact = {}
    by_name = defaultdict(list)
    raw_rows = []

    for i, row in enumerate(data):
        dataset_raw = first_present(row, DATASET_KEYS)
        field_path_raw = first_present(row, FIELD_PATH_KEYS)
        field_name_raw = first_present(row, FIELD_NAME_KEYS)
        description = first_present(row, DESCRIPTION_KEYS)

        dataset = norm_dataset(dataset_raw)
        field_path = norm_path(field_path_raw)
        field_name = norm_name(field_name_raw or field_path_raw)

        if not description or not str(description).strip():
            continue

        description = str(description).strip()

        item = {
            "row_number": i + 1,
            "dataset": dataset,
            "field_path": field_path,
            "field_name": field_name,
            "description": description,
            "raw": row,
        }

        raw_rows.append(item)

        if dataset and field_path:
            exact[(dataset, field_path)] = item

        if dataset and field_name:
            by_name[(dataset, field_name)].append(item)

    unique_by_name = {}

    for key, items in by_name.items():
        descriptions = {x["description"] for x in items}

        # По имени поля используем только безопасное совпадение:
        # внутри dataset одно уникальное описание.
        if len(descriptions) == 1:
            unique_by_name[key] = items[0]

    return exact, unique_by_name, raw_rows


def extract_generated_source(desc):
    if not desc:
        return None

    # Примеры:
    # Автоматически распарсенное поле `nmId` из JSON dataset `items_cards`.
    # Автоматически распарсенное поле `nmId` из уровня `nm` dataset `promotion_fullstats`.
    m = re.search(r"поле\s+`([^`]+)`.*?dataset\s+`([^`]+)`", desc, flags=re.IGNORECASE | re.DOTALL)

    if not m:
        return None

    field_path = m.group(1)
    dataset = m.group(2)

    return norm_dataset(dataset), norm_path(field_path), field_path


def is_generated_description(desc):
    if not desc:
        return False

    return "Автоматически распарсенное" in desc or "Предложенный SQL-тип" in desc


def enrich_description(wb_description, dataset, original_field_path):
    wb_description = wb_description.strip()

    suffix = f" JSON path: `{original_field_path}`. Dataset: `{dataset}`."

    if "JSON path:" in wb_description:
        return wb_description

    return wb_description + suffix


def process_file(path, exact, unique_by_name, apply):
    original_text = path.read_text(encoding="utf-8")
    doc = yaml.safe_load(original_text)

    if not doc or "models" not in doc:
        return [], False

    changed = False
    report_rows = []

    for model in doc.get("models", []):
        model_name = model.get("name")

        # stg_orders_current уже был вычищен вручную, его не трогаем.
        if model_name == "stg_orders_current":
            continue

        for col in model.get("columns", []):
            col_name = col.get("name")
            old_desc = col.get("description", "") or ""

            source = extract_generated_source(old_desc)

            if not source:
                report_rows.append({
                    "model": model_name,
                    "column": col_name,
                    "file": str(path.relative_to(PROJECT)),
                    "status": "skipped_not_generated_description",
                    "dataset": "",
                    "field_path": "",
                    "match_mode": "",
                    "old_description": old_desc,
                    "new_description": "",
                })
                continue

            dataset, field_path_norm, original_field_path = source
            col_name_norm = norm_name(col_name)
            field_name_norm = norm_name(original_field_path)

            match = None
            match_mode = ""

            if (dataset, field_path_norm) in exact:
                match = exact[(dataset, field_path_norm)]
                match_mode = "dataset_plus_field_path"
            elif (dataset, field_name_norm) in unique_by_name:
                match = unique_by_name[(dataset, field_name_norm)]
                match_mode = "dataset_plus_unique_field_name"
            elif (dataset, col_name_norm) in unique_by_name:
                match = unique_by_name[(dataset, col_name_norm)]
                match_mode = "dataset_plus_unique_column_name"

            if not match:
                report_rows.append({
                    "model": model_name,
                    "column": col_name,
                    "file": str(path.relative_to(PROJECT)),
                    "status": "no_safe_match",
                    "dataset": dataset,
                    "field_path": original_field_path,
                    "match_mode": "",
                    "old_description": old_desc,
                    "new_description": "",
                })
                continue

            new_desc = enrich_description(match["description"], dataset, original_field_path)

            if new_desc.strip() == old_desc.strip():
                status = "already_ok"
            else:
                status = "updated"
                col["description"] = new_desc
                changed = True

            report_rows.append({
                "model": model_name,
                "column": col_name,
                "file": str(path.relative_to(PROJECT)),
                "status": status,
                "dataset": dataset,
                "field_path": original_field_path,
                "match_mode": match_mode,
                "old_description": old_desc,
                "new_description": new_desc,
            })

    if apply and changed:
        new_text = yaml.safe_dump(
            doc,
            allow_unicode=True,
            sort_keys=False,
            width=140,
        )
        path.write_text(new_text, encoding="utf-8")

    return report_rows, changed


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="Actually rewrite staging yml files")
    args = parser.parse_args()

    exact, unique_by_name, raw_rows = load_dictionary()

    print("========== WB YAML dictionary ==========")
    print("usable description rows:", len(raw_rows))
    print("exact dataset+field_path matches:", len(exact))
    print("safe dataset+field_name matches:", len(unique_by_name))
    print()

    yml_files = sorted((PROJECT / "models" / "staging").glob("**/*.yml"))

    all_rows = []
    changed_files = 0

    for path in yml_files:
        rows, changed = process_file(path, exact, unique_by_name, args.apply)
        all_rows.extend(rows)
        if changed:
            changed_files += 1

    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)

    with REPORT_PATH.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "model",
            "column",
            "file",
            "status",
            "dataset",
            "field_path",
            "match_mode",
            "old_description",
            "new_description",
        ])
        writer.writeheader()
        writer.writerows(all_rows)

    by_status = defaultdict(int)
    for r in all_rows:
        by_status[r["status"]] += 1

    print("========== staging description enrichment ==========")
    print("mode:", "APPLY" if args.apply else "DRY RUN")
    print("yml files:", len(yml_files))
    print("changed files:", changed_files)
    print()

    for status, count in sorted(by_status.items()):
        print(f"{status}: {count}")

    print()
    print("report:", REPORT_PATH)

    print()
    print("========== examples updated/no_safe_match ==========")
    shown = 0
    for r in all_rows:
        if r["status"] in {"updated", "no_safe_match"}:
            print(
                f"{r['status']:14s} "
                f"{r['model']}.{r['column']} "
                f"dataset={r['dataset']} field={r['field_path']} mode={r['match_mode']}"
            )
            shown += 1
            if shown >= 30:
                break


if __name__ == "__main__":
    main()
