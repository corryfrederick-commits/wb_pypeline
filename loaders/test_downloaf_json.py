import json
import os
from pathlib import Path

import requests
from dotenv import load_dotenv

from json_sources import JSON_FILES


PROJECT_DIR = Path("/opt/wb_pipeline")
load_dotenv(PROJECT_DIR / ".env")

BASE_URL = os.getenv("WB_MOCK_BASE_URL")
DOWNLOAD_DIR = PROJECT_DIR / "data" / "tmp_downloads"


def print_json_diagnostics(data):
    if isinstance(data, dict):
        print("    Тип JSON: object / dict")
        print(f"    Верхнеуровневых ключей: {len(data)}")
        print(f"    Первые ключи: {list(data.keys())[:10]}")

        for key, value in data.items():
            if isinstance(value, list):
                print(f"    Массив '{key}': {len(value)} элементов")
                if value and isinstance(value[0], dict):
                    print(f"    Первые поля '{key}[0]': {list(value[0].keys())[:10]}")
                break

    elif isinstance(data, list):
        print("    Тип JSON: array / list")
        print(f"    Количество элементов: {len(data)}")

        if data and isinstance(data[0], dict):
            print(f"    Первые поля первой записи: {list(data[0].keys())[:10]}")

    else:
        print(f"    Тип JSON: {type(data).__name__}")


def download_one_json(filename: str) -> Path:
    url = f"{BASE_URL.rstrip('/')}/{filename}"
    output_path = DOWNLOAD_DIR / filename

    print("\\n[+] Скачиваю JSON:")
    print(f"    URL: {url}")

    response = requests.get(url, timeout=30)
    response.raise_for_status()

    try:
        data = response.json()
    except json.JSONDecodeError as error:
        raise ValueError(f"Сервер вернул невалидный JSON: {filename}") from error

    with open(output_path, "w", encoding="utf-8") as file:
        json.dump(data, file, indent=4, ensure_ascii=False)

    print("[+] JSON успешно скачан:")
    print(f"    Файл: {output_path}")
    print("[+] Диагностика JSON:")
    print_json_diagnostics(data)

    return output_path


def main():
    if not BASE_URL:
        raise ValueError("Не задана WB_MOCK_BASE_URL в /opt/wb_pipeline/.env")

    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

    for old_file in DOWNLOAD_DIR.glob("*.json"):
        old_file.unlink()

    print("[+] Начинаю скачивание mock JSON")
    print(f"    BASE_URL: {BASE_URL}")
    print(f"    DOWNLOAD_DIR: {DOWNLOAD_DIR}")
    print(f"    Количество файлов: {len(JSON_FILES)}")

    downloaded_files = []
    failed_files = []

    for filename in JSON_FILES:
        try:
            downloaded_files.append(download_one_json(filename))
        except Exception as error:
            print("\\n[!] Ошибка при скачивании:")
            print(f"    Файл: {filename}")
            print(f"    Ошибка: {error}")
            failed_files.append(filename)

    print("\\n========== ИТОГ СКАЧИВАНИЯ ==========")
    print(f"[+] Успешно скачано: {len(downloaded_files)}")

    for path in downloaded_files:
        print(f"    {path.name}")

    if failed_files:
        print(f"\\n[!] Не удалось скачать: {len(failed_files)}")
        for filename in failed_files:
            print(f"    {filename}")
        raise RuntimeError("Часть JSON-файлов не была скачана.")

    print("\\n[+] Все JSON успешно скачаны.")


if __name__ == "__main__":
    main()
