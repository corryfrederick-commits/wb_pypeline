import os
import sys
import urllib.request
from pathlib import Path

from dotenv import load_dotenv
from json_sources import JSON_FILES


PROJECT_DIR = Path(os.getenv("WB_PIPELINE_PROJECT_DIR", "/opt/wb_pipeline"))
ENV_PATH = PROJECT_DIR / ".env"

load_dotenv(ENV_PATH)

WB_MOCK_BASE_URL = os.getenv("WB_MOCK_BASE_URL")
DOWNLOAD_DIR = PROJECT_DIR / "data" / "tmp_downloads"


def download_file(base_url: str, filename: str) -> Path:
    url = f"{base_url.rstrip('/')}/{filename}"
    target = DOWNLOAD_DIR / filename

    print(f"[+] downloading {url}")

    with urllib.request.urlopen(url, timeout=30) as response:
        body = response.read()

    if not body:
        raise RuntimeError(f"empty response: {url}")

    target.write_bytes(body)
    print(f"    saved: {target} bytes={len(body)}")

    return target


def main() -> None:
    if not WB_MOCK_BASE_URL:
        raise RuntimeError("WB_MOCK_BASE_URL is not set")

    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

    for old_file in DOWNLOAD_DIR.glob("*.json"):
        old_file.unlink()

    downloaded = []

    for filename in JSON_FILES:
        downloaded.append(download_file(WB_MOCK_BASE_URL, filename))

    print()
    print(f"Downloaded JSON files: {len(downloaded)}")

    if len(downloaded) != len(JSON_FILES):
        raise RuntimeError(
            f"Expected {len(JSON_FILES)} files, downloaded {len(downloaded)}"
        )


if __name__ == "__main__":
    main()
