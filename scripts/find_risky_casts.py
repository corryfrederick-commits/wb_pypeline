import re
from pathlib import Path

ROOT = Path("/opt/wb_pipeline")
MODELS_DIR = ROOT / "dbt" / "wb_dbt" / "models"

SEARCH_DIRS = [
    MODELS_DIR / "staging",
    MODELS_DIR / "staging_cleaned",
]

CAST_PATTERNS = [
    re.compile(r"::\s*(integer|int|bigint|numeric|decimal|boolean|bool|date|timestamp|timestamptz|jsonb)\b", re.I),
    re.compile(r"\bcast\s*\(.+?\s+as\s+(integer|int|bigint|numeric|decimal|boolean|bool|date|timestamp|timestamptz|jsonb)\s*\)", re.I),
]

def main():
    total = 0

    for search_dir in SEARCH_DIRS:
        if not search_dir.exists():
            continue

        for path in sorted(search_dir.rglob("*.sql")):
            text = path.read_text(encoding="utf-8", errors="ignore")
            lines = text.splitlines()

            hits = []

            for line_no, line in enumerate(lines, start=1):
                lowered = line.lower()

                if "safe_cast(" in lowered:
                    continue

                if any(pattern.search(line) for pattern in CAST_PATTERNS):
                    hits.append((line_no, line.rstrip()))

            if hits:
                rel = path.relative_to(ROOT)
                print(f"\n{rel}")

                for line_no, line in hits:
                    print(f"  {line_no}: {line}")
                    total += 1

    print("\n========== SUMMARY ==========")
    print(f"risky cast lines found: {total}")

if __name__ == "__main__":
    main()
