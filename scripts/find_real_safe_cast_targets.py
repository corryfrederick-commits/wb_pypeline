import re
from pathlib import Path

ROOT = Path("/opt/wb_pipeline")
MODELS = ROOT / "dbt" / "wb_dbt" / "models"

SEARCH_DIRS = [
    MODELS / "staging_cleaned",
    MODELS / "staging",
]

CAST_RE = re.compile(
    r"""
    (?P<expr>
        [a-zA-Z_][a-zA-Z0-9_\.]* |
        [a-zA-Z_][a-zA-Z0-9_\.]*\s*->>?\s*'[^']+' |
        [a-zA-Z_][a-zA-Z0-9_\.]*\s*->>?\s*"[^"]+"
    )
    \s*::\s*
    (?P<type>
        integer|int|bigint|numeric|decimal|boolean|bool|
        date|timestamp|timestamptz|jsonb
    )
    \b
    """,
    re.I | re.X,
)

IGNORE = [
    "ordinality::integer",
    "'[]'::jsonb",
    "'{}'::jsonb",
    "null::",
    "safe_cast(",
]

SAFE_TECHNICAL_COLUMNS = [
    "record_index",
    "root_index",
    "day_index",
    "app_index",
    "nm_index",
]

hits = []

for search_dir in SEARCH_DIRS:
    if not search_dir.exists():
        continue

    for path in sorted(search_dir.rglob("*.sql")):
        text = path.read_text(encoding="utf-8", errors="ignore")

        for line_no, line in enumerate(text.splitlines(), start=1):
            low = line.lower()

            if any(x in low for x in IGNORE):
                continue

            if any(col in low and "ordinality" in low for col in SAFE_TECHNICAL_COLUMNS):
                continue

            match = CAST_RE.search(line)

            if match:
                hits.append(
                    {
                        "file": str(path.relative_to(ROOT)),
                        "line_no": line_no,
                        "line": line.rstrip(),
                        "expr": match.group("expr").strip(),
                        "type": match.group("type").lower(),
                    }
                )

print("Real safe_cast targets in staging/staging_cleaned")
print("================================================")

for h in hits:
    print(f"{h['file']}:{h['line_no']}: {h['line']}")
    print(f"  expr={h['expr']}")
    print(f"  type={h['type']}")

print()
print(f"total real targets: {len(hits)}")
