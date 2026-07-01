import csv
from collections import defaultdict
from pathlib import Path

import psycopg2


PROJECT = Path("/opt/wb_pipeline/dbt/wb_dbt")
RULES_PATH = PROJECT / "metadata" / "row_quality_rules.csv"
OUT_CSV = PROJECT / "metadata" / "row_quality_rule_evaluation.csv"
ENV_PATH = Path("/opt/wb_pipeline/.env")


def load_env(path):
    env = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def quote_ident(name):
    return '"' + name.replace('"', '""') + '"'


env = load_env(ENV_PATH)

rules = list(csv.DictReader(RULES_PATH.open("r", encoding="utf-8")))

conn = psycopg2.connect(
    host=env.get("DB_HOST", "localhost"),
    port=env.get("DB_PORT", "5432"),
    dbname=env.get("DB_NAME", "wb_pipeline"),
    user=env.get("DB_USER", "wb_user"),
    password=env["DB_PASSWORD"],
)

results = []

cur = conn.cursor()

for r in rules:
    if r["enabled"] != "true":
        continue

    if r["severity"] == "info":
        continue

    condition = (r["sql_condition"] or "").strip()

    if not condition:
        continue

    model_name = r["model_name"]

    sql = f"""
        select count(*)
        from (
            select
                *,
                ({condition}) as is_issue
            from staging.{quote_ident(model_name)}
        ) q
        where is_issue is true;
    """

    try:
        cur.execute(sql)
        issue_rows = cur.fetchone()[0]
        status = "OK"
        error = ""
    except Exception as e:
        conn.rollback()
        issue_rows = ""
        status = "ERROR"
        error = str(e).replace("\\n", " ")[:500]

    out = dict(r)
    out["issue_rows"] = issue_rows
    out["eval_status"] = status
    out["eval_error"] = error
    results.append(out)

cur.close()
conn.close()

fieldnames = list(rules[0].keys()) + ["issue_rows", "eval_status", "eval_error"]

with OUT_CSV.open("w", encoding="utf-8", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(results)

summary = defaultdict(lambda: {
    "rules": 0,
    "rules_with_issues": 0,
    "issue_rows": 0,
    "errors": 0,
})

for r in results:
    model_name = r["model_name"]
    summary[model_name]["rules"] += 1

    if r["eval_status"] == "ERROR":
        summary[model_name]["errors"] += 1
        continue

    issue_rows = int(r["issue_rows"] or 0)
    summary[model_name]["issue_rows"] += issue_rows

    if issue_rows > 0:
        summary[model_name]["rules_with_issues"] += 1

print("OK: evaluated row quality rules")
print("rules evaluated:", len(results))
print("report:", OUT_CSV)

print()
print("========== evaluation summary ==========")
for model_name in sorted(summary):
    s = summary[model_name]
    print(
        f"{model_name:40s} "
        f"rules={s['rules']:3d} "
        f"rules_with_issues={s['rules_with_issues']:3d} "
        f"issue_rows={s['issue_rows']:5d} "
        f"errors={s['errors']:3d}"
    )

print()
print("========== rules with issues ==========")
shown = 0
for r in results:
    if r["eval_status"] == "OK" and int(r["issue_rows"] or 0) > 0:
        print(
            f"{r['severity']:8s} "
            f"{r['model_name']:40s} "
            f"{r['issue_code']:45s} "
            f"rows={r['issue_rows']}"
        )
        shown += 1
        if shown >= 100:
            break

print()
print("========== errors ==========")
shown = 0
for r in results:
    if r["eval_status"] == "ERROR":
        print()
        print(r["model_name"], r["issue_code"])
        print(r["eval_error"])
        shown += 1
        if shown >= 30:
            break
