---
allowed-tools: Bash(cd:*),Bash(./run.sh:*),Bash(bash:*),Bash(python*:*)
---

DB 현황 통계 출력. Run:

```bash
cd /home/sunjgaeyoon/Job_ReSearch_Agent && ./run.sh --stats $ARGUMENTS
```

If `--stats` flag is not supported, fall back to running inline:

```bash
cd /home/sunjgaeyoon/Job_ReSearch_Agent && python3 << 'PYEOF'
import sqlite3, os
db_path = os.path.join(os.path.dirname(__file__) if '__file__' in dir() else '.', 'data', 'jobs.db')
conn = sqlite3.connect('data/jobs.db')
cur = conn.cursor()

cur.execute("SELECT source, COUNT(*) FROM jobs WHERE status != 'dismissed' GROUP BY source ORDER BY COUNT(*) DESC")
print("=== Jobs by Source ===")
total = 0
for r in cur.fetchall():
    print(f"  {r[0]:25s} {r[1]:4d}")
    total += r[1]
print(f"  {'TOTAL':25s} {total:4d}")

cur.execute("SELECT COUNT(*) FROM jobs WHERE pi_name IS NOT NULL AND pi_name != '' AND status != 'dismissed'")
pi_count = cur.fetchone()[0]
print(f"\n=== PI Names: {pi_count}/{total} ({100*pi_count//max(total,1)}%) ===")

cur.execute("SELECT COUNT(*) FROM pis WHERE is_recommended=1")
rec = cur.fetchone()[0]
cur.execute("SELECT COUNT(*) FROM pis WHERE is_seed=1")
seed = cur.fetchone()[0]
print(f"=== PIs: {seed} seeds, {rec} recommended ===")

cur.execute("SELECT status, COUNT(*) FROM jobs GROUP BY status ORDER BY COUNT(*) DESC")
print("\n=== Jobs by Status ===")
for r in cur.fetchall():
    print(f"  {r[0]:25s} {r[1]:4d}")

cur.execute("SELECT COUNT(*) FROM jobs WHERE match_score > 0 AND status != 'dismissed'")
scored = cur.fetchone()[0]
cur.execute("SELECT COUNT(*) FROM jobs WHERE scholar_url IS NOT NULL AND scholar_url != '' AND status != 'dismissed'")
scholar = cur.fetchone()[0]
cur.execute("SELECT COUNT(*) FROM jobs WHERE lab_url IS NOT NULL AND lab_url != '' AND status != 'dismissed'")
lab = cur.fetchone()[0]
print(f"\n=== Job Enrichment: {scored} scored, {scholar} scholar_url, {lab} lab_url ===")

cur.execute("SELECT COUNT(*) FROM pis WHERE scholar_url IS NOT NULL AND scholar_url != ''")
pi_scholar = cur.fetchone()[0]
cur.execute("SELECT COUNT(*) FROM pis WHERE lab_url IS NOT NULL AND lab_url != ''")
pi_lab = cur.fetchone()[0]
print(f"=== PI Enrichment: {pi_scholar} scholar_url, {pi_lab} lab_url (of {rec} recommended) ===")

cur.execute("SELECT region, COUNT(*) FROM jobs WHERE status='new' GROUP BY region ORDER BY COUNT(*) DESC")
print("\n=== New Jobs by Region ===")
for r in cur.fetchall():
    print(f"  {(r[0] or 'Unknown'):25s} {r[1]:4d}")
conn.close()
PYEOF
```

Show the output to the user.
