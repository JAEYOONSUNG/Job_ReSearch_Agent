---
allowed-tools: Bash(/home/sunjgaeyoon/Desktop/miniconda3/envs/jobsearch/bin/python3:*)
---

DB 현황 통계 출력. Run:

```bash
cd /home/sunjgaeyoon/Job_ReSearch_Agent && /home/sunjgaeyoon/Desktop/miniconda3/envs/jobsearch/bin/python3 << 'PYEOF'
import sqlite3
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

cur.execute("SELECT COUNT(*) FROM jobs WHERE match_score > 0")
scored = cur.fetchone()[0]
cur.execute("SELECT COUNT(*) FROM jobs WHERE scholar_url IS NOT NULL AND scholar_url != ''")
scholar = cur.fetchone()[0]
print(f"\n=== Enrichment: {scored} scored, {scholar} with Scholar URL ===")
conn.close()
PYEOF
```

Show the output to the user.
