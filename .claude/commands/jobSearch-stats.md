Show current database statistics. Run:

```python
/Users/JaeYoon/miniconda3/envs/jobsearch/bin/python3 << 'PYEOF'
import sqlite3
conn = sqlite3.connect('data/jobs.db')
cur = conn.cursor()

cur.execute("SELECT source, COUNT(*) FROM jobs GROUP BY source ORDER BY COUNT(*) DESC")
print("=== Jobs by Source ===")
total = 0
for r in cur.fetchall():
    print(f"  {r[0]:25s} {r[1]:4d}")
    total += r[1]
print(f"  {'TOTAL':25s} {total:4d}")

cur.execute("SELECT COUNT(*) FROM jobs WHERE pi_name IS NOT NULL AND pi_name != ''")
pi_count = cur.fetchone()[0]
print(f"\n=== PI Names: {pi_count}/{total} ({100*pi_count//total}%) ===")

cur.execute("SELECT COUNT(*) FROM pis WHERE is_recommended=1")
rec = cur.fetchone()[0]
cur.execute("SELECT COUNT(*) FROM pis WHERE is_seed=1")
seed = cur.fetchone()[0]
print(f"=== PIs: {seed} seeds, {rec} recommended ===")

cur.execute("SELECT COUNT(*) FROM jobs WHERE description LIKE '%...'")
trunc = cur.fetchone()[0]
cur.execute("SELECT COUNT(*) FROM jobs WHERE length(description) < 100")
short = cur.fetchone()[0]
print(f"=== Description health: {trunc} truncated, {short} short ===")
conn.close()
PYEOF
```

Show the output to the user.
