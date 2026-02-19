"""One-time DB fix: resolve empty institutes, update tiers, fix aggregators."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
import re
import sqlite3

logging.basicConfig(level=logging.INFO, format="%(levelname)-8s %(name)s: %(message)s")

from src import db
from src.matching.job_enricher import fix_existing_aggregators
from src.matching.scorer import get_institution_tier, guess_country_from_institute, get_region

db.init_db()

# 1. Fix existing aggregator jobs
n = fix_existing_aggregators()
print(f"\n=== Fixed {n} aggregator jobs ===\n")

# 2. Fix empty institute jobs using Organisation/Company pattern
conn = sqlite3.connect("data/jobs.db")
conn.row_factory = sqlite3.Row
empty_inst_rows = conn.execute(
    "SELECT id, description FROM jobs "
    "WHERE (institute IS NULL OR institute = '') "
    "AND description IS NOT NULL AND description != '' "
    "AND status != 'excluded'"
).fetchall()

fixed_empty = 0
for row in empty_inst_rows:
    desc = row["description"]
    m = re.search(
        r"Organisation/Company\s*\n?\s*(.+?)(?:\n|Research Field|Department|$)",
        desc,
    )
    if m:
        org = m.group(1).strip()
        if len(org) >= 3 and org.lower() not in ("n/a", "unknown", "various", "multiple"):
            conn.execute("UPDATE jobs SET institute = ? WHERE id = ?", (org, row["id"]))
            fixed_empty += 1
            print(f"  Fixed empty #{row['id']}: -> {org}")

conn.commit()
print(f"\n=== Fixed {fixed_empty} empty-institute jobs ===\n")

# 3. Re-score tiers for all jobs
rows = conn.execute(
    "SELECT id, institute, tier, country, region FROM jobs "
    "WHERE institute IS NOT NULL AND institute != '' "
    "AND status != 'excluded'"
).fetchall()

tier_fixed = 0
country_fixed = 0
for row in rows:
    inst = row["institute"]
    new_tier = get_institution_tier(inst)
    old_tier = row["tier"] or 5

    updates = []
    params = []

    if new_tier != old_tier:
        updates.append("tier = ?")
        params.append(new_tier)

    if not row["country"]:
        country = guess_country_from_institute(inst)
        if country:
            updates.append("country = ?")
            params.append(country)
            region = get_region(country)
            if region:
                updates.append("region = ?")
                params.append(region)
            country_fixed += 1

    if updates:
        if new_tier != old_tier:
            tier_fixed += 1
        params.append(row["id"])
        sql = f"UPDATE jobs SET {', '.join(updates)} WHERE id = ?"
        conn.execute(sql, params)

conn.commit()
print(f"=== Tier changes: {tier_fixed} jobs ===")
print(f"=== Country fixes: {country_fixed} jobs ===")

# Summary
for row in conn.execute(
    "SELECT tier, COUNT(*) FROM jobs WHERE status != 'excluded' GROUP BY tier ORDER BY tier"
):
    label = f"T{row[0]}" if row[0] and row[0] < 5 else "Unranked"
    print(f"  {label}: {row[1]} jobs")

for row in conn.execute(
    "SELECT institute, COUNT(*) as cnt FROM jobs "
    "WHERE institute IN ('Nature Careers', 'Inside Higher Ed', 'Times Higher Education') "
    "AND status != 'excluded' GROUP BY institute"
):
    print(f"  Still unresolved: {row[0]} = {row[1]}")

for row in conn.execute(
    "SELECT COUNT(*) FROM jobs WHERE (institute IS NULL OR institute = '') AND status != 'excluded'"
):
    print(f"  Empty institute: {row[0]}")

conn.close()
