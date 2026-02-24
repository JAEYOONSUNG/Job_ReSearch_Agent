"""One-time DB rescore: recalculate institution tiers from rankings JSON.

Reads all jobs, recalculates tier via get_institution_tier(),
and updates rows where the tier has changed.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
import sqlite3

logging.basicConfig(level=logging.INFO, format="%(levelname)-8s %(message)s")

from src import db
from src.matching.scorer import get_institution_tier

db.init_db()

conn = sqlite3.connect(str(db.DB_PATH))
conn.row_factory = sqlite3.Row

rows = conn.execute(
    "SELECT id, institute, tier FROM jobs WHERE institute IS NOT NULL AND institute != ''"
).fetchall()

updated = 0
for row in rows:
    old_tier = row["tier"]
    new_tier = get_institution_tier(row["institute"])
    # get_institution_tier returns 5 for unranked; keep existing if both are 5/None
    if new_tier != old_tier and new_tier != 5:
        conn.execute("UPDATE jobs SET tier = ? WHERE id = ?", (new_tier, row["id"]))
        logging.info(
            "%-60s  tier %s → %s", row["institute"][:60], old_tier, new_tier
        )
        updated += 1
    elif old_tier is not None and old_tier != 5 and new_tier == 5:
        # Institution was ranked before but now unranked — recalc may be wrong, skip
        pass

conn.commit()
conn.close()

print(f"\n=== Rescored {updated} jobs ===")
