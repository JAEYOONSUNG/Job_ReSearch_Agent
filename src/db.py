"""SQLite database schema and CRUD operations."""

import json
import logging
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime
from typing import Optional

from src.config import DB_PATH

logger = logging.getLogger(__name__)

# Lock protecting all write operations so that concurrent scrapers don't
# step on each other (SQLite allows concurrent reads but not concurrent writes).
_DB_LOCK = threading.Lock()

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    pi_name TEXT,
    institute TEXT,
    department TEXT,
    country TEXT,
    region TEXT,
    tier INTEGER,
    field TEXT,
    description TEXT,
    url TEXT UNIQUE,
    lab_url TEXT,
    scholar_url TEXT,
    posted_date TEXT,
    deadline TEXT,
    source TEXT,
    h_index INTEGER,
    citations INTEGER,
    match_score REAL DEFAULT 0.0,
    status TEXT DEFAULT 'new',
    notes TEXT,
    discovered_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS pis (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    institute TEXT,
    department TEXT,
    country TEXT,
    region TEXT,
    tier INTEGER,
    scholar_id TEXT,
    semantic_id TEXT,
    scholar_url TEXT,
    lab_url TEXT,
    h_index INTEGER,
    citations INTEGER,
    fields TEXT,
    keywords TEXT,
    is_seed INTEGER DEFAULT 0,
    is_recommended INTEGER DEFAULT 0,
    recommendation_score REAL DEFAULT 0.0,
    connected_seeds TEXT,
    last_scraped TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now')),
    UNIQUE(name, institute)
);

CREATE TABLE IF NOT EXISTS coauthorships (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pi_id_1 INTEGER REFERENCES pis(id),
    pi_id_2 INTEGER REFERENCES pis(id),
    shared_papers INTEGER DEFAULT 0,
    recent_shared_papers INTEGER DEFAULT 0,
    discovered_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS citations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    citing_pi_id INTEGER REFERENCES pis(id),
    cited_pi_id INTEGER REFERENCES pis(id),
    citation_count INTEGER DEFAULT 0,
    discovered_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS watchlist (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pi_name TEXT NOT NULL,
    institute TEXT,
    lab_url TEXT,
    check_frequency TEXT DEFAULT 'daily',
    last_checked TEXT,
    last_content_hash TEXT,
    added_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS scrape_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    status TEXT,
    jobs_found INTEGER DEFAULT 0,
    new_jobs INTEGER DEFAULT 0,
    error TEXT,
    started_at TEXT DEFAULT (datetime('now')),
    finished_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_jobs_url ON jobs(url);
CREATE INDEX IF NOT EXISTS idx_jobs_region ON jobs(region);
CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
CREATE INDEX IF NOT EXISTS idx_pis_name ON pis(name);
CREATE INDEX IF NOT EXISTS idx_pis_is_seed ON pis(is_seed);

-- Composite indexes matching common query patterns
CREATE INDEX IF NOT EXISTS idx_jobs_status_discovered ON jobs(status, discovered_at);
CREATE INDEX IF NOT EXISTS idx_jobs_region_tier_hindex ON jobs(region ASC, tier ASC, h_index DESC);
CREATE INDEX IF NOT EXISTS idx_jobs_discovered_at ON jobs(discovered_at);
CREATE INDEX IF NOT EXISTS idx_pis_name_institute ON pis(name, institute);
CREATE INDEX IF NOT EXISTS idx_pis_recommended_score ON pis(is_recommended, recommendation_score DESC);
CREATE INDEX IF NOT EXISTS idx_pis_seed_score ON pis(is_seed, recommendation_score DESC);
CREATE INDEX IF NOT EXISTS idx_coauthorships_pi1_pi2 ON coauthorships(pi_id_1, pi_id_2);
CREATE INDEX IF NOT EXISTS idx_coauthorships_pi2_pi1 ON coauthorships(pi_id_2, pi_id_1);
CREATE INDEX IF NOT EXISTS idx_citations_citing ON citations(citing_pi_id, cited_pi_id);
CREATE INDEX IF NOT EXISTS idx_citations_cited ON citations(cited_pi_id, citing_pi_id);
CREATE INDEX IF NOT EXISTS idx_watchlist_pi_name ON watchlist(pi_name);
CREATE INDEX IF NOT EXISTS idx_scrape_log_source ON scrape_log(source);
"""

# Columns added after initial schema — applied via ALTER TABLE if missing
_MIGRATIONS = [
    ("jobs", "requirements", "TEXT"),
    ("jobs", "conditions", "TEXT"),
    ("jobs", "keywords", "TEXT"),
    ("jobs", "pi_research_summary", "TEXT"),
    ("jobs", "dept_url", "TEXT"),
    ("pis", "dept_url", "TEXT"),
    ("jobs", "recent_papers", "TEXT"),
    ("jobs", "top_cited_papers", "TEXT"),
    ("pis", "recent_papers", "TEXT"),
    ("pis", "top_cited_papers", "TEXT"),
    ("pis", "s2_author_id", "TEXT"),
]


def _run_migrations(conn: sqlite3.Connection) -> None:
    """Add columns that may be missing from an older schema."""
    for table, column, col_type in _MIGRATIONS:
        try:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")
            logger.info("Migration: added %s.%s", table, column)
        except sqlite3.OperationalError:
            pass  # column already exists


def init_db() -> None:
    """Initialize database with schema."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with get_connection() as conn:
        conn.executescript(SCHEMA)
        _run_migrations(conn)
    logger.info("Database initialized at %s", DB_PATH)


@contextmanager
def get_connection():
    """Context manager for database connections.

    Applies WAL mode and tuned PRAGMAs for optimal read/write concurrency
    and reduced I/O overhead on typical workloads (hundreds to low-thousands
    of rows per table).
    """
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA cache_size=-8000")          # 8 MB page cache
    conn.execute("PRAGMA mmap_size=67108864")         # 64 MB memory-mapped I/O
    conn.execute("PRAGMA journal_size_limit=16777216") # 16 MB WAL size limit
    conn.execute("PRAGMA temp_store=MEMORY")          # temp tables in RAM
    conn.execute("PRAGMA synchronous=NORMAL")         # safe with WAL mode
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ── Job CRUD ───────────────────────────────────────────────────────────────


def upsert_job(job: dict) -> tuple[int, bool]:
    """Insert or update a job. Returns (job_id, is_new).

    Thread-safe: acquires ``_DB_LOCK`` before writing.
    """
    with _DB_LOCK, get_connection() as conn:
        existing = conn.execute(
            "SELECT id FROM jobs WHERE url = ?", (job.get("url"),)
        ).fetchone()

        if existing:
            job_id = existing["id"]
            fields = {k: v for k, v in job.items() if k != "url" and v is not None}
            if fields:
                set_clause = ", ".join(f"{k} = ?" for k in fields)
                values = list(fields.values()) + [job_id]
                conn.execute(
                    f"UPDATE jobs SET {set_clause}, updated_at = datetime('now') WHERE id = ?",
                    values,
                )
            return job_id, False

        cols = [k for k, v in job.items() if v is not None]
        placeholders = ", ".join("?" for _ in cols)
        values = [job[k] for k in cols]
        cursor = conn.execute(
            f"INSERT INTO jobs ({', '.join(cols)}) VALUES ({placeholders})",
            values,
        )
        return cursor.lastrowid, True


def get_jobs(
    region: Optional[str] = None,
    status: Optional[str] = None,
    since: Optional[str] = None,
    limit: int = 500,
) -> list[dict]:
    """Retrieve jobs with optional filters.

    Builds a dynamic WHERE clause so SQLite can use the composite indexes
    ``idx_jobs_status_discovered`` and ``idx_jobs_region_tier_hindex``.
    """
    clauses: list[str] = []
    params: list = []

    if region:
        clauses.append("region = ?")
        params.append(region)
    if status:
        clauses.append("status = ?")
        params.append(status)
    if since:
        clauses.append("discovered_at >= ?")
        params.append(since)

    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
    query = f"SELECT * FROM jobs{where} ORDER BY region ASC, tier ASC, h_index DESC LIMIT ?"
    params.append(limit)

    with get_connection() as conn:
        rows = conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]


def get_new_jobs_since(since: str) -> list[dict]:
    """Get jobs discovered since a given datetime string."""
    return get_jobs(status="new", since=since)


# ── PI CRUD ────────────────────────────────────────────────────────────────


def upsert_pi(pi: dict) -> tuple[int, bool]:
    """Insert or update a PI. Returns (pi_id, is_new)."""
    with get_connection() as conn:
        existing = conn.execute(
            "SELECT id FROM pis WHERE name = ? AND institute = ?",
            (pi.get("name"), pi.get("institute")),
        ).fetchone()

        if existing:
            pi_id = existing["id"]
            fields = {
                k: v
                for k, v in pi.items()
                if k not in ("name", "institute") and v is not None
            }
            if fields:
                set_clause = ", ".join(f"{k} = ?" for k in fields)
                values = list(fields.values()) + [pi_id]
                conn.execute(
                    f"UPDATE pis SET {set_clause}, updated_at = datetime('now') WHERE id = ?",
                    values,
                )
            return pi_id, False

        cols = [k for k, v in pi.items() if v is not None]
        placeholders = ", ".join("?" for _ in cols)
        values = [pi[k] for k in cols]
        cursor = conn.execute(
            f"INSERT INTO pis ({', '.join(cols)}) VALUES ({placeholders})",
            values,
        )
        return cursor.lastrowid, True


def get_seed_pis() -> list[dict]:
    """Get all seed PIs."""
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT * FROM pis WHERE is_seed = 1 ORDER BY h_index DESC"
        ).fetchall()
        return [dict(r) for r in rows]


def get_recommended_pis(min_score: float = 0.0) -> list[dict]:
    """Get recommended PIs above a minimum score."""
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT * FROM pis WHERE is_recommended = 1 AND recommendation_score >= ? "
            "ORDER BY recommendation_score DESC",
            (min_score,),
        ).fetchall()
        return [dict(r) for r in rows]


def get_all_pis() -> list[dict]:
    """Get all PIs (seed + recommended)."""
    with get_connection() as conn:
        rows = conn.execute(
            "SELECT * FROM pis ORDER BY is_seed DESC, recommendation_score DESC"
        ).fetchall()
        return [dict(r) for r in rows]


# ── Coauthorship CRUD ─────────────────────────────────────────────────────


def add_coauthorship(pi_id_1: int, pi_id_2: int, shared_papers: int = 1) -> None:
    """Record a coauthorship relationship.

    Uses UNION of two indexed lookups instead of OR to allow SQLite to
    leverage ``idx_coauthorships_pi1_pi2`` and ``idx_coauthorships_pi2_pi1``.
    """
    with get_connection() as conn:
        existing = conn.execute(
            "SELECT id, shared_papers FROM coauthorships WHERE pi_id_1 = ? AND pi_id_2 = ? "
            "UNION ALL "
            "SELECT id, shared_papers FROM coauthorships WHERE pi_id_1 = ? AND pi_id_2 = ? "
            "LIMIT 1",
            (pi_id_1, pi_id_2, pi_id_2, pi_id_1),
        ).fetchone()

        if existing:
            conn.execute(
                "UPDATE coauthorships SET shared_papers = ?, "
                "recent_shared_papers = recent_shared_papers + 1 WHERE id = ?",
                (max(existing["shared_papers"], shared_papers), existing["id"]),
            )
        else:
            conn.execute(
                "INSERT INTO coauthorships (pi_id_1, pi_id_2, shared_papers, recent_shared_papers) "
                "VALUES (?, ?, ?, ?)",
                (pi_id_1, pi_id_2, shared_papers, shared_papers),
            )


# ── Watchlist CRUD ─────────────────────────────────────────────────────────


def add_to_watchlist(pi_name: str, institute: str = "", lab_url: str = "") -> int:
    """Add a PI to the watchlist."""
    with get_connection() as conn:
        cursor = conn.execute(
            "INSERT OR IGNORE INTO watchlist (pi_name, institute, lab_url) VALUES (?, ?, ?)",
            (pi_name, institute, lab_url),
        )
        return cursor.lastrowid


def get_watchlist() -> list[dict]:
    """Get all watchlist entries."""
    with get_connection() as conn:
        rows = conn.execute("SELECT * FROM watchlist ORDER BY added_at DESC").fetchall()
        return [dict(r) for r in rows]


# ── Scrape Log ─────────────────────────────────────────────────────────────


def log_scrape(source: str, status: str, jobs_found: int = 0, new_jobs: int = 0, error: str = "") -> int:
    """Log a scrape run. Thread-safe."""
    with _DB_LOCK, get_connection() as conn:
        cursor = conn.execute(
            "INSERT INTO scrape_log (source, status, jobs_found, new_jobs, error, finished_at) "
            "VALUES (?, ?, ?, ?, ?, datetime('now'))",
            (source, status, jobs_found, new_jobs, error),
        )
        return cursor.lastrowid
