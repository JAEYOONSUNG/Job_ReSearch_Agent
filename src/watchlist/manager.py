"""Manage PI watchlist for targeted monitoring."""

import logging

from src.db import add_to_watchlist, get_watchlist, get_connection

logger = logging.getLogger(__name__)


def add_pi(pi_name: str, institute: str = "", lab_url: str = "") -> int:
    """Add a PI to the watchlist."""
    wid = add_to_watchlist(pi_name, institute, lab_url)
    logger.info("Added to watchlist: %s (%s)", pi_name, institute)
    return wid


def remove_pi(pi_name: str) -> bool:
    """Remove a PI from the watchlist."""
    with get_connection() as conn:
        cursor = conn.execute(
            "DELETE FROM watchlist WHERE pi_name = ?", (pi_name,)
        )
        removed = cursor.rowcount > 0
    if removed:
        logger.info("Removed from watchlist: %s", pi_name)
    else:
        logger.warning("PI not found in watchlist: %s", pi_name)
    return removed


def list_watchlist() -> list[dict]:
    """List all PIs on the watchlist."""
    return get_watchlist()


def update_check(pi_name: str, content_hash: str) -> None:
    """Update last checked time and content hash for a watchlist entry."""
    with get_connection() as conn:
        conn.execute(
            "UPDATE watchlist SET last_checked = datetime('now'), "
            "last_content_hash = ? WHERE pi_name = ?",
            (content_hash, pi_name),
        )
