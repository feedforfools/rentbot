"""SQLite database initialisation for Rentbot.

This module is responsible for:

* Opening (or creating) the SQLite file.
* Configuring low-level PRAGMA settings (WAL journal mode, foreign keys).
* Bootstrapping the schema via ``CREATE TABLE IF NOT EXISTS`` — safe to
  call on every startup because the statement is idempotent.

Consumers should call :func:`open_db` once at process startup and share the
returned connection with the repository layer.  The connection must be closed
explicitly (``await conn.close()``) or via an explicit ``async with`` context
manager wrapper in the orchestrator.

Typical usage::

    from rentbot.storage.database import open_db

    async def main() -> None:
        conn = await open_db()          # creates file + schema if absent
        # ... pass conn to ListingRepository ...
        await conn.close()
"""

from __future__ import annotations

import logging
from pathlib import Path

import aiosqlite

__all__ = [
    "DEFAULT_DB_PATH",
    "open_db",
    "create_schema",
]

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------

#: Fallback database path when no explicit path is passed to :func:`open_db`.
#: Resolves to ``rentbot.db`` in the current working directory.
DEFAULT_DB_PATH: Path = Path("rentbot.db")

# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

#: ``seen_listings`` is the deduplication + metadata store.
#:
#: Column notes
#: ------------
#: id            Provider-local listing ID (not the canonical ``source:id`` key
#:               used in the application — the repo layer handles that mapping).
#:               PRIMARY KEY prevents duplicate rows at the DB level.
#: source        Provider name string (immobiliare / casa / subito / …).
#: title         Headline text, stored for human-readable log inspection.
#: price         Monthly rent in EUR (nullable — some sources omit it).
#: url           Canonical link to the listing detail page.
#: notified      Boolean (0/1) — has a Telegram alert been sent for this row?
#: filter_result Free-form string recording why a listing was accepted or
#:               rejected: ``"pass"`` | ``"block:<reason>"``.  NULL means
#:               not yet evaluated.
#: raw_json      Full serialised provider payload, kept for debugging /
#:               re-evaluation if filter criteria change.  NULL acceptable.
#: date_added    ISO-8601 UTC timestamp — set by the INSERT, not by a DB
#:               trigger, so the application controls the value explicitly.
_DDL_SEEN_LISTINGS = """\
CREATE TABLE IF NOT EXISTS seen_listings (
    id            TEXT     NOT NULL,
    source        TEXT     NOT NULL,
    title         TEXT     NOT NULL DEFAULT '',
    price         INTEGER,
    url           TEXT     NOT NULL DEFAULT '',
    notified      INTEGER  NOT NULL DEFAULT 0,
    filter_result TEXT,
    raw_json      TEXT,
    date_added    TEXT     NOT NULL,
    PRIMARY KEY (id)
)"""

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


async def open_db(path: Path | None = None) -> aiosqlite.Connection:
    """Open (or create) the SQLite database and configure it for production.

    Steps performed on every call:

    1. Create parent directories for the DB file if they do not exist.
    2. Open the ``aiosqlite`` connection.
    3. Set ``row_factory = aiosqlite.Row`` so columns can be accessed by name.
    4. Enable WAL journal mode for safe concurrent reads.
    5. Enable foreign-key constraint enforcement.
    6. Call :func:`create_schema` to bootstrap tables (idempotent).

    Args:
        path: Filesystem path for the SQLite file.  Defaults to
            :data:`DEFAULT_DB_PATH` (``rentbot.db`` in the working directory).

    Returns:
        An open, configured :class:`aiosqlite.Connection`.  The caller is
        responsible for closing it.

    Raises:
        aiosqlite.OperationalError: If the database file cannot be opened or
            created (e.g. permission denied on the parent directory).
    """
    db_path = path or DEFAULT_DB_PATH
    db_path = Path(db_path)  # ensure Path even if str slips through
    db_path.parent.mkdir(parents=True, exist_ok=True)

    logger.debug("Opening SQLite database at %s", db_path)

    conn: aiosqlite.Connection = await aiosqlite.connect(db_path)
    conn.row_factory = aiosqlite.Row

    await _configure_pragmas(conn)
    await create_schema(conn)

    logger.info(
        "SQLite database ready at %s (WAL mode enabled, schema verified)", db_path
    )
    return conn


async def create_schema(conn: aiosqlite.Connection) -> None:
    """Create all required tables if they do not already exist.

    This function is idempotent and safe to call on every process startup.
    It does **not** perform any destructive migrations — existing data is
    untouched.

    Args:
        conn: An open :class:`aiosqlite.Connection`.
    """
    await conn.execute(_DDL_SEEN_LISTINGS)
    await conn.commit()
    logger.debug("Schema bootstrap complete (seen_listings table verified)")


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------


async def _configure_pragmas(conn: aiosqlite.Connection) -> None:
    """Apply PRAGMA settings that must be set immediately after opening.

    * ``journal_mode=WAL``:  Write-Ahead Logging allows concurrent readers
      while the single writer is active.  Required by the architecture contract
      (SQLite WAL mode, single writer).
    * ``foreign_keys=ON``:  SQLite disables FK enforcement by default; turn
      it on so any future FK constraints are actually enforced.
    """
    result = await conn.execute("PRAGMA journal_mode=WAL")
    row = await result.fetchone()
    mode = row[0] if row else "unknown"
    if mode != "wal":
        logger.warning(
            "Requested WAL journal mode but SQLite reported: %r. "
            "This may happen for in-memory databases (':memory:').",
            mode,
        )
    else:
        logger.debug("SQLite journal_mode set to WAL")

    await conn.execute("PRAGMA foreign_keys=ON")
    logger.debug("SQLite foreign_keys enforcement enabled")
