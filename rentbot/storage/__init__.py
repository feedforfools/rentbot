"""SQLite-backed repository for deduplication and listing metadata persistence."""

from rentbot.storage.database import DEFAULT_DB_PATH, create_schema, open_db

__all__ = [
    "DEFAULT_DB_PATH",
    "open_db",
    "create_schema",
]
