"""Listing repository for deduplication and metadata persistence.

Provides :class:`ListingRepository`, the single data-access object for the
``seen_listings`` SQLite table.  All application state about which listings
have been seen, filtered, and notified lives here.

The repository computes the canonical ``"<source>:<provider_id>"`` key from
each :class:`~rentbot.core.models.Listing` and stores that as the DB primary
key.  This guarantees uniqueness across all providers even if two providers
happen to issue the same numeric local ID.

Typical usage::

    from rentbot.storage.database import open_db
    from rentbot.storage.repository import ListingRepository

    async def run() -> None:
        conn = await open_db()
        repo = ListingRepository(conn)

        if not await repo.exists("immobiliare:12345678"):
            cid = await repo.insert(listing)

        await repo.mark_notified(cid)
        await conn.close()
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime

import aiosqlite

from rentbot.core.exceptions import ListingAlreadyExistsError
from rentbot.core.ids import canonical_id, canonical_id_from_listing
from rentbot.core.models import Listing

__all__ = [
    "canonical_id",
    "canonical_id_from_listing",
    "ListingRepository",
]

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Repository
# ---------------------------------------------------------------------------


class ListingRepository:
    """Data-access object for the ``seen_listings`` table.

    All reads and writes to the deduplication / metadata store go through
    this class.  It owns no connection lifecycle — the caller must supply an
    open :class:`aiosqlite.Connection` and close it when done (see
    :func:`~rentbot.storage.database.open_db`).

    The general pipeline contract is:

    1. **Check** — call :meth:`exists` to see if a listing is already stored.
    2. **Store** — call :meth:`insert` *before* running the filter.
    3. **Filter** — apply filter logic externally.
    4. **Update** — call :meth:`update_filter_result` to record the verdict.
    5. **Notify** — if the filter passes, send alert then call
       :meth:`mark_notified`.

    For seed / bulk ingestion, use :meth:`bulk_insert` instead of individual
    :meth:`insert` calls.

    Args:
        conn: Open, configured :class:`aiosqlite.Connection` (WAL mode should
            already be enabled by
            :func:`~rentbot.storage.database.open_db`).
    """

    def __init__(self, conn: aiosqlite.Connection) -> None:
        self._conn = conn

    # ------------------------------------------------------------------
    # Read helpers
    # ------------------------------------------------------------------

    async def exists(self, cid: str) -> bool:
        """Return ``True`` if a canonical listing ID is already stored.

        Args:
            cid: Canonical key of the form ``"<source>:<provider_id>"``.

        Returns:
            ``True`` if a row with this ``id`` exists, ``False`` otherwise.
        """
        cursor = await self._conn.execute(
            "SELECT 1 FROM seen_listings WHERE id = ? LIMIT 1",
            (cid,),
        )
        row = await cursor.fetchone()
        return row is not None

    # ------------------------------------------------------------------
    # Write helpers
    # ------------------------------------------------------------------

    async def insert(
        self,
        listing: Listing,
        filter_result: str | None = None,
    ) -> str:
        """Insert a new listing row and return its canonical ID.

        This is the primary write path for the ingestion pipeline.  **Call
        before running the filter** so the listing is persisted regardless of
        whether an alert is ultimately sent.

        Args:
            listing: Normalised listing to persist.
            filter_result: Optional pre-computed filter verdict
                (``"pass"`` or ``"block:<reason>"``).  Usually ``None`` at
                insert time — set later via :meth:`update_filter_result`.

        Returns:
            The canonical ID string used as the DB primary key.

        Raises:
            :exc:`~rentbot.core.exceptions.ListingAlreadyExistsError`:
                If a row with the same canonical ID already exists.
        """
        cid = canonical_id_from_listing(listing)

        if await self.exists(cid):
            raise ListingAlreadyExistsError(cid)

        now_utc = datetime.now(UTC).isoformat()
        raw_json = listing.model_dump_json()

        await self._conn.execute(
            """
            INSERT INTO seen_listings
                (id, source, title, price, url, notified, filter_result, raw_json, date_added)
            VALUES
                (?, ?, ?, ?, ?, 0, ?, ?, ?)
            """,
            (
                cid,
                str(listing.source),
                listing.title,
                listing.price,
                listing.url,
                filter_result,
                raw_json,
                now_utc,
            ),
        )
        await self._conn.commit()

        logger.debug(
            "Inserted listing %s (source=%s price=%s)",
            cid,
            listing.source,
            listing.price,
        )
        return cid

    async def bulk_insert(
        self,
        listings: list[Listing],
        filter_result: str | None = None,
    ) -> list[str]:
        """Insert multiple listings efficiently and return the new ones.

        Determines which listings are already seen *before* the insert, then
        inserts only the new ones in a single ``executemany`` call.  This is
        the preferred path for seed mode and bulk-ingestion scenarios.

        Args:
            listings: Batch of normalised listings to persist.
            filter_result: Optional filter verdict applied uniformly to all
                newly inserted rows.  Usually ``None`` — filter is applied
                after ingestion.

        Returns:
            Canonical IDs of listings that were **newly** inserted (i.e. not
            previously seen).  Already-seen listings are silently skipped.
        """
        if not listings:
            return []

        cids = [canonical_id_from_listing(listing) for listing in listings]

        # Determine which canonical IDs are already in the DB before inserting.
        placeholders = ",".join("?" * len(cids))
        cursor = await self._conn.execute(
            f"SELECT id FROM seen_listings WHERE id IN ({placeholders})",
            cids,
        )
        existing_rows = await cursor.fetchall()
        existing: set[str] = {row[0] for row in existing_rows}

        new_pairs = [
            (listing, cid)
            for listing, cid in zip(listings, cids, strict=True)
            if cid not in existing
        ]

        if not new_pairs:
            logger.debug(
                "bulk_insert: all %d listings already seen — nothing inserted",
                len(listings),
            )
            return []

        now_utc = datetime.now(UTC).isoformat()
        rows = [
            (
                cid,
                str(listing.source),
                listing.title,
                listing.price,
                listing.url,
                0,  # notified
                filter_result,
                listing.model_dump_json(),
                now_utc,
            )
            for listing, cid in new_pairs
        ]

        await self._conn.executemany(
            """
            INSERT INTO seen_listings
                (id, source, title, price, url, notified, filter_result, raw_json, date_added)
            VALUES
                (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        await self._conn.commit()

        newly_inserted = [cid for _, cid in new_pairs]
        logger.debug(
            "bulk_insert: %d submitted, %d new, %d already seen",
            len(listings),
            len(newly_inserted),
            len(listings) - len(newly_inserted),
        )
        return newly_inserted

    async def mark_notified(self, cid: str) -> None:
        """Set ``notified = 1`` for the given canonical listing ID.

        Should be called after a Telegram alert has been successfully
        delivered.

        Args:
            cid: Canonical key of the form ``"<source>:<provider_id>"``.
        """
        await self._conn.execute(
            "UPDATE seen_listings SET notified = 1 WHERE id = ?",
            (cid,),
        )
        await self._conn.commit()
        logger.debug("Marked listing %s as notified", cid)

    async def update_filter_result(self, cid: str, filter_result: str) -> None:
        """Record the filter verdict for a previously-inserted listing.

        Called after the filter pipeline evaluates a listing that was stored
        without a verdict at insert time.

        Args:
            cid: Canonical key of the form ``"<source>:<provider_id>"``.
            filter_result: Filter decision string: ``"pass"`` or
                ``"block:<reason>"``.
        """
        await self._conn.execute(
            "UPDATE seen_listings SET filter_result = ? WHERE id = ?",
            (filter_result, cid),
        )
        await self._conn.commit()
        logger.debug("Updated filter_result for %s → %s", cid, filter_result)
