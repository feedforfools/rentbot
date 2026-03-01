"""Single-cycle pipeline: fetch → dedup → store → filter → notify.

This module implements the core processing loop for one poll cycle.
It is **provider-agnostic**: callers supply any list of
:class:`~rentbot.providers.base.BaseProvider` instances and the pipeline
threads each listing they emit through the following stages:

1. **Fetch** — call :meth:`~rentbot.providers.base.BaseProvider.fetch_latest`
   to retrieve the newest listings from the provider.
2. **Dedup** — check :meth:`~rentbot.storage.repository.ListingRepository.exists`
   (canonical ``source:id`` key); skip already-seen listings.
3. **Store** — call :meth:`~rentbot.storage.repository.ListingRepository.insert`
   *before* filtering (constraint: store before filter).
4. **Filter** — run :class:`~rentbot.filters.heuristic.HeuristicFilter` and
   persist the verdict via
   :meth:`~rentbot.storage.repository.ListingRepository.update_filter_result`.
5. **Notify** — if the listing passes the filter, dispatch an alert via
   :meth:`~rentbot.notifiers.notifier.Notifier.send_alert` and mark the
   row as notified on success.

Provider failures are **isolated**: an exception from one provider's fetch
call is caught and logged; all other providers continue unaffected.  Per-
listing errors are likewise isolated so a single malformed payload cannot
abort a batch.

Concurrency model
-----------------
:func:`run_cycle` runs all providers concurrently via
``asyncio.gather(..., return_exceptions=True)``.  Each provider's results are
then processed serially within its own coroutine to keep SQLite writes from
the same process serialised (no concurrent writes to the same connection).

Typical usage::

    from rentbot.orchestrator.pipeline import run_cycle, CycleStats
    from rentbot.core.settings import Settings
    from rentbot.core.run_context import RunContext
    from rentbot.filters.heuristic import HeuristicFilter
    from rentbot.notifiers.notifier import Notifier
    from rentbot.notifiers.telegram import TelegramClient
    from rentbot.providers.api.immobiliare import ImmobiliareProvider
    from rentbot.storage.database import open_db
    from rentbot.storage.repository import ListingRepository

    async def main() -> None:
        settings = Settings()
        ctx = RunContext(seed=False, dry_run=True)
        criteria = settings.to_filter_criteria()
        hf = HeuristicFilter(criteria)

        conn = await open_db()
        repo = ListingRepository(conn)

        async with TelegramClient(...) as client:
            notifier = Notifier(client=client, ctx=ctx)
            async with ImmobiliareProvider(settings) as provider:
                stats = await run_cycle(
                    providers=[provider],
                    repo=repo,
                    hf=hf,
                    notifier=notifier,
                    ctx=ctx,
                )
        await conn.close()
        print(stats)
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field

from rentbot.core.exceptions import ListingAlreadyExistsError
from rentbot.core.models import Listing, ListingSource
from rentbot.core.run_context import RunContext
from rentbot.filters.heuristic import HeuristicFilter
from rentbot.notifiers.notifier import Notifier
from rentbot.providers.base import BaseProvider
from rentbot.storage.repository import ListingRepository, canonical_id_from_listing

__all__ = [
    "ProviderCycleStats",
    "CycleStats",
    "process_listing",
    "run_provider",
    "run_cycle",
]

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Stats data classes
# ---------------------------------------------------------------------------


@dataclass
class ProviderCycleStats:
    """Counters for one provider's contribution to a single poll cycle.

    Attributes:
        source: Provider source label (e.g. ``"immobiliare"``).
        fetched: Total listings returned by the provider's
            :meth:`~rentbot.providers.base.BaseProvider.fetch_latest` call.
        new: Listings that were not previously in the dedup DB and were
            inserted this cycle.
        duplicate: Listings already present in the DB and skipped.
        passed_filter: Listings that cleared the heuristic filter.
        alerted: Listings for which a Telegram alert was successfully sent
            (or logged in dry-run mode).
        errors: Per-listing processing errors (not including provider-level
            fetch failures).
        provider_failed: ``True`` if the provider itself raised during fetch,
            meaning ``fetched`` / ``new`` / etc. are all 0.
    """

    source: str
    fetched: int = 0
    new: int = 0
    duplicate: int = 0
    passed_filter: int = 0
    alerted: int = 0
    errors: int = 0
    provider_failed: bool = False


@dataclass
class CycleStats:
    """Aggregated statistics for a single full poll cycle.

    Attributes:
        provider_stats: One :class:`ProviderCycleStats` entry per provider
            that participated in the cycle.
    """

    provider_stats: list[ProviderCycleStats] = field(default_factory=list)

    @property
    def total_fetched(self) -> int:
        """Sum of fetched counts across all providers."""
        return sum(p.fetched for p in self.provider_stats)

    @property
    def total_new(self) -> int:
        """Sum of newly-inserted listings across all providers."""
        return sum(p.new for p in self.provider_stats)

    @property
    def total_duplicate(self) -> int:
        """Sum of duplicate/skipped listings across all providers."""
        return sum(p.duplicate for p in self.provider_stats)

    @property
    def total_passed_filter(self) -> int:
        """Sum of listings that cleared the heuristic filter."""
        return sum(p.passed_filter for p in self.provider_stats)

    @property
    def total_alerted(self) -> int:
        """Sum of successfully alertedf listings across all providers."""
        return sum(p.alerted for p in self.provider_stats)

    @property
    def total_errors(self) -> int:
        """Sum of per-listing processing errors across all providers."""
        return sum(p.errors for p in self.provider_stats)

    @property
    def failed_providers(self) -> list[str]:
        """Source labels of providers whose fetch call raised an exception."""
        return [p.source for p in self.provider_stats if p.provider_failed]


# ---------------------------------------------------------------------------
# Per-listing processing
# ---------------------------------------------------------------------------


async def process_listing(
    listing: Listing,
    repo: ListingRepository,
    hf: HeuristicFilter,
    notifier: Notifier,
    stats: ProviderCycleStats,
) -> None:
    """Run one listing through the full pipeline: dedup → store → filter → notify.

    This function is safe to call in a loop; failures for one listing do not
    propagate to the caller — exceptions should be caught by the caller
    (i.e. :func:`run_provider`) and counted in ``stats.errors``.

    Pipeline contract (see *_DESCRIPTION.md §4*):
    - **Check before insert**: dedup check happens first.
    - **Store before filter**: the row is written regardless of filter outcome.
    - **Filter before notify**: only passing listings generate alerts.
    - **Notify then mark**: :meth:`~rentbot.storage.repository
      .ListingRepository.mark_notified` is called *after* a successful send.

    Args:
        listing: Normalised listing emitted by a provider.
        repo: Open :class:`~rentbot.storage.repository.ListingRepository`
            instance.
        hf: Configured :class:`~rentbot.filters.heuristic.HeuristicFilter`.
        notifier: :class:`~rentbot.notifiers.notifier.Notifier` that
            respects the current :class:`~rentbot.core.run_context.RunContext`
            (seed / dry-run / live).
        stats: Mutable counter bag updated in-place.
    """
    cid = canonical_id_from_listing(listing)

    # ------------------------------------------------------------------
    # Stage 1 — Deduplication
    # ------------------------------------------------------------------
    if await repo.exists(cid):
        stats.duplicate += 1
        logger.debug("DEDUP  %s — already seen, skipping", cid)
        return

    # ------------------------------------------------------------------
    # Stage 2 — Store (before filter — project constraint #8)
    # ------------------------------------------------------------------
    try:
        await repo.insert(listing)
    except ListingAlreadyExistsError:
        # Race condition: another coroutine (same process) inserted between
        # our exists() check and insert() call.  This should not happen with
        # sequential per-provider processing but guard it anyway.
        stats.duplicate += 1
        logger.debug("DEDUP  %s — insert race, treated as duplicate", cid)
        return

    stats.new += 1

    # ------------------------------------------------------------------
    # Stage 3 — Heuristic filter
    # ------------------------------------------------------------------
    filter_result = hf.evaluate(listing)
    verdict = "pass" if filter_result.passed else f"block:{filter_result.reason}"
    await repo.update_filter_result(cid, verdict)

    if not filter_result.passed:
        logger.debug(
            "FILTER DROP  %s — %s",
            cid,
            filter_result.reason,
        )
        return

    stats.passed_filter += 1

    # ------------------------------------------------------------------
    # Stage 4 — Notify
    # ------------------------------------------------------------------
    try:
        sent = await notifier.send_alert(listing)
    except Exception as exc:  # noqa: BLE001
        # send_alert already logs the error at ERROR/CRITICAL level.
        logger.error(
            "Notification failed for %s — incrementing error counter: %s",
            cid,
            exc,
        )
        stats.errors += 1
        return

    if sent:
        await repo.mark_notified(cid)
        stats.alerted += 1


# ---------------------------------------------------------------------------
# Per-provider runner
# ---------------------------------------------------------------------------


async def run_provider(
    provider: BaseProvider,
    repo: ListingRepository,
    hf: HeuristicFilter,
    notifier: Notifier,
    ctx: RunContext,  # noqa: ARG001  (threaded for symmetry; notifier owns mode)
) -> ProviderCycleStats:
    """Fetch all listings from one provider and push them through the pipeline.

    Provider-level errors (fetch failures) are caught here and reflected in
    :attr:`ProviderCycleStats.provider_failed`.  Per-listing errors bubble up
    from :func:`process_listing` and are caught here as well.

    Args:
        provider: Fully configured provider instance (already inside its async
            context).
        repo: Open repository for dedup + persistence.
        hf: Heuristic filter instance shared across all providers in the cycle.
        notifier: Notifier instance shared across all providers.
        ctx: Runtime context flags (kept for API symmetry; notifier already
            honours them internally).

    Returns:
        A :class:`ProviderCycleStats` with counters for this provider's
        contribution to the cycle.
    """
    source_label = str(provider.source)
    stats = ProviderCycleStats(source=source_label)

    # ------------------------------------------------------------------
    # Fetch
    # ------------------------------------------------------------------
    try:
        listings = await provider.fetch_latest()
    except Exception as exc:  # noqa: BLE001
        logger.error(
            "Provider %s: fetch_latest raised — provider skipped this cycle: %s",
            source_label,
            exc,
            exc_info=True,
        )
        stats.provider_failed = True
        return stats

    stats.fetched = len(listings)
    logger.info(
        "Provider %s: fetched %d listing(s)",
        source_label,
        stats.fetched,
    )

    # ------------------------------------------------------------------
    # Process each listing
    # ------------------------------------------------------------------
    for listing in listings:
        try:
            await process_listing(listing, repo, hf, notifier, stats)
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "Provider %s: unexpected error processing listing %s — skipped: %s",
                source_label,
                listing.id,
                exc,
                exc_info=True,
            )
            stats.errors += 1

    logger.info(
        "Provider %s: done — fetched=%d new=%d dup=%d passed_filter=%d alerted=%d errors=%d",
        source_label,
        stats.fetched,
        stats.new,
        stats.duplicate,
        stats.passed_filter,
        stats.alerted,
        stats.errors,
    )
    return stats


# ---------------------------------------------------------------------------
# Cycle entry-point
# ---------------------------------------------------------------------------


async def run_cycle(
    providers: list[BaseProvider],
    repo: ListingRepository,
    hf: HeuristicFilter,
    notifier: Notifier,
    ctx: RunContext,
) -> CycleStats:
    """Run one full poll cycle across all providers concurrently.

    Providers are dispatched with ``asyncio.gather(..., return_exceptions=True)``
    so that a failing provider does not cancel the others.  Each provider's
    listings are processed serially within that provider's own coroutine so
    that SQLite writes from the same process remain sequential (no concurrent
    access to the same ``aiosqlite`` connection).

    Args:
        providers: List of initialised provider instances.  May be empty
            (returns a :class:`CycleStats` with no providers).
        repo: Shared repository instance (single connection — not shared
            across processes).
        hf: Shared heuristic filter instance (stateless; safe for concurrent
            coroutines).
        notifier: Shared notifier instance (stateless per-call).
        ctx: Runtime context threaded to every provider coroutine.

    Returns:
        A :class:`CycleStats` aggregating counters from every provider.
    """
    if not providers:
        logger.warning("run_cycle called with no providers — nothing to do")
        return CycleStats()

    tasks = [
        run_provider(p, repo, hf, notifier, ctx)
        for p in providers
    ]
    raw_results = await asyncio.gather(*tasks, return_exceptions=True)

    provider_stats: list[ProviderCycleStats] = []
    for provider, result in zip(providers, raw_results):
        source_label = str(provider.source)
        if isinstance(result, BaseException):
            # run_provider itself raised unexpectedly (not the fetch call).
            logger.error(
                "Provider %s: run_provider raised an unexpected exception: %s",
                source_label,
                result,
                exc_info=result,
            )
            provider_stats.append(
                ProviderCycleStats(source=source_label, provider_failed=True)
            )
        else:
            provider_stats.append(result)

    cycle = CycleStats(provider_stats=provider_stats)

    logger.info(
        "Cycle summary: providers=%d fetched=%d new=%d dup=%d "
        "passed_filter=%d alerted=%d errors=%d failed_providers=%s",
        len(providers),
        cycle.total_fetched,
        cycle.total_new,
        cycle.total_duplicate,
        cycle.total_passed_filter,
        cycle.total_alerted,
        cycle.total_errors,
        cycle.failed_providers or "none",
    )

    return cycle
