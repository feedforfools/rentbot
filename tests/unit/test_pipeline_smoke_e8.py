"""Integration smoke flow — E8-T1.

Full pipeline: controlled fake providers → real SQLite dedup → real heuristic
filter → dry-run notification.

This test suite validates that every stage of the processing pipeline is
correctly wired end-to-end.  It differs from the unit tests in
``test_providers_e3.py`` and ``test_orchestrator_e6.py`` in that:

* The SQLite **database is real** (temp file, WAL mode, full repository API).
* The :class:`~rentbot.filters.heuristic.HeuristicFilter` is **real** (not
  mocked).
* The :class:`~rentbot.notifiers.notifier.Notifier` is **real** in dry-run /
  seed mode — it processes every call using the real code-path but never
  emits an HTTP request.
* Providers are **controlled fakes** — concrete
  :class:`~rentbot.providers.base.BaseProvider` subclasses with
  deterministic, configurable return values.

All external network I/O is absent. The :class:`TelegramClient` is
constructed with placeholder credentials; in dry-run mode it never calls
``send_message``, so no HTTP connection is attempted.

Scenarios
---------
1. New listing within criteria → stored + dry-run alerted (``alerted=1``).
2. Duplicate detection → same listing re-submitted in a second cycle is
   skipped by the dedup check (``duplicate=1``, ``alerted=0``).
3. Listing outside price range → inserted into DB but not alerted
   (``passed_filter=0``, ``alerted=0``).
4. Empty provider → completes cleanly with all counters at zero.
5. Mixed batch — passing + filtered + duplicate in one cycle → aggregated
   stats are correct (``new=2``, ``duplicate=1``, ``passed_filter=1``,
   ``alerted=1``).
6. Provider fetch failure → ``provider_failed=True``; a second healthy
   provider in the same cycle still processes its listings.
7. Seed mode → listing passes filter criteria but ``ctx.seed=True`` →
   ``new=1``, ``passed_filter=1``, ``alerted=0``.
8. Multi-provider cycle → two providers contribute independent listings;
   aggregated stats cover both.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncGenerator
from datetime import UTC, datetime
from pathlib import Path

import pytest

from rentbot.core.criteria import FilterCriteria
from rentbot.core.models import Listing, ListingSource
from rentbot.core.run_context import RunContext
from rentbot.filters.heuristic import HeuristicFilter
from rentbot.notifiers.notifier import Notifier
from rentbot.notifiers.telegram import TelegramClient
from rentbot.orchestrator.pipeline import run_cycle
from rentbot.providers.base import BaseProvider
from rentbot.storage.database import open_db
from rentbot.storage.repository import ListingRepository

__all__: list[str] = []

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Shared filter criteria
# ---------------------------------------------------------------------------

#: Criteria that accepts listings with price 400–900 and 1–4 rooms.
_PERMISSIVE_CRITERIA = FilterCriteria(
    price_min=400,
    price_max=900,
    rooms_min=1,
    rooms_max=4,
)

#: Listing whose price is within _PERMISSIVE_CRITERIA bounds.
_PRICE_WITHIN: int = 650
#: Listing whose price exceeds _PERMISSIVE_CRITERIA price_max.
_PRICE_TOO_HIGH: int = 1_200


# ---------------------------------------------------------------------------
# Fake provider implementation
# ---------------------------------------------------------------------------


class _FakeProvider(BaseProvider):
    """Deterministic ``BaseProvider`` that returns a pre-set list of listings.

    Unlike a plain ``AsyncMock``, this is a real ``BaseProvider`` subclass so
    the pipeline correctly resolves ``provider.source`` and stats labels.

    Args:
        source: Provider source identity.
        listings: Fixed list returned by every :meth:`fetch_latest` call.
        raise_on_fetch: When ``True``, :meth:`fetch_latest` raises
            ``RuntimeError`` to simulate a provider-level fetch failure.
    """

    def __init__(
        self,
        source: ListingSource,
        listings: list[Listing],
        *,
        raise_on_fetch: bool = False,
    ) -> None:
        self._source = source
        self._listings = listings
        self._raise_on_fetch = raise_on_fetch

    # BaseProvider requires source as a ClassVar, but for tests we need
    # it to be instance-configurable.  Override the attribute on the
    # instance instead of the class so each _FakeProvider can carry its
    # own source without affecting the class.
    @property  # type: ignore[override]
    def source(self) -> ListingSource:  # type: ignore[override]
        return self._source

    async def fetch_latest(self) -> list[Listing]:
        """Return the fixed listing list or raise if configured to do so."""
        if self._raise_on_fetch:
            raise RuntimeError("Simulated provider fetch failure")
        return self._listings


# ---------------------------------------------------------------------------
# Listing factory helper
# ---------------------------------------------------------------------------


def _make_listing(
    *,
    listing_id: str = "test-001",
    source: ListingSource = ListingSource.IMMOBILIARE,
    price: int = _PRICE_WITHIN,
    rooms: int = 2,
    area_sqm: int = 65,
    title: str = "Bilocale accogliente",
    url: str = "https://www.immobiliare.it/annunci/test001/",
    furnished: bool | None = True,
    zone: str | None = "Centro",
) -> Listing:
    """Build a minimal but fully-valid :class:`Listing` for pipeline tests."""
    return Listing(
        id=listing_id,
        source=source,
        title=title,
        price=price,
        rooms=rooms,
        area_sqm=area_sqm,
        address=None,
        zone=zone,
        furnished=furnished,
        url=url,
        image_url=None,
        description="Appartamento in ottimo stato.",
        listing_date=datetime(2026, 3, 1, 10, 0, 0, tzinfo=UTC),
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
async def db_conn(tmp_path: Path) -> AsyncGenerator:
    """Open a real SQLite database in a temp directory with WAL mode."""
    db_path = tmp_path / "smoke_test.db"
    conn = await open_db(db_path)
    yield conn
    await conn.close()


@pytest.fixture()
def repo(db_conn) -> ListingRepository:
    """Return a real :class:`ListingRepository` backed by the temp database."""
    return ListingRepository(db_conn)


@pytest.fixture()
def hf() -> HeuristicFilter:
    """Return a real :class:`HeuristicFilter` using permissive test criteria."""
    return HeuristicFilter(_PERMISSIVE_CRITERIA)


@pytest.fixture()
async def dry_run_notifier() -> AsyncGenerator[Notifier, None]:
    """Return a real :class:`Notifier` in dry-run mode (no HTTP calls)."""
    ctx = RunContext(seed=False, dry_run=True)
    async with TelegramClient(token="placeholder:dry_run", chat_id="0") as client:
        yield Notifier(client=client, ctx=ctx)


@pytest.fixture()
async def seed_notifier() -> AsyncGenerator[Notifier, None]:
    """Return a real :class:`Notifier` in seed mode (no alerts emitted)."""
    ctx = RunContext(seed=True, dry_run=False)
    async with TelegramClient(token="placeholder:seed", chat_id="0") as client:
        yield Notifier(client=client, ctx=ctx)


# ---------------------------------------------------------------------------
# Scenario 1 — New listing passes filter: stored + dry-run alert counted
# ---------------------------------------------------------------------------


class TestNewListingPassesFilter:
    """Scenario 1: single fresh listing within criteria → alerted once."""

    async def test_fetched_and_alerted(
        self,
        repo: ListingRepository,
        hf: HeuristicFilter,
        dry_run_notifier: Notifier,
    ) -> None:
        ctx = RunContext(seed=False, dry_run=True)
        listing = _make_listing(listing_id="s1-pass", price=_PRICE_WITHIN)
        provider = _FakeProvider(ListingSource.IMMOBILIARE, [listing])

        stats = await run_cycle(
            providers=[provider],
            repo=repo,
            hf=hf,
            notifier=dry_run_notifier,
            ctx=ctx,
        )

        assert stats.total_fetched == 1
        assert stats.total_new == 1
        assert stats.total_duplicate == 0
        assert stats.total_passed_filter == 1
        assert stats.total_alerted == 1
        assert stats.total_errors == 0
        assert stats.failed_providers == []

    async def test_listing_is_stored_in_db(
        self,
        repo: ListingRepository,
        hf: HeuristicFilter,
        dry_run_notifier: Notifier,
    ) -> None:
        """The listing must be present in the DB after the cycle."""
        ctx = RunContext(seed=False, dry_run=True)
        listing = _make_listing(listing_id="s1-store", price=_PRICE_WITHIN)
        provider = _FakeProvider(ListingSource.IMMOBILIARE, [listing])

        await run_cycle(providers=[provider], repo=repo, hf=hf, notifier=dry_run_notifier, ctx=ctx)

        cid = "immobiliare:s1-store"
        assert await repo.exists(cid), "Listing should be stored in the dedup DB"


# ---------------------------------------------------------------------------
# Scenario 2 — Duplicate detection
# ---------------------------------------------------------------------------


class TestDuplicateDetection:
    """Scenario 2: same listing submitted twice → second run is deduplicated."""

    async def test_second_cycle_deduplicates(
        self,
        repo: ListingRepository,
        hf: HeuristicFilter,
        dry_run_notifier: Notifier,
    ) -> None:
        ctx = RunContext(seed=False, dry_run=True)
        listing = _make_listing(listing_id="s2-dup")
        provider = _FakeProvider(ListingSource.IMMOBILIARE, [listing])

        # First cycle — should be new.
        stats1 = await run_cycle(
            providers=[provider], repo=repo, hf=hf, notifier=dry_run_notifier, ctx=ctx
        )
        assert stats1.total_new == 1
        assert stats1.total_duplicate == 0
        assert stats1.total_alerted == 1

        # Second cycle — same provider returning the same listing.
        stats2 = await run_cycle(
            providers=[provider], repo=repo, hf=hf, notifier=dry_run_notifier, ctx=ctx
        )
        assert stats2.total_fetched == 1
        assert stats2.total_new == 0
        assert stats2.total_duplicate == 1
        assert stats2.total_alerted == 0

    async def test_dedup_does_not_double_count_errors(
        self,
        repo: ListingRepository,
        hf: HeuristicFilter,
        dry_run_notifier: Notifier,
    ) -> None:
        """Dedup path must not increment the error counter."""
        ctx = RunContext(seed=False, dry_run=True)
        listing = _make_listing(listing_id="s2-err")
        provider = _FakeProvider(ListingSource.IMMOBILIARE, [listing])

        # Prime the DB.
        await run_cycle(providers=[provider], repo=repo, hf=hf, notifier=dry_run_notifier, ctx=ctx)
        # Second run — duplicate must not register as an error.
        stats2 = await run_cycle(
            providers=[provider], repo=repo, hf=hf, notifier=dry_run_notifier, ctx=ctx
        )
        assert stats2.total_errors == 0


# ---------------------------------------------------------------------------
# Scenario 3 — Listing outside filter criteria: stored but not alerted
# ---------------------------------------------------------------------------


class TestListingFailsFilter:
    """Scenario 3: listing exceeds price cap → stored in DB, not alerted."""

    async def test_stored_but_not_alerted(
        self,
        repo: ListingRepository,
        hf: HeuristicFilter,
        dry_run_notifier: Notifier,
    ) -> None:
        ctx = RunContext(seed=False, dry_run=True)
        listing = _make_listing(listing_id="s3-filtered", price=_PRICE_TOO_HIGH)
        provider = _FakeProvider(ListingSource.IMMOBILIARE, [listing])

        stats = await run_cycle(
            providers=[provider], repo=repo, hf=hf, notifier=dry_run_notifier, ctx=ctx
        )

        assert stats.total_fetched == 1
        assert stats.total_new == 1, "Filtered listings must still be stored"
        assert stats.total_passed_filter == 0
        assert stats.total_alerted == 0
        assert stats.total_errors == 0

    async def test_filtered_listing_is_in_db(
        self,
        repo: ListingRepository,
        hf: HeuristicFilter,
        dry_run_notifier: Notifier,
    ) -> None:
        """Filtered listing must exist in DB (store-before-filter contract)."""
        ctx = RunContext(seed=False, dry_run=True)
        listing = _make_listing(listing_id="s3-indb", price=_PRICE_TOO_HIGH)
        provider = _FakeProvider(ListingSource.IMMOBILIARE, [listing])

        await run_cycle(providers=[provider], repo=repo, hf=hf, notifier=dry_run_notifier, ctx=ctx)

        cid = "immobiliare:s3-indb"
        assert await repo.exists(cid), (
            "Filtered listing must be in the DB even though it did not generate an alert"
        )


# ---------------------------------------------------------------------------
# Scenario 4 — Empty provider
# ---------------------------------------------------------------------------


class TestEmptyProvider:
    """Scenario 4: provider returns an empty list → all counters zero, no error."""

    async def test_zero_counts_no_errors(
        self,
        repo: ListingRepository,
        hf: HeuristicFilter,
        dry_run_notifier: Notifier,
    ) -> None:
        ctx = RunContext(seed=False, dry_run=True)
        provider = _FakeProvider(ListingSource.CASA, [])

        stats = await run_cycle(
            providers=[provider], repo=repo, hf=hf, notifier=dry_run_notifier, ctx=ctx
        )

        ps = stats.provider_stats[0]
        assert ps.fetched == 0
        assert ps.new == 0
        assert ps.passed_filter == 0
        assert ps.alerted == 0
        assert ps.errors == 0
        assert not ps.provider_failed


# ---------------------------------------------------------------------------
# Scenario 5 — Mixed batch
# ---------------------------------------------------------------------------


class TestMixedBatch:
    """Scenario 5: passing + filtered + duplicate listing in one cycle."""

    async def test_aggregated_stats(
        self,
        repo: ListingRepository,
        hf: HeuristicFilter,
        dry_run_notifier: Notifier,
    ) -> None:
        ctx = RunContext(seed=False, dry_run=True)

        passing = _make_listing(
            listing_id="s5-pass",
            price=_PRICE_WITHIN,
            url="https://www.immobiliare.it/annunci/s5-pass/",
        )
        too_expensive = _make_listing(
            listing_id="s5-filtered",
            price=_PRICE_TOO_HIGH,
            url="https://www.immobiliare.it/annunci/s5-filtered/",
        )
        duplicate_seed = _make_listing(
            listing_id="s5-dup",
            url="https://www.immobiliare.it/annunci/s5-dup/",
        )

        # Pre-seed the DB so that duplicate_seed is already known.
        seed_provider = _FakeProvider(ListingSource.IMMOBILIARE, [duplicate_seed])
        await run_cycle(
            providers=[seed_provider],
            repo=repo,
            hf=hf,
            notifier=dry_run_notifier,
            ctx=ctx,
        )

        # Main mixed-batch cycle.
        mixed_provider = _FakeProvider(
            ListingSource.IMMOBILIARE, [passing, too_expensive, duplicate_seed]
        )
        stats = await run_cycle(
            providers=[mixed_provider],
            repo=repo,
            hf=hf,
            notifier=dry_run_notifier,
            ctx=ctx,
        )

        assert stats.total_fetched == 3
        assert stats.total_new == 2, "passing + filtered; duplicate is skipped"
        assert stats.total_duplicate == 1
        assert stats.total_passed_filter == 1, "only 'passing' clears the filter"
        assert stats.total_alerted == 1
        assert stats.total_errors == 0


# ---------------------------------------------------------------------------
# Scenario 6 — Provider fetch failure
# ---------------------------------------------------------------------------


class TestProviderFetchFailure:
    """Scenario 6: one provider raises during fetch; others still process."""

    async def test_failed_provider_marked_and_second_runs(
        self,
        repo: ListingRepository,
        hf: HeuristicFilter,
        dry_run_notifier: Notifier,
    ) -> None:
        ctx = RunContext(seed=False, dry_run=True)
        broken = _FakeProvider(ListingSource.IMMOBILIARE, [], raise_on_fetch=True)
        healthy_listing = _make_listing(
            listing_id="s6-healthy",
            source=ListingSource.CASA,
            url="https://www.casa.it/annunci/s6-healthy/",
        )
        healthy = _FakeProvider(ListingSource.CASA, [healthy_listing])

        stats = await run_cycle(
            providers=[broken, healthy],
            repo=repo,
            hf=hf,
            notifier=dry_run_notifier,
            ctx=ctx,
        )

        assert ListingSource.IMMOBILIARE in stats.failed_providers
        assert ListingSource.CASA not in stats.failed_providers

        # The healthy provider's listing must still be processed.
        casa_ps = next(ps for ps in stats.provider_stats if ps.source == ListingSource.CASA)
        assert casa_ps.new == 1
        assert casa_ps.alerted == 1

    async def test_failed_provider_does_not_raise(
        self,
        repo: ListingRepository,
        hf: HeuristicFilter,
        dry_run_notifier: Notifier,
    ) -> None:
        """run_cycle must not propagate the provider's exception to the caller."""
        ctx = RunContext(seed=False, dry_run=True)
        broken = _FakeProvider(ListingSource.IMMOBILIARE, [], raise_on_fetch=True)

        # This should not raise.
        stats = await run_cycle(
            providers=[broken],
            repo=repo,
            hf=hf,
            notifier=dry_run_notifier,
            ctx=ctx,
        )
        assert stats.failed_providers == [str(ListingSource.IMMOBILIARE)]


# ---------------------------------------------------------------------------
# Scenario 7 — Seed mode
# ---------------------------------------------------------------------------


class TestSeedMode:
    """Scenario 7: ctx.seed=True → listings stored and filtered but not alerted."""

    async def test_seed_stores_but_does_not_alert(
        self,
        repo: ListingRepository,
        hf: HeuristicFilter,
        seed_notifier: Notifier,
    ) -> None:
        ctx = RunContext(seed=True, dry_run=False)
        listing = _make_listing(listing_id="s7-seed", price=_PRICE_WITHIN)
        provider = _FakeProvider(ListingSource.IMMOBILIARE, [listing])

        stats = await run_cycle(
            providers=[provider],
            repo=repo,
            hf=hf,
            notifier=seed_notifier,
            ctx=ctx,
        )

        assert stats.total_new == 1
        assert stats.total_passed_filter == 1, "filter still evaluates in seed mode"
        assert stats.total_alerted == 0, "seed mode must suppress all alerts"

    async def test_seed_listing_is_stored_in_db(
        self,
        repo: ListingRepository,
        hf: HeuristicFilter,
        seed_notifier: Notifier,
    ) -> None:
        ctx = RunContext(seed=True, dry_run=False)
        listing = _make_listing(listing_id="s7-db", price=_PRICE_WITHIN)
        provider = _FakeProvider(ListingSource.IMMOBILIARE, [listing])

        await run_cycle(providers=[provider], repo=repo, hf=hf, notifier=seed_notifier, ctx=ctx)

        assert await repo.exists("immobiliare:s7-db")


# ---------------------------------------------------------------------------
# Scenario 8 — Multi-provider cycle
# ---------------------------------------------------------------------------


class TestMultiProviderCycle:
    """Scenario 8: two providers with independent listings → correct aggregates."""

    async def test_two_providers_aggregate_stats(
        self,
        repo: ListingRepository,
        hf: HeuristicFilter,
        dry_run_notifier: Notifier,
    ) -> None:
        ctx = RunContext(seed=False, dry_run=True)
        immo_listing = _make_listing(
            listing_id="s8-immo",
            source=ListingSource.IMMOBILIARE,
            price=_PRICE_WITHIN,
            url="https://www.immobiliare.it/annunci/s8-immo/",
        )
        casa_listing = _make_listing(
            listing_id="s8-casa",
            source=ListingSource.CASA,
            price=_PRICE_WITHIN,
            url="https://www.casa.it/annunci/s8-casa/",
        )
        immo_provider = _FakeProvider(ListingSource.IMMOBILIARE, [immo_listing])
        casa_provider = _FakeProvider(ListingSource.CASA, [casa_listing])

        stats = await run_cycle(
            providers=[immo_provider, casa_provider],
            repo=repo,
            hf=hf,
            notifier=dry_run_notifier,
            ctx=ctx,
        )

        assert stats.total_fetched == 2
        assert stats.total_new == 2
        assert stats.total_passed_filter == 2
        assert stats.total_alerted == 2
        assert len(stats.provider_stats) == 2

    async def test_each_provider_has_its_own_stats_entry(
        self,
        repo: ListingRepository,
        hf: HeuristicFilter,
        dry_run_notifier: Notifier,
    ) -> None:
        """Every provider in the cycle must produce its own stats record."""
        ctx = RunContext(seed=False, dry_run=True)
        immo_provider = _FakeProvider(
            ListingSource.IMMOBILIARE,
            [
                _make_listing(
                    listing_id="s8-sep-immo",
                    source=ListingSource.IMMOBILIARE,
                    url="https://www.immobiliare.it/annunci/s8-sep-immo/",
                )
            ],
        )
        casa_provider = _FakeProvider(
            ListingSource.CASA,
            [
                _make_listing(
                    listing_id="s8-sep-casa",
                    source=ListingSource.CASA,
                    url="https://www.casa.it/annunci/s8-sep-casa/",
                )
            ],
        )

        stats = await run_cycle(
            providers=[immo_provider, casa_provider],
            repo=repo,
            hf=hf,
            notifier=dry_run_notifier,
            ctx=ctx,
        )

        sources = {ps.source for ps in stats.provider_stats}
        assert str(ListingSource.IMMOBILIARE) in sources
        assert str(ListingSource.CASA) in sources

    async def test_providers_do_not_deduplicate_across_sources(
        self,
        repo: ListingRepository,
        hf: HeuristicFilter,
        dry_run_notifier: Notifier,
    ) -> None:
        """Same provider-local ID from different sources must be treated as distinct."""
        ctx = RunContext(seed=False, dry_run=True)
        # Both share the same raw ID but different sources → different canonical IDs.
        shared_raw_id = "shared-99999"
        immo_listing = _make_listing(
            listing_id=shared_raw_id,
            source=ListingSource.IMMOBILIARE,
            url="https://www.immobiliare.it/annunci/shared99999/",
        )
        casa_listing = _make_listing(
            listing_id=shared_raw_id,
            source=ListingSource.CASA,
            url="https://www.casa.it/annunci/shared99999/",
        )
        immo_provider = _FakeProvider(ListingSource.IMMOBILIARE, [immo_listing])
        casa_provider = _FakeProvider(ListingSource.CASA, [casa_listing])

        stats = await run_cycle(
            providers=[immo_provider, casa_provider],
            repo=repo,
            hf=hf,
            notifier=dry_run_notifier,
            ctx=ctx,
        )

        # Both should be treated as new (canonical IDs differ: "immobiliare:shared-99999"
        # vs "casa:shared-99999").
        assert stats.total_new == 2
        assert stats.total_duplicate == 0


# ---------------------------------------------------------------------------
# Scenario 9 — CycleStats format_cycle_report integration
# ---------------------------------------------------------------------------


class TestCycleReportIntegration:
    """Verify format_cycle_report output reflects real pipeline outcomes."""

    async def test_report_contains_correct_counts(
        self,
        repo: ListingRepository,
        hf: HeuristicFilter,
        dry_run_notifier: Notifier,
    ) -> None:
        ctx = RunContext(seed=False, dry_run=True)
        listing = _make_listing(listing_id="s9-report", price=_PRICE_WITHIN)
        provider = _FakeProvider(ListingSource.IMMOBILIARE, [listing])

        stats = await run_cycle(
            providers=[provider], repo=repo, hf=hf, notifier=dry_run_notifier, ctx=ctx
        )
        report = stats.format_cycle_report()

        assert "fetched=1" in report
        assert "new=1" in report
        assert "passed_filter=1" in report
        assert "alerted=1" in report
        assert "errors=0" in report
