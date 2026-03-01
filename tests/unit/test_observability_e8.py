"""Unit tests for E8-T3 — structured log event names and correlation IDs.

Coverage
--------
* :class:`~rentbot.core.logging_config.CycleContextFilter` — injects
  ``record.cycle_id`` from the ``CYCLE_ID_CTX`` ContextVar; default is
  ``"-"``; always returns ``True``.
* :data:`~rentbot.core.logging_config.CYCLE_ID_CTX` — ContextVar behaviour:
  default value, set/get/reset lifecycle, token-based restoration.
* :data:`rentbot.core.events` — all exported constants are non-empty strings
  with the expected values.
* :func:`~rentbot.orchestrator.runner.run_once` — generates a unique 8-char
  hex cycle ID per call, sets the ContextVar inside the cycle, and resets it
  after the call returns.
* Two consecutive cycles produce **different** cycle IDs.
* Event name annotations — ``extra={"event": X}`` is present on key log
  records emitted by :func:`~rentbot.orchestrator.pipeline.process_listing`,
  :func:`~rentbot.orchestrator.pipeline.run_provider`, and
  :func:`~rentbot.orchestrator.runner.run_once`.
* :func:`asyncio.gather` propagates the cycle_id ContextVar into child tasks.
"""

from __future__ import annotations

import asyncio
import logging
import re
import tempfile
from collections.abc import AsyncGenerator
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from rentbot.core import events
from rentbot.core.criteria import FilterCriteria
from rentbot.core.logging_config import CYCLE_ID_CTX, CycleContextFilter
from rentbot.core.models import Listing, ListingSource
from rentbot.core.run_context import RunContext
from rentbot.core.settings import Settings
from rentbot.filters.heuristic import HeuristicFilter
from rentbot.notifiers.notifier import Notifier
from rentbot.notifiers.telegram import TelegramClient
from rentbot.orchestrator.pipeline import (
    CycleStats,
    ProviderCycleStats,
    process_listing,
    run_provider,
)
from rentbot.orchestrator.runner import run_once
from rentbot.providers.base import BaseProvider
from rentbot.storage.database import open_db
from rentbot.storage.repository import ListingRepository

__all__: list[str] = []

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Shared test data
# ---------------------------------------------------------------------------

_DRY_RUN_CTX = RunContext(dry_run=True)

_PASSING_CRITERIA = FilterCriteria(price_min=400, price_max=900, rooms_min=1, rooms_max=4)

_LISTING_PASS = Listing(
    id="list-1",
    source=ListingSource.IMMOBILIARE,
    title="Nice flat",
    price=700,
    rooms=2,
    url="https://www.immobiliare.it/annunci/1",
    description="Great flat",
    listing_date=datetime(2026, 3, 1, tzinfo=UTC),
)

_LISTING_FAIL = Listing(
    id="list-expensive",
    source=ListingSource.IMMOBILIARE,
    title="Luxury penthouse",
    price=9999,
    rooms=2,
    url="https://www.immobiliare.it/annunci/2",
    description="Way too expensive",
    listing_date=datetime(2026, 3, 1, tzinfo=UTC),
)


# ---------------------------------------------------------------------------
# Minimal real DB fixture (shared pattern with smoke tests)
# ---------------------------------------------------------------------------


@pytest.fixture()
async def db_and_repo() -> AsyncGenerator[tuple[object, ListingRepository], None]:
    """Provide a real WAL-mode SQLite DB in a temp file."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = Path(f.name)
    conn = await open_db(db_path)
    repo = ListingRepository(conn)
    yield conn, repo
    await conn.close()
    db_path.unlink(missing_ok=True)


@pytest.fixture()
def dry_run_notifier() -> Notifier:
    """Notifier in dry-run mode (never sends HTTP)."""
    client = TelegramClient(token="placeholder:test", chat_id="0")
    return Notifier(client=client, ctx=_DRY_RUN_CTX)


@pytest.fixture()
def passing_hf() -> HeuristicFilter:
    """HeuristicFilter that accepts listings in 400–900 / 1–4 rooms range."""
    return HeuristicFilter(_PASSING_CRITERIA)


# ---------------------------------------------------------------------------
# TestCycleContextFilter
# ---------------------------------------------------------------------------


class TestCycleContextFilter:
    """Unit tests for CycleContextFilter — pure, no I/O."""

    def _make_record(self, msg: str = "test") -> logging.LogRecord:
        return logging.LogRecord(
            name="test.logger",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg=msg,
            args=(),
            exc_info=None,
        )

    def test_default_cycle_id_is_dash(self) -> None:
        """When no cycle is active the filter injects '-'."""
        f = CycleContextFilter()
        record = self._make_record()
        # Ensure no stale value from a previous test.
        token = CYCLE_ID_CTX.set("-")
        try:
            result = f.filter(record)
        finally:
            CYCLE_ID_CTX.reset(token)
        assert result is True
        assert record.cycle_id == "-"  # type: ignore[attr-defined]

    def test_injects_active_cycle_id(self) -> None:
        """When a cycle is active filters set the correct hex ID."""
        f = CycleContextFilter()
        record = self._make_record()
        token = CYCLE_ID_CTX.set("deadbeef")
        try:
            f.filter(record)
        finally:
            CYCLE_ID_CTX.reset(token)
        assert record.cycle_id == "deadbeef"  # type: ignore[attr-defined]

    def test_always_returns_true(self) -> None:
        """Filter never suppresses records — return value is always True."""
        f = CycleContextFilter()
        for level in (logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR):
            record = logging.LogRecord("t", level, "t.py", 1, "msg", (), None)
            assert f.filter(record) is True

    def test_multiple_records_independent(self) -> None:
        """Each record gets the cycle_id that is active at filter time."""
        f = CycleContextFilter()
        record_a = self._make_record("a")
        record_b = self._make_record("b")

        t1 = CYCLE_ID_CTX.set("cycle_a1")
        f.filter(record_a)
        CYCLE_ID_CTX.reset(t1)

        t2 = CYCLE_ID_CTX.set("cycle_b2")
        f.filter(record_b)
        CYCLE_ID_CTX.reset(t2)

        assert record_a.cycle_id == "cycle_a1"  # type: ignore[attr-defined]
        assert record_b.cycle_id == "cycle_b2"  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# TestEventConstants
# ---------------------------------------------------------------------------


class TestEventConstants:
    """Verify all exported event name constants have expected string values."""

    @pytest.mark.parametrize(
        "name,expected",
        [
            ("CYCLE_START", "CYCLE_START"),
            ("CYCLE_COMPLETE", "CYCLE_COMPLETE"),
            ("CYCLE_ABORT", "CYCLE_ABORT"),
            ("PROVIDER_FETCH_OK", "PROVIDER_FETCH_OK"),
            ("PROVIDER_FETCH_ERROR", "PROVIDER_FETCH_ERROR"),
            ("PROVIDER_CIRCUIT_OPEN", "PROVIDER_CIRCUIT_OPEN"),
            ("PROVIDER_INIT_ERROR", "PROVIDER_INIT_ERROR"),
            ("PROVIDER_DONE", "PROVIDER_DONE"),
            ("LISTING_DUPLICATE", "LISTING_DUPLICATE"),
            ("LISTING_NEW", "LISTING_NEW"),
            ("LISTING_FILTERED", "LISTING_FILTERED"),
            ("LISTING_ALERTED", "LISTING_ALERTED"),
            ("LISTING_NOTIFY_ERROR", "LISTING_NOTIFY_ERROR"),
        ],
    )
    def test_event_constant_value(self, name: str, expected: str) -> None:
        """Each constant should equal its own name (for grep-friendliness)."""
        assert getattr(events, name) == expected

    def test_all_exported_constants_are_strings(self) -> None:
        """Every item in __all__ should be a non-empty str constant."""
        for name in events.__all__:
            value = getattr(events, name)
            assert isinstance(value, str) and value, f"events.{name} is empty or not a str"


# ---------------------------------------------------------------------------
# TestCycleIdContextVar
# ---------------------------------------------------------------------------


class TestCycleIdContextVar:
    """CYCLE_ID_CTX ContextVar lifecycle tests."""

    def test_default_is_dash(self) -> None:
        """CYCLE_ID_CTX returns '-' when no value has been set."""
        assert CYCLE_ID_CTX.get("-") == "-"

    def test_set_and_get(self) -> None:
        """Set a value, get it back, then reset."""
        token = CYCLE_ID_CTX.set("abc12345")
        try:
            assert CYCLE_ID_CTX.get("-") == "abc12345"
        finally:
            CYCLE_ID_CTX.reset(token)

    def test_reset_restores_previous(self) -> None:
        """Resetting a token restores the value that was set before set()."""
        assert CYCLE_ID_CTX.get("-") == "-"  # start at default
        token = CYCLE_ID_CTX.set("xyz99")
        assert CYCLE_ID_CTX.get("-") == "xyz99"
        CYCLE_ID_CTX.reset(token)
        assert CYCLE_ID_CTX.get("-") == "-"


# ---------------------------------------------------------------------------
# TestCycleIdInRunOnce
# ---------------------------------------------------------------------------


class TestCycleIdInRunOnce:
    """run_once generates a cycle_id, sets CYCLE_ID_CTX, and resets on exit."""

    def _minimal_settings(self, db_path: Path) -> Settings:
        """Create Settings with no providers and a temp DB path."""
        return Settings(
            telegram_bot_token="placeholder",
            telegram_chat_id="0",
            database_path=str(db_path),
        )

    @pytest.mark.asyncio
    async def test_cycle_id_set_during_execution(self, tmp_path: Path) -> None:
        """CYCLE_ID_CTX has a non-default value while run_once is running."""
        captured: list[str] = []

        async def _capturing_run_cycle(*args: object, **kwargs: object) -> CycleStats:
            captured.append(CYCLE_ID_CTX.get("-"))
            return CycleStats()

        settings = self._minimal_settings(tmp_path / "test.db")
        ctx = RunContext(dry_run=True)

        with (
            patch("rentbot.orchestrator.runner._build_providers", return_value=[]),
            patch("rentbot.orchestrator.runner.run_cycle", _capturing_run_cycle),
        ):
            await run_once(ctx, settings)

        assert len(captured) == 1
        assert captured[0] != "-", "cycle_id should be set during run_once"

    @pytest.mark.asyncio
    async def test_cycle_id_is_8_char_hex(self, tmp_path: Path) -> None:
        """Generated cycle_id is exactly 8 lowercase hex characters."""
        captured: list[str] = []

        async def _capturing_run_cycle(*args: object, **kwargs: object) -> CycleStats:
            captured.append(CYCLE_ID_CTX.get("-"))
            return CycleStats()

        settings = self._minimal_settings(tmp_path / "test.db")
        ctx = RunContext(dry_run=True)

        with (
            patch("rentbot.orchestrator.runner._build_providers", return_value=[]),
            patch("rentbot.orchestrator.runner.run_cycle", _capturing_run_cycle),
        ):
            await run_once(ctx, settings)

        assert re.match(r"^[0-9a-f]{8}$", captured[0])

    @pytest.mark.asyncio
    async def test_two_consecutive_cycles_have_different_ids(self, tmp_path: Path) -> None:
        """Each call to run_once produces a unique cycle ID."""
        captured: list[str] = []

        async def _capturing_run_cycle(*args: object, **kwargs: object) -> CycleStats:
            captured.append(CYCLE_ID_CTX.get("-"))
            return CycleStats()

        settings = self._minimal_settings(tmp_path / "test.db")
        ctx = RunContext(dry_run=True)

        with (
            patch("rentbot.orchestrator.runner._build_providers", return_value=[]),
            patch("rentbot.orchestrator.runner.run_cycle", _capturing_run_cycle),
        ):
            await run_once(ctx, settings)
            await run_once(ctx, settings)

        assert len(captured) == 2
        assert captured[0] != captured[1], "each cycle must have a unique ID"

    @pytest.mark.asyncio
    async def test_cycle_id_reset_after_run_once(self, tmp_path: Path) -> None:
        """CYCLE_ID_CTX is restored to '-' after run_once completes."""
        # Confirm default before.
        assert CYCLE_ID_CTX.get("-") == "-"

        settings = self._minimal_settings(tmp_path / "test.db")
        ctx = RunContext(dry_run=True)

        with (
            patch("rentbot.orchestrator.runner._build_providers", return_value=[]),
            patch("rentbot.orchestrator.runner.run_cycle", AsyncMock(return_value=CycleStats())),
        ):
            await run_once(ctx, settings)

        # Must be restored after the call.
        assert CYCLE_ID_CTX.get("-") == "-"

    @pytest.mark.asyncio
    async def test_cycle_id_reset_even_on_exception(self, tmp_path: Path) -> None:
        """CYCLE_ID_CTX is restored even when run_once raises an exception."""
        assert CYCLE_ID_CTX.get("-") == "-"

        settings = self._minimal_settings(tmp_path / "test.db")
        ctx = RunContext(dry_run=True)

        async def _exploding_run_cycle(*args: object, **kwargs: object) -> CycleStats:
            raise RuntimeError("boom")

        with (
            patch("rentbot.orchestrator.runner._build_providers", return_value=[]),
            patch("rentbot.orchestrator.runner.run_cycle", _exploding_run_cycle),
            pytest.raises(RuntimeError),
        ):
            await run_once(ctx, settings)

        assert CYCLE_ID_CTX.get("-") == "-"


# ---------------------------------------------------------------------------
# TestEventAnnotationsPipeline
# ---------------------------------------------------------------------------


class TestEventAnnotationsPipeline:
    """Verify event=... extra fields on key pipeline log records (via caplog)."""

    @pytest.mark.asyncio
    async def test_listing_duplicate_event(
        self,
        db_and_repo: tuple,
        dry_run_notifier: Notifier,
        passing_hf: HeuristicFilter,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """process_listing for a duplicate emits event=LISTING_DUPLICATE."""
        conn, repo = db_and_repo
        # Insert the listing so it's already in the DB.
        await repo.insert(_LISTING_PASS)

        stats = ProviderCycleStats(source="immobiliare")
        with caplog.at_level(logging.DEBUG, logger="rentbot.orchestrator.pipeline"):
            await process_listing(_LISTING_PASS, repo, passing_hf, dry_run_notifier, stats)

        dup_events = [
            getattr(r, "event", None)
            for r in caplog.records
            if getattr(r, "event", None) == events.LISTING_DUPLICATE
        ]
        assert dup_events, "Expected at least one LISTING_DUPLICATE event log"
        assert stats.duplicate == 1

    @pytest.mark.asyncio
    async def test_listing_new_event(
        self,
        db_and_repo: tuple,
        dry_run_notifier: Notifier,
        passing_hf: HeuristicFilter,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """process_listing for a new listing emits event=LISTING_NEW."""
        conn, repo = db_and_repo
        stats = ProviderCycleStats(source="immobiliare")
        with caplog.at_level(logging.DEBUG, logger="rentbot.orchestrator.pipeline"):
            await process_listing(_LISTING_PASS, repo, passing_hf, dry_run_notifier, stats)

        new_events = [
            getattr(r, "event", None)
            for r in caplog.records
            if getattr(r, "event", None) == events.LISTING_NEW
        ]
        assert new_events, "Expected at least one LISTING_NEW event log"
        assert stats.new == 1

    @pytest.mark.asyncio
    async def test_listing_filtered_event(
        self,
        db_and_repo: tuple,
        dry_run_notifier: Notifier,
        passing_hf: HeuristicFilter,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """process_listing for a filtered listing emits event=LISTING_FILTERED."""
        conn, repo = db_and_repo
        stats = ProviderCycleStats(source="immobiliare")
        with caplog.at_level(logging.DEBUG, logger="rentbot.orchestrator.pipeline"):
            await process_listing(_LISTING_FAIL, repo, passing_hf, dry_run_notifier, stats)

        filtered_events = [
            getattr(r, "event", None)
            for r in caplog.records
            if getattr(r, "event", None) == events.LISTING_FILTERED
        ]
        assert filtered_events, "Expected at least one LISTING_FILTERED event log"
        assert stats.passed_filter == 0

    @pytest.mark.asyncio
    async def test_listing_alerted_event(
        self,
        db_and_repo: tuple,
        dry_run_notifier: Notifier,
        passing_hf: HeuristicFilter,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """process_listing for an alerted listing emits event=LISTING_ALERTED."""
        conn, repo = db_and_repo
        stats = ProviderCycleStats(source="immobiliare")
        with caplog.at_level(logging.DEBUG, logger="rentbot.orchestrator.pipeline"):
            await process_listing(_LISTING_PASS, repo, passing_hf, dry_run_notifier, stats)

        alerted_events = [
            getattr(r, "event", None)
            for r in caplog.records
            if getattr(r, "event", None) == events.LISTING_ALERTED
        ]
        assert alerted_events, "Expected at least one LISTING_ALERTED event log"
        assert stats.alerted == 1

    @pytest.mark.asyncio
    async def test_provider_fetch_ok_event(
        self,
        db_and_repo: tuple,
        dry_run_notifier: Notifier,
        passing_hf: HeuristicFilter,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """run_provider for a successful fetch emits event=PROVIDER_FETCH_OK."""
        conn, repo = db_and_repo

        class _GoodProvider(BaseProvider):
            source = ListingSource.IMMOBILIARE

            async def fetch_latest(self) -> list[Listing]:
                return []

        with caplog.at_level(logging.INFO, logger="rentbot.orchestrator.pipeline"):
            await run_provider(_GoodProvider(), repo, passing_hf, dry_run_notifier, _DRY_RUN_CTX)

        fetch_ok_events = [
            getattr(r, "event", None)
            for r in caplog.records
            if getattr(r, "event", None) == events.PROVIDER_FETCH_OK
        ]
        assert fetch_ok_events, "Expected at least one PROVIDER_FETCH_OK event log"

    @pytest.mark.asyncio
    async def test_provider_fetch_error_event(
        self,
        db_and_repo: tuple,
        dry_run_notifier: Notifier,
        passing_hf: HeuristicFilter,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """run_provider for a failing fetch emits event=PROVIDER_FETCH_ERROR."""
        conn, repo = db_and_repo

        class _BrokenProvider(BaseProvider):
            source = ListingSource.IMMOBILIARE

            async def fetch_latest(self) -> list[Listing]:
                raise RuntimeError("network error")

        with caplog.at_level(logging.ERROR, logger="rentbot.orchestrator.pipeline"):
            stats = await run_provider(
                _BrokenProvider(), repo, passing_hf, dry_run_notifier, _DRY_RUN_CTX
            )

        fetch_err_events = [
            getattr(r, "event", None)
            for r in caplog.records
            if getattr(r, "event", None) == events.PROVIDER_FETCH_ERROR
        ]
        assert fetch_err_events, "Expected at least one PROVIDER_FETCH_ERROR event log"
        assert stats.provider_failed is True

    @pytest.mark.asyncio
    async def test_provider_done_event(
        self,
        db_and_repo: tuple,
        dry_run_notifier: Notifier,
        passing_hf: HeuristicFilter,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """run_provider emits event=PROVIDER_DONE after processing all listings."""
        conn, repo = db_and_repo

        class _GoodProvider(BaseProvider):
            source = ListingSource.IMMOBILIARE

            async def fetch_latest(self) -> list[Listing]:
                return [_LISTING_PASS]

        with caplog.at_level(logging.INFO, logger="rentbot.orchestrator.pipeline"):
            await run_provider(_GoodProvider(), repo, passing_hf, dry_run_notifier, _DRY_RUN_CTX)

        done_events = [
            getattr(r, "event", None)
            for r in caplog.records
            if getattr(r, "event", None) == events.PROVIDER_DONE
        ]
        assert done_events, "Expected at least one PROVIDER_DONE event log"


# ---------------------------------------------------------------------------
# TestEventAnnotationsRunner
# ---------------------------------------------------------------------------


class TestEventAnnotationsRunner:
    """Verify event annotations on run_once log records."""

    def _minimal_settings(self, db_path: Path) -> Settings:
        return Settings(
            telegram_bot_token="placeholder",
            telegram_chat_id="0",
            database_path=str(db_path),
        )

    @pytest.mark.asyncio
    async def test_cycle_start_event_logged(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """run_once emits a log record with event=CYCLE_START."""
        settings = self._minimal_settings(tmp_path / "test.db")
        ctx = RunContext(dry_run=True)

        with (
            patch("rentbot.orchestrator.runner._build_providers", return_value=[]),
            patch("rentbot.orchestrator.runner.run_cycle", AsyncMock(return_value=CycleStats())),
            caplog.at_level(logging.INFO, logger="rentbot.orchestrator.runner"),
        ):
            await run_once(ctx, settings)

        start_events = [
            getattr(r, "event", None)
            for r in caplog.records
            if getattr(r, "event", None) == events.CYCLE_START
        ]
        assert start_events, "Expected at least one CYCLE_START event log from run_once"

    @pytest.mark.asyncio
    async def test_cycle_complete_event_logged(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """run_once emits a log record with event=CYCLE_COMPLETE."""
        settings = self._minimal_settings(tmp_path / "test.db")
        ctx = RunContext(dry_run=True)

        with (
            patch("rentbot.orchestrator.runner._build_providers", return_value=[]),
            patch("rentbot.orchestrator.runner.run_cycle", AsyncMock(return_value=CycleStats())),
            caplog.at_level(logging.INFO, logger="rentbot.orchestrator.runner"),
        ):
            await run_once(ctx, settings)

        complete_events = [
            getattr(r, "event", None)
            for r in caplog.records
            if getattr(r, "event", None) == events.CYCLE_COMPLETE
        ]
        assert complete_events, "Expected at least one CYCLE_COMPLETE event log from run_once"

    @pytest.mark.asyncio
    async def test_cycle_abort_event_logged_on_config_error(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """run_once emits event=CYCLE_ABORT when Telegram is unconfigured in live mode."""
        from unittest.mock import PropertyMock, patch

        from rentbot.core.exceptions import ConfigError

        settings = self._minimal_settings(tmp_path / "test.db")
        # Live mode (should_notify=True); force telegram_configured → False
        # regardless of what the real .env contains.
        ctx = RunContext(seed=False, dry_run=False)

        with (
            patch("rentbot.orchestrator.runner._build_providers", return_value=[]),
            patch.object(
                Settings, "telegram_configured", new_callable=PropertyMock, return_value=False
            ),
            caplog.at_level(logging.ERROR, logger="rentbot.orchestrator.runner"),
            pytest.raises(ConfigError),
        ):
            await run_once(ctx, settings)

        abort_events = [
            getattr(r, "event", None)
            for r in caplog.records
            if getattr(r, "event", None) == events.CYCLE_ABORT
        ]
        assert abort_events, "Expected CYCLE_ABORT event log when ConfigError is raised"


# ---------------------------------------------------------------------------
# TestContextVarPropagation
# ---------------------------------------------------------------------------


class TestContextVarPropagation:
    """cycle_id ContextVar is inherited by tasks spawned via asyncio.gather."""

    @pytest.mark.asyncio
    async def test_gathered_tasks_see_parent_cycle_id(self) -> None:
        """Child tasks created by asyncio.gather inherit the caller's cycle_id."""
        captured: list[str] = []

        async def _read_cycle_id() -> None:
            captured.append(CYCLE_ID_CTX.get("-"))

        token = CYCLE_ID_CTX.set("parent01")
        try:
            await asyncio.gather(_read_cycle_id(), _read_cycle_id())
        finally:
            CYCLE_ID_CTX.reset(token)

        assert captured == ["parent01", "parent01"], (
            "Both child tasks should inherit 'parent01' from the parent context"
        )

    @pytest.mark.asyncio
    async def test_child_mutation_does_not_affect_parent(self) -> None:
        """A child task setting CYCLE_ID_CTX does not leak into the parent context."""

        async def _mutate() -> None:
            CYCLE_ID_CTX.set("child_value")

        token = CYCLE_ID_CTX.set("parent_value")
        try:
            await asyncio.gather(_mutate())
            # Parent context is unaffected by the child's set().
            assert CYCLE_ID_CTX.get("-") == "parent_value"
        finally:
            CYCLE_ID_CTX.reset(token)
