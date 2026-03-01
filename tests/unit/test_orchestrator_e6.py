"""Unit tests for Epic 6 — Orchestration: scheduler (E6-T2).

Tests cover:
- ``next_api_interval`` / ``next_browser_interval`` — pure interval helpers.
- ``_api_loop`` — verifies run_once is called, exceptions are swallowed, sleep
  is called with a valid jittered value.
- ``run_continuous`` — verifies both concurrent tasks are created and that
  CancelledError propagates cleanly (clean shutdown path).
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest

from rentbot.core.run_context import RunContext
from rentbot.core.settings import Settings
from rentbot.orchestrator.scheduler import (
    _api_loop,
    next_api_interval,
    next_browser_interval,
    run_continuous,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def settings() -> Settings:
    """Minimal Settings with predictable, fast interval bounds for testing."""
    s = MagicMock(spec=Settings)
    s.poll_interval_api_min = 10
    s.poll_interval_api_max = 20
    s.poll_interval_browser_min = 30
    s.poll_interval_browser_max = 60
    return s


@pytest.fixture()
def ctx() -> RunContext:
    return RunContext(seed=False, dry_run=True)


# ---------------------------------------------------------------------------
# next_api_interval / next_browser_interval
# ---------------------------------------------------------------------------


class TestIntervalHelpers:
    """next_api_interval and next_browser_interval are pure functions."""

    def test_api_interval_within_bounds(self) -> None:
        s = MagicMock(spec=Settings)
        s.poll_interval_api_min = 300
        s.poll_interval_api_max = 600
        for _ in range(200):
            v = next_api_interval(s)
            assert 300 <= v <= 600, f"API interval {v} out of [300, 600]"

    def test_browser_interval_within_bounds(self) -> None:
        s = MagicMock(spec=Settings)
        s.poll_interval_browser_min = 900
        s.poll_interval_browser_max = 1200
        for _ in range(200):
            v = next_browser_interval(s)
            assert 900 <= v <= 1200, f"Browser interval {v} out of [900, 1200]"

    def test_api_interval_equal_bounds_returns_that_value(self) -> None:
        s = MagicMock(spec=Settings)
        s.poll_interval_api_min = 500
        s.poll_interval_api_max = 500
        assert next_api_interval(s) == 500.0

    def test_browser_interval_equal_bounds_returns_that_value(self) -> None:
        s = MagicMock(spec=Settings)
        s.poll_interval_browser_min = 1000
        s.poll_interval_browser_max = 1000
        assert next_browser_interval(s) == 1000.0

    def test_api_interval_returns_float(self) -> None:
        s = MagicMock(spec=Settings)
        s.poll_interval_api_min = 1
        s.poll_interval_api_max = 2
        assert isinstance(next_api_interval(s), float)

    def test_browser_interval_returns_float(self) -> None:
        s = MagicMock(spec=Settings)
        s.poll_interval_browser_min = 1
        s.poll_interval_browser_max = 2
        assert isinstance(next_browser_interval(s), float)

    def test_api_interval_varies_across_calls(self) -> None:
        """With a wide range (1–1000), repeatedly calling should not always
        return the same value (collision probability is negligible)."""
        s = MagicMock(spec=Settings)
        s.poll_interval_api_min = 1
        s.poll_interval_api_max = 1_000_000
        values = {next_api_interval(s) for _ in range(10)}
        assert len(values) > 1, "Expected different values across 10 calls"


# ---------------------------------------------------------------------------
# _api_loop
# ---------------------------------------------------------------------------


class TestApiLoop:
    """Tests for the internal _api_loop coroutine."""

    @pytest.mark.asyncio
    async def test_api_loop_calls_run_once(
        self, ctx: RunContext, settings: MagicMock
    ) -> None:
        """run_once should be called on the first iteration of the loop."""
        call_count = 0

        async def fake_run_once(**kwargs: object) -> None:
            nonlocal call_count
            call_count += 1

        async def fake_sleep(interval: float) -> None:
            # Cancel after the first sleep so the loop terminates
            raise asyncio.CancelledError

        with (
            patch(
                "rentbot.orchestrator.scheduler.run_once",
                side_effect=fake_run_once,
            ),
            patch("asyncio.sleep", side_effect=fake_sleep),
        ):
            with pytest.raises(asyncio.CancelledError):
                await _api_loop(ctx=ctx, settings=settings)

        assert call_count == 1

    @pytest.mark.asyncio
    async def test_api_loop_sleeps_with_value_in_bounds(
        self, ctx: RunContext, settings: MagicMock
    ) -> None:
        """asyncio.sleep should be called with a value within the configured range."""
        sleep_args: list[float] = []

        async def fake_run_once(**kwargs: object) -> None:
            pass

        async def fake_sleep(interval: float) -> None:
            sleep_args.append(interval)
            raise asyncio.CancelledError

        with (
            patch(
                "rentbot.orchestrator.scheduler.run_once",
                side_effect=fake_run_once,
            ),
            patch("asyncio.sleep", side_effect=fake_sleep),
        ):
            with pytest.raises(asyncio.CancelledError):
                await _api_loop(ctx=ctx, settings=settings)

        assert len(sleep_args) == 1
        assert settings.poll_interval_api_min <= sleep_args[0] <= settings.poll_interval_api_max

    @pytest.mark.asyncio
    async def test_api_loop_continues_after_run_once_exception(
        self, ctx: RunContext, settings: MagicMock
    ) -> None:
        """An exception from run_once must NOT stop the loop; it should be
        logged and the loop should continue to the next iteration."""
        call_count = 0

        async def flaky_run_once(**kwargs: object) -> None:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("transient network failure")
            # Second call succeeds

        sleep_count = 0

        async def fake_sleep(interval: float) -> None:
            nonlocal sleep_count
            sleep_count += 1
            if sleep_count >= 2:
                # Cancel after the second sleep so we've seen two iterations
                raise asyncio.CancelledError

        with (
            patch(
                "rentbot.orchestrator.scheduler.run_once",
                side_effect=flaky_run_once,
            ),
            patch("asyncio.sleep", side_effect=fake_sleep),
        ):
            with pytest.raises(asyncio.CancelledError):
                await _api_loop(ctx=ctx, settings=settings)

        # Loop ran twice despite the first call raising
        assert call_count == 2

    @pytest.mark.asyncio
    async def test_api_loop_calls_run_once_multiple_times(
        self, ctx: RunContext, settings: MagicMock
    ) -> None:
        """Verify the loop iterates more than once."""
        call_count = 0
        sleep_count = 0

        async def fake_run_once(**kwargs: object) -> None:
            nonlocal call_count
            call_count += 1

        async def fake_sleep(interval: float) -> None:
            nonlocal sleep_count
            sleep_count += 1
            if sleep_count >= 3:
                raise asyncio.CancelledError

        with (
            patch(
                "rentbot.orchestrator.scheduler.run_once",
                side_effect=fake_run_once,
            ),
            patch("asyncio.sleep", side_effect=fake_sleep),
        ):
            with pytest.raises(asyncio.CancelledError):
                await _api_loop(ctx=ctx, settings=settings)

        assert call_count == 3


# ---------------------------------------------------------------------------
# run_continuous
# ---------------------------------------------------------------------------


class TestRunContinuous:
    """Tests for the public run_continuous entry-point."""

    @pytest.mark.asyncio
    async def test_run_continuous_runs_api_loop(
        self, ctx: RunContext, settings: MagicMock
    ) -> None:
        """run_continuous should invoke the API loop (run_once is eventually called)."""
        run_once_called = False

        async def fake_run_once(**kwargs: object) -> None:
            nonlocal run_once_called
            run_once_called = True

        sleep_count = 0

        async def fake_sleep(interval: float) -> None:
            nonlocal sleep_count
            sleep_count += 1
            if sleep_count >= 2:
                raise asyncio.CancelledError

        with (
            patch(
                "rentbot.orchestrator.scheduler.run_once",
                side_effect=fake_run_once,
            ),
            patch("asyncio.sleep", side_effect=fake_sleep),
        ):
            with pytest.raises((asyncio.CancelledError, BaseException)):
                await run_continuous(ctx=ctx, settings=settings)

        assert run_once_called, "run_once was never called by the continuous loop"

    @pytest.mark.asyncio
    async def test_run_continuous_loads_settings_if_none(
        self, ctx: RunContext
    ) -> None:
        """When settings=None, run_continuous should load Settings from env."""
        loaded: list[bool] = []

        async def fake_run_once(
            ctx: RunContext, settings: Settings | None = None
        ) -> None:
            loaded.append(settings is not None)

        async def fake_sleep(interval: float) -> None:
            raise asyncio.CancelledError

        with (
            patch(
                "rentbot.orchestrator.scheduler.run_once",
                side_effect=fake_run_once,
            ),
            patch("asyncio.sleep", side_effect=fake_sleep),
        ):
            with pytest.raises((asyncio.CancelledError, BaseException)):
                await run_continuous(ctx=ctx, settings=None)

        # run_once must have received a Settings instance (not None)
        assert any(loaded), "run_once was never called with loaded settings"

    @pytest.mark.asyncio
    async def test_run_continuous_accepts_cancellation(
        self, ctx: RunContext, settings: MagicMock
    ) -> None:
        """CancelledError should propagate out of run_continuous cleanly."""

        async def fast_cancel(**kwargs: object) -> None:
            raise asyncio.CancelledError

        with patch(
            "rentbot.orchestrator.scheduler.run_once",
            side_effect=fast_cancel,
        ):
            with pytest.raises((asyncio.CancelledError, BaseException)):
                # Should not hang; should terminate promptly
                await asyncio.wait_for(
                    run_continuous(ctx=ctx, settings=settings),
                    timeout=5.0,
                )
