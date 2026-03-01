"""Unit tests for Epic 6 — Orchestration: scheduler (E6-T2) + circuit breaker (E6-T3) + run summary (E6-T4) + graceful shutdown (E6-T6).

Tests cover:
- ``next_api_interval`` / ``next_browser_interval`` — pure interval helpers.
- ``_api_loop`` — verifies run_once is called, exceptions are swallowed, sleep
  is called with a valid jittered value.
- ``run_continuous`` — verifies both concurrent tasks are created and that
  CancelledError propagates cleanly (clean shutdown path).
- ``TestGracefulShutdown`` — SIGTERM handler registration, idempotent log
  emission, task cancellation, and cleanup in the finally block (E6-T6).
- ``CircuitBreakerRegistry`` — state transitions (CLOSED → OPEN → HALF_OPEN →
  CLOSED), exponential backoff, threshold behaviour, reset on success.
- ``run_provider`` with circuit breaker — skips when OPEN, records outcomes.
- ``CycleStats.format_cycle_report`` + ``duration_s`` — run summary metrics
  (E6-T4): structure, content, timing assignment in ``run_once``.
"""

from __future__ import annotations

import asyncio
import logging
import signal
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from rentbot.core.run_context import RunContext
from rentbot.core.settings import Settings
from rentbot.orchestrator.circuit_breaker import (
    CircuitBreakerRegistry,
    CircuitState,
)
from rentbot.orchestrator.pipeline import CycleStats, ProviderCycleStats, run_provider
from rentbot.orchestrator.scheduler import (
    HEARTBEAT_PATH,
    HEARTBEAT_STALE_AFTER_S,
    _api_loop,
    _write_heartbeat,
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
    async def test_api_loop_calls_run_once(self, ctx: RunContext, settings: MagicMock) -> None:
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
            pytest.raises(asyncio.CancelledError),
        ):
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
            pytest.raises(asyncio.CancelledError),
        ):
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
            pytest.raises(asyncio.CancelledError),
        ):
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
            pytest.raises(asyncio.CancelledError),
        ):
            await _api_loop(ctx=ctx, settings=settings)

        assert call_count == 3


# ---------------------------------------------------------------------------
# Heartbeat (E7-T4)
# ---------------------------------------------------------------------------


class TestHeartbeat:
    """E7-T4: health-check heartbeat file written by the API polling loop."""

    # ------------------------------------------------------------------
    # _write_heartbeat unit tests
    # ------------------------------------------------------------------

    def test_write_heartbeat_creates_file(self, tmp_path: object) -> None:
        """_write_heartbeat must create the file if it does not exist."""
        import pathlib

        dest = str(pathlib.Path(str(tmp_path)) / "hb.txt")  # type: ignore[arg-type]
        _write_heartbeat(path=dest)
        assert pathlib.Path(dest).exists()

    def test_write_heartbeat_writes_valid_epoch(self, tmp_path: object) -> None:
        """File content must be parseable as a float close to the current time."""
        import pathlib
        import time

        dest = pathlib.Path(str(tmp_path)) / "hb.txt"  # type: ignore[operator]
        t_before = time.time()
        _write_heartbeat(path=str(dest))
        t_after = time.time()

        written = float(dest.read_text(encoding="utf-8"))
        assert t_before <= written <= t_after

    def test_write_heartbeat_overwrites_existing_file(self, tmp_path: object) -> None:
        """Calling _write_heartbeat twice must update the timestamp."""
        import pathlib
        import time

        dest = pathlib.Path(str(tmp_path)) / "hb.txt"  # type: ignore[operator]
        _write_heartbeat(path=str(dest))
        first = float(dest.read_text(encoding="utf-8"))
        time.sleep(0.05)  # small gap to ensure monotonic difference
        _write_heartbeat(path=str(dest))
        second = float(dest.read_text(encoding="utf-8"))

        assert second >= first

    def test_write_heartbeat_does_not_raise_on_bad_path(self, caplog: object) -> None:
        """An unwritable path must be logged as a warning, not raise."""
        import logging

        with caplog.at_level(logging.WARNING, logger="rentbot.orchestrator.scheduler"):  # type: ignore[union-attr]
            _write_heartbeat(path="/nonexistent_dir_RENTBOT_TEST/hb.txt")
        # No exception propagated; a warning was logged.
        assert any("heartbeat" in r.message.lower() for r in caplog.records)  # type: ignore[union-attr]

    def test_heartbeat_path_constant_has_expected_suffix(self) -> None:
        """HEARTBEAT_PATH must end with 'rentbot_heartbeat'."""
        assert HEARTBEAT_PATH.endswith("rentbot_heartbeat")

    def test_heartbeat_stale_threshold_is_positive(self) -> None:
        """HEARTBEAT_STALE_AFTER_S must be a positive integer."""
        assert isinstance(HEARTBEAT_STALE_AFTER_S, int)
        assert HEARTBEAT_STALE_AFTER_S > 0

    # ------------------------------------------------------------------
    # Integration of heartbeat writing in _api_loop
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_api_loop_writes_heartbeat_on_success(
        self, ctx: RunContext, settings: MagicMock
    ) -> None:
        """_write_heartbeat is called after a successful run_once."""
        heartbeat_calls: list[int] = []

        async def fake_run_once(**kwargs: object) -> CycleStats:
            return CycleStats()

        async def fake_sleep(interval: float) -> None:
            raise asyncio.CancelledError

        def fake_write_heartbeat(*args: object, **kwargs: object) -> None:
            heartbeat_calls.append(1)

        with (
            patch("rentbot.orchestrator.scheduler.run_once", side_effect=fake_run_once),
            patch("asyncio.sleep", side_effect=fake_sleep),
            patch(
                "rentbot.orchestrator.scheduler._write_heartbeat",
                side_effect=fake_write_heartbeat,
            ),
            pytest.raises(asyncio.CancelledError),
        ):
            await _api_loop(ctx=ctx, settings=settings)

        assert len(heartbeat_calls) == 1, "_write_heartbeat not called after successful cycle"

    @pytest.mark.asyncio
    async def test_api_loop_writes_heartbeat_after_run_once_failure(
        self, ctx: RunContext, settings: MagicMock
    ) -> None:
        """_write_heartbeat is called even when run_once raises an exception."""
        heartbeat_calls: list[int] = []

        async def boom(**kwargs: object) -> None:
            raise RuntimeError("provider failure")

        async def fake_sleep(interval: float) -> None:
            raise asyncio.CancelledError

        def fake_write_heartbeat(*args: object, **kwargs: object) -> None:
            heartbeat_calls.append(1)

        with (
            patch("rentbot.orchestrator.scheduler.run_once", side_effect=boom),
            patch("asyncio.sleep", side_effect=fake_sleep),
            patch(
                "rentbot.orchestrator.scheduler._write_heartbeat",
                side_effect=fake_write_heartbeat,
            ),
            pytest.raises(asyncio.CancelledError),
        ):
            await _api_loop(ctx=ctx, settings=settings)

        assert len(heartbeat_calls) == 1, "_write_heartbeat not called after failing cycle"


# ---------------------------------------------------------------------------
# run_continuous
# ---------------------------------------------------------------------------


class TestRunContinuous:
    """Tests for the public run_continuous entry-point."""

    @pytest.mark.asyncio
    async def test_run_continuous_runs_api_loop(self, ctx: RunContext, settings: MagicMock) -> None:
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
            pytest.raises((asyncio.CancelledError, BaseException)),
        ):
            await run_continuous(ctx=ctx, settings=settings)

        assert run_once_called, "run_once was never called by the continuous loop"

    @pytest.mark.asyncio
    async def test_run_continuous_loads_settings_if_none(self, ctx: RunContext) -> None:
        """When settings=None, run_continuous should load Settings from env."""
        loaded: list[bool] = []

        async def fake_run_once(
            ctx: RunContext,
            settings: Settings | None = None,
            **kwargs: object,
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
            pytest.raises((asyncio.CancelledError, BaseException)),
        ):
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

        with (
            patch(
                "rentbot.orchestrator.scheduler.run_once",
                side_effect=fast_cancel,
            ),
            pytest.raises((asyncio.CancelledError, BaseException)),
        ):
            # Should not hang; should terminate promptly
            await asyncio.wait_for(
                run_continuous(ctx=ctx, settings=settings),
                timeout=5.0,
            )


# ---------------------------------------------------------------------------
# Graceful shutdown (E6-T6)
# ---------------------------------------------------------------------------


class TestGracefulShutdown:
    """E6-T6: SIGTERM handler registration, idempotent log, cleanup."""

    @pytest.mark.asyncio
    async def test_sigterm_handler_registered_on_loop(
        self, ctx: RunContext, settings: MagicMock
    ) -> None:
        """run_continuous calls loop.add_signal_handler(SIGTERM, …) on entry."""
        loop = asyncio.get_running_loop()
        registered: dict[Any, Any] = {}

        async def fast_cancel(**kwargs: object) -> None:
            raise asyncio.CancelledError

        with (
            patch.object(
                loop,
                "add_signal_handler",
                side_effect=lambda sig, cb: registered.update({sig: cb}),
            ),
            patch.object(loop, "remove_signal_handler"),
            patch(
                "rentbot.orchestrator.scheduler.run_once",
                side_effect=fast_cancel,
            ),
            pytest.raises((asyncio.CancelledError, BaseException)),
        ):
            await run_continuous(ctx=ctx, settings=settings)

        assert signal.SIGTERM in registered, "SIGTERM handler was not registered on the event loop"

    @pytest.mark.asyncio
    async def test_sigterm_handler_removed_in_finally(
        self, ctx: RunContext, settings: MagicMock
    ) -> None:
        """loop.remove_signal_handler(SIGTERM) is called even after cancellation."""
        loop = asyncio.get_running_loop()
        removed: list[Any] = []

        async def fast_cancel(**kwargs: object) -> None:
            raise asyncio.CancelledError

        with (
            patch.object(loop, "add_signal_handler"),
            patch.object(
                loop,
                "remove_signal_handler",
                side_effect=lambda sig: removed.append(sig),
            ),
            patch(
                "rentbot.orchestrator.scheduler.run_once",
                side_effect=fast_cancel,
            ),
            pytest.raises((asyncio.CancelledError, BaseException)),
        ):
            await run_continuous(ctx=ctx, settings=settings)

        assert signal.SIGTERM in removed, (
            "SIGTERM handler was not removed from the event loop in finally block"
        )

    @pytest.mark.asyncio
    async def test_sigterm_callback_logs_graceful_shutdown(
        self, ctx: RunContext, settings: MagicMock, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Invoking the registered SIGTERM callback emits a 'graceful shutdown' log."""
        loop = asyncio.get_running_loop()
        captured: dict[Any, Any] = {}

        async def invoke_sigterm_then_cancel(**kwargs: object) -> None:
            # Simulate SIGTERM arriving while a cycle is in progress.
            if signal.SIGTERM in captured:
                captured[signal.SIGTERM]()
            raise asyncio.CancelledError

        with (
            patch.object(
                loop,
                "add_signal_handler",
                side_effect=lambda sig, cb: captured.update({sig: cb}),
            ),
            patch.object(loop, "remove_signal_handler"),
            patch(
                "rentbot.orchestrator.scheduler.run_once",
                side_effect=invoke_sigterm_then_cancel,
            ),
            caplog.at_level(logging.INFO, logger="rentbot.orchestrator.scheduler"),
            pytest.raises((asyncio.CancelledError, BaseException)),
        ):
            await run_continuous(ctx=ctx, settings=settings)

        sigterm_log = [r.message for r in caplog.records if "SIGTERM" in r.message]
        assert sigterm_log, "No log message mentioning SIGTERM was emitted"

    @pytest.mark.asyncio
    async def test_sigterm_callback_logs_only_once_on_repeated_signals(
        self, ctx: RunContext, settings: MagicMock, caplog: pytest.LogCaptureFixture
    ) -> None:
        """The 'graceful shutdown' message appears exactly once even if SIGTERM fires twice."""
        loop = asyncio.get_running_loop()
        captured: dict[Any, Any] = {}

        async def invoke_sigterm_twice_then_cancel(**kwargs: object) -> None:
            if signal.SIGTERM in captured:
                captured[signal.SIGTERM]()  # first fire
                captured[signal.SIGTERM]()  # impatient second fire
            raise asyncio.CancelledError

        with (
            patch.object(
                loop,
                "add_signal_handler",
                side_effect=lambda sig, cb: captured.update({sig: cb}),
            ),
            patch.object(loop, "remove_signal_handler"),
            patch(
                "rentbot.orchestrator.scheduler.run_once",
                side_effect=invoke_sigterm_twice_then_cancel,
            ),
            caplog.at_level(logging.INFO, logger="rentbot.orchestrator.scheduler"),
            pytest.raises((asyncio.CancelledError, BaseException)),
        ):
            await run_continuous(ctx=ctx, settings=settings)

        shutdown_log_count = sum(
            1 for r in caplog.records if "graceful shutdown requested" in r.message
        )
        assert shutdown_log_count == 1, (
            f"Expected exactly 1 'graceful shutdown requested' log, got {shutdown_log_count}"
        )

    @pytest.mark.asyncio
    async def test_complete_message_logged_after_sigterm_shutdown(
        self, ctx: RunContext, settings: MagicMock, caplog: pytest.LogCaptureFixture
    ) -> None:
        """'Graceful shutdown complete' is logged in the except block after a SIGTERM."""
        loop = asyncio.get_running_loop()
        captured: dict[Any, Any] = {}

        async def invoke_sigterm_then_cancel(**kwargs: object) -> None:
            if signal.SIGTERM in captured:
                captured[signal.SIGTERM]()
            raise asyncio.CancelledError

        with (
            patch.object(
                loop,
                "add_signal_handler",
                side_effect=lambda sig, cb: captured.update({sig: cb}),
            ),
            patch.object(loop, "remove_signal_handler"),
            patch(
                "rentbot.orchestrator.scheduler.run_once",
                side_effect=invoke_sigterm_then_cancel,
            ),
            caplog.at_level(logging.INFO, logger="rentbot.orchestrator.scheduler"),
            pytest.raises((asyncio.CancelledError, BaseException)),
        ):
            await run_continuous(ctx=ctx, settings=settings)

        complete_msgs = [
            r.message for r in caplog.records if "Graceful shutdown complete" in r.message
        ]
        assert complete_msgs, "'Graceful shutdown complete' log line was not emitted"


# ---------------------------------------------------------------------------
# CircuitBreakerRegistry
# ---------------------------------------------------------------------------


class TestCircuitBreakerBasicTransitions:
    """Core state machine: CLOSED → OPEN → HALF_OPEN → CLOSED."""

    def test_initial_state_is_closed(self) -> None:
        cb = CircuitBreakerRegistry()
        pcs = cb.get_state("immobiliare")
        assert pcs.state == CircuitState.CLOSED
        assert pcs.consecutive_failures == 0
        assert pcs.trip_count == 0

    def test_allow_request_true_when_closed(self) -> None:
        cb = CircuitBreakerRegistry()
        assert cb.allow_request("immobiliare") is True

    def test_stays_closed_below_threshold(self) -> None:
        cb = CircuitBreakerRegistry(failure_threshold=3)
        cb.record_failure("x")
        cb.record_failure("x")
        assert cb.get_state("x").state == CircuitState.CLOSED
        assert cb.allow_request("x") is True

    def test_opens_at_threshold(self) -> None:
        cb = CircuitBreakerRegistry(failure_threshold=3)
        for _ in range(3):
            cb.record_failure("x")
        assert cb.get_state("x").state == CircuitState.OPEN
        assert cb.get_state("x").trip_count == 1

    def test_open_blocks_requests(self) -> None:
        clock = _FakeClock(0.0)
        cb = CircuitBreakerRegistry(
            failure_threshold=2,
            base_recovery_timeout=100.0,
            clock=clock,
        )
        cb.record_failure("x")
        cb.record_failure("x")
        assert cb.allow_request("x") is False

    def test_half_open_after_recovery_timeout(self) -> None:
        clock = _FakeClock(0.0)
        cb = CircuitBreakerRegistry(
            failure_threshold=2,
            base_recovery_timeout=100.0,
            clock=clock,
        )
        cb.record_failure("x")
        cb.record_failure("x")
        # Advance past the recovery timeout.
        clock.advance(101.0)
        assert cb.allow_request("x") is True
        assert cb.get_state("x").state == CircuitState.HALF_OPEN

    def test_half_open_success_closes_circuit(self) -> None:
        clock = _FakeClock(0.0)
        cb = CircuitBreakerRegistry(
            failure_threshold=2,
            base_recovery_timeout=100.0,
            clock=clock,
        )
        cb.record_failure("x")
        cb.record_failure("x")
        clock.advance(101.0)
        cb.allow_request("x")  # triggers HALF_OPEN
        cb.record_success("x")
        pcs = cb.get_state("x")
        assert pcs.state == CircuitState.CLOSED
        assert pcs.consecutive_failures == 0
        assert pcs.trip_count == 0

    def test_half_open_failure_reopens_circuit(self) -> None:
        clock = _FakeClock(0.0)
        cb = CircuitBreakerRegistry(
            failure_threshold=2,
            base_recovery_timeout=100.0,
            clock=clock,
        )
        cb.record_failure("x")
        cb.record_failure("x")
        clock.advance(101.0)
        cb.allow_request("x")  # triggers HALF_OPEN
        cb.record_failure("x")
        pcs = cb.get_state("x")
        assert pcs.state == CircuitState.OPEN
        assert pcs.trip_count == 2  # second trip

    def test_success_resets_failure_counter_in_closed(self) -> None:
        cb = CircuitBreakerRegistry(failure_threshold=5)
        cb.record_failure("x")
        cb.record_failure("x")
        cb.record_success("x")
        assert cb.get_state("x").consecutive_failures == 0


class TestCircuitBreakerBackoff:
    """Exponential backoff on recovery timeout."""

    def test_first_trip_uses_base_timeout(self) -> None:
        clock = _FakeClock(0.0)
        cb = CircuitBreakerRegistry(
            failure_threshold=1,
            base_recovery_timeout=60.0,
            backoff_multiplier=2.0,
            clock=clock,
        )
        cb.record_failure("x")  # trip #1
        # Not enough time for base timeout.
        clock.advance(59.0)
        assert cb.allow_request("x") is False
        # Just past base timeout → HALF_OPEN.
        clock.advance(2.0)
        assert cb.allow_request("x") is True

    def test_second_trip_doubles_timeout(self) -> None:
        clock = _FakeClock(0.0)
        cb = CircuitBreakerRegistry(
            failure_threshold=1,
            base_recovery_timeout=60.0,
            backoff_multiplier=2.0,
            clock=clock,
        )
        # Trip #1.
        cb.record_failure("x")
        clock.advance(61.0)
        cb.allow_request("x")  # HALF_OPEN
        # Probe fails → trip #2 (timeout: 60 * 2^1 = 120 s).
        cb.record_failure("x")
        assert cb.get_state("x").trip_count == 2
        # 100 s is not enough for 120 s timeout.
        clock.advance(100.0)
        assert cb.allow_request("x") is False
        # 21 more → 121 s total from last failure.
        clock.advance(21.0)
        assert cb.allow_request("x") is True

    def test_timeout_capped_at_max(self) -> None:
        clock = _FakeClock(0.0)
        cb = CircuitBreakerRegistry(
            failure_threshold=1,
            base_recovery_timeout=60.0,
            max_recovery_timeout=100.0,
            backoff_multiplier=10.0,
            clock=clock,
        )
        # Trip #1.
        cb.record_failure("x")
        clock.advance(61.0)
        cb.allow_request("x")
        # Trip #2 — raw would be 60*10 = 600, capped at 100.
        cb.record_failure("x")
        clock.advance(99.0)
        assert cb.allow_request("x") is False
        clock.advance(2.0)
        assert cb.allow_request("x") is True

    def test_full_reset_clears_trip_count(self) -> None:
        """After a successful probe, trip_count resets to 0, so next
        failure sequence starts with base timeout again."""
        clock = _FakeClock(0.0)
        cb = CircuitBreakerRegistry(
            failure_threshold=1,
            base_recovery_timeout=60.0,
            backoff_multiplier=2.0,
            clock=clock,
        )
        cb.record_failure("x")  # trip #1
        clock.advance(61.0)
        cb.allow_request("x")  # HALF_OPEN
        cb.record_failure("x")  # trip #2
        clock.advance(121.0)
        cb.allow_request("x")  # HALF_OPEN again
        cb.record_success("x")  # probe succeeds → CLOSED, trip_count=0
        assert cb.get_state("x").trip_count == 0
        # New failure starts fresh.
        cb.record_failure("x")  # trip #1 again
        assert cb.get_state("x").trip_count == 1
        # Should use base timeout (60 s), not escalated.
        clock.advance(59.0)
        assert cb.allow_request("x") is False
        clock.advance(2.0)
        assert cb.allow_request("x") is True


class TestCircuitBreakerMultiProvider:
    """Each provider has an independent circuit state."""

    def test_independent_states(self) -> None:
        cb = CircuitBreakerRegistry(failure_threshold=2)
        cb.record_failure("a")
        cb.record_failure("a")
        assert cb.get_state("a").state == CircuitState.OPEN
        assert cb.get_state("b").state == CircuitState.CLOSED

    def test_summary(self) -> None:
        cb = CircuitBreakerRegistry(failure_threshold=1)
        cb.record_failure("a")
        _ = cb.allow_request("b")  # lazy-create
        result = cb.summary()
        assert result == {"a": "open", "b": "closed"}


class TestCircuitBreakerEdgeCases:
    """Edge cases and robustness."""

    def test_record_success_on_unknown_provider(self) -> None:
        """Should lazily create closed state."""
        cb = CircuitBreakerRegistry()
        cb.record_success("new_provider")
        assert cb.get_state("new_provider").state == CircuitState.CLOSED

    def test_half_open_allows_single_probe(self) -> None:
        """Calling allow_request twice when HALF_OPEN should still return True
        (no automatic re-open just from checking)."""
        clock = _FakeClock(0.0)
        cb = CircuitBreakerRegistry(
            failure_threshold=1,
            base_recovery_timeout=10.0,
            clock=clock,
        )
        cb.record_failure("x")
        clock.advance(11.0)
        assert cb.allow_request("x") is True  # → HALF_OPEN
        assert cb.allow_request("x") is True  # still HALF_OPEN, still allows
        assert cb.get_state("x").state == CircuitState.HALF_OPEN

    def test_threshold_one_trips_immediately(self) -> None:
        cb = CircuitBreakerRegistry(failure_threshold=1)
        cb.record_failure("x")
        assert cb.get_state("x").state == CircuitState.OPEN


# ---------------------------------------------------------------------------
# run_provider with circuit breaker
# ---------------------------------------------------------------------------


class TestRunProviderCircuitBreaker:
    """Integration of CircuitBreakerRegistry with run_provider."""

    @pytest.mark.asyncio
    async def test_skips_provider_when_circuit_open(self) -> None:
        """When the circuit is OPEN, run_provider should skip fetch entirely."""
        cb = CircuitBreakerRegistry(failure_threshold=1)
        cb.record_failure("immobiliare")
        assert cb.get_state("immobiliare").state == CircuitState.OPEN

        provider = _make_fake_provider("immobiliare")
        repo = MagicMock()
        hf = MagicMock()
        notifier = MagicMock()
        ctx = RunContext(seed=False, dry_run=True)

        stats = await run_provider(provider, repo, hf, notifier, ctx, circuit_breaker=cb)

        assert stats.provider_failed is True
        assert stats.fetched == 0
        # fetch_latest should NOT have been called.
        provider.fetch_latest.assert_not_called()

    @pytest.mark.asyncio
    async def test_records_success_on_successful_fetch(self) -> None:
        cb = CircuitBreakerRegistry(failure_threshold=3)
        cb.record_failure("immobiliare")
        cb.record_failure("immobiliare")
        assert cb.get_state("immobiliare").consecutive_failures == 2

        provider = _make_fake_provider("immobiliare", listings=[])
        repo = MagicMock()
        hf = MagicMock()
        notifier = MagicMock()
        ctx = RunContext(seed=False, dry_run=True)

        stats = await run_provider(provider, repo, hf, notifier, ctx, circuit_breaker=cb)

        assert stats.provider_failed is False
        assert cb.get_state("immobiliare").consecutive_failures == 0

    @pytest.mark.asyncio
    async def test_records_failure_on_fetch_exception(self) -> None:
        cb = CircuitBreakerRegistry(failure_threshold=5)

        provider = _make_fake_provider("immobiliare")
        provider.fetch_latest = AsyncMock(side_effect=RuntimeError("timeout"))
        repo = MagicMock()
        hf = MagicMock()
        notifier = MagicMock()
        ctx = RunContext(seed=False, dry_run=True)

        stats = await run_provider(provider, repo, hf, notifier, ctx, circuit_breaker=cb)

        assert stats.provider_failed is True
        assert cb.get_state("immobiliare").consecutive_failures == 1

    @pytest.mark.asyncio
    async def test_no_circuit_breaker_works_as_before(self) -> None:
        """Without a circuit breaker, run_provider should behave identically."""
        provider = _make_fake_provider("immobiliare", listings=[])
        repo = MagicMock()
        hf = MagicMock()
        notifier = MagicMock()
        ctx = RunContext(seed=False, dry_run=True)

        stats = await run_provider(provider, repo, hf, notifier, ctx, circuit_breaker=None)

        assert stats.provider_failed is False
        assert stats.fetched == 0


# ---------------------------------------------------------------------------
# CycleStats.format_cycle_report + duration_s  (E6-T4)
# ---------------------------------------------------------------------------


class TestCycleStatsReport:
    """Tests for CycleStats.duration_s and format_cycle_report()."""

    def test_duration_s_default_is_zero(self) -> None:
        stats = CycleStats()
        assert stats.duration_s == 0.0

    def test_duration_s_is_settable(self) -> None:
        stats = CycleStats()
        stats.duration_s = 3.75
        assert stats.duration_s == 3.75

    def test_format_report_no_providers(self) -> None:
        """With no providers the header line is still emitted cleanly."""
        stats = CycleStats(duration_s=1.2)
        report = stats.format_cycle_report()
        assert "cycle complete in 1.2s" in report
        assert "fetched=0" in report
        assert "new=0" in report
        assert "dup=0" in report
        assert "passed_filter=0" in report
        assert "alerted=0" in report
        assert "errors=0" in report
        # No failed_providers section when list is empty
        assert "failed_providers" not in report

    def test_format_report_single_provider_passing(self) -> None:
        ps = ProviderCycleStats(
            source="immobiliare",
            fetched=10,
            new=4,
            duplicate=6,
            passed_filter=3,
            alerted=3,
            errors=0,
        )
        stats = CycleStats(provider_stats=[ps], duration_s=2.0)
        report = stats.format_cycle_report()

        lines = report.splitlines()
        assert len(lines) == 2  # header + 1 provider

        # Header totals match the single provider
        assert "fetched=10" in lines[0]
        assert "new=4" in lines[0]
        assert "dup=6" in lines[0]
        assert "passed_filter=3" in lines[0]
        assert "alerted=3" in lines[0]
        assert "errors=0" in lines[0]

        # Provider line
        assert "immobiliare" in lines[1]
        assert "fetched=10" in lines[1]
        assert "[FAILED]" not in lines[1]

    def test_format_report_failed_provider_marker(self) -> None:
        ps = ProviderCycleStats(source="casa", provider_failed=True)
        stats = CycleStats(provider_stats=[ps], duration_s=0.5)
        report = stats.format_cycle_report()

        # Header must mention failed provider name
        assert "casa" in report
        assert "failed_providers" in report
        # Provider line (indented) must include [FAILED] marker
        provider_line = [
            line for line in report.splitlines() if line.startswith("  ") and "casa" in line
        ]
        assert len(provider_line) == 1
        assert "[FAILED]" in provider_line[0]

    def test_format_report_multiple_providers_aggregate_totals(self) -> None:
        ps1 = ProviderCycleStats(
            source="immobiliare",
            fetched=8,
            new=3,
            duplicate=5,
            passed_filter=2,
            alerted=2,
            errors=0,
        )
        ps2 = ProviderCycleStats(
            source="casa",
            fetched=4,
            new=1,
            duplicate=3,
            passed_filter=1,
            alerted=1,
            errors=1,
        )
        stats = CycleStats(provider_stats=[ps1, ps2], duration_s=3.14)
        report = stats.format_cycle_report()

        lines = report.splitlines()
        assert len(lines) == 3  # header + 2 providers

        # Aggregate totals in header
        assert "fetched=12" in lines[0]
        assert "new=4" in lines[0]
        assert "dup=8" in lines[0]
        assert "passed_filter=3" in lines[0]
        assert "alerted=3" in lines[0]
        assert "errors=1" in lines[0]
        assert "3.1s" in lines[0]  # duration rounded to 1 decimal

        # Both provider names appear in their respective lines
        provider_sources = [line.split(":")[0].strip() for line in lines[1:]]
        assert "immobiliare" in provider_sources
        assert "casa" in provider_sources

    def test_format_report_duration_one_decimal(self) -> None:
        """Duration is always formatted to exactly one decimal place."""
        stats = CycleStats(duration_s=10.0)
        assert "10.0s" in stats.format_cycle_report()

        stats2 = CycleStats(duration_s=0.123)
        assert "0.1s" in stats2.format_cycle_report()

    @pytest.mark.asyncio
    async def test_run_once_sets_duration_s(self) -> None:
        """run_once must assign a positive duration_s to the returned CycleStats."""
        from rentbot.orchestrator.runner import run_once

        ctx = RunContext(seed=True, dry_run=False)
        settings = MagicMock(spec=Settings)
        settings.database_path = ":memory:"
        settings.database_path_resolved = MagicMock()
        settings.database_path_resolved.parent.mkdir = MagicMock()
        settings.telegram_configured = False
        settings.immobiliare_vrt = None
        settings.casa_search_url = None
        settings.to_filter_criteria.return_value = MagicMock()

        # Patch open_db so we don't touch the filesystem.
        fake_conn = AsyncMock()
        fake_conn.close = AsyncMock()

        # Patch run_cycle to return a bare CycleStats instantly.
        fake_stats = CycleStats()

        with (
            patch("rentbot.orchestrator.runner.open_db", return_value=fake_conn),
            patch(
                "rentbot.orchestrator.runner.run_cycle",
                new=AsyncMock(return_value=fake_stats),
            ),
            patch("rentbot.orchestrator.runner.time") as mock_time,
        ):
            # Simulate monotonic advancing by 1.5 s over the cycle.
            mock_time.monotonic.side_effect = [100.0, 101.5]
            returned = await run_once(ctx=ctx, settings=settings)

        assert returned.duration_s == pytest.approx(1.5, abs=0.01)

    @pytest.mark.asyncio
    async def test_run_once_logs_cycle_report(self, caplog: pytest.LogCaptureFixture) -> None:
        """run_once must emit the format_cycle_report() text at INFO level."""
        import logging

        from rentbot.orchestrator.runner import run_once

        ctx = RunContext(seed=True, dry_run=False)
        settings = MagicMock(spec=Settings)
        settings.database_path = ":memory:"
        settings.database_path_resolved = MagicMock()
        settings.database_path_resolved.parent.mkdir = MagicMock()
        settings.telegram_configured = False
        settings.immobiliare_vrt = None
        settings.casa_search_url = None
        settings.to_filter_criteria.return_value = MagicMock()

        fake_conn = AsyncMock()
        fake_conn.close = AsyncMock()
        fake_stats = CycleStats()

        with (
            patch("rentbot.orchestrator.runner.open_db", return_value=fake_conn),
            patch(
                "rentbot.orchestrator.runner.run_cycle",
                new=AsyncMock(return_value=fake_stats),
            ),
            caplog.at_level(logging.INFO, logger="rentbot.orchestrator.runner"),
        ):
            await run_once(ctx=ctx, settings=settings)

        # The INFO log must contain the key phrase from format_cycle_report.
        info_messages = " ".join(r.message for r in caplog.records if r.levelno == logging.INFO)
        assert "cycle complete in" in info_messages


# ---------------------------------------------------------------------------
# E6-T5 — Partial provider failure tolerance
# ---------------------------------------------------------------------------


class TestPartialProviderFailureTolerance:
    """E6-T5: provider context-manager initialisation failures are isolated.

    If one (or more) providers raise during ``__aenter__`` only the affected
    providers are skipped; the cycle still runs for healthy providers and
    ``run_once`` does *not* propagate the provider-level exception.
    """

    # ------------------------------------------------------------------
    # Shared helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _base_settings() -> MagicMock:
        """Minimal settings stub that doesn't require real env vars."""
        s = MagicMock(spec=Settings)
        s.database_path = ":memory:"
        s.database_path_resolved = MagicMock()
        s.database_path_resolved.parent.mkdir = MagicMock()
        s.telegram_configured = False
        s.immobiliare_vrt = None
        s.casa_search_url = None
        s.to_filter_criteria.return_value = MagicMock()
        return s

    @staticmethod
    def _bad_provider(source_label: str = "immobiliare") -> MagicMock:
        """Provider whose ``__aenter__`` always raises RuntimeError."""
        from rentbot.core.models import ListingSource

        p = MagicMock()
        p.source = ListingSource(source_label)
        p.__aenter__ = AsyncMock(side_effect=RuntimeError("session init failed"))
        p.__aexit__ = AsyncMock(return_value=None)
        return p

    @staticmethod
    def _good_provider(source_label: str = "casa") -> MagicMock:
        """Provider whose ``__aenter__`` succeeds and returns itself."""
        from rentbot.core.models import ListingSource

        p = MagicMock()
        p.source = ListingSource(source_label)
        p.__aenter__ = AsyncMock(return_value=p)
        p.__aexit__ = AsyncMock(return_value=None)
        return p

    # ------------------------------------------------------------------
    # Tests
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_healthy_provider_runs_despite_sibling_init_failure(self) -> None:
        """One provider fails __aenter__; the other is still passed to run_cycle."""
        from rentbot.orchestrator.runner import run_once

        bad = self._bad_provider("immobiliare")
        good = self._good_provider("casa")

        # Capture the providers arg actually passed to run_cycle.
        captured: list = []

        async def _spy_run_cycle(**kwargs: object) -> CycleStats:
            captured.extend(kwargs.get("providers", []))
            return CycleStats()

        fake_conn = AsyncMock()
        fake_conn.close = AsyncMock()

        with (
            patch("rentbot.orchestrator.runner.open_db", return_value=fake_conn),
            patch(
                "rentbot.orchestrator.runner._build_providers",
                return_value=[bad, good],
            ),
            patch(
                "rentbot.orchestrator.runner.run_cycle",
                new=AsyncMock(side_effect=_spy_run_cycle),
            ),
        ):
            await run_once(
                ctx=RunContext(seed=True, dry_run=False),
                settings=self._base_settings(),
            )

        assert good in captured, "Healthy provider must reach run_cycle"
        assert bad not in captured, "Failed provider must not reach run_cycle"

    @pytest.mark.asyncio
    async def test_provider_init_failure_does_not_raise_from_run_once(self) -> None:
        """run_once must not propagate a provider __aenter__ exception."""
        from rentbot.orchestrator.runner import run_once

        bad = self._bad_provider("immobiliare")

        fake_conn = AsyncMock()
        fake_conn.close = AsyncMock()

        # Should complete without raising.
        with (
            patch("rentbot.orchestrator.runner.open_db", return_value=fake_conn),
            patch(
                "rentbot.orchestrator.runner._build_providers",
                return_value=[bad],
            ),
            patch(
                "rentbot.orchestrator.runner.run_cycle",
                new=AsyncMock(return_value=CycleStats()),
            ),
        ):
            result = await run_once(
                ctx=RunContext(seed=True, dry_run=False),
                settings=self._base_settings(),
            )

        assert isinstance(result, CycleStats)

    @pytest.mark.asyncio
    async def test_provider_init_failure_is_logged_at_error_level(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """An ERROR-level log line must reference the failing provider and cause."""
        import logging

        from rentbot.orchestrator.runner import run_once

        bad = self._bad_provider("immobiliare")

        fake_conn = AsyncMock()
        fake_conn.close = AsyncMock()

        with (
            patch("rentbot.orchestrator.runner.open_db", return_value=fake_conn),
            patch(
                "rentbot.orchestrator.runner._build_providers",
                return_value=[bad],
            ),
            patch(
                "rentbot.orchestrator.runner.run_cycle",
                new=AsyncMock(return_value=CycleStats()),
            ),
            caplog.at_level(logging.ERROR, logger="rentbot.orchestrator.runner"),
        ):
            await run_once(
                ctx=RunContext(seed=True, dry_run=False),
                settings=self._base_settings(),
            )

        error_msgs = " ".join(r.message for r in caplog.records if r.levelno == logging.ERROR)
        assert "immobiliare" in error_msgs.lower()
        assert "initialise" in error_msgs or "init" in error_msgs.lower()

    @pytest.mark.asyncio
    async def test_all_providers_init_failure_cycle_runs_empty(self) -> None:
        """If every provider fails __aenter__, run_cycle is invoked with []."""
        from rentbot.orchestrator.runner import run_once

        bad1 = self._bad_provider("immobiliare")
        bad2 = self._bad_provider("casa")

        captured: list = []

        async def _spy_run_cycle(**kwargs: object) -> CycleStats:
            captured.extend(kwargs.get("providers", []))
            return CycleStats()

        fake_conn = AsyncMock()
        fake_conn.close = AsyncMock()

        with (
            patch("rentbot.orchestrator.runner.open_db", return_value=fake_conn),
            patch(
                "rentbot.orchestrator.runner._build_providers",
                return_value=[bad1, bad2],
            ),
            patch(
                "rentbot.orchestrator.runner.run_cycle",
                new=AsyncMock(side_effect=_spy_run_cycle),
            ),
        ):
            result = await run_once(
                ctx=RunContext(seed=True, dry_run=False),
                settings=self._base_settings(),
            )

        # run_cycle received an empty list — no providers reached it.
        assert captured == []
        # run_once still returns a valid CycleStats, not an exception.
        assert isinstance(result, CycleStats)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _FakeClock:
    """Deterministic clock for circuit breaker tests."""

    def __init__(self, start: float = 0.0) -> None:
        self._now = start

    def __call__(self) -> float:
        return self._now

    def advance(self, seconds: float) -> None:
        self._now += seconds


def _make_fake_provider(
    source: str,
    listings: list | None = None,
) -> MagicMock:
    """Create a MagicMock that quacks like a BaseProvider."""
    from rentbot.core.models import ListingSource

    provider = MagicMock()
    provider.source = ListingSource(source)
    provider.fetch_latest = AsyncMock(return_value=listings or [])
    return provider
