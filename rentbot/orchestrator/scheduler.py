"""Continuous polling scheduler for Rentbot.

Drives repeated poll cycles with randomised sleep intervals between runs.
Randomised jitter is an explicit anti-ban measure — predictable, metronomic
access patterns increase the risk of IP-level throttling on Italian real-
estate portals.

Two provider cadences are tracked independently:

* **API cadence** — governs API-first providers (Immobiliare, Casa).
  Controlled by ``POLL_INTERVAL_API_MIN`` / ``POLL_INTERVAL_API_MAX``
  (defaults: 300 s / 600 s).

* **Browser cadence** — reserved for browser-based providers (Facebook,
  Idealista).  Controlled by ``POLL_INTERVAL_BROWSER_MIN`` /
  ``POLL_INTERVAL_BROWSER_MAX`` (defaults: 900 s / 1200 s).
  Browser providers are not yet active (Epic 5); this loop is a placeholder
  so the dual-cadence architecture is in place when they land.

Architecture
~~~~~~~~~~~~
The scheduler uses ``asyncio.sleep`` for interval management — no external
scheduler library is required.  Resource lifecycle is fully owned by
:func:`~rentbot.orchestrator.runner.run_once`, which opens and closes the
database connection and all provider sessions on every cycle.  The scheduler
itself is stateless: it only tracks the next sleep interval.

Typical usage::

    import asyncio
    from rentbot.core.run_context import RunContext
    from rentbot.orchestrator.scheduler import run_continuous

    asyncio.run(run_continuous(RunContext()))
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import random
import signal
import time
from typing import NoReturn

from rentbot.core.run_context import RunContext
from rentbot.core.settings import Settings
from rentbot.orchestrator.circuit_breaker import CircuitBreakerRegistry
from rentbot.orchestrator.runner import run_once

__all__ = [
    "HEARTBEAT_PATH",
    "next_api_interval",
    "next_browser_interval",
    "run_continuous",
]

# ---------------------------------------------------------------------------
# Health-check heartbeat (E7-T4)
# ---------------------------------------------------------------------------

#: Path to the heartbeat file written after each API cycle.
#: The Docker ``HEALTHCHECK`` in ``Dockerfile.core`` reads this file and
#: considers the container unhealthy if the timestamp is stale
#: (older than ``HEARTBEAT_STALE_AFTER_S`` seconds).
#: Override via the ``RENTBOT_HEARTBEAT_PATH`` environment variable if
#: the default ``/tmp`` location is not writable.
HEARTBEAT_PATH: str = os.environ.get("RENTBOT_HEARTBEAT_PATH", "/tmp/rentbot_heartbeat")

#: Threshold used by the container health check.  Exposed as a constant so
#: the Dockerfile and the code stay in sync — 1800 s = 3 × max API interval
#: (default max = 600 s).  Generous enough to tolerate consecutive cycle
#: failures without immediately marking the container unhealthy.
HEARTBEAT_STALE_AFTER_S: int = 1800


def _write_heartbeat(path: str = HEARTBEAT_PATH) -> None:
    """Write the current epoch timestamp to the heartbeat file.

    Called at the end of every :func:`_api_loop` iteration (success *and*
    failure) so that the Docker health check can distinguish a live-but-
    failing process from a completely hung or OOM-killed one.

    Errors are logged at WARNING level and never propagated — a heartbeat
    write failure must not crash the polling loop.

    Args:
        path: Destination file path.  Defaults to :data:`HEARTBEAT_PATH`.
    """
    try:
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(str(time.time()))
    except OSError:
        logger.warning("Failed to write heartbeat file '%s'.", path, exc_info=True)


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Interval helpers (pure — safe to test without async)
# ---------------------------------------------------------------------------


def next_api_interval(settings: Settings) -> float:
    """Return a randomised sleep interval for the API polling loop.

    Draws uniformly from
    ``[poll_interval_api_min, poll_interval_api_max]``.

    Args:
        settings: Active application settings.

    Returns:
        Seconds to sleep before the next API cycle.
    """
    return random.uniform(
        settings.poll_interval_api_min,
        settings.poll_interval_api_max,
    )


def next_browser_interval(settings: Settings) -> float:
    """Return a randomised sleep interval for the browser polling loop.

    Draws uniformly from
    ``[poll_interval_browser_min, poll_interval_browser_max]``.

    Args:
        settings: Active application settings.

    Returns:
        Seconds to sleep before the next browser cycle.
    """
    return random.uniform(
        settings.poll_interval_browser_min,
        settings.poll_interval_browser_max,
    )


# ---------------------------------------------------------------------------
# Internal polling loops
# ---------------------------------------------------------------------------


async def _api_loop(ctx: RunContext, settings: Settings) -> NoReturn:
    """Run the API provider cycle on a randomised repeating interval.

    Each iteration calls :func:`~rentbot.orchestrator.runner.run_once`,
    which owns all resource lifecycle (DB connection, provider HTTP sessions,
    Telegram client).  Any unhandled exception is caught and logged so that a
    transient failure in one cycle does not halt the scheduler — the loop
    always waits the configured interval and retries.

    A :class:`~rentbot.orchestrator.circuit_breaker.CircuitBreakerRegistry`
    is created once and passed to every cycle so that per-provider failure
    history persists across iterations.

    Args:
        ctx: Runtime operating-mode flags (seed / dry-run / live).
        settings: Active application settings with interval bounds.
    """
    logger.info(
        "API polling loop started — interval range: %d–%d s.",
        settings.poll_interval_api_min,
        settings.poll_interval_api_max,
    )

    circuit_breaker = CircuitBreakerRegistry()

    while True:
        try:
            await run_once(
                ctx=ctx,
                settings=settings,
                circuit_breaker=circuit_breaker,
            )
        except Exception:
            logger.exception("Unhandled exception in API cycle — will retry after interval.")

        # Write heartbeat after every iteration (success or failure) so the
        # Docker health check can confirm the process is alive and looping.
        _write_heartbeat()

        cb_summary = circuit_breaker.summary()
        if cb_summary:
            logger.debug("Circuit breaker state: %s", cb_summary)

        interval = next_api_interval(settings)
        logger.info("Next API poll in %.0f s.", interval)
        await asyncio.sleep(interval)


async def _browser_loop(ctx: RunContext, settings: Settings) -> NoReturn:
    """Run the browser provider cycle on a randomised repeating interval.

    .. note::
        Browser providers (Facebook, Idealista) are not yet implemented
        (Epic 5).  This loop is a placeholder so the dual-cadence scheduler
        architecture is in place when browser support lands.  It currently
        sleeps and logs a trace message each iteration without doing any
        real work.

    Args:
        ctx: Runtime operating-mode flags.
        settings: Active application settings with browser interval bounds.
    """
    logger.info(
        "Browser polling loop started (placeholder — no active browser "
        "providers yet) — interval range: %d–%d s.",
        settings.poll_interval_browser_min,
        settings.poll_interval_browser_max,
    )

    while True:
        logger.debug("Browser cycle tick — no active browser providers (Epic 5 pending).")
        interval = next_browser_interval(settings)
        logger.debug("Next browser poll in %.0f s.", interval)
        await asyncio.sleep(interval)


# ---------------------------------------------------------------------------
# Public entry-point
# ---------------------------------------------------------------------------


async def run_continuous(
    ctx: RunContext,
    settings: Settings | None = None,
) -> NoReturn:
    """Run Rentbot continuously with randomised polling intervals.

    Starts two independent ``asyncio`` tasks — one for API providers and
    one (placeholder) for browser providers — and runs them concurrently
    via :func:`asyncio.gather`.  The function never returns normally.  Stop
    the process with ``SIGINT`` (Ctrl+C) or ``SIGTERM``.

    **Graceful shutdown (E6-T6):** A ``SIGTERM`` handler is registered on
    the running event loop immediately after the polling tasks are created.
    When ``SIGTERM`` is received (e.g. from ``docker stop``), both tasks are
    cancelled, the current provider cycle is allowed to finish its current
    ``await`` point, and then resource teardown proceeds via the existing
    :class:`contextlib.AsyncExitStack` in
    :func:`~rentbot.orchestrator.runner.run_once`.  The handler is removed in
    a ``finally`` block so it does not interfere with any subsequent
    :func:`asyncio.run` call.  ``SIGINT`` (Ctrl+C) is handled by Python's
    default asyncio behaviour which raises :exc:`KeyboardInterrupt` / cancels
    the main task — no additional handler is required.

    If either loop raises an unhandled ``BaseException`` (e.g. a
    ``CancelledError`` during shutdown), :func:`asyncio.gather` re-raises it
    here so the event loop can perform top-level cleanup.

    Args:
        ctx: Runtime operating-mode flags (seed / dry-run / live).
        settings: Application settings.  Loaded from environment if ``None``.

    Raises:
        asyncio.CancelledError: When the event loop is cancelled (normal
            shutdown path via Ctrl+C or SIGTERM).
        Exception: Any unhandled exception that escapes both loops.
    """
    if settings is None:
        settings = Settings()

    logger.info(
        "Rentbot entering continuous mode — "
        "api interval: %d–%d s | browser interval: %d–%d s (Epic 5 placeholder).",
        settings.poll_interval_api_min,
        settings.poll_interval_api_max,
        settings.poll_interval_browser_min,
        settings.poll_interval_browser_max,
    )

    api_task = asyncio.create_task(
        _api_loop(ctx=ctx, settings=settings),
        name="rentbot-api-loop",
    )
    browser_task = asyncio.create_task(
        _browser_loop(ctx=ctx, settings=settings),
        name="rentbot-browser-loop",
    )

    # ------------------------------------------------------------------
    # Graceful shutdown (E6-T6) — SIGTERM handler
    # ------------------------------------------------------------------
    loop = asyncio.get_running_loop()
    # One-element mutable cell so the inner closure can write to it.
    _shutdown_signal: list[str] = []

    def _request_graceful_shutdown(signame: str) -> None:
        """Cancel running tasks when a termination signal is received.

        Idempotent: the 'graceful shutdown' log line is emitted only once
        even if the handler fires multiple times (e.g. impatient Ctrl+C).
        Task cancellation itself is safe to call repeatedly.
        """
        if not _shutdown_signal:
            _shutdown_signal.append(signame)
            logger.info(
                "Received %s — graceful shutdown requested; cancelling active tasks.",
                signame,
            )
        api_task.cancel()
        browser_task.cancel()

    loop.add_signal_handler(signal.SIGTERM, lambda: _request_graceful_shutdown("SIGTERM"))

    try:
        # Both loops are infinite; gather propagates the first exception
        # (including CancelledError on shutdown).
        await asyncio.gather(api_task, browser_task)
    except (asyncio.CancelledError, KeyboardInterrupt):
        if _shutdown_signal:
            logger.info(
                "Graceful shutdown complete (signal: %s).",
                _shutdown_signal[0],
            )
        else:
            logger.info("Continuous loop cancelled — stopping tasks.")
        api_task.cancel()
        browser_task.cancel()
        # Await cancellation so tasks can clean up.
        await asyncio.gather(api_task, browser_task, return_exceptions=True)
        raise
    finally:
        # Always deregister the signal handler so it does not interfere
        # with any subsequent asyncio.run() call or test teardown.
        with contextlib.suppress(Exception):
            loop.remove_signal_handler(signal.SIGTERM)

    # asyncio.gather should never return normally from infinite loops.
    # The except block always re-raises, making this unreachable.
    raise RuntimeError("run_continuous exited unexpectedly — this is a bug")
