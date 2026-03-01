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
import logging
import random
from typing import NoReturn

from rentbot.core.run_context import RunContext
from rentbot.core.settings import Settings
from rentbot.orchestrator.circuit_breaker import CircuitBreakerRegistry
from rentbot.orchestrator.runner import run_once

__all__ = [
    "next_api_interval",
    "next_browser_interval",
    "run_continuous",
]

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
            logger.exception(
                "Unhandled exception in API cycle — will retry after interval."
            )

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
    the process with ``SIGINT`` (Ctrl+C) or ``SIGTERM``; graceful shutdown
    (E6-T6) will add clean teardown on those signals.

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

    try:
        # Both loops are infinite; gather propagates the first exception
        # (including CancelledError on shutdown).
        await asyncio.gather(api_task, browser_task)
    except (asyncio.CancelledError, KeyboardInterrupt):
        logger.info("Continuous loop cancelled — stopping tasks.")
        api_task.cancel()
        browser_task.cancel()
        # Await cancellation so tasks can clean up.
        await asyncio.gather(api_task, browser_task, return_exceptions=True)
        raise
