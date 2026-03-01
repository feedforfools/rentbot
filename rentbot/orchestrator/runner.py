"""Orchestrator entry-point: assemble all components and execute one poll cycle.

This module provides :func:`run_once`, the top-level async function invoked
by :mod:`rentbot.__main__` for each poll cycle.  The scheduler (E6-T2) will
call :func:`run_once` repeatedly on a randomised timed loop.

Component wiring
----------------
Each call to :func:`run_once`:

1. Loads :class:`~rentbot.core.settings.Settings` (or uses the supplied
   instance).
2. Ensures the SQLite database directory exists and opens the connection
   via :func:`~rentbot.storage.database.open_db`.
3. Creates a :class:`~rentbot.storage.repository.ListingRepository`,
   :class:`~rentbot.filters.heuristic.HeuristicFilter`, and a
   :class:`~rentbot.notifiers.notifier.Notifier`.
4. Instantiates all enabled :class:`~rentbot.providers.base.BaseProvider`
   instances and enters their async context managers via
   :class:`contextlib.AsyncExitStack`.
5. Calls :func:`~rentbot.orchestrator.pipeline.run_cycle` — the core
   ``asyncio.gather``-based concurrent provider loop.
6. Tears down every resource cleanly on exit, including on exceptions.

Provider enablement
-------------------
API providers are enabled by setting the corresponding environment variable:

- ``IMMOBILIARE_VRT``  → enables
  :class:`~rentbot.providers.api.immobiliare.ImmobiliareProvider`
- ``CASA_SEARCH_URL``  → enables
  :class:`~rentbot.providers.api.casa.CasaProvider`

Telegram / dry-run behaviour
-----------------------------
In **live mode** (``ctx.should_notify is True``) Telegram credentials
(``TELEGRAM_BOT_TOKEN``, ``TELEGRAM_CHAT_ID``) must be configured, or
:func:`run_once` raises :exc:`~rentbot.core.exceptions.ConfigError` before
any network I/O is attempted.

In **dry-run** and **seed** modes, Telegram credentials are optional.  When
absent, a :class:`~rentbot.notifiers.telegram.TelegramClient` is still
constructed with harmless placeholder credentials.  The Notifier will never
call ``send_message`` in these modes, so no actual HTTP request is made.

Typical usage::

    import asyncio
    from rentbot.core.run_context import RunContext
    from rentbot.orchestrator.runner import run_once

    ctx = RunContext(seed=False, dry_run=True)
    stats = asyncio.run(run_once(ctx))
    print(stats)
"""

from __future__ import annotations

import logging
import time
from contextlib import AsyncExitStack

from rentbot.core.exceptions import ConfigError
from rentbot.core.run_context import RunContext
from rentbot.core.settings import Settings
from rentbot.filters.heuristic import HeuristicFilter
from rentbot.notifiers.notifier import Notifier
from rentbot.notifiers.telegram import TelegramClient
from rentbot.orchestrator.circuit_breaker import CircuitBreakerRegistry
from rentbot.orchestrator.pipeline import CycleStats, run_cycle
from rentbot.providers.api.casa import CasaProvider
from rentbot.providers.api.immobiliare import ImmobiliareProvider
from rentbot.providers.base import BaseProvider
from rentbot.storage.database import open_db
from rentbot.storage.repository import ListingRepository

__all__ = ["run_once"]

logger = logging.getLogger(__name__)

# Placeholder credentials used to satisfy TelegramClient's constructor when
# the operating mode (dry-run / seed) guarantees no real message will be sent.
_PLACEHOLDER_TOKEN: str = "placeholder:dry_run_or_seed"
_PLACEHOLDER_CHAT_ID: str = "0"


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _build_providers(settings: Settings) -> list[BaseProvider]:
    """Instantiate all enabled API providers.

    A provider is **enabled** when its mandatory configuration field is set:

    * :class:`~rentbot.providers.api.immobiliare.ImmobiliareProvider` —
      requires non-empty ``immobiliare_vrt``.
    * :class:`~rentbot.providers.api.casa.CasaProvider` —
      requires non-empty ``casa_search_url``.

    Providers whose config is absent are logged at ``INFO`` level and omitted.

    Args:
        settings: Loaded :class:`~rentbot.core.settings.Settings` instance.

    Returns:
        Ordered list of provider instances ready to be used as async context
        managers.  May be empty if no providers are configured.
    """
    providers: list[BaseProvider] = []

    if settings.immobiliare_vrt:
        providers.append(ImmobiliareProvider(settings))
        logger.debug("Immobiliare provider enabled.")
    else:
        logger.info("Immobiliare provider disabled — set IMMOBILIARE_VRT in .env to enable.")

    if settings.casa_search_url:
        providers.append(CasaProvider(settings))
        logger.debug("Casa provider enabled.")
    else:
        logger.info("Casa provider disabled — set CASA_SEARCH_URL in .env to enable.")

    return providers


# ---------------------------------------------------------------------------
# Public entry-point
# ---------------------------------------------------------------------------


async def run_once(
    ctx: RunContext,
    settings: Settings | None = None,
    circuit_breaker: CircuitBreakerRegistry | None = None,
) -> CycleStats:
    """Execute a single full poll cycle.

    Assembles all runtime components — database, repository, heuristic filter,
    Telegram notifier, and enabled providers — then delegates concurrent
    provider polling to :func:`~rentbot.orchestrator.pipeline.run_cycle`.
    All resources are released on exit regardless of exceptions.

    Args:
        ctx: Runtime operating-mode flags (seed / dry-run / live).
        settings: Pre-loaded :class:`~rentbot.core.settings.Settings`
            instance.  If ``None``, a fresh instance is loaded from the
            environment and ``.env`` file.
        circuit_breaker: Optional :class:`CircuitBreakerRegistry` that
            persists across cycles.  When provided, providers whose circuit
            is OPEN are skipped and fetch outcomes drive state transitions.

    Returns:
        A :class:`~rentbot.orchestrator.pipeline.CycleStats` record
        summarising the cycle: listings fetched, new, deduplicated, filtered,
        alerted, and any per-listing errors.

    Raises:
        ConfigError: If the process is in live mode but Telegram credentials
            are absent.
        Exception: Any unhandled exception from DB open or resource teardown
            propagates to the caller.  Provider-level and per-listing errors
            are isolated inside :func:`~rentbot.orchestrator.pipeline.run_cycle`
            and do not propagate here.
    """
    if settings is None:
        settings = Settings()

    t0: float = time.monotonic()

    logger.info(
        "run_once starting — mode=%s db=%s",
        ctx.mode_label,
        settings.database_path,
    )

    # ------------------------------------------------------------------
    # Guard: live mode requires Telegram credentials.
    # ------------------------------------------------------------------
    if ctx.should_notify and not settings.telegram_configured:
        raise ConfigError(
            "Live mode requires Telegram credentials. "
            "Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID in .env (or env vars)."
        )

    # ------------------------------------------------------------------
    # Resolve Telegram credentials.
    # In dry-run / seed modes a placeholder satisfies TelegramClient's
    # non-empty-string requirement without triggering any real HTTP call.
    # ------------------------------------------------------------------
    if settings.telegram_configured:
        tg_token = settings.telegram_bot_token
        tg_chat_id = settings.telegram_chat_id
    else:
        tg_token = _PLACEHOLDER_TOKEN
        tg_chat_id = _PLACEHOLDER_CHAT_ID
        logger.debug(
            "Telegram not configured — using placeholder credentials (safe in %s mode).",
            ctx.mode_label,
        )

    # ------------------------------------------------------------------
    # Ensure the DB directory exists before opening the connection.
    # ------------------------------------------------------------------
    db_path = settings.database_path_resolved
    db_path.parent.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Open the database connection (single writer for the full cycle).
    # ------------------------------------------------------------------
    conn = await open_db(db_path)
    try:
        repo = ListingRepository(conn)
        criteria = settings.to_filter_criteria()
        hf = HeuristicFilter(criteria)
        providers = _build_providers(settings)

        if not providers:
            logger.warning(
                "No providers are enabled. "
                "Set IMMOBILIARE_VRT or CASA_SEARCH_URL in .env to enable them."
            )

        # Use AsyncExitStack to manage TelegramClient + all provider contexts
        # in a single deterministic teardown sequence.
        async with AsyncExitStack() as stack:
            # 1 — Enter TelegramClient context (opens the httpx.AsyncClient).
            telegram: TelegramClient = await stack.enter_async_context(
                TelegramClient(token=tg_token, chat_id=tg_chat_id)
            )
            notifier = Notifier(client=telegram, ctx=ctx)

            # 2 — Enter each provider's context (opens per-provider httpx sessions).
            # Failures here (e.g. network error during session setup) are caught
            # per-provider so that one broken provider does not abort the whole cycle.
            active_providers: list[BaseProvider] = []
            for provider in providers:
                try:
                    active: BaseProvider = await stack.enter_async_context(provider)
                    active_providers.append(active)
                except Exception as exc:  # noqa: BLE001
                    logger.error(
                        "Provider %s: failed to initialise (context enter) — "
                        "skipping this cycle: %s",
                        provider.source,
                        exc,
                        exc_info=True,
                    )

            # 3 — Run the concurrent poll cycle.
            stats = await run_cycle(
                providers=active_providers,
                repo=repo,
                hf=hf,
                notifier=notifier,
                ctx=ctx,
                circuit_breaker=circuit_breaker,
            )

        stats.duration_s = time.monotonic() - t0
        logger.info("%s", stats.format_cycle_report())
        for ps in stats.provider_stats:
            logger.debug(
                "  provider %-20s fetched=%-3d new=%-3d dup=%-3d "
                "passed_filter=%-3d alerted=%-3d errors=%-2d%s",
                ps.source + ":",
                ps.fetched,
                ps.new,
                ps.duplicate,
                ps.passed_filter,
                ps.alerted,
                ps.errors,
                " [FAILED]" if ps.provider_failed else "",
            )
        return stats

    finally:
        await conn.close()
        logger.debug("Database connection closed.")
