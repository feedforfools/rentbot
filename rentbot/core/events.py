"""Structured log event name constants for the Rentbot pipeline.

Every key transition in the orchestrator emits a log record with an
``event`` field (passed via ``extra={"event": events.X}``).  Using named
constants instead of raw strings gives:

* **Greppability** — ``grep 'event=CYCLE_START' app.log`` locates all cycle
  starts instantly.
* **Type safety** — IDEs and linters can catch typos.
* **Discoverability** — all observable events are documented in one place.
* **JSON observability** — in ``LOG_FORMAT=json`` mode, ``event`` appears
  under the ``extra`` key of each emitted JSON object, making it trivial to
  write structured log queries (e.g. in Loki or CloudWatch Insights).

Usage example::

    import logging
    from rentbot.core import events

    logger = logging.getLogger(__name__)

    logger.info("Cycle started", extra={"event": events.CYCLE_START})

In text mode the ``event`` value is not interpolated into the log line (the
message text is self-describing).  In JSON mode it surfaces as
``extra.event``.
"""

from __future__ import annotations

__all__ = [
    # Cycle lifecycle
    "CYCLE_START",
    "CYCLE_COMPLETE",
    "CYCLE_ABORT",
    # Provider lifecycle
    "PROVIDER_FETCH_OK",
    "PROVIDER_FETCH_ERROR",
    "PROVIDER_CIRCUIT_OPEN",
    "PROVIDER_INIT_ERROR",
    "PROVIDER_DONE",
    # Listing pipeline stages
    "LISTING_DUPLICATE",
    "LISTING_NEW",
    "LISTING_FILTERED",
    "LISTING_ALERTED",
    "LISTING_NOTIFY_ERROR",
]

# ---------------------------------------------------------------------------
# Cycle lifecycle
# ---------------------------------------------------------------------------

#: Emitted once at the very start of :func:`~rentbot.orchestrator.runner.run_once`.
CYCLE_START: str = "CYCLE_START"

#: Emitted once when :func:`~rentbot.orchestrator.runner.run_once` finishes
#: successfully and logs the cycle summary report.
CYCLE_COMPLETE: str = "CYCLE_COMPLETE"

#: Emitted when :func:`~rentbot.orchestrator.runner.run_once` aborts early
#: (e.g. missing Telegram credentials in live mode).
CYCLE_ABORT: str = "CYCLE_ABORT"

# ---------------------------------------------------------------------------
# Provider lifecycle
# ---------------------------------------------------------------------------

#: Provider fetched listings without raising an exception.
PROVIDER_FETCH_OK: str = "PROVIDER_FETCH_OK"

#: Provider's ``fetch_latest()`` raised an exception; provider skipped.
PROVIDER_FETCH_ERROR: str = "PROVIDER_FETCH_ERROR"

#: Provider's circuit breaker is OPEN; provider skipped for this cycle.
PROVIDER_CIRCUIT_OPEN: str = "PROVIDER_CIRCUIT_OPEN"

#: Provider failed to enter its async context (init-time error); skipped.
PROVIDER_INIT_ERROR: str = "PROVIDER_INIT_ERROR"

#: Provider completed processing all its listings for the cycle.
PROVIDER_DONE: str = "PROVIDER_DONE"

# ---------------------------------------------------------------------------
# Listing pipeline stages
# ---------------------------------------------------------------------------

#: Listing was already in the dedup DB; skipped without re-processing.
LISTING_DUPLICATE: str = "LISTING_DUPLICATE"

#: Listing is new; inserted into the DB (store-before-filter contract).
LISTING_NEW: str = "LISTING_NEW"

#: Listing failed the heuristic filter; stored but no alert sent.
LISTING_FILTERED: str = "LISTING_FILTERED"

#: Listing passed the filter and an alert was successfully dispatched.
LISTING_ALERTED: str = "LISTING_ALERTED"

#: Notification attempt failed; error counter incremented.
LISTING_NOTIFY_ERROR: str = "LISTING_NOTIFY_ERROR"
