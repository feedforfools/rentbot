"""Per-provider circuit breaker with exponential backoff.

Prevents a failing provider from consuming resources every cycle.  After a
configurable number of consecutive failures the circuit **opens**, skipping
that provider until a recovery timeout elapses.  The timeout grows
exponentially with each successive trip to avoid hammering a persistently-
broken endpoint.

State machine
~~~~~~~~~~~~~
::

    CLOSED ──(failure_threshold reached)──▶ OPEN
      ▲                                       │
      │                                       │ (recovery_timeout elapsed)
      │                                       ▼
      └────(probe succeeds)──── HALF_OPEN ────┘
                                   │
                                   │ (probe fails)
                                   ▼
                                  OPEN  (backoff multiplied)

Terminology
~~~~~~~~~~~
* **Trip** — a transition from CLOSED/HALF_OPEN → OPEN.
* **Probe** — the single ``fetch_latest`` attempt allowed in HALF_OPEN
  state to test whether the provider has recovered.
* **Recovery timeout** — the wall-clock duration the circuit stays OPEN
  before transitioning to HALF_OPEN.  Grows as
  ``base_recovery_timeout × backoff_multiplier^trip_count``
  up to ``max_recovery_timeout``.

Thread-safety
~~~~~~~~~~~~~
The registry is a plain in-process object with no locking.  It is safe for
single-threaded ``asyncio`` usage (which is how Rentbot operates) but is
**not** thread-safe.

Typical usage::

    from rentbot.orchestrator.circuit_breaker import CircuitBreakerRegistry

    cb = CircuitBreakerRegistry(failure_threshold=5)

    if cb.allow_request("immobiliare"):
        try:
            listings = await provider.fetch_latest()
            cb.record_success("immobiliare")
        except Exception:
            cb.record_failure("immobiliare")
    else:
        logger.info("Provider immobiliare is circuit-broken — skipped")
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from enum import StrEnum
from typing import Final

__all__ = [
    "CircuitState",
    "ProviderCircuitState",
    "CircuitBreakerRegistry",
]

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

#: Default number of consecutive failures before the circuit opens.
_DEFAULT_FAILURE_THRESHOLD: Final[int] = 5

#: Default base duration (seconds) the circuit stays OPEN before probing.
_DEFAULT_BASE_RECOVERY_TIMEOUT: Final[float] = 300.0  # 5 minutes

#: Default hard cap on escalated recovery timeout.
_DEFAULT_MAX_RECOVERY_TIMEOUT: Final[float] = 3600.0  # 1 hour

#: Default multiplier applied for each successive trip.
_DEFAULT_BACKOFF_MULTIPLIER: Final[float] = 2.0


# ---------------------------------------------------------------------------
# State types
# ---------------------------------------------------------------------------


class CircuitState(StrEnum):
    """Possible states for a provider circuit breaker."""

    CLOSED = "closed"
    """Normal operation — requests are allowed."""

    OPEN = "open"
    """Provider is failing — requests are blocked until recovery timeout."""

    HALF_OPEN = "half_open"
    """Recovery timeout elapsed — one probe request is allowed."""


@dataclass
class ProviderCircuitState:
    """Mutable state bag for one provider's circuit breaker.

    Attributes:
        state: Current :class:`CircuitState`.
        consecutive_failures: Number of consecutive failures since the last
            success (or since circuit creation).
        last_failure_time: Monotonic timestamp of the most recent failure,
            or ``None`` if no failure has been recorded yet.
        trip_count: Number of times the circuit has transitioned to OPEN.
            Used to compute exponential backoff on the recovery timeout.
    """

    state: CircuitState = CircuitState.CLOSED
    consecutive_failures: int = 0
    last_failure_time: float | None = None
    trip_count: int = 0


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


class CircuitBreakerRegistry:
    """Manages circuit breakers for all providers.

    One instance is created per scheduler loop and passed through each
    ``run_once`` → ``run_cycle`` → ``run_provider`` call chain so that
    failure state persists across cycles.

    Args:
        failure_threshold: Consecutive failures required to trip the circuit.
        base_recovery_timeout: Base seconds to wait in OPEN before probing.
        max_recovery_timeout: Hard ceiling on escalated recovery timeouts.
        backoff_multiplier: Factor applied per successive trip to escalate
            the recovery timeout.
        clock: Callable returning a monotonic timestamp (seconds).  Defaults
            to :func:`time.monotonic`.  Override in tests for deterministic
            behaviour.
    """

    def __init__(
        self,
        failure_threshold: int = _DEFAULT_FAILURE_THRESHOLD,
        base_recovery_timeout: float = _DEFAULT_BASE_RECOVERY_TIMEOUT,
        max_recovery_timeout: float = _DEFAULT_MAX_RECOVERY_TIMEOUT,
        backoff_multiplier: float = _DEFAULT_BACKOFF_MULTIPLIER,
        *,
        clock: type[float] | None = None,
    ) -> None:
        self._failure_threshold = failure_threshold
        self._base_recovery_timeout = base_recovery_timeout
        self._max_recovery_timeout = max_recovery_timeout
        self._backoff_multiplier = backoff_multiplier
        self._clock = clock or time.monotonic
        self._states: dict[str, ProviderCircuitState] = {}

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_state(self, source: str) -> ProviderCircuitState:
        """Return the circuit state for *source*, lazily creating if absent."""
        if source not in self._states:
            self._states[source] = ProviderCircuitState()
        return self._states[source]

    def _effective_recovery_timeout(self, pcs: ProviderCircuitState) -> float:
        """Compute the escalated recovery timeout for a given state.

        Formula: ``base × multiplier^(trip_count - 1)`` capped at
        ``max_recovery_timeout``.  ``trip_count`` is at least 1 when the
        circuit is OPEN, so the first trip uses the base timeout.
        """
        exponent = max(pcs.trip_count - 1, 0)
        raw = self._base_recovery_timeout * (self._backoff_multiplier**exponent)
        return min(raw, self._max_recovery_timeout)

    def _trip(self, source: str, pcs: ProviderCircuitState) -> None:
        """Transition a circuit to OPEN (trip)."""
        pcs.state = CircuitState.OPEN
        pcs.trip_count += 1
        logger.warning(
            "Circuit OPEN for provider %s — trip #%d, "
            "consecutive failures=%d, recovery timeout=%.0f s.",
            source,
            pcs.trip_count,
            pcs.consecutive_failures,
            self._effective_recovery_timeout(pcs),
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def allow_request(self, source: str) -> bool:
        """Check whether a request to *source* should be attempted.

        * **CLOSED** → always ``True``.
        * **HALF_OPEN** → ``True`` (one probe allowed).
        * **OPEN** → ``True`` only if the recovery timeout has elapsed
          (transitions to HALF_OPEN first); otherwise ``False``.

        Args:
            source: Provider source label (e.g. ``"immobiliare"``).

        Returns:
            ``True`` if the caller should proceed with the request.
        """
        pcs = self._get_state(source)

        if pcs.state == CircuitState.CLOSED:
            return True

        if pcs.state == CircuitState.HALF_OPEN:
            # Already in probe mode — allow the single attempt.
            return True

        # OPEN — check if recovery timeout has elapsed.
        assert pcs.last_failure_time is not None  # noqa: S101
        elapsed = self._clock() - pcs.last_failure_time
        timeout = self._effective_recovery_timeout(pcs)

        if elapsed >= timeout:
            pcs.state = CircuitState.HALF_OPEN
            logger.info(
                "Circuit HALF_OPEN for provider %s — "
                "recovery timeout (%.0f s) elapsed, allowing probe.",
                source,
                timeout,
            )
            return True

        logger.debug(
            "Circuit OPEN for provider %s — %.0f / %.0f s until probe.",
            source,
            elapsed,
            timeout,
        )
        return False

    def record_success(self, source: str) -> None:
        """Record a successful fetch for *source*.

        Resets consecutive failures and closes the circuit if it was
        HALF_OPEN (successful probe).

        Args:
            source: Provider source label.
        """
        pcs = self._get_state(source)

        if pcs.state == CircuitState.HALF_OPEN:
            logger.info(
                "Circuit CLOSED for provider %s — probe succeeded after "
                "trip #%d.  Resetting failure counters.",
                source,
                pcs.trip_count,
            )
            pcs.state = CircuitState.CLOSED
            pcs.consecutive_failures = 0
            pcs.trip_count = 0
            pcs.last_failure_time = None
            return

        # CLOSED — just reset the failure counter.
        if pcs.consecutive_failures > 0:
            logger.debug(
                "Provider %s recovered — resetting %d consecutive failure(s).",
                source,
                pcs.consecutive_failures,
            )
        pcs.consecutive_failures = 0

    def record_failure(self, source: str) -> None:
        """Record a failed fetch for *source*.

        Increments the consecutive failure counter.  If the threshold is
        reached (or exceeded), trips the circuit to OPEN.  If the circuit
        was HALF_OPEN (failed probe), it re-opens with an escalated timeout.

        Args:
            source: Provider source label.
        """
        pcs = self._get_state(source)
        pcs.consecutive_failures += 1
        pcs.last_failure_time = self._clock()

        if pcs.state == CircuitState.HALF_OPEN:
            # Probe failed — re-trip with escalated timeout.
            logger.warning(
                "Half-open probe failed for provider %s — re-tripping circuit.",
                source,
            )
            self._trip(source, pcs)
            return

        # CLOSED — check whether the threshold has been reached.
        if pcs.consecutive_failures >= self._failure_threshold:
            self._trip(source, pcs)
        else:
            logger.debug(
                "Provider %s failure %d / %d — circuit remains CLOSED.",
                source,
                pcs.consecutive_failures,
                self._failure_threshold,
            )

    def get_state(self, source: str) -> ProviderCircuitState:
        """Return a snapshot of the circuit state for *source*.

        This is a **reference** to the internal state object — callers should
        treat it as read-only.

        Args:
            source: Provider source label.

        Returns:
            The :class:`ProviderCircuitState` for this provider (lazily
            created if not yet tracked).
        """
        return self._get_state(source)

    def summary(self) -> dict[str, str]:
        """Return a ``{source: state_label}`` dict of all tracked providers.

        Useful for log messages and diagnostics.
        """
        return {src: pcs.state.value for src, pcs in self._states.items()}
