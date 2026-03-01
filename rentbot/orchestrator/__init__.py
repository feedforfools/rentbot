"""Scheduling, task execution, failure isolation, and backoff controls.

Public API
----------
* :func:`~rentbot.orchestrator.scheduler.run_continuous` — default runtime
  entry-point; runs API and browser loops indefinitely with randomised sleep
  intervals.
* :func:`~rentbot.orchestrator.runner.run_once` — single poll cycle; used
  by :func:`run_continuous` and exposed for ``--once`` mode / testing.
* :func:`~rentbot.orchestrator.pipeline.run_cycle` — concurrent per-provider
  gather-and-process loop; useful for testing or custom orchestration.
* :func:`~rentbot.orchestrator.scheduler.next_api_interval` /
  :func:`~rentbot.orchestrator.scheduler.next_browser_interval` — interval
  jitter helpers; also exposed for testing.
* :class:`~rentbot.orchestrator.circuit_breaker.CircuitBreakerRegistry` —
  per-provider circuit breaker with exponential backoff.
* :class:`~rentbot.orchestrator.metrics.LifetimeStats` /
  :class:`~rentbot.orchestrator.metrics.ProviderLifetimeStats` —
  cumulative cross-cycle statistics tracker (E8-T5).
* :func:`~rentbot.orchestrator.metrics.write_stats_file` — serialise
  lifetime stats to a JSON file for operator inspection.
"""

from rentbot.orchestrator.circuit_breaker import (
    CircuitBreakerRegistry,
    CircuitState,
    ProviderCircuitState,
)
from rentbot.orchestrator.metrics import (
    LifetimeStats,
    ProviderLifetimeStats,
    write_stats_file,
)
from rentbot.orchestrator.pipeline import (
    CycleStats,
    ProviderCycleStats,
    process_listing,
    run_cycle,
    run_provider,
)
from rentbot.orchestrator.runner import run_once
from rentbot.orchestrator.scheduler import (
    next_api_interval,
    next_browser_interval,
    run_continuous,
)

__all__ = [
    # Circuit breaker (E6-T3)
    "CircuitBreakerRegistry",
    "CircuitState",
    "ProviderCircuitState",
    # Continuous scheduler (E6-T2)
    "run_continuous",
    "next_api_interval",
    "next_browser_interval",
    # Single-cycle entry-point (E6-T1)
    "run_once",
    # Pipeline primitives
    "CycleStats",
    "ProviderCycleStats",
    "process_listing",
    "run_cycle",
    "run_provider",
    # Lifetime metrics (E8-T5)
    "LifetimeStats",
    "ProviderLifetimeStats",
    "write_stats_file",
]
