"""Scheduling, task execution, failure isolation, and backoff controls.

Public API
----------
* :func:`~rentbot.orchestrator.runner.run_once` — top-level entry-point for
  one full poll cycle; assembles all components and delegates to
  :func:`~rentbot.orchestrator.pipeline.run_cycle`.
* :func:`~rentbot.orchestrator.pipeline.run_cycle` — concurrent per-provider
  gather-and-process loop; useful for testing or custom orchestration.
"""

from rentbot.orchestrator.pipeline import (
    CycleStats,
    ProviderCycleStats,
    process_listing,
    run_cycle,
    run_provider,
)
from rentbot.orchestrator.runner import run_once

__all__ = [
    # High-level entry-point (E6-T1)
    "run_once",
    # Pipeline primitives
    "CycleStats",
    "ProviderCycleStats",
    "process_listing",
    "run_cycle",
    "run_provider",
]
