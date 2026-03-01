"""Cumulative cross-cycle statistics for Rentbot (E8-T5).

Tracks lifetime totals across all poll cycles and provides two output
paths:

1. **Log summary** — :meth:`LifetimeStats.format_summary` returns a
   human-readable string suitable for a single ``logger.info()`` call.
2. **JSON stats file** — :func:`write_stats_file` serialises
   :meth:`LifetimeStats.as_dict` to a file (default
   ``/tmp/rentbot_stats.json``, overridable via
   ``RENTBOT_STATS_PATH``).  This gives operators an on-demand snapshot
   via ``cat /tmp/rentbot_stats.json`` or
   ``docker exec <container> cat /tmp/rentbot_stats.json``.

The stats file is rewritten after *every* API cycle (success or failure)
so the timestamp data stays fresh.  Write errors are logged at WARNING
level and never propagated — a stats-file failure must not crash the
polling loop.

Typical usage::

    from rentbot.orchestrator.metrics import LifetimeStats, write_stats_file

    stats = LifetimeStats()

    # Inside the polling loop, after each cycle:
    stats.update(cycle_stats)
    write_stats_file(stats)
    logger.debug("%s", stats.format_summary())
"""

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime

from rentbot.orchestrator.pipeline import CycleStats

__all__ = [
    "STATS_PATH",
    "ProviderLifetimeStats",
    "LifetimeStats",
    "write_stats_file",
]

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Stats file path constant
# ---------------------------------------------------------------------------

#: Destination for the JSON stats snapshot.  Override via the
#: ``RENTBOT_STATS_PATH`` environment variable if ``/tmp`` is not writable.
STATS_PATH: str = os.environ.get("RENTBOT_STATS_PATH", "/tmp/rentbot_stats.json")


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class ProviderLifetimeStats:
    """Accumulated counters for a single provider across all completed cycles.

    Attributes:
        source: Provider source label (e.g. ``"immobiliare"``).
        cycles_contributed: Number of cycles in which this provider was
            active (i.e. its :class:`~rentbot.providers.base.BaseProvider`
            appeared in the cycle's :class:`~rentbot.orchestrator.pipeline
            .CycleStats`).
        failed_cycles: Cycles where the provider's fetch call raised an
            exception.
        fetched: Lifetime total listings returned by the provider.
        new: Lifetime total newly-inserted listings (not previously seen).
        duplicate: Lifetime total deduplicated/skipped listings.
        passed_filter: Lifetime total listings that cleared the heuristic
            filter.
        alerted: Lifetime total successfully delivered alerts.
        errors: Lifetime total per-listing processing errors.
    """

    source: str
    cycles_contributed: int = 0
    failed_cycles: int = 0
    fetched: int = 0
    new: int = 0
    duplicate: int = 0
    passed_filter: int = 0
    alerted: int = 0
    errors: int = 0


@dataclass
class LifetimeStats:
    """Cumulative statistics accumulated across all completed poll cycles.

    Attributes:
        cycles_run: Total number of completed cycles (including failed ones).
        failed_cycles: Cycles where at least one provider failed.
        total_fetched: Lifetime sum of ``fetched`` across all providers.
        total_new: Lifetime sum of newly-seen listings.
        total_duplicate: Lifetime sum of deduplicated listings.
        total_passed_filter: Lifetime sum of filter-passing listings.
        total_alerted: Lifetime sum of successfully delivered alerts.
        total_errors: Lifetime sum of per-listing processing errors.
    """

    cycles_run: int = 0
    failed_cycles: int = 0
    total_fetched: int = 0
    total_new: int = 0
    total_duplicate: int = 0
    total_passed_filter: int = 0
    total_alerted: int = 0
    total_errors: int = 0

    # --- private (excluded from repr for brevity) ---
    _start_monotonic: float = field(default_factory=time.monotonic, repr=False)
    _started_at: datetime = field(
        default_factory=lambda: datetime.now(UTC),
        repr=False,
    )
    _providers: dict[str, ProviderLifetimeStats] = field(
        default_factory=dict,
        repr=False,
    )

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def uptime_s(self) -> float:
        """Seconds since this :class:`LifetimeStats` instance was created."""
        return time.monotonic() - self._start_monotonic

    @property
    def providers(self) -> dict[str, ProviderLifetimeStats]:
        """Read-only view of per-provider lifetime stats, keyed by source name."""
        return self._providers

    # ------------------------------------------------------------------
    # Mutation
    # ------------------------------------------------------------------

    def update(self, cycle_stats: CycleStats) -> None:
        """Accumulate a completed cycle's stats into lifetime totals.

        This method is safe to call after every cycle, regardless of whether
        individual providers failed.  Failed-provider stats (where
        ``provider_failed=True``) are counted in ``failed_cycles`` but their
        per-listing counters (``fetched``, ``new``, etc.) remain zero by
        definition (no listings were retrieved).

        Args:
            cycle_stats: Statistics from the just-completed cycle, as
                returned by :func:`~rentbot.orchestrator.runner.run_once`.
        """
        self.cycles_run += 1

        if cycle_stats.failed_providers:
            self.failed_cycles += 1

        self.total_fetched += cycle_stats.total_fetched
        self.total_new += cycle_stats.total_new
        self.total_duplicate += cycle_stats.total_duplicate
        self.total_passed_filter += cycle_stats.total_passed_filter
        self.total_alerted += cycle_stats.total_alerted
        self.total_errors += cycle_stats.total_errors

        for pstat in cycle_stats.provider_stats:
            if pstat.source not in self._providers:
                self._providers[pstat.source] = ProviderLifetimeStats(source=pstat.source)
            p = self._providers[pstat.source]
            p.cycles_contributed += 1
            if pstat.provider_failed:
                p.failed_cycles += 1
            p.fetched += pstat.fetched
            p.new += pstat.new
            p.duplicate += pstat.duplicate
            p.passed_filter += pstat.passed_filter
            p.alerted += pstat.alerted
            p.errors += pstat.errors

    # ------------------------------------------------------------------
    # Serialisation / formatting
    # ------------------------------------------------------------------

    def format_summary(self) -> str:
        """Return a human-readable multi-line lifetime summary for logging.

        The first line contains the aggregate counters and uptime.  One
        additional indented line per provider follows in source-name order.

        Example output::

            lifetime stats — uptime: 0h14m22s | cycles=8 failed_cycles=0
              fetched=187 new=23 dup=164 passed_filter=15 alerted=15 errors=0
              immobiliare: cycles=8  fetched=124 new=15 dup=109 passed=10 alerted=10
              casa:        cycles=8  fetched=63  new=8  dup=55  passed=5  alerted=5

        Returns:
            Multi-line string suitable for a single ``logger.info()`` call.
        """
        uptime = self.uptime_s
        hours, rem = divmod(int(uptime), 3600)
        minutes, seconds = divmod(rem, 60)

        header = (
            f"lifetime stats — uptime: {hours}h{minutes:02d}m{seconds:02d}s | "
            f"cycles={self.cycles_run} failed_cycles={self.failed_cycles}"
        )
        aggregate = (
            f"  fetched={self.total_fetched} new={self.total_new} "
            f"dup={self.total_duplicate} passed_filter={self.total_passed_filter} "
            f"alerted={self.total_alerted} errors={self.total_errors}"
        )
        lines: list[str] = [header, aggregate]

        for p in sorted(self._providers.values(), key=lambda x: x.source):
            failed_clause = f" failed_cycles={p.failed_cycles}" if p.failed_cycles else ""
            lines.append(
                f"  {p.source}: cycles={p.cycles_contributed}{failed_clause} "
                f"fetched={p.fetched} new={p.new} dup={p.duplicate} "
                f"passed_filter={p.passed_filter} alerted={p.alerted} "
                f"errors={p.errors}"
            )
        return "\n".join(lines)

    def as_dict(self) -> dict[str, object]:
        """Return a JSON-serialisable representation of lifetime stats.

        The ``started_at`` key is an ISO-8601 string in UTC.
        ``uptime_s`` is rounded to one decimal place.

        Returns:
            Dictionary with aggregate and per-provider lifetime statistics.
        """
        return {
            "started_at": self._started_at.isoformat(),
            "uptime_s": round(self.uptime_s, 1),
            "cycles_run": self.cycles_run,
            "failed_cycles": self.failed_cycles,
            "total_fetched": self.total_fetched,
            "total_new": self.total_new,
            "total_duplicate": self.total_duplicate,
            "total_passed_filter": self.total_passed_filter,
            "total_alerted": self.total_alerted,
            "total_errors": self.total_errors,
            "providers": {
                src: {
                    "cycles_contributed": p.cycles_contributed,
                    "failed_cycles": p.failed_cycles,
                    "fetched": p.fetched,
                    "new": p.new,
                    "duplicate": p.duplicate,
                    "passed_filter": p.passed_filter,
                    "alerted": p.alerted,
                    "errors": p.errors,
                }
                for src, p in sorted(self._providers.items())
            },
        }


# ---------------------------------------------------------------------------
# Stats file writer
# ---------------------------------------------------------------------------


def write_stats_file(
    stats: LifetimeStats,
    path: str = STATS_PATH,
) -> None:
    """Write a JSON snapshot of *stats* to *path*.

    Called after every API cycle so the file reflects the most recent
    completed cycle.  Errors are logged at ``WARNING`` level and never
    propagated — a write failure must not crash the polling loop.

    Args:
        stats: Current :class:`LifetimeStats` instance to serialise.
        path: Destination file path.  Defaults to :data:`STATS_PATH`.
    """
    try:
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(stats.as_dict(), fh, indent=2)
    except OSError:
        logger.warning("Failed to write stats file '%s'.", path, exc_info=True)
