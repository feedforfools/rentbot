"""Runtime context for a single Rentbot execution.

Encapsulates the user-selected operating modes that alter pipeline behaviour
without changing any configuration values.  A single :class:`RunContext`
instance is created once in :mod:`rentbot.__main__` and threaded through
every layer — orchestrator, providers, storage, notifiers — so each layer can
inspect the flags without needing access to the raw CLI args.

Current flags
-------------
seed
    The caller wants to populate the ``seen_listings`` DB with the current
    set of live listings **without sending any Telegram alerts**.  This
    prevents the first-run flood: when the DB is empty, every listing looks
    "new."  Running in seed mode first silently marks all current listings as
    seen; subsequent normal runs then only alert on genuinely new ones.

    Pipeline behaviour when ``seed=True``:

    * Providers **still run** — we need real listing IDs to populate the DB.
    * The deduplication + storage layer **still runs** (that's the point).
    * The filter layer **still runs** — filter results are recorded for
      future re-evaluation.
    * The notifier layer **skips all sends** — :attr:`should_notify` is
      ``False``.

dry_run
    Run the full pipeline including the notifier formatter, but **log the
    alert payload** instead of POSTing to Telegram.  Useful for local
    development and CI validation.

    Pipeline behaviour when ``dry_run=True`` (and ``seed=False``):

    * Everything runs as normal up to the send step.
    * The notifier logs the formatted message at ``INFO`` level and returns
      without calling the Telegram API.

    Note: ``seed=True`` takes precedence over ``dry_run=True``.  If both are
    set, no notifications are produced and a warning is logged.

:attr:`should_notify` is the single property every layer should read:

    >>> ctx = RunContext(seed=False, dry_run=False)
    >>> ctx.should_notify
    True

    >>> RunContext(seed=True).should_notify
    False

    >>> RunContext(dry_run=True).should_notify
    False

Typical usage::

    from rentbot.core.run_context import RunContext

    ctx = RunContext(seed=args.seed, dry_run=args.dry_run)

    # In the notifier:
    if not ctx.should_notify:
        logger.info("Skipping Telegram send (seed=%s dry_run=%s)", ctx.seed, ctx.dry_run)
        return

    # In bulk-ingestion loop (seed mode: insert, never alert):
    cids = await repo.bulk_insert(listings)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

__all__ = ["RunContext"]

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RunContext:
    """Immutable container for per-run operating-mode flags.

    Instances are created once at process startup and passed by value through
    the pipeline.  Being frozen (immutable) ensures no layer accidentally
    flips a flag mid-run.

    Attributes:
        seed: When ``True``, the pipeline ingests listings into the DB but
            sends **no Telegram alerts**.  Used on first deployment to silence
            the initial flood of "new" listings.
        dry_run: When ``True``, the pipeline runs end-to-end except the
            notifier sends nothing — it logs the payload instead.  Ignored
            when ``seed=True``.
    """

    seed: bool = field(default=False)
    dry_run: bool = field(default=False)

    def __post_init__(self) -> None:
        if self.seed and self.dry_run:
            logger.warning(
                "Both --seed and --dry-run are set; --seed takes precedence "
                "and no notifications will be produced."
            )

    # ------------------------------------------------------------------
    # Derived properties
    # ------------------------------------------------------------------

    @property
    def should_notify(self) -> bool:
        """Return ``True`` if the notifier should actually send messages.

        Returns ``False`` whenever either :attr:`seed` or :attr:`dry_run` is
        active.  Every layer that would produce an alert must gate on this
        property instead of checking the individual flags.

        Returns:
            ``True`` for a normal (live) run; ``False`` for seed or dry-run.
        """
        return not self.seed and not self.dry_run

    @property
    def mode_label(self) -> str:
        """Human-readable label for the current mode, used in log lines.

        Returns:
            ``"seed"``, ``"dry-run"``, or ``"live"``.
        """
        if self.seed:
            return "seed"
        if self.dry_run:
            return "dry-run"
        return "live"

    def __str__(self) -> str:
        return (
            f"RunContext(mode={self.mode_label}, "
            f"should_notify={self.should_notify})"
        )
