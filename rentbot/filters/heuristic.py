"""Heuristic listing filter for Rentbot.

Provides :class:`HeuristicFilter`, a rule-based gate that evaluates a
:class:`~rentbot.core.models.Listing` against the user-configured
:class:`~rentbot.core.criteria.FilterCriteria`.  It covers all
quantitative/qualitative axes that can be checked *without* an LLM:

* **Price** — monthly rent within ``[price_min, price_max]``.
* **Rooms** — room count within ``[rooms_min, rooms_max]``.
* **Area** — floor area within ``[area_sqm_min, area_sqm_max]``.
* **Furnished** — furnished/unfurnished constraint.
* **Zone** — neighbourhood allowlist (case-insensitive substring).
* **Keywords** — title/description blocklist (case-insensitive substring).

LLM-based classification (for unstructured Facebook posts, etc.) is handled
separately in Epic 4's filter modules.

Typical pipeline usage::

    from rentbot.core.criteria import FilterCriteria
    from rentbot.filters.heuristic import HeuristicFilter

    criteria = FilterCriteria(price_max=800, rooms_min=2)
    hf = HeuristicFilter(criteria)

    passing, results = hf.filter_many(raw_listings)
    # ``passing`` contains only the listings that should generate an alert.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from rentbot.core.criteria import FilterCriteria
from rentbot.core.models import Listing

__all__ = ["FilterResult", "HeuristicFilter"]

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class FilterResult:
    """Immutable outcome of a single heuristic filter evaluation.

    Attributes:
        passed: ``True`` if the listing satisfies all active criteria.
        reason: Empty string when passed; human-readable rejection reason
            describing the *first* criteria check that failed.
        listing_id: Raw provider-local id of the evaluated listing (matches
            ``Listing.id``; *not* the canonical ``source:id`` form).
        source: Provider source label (e.g. ``"immobiliare"``).
    """

    passed: bool
    reason: str
    listing_id: str
    source: str


# ---------------------------------------------------------------------------
# Filter
# ---------------------------------------------------------------------------


class HeuristicFilter:
    """Rule-based gate that decides whether a listing should generate an alert.

    Uses :meth:`~rentbot.core.criteria.FilterCriteria.matches_listing` to
    evaluate all configured axes in a single call.  Outcomes are logged so
    the pipeline has a clear audit trail of why each listing was accepted or
    rejected.

    This filter does **not** network, does **not** use an LLM, and does
    **not** mutate any state — it is safe to call concurrently.

    Args:
        criteria: The active search/alert configuration, typically constructed
            from :meth:`~rentbot.core.settings.Settings.to_filter_criteria`.
    """

    def __init__(self, criteria: FilterCriteria) -> None:
        self._criteria = criteria

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def evaluate(self, listing: Listing) -> FilterResult:
        """Evaluate a single listing against the configured criteria.

        Logs the outcome at ``DEBUG`` level for passes and ``INFO`` level for
        rejections so operator logs capture why each listing was dropped.

        Args:
            listing: The normalised listing to evaluate.

        Returns:
            A :class:`FilterResult` describing the outcome.
        """
        passed, reason = self._criteria.matches_listing(listing)
        result = FilterResult(
            passed=passed,
            reason=reason,
            listing_id=listing.id,
            source=listing.source,
        )
        if passed:
            logger.debug(
                "PASS  %s:%s — %r (price=%s, rooms=%s, area=%s)",
                listing.source,
                listing.id,
                listing.title,
                listing.price,
                listing.rooms,
                listing.area_sqm,
            )
        else:
            logger.info(
                "DROP  %s:%s — %s | %r",
                listing.source,
                listing.id,
                reason,
                listing.title,
            )
        return result

    def filter_many(
        self,
        listings: list[Listing],
    ) -> tuple[list[Listing], list[FilterResult]]:
        """Evaluate a batch of listings and return passing listings separately.

        Processes each listing in order and collects results.  A summary
        ``INFO`` log line is emitted after the batch regardless of batch size.

        Args:
            listings: Listings to evaluate.  May be empty.

        Returns:
            A two-element tuple ``(passing_listings, all_results)`` where
            ``passing_listings`` contains only the listings whose
            :class:`FilterResult` has ``passed=True``.  The ``all_results``
            list has the same length and order as the input.
        """
        if not listings:
            logger.debug("filter_many called with empty listing batch — nothing to do")
            return [], []

        all_results: list[FilterResult] = [self.evaluate(listing) for listing in listings]
        passing: list[Listing] = [
            listing for listing, result in zip(listings, all_results) if result.passed
        ]

        logger.info(
            "Heuristic filter: %d/%d listings passed",
            len(passing),
            len(listings),
        )
        return passing, all_results
