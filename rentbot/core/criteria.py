"""Rentbot filtering criteria model.

Defines :class:`FilterCriteria`, the single data structure that describes
*what the user is looking for*.  Every listing is evaluated against these
criteria to decide whether an alert should be sent.

Criteria are loaded once at startup (from env / settings) and passed through
the filter pipeline.  They are **not** mutated at runtime.

Typical usage::

    from rentbot.core.criteria import FilterCriteria

    criteria = FilterCriteria(
        price_max=700,
        rooms_min=2,
        rooms_max=3,
        area_sqm_min=50,
        zones_include=["Centro", "Villanova"],
        keyword_blocklist=["affittasi stanza", "posto letto"],
    )

    if criteria.matches_price(550):
        ...  # proceed
"""

from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING

from pydantic import BaseModel, Field, model_validator

if TYPE_CHECKING:
    from rentbot.core.models import Listing

__all__ = ["FilterCriteria"]

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_WHITESPACE_RE = re.compile(r"\s+")


def _normalise(text: str) -> str:
    """Lowercase and collapse internal whitespace for case-insensitive matching."""
    return _WHITESPACE_RE.sub(" ", text).strip().lower()


# ---------------------------------------------------------------------------
# Model
# ---------------------------------------------------------------------------


class FilterCriteria(BaseModel):
    """User-defined search and alert criteria.

    All bounds are *inclusive*.  ``None`` means "no constraint on this axis."

    Attributes:
        price_min: Minimum acceptable monthly rent in EUR.  Listings below
            this threshold are not alerted on (but are still stored).
        price_max: Maximum acceptable monthly rent in EUR.
        rooms_min: Minimum number of rooms (Italian *locali*).
        rooms_max: Maximum number of rooms.
        area_sqm_min: Minimum floor area in m².
        area_sqm_max: Maximum floor area in m².
        furnished_required: If ``True``, only furnished listings pass.
            ``False`` means only unfurnished.  ``None`` means no constraint.
        zones_include: Allowlist of neighbourhood / district labels.  A
            listing's ``zone`` field is checked with a case-insensitive
            substring match.  Empty list means *no zone restriction*.
        keyword_blocklist: List of substrings that, if found in a listing's
            ``title`` or ``description``, cause it to be rejected.  Useful to
            filter out "stanza singola", "posto letto" (room-share) posts.
            Matching is case-insensitive.  Empty list means *no blocklist*.
    """

    model_config = {"frozen": True}

    # ------------------------------------------------------------------
    # Price
    # ------------------------------------------------------------------
    price_min: int | None = Field(
        None,
        ge=0,
        description="Minimum monthly rent in EUR (inclusive); None = no lower bound.",
    )
    price_max: int | None = Field(
        None,
        ge=0,
        description="Maximum monthly rent in EUR (inclusive); None = no upper bound.",
    )

    # ------------------------------------------------------------------
    # Rooms
    # ------------------------------------------------------------------
    rooms_min: int | None = Field(
        None,
        ge=0,
        description="Minimum number of rooms (inclusive); None = no lower bound.",
    )
    rooms_max: int | None = Field(
        None,
        ge=0,
        description="Maximum number of rooms (inclusive); None = no upper bound.",
    )

    # ------------------------------------------------------------------
    # Area
    # ------------------------------------------------------------------
    area_sqm_min: int | None = Field(
        None,
        ge=0,
        description="Minimum floor area in m² (inclusive); None = no lower bound.",
    )
    area_sqm_max: int | None = Field(
        None,
        ge=0,
        description="Maximum floor area in m² (inclusive); None = no upper bound.",
    )

    # ------------------------------------------------------------------
    # Furnishing
    # ------------------------------------------------------------------
    furnished_required: bool | None = Field(
        None,
        description=(
            "True = only furnished; False = only unfurnished; "
            "None = no constraint."
        ),
    )

    # ------------------------------------------------------------------
    # Location allowlist
    # ------------------------------------------------------------------
    zones_include: list[str] = Field(
        default_factory=list,
        description=(
            "Allowlist of zone/neighbourhood labels (case-insensitive substring "
            "match).  Empty list = no zone restriction."
        ),
    )

    # ------------------------------------------------------------------
    # Keyword blocklist
    # ------------------------------------------------------------------
    keyword_blocklist: list[str] = Field(
        default_factory=list,
        description=(
            "Substrings that disqualify a listing when found in title or "
            "description.  Case-insensitive.  Empty list = no blocklist."
        ),
    )

    # ------------------------------------------------------------------
    # Cross-field validation
    # ------------------------------------------------------------------

    @model_validator(mode="after")
    def _validate_ranges(self) -> "FilterCriteria":
        """Ensure min ≤ max for all bounded pairs."""
        pairs: list[tuple[str, int | None, str, int | None]] = [
            ("price_min", self.price_min, "price_max", self.price_max),
            ("rooms_min", self.rooms_min, "rooms_max", self.rooms_max),
            ("area_sqm_min", self.area_sqm_min, "area_sqm_max", self.area_sqm_max),
        ]
        for lo_name, lo, hi_name, hi in pairs:
            if lo is not None and hi is not None and lo > hi:
                raise ValueError(
                    f"{lo_name} ({lo}) must be ≤ {hi_name} ({hi})"
                )
        return self

    # ------------------------------------------------------------------
    # Matching helpers (used by the filter layer)
    # ------------------------------------------------------------------

    def matches_price(self, price: int) -> bool:
        """Return True if *price* falls within [price_min, price_max].

        Args:
            price: Monthly rent in EUR to check.

        Returns:
            True if the price satisfies the configured bounds.
        """
        if self.price_min is not None and price < self.price_min:
            return False
        if self.price_max is not None and price > self.price_max:
            return False
        return True

    def matches_rooms(self, rooms: int | None) -> bool:
        """Return True if *rooms* satisfies the configured bounds.

        If either bound is set but *rooms* is ``None`` (unknown), the listing
        is **accepted** — it's better to over-alert than silently miss a match
        when the source didn't include room count.

        Args:
            rooms: Number of rooms to check; None means unknown.

        Returns:
            True if the value satisfies the configured bounds (or is unknown).
        """
        if rooms is None:
            return True  # unknown → err on the side of alerting
        if self.rooms_min is not None and rooms < self.rooms_min:
            return False
        if self.rooms_max is not None and rooms > self.rooms_max:
            return False
        return True

    def matches_area(self, area_sqm: int | None) -> bool:
        """Return True if *area_sqm* satisfies the configured bounds.

        Same unknown-passes policy as :meth:`matches_rooms`.

        Args:
            area_sqm: Floor area in m²; None means unknown.

        Returns:
            True if the value satisfies the configured bounds (or is unknown).
        """
        if area_sqm is None:
            return True
        if self.area_sqm_min is not None and area_sqm < self.area_sqm_min:
            return False
        if self.area_sqm_max is not None and area_sqm > self.area_sqm_max:
            return False
        return True

    def matches_furnished(self, furnished: bool | None) -> bool:
        """Return True if *furnished* satisfies the furnishing constraint.

        If ``furnished_required`` is ``None`` there is no constraint and the
        method always returns True.  If the listing's furnished status is
        unknown (``None``), the listing is accepted.

        Args:
            furnished: Listing furnished status; None means unknown.

        Returns:
            True if the furnished status satisfies the constraint.
        """
        if self.furnished_required is None:
            return True
        if furnished is None:
            return True  # unknown → err on the side of alerting
        return furnished == self.furnished_required

    def matches_zone(self, zone: str | None) -> bool:
        """Return True if *zone* matches any entry in the zone allowlist.

        Zone matching uses a **case-insensitive substring** check so that
        e.g. ``zones_include=["centro"]`` matches ``"Centro Storico"``.

        If :attr:`zones_include` is empty there is no restriction and the
        method always returns True.  A listing with an unknown zone (``None``)
        is also accepted when a zone restriction is active — the address field
        may still carry useful info that downstream code can inspect.

        Args:
            zone: Listing zone label; None means unknown.

        Returns:
            True if the zone passes the allowlist check (or list is empty).
        """
        if not self.zones_include:
            return True
        if zone is None:
            return True  # unknown zone → do not filter out
        normalised_zone = _normalise(zone)
        return any(_normalise(z) in normalised_zone for z in self.zones_include)

    def matches_keywords(self, title: str, description: str = "") -> bool:
        """Return True if neither *title* nor *description* contains a blocked keyword.

        Matching is case-insensitive.  If :attr:`keyword_blocklist` is empty
        this method always returns True.

        Args:
            title: Listing headline text.
            description: Listing body text (defaults to empty string).

        Returns:
            True if no blocklisted keyword is found.
        """
        if not self.keyword_blocklist:
            return True
        haystack = _normalise(f"{title} {description}")
        for kw in self.keyword_blocklist:
            if _normalise(kw) in haystack:
                logger.debug("Listing blocked by keyword %r", kw)
                return False
        return True

    def matches(
        self,
        price: int,
        *,
        rooms: int | None = None,
        area_sqm: int | None = None,
        furnished: bool | None = None,
        zone: str | None = None,
        title: str = "",
        description: str = "",
    ) -> tuple[bool, str]:
        """Evaluate all criteria in one call.

        Runs every check in a defined order and returns on the first failure.
        Callers that only need a boolean can ignore the reason string.

        Args:
            price: Monthly rent in EUR.
            rooms: Number of rooms; None if unknown.
            area_sqm: Floor area in m²; None if unknown.
            furnished: Furnished status; None if unknown.
            zone: Zone/neighbourhood label; None if unknown.
            title: Listing headline text.
            description: Listing body text.

        Returns:
            A ``(passed, reason)`` tuple.  *passed* is ``True`` when all
            criteria are satisfied.  *reason* is an empty string on pass, or a
            short human-readable explanation of the first failed check.
        """
        if not self.matches_price(price):
            return False, f"price {price} outside [{self.price_min}, {self.price_max}]"
        if not self.matches_rooms(rooms):
            return False, f"rooms {rooms} outside [{self.rooms_min}, {self.rooms_max}]"
        if not self.matches_area(area_sqm):
            return (
                False,
                f"area {area_sqm} m² outside [{self.area_sqm_min}, {self.area_sqm_max}]",
            )
        if not self.matches_furnished(furnished):
            return False, f"furnished={furnished} does not match required={self.furnished_required}"
        if not self.matches_zone(zone):
            return False, f"zone {zone!r} not in allowlist {self.zones_include}"
        if not self.matches_keywords(title, description):
            kw_hit = next(
                (kw for kw in self.keyword_blocklist if _normalise(kw) in _normalise(f"{title} {description}")),
                "unknown",
            )
            return False, f"blocked by keyword {kw_hit!r}"
        return True, ""

    def matches_listing(self, listing: "Listing") -> tuple[bool, str]:  # noqa: F821
        """Evaluate all criteria against a :class:`~rentbot.core.models.Listing`.

        Convenience wrapper around :meth:`matches` that unpacks the relevant
        fields from a fully-constructed listing object.  This is the preferred
        API inside the ingestion pipeline where a ``Listing`` is always
        available.

        Args:
            listing: The normalised listing to evaluate.

        Returns:
            A ``(passed, reason)`` tuple.  *passed* is ``True`` when all
            criteria are satisfied.  *reason* is ``""`` on pass, or a short
            human-readable explanation of the first failed check.
        """
        return self.matches(
            listing.price,
            rooms=listing.rooms,
            area_sqm=listing.area_sqm,
            furnished=listing.furnished,
            zone=listing.zone,
            title=listing.title,
            description=listing.description,
        )
