"""Subito.it API provider for Rentbot.

Fetches rental listings from the Subito.it Hades JSON search API
(``hades.subito.it/v1/search/items``), captured via browser DevTools.

The provider:
* Builds the correct query-parameter set from :class:`~rentbot.core.settings.Settings`.
* Paginates using ``start`` / ``lim`` offsets up to ``subito_max_pages`` pages.
* Maps each raw JSON ad object to a normalised :class:`~rentbot.core.models.Listing`.
* Extracts structured features (price, rooms, area, furnished, etc.) from the
  ``features`` array on each ad.

Configuration
-------------
``SUBITO_REGION``
    Numeric Subito region code.  Default: ``7`` (Friuli-Venezia Giulia).

``SUBITO_CITY``
    Numeric Subito city/province code.  Default: ``2`` (Pordenone).

``SUBITO_MAX_PAGES``
    Maximum number of result pages to fetch per poll.  Default: 3.

Set ``SUBITO_REGION`` to an empty string to disable the Subito provider.

Typical usage::

    from rentbot.core.settings import Settings
    from rentbot.providers.api.subito import SubitoProvider

    settings = Settings()
    async with SubitoProvider(settings) as provider:
        listings = await provider.fetch_latest()
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from curl_cffi.requests import AsyncSession

from rentbot.core.exceptions import ProviderFetchError
from rentbot.core.models import Listing, ListingSource
from rentbot.core.settings import Settings
from rentbot.providers.base import BaseProvider
from rentbot.providers.normalizers import (
    normalise_text,
    normalise_title,
)

__all__ = ["SubitoProvider"]

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_BASE_URL: str = "https://hades.subito.it"
_SEARCH_PATH: str = "/v1/search/items"

#: Category 7 = Appartamenti on Subito.it.
_CATEGORY_APARTMENTS: str = "7"

#: Ad type "u" = In affitto (rental).
_AD_TYPE_RENTAL: str = "u"

#: Number of results per page (Subito default is 30).
_PAGE_SIZE: int = 30

#: Maximum age for a listing to be considered relevant.  Listings older than
#: this are silently dropped because the Pordenone market moves quickly and
#: stale ads are almost certainly no longer available.
_MAX_LISTING_AGE: timedelta = timedelta(days=45)

#: CDN base URL for constructing full image URLs.
_IMAGE_CDN_BASE: str = "https://images.sbito.it/api/v1/sbt-ads-images-pro/images"

#: Subito listing detail base URL for fallback URL construction.
_LISTING_BASE_URL: str = "https://www.subito.it"

# ---------------------------------------------------------------------------
# Feature extraction helpers (module-level, stateless)
# ---------------------------------------------------------------------------


def _extract_features(features: list[dict[str, Any]]) -> dict[str, Any]:
    """Build a flat dict of feature values from a Subito ``features`` array.

    Each feature has a ``uri`` (e.g. ``"/price"``, ``"/room"``), a ``type``,
    and a ``values`` list.  We extract the first value's ``key`` and ``value``
    fields for convenient access.

    Returns a dict keyed by the URI *without* leading slash, e.g.::

        {
            "price": {"key": "1000", "value": "1000 €"},
            "room": {"key": "4", "value": "4"},
            "furnished": {"key": "1", "value": "Sì"},
            ...
        }
    """
    result: dict[str, Any] = {}
    if not features:
        return result
    for feat in features:
        uri = (feat.get("uri") or "").lstrip("/")
        if not uri:
            continue
        values = feat.get("values") or []
        if values:
            result[uri] = values[0]
        else:
            result[uri] = {}
    return result


def _feature_int(features: dict[str, Any], key: str) -> int | None:
    """Extract an integer from a feature's ``key`` field. Returns None on failure."""
    entry = features.get(key)
    if not entry:
        return None
    raw = entry.get("key")
    if raw is None:
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def _feature_bool(features: dict[str, Any], key: str) -> bool | None:
    """Extract a boolean from a feature's ``key`` field.

    Subito convention: ``"1"`` = Yes, ``"0"`` = No.
    Returns ``None`` if the feature is missing.
    """
    entry = features.get(key)
    if not entry:
        return None
    raw = entry.get("key")
    if raw == "1":
        return True
    if raw == "0":
        return False
    return None


def _extract_image_url(images: list[dict[str, Any]] | None) -> str | None:
    """Return the first image CDN URL from the ad's ``images`` array, or None."""
    if not images:
        return None
    first = images[0]
    cdn_url = first.get("cdn_base_url")
    if cdn_url:
        return cdn_url
    base_url = first.get("base_url")
    return base_url or None


def _parse_display_date(dates: dict[str, Any] | None) -> datetime | None:
    """Parse the ``display_iso8601`` timestamp from a Subito ad.

    Returns ``None`` if parsing fails or the field is absent.
    """
    if not dates:
        return None
    iso_str = dates.get("display_iso8601")
    if not iso_str:
        return None
    try:
        return datetime.fromisoformat(iso_str)
    except (ValueError, TypeError):
        return None


def _extract_ad_id(urn: str) -> str:
    """Extract the numeric ad ID from a Subito URN.

    URN format: ``"id:ad:<ad_id>:list:<list_id>"``.
    Falls back to using the full URN if the format is unexpected.
    """
    parts = urn.split(":")
    # Expected: ["id", "ad", "<ad_id>", "list", "<list_id>"]
    if len(parts) >= 3 and parts[0] == "id" and parts[1] == "ad":
        return parts[2]
    return urn


# ---------------------------------------------------------------------------
# Provider class
# ---------------------------------------------------------------------------


class SubitoProvider(BaseProvider):
    """Listing provider backed by the Subito.it Hades search API.

    Instantiate with a :class:`~rentbot.core.settings.Settings` instance.
    Uses :mod:`curl_cffi` with Chrome TLS impersonation to bypass Akamai
    bot detection on ``hades.subito.it``.

    If :attr:`~rentbot.core.settings.Settings.subito_region` is empty the
    provider logs a warning and returns an empty list — this allows the
    application to start without the Subito region configured.

    Args:
        settings: Application settings.
        session: Optional pre-built :class:`curl_cffi.requests.AsyncSession`
            (useful for testing).
    """

    source = ListingSource.SUBITO

    def __init__(
        self,
        settings: Settings,
        session: AsyncSession | None = None,
    ) -> None:
        self._settings = settings
        self._session = session
        self._owns_session = session is None

    # ------------------------------------------------------------------
    # BaseProvider interface
    # ------------------------------------------------------------------

    async def _ensure_session(self) -> AsyncSession:
        """Return the open session, creating it lazily if needed."""
        if self._session is None:
            self._session = AsyncSession(
                impersonate="chrome",
                headers={
                    "Accept": "application/json, text/plain, */*",
                    "Accept-Language": "it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7",
                    "Cache-Control": "no-cache",
                    "Referer": "https://www.subito.it/",
                    "Origin": "https://www.subito.it",
                },
                timeout=30,
            )
        return self._session

    async def fetch_latest(self) -> list[Listing]:
        """Fetch the most recent rental listings from Subito.it.

        Paginates from offset 0, stepping by ``_PAGE_SIZE``, up to
        :attr:`~rentbot.core.settings.Settings.subito_max_pages` pages or
        until the total count is exhausted.

        Returns:
            Deduplicated list of normalised :class:`~rentbot.core.models.Listing`
            objects.  Returns an empty list when the region is unconfigured or no
            results are found.
        """
        if not self._settings.subito_region:
            logger.warning("SUBITO_REGION is not configured — skipping Subito provider.")
            return []

        session = await self._ensure_session()

        listings: list[Listing] = []
        seen_ids: set[str] = set()
        max_pages = self._settings.subito_max_pages
        cutoff = datetime.now(timezone.utc) - _MAX_LISTING_AGE

        for page in range(max_pages):
            start = page * _PAGE_SIZE
            params = self._build_params(start)
            logger.debug("Subito fetch page %d/%d (start=%d)", page + 1, max_pages, start)

            url = f"{_BASE_URL}{_SEARCH_PATH}"
            response = await session.get(url, params=params)

            if response.status_code != 200:
                raise ProviderFetchError(
                    provider="subito",
                    message=(
                        f"HTTP {response.status_code} from {url}"
                    ),
                )

            payload: dict[str, Any] = response.json()

            ads: list[dict[str, Any]] = payload.get("ads", [])
            total: int = int(payload.get("count_all", 0))

            for raw_ad in ads:
                listing = self._map_ad(raw_ad)
                if listing is None:
                    continue
                if listing.id in seen_ids:
                    continue
                # Drop stale listings older than _MAX_LISTING_AGE.
                if listing.listing_date is not None:
                    ld = listing.listing_date
                    # Make offset-naive dates comparable by assuming UTC.
                    if ld.tzinfo is None:
                        ld = ld.replace(tzinfo=timezone.utc)
                    if ld < cutoff:
                        logger.debug(
                            "Subito: skipping stale listing %s (date=%s).",
                            listing.id,
                            listing.listing_date,
                        )
                        continue
                seen_ids.add(listing.id)
                listings.append(listing)

            # Stop if we've fetched all available results.
            if start + _PAGE_SIZE >= total:
                logger.debug(
                    "Subito: reached last page (start=%d, total=%d), stopping.",
                    start,
                    total,
                )
                break

        logger.info(
            "Subito: fetched %d listing(s) across up to %d page(s).",
            len(listings),
            max_pages,
        )
        return listings

    async def close(self) -> None:
        """Close the HTTP session if it was created by this provider."""
        if self._owns_session and self._session is not None:
            await self._session.close()
            logger.debug("SubitoProvider curl_cffi session closed.")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_params(self, start: int) -> dict[str, Any]:
        """Build the query-parameter dict for a single page request.

        Args:
            start: 0-based offset for pagination.

        Returns:
            Dict suitable for passing to :meth:`ProviderHttpClient.get`.
        """
        return {
            "c": _CATEGORY_APARTMENTS,
            "r": self._settings.subito_region,
            "ci": self._settings.subito_city,
            "t": _AD_TYPE_RENTAL,
            "qso": "false",
            "shp": "false",
            "urg": "false",
            "sort": "datedesc",
            "lim": _PAGE_SIZE,
            "start": start,
        }

    def _map_ad(self, ad: dict[str, Any]) -> Listing | None:
        """Map a single Subito ad JSON object to a :class:`Listing`.

        Returns ``None`` (and logs a debug message) when:

        * The ad has no ``urn`` field.
        * The ad type is not rental (``"u"``).
        * Pydantic validation fails for the assembled fields.

        Args:
            ad: One element from ``response["ads"]``.

        Returns:
            A populated :class:`~rentbot.core.models.Listing`, or ``None``.
        """
        urn = ad.get("urn", "")
        if not urn:
            logger.debug("Skipping ad with no 'urn': %r", ad)
            return None

        # Verify this is a rental listing.
        ad_type = (ad.get("type") or {}).get("key", "")
        if ad_type != _AD_TYPE_RENTAL:
            logger.debug("Skipping non-rental ad %s (type=%s)", urn, ad_type)
            return None

        provider_id = _extract_ad_id(urn)
        if not provider_id:
            logger.debug("Skipping ad with unparseable URN: %r", urn)
            return None

        # ------------------------------------------------------------------
        # Features → structured fields
        # ------------------------------------------------------------------
        features = _extract_features(ad.get("features") or [])

        price = _feature_int(features, "price") or 0
        rooms = _feature_int(features, "room")
        area_sqm = _feature_int(features, "size")
        furnished = _feature_bool(features, "furnished")

        # ------------------------------------------------------------------
        # Geo → address / zone
        # ------------------------------------------------------------------
        geo = ad.get("geo") or {}
        town = geo.get("town") or {}
        city = geo.get("city") or {}
        address_parts: list[str] = []
        map_data = geo.get("map") or {}
        if map_data.get("address"):
            address_parts.append(map_data["address"])
        address = ", ".join(address_parts) if address_parts else None
        zone = town.get("value") or city.get("value") or None

        # ------------------------------------------------------------------
        # Image URL
        # ------------------------------------------------------------------
        image_url = _extract_image_url(ad.get("images"))

        # ------------------------------------------------------------------
        # URL
        # ------------------------------------------------------------------
        urls = ad.get("urls") or {}
        listing_url = urls.get("default") or ""

        # ------------------------------------------------------------------
        # Title / description / date
        # ------------------------------------------------------------------
        title = normalise_title(ad.get("subject"), fallback_id=provider_id)
        description = normalise_text(ad.get("body"))
        listing_date = _parse_display_date(ad.get("dates"))

        # ------------------------------------------------------------------
        # Build Listing
        # ------------------------------------------------------------------
        try:
            return Listing(
                id=provider_id,
                source=ListingSource.SUBITO,
                title=title,
                price=price,
                rooms=rooms,
                area_sqm=area_sqm,
                address=address,
                zone=zone,
                furnished=furnished,
                url=listing_url if listing_url else f"{_LISTING_BASE_URL}/annunci/{provider_id}",
                image_url=image_url,
                description=description,
                listing_date=listing_date,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Failed to build Listing for Subito ID %s: %s",
                provider_id,
                exc,
            )
            return None
