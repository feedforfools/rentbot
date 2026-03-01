"""Immobiliare.it API provider for Rentbot.

Fetches rental listings from the undocumented Immobiliare.it JSON search API
(``/api-next/search-list/listings/``), captured via browser DevTools.

The provider:
* Builds the correct query-parameter set from :class:`~rentbot.core.settings.Settings`.
* Paginates up to ``settings.immobiliare_max_pages`` pages per poll cycle.
* Maps each raw JSON result to a normalised :class:`~rentbot.core.models.Listing`.
* Skips listings where the price is not publicly visible.

Configuration
-------------
``IMMOBILIARE_VRT``
    Semicolon-separated ``lat,lng`` polygon vertices for the geo filter.
    Obtain this from the ``vrt`` query parameter when performing a search on
    the Immobiliare.it site (Network tab → Fetch/XHR).  Leave empty to
    disable this provider entirely.

``IMMOBILIARE_MAX_PAGES``
    Maximum number of result pages to request per poll.  Default: 3.

Typical usage::

    from rentbot.core.settings import Settings
    from rentbot.providers.api.immobiliare import ImmobiliareProvider

    settings = Settings()
    async with ImmobiliareProvider(settings) as provider:
        listings = await provider.fetch_latest()
"""

from __future__ import annotations

import logging
from typing import Any

from rentbot.core.models import Listing, ListingSource
from rentbot.core.settings import Settings
from rentbot.providers.api.http_client import ProviderHttpClient
from rentbot.providers.base import BaseProvider
from rentbot.providers.normalizers import (
    normalise_area_sqm,
    normalise_price,
    normalise_rooms,
    normalise_text,
    normalise_title,
    normalise_url,
)

__all__ = ["ImmobiliareProvider"]

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_BASE_URL: str = "https://www.immobiliare.it"
_SEARCH_PATH: str = "/api-next/search-list/listings/"

#: Fixed query parameters that never change between requests.
_FIXED_PARAMS: dict[str, str | int] = {
    "idContratto": 2,   # 2 = affitto (rent)
    "idCategoria": 1,   # 1 = residenziale
    "__lang": "it",
    "paramsCount": 5,
    "path": "/search-list/",
}

#: Referer sent with every request so the API accepts it as same-origin XHR.
_REFERER: str = "https://www.immobiliare.it/search-list/"

# ---------------------------------------------------------------------------
# Parsing helpers (module-level, stateless)
# ---------------------------------------------------------------------------


# _parse_rooms and _parse_surface have been consolidated into the shared
# rentbot.providers.normalizers module (normalise_rooms / normalise_area_sqm).


def _detect_furnished(feature_list: list[dict[str, Any]] | None) -> bool | None:
    """Determine furnished status from an Immobiliare.it feature list.

    The API returns a list of feature dicts, e.g.::

        [{"type": "furniture", "label": "Arredato"}, ...]

    Mapping:

    +----------------------------------+------------------+
    | label                            | furnished result |
    +==================================+==================+
    | "Arredato"                       | ``True``         |
    +----------------------------------+------------------+
    | "Parzialmente Arredato"          | ``True``         |
    +----------------------------------+------------------+
    | "Solo Cucina Arredata"           | ``False``        |
    +----------------------------------+------------------+
    | "Non Arredato"                   | ``False``        |
    +----------------------------------+------------------+
    | absent / no furniture entry      | ``None``         |
    +----------------------------------+------------------+

    Args:
        feature_list: List of feature dicts from ``properties[0].featureList``.

    Returns:
        ``True`` / ``False`` / ``None``.
    """
    if not feature_list:
        return None
    for feature in feature_list:
        if feature.get("type") != "furniture":
            continue
        label: str = (feature.get("label") or "").strip().lower()
        if label in {"arredato", "parzialmente arredato"}:
            return True
        if label in {"non arredato", "solo cucina arredata"}:
            return False
    # furniture feature not present — status unknown
    return None


# ---------------------------------------------------------------------------
# Provider class
# ---------------------------------------------------------------------------


class ImmobiliareProvider(BaseProvider):
    """Listing provider backed by the Immobiliare.it search API.

    Instantiate with a :class:`~rentbot.core.settings.Settings` instance.
    The provider lazily creates its :class:`ProviderHttpClient` on first use
    and tears it down in :meth:`close`.

    If :attr:`~rentbot.core.settings.Settings.immobiliare_vrt` is empty the
    provider logs a warning and returns an empty list without hitting the
    network — this allows the application to start without the VRT configured.

    Args:
        settings: Application settings.
        http_client: Optional pre-built HTTP client (useful for testing).
    """

    source = ListingSource.IMMOBILIARE

    def __init__(
        self,
        settings: Settings,
        http_client: ProviderHttpClient | None = None,
    ) -> None:
        self._settings = settings
        self._http = http_client or ProviderHttpClient(base_url=_BASE_URL)
        self._owns_http = http_client is None

    # ------------------------------------------------------------------
    # BaseProvider interface
    # ------------------------------------------------------------------

    async def fetch_latest(self) -> list[Listing]:
        """Fetch the most recent rental listings from Immobiliare.it.

        Paginates from page 1 up to
        :attr:`~rentbot.core.settings.Settings.immobiliare_max_pages` or the
        ``maxPages`` value returned by the API, whichever is smaller.

        Returns:
            Deduplicated list of normalised :class:`~rentbot.core.models.Listing`
            objects.  Returns an empty list when the VRT is unconfigured or no
            results are found.
        """
        if not self._settings.immobiliare_vrt:
            logger.warning(
                "IMMOBILIARE_VRT is not configured — skipping Immobiliare provider."
            )
            return []

        listings: list[Listing] = []
        seen_ids: set[str] = set()
        max_pages = self._settings.immobiliare_max_pages

        for page in range(1, max_pages + 1):
            params = self._build_params(page)
            logger.debug("Immobiliare fetch page %d/%d", page, max_pages)

            response = await self._http.get(
                _SEARCH_PATH,
                params=params,
                headers={"Referer": _REFERER},
            )
            payload: dict[str, Any] = response.json()

            api_max_pages: int = int(payload.get("maxPages", 1))
            results: list[dict[str, Any]] = payload.get("results", [])

            for raw in results:
                listing = self._map_result(raw)
                if listing is None:
                    continue
                if listing.id in seen_ids:
                    continue
                seen_ids.add(listing.id)
                listings.append(listing)

            if page >= api_max_pages:
                logger.debug(
                    "Immobiliare: reached last page (%d/%d), stopping.", page, api_max_pages
                )
                break

        logger.info(
            "Immobiliare: fetched %d listing(s) across up to %d page(s).",
            len(listings),
            max_pages,
        )
        return listings

    async def close(self) -> None:
        """Close the HTTP client if it was created by this provider."""
        if self._owns_http:
            await self._http.close()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_params(self, page: int) -> dict[str, Any]:
        """Build the query-parameter dict for a single page request.

        Args:
            page: 1-based page number.

        Returns:
            Dict suitable for passing to :meth:`ProviderHttpClient.get`.
        """
        return {
            **_FIXED_PARAMS,
            "vrt": self._settings.immobiliare_vrt,
            "pag": page,
        }

    def _map_result(self, result: dict[str, Any]) -> Listing | None:
        """Map a single API result entry to a :class:`~rentbot.core.models.Listing`.

        Returns ``None`` (and logs a debug message) when:

        * The result is missing the expected ``realEstate`` key.
        * The ``price.visible`` field is ``False``.
        * Pydantic validation fails for the assembled field dict.

        Args:
            result: One element from ``response["results"]``.

        Returns:
            A populated :class:`~rentbot.core.models.Listing`, or ``None``.
        """
        real_estate: dict[str, Any] = result.get("realEstate", {})
        if not real_estate:
            logger.debug("Skipping result with no 'realEstate' key: %r", result)
            return None

        provider_id = str(real_estate.get("id", ""))
        if not provider_id:
            logger.debug("Skipping result with missing 'id'")
            return None

        # ------------------------------------------------------------------
        # Price
        # ------------------------------------------------------------------
        price_block: dict[str, Any] = real_estate.get("price") or {}
        if not price_block.get("visible", True):
            logger.debug("Skipping listing %s: price not visible", provider_id)
            return None
        price: int = normalise_price(price_block.get("value", 0))

        # ------------------------------------------------------------------
        # Properties (first element carries all room/surface/location data)
        # ------------------------------------------------------------------
        props_list: list[dict[str, Any]] = real_estate.get("properties") or []
        props: dict[str, Any] = props_list[0] if props_list else {}

        rooms = normalise_rooms(props.get("rooms"))
        area_sqm = normalise_area_sqm(props.get("surface"))

        location: dict[str, Any] = props.get("location") or {}
        address: str | None = location.get("address") or None
        zone: str | None = location.get("macrozone") or location.get("city") or None

        furnished = _detect_furnished(props.get("featureList"))

        # ------------------------------------------------------------------
        # Image URL (prefer large → medium → small)
        # ------------------------------------------------------------------
        photo: dict[str, Any] = props.get("photo") or {}
        urls: dict[str, Any] = photo.get("urls") or {}
        image_url: str | None = (
            urls.get("large") or urls.get("medium") or urls.get("small") or None
        )

        # ------------------------------------------------------------------
        # Description
        # ------------------------------------------------------------------
        description: str = normalise_text(props.get("caption"))

        # ------------------------------------------------------------------
        # URL and title
        # ------------------------------------------------------------------
        seo: dict[str, Any] = result.get("seo") or {}
        url: str = normalise_url(
            seo.get("url"),
            base_url=_BASE_URL,
            fallback_path=f"/annunci/{provider_id}/",
        )

        title: str = normalise_title(real_estate.get("title"), fallback_id=provider_id)

        # ------------------------------------------------------------------
        # Build Listing — catch validation errors to avoid dropping the run
        # Providers set listing.id to the raw provider-local ID.
        # The storage layer computes the canonical "<source>:<id>" key via
        # canonical_id_from_listing() at persistence time.
        # ------------------------------------------------------------------
        try:
            return Listing(
                id=provider_id,
                source=ListingSource.IMMOBILIARE,
                title=title,
                price=price,
                rooms=rooms,
                area_sqm=area_sqm,
                address=address,
                zone=zone,
                furnished=furnished,
                url=url,
                image_url=image_url,
                description=description,
                listing_date=None,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Failed to build Listing for Immobiliare ID %s: %s",
                provider_id,
                exc,
            )
            return None
