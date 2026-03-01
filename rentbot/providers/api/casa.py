"""Casa.it HTML provider for Rentbot.

Fetches rental listings by scraping the Casa.it search-results page HTML and
extracting the ``window.__INITIAL_STATE__`` JavaScript object that is embedded
in every server-side-rendered response.

The provider:
* Builds the correct page URL from :class:`~rentbot.core.settings.Settings`.
* Fetches the page HTML using a browser-like ``Accept`` header.
* Extracts and parses the ``JSON.parse("...")`` style ``__INITIAL_STATE__``
  blob embedded in a ``<script>`` tag.
* Navigates to ``state["search"]["list"]`` for the listings array.
* Maps each raw listing dict to a normalised :class:`~rentbot.core.models.Listing`.
* Paginates using the ``totalPages`` value from the embedded paginator.
* Skips listings where the price is not publicly visible.

Configuration
-------------
``CASA_SEARCH_URL``
    Full URL of the Casa.it search-results page for your area.  Example::

        https://www.casa.it/affitto/residenziale/pordenone/

    Leave empty to disable the Casa provider.

``CASA_MAX_PAGES``
    Maximum number of result pages to fetch per poll.  Default: 3.

Typical usage::

    from rentbot.core.settings import Settings
    from rentbot.providers.api.casa import CasaProvider

    settings = Settings()
    async with CasaProvider(settings) as provider:
        listings = await provider.fetch_latest()
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any
from urllib.parse import parse_qs, urlparse

from rentbot.core.models import Listing, ListingSource
from rentbot.core.settings import Settings
from rentbot.providers.api.http_client import ProviderHttpClient
from rentbot.providers.base import BaseProvider
from rentbot.providers.normalizers import (
    furnished_from_text,
    normalise_area_sqm,
    normalise_price,
    normalise_rooms,
    normalise_text,
    normalise_title,
    normalise_url,
)

__all__ = ["CasaProvider"]

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_BASE_URL: str = "https://www.casa.it"

#: Base URL for listing thumbnail images served by Casa.it's image CDN.
#: Individual image URIs from the API are appended directly to this prefix.
_IMAGE_BASE: str = "https://images-1.casa.it/360x265"

#: Regex to extract the ``window.__INITIAL_STATE__`` assignment from page HTML.
#: Casa.it embeds: ``window.__INITIAL_STATE__ = JSON.parse("{...}");``
#: The inner string uses ``\"`` to escape embedded quotes.
_INITIAL_STATE_RE: re.Pattern[str] = re.compile(
    r'window\.__INITIAL_STATE__\s*=\s*JSON\.parse\("((?:[^"\\]|\\.)*)"\)',
    re.DOTALL,
)

#: HTML Accept header sent with every page request to look like a real browser.
_HTML_ACCEPT: str = (
    "text/html,application/xhtml+xml,application/xml;q=0.9,"
    "image/avif,image/webp,*/*;q=0.8"
)

# ---------------------------------------------------------------------------
# Parsing helpers (module-level, stateless)
# ---------------------------------------------------------------------------


def _extract_initial_state(html: str) -> dict[str, Any]:
    """Parse ``window.__INITIAL_STATE__`` from Casa.it page HTML.

    Casa.it embeds the full SRP state as a JSON-encoded string inside a
    ``JSON.parse("…")`` call.  The inner string uses ``\"`` for quotes and
    ``\\uXXXX`` for non-ASCII characters.

    Args:
        html: Raw HTML body of a Casa.it search-results page.

    Returns:
        Decoded state dict.

    Raises:
        ValueError: If the ``__INITIAL_STATE__`` marker is not found.
        json.JSONDecodeError: If either the outer string or the embedded JSON
            cannot be parsed.
    """
    match = _INITIAL_STATE_RE.search(html)
    if not match:
        raise ValueError(
            "window.__INITIAL_STATE__ not found in Casa.it response — "
            "page structure may have changed."
        )
    # Wrap the captured group in quotes so json.loads decodes all \" and \uXXXX
    inner_str: str = json.loads('"' + match.group(1) + '"')
    return json.loads(inner_str)  # type: ignore[return-value]


# _detect_furnished has been consolidated into the shared
# rentbot.providers.normalizers module as furnished_from_text().


def _build_page_path(search_path: str, page: int) -> str:
    """Build the URL path for a specific search-result page number.

    Casa.it uses ``/path/`` for page 1 and ``/path/{page}/`` for subsequent
    pages, where the path already ends with a trailing slash.

    Args:
        search_path: URL path extracted from ``settings.casa_search_url``
            (e.g. ``"/affitto/residenziale/pordenone/"``).
        page: 1-based page number.

    Returns:
        URL path string ready to pass to :class:`ProviderHttpClient`.
    """
    if page == 1:
        return search_path
    return search_path.rstrip("/") + f"/{page}/"


# ---------------------------------------------------------------------------
# Provider class
# ---------------------------------------------------------------------------


class CasaProvider(BaseProvider):
    """Listing provider backed by the Casa.it search-results page HTML.

    Instantiate with a :class:`~rentbot.core.settings.Settings` instance.
    The provider lazily creates its :class:`ProviderHttpClient` on first use
    and tears it down in :meth:`close`.

    If :attr:`~rentbot.core.settings.Settings.casa_search_url` is empty the
    provider logs a warning and returns an empty list without hitting the
    network — this allows the application to start without Casa configured.

    Args:
        settings: Application settings.
        http_client: Optional pre-built HTTP client (useful for testing).
    """

    source = ListingSource.CASA

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
        """Fetch the most recent rental listings from Casa.it.

        Paginates from page 1 up to
        :attr:`~rentbot.core.settings.Settings.casa_max_pages` or the
        ``totalPages`` value embedded in the page state, whichever is smaller.

        Returns:
            Deduplicated list of normalised :class:`~rentbot.core.models.Listing`
            objects.  Returns an empty list when the search URL is unconfigured
            or no results are found.
        """
        if not self._settings.casa_search_url:
            logger.warning(
                "CASA_SEARCH_URL is not configured — skipping Casa provider."
            )
            return []

        parsed_url = urlparse(self._settings.casa_search_url)
        search_path = parsed_url.path
        # Flatten parse_qs lists (each key maps to a single-item list) into
        # a plain {str: str} dict so httpx can re-encode them cleanly.
        base_query_params: dict[str, str] = (
            {k: v[0] for k, v in parse_qs(parsed_url.query, keep_blank_values=True).items()}
            if parsed_url.query
            else {}
        )
        # Detect the pagination style from the configured URL:
        # * Query-string URLs (e.g. /srp/map/?geopolygon=…) page via ?page=N.
        # * Plain path URLs (e.g. /affitto/residenziale/pordenone/) page via /N/.
        uses_query_params: bool = bool(base_query_params)

        listings: list[Listing] = []
        seen_ids: set[str] = set()
        max_pages = self._settings.casa_max_pages

        for page in range(1, max_pages + 1):
            if uses_query_params:
                path = search_path
                page_params: dict[str, Any] = {**base_query_params}
                if page > 1:
                    page_params["page"] = str(page)
            else:
                path = _build_page_path(search_path, page)
                page_params = {}

            logger.debug(
                "Casa fetch page %d/%d  path=%r  query_params=%s",
                page,
                max_pages,
                path,
                "(from base URL)" if uses_query_params else "none",
            )

            response = await self._http.get(
                path,
                params=page_params or None,
                headers={"Accept": _HTML_ACCEPT},
            )

            try:
                state = _extract_initial_state(response.text)
            except (ValueError, json.JSONDecodeError) as exc:
                logger.error(
                    "Casa: failed to parse __INITIAL_STATE__ on page %d: %s",
                    page,
                    exc,
                )
                break

            search_state: dict[str, Any] = state.get("search") or {}
            raw_list: list[dict[str, Any]] = search_state.get("list") or []
            paginator: dict[str, Any] = search_state.get("paginator") or {}
            total_pages: int = int(paginator.get("totalPages", 1))

            for raw in raw_list:
                listing = self._map_result(raw)
                if listing is None:
                    continue
                if listing.id in seen_ids:
                    continue
                seen_ids.add(listing.id)
                listings.append(listing)

            if page >= total_pages:
                logger.debug(
                    "Casa: reached last page (%d/%d), stopping.", page, total_pages
                )
                break

        logger.info(
            "Casa: fetched %d listing(s) across up to %d page(s).",
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

    def _map_result(self, raw: dict[str, Any]) -> Listing | None:
        """Map a single Casa.it listing dict to a :class:`~rentbot.core.models.Listing`.

        Returns ``None`` (and logs a debug/warning message) when:

        * The listing is missing an ``id`` field.
        * The ``features.price.show`` flag is ``False``.
        * Pydantic validation fails for the assembled field dict.

        Args:
            raw: One element from ``state["search"]["list"]``.

        Returns:
            A populated :class:`~rentbot.core.models.Listing`, or ``None``.
        """
        provider_id = str(raw.get("id", ""))
        if not provider_id:
            logger.debug("Casa: skipping result with missing 'id'")
            return None

        # ------------------------------------------------------------------
        # Price
        # ------------------------------------------------------------------
        features: dict[str, Any] = raw.get("features") or {}
        price_block: dict[str, Any] = features.get("price") or {}
        if not price_block.get("show", True):
            logger.debug("Casa: skipping listing %s — price not visible", provider_id)
            return None
        try:
            price = normalise_price(price_block.get("value"))
        except (TypeError, ValueError):
            price = 0

        # ------------------------------------------------------------------
        # Rooms and area
        # ------------------------------------------------------------------
        rooms: int | None = normalise_rooms(features.get("rooms"))
        area_sqm: int | None = normalise_area_sqm(features.get("mq"))

        # ------------------------------------------------------------------
        # Location
        # ------------------------------------------------------------------
        geo: dict[str, Any] = raw.get("geoInfos") or {}
        address: str | None = geo.get("street") or None
        zone: str | None = geo.get("district_name") or None

        # ------------------------------------------------------------------
        # Image URL
        # ------------------------------------------------------------------
        media: dict[str, Any] = raw.get("media") or {}
        items: list[dict[str, Any]] = media.get("items") or []
        image_url: str | None = None
        if items and isinstance(items[0], dict):
            uri = items[0].get("uri") or ""
            if uri:
                image_url = _IMAGE_BASE + uri

        # ------------------------------------------------------------------
        # Description and furnished detection
        # ------------------------------------------------------------------
        description: str = normalise_text(raw.get("description"))
        furnished = furnished_from_text(description)

        # ------------------------------------------------------------------
        # Title
        # ------------------------------------------------------------------
        title_block = raw.get("title") or {}
        if isinstance(title_block, dict):
            raw_title: str | None = title_block.get("main")
        else:
            raw_title = str(title_block) if title_block else None
        title = normalise_title(raw_title, fallback_id=provider_id)

        # ------------------------------------------------------------------
        # URL
        # ------------------------------------------------------------------
        uri_path: str = raw.get("uri") or ""
        url = normalise_url(
            uri_path or None,
            base_url=_BASE_URL,
            fallback_path=f"/immobili/{provider_id}/",
        )

        # ------------------------------------------------------------------
        # Build Listing — catch validation errors to avoid dropping the run
        # Providers set listing.id to the raw provider-local ID.
        # The storage layer computes the canonical "<source>:<id>" key via
        # canonical_id_from_listing() at persistence time.
        # ------------------------------------------------------------------
        try:
            return Listing(
                id=provider_id,
                source=ListingSource.CASA,
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
                "Casa: failed to build Listing for ID %s: %s",
                provider_id,
                exc,
            )
            return None
