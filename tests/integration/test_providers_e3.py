"""Live integration tests — API provider HTTP wiring (E3-T9).

These tests exercise the full :class:`~rentbot.providers.api.immobiliare.ImmobiliareProvider`
and :class:`~rentbot.providers.api.casa.CasaProvider` stacks against the **real**
live endpoints.  They validate that:

* Our request structure (headers, query parameters, URL shape) is still
  accepted by the live site.
* The response body can be parsed by our mapper into valid
  :class:`~rentbot.core.models.Listing` objects without uncaught exceptions.
* Provider IDs are stored as raw provider-local values, *not* pre-canonicalized
  (guards against the ``"immobiliare:immobiliare:…"`` double-canonicalization bug).

Default behaviour
-----------------
All tests in this module are marked ``@pytest.mark.integration`` and are
**excluded from the default test run** (``addopts = "-m 'not integration'"``
in ``pyproject.toml``).  No network calls are made during routine ``pytest``
invocations.

Run on demand
-------------
To run *only* the provider integration tests::

    pytest -m integration tests/integration/test_providers_e3.py

To run all integration tests::

    pytest -m integration

Credential guards
-----------------
* Immobiliare tests are **skipped** when ``IMMOBILIARE_VRT`` is absent.
* Casa tests are **skipped** when ``CASA_SEARCH_URL`` is absent.

These values are read from the real ``.env`` file loaded at module import time
so that developers can run these tests locally without manually exporting
environment variables.

Scope
-----
These tests are **not part of CI** — they exist to catch API-shape regressions
(e.g. a portal silently changing its response JSON schema) before they cause
silent data loss in production.  Run them after updating the request logic or
before a production deployment.
"""

from __future__ import annotations

import logging
import os

import pytest
from dotenv import load_dotenv

from rentbot.core.models import Listing, ListingSource
from rentbot.core.settings import Settings
from rentbot.providers.api.casa import CasaProvider
from rentbot.providers.api.immobiliare import ImmobiliareProvider

__all__: list[str] = []

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Load .env so developers can run integration tests locally without exporting
# credentials into the shell manually.
# ---------------------------------------------------------------------------
load_dotenv()

# ---------------------------------------------------------------------------
# Skip guards — evaluated once at collection time.
# ---------------------------------------------------------------------------

_IMMOBILIARE_CONFIGURED: bool = bool(os.environ.get("IMMOBILIARE_VRT"))
_CASA_CONFIGURED: bool = bool(os.environ.get("CASA_SEARCH_URL"))

_skip_if_no_immobiliare = pytest.mark.skipif(
    not _IMMOBILIARE_CONFIGURED,
    reason=(
        "IMMOBILIARE_VRT is not set — skipping live Immobiliare integration tests. "
        "Add the VRT polygon string to .env or export it in your shell."
    ),
)

_skip_if_no_casa = pytest.mark.skipif(
    not _CASA_CONFIGURED,
    reason=(
        "CASA_SEARCH_URL is not set — skipping live Casa integration tests. "
        "Set it in .env, e.g. CASA_SEARCH_URL=https://www.casa.it/affitto/residenziale/pordenone/"
    ),
)


# ---------------------------------------------------------------------------
# Shared fixture
# ---------------------------------------------------------------------------


@pytest.fixture()
def live_settings() -> Settings:
    """Return application :class:`~rentbot.core.settings.Settings` capped to
    one result page per provider.

    Fetching a single page is sufficient to validate the HTTP and parsing
    layers and keeps the test suite fast (typically 1-2 s per provider).

    Settings are loaded from the real ``.env`` / environment so that
    credentials like ``IMMOBILIARE_VRT`` and ``CASA_SEARCH_URL`` are present.
    """
    settings = Settings()
    # Cap pages to 1 so each live test makes exactly one HTTP request.
    return settings.model_copy(update={"immobiliare_max_pages": 1, "casa_max_pages": 1})


# ---------------------------------------------------------------------------
# ImmobiliareProvider live tests
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestImmobiliareProviderLive:
    """Live integration tests for :class:`~rentbot.providers.api.immobiliare.ImmobiliareProvider`.

    All tests in this class are skipped when ``IMMOBILIARE_VRT`` is absent.
    """

    @_skip_if_no_immobiliare
    async def test_fetch_completes_without_error(self, live_settings: Settings) -> None:
        """Provider calls the live API and returns a list without raising.

        This is the primary liveness check — it validates that:

        * The ``/api-next/search-list/listings/`` endpoint is reachable.
        * Our request parameters (``vrt``, ``idContratto``, ``idCategoria``,
          ``pag``) are still accepted without an HTTP error.
        * The ``results`` key exists in the response JSON so our page loop
          does not crash.

        No assertion is made on the *count* of returned listings — the portal
        may legitimately return zero results for a very narrow search area.
        """
        async with ImmobiliareProvider(live_settings) as provider:
            results = await provider.fetch_latest()

        assert isinstance(results, list), "fetch_latest() must return a list."
        logger.info(
            "Immobiliare live: fetch completed successfully — %d listing(s) returned.",
            len(results),
        )

    @_skip_if_no_immobiliare
    async def test_returned_listings_are_valid_listing_objects(
        self, live_settings: Settings
    ) -> None:
        """Every item returned by the provider is a structurally valid :class:`~rentbot.core.models.Listing`.

        For each returned listing the test asserts:

        * The object is an instance of :class:`~rentbot.core.models.Listing`.
        * ``source`` is :attr:`~rentbot.core.models.ListingSource.IMMOBILIARE`.
        * ``id`` is non-empty.
        * ``url`` is an absolute HTTPS URL.
        * ``price`` is non-negative.

        These checks confirm that the field mapping in ``_map_result()`` still
        handles the live JSON structure correctly.
        """
        async with ImmobiliareProvider(live_settings) as provider:
            results = await provider.fetch_latest()

        for listing in results:
            assert isinstance(listing, Listing), (
                f"Expected Listing, got {type(listing)!r}: {listing!r}"
            )
            assert listing.source == ListingSource.IMMOBILIARE, f"Wrong source: {listing.source!r}"
            assert listing.id, "Listing.id must not be empty."
            assert listing.url.startswith("https://"), (
                f"Listing URL should be an absolute HTTPS URL, got: {listing.url!r}"
            )
            assert listing.price >= 0, f"Listing price must be non-negative, got: {listing.price}"

        logger.info(
            "Immobiliare live: all %d returned listing(s) passed structural checks.",
            len(results),
        )

    @_skip_if_no_immobiliare
    async def test_provider_ids_are_raw_not_canonicalized(self, live_settings: Settings) -> None:
        """``Listing.id`` must be the raw provider-local ID, not a ``"immobiliare:…"`` key.

        The storage layer is the sole place where canonical
        ``"<source>:<provider_id>"`` keys are computed via
        :func:`~rentbot.core.ids.canonical_id_from_listing`.  If a provider
        pre-canonicalizes the ID the storage layer would produce a doubled key
        like ``"immobiliare:immobiliare:12345678"`` causing dedup to fail.

        This regression guard catches that class of bug on real API output.
        """
        async with ImmobiliareProvider(live_settings) as provider:
            results = await provider.fetch_latest()

        for listing in results:
            assert not listing.id.startswith("immobiliare:"), (
                f"Listing.id appears already canonicalized: {listing.id!r}. "
                "ImmobiliareProvider must set the raw numeric ID, not the "
                "canonical key — canonicalization happens in the storage layer."
            )

        logger.info(
            "Immobiliare live: all %d listing IDs are raw (non-canonicalized).",
            len(results),
        )


# ---------------------------------------------------------------------------
# CasaProvider live tests
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestCasaProviderLive:
    """Live integration tests for :class:`~rentbot.providers.api.casa.CasaProvider`.

    All tests in this class are skipped when ``CASA_SEARCH_URL`` is absent.
    """

    @_skip_if_no_casa
    async def test_fetch_completes_without_error(self, live_settings: Settings) -> None:
        """Provider fetches the search page and parses ``__INITIAL_STATE__`` without raising.

        This is the primary liveness check — it validates that:

        * The Casa.it search URL is reachable.
        * The HTML response still contains the ``window.__INITIAL_STATE__``
          JSON payload that our parser expects.
        * The ``search.list`` key exists in the parsed state object.

        No assertion is made on the *count* of returned listings.
        """
        async with CasaProvider(live_settings) as provider:
            results = await provider.fetch_latest()

        assert isinstance(results, list), "fetch_latest() must return a list."
        logger.info(
            "Casa live: fetch completed successfully — %d listing(s) returned.",
            len(results),
        )

    @_skip_if_no_casa
    async def test_returned_listings_are_valid_listing_objects(
        self, live_settings: Settings
    ) -> None:
        """Every item returned by the provider is a structurally valid :class:`~rentbot.core.models.Listing`.

        For each returned listing the test asserts:

        * The object is an instance of :class:`~rentbot.core.models.Listing`.
        * ``source`` is :attr:`~rentbot.core.models.ListingSource.CASA`.
        * ``id`` is non-empty.
        * ``url`` is an absolute HTTPS URL.
        * ``price`` is non-negative.

        These checks confirm that the field mapping in ``_map_result()`` still
        handles the live ``__INITIAL_STATE__`` JSON structure correctly.
        """
        async with CasaProvider(live_settings) as provider:
            results = await provider.fetch_latest()

        for listing in results:
            assert isinstance(listing, Listing), (
                f"Expected Listing, got {type(listing)!r}: {listing!r}"
            )
            assert listing.source == ListingSource.CASA, f"Wrong source: {listing.source!r}"
            assert listing.id, "Listing.id must not be empty."
            assert listing.url.startswith("https://"), (
                f"Listing URL should be an absolute HTTPS URL, got: {listing.url!r}"
            )
            assert listing.price >= 0, f"Listing price must be non-negative, got: {listing.price}"

        logger.info(
            "Casa live: all %d returned listing(s) passed structural checks.",
            len(results),
        )

    @_skip_if_no_casa
    async def test_provider_ids_are_raw_not_canonicalized(self, live_settings: Settings) -> None:
        """``Listing.id`` must be the raw provider-local ID, not a ``"casa:…"`` key.

        Mirrors the same regression guard as the Immobiliare variant — ensures
        the storage layer remains the sole canonicalization point.
        """
        async with CasaProvider(live_settings) as provider:
            results = await provider.fetch_latest()

        for listing in results:
            assert not listing.id.startswith("casa:"), (
                f"Listing.id appears already canonicalized: {listing.id!r}. "
                "CasaProvider must set the raw provider-local ID, not the "
                "canonical key — canonicalization happens in the storage layer."
            )

        logger.info(
            "Casa live: all %d listing IDs are raw (non-canonicalized).",
            len(results),
        )
