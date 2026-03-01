"""Unit tests for Epic 3 deliverables.

Covers:
- :func:`~rentbot.providers.api.immobiliare._detect_furnished` edge cases.
- :meth:`~rentbot.providers.api.immobiliare.ImmobiliareProvider._map_result`
  for all skip conditions and all field mappings.
- :meth:`~rentbot.providers.api.immobiliare.ImmobiliareProvider.fetch_latest`
  pagination, dedup, and unconfigured-VRT short-circuit.
- :func:`~rentbot.providers.api.casa._extract_initial_state` parsing.
- :func:`~rentbot.providers.api.casa._build_page_path` path construction.
- :meth:`~rentbot.providers.api.casa.CasaProvider._map_result` for all skip
  conditions and field mappings.
- :meth:`~rentbot.providers.api.casa.CasaProvider.fetch_latest` pagination,
  parse-error isolation, and unconfigured-URL short-circuit.
- :func:`~rentbot.orchestrator.pipeline.process_listing` full pipeline wiring:
  dedup bypass, store-before-filter contract, filter pass/fail routing, notify
  and mark-notified, notification error handling.
- :func:`~rentbot.orchestrator.pipeline.run_provider` fetch-failure isolation
  and per-listing error isolation.
- :func:`~rentbot.orchestrator.pipeline.run_cycle` concurrent dispatch, stats
  aggregation, and failed-provider surfacing.

All external I/O (HTTP calls, DB, Telegram) is replaced by
:class:`~unittest.mock.AsyncMock` / :class:`~unittest.mock.MagicMock`
so the suite is deterministic and network-free.
"""

from __future__ import annotations

import json
import logging
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from rentbot.core.exceptions import ListingAlreadyExistsError
from rentbot.core.ids import canonical_id_from_listing
from rentbot.core.models import Listing, ListingSource
from rentbot.core.run_context import RunContext
from rentbot.core.settings import Settings
from rentbot.filters.heuristic import FilterResult, HeuristicFilter
from rentbot.notifiers.notifier import Notifier
from rentbot.orchestrator.pipeline import (
    CycleStats,
    ProviderCycleStats,
    process_listing,
    run_cycle,
    run_provider,
)
from rentbot.providers.api.casa import (
    CasaProvider,
    _build_page_path,
    _extract_initial_state,
)
from rentbot.providers.api.immobiliare import (
    ImmobiliareProvider,
    _detect_furnished,
)
from rentbot.storage.repository import ListingRepository

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _make_listing(
    *,
    id: str = "99999",
    source: ListingSource = ListingSource.IMMOBILIARE,
    title: str = "Bilocale centro storico",
    price: int = 650,
    rooms: int | None = 2,
    area_sqm: int | None = 60,
    address: str | None = "Via Roma 1",
    zone: str | None = "Centro",
    furnished: bool | None = True,
    url: str = "https://www.immobiliare.it/annunci/99999/",
    image_url: str | None = None,
    description: str = "Luminoso bilocale ristrutturato.",
) -> Listing:
    """Return a valid :class:`Listing` with sensible defaults."""
    return Listing(
        id=id,
        source=source,
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
    )


def _immobiliare_result(
    *,
    provider_id: str = "12345678",
    title: str = "Bilocale in affitto Pordenone",
    price_value: int = 650,
    price_visible: bool = True,
    rooms: str = "2",
    surface: str = "60 m²",
    address: str = "Via Roma 1",
    macrozone: str = "Centro",
    furniture_label: str | None = "Arredato",
    image_large: str | None = "https://img.immobiliare.it/img.jpg",
    caption: str = "Luminoso bilocale.",
    seo_url: str | None = None,
) -> dict[str, Any]:
    """Build a synthetic Immobiliare.it API ``results[]`` entry."""
    feature_list: list[dict[str, Any]] = []
    if furniture_label is not None:
        feature_list.append({"type": "furniture", "label": furniture_label})

    return {
        "realEstate": {
            "id": provider_id,
            "title": title,
            "price": {"value": price_value, "visible": price_visible},
            "properties": [
                {
                    "rooms": rooms,
                    "surface": surface,
                    "location": {"address": address, "macrozone": macrozone},
                    "featureList": feature_list,
                    "photo": {"urls": {"large": image_large}},
                    "caption": caption,
                }
            ],
        },
        "seo": {"url": seo_url or f"/annunci/{provider_id}/"},
    }


def _immobiliare_page(
    results: list[dict[str, Any]],
    max_pages: int = 1,
) -> dict[str, Any]:
    """Wrap immobiliare result entries in a full API page response."""
    return {"maxPages": max_pages, "results": results}


def _casa_listing_dict(
    *,
    provider_id: str = "casa001",
    title: str = "Trilocale in affitto",
    price_value: int = 750,
    price_show: bool = True,
    rooms: str = "3",
    mq: str = "80",
    street: str = "Via Garibaldi 5",
    district: str = "Semicentro",
    image_uri: str = "/photos/casa001.jpg",
    description: str = "Appartamento arredato con vista panoramica.",
    uri: str | None = None,
) -> dict[str, Any]:
    """Build a synthetic Casa.it listing dict (one element of ``search.list``)."""
    return {
        "id": provider_id,
        "title": {"main": title},
        "features": {
            "price": {"value": price_value, "show": price_show},
            "rooms": rooms,
            "mq": mq,
        },
        "geoInfos": {"street": street, "district_name": district},
        "media": {"items": [{"uri": image_uri}]},
        "description": description,
        "uri": uri or f"/immobili/{provider_id}/",
    }


def _casa_state(
    listings: list[dict[str, Any]],
    total_pages: int = 1,
) -> dict[str, Any]:
    """Build a synthetic Casa.it ``window.__INITIAL_STATE__`` state dict."""
    return {
        "search": {
            "list": listings,
            "paginator": {"totalPages": total_pages},
        }
    }


def _casa_html(state: dict[str, Any]) -> str:
    """Encode a state dict into the Casa.it HTML __INITIAL_STATE__ fragment."""
    inner_json = json.dumps(state)
    # Escape backslashes first, then double-quotes, to produce the
    # JSON.parse("...") string literal that Casa.it embeds in the page.
    escaped = inner_json.replace("\\", "\\\\").replace('"', '\\"')
    return f'<script>window.__INITIAL_STATE__ = JSON.parse("{escaped}");</script>'


def _mock_http_get(*response_objects: Any) -> AsyncMock:
    """Return an async mock whose ``get`` side effects are *response_objects* in order.

    Each element can be returned directly by ``await http.get(...)``.
    """
    http = AsyncMock()
    if len(response_objects) == 1:
        http.get.return_value = response_objects[0]
    else:
        http.get.side_effect = list(response_objects)
    return http


def _mock_response(*, json_data: dict | None = None, text: str | None = None) -> MagicMock:
    """Create a lightweight mock of an httpx response."""
    response = MagicMock()
    if json_data is not None:
        response.json.return_value = json_data
    if text is not None:
        response.text = text
    return response


def _minimal_settings(**overrides: Any) -> Settings:
    """Construct a :class:`Settings` instance with all required fields set to safe defaults."""
    defaults: dict[str, Any] = {
        "immobiliare_vrt": "45.9,12.6;45.9,12.7;46.0,12.7;46.0,12.6",
        "immobiliare_max_pages": 1,
        "casa_search_url": "https://www.casa.it/affitto/residenziale/pordenone/",
        "casa_max_pages": 1,
        # Avoid loading real credentials from .env during tests
        "telegram_bot_token": "",
        "telegram_chat_id": "",
    }
    defaults.update(overrides)
    return Settings(**defaults)


# ---------------------------------------------------------------------------
# Immobiliare — _detect_furnished
# ---------------------------------------------------------------------------


class TestDetectFurnished:
    """Unit tests for the ``_detect_furnished`` helper."""

    def test_arredato_label_returns_true(self) -> None:
        result = _detect_furnished([{"type": "furniture", "label": "Arredato"}])
        assert result is True

    def test_parzialmente_arredato_returns_true(self) -> None:
        result = _detect_furnished([{"type": "furniture", "label": "Parzialmente Arredato"}])
        assert result is True

    def test_non_arredato_returns_false(self) -> None:
        result = _detect_furnished([{"type": "furniture", "label": "Non Arredato"}])
        assert result is False

    def test_solo_cucina_returns_false(self) -> None:
        result = _detect_furnished([{"type": "furniture", "label": "Solo Cucina Arredata"}])
        assert result is False

    def test_absent_furniture_type_returns_none(self) -> None:
        result = _detect_furnished([{"type": "elevator", "label": "Ascensore"}])
        assert result is None

    def test_empty_list_returns_none(self) -> None:
        assert _detect_furnished([]) is None

    def test_none_returns_none(self) -> None:
        assert _detect_furnished(None) is None

    def test_case_insensitive_comparison(self) -> None:
        result = _detect_furnished([{"type": "furniture", "label": "ARREDATO"}])
        assert result is True


# ---------------------------------------------------------------------------
# Immobiliare — ImmobiliareProvider._map_result
# ---------------------------------------------------------------------------


class TestImmobiliareMapResult:
    """Unit tests for ImmobiliareProvider._map_result."""

    def setup_method(self) -> None:
        self.settings = _minimal_settings()
        self.provider = ImmobiliareProvider(settings=self.settings, http_client=AsyncMock())

    def test_full_valid_listing_maps_all_fields(self) -> None:
        raw = _immobiliare_result()
        listing = self.provider._map_result(raw)

        assert listing is not None
        assert listing.id == "12345678"
        assert listing.source == ListingSource.IMMOBILIARE
        assert listing.price == 650
        assert listing.rooms == 2
        assert listing.area_sqm == 60
        assert listing.address == "Via Roma 1"
        assert listing.zone == "Centro"
        assert listing.furnished is True
        assert listing.image_url == "https://img.immobiliare.it/img.jpg"
        assert listing.url == "https://www.immobiliare.it/annunci/12345678/"
        assert "Luminoso" in listing.description

    def test_id_is_raw_provider_id_not_canonical(self) -> None:
        """Listing.id must be the raw provider ID, not 'immobiliare:12345678'."""
        raw = _immobiliare_result(provider_id="99001")
        listing = self.provider._map_result(raw)
        assert listing is not None
        assert listing.id == "99001"
        assert ":" not in listing.id

    def test_missing_real_estate_returns_none(self) -> None:
        assert self.provider._map_result({}) is None
        assert self.provider._map_result({"other": "data"}) is None

    def test_price_not_visible_returns_none(self) -> None:
        raw = _immobiliare_result(price_visible=False)
        assert self.provider._map_result(raw) is None

    def test_missing_provider_id_returns_none(self) -> None:
        raw = _immobiliare_result()
        raw["realEstate"]["id"] = ""
        assert self.provider._map_result(raw) is None

    def test_rooms_five_plus_parses_as_five(self) -> None:
        raw = _immobiliare_result(rooms="5+")
        listing = self.provider._map_result(raw)
        assert listing is not None
        assert listing.rooms == 5

    def test_area_parsed_from_surface_string(self) -> None:
        raw = _immobiliare_result(surface="75 m²")
        listing = self.provider._map_result(raw)
        assert listing is not None
        assert listing.area_sqm == 75

    def test_image_url_none_when_absent(self) -> None:
        raw = _immobiliare_result(image_large=None)
        listing = self.provider._map_result(raw)
        assert listing is not None
        assert listing.image_url is None

    def test_url_falls_back_to_annunci_path(self) -> None:
        raw = _immobiliare_result(provider_id="55555", seo_url=None)
        raw["seo"] = {}
        listing = self.provider._map_result(raw)
        assert listing is not None
        assert listing.url == "https://www.immobiliare.it/annunci/55555/"

    def test_furnished_none_when_feature_absent(self) -> None:
        raw = _immobiliare_result(furniture_label=None)
        listing = self.provider._map_result(raw)
        assert listing is not None
        assert listing.furnished is None

    def test_title_falls_back_to_id_when_absent(self) -> None:
        raw = _immobiliare_result(provider_id="77777", title="")
        raw["realEstate"]["title"] = None
        listing = self.provider._map_result(raw)
        assert listing is not None
        assert "77777" in listing.title


# ---------------------------------------------------------------------------
# Immobiliare — ImmobiliareProvider.fetch_latest
# ---------------------------------------------------------------------------


class TestImmobiliareProviderFetchLatest:
    """Integration-level tests for fetch_latest using a mocked HTTP client."""

    def _make_provider(self, settings: Settings, http: AsyncMock) -> ImmobiliareProvider:
        return ImmobiliareProvider(settings=settings, http_client=http)

    async def test_returns_empty_list_when_vrt_not_configured(self) -> None:
        settings = _minimal_settings(immobiliare_vrt="")
        http = AsyncMock()
        provider = self._make_provider(settings, http)

        listings = await provider.fetch_latest()

        assert listings == []
        http.get.assert_not_called()

    async def test_single_page_returns_one_listing(self) -> None:
        settings = _minimal_settings(immobiliare_max_pages=1)
        page = _immobiliare_page([_immobiliare_result()], max_pages=1)
        http = _mock_http_get(_mock_response(json_data=page))
        provider = self._make_provider(settings, http)

        listings = await provider.fetch_latest()

        assert len(listings) == 1
        assert listings[0].id == "12345678"
        assert listings[0].source == ListingSource.IMMOBILIARE

    async def test_dedup_within_fetch_removes_duplicate_ids(self) -> None:
        """Two results with the same ID on the same page must yield one listing."""
        settings = _minimal_settings(immobiliare_max_pages=1)
        result = _immobiliare_result(provider_id="dup001")
        page = _immobiliare_page([result, result], max_pages=1)
        http = _mock_http_get(_mock_response(json_data=page))
        provider = self._make_provider(settings, http)

        listings = await provider.fetch_latest()

        assert len(listings) == 1

    async def test_pagination_stops_at_api_max_pages(self) -> None:
        """If the API reports maxPages=1, only one request should be made."""
        settings = _minimal_settings(immobiliare_max_pages=3)
        page = _immobiliare_page([_immobiliare_result()], max_pages=1)
        http = _mock_http_get(_mock_response(json_data=page))
        provider = self._make_provider(settings, http)

        listings = await provider.fetch_latest()

        assert len(listings) == 1
        http.get.assert_called_once()

    async def test_pagination_stops_at_settings_max_pages(self) -> None:
        """Settings.immobiliare_max_pages caps the fetch even if API reports more pages."""
        settings = _minimal_settings(immobiliare_max_pages=2)
        # Both pages return different listings; API claims 5 pages exist.
        page1 = _immobiliare_page([_immobiliare_result(provider_id="p1")], max_pages=5)
        page2 = _immobiliare_page([_immobiliare_result(provider_id="p2")], max_pages=5)
        http = _mock_http_get(
            _mock_response(json_data=page1),
            _mock_response(json_data=page2),
        )
        provider = self._make_provider(settings, http)

        listings = await provider.fetch_latest()

        assert len(listings) == 2
        assert http.get.call_count == 2

    async def test_hidden_price_listing_omitted(self) -> None:
        settings = _minimal_settings(immobiliare_max_pages=1)
        page = _immobiliare_page(
            [_immobiliare_result(price_visible=False)], max_pages=1
        )
        http = _mock_http_get(_mock_response(json_data=page))
        provider = self._make_provider(settings, http)

        listings = await provider.fetch_latest()

        assert listings == []


# ---------------------------------------------------------------------------
# Casa — helpers
# ---------------------------------------------------------------------------


class TestCasaExtractInitialState:
    """Unit tests for the ``_extract_initial_state`` HTML parser."""

    def test_valid_html_returns_state_dict(self) -> None:
        state = _casa_state([_casa_listing_dict()])
        html = _casa_html(state)
        result = _extract_initial_state(html)
        assert "search" in result
        assert result["search"]["paginator"]["totalPages"] == 1

    def test_raises_value_error_when_marker_absent(self) -> None:
        with pytest.raises(ValueError, match="__INITIAL_STATE__"):
            _extract_initial_state("<html><body>No state here</body></html>")

    def test_unicode_characters_survive_round_trip(self) -> None:
        state = _casa_state(
            [_casa_listing_dict(description="Città di Pordenone — appartamento.")]
        )
        html = _casa_html(state)
        result = _extract_initial_state(html)
        listing = result["search"]["list"][0]
        assert "Città" in listing["description"]


class TestCasaBuildPagePath:
    """Unit tests for ``_build_page_path``."""

    def test_page_1_returns_unmodified_path(self) -> None:
        assert _build_page_path("/affitto/residenziale/pordenone/", 1) == \
               "/affitto/residenziale/pordenone/"

    def test_page_2_appends_page_segment(self) -> None:
        assert _build_page_path("/affitto/residenziale/pordenone/", 2) == \
               "/affitto/residenziale/pordenone/2/"

    def test_page_3_appends_correct_number(self) -> None:
        assert _build_page_path("/affitto/residenziale/pordenone/", 3) == \
               "/affitto/residenziale/pordenone/3/"


# ---------------------------------------------------------------------------
# Casa — CasaProvider._map_result
# ---------------------------------------------------------------------------


class TestCasaMapResult:
    """Unit tests for CasaProvider._map_result."""

    def setup_method(self) -> None:
        self.settings = _minimal_settings()
        self.provider = CasaProvider(settings=self.settings, http_client=AsyncMock())

    def test_full_valid_listing_maps_all_fields(self) -> None:
        raw = _casa_listing_dict()
        listing = self.provider._map_result(raw)

        assert listing is not None
        assert listing.id == "casa001"
        assert listing.source == ListingSource.CASA
        assert listing.price == 750
        assert listing.rooms == 3
        assert listing.area_sqm == 80
        assert listing.address == "Via Garibaldi 5"
        assert listing.zone == "Semicentro"
        assert listing.image_url == "https://images-1.casa.it/360x265/photos/casa001.jpg"
        assert listing.url == "https://www.casa.it/immobili/casa001/"

    def test_id_is_raw_provider_id_not_canonical(self) -> None:
        """Listing.id must be the raw provider ID, not 'casa:casa002'."""
        raw = _casa_listing_dict(provider_id="casa002")
        listing = self.provider._map_result(raw)
        assert listing is not None
        assert listing.id == "casa002"
        assert ":" not in listing.id

    def test_missing_id_returns_none(self) -> None:
        raw = _casa_listing_dict()
        raw["id"] = ""
        assert self.provider._map_result(raw) is None

    def test_price_not_shown_returns_none(self) -> None:
        raw = _casa_listing_dict(price_show=False)
        assert self.provider._map_result(raw) is None

    def test_image_url_none_when_media_empty(self) -> None:
        raw = _casa_listing_dict()
        raw["media"] = {"items": []}
        listing = self.provider._map_result(raw)
        assert listing is not None
        assert listing.image_url is None

    def test_furnished_detected_from_description(self) -> None:
        raw = _casa_listing_dict(description="Appartamento completamente arredato.")
        listing = self.provider._map_result(raw)
        assert listing is not None
        assert listing.furnished is True

    def test_url_falls_back_to_immobili_path(self) -> None:
        """When uri is absent, URL should be constructed from provider ID."""
        raw = _casa_listing_dict(provider_id="fallback99")
        raw["uri"] = ""
        listing = self.provider._map_result(raw)
        assert listing is not None
        assert listing.url == "https://www.casa.it/immobili/fallback99/"

    def test_rooms_parsed_as_integer(self) -> None:
        raw = _casa_listing_dict(rooms="2")
        listing = self.provider._map_result(raw)
        assert listing is not None
        assert listing.rooms == 2


# ---------------------------------------------------------------------------
# Casa — CasaProvider.fetch_latest
# ---------------------------------------------------------------------------


class TestCasaProviderFetchLatest:
    """Integration-level tests for Casa fetch_latest using a mocked HTTP client."""

    def _make_provider(self, settings: Settings, http: AsyncMock) -> CasaProvider:
        return CasaProvider(settings=settings, http_client=http)

    async def test_returns_empty_when_url_not_configured(self) -> None:
        settings = _minimal_settings(casa_search_url="")
        http = AsyncMock()
        provider = self._make_provider(settings, http)

        listings = await provider.fetch_latest()

        assert listings == []
        http.get.assert_not_called()

    async def test_single_page_returns_one_listing(self) -> None:
        settings = _minimal_settings(casa_max_pages=1)
        state = _casa_state([_casa_listing_dict()])
        http = _mock_http_get(_mock_response(text=_casa_html(state)))
        provider = self._make_provider(settings, http)

        listings = await provider.fetch_latest()

        assert len(listings) == 1
        assert listings[0].id == "casa001"
        assert listings[0].source == ListingSource.CASA

    async def test_parse_error_stops_pagination_gracefully(self) -> None:
        """Malformed HTML on a page should stop pagination without raising."""
        settings = _minimal_settings(casa_max_pages=3)
        http = _mock_http_get(_mock_response(text="<html>no state here</html>"))
        provider = self._make_provider(settings, http)

        listings = await provider.fetch_latest()

        assert listings == []
        http.get.assert_called_once()

    async def test_pagination_stops_at_total_pages(self) -> None:
        """If totalPages == 1, only one HTTP request should be made."""
        settings = _minimal_settings(casa_max_pages=5)
        state = _casa_state([_casa_listing_dict()], total_pages=1)
        http = _mock_http_get(_mock_response(text=_casa_html(state)))
        provider = self._make_provider(settings, http)

        listings = await provider.fetch_latest()

        assert len(listings) == 1
        http.get.assert_called_once()

    async def test_hidden_price_listing_omitted(self) -> None:
        settings = _minimal_settings(casa_max_pages=1)
        state = _casa_state([_casa_listing_dict(price_show=False)])
        http = _mock_http_get(_mock_response(text=_casa_html(state)))
        provider = self._make_provider(settings, http)

        listings = await provider.fetch_latest()

        assert listings == []


# ---------------------------------------------------------------------------
# Pipeline — process_listing
# ---------------------------------------------------------------------------


def _make_mock_repo(
    *,
    exists: bool = False,
    insert_raises: bool = False,
) -> AsyncMock:
    """Build a mocked :class:`ListingRepository`."""
    repo: AsyncMock = AsyncMock(spec=ListingRepository)
    repo.exists.return_value = exists
    if insert_raises:
        repo.insert.side_effect = ListingAlreadyExistsError("immobiliare:99999")
    else:
        repo.insert.return_value = "immobiliare:99999"
    return repo


def _make_mock_hf(*, passed: bool = True, reason: str = "") -> MagicMock:
    """Build a mocked :class:`HeuristicFilter`."""
    hf: MagicMock = MagicMock(spec=HeuristicFilter)
    hf.evaluate.return_value = FilterResult(
        passed=passed,
        reason=reason,
        listing_id="99999",
        source="immobiliare",
    )
    return hf


def _make_mock_notifier(*, send_returns: bool = True, raises: bool = False) -> AsyncMock:
    """Build a mocked :class:`Notifier`."""
    notifier: AsyncMock = AsyncMock(spec=Notifier)
    if raises:
        from rentbot.core.exceptions import TelegramError
        notifier.send_alert.side_effect = TelegramError("send failed")
    else:
        notifier.send_alert.return_value = send_returns
    return notifier


class TestProcessListing:
    """Unit tests for ``process_listing`` pipeline function."""

    async def test_duplicate_listing_increments_dup_counter(self) -> None:
        listing = _make_listing()
        repo = _make_mock_repo(exists=True)
        hf = _make_mock_hf()
        notifier = _make_mock_notifier()
        stats = ProviderCycleStats(source="immobiliare")

        await process_listing(listing, repo, hf, notifier, stats)

        assert stats.duplicate == 1
        assert stats.new == 0
        repo.insert.assert_not_called()

    async def test_insert_race_treated_as_duplicate(self) -> None:
        """ListingAlreadyExistsError after exists=False counts as duplicate."""
        listing = _make_listing()
        repo = _make_mock_repo(exists=False, insert_raises=True)
        hf = _make_mock_hf()
        notifier = _make_mock_notifier()
        stats = ProviderCycleStats(source="immobiliare")

        await process_listing(listing, repo, hf, notifier, stats)

        assert stats.duplicate == 1
        assert stats.new == 0

    async def test_new_listing_is_stored_before_filter(self) -> None:
        """repo.insert must be called before hf.evaluate regardless of filter outcome."""
        call_order: list[str] = []
        listing = _make_listing()
        repo = _make_mock_repo(exists=False)
        repo.insert.side_effect = lambda l, **kw: call_order.append("insert") or "cid"  # type: ignore[misc]
        hf = _make_mock_hf(passed=False)
        hf.evaluate.side_effect = lambda l: call_order.append("evaluate") or FilterResult(  # type: ignore[assignment]
            passed=False, reason="price_too_high", listing_id=l.id, source=str(l.source)
        )
        notifier = _make_mock_notifier()
        stats = ProviderCycleStats(source="immobiliare")

        await process_listing(listing, repo, hf, notifier, stats)

        assert call_order == ["insert", "evaluate"]

    async def test_passing_filter_triggers_alert_and_mark_notified(self) -> None:
        listing = _make_listing()
        repo = _make_mock_repo(exists=False)
        hf = _make_mock_hf(passed=True)
        notifier = _make_mock_notifier(send_returns=True)
        stats = ProviderCycleStats(source="immobiliare")

        await process_listing(listing, repo, hf, notifier, stats)

        assert stats.new == 1
        assert stats.passed_filter == 1
        assert stats.alerted == 1
        notifier.send_alert.assert_called_once_with(listing)
        repo.mark_notified.assert_called_once()

    async def test_failing_filter_stores_but_does_not_alert(self) -> None:
        listing = _make_listing()
        repo = _make_mock_repo(exists=False)
        hf = _make_mock_hf(passed=False, reason="price_too_high")
        notifier = _make_mock_notifier()
        stats = ProviderCycleStats(source="immobiliare")

        await process_listing(listing, repo, hf, notifier, stats)

        assert stats.new == 1
        assert stats.passed_filter == 0
        assert stats.alerted == 0
        notifier.send_alert.assert_not_called()
        repo.mark_notified.assert_not_called()

    async def test_filter_verdict_persisted_to_repo(self) -> None:
        listing = _make_listing()
        repo = _make_mock_repo(exists=False)
        hf = _make_mock_hf(passed=False, reason="price_too_high")
        notifier = _make_mock_notifier()
        stats = ProviderCycleStats(source="immobiliare")

        await process_listing(listing, repo, hf, notifier, stats)

        cid = canonical_id_from_listing(listing)
        repo.update_filter_result.assert_called_once_with(cid, "block:price_too_high")

    async def test_notification_error_increments_error_counter(self) -> None:
        listing = _make_listing()
        repo = _make_mock_repo(exists=False)
        hf = _make_mock_hf(passed=True)
        notifier = _make_mock_notifier(raises=True)
        stats = ProviderCycleStats(source="immobiliare")

        await process_listing(listing, repo, hf, notifier, stats)

        assert stats.errors == 1
        assert stats.alerted == 0
        repo.mark_notified.assert_not_called()

    async def test_seed_notifier_returns_false_does_not_increment_alerted(self) -> None:
        """When notifier returns False (seed mode), alerted stays 0."""
        listing = _make_listing()
        repo = _make_mock_repo(exists=False)
        hf = _make_mock_hf(passed=True)
        # Notifier in seed mode returns False (no actual send)
        notifier = _make_mock_notifier(send_returns=False)
        stats = ProviderCycleStats(source="immobiliare")

        await process_listing(listing, repo, hf, notifier, stats)

        assert stats.alerted == 0
        assert stats.errors == 0
        repo.mark_notified.assert_not_called()


# ---------------------------------------------------------------------------
# Pipeline — run_provider
# ---------------------------------------------------------------------------


class _MockProvider:
    """Minimal BaseProvider stub for pipeline tests."""

    source = ListingSource.IMMOBILIARE

    def __init__(self, listings: list[Listing] | Exception) -> None:
        self._listings = listings

    async def fetch_latest(self) -> list[Listing]:
        if isinstance(self._listings, Exception):
            raise self._listings
        return self._listings


class TestRunProvider:
    """Unit tests for ``run_provider``."""

    async def test_fetch_failure_sets_provider_failed(self) -> None:
        provider = _MockProvider(RuntimeError("network timeout"))
        repo = _make_mock_repo()
        hf = _make_mock_hf()
        notifier = _make_mock_notifier()
        ctx = RunContext(seed=False, dry_run=False)

        stats = await run_provider(provider, repo, hf, notifier, ctx)  # type: ignore[arg-type]

        assert stats.provider_failed is True
        assert stats.fetched == 0

    async def test_empty_provider_returns_zero_counts(self) -> None:
        provider = _MockProvider([])
        repo = _make_mock_repo()
        hf = _make_mock_hf()
        notifier = _make_mock_notifier()
        ctx = RunContext(seed=False, dry_run=False)

        stats = await run_provider(provider, repo, hf, notifier, ctx)  # type: ignore[arg-type]

        assert stats.fetched == 0
        assert stats.new == 0
        assert stats.provider_failed is False

    async def test_multiple_listings_all_processed(self) -> None:
        listings = [
            _make_listing(id="aaa", url="https://www.immobiliare.it/annunci/aaa/"),
            _make_listing(id="bbb", url="https://www.immobiliare.it/annunci/bbb/"),
        ]
        provider = _MockProvider(listings)
        # repo reports nothing seen yet
        repo: AsyncMock = AsyncMock(spec=ListingRepository)
        repo.exists.return_value = False
        repo.insert.return_value = "cid"
        hf = _make_mock_hf(passed=True)
        notifier = _make_mock_notifier(send_returns=True)
        ctx = RunContext(seed=False, dry_run=False)

        stats = await run_provider(provider, repo, hf, notifier, ctx)  # type: ignore[arg-type]

        assert stats.fetched == 2
        assert stats.new == 2
        assert stats.alerted == 2


# ---------------------------------------------------------------------------
# Pipeline — run_cycle
# ---------------------------------------------------------------------------


class TestRunCycle:
    """Unit tests for ``run_cycle``."""

    async def test_empty_providers_returns_empty_stats(self) -> None:
        repo = _make_mock_repo()
        hf = _make_mock_hf()
        notifier = _make_mock_notifier()
        ctx = RunContext()

        result = await run_cycle([], repo, hf, notifier, ctx)

        assert isinstance(result, CycleStats)
        assert result.provider_stats == []
        assert result.total_fetched == 0

    async def test_single_provider_stats_surfaced(self) -> None:
        listing = _make_listing()
        provider = _MockProvider([listing])
        repo: AsyncMock = AsyncMock(spec=ListingRepository)
        repo.exists.return_value = False
        repo.insert.return_value = "cid"
        hf = _make_mock_hf(passed=True)
        notifier = _make_mock_notifier(send_returns=True)
        ctx = RunContext()

        result = await run_cycle([provider], repo, hf, notifier, ctx)  # type: ignore[list-item]

        assert result.total_fetched == 1
        assert result.total_new == 1
        assert result.total_alerted == 1
        assert result.failed_providers == []

    async def test_failing_provider_does_not_stop_others(self) -> None:
        """A provider whose fetch raises must not prevent other providers from running."""
        good_listing = _make_listing(id="good1", url="https://www.immobiliare.it/annunci/good1/")
        bad_provider = _MockProvider(RuntimeError("connection refused"))
        good_provider = _MockProvider([good_listing])

        repo: AsyncMock = AsyncMock(spec=ListingRepository)
        repo.exists.return_value = False
        repo.insert.return_value = "cid"
        hf = _make_mock_hf(passed=True)
        notifier = _make_mock_notifier(send_returns=True)
        ctx = RunContext()

        result = await run_cycle(
            [bad_provider, good_provider],  # type: ignore[list-item]
            repo,
            hf,
            notifier,
            ctx,
        )

        assert result.total_new == 1
        assert len(result.failed_providers) == 1

    async def test_stats_aggregated_across_providers(self) -> None:
        """run_cycle totals should sum across all providers."""
        listing_a = _make_listing(
            id="pa001",
            source=ListingSource.IMMOBILIARE,
            url="https://www.immobiliare.it/annunci/pa001/",
        )
        listing_b = _make_listing(
            id="pb001",
            source=ListingSource.CASA,
            url="https://www.casa.it/immobili/pb001/",
        )

        class _CasaProvider(_MockProvider):
            source = ListingSource.CASA

        provider_a = _MockProvider([listing_a])
        provider_b = _CasaProvider([listing_b])

        repo: AsyncMock = AsyncMock(spec=ListingRepository)
        repo.exists.return_value = False
        repo.insert.return_value = "cid"
        hf = _make_mock_hf(passed=True)
        notifier = _make_mock_notifier(send_returns=True)
        ctx = RunContext()

        result = await run_cycle(
            [provider_a, provider_b],  # type: ignore[list-item]
            repo,
            hf,
            notifier,
            ctx,
        )

        assert result.total_fetched == 2
        assert result.total_new == 2
        assert result.total_alerted == 2
        assert len(result.provider_stats) == 2
