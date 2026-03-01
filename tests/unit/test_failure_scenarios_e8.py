"""Failure scenario tests — E8-T4.

Tests the system's resilience under adverse conditions:

1. **HTTP transport failures** — :class:`~rentbot.providers.api.http_client.ProviderHttpClient`
   with mocked :class:`httpx.AsyncClient` that raises
   :class:`httpx.TransportError` subclasses (timeouts, network errors).
   Verifies that retries are attempted and the correct exception type is
   reraised once the retry budget is exhausted.

2. **HTTP status-code errors** — 5xx codes are retried and ultimately raise
   :class:`~rentbot.core.exceptions.ProviderFetchError`; 4xx codes raise
   immediately without retrying; 429 raises
   :class:`~rentbot.core.exceptions.ProviderRateLimitError` with the correct
   ``retry_after`` value parsed from the response header or JSON body.

3. **Provider payload parsing resilience** — :meth:`fetch_latest` returns
   ``[]`` (and does not raise) when the remote API returns an empty dict,
   a dict without the expected ``results`` / ``search.list`` key, or a
   batch where every entry is invalid.  When only some entries are invalid,
   the valid ones are still returned.

4. **Provider HTTP error propagation** — when the HTTP client raises after
   retries are exhausted, :meth:`fetch_latest` propagates the exception so
   that :func:`~rentbot.orchestrator.pipeline.run_provider` can isolate the
   failure and mark the provider as failed for the cycle.

No network I/O occurs in this test suite — all HTTP calls are replaced by
``AsyncMock`` / ``MagicMock`` stubs.
"""

from __future__ import annotations

import json as _json
import logging
from collections.abc import AsyncGenerator
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from rentbot.core.exceptions import ProviderFetchError, ProviderRateLimitError
from rentbot.core.settings import Settings
from rentbot.providers.api.casa import CasaProvider
from rentbot.providers.api.http_client import ProviderHttpClient
from rentbot.providers.api.immobiliare import ImmobiliareProvider

__all__: list[str] = []

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _minimal_settings(**overrides: Any) -> Settings:
    """Construct a :class:`Settings` instance with safe test-only defaults."""
    defaults: dict[str, Any] = {
        "immobiliare_vrt": "45.9,12.6;45.9,12.7;46.0,12.7;46.0,12.6",
        "immobiliare_max_pages": 1,
        "casa_search_url": "https://www.casa.it/affitto/residenziale/pordenone/",
        "casa_max_pages": 1,
        "telegram_bot_token": "",
        "telegram_chat_id": "",
    }
    defaults.update(overrides)
    return Settings(**defaults)


def _mock_httpx_response(
    *,
    status_code: int = 200,
    json_data: dict[str, Any] | None = None,
    text: str | None = None,
    headers: dict[str, str] | None = None,
) -> MagicMock:
    """Create a minimal mock of an :class:`httpx.Response`.

    Args:
        status_code: HTTP status code.
        json_data: If supplied, ``response.json()`` returns this dict and
            ``response.text`` is its JSON serialisation.
        text: Raw response body text (used when *json_data* is ``None``).
        headers: Response headers dict (e.g. ``{"retry-after": "30"}``).

    Returns:
        A :class:`MagicMock` that duck-types as an :class:`httpx.Response`.
    """
    resp = MagicMock()
    resp.status_code = status_code
    resp.is_success = 200 <= status_code < 300
    resp.headers = headers or {}
    _text = text or (_json.dumps(json_data) if json_data is not None else "")
    resp.text = _text
    if json_data is not None:
        resp.json.return_value = json_data
    resp.content = _text.encode() if isinstance(_text, str) else b""
    elapsed = MagicMock()
    elapsed.total_seconds.return_value = 0.05
    resp.elapsed = elapsed
    return resp


def _inject_mock_underlying(client: ProviderHttpClient, mock_http: MagicMock) -> None:
    """Replace the internal :class:`httpx.AsyncClient` of *client* with *mock_http*.

    :class:`ProviderHttpClient` lazily creates an internal ``httpx.AsyncClient``
    in ``__aenter__`` / ``_ensure_client()``.  After the context is entered,
    we can swap it out for a mock.  The mock must report ``is_closed = False``
    so that ``_ensure_client`` does not try to recreate it, and must provide
    an async-compatible ``aclose`` so the lifecycle teardown succeeds.

    Args:
        client: An open :class:`ProviderHttpClient` context (already entered).
        mock_http: Mock object to install as the internal HTTP session.
    """
    mock_http.is_closed = False
    mock_http.aclose = AsyncMock()
    client._http = mock_http  # noqa: SLF001


# ---------------------------------------------------------------------------
# Section 1 — HTTP transport failures (timeouts, network errors)
# ---------------------------------------------------------------------------


class TestHttpClientTransportFailures:
    """ProviderHttpClient propagates transport errors correctly to its callers.

    Tests use ``max_attempts=1`` (no retries) to verify the exception type,
    and ``max_attempts=2/3`` with :func:`asyncio.sleep` mocked to verify
    retry count and recovery behaviour without introducing real wait times.
    """

    @pytest.fixture()
    async def http1(self) -> AsyncGenerator[ProviderHttpClient, None]:
        """Open :class:`ProviderHttpClient` with ``max_attempts=1`` (no retries)."""
        async with ProviderHttpClient(max_attempts=1) as client:
            yield client

    async def test_connect_timeout_is_reraised(self, http1: ProviderHttpClient) -> None:
        """ConnectTimeout (a TransportError) must propagate from ``get()``."""
        mock_http = MagicMock()
        mock_http.request = AsyncMock(side_effect=httpx.ConnectTimeout("timed out"))
        _inject_mock_underlying(http1, mock_http)

        with pytest.raises(httpx.TransportError):
            await http1.get("https://example.com/api")

    async def test_read_timeout_is_reraised(self, http1: ProviderHttpClient) -> None:
        """ReadTimeout must propagate from ``get()``."""
        mock_http = MagicMock()
        mock_http.request = AsyncMock(side_effect=httpx.ReadTimeout("read timed out"))
        _inject_mock_underlying(http1, mock_http)

        with pytest.raises(httpx.TransportError):
            await http1.get("https://example.com/api")

    async def test_connect_error_is_reraised(self, http1: ProviderHttpClient) -> None:
        """ConnectError (e.g. network unreachable) must propagate from ``get()``."""
        mock_http = MagicMock()
        mock_http.request = AsyncMock(side_effect=httpx.ConnectError("connection refused"))
        _inject_mock_underlying(http1, mock_http)

        with pytest.raises(httpx.TransportError):
            await http1.get("https://example.com/api")

    async def test_single_failure_then_success_returns_response(self) -> None:
        """First attempt times out; second succeeds → response returned, no exception."""
        async with ProviderHttpClient(max_attempts=2) as client:
            success_resp = _mock_httpx_response(json_data={"status": "ok"})
            mock_http = MagicMock()
            mock_http.request = AsyncMock(
                side_effect=[
                    httpx.ConnectTimeout("first attempt timed out"),
                    success_resp,
                ]
            )
            _inject_mock_underlying(client, mock_http)

            with patch("asyncio.sleep", new=AsyncMock(return_value=None)):
                response = await client.get("https://example.com/api")

        assert response.status_code == 200
        assert mock_http.request.call_count == 2, "should have been called twice (retry)"

    async def test_all_retries_exhausted_reraises_last_transport_error(self) -> None:
        """All 3 attempts time out → the last TransportError is reraised."""
        async with ProviderHttpClient(max_attempts=3) as client:
            mock_http = MagicMock()
            mock_http.request = AsyncMock(side_effect=httpx.ConnectTimeout("always times out"))
            _inject_mock_underlying(client, mock_http)

            with (
                patch("asyncio.sleep", new=AsyncMock(return_value=None)),
                pytest.raises(httpx.TransportError),
            ):
                await client.get("https://example.com/api")

        assert mock_http.request.call_count == 3, "should retry exactly max_attempts times"


# ---------------------------------------------------------------------------
# Section 2 — HTTP status-code error handling
# ---------------------------------------------------------------------------


class TestHttpClientStatusErrors:
    """ProviderHttpClient maps HTTP error codes to the correct exception types.

    All tests use ``max_attempts=1`` so that retryable errors (5xx, 429) raise
    on the first attempt — the retry loop behaviour is already covered in
    :class:`TestHttpClientTransportFailures`.
    """

    @pytest.fixture()
    async def http1(self) -> AsyncGenerator[ProviderHttpClient, None]:
        """Open :class:`ProviderHttpClient` with ``max_attempts=1`` (no retries)."""
        async with ProviderHttpClient(max_attempts=1) as client:
            yield client

    async def test_500_raises_provider_fetch_error(self, http1: ProviderHttpClient) -> None:
        """HTTP 500 must raise :class:`ProviderFetchError`."""
        mock_http = MagicMock()
        mock_http.request = AsyncMock(
            return_value=_mock_httpx_response(status_code=500, text="Internal Server Error")
        )
        _inject_mock_underlying(http1, mock_http)

        with pytest.raises(ProviderFetchError):
            await http1.get("https://example.com/api")

    async def test_503_raises_provider_fetch_error(self, http1: ProviderHttpClient) -> None:
        """HTTP 503 must raise :class:`ProviderFetchError`."""
        mock_http = MagicMock()
        mock_http.request = AsyncMock(
            return_value=_mock_httpx_response(status_code=503, text="Service Unavailable")
        )
        _inject_mock_underlying(http1, mock_http)

        with pytest.raises(ProviderFetchError):
            await http1.get("https://example.com/api")

    async def test_404_raises_provider_fetch_error_without_retry(
        self, http1: ProviderHttpClient
    ) -> None:
        """HTTP 404 must raise :class:`ProviderFetchError` immediately (non-retryable).

        Unlike 5xx, a 404 is *not* in ``_RETRYABLE_STATUS``, so tenacity must
        not schedule any further attempts.  The underlying HTTP client must be
        called exactly once.
        """
        mock_http = MagicMock()
        mock_http.request = AsyncMock(
            return_value=_mock_httpx_response(status_code=404, text="Not Found")
        )
        _inject_mock_underlying(http1, mock_http)

        with pytest.raises(ProviderFetchError):
            await http1.get("https://example.com/api")

        mock_http.request.assert_called_once()

    async def test_403_raises_provider_fetch_error_without_retry(
        self, http1: ProviderHttpClient
    ) -> None:
        """HTTP 403 (Forbidden) must also be non-retryable."""
        mock_http = MagicMock()
        mock_http.request = AsyncMock(
            return_value=_mock_httpx_response(status_code=403, text="Forbidden")
        )
        _inject_mock_underlying(http1, mock_http)

        with pytest.raises(ProviderFetchError):
            await http1.get("https://example.com/api")

        mock_http.request.assert_called_once()

    async def test_429_raises_rate_limit_error(self, http1: ProviderHttpClient) -> None:
        """:class:`ProviderRateLimitError` raised on HTTP 429."""
        mock_http = MagicMock()
        mock_http.request = AsyncMock(
            return_value=_mock_httpx_response(status_code=429, text="Too Many Requests")
        )
        _inject_mock_underlying(http1, mock_http)

        with pytest.raises(ProviderRateLimitError):
            await http1.get("https://example.com/api")

    async def test_429_retry_after_parsed_from_header(self, http1: ProviderHttpClient) -> None:
        """``retry_after`` value is parsed from the ``Retry-After`` response header."""
        mock_http = MagicMock()
        mock_http.request = AsyncMock(
            return_value=_mock_httpx_response(
                status_code=429,
                headers={"retry-after": "45"},
                text="Rate limited",
            )
        )
        _inject_mock_underlying(http1, mock_http)

        with pytest.raises(ProviderRateLimitError) as exc_info:
            await http1.get("https://example.com/api")

        assert exc_info.value.retry_after == 45.0

    async def test_429_retry_after_parsed_from_json_body(self, http1: ProviderHttpClient) -> None:
        """``retry_after`` falls back to ``retryAfter`` key in the JSON response body."""
        resp = _mock_httpx_response(
            status_code=429,
            text='{"retryAfter": 60}',
        )
        # No Retry-After header; body carries the hint.
        resp.headers = {}
        resp.json.return_value = {"retryAfter": 60}

        mock_http = MagicMock()
        mock_http.request = AsyncMock(return_value=resp)
        _inject_mock_underlying(http1, mock_http)

        with pytest.raises(ProviderRateLimitError) as exc_info:
            await http1.get("https://example.com/api")

        assert exc_info.value.retry_after == 60.0

    async def test_429_retry_after_defaults_to_one_when_absent(
        self, http1: ProviderHttpClient
    ) -> None:
        """``retry_after`` defaults to 1.0 when neither header nor JSON body has a hint."""
        resp = _mock_httpx_response(status_code=429, text="rate limit no hint")
        resp.headers = {}
        resp.json.side_effect = _json.JSONDecodeError("no json", "", 0)

        mock_http = MagicMock()
        mock_http.request = AsyncMock(return_value=resp)
        _inject_mock_underlying(http1, mock_http)

        with pytest.raises(ProviderRateLimitError) as exc_info:
            await http1.get("https://example.com/api")

        assert exc_info.value.retry_after == 1.0


# ---------------------------------------------------------------------------
# Immobiliare.it — helpers
# ---------------------------------------------------------------------------


def _immo_page(results: list[dict[str, Any]], max_pages: int = 1) -> dict[str, Any]:
    """Wrap *results* into a full Immobiliare.it API page response."""
    return {"maxPages": max_pages, "results": results}


def _immo_result_valid(provider_id: str = "12345") -> dict[str, Any]:
    """Return a minimal fully-valid Immobiliare.it result entry."""
    return {
        "realEstate": {
            "id": provider_id,
            "title": "Appartamento in affitto",
            "price": {"value": 700, "visible": True},
            "properties": [
                {
                    "rooms": "2",
                    "surface": "70 m²",
                    "location": {"address": "Via Roma 1", "macrozone": "Centro"},
                    "featureList": [],
                    "photo": None,
                    "caption": "Bello",
                }
            ],
        },
        "seo": {"url": f"/annunci/{provider_id}/"},
    }


def _immo_result_hidden_price(provider_id: str = "99001") -> dict[str, Any]:
    """Return an Immobiliare.it result entry whose price is not publicly visible."""
    return {
        "realEstate": {
            "id": provider_id,
            "title": "Appartamento (prezzo nascosto)",
            "price": {"value": 0, "visible": False},
            "properties": [{"rooms": "2", "surface": "60 m²", "location": {}, "featureList": []}],
        },
        "seo": {"url": f"/annunci/{provider_id}/"},
    }


def _immo_result_no_id() -> dict[str, Any]:
    """Return an Immobiliare.it result entry missing the ``id`` field."""
    return {
        "realEstate": {
            "id": "",
            "title": "No ID listing",
            "price": {"value": 600, "visible": True},
            "properties": [],
        },
        "seo": {},
    }


def _immo_http(payload: dict[str, Any]) -> AsyncMock:
    """Return a mock :class:`ProviderHttpClient` whose ``get`` returns *payload*."""
    http = AsyncMock()
    http.get.return_value = MagicMock(**{"json.return_value": payload})
    return http


# ---------------------------------------------------------------------------
# Section 3 — Immobiliare provider with empty / malformed payloads
# ---------------------------------------------------------------------------


class TestImmobiliareEmptyAndMalformedPayloads:
    """ImmobiliareProvider.fetch_latest() handles broken API responses safely.

    The provider must not raise when the remote API returns unexpected data
    structures — it should return an empty list and log the anomaly.
    """

    async def test_empty_dict_response_returns_empty_list(self) -> None:
        """API returns ``{}`` → ``fetch_latest()`` returns ``[]`` without raising."""
        provider = ImmobiliareProvider(settings=_minimal_settings(), http_client=_immo_http({}))
        assert await provider.fetch_latest() == []

    async def test_missing_results_key_returns_empty_list(self) -> None:
        """API returns ``{"maxPages": 1}`` (no ``results`` key) → ``[]``."""
        provider = ImmobiliareProvider(
            settings=_minimal_settings(),
            http_client=_immo_http({"maxPages": 1}),
        )
        assert await provider.fetch_latest() == []

    async def test_all_hidden_price_returns_empty_list(self) -> None:
        """Every result has ``price.visible=False`` → all skipped → ``[]``."""
        page = _immo_page([_immo_result_hidden_price("a"), _immo_result_hidden_price("b")])
        provider = ImmobiliareProvider(settings=_minimal_settings(), http_client=_immo_http(page))
        assert await provider.fetch_latest() == []

    async def test_all_missing_id_returns_empty_list(self) -> None:
        """Every result is missing an ``id`` → all skipped → ``[]``."""
        page = _immo_page([_immo_result_no_id(), _immo_result_no_id()])
        provider = ImmobiliareProvider(settings=_minimal_settings(), http_client=_immo_http(page))
        assert await provider.fetch_latest() == []

    async def test_mixed_valid_and_invalid_returns_only_valid(self) -> None:
        """One valid result + one hidden-price result → only the valid listing returned."""
        page = _immo_page([_immo_result_valid("good-1"), _immo_result_hidden_price("skip-2")])
        provider = ImmobiliareProvider(settings=_minimal_settings(), http_client=_immo_http(page))
        listings = await provider.fetch_latest()

        assert len(listings) == 1
        assert listings[0].id == "good-1"

    async def test_missing_realEstate_key_in_results(self) -> None:
        """Result entries without the ``realEstate`` key are silently skipped."""
        page = _immo_page(
            [
                {"seo": {"url": "/annunci/00000/"}},  # no realEstate key
                _immo_result_valid("keep-me"),
            ]
        )
        provider = ImmobiliareProvider(settings=_minimal_settings(), http_client=_immo_http(page))
        listings = await provider.fetch_latest()

        assert len(listings) == 1
        assert listings[0].id == "keep-me"


# ---------------------------------------------------------------------------
# Casa.it — helpers
# ---------------------------------------------------------------------------


def _casa_html(state: dict[str, Any]) -> str:
    """Encode *state* into a Casa.it ``window.__INITIAL_STATE__`` script snippet."""
    inner_json = _json.dumps(state)
    escaped = inner_json.replace("\\", "\\\\").replace('"', '\\"')
    return f'<script>window.__INITIAL_STATE__ = JSON.parse("{escaped}");</script>'


def _casa_listing_valid(provider_id: str = "casa1") -> dict[str, Any]:
    """Return a minimal fully-valid Casa.it listing dict."""
    return {
        "id": provider_id,
        "title": {"main": "Appartamento"},
        "features": {
            "price": {"value": 700, "show": True},
            "rooms": "3",
            "mq": "80",
        },
        "geoInfos": {"street": "Via Garibaldi 5", "district_name": "Semicentro"},
        "media": {"items": [{"uri": "/photos/img.jpg"}]},
        "description": "Arredato, luminoso.",
        "uri": f"/immobili/{provider_id}/",
    }


def _casa_listing_hidden_price(provider_id: str = "casa-hp") -> dict[str, Any]:
    """Return a Casa.it listing with ``price.show=False``."""
    return {
        "id": provider_id,
        "title": {"main": "Hidden price apartment"},
        "features": {"price": {"value": 0, "show": False}},
        "geoInfos": {},
        "media": {"items": []},
        "description": "Price not shown.",
        "uri": f"/immobili/{provider_id}/",
    }


def _casa_http(html: str) -> AsyncMock:
    """Return a mock :class:`ProviderHttpClient` whose ``get`` returns *html*."""
    http = AsyncMock()
    resp = MagicMock()
    resp.text = html
    http.get.return_value = resp
    return http


# ---------------------------------------------------------------------------
# Section 4 — Casa provider with empty / malformed HTML responses
# ---------------------------------------------------------------------------


class TestCasaEmptyAndMalformedPayloads:
    """CasaProvider.fetch_latest() handles broken HTML responses safely.

    If the HTML does not contain ``window.__INITIAL_STATE__`` the provider
    logs the error, breaks the page loop, and returns an empty list.
    """

    async def test_html_without_initial_state_returns_empty_list(self) -> None:
        """Plain HTML with no ``__INITIAL_STATE__`` → ``[]`` (no exception)."""
        provider = CasaProvider(
            settings=_minimal_settings(),
            http_client=_casa_http("<html><body>Just a plain page</body></html>"),
        )
        assert await provider.fetch_latest() == []

    async def test_state_with_empty_search_list_returns_empty_list(self) -> None:
        """State present but ``search.list = []`` → ``[]``."""
        state: dict[str, Any] = {"search": {"list": [], "paginator": {"totalPages": 1}}}
        provider = CasaProvider(
            settings=_minimal_settings(),
            http_client=_casa_http(_casa_html(state)),
        )
        assert await provider.fetch_latest() == []

    async def test_state_missing_search_key_returns_empty_list(self) -> None:
        """State present but ``search`` key absent → falls back to empty list."""
        state: dict[str, Any] = {"unrelated": {"data": []}}
        provider = CasaProvider(
            settings=_minimal_settings(),
            http_client=_casa_http(_casa_html(state)),
        )
        assert await provider.fetch_latest() == []

    async def test_all_hidden_price_returns_empty_list(self) -> None:
        """Every listing has ``price.show=False`` → all skipped → ``[]``."""
        state = {
            "search": {
                "list": [_casa_listing_hidden_price("hp1"), _casa_listing_hidden_price("hp2")],
                "paginator": {"totalPages": 1},
            }
        }
        provider = CasaProvider(
            settings=_minimal_settings(),
            http_client=_casa_http(_casa_html(state)),
        )
        assert await provider.fetch_latest() == []

    async def test_mixed_valid_and_no_id_listings(self) -> None:
        """Valid listing + listing with ``id=""`` → only the valid listing returned."""
        state = {
            "search": {
                "list": [
                    _casa_listing_valid("valid-1"),
                    {"id": "", "title": {"main": "no id"}, "features": {}},
                ],
                "paginator": {"totalPages": 1},
            }
        }
        provider = CasaProvider(
            settings=_minimal_settings(),
            http_client=_casa_http(_casa_html(state)),
        )
        listings = await provider.fetch_latest()

        assert len(listings) == 1
        assert listings[0].id == "valid-1"

    async def test_malformed_json_in_initial_state_returns_empty_list(self) -> None:
        """Broken JSON in ``__INITIAL_STATE__`` → parse error caught → ``[]``."""
        # Hand-craft a broken state blob that `_extract_initial_state` cannot decode.
        broken_html = '<script>window.__INITIAL_STATE__ = JSON.parse("{broken json }");</script>'
        provider = CasaProvider(
            settings=_minimal_settings(),
            http_client=_casa_http(broken_html),
        )
        assert await provider.fetch_latest() == []


# ---------------------------------------------------------------------------
# Section 5 — Provider HTTP error propagation through fetch_latest()
# ---------------------------------------------------------------------------


class TestProviderHttpErrorPropagation:
    """HTTP errors from the HTTP client propagate out of ``fetch_latest()``.

    Providers do not suppress HTTP-level exceptions — they bubble up to
    :func:`~rentbot.orchestrator.pipeline.run_provider` which marks the
    provider as failed for the cycle.
    """

    async def test_immobiliare_propagates_provider_fetch_error(self) -> None:
        """``ProviderFetchError`` from http client propagates out of ``fetch_latest()``."""
        mock_http = AsyncMock()
        mock_http.get.side_effect = ProviderFetchError("immobiliare", "HTTP 503")
        provider = ImmobiliareProvider(settings=_minimal_settings(), http_client=mock_http)

        with pytest.raises(ProviderFetchError):
            await provider.fetch_latest()

    async def test_casa_propagates_provider_fetch_error(self) -> None:
        """``ProviderFetchError`` from http client propagates out of ``fetch_latest()``."""
        mock_http = AsyncMock()
        mock_http.get.side_effect = ProviderFetchError("casa", "HTTP 500")
        provider = CasaProvider(settings=_minimal_settings(), http_client=mock_http)

        with pytest.raises(ProviderFetchError):
            await provider.fetch_latest()

    async def test_immobiliare_propagates_rate_limit_error(self) -> None:
        """``ProviderRateLimitError`` propagates out of ``fetch_latest()``."""
        mock_http = AsyncMock()
        mock_http.get.side_effect = ProviderRateLimitError("immobiliare", retry_after=30.0)
        provider = ImmobiliareProvider(settings=_minimal_settings(), http_client=mock_http)

        with pytest.raises(ProviderRateLimitError):
            await provider.fetch_latest()

    async def test_immobiliare_propagates_transport_error(self) -> None:
        """``httpx.TransportError`` (timeout/network) propagates out of ``fetch_latest()``."""
        mock_http = AsyncMock()
        mock_http.get.side_effect = httpx.ReadTimeout("read timed out")
        provider = ImmobiliareProvider(settings=_minimal_settings(), http_client=mock_http)

        with pytest.raises(httpx.TransportError):
            await provider.fetch_latest()

    async def test_casa_propagates_transport_error(self) -> None:
        """``httpx.TransportError`` propagates out of ``fetch_latest()``."""
        mock_http = AsyncMock()
        mock_http.get.side_effect = httpx.ConnectTimeout("connection timed out")
        provider = CasaProvider(settings=_minimal_settings(), http_client=mock_http)

        with pytest.raises(httpx.TransportError):
            await provider.fetch_latest()
