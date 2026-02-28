"""Shared async HTTP client for all API-first Rentbot providers.

Wraps :class:`httpx.AsyncClient` with:

* **User-Agent rotation** — a curated pool of modern browser UA strings; a
  random UA is injected into every outgoing request to reduce fingerprinting.
* **Automatic retries** — exponential back-off with random jitter via
  :mod:`tenacity`; configurable number of attempts.
* **Rate-limit awareness** — HTTP 429 responses pause retries for the
  duration specified in the ``Retry-After`` header (or JSON body), then
  raise :class:`~rentbot.core.exceptions.ProviderRateLimitError` if retries
  are exhausted.
* **Structured error mapping** — transient (5xx, network) errors are retried;
  persistent client errors (4xx ≠ 429) raise
  :class:`~rentbot.core.exceptions.ProviderFetchError` immediately without
  consuming retry budget.

All three API-first providers (Immobiliare, Casa, Subito) should share one
instance of :class:`ProviderHttpClient` per scraping cycle to reuse the
underlying connection pool.

Typical usage::

    from rentbot.providers.api.http_client import ProviderHttpClient

    async with ProviderHttpClient() as client:
        response = await client.get("https://api.example.com/listings")
        data = response.json()

For providers that use a fixed base URL::

    async with ProviderHttpClient(base_url="https://api.immobiliare.it") as client:
        response = await client.get("/search", params={"...": "..."})
"""

from __future__ import annotations

import logging
import random
from types import TracebackType
from typing import Any, Final

import httpx
from tenacity import (
    AsyncRetrying,
    RetryCallState,
    retry_if_exception_type,
    stop_after_attempt,
)

from rentbot.core.exceptions import ProviderFetchError, ProviderRateLimitError

__all__ = ["ProviderHttpClient"]

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

#: HTTP status codes that signal a transient server-side fault (safe to retry).
_RETRYABLE_STATUS: Final[frozenset[int]] = frozenset({500, 502, 503, 504})

#: Default connection timeout in seconds.
_DEFAULT_CONNECT_TIMEOUT: Final[float] = 10.0

#: Default timeout waiting for the first byte of the response body.
_DEFAULT_READ_TIMEOUT: Final[float] = 20.0

#: Default timeout for uploading the request body (POST payloads).
_DEFAULT_WRITE_TIMEOUT: Final[float] = 10.0

#: Default total attempts (1 initial + 2 retries).
_DEFAULT_MAX_ATTEMPTS: Final[int] = 3

#: Hard cap on exponential back-off base before adding jitter (seconds).
_MAX_BACKOFF_BASE: Final[float] = 60.0

#: Upper bound on jitter added on top of the exponential base (seconds).
_MAX_BACKOFF_JITTER: Final[float] = 10.0

# ---------------------------------------------------------------------------
# User-Agent pool
# ---------------------------------------------------------------------------

#: A curated pool of realistic, modern browser User-Agent strings.
#:
#: Strings are intentionally varied across OS, browser, and version to avoid
#: presenting a single detectable fingerprint across repeated requests.
_USER_AGENTS: Final[list[str]] = [
    # Chrome 124 on Windows 11
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    # Chrome 124 on macOS (Sequoia)
    (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    # Chrome 124 on Linux
    (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    # Firefox 125 on Windows
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) "
        "Gecko/20100101 Firefox/125.0"
    ),
    # Firefox 125 on macOS
    (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.4; rv:125.0) "
        "Gecko/20100101 Firefox/125.0"
    ),
    # Firefox 125 on Linux
    (
        "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:125.0) "
        "Gecko/20100101 Firefox/125.0"
    ),
    # Safari 17 on macOS
    (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) "
        "Version/17.4 Safari/605.1.15"
    ),
    # Edge 124 on Windows
    (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0"
    ),
    # Chrome 124 on Android (mobile)
    (
        "Mozilla/5.0 (Linux; Android 14; Pixel 8) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.6367.82 Mobile Safari/537.36"
    ),
    # Safari on iPhone (iOS 17)
    (
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) "
        "Version/17.4 Mobile/15E148 Safari/604.1"
    ),
]


def _pick_user_agent() -> str:
    """Return a randomly selected User-Agent string from :data:`_USER_AGENTS`.

    Returns:
        A browser UA string.
    """
    return random.choice(_USER_AGENTS)


# ---------------------------------------------------------------------------
# Internal sentinel exception
# ---------------------------------------------------------------------------


class _RetryableServerError(ProviderFetchError):
    """Internal: signals a 5xx status for tenacity to retry.

    Never escapes the :meth:`ProviderHttpClient._request_with_retry` boundary.
    """


# ---------------------------------------------------------------------------
# Retry wait strategy
# ---------------------------------------------------------------------------


def _provider_wait(retry_state: RetryCallState) -> float:
    """Compute the wait duration before the next retry attempt.

    * :class:`~rentbot.core.exceptions.ProviderRateLimitError` with a
      positive ``retry_after`` → honour that value exactly.
    * All other retryable errors → exponential back-off with random jitter,
      capped at :data:`_MAX_BACKOFF_BASE` seconds.

    Args:
        retry_state: Tenacity call-state for the current attempt.

    Returns:
        Seconds to sleep before the next attempt.
    """
    if retry_state.outcome is not None:
        exc = retry_state.outcome.exception()
        if (
            isinstance(exc, ProviderRateLimitError)
            and exc.retry_after is not None
            and exc.retry_after > 0
        ):
            logger.debug("Honouring provider Retry-After of %.1f s", exc.retry_after)
            return exc.retry_after

    # Exponential back-off: 1 s, 2 s, 4 s, … capped at _MAX_BACKOFF_BASE.
    attempt = max(retry_state.attempt_number, 1)
    base = min(2.0 ** (attempt - 1), _MAX_BACKOFF_BASE)
    jitter = random.uniform(0.0, min(base, _MAX_BACKOFF_JITTER))
    return base + jitter


# ---------------------------------------------------------------------------
# Public client
# ---------------------------------------------------------------------------


class ProviderHttpClient:
    """Async HTTP client shared by all API-first providers.

    Wraps a :class:`httpx.AsyncClient` with UA rotation, retry logic, and
    provider-specific error mapping.  Each public request method returns the
    :class:`httpx.Response` on HTTP 2xx and raises on all other outcomes.

    Use as an ``async with`` context manager (preferred) to guarantee the
    underlying connection pool is closed on exit::

        async with ProviderHttpClient(base_url="https://api.example.com") as c:
            resp = await c.get("/path", params={"q": "pordenone"})

    Alternatively, manage the lifecycle manually::

        client = ProviderHttpClient()
        try:
            resp = await client.post("https://example.com/search", json={...})
        finally:
            await client.close()

    Args:
        base_url: Optional base URL prepended to all relative request paths.
            Use this when a provider always talks to one host.  Defaults to
            ``""`` (no base URL; callers pass full URLs).
        headers: Additional default headers merged into every request.  These
            are applied *before* UA rotation — the UA header always wins.
        connect_timeout: TCP connection establishment timeout in seconds.
        read_timeout: Timeout for receiving the first byte of the response.
        write_timeout: Timeout for uploading the request body.
        max_attempts: Total attempts including the initial try (≥ 1).

    Raises:
        ValueError: If ``max_attempts`` is less than 1.
    """

    def __init__(
        self,
        *,
        base_url: str = "",
        headers: dict[str, str] | None = None,
        connect_timeout: float = _DEFAULT_CONNECT_TIMEOUT,
        read_timeout: float = _DEFAULT_READ_TIMEOUT,
        write_timeout: float = _DEFAULT_WRITE_TIMEOUT,
        max_attempts: int = _DEFAULT_MAX_ATTEMPTS,
    ) -> None:
        if max_attempts < 1:
            raise ValueError(f"max_attempts must be ≥ 1, got {max_attempts!r}.")

        self._base_url = base_url
        self._default_headers: dict[str, str] = headers or {}
        self._max_attempts = max_attempts
        self._timeout = httpx.Timeout(
            connect=connect_timeout,
            read=read_timeout,
            write=write_timeout,
            pool=5.0,
        )
        self._http: httpx.AsyncClient | None = None

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    async def __aenter__(self) -> "ProviderHttpClient":
        """Open the connection pool and return ``self``."""
        await self._ensure_client()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Close the connection pool on exit."""
        await self.close()

    # ------------------------------------------------------------------
    # Public request methods
    # ------------------------------------------------------------------

    async def get(
        self,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        """Perform an HTTP GET request with retries and UA rotation.

        Args:
            url: The request URL or path (relative to ``base_url`` if set).
            params: Optional query-string parameters.
            headers: Per-request headers that override session defaults.
                The ``User-Agent`` set by UA rotation always takes precedence.

        Returns:
            The :class:`httpx.Response` on HTTP 2xx.

        Raises:
            ProviderRateLimitError: On HTTP 429 after exhausting retries.
            ProviderFetchError: On any other persistent HTTP or network error.
        """
        return await self._request_with_retry(
            "GET",
            url,
            params=params,
            extra_headers=headers,
        )

    async def post(
        self,
        url: str,
        *,
        json: Any | None = None,
        data: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        """Perform an HTTP POST request with retries and UA rotation.

        Args:
            url: The request URL or path (relative to ``base_url`` if set).
            json: JSON-serialisable body.  Mutually exclusive with ``data``.
            data: Form-encoded body.  Mutually exclusive with ``json``.
            params: Optional query-string parameters.
            headers: Per-request headers that override session defaults.

        Returns:
            The :class:`httpx.Response` on HTTP 2xx.

        Raises:
            ProviderRateLimitError: On HTTP 429 after exhausting retries.
            ProviderFetchError: On any other persistent HTTP or network error.
        """
        return await self._request_with_retry(
            "POST",
            url,
            json=json,
            data=data,
            params=params,
            extra_headers=headers,
        )

    async def close(self) -> None:
        """Close the underlying HTTP client and release pooled connections.

        Safe to call multiple times or when no requests have been made yet.
        """
        if self._http is not None and not self._http.is_closed:
            await self._http.aclose()
            logger.debug("ProviderHttpClient HTTP session closed.")
        self._http = None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _ensure_client(self) -> httpx.AsyncClient:
        """Return the open HTTP client, creating it lazily if needed."""
        if self._http is None or self._http.is_closed:
            self._http = httpx.AsyncClient(
                base_url=self._base_url,
                timeout=self._timeout,
                # Follow redirects transparently (common on aggregator APIs).
                follow_redirects=True,
                # Default headers applied to every request before per-call overrides.
                headers={
                    "Accept": "application/json, text/plain, */*",
                    "Accept-Language": "it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7",
                    "Accept-Encoding": "gzip, deflate, br",
                    "Cache-Control": "no-cache",
                    "Pragma": "no-cache",
                    **self._default_headers,
                },
            )
            logger.debug(
                "ProviderHttpClient session opened (base_url=%r).", self._base_url or "(none)"
            )
        return self._http

    async def _request_with_retry(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        json: Any | None = None,
        data: dict[str, Any] | None = None,
        extra_headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        """Execute a single HTTP request with tenacity-managed retries.

        Injects a fresh random UA header on *every attempt* so that retried
        requests do not repeat the same fingerprint.

        Args:
            method: HTTP method (``"GET"`` or ``"POST"``).
            url: Request URL or path.
            params: Query string parameters.
            json: JSON request body.
            data: Form-encoded request body.
            extra_headers: Per-request headers (merged on top of session defaults).

        Returns:
            :class:`httpx.Response` on HTTP 2xx.

        Raises:
            ProviderRateLimitError: HTTP 429 after exhausting retries.
            ProviderFetchError: All other HTTP errors after exhausting retries.
        """
        retry_types = (
            _RetryableServerError,
            ProviderRateLimitError,
            httpx.TransportError,
        )

        def _before_sleep(rs: RetryCallState) -> None:
            exc = rs.outcome.exception() if rs.outcome else None
            wait = _provider_wait(rs)
            logger.warning(
                "HTTP %s %s — attempt %d/%d failed (%s). Retrying in %.1f s…",
                method,
                url,
                rs.attempt_number,
                self._max_attempts,
                type(exc).__name__ if exc else "?",
                wait,
            )

        source_label = self._base_url or url  # for log context
        response: httpx.Response | None = None

        async for attempt in AsyncRetrying(
            wait=_provider_wait,
            stop=stop_after_attempt(self._max_attempts),
            retry=retry_if_exception_type(retry_types),
            reraise=True,
            before_sleep=_before_sleep,
        ):
            with attempt:
                response = await self._single_request(
                    method=method,
                    url=url,
                    params=params,
                    json=json,
                    data=data,
                    extra_headers=extra_headers,
                    source_label=source_label,
                )

        # If tenacity reraises the last exception, we never reach here.
        # The assignment above is guaranteed to have been set on success.
        assert response is not None, "tenacity exited without a response or exception"
        return response

    async def _single_request(
        self,
        *,
        method: str,
        url: str,
        params: dict[str, Any] | None,
        json: Any | None,
        data: dict[str, Any] | None,
        extra_headers: dict[str, str] | None,
        source_label: str,
    ) -> httpx.Response:
        """Perform exactly one HTTP request.

        A fresh User-Agent is chosen on every call so retried requests
        present a different UA to the target server.

        Args:
            method: HTTP verb.
            url: Full URL or path relative to ``base_url``.
            params: Query parameters.
            json: JSON body.
            data: Form-encoded body.
            extra_headers: Per-request extra headers.
            source_label: Short label for log messages.

        Returns:
            :class:`httpx.Response` on HTTP 2xx.

        Raises:
            ProviderRateLimitError: On HTTP 429.
            _RetryableServerError: On HTTP 5xx (internal sentinel).
            ProviderFetchError: On non-retryable HTTP errors.
            httpx.TransportError: Network-level failures (propagated for retry).
        """
        client = await self._ensure_client()

        # Rotate UA on every attempt.
        ua = _pick_user_agent()
        request_headers: dict[str, str] = {"User-Agent": ua}
        if extra_headers:
            request_headers.update(extra_headers)

        logger.debug(
            "HTTP %s %s (ua_hint=%s…)",
            method,
            url,
            ua[:40],
        )

        try:
            response = await client.request(
                method=method,
                url=url,
                params=params,
                json=json,
                data=data,
                headers=request_headers,
            )
        except httpx.TransportError:
            logger.debug("Transport error on %s %s.", method, url, exc_info=True)
            raise  # propagate for tenacity retry

        logger.debug(
            "HTTP %s %s → %d (%.0f ms, %d bytes)",
            method,
            url,
            response.status_code,
            response.elapsed.total_seconds() * 1000 if response.elapsed else 0,
            len(response.content),
        )

        # ------------------------------------------------------------------
        # 2xx — success
        # ------------------------------------------------------------------
        if response.is_success:
            return response

        # ------------------------------------------------------------------
        # 429 — rate limited; honour Retry-After
        # ------------------------------------------------------------------
        if response.status_code == 429:
            retry_after = _parse_retry_after(response, source_label)
            logger.warning(
                "Provider rate limit (%s) HTTP 429 — retry_after=%.1f s",
                source_label,
                retry_after,
            )
            raise ProviderRateLimitError(
                provider=source_label,
                retry_after=retry_after,
            )

        # ------------------------------------------------------------------
        # 5xx — transient; signal tenacity to retry
        # ------------------------------------------------------------------
        if response.status_code in _RETRYABLE_STATUS:
            raise _RetryableServerError(
                source_label,
                f"Transient HTTP {response.status_code} from {source_label}",
            )

        # ------------------------------------------------------------------
        # All other errors (4xx, etc.) — non-retryable
        # ------------------------------------------------------------------
        raise ProviderFetchError(
            source_label,
            f"HTTP {response.status_code} from {source_label}: {response.text[:200]}",
        )


# ---------------------------------------------------------------------------
# Response parsing helpers
# ---------------------------------------------------------------------------


def _parse_retry_after(response: httpx.Response, source_label: str) -> float:
    """Extract back-off duration from an HTTP 429 response.

    Checks the standard ``Retry-After`` header; falls back to ``1.0 s``.

    Args:
        response: The HTTP 429 response.
        source_label: Provider name for log context.

    Returns:
        Seconds to wait before retrying (always ≥ 1.0).
    """
    header = response.headers.get("retry-after", "")
    if header:
        try:
            return max(float(header), 1.0)
        except ValueError:
            logger.debug(
                "Could not parse Retry-After header %r for %s.", header, source_label
            )

    # Some APIs embed retry info in a JSON body.
    try:
        body = response.json()
        ra = body.get("retryAfter") or body.get("retry_after")
        if ra is not None:
            return max(float(ra), 1.0)
    except Exception:  # noqa: BLE001
        pass

    return 1.0
