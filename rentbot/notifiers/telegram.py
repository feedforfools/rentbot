"""Telegram Bot API client for Rentbot.

Provides :class:`TelegramClient`, a lightweight async wrapper around the
Telegram Bot API's ``sendMessage`` endpoint.  It handles:

* A keep-alive :class:`httpx.AsyncClient` with an explicit timeout budget.
* Automatic retries with capped exponential back-off via :mod:`tenacity`.
* ``Retry-After`` header honoring on HTTP 429 responses.
* Structured exception mapping to
  :class:`~rentbot.core.exceptions.TelegramError` and
  :class:`~rentbot.core.exceptions.TelegramRateLimitError`.

This module owns *transport* concerns only (connection, auth, retries).
Message formatting lives in :mod:`rentbot.notifiers.formatter` and the
high-level :func:`~rentbot.notifiers.notifier.send_alert` entry point lives
in :mod:`rentbot.notifiers.notifier`.

Typical usage::

    async with TelegramClient(token="123:ABC", chat_id="-1001234") as client:
        await client.send_message("Hello from Rentbot\\!")

Or without a context manager::

    client = TelegramClient(token="123:ABC", chat_id="-1001234")
    try:
        await client.send_message("Hello\\!")
    finally:
        await client.close()
"""

from __future__ import annotations

import logging
import random
from typing import Final

import httpx
from tenacity import (
    AsyncRetrying,
    RetryCallState,
    retry_if_exception_type,
    stop_after_attempt,
)

from rentbot.core.exceptions import TelegramError, TelegramRateLimitError

__all__ = ["TelegramClient"]

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_TELEGRAM_BASE_URL: Final[str] = "https://api.telegram.org"

#: HTTP status codes that indicate a transient server error and are safe to retry.
_RETRYABLE_STATUS: Final[frozenset[int]] = frozenset({500, 502, 503, 504})

#: Default connection timeout in seconds.
_DEFAULT_CONNECT_TIMEOUT: Final[float] = 5.0

#: Default read timeout in seconds (Telegram usually responds within 2 s).
_DEFAULT_READ_TIMEOUT: Final[float] = 10.0

#: Default write timeout in seconds (request body upload).
_DEFAULT_WRITE_TIMEOUT: Final[float] = 10.0

#: Default total send attempts (1 initial + 3 retries).
_DEFAULT_MAX_ATTEMPTS: Final[int] = 4

#: Upper bound on exponential back-off jitter (seconds).
_MAX_BACKOFF_JITTER: Final[float] = 5.0

#: Hard cap on exponential back-off base (seconds).
_MAX_BACKOFF_BASE: Final[float] = 30.0


# ---------------------------------------------------------------------------
# Internal sentinel exception
# ---------------------------------------------------------------------------


class _RetryableServerError(TelegramError):
    """Internal sentinel raised on 5xx to trigger a tenacity retry.

    Never escapes the :meth:`TelegramClient._send_with_retry` boundary.
    """


# ---------------------------------------------------------------------------
# Wait strategy
# ---------------------------------------------------------------------------


def _telegram_wait(retry_state: RetryCallState) -> float:
    """Compute the wait duration before the next attempt.

    * HTTP 429 with a ``TelegramRateLimitError`` carrying ``retry_after`` →
      honour that value exactly.
    * All other retryable errors → exponential back-off with random jitter,
      capped at :data:`_MAX_BACKOFF_BASE` seconds.

    Args:
        retry_state: Tenacity call-state for the current attempt.

    Returns:
        Seconds to sleep before retrying.
    """
    if retry_state.outcome is not None:
        exc = retry_state.outcome.exception()
        if isinstance(exc, TelegramRateLimitError) and exc.retry_after > 0:
            logger.debug(
                "Honouring Telegram Retry-After of %.1f s", exc.retry_after
            )
            return exc.retry_after

    # Exponential back-off: 1, 2, 4, 8 … s + uniform jitter.
    attempt = max(retry_state.attempt_number, 1)  # 1-based
    base = min(2.0 ** (attempt - 1), _MAX_BACKOFF_BASE)
    jitter = random.uniform(0.0, min(base, _MAX_BACKOFF_JITTER))
    return base + jitter


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------


class TelegramClient:
    """Async Telegram Bot API client with timeout budget and automatic retries.

    Manages a single :class:`httpx.AsyncClient` for the object lifetime.
    Use as an ``async with`` context manager (preferred), or call
    :meth:`close` explicitly when done.

    Args:
        token: Bot token as provided by @BotFather (non-empty).
        chat_id: Destination chat / channel identifier (non-empty string,
            typically a numeric ID such as ``"-1001234567890"``).
        connect_timeout: Seconds to wait for a TCP connection to be
            established.  Defaults to :data:`_DEFAULT_CONNECT_TIMEOUT`.
        read_timeout: Seconds to wait for the response body after the request
            has been sent.  Defaults to :data:`_DEFAULT_READ_TIMEOUT`.
        write_timeout: Seconds to wait while uploading the request body
            (relevant for large messages).  Defaults to
            :data:`_DEFAULT_WRITE_TIMEOUT`.
        max_attempts: Total send attempts including the initial try.  Must
            be ≥ 1.  Defaults to :data:`_DEFAULT_MAX_ATTEMPTS` (4).

    Raises:
        ValueError: If ``token``, ``chat_id``, or ``max_attempts`` are invalid.
    """

    def __init__(
        self,
        token: str,
        chat_id: str,
        *,
        connect_timeout: float = _DEFAULT_CONNECT_TIMEOUT,
        read_timeout: float = _DEFAULT_READ_TIMEOUT,
        write_timeout: float = _DEFAULT_WRITE_TIMEOUT,
        max_attempts: int = _DEFAULT_MAX_ATTEMPTS,
    ) -> None:
        if not token:
            raise ValueError("TelegramClient requires a non-empty token.")
        if not chat_id:
            raise ValueError("TelegramClient requires a non-empty chat_id.")
        if max_attempts < 1:
            raise ValueError(f"max_attempts must be ≥ 1, got {max_attempts!r}.")

        self._token = token
        self._chat_id = chat_id
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

    async def __aenter__(self) -> TelegramClient:
        """Open the HTTP session and return self."""
        await self._ensure_http_client()
        return self

    async def __aexit__(self, *_args: object) -> None:
        """Close the HTTP session on exit."""
        await self.close()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def send_message(
        self,
        text: str,
        *,
        parse_mode: str = "MarkdownV2",
    ) -> None:
        """Send a text message to the configured chat.

        Retries automatically on:
        * :exc:`httpx.TransportError` — network-level failures.
        * HTTP 429 — rate limited; honours ``Retry-After`` / ``parameters.retry_after``.
        * HTTP 5xx — transient server errors.

        Raises immediately (without retrying) on non-retryable 4xx errors
        (e.g. HTTP 400 bad request, HTTP 401 invalid token, HTTP 403
        forbidden).

        Args:
            text: Ready-to-send message text.  The caller is responsible for
                escaping special characters for the chosen ``parse_mode``.
            parse_mode: Telegram parse mode.  Defaults to ``"MarkdownV2"``.
                Pass ``"HTML"`` for HTML formatting, or ``""`` to send plain
                text.

        Raises:
            TelegramRateLimitError: After exhausting retries on HTTP 429.
            TelegramError: For any other non-recoverable Telegram API or
                network error.
        """
        await self._send_with_retry(text=text, parse_mode=parse_mode)

    async def close(self) -> None:
        """Close the underlying HTTP client and release pooled connections.

        Safe to call multiple times or when the client was never used.
        """
        if self._http is not None and not self._http.is_closed:
            await self._http.aclose()
            logger.debug("TelegramClient HTTP session closed.")
        self._http = None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _ensure_http_client(self) -> httpx.AsyncClient:
        """Return the open HTTP client, creating it lazily if necessary."""
        if self._http is None or self._http.is_closed:
            self._http = httpx.AsyncClient(
                base_url=_TELEGRAM_BASE_URL,
                timeout=self._timeout,
                headers={"User-Agent": "Rentbot/0.1 (+https://github.com/rentbot)"},
            )
            logger.debug("TelegramClient HTTP session opened.")
        return self._http

    async def _send_with_retry(self, *, text: str, parse_mode: str) -> None:
        """Execute :meth:`_single_attempt` with tenacity-managed retries.

        Uses :class:`tenacity.AsyncRetrying` as a context manager so the
        retry policy can reference ``self._max_attempts`` without relying on
        class-level decorators.

        Args:
            text: Message text to send.
            parse_mode: Telegram parse mode string.
        """
        retry_types = (
            TelegramRateLimitError,
            _RetryableServerError,
            httpx.TransportError,
        )

        def _before_sleep(rs: RetryCallState) -> None:
            exc = rs.outcome.exception() if rs.outcome else None
            logger.warning(
                "Telegram send attempt %d/%d failed (%s) — retrying in %.1f s…",
                rs.attempt_number,
                self._max_attempts,
                type(exc).__name__ if exc else "?",
                _telegram_wait(rs),
            )

        async for attempt in AsyncRetrying(
            wait=_telegram_wait,
            stop=stop_after_attempt(self._max_attempts),
            retry=retry_if_exception_type(retry_types),
            reraise=True,
            before_sleep=_before_sleep,
        ):
            with attempt:
                await self._single_attempt(text=text, parse_mode=parse_mode)

    async def _single_attempt(self, *, text: str, parse_mode: str) -> None:
        """Perform exactly one HTTP POST to the Telegram ``sendMessage`` endpoint.

        Args:
            text: Message text.
            parse_mode: Telegram parse mode.

        Raises:
            TelegramRateLimitError: HTTP 429 — passes ``retry_after`` value.
            _RetryableServerError: HTTP 5xx — tenacity will re-try.
            TelegramError: Non-retryable HTTP error or malformed response.
            httpx.TransportError: Network-level error — propagated as-is so
                tenacity can re-try.
        """
        client = await self._ensure_http_client()
        endpoint = f"/bot{self._token}/sendMessage"

        payload: dict[str, object] = {
            "chat_id": self._chat_id,
            "text": text,
            "disable_web_page_preview": True,
        }
        if parse_mode:
            payload["parse_mode"] = parse_mode

        logger.debug(
            "Telegram POST %s (chat_id=%s, chars=%d, parse_mode=%r)",
            endpoint,
            self._chat_id,
            len(text),
            parse_mode,
        )

        try:
            response = await client.post(endpoint, json=payload)
        except httpx.TransportError:
            logger.debug("Transport error on Telegram POST.", exc_info=True)
            raise  # let tenacity handle the retry

        logger.debug("Telegram response: HTTP %d", response.status_code)

        # ----------------------------------------------------------
        # 200 OK — verify the JSON body says ok=true.
        # ----------------------------------------------------------
        if response.status_code == 200:
            _assert_telegram_ok(response)
            return

        # ----------------------------------------------------------
        # 429 Too Many Requests — honour Retry-After then re-raise.
        # ----------------------------------------------------------
        if response.status_code == 429:
            retry_after = _parse_retry_after(response)
            logger.warning(
                "Telegram rate limit (HTTP 429) — retry_after=%.1f s", retry_after
            )
            raise TelegramRateLimitError(retry_after=retry_after)

        # ----------------------------------------------------------
        # 5xx — transient server fault, let tenacity retry.
        # ----------------------------------------------------------
        if response.status_code in _RETRYABLE_STATUS:
            raise _RetryableServerError(
                f"Transient server error: HTTP {response.status_code}",
                status_code=response.status_code,
            )

        # ----------------------------------------------------------
        # All other errors (4xx etc.) — non-retryable, raise now.
        # ----------------------------------------------------------
        description = _extract_description(response)
        raise TelegramError(description, status_code=response.status_code)


# ---------------------------------------------------------------------------
# Response parsing helpers
# ---------------------------------------------------------------------------


def _assert_telegram_ok(response: httpx.Response) -> None:
    """Verify an HTTP-200 Telegram response has ``"ok": true``.

    Telegram occasionally returns HTTP 200 with ``"ok": false`` for
    application-layer errors (e.g. message too long).  This helper surfaces
    those as :class:`TelegramError` rather than treating them as success.

    Args:
        response: HTTP 200 response to validate.

    Raises:
        TelegramError: If the response body is not ``{"ok": true, ...}``.
    """
    try:
        body = response.json()
    except Exception as exc:  # noqa: BLE001
        raise TelegramError(
            f"Could not parse Telegram 200 response: {exc}",
            status_code=200,
        ) from exc

    if not body.get("ok"):
        description = body.get("description", "(no description)")
        raise TelegramError(
            f"Telegram ok=false: {description}",
            status_code=200,
        )


def _parse_retry_after(response: httpx.Response) -> float:
    """Extract the back-off delay from a Telegram HTTP 429 response.

    Telegram communicates the retry delay in two possible places:

    1. ``parameters.retry_after`` in the JSON body (preferred).
    2. Standard HTTP ``Retry-After`` header (fallback).

    Returns ``1.0`` as a safe conservative default when neither is present.

    Args:
        response: The HTTP 429 response object.

    Returns:
        Seconds to wait before retrying (always ≥ 1.0).
    """
    # Telegram JSON body: {"ok": false, "parameters": {"retry_after": N}}
    try:
        body = response.json()
        ra = body.get("parameters", {}).get("retry_after")
        if ra is not None:
            return max(float(ra), 1.0)
    except Exception:  # noqa: BLE001
        pass

    # Standard HTTP Retry-After header (integer seconds).
    header = response.headers.get("retry-after", "")
    if header:
        try:
            return max(float(header), 1.0)
        except ValueError:
            pass

    return 1.0


def _extract_description(response: httpx.Response) -> str:
    """Extract a human-readable error description from a non-2xx response.

    Tries the Telegram JSON ``"description"`` field first, then falls back
    to the raw response text.

    Args:
        response: Non-2xx HTTP response.

    Returns:
        Human-readable error string (never empty).
    """
    try:
        body = response.json()
        return str(body.get("description") or response.text or f"HTTP {response.status_code}")
    except Exception:  # noqa: BLE001
        return response.text or f"HTTP {response.status_code}"
