"""Rentbot exception taxonomy.

Every custom exception inherits from :class:`RentbotError`.  Exceptions are
organised by architectural layer so callers can catch at the right granularity:

    Layer hierarchy
    ---------------
    RentbotError
    ├── ConfigError
    ├── StorageError
    │   └── ListingAlreadyExistsError
    ├── ProviderError
    │   ├── ProviderFetchError
    │   ├── ProviderParseError
    │   ├── ProviderAuthError
    │   ├── ProviderRateLimitError
    │   └── BrowserProviderError
    ├── FilterError
    │   └── LLMFilterError
    ├── NotificationError
    │   └── TelegramError
    │       └── TelegramRateLimitError
    └── OrchestratorError

Usage:

    from rentbot.core.exceptions import ProviderFetchError

    raise ProviderFetchError("immobiliare", "Connection refused") from exc
"""

from __future__ import annotations

import logging

__all__ = [
    "RentbotError",
    # Config
    "ConfigError",
    # Storage
    "StorageError",
    "ListingAlreadyExistsError",
    # Provider
    "ProviderError",
    "ProviderFetchError",
    "ProviderParseError",
    "ProviderAuthError",
    "ProviderRateLimitError",
    "BrowserProviderError",
    # Filter
    "FilterError",
    "LLMFilterError",
    # Notification
    "NotificationError",
    "TelegramError",
    "TelegramRateLimitError",
    # Orchestrator
    "OrchestratorError",
]

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Base
# ---------------------------------------------------------------------------


class RentbotError(Exception):
    """Root exception for all Rentbot errors.

    Catch this to handle any application-level error uniformly.  Prefer
    catching layer-specific subclasses wherever possible for precise error
    handling.
    """


# ---------------------------------------------------------------------------
# Config layer
# ---------------------------------------------------------------------------


class ConfigError(RentbotError):
    """Raised when the application configuration is invalid or incomplete.

    Examples:
        - A required environment variable is missing.
        - A variable contains an out-of-range value (e.g. negative price).
    """


# ---------------------------------------------------------------------------
# Storage layer
# ---------------------------------------------------------------------------


class StorageError(RentbotError):
    """Raised when a database or persistence operation fails.

    Args:
        message: Human-readable error description.
    """


class ListingAlreadyExistsError(StorageError):
    """Raised when attempting to insert a listing that is already in the DB.

    This is an *expected* condition during normal operation (dedup) and should
    typically be caught and silently discarded rather than propagated.

    Args:
        listing_id: The canonical listing identifier that already exists.
    """

    def __init__(self, listing_id: str) -> None:
        self.listing_id = listing_id
        super().__init__(f"Listing already exists in DB: {listing_id!r}")


# ---------------------------------------------------------------------------
# Provider layer
# ---------------------------------------------------------------------------


class ProviderError(RentbotError):
    """Base class for all provider-level errors.

    Args:
        provider: Short name of the provider (e.g. ``"immobiliare"``).
        message: Human-readable error description.
    """

    def __init__(self, provider: str, message: str) -> None:
        self.provider = provider
        super().__init__(f"[{provider}] {message}")


class ProviderFetchError(ProviderError):
    """Raised when a provider fails to fetch data from the remote source.

    Covers network errors, unexpected HTTP status codes, and timeouts.
    """


class ProviderParseError(ProviderError):
    """Raised when a provider cannot parse or map the remote response.

    Covers schema mismatches, missing required fields, and unexpected
    response shapes.
    """


class ProviderAuthError(ProviderError):
    """Raised when a provider cannot authenticate with the remote source.

    Examples:
        - Invalid or expired session cookies for Facebook.
        - API key rejection by a third-party scraping proxy.
    """


class ProviderRateLimitError(ProviderError):
    """Raised when a provider receives an HTTP 429 or equivalent signal.

    Callers should back off and retry after a suitable delay.

    Args:
        provider: Short name of the provider.
        retry_after: Recommended back-off interval in seconds, if known.
    """

    def __init__(self, provider: str, retry_after: float | None = None) -> None:
        self.retry_after = retry_after
        detail = f"retry after {retry_after}s" if retry_after is not None else "no retry hint"
        super().__init__(provider, f"Rate limited — {detail}")


class BrowserProviderError(ProviderError):
    """Raised for failures specific to Playwright-based browser providers.

    Examples:
        - Browser launch failure.
        - Page navigation timeout.
        - Selector not found after waiting.
    """


# ---------------------------------------------------------------------------
# Filter layer
# ---------------------------------------------------------------------------


class FilterError(RentbotError):
    """Base class for errors raised inside the filtering layer.

    Args:
        message: Human-readable error description.
    """


class LLMFilterError(FilterError):
    """Raised when the LLM-based filter cannot produce a valid decision.

    Examples:
        - API call to the LLM provider fails or times out.
        - Response JSON does not conform to the expected schema.
        - Confidence score below the actionable threshold.

    These errors should *not* crash the pipeline; the calling code should
    treat the listing as unclassified and skip alerting.
    """


# ---------------------------------------------------------------------------
# Notification layer
# ---------------------------------------------------------------------------


class NotificationError(RentbotError):
    """Base class for notification delivery errors.

    Args:
        message: Human-readable error description.
    """


class TelegramError(NotificationError):
    """Raised when the Telegram Bot API returns an error or is unreachable.

    Args:
        message: Human-readable error description.
        status_code: HTTP status code from the Telegram API, if available.
    """

    def __init__(self, message: str, status_code: int | None = None) -> None:
        self.status_code = status_code
        detail = f" (HTTP {status_code})" if status_code is not None else ""
        super().__init__(f"Telegram error{detail}: {message}")


class TelegramRateLimitError(TelegramError):
    """Raised when the Telegram Bot API returns HTTP 429 (Too Many Requests).

    Args:
        retry_after: Seconds to wait before retrying, as reported by Telegram.
    """

    def __init__(self, retry_after: float) -> None:
        self.retry_after = retry_after
        super().__init__(
            f"Rate limited — retry after {retry_after}s",
            status_code=429,
        )


# ---------------------------------------------------------------------------
# Orchestrator layer
# ---------------------------------------------------------------------------


class OrchestratorError(RentbotError):
    """Raised for errors originating in the scheduling or orchestration layer.

    Examples:
        - Scheduler fails to start.
        - A provider task cannot be registered.
        - Unrecoverable error during a polling cycle.
    """
