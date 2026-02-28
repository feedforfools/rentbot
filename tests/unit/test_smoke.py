"""Smoke tests — verify the test harness itself is wired up correctly.

These tests assert nothing about business logic.  Their sole purpose is to
confirm:

1. pytest discovers and runs tests in this suite.
2. pytest-asyncio's ``asyncio_mode = "auto"`` setting works (async tests
   run without any decorator).
3. Core rentbot modules import without errors.
4. ``configure_logging()`` executes without raising.
5. The exception taxonomy is importable and the hierarchy is intact.

If any of these fail it means the project foundation is broken and no
subsequent tests can be trusted.
"""

from __future__ import annotations

import asyncio
import logging

import pytest

from rentbot.core import (
    BrowserProviderError,
    ConfigError,
    FilterError,
    JsonFormatter,
    LLMFilterError,
    ListingAlreadyExistsError,
    NotificationError,
    OrchestratorError,
    ProviderAuthError,
    ProviderError,
    ProviderFetchError,
    ProviderParseError,
    ProviderRateLimitError,
    RentbotError,
    StorageError,
    TelegramError,
    TelegramRateLimitError,
    configure_logging,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Import & startup smoke
# ---------------------------------------------------------------------------


def test_core_imports_succeed() -> None:
    """All public names exported from ``rentbot.core`` are importable."""
    assert configure_logging is not None
    assert JsonFormatter is not None
    assert RentbotError is not None


def test_configure_logging_text(caplog: pytest.LogCaptureFixture) -> None:
    """``configure_logging`` runs without raising in text mode."""
    configure_logging(level="INFO", fmt="text", force=True)


def test_configure_logging_json() -> None:
    """``configure_logging`` runs without raising in JSON mode."""
    configure_logging(level="DEBUG", fmt="json", force=True)
    # Restore text mode so subsequent test output remains readable.
    configure_logging(level="DEBUG", fmt="text", force=True)


def test_configure_logging_invalid_level() -> None:
    """``configure_logging`` raises ``ValueError`` for unknown log levels."""
    with pytest.raises(ValueError, match="Unknown LOG_LEVEL"):
        configure_logging(level="VERBOSE", force=True)


def test_configure_logging_invalid_format() -> None:
    """``configure_logging`` raises ``ValueError`` for unknown log formats."""
    with pytest.raises(ValueError, match="Unknown LOG_FORMAT"):
        configure_logging(fmt="xml", force=True)


# ---------------------------------------------------------------------------
# Exception taxonomy
# ---------------------------------------------------------------------------


def test_exception_hierarchy_base() -> None:
    """All custom exceptions are subclasses of ``RentbotError``."""
    for exc_class in (
        ConfigError,
        StorageError,
        ListingAlreadyExistsError,
        ProviderError,
        ProviderFetchError,
        ProviderParseError,
        ProviderAuthError,
        ProviderRateLimitError,
        BrowserProviderError,
        FilterError,
        LLMFilterError,
        NotificationError,
        TelegramError,
        TelegramRateLimitError,
        OrchestratorError,
    ):
        assert issubclass(exc_class, RentbotError), (
            f"{exc_class.__name__} is not a subclass of RentbotError"
        )


def test_exception_hierarchy_layers() -> None:
    """Layer-specific subclass relationships are correct."""
    assert issubclass(ListingAlreadyExistsError, StorageError)
    assert issubclass(ProviderFetchError, ProviderError)
    assert issubclass(ProviderParseError, ProviderError)
    assert issubclass(ProviderAuthError, ProviderError)
    assert issubclass(ProviderRateLimitError, ProviderError)
    assert issubclass(BrowserProviderError, ProviderError)
    assert issubclass(LLMFilterError, FilterError)
    assert issubclass(TelegramError, NotificationError)
    assert issubclass(TelegramRateLimitError, TelegramError)


def test_listing_already_exists_carries_id() -> None:
    """``ListingAlreadyExistsError`` exposes the conflicting listing ID."""
    exc = ListingAlreadyExistsError("immobiliare:99999")
    assert exc.listing_id == "immobiliare:99999"
    assert "immobiliare:99999" in str(exc)


def test_provider_rate_limit_carries_retry_after() -> None:
    """``ProviderRateLimitError`` exposes the optional retry_after value."""
    exc = ProviderRateLimitError("subito", retry_after=45.0)
    assert exc.provider == "subito"
    assert exc.retry_after == 45.0

    exc_no_hint = ProviderRateLimitError("casa")
    assert exc_no_hint.retry_after is None


def test_telegram_rate_limit_carries_retry_after() -> None:
    """``TelegramRateLimitError`` exposes retry_after and HTTP 429 status."""
    exc = TelegramRateLimitError(retry_after=10.0)
    assert exc.retry_after == 10.0
    assert exc.status_code == 429


def test_provider_error_formats_message() -> None:
    """``ProviderError`` includes the provider name in its string representation."""
    exc = ProviderFetchError("immobiliare", "Connection refused")
    assert "immobiliare" in str(exc)
    assert "Connection refused" in str(exc)


# ---------------------------------------------------------------------------
# Async harness
# ---------------------------------------------------------------------------


async def test_async_test_runs() -> None:
    """Simplest possible async test — confirms pytest-asyncio is operational."""
    await asyncio.sleep(0)  # yield to the event loop once
    assert True


async def test_async_exception_is_catchable() -> None:
    """Async tests can raise and catch custom exceptions correctly."""

    async def _failing_coro() -> None:
        raise ProviderFetchError("test_provider", "simulated failure")

    with pytest.raises(ProviderFetchError, match="simulated failure"):
        await _failing_coro()
