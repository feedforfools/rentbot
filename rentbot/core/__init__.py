"""Core domain models, settings, logging configuration, and shared utilities."""

from rentbot.core.criteria import FilterCriteria
from rentbot.core.exceptions import (
    BrowserProviderError,
    ConfigError,
    FilterError,
    ListingAlreadyExistsError,
    LLMFilterError,
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
)
from rentbot.core.logging_config import JsonFormatter, configure_logging
from rentbot.core.models import Listing, ListingSource
from rentbot.core.settings import Settings

__all__ = [
    # Logging
    "configure_logging",
    "JsonFormatter",
    # Domain models
    "Listing",
    "ListingSource",
    # Settings
    "Settings",
    # Filter criteria
    "FilterCriteria",
    # Exceptions — base
    "RentbotError",
    # Exceptions — config
    "ConfigError",
    # Exceptions — storage
    "StorageError",
    "ListingAlreadyExistsError",
    # Exceptions — provider
    "ProviderError",
    "ProviderFetchError",
    "ProviderParseError",
    "ProviderAuthError",
    "ProviderRateLimitError",
    "BrowserProviderError",
    # Exceptions — filter
    "FilterError",
    "LLMFilterError",
    # Exceptions — notification
    "NotificationError",
    "TelegramError",
    "TelegramRateLimitError",
    # Exceptions — orchestrator
    "OrchestratorError",
]
