"""Rentbot application settings loaded from environment and ``.env`` files.

Uses :mod:`pydantic_settings` to parse environment variables (and optionally
an ``.env`` file) into a validated, immutable settings object.

Every environment variable documented in ``.env.example`` maps 1-to-1 to a
field in :class:`Settings`.  The field name is the **lowercase** version of
the env-var name (e.g. ``TELEGRAM_BOT_TOKEN`` → ``telegram_bot_token``).

Typical usage::

    from rentbot.core.settings import Settings

    settings = Settings()                         # loads from env + .env
    criteria = settings.to_filter_criteria()      # build FilterCriteria
    print(settings.telegram_configured)           # True / False
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Annotated

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

from rentbot.core.criteria import FilterCriteria

__all__ = ["Settings"]

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _csv_to_list(value: str) -> list[str]:
    """Split a comma-separated string into a list of non-empty, stripped items.

    Returns an empty list for blank / whitespace-only input.
    """
    if not value or not value.strip():
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


# ---------------------------------------------------------------------------
# Settings model
# ---------------------------------------------------------------------------


class Settings(BaseSettings):
    """Central application configuration.

    Values are loaded in priority order:

    1. Actual environment variables (highest priority).
    2. ``.env`` file in the working directory.
    3. Field defaults (lowest priority).

    Attributes marked *"required for live alerts"* may be left empty during
    development; the corresponding ``*_configured`` property will return
    ``False`` and the relevant subsystem will be disabled at runtime.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # ------------------------------------------------------------------
    # Telegram
    # ------------------------------------------------------------------
    telegram_bot_token: str = Field(
        default="",
        description="Bot token from @BotFather (required for live alerts).",
    )
    telegram_chat_id: str = Field(
        default="",
        description="Numeric chat ID for alert delivery.",
    )
    telegram_send_delay: float = Field(
        default=0.05,
        ge=0.0,
        description="Seconds between consecutive Telegram messages.",
    )

    # ------------------------------------------------------------------
    # LLM
    # ------------------------------------------------------------------
    openai_api_key: str = Field(default="", description="OpenAI API key.")
    anthropic_api_key: str = Field(default="", description="Anthropic API key.")
    llm_provider: str = Field(
        default="openai",
        description="LLM backend: 'openai' or 'anthropic'.",
    )
    llm_model: str = Field(
        default="gpt-4o-mini",
        description="Model name for the chosen LLM provider.",
    )
    llm_max_tokens: int = Field(
        default=256,
        ge=1,
        description="Maximum tokens in a single LLM completion.",
    )

    # ------------------------------------------------------------------
    # Facebook
    # ------------------------------------------------------------------
    facebook_email: str = Field(default="", description="Facebook login email.")
    facebook_password: str = Field(default="", description="Facebook login password.")
    facebook_session_path: str = Field(
        default="",
        description="Path to Playwright storage-state JSON file.",
    )
    facebook_group_ids: Annotated[list[str], NoDecode] = Field(
        default_factory=list,
        description="Facebook group IDs or slugs to monitor (comma-separated in env).",
    )
    facebook_post_limit: int = Field(
        default=5,
        ge=1,
        description="Number of recent posts to inspect per group per cycle.",
    )

    # ------------------------------------------------------------------
    # Search parameters
    # ------------------------------------------------------------------
    search_city: str = Field(
        default="Pordenone",
        description="Target city / search area for API providers.",
    )
    search_min_price: int = Field(
        default=0,
        ge=0,
        description="Minimum monthly rent in EUR (0 = no lower bound).",
    )
    search_max_price: int = Field(
        default=800,
        ge=0,
        description="Maximum monthly rent in EUR (0 = no upper bound).",
    )
    search_min_rooms: int = Field(
        default=2,
        ge=0,
        description="Minimum number of rooms (0 = no lower bound).",
    )
    search_max_rooms: int = Field(
        default=0,
        ge=0,
        description="Maximum number of rooms (0 = no upper bound).",
    )
    search_min_area: int = Field(
        default=0,
        ge=0,
        description="Minimum floor area in sqm (0 = no lower bound).",
    )
    search_keyword_blocklist: Annotated[list[str], NoDecode] = Field(
        default_factory=lambda: ["garage", "box", "posto auto", "magazzino", "deposito"],
        description="Keywords that reject a listing (comma-separated in env).",
    )

    # ------------------------------------------------------------------
    # Storage
    # ------------------------------------------------------------------
    database_path: str = Field(
        default="data/rentbot.db",
        description="Path to the SQLite database file.",
    )

    # ------------------------------------------------------------------
    # Polling intervals
    # ------------------------------------------------------------------
    poll_interval_api_min: int = Field(
        default=300,
        ge=1,
        description="Min seconds between API provider polls.",
    )
    poll_interval_api_max: int = Field(
        default=600,
        ge=1,
        description="Max seconds between API provider polls.",
    )
    poll_interval_browser_min: int = Field(
        default=900,
        ge=1,
        description="Min seconds between browser provider polls.",
    )
    poll_interval_browser_max: int = Field(
        default=1200,
        ge=1,
        description="Max seconds between browser provider polls.",
    )

    # ------------------------------------------------------------------
    # Runtime flags
    # ------------------------------------------------------------------
    seed_mode: bool = Field(
        default=False,
        description="Populate DB without sending alerts (first-run seeding).",
    )
    dry_run: bool = Field(
        default=False,
        description="Log alert payloads without sending Telegram messages.",
    )
    log_level: str = Field(default="INFO", description="Logging level.")
    log_format: str = Field(default="text", description="Log format: 'text' or 'json'.")

    # ------------------------------------------------------------------
    # Field validators
    # ------------------------------------------------------------------

    @field_validator("facebook_group_ids", mode="before")
    @classmethod
    def _parse_csv_group_ids(cls, v: str | list[str]) -> list[str]:
        """Accept a comma-separated string **or** an already-parsed list."""
        if isinstance(v, str):
            return _csv_to_list(v)
        return v

    @field_validator("search_keyword_blocklist", mode="before")
    @classmethod
    def _parse_csv_blocklist(cls, v: str | list[str]) -> list[str]:
        """Accept a comma-separated string **or** an already-parsed list."""
        if isinstance(v, str):
            return _csv_to_list(v)
        return v

    @field_validator("llm_provider")
    @classmethod
    def _validate_llm_provider(cls, v: str) -> str:
        allowed = {"openai", "anthropic"}
        if v not in allowed:
            raise ValueError(f"llm_provider must be one of {allowed}, got {v!r}")
        return v

    @field_validator("log_level")
    @classmethod
    def _validate_log_level(cls, v: str) -> str:
        allowed = {"DEBUG", "INFO", "WARNING", "ERROR"}
        v_upper = v.upper()
        if v_upper not in allowed:
            raise ValueError(f"log_level must be one of {allowed}, got {v!r}")
        return v_upper

    @field_validator("log_format")
    @classmethod
    def _validate_log_format(cls, v: str) -> str:
        allowed = {"text", "json"}
        v_lower = v.lower()
        if v_lower not in allowed:
            raise ValueError(f"log_format must be one of {allowed}, got {v!r}")
        return v_lower

    # ------------------------------------------------------------------
    # Model validators
    # ------------------------------------------------------------------

    @model_validator(mode="after")
    def _validate_poll_intervals(self) -> Settings:
        """Ensure min ≤ max for polling intervals."""
        if self.poll_interval_api_min > self.poll_interval_api_max:
            raise ValueError(
                f"poll_interval_api_min ({self.poll_interval_api_min}) "
                f"> poll_interval_api_max ({self.poll_interval_api_max})"
            )
        if self.poll_interval_browser_min > self.poll_interval_browser_max:
            raise ValueError(
                f"poll_interval_browser_min ({self.poll_interval_browser_min}) "
                f"> poll_interval_browser_max ({self.poll_interval_browser_max})"
            )
        return self

    # ------------------------------------------------------------------
    # Derived helpers
    # ------------------------------------------------------------------

    def to_filter_criteria(self) -> FilterCriteria:
        """Build a :class:`FilterCriteria` from the search parameters.

        Convention: a raw value of ``0`` means *"no bound"* and is converted
        to ``None`` in the criteria model.  This lets users express "no upper
        limit" naturally in ``.env`` files.
        """
        return FilterCriteria(
            price_min=self.search_min_price or None,
            price_max=self.search_max_price or None,
            rooms_min=self.search_min_rooms or None,
            rooms_max=self.search_max_rooms or None,
            area_sqm_min=self.search_min_area or None,
            keyword_blocklist=self.search_keyword_blocklist,
        )

    @property
    def database_path_resolved(self) -> Path:
        """Return the database path as a resolved :class:`~pathlib.Path`."""
        return Path(self.database_path).resolve()

    @property
    def telegram_configured(self) -> bool:
        """``True`` if both Telegram credentials are set."""
        return bool(self.telegram_bot_token and self.telegram_chat_id)

    @property
    def llm_configured(self) -> bool:
        """``True`` if the chosen LLM provider has an API key set."""
        if self.llm_provider == "openai":
            return bool(self.openai_api_key)
        if self.llm_provider == "anthropic":
            return bool(self.anthropic_api_key)
        return False

    @property
    def facebook_configured(self) -> bool:
        """``True`` if Facebook auth and at least one group ID are present."""
        has_auth = bool(self.facebook_email and self.facebook_password) or bool(
            self.facebook_session_path
        )
        return has_auth and bool(self.facebook_group_ids)
