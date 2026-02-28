"""Shared pytest fixtures and configuration for the Rentbot test suite.

This file is loaded automatically by pytest before any test module.
It provides project-wide fixtures used across unit and integration tests.
"""

from __future__ import annotations

import logging
import os

import pytest
from pydantic_settings import SettingsConfigDict

from rentbot.core import configure_logging
from rentbot.core.settings import Settings


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _configure_test_logging() -> None:
    """Force DEBUG logging in text format for every test.

    The ``autouse=True`` flag means this fixture runs for every test without
    needing to be requested explicitly.  Using ``force=True`` ensures the
    configuration is applied even when pytest's own ``log_cli`` handler is
    already present.
    """
    configure_logging(level="DEBUG", fmt="text", force=True)


# ---------------------------------------------------------------------------
# Environment
# ---------------------------------------------------------------------------


@pytest.fixture()
def clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Remove all ``RENTBOT_*`` and sensitive env vars for the duration of a test.

    Useful when testing settings loading to ensure a clean environment
    without real credentials bleeding in from the developer's shell.

    Also disables pydantic-settings `.env` file loading so that credentials
    present in a local `.env` file do not leak into Settings isolation tests.
    """
    sensitive_prefixes = (
        "TELEGRAM_",
        "OPENAI_",
        "ANTHROPIC_",
        "FACEBOOK_",
        "SEARCH_",
        "DATABASE_",
        "POLL_",
        "LLM_",
        "SEED_MODE",
        "DRY_RUN",
        "LOG_LEVEL",
        "LOG_FORMAT",
    )
    for key in list(os.environ):
        if any(key.startswith(prefix) for prefix in sensitive_prefixes):
            monkeypatch.delenv(key, raising=False)

    # Prevent pydantic-settings from reading the on-disk .env file.
    # Without this, real credentials present in .env bleed into Settings()
    # even after the env-var patch above, because pydantic-settings reads the
    # file directly rather than via os.environ.
    monkeypatch.setattr(
        Settings,
        "model_config",
        SettingsConfigDict(
            env_file=None,
            env_file_encoding="utf-8",
            extra="ignore",
        ),
    )


# ---------------------------------------------------------------------------
# Misc helpers
# ---------------------------------------------------------------------------


@pytest.fixture()
def logger() -> logging.Logger:
    """Return a ``logging.Logger`` scoped to the running test.

    The logger name is ``tests.<test-module>`` so log lines are clearly
    attributed to test code rather than production code under test.
    """
    return logging.getLogger("tests")
