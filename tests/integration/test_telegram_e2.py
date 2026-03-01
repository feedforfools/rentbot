"""Integration tests — Telegram delivery with real API connection (E2-T8).

These tests exercise the full :class:`~rentbot.notifiers.telegram.TelegramClient`
→ :class:`~rentbot.notifiers.notifier.Notifier` → Telegram API round-trip
using real HTTP against the live Bot API endpoint.

Default behaviour
-----------------
All tests in this module are marked ``@pytest.mark.integration`` and are
**excluded from the default test run** (``addopts = "-m 'not integration'"``
in ``pyproject.toml``).  This prevents Telegram spam during routine ``pytest``
invocations.

Run on demand
-------------
To run *only* the integration tests::

    pytest -m integration

To run the full suite including integration tests::

    pytest -m ""

Credentials
-----------
Tests that require a real connection are skipped if ``TELEGRAM_BOT_TOKEN``
or ``TELEGRAM_CHAT_ID`` are not present in the environment (or in a ``.env``
file in the project root).
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

import pytest
from dotenv import load_dotenv

from rentbot.core.models import Listing, ListingSource
from rentbot.core.run_context import RunContext
from rentbot.notifiers.notifier import Notifier
from rentbot.notifiers.telegram import TelegramClient

__all__: list[str] = []

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Load .env so developers can run integration tests locally without exporting
# credentials into the shell manually.
# ---------------------------------------------------------------------------
load_dotenv()

# ---------------------------------------------------------------------------
# Skip guard — evaluated once at collection time.
# ---------------------------------------------------------------------------

_TELEGRAM_CONFIGURED: bool = bool(
    os.environ.get("TELEGRAM_BOT_TOKEN") and os.environ.get("TELEGRAM_CHAT_ID")
)

_skip_if_unconfigured = pytest.mark.skipif(
    not _TELEGRAM_CONFIGURED,
    reason=(
        "TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID must be set to run Telegram "
        "integration tests. Add them to .env or export them in your shell."
    ),
)


# ---------------------------------------------------------------------------
# Fixture: canonical mock listing used across all integration tests
# ---------------------------------------------------------------------------


@pytest.fixture()
def mock_listing() -> Listing:
    """Return a clearly-labelled mock :class:`Listing` for integration tests.

    The title prefix ``[Rentbot Integration Test]`` makes automated messages
    easily distinguishable from real alerts in the target Telegram chat.
    """
    return Listing(
        id="integration-test-001",
        source=ListingSource.IMMOBILIARE,
        title="[Rentbot Integration Test] Bilocale Centro Storico",
        price=650,
        rooms=2,
        area_sqm=65,
        address="Via Mazzini 12, Pordenone",
        zone="Centro",
        furnished=True,
        url="https://www.immobiliare.it/annunci/999999999/",
        image_url=None,
        description=(
            "⚠️ Automated integration test message from Rentbot. "
            "This is NOT a real listing — safe to ignore. "
            "A real alert would show full property details and contact info here."
        ),
        listing_date=datetime(2026, 2, 28, 12, 0, 0, tzinfo=timezone.utc),
    )


@pytest.fixture()
def bulk_mock_listings() -> list[Listing]:
    """Return three slightly-varied mock listings for bulk-send tests."""
    return [
        Listing(
            id=f"integration-test-bulk-{i:03d}",
            source=ListingSource.IMMOBILIARE,
            title=f"[Rentbot Integration Test] Batch message {i}/3",
            price=500 + i * 50,
            rooms=i,
            area_sqm=50 + i * 10,
            address=f"Via Test {i}, Pordenone",
            zone="Centro",
            furnished=bool(i % 2),
            url=f"https://www.immobiliare.it/annunci/{9000 + i}/",
            description=(
                f"Batch integration test listing #{i}/3. "
                "⚠️ Automated test — safe to ignore."
            ),
            listing_date=datetime(2026, 2, 28, 12, i, 0, tzinfo=timezone.utc),
        )
        for i in range(1, 4)
    ]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestTelegramIntegration:
    """End-to-end integration tests for the Telegram notification pipeline."""

    # ------------------------------------------------------------------
    # Dry-run path — always runs (no credentials required)
    # ------------------------------------------------------------------

    async def test_dry_run_always_passes(
        self,
        mock_listing: Listing,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Full stack in dry-run mode exercises formatting without a real send.

        This test **always runs** regardless of whether Telegram credentials
        are configured.  It verifies:

        * :meth:`~rentbot.notifiers.notifier.Notifier.send_alert` returns
          ``True`` in dry-run mode.
        * A ``[dry-run]`` log line is emitted at ``INFO`` level.
        * No real HTTP request is ever made (TelegramClient transport is
          opened but :meth:`~rentbot.notifiers.telegram.TelegramClient.send_message`
          is never called in dry-run mode).

        Placeholder ``token`` / ``chat_id`` values are accepted because
        :class:`~rentbot.notifiers.telegram.TelegramClient` only validates
        that they are non-empty strings; network I/O is never triggered in
        dry-run mode.
        """
        ctx = RunContext(dry_run=True, seed=False)

        async with TelegramClient(
            token="placeholder:dry_run_token",
            chat_id="0",
            # Keep connection timeout short — no real network call happens, but
            # httpx still creates the client object.
            connect_timeout=1.0,
        ) as client:
            notifier = Notifier(client=client, ctx=ctx, min_interval_s=0.0)

            with caplog.at_level(logging.INFO, logger="rentbot.notifiers.notifier"):
                result = await notifier.send_alert(mock_listing)

        assert result is True, "send_alert should return True in dry-run mode."
        dry_run_records = [r for r in caplog.records if "[dry-run]" in r.message]
        assert dry_run_records, (
            "Expected at least one '[dry-run]' log record from the notifier, "
            f"got records: {[r.message for r in caplog.records]}"
        )
        logger.info("dry-run integration test passed — formatted message logged correctly.")

    # ------------------------------------------------------------------
    # Live paths — skipped when credentials are absent
    # ------------------------------------------------------------------

    @_skip_if_unconfigured
    async def test_live_send_single_listing(self, mock_listing: Listing) -> None:
        """Send a single mock listing to the real Telegram chat.

        Verifies that:

        * No exception is raised (HTTP 200 from the Telegram Bot API).
        * :meth:`~rentbot.notifiers.notifier.Notifier.send_alert` returns
          ``True``.

        **Requires:** ``TELEGRAM_BOT_TOKEN`` and ``TELEGRAM_CHAT_ID`` in
        the environment or ``.env``.
        """
        token = os.environ["TELEGRAM_BOT_TOKEN"]
        chat_id = os.environ["TELEGRAM_CHAT_ID"]

        ctx = RunContext(dry_run=False, seed=False)

        async with TelegramClient(token=token, chat_id=chat_id) as client:
            notifier = Notifier(client=client, ctx=ctx, min_interval_s=0.0)
            result = await notifier.send_alert(mock_listing)

        assert result is True, "send_alert should return True after a successful live send."
        logger.info(
            "Live single-message integration test passed — alert delivered to chat %s.",
            chat_id,
        )

    @_skip_if_unconfigured
    async def test_live_send_bulk_listings(
        self,
        bulk_mock_listings: list[Listing],
    ) -> None:
        """Send a batch of three mock listings to the real Telegram chat.

        Verifies that:

        * :meth:`~rentbot.notifiers.notifier.Notifier.send_alerts` returns
          ``(sent=3, failed=0)``.
        * No messages are dropped across the throttled burst.

        **Requires:** ``TELEGRAM_BOT_TOKEN`` and ``TELEGRAM_CHAT_ID`` in
        the environment or ``.env``.
        """
        token = os.environ["TELEGRAM_BOT_TOKEN"]
        chat_id = os.environ["TELEGRAM_CHAT_ID"]

        ctx = RunContext(dry_run=False, seed=False)

        # Use a 100 ms inter-message gap for the bulk test — well under the
        # 30 msgs/sec Telegram limit and short enough for a test.
        async with TelegramClient(token=token, chat_id=chat_id) as client:
            notifier = Notifier(client=client, ctx=ctx, min_interval_s=0.1)
            sent, failed = await notifier.send_alerts(bulk_mock_listings)

        assert sent == 3, f"Expected 3 sent, got {sent}."
        assert failed == 0, f"Expected 0 failed, got {failed}."
        logger.info(
            "Live bulk integration test passed — %d/3 messages delivered to chat %s.",
            sent,
            chat_id,
        )

    @_skip_if_unconfigured
    async def test_seed_mode_suppresses_real_send(self, mock_listing: Listing) -> None:
        """Seed mode must not send any messages even with live credentials.

        This guards against accidentally flooding Telegram during a first-run
        seeding cycle.  The test verifies:

        * :meth:`~rentbot.notifiers.notifier.Notifier.send_alert` returns
          ``False`` (seed-suppressed).
        * No HTTP call is made to the Telegram API (confirmed by zero messages
          appearing in the chat and the ``False`` return value).

        **Requires:** ``TELEGRAM_BOT_TOKEN`` and ``TELEGRAM_CHAT_ID`` in
        the environment or ``.env``.
        """
        token = os.environ["TELEGRAM_BOT_TOKEN"]
        chat_id = os.environ["TELEGRAM_CHAT_ID"]

        ctx = RunContext(dry_run=False, seed=True)

        async with TelegramClient(token=token, chat_id=chat_id) as client:
            notifier = Notifier(client=client, ctx=ctx, min_interval_s=0.0)
            result = await notifier.send_alert(mock_listing)

        assert result is False, (
            "send_alert must return False in seed mode — no message should be sent."
        )
        logger.info(
            "Seed-mode suppression test passed — no message sent despite live credentials."
        )
