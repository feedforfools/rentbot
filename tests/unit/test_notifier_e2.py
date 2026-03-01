"""Unit tests for Epic 2 deliverables.

Covers:
- :func:`~rentbot.notifiers.formatter.escape_mdv2` escaping correctness.
- :func:`~rentbot.notifiers.formatter.escape_url` escaping correctness.
- :func:`~rentbot.notifiers.formatter.format_listing` output structure,
  optional-field omission, special-character handling, and description
  truncation.
- :class:`~rentbot.notifiers.notifier.Notifier` constructor validation.
- :meth:`~rentbot.notifiers.notifier.Notifier.send_alert` in seed, dry-run,
  live-success, and live-failure modes.
- :meth:`~rentbot.notifiers.notifier.Notifier.send_alerts` throttled bulk
  delivery: ordering, sleep cadence, failure isolation, and seed bypass.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from rentbot.core.exceptions import TelegramError
from rentbot.core.models import Listing, ListingSource
from rentbot.core.run_context import RunContext
from rentbot.notifiers.formatter import (
    DESCRIPTION_MAX_CHARS,
    escape_mdv2,
    escape_url,
    format_listing,
)
from rentbot.notifiers.notifier import _DEFAULT_MIN_INTERVAL, Notifier
from rentbot.notifiers.telegram import TelegramClient

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_listing(
    *,
    id: str = "99999",
    source: ListingSource = ListingSource.IMMOBILIARE,
    title: str = "Bilocale centro storico",
    price: int = 600,
    rooms: int | None = 2,
    area_sqm: int | None = 65,
    address: str | None = "Via Roma 1",
    zone: str | None = "Centro",
    furnished: bool | None = True,
    url: str = "https://www.immobiliare.it/annunci/99999/",
    image_url: str | None = None,
    description: str = "Luminoso appartamento ristrutturato.",
    listing_date: datetime | None = datetime(2026, 2, 28, 10, 0, 0, tzinfo=UTC),
) -> Listing:
    """Return a valid :class:`Listing` with sensible defaults for E2 tests."""
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
        listing_date=listing_date,
    )


def _make_notifier(
    *,
    dry_run: bool = False,
    seed: bool = False,
    min_interval_s: float = 0.0,
    send_message_side_effect: Any = None,
) -> Notifier:
    """Return a :class:`Notifier` wired to a mock :class:`TelegramClient`."""
    client = MagicMock(spec=TelegramClient)
    client.send_message = AsyncMock(side_effect=send_message_side_effect)
    ctx = RunContext(dry_run=dry_run, seed=seed)
    return Notifier(client=client, ctx=ctx, min_interval_s=min_interval_s)


# ---------------------------------------------------------------------------
# escape_mdv2
# ---------------------------------------------------------------------------


class TestEscapeMdv2:
    """Tests for :func:`escape_mdv2`."""

    def test_plain_text_unchanged(self) -> None:
        assert escape_mdv2("hello world") == "hello world"

    def test_all_special_chars_escaped(self) -> None:
        # Each of these characters must be escaped by a leading backslash.
        special_chars = r"_*[]()~`>#+\-=|{}.!"
        for char in special_chars:
            result = escape_mdv2(char)
            assert result == f"\\{char}", (
                f"Character {char!r} should be escaped to '\\\\{char}', got {result!r}"
            )

    def test_underscore_escaped(self) -> None:
        assert escape_mdv2("hello_world") == r"hello\_world"

    def test_dot_escaped(self) -> None:
        assert escape_mdv2("Casa.it") == r"Casa\.it"

    def test_dash_escaped(self) -> None:
        assert escape_mdv2("600-800") == r"600\-800"

    def test_exclamation_escaped(self) -> None:
        assert escape_mdv2("Ciao!") == r"Ciao\!"

    def test_backslash_escaped(self) -> None:
        assert escape_mdv2("a\\b") == r"a\\b"

    def test_opening_paren_escaped(self) -> None:
        assert escape_mdv2("(test)") == r"\(test\)"

    def test_euro_sign_not_escaped(self) -> None:
        """€ is not a MarkdownV2 special character."""
        assert escape_mdv2("€600") == "€600"

    def test_unicode_not_escaped(self) -> None:
        assert escape_mdv2("Pordenone") == "Pordenone"

    def test_empty_string(self) -> None:
        assert escape_mdv2("") == ""


# ---------------------------------------------------------------------------
# escape_url
# ---------------------------------------------------------------------------


class TestEscapeUrl:
    """Tests for :func:`escape_url`."""

    def test_plain_url_unchanged(self) -> None:
        url = "https://www.immobiliare.it/annunci/99999/"
        assert escape_url(url) == url

    def test_closing_paren_escaped(self) -> None:
        url = "https://example.com/search?q=foo(bar)"
        assert escape_url(url) == "https://example.com/search?q=foo(bar\\)"

    def test_backslash_escaped(self) -> None:
        url = "https://example.com/a\\b"
        assert escape_url(url) == "https://example.com/a\\\\b"

    def test_underscore_not_escaped(self) -> None:
        """_ is specially treated in body text but NOT inside a URL span."""
        url = "https://example.com/a_b"
        assert escape_url(url) == url

    def test_dot_not_escaped_in_url(self) -> None:
        """Dots do NOT need escaping inside a URL link target."""
        url = "https://casa.it/annunci/1/"
        assert escape_url(url) == url

    def test_empty_string(self) -> None:
        assert escape_url("") == ""


# ---------------------------------------------------------------------------
# format_listing
# ---------------------------------------------------------------------------


class TestFormatListing:
    """Tests for :func:`format_listing`."""

    def test_title_in_bold(self) -> None:
        listing = _make_listing(title="Bilocale luminoso")
        text = format_listing(listing)
        assert "*Bilocale luminoso*" in text

    def test_price_present(self) -> None:
        listing = _make_listing(price=650)
        text = format_listing(listing)
        # Price is formatted as €650/mo and bolded; dots/slash are escaped.
        assert "650" in text
        assert "€" in text

    def test_rooms_present_when_set(self) -> None:
        listing = _make_listing(rooms=3)
        text = format_listing(listing)
        assert "3 locali" in text

    def test_rooms_absent_when_none(self) -> None:
        listing = _make_listing(rooms=None)
        text = format_listing(listing)
        assert "locali" not in text

    def test_area_present_when_set(self) -> None:
        listing = _make_listing(area_sqm=70)
        text = format_listing(listing)
        assert "70" in text

    def test_area_absent_when_none(self) -> None:
        listing = _make_listing(area_sqm=None)
        text = format_listing(listing)
        # m² is used only for area; verify the symbol is absent.
        assert "m²" not in text

    def test_address_present_when_set(self) -> None:
        listing = _make_listing(address="Via Mazzini 5", zone=None)
        text = format_listing(listing)
        assert "Via Mazzini 5" in text
        assert "📍" in text

    def test_zone_present_when_set(self) -> None:
        listing = _make_listing(address=None, zone="Borgomeduna")
        text = format_listing(listing)
        assert "Borgomeduna" in text
        assert "📍" in text

    def test_location_absent_when_both_none(self) -> None:
        listing = _make_listing(address=None, zone=None)
        text = format_listing(listing)
        assert "📍" not in text

    def test_furnished_arredato(self) -> None:
        listing = _make_listing(furnished=True)
        text = format_listing(listing)
        assert "Arredato" in text
        assert "🛋" in text

    def test_furnished_non_arredato(self) -> None:
        listing = _make_listing(furnished=False)
        text = format_listing(listing)
        assert "Non arredato" in text

    def test_furnished_absent_when_none(self) -> None:
        listing = _make_listing(furnished=None)
        text = format_listing(listing)
        assert "🛋" not in text

    def test_source_in_footer(self) -> None:
        listing = _make_listing(source=ListingSource.SUBITO)
        text = format_listing(listing)
        # The dot is MarkdownV2-escaped in the output: Subito\.it
        assert "Subito" in text
        assert "\\.it" in text

    def test_listing_date_absent_when_none(self) -> None:
        listing = _make_listing(listing_date=None)
        text = format_listing(listing)
        # Date absent — source label still present, no year or month number.
        assert "Immobiliare" in text

    def test_description_present_when_set(self) -> None:
        listing = _make_listing(description="Ottimo appartamento vicino al centro.")
        text = format_listing(listing)
        assert "Ottimo appartamento" in text

    def test_description_absent_when_empty(self) -> None:
        listing = _make_listing(description="")
        text = format_listing(listing)
        # Should not add any extra blank lines for missing description.
        assert text.count("\n\n") <= 3  # header + details + footer gaps only

    def test_description_truncated_to_max_chars(self) -> None:
        # Use 'X' — a char that won't appear in any other listing field.
        long_desc = "X" * (DESCRIPTION_MAX_CHARS + 50)
        listing = _make_listing(description=long_desc)
        text = format_listing(listing)
        # The ellipsis marker is added after truncation.
        assert "\u2026" in text
        # X count must be exactly DESCRIPTION_MAX_CHARS (truncated to max).
        assert text.count("X") == DESCRIPTION_MAX_CHARS

    def test_cta_link_present(self) -> None:
        listing = _make_listing(url="https://www.immobiliare.it/annunci/99999/")
        text = format_listing(listing)
        assert "Vedi annuncio" in text
        assert "https://www.immobiliare.it/annunci/99999/" in text

    def test_url_closing_paren_escaped_in_link(self) -> None:
        """A ) in the listing URL must be escaped so the MD link doesn't break."""
        listing = _make_listing(url="https://example.com/search?q=foo(bar)")
        text = format_listing(listing)
        # The closing ) after "bar" must be escaped to \\) inside the link.
        assert "bar\\)" in text

    def test_special_chars_in_title_escaped(self) -> None:
        listing = _make_listing(title="Flat! 3 rooms – Via Roma_Nord")
        text = format_listing(listing)
        assert "\\!" in text
        assert "\\_" in text

    def test_single_room_label(self) -> None:
        listing = _make_listing(rooms=1)
        text = format_listing(listing)
        assert "1 locale" in text
        assert "locali" not in text

    def test_minimum_listing_no_optional_fields(self) -> None:
        """A listing with only required fields must not raise."""
        listing = Listing(
            id="1",
            source=ListingSource.SUBITO,
            title="Monolocale",
            price=400,
            url="https://subito.it/1/",
        )
        text = format_listing(listing)
        assert "Monolocale" in text
        assert "400" in text
        assert "Subito" in text


# ---------------------------------------------------------------------------
# Notifier — constructor
# ---------------------------------------------------------------------------


class TestNotifierConstructor:
    """Tests for :class:`Notifier` constructor validation."""

    def test_default_min_interval(self) -> None:
        notifier = _make_notifier()
        assert notifier._min_interval_s == 0.0  # overridden in _make_notifier helper

    def test_constant_default_value(self) -> None:
        assert pytest.approx(0.05) == _DEFAULT_MIN_INTERVAL

    def test_negative_min_interval_raises(self) -> None:
        client = MagicMock(spec=TelegramClient)
        ctx = RunContext()
        with pytest.raises(ValueError, match="min_interval_s"):
            Notifier(client=client, ctx=ctx, min_interval_s=-0.1)

    def test_zero_min_interval_valid(self) -> None:
        client = MagicMock(spec=TelegramClient)
        ctx = RunContext()
        notifier = Notifier(client=client, ctx=ctx, min_interval_s=0.0)
        assert notifier._min_interval_s == 0.0


# ---------------------------------------------------------------------------
# Notifier.send_alert — mode gating
# ---------------------------------------------------------------------------


class TestNotifierSendAlert:
    """Tests for :meth:`Notifier.send_alert`."""

    async def test_seed_mode_returns_false(self) -> None:
        notifier = _make_notifier(seed=True)
        result = await notifier.send_alert(_make_listing())
        assert result is False

    async def test_seed_mode_does_not_call_send_message(self) -> None:
        notifier = _make_notifier(seed=True)
        await notifier.send_alert(_make_listing())
        notifier._client.send_message.assert_not_called()  # type: ignore[attr-defined]

    async def test_dry_run_returns_true(self) -> None:
        notifier = _make_notifier(dry_run=True)
        result = await notifier.send_alert(_make_listing())
        assert result is True

    async def test_dry_run_does_not_call_send_message(self) -> None:
        notifier = _make_notifier(dry_run=True)
        await notifier.send_alert(_make_listing())
        notifier._client.send_message.assert_not_called()  # type: ignore[attr-defined]

    async def test_dry_run_logs_payload(self, caplog: pytest.LogCaptureFixture) -> None:
        notifier = _make_notifier(dry_run=True)
        listing = _make_listing(title="Bilocale luminoso")
        with caplog.at_level(logging.INFO, logger="rentbot.notifiers.notifier"):
            await notifier.send_alert(listing)
        assert any("[dry-run]" in r.message for r in caplog.records)

    async def test_live_success_returns_true(self) -> None:
        notifier = _make_notifier()
        result = await notifier.send_alert(_make_listing())
        assert result is True

    async def test_live_success_calls_send_message(self) -> None:
        notifier = _make_notifier()
        await notifier.send_alert(_make_listing())
        notifier._client.send_message.assert_called_once()  # type: ignore[attr-defined]

    async def test_live_success_logs_sent(self, caplog: pytest.LogCaptureFixture) -> None:
        notifier = _make_notifier()
        with caplog.at_level(logging.INFO, logger="rentbot.notifiers.notifier"):
            await notifier.send_alert(_make_listing())
        assert any("Alert sent" in r.message for r in caplog.records)

    async def test_telegram_error_is_logged_and_reraised(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        err = TelegramError("Bad request", status_code=400)
        notifier = _make_notifier(send_message_side_effect=err)
        with (
            caplog.at_level(logging.ERROR, logger="rentbot.notifiers.notifier"),
            pytest.raises(TelegramError),
        ):
            await notifier.send_alert(_make_listing())
        assert any(
            r.levelno == logging.ERROR and "Failed to send alert" in r.message
            for r in caplog.records
        )

    async def test_unexpected_exception_is_logged_critical_and_reraised(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        notifier = _make_notifier(send_message_side_effect=RuntimeError("boom"))
        with (
            caplog.at_level(logging.CRITICAL, logger="rentbot.notifiers.notifier"),
            pytest.raises(RuntimeError, match="boom"),
        ):
            await notifier.send_alert(_make_listing())
        assert any(r.levelno == logging.CRITICAL for r in caplog.records)


# ---------------------------------------------------------------------------
# Notifier.send_alerts — bulk throttle
# ---------------------------------------------------------------------------


class TestNotifierSendAlerts:
    """Tests for :meth:`Notifier.send_alerts`."""

    async def test_empty_list_returns_zero_zero(self) -> None:
        notifier = _make_notifier()
        sent, failed = await notifier.send_alerts([])
        assert (sent, failed) == (0, 0)

    async def test_empty_list_no_send_message_call(self) -> None:
        notifier = _make_notifier()
        await notifier.send_alerts([])
        notifier._client.send_message.assert_not_called()  # type: ignore[attr-defined]

    async def test_single_listing_returns_one_zero(self) -> None:
        notifier = _make_notifier()
        sent, failed = await notifier.send_alerts([_make_listing()])
        assert (sent, failed) == (1, 0)

    async def test_multiple_listings_all_sent(self) -> None:
        listings = [_make_listing(id=str(i)) for i in range(5)]
        notifier = _make_notifier()
        sent, failed = await notifier.send_alerts(listings)
        assert (sent, failed) == (5, 0)

    async def test_sleep_called_between_messages(self) -> None:
        """Sleep is called exactly N-1 times for N listings."""
        listings = [_make_listing(id=str(i)) for i in range(4)]
        notifier = _make_notifier(min_interval_s=0.01)

        with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            await notifier.send_alerts(listings)

        assert mock_sleep.call_count == 3
        mock_sleep.assert_called_with(0.01)

    async def test_no_sleep_for_single_listing(self) -> None:
        notifier = _make_notifier(min_interval_s=0.01)
        with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            await notifier.send_alerts([_make_listing()])
        mock_sleep.assert_not_called()

    async def test_no_sleep_when_interval_is_zero(self) -> None:
        listings = [_make_listing(id=str(i)) for i in range(3)]
        notifier = _make_notifier(min_interval_s=0.0)
        with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            await notifier.send_alerts(listings)
        mock_sleep.assert_not_called()

    async def test_failed_listing_counted_and_batch_continues(self) -> None:
        """A single TelegramError mid-batch does not abort remaining sends."""
        err = TelegramError("rate limited", status_code=429)
        # Fail on second call, succeed on others.
        side_effects = [None, err, None]
        client = MagicMock(spec=TelegramClient)
        client.send_message = AsyncMock(side_effect=side_effects)
        ctx = RunContext()
        notifier = Notifier(client=client, ctx=ctx, min_interval_s=0.0)

        listings = [_make_listing(id=str(i)) for i in range(3)]
        sent, failed = await notifier.send_alerts(listings)

        assert sent == 2
        assert failed == 1
        assert client.send_message.call_count == 3  # all three attempted

    async def test_all_fail_returns_zero_n(self) -> None:
        err = TelegramError("server error", status_code=500)
        notifier = _make_notifier(send_message_side_effect=err)
        listings = [_make_listing(id=str(i)) for i in range(3)]
        sent, failed = await notifier.send_alerts(listings)
        assert sent == 0
        assert failed == 3

    async def test_seed_mode_bulk_returns_zero_zero(self) -> None:
        """In seed mode no alerts are sent; neither sent nor failed increments."""
        notifier = _make_notifier(seed=True)
        listings = [_make_listing(id=str(i)) for i in range(5)]
        sent, failed = await notifier.send_alerts(listings)
        assert (sent, failed) == (0, 0)

    async def test_seed_mode_no_send_message_in_bulk(self) -> None:
        notifier = _make_notifier(seed=True)
        listings = [_make_listing(id=str(i)) for i in range(3)]
        await notifier.send_alerts(listings)
        notifier._client.send_message.assert_not_called()  # type: ignore[attr-defined]

    async def test_dry_run_bulk_all_logged_not_sent(self) -> None:
        notifier = _make_notifier(dry_run=True)
        listings = [_make_listing(id=str(i)) for i in range(3)]
        sent, failed = await notifier.send_alerts(listings)
        assert sent == 3
        assert failed == 0
        notifier._client.send_message.assert_not_called()  # type: ignore[attr-defined]

    async def test_summary_warning_logged_on_failures(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        err = TelegramError("err", status_code=500)
        notifier = _make_notifier(send_message_side_effect=err)
        listings = [_make_listing(id=str(i)) for i in range(2)]
        with caplog.at_level(logging.WARNING, logger="rentbot.notifiers.notifier"):
            await notifier.send_alerts(listings)
        assert any("failed" in r.message for r in caplog.records)

    async def test_summary_info_logged_on_full_success(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        notifier = _make_notifier()
        listings = [_make_listing(id=str(i)) for i in range(2)]
        with caplog.at_level(logging.INFO, logger="rentbot.notifiers.notifier"):
            await notifier.send_alerts(listings)
        assert any("batch finished" in r.message for r in caplog.records)
