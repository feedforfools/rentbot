"""Telegram MarkdownV2 listing message formatter.

Converts a :class:`~rentbot.core.models.Listing` into a ready-to-send
Telegram ``MarkdownV2`` message string.

Telegram MarkdownV2 escaping rules
-----------------------------------
The following characters **must** be escaped with a leading backslash when
they appear in ordinary message text::

    _ * [ ] ( ) ~ ` > # + - = | { } . !

Inside a ``[text](url)`` construct the URL portion follows different rules:
only ``)`` and ``\\`` need escaping so that Telegram's parser can locate the
closing parenthesis of the link target unambiguously.

Reference: https://core.telegram.org/bots/api#markdownv2-style

Public API
----------
:func:`escape_mdv2` — Escape a plain-text string for safe embedding in a
    MarkdownV2 message body.

:func:`escape_url` — Escape a URL for safe use inside a ``[text](url)`` span.

:func:`format_listing` — Format a :class:`~rentbot.core.models.Listing` into
    a complete, copy-ready MarkdownV2 Telegram message.

Typical usage::

    from rentbot.notifiers.formatter import format_listing

    text = format_listing(listing)
    await telegram_client.send_message(text, parse_mode="MarkdownV2")
"""

from __future__ import annotations

import logging
import re
from datetime import datetime

from rentbot.core.models import Listing, ListingSource

__all__ = [
    "escape_mdv2",
    "escape_url",
    "format_listing",
]

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Escaping
# ---------------------------------------------------------------------------

# All characters that the Telegram MarkdownV2 parser treats as special in the
# *body* (i.e. outside pre/code blocks).
_MDV2_SPECIAL = re.compile(r"([_*\[\]()~`>#+\-=|{}.!\\])")


def escape_mdv2(text: str) -> str:
    """Escape a plain-text string for safe embedding in MarkdownV2 message body.

    Prepends a backslash before every character in the MarkdownV2 special set:
    ``_ * [ ] ( ) ~ ` > # + - = | { } . ! \\``

    Args:
        text: Raw text to escape.  May contain any Unicode characters.

    Returns:
        Escaped string safe for direct embedding in a MarkdownV2 message.

    Examples:
        >>> escape_mdv2("€600/mese – Via Roma 1.")
        '€600/mese \\\\– Via Roma 1\\\\.'
    """
    return _MDV2_SPECIAL.sub(r"\\\1", text)


# Characters that must be escaped inside the URL part of a [text](url) span.
# Only ) and \ need escaping there; everything else is passed through verbatim.
_URL_SPECIAL = re.compile(r"([)\\])")


def escape_url(url: str) -> str:
    """Escape a URL for use inside the ``(url)`` part of a MarkdownV2 link.

    Only ``)`` and ``\\`` are special inside the parentheses of a
    ``[text](url)`` span — they would prematurely close the link construct.

    Args:
        url: Raw URL string (e.g. ``"https://www.immobiliare.it/annunci/1/"``).

    Returns:
        URL with ``)`` and ``\\`` backslash-escaped.
    """
    return _URL_SPECIAL.sub(r"\\\1", url)


# ---------------------------------------------------------------------------
# Source display names and icons
# ---------------------------------------------------------------------------

_SOURCE_LABEL: dict[ListingSource, str] = {
    ListingSource.IMMOBILIARE: "Immobiliare.it",
    ListingSource.CASA: "Casa.it",
    ListingSource.SUBITO: "Subito.it",
    ListingSource.FACEBOOK: "Facebook",
    ListingSource.IDEALISTA: "Idealista.it",
}


def _source_label(source: ListingSource) -> str:
    """Return the human-readable display name for a :class:`ListingSource`."""
    return _SOURCE_LABEL.get(source, source.value.capitalize())


# ---------------------------------------------------------------------------
# Field formatters (all return escaped MarkdownV2 fragments)
# ---------------------------------------------------------------------------


def _fmt_price(price: int) -> str:
    """Format price as ``€NNN/mo`` (bold), MarkdownV2-escaped."""
    # Thousands separator for large values.
    formatted = f"€{price:,}/mo".replace(",", ".")
    return f"*{escape_mdv2(formatted)}*"


def _fmt_rooms(rooms: int) -> str:
    """Format room count as ``N locali``."""
    label = "locale" if rooms == 1 else "locali"
    return escape_mdv2(f"{rooms} {label}")


def _fmt_area(area_sqm: int) -> str:
    """Format floor area as ``N m²``."""
    return escape_mdv2(f"{area_sqm} m²")


def _fmt_location(address: str | None, zone: str | None) -> str | None:
    """Format location line combining address and zone.

    Returns ``None`` if both are absent so the caller can skip the line.

    Examples:
        address="Via Roma 1", zone="Centro"  → "Via Roma 1 _(Centro)_"
        address=None, zone="Centro"          → "_(Centro)_"
        address="Via Roma 1", zone=None      → "Via Roma 1"
        address=None, zone=None              → None
    """
    if address and zone:
        return f"{escape_mdv2(address)} _\\({escape_mdv2(zone)}\\)_"
    if address:
        return escape_mdv2(address)
    if zone:
        return f"_\\({escape_mdv2(zone)}\\)_"
    return None


def _fmt_furnished(furnished: bool) -> str:
    """Format furnished status label."""
    return escape_mdv2("Arredato" if furnished else "Non arredato")


def _fmt_date(dt: datetime) -> str:
    """Format publication date as a short localised string."""
    # Day Month Year — no time (noisy in a chat notification).
    month_names = [
        "Gen",
        "Feb",
        "Mar",
        "Apr",
        "Mag",
        "Giu",
        "Lug",
        "Ago",
        "Set",
        "Ott",
        "Nov",
        "Dic",
    ]
    return escape_mdv2(f"{dt.day} {month_names[dt.month - 1]} {dt.year}")


# ---------------------------------------------------------------------------
# Main formatter
# ---------------------------------------------------------------------------

#: Maximum number of description characters to include in the message.
#: Telegram message limit is 4 096 chars; we leave margins for the header.
DESCRIPTION_MAX_CHARS: int = 300


def format_listing(listing: Listing) -> str:
    """Format a :class:`~rentbot.core.models.Listing` as a MarkdownV2 message.

    The message is structured in three sections:

    1. **Header** — title (bold) and source tag.
    2. **Details** — price, rooms, area, location, furnished status.
    3. **Footer** — optional description snippet and a deep-link button.

    Optional fields (``rooms``, ``area_sqm``, ``address``, ``zone``,
    ``furnished``, ``listing_date``, ``description``) are omitted gracefully
    when absent — no placeholder text is shown.

    Args:
        listing: Normalised listing to format.

    Returns:
        Complete MarkdownV2-formatted message string.  Safe to pass directly
        to :meth:`~rentbot.notifiers.telegram.TelegramClient.send_message`
        with ``parse_mode="MarkdownV2"``.
    """
    lines: list[str] = []

    # ------------------------------------------------------------------
    # Section 1: Title
    # ------------------------------------------------------------------
    lines.append(f"*{escape_mdv2(listing.title)}*")

    # ------------------------------------------------------------------
    # Section 2: Key details (price | rooms | area on one line)
    # ------------------------------------------------------------------
    detail_parts: list[str] = [_fmt_price(listing.price)]
    if listing.rooms is not None:
        detail_parts.append(_fmt_rooms(listing.rooms))
    if listing.area_sqm is not None:
        detail_parts.append(_fmt_area(listing.area_sqm))

    lines.append("  •  ".join(detail_parts))

    # Location line (optional)
    location = _fmt_location(listing.address, listing.zone)
    if location:
        lines.append(f"📍 {location}")

    # Furnished status (optional)
    if listing.furnished is not None:
        lines.append(f"🛋 {_fmt_furnished(listing.furnished)}")

    # ------------------------------------------------------------------
    # Section 3: Source + date footer
    # ------------------------------------------------------------------
    source_text = _source_label(listing.source)
    if listing.listing_date is not None:
        footer = f"{source_text} · {listing.listing_date.strftime('%d %b %Y')}"
    else:
        footer = source_text
    lines.append(f"\n_{escape_mdv2(footer)}_")

    # ------------------------------------------------------------------
    # Section 4: Description snippet (optional)
    # ------------------------------------------------------------------
    if listing.description:
        snippet = listing.description.strip()
        if len(snippet) > DESCRIPTION_MAX_CHARS:
            snippet = snippet[:DESCRIPTION_MAX_CHARS].rstrip() + "…"
        lines.append(f"\n{escape_mdv2(snippet)}")

    # ------------------------------------------------------------------
    # Section 5: CTA link
    # ------------------------------------------------------------------
    lines.append(f"\n[🔗 Vedi annuncio]({escape_url(listing.url)})")

    return "\n".join(lines)
