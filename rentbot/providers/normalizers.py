"""Provider-level field normalisation utilities for Rentbot.

All provider ``_map_result`` methods should use these helpers to normalise
raw API / HTML data into the typed fields expected by
:class:`~rentbot.core.models.Listing`.  Centralising the logic here means:

* Field-parsing logic is tested once, not per-provider.
* New providers can normalise data consistently without copy-pasting helpers.
* Edge cases (e.g. ``"5+"`` room strings, ``"60 m²"`` area strings) are
  handled in a single place.

**ID contract**
---------------
Providers **must** set ``Listing.id`` to the raw, provider-local identifier
(e.g. ``"12345678"`` from Immobiliare, ``"abc-def-ghi"`` from Subito).  They
must *not* call :func:`~rentbot.core.ids.canonical_id` themselves — the
storage layer computes the canonical ``"<source>:<id>"`` key via
:func:`~rentbot.core.ids.canonical_id_from_listing` at persistence time.

+-----------+---------------------------------------+------------------------+
| Layer     | What it stores in ``id``              | Example                |
+===========+=======================================+========================+
| Provider  | Raw provider-local id (``str``)       | ``"12345678"``         |
+-----------+---------------------------------------+------------------------+
| Storage   | ``canonical_id(source, raw_id)``      | ``"immobiliare:12345678"`` |
+-----------+---------------------------------------+------------------------+

Typical usage::

    from rentbot.providers.normalizers import (
        normalise_price,
        normalise_rooms,
        normalise_area_sqm,
        normalise_text,
        normalise_title,
        normalise_url,
        furnished_from_text,
    )

    price = normalise_price(raw["price"])
    rooms = normalise_rooms(raw.get("rooms"))
    area  = normalise_area_sqm(raw.get("surface"))
"""

from __future__ import annotations

import logging
import re
from typing import Any

__all__ = [
    "normalise_text",
    "normalise_title",
    "normalise_price",
    "normalise_rooms",
    "normalise_area_sqm",
    "normalise_url",
    "furnished_from_text",
]

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

# Matches one or more whitespace characters (for general text normalisation).
_WHITESPACE_RE: re.Pattern[str] = re.compile(r"\s+")

# Matches the leading integer in an area / surface string (e.g. "60 m²").
_LEADING_INT_RE: re.Pattern[str] = re.compile(r"\d+")

# Matches a trailing "+" in Italian room-count strings (e.g. "5+").
_TRAILING_PLUS_RE: re.Pattern[str] = re.compile(r"\+\s*$")


# ---------------------------------------------------------------------------
# Text normalisation
# ---------------------------------------------------------------------------


def normalise_text(value: str | None, *, fallback: str = "") -> str:
    """Strip and collapse whitespace in a text value.

    Returns *fallback* (default ``""``) when *value* is ``None`` or empty
    after stripping.  All internal whitespace sequences are collapsed to a
    single space.

    Args:
        value: Raw string from an API or HTML response.
        fallback: Value to return when *value* is absent or blank.

    Returns:
        Cleaned string, or *fallback*.

    Examples::

        normalise_text("  hello  world  ")  # → "hello world"
        normalise_text(None)                # → ""
        normalise_text(None, fallback="—")  # → "—"
    """
    if not value:
        return fallback
    cleaned = _WHITESPACE_RE.sub(" ", value).strip()
    return cleaned if cleaned else fallback


def normalise_title(value: Any, *, fallback_id: str) -> str:
    """Return a listing title, falling back to a generic label if absent.

    Accepts any type for *value*; non-string scalars are converted via
    ``str()``.  Dicts are silently discarded in favour of the fallback (use
    :func:`.normalise_text` on dict sub-fields before calling this).

    Args:
        value: Raw title value from the API (string, dict, ``None``, …).
        fallback_id: Provider-local listing ID used to build the fallback
            label ``"Listing <fallback_id>"``.

    Returns:
        A non-empty title string.

    Examples::

        normalise_title("Bilocale centro", fallback_id="123")  # → "Bilocale centro"
        normalise_title(None, fallback_id="123")               # → "Listing 123"
        normalise_title("", fallback_id="456")                 # → "Listing 456"
    """
    fallback = f"Listing {fallback_id}"
    if value is None or isinstance(value, dict):
        return fallback
    result = normalise_text(str(value), fallback=fallback)
    return result


# ---------------------------------------------------------------------------
# Numeric parsing
# ---------------------------------------------------------------------------


def normalise_price(value: Any) -> int:
    """Coerce a raw price value to a non-negative integer.

    Handles ``None``, numeric strings (``"650"``), and plain integers.
    Returns ``0`` when the value is absent or cannot be parsed.

    Args:
        value: Raw price from the API (``int``, ``float``, ``str``, or
            ``None``).

    Returns:
        Non-negative integer price in EUR, or ``0``.

    Examples::

        normalise_price(650)     # → 650
        normalise_price("650")   # → 650
        normalise_price(None)    # → 0
        normalise_price("n/a")   # → 0
    """
    if value is None:
        return 0
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        logger.debug("normalise_price: cannot parse %r as int — defaulting to 0", value)
        return 0


def normalise_rooms(value: str | int | None) -> int | None:
    """Parse an Italian room-count value to an integer.

    Handles:

    * Plain integers (e.g. ``2``).
    * Numeric strings (e.g. ``"2"``).
    * Trailing ``"+"`` notation (e.g. ``"5+"`` → ``5``).

    Returns ``None`` when the input is absent or unparseable.

    Args:
        value: Raw room value from the API.

    Returns:
        Integer room count, or ``None``.

    Examples::

        normalise_rooms(3)     # → 3
        normalise_rooms("3")   # → 3
        normalise_rooms("5+")  # → 5
        normalise_rooms(None)  # → None
        normalise_rooms("n/a") # → None
    """
    if value is None:
        return None
    if isinstance(value, int):
        return value
    stripped = _TRAILING_PLUS_RE.sub("", str(value)).strip()
    try:
        return int(stripped)
    except ValueError:
        logger.debug("normalise_rooms: cannot parse %r — returning None", value)
        return None


def normalise_area_sqm(value: str | int | None) -> int | None:
    """Extract the floor area in square metres from a raw value.

    Handles:

    * Plain integers (e.g. ``60``).
    * Strings with units (e.g. ``"60 m²"``, ``"60mq"``).
    * Extracts the first sequence of digits found in the string.

    Returns ``None`` when the input is absent or contains no digits.

    Args:
        value: Raw surface/area value from the API.

    Returns:
        Floor area as an integer (square metres), or ``None``.

    Examples::

        normalise_area_sqm(60)       # → 60
        normalise_area_sqm("60 m²")  # → 60
        normalise_area_sqm("75mq")   # → 75
        normalise_area_sqm(None)     # → None
        normalise_area_sqm("n/a")    # → None
    """
    if value is None:
        return None
    if isinstance(value, int):
        return value
    match = _LEADING_INT_RE.search(str(value))
    if match is None:
        logger.debug("normalise_area_sqm: no digits in %r — returning None", value)
        return None
    return int(match.group())


# ---------------------------------------------------------------------------
# URL normalisation
# ---------------------------------------------------------------------------


def normalise_url(
    path_or_url: str | None,
    *,
    base_url: str,
    fallback_path: str,
) -> str:
    """Return a fully-qualified listing URL.

    If *path_or_url* is already an absolute URL (starts with ``"http"``), it
    is returned as-is.  If it is a relative path it is prefixed with
    *base_url*.  If it is absent, *base_url* is joined with *fallback_path*.

    Args:
        path_or_url: URL or path from the API.  May be ``None`` or empty.
        base_url: Base URL for the provider (e.g. ``"https://www.immobiliare.it"``).
            Must not include a trailing slash.
        fallback_path: Path to append to *base_url* when *path_or_url* is
            absent (e.g. ``"/annunci/12345678/"``).  Must start with ``"/"``.

    Returns:
        Absolute URL string.

    Examples::

        normalise_url("/annunci/123/", base_url="https://example.com", fallback_path="/")
        # → "https://example.com/annunci/123/"

        normalise_url("https://other.com/123", base_url="https://example.com", fallback_path="/")
        # → "https://other.com/123"

        normalise_url(None, base_url="https://example.com", fallback_path="/annunci/123/")
        # → "https://example.com/annunci/123/"
    """
    if not path_or_url:
        return base_url.rstrip("/") + fallback_path
    if path_or_url.startswith("http"):
        return path_or_url
    # Relative path — ensure exactly one "/" between base and path.
    return base_url.rstrip("/") + "/" + path_or_url.lstrip("/")


# ---------------------------------------------------------------------------
# Furnished status inference
# ---------------------------------------------------------------------------


def furnished_from_text(description: str | None) -> bool | None:
    """Infer furnished status from an Italian free-text description.

    Uses case-insensitive keyword matching on the lowercased description.
    Negative phrases (``"non arredato"``, ``"senza arredo"``) take precedence
    over positive ones.

    +-----------------------------------+------------------+
    | Keyword pattern                   | Result           |
    +===================================+==================+
    | ``"non arredato"``                | ``False``        |
    +-----------------------------------+------------------+
    | ``"senza arredo"``                | ``False``        |
    +-----------------------------------+------------------+
    | ``"arredat"``                     | ``True``         |
    +-----------------------------------+------------------+
    | No match                          | ``None``         |
    +-----------------------------------+------------------+

    Args:
        description: Raw listing description text.

    Returns:
        ``True`` if furnished, ``False`` if unfurnished, ``None`` if unknown.

    Examples::

        furnished_from_text("Appartamento non arredato")  # → False
        furnished_from_text("Arredato con mobili nuovi")  # → True
        furnished_from_text(None)                         # → None
        furnished_from_text("Grande terrazzo")            # → None
    """
    if not description:
        return None
    text = description.lower()
    if "non arredato" in text or "senza arredo" in text:
        return False
    if "arredat" in text:
        return True
    return None
