"""Canonical listing ID strategy for Rentbot.

This module defines and documents the **id contract** shared by every layer of
the application.  All providers, the storage layer, and the notification layer
must follow these rules:

Provider contract
-----------------
Every provider sets ``listing.id`` to the **provider-local identifier** — the
raw, opaque string or number issued by the source platform (e.g. ``"12345678"``
from Immobiliare, or ``"abc-def-ghi"`` from Subito).  Providers must **not**
add any prefix themselves.

Storage contract
----------------
The storage (repository) layer always computes the **canonical key** by calling
:func:`canonical_id` before any DB read or write.  The canonical key takes the
form ``"<source>:<provider_id>"``, e.g. ``"immobiliare:12345678"``.  This
guarantees uniqueness across all providers even when two platforms issue the
same numeric ID.

The canonical key is the PRIMARY KEY in ``seen_listings``.  It is never
exposed back to the ``Listing`` model — the model retains the provider-local
``id`` because that is what URLs and provider-specific follow-up calls need.

Summary
-------
+-----------+---------------------------------+---------------------------------+
| Layer     | What it stores                  | Example                         |
+===========+=================================+=================================+
| Provider  | ``listing.id = "12345678"``     | Raw API/HTML id                 |
+-----------+---------------------------------+---------------------------------+
| Storage   | ``canonical_id(source, id)``    | ``"immobiliare:12345678"``      |
+-----------+---------------------------------+---------------------------------+

Typical usage::

    from rentbot.core.ids import canonical_id, canonical_id_from_listing

    # In the storage layer:
    cid = canonical_id("immobiliare", "12345678")
    cid = canonical_id_from_listing(listing)
"""

from __future__ import annotations

import logging
import re

from rentbot.core.models import Listing

__all__ = [
    "canonical_id",
    "canonical_id_from_listing",
    "content_fingerprint",
    "desc_fingerprint",
]

logger = logging.getLogger(__name__)

#: Separator character used between source name and provider-local ID.
CANONICAL_ID_SEPARATOR: str = ":"


def canonical_id(source: str, provider_id: str) -> str:
    """Return the canonical database key for a listing.

    The canonical key is ``"<source>:<provider_id>"``, e.g.
    ``"immobiliare:12345678"``.  This is used as the PRIMARY KEY in
    ``seen_listings`` so rows are unique across all providers.

    Args:
        source: Provider name string (e.g. ``"immobiliare"``).  Usually the
            :attr:`~rentbot.core.models.ListingSource` enum value, which
            serialises as a plain lowercase string.
        provider_id: Provider-local listing identifier as returned by the
            source platform.

    Returns:
        A ``"<source>:<provider_id>"`` string such as
        ``"immobiliare:12345678"``.

    Example::

        assert canonical_id("immobiliare", "12345678") == "immobiliare:12345678"
        assert canonical_id("subito", "abc") == "subito:abc"
    """
    return f"{source}{CANONICAL_ID_SEPARATOR}{provider_id}"


def canonical_id_from_listing(listing: Listing) -> str:
    """Compute the canonical database key from a :class:`~rentbot.core.models.Listing`.

    Convenience wrapper around :func:`canonical_id` for the common case where
    you have a fully constructed listing and want its storage key.

    Args:
        listing: A normalised :class:`~rentbot.core.models.Listing`.

    Returns:
        The canonical ``"<source>:<id>"`` key, e.g. ``"casa:99999"``.

    Example::

        listing = Listing(
            id="99999",
            source=ListingSource.CASA,
            title="Bilocale centro",
            price=600,
            url="https://casa.it/annunci/99999/",
        )
        assert canonical_id_from_listing(listing) == "casa:99999"
    """
    return canonical_id(str(listing.source), listing.id)


# ---------------------------------------------------------------------------
# Cross-platform content fingerprint
# ---------------------------------------------------------------------------

#: Regex that collapses commas, extra whitespace, and common noise.
_ADDR_NOISE_RE: re.Pattern[str] = re.compile(r"[,./]+")
_WHITESPACE_RE: re.Pattern[str] = re.compile(r"\s+")
#: Immobiliare.it emits this literal token when a civic number is unknown.
_NO_NUMBER_RE: re.Pattern[str] = re.compile(r"\bno\s+number\b", re.IGNORECASE)
#: Trailing standalone integer — civic number that may be present on one
#: platform and absent on another (e.g. "via vallona 33" vs "via vallona").
_TRAILING_NUMBER_RE: re.Pattern[str] = re.compile(r"\s+\d+$")


def _normalise_address(address: str) -> str:
    """Normalise an address for cross-platform comparison.

    Lowercases, removes commas/dots/slashes, collapses whitespace.
    Also strips:
    - the literal ``"No Number"`` placeholder emitted by Immobiliare.it when
      the civic number is unknown.
    - a trailing standalone integer (civic number) so that ``"via vallona 33"``
      and ``"via vallona"`` resolve to the same fingerprint.

    Handles differences like ``"via Camucina, 18"`` vs ``"Via Camucina 18"``.
    """
    text = address.lower()
    text = _NO_NUMBER_RE.sub(" ", text)
    text = _ADDR_NOISE_RE.sub(" ", text)
    text = _WHITESPACE_RE.sub(" ", text).strip()
    text = _TRAILING_NUMBER_RE.sub("", text).strip()
    return text


def content_fingerprint(listing: Listing) -> str | None:
    """Compute a cross-platform content fingerprint for duplicate detection.

    Returns a string of the form ``"<normalised_address>|<price>|<area>"``
    that is identical for listings describing the same property regardless
    of which provider they come from.

    Returns ``None`` when any of the three required fields (address, price,
    area_sqm) is missing or zero — without all three we cannot reliably
    determine sameness.

    Args:
        listing: A normalised :class:`~rentbot.core.models.Listing`.

    Returns:
        Fingerprint string, or ``None`` if insufficient data.

    Examples::

        # Same property, different providers:
        fp1 = content_fingerprint(immo_listing)   # "via camucina 18|750|85"
        fp2 = content_fingerprint(casa_listing)    # "via camucina 18|750|85"
        assert fp1 == fp2

        # Missing address → None
        fp3 = content_fingerprint(no_address_listing)  # None
    """
    if not listing.address or not listing.price or not listing.area_sqm:
        return None

    norm_addr = _normalise_address(listing.address)
    if not norm_addr:
        return None

    return f"{norm_addr}|{listing.price}|{listing.area_sqm}"


# ---------------------------------------------------------------------------
# Description fingerprint
# ---------------------------------------------------------------------------

#: Collapse any run of whitespace (newlines, tabs, spaces) to a single space.
_DESC_WHITESPACE_RE: re.Pattern[str] = re.compile(r"\s+")

#: Minimum number of normalised characters required before we trust the
#: fingerprint.  Short / boilerplate descriptions (e.g. "Appartamento in
#: affitto a Pordenone") are far too generic to use as a dedup signal.
_DESC_MIN_LEN: int = 150

#: How many leading normalised characters form the fingerprint key stored in
#: the DB.  Long enough to be unique, short enough to index efficiently.
_DESC_FP_CHARS: int = 200


def desc_fingerprint(listing: Listing) -> str | None:
    """Compute a description-based fingerprint for cross-platform deduplication.

    Agents frequently copy-paste the full listing description across platforms.
    This fingerprint captures the first :data:`_DESC_FP_CHARS` normalised
    characters of the description so that two listings with identical (or
    near-identical) descriptions are recognised as the same property.

    Returns ``None`` when:

    * ``listing.description`` is empty / ``None``.
    * The normalised description is shorter than :data:`_DESC_MIN_LEN`
      characters (too generic to be distinctive).

    Args:
        listing: A normalised :class:`~rentbot.core.models.Listing`.

    Returns:
        Fingerprint string of at most :data:`_DESC_FP_CHARS` characters, or
        ``None`` if the description is absent or too short.

    Examples::

        # Same description on two platforms → same fingerprint:
        fp1 = desc_fingerprint(subito_listing)      # "rif lsv1 nel cuore del ..."
        fp2 = desc_fingerprint(immobiliare_listing)  # "rif lsv1 nel cuore del ..."
        assert fp1 == fp2

        # Empty description → None
        fp3 = desc_fingerprint(no_desc_listing)  # None
    """
    if not listing.description:
        return None
    text = listing.description.lower()
    text = _DESC_WHITESPACE_RE.sub(" ", text).strip()
    if len(text) < _DESC_MIN_LEN:
        return None
    return text[:_DESC_FP_CHARS]
