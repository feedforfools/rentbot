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

from rentbot.core.models import Listing

__all__ = [
    "canonical_id",
    "canonical_id_from_listing",
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
