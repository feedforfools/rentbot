"""Rentbot core domain models.

This module defines the canonical :class:`Listing` data model and all related
types shared across every provider, storage, filter, and notification layer.

All providers must normalise their raw data into a :class:`Listing` before
handing it to the deduplication / alerting pipeline.

Typical usage::

    from rentbot.core.models import Listing, ListingSource

    listing = Listing(
        id="12345678",
        source=ListingSource.IMMOBILIARE,
        title="Appartamento 3 locali, centro",
        price=650,
        rooms=3,
        url="https://www.immobiliare.it/annunci/12345678/",
    )
"""

from __future__ import annotations

import logging
from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, Field, field_validator

__all__ = [
    "ListingSource",
    "Listing",
]

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------


class ListingSource(StrEnum):
    """Canonical names for every supported listing source.

    Using :class:`enum.StrEnum` means the enum value serialises as a plain
    string (e.g. ``"immobiliare"``), which keeps DB storage and JSON output
    clean without extra serialisation plumbing.
    """

    IMMOBILIARE = "immobiliare"
    CASA = "casa"
    SUBITO = "subito"
    FACEBOOK = "facebook"
    IDEALISTA = "idealista"


# ---------------------------------------------------------------------------
# Core domain model
# ---------------------------------------------------------------------------


class Listing(BaseModel):
    """Normalised representation of a rental listing.

    Every provider maps its raw payload to this model.  Required fields are
    the minimum set needed to deduplicate, filter, and format an alert.
    Optional fields are populated when available and enrich the Telegram
    message.

    The ``id`` field holds the *provider-local* identifier (opaque string).
    The canonical database key ``"<source>:<id>"`` is computed by the storage
    layer — see :data:`rentbot.storage.canonical_id`.

    The model is **frozen** (immutable) so instances can be used as dict keys
    and safely passed between coroutines without accidental mutation.

    Attributes:
        id: Provider-local listing identifier (opaque, non-empty string).
        source: Which platform the listing came from.
        title: Listing headline / title text.
        price: Monthly rent in EUR (non-negative integer).
        rooms: Number of rooms (Italian *locali*). ``None`` if unparseable.
        area_sqm: Floor area in square metres. ``None`` if not stated.
        address: Full or partial street address. ``None`` if not stated.
        zone: Neighbourhood / district label as provided by the source.
            ``None`` if not stated.
        furnished: ``True`` = furnished, ``False`` = unfurnished,
            ``None`` = not stated.
        url: Canonical URL to the listing detail page.
        image_url: URL of the listing's primary image. ``None`` if absent.
        description: Full or partial listing description text. Defaults to
            an empty string so downstream code can always do string ops on it
            without a ``None`` guard.
        listing_date: UTC-aware (or naive) datetime when the listing was
            published, if available from the source. ``None`` otherwise.
    """

    model_config = {"frozen": True}

    id: str = Field(
        ...,
        min_length=1,
        description="Provider-local listing identifier.",
    )
    source: ListingSource = Field(
        ...,
        description="Source platform.",
    )
    title: str = Field(
        ...,
        min_length=1,
        description="Listing headline.",
    )
    price: int = Field(
        ...,
        ge=0,
        description="Monthly rent in EUR (non-negative integer).",
    )
    rooms: int | None = Field(
        None,
        ge=0,
        description="Number of rooms; None if unknown.",
    )
    area_sqm: int | None = Field(
        None,
        ge=0,
        description="Floor area in m²; None if unknown.",
    )
    address: str | None = Field(
        None,
        description="Street address; None if not stated.",
    )
    zone: str | None = Field(
        None,
        description="Neighbourhood or district label; None if not stated.",
    )
    furnished: bool | None = Field(
        None,
        description="Furnished status; None if not stated.",
    )
    url: str = Field(
        ...,
        min_length=1,
        description="Canonical URL to the listing detail page.",
    )
    image_url: str | None = Field(
        None,
        description="Primary listing image URL; None if absent.",
    )
    description: str = Field(
        default="",
        description="Listing body text. Defaults to empty string.",
    )
    listing_date: datetime | None = Field(
        None,
        description="Publication timestamp; None if unknown.",
    )

    # ------------------------------------------------------------------
    # Validators
    # ------------------------------------------------------------------

    @field_validator("url", mode="before")
    @classmethod
    def _url_non_empty(cls, v: object) -> object:
        """Reject blank URL strings early with a clear error message."""
        if isinstance(v, str) and not v.strip():
            raise ValueError("url must not be blank")
        return v

    @field_validator("image_url", mode="before")
    @classmethod
    def _image_url_blank_to_none(cls, v: object) -> object:
        """Coerce a blank image_url string to None rather than raising."""
        if isinstance(v, str) and not v.strip():
            return None
        return v

    @field_validator("address", "zone", mode="before")
    @classmethod
    def _strip_blank_strings(cls, v: object) -> object:
        """Coerce blank address/zone strings to None."""
        if isinstance(v, str) and not v.strip():
            return None
        return v
