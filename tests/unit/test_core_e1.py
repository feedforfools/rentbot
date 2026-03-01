"""Unit tests for Epic 1 deliverables.

Covers:
- :class:`~rentbot.core.models.Listing` field validation and coercions.
- :class:`~rentbot.core.criteria.FilterCriteria` matching helpers.
- :func:`~rentbot.core.ids.canonical_id` and
  :func:`~rentbot.core.ids.canonical_id_from_listing`.
- :class:`~rentbot.core.run_context.RunContext` mode logic.
- :class:`~rentbot.storage.repository.ListingRepository` CRUD operations.
- :class:`~rentbot.core.settings.Settings` loading, validation, and helpers.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from pathlib import Path

import aiosqlite
import pytest
from pydantic import ValidationError

from rentbot.core.criteria import FilterCriteria
from rentbot.core.exceptions import ListingAlreadyExistsError
from rentbot.core.ids import canonical_id, canonical_id_from_listing
from rentbot.core.models import Listing, ListingSource
from rentbot.core.run_context import RunContext
from rentbot.core.settings import Settings
from rentbot.storage.database import create_schema, open_db
from rentbot.storage.repository import ListingRepository

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Factories / helpers
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
    image_url: str | None = "https://img.example.com/img.jpg",
    description: str = "Luminoso appartamento ristrutturato.",
    listing_date: datetime | None = None,
) -> Listing:
    """Return a valid :class:`Listing` with overridable defaults."""
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


async def _open_memory_db() -> aiosqlite.Connection:
    """Open an in-memory SQLite database with the Rentbot schema applied."""
    conn: aiosqlite.Connection = await aiosqlite.connect(":memory:")
    conn.row_factory = aiosqlite.Row
    # Pragma: WAL is a no-op for :memory: but must not raise.
    await conn.execute("PRAGMA foreign_keys=ON")
    await create_schema(conn)
    return conn


# ===========================================================================
# Listing model validation
# ===========================================================================


class TestListingValidation:
    """Tests for :class:`~rentbot.core.models.Listing`."""

    def test_minimal_valid_listing(self) -> None:
        """Only required fields — optional fields default to None / empty."""
        listing = Listing(
            id="1",
            source=ListingSource.SUBITO,
            title="Monolocale",
            price=400,
            url="https://subito.it/annunci/1/",
        )
        assert listing.id == "1"
        assert listing.source == ListingSource.SUBITO
        assert listing.price == 400
        assert listing.rooms is None
        assert listing.area_sqm is None
        assert listing.address is None
        assert listing.zone is None
        assert listing.furnished is None
        assert listing.image_url is None
        assert listing.description == ""
        assert listing.listing_date is None

    def test_full_listing_round_trips(self) -> None:
        """All fields survive construction unchanged."""
        dt = datetime(2026, 2, 28, 12, 0, 0, tzinfo=UTC)
        listing = _make_listing(listing_date=dt, rooms=3, area_sqm=80)
        assert listing.rooms == 3
        assert listing.area_sqm == 80
        assert listing.listing_date == dt
        assert listing.furnished is True

    def test_listing_is_frozen(self) -> None:
        """Listing instances are immutable (frozen model)."""
        listing = _make_listing()
        with pytest.raises(Exception):  # noqa: B017
            listing.price = 999  # type: ignore[misc]

    def test_blank_id_rejected(self) -> None:
        with pytest.raises(ValidationError, match="string_too_short|at least 1"):
            Listing(
                id="",
                source=ListingSource.CASA,
                title="Test",
                price=500,
                url="https://casa.it/1/",
            )

    def test_blank_title_rejected(self) -> None:
        with pytest.raises(ValidationError, match="string_too_short|at least 1"):
            Listing(
                id="1",
                source=ListingSource.CASA,
                title="",
                price=500,
                url="https://casa.it/1/",
            )

    def test_blank_url_rejected(self) -> None:
        with pytest.raises(ValidationError, match="blank"):
            Listing(
                id="1",
                source=ListingSource.CASA,
                title="Test",
                price=500,
                url="   ",
            )

    def test_negative_price_rejected(self) -> None:
        with pytest.raises(ValidationError, match="greater than or equal"):
            _make_listing(price=-1)

    def test_zero_price_accepted(self) -> None:
        """Price of 0 is valid (free / negotiable)."""
        listing = _make_listing(price=0)
        assert listing.price == 0

    def test_negative_rooms_rejected(self) -> None:
        with pytest.raises(ValidationError, match="greater than or equal"):
            _make_listing(rooms=-1)

    def test_negative_area_rejected(self) -> None:
        with pytest.raises(ValidationError, match="greater than or equal"):
            _make_listing(area_sqm=-5)

    def test_blank_image_url_coerced_to_none(self) -> None:
        """A blank image_url string becomes None."""
        listing = _make_listing(image_url="   ")
        assert listing.image_url is None

    def test_blank_address_coerced_to_none(self) -> None:
        listing = _make_listing(address="")
        assert listing.address is None

    def test_blank_zone_coerced_to_none(self) -> None:
        listing = _make_listing(zone="  ")
        assert listing.zone is None

    def test_all_listing_sources_accepted(self) -> None:
        """Every value in ListingSource can be used."""
        for src in ListingSource:
            listing = _make_listing(source=src)
            assert listing.source == src

    def test_listing_source_str_values(self) -> None:
        """ListingSource serialises as lowercase strings (StrEnum)."""
        assert str(ListingSource.IMMOBILIARE) == "immobiliare"
        assert str(ListingSource.CASA) == "casa"
        assert str(ListingSource.SUBITO) == "subito"
        assert str(ListingSource.FACEBOOK) == "facebook"
        assert str(ListingSource.IDEALISTA) == "idealista"


# ===========================================================================
# FilterCriteria matching
# ===========================================================================


class TestFilterCriteriaMatching:
    """Tests for :class:`~rentbot.core.criteria.FilterCriteria`."""

    def test_empty_criteria_matches_everything(self) -> None:
        criteria = FilterCriteria()
        listing = _make_listing(price=999, rooms=5, area_sqm=200)
        passed, reason = criteria.matches_listing(listing)
        assert passed, reason

    def test_price_max_blocks_expensive_listing(self) -> None:
        criteria = FilterCriteria(price_max=600)
        passed, reason = criteria.matches_listing(_make_listing(price=601))
        assert not passed
        assert "price" in reason.lower()

    def test_price_max_passes_at_boundary(self) -> None:
        criteria = FilterCriteria(price_max=600)
        passed, _ = criteria.matches_listing(_make_listing(price=600))
        assert passed

    def test_price_min_blocks_cheap_listing(self) -> None:
        criteria = FilterCriteria(price_min=400)
        passed, reason = criteria.matches_listing(_make_listing(price=300))
        assert not passed
        assert "price" in reason.lower()

    def test_rooms_min_blocks_too_few_rooms(self) -> None:
        criteria = FilterCriteria(rooms_min=3)
        passed, reason = criteria.matches_listing(_make_listing(rooms=2))
        assert not passed
        assert "room" in reason.lower()

    def test_rooms_max_blocks_too_many_rooms(self) -> None:
        criteria = FilterCriteria(rooms_max=2)
        passed, reason = criteria.matches_listing(_make_listing(rooms=3))
        assert not passed

    def test_rooms_unknown_not_blocked_by_default(self) -> None:
        """A listing with rooms=None should still pass room constraints."""
        criteria = FilterCriteria(rooms_min=2, rooms_max=4)
        passed, _ = criteria.matches_listing(_make_listing(rooms=None))
        assert passed

    def test_keyword_blocklist_blocks_on_title(self) -> None:
        criteria = FilterCriteria(keyword_blocklist=["stanza singola"])
        listing = _make_listing(title="Affittasi stanza singola in centro")
        passed, reason = criteria.matches_listing(listing)
        assert not passed
        assert "stanza singola" in reason.lower()

    def test_keyword_blocklist_blocks_on_description(self) -> None:
        criteria = FilterCriteria(keyword_blocklist=["posto letto"])
        listing = _make_listing(description="posto letto disponibile subito")
        passed, reason = criteria.matches_listing(listing)
        assert not passed

    def test_keyword_blocklist_case_insensitive(self) -> None:
        criteria = FilterCriteria(keyword_blocklist=["Garage"])
        listing = _make_listing(title="GARAGE doppio in vendita")
        passed, _ = criteria.matches_listing(listing)
        assert not passed

    def test_zone_allowlist_passes_matching_zone(self) -> None:
        criteria = FilterCriteria(zones_include=["Centro", "Villanova"])
        passed, _ = criteria.matches_listing(_make_listing(zone="Centro storico"))
        assert passed  # substring match

    def test_zone_allowlist_blocks_unknown_zone(self) -> None:
        criteria = FilterCriteria(zones_include=["Centro"])
        passed, reason = criteria.matches_listing(_make_listing(zone="Periferia"))
        assert not passed

    def test_inverted_price_range_rejected(self) -> None:
        with pytest.raises(ValidationError, match="price_min.*price_max|price_max.*price_min"):
            FilterCriteria(price_min=700, price_max=600)

    def test_inverted_rooms_range_rejected(self) -> None:
        with pytest.raises(ValidationError, match="rooms_min.*rooms_max|rooms_max.*rooms_min"):
            FilterCriteria(rooms_min=4, rooms_max=2)


# ===========================================================================
# Canonical ID strategy
# ===========================================================================


class TestCanonicalId:
    """Tests for :func:`canonical_id` and :func:`canonical_id_from_listing`."""

    def test_canonical_id_format(self) -> None:
        assert canonical_id("immobiliare", "12345") == "immobiliare:12345"

    def test_canonical_id_all_sources(self) -> None:
        for src in ListingSource:
            cid = canonical_id(str(src), "ABC")
            assert cid == f"{src}:ABC"

    def test_canonical_id_from_listing(self) -> None:
        listing = _make_listing(id="99999", source=ListingSource.CASA)
        assert canonical_id_from_listing(listing) == "casa:99999"

    def test_canonical_id_separator_colon(self) -> None:
        """The separator is always a colon."""
        cid = canonical_id("subito", "x:y")  # colon in provider id
        assert cid.startswith("subito:")

    def test_canonical_id_stable(self) -> None:
        """Same inputs always yield same output."""
        assert canonical_id("immobiliare", "1") == canonical_id("immobiliare", "1")

    def test_canonical_id_cross_provider_differs(self) -> None:
        """Two providers with the same local ID produce different canonical IDs."""
        assert canonical_id("casa", "1") != canonical_id("subito", "1")


# ===========================================================================
# RunContext
# ===========================================================================


class TestRunContext:
    """Tests for :class:`~rentbot.core.run_context.RunContext`."""

    def test_live_mode_should_notify(self) -> None:
        ctx = RunContext()
        assert ctx.should_notify is True
        assert ctx.mode_label == "live"

    def test_seed_mode_no_notify(self) -> None:
        ctx = RunContext(seed=True)
        assert ctx.should_notify is False
        assert ctx.mode_label == "seed"

    def test_dry_run_no_notify(self) -> None:
        ctx = RunContext(dry_run=True)
        assert ctx.should_notify is False
        assert ctx.mode_label == "dry-run"

    def test_both_flags_seed_wins(self) -> None:
        ctx = RunContext(seed=True, dry_run=True)
        assert ctx.should_notify is False
        assert ctx.mode_label == "seed"

    def test_run_context_is_immutable(self) -> None:
        ctx = RunContext(seed=False)
        with pytest.raises(Exception):  # noqa: B017
            ctx.seed = True  # type: ignore[misc]

    def test_str_representation_contains_mode(self) -> None:
        ctx = RunContext(dry_run=True)
        s = str(ctx)
        assert "dry-run" in s
        assert "should_notify=False" in s


# ===========================================================================
# ListingRepository CRUD
# ===========================================================================


class TestListingRepository:
    """Async tests for :class:`~rentbot.storage.repository.ListingRepository`."""

    async def test_exists_returns_false_for_unknown(self) -> None:
        conn = await _open_memory_db()
        try:
            repo = ListingRepository(conn)
            assert await repo.exists("immobiliare:99999") is False
        finally:
            await conn.close()

    async def test_insert_returns_canonical_id(self) -> None:
        conn = await _open_memory_db()
        try:
            repo = ListingRepository(conn)
            listing = _make_listing(id="1", source=ListingSource.IMMOBILIARE)
            cid = await repo.insert(listing)
            assert cid == "immobiliare:1"
        finally:
            await conn.close()

    async def test_insert_then_exists_true(self) -> None:
        conn = await _open_memory_db()
        try:
            repo = ListingRepository(conn)
            listing = _make_listing(id="2")
            cid = await repo.insert(listing)
            assert await repo.exists(cid) is True
        finally:
            await conn.close()

    async def test_insert_duplicate_raises(self) -> None:
        conn = await _open_memory_db()
        try:
            repo = ListingRepository(conn)
            listing = _make_listing(id="3")
            await repo.insert(listing)
            with pytest.raises(ListingAlreadyExistsError) as exc_info:
                await repo.insert(listing)
            assert "immobiliare:3" in str(exc_info.value)
        finally:
            await conn.close()

    async def test_bulk_insert_new_listings(self) -> None:
        conn = await _open_memory_db()
        try:
            repo = ListingRepository(conn)
            listings = [_make_listing(id=str(i)) for i in range(5)]
            inserted = await repo.bulk_insert(listings)
            assert len(inserted) == 5
            assert all(cid.startswith("immobiliare:") for cid in inserted)
        finally:
            await conn.close()

    async def test_bulk_insert_skips_existing(self) -> None:
        conn = await _open_memory_db()
        try:
            repo = ListingRepository(conn)
            l1 = _make_listing(id="10")
            l2 = _make_listing(id="11")
            # Pre-insert l1
            await repo.insert(l1)
            # Bulk-insert both — only l2 should be new
            inserted = await repo.bulk_insert([l1, l2])
            assert len(inserted) == 1
            assert "immobiliare:11" in inserted
        finally:
            await conn.close()

    async def test_bulk_insert_empty_returns_empty(self) -> None:
        conn = await _open_memory_db()
        try:
            repo = ListingRepository(conn)
            result = await repo.bulk_insert([])
            assert result == []
        finally:
            await conn.close()

    async def test_bulk_insert_all_existing_returns_empty(self) -> None:
        conn = await _open_memory_db()
        try:
            repo = ListingRepository(conn)
            listings = [_make_listing(id=str(i)) for i in range(3)]
            await repo.bulk_insert(listings)
            # Second call with same batch
            inserted = await repo.bulk_insert(listings)
            assert inserted == []
        finally:
            await conn.close()

    async def test_mark_notified(self) -> None:
        conn = await _open_memory_db()
        try:
            repo = ListingRepository(conn)
            listing = _make_listing(id="20")
            cid = await repo.insert(listing)
            await repo.mark_notified(cid)
            cursor = await conn.execute("SELECT notified FROM seen_listings WHERE id = ?", (cid,))
            row = await cursor.fetchone()
            assert row is not None
            assert row["notified"] == 1
        finally:
            await conn.close()

    async def test_notified_defaults_to_zero(self) -> None:
        conn = await _open_memory_db()
        try:
            repo = ListingRepository(conn)
            listing = _make_listing(id="21")
            cid = await repo.insert(listing)
            cursor = await conn.execute("SELECT notified FROM seen_listings WHERE id = ?", (cid,))
            row = await cursor.fetchone()
            assert row is not None
            assert row["notified"] == 0
        finally:
            await conn.close()

    async def test_update_filter_result(self) -> None:
        conn = await _open_memory_db()
        try:
            repo = ListingRepository(conn)
            listing = _make_listing(id="30")
            cid = await repo.insert(listing)
            await repo.update_filter_result(cid, "pass")
            cursor = await conn.execute(
                "SELECT filter_result FROM seen_listings WHERE id = ?", (cid,)
            )
            row = await cursor.fetchone()
            assert row is not None
            assert row["filter_result"] == "pass"
        finally:
            await conn.close()

    async def test_update_filter_result_block(self) -> None:
        conn = await _open_memory_db()
        try:
            repo = ListingRepository(conn)
            listing = _make_listing(id="31")
            cid = await repo.insert(listing)
            await repo.update_filter_result(cid, "block:price_too_high")
            cursor = await conn.execute(
                "SELECT filter_result FROM seen_listings WHERE id = ?", (cid,)
            )
            row = await cursor.fetchone()
            assert row is not None
            assert row["filter_result"] == "block:price_too_high"
        finally:
            await conn.close()

    async def test_insert_stores_raw_json(self) -> None:
        conn = await _open_memory_db()
        try:
            repo = ListingRepository(conn)
            listing = _make_listing(id="40", title="JSON roundtrip test")
            cid = await repo.insert(listing)
            cursor = await conn.execute("SELECT raw_json FROM seen_listings WHERE id = ?", (cid,))
            row = await cursor.fetchone()
            assert row is not None
            assert "JSON roundtrip test" in row["raw_json"]
        finally:
            await conn.close()

    async def test_insert_with_filter_result(self) -> None:
        conn = await _open_memory_db()
        try:
            repo = ListingRepository(conn)
            listing = _make_listing(id="50")
            cid = await repo.insert(listing, filter_result="pass")
            cursor = await conn.execute(
                "SELECT filter_result FROM seen_listings WHERE id = ?", (cid,)
            )
            row = await cursor.fetchone()
            assert row is not None
            assert row["filter_result"] == "pass"
        finally:
            await conn.close()

    async def test_cross_provider_no_collision(self) -> None:
        """Same local ID from two different providers must not collide."""
        conn = await _open_memory_db()
        try:
            repo = ListingRepository(conn)
            l_immo = _make_listing(id="999", source=ListingSource.IMMOBILIARE)
            l_casa = _make_listing(id="999", source=ListingSource.CASA, url="https://casa.it/999/")
            cid_immo = await repo.insert(l_immo)
            cid_casa = await repo.insert(l_casa)
            assert cid_immo != cid_casa
            assert await repo.exists(cid_immo) is True
            assert await repo.exists(cid_casa) is True
        finally:
            await conn.close()

    async def test_open_db_creates_file_and_schema(self, tmp_path: Path) -> None:
        """``open_db`` creates the DB file with schema when path doesn't exist."""
        db_path = tmp_path / "subdir" / "test.db"
        conn = await open_db(db_path)
        try:
            assert db_path.exists()
            cursor = await conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='seen_listings'"
            )
            row = await cursor.fetchone()
            assert row is not None
        finally:
            await conn.close()


# ===========================================================================
# Settings loading
# ===========================================================================


class TestSettings:
    """Tests for :class:`~rentbot.core.settings.Settings`."""

    def test_defaults_load_without_env(self, clean_env: None) -> None:
        """Settings with no env vars use documented defaults."""
        s = Settings()
        assert s.telegram_bot_token == ""
        assert s.search_city == "Pordenone"
        assert s.search_max_price == 800
        assert s.search_min_rooms == 2
        assert s.search_max_rooms == 0
        assert s.log_level == "INFO"
        assert s.log_format == "text"

    def test_telegram_configured_false_by_default(self, clean_env: None) -> None:
        s = Settings()
        assert s.telegram_configured is False

    def test_telegram_configured_requires_both_fields(
        self, monkeypatch: pytest.MonkeyPatch, clean_env: None
    ) -> None:
        monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "tok123")
        monkeypatch.setenv("TELEGRAM_CHAT_ID", "456")
        s = Settings()
        assert s.telegram_configured is True

    def test_telegram_configured_partial_is_false(
        self, monkeypatch: pytest.MonkeyPatch, clean_env: None
    ) -> None:
        monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "tok123")
        s = Settings()
        assert s.telegram_configured is False

    def test_llm_configured_openai(self, monkeypatch: pytest.MonkeyPatch, clean_env: None) -> None:
        monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
        s = Settings()
        assert s.llm_configured is True

    def test_llm_configured_anthropic(
        self, monkeypatch: pytest.MonkeyPatch, clean_env: None
    ) -> None:
        monkeypatch.setenv("LLM_PROVIDER", "anthropic")
        monkeypatch.setenv("ANTHROPIC_API_KEY", "ant-test")
        s = Settings()
        assert s.llm_configured is True

    def test_llm_configured_false_without_key(self, clean_env: None) -> None:
        s = Settings()
        assert s.llm_configured is False

    def test_invalid_llm_provider_raises(
        self, monkeypatch: pytest.MonkeyPatch, clean_env: None
    ) -> None:
        monkeypatch.setenv("LLM_PROVIDER", "gemini")
        with pytest.raises(ValidationError, match="llm_provider"):
            Settings()

    def test_invalid_log_level_raises(
        self, monkeypatch: pytest.MonkeyPatch, clean_env: None
    ) -> None:
        monkeypatch.setenv("LOG_LEVEL", "VERBOSE")
        with pytest.raises(ValidationError, match="log_level"):
            Settings()

    def test_invalid_log_format_raises(
        self, monkeypatch: pytest.MonkeyPatch, clean_env: None
    ) -> None:
        monkeypatch.setenv("LOG_FORMAT", "xml")
        with pytest.raises(ValidationError, match="log_format"):
            Settings()

    def test_api_poll_interval_inverted_raises(
        self, monkeypatch: pytest.MonkeyPatch, clean_env: None
    ) -> None:
        monkeypatch.setenv("POLL_INTERVAL_API_MIN", "600")
        monkeypatch.setenv("POLL_INTERVAL_API_MAX", "300")
        with pytest.raises(ValidationError, match="poll_interval"):
            Settings()

    def test_to_filter_criteria_zero_becomes_none(self, clean_env: None) -> None:
        """0-valued search limits are mapped to None in FilterCriteria."""
        s = Settings()
        # defaults: min_price=0, min_area=0, max_rooms=0
        criteria = s.to_filter_criteria()
        assert criteria.price_min is None  # 0 → None
        assert criteria.area_sqm_min is None
        assert criteria.rooms_max is None

    def test_to_filter_criteria_nonzero_values_propagate(
        self, monkeypatch: pytest.MonkeyPatch, clean_env: None
    ) -> None:
        monkeypatch.setenv("SEARCH_MAX_PRICE", "700")
        monkeypatch.setenv("SEARCH_MIN_ROOMS", "2")
        monkeypatch.setenv("SEARCH_MAX_ROOMS", "3")
        s = Settings()
        criteria = s.to_filter_criteria()
        assert criteria.price_max == 700
        assert criteria.rooms_min == 2
        assert criteria.rooms_max == 3

    def test_to_filter_criteria_keyword_blocklist(self, clean_env: None) -> None:
        """Default blocklist flows into FilterCriteria."""
        s = Settings()
        criteria = s.to_filter_criteria()
        blocklist = criteria.keyword_blocklist
        assert "garage" in blocklist
        assert "posto auto" in blocklist

    def test_facebook_configured_false_by_default(self, clean_env: None) -> None:
        s = Settings()
        assert s.facebook_configured is False

    def test_facebook_configured_with_session_and_group(
        self, monkeypatch: pytest.MonkeyPatch, clean_env: None
    ) -> None:
        monkeypatch.setenv("FACEBOOK_SESSION_PATH", "/tmp/session.json")
        monkeypatch.setenv("FACEBOOK_GROUP_IDS", "group1,group2")
        s = Settings()
        assert s.facebook_configured is True
        assert s.facebook_group_ids == ["group1", "group2"]

    def test_database_path_resolved_is_absolute(self, clean_env: None) -> None:
        s = Settings()
        assert s.database_path_resolved.is_absolute()

    def test_log_level_normalised_to_uppercase(
        self, monkeypatch: pytest.MonkeyPatch, clean_env: None
    ) -> None:
        monkeypatch.setenv("LOG_LEVEL", "debug")
        s = Settings()
        assert s.log_level == "DEBUG"
