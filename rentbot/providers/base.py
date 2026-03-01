"""Provider interface contract for all Rentbot listing sources.

Every provider — whether API-based (Immobiliare, Casa, Subito) or
browser-based (Facebook, Idealista) — must subclass :class:`BaseProvider`
and implement :meth:`fetch_latest`.

Design decisions
----------------
* **Abstract base class (ABC)** rather than a ``Protocol``: an ABC lets us
  enforce that subclasses call ``super().__init__()`` and share lifecycle
  helpers (``close``, ``__aenter__``/``__aexit__``) without duplication.
* **``source`` as a class variable**: providers declare their
  :class:`~rentbot.core.models.ListingSource` at the class level so the
  orchestrator and tests can inspect it without instantiating the class.
* **Async context manager built-in**: providers that hold a persistent
  HTTP client or browser context benefit from deterministic teardown.
  Providers that hold no resources can leave ``close()`` as-is (a no-op).

Typical usage::

    from rentbot.providers.base import BaseProvider
    from rentbot.core.models import Listing, ListingSource


    class MyProvider(BaseProvider):
        source = ListingSource.IMMOBILIARE

        async def fetch_latest(self) -> list[Listing]:
            ...

    async with MyProvider() as provider:
        listings = await provider.fetch_latest()
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from types import TracebackType
from typing import ClassVar

from rentbot.core.models import Listing, ListingSource

__all__ = ["BaseProvider"]

logger = logging.getLogger(__name__)


class BaseProvider(ABC):
    """Abstract base for all Rentbot listing providers.

    Subclasses **must** declare :attr:`source` as a class-level attribute and
    implement :meth:`fetch_latest`.  The async context manager protocol is
    provided for free; override :meth:`close` to release resources.

    Attributes:
        source: The :class:`~rentbot.core.models.ListingSource` enum member
            that identifies this provider.  Declared as a :data:`ClassVar` so
            the orchestrator can inspect it without construction.
    """

    source: ClassVar[ListingSource]

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def close(self) -> None:  # noqa: B027
        """Release any resources held by this provider.

        The default implementation is a no-op.  Override in subclasses that
        own an :class:`httpx.AsyncClient`, Playwright browser context, or
        similar long-lived resource.
        """

    async def __aenter__(self) -> BaseProvider:
        """Enter the async context manager.  Returns ``self``."""
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Exit the async context manager by delegating to :meth:`close`."""
        await self.close()

    # ------------------------------------------------------------------
    # Core contract
    # ------------------------------------------------------------------

    @abstractmethod
    async def fetch_latest(self) -> list[Listing]:
        """Fetch the most recent listings from this provider.

        Implementations should:

        * Return a (possibly empty) list of **normalised**
          :class:`~rentbot.core.models.Listing` objects.
        * **Not** perform deduplication — that is the storage layer's job.
        * Handle recoverable errors internally (log and return ``[]``) so a
          single provider failure never aborts the full polling cycle.
        * Respect async best-practices: use ``await`` for every I/O call;
          avoid blocking the event loop.

        Returns:
            A list of :class:`~rentbot.core.models.Listing` objects,
            possibly empty.

        Raises:
            :class:`~rentbot.core.exceptions.ProviderError`: for
            unrecoverable errors that the caller should surface.
        """
