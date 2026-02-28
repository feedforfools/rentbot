"""High-level alert entry point for Rentbot.

Provides :class:`Notifier`, the single object every pipeline stage calls to
deliver a listing alert.  It owns the decision of *whether* to send (based on
:class:`~rentbot.core.run_context.RunContext`) and delegates to:

* :func:`~rentbot.notifiers.formatter.format_listing` — message formatting.
* :class:`~rentbot.notifiers.telegram.TelegramClient` — transport.

The class is intentionally thin: it applies no filter logic (that lives in
the filter layer) and holds no listing state.  Each call to
:meth:`send_alert` is independent and idempotent from the Notifier's
perspective.

Typical usage::

    from rentbot.notifiers.notifier import Notifier
    from rentbot.notifiers.telegram import TelegramClient
    from rentbot.core.run_context import RunContext

    ctx = RunContext(seed=False, dry_run=False)

    async with TelegramClient(token=settings.telegram_bot_token,
                               chat_id=settings.telegram_chat_id) as client:
        notifier = Notifier(client=client, ctx=ctx)
        sent = await notifier.send_alert(listing)
"""

from __future__ import annotations

import logging

from rentbot.core.models import Listing
from rentbot.core.run_context import RunContext
from rentbot.notifiers.formatter import format_listing
from rentbot.notifiers.telegram import TelegramClient

__all__ = ["Notifier"]

logger = logging.getLogger(__name__)


class Notifier:
    """Orchestrates formatting and delivery of a single listing alert.

    Respects the :class:`~rentbot.core.run_context.RunContext` operating
    mode:

    * **seed mode** (``ctx.seed=True``) — skips all notification output
      silently.  The DB insertion still happens in the caller; the Notifier
      simply does nothing.
    * **dry-run mode** (``ctx.dry_run=True``) — formats the message and
      logs it at ``INFO`` level instead of sending it to Telegram.  Useful
      for local development and CI.
    * **live mode** — formats and sends via Telegram.

    Args:
        client: Configured and open :class:`TelegramClient` instance.
            The Notifier does **not** manage the client's lifecycle
            (``open`` / ``close``); the caller is responsible for that.
        ctx: Runtime operating mode flags.
    """

    def __init__(self, client: TelegramClient, ctx: RunContext) -> None:
        self._client = client
        self._ctx = ctx

    async def send_alert(self, listing: Listing) -> bool:
        """Format and deliver an alert for *listing*.

        Args:
            listing: Normalised listing that has already passed the filter
                and dedup layers.

        Returns:
            ``True`` if a message was sent (live) or logged (dry-run).
            ``False`` if the current mode suppresses output (seed mode).

        Raises:
            TelegramError: Propagated from the transport layer when a live
                send fails after all retries are exhausted.  The caller
                should catch this and decide whether to mark the listing as
                un-notified for a future retry.
        """
        cid = f"{listing.source}:{listing.id}"

        # ------------------------------------------------------------------
        # Seed mode — populate DB only, no notifications at all.
        # ------------------------------------------------------------------
        if self._ctx.seed:
            logger.debug(
                "[seed] Skipping notification for %s (%s)",
                cid,
                listing.title[:60],
            )
            return False

        # ------------------------------------------------------------------
        # Format the message (both live and dry-run paths need it).
        # ------------------------------------------------------------------
        text = format_listing(listing)

        # ------------------------------------------------------------------
        # Dry-run mode — log the payload, do not POST to Telegram.
        # ------------------------------------------------------------------
        if self._ctx.dry_run:
            logger.info(
                "[dry-run] Would send alert for %s (%s)\n%s",
                cid,
                listing.title[:60],
                text,
            )
            return True

        # ------------------------------------------------------------------
        # Live mode — send via Telegram.
        # ------------------------------------------------------------------
        logger.debug("Sending alert for %s (%s)", cid, listing.title[:60])
        await self._client.send_message(text, parse_mode="MarkdownV2")
        logger.info("Alert sent: %s — %s", cid, listing.title[:60])
        return True
