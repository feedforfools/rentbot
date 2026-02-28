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

Bulk delivery
-------------
For bulk alert bursts (e.g. first real run after seeding returns many new
listings), use :meth:`send_alerts` rather than calling :meth:`send_alert` in
a tight loop.  :meth:`send_alerts` enforces a minimum inter-message interval
(default :data:`_DEFAULT_MIN_INTERVAL`) so the process stays well under
Telegram's 30 messages/second per-chat limit.  Individual send failures do
not abort the batch — they are logged and counted.

Typical usage::

    from rentbot.notifiers.notifier import Notifier
    from rentbot.notifiers.telegram import TelegramClient
    from rentbot.core.run_context import RunContext

    ctx = RunContext(seed=False, dry_run=False)

    async with TelegramClient(token=settings.telegram_bot_token,
                               chat_id=settings.telegram_chat_id) as client:
        notifier = Notifier(client=client, ctx=ctx)

        # Single alert:
        sent = await notifier.send_alert(listing)

        # Bulk alerts with throttle:
        sent_count, failed_count = await notifier.send_alerts(listings)
"""

from __future__ import annotations

import asyncio
import logging
from typing import Final

from rentbot.core.exceptions import TelegramError
from rentbot.core.models import Listing
from rentbot.core.run_context import RunContext
from rentbot.notifiers.formatter import format_listing
from rentbot.notifiers.telegram import TelegramClient

__all__ = ["Notifier"]

logger = logging.getLogger(__name__)

# Telegram Bot API throttle: hard limit is 30 messages/sec per chat.
# Using 20 msgs/sec (0.05 s gap) as the default to leave comfortable head-room
# and absorb any jitter from the async event loop and network latency.
_DEFAULT_MIN_INTERVAL: Final[float] = 0.05


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
        min_interval_s: Minimum seconds between consecutive sends in
            :meth:`send_alerts`.  Defaults to
            :data:`_DEFAULT_MIN_INTERVAL` (0.05 s = 20 msgs/sec).
    """

    def __init__(
        self,
        client: TelegramClient,
        ctx: RunContext,
        *,
        min_interval_s: float = _DEFAULT_MIN_INTERVAL,
    ) -> None:
        if min_interval_s < 0:
            raise ValueError(
                f"min_interval_s must be ≥ 0, got {min_interval_s!r}."
            )
        self._client = client
        self._ctx = ctx
        self._min_interval_s = min_interval_s

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
                send fails after all retries are exhausted.  A structured
                ``ERROR`` log entry (including listing id and title) is
                always emitted before the exception propagates, so callers
                do not need to repeat the logging.
            Exception: Unexpected non-Telegram exceptions are logged at
                ``CRITICAL`` level and re-raised.
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
        try:
            await self._client.send_message(text, parse_mode="MarkdownV2")
        except TelegramError as exc:
            logger.error(
                "Failed to send alert for %s (%s) after all retries: %s",
                cid,
                listing.title[:60],
                exc,
            )
            raise
        except Exception as exc:  # noqa: BLE001
            logger.critical(
                "Unexpected error sending alert for %s (%s): %s",
                cid,
                listing.title[:60],
                exc,
                exc_info=True,
            )
            raise
        logger.info("Alert sent: %s — %s", cid, listing.title[:60])
        return True

    async def send_alerts(self, listings: list[Listing]) -> tuple[int, int]:
        """Deliver alerts for a batch of listings with inter-message throttling.

        Iterates through *listings* sequentially, calling :meth:`send_alert`
        for each one.  A minimum sleep of :attr:`_min_interval_s` is
        inserted **between** sends (not before the first or after the last)
        to keep the delivery rate under Telegram's 30 messages/second limit.

        Individual send failures (:exc:`~rentbot.core.exceptions.TelegramError`
        or unexpected exceptions) are caught, logged by :meth:`send_alert`,
        and counted as failures.  The batch continues with the remaining
        listings — a single failing message does not abort the burst.

        Args:
            listings: Ordered list of listings to alert on.  May be empty.

        Returns:
            A ``(sent, failed)`` tuple where:

            * ``sent`` — number of listings for which :meth:`send_alert`
              returned ``True``.
            * ``failed`` — number of listings for which :meth:`send_alert`
              raised an exception.

            Seed-mode skips (``send_alert`` returns ``False``) are counted
            in neither bucket.
        """
        if not listings:
            return 0, 0

        sent = 0
        failed = 0

        for index, listing in enumerate(listings):
            # Throttle: sleep before every message except the very first.
            if index > 0 and self._min_interval_s > 0:
                await asyncio.sleep(self._min_interval_s)

            try:
                result = await self.send_alert(listing)
            except Exception:  # noqa: BLE001
                # send_alert already logged the error; just tally it.
                failed += 1
                continue

            if result:
                sent += 1

        if failed:
            logger.warning(
                "Bulk alert batch finished: %d sent, %d failed (total=%d).",
                sent,
                failed,
                len(listings),
            )
        else:
            logger.info(
                "Bulk alert batch finished: %d sent (total=%d).",
                sent,
                len(listings),
            )

        return sent, failed
