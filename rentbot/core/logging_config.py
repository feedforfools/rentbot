"""Rentbot logging configuration.

Call ``configure_logging()`` once at process startup (e.g. in ``__main__``).
Every other module must define its own logger at module scope:

    import logging
    logger = logging.getLogger(__name__)

Supported environment variables (read at call time):
    LOG_LEVEL   DEBUG | INFO | WARNING | ERROR   (default: INFO)
    LOG_FORMAT  text | json                      (default: text)
"""

from __future__ import annotations

import json
import logging
import os
import sys
import traceback
from contextvars import ContextVar
from datetime import UTC, datetime
from typing import Any

__all__ = ["configure_logging", "JsonFormatter", "CYCLE_ID_CTX", "CycleContextFilter"]

# ---------------------------------------------------------------------------
# Cycle-scoped context variable
# ---------------------------------------------------------------------------

#: Async-safe context variable that holds the current poll-cycle identifier.
#: Set to a short hex string (``uuid4().hex[:8]``) at the start of every call
#: to :func:`~rentbot.orchestrator.runner.run_once`; automatically inherited
#: by all child tasks spawned via ``asyncio.gather``.  Defaults to ``"-"``
#: outside of any cycle (startup, teardown, tests).
CYCLE_ID_CTX: ContextVar[str] = ContextVar("cycle_id", default="-")

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

logger = logging.getLogger(__name__)

_VALID_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
_VALID_FORMATS = {"text", "json"}

# Human-readable text format used in ``text`` mode.
# ``%(cycle_id)s`` is injected by :class:`CycleContextFilter` and resolves to
# the current cycle correlation ID (e.g. ``a3f2b1c0``) or ``-`` outside cycles.
_TEXT_FORMAT = "%(asctime)s %(levelname)-8s [%(cycle_id)s] %(name)s: %(message)s"
_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


class CycleContextFilter(logging.Filter):
    """Inject the current poll-cycle ID into every log record.

    Reads :data:`CYCLE_ID_CTX` from the async context and sets
    ``record.cycle_id`` before the record reaches any formatter.  This means:

    * **Text format** — the ``%(cycle_id)s`` token in :data:`_TEXT_FORMAT`
      resolves to the 8-char hex cycle ID (or ``"-"`` outside a cycle).
    * **JSON format** — ``cycle_id`` appears in the ``"extra"`` object of the
      emitted JSON line, making it trivial to group records by cycle in any
      log aggregator.

    The filter is installed on every handler by :func:`configure_logging`.
    Because the filter is on the *handler* (not the logger), it runs just
    before formatting, after propagation — ensuring every handler that goes
    through ``configure_logging`` gets the attribute.
    """

    def filter(self, record: logging.LogRecord) -> bool:  # noqa: A003
        """Attach ``cycle_id`` to *record* and allow all records through.

        Args:
            record: The log record being processed.

        Returns:
            Always ``True`` — this filter never suppresses records.
        """
        record.cycle_id = CYCLE_ID_CTX.get("-")
        return True


def configure_logging(
    level: str | None = None,
    fmt: str | None = None,
    *,
    force: bool = False,
) -> None:
    """Configure the root logger for the entire process.

    Args:
        level: Logging level string (DEBUG/INFO/WARNING/ERROR/CRITICAL).
            Falls back to ``$LOG_LEVEL`` env var, then "INFO".
        fmt: Output format ("text" or "json").
            Falls back to ``$LOG_FORMAT`` env var, then "text".
        force: If True, reconfigure even if logging has already been set up.
            Useful in tests and CLI entry-points.

    Raises:
        ValueError: If *level* or *fmt* contain an unrecognised value.
    """
    resolved_level = (level or os.environ.get("LOG_LEVEL", "INFO")).upper()
    resolved_fmt = (fmt or os.environ.get("LOG_FORMAT", "text")).lower()

    if resolved_level not in _VALID_LEVELS:
        raise ValueError(
            f"Unknown LOG_LEVEL {resolved_level!r}. "
            f"Must be one of: {', '.join(sorted(_VALID_LEVELS))}"
        )
    if resolved_fmt not in _VALID_FORMATS:
        raise ValueError(
            f"Unknown LOG_FORMAT {resolved_fmt!r}. "
            f"Must be one of: {', '.join(sorted(_VALID_FORMATS))}"
        )

    root = logging.getLogger()

    if root.handlers and not force:
        # Logging was already configured (e.g. by pytest's log_cli).  Respect
        # the existing setup but still propagate the requested level.
        root.setLevel(resolved_level)
        return

    # Remove any handlers added by earlier configure_logging() calls or by
    # Python's logging.basicConfig default initialisation.
    root.handlers.clear()

    handler = logging.StreamHandler(sys.stderr)
    handler.setLevel(resolved_level)
    # Inject cycle_id from async ContextVar into every log record so that
    # the text format and JSON "extra" both carry the correlation ID.
    handler.addFilter(CycleContextFilter())

    if resolved_fmt == "json":
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(logging.Formatter(fmt=_TEXT_FORMAT, datefmt=_DATE_FORMAT))

    root.setLevel(resolved_level)
    root.addHandler(handler)

    # Quieten noisy third-party libraries to WARNING unless DEBUG is active.
    if resolved_level != "DEBUG":
        for noisy in ("httpx", "httpcore", "asyncio", "apscheduler"):
            logging.getLogger(noisy).setLevel(logging.WARNING)


# ---------------------------------------------------------------------------
# JSON formatter
# ---------------------------------------------------------------------------


class JsonFormatter(logging.Formatter):
    """Emit one JSON object per log record.

    Each line is a self-contained JSON object with a stable set of fields,
    making it straightforward to ingest into log aggregators (Loki, CloudWatch,
    etc.).  Additional ``extra`` kwargs passed to the logger call are surfaced
    under the ``"extra"`` key.

    Output shape (all fields always present)::

        {
            "ts":      "2026-02-28T12:34:56.789Z",
            "level":   "INFO",
            "logger":  "rentbot.providers.api.immobiliare",
            "message": "Fetched 12 listings",
            "extra":   {}
        }

    Optional fields (present only when applicable)::

        "exc_info": "<traceback string>"
    """

    # Fields that belong to LogRecord but should NOT appear under "extra".
    _RECORD_ATTRS: frozenset[str] = frozenset(
        {
            "args",
            "created",
            "exc_info",
            "exc_text",
            "filename",
            "funcName",
            "levelname",
            "levelno",
            "lineno",
            "message",
            "module",
            "msecs",
            "msg",
            "name",
            "pathname",
            "process",
            "processName",
            "relativeCreated",
            "stack_info",
            "taskName",
            "thread",
            "threadName",
        }
    )

    def format(self, record: logging.LogRecord) -> str:  # noqa: A003
        """Serialise *record* to a JSON string."""
        record.message = record.getMessage()

        ts = (
            datetime.fromtimestamp(record.created, tz=UTC).strftime("%Y-%m-%dT%H:%M:%S.")
            + f"{int(record.msecs):03d}Z"
        )

        payload: dict[str, Any] = {
            "ts": ts,
            "level": record.levelname,
            "logger": record.name,
            "message": record.message,
        }

        # Collect any kwargs passed via logging.info(..., extra={...}).
        extra = {k: v for k, v in record.__dict__.items() if k not in self._RECORD_ATTRS}
        payload["extra"] = extra

        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        elif record.exc_text:
            payload["exc_info"] = record.exc_text

        if record.stack_info:
            payload["stack_info"] = self.formatStack(record.stack_info)

        try:
            return json.dumps(payload, default=str)
        except Exception:  # pragma: no cover — safety net
            # Fall back to a minimal safe payload rather than crashing.
            return json.dumps(
                {
                    "ts": ts,
                    "level": "ERROR",
                    "logger": __name__,
                    "message": "JsonFormatter serialisation error",
                    "exc_info": traceback.format_exc(),
                    "extra": {},
                }
            )
