"""Rentbot process entry-point.

Usage:
    python -m rentbot [--seed] [--dry-run] [--once]

The full orchestration logic lives in ``rentbot.orchestrator``.  This module
is intentionally thin — it calls ``configure_logging()`` first so that every
subsequent import already has a working logger, then hands off to the
orchestrator.

Default behaviour (no ``--once``) is continuous: the scheduler loops
indefinitely, sleeping a randomised interval between cycles.  Pass ``--once``
to execute a single poll cycle and exit (useful for ``--seed`` runs and
manual testing).
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys

from rentbot.core import configure_logging
from rentbot.core.exceptions import ConfigError
from rentbot.core.run_context import RunContext
from rentbot.core.settings import Settings


def main() -> None:
    """CLI entry-point registered in ``pyproject.toml``."""
    parser = argparse.ArgumentParser(
        prog="rentbot",
        description="Real-time Italian rental listing monitor.",
    )
    parser.add_argument(
        "--seed",
        action="store_true",
        help=(
            "Seed mode: populate the DB with current listings "
            "without sending any Telegram alerts."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Log alert payloads without actually sending Telegram messages.",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help=(
            "Run a single poll cycle and exit instead of looping continuously. "
            "Implied by --seed (seeding is always a one-shot operation)."
        ),
    )
    parser.add_argument(
        "--log-level",
        default=None,
        metavar="LEVEL",
        help="Override LOG_LEVEL env var (DEBUG|INFO|WARNING|ERROR).",
    )
    parser.add_argument(
        "--log-format",
        default=None,
        metavar="FORMAT",
        help="Override LOG_FORMAT env var (text|json).",
    )

    args = parser.parse_args()

    # Configure logging BEFORE any other rentbot imports so that every module
    # obtains a correctly-configured logger on first import.
    try:
        configure_logging(level=args.log_level, fmt=args.log_format)
    except ValueError as exc:
        print(f"rentbot: configuration error: {exc}", file=sys.stderr)  # noqa: T201
        sys.exit(1)

    logger = logging.getLogger(__name__)
    logger.info("Rentbot starting up")

    ctx = RunContext(seed=args.seed, dry_run=args.dry_run)
    logger.info("Run context: %s", ctx)

    # Lazy import keeps startup fast when module is imported without running.
    from rentbot.orchestrator.runner import run_once  # noqa: PLC0415
    from rentbot.orchestrator.scheduler import run_continuous  # noqa: PLC0415

    settings = Settings()

    # --seed is always a one-shot operation (populates DB, then exits).
    run_once_mode: bool = args.once or args.seed

    try:
        if run_once_mode:
            logger.info("Running single poll cycle (--once / --seed mode).")
            asyncio.run(run_once(ctx=ctx, settings=settings))
        else:
            logger.info("Running in continuous mode (Ctrl+C to stop).")
            asyncio.run(run_continuous(ctx=ctx, settings=settings))
    except ConfigError as exc:
        logger.critical("Configuration error: %s", exc)
        sys.exit(1)
    except KeyboardInterrupt:
        logger.info("Interrupted — exiting.")
        sys.exit(0)
    except asyncio.CancelledError:
        # Raised when run_continuous() is stopped via SIGTERM — the
        # signal handler in scheduler.py already logged the shutdown
        # message, so we exit silently here.
        logger.info("Shutdown complete — exiting.")
        sys.exit(0)


if __name__ == "__main__":
    main()
