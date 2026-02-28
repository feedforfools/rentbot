"""Rentbot process entry-point.

Usage:
    python -m rentbot [--seed] [--dry-run]

The full orchestration logic lives in ``rentbot.orchestrator``.  This module
is intentionally thin — it calls ``configure_logging()`` first so that every
subsequent import already has a working logger, then hands off to the
orchestrator.
"""

from __future__ import annotations

import argparse
import logging
import sys

from rentbot.core import configure_logging


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

    if args.seed:
        logger.info("Seed mode enabled — no alerts will be sent")
    if args.dry_run:
        logger.info("Dry-run mode enabled — Telegram messages will not be sent")

    # TODO (E6-T1): replace with orchestrator.run(seed=args.seed, dry_run=args.dry_run)
    logger.warning("Orchestrator not yet implemented — exiting")
    sys.exit(0)


if __name__ == "__main__":
    main()
