# ---------------------------------------------------------------------------
# Rentbot — Developer convenience targets
# ---------------------------------------------------------------------------
# Usage:
#   make check       — run lint + format-check + typecheck (CI gate)
#   make test        — run unit tests
#   make fix         — auto-fix lint violations and reformat in place
#   make all         — check + test
#
# Each target delegates to the tool configured in pyproject.toml so that
# all rule sets and tolerance thresholds live in one place.

PYTHON   := python
RUFF     := $(PYTHON) -m ruff
MYPY     := $(PYTHON) -m mypy
PYTEST   := $(PYTHON) -m pytest
SRC      := rentbot/
TESTS    := tests/

.PHONY: all check lint format-check typecheck test test-integration test-all fix reset-db help

# ---------------------------------------------------------------------------
# Composite targets
# ---------------------------------------------------------------------------

all: check test  ## Run all quality gates and unit tests.

check: lint format-check typecheck  ## Run all static checks (lint + format + types).

# ---------------------------------------------------------------------------
# Individual quality gates
# ---------------------------------------------------------------------------

lint:  ## Check for linting violations (ruff check, no writes).
	$(RUFF) check $(SRC) $(TESTS)

format-check:  ## Check formatting without writing (fails if files would change).
	$(RUFF) format --check $(SRC) $(TESTS)

typecheck:  ## Run strict static type checking (mypy).
	$(MYPY) $(SRC)

# ---------------------------------------------------------------------------
# Auto-fix targets (writes to disk — do not run in CI)
# ---------------------------------------------------------------------------

fix:  ## Auto-fix lint violations and reformat files in place.
	$(RUFF) check --fix $(SRC) $(TESTS)
	$(RUFF) format $(SRC) $(TESTS)

# ---------------------------------------------------------------------------
# Test targets
# ---------------------------------------------------------------------------

test:  ## Run unit tests (excludes integration tests).
	$(PYTEST) $(TESTS)unit/ -q

test-integration:  ## Run integration tests (requires external credentials in .env).
	$(PYTEST) -m integration $(TESTS)integration/ -v

test-all:  ## Run unit + integration tests.
	$(PYTEST) -m "" $(TESTS) -v

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

DB_PATH := data/rentbot.db

reset-db:  ## Delete the SQLite database so the next run starts fresh.
	@if [ -f "$(DB_PATH)" ]; then \
		rm -f "$(DB_PATH)" "$(DB_PATH)-wal" "$(DB_PATH)-shm"; \
		echo "Deleted $(DB_PATH) (and WAL/SHM files)."; \
	else \
		echo "$(DB_PATH) does not exist — nothing to delete."; \
	fi

# ---------------------------------------------------------------------------
# Help
# ---------------------------------------------------------------------------

help:  ## Show this help message.
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'
