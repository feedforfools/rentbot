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

.PHONY: all check lint format-check typecheck test test-integration test-all fix reset-db help \
       prod-up prod-down prod-logs prod-seed prod-run-once prod-reset prod-rebuild \
       staging-up staging-down staging-logs staging-seed staging-run-once staging-reset staging-rebuild \
       clean-images promote

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

# ---------------------------------------------------------------------------
# Docker — prod / staging
# ---------------------------------------------------------------------------

COMPOSE := docker compose -f docker/docker-compose.yml

prod-up:  ## Start production container (uses docker/env/prod.env).
	$(COMPOSE) up -d prod

prod-down:  ## Stop production container.
	$(COMPOSE) stop prod

prod-logs:  ## Follow production logs.
	$(COMPOSE) logs -f prod

prod-seed:  ## Run a seed cycle in production (populate DB, no alerts).
	$(COMPOSE) run --rm prod --seed

prod-run-once:  ## Run ONE cycle in production WITH notifications (fetch + alert + exit).
	$(COMPOSE) run --rm prod --once

staging-up:  ## Build & start staging container (uses docker/env/staging.env).
	$(COMPOSE) up -d --build staging

staging-down:  ## Stop staging container.
	$(COMPOSE) stop staging

staging-logs:  ## Follow staging logs.
	$(COMPOSE) logs -f staging

staging-seed:  ## Run a seed cycle in staging (populate DB, no alerts).
	$(COMPOSE) run --rm staging --seed

staging-run-once:  ## Run ONE cycle in staging WITH notifications (fetch + alert + exit).
	$(COMPOSE) run --rm staging --once

# ---------------------------------------------------------------------------
# Docker — maintenance
# ---------------------------------------------------------------------------

staging-reset:  ## Stop staging, wipe its DB volume, and rebuild from scratch.
	$(COMPOSE) rm -sf staging
	docker volume rm -f docker_rentbot_staging_data
	@echo "Staging container and database removed. Run 'make staging-up' to start fresh."

staging-rebuild:  ## Rebuild staging image without starting it.
	$(COMPOSE) build staging

prod-reset:  ## Stop prod, wipe its DB volume, and rebuild from scratch (CAUTION).
	@echo "This will DELETE the production database. Press Ctrl-C to abort."
	@read -r _confirm
	$(COMPOSE) rm -sf prod
	docker volume rm -f docker_rentbot_prod_data
	@echo "Prod container and database removed. Run 'make prod-up' to start fresh."

prod-rebuild:  ## Rebuild prod image without starting it.
	$(COMPOSE) build prod

clean-images:  ## Remove all rentbot Docker images (forces full rebuild next time).
	docker image rm -f rentbot-core:staging rentbot-core:prod 2>/dev/null || true
	@echo "Removed rentbot-core images."

promote:  ## Copy staging.env → prod.env (backs up existing prod.env first).
	@if [ -f docker/env/prod.env ]; then \
		cp docker/env/prod.env docker/env/prod.env.bak; \
		echo "Backed up prod.env → prod.env.bak"; \
	fi
	cp docker/env/staging.env docker/env/prod.env
	@echo "Promoted staging.env → prod.env"
	@echo "Run 'make prod-up' to restart production with the new config."
