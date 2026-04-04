# Rentbot

Real-time Italian rental listing monitor. Polls Immobiliare.it, Casa.it, and Subito.it, then delivers instant Telegram push notifications for new listings that match your criteria.

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Running](#running)
- [Docker](#docker)
- [Coding Conventions](#coding-conventions)
- [Testing](#testing)
- [Project Status](#project-status)

---

## Architecture Overview

```
rentbot/
├── core/           # Domain models, settings, logging, shared utilities, exceptions
├── storage/        # SQLite repository — dedup and listing metadata
├── notifiers/      # Telegram delivery and message formatting
├── filters/        # Heuristic listing qualification
├── providers/
│   └── api/        # API providers: Immobiliare (httpx), Casa (httpx), Subito (curl_cffi)
└── orchestrator/   # Scheduling, concurrent execution, failure isolation
```

**Single-container Docker topology:**

| Container | Responsibility | Base image |
|-----------|----------------|------------|
| `core` | API polling, dedup, filtering, Telegram alerts | `python:3.11-slim` (~164 MB) |

All three providers are API-based and run in a single lightweight container. SQLite is persisted on a mounted volume (WAL mode, single writer).

---

## Quick Start

```bash
# 1. Clone and enter the repo
git clone <repo-url> && cd rentbot

# 2. Create a virtual environment
python3.11 -m venv .venv && source .venv/bin/activate

# 3. Install all dependency groups
pip install -e ".[dev,test]"

# 4. Copy and fill in secrets
cp .env.example .env
$EDITOR .env

# 5. Smoke-test the startup
python -m rentbot --dry-run
```

---

## Configuration

All configuration is via environment variables. Copy `.env.example` to `.env` and fill in the required values.

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `TELEGRAM_BOT_TOKEN` | ✓ | — | Bot token from `@BotFather` |
| `TELEGRAM_CHAT_ID` | ✓ | — | Numeric chat ID for alert delivery |
| `SEARCH_CITY` | | `Pordenone` | City / search area for all providers |
| `SEARCH_MAX_PRICE` | | `800` | Maximum monthly rent (EUR) |
| `SEARCH_MIN_ROOMS` | | `2` | Minimum number of rooms |
| `DATABASE_PATH` | | `data/rentbot.db` | SQLite file path |
| `SEED_MODE` | | `false` | Populate DB without sending alerts |
| `DRY_RUN` | | `false` | Log alert payloads without sending |
| `LOG_LEVEL` | | `INFO` | `DEBUG` / `INFO` / `WARNING` / `ERROR` |
| `LOG_FORMAT` | | `text` | `text` (human-readable) or `json` (structured) |

See `.env.example` for the full list including per-provider overrides (`IMMOBILIARE_VRT`, `CASA_SEARCH_URL`, `SUBITO_REGION`, etc.).

---

## Running

```bash
# Normal run
python -m rentbot

# First-run seeding (populate DB without alerts)
python -m rentbot --seed

# Local development without sending Telegram messages
python -m rentbot --dry-run

# Override log level/format at runtime
python -m rentbot --log-level DEBUG --log-format json
```

---

## Docker

The system runs as a single container with separate **prod** and **staging** environments, each with its own `.env` file and SQLite database volume.

### Prerequisites

- Docker ≥ 24 and Docker Compose v2
- Environment files: `docker/env/prod.env` and/or `docker/env/staging.env` (copy from `.example` files and fill in credentials)

### Quick Start

```bash
# 1. Set up environment files
cp docker/env/staging.env.example docker/env/staging.env
$EDITOR docker/env/staging.env

# 2. Build & start staging (runs first cycle immediately)
make staging-up

# 3. Follow live logs
make staging-logs

# 4. Stop
make staging-down
```

### Common Operations

| Task | Makefile target |
|------|-----------------|
| Build & start staging | `make staging-up` |
| Build & start production | `make prod-up` |
| Stop staging / prod | `make staging-down` / `make prod-down` |
| Follow staging / prod logs | `make staging-logs` / `make prod-logs` |
| Seed (populate DB, no alerts) | `make staging-seed` / `make prod-seed` |
| Run one cycle with alerts | `make staging-run-once` / `make prod-run-once` |
| Rebuild image only | `make staging-rebuild` / `make prod-rebuild` |
| Wipe container + DB (fresh start) | `make staging-reset` / `make prod-reset` |
| Remove all Docker images | `make clean-images` |
| Promote staging config to prod | `make promote` |

### Resource Sizing

The core container polls three API providers on a randomised 5–10 minute interval. It is deliberately lightweight.

| Resource | Minimum | Recommended | Notes |
|----------|---------|-------------|-------|
| CPU | 0.05 vCPU | 0.1 vCPU | Mostly idle between polls; brief burst during fetch + parse |
| Memory | 64 MB | 128 MB | Python runtime + aiosqlite; use 256 MB for a comfortable safety margin |
| Disk (DB) | 10 MB | 50 MB | SQLite grows a few KB/day; years of history before sizing matters |
| Network | < 0.5 MB/poll | < 2 MB/poll | Three provider API calls per cycle, compressed JSON responses |

To enforce hard limits, add a `deploy.resources` block to the `core` service in `docker-compose.yml`:

```yaml
    deploy:
      resources:
        limits:
          cpus: "0.25"
          memory: 256M
        reservations:
          cpus: "0.05"
          memory: 64M
```

### Persistent Data

Each environment has its own Docker managed volume (`rentbot_staging_data`, `rentbot_prod_data`), mounted at `/data/rentbot.db` inside the container.

```bash
# Find where Docker stores the staging volume on the host
docker volume inspect docker_rentbot_staging_data

# Backup the staging database to the current directory
docker run --rm \
    -v docker_rentbot_staging_data:/data \
    -v "$(pwd)":/backup \
    busybox cp /data/rentbot.db /backup/rentbot-staging-backup.db
```

> **Production tip:** swap the named volume for a host-bind mount so the DB path is explicit and easy to include in existing backup routines:
> ```yaml
> volumes:
>   - /opt/rentbot/data:/data
> ```

### Health Checks

After each polling cycle the scheduler writes a heartbeat timestamp to `/tmp/rentbot_heartbeat`. The Docker `HEALTHCHECK` reads that file and reports `healthy` as long as the last cycle completed within the past 30 minutes.

```bash
# Quick status check (staging)
docker inspect --format='{{.State.Health.Status}}' rentbot_staging

# Quick status check (production)
docker inspect --format='{{.State.Health.Status}}' rentbot_prod

# Full health history (last 5 check results)
docker inspect --format='{{json .State.Health}}' rentbot_staging | python -m json.tool
```

---

## Coding Conventions

These rules are non-negotiable and apply to every file in the codebase.

### Language & Runtime

- **Python 3.11+** is the minimum supported version.
- All function and method signatures **must have type hints** on every parameter and return value.
- Use `from __future__ import annotations` at the top of every module for forward-reference compatibility.

### Data Models & Settings

- Use **Pydantic v2** (`BaseModel`, `model_validator`, `Field`) for all data models and config.
- Application settings are loaded via **`pydantic-settings`** (`BaseSettings`).
- **Never hardcode secrets.** All tokens, passwords, and API keys live in `.env` and are loaded through the settings model.

### Async I/O

- I/O-bound operations (HTTP, DB, file reads) **must be async** using `asyncio`.
- Use `httpx.AsyncClient` for HTTP calls (Immobiliare, Casa). Subito uses `curl_cffi.requests.AsyncSession` (Chrome TLS impersonation to bypass Akamai WAF).
- Providers run concurrently via `asyncio.gather()` — never sequentially in the hot path.

### Resilience

- Wrap every external call (HTTP, SQLite, LLM API, Telegram) in **`tenacity` retry logic** with exponential backoff and a sensible `stop_after_attempt` limit.
- Set explicit **timeouts** on all `httpx` requests.
- Provider failures must be **isolated** — one provider crashing must not abort the polling cycle.

### Logging

- **No `print()` calls** in production code. Use the `logging` module exclusively.
- Every module must declare a **module-level logger** as its first non-import statement:
  ```python
  logger = logging.getLogger(__name__)
  ```
- Use structured `extra` dicts for machine-parseable context:
  ```python
  logger.info("Fetched listings", extra={"provider": "immobiliare", "count": 12})
  ```
- Log at the appropriate level: `DEBUG` for per-listing detail, `INFO` for cycle summaries, `WARNING` for recoverable issues, `ERROR` for failures requiring attention.

### Exceptions

- All custom exceptions inherit from `RentbotError` (defined in `rentbot.core.exceptions`).
- Exceptions are organised by layer: `StorageError`, `ProviderError`, `FilterError`, `NotificationError`, `OrchestratorError`.
- **Always chain exceptions** with `raise XxxError(...) from original_exc` to preserve tracebacks.
- Catch exceptions at the layer boundary that can meaningfully handle them; let the rest propagate.

### Code Style

- **Formatter & linter:** `ruff` (configured in `pyproject.toml`). Use the `Makefile` targets — they delegate to the same config:
  ```bash
  make fix        # auto-fix violations and reformat in place
  make check      # lint + format-check + typecheck (CI gate, no writes)
  ```
  Or call directly:
  ```bash
  ruff check --fix rentbot/ tests/
  ruff format rentbot/ tests/
  ```
- **Line length:** 100 characters.
- **Imports:** isort-style, `ruff` enforces ordering. Standard library → third-party → first-party (`rentbot`).
- **Quote style:** double quotes (`"`).
- **Docstrings:** Google style on all public functions, methods, and classes. One-line summary, then `Args:` / `Returns:` / `Raises:` sections where applicable.

### Module Structure

Each Python module should follow this top-to-bottom order:

```
1. module docstring
2. from __future__ import annotations
3. stdlib imports
4. third-party imports
5. first-party imports
6. __all__ (if the module is a public API surface)
7. logger = logging.getLogger(__name__)
8. constants
9. public classes / functions
10. private helpers (_prefixed)
```

### Storage Rules

- **Store before filter:** a listing must be inserted into the DB immediately after the dedup check, _before_ the filter runs. Filtering decides whether to alert, not whether to store.
- **SQLite WAL mode** must be enabled at DB initialisation.
- The core process is the **single SQLite writer**.

### Testing

- Tests live in `tests/unit/` (pure logic, no I/O) and `tests/integration/` (real or carefully faked I/O).
- Each epic ships its own tests — tests are **never deferred**.
- Async tests use `pytest-asyncio` with `asyncio_mode = "auto"` (set in `pyproject.toml`).
- External HTTP calls must be **mocked** in unit tests (use `pytest-mock` or `respx`).
- Aim for tests that are fast, deterministic, and independent of execution order.

### Dependencies

- Pin core versions with lower and upper bounds in `pyproject.toml` (e.g. `httpx>=0.27,<1`).
- Optional groups: `[dev]`, `[test]`.
- Never add a dependency solely to `requirements.txt` — `pyproject.toml` is the single source of truth.

---

## Testing

A `Makefile` at the project root exposes the standard developer workflows:

```bash
# Run quality gates (lint + format-check + typecheck) — use as a pre-commit check
make check

# Run unit tests only (fast, no external I/O)
make test

# Auto-fix lint violations and reformat files in place
make fix

# Run unit + integration tests (integration tests require .env credentials)
make test-all

# Run integration tests only
make test-integration

# Show all available Makefile targets
make help
```

Or invoke the tools directly:

```bash
# Unit tests
pytest tests/unit/

# Full suite including integration tests
pytest -m ""

# Verbose output
pytest -v

# With log output visible
pytest -s
```

Integration tests (in `tests/integration/`) make real HTTP calls or require Telegram credentials. They are excluded from the default `pytest` run (`-m 'not integration'` in `pyproject.toml`) and must be opted into explicitly via `make test-integration` or `pytest -m integration`.

---

## Project Status

**All planned providers are operational.** The full pipeline (Immobiliare.it + Casa.it + Subito.it → dedup → heuristic filter → Telegram) is deployed with prod/staging Docker environments.

| Phase | Scope | Status |
|-------|-------|--------|
| Phase 1 — MVP | Epics 0–3 (foundation, domain, notifications, API providers), Epic 6 (orchestration), Epic 7 (Docker), Epic 8 (quality gates + observability) | ✅ Complete |
| Phase 2 — Subito.it | Subito provider (Hades JSON API + `curl_cffi` Akamai bypass), Docker staging/prod split, Makefile maintenance targets | ✅ Complete |
| Phase 3 — Extensibility | Epic 9 (provider plugin registry) | 🔜 Future |

448 unit tests pass. Run `make check && make test` to verify.