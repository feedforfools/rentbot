# Rentbot

Real-time Italian rental listing monitor. Polls Immobiliare.it, Casa.it, Subito.it, Idealista.it, and local Facebook Groups, then delivers instant Telegram push notifications for new listings that match your criteria.

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
├── filters/        # Heuristic and LLM-based listing qualification
├── providers/
│   ├── api/        # API-first providers: Immobiliare, Casa, Subito
│   └── browser/    # Browser-based providers: Facebook Groups, Idealista
└── orchestrator/   # Scheduling, concurrent execution, failure isolation
```

**Two-container Docker topology:**

| Container | Responsibility | Base image |
|-----------|----------------|------------|
| `core` | API polling, dedup, filtering, Telegram alerts | `python:3.11-slim` |
| `worker` | Playwright browser scraping (Facebook, Idealista) | Playwright runtime |

The worker writes listing candidates to a shared staging file; the core container ingests and deduplicates them. SQLite is mounted only in the core container to avoid concurrent write contention.

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

See `.env.example` for the full list including LLM, Facebook, and per-provider overrides.

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

The MVP ships a single **core** container. A browser-automation worker container (for Facebook / Idealista) will be added in Phase 2.

### Prerequisites

- Docker ≥ 24 and Docker Compose v2
- `.env` file with credentials (see [Configuration](#configuration))

### Quick Start

All commands can be run from the **project root** (the directory containing `.env`):

```bash
# 1. Build the image
docker compose -f docker/docker-compose.yml build

# 2. First-run seed — populate DB without sending any alerts
docker compose -f docker/docker-compose.yml run --rm core --seed

# 3. Start continuous polling in the background
docker compose -f docker/docker-compose.yml up -d

# 4. Follow live logs
docker compose -f docker/docker-compose.yml logs -f core

# 5. Stop
docker compose -f docker/docker-compose.yml down
```

Alternatively, run commands from inside the `docker/` subdirectory (no `-f` flag needed):

```bash
cd docker/
docker compose build
docker compose run --rm core --seed
docker compose up -d
```

### Common Operations

| Task | Command (from project root) |
|------|-----------------------------|
| Build image | `docker compose -f docker/docker-compose.yml build` |
| Seed (first run, no alerts) | `docker compose -f docker/docker-compose.yml run --rm core --seed` |
| Start background | `docker compose -f docker/docker-compose.yml up -d` |
| Start foreground | `docker compose -f docker/docker-compose.yml up` |
| Dry-run test | `docker compose -f docker/docker-compose.yml run --rm core --dry-run --once` |
| Follow logs | `docker compose -f docker/docker-compose.yml logs -f core` |
| Stop | `docker compose -f docker/docker-compose.yml down` |
| Stop + wipe DB | `docker compose -f docker/docker-compose.yml down -v` |
| Check container health | `docker inspect --format='{{.State.Health.Status}}' rentbot_core` |

### Resource Sizing

The core container polls two API providers on a randomised 5–10 minute interval. It is deliberately lightweight.

| Resource | Minimum | Recommended | Notes |
|----------|---------|-------------|-------|
| CPU | 0.05 vCPU | 0.1 vCPU | Mostly idle between polls; brief burst during fetch + parse |
| Memory | 64 MB | 128 MB | Python runtime + aiosqlite; use 256 MB for a comfortable safety margin |
| Disk (DB) | 10 MB | 50 MB | SQLite grows a few KB/day; years of history before sizing matters |
| Network | < 0.5 MB/poll | < 2 MB/poll | Two provider API calls per cycle, compressed JSON responses |

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

SQLite is stored in the `rentbot_data` Docker managed volume, mounted at `/data/rentbot.db` inside the container.

```bash
# Find where Docker stores the volume on the host
docker volume inspect rentbot_data

# Backup the database to the current directory
docker run --rm \
    -v rentbot_data:/data \
    -v "$(pwd)":/backup \
    busybox cp /data/rentbot.db /backup/rentbot-backup.db
```

> **Production tip:** swap the named volume for a host-bind mount so the DB path is explicit and easy to include in existing backup routines:
> ```yaml
> volumes:
>   - /opt/rentbot/data:/data
> ```

### Health Checks

After each polling cycle the scheduler writes a heartbeat timestamp to `/tmp/rentbot_heartbeat`. The Docker `HEALTHCHECK` reads that file and reports `healthy` as long as the last cycle completed within the past 30 minutes.

```bash
# Quick status check
docker inspect --format='{{.State.Health.Status}}' rentbot_core

# Full health history (last 5 check results)
docker inspect --format='{{json .State.Health}}' rentbot_core | python -m json.tool
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
- Use `httpx.AsyncClient` for all HTTP calls.
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

- **Formatter & linter:** `ruff` (configured in `pyproject.toml`). Run before every commit:
  ```bash
  ruff check --fix .
  ruff format .
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
- The core process is the **single SQLite writer**. Browser workers write to a shared staging file; the core process ingests it.

### Testing

- Tests live in `tests/unit/` (pure logic, no I/O) and `tests/integration/` (real or carefully faked I/O).
- Each epic ships its own tests — tests are **never deferred**.
- Async tests use `pytest-asyncio` with `asyncio_mode = "auto"` (set in `pyproject.toml`).
- External HTTP calls must be **mocked** in unit tests (use `pytest-mock` or `respx`).
- Aim for tests that are fast, deterministic, and independent of execution order.

### Dependencies

- Pin core versions with lower and upper bounds in `pyproject.toml` (e.g. `httpx>=0.27,<1`).
- Optional groups: `[dev]`, `[test]`, `[browser]`, `[llm]`.
- Never add a dependency solely to `requirements.txt` — `pyproject.toml` is the single source of truth.

---

## Testing

```bash
# Run all tests
pytest

# Run only unit tests
pytest tests/unit/

# Run with verbose output
pytest -v

# Run with log output visible
pytest -s
```

---

## Project Status

**MVP phase complete.** The API-based pipeline (Immobiliare.it + Casa.it → dedup → heuristic filter → Telegram) is operational and containerised.

| Phase | Scope | Status |
|-------|-------|--------|
| Phase 1 — MVP | Epics 0–3 (foundation, domain, notifications, API providers), Epic 6 (orchestration), Epic 7 (Docker), Epic 8 (integration tests + observability) | 🔄 Epic 8 remaining |
| Phase 2 — LLM + Browser | Epic 4 (LLM filter), Epic 5 (Facebook + Idealista), E6-T7 (core↔worker contract), Epic 7 Phase 2 additions | 🔜 Planned |
| Phase 3 — Extensibility | Epic 9 (provider plugin registry) | 🔜 Future |

All 276 unit and integration tests pass. Run `pytest` to verify.