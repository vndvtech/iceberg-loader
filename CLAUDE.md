# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Set up dev environment
uv sync

# Run all tests
uv run pytest

# Run a single test file
uv run pytest tests/test_iceberg_loader.py

# Run tests with coverage
uv run pytest --cov=iceberg_loader --cov-report=html

# Lint + format check + type check
uv run ruff check . && uv run ruff format --check . && uv run mypy src/iceberg_loader tests

# Auto-fix lint issues
uv run ruff check --fix .

# Format code
uv run ruff format .
```

## Architecture

The library is a thin convenience wrapper around PyIceberg for loading PyArrow data into Apache Iceberg tables.

**Public API** (`src/iceberg_loader/__init__.py`):
- `load_data_to_iceberg(table, identifier, catalog, config)` — single `pa.Table` in memory
- `load_batches_to_iceberg(iterator, identifier, catalog, config)` — iterator of `pa.RecordBatch`
- `load_ipc_stream_to_iceberg(stream, identifier, catalog, config)` — Arrow IPC stream file/socket
- `IcebergLoader` — stateful class for reuse across calls
- `LoaderConfig` — Pydantic config model; all options go here

**Core layer** (`src/iceberg_loader/core/`):
- `loader.py` — `IcebergLoader` is the main orchestrator. `load_data_batches` is the central method: it buffers batches, calls `SchemaManager` to create/evolve the table, then delegates writes to the selected `WriteStrategy`.
- `config.py` — `LoaderConfig` (frozen Pydantic model). Validates partition expressions, rejects invalid combos (`replace_filter` + `upsert`, identity partition on the load timestamp column). Default table properties (Parquet/zstd, format v2, commit retry) live here as `TABLE_PROPERTIES`.
- `strategies.py` — Strategy pattern for writes: `AppendStrategy`, `OverwriteStrategy` (overwrites on first batch, appends after), `IdempotentStrategy` (delete-then-append via `replace_filter`), `UpsertStrategy` (PyIceberg Merge Into). Selected by `get_write_strategy()`.
- `schema.py` — `SchemaManager` converts Arrow↔Iceberg schemas, creates tables, and handles schema evolution (adds new columns only, top-level).
- `partitioning.py` — Parses partition transform strings like `day(signup_date)` or `bucket(16, id)`.

**Utils** (`src/iceberg_loader/utils/`):
- `arrow.py` — `create_arrow_table_from_data`, `create_record_batches_from_dicts` (auto-serializes dicts/lists to strings), `convert_table_types` (casts Arrow table to match target schema).
- `types.py` — Bidirectional Arrow↔Iceberg type mappings.

**Services** (`src/iceberg_loader/services/`):
- `logging.py` — module-level logger
- `maintenance.py` — `expire_snapshots` utility

## Key Constraints

- `schema_evolution=False` by default; set to `True` to add columns automatically.
- `replace_filter` and `write_mode='upsert'` are mutually exclusive.
- Identity partition on the load timestamp column (`_load_dttm`) is rejected — use `day(...)` or `hour(...)`.
- Partition string columns used with time transforms (`year`/`month`/`day`/`hour`) are auto-promoted to `timestamp(us)` at table creation.
- All public API options must be passed via `LoaderConfig`; avoid legacy positional arguments.

## Versioning & Release

- Bump version in both `pyproject.toml` and `src/iceberg_loader/__about__.py` (must match).
- Update `RELEASE.md` and run `uv lock --locked` before tagging.
- Tag format: `git tag -a vX.Y.Z` — CI publishes to PyPI automatically.
