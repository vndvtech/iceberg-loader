# File Map

Directory structure, file purposes, and key navigation notes for `iceberg-loader`.

---

## Top-Level Layout

```
iceberg-loader/
├── src/                          # Source code (package: iceberg_loader)
│   └── iceberg_loader/
│       ├── __init__.py           # Public API re-exports
│       ├── __about__.py         # Version string
│       ├── iceberg_loader.py    # Backward-compat shim
│       ├── core/                 # Core architecture
│       │   ├── __init__.py       # Re-exports public core symbols
│       │   ├── config.py         # LoaderConfig, TABLE_PROPERTIES
│       │   ├── conform.py        # BatchConformer: conform batch buffers to table
│       │   ├── loader.py         # IcebergLoader facade
│       │   ├── partitioning.py   # Partition string parser
│       │   ├── schema.py         # SchemaManager
│       │   └── strategies.py     # Write strategies
│       ├── utils/                # Arrow/Iceberg type utilities
│       └── services/             # Logging & maintenance
├── tests/                        # Test suite (pytest)
├── examples/                     # Usage examples + compose files
├── docs/                         # Documentation
│   ├── CODEMAPS/                # Generated architecture/module/file maps
│   ├── examples.md              # Examples guide
│   └── index.md                 # Home page
├── pyproject.toml               # Project config, deps, tools
├── README.md                     # Overview
├── RELEASE.md                    # Release checklist
├── CONTRIBUTING.md               # Contributor guide
├── mkdocs.yml                    # MkDocs config
├── Docker / Compose files        # examples/docker-compose.yml
└── .pre-commit-config.yaml       # Pre-commit hooks
```

---

## Source Files (`src/iceberg_loader/`)

### Package Root

| File | Purpose |
|------|---------|
| `__init__.py` | Stable public API. Re-exports `IcebergLoader`, `LoaderConfig`, load functions, logger, `expire_snapshots`, `get_rest_catalog`, `__version__`. |
| `__about__.py` | Single source of truth for version: `__version__ = '0.1.4'`. Must match `pyproject.toml`. |
| `py.typed` | PEP 561 marker — tells type checkers this package is typed. |
| `iceberg_loader.py` | Backward-compat shim: aliases `IcebergLoader = CoreIcebergLoader` and re-defines duplicate wrapper functions, kept so external code importing from `iceberg_loader.iceberg_loader` instead of the package root keeps working. |
| `catalog.py` | **`get_rest_catalog()`** — factory building a PyIceberg `RestCatalog` from explicit args or env vars (`ICEBERG_REST_URI`, `S3_*`). |

---

### Core Layer (`src/iceberg_loader/core/`)

| File | Purpose |
|------|---------|
| `__init__.py` | Re-exports all public core symbols (classes, functions, `TABLE_PROPERTIES`). |
| `loader.py` | **`IcebergLoader`** facade. Central orchestration: buffering, write delegation. Entry point for all ingestion flows. |
| `conform.py` | **`BatchConformer`** — per-load table resolution, load timestamp, schema evolution, type casting. |
| `config.py` | **`LoaderConfig`** frozen Pydantic model. Default `TABLE_PROPERTIES`. Input validation for partition strings, combo rules, column names. |
| `schema.py` | **`SchemaManager`** — table create/load, schema evolution (add columns), Arrow↔Iceberg schema conversion, partition spec creation. |
| `strategies.py` | Strategy pattern for writes: `AppendStrategy`, `OverwriteStrategy`, `IdempotentStrategy`, `UpsertStrategy`, `get_write_strategy()`. |
| `partitioning.py` | Partition string parser (`day(...)`, `bucket(16, id)`, etc.), transform instantiation, type compatibility checks. |

---

### Utilities (`src/iceberg_loader/utils/`)

| File | Purpose |
|------|---------|
| `__init__.py` | Re-exports `arrow` and `types` public functions. |
| `types.py` | **`TypeRegistry`** — bidirectional Arrow↔Iceberg type maps. `get_arrow_type()`, `get_iceberg_type()`, `register_custom_mapping()`. |
| `arrow.py` | Arrow table helpers: `create_arrow_table_from_data()` (JSON serialization), `convert_table_types()` (safe casting), `create_record_batches_from_dicts()`. |

---

### Services (`src/iceberg_loader/services/`)

| File | Purpose |
|------|---------|
| `__init__.py` | Re-exports logging and maintenance public APIs. |
| `logging.py` | Logging setup (`configure_logging`), JSON/text formatters, module-level logger proxy with `__getattr__` forwarding, `metrics()` utility. |
| `maintenance.py` | **`SnapshotMaintenance`** — expire old snapshots by count or age. Free function `expire_snapshots()`. |

---

## Test Files (`tests/`)

| File | Coverage |
|------|----------|
| `conftest.py` | `sql_catalog` fixture: sqlite-backed SqlCatalog per test |
| `test_load_behavior.py` | End-to-end load behavior against a real catalog |
| `test_conform.py` | BatchConformer interface tests |
| `test_iceberg_loader.py` | `IcebergLoader` unit tests (init, public API wrappers, schema field-id preservation) |
| `test_config_validation.py` | `LoaderConfig` validators (partition strings, combo errors, defaults) |
| `test_partitioning.py` | `partitioning.py` functions (parsing, transform instantiation) |
| `test_arrow_utils.py` | `arrow.py` helpers (table creation, type conversion, batch generation) |
| `test_type_mappings.py` | `types.py` mapping round-trips, custom mappings, unsupported types |
| `test_maintenance.py` | Snapshot expiration logic |
| `test_streaming.py` | IPC stream and batch streaming tests |
| `test_examples_smoke.py` | Smoke tests for example scripts |
| `test_catalog.py` | `get_rest_catalog()` factory (env/arg resolution, S3 properties, path-style access) |

---

## Example Files (`examples/`)

| File | Purpose |
|------|---------|
| `README.md` | Guide to running examples |
| `catalog.py` | PyIceberg catalog setup helpers |
| `load_from_api.py` | Basic API usage |
| `load_stream.py` | Streaming batch load |
| `load_upsert.py` | Upsert / merge-into example |
| `load_complex_json.py` | Dict → JSON string serialization demo |
| `load_timestamp_partitioning.py` | `_load_dttm` + time-transform partition example |
| `load_with_commits.py` | Commit interval / batch buffering demo |
| `maintenance_example.py` | Snapshot expiration example |
| `rest_adapter.py` | HTTP adapter for fetching demo data from a REST API (`requests.get`); unrelated to Iceberg REST catalogs. |
| `rest_catalog_example.py` | REST Catalog setup example (Tabular, Polaris, self-hosted) using `get_rest_catalog()` / `RestCatalog`. |
| `settings.py` | Shared example settings |
| `advanced_scenarios.py` | Combined advanced usage |
| `docker-compose.yml` | Trino + Hive Metastore sandbox |
| `trino-etc/` | Trino configuration for the sandbox |

---

## Key Configuration Files

| File | Tool | Purpose |
|------|------|---------|
| `pyproject.toml` | `uv`, `build`, `ruff`, `tox`, `pytest`, `coverage` | Project metadata, dependencies, lint rules, test config, tox envs |
| `.pre-commit-config.yaml` | `pre-commit` | Git hooks for lint/format enforcement |
| `mkdocs.yml` | `mkdocs` | Documentation site build config |

---

## Navigation Quick-Reference

| Task | Go To |
|------|-------|
| Public API entry | `src/iceberg_loader/__init__.py` |
| Add new write mode | `src/iceberg_loader/core/strategies.py` |
| Change default table format | `src/iceberg_loader/core/config.py` → `TABLE_PROPERTIES` |
| Add new type mapping | `src/iceberg_loader/utils/types.py` → `TypeRegistry.register_custom_mapping()` |
| Change batch buffering logic | `src/iceberg_loader/core/loader.py` → `IcebergLoader.load_data_batches()` |
| Add logger format | `src/iceberg_loader/services/logging.py` → `JsonFormatter` / `TextFormatter` |
| Add maintenance utility | `src/iceberg_loader/services/maintenance.py` → `SnapshotMaintenance` |
| Configure REST catalog | `src/iceberg_loader/catalog.py` → `get_rest_catalog()` |
| Add test | `tests/test_<module>.py` |
| Add example | `examples/<name>.py` + update `docs/examples.md` |
