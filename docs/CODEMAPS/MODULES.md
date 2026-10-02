# Module Map

Modules in `iceberg-loader`, with their exports and dependencies.

---

## `iceberg_loader` (Public Package)

**Purpose**: Re-exports the public API and module-level logger.

**Location**: `src/iceberg_loader/__init__.py`

**Key Files**:
- `__init__.py` — public re-exports
- `__about__.py` — single `__version__` string (`0.1.4`)
- `iceberg_loader.py` — backward-compat shim (`IcebergLoader = CoreIcebergLoader` plus duplicate wrapper functions) kept so external imports from `iceberg_loader.iceberg_loader` keep working
- `catalog.py` — `get_rest_catalog()` factory for PyIceberg `RestCatalog`

**Dependencies**:
- `iceberg_loader.core`
- `iceberg_loader.services.logging`
- `iceberg_loader.services.maintenance`
- `iceberg_loader.catalog`

**Exports**:
- `IcebergLoader` — orchestrator class
- `LoaderConfig` — immutable configuration
- `load_data_to_iceberg(table, identifier, catalog, config)` — load a `pa.Table`
- `load_batches_to_iceberg(iterator, identifier, catalog, config)` — load batch iterator
- `load_ipc_stream_to_iceberg(stream, identifier, catalog, config)` — load Arrow IPC stream
- `expire_snapshots(table, ...)` — maintenance utility
- `get_rest_catalog(...)` — build a `RestCatalog` from args/env vars
- `logger` — module-level logger proxy

**Usage Example**:
```python
from iceberg_loader import IcebergLoader, LoaderConfig, load_data_to_iceberg
```

---

## `iceberg_loader.catalog` (Catalog Helpers)

**Purpose**: Creates a PyIceberg `RestCatalog` from arguments or environment variables.

**Location**: `src/iceberg_loader/catalog.py`

**Key Files**:
- `get_rest_catalog()` — resolves `uri`/`warehouse`/`credential` and `s3.*` properties, then constructs a `RestCatalog`

**Dependencies**:
- `pyiceberg.catalog.rest` (`RestCatalog`)

**Exports**:
- `get_rest_catalog(name='rest-catalog', *, uri, warehouse, credential, s3_endpoint, s3_access_key, s3_secret_key, s3_region, s3_path_style, extra_properties) -> RestCatalog`
  - Reads `ICEBERG_REST_URI`, `ICEBERG_WAREHOUSE`, `ICEBERG_CREDENTIAL`, `S3_ENDPOINT`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `S3_REGION` when args are omitted
  - Sets `s3.path-style-access='true'` when `s3_path_style=True` or an S3 endpoint is configured
  - `extra_properties` is applied last and overrides any other key (escape hatch)

---

## `iceberg_loader.core` (Core Layer)

**Purpose**: Coordinates loading, writes, schema changes, configuration, and partitioning.

**Location**: `src/iceberg_loader/core/`

### Sub-Modules

#### `loader.py`
**Key Files**:
- `IcebergLoader` — facade; buffers batches, delegates conforming to BatchConformer and writes to WriteStrategy
- `load_data_to_iceberg()` / `load_batches_to_iceberg()` / `load_ipc_stream_to_iceberg()` — thin wrappers

**Dependencies**:
- `core.config` (`LoaderConfig`, `TABLE_PROPERTIES`)
- `core.conform` (`BatchConformer`)
- `core.schema` (`SchemaManager`)
- `core.strategies` (`get_write_strategy`)

**Exports**:
- `IcebergLoader(catalog, table_properties, default_config)`
  - `.load_data(table_data, table_identifier, config)`
  - `.load_data_batches(batch_iterator, table_identifier, config)`
  - `.load_ipc_stream(stream_source, table_identifier, config)`

---

#### `conform.py`
**Key Files**:
- `BatchConformer` — per-load; table resolution/creation, load timestamp, schema evolution, casting

**Dependencies**:
- `core.config` (`LoaderConfig`)
- `core.schema` (`SchemaManager`)
- `utils.arrow` (`convert_table_types`)

**Exports**:
- `BatchConformer(schema_manager, table_identifier, config, table_properties)`
  - `.conform(batches) -> pa.Table`
  - `.table`, `.new_table_created`

---

#### `config.py`
**Key Files**:
- `LoaderConfig` — frozen Pydantic model
- `TABLE_PROPERTIES` — default Iceberg table properties dict
- `ensure_loader_config()` — validate/normalize config helper

**Dependencies**:
- `core.partitioning` (`parse_partition_transform`)

**Exports**:
- `LoaderConfig` — fields: `write_mode`, `partition_col`, `replace_filter`, `schema_evolution`, `table_properties`, `commit_interval`, `join_cols`, `load_timestamp`, `load_ts_col`
- `TABLE_PROPERTIES` — dict with `write.format.default`, `format-version`, etc.

---

#### `schema.py`
**Key Files**:
- `SchemaManager` — table lifecycle and schema conversion

**Dependencies**:
- `pyiceberg.catalog` / `pyiceberg.schema` / `pyiceberg.partitioning`
- `core.partitioning` (`parse_partition_transform`, `get_transform_impl`, etc.)
- `utils.types` (`get_arrow_type`, `get_iceberg_type`)
- `services.logging` (`logger`)

**Exports**:
- `SchemaManager(catalog, table_properties)`
  - `.ensure_table_exists(identifier, arrow_schema, partition_col, table_properties)`
  - `.evolve_schema_if_needed(table, batch_schema)` — adds new columns
  - `.get_arrow_schema(table)` — returns `pa.Schema`

---

#### `strategies.py`
**Key Files**:
- `WriteStrategy` — ABC
- `AppendStrategy` / `OverwriteStrategy` / `IdempotentStrategy` / `UpsertStrategy`
- `get_write_strategy()` — factory function

**Dependencies**:
- `services.logging` (`logger`)

**Exports**:
- `get_write_strategy(write_mode, replace_filter, join_cols)` → `WriteStrategy`

---

#### `partitioning.py`
**Key Files**:
- `parse_partition_transform(partition_str)` → `(transform, source_col, param)`
- `get_transform_impl()` — maps names to PyIceberg transform instances
- `is_timestamp_identity()` / `transform_supports_type()` — validators

**Dependencies**:
- `pyiceberg.transforms` / `pyiceberg.types`

**Exports**:
- `parse_partition_transform(partition_str: str) -> (str, str, int | None)`
- `get_transform_impl(transform_name, param)`

---

## `iceberg_loader.utils` (Utilities)

**Purpose**: Type mappings, Arrow table helpers, and type casting.

**Location**: `src/iceberg_loader/utils/`

### Sub-Modules

#### `types.py`
**Key Files**:
- `TypeRegistry` — bidirectional Arrow↔Iceberg type maps
- `get_arrow_type()` / `get_iceberg_type()` / `register_custom_mapping()`

**Dependencies**:
- `pyarrow` / `pyiceberg.types`

**Exports**:
- `get_iceberg_type(arrow_type: pa.DataType) -> IcebergType`
- `get_arrow_type(iceberg_type: IcebergType) -> pa.DataType`
- `register_custom_mapping(arrow_type, iceberg_type)`

---

#### `arrow.py`
**Key Files**:
- `create_arrow_table_from_data(dicts)` — serializes dicts/lists to JSON strings
- `create_record_batches_from_dicts(iterator, batch_size)` — dicts → `RecordBatch` iterator
- `convert_table_types(table, target_schema)` — safe type casting with null fallback
- `convert_column_type(column, target_type)` — single-column cast

**Dependencies**:
- `pyarrow` / `pyarrow.compute`
- `services.logging` (`logger`)

**Exports**:
- `create_arrow_table_from_data(data: list[dict]) -> pa.Table`
- `create_record_batches_from_dicts(data_iterator, batch_size=10000) -> Iterator[pa.RecordBatch]`
- `convert_table_types(table, target_schema) -> pa.Table`

---

## `iceberg_loader.services` (Services)

**Purpose**: Logging and snapshot maintenance.

**Location**: `src/iceberg_loader/services/`

### Sub-Modules

#### `logging.py`
**Key Files**:
- `configure_logging(level, log_format, component, version)` — initialize global logger
- `logger` — module-level proxy object (via `__getattr__`)
- `metrics()` / `suppress_and_warn()` / `get_logger()` / pretty format helpers

**Dependencies**:
- `pyarrow` (for type introspection in some callers, though not strictly here)

**Exports**:
- `configure_logging(...)`
- `get_logger()`
- `metrics(name, extra)`
- `logger` (via `from iceberg_loader import logger`)

---

#### `maintenance.py`
**Key Files**:
- `SnapshotMaintenance` — class with `.expire_snapshots(table, keep_last, older_than_ms)`
- `expire_snapshots()` — convenience free function

**Dependencies**:
- `services.logging` (`logger`)

**Exports**:
- `SnapshotMaintenance().expire_snapshots(table, keep_last=1, older_than_ms=None)`
- `expire_snapshots(table, keep_last=1, older_than_ms=None)`

---

## Third-Party Dependencies

| Package | Used By |
|---------|---------|
| `pyarrow` (≥18) | `core/loader`, `utils/arrow`, `utils/types` |
| `pyiceberg` (≥0.7.1) | `core/loader`, `core/schema`, `core/strategies`, `core/partitioning`, `services/maintenance` |
| `pydantic` | `core/config` |

---

## Test Matrix

| Module | Primary Test File |
|--------|-------------------|
| `core.loader` | `tests/test_load_behavior.py`, `tests/test_iceberg_loader.py` |
| `core.conform` | `tests/test_conform.py` |
| `core.config` | `tests/test_config_validation.py` |
| `core.partitioning` | `tests/test_partitioning.py` |
| `utils.arrow` | `tests/test_arrow_utils.py` |
| `utils.types` | `tests/test_type_mappings.py` |
| `services.maintenance` | `tests/test_maintenance.py` |
| Streaming / IPC | `tests/test_streaming.py` |
| Examples smoke | `tests/test_examples_smoke.py` |
| `catalog` | `tests/test_catalog.py` |
