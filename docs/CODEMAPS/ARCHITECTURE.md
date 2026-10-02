# Architecture Map

How `iceberg-loader` moves Arrow data through schema handling and write strategies.

---

## System Overview

`IcebergLoader` receives PyArrow data, uses `SchemaManager` to prepare the table, and delegates writes to a strategy for the selected mode.

The main responsibilities are:
- `IcebergLoader` buffers input and coordinates writes.
- `WriteStrategy` selects the behavior for append, overwrite, idempotent replace, or upsert.
- `LoaderConfig` holds validated write options in a frozen Pydantic model.
- `SchemaManager` creates tables and adds columns when schema evolution is enabled.
- `convert_table_types` casts Arrow data to the table schema.

---

## Component Relationships

```
┌──────────────────────────────────────────────────────┐
│           Public API (iceberg_loader)                │
│  load_data_to_iceberg()                               │
│  load_batches_to_iceberg()                            │
│  load_ipc_stream_to_iceberg()                         │
│  IcebergLoader class                                  │
└────────────────────┬─────────────────────────────────┘
                     │ constructs, delegates
┌────────────────────▼─────────────────────────────────┐
│              IcebergLoader (Orchestrator)              │
│  ┌─────────────┐  ┌──────────────┐  ┌──────────────┐ │
│  │ SchemaManager│  │ WriteStrategy│  │ LoaderConfig │ │
│  │ (create/evolve│  │ (append/etc) │  │ (frozen VO)  │ │
│  └─────────────┘  └──────────────┘  └──────────────┘ │
└────────────────────┬───────────────────────────────────┘
                     │ uses
┌────────────────────▼─────────────────────────────────┐
│              PyIceberg + PyArrow                       │
│         Catalog, Table, Snapshots, Schema               │
└────────────────────────────────────────────────────────┘
```

---

## Data Flow

### 1. Single Table Load (`load_data_to_iceberg`)

```
pa.Table ──► split to RecordBatches ──► IcebergLoader.load_data() ──► load_data_batches() ──► WriteStrategy.write()
```

### 2. Streaming Batch Load (`load_batches_to_iceberg`)

```
Iterator[RecordBatch] ──► buffer up to commit_interval ──► BatchConformer.conform() ──► WriteStrategy.write()
```

### 3. IPC Stream Load (`load_ipc_stream_to_iceberg`)

```
IPC stream file/socket ──► pa.ipc.open_stream() ──► RecordBatchReader ──► load_data_batches()
```

### 4. Internal Batch Processing (`BatchConformer.conform`)

```
buffer of RecordBatches
       │
       ├─ add load_timestamp column (config.load_timestamp)
       ├─ without schema_evolution: concat (mixed schemas → pa.ArrowInvalid)
       ├─ ensure table exists (first call only; partition spec sees the load timestamp column)
       ├─ evolve: load timestamp column; with schema_evolution, every distinct batch schema
       └─ convert_table_types() — cast to target Arrow schema
then IcebergLoader: strategy.write(conformer.table, data, is_first_write)
```

---

## Core Components

### `IcebergLoader`
**Role**: Facade / orchestrator. Manages catalog, config resolution, buffering, and delegates schema + write operations.

### `BatchConformer`
**Role**: Per-load conform step. Owns the target table and turns each batch buffer into a table-shaped `pa.Table`.

### `SchemaManager`
**Role**: Table lifecycle + schema evolution.
- `ensure_table_exists()` — load or create table with partition spec
- `evolve_schema_if_needed()` — add missing top-level columns
- `_arrow_to_iceberg()` / `_iceberg_to_arrow()` — bidirectional schema conversion

### `WriteStrategy`
**Role**: Abstract interface for writing data.
- `AppendStrategy` — simple append
- `OverwriteStrategy` — overwrite on first batch, then append
- `IdempotentStrategy` — delete rows via `replace_filter`, then append
- `UpsertStrategy` — PyIceberg `table.upsert()` merge-into

### `LoaderConfig`
**Role**: Immutable configuration model. Validates:
- partition expressions (`day(ts)`, `bucket(16, id)`)
- mutually exclusive options (`upsert` + `replace_filter`)
- identity partition on `_load_dttm` column (rejected)

---

## Dependency Direction

```
Public API ──► IcebergLoader ──► core: strategies, schema, config
                IcebergLoader ──► utils: arrow (convert_table_types)
                IcebergLoader ──► services: logging
                core/         ──► services: logging
                core/         ──► utils: types, partitioning
                utils/        ──► services: logging
                services/     ◄── (logging and maintenance are bottom)
```

---

## Entry & Exit Points

| Entry Point | Purpose | Returns |
|-------------|---------|---------|
| `load_data_to_iceberg(table, ...)` | One-shot in-memory table | `dict` with rows_loaded, snapshot_id, etc. |
| `load_batches_to_iceberg(iterator, ...)` | Streaming batches | `dict` |
| `load_ipc_stream_to_iceberg(stream, ...)` | IPC stream source | `dict` |
| `IcebergLoader.load_data()` | Stateful reuse | `dict` |
| `IcebergLoader.load_data_batches()` | Batch orchestration | `dict` |
| `expire_snapshots(table, ...)` | Maintenance utility | `None` |
| `iceberg_loader.services.logging.configure_logging(...)` | Initialize logger (not top-level exported) | `Logger` |
