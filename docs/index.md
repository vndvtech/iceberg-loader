# iceberg-loader

`iceberg-loader` wraps [PyIceberg](https://py.iceberg.apache.org/) to load PyArrow data into Apache Iceberg tables. It handles mixed JSON fields, schema evolution, partition replacement, upserts, batches, and IPC streams.

> **Status:** Actively developed and under testing. PRs are welcome!
> Tested against Hive Metastore and REST Catalog (Tabular, Polaris, self-hosted).

## Features

- **Arrow-first:** `pa.Table`, `RecordBatch`, IPC.
- **Messy JSON friendly:** dict/list/mixed → JSON strings.
- **Schema evolution** (opt-in).
- **Idempotent replace** (`replace_filter`) and **upsert**.
- **Commit interval** for long streams.
- **Maintenance helpers** (expire snapshots).

## Benchmark

[iceberg-loader-benchmark](https://github.com/vndv/iceberg-loader-benchmark) compares iceberg-loader with the [dlt](https://github.com/dlt-hub/dlt) Iceberg destination. Both write the same Arrow data through a Polaris REST Catalog and MinIO. On a 1 GiB NYC 311 snapshot (1.72M rows), iceberg-loader had a median write time of 3.7 s and a median peak RSS of 1.36 GiB; dlt took 11.4 s and 2.26 GiB. The repo covers the methodology, raw data, and how to reproduce the run.

## Install

```bash
pip install "iceberg-loader[all]"
```

Or with [uv](https://docs.astral.sh/uv/):

```bash
uv pip install "iceberg-loader[all]"
```

### Extras

| Extra | Description |
|-------|-------------|
| `hive` | Hive Metastore support |
| `s3` | S3 filesystem support |
| `rest` | REST Catalog support (Tabular, Polaris, self-hosted) |
| `all` | All extras |

## Compatibility

- Python: 3.10, 3.11, 3.12, 3.13, 3.14
- PyArrow: >= 18.0.0
- PyIceberg: >= 0.7.1

---

## Quickstart

```python
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from iceberg_loader import LoaderConfig, load_data_to_iceberg

catalog = load_catalog("default")
data = pa.Table.from_pydict({"id": [1, 2], "signup_date": ["2023-01-01", "2023-01-02"]})

config = LoaderConfig(write_mode="append", partition_col="day(signup_date)", schema_evolution=True)
load_data_to_iceberg(data, ("db", "users"), catalog, config=config)
```

---

## Usage

### REST Catalog Setup

You can use REST Catalog via the built-in `get_rest_catalog()` helper, the `RestCatalog` constructor directly, or through a `~/.pyiceberg.yaml` configuration file.

**Option A: get_rest_catalog() helper (recommended)**

```python
from iceberg_loader import get_rest_catalog

catalog = get_rest_catalog(
    uri="https://api.tabular.io/ws",
    warehouse="s3://my-bucket/warehouse/",
    credential="your-oauth2-credential",
    s3_endpoint="https://s3.amazonaws.com",
    s3_region="us-east-1",
)
```

The helper also reads from environment variables — see `help(get_rest_catalog)` for all options.

**Option B: Direct constructor**

```python
import os
from pyiceberg.catalog.rest import RestCatalog

# Only include S3 properties that are actually set, so unset env vars
# don't pass None into RestCatalog.
s3_properties = {
    key: value
    for key, value in {
        "s3.endpoint": os.environ.get("S3_ENDPOINT"),
        "s3.access-key-id": os.environ.get("S3_ACCESS_KEY"),
        "s3.secret-access-key": os.environ.get("S3_SECRET_KEY"),
        "s3.region": os.environ.get("S3_REGION"),
    }.items()
    if value is not None
}

catalog = RestCatalog(
    name="my-rest-catalog",
    uri=os.environ["ICEBERG_REST_URI"],          # e.g. https://api.tabular.io/ws
    warehouse=os.environ["ICEBERG_WAREHOUSE"],   # e.g. s3://my-bucket/warehouse/
    credential=os.environ["ICEBERG_CREDENTIAL"], # OAuth2 credential
    **s3_properties,
)
```

> Prefer `get_rest_catalog()` (Option A) — it handles env-var resolution and
> omits unset properties for you.

**Option C: pyiceberg.yaml + load_catalog()**

```yaml
# ~/.pyiceberg.yaml (see examples/pyiceberg.yaml.sample for a full annotated example)
catalog:
  my-rest-catalog:
    type: rest
    uri: https://api.tabular.io/ws
    warehouse: s3://my-bucket/warehouse/
    credential: your-oauth2-credential
    s3.endpoint: https://s3.amazonaws.com
    s3.region: us-east-1
```

```python
from pyiceberg.catalog import load_catalog

catalog = load_catalog("my-rest-catalog")
```

### Basic Example

```python
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from iceberg_loader import LoaderConfig, load_data_to_iceberg

catalog = load_catalog("default")

data = pa.Table.from_pydict({
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Charlie"],
    "created_at": [1672531200000, 1672617600000, 1672704000000],
    "signup_date": ["2023-01-01", "2023-01-01", "2023-01-02"]
})

config = LoaderConfig(write_mode="append", partition_col="day(signup_date)", schema_evolution=True)
result = load_data_to_iceberg(
    table_data=data,
    table_identifier=("my_db", "my_table"),
    catalog=catalog,
    config=config,
)

print(result)
# {'rows_loaded': 3, 'write_mode': 'append', 'partition_col': 'day(signup_date)', ...}
```

### Idempotent Load (Replace Partition)

Replace data for a specific day without adding duplicate rows:

```python
config = LoaderConfig(
    write_mode="append",
    replace_filter="signup_date == '2023-01-01'",
    partition_col="day(signup_date)",
)

load_data_to_iceberg(table_data=data, table_identifier=("my_db", "my_table"), catalog=catalog, config=config)
```

### Upsert (Merge Into)

Update matching rows and insert new ones using key columns. Requires PyIceberg >= 0.7.1.

```python
config = LoaderConfig(write_mode="upsert", join_cols=["id"])
load_data_to_iceberg(table_data=data, table_identifier=("my_db", "my_table"), catalog=catalog, config=config)
```

### Batch Loading

For large datasets, use `load_batches_to_iceberg` with an iterator of RecordBatches:

```python
from iceberg_loader import load_batches_to_iceberg

def batch_generator():
    for i in range(10):
        yield some_record_batch

result = load_batches_to_iceberg(
    batch_iterator=batch_generator(),
    table_identifier=("my_db", "large_table"),
    catalog=catalog,
    config=LoaderConfig(write_mode="append", commit_interval=100),
)
```

### Stream Loading (Arrow IPC)

Load data directly from an Apache Arrow IPC stream:

```python
from iceberg_loader import load_ipc_stream_to_iceberg

result = load_ipc_stream_to_iceberg(
    stream_source="data.arrow",
    table_identifier=("my_db", "stream_table"),
    catalog=catalog,
    config=LoaderConfig(write_mode="append"),
)
```

### Custom Settings

Override default table properties:

```python
custom_props = {
    'write.parquet.compression-codec': 'snappy',
    'history.expire.min-snapshots-to-keep': 5,
}

config = LoaderConfig(table_properties=custom_props, write_mode="append")
load_data_to_iceberg(..., config=config)
```

### Override Default Table Properties Globally

Built-in defaults live in `iceberg_loader.core.config.TABLE_PROPERTIES` (format version, Parquet compression, commit retries). To tweak them, copy the dictionary, override keys, and pass into `LoaderConfig`:

```python
from pyiceberg.catalog import load_catalog
from iceberg_loader import LoaderConfig, load_data_to_iceberg
from iceberg_loader.core.config import TABLE_PROPERTIES

catalog = load_catalog("default")

custom_properties = {**TABLE_PROPERTIES, "write.parquet.compression-codec": "gzip"}

config = LoaderConfig(
    write_mode="append",
    table_properties=custom_properties,
)

load_data_to_iceberg(table_data, ("default", "events"), catalog, config=config)
```

Because this copies `format-version` into your own `table_properties`, it counts as set explicitly: loading into an existing table of another version logs a warning (see below).

### Timestamp precision and format version

- Iceberg format v1/v2 stores timestamps in microseconds. Nanosecond input (`pa.timestamp('ns')`) is truncated to microseconds; when values actually lose precision, the loader logs a warning naming the column.
- `format-version` in `table_properties` applies only when the loader creates a table. Loading into an existing table never changes its version; if you set `format-version` explicitly and it differs from the table's version, the loader logs a warning. The library default (`format-version: 2`) does not trigger it.
- PyIceberg cannot write format v3 tables yet. Requesting `format-version: 3` for a new table, or loading into an existing v3 table, raises `NotImplementedError` before anything is written.
- A `format-version` that is not a whole number (e.g. `'v2'` or `2.9`) is ignored for existing tables, with a warning.

### Maintenance Helper

```python
from iceberg_loader import expire_snapshots

table = catalog.load_table(("db", "users"))
expire_snapshots(table, keep_last=2)
```

### Adding Load Timestamp

Set `load_timestamp` to add a timestamp column (by default, `_load_dttm`) to every row. You can use it for audit trails or partitioning by load time.

```python
from datetime import datetime

# Will add column '_load_dttm' with current time and partition by hour
config = LoaderConfig(
    write_mode="append",
    load_timestamp=datetime.now(),
    partition_col="hour(_load_dttm)",
)

# You can also customize the column name and day-transform it
config_custom = LoaderConfig(
    write_mode="append",
    load_timestamp=datetime(2025, 1, 1),
    load_ts_col="etl_ts",
    partition_col="day(etl_ts)",
)
```

---

## LoaderConfig Reference

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `write_mode` | `'append'` \| `'overwrite'` \| `'upsert'` | `'overwrite'` | Data write mode |
| `partition_col` | `str \| None` | `None` | Partition column or transform (e.g., `month(ts)`, `bucket(16,id)`); prefer day/hour on timestamps |
| `replace_filter` | `str \| None` | `None` | SQL-style filter for idempotent loads |
| `schema_evolution` | `bool` | `False` | Auto-add new columns |
| `commit_interval` | `int` | `0` | Commit every N batches (0 = single transaction) |
| `join_cols` | `list[str] \| None` | `None` | Merge keys for upsert |
| `table_properties` | `dict \| None` | `None` | Custom Iceberg table properties; `format-version` applies only to new tables |
| `load_timestamp` | `datetime \| None` | `None` | If set, adds `_load_dttm` column with this value |
| `load_ts_col` | `str` | `'_load_dttm'` | Name of the load timestamp column |

---

## API Reference

### `load_batches_to_iceberg()`

Loads a stream of batches into an Iceberg table.

```python
def load_batches_to_iceberg(
    batch_iterator: Iterator[pa.RecordBatch] | pa.RecordBatchReader,
    table_identifier: tuple[str, str],
    catalog: Catalog,
    config: LoaderConfig | None = None,
) -> dict[str, Any]
```

Pass write options **only** through `LoaderConfig`; the table above lists its fields.

#### Return Value

Dictionary with loading results:
```python
{
    'rows_loaded': int,
    'batches_processed': int,
    'write_mode': str,
    'partition_col': str,
    'table_location': str,
    'snapshot_id': int | str,
    'new_table_created': bool,
}
```

#### Usage Examples

**Basic stream loading:**
```python
def generate_batches():
    for i in range(100):
        data = {"id": [i], "value": [f"row_{i}"]}
        yield pa.RecordBatch.from_pydict(data)

result = load_batches_to_iceberg(
    batch_iterator=generate_batches(),
    table_identifier=("db", "table"),
    catalog=catalog,
    config=LoaderConfig(write_mode="append")
)
```

**With commits for memory management:**
```python
result = load_batches_to_iceberg(
    batch_iterator=large_batch_stream,
    table_identifier=("db", "large_table"),
    catalog=catalog,
    config=LoaderConfig(
        commit_interval=50,
        schema_evolution=True
    )
)
```

**Idempotent partition loading:**
```python
result = load_batches_to_iceberg(
    batch_iterator=daily_batches,
    table_identifier=("db", "events"),
    catalog=catalog,
    config=LoaderConfig(
        write_mode="append",
        replace_filter="event_date == '2023-12-09'",
        partition_col="day(event_date)"
    )
)
```

---

## Examples

See [Examples](examples.md) for scripts covering streaming, upserts, schema evolution, and mixed JSON fields.

---

## How we version

- Semantic Versioning from `0.1.x`: MINOR = new compatible features, PATCH = fixes, MAJOR = breaking changes.
- Public API: `LoaderConfig`, `IcebergLoader`, `load_data_to_iceberg`, `load_batches_to_iceberg`, `load_ipc_stream_to_iceberg`, `get_rest_catalog`, `expire_snapshots`; other modules are internal.
- Prefer partition transforms for timestamps (`day(ts)`, `hour(ts)`, `day(_load_dttm)`) to avoid unbounded partition counts.
- LoaderConfig validates partition expressions and forbids unsafe mixes (e.g., `replace_filter` with `upsert`, identity partition on `_load_dttm`).

## Release checklist

- Align versions in `pyproject.toml` and `src/iceberg_loader/__about__.py`.
- Update `RELEASE.md` with highlights/breaking changes.
- Run `uv lock`, commit `uv.lock` if it changes, then verify it with `uv lock --locked`.
- Run lint (`uv run ruff check .`), types (`uv run ty check`), and tests (`uv run python -m pytest`).
- Tag and push (`git tag -a vX.Y.Z -m "Release X.Y.Z"`), let CI publish.

---

## Development

This project uses [uv](https://docs.astral.sh/uv/) for all local workflows.

### Run Tests

```bash
uv run python -m pytest
```

### Linting

```bash
uv run ruff check
uv run ruff format --check
uv run ty check
```

### Release

```bash
uv build
twine check dist/*
twine upload --repository testpypi dist/*
twine upload dist/*
```

---

## License

`iceberg-loader` is distributed under the terms of the [MIT](https://spdx.org/licenses/MIT.html) license.
