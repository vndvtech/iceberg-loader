<p align="center">
  <img src="https://raw.githubusercontent.com/vndvtech/iceberg-loader/main/logo.png" alt="iceberg-loader" width="600">
</p>

# iceberg-loader

[![PyPI - Version](https://img.shields.io/pypi/v/iceberg-loader.svg)](https://pypi.org/project/iceberg-loader)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/iceberg-loader.svg)](https://pypi.org/project/iceberg-loader)
[![PyPI - Downloads](https://img.shields.io/pypi/dm/iceberg-loader.svg)](https://pypi.org/project/iceberg-loader)
[![Total Downloads](https://img.shields.io/pepy/dt/iceberg-loader.svg)](https://pepy.tech/projects/iceberg-loader)
[![CI](https://github.com/vndvtech/iceberg-loader/actions/workflows/ci.yml/badge.svg)](https://github.com/vndvtech/iceberg-loader/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

📚 [Documentation](https://vndvtech.github.io/iceberg-loader/)

iceberg-loader wraps [PyIceberg](https://py.iceberg.apache.org/) to load data into Apache Iceberg tables. It uses PyArrow and handles mixed JSON fields, schema evolution, idempotent replacement, upserts, batching, and streaming.


> **Status:** Actively developed and under testing.
> Tested against Hive Metastore and REST Catalog (Tabular, Polaris, self-hosted).

## Why iceberg-loader?

- **Mixed JSON fields:** converts dict and list values to strings before writing, including fields with mixed types.
- **Schema evolution:** adds columns when enabled and preserves field IDs.
- **Write modes:** append, overwrite, idempotent replacement via `replace_filter`, and upsert.
- **Streaming:** commit intervals, batches, and IPC streams.
- **Configuration:** `LoaderConfig` sets defaults that can be overridden per call.

## Benchmark

[iceberg-loader-benchmark](https://github.com/vndv/iceberg-loader-benchmark) compares iceberg-loader with the [dlt](https://github.com/dlt-hub/dlt) Iceberg destination. Both write the same Arrow data through a Polaris REST Catalog and MinIO. On a 1 GiB NYC 311 snapshot (1.72M rows), iceberg-loader had a median write time of 3.7 s and a median peak RSS of 1.36 GiB; dlt took 11.4 s and 2.26 GiB. The repo covers the methodology, raw data, and how to reproduce the run.

## Install

```bash
pip install "iceberg-loader[all]"
```

Or with [uv](https://docs.astral.sh/uv/):

```bash
uv add "iceberg-loader[all]"
```

## Quickstart

### Hive Metastore

```python
from pyiceberg.catalog import load_catalog
from iceberg_loader import LoaderConfig, load_data_to_iceberg
from iceberg_loader.utils.arrow import create_arrow_table_from_data

catalog = load_catalog("default")
table_id = ("default", "comparison_complex_json")

data = [
    {"id": 1, "complex_field": {"a": 1, "b": "nested"}, "signup_date": "2023-01-01"},
    {"id": 2, "complex_field": {"a": 2, "b": "another", "c": [1, 2]}, "signup_date": "2023-01-02"},
    {"id": 3, "complex_field": [1, 2, 3], "signup_date": "2023-01-02"},
]

arrow_table = create_arrow_table_from_data(data)

config = LoaderConfig(write_mode="append", partition_col="day(signup_date)", schema_evolution=True)
load_data_to_iceberg(arrow_table, table_id, catalog, config=config)
```

### REST Catalog

```python
from iceberg_loader import LoaderConfig, get_rest_catalog, load_data_to_iceberg
from iceberg_loader.utils.arrow import create_arrow_table_from_data

catalog = get_rest_catalog(
    uri="https://api.tabular.io/ws",
    warehouse="s3://my-bucket/warehouse/",
    credential="your-oauth2-credential",
)
table_id = ("default", "my_table")

data = [
    {"id": 1, "name": "Alice", "signup_date": "2023-01-01"},
    {"id": 2, "name": "Bob", "signup_date": "2023-01-02"},
]

arrow_table = create_arrow_table_from_data(data)

config = LoaderConfig(write_mode="append", partition_col="day(signup_date)", schema_evolution=True)
load_data_to_iceberg(arrow_table, table_id, catalog, config=config)
```

Or use `pyiceberg.yaml` to configure your REST catalog and load it by name:

```yaml
# ~/.pyiceberg.yaml (see examples/pyiceberg.yaml.sample for a full annotated example)
catalog:
  my-rest-catalog:
    type: rest
    uri: https://your-catalog-server.com/api
    warehouse: s3://my-bucket/warehouse/
    credential: your-oauth2-credential
    s3.endpoint: https://s3.amazonaws.com
    s3.region: us-east-1
```

```python
from pyiceberg.catalog import load_catalog

catalog = load_catalog("my-rest-catalog")
# ... rest is identical to above
```

## Which function to use?

| Function                     | Use when...                                                  | Input Format                      |
|------------------------------|--------------------------------------------------------------|-----------------------------------|
| `load_data_to_iceberg`       | You have a single `pa.Table` in memory.                      | `pyarrow.Table`                   |
| `load_batches_to_iceberg`    | You have a generator/iterator of batches (memory efficient). | Iterator of `pyarrow.RecordBatch` |
| `load_ipc_stream_to_iceberg` | You are reading from an Arrow IPC stream file/socket.        | File-like object or path          |

## Preparing Data

These helpers convert Python dictionaries to Arrow data, including mixed fields:

```python
from iceberg_loader.utils.arrow import create_arrow_table_from_data, create_record_batches_from_dicts

# 1. Convert list of dicts -> pa.Table
arrow_table = create_arrow_table_from_data(data_list)

# 2. Convert iterator of dicts -> Iterator[pa.RecordBatch]
batches = create_record_batches_from_dicts(data_generator(), batch_size=10000)
```

Alternatively, use standard PyArrow conversion: `pa.Table.from_pylist(data)`.

The helpers serialize dicts and lists as JSON and convert other non-null values to strings too. For example, an integer `id` becomes an Arrow string. If you need to preserve scalar types, prepare a typed Arrow table with PyArrow instead.

For timestamp columns, prefer partition transforms such as `day(ts)` or `hour(ts)`, especially when using `load_timestamp`.

### Timestamp precision and format version

- Iceberg format v1/v2 stores timestamps in microseconds. Nanosecond input (`pa.timestamp('ns')`) is truncated to microseconds; when values actually lose precision, the loader logs a warning naming the column.
- `format-version` in `table_properties` applies only when the loader creates a table. Loading into an existing table never changes its version; if you set `format-version` explicitly and it differs from the table's version, the loader logs a warning. The library default (`format-version: 2`) does not trigger it.
- PyIceberg cannot write format v3 tables yet. Requesting `format-version: 3` for a new table, or loading into an existing v3 table, raises `NotImplementedError` before anything is written.
- A `format-version` that is not a whole number (e.g. `'v2'` or `2.9`) is ignored for existing tables, with a warning.

## Public API & Stability

- Top-level exports include `LoaderConfig`, `IcebergLoader`, `load_data_to_iceberg`, `load_batches_to_iceberg`, `load_ipc_stream_to_iceberg`, `get_rest_catalog`, and `expire_snapshots`.
- Data conversion helpers are available from `iceberg_loader.utils.arrow`. Pass loading options via `LoaderConfig`.
- Avoid legacy positional arguments—use the `config` parameter only.
- LoaderConfig validates partition expressions and rejects unsafe combos (e.g., `replace_filter` with `upsert`, identity partition on `_load_dttm`).

## How we version

- Semantic Versioning starting at `0.1.x`: **MINOR** for compatible features, **PATCH** for fixes, **MAJOR** for breaking API changes.
- Breaking changes to documented APIs are noted in `RELEASE.md`.

## Release checklist

- Bump version in `pyproject.toml` and `src/iceberg_loader/__about__.py` (they must match).
- Update `RELEASE.md` with highlights and breaking notes.
- Run `uv lock`, commit `uv.lock` if it changes, then verify it with `uv lock --locked`.
- Run `uv run ruff check .`, `uv run ty check`, and `uv run python -m pytest`.
- Tag and push (`git tag -a vX.Y.Z ...`), then let CI publish.


## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for setup, coding style, and PR guidelines.

```bash
uv run ruff check . && uv run ruff format --check . && uv run ty check
uv run pytest
```

## Contributors

<a href="https://github.com/vndvtech/iceberg-loader/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=vndvtech/iceberg-loader" />
</a>

Made with [contrib.rocks](https://contrib.rocks).

## License

`iceberg-loader` is distributed under the terms of the [MIT](https://spdx.org/licenses/MIT.html) license.
