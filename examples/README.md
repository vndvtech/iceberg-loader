# Examples

This directory contains runnable examples demonstrating various features of `iceberg-loader`.

## Prerequisites

You need a running Iceberg catalog and MinIO/S3.
A `docker-compose.yml` is provided to spin up a local Hive Metastore + MinIO environment.

```bash
cd examples
docker compose up -d
```

For the REST Catalog example (`rest_catalog_example.py`), you need a REST Catalog server
(Tabular, Polaris, or self-hosted). Set these environment variables:

```bash
export ICEBERG_REST_URI="https://your-rest-catalog-server.com/api"
export ICEBERG_WAREHOUSE="s3://my-bucket/warehouse/"
export ICEBERG_CREDENTIAL="your-oauth2-credential"
export S3_ENDPOINT="http://localhost:9000"
export S3_ACCESS_KEY="minio"
export S3_SECRET_KEY="minio123"
export S3_REGION="us-east-1"
```

Alternatively, copy `examples/pyiceberg.yaml.sample` to `~/.pyiceberg.yaml`, adjust the values,
and use `load_catalog("my-rest-catalog")` instead of the direct constructor.

Trino connection:

```
host: localhost
port: 8080
database: iceberg
username: trion
password: <empty>
```

MinIO:

```
endpoint: http://localhost:9001
access_key: minio
secret_key: minio123
```

## Install dependencies with UV

```bash
uv init --python3.14
uv add "iceberg-loader[all]"
```

## Running Examples

Run from the `examples/` directory with `uv`:

```bash
# REST Catalog example
uv run python rest_catalog_example.py

# Upsert
uv run python load_upsert.py

# Commit interval for long streams
uv run python load_with_commits.py

# Messy JSON: PyArrow failure vs iceberg-loader success
uv run python compare_complex_json_fail.py

# Advanced scenarios (schema evolution, types, partitioning)
uv run python advanced_scenarios.py
```

Other examples:

```bash
# Arrow IPC stream loading
uv run python load_stream.py

# Simulated REST API loading
uv run python load_from_api.py

# Maintenance: expiring snapshots
uv run python maintenance_example.py
```

Run the local smoke subset:

```bash
bash ../tools/run_examples_smoke.sh
```

This smoke suite covers the fast local examples that only depend on the bundled Docker stack.
It intentionally skips:

- `rest_catalog_example.py`: requires a REST Catalog server (Tabular, Polaris, or self-hosted).
- `load_from_api.py`: depends on an external public API.
- `load_stream.py`: writes a very large in-memory IPC stream and is too heavy for routine smoke checks.

## Example summary

- `rest_catalog_example.py`: REST Catalog connection, table creation, append, read-back.
- `load_upsert.py`: upsert by keys.
- `load_with_commits.py`: commit_interval for streams.
- `compare_complex_json_fail.py`: PyArrow fails on mixed types, `iceberg-loader` succeeds.
- `advanced_scenarios.py`: schema evolution, custom types, partitioning.
- `load_stream.py`: Arrow IPC stream.
- `load_from_api.py`: REST API batches.
- `maintenance_example.py`: snapshot expiration.
