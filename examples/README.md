# iceberg-loader Examples

This directory contains executable examples demonstrating the capabilities of `iceberg-loader`.

## Prerequisites

The examples require a running MinIO (S3-compatible storage) and Hive Metastore. A `docker-compose.yml` is provided to set up this infrastructure locally.

1. **Start the infrastructure:**
   ```bash
   docker-compose up -d
   ```
   This will start:
   - MinIO (S3) on http://localhost:9000
   - Hive Metastore on thrift://localhost:9083
   - Postgres (backend for Hive)

2. **Install dependencies (uv or pip, with TestPyPI while WIP):**
   ```bash
   uv venv .venv && source .venv/bin/activate
   uv pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple "iceberg-loader[all]==0.0.1"
   # or
   python3.12 -m venv .venv && source .venv/bin/activate
   pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple "iceberg-loader[all]==0.0.1"
   ```

## Shared Modules

The examples use shared modules for common functionality:
- **`catalog.py`**: Iceberg catalog configuration (connects to local Hive Metastore)
- **`settings.py`**: Configuration with defaults for local MinIO/Hive setup
- **`rest_adapter.py`**: REST API client for fetching external data

All examples import these modules to avoid code duplication.

## Running Examples

You can run the examples from the project root using `uv run` (or plain `python` with the venv activated).

### 1. Basic Load (`load_example.py`)
Demonstrates the simplest flow: creating a table and appending data.

```bash
uv run python examples/load_example.py
```

### 2. Advanced Scenarios (`advanced_scenarios.py`)
A comprehensive suite demonstrating key features:
- **Initial Load**: Creating a partitioned table.
- **Partitioning**: Adding data to new partitions.
- **Idempotency**: Safely reloading data for a specific partition (overwrite/replace) using `replace_filter`.
- **Schema Evolution**: Automatically adding new columns when data structure changes.
- **Full Overwrite**: Replacing the entire table content.

```bash
uv run python examples/advanced_scenarios.py
```

### 3. Complex JSON Handling (`load_complex_json.py`)
Demonstrates how `iceberg-loader` handles "messy" or complex nested data that typically breaks standard PyArrow/Iceberg ingestion.
- Automatically serializes nested Dictionaries and Lists into JSON strings.
- Handles mixed types (e.g., a field being a Dict in one row and a List in another).

```bash
uv run python examples/load_complex_json.py
```

### 4. IPC Stream Loading (`load_stream.py`)
Demonstrates loading data from an Apache Arrow IPC stream source.

```bash
uv run python examples/load_stream.py
```

### 5. Memory-Efficient Batch Loading (`load_with_commits.py`)
Demonstrates using `commit_interval` to periodically commit transactions during a long running batch load. This prevents memory issues by clearing metadata periodically.

```bash
uv run python examples/load_with_commits.py
```

### 6. REST API Integration (`load_from_api.py`)
Real-world example that loads data from a REST API into Iceberg tables.

Features:
- REST API data fetching with error handling (using `rest_adapter.py`)
- Structured logging for production monitoring
- Schema evolution for API changes
- Batch processing of multiple endpoints

```bash
uv run python examples/load_from_api.py
```

Note: This example uses the Jaffle Shop demo API and loads data into 6 tables (customers, orders, items, products, supplies, stores).

### 6. Snapshot Maintenance (`maintenance_example.py`)
Expire old snapshots in a table (keep last N or older_than_ms):

```bash
uv run python examples/maintenance_example.py
```

## Cleanup

To stop and remove the local infrastructure:

```bash
docker-compose down
```

