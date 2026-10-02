"""REST Catalog example for iceberg-loader.

This example demonstrates how to use iceberg-loader with a REST Catalog
(Tabular, Polaris, or any self-hosted Iceberg REST Catalog server).

Prerequisites:
    A running REST Catalog server and MinIO/S3 for storage.

Quick setup with the local docker-compose stack + a REST catalog:
    cd examples
    docker compose up -d   # starts MinIO + Hive Metastore + Trino

    # Then point to your REST Catalog server via env vars:
    export ICEBERG_REST_URI="https://api.tabular.io/ws"
    export ICEBERG_WAREHOUSE="s3://my-bucket/warehouse/"
    export ICEBERG_CREDENTIAL="your-oauth2-credential"

    # S3 credentials (use defaults for local MinIO):
    export S3_ENDPOINT="http://localhost:9000"
    export S3_ACCESS_KEY="minio"
    export S3_SECRET_KEY="minio123"
    export S3_REGION="us-east-1"

    uv run python rest_catalog_example.py

Usage:
    uv run python rest_catalog_example.py
"""

import logging

import pyarrow as pa
from pyiceberg.exceptions import NoSuchTableError

from iceberg_loader import LoaderConfig, get_rest_catalog, load_data_to_iceberg

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def run_example() -> None:
    """Demonstrate basic ETL with REST Catalog."""
    catalog = get_rest_catalog()
    table_id = ('default', 'rest_catalog_example')

    # Cleanup any previous run
    try:
        catalog.drop_table(table_id)
        logger.info('Dropped existing table %s', table_id)
    except NoSuchTableError:
        pass

    # 1. Create table and append data
    logger.info('--- Step 1: Initial load ---')
    data = pa.Table.from_pydict({
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'score': [95.5, 87.0, 92.3],
        'signup_date': ['2023-01-01', '2023-01-01', '2023-01-15'],
    })

    config = LoaderConfig(
        write_mode='append',
        partition_col='day(signup_date)',
        schema_evolution=True,
    )
    result = load_data_to_iceberg(data, table_id, catalog, config=config)
    logger.info('Initial load result: %s', result)

    # 2. Read back and verify
    table = catalog.load_table(table_id)
    rows = table.scan().to_arrow()
    logger.info('Rows after initial load: %d', len(rows))
    logger.info('Data:\n%s', rows.to_pydict())

    # 3. Append more data (new partition)
    logger.info('--- Step 2: Append new partition ---')
    new_data = pa.Table.from_pydict({
        'id': [4, 5],
        'name': ['Diana', 'Eve'],
        'score': [88.0, 79.5],
        'signup_date': ['2023-02-01', '2023-02-01'],
    })

    result2 = load_data_to_iceberg(new_data, table_id, catalog, config=config)
    logger.info('Append result: %s', result2)

    table = catalog.load_table(table_id)
    rows = table.scan().to_arrow()
    logger.info('Total rows after append: %d', len(rows))
    logger.info('Final data:\n%s', rows.to_pydict())

    # 4. Show table metadata
    logger.info('--- Table metadata ---')
    logger.info('  Location: %s', table.location())
    logger.info('  Schema: %s', table.schema())
    logger.info('  Partition spec: %s', table.spec())
    snapshots = list(table.snapshots())
    logger.info('  Snapshots: %d', len(snapshots))

    # 5. Cleanup
    logger.info('--- Cleanup ---')
    catalog.drop_table(table_id)
    logger.info('Dropped table %s', table_id)


if __name__ == '__main__':
    run_example()
