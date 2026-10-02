"""REST Catalog example for iceberg-loader.

This example demonstrates how to use iceberg-loader with an Iceberg REST
Catalog. By default it targets the local Apache Polaris + MinIO stack that
ships with ``examples/docker-compose.yml``, so it runs out of the box:

    cd examples
    docker compose up -d   # MinIO + Hive Metastore + Trino + Polaris
    uv run python rest_catalog_example.py

Every connection setting can be overridden with environment variables to
point at a remote catalog (Tabular, a self-hosted Polaris, etc.):

    ICEBERG_REST_URI          REST Catalog endpoint (default: http://localhost:8181/api/catalog)
    ICEBERG_WAREHOUSE         Catalog name (default: datalake)
    ICEBERG_CREDENTIAL        OAuth2 credential (default: root:root)
    ICEBERG_OAUTH2_SERVER_URI OAuth2 token endpoint (default: <REST_URI>/v1/oauth/tokens)
    S3_ENDPOINT               S3 endpoint (default: http://localhost:9000)
    S3_ACCESS_KEY             S3 access key (default: minio)
    S3_SECRET_KEY             S3 secret key (default: minio123)
    S3_REGION                 S3 region (default: us-east-1)

Usage:
    uv run python rest_catalog_example.py
"""

import logging
import os

import pyarrow as pa
from pyiceberg.exceptions import NoSuchTableError

from iceberg_loader import LoaderConfig, get_rest_catalog, load_data_to_iceberg

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

REST_URI = os.environ.get('ICEBERG_REST_URI', 'http://localhost:8181/api/catalog')
OAUTH2_SERVER_URI = os.environ.get('ICEBERG_OAUTH2_SERVER_URI', f'{REST_URI}/v1/oauth/tokens')


def build_catalog():
    """Build a REST Catalog client for the local Polaris stack (env-overridable)."""
    return get_rest_catalog(
        uri=REST_URI,
        warehouse=os.environ.get('ICEBERG_WAREHOUSE', 'datalake'),
        credential=os.environ.get('ICEBERG_CREDENTIAL', 'root:root'),
        s3_endpoint=os.environ.get('S3_ENDPOINT', 'http://localhost:9000'),
        s3_access_key=os.environ.get('S3_ACCESS_KEY', 'minio'),
        s3_secret_key=os.environ.get('S3_SECRET_KEY', 'minio123'),
        s3_region=os.environ.get('S3_REGION', 'us-east-1'),
        extra_properties={
            'scope': 'PRINCIPAL_ROLE:ALL',
            'oauth2-server-uri': OAUTH2_SERVER_URI,
            # PyIceberg requests vended credentials by default, which Polaris
            # rejects for a plain root principal. An empty value disables the
            # delegation header so the static S3 credentials above are used.
            'header.X-Iceberg-Access-Delegation': '',
        },
    )


def run_example() -> None:
    """Demonstrate basic ETL with REST Catalog."""
    catalog = build_catalog()
    table_id = ('default', 'rest_catalog_example')

    # Polaris does not auto-create namespaces, so ensure "default" exists first.
    catalog.create_namespace_if_not_exists('default')

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
