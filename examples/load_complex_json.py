import json
import logging

from pyiceberg.catalog.hive import HiveCatalog

from iceberg_loader import load_data_to_iceberg
from iceberg_loader.arrow_utils import create_arrow_table_from_data

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_catalog():
    s3_properties = {
        's3.endpoint': 'http://localhost:9000',
        's3.access-key-id': 'minio',
        's3.secret-access-key': 'minio123',
        's3.path-style-access': 'true',
        's3.region': 'us-east-1',
        'py-io-impl': 'pyiceberg.io.fsspec.FsspecFileIO',
    }

    return HiveCatalog(
        name='default',
        uri='thrift://localhost:9083',
        warehouse='s3://datalake/warehouse',
        **s3_properties,
    )


def drop_if_exists(catalog, table_id):
    try:
        catalog.drop_table(table_id)
        logger.info('Dropped existing table %s', table_id)
    except Exception:
        pass


def run_complex_load():
    catalog = get_catalog()
    table_id = ('default', 'complex_json_test')
    drop_if_exists(catalog, table_id)

    data = [
        {'id': 1, 'complex_field': {'a': 1, 'b': 'nested'}},
        {'id': 2, 'complex_field': {'a': 2, 'b': 'another', 'c': [1, 2]}},
        {'id': 3, 'complex_field': [1, 2, 3]},
    ]

    logger.info('Original Data:')
    for row in data:
        logger.info(row)

    logger.info('\nConverting to Arrow using create_arrow_table_from_data...')
    arrow_table = create_arrow_table_from_data(data)

    logger.info('Arrow Schema:')
    logger.info(arrow_table.schema)

    logger.info('Arrow Data (first 3 rows):')
    logger.info(arrow_table.to_pydict())

    logger.info('\nLoading to Iceberg...')
    load_data_to_iceberg(
        table_data=arrow_table, table_identifier=table_id, catalog=catalog, write_mode='append', schema_evolution=True
    )

    logger.info('\nVerifying data in Iceberg...')
    table = catalog.load_table(table_id)
    result_arrow = table.scan().to_arrow()
    result_data = result_arrow.to_pylist()

    for row in result_data:
        logger.info(f'Row: {row}')
        # Check if complex_field is a string
        val = row['complex_field']
        if isinstance(val, str):
            logger.info(f'  -> complex_field is STRING as expected. Parsed: {json.loads(val)}')
        else:
            logger.error(f'  -> complex_field is NOT a string! Type: {type(val)}')


if __name__ == '__main__':
    run_complex_load()
