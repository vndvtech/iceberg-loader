import logging
import sys
from pathlib import Path

from catalog import get_catalog
from optional.rest_adapter import RestAdapter

try:
    from iceberg_loader import LoaderConfig, load_data_to_iceberg
    from iceberg_loader.arrow_utils import create_arrow_table_from_data
except ImportError:  # fallback for local src run
    sys.path.append(str(Path(__file__).resolve().parents[2] / 'src'))
    from iceberg_loader import LoaderConfig, load_data_to_iceberg
    from iceberg_loader.arrow_utils import create_arrow_table_from_data

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def main():
    catalog = get_catalog()
    adapter = RestAdapter()
    config = LoaderConfig(write_mode='overwrite', schema_evolution=True)

    endpoint_list = ['customers', 'orders', 'items', 'products', 'supplies', 'stores']

    for endpoint in endpoint_list:
        logger.info('Loading data from endpoint: %s', endpoint)

        endpoint_data_generator = adapter.get_data(endpoint)

        endpoint_data = []
        for batch in endpoint_data_generator:
            endpoint_data.extend(batch if isinstance(batch, list) else [batch])

        logger.info('Fetched %d records from %s', len(endpoint_data), endpoint)

        arrow_table = create_arrow_table_from_data(endpoint_data)

        result = load_data_to_iceberg(
            table_data=arrow_table,
            table_identifier=('default', endpoint),
            catalog=catalog,
            config=config,
        )

        logger.info('Loaded endpoint %s: %s', endpoint, result)


if __name__ == '__main__':
    main()
