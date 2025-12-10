import logging
import sys
from pathlib import Path

from catalog import get_catalog

try:
    from iceberg_loader import expire_snapshots
except ImportError:  # fallback for local src run
    sys.path.append(str(Path(__file__).resolve().parents[2] / 'src'))
    from iceberg_loader import expire_snapshots

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)


def main() -> None:
    catalog = get_catalog()
    table_id = ('default', 'advanced_s1_initial_append')

    logger.info('Running snapshot maintenance for table: %s', table_id)
    table = catalog.load_table(table_id)

    expire_snapshots(table, keep_last=2)


if __name__ == '__main__':
    main()
