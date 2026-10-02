from pathlib import Path

import pytest
from pyiceberg.catalog.sql import SqlCatalog


@pytest.fixture()
def sql_catalog(tmp_path: Path) -> SqlCatalog:
    """Real Iceberg catalog backed by sqlite and a local warehouse; isolated per test."""
    catalog = SqlCatalog(
        'test',
        uri=f'sqlite:///{tmp_path}/catalog.db',
        warehouse=f'file://{tmp_path}/warehouse',
    )
    catalog.create_namespace('default')
    return catalog
