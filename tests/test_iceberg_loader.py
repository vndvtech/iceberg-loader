from datetime import date
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from iceberg_loader.core.config import TABLE_PROPERTIES
from iceberg_loader.iceberg_loader import IcebergLoader, load_data_to_iceberg


@pytest.fixture()
def mock_catalog() -> MagicMock:
    return MagicMock()


@pytest.fixture()
def table_identifier() -> tuple[str, str]:
    return ('default', 'test_table')


@pytest.fixture()
def arrow_schema() -> pa.Schema:
    return pa.schema([pa.field('id', pa.int64()), pa.field('name', pa.string()), pa.field('date_col', pa.date32())])


@pytest.fixture()
def arrow_table(arrow_schema: pa.Schema) -> pa.Table:
    return pa.Table.from_pydict(
        {'id': [1, 2], 'name': ['a', 'b'], 'date_col': [date(2023, 1, 1), date(2023, 1, 2)]},
        schema=arrow_schema,
    )


@pytest.fixture()
def loader(mock_catalog: MagicMock) -> IcebergLoader:
    return IcebergLoader(mock_catalog)


def test_init_default_properties(loader: IcebergLoader) -> None:
    assert loader.table_properties == TABLE_PROPERTIES


def test_init_custom_properties(mock_catalog: MagicMock) -> None:
    custom_props = {'write.format.default': 'orc', 'new.prop': 'value'}
    loader = IcebergLoader(mock_catalog, table_properties=custom_props)
    expected_props = TABLE_PROPERTIES.copy()
    expected_props.update(custom_props)
    assert loader.table_properties == expected_props


def test_public_api_wrapper(arrow_table: pa.Table, table_identifier: tuple[str, str], mock_catalog: MagicMock) -> None:
    with patch('iceberg_loader.iceberg_loader.IcebergLoader') as mock_loader_cls:
        mock_instance = mock_loader_cls.return_value
        mock_instance.load_data.return_value = {'status': 'ok'}

        load_data_to_iceberg(arrow_table, table_identifier, mock_catalog)

        mock_loader_cls.assert_called_with(mock_catalog, default_config=None)
        mock_instance.load_data.assert_called_once()


def test_field_ids_preserved_on_evolution(loader: IcebergLoader, arrow_schema: pa.Schema) -> None:
    base_schema = loader.schema_manager._arrow_to_iceberg(arrow_schema)
    extended_arrow = pa.schema(
        [
            pa.field('id', pa.int64()),
            pa.field('name', pa.string()),
            pa.field('date_col', pa.date32()),
            pa.field('extra', pa.string()),
        ],
    )
    evolved = loader.schema_manager._arrow_to_iceberg(extended_arrow, existing_schema=base_schema)
    ids = {field.name: field.field_id for field in evolved.fields}
    assert ids['id'] == base_schema.find_field('id').field_id
    assert ids['name'] == base_schema.find_field('name').field_id
    assert ids['date_col'] == base_schema.find_field('date_col').field_id
    assert ids['extra'] > max(field.field_id for field in base_schema.fields)
