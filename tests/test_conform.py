from datetime import datetime
from typing import Any

import pyarrow as pa
import pytest
from pyiceberg.catalog.sql import SqlCatalog

from iceberg_loader import LoaderConfig, load_data_to_iceberg
from iceberg_loader.core.config import TABLE_PROPERTIES
from iceberg_loader.core.conform import BatchConformer
from iceberg_loader.core.schema import SchemaManager

TID = ('default', 'events')


def make_conformer(catalog: SqlCatalog, config: LoaderConfig) -> BatchConformer:
    properties = TABLE_PROPERTIES.copy()
    return BatchConformer(SchemaManager(catalog, properties), TID, config, properties)


def batch(**columns: list[Any]) -> pa.RecordBatch:
    return pa.RecordBatch.from_pydict(columns)


def test_first_conform_creates_table_and_marks_it_new(sql_catalog: SqlCatalog) -> None:
    conformer = make_conformer(sql_catalog, LoaderConfig(write_mode='append'))
    assert conformer.table is None

    data = conformer.conform([batch(id=[1, 2])])

    assert sql_catalog.table_exists(TID)
    assert conformer.table is not None
    assert conformer.new_table_created is True
    assert data.to_pylist() == [{'id': 1}, {'id': 2}]


def test_existing_table_with_data_is_not_marked_new(sql_catalog: SqlCatalog) -> None:
    load_data_to_iceberg(pa.table({'id': [1]}), TID, sql_catalog, LoaderConfig(write_mode='append'))
    conformer = make_conformer(sql_catalog, LoaderConfig(write_mode='append'))

    conformer.conform([batch(id=[2])])

    assert conformer.new_table_created is False


def test_output_matches_table_schema(sql_catalog: SqlCatalog) -> None:
    load_data_to_iceberg(pa.table({'id': [1], 'v': ['a']}), TID, sql_catalog, LoaderConfig(write_mode='append'))
    conformer = make_conformer(sql_catalog, LoaderConfig(write_mode='append'))

    data = conformer.conform([batch(id=pa.array([2], pa.int32()), zzz=['dropped'])])

    assert data.schema.names == ['id', 'v']
    assert data.schema.field('id').type == pa.int64()
    assert data.to_pylist() == [{'id': 2, 'v': None}]


def test_mixed_schemas_with_evolution_add_columns(sql_catalog: SqlCatalog) -> None:
    conformer = make_conformer(sql_catalog, LoaderConfig(write_mode='append', schema_evolution=True))

    data = conformer.conform([batch(id=[1]), batch(id=[2], extra=['x'])])

    assert sql_catalog.load_table(TID).schema().column_names == ['id', 'extra']
    assert data.to_pylist() == [{'id': 1, 'extra': None}, {'id': 2, 'extra': 'x'}]


def test_mixed_schemas_without_evolution_raise_before_table_creation(sql_catalog: SqlCatalog) -> None:
    conformer = make_conformer(sql_catalog, LoaderConfig(write_mode='append'))

    with pytest.raises(pa.ArrowInvalid):
        conformer.conform([batch(id=[1]), batch(id=[2], extra=['x'])])

    assert not sql_catalog.table_exists(TID)
    assert conformer.table is None


def test_load_timestamp_exists_before_table_creation(sql_catalog: SqlCatalog) -> None:
    load_ts = datetime(2025, 1, 1)
    config = LoaderConfig(
        write_mode='append',
        schema_evolution=True,
        load_timestamp=load_ts,
        partition_col='day(_load_dttm)',
    )
    conformer = make_conformer(sql_catalog, config)

    data = conformer.conform([batch(id=[1]), batch(id=[2], extra=['x'])])

    assert [f.name for f in sql_catalog.load_table(TID).spec().fields] == ['_load_dttm_day']
    assert data.column('_load_dttm').to_pylist() == [load_ts, load_ts]


def test_second_conform_reuses_table(sql_catalog: SqlCatalog) -> None:
    conformer = make_conformer(sql_catalog, LoaderConfig(write_mode='append'))
    conformer.conform([batch(id=[1])])
    first_table = conformer.table

    conformer.conform([batch(id=[2])])

    assert conformer.table is first_table
    assert conformer.new_table_created is True


def test_empty_buffer_is_rejected(sql_catalog: SqlCatalog) -> None:
    conformer = make_conformer(sql_catalog, LoaderConfig(write_mode='append'))

    with pytest.raises(ValueError, match='at least one batch'):
        conformer.conform([])
