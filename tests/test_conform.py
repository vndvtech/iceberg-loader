from collections.abc import Callable
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


def test_explicit_format_version_mismatch_warns(
    sql_catalog: SqlCatalog,
    loader_warnings: Callable[[], list[str]],
) -> None:
    v1 = LoaderConfig(write_mode='append', table_properties={'format-version': 1})
    load_data_to_iceberg(pa.table({'id': [1]}), TID, sql_catalog, v1)

    v2 = LoaderConfig(write_mode='append', table_properties={'format-version': 2})
    load_data_to_iceberg(pa.table({'id': [2]}), TID, sql_catalog, v2)

    assert sql_catalog.load_table(TID).format_version == 1
    assert loader_warnings() == [
        'Table default.events has format-version 1, but table_properties request 2. '
        'format-version applies only when a table is created; the table keeps version 1.',
    ]


def test_default_format_version_does_not_warn_for_existing_table(
    sql_catalog: SqlCatalog,
    loader_warnings: Callable[[], list[str]],
) -> None:
    v1 = LoaderConfig(write_mode='append', table_properties={'format-version': 1})
    load_data_to_iceberg(pa.table({'id': [1]}), TID, sql_catalog, v1)

    load_data_to_iceberg(pa.table({'id': [2]}), TID, sql_catalog, LoaderConfig(write_mode='append'))

    assert loader_warnings() == []


def test_matching_format_version_does_not_warn(
    sql_catalog: SqlCatalog,
    loader_warnings: Callable[[], list[str]],
) -> None:
    v2 = LoaderConfig(write_mode='append', table_properties={'format-version': '2'})
    load_data_to_iceberg(pa.table({'id': [1]}), TID, sql_catalog, LoaderConfig(write_mode='append'))

    load_data_to_iceberg(pa.table({'id': [2]}), TID, sql_catalog, v2)

    assert loader_warnings() == []


def test_non_integer_format_version_for_existing_table_warns_and_loads(
    sql_catalog: SqlCatalog,
    loader_warnings: Callable[[], list[str]],
) -> None:
    load_data_to_iceberg(pa.table({'id': [1]}), TID, sql_catalog, LoaderConfig(write_mode='append'))
    bad = LoaderConfig(write_mode='append', table_properties={'format-version': 'v2'})

    load_data_to_iceberg(pa.table({'id': [2]}), TID, sql_catalog, bad)

    assert sql_catalog.load_table(TID).scan().to_arrow().num_rows == 2
    assert loader_warnings() == ["Ignoring non-integer format-version 'v2' for existing table."]
