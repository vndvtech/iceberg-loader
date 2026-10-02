import io

import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog

from iceberg_loader import LoaderConfig, load_ipc_stream_to_iceberg


def test_load_ipc_stream(sql_catalog: SqlCatalog) -> None:
    schema = pa.schema([pa.field('id', pa.int64()), pa.field('value', pa.string())])
    sink = io.BytesIO()
    with pa.ipc.new_stream(sink, schema) as writer:
        for i in range(2):
            batch = pa.RecordBatch.from_pydict(
                {'id': [i * 2, i * 2 + 1], 'value': [f'v{i}a', f'v{i}b']},
                schema=schema,
            )
            writer.write_batch(batch)
    sink.seek(0)
    identifier = ('default', 'streaming_test')

    result = load_ipc_stream_to_iceberg(sink, identifier, sql_catalog, LoaderConfig(write_mode='append'))

    table = sql_catalog.load_table(identifier)
    assert table.scan().to_arrow().sort_by('id').column('id').to_pylist() == [0, 1, 2, 3]
    assert len(list(table.snapshots())) == 2
    assert result['rows_loaded'] == 4
    assert result['batches_processed'] == 2
