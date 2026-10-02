from collections.abc import Iterable, Iterator
from typing import Any, BinaryIO

import pyarrow as pa
from pyiceberg.catalog import Catalog

from iceberg_loader.core.config import TABLE_PROPERTIES, LoaderConfig, ensure_loader_config
from iceberg_loader.core.conform import BatchConformer
from iceberg_loader.core.schema import SchemaManager
from iceberg_loader.core.strategies import get_write_strategy


class IcebergLoader:
    """
    Facade for loading data into Iceberg tables.
    Buffers batches, conforms each buffer to the table via BatchConformer, and writes it via a WriteStrategy.
    """

    def __init__(
        self,
        catalog: Catalog,
        table_properties: dict[str, Any] | None = None,
        default_config: LoaderConfig | None = None,
    ):
        self.catalog = catalog
        self.table_properties = TABLE_PROPERTIES.copy()
        if table_properties:
            self.table_properties.update(table_properties)

        self.schema_manager = SchemaManager(self.catalog, self.table_properties)
        self.default_config = ensure_loader_config(default_config)

    def _resolve_config(self, config: LoaderConfig | None) -> LoaderConfig:
        if config is None:
            return self.default_config
        return ensure_loader_config(config)

    def load_data(
        self,
        table_data: pa.Table,
        table_identifier: tuple[str, str],
        config: LoaderConfig | None = None,
    ) -> dict[str, Any]:
        """
        Load PyArrow Table into Iceberg table.
        Delegates to load_data_batches for consistency.
        """
        batches = table_data.to_batches()
        return self.load_data_batches(
            batch_iterator=iter(batches),
            table_identifier=table_identifier,
            config=config,
        )

    def load_ipc_stream(
        self,
        stream_source: str | BinaryIO | pa.NativeFile,
        table_identifier: tuple[str, str],
        config: LoaderConfig | None = None,
    ) -> dict[str, Any]:
        """Loads data from an Apache Arrow IPC stream source."""
        with pa.ipc.open_stream(stream_source) as reader:
            return self.load_data_batches(
                batch_iterator=reader,
                table_identifier=table_identifier,
                config=config,
            )

    def load_data_batches(
        self,
        batch_iterator: Iterator[pa.RecordBatch] | pa.RecordBatchReader,
        table_identifier: tuple[str, str],
        config: LoaderConfig | None = None,
    ) -> dict[str, Any]:
        """
        Main orchestration method.
        Buffers batches up to commit_interval, conforms each buffer to the table, and delegates writing.
        """
        effective_config = self._resolve_config(config)
        effective_table_properties = self.table_properties.copy()
        if effective_config.table_properties:
            effective_table_properties.update(effective_config.table_properties)

        strategy = get_write_strategy(
            effective_config.write_mode,
            effective_config.replace_filter,
            effective_config.join_cols,
        )
        conformer = BatchConformer(
            self.schema_manager,
            table_identifier,
            effective_config,
            effective_table_properties,
        )

        batches_processed = 0
        total_rows = 0
        is_first_write = True
        for buffer in _buffered(batch_iterator, max(1, effective_config.commit_interval)):
            data = conformer.conform(buffer)
            strategy.write(conformer.table, data, is_first_write)
            is_first_write = False
            batches_processed += len(buffer)
            total_rows += len(data)

        # conformer.table stays None when the iterator yielded no batches.
        snapshot_id = 'none'
        table_loc = 'none'
        if conformer.table is not None:
            table_loc = conformer.table.location()
            current_snap = conformer.table.current_snapshot()
            if current_snap:
                snapshot_id = current_snap.snapshot_id

        return {
            'rows_loaded': total_rows,
            'write_mode': effective_config.write_mode,
            'partition_col': effective_config.partition_col if effective_config.partition_col else 'none',
            'table_location': table_loc,
            'snapshot_id': snapshot_id,
            'batches_processed': batches_processed,
            'new_table_created': conformer.new_table_created,
        }


def _buffered(batches: Iterable[pa.RecordBatch], size: int) -> Iterator[list[pa.RecordBatch]]:
    """Yields lists of up to `size` batches; the last list may be shorter."""
    buffer: list[pa.RecordBatch] = []
    for batch in batches:
        buffer.append(batch)
        if len(buffer) >= size:
            yield buffer
            buffer = []
    if buffer:
        yield buffer


# Public API functions (thin wrappers)


def load_data_to_iceberg(
    table_data: pa.Table,
    table_identifier: tuple[str, str],
    catalog: Catalog,
    config: LoaderConfig | None = None,
) -> dict[str, Any]:
    """Public wrapper around IcebergLoader.load_data using an optional LoaderConfig."""
    loader = IcebergLoader(catalog, default_config=config)
    return loader.load_data(
        table_data,
        table_identifier,
        config=config,
    )


def load_batches_to_iceberg(
    batch_iterator: Iterator[pa.RecordBatch] | pa.RecordBatchReader,
    table_identifier: tuple[str, str],
    catalog: Catalog,
    config: LoaderConfig | None = None,
) -> dict[str, Any]:
    """Public wrapper around IcebergLoader.load_data_batches using an optional LoaderConfig."""
    loader = IcebergLoader(catalog, default_config=config)
    return loader.load_data_batches(
        batch_iterator,
        table_identifier,
        config,
    )


def load_ipc_stream_to_iceberg(
    stream_source: str | BinaryIO | pa.NativeFile,
    table_identifier: tuple[str, str],
    catalog: Catalog,
    config: LoaderConfig | None = None,
) -> dict[str, Any]:
    """Public wrapper around IcebergLoader.load_ipc_stream using an optional LoaderConfig."""
    loader = IcebergLoader(catalog, default_config=config)
    return loader.load_ipc_stream(
        stream_source,
        table_identifier,
        config,
    )
