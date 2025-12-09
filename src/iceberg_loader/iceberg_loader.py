import logging
from collections.abc import Iterator
from itertools import count
from typing import Any, BinaryIO, Literal

import pyarrow as pa
from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import NestedField

from iceberg_loader.arrow_utils import convert_table_types
from iceberg_loader.settings import TABLE_PROPERTIES
from iceberg_loader.type_mappings import get_arrow_type, get_iceberg_type

logger = logging.getLogger(__name__)


class IcebergLoader:
    """
    Loader for Apache Iceberg tables with support for complex JSON data.
    
    Handles schema inference, type conversion, partitioning, and various write modes
    (overwrite, append, idempotent). Automatically serializes complex types to JSON strings.
    """

    def __init__(self, catalog: Catalog, table_properties: dict[str, Any] | None = None):
        self.catalog = catalog
        self.table_properties = TABLE_PROPERTIES.copy()
        if table_properties:
            self.table_properties.update(table_properties)

    def _convert_arrow_to_iceberg_schema(self, arrow_schema: pa.Schema, existing_schema: Schema | None = None) -> Schema:
        existing_fields = {field.name: field for field in existing_schema.fields} if existing_schema else {}
        field_id_counter = count(max((f.field_id for f in existing_fields.values()), default=0) + 1)
        fields = []

        for field in arrow_schema:
            iceberg_type = get_iceberg_type(field.type)
            if field.name in existing_fields:
                existing = existing_fields[field.name]
                iceberg_field = NestedField(
                    field_id=existing.field_id,
                    name=field.name,
                    field_type=iceberg_type,
                    required=existing.required,
                )
            else:
                iceberg_field = NestedField(
                    field_id=next(field_id_counter),
                    name=field.name,
                    field_type=iceberg_type,
                    required=not field.nullable,
                )
            fields.append(iceberg_field)

        return Schema(*fields)

    def _create_iceberg_table(
        self,
        table_identifier: tuple[str, str],
        schema: Schema,
        partition_col: str | None = None,
    ) -> None:
        partition_spec = None

        if partition_col:
            try:
                field = schema.find_field(partition_col)
                max_field_id = max(f.field_id for f in schema.fields)
                partition_field_id = max_field_id + 1

                partition_spec = PartitionSpec(
                    PartitionField(
                        source_id=field.field_id,
                        field_id=partition_field_id,
                        transform=IdentityTransform(),
                        name=f'{partition_col}_partition',
                    ),
                )
            except ValueError:
                logger.warning(
                    "Partition column '%s' not found in schema. Creating table without partition.", partition_col
                )

        if partition_spec:
            self.catalog.create_table(
                identifier=table_identifier,
                schema=schema,
                partition_spec=partition_spec,
                properties=self.table_properties,
            )
        else:
            self.catalog.create_table(
                identifier=table_identifier,
                schema=schema,
                properties=self.table_properties,
            )

    def _convert_iceberg_schema_to_arrow(self, schema: Schema) -> pa.Schema:
        fields = [
            pa.field(field.name, get_arrow_type(field.field_type), nullable=not field.required)
            for field in schema.fields
        ]
        return pa.schema(fields)

    def _load_table_if_exists(self, table_identifier: tuple[str, str]) -> Any | None:
        try:
            return self.catalog.load_table(table_identifier)
        except (FileNotFoundError, ValueError, NoSuchTableError):
            return None

    def load_data(
        self,
        table_data: pa.Table,
        table_identifier: tuple[str, str],
        write_mode: Literal['overwrite', 'append'] = 'overwrite',
        partition_col: str | None = None,
        replace_filter: str | None = None,
        schema_evolution: bool = False,
    ) -> dict[str, Any]:
        """
        Load PyArrow Table into Iceberg table with automatic schema handling.
        
        Creates table if it doesn't exist, converts types to match Iceberg schema,
        and supports overwrite/append modes with optional idempotent writes.
        """
        # For single table load, we can use a simpler approach or delegate to load_data_batches
        # Delegating ensures consistent logic for schema evolution and transaction handling.
        
        # Convert table to batches (single batch)
        batches = table_data.to_batches()
        
        return self.load_data_batches(
            batch_iterator=iter(batches),
            table_identifier=table_identifier,
            write_mode=write_mode,
            partition_col=partition_col,
            replace_filter=replace_filter,
            schema_evolution=schema_evolution,
        )

    def load_ipc_stream(
        self,
        stream_source: str | BinaryIO | pa.NativeFile,
        table_identifier: tuple[str, str],
        write_mode: Literal['overwrite', 'append'] = 'overwrite',
        partition_col: str | None = None,
        replace_filter: str | None = None,
        schema_evolution: bool = False,
        commit_interval: int = 0,
    ) -> dict[str, Any]:
        """
        Loads data from an Apache Arrow IPC stream source.
        """
        with pa.ipc.open_stream(stream_source) as reader:
            return self.load_data_batches(
                batch_iterator=reader,
                table_identifier=table_identifier,
                write_mode=write_mode,
                partition_col=partition_col,
                replace_filter=replace_filter,
                schema_evolution=schema_evolution,
                commit_interval=commit_interval,
            )

    def load_data_batches(
        self,
        batch_iterator: Iterator[pa.RecordBatch] | pa.RecordBatchReader,
        table_identifier: tuple[str, str],
        write_mode: Literal['overwrite', 'append'] = 'overwrite',
        partition_col: str | None = None,
        replace_filter: str | None = None,
        schema_evolution: bool = False,
        commit_interval: int = 0,
    ) -> dict[str, Any]:
        """
        Load data from RecordBatch iterator/reader into Iceberg table.

        Efficiently processes streaming data in batches, creating table if needed
        and handling schema evolution. Supports overwrite/append with idempotent writes.

        Args:
            commit_interval: If > 0, accumulates N batches in memory before writing and committing.
                             This reduces the number of snapshots/commits for large streams.
                             If 0, writes and commits every batch immediately (safe for small loads).
        """
        total_rows = 0
        batches_processed = 0
        
        # Buffer for accumulating batches before write
        pending_batches: list[pa.RecordBatch] = []

        table = self._load_table_if_exists(table_identifier)
        created_new_table = False

        is_first_write = True
        has_overwritten = False
        
        # Helper to flush pending batches
        def flush_batches(batches_to_write: list[pa.RecordBatch]):
            nonlocal table, created_new_table, is_first_write, has_overwritten, total_rows

            if not batches_to_write:
                return

            # Combine batches
            combined_table = pa.Table.from_batches(batches_to_write)
            
            # 1. Initialize Table if needed (lazy init on first write)
            if table is None:
                schema = self._convert_arrow_to_iceberg_schema(combined_table.schema)
                self._create_iceberg_table(table_identifier, schema, partition_col=partition_col)
                table = self.catalog.load_table(table_identifier)
                created_new_table = True

            # 2. Schema Evolution
            arrow_schema = self._convert_iceberg_schema_to_arrow(table.schema())
            incoming_names = set(combined_table.schema.names)
            existing_names = set(arrow_schema.names)
            
            evolution_needed = schema_evolution and bool(incoming_names - existing_names)
            
            if evolution_needed:
                logger.info('Schema evolution detected.')
                new_schema = self._convert_arrow_to_iceberg_schema(combined_table.schema, existing_schema=table.schema())
                with table.update_schema() as update:
                    update.union_by_name(new_schema)
                table.refresh()
                arrow_schema = self._convert_iceberg_schema_to_arrow(table.schema())

            # 3. Type Conversion
            combined_table = convert_table_types(combined_table, arrow_schema)

            # 4. Write Transaction
            # We use table.transaction() to group potential replace_filter + append
            # OR just use table.append/overwrite if no filter is needed.
            
            # Note: pyiceberg's table.append() creates a transaction internally.
            # But if we want to combine delete + append, we must use an explicit transaction.
            
            # If we need to overwrite or delete, we MUST use a transaction.
            # If it's a simple append, table.append() is safer/easier, but mixing approaches is bad.
            # Let's use explicit transaction for consistency.
            
            txn = table.transaction()
            
            with txn:
                if is_first_write:
                    if replace_filter:
                         logger.info('Deleting rows matching filter: %s', replace_filter)
                         txn.delete(replace_filter)
                    
                    if write_mode == 'overwrite' and not has_overwritten:
                        txn.overwrite(combined_table)
                        has_overwritten = True
                    else:
                        txn.append(combined_table)
                    
                    is_first_write = False
                else:
                    txn.append(combined_table)
            
            total_rows += len(combined_table)


        for batch in batch_iterator:
            pending_batches.append(batch)
            batches_processed += 1
            
            # Check if we should flush
            # If commit_interval is 0 or 1, we flush every batch.
            # If > 1, we flush when we reach the limit.
            limit = 1 if commit_interval <= 1 else commit_interval
            
            if len(pending_batches) >= limit:
                flush_batches(pending_batches)
                pending_batches = []

        # Flush any remaining batches
        if pending_batches:
            flush_batches(pending_batches)

        return {
            'rows_loaded': total_rows,
            'write_mode': write_mode,
            'partition_col': partition_col if partition_col else 'none',
            'table_location': table.location() if table else 'none',
            'snapshot_id': table.current_snapshot().snapshot_id if table and table.current_snapshot() else 'none',
            'batches_processed': batches_processed,
            'new_table_created': created_new_table,
        }


def load_data_to_iceberg(
    table_data: pa.Table,
    table_identifier: tuple[str, str],
    catalog: Catalog,
    write_mode: Literal['overwrite', 'append'] = 'overwrite',
    partition_col: str | None = None,
    replace_filter: str | None = None,
    schema_evolution: bool = False,
    table_properties: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Public API wrapper for loading PyArrow Table into Iceberg table."""
    loader = IcebergLoader(catalog, table_properties)
    return loader.load_data(table_data, table_identifier, write_mode, partition_col, replace_filter, schema_evolution)


def load_batches_to_iceberg(
    batch_iterator: Iterator[pa.RecordBatch] | pa.RecordBatchReader,
    table_identifier: tuple[str, str],
    catalog: Catalog,
    write_mode: Literal['overwrite', 'append'] = 'overwrite',
    partition_col: str | None = None,
    replace_filter: str | None = None,
    schema_evolution: bool = False,
    table_properties: dict[str, Any] | None = None,
    commit_interval: int = 0,
) -> dict[str, Any]:
    """Public API wrapper for loading RecordBatch iterator/reader into Iceberg table."""
    loader = IcebergLoader(catalog, table_properties)
    return loader.load_data_batches(
        batch_iterator,
        table_identifier,
        write_mode,
        partition_col,
        replace_filter,
        schema_evolution,
        commit_interval,
    )


def load_ipc_stream_to_iceberg(
    stream_source: str | BinaryIO | pa.NativeFile,
    table_identifier: tuple[str, str],
    catalog: Catalog,
    write_mode: Literal['overwrite', 'append'] = 'overwrite',
    partition_col: str | None = None,
    replace_filter: str | None = None,
    schema_evolution: bool = False,
    table_properties: dict[str, Any] | None = None,
    commit_interval: int = 0,
) -> dict[str, Any]:
    """Public API wrapper for loading Apache Arrow IPC stream into Iceberg table."""
    loader = IcebergLoader(catalog, table_properties)
    return loader.load_ipc_stream(
        stream_source,
        table_identifier,
        write_mode,
        partition_col,
        replace_filter,
        schema_evolution,
        commit_interval,
    )
