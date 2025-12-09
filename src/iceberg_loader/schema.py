import logging
from itertools import count
from typing import Any

import pyarrow as pa
from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import NestedField

from iceberg_loader.type_mappings import get_arrow_type, get_iceberg_type

logger = logging.getLogger(__name__)


class SchemaManager:
    """
    Responsible for Iceberg table schema operations:
    - Converting Arrow schema to Iceberg schema
    - Creating tables
    - Handling schema evolution
    """

    def __init__(self, catalog: Catalog, table_properties: dict[str, Any]):
        self.catalog = catalog
        self.table_properties = table_properties

    def ensure_table_exists(
        self,
        table_identifier: tuple[str, str],
        arrow_schema: pa.Schema,
        partition_col: str | None = None,
    ) -> Any:
        """Loads the table, or creates it if it doesn't exist."""
        try:
            return self.catalog.load_table(table_identifier)
        except (FileNotFoundError, ValueError, NoSuchTableError):
            logger.info('Table %s not found, creating new table.', table_identifier)
            iceberg_schema = self._arrow_to_iceberg(arrow_schema)
            self._create_table(table_identifier, iceberg_schema, partition_col)
            return self.catalog.load_table(table_identifier)

    def evolve_schema_if_needed(self, table: Any, batch_schema: pa.Schema) -> bool:
        """
        Checks if the batch schema has new columns compared to the table schema.
        Updates the table schema if evolution is needed.
        """
        arrow_schema = self._iceberg_to_arrow(table.schema())
        incoming_names = set(batch_schema.names)
        existing_names = set(arrow_schema.names)

        if not (incoming_names - existing_names):
            return False

        logger.info('Schema evolution detected. Adding new columns...')
        new_schema = self._arrow_to_iceberg(batch_schema, existing_schema=table.schema())

        with table.update_schema() as update:
            update.union_by_name(new_schema)

        # Refresh table to apply changes immediately
        table.refresh()
        return True

    def get_arrow_schema(self, table: Any) -> pa.Schema:
        """Returns the current table schema as PyArrow schema."""
        return self._iceberg_to_arrow(table.schema())

    def _create_table(
        self,
        table_identifier: tuple[str, str],
        schema: Schema,
        partition_col: str | None = None,
    ) -> None:
        partition_spec = None

        if partition_col:
            try:
                field = schema.find_field(partition_col)
                # Heuristic for new partition field ID
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

    def _arrow_to_iceberg(self, arrow_schema: pa.Schema, existing_schema: Schema | None = None) -> Schema:
        existing_fields = {field.name: field for field in existing_schema.fields} if existing_schema else {}
        # Ensure new field IDs don't clash
        start_id = max((f.field_id for f in existing_fields.values()), default=0) + 1
        field_id_counter = count(start_id)
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

    def _iceberg_to_arrow(self, schema: Schema) -> pa.Schema:
        fields = [
            pa.field(field.name, get_arrow_type(field.field_type), nullable=not field.required)
            for field in schema.fields
        ]
        return pa.schema(fields)
