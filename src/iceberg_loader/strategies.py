import logging
from abc import ABC, abstractmethod
from typing import Any

import pyarrow as pa

logger = logging.getLogger(__name__)


class WriteStrategy(ABC):
    """
    Abstract base class for write strategies.
    Defines how data should be written to the Iceberg table transaction.
    """

    @abstractmethod
    def write(self, txn: Any, data: pa.Table, is_first_write: bool) -> None:
        """
        Write data to the transaction.

        Args:
            txn: The open Iceberg transaction.
            data: The PyArrow table to write.
            is_first_write: True if this is the first batch/transaction in the stream.
                            Crucial for Overwrite logic (overwrite only once).
        """
        pass


class AppendStrategy(WriteStrategy):
    """Simply appends data to the table."""

    def write(self, txn: Any, data: pa.Table, is_first_write: bool) -> None:
        txn.append(data)


class OverwriteStrategy(WriteStrategy):
    """
    Overwrites the table content on the first write,
    then appends for subsequent batches (to preserve the stream data).
    """

    def write(self, txn: Any, data: pa.Table, is_first_write: bool) -> None:
        if is_first_write:
            logger.info('First write in Overwrite mode: Overwriting table data.')
            txn.overwrite(data)
        else:
            txn.append(data)


class IdempotentStrategy(WriteStrategy):
    """
    Deletes rows matching a filter before writing (on first write),
    then appends data. Implements 'Replace Partition' pattern.
    """

    def __init__(self, replace_filter: str):
        self.replace_filter = replace_filter

    def write(self, txn: Any, data: pa.Table, is_first_write: bool) -> None:
        if is_first_write:
            logger.info("First write in Idempotent mode: Deleting rows matching '%s'", self.replace_filter)
            txn.delete(self.replace_filter)

        txn.append(data)


def get_write_strategy(write_mode: str, replace_filter: str | None = None) -> WriteStrategy:
    """Factory method to select the appropriate strategy."""
    if replace_filter:
        return IdempotentStrategy(replace_filter)

    if write_mode == 'overwrite':
        return OverwriteStrategy()

    return AppendStrategy()
