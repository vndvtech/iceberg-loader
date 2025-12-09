import logging
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)


class SnapshotMaintenance:
    """Handles maintenance tasks for Iceberg tables, such as expiring snapshots."""

    def expire_snapshots(self, table: Any) -> None:
        """Expire old snapshots to prevent metadata corruption and unsorted snapshot issues."""
        try:
            table.refresh()

            snapshots = list(table.snapshots())
            logger.info(f'Found {len(snapshots)} snapshots')

            if len(snapshots) == 0:
                logger.info('No snapshots found, nothing to fix')
                return

            sorted_snapshots = sorted(snapshots, key=lambda s: s.timestamp_ms)

            logger.info('\nSnapshot details (sorted by timestamp):')
            for i, snap in enumerate(sorted_snapshots):
                is_current = table.current_snapshot() and snap.snapshot_id == table.current_snapshot().snapshot_id
                marker = ' <-- CURRENT' if is_current else ''
                logger.info(f'  {i + 1}. ID={snap.snapshot_id}, timestamp={snap.timestamp_ms}{marker}')

            if len(snapshots) > 1:
                logger.info(f'\nExpiring {len(snapshots) - 1} old snapshots, keeping only the most recent...')
                current_snapshot = table.current_snapshot()
                if current_snapshot:
                    cutoff_datetime = datetime.fromtimestamp((current_snapshot.timestamp_ms - 1000) / 1000.0)
                    table.maintenance.expire_snapshots().older_than(cutoff_datetime).commit()
                logger.info('✓ Successfully expired old snapshots')

                table.refresh()
                remaining_snapshots = list(table.snapshots())
                logger.info(f'✓ Table now has {len(remaining_snapshots)} snapshot(s)')
            else:
                logger.info('Only one snapshot exists, no action needed')

        except Exception as e:
            logger.warning(f'Failed to expire snapshots for table {table.name()}: {e}')
