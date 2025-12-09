import unittest

import pyarrow as pa
from pyiceberg.types import IntegerType, LongType, StringType, TimestampType, TimestamptzType

from iceberg_loader.type_mappings import get_arrow_type, get_iceberg_type


class TestTypeMappings(unittest.TestCase):
    def test_arrow_to_iceberg_basic(self):
        self.assertEqual(get_iceberg_type(pa.string()), StringType())
        self.assertEqual(get_iceberg_type(pa.int32()), IntegerType())
        self.assertEqual(get_iceberg_type(pa.int64()), LongType())

    def test_arrow_to_iceberg_timestamp(self):
        self.assertEqual(get_iceberg_type(pa.timestamp('us')), TimestampType())
        self.assertEqual(get_iceberg_type(pa.timestamp('us', tz='UTC')), TimestamptzType())

    def test_iceberg_to_arrow_basic(self):
        self.assertEqual(get_arrow_type(StringType()), pa.string())
        self.assertEqual(get_arrow_type(IntegerType()), pa.int32())
        self.assertEqual(get_arrow_type(LongType()), pa.int64())


if __name__ == '__main__':
    unittest.main()
