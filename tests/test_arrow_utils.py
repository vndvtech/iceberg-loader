import unittest

import pyarrow as pa

from iceberg_loader.arrow_utils import create_arrow_table_from_data


class TestArrowUtils(unittest.TestCase):
    def test_create_arrow_table_serializes_complex(self):
        data = [
            {'id': 1, 'complex_field': {'a': 1, 'b': 'x'}},
            {'id': 2, 'complex_field': [1, 2, 3]},
        ]

        table = create_arrow_table_from_data(data)

        self.assertEqual(table.schema.field('complex_field').type, pa.string())
        self.assertEqual(
            table.to_pydict()['complex_field'],
            ['{"a":1,"b":"x"}', '[1,2,3]'],
        )


if __name__ == '__main__':
    unittest.main()
