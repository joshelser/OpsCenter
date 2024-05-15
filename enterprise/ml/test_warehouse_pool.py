from unittest import TestCase

import pandas as pd
from pandas.testing import assert_frame_equal


class Test(TestCase):
    def test_compute_set_of_warehouses(self):
        from ml.warehouse_pool import compute_set_of_warehouses

        with self.assertRaises(KeyError):
            compute_set_of_warehouses("UnknownSize", 15)
        with self.assertRaises(ValueError):
            compute_set_of_warehouses("X-Large", 15)
        assert_frame_equal(
            compute_set_of_warehouses("Small", 2),
            pd.DataFrame({"warehouse_size": ["Small"], "num_clusters": [1]}),
        )
        assert_frame_equal(
            compute_set_of_warehouses("X-Small", 5),
            pd.DataFrame(
                {"warehouse_size": ["X-Small", "Small"], "num_clusters": [3, 1]}
            ),
        )
        assert_frame_equal(
            compute_set_of_warehouses("Small", 3),
            pd.DataFrame(
                {"warehouse_size": ["X-Small", "Small"], "num_clusters": [1, 1]}
            ),
        )
        assert_frame_equal(
            compute_set_of_warehouses("Medium", 10),
            pd.DataFrame(
                {
                    "warehouse_size": ["X-Small", "Small", "Medium"],
                    "num_clusters": [2, 2, 1],
                }
            ),
        )
        assert_frame_equal(
            compute_set_of_warehouses("Large", 10),
            pd.DataFrame(
                {"warehouse_size": ["Small", "Large"], "num_clusters": [1, 1]}
            ),
        )
        assert_frame_equal(
            compute_set_of_warehouses("Large", 20),
            pd.DataFrame(
                {
                    "warehouse_size": ["X-Small", "Small", "Medium", "Large"],
                    "num_clusters": [2, 1, 2, 1],
                }
            ),
        )
        assert_frame_equal(
            compute_set_of_warehouses("Large", 50),
            pd.DataFrame(
                {
                    "warehouse_size": [
                        "X-Small",
                        "Small",
                        "Medium",
                        "Large",
                        "X-Large",
                    ],
                    "num_clusters": [2, 2, 3, 2, 1],
                }
            ),
        )
        assert_frame_equal(
            compute_set_of_warehouses("Large", 51),
            pd.DataFrame(
                {
                    "warehouse_size": [
                        "X-Small",
                        "Small",
                        "Medium",
                        "Large",
                        "X-Large",
                    ],
                    "num_clusters": [3, 2, 3, 2, 1],
                }
            ),
        )
        assert_frame_equal(
            compute_set_of_warehouses("Large", 66),
            pd.DataFrame(
                {
                    "warehouse_size": [
                        "X-Small",
                        "Small",
                        "Medium",
                        "Large",
                        "X-Large",
                        "2X-Large",
                    ],
                    "num_clusters": [2, 2, 1, 1, 1, 1],
                }
            ),
        )
        assert_frame_equal(
            compute_set_of_warehouses("Large", 100),
            pd.DataFrame(
                {
                    "warehouse_size": [
                        "X-Small",
                        "Small",
                        "Medium",
                        "Large",
                        "X-Large",
                        "2X-Large",
                    ],
                    "num_clusters": [2, 3, 3, 2, 2, 1],
                }
            ),
        )
        assert_frame_equal(
            compute_set_of_warehouses("Large", 200),
            pd.DataFrame(
                {
                    "warehouse_size": [
                        "X-Small",
                        "Small",
                        "Medium",
                        "Large",
                        "X-Large",
                        "2X-Large",
                        "3X-Large",
                    ],
                    "num_clusters": [2, 3, 2, 3, 2, 2, 1],
                }
            ),
        )
        assert_frame_equal(
            compute_set_of_warehouses("Large", 300),
            pd.DataFrame(
                {
                    "warehouse_size": [
                        "X-Small",
                        "Small",
                        "Medium",
                        "Large",
                        "X-Large",
                        "2X-Large",
                        "3X-Large",
                        "4X-Large",
                    ],
                    "num_clusters": [2, 3, 3, 3, 2, 1, 1, 1],
                }
            ),
        )
        assert_frame_equal(
            compute_set_of_warehouses("Large", 500),
            pd.DataFrame(
                {
                    "warehouse_size": [
                        "X-Small",
                        "Small",
                        "Medium",
                        "Large",
                        "X-Large",
                        "2X-Large",
                        "3X-Large",
                        "4X-Large",
                    ],
                    "num_clusters": [6, 5, 5, 6, 4, 3, 2, 1],
                }
            ),
        )
        assert_frame_equal(
            compute_set_of_warehouses("X-Large", 16),
            pd.DataFrame({"warehouse_size": ["X-Large"], "num_clusters": [1]}),
        )
        assert_frame_equal(
            compute_set_of_warehouses("X-Large", 20),
            pd.DataFrame(
                {"warehouse_size": ["Medium", "X-Large"], "num_clusters": [1, 1]}
            ),
        )
        assert_frame_equal(
            compute_set_of_warehouses("X-Large", 30),
            pd.DataFrame(
                {
                    "warehouse_size": ["Small", "Medium", "Large", "X-Large"],
                    "num_clusters": [1, 1, 1, 1],
                }
            ),
        )
        assert_frame_equal(
            compute_set_of_warehouses("X-Large", 31),
            pd.DataFrame(
                {
                    "warehouse_size": [
                        "X-Small",
                        "Small",
                        "Medium",
                        "Large",
                        "X-Large",
                    ],
                    "num_clusters": [1, 1, 1, 1, 1],
                }
            ),
        )
        assert_frame_equal(
            compute_set_of_warehouses("X-Large", 32),
            pd.DataFrame(
                {
                    "warehouse_size": [
                        "X-Small",
                        "Small",
                        "Medium",
                        "Large",
                        "X-Large",
                    ],
                    "num_clusters": [2, 1, 1, 1, 1],
                }
            ),
        )
        assert_frame_equal(
            compute_set_of_warehouses("X-Large", 50),
            pd.DataFrame(
                {
                    "warehouse_size": [
                        "X-Small",
                        "Small",
                        "Medium",
                        "Large",
                        "X-Large",
                    ],
                    "num_clusters": [2, 2, 1, 1, 2],
                }
            ),
        )
        assert_frame_equal(
            compute_set_of_warehouses("X-Large", 66),
            pd.DataFrame(
                {
                    "warehouse_size": [
                        "X-Small",
                        "Small",
                        "Medium",
                        "Large",
                        "X-Large",
                        "2X-Large",
                    ],
                    "num_clusters": [2, 2, 1, 1, 1, 1],
                }
            ),
        )
        assert_frame_equal(
            compute_set_of_warehouses("X-Large", 100),
            pd.DataFrame(
                {
                    "warehouse_size": [
                        "X-Small",
                        "Small",
                        "Medium",
                        "Large",
                        "X-Large",
                        "2X-Large",
                    ],
                    "num_clusters": [2, 3, 3, 2, 2, 1],
                }
            ),
        )
        assert_frame_equal(
            compute_set_of_warehouses("2X-Large", 63),
            pd.DataFrame(
                {
                    "warehouse_size": [
                        "X-Small",
                        "Small",
                        "Medium",
                        "Large",
                        "X-Large",
                        "2X-Large",
                    ],
                    "num_clusters": [1, 1, 1, 1, 1, 1],
                }
            ),
        )
        assert_frame_equal(
            compute_set_of_warehouses("2X-Large", 64),
            pd.DataFrame(
                {
                    "warehouse_size": [
                        "X-Small",
                        "Small",
                        "Medium",
                        "Large",
                        "X-Large",
                        "2X-Large",
                    ],
                    "num_clusters": [2, 1, 1, 1, 1, 1],
                }
            ),
        )
        assert_frame_equal(
            compute_set_of_warehouses("3X-Large", 64),
            pd.DataFrame({"warehouse_size": ["3X-Large"], "num_clusters": [1]}),
        )
        assert_frame_equal(
            compute_set_of_warehouses("3X-Large", 75),
            pd.DataFrame(
                {
                    "warehouse_size": ["X-Small", "Small", "Large", "3X-Large"],
                    "num_clusters": [1, 1, 1, 1],
                }
            ),
        )
        assert_frame_equal(
            compute_set_of_warehouses("3X-Large", 200),
            pd.DataFrame(
                {
                    "warehouse_size": [
                        "X-Small",
                        "Small",
                        "Medium",
                        "Large",
                        "X-Large",
                        "2X-Large",
                        "3X-Large",
                    ],
                    "num_clusters": [2, 1, 1, 2, 1, 1, 2],
                }
            ),
        )
        assert_frame_equal(
            compute_set_of_warehouses("3X-Large", 300),
            pd.DataFrame(
                {
                    "warehouse_size": [
                        "X-Small",
                        "Small",
                        "Medium",
                        "Large",
                        "X-Large",
                        "2X-Large",
                        "3X-Large",
                        "4X-Large",
                    ],
                    "num_clusters": [2, 1, 2, 2, 1, 2, 1, 1],
                }
            ),
        )
        assert_frame_equal(
            compute_set_of_warehouses("3X-Large", 400),
            pd.DataFrame(
                {
                    "warehouse_size": [
                        "X-Small",
                        "Small",
                        "Medium",
                        "Large",
                        "X-Large",
                        "2X-Large",
                        "3X-Large",
                        "4X-Large",
                    ],
                    "num_clusters": [2, 3, 2, 2, 3, 2, 2, 1],
                }
            ),
        )
        assert_frame_equal(
            compute_set_of_warehouses("4X-Large", 500),
            pd.DataFrame(
                {
                    "warehouse_size": [
                        "Medium",
                        "X-Large",
                        "2X-Large",
                        "3X-Large",
                        "4X-Large",
                        "5X-Large",
                    ],
                    "num_clusters": [1, 1, 1, 1, 1, 1],
                }
            ),
        )
        assert_frame_equal(
            compute_set_of_warehouses("4X-Large", 600),
            pd.DataFrame(
                {
                    "warehouse_size": [
                        "X-Small",
                        "Small",
                        "Medium",
                        "Large",
                        "X-Large",
                        "2X-Large",
                        "3X-Large",
                        "4X-Large",
                        "5X-Large",
                    ],
                    "num_clusters": [2, 1, 1, 2, 2, 1, 2, 1, 1],
                }
            ),
        )
        assert_frame_equal(
            compute_set_of_warehouses("6X-Large", 520),
            pd.DataFrame(
                {"warehouse_size": ["Large", "6X-Large"], "num_clusters": [1, 1]}
            ),
        )
        assert_frame_equal(
            compute_set_of_warehouses("6X-Large", 1023),
            pd.DataFrame(
                {
                    "warehouse_size": [
                        "X-Small",
                        "Small",
                        "Medium",
                        "Large",
                        "X-Large",
                        "2X-Large",
                        "3X-Large",
                        "4X-Large",
                        "5X-Large",
                        "6X-Large",
                    ],
                    "num_clusters": [1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
                }
            ),
        )
        assert_frame_equal(
            compute_set_of_warehouses("6X-Large", 1024),
            pd.DataFrame(
                {
                    "warehouse_size": [
                        "X-Small",
                        "Small",
                        "Medium",
                        "Large",
                        "X-Large",
                        "2X-Large",
                        "3X-Large",
                        "4X-Large",
                        "5X-Large",
                        "6X-Large",
                    ],
                    "num_clusters": [2, 1, 1, 1, 1, 1, 1, 1, 1, 1],
                }
            ),
        )
        assert_frame_equal(
            compute_set_of_warehouses("6X-Large", 2048),
            pd.DataFrame(
                {
                    "warehouse_size": [
                        "X-Small",
                        "Small",
                        "Medium",
                        "Large",
                        "X-Large",
                        "2X-Large",
                        "3X-Large",
                        "4X-Large",
                        "5X-Large",
                        "6X-Large",
                    ],
                    "num_clusters": [2, 3, 2, 2, 2, 2, 2, 2, 2, 2],
                }
            ),
        )
        assert_frame_equal(
            compute_set_of_warehouses("6X-Large", 2049),
            pd.DataFrame(
                {
                    "warehouse_size": [
                        "X-Small",
                        "Small",
                        "Medium",
                        "Large",
                        "X-Large",
                        "2X-Large",
                        "3X-Large",
                        "4X-Large",
                        "5X-Large",
                        "6X-Large",
                    ],
                    "num_clusters": [3, 3, 2, 2, 2, 2, 2, 2, 2, 2],
                }
            ),
        )
