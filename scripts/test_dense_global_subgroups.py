import unittest

import numpy as np

from probe_dense_global_subgroups import (
    balanced_groups,
    normalize,
    quantize_i8,
    screen,
    select_whole_groups,
)


class GlobalSubgroupTest(unittest.TestCase):
    def test_balanced_deterministic_duplicates(self):
        rows = np.tile([1.0, 0.0], (33, 1)).astype(np.float32)
        groups = balanced_groups(rows, np.arange(33), 8)
        self.assertEqual(sorted(np.concatenate(groups).tolist()), list(range(33)))
        self.assertLessEqual(max(map(len, groups)) - min(map(len, groups)), 1)
        for left, right in zip(groups, balanced_groups(rows, np.arange(33), 8)):
            np.testing.assert_array_equal(left, right)

    def test_overshoot_is_counted(self):
        np.testing.assert_array_equal(
            select_whole_groups(np.array([2, 0, 1]), np.array([5, 6, 7]), 8), [2, 0]
        )
        with self.assertRaises(ValueError):
            select_whole_groups(np.array([0]), np.array([5]), 6)

    def test_precision_and_full_frontier(self):
        rng = np.random.default_rng(593)
        rows = normalize(rng.normal(size=(64, 7)).astype(np.float32))
        queries = normalize(rng.normal(size=(5, 7)).astype(np.float32))
        report, _ = screen(rows, queries, 4, 4, 3)
        for row in report["treatments"]:
            if row["requested_fraction_of_frontier"] == 1:
                self.assertAlmostEqual(row["coverage_loss_pp"], 0)
                if row["frontier_fraction"] == 1:
                    self.assertEqual(row["sample_neighbor_coverage"], 1)
        codes, scales = quantize_i8(np.array([[0.0, 0], [0.2, -1]], np.float32))
        self.assertTrue(np.isfinite(scales).all())
        self.assertTrue(
            np.all(
                np.abs(codes.astype(float) * scales[:, None] - [[0, 0], [0.2, -1]])
                <= scales[:, None] / 2 + 1e-7
            )
        )

    def test_int8_halfway_matches_zig_round(self):
        codes, scales = quantize_i8(np.array([[127, 0.5, -0.5, 1.5, -1.5]], np.float32))
        np.testing.assert_array_equal(codes, [[127, 1, -1, 2, -2]])
        np.testing.assert_array_equal(scales, [1])
        below = np.nextafter(np.float32(0.5), np.float32(0))
        codes, _ = quantize_i8(np.array([[127, below, -below]], np.float32))
        np.testing.assert_array_equal(codes, [[127, 0, 0]])


if __name__ == "__main__":
    unittest.main()
