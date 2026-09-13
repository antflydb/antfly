import unittest

import numpy as np
from probe_dense_subgroup_bounds import cap_bounds, normalize, probe


class SubgroupProbeTest(unittest.TestCase):
    def test_caps_cover_members_and_handle_antipodal_center(self):
        rows = normalize(np.random.default_rng(593).normal(size=(37, 9)))
        queries = normalize(np.random.default_rng(594).normal(size=(19, 9)))
        lower, _ = cap_bounds(rows, queries)
        self.assertTrue(np.all(lower[:, None] <= 1 - queries @ rows.T + 1e-12))
        lower, radius = cap_bounds(np.array([[1.0, 0], [-1.0, 0]]), queries[:, :2])
        self.assertTrue(np.all(lower == 0))
        self.assertEqual(radius, 2)

    def test_oracle_is_labeled_and_singletons_prune_without_losing_ties(self):
        rows = np.array([[1, 0], [1, 0], [0, 1], [-1, 0]], dtype=np.float32)
        report = probe(rows, rows[:1], 1, [1, 4], 1, 593)
        self.assertIn("NOT production", report["qualification"])
        self.assertTrue(report["all_member_bounds_validated"])
        self.assertGreaterEqual(
            report["treatments"][-1]["oracle_surviving_vectors_mean"], 2
        )
        self.assertLess(report["treatments"][-1]["oracle_surviving_fraction"], 1)

    def test_invalid_vectors_fail_closed(self):
        with self.assertRaises(ValueError):
            normalize(np.zeros((1, 3)))
        with self.assertRaises(ValueError):
            normalize(np.array([[np.nan, 1]]))


if __name__ == "__main__":
    unittest.main()
