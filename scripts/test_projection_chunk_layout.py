import unittest

import numpy as np
from probe_projection_chunk_layout import (
    packed_locations,
    replay,
    replay_contiguous,
    source_order,
)


class ProjectionChunkLayoutTest(unittest.TestCase):
    def test_contiguous_scheduler_never_reads_gaps(self):
        observed = replay_contiguous(
            [[0, 1, 2, 3]], [(0, 0, 4), (0, 4, 4), (0, 12, 4), (1, 0, 4)]
        )
        self.assertEqual(observed, {"physical_reads": 3, "physical_bytes": 16})

    def test_source_training_is_deterministic_and_single_copy(self):
        rows = np.asarray(
            [[1, 0], [-1, 0], [0.9, 0.1], [-0.9, -0.1], [0, 0]], dtype=np.float32
        )
        order = source_order(rows, 2)
        np.testing.assert_array_equal(order, source_order(rows, 2))
        self.assertEqual(sorted(order), list(range(len(rows))))
        layout = packed_locations(order.tolist(), [4] * len(rows), 8)
        self.assertEqual(len(set(layout)), len(rows))
        self.assertEqual(sum(x[2] for x in layout), 20)

    def test_coalescing_cross_page_and_cache_are_counted(self):
        locations = [(0, 0, 4), (0, 4, 4), (1, 6, 4)]
        result = replay([[0, 1, 2]], locations, page_bytes=8)
        self.assertEqual(result["physical_reads"], 3)
        self.assertEqual(result["physical_bytes"], 12)
        result = replay([[0, 1], [0, 1]], locations, page_bytes=8, cache_bytes=8)
        self.assertEqual(result["physical_reads"], 1)
        self.assertEqual(result["physical_bytes"], 8)
        self.assertEqual(result["page_hits"], 1)

    def test_small_cache_cannot_hide_scattered_read_cost(self):
        result = replay(
            [[0, 1], [0, 1]], [(0, 0, 4), (1, 0, 4)], page_bytes=8, cache_bytes=8
        )
        self.assertEqual(result["physical_reads"], 4)
        self.assertEqual(result["physical_bytes"], 32)

    def test_invalid_and_duplicate_placements_rejected(self):
        with self.assertRaises(ValueError):
            packed_locations([0, 0], [4, 4], 8)
        with self.assertRaises(ValueError):
            source_order(np.asarray([[np.nan]], dtype=np.float32), 1)


if __name__ == "__main__":
    unittest.main()
