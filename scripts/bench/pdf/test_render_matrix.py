import unittest

from compare import summarize
from render_matrix import ablation_pair, render_observations
from test_compare import run


class RenderMatrixTests(unittest.TestCase):
    def test_only_declared_controls_may_change(self):
        before, after = run(), run()
        after["provenance"].update(render_workers=4, render_prefetch=0)
        self.assertTrue(
            summarize([ablation_pair(before, after)], 3)["timing_comparable"]
        )
        self.assertEqual(after["provenance"]["render_workers"], 4)
        for field, value in (("render_memory_bytes", 512), ("reader_batch_size", 8)):
            changed = run()
            changed["provenance"][field] = value
            self.assertFalse(
                summarize([ablation_pair(before, changed)], 3)["timing_comparable"]
            )
        after["provenance"]["binary_sha256"] = "other"
        with self.assertRaises(ValueError):
            ablation_pair(before, after)

    def test_profiling_and_quality_failures_remain_disqualifying(self):
        before, after = run(), run()
        before["provenance"]["read_profile"] = True
        after["provenance"]["read_profile"] = True
        self.assertIsNone(summarize([ablation_pair(before, after)], 3)["timings"])
        before, after = run(), run()
        after["results"][0]["unit_text_sha256"] = {}
        self.assertFalse(
            summarize([ablation_pair(before, after)], 3)["timing_comparable"]
        )

    def test_admission_and_execution_are_distinct(self):
        rows = render_observations(
            """info: read-profile phase=pdf_window_grant requested_workers=4 admitted_workers=1
info: read-profile phase=pdf_render_window requested_parallelism=1 peak_parallelism=1 elapsed_ms=12.5
info: unrelated phase=pdf_render
info: read-profile phase=batch_encoder elapsed_ms=3"""
        )
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["requested_workers"], "4")
        self.assertEqual(rows[0]["admitted_workers"], "1")
        self.assertEqual(rows[1]["peak_parallelism"], "1")
