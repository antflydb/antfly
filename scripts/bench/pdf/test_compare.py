import copy
import unittest

from compare import comparable_pair, summarize


def run(seconds=(10, 8, 6)):
    manifest = {
        "unit_count": 1,
        "chunk_count": 1,
        "ocr_attempted_count": 1,
        "ocr_selected_count": 1,
        "ocr_failed_count": 0,
    }
    return {
        "returncode": 0,
        "metal_confirmed": True,
        "models": {"model.safetensors": {"sha256": "same"}},
        "table_config": {"same": True},
        "server_config": {"same": True},
        "provenance": {
            "binary_sha256": "binary",
            "revision": "revision",
            "selected": [{"path": "scan.pdf", "sha256": "same"}],
            "mode": "always",
            "suite": "scan",
            "batch": True,
            "circus_revision": "same",
            "read_profile": False,
            "reader_batch_size": None,
            "render_workers": None,
            "render_prefetch": None,
            "render_memory_bytes": None,
        },
        "results": [
            {
                "passed": True,
                "unit_text_sha256": {"scan.pdf": {"page:000001": "same"}},
                "unit_render_geometry": {
                    "scan.pdf": {"page:000001": {"ocr_effective_render_dpi": 150}}
                },
                "seconds": seconds[i],
                "documents": 1,
                "pages": 1,
                "manifests": {"scan.pdf": dict(manifest)},
                "indexes": [
                    {
                        "config": {"name": "document_vectors"},
                        "status": {"searchable_vectors": 1},
                    }
                ],
            }
            for i in range(len(seconds))
        ],
    }


class CompareTests(unittest.TestCase):
    def test_sync_level_paths_are_not_equivalent_experiments(self):
        before, after = run(), run()
        after["provenance"]["sync_level"] = "write"
        self.assertFalse(
            summarize([{"order": [], "main": before, "pr": after}], 3)[
                "timing_comparable"
            ]
        )

    def test_secondary_consumers_require_independent_output_checks(self):
        before, after = run(), run()
        for subject in (before, after):
            subject["provenance"]["consumers"] = 2
        self.assertFalse(
            summarize([{"order": [], "main": before, "pr": after}], 3)[
                "timing_comparable"
            ]
        )
        for subject in (before, after):
            for row in subject["results"]:
                row["consumer_results"] = [{}]
        self.assertFalse(
            summarize([{"order": [], "main": before, "pr": after}], 3)[
                "timing_comparable"
            ]
        )

    def test_resolution_and_batch_policy_changes_prevent_speedup(self):
        before = run()
        after = run()
        after["results"][0]["unit_render_geometry"]["scan.pdf"]["page:000001"][
            "ocr_effective_render_dpi"
        ] = 39
        self.assertFalse(
            summarize([{"order": ["main", "pr"], "main": before, "pr": after}], 3)[
                "timing_comparable"
            ]
        )
        after = run()
        after["provenance"]["reader_batch_size"] = 4
        self.assertFalse(
            summarize([{"order": ["main", "pr"], "main": before, "pr": after}], 3)[
                "timing_comparable"
            ]
        )

    def test_separates_first_and_warm_process_samples(self):
        report = summarize(
            [{"order": ["main", "pr"], "main": run(), "pr": run((5, 4, 3))}], 3
        )
        self.assertTrue(report["timing_comparable"])
        self.assertEqual(
            2, report["timings"]["first_process_trial"]["speedup_main_over_pr"]
        )
        warm = report["timings"]["warm_process_trials"]
        self.assertEqual(7, warm["main_seconds"])
        self.assertEqual([3.5], warm["per_process_median_seconds"]["pr"])

    def test_no_speedup_for_failed_missing_or_unequal_work(self):
        for mutation in (
            lambda r: r.update(returncode=1),
            lambda r: r["results"].pop(),
            lambda r: r["results"][0].update(passed=False),
            lambda r: r["results"][0]["manifests"]["scan.pdf"].update(chunk_count=2),
            lambda r: r["results"][0]["manifests"]["scan.pdf"].pop(
                "ocr_selected_count"
            ),
            lambda r: r["results"][0]["indexes"][0]["status"].update(
                searchable_vectors=2
            ),
            lambda r: r["results"][0].update(seconds=float("nan")),
            lambda r: r.update(metal_confirmed=False),
            lambda r: r.update(models={"different": True}),
            lambda r: r["provenance"].update(mode="auto"),
            lambda r: r["provenance"].update(render_workers=4),
            lambda r: r["provenance"].update(render_prefetch=0),
            lambda r: r["provenance"].update(render_memory_bytes=1),
        ):
            candidate = run()
            mutation(candidate)
            report = summarize([{"order": [], "main": run(), "pr": candidate}], 3)
            self.assertFalse(report["timing_comparable"])
            self.assertIsNone(report["timings"])

    def test_failed_pair_is_not_dropped_from_aggregate(self):
        good = {"order": [], "main": run(), "pr": run()}
        bad = copy.deepcopy(good)
        bad["pr"]["returncode"] = 1
        self.assertIsNone(summarize([good, bad], 3)["timings"])
        self.assertIsNone(summarize([], 3)["timings"])

    def test_binary_drift_and_text_differences_prevent_speedup(self):
        first = {"order": [], "main": run(), "pr": run()}
        second = copy.deepcopy(first)
        second["pr"]["provenance"]["binary_sha256"] = "changed"
        self.assertIsNone(summarize([first, second], 3)["timings"])
        second = copy.deepcopy(first)
        second["pr"]["results"][0]["unit_text_sha256"] = {"different": True}
        self.assertIsNone(summarize([second], 3)["timings"])

    def test_even_matching_profiled_runs_are_not_timing_evidence(self):
        before, after = run(), run()
        before["provenance"]["read_profile"] = True
        after["provenance"]["read_profile"] = True
        self.assertTrue(comparable_pair(before, after, 3))


if __name__ == "__main__":
    unittest.main()
