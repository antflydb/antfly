import copy
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import benchmark
from benchmark import artifact_errors, coverage_ready, unit_text_hashes


class CompletionTests(unittest.TestCase):
    def test_geometry_reads_provenance_and_rejects_missing_render_metadata(self):
        manifests = {
            "scan.pdf": {
                "unit_count": 1,
                "state_json": json.dumps({"unit_keys": ["key"]}),
            }
        }
        unit = {
            "unit_id": "page:000001",
            "text": "A short note",
            "ocr_attempted": True,
            "ocr_render_dpi": 150,
            "ocr_effective_render_dpi": 138,
            "ocr_rendered_width": 4094,
            "ocr_rendered_height": 2750,
            "provenance": {
                "page_number": 1,
                "page_bbox": [0, 0, 2136, 1435],
                "page_rotation": 0,
            },
        }
        geometry = {}
        unit_text_hashes(manifests, lambda _: unit, geometry)
        page = geometry["scan.pdf"]["page:000001"]
        self.assertEqual(page["page_number"], 1)
        self.assertEqual(page["ocr_effective_render_dpi"], 138)
        del unit["ocr_effective_render_dpi"]
        with self.assertRaisesRegex(ValueError, "missing page geometry"):
            unit_text_hashes(manifests, lambda _: unit, {})
        unit["ocr_attempted"] = False
        unit_text_hashes(manifests, lambda _: unit, {})
        del unit["provenance"]["page_number"]
        with self.assertRaisesRegex(ValueError, "missing page geometry"):
            unit_text_hashes(manifests, lambda _: unit, {})

    def test_unit_hashes_compare_text_not_volatile_metadata(self):
        manifest = {
            "scan.pdf": {
                "unit_count": 1,
                "state_json": json.dumps({"unit_keys": ["key"]}),
            }
        }
        first = unit_text_hashes(
            manifest,
            lambda _: {
                "unit_id": "page:000001",
                "text": "A short note",
                "generation": 1,
            },
        )
        second = unit_text_hashes(
            manifest,
            lambda _: {
                "unit_id": "page:000001",
                "text": "A short note",
                "generation": 2,
            },
        )
        changed = unit_text_hashes(
            manifest, lambda _: {"unit_id": "page:000001", "text": "different"}
        )
        self.assertEqual(first, second)
        self.assertNotEqual(first, changed)
        manifest["scan.pdf"]["unit_count"] = 2
        with self.assertRaises(ValueError):
            unit_text_hashes(manifest, lambda _: {})

    def setUp(self):
        self.status = {
            "searchable_vectors": 8,
            "coverage": {
                "complete": True,
                "healthy": True,
                "observation_complete": True,
                "source_total": 4,
                "produced": 4,
                "covered": 4,
                "terminal_failed": 0,
            },
        }

    def test_requires_nonempty_current_coverage(self):
        self.assertTrue(coverage_ready(self.status, 4))
        self.assertFalse(coverage_ready(self.status, 5))
        for field in ("source_total", "produced", "covered"):
            status = copy.deepcopy(self.status)
            status["coverage"][field] = 0
            self.assertFalse(coverage_ready(status, 4))

    def test_requires_published_vectors_and_healthy_observation(self):
        for field in ("complete", "healthy", "observation_complete"):
            status = copy.deepcopy(self.status)
            status["coverage"][field] = False
            self.assertFalse(coverage_ready(status, 4))
        self.status["searchable_vectors"] = 0
        self.assertFalse(coverage_ready(self.status, 4))

    def test_rejects_partial_pdf_or_failed_ocr(self):
        selected = [{"path": "scan.pdf", "pages": 3, "role": "ocr_required"}]
        manifest = {
            "unit_count": 3,
            "chunk_count": 5,
            "ocr_failed_count": 0,
            "ocr_selected_count": 3,
        }
        self.assertEqual([], artifact_errors(selected, {"scan.pdf": manifest}))
        for field, value in (
            ("unit_count", 2),
            ("chunk_count", 0),
            ("ocr_failed_count", 1),
            ("ocr_selected_count", 0),
        ):
            changed = dict(manifest, **{field: value})
            self.assertTrue(artifact_errors(selected, {"scan.pdf": changed}))

    def test_setup_failure_is_retained_and_previous_run_is_not_overwritten(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            args = SimpleNamespace(name="failed-run")
            with (
                patch.object(benchmark, "ROOT", root),
                patch.object(
                    benchmark, "run_created", side_effect=ValueError("missing model")
                ),
            ):
                with self.assertRaisesRegex(ValueError, "missing model"):
                    benchmark.run(args)
                failure = root / args.name / "failure.json"
                original = failure.read_bytes()
                self.assertEqual(json.loads(original)["completed_trials"], 0)
                with self.assertRaises(FileExistsError):
                    benchmark.run(args)
                self.assertEqual(original, failure.read_bytes())


if __name__ == "__main__":
    unittest.main()
