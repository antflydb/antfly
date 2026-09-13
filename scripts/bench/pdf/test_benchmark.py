import copy
import io
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import benchmark
from benchmark import (
    artifact_errors,
    completed_log_offset,
    coverage_ready,
    unit_text_hashes,
)


class LogCheckpointTests(unittest.TestCase):
    def test_checkpoint_retains_partial_records_for_later_reads(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "server.log"
            for complete, partial in (
                (b"", b""),
                (b"", b"partial"),
                (b"complete\n", b""),
                ("diagnostic café\r\n".encode(), b"partial \xc3"),
                (b"complete\n", b"x" * 20000),
            ):
                with self.subTest(size=len(partial)):
                    path.write_bytes(complete + partial)
                    self.assertEqual(len(complete), completed_log_offset(path))
                    with path.open("ab") as stream:
                        stream.write(b"finished\n")
                    self.assertEqual(path.stat().st_size, completed_log_offset(path))

    def test_checkpoint_does_not_chase_concurrent_appends_or_read_unboundedly(self):
        class AppendingLog(io.BytesIO):
            max_read = 0

            def read(self, size=-1):
                self.max_read = max(self.max_read, size)
                self.assert_bounded(size)
                position = self.tell()
                self.seek(0, 2)
                self.write(b"later complete record\n")
                self.seek(position)
                return super().read(size)

            @staticmethod
            def assert_bounded(size):
                if not 0 < size <= 8192:
                    raise AssertionError(f"unbounded read: {size}")

        log = AppendingLog(b"complete\n" + b"x" * 20000)
        with patch.object(Path, "open", return_value=log):
            self.assertEqual(len(b"complete\n"), completed_log_offset(Path("unused")))
        self.assertEqual(8192, log.max_read)


class CompletionTests(unittest.TestCase):
    def test_render_controls_are_explicit_and_ambient_overrides_removed(self):
        args = SimpleNamespace(
            read_profile=False,
            reader_batch_size=4,
            render_workers=2,
            render_prefetch=0,
            render_memory_bytes=268435456,
        )
        environment = benchmark.runtime_environment(
            args,
            {
                "PATH": "/bin",
                "ANTFLY_SECRET": "ignored",
                "ANTFLY_ENRICHMENT_OCR_RENDER_PARALLEL_PAGES": "8",
            },
        )
        self.assertNotIn("ANTFLY_SECRET", environment)
        self.assertEqual(environment["PATH"], "/bin")
        self.assertEqual(environment[benchmark.RENDER_CONTROLS["render_workers"]], "2")
        self.assertEqual(environment[benchmark.RENDER_CONTROLS["render_prefetch"]], "0")
        self.assertEqual(
            environment[benchmark.RENDER_CONTROLS["render_memory_bytes"]], "268435456"
        )

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
        self.assertEqual(page["render_quality_warnings"], [])
        unit["extraction_warning"] = (
            "pdf_render_quality:degraded:fallback_groups=1:reason=materialization_limit;ocr_numeric_table_hybrid"
        )
        unit_text_hashes(manifests, lambda _: unit, geometry)
        self.assertEqual(
            geometry["scan.pdf"]["page:000001"]["render_quality_warnings"],
            [
                "pdf_render_quality:degraded:fallback_groups=1:reason=materialization_limit"
            ],
        )
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
