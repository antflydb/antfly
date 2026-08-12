#!/usr/bin/env python3

from __future__ import annotations

import json
import pathlib
import sys
import tempfile
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from paired_benchmark import (
    MANIFEST_SCHEMA,
    balanced_pair_order,
    build_evidence_manifest,
    paired_log_ratio_ci,
    write_evidence_manifest,
)


class PairedBenchmarkTest(unittest.TestCase):
    def test_balanced_pair_order_is_ab_ba(self) -> None:
        self.assertEqual(("a", "b"), balanced_pair_order(1, "a", "b"))
        self.assertEqual(("b", "a"), balanced_pair_order(2, "a", "b"))
        self.assertEqual(("a", "b"), balanced_pair_order(3, "a", "b"))
        with self.assertRaisesRegex(ValueError, "positive"):
            balanced_pair_order(0, "a", "b")

    def test_paired_log_ratio_ci_is_deterministic_and_paired(self) -> None:
        pairs = [(90.0, 100.0), (99.0, 110.0), (81.0, 90.0)]
        first = paired_log_ratio_ci(pairs, samples=1_000, seed=7)
        second = paired_log_ratio_ci(pairs, samples=1_000, seed=7)
        self.assertEqual(first, second)
        self.assertAlmostEqual(0.9, first["median"])
        self.assertLessEqual(first["lower_95"], first["median"])
        self.assertGreaterEqual(first["upper_95"], first["median"])
        self.assertEqual("median_paired_log_ratio", first["estimator"])

    def test_manifest_hashes_every_artifact_but_not_itself(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            (root / "raw").mkdir()
            (root / "raw/sample.json").write_text('{"sample":1}\n', encoding="utf-8")
            (root / "summary.json").write_text('{"passed":true}\n', encoding="utf-8")
            output = root / "evidence_manifest.json"
            manifest = write_evidence_manifest(root, output)
            stored = json.loads(output.read_text(encoding="utf-8"))
            self.assertEqual(manifest, stored)
            self.assertEqual(MANIFEST_SCHEMA, stored["schema"])
            self.assertEqual(2, stored["file_count"])
            self.assertNotIn("evidence_manifest.json", [item["path"] for item in stored["files"]])
            self.assertEqual(
                stored["files_sha256"],
                build_evidence_manifest(root, exclude=("evidence_manifest.json",))["files_sha256"],
            )


if __name__ == "__main__":
    unittest.main()
