import hashlib
import json
import tarfile
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import preserve_vdbbench_baseline as baseline


class PreserveBaselineTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.base = Path(temporary.name)
        self.root = self.base / "runs"
        self.root.mkdir()
        self.binary = self.base / "antfly"
        self.binary.write_bytes(b"measured executable")
        helper = self.base / "changed-helper.py"
        helper.write_text("new helper, not historical bytes")
        self.arm = self.root / "failed-arm"
        self.arm.mkdir()
        (self.arm / "server.log").write_text("catch-up failed")
        (self.arm / "data").mkdir()
        (self.arm / "data" / "large-payload").write_bytes(b"excluded data")
        (self.arm / "results").mkdir()
        (self.arm / "results" / "raw.json").write_text('{"status":"failed"}')
        self.receipts = [
            {
                "command": ["runner", str(self.arm)],
                "environment": {"ANTFLY_BIN": str(self.binary)},
                "exit_code": 1,
                "inputs_sha256": {
                    str(self.binary): baseline.digest(self.binary),
                    str(helper): "old-helper-digest",
                },
            }
        ]
        self.write_receipts()
        self.archive = self.base / "archive.tar.gz"
        self.catalog = self.base / "catalog.json"

    def write_receipts(self):
        (self.root / "ab-runs.json").write_text(json.dumps(self.receipts))

    def capture(self, summary=None):
        with patch.object(
            baseline,
            "summarize",
            side_effect=summary or (lambda _: {"individual_arms": []}),
        ):
            return baseline.preserve(self.root, self.archive, self.catalog)

    def test_preserves_binary_failure_and_raw_evidence_without_runtime_data(self):
        result = self.capture()
        self.assertEqual(result["receipts"][0]["exit_code"], 1)
        self.assertEqual(result["qualified_results"]["individual_arms"], [])
        self.assertEqual(result["archive"]["sha256"], baseline.digest(self.archive))
        with tarfile.open(self.archive) as archive:
            names = archive.getnames()
            self.assertIn("evidence/failed-arm/results/raw.json", names)
            self.assertTrue(any(name.endswith("/antfly") for name in names))
            self.assertFalse(
                any(
                    "large-payload" in name or "changed-helper.py" in name
                    for name in names
                )
            )
            for item in result["files"]:
                self.assertEqual(
                    hashlib.sha256(
                        archive.extractfile(item["path"]).read()
                    ).hexdigest(),
                    item["sha256"],
                )
        unavailable = [item for item in result["inputs"] if item["status"] != "matched"]
        self.assertEqual(len(unavailable), 1)
        original = self.catalog.read_bytes()
        with self.assertRaises(FileExistsError):
            self.capture()
        self.assertEqual(original, self.catalog.read_bytes())

    def test_rejects_changed_executable(self):
        self.binary.write_bytes(b"different executable")
        with self.assertRaisesRegex(ValueError, "executed binary"):
            self.capture()
        self.assertFalse(self.archive.exists())

    def test_rejects_evidence_changed_during_summary(self):
        def mutate(_):
            (self.arm / "server.log").write_text("edited during summary")
            return {}

        with self.assertRaisesRegex(ValueError, "changed while summarizing"):
            self.capture(mutate)
        self.assertFalse(self.archive.exists())
        self.assertFalse(self.catalog.exists())

    def test_rejects_escaping_arm(self):
        self.receipts[0]["command"][1] = str(self.base)
        self.write_receipts()
        with self.assertRaisesRegex(ValueError, "immediate child"):
            self.capture()

    def test_rejects_symlink_evidence(self):
        (self.arm / "linked.log").symlink_to(self.binary)
        with self.assertRaisesRegex(ValueError, "symlink"):
            self.capture()


if __name__ == "__main__":
    unittest.main()
