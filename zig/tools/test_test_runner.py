#!/usr/bin/env python3
"""Exercise runtime selection against a small compiled test inventory."""

import subprocess
import tempfile
import unittest
from pathlib import Path

ZIG_ROOT = Path(__file__).resolve().parents[1]


class TestRunnerSelection(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        root = Path(cls.temp.name)
        source = root / "selection.zig"
        source.write_text(
            'test "enrichment split" {}\ntest "enrichment merge" {}\ntest "unrelated anchor" {}\n'
        )
        cls.binary = root / "tests"
        subprocess.run(
            [
                "zig",
                "test",
                str(source),
                "--test-runner",
                str(ZIG_ROOT / "pkg/antfly/src/test_runner.zig"),
                "--test-no-exec",
                f"-femit-bin={cls.binary}",
                "--cache-dir",
                str(root / "cache"),
                "--global-cache-dir",
                "/tmp/zig-global-cache",
            ],
            check=True,
        )

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    def run_selection(self, *args):
        return subprocess.run(
            [str(self.binary), "--suite-filter", "enrichment", *args],
            text=True,
            capture_output=True,
        )

    def test_default_and_narrowed_inventory(self):
        result = self.run_selection("--list-tests")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stderr.count("TEST\t"), 2)
        self.assertNotIn("unrelated", result.stderr)
        result = self.run_selection("--list-tests", "--test-filter", "split")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stderr.count("TEST\t"), 1)
        self.assertIn("enrichment split", result.stderr)

    def test_anchor_cannot_satisfy_caller_filter(self):
        result = self.run_selection("--test-filter", "anchor")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("matched no declared tests", result.stderr)

    def test_execution_and_skip(self):
        result = self.run_selection("--skip-test-filter", "merge")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("enrichment split", result.stderr)
        self.assertNotIn("enrichment merge", result.stderr)


if __name__ == "__main__":
    unittest.main()
