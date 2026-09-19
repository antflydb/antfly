#!/usr/bin/env python3
"""Exercise runtime selection against a small compiled test inventory."""

import os
import subprocess
import tempfile
import time
import unittest
from pathlib import Path

ZIG_ROOT = Path(__file__).resolve().parents[1]


class TestRunnerSelection(unittest.TestCase):
    runner_path = ZIG_ROOT / "pkg/antfly/src/test_runner.zig"
    progress_prefix = "test"

    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        root = Path(cls.temp.name)
        source = root / "selection.zig"
        source.write_text(
            'const std = @import("std");\n'
            'test "enrichment split" {}\ntest "enrichment merge" {}\ntest "unrelated anchor" {}\n'
            'test "enrichment logging" { std.log.debug("quiet debug", .{}); '
            'std.log.info("quiet info", .{}); std.log.warn("visible warning", .{}); }\n'
            'test "verbose logging" { std.testing.log_level = .debug; '
            'std.log.debug("visible debug", .{}); }\n'
            'test "timed body" { try std.testing.io.sleep(.fromMilliseconds(20), .awake); }\n'
            'test "progress body" { try std.testing.io.sleep(.fromSeconds(2), .awake); }\n'
            'test "error logging" { std.log.err("visible error", .{}); }\n'
        )
        cls.binary = root / "tests"
        subprocess.run(
            [
                "zig",
                "test",
                "--dep",
                "antfly_platform",
                f"-Mroot={source}",
                f"-Mantfly_platform={ZIG_ROOT / 'lib/platform/src/root.zig'}",
                "--test-runner",
                str(cls.runner_path),
                "--test-no-exec",
                "-lc",
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

    def test_timings_measure_body_and_cleanup(self):
        result = subprocess.run(
            [str(self.binary), "--test-filter", "timed body"],
            text=True,
            capture_output=True,
            env={**os.environ, "ANTFLY_TEST_TIMINGS": "1"},
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        records = [
            line.split("\t")
            for line in result.stderr.splitlines()
            if line.startswith("TIMING\t")
        ]
        self.assertEqual(len(records), 1, result.stderr)
        _, setup, body, io_cleanup, allocator_cleanup, name = records[0]
        self.assertIn("timed body", name)
        self.assertGreaterEqual(int(body), 20_000_000)
        for value in (setup, io_cleanup, allocator_cleanup):
            self.assertGreaterEqual(int(value), 0)
        disabled = subprocess.run(
            [str(self.binary), "--test-filter", "timed body"],
            text=True,
            capture_output=True,
            env={**os.environ, "ANTFLY_TEST_TIMINGS": "0"},
        )
        self.assertEqual(disabled.returncode, 0, disabled.stderr)
        self.assertNotIn("TIMING\t", disabled.stderr)

    def test_progress_survives_captured_output_before_exit(self):
        with tempfile.TemporaryDirectory() as directory:
            process = subprocess.Popen(
                [str(self.binary), "--test-filter", "progress body"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env={**os.environ, "ANTFLY_TEST_LOG_DIR": directory},
            )
            try:
                path = Path(directory) / f"{self.progress_prefix}-{process.pid}.log"
                deadline = time.monotonic() + 5
                while time.monotonic() < deadline:
                    if path.exists() and "START\t" in path.read_text():
                        break
                    time.sleep(0.01)
                else:
                    self.fail("test attribution was buffered until process exit")
                self.assertIsNone(process.poll())
                progress = path.read_text()
                self.assertIn(f"ARG\t{self.binary}\n", progress)
                self.assertIn("progress body", progress)
                self.assertNotIn("DONE\t", progress)
                _, stderr = process.communicate(timeout=5)
                self.assertEqual(process.returncode, 0, stderr.decode())
                progress = path.read_text()
                for phase in ("IO_DEINIT", "ALLOCATOR_DEINIT", "DONE"):
                    self.assertIn(f"{phase}\tselection.test.progress body\n", progress)
            finally:
                if process.poll() is None:
                    process.kill()
                process.communicate()

    def test_inventory_does_not_create_progress_logs(self):
        with tempfile.TemporaryDirectory() as directory:
            result = subprocess.run(
                [str(self.binary), "--list-tests"],
                capture_output=True,
                env={**os.environ, "ANTFLY_TEST_LOG_DIR": directory},
            )
            self.assertEqual(result.returncode, 0, result.stderr.decode())
            self.assertEqual(list(Path(directory).iterdir()), [])

    def test_default_and_narrowed_inventory(self):
        result = self.run_selection("--list-tests")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stderr.count("TEST\t"), 3)
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

    def test_logging_obeys_per_test_level(self):
        result = self.run_selection("--test-filter", "logging")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotIn("quiet debug", result.stderr)
        self.assertNotIn("quiet info", result.stderr)
        self.assertIn("visible warning", result.stderr)
        verbose = subprocess.run(
            [str(self.binary), "--test-filter", "verbose logging"],
            text=True,
            capture_output=True,
        )
        self.assertEqual(verbose.returncode, 0, verbose.stderr)
        self.assertIn("visible debug", verbose.stderr)
        failure = subprocess.run(
            [str(self.binary), "--test-filter", "error logging"],
            text=True,
            capture_output=True,
            env={**os.environ, "ANTFLY_TEST_FAIL_ON_ERROR_LOGS": "1"},
        )
        self.assertNotEqual(failure.returncode, 0)
        self.assertIn("visible error", failure.stderr)


class InferenceRunnerProgress(unittest.TestCase):
    runner_path = ZIG_ROOT / "pkg/inference/src/test_runner_filter.zig"
    progress_prefix = "inference-test"
    setUpClass = classmethod(TestRunnerSelection.setUpClass.__func__)
    tearDownClass = classmethod(TestRunnerSelection.tearDownClass.__func__)
    test_progress_survives_captured_output_before_exit = (
        TestRunnerSelection.test_progress_survives_captured_output_before_exit
    )

    def test_inventory_preserves_selection_without_running_tests(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "inventory.txt"
            result = subprocess.run(
                [
                    str(self.binary),
                    "--test-filter",
                    "enrichment",
                    "--skip-test-filter",
                    "merge",
                ],
                capture_output=True,
                timeout=5,
                env={**os.environ, "ANTFLY_INFERENCE_TEST_LIST_FILE": str(output)},
            )
            self.assertEqual(result.returncode, 0, result.stderr.decode())
            self.assertEqual(
                output.read_text().splitlines(),
                [
                    "selection.test.enrichment split",
                    "selection.test.enrichment logging",
                ],
            )
            self.assertNotIn(b"visible warning", result.stderr)


if __name__ == "__main__":
    unittest.main()
