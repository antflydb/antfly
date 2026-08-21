#!/usr/bin/env python3

import importlib.util
import sys
import time
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name("run_recall_checks.py")
SPEC = importlib.util.spec_from_file_location("run_recall_checks", SCRIPT)
assert SPEC and SPEC.loader
recall = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = recall
SPEC.loader.exec_module(recall)


class RunRecallChecksTest(unittest.TestCase):
    def test_builds_storage_and_per_metric_commands(self):
        commands = recall.build_commands(
            Path("storage-tests"),
            Path("recall-harness"),
            Path("vectors"),
        )
        self.assertEqual(
            ("storage-hbc", ["storage-tests", "--test-filter", "HBC recall"]),
            commands[0],
        )
        self.assertEqual(
            ["harness-l2_squared", "harness-inner_product", "harness-cosine"],
            [label for label, _ in commands[1:]],
        )
        self.assertEqual(
            [
                "recall-harness",
                "--dataset-dir",
                "vectors",
                "--metric",
                "cosine",
            ],
            commands[-1][1],
        )

    def test_runs_all_commands_concurrently(self):
        command = [sys.executable, "-c", "import time; time.sleep(0.5)"]
        started = time.monotonic()
        self.assertEqual(
            0,
            recall.run_checks(tuple((str(index), command) for index in range(4))),
        )
        self.assertLess(time.monotonic() - started, 0.9)


if __name__ == "__main__":
    unittest.main()
