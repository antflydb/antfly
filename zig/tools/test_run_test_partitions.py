#!/usr/bin/env python3

import importlib.util
import sys
import time
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name("run_test_partitions.py")
SPEC = importlib.util.spec_from_file_location("run_test_partitions", SCRIPT)
assert SPEC and SPEC.loader
partitions = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = partitions
SPEC.loader.exec_module(partitions)


class RunTestPartitionsTest(unittest.TestCase):
    def test_builds_disjoint_partition_and_complement_commands(self):
        partition, complement = partitions.build_commands(
            Path("test-binary"),
            ["db restore", "db dense"],
            ["simulation", "release scale"],
            ["--seed=123", "--skip-test-filter", "caller skip"],
        )
        self.assertEqual(
            [
                "test-binary",
                "--test-filter",
                "db restore",
                "--test-filter",
                "db dense",
                "--skip-test-filter",
                "simulation",
                "--skip-test-filter",
                "release scale",
                "--seed=123",
                "--skip-test-filter",
                "caller skip",
            ],
            partition,
        )
        self.assertEqual(
            [
                "test-binary",
                "--test-filter",
                "storage.",
                "--skip-test-filter",
                "simulation",
                "--skip-test-filter",
                "release scale",
                "--skip-test-filter",
                "db restore",
                "--skip-test-filter",
                "db dense",
                "--seed=123",
                "--skip-test-filter",
                "caller skip",
            ],
            complement,
        )

    def test_runs_both_commands_concurrently(self):
        command = [sys.executable, "-c", "import time; time.sleep(0.5)"]
        started = time.monotonic()
        self.assertEqual(
            0,
            partitions.run_partitions((("first", command), ("second", command))),
        )
        self.assertLess(time.monotonic() - started, 0.85)


if __name__ == "__main__":
    unittest.main()
