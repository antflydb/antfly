import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

import benchmark_resources as guard


class ResourceGuardTests(unittest.TestCase):
    def test_group_rss_counts_workers_without_the_leader(self):
        self.assertEqual(guard.group_rss_mib(7, "7 1024\n9 8192\n7 2048\n"), 3)

    def test_preflight_failure_does_not_launch(self):
        with (
            tempfile.TemporaryDirectory() as directory,
            patch.object(guard, "memory_free_percent", return_value=2),
            patch.object(guard, "swapout_bytes", return_value=0),
            patch.object(guard.subprocess, "Popen") as launch,
        ):
            output = Path(directory) / "report.json"
            result = guard.run_guarded(["unused"], output, min_disk_free_mib=0)
            launch.assert_not_called()
            self.assertFalse(result["pass"])
            self.assertIn("free memory", result["violation"])
            self.assertEqual(json.loads(output.read_text()), result)

    def test_timeout_terminates_owned_command(self):
        with (
            tempfile.TemporaryDirectory() as directory,
            patch.object(guard, "memory_free_percent", return_value=80),
            patch.object(guard, "swapout_bytes", return_value=0),
            patch.object(guard, "group_rss_mib", return_value=1),
        ):
            result = guard.run_guarded(
                [sys.executable, "-c", "import time; time.sleep(30)"],
                Path(directory) / "report.json",
                timeout=0.1,
                interval=0.05,
                min_disk_free_mib=0,
            )
            self.assertFalse(result["pass"])
            self.assertIn("timeout", result["violation"])
            self.assertLess(result["elapsed_seconds"], 5)
            self.assertLess(result["returncode"], 0)


if __name__ == "__main__":
    unittest.main()
