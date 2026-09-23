# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import run_laya_qualification as runner


class QualificationRunnerTest(unittest.TestCase):
    def run_child(self, status, fail_sampling=False):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / "fake-trainer"
            binary.write_text(
                "#!/usr/bin/env python3\nimport json,sys\nfrom pathlib import Path\nj=json.loads(Path(sys.argv[1]).read_text())\np=Path(j['output_dir']); p.mkdir()\n(p/'report.json').write_text("
                + repr(json.dumps({"status": status}))
                + ")\n"
            )
            binary.chmod(0o700)
            job = root / "original-job.json"
            job.write_text(
                json.dumps({"model_dir": str(root), "output_dir": str(root / "output")})
            )
            evidence = root / "evidence"
            counter = {
                "process_rss_bytes": 0,
                "swap_used_mib": 0,
                "Swapouts": 0,
                "Pageouts": 0,
            }
            samples = (
                [counter, RuntimeError("monitor unavailable"), counter]
                if fail_sampling
                else None
            )
            with (
                patch.object(runner, "required_disk_bytes", return_value=0),
                patch.object(
                    runner, "sample", return_value=counter, side_effect=samples
                ),
                patch.object(runner, "output", return_value="4096"),
                patch.object(runner.time, "sleep", return_value=None),
                self.assertRaises(SystemExit) as exited,
            ):
                runner.main(
                    [
                        "--binary",
                        str(binary),
                        "--job",
                        str(job),
                        "--evidence",
                        str(evidence),
                    ]
                )
            manifest = json.loads((evidence / "manifest.json").read_text())
            self.assertEqual(
                manifest["command"],
                [
                    str((evidence / "train-laya").resolve()),
                    str((evidence / "job.json").resolve()),
                ],
            )
            return exited.exception.code, json.loads(
                (evidence / "result.json").read_text()
            )

    def test_only_complete_report_returns_success(self):
        for status in ("complete", "paused", "failed"):
            with self.subTest(status=status):
                code, result = self.run_child(status)
                self.assertEqual(code == 0, status == "complete")
                self.assertEqual(result["complete"], status == "complete")

    def test_monitor_failure_is_durable_and_fails_closed(self):
        code, result = self.run_child("complete", fail_sampling=True)
        self.assertNotEqual(code, 0)
        self.assertFalse(result["complete"])
        self.assertIn("monitor unavailable", result["failure"])


if __name__ == "__main__":
    unittest.main()
