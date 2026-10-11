# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
from pathlib import Path
import subprocess
import tempfile
import unittest

from summarize_zig_compile_memory import summarize


class DiagnosticTests(unittest.TestCase):
    def test_exact_command_keeps_arguments_failure_and_compiler_cpu_samples(self):
        script = Path(__file__).with_name("diagnose-zig-build-memory.sh")
        with tempfile.TemporaryDirectory(prefix="antfly-command-test-") as tmp:
            root = Path(tmp)
            response = root / "compiler-args"
            response.write_text("--name antfly-storage-kernel -Ofast")
            zig = root / "zig"
            zig.write_text(
                "#!/bin/sh\n"
                'if [ "$1" = build ]; then\n'
                '  printf "%s" "$2" > "$ARGUMENT_FILE"\n'
                '  "$0" build-lib "@$RESPONSE_FILE" &\n'
                '  wait "$!"\n'
                "else\n"
                "  sleep 1\n"
                "  exit 7\n"
                "fi\n"
            )
            zig.chmod(0o700)
            result = subprocess.run(
                [
                    str(script),
                    "--command",
                    "--out-dir",
                    str(root / "logs"),
                    "--prefix",
                    str(root / "prefix"),
                    "--label",
                    "command",
                    "--interval",
                    "0.05",
                    "--no-stack-sample",
                    "--",
                    str(zig),
                    "build",
                    "two words",
                ],
                env=dict(
                    os.environ,
                    ARGUMENT_FILE=str(root / "argument"),
                    RESPONSE_FILE=str(response),
                ),
                capture_output=True,
                text=True,
                timeout=10,
            )
            self.assertEqual(result.returncode, 7, result.stderr)
            self.assertEqual((root / "argument").read_text(), "two words")
            summary = (root / "logs/command.summary.txt").read_text()
            self.assertIn("command_mode=1\n", summary)
            self.assertIn("jobs=command-defined\n", summary)
            self.assertIn("memory_report_status=0\n", summary)
            rows = (root / "logs/command.cpu.tsv").read_text().splitlines()
            self.assertGreater(len(rows), 1)
            for row in rows[1:]:
                fields = row.split("\t")
                self.assertEqual(len(fields), 5)
                self.assertTrue(fields[2])
                self.assertTrue(fields[3])
            report = summarize((root / "logs/command.rss.tsv").read_text())
            self.assertGreater(report["units"]["storage_kernel"]["peak_rss_bytes"], 0)
            response.unlink()
            self.assertEqual(
                summarize(
                    (root / "logs/command.rss.tsv").read_text(),
                    response_dir=root / "logs/command.response-files",
                ),
                report,
            )

    def test_live_build_produces_complete_seven_column_samples_and_summary(self):
        script = Path(__file__).with_name("diagnose-zig-build-memory.sh")
        with tempfile.TemporaryDirectory(prefix="antfly-memory-test-") as tmp:
            root = Path(tmp)
            zig = root / "zig"
            zig.write_text(
                "#!/bin/sh\n"
                'if [ "$1" = build ]; then\n'
                '  "$0" build-exe --name antfly-runtime-inference &\n'
                '  wait "$!"\n'
                "else\n"
                "  sleep 1\n"
                "fi\n"
            )
            zig.chmod(0o700)
            env = dict(os.environ, ZIG_BIN=str(zig))
            env.pop("sample_id", None)
            result = subprocess.run(
                [
                    "bash",
                    str(script),
                    "--out-dir",
                    str(root / "logs"),
                    "--prefix",
                    str(root / "prefix"),
                    "--label",
                    "test",
                    "--interval",
                    "0.05",
                    "--no-stack-sample",
                ],
                env=env,
                capture_output=True,
                text=True,
                timeout=10,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("status=0\n", (root / "logs/test.summary.txt").read_text())
            trace = (root / "logs/test.rss.tsv").read_text()
            rows = [line.split("\t") for line in trace.splitlines()]
            self.assertEqual(
                rows[0],
                [
                    "timestamp",
                    "elapsed_s",
                    "pid",
                    "rss_kb",
                    "rss_mb",
                    "command",
                    "sample_id",
                ],
            )
            self.assertGreater(len(rows), 1)
            for row in rows[1:]:
                self.assertEqual(len(row), 7)
                self.assertGreater(int(row[6]), 0)
            report = summarize(trace)
            self.assertTrue(report["aggregate_is_same_poll"])
            self.assertGreater(report["units"]["inference"]["peak_rss_bytes"], 0)


if __name__ == "__main__":
    unittest.main()
