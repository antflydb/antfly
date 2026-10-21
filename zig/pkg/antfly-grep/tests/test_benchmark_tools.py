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

"""Ensure Linux RSS excludes the allocating benchmark parent."""

import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "bench"))
from compare import linux_time, measure, output_digest, target_rss


class BenchmarkOutputTests(unittest.TestCase):
    def test_statistics_compare_counters_across_output_streams(self):
        rows = "1:1:hit\n\n3:1:hit\n"
        counters = "2 matches\n2 matched lines\n1 files contained matches\n1 files searched\n9 bytes searched\n"
        results = []
        for stdout, stderr in (
            (rows, counters + "0.1s total\n"),
            (rows + "\n" + counters + "0.9s time elapsed\n", ""),
        ):
            script = (
                f"import sys;sys.stdout.write({stdout!r});sys.stderr.write({stderr!r})"
            )
            results.append(
                output_digest(
                    [sys.executable, "-c", script],
                    None,
                    os.environ,
                    statistics_output=True,
                )
            )
        self.assertEqual(*results)
        self.assertEqual(results[0]["statistics"]["matches"], 2)
        self.assertEqual(results[0]["bytes"], len(rows.encode()))
        with self.assertRaisesRegex(RuntimeError, "missing statistic"):
            output_digest(
                [sys.executable, "-c", "print('hit')"],
                None,
                os.environ,
                statistics_output=True,
            )


@unittest.skipUnless(
    sys.platform.startswith("linux") and linux_time(), "Linux GNU time required"
)
class BenchmarkMemoryTests(unittest.TestCase):
    def test_small_child_does_not_inherit_parent_peak(self):
        parent = bytearray(96 * 1024 * 1024)
        parent[-1] = 1
        with tempfile.TemporaryDirectory() as directory:
            small = target_rss(
                [sys.executable, "-c", "pass"], directory, os.environ, linux_time()
            )
            large = target_rss(
                [sys.executable, "-c", "a=bytearray(64*1024*1024);a[-1]=1"],
                directory,
                os.environ,
                linux_time(),
            )
            self.assertLess(small, 64 * 1024)
            self.assertGreaterEqual(large, 64 * 1024)
            self.assertGreater(large, small)
            self.assertIsNone(
                measure([sys.executable, "-c", "pass"], directory, os.environ)[
                    "peak_rss_kib"
                ]
            )
        self.assertEqual(parent[-1], 1)


if __name__ == "__main__":
    unittest.main()
