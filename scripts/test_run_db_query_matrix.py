#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
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

"""Check matrix workloads, evidence collection, and failure propagation."""

import argparse
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch
import run_db_query_matrix as matrix


class QueryMatrixTest(unittest.TestCase):
    def test_workloads_preserve_previous_settings(self):
        storage = matrix.cases("smoke", "storage")
        self.assertEqual(len(storage), 3)
        self.assertEqual([c[2][2] for c in storage], ["128", "256", "384"])
        self.assertEqual(
            [c[2][2] for c in matrix.cases("bounded", "storage")],
            ["1024", "2048", "2048"],
        )
        public = matrix.cases("bounded", "public")
        self.assertEqual(len(public), 6)
        self.assertIn("100000", public[0][2])
        self.assertIn("--with-sparse", public[-1][2])
        self.assertIn("--with-algebraic", public[-1][2])

    def run_fake(self, root, *, exit_code=0, emit_summary=True, fail_comparison=False):
        calls = []

        def run(command, **kwargs):
            calls.append(command)
            if command[0] == "zig":
                return subprocess.CompletedProcess(command, 0)
            binary = Path(command[0]).name
            event = (
                "public_query_guardrail_summary"
                if binary == "api_bench"
                else {
                    "query": "docid_query_bench_summary",
                    "analytics": "graph_algebraic_traversal"
                    if "graph-traversal" in command
                    else "dataset",
                    "summary": "performance_evidence_summary",
                }[command[1]]
            )
            if binary == "storage_bench" and command[1] == "summary":
                records = [
                    json.loads(line)
                    for line in (root / "out/combined.jsonl").read_text().splitlines()
                ]
                self.assertTrue(records)
                self.assertTrue(
                    all(record["case"] == "internal_case" for record in records)
                )
                self.assertTrue(all("matrix_case" in record for record in records))
                if fail_comparison:
                    return subprocess.CompletedProcess(command, 8)
            if emit_summary:
                kwargs["stderr"].write(
                    json.dumps({"event": event, "case": "internal_case"}) + "\n"
                )
            return subprocess.CompletedProcess(command, exit_code)

        args = argparse.Namespace(
            profile="smoke",
            suite="all",
            public_docs=None,
            analytics_docs=None,
            analytics_arg=[],
            summary_arg=["--max-algebraic-query-ms", "25"],
            baseline=root / "baseline.jsonl",
            out=root / "out",
            bin_dir=root / "bin",
            skip_build=False,
            storage_arg=[],
            public_arg=[],
        )
        with (
            patch.object(matrix.platform, "platform", return_value="test-platform"),
            patch.object(matrix.subprocess, "run", side_effect=run),
            patch.object(matrix.subprocess, "check_output", return_value="commit\n"),
        ):
            matrix.run_matrix(args)
        return calls

    def test_build_once_and_collect_both_streams(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            calls = self.run_fake(root)
            self.assertEqual(
                calls[0],
                ["zig", "build", "antfly-storage-bench", "antfly-api-bench"],
            )
            self.assertEqual(len(calls), 18)
            self.assertEqual(Path(calls[-1][0]).name, "storage_bench")
            self.assertEqual(calls[-1][1], "summary")
            self.assertIn(str((root / "baseline.jsonl").resolve()), calls[-1])
            self.assertEqual(calls[-1][-2:], ["--max-algebraic-query-ms", "25"])
            self.assertEqual(
                len((root / "out/summary.jsonl").read_text().splitlines()), 17
            )
            self.assertEqual(
                len((root / "out/status.tsv").read_text().splitlines()), 17
            )

    def test_analytics_preserves_production_workloads(self):
        bounded = matrix.cases("bounded", "analytics")
        self.assertEqual(len(bounded), 7)
        self.assertEqual(
            [c[0] for c in bounded[:4]],
            ["analytics", "adaptive_coverage", "cold_warm_reads", "graph_traversal"],
        )
        self.assertIn("50000", bounded[0][2])
        self.assertIn("5000", bounded[0][2])
        self.assertIn("10000", bounded[3][2])
        self.assertIn("128", bounded[4][2])
        self.assertIn("--with-schema", bounded[5][2])
        self.assertIn("--with-algebraic", bounded[6][2])
        smoke = matrix.cases("smoke", "analytics", public_docs=42, analytics_docs=50)
        self.assertIn("50", smoke[0][2])
        self.assertIn("42", smoke[4][2])

    def test_comparison_failure_stops_the_matrix(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            with self.assertRaisesRegex(SystemExit, "comparison failed"):
                self.run_fake(root, fail_comparison=True)
            self.assertTrue(
                (root / "out/status.tsv").read_text().endswith("comparison\t8\n")
            )

    def test_failure_and_missing_evidence_fail(self):
        for code, summary in ((7, True), (0, False)):
            with (
                self.subTest(code=code, summary=summary),
                tempfile.TemporaryDirectory() as temp,
            ):
                root = Path(temp)
                with self.assertRaises(SystemExit):
                    self.run_fake(root, exit_code=code, emit_summary=summary)
                self.assertNotIn("\t0\n", (root / "out/status.tsv").read_text())


class QuerySummaryTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.temp.cleanup)
        cls.binary = Path(cls.temp.name) / "db_query_summary"
        cls.zig_root = matrix.ROOT / "zig"
        subprocess.run(
            [
                "zig",
                "build-exe",
                "bench/storage/db_query_summary.zig",
                "-lc",
                f"-femit-bin={cls.binary}",
                "--cache-dir",
                os.environ.get("ZIG_LOCAL_CACHE_DIR", "/tmp/zig-local-cache"),
                "--global-cache-dir",
                os.environ.get("ZIG_GLOBAL_CACHE_DIR", "/tmp/zig-global-cache"),
            ],
            cwd=cls.zig_root,
            check=True,
        )

    def run_summary(self, *extra):
        return subprocess.run(
            [
                str(self.binary),
                "--input",
                "bench/storage/db_query_summary_fixture.jsonl",
                "--baseline",
                "bench/storage/db_query_summary_baseline.jsonl",
                *matrix.summary_args(),
                *extra,
            ],
            cwd=self.zig_root,
            text=True,
            capture_output=True,
        )

    def test_fixture_coverage_and_baseline(self):
        result = self.run_summary("--max-algebraic-query-ms-ratio-vs-baseline", "1.0")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('"event":"performance_evidence_summary"', result.stderr)
        self.assertIn('"event":"performance_baseline_comparison"', result.stderr)

    def test_churn_comparisons_match_workload_identity(self):
        records = []
        for backend, docs, latency in (
            ("lsm", 25, 100),
            ("mem", 100, 2),
            ("lsm", 100, 10),
        ):
            for case, cost in (("static", latency), ("materialized", latency / 2)):
                records.append(
                    {
                        "event": "churn",
                        "case": "adaptive_coverage_" + case,
                        "algebraic_backend": backend,
                        "algebraic_profile": "test",
                        "matrix_case": "analytics",
                        "docs": docs,
                        "ops": 4,
                        "batch_size": 25,
                        "algebraic_update_ms": cost,
                    }
                )
        path = Path(self.temp.name) / "churn.jsonl"
        path.write_text("".join(json.dumps(record) + "\n" for record in records))
        result = subprocess.run(
            [str(self.binary), "--input", str(path)], text=True, capture_output=True
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        comparisons = [
            json.loads(line)
            for line in result.stderr.splitlines()
            if '"event":"adaptive_churn_compare"' in line
        ]
        self.assertEqual(len(comparisons), 3)
        self.assertTrue(all(row["speedup_vs_baseline"] == 2 for row in comparisons))
        self.assertEqual(
            [row["baseline_update_ms"] for row in comparisons], [100, 2, 10]
        )

    def test_latency_and_baseline_regressions_fail(self):
        for flags in (
            ("--max-algebraic-query-ms", "0.001"),
            ("--max-algebraic-query-ms-ratio-vs-baseline", "0.001"),
        ):
            with self.subTest(flags=flags):
                result = self.run_summary(*flags)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("GuardrailFailed", result.stderr)


if __name__ == "__main__":
    unittest.main()
