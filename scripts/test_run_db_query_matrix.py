#!/usr/bin/env python3
"""Check matrix workloads, evidence collection, and failure propagation."""

import argparse
import json
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
        self.assertEqual([c[2][1] for c in storage], ["128", "256", "384"])
        self.assertEqual(
            [c[2][1] for c in matrix.cases("bounded", "storage")],
            ["1024", "2048", "2048"],
        )
        public = matrix.cases("bounded", "public")
        self.assertEqual(len(public), 6)
        self.assertIn("100000", public[0][2])
        self.assertIn("--with-sparse", public[-1][2])
        self.assertIn("--with-algebraic", public[-1][2])

    def run_fake(self, root, *, exit_code=0, emit_summary=True):
        calls = []

        def run(command, **kwargs):
            calls.append(command)
            if command[0] == "zig":
                return subprocess.CompletedProcess(command, 0)
            event = (
                "docid_query_bench_summary"
                if command[0].endswith("db_query_bench")
                else "public_query_guardrail_summary"
            )
            if emit_summary:
                kwargs["stderr"].write(json.dumps({"event": event}) + "\n")
            return subprocess.CompletedProcess(command, exit_code)

        args = argparse.Namespace(
            profile="smoke",
            suite="all",
            public_docs=None,
            out=root / "out",
            bin_dir=root / "bin",
            skip_build=False,
            storage_arg=[],
            public_arg=[],
        )
        with patch.object(
            matrix.platform, "platform", return_value="test-platform"
        ), patch.object(matrix.subprocess, "run", side_effect=run), patch.object(
            matrix.subprocess, "check_output", return_value="commit\n"
        ):
            matrix.run_matrix(args)
        return calls

    def test_build_once_and_collect_both_streams(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            calls = self.run_fake(root)
            self.assertEqual(
                calls[0],
                ["zig", "build", "antfly-storage-db-bench", "public-query-guardrail"],
            )
            self.assertEqual(len(calls), 10)
            self.assertEqual(
                len((root / "out/summary.jsonl").read_text().splitlines()), 9
            )
            self.assertEqual(len((root / "out/status.tsv").read_text().splitlines()), 9)

    def test_failure_and_missing_evidence_fail(self):
        for code, summary in ((7, True), (0, False)):
            with self.subTest(
                code=code, summary=summary
            ), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                with self.assertRaises(SystemExit):
                    self.run_fake(root, exit_code=code, emit_summary=summary)
                self.assertNotIn("\t0\n", (root / "out/status.tsv").read_text())


if __name__ == "__main__":
    unittest.main()
