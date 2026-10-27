#!/usr/bin/env python3
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

"""Verify qualification provenance, exact result oracles and counter semantics."""

import copy
import hashlib
import json
import os
import sqlite3
import struct
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

if __package__:
    from . import benchmark_lake_archive as bench
else:
    import benchmark_lake_archive as bench


class QualificationTest(unittest.TestCase):
    def test_input_identity_rejects_replacement_size_changes_and_removal(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "binary"
            for mutation in ("replace", "resize", "remove"):
                path.write_bytes(b"versionA")
                identity = bench.VerifiedInput(path)
                identity.verify(content=True)
                if mutation == "replace":
                    replacement = path.with_suffix(".new")
                    replacement.write_bytes(b"versionB")
                    os.replace(replacement, path)
                elif mutation == "resize":
                    # The cheap check cannot promise same-size edit detection
                    # on filesystems whose timestamps have coarse resolution.
                    path.write_bytes(b"versionB-longer")
                else:
                    path.unlink()
                with self.assertRaisesRegex(RuntimeError, "Qualification input"):
                    identity.verify()

    def test_final_content_check_rejects_edits_with_unchanged_metadata(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "binary"
            path.write_bytes(b"versionA")
            identity = bench.VerifiedInput(path)
            path.write_bytes(b"versionB")
            # Model a coarse timestamp filesystem deterministically rather
            # than relying on whether two writes land in the same clock tick.
            with mock.patch.object(
                identity, "signature", return_value=identity.identity
            ):
                identity.verify()
                with self.assertRaisesRegex(RuntimeError, "Qualification input"):
                    identity.verify(content=True)

    def test_restart_cannot_accept_samples_from_a_replaced_binary(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "binary"
            path.write_bytes(b"versionA")
            identity = bench.VerifiedInput(path)
            client = mock.Mock()
            client.server.restart.side_effect = lambda: path.write_bytes(
                b"versionB-longer"
            )
            with mock.patch.object(bench, "MemorySample"):
                with self.assertRaisesRegex(RuntimeError, "input changed"):
                    bench.run_queries(
                        client,
                        {"cases": [{"name": "scan", "statement": "SELECT 1"}]},
                        3,
                        Path(root),
                        (identity,),
                    )
            client.query.assert_not_called()
            self.assertFalse((Path(root) / "samples.json").exists())

    def test_main_owns_resources_through_initialization_interrupt_and_shutdown(self):
        for failure in ("provenance", "client", "query", "stop", "identity"):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as root:
                root = Path(root)
                binary = root / "binary"
                binary.write_bytes(b"binary")
                plan = bench.generated_plan({"manifest_uri": "file:///source"}, 16, 8)
                plan_path = root / "plan.json"
                plan_path.write_text(json.dumps(plan))
                output = root / "output"
                server = mock.Mock()
                server.log_path = root / "server.log"
                server.log_path.write_text("diagnostics")
                factory = mock.Mock(return_value=server)
                client = mock.Mock()
                client.ready.return_value = {
                    "text": "revision",
                    "archive_id": "revision",
                }
                if failure == "stop":
                    server.stop.side_effect = RuntimeError("stop failure")
                elif failure == "identity":
                    server.stop.side_effect = lambda **_: binary.write_bytes(b"changed")
                with (
                    mock.patch.dict(
                        sys.modules,
                        {
                            "conftest": SimpleNamespace(
                                StandaloneAntflyServer=factory,
                                __file__=__file__,
                                _write_remote_content_e2e_config=lambda _: plan_path,
                            )
                        },
                    ),
                    mock.patch.object(
                        bench.platform, "platform", return_value="test-machine"
                    ),
                    mock.patch.object(
                        bench.subprocess,
                        "check_output",
                        side_effect=RuntimeError("provenance failure")
                        if failure == "provenance"
                        else None,
                        return_value="git-head\n",
                    ),
                    mock.patch.object(
                        bench,
                        "Client",
                        return_value=client,
                        side_effect=RuntimeError("client failure")
                        if failure == "client"
                        else None,
                    ),
                    mock.patch.object(
                        bench,
                        "run_queries",
                        return_value=[],
                        side_effect=KeyboardInterrupt() if failure == "query" else None,
                    ),
                    self.assertRaisesRegex(
                        KeyboardInterrupt if failure == "query" else RuntimeError,
                        ""
                        if failure == "query"
                        else (
                            "input changed"
                            if failure == "identity"
                            else f"{failure} failure"
                        ),
                    ),
                ):
                    bench.main(
                        [
                            "run",
                            "--binary",
                            str(binary),
                            "--plan",
                            str(plan_path),
                            "--output-dir",
                            str(output),
                        ]
                    )
                if failure == "provenance":
                    factory.assert_not_called()
                else:
                    server.stop.assert_called_once_with(cleanup_root=False)
                    server.tempdir.cleanup.assert_called_once()
                    self.assertEqual((output / "server.log").read_text(), "diagnostics")
                    report = json.loads((output / "report.json").read_text())
                    self.assertFalse(report["complete"])
                    if failure != "client":
                        client.session.close.assert_called_once()

    def test_embedded_indexes_and_schema_indexes_are_all_awaited(self):
        plan = bench.generated_plan({"manifest_uri": "file:///source"}, 16, 8)
        plan["definition"]["indexes"] = copy.deepcopy(plan["indexes"])
        bench.validate_plan(plan)
        self.assertEqual(set(bench.effective_indexes(plan)), {"text"})
        self.assertEqual(set(bench.readiness_indexes(plan)), {"text", "archive_id"})
        del plan["indexes"]
        self.assertEqual(set(bench.effective_indexes(plan)), {"text"})
        plan["indexes"] = {"text": {"type": "full_text", "field": "body"}}
        with self.assertRaisesRegex(ValueError, "Conflicting"):
            bench.validate_plan(plan)

    def test_custom_identity_field_controls_the_result_oracle(self):
        case = {"name": "top", "identity_field": "hn_id", "expected_ids": [123]}
        response = {"hits": {"hits": [{"_id": "opaque", "_source": {"hn_id": 123}}]}}
        bench.verify_case(case, response)
        response["hits"]["hits"][0]["_source"]["hn_id"] = 456
        with self.assertRaisesRegex(ValueError, "Incorrect logical IDs"):
            bench.verify_case(case, response)

    def test_default_configuration_and_environment_are_comparison_inputs(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            binary = root / "binary"
            binary.write_bytes(b"same executable")
            plan = bench.generated_plan({"manifest_uri": "file:///frozen"}, 16, 8)
            plan["source_fingerprint"] = "same-source"
            plan_path = root / "plan.json"
            plan_path.write_text(json.dumps(plan))
            reports = []
            configs = []

            def make_config(output):
                path = output / "generated.json"
                path.write_text(
                    json.dumps(
                        {
                            "models_dir": os.environ[
                                "ANTFLY_INFERENCE_STANDALONE_MODELS_DIR"
                            ]
                        }
                    )
                )
                configs.append(path)
                return path

            for label in ("a", "b"):
                server = mock.Mock()
                server.log_path = root / "absent.log"
                factory = mock.Mock(return_value=server)
                client = mock.Mock()
                client.ready.return_value = {"text": 1, "archive_id": 1}
                harness = SimpleNamespace(
                    __file__=__file__,
                    StandaloneAntflyServer=factory,
                    _write_remote_content_e2e_config=make_config,
                )
                with (
                    mock.patch.dict(
                        os.environ,
                        {"ANTFLY_INFERENCE_STANDALONE_MODELS_DIR": f"/models/{label}"},
                    ),
                    mock.patch.dict(sys.modules, {"conftest": harness}),
                    mock.patch.object(bench, "Client", return_value=client),
                    mock.patch.object(
                        bench, "run_queries", return_value=self.report()["samples"]
                    ),
                    mock.patch("builtins.print"),
                ):
                    bench.main(
                        [
                            "run",
                            "--binary",
                            str(binary),
                            "--plan",
                            str(plan_path),
                            "--output-dir",
                            str(root / label),
                        ]
                    )
                self.assertEqual(
                    factory.call_args.kwargs["config_path"], configs[-1].resolve()
                )
                self.assertEqual(factory.call_args.kwargs["api_root"], "/api/v1")
                report = json.loads((root / label / "report.json").read_text())
                self.assertTrue(report["complete"])
                self.assertEqual(
                    report["server_config_sha256"], bench.file_digest(configs[-1])
                )
                self.assertEqual(report["server_config_origin"], "generated")
                reports.append(report)
            self.assertNotEqual(
                reports[0]["runtime_environment_sha256"],
                reports[1]["runtime_environment_sha256"],
            )
            with self.assertRaisesRegex(ValueError, "server_config_sha256"):
                bench.compare_reports(*reports)

    def test_environment_edits_invalidate_the_active_run(self):
        with mock.patch.dict(os.environ, {"ANTFLY_INFERENCE_HOST_BUDGET_MB": "1024"}):
            identity = bench.VerifiedEnvironment()
            identity.verify()
            os.environ["ANTFLY_INFERENCE_HOST_BUDGET_MB"] = "2048"
            with self.assertRaisesRegex(RuntimeError, "environment changed"):
                identity.verify()

    def report(self, count=3):
        rows = [
            {
                "case": "scan",
                "cache_mode": mode,
                "sample": i,
                "elapsed_ns": 100 + i,
                "result_sha256": "answer",
            }
            for mode in ("process_restarted", "warm")
            for i in range(count)
        ]
        return {
            "version": 3,
            "refresh_workload": None,
            "complete": True,
            "source_identity": "archive-hash",
            "source_identity_assurance": "verified_local_file_bytes",
            "workload_sha256": "workload",
            "cache_contract": {"empty_cache_qualified": False},
            "binary_sha256": "binary",
            "tool_sha256": "tool",
            "harness_sha256": "harness",
            "server_config_sha256": "config",
            "runtime_environment_sha256": "environment",
            "platform": "machine",
            "samples": rows,
        }

    def test_generated_oracles_against_independent_sql_execution(self):
        for count in (1, 63, 257):
            with sqlite3.connect(":memory:") as db:
                db.execute(
                    f"CREATE TABLE {bench.TABLE}(id, uniform_key, skew_key, category)"
                )
                db.executemany(
                    f"INSERT INTO {bench.TABLE} VALUES (?, ?, ?, ?)",
                    [
                        (
                            i,
                            i % 64,
                            0 if i % 10 else i % 64,
                            "story" if i % 4 == 0 else "comment",
                        )
                        for i in range(count)
                    ],
                )
                plan = bench.generated_plan(
                    {"manifest_uri": "file:///source"}, count, min(17, count)
                )
                bench.validate_plan(plan)
                for case in plan["cases"]:
                    if "statement" in case:
                        answer = [
                            [str(v) for v in row]
                            for row in db.execute(case["statement"])
                        ]
                        self.assertEqual(case["expected_rows"], answer, case["name"])

    def test_search_comparison_preserves_membership_totals_and_highlights(self):
        result = {
            "hits": {
                "total": 10,
                "hits": [
                    {
                        "_id": "public1",
                        "_score": 1,
                        "_source": {"id": 7},
                        "_highlight": {"body": ["term"]},
                    }
                ],
            },
            "profile": {"elapsed": 10},
            "took": 20,
        }
        base = bench.digest(bench.result_value(result))
        for path in ("id", "total", "highlight"):
            changed = copy.deepcopy(result)
            if path == "id":
                changed["hits"]["hits"][0]["_id"] = "public2"
            elif path == "total":
                changed["hits"]["total"] = 11
            else:
                changed["hits"]["hits"][0]["_highlight"] = {}
            self.assertNotEqual(base, bench.digest(bench.result_value(changed)))
        changed = copy.deepcopy(result)
        changed["profile"] = {"elapsed": 999}
        changed["took"] = 999
        self.assertEqual(base, bench.digest(bench.result_value(changed)))
        first_cut = copy.deepcopy(result)
        first_cut["remote_snapshot"] = "first-cut"
        other_cut = copy.deepcopy(first_cut)
        other_cut["remote_snapshot"] = "different-expiry"
        self.assertEqual(bench.result_value(first_cut), bench.result_value(other_cut))
        del other_cut["remote_snapshot"]
        self.assertNotEqual(
            bench.result_value(first_cut), bench.result_value(other_cut)
        )
        changed["hits"]["hits"][0]["_id"] = "physical-other-publication"
        self.assertEqual(
            bench.result_value(result, "id"), bench.result_value(changed, "id")
        )
        with self.assertRaisesRegex(ValueError, "project"):
            bench.result_value(result, "missing")
        plan = bench.generated_plan({"manifest_uri": "file:///source"}, 10, 2)
        plan["cases"][-1]["identity_field"] = "body"
        with self.assertRaisesRegex(ValueError, "row identity"):
            bench.validate_plan(plan)

    def test_streamed_object_envelope_is_exact(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "raw.parquet"
            raw = b"PAR1" + b"x" * (2 * 1024 * 1024) + b"PAR1"
            source.write_bytes(raw)
            checksum = bench.put_object(root, "object", source)
            body = (root / "object").read_bytes()
            self.assertEqual(body[:8], b"AFOBJ001")
            self.assertEqual(struct.unpack("<QI", body[8:20]), (len(raw), 0))
            self.assertEqual(body[20:84].decode(), hashlib.sha256(raw).hexdigest())
            self.assertEqual(checksum, hashlib.sha256(raw).hexdigest())
            self.assertEqual(body[84:], raw)
            self.assertFalse((root / "object.pending").exists())
            self.assertEqual(source.read_bytes(), raw)

    def test_changed_input_is_not_published_with_an_old_fingerprint(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "raw.parquet"
            source.write_bytes(b"PAR1-current-PAR1")
            with mock.patch.object(bench, "file_digest", return_value="0" * 64):
                with self.assertRaisesRegex(ValueError, "Input changed"):
                    bench.put_object(root, "object", source)
            self.assertFalse((root / "object").exists())
            self.assertFalse((root / "object.pending").exists())

    def test_plan_rejects_path_traversal_and_duplicate_case_names(self):
        plan = bench.generated_plan({"manifest_uri": "file:///source"}, 100, 5)
        for name in ("../escape", "", "a/b", "A", "x" * 65):
            altered = copy.deepcopy(plan)
            altered["cases"][0]["name"] = name
            with self.assertRaises(ValueError):
                bench.validate_plan(altered)
        plan["cases"].append(copy.deepcopy(plan["cases"][0]))
        with self.assertRaises(ValueError):
            bench.validate_plan(plan)

    def test_counters_preserve_large_integers_and_unknown_coverage(self):
        value = 2**60
        before = bench.parse_metrics(
            f"# counter\nantfly_lake_cache_provider_bytes_total {value}\nantfly_lake_cache_disk_ready 1\nantfly_sql_lost_total 1\nantfly_sql_reset_total 100\n"
        )
        after = bench.parse_metrics(
            f"antfly_lake_cache_provider_bytes_total {value + 1}\nantfly_lake_cache_disk_ready 0\nantfly_sql_reset_total 1\n"
        )
        self.assertEqual(
            bench.metric_deltas(before, after),
            {"antfly_lake_cache_provider_bytes_total": 1},
        )
        report = self.report()
        self.assertIsNone(
            bench.summaries(report["samples"])[0]["cache_provider_reads_median"]
        )

    def test_comparison_requires_equal_inputs_cache_scope_and_answers(self):
        base = self.report()
        self.assertTrue(
            bench.compare_reports(base, copy.deepcopy(base))["timing_qualified"]
        )
        for key in (
            "source_identity",
            "source_identity_assurance",
            "workload_sha256",
            "cache_contract",
            "tool_sha256",
            "platform",
            "server_config_sha256",
            "harness_sha256",
            "runtime_environment_sha256",
        ):
            candidate = copy.deepcopy(base)
            candidate[key] = "changed"
            with self.assertRaisesRegex(ValueError, key):
                bench.compare_reports(base, candidate)
        candidate = copy.deepcopy(base)
        candidate["samples"][0]["result_sha256"] = "wrong"
        with self.assertRaisesRegex(ValueError, "Unequal"):
            bench.compare_reports(base, candidate)
        for broken in (
            {**base, "version": 1},
            {**base, "server_config_sha256": None},
            {**base, "harness_sha256": None},
            {**base, "complete": False},
            {**base, "source_identity": None},
            {**base, "samples": []},
        ):
            with self.assertRaises(ValueError):
                bench.compare_reports(base, broken)

    def test_comparison_binds_refresh_recipe_and_actual_rewritten_source(self):
        base = self.report()
        base["refresh_workload"] = {"refresh_every": 10, "generation": 1}
        base["refresh"] = {"input": {"sha256": "rewritten"}, "changed_rows": 13}
        self.assertTrue(
            bench.compare_reports(base, copy.deepcopy(base))["timing_qualified"]
        )
        for key, value in (("refresh_every", 2), ("generation", 2)):
            changed = copy.deepcopy(base)
            changed["refresh_workload"][key] = value
            with self.assertRaisesRegex(ValueError, "refresh_workload"):
                bench.compare_reports(base, changed)
        for key in ("input", "changed_rows"):
            changed = copy.deepcopy(base)
            changed["refresh"][key] = {"sha256": "other"} if key == "input" else 2
            with self.assertRaisesRegex(ValueError, "refresh " + key):
                bench.compare_reports(base, changed)
            del changed["refresh"][key]
            with self.assertRaisesRegex(ValueError, "refresh input provenance"):
                bench.compare_reports(base, changed)
        for changed in (self.report(), {**base, "refresh_workload": None}):
            with self.assertRaises(ValueError):
                bench.compare_reports(base, changed)
        query_only = self.report()
        del query_only["refresh_workload"]
        with self.assertRaisesRegex(ValueError, "refresh_workload"):
            bench.compare_reports(self.report(), query_only)

    def test_comparison_rejects_duplicate_or_mismatched_case_coverage(self):
        base = self.report()
        duplicate = copy.deepcopy(base)
        duplicate["samples"].append(duplicate["samples"][0])
        with self.assertRaisesRegex(ValueError, "Duplicate"):
            bench.compare_reports(base, duplicate)
        incomplete = copy.deepcopy(base)
        incomplete["samples"] = [
            r for r in incomplete["samples"] if r["cache_mode"] == "warm"
        ]
        with self.assertRaisesRegex(ValueError, "coverage"):
            bench.compare_reports(base, incomplete)
        self.assertFalse(
            bench.compare_reports(self.report(1), self.report(1))["timing_qualified"]
        )

    def test_artifact_bytes_are_retained_growth_not_reclaimed_write_io(self):
        report = bench.artifact_growth(
            {"retired": 100, "shared": 200}, {"new": 50, "shared": 200}
        )
        self.assertEqual(report["new_retained_bytes"], 50)
        self.assertEqual(report["retained_bytes_after"], 250)
        self.assertIn("excludes reclaimed", report["coverage"])

    def test_mismatched_result_oracle_fails_before_a_sample_is_accepted(self):
        with self.assertRaisesRegex(ValueError, "Incorrect SQL"):
            bench.verify_case(
                {"name": "scan", "expected_rows": [["100"]]}, {"rows": [["99"]]}
            )
        with self.assertRaisesRegex(ValueError, "Incorrect logical IDs"):
            bench.verify_case(
                {"name": "top", "expected_ids": [1]},
                {"hits": {"hits": [{"_source": {"id": 2}}]}},
            )


if __name__ == "__main__":
    unittest.main()
