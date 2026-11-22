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

import contextlib
import io
import json
import math
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import threading

import benchmark_inference_policies as bench
import inference_benchmark_state as state

MODEL = "owner/model:safetensors@" + "a" * 40
IMAGE = "ghcr.io/antflydb/antfly@sha256:" + "b" * 64


class FakeInference:
    def __init__(self):
        self.models = {}
        self.generation = 0
        self.posts = []
        self.gets_after_post = {}
        self.interrupt_post = None
        self.wrong_arm_vector = False
        self.evict_after_posts = None
        self.restart_on_post = None
        self.direct_truncation = False

    def __call__(self, endpoint, route, body, timeout, response_grace=30):
        if route == "/models/instances":
            n = len(self.posts)
            self.gets_after_post[n] = self.gets_after_post.get(n, 0) + 1
            if self.evict_after_posts == n and self.gets_after_post[n] == 2:
                self.models.pop(endpoint, None)
                self.evict_after_posts = None
            data = []
            if endpoint in self.models:
                data = [
                    dict(
                        model=self.models[endpoint][0],
                        id=self.models[endpoint][1],
                        state="loaded",
                        backend="native",
                    )
                ]
            return 200, dict(data=data)
        n = len(self.posts) + 1
        if self.interrupt_post == n:
            self.interrupt_post = None
            raise KeyboardInterrupt()
        self.posts.append((endpoint, route, body))
        if (
            endpoint not in self.models
            or self.models[endpoint][0] != body["model"]
            or self.restart_on_post == n
        ):
            self.generation += 1
            self.models[endpoint] = (body["model"], str(self.generation))
        if route == "/embeddings":
            assert body["input"] == ["example"]
            vector = (
                [0.8, -0.6]
                if self.wrong_arm_vector and endpoint.endswith("8002/ai/v1")
                else [0.6, 0.8]
            )
            return 200, dict(data=[dict(embedding=vector)], usage=dict(total_tokens=1))
        assert route == "/read"
        if body.get("max_tokens") == 4096:
            if self.direct_truncation:
                return 400, {"error": "OUTPUT_TRUNCATED"}
            return 200, dict(
                data=[dict(status="truncated", text="partial")],
                usage=dict(completion_tokens=4096),
            )
        return 200, dict(
            data=[dict(status="complete", text="verified footer")],
            usage=dict(completion_tokens=4200),
        )


class PolicyBenchmarkTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / "page.png").write_bytes(b"page fixture")
        (self.root / "text.txt").write_text("example")
        (self.root / "speech.wav").write_bytes(b"bounded audio fixture")

    def case(self, task="read", kind="image"):
        if task == "embed":
            return dict(
                id="embedding",
                task="embed",
                model=MODEL,
                fixture="text.txt",
                dimensions=2,
            )
        return dict(
            id=kind,
            task="read",
            kind=kind,
            model=MODEL,
            fixture="page.png" if kind == "image" else "speech.wav",
            expected=["verified footer"],
        )

    def argv(self, cases, paired=False):
        (self.root / "cases.json").write_text(json.dumps(cases))
        argv = [
            "bench",
            "--cases",
            str(self.root / "cases.json"),
            "--report",
            str(self.root / "report.json"),
            "--endpoint",
            "x86=http://127.0.0.1:8001/ai/v1",
            "--backend",
            "x86=native",
            "--runtime-image",
            IMAGE,
            "--trials",
            "1",
        ]
        if paired:
            argv += [
                "--endpoint",
                "arm=http://127.0.0.1:8002/ai/v1",
                "--backend",
                "arm=native",
            ]
        if any(case["task"] == "embed" for case in cases):
            argv += ["--reference-endpoint", "x86"]
        return argv

    def run_bench(self, argv, fake):
        with (
            patch("sys.argv", argv),
            patch.object(bench, "request", side_effect=fake),
            contextlib.redirect_stdout(io.StringIO()),
        ):
            return bench.main()

    def report(self):
        return json.loads((self.root / "report.json").read_text())

    def test_resume_retains_completed_attempts_and_rejects_changed_fixtures(self):
        argv = self.argv([self.case()])
        fake = FakeInference()
        self.assertEqual(self.run_bench(argv, fake), 0)
        self.assertEqual(len(fake.posts), 4)
        report = self.report()
        self.assertEqual(report["coverage"]["recorded_samples"], 2)
        self.assertEqual(report["policy_comparison"][0]["old_policy_calls"], 2)
        self.assertNotIn("verified footer", json.dumps(report))
        with (
            patch("sys.argv", argv + ["--resume"]),
            patch.object(bench, "request") as request,
        ):
            self.assertEqual(bench.main(), 0)
            request.assert_not_called()
        (self.root / "page.png").write_bytes(b"different")
        with self.assertRaisesRegex(state.StateError, "RESUME_INPUTS_CHANGED"):
            self.run_bench(argv + ["--resume"], fake)

    def test_interrupted_resume_rewarms_without_repeating_recorded_measurement(self):
        argv = self.argv([self.case()])
        fake = FakeInference()
        fake.interrupt_post = 4
        with self.assertRaises(KeyboardInterrupt):
            self.run_bench(argv, fake)
        first = self.report()
        self.assertFalse(first["success"])
        self.assertEqual(first["coverage"]["recorded_samples"], 1)
        fake.models.clear()
        fake.generation = 100
        self.assertEqual(self.run_bench(argv + ["--resume"], fake), 0)
        report = self.report()
        self.assertEqual(report["epochs"], 2)
        measured = [r for r in report["rows"] if not r["warmup"]]
        self.assertEqual(len(measured), 2)
        self.assertNotEqual(measured[0]["instance_id"], measured[1]["instance_id"])
        self.assertEqual(
            report["policy_comparison"], [], "never pair different execution epochs"
        )
        self.assertFalse(report["policy_comparison_complete"])
        self.assertIn("POLICY_COMPARISON_INCOMPLETE", report["warnings"])
        self.assertEqual([p[2].get("max_tokens") for p in fake.posts[3:]], [8192, 8192])

    def test_idle_eviction_triggers_untimed_warmup(self):
        fake = FakeInference()
        fake.evict_after_posts = 2
        self.assertEqual(self.run_bench(self.argv([self.case()]), fake), 0)
        report = self.report()
        self.assertTrue(
            any(
                row.get("warmup_reason") == "resident_model_changed"
                for row in report["rows"]
            )
        )
        self.assertTrue(
            all(
                row["instance_id"] == "2" for row in report["rows"] if not row["warmup"]
            )
        )

    def test_worker_restart_during_timed_call_invalidates_measurement(self):
        fake = FakeInference()
        fake.restart_on_post = 3
        self.assertEqual(self.run_bench(self.argv([self.case()]), fake), 1)
        rows = self.report()["rows"]
        self.assertTrue(
            any(row.get("error_code") == "MODEL_CHANGED_DURING_SAMPLE" for row in rows)
        )
        self.assertEqual(self.report()["policy_comparison"], [])

    def test_read_completion_shapes_preserve_independent_errors(self):
        case = self.case()
        for response in [
            dict(data=[dict(status="truncated")]),
            dict(document=dict(status="truncated"), data=[dict(status="complete")]),
            dict(truncated=True, data=[dict(status="complete")]),
            dict(data=[dict(status="complete", finish_reason="length")]),
        ]:
            self.assertTrue(bench.inspect_response(case, response)["truncated"])
        for response in [
            dict(data=[dict(status="truncated", error="failed")]),
            dict(document=dict(status="unresolved"), data=[dict(status="truncated")]),
            dict(data=[dict(status="complete", text="wrong")]),
            dict(data=[]),
        ]:
            with self.assertRaises(bench.BenchmarkError):
                bench.inspect_response(case, response)

    def test_direct_truncation_error_is_expected_only_at_lower_budget(self):
        fake = FakeInference()
        fake.direct_truncation = True
        self.assertEqual(self.run_bench(self.argv([self.case()]), fake), 0)
        self.assertEqual(self.report()["policy_comparison"][0]["old_policy_calls"], 2)

    def test_bad_embedding_is_not_accepted_just_because_it_has_unit_norm(self):
        fake = FakeInference()
        fake.wrong_arm_vector = True
        self.assertEqual(
            self.run_bench(self.argv([self.case("embed")], paired=True), fake), 1
        )
        report = self.report()
        self.assertTrue(
            any(
                row.get("error_code") == "EMBEDDING_REFERENCE_MISMATCH"
                for row in report["rows"]
            )
        )
        self.assertGreater(report["coverage"]["missing_samples"], 0)
        self.assertFalse(
            any(
                row.get("complete")
                for row in report["rows"]
                if row["endpoint"] == "arm"
            )
        )
        self.assertNotIn("vector", json.dumps(report))
        journal = self.root / "report.json.state/journal.jsonl"
        self.assertIn('"reference"', journal.read_text())
        self.assertNotIn('"vector"', journal.read_text())
        self.assertEqual(len(list(journal.parent.glob("reference-*.json"))), 1)
        self.assertEqual(journal.stat().st_mode & 0o777, 0o600)
        self.assertEqual(journal.parent.stat().st_mode & 0o777, 0o700)

    def test_equivalent_embeddings_pass_and_reference_is_retained_across_resume(self):
        fake = FakeInference()
        self.assertEqual(
            self.run_bench(self.argv([self.case("embed")], paired=True), fake), 0
        )
        report = self.report()
        self.assertTrue(
            all(row["embedding_comparison"]["passed"] for row in report["rows"])
        )
        self.assertEqual(len(report["references"]), 1)
        self.assertTrue(
            all(
                math.isclose(row["embedding_comparison"]["cosine"], 1)
                for row in report["rows"]
            )
        )

    def test_embedding_reference_is_explicit_and_tolerances_are_enforced(self):
        argv = self.argv([self.case("embed")])
        argv = argv[:-2]
        with self.assertRaisesRegex(bench.BenchmarkError, "REFERENCE_ENDPOINT"):
            self.run_bench(argv, FakeInference())
        for vector in [[float("nan"), 0], [2, 0], [], [True, False]]:
            with self.assertRaises(bench.BenchmarkError):
                bench.embedding_vector(
                    self.case("embed"), dict(data=[dict(embedding=vector)])
                )
        comparison = bench.compare_embedding(
            [0.60000001, 0.8], [0.6, 0.8], bench.DEFAULT_TOLERANCES
        )
        self.assertTrue(comparison["passed"])

    def test_fixture_change_between_identity_and_execution_is_rejected(self):
        case = self.case()
        sha = bench.fixture_hash(self.root / "page.png")
        (self.root / "page.png").write_bytes(b"changed")
        with self.assertRaisesRegex(bench.BenchmarkError, "FIXTURE_CHANGED"):
            bench.prepare_case(case, self.root, sha)

    def test_client_keeps_only_one_case_payload_and_projects_report_once(self):
        cases = [dict(self.case(), id="page-" + str(i)) for i in range(5)]
        fake = FakeInference()
        original = bench.prepare_case

        def prepare(case, root, expected_hash=None):
            # Previous case measurements must already have run; no eager materialization.
            i = int(case["id"].split("-")[1])
            self.assertEqual(len(fake.posts), i * 4)
            return original(case, root, expected_hash)

        with (
            patch.object(bench, "prepare_case", side_effect=prepare),
            patch.object(
                state.CampaignStore, "snapshot", autospec=True, wraps=None
            ) as snapshot,
        ):
            self.assertEqual(self.run_bench(self.argv(cases), fake), 0)
            self.assertEqual(snapshot.call_count, 1)

    def test_failed_work_is_charged_warmup_is_separate(self):
        rows = [
            dict(
                endpoint="arm",
                case="page",
                max_tokens=8192,
                complete=complete,
                seconds=seconds,
                warmup=warmup,
            )
            for complete, seconds, warmup in [
                (True, 100, True),
                (False, 10, False),
                (True, 20, False),
            ]
        ]
        entry = bench.summarize(rows, {"arm": 0.2})[0]
        self.assertEqual(entry["complete"], 1)
        self.assertEqual(entry["calls"], 2)
        self.assertEqual(entry["seconds"], 30)
        self.assertAlmostEqual(entry["usd_per_complete"], 30 / 3600 * 0.2)

    def test_network_refuses_nonlocal_plaintext_and_credentials(self):
        for endpoint in [
            "http://example.com/ai/v1",
            "https://user:secret@example.com/ai/v1",
            "https://example.com/ai/v1?token=secret",
        ]:
            with self.assertRaises(bench.BenchmarkError):
                bench.request(endpoint, "/read", {}, 1)

    def test_case_rejects_string_expected_and_identity_overrides(self):
        for update in [
            dict(expected="ABC"),
            dict(options={"documents": []}),
            dict(options={"max_tokens": 100}),
            dict(kind="video"),
        ]:
            with self.assertRaises(bench.BenchmarkError):
                bench.validate_case(dict(self.case(), **update))

    def test_real_http_campaign_checks_wire_contracts_deadlines_and_backend(self):
        fake = FakeInference()

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_GET(self):
                self.reply(*fake("http://local/ai/v1", "/models/instances", None, 30))

            def do_POST(self):
                if self.headers.get("X-Antfly-Inference-Timeout-Ms") != "600000":
                    self.reply(400, {"error": "MISSING_DEADLINE"})
                    return
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                self.reply(
                    *fake(
                        "http://local/ai/v1",
                        self.path.removeprefix("/ai/v1"),
                        body,
                        600,
                    )
                )

            def reply(self, status, body):
                raw = json.dumps(body).encode()
                self.send_response(status)
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

        with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            argv = self.argv([self.case(), self.case("embed"), self.case(kind="audio")])
            argv[argv.index("x86=http://127.0.0.1:8001/ai/v1")] = (
                f"x86=http://127.0.0.1:{server.server_port}/ai/v1"
            )
            try:
                with patch("sys.argv", argv), contextlib.redirect_stdout(io.StringIO()):
                    self.assertEqual(bench.main(), 0)
            finally:
                server.shutdown()
                thread.join(5)
        self.assertEqual(self.report()["coverage"]["recorded_samples"], 4)
        self.assertEqual(len(fake.posts), 8)
        self.assertEqual(self.report()["policy_comparison"][0]["old_policy_calls"], 2)


class JournalTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.report = Path(self.temp.name) / "report.json"
        self.identity = {"schema": 2, "inputs": "fixed"}

    def row(self):
        return dict(
            case="page",
            endpoint="cpu",
            trial=0,
            max_tokens=8192,
            warmup=False,
            complete=True,
            seconds=1,
        )

    def test_second_writer_cannot_open_or_overwrite_active_campaign(self):
        with state.CampaignStore(self.report, self.identity) as first:
            first.record(self.row())
            before = (first.directory / "journal.jsonl").read_bytes()
            with self.assertRaisesRegex(state.StateError, "ALREADY_RUNNING"):
                with state.CampaignStore(self.report, self.identity, resume=True):
                    self.fail("second writer acquired lock")
            self.assertEqual((first.directory / "journal.jsonl").read_bytes(), before)

    def test_recovery_keeps_committed_rows_and_trims_only_unterminated_tail(self):
        with state.CampaignStore(self.report, self.identity) as store:
            store.record(self.row())
        journal = self.report.with_name(self.report.name + ".state") / "journal.jsonl"
        with journal.open("ab") as stream:
            stream.write(b'{"partial":')
        with state.CampaignStore(self.report, self.identity, resume=True) as store:
            self.assertEqual(len(store.samples), 1)
            self.assertEqual(store.recovered_bytes, len(b'{"partial":'))
            self.assertEqual(len(store.epochs), 2)
        with state.CampaignStore(self.report, self.identity, resume=True) as store:
            self.assertEqual(len(store.samples), 1)

    def test_complete_corrupt_record_is_not_silently_discarded(self):
        with state.CampaignStore(self.report, self.identity):
            pass
        journal = self.report.with_name(self.report.name + ".state") / "journal.jsonl"
        with journal.open("ab") as stream:
            stream.write(b'{"record":{},"sha256":"wrong"}\n')
        with self.assertRaisesRegex(state.StateError, "CHECKSUM"):
            with state.CampaignStore(self.report, self.identity, resume=True):
                pass

    def test_private_reference_is_loaded_lazily_and_checksum_verified(self):
        with state.CampaignStore(self.report, self.identity) as store:
            store.reference("embedding", "x86", [0.6, 0.8])
            self.assertNotIn("vector", store.references["embedding"])
            self.assertEqual(store.reference_vector("embedding"), [0.6, 0.8])
            path = store.directory / store.references["embedding"]["filename"]
            path.write_text("[1,0]")
            with self.assertRaisesRegex(state.StateError, "REFERENCE_CHECKSUM"):
                store.reference_vector("embedding")

    def test_interrupted_identity_creation_recovers_before_any_requests(self):
        directory = self.report.with_name(self.report.name + ".state")
        directory.mkdir(mode=0o700)
        journal = directory / "journal.jsonl"
        journal.write_bytes(b'{"partial identity')
        journal.chmod(0o600)
        with state.CampaignStore(self.report, self.identity, resume=True) as store:
            self.assertEqual(store.samples, {})
            store.record(self.row())
        with state.CampaignStore(self.report, self.identity, resume=True) as store:
            self.assertEqual(len(store.samples), 1)

    def test_duplicate_measurement_is_rejected_before_journal_write(self):
        with state.CampaignStore(self.report, self.identity) as store:
            store.record(self.row())
            before = (store.directory / "journal.jsonl").read_bytes()
            with self.assertRaisesRegex(state.StateError, "DUPLICATE_MEASUREMENT"):
                store.record(self.row())
            self.assertEqual((store.directory / "journal.jsonl").read_bytes(), before)

    def test_failed_commit_does_not_claim_a_durable_measurement(self):
        with state.CampaignStore(self.report, self.identity) as store:
            with patch.object(
                state.os, "fsync", side_effect=OSError("synthetic failure")
            ):
                with self.assertRaises(OSError):
                    store.record(self.row())
            self.assertEqual(store.samples, {})

    def test_atomic_report_projection_is_private_and_contains_no_reference_vectors(
        self,
    ):
        with state.CampaignStore(self.report, self.identity) as store:
            store.reference("embedding", "x86", [0.6, 0.8])
            store.record(self.row())
            store.snapshot(dict(rows=store.rows))
        self.assertEqual(self.report.stat().st_mode & 0o777, 0o600)
        self.assertNotIn("vector", self.report.read_text())

    def test_report_without_journal_is_never_overwritten(self):
        self.report.write_text('{"old":"report"}')
        with self.assertRaisesRegex(state.StateError, "NO_COMPATIBLE_JOURNAL"):
            with state.CampaignStore(self.report, self.identity, resume=True):
                pass
        self.assertEqual(self.report.read_text(), '{"old":"report"}')


if __name__ == "__main__":
    unittest.main()
