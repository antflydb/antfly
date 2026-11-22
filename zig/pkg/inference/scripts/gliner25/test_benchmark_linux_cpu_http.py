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

import json
import io
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from urllib.error import HTTPError
from unittest.mock import patch

import benchmark_linux_cpu_http as bench
import qualify_context_http as capacity


class ContractTests(unittest.TestCase):
    def test_admission_failure_records_endpoint_and_bounded_error_body(self):
        error = HTTPError(
            "http://localhost/extract",
            503,
            "busy",
            {},
            io.BytesIO(b'{"reason":"live_host"}'),
        )
        with patch.object(bench, "urlopen", side_effect=error):
            with self.assertRaisesRegex(
                ValueError, "localhost/extract: HTTP 503: .*live_host"
            ):
                bench.receive(
                    "http://localhost", {"path": "/extract", "request": {}}, 10
                )

    def test_capacity_campaign_fills_exact_boundary_and_rejects_inexact_usage(self):
        word = "configuration_visibility_maintenance_acknowledgement "
        exact = word * 1819 + "the " * 3
        counts = {word: 19, word * 2: 28, "the ": 11, exact: 16384}
        with tempfile.TemporaryDirectory() as directory:
            for fail in (False, True):
                output = Path(directory) / f"{fail}.json"

                def execute(_, text, __):
                    return {
                        "output": {
                            "usage": {
                                "prompt_tokens": counts[text]
                                - (1 if fail and text == exact else 0)
                            }
                        }
                    }

                arguments = [
                    "qualify_context_http",
                    "--endpoint",
                    "http://localhost/extract",
                    "--output",
                    str(output),
                ]
                with (
                    patch.object(sys, "argv", arguments),
                    patch.object(capacity, "execute", side_effect=execute),
                ):
                    if fail:
                        with self.assertRaisesRegex(
                            ValueError, "encoded-token boundary"
                        ):
                            capacity.main()
                    else:
                        capacity.main()
                report = json.loads(output.read_text())
                self.assertEqual(report["status"], "failed" if fail else "complete")
                self.assertEqual(report["source_words"], 1822)
                self.assertEqual(report["filler_words"], 3)

    def test_capacity_check_rejects_split_windows_and_invalid_source_spans(self):
        class Response(io.BytesIO):
            status = 200

        good = {
            "data": [
                {
                    "long_document": {"window_count": 1},
                    "entities": [{"start": 0, "end": 3, "text": "abc"}],
                }
            ],
            "usage": {"prompt_tokens": 27, "completion_tokens": 0, "total_tokens": 27},
        }
        with patch.object(
            capacity, "urlopen", return_value=Response(json.dumps(good).encode())
        ):
            result = capacity.execute("http://localhost/extract", "abc", 10)
        self.assertEqual(result["output"], good)
        changed = json.loads(json.dumps(good))
        changed["usage"]["total_tokens"] = 28
        with patch.object(
            capacity, "urlopen", return_value=Response(json.dumps(changed).encode())
        ):
            with self.assertRaisesRegex(
                ValueError, "inconsistent extraction token usage"
            ):
                capacity.execute("http://localhost/extract", "abc", 10)
        for split, text in ((True, "abc"), (False, "wrong")):
            changed = json.loads(json.dumps(good))
            changed["data"][0]["long_document"]["window_count"] = 2 if split else 1
            changed["data"][0]["entities"][0]["text"] = text
            with patch.object(
                capacity, "urlopen", return_value=Response(json.dumps(changed).encode())
            ):
                with self.assertRaises(ValueError):
                    capacity.execute("http://localhost/extract", "abc", 10)

    def test_burst_ends_at_response_completion_not_after_validation(self):
        response = {"duration_ns": 1, "response_completed_ns": 150, "raw": b"{}"}
        with (
            patch.object(bench, "receive", return_value=response),
            patch.object(bench.time, "perf_counter_ns", side_effect=[100, 1000]),
        ):
            result = bench.request_round(
                "http://localhost", {"expected": {}, "confidence_tolerance": 0}, 10, 4
            )
        self.assertEqual(result["duration_ns"], 50)

    def test_no_parsing_or_validation_until_every_response_is_captured(self):
        entered = threading.Barrier(4, timeout=5)
        captured = 0
        lock = threading.Lock()

        def receive(*_):
            nonlocal captured
            entered.wait()
            with lock:
                captured += 1
            return {
                "duration_ns": 1,
                "response_completed_ns": time.perf_counter_ns(),
                "raw": b'{"ok":true}',
            }

        real_parse = bench.strict_json

        def parse(raw):
            self.assertEqual(captured, 4)
            return real_parse(raw)

        with (
            patch.object(bench, "receive", side_effect=receive),
            patch.object(bench, "strict_json", side_effect=parse) as parsed,
        ):
            result = bench.request_round(
                "http://localhost",
                {"expected": {"ok": True}, "confidence_tolerance": 0},
                10,
                4,
            )
        self.assertEqual(parsed.call_count, 4)
        self.assertEqual(len(result["requests"]), 4)

    def test_concurrent_round_overlaps_clients_and_checks_every_result(self):
        entered = threading.Barrier(4, timeout=5)

        def checked_request(*_):
            entered.wait()
            return {
                "duration_ns": 1,
                "response_completed_ns": time.perf_counter_ns(),
                "raw": b'{"ok":true}',
            }

        case = {"expected": {"ok": True}, "confidence_tolerance": 0}
        with patch.object(bench, "receive", side_effect=checked_request):
            result = bench.request_round("http://localhost", case, 10, 4)
        self.assertEqual(len(result["requests"]), 4)
        self.assertGreater(result["duration_ns"], 0)
        with patch.object(
            bench,
            "receive",
            return_value={
                "duration_ns": 1,
                "response_completed_ns": time.perf_counter_ns(),
                "raw": b'{"ok":false}',
            },
        ):
            with self.assertRaisesRegex(ValueError, "value differs"):
                bench.request_round("http://localhost", case, 10, 4)

    def test_numeric_tolerance_does_not_hide_decision_or_offset_changes(self):
        expected = {"label": "refund", "start": 2, "confidence": 0.8}
        bench.require_equal(expected, dict(expected, confidence=0.8001), 0.0005)
        for changed in (
            dict(expected, label="sales"),
            dict(expected, start=3),
            dict(expected, confidence=float("nan")),
            dict(expected, confidence=True),
            dict(expected, confidence=0.81),
        ):
            with self.assertRaises(ValueError):
                bench.require_equal(expected, changed, 0.0005)
        for raw in ('{"x":1,"x":2}', '{"x":NaN}'):
            with self.assertRaises(ValueError):
                bench.strict_json(raw)

    def test_http_campaign_records_samples_and_fails_closed_on_mismatch(self):
        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                self.rfile.read(int(self.headers["Content-Length"]))
                raw = json.dumps({"label": "refund", "confidence": 0.8}).encode()
                self.send_response(200)
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

            def log_message(self, *_):
                pass

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                cases = root / "cases.json"
                manifest = {
                    "artifacts": [__file__],
                    "cases": [
                        {
                            "id": "decide",
                            "path": "/decide",
                            "request": {"text": "refund"},
                            "expected": {"label": "refund", "confidence": 0.8},
                            "confidence_tolerance": 0.0005,
                        }
                    ],
                }
                url = f"http://127.0.0.1:{server.server_port}"
                for fail in (False, True):
                    if fail:
                        manifest["cases"][0]["expected"]["label"] = "sales"
                    cases.write_text(json.dumps(manifest))
                    output = root / str(fail)
                    result = subprocess.run(
                        [
                            sys.executable,
                            bench.__file__,
                            "--cases",
                            str(cases),
                            "--baseline",
                            url,
                            "--candidate",
                            url,
                            "--reference",
                            url,
                            "--output",
                            str(output),
                            "--warmup",
                            "0",
                            "--pairs",
                            "2",
                            "--concurrency",
                            "4",
                        ],
                        capture_output=True,
                        text=True,
                        timeout=30,
                    )
                    self.assertEqual(result.returncode == 0, not fail, result.stderr)
                    report = json.loads((output / "report.json").read_text())
                    self.assertEqual(report["status"], "failed" if fail else "complete")
                    self.assertFalse(report["performance_release_qualified"])
                    if not fail:
                        self.assertEqual(
                            report["samples"][1]["order"],
                            ["reference", "candidate", "baseline"],
                        )
                        self.assertIn(
                            "candidate_over_reference", report["comparisons"]["decide"]
                        )
                        self.assertEqual(report["concurrency"], 4)
                        summary = report["comparisons"]["decide"]
                        self.assertIn("client_latency_ns", summary)
                        self.assertGreater(
                            summary["requests_per_second"]["candidate"], 0
                        )
        finally:
            server.shutdown()
            server.server_close()
            thread.join()


class RegressionContracts(unittest.TestCase):
    def receipts(self, root, backend="native"):
        def pin(name, data):
            path = root / name
            path.write_bytes(data)
            return {"path": str(path), "sha256": bench.sha256(path)}
        binary = pin("binary", b"hermetic test image, never executed")
        build = pin("build.json", json.dumps({"passed": True, "binary_sha256": binary["sha256"], "source": {"head": "a" * 40, "tracked_diff_sha256": bench.hashlib.sha256(b"").hexdigest()}}).encode())
        configuration = pin("config.json", b'{"inference":{"models_dir":"models"}}')
        model = {"version": 1, "models": [{"model": "model", "backend": backend, "tasks": ["decide"], "files": [{"path": "config.json", "size": 2, "sha256": "b" * 64}, {"path": "model.safetensors", "size": 1, "sha256": "c" * 64}]}]}
        arms = {name: "http://localhost:" + port for name, port in [("baseline", "8001"), ("candidate", "8002")]}
        row = {"source_commit": "a" * 40, "binary": binary, "build_receipt": build, "configuration": configuration, "working_directory": str(root), "precision": "float32", "thread_budget": {"cpu": 2}, "resource_limits": {"host_bytes": 1024}, "model_artifacts": model}
        path = root / "arms.json"
        path.write_text(json.dumps({"schema": 1, "arms": {name: dict(row, endpoint=url) for name, url in arms.items()}}))
        return path, arms, model

    def test_regression_and_faster_are_independent_fixed_gates(self):
        def check(candidate, baseline, faster=False):
            summary = {"candidate_ns": bench.paired_benchmark.distribution(candidate), "baseline_ns": bench.paired_benchmark.distribution(baseline)}
            return bench.acceptance(summary, list(zip(candidate, baseline)), faster)
        same = check([100] * 20, [100] * 20)
        self.assertTrue(same["passed"])
        self.assertFalse(same["faster_pass"])
        self.assertFalse(check([100] * 20, [100] * 20, True)["passed"])
        self.assertTrue(check([80] * 20, [100] * 20, True)["passed"])
        self.assertTrue(check([105] * 20, [100] * 20)["passed"])
        self.assertFalse(check([106] * 20, [100] * 20)["no_regression_pass"])
        self.assertFalse(check([100] * 16 + [200] * 4, [100] * 20)["no_regression_pass"])

    def test_receipt_binding_observes_actual_backend_without_cpu_filename_inference(self):
        with tempfile.TemporaryDirectory() as directory:
            path, arms, model = self.receipts(Path(directory), backend="metal")
            inventory = {"data": [{"id": "actual-test-incarnation", "model": "model", "backend": "metal", "state": "loaded"}]}
            with patch.object(bench.qualification_identity, "model_artifacts", return_value=model) as observed, patch.object(bench.qualification_identity.q, "http", return_value=(200, inventory, None)):
                bound = bench.bind_arms(path, arms)
                self.assertEqual(observed.call_count, 2)
                self.assertTrue(bound["arms"]["candidate"]["live_inventory_observed"])
                self.assertEqual(bound["arms"]["candidate"]["model_artifacts_checked"]["models"][0]["backend"], "metal")
            with patch.object(bench.qualification_identity, "model_artifacts") as observed:
                declared = bench.bind_arms(path, arms, observe=False)
                observed.assert_not_called()
                self.assertFalse(declared["arms"]["baseline"]["live_inventory_observed"])
            (Path(directory) / "binary").write_bytes(b"changed")
            with self.assertRaisesRegex(ValueError, "artifact changed"):
                bench.bind_arms(path, arms)

    def test_receipt_binding_refuses_endpoint_envelope_dirty_build_and_model_mismatch(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path, arms, model = self.receipts(root)
            original = json.loads(path.read_text())
            for field, value in [("endpoint", "http://localhost:9999"), ("precision", "bfloat16"), ("thread_budget", {"cpu": 4}), ("source_commit", "d" * 40)]:
                changed = json.loads(json.dumps(original))
                changed["arms"]["candidate"][field] = value
                path.write_text(json.dumps(changed))
                with self.subTest(field=field), patch.object(bench.qualification_identity, "model_artifacts", return_value=model), patch.object(bench.qualification_identity.q, "http", return_value=(200, {"data": [{"id": "test", "model": "model", "backend": "native", "state": "loaded"}]}, None)), self.assertRaises(ValueError):
                    bench.bind_arms(path, arms)
            path.write_text(json.dumps(original))
            wrong = json.loads(json.dumps(model))
            wrong["models"][0]["backend"] = "cuda"
            with patch.object(bench.qualification_identity, "model_artifacts", return_value=wrong), self.assertRaisesRegex(bench.qualification_identity.q.EvidenceUnavailable, "artifacts differ"):
                bench.bind_arms(path, arms)

    def test_guarded_build_map_keeps_compiled_revision_independent_of_tool_edits(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path, arms, model = self.receipts(root)
            source = root / "kernel.zig"
            source.write_text("const value = 1;\n")
            tool_name = "zig/pkg/inference/scripts/gliner25/benchmark_linux_cpu_http.py"
            tool = root / tool_name
            tool.parent.mkdir(parents=True)
            tool.write_text("old tool")
            payload = root / "kernels.cubin"
            payload.write_bytes(b"actual immutable embedded fixture")
            source_map = root / "source-map.json"
            source_map.write_text(json.dumps({"revision": "a" * 40, "files": {"kernel.zig": bench.sha256(source), tool_name: bench.sha256(tool), "kernels.cubin": bench.sha256(payload)}}))
            build = root / "guarded-build.json"
            build.write_text(json.dumps({"revision": "a" * 40, "returncode": 0, "source_unchanged": True, "source_map_sha256": bench.sha256(source_map), "image": str(root / "original-build-image"), "image_sha256": bench.sha256(root / "binary"), "image_bytes": (root / "binary").stat().st_size}))
            receipt = json.loads(path.read_text())
            for arm in receipt["arms"].values():
                arm["build_receipt"] = {"format": "guarded_source_map", "path": str(build), "sha256": bench.sha256(build)}
                arm["source_map"] = {"path": str(source_map), "sha256": bench.sha256(source_map)}
                arm["source_root"] = str(root)
            path.write_text(json.dumps(receipt))
            tool.write_text("later independent benchmark tool")
            bench.bind_arms(path, arms, observe=False)
            # A copied image is valid by exact SHA/size, not original path.
            copied = root / "frozen-image-copy"
            copied.write_bytes((root / "binary").read_bytes())
            receipt["arms"]["candidate"]["binary"] = {"path": str(copied), "sha256": bench.sha256(copied)}
            path.write_text(json.dumps(receipt))
            bench.bind_arms(path, arms, observe=False)
            unrelated = root / "unrelated-image"
            unrelated.write_bytes(b"unrelated executable")
            receipt["arms"]["candidate"]["binary"] = {"path": str(unrelated), "sha256": bench.sha256(unrelated)}
            path.write_text(json.dumps(receipt))
            with self.assertRaisesRegex(ValueError, "build image differs"):
                bench.bind_arms(path, arms, observe=False)
            receipt["arms"]["candidate"]["binary"] = {"path": str(copied), "sha256": bench.sha256(copied)}
            path.write_text(json.dumps(receipt))
            payload.write_bytes(b"changed embedded fixture")
            with self.assertRaisesRegex(ValueError, "artifact changed"):
                bench.bind_arms(path, arms, observe=False)
            payload.write_bytes(b"actual immutable embedded fixture")
            source.write_text("changed compiled source")
            with self.assertRaisesRegex(ValueError, "artifact changed"):
                bench.bind_arms(path, arms, observe=False)

    def test_audited_attempt_keeps_request_response_and_http_failure(self):
        class Response(io.BytesIO):
            status = 200
        case = {"id": "q", "path": "/decide", "request": {"text": "x"}, "expected": {"ok": True}, "confidence_tolerance": 0}
        events = []
        with patch.object(bench, "urlopen", return_value=Response(b'{"ok":true}')):
            result = bench.audited_round("http://localhost", case, 5, 1, events.append, {"arm": "candidate"})
        self.assertEqual(result["output"], {"ok": True})
        self.assertEqual(events[0]["status"], "pending")
        self.assertEqual(bench.base64.b64decode(events[-1]["clients"][0]["raw_body_base64"]), b'{"ok":true}')
        failure = HTTPError("http://localhost/decide", 503, "busy", {}, io.BytesIO(b'{"error":"busy"}'))
        with patch.object(bench, "urlopen", side_effect=failure), self.assertRaises(ValueError):
            bench.audited_round("http://localhost", case, 5, 1, events.append, {"arm": "baseline"})
        self.assertEqual(events[-1]["status"], "failed")
        self.assertEqual(events[-1]["clients"][0]["http_status"], 503)
        self.assertEqual(bench.base64.b64decode(events[-1]["clients"][0]["raw_body_base64"]), b'{"error":"busy"}')

    def test_body_read_failure_retains_actual_status_and_partial_wire_bytes(self):
        from http.client import IncompleteRead
        class Response(io.BytesIO):
            status = 200
            def read(self, *_):
                raise IncompleteRead(b'{"partial":', 100)
        events = []
        case = {"path": "/decide", "request": {}}
        with patch.object(bench, "urlopen", return_value=Response()), self.assertRaises(IncompleteRead):
            bench.audited_round("http://localhost", case, 5, 1, events.append, {})
        trace = events[-1]["clients"][0]
        self.assertEqual(events[-1]["status"], "failed")
        self.assertEqual(trace["http_status"], 200)
        self.assertTrue(trace["response_incomplete"])
        self.assertEqual(bench.base64.b64decode(trace["raw_body_base64"]), b'{"partial":')
        self.assertGreater(trace["duration_ns"], 0)

    def test_http_error_body_read_failure_keeps_error_status_and_partial_bytes(self):
        from http.client import IncompleteRead
        class TruncatedBody(io.BytesIO):
            def read(self, *_):
                raise IncompleteRead(b'{"error":', 100)
        error = HTTPError("http://localhost/decide", 503, "busy", {}, TruncatedBody())
        events = []
        with patch.object(bench, "urlopen", side_effect=error), self.assertRaises(IncompleteRead):
            bench.audited_round("http://localhost", {"path": "/decide", "request": {}}, 5, 1, events.append, {})
        trace = events[-1]["clients"][0]
        self.assertEqual(events[-1]["status"], "failed")
        self.assertEqual(trace["http_status"], 503)
        self.assertTrue(trace["response_incomplete"])
        self.assertEqual(bench.base64.b64decode(trace["raw_body_base64"]), b'{"error":')
        self.assertGreater(trace["duration_ns"], 0)

    def test_audited_interrupt_never_drops_or_pads_attempts(self):
        events = []
        case = {"path": "/decide", "request": {}}
        with patch.object(bench, "request_round", side_effect=KeyboardInterrupt), self.assertRaises(KeyboardInterrupt):
            bench.audited_round("http://localhost", case, 5, 1, events.append, {})
        self.assertEqual([x["status"] for x in events], ["pending", "failed"])
        self.assertEqual(events[-1]["error"]["type"], "KeyboardInterrupt")
        self.assertIsNone(events[-1]["clients"][0]["raw_body_base64"])

    def test_gate_cli_requires_receipts_before_network(self):
        with patch.object(sys, "argv", ["bench", "--cases", "unused", "--baseline", "http://localhost:1", "--candidate", "http://localhost:2", "--output", "unused", "--require-faster"]), patch.object(bench, "urlopen") as network:
            with self.assertRaises(SystemExit):
                bench.main()
            network.assert_not_called()

    def test_underpowered_gated_pairs_are_rejected_but_legacy_two_pairs_remain_allowed(self):
        argv = ["bench", "--cases", "unused", "--baseline", "http://localhost:1", "--candidate", "http://localhost:2", "--output", "unused", "--arm-receipts", "unused", "--pairs", "2", "--enforce-no-regression"]
        with patch.object(sys, "argv", argv), patch.object(bench, "urlopen") as network, self.assertRaises(SystemExit):
            bench.main()
        network.assert_not_called()
        # The original live-loopback test still uses2 pairs in legacy mode.

    def test_explicit_load_preflight_precedes_live_identity_and_preserves_all_attempts(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = root / "cases.json"
            manifest.write_text(json.dumps({"artifacts": [__file__], "cases": [{"id": "q", "path": "/decide", "request": {}, "expected": {}, "confidence_tolerance": 0}]}))
            output = root / "run"
            calls = []
            def bind(*_, observe=True):
                calls.append("live" if observe else "declared")
                if observe:
                    self.assertGreaterEqual(calls.count("request"), 2)
                return {"identity": "stable", "observed": observe}
            def request(*args):
                calls.append("request")
                # Trace fields are normally filled by actual receive.
                for trace in args[4]:
                    trace.update(raw=b"{}", http_status=200, duration_ns=100)
                return {"duration_ns": 100, "output": {}}
            argv = ["bench", "--cases", str(manifest), "--baseline", "http://localhost:1", "--candidate", "http://localhost:2", "--output", str(output), "--arm-receipts", "arms", "--load-preflight", "--enforce-no-regression", "--pairs", "10", "--warmup", "1"]
            with patch.object(sys, "argv", argv), patch.object(bench, "host_provenance", return_value={}), patch.object(bench, "bind_arms", side_effect=bind), patch.object(bench, "request_round", side_effect=request):
                self.assertEqual(bench.main(), 0)
            self.assertEqual(calls[:4], ["declared", "request", "request", "live"])
            report = json.loads((output / "report.json").read_text())
            self.assertTrue(report["acceptance_passed"])
            self.assertFalse(report["performance_release_qualified"])
            self.assertEqual([r["order"] for r in report["samples"][:2]], [["baseline", "candidate"], ["candidate", "baseline"]])
            events = [json.loads(line) for line in (output / "attempts.jsonl").read_text().splitlines()]
            self.assertEqual(len(events), 48)  # 2load+2warm+20measured, pending+finished each.

    def test_partial_failed_gate_report_retains_completed_arm_and_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = root / "cases.json"
            manifest.write_text(json.dumps({"artifacts": [__file__], "cases": [{"id": "q", "path": "/decide", "request": {}, "expected": {}, "confidence_tolerance": 0}]}))
            output = root / "run"
            calls = 0
            def request(*args):
                nonlocal calls
                calls += 1
                args[4][0].update(raw=b"{}", http_status=200)
                if calls == 2:
                    raise ValueError("output mismatch")
                return {"duration_ns": 100, "output": {}}
            argv = ["bench", "--cases", str(manifest), "--baseline", "http://localhost:1", "--candidate", "http://localhost:2", "--output", str(output), "--arm-receipts", "arms", "--enforce-no-regression", "--pairs", "10", "--warmup", "0"]
            with patch.object(sys, "argv", argv), patch.object(bench, "host_provenance", return_value={}), patch.object(bench, "bind_arms", return_value={"stable": True}), patch.object(bench, "request_round", side_effect=request), self.assertRaisesRegex(ValueError, "output mismatch"):
                bench.main()
            report = json.loads((output / "report.json").read_text())
            self.assertEqual(report["status"], "failed")
            self.assertEqual(calls, 2)
            self.assertIn("baseline", report["samples"][0])
            self.assertNotIn("candidate", report["samples"][0])
            events = [json.loads(line) for line in (output / "attempts.jsonl").read_text().splitlines()]
            self.assertEqual(events[-1]["status"], "failed")
            self.assertEqual(len(events), 4)


if __name__ == "__main__":
    unittest.main()
