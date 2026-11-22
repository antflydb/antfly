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
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import threading

import benchmark_inference_policies as bench


class PolicyBenchmarkTests(unittest.TestCase):
    def test_failed_work_is_charged_and_warmup_is_separate(self):
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

    def test_retry_policy_compares_identical_page_and_trial(self):
        rows = [
            dict(
                endpoint="gpu",
                case="page",
                trial=0,
                max_tokens=4096,
                complete=False,
                truncated=True,
                seconds=10,
            ),
            dict(
                endpoint="gpu",
                case="page",
                trial=0,
                max_tokens=8192,
                complete=True,
                seconds=20,
            ),
        ]
        entry = bench.compare_policies(rows)[0]
        self.assertEqual(entry["old_policy_calls"], 2)
        self.assertEqual(entry["old_policy_seconds"], 30)
        self.assertEqual(entry["estimated_speedup"], 1.5)
        rows[1]["complete"] = False
        self.assertEqual(bench.compare_policies(rows), [])

    def test_read_requires_complete_expected_text(self):
        case = {"task": "read", "expected": ["invoice 42"]}
        for response in [
            {"data": []},
            {"data": [{"status": "complete", "text": "wrong"}]},
            {"data": [{"status": "failed", "text": "invoice 42"}]},
        ]:
            with self.assertRaises(ValueError):
                bench.inspect_response(case, response)
        self.assertFalse(
            bench.inspect_response(case, {"data": [{"finish_reason": "length"}]})[
                "complete"
            ]
        )

    def test_network_refuses_credentials_and_nonlocal_plaintext(self):
        for endpoint in [
            "http://example.com/ai/v1",
            "https://user:secret@example.com/ai/v1",
            "https://example.com/ai/v1?token=secret",
        ]:
            with self.assertRaises(ValueError):
                bench.request(endpoint, "/read", {}, 1)

    def test_resume_retains_results_and_rejects_changed_fixture(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fixture = root / "page.png"
            fixture.write_bytes(b"fixture")
            model = "owner/model:safetensors@" + "a" * 40
            case = dict(
                id="table",
                task="read",
                kind="image",
                fixture="page.png",
                model=model,
                expected=["private fragment"],
            )
            cases = root / "cases.json"
            cases.write_text(json.dumps([case]))
            report = root / "report.json"
            argv = [
                "bench",
                "--cases",
                str(cases),
                "--report",
                str(report),
                "--endpoint",
                "arm=http://localhost:8000/ai/v1",
                "--backend",
                "arm=native",
                "--trials",
                "1",
                "--runtime-image",
                "ghcr.io/a@sha256:" + "b" * 64,
            ]

            def reply(endpoint, route, body, timeout):
                if route == "/models/instances":
                    return 200, {"data": [{"model": model, "backend": "native"}]}
                return 200, {
                    "data": [{"status": "complete", "text": "private fragment"}],
                    "usage": {"completion_tokens": 2},
                }

            with (
                patch("sys.argv", argv),
                patch.object(bench, "request", side_effect=reply) as request,
                contextlib.redirect_stdout(io.StringIO()),
            ):
                self.assertEqual(bench.main(), 0)
                self.assertEqual(request.call_count, 8)
            self.assertNotIn("private fragment", report.read_text())
            with (
                patch("sys.argv", argv + ["--resume"]),
                patch.object(bench, "request") as request,
            ):
                self.assertEqual(bench.main(), 0)
                request.assert_not_called()
            fixture.write_bytes(b"different")
            with (
                patch("sys.argv", argv + ["--resume"]),
                contextlib.redirect_stderr(io.StringIO()),
                self.assertRaises(SystemExit),
            ):
                bench.main()

    def test_embedding_contract_and_nonfinite_values(self):
        case = {"task": "embed", "dimensions": 2}
        self.assertTrue(
            bench.inspect_response(case, {"data": [{"embedding": [1.0, 0.0]}]})[
                "complete"
            ]
        )
        for vector in [[float("nan"), 0], [2, 0], []]:
            with self.assertRaises(ValueError):
                bench.inspect_response(case, {"data": [{"embedding": vector}]})

    def test_http_campaign_handles_direct_truncation_and_embedding_wire_contract(self):
        model = "owner/model:safetensors@" + "a" * 40
        calls = []

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_GET(self):
                self.reply(200, {"data": [{"model": model, "backend": "native"}]})

            def do_POST(self):
                if self.headers.get("X-Antfly-Inference-Timeout-Ms") != "600000":
                    self.reply(400, {"error": "MISSING_INFERENCE_DEADLINE"})
                    return
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                calls.append((self.path, body))
                if self.path == "/ai/v1/embeddings" and body.get("input") == [
                    "example"
                ]:
                    self.reply(200, {"data": [{"embedding": [1.0, 0.0]}]})
                elif self.path == "/ai/v1/read" and body.get("max_tokens") == 4096:
                    self.reply(422, {"error": "OUTPUT_TRUNCATED"})
                elif self.path == "/ai/v1/read" and body.get("max_tokens") == 8192:
                    self.reply(
                        200,
                        {"data": [{"status": "complete", "text": "verified footer"}]},
                    )
                else:
                    self.reply(400, {"error": "INVALID_REQUEST"})

            def reply(self, status, body):
                data = json.dumps(body).encode()
                self.send_response(status)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

        with (
            tempfile.TemporaryDirectory() as directory,
            ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server,
        ):
            root = Path(directory)
            (root / "page.png").write_bytes(b"page fixture")
            (root / "text.txt").write_text("example")
            cases = [
                dict(
                    id="page",
                    task="read",
                    kind="image",
                    fixture="page.png",
                    model=model,
                    expected=["verified footer"],
                ),
                dict(
                    id="embedding",
                    task="embed",
                    fixture="text.txt",
                    model=model,
                    dimensions=2,
                ),
            ]
            (root / "cases.json").write_text(json.dumps(cases))
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            argv = [
                "bench",
                "--cases",
                str(root / "cases.json"),
                "--report",
                str(root / "report.json"),
                "--endpoint",
                f"cpu=http://127.0.0.1:{server.server_port}/ai/v1",
                "--backend",
                "cpu=native",
                "--runtime-image",
                "ghcr.io/a@sha256:" + "b" * 64,
                "--trials",
                "1",
            ]
            try:
                with patch("sys.argv", argv), contextlib.redirect_stdout(io.StringIO()):
                    self.assertEqual(bench.main(), 0)
            finally:
                server.shutdown()
                thread.join(5)
            report = json.loads((root / "report.json").read_text())
            self.assertEqual(len(calls), 6)
            self.assertEqual(report["policy_comparison"][0]["old_policy_calls"], 2)
            self.assertTrue(any(row.get("dimensions") == 2 for row in report["rows"]))


if __name__ == "__main__":
    unittest.main()
