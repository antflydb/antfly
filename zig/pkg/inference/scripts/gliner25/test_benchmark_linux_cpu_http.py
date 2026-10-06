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
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import unittest

import benchmark_linux_cpu_http as bench


class ContractTests(unittest.TestCase):
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
        finally:
            server.shutdown()
            server.server_close()
            thread.join()


if __name__ == "__main__":
    unittest.main()
