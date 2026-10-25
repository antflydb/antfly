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

"""Verify that the diagnostic observes DONE without waiting for transport EOF."""

import http.client
import io
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import threading
import unittest

from qualify_completion import ProbeOnEof, guard_probe, read_response, require_released


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Connection", "close")
        self.end_headers()
        event = {
            "choices": [
                {"index": 0, "delta": {"content": "answer"}, "finish_reason": "stop"}
            ],
            "usage": {
                "prompt_tokens": 1,
                "completion_tokens": 1,
                "total_tokens": 2,
                "cached_prompt_tokens": 1,
            },
        }
        self.wfile.write(
            ("data: " + json.dumps(event) + "\n\ndata: [DONE]\n\n").encode()
        )
        self.wfile.flush()
        # Deliberately leave the SSE body open until the next request arrives.
        self.server.probe_before_eof = self.server.probed.wait(3)
        self.close_connection = True

    def do_POST(self):
        self.rfile.read(int(self.headers["Content-Length"]))
        self.server.probe_count += 1
        self.server.probed.set()
        payload = json.dumps(
            {"error": "INVALID_KV_CHECKPOINT", "message": "InvalidNativeKvFrame"}
        ).encode()
        self.send_response(self.server.probe_status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


class CompletionProbeTests(unittest.TestCase):
    def run_probe(self, status):
        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        server.probed = threading.Event()
        server.probe_count = 0
        server.probe_status = status
        server.probe_before_eof = False
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        stream = http.client.HTTPConnection("127.0.0.1", server.server_port, timeout=5)
        probe = http.client.HTTPConnection("127.0.0.1", server.server_port, timeout=5)
        try:
            probe.connect()
            stream.request("GET", "/stream")
            result = read_response(stream.getresponse(), True)
            self.assertEqual(result["choices"][0]["message"]["content"], "answer")
            observed = guard_probe(probe)
            # EOF can only occur after the probe; there is no timing sleep.
            stream.close()
            probe.close()
            server.shutdown()
            thread.join()
            self.assertTrue(server.probe_before_eof)
            self.assertEqual(server.probe_count, 1)
            return observed
        finally:
            server.probed.set()
            stream.close()
            probe.close()
            server.shutdown()
            server.server_close()
            thread.join()

    def test_done_triggers_probe_before_eof(self):
        require_released(self.run_probe(400))

    def test_busy_after_done_is_preserved_without_retry(self):
        with self.assertRaisesRegex(AssertionError, "HTTP 503"):
            require_released(self.run_probe(503))

    def test_arbitrary_bad_request_does_not_prove_guard_released(self):
        with self.assertRaisesRegex(AssertionError, "unexpected guard probe"):
            require_released({"status": 400, "body": '{"error":"INVALID_REQUEST"}'})

    def test_export_probe_runs_at_eof_once_not_on_zero_size_read(self):
        calls = []

        def probe():
            calls.append("probe")
            return {"status": 400}

        reader = ProbeOnEof(io.BytesIO(b"payload"), probe)
        self.assertEqual(reader.read(0), b"")
        self.assertEqual(calls, [])
        self.assertEqual(reader.read(7), b"payload")
        self.assertEqual(calls, [])
        self.assertEqual(reader.read(1), b"")
        self.assertEqual(calls, ["probe"])
        self.assertEqual(reader.result, {"status": 400})
        self.assertEqual(reader.read(1), b"")
        self.assertEqual(calls, ["probe"])


if __name__ == "__main__":
    unittest.main()
