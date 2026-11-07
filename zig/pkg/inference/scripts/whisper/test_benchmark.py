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

"""Hermetic HTTP transcription comparison controls; no model or server required."""
import base64
import contextlib
import importlib.util
import io
import json
import tempfile
import unittest
import urllib.error
import wave
from pathlib import Path
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("whisper_benchmark", Path(__file__).with_name("benchmark.py"))
benchmark = importlib.util.module_from_spec(spec)
spec.loader.exec_module(benchmark)


def response(text="hello", status="complete", **extra):
    return {"data": [{"text": text, "status": status, **extra}]}


def record(seconds, payload):
    raw = json.dumps(payload).encode()
    return {"seconds": seconds, "http_status": 200, "response_bytes_base64": base64.b64encode(raw).decode(), "response": payload, "error": None}


class WhisperBaselineTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.audio = self.root / "audio.wav"
        with wave.open(str(self.audio), "wb") as output:
            output.setnchannels(1)
            output.setsampwidth(2)
            output.setframerate(16000)
            output.writeframes(b"\0\0" * 16)
        self.reference = self.root / "reference.txt"
        self.reference.write_text("hello")
        self.output = self.root / "report.json"
        self.common = ["--audio", str(self.audio), "--reference", str(self.reference), "--output", str(self.output), "--trials", "3", "--warmup", "1"]

    def run_baseline(self, candidate=(1, 1, 1), baseline=(1, 1, 1), require_faster=False, payloads=None):
        positions = {"candidate": 0, "baseline": 0}
        calls = []

        def post(url, body, content_type):
            calls.append((url, body, content_type))
            index = positions[url]
            positions[url] += 1
            times = candidate if url == "candidate" else baseline
            seconds = times[max(0, index - 1)]
            payload = payloads(url, index) if payloads else response()
            return record(seconds, payload)

        argv = self.common + ["--antfly-url", "candidate", "--baseline-antfly-url", "baseline", "--language", "es", "--timestamps"]
        if require_faster:
            argv.append("--require-faster")
        with patch.object(benchmark, "post_record", side_effect=post), patch.object(benchmark, "multipart", side_effect=AssertionError("baseline must not build cpp request")), contextlib.redirect_stdout(io.StringIO()):
            code = benchmark.main(argv)
        return code, json.loads(self.output.read_text()), calls

    def test_identical_request_bytes_balanced_order_and_complete_raw_evidence(self):
        code, report, calls = self.run_baseline()
        self.assertEqual(0, code)
        self.assertEqual(["candidate", "baseline", "baseline", "candidate", "candidate", "baseline", "baseline", "candidate"], [call[0] for call in calls])
        self.assertEqual(1, len({call[1] for call in calls}))
        packet = json.loads(calls[0][1])
        self.assertEqual({"language": "es", "timestamps": True}, packet["audio_options"])
        self.assertEqual("openai/whisper-small", packet["model"])
        self.assertNotIn("temperature", packet)
        self.assertTrue(report["complete"] and report["regression_passed"])
        self.assertFalse(report["faster_passed"])
        for rows in report["measurements"].values():
            self.assertEqual(4, len(rows))
            self.assertEqual(3, sum(row["phase"] == "measurement" for row in rows))
            for row in rows:
                self.assertEqual(response(), json.loads(base64.b64decode(row["response_bytes_base64"])))
                self.assertEqual("hello", row["text"])

    def test_explicit_regression_limits_and_separate_require_faster(self):
        code, report, _ = self.run_baseline(candidate=(1.05, 1.05, 1.05))
        self.assertEqual(0, code)
        self.assertTrue(report["regression_passed"])
        code, report, _ = self.run_baseline(candidate=(1.05, 1.05, 1.05), require_faster=True)
        self.assertEqual(1, code)
        self.assertFalse(report["faster_passed"])
        code, report, _ = self.run_baseline(candidate=(1.06, 1.06, 1.06))
        self.assertEqual(1, code)
        self.assertFalse(report["regression_passed"])
        code, report, _ = self.run_baseline(candidate=(1, 1, 2))
        self.assertEqual(1, code)
        self.assertEqual(1, report["candidate_baseline_latency_ratio"]["median"])
        self.assertGreater(report["candidate_baseline_latency_ratio"]["p95"], 1.10)
        code, report, _ = self.run_baseline(candidate=(0.9, 0.9, 0.9), require_faster=True)
        self.assertEqual(0, code)
        self.assertGreater(report["baseline_candidate_speedup_ci"]["lower_95"], 1)

    def test_incomplete_error_and_quality_regressions_persist_all_attempts(self):
        for payload in [response(status="partial"), response(error={"message": "failed"}), {"error": "failed"}, {"data": []}]:
            with self.subTest(payload=payload):
                code, report, calls = self.run_baseline(payloads=lambda url, index: payload if url == "baseline" and index == 1 else response())
                self.assertEqual(1, code)
                self.assertFalse(report["complete"] or report["performance_passed"])
                self.assertEqual(3, len(calls))
                self.assertEqual("baseline_antfly", report["aborted"]["target"])
                failed = report["measurements"]["baseline_antfly"][1]
                self.assertEqual(payload, failed["response"])
                self.assertEqual("failed", failed["state"])
        code, report, _ = self.run_baseline(candidate=(0.5, 0.5, 0.5), payloads=lambda url, _: response("wrong" if url == "candidate" else "hello"))
        self.assertEqual(1, code)
        self.assertFalse(report["quality_passed"] or report["performance_passed"])

    def test_interrupted_pending_attempt_remains_in_failed_report(self):
        with patch.object(benchmark, "post_record", side_effect=KeyboardInterrupt), contextlib.redirect_stdout(io.StringIO()), self.assertRaises(KeyboardInterrupt):
            benchmark.main(self.common + ["--baseline-antfly-url", "baseline"])
        report = json.loads(self.output.read_text())
        self.assertFalse(report["complete"] or report["performance_passed"])
        self.assertEqual("pending", report["measurements"]["antfly"][0]["state"])
        self.assertEqual([], report["measurements"]["baseline_antfly"])

    def test_http_failure_retains_exact_body_and_exception(self):
        failure = urllib.error.HTTPError("endpoint", 503, "busy", {}, io.BytesIO(b'{"error":"busy"}'))
        with patch.object(benchmark.urllib.request, "urlopen", side_effect=failure):
            row = benchmark.post_record("http://example.test", b"{}", "application/json")
        self.assertEqual(503, row["http_status"])
        self.assertEqual(b'{"error":"busy"}', base64.b64decode(row["response_bytes_base64"]))
        self.assertIsNotNone(row["error"])
        self.assertIsNone(row["response"])

    def test_cpp_default_report_and_estimator_remain_original(self):
        calls = []

        def post(url, body, content_type):
            calls.append(url)
            return (1, response()) if url.endswith("/read") else (2, {"text": "hello"})

        with patch.object(benchmark, "post", side_effect=post), contextlib.redirect_stdout(io.StringIO()):
            code = benchmark.main(self.common)
        report = json.loads(self.output.read_text())
        self.assertEqual(0, code)
        self.assertEqual(2, report["antfly_speedup"])
        self.assertEqual([2, 2], report["speedup_95_percent_interval"])
        self.assertIn("http://127.0.0.1:18082/inference", calls)
        self.assertNotIn("schema", report)
        self.assertNotIn("regression_passed", report)
        self.assertEqual({"antfly", "whisper_cpp"}, set(report["measurements"]))

    def test_comparison_urls_are_exclusive_and_faster_is_baseline_only(self):
        for extra in [["--baseline-antfly-url", "baseline", "--whisper-url", "cpp"], ["--require-faster"]]:
            with self.subTest(extra=extra), contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                benchmark.main(self.common + extra)
        self.assertEqual("", benchmark.extract_antfly_text(response(text="")))
        with self.assertRaises(ValueError):
            benchmark.extract_antfly_text({"data": [response()["data"][0], response()["data"][0]]})


if __name__ == "__main__":
    unittest.main()
