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

import copy
import hashlib
import json
from pathlib import Path
import tempfile
import io
import urllib.error
import unittest
from unittest import mock
from types import SimpleNamespace

if __package__:
    from . import video_compare as video
else:
    import video_compare as video


class VideoRetrievalContracts(unittest.TestCase):
    def setUp(self):
        self.reference = {
            "scores": {"q": {"a": 0.8, "b": 0.4, "c": 0.1}},
            "rankings": {"q": ["a", "b", "c"]},
        }

    def test_timestamp_oracle_uses_rational_pts_and_stable_ties(self):
        frames = [{"best_effort_timestamp": pts} for pts in (3003, 0, 2002, 1001, 2002)]
        self.assertEqual(
            video.timestamp_selection(
                frames,
                "1/30000",
                {
                    "policy": "timestamps",
                    "step_ns": 33_366_667,
                },
            ),
            [1, 2, 0],
        )
        self.assertEqual(
            video.timestamp_selection(
                frames,
                "1/30000",
                {
                    "policy": "timestamps",
                    "step_ns": 50_000_000,
                    "end_ns": 100_100_000,
                },
            ),
            [1, 2],
        )

    def test_timestamp_oracle_caps_long_windows_and_rejects_bad_policies(self):
        frames = [{"best_effort_timestamp": i, "duration": 1} for i in range(100)]
        selected = video.timestamp_selection(frames, "1/1", {"policy": "timestamps"})
        self.assertEqual((len(selected), selected[0], selected[-1]), (32, 0, 99))
        self.assertEqual(
            video.timestamp_selection(
                frames,
                "1/1",
                {
                    "policy": "timestamps",
                    "start_ns": 60_000_000_000,
                    "end_ns": 90_000_000_000,
                },
            ),
            list(range(60, 90)),
        )
        for policy in (
            {"policy": "timestamps", "step_ns": 0},
            {"policy": "timestamps", "max_frames": True},
            {"policy": "timestamps", "end_ns": 0},
            {"policy": "timestamps", "start_ns": 101_000_000_000},
            {"policy": "timestamps", "unknown": 1},
        ):
            with self.assertRaises(ValueError):
                video.timestamp_selection(frames, "1/1", policy)

    def test_memory_counts_supervised_workers_and_excludes_unrelated_processes(self):
        records = {1: (0, 8), 2: (1, 1500), 3: (2, 100), 4: (0, 9999)}
        self.assertEqual(video.process_tree_rss(1, records), 1608)
        self.assertEqual(video.process_tree_rss(2, records), 1600)
        with self.assertRaises(ValueError):
            video.process_tree_rss(5, records)

    def test_exact_scores_rankings_and_capped_top_k(self):
        result = video.rank_agreement(self.reference, self.reference, [1, 2, 10], 1e-4)
        self.assertTrue(result["pass"])
        self.assertEqual(
            result["queries"]["q"]["top_k_overlap"], {"1": 1, "2": 1, "10": 1}
        )

    def test_distinct_ranking_change_fails_even_without_score_drift(self):
        actual = copy.deepcopy(self.reference)
        actual["rankings"]["q"] = ["b", "a", "c"]
        result = video.rank_agreement(actual, self.reference, [1], 1e-4)
        self.assertFalse(result["pass"])
        self.assertEqual(result["queries"]["q"]["top_k_overlap"]["1"], 0)
        self.assertEqual(len(result["queries"]["q"]["non_tied_inversions"]), 1)

    def test_numerical_ties_report_exact_overlap_without_false_quality_claim(self):
        expected = {
            "scores": {"q": {"a": 0.8, "b": 0.800001}},
            "rankings": {"q": ["b", "a"]},
        }
        actual = copy.deepcopy(expected)
        actual["rankings"]["q"] = ["a", "b"]
        result = video.rank_agreement(actual, expected, [1], 1e-4)
        self.assertTrue(result["pass"])
        self.assertFalse(result["queries"]["q"]["exact_ranking"])
        self.assertEqual(result["queries"]["q"]["top_k_overlap"]["1"], 0)

    def test_score_drift_missing_documents_and_nonfinite_values_fail(self):
        actual = copy.deepcopy(self.reference)
        actual["scores"]["q"]["a"] += 0.01
        self.assertFalse(
            video.rank_agreement(actual, self.reference, [1], 1e-4)["pass"]
        )
        for mutation in (
            lambda a: a["scores"]["q"].pop("a"),
            lambda a: a["scores"]["q"].update(a=float("nan")),
            lambda a: a["rankings"]["q"].append("a"),
        ):
            actual = copy.deepcopy(self.reference)
            mutation(actual)
            with self.assertRaises(ValueError):
                video.rank_agreement(actual, self.reference, [1], 1e-4)

    def test_corpus_requires_verified_clips_unique_ids_and_provenance(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            clip = root / "clip.mp4"
            clip.write_bytes(b"original test bytes")
            data = {
                "documents": [
                    {
                        "id": "d",
                        "path": "clip.mp4",
                        "sha256": hashlib.sha256(clip.read_bytes()).hexdigest(),
                        "provenance": "original test fixture",
                    }
                ],
                "queries": [{"id": "q", "text": "scene"}],
            }
            path = root / "corpus.json"
            path.write_text(json.dumps(data))
            self.assertEqual(
                video.corpus(path)["documents"][0]["path"], str(clip.resolve())
            )
            for change in (
                lambda d: d["queries"][0].update(id="d"),
                lambda d: d["documents"][0].update(provenance=""),
                lambda d: d["documents"][0].update(sha256="0" * 64),
            ):
                modified = copy.deepcopy(data)
                change(modified)
                path.write_text(json.dumps(modified))
                with self.assertRaises(ValueError):
                    video.corpus(path)


class CompletedReferenceEvidenceTests(unittest.TestCase):
    def test_complete_vectors_keep_rank_failures_when_stress_failed(self):
        data = {"documents": [{"id": "a"}, {"id": "b"}], "queries": [{"id": "q"}]}
        metadata = {
            "corpus_sha256": "corpus",
            "checkpoint_receipt_sha256": "weights",
            "precision": "float32",
        }
        expected = dict(
            metadata,
            status="pass",
            cases={
                "q": {"vector": [1.0, 0.0]},
                "a": {"vector": [1.0, 0.0]},
                "b": {"vector": [0.0, 1.0]},
            },
        )
        expected["retrieval"] = video.retrieval(
            data, {n: c["vector"] for n, c in expected["cases"].items()}
        )
        result = dict(
            metadata,
            status="failed",
            failure={"http_status": 503},
            cases={
                "q": {"vector": [1.0, 0.0]},
                "a": {"vector": [0.6, 0.8]},
                "b": {"vector": [0.8, 0.6]},
            },
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "reference.json"
            path.write_text(json.dumps(expected))
            args = SimpleNamespace(reference=path, top_k=[1, 2], score_tolerance=1e-4)
            video.collect_reference_evidence(args, data, result)
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["failure"], {"http_status": 503})
        self.assertEqual(result["retrieval"]["rankings"]["q"], ["b", "a"])
        self.assertFalse(result["retrieval_agreement"]["pass"])
        self.assertEqual(
            result["retrieval_agreement"]["queries"]["q"]["top_k_overlap"]["1"], 0.0
        )
        self.assertGreater(result["vector_agreement"]["a"]["max_abs"], 1e-4)

    def test_missing_cases_are_not_reported_as_complete_reference_evidence(self):
        metadata = {
            "corpus_sha256": "corpus",
            "checkpoint_receipt_sha256": "weights",
            "precision": "float32",
        }
        data = {"documents": [{"id": "a"}], "queries": [{"id": "q"}]}
        vectors = {"q": {"vector": [1.0, 0.0]}, "a": {"vector": [1.0, 0.0]}}
        expected = dict(
            metadata, status="pass", cases=dict(vectors, missing={"vector": [0.0, 1.0]})
        )
        result = dict(metadata, status="running", cases=vectors)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "reference.json"
            path.write_text(json.dumps(expected))
            args = SimpleNamespace(reference=path, top_k=[1], score_tolerance=1e-4)
            with self.assertRaisesRegex(ValueError, "reference cases differ"):
                video.collect_reference_evidence(args, data, result)
        self.assertNotIn("vector_agreement", result)


class FailureDetailsTests(unittest.TestCase):
    def test_http_failure_retains_bounded_response_and_closes_it(self):
        body = io.BytesIO(b'{"message":"UnsupportedTensorType"}')
        error = urllib.error.HTTPError(
            "http://127.0.0.1/embed", 400, "Bad Request", {}, body
        )
        details = video.failure_details(error)
        self.assertEqual(details["http_status"], 400)
        self.assertIn("UnsupportedTensorType", details["response_body"])
        self.assertFalse(details["response_body_truncated"])
        self.assertTrue(body.closed)

    def test_large_or_invalid_utf8_body_is_bounded(self):
        body = io.BytesIO(b"\xff" * 70000)
        error = urllib.error.HTTPError("http://127.0.0.1/embed", 503, "Busy", {}, body)
        details = video.failure_details(error)
        self.assertEqual(len(details["response_body"]), 65536)
        self.assertTrue(details["response_body_truncated"])
        self.assertTrue(body.closed)

    def test_non_http_exception_keeps_type_and_message(self):
        self.assertEqual(
            video.failure_details(ValueError("invalid corpus")),
            {"type": "ValueError", "message": "invalid corpus"},
        )


class AdmissionRetryAndCleanupTests(unittest.TestCase):
    @staticmethod
    def denial(status=503, **fields):
        body = dict(retryable=True, reason="inference_capacity", retry_after_ms=1000)
        body.update(fields)
        return urllib.error.HTTPError(
            "http://127.0.0.1/embed",
            status,
            "Busy",
            {},
            io.BytesIO(json.dumps(body).encode()),
        )

    def clock(self):
        now = [0.0]
        return (
            now,
            mock.patch.object(video.time, "monotonic", side_effect=lambda: now[0]),
            mock.patch.object(
                video.time,
                "sleep",
                side_effect=lambda seconds: now.__setitem__(0, now[0] + seconds),
            ),
        )

    def test_declared_capacity_delay_and_remaining_request_timeout_are_honored(self):
        denial = self.denial()
        now, clock, sleep = self.clock()
        with (
            clock,
            sleep,
            mock.patch.object(
                video.compare, "post", side_effect=[denial, {"ok": True}]
            ) as post,
        ):
            response, attempts = video.post_capacity_retry("http://127.0.0.1", {})
        self.assertEqual(response, {"ok": True})
        self.assertEqual(now[0], 1)
        self.assertEqual(
            [call.kwargs["timeout"] for call in post.call_args_list], [30, 29]
        )
        self.assertEqual(attempts[0]["retry_delay_seconds"], 1)
        self.assertTrue(denial.closed)

    def test_persistent_denials_expire_and_preserve_all_failure_bodies(self):
        denials = [self.denial() for _ in range(3)]
        now, clock, sleep = self.clock()
        with (
            clock,
            sleep,
            mock.patch.object(video.compare, "post", side_effect=denials),
        ):
            with self.assertRaises(video.CapacityRetryFailure) as failure:
                video.post_capacity_retry("http://127.0.0.1", {}, timeout=2.5)
        self.assertEqual(now[0], 2)
        details = video.failure_details(failure.exception)
        self.assertEqual(len(details["attempts"]), 3)
        self.assertEqual(details["http_status"], 503)
        self.assertTrue(all(error.closed for error in denials))

    def test_non_capacity_or_malformed_retry_contract_fails_immediately(self):
        for policy in (
            {"retryable": False},
            {"reason": "model_publication"},
            {"retry_after_ms": True},
            {"retry_after_ms": 0},
            {"retry_after_ms": 31000},
        ):
            with self.subTest(policy=policy):
                denial = self.denial(**policy)
                with (
                    mock.patch.object(
                        video.compare, "post", side_effect=denial
                    ) as post,
                    mock.patch.object(video.time, "sleep") as sleep,
                ):
                    with self.assertRaises(video.CapacityRetryFailure):
                        video.post_capacity_retry("http://127.0.0.1", {})
                self.assertEqual(post.call_count, 1)
                sleep.assert_not_called()
                self.assertTrue(denial.closed)
        with mock.patch.object(
            video.compare, "post", side_effect=self.denial(status=400)
        ) as post:
            with self.assertRaises(video.CapacityRetryFailure):
                video.post_capacity_retry("http://127.0.0.1", {})
            self.assertEqual(post.call_count, 1)

    def test_transport_errors_are_not_hidden_by_capacity_retry(self):
        with mock.patch.object(
            video.compare, "post", side_effect=ConnectionResetError("reset")
        ) as post:
            with self.assertRaises(video.CapacityRetryFailure) as failure:
                video.post_capacity_retry("http://127.0.0.1", {})
            self.assertEqual(post.call_count, 1)
            self.assertEqual(
                video.failure_details(failure.exception)["last_failure"]["type"],
                "ConnectionResetError",
            )

    def test_small_retry_delays_remain_bounded_by_attempt_count(self):
        now, clock, sleep = self.clock()
        with (
            clock,
            sleep,
            mock.patch.object(
                video.compare,
                "post",
                side_effect=[self.denial(retry_after_ms=1) for _ in range(3)],
            ) as post,
        ):
            with self.assertRaisesRegex(video.CapacityRetryFailure, "attempt limit"):
                video.post_capacity_retry("http://127.0.0.1", {}, max_attempts=3)
        self.assertEqual(post.call_count, 3)
        self.assertLess(now[0], 0.01)

    def test_cleanup_metrics_use_operational_route_and_close_response(self):
        response = io.BytesIO(
            b"antfly_inference_embedding_groups_active 1\n"
            b"antfly_inference_embedding_groups_cancelled_total 2\n"
            b"antfly_inference_embedding_groups_completed_total 10\n"
            b"antfly_admission_inference_in_flight_requests 1\n"
        )
        with mock.patch.object(
            video.urllib.request, "urlopen", return_value=response
        ) as get:
            self.assertEqual(
                video.embedding_metrics("http://127.0.0.1"),
                dict(active=1, cancelled=2, completed=10, admitted=1),
            )
        get.assert_called_once_with("http://127.0.0.1/ml/v1/metrics", timeout=5)
        self.assertTrue(response.closed)

    def test_cleanup_waits_for_admission_release_after_cancelled_outcome(self):
        before = dict(active=0, admitted=0, cancelled=0, completed=0)
        pending = dict(active=0, admitted=1, cancelled=1, completed=0)
        done = dict(pending, admitted=0)
        now, clock, sleep = self.clock()
        with (
            clock,
            sleep,
            mock.patch.object(video, "embedding_metrics", side_effect=[pending, done]),
        ):
            self.assertEqual(
                video.wait_embedding_cleanup("http://127.0.0.1", before, "cancelled"),
                done,
            )
        self.assertGreater(now[0], 0)

    def test_retry_completion_cannot_be_confused_with_original_completion(self):
        before = dict(active=0, admitted=0, cancelled=0, completed=0)
        pending = dict(active=1, admitted=1, cancelled=0, completed=1)
        done = dict(active=0, admitted=0, cancelled=1, completed=1)
        now, clock, sleep = self.clock()
        with (
            clock,
            sleep,
            mock.patch.object(video, "embedding_metrics", side_effect=[pending, done]),
        ):
            self.assertEqual(
                video.wait_embedding_cleanup(
                    "http://127.0.0.1", before, "cancelled", expected_completions=1
                ),
                done,
            )

    def test_completed_request_is_not_reported_as_cancelled(self):
        before = dict(active=0, admitted=0, cancelled=0, completed=0)
        with mock.patch.object(
            video, "embedding_metrics", return_value=dict(before, completed=1)
        ):
            with self.assertRaisesRegex(ValueError, "instead of cancelling"):
                video.wait_embedding_cleanup("http://127.0.0.1", before, "cancelled")

    def test_cleanup_deadline_remains_a_failure(self):
        before = dict(active=0, admitted=0, cancelled=0, completed=0)
        now, clock, sleep = self.clock()
        with (
            clock,
            sleep,
            mock.patch.object(video, "embedding_metrics", return_value=before),
        ):
            with self.assertRaises(TimeoutError):
                video.wait_embedding_cleanup(
                    "http://127.0.0.1", before, "cancelled", timeout=0.1
                )
        self.assertGreaterEqual(now[0], 0.1)


class SustainedContracts(unittest.TestCase):
    def run_load(self, responses, metrics_after=None):
        import threading

        lock = threading.Lock()
        state = {"active": 0, "admitted": 0, "cancelled": 0, "completed": 0}

        def metrics(url):
            with lock:
                if metrics_after is not None and state["completed"] == len(responses):
                    return dict(state, **metrics_after)
                return dict(state)

        def post(url, body, timeout):
            response = responses[body["index"]]
            if isinstance(response, Exception):
                raise response
            with lock:
                state["completed"] += 1
            return response, [{"http_status": 503, "retry_delay_seconds": 1.0}] if body[
                "index"
            ] == 0 else []

        bodies = {str(i): {"index": i} for i in range(len(responses))}
        vectors = {name: [1.0, 0.0] for name in bodies}
        with (
            mock.patch.object(video, "embedding_metrics", side_effect=metrics),
            mock.patch.object(video, "post_capacity_retry", side_effect=post),
        ):
            return video.sustained_requests(
                "http://127.0.0.1", bodies, vectors, "stable", list(bodies), 2
            )

    @staticmethod
    def response(vector=None, identity="stable"):
        return {
            "model_identity": identity,
            "data": [{"embedding": vector or [1.0, 0.0]}],
        }

    def test_load_requires_all_vectors_and_records_declared_retry_and_timing(self):
        report = self.run_load([self.response(), self.response()])
        self.assertEqual(report["status"], "pass")
        self.assertEqual(report["capacity_denials"], 1)
        self.assertEqual(report["request_timing"]["count"], 2)
        self.assertTrue(report["cleanup_pass"])
        for row in report["requests"]:
            self.assertGreaterEqual(row["total_seconds"], row["request_seconds"])
            self.assertGreaterEqual(row["queue_seconds"], 0)

    def test_failed_request_does_not_discard_other_outcomes_or_pass_load(self):
        report = self.run_load([ValueError("invalid denial"), self.response()])
        self.assertEqual(report["status"], "failed")
        self.assertEqual([r["status"] for r in report["requests"]], ["failed", "pass"])
        self.assertIn("invalid denial", report["requests"][0]["failure"]["message"])

    def test_vector_or_identity_drift_fails_load(self):
        for response in (self.response([0.0, 1.0]), self.response(identity="changed")):
            with self.subTest(response=response):
                self.assertEqual(self.run_load([response])["status"], "failed")

    def test_leaked_admission_or_unexpected_cancellation_fails_load(self):
        for metrics in (
            {"admitted": 1},
            {"active": 1},
            {"cancelled": 1},
        ):
            with self.subTest(metrics=metrics):
                report = self.run_load([self.response()], metrics_after=metrics)
                self.assertEqual(report["status"], "failed")
                self.assertFalse(report["cleanup_pass"])

    def test_late_completions_and_failed_retries_do_not_imply_leaked_admission(self):
        before = {"active": 0, "admitted": 0, "cancelled": 1, "completed": 3}
        after = dict(before, completed=6)
        denial = {"http_status": 503, "retry_delay_seconds": 1.0}
        rows = [
            {"status": "failed", "failure": {"attempts": [{"type": "TimeoutError"}]}},
            {"status": "failed", "failure": {"attempts": [{"type": "TimeoutError"}]}},
            {"status": "failed", "failure": {"attempts": [denial] * 16}},
            {"status": "pass", "capacity_denials": [denial] * 12},
        ]
        evidence = video.sustained_evidence(rows, before, after)
        self.assertEqual(evidence["capacity_denials"], 28)
        self.assertTrue(evidence["cleanup_pass"])
        self.assertFalse(evidence["completion_accounting_pass"])
        self.assertEqual(evidence["unacknowledged_server_completions"], 2)
        self.assertEqual(
            self.run_load([self.response()], {"completed": 9})["status"], "failed"
        )

    def test_idle_owned_server_is_required(self):
        with mock.patch.object(
            video, "embedding_metrics", return_value={"active": 1, "admitted": 1}
        ):
            with self.assertRaisesRegex(ValueError, "idle owned server"):
                video.sustained_requests("http://127.0.0.1", {}, {}, "stable", ["x"], 1)


class ReferenceDecodeContracts(unittest.TestCase):
    def test_reference_input_hash_and_presentation_are_required_when_recorded(self):
        expected = {
            "rgb_sha256": "pinned",
            "frames": 1253,
            "selected_frames": [0, 24],
            "duration": 52.208008,
            "retained_rgb_bytes": 32,
        }
        video.assert_reference_decode(dict(expected, timestamp_evidence=None), expected)
        for key in expected:
            changed = dict(expected)
            changed[key] = "different"
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, key):
                video.assert_reference_decode(changed, expected)
        with self.assertRaisesRegex(ValueError, "rgb_sha256"):
            video.assert_reference_decode({}, expected)

    def test_oracle_without_decoded_pixels_keeps_vector_gate(self):
        video.assert_reference_decode({}, None)
        video.assert_reference_decode({}, {"policy": "legacy oracle"})


class MetalTelemetryContracts(unittest.TestCase):
    def metrics(self, text):
        with mock.patch.object(video.urllib.request, "urlopen") as request:
            request.return_value.__enter__.return_value.read.return_value = (
                text.encode()
            )
            return video.metal_telemetry_snapshot("http://owned")

    def text(self, values):
        return "".join(
            f"{video.metal_metric_name(key)} {value}\n" for key, value in values.items()
        )

    def test_integer_precision_and_zero_platform_metrics(self):
        values = dict.fromkeys(video.METAL_TELEMETRY_NAMES, 0)
        values["buffer_upload_bytes"] = 2**64 - 1
        self.assertEqual(self.metrics(self.text(values)), values)
        evidence = video.metal_telemetry_delta(values, values)
        self.assertEqual(evidence["delta"], dict.fromkeys(values, 0))
        self.assertEqual(evidence["saturated"], ["buffer_upload_bytes"])

    def test_missing_duplicate_and_invalid_metrics_fail(self):
        values = dict.fromkeys(video.METAL_TELEMETRY_NAMES, 0)
        complete = self.text(values)
        for text in (
            "",
            complete + complete,
            complete.replace(" 0", " -1", 1),
            complete.replace(" 0", " 1.0", 1),
            complete.replace(" 0", " nan", 1),
            complete.replace(" 0", " " + str(2**64), 1),
        ):
            with self.subTest(text=text[:60]), self.assertRaises(ValueError):
                self.metrics(text)

    def test_worker_reset_and_invalid_snapshots_fail(self):
        before = dict.fromkeys(video.METAL_TELEMETRY_NAMES, 7)
        after = dict(before, buffer_upload_bytes=6)
        with self.assertRaisesRegex(ValueError, "reset"):
            video.metal_telemetry_delta(before, after)
        for values in (
            {},
            dict(before, buffer_upload_bytes=True),
            dict(before, commands_failed=-1),
        ):
            with self.assertRaises(ValueError):
                video.metal_telemetry_delta(before, values)
        after = {key: value + 3 for key, value in before.items()}
        self.assertEqual(
            video.metal_telemetry_delta(before, after)["delta"],
            dict.fromkeys(before, 3),
        )

    def test_memory_gauges_decrease_without_resetting_lifetime_peaks(self):
        before = dict.fromkeys(video.METAL_TELEMETRY_NAMES, 32)
        after = dict(before, owned_buffer_live_bytes=8, alias_buffer_live_count=0)
        result = video.metal_telemetry_delta(before, after)
        self.assertEqual(result["delta"]["owned_buffer_live_bytes"], -24)
        self.assertEqual(result["delta"]["alias_buffer_live_count"], -32)
        self.assertEqual(result["after"]["owned_buffer_peak_bytes"], 32)
        self.assertEqual(result["gauge_names"], list(video.METAL_MEMORY_GAUGES))
        text = self.text(after)
        self.assertIn("antfly_inference_metal_owned_buffer_live_bytes 8\n", text)
        self.assertNotIn("owned_buffer_live_bytes_total", text)
        self.assertEqual(self.metrics(text), after)
        with self.assertRaisesRegex(ValueError, "reset"):
            video.metal_telemetry_delta(before, dict(after, owned_buffer_peak_bytes=31))


if __name__ == "__main__":
    unittest.main()
