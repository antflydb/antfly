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


if __name__ == "__main__":
    unittest.main()
