"""Contract tests of the qualifier, not evidence of real model qualification."""

import io
import json
import sys
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
import qualify_qwen3_embedding_common as common
from test_qualify_qwen3_embedding_metal import make_oracle_payload


class ConcurrentQualificationTests(unittest.TestCase):
    def setUp(self):
        payload = make_oracle_payload()
        self.cases = common.validate_oracle(payload)
        self.vectors = {
            key: case["embeddings"]["1024"] for key, case in self.cases.items()
        }
        self.by_text = {
            common.case_input(case): key for key, case in self.cases.items()
        }
        self.args = SimpleNamespace(
            model="model",
            base_url="http://unused",
            timeout=5,
            concurrency=4,
            concurrent_rounds=2,
        )
        self.instruction = payload["contract"]["default_instruction"]

    def response(self, body):
        vector = self.vectors[self.by_text[body["input"]]]
        return [
            (
                common.truncate_and_renormalize(vector, body["dimensions"])
                if "dimensions" in body
                else vector
            )
        ]

    def test_bounded_requests_overlap_and_preserve_roles_dimensions_and_order(self):
        barrier = threading.Barrier(4, timeout=5)
        lock = threading.Lock()
        windows = []
        current = []

        def post(_url, body, _timeout):
            with lock:
                current.append(body)
                if len(current) == 4:
                    windows.append(list(current))
                    current.clear()
            barrier.wait()
            return self.response(body)

        with patch.object(common, "post_embeddings", side_effect=post):
            rows = common.concurrent_request_gates(
                self.args, self.cases, self.vectors, self.instruction
            )
        self.assertEqual(64, len(rows))
        self.assertTrue(all(row["pass"] for row in rows))
        for window in windows:
            self.assertEqual(
                {None, "RETRIEVAL_QUERY"}, {body.get("task_type") for body in window}
            )
            self.assertEqual({None, 256}, {body.get("dimensions") for body in window})

    def test_cross_request_leakage_and_dimension_leakage_fail_existing_tolerance(self):
        for bad in ([1.0] + [0.0] * 1023, [1.0]):
            with (
                self.subTest(width=len(bad)),
                patch.object(common, "post_embeddings", return_value=[bad]),
            ):
                rows = common.concurrent_request_gates(
                    self.args, self.cases, self.vectors, self.instruction
                )
                self.assertTrue(any(not row["pass"] for row in rows))

    def test_request_failure_is_not_dropped(self):
        with (
            patch.object(
                common,
                "post_embeddings",
                side_effect=common.QualificationError("failed"),
            ),
            self.assertRaises(common.QualificationError),
        ):
            common.concurrent_request_gates(
                self.args, self.cases, self.vectors, self.instruction
            )

    def test_concurrent_normalization_uses_existing_family_tolerance(self):
        for scale, passes in ((7.0, False), (0.5, False), (1.0005, True)):

            def post(_url, body, _timeout):
                return [[value * scale for value in self.response(body)[0]]]

            with (
                self.subTest(scale=scale),
                patch.object(common, "post_embeddings", side_effect=post),
            ):
                rows = common.concurrent_request_gates(
                    self.args, self.cases, self.vectors, self.instruction
                )
                norms = [
                    row for row in rows if row["gate"] == "cross_request_unit_norm"
                ]
                self.assertEqual(32, len(norms))
                self.assertEqual(passes, all(row["pass"] for row in norms))

    def test_http_item_indexes_are_a_permutation_not_just_the_right_count(self):
        for indexes in ((0, 0), (1, 2), (False, 1), (None, 1)):
            body = {"data": [{"index": index, "embedding": [1.0]} for index in indexes]}
            with (
                self.subTest(indexes=indexes),
                patch.object(
                    common.urllib.request,
                    "urlopen",
                    return_value=io.BytesIO(json.dumps(body).encode()),
                ),
                self.assertRaises(common.QualificationError),
            ):
                common.post_embeddings("http://unused", {"input": ["a", "b"]}, 5)

    def test_http_reorders_unique_item_indexes(self):
        body = {
            "data": [{"index": 1, "embedding": [2.0]}, {"index": 0, "embedding": [1.0]}]
        }
        with patch.object(
            common.urllib.request,
            "urlopen",
            return_value=io.BytesIO(json.dumps(body).encode()),
        ):
            self.assertEqual(
                [[1.0], [2.0]],
                common.post_embeddings("http://unused", {"input": ["a", "b"]}, 5),
            )


if __name__ == "__main__":
    unittest.main()
