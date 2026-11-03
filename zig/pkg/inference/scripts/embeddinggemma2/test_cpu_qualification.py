# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Fail-closed checks for CPU qualification evidence and relevance metrics."""

import io
import json
import math
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import beir_cpu_query_qualification as beir
import cpu_http_qualification as http


class RetrievalMetricsTest(unittest.TestCase):
    def test_graded_relevance_and_cutoffs(self):
        labels = {"strong": 2, "weak": 1}
        ideal = beir.metrics(["strong", "weak"], labels)
        self.assertEqual(ideal["ndcg10"], 1)
        self.assertEqual(ideal["recall10"], 1)
        swapped = beir.metrics(["weak", "strong"], labels)
        self.assertLess(swapped["ndcg10"], 1)
        order = [f"irrelevant-{n}" for n in range(10)] + ["strong", "weak"]
        result = beir.metrics(order, labels)
        self.assertEqual(result["ndcg10"], 0)
        self.assertEqual(result["recall10"], 0)
        self.assertEqual(result["mrr10"], 0)
        self.assertEqual(result["recall100"], 1)

    def test_archive_must_match_publisher_digest(self):
        with tempfile.TemporaryDirectory() as temporary:
            archive = Path(temporary) / "scifact.zip"
            archive.write_bytes(b"an archive with a different corpus")
            with self.assertRaisesRegex(ValueError, "publisher digest"):
                beir.load_dataset(archive)


class ServingEvidenceTest(unittest.TestCase):
    def test_vector_validation_rejects_shape_nonfinite_drift_and_identity(self):
        expected = [1.0, 0.0]
        response = {
            "model_identity": "a" * 64,
            "backend": "native",
            "data": [{"index": 0, "embedding": expected}],
        }
        http.validate(response, [expected], "a" * 64)
        for bad in [[1], [math.nan, 0], [0, 1], [0.5, 0]]:
            response["data"][0]["embedding"] = bad
            with self.assertRaises(AssertionError):
                http.validate(response, [expected], "a" * 64)
        response["data"][0]["embedding"] = expected
        with self.assertRaisesRegex(ValueError, "identity changed"):
            http.validate(response, [expected], "b" * 64)
        response["data"][0]["index"] = 1
        with self.assertRaisesRegex(ValueError, "coverage/order"):
            http.validate(response, [expected], "a" * 64)

    def test_gpu_result_cannot_qualify_as_cpu(self):
        with self.assertRaisesRegex(ValueError, "native CPU"):
            http.validate({"backend": "cuda"}, [[1]], "cpu")

    def test_missing_lifecycle_metrics_fail_closed(self):
        with (
            patch(
                "urllib.request.urlopen",
                return_value=io.BytesIO(b"antfly_inference_requests_active 0\n"),
            ),
            self.assertRaisesRegex(ValueError, "lifecycle metrics missing"),
        ):
            http.cpu_metrics("http://unused")

    def test_dimension_prefix_is_renormalized(self):
        self.assertEqual(http.normalized_prefix([0.6, 0.8], 1), [1])


class RetrievalEvidenceTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.write("dataset.json", {"qrels": {"q": {"d": 1}}})
        self.write("queries.json", {"cases": [{"name": "q:q"}]})
        self.write("documents.json", {"cases": [{"name": "d:d"}]})
        suite = self.write(
            "documents-000.suite.json", {"cases": [{"name": "d:d"}, {"name": "q:q"}]}
        )
        receipt = self.write(
            "documents-000.cuda.json",
            {
                "backend": "cuda",
                "suite_sha256": beir.sha256(suite),
                "model_identity": "gpu",
                "cases": [{"name": name, "vector": [1]} for name in ["d:d", "q:q"]],
            },
        )
        self.write(
            "cuda-manifest.json",
            {
                "model_identity": "gpu",
                "chunks": {receipt.name: {"sha256": beir.sha256(receipt)}},
            },
        )
        candidate = self.write(
            "cpu.json",
            {
                "status": "pass",
                "model_identity": "cpu",
                "cases": {"q:q": {"vector": [1]}},
            },
        )
        self.args = SimpleNamespace(output_dir=self.root, cpu_receipt=candidate)

    def write(self, name, data):
        path = self.root / name
        path.write_text(json.dumps(data))
        return path

    def test_tampered_cuda_receipt_rejected(self):
        receipt = self.root / "documents-000.cuda.json"
        receipt.write_text(receipt.read_text() + " ")
        with self.assertRaisesRegex(ValueError, "receipt digest"):
            beir.evaluate(self.args)

    def test_missing_manifest_chunk_rejected(self):
        self.write("cuda-manifest.json", {"chunks": {}})
        with self.assertRaisesRegex(ValueError, "manifest coverage"):
            beir.evaluate(self.args)

    def test_failed_candidate_cannot_qualify(self):
        self.write("cpu.json", {"status": "fail", "cases": {}})
        with self.assertRaisesRegex(ValueError, "did not pass"):
            beir.evaluate(self.args)

    def test_candidate_cannot_drop_query(self):
        self.write("cpu.json", {"status": "pass", "cases": {}})
        with self.assertRaisesRegex(ValueError, "query coverage"):
            beir.evaluate(self.args)


if __name__ == "__main__":
    unittest.main()
