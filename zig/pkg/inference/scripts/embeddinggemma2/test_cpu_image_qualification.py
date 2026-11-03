# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Image numerical, coverage, provenance and cross-modal relevance guards."""

import json
import math
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

import cpu_http_qualification as common
import cpu_image_qualification as image
import cpu_image_reference as reference


class ImageVectorGates(unittest.TestCase):
    def test_nonfinite_shape_norm_and_drift_fail_closed(self):
        expected = [1.0] + [0.0] * 767
        image.vector_agreement(expected, expected, 1e-4, 0.99999)
        cases = [
            [1.0],
            [math.nan] + [0.0] * 767,
            [0.5] + [0.0] * 767,
            [0.0, 1.0] + [0.0] * 766,
        ]
        for vector in cases:
            with self.assertRaises(ValueError):
                image.vector_agreement(vector, expected, 1e-4, 0.99999)
        with self.assertRaises(ValueError):
            image.vector_agreement(expected, [math.nan] + [0.0] * 767, 1e-4, 0.99999)

    def test_image_retrieval_recall_counts_a_matching_caption_as_a_hit(self):
        scores = image.rank_metrics(["caption-a", "other"], {"caption-a", "caption-b"})
        self.assertEqual(scores["recall1"], 1)
        self.assertEqual(scores["recall5"], 1)
        self.assertLess(scores["ndcg10"], 1)
        with self.assertRaises(ValueError):
            image.rank_metrics([], set())

    def test_reversed_image_vectors_expose_relevance_loss_in_both_directions(self):
        suite = {"cases": [], "queries": []}
        reference_vectors = {"cases": {}}
        candidate = {"cases": {}}
        queries = {}
        for at in range(128):
            name = f"image-{at}"
            suite["cases"].append({"name": name, "role": "retrieval"})
            vector = [float(i == at) for i in range(768)]
            reversed_vector = [float(i == (at + 1) % 128) for i in range(768)]
            reference_vectors["cases"][name] = {"vector": vector}
            candidate["cases"][name] = {"vector": reversed_vector}
            for n in range(5):
                q = f"caption-{at}-{n}"
                queries[q] = vector
                suite["queries"].append({"name": q, "relevant_image": name})
        result = image.retrieval(suite, reference_vectors, candidate, queries)
        for direction in ["text_to_image", "image_to_text"]:
            self.assertEqual(result["upstream"][direction]["recall1"], 1)
            self.assertEqual(result["cpu"][direction]["recall1"], 0)
            self.assertEqual(result["losses"][direction]["recall1"], 1)
        queries.pop(next(iter(queries)))
        with self.assertRaisesRegex(ValueError, "coverage"):
            image.retrieval(suite, reference_vectors, candidate, queries)


class ImageEvidenceGates(unittest.TestCase):
    def test_asset_mutation_and_duplicate_cases_are_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            asset = directory / "image.png"
            asset.write_bytes(b"fixture")
            row = {"name": "image", "path": str(asset), "sha256": common.digest(asset)}
            path = directory / "suite.json"
            path.write_text(json.dumps({"cases": [row]}))
            args = SimpleNamespace(suite=path)
            image.load_suite(args)
            asset.write_bytes(b"changed")
            with self.assertRaisesRegex(ValueError, "digest"):
                image.load_suite(args)
            path.write_text(json.dumps({"cases": [row, row]}))
            with self.assertRaisesRegex(ValueError, "duplicate"):
                image.load_suite(args)

    def test_failed_and_wrong_suite_candidates_are_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            suite_path = directory / "suite.json"
            suite_path.write_text("{}")
            candidate = directory / "cpu.json"
            args = SimpleNamespace(suite=suite_path, candidate=candidate)
            for data in [
                {"status": "fail"},
                {"status": "pass", "suite_sha256": "wrong", "cases": {}},
            ]:
                candidate.write_text(json.dumps(data))
                with self.assertRaisesRegex(ValueError, "matching CPU"):
                    image.candidate(args, {"cases": []})

    def test_published_archive_hash_is_required(self):
        with tempfile.TemporaryDirectory() as temporary:
            archive = Path(temporary) / "fake.parquet"
            archive.write_bytes(b"wrong dataset")
            with self.assertRaisesRegex(ValueError, "publisher SHA256"):
                reference.prepare(archive, Path(temporary) / "out")

    def test_ordered_reference_places_document_prefix_once(self):
        case = {"parts": [{"image": "a"}, {"text": "Describe."}, {"image": "b"}]}
        self.assertEqual(
            reference.rendered_parts(case),
            "title: none | text: <|image|>\nDescribe.\n<|image|>",
        )
        self.assertEqual(
            reference.rendered_parts({"parts": [{"image": "a"}]}), "<|image|>"
        )


if __name__ == "__main__":
    unittest.main()
