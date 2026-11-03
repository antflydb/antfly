# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0

import copy
import unittest

if __package__:
    from . import cpu_query_campaign as campaign
else:
    import cpu_query_campaign as campaign


class CrossBackendQueryContracts(unittest.TestCase):
    def fixture(self):
        suite = {
            "retrieval": {
                "queries": ["q"],
                "documents": ["a", "b"],
                "expected": {"q": "a"},
            }
        }

        def report(backend, vectors):
            return {
                "backend": backend,
                "model_identity": backend,
                "suite_sha256": "same-corpus",
                "cases": [
                    {
                        "name": name,
                        "vector": vector,
                        "dimensions": 2,
                        "input_tokens": 3,
                        "p50_seconds": 0.01,
                        "p95_seconds": 0.02,
                    }
                    for name, vector in vectors.items()
                ],
            }

        # Each backend retrieves A using its own documents. CPU queries against
        # the fixed CUDA index retrieve B: testing CPU-to-CPU would hide this.
        native = report("native", {"q": [0, 1], "a": [0, 1], "b": [1, 0]})
        cuda = report("cuda", {"q": [1, 0], "a": [1, 0], "b": [0, 1]})
        return suite, native, cuda

    def test_fallback_queries_search_the_existing_cuda_documents(self):
        result = campaign.assess(*self.fixture())
        query = result["retrieval"]["rankings"]["q"]
        self.assertEqual(query["cpu_query_ranking"], ["b", "a"])
        self.assertEqual(query["cuda_query_ranking"], ["a", "b"])
        self.assertFalse(query["same_top1"])
        self.assertEqual(result["retrieval"]["same_top1"], 0)

    def test_different_tokenization_cannot_be_compared_as_same_query(self):
        suite, native, cuda = self.fixture()
        native["cases"][0]["input_tokens"] = 4
        with self.assertRaisesRegex(ValueError, "token counts"):
            campaign.assess(suite, native, cuda)

    def test_incomplete_case_coverage_cannot_hide_a_failed_query(self):
        suite, native, cuda = self.fixture()
        native["cases"].pop()
        with self.assertRaisesRegex(ValueError, "coverage"):
            campaign.assess(suite, native, cuda)

    def test_unmatched_corpus_and_wrong_backend_are_rejected(self):
        suite, native, cuda = self.fixture()
        different = copy.deepcopy(cuda)
        different["suite_sha256"] = "other-corpus"
        with self.assertRaisesRegex(ValueError, "fingerprints"):
            campaign.assess(suite, native, different)
        different["backend"] = "native"
        with self.assertRaisesRegex(ValueError, "backends"):
            campaign.assess(suite, native, different)


if __name__ == "__main__":
    unittest.main()
