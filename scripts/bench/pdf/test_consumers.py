import unittest
from types import SimpleNamespace

from consumers import all_consumers_complete, table_with_consumers


class ConsumerTests(unittest.TestCase):
    def test_consumers_have_independent_artifacts_and_indexes(self):
        original = {
            "indexes": {
                "document_units": {
                    "artifact": {
                        "name": "document_units_v1",
                        "producer_json": {"type": "document_extraction"},
                    }
                },
                "document_text": {
                    "artifact_name": "document_chunks_v1",
                    "enrichments": [{"name": "document_units_v1"}],
                },
                "document_vectors": {
                    "source_artifact_name": "document_chunks_v1",
                    "embedding_name": "document_chunk_dense_v1",
                },
            }
        }
        result = table_with_consumers(original, 2)
        self.assertEqual(len(original["indexes"]), 3)
        self.assertEqual(len(result["indexes"]), 6)
        other = result["indexes"]["document_units_consumer2"]["artifact"]
        self.assertEqual(other["name"], "document_units_v1_consumer2")
        self.assertEqual(
            other["producer_json"],
            original["indexes"]["document_units"]["artifact"]["producer_json"],
        )
        self.assertEqual(
            result["indexes"]["document_vectors_consumer2"]["source_artifact_name"],
            "document_chunks_v1_consumer2",
        )
        with self.assertRaises(ValueError):
            table_with_consumers(original, 0)

    def test_secondary_coverage_cannot_be_ignored(self):
        api = SimpleNamespace(_index_statuses=lambda values: values)
        coverage = {"observation_complete": True, "complete": True, "source_total": 1}
        manifests = {"document_units_v1": {"a": {"merge_status": "converged"}}}
        statuses = {"document_vectors": {"coverage": coverage}}
        self.assertTrue(all_consumers_complete(api, statuses, manifests, 1, 1))
        self.assertFalse(all_consumers_complete(api, statuses, manifests, 1, 2))
        manifests["document_units_v1_consumer2"] = {"a": {"merge_status": "converged"}}
        self.assertFalse(all_consumers_complete(api, statuses, manifests, 1, 2))
        statuses["document_vectors_consumer2"] = {"coverage": coverage}
        self.assertTrue(all_consumers_complete(api, statuses, manifests, 1, 2))
