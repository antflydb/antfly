import unittest

from replay_embedding_trace import compare_vectors, endpoint_url, replay_body


class ReplayTests(unittest.TestCase):
    def test_destination_requires_explicit_remote_authority(self):
        self.assertEqual(
            endpoint_url("http://127.0.0.1:8080"),
            "http://127.0.0.1:8080/ai/v1/embeddings",
        )
        self.assertEqual(
            endpoint_url("http://[::1]:8080"), "http://[::1]:8080/ai/v1/embeddings"
        )
        with self.assertRaises(ValueError):
            endpoint_url("https://example.com")
        with self.assertRaises(ValueError):
            endpoint_url("http://user:password@localhost")
        self.assertEqual(
            endpoint_url("https://example.com", True),
            "https://example.com/ai/v1/embeddings",
        )

    def test_replay_preserves_text_role_and_instruction(self):
        capture = {
            "model": "model",
            "input": ['quotes "\n한국'],
            "task_type": "RETRIEVAL_DOCUMENT",
            "instruction": "exact",
            "vectors": [[1.0]],
            "path": "managed_direct",
        }
        body = replay_body(capture)
        self.assertEqual(body["input"], capture["input"])
        self.assertEqual(body["instruction"], "exact")
        self.assertNotIn("vectors", body)
        self.assertNotIn("path", body)

    def test_parity_rejects_shape_nonfinite_and_drift(self):
        self.assertTrue(compare_vectors([[1.0, 0.0]], [[1.0, 0.0]], 1e-4)["passed"])
        self.assertFalse(compare_vectors([[1.0]], [[0.9]], 1e-4)["passed"])
        for actual in ([[float("nan")]], [[float("inf")]], [[True]], [[]], []):
            with self.assertRaises(ValueError):
                compare_vectors([[1.0]], actual, 1e-4)


if __name__ == "__main__":
    unittest.main()
