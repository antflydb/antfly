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
