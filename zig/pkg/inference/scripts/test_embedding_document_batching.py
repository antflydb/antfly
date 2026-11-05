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

"""Retrieval qualification must consume actual concurrent-forward vectors."""
import copy
import unittest

import qualify_embedding_document_batching as d


class RecordedRetrievalTests(unittest.TestCase):
    def setUp(self):
        identity = {'model_artifacts': {'version': 1, 'models': [{'model': 'model', 'backend': 'cuda', 'tasks': ['embedder'], 'files': [
            {'path': 'config.json', 'size': 2, 'sha256': '0'*64},
            {'path': 'model.safetensors', 'size': 16, 'sha256': '1'*64},
        ]}]}}
        self.rows = []
        self.candidate = {'identity': identity, 'cases': []}
        for i, query in enumerate(d.QUERIES):
            vector = [float(i==j) for j in range(4)]
            response = {'data': [{'embedding': vector}]}
            request = d.body(f'document {i}')
            self.candidate['cases'].append({'level': 128, 'request': request, 'reference': copy.deepcopy(response)})
            self.rows.append({'phase': 'documents-128-2', 'kind': 'embed', 'status': 200, 'request': request, 'response': copy.deepcopy(response)})
            self.rows.append({'phase': 'document-retrieval', 'kind': 'embed', 'status': 200, 'request': d.body(query, task='RETRIEVAL_QUERY'), 'response': copy.deepcopy(response)})
        self.control = {'identity': copy.deepcopy(identity), 'retrieval': {'128': {'top1': [0,1,2,3], 'scores': [[float(i==j) for j in range(4)] for i in range(4)]}}}

    def test_recorded_batches_preserve_rankings(self):
        result = d.verify_recorded_retrieval(self.control, self.candidate, self.rows)
        self.assertEqual(result['128']['top1'], [0,1,2,3])
        self.assertEqual(result['128']['documents_from'], 'recorded concurrent burst')

    def test_good_singletons_cannot_hide_a_broken_concurrent_vector(self):
        self.rows[0]['response']['data'][0]['embedding'] = [0,1,0,0]
        with self.assertRaises(AssertionError):
            d.verify_recorded_retrieval(self.control, self.candidate, self.rows)

    def test_missing_document_vector_is_incomplete(self):
        self.rows.pop(0)
        with self.assertRaises(d.q.EvidenceUnavailable):
            d.verify_recorded_retrieval(self.control, self.candidate, self.rows)

    def test_missing_query_vector_is_incomplete(self):
        self.rows.pop(1)
        with self.assertRaises(d.q.EvidenceUnavailable):
            d.verify_recorded_retrieval(self.control, self.candidate, self.rows)


if __name__ == '__main__':
    unittest.main()
