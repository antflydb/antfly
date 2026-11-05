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
import unittest

import qualify_read_pool_40g as q
from qualify_write_pool_production import assert_equivalent, frozen_boundary_output, latency_gate, require_observed_batch


class QualificationTests(unittest.TestCase):
    def test_queue_turnover_does_not_establish_physical_batching(self):
        for kind in ('embed', 'extract', 'whisper'):
            with self.assertRaises(q.EvidenceUnavailable):
                require_observed_batch(kind, {}, {'antfly_inference_executor_microbatch_groups_total': 10}, [{}])
            prefix = 'antfly_inference_executor_microbatch_'
            zero = {prefix+'native_batches_total': 0, prefix+'native_items_total': 0}
            positive = {prefix+'native_batches_total': 1, prefix+'native_items_total': 2}
            self.assertEqual(require_observed_batch(kind, zero, positive, [{}])['native_items'], 2)
            with self.assertRaises(q.EvidenceUnavailable):
                require_observed_batch(kind, {}, positive, [{}])
            with self.assertRaises(q.EvidenceUnavailable):
                require_observed_batch(kind, positive, zero, [{}])

    def test_native_attribution_without_measured_ocr_width_is_incomplete(self):
        for response in ({}, {'execution': {'native_items': 1}}, {'execution': {'max_native_step_items': 1}}):
            with self.assertRaises(q.EvidenceUnavailable):
                require_observed_batch('ocr', {}, {}, [response])
        self.assertEqual(require_observed_batch('ocr', {}, {}, [{'execution': {'max_native_step_items': 2}}])['batched_requests'], 1)

    def test_usage_or_source_identity_changes_fail_parity(self):
        reference = {'data': [{'id': 'first', 'entities': [{'score': .9}]}], 'usage': {'prompt_tokens': 17}}
        assert_equivalent('extract', reference, reference)
        for mutate in ('id', 'tokens', 'score'):
            observed = copy.deepcopy(reference)
            if mutate == 'id':
                observed['data'][0]['id'] = 'other'
            elif mutate == 'tokens':
                observed['usage']['prompt_tokens'] = 18
            else:
                observed['data'][0]['entities'][0]['score'] = .8
            with self.assertRaises(AssertionError):
                assert_equivalent('extract', reference, observed)

    def test_regression_and_promotion_are_separate_gates(self):
        self.assertFalse(latency_gate(1, 1, [.9])['promotion_10_percent_pass'])
        self.assertTrue(latency_gate(1, .8, [.7])['promotion_10_percent_pass'])
        with self.assertRaises(AssertionError):
            latency_gate(1, 1.2, [1.2])
        with self.assertRaises(q.EvidenceUnavailable):
            latency_gate(0, 1, [])

    def test_frozen_oracle_rejects_wrong_spans_scores_and_non_numeric_confidence(self):
        expected = {'entities': [{'name': 'person', 'values': [{'text': 'Ada', 'confidence': .9, 'source': {'start': 1, 'end': 4}}]}], 'classifications': [], 'relations': [], 'structures': []}
        actual = {'entities': [{'label': 'person', 'text': 'Ada', 'score': .9, 'start': 1, 'end': 4}]}
        frozen_boundary_output(expected, actual)
        for field, value in (('start', 2), ('label', 'place'), ('score', True), ('score', float('nan')), ('score', .8)):
            changed = copy.deepcopy(actual)
            changed['entities'][0][field] = value
            with self.assertRaises(AssertionError):
                frozen_boundary_output(expected, changed)


if __name__ == '__main__':
    unittest.main()
