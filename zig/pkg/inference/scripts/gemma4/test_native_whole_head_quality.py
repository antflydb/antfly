# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0

"""Portable consumed-record regression checks; never fresh model evidence."""
import base64
import copy
import hashlib
import json
from pathlib import Path
import unittest

import native_whole_head_quality as contract


def replace(path, value):
    def mutate(report):
        part = report
        for key in path[:-1]:
            part = part[key]
        part[path[-1]] = value
    return mutate


class ObservedContract(unittest.TestCase):
    def setUp(self):
        fixture = json.loads((Path(__file__).parent / 'testdata/native_whole_head_quality_v2.json').read_bytes())
        self.report, self.case, self.expected = [fixture[key] for key in ['report', 'case', 'expected']]
        self.inputs = {key: base64.b64decode(raw, validate=True) for key, raw in fixture['inputs_base64'].items()}
        self.assertFalse(fixture['provenance']['fresh_quality_evidence'])
        self.assertFalse(fixture['provenance']['release_authority'])

    def check(self, report=None):
        return contract.case_contract(self.report if report is None else report, self.case, self.inputs, self.expected)

    def test_observed_baseline_failure_is_retained(self):
        row = self.check()
        self.assertFalse(row['exact_correct'])
        self.assertFalse(row['candidate_correct'])
        self.assertFalse(row['release_authority'])
        self.assertFalse(row['full_serving_owner_qualified'])
        self.assertEqual(row['scored_tokens'], self.case['reference_count'])

    def test_observed_nll_remains_token_weighted(self):
        row = self.check()
        self.assertAlmostEqual((row['candidate_nll_sum'] - row['exact_nll_sum']) / row['scored_tokens'], 8.632788765858665e-6)

    def test_binding_ownership_route_and_measurement_tampering_rejected(self):
        r = self.report
        mutations = {
            'different_device': replace(['frozen', 'observed', 'destination', 'device_registry_id'], 2),
            'different_execution': replace(['frozen', 'execution', 'coalesced_stage'], True),
            'forged_configuration_digest': replace(['frozen', 'configuration_sha256'], [9] * 32),
            'public_scope': replace(['full_serving_owner_qualified'], True),
            'zero_load': replace(['load_generation'], 0),
            'boolean_case_index': replace(['case_index'], False),
            'gold_early': replace(['reference_revealed_after_both_greedy_branches'], False),
            'source_mutation': replace(['source_unchanged'], False),
            'consumer_missing': lambda x: x['calibration_consumers'].pop(),
            'calibration_tamper': lambda x: x['frozen']['calibration']['train_values_sha256'].__setitem__(0, x['frozen']['calibration']['train_values_sha256'][0] ^ 1),
            'wrong_destination_epoch': replace(['frozen', 'destination_revision_before_install'], r['frozen']['source_revision']),
            'boolean_installed_epoch': replace(['frozen', 'installed_revision'], True),
            'runtime_kv_grew': replace(['compacted_greedy_runtime_kv_bytes_after'], r['compacted_greedy_runtime_kv_bytes_after'] + 4096),
            'partial_layer_route': replace(['compacted_teacher', 'prepared_layer_blocks'], r['compacted_teacher']['prepared_layer_blocks'] - 1),
            'nan_probability': replace(['compacted_teacher', 'log_probabilities', 1], float('nan')),
            'forged_nll': replace(['compacted_teacher', 'nll'], 0.),
            'owner_alias': replace(['compacted_greedy', 'owner_id'], r['source_owner_id']),
            'forged_measurement': replace(['exact_greedy', 'release', 'measurement', 'committed_live_bytes'], 1),
            'snapshot_other_owner': replace(['compacted_teacher', 'release', 'committed', 'owner_identity'], 999),
            'retained_private_bytes': replace(['compacted_greedy', 'release', 'final', 'private_device_live_bytes'], 4096),
            'nonadditive_components': replace(['source_release', 'committed', 'tracked_combined_live_bytes'], 42),
            'missing_physical_backing': replace(['compacted_release', 'committed', 'physical_available'], False),
            'unvalidated_owner': replace(['frozen', 'validated_heads'], 1),
            'wrong_first_score': replace(['compacted_teacher', 'log_probabilities', 0], -1.),
            'fit_mass_failed': replace(['max_head_mass_error'], .051),
            'fit_output_failed': replace(['max_head_output_rmse'], .051),
            'no_global_savings': replace(['candidate_global_bytes'], r['source_global_bytes']),
            'wrong_implementation': replace(['frozen', 'implementation_sha256'], [7] * 32),
            'different_checkpoint': replace(['frozen', 'installed_sha256'], [7] * 32),
            'wrong_parent_clock': replace(['collection_elapsed_ns'], 1),
        }
        for name, mutate in mutations.items():
            with self.subTest(name=name):
                report = copy.deepcopy(self.report)
                mutate(report)
                with self.assertRaises(ValueError):
                    self.check(report)

    def test_all_input_roles_are_bound(self):
        for role in self.inputs:
            with self.subTest(role=role):
                inputs = dict(self.inputs)
                inputs[role] += b'!'
                with self.assertRaises(ValueError):
                    contract.case_contract(self.report, self.case, inputs, self.expected)

    def test_case_must_belong_to_the_sealed_manifest(self):
        case = copy.deepcopy(self.case)
        case['id'] = 'substituted-case'
        report = copy.deepcopy(self.report)
        report['case'] = case
        with self.assertRaisesRegex(ValueError, 'sealed manifest'):
            contract.case_contract(report, case, self.inputs, self.expected)

    def test_ordinary_backend_route_requires_pinned_complete_frame_counts(self):
        report = copy.deepcopy(self.report)
        ordinary = report['exact_greedy']
        ordinary['ordinary_serving_forward_steps'] = ordinary['forward_steps']
        ordinary['ordinary_backend_frames'] = ordinary['forward_steps']
        with self.assertRaisesRegex(ValueError, 'ordinary serving route'):
            self.check(report)
        expected = dict(self.expected, ordinary_greedy_route='backend_owned')
        row = contract.case_contract(report, self.case, self.inputs, expected)
        self.assertFalse(row['release_authority'])
        ordinary['ordinary_backend_frames'] -= 1
        with self.assertRaisesRegex(ValueError, 'ordinary serving route'):
            contract.case_contract(report, self.case, self.inputs, expected)

    def test_source_prefill_route_must_match_campaign(self):
        report = copy.deepcopy(self.report)
        report['source_serving_prefill'] = True
        with self.assertRaisesRegex(ValueError, 'source prefill route'):
            self.check(report)
        expected = dict(self.expected, source_serving_prefill=True)
        contract.case_contract(report, self.case, self.inputs, expected)
        report['source_serving_prefill'] = 1
        with self.assertRaisesRegex(ValueError, 'source prefill route'):
            contract.case_contract(report, self.case, self.inputs, expected)

    def test_requested_codec_requires_actual_residency(self):
        for encoding in ['fp8', 'rotated4']:
            case = copy.deepcopy(self.case)
            case['request']['encoding'] = encoding
            report = copy.deepcopy(self.report)
            report['case'] = case
            expected = dict(self.expected, encoding=encoding)
            inputs = dict(self.inputs)
            manifest = json.loads(inputs['manifest'])
            manifest['cases'][0] = case
            inputs['manifest'] = (json.dumps(manifest) + '\n').encode()
            report['manifest_sha256'] = list(hashlib.sha256(inputs['manifest']).digest())
            with self.assertRaisesRegex(ValueError, 'resident encoding'):
                contract.case_contract(report, case, inputs, expected)


if __name__ == '__main__':
    unittest.main()
