# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
import copy
import hashlib
import json
from pathlib import Path
import unittest

import native_checkpoint_soak_evidence as soak
from test_native_whole_head_campaign import ResourceContract

SCRIPTS = Path(__file__).resolve().parent
if not (SCRIPTS / 'testdata/native_whole_head_quality_v2.json').exists():
    SCRIPTS = Path(__file__).resolve().parents[3] / 'zig/pkg/inference/scripts/gemma4'


class PersistentSoak(unittest.TestCase):
    def setUp(self):
        fixture = json.loads((SCRIPTS / 'testdata/native_whole_head_quality_v2.json').read_bytes())['report']
        configuration = {key: fixture['frozen'][key] for key in ('observed', 'execution', 'configuration_sha256')}
        self.expected = {'model': 'gemma4-e2b', 'encoding': 'f16', 'model_directory': '/consumed/model',
                         'input_sha256': '11' * 32, 'artifact_sha256': '22' * 32, 'implementation_sha256': '33' * 32,
                         'native_identity': fixture['frozen']['identity'], 'configuration': configuration,
                         'duration_seconds': 14400, 'prefix_tokens': [7936, 32512],
                         'checkpoint_sha256': ['44' * 32, '55' * 32]}
        releases = [fixture['source_release'], fixture['compacted_release']]
        initial = [{'owner_id': i + 1, 'revision': 100, 'logical_tokens': prefix, 'attention_calls': 280,
                    'donor_appends': 120, 'live_bytes': release['measurement']['committed_live_bytes'],
                    'prepared_layer_blocks': 280}
                   for i, (prefix, release) in enumerate(zip(self.expected['prefix_tokens'], releases))]
        self.rows = []
        previous = initial
        for index in range(2):
            finish = copy.deepcopy(previous)
            for value in finish:
                value['revision'] += 64
                value['attention_calls'] += 32 * 35
                value['donor_appends'] += 32 * 15
                value['prepared_layer_blocks'] += 32 * 35
            self.rows.append({'index': index, 'steps': [32, 32], 'start': previous, 'finish': finish,
                              'active_ns': 7200 * 10**9, 'elapsed_ns': (index + 1) * 7205 * 10**9,
                              'output_tokens': [12, 13]})
            previous = finish
        self.report = {'schema': 'antfly.native-checkpoint-soak.report.v2',
                       'model': soak.MODELS['gemma4-e2b'][0], 'encoding': 'f16', 'model_directory': '/consumed/model',
                       'input_sha256': [0x11] * 32, 'main_gguf_sha256': [0x22] * 32, 'implementation_sha256': [0x33] * 32,
                       'model_identity': fixture['frozen']['identity'], 'load_generation': 1, **configuration,
                       'report': {'collection_complete': True, 'production_ready': False, 'release_authority': False,
                                  'load_generation': 1, 'diagnostic_only': False, 'active_ns': 14400 * 10**9,
                                  'window_elapsed_ns': 14410 * 10**9, 'initial': initial, 'final': previous,
                                  'output_tokens': [12, 13], 'release': releases, 'batches': 2, 'forward_steps': 128,
                                  'prefix_sha256_before': [[0x44] * 32, [0x55] * 32],
                                  'prefix_sha256_after': [[0x44] * 32, [0x55] * 32],
                                  'runtime_kv_bytes_before': 0, 'runtime_kv_bytes_after': 0}}
        self.resource = ResourceContract().report()
        self.resource['elapsed_seconds'] = 14500
        self.resource['limits']['timeout_seconds'] = 25000
        self.resource['rss_samples'][1]['elapsed_seconds'] = 14499

    def audit(self, formal=True, journal=None):
        if journal is None:
            journal = b''.join((json.dumps(row, sort_keys=True) + '\n').encode() for row in self.rows)
        self.report['report']['journal_sha256'] = list(hashlib.sha256(journal).digest())
        return soak.audit(self.report, journal, self.resource, self.expected, formal)

    def test_complete_formal_activity_reconciles_both_owners_and_parent_clock(self):
        result = self.audit()
        self.assertTrue(result['persistent_owner_soak_qualified'])
        self.assertEqual(result['active_seconds'], 14400)
        self.assertEqual(result['post_release_live_bytes'], [0, 0])
        self.assertFalse(result['release_authority'])
        self.assertFalse(result['production_ready'])

    def test_short_request_retains_diagnostic_scope(self):
        self.expected['duration_seconds'] = 1
        self.report['report']['diagnostic_only'] = True
        with self.assertRaisesRegex(ValueError, 'four-hour'):
            self.audit()
        self.assertFalse(self.audit(formal=False)['persistent_owner_soak_qualified'])

    def test_independent_parent_clock_excludes_idle_or_unobserved_time(self):
        self.resource['elapsed_seconds'] = 20000
        with self.assertRaisesRegex(ValueError, 'parent-clock'):
            self.audit()
        self.resource['elapsed_seconds'] = 14000
        self.resource['rss_samples'][1]['elapsed_seconds'] = 13999
        with self.assertRaisesRegex(ValueError, 'window duration'):
            self.audit()

    def test_replaced_owner_skipped_attention_and_allocations_fail(self):
        original = copy.deepcopy(self.rows)
        for field in ['owner_id', 'attention_calls', 'donor_appends', 'prepared_layer_blocks', 'live_bytes', 'logical_tokens']:
            self.rows = copy.deepcopy(original)
            self.rows[0]['finish'][0][field] += 1
            with self.subTest(field=field):
                with self.assertRaises(ValueError):
                    self.audit()

    def test_missing_reordered_or_replayed_rows_and_false_clocks_fail(self):
        original = copy.deepcopy(self.rows)
        for replacement in [original[:1], original[::-1], [original[0], original[0]]]:
            self.rows = copy.deepcopy(replacement)
            with self.assertRaises(ValueError):
                self.audit()
        self.rows = copy.deepcopy(original)
        self.rows[1]['elapsed_ns'] = self.rows[0]['elapsed_ns']
        with self.assertRaisesRegex(ValueError, 'clocks'):
            self.audit()

    def test_prefix_mutation_reload_runtime_kv_and_release_leak_fail(self):
        original = copy.deepcopy(self.report)
        mutations = [lambda x: x['report']['prefix_sha256_after'][0].__setitem__(0, 0),
                     lambda x: x['report'].update(load_generation=2),
                     lambda x: x['report'].update(runtime_kv_bytes_after=4096),
                     lambda x: x['report']['release'][0].update(post_release_live_bytes=4096),
                     lambda x: x.update(main_gguf_sha256=[0x23] * 32),
                     lambda x: x.update(implementation_sha256=[0x34] * 32)]
        for mutate in mutations:
            self.report = copy.deepcopy(original)
            mutate(self.report)
            with self.assertRaises(ValueError):
                self.audit()

    def test_duplicate_json_and_boolean_integer_are_rejected(self):
        with self.assertRaises(ValueError):
            self.audit(journal=b'{"index":0,"index":0}\n')
        self.report['report']['initial'][0]['owner_id'] = True
        with self.assertRaisesRegex(ValueError, 'owner'):
            self.audit()


if __name__ == '__main__':
    unittest.main()
