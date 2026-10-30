# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""CPU-only adversarial checks of matched performance receipt validation."""
import copy
import json
from pathlib import Path
import tempfile
import unittest

import compare_performance as gate


class ComparisonTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.manifest, cls.manifest_receipt = gate.read(gate.http.MANIFEST)
        cls.refs = gate.references(cls.manifest)

    def setUp(self):
        root = gate.http.REPO / '.specialty-artifacts/performance-unit-fixtures'
        root.mkdir(parents=True, exist_ok=True)
        self.directory = tempfile.TemporaryDirectory(dir=root)
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.log = self.root / 'server.log'
        self.log.write_text(''.join('info: gliner_specialty_cuda_policy profile=' + p +
            ' weights=f32 attention=f32 diagnostic_overrides=ignored\n' for p in gate.http.PROFILES))
        self.hardware = {'nvidia_smi': 'NVIDIA GB10, GPU-unit-fixture, 12.1, 580.178.04, [N/A]',
                         'machine': 'aarch64', 'platform': 'unit-test-platform', 'python': '3.12.3'}
        self.http = self.http_receipt()
        self.pii = self.torch_receipt('pii')
        self.guard = self.torch_receipt('gliguard')

    def row(self, profile, case_id, batch=1, base=2):
        samples = [base, base + 1] * 25
        median = base + .5
        tokens = len(self.refs[profile, case_id]['input_ids'])
        return {'profile': profile, 'id': case_id, 'request_items': batch,
                'physical_batch_items': min(8, batch) if profile == 'gliguard' else 1,
                'prepared_tokens_per_item': tokens, 'warmups': 10, 'iterations': 50,
                'latency_ms': {'median': median, 'p95': base + 1, 'samples': samples},
                'items_per_second': batch * 1000 / median,
                'prepared_tokens_per_second': batch * tokens * 1000 / median}

    def http_receipt(self):
        instances = {'object': 'list', 'data': [{'model': p, 'id': p + '-fixture', 'backend': 'cuda', 'state': 'loaded',
                              'tasks': ['extractor'], 'startup_managed': True} for p in gate.http.PROFILES]}
        result = {'format_version': 1, 'status': 'passed', 'artifact_pins': copy.deepcopy(self.manifest),
            'manifest_sha256': self.manifest_receipt['sha256'],
            'harness_sha256': gate.http.digest_file(gate.HERE / 'qualify_http.py'), 'timing_scope': gate.HTTP_SCOPE,
            'hardware': copy.deepcopy(self.hardware), 'server_binary': {'path': '/fixture-server', 'sha256': 'a' * 64},
            'server_returncode': -15, 'server_log': str(self.log), 'server_log_sha256': gate.http.digest_file(self.log),
            'validation': [], 'timings': [self.row('pii', 'language_en'), self.row('gliguard', 'prompt_safety')]}
        for phase in ('before', 'after'):
            result['backend_instances_' + phase] = copy.deepcopy(instances)
            result['running_binary_' + phase] = {'sha256': 'a' * 64}
            result['cuda_policy_' + phase] = gate.http.parse_cuda_policy(self.log.read_text())
        for (profile, case_id), case in self.refs.items():
            result['validation'].append({'profile': profile, 'id': case_id, 'response': {'schema_version': 2,
                'data': [{'id': case_id, 'offset_unit': 'unicode_codepoints',
                          'entities': copy.deepcopy(case['expected_entities']),
                          'classifications': copy.deepcopy(case['expected_classifications'])}]}})
        return result

    def torch_receipt(self, profile):
        return {'format_version': 1, 'profile': profile, 'model': copy.deepcopy(self.manifest['models'][profile]),
            'upstream': copy.deepcopy(self.manifest['upstream']), 'timing_scope': gate.TORCH_SCOPE,
            'benchmark_script_sha256': gate.http.digest_file(gate.HERE / 'benchmark_oracle.py'),
            'runtime': {'device': 'cuda', 'dtype': 'float32', 'tf32': False, 'cudnn_allow_tf32': False,
                'float32_matmul_precision': 'highest', 'capture_hooks': False, 'matches_frozen_oracle_runtime': True,
                'packages': copy.deepcopy(self.manifest['runtime']['packages']), 'python': '3.12.3 fixture',
                'cuda_device': 'NVIDIA GB10', 'platform': self.hardware['platform'], 'threads': 4, 'interop_threads': 20},
            'cases': [self.row(profile, 'language_en' if profile == 'pii' else 'prompt_safety', base=5)]}

    def write(self, name, data):
        path = self.root / name
        path.write_text(json.dumps(data))
        return path

    def compare(self, http_reports=None, torch_reports=None, **kwargs):
        hp = [self.write('http' + str(i) + '.json', r) for i, r in enumerate(http_reports or [self.http])]
        pt = [self.write('torch' + str(i) + '.json', r) for i, r in enumerate(torch_reports or [self.pii, self.guard])]
        return gate.compare(hp, pt, **kwargs)

    def test_pass_preserves_samples_receipt_and_binary_hashes_and_scope(self):
        report = self.compare()
        self.assertEqual('passed', report['status'])
        self.assertEqual(2, report['matched_cases'])
        self.assertEqual('a' * 64, report['server_binary_sha256'])
        self.assertEqual(gate.http.digest_file(Path(gate.__file__)), report['comparison_script_sha256'])
        self.assertIn('conservative HTTP-versus-in-process', report['timing_boundary'])
        for row in report['cases']:
            self.assertAlmostEqual(2.2, row['speedup'])
            self.assertEqual(50, len(row['antfly']['latency_ms']['samples']))
            self.assertEqual(64, len(row['pytorch']['receipt_sha256']))

    def test_repeated_receipts_join_singletons_and_guard_batch8(self):
        batch_http = copy.deepcopy(self.http)
        batch_http['timings'] = [self.row('gliguard', 'prompt_safety', batch=8)]
        batch_torch = copy.deepcopy(self.guard)
        batch_torch['cases'] = [self.row('gliguard', 'prompt_safety', batch=8, base=5)]
        report = self.compare([self.http, batch_http], [self.pii, self.guard, batch_torch])
        self.assertEqual('passed', report['status'])
        self.assertEqual(3, report['matched_cases'])

    def test_slower_or_tied_case_fails_without_hiding_other_cases(self):
        for base in (5, 8):
            with self.subTest(base=base):
                self.http['timings'][0] = self.row('pii', 'language_en', base=base)
                report = self.compare()
                self.assertEqual('failed', report['status'])
                self.assertEqual(2, len(report['cases']))
                self.assertIn('pii/language_en/b1', report['error'])
                self.assertEqual(1, sum(not row['passed'] for row in report['cases']))

    def test_duplicate_and_unmatched_rows_rejected_in_both_directions(self):
        for kind in ('http', 'pytorch'):
            with self.subTest(kind=kind):
                h, p = copy.deepcopy(self.http), copy.deepcopy(self.pii)
                (h['timings'] if kind == 'http' else p['cases']).append(self.row('pii', 'language_en'))
                with self.assertRaisesRegex(ValueError, 'duplicate timing'):
                    self.compare([h], [p, self.guard])
        for kind in ('http', 'pytorch'):
            h, p = copy.deepcopy(self.http), copy.deepcopy(self.pii)
            (h['timings'] if kind == 'http' else p['cases']).append(self.row('pii', 'language_fr'))
            with self.assertRaisesRegex(ValueError, 'unmatched timing'):
                self.compare([h], [p, self.guard])

    def test_raw_sample_counts_values_and_derived_statistics_enforced(self):
        mutations = [lambda row: row['latency_ms']['samples'].pop(),
                     lambda row: row['latency_ms']['samples'].__setitem__(0, float('nan')),
                     lambda row: row['latency_ms']['samples'].__setitem__(0, float('inf')),
                     lambda row: row['latency_ms']['samples'].__setitem__(0, 0),
                     lambda row: row['latency_ms'].__setitem__('median', 1),
                     lambda row: row['latency_ms'].__setitem__('p95', 1),
                     lambda row: row.__setitem__('items_per_second', 1),
                     lambda row: row.__setitem__('iterations', True),
                     lambda row: row.__setitem__('warmups', 9),
                     lambda row: row.__setitem__('physical_batch_items', 2),
                     lambda row: row.__setitem__('prepared_tokens_per_item', 512)]
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                h = copy.deepcopy(self.http)
                mutation(h['timings'][0])
                with self.assertRaises(ValueError):
                    self.compare([h])

    def test_runtime_precision_and_pins_are_verified_without_trusting_match_boolean(self):
        mutations = [lambda p: p['runtime'].__setitem__('tf32', True),
                     lambda p: p['runtime'].__setitem__('float32_matmul_precision', 'high'),
                     lambda p: p['runtime'].__setitem__('dtype', 'float16'),
                     lambda p: p['runtime'].__setitem__('device', 'cpu'),
                     lambda p: p['runtime']['packages'].__setitem__('torch', '2.10.0'),
                     lambda p: p['upstream'].__setitem__('commit', '0' * 40),
                     lambda p: p['model']['files']['model.safetensors'].__setitem__('sha256', '0' * 64),
                     lambda p: p.__setitem__('benchmark_script_sha256', '0' * 64),
                     lambda p: p['runtime'].__setitem__('platform', 'other-host')]
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                p = copy.deepcopy(self.pii)
                mutation(p)
                with self.assertRaises(ValueError):
                    self.compare(torch_reports=[p, self.guard])

    def test_changed_http_identity_policy_and_incomplete_or_wrong_outputs_rejected(self):
        mutations = [lambda h: h['running_binary_after'].__setitem__('sha256', 'b' * 64),
                     lambda h: h.__setitem__('manifest_sha256', '0' * 64),
                     lambda h: h.__setitem__('harness_sha256', '0' * 64),
                     lambda h: h['cuda_policy_after']['pii'].__setitem__('weights', 'f16'),
                     lambda h: h.__setitem__('server_log_sha256', '0' * 64),
                     lambda h: h.__setitem__('server_returncode', -9),
                     lambda h: h['validation'].pop(),
                     lambda h: h['validation'][0]['response']['data'][0]['entities'][0].__setitem__('text', 'changed')]
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                h = copy.deepcopy(self.http)
                mutation(h)
                with self.assertRaises(ValueError):
                    self.compare([h])

    def test_mixed_hardware_binaries_or_runtime_settings_rejected(self):
        for changed in ('hardware', 'binary'):
            h = copy.deepcopy(self.http)
            if changed == 'hardware':
                h['hardware']['nvidia_smi'] = h['hardware']['nvidia_smi'].replace('GPU-unit', 'GPU-other')
            else:
                h['server_binary']['sha256'] = 'b' * 64
                for phase in ('before', 'after'):
                    h['running_binary_' + phase]['sha256'] = 'b' * 64
            with self.assertRaisesRegex(ValueError, 'mixed'):
                self.compare([self.http, h])
        self.guard['runtime']['threads'] = 8
        with self.assertRaisesRegex(ValueError, 'mixed runtime'):
            self.compare()

    def test_cli_writes_failure_json_and_legacy_minimums_are_explicit(self):
        for row in self.http['timings'] + self.pii['cases'] + self.guard['cases']:
            row['warmups'] = 2
            row['iterations'] = 5
            base = row['latency_ms']['samples'][0]
            row['latency_ms'] = {'median': base, 'p95': base, 'samples': [base] * 5}
            row['items_per_second'] = 1000 / base
            row['prepared_tokens_per_second'] = row['prepared_tokens_per_item'] * 1000 / base
        hp, pp, gp = [self.write(name, data) for name, data in [('http.json', self.http), ('pii.json', self.pii), ('guard.json', self.guard)]]
        output = self.root / 'result.json'
        args = ['--http', str(hp), '--pytorch', str(pp), '--pytorch', str(gp), '--output', str(output)]
        self.assertEqual(1, gate.main(args))
        self.assertEqual('failed', json.loads(output.read_text())['status'])
        self.assertEqual(0, gate.main(args + ['--minimum-warmups', '2', '--minimum-iterations', '5']))
        self.assertEqual(2, json.loads(output.read_text())['minimum_warmups'])
        self.http['timings'][0] = copy.deepcopy(self.pii['cases'][0])
        self.write('http.json', self.http)
        self.assertEqual(1, gate.main(args + ['--minimum-warmups', '2', '--minimum-iterations', '5']))
        report = json.loads(output.read_text())
        self.assertIn('slower or tied', report['error'])
        self.assertEqual(2, len(report['cases']))


if __name__ == '__main__':
    unittest.main()
