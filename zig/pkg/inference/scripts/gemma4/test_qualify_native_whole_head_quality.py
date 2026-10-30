# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Synthetic freeze and worker orchestration; never load models or fresh gold."""
import copy
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import patch

import qualify_native_whole_head_quality as collector
from test_native_whole_head_campaign import ResourceContract

SCRIPTS = Path(__file__).resolve().parent
if not (SCRIPTS / 'testdata/native_whole_head_quality_v2.json').exists():
    SCRIPTS = Path(__file__).resolve().parents[3] / 'zig/pkg/inference/scripts/gemma4'


class FrozenCollection(unittest.TestCase):
    def put(self, name, value):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value, sort_keys=True) + '\n')
        return {'path': name, 'sha256': collector.sha(path)}

    def setUp(self):
        temporary = tempfile.TemporaryDirectory(dir=Path(__file__).parent)
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.source = {'head': 'a' * 40, 'source_snapshot_sha256': 'b' * 64}
        self.binary_path = self.root / 'context-tests'
        self.binary_path.write_bytes(b'consumed synthetic executable, never run')
        self.binary = {'path': str(self.binary_path), 'sha256': collector.sha(self.binary_path)}
        self.build = {'binary_path': self.binary_path, 'binary_sha256': self.binary['sha256'],
                      'head': self.source['head'], 'source': {'synthetic': True}}
        self.configuration = json.loads((SCRIPTS / 'testdata/native_whole_head_quality_v2.json').read_bytes())['report']['frozen']
        self.configuration = {key: self.configuration[key] for key in ('observed', 'execution', 'configuration_sha256')}
        self.configuration['implementation_sha256'] = [37] * 32
        self.environment = {collector.FLAGS[key]: '1' if self.configuration['execution'][key] else '0'
                            for key in collector.EXECUTION_FIELDS}
        self.probe = {'passed': True, 'source': self.source, 'binary': self.binary,
                      'configuration': self.configuration, 'runtime_environment': self.environment}
        probe_ref = self.put('probe.json', self.probe)
        self.audit = {'passed': True, 'source': self.source,
                      'probe_observations_sha256': probe_ref['sha256'],
                      'backend_environment_sha256': bytes(self.configuration['observed']['backend_environment_sha256']).hex()}
        archive = self.root / 'preparation.zip'
        archive.write_bytes(b'synthetic preparation source archive')
        corpus = 'c' * 64
        self.preparation = {'schema': 'antfly.native-quality-preparation.audit.v2', 'passed': True,
                            'prepared_cases': 2000, 'model_quality_outputs_observed': False,
                            'model_quality_qualified': False, 'release_eligible': False,
                            'protected_prefix_rows': 128, 'protected_recent_rows': 2048,
                            'corpus_fingerprint_sha256': corpus, 'source': self.source, 'binary': self.binary,
                            'cells': [{'model': model, 'context_tokens': context, 'bucket': bucket, 'cases': 100}
                                      for model in collector.contract.MODELS for context in (8192, 32768)
                                      for bucket in sorted(collector.BUCKETS)]}
        self.preparation_seal = {'schema': 'antfly.native-quality-preparation.source-seal.v1',
                                 'preparation_only': True, 'release_eligible': False,
                                 'source': self.source, 'producer_binary': self.binary,
                                 'archive': {'path': str(archive), 'sha256': collector.sha(archive),
                                             'bytes': archive.stat().st_size}}
        self.index = {'schema': 'antfly.native-quality-input-preparation.v1', 'preparation_only': True,
                      'model_quality_outputs_observed': False, 'corpus_fingerprint_sha256': corpus,
                      'scoring_cases': self.put('scoring.json', []),
                      'preparation_audit': self.put('preparation-audit.json', self.preparation),
                      'preparation_source_seal': self.put('preparation-seal.json', self.preparation_seal),
                      'batches': []}
        model_specs = {}
        for model, geometry in collector.contract.MODELS.items():
            identity = {key: [11 + len(model)] * 32 for key in ('weights', 'tokenizer', 'template', 'layout')}
            model_specs[model] = {'loaded_identity': {key: bytes(value).hex() for key, value in identity.items()},
                                  'artifact_sha256': 'd' * 64}
            for encoding in sorted(collector.contract.ENCODINGS):
                for context in (8192, 32768):
                    for bucket in sorted(collector.BUCKETS):
                        manifest = {'schema': 'antfly.native-checkpoint-quality.input.v1', 'model': geometry[0],
                                    'native_identity': identity, 'main_gguf_sha256': 'd' * 64,
                                    'cases': [{'id': f'synthetic-{i}', 'bucket': bucket, 'prompt_count': context - 256,
                                               'request': {'encoding': encoding, 'output_tokens': 256,
                                                           'minimum_savings_ratio': 1.5}} for i in range(100)]}
                        index = len(self.index['batches'])
                        self.index['batches'].append({'model': model, 'encoding': encoding, 'context_tokens': context,
                                                      'bucket': bucket, 'cases': 100,
                                                      'input': self.put(f'manifest-{index}.json', manifest)})
        self.frozen = {'schema': 'antfly.native-whole-head.campaign-freeze.v2', 'runtime_frozen': True,
                       'release_authority': False, 'build': self.put('build.json', {'synthetic': True}),
                       'source': self.source, 'binary': self.binary, 'configuration': probe_ref,
                       'configuration_audit': self.put('probe-audit.json', self.audit),
                       'implementation_sha256': bytes(self.configuration['implementation_sha256']).hex(),
                       'runtime_environment': self.environment, 'corpus_fingerprint_sha256': corpus,
                       'input_index': self.put('index.json', self.index), 'models': model_specs, 'performance': []}
        self.job = {'source': self.source, 'binary': self.binary, 'runtime_environment': self.environment,
                    'producers': [self.put('producer.json', {'consumed': True})]}
        job = self.put('performance-job.json', self.job)
        resource = self.put('resource.json', ResourceContract().report())
        report = self.put('performance.json', {'synthetic': True})
        log = self.put('worker-log.json', {'synthetic': True})
        for model in collector.contract.MODELS:
            for encoding in sorted(collector.contract.ENCODINGS):
                for context in (8192, 32768):
                    self.frozen['performance'].append({'model': model, 'encoding': encoding, 'context_tokens': context,
                                                        'request': {'path': 'synthetic', 'sha256': 'e' * 64},
                                                        'observations': report, 'resource': resource,
                                                        'worker_log': log, 'job': job})
        self.freeze_ref = self.put('freeze.json', self.frozen)
        for target, value in [('builds.validate', self.build), ('source_snapshot', (self.source['head'], self.build['source'])),
                              ('git_provenance', self.source),
                              ('contract.performance_contract', {'performance_gate_passed': True})]:
            mock = patch.object(collector, target, return_value=value) if '.' not in target else patch(
                'qualify_native_whole_head_quality.' + target, return_value=value)
            mock.start()
            self.addCleanup(mock.stop)

    def refresh(self):
        self.frozen['input_index'] = self.put('index.json', self.index)
        self.freeze_ref = self.put('freeze.json', self.frozen)

    def validate(self):
        self.refresh()
        return collector.validate_freeze(self.root, self.root, self.freeze_ref)

    def test_complete_frozen_matrix_passes_without_reading_scoring_answers(self):
        # Invalid JSON is intentional: the preflight hashes scoring bytes only.
        (self.root / 'scoring.json').write_bytes(b'not decoded before model generation')
        self.index['scoring_cases']['sha256'] = collector.sha(self.root / 'scoring.json')
        frozen, build, index, configuration = self.validate()
        self.assertEqual(len(index['batches']), 60)
        self.assertEqual(len(frozen['performance']), 12)
        self.assertEqual(configuration, self.configuration)

    def test_runtime_source_build_and_compiled_identity_are_bound(self):
        with patch.object(collector, 'git_provenance', return_value={}):
            with self.assertRaisesRegex(ValueError, 'runtime source changed'):
                self.validate()
        with patch.object(collector.builds, 'validate', side_effect=ValueError('scoped build')):
            with self.assertRaisesRegex(ValueError, 'scoped build'):
                self.validate()
        self.frozen['implementation_sha256'] = 'f' * 64
        with self.assertRaisesRegex(ValueError, 'compiled implementation identity'):
            self.validate()

    def test_configuration_environment_and_independent_audit_are_required(self):
        self.frozen['runtime_environment'] = dict(self.environment, UNTRACKED='1')
        with self.assertRaisesRegex(ValueError, 'execution flags changed'):
            self.validate()
        self.frozen['runtime_environment'] = self.environment
        self.audit['probe_observations_sha256'] = 'f' * 64
        self.frozen['configuration_audit'] = self.put('probe-audit.json', self.audit)
        with self.assertRaisesRegex(ValueError, 'independently audited'):
            self.validate()

    def test_preparation_must_remain_unexposed_complete_and_archived(self):
        self.preparation['model_quality_outputs_observed'] = True
        self.index['preparation_audit'] = self.put('preparation-audit.json', self.preparation)
        with self.assertRaisesRegex(ValueError, 'exposed preparation'):
            self.validate()
        self.preparation['model_quality_outputs_observed'] = False
        self.preparation['cells'].pop()
        self.index['preparation_audit'] = self.put('preparation-audit.json', self.preparation)
        with self.assertRaisesRegex(ValueError, 'audited preparation cohort'):
            self.validate()
        self.preparation['cells'] = copy.deepcopy(self.preparation['cells']) + [self.preparation['cells'][0]]
        self.index['preparation_audit'] = self.put('preparation-audit.json', self.preparation)
        (self.root / 'preparation.zip').write_bytes(b'changed')
        with self.assertRaisesRegex(ValueError, 'archive changed'):
            self.validate()

    def test_model_identity_and_case_reservation_cannot_be_substituted(self):
        path = self.root / self.index['batches'][0]['input']['path']
        manifest = json.loads(path.read_bytes())
        original = copy.deepcopy(manifest)
        for mutate, reason in [(lambda x: x['native_identity']['layout'].__setitem__(0, 0), 'model identity'),
                               (lambda x: x['cases'][0].update(prompt_count=8192), 'reservation'),
                               (lambda x: x['cases'][0]['request'].update(output_tokens=64), 'reservation')]:
            manifest = copy.deepcopy(original)
            mutate(manifest)
            self.index['batches'][0]['input'] = self.put(path.name, manifest)
            with self.assertRaisesRegex(ValueError, reason):
                self.validate()

    def test_performance_requires_all_twelve_passing_cells_and_runtime_job(self):
        removed = self.frozen['performance'].pop()
        with self.assertRaisesRegex(ValueError, 'matrix is incomplete'):
            self.validate()
        self.frozen['performance'].append(removed)
        with patch.object(collector.contract, 'performance_contract', return_value={'performance_gate_passed': False}):
            with self.assertRaisesRegex(ValueError, 'prerequisite failed'):
                self.validate()
        self.job['binary'] = dict(self.binary, sha256='f' * 64)
        changed = self.put('performance-job.json', self.job)
        for entry in self.frozen['performance']:
            entry['job'] = changed
        with self.assertRaisesRegex(ValueError, 'job configuration differs'):
            self.validate()

    def args(self, name='batch-output'):
        self.refresh()
        return SimpleNamespace(root=self.root, campaign_root=self.root, freeze=Path(self.freeze_ref['path']),
                               freeze_sha256=self.freeze_ref['sha256'], batch=0, output=self.root / name)

    def simulated_guard(self, command, output, **limits):
        self.assertEqual(limits['max_rss_mib'], 8192)
        self.assertEqual(limits['max_swap_growth_mib'], 0)
        self.assertIn('--worker-spec-sha256', command)
        for index in range(100):
            (Path(output).parent / 'outputs' / f'case-{index:04}.json').write_text('{}\n')
        report = ResourceContract().report()
        Path(output).write_text(json.dumps(report))
        return report

    def test_parent_seals_complete_batch_and_binds_worker_spec(self):
        args = self.args()
        with patch.object(collector, 'model_artifact', return_value=(self.root, self.root)), \
             patch.object(collector, 'run_guarded_combined', side_effect=self.simulated_guard), \
             patch.object(collector.contract, 'audit_batch', return_value={'synthetic': True}) as audit:
            self.assertEqual(collector.collect(args), 0)
        seal = json.loads((args.output / 'seal.json').read_bytes())
        self.assertTrue(seal['collection_complete'])
        self.assertFalse(seal['release_authority'])
        self.assertEqual(len(seal['outputs']), 100)
        audit.assert_called_once()

    def test_failed_guard_does_not_audit_gold_and_remains_nonqualified(self):
        args = self.args()
        def fail(command, output, **limits):
            report = self.simulated_guard(command, output, **limits)
            report['swapout_growth_mib'] = 1
            Path(output).write_text(json.dumps(report))
            return report
        with patch.object(collector, 'model_artifact', return_value=(self.root, self.root)), \
             patch.object(collector, 'run_guarded_combined', side_effect=fail), \
             patch.object(collector.contract, 'audit_batch') as audit:
            self.assertEqual(collector.collect(args), 1)
        audit.assert_not_called()
        seal = json.loads((args.output / 'seal.json').read_bytes())
        self.assertFalse(seal['passed'])
        self.assertFalse(seal['collection_complete'])

    def test_worker_spec_tamper_prevents_model_preflight(self):
        args = self.args()
        args.output.mkdir()
        (args.output / 'spec.json').write_text('{}\n')
        args.worker_spec_sha256 = 'f' * 64
        with patch.object(collector.models, 'headroom') as headroom:
            with self.assertRaisesRegex(ValueError, 'worker spec'):
                collector.worker(args)
        headroom.assert_not_called()


if __name__ == '__main__':
    unittest.main()
