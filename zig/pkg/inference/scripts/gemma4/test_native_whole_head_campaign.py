# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Consumed synthetic observations test the batch trust boundary only."""
import base64
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

import native_whole_head_campaign as campaign

SCRIPTS = Path(__file__).resolve().parent
if not (SCRIPTS / 'testdata/native_whole_head_quality_v2.json').exists():
    SCRIPTS = Path(__file__).resolve().parents[3] / 'zig/pkg/inference/scripts/gemma4'


def encoded(value):
    return (json.dumps(value, sort_keys=True) + '\n').encode()


def ref(path, raw):
    return {'path': path, 'sha256': hashlib.sha256(raw).hexdigest()}


def owner_ids(value, delta):
    if isinstance(value, dict):
        return {key: item + delta if key in {'owner_id', 'owner_identity', 'source_owner_id', 'compacted_owner_id'}
                and type(item) is int and item > 0 else owner_ids(item, delta) for key, item in value.items()}
    if isinstance(value, list):
        return [owner_ids(item, delta) for item in value]
    return value


class BatchContract(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fixture = json.loads((SCRIPTS / 'testdata/native_whole_head_quality_v2.json').read_bytes())

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(dir=Path(__file__).parent)
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name).resolve()
        fixture = self.fixture
        self.case = copy.deepcopy(fixture['case'])
        self.case['bucket'] = 'document_qa'
        self.manifest = json.loads(base64.b64decode(fixture['inputs_base64']['manifest']))
        self.manifest['model'] = campaign.MODELS['gemma4-e2b'][0]
        cases = []
        for index in range(100):
            case = dict(self.case, id=f'consumed-synthetic-batch-{index:03}')
            cases.append(case)
        self.manifest['cases'] = cases
        raw_manifest = encoded(self.manifest)
        (self.root / 'input.json').write_bytes(raw_manifest)
        self.manifest_ref = ref('input.json', raw_manifest)
        for role in campaign.ROLES:
            raw = base64.b64decode(fixture['inputs_base64'][role])
            path = self.root / self.case[role]['path']
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(raw)
        self.outputs, self.sampling = [], []
        for index, case in enumerate(cases):
            report = owner_ids(copy.deepcopy(fixture['report']), index * 1000)
            report['case'], report['case_index'] = case, index
            report['exact_greedy']['ordinary_serving_forward_steps'] = report['exact_greedy']['forward_steps']
            report['exact_greedy']['ordinary_backend_frames'] = report['exact_greedy']['forward_steps']
            report['manifest_sha256'] = list(hashlib.sha256(raw_manifest).digest())
            raw = encoded(report)
            name = f'case-{index:04}.json'
            (self.root / name).write_bytes(raw)
            self.outputs.append(ref(name, raw))
            self.sampling.append({'id': case['id'], 'model': 'gemma4-e2b', 'context_tokens': 8192,
                                  'bucket': 'document_qa', 'blueprint_id': f'consumed-{index}',
                                  'primary_family': f'synthetic-{index}'})
        expected = fixture['expected']
        self.configuration = {'observed': expected['observed'], 'execution': expected['execution']}
        self.configuration.update({key: fixture['report']['frozen'][key] for key in
                                   ['configuration_sha256']})
        self.implementation = expected['implementation_sha256']
        self.batch = {'model': 'gemma4-e2b', 'encoding': 'f16', 'context_tokens': 8192,
                      'bucket': 'document_qa', 'cases': 100}

    def audit(self, outputs=None, batch=None, sampling=None):
        return campaign.audit_batch(self.root, self.manifest_ref, self.root,
                                    self.outputs if outputs is None else outputs,
                                    self.batch if batch is None else batch,
                                    self.configuration, self.implementation,
                                    self.sampling if sampling is None else sampling)

    def test_complete_consumed_batch_has_no_serving_authority(self):
        audit = self.audit()
        self.assertEqual(audit['private_owner_count'], 600)
        self.assertEqual(len(audit['rows']), 100)
        self.assertFalse(audit['release_authority'])
        self.assertFalse(audit['full_serving_owner_qualified'])
        self.assertFalse(audit['production_ready'])

    def test_repeated_missing_or_swapped_observations_fail(self):
        for rows in [self.outputs[:-1], [self.outputs[0]] * 100,
                     [self.outputs[1], self.outputs[0], *self.outputs[2:]]]:
            with self.subTest(count=len(rows)):
                with self.assertRaises(ValueError):
                    self.audit(outputs=rows)

    def test_source_case_membership_and_context_cannot_be_substituted(self):
        for key, replacement in [('model', 'gemma4-e4b'), ('context_tokens', 32768),
                                 ('encoding', 'rotated4'), ('bucket', 'code'), ('cases', True)]:
            with self.subTest(field=key):
                with self.assertRaises(ValueError):
                    self.audit(batch=dict(self.batch, **{key: replacement}))
        sampling = copy.deepcopy(self.sampling)
        sampling[0]['id'] = 'unknown-case'
        with self.assertRaises(ValueError):
            self.audit(sampling=sampling)

    def test_changed_input_and_observation_bytes_are_rejected(self):
        for path in ['input.json', self.case['prompt_tokens']['path'], 'case-0000.json']:
            target = self.root / path
            raw = target.read_bytes()
            target.write_bytes(raw + b' ')
            with self.assertRaises(ValueError):
                self.audit()
            target.write_bytes(raw)

    def test_strict_json_and_path_contract(self):
        for raw in [b'{"x":1,"x":2}', b'{"x":NaN}', b'{"x":1e999}']:
            path = self.root / 'strict.json'
            path.write_bytes(raw)
            with self.assertRaises(ValueError):
                campaign.json_ref(self.root, ref(path.name, raw), 'strict')
        for path in ['../input.json', '/input.json', './input.json', 'x//y', 'x/../input.json']:
            with self.assertRaises(ValueError):
                campaign.read_ref(self.root, {'path': path, 'sha256': '1' * 64}, 'unsafe')
        (self.root / 'alias.json').symlink_to(self.root / 'input.json')
        with self.assertRaises(ValueError):
            campaign.read_ref(self.root, dict(self.manifest_ref, path='alias.json'), 'symlink')

    def test_replayed_owners_and_old_engine_reports_are_rejected(self):
        target = self.root / 'case-0001.json'
        report = json.loads(target.read_bytes())
        report = owner_ids(report, -1000)
        raw = encoded(report)
        target.write_bytes(raw)
        outputs = [*self.outputs]
        outputs[1] = ref(target.name, raw)
        with self.assertRaisesRegex(ValueError, 'owner identity replayed'):
            self.audit(outputs=outputs)
        report['schema'] = 'antfly.independent-block-quality.case.v1'
        raw = encoded(report)
        target.write_bytes(raw)
        outputs[1] = ref(target.name, raw)
        with self.assertRaisesRegex(ValueError, 'observation scope'):
            self.audit(outputs=outputs)

    def test_incomplete_matrix_cannot_pass_quality(self):
        audit = self.audit()
        with self.assertRaises(ValueError):
            campaign.evaluate_model_encoding([(self.batch, audit)], 'gemma4-e2b', 'f16', 'consumed')


class ResourceContract(unittest.TestCase):
    def report(self):
        return {'schema': 'antfly.attention-matching.combined-resource-guard.v1',
                'rss_scope': 'campaign_parent_plus_active_child_pgid', 'pass': True,
                'returncode': 0, 'violation': None, 'campaign_parent_pid': 123, 'child_pgid': 456,
                'limits': {'timeout_seconds': 900, 'max_rss_mib': 8192, 'min_free_percent': 15,
                           'max_swap_growth_mib': 0, 'min_disk_free_mib': 4096},
                'elapsed_seconds': 3, 'min_free_percent': 50, 'min_disk_free_mib': 10000,
                'swapout_growth_mib': 0, 'peak_combined_rss_mib': 1030,
                'peak_parent_rss_mib': 30, 'peak_child_group_rss_mib': 1000,
                'remaining_child_group_live_processes': 0, 'remaining_child_group_rss_mib': 0,
                'samples': 2, 'rss_samples': [
                    {'elapsed_seconds': 0, 'parent_rss_mib': 20, 'child_group_rss_mib': 0, 'combined_rss_mib': 20},
                    {'elapsed_seconds': 2, 'parent_rss_mib': 30, 'child_group_rss_mib': 1000, 'combined_rss_mib': 1030}]}

    def test_independent_parent_and_group_measurements_reconcile(self):
        result = campaign.combined_resource_contract(self.report())
        self.assertEqual(result['peak_combined_rss_mib'], 1030)
        self.assertEqual(result['swapout_growth_mib'], 0)

    def test_pass_bit_cannot_hide_resource_failure(self):
        mutations = [lambda x: x['limits'].update(max_rss_mib=16384),
                     lambda x: x['limits'].update(max_swap_growth_mib=1),
                     lambda x: x.update(swapout_growth_mib=1),
                     lambda x: x.update(remaining_child_group_live_processes=1),
                     lambda x: x.update(remaining_child_group_rss_mib=10),
                     lambda x: x.update(returncode=False),
                     lambda x: x.update(peak_combined_rss_mib=1000),
                     lambda x: x['rss_samples'][1].update(combined_rss_mib=999),
                     lambda x: x['rss_samples'][1].update(elapsed_seconds=-1),
                     lambda x: x['rss_samples'][0].update(parent_rss_mib=float('nan')),
                     lambda x: x.update(samples=3)]
        for mutate in mutations:
            row = self.report()
            mutate(row)
            with self.subTest(receipt=row):
                with self.assertRaises(ValueError):
                    campaign.combined_resource_contract(row)


class PerformanceContract(unittest.TestCase):
    def fixture(self):
        import qualify_metal_native_checkpoint_performance as performance
        from test_metal_checkpoint_performance_observations import PendingSampledTokenLedger
        model = campaign.MODELS['gemma4-e4b'][0]
        loaded = {key: str(index + 1) * 64 for index, key in enumerate(('weights', 'tokenizer', 'template', 'layout'))}
        binary = {'path': '/consumed/synthetic/context-tests', 'sha256': 'a' * 64}
        source = {'head': 'b' * 40, 'source_snapshot_sha256': 'c' * 64}
        request = {'path': '/consumed/synthetic/request.json', 'sha256': 'd' * 64}
        expected = {'model': 'gemma4-e4b', 'encoding': 'f16', 'context_tokens': 8192,
                    'source': source, 'binary': binary, 'request': request, 'loaded_identity': loaded}
        memory = {'scope': 'unified_process_group', 'max_combined_rss_bytes': 8 * 1024**3,
                  'samples': 2, 'errors': [], 'swapout_growth_bytes': 0,
                  'minimum_free_memory_percent': 50, 'minimum_disk_free_mib': 10000,
                  'peak_group_rss_bytes': 1024 * 1024}
        report = {'schema': performance.SCHEMA, 'backend': 'metal', 'server_kind': 'metal-test',
                  'qualification_profile': 'metal_test_native_checkpoint_v1', 'execution_mode': 'eager',
                  'production_ready': False, 'source': source, 'binary_sha256': binary['sha256'],
                  'request': request, 'encoding': 'f16', 'declared_context_tokens': 8192,
                  'warmups': 5, 'pairs_requested': 30, 'qualification_sized': True, 'preparation_reused': False,
                  'release_cell': {'declared_context_tokens': 8192, 'reserved_output_tokens': 64,
                                   'rendered_prompt_tokens': 8128, 'context_cell_matched': True,
                                   'output_reservation_filled': True},
                  'exact_control': {'encoding': 'f16', 'logical_tokens': 8128, 'validated_heads': 0,
                                    'global_savings_ratio': 1},
                  'physical_ledger': {'encoding': 'f16', 'logical_tokens': 8128, 'outcome': 'compacted',
                                      'validated_heads': 8, 'global_savings_ratio': 2},
                  'worker': {'memory': memory, 'graceful_stop': True, 'remaining_group_members': [], 'stopped': True,
                             'command': [binary['path'], '--test-filter', 'context Metal native checkpoint qualification server'],
                             'ready': {'model': model, 'backend': 'metal', 'metal_enabled': True,
                                       'cuda_enabled': False, 'loaded_identity': loaded}, 'performance_observations': []},
                  'observations': []}
        schedule = list(performance.paired_schedule(30))
        modes = ['full_replay'] + ['ordinary', 'exact', 'compacted'] * 5 + [mode for order in schedule for mode in order]
        log = bytearray(b'Consumed synthetic parser fixture. No model inference or performance authority.\n')
        for index, mode in enumerate(modes):
            ready, committed, released = [copy.deepcopy(x) for x in PendingSampledTokenLedger().rows()]
            bound = mode in ('exact', 'compacted')
            route = 'checkpoint_resume' if bound else 'exact'
            context_bytes = 100 if mode == 'compacted' else 200
            for row in (ready, committed, released):
                row.update(owner_id=index + 1, request=index + 1, route=route)
            for row in (ready, committed):
                row.update(tracked_combined_live_bytes=105 + context_bytes, tracked_combined_peak_bytes=1000)
            released.update(measured_committed_bytes=context_bytes, committed_live_bytes=105 + context_bytes,
                            final_live_bytes=105, initial_peak_bytes=105, committed_peak_bytes=1000, final_peak_bytes=1000)
            if not bound:
                committed['binding'] = False
                committed['binding_representation'] = 'none'
                for key in list(committed):
                    if key.startswith('binding_') and key != 'binding_representation':
                        committed[key] = 0
                committed.update(attention_calls=0, donor_appends=0)
            rows = [ready, committed, released] if bound else [committed, released]
            ledger = performance.ledger_contract(rows, route, require_binding=bound,
                                                 expected_representation='f16' if bound else None, expected_model=model)
            timing = {'prompt_tokens': 8128, 'completion_tokens': 64, 'cached_prompt_tokens': 8128 if bound else 0,
                      'finish_reason': 'length', 'content_sha256': 'e' * 64, 'ttft_ms': 10., 'decode_ms': 100.,
                      'e2e_ms': 110., 'wire_send_ms': 1. if bound else None,
                      'post_upload_to_first_token_ms': 9. if bound else None}
            attempt = {'attempt_index': index, 'mode': mode, 'route': route, 'validated': True,
                       'observation': timing, 'owner_ledger': ledger, 'ledger_rows': rows, 'log_offset': len(log)}
            for row in rows:
                log.extend(b'metal_checkpoint_qualification ' + encoded(row))
            report['worker']['performance_observations'].append(attempt)
            if index >= 16:
                pair_index = (index - 16) // 3
                if len(report['observations']) <= pair_index:
                    report['observations'].append({'index': pair_index, 'order': list(schedule[pair_index])})
                report['observations'][pair_index][mode] = {**timing, 'owner_ledger': ledger}
        return report, expected, bytes(log)

    def test_complete_synthetic_cell_recomputes_fixed_gates_without_release_authority(self):
        report, expected, log = self.fixture()
        result = campaign.performance_contract(report, expected, log)
        self.assertTrue(result['performance_gate_passed'])
        self.assertEqual(result['full_public_owners_released'], 106)
        self.assertEqual(result['confidence']['committed_context_savings']['median'], .5)
        self.assertFalse(result['production_ready'])
        self.assertFalse(result['release_authority'])

    def test_claimed_pass_cannot_replace_raw_data_or_full_coverage(self):
        mutations = [lambda x: x.update(warmups=1), lambda x: x.update(preparation_reused=True),
                     lambda x: x.update(pairs_requested=6), lambda x: x.update(binary_sha256='f' * 64),
                     lambda x: x['observations'][0].update(order=['exact', 'compacted', 'ordinary']),
                     lambda x: x['worker']['performance_observations'][20].update(validated=False),
                     lambda x: x['observations'][0]['compacted'].update(decode_ms=1),
                     lambda x: x['worker']['performance_observations'][20]['ledger_rows'][-1].update(post_release_live_bytes=1),
                     lambda x: x['worker']['performance_observations'][20]['observation'].update(completion_tokens=63)]
        for mutate in mutations:
            report, expected, log = self.fixture()
            mutate(report)
            with self.subTest(mutation=mutate):
                with self.assertRaises((ValueError, AssertionError)):
                    campaign.performance_contract(report, expected, log)
        report, expected, log = self.fixture()
        with self.assertRaisesRegex(ValueError, 'worker log'):
            campaign.performance_contract(report, expected, log.replace(b'owner_id', b'owner_ix', 1))

    def test_speed_and_savings_thresholds_are_recomputed(self):
        report, expected, log = self.fixture()
        for attempt in report['worker']['performance_observations'][16:]:
            if attempt['mode'] == 'compacted':
                attempt['observation']['decode_ms'] = 200.
                attempt['observation']['e2e_ms'] = 210.
        for pair in report['observations']:
            pair['compacted'].update(decode_ms=200., e2e_ms=210.)
        result = campaign.performance_contract(report, expected, log)
        self.assertFalse(result['performance_gate_passed'])
        self.assertEqual(result['confidence']['ordinary_decode_ratio']['lower_95'], .5)


if __name__ == '__main__':
    unittest.main()
