# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Hermetic validation of final receipt assembly; no model or GPU execution."""
import copy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

import capture_oracle
import summarize_qualification as summary

SERVER = 'a' * 64
TEST = 'b' * 64
SOURCE = {'git_commit': 'c' * 40, 'source_sha256': summary.FROZEN_SOURCE, 'files': {},
          'binding_scope': 'observed inputs', 'captured_unix_seconds': 1}
POLICY_LOG = ''.join('info: gliner_specialty_cuda_policy profile=' + p +
    ' weights=f32 attention=f32 diagnostic_overrides=ignored\n' for p in summary.http.PROFILES)
POLICY = summary.http.parse_cuda_policy(POLICY_LOG)


def references():
    return {p: {'cases': [{'id': c['id'], 'text': 'x', 'input_ids': [1],
        'expected_entities': [{'label': 'person', 'text': 'x', 'start': 0, 'end': 1, 'score': .9}] if p == 'pii' else [],
        'expected_classifications': [{'name': 'safety', 'label': 'safe', 'score': .9}] if p == 'gliguard' else []}
        for c in capture_oracle.cases(p)]} for p in summary.http.PROFILES}


def response(case, model='example'):
    return {'object': 'extraction', 'model': model, 'schema_version': 2, 'data': [{'id': case['id'],
        'offset_unit': 'unicode_codepoints', 'entities': copy.deepcopy(case['expected_entities']),
        'classifications': copy.deepcopy(case['expected_classifications'])}]}


class FakeEvidence:
    def log(self, row, *args, **kwargs):
        return row.get('_text', POLICY_LOG)
    def read(self, *_args, **_kwargs):
        return b'log'


def common_server():
    return {'format_version': 1, 'status': 'passed', 'server_binary': {'sha256': SERVER},
        'running_binary_before': {'sha256': SERVER, 'pid': 123, 'process_group': 123},
        'running_binary_after': {'sha256': SERVER, 'pid': 123, 'process_group': 123},
        'server_returncode': -15, 'backend_environment': {'ANTFLY_INFERENCE_REQUIRED_BACKEND': 'cuda'}}


def http_receipt(refs, batch=1):
    report = common_server()
    report.update(confidence_abs_tolerance=.0005, requested_attention_diagnostics={'attention_mode': 'f32', 'generated_tc_auto': '0'},
        cuda_policy_before=copy.deepcopy(POLICY), cuda_policy_after=copy.deepcopy(POLICY), timing_scope='HTTP', validation=[], timings=[])
    report['backend_environment'].update(ANTFLY_INFERENCE_CUDA_DEBERTA_ATTENTION_MODE='f32', ANTFLY_CUDA_DEBERTA_GENERATED_TC_AUTO='0')
    instances = {'object': 'list', 'data': [{'model': p, 'id': p + '-id', 'backend': 'cuda', 'state': 'loaded',
        'tasks': ['extractor'], 'startup_managed': True} for p in summary.http.PROFILES]}
    report['backend_instances_before'] = copy.deepcopy(instances)
    report['backend_instances_after'] = copy.deepcopy(instances)
    for p, ref in refs.items():
        for c in ref['cases']:
            report['validation'].append({'profile': p, 'id': c['id'], 'response': response(c)})
            report['timings'].append({'profile': p, 'id': c['id'], 'request_items': batch,
                'physical_batch_items': 1 if p == 'pii' else min(8, batch), 'prepared_tokens_per_item': 1,
                'warmups': 2, 'iterations': 5, 'latency_ms': {'median': 1, 'samples': [1] * 5},
                'items_per_second': batch * 1000, 'prepared_tokens_per_second': batch * 1000})
    return report


def sanitizer_receipt(refs, tools=('memcheck', 'initcheck'), pattern=None):
    report = {'format_version': 1, 'status': 'passed', 'test_binary': {'path': '/test', 'sha256': TEST},
        'candidate_server': {'sha256': SERVER}, 'source': {k: v for k, v in SOURCE.items() if k != 'captured_unix_seconds'},
        'tools': list(tools), 'requested_suites': list(summary.sanitizers.BASE_SUITES), 'case_selection': None,
        'sanitizer_kernel_filter': pattern, 'full_head_case_coverage': True, 'all_kernel_coverage': pattern is None,
        'sanitizer_kernel_scope': {'regex': pattern, 'all_kernel_coverage_claimed': pattern is None,
                                 'vendor_library_scope': 'unfiltered' if pattern is None else 'vendor excluded'},
        'sanitizer': {'version': 'fixture'}, 'lanes': []}
    for tool in tools:
        for suite in summary.sanitizers.BASE_SUITES:
            spec = summary.sanitizers.SUITES[suite]
            ids = [c['id'] for c in refs[spec['profile']]['cases']] if 'profile' in spec else []
            text = '========= COMPUTE-SANITIZER\n1/316 example.test.' + spec['filter'] + '...\n'
            if ids:
                text += 'specialty_head_receipt: ' + json.dumps({'profile': spec['profile'], 'backend': 'cuda',
                    'case_ids': ids, 'cases': len(ids), 'fixture_cases': len(ids)}) + '\n'
            text += 'OK\n1 selected; 1 passed; 0 skipped.\n'
            text += ('========= LEAK SUMMARY: 0 bytes leaked in 0 allocations\n' if tool == 'memcheck' else '')
            text += ('========= RACECHECK SUMMARY: 0 hazards displayed (0 errors, 0 warnings)\n' if tool == 'racecheck'
                     else '========= ERROR SUMMARY: 0 errors\n')
            report['lanes'].append({'tool': tool, 'suite': suite, 'pass': True, 'failures': [], 'returncode': 0,
                'timed_out': False, 'elapsed_seconds': .1, 'oracle_case_ids': ids, '_text': text,
                'command': summary.sanitizers.sanitizer_command(Path('/sanitizer'), tool, Path('/test'), spec['filter'], pattern)})
    return report


def owned_memory(profile):
    return {'profile': profile, 'backend': 'cuda', 'cases': 12 if profile == 'pii' else 11, 'kernel_launches': 1,
        'gru_calls': 2 if profile == 'pii' else 0, 'gru_no_residual_calls': 2 if profile == 'pii' else 0,
        'initial_device_memory': {'live': 100, 'peak': 100}, 'final_device_memory': {'live': 130, 'peak': 150},
        'max_reserved_scratch_bytes': 100, 'reserved_physical_peak_bytes': 200,
        'idle_admission': {'host_scratch_bytes': 0, 'backend_scratch_bytes': 0}}


def manifest_fixture():
    return {'models': {p: {'repository': 'owner/' + p, 'files': {}} for p in summary.http.PROFILES}}


def drift_receipt(refs, manifest):
    variants = (*summary.drift.CONTROLS, *summary.drift.NEGATIVES, 'managed_valid')
    report = {'format_version': 1, 'passed': True, 'scope': 'full_matrix', 'selected_variants': list(variants),
        'expected_load_extraction_checks': 72, 'server_binary': {'sha256': SERVER}, 'cpu_only': True,
        'server_cleanup_complete': True, 'probe_models_cleaned_up': True, 'server_exit_code': -15,
        'original_artifacts_after': {p: {} for p in summary.http.PROFILES}, 'cases': []}
    for p in summary.http.PROFILES:
        for variant in variants:
            model = variant + '_' + p
            if variant in summary.drift.CONTROLS:
                load = {'status': 200, 'body': {'backend': 'native', 'state': 'loaded', 'tasks': ['extractor']}}
                extraction = {'status': 200, 'body': response(refs[p]['cases'][0], model)}
            else:
                load = {'status': 400, 'body': {'error': 'INVALID_MODEL', 'message': summary.drift.EXPECTED_REJECTION_MESSAGE}}
                code = 'UNSUPPORTED_EXTRACTION_FEATURE' if variant == 'raw_config_drift' else 'INVALID_MODEL'
                extraction = {'status': 400, 'body': {'error': code, 'stage': 'model'}}
            report['cases'].append({'profile': p, 'variant': variant, 'request': {'model': model},
                'response': load, 'cold_extraction': extraction,
                'unload': {'status': 200, 'body': {'outcome': 'unloaded'}},
                'extraction_unload': {'status': 200, 'body': {'outcome': 'unloaded'}},
                'instances_after': {'status': 200, 'body': {'object': 'list', 'data': []}}})
    return report


def soak_receipt(refs, manifest):
    report = common_server()
    report.update(duration_seconds=300, traffic_duration_seconds=301, concurrency=8,
        cuda_policy_after_load=copy.deepcopy(POLICY), cuda_policy_after_reload=copy.deepcopy(POLICY),
        validation=[], versionless_validation=[], ragged_validation=[], chunked_and_maximum=[], rejections=[],
        loaded_instances={}, lifecycle=[], traffic=[], memory_samples=[])
    metrics = {'antfly_inference_admission_' + name: value for name, value in
        [('host_scratch_bytes', 0), ('backend_scratch_bytes', 0), ('host_weight_bytes', 123), ('backend_weight_bytes', 456)]}
    report['idle_admission_before'] = metrics
    report['idle_admission_after'] = copy.deepcopy(metrics)
    catalog = {}
    units = ('utf8_bytes', 'unicode_codepoints', 'utf16_codeunits')
    for p in summary.http.PROFILES:
        first = refs[p]['cases'][0]['id']
        for c in refs[p]['cases']:
            for unit in units:
                report['validation'].append({'profile': p, 'case': c['id'], 'unit': unit, 'max_confidence_abs_error': 0})
            report['versionless_validation'].append({'profile': p, 'case': c['id'], 'max_confidence_abs_error': 0})
        report['ragged_validation'].append({'profile': p, 'items': len(refs[p]['cases']),
            'ordered_case_ids': [c['id'] for c in refs[p]['cases']], 'max_confidence_abs_error': 0})
        for count in (8, 9, 128):
            report['chunked_and_maximum'].append({'profile': p, 'count': count, 'max_confidence_abs_error': 0})
        for probe, status in [('oversized', 413), ('too_many_items', 413), ('unsupported_task', 400), ('long_document', 400)]:
            report['rejections'].append({'profile': p, 'probe': probe, 'status': status, 'response': {'error': 'EXPECTED'}})
        catalog[p] = {'inputs': ['text'], 'capabilities': ['named_entity_recognition', 'extraction'] if p == 'pii'
            else ['classification', 'zero_shot', 'multi_label'], 'qualified_limits': {'specialty': {'sequence_tokens': 512,
            'physical_batch_items': 1 if p == 'pii' else 8}}, 'inference_capabilities': {'task': 'extract',
            'input_modalities': ['text'], 'batch': {'max_items': 128}}}
        report['loaded_instances'][p] = {'backend': 'cuda', 'model': manifest['models'][p]['repository'], 'id': p + '-old'}
        report['lifecycle'].append({'profile': p, 'unload': {'outcome': 'unloaded'}, 'repeat': {'outcome': 'already_unloaded'},
            'reloaded': {'backend': 'cuda', 'state': 'loaded', 'model': manifest['models'][p]['repository'], 'id': p + '-new'},
            'recovery': {'profile': p, 'case': first, 'max_confidence_abs_error': 0}})
    report['catalog_before_load'] = catalog
    report['catalog_after_reload'] = copy.deepcopy(catalog)
    for i in range(32):
        p = summary.http.PROFILES[i % 2]
        report['traffic'].append({'profile': p, 'case': refs[p]['cases'][i % len(refs[p]['cases'])]['id'],
            'unit': units[i % 3], 'count': (1, 2, 8, 9)[i % 4], 'max_confidence_abs_error': 0})
    report['summary'] = {'requests': 32, 'items': sum(r['count'] for r in report['traffic'])}
    report['memory_samples'] = [{'elapsed_seconds': i * 30, 'pss_bytes': 100000000} for i in range(10)]
    report['host_pss_gate'] = {'maximum_growth_bytes': 64 << 20, 'late_window_median_growth_bytes': 0}
    return report


def baseline_metadata():
    return {'schema_version': 1, 'exception_kind': 'preexisting_native_q8_out_of_scope',
        'source_snapshot': {'source_sha256': summary.FROZEN_SOURCE},
        'failing_test': {'name': summary.BASELINE_TEST, 'backend': 'native', 'format': 'Q8_0',
            'status': 'failed', 'error': 'TestUnexpectedResult', 'metric': 'maximum_classifier_logit_absolute_error',
            'actual': summary.BASELINE_NUMBERS['native_q8'], 'absolute_threshold': .05},
        'numeric_errors': {'current': copy.deepcopy(summary.BASELINE_NUMBERS), 'historical': copy.deepcopy(summary.BASELINE_NUMBERS)},
        'historical_log': {'sha256': summary.BASELINE_HISTORICAL_SHA},
        'documentation': {'sha256': summary.BASELINE_DOCUMENT_SHA, 'base_sha256': summary.BASELINE_DOCUMENT_SHA, 'unchanged_vs_base': True},
        'scope': {'native_q8_qualified': False, 'tolerance_changed': False, 'implementation_changed': False,
            'only_failure_accepted': summary.BASELINE_TEST, 'raw_result_remains_failed': True, 'historical_other_failures_accepted': False}}


def baseline_log(refs):
    text = ''
    for key, name in summary.BASELINE_TESTS.items():
        text += '1/316 ' + name + '...\nGLiNER2.5-Decide ' + ('cuda' if key.startswith('cuda') else 'native')
        text += ' max classifier logit error ' + str(summary.BASELINE_NUMBERS[key]) + '\n'
        text += 'FAIL (TestUnexpectedResult)\n' if key == 'native_q8' else 'OK\n'
    for spec in summary.sanitizers.SUITES.values():
        text += '1/316 example.test.' + spec['filter'] + '...OK\n'
    for p in summary.http.PROFILES:
        row = owned_memory(p)
        row['timings'] = [{'id': c['id']} for c in refs[p]['cases']]
        text += 'specialty_http_receipt: ' + json.dumps(row) + '\n'
    return text + '9 selected; 8 passed; 0 skipped; 1 failed; 0 leaked.\n'


def verified(text):
    return summary.VerifiedBaseline(summary.FROZEN_SOURCE, hashlib.sha256(text.encode()).hexdigest(), summary.BASELINE_TEST, {})


class QualificationEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.refs = references()
        self.evidence = FakeEvidence()

    def validate_sanitizers(self, report, tools=('memcheck', 'initcheck'), pattern=None):
        return summary.validate_sanitizers(report, tools, pattern, SOURCE, TEST, SERVER, self.refs, self.evidence, 'fixture')

    def test_accepts_complete_23_case_http_receipt_and_rejects_missing_or_changed_results(self):
        report = http_receipt(self.refs)
        self.assertEqual(23, summary.validate_http(report, self.refs, SERVER, self.evidence, 'fixture')['validated_cases'])
        changed = copy.deepcopy(report)
        changed['validation'].pop()
        with self.assertRaisesRegex(ValueError, 'coverage'):
            summary.validate_http(changed, self.refs, SERVER, self.evidence, 'fixture')
        changed = copy.deepcopy(report)
        changed['validation'][0]['response']['data'][0]['entities'][0]['score'] += .001
        with self.assertRaises(ValueError):
            summary.validate_http(changed, self.refs, SERVER, self.evidence, 'fixture')

    def test_mixed_binary_failure_status_and_unknown_format_never_pass(self):
        for mutation in [{'server_binary': {'sha256': 'wrong'}}, {'status': 'running'},
                         {'format_version': 2}, {'status': 'passed', 'error': 'retained failure'}]:
            report = http_receipt(self.refs)
            report.update(mutation)
            with self.assertRaises(ValueError):
                summary.validate_http(report, self.refs, SERVER, self.evidence, 'fixture')

    def test_accepts_six_unfiltered_memory_lanes_and_six_scoped_sync_lanes(self):
        self.assertEqual(6, len(self.validate_sanitizers(sanitizer_receipt(self.refs))['lanes']))
        scoped = sanitizer_receipt(self.refs, ('racecheck', 'synccheck'), '^termite_.*')
        result = self.validate_sanitizers(scoped, ('racecheck', 'synccheck'), '^termite_.*')
        self.assertFalse(result['all_kernel_coverage'])
        self.assertEqual(6, len(result['lanes']))

    def test_rejects_missing_or_duplicate_lanes_reduced_cases_and_mixed_source_or_binary(self):
        for kind in ['missing', 'duplicate', 'cases', 'source', 'test', 'server', 'failed']:
            report = sanitizer_receipt(self.refs)
            if kind == 'missing': report['lanes'].pop()
            elif kind == 'duplicate': report['lanes'][-1] = copy.deepcopy(report['lanes'][0])
            elif kind == 'cases': report['lanes'][1]['oracle_case_ids'].pop()
            elif kind == 'source': report['source']['source_sha256'] = 'wrong'
            elif kind == 'test': report['test_binary']['sha256'] = 'wrong'
            elif kind == 'server': report['candidate_server']['sha256'] = 'wrong'
            else: report['lanes'][0]['pass'] = False
            with self.subTest(kind=kind):
                with self.assertRaises(ValueError):
                    self.validate_sanitizers(report)

    def test_actual_sanitizer_log_failure_cannot_be_hidden_by_passing_json(self):
        report = sanitizer_receipt(self.refs)
        report['lanes'][0]['_text'] += '========= ERROR SUMMARY: 1 error\n'
        with self.assertRaisesRegex(ValueError, 'actual log failed'):
            self.validate_sanitizers(report)
        report = sanitizer_receipt(self.refs)
        report['lanes'][0]['command'].extend(['--launch-count', '1'])
        with self.assertRaises(ValueError):
            self.validate_sanitizers(report)

    def test_full_drift_matrix_means_36_rows_and_72_actions_with_exact_rejection_policy(self):
        manifest = manifest_fixture()
        report = drift_receipt(self.refs, manifest)
        result = summary.validate_drift(report, manifest, self.refs, SERVER, self.evidence)
        self.assertEqual(36, result['matrix_rows'])
        self.assertEqual(72, result['instance_and_cold_extraction_probes'])
        for kind in ['row', 'scope', 'wrong_code', 'stage']:
            changed = copy.deepcopy(report)
            if kind == 'row': changed['cases'].pop()
            elif kind == 'scope': changed['scope'] = 'filtered'
            elif kind == 'wrong_code': changed['cases'][4]['cold_extraction']['body']['error'] = 'UNSUPPORTED_EXTRACTION_FEATURE'
            else: changed['cases'][3]['cold_extraction']['body']['stage'] = 'inference'
            with self.subTest(kind=kind):
                with self.assertRaises(ValueError):
                    summary.validate_drift(changed, manifest, self.refs, SERVER, self.evidence)

    def test_soak_requires_full_duration_pss_bound_coverage_catalog_and_idle_recovery(self):
        manifest = manifest_fixture()
        report = soak_receipt(self.refs, manifest)
        result = summary.validate_soak(report, manifest, self.refs, SERVER, self.evidence)
        self.assertEqual(69, result['coordinate_unit_validations'])
        self.assertEqual(23, result['versionless_cases'])
        for kind in ['duration', 'bound', 'coverage', 'catalog', 'ledger', 'ragged', 'late_samples']:
            changed = copy.deepcopy(report)
            if kind == 'duration': changed['duration_seconds'] = 60
            elif kind == 'bound': changed['host_pss_gate']['maximum_growth_bytes'] = 256 << 20
            elif kind == 'coverage': changed['versionless_validation'].pop()
            elif kind == 'catalog': changed['catalog_before_load']['pii']['capabilities'].append('decide')
            elif kind == 'ledger': changed['idle_admission_after']['antfly_inference_admission_host_scratch_bytes'] = 1
            elif kind == 'ragged': changed['ragged_validation'][0]['ordered_case_ids'].reverse()
            else: changed['memory_samples'][-1]['elapsed_seconds'] = 200
            with self.subTest(kind=kind):
                with self.assertRaises(ValueError):
                    summary.validate_soak(changed, manifest, self.refs, SERVER, self.evidence)

    def test_bound_log_hash_mismatch_and_missing_receipt_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'log'
            path.write_text('actual log')
            evidence = summary.Evidence(Path(directory) / 'report.json')
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            self.assertEqual(b'actual log', evidence.read(path, digest))
            with self.assertRaisesRegex(ValueError, 'digest mismatch'):
                evidence.read(path, '0' * 64)
            with self.assertRaises(FileNotFoundError):
                evidence.document(Path(directory) / 'missing.json')

    def test_build_receipt_allows_direct_test_execution_but_requires_successful_compilation(self):
        row = {'returncode': 0, 'command': ['/test', '--test-filter', 'specialty'], 'compile_command': ['zig', 'build'],
               'compile_returncode': 0, 'effective_flags': {'cuda': True, 'metal': False, 'cuda_artifacts': 'fatbin', 'optimize': 'ReleaseFast'}}
        report = {'format_version': 1, 'status': 'passed', 'source_sha256': summary.FROZEN_SOURCE, 'zig_version': '0.17.0',
                  'server_binary': {'sha256': SERVER}, 'test_binary': {'sha256': TEST}, 'builds': {'server': copy.deepcopy(row), 'tests': copy.deepcopy(row)}}
        summary.validate_build(report, SERVER, TEST, self.evidence)
        report['builds']['tests']['compile_returncode'] = 1
        with self.assertRaises(ValueError):
            summary.validate_build(report, SERVER, TEST, self.evidence)

    def test_regression_summary_rejects_failed_or_leaked_tests_and_reports_skips(self):
        text = ''
        for spec in summary.sanitizers.SUITES.values():
            text += '1/316 example.test.' + spec['filter'] + '...OK\n'
        for p in summary.http.PROFILES:
            row = owned_memory(p)
            row['timings'] = [{'id': c['id']} for c in self.refs[p]['cases']]
            text += 'specialty_http_receipt: ' + json.dumps(row) + '\n'
        text += '6/316 example.test.Metal fixture...SKIP\n6 selected; 5 passed; 1 skipped.\n'
        counts, receipts = summary.validate_regressions(text, self.refs)
        self.assertEqual(1, counts['skipped'])
        self.assertEqual(2, len(receipts))
        for changed in [text.replace('6 selected; 5 passed; 1 skipped.', '6 selected; 5 passed; 0 skipped; 1 failed; 0 leaked.'),
                        text.replace('6 selected; 5 passed; 1 skipped.', '6 selected; 5 passed; 1 skipped; 0 failed; 1 leaked.'),
                        text + 'Build Summary: 1/2 steps succeeded (1 failed)\n',
                        text.replace('specialty_http_receipt:', 'missing_receipt:')]:
            with self.assertRaises(ValueError):
                summary.validate_regressions(changed, self.refs)


    def test_baseline_metadata_requires_exact_test_pins_scope_and_bound(self):
        record = baseline_metadata()
        summary.verify_baseline_metadata(record)
        for group, key, wrong in [('failing_test', 'name', 'a different failure'), ('failing_test', 'backend', 'cuda'),
                ('failing_test', 'absolute_threshold', .1), ('historical_log', 'sha256', 'untrusted'),
                ('documentation', 'sha256', 'untrusted'), ('source_snapshot', 'source_sha256', 'untrusted'),
                ('scope', 'native_q8_qualified', True), ('scope', 'raw_result_remains_failed', False)]:
            changed = copy.deepcopy(record)
            changed[group][key] = wrong
            with self.subTest(group=group, key=key), self.assertRaises(ValueError):
                summary.verify_baseline_metadata(changed)

    def test_only_exact_single_baseline_failure_is_accepted_and_raw_failure_is_preserved(self):
        text = baseline_log(self.refs)
        self.assertEqual(summary.BASELINE_NUMBERS, summary.observed_baseline_numbers(text))
        with self.assertRaises(ValueError):
            summary.validate_regressions(text, self.refs)
        counts, _ = summary.validate_regressions(text, self.refs, verified(text))
        self.assertEqual('documented_out_of_scope_baseline_failure', counts['status'])
        self.assertEqual((1, 0), (counts['failed'], counts['leaked']))
        for changed in [text.replace(summary.BASELINE_TEST, 'different.test'),
                text.replace(str(summary.BASELINE_NUMBERS['native_q8']), '.066'),
                text.replace('0 leaked.', '1 leaked.'),
                text.replace('9 selected; 8 passed; 0 skipped; 1 failed;', '10 selected; 8 passed; 0 skipped; 2 failed;'),
                text.replace('...OK', '...FAIL (TestUnexpectedResult)', 1)]:
            with self.subTest(changed=changed[-90:]), self.assertRaises(ValueError):
                summary.validate_regressions(changed, self.refs, verified(changed))
        with self.assertRaises(ValueError):
            summary.validate_regressions(text + '\n', self.refs, verified(text))

    def test_build_nonzero_requires_verified_bound_direct_test_run_and_successful_compile(self):
        text = baseline_log(self.refs)
        exception = verified(text)
        row = {'returncode': 0, 'command': ['/test'], 'compile_command': ['zig', 'build'],
               'compile_returncode': 0, 'effective_flags': {'cuda': True, 'metal': False, 'cuda_artifacts': 'fatbin', 'optimize': 'ReleaseFast'}}
        report = {'format_version': 1, 'status': 'passed', 'source_sha256': summary.FROZEN_SOURCE, 'zig_version': '0.17.0',
                  'server_binary': {'sha256': SERVER}, 'test_binary': {'sha256': TEST, 'path': '/test'},
                  'builds': {'server': copy.deepcopy(row), 'tests': copy.deepcopy(row)}}
        report['builds']['tests'].update(returncode=1, log_sha256=exception.current_log_sha256)
        summary.validate_build(report, SERVER, TEST, self.evidence, exception)
        with self.assertRaises(ValueError):
            summary.validate_build(report, SERVER, TEST, self.evidence)
        for name, key, value in [('server', 'returncode', 1), ('tests', 'returncode', 2),
                ('tests', 'compile_returncode', 1), ('tests', 'log_sha256', 'wrong'), ('tests', 'command', ['zig', 'build'])]:
            changed = copy.deepcopy(report)
            changed['builds'][name][key] = value
            with self.subTest(name=name, key=key), self.assertRaises(ValueError):
                summary.validate_build(changed, SERVER, TEST, self.evidence, exception)


if __name__ == '__main__':
    unittest.main()
