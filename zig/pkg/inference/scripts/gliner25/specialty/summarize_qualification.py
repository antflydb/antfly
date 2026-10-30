#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Assemble the final GB10 campaign only after every required receipt passes.

Reads existing receipts and logs; never builds or launches inference. The output
binds observed sources, executables, artifacts, and scoped hardware evidence.
It does not attest reproducible compilation or Apple Metal qualification.
"""
from __future__ import annotations

import argparse
from collections import Counter
from dataclasses import dataclass
import hashlib
import json
import math
import os
from pathlib import Path
import re
import statistics
import subprocess
import sys
import time

import qualify_artifact_drift as drift
import qualify_cuda_sanitizers as sanitizers
import qualify_http as http
import qualify_managed_install as managed
import qualify_soak as soak

FROZEN_SOURCE = '473a568bfe242cf86f80dc4bbba395798a918e1dc0057cc63656a4e0d4a3073b'
POLICY_MODES = ('auto', 'streaming-f16', 'materialized-f16', 'generated-tc')
FINAL_FILES = {
    'build': 'production-final-build-receipt.json',
    'artifacts': 'production-final-cuda-artifacts.json',
    'managed': 'production-final-managed-install.json',
    'drift': 'production-final-artifact-drift.json',
    'soak': 'production-final-soak.json',
    'http': 'production-final-http.json',
    'http_batch8': 'production-final-http-batch8.json',
    'memory': 'production-final-sanitizers-memory.json',
    'sync': 'production-final-sanitizers-sync.json',
    **{'policy_' + mode: f'production-final-policy-{mode}.json' for mode in POLICY_MODES},
}


BASELINE_TEST = 'extractors.gliner_span_v2_executor.test.gliner span v2 GLiNER2.5-Decide Q8_0 bundle native parity'
BASELINE_DOCUMENT_SHA = '39823106bcd5c9ded0c6d31e9715883ce773ffe6a94b402da841e4cdd5f3c36d'
BASELINE_HISTORICAL_SHA = 'e74b321a1a2fae78656a8221b5e2c64c2ccce0abb2924618885e3a6e2e600c82'
BASELINE_NUMBERS = {'native_fp32': 7.62939453125e-6, 'native_q8': .06692218780517578,
                    'cuda_fp32': 8.106231689453125e-6, 'cuda_q8': .03154325485229492}
BASELINE_TESTS = {
    'native_fp32': 'extractors.gliner_span_v2_executor.test.gliner span v2 GLiNER2.5-Decide native parity',
    'native_q8': BASELINE_TEST,
    'cuda_fp32': 'extractors.gliner_span_v2_executor.test.gliner span v2 GLiNER2.5-Decide CUDA classification oracle parity',
    'cuda_q8': 'extractors.gliner_span_v2_executor.test.gliner span v2 GLiNER2.5-Decide Q8 CUDA classification oracle parity',
}
BASELINE_IMPLEMENTATION = {
    'zig/pkg/inference/src/ops/native_compute.zig': 'de6f0f48fe6261c82ebc953dc6611eb8f6eda21dbcf5ad3a1ecd16bde3d3175c',
    'zig/pkg/inference/src/backends/native.zig': 'bb7b8a0c9b4fcb8396397c18a3e7db199f4e1049087b8f81c74dd6f6d33d6137',
    'zig/pkg/inference/src/graph/quant_matmul.zig': 'ebd78236578a6671316cc2929e6236f5cf7371df79cb81b271965e6db578a6a0',
    'zig/pkg/inference/src/gguf/quant_codec.zig': '8be37848076ad923bf3139f557c1b3b4130ffb6b6e3aed822d1de0cba561abe4',
    'zig/pkg/inference/src/gguf/tensor_types.zig': '323e617a454ac26b2b4c862d2b46b7d9ed81fe6b43704b3cd77e41d9c6c0393c',
}
BASELINE_MODEL_FILES = {
    'gliner2-encoder.Q8_0.gguf': ('8d9bea7df21ab4a8a17333f51f246faab5280159e5751ebbadd81a85ec96c871', 465145600),
    'gliner2-head.Q8_0.gguf': ('52cb6197d3dcfe5ad6276ceb037a3333d2921bbcf3dfb8289aeff82fbd5693dc', 62064576),
    'config.json': ('e748e5b80575471c91b3f0dd00f513ba58242544fcb7e1236e0021e61abd7673', 594),
    'tokenizer.json': ('3ad87d9ffe669147063e70850927dd2da90249e2acc5c8527f1eb65df467bcc8', 8333952),
}


@dataclass(frozen=True)
class VerifiedBaseline:
    source_sha256: str
    current_log_sha256: str
    failing_test: str
    evidence: dict


def test_sections(text):
    starts = list(re.finditer(r'^\d+/\d+ (.*?)\.\.\.', text, re.M))
    sections = []
    for i, start in enumerate(starts):
        end = starts[i + 1].start() if i + 1 < len(starts) else len(text)
        sections.append((start.group(1), text[start.end():end]))
    return sections


def observed_baseline_numbers(text):
    sections = test_sections(text)
    result = {}
    for key, name in BASELINE_TESTS.items():
        matched = [body for observed, body in sections if observed == name]
        require(len(matched) == 1, f'baseline: missing/duplicate numeric comparison test {name}')
        body = matched[0]
        values = re.findall(r'GLiNER2\.5-Decide (?:native|cuda) max classifier logit error ([0-9.eE+-]+)', body)
        require(len(values) == 1, f'baseline: numeric error missing for {name}')
        result[key] = float(values[0])
        if key == 'native_q8':
            require(re.search(r'^FAIL \(TestUnexpectedResult\)$', body, re.M), 'baseline: native Q8 failed for a different reason')
        else:
            require(re.search(r'^OK$', body, re.M) and not re.search(r'^FAIL', body, re.M), f'baseline: other precision/backend comparison failed: {key}')
    require(result == BASELINE_NUMBERS, 'baseline: raw numeric quartet differs from the documented historical failure')
    return result


def verify_baseline_metadata(record):
    require(record.get('schema_version') == 1 and record.get('exception_kind') == 'preexisting_native_q8_out_of_scope', 'baseline: unknown exception schema/kind')
    require(record.get('source_snapshot', {}).get('source_sha256') == FROZEN_SOURCE, 'baseline: unpinned source snapshot')
    failure = record.get('failing_test', {})
    expected = {'name': BASELINE_TEST, 'backend': 'native', 'format': 'Q8_0', 'status': 'failed',
        'error': 'TestUnexpectedResult', 'metric': 'maximum_classifier_logit_absolute_error',
        'actual': BASELINE_NUMBERS['native_q8'], 'absolute_threshold': .05}
    require(all(failure.get(k) == value for k, value in expected.items()), 'baseline: wrong failed test/backend/format/numerical bound')
    require(record.get('numeric_errors') == {'current': BASELINE_NUMBERS, 'historical': BASELINE_NUMBERS}, 'baseline: numeric identity proof differs')
    require(record.get('historical_log', {}).get('sha256') == BASELINE_HISTORICAL_SHA, 'baseline: unpinned historical log')
    documentation = record.get('documentation', {})
    require(documentation.get('sha256') == documentation.get('base_sha256') == BASELINE_DOCUMENT_SHA
            and documentation.get('unchanged_vs_base') is True, 'baseline: unpinned or changed pre-existing documentation')
    scope = record.get('scope', {})
    require(scope.get('native_q8_qualified') is False and scope.get('tolerance_changed') is False
            and scope.get('implementation_changed') is False and scope.get('only_failure_accepted') == BASELINE_TEST
            and scope.get('raw_result_remains_failed') is True and scope.get('historical_other_failures_accepted') is False,
            'baseline: exception broadens qualification or hides the raw failure')


def validate_baseline_exception(record, current_text, evidence, frozen):
    verify_baseline_metadata(record)
    require(record.get('base_commit') == frozen['git_commit'], 'baseline: wrong pre-task source commit')
    source = record['source_snapshot']
    source_raw = evidence.read(source['path'], source['sha256'])
    require(json.loads(source_raw) == frozen, 'baseline: different frozen source receipt')
    current_sha = hashlib.sha256(current_text.encode()).hexdigest()
    require(record.get('current_log', {}).get('sha256') == current_sha, 'baseline: proof does not bind this regression log')
    for key in ('current_log', 'historical_log'):
        row = record[key]
        raw = evidence.read(row['path'], row['sha256'])
        require(len(raw) == row.get('size_bytes'), f'baseline: wrong {key} byte count')
        saved = row.get('preserved_copy', {})
        require(saved.get('sha256') == row['sha256'] and evidence.read(saved['path'], saved['sha256']) == raw,
                f'baseline: missing exact preserved {key}')
    historical = evidence.read(record['historical_log']['path'], BASELINE_HISTORICAL_SHA).decode()
    observed_baseline_numbers(current_text)
    observed_baseline_numbers(historical)
    failed = [name for name, body in test_sections(current_text) if re.search(r'^FAIL(?:\s|$)', body, re.M)]
    require(failed == [BASELINE_TEST], 'baseline: another or additional test failed')
    doc = record['documentation']
    expected_doc_path = http.REPO / 'zig/pkg/inference/scripts/gliner25/CUDA.md'
    require(Path(doc['path']).resolve() == expected_doc_path and doc.get('base_commit') == frozen['git_commit'], 'baseline: incorrect documentation source')
    doc_raw = evidence.read(doc['path'], BASELINE_DOCUMENT_SHA)
    base_doc = subprocess.check_output(['git', 'show', frozen['git_commit'] + ':zig/pkg/inference/scripts/gliner25/CUDA.md'], cwd=http.REPO)
    require(hashlib.sha256(base_doc).hexdigest() == BASELINE_DOCUMENT_SHA, 'baseline: documentation was not present unchanged before this task')
    lines = doc_raw.decode().splitlines()
    require('native Q8 remains unqualified' in lines[548] and '5e-2 raw-logit bound' in lines[547], 'baseline: documentation does not describe this out-of-scope failure')
    require(doc.get('supporting_lines') == [{'line': i, 'text': lines[i - 1]} for i in (548, 549, 550)], 'baseline: documentation citation differs')
    implementation = record.get('unchanged_native_implementation', [])
    require(len(implementation) == len(BASELINE_IMPLEMENTATION) and {r.get('repository_path') for r in implementation} == set(BASELINE_IMPLEMENTATION), 'baseline: incomplete native implementation comparison')
    for row in implementation:
        relative, pinned = row['repository_path'], BASELINE_IMPLEMENTATION[row['repository_path']]
        require(row.get('sha256') == row.get('base_sha256') == pinned and row.get('unchanged_vs_base') is True
                and Path(row['path']).resolve() == http.REPO / relative, 'baseline: native quantization implementation changed')
        evidence.read(row['path'], pinned)
        before = subprocess.check_output(['git', 'show', frozen['git_commit'] + ':' + relative], cwd=http.REPO)
        require(hashlib.sha256(before).hexdigest() == pinned, 'baseline: native implementation differs from its pre-task version')
    models = record.get('model_artifacts', [])
    require(len(models) == 4 and {Path(row['path']).name for row in models} == set(BASELINE_MODEL_FILES), 'baseline: missing model artifact pins')
    for row in models:
        pinned, size = BASELINE_MODEL_FILES[Path(row['path']).name]
        require(row.get('sha256') == row.get('expected_sha256') == pinned and row.get('size_bytes') == size
                and Path(row['path']).stat().st_size == size and http.digest_file(Path(row['path'])) == pinned,
                'baseline: quantized diagnostic artifacts differ')
    return VerifiedBaseline(FROZEN_SOURCE, current_sha, BASELINE_TEST, record)


def require(condition, message):
    if not condition:
        raise ValueError(message)


def number(value, label, minimum=0):
    require(type(value) in (int, float) and math.isfinite(value) and value >= minimum,
            f'{label}: expected finite number >= {minimum}')
    return value


def snapshot(path):
    path = Path(path)
    before = path.stat()
    raw = path.read_bytes()
    after = path.stat()
    require((before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) ==
            (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns), f'input changed while reading: {path}')
    return raw, hashlib.sha256(raw).hexdigest()


class Evidence:
    def __init__(self, output):
        self.output = Path(output)
        self.references = {}

    def read(self, path, expected_sha=None):
        path = Path(path).resolve()
        raw, digest = snapshot(path)
        require(expected_sha is None or digest == expected_sha, f'log/receipt digest mismatch: {path}')
        self.references[str(path)] = {'path': os.path.relpath(path, self.output.parent), 'sha256': digest,
                                       'size_bytes': len(raw)}
        return raw

    def document(self, path):
        value = json.loads(self.read(path))
        require(isinstance(value, dict), f'receipt is not an object: {path}')
        return value

    def log(self, record, path_key='log', sha_key='log_sha256'):
        require(isinstance(record.get(path_key), str) and isinstance(record.get(sha_key), str),
                f'missing bound log fields: {path_key}/{sha_key}')
        return self.read(record[path_key], record[sha_key]).decode(errors='replace')


def passed(report, label, boolean=False):
    require(report.get('format_version') == 1, f'{label}: unsupported receipt format')
    require(report.get('passed') is True if boolean else report.get('status') == 'passed', f'{label}: gate did not pass')
    require(not any(report.get(key) for key in ('failure', 'error', 'traceback', 'validation_failures')),
            f'{label}: receipt retains a failure diagnostic')


def binary_association(report, key, expected_sha, label):
    require(report.get(key, {}).get('sha256') == expected_sha, f'{label}: mixed or missing {key} SHA')


def manifest_binding(report, manifest, manifest_sha, label):
    require(report.get('artifact_pins') == manifest, f'{label}: model/oracle manifest pins differ')
    values = [report[k] for k in ('artifact_manifest_sha256', 'manifest_sha256') if k in report]
    require(bool(values) and all(value == manifest_sha for value in values), f'{label}: manifest file SHA differs')


def policy_receipt(receipts, label):
    require(set(receipts) == set(http.PROFILES), f'{label}: missing profile arithmetic policy')
    for profile, value in receipts.items():
        require((value.get('weights'), value.get('attention'), value.get('diagnostic_overrides')) ==
                ('f32', 'f32', 'ignored') and bool(value.get('log_lines')), f'{label}/{profile}: arithmetic policy drift')


def server_binding(report, expected_sha, evidence, label, policies):
    binary_association(report, 'server_binary', expected_sha, label)
    for key in ('running_binary_before', 'running_binary_after'):
        value = report.get(key, {})
        require(value.get('sha256') == expected_sha and type(value.get('pid')) is int and value['pid'] > 0
                and value.get('process_group') == value['pid'], f'{label}: missing actual running-image binding {key}')
    require(report['running_binary_before']['pid'] == report['running_binary_after']['pid'], f'{label}: server leader changed')
    require(report.get('server_returncode') in (0, -15), f'{label}: abnormal or forced server shutdown')
    log = evidence.log(report, 'server_log', 'server_log_sha256')
    observed = http.parse_cuda_policy(log)
    for key in policies:
        policy_receipt(report.get(key, {}), label + '/' + key)
    policy_receipt(observed, label + '/actual_log')
    require(report.get('backend_environment', {}).get('ANTFLY_INFERENCE_REQUIRED_BACKEND') == 'cuda',
            f'{label}: backend was not required CUDA')


def errors_at_most(rows, label):
    worst = 0
    for row in rows:
        worst = max(worst, number(row.get('max_confidence_abs_error'), label + '/confidence'))
    require(worst <= http.SCORE_TOLERANCE, f'{label}: numerical tolerance exceeded')
    return worst


def case_index(references):
    return {(profile, case['id']): case for profile, ref in references.items() for case in ref['cases']}


def validate_http(report, references, expected_sha, evidence, label, batch=1, mode='f32', generated='0', benchmark=True):
    passed(report, label)
    server_binding(report, expected_sha, evidence, label, ('cuda_policy_before', 'cuda_policy_after'))
    require(report.get('confidence_abs_tolerance') == http.SCORE_TOLERANCE, f'{label}: changed confidence tolerance')
    require(report.get('requested_attention_diagnostics') == {'attention_mode': mode, 'generated_tc_auto': generated},
            f'{label}: wrong requested diagnostic modes')
    env = report['backend_environment']
    require(env.get('ANTFLY_INFERENCE_CUDA_DEBERTA_ATTENTION_MODE') == mode and
            env.get('ANTFLY_CUDA_DEBERTA_GENERATED_TC_AUTO') == generated, f'{label}: diagnostic environment differs')
    expected = case_index(references)
    rows = report.get('validation', [])
    require(Counter((row.get('profile'), row.get('id')) for row in rows) == Counter(expected.keys()),
            f'{label}: frozen 23-case validation coverage differs')
    worst = 0
    for row in rows:
        key = row['profile'], row['id']
        worst = max(worst, http.check_response(key[0], expected[key], row['response']))
    before, after = http.check_instances(report['backend_instances_before']), http.check_instances(report['backend_instances_after'])
    require(all(before[p]['id'] == after[p]['id'] for p in http.PROFILES), f'{label}: runtime incarnation changed')
    timings, seen = [], set()
    for row in report.get('timings', []):
        key = row.get('profile'), row.get('id')
        require(key in expected and key not in seen, f'{label}: unknown/duplicate timing case {key}')
        seen.add(key)
        require(row.get('request_items') == batch and row.get('physical_batch_items') ==
                (1 if key[0] == 'pii' else min(8, batch)), f'{label}: wrong physical/public batch geometry')
        require(row.get('prepared_tokens_per_item') == len(expected[key]['input_ids']), f'{label}: prepared token drift')
        samples = row.get('latency_ms', {}).get('samples', [])
        minimum = 5 if benchmark else 1
        require(len(samples) == row.get('iterations') and len(samples) >= minimum and
                row.get('warmups', -1) >= (2 if benchmark else 0), f'{label}: insufficient timing trials/warmups')
        for sample in samples:
            number(sample, label + '/latency', minimum=1e-12)
        median = statistics.median(samples)
        require(abs(row['latency_ms']['median'] - median) <= 1e-8, f'{label}: timing median differs from raw samples')
        timings.append({key: row[key] for key in ('profile', 'id', 'request_items', 'physical_batch_items',
            'prepared_tokens_per_item', 'warmups', 'iterations', 'latency_ms', 'items_per_second', 'prepared_tokens_per_second')})
    require(timings, f'{label}: no measured requests')
    if benchmark and batch == 1:
        require(seen == set(expected), f'{label}: singleton benchmark omitted frozen cases')
    if batch == 8:
        require({('gliguard', 'prompt_safety'), ('gliguard', 'composed_all_six')}.issubset(seen),
                f'{label}: missing matched GLiGuard batch workloads')
    return {'validated_cases': len(rows), 'maximum_confidence_abs_error': worst, 'timing_scope': report['timing_scope'],
            'timings': timings, 'backend_environment': env, 'server_command': report.get('server_command')}


def validate_managed(report, manifest, expected_sha, evidence):
    passed(report, 'managed install', boolean=True)
    binary_association(report, 'cli', expected_sha, 'managed install')
    require(report.get('backend') == 'native' and report.get('gpu_used') is False, 'managed install: unexpected GPU use')
    counts = {}
    require(set(report.get('models', {})) == set(http.PROFILES), 'managed install: missing model')
    for profile in http.PROFILES:
        pin = manifest['models'][profile]
        row = report['models'][profile]
        pulls, publications = row.get('pulls', []), row.get('published', [])
        require(len(pulls) >= 2 and len(publications) == len(pulls), f'managed install/{profile}: missing repeat publication')
        owner, name = pin['repository'].split('/', 1)
        source = {'owner': owner, 'name': name, 'variant': 'auto', 'requested_revision': pin['revision'],
                  'resolved_revision': pin['revision'], 'selected_format': 'safetensors'}
        for pull in pulls:
            require(pull.get('exit_code') == 0 and set(pull.get('cache_verified_files', [])) == set(pin['files'])
                    and pull.get('large_payload_network_downloads') == 0, f'managed install/{profile}: pull/cache failure')
            text = evidence.log(pull)
            require(f"Resolved {pin['repository']}@{pin['revision']} to {pin['revision']}" in text,
                    f'managed install/{profile}: immutable resolution absent from actual log')
        for publication in publications:
            require(publication.get('source') == source and publication.get('model') == pin['repository'],
                    f'managed install/{profile}: wrong provenance')
            require(publication.get('model_manifest') == managed.expected_manifest(profile), f'managed install/{profile}: task/capability grants drift')
            require(publication.get('original_files') == pin['files'], f'managed install/{profile}: original bytes drift')
            completion = publication.get('completion_receipt', {})
            require(completion.get('version') == 2 and completion.get('source') == source, f'managed install/{profile}: incomplete publication')
            entries = completion.get('artifacts', [])
            require(len(entries) == len(pin['files']) + 1 and {e.get('path') for e in entries} == set(pin['files']) | {'model_manifest.json'},
                    f'managed install/{profile}: published artifact set differs')
        require(row.get('source_artifacts_after') == pin['files'], f'managed install/{profile}: source preservation failed')
        counts[profile] = {'verified_pulls': len(pulls), 'manifest': managed.expected_manifest(profile)}
    listing = report.get('canonical_listing', {})
    require(listing.get('exit_code') == 0, 'managed install: listing failed')
    text = evidence.log(listing, sha_key='sha256')
    for profile in http.PROFILES:
        require(re.search(r'^extractor\s+' + re.escape(manifest['models'][profile]['repository']) + r'\s*$', text, re.M),
                f'managed install: canonical listing omitted {profile}')
    return counts


def validate_drift(report, manifest, references, expected_sha, evidence):
    passed(report, 'artifact drift', boolean=True)
    binary_association(report, 'server_binary', expected_sha, 'artifact drift')
    require(report.get('cpu_only') is True and report.get('server_cleanup_complete') is True and
            report.get('probe_models_cleaned_up') is True and report.get('server_exit_code') in (0, -15), 'artifact drift: incomplete cleanup/CPU control')
    variants = (*drift.CONTROLS, *drift.NEGATIVES, 'managed_valid')
    rows = report.get('cases', [])
    require(report.get('scope') == 'full_matrix' and report.get('selected_variants') == list(variants) and report.get('expected_load_extraction_checks') == 72, 'artifact drift: filtered or unknown matrix scope')
    require([(row.get('profile'), row.get('variant')) for row in rows] ==
            [(p, v) for p in http.PROFILES for v in variants], 'artifact drift: missing/filtered default 36-row matrix')
    for row in rows:
        profile, variant = row['profile'], row['variant']
        load, extraction = row.get('response', {}), row.get('cold_extraction', {})
        if variant in drift.CONTROLS:
            require(load.get('body', {}).get('state') == 'loaded' and load['body'].get('tasks') == ['extractor'], f'artifact drift/{profile}/{variant}: missing loaded extractor identity')
            require(load.get('status') == 200 and load.get('body', {}).get('backend') == 'native' and
                    extraction.get('status') == 200, f'artifact drift/{profile}/{variant}: valid control failed')
            require(extraction['body'].get('object') == 'extraction' and extraction['body'].get('model') == row['request']['model'], f'artifact drift/{profile}/{variant}: cold response identity mismatch')
            http.check_response(profile, references[profile]['cases'][0], extraction['body'])
            for key in ('unload', 'extraction_unload'):
                require(row.get(key, {}).get('status') == 200 and row[key].get('body', {}).get('outcome') == 'unloaded',
                        f'artifact drift/{profile}/{variant}: control unload failed')
        else:
            require(load.get('status') == 400 and load.get('body', {}).get('error') == 'INVALID_MODEL' and
                    load['body'].get('message') == drift.EXPECTED_REJECTION_MESSAGE,
                    f'artifact drift/{profile}/{variant}: instance load did not explicitly reject the artifact')
            expected_code = 'UNSUPPORTED_EXTRACTION_FEATURE' if variant == 'raw_config_drift' else 'INVALID_MODEL'
            require(extraction.get('status') == 400 and extraction.get('body', {}).get('error') == expected_code and
                    extraction['body'].get('stage') == 'model',
                    f'artifact drift/{profile}/{variant}: cold extraction did not reject at its allowed model-stage policy')
        state = row.get('instances_after', {})
        require(state.get('status') == 200 and state.get('body', {}).get('data') == [], f'artifact drift/{profile}/{variant}: runtime leaked')
    for p in http.PROFILES:
        require(report.get('original_artifacts_after', {}).get(p) == {name: pin['sha256'] for name, pin in manifest['models'][p]['files'].items()},
                f'artifact drift/{p}: original artifacts changed')
    if report.get('server_log'):
        evidence.read(report['server_log'])
    return {'matrix_rows': len(rows), 'instance_and_cold_extraction_probes': len(rows) * 2,
            'negative_rows': 2 * len(drift.NEGATIVES), 'variants_in_order': list(variants)}


def validate_soak(report, manifest, references, expected_sha, evidence):
    passed(report, 'soak')
    server_binding(report, expected_sha, evidence, 'soak', ('cuda_policy_after_load', 'cuda_policy_after_reload'))
    require(number(report.get('duration_seconds'), 'soak/duration') >= 300 and report.get('concurrency') == 8,
            'soak: requires at least 300 seconds at concurrency 8')
    require(number(report.get('traffic_duration_seconds'), 'soak/elapsed') >= report['duration_seconds'], 'soak: load ended early')
    expected = case_index(references)
    validation = report.get('validation', [])
    require(Counter((r.get('profile'), r.get('case'), r.get('unit')) for r in validation) ==
            Counter((p, cid, unit) for p, cid in expected for unit in ('utf8_bytes', 'unicode_codepoints', 'utf16_codeunits')),
            'soak: incomplete frozen-case coordinate-unit coverage')
    require(Counter((r.get('profile'), r.get('case')) for r in report.get('versionless_validation', [])) == Counter(expected.keys()),
            'soak: incomplete versionless coverage')
    ragged = report.get('ragged_validation', [])
    require({r.get('profile') for r in ragged} == set(http.PROFILES) and len(ragged) == 2, 'soak: missing ragged profiles')
    for row in ragged:
        ids = [c['id'] for c in references[row['profile']]['cases']]
        require(row.get('ordered_case_ids') == ids and row.get('items') == len(ids), 'soak: ragged item ordering/count drift')
    geometry = report.get('chunked_and_maximum', [])
    require(Counter((r.get('profile'), r.get('count')) for r in geometry) == Counter((p, n) for p in http.PROFILES for n in (8, 9, 128)),
            'soak: missing chunked/maximum request geometry')
    negatives = report.get('rejections', [])
    expected_status = {'oversized': 413, 'too_many_items': 413, 'unsupported_task': 400, 'long_document': 400}
    require(Counter((r.get('profile'), r.get('probe')) for r in negatives) == Counter((p, n) for p in http.PROFILES for n in expected_status),
            'soak: incomplete rejection/recovery matrix')
    require(all(r.get('status') == expected_status[r['probe']] and isinstance(r.get('response', {}).get('error'), str) for r in negatives),
            'soak: incorrect rejection statuses')
    names = {p: manifest['models'][p]['repository'] for p in http.PROFILES}
    for key in ('catalog_before_load', 'catalog_after_reload'):
        rows = report.get(key, {})
        require(set(rows) == set(http.PROFILES), f'soak: missing {key} model/task assertions')
        soak.assert_catalog({'extractors': {names[p]: rows[p] for p in http.PROFILES}, 'deciders': {}}, names)
    before, after = report.get('idle_admission_before'), report.get('idle_admission_after')
    require(isinstance(before, dict) and before == after, 'soak: idle admission ledger changed')
    required_metrics = {'antfly_inference_admission_' + name for name in ('host_scratch_bytes', 'backend_scratch_bytes', 'host_weight_bytes', 'backend_weight_bytes')}
    require(required_metrics.issubset(before) and all(type(v) is int and v >= 0 for v in before.values()), 'soak: incomplete/invalid admission ledger')
    soak.assert_idle(before)
    lifecycle = report.get('lifecycle', [])
    require(len(lifecycle) == 2 and {r.get('profile') for r in lifecycle} == set(http.PROFILES), 'soak: incomplete unload/reload matrix')
    for row in lifecycle:
        p = row['profile']
        require(row.get('unload', {}).get('outcome') == 'unloaded' and row.get('repeat', {}).get('outcome') == 'already_unloaded', 'soak: unload idempotence failed')
        old, new = report['loaded_instances'][p], row.get('reloaded', {})
        require(old.get('backend') == new.get('backend') == 'cuda' and old.get('model') == new.get('model') == names[p]
                and new.get('state') == 'loaded' and new.get('id') != old.get('id'), 'soak: reload did not create a new CUDA incarnation')
    traffic = report.get('traffic', [])
    require(len(traffic) >= 32 and all((r.get('profile'), r.get('case')) in expected for r in traffic), 'soak: insufficient/unknown mixed requests')
    require({r.get('profile') for r in traffic} == set(http.PROFILES) and {r.get('count') for r in traffic} == {1, 2, 8, 9}
            and {r.get('unit') for r in traffic} == {'utf8_bytes', 'unicode_codepoints', 'utf16_codeunits'}, 'soak: sustained traffic was not mixed across profiles, geometry, and offsets')
    all_rows = validation + report['versionless_validation'] + ragged + geometry + traffic + [r['recovery'] for r in lifecycle]
    worst = errors_at_most(all_rows, 'soak')
    samples = report.get('memory_samples', [])
    require(len(samples) >= 10, 'soak: insufficient memory sampling')
    for sample in samples:
        number(sample.get('pss_bytes'), 'soak/PSS', minimum=1)
    elapsed_samples = [number(sample.get('elapsed_seconds'), 'soak/sample time') for sample in samples]
    require(elapsed_samples == sorted(elapsed_samples) and elapsed_samples[-1] >= report['duration_seconds'] * .8, 'soak: memory samples do not span the sustained-load interval')
    late = samples[len(samples) // 2:]
    split = len(late) // 2
    growth = statistics.median(s['pss_bytes'] for s in late[split:]) - statistics.median(s['pss_bytes'] for s in late[:split])
    bound = report.get('host_pss_gate', {}).get('maximum_growth_bytes')
    require(type(bound) is int and 0 < bound <= 64 << 20 and growth <= bound and
            report['host_pss_gate'].get('late_window_median_growth_bytes') == growth, 'soak: PSS plateau exceeded or incorrectly summarized')
    summary = report.get('summary', {})
    require(summary.get('requests') == len(traffic) and summary.get('items') == sum(r['count'] for r in traffic), 'soak: traffic summary mismatch')
    return {'duration_seconds': report['traffic_duration_seconds'], 'concurrency': 8, 'requests': len(traffic),
            'items': summary['items'], 'coordinate_unit_validations': len(validation), 'versionless_cases': len(report['versionless_validation']),
            'ragged_items': {r['profile']: r['items'] for r in ragged}, 'public_maximum_request_items': 128,
            'maximum_confidence_abs_error': worst, 'host_pss_gate': report['host_pss_gate'],
            'maximum_sampled_pss_bytes': max(s['pss_bytes'] for s in samples), 'catalog': report['catalog_before_load'],
            'idle_admission': before, 'backend_environment': report['backend_environment'], 'server_command': report.get('command')}


def validate_sanitizers(report, tools, pattern, frozen_source, expected_test_sha, expected_server_sha, references, evidence, label):
    passed(report, label)
    binary_association(report, 'test_binary', expected_test_sha, label)
    binary_association(report, 'candidate_server', expected_server_sha, label)
    require(report.get('source') == {k: v for k, v in frozen_source.items() if k != 'captured_unix_seconds'}, f'{label}: frozen source differs')
    require(set(report.get('tools', [])) == set(tools) and set(report.get('requested_suites', [])) == set(sanitizers.BASE_SUITES), f'{label}: missing tool/suite coverage')
    require(report.get('case_selection') is None and report.get('sanitizer_kernel_filter') == pattern, f'{label}: reduced cases or wrong kernel scope')
    require(report.get('full_head_case_coverage') is True, f'{label}: full head coverage not confirmed')
    scope = report.get('sanitizer_kernel_scope', {})
    require(scope.get('regex') == pattern and scope.get('all_kernel_coverage_claimed') is (pattern is None), f'{label}: inaccurate kernel coverage claim')
    require(report.get('all_kernel_coverage') is (pattern is None), f'{label}: inaccurate overall kernel coverage')
    rows = report.get('lanes', [])
    require(Counter((r.get('tool'), r.get('suite')) for r in rows) == Counter((tool, suite) for tool in tools for suite in sanitizers.BASE_SUITES), f'{label}: missing/duplicate sanitizer lanes')
    compact = []
    for row in rows:
        require(row.get('pass') is True and row.get('failures') == [] and row.get('timed_out') is False and row.get('returncode') == 0, f'{label}: failed sanitizer lane')
        text = evidence.log(row)
        actual = sanitizers.assess_log(text, row['tool'], sanitizers.SUITES[row['suite']]['filter'], row['returncode'], row['timed_out'])
        require(actual['pass'], f"{label}/{row['tool']}/{row['suite']}: actual log failed: {actual['failures']}")
        command = row.get('command', [])
        require(command[-3:] == [report['test_binary']['path'], '--test-filter', sanitizers.SUITES[row['suite']]['filter']], f'{label}: lane ran a different test binary/filter')
        for flag, expected_value in [('--target-processes', 'all'), ('--require-cuda-init', 'yes'), ('--error-exitcode', '99')]:
            require(flag in command and command[command.index(flag) + 1] == expected_value, f'{label}: missing CUDA/exit instrumentation requirement {flag}')
        require(not any(flag in command for flag in ('--kernel-name-exclude', '--launch-skip', '--launch-count', '--suppressions')), f'{label}: hidden reduced sanitizer coverage')
        if pattern is None:
            require('--kernel-name' not in command, f'{label}: hidden memory-tool kernel filter')
        else:
            require('--kernel-name' in command and command[command.index('--kernel-name') + 1] == 'regex=' + pattern,
                    f'{label}: wrong sanitizer regex argument')
        suite = row['suite']
        if suite != 'gru':
            p = sanitizers.SUITES[suite]['profile']
            ids = [c['id'] for c in references[p]['cases']]
            require(row.get('oracle_case_ids') == ids, f'{label}/{suite}: reduced metadata case selection')
            receipts = sanitizers.json_receipts(text, 'specialty_head_receipt')
            require(len(receipts) == 1 and receipts[0].get('backend') == 'cuda' and receipts[0].get('profile') == p and
                    receipts[0].get('case_ids') == ids and receipts[0].get('cases') == len(ids) and receipts[0].get('fixture_cases') == len(ids),
                    f'{label}/{suite}: actual log omitted frozen head cases')
        compact.append({'tool': row['tool'], 'suite': suite, 'elapsed_seconds': row['elapsed_seconds'],
                        'oracle_cases': len(row.get('oracle_case_ids', [])), 'error_counts': actual['sanitizer_error_counts'],
                        'racecheck_counts': actual['racecheck_counts'], 'leak_counts': actual['sanitizer_leak_counts']})
    return {'tools': list(tools), 'kernel_regex': pattern, 'all_kernel_coverage': pattern is None,
            'vendor_library_scope': scope['vendor_library_scope'], 'lanes': compact, 'sanitizer': report['sanitizer']}


def validate_regressions(text, references, baseline_exception=None):
    summaries = list(sanitizers.SUMMARY.finditer(text))
    require(summaries, 'regressions: missing runtime summary')
    total = dict(selected=0, passed=0, skipped=0, failed=0, leaked=0)
    for match in summaries:
        selected, passed_count, skipped, failed, leaked = [int(v or 0) for v in match.groups()]
        require(leaked == 0 and selected == passed_count + skipped + failed, 'regressions: failed/leaked tests')
        for key, value in [('selected', selected), ('passed', passed_count), ('skipped', skipped), ('failed', failed), ('leaked', leaked)]:
            total[key] += value
    require(total['passed'] > 0 and not re.search(r'(?m)^failed command:|^Build Summary:.*\bfailed\b|transitive failure', text), 'regressions: failing build/test output')
    if baseline_exception is None:
        require(total['failed'] == 0 and not re.search(r'(?m)\.\.\.FAIL\b|^FAIL\b', text), 'regressions: failed/leaked tests')
        total['status'] = 'passed'
    else:
        require(isinstance(baseline_exception, VerifiedBaseline) and baseline_exception.source_sha256 == FROZEN_SOURCE
                and baseline_exception.failing_test == BASELINE_TEST
                and baseline_exception.current_log_sha256 == hashlib.sha256(text.encode()).hexdigest(), 'regressions: unverified or different baseline proof')
        require(total['failed'] == 1, 'regressions: baseline permits exactly one failed test')
        failed_names = [name for name, body in test_sections(text) if re.search(r'^FAIL(?:\s|$)', body, re.M)]
        require(failed_names == [BASELINE_TEST], 'regressions: baseline does not permit another failed test')
        observed_baseline_numbers(text)
        total['status'] = 'documented_out_of_scope_baseline_failure'
        total['baseline_exception'] = {'test': BASELINE_TEST, 'native_q8_qualified': False,
            'raw_result_remains_failed': True, 'absolute_error': BASELINE_NUMBERS['native_q8'],
            'unchanged_absolute_threshold': .05, 'historical_log_sha256': BASELINE_HISTORICAL_SHA,
            'preexisting_documentation_sha256': BASELINE_DOCUMENT_SHA}

    for spec in sanitizers.SUITES.values():
        require(re.search(r'^\d+/\d+ .*' + re.escape(spec['filter']) + r'\.\.\.(?!SKIP)', text, re.M), f"regressions: required CUDA test missing/skipped: {spec['filter']}")
    receipts = sanitizers.json_receipts(text, 'specialty_http_receipt')
    require(len(receipts) == 2 and {r.get('profile') for r in receipts} == set(http.PROFILES), 'regressions: missing CUDA public HTTP receipts')
    for row in receipts:
        sanitizers.validate_http_receipt(row, row['profile'], len(references[row['profile']]['cases']))
        require([r['id'] for r in row['timings']] == [c['id'] for c in references[row['profile']]['cases']], 'regressions: incomplete internal timing/case receipts')
    total['skipped_tests'] = re.findall(r'^\d+/\d+ (.*?)\.\.\.SKIP$', text, re.M)
    return total, receipts


def validate_build(report, expected_server_sha, expected_test_sha, evidence, baseline_exception=None):
    passed(report, 'build')
    require(report.get('source_sha256') == FROZEN_SOURCE and report.get('zig_version') == '0.17.0', 'build: wrong frozen source/compiler')
    binary_association(report, 'server_binary', expected_server_sha, 'build')
    binary_association(report, 'test_binary', expected_test_sha, 'build')
    require(set(report.get('builds', {})) == {'server', 'tests'}, 'build: missing server/test build receipt')
    for name, row in report['builds'].items():
        require(isinstance(row.get('command'), list) and row['command'], f'build/{name}: missing command')
        if row.get('returncode') != 0:
            require(name == 'tests' and row.get('returncode') == 1 and isinstance(baseline_exception, VerifiedBaseline)
                    and baseline_exception.source_sha256 == FROZEN_SOURCE and baseline_exception.failing_test == BASELINE_TEST
                    and row.get('compile_returncode') == 0
                    and row.get('log_sha256') == baseline_exception.current_log_sha256
                    and Path(row['command'][0]).resolve() == Path(report['test_binary'].get('path', '')).resolve(),
                    f'build/{name}: nonzero command lacks the exact verified native Q8 baseline exception')
        flags = row.get('effective_flags', {})
        require(flags.get('cuda') is True and flags.get('metal') is False and flags.get('cuda_artifacts') == 'fatbin' and
                flags.get('optimize') == 'ReleaseFast', f'build/{name}: wrong backend/artifact/optimization flags')
        if 'compile_returncode' in row:
            require(row['compile_returncode'] == 0 and isinstance(row.get('compile_command'), list), f'build/{name}: compiler did not succeed')
        evidence.log(row)
    return {'zig_version': report['zig_version'], 'builds': report['builds']}


def assemble(args):
    evidence = Evidence(args.output)
    manifest, references = http.verify_inputs(args.artifacts_root)
    manifest_sha = hashlib.sha256(evidence.read(http.MANIFEST)).hexdigest()
    server_sha, test_sha = http.digest_file(args.server), http.digest_file(args.test_binary)
    frozen = evidence.document(args.artifacts_root / 'production-source-frozen.json')
    require(frozen.get('source_sha256') == FROZEN_SOURCE, 'frozen source: wrong campaign fingerprint')
    current = sanitizers.source_receipt()
    require(current == {k: v for k, v in frozen.items() if k != 'captured_unix_seconds'}, 'frozen source: workspace differs from qualified inputs')
    regression_text = evidence.read(args.regression_log).decode()
    exception_path = getattr(args, 'baseline_exception', None)
    exception = validate_baseline_exception(evidence.document(exception_path), regression_text, evidence, frozen) if exception_path else None
    reports = {key: evidence.document(args.artifacts_root / filename) for key, filename in FINAL_FILES.items()}
    build = validate_build(reports['build'], server_sha, test_sha, evidence, exception)
    artifact = reports['artifacts']
    passed(artifact, 'CUDA artifacts')
    require(artifact.get('source_sha256') == FROZEN_SOURCE and artifact.get('cuda_compiler_version') == '13.2.86'
            and artifact.get('returncode') == 0 and '--check' in artifact.get('command', []) and '--all' in artifact['command'], 'CUDA artifacts: incomplete pinned freshness verification')
    actual_artifacts = {name: http.digest_file(http.PACKAGE / 'src/ops/cuda/artifacts' / name) for name in sanitizers.ARTIFACT_NAMES}
    require(artifact.get('cuda_artifact_files') == actual_artifacts, 'CUDA artifacts: recorded hashes differ from current bundle')
    artifact_log = evidence.log(artifact)
    require(all(f'CUDA {name} artifact is up to date:' in artifact_log for name in ('portable', 'fatbin', 'sm89')), 'CUDA artifacts: missing successful freshness log outcomes')
    for key in ('managed',):
        require(reports[key].get('artifact_manifest_sha256') == manifest_sha, f'{key}: manifest SHA differs')
    gpu_keys = ('soak', 'http', 'http_batch8', 'memory', 'sync', *('policy_' + m for m in POLICY_MODES))
    hardware = None
    for key in gpu_keys:
        manifest_binding(reports[key], manifest, manifest_sha, key)
        if key != 'soak':
            require(reports[key].get('cuda_artifact_files') == actual_artifacts, f'{key}: CUDA artifact SHA differs')
        observed = reports[key].get('hardware', {})
        require('NVIDIA GB10' in observed.get('nvidia_smi', '') and observed.get('machine') == 'aarch64', f'{key}: missing GB10 hardware identity')
        require(hardware is None or hardware == observed, f'{key}: mixed hardware identities')
        hardware = observed
    harnesses = {'managed': 'qualify_managed_install.py', 'drift': 'qualify_artifact_drift.py',
                 'soak': 'qualify_soak.py', 'http': 'qualify_http.py', 'http_batch8': 'qualify_http.py',
                 'memory': 'qualify_cuda_sanitizers.py', 'sync': 'qualify_cuda_sanitizers.py',
                 **{'policy_' + m: 'qualify_http.py' for m in POLICY_MODES}}
    for key, filename in harnesses.items():
        # Sanitizer sources are captured inside the bound source inventory.
        recorded = reports[key].get('harness_sha256') if key not in ('memory', 'sync') else reports[key]['source']['files'].get(str(Path(__file__).resolve().with_name(filename).relative_to(http.REPO)))
        require(recorded == http.digest_file(Path(__file__).with_name(filename)), f'{key}: harness source changed after execution')
    managed_summary = validate_managed(reports['managed'], manifest, server_sha, evidence)
    drift_summary = validate_drift(reports['drift'], manifest, references, server_sha, evidence)
    baseline = validate_http(reports['http'], references, server_sha, evidence, 'HTTP singleton')
    batch = validate_http(reports['http_batch8'], references, server_sha, evidence, 'HTTP batch8', batch=8)
    policies = {mode: validate_http(reports['policy_' + mode], references, server_sha, evidence,
                'policy/' + mode, mode=mode, generated='1', benchmark=False) for mode in POLICY_MODES}
    soak_summary = validate_soak(reports['soak'], manifest, references, server_sha, evidence)
    memory = validate_sanitizers(reports['memory'], ('memcheck', 'initcheck'), None, frozen, test_sha, server_sha, references, evidence, 'memory sanitizers')
    sync = validate_sanitizers(reports['sync'], ('racecheck', 'synccheck'), '^termite_.*', frozen, test_sha, server_sha, references, evidence, 'synchronization sanitizers')
    require(reports['memory']['sanitizer'] == reports['sync']['sanitizer'], 'sanitizers: mixed tool versions/injection libraries')
    regressions, physical = validate_regressions(regression_text, references, exception)
    runtime_libraries = None
    runtime_path = args.artifacts_root / 'production-final-runtime-libraries.json'
    if runtime_path.exists():
        runtime_libraries = evidence.document(runtime_path)
        require(runtime_libraries.get('format_version') == 1 and runtime_libraries.get('status') == 'captured'
                and runtime_libraries.get('processes') and all(p.get('server_binary_sha256') == server_sha
                    for p in runtime_libraries['processes']), 'runtime libraries: candidate image association differs')
    # Recheck the observed source and executables after collecting evidence.
    require(http.digest_file(args.server) == server_sha and http.digest_file(args.test_binary) == test_sha and
            sanitizers.source_receipt() == current, 'assembler inputs changed while collecting receipts')
    return {'format_version': 1, 'status': 'passed', 'campaign': 'original FP32 GLiNER2 specialty production on NVIDIA GB10',
        'assembled_unix_seconds': time.time(), 'source': {'git_commit': frozen['git_commit'], 'source_sha256': FROZEN_SOURCE,
            'source_file_count': len(frozen['files']), 'binding_scope': frozen['binding_scope']},
        'binaries': {'server': {'path': os.path.relpath(args.server, args.output.parent), 'sha256': server_sha},
                     'tests': {'path': os.path.relpath(args.test_binary, args.output.parent), 'sha256': test_sha}},
        'build': build, 'cuda_compiler_version': artifact['cuda_compiler_version'], 'cuda_artifact_files': actual_artifacts,
        'hardware': hardware, 'artifact_manifest_sha256': manifest_sha, 'model_pins': manifest['models'],
        'managed_install': managed_summary, 'artifact_drift': drift_summary, 'soak': soak_summary,
        'public_http': baseline, 'public_http_batch8': batch,
        'diagnostic_policies': {mode: {k: v for k, v in row.items() if k != 'timings'} for mode, row in policies.items()},
        'sanitizers': {'memory': memory, 'synchronization': sync,
            'scope': 'memcheck/initcheck cover unfiltered kernels; racecheck/synccheck cover ^termite_.* project kernels. Unmatched vendor-library race/synchronization instrumentation is excluded.',
            'complete_unfiltered_four_tool_campaign': False},
        'regressions': regressions, 'cuda_owned_buffer_receipts': physical,
        'memory_scope': 'Owned CUDA buffers and admission reservations; vendor/driver-private allocations excluded. Host process-group PSS is reported separately.',
        'runtime_library_provenance': runtime_libraries,
        'metal_qualification': {'executed': False, 'qualified': False},
        'reproducible_build_attested': False, 'references': list(evidence.references.values())}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--artifacts-root', type=Path, default=http.REPO / '.specialty-artifacts')
    parser.add_argument('--test-binary', required=True, type=Path)
    parser.add_argument('--server', required=True, type=Path)
    parser.add_argument('--regression-log', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--baseline-exception', type=Path, help='Pinned proof for the sole documented pre-existing native Q8 failure; strict by default')
    args = parser.parse_args()
    for name in ('artifacts_root', 'test_binary', 'server', 'regression_log', 'output'):
        setattr(args, name, getattr(args, name).resolve())
    if args.baseline_exception is not None:
        args.baseline_exception = args.baseline_exception.resolve()
    try:
        result = assemble(args)
    except Exception as error:
        result = {'format_version': 1, 'status': 'failed', 'error': str(error)}
        print(str(error), file=sys.stderr)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    pending = args.output.with_name(args.output.name + '.pending')
    pending.write_text(json.dumps(result, indent=2) + '\n')
    pending.replace(args.output)
    return 0 if result['status'] == 'passed' else 1


if __name__ == '__main__':
    sys.exit(main())
