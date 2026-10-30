#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Compare existing GB10 FP32 HTTP and pinned upstream benchmark receipts.

Every supplied timing row must have exactly one counterpart. No GPU work is
performed. A slower or tied case fails the gate and remains in the output.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path
import statistics
import sys

import qualify_http as http

HERE = Path(__file__).resolve().parent
HTTP_SCOPE = ("Loopback HTTP request send through complete response read; separate TCP request per sample; "
              "excludes model load, validation, warmups, and client JSON decode/checks")
TORCH_SCOPE = "Public upstream preprocessing, model, decoder and formatting; excludes model load"
BOUNDARY = ("Antfly includes loopback HTTP transport and response serialization; PyTorch is an in-process "
            "public API call including preprocessing, model, decoding and formatting. Both exclude model load. "
            "This is a conservative HTTP-versus-in-process comparison, not a GPU-kernel-only benchmark.")


def require(condition, message):
    if not condition:
        raise ValueError(message)


def read(path):
    path = Path(path).resolve()
    raw = path.read_bytes()
    return json.loads(raw), {"path": str(path), "sha256": hashlib.sha256(raw).hexdigest()}


def positive(value, label):
    require(type(value) in (int, float) and math.isfinite(value) and value > 0,
            f"{label}: expected positive finite number")
    return value


def integer(value, label, minimum=1):
    require(type(value) is int and value >= minimum, f"{label}: expected integer >= {minimum}")
    return value


def fingerprint(value, label):
    require(isinstance(value, str) and len(value) == 64 and all(c in '0123456789abcdef' for c in value),
            f"{label}: expected SHA256")
    return value


def references(manifest):
    result = {}
    for profile, pin in manifest['models'].items():
        path = http.MANIFEST.parent / pin['oracle']['path']
        raw = path.read_bytes()
        require(len(raw) == pin['oracle']['size_bytes'] and
                hashlib.sha256(raw).hexdigest() == pin['oracle']['sha256'], f"{profile}: frozen oracle hash differs")
        for case in json.loads(raw)['cases']:
            result[profile, case['id']] = case
    return result


def timing(row, profile, refs, minimum_warmups, minimum_iterations):
    case_id = row.get('id')
    require((profile, case_id) in refs, f"unknown frozen case: {profile}/{case_id}")
    label = f"{profile}/{case_id}"
    batch = integer(row.get('request_items'), label + ' request_items')
    physical = integer(row.get('physical_batch_items'), label + ' physical_batch_items')
    require(physical == (min(8, batch) if profile == 'gliguard' else 1), label + ': unexpected physical batch')
    tokens = integer(row.get('prepared_tokens_per_item'), label + ' prepared_tokens')
    require(tokens == len(refs[profile, case_id]['input_ids']), label + ': prepared tokens differ from oracle')
    warmups = integer(row.get('warmups'), label + ' warmups', 0)
    iterations = integer(row.get('iterations'), label + ' iterations')
    require(warmups >= minimum_warmups and iterations >= minimum_iterations, label + ': insufficient warmups/iterations')
    latency = row.get('latency_ms', {})
    samples = latency.get('samples')
    require(isinstance(samples, list) and len(samples) == iterations, label + ': raw sample count differs')
    for sample in samples:
        positive(sample, label + ' sample')
    median = statistics.median(samples)
    p95 = http.quantile(samples, .95)
    for key, expected in [('median', median), ('p95', p95)]:
        actual = positive(latency.get(key), label + ' ' + key)
        require(math.isclose(actual, expected, rel_tol=1e-12, abs_tol=1e-12), label + ': recorded ' + key + ' differs from raw samples')
    for key, expected in [('items_per_second', batch * 1000 / median),
                          ('prepared_tokens_per_second', batch * tokens * 1000 / median)]:
        require(math.isclose(positive(row.get(key), label + ' ' + key), expected, rel_tol=1e-12),
                label + ': inconsistent throughput')
    return (profile, case_id, batch, physical, tokens), {'warmups': warmups, 'iterations': iterations,
            'latency_ms': {'median': median, 'p95': p95, 'samples': samples}}


def validate_http(report, manifest, manifest_sha, refs):
    require(report.get('format_version') == 1 and report.get('status') == 'passed', 'HTTP: receipt did not pass')
    require(report.get('artifact_pins') == manifest and report.get('manifest_sha256') == manifest_sha,
            'HTTP: model/runtime/upstream pins differ')
    require(report.get('harness_sha256') == http.digest_file(HERE / 'qualify_http.py'), 'HTTP: harness source hash differs')
    require(report.get('timing_scope') == HTTP_SCOPE, 'HTTP: unexpected timing scope')
    server = report.get('server_binary', {})
    sha = fingerprint(server.get('sha256'), 'HTTP binary')
    for phase in ('before', 'after'):
        require(report.get('running_binary_' + phase, {}).get('sha256') == sha, 'HTTP: running binary changed')
        http.check_instances(report['backend_instances_' + phase])
    before, after = (http.check_instances(report['backend_instances_' + phase]) for phase in ('before', 'after'))
    require(all(before[p]['id'] == after[p]['id'] for p in http.PROFILES), 'HTTP: runtime incarnation changed')
    require(type(report.get('server_returncode')) is int and report['server_returncode'] in (0, -15),
            'HTTP: server did not exit cleanly')
    hardware = report.get('hardware', {})
    require('NVIDIA GB10' in hardware.get('nvidia_smi', '') and hardware.get('machine') == 'aarch64',
            'HTTP: missing GB10 identity')
    require(hardware.get('platform') and hardware.get('python') == manifest['runtime']['python'], 'HTTP: host runtime differs')
    log = Path(report['server_log'])
    require(http.digest_file(log) == report.get('server_log_sha256'), 'HTTP: policy log hash differs')
    policies = http.parse_cuda_policy(log.read_text())
    for phase in ('before', 'after'):
        recorded = report.get('cuda_policy_' + phase, {})
        for profile in http.PROFILES:
            require(profile in policies and profile in recorded, 'HTTP: missing FP32 policy')
            for key, expected in [('weights', 'f32'), ('attention', 'f32'), ('diagnostic_overrides', 'ignored')]:
                require(recorded[profile].get(key) == policies[profile].get(key) == expected, 'HTTP: non-FP32 policy')
    seen = set()
    for validation in report.get('validation', []):
        key = validation.get('profile'), validation.get('id')
        require(key in refs and key not in seen, 'HTTP: unknown or duplicate validation case')
        http.check_response(key[0], refs[key], validation['response'])
        seen.add(key)
    require(seen == set(refs), 'HTTP: incomplete frozen correctness validation')
    return sha, hardware


def validate_torch(report, manifest):
    profile = report.get('profile')
    require(report.get('format_version') == 1 and profile in http.PROFILES, 'PyTorch: unknown receipt/profile')
    require(report.get('model') == manifest['models'][profile] and report.get('upstream') == manifest['upstream'],
            'PyTorch: frozen model/upstream/source pins differ')
    require(report.get('benchmark_script_sha256') == http.digest_file(HERE / 'benchmark_oracle.py'),
            'PyTorch: benchmark source hash differs')
    require(report.get('timing_scope') == TORCH_SCOPE, 'PyTorch: unexpected timing scope')
    runtime = report.get('runtime', {})
    for key, expected in [('device', 'cuda'), ('dtype', 'float32'), ('float32_matmul_precision', 'highest'),
                          ('tf32', False), ('cudnn_allow_tf32', False), ('capture_hooks', False),
                          ('matches_frozen_oracle_runtime', True)]:
        require(runtime.get(key) == expected and type(runtime.get(key)) is type(expected), 'PyTorch: invalid ' + key)
    packages = runtime.get('packages', {})
    require(set(packages) == set(manifest['runtime']['packages']) and all(
        isinstance(packages[k], str) and packages[k].split('+')[0] == v for k, v in manifest['runtime']['packages'].items()),
        'PyTorch: frozen runtime package versions differ')
    require(runtime.get('python', '').split()[0] == manifest['runtime']['python'], 'PyTorch: Python version differs')
    require(runtime.get('cuda_device') == 'NVIDIA GB10' and runtime.get('platform'), 'PyTorch: missing GB10 identity')
    integer(runtime.get('threads'), 'PyTorch threads')
    integer(runtime.get('interop_threads'), 'PyTorch interop_threads')
    return profile, runtime


def compare(http_paths, pytorch_paths, minimum_warmups=10, minimum_iterations=50):
    integer(minimum_warmups, 'minimum warmups', 0)
    integer(minimum_iterations, 'minimum iterations')
    manifest, manifest_receipt = read(http.MANIFEST)
    refs = references(manifest)
    report = {'format_version': 1, 'status': 'failed', 'minimum_warmups': minimum_warmups,
              'minimum_iterations': minimum_iterations, 'timing_boundary': BOUNDARY,
              'comparison_script_sha256': http.digest_file(Path(__file__)),
              'gate': 'Every matched Antfly median must be strictly less than the PyTorch median.',
              'manifest': manifest_receipt, 'receipts': {'http': [], 'pytorch': []}, 'cases': []}
    antfly, torch = {}, {}
    binary_sha, hardware, torch_runtime = None, None, None
    for kind, paths in [('http', http_paths), ('pytorch', pytorch_paths)]:
        require(paths, 'missing ' + kind + ' receipts')
        for path in paths:
            receipt, binding = read(path)
            report['receipts'][kind].append(binding)
            if kind == 'http':
                sha, observed = validate_http(receipt, manifest, manifest_receipt['sha256'], refs)
                require(binary_sha is None or sha == binary_sha, 'HTTP: mixed candidate binaries')
                require(hardware is None or observed == hardware, 'HTTP: mixed GB10 hardware identities')
                binary_sha, hardware = sha, observed
                rows, target = receipt.get('timings'), antfly
                binding.update(server_binary=receipt['server_binary'], timing_scope=receipt['timing_scope'], hardware=observed)
            else:
                profile, observed = validate_torch(receipt, manifest)
                require(torch_runtime is None or observed == torch_runtime, 'PyTorch: mixed runtime settings')
                torch_runtime = observed
                # Newer receipts may carry the fuller nvidia-smi identity. Older
                # upstream receipts have only device name and host platform.
                if 'hardware' in receipt:
                    require(receipt['hardware'] == hardware, 'PyTorch: GPU identity differs')
                require(observed['platform'] == hardware['platform'], 'PyTorch: host platform differs from HTTP')
                rows, target = receipt.get('cases'), torch
                binding.update(profile=profile, timing_scope=receipt['timing_scope'], runtime=observed)
            require(isinstance(rows, list) and rows, kind + ': no timing rows')
            for row in rows:
                key, values = timing(row, row.get('profile') if kind == 'http' else profile, refs,
                                     minimum_warmups, minimum_iterations)
                require(key not in target, kind + ': duplicate timing case ' + str(key))
                target[key] = dict(values, receipt_sha256=binding['sha256'])
    require(set(antfly) == set(torch), 'unmatched timing rows: HTTP only=' + str(sorted(set(antfly) - set(torch))) +
            '; PyTorch only=' + str(sorted(set(torch) - set(antfly))))
    for key in sorted(antfly):
        a, p = antfly[key], torch[key]
        report['cases'].append(dict(zip(('profile', 'id', 'request_items', 'physical_batch_items', 'prepared_tokens_per_item'), key),
            antfly=a, pytorch=p, speedup=p['latency_ms']['median'] / a['latency_ms']['median'],
            passed=a['latency_ms']['median'] < p['latency_ms']['median']))
    report.update(server_binary_sha256=binary_sha, hardware=hardware, matched_cases=len(antfly),
        hardware_identity_scope='HTTP receipts bind the full nvidia-smi identity. PyTorch binds GB10 name and host platform; GPU UUID/driver equality is checked only when its receipt supplies hardware.',
        coverage=[{k: row[k] for k in ('profile', 'id', 'request_items')} for row in report['cases']],
        correctness_scope='HTTP revalidated against every frozen oracle case. PyTorch benchmark receipts carry no per-trial numerical outputs; the original numerical qualification gates remain separate.')
    failed = [row for row in report['cases'] if not row['passed']]
    if failed:
        report['error'] = 'Antfly was slower or tied for: ' + ', '.join(f"{r['profile']}/{r['id']}/b{r['request_items']}" for r in failed)
    else:
        report['status'] = 'passed'
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--http', type=Path, action='append', required=True)
    parser.add_argument('--pytorch', type=Path, action='append', required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--minimum-warmups', type=int, default=10)
    parser.add_argument('--minimum-iterations', type=int, default=50)
    args = parser.parse_args(argv)
    require(args.output.resolve() not in {p.resolve() for p in args.http + args.pytorch}, 'output would overwrite an input receipt')
    try:
        report = compare(args.http, args.pytorch, args.minimum_warmups, args.minimum_iterations)
    except Exception as error:
        report = {'format_version': 1, 'status': 'failed', 'error': str(error), 'timing_boundary': BOUNDARY,
                  'comparison_script_sha256': http.digest_file(Path(__file__)),
                  'minimum_warmups': args.minimum_warmups, 'minimum_iterations': args.minimum_iterations,
                  'inputs': {'http': [str(p.resolve()) for p in args.http], 'pytorch': [str(p.resolve()) for p in args.pytorch]}}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, allow_nan=False) + '\n')
    print(report.get('error', f"Passed {report.get('matched_cases', 0)} matched cases"), file=sys.stderr)
    return 0 if report['status'] == 'passed' else 1


if __name__ == '__main__':
    sys.exit(main())
