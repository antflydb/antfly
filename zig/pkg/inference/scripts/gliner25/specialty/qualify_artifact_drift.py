#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""CPU-only production HTTP checks of specialty artifact rejection and recovery.

Clones contain private sidecars and hard-linked immutable original weights.
Wrong-weight/FP16 probes use private sparse SafeTensors payloads, never a full
checkpoint copy or writes through the original hard link. All clones and the
owned loopback process are removed on success or failure.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import shutil
import signal
import socket
import struct
import subprocess
import time
import traceback
import urllib.error
import urllib.request

import qualify_http as public_http
import qualify_managed_install as managed_install

HERE = Path(__file__).resolve()
PACKAGE = HERE.parents[3]
REPO = PACKAGE.parents[2]
MANIFEST = PACKAGE / 'testdata/gliner25/specialty/manifest.json'
COMPLETE = '.antfly-download-complete.json'
EXPECTED_REJECTION_MESSAGE = 'GLiNER specialty artifacts must match the qualified FP32 checkpoint and provenance'
CONTROLS = ('valid', 'asserted_valid', 'managed_valid')
NEGATIVES = ('raw_config_drift', 'asserted_config_drift', 'managed_config_drift',
             'managed_wrong_revision', 'managed_missing_revision', 'managed_wrong_format',
             'managed_missing_format', 'asserted_wrong_profile', 'asserted_wrong_capability',
             'asserted_decide_claim', 'asserted_malformed_capabilities', 'asserted_malformed_tasks',
             'raw_wrong_weights', 'raw_fp16')


def sha256(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(8 << 20), b''):
            digest.update(block)
    return digest.hexdigest()


def source_for_profile(root, profile, pin):
    for candidate in (root / profile, root / pin['repository']):
        if (candidate / 'model.safetensors').is_file():
            return candidate
    raise ValueError(f'original model absent: {profile} below {root}')


def sparse_weights(original, target, *, fp16=False):
    with original.open('rb') as source:
        prefix = source.read(8)
        header_size = struct.unpack('<Q', prefix)[0]
        header_bytes = source.read(header_size)
    header = json.loads(header_bytes)
    if fp16:
        cursor = 0
        for name, tensor in header.items():
            if name == '__metadata__':
                continue
            if tensor['dtype'] != 'F32':
                raise ValueError('original tensor inventory is not FP32')
            size = math.prod(tensor['shape']) * 2
            tensor['dtype'] = 'F16'
            tensor['data_offsets'] = [cursor, cursor + size]
            cursor += size
        header_bytes = json.dumps(header, separators=(',', ':')).encode()
        header_bytes += b' ' * (-len(header_bytes) % 8)
        prefix = struct.pack('<Q', len(header_bytes))
        total = 8 + len(header_bytes) + cursor
    else:
        total = original.stat().st_size
    with target.open('wb') as output:
        output.write(prefix)
        output.write(header_bytes)
        output.truncate(total)
    target.chmod(0o444)
    return {'precision': 'fp16' if fp16 else 'fp32', 'size_bytes': total,
            'sha256': sha256(target), 'sparse_disk_bytes': target.stat().st_blocks * 512}


def prepare(source, models_root, profile, variant, pin):
    target = models_root / f'{variant}_{profile}'
    target.mkdir(parents=True)
    weight_info = None
    for name in pin['files']:
        copied = target / name
        copied.parent.mkdir(parents=True, exist_ok=True)
        if name == 'model.safetensors':
            if variant in ('raw_wrong_weights', 'raw_fp16'):
                weight_info = sparse_weights(source / name, copied, fp16=variant == 'raw_fp16')
                if weight_info['sha256'] == pin['files'][name]['sha256']:
                    raise ValueError('negative weights unexpectedly equal the original')
            else:
                os.link(source / name, copied)
        else:
            copied.write_bytes((source / name).read_bytes())
            copied.chmod(0o444)
    config = target / 'config.json'
    if variant.endswith('config_drift'):
        original = config.read_bytes()
        changed = original.replace(b'5.', b'6.', 1)
        if changed == original or len(changed) != len(original):
            raise ValueError('config probe must change bytes without changing size')
        config.chmod(0o600)
        config.write_bytes(changed)
        config.chmod(0o444)
    if variant.startswith('asserted_'):
        claim = ('gliguard' if profile == 'pii' else 'pii') if variant == 'asserted_wrong_profile' else profile
        declaration = {'type': 'extractor', 'tasks': ['extract'], 'inputs': ['text'],
            'capabilities': ['named_entity_recognition' if profile == 'pii' else 'classification'],
            'gliner_specialty_profile': claim}
        if variant == 'asserted_wrong_capability':
            declaration['capabilities'] = ['relations']
        elif variant == 'asserted_decide_claim':
            declaration['capabilities'] = ['typed_decisions']
            declaration['tasks'] = ['decide']
        elif variant == 'asserted_malformed_capabilities':
            declaration['capabilities'] = False
        elif variant == 'asserted_malformed_tasks':
            declaration['tasks'] = False
        (target / 'model_manifest.json').write_text(json.dumps(declaration))
    elif variant.startswith('managed_'):
        if variant == 'managed_valid':
            (target / 'model_manifest.json').write_text(json.dumps(managed_install.expected_manifest(profile)))
        owner, name = pin['repository'].split('/', 1)
        source_identity = {'owner': owner, 'name': name, 'variant': 'auto',
            'requested_revision': pin['revision'], 'resolved_revision': pin['revision'],
            'selected_format': 'safetensors'}
        if variant == 'managed_wrong_revision':
            source_identity['resolved_revision'] = '0' * 40
        elif variant == 'managed_missing_revision':
            del source_identity['resolved_revision']
        elif variant == 'managed_wrong_format':
            source_identity['selected_format'] = 'gguf'
        elif variant == 'managed_missing_format':
            del source_identity['selected_format']
        receipt = {'version': 2, 'source': source_identity,
            'artifacts': [{'path': name, 'size': value['size_bytes'], 'sha256': value['sha256']}
                          for name, value in pin['files'].items()]}
        if variant == 'managed_valid':
            generated = target / 'model_manifest.json'
            receipt['artifacts'].append({'path': generated.name, 'size': generated.stat().st_size, 'sha256': sha256(generated)})
        (target / COMPLETE).write_text(json.dumps(receipt))
    return {'profile': profile, 'variant': variant, 'model': target.name,
        'config_size_bytes': config.stat().st_size, 'config_sha256': sha256(config),
        'weight': weight_info or {**pin['files']['model.safetensors'], 'hard_linked': True,
            'original_inode': (source / 'model.safetensors').stat().st_ino,
            'probe_inode': (target / 'model.safetensors').stat().st_ino},
        'tokenizer_copy_read_only': (target / 'tokenizer.json').stat().st_mode & 0o222 == 0}


def call(base, path, payload=None, *, method=None, timeout=120):
    body = json.dumps(payload).encode() if payload is not None else None
    request = urllib.request.Request(base + path, data=body,
        headers={'Content-Type': 'application/json'} if body is not None else {},
        method=method or ('POST' if body is not None else 'GET'))
    start = time.perf_counter()
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            status, raw = response.status, response.read()
    except urllib.error.HTTPError as response:
        status, raw = response.code, response.read()
    try:
        result = json.loads(raw)
    except ValueError:
        result = {'raw': raw.decode(errors='replace')}
    return {'status': status, 'body': result, 'elapsed_seconds': time.perf_counter() - start}


def terminate(process):
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        process.wait(timeout=10)


def qualify(args, report):
    manifest = json.loads(MANIFEST.read_text())
    variants = args.variant if args.variant is not None else [*CONTROLS, *NEGATIVES, 'managed_valid']
    if args.models_root.exists():
        raise ValueError(f'probe clone directory already exists; refusing to overwrite: {args.models_root}')
    args.models_root.parent.mkdir(parents=True, exist_ok=True)
    args.models_root.mkdir()  # Atomic ownership: a concurrent probe cannot share/clean this tree.
    report.update({'server_binary': {'path': str(args.server), 'sha256': sha256(args.server)},
        'harness_sha256': sha256(HERE), 'cpu_only': True,
        'backend_environment': {'ANTFLY_INFERENCE_REQUIRED_BACKEND': 'native', 'CUDA_VISIBLE_DEVICES': ''},
        'scope': 'filtered_diagnostic' if args.variant is not None else 'full_matrix',
        'selected_variants': variants, 'expected_load_extraction_checks': len(variants) * len(manifest['models']) * 2,
        'prepared': [], 'cases': [], 'validation_failures': []})
    process = None
    try:
        sources, references = {}, {}
        for profile, pin in manifest['models'].items():
            source = source_for_profile(args.artifacts_root, profile, pin)
            sources[profile] = source
            oracle_path = MANIFEST.parent / pin['oracle']['path']
            if sha256(oracle_path) != pin['oracle']['sha256'] or oracle_path.stat().st_size != pin['oracle']['size_bytes']:
                raise ValueError(f'frozen oracle drift: {oracle_path}')
            references[profile] = json.loads(oracle_path.read_text())['cases'][0]
            for name, expected in pin['files'].items():
                if (source / name).stat().st_size != expected['size_bytes'] or sha256(source / name) != expected['sha256']:
                    raise ValueError(f'original artifact drift: {source}/{name}')
            for variant in dict.fromkeys(variants):
                report['prepared'].append(prepare(source, args.models_root, profile, variant, pin))
        with socket.socket() as probe:
            probe.bind(('127.0.0.1', 0))
            port = probe.getsockname()[1]
        base = f'http://127.0.0.1:{port}'
        command = [str(args.server), 'run', '--host', '127.0.0.1', '--port', str(port),
            '--models-dir', str(args.models_root), '--max-loaded-models', '2', '--max-concurrent-requests', '1',
            '--kernel-jit-mode', 'off', '--host-budget-mb', '8192', '--backend-budget-mb', '8192',
            '--combined-budget-mb', '16384', '--scratch-budget-mb', '4096']
        environment = {**os.environ, **report['backend_environment']}
        log_path = args.output.with_suffix('.server.log')
        report['server_log'] = str(log_path)
        with log_path.open('wb') as log:
            process = subprocess.Popen(command, cwd=REPO, env=environment, stdout=log, stderr=subprocess.STDOUT,
                start_new_session=True)
            report['server_command'], report['server_pid'] = command, process.pid
            deadline = time.monotonic() + args.startup_timeout
            while True:
                if process.poll() is not None:
                    raise RuntimeError(f'server exited before ready: {process.returncode}')
                try:
                    response = call(base, '/ai/v1/models/instances', timeout=2)
                    if response['status'] == 200:
                        if response['body'].get('data') != []:
                            raise ValueError('unexpected preload or existing server state')
                        break
                except (OSError, ValueError):
                    pass
                if time.monotonic() > deadline:
                    raise TimeoutError('server startup')
                time.sleep(.1)
            report['initial_instances'] = response
            for profile in ('pii', 'gliguard'):
                for variant in variants:
                    model = f'{variant}_{profile}'
                    response = call(base, '/ai/v1/models/instances', {'model': model, 'task': 'extractor'}, timeout=args.timeout)
                    row = {'profile': profile, 'variant': variant, 'request': {'model': model, 'task': 'extractor'},
                        'response': response}
                    report['cases'].append(row)
                    if variant in CONTROLS:
                        if response['status'] != 200 or response['body'].get('backend') != 'native':
                            raise ValueError(f'valid native control/recovery failed: {row}')
                        row['unload'] = call(base, '/ai/v1/models/instances/' + response['body']['id'], method='DELETE')
                        if row['unload']['status'] != 200:
                            raise ValueError(f'native control unload failed: {row}')
                    elif response['status'] != 400 or response['body'].get('error') != 'INVALID_MODEL':
                        row.setdefault('validation_errors', []).append('model-instance load must reject as INVALID_MODEL')
                        report['validation_failures'].append({'profile': profile, 'variant': variant, 'path': 'load'})
                    # Start from an unloaded instance so rejection exercises the
                    # extraction dispatch preflight as well as actual session load.
                    case = references[profile]
                    extract_body = json.loads(public_http.request_body(profile, case))
                    extract_body['model'] = model
                    extraction = call(base, '/ai/v1/extract', extract_body, timeout=args.timeout)
                    row['cold_extraction'] = extraction
                    if variant in CONTROLS:
                        if extraction['status'] != 200:
                            raise ValueError(f'valid native extraction/recovery failed: {row}')
                        row['extraction_max_confidence_error'] = public_http.check_response(profile, case, extraction['body'])
                        instances = call(base, '/ai/v1/models/instances')
                        loaded = [item for item in instances['body'].get('data', []) if item.get('state') == 'loaded']
                        if len(loaded) != 1 or loaded[0].get('backend') != 'native':
                            raise ValueError(f'native extraction backend/instance mismatch: {instances}')
                        row['extraction_unload'] = call(base, '/ai/v1/models/instances/' + loaded[0]['id'], method='DELETE')
                        if row['extraction_unload']['status'] != 200:
                            raise ValueError(f'native extraction unload failed: {row}')
                    else:
                        # An unmanaged config whose digest no longer identifies
                        # either profile can fail the generic v2 feature preflight
                        # before mapped-weight validation. This is a safe model-
                        # stage rejection; declarations/managed provenance and
                        # recognized configs must still reject as INVALID_MODEL.
                        expected_code = ('UNSUPPORTED_EXTRACTION_FEATURE'
                                         if variant == 'raw_config_drift' else 'INVALID_MODEL')
                        row['cold_extraction_rejection_policy'] = {
                            'error': expected_code, 'stage': 'model',
                            'reason': ('Unrecognized unmanaged config rejected before session load'
                                       if variant == 'raw_config_drift' else
                                       'Recognized specialty artifacts or assertion/provenance rejected')}
                        if (extraction['status'] != 400 or extraction['body'].get('error') != expected_code
                                or extraction['body'].get('stage') != 'model'):
                            row.setdefault('validation_errors', []).append('cold extraction must reject with its allowed code at model stage')
                            report['validation_failures'].append({'profile': profile, 'variant': variant, 'path': 'cold_extraction', 'response': extraction})
                    row['instances_after'] = call(base, '/ai/v1/models/instances')
                    if any(item.get('state') == 'loaded' for item in row['instances_after']['body'].get('data', [])):
                        raise ValueError(f'failed probe retained a loaded model: {row}')
                    print(profile, variant, response['status'], response['body'].get('message', 'native control passed'), flush=True)
            report['original_artifacts_after'] = {}
            for profile, pin in manifest['models'].items():
                source = sources[profile]
                actual = {name: sha256(source / name) for name in pin['files']}
                if any(actual[name] != value['sha256'] for name, value in pin['files'].items()):
                    raise ValueError(f'probe changed original artifacts: {profile}')
                report['original_artifacts_after'][profile] = actual
            if report['validation_failures']:
                raise ValueError(f"artifact rejection contract failed in {len(report['validation_failures'])} checks; see validation_failures")
    finally:
        if process is not None:
            terminate(process)
            report['server_exit_code'] = process.returncode
            report['server_cleanup_complete'] = process.poll() is not None
        if args.models_root.exists():
            shutil.rmtree(args.models_root)
        report['probe_models_cleaned_up'] = not args.models_root.exists()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--server', type=Path, required=True)
    parser.add_argument('--artifacts-root', type=Path, default=REPO / '.specialty-artifacts')
    parser.add_argument('--models-root', type=Path, default=REPO / '.specialty-artifacts/production-drift-models',
                        help='Owned clone tree; must not already exist and is removed after the probe')
    parser.add_argument('--output', type=Path, default=REPO / '.specialty-artifacts/production-artifact-drift.json')
    parser.add_argument('--variant', action='append', choices=(*CONTROLS, *NEGATIVES),
                        help='Restrict to explicit variants for a partial diagnostic; default runs the entire matrix')
    parser.add_argument('--timeout', type=float, default=120)
    parser.add_argument('--startup-timeout', type=float, default=60)
    args = parser.parse_args()
    if args.timeout <= 0 or args.startup_timeout <= 0:
        parser.error('positive timeouts required')
    args.server = args.server.resolve(strict=True)
    args.artifacts_root = args.artifacts_root.resolve(strict=True)
    args.models_root, args.output = args.models_root.resolve(), args.output.resolve()
    if args.models_root == args.artifacts_root or args.artifacts_root.is_relative_to(args.models_root):
        parser.error('clone cleanup must not contain original artifacts')
    if args.output.is_relative_to(args.models_root):
        parser.error('receipt output must be outside the disposable clone tree')
    args.output.parent.mkdir(parents=True, exist_ok=True)
    report = {'format_version': 1, 'passed': False}
    try:
        qualify(args, report)
        report['passed'] = True
    except Exception:
        report['failure'] = traceback.format_exc()
        print(report['failure'], flush=True)
    finally:
        args.output.write_text(json.dumps(report, indent=2) + '\n')
    if not report['passed']:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
