#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Qualify official CLI pull/publication using verified original cached bytes.

The initial seed has no generated model_manifest.json. The real CLI resolves
Hub metadata at the built-in immutable revision, verifies all cached payloads,
synthesizes its manifest, and publishes the completion receipt transactionally.
No GPU is used. Existing verified upstream files are never rewritten by this
script; CLI cache-hit logs prove large payloads were not downloaded again.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import time
import traceback

HERE = Path(__file__).resolve()
PACKAGE = HERE.parents[3]
REPO = PACKAGE.parents[2]
MANIFEST = PACKAGE / 'testdata/gliner25/specialty/manifest.json'
PROFILES = ('pii', 'gliguard')
COMPLETE = '.antfly-download-complete.json'


def sha256(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(8 << 20), b''):
            digest.update(block)
    return digest.hexdigest()


def verify_upstream(directory, pin):
    observed = {}
    for name, expected in pin['files'].items():
        path = directory / name
        actual = {'size_bytes': path.stat().st_size, 'sha256': sha256(path)}
        if actual != expected:
            raise ValueError(f'upstream artifact drift: {directory}/{name}')
        observed[name] = actual
    return observed


def expected_manifest(profile):
    result = {'type': 'extractor', 'tasks': ['extract'], 'inputs': ['text'],
              'capabilities': ['named_entity_recognition' if profile == 'pii' else 'classification'],
              'gliner_specialty_profile': profile,
              'qualified_limits': {'specialty': {'sequence_tokens': 512,
                  'physical_batch_items': 1 if profile == 'pii' else 8}}}
    if profile == 'gliguard':
        result['gliner_classification_head'] = 'label_marker_mlp'
    return result


def seed_cached_inputs(source, destination, pin):
    """Only seed a new owned canonical directory; preserve existing installs."""
    if destination.exists():
        verify_upstream(destination, pin)
        if not (destination / COMPLETE).is_file():
            raise ValueError(f'existing install lacks a published receipt: {destination}')
        return {'created': False, 'preexisting_manifest': (destination / 'model_manifest.json').exists()}
    destination.mkdir(parents=True)
    for name in pin['files']:
        target = destination / name
        target.parent.mkdir(parents=True, exist_ok=True)
        os.link(source / name, target)
    owner, name = pin['repository'].split('/', 1)
    receipt = {'version': 2,
        'source': {'owner': owner, 'name': name, 'variant': 'auto',
                   'requested_revision': pin['revision'], 'resolved_revision': pin['revision'],
                   'selected_format': 'safetensors'},
        'artifacts': [{'path': name, 'size': value['size_bytes'], 'sha256': value['sha256']}
                      for name, value in pin['files'].items()]}
    (destination / COMPLETE).write_text(json.dumps(receipt, indent=2) + '\n')
    return {'created': True, 'preexisting_manifest': False,
            'seed_receipt_sha256': sha256(destination / COMPLETE),
            'seed_scope': 'Verified upstream cache bytes only; no Antfly model manifest'}


def verify_published(destination, profile, pin):
    for marker in ('.antfly-download-in-progress', '.antfly-download-plan.json'):
        if (destination / marker).exists():
            raise ValueError(f'unfinished managed publication marker: {destination}/{marker}')
    actual_files = verify_upstream(destination, pin)
    generated = json.loads((destination / 'model_manifest.json').read_text())
    if generated != expected_manifest(profile):
        raise ValueError(f'wrong generated specialty manifest for {profile}: {generated}')
    receipt = json.loads((destination / COMPLETE).read_text())
    owner, name = pin['repository'].split('/', 1)
    expected_source = {'owner': owner, 'name': name, 'variant': 'auto',
        'requested_revision': pin['revision'], 'resolved_revision': pin['revision'],
        'selected_format': 'safetensors'}
    if receipt.get('version') != 2 or receipt.get('source') != expected_source:
        raise ValueError(f'wrong immutable source receipt for {profile}: {receipt.get("source")}')
    artifacts = {item['path']: item for item in receipt['artifacts']}
    if len(artifacts) != len(receipt['artifacts']) or set(artifacts) != set(pin['files']) | {'model_manifest.json'}:
        raise ValueError(f'wrong or duplicate published artifact set: {profile}')
    for name, artifact in artifacts.items():
        path = destination / name
        if path.stat().st_size != artifact['size']:
            raise ValueError(f'receipt size mismatch: {name}')
        if artifact.get('sha256') is not None and sha256(path) != artifact['sha256']:
            raise ValueError(f'receipt digest mismatch: {name}')
    if not artifacts['model_manifest.json'].get('sha256'):
        raise ValueError('generated task manifest lacks a receipt hash')
    return {'model': pin['repository'], 'install_dir': str(destination), 'source': receipt['source'],
        'model_manifest': generated, 'model_manifest_sha256': sha256(destination / 'model_manifest.json'),
        'completion_receipt': receipt, 'completion_receipt_sha256': sha256(destination / COMPLETE),
        'original_files': actual_files,
        'weight_inode': (destination / 'model.safetensors').stat().st_ino}


def run_pull(args, profile, pin, repetition):
    log = args.output.with_name(args.output.stem + f'.{profile}.pull-{repetition}.log')
    command = [str(args.cli), 'pull', pin['repository'], '--models-dir', str(args.models_root)]
    environment = dict(os.environ)
    environment.pop('HF_TOKEN', None)
    environment['CUDA_VISIBLE_DEVICES'] = ''
    environment['ANTFLY_INFERENCE_REQUIRED_BACKEND'] = 'native'
    started = time.perf_counter()
    with log.open('wb') as output:
        result = subprocess.run(command, cwd=REPO, env=environment, stdout=output, stderr=subprocess.STDOUT,
                                timeout=args.timeout)
    if result.returncode != 0:
        raise RuntimeError(f'official CLI pull failed ({result.returncode}); see {log}')
    text = log.read_text(errors='replace')
    cached = set(re.findall(r'\[\d+/\d+\]\s+(\S+) cached \(', text))
    missing = set(pin['files']) - cached
    if missing:
        raise ValueError(f'pull did not prove cache validation for {sorted(missing)}; see {log}')
    expected_resolution = f"Resolved {pin['repository']}@{pin['revision']} to {pin['revision']}"
    if expected_resolution not in text:
        raise ValueError(f'pull did not resolve the built-in immutable pin; see {log}')
    return {'command': command, 'exit_code': result.returncode, 'elapsed_seconds': time.perf_counter() - started,
        'log': str(log), 'log_sha256': sha256(log), 'cache_verified_files': sorted(cached),
        'large_payload_network_downloads': 0, 'network_scope': 'Hub immutable metadata only; all model files cache-verified'}


def qualify(args, report):
    manifest = json.loads(MANIFEST.read_text())
    report.update({'cli': {'path': str(args.cli), 'sha256': sha256(args.cli)},
        'harness_sha256': sha256(HERE), 'artifact_manifest_sha256': sha256(MANIFEST),
        'model_names': {profile: manifest['models'][profile]['repository'] for profile in PROFILES},
        'models_root': str(args.models_root), 'backend': 'native', 'gpu_used': False, 'models': {}})
    args.models_root.mkdir(parents=True, exist_ok=True)
    for profile in PROFILES:
        pin = manifest['models'][profile]
        source = args.artifacts_root / profile
        verify_upstream(source, pin)
        destination = args.models_root / pin['repository']
        seed = seed_cached_inputs(source, destination, pin)
        pulls = []
        observations = []
        for repetition in range(1, args.repetitions + 1):
            pulls.append(run_pull(args, profile, pin, repetition))
            observations.append(verify_published(destination, profile, pin))
        if any(item['model_manifest'] != observations[0]['model_manifest'] for item in observations):
            raise ValueError(f're-pull changed the public task contract: {profile}')
        report['models'][profile] = {'seed': seed, 'pulls': pulls, 'published': observations,
            'source_artifacts_after': verify_upstream(source, pin)}
        print(f'{profile}: official canonical install + {args.repetitions} verified pulls passed', flush=True)
    list_log = args.output.with_name(args.output.stem + '.list.log')
    list_command = [str(args.cli), 'list', str(args.models_root)]
    environment = {**os.environ, 'ANTFLY_INFERENCE_REQUIRED_BACKEND': 'native', 'CUDA_VISIBLE_DEVICES': ''}
    with list_log.open('wb') as output:
        result = subprocess.run(list_command, cwd=REPO, env=environment, stdout=output,
                                stderr=subprocess.STDOUT, timeout=args.timeout)
    text = list_log.read_text(errors='replace')
    if result.returncode != 0:
        raise ValueError(f'canonical CLI listing failed; see {list_log}')
    for name in report['model_names'].values():
        if not re.search(r'^extractor\s+' + re.escape(name) + r'\s*$', text, re.MULTILINE):
            raise ValueError(f'canonical name absent from extractor listing: {name}')
    report['canonical_listing'] = {'command': list_command, 'exit_code': result.returncode,
                                 'log': str(list_log), 'sha256': sha256(list_log)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cli', type=Path, required=True)
    parser.add_argument('--artifacts-root', type=Path, default=REPO / '.specialty-artifacts')
    parser.add_argument('--models-root', type=Path, default=REPO / '.specialty-artifacts/production-managed')
    parser.add_argument('--output', type=Path, default=REPO / '.specialty-artifacts/production-managed-install.json')
    parser.add_argument('--repetitions', type=int, default=2)
    parser.add_argument('--timeout', type=float, default=180)
    args = parser.parse_args()
    if args.repetitions < 1 or args.timeout <= 0:
        parser.error('positive repetitions and timeout required')
    args.cli = args.cli.resolve(strict=True)
    args.artifacts_root = args.artifacts_root.resolve(strict=True)
    args.models_root = args.models_root.resolve()
    args.output = args.output.resolve()
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
