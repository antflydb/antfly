# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Collect continuous activity on the same installed 8K and 32K native owners.

The frozen public-performance artifacts supply consumed workload checkpoints.
This test-driver reliability gate grants no serving or overall release authority.
"""
import argparse
import fcntl
import json
import os
from pathlib import Path
import subprocess
import sys

import native_checkpoint_soak_evidence as evidence
import native_whole_head_campaign as contract
import qualify_native_whole_head_quality as campaign

FILTER = 'context Metal checkpoint persistent installed owner soak qualification'
PREFIXES = {8192: 8128, 32768: 32512}


def checkpoint_ref(root, value):
    contract.require(type(value) is dict and set(value) == {'path', 'sha256'}, 'invalid soak checkpoint reference')
    path = contract.regular(root, value['path'], 'soak checkpoint')
    contract.require(0 < path.stat().st_size <= 2 * 1024**3 and
                     campaign.sha(path) == contract.digest(value['sha256'], 'soak checkpoint'),
                     'soak checkpoint changed')
    return path


def worker(args):
    root, output = args.campaign_root.resolve(strict=True), args.output.resolve(strict=True)
    spec = contract.json_ref(output, {'path': 'spec.json', 'sha256': args.worker_spec_sha256}, 'soak worker spec')
    contract.require(campaign.git_provenance(args.root) == spec['source'] and
                     campaign.sha(spec['binary']['path']) == spec['binary']['sha256'], 'soak worker runtime changed')
    contract.read_ref(output, spec['input'], 'soak worker input')
    for ref in spec['checkpoints']:
        checkpoint_ref(root, ref)
    env = os.environ.copy()
    for key in ('ANTFLY_CONTEXT_PROFILE', 'TERMITE_METAL_TRACE_ENCODER_TIMING', 'TERMITE_METAL_TRACE_QUANT_BLOCK',
                'TERMITE_METAL_TRACE_Q80_BLOCK', 'TERMITE_METAL_TRACE_DISPATCH_PROFILE', 'TERMITE_METAL_TRACE_FRAME',
                'ANTFLY_METAL_QUALITY_ORDINARY_PREPARED_GREEDY', 'ANTFLY_METAL_QUALITY_SERVING_PREFILL',
                'ANTFLY_METAL_QUALITY_CONFIGURATION_PROBE'):
        env.pop(key, None)
    flags = dict(spec['runtime_environment'],
                 ANTFLY_METAL_CHECKPOINT_SOAK_INPUT=str(output / 'input.json'),
                 ANTFLY_METAL_CHECKPOINT_SOAK_OUTPUT=str(output / 'report.json'),
                 ANTFLY_METAL_CHECKPOINT_SOAK_JOURNAL=str(output / 'journal.jsonl'),
                 ANTFLY_METAL_CHECKPOINT_MODELS_ROOT=spec['models_root'],
                 ANTFLY_METAL_CHECKPOINT_MODEL=spec['request_model'])
    env.update(flags)
    campaign.write_new(output / 'worker-environment.json', flags)
    campaign.models.headroom(output, 'native-persistent-owner-soak')
    with (output / 'soak.stdout').open('xb') as stdout, (output / 'soak.stderr').open('xb') as stderr:
        process = subprocess.run(spec['command'], env=env, stdout=stdout, stderr=stderr,
                                 timeout=spec['timeout_seconds'] - 30)
    return process.returncode


def collect(args):
    root, repo = args.campaign_root.resolve(strict=True), args.root.resolve(strict=True)
    freeze_ref = {'path': str(args.freeze), 'sha256': args.freeze_sha256}
    frozen, build, index, configuration = campaign.validate_freeze(repo, root, freeze_ref)
    contract.require(args.model in contract.MODELS and args.encoding in contract.ENCODINGS and
                     type(args.duration_seconds) is int and 0 < args.duration_seconds <= evidence.FORMAL_SECONDS and
                     (args.diagnostic or args.duration_seconds == evidence.FORMAL_SECONDS),
                     'short soak requires explicit diagnostic scope')
    batch = next(row for row in index['batches'] if row['model'] == args.model and row['encoding'] == args.encoding)
    manifest = contract.json_ref(root, batch['input'], 'soak model identity')
    directory, models_root = campaign.model_artifact(root, frozen, batch, manifest)
    selected = [next(row for row in frozen['performance'] if
                     (row['model'], row['encoding'], row['context_tokens']) == (args.model, args.encoding, context))
                for context in (8192, 32768)]
    refs = [row['checkpoint'] for row in selected]
    paths = [checkpoint_ref(root, ref) for ref in refs]
    # Bind each checkpoint to the actual fresh preparation receipt, rather than
    # accepting a filename adjacent to otherwise qualified performance evidence.
    for row, ref in zip(selected, refs):
        report = contract.json_ref(root, row['observations'], 'soak performance preparation', 256 * 1024**2)
        preparations = [request['result'] for request in report['worker']['requests'] if request['kind'] == 'prepare']
        contract.require(len(preparations) == 2 and preparations[1]['file_sha256'] == ref['sha256'],
                         'soak checkpoint differs from the fresh public preparation')
    output = args.output.absolute().resolve(strict=False)
    contract.require(output.is_relative_to(root) and not output.exists() and not output.is_symlink(),
                     'unsafe or existing soak output')
    with (root / 'native-whole-head-active.lock').open('a+') as active:
        fcntl.flock(active.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        output.mkdir(parents=True, exist_ok=False)
        timeout = int(args.duration_seconds / .9) + 900
        value = {'schema': 'antfly.native-checkpoint-soak.input.v2', 'model': contract.MODELS[args.model][0],
                 'main_gguf_sha256': manifest['main_gguf_sha256'], 'native_identity': manifest['native_identity'],
                 'checkpoints': [{'path': str(path), 'sha256': ref['sha256']} for path, ref in zip(paths, refs)],
                 'request': {'encoding': args.encoding, 'duration_seconds': args.duration_seconds}}
        campaign.write_new(output / 'input.json', value)
        spec = {'schema': 'antfly.native-checkpoint-soak.job.v2', 'source': frozen['source'], 'binary': frozen['binary'],
                'freeze': freeze_ref, 'input': campaign.reference(output, output / 'input.json'),
                'checkpoints': refs, 'model': args.model, 'encoding': args.encoding,
                'duration_seconds': args.duration_seconds, 'diagnostic_only': args.diagnostic,
                'runtime_environment': frozen['runtime_environment'], 'request_model': contract.MODELS[args.model][0],
                'models_root': str(models_root), 'model_directory': str(directory),
                'command': [str(build['binary_path']), '--test-filter', FILTER], 'timeout_seconds': timeout,
                'producer_sha256': campaign.sha(__file__), 'release_authority': False}
        campaign.write_new(output / 'spec.json', spec)
        spec_sha = campaign.sha(output / 'spec.json')
        child = [sys.executable, str(Path(__file__).resolve()), *sys.argv[1:], '--worker', '--worker-spec-sha256', spec_sha]
        guard = campaign.run_guarded_combined(child, output / 'resource.json', timeout=timeout, max_rss_mib=8192,
                                             min_free_percent=15, max_swap_growth_mib=0, min_disk_free_mib=4096)
        result = {'schema': 'antfly.native-checkpoint-soak.seal.v2', 'collection_complete': False, 'passed': False,
                  'source': frozen['source'], 'binary': frozen['binary'], 'spec': campaign.reference(output, output / 'spec.json'),
                  'resource': campaign.reference(output, output / 'resource.json'),
                  'production_ready': False, 'release_authority': False}
        try:
            contract.combined_resource_contract(guard)
            contract.require(campaign.git_provenance(repo) == frozen['source'] and
                             campaign.sha(build['binary_path']) == build['binary_sha256'] and
                             campaign.sha(output / 'spec.json') == spec_sha and campaign.sha(__file__) == spec['producer_sha256'],
                             'soak runtime, spec or producer changed')
            campaign.model_artifact(root, frozen, batch, manifest)
            for ref in refs:
                checkpoint_ref(root, ref)
            report_ref = campaign.reference(output, output / 'report.json')
            journal_ref = campaign.reference(output, output / 'journal.jsonl')
            report = contract.json_ref(output, report_ref, 'native soak report')
            journal = contract.read_ref(output, journal_ref, 'native soak journal', 256 * 1024**2)
            expected = {'model': args.model, 'encoding': args.encoding, 'model_directory': str(directory),
                        'input_sha256': spec['input']['sha256'], 'artifact_sha256': manifest['main_gguf_sha256'],
                        'implementation_sha256': frozen['implementation_sha256'], 'native_identity': manifest['native_identity'],
                        'configuration': configuration, 'duration_seconds': args.duration_seconds,
                        'prefix_tokens': [PREFIXES[context] for context in (8192, 32768)],
                        'checkpoint_sha256': [ref['sha256'] for ref in refs]}
            audit = evidence.audit(report, journal, guard, expected, formal=not args.diagnostic)
            campaign.write_new(output / 'audit.json', audit)
            result.update(passed=True, collection_complete=True, report=report_ref, journal=journal_ref,
                          audit=campaign.reference(output, output / 'audit.json'))
        except Exception as error:
            result['error'] = f'{type(error).__name__}: {error}'
        finally:
            result['evidence'] = [campaign.reference(output, path) for path in sorted(output.rglob('*')) if path.is_file()]
            campaign.write_new(output / 'seal.json', result)
    return 0 if result['passed'] else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--campaign-root', type=Path, required=True)
    parser.add_argument('--freeze', type=Path, required=True)
    parser.add_argument('--freeze-sha256', required=True)
    parser.add_argument('--model', choices=sorted(contract.MODELS), required=True)
    parser.add_argument('--encoding', choices=sorted(contract.ENCODINGS), required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--duration-seconds', type=int, default=evidence.FORMAL_SECONDS)
    parser.add_argument('--diagnostic', action='store_true')
    parser.add_argument('--worker', action='store_true', help=argparse.SUPPRESS)
    parser.add_argument('--worker-spec-sha256', help=argparse.SUPPRESS)
    args = parser.parse_args()
    return worker(args) if args.worker else collect(args)


if __name__ == '__main__':
    raise SystemExit(main())
