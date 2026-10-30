# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Collect frozen native whole-head v2 quality batches after public performance passes.

One source/configuration/model-bound worker owns each complete 100-case batch.
The output is private-owner numerical quality evidence and never supplies
public-serving, reliability, repeated-owner or persistent-soak authority.
"""
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

import context_release_build_adapter as builds
import native_whole_head_campaign as contract
from native_whole_head_quality import EXECUTION_FIELDS, configuration_contract
from native_repeat_campaign_resources import run_guarded_combined
import qualify_metal_native_checkpoint as models
from qualify_context_management import git_provenance
from record_context_release_build import source_snapshot

FILTER = 'context Metal native checkpoint paired quality qualification'
FLAGS = {field: 'ANTFLY_METAL_ATTENTION_MATCH_' + field.upper() for field in EXECUTION_FIELDS}
SCRIPTS = Path(__file__).resolve().parent
ROUTE_FLAGS = {'ANTFLY_METAL_QUALITY_ORDINARY_PREPARED_GREEDY': '1', 'ANTFLY_METAL_QUALITY_SERVING_PREFILL': '0'}
BUCKETS = frozenset(('document_qa', 'exact_retrieval', 'code', 'structured_tool', 'multiturn'))


def sha(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for raw in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(raw)
    return h.hexdigest()


def write_new(path, value):
    with Path(path).open('x') as stream:
        json.dump(value, stream, sort_keys=True, indent=2, allow_nan=False)
        stream.write('\n')
        stream.flush()
        os.fsync(stream.fileno())


def reference(root, path):
    path = Path(path)
    contract.require(path.is_relative_to(root) and path.is_file() and not path.is_symlink(), 'unsafe output reference')
    return {'path': str(path.relative_to(root)), 'sha256': sha(path)}


def validate_freeze(repo, root, freeze_ref):
    """Authenticate the complete runtime and frozen corpus before any gold read or model load."""
    frozen = contract.json_ref(root, freeze_ref, 'native campaign freeze')
    contract.require(type(frozen) is dict and frozen.get('schema') == 'antfly.native-whole-head.campaign-freeze.v2' and
                     frozen.get('release_authority') is False and frozen.get('runtime_frozen') is True,
                     'unsupported or unfrozen whole-head campaign')
    build_path = contract.regular(root, frozen['build']['path'], 'native build')
    contract.read_ref(root, frozen['build'], 'native build')
    build = builds.validate(build_path)
    current_head, closure = source_snapshot(repo)
    contract.require(current_head == build['head'] and closure == build['source'] and
                     git_provenance(repo) == frozen['source'], 'runtime source changed after campaign freeze')
    binary = {'path': str(build['binary_path']), 'sha256': build['binary_sha256']}
    contract.require(frozen.get('binary') == binary, 'frozen runtime executable changed')
    configuration = contract.json_ref(root, frozen['configuration'], 'native configuration probe')
    contract.require(configuration.get('passed') is True and configuration.get('source') == frozen['source'] and
                     configuration.get('binary') == binary, 'configuration probe does not attest the frozen executable')
    configuration_contract(configuration['configuration'], configuration['configuration'])
    implementation = configuration['configuration'].get('implementation_sha256')
    contract.require(type(implementation) is list and len(implementation) == 32 and
                     all(type(value) is int and 0 <= value <= 255 for value in implementation) and any(implementation) and
                     frozen.get('implementation_sha256') == bytes(implementation).hex(),
                     'configuration probe omits the compiled implementation identity')
    expected_flags = {FLAGS[key]: '1' if configuration['configuration']['execution'][key] else '0' for key in EXECUTION_FIELDS}
    contract.require(frozen.get('runtime_environment') == expected_flags and
                     configuration.get('runtime_environment') == expected_flags, 'frozen execution flags changed')
    audit = contract.json_ref(root, frozen['configuration_audit'], 'configuration environment audit')
    contract.require(audit.get('passed') is True and audit.get('source') == frozen['source'] and
                     audit.get('probe_observations_sha256') == frozen['configuration']['sha256'] and
                     audit.get('backend_environment_sha256') == bytes(configuration['configuration']['observed']['backend_environment_sha256']).hex(),
                     'configuration environment was not independently audited')
    index = contract.json_ref(root, frozen['input_index'], 'frozen quality index')
    contract.require(index.get('schema') == 'antfly.native-quality-input-preparation.v1' and
                     index.get('preparation_only') is True and index.get('model_quality_outputs_observed') is False and
                     index.get('corpus_fingerprint_sha256') == contract.digest(frozen.get('corpus_fingerprint_sha256'), 'corpus'),
                     'quality inputs differ from the frozen fresh preparation')
    # Do not decode reference text or scoring answers here. The worker reveals
    # each reference only after its two greedy branches; audits run afterwards.
    contract.read_ref(root, index['scoring_cases'], 'frozen scoring metadata', 64 * 1024 * 1024)
    preparation = contract.json_ref(root, index['preparation_audit'], 'preparation audit')
    preparation_seal = contract.json_ref(root, index['preparation_source_seal'], 'preparation source seal')
    contract.require(preparation.get('schema') == 'antfly.native-quality-preparation.audit.v2' and
                     preparation.get('passed') is True and preparation.get('prepared_cases') == 2000 and
                     preparation.get('model_quality_outputs_observed') is False and
                     preparation.get('model_quality_qualified') is False and preparation.get('release_eligible') is False and
                     preparation.get('protected_prefix_rows') == 128 and preparation.get('protected_recent_rows') == 2048 and
                     preparation.get('corpus_fingerprint_sha256') == frozen['corpus_fingerprint_sha256'],
                     'invalid or exposed preparation audit')
    contract.require(preparation_seal.get('schema') == 'antfly.native-quality-preparation.source-seal.v1' and
                     preparation_seal.get('preparation_only') is True and preparation_seal.get('release_eligible') is False and
                     preparation_seal.get('source') == preparation.get('source') and
                     preparation_seal.get('producer_binary') == preparation.get('binary'),
                     'preparation source and audit do not agree')
    archive = Path(preparation_seal['archive']['path'])
    contract.require(archive.is_absolute() and archive.is_relative_to(root), 'preparation archive escaped campaign')
    archive = contract.regular(root, str(archive.relative_to(root)), 'preparation archive')
    contract.require(archive.stat().st_size == preparation_seal['archive']['bytes'] and
                     sha(archive) == contract.digest(preparation_seal['archive']['sha256'], 'preparation archive'),
                     'preparation archive changed')
    preparation_cells = {(model, context, bucket) for model in contract.MODELS
                         for context in (8192, 32768) for bucket in BUCKETS}
    audited_cells = preparation.get('cells')
    contract.require(type(audited_cells) is list and len(audited_cells) == len(preparation_cells) and
                     {(row['model'], row['context_tokens'], row['bucket']) for row in audited_cells} == preparation_cells and
                     all(type(row['cases']) is int and row['cases'] == 100 for row in audited_cells),
                     'incomplete audited preparation cohort')
    expected_cells = {(model, encoding, context, bucket) for model in contract.MODELS
                      for encoding in contract.ENCODINGS for context in (8192, 32768) for bucket in BUCKETS}
    batches = index['batches']
    contract.require(type(batches) is list and len(batches) == len(expected_cells), 'incomplete frozen native cohort')
    cells, identities = set(), {}
    for batch in batches:
        cell = (batch['model'], batch['encoding'], batch['context_tokens'], batch['bucket'])
        contract.require(cell in expected_cells and cell not in cells and type(batch['cases']) is int and batch['cases'] == 100,
                         'duplicate, missing or undersized frozen native cell')
        cells.add(cell)
        manifest = contract.json_ref(root, batch['input'], 'frozen native manifest')
        model = batch['model']
        contract.require(manifest.get('schema') == 'antfly.native-checkpoint-quality.input.v1' and
                         manifest.get('model') == contract.MODELS[model][0] and len(manifest['cases']) == 100,
                         'manifest differs from frozen native cell')
        identity = (manifest['main_gguf_sha256'], manifest['native_identity'])
        model_spec = frozen['models'][model]
        contract.require(type(manifest['native_identity']) is dict and
                         set(manifest['native_identity']) == {'weights', 'tokenizer', 'template', 'layout'} and
                         all(type(value) is list and len(value) == 32 and
                             all(type(byte) is int and 0 <= byte <= 255 for byte in value)
                             for value in manifest['native_identity'].values()) and
                         {key: bytes(value).hex() for key, value in manifest['native_identity'].items()} ==
                         model_spec['loaded_identity'] and
                         manifest['main_gguf_sha256'] == model_spec['artifact_sha256'],
                         'frozen model identity differs from native cohort')
        contract.require(model not in identities or identities[model] == identity, 'model identity varies between native cells')
        identities[model] = identity
        contract.require(len({case['id'] for case in manifest['cases']}) == 100 and
                         all(case['bucket'] == batch['bucket'] and case['prompt_count'] + 256 == batch['context_tokens'] and
                             case['request'] == {'encoding': batch['encoding'], 'output_tokens': 256, 'minimum_savings_ratio': 1.5}
                             for case in manifest['cases']), 'changed native cases or reservation')
    contract.require(cells == expected_cells, 'frozen cohort coverage changed')
    performance_cells = {(model, encoding, context) for model in contract.MODELS
                         for encoding in contract.ENCODINGS for context in (8192, 32768)}
    observed_cells = set()
    for entry in frozen['performance']:
        cell = (entry['model'], entry['encoding'], entry['context_tokens'])
        contract.require(cell in performance_cells and cell not in observed_cells, 'repeated performance prerequisite')
        observed_cells.add(cell)
        report = contract.json_ref(root, entry['observations'], 'public performance', 256 * 1024 * 1024)
        resource = contract.json_ref(root, entry['resource'], 'public performance combined guard', 256 * 1024 * 1024)
        contract.combined_resource_contract(resource)
        worker_log = contract.read_ref(root, entry['worker_log'], 'public performance worker log', 256 * 1024 * 1024)
        job = contract.json_ref(root, entry['job'], 'public performance producer job')
        contract.require(job.get('source') == frozen['source'] and job.get('binary') == binary and
                         job.get('runtime_environment') == expected_flags, 'performance job configuration differs')
        for ref in job['producers']:
            contract.read_ref(root, ref, 'public performance producer')
        expected = {key: entry[key] for key in ('model', 'encoding', 'context_tokens', 'request')}
        expected.update(source=frozen['source'], binary=binary, loaded_identity=frozen['models'][entry['model']]['loaded_identity'])
        result = contract.performance_contract(report, expected, worker_log)
        contract.require(result['performance_gate_passed'], 'public performance prerequisite failed')
    contract.require(observed_cells == performance_cells, 'public performance prerequisite matrix is incomplete')
    return frozen, build, index, configuration['configuration']


def model_artifact(root, frozen, batch, manifest):
    spec = frozen['models'][batch['model']]
    directory = Path(spec['directory']).resolve(strict=True)
    registry_root = directory.parent.parent
    models.checked_model_registry(registry_root, directory, contract.MODELS[batch['model']][0])
    download = directory / '.antfly-download-complete.json'
    contract.require(sha(download) == spec['manifest_sha256'], 'model download manifest changed')
    contract.require(spec['artifact_sha256'] == manifest['main_gguf_sha256'], 'main model artifact differs from native inputs')
    models.checked_model_artifact(directory, spec['artifact'], spec['artifact_sha256'])
    return directory, registry_root


def worker(args):
    root, output = args.campaign_root.resolve(strict=True), args.output.resolve(strict=True)
    spec = contract.json_ref(output, {'path': 'spec.json', 'sha256': args.worker_spec_sha256}, 'quality worker spec')
    contract.read_ref(root, spec['manifest'], 'quality worker manifest')
    contract.require(git_provenance(args.root) == spec['source'] and sha(spec['binary']['path']) == spec['binary']['sha256'],
                     'quality worker runtime changed')
    flags = dict(spec['runtime_environment'], **ROUTE_FLAGS)
    flags.update(ANTFLY_METAL_NATIVE_QUALITY_INPUT=str(contract.regular(root, spec['manifest']['path'], 'native batch')),
                 ANTFLY_METAL_NATIVE_QUALITY_INPUT_ROOT=str(root), ANTFLY_METAL_NATIVE_QUALITY_OUTPUT_ROOT=str(output / 'outputs'),
                 ANTFLY_METAL_CHECKPOINT_MODELS_ROOT=spec['models_root'], ANTFLY_METAL_CHECKPOINT_MODEL=spec['request_model'])
    env = os.environ.copy()
    for key in ('ANTFLY_CONTEXT_PROFILE', 'TERMITE_METAL_TRACE_ENCODER_TIMING', 'TERMITE_METAL_TRACE_QUANT_BLOCK',
                'TERMITE_METAL_TRACE_DISPATCH_PROFILE', 'ANTFLY_METAL_QUALITY_CONFIGURATION_PROBE'):
        env.pop(key, None)
    env.update(flags)
    write_new(output / 'worker-environment.json', flags)
    models.headroom(output, 'native-whole-head-quality')
    with (output / 'quality.stdout').open('xb') as stdout, (output / 'quality.stderr').open('xb') as stderr:
        process = subprocess.run(spec['command'], env=env, stdout=stdout, stderr=stderr, timeout=spec['timeout_seconds'] - 30)
    return process.returncode


def collect(args):
    root, repo = args.campaign_root.resolve(strict=True), args.root.resolve(strict=True)
    freeze_ref = {'path': str(args.freeze), 'sha256': args.freeze_sha256}
    frozen, build, index, configuration = validate_freeze(repo, root, freeze_ref)
    contract.require(type(args.batch) is int and 0 <= args.batch < len(index['batches']), 'invalid batch index')
    batch = index['batches'][args.batch]
    manifest = contract.json_ref(root, batch['input'], 'native input')
    directory, registry_root = model_artifact(root, frozen, batch, manifest)
    output = args.output.absolute().resolve(strict=False)
    contract.require(output.is_relative_to(root) and not output.exists() and not output.is_symlink(), 'unsafe or existing quality output')
    # The advisory lock survives no worker process. A stale file after a crash
    # cannot trap a resumed campaign or permit overlapping model loads.
    with (root / 'native-whole-head-active.lock').open('a+') as active:
        fcntl.flock(active.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        output.mkdir(parents=True, exist_ok=False)
        (output / 'outputs').mkdir()
        timeout = 900 * batch['cases'] + 120
        command = [str(build['binary_path']), '--test-filter', FILTER]
        spec = {'schema': 'antfly.native-whole-head.quality-job.v2', 'source': frozen['source'],
                'binary': frozen['binary'], 'freeze': freeze_ref, 'manifest': batch['input'], 'batch': batch,
                'configuration_sha256': frozen['configuration']['sha256'], 'runtime_environment': frozen['runtime_environment'],
                'request_model': contract.MODELS[batch['model']][0], 'models_root': str(registry_root),
                'model_directory': str(directory), 'command': command, 'timeout_seconds': timeout,
                'producer_sha256': sha(__file__), 'release_authority': False, 'production_ready': False}
        write_new(output / 'spec.json', spec)
        spec_sha256 = sha(output / 'spec.json')
        # This immutable claim prevents a changed runtime from silently starting
        # a new campaign on a corpus whose model outputs have been exposed.
        claim = root / ('native-whole-head-exposure-' + frozen['corpus_fingerprint_sha256'] + '.json')
        expected_claim = {'schema': 'antfly.native-whole-head.exposure-claim.v2', 'freeze': freeze_ref,
                          'source': frozen['source'], 'binary': frozen['binary']}
        if claim.exists():
            contract.require(json.loads(claim.read_bytes()) == expected_claim, 'fresh corpus was exposed under a different runtime freeze')
        else:
            write_new(claim, expected_claim)
        child = [sys.executable, str(Path(__file__).resolve()), *sys.argv[1:], '--worker',
                 '--worker-spec-sha256', spec_sha256]
        guard = run_guarded_combined(child, output / 'resource.json', timeout=timeout, max_rss_mib=8192,
                                     min_free_percent=15, max_swap_growth_mib=0, min_disk_free_mib=4096)
        result = {'schema': 'antfly.native-whole-head.quality-batch-seal.v2', 'collection_complete': False,
                  'source': frozen['source'], 'binary': frozen['binary'], 'batch': batch,
                  'spec': reference(output, output / 'spec.json'), 'resource': reference(output, output / 'resource.json'),
                  'passed': False, 'full_serving_owner_qualified': False, 'production_ready': False, 'release_authority': False}
        try:
            contract.combined_resource_contract(guard)
            contract.require(sha(output / 'spec.json') == spec_sha256, 'quality worker spec changed')
            contract.require(git_provenance(repo) == frozen['source'] and sha(build['binary_path']) == build['binary_sha256'] and
                             sha(__file__) == spec['producer_sha256'], 'quality runtime or producer changed')
            model_artifact(root, frozen, batch, manifest)
            outputs = [reference(output / 'outputs', output / 'outputs' / f'case-{i:04}.json') for i in range(batch['cases'])]
            sampling = contract.json_ref(root, index['scoring_cases'], 'scoring metadata', 64 * 1024 * 1024)
            implementation = bytes(configuration['implementation_sha256']).hex()
            audit = contract.audit_batch(root, batch['input'], output / 'outputs', outputs, batch, configuration,
                                         implementation, sampling)
            write_new(output / 'audit.json', audit)
            result.update(passed=True, collection_complete=True, audit=reference(output, output / 'audit.json'), outputs=outputs)
        except Exception as error:
            result['error'] = f'{type(error).__name__}: {error}'
        finally:
            result['evidence'] = [reference(output, path) for path in sorted(output.rglob('*')) if path.is_file()]
            write_new(output / 'seal.json', result)
    return 0 if result['passed'] else 1


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--root', type=Path, required=True)
    p.add_argument('--campaign-root', type=Path, required=True)
    p.add_argument('--freeze', type=Path, required=True, help='Immutable campaign-relative freeze path')
    p.add_argument('--freeze-sha256', required=True)
    p.add_argument('--batch', type=int, required=True)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--worker', action='store_true', help=argparse.SUPPRESS)
    p.add_argument('--worker-spec-sha256', help=argparse.SUPPRESS)
    return p


def main():
    args = parser().parse_args()
    return worker(args) if args.worker else collect(args)


if __name__ == '__main__':
    raise SystemExit(main())
