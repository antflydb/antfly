#!/usr/bin/env python3
"""Read-only rederivation and bounded additive archive; imports no Torch."""
import json
from pathlib import Path
import sys

import run

ROOT = Path(__file__).resolve().parent
REPO = Path('/Users/timkaye/Documents/af/antfly')
DEST = REPO / 'zig/pkg/inference/testdata/gliner25/published_inactive_classifier_export_reload_v1'
EXPECTED = {
    'cpu-lora': '395ad29816c2c5b1b5118b4b54b44802bb3170ae1940cd9942ef1d4cfa57c6d2',
    'cpu-dora': '331e90537cb77328b49c9d22e20814b9d5bb628f442cbb39028b9c665a8465aa',
    'metal-lora': '1071336faa98a937b43d207d61f6ca6501ea3043ce45832e5c740af22666c97d',
    'metal-dora': 'b18b9ed51fe32f0110356efec0f9cf90c980458f10a6a52484eb680b9f461c5c',
}


def main():
    plan = run.load_plan()
    oracle, _ = run.modules(plan)
    import check_training_export as checker
    require = run.require
    archive = {}
    profiles = {}
    def retain(path, relative, expected=None):
        identity, raw = run.file_pin(path, 4 * run.MiB, contents=True)
        if expected is not None:
            require(identity == {key: expected[key] for key in ('size_bytes', 'sha256')}, 'archive input changed')
        require(relative not in archive, 'duplicate archive destination')
        archive[relative] = (Path(path), identity, raw)
        return identity
    for name, entry in plan['profiles'].items():
        current_plan, _, static, originals = run.preflight(name)
        require(current_plan == plan, 'prepared contract changed')
        directory = Path(entry['execution_dir'])
        process_raw = run.file_pin(directory / 'process.json', run.MiB, contents=True)[1]
        require(run.pin(process_raw)['sha256'] == EXPECTED[name], 'root-completed process receipt changed')
        process = run.decode(process_raw)
        require(process['status'] == 'verified' and process['qualification'] is False and
                process['numerical_runtime_executed'] is True and process['original_inputs_unchanged'] is True and
                process['private_copies_cleaned'] is True and process['original_pins'] == originals and
                process['plan'] == run.PLAN_PIN and process['wrapper'] == run.file_pin(ROOT / 'run.py', run.MiB) and
                process['command'] == entry['command'] and process['limits'] == plan['limits'], 'process contract changed')
        owner = process['process']
        require(owner['failure'] is None and owner['returncode'] == 0 and owner['cleanup']['complete'] and
                owner['cleanup']['direct_child_reaped'] and owner['cleanup']['known_children_gone'] and
                not owner['cleanup']['survivors'] and not owner['cleanup']['errors'] and
                not owner['cleanup']['inspection_errors'], 'owned process cleanup failed')
        require(owner['peak_child_tree_rss_bytes'] <= plan['limits']['rss_limit_bytes'] and
                owner['elapsed_seconds'] < plan['limits']['timeout_seconds'] and
                owner['sampled_artifact_peak_bytes'] <= plan['max_artifact_bytes'], 'run exceeded its declared guard')
        rederived = run.validate_success(directory, static, entry, plan)
        for key, value in rederived.items():
            # The wrapper names these two summary fields differently.
            key = {'runtime_private_copy_bytes': 'runtime_private_copy_bytes'}.get(key, key)
            require(process[key] == value, 'independent success summary differs: ' + key)
        audited = checker.audit_export('small', Path(plan['source_dir']), Path(entry['export_dir']), Path(entry['run_dir']))
        require(audited == static, 'fresh exact checkpoint/export audit differs')
        report = run.read_json(directory / 'report/report.json')
        require(len(report['source_tensors']) == 334 and len(report['export_tensors']) == entry['expected_adapter_tensor_count'] and
                [value['name'] for value in report['adapter_modules']] == ['classifier.0', 'classifier.3'], 'target inventory changed')
        require(report['provenance']['optimizer_identity'] == {'optimizer_step': 3, 'microbatch_step': 5}, 'training epoch changed')
        saved_outputs = []
        for output in report['runtime']['outputs']:
            # Bind canonical saved outputs without interpreting curated outputs
            # as labels, quality scores, training gradients, or convergence.
            raw = json.dumps(output, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode()
            saved_outputs.append({'id': output['id'], 'kind': output['kind'], 'canonical_output': run.pin(raw),
                                  'tensor_capture': output.get('tensor_capture')})
        final_exports = {}
        for filename, expected in report['export_files'].items():
            final_exports[filename] = retain(Path(entry['export_dir']) / filename, f'exports/{name}/{filename}', expected)
            resumed = Path(entry['run_dir']).parent / (entry['mode'] + '-resumed') / 'model' / filename
            require(run.file_pin(resumed, run.MiB) == expected, 'loaded artifact differs from fresh-resumed export')
        profiles[name] = {
            'training_backend': entry['training_backend'], 'runtime_backend': 'pinned upstream CPU',
            'mode': entry['mode'], 'targets': ['classifier.0', 'classifier.3'], 'source_tensor_count': 334,
            'adapter_tensor_count': len(report['export_tensors']), 'requests': 10, 'errors': 0,
            'optimizer_identity': report['provenance']['optimizer_identity'], 'state_sha256': entry['state_sha256'],
            'export_files': final_exports, 'training_ledger': entry['training_ledger'],
            'final_checkpoint': report['job'], 'provenance': report['provenance'],
            'report': retain(directory / 'report/report.json', f'receipts/{name}/report.json', process['report']),
            'process': retain(directory / 'process.json', f'receipts/{name}/process.json'),
            'start': retain(directory / 'start.json', f'receipts/{name}/start.json'),
            'stdout': retain(directory / 'stdout.jsonl', f'receipts/{name}/stdout.jsonl', process['stdout.jsonl']),
            'stderr': retain(directory / 'stderr.log', f'receipts/{name}/stderr.log', process['stderr.log']),
            'sampled_child_tree_rss_bytes': owner['peak_child_tree_rss_bytes'],
            'elapsed_child_seconds': owner['elapsed_seconds'], 'sampled_active_artifact_bytes': owner['sampled_artifact_peak_bytes'],
            'private_runtime_copy_bytes': report['runtime']['private_copy_bytes'],
            'all_loaded_tensor_bytes_equal': True, 'fresh_resumed_export_bytes_equal': True,
            'owned_process_cleanup_complete': True, 'private_copies_cleaned': True, 'original_inputs_unchanged': True,
            'request_outputs': saved_outputs, 'raw_capture_files': process['output_files'],
        }
        require(run.input_pins(plan, entry) == originals, 'original files changed during independent audit')
    for name in ('run.py', 'test_plan.py', 'plan.json', 'preflight.json', 'contract-tests-v1.log', 'frozen-files.json', 'supervision.py'):
        retain(ROOT / name, 'helpers/' + name)
    for name, row in plan['helper_closure'].items():
        retain(row['snapshot'], 'helpers/checker/' + name, row)
    failure = Path('/private/tmp/gliner25-inactive-export-reload-metal-lora-v1.stderr.log')
    history = None
    if failure.exists():
        identity, raw = run.file_pin(failure, run.MiB, contents=True)
        require(b'insufficient disk headroom' in raw and b'preflight' in raw, 'historical failure is not the stated pre-model admission denial')
        history = {'kind': 'disk_admission_before_child_creation', 'model_execution': False,
                   'stderr': retain(failure, 'history/metal-lora-preflight-v1.stderr.log', identity),
                   'resolution': 'Root reclaimed only five exact completed test object files, preserving test binaries; the same frozen model command and guards subsequently passed.'}
    retain(Path(__file__), 'helpers/audit_completed.py')
    require(not any(key == value or key.startswith(value + '.') for key in sys.modules for value in ('torch', 'gliner2', 'peft')), 'audit imported a numerical runtime')
    require(sum(len(value[2]) for value in archive.values()) <= 2 * run.MiB, 'compact archive size exceeded')
    DEST.mkdir(mode=0o755)
    files = {}
    for relative, (original, identity, raw) in archive.items():
        target = DEST / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open('xb') as output:
            output.write(raw)
        require(run.file_pin(target, 4 * run.MiB) == identity, 'archive copy differs')
        files[relative] = {'original_path': str(original), **identity}
    manifest = {
        'format': 'antfly.gliner25-published-inactive-classifier-export-reload/v1',
        'runtime_available': False, 'release_ready': False, 'qualification': False,
        'scope': 'Four completed published-small classifier-only CPU/Metal LoRA/DoRA training artifacts reloaded in the exact pinned Fastino CPU runtime with official isolated PEFT0.18; all 334 base tensors and 4/6 adapter tensors byte-exact; ten fixed execution requests each.',
        'training_attention_profile': 'materialized_v1', 'training_constructor': 'ordinary published Source and native/resident trainer',
        'source': checker.source_identity('small', plan['source_files']), 'source_files': plan['source_files'],
        'source_commit': plan['upstream']['commit'], 'runtime': plan['isolated_runtime'],
        'python_invocation': plan['python_invocation'], 'python_executable': plan['python_executable'], 'pyvenv_cfg': plan['pyvenv_cfg'],
        'loader_profile': plan['loader_profile'], 'peft_wheel': plan['wheel'], 'limits': plan['limits'],
        'max_runtime_copy_bytes': plan['max_runtime_copy_bytes'], 'max_active_artifact_bytes': plan['max_artifact_bytes'],
        'max_final_artifact_bytes': plan['max_final_artifact_bytes'], 'requests': plan['requests'],
        'completed_model_processes': 4, 'completed_fixed_requests': 40, 'errors': 0,
        'all_original_inputs_rehashed_unchanged': True, 'all_four_private_copy_lifecycles_clean': True,
        'all_four_resumed_exports_equal_loaded_artifacts': True, 'independent_audit_loaded_model': False,
        'profiles': profiles, 'historical_preflight_denial': history,
        'scope_exclusions': ['No published Fastino training-loss/VJP or CPU-Metal update equality comparison.',
            'No convergence, held-out quality, full probability-vector or performance claim.',
            'No replay-tiled published job, other target/rank/backbone, or GA claim.',
            'Reloads run on upstream CPU even when the artifact was trained on Metal.',
            'The historical PEFT0.17.1 inside_weight key-rewriting failure remains unchanged.',
            'Large original base weights and bounded intermediate output tensor captures remain external and hash-bound; the four small portable adapter artifacts and all complete JSON/process reports are archived.'],
        'files': files,
    }
    identity = run.write_json(DEST / 'manifest.json', manifest)
    print(json.dumps({'status': 'verified', 'manifest': str(DEST / 'manifest.json'), **identity,
                      'archived_files': len(files), 'archived_payload_bytes': sum(v['size_bytes'] for v in files.values()),
                      'completed_model_processes': 4, 'requests': 40, 'audit_imported_numerical_runtime': False}))


if __name__ == '__main__':
    main()
