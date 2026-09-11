#!/usr/bin/env python3
"""Explicit continuation of the immutable successful Metal pause probes."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import signal
import sys
import time

ROOT = Path(__file__).resolve().parent
PAUSE_ROOT = Path('/private/tmp/antfly-gliner25-training-inactive-published-small-metal-v1')
sys.path.insert(0, str(PAUSE_ROOT))
import metal_pause as pause

reference = pause.reference
supervision = pause.supervision
require = reference.require
MIB = 1024**2
PREPARATION_SHA256 = '9419477901e56f72cbb681f2bd46ffe4a9f1b4fb2b3310d30ccfce91e07b8c34'
PHASES = ('uninterrupted', 'resumed')


def helper_pins():
    return {name: reference.digest(path, MIB) for name, path in (
        ('driver', Path(__file__)), ('pause_driver', Path(pause.__file__)),
        ('reference', Path(reference.__file__)), ('supervision', Path(supervision.__file__)))}


def preparation():
    raw = reference.read(ROOT / 'preparation.json', 64 * 1024)
    require(hashlib.sha256(raw).hexdigest() == PREPARATION_SHA256, 'preparation changed')
    value = reference.loads(raw)
    require(value['format'] == 'antfly.gliner25-published-classifier-metal-continuation-preparation/v1', 'wrong continuation format')
    require(value['model_execution'] is False and value['qualification'] is False, 'preparation is not execution evidence')
    require(value['pause_root'] == str(PAUSE_ROOT), 'wrong paused owner')
    require(reference.digest(PAUSE_ROOT / 'preparation.json', 64 * 1024) == value['pause_preparation'], 'pause profile changed')
    pause.preparation()
    for name, pin in value['helpers'].items():
        require(reference.digest(PAUSE_ROOT / name, MIB) == pin, 'frozen helper changed: ' + name)
    for module, name in ((pause, 'metal_pause.py'), (reference, 'cpu_reference.py'), (supervision, 'supervision.py')):
        require(Path(module.__file__).resolve() == PAUSE_ROOT / name, 'helper loaded from wrong path')
    for name, pin in value['configs'].items():
        require(reference.digest(ROOT / name, 64 * 1024) == pin, 'configuration changed: ' + name)
    for data in value['paused'].values():
        for name, pin in data['files'].items():
            require(reference.digest(PAUSE_ROOT / name, 4 * MIB) == pin, 'paused artifact changed: ' + name)
    require(reference.digest(Path(value['data']['path']), MIB) == {key: value['data'][key] for key in ('size_bytes', 'sha256')}, 'authored data changed')
    return value


def command_for(binary, config):
    return [str(binary), 'finetune', 'train', 'gliner25', str(config), '--shutdown-grace-seconds', '30']


def validate_process(process, mode, phase, prep):
    require(type(process['returncode']) is int and process['returncode'] == 0 and process['failure'] is None, 'invocation failed')
    cleanup = process['cleanup']
    require(cleanup['complete'] is True and cleanup['direct_child_reaped'] is True and cleanup['known_children_gone'] is True, 'incomplete owned cleanup')
    require(process['psutil_version'] == '7.1.3' and process['max_child_tree_rss_bytes'] == pause.RSS_LIMIT, 'wrong RSS guard profile')
    require(0 < pause.strict_nonnegative(process['peak_child_tree_rss_bytes'], 'RSS peak') <= pause.RSS_LIMIT, 'RSS bound exceeded')
    require(any(entry['relation'] == 'observed_descendant' for entry in process['tracked_processes']), 'public worker not observed')
    require(process['helpers'] == helper_pins(), 'consumed helper bytes changed')
    require(process['binary'] == {key: prep['binary'][key] for key in ('size_bytes', 'sha256')}, 'wrong binary')
    config = ROOT / f'{mode}-{phase}.json'
    require(reference.digest(config, 64 * 1024) == process['config'], 'consumed config changed')
    require(process['command'] == command_for(Path(prep['binary']['path']), config), 'wrong public CLI argv')


def validate_export(output, mode, result, config):
    model = output / 'model'
    require({p.name for p in model.iterdir()} == reference.MODEL_FILES, 'unexpected portable files')
    portable = result['portable_model']
    require(type(portable) is dict and portable['mode'] == mode, 'missing portable export')
    require(portable['weights'] == reference.digest(model / 'adapter_model.safetensors', 4 * MIB), 'portable weight digest changed')
    require(portable['provenance'] == reference.digest(model / 'antfly_gliner25_training.json', 4 * MIB), 'portable provenance digest changed')
    output_bytes = sum(reference.digest(model / name, 4 * MIB)['size_bytes'] for name in reference.MODEL_FILES)
    require(pause.strict_nonnegative(portable['output_bytes'], 'export bytes') == output_bytes <= config['export_limits']['max_output_bytes'], 'portable output size mismatch')
    require(pause.strict_nonnegative(portable['peak_scratch_bytes'], 'export scratch') <= config['export_limits']['max_scratch_bytes'], 'portable scratch exceeded')
    exported = reference.tensors(model / 'adapter_model.safetensors')
    require({name: item['shape'] for name, item in exported.items()} == reference.slot_shapes(mode, saved=True), 'wrong exported classifier inventory')
    settings = reference.load(model / 'adapter_config.json', 64 * 1024)
    require(settings['target_modules'] == ['classifier.0', 'classifier.3'] and settings['r'] == 2 and settings['lora_alpha'] == 3 and settings['lora_dropout'] == 0 and settings['use_dora'] is (mode == 'dora'), 'PEFT settings changed')


def validate_phase(mode, phase, prep):
    folder = ROOT / 'executions' / f'{mode}-{phase}'
    process = reference.load(folder / 'process.json')
    validate_process(process, mode, phase, prep)
    config = reference.load(ROOT / f'{mode}-{phase}.json', 64 * 1024)
    output = Path(config['output_dir'])
    result = reference.load(output / 'result.json', 64 * 1024)
    manifest_bytes = reference.read(output / 'run.json', MIB)
    manifest = reference.loads(manifest_bytes)
    reference.config_subset(config, manifest['config'])
    require(manifest['source'] == prep['source'] and manifest['backend'] == 'metal' and manifest['math_policy'] == 'strict_f32_activations_v1', 'wrong source/backend/math policy')
    require(reference.executable_snapshot_digest(manifest['observed_executable']['digest']) == process['binary'], 'actual executable changed')
    require(reference.byte_array(manifest['train_sha256']).hex() == prep['data']['sha256'], 'consumed data changed')
    require(manifest['evaluation_performed'] is False and manifest['calibration_sha256'] is None and manifest['test_sha256'] is None, 'unexpected evaluation or holdout data')
    require(result['status'] == 'complete' and result['identity'] == {'optimizer_step': 3, 'microbatch_step': 5}, 'wrong final state')
    require(type(result['accumulated_microbatches']) is int and result['accumulated_microbatches'] == 0, 'accumulation not flushed')
    require(result['run_fingerprint'] == manifest['run_fingerprint'] == prep['paused'][mode]['run_fingerprint'], 'run semantics differ from original pause')
    if phase == 'resumed':
        require(config['expected_restore_state_sha256'] == prep['paused'][mode]['state_sha256'], 'wrong expected resume state')
        require(config['resume_from'] == str(PAUSE_ROOT / f'{mode}-paused/latest.safetensors'), 'wrong restore path')
        require(manifest['initial_identity'] == {'optimizer_step': 0, 'microbatch_step': 1} and manifest['restore_receipt'] is not None, 'fresh owner did not resume unfinished window')
    else:
        require(manifest['initial_identity'] == {'optimizer_step': 0, 'microbatch_step': 0} and manifest['restore_receipt'] is None, 'unexpected restore in uninterrupted run')
    reports = [reference.loads(line) for line in reference.read(output / 'progress.jsonl', MIB).splitlines()]
    reference.validate_reports(reports, phase)
    for report in reports:
        pause.validate_resource_report(report, config)
    events = [reference.loads(line) for line in reference.read(folder / 'stdout.jsonl', 4 * MIB).splitlines()]
    require(events == [{'event': 'step', 'report': report} for report in reports] + [{'event': 'result', 'result': result, 'output_dir': str(output)}], 'stdout/durable events disagree')
    state = reference.validate_checkpoint(output / 'latest.safetensors', mode, False, pause.controller_fingerprint(mode, manifest_bytes), result['state_sha256'])
    validate_export(output, mode, result, config)
    files = {name: reference.digest(output / name, 4 * MIB) for name in ('run.json', 'progress.jsonl', 'result.json', 'latest.safetensors')}
    files.update({'model/' + name: reference.digest(output / 'model' / name, 4 * MIB) for name in sorted(reference.MODEL_FILES)})
    files.update({name: reference.digest(folder / name, 4 * MIB) for name in ('process.json', 'start.json', 'stdout.jsonl')})
    return {'result': result, 'manifest': manifest, 'reports': reports, 'process': process, 'slots': state, 'files': files}


def phase_receipt(mode, phase, prep, checked):
    return {'format': 'antfly.gliner25-published-classifier-metal-continuation-phase/v1', 'mode': mode, 'phase': phase, 'status': 'pass', 'binary': checked['process']['binary'], 'preparation_sha256': PREPARATION_SHA256, 'checker': reference.digest(Path(__file__)), 'files': checked['files'], 'result': checked['result'], 'slots': checked['slots'], 'resources': {'host_peak_bytes': max(report['host_peak_bytes'] for report in checked['reports']), 'backend_metadata_peak_bytes': max(report['backend_peak_bytes'] for report in checked['reports']), 'resident_device_upper_bound_bytes': max(report['resident_device_upper_bound_bytes'] for report in checked['reports']), 'sampled_child_tree_rss_bytes': checked['process']['peak_child_tree_rss_bytes']}, 'published_source_numerical_parity': False, 'claim': prep['scope']}


def compare_continuity(whole, paused, resumed):
    require(whole['result'] == resumed['result'], 'final result/state differs after resume')
    require(whole['result']['run_fingerprint'] == paused['result']['run_fingerprint'], 'paused run semantics differ')
    reports = paused['reports'] + resumed['reports']
    require(len(whole['reports']) == len(reports), 'missing/extra stitched reports')
    for expected, actual in zip(whole['reports'], reports):
        require({key: expected[key] for key in reference.SEMANTIC_REPORT} == {key: actual[key] for key in reference.SEMANTIC_REPORT}, 'resumed semantic decisions or optimizer values differ')


def validate_all(mode, prep):
    pause_receipt = pause.validate(mode, pause.preparation())
    original_reports = [reference.loads(line) for line in reference.read(PAUSE_ROOT / f'{mode}-paused/progress.jsonl', MIB).splitlines()]
    whole = validate_phase(mode, 'uninterrupted', prep)
    resumed = validate_phase(mode, 'resumed', prep)
    compare_continuity(whole, {'result': pause_receipt['result'], 'reports': original_reports}, resumed)
    artifacts = {}
    for name in ('result.json', 'latest.safetensors', *('model/' + item for item in sorted(reference.MODEL_FILES))):
        before = reference.digest(ROOT / f'{mode}-uninterrupted' / name, 4 * MIB)
        require(before == reference.digest(ROOT / f'{mode}-resumed' / name, 4 * MIB), 'nonidentical final bytes: ' + name)
        artifacts[name] = before
    reference.verify_source(prep)
    preparation()
    return {'format': 'antfly.gliner25-published-classifier-metal-resume-validation/v1', 'mode': mode, 'status': 'pass', 'binary': whole['process']['binary'], 'source': prep['source'], 'data': prep['data'], 'preparation_sha256': PREPARATION_SHA256, 'checker': reference.digest(Path(__file__)), 'microbatches': 5, 'optimizer_updates': 3, 'flush_after': [2, 4, 5], 'fallback_sequence': [False, True, False, True, True], 'state_sha256': whole['result']['state_sha256'], 'run_fingerprint': whole['result']['run_fingerprint'], 'artifacts': artifacts, 'resources': {phase: phase_receipt(mode, phase, prep, checked)['resources'] for phase, checked in (('uninterrupted', whole), ('resumed', resumed))}, 'original_pause_validation': prep['paused'][mode]['files'][f'executions/{mode}-paused/validation.json'], 'published_source_numerical_parity': False, 'claim': prep['scope']}


def run(mode, phase, prep):
    # Revalidate the original durable state with its frozen independent checker
    # before starting either branch, preserving the old process evidence.
    pause.validate(mode, pause.preparation())
    binary = Path(prep['binary']['path'])
    observed = reference.digest(binary)
    require(observed == {key: prep['binary'][key] for key in ('size_bytes', 'sha256')}, 'binary changed')
    reference.verify_source(prep)
    config_path = ROOT / f'{mode}-{phase}.json'
    config = reference.load(config_path, 64 * 1024)
    require(not Path(config['output_dir']).exists(), 'output exists; no retry/overwrite')
    folder = ROOT / 'executions' / f'{mode}-{phase}'
    folder.parent.mkdir(exist_ok=True)
    folder.mkdir(mode=0o700)
    command = command_for(binary, config_path)
    receipt = {'format': 'antfly.gliner25-published-classifier-metal-continuation-process/v1', 'command': command, 'binary': observed, 'config': reference.digest(config_path, 64 * 1024), 'helpers': helper_pins(), 'preparation_sha256': PREPARATION_SHA256, 'outer_timeout_seconds': config['timeout_seconds'] + 90, 'max_child_tree_rss_bytes': pause.RSS_LIMIT, 'max_stdout_bytes': 4 * MIB, 'max_stderr_bytes': 4 * MIB, 'failure': None, 'returncode': None}
    reference.write_new(folder / 'start.json', receipt)
    started = time.monotonic()
    try:
        receipt.update(supervision.run(command, folder / 'stdout.jsonl', folder / 'stderr.log', timeout_seconds=receipt['outer_timeout_seconds'], rss_limit_bytes=pause.RSS_LIMIT))
        require(receipt['failure'] is None and receipt['returncode'] == 0 and receipt['cleanup']['complete'], receipt['failure'] or 'invocation/cleanup failure')
        require(reference.digest(binary) == observed, 'executable changed during invocation')
        require(reference.digest(config_path, 64 * 1024) == receipt['config'] and helper_pins() == receipt['helpers'], 'configuration/helper changed during invocation')
        preparation()
        reference.verify_source(prep)
    except BaseException as error:
        if receipt['failure'] is None:
            receipt['failure'] = f'{type(error).__name__}: {error}'
        raise
    finally:
        receipt['elapsed_seconds'] = time.monotonic() - started
        reference.write_new(folder / 'process.json', receipt)
    checked = validate_phase(mode, phase, prep)
    result = phase_receipt(mode, phase, prep, checked)
    reference.write_new(folder / 'validation.json', result)
    print(json.dumps(result, allow_nan=False))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('inspect', 'run', 'validate-phase', 'validate'))
    parser.add_argument('--mode', choices=('lora', 'dora'))
    parser.add_argument('--phase', choices=PHASES)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    def terminate(_signal, _frame):
        raise InterruptedError('driver termination requested')
    signal.signal(signal.SIGTERM, terminate)
    prep = preparation()
    if args.command == 'inspect':
        require(args.mode is None and args.phase is None and args.output is None, 'inspect takes no run options')
        print(json.dumps({'preparation_sha256': PREPARATION_SHA256, 'model_execution': False, 'recipe': prep['recipe'], 'resources': prep['resource_analysis']}, indent=2))
    else:
        require(args.mode is not None, 'mode required')
        if args.command == 'run':
            require(args.phase is not None and args.output is None, 'run requires exactly one fixed phase')
            run(args.mode, args.phase, prep)
        elif args.command == 'validate-phase':
            require(args.phase is not None and args.output is not None and not args.output.exists(), 'offline phase requires a new output')
            checked = validate_phase(args.mode, args.phase, prep)
            reference.verify_source(prep)
            result = phase_receipt(args.mode, args.phase, prep, checked)
            reference.write_new(args.output, result)
            print(json.dumps(result, allow_nan=False))
        else:
            require(args.phase is None and args.output is None, 'final validation uses fixed new mode receipt')
            output = ROOT / f'{args.mode}-validation.json'
            require(not output.exists(), 'final validation exists; no overwrite')
            result = validate_all(args.mode, prep)
            reference.write_new(output, result)
            print(json.dumps(result, allow_nan=False))


if __name__ == '__main__':
    main()
