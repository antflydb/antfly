#!/usr/bin/env python3
"""One explicit bounded CLI phase, or independent offline validation.

Only the `run` subcommand starts a child. bind/inspect/validate-phase/validate
perform file checks only. No retry, model copy, Torch import or cap override.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import stat
import sys
import time
import types

import checker as c

ROOT = Path(__file__).resolve().parent
MIB = 1024**2
RSS_LIMIT = 4*1024**3
# The job owns its 1,800-second deadline. The outer supervisor retains the
# prepared 90-second margin for checkpoint/teardown and owned-child reaping.
TIMEOUT = 1890
PHASES = ('paused', 'resumed', 'uninterrupted')
LEGACY = {'f482989317fd34c0a8852d5a36d837fb78f6cc57862863f5a0b77981125f02b8', '73695721460619998009dc6ea10e8cd436616c36897e1732d99cab57009d1c0f', '6cd281cf04e326aab187647173ab645780f2a789621d6dc4607bf39fb91f5fa5', '12e8f2e9723fda9dd1ba6d2317b24ee07e2cc95bcce5baddee557ddd5a5bc279', '9f0d349efa3f33dd29b6c3dd26a12a8bcf2babf2bc78400c2b84d87d58d786f9'}


def helper_pins():
    return {name: c.digest(ROOT/name, MIB) for name in ('checker.py', 'run_phase.py')}


def load_supervision():
    _, _, _ = c.preparation()
    plan = c.load(ROOT/'qualification_plan.json', 64*1024)['supervisor']
    raw = c.read(Path(plan['dependency']), MIB)
    c.require(c.pin(raw) == plan['dependency_pin'], 'immutable supervisor changed')
    module = types.ModuleType('_gliner25_regional_frozen_supervision')
    module.__file__ = plan['dependency']
    # Execute the verified bytes themselves: importing by path would reopen
    # a file after its digest check. The immutable dependency imports no Torch.
    exec(compile(raw, module.__file__, 'exec'), module.__dict__)
    return module, {'path': plan['dependency'], **plan['dependency_pin']}


def exact_digest(value):
    c.require(type(value) is dict and 'size_bytes' in value and 'sha256' in value, 'invalid artifact digest')
    size = c.integer(value['size_bytes'], 'artifact size', 1, 1024*MIB)
    sha = value['sha256']
    c.require(type(sha) is str and len(sha) == 64 and all(x in '0123456789abcdef' for x in sha), 'invalid lowerhex digest')
    return {'size_bytes': size, 'sha256': sha}


def artifact_argument(path, sha, maximum):
    path = Path(path)
    c.require(path.is_absolute(), 'artifact path must be absolute')
    observed = c.digest(path, maximum)
    c.require(observed['sha256'] == sha, 'explicit artifact pin mismatch: '+str(path))
    return {'path': str(path), **observed}


def bind(args):
    prep, inventory, resources = c.preparation()
    binary = artifact_argument(args.binary, args.binary_sha256, 1024*MIB)
    c.require(binary['sha256'] not in LEGACY, 'historical executable predates regional training')
    build = artifact_argument(args.build_receipt, args.build_receipt_sha256, 4*MIB)
    receipt = c.load(Path(build['path']))
    c.require(exact_digest(receipt['binary']) == exact_digest(binary), 'build receipt binary mismatch')
    c.require(receipt.get('entrypoint', args.entrypoint) == args.entrypoint, 'build entrypoint mismatch')
    observed = receipt.get('live_help_observation')
    if args.entrypoint == 'standalone':
        c.require(type(observed) is dict and observed['argv'][-1] == '--help' and exact_digest(observed['executable']) == exact_digest(binary), 'missing exact standalone help observation')
    source_inventory_raw = c.read(Path(args.source_inventory), 4*MIB)
    source_inventory = c.pin(source_inventory_raw)
    c.validate_formula_inventory(c.loads(source_inventory_raw), resources)
    inventory_receipt = receipt.get('source_inventory', receipt.get('repository_source_inventory'))
    c.require(type(inventory_receipt) is dict and source_inventory == exact_digest(inventory_receipt) and inventory_receipt['all_recorded_files_reverified_unchanged'] is True, 'build source inventory mismatch')
    archive = artifact_argument(args.source_archive_receipt, args.source_archive_receipt_sha256, 4*MIB)
    archive_value = c.load(Path(archive['path']))
    c.require(type(archive_value) is dict and exact_digest(archive_value['build_receipt']) == exact_digest(build) and exact_digest(archive_value['source_inventory']) == source_inventory, 'source archive/build identity mismatch')
    source_archive = {'path': str(Path(args.source_archive)), **c.digest(Path(args.source_archive), 256*MIB)}
    c.require(exact_digest(source_archive) == exact_digest(archive_value['archive']), 'source archive payload changed')
    c.require(c.integer(archive_value['source_files_verified'], 'archived file count', 1) == c.integer(inventory_receipt['files'], 'source inventory count', 1), 'source archive file count mismatch')
    _, supervisor = load_supervision()
    helpers = helper_pins()
    directory = ROOT/'helper_archive'
    directory.mkdir(mode=0o700, exist_ok=True)
    c.require(stat.S_ISDIR(directory.lstat().st_mode), 'helper archive must be an actual directory')
    for name, pin in helpers.items():
        raw = c.read(ROOT/name, MIB)
        archive_name = pin['sha256']+'-'+name
        path = directory/archive_name
        if path.exists():
            c.require(c.digest(path, MIB) == pin, 'archived helper collision')
        else:
            # Small script source, never model/source-weight copies.
            with path.open('xb') as stream:
                stream.write(raw); stream.flush(); os.fsync(stream.fileno())
    value = {'format': 'antfly.gliner25-regional-all-native-executable/v1', 'entrypoint': args.entrypoint, 'binary': binary, 'build_receipt': build, 'source_inventory': {'path': str(Path(args.source_inventory)), **source_inventory}, 'source_archive_receipt': archive, 'source_archive': source_archive, 'preparation_sha256': c.PREPARATION_SHA256, 'helpers': helpers, 'supervisor': supervisor, 'limits': {'sampled_child_tree_rss_bytes': RSS_LIMIT, 'outer_timeout_seconds': TIMEOUT, 'stdout_bytes': 4*MIB, 'stderr_bytes': 4*MIB}, 'model_execution': False, 'source_checkout_reverification_required': False}
    c.write_new(ROOT/'executable.json', value)
    print(json.dumps({'status': 'bound', 'executable': binary, 'entrypoint': args.entrypoint, 'model_execution': False}))


def binding(require_current_helpers=False):
    value = c.load(ROOT/'executable.json', 64*1024)
    c.require(value['format'] == 'antfly.gliner25-regional-all-native-executable/v1' and value['preparation_sha256'] == c.PREPARATION_SHA256, 'wrong executable binding')
    c.require(value['entrypoint'] in ('standalone', 'public-runtime') and value['binary']['sha256'] not in LEGACY, 'unsupported executable contract')
    for key in ('binary', 'build_receipt', 'source_inventory', 'source_archive_receipt', 'source_archive'):
        maximum = 1024*MIB if key == 'binary' else 256*MIB if key == 'source_archive' else 4*MIB
        c.require(c.digest(Path(value[key]['path']), maximum) == exact_digest(value[key]), 'frozen '+key+' changed')
    _, _, resources = c.preparation()
    source_inventory_raw = c.read(Path(value['source_inventory']['path']), 4*MIB)
    c.require(c.pin(source_inventory_raw) == exact_digest(value['source_inventory']), 'build inventory changed during validation')
    c.validate_formula_inventory(c.loads(source_inventory_raw), resources)
    c.require(value['limits'] == {'sampled_child_tree_rss_bytes': RSS_LIMIT, 'outer_timeout_seconds': TIMEOUT, 'stdout_bytes': 4*MIB, 'stderr_bytes': 4*MIB}, 'execution limits changed')
    for name, expected in value['helpers'].items():
        c.require(name in ('checker.py', 'run_phase.py'), 'unexpected helper')
        archive = ROOT/'helper_archive'/(expected['sha256']+'-'+name)
        c.require(c.digest(archive, MIB) == expected, 'historical helper bytes unavailable')
    c.require(set(value['helpers']) == {'checker.py', 'run_phase.py'}, 'missing helpers')
    c.require(c.digest(Path(value['supervisor']['path']), MIB) == exact_digest(value['supervisor']), 'frozen supervision changed')
    if require_current_helpers:
        c.require(helper_pins() == value['helpers'], 'run helper changed after binding; no implicit new revision')
    return value


def command_for(bound, config, phase):
    prefix = [] if bound['entrypoint'] == 'standalone' else ['finetune', 'train', 'gliner25']
    result = [bound['binary']['path'], *prefix, str(config), '--shutdown-grace-seconds', '30']
    if phase == 'paused':
        result += ['--stop-after-microbatches', '1']
    return result


def validate_process(process, mode, phase, bound, config_path):
    c.require(process['format'] == 'antfly.gliner25-regional-all-native-process/v1', 'wrong process receipt')
    c.require(c.integer(process['returncode'], 'process returncode', 0, 255) == 0 and process['failure'] is None, 'process did not succeed')
    clean = process['cleanup']
    c.require(clean['complete'] is True and clean['direct_child_reaped'] is True and clean['known_children_gone'] is True and not clean['errors'] and not clean['survivors'] and not clean.get('inspection_errors'), 'owned cleanup incomplete')
    c.require(process['psutil_version'] == '7.1.3' and c.integer(process['max_child_tree_rss_bytes'], 'RSS cap') == RSS_LIMIT and 0 < c.integer(process['peak_child_tree_rss_bytes'], 'RSS peak') <= RSS_LIMIT, 'missing/exceeded tree RSS guard')
    c.require(c.integer(process['timeout_seconds'], 'timeout') == TIMEOUT and c.integer(process['max_stdout_bytes'], 'stdout cap') == 4*MIB and c.integer(process['max_stderr_bytes'], 'stderr cap') == 4*MIB, 'process bound changed')
    c.require(process['parent_grace_seconds'] == 35 and process['kill_wait_seconds'] == 10 and process['worker_parent_loss_grace_seconds'] == 2 and process['rss_poll_seconds'] == 0.05, 'process lifetime contract changed')
    c.require(any(x['relation'] == 'observed_descendant' for x in process['tracked_processes']), 'training worker never observed')
    c.require(process['binary'] == exact_digest(bound['binary']) and process['helpers'] == bound['helpers'] and process['supervisor'] == bound['supervisor'], 'consumed executable/helper bytes changed')
    c.require(process['executable_binding'] == c.digest(ROOT/'executable.json', 64*1024) and process['preparation_sha256'] == c.PREPARATION_SHA256, 'binding/preparation changed')
    c.require(process['command'] == command_for(bound, config_path, phase) and process['mode'] == mode and process['phase'] == phase, 'executed argv changed')
    c.require(process['config'] == c.digest(config_path, 64*1024), 'consumed configuration changed')


def validate_phase(mode, phase, prep, inventory, resources, *, verify_artifacts=True):
    bound = binding()
    if verify_artifacts:
        c.verify_source(prep)
    config_path = ROOT/f'{mode}-{phase}.json'
    config = c.load(config_path, 64*1024)
    c.require(config['output_dir'] == str(ROOT/f'{mode}-{phase}') and config['execution'] == 'native' and config['attention_profile'] == 'replay_tiled_v1' and config['activation_profile'] == 'layer_recompute_v1', 'wrong configured output/profiles')
    if phase == 'resumed':
        template = c.load(ROOT/f'{mode}-resumed.template.json', 64*1024)
        expected = dict(template)
        expected['expected_restore_state_sha256'] = config['expected_restore_state_sha256']
        c.require(expected == config, 'resolved resume changed fields besides exact state pin')
        c.byte_array(config['expected_restore_state_sha256'])
    process_path = ROOT/'executions'/f'{mode}-{phase}'/'process.json'
    process = c.load(process_path)
    validate_process(process, mode, phase, bound, config_path)
    output = ROOT/f'{mode}-{phase}'
    c.require(stat.S_ISDIR(output.lstat().st_mode), 'output directory is not private regular directory')
    expected_files = {'run.json', 'progress.jsonl', 'result.json', 'latest.safetensors'} | (set() if phase == 'paused' else {'model'})
    c.require({p.name for p in output.iterdir()} == expected_files, 'unexpected/unpublished output files')
    result = c.load(output/'result.json', 64*1024)
    manifest_raw = c.read(output/'run.json', MIB)
    manifest = c.loads(manifest_raw)
    c.config_subset(config, manifest['config'])
    c.require(manifest['format'] == 'antfly.gliner25-training-run/v1' and manifest['source'] == prep['source'] and manifest['backend'] == 'native' and manifest['math_policy'] == 'strict_f32_activations_v1' and manifest['training_contract'] == 'boundary-native-training-v1', 'runtime source/backend contract changed')
    observed = manifest['observed_executable']['digest']
    c.require({'size_bytes': c.integer(observed['size_bytes'], 'executable size'), 'sha256': c.byte_array(observed['sha256']).hex()} == process['binary'], 'actual runtime executable changed')
    c.require(c.byte_array(manifest['train_sha256']).hex() == prep['data']['sha256'] and manifest['evaluation_performed'] is False and manifest['calibration_sha256'] is None and manifest['test_sha256'] is None, 'dataset/evaluation contract changed')
    c.byte_array(manifest['schema_sha256'])
    source = manifest['source_usage']
    source_reserved = c.integer(source['reserved_bytes'], 'source reservation', 1, config['source_limits']['max_source_bytes'])
    c.require(0 < c.integer(source['live_bytes'], 'source live') <= c.integer(source['peak_bytes'], 'source peak') <= source_reserved, 'source owner exceeded')
    expected_admission = source_reserved+config['memory']['job_bytes']+config['dataset_limits']['max_host_bytes']+config['memory']['host_bytes']+config['memory']['backend_bytes']
    c.require(c.integer(manifest['admitted_bytes'], 'admitted bytes') == expected_admission <= resources['outer_job_admission_using_source_cap_upper_bound_bytes'] <= config['memory']['combined_bytes'], 'actual admission formula changed/exceeded')
    c.require(c.integer(result['version'], 'result version') == 1 and result['status'] == ('paused' if phase == 'paused' else 'complete'), 'terminal result status/version')
    c.validate_identity(result['identity'], {'optimizer_step': 0, 'microbatch_step': 1} if phase == 'paused' else {'optimizer_step': 3, 'microbatch_step': 5})
    c.require(c.integer(result['accumulated_microbatches'], 'terminal accumulation') == int(phase == 'paused') and result['run_fingerprint'] == manifest['run_fingerprint'], 'terminal run/count mismatch')
    c.byte_array(result['run_fingerprint']); c.byte_array(result['state_sha256'])
    if phase == 'resumed':
        paused = c.load(ROOT/f'{mode}-paused/result.json', 64*1024)
        c.require(config['expected_restore_state_sha256'] == paused['state_sha256'] and result['run_fingerprint'] == paused['run_fingerprint'] and manifest['restore_receipt'] is not None, 'fresh resume lacks exact state/run pin')
        c.validate_identity(manifest['initial_identity'], {'optimizer_step': 0, 'microbatch_step': 1})
        restore = manifest['restore_receipt']
        c.require(c.integer(restore['version'], 'restore version') == 1 and c.integer(restore['accumulated_microbatches'], 'restore accumulation') == 1 and restore['state_sha256'] == paused['state_sha256'], 'restore receipt state mismatch')
        c.validate_identity(restore['identity'], {'optimizer_step': 0, 'microbatch_step': 1})
        saved = c.digest(ROOT/f'{mode}-paused/latest.safetensors', 16*MIB)
        c.require({'size_bytes': c.integer(restore['checkpoint']['size_bytes'], 'restored checkpoint bytes'), 'sha256': c.byte_array(restore['checkpoint']['sha256']).hex()} == saved, 'restore receipt checkpoint digest mismatch')
    else:
        c.require(manifest['restore_receipt'] is None, 'unexpected restore')
        c.validate_identity(manifest['initial_identity'], {'optimizer_step': 0, 'microbatch_step': 0})
    reports = [c.loads(line) for line in c.read(output/'progress.jsonl', MIB).splitlines()]
    c.validate_reports(reports, phase)
    for report in reports:
        c.require(c.integer(report['host_peak_bytes'], 'host peak') <= config['memory']['host_bytes'] and c.integer(report['backend_peak_bytes'], 'backend peak') <= config['memory']['backend_bytes'] and c.integer(report['resident_device_upper_bound_bytes'], 'resident bytes') == 0, 'native owner exceeded/profile changed')
    events = [c.loads(line) for line in c.read(process_path.parent/'stdout.jsonl', 4*MIB).splitlines()]
    c.require(events == [{'event': 'step', 'report': report} for report in reports]+[{'event': 'result', 'result': result, 'output_dir': str(output)}], 'stdout/durable events disagree')
    slots = inventory['modes'][mode]['slots']
    checkpoint = c.validate_checkpoint(output/'latest.safetensors', slots, phase == 'paused', c.controller_fingerprint(manifest_raw, slots), result['state_sha256'])
    files = {name: c.digest(output/name, 16*MIB if name == 'latest.safetensors' else MIB) for name in ('run.json', 'progress.jsonl', 'result.json', 'latest.safetensors')}
    c.require(files['latest.safetensors'] == checkpoint['file'], 'checkpoint changed between inspections')
    if phase == 'paused':
        c.require(result['portable_model'] is None, 'unfinished window unexpectedly exported')
    else:
        files.update({'model/'+name: value for name, value in c.validate_export(output, config, result, manifest, inventory, checkpoint).items()})
    if verify_artifacts:
        c.verify_source(prep)
    return {'result': result, 'reports': reports, 'checkpoint': checkpoint, 'files': files, 'process': process, 'manifest': manifest}


def phase_receipt(mode, phase, checked):
    return {'format': 'antfly.gliner25-regional-all-native-phase/v1', 'mode': mode, 'phase': phase, 'status': 'pass', 'preparation_sha256': c.PREPARATION_SHA256, 'checker': helper_pins(), 'process_receipt': c.digest(ROOT/'executions'/f'{mode}-{phase}'/'process.json'), 'result': checked['result'], 'checkpoint': checked['checkpoint'], 'files': checked['files'], 'resources': {'admitted_bytes': checked['manifest']['admitted_bytes'], 'host_peak_bytes': max(r['host_peak_bytes'] for r in checked['reports']), 'backend_peak_bytes': max(r['backend_peak_bytes'] for r in checked['reports']), 'sampled_child_tree_rss_bytes': checked['process']['peak_child_tree_rss_bytes']}, 'published_source_numerical_parity': False, 'claim': 'Exact within-native-profile restart/bytes with published source dropout0.1; no PyTorch RNG/VJP/update or cross-backend parity claim.'}


def offline_phase(mode, phase, receipt_name='validation.json'):
    c.require(Path(receipt_name).name == receipt_name and receipt_name.endswith('.json') and receipt_name not in ('process.json', 'start.json'), 'invalid additive receipt name')
    prep, inventory, resources = c.preparation()
    folder = ROOT/'executions'/f'{mode}-{phase}'
    try:
        checked = validate_phase(mode, phase, prep, inventory, resources)
    except BaseException as error:
        with c.protected_receipt():
            c.write_new(folder/(receipt_name.removesuffix('.json')+'-failure.json'), {'format': 'antfly.gliner25-regional-all-native-offline-failure/v1', 'checker': helper_pins(), 'mode': mode, 'phase': phase, 'failure': type(error).__name__+': '+str(error), 'model_execution': False})
        raise
    c.write_new(folder/receipt_name, phase_receipt(mode, phase, checked))
    return checked


def disk_guard(resources):
    info = os.statvfs(ROOT)
    available = info.f_bavail*info.f_frsize
    required = resources['disk']['minimum_initial_free_for_entire_plan_bytes']
    c.require(available >= required, 'private plan disk guard: available'+str(available)+' required'+str(required))
    return {'available_bytes': available, 'required_bytes': required, 'entire_six_phase_growth_upper_bound_bytes': resources['disk']['all_six_phase_growth_upper_bound_bytes'], 'headroom_bytes': resources['disk']['required_free_headroom_bytes']}


def resolve_config(mode, phase, prep, inventory, resources):
    path = ROOT/f'{mode}-{phase}.json'
    if phase != 'resumed':
        return path
    c.require(not path.exists(), 'resolved resume config already exists; no hidden retries')
    paused = validate_phase(mode, 'paused', prep, inventory, resources)
    config = c.load(ROOT/f'{mode}-resumed.template.json', 64*1024)
    c.require(config['expected_restore_state_sha256'] == 'RESOLVE_FROM_VERIFIED_PAUSED_RESULT_BEFORE_EXECUTION', 'invalid resume template')
    config['expected_restore_state_sha256'] = list(c.byte_array(paused['result']['state_sha256']))
    c.write_new(path, config)
    return path


def run(mode, phase):
    # Exclusive invocation ownership is claimed once. A failed preflight leaves
    # a receipt here; the same phase is never retried by this driver.
    folder = ROOT/'executions'/f'{mode}-{phase}'
    folder.parent.mkdir(mode=0o700, exist_ok=True)
    c.require(stat.S_ISDIR(folder.parent.lstat().st_mode), 'executions must be a directory')
    folder.mkdir(mode=0o700)
    process = {'format': 'antfly.gliner25-regional-all-native-process/v1', 'mode': mode, 'phase': phase, 'failure': None, 'failure_phase': 'preflight', 'returncode': None, 'cleanup': None, 'model_launch_attempted': False}
    started = time.monotonic()
    try:
        prep, inventory, resources = c.preparation()
        bound = binding(require_current_helpers=True)
        process.update({'preparation_sha256': c.PREPARATION_SHA256, 'binary': exact_digest(bound['binary']), 'helpers': helper_pins(), 'supervisor': bound['supervisor'], 'executable_binding': c.digest(ROOT/'executable.json', 64*1024), 'disk_guard': disk_guard(resources)})
        c.require(not (ROOT/f'{mode}-{phase}').exists(), 'output already exists')
        c.verify_source(prep)
        if mode == 'dora':
            prior = c.load(ROOT/'lora-validation.json')
            c.require(prior['status'] == 'pass' and prior['executable_binding'] == process['executable_binding'], 'LoRA sequence must complete before DoRA')
        if phase == 'uninterrupted':
            validate_phase(mode, 'resumed', prep, inventory, resources)
        config_path = resolve_config(mode, phase, prep, inventory, resources)
        process.update({'config': c.digest(config_path, 64*1024), 'command': command_for(bound, config_path, phase)})
        supervisor, pin = load_supervision()
        c.require(pin == bound['supervisor'], 'supervisor binding changed')
        c.write_new(folder/'start.json', process)
        process['model_launch_attempted'] = True
        process.update(supervisor.run(process['command'], folder/'stdout.jsonl', folder/'stderr.log', timeout_seconds=TIMEOUT, rss_limit_bytes=RSS_LIMIT, output_limit_bytes=4*MIB))
        c.require(process['failure'] is None and process['returncode'] == 0 and process['cleanup']['complete'], process['failure'] or 'incomplete process cleanup')
        process['failure_phase'] = 'postflight'
        binding(require_current_helpers=True)
        c.require(process['config'] == c.digest(config_path, 64*1024), 'config changed during invocation')
        c.verify_source(prep)
        process['failure_phase'] = None
    except BaseException as error:
        if process['failure'] is None:
            process['failure'] = type(error).__name__+': '+str(error)
        raise
    finally:
        process['driver_elapsed_seconds'] = time.monotonic()-started
        with c.protected_receipt():
            c.write_new(folder/'process.json', process)
    checked = offline_phase(mode, phase)
    print(json.dumps({'status': 'pass', 'mode': mode, 'phase': phase, 'result': checked['result']}, allow_nan=False))


def validate_all(mode):
    prep, inventory, resources = c.preparation()
    checked = {phase: validate_phase(mode, phase, prep, inventory, resources) for phase in PHASES}
    c.require(len({x['process']['executable_binding']['sha256'] for x in checked.values()}) == 1, 'phase executable bindings differ')
    c.compare_continuity(checked['uninterrupted'], checked['paused'], checked['resumed'])
    receipt = {'format': 'antfly.gliner25-regional-all-native-resume/v1', 'status': 'pass', 'mode': mode, 'preparation_sha256': c.PREPARATION_SHA256, 'executable_binding': c.digest(ROOT/'executable.json', 64*1024), 'checker': helper_pins(), 'source': prep['source'], 'dataset': prep['data'], 'profiles': prep['semantic_changes'], 'microbatches': 5, 'updates': 3, 'flush_after': [2, 4, 5], 'zero_loss_fallback': [False]*5, 'result': checked['uninterrupted']['result'], 'artifacts': checked['uninterrupted']['files'], 'phases': {name: phase_receipt(mode, name, value) for name, value in checked.items()}, 'quality_or_published_source_numerical_parity': False}
    c.write_new(ROOT/f'{mode}-validation.json', receipt)
    print(json.dumps({'status': 'pass', 'mode': mode, 'result': receipt['result']}, allow_nan=False))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    bind_parser = commands.add_parser('bind')
    for name in ('binary', 'binary-sha256', 'build-receipt', 'build-receipt-sha256', 'source-inventory', 'source-archive-receipt', 'source-archive-receipt-sha256', 'source-archive'):
        bind_parser.add_argument('--'+name, required=True)
    bind_parser.add_argument('--entrypoint', required=True, choices=('standalone', 'public-runtime'))
    commands.add_parser('inspect')
    for name in ('run', 'validate-phase', 'validate'):
        sub = commands.add_parser(name)
        sub.add_argument('--mode', choices=('lora', 'dora'), required=True)
        if name != 'validate':
            sub.add_argument('--phase', choices=PHASES, required=True)
        if name == 'validate-phase':
            sub.add_argument('--receipt-name', default='validation.json')
    args = parser.parse_args()
    def interrupted(signum, frame):
        raise KeyboardInterrupt('signal '+str(signum))
    signal.signal(signal.SIGINT, interrupted)
    signal.signal(signal.SIGTERM, interrupted)
    if args.command == 'bind':
        bind(args)
    elif args.command == 'inspect':
        prep, inventory, resources = c.preparation()
        print(json.dumps({'preparation_sha256': c.PREPARATION_SHA256, 'slots': {mode: inventory['modes'][mode]['slot_count'] for mode in ('lora', 'dora')}, 'disk': disk_guard(resources), 'model_execution': False}, allow_nan=False))
    elif args.command == 'run':
        run(args.mode, args.phase)
    elif args.command == 'validate-phase':
        offline_phase(args.mode, args.phase, args.receipt_name)
    else:
        validate_all(args.mode)


if __name__ == '__main__':
    main()
