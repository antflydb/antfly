"""Prepare a fresh campaign only. No process, model, binding or source copy."""
import copy
import hashlib
import json
import os
from pathlib import Path
import stat

ROOT = Path(__file__).resolve().parent
PRIOR = Path('/private/tmp/antfly-gliner25-regional-all-small-native-v1')
REPO = Path('/Users/timkaye/Documents/af/antfly')
MIB = 1024**2
PRIOR_PREPARATION_SHA = 'd86dafd5c9c826c65ef132d6b9b6d4e161e0a503101a9e1abdc230dab9ecfa98'
PRIOR_BINARY_SHA = 'f482989317fd34c0a8852d5a36d837fb78f6cc57862863f5a0b77981125f02b8'


def read(path, maximum=2*MIB):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC)
    try:
        before = os.fstat(fd)
        assert stat.S_ISREG(before.st_mode) and 0 <= before.st_size <= maximum
        raw = bytearray()
        while len(raw) < before.st_size:
            block = os.read(fd, min(MIB, before.st_size-len(raw)))
            assert block
            raw.extend(block)
        after = os.fstat(fd)
        assert (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) == (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns)
        return bytes(raw)
    finally:
        os.close(fd)


def pin(raw):
    return {'size_bytes': len(raw), 'sha256': hashlib.sha256(raw).hexdigest()}


def file_pin(path, maximum=32*MIB):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC)
    try:
        before = os.fstat(fd)
        assert stat.S_ISREG(before.st_mode) and 0 <= before.st_size <= maximum
        digest = hashlib.sha256()
        total = 0
        while total < before.st_size:
            block = os.read(fd, min(MIB, before.st_size-total))
            assert block
            total += len(block)
            digest.update(block)
        after = os.fstat(fd)
        assert (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) == (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns)
        return {'size_bytes': total, 'sha256': digest.hexdigest()}
    finally:
        os.close(fd)


def tree_pins():
    result = {}
    for path in sorted(PRIOR.rglob('*')):
        kind = path.lstat().st_mode
        assert stat.S_ISDIR(kind) or stat.S_ISREG(kind), path
        if stat.S_ISREG(kind):
            result[str(path.relative_to(PRIOR))] = file_pin(path)
    return result


def save_bytes(name, raw):
    assert len(raw) <= 2*MIB
    with (ROOT/name).open('xb') as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    return pin(raw)


def save(name, value):
    return save_bytes(name, (json.dumps(value, indent=2, allow_nan=False)+'\n').encode())


before = tree_pins()
raw_prior = read(PRIOR/'preparation.json')
assert pin(raw_prior)['sha256'] == PRIOR_PREPARATION_SHA
prior = json.loads(raw_prior)
for key, filename in (('inventory', 'inventory.json'), ('resource_plan', 'resource_plan.json'), ('qualification_plan', 'qualification_plan.json')):
    assert pin(read(PRIOR/filename)) == prior[key]
for name, expected in prior['configs'].items():
    assert pin(read(PRIOR/name)) == expected
assert file_pin(Path(prior['data']['path'])) == {key: prior['data'][key] for key in ('size_bytes', 'sha256')}
old_binding = json.loads(read(PRIOR/'executable.json'))
assert old_binding['binary']['sha256'] == PRIOR_BINARY_SHA
old_paused = json.loads(read(PRIOR/'executions/lora-paused/validation.json'))
old_denied = json.loads(read(PRIOR/'executions/lora-resumed/process.json'))
assert old_paused['status'] == 'pass'
assert old_denied['returncode'] == 1 and old_denied['cleanup']['complete'] is True
assert b'TrainingOptimizerLimitExceeded' in read(PRIOR/'executions/lora-resumed/stderr.log')
history_pin = save('prior_campaign.json', {
    'format': 'antfly.gliner25-regional-all-native-prior-campaign/v1',
    'path': str(PRIOR), 'preparation': pin(raw_prior), 'binary_sha256': PRIOR_BINARY_SHA,
    'historical_status': 'V1 LoRA pause passed; fresh resume received TrainingOptimizerLimitExceeded before an update. Both original receipts remain untouched.',
    'new_campaign_success_inferred': False, 'checkpoint_reuse': False, 'copied_results': False,
    'files_before_and_after_preparation': before,
})
inventory_raw = read(PRIOR/'inventory.json')
inventory_pin = save_bytes('inventory.json', inventory_raw)
inventory = json.loads(inventory_raw)
assert inventory['module_count'] == 131
configs = {}
for name in prior['configs']:
    raw = read(PRIOR/name)
    changed = raw.replace(str(PRIOR).encode(), str(ROOT).encode())
    old_value, value = json.loads(raw), json.loads(changed)
    expected = copy.deepcopy(old_value)
    expected['output_dir'] = str(ROOT/Path(old_value['output_dir']).name)
    if 'resume_from' in expected:
        expected['resume_from'] = str(ROOT/Path(old_value['resume_from']).relative_to(PRIOR))
    assert value == expected
    assert value['memory']['optimizer_transaction_bytes'] == 32*MIB
    assert value['source_dir'] == prior['source_dir'] and value['expected_source'] == prior['source']
    configs[name] = save_bytes(name, changed)

resource = json.loads(read(PRIOR/'resource_plan.json'))
original_resource = copy.deepcopy(resource)
source_locations = {name: item['path'] for name, item in resource['code_formula_pins'].items()}
source_locations.update({
    'run_plan': 'zig/pkg/inference/src/finetune/gliner_boundary_run.zig',
    'dataset': 'zig/pkg/inference/src/finetune/gliner_boundary_dataset.zig',
})
resource['code_formula_pins'] = {name: {'path': path, **pin(read(REPO/path))} for name, path in source_locations.items()}
controller = read(REPO/source_locations['controller']).decode()
required = (
    'const remaining = self.limits.max_transaction_bytes - staging_bytes;',
    'if (snapshot_bytes >= remaining) return error.TrainingOptimizerLimitExceeded;',
    'const header_heap_bytes = @min(self.limits.max_checkpoint_header_heap_bytes, remaining - snapshot_bytes);',
    '.limit = header_heap_bytes',
)
for line in required:
    assert line in controller, line
restore = {}
for mode in ('lora', 'dora'):
    slots = inventory['modes'][mode]['slots']
    payload = inventory['modes'][mode]['parameter_payload_bytes']
    staging = 4*payload + sum(2*len(s['name'].encode()) + 4*len(s['shape']) + 1024 for s in slots)
    snapshot_bound = resource['disk']['modes'][mode]['checkpoint_job_upper_bound_bytes']
    parser_bound = 32*MIB-staging-snapshot_bound
    assert 0 < parser_bound < 64*MIB
    restore[mode] = {'native_staging_formula_bytes': staging, 'snapshot_job_upper_bound_bytes': snapshot_bound, 'parser_capacity_at_snapshot_upper_bound_bytes': parser_bound}
resource['campaign_version'] = 2
resource['restore_transaction_accounting'] = {
    'transaction_cap_bytes': 32*MIB, 'default_header_heap_ceiling_bytes': 64*MIB,
    'formula': 'staging = 4*adapter_payload + sum(2*name_bytes + 4*rank + 1024); require staging + actual_snapshot < transaction cap; parser allowance = min(default64MiB, transaction cap - staging - actual_snapshot).',
    'source_path': source_locations['controller'],
    'source_lines': [next(i for i, line in enumerate(controller.splitlines(), 1) if wanted in line) for wanted in required],
    'modes': restore,
    'parent_owner_note': 'Existing host owner separately charges descriptor/metadata allocations. These are static admission terms, not measured runtime peaks or a successful restore.',
    'caps_changed': False, 'measured_in_this_campaign': False,
}
for key, value in original_resource.items():
    if key != 'code_formula_pins':
        assert resource[key] == value
resource_pin = save('resource_plan.json', resource)
proposal = json.loads(read(PRIOR/'qualification_plan.json'))
proposal['campaign_version'] = 2
proposal['binary_binding'] = 'Require a newly frozen production executable, exact observed standalone/help identity, build receipt, frozen source inventory and archive. Formula source pins must match that inventory. Reject f4829893 and all older executables. No executable is selected during preparation; never rehash live checkout files during execution.'
proposal['argv_template'] = ['NEW_STANDALONE_BINARY', str(ROOT/'lora-paused.json'), '--shutdown-grace-seconds', '30', '--stop-after-microbatches', '1']
proposal['checkpoint_validation']['other_boundary_head_slots'] = 'Exact fixed-schema route contract:14 dormant modules at0 updates,115 modules at3 and2 classifier modules at2 final updates; paused117 present/14 absent. Full262/393 slot inventory remains enrolled. No v1 checkpoint is reused.'
proposal['bounded_checker_proposal'] = 'Version2 reuses the exact v1 bounded streaming/counter/identity checker, changing only preparation pin and fresh-build source admission. Existing nine synthetic checks and new version2 preservation/binding checks must pass before root may bind. No model is launched by preparation or tests.'
proposal['prior_campaign'] = {'path': str(PRIOR), 'manifest': history_pin, 'success_inherited': False}
proposal_pin = save('qualification_plan.json', proposal)
prep = copy.deepcopy(prior)
prep.update({
    'campaign_version': 2, 'campaign_id': ROOT.name, 'prior_campaign': history_pin,
    'inventory': inventory_pin, 'resource_plan': resource_pin, 'qualification_plan': proposal_pin,
    'configs': configs, 'status': 'Prepared only; no executable bound or campaign process launched. V1 remains immutable; fresh CLI with corrected restore accounting is required.',
})
prep_sha = save('preparation.json', prep)['sha256']
source_helper_pins = {}
for name in ('checker.py', 'run_phase.py', 'test_checker.py'):
    source = read(PRIOR/name)
    source_helper_pins[name] = pin(source)
    if name in old_binding['helpers']:
        assert pin(source) == old_binding['helpers'][name]
    if name == 'checker.py':
        assert source.count(PRIOR_PREPARATION_SHA.encode()) == 1
        source = source.replace(PRIOR_PREPARATION_SHA.encode(), prep_sha.encode())
    elif name == 'run_phase.py':
        old = b"LEGACY = {"
        assert source.count(old) == 1
        source = source.replace(old, ("LEGACY = {'"+PRIOR_BINARY_SHA+"', ").encode(), 1)
    save_bytes(name, source)
save('helper_origin.json', {'format': 'antfly.gliner25-regional-all-native-helper-origin/v1', 'prior_directory': str(PRIOR), 'source_helpers': source_helper_pins, 'prior_preparation_sha256': PRIOR_PREPARATION_SHA, 'current_preparation_sha256': prep_sha, 'binding_pending': True, 'model_execution': False, 'note': 'New helper source edits and their final pins are recorded in the version2 checkpoint; prior helpers are never modified.'})
assert tree_pins() == before, 'V1 changed during preparation'
assert not (ROOT/'executable.json').exists() and not (ROOT/'executions').exists()
print(json.dumps({'directory': str(ROOT), 'preparation_sha256': prep_sha, 'inventory_unchanged': inventory_pin == prior['inventory'], 'all_memory_caps_unchanged': True, 'v1_unchanged': True, 'new_model_execution': False, 'restore_static_terms': restore}))
