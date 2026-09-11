"""Verify retained evidence only; no child, model, network or original paths."""
import hashlib
import json
import os
from pathlib import Path
import stat

ROOT = Path(__file__).resolve().parent


def digest(path):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC)
    try:
        before = os.fstat(fd)
        assert stat.S_ISREG(before.st_mode) and before.st_size <= 2*1024*1024
        value = hashlib.sha256()
        total = 0
        while total < before.st_size:
            block = os.read(fd, min(1024*1024, before.st_size-total))
            assert block
            value.update(block)
            total += len(block)
        after = os.fstat(fd)
        assert (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) == (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns)
        return {'size_bytes': total, 'sha256': value.hexdigest()}
    finally:
        os.close(fd)


manifest = json.loads((ROOT/'manifest.json').read_bytes())
assert manifest['format_version'] == 1 and manifest['release_qualification'] is False
assert len(manifest['files']) == 42
total = 0
for relative, expected in manifest['files'].items():
    path = Path(relative)
    assert not path.is_absolute() and '..' not in path.parts
    actual = digest(ROOT/path)
    assert actual == {key: expected[key] for key in ('size_bytes', 'sha256')}, relative
    total += actual['size_bytes']
assert total == 2007202
for version in (1, 2, 3):
    receipt = json.loads((ROOT/f'cpu_v{version}/process.json').read_bytes())
    process = receipt['process']
    assert process['cleanup']['complete'] is True and not process['cleanup']['errors']
    assert not process['cleanup']['survivors'] and not process['cleanup']['inspection_errors']
    assert process['source_inventory_unchanged'] is True and not process['changed_sources']
assert manifest['cpu_attempts']['cpu_v3']['tests']['target_passed'] == 9
assert manifest['cpu_attempts']['cpu_v3']['tests']['target_skipped'] == 1
report = json.loads((ROOT/'process_children/report.json').read_bytes())
assert report['pass'] is True and report['binary_unchanged'] is True
assert len(report['cases']) == 7 and report['expected_watchdog_exit'] == 86
for case in report['cases']:
    assert case['pass'] is True and case['exit_code'] == 86 and case['outer_failure'] is None
    assert case['reaped'] is True and case['lease_order_evidence'] is (case['mode'] != 'escaped')
    for stream in ('stdout', 'stderr'):
        assert digest(ROOT/'process_children'/f'{case["mode"]}.{stream}.log') == case[stream]
inventory = json.loads((ROOT/'cpu_v3/source_inventory.json').read_bytes())
for original, relative in manifest['frozen_build']['selected_source_snapshots'].items():
    assert digest(ROOT/relative) == inventory[original]
print(json.dumps({'verified_files': 42, 'verified_bytes': total, 'cpu_target_passed': 9, 'expected_child_skip': 1, 'watchdog_children_passed': 7, 'model_or_child_execution': False}))
