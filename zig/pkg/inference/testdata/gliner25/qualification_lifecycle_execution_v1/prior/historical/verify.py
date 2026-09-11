#!/usr/bin/env python3
"""Offline integrity and evidence-chain checks; no child process or original artifact access."""
from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
from pathlib import Path
import re
import stat


def digest(raw):
    return dict(size_bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest())


def read(root, name):
    path = Path(name)
    assert not path.is_absolute() and '..' not in path.parts
    for part in path.parents:
        assert not (root / part).is_symlink()
    with os.fdopen(os.open(root / path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK), 'rb') as stream:
        before = os.fstat(stream.fileno())
        assert stat.S_ISREG(before.st_mode) and 0 <= before.st_size <= 1024**2
        raw = stream.read(1024**2 + 1)
        after = os.fstat(stream.fileno())
        assert len(raw) == before.st_size
        assert (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) == (
            after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns)
    return raw


def unique(pairs):
    result = {}
    for key, value in pairs:
        assert key not in result, key
        result[key] = value
    return result


def load(raw):
    return json.loads(raw, object_pairs_hook=unique, parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)))


def bare(pin):
    return {key: pin[key] for key in ('sha256', 'size_bytes')}


def validate(ledger, contents):
    assert ledger['scope'] == 'gliner25_qualification_lifecycle_evidence/v1'
    assert ledger['version'] == 1 and ledger['qualification'] is False and ledger['local_only'] is True
    assert ledger['storage']['binaries_included'] is False and ledger['storage']['full_source_archives_included'] is False
    assert ledger['storage']['source_inventory_is_dependency_closure'] is False
    assert ledger['storage']['maximum_bytes'] == 8 * 1024**2
    assert ledger['storage']['payload_bytes_excluding_ledger'] == sum(map(len, contents.values()))
    assert [run['id'] for run in ledger['checkpoints']] == ['v1', 'v2', 'v3']
    assert len(ledger['checkpoints']) == 3

    def artifact(ref):
        assert ref['path'] in ledger['files']
        raw = contents[ref['path']]
        assert digest(raw) == bare(ref) == ledger['files'][ref['path']]
        return raw

    for run in ledger['checkpoints']:
        receipt = load(artifact(run['raw']['process.json']))
        start = load(artifact(run['raw']['start.json']))
        inventory_raw = artifact(run['raw']['source_inventory.json'])
        inventory = load(inventory_raw)
        process = receipt['process']
        assert all(receipt[key] == value for key, value in start.items())
        assert digest(inventory_raw) == start['source_inventory'] == run['source_inventory']
        assert len(inventory) == run['source_count'] == start['source_count'] == 2587
        assert start['source_selection'] == run['source_selection']
        assert start['qualification'] is False and process['returncode'] == run['returncode'] == 1
        assert process['source_inventory_unchanged'] is True and process['changed_sources'] == []
        assert run['source_unchanged_after_run'] is True
        for key in ('command', 'cwd', 'environment'):
            assert start[key] == run[key]
        for key in ('elapsed_seconds', 'peak_child_tree_rss_bytes', 'rss_measurement',
                    'optional_executable_identity_errors', 'failure', 'failure_phase', 'inspection_error_count'):
            assert process[key] == run[key]
        assert process['observed_test_executables'] == run['observed_test_executables']
        for i, value in enumerate(process['observed_test_executables'], 1):
            assert load(artifact(run['raw'][f'observed-test-{i}.json'])) == value
        assert process['cleanup'] == run['process_cleanup']
        for key in ('complete', 'direct_child_reaped', 'known_children_gone'):
            assert process['cleanup'][key] is True
        for key in ('survivors', 'errors', 'inspection_errors'):
            assert process['cleanup'][key] == []
        assert all(process[key] == value for key, value in run['guards'].items())
        assert process['timeout_seconds'] == 1800 and process['max_child_tree_rss_bytes'] == 6 * 1024**3
        assert process['peak_child_tree_rss_bytes'] <= process['max_child_tree_rss_bytes']
        for key in ('stdout', 'stderr'):
            assert digest(artifact(run['raw'][key + '.log'])) == process[key]
            assert process[key]['size_bytes'] <= process['max_' + key + '_bytes'] == 8 * 1024**2
        stderr = artifact(run['raw']['stderr.log']).decode()
        assert '23/23 tests passed' in stderr
        assert run['ancillary_build_tests'] == dict(passed=23, total=23, is_main_selected_suite=False)
        if run['id'] == 'v1':
            assert run['outcome'] == 'compile_failure' and run['main_test_totals'] is None
            assert run['main_executable'] is None and run['original_source_archive'] is None
            assert 'switch must handle all possibilities' in stderr and "unhandled enumeration value: 'allow'" in stderr
            assert not re.search(r'^\d+/\d+ ', stderr, re.M)
            assert len(run['observed_test_executables']) == 1
            assert '--listen=-' in run['observed_test_executables'][0]['argv']
        else:
            assert run['outcome'] == 'test_failure'
            results = re.findall(r'(\d+) selected; (\d+) passed; (\d+) skipped; (\d+) failed; (\d+) leaked', stderr)
            assert len(results) == 1
            counts = dict(zip(('selected', 'passed', 'skipped', 'failed', 'leaked'), map(int, results[0])))
            assert counts == run['main_test_totals']
            assert counts['failed'] == counts['skipped'] == 1 and counts['leaked'] == 0
            assert counts['passed'] + counts['skipped'] + counts['failed'] == counts['selected']
            archive = load(artifact(run['archive_receipt']))
            assert archive['tests'] == counts
            assert archive['process_receipt'] == digest(artifact(run['raw']['process.json']))
            assert archive['source_inventory'] == run['source_inventory']
            assert bare(archive['frozen_executable']) == bare(run['main_executable'])
            assert archive['live_observation'] == run['observed_test_executables'][1]
            assert bare(archive['live_observation']['executable']) == bare(run['main_executable'])
            assert archive['source_archive'] == bare(run['original_source_archive'])
            assert run['main_executable']['bytes_included'] is False and run['main_executable']['rehashed'] is True
            assert run['original_source_archive']['bytes_included'] is False and run['original_source_archive']['rehashed'] is True
            assert run['archivist_failure_interpretation'] == archive['failure']
            if run['id'] == 'v2':
                assert 'warm_physical.device_owned_live_bytes > initial_physical.device_owned_live_bytes' in stderr
            else:
                assert 'FAIL (Timeout)' in stderr and 'try active.check();' in stderr
        assert len(run['selected_sources']) == ledger['storage']['selected_sources_per_checkpoint'] == 13
        assert len({source['repo_path'] for source in run['selected_sources']}) == 13
        for source in run['selected_sources']:
            raw = artifact(source['artifact'])
            assert digest(raw) == inventory[source['repo_path']]
        for name in ('supervisor', 'wrapper'):
            assert digest(artifact(ledger['helper_sources'][name])) == start[name] == run['helper_pins'][name]
        reason = run['failure_interpretation']
        source = next(item for item in run['selected_sources'] if item['repo_path'] == reason['source'])
        assert artifact(source['artifact']).decode().splitlines()[reason['line'] - 1] == reason['source_line']

    runs = ledger['contracts']['runs']
    assert [run['id'] for run in runs] == ['handoff', 'clean-imports', 'isolated']
    for run in runs:
        text = artifact(run['raw']).decode()
        result = re.findall(r'^Ran (\d+) tests in ([0-9.]+)s$', text, re.M)
        assert len(result) == 1 and (int(result[0][0]), float(result[0][1])) == (run['tests_run'], run['reported_seconds'])
        assert text.rstrip().endswith('\n' + run['outcome'])
        assert run['exact_runtime_command_recorded'] is False and run['interpreter_identity_recorded'] is False
        if run['id'] == 'clean-imports':
            assert run['tests_run'] == 201 and run['outcome'] == 'FAILED (errors=25)' and run['errors'] == 25
            assert run['missing_modules'] == sorted(set(re.findall(r"ModuleNotFoundError: No module named '([^']+)'", text)))
        else:
            assert run['tests_run'] == 203 and run['outcome'] == 'OK' and run['errors'] == 0
    assert ledger['contracts']['remote_ci_executed'] is False
    snapshots = ledger['contracts']['source_snapshots']
    requirements = artifact(snapshots['zig/pkg/inference/scripts/gliner25/requirements-contract.txt']).decode()
    assert [line for line in requirements.splitlines() if line and not line.startswith('#')] == ledger['contracts']['exact_minimal_dependencies']
    workflow = artifact(snapshots['.github/workflows/zig-tests.yml']).decode()
    assert workflow.count('uv run --no-project --isolated --no-python-downloads --no-build') == 2
    assert b'gliner25' in artifact(snapshots['zig/pkg/inference/scripts/run_model_contract_tests.py'])
    assert b'requirements-contract.txt' in artifact(ledger['contracts']['current_diff'])


def main():
    assert __debug__, 'Run without -O; integrity checks are mandatory.'
    parser = argparse.ArgumentParser()
    parser.add_argument('directory', type=Path)
    parser.add_argument('--self-test', action='store_true')
    args = parser.parse_args()
    root = args.directory.absolute()
    assert root.is_dir() and not root.is_symlink()
    raw = read(root, 'ledger.json')
    ledger = load(raw)
    actual = {str(path.relative_to(root)) for path in root.rglob('*') if not path.is_dir()}
    assert actual == set(ledger['files']) | {'ledger.json'}, 'Unrecorded or missing artifact.'
    contents = {name: read(root, name) for name in ledger['files']}
    assert all(digest(value) == ledger['files'][name] for name, value in contents.items())
    assert sum(map(len, contents.values())) + len(raw) <= 8 * 1024**2
    validate(ledger, contents)
    rejected = []
    if args.self_test:
        def wrong_binary(value):
            value['checkpoints'][1]['main_executable']['sha256'] = value['checkpoints'][2]['main_executable']['sha256']
        def wrong_source(value):
            value['checkpoints'][0]['selected_sources'][0]['repo_path'] = value['checkpoints'][0]['selected_sources'][1]['repo_path']
        def promote_failure(value):
            value['checkpoints'][1]['main_test_totals']['failed'] = 0
        for name, mutation in [('mismatched_executable', wrong_binary), ('mismatched_source', wrong_source), ('failed_aggregate_promoted', promote_failure)]:
            bad = copy.deepcopy(ledger)
            mutation(bad)
            try:
                validate(bad, contents)
            except (AssertionError, KeyError, ValueError):
                rejected.append(name)
            else:
                raise AssertionError('Accepted adversarial evidence: ' + name)
    print(json.dumps(dict(scope='gliner25_qualification_lifecycle_evidence_verification/v1',
                          qualification=False, status='passed', ledger=digest(raw),
                          artifacts=len(contents), bytes=sum(map(len, contents.values())) + len(raw),
                          historical_failed_zig_checkpoints=3, retained_failed_dependency_run=1,
                          local_203_test_passes=2, adversarial_rejections=rejected), sort_keys=True))


if __name__ == '__main__':
    main()
