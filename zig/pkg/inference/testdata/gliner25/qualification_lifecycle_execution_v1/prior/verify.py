#!/usr/bin/env python3
"""Offline follow-up evidence validation; imports only the pinned local stdlib verifier."""
from pathlib import Path
import argparse
import copy
import hashlib
import json
import os
import re
import stat


def validate(ledger, contents, old):
    digest, load = old['digest'], old['load']

    def raw(ref):
        value = contents[ref['path']]
        assert digest(value) == old['bare'](ref) == ledger['files'][ref['path']]
        return value

    def pin(ref):
        return old['bare'](ref)

    assert ledger['scope'] == 'gliner25_qualification_lifecycle_evidence/v2'
    assert ledger['version'] == 2 and ledger['qualification'] is False and ledger['local_only'] is True
    assert ledger['storage']['binaries_included'] is ledger['storage']['full_source_archives_included'] is False
    assert ledger['storage']['maximum_bytes'] == 8 * 1024**2
    assert ledger['storage']['payload_bytes_excluding_ledger'] == sum(map(len, contents.values()))
    historic_raw = raw(ledger['historical_ledger'])
    assert digest(historic_raw)['sha256'] == '9acc6ffcca8724acdd4247dd7a8e4c2b0295478f8cf9bbd2bb5c3f13b0aaafc3'
    historical = load(historic_raw)
    old_contents = {name: contents['historical/' + name] for name in historical['files']}
    assert all(digest(value) == historical['files'][name] for name, value in old_contents.items())
    old['validate'](historical, old_contents)
    assert [run['id'] for run in ledger['final_focused']] == ['final_focused_v1', 'final_focused_v2']
    for run in ledger['final_focused']:
        receipt = load(raw(run['raw']['process.json']))
        start = load(raw(run['raw']['start.json']))
        inventory_raw = raw(run['raw']['source_inventory.json'])
        inventory = load(inventory_raw)
        process = receipt['process']
        assert all(receipt[key] == value for key, value in start.items())
        assert start['source_inventory'] == digest(inventory_raw) == run['source_inventory']
        assert start['source_count'] == len(inventory) == run['source_count'] == 2588
        for name in ('command', 'environment', 'cwd', 'source_selection'):
            assert start[name] == run[name]
        for name in ('returncode', 'elapsed_seconds', 'peak_child_tree_rss_bytes', 'cleanup', 'observed_test_executables'):
            assert process[name] == run[name]
        assert run['returncode'] == 1
        assert process['source_inventory_unchanged'] is True and process['changed_sources'] == []
        assert all(process['cleanup'][name] is True for name in ('complete', 'direct_child_reaped', 'known_children_gone'))
        assert all(process['cleanup'][name] == [] for name in ('survivors', 'errors', 'inspection_errors'))
        assert process['peak_child_tree_rss_bytes'] <= process['max_child_tree_rss_bytes'] == 6 * 1024**3
        assert process['elapsed_seconds'] <= process['timeout_seconds'] == 1800
        for channel in ('stdout', 'stderr'):
            assert digest(raw(run['raw'][channel + '.log'])) == process[channel]
        for i, observation in enumerate(process['observed_test_executables'], 1):
            assert load(raw(run['raw'][f'observed-test-{i}.json'])) == observation
        for name in ('supervisor', 'wrapper'):
            assert start[name] == run['helper_pins'][name] == historical['checkpoints'][0]['helper_pins'][name]
        assert len(run['selected_sources']) == ledger['storage']['selected_sources_per_new_checkpoint'] == 17
        assert len({source['repo_path'] for source in run['selected_sources']}) == 17
        for source in run['selected_sources']:
            assert digest(raw(source['artifact'])) == inventory[source['repo_path']]
        failure = run['failure_interpretation']
        source = next(source for source in run['selected_sources'] if source['repo_path'] == failure['repo_path'])
        assert raw(source['artifact']).decode().splitlines()[failure['line'] - 1] == failure['source_line']
        text = raw(run['raw']['stderr.log']).decode()
        assert '23/23 tests passed' in text
        assert run['ancillary_build_tests'] == dict(passed=23, total=23, is_main_selected_suite=False)
        if run['id'] == 'final_focused_v1':
            assert run['outcome'] == 'compile_failure' and run['main_test_totals'] is None and run['main_executable'] is None
            assert 'expected optional type' in text and 'gliner_boundary_long_tasks_test.zig:291:9' in text
            assert not re.search(r'^\d+/\d+ ', text, re.M)
        else:
            assert run['outcome'] == 'test_failure'
            counts = re.findall(r'(\d+) selected; (\d+) passed; (\d+) skipped; (\d+) failed; (\d+) leaked', text)
            assert len(counts) == 1 and tuple(map(int, counts[0])) == (67, 65, 1, 1, 0)
            assert run['main_test_totals'] == dict(selected=67, passed=65, skipped=1, failed=1, leaked=0)
            assert 'expected 0, found 1' in text and 'gliner_boundary_cache_lifecycle_test.zig:373:44' in text
            assert failure['code'] == 'metrics_observation_refreshes_idle_timestamp'
            archive = load(raw(run['archive_receipt']))
            assert archive['source_inventory'] == start['source_inventory'] and archive['source_files'] == 2588
            assert archive['tests'] == run['main_test_totals'] and archive['process_receipt'] == digest(raw(run['raw']['process.json']))
            assert pin(archive['frozen_executable']) == pin(run['main_executable'])
            assert archive['live_observation'] == run['observed_test_executables'][1]
            assert pin(archive['live_observation']['executable']) == pin(run['main_executable'])
            assert archive['source_archive'] == pin(run['original_source_archive'])
            assert archive['failure'] == run['archivist_failure_interpretation']
            assert run['main_executable']['bytes_included'] is False and run['main_executable']['rehashed'] is True
            passed = re.findall(r'^\d+/\d+ (.+?)\.\.\.OK$', text, re.M)
            assert run['scoped_passes'] == [name for name in passed if any(key in name for key in ('learned multi window', 'terminal allocation', 'model manager teardown'))]
    probe = ledger['teardown_process']
    report = load(raw(probe['report']))
    assert digest(raw(probe['report']))['sha256'] == '4fa0732a535c7777c9a93aafc5c272de11b4c5938835d93831f755e9f379c41c'
    assert report['binary'] == probe['binary'] == pin(ledger['final_focused'][1]['main_executable'])
    assert report['binary_unchanged'] is True and report['pass'] is True and report['qualification'] is False
    assert report['expected_watchdog_exit'] == 86 and report['limits'] == dict(child_seconds=5, reap_seconds=5, stream_bytes=1048576)
    assert digest(raw(probe['driver'])) == report['driver']
    modes = ['cache', 'ttl', 'admission', 'retired', 'shutdown', 'rollback', 'escaped', 'stderr-close', 'stderr-return']
    assert [case['mode'] for case in report['cases']] == modes and probe['cases'] == probe['passed'] == 9
    assert probe['exit_codes'] == [86] * 9 and probe['no_model'] is True and probe['no_outer_kills'] is True
    assert probe['escaped_session_has_no_admission_lease'] is True
    for case in report['cases']:
        mode = case['mode']
        stdout = raw(probe['raw'][mode + '.stdout.log'])
        stderr = raw(probe['raw'][mode + '.stderr.log'])
        assert digest(stdout) == case['stdout'] and digest(stderr) == case['stderr']
        assert max(len(stdout), len(stderr)) <= 1048576
        markers = stderr + stdout
        assert case['exit_code'] == 86 and case['pass'] is True and case['reaped'] is True and case['outer_failure'] is None
        assert case['elapsed_seconds'] < 5
        assert case['expected_destructor_entered'] is True and case['started_expected_operation'] is True
        assert stderr.count(f'teardown-fixture operation-start:{mode}\n'.encode()) == 1
        counts = (markers.count(b'teardown-fixture close-entered lease-held64\n'),
                  markers.count(b'teardown-fixture close-entered no-lease\n'),
                  markers.count(b'teardown-fixture cache-entered primary-and-optional-active lease-held64\n'))
        assert counts == ((0, 0, 1) if mode == 'cache' else (0, 1, 0) if mode == 'escaped' else (1, 0, 0))
        assert counts == (case['session_close_lease_held64_entries'], case['session_close_no_lease_entries'], case['cache_destruction_entries'])
        assert sum(counts[:2]) == case['session_close_entries']
        assert case['expected_admission_state'] == ('no_lease' if mode == 'escaped' else 'lease_held64')
        assert case['lease_order_evidence'] is (mode != 'escaped')
        assert case['stderr_lock_confirmed'] is (mode in ('stderr-close', 'stderr-return'))
        assert stderr.count(b'teardown-fixture stderr-lock-held\n') == int(mode in ('stderr-close', 'stderr-return'))
        assert case['final_release_check_isolated'] is (mode == 'stderr-return')
        assert stderr.count(b'teardown-fixture final-check-isolated\n') == int(mode == 'stderr-return')
        for key, marker in [('operation_error', b'teardown-fixture operation-error:'),
                            ('cleanup_returned', b'teardown-fixture cleanup-returned'),
                            ('lease_release_reported', b'teardown-fixture lease-released\n')]:
            assert case[key] is False and marker not in markers
    assert probe['stderr_close_seconds'] == report['cases'][7]['elapsed_seconds']
    assert probe['stderr_final_release_seconds'] == report['cases'][8]['elapsed_seconds']


def main():
    assert __debug__
    parser = argparse.ArgumentParser()
    parser.add_argument('directory', type=Path)
    parser.add_argument('--self-test', action='store_true')
    args = parser.parse_args()
    root = args.directory.absolute()
    # Validate the exact helper bytes before execution, using one regular-file descriptor.
    helper_path = root / 'historical/verify.py'
    with os.fdopen(os.open(helper_path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK), 'rb') as stream:
        meta = os.fstat(stream.fileno())
        assert stat.S_ISREG(meta.st_mode) and meta.st_size == 11949
        helper = stream.read(11950)
    assert len(helper) == 11949 and hashlib.sha256(helper).hexdigest() == '9735f15a8f01844e85ff350c5ac340dfd8c02e6aa5f34f69e6c9f002152f2f72'
    old = {'__name__': 'pinned_historical_verifier', '__file__': str(helper_path)}
    exec(compile(helper, str(helper_path), 'exec'), old)
    raw = old['read'](root, 'ledger.json')
    ledger = old['load'](raw)
    actual = {str(path.relative_to(root)) for path in root.rglob('*') if not path.is_dir()}
    assert actual == set(ledger['files']) | {'ledger.json'}
    contents = {name: old['read'](root, name) for name in ledger['files']}
    assert all(old['digest'](value) == ledger['files'][name] for name, value in contents.items())
    assert len(raw) + sum(map(len, contents.values())) <= 8 * 1024**2
    validate(ledger, contents, old)
    rejections = []
    if args.self_test:
        def promote(value):
            value['final_focused'][1]['main_test_totals']['failed'] = 0
        def foreign_binary(value):
            value['teardown_process']['binary']['sha256'] = '0' * 64
        def outer_kill(value):
            value['teardown_process']['exit_codes'][0] = -9
        for name, mutation in [('failed_followup_promoted', promote), ('foreign_probe_binary', foreign_binary), ('outer_kill_as_watchdog', outer_kill)]:
            bad = copy.deepcopy(ledger)
            mutation(bad)
            try:
                validate(bad, contents, old)
            except (AssertionError, KeyError, ValueError):
                rejections.append(name)
            else:
                raise AssertionError('Accepted invalid evidence: ' + name)
    print(json.dumps(dict(scope='gliner25_qualification_lifecycle_evidence_verification/v2', qualification=False,
                          status='passed', ledger=old['digest'](raw), artifacts=len(contents),
                          bytes=len(raw) + sum(map(len, contents.values())), failed_build_checkpoints=5,
                          separate_model_free_teardown_probes=9, historical_203_test_passes=2,
                          adversarial_rejections=rejections), sort_keys=True))


if __name__ == '__main__':
    main()
