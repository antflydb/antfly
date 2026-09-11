#!/usr/bin/env python3
"""Offline transitive integrity validation for the final observation/TTL checkpoint."""
import argparse
import copy
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import zlib


def read(root, name):
    path = Path(name)
    assert not path.is_absolute() and '..' not in path.parts
    for parent in path.parents:
        assert not (root / parent).is_symlink()
    with os.fdopen(os.open(root / path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK), 'rb') as stream:
        before = os.fstat(stream.fileno())
        assert stat.S_ISREG(before.st_mode) and before.st_size <= 1024**2
        value = stream.read(1024**2 + 1)
        after = os.fstat(stream.fileno())
        assert len(value) == before.st_size
        assert (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) == (
            after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns)
        return value


def digest(value):
    return dict(size_bytes=len(value), sha256=hashlib.sha256(value).hexdigest())


def pinned_helper(root, path, size, sha):
    value = read(root, path)
    assert digest(value) == dict(size_bytes=size, sha256=sha)
    result = {'__name__': 'pinned_evidence_verifier', '__file__': str(root / path)}
    exec(compile(value, str(root / path), 'exec'), result)
    return result


def decoded_source(source, value):
    if source['encoding'] == 'identity':
        result = value
    else:
        assert source['encoding'] == 'gzip'
        assert value[:3] == b'\x1f\x8b\x08' and value[4:8] == bytes(4) and value[3] & 8 == 0
        inflater = zlib.decompressobj(16 + zlib.MAX_WBITS)
        result = inflater.decompress(value, 2 * 1024**2 + 1)
        assert len(result) <= 2 * 1024**2 and not inflater.unconsumed_tail
        result += inflater.flush(2 * 1024**2 + 1 - len(result))
        assert len(result) <= 2 * 1024**2 and inflater.eof and not inflater.unused_data
    assert digest(result) == source['original']
    return result


def validate(ledger, contents, old1, old2):
    def raw(ref):
        value = contents[ref['path']]
        assert digest(value) == old1['bare'](ref) == ledger['files'][ref['path']]
        return value

    assert ledger['scope'] == 'gliner25_qualification_lifecycle_evidence/v3'
    assert ledger['version'] == 3 and ledger['qualification'] is False and ledger['local_only'] is True
    assert ledger['storage']['maximum_bytes'] == 8 * 1024**2 and ledger['storage']['max_source_inflated_bytes'] == 2 * 1024**2
    assert ledger['storage']['binaries_included'] is ledger['storage']['full_source_archives_included'] is False
    assert ledger['storage']['payload_bytes_excluding_ledger'] == sum(map(len, contents.values()))
    previous_raw = raw(ledger['previous_ledger'])
    assert digest(previous_raw)['sha256'] == '7718fab9bfa79e8cb650c1eacaca5bd9a075f7856346a63d6e6342e7e7feb95a'
    previous = old1['load'](previous_raw)
    old_contents = {name: contents['prior/' + name] for name in previous['files']}
    assert all(digest(value) == previous['files'][name] for name, value in old_contents.items())
    old2['validate'](previous, old_contents, old1)
    assert [run['id'] for run in ledger['observation_runs']] == ['observation_v1', 'observation_v2']
    assert ledger['status'] == 'corrected_snapshot_and_actual_ttl_passed_prior_failures_retained'
    archive = old1['load'](raw(ledger['observation_archive']['receipt']))
    expected_binary = dict(size_bytes=82720136, sha256='4a7af5cb45c5202b51ddfd57065d866a67c521be00bba85667f71b02d749fc64')
    assert old1['bare'](archive['frozen_executable']) == ledger['observation_archive']['executable'] == expected_binary
    assert archive['source_archive'] == ledger['observation_archive']['source_archive']
    assert ledger['observation_archive']['executable_rehashed'] is ledger['observation_archive']['source_archive_rehashed'] is True
    assert ledger['observation_archive']['executable_bytes_included'] is ledger['observation_archive']['full_archive_bytes_included'] is False
    inv = None
    for index, run in enumerate(ledger['observation_runs'], 1):
        receipt = old1['load'](raw(run['raw']['process.json']))
        start = old1['load'](raw(run['raw']['start.json']))
        inventory_raw = raw(run['raw']['source_inventory.json'])
        inventory = old1['load'](inventory_raw)
        process = receipt['process']
        assert all(receipt[key] == value for key, value in start.items())
        if inv is None:
            inv = inventory
        else:
            assert inv == inventory
        assert start['source_inventory'] == digest(inventory_raw) == run['source_inventory'] == archive['source_inventory']
        assert len(inventory) == start['source_count'] == run['source_count'] == 2588
        for key in ('command', 'cwd', 'environment', 'source_selection'):
            assert start[key] == run[key]
        for key in ('returncode', 'elapsed_seconds', 'peak_child_tree_rss_bytes', 'cleanup'):
            assert process[key] == run[key]
        assert all(process[key] == value for key, value in run['guards'].items())
        assert run['guards']['max_child_tree_rss_bytes'] == 6 * 1024**3 and run['guards']['timeout_seconds'] == 1800
        assert process['peak_child_tree_rss_bytes'] <= run['guards']['max_child_tree_rss_bytes']
        assert process['elapsed_seconds'] <= run['guards']['timeout_seconds']
        assert process['source_inventory_unchanged'] is True and process['changed_sources'] == []
        assert all(process['cleanup'][key] is True for key in ('complete', 'direct_child_reaped', 'known_children_gone'))
        assert all(process['cleanup'][key] == [] for key in ('survivors', 'errors', 'inspection_errors'))
        for key in ('supervisor', 'wrapper'):
            assert start[key] == run['helper_pins'][key] == previous['final_focused'][0]['helper_pins'][key]
        for channel in ('stdout', 'stderr'):
            assert digest(raw(run['raw'][channel + '.log'])) == process[channel]
        observations = process['observed_test_executables']
        for count, value in enumerate(observations, 1):
            assert old1['load'](raw(run['raw'][f'observed-test-{count}.json'])) == value
        assert run['observed_main'] in observations and old1['bare'](run['observed_main']['executable']) == expected_binary
        text = raw(run['raw']['stderr.log']).decode()
        counts = re.findall(r'(\d+) selected; (\d+) passed; (\d+) skipped(?:; (\d+) failed; (\d+) leaked)?\.', text)
        assert len(counts) == 1
        totals = dict(zip(('selected', 'passed', 'skipped', 'failed', 'leaked'), (int(value or '0') for value in counts[0])))
        assert totals == run['main_test_totals']
        if index == 1:
            assert run['outcome'] == 'pre_inference_live_memory_admission_failure' and run['actual_model_ttl_exercised'] is False
            assert run['returncode'] == 1 and totals == dict(selected=11, passed=10, skipped=0, failed=1, leaked=0)
            assert digest(raw(run['raw']['process.json'])) == archive['process_receipt']
            assert archive['live_observation'] == run['observed_main']
            denial = run['admission_denial']
            assert re.findall(r'live-memory admission denied requested_bytes=(\d+) pending_bytes=(\d+) capacity_bytes=(\d+) basis=(\w+)', text) == [
                ('2415919104', '536870912', '2807284480', 'shared_epoch')]
            assert denial == dict(requested_bytes=2415919104, pending_bytes=536870912, capacity_bytes=2807284480,
                                  basis='shared_epoch', sum_requested_pending_bytes=2952790016, excess_bytes=145505536,
                                  http_status=503, error='MODEL_RESOURCE_BUSY')
            assert 'status=503' in text and 'MODEL_RESOURCE_BUSY' in text
            assert denial['requested_bytes'] + denial['pending_bytes'] == denial['sum_requested_pending_bytes']
            assert denial['sum_requested_pending_bytes'] - denial['capacity_bytes'] == denial['excess_bytes']
        else:
            assert run['outcome'] == 'passed' and run['actual_model_ttl_exercised'] is True
            assert run['returncode'] == 0 and process['failure'] is None
            assert totals == dict(selected=11, passed=11, skipped=0, failed=0, leaked=0)
            assert run['admission_denial'] is None and 'MODEL_RESOURCE_BUSY' not in text
            first = ledger['observation_runs'][0]
            def filters(command):
                return [command[i + 1] for i, value in enumerate(command[:-1]) if value == '--test-filter']
            assert filters(run['command']) == filters(first['command'])
            assert run['guards'] == first['guards'] and run['environment'] == first['environment']
            binding = old1['load'](raw(run['archive_binding']))
            assert binding['live_observation'] == run['observed_main'] and binding['tests'] == totals
            assert binding['process_receipt'] == digest(raw(run['raw']['process.json']))
            assert binding['source_inventory'] == run['source_inventory']
            assert old1['bare'](binding['frozen_executable']) == expected_binary
            assert old1['bare'](binding['original_build_archive']) == digest(raw(ledger['observation_archive']['receipt']))
            assert old1['bare'](binding['source_archive']) == archive['source_archive']
            assert binding['same_source_selection_and_executable_as_failed_build_run'] is True
            assert binding['source_and_executable_rehashed'] is True
            phases = {phase:dict(elapsed_ns=int(elapsed), budget_ns=int(budget)) for phase, elapsed, budget in re.findall(
                r'gliner-boundary-cache-maintenance returned phase=(\w+) elapsed_ns=(\d+) budget_ns=(\d+)', text)}
            assert phases == binding['maintenance_phases'] == run['maintenance_phases']
            assert set(phases) == {'held_handle', 'before_expiry', 'eligible_eviction', 'empty_cache', 'reload_eviction'}
            assert all(phase['elapsed_ns'] < phase['budget_ns'] for phase in phases.values())
            assert phases['eligible_eviction'] == dict(elapsed_ns=15871518000, budget_ns=30000000000)
            assert phases['reload_eviction'] == dict(elapsed_ns=20680144000, budget_ns=30000000000)
    assert len(ledger['selected_observation_sources']) == 6
    sources = {}
    for source in ledger['selected_observation_sources']:
        assert source['repo_path'] not in sources
        value = decoded_source(source, raw(source['artifact']))
        assert digest(value) == inv[source['repo_path']]
        sources[source['repo_path']] = value
    change = ledger['applied_observation_change']
    applied = old1['load'](raw(change['applied']))
    proposed = old1['load'](raw(change['proposal']))
    assert applied['applied'] is True and applied['files'] == proposed['files']
    assert digest(raw(change['patch'])) == old1['bare'](proposed['patch'])
    assert len(applied['files']) == 3
    for item in applied['files']:
        assert item['proposed'] == digest(sources[item['path']]) == inv[item['path']]


def main():
    assert __debug__
    parser = argparse.ArgumentParser()
    parser.add_argument('directory', type=Path)
    parser.add_argument('--self-test', action='store_true')
    args = parser.parse_args()
    root = args.directory.absolute()
    old1 = pinned_helper(root, 'prior/historical/verify.py', 11949, '9735f15a8f01844e85ff350c5ac340dfd8c02e6aa5f34f69e6c9f002152f2f72')
    old2 = pinned_helper(root, 'prior/verify.py', 12298, '1c2279bcc3151eeeee56029e768cb706ec79f04b4f30be6dbfb8a498c64ce034')
    raw = read(root, 'ledger.json')
    ledger = old1['load'](raw)
    actual = {str(path.relative_to(root)) for path in root.rglob('*') if not path.is_dir()}
    assert actual == set(ledger['files']) | {'ledger.json'}
    contents = {name:read(root, name) for name in ledger['files']}
    assert all(digest(value) == ledger['files'][name] for name,value in contents.items())
    assert len(raw) + sum(map(len, contents.values())) <= 8 * 1024**2
    validate(ledger, contents, old1, old2)
    rejected = []
    if args.self_test:
        def promote_admission(value):
            value['observation_runs'][0]['actual_model_ttl_exercised'] = True
        def relaxed_guard(value):
            value['observation_runs'][1]['guards']['max_child_tree_rss_bytes'] *= 2
        def foreign_binary(value):
            value['observation_runs'][1]['observed_main']['executable']['sha256'] = '0' * 64
        for name, mutate in [('admission_as_ttl_result', promote_admission), ('retry_guard_relaxed', relaxed_guard), ('retry_binary_mismatch', foreign_binary)]:
            bad = copy.deepcopy(ledger)
            mutate(bad)
            try:
                validate(bad, contents, old1, old2)
            except (AssertionError, KeyError, ValueError):
                rejected.append(name)
            else:
                raise AssertionError('Accepted invalid evidence: ' + name)
    print(json.dumps(dict(scope='gliner25_qualification_lifecycle_evidence_verification/v3', qualification=False,
                          status='passed', ledger=digest(raw), artifacts=len(contents), bytes=len(raw) + sum(map(len, contents.values())),
                          retained_failed_commands=6, corrected_same_executable_ttl_tests=11,
                          separate_prior_teardown_probes=9, adversarial_rejections=rejected), sort_keys=True))


if __name__ == '__main__':
    main()
