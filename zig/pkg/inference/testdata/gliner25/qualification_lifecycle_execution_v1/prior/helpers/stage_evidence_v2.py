#!/usr/bin/env python3
"""Add focused-build failures and model-free teardown probes to unchanged prior evidence."""
from pathlib import Path
import hashlib
import json
import os
import re
import stat
import tarfile

HIST = Path('/private/tmp/gliner25-qualification-lifecycle-evidence-v1')
OUT = Path('/private/tmp/gliner25-qualification-lifecycle-evidence-v2')
with os.fdopen(os.open(HIST / 'helpers/stage_evidence.py', os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK), 'rb') as helper_file:
    meta = os.fstat(helper_file.fileno())
    assert stat.S_ISREG(meta.st_mode) and meta.st_size == 21488
    helper_raw = helper_file.read(21489)
assert len(helper_raw) == 21488 and hashlib.sha256(helper_raw).hexdigest() == '31b212b660ecbf6d20ab7d0ad74e46202885b12134c24dc7000dffbd50c15b92'
HELPERS = {'__name__': 'pinned_evidence_assembly', '__file__': str(HIST / 'helpers/stage_evidence.py')}
exec(compile(helper_raw, HELPERS['__file__'], 'exec'), HELPERS)
read, digest, hash_file, json_bytes = (HELPERS[name] for name in ('read', 'digest', 'hash_file', 'json_bytes'))
SELECTED = HELPERS['SELECTED'] + ['zig/pkg/inference/src/' + name for name in (
    'extractors/gliner_boundary_long_tasks_test.zig',
    'extractors/gliner_boundary_long_executor.zig',
    'pipelines/gliner_boundary_long_document.zig',
    'inference.zig',
)]


def main():
    assert __debug__
    historical_raw = read(HIST / 'ledger.json')
    assert digest(historical_raw)['sha256'] == '9acc6ffcca8724acdd4247dd7a8e4c2b0295478f8cf9bbd2bb5c3f13b0aaafc3'
    historical = json.loads(historical_raw)
    OUT.mkdir(mode=0o700)
    files = {}
    reused_sources = {}

    def save(name, value):
        pin = digest(value)
        assert len(value) <= 1024**2
        if name in files:
            assert files[name] == pin and read(OUT / name) == value
        else:
            path = OUT / name
            path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            with path.open('xb') as f:
                f.write(value)
            files[name] = pin
        assert sum(pin['size_bytes'] for pin in files.values()) <= 8 * 1024**2
        return dict(path=name, **pin)

    for name, pin in historical['files'].items():
        value = read(HIST / name)
        assert digest(value) == pin
        ref = save('historical/' + name, value)
        if name.startswith('sources/'):
            reused_sources[(pin['sha256'], Path(name).name)] = ref
    historical_ref = save('historical/ledger.json', historical_raw)
    original_verification = read(Path('/private/tmp/gliner25-qualification-lifecycle-evidence-v1.verification.json'))
    assert json.loads(original_verification)['ledger'] == digest(historical_raw)
    save('historical_verification.json', original_verification)

    archive_root = Path('/private/tmp/antfly-gliner25-final-focused-metal-v2-archive')
    archive_raw = read(archive_root / 'archive.json')
    archive = json.loads(archive_raw)
    assert digest(archive_raw)['sha256'] == 'ec54337a2dd00fb8b6ba5efbf9803c21cbe0f1b5fb926f75d618161fdbbf834a'
    archive_ref = save('final_focused/v2/archive.json', archive_raw)
    binary = hash_file(archive_root / 'test', 128 * 1024**2)
    assert binary == dict(size_bytes=85775944, sha256='7e9519f774211ef0065137eafaa9ef9c7995f19d2f75817ed0fbe36ad01aee74')
    assert all(archive['frozen_executable'][key] == value for key, value in binary.items())
    tar_path = archive_root / 'recorded-source-selection.tar.gz'
    tar_pin = hash_file(tar_path, 64 * 1024**2)
    assert tar_pin == archive['source_archive'] == dict(size_bytes=21915127, sha256='0a378406c797fb4be7d14885be8057e615ab0041f63240ab7d692218c8180ec9')
    source_bytes = {}
    with tarfile.open(tar_path, 'r|gz') as tar:
        for member in tar:
            if member.name not in SELECTED:
                continue
            assert member.isreg() and member.name not in source_bytes and member.size <= 1024**2
            stream = tar.extractfile(member)
            assert stream is not None
            value = stream.read(1024**2 + 1)
            assert len(value) == member.size
            source_bytes[member.name] = value
    assert set(source_bytes) == set(SELECTED)
    assert hash_file(tar_path, 64 * 1024**2) == tar_pin
    runs = []
    for version in (1, 2):
        root = Path(f'/private/tmp/antfly-gliner25-final-focused-metal-v{version}')
        originals = {path.name: read(path) for path in sorted(root.iterdir())}
        refs = {name: save(f'final_focused/v{version}/' + name, raw) for name, raw in originals.items()}
        receipt = json.loads(originals['process.json'])
        start = json.loads(originals['start.json'])
        inventory = json.loads(originals['source_inventory.json'])
        process = receipt['process']
        assert all(receipt[key] == value for key, value in start.items())
        assert digest(originals['source_inventory.json']) == receipt['source_inventory']
        assert len(inventory) == receipt['source_count'] == 2588
        assert process['returncode'] == 1 and process['cleanup']['complete'] is True
        assert process['cleanup']['survivors'] == process['cleanup']['errors'] == process['cleanup']['inspection_errors'] == []
        assert process['source_inventory_unchanged'] is True and process['changed_sources'] == []
        for channel in ('stdout', 'stderr'):
            assert digest(originals[channel + '.log']) == process[channel]
        for index, observed in enumerate(process['observed_test_executables'], 1):
            assert json.loads(originals[f'observed-test-{index}.json']) == observed
        text = originals['stderr.log'].decode()
        totals = None
        main = None
        if version == 1:
            assert digest(originals['process.json'])['sha256'] == '068ecbc4fec1069174331a42d237bcd8fe6c1a399a99cd51d8a19fe30eca8353'
            assert 'expected optional type' in text and 'gliner_boundary_long_tasks_test.zig:291:9' in text
            assert not re.search(r'^\d+/\d+ ', text, re.M)
            failure = dict(code='test_watchdog_optional_type', detail='The learned-window test inferred a required watchdog pointer in the device branch, then used optional capture. No main inference tests ran.',
                           repo_path='zig/pkg/inference/src/extractors/gliner_boundary_long_tasks_test.zig', line=291)
        else:
            assert digest(originals['process.json']) == archive['process_receipt']
            assert receipt['source_inventory'] == archive['source_inventory']
            assert archive['live_observation'] == process['observed_test_executables'][1]
            for name in ('start.json', 'process.json', 'source_inventory.json', 'stdout.log', 'stderr.log'):
                assert read(archive_root / name) == originals[name]
            totals = dict(selected=67, passed=65, skipped=1, failed=1, leaked=0)
            assert totals == archive['tests'] and '67 selected; 65 passed; 1 skipped; 1 failed; 0 leaked.' in text
            assert 'gliner_boundary_cache_lifecycle_test.zig:373:44' in text and 'expected 0, found 1' in text
            main = dict(**binary, original_path=str(archive_root / 'test'), rehashed=True, bytes_included=False)
            failure = dict(code='metrics_observation_refreshes_idle_timestamp',
                           detail='After reload and metrics observation, final eviction retained one owner. Parent identified metrics snapshot release refreshing the idle-use timestamp; a production fix is pending at this evidence checkpoint. The earlier raw archive interpretation remains verbatim and is not an applied stale-test-timestamp fix.',
                           diagnosis_source='parent static review after the failed run; no changed model or test rerun in this archive',
                           repo_path='zig/pkg/inference/src/server/gliner_boundary_cache_lifecycle_test.zig', line=373)
        sources = []
        for name in SELECTED:
            if version == 1 and name.endswith('/gliner_boundary_long_tasks_test.zig'):
                origin = Path('/private/tmp/gliner25-learned-multiwindow-v1/gliner_boundary_long_tasks_test.zig')
                value = read(origin)
                recovery = dict(method='exact pre-existing private draft, matched checkpoint inventory', original_path=str(origin))
            else:
                value = source_bytes[name]
                recovery = dict(method='selected regular final-focused v2 tar member, matched this checkpoint inventory', member=name)
            pin = digest(value)
            assert pin == inventory[name], (version, name)
            ref = reused_sources.get((pin['sha256'], Path(name).name))
            if ref is None:
                ref = save('sources/' + pin['sha256'] + '/' + Path(name).name, value)
                reused_sources[(pin['sha256'], Path(name).name)] = ref
            sources.append(dict(repo_path=name, artifact=ref, recovery=recovery))
        failing_source = next(item for item in sources if item['repo_path'] == failure['repo_path'])
        failure['source_line'] = read(OUT / failing_source['artifact']['path']).decode().splitlines()[failure['line'] - 1]
        run = dict(id=f'final_focused_v{version}', raw=refs, main_test_totals=totals, main_executable=main,
                   outcome='compile_failure' if version == 1 else 'test_failure', failure_interpretation=failure,
                   source_count=len(inventory), selected_sources=sources,
                   ancillary_build_tests=dict(passed=23, total=23, is_main_selected_suite=False),
                   command=start['command'], environment=start['environment'], cwd=start['cwd'],
                   source_selection=start['source_selection'], source_inventory=start['source_inventory'],
                   helper_pins=dict(wrapper=start['wrapper'], supervisor=start['supervisor']),
                   returncode=process['returncode'], elapsed_seconds=process['elapsed_seconds'],
                   peak_child_tree_rss_bytes=process['peak_child_tree_rss_bytes'], cleanup=process['cleanup'],
                   observed_test_executables=process['observed_test_executables'])
        if version == 2:
            run['archive_receipt'] = archive_ref
            run['original_source_archive'] = dict(**tar_pin, rehashed=True, bytes_included=False, original_path=str(tar_path))
            run['archivist_failure_interpretation'] = archive['failure']
            passed_tests = re.findall(r'^\d+/\d+ (.+?)\.\.\.OK$', text, re.M)
            run['scoped_passes'] = [name for name in passed_tests if any(key in name for key in ('learned multi window', 'terminal allocation', 'model manager teardown'))]
            assert len([name for name in run['scoped_passes'] if 'learned multi window' in name]) == 4
        runs.append(run)

    probe_root = Path('/private/tmp/gliner25-manager-teardown-process-v2')
    raw_probes = {path.name: read(path) for path in sorted(probe_root.iterdir())}
    probe_refs = {name: save('teardown_process/' + name, raw) for name, raw in raw_probes.items()}
    report = json.loads(raw_probes['report.json'])
    assert digest(raw_probes['report.json'])['sha256'] == '4fa0732a535c7777c9a93aafc5c272de11b4c5938835d93831f755e9f379c41c'
    assert report['binary'] == binary and report['pass'] is True and report['qualification'] is False and report['binary_unchanged'] is True
    modes = ['cache', 'ttl', 'admission', 'retired', 'shutdown', 'rollback', 'escaped', 'stderr-close', 'stderr-return']
    assert [case['mode'] for case in report['cases']] == modes
    for case in report['cases']:
        assert case['exit_code'] == 86 and case['pass'] is True and case['reaped'] is True and case['outer_failure'] is None
        assert case['cleanup_returned'] is False and case['lease_release_reported'] is False
        for channel in ('stdout', 'stderr'):
            assert digest(raw_probes[case['mode'] + '.' + channel + '.log']) == case[channel]
    driver = read(Path('/private/tmp/gliner25-fatal-watchdog-v1/check_model_manager_teardown.py'))
    assert digest(driver) == report['driver']
    driver_ref = save('helpers/check_model_manager_teardown.py', driver)
    save('helpers/stage_evidence_v2.py', read(Path(__file__)))
    save('verify.py', read(Path('/private/tmp/verify_gliner25_qualification_lifecycle_evidence_v2.py')))
    save('README.md', read(Path('/private/tmp/gliner25_qualification_lifecycle_evidence_README_v2.md')))
    ledger = dict(
        scope='gliner25_qualification_lifecycle_evidence/v2', version=2, qualification=False, local_only=True,
        status='five_failed_build_checkpoints_preserved_metrics_lifecycle_fix_pending',
        historical_ledger=historical_ref, files=files, final_focused=runs,
        teardown_process=dict(raw=probe_refs, report=probe_refs['report.json'], driver=driver_ref,
                              cases=9, passed=9, binary=binary, no_model=True, no_outer_kills=True,
                              exit_codes=[case['exit_code'] for case in report['cases']],
                              scope='Model-free fake close/cache destructors in fresh child processes; separate from actual-model TTL correctness.',
                              escaped_session_has_no_admission_lease=True,
                              stderr_close_seconds=report['cases'][7]['elapsed_seconds'],
                              stderr_final_release_seconds=report['cases'][8]['elapsed_seconds']),
        storage=dict(maximum_bytes=8 * 1024**2, payload_bytes_excluding_ledger=sum(pin['size_bytes'] for pin in files.values()),
                     binaries_included=False, full_source_archives_included=False, selected_sources_per_new_checkpoint=len(SELECTED)),
        limits=[
            'Historical v1 directory is copied byte-for-byte under historical; its ledger hash and raw failed evidence remain unchanged.',
            'All five build commands failed. The four learned-window tests and other individual passes do not promote the final-focused v2 aggregate.',
            'No stale-timestamp test adaptation is recorded as applied. The metrics observation touching model idle time is a pending production defect.',
            'Nine process probes demonstrate bounded fatal close in test-only fake destructors, including blocked stderr polling/final-release paths; they do not prove the actual-model TTL timer is correct.',
            'The escaped raw session intentionally has no admission lease. Its probe proves ticket/owner lifetime, not lease-order retention.',
            'Original full binaries and source archives were rehashed but not copied. Source selections are not dependency closures.',
            'No new build, model, GPU, Torch, process-lifecycle execution, commit, push, remote CI, quality or performance qualification occurred while assembling this evidence.',
        ],
    )
    value = json_bytes(ledger)
    assert ledger['storage']['payload_bytes_excluding_ledger'] + len(value) <= 8 * 1024**2
    with (OUT / 'ledger.json').open('xb') as f:
        f.write(value)
    print(json.dumps(dict(output=str(OUT), ledger=digest(value), artifacts=len(files), bytes=ledger['storage']['payload_bytes_excluding_ledger'] + len(value)), sort_keys=True))


if __name__ == '__main__':
    main()
