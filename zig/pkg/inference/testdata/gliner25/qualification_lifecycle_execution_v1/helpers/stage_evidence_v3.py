#!/usr/bin/env python3
"""Archive the applied observation fix, retained admission failure, and optional frozen retry."""
import argparse
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import re
import stat
import tarfile

PRIOR = Path('/private/tmp/gliner25-qualification-lifecycle-evidence-v2')
OUT = Path('/private/tmp/gliner25-qualification-lifecycle-evidence-v3')
ARCHIVE = Path('/private/tmp/antfly-gliner25-observation-ttl-metal-v1-archive')
RUN1 = Path('/private/tmp/antfly-gliner25-observation-ttl-metal-v1')
PATCH = Path('/private/tmp/gliner25-observation-ttl-v1')
EXPECTED_BINARY = dict(size_bytes=82720136, sha256='4a7af5cb45c5202b51ddfd57065d866a67c521be00bba85667f71b02d749fc64')


def digest(value):
    return dict(size_bytes=len(value), sha256=hashlib.sha256(value).hexdigest())


def signature(meta):
    return (meta.st_dev, meta.st_ino, meta.st_size, meta.st_mtime_ns, meta.st_ctime_ns)


def read(path, cap=2 * 1024**2):
    with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK), 'rb') as stream:
        before = os.fstat(stream.fileno())
        assert stat.S_ISREG(before.st_mode) and before.st_size <= cap
        value = stream.read(cap + 1)
        assert len(value) == before.st_size and signature(before) == signature(os.fstat(stream.fileno()))
        return value


def hash_file(path, cap):
    with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK), 'rb') as stream:
        before = os.fstat(stream.fileno())
        assert stat.S_ISREG(before.st_mode) and before.st_size <= cap
        sha = hashlib.sha256()
        size = 0
        for block in iter(lambda: stream.read(1024**2), b''):
            size += len(block)
            assert size <= cap
            sha.update(block)
        assert size == before.st_size and signature(before) == signature(os.fstat(stream.fileno()))
        return dict(size_bytes=size, sha256=sha.hexdigest())


def pack(value):
    return (json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + '\n').encode()


def filters(command):
    return [command[i + 1] for i, value in enumerate(command[:-1]) if value == '--test-filter']


def main():
    assert __debug__
    parser = argparse.ArgumentParser()
    parser.add_argument('--retry-root', type=Path)
    args = parser.parse_args()
    prior_raw = read(PRIOR / 'ledger.json')
    assert digest(prior_raw)['sha256'] == '7718fab9bfa79e8cb650c1eacaca5bd9a075f7856346a63d6e6342e7e7feb95a'
    prior = json.loads(prior_raw)
    OUT.mkdir(mode=0o700)
    files = {}
    source_refs = {}

    def save(name, value):
        pin = digest(value)
        assert len(value) <= 1024**2
        if name not in files:
            path = OUT / name
            path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            with path.open('xb') as stream:
                stream.write(value)
            files[name] = pin
        else:
            assert files[name] == pin and read(OUT / name) == value
        assert sum(item['size_bytes'] for item in files.values()) <= 8 * 1024**2
        return dict(path=name, **pin)

    for name, pin in prior['files'].items():
        value = read(PRIOR / name)
        assert digest(value) == pin
        ref = save('prior/' + name, value)
        if name.startswith('sources/') or '/sources/' in name:
            source_refs[(pin['sha256'], Path(name).name)] = ref
    prior_ref = save('prior/ledger.json', prior_raw)
    previous_verification = read(Path('/private/tmp/gliner25-qualification-lifecycle-evidence-v2.verification.json'))
    assert json.loads(previous_verification)['ledger'] == digest(prior_raw)
    save('prior_verification.json', previous_verification)
    archive_raw = read(ARCHIVE / 'archive.json')
    assert digest(archive_raw)['sha256'] == '06aa79b1a59ad188dd9b22f56296c7c4fa8797fb309e6f356a19f7d194fd5e61'
    archive = json.loads(archive_raw)
    archive_ref = save('observation/v1/archive.json', archive_raw)
    assert hash_file(ARCHIVE / 'test', 128 * 1024**2) == EXPECTED_BINARY
    assert all(archive['frozen_executable'][key] == value for key, value in EXPECTED_BINARY.items())
    tar_pin = hash_file(ARCHIVE / 'recorded-source-selection.tar.gz', 64 * 1024**2)
    assert tar_pin == archive['source_archive'] == dict(size_bytes=21602822, sha256='671fca4664ced1a8343bba7604f90b3debb512257882d737dad38ea3dec1229d')
    applied_raw = read(PATCH / 'applied.json')
    applied = json.loads(applied_raw)
    proposal_raw = read(PATCH / 'apply-pins.json')
    proposal = json.loads(proposal_raw)
    assert applied['applied'] is True and applied['files'] == proposal['files']
    patch_raw = read(PATCH / 'observation-ttl.patch')
    assert digest(patch_raw) == {key: proposal['patch'][key] for key in ('size_bytes', 'sha256')}
    application = dict(
        applied=save('observation/patch/applied.json', applied_raw),
        proposal=save('observation/patch/apply-pins.json', proposal_raw),
        patch=save('observation/patch/observation-ttl.patch', patch_raw),
        review=save('observation/patch/REVIEW.md', read(PATCH / 'REVIEW.md')),
        note='Original pre-application flags and applied receipt are both preserved; execution evidence is recorded separately.',
    )
    inventory1 = json.loads(read(RUN1 / 'source_inventory.json'))
    wanted = [item['path'] for item in applied['files']]
    wanted += ['zig/pkg/inference/src/' + name for name in ('backends/session.zig', 'hard_cancellation_watchdog.zig', 'execution_control.zig')]
    recovered = {}
    with tarfile.open(ARCHIVE / 'recorded-source-selection.tar.gz', 'r|gz') as tar:
        for member in tar:
            if member.name not in wanted:
                continue
            assert member.isreg() and member.name not in recovered and member.size <= 2 * 1024**2
            stream = tar.extractfile(member)
            assert stream is not None
            value = stream.read(2 * 1024**2 + 1)
            assert len(value) == member.size and digest(value) == inventory1[member.name]
            recovered[member.name] = value
    assert set(recovered) == set(wanted)
    assert hash_file(ARCHIVE / 'recorded-source-selection.tar.gz', 64 * 1024**2) == tar_pin
    selected_sources = []
    for name in wanted:
        value = recovered[name]
        pin = digest(value)
        ref = source_refs.get((pin['sha256'], Path(name).name))
        encoding = 'identity'
        if ref is None and len(value) > 1024**2:
            target = io.BytesIO()
            with gzip.GzipFile(filename='', fileobj=target, mode='wb', compresslevel=9, mtime=0) as zipped:
                zipped.write(value)
            ref = save('sources/' + pin['sha256'] + '/' + Path(name).name + '.gz', target.getvalue())
            encoding = 'gzip'
        elif ref is None:
            ref = save('sources/' + pin['sha256'] + '/' + Path(name).name, value)
        selected_sources.append(dict(repo_path=name, artifact=ref, encoding=encoding, original=pin,
                                     recovery='selected regular archive member; exact original content hash matches source inventory'))
    for changed in applied['files']:
        assert inventory1[changed['path']] == changed['proposed']

    runs = []
    for version, root in [(1, RUN1)] + ([(2, args.retry_root)] if args.retry_root else []):
        originals = {path.name: read(path) for path in sorted(root.iterdir()) if path.is_file()}
        assert 'process.json' in originals, 'Complete parent receipt required before archiving.'
        refs = {name: save(f'observation/v{version}/' + name, value) for name, value in originals.items()}
        receipt, start, inventory = (json.loads(originals[name]) for name in ('process.json', 'start.json', 'source_inventory.json'))
        process = receipt['process']
        assert all(receipt[key] == value for key, value in start.items())
        assert receipt['source_count'] == len(inventory) == 2588
        assert inventory == inventory1 and digest(originals['source_inventory.json']) == receipt['source_inventory'] == archive['source_inventory']
        assert process['source_inventory_unchanged'] is True and process['changed_sources'] == []
        assert all(process['cleanup'][key] is True for key in ('complete', 'direct_child_reaped', 'known_children_gone'))
        assert all(process['cleanup'][key] == [] for key in ('survivors', 'errors', 'inspection_errors'))
        for channel in ('stdout', 'stderr'):
            assert digest(originals[channel + '.log']) == process[channel]
        for index, observation in enumerate(process['observed_test_executables'], 1):
            assert json.loads(originals[f'observed-test-{index}.json']) == observation
        main = [value for value in process['observed_test_executables'] if value['executable']['sha256'] == EXPECTED_BINARY['sha256']]
        assert len(main) == 1 and all(main[0]['executable'][key] == value for key, value in EXPECTED_BINARY.items())
        text = originals['stderr.log'].decode()
        counts = re.findall(r'(\d+) selected; (\d+) passed; (\d+) skipped(?:; (\d+) failed; (\d+) leaked)?\.', text)
        assert len(counts) == 1
        totals = dict(zip(('selected', 'passed', 'skipped', 'failed', 'leaked'), (int(value or '0') for value in counts[0])))
        if version == 1:
            assert digest(originals['process.json']) == archive['process_receipt']
            assert totals == archive['tests'] == dict(selected=11, passed=10, skipped=0, failed=1, leaked=0)
            assert archive['live_observation'] == main[0]
            assert process['returncode'] == 1 and 'MODEL_RESOURCE_BUSY' in text
            denial = re.findall(r'live-memory admission denied requested_bytes=(\d+) pending_bytes=(\d+) capacity_bytes=(\d+) basis=(\w+)', text)
            assert denial == [('2415919104', '536870912', '2807284480', 'shared_epoch')]
            admitted = dict(requested_bytes=2415919104, pending_bytes=536870912, capacity_bytes=2807284480,
                            basis='shared_epoch', sum_requested_pending_bytes=2952790016,
                            excess_bytes=145505536, http_status=503, error='MODEL_RESOURCE_BUSY')
            outcome = 'pre_inference_live_memory_admission_failure'
            ttl_exercised = False
            for name in ('start.json', 'process.json', 'source_inventory.json', 'stdout.log', 'stderr.log'):
                assert read(ARCHIVE / name) == originals[name]
        else:
            assert filters(start['command']) == filters(runs[0]['command'])
            assert start['environment'] == runs[0]['environment']
            assert start['command'][0] == main[0]['executable']['path']
            assert hash_file(Path(start['command'][0]), 128 * 1024**2) == EXPECTED_BINARY
            admitted = None
            outcome = 'passed' if process['returncode'] == 0 else 'pre_inference_live_memory_admission_failure' if 'MODEL_RESOURCE_BUSY' in text else 'test_failure'
            ttl_exercised = outcome == 'passed'
            if outcome == 'passed':
                assert totals == dict(selected=11, passed=11, skipped=0, failed=0, leaked=0)
                assert 'gliner boundary cache pinned small Metal handle retention eviction and reload' in text
        run = dict(id=f'observation_v{version}', raw=refs, outcome=outcome, main_test_totals=totals,
                         actual_model_ttl_exercised=ttl_exercised, admission_denial=admitted,
                         command=start['command'], cwd=start['cwd'], environment=start['environment'],
                         source_inventory=start['source_inventory'], source_count=start['source_count'],
                         source_selection=start['source_selection'], observed_main=main[0],
                         returncode=process['returncode'], elapsed_seconds=process['elapsed_seconds'],
                         peak_child_tree_rss_bytes=process['peak_child_tree_rss_bytes'], cleanup=process['cleanup'],
                         helper_pins=dict(supervisor=start['supervisor'], wrapper=start['wrapper']),
                         guards={key:process[key] for key in ('timeout_seconds', 'max_child_tree_rss_bytes', 'max_stdout_bytes',
                                  'max_stderr_bytes', 'max_tracked_processes', 'parent_grace_seconds', 'kill_wait_seconds')})
        if version == 2:
            binding_root = Path(str(root) + '-archive')
            binding_raw = read(binding_root / 'archive.json')
            assert digest(binding_raw)['sha256'] == 'ea66ccede7190051bca390cffa4181c00edf3522a521029b0dff3b0bed6f411a'
            binding = json.loads(binding_raw)
            assert binding['process_receipt'] == digest(originals['process.json'])
            assert binding['source_inventory'] == receipt['source_inventory']
            assert binding['live_observation'] == main[0] and binding['tests'] == totals
            assert binding['same_source_selection_and_executable_as_failed_build_run'] is True
            assert binding['source_and_executable_rehashed'] is True
            for key, value in EXPECTED_BINARY.items():
                assert binding['frozen_executable'][key] == value
            assert {key:binding['original_build_archive'][key] for key in ('size_bytes','sha256')} == digest(archive_raw)
            for name in ('start.json', 'process.json', 'source_inventory.json', 'stdout.log', 'stderr.log'):
                assert read(binding_root / name) == originals[name]
            phases = {phase: dict(elapsed_ns=int(elapsed), budget_ns=int(budget)) for phase, elapsed, budget in re.findall(
                r'gliner-boundary-cache-maintenance returned phase=(\w+) elapsed_ns=(\d+) budget_ns=(\d+)', text)}
            assert phases == binding['maintenance_phases'] and len(phases) == 5
            assert all(value['elapsed_ns'] < value['budget_ns'] for value in phases.values())
            run['maintenance_phases'] = phases
            run['archive_binding'] = save('observation/v2/archive.json', binding_raw)
            run['runtime_cwd_note'] = 'Observed runtime cwd is zig; the build-target runtime cwd was zig/pkg/inference. The fixture resolver explicitly supports both.'
        runs.append(run)
    if len(runs) == 2:
        assert runs[0]['guards'] == runs[1]['guards']
    save('helpers/stage_evidence_v3.py', read(Path(__file__)))
    save('verify.py', read(Path('/private/tmp/verify_gliner25_qualification_lifecycle_evidence_v3.py')))
    save('README.md', read(Path('/private/tmp/gliner25_qualification_lifecycle_evidence_README_v3.md')))
    ledger = dict(scope='gliner25_qualification_lifecycle_evidence/v3', version=3, qualification=False,
                  local_only=True, previous_ledger=prior_ref, files=files, observation_runs=runs,
                  status='corrected_snapshot_and_actual_ttl_passed_prior_failures_retained' if len(runs) == 2 and runs[1]['outcome'] == 'passed' else 'admission_failure_preserved_actual_ttl_retry_pending_or_failed',
                  applied_observation_change=application, selected_observation_sources=selected_sources,
                  observation_archive=dict(receipt=archive_ref, source_archive=tar_pin, executable=EXPECTED_BINARY,
                                           original_directory=str(ARCHIVE), executable_rehashed=True, source_archive_rehashed=True,
                                           executable_bytes_included=False, full_archive_bytes_included=False),
                  storage=dict(maximum_bytes=8 * 1024**2, payload_bytes_excluding_ledger=sum(item['size_bytes'] for item in files.values()),
                               large_source_encoding='gzip level9 mtime0 empty filename; exact original digest retained; bounded2MiB inflation',
                               max_source_inflated_bytes=2 * 1024**2, binaries_included=False, full_source_archives_included=False),
                  limits=[
                      'The previous v1/v2 ledgers and five failed builds are copied unchanged, including all nine child probes tied to their original 7e9519 binary.',
                      'Observation v1 is a correct live-memory admission denial before inference. It is not an actual-model TTL execution result.',
                      'Three snapshot regressions, one existing retired-owner regression and six listing tests passed in the observation v1 failed aggregate.',
                      'Any successful observation v2 result uses the exact same 4a7af5 frozen linked executable, source inventory, filters, model path and guards; there is no rebuild or cap increase.',
                      'The actual admitted artifact tests retain their own source-file pins. This packaging verifies source/test/protocol evidence, not new numerical quality or broad model coverage.',
                      'The selected large server source alone is gzip encoded; raw receipts and every previous source artifact are unchanged.',
                      'No source changes, builds, models, GPU or process probes are executed by this evidence assembler. No remote CI, benchmark, GA or release qualification is granted.',
                  ])
    value = pack(ledger)
    assert len(value) + ledger['storage']['payload_bytes_excluding_ledger'] <= 8 * 1024**2
    with (OUT / 'ledger.json').open('xb') as stream:
        stream.write(value)
    print(json.dumps(dict(output=str(OUT), ledger=digest(value), artifacts=len(files), bytes=len(value) + ledger['storage']['payload_bytes_excluding_ledger']), sort_keys=True))


if __name__ == '__main__':
    main()
