#!/usr/bin/env python3
"""Archive bounded existing evidence only; never executes a model or a build."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import tarfile

REPO = Path('/Users/timkaye/Documents/af/antfly')
OUT = Path('/private/tmp/gliner25-qualification-lifecycle-evidence-v1')
MAX_STORED = 8 * 1024**2
SELECTED = [
    'extractors/gliner_boundary_qualification.zig',
    'models/gliner_boundary_qualification.zig',
    'server/gliner_boundary_cache_lifecycle_test.zig',
    'server/model_manager.zig',
    'backends/session.zig',
    'architectures/session_factory.zig',
    'execution_control.zig',
    'hard_cancellation_watchdog.zig',
    'pipelines/gliner_boundary_decode.zig',
    'finetune/gliner_boundary_dataset.zig',
    'finetune/gliner_boundary_training_export.zig',
    'finetune/gliner_boundary_training_job.zig',
    'finetune/seeded_gradient_trainer.zig',
]
SELECTED = ['zig/pkg/inference/src/' + name for name in SELECTED]
EXPECTED = {
    1: dict(inventory='58aa9063f8b723850f943ccb49cf3bd5dd1bed9436f890ef53b4e4516330d53d',
            stderr='4400fbaecd14a533a8139d3daf1f244c155f7d30ab39b939a9cfd9cab7e4b5c5'),
    2: dict(inventory='6047ee7c164dca1a114af134331f74d5b49eb2eb23a10f1ae49e10925afdc0b9',
            process='7d57125e2c778691cee128ec1b4646662ea24bbbec04569f9137f2aa4bb3ab7b',
            archive='91230afa7e2fc8dcd5f098dc13f420570cbbc9236b12bc47f6539d72fac53624',
            executable='ddc9797781b4e98bf8b8f88613aa209f3fbf283124e2b326090a9b0e84406415'),
    3: dict(inventory='9702432df97c5ddf55c730b60def46a194f64fb8d2521a591f91f4d2cef6d933',
            process='14ff88ff7f11151ffe2297ce7bac7b7028705c7f85cceb133eef5387b93f8a9f',
            archive='e18598ddf04be59fd37f5c99602c28dd84e2413aa00160f371d587a2720d8300',
            executable='13d9487cc46e99945be9b5f813bc8660fe1d45c341dccf092126a19cc8745c83'),
}
SUPERVISOR = REPO / 'zig/pkg/inference/testdata/gliner25/published_inactive_classifier_cpu_v1/helpers/v2/supervision.py'
WRAPPER = Path('/private/tmp/antfly-gliner25-regional-validation/run.py')
CI_PATHS = [
    '.github/workflows/zig-tests.yml',
    'zig/pkg/inference/scripts/run_model_contract_tests.py',
    'zig/pkg/inference/scripts/gliner25/requirements-contract.txt',
    'zig/pkg/inference/scripts/gliner25/requirements.txt',
    'zig/pkg/inference/scripts/gliner25/bootstrap_monitoring.py',
]


def digest(raw: bytes) -> dict:
    return dict(size_bytes=len(raw), sha256=hashlib.sha256(raw).hexdigest())


def signature(value):
    return (value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns)


def read(path: Path, cap: int = 1024**2) -> bytes:
    with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK), 'rb') as f:
        before = os.fstat(f.fileno())
        assert stat.S_ISREG(before.st_mode) and 0 <= before.st_size <= cap, str(path)
        raw = f.read(cap + 1)
        assert len(raw) == before.st_size and signature(before) == signature(os.fstat(f.fileno())), str(path)
        return raw


def hash_file(path: Path, cap: int) -> dict:
    with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK), 'rb') as f:
        before = os.fstat(f.fileno())
        assert stat.S_ISREG(before.st_mode) and 0 <= before.st_size <= cap
        sha = hashlib.sha256()
        size = 0
        for chunk in iter(lambda: f.read(1024**2), b''):
            size += len(chunk)
            assert size <= cap
            sha.update(chunk)
        assert size == before.st_size and signature(before) == signature(os.fstat(f.fileno()))
        return dict(size_bytes=size, sha256=sha.hexdigest())


def json_bytes(value):
    return (json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + '\n').encode()


def main():
    OUT.mkdir(mode=0o700)
    files = {}
    stored = 0

    def save(name, raw):
        nonlocal stored
        assert not Path(name).is_absolute() and '..' not in Path(name).parts
        pin = digest(raw)
        if name in files:
            assert files[name] == pin and read(OUT / name) == raw
            return dict(path=name, **pin)
        stored += len(raw)
        assert stored <= MAX_STORED
        target = OUT / name
        target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        with target.open('xb') as f:
            f.write(raw)
        files[name] = pin
        return dict(path=name, **pin)

    def copy(name, path, cap=1024**2):
        return save(name, read(path, cap))

    source_bytes = {}
    checkpoints = []
    inventories = {}
    archived_sources = {}
    for version in (1, 2, 3):
        root = Path(f'/private/tmp/antfly-gliner25-qualification-lifecycle-metal-v{version}')
        prefix = f'runs/v{version}/'
        raw = {p.name: read(p) for p in sorted(root.iterdir())}
        assert set(raw) == {'start.json', 'process.json', 'source_inventory.json', 'stdout.log', 'stderr.log'} | {
            f'observed-test-{i}.json' for i in range(1, 2 if version == 1 else 3)}
        record = {name: save(prefix + name, value) for name, value in raw.items()}
        start, receipt, inventory = (json.loads(raw[name]) for name in ('start.json', 'process.json', 'source_inventory.json'))
        process = receipt['process']
        assert all(receipt[name] == value for name, value in start.items())
        assert digest(raw['source_inventory.json']) == receipt['source_inventory']
        assert receipt['source_count'] == len(inventory) == 2587
        assert receipt['qualification'] is False and process['returncode'] == 1
        assert process['source_inventory_unchanged'] is True and process['changed_sources'] == []
        assert process['cleanup']['complete'] is True and process['cleanup']['direct_child_reaped'] is True
        assert process['cleanup']['known_children_gone'] is True
        assert process['cleanup']['survivors'] == process['cleanup']['errors'] == process['cleanup']['inspection_errors'] == []
        for name in ('stdout', 'stderr'):
            assert digest(raw[name + '.log']) == process[name]
        for i, observed in enumerate(process['observed_test_executables'], 1):
            assert json.loads(raw[f'observed-test-{i}.json']) == observed
        assert digest(raw['source_inventory.json'])['sha256'] == EXPECTED[version]['inventory']
        if version == 1:
            assert digest(raw['stderr.log'])['sha256'] == EXPECTED[version]['stderr']
        else:
            assert digest(raw['process.json'])['sha256'] == EXPECTED[version]['process']
        stderr = raw['stderr.log'].decode()
        assert '23/23 tests passed' in stderr
        main_totals = None
        if version == 1:
            assert 'switch must handle all possibilities' in stderr and "unhandled enumeration value: 'allow'" in stderr
            assert not re.search(r'^\d+/\d+ ', stderr, re.M)
        else:
            matches = re.findall(r'(\d+) passed; (\d+) skipped; (\d+) failed; (\d+) leaked', stderr)
            assert len(matches) == 1, matches
            passed, skipped, failed, leaked = map(int, matches[0])
            main_totals = dict(selected=passed + skipped + failed, passed=passed, skipped=skipped, failed=failed, leaked=leaked)
            assert main_totals == dict(selected=47 + version, passed=45 + version, skipped=1, failed=1, leaked=0)
        checkpoint = dict(
            id=f'v{version}', outcome='compile_failure' if version == 1 else 'test_failure',
            raw=record, original_directory=str(root), main_test_totals=main_totals,
            ancillary_build_tests=dict(passed=23, total=23, is_main_selected_suite=False),
            command=start['command'], cwd=start['cwd'], environment=start['environment'],
            source_inventory=start['source_inventory'], source_count=start['source_count'], source_selection=start['source_selection'],
            helper_pins=dict(supervisor=start['supervisor'], wrapper=start['wrapper']),
            observed_test_executables=process['observed_test_executables'],
            returncode=process['returncode'], failure=process['failure'], failure_phase=process['failure_phase'],
            elapsed_seconds=process['elapsed_seconds'], peak_child_tree_rss_bytes=process['peak_child_tree_rss_bytes'],
            rss_measurement=process['rss_measurement'], optional_executable_identity_errors=process['optional_executable_identity_errors'],
            process_cleanup=process['cleanup'], inspection_error_count=process['inspection_error_count'],
            guards={key: process[key] for key in (
                'timeout_seconds', 'max_child_tree_rss_bytes', 'max_stdout_bytes', 'max_stderr_bytes',
                'max_tracked_processes', 'rss_poll_seconds', 'parent_grace_seconds', 'kill_wait_seconds',
                'worker_parent_loss_grace_seconds')},
            source_unchanged_after_run=True, main_executable=None, original_source_archive=None,
        )
        inventories[version] = inventory
        if version != 1:
            archive_root = Path(str(root) + '-archive')
            archive_raw = read(archive_root / 'archive.json')
            archive = json.loads(archive_raw)
            checkpoint['archive_receipt'] = save(prefix + 'archive.json', archive_raw)
            assert archive['tests'] == main_totals and archive['source_files'] == len(inventory)
            assert archive['process_receipt'] == digest(raw['process.json']) and archive['source_inventory'] == digest(raw['source_inventory.json'])
            for name in ('start.json', 'process.json', 'source_inventory.json', 'stdout.log', 'stderr.log'):
                assert read(archive_root / name) == raw[name]
            exe_pin = hash_file(archive_root / 'test', 128 * 1024**2)
            assert exe_pin['sha256'] == EXPECTED[version]['executable']
            assert all(archive['frozen_executable'][key] == value for key, value in exe_pin.items())
            assert archive['live_observation'] == process['observed_test_executables'][1]
            assert all(archive['live_observation']['executable'][key] == value for key, value in exe_pin.items())
            checkpoint['main_executable'] = dict(**exe_pin, original_path=str(archive_root / 'test'), rehashed=True, bytes_included=False)
            tar_path = archive_root / 'recorded-source-selection.tar.gz'
            archive_pin = hash_file(tar_path, 64 * 1024**2)
            assert archive_pin == archive['source_archive'] and archive_pin['sha256'] == EXPECTED[version]['archive']
            checkpoint['original_source_archive'] = dict(**archive_pin, original_path=str(tar_path), rehashed=True, bytes_included=False)
            wanted = set(SELECTED)
            recovered = {}
            with tarfile.open(tar_path, 'r|gz') as tar:
                for member in tar:
                    if member.name not in wanted:
                        continue
                    assert member.isreg() and member.name not in recovered and member.size <= 1024**2
                    stream = tar.extractfile(member)
                    assert stream is not None
                    value = stream.read(1024**2 + 1)
                    assert len(value) == member.size and digest(value) == inventory[member.name]
                    recovered[member.name] = value
            assert set(recovered) == wanted
            assert hash_file(tar_path, 64 * 1024**2) == archive_pin
            archived_sources[version] = recovered
            checkpoint['archivist_failure_interpretation'] = archive['failure']
        checkpoints.append(checkpoint)

    patch_path = Path('/private/tmp/gliner25-runtime-qualification-policy-v1.patch')
    patch = read(patch_path)
    patch_ref = save('source_recovery/v1_model_policy.patch', patch)
    lines = patch.decode().splitlines(keepends=True)
    target = 'zig/pkg/inference/src/models/gliner_boundary_qualification.zig'
    start = lines.index('*** Add File: ' + target + '\n') + 1
    end = next(i for i in range(start, len(lines)) if lines[i].startswith('*** '))
    assert all(line.startswith('+') for line in lines[start:end])
    model_policy = ''.join(line[1:] for line in lines[start:end]).encode()
    assert digest(model_policy) == inventories[1][target]
    extractor_target = 'zig/pkg/inference/src/extractors/gliner_boundary_qualification.zig'
    extractor_path = Path('/private/tmp/gliner25-runtime-qualification-integration-v1/proposed') / extractor_target
    extractor_policy = read(extractor_path)
    assert digest(extractor_policy) == inventories[1][extractor_target]
    for version, checkpoint in enumerate(checkpoints, 1):
        selected = []
        for name in SELECTED:
            if version == 1 and name == target:
                value = model_policy
                origin = dict(method='unchanged Add File payload recovered from pre-existing private patch', original_path=str(patch_path), patch=patch_ref)
            elif version == 1 and name == extractor_target:
                value = extractor_policy
                origin = dict(method='pre-existing private proposed source', original_path=str(extractor_path))
            else:
                archive_version = max(2, version)
                value = archived_sources[archive_version][name]
                origin = dict(method='selected regular tar member; hash matched this checkpoint inventory', archive_version=archive_version, member=name)
            assert digest(value) == inventories[version][name], (version, name)
            pin = digest(value)
            source_ref = save('sources/' + pin['sha256'] + '/' + Path(name).name, value)
            selected.append(dict(repo_path=name, artifact=source_ref, recovery=origin))
        checkpoint['selected_sources'] = selected

    reasons = [
        dict(code='unhandled_allow_enum', detail="Extractor qualification options.overlap switch omitted enum .allow; main inference tests did not execute.",
             source='zig/pkg/inference/src/extractors/gliner_boundary_qualification.zig', line=94),
        dict(code='warm_device_counter_assumption', detail='Test expected request MetalTensor buffers to remain owned after HTTP completion; failed assertion is preserved.',
             source='zig/pkg/inference/src/server/gliner_boundary_cache_lifecycle_test.zig', line=240),
        dict(code='five_second_fixture_deadline', detail='TTL eviction returned but the fixture observation control exceeded five seconds. The thirty-second production teardown ticket did not expire; this was not watchdog exit 86.',
             source='zig/pkg/inference/src/server/gliner_boundary_cache_lifecycle_test.zig', line=179),
    ]
    for checkpoint, reason in zip(checkpoints, reasons):
        checkpoint['failure_interpretation'] = reason
        selected = next(item for item in checkpoint['selected_sources'] if item['repo_path'] == reason['source'])
        source = read(OUT / selected['artifact']['path']).decode().splitlines()
        assert reason['line'] <= len(source)
        checkpoint['failure_interpretation']['source_line'] = source[reason['line'] - 1]

    helper_refs = dict(supervisor=copy('helpers/supervision.py', SUPERVISOR), wrapper=copy('helpers/run.py', WRAPPER))
    for checkpoint in checkpoints:
        assert all({k: v for k, v in helper_refs[name].items() if k != 'path'} == checkpoint['helper_pins'][name] for name in helper_refs)

    logs = []
    for name, expected_count, expected_outcome, expected_sha in (
        ('handoff', 203, 'OK', '07d779b415050beb27348bbc85f209b0835b7eb408409b0742270a147a04cf31'),
        ('clean-imports', 201, 'FAILED (errors=25)', 'f2cdad11ad1ace24dd417276711f9413e7f0d8155f2736f2782cfe6712707b06'),
        ('isolated', 203, 'OK', '95e98a1e74854468d891ea5b056f2e3ba4553d7d4e1931588185e9ea3ca870e2'),
    ):
        original = Path(f'/private/tmp/gliner25-contracts-pr-{name}-v1.log')
        value = read(original)
        text = value.decode()
        matches = re.findall(r'^Ran (\d+) tests in ([0-9.]+)s$', text, re.M)
        assert len(matches) == 1 and int(matches[0][0]) == expected_count
        assert text.rstrip().endswith('\n' + expected_outcome) and digest(value)['sha256'] == expected_sha
        record = dict(id=name, raw=save('contracts/' + original.name, value), original_path=str(original),
                      tests_run=expected_count, outcome=expected_outcome, reported_seconds=float(matches[0][1]),
                      errors=25 if name == 'clean-imports' else 0, skips_reported=0,
                      exact_runtime_command_recorded=False, interpreter_identity_recorded=False)
        if name == 'clean-imports':
            record['missing_modules'] = sorted(set(re.findall(r"ModuleNotFoundError: No module named '([^']+)'", text)))
            assert record['missing_modules'] == ['packaging', 'psutil', 'pytest']
            record['context'] = 'Parent reports this was the clean python -S reproduction; the raw log does not record its argv.'
        else:
            record['context'] = 'Parent identifies oracle environment handoff run.' if name == 'handoff' else 'Parent identifies isolated minimal dependency run; raw log records six installed packages.'
        logs.append(record)
    ci_sources = {name: copy('contracts/current/' + name, REPO / name) for name in CI_PATHS}
    requirements = read(REPO / CI_PATHS[2]).decode()
    packages = [line for line in requirements.splitlines() if line and not line.startswith('#')]
    assert packages == ['iniconfig==2.3.0', 'packaging==25.0', 'pluggy==1.6.0', 'psutil==7.1.3', 'Pygments==2.19.2', 'pytest==9.0.2']
    workflow = read(REPO / CI_PATHS[0]).decode()
    command = 'uv run --no-project --isolated --no-python-downloads --no-build'
    assert workflow.count(command) == 2
    base_head = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=REPO, timeout=5).decode().strip()
    diff = subprocess.check_output(['git', 'diff', '--no-ext-diff', '--', CI_PATHS[0], CI_PATHS[1]], cwd=REPO, timeout=5)
    assert len(diff) <= 65536 and b'requirements-contract.txt' in diff
    ci_diff = save('contracts/ci_changes_from_head.patch', diff)
    for name in CI_PATHS[:2]:
        value = subprocess.check_output(['git', 'show', base_head + ':' + name], cwd=REPO, timeout=5)
        assert len(value) <= 1024**2
        copy_name = 'contracts/base/' + name
        save(copy_name, value)
    # Recheck current CI bytes after collecting the patch and baseline; no checkout mutation.
    assert all(digest(read(REPO / name)) == {k: v for k, v in ref.items() if k != 'path'} for name, ref in ci_sources.items())

    builder = copy('helpers/stage_evidence.py', Path(__file__))
    verifier = copy('verify.py', Path('/private/tmp/verify_gliner25_qualification_lifecycle_evidence_v1.py'))
    readme = copy('README.md', Path('/private/tmp/gliner25_qualification_lifecycle_evidence_README_v1.md'))
    ledger = dict(
        scope='gliner25_qualification_lifecycle_evidence/v1', version=1, qualification=False,
        status='historical_failed_checkpoints_preserved_corrected_run_pending',
        local_only=True, files=files, checkpoints=checkpoints, helper_sources=helper_refs,
        contracts=dict(runs=logs, exact_minimal_dependencies=packages, source_snapshots=ci_sources,
                       current_head=base_head, current_diff=ci_diff, remote_ci_executed=False,
                       workflow_jobs=['zig-base-tests', 'zig-full-tests'],
                       limits='Logs prove the stated local test outcomes, not an independently pinned interpreter environment or remote CI run. CI snapshots preserve current proposed invocation and dependency pins.'),
        storage=dict(maximum_bytes=MAX_STORED, payload_bytes_excluding_ledger=stored, binaries_included=False,
                     full_source_archives_included=False, selected_sources_per_checkpoint=len(SELECTED),
                     source_inventory_is_dependency_closure=False),
        interpretation_limits=[
            'All three Zig build commands failed. Passing selected tests do not make a failed aggregate pass.',
            'The ancillary 23-test build results are separate from the main inference selection; v1 has no main selected-suite result.',
            'The full 2587-entry inventories are recorded source selections, not complete dependency closures or proof every source was imported.',
            'Selected historical source bytes are hash-verified against each checkpoint; v1 model policy is the exact prior patch payload, not a reconstructed current implementation.',
            'Main v2/v3 executables and original tar archives were rehashed from their archives, but only their identities are stored here.',
            'Process RSS and elapsed times are diagnostic supervisor evidence, not a performance benchmark.',
            'No new model, numerical fixture, Zig build, process-lifecycle test, commit, push, or remote CI execution occurred while preparing this ledger.',
            'A corrected later run must be appended as new evidence; these failed histories remain unchanged.',
        ],
        assembler=builder, verifier=verifier, readme=readme,
    )
    payload = json_bytes(ledger)
    assert stored + len(payload) <= MAX_STORED
    with (OUT / 'ledger.json').open('xb') as f:
        f.write(payload)
    print(json.dumps(dict(output=str(OUT), ledger=digest(payload), stored_bytes=stored + len(payload), artifacts=len(files)), sort_keys=True))


if __name__ == '__main__':
    main()
