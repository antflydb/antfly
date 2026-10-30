#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Run an owned GB10 production server through mixed traffic and lifecycle gates.

Uses canonical managed installs. Every successful extraction is checked against
frozen outputs, including original-text offsets in each public coordinate unit.
Public admission ledgers and Linux process-group PSS are sampled separately from
CUDA owned-buffer measurements in the Zig service qualification.
"""
from __future__ import annotations
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import copy
from decimal import Decimal, InvalidOperation
import json
import os
from pathlib import Path
import signal
import socket
import statistics
import subprocess
import sys
import threading
import time
import traceback
import urllib.error
import urllib.request
import qualify_http as gate


def call(base, path, body=None, method=None, timeout=180):
    data = json.dumps(body, ensure_ascii=False).encode() if body is not None else None
    request = urllib.request.Request(base + path, data=data, method=method or ('POST' if data else 'GET'),
        headers={'Content-Type': 'application/json'} if data else {})
    started = time.monotonic()
    try:
        response = urllib.request.urlopen(request, timeout=timeout)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        raw = response.read().decode()
        content = json.loads(raw) if raw.lstrip().startswith(('{', '[')) else raw
        return response.code, content, (time.monotonic() - started) * 1000


def checked_call(base, path, body=None, method=None):
    status, result, elapsed = call(base, path, body, method)
    if status != 200:
        raise ValueError(f'{method or ("POST" if body is not None else "GET")} {path}: HTTP {status}: {result}')
    return result, elapsed


def coordinate_map(text, unit):
    if unit == 'unicode_codepoints':
        return list(range(len(text) + 1))
    encoding = {'utf8_bytes': 'utf-8', 'utf16_codeunits': 'utf-16-le'}[unit]
    divisor = 2 if unit == 'utf16_codeunits' else 1
    result = [0]
    for char in text:
        result.append(result[-1] + len(char.encode(encoding)) // divisor)
    return result


def check_units(profile, case, response, count, unit):
    normalized = copy.deepcopy(response)
    inverse = {offset: i for i, offset in enumerate(coordinate_map(case['text'], unit))}
    for item in normalized.get('data', []):
        if item.get('offset_unit') != unit:
            raise ValueError(f'offset unit {item.get("offset_unit")} != {unit}')
        item['offset_unit'] = 'unicode_codepoints'
        for entity in item.get('entities', []):
            # Reject coordinates inside a UTF-8 sequence or UTF-16 surrogate pair.
            for endpoint in ('start', 'end'):
                offset = entity.get(endpoint)
                if type(offset) is not int or offset not in inverse:
                    raise ValueError(f'{endpoint} offset {offset!r} is not an integer Unicode boundary in {unit}')
                entity[endpoint] = inverse[offset]
    return gate.check_response(profile, case, normalized, count)


def parse_metrics(raw):
    if not isinstance(raw, str):
        raise ValueError('admission metrics endpoint did not return text exposition')
    values = {}
    for line in raw.splitlines():
        if line.startswith('antfly_inference_admission_') and '{' not in line:
            fields = line.split()
            if len(fields) != 2:
                raise ValueError(f'malformed admission metric: {line}')
            key, encoded = fields
            if key in values:
                raise ValueError(f'duplicate admission metric: {key}')
            try:
                value = Decimal(encoded)
            except InvalidOperation as error:
                raise ValueError(f'invalid admission metric: {line}') from error
            if not value.is_finite() or value < 0 or value > (1 << 64) - 1 or value != value.to_integral_value():
                raise ValueError(f'admission metric must be a nonnegative integer byte count: {line}')
            values[key] = int(value)
    required = ['host_scratch_bytes', 'backend_scratch_bytes', 'host_weight_bytes', 'backend_weight_bytes']
    if any('antfly_inference_admission_' + name not in values for name in required):
        raise ValueError('admission metrics missing from production endpoint')
    return values


def metrics(base):
    raw, _ = checked_call(base, '/ml/v1/metrics')
    return parse_metrics(raw)


def assert_ready(response):
    # Readiness counts installed, discoverable models independently of loaded
    # runtime instances. Both canonical managed extractors are already installed.
    models = response.get('models') if isinstance(response, dict) else None
    count = models.get('extractors') if isinstance(models, dict) else None
    if not isinstance(response, dict) or response.get('status') != 'ready' or type(count) is not int or count < 2:
        raise ValueError(f'expected ready inventory with both installed extractors: {response}')


def assert_idle(values):
    for name in ('host_scratch_bytes', 'backend_scratch_bytes'):
        if values['antfly_inference_admission_' + name] != 0:
            raise ValueError(f'request reservation retained while idle: {values}')


def assert_catalog(catalog, names):
    expected = {'pii': {'named_entity_recognition', 'extraction'},
        'gliguard': {'classification', 'zero_shot', 'multi_label'}}
    observed = {}
    for profile, name in names.items():
        entry = catalog.get('extractors', {}).get(name)
        if not isinstance(entry, dict):
            raise ValueError(f'{profile}: canonical extractor missing from public catalog')
        capabilities = entry.get('capabilities', [])
        if len(capabilities) != len(set(capabilities)) or set(capabilities) != expected[profile]:
            raise ValueError(f'{profile}: unsupported or missing public capabilities: {capabilities}')
        limits = entry.get('qualified_limits', {}).get('specialty')
        if limits != {'sequence_tokens': 512, 'physical_batch_items': 1 if profile == 'pii' else 8}:
            raise ValueError(f'{profile}: public qualified limits drifted: {limits}')
        if entry.get('inputs') != ['text'] or name in catalog.get('deciders', {}):
            raise ValueError(f'{profile}: unsupported public task or input appeared')
        inference = entry.get('inference_capabilities', {})
        if inference.get('task') != 'extract' or inference.get('input_modalities') != ['text']:
            raise ValueError(f'{profile}: public inference contract drifted')
        if inference.get('batch', {}).get('max_items') != 128:
            raise ValueError(f'{profile}: configured public request cardinality changed')
        observed[profile] = entry
    return observed


def capacity_retry_delay(response, elapsed_seconds):
    milliseconds = response.get('retry_after_ms', 1000)
    if type(milliseconds) is not int or milliseconds <= 0:
        raise ValueError(f'invalid capacity retry interval: {response}')
    seconds = milliseconds / 1000
    if elapsed_seconds + seconds > 60:
        raise ValueError(f'capacity did not recover: {response}')
    return seconds


def assert_server_shutdown(was_running, returncode):
    if not was_running or returncode not in (0, -signal.SIGTERM):
        raise ValueError(f'owned server exited unexpectedly or required forced shutdown: {returncode}')


def pss_bytes(group):
    total = 0
    members = []
    for path in Path('/proc').iterdir():
        if not path.name.isdigit():
            continue
        try:
            if os.getpgid(int(path.name)) != group:
                continue
            for line in (path / 'smaps_rollup').read_text().splitlines():
                if line.startswith('Pss:'):
                    total += int(line.split()[1]) * 1024
                    members.append(int(path.name))
        except (ProcessLookupError, FileNotFoundError):
            continue
    if not members:
        raise ValueError('owned server process group has no readable memory samples')
    return {'pss_bytes': total, 'pids': members}


def qualify(args, report):
    manifest, references = gate.verify_inputs(args.artifacts_root)
    report.update({'artifact_manifest_sha256': gate.digest_file(gate.MANIFEST),
        'server_binary': {'path': str(args.server), 'sha256': gate.digest_file(args.server)},
        'harness_sha256': gate.digest_file(Path(__file__)), 'hardware': gate.hardware(),
        'artifact_pins': manifest})
    names = {profile: manifest['models'][profile]['repository'] for profile in gate.PROFILES}
    with socket.socket() as probe:
        probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        probe.bind(('127.0.0.1', args.port))
    command = [str(args.server), 'run', '--host', '127.0.0.1', '--port', str(args.port),
        '--models-dir', str(args.models_root), '--max-loaded-models', '2',
        '--max-concurrent-requests', str(args.concurrency), '--kernel-jit-mode', 'off',
        '--host-budget-mb', '8192', '--backend-budget-mb', '12288',
        '--combined-budget-mb', '20480', '--scratch-budget-mb', '4096']
    env = gate.backend_environment('f32', '0')
    report['command'] = command
    report['backend_environment'] = gate.backend_environment_receipt(env)
    log_path = args.output.with_suffix('.server.log')
    report['server_log'] = str(log_path)
    base = f'http://127.0.0.1:{args.port}'
    with log_path.open('wb') as log:
        process = subprocess.Popen(command, cwd=gate.REPO, env=env, stdout=log,
            stderr=subprocess.STDOUT, start_new_session=True)
        report['pid'] = process.pid
        stop = threading.Event()
        sample_errors = []
        samples = []
        sampler = None
        try:
            report['running_binary_before'] = gate.running_binary_receipt(process, report['server_binary']['sha256'])
            deadline = time.monotonic() + 300
            while True:
                if process.poll() is not None:
                    raise RuntimeError(f'server exited {process.returncode}')
                try:
                    rows, _ = checked_call(base, '/ai/v1/models/instances')
                    if rows.get('data') != []:
                        raise ValueError('fresh owned server unexpectedly has loaded instances')
                    break
                except OSError:
                    if time.monotonic() >= deadline:
                        raise TimeoutError('server failed to start')
                    time.sleep(.2)
            checked_call(base, '/healthz')
            report['readyz_before_load'], _ = checked_call(base, '/readyz')
            assert_ready(report['readyz_before_load'])
            catalog, _ = checked_call(base, '/ai/v1/models')
            report['catalog_before_load'] = assert_catalog(catalog, names)
            report['loaded_instances'] = {}
            for profile, name in names.items():
                row, _ = checked_call(base, '/ai/v1/models/instances', {'model': name, 'task': 'extractor'})
                if row.get('backend') != 'cuda' or row.get('state') != 'loaded' or row.get('startup_managed'):
                    raise ValueError(f'expected on-demand CUDA instance: {row}')
                report['loaded_instances'][profile] = row
            report['readyz_after_load'], _ = checked_call(base, '/readyz')
            assert_ready(report['readyz_after_load'])
            report['cuda_policy_after_load'] = gate.parse_cuda_policy(log_path.read_text(errors='replace'))
            cases = [(profile, case) for profile in gate.PROFILES for case in references[profile]['cases']]
            def extract(index, count=1, unit='unicode_codepoints', retry=True):
                profile, case = cases[index % len(cases)]
                body = json.loads(gate.request_body(names[profile], case, count))
                body['options']['offset_unit'] = unit
                if unit == 'utf8_bytes':
                    del body['options']['offset_unit']  # Exercise the declared public default.
                started = time.monotonic()
                retries = 0
                while True:
                    status, response, elapsed = call(base, '/ai/v1/extract', body)
                    if status == 503 and retry and isinstance(response, dict) and response.get('retryable') is True:
                        delay = capacity_retry_delay(response, time.monotonic() - started)
                        retries += 1
                        time.sleep(delay)
                        continue
                    if status != 200:
                        raise ValueError(f'{profile}/{case["id"]} b{count}: HTTP{status}: {response}')
                    worst = check_units(profile, case, response, count, unit)
                    return {'profile': profile, 'case': case['id'], 'count': count, 'unit': unit,
                        'latency_ms': (time.monotonic() - started) * 1000, 'retries': retries,
                        'max_confidence_abs_error': worst}
            # Validate every frozen case in every public offset unit before load.
            report['validation'] = [extract(i, unit=unit) for i in range(len(cases))
                for unit in ('utf8_bytes', 'unicode_codepoints', 'utf16_codeunits')]
            report['versionless_validation'] = []
            for profile, case in cases:
                body = json.loads(gate.request_body(names[profile], case))
                del body['schema_version']
                response, _ = checked_call(base, '/ai/v1/extract', body)
                worst = check_units(profile, case, response, 1, 'unicode_codepoints')
                report['versionless_validation'].append({'profile': profile, 'case': case['id'],
                    'max_confidence_abs_error': worst})
            report['ragged_validation'] = []
            for profile in gate.PROFILES:
                batch = references[profile]['cases']
                body = json.loads(gate.request_body(names[profile], batch[0]))
                units = ('utf8_bytes', 'unicode_codepoints', 'utf16_codeunits')
                body['inputs'] = [{'id': case['id'], 'content': case['text'],
                    'schema': json.loads(case['native_schema_json']),
                    'options': {'threshold': case.get('threshold', .5), 'offset_unit': units[i % 3],
                        'include_spans': True, 'include_confidence': True}}
                    for i, case in enumerate(batch)]
                response, _ = checked_call(base, '/ai/v1/extract', body)
                if response.get('schema_version') != 2 or len(response.get('data', [])) != len(batch):
                    raise ValueError(f'{profile}: ragged response version/count mismatch')
                errors = [check_units(profile, case, {'schema_version': 2, 'data': [item]}, 1, units[i % 3])
                    for i, (case, item) in enumerate(zip(batch, response['data']))]
                report['ragged_validation'].append({'profile': profile, 'items': len(batch),
                    'ordered_case_ids': [case['id'] for case in batch],
                    'max_confidence_abs_error': max(errors)})
            report['chunked_and_maximum'] = [extract(i, count=count) for i in (0, len(references['pii']['cases']))
                for count in (8, 9, 128)]
            report['rejections'] = []
            for profile in gate.PROFILES:
                case = references[profile]['cases'][0]
                base_body = json.loads(gate.request_body(names[profile], case))
                probes = [ ('oversized', {'inputs': [{'content': 'word ' * 600}]}, 413),
                    ('too_many_items', {'inputs': [{'content': 'x'}] * 129}, 413),
                    ('unsupported_task', {'schema': json.loads(references['gliguard' if profile == 'pii' else 'pii']['cases'][0]['native_schema_json'])}, 400),
                    ('long_document', {'options': {**base_body['options'], 'long_document': {'mode': 'window'}}}, 400)]
                for label, changes, expected_status in probes:
                    body = copy.deepcopy(base_body)
                    body.update(changes)
                    status, response, _ = call(base, '/ai/v1/extract', body)
                    if status != expected_status:
                        raise ValueError(f'{profile}/{label}: HTTP{status}, expected{expected_status}: {response}')
                    expected_code = 'EXTRACTION_LIMIT_EXCEEDED' if status == 413 else 'UNSUPPORTED_EXTRACTION_FEATURE'
                    if not isinstance(response, dict) or response.get('error') != expected_code:
                        raise ValueError(f'{profile}/{label}: wrong rejection contract: {response}')
                    report['rejections'].append({'profile': profile, 'probe': label, 'status': status, 'response': response})
                extract(0 if profile == 'pii' else len(references['pii']['cases']))
            before = metrics(base)
            assert_idle(before)
            report['idle_admission_before'] = before
            def sample():
                try:
                    while not stop.wait(2):
                        samples.append({'elapsed_seconds': time.monotonic() - started,
                            **pss_bytes(process.pid), 'admission': metrics(base)})
                except Exception as error:
                    sample_errors.append(str(error))
            started = time.monotonic()
            sampler = threading.Thread(target=sample)
            sampler.start()
            def worker(worker_index):
                rows = []
                serial = worker_index
                while time.monotonic() - started < args.duration:
                    count = (1, 2, 8, 9)[serial % 4]
                    unit = ('utf8_bytes', 'unicode_codepoints', 'utf16_codeunits')[serial % 3]
                    rows.append(extract(serial, count, unit))
                    serial += args.concurrency
                return rows
            report['traffic'] = []
            with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
                for future in as_completed([pool.submit(worker, i) for i in range(args.concurrency)]):
                    report['traffic'].extend(future.result())
            report['traffic_duration_seconds'] = time.monotonic() - started
            stop.set()
            sampler.join()
            if sample_errors:
                raise ValueError(f'process/admission sampler failed: {sample_errors}')
            if len(samples) < 10 or len(report['traffic']) < args.concurrency * 4:
                raise ValueError('insufficient sustained load evidence')
            report['memory_samples'] = samples
            # Compare medians in two late windows after all maximum shapes warmed.
            late = samples[len(samples) // 2:]
            split = len(late) // 2
            growth = statistics.median(x['pss_bytes'] for x in late[split:]) - statistics.median(x['pss_bytes'] for x in late[:split])
            report['host_pss_gate'] = {'late_window_median_growth_bytes': growth,
                'maximum_growth_bytes': args.max_pss_growth_mb << 20,
                'scope': 'Linux process-group proportional set size; includes CUDA mapped host pages; excludes GPU-private memory'}
            if growth > args.max_pss_growth_mb << 20:
                raise ValueError(f'host PSS did not plateau: {growth} bytes')
            after = metrics(base)
            assert_idle(after)
            report['idle_admission_after'] = after
            if before != after:
                raise ValueError(f'idle reservation ledger changed: before={before}, after={after}')
            report['lifecycle'] = []
            for profile, name in names.items():
                previous = report['loaded_instances'][profile]
                path = '/ai/v1/models/instances/' + previous['id']
                result, _ = checked_call(base, path, method='DELETE')
                if result.get('outcome') != 'unloaded':
                    raise ValueError(f'unload failed: {result}')
                repeated, _ = checked_call(base, path, method='DELETE')
                if repeated.get('outcome') != 'already_unloaded':
                    raise ValueError(f'repeated unload failed: {repeated}')
                reloaded, _ = checked_call(base, '/ai/v1/models/instances', {'model': name, 'task': 'extractor'})
                if reloaded['id'] == previous['id'] or reloaded.get('backend') != 'cuda':
                    raise ValueError(f'reload failed to create CUDA incarnation: {reloaded}')
                recovery = extract(0 if profile == 'pii' else len(references['pii']['cases']))
                report['lifecycle'].append({'profile': profile, 'unload': result, 'repeat': repeated,
                    'reloaded': reloaded, 'recovery': recovery})
            assert_idle(metrics(base))
            checked_call(base, '/healthz')
            report['readyz_after_reload'], _ = checked_call(base, '/readyz')
            assert_ready(report['readyz_after_reload'])
            catalog, _ = checked_call(base, '/ai/v1/models')
            report['catalog_after_reload'] = assert_catalog(catalog, names)
            report['cuda_policy_after_reload'] = gate.parse_cuda_policy(log_path.read_text(errors='replace'))
            report['running_binary_after'] = gate.running_binary_receipt(process, report['server_binary']['sha256'])
            report['summary'] = {'requests': len(report['traffic']),
                'items': sum(x['count'] for x in report['traffic']),
                'retryable_capacity_responses': sum(x['retries'] for x in report['traffic']),
                'max_confidence_abs_error': max(x['max_confidence_abs_error'] for x in report['traffic'])}
        finally:
            stop.set()
            if sampler:
                sampler.join(timeout=190)
            was_running = process.poll() is None
            gate.terminate_server(process)
            report['server_returncode'] = process.returncode
            assert_server_shutdown(was_running, process.returncode)
    report['server_log_sha256'] = gate.digest_file(log_path)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--server', type=Path, required=True)
    parser.add_argument('--models-root', type=Path, required=True)
    parser.add_argument('--artifacts-root', type=Path, default=gate.REPO / '.specialty-artifacts')
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--port', type=int, default=18985)
    parser.add_argument('--duration', type=float, default=300)
    parser.add_argument('--concurrency', type=int, default=8)
    parser.add_argument('--max-pss-growth-mb', type=int, default=256)
    args = parser.parse_args()
    if args.duration < 60 or not 1 <= args.concurrency <= 8 or args.max_pss_growth_mb < 1 or not 1 <= args.port <= 65535:
        parser.error('require duration>=60, concurrency1..8, positivePSSbound, validport')
    for field in ('server', 'models_root', 'artifacts_root', 'output'):
        setattr(args, field, getattr(args, field).resolve())
    args.output.parent.mkdir(parents=True, exist_ok=True)
    report = {'format_version': 1, 'campaign': 'GB10 managed specialty production soak',
        'status': 'failed', 'started_unix_seconds': time.time(), 'duration_seconds': args.duration,
        'concurrency': args.concurrency, 'memory_scope': 'Public admission + host PSS; CUDA owned-buffer peaks proven separately in internal service gate'}
    signal.signal(signal.SIGTERM, lambda *_: (_ for _ in ()).throw(KeyboardInterrupt()))
    try:
        qualify(args, report)
        report['status'] = 'passed'
    except (Exception, KeyboardInterrupt) as error:
        report['error'] = str(error)
        report['traceback'] = traceback.format_exc()
        print(report['traceback'], file=sys.stderr)
    finally:
        report['finished_unix_seconds'] = time.time()
        args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n')
    return 0 if report['status'] == 'passed' else 1


if __name__ == '__main__':
    sys.exit(main())
