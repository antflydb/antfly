"""Probe native request ownership immediately after SSE DONE; no retry or delay.

Diagnostic runner, staged outside the source tree. The tiny invalid request
checks that the native checkpoint guard is released, without another large
checkpoint upload hiding its release latency. Valid resumes also check recovery.
"""
import argparse
import copy
import http.client
import json
import math
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path.cwd() / 'zig/pkg/inference/scripts/gemma4'))
from native_checkpoint_transport import receive_checkpoint, send_resume
from qualify_native_checkpoint_http import (REPO, canonical_choices, git_provenance,
    read_response, requests, response_headers, resume, server, sha256_file)


def guard_probe(connection):
    started = time.monotonic_ns()
    connection.request('POST', '/ai/v1/chat/context/resume', b'{}',
                       {'Content-Type': 'application/json'})
    response = connection.getresponse()
    payload = response.read(65536)
    return {'status': response.status, 'body': payload.decode(errors='replace'),
            'seconds': (time.monotonic_ns() - started) / 1e9}


def require_released(probe):
    if probe['status'] != 400:
        raise AssertionError(f"post-DONE guard probe HTTP {probe['status']}: {probe['body']}")
    result = json.loads(probe['body'])
    if result != {'error': 'INVALID_KV_CHECKPOINT', 'message': 'InvalidNativeKvFrame'}:
        raise AssertionError(f'unexpected guard probe rejection: {result}')


class ProbeOnEof:
    def __init__(self, response, probe):
        self.response = response
        self.probe = probe
        self.result = None
    def read(self, size=-1):
        data = self.response.read(size)
        if size != 0 and not data and self.result is None:
            # receive_checkpoint checks EOF before fsync/atomic publication.
            # Probe here so client-side disk work cannot hide server cleanup.
            self.result = self.probe()
        return data


def percentiles(values):
    ordered = sorted(values)
    if not ordered or any(not math.isfinite(x) or x <= 0 for x in ordered):
        raise ValueError('positive finite latency samples required')
    return {name: ordered[math.ceil(q * len(ordered)) - 1]
            for name, q in [('p50', .5), ('p95', .95), ('max', 1)]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--server', required=True, type=Path)
    parser.add_argument('--server-kind', choices=['inference', 'standalone'], default='standalone')
    parser.add_argument('--models-dir', default=Path('.models'), type=Path)
    parser.add_argument('--reference', required=True, type=Path)
    parser.add_argument('--checkpoint', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--model', default='google/gemma-4-E4B-it-qat-q4_0-gguf')
    parser.add_argument('--padding-lines', type=int, default=742)
    parser.add_argument('--port', type=int, default=8509)
    parser.add_argument('--timeout', type=float, default=1800)
    parser.add_argument('--warmups', type=int, default=5)
    parser.add_argument('--samples', type=int, default=100)
    args = parser.parse_args()
    args.fixture = 'fact'
    if args.warmups < 0 or args.samples < 1: parser.error('invalid sampling counts')
    if args.output.exists(): raise FileExistsError(args.output)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    evidence = {'passed': False, 'production_ready': False,
                'server_kind': args.server_kind,
                'scope': 'Checkpoint export EOF/SSE completion guard and one saved-prefix resume workload only',
                'source': git_provenance(REPO), 'binary_sha256': sha256_file(args.server),
                'checkpoint_sha256': sha256_file(args.checkpoint),
                'reference_sha256': sha256_file(args.reference),
                'runner_sha256': sha256_file(Path(__file__)),
                'warmups_requested': args.warmups, 'samples_requested': args.samples,
                'observations': []}
    def publish():
        temp = args.output.with_suffix('.json.part')
        temp.write_text(json.dumps(evidence, indent=2) + '\n')
        temp.replace(args.output)
    try:
        reference = json.loads(args.reference.read_text())
        if reference.get('passed') is not True: raise AssertionError('reference did not pass')
        exports = [r['result'] for r in reference['exporter']['requests'] if r['kind'] == 'prepare']
        if len(exports) != 1: raise AssertionError('ambiguous reference checkpoint')
        prepared = exports[0]
        if prepared['file_sha256'] != evidence['checkpoint_sha256']:
            raise AssertionError('checkpoint differs from reference')
        expected = next(r['result'] for r in reference['exporter']['requests'] if r['kind'] == 'same_process')
        body = copy.deepcopy(next(iter(requests(args.model, 'f16', [args.padding_lines], False, 'fact'))))
        body.pop('context_management'); body.pop('cache_dtype'); body['stream'] = True
        with server(args, 'worker', evidence) as record:
            cold_body = copy.deepcopy(body); cold_body['stream'] = False
            resume(args, cold_body, record, prepared, 'initial_model_load_and_restore')
            exported_path = args.output.with_suffix('.completion-export.kv')
            if exported_path.exists(): raise FileExistsError(exported_path)
            connection = http.client.HTTPConnection('127.0.0.1',args.port,timeout=args.timeout)
            probe_connection = http.client.HTTPConnection('127.0.0.1',args.port,timeout=args.timeout)
            try:
                connection.request('POST','/ai/v1/chat/context/checkpoint',json.dumps(cold_body).encode(),
                                   {'Content-Type':'application/json',
                                    'X-Antfly-KV-Encoding':prepared['metadata'].get('encoding', 'f32')})
                response = connection.getresponse()
                if response.status != 200:
                    raise AssertionError(f'prepare HTTP {response.status}: {response.read(65536)!r}')
                response_headers(response,record)
                probe_connection.connect()
                observed = ProbeOnEof(response,lambda: guard_probe(probe_connection))
                exported = receive_checkpoint(observed,exported_path)
                evidence['export'] = {'checkpoint':exported,'post_eof_probe':observed.result,
                                      'path':str(exported_path)}
                require_released(observed.result)
                if exported['file_sha256'] != prepared['file_sha256']:
                    raise AssertionError('checkpoint changed from reference')
                publish()
            finally:
                probe_connection.close();connection.close()
            for index in range(args.warmups + args.samples):
                connection = http.client.HTTPConnection('127.0.0.1', args.port, timeout=args.timeout)
                probe_connection = http.client.HTTPConnection('127.0.0.1', args.port, timeout=args.timeout)
                try:
                    # Connect before the model request. After DONE there is no
                    # reconnect, checkpoint upload, sleep or retry before probing.
                    probe_connection.connect()
                    started = time.monotonic_ns()
                    response = send_resume(connection, '/ai/v1/chat/context/resume', body, args.checkpoint)
                    if response.status != 200:
                        raise AssertionError(f'resume HTTP {response.status}: {response.read(65536)!r}')
                    response_headers(response, record)
                    result = read_response(response, True)  # Returns at DONE, before EOF.
                    completed = time.monotonic_ns()
                    probe = guard_probe(probe_connection)
                    observation = {'index': index, 'warmup': index < args.warmups,
                                   'resume_seconds': (completed-started)/1e9,
                                   'result': result, 'post_done_probe': probe}
                    evidence['observations'].append(observation)
                    require_released(probe)
                    if canonical_choices(result['choices']) != canonical_choices(expected['choices']):
                        raise AssertionError('continuation differs from reference')
                    if result['usage'] != expected['usage']:
                        raise AssertionError('usage or saved prefix reuse differs from reference')
                    publish()
                    print(json.dumps({'index': index, 'seconds': observation['resume_seconds'],
                                      'probe_status': probe['status']}), flush=True)
                finally:
                    probe_connection.close(); connection.close()
        memory = evidence['worker']['memory']
        if memory['samples'] < 2 or memory['errors'] or not memory['gpu_uuids']:
            raise AssertionError('incomplete memory evidence')
        if not 0 < memory['peak_host_rss_bytes'] <= 8*1024**3 or not 0 < memory['peak_gpu_memory_bytes'] <= 18*1024**3:
            raise AssertionError('memory cap exceeded')
        if git_provenance(REPO) != evidence['source']: raise AssertionError('source changed')
        for path, key in [(args.server,'binary_sha256'), (args.checkpoint,'checkpoint_sha256'),
                          (args.reference,'reference_sha256'), (Path(__file__),'runner_sha256')]:
            if sha256_file(path) != evidence[key]: raise AssertionError(f'evidence changed: {path}')
        evidence['resume_seconds'] = percentiles([r['resume_seconds'] for r in evidence['observations'] if not r['warmup']])
        evidence['sampling_count_met_for_this_workload'] = args.warmups >= 5 and args.samples >= 100
        evidence['passed'] = True
    except Exception as error:
        evidence['error'] = f'{type(error).__name__}: {error}'
    finally:
        publish()
    print(json.dumps({'passed':evidence['passed'], 'error':evidence.get('error')}), flush=True)
    return 0 if evidence['passed'] else 1


if __name__ == '__main__': raise SystemExit(main())
