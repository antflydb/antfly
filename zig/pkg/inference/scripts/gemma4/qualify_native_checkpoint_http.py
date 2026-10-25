#!/usr/bin/env python3
"""Public native checkpoint roundtrip qualification; no private compaction flags."""
import argparse
import contextlib
import copy
import http.client
import json
import math
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

REPO = Path(__file__).resolve().parents[5]
SCRIPTS = REPO / 'zig/pkg/inference/scripts/gemma4'
sys.path.insert(0, str(SCRIPTS))
from qualify_attention_match_queries import (active_group_members, canonical_choices,
    discover_worker_pid, read_response, requests, validate_fixture_choice, wait_server_ready)
from qualify_context_management import CudaProcessSampler, git_provenance, sha256_file
from native_checkpoint_transport import CONTENT_TYPE, receive_checkpoint, send_resume


@contextlib.contextmanager
def server(args, label, evidence):
    from native_checkpoint_unified_host import qualification_admission_capacity
    capacity = qualification_admission_capacity(args)
    if getattr(args, 'server_kind', 'inference') == 'standalone':
        from native_checkpoint_unified_host import unified_server
        with unified_server(args, label, evidence, SCRIPTS) as record:
            yield record
        return
    environment = {k: v for k, v in os.environ.items() if not k.startswith('ANTFLY_INFERENCE_ATTENTION_MATCH_')}
    overrides = {'ANTFLY_INFERENCE_PREFERRED_BACKEND': 'cuda', 'ANTFLY_INFERENCE_REQUIRED_BACKEND': 'cuda',
                 'ANTFLY_SERVER_DECODE_GRAPH_REPLAY': 'off', 'ANTFLY_INFERENCE_CUDA_REPLAY_LAST_LOGITS': '0',
                 'ANTFLY_SERVER_DISABLE_CONTINUOUS_BATCHING': '1'}
    overrides['ANTFLY_INFERENCE_EXPERIMENTAL_ROTATED4_CHECKPOINTS'] = str(int(getattr(args, 'experimental_rotated4', False)))
    environment.update(overrides)
    if getattr(args, 'generation_timing', False):
        overrides['TERMITE_SERVER_GENERATE_TIMING'] = '1'
        environment['TERMITE_SERVER_GENERATE_TIMING'] = '1'
    command = ['bash', str(SCRIPTS / 'with_gemma4_qat_cuda_tuning.sh'), str(args.server.resolve()),
               'run', '--host', '127.0.0.1', '--port', str(args.port), '--models-dir', str(args.models_dir.resolve()),
               '--max-concurrent-requests', str(capacity), '--host-budget-mb', '8192', '--backend-budget-mb', '18432',
               '--combined-budget-mb', '26624', '--kv-budget-mb', '4096', '--scratch-budget-mb', '6144']
    record = {'command': command, 'environment_overrides': overrides, 'requests': []}
    evidence[label] = record
    log_path = args.output.with_suffix('.' + label + '.log')
    sampler = None
    with log_path.open('w') as log:
        process = subprocess.Popen(command, env=environment, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        try:
            wait_server_ready(process, log_path, args.port)
            worker = discover_worker_pid(process.pid, args.server)
            record['worker_pid'] = worker
            sampler = CudaProcessSampler(worker); sampler.start()
            yield record
        finally:
            if sampler is not None: record['memory'] = sampler.finish()
            try: os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError: pass
            try: process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL); process.wait(timeout=30)
            deadline = time.monotonic() + 30
            while active_group_members(process.pid) and time.monotonic() < deadline: time.sleep(0.05)
            if active_group_members(process.pid): raise RuntimeError('qualification worker group did not exit')
            record['stopped'] = True


def response_headers(response, record):
    headers = {k.lower(): v for k, v in response.getheaders()}
    if headers.get('x-antfly-inference-backend') != 'cuda': raise AssertionError('missing CUDA attestation')
    if int(headers.get('x-antfly-inference-pid', '0')) != record['worker_pid']: raise AssertionError('worker changed')
    return headers


def generate(args, body, record):
    connection = http.client.HTTPConnection('127.0.0.1', args.port, timeout=args.timeout)
    try:
        start = time.monotonic()
        connection.request('POST', '/ai/v1/generate', json.dumps(body).encode(), {'Content-Type': 'application/json'})
        response = connection.getresponse()
        if response.status != 200: raise RuntimeError(f'baseline HTTP {response.status}: {response.read(65536)!r}')
        headers = {k.lower(): v for k, v in response.getheaders()}
        result = read_response(response, False)
        record['requests'].append({'kind': 'baseline', 'seconds': time.monotonic() - start, 'headers': headers, 'result': result})
        response_headers(response, record)
        return result
    finally: connection.close()


def prepare(args, body, record):
    connection = http.client.HTTPConnection('127.0.0.1', args.port, timeout=args.timeout)
    try:
        start = time.monotonic()
        connection.request('POST', '/ai/v1/generate/context/checkpoint', json.dumps(body).encode(), {'Content-Type': 'application/json', 'X-Antfly-KV-Encoding': getattr(args, 'encoding', 'f16')})
        response = connection.getresponse()
        if response.status != 200: raise RuntimeError(f'prepare HTTP {response.status}: {response.read(65536)!r}')
        headers = response_headers(response, record)
        if headers.get('content-type', '').split(';')[0] != CONTENT_TYPE: raise AssertionError('wrong export content type')
        result = receive_checkpoint(response, args.checkpoint, allow_rotated4=getattr(args, 'encoding', 'f16') == 'rotated4')
        metadata = result['metadata']
        encoding = getattr(args, 'encoding', 'f16')
        if metadata.get('encoding') != encoding or metadata.get('wire_version') != {'f32': 2, 'f16': 3, 'fp8': 3, 'rotated4': 4}[encoding]:
            raise AssertionError('checkpoint did not use the requested encoding')
        if metadata['model'] != args.model or metadata['outcome'] != args.expected_outcome: raise AssertionError(metadata)
        if metadata['outcome'] == 'compacted':
            savings = metadata['global_savings_ratio']
            if not math.isfinite(savings) or savings < args.minimum_savings: raise AssertionError(metadata)
        if encoding == 'rotated4':
            from qualify_attention_match_queries import validate_quantization_evidence
            validate_quantization_evidence([metadata], encoding, require_actual=args.expected_outcome == 'compacted')
        if metadata.get('fp8_heads', 0) < getattr(args, 'minimum_fp8_heads', 0):
            raise AssertionError('FP8 campaign did not exercise enough quantized heads')
        record['requests'].append({'kind': 'prepare', 'seconds': time.monotonic() - start, 'headers': headers, 'result': result})
        return result
    finally: connection.close()


def resume(args, body, record, prepared, label, expected_status=200, mutate=None):
    connection = http.client.HTTPConnection('127.0.0.1', args.port, timeout=args.timeout)
    try:
        start = time.monotonic()
        response = send_resume(connection, '/ai/v1/generate/context/resume', body, args.checkpoint, mutate)
        if response.status != expected_status:
            raise AssertionError(f'{label} HTTP {response.status}, wanted {expected_status}: {response.read(65536)!r}')
        if expected_status != 200:
            if response.getheader('Content-Type', '').startswith('text/event-stream'): raise AssertionError('invalid checkpoint started SSE')
            result = json.loads(response.read(65536))
            if not result.get('error'): raise AssertionError('missing rejection error')
        else:
            response_headers(response, record)
            result = read_response(response, body.get('stream', False))
        record['requests'].append({'kind': label, 'status': response.status, 'seconds': time.monotonic() - start, 'result': result})
        if expected_status == 200:
            if result.get('usage', {}).get('cached_prompt_tokens') != prepared['metadata']['logical_tokens']:
                raise AssertionError('resume did not attest reuse of saved prefix')
            validate_fixture_choice(result['choices'][0], args.fixture)
        return result
    finally: connection.close()


def python_sdk_roundtrip(args, body, record, prepared):
    # Run this qualifier with py/packages/sdk/.venv/bin/python when enabled.
    sys.path.insert(0, str(REPO / 'py/packages/sdk/src'))
    from antfly import AntflyClient
    from antfly.client_generated.models import InferenceGenerateRequest
    client = AntflyClient(f'http://127.0.0.1:{args.port}', timeout=args.timeout)
    http = client._client.get_httpx_client()
    observed_headers = []
    def attest(response):
        headers = dict(response.headers)
        if response.status_code == 200:
            if headers.get('x-antfly-inference-backend') != 'cuda': raise AssertionError('SDK missing CUDA attestation')
            if int(headers.get('x-antfly-inference-pid', '0')) != record['worker_pid']: raise AssertionError('SDK worker changed')
        observed_headers.append({'status': response.status_code, 'headers': headers})
    http.event_hooks['response'].append(attest)
    temporary = args.checkpoint.with_name(args.checkpoint.name + '.sdk.kv')
    if temporary.exists(): raise FileExistsError(temporary)
    start = time.monotonic()
    try:
        request = InferenceGenerateRequest.from_dict(body)
        result = client.resume_native_kv_checkpoint(request, args.checkpoint).to_dict()
        validate_fixture_choice(result['choices'][0], args.fixture)
        if result['usage']['cached_prompt_tokens'] != prepared['metadata']['logical_tokens']:
            raise AssertionError('SDK resume did not reuse saved prefix')
        exported = client.prepare_native_kv_checkpoint(request, temporary, encoding=getattr(args, 'encoding', 'f16'))
        if exported.sha256 != prepared['file_sha256']:
            raise AssertionError('SDK export differs from identical raw HTTP preparation')
        record['requests'].append({'kind': 'python_sdk_roundtrip', 'seconds': time.monotonic()-start,
            'headers': observed_headers, 'result': result, 'checkpoint_sha256': exported.sha256,
            'metadata': dict(exported.metadata)})
    finally:
        http.close()
        temporary.unlink(missing_ok=True)


def reject_append_limit(args, body, record, prepared, label):
    # Request streaming so a rejection must precede both generation and SSE headers.
    request = copy.deepcopy(body)
    request['stream'] = True
    result = resume(args, request, record, prepared, label, expected_status=400)
    if result.get('message') != 'KvCheckpointAppendLimit':
        raise AssertionError(f'append rejected for an unexpected reason: {result}')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--server', type=Path, required=True)
    parser.add_argument('--server-kind', choices=['inference', 'standalone'], default='inference')
    parser.add_argument('--models-dir', type=Path, default=Path('.models'))
    parser.add_argument('--model', default='google/gemma-4-E4B-it-qat-q4_0-gguf')
    parser.add_argument('--port', type=int, default=8500)
    parser.add_argument('--padding-lines', type=int, default=742)
    parser.add_argument('--encoding', choices=['f32', 'f16', 'fp8', 'rotated4'], default='f16')
    parser.add_argument('--experimental-rotated4', action='store_true')
    parser.add_argument('--fixture', choices=['fact', 'tool_call'], default='fact')
    parser.add_argument('--expected-outcome', choices=['compacted', 'unchanged_insufficient_savings'], default='compacted')
    parser.add_argument('--timeout', type=float, default=1800)
    parser.add_argument('--python-sdk', action='store_true')
    parser.add_argument('--append-mode', choices=['multichunk', 'limit_rejection'], default='multichunk')
    parser.add_argument('--minimum-savings', type=float, default=1.5)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--checkpoint', type=Path, required=True)
    args = parser.parse_args()
    args.minimum_fp8_heads = 1 if args.encoding == 'fp8' and args.expected_outcome == 'compacted' else 0
    if not math.isfinite(args.minimum_savings) or not 1.5 <= args.minimum_savings <= 4:
        parser.error('--minimum-savings must be finite and between 1.5 and 4')
    if args.append_mode == 'limit_rejection' and args.fixture != 'fact':
        parser.error('--append-mode limit_rejection requires --fixture fact')
    args.output.parent.mkdir(parents=True, exist_ok=True); args.checkpoint.parent.mkdir(parents=True, exist_ok=True)
    evidence = {'passed': False, 'scope': 'public native checkpoint roundtrip and restart; not full production qualification',
                'server_kind': args.server_kind, 'encoding': args.encoding, 'append_mode': args.append_mode, 'minimum_savings': args.minimum_savings,
                'source': git_provenance(REPO), 'binary_sha256': sha256_file(args.server),
                'runner_sha256': sha256_file(Path(__file__)), 'transport_sha256': sha256_file(Path(__file__).with_name('native_checkpoint_transport.py'))}
    try:
        original = next(iter(requests(args.model, 'f16', [args.padding_lines], False, args.fixture)))
        public = copy.deepcopy(original); public.pop('context_management'); public.pop('cache_dtype')
        with server(args, 'exporter', evidence) as record:
            baseline = generate(args, original, record); validate_fixture_choice(baseline['choices'][0], args.fixture)
            if args.append_mode == 'limit_rejection' and baseline['usage']['prompt_tokens'] < 32000:
                raise AssertionError('limit-rejection workload must exercise at least 32000 prompt tokens')
            prepared = prepare(args, public, record)
            first = resume(args, public, record, prepared, 'same_process')
            appended = None
            if args.fixture == 'fact':
                appended = copy.deepcopy(public)
                appended['messages'] += [
                    {'role': 'assistant', 'content': 'Acknowledged.'},
                    {'role': 'user', 'content': 'This follow-up log entry has no changes.\n' * 128 +
                     'Retrieve the access code from the original log. Reply with only the access code.'}]
                if args.append_mode == 'limit_rejection':
                    reject_append_limit(args, appended, record, prepared, 'appended_limit_rejection')
                else:
                    full_append = copy.deepcopy(appended); full_append['cache_dtype'] = 'f16'
                    # Ordinary generation exposes runtime headers when context
                    # diagnostics are requested, as on the original baseline.
                    full_append['context_management'] = copy.deepcopy(original['context_management'])
                    append_baseline = generate(args, full_append, record)
                    validate_fixture_choice(append_baseline['choices'][0], args.fixture)
                    append_result = resume(args, appended, record, prepared, 'appended_multichunk')
                    suffix_tokens = append_result['usage']['prompt_tokens'] - prepared['metadata']['logical_tokens']
                    if suffix_tokens < 512: raise AssertionError('append did not cover multiple 256-token prefill chunks')
                    record['appended_suffix_tokens'] = suffix_tokens
                stream = copy.deepcopy(public); stream['stream'] = True
                for i in range(2): resume(args, stream, record, prepared, f'sse_{i}')
            wrong = copy.deepcopy(public); wrong['messages'][0]['content'] = 'Changed prefix.\n' + wrong['messages'][0]['content']; wrong['stream'] = True
            resume(args, wrong, record, prepared, 'wrong_prefix', 400)
            size = args.checkpoint.stat().st_size
            def corrupt(offset, block):
                return block[:-1] + bytes([block[-1] ^ 1]) if offset + len(block) == size else block
            bad = copy.deepcopy(public); bad['stream'] = True
            resume(args, bad, record, prepared, 'checksum_corruption', 400, corrupt)
            resume(args, public, record, prepared, 'recovery_after_rejections')
            if args.python_sdk: python_sdk_roundtrip(args, public, record, prepared)
        with server(args, 'importer', evidence) as record:
            if record['worker_pid'] == evidence['exporter']['worker_pid']: raise AssertionError('restart reused worker PID')
            restored = resume(args, public, record, prepared, 'cross_process')
            if canonical_choices(first['choices']) != canonical_choices(restored['choices']): raise AssertionError('restart changed deterministic continuation')
            if appended is not None:
                if args.append_mode == 'limit_rejection':
                    reject_append_limit(args, appended, record, prepared, 'cross_process_limit_rejection')
                    resume(args, public, record, prepared, 'cross_process_recovery_after_limit')
                else:
                    restored_append = resume(args, appended, record, prepared, 'cross_process_multichunk_append')
                    if canonical_choices(append_result['choices']) != canonical_choices(restored_append['choices']):
                        raise AssertionError('restart changed appended deterministic continuation')
        if sha256_file(args.checkpoint) != prepared['file_sha256']: raise AssertionError('checkpoint mutated')
        if git_provenance(REPO) != evidence['source']: raise AssertionError('source changed during qualification')
        if sha256_file(args.server) != evidence['binary_sha256']: raise AssertionError('server changed during qualification')
        for label in ['exporter', 'importer']:
            memory = evidence[label]['memory']
            if memory['samples'] < 2 or memory['errors'] or not memory['gpu_uuids']:
                raise AssertionError(f'{label}: incomplete memory evidence')
            for key, limit in [('peak_host_rss_bytes', 8 * 1024**3), ('peak_gpu_memory_bytes', 18 * 1024**3)]:
                if memory[key] is None or memory[key] > limit:
                    raise AssertionError(f'{label}: {key}={memory[key]} exceeds {limit}')
        evidence['python_sdk_exercised'] = args.python_sdk
        evidence['passed'] = True
    except Exception as error:
        evidence['error'] = f'{type(error).__name__}: {error}'
    finally:
        args.output.write_text(json.dumps(evidence, indent=2) + '\n')
    print(json.dumps({'passed': evidence['passed'], 'error': evidence.get('error'), 'output': str(args.output)}), flush=True)
    return 0 if evidence['passed'] else 1


if __name__ == '__main__': raise SystemExit(main())
