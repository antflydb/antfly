#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Qualify automatic CUDA read-pool batching through real HTTP requests.

Native batch counters must advance in each isolated model phase. Independent
references, request usage/identity, cancellation recovery and mandatory managed
and service-NVML evidence are checked separately from throughput measurements.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import copy
import hashlib
import json
import math
from pathlib import Path
import socket
import statistics
import struct
import threading
import time
import urllib.parse

import qualify_read_pool_40g as q
from qualify_cooperative_execution import serving_identity
from embeddinggemma2.compare import assert_agreement


def metrics(url):
    status, raw, _ = q.http(url, '/metrics', text=True)
    assert status == 200
    return {parts[0]: float(parts[1]) for line in raw.splitlines()
            if not line.startswith('#') and len(parts := line.split()) == 2}


def equivalent(a, b, tolerance):
    if isinstance(a, dict):
        assert a.keys() == b.keys(), (a.keys(), b.keys())
        for key in a:
            equivalent(a[key], b[key], tolerance)
    elif isinstance(a, list):
        assert len(a) == len(b)
        for x, y in zip(a, b, strict=True):
            equivalent(x, y, tolerance)
    elif isinstance(a, (int, float)) and not isinstance(a, bool):
        assert math.isfinite(b) and abs(a-b) <= tolerance, (a, b)
    else:
        assert a == b, (a, b)



def embedding_equivalent(expected, actual, max_error=.005):
    old, new = copy.deepcopy(expected), copy.deepcopy(actual)
    assert len(old['data']) == len(new['data'])
    checks = []
    for a, b in zip(old['data'], new['data'], strict=True):
        checks.append(assert_agreement(b.pop('embedding'), a.pop('embedding'), max_error=max_error))
    equivalent(old, new, 0)
    return checks


def native_cases():
    inputs = [body['input'][0] for body in cases('embed', 4)]
    result = [{'name': f'native-query-{dim}', 'exact': True, 'body':
               {'model': q.EMBED, 'input': inputs, 'task_type': 'RETRIEVAL_QUERY', 'dimensions': dim}}
              for dim in (128, 256)]
    queries = ['How can I reset a forgotten account password?',
               'Why was my subscription charged twice?',
               'Where is the warehouse stock report?',
               'How often should garden plants be watered?']
    documents = ['Account access instructions explain how to reset a forgotten password and sign in.',
                 'Billing support investigates duplicate subscription charges and payment refunds.',
                 'Warehouse inventory reports list stock levels, shipments, and shelf locations.',
                 'Gardening instructions describe watering plants and checking soil moisture.']
    for name, texts, task in [('retrieval-query', queries, 'RETRIEVAL_QUERY'), ('retrieval-document', documents, 'RETRIEVAL_DOCUMENT')]:
        result.append({'name': name, 'exact': False, 'body': {'model': q.EMBED, 'input': texts, 'task_type': task, 'dimensions': 256}})
    return result


def retrieval_agreement(old, new, tolerance=.005):
    def scores(values):
        queries = values['retrieval-query']['data']
        docs = values['retrieval-document']['data']
        return [[sum(x*y for x,y in zip(query['embedding'], doc['embedding'], strict=True)) for doc in docs] for query in queries]
    a, b = scores(old), scores(new)
    max_error = max(abs(x-y) for row1,row2 in zip(a,b,strict=True) for x,y in zip(row1,row2,strict=True))
    assert max_error <= tolerance, ('retrieval score drift', max_error)
    first = lambda values: [max(range(len(row)), key=row.__getitem__) for row in values]
    assert first(a) == first(b) == [0,1,2,3], ('retrieval top result changed', first(a), first(b))
    return {'max_score_error': max_error, 'top1': first(b), 'baseline_scores': a, 'candidate_scores': b}


def capture_native(args):
    args.mode = 'cooperative'
    c = q.Campaign(args)
    result = {'scope': 'Existing explicit native batch control; this is not a qualification campaign',
              'identity': serving_identity(args, c), 'cases': []}
    responses = {}
    for case in native_cases():
        status, response, _ = q.http(args.url, '/embed', case['body'])
        assert status == 200, response
        responses[case['name']] = response
        result['cases'].append({**case, 'response': response})
    result['retrieval'] = retrieval_agreement(responses, responses)
    (args.output/'native-references.json').write_text(json.dumps(result, indent=2)+'\n')
    print('Captured native batch and retrieval controls', flush=True)

def cases(kind, count=16):
    result = []
    for i in range(count):
        if kind == 'embed':
            body = {'model': q.EMBED, 'input': [f'How can I reset my account password for user {i}?'],
                    'task_type': 'RETRIEVAL_QUERY', 'dimensions': 128 if i % 2 else 256}
        elif kind == 'extract':
            labels = ['account_access', 'billing'] if i % 2 else ['account_access', 'billing', 'other']
            body = {'model': q.DECIDE, 'schema_version': 2,
                    'schema': {'classifications': [{'name': f'route_{i}', 'labels': labels}]},
                    'inputs': [{'id': f'input-{i}', 'content': f'I cannot sign in to account {i}. Please reset my password.'}],
                    'options': {'include_confidence': True}}
        else:
            body = copy.deepcopy(q.Campaign.decide())
            body['input'] += f' My account number is {i}.'
            body['questions'][0]['name'] = f'route_{i}'
        result.append(body)
    return result


def run(args):
    args.output.mkdir(parents=True, exist_ok=True)
    args.generation_timeout = 180
    args.disable_thinking = True
    args.pressure = False
    args.mode = 'cooperative'
    c = q.Campaign(args)
    c.task_kinds = ('generate', 'embed', 'extract', 'decisions')
    monitor = threading.Thread(target=c.monitor, daemon=True)
    monitor.start()
    result = {'status': 'running', 'scope': 'GB10 functional and memory qualification; no A100 performance claim',
              'binary_sha256': hashlib.sha256(args.binary.read_bytes()).hexdigest(),
              'configuration': json.loads(args.config.read_text()), 'baseline': args.baseline, 'routes': args.kinds, 'rounds': args.rounds,
              'acceptance': {'embedding_max_abs': .005, 'embedding_min_cosine': .9999, 'embedding_norm_error': 1e-4, 'exact_native_batch_max_abs': 1e-5, 'retrieval_score_max_abs': .005, 'classification_max_abs': .005}, 'phases': {}}
    lock = threading.Lock()
    rows = []
    reference_lookup = {}

    def call(kind, body, deadline=None, expected=200):
        status, response, seconds = q.http(args.url, '/'+kind, body, deadline_ms=deadline)
        row = {'phase': c.phase, 'kind': kind, 'status': status, 'seconds': seconds,
               'response': response, 'request': body}
        with lock:
            rows.append(row)
            c.rows.append(row)
            with (args.output/'requests.jsonl').open('a') as f:
                f.write(json.dumps(row)+'\n')
        if expected is not None:
            assert status == expected, (kind, status, response)
        if status == 200 and kind == 'embed':
            assert len(response['data']) == len(body['input'])
            for item in response['data']:
                vector = item['embedding']
                assert len(vector) == body['dimensions'] and all(math.isfinite(x) for x in vector)
                assert abs(math.sqrt(sum(x*x for x in vector))-1) <= 1e-4
        reference = reference_lookup.get((kind, json.dumps(body, sort_keys=True)))
        if status == 200 and reference is not None:
            if kind == 'embed':
                row['embedding_agreement'] = embedding_equivalent(reference, response)
            else:
                equivalent(reference, response, .005)
        return row

    try:
        result['identity'] = serving_identity(args, c)
        status, inventory, _ = q.http(args.url, '/models/instances')
        assert status == 200 and {i['model'] for i in inventory['data']} == {q.GEMMA, q.EMBED, q.DECIDE}
        assert all(i['backend'] == 'cuda' and i['startup_managed'] for i in inventory['data'])
        c.instances_before = inventory['data']
        c.set_phase('references')
        bodies = {kind: cases(kind) for kind in args.kinds}
        references = {kind: [call(kind, body)['response'] for body in batch] for kind, batch in bodies.items()}
        for kind, batch in bodies.items():
            for body, reference in zip(batch, references[kind], strict=True):
                reference_lookup[kind, json.dumps(body, sort_keys=True)] = reference
        for round_index in range(args.rounds):
            for kind in bodies:
                c.set_phase(f'{kind}-{round_index}')
                before = metrics(args.url)
                barrier = threading.Barrier(len(bodies[kind]))
                def checked(i):
                    barrier.wait()
                    row = call(kind, bodies[kind][i])
                    embedding_equivalent(references[kind][i], row['response']) if kind == 'embed' else equivalent(references[kind][i], row['response'], .005)
                    return row
                started = time.monotonic()
                with ThreadPoolExecutor(max_workers=len(bodies[kind])) as pool:
                    completed = list(pool.map(checked, range(len(bodies[kind]))))
                after = metrics(args.url)
                native = after.get('antfly_inference_executor_microbatch_native_batches_total', 0)-before.get('antfly_inference_executor_microbatch_native_batches_total', 0)
                items = after.get('antfly_inference_executor_microbatch_native_items_total', 0)-before.get('antfly_inference_executor_microbatch_native_items_total', 0)
                if not args.baseline:
                    assert native > 0 and items >= native*2, ('no native batches observed', kind, native, items)
                result['phases'][c.phase] = {'requests': len(completed), 'native_batches': native, 'native_items': items,
                    'wall_seconds': time.monotonic()-started, 'p95_ms': q.summary(completed)['p95_ms']}
                print(c.phase, result['phases'][c.phase], flush=True)
        if 'embed' in bodies and not args.baseline:
            c.set_phase('native-batch-and-retrieval')
            if args.native_references is None or not args.native_references.is_file():
                raise q.EvidenceUnavailable('existing native batch controls are required')
            native = json.loads(args.native_references.read_text())
            for key in ('gpu', 'runtime_environment', 'configuration'):
                if native['identity'][key] != result['identity'][key]:
                    raise q.EvidenceUnavailable(f'native control {key} does not match serving campaign')
            old, current = {}, {}
            for case in native['cases']:
                response = call('embed', case['body'])['response']
                checks = embedding_equivalent(case['response'], response, 1e-5 if case['exact'] else .005)
                result.setdefault('native_batch_checks', {})[case['name']] = checks
                old[case['name']], current[case['name']] = case['response'], response
            result['retrieval'] = retrieval_agreement(old, current)
            # Distinct semantic requests also traverse the automatic HTTP path.
            semantic = []
            for case in native['cases']:
                if case['exact']:
                    continue
                for text in case['body']['input']:
                    body = {**case['body'], 'input': [text]}
                    reference_lookup['embed', json.dumps(body, sort_keys=True)] = call('embed', body)['response']
                    semantic.append(body)
            with ThreadPoolExecutor(max_workers=16) as pool:
                for future in [pool.submit(call, 'embed', body) for body in semantic for _ in range(2)]:
                    future.result()
        c.set_phase('options-and-arrays')
        extras = []
        if 'embed' in bodies:
            body = copy.deepcopy(bodies['embed'][0])
            body.update(task_type='RETRIEVAL_DOCUMENT', dimensions=128,
                        input=['A warehouse account needs a new password.', 'The garden invoice has a duplicate payment.'])
            extras.append(('embed', body))
        if 'extract' in bodies:
            body = copy.deepcopy(bodies['extract'][0])
            body['inputs'] = [{'id': 'array-a', 'content': 'Please reset my password.'},
                              {'id': 'array-b', 'content': 'Please refund this invoice.'}]
            extras.append(('extract', body))
        if 'decisions' in bodies:
            body = copy.deepcopy(bodies['decisions'][0])
            text = body.pop('input')
            body['inputs'] = [{'id': 'array-a', 'input': text}, {'id': 'array-b', 'input': 'Please refund the duplicate subscription charge.'}]
            extras.append(('decisions', body))
        for kind, body in extras:
            reference_lookup[kind, json.dumps(body, sort_keys=True)] = call(kind, body)['response']
        with ThreadPoolExecutor(max_workers=6) as pool:
            for future in [pool.submit(call, kind, body) for kind, body in extras for _ in range(2)]:
                future.result()
        if 'embed' in bodies:
            c.set_phase('long-embedding-fallback')
            body = copy.deepcopy(bodies['embed'][0])
            body['input'] = ['An account password needs resetting. ' * 24]
            before = metrics(args.url)
            reference = call('embed', body)['response']
            equivalent(reference, call('embed', body)['response'], .002)
            after = metrics(args.url)
            assert after.get('antfly_inference_executor_microbatch_groups_total', 0) == before.get('antfly_inference_executor_microbatch_groups_total', 0), 'long embedding unexpectedly entered short-query broker'
        c.set_phase('request-limits')
        for kind in bodies:
            body = copy.deepcopy(bodies[kind][0])
            if kind == 'embed':
                body['input'] *= 9
            elif kind == 'extract':
                body['inputs'] = [{'id': str(i), 'content': body['inputs'][0]['content']} for i in range(9)]
            else:
                text = body.pop('input')
                body['inputs'] = [{'id': str(i), 'input': text} for i in range(9)]
            call(kind, body, expected=413)
        c.set_phase('mixed-generation')
        generation = q.Campaign.generate(16, 128)
        generation['chat_template_kwargs'] = {'enable_thinking': False}
        with ThreadPoolExecutor(max_workers=10) as pool:
            active = [pool.submit(call, 'generate', generation) for _ in range(2)]
            traffic = [pool.submit(call, kind, body) for kind in bodies for body in bodies[kind][:8]]
            for future in active+traffic:
                future.result()
        c.set_phase('deadline-and-disconnect')
        for kind in bodies:
            body = copy.deepcopy(bodies[kind][0])
            with ThreadPoolExecutor(max_workers=8) as pool:
                denied = [pool.submit(call, kind, body, 1, None) for _ in range(4)]
                healthy = [pool.submit(call, kind, body) for _ in range(4)]
                for future in denied:
                    row = future.result()
                    assert row['status'] in (200, 503, 504), row
                for future in healthy:
                    embedding_equivalent(references[kind][0], future.result()['response']) if kind == 'embed' else equivalent(references[kind][0], future.result()['response'], .005)
            host = urllib.parse.urlsplit(args.url)
            payload = json.dumps(body).encode()
            for _ in range(4):
                with socket.create_connection((host.hostname, host.port)) as client:
                    client.sendall(f'POST /ai/v1/{kind} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {len(payload)}\r\n\r\n'.encode()+payload)
                    time.sleep(.001)
                    client.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
        c.set_phase('recovery')
        for kind in bodies:
            embedding_equivalent(references[kind][0], call(kind, bodies[kind][0])['response']) if kind == 'embed' else equivalent(references[kind][0], call(kind, bodies[kind][0])['response'], .005)
        deadline = time.monotonic()+20
        while True:
            m = metrics(args.url)
            if m['antfly_admission_inference_in_flight_requests'] == 0 and m['antfly_inference_generation_active_requests'] == 0 and m['antfly_inference_admission_backend_kv_bytes'] == 0:
                break
            assert time.monotonic() < deadline, 'request/KV ownership did not recover'
            time.sleep(.05)
        status, inventory, _ = q.http(args.url, '/models/instances')
        c.instances_after = inventory['data']
        assert status == 200 and {i['id'] for i in c.instances_before} == {i['id'] for i in c.instances_after}
        c.require_memory_evidence()
        c.status = result['status'] = 'passed'
    except BaseException as error:
        c.status = result['status'] = 'incomplete' if isinstance(error, q.EvidenceUnavailable) else 'failed'
        c.error = result['error'] = str(error)
        raise
    finally:
        try:
            c.stop_monitor(monitor)
        finally:
            result['status'] = c.status
            if c.error:
                result['error'] = c.error
            # Persist agreement measurements added after each HTTP response.
            (args.output/'requests.jsonl').write_text(''.join(json.dumps(row)+'\n' for row in rows))
            result['memory_evidence'] = {'complete': c.status == 'passed', 'sampling_errors': c.evidence_errors}
            (args.output/'qualification.json').write_text(json.dumps(result, indent=2)+'\n')


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--url', default='http://127.0.0.1:18840')
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--cgroup', type=Path, required=True)
    p.add_argument('--binary', type=Path, required=True)
    p.add_argument('--config', type=Path, required=True)
    p.add_argument('--rounds', type=int, default=3)
    p.add_argument('--native-references', type=Path, help='Captured existing explicit-batch control on the same GPU/configuration')
    p.add_argument('--capture-native-references', action='store_true')
    p.add_argument('--baseline', action='store_true', help='Control campaign: do not require cross-request native batch counters')
    p.add_argument('--kinds', nargs='+', choices=('embed','extract','decisions'), default=['embed','extract','decisions'])
    args = p.parse_args()
    if args.rounds < 1:
        p.error('--rounds must be positive')
    args.output.mkdir(parents=True, exist_ok=True)
    if args.capture_native_references:
        capture_native(args)
    else:
        run(args)


if __name__ == '__main__':
    main()
