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

"""Qualify document HTTP cohorts against an artifact-matched previous server.

The corpus is a synthetic regression fixture, not broad retrieval-quality evidence.
Fixed acceptance: cosine >= .9999, coordinate error <= .005, norm error <= .0001;
median burst p95 <= 1.25x control (+10 ms noise allowance), throughput >= .90x.
Mixed-model tails <= 2x control (+50 ms), generation completion <= 1.25x.
"""
import argparse
import copy
from concurrent.futures import ThreadPoolExecutor
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
from qualification_identity import require_matching_artifacts, verify_artifacts
from qualify_read_pool_microbatch import embedding_equivalent, equivalent, metrics

PARAGRAPHS = [
    'Account access support explains password resets, sign in problems, account security, and recovery instructions. ',
    'Billing support investigates duplicate subscription charges, customer invoices, payment refunds, and account balances. ',
    'Warehouse inventory reports list stock levels, outgoing shipments, incoming deliveries, and shelf locations. ',
    'Gardening instructions describe watering plants, checking soil moisture, growing vegetables, and preparing garden beds. ',
]
QUERIES = ['How can I reset my password?', 'Why was I charged twice?', 'Where is the warehouse stock report?', 'How often should I water my garden?']


def bucket(tokens):
    if tokens < 16:
        return tokens
    step = (1 << (tokens.bit_length()-1)) // 4
    return min(8192, (tokens+step-1)//step*step)


def body(text, dimensions=256, task='RETRIEVAL_DOCUMENT'):
    return {'model': q.EMBED, 'input': [text], 'dimensions': dimensions, 'task_type': task}


def retrieve(documents, queries):
    scores = [[sum(x*y for x,y in zip(query['data'][0]['embedding'], doc['data'][0]['embedding'], strict=True)) for doc in documents] for query in queries]
    return {'scores': scores, 'top1': [max(range(len(row)), key=row.__getitem__) for row in scores]}


def verify_recorded_retrieval(control, candidate, rows):
    """Rank with document vectors returned by concurrent bursts, never B1 replays."""
    require_matching_artifacts(control['identity'], candidate['identity'])
    query_rows={row['request']['input'][0]:row['response'] for row in rows
                if row['phase']=='document-retrieval' and row['kind']=='embed'
                and row['request'].get('task_type')=='RETRIEVAL_QUERY' and row['status']==200}
    if set(query_rows)!=set(QUERIES):
        raise q.EvidenceUnavailable('retrieval lacks complete recorded query vectors')
    completed={json.dumps(row['request'],sort_keys=True):row['response'] for row in rows
               if row['phase'].startswith('documents-') and row['kind']=='embed' and row['status']==200}
    result={}
    for level in sorted({case['level'] for case in candidate['cases']}):
        cases=[case for case in candidate['cases'] if case['level']==level][:4]
        try:
            documents=[completed[json.dumps(case['request'],sort_keys=True)] for case in cases]
        except KeyError as error:
            raise q.EvidenceUnavailable('retrieval lacks recorded burst document vectors') from error
        ranking=retrieve(documents,[query_rows[text] for text in QUERIES])
        old=control['retrieval'][str(level)]
        drift=max(abs(a-b) for x,y in zip(old['scores'],ranking['scores'],strict=True) for a,b in zip(x,y,strict=True))
        assert ranking['top1']==old['top1']==[0,1,2,3], ('batched retrieval ranking changed',level,ranking)
        assert drift<=.005, ('batched retrieval score drift',level,drift)
        result[str(level)]={**ranking,'max_score_drift':drift,'documents_from':'recorded concurrent burst'}
    return result


def run(args):
    args.output.mkdir(parents=True, exist_ok=True)
    args.mode = 'cooperative'
    args.pressure = False
    args.generation_timeout = 180
    args.disable_thinking = True
    c = q.Campaign(args)
    c.task_kinds = ('embed', 'decide', 'generate')
    result = {'status': 'running', 'scope': 'GB10 document batching regression, shared GPU; no A100 or broad quality/SLO claim',
              'rounds': args.rounds, 'phases': {}, 'acceptance': {'cosine_min': .9999, 'max_abs': .005, 'norm_error_max': .0001,
              'p95_ratio_max': 1.25, 'p95_noise_ms': 10, 'throughput_ratio_min': .90, 'mixed_tail_ratio_max': 2, 'mixed_noise_ms': 50}}
    monitor = threading.Thread(target=c.monitor, daemon=True)
    monitor.start()
    lock = threading.Lock()
    rows = []
    references = {}

    def call(kind, request, expected=200, deadline=None):
        phase = c.phase
        status, response, seconds = q.http(args.url, {'embed':'/embed', 'decide':'/decisions', 'generate':'/generate'}[kind], request, deadline_ms=deadline)
        row = {'phase': phase, 'kind': kind, 'status': status, 'seconds': seconds, 'completed_at': time.monotonic(), 'request': request, 'response': response}
        with lock:
            rows.append(row)
            c.rows.append(row)
        if expected is not None:
            assert status == expected, (kind, status, response)
        if status == 200 and kind == 'embed':
            assert len(response['data']) == len(request['input'])
            for item in response['data']:
                vector = item['embedding']
                assert len(vector) == request['dimensions'] and all(math.isfinite(v) for v in vector)
                assert abs(math.sqrt(sum(v*v for v in vector))-1) <= .0001
            reference = references.get(json.dumps(request, sort_keys=True))
            if reference is not None:
                row['agreement'] = embedding_equivalent(reference, response)
        elif status == 200 and kind == 'decide':
            assert response['answers'][0]['choice'] == 'account_access'
        elif status == 200 and kind == 'generate':
            assert response['choices'][0]['message']['content'].strip()
            assert response['usage']['completion_tokens'] == request['max_tokens']
        return row

    def burst(cases, phase):
        c.set_phase(phase)
        before = metrics(args.url)
        barrier = threading.Barrier(len(cases))
        def start(case):
            barrier.wait()
            return call('embed', case['request'])
        started = time.monotonic()
        with ThreadPoolExecutor(max_workers=len(cases)) as pool:
            completed = list(pool.map(start, cases))
        elapsed = time.monotonic()-started
        after = metrics(args.url)
        observed = {'wall_seconds': elapsed, 'p95_ms': q.summary(completed)['p95_ms'], 'requests': len(completed),
                    'native_batches': after.get('antfly_inference_executor_microbatch_native_batches_total',0)-before.get('antfly_inference_executor_microbatch_native_batches_total',0),
                    'native_items': after.get('antfly_inference_executor_microbatch_native_items_total',0)-before.get('antfly_inference_executor_microbatch_native_items_total',0)}
        result['phases'][phase] = observed
        print(phase, observed, flush=True)
        return completed

    try:
        result['identity'] = serving_identity(args, c)
        status, inventory, _ = q.http(args.url, '/models/instances')
        assert status == 200
        c.instances_before = inventory['data']
        if args.control:
            previous = json.loads((args.control/'qualification.json').read_text())
            if previous['status'] != 'passed':
                raise q.EvidenceUnavailable('control did not pass')
            require_matching_artifacts(previous['identity'], result['identity'])
            for key in ('configuration','gpu','runtime_environment'):
                if previous['identity'][key] != result['identity'][key]:
                    raise q.EvidenceUnavailable(f'control identity differs: {key}')
            assert previous['rounds'] == args.rounds
            cases = previous['cases']
            for case in cases:
                references[json.dumps(case['request'], sort_keys=True)] = case['reference']
        else:
            c.set_phase('reference-calibration')
            corpus = []
            for paragraph in PARAGRAPHS:
                first = call('embed', body(paragraph))['response']['usage']['prompt_tokens']
                second = call('embed', body(paragraph*2))['response']['usage']['prompt_tokens']
                corpus.append((paragraph, first, second-first))
            cases = []
            for level in args.levels:
                for index in range(8):
                    paragraph, first, stride = corpus[index % 4]
                    target = level-index
                    repetitions = max(1, 1+(target-first-32)//stride)
                    text = paragraph*repetitions
                    response = call('embed', body(text))['response']
                    deficit = target-response['usage']['prompt_tokens']
                    assert deficit >= 0, ('bad calibration', target, response['usage'])
                    for _ in range(3):
                        text += ' detail'*deficit
                        request = body(text)
                        response = call('embed', request)['response']
                        deficit = target-response['usage']['prompt_tokens']
                        if deficit == 0:
                            break
                        assert deficit > 0, ('calibration overshot', target, response['usage'])
                    assert deficit == 0
                    cases.append({'level': level, 'tokens': target, 'bucket': bucket(target), 'request': request, 'reference': response})
                    references[json.dumps(request, sort_keys=True)] = response
        result['cases'] = cases
        for round_index in range(args.rounds):
            for level in args.levels:
                cohort = [case for case in cases if case['level'] == level]
                assert len(cohort) == 8
                phase = f'documents-{level}-{round_index}'
                burst(cohort, phase)
                observed = result['phases'][phase]
                if args.control:
                    if level <= 4096:
                        assert observed['native_batches'] > 0 and observed['native_items'] >= 2, ('no native document batches', phase, observed)
                    else:
                        assert observed['native_batches'] == 0, ('oversized cohort', phase, observed)
        c.set_phase('arrays-and-options')
        array = {**cases[0]['request'], 'input': [next(case for case in cases if case['level']==level and case['tokens']==level-offset)['request']['input'][0] for level,offset in [(128,0),(2048,1),(512,2),(4096,3),(1024,4),(8192,5),(4096,6),(256 if 256 in args.levels else 128,7)]]}
        if args.control:
            references[json.dumps(array, sort_keys=True)] = previous['array_reference']
        result['array_reference'] = call('embed', array)['response']
        option_cases = [body(cases[16+i]['request']['input'][0], dimensions=128 if i%2 else 768) for i in range(8)]
        if args.control:
            for request, reference in zip(option_cases, previous['option_references'], strict=True):
                references[json.dumps(request, sort_keys=True)] = reference
        result['option_references'] = [call('embed', request)['response'] for request in option_cases]
        with ThreadPoolExecutor(max_workers=8) as pool:
            for future in [pool.submit(call, 'embed', request) for request in option_cases]:
                future.result()
        if args.control:
            c.set_phase('three-row-documents')
            thirds = []
            for case in [case for case in cases if case['level']==4096][:3]:
                text = case['request']['input'][0]
                request = body(text[:int(len(text)*.58)])
                reference = call('embed',request)['response']
                assert 2048 < reference['usage']['prompt_tokens'] <= 2560
                references[json.dumps(request,sort_keys=True)] = reference
                thirds.append((request,reference))
            combined = {**thirds[0][0], 'input':[item[0]['input'][0] for item in thirds]}
            expected = copy.deepcopy(thirds[0][1])
            expected['data'] = [copy.deepcopy(item[1]['data'][0]) for item in thirds]
            for index,item in enumerate(expected['data']): item['index']=index
            expected['usage'] = {key:sum(item[1]['usage'][key] for item in thirds) for key in expected['usage']}
            references[json.dumps(combined,sort_keys=True)] = expected
            before=metrics(args.url)
            call('embed',combined)
            after=metrics(args.url)
            assert after['antfly_inference_executor_microbatch_native_items_total']-before['antfly_inference_executor_microbatch_native_items_total']==3
        c.set_phase('document-retrieval')
        queries = [call('embed', body(query, task='RETRIEVAL_QUERY'))['response'] for query in QUERIES]
        result['retrieval'] = {}
        if args.control:
            result['retrieval']=verify_recorded_retrieval(previous,result,rows)
        else:
            for level in args.levels:
                documents = [case['reference'] for case in cases if case['level']==level][:4]
                ranking=retrieve(documents,queries)
                assert ranking['top1']==[0,1,2,3], ('semantic regression',level,ranking)
                result['retrieval'][str(level)]=ranking
        c.set_phase('mixed-models')
        mixed_docs = [case for case in cases if case['level'] in (512,2048)]
        generation = {**q.Campaign.generate(16,64), 'chat_template_kwargs': {'enable_thinking': False}}
        with ThreadPoolExecutor(max_workers=16) as pool:
            futures = [pool.submit(call, 'generate', generation) for _ in range(2)]
            futures += [pool.submit(call, 'embed', case['request']) for case in mixed_docs]
            futures += [pool.submit(call, 'decide', q.Campaign.decide()) for _ in range(8)]
            for future in futures:
                future.result()
        if args.control:
            c.set_phase('interactive-query-turnover')
            query=body(QUERIES[0],task='RETRIEVAL_QUERY')
            references[json.dumps(query,sort_keys=True)]=call('embed',query)['response']
            document_jobs=[case for case in cases if case['level']==2048]
            with ThreadPoolExecutor(max_workers=16) as pool:
                documents=[pool.submit(call,'embed',case['request']) for case in document_jobs]
                time.sleep(.02)
                queries=[pool.submit(call,'embed',query) for _ in range(8)]
                completed_queries=[future.result() for future in queries]
                completed_documents=[future.result() for future in documents]
            result['interactive_queries']=q.summary(completed_queries)
            assert result['interactive_queries']['p95_ms'] <= 1500, ('interactive query tail exceeds 1500ms fixture gate',result['interactive_queries'])
            assert max(row['completed_at'] for row in completed_queries) < max(row['completed_at'] for row in completed_documents), 'short queries did not turn over before the document backlog drained'
        c.set_phase('deadline-and-disconnect')
        request = next(case for case in cases if case['level']==2048)['request']
        with ThreadPoolExecutor(max_workers=8) as pool:
            expired = [pool.submit(call, 'embed', request, None, 1) for _ in range(4)]
            healthy = [pool.submit(call, 'embed', request) for _ in range(4)]
            failures = [future.result()['status'] for future in expired]
            assert all(status in (200,503,504) for status in failures) and any(status != 200 for status in failures)
            for future in healthy:
                future.result()
        host = urllib.parse.urlsplit(args.url)
        payload = json.dumps(request).encode()
        for _ in range(4):
            with socket.create_connection((host.hostname,host.port)) as client:
                client.sendall(f'POST /ai/v1/embed HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {len(payload)}\r\n\r\n'.encode()+payload)
                time.sleep(.001)
                client.setsockopt(socket.SOL_SOCKET,socket.SO_LINGER,struct.pack('ii',1,0))
        c.set_phase('model-limit-and-recovery')
        largest = next(case for case in cases if case['tokens']==8192)['request']
        status = call('embed', {**largest,'input':[largest['input'][0]+' detail']}, None)['status']
        assert status in (400,413), ('over-limit input was not rejected', status)
        call('embed', request)
        call('decide', q.Campaign.decide())
        deadline = time.monotonic()+30
        while True:
            live = metrics(args.url)
            if live['antfly_admission_inference_in_flight_requests']==0 and live['antfly_inference_generation_active_requests']==0 and live['antfly_inference_admission_backend_kv_bytes']==0:
                break
            assert time.monotonic()<deadline, 'request ownership did not recover'
            time.sleep(.05)
        status, inventory, _ = q.http(args.url, '/models/instances')
        c.instances_after = inventory['data']
        assert status == 200 and {item['id'] for item in c.instances_after} == {item['id'] for item in c.instances_before}
        verify_artifacts(result['identity'],args)
        c.require_memory_evidence()
        if args.control:
            gates = {}
            for level in args.levels:
                names = [f'documents-{level}-{i}' for i in range(args.rounds)]
                old_p95 = statistics.median(previous['phases'][name]['p95_ms'] for name in names)
                new_p95 = statistics.median(result['phases'][name]['p95_ms'] for name in names)
                old_wall = statistics.median(previous['phases'][name]['wall_seconds'] for name in names)
                new_wall = statistics.median(result['phases'][name]['wall_seconds'] for name in names)
                gates[str(level)] = {'control_p95_ms': old_p95, 'candidate_p95_ms': new_p95, 'throughput_ratio': old_wall/new_wall,
                                     'passed': new_p95 <= old_p95*1.25+10 and old_wall/new_wall>=.90}
            result['performance_gates'] = gates
            assert all(gate['passed'] for gate in gates.values()), ('document performance gate failed',gates)
        result['mixed'] = {kind: q.summary([row for row in rows if row['phase']=='mixed-models' and row['kind']==kind]) for kind in ('embed','decide','generate')}
        if args.control:
            for kind in ('embed','decide','generate'):
                old = previous['mixed'][kind]['p95_ms']
                new = result['mixed'][kind]['p95_ms']
                assert new <= old*(1.25 if kind=='generate' else 2)+50, ('mixed latency regression',kind,old,new)
        c.status = result['status'] = 'passed'
    except BaseException as error:
        c.status = result['status'] = 'incomplete' if isinstance(error,q.EvidenceUnavailable) else 'failed'
        c.error = result['error'] = str(error)
        raise
    finally:
        try:
            c.stop_monitor(monitor)
        finally:
            result['status'] = c.status
            result['memory_evidence'] = {'complete': c.status=='passed', 'sampling_errors': c.evidence_errors}
            (args.output/'requests.jsonl').write_text(''.join(json.dumps(row)+'\n' for row in rows))
            (args.output/'qualification.json').write_text(json.dumps(result,indent=2)+'\n')


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--url',default='http://127.0.0.1:18840')
    p.add_argument('--binary',type=Path,required=True)
    p.add_argument('--config',type=Path,required=True)
    p.add_argument('--cgroup',type=Path,required=True)
    p.add_argument('--output',type=Path,required=True)
    p.add_argument('--control',type=Path)
    p.add_argument('--rounds',type=int,default=3)
    p.add_argument('--levels',type=int,nargs='+',default=[128,512,1024,2048,4096,8192])
    args=p.parse_args()
    if args.rounds<1 or args.levels!=[128,512,1024,2048,4096,8192]:
        p.error('qualification requires positive rounds and full 128..8192 coverage')
    run(args)


if __name__=='__main__':
    main()
