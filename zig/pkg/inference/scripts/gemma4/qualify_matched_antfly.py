"""Matched Antfly full replay or portable restore on the production standalone host.

Use the same verified checkpoints/token fixtures as qualify_matched_llama.py.
Full prompt replay can be compared with llama replay. Portable checkpoint restore
and llama resident reuse are different operations and must not be called parity.
First content is an API timing observation, not an internal first-token/decode timer.
"""
import argparse
import copy
import http.client
import json
import math
from pathlib import Path
import sys
import time

ROOT=Path.cwd();BASE=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT/'zig/pkg/inference/scripts/gemma4'))
from native_checkpoint_transport import send_resume
from qualify_native_checkpoint_http import (canonical_choices,git_provenance,read_response,
    requests,response_headers,server,sha256_file)
from qualify_context_management import cuda_device_evidence
from qualify_matched_llama import percentiles,require_idle


from native_benchmark_fixture import qualified_fixture
from native_phase_timing import GENERATION_PHASES, generation_timing, request_log


class TimedStream:
    def __init__(self,response,started):
        self.response=response;self.started=started;self.first_content=None
    def __iter__(self):
        for raw in self.response:
            if self.first_content is None and raw.startswith(b'data: '):
                data=raw[6:].strip()
                if data!=b'[DONE]':
                    event=json.loads(data)
                    for choice in event.get('choices',[]):
                        content=choice.get('delta',{}).get('content')
                        if isinstance(content,str) and content:
                            self.first_content=time.monotonic()-self.started
                            break
            yield raw


def read_timed_response(response,started):
    stream=TimedStream(response,started)
    result=read_response(stream,True)
    total=time.monotonic()-started
    if stream.first_content is None or not 0<stream.first_content<=total:
        raise AssertionError('no valid first-content timing')
    return {'result':result,'total_seconds':total,'first_content_seconds':stream.first_content,
            'first_content_to_done_seconds':total-stream.first_content}


def reference_result(reference,kind,logical_tokens):
    found=[r['result'] for r in reference['exporter']['requests'] if r['kind']==kind]
    # The 8K roundtrip also measures a longer appended full-KV baseline.
    # Select by exact logical length, never take the first arbitrary baseline.
    if kind=='baseline':
        found=[r for r in found if r.get('usage',{}).get('prompt_tokens')==logical_tokens]
    if len(found)!=1:raise AssertionError(f'missing/ambiguous reference: {kind}')
    return found[0]


def validate_observation(row,expected,logical_tokens,mode):
    result=row['result'];usage=result['usage']
    if mode not in ('replay','portable_restore'):raise ValueError('unknown workload mode')
    if canonical_choices(result['choices'])!=canonical_choices(expected['choices']):
        raise AssertionError('deterministic output differs from reference')
    if usage!=expected['usage']:raise AssertionError('token accounting differs from reference')
    if usage['prompt_tokens']!=logical_tokens or usage['completion_tokens']<=0:
        raise AssertionError('input/output length mismatch')
    if usage.get('cached_prompt_tokens',0)!=(logical_tokens if mode=='portable_restore' else 0):
        raise AssertionError('prefix reuse differs from requested workload')
    if result['choices'][0]['finish_reason']!='stop':raise AssertionError('continuation did not reach EOS')
    first=row['first_content_seconds'];total=row['total_seconds']
    if not all(type(v) in (int,float) and math.isfinite(v) for v in (first,total)) or not 0<first<=total:
        raise AssertionError('invalid wall timings')


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--server',type=Path,required=True)
    parser.add_argument('--models-dir',type=Path,default=Path('.models'))
    parser.add_argument('--model',default='google/gemma-4-E4B-it-qat-q4_0-gguf')
    parser.add_argument('--reference',type=Path,required=True)
    parser.add_argument('--checkpoint',type=Path,required=True,help='checkpoint exported by the passing reference campaign')
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--size',choices=['8k','32k'],required=True)
    parser.add_argument('--mode',choices=['replay','portable_restore'],required=True)
    parser.add_argument('--warmups',type=int,default=5)
    parser.add_argument('--samples',type=int,default=100)
    parser.add_argument('--smoke',action='store_true')
    parser.add_argument('--port',type=int,default=8524)
    parser.add_argument('--timeout',type=float,default=1800)
    args=parser.parse_args();args.server_kind='standalone'
    args.generation_timing=True
    if args.warmups<0 or args.samples<1 or not math.isfinite(args.timeout) or args.timeout<=0:
        parser.error('invalid sampling counts or timeout')
    if not args.smoke and (args.warmups<5 or args.samples<100):
        parser.error('release sampling requires five warmups and 100 measured requests')
    if args.output.exists():raise FileExistsError(args.output)
    args.output.parent.mkdir(parents=True,exist_ok=True)
    evidence={'passed':False,'production_ready':False,'generation_latency_gate_qualified':False,
              'scope':__doc__,'mode':args.mode,'size':args.size,'smoke':args.smoke,
              'warmups_requested':args.warmups,'samples_requested':args.samples,'observations':[],
              'source':git_provenance(ROOT),'binary_sha256':sha256_file(args.server),
              'reference_sha256':sha256_file(args.reference),'runner_sha256':sha256_file(Path(__file__))}
    def publish():
        temporary=args.output.with_suffix('.json.part')
        temporary.write_text(json.dumps(evidence,indent=2)+'\n');temporary.replace(args.output)
    try:
        require_idle()
        reference,fixture=qualified_fixture(args.reference,args.checkpoint,args.size)
        fixture.pop('token_ids')
        pinned={args.server:evidence['binary_sha256'],args.reference:evidence['reference_sha256'],
                Path(__file__):evidence['runner_sha256'],
                args.checkpoint:fixture['checkpoint_sha256_to_reverify']}
        for helper in [BASE/'qualify_matched_llama.py',BASE/'native_benchmark_fixture.py',BASE/'native_phase_timing.py']:
            pinned[helper]=sha256_file(helper)
        model=args.models_dir/args.model/'gemma-4-E4B_q4_0-it.gguf'
        pinned[model]='676c35070db6dbe52f93e9c864ee0fba4eddea94b9c875d9cb10daff453fbaee'
        for path,digest in pinned.items():
            if sha256_file(path)!=digest:raise AssertionError(f'input artifact changed: {path}')
        if reference['binary_sha256']!=evidence['binary_sha256'] or reference['source']!=evidence['source']:
            raise AssertionError('reference must qualify this exact source and binary')
        def prior(kind):return reference_result(reference,kind,fixture['logical_tokens'])
        if prior('prepare')['file_sha256']!=fixture['checkpoint_sha256_to_reverify']:
            raise AssertionError('reference exported different token/identity state')
        expected=prior('baseline' if args.mode=='replay' else 'same_process')
        body=copy.deepcopy(next(iter(requests(args.model,'f16',[742 if args.size=='8k' else 2975],True,'fact'))))
        body['prompt_cache']=False
        if args.mode=='portable_restore':body.pop('context_management');body.pop('cache_dtype')
        hardware=cuda_device_evidence()
        if len(hardware)!=1 or hardware[0]['name']!='NVIDIA L4':raise AssertionError('expected the designated L4')
        evidence.update(hardware=hardware,fixture=fixture,request=body,
                        input_sha256={str(p):d for p,d in pinned.items()},
                        timing_limits='First content and first-content-to-DONE are API wall observations. Pipeline phase milliseconds come from the existing generation timer; they include callback work and are not GPU-event timings. Portable-restore prefill includes restore/suffix execution.')
        require_idle();publish()
        with server(args,'worker',evidence) as record:
            for index in range(1+args.warmups+args.samples):
                row={'index':index,'cold':index==0,'warmup':0<index<=args.warmups}
                evidence['observations'].append(row);publish()
                log_path=args.output.with_suffix('.worker.log')
                log_offset=log_path.stat().st_size
                connection=http.client.HTTPConnection('127.0.0.1',args.port,timeout=args.timeout)
                try:
                    started=time.monotonic()
                    if args.mode=='portable_restore':
                        response=send_resume(connection,'/ai/v1/chat/context/resume',body,args.checkpoint)
                    else:
                        connection.request('POST','/ai/v1/generate',json.dumps(body).encode(),{'Content-Type':'application/json'})
                        response=connection.getresponse()
                    if response.status!=200:raise AssertionError(f'HTTP {response.status}: {response.read(65536)!r}')
                    response_headers(response,record)
                    if not response.getheader('Content-Type','').startswith('text/event-stream'):
                        raise AssertionError('missing SSE response')
                    row.update(read_timed_response(response,started))
                finally:connection.close()
                validate_observation(row,expected,fixture['logical_tokens'],args.mode)
                row['pipeline_timing_ms']=generation_timing(request_log(log_path,log_offset),row['total_seconds'])
                row['passed']=True;publish()
                print(json.dumps({'index':index,'mode':args.mode,'seconds':row['total_seconds']}),flush=True)
        memory=evidence['worker']['memory']
        if memory['samples']<2 or memory['errors'] or memory['gpu_uuids']!=[hardware[0]['uuid']]:
            raise AssertionError('incomplete/misattributed memory sampling')
        if not 0<memory['peak_host_rss_bytes']<=8*1024**3 or not 0<memory['peak_gpu_memory_bytes']<=18*1024**3:
            raise AssertionError('memory cap exceeded')
        if git_provenance(ROOT)!=evidence['source']:raise AssertionError('source changed during campaign')
        for path,digest in pinned.items():
            if sha256_file(path)!=digest:raise AssertionError(f'evidence artifact changed: {path}')
        measured=[r for r in evidence['observations'] if not r['cold'] and not r['warmup']]
        evidence['latency_seconds']={key:percentiles([r[key] for r in measured],allow_zero=key=='first_content_to_done_seconds')
            for key in ('total_seconds','first_content_seconds','first_content_to_done_seconds')}
        evidence['pipeline_timing_ms']={key:percentiles([r['pipeline_timing_ms'][key] for r in measured],allow_zero=True)
            for key in GENERATION_PHASES+('total',)}
        evidence.update(passed=True,sampling_qualified=not args.smoke and args.warmups>=5 and len(measured)>=100)
    except Exception as error:evidence.update(passed=False,sampling_qualified=False,error=f'{type(error).__name__}: {error}')
    finally:publish()
    print(json.dumps({'passed':evidence['passed'],'sampling_qualified':evidence.get('sampling_qualified',False),'error':evidence.get('error')}),flush=True)
    return 0 if evidence['passed'] else 1


if __name__=='__main__':raise SystemExit(main())
