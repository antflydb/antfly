"""Native checkpoint TCP-abort/recovery diagnostics, staged for real-model use.

This covers partial uploads, prefill, export and fitting. A calibrated timer
only aims the fitting abort; fresh server error/phase/chunk evidence must prove
the cancellation actually interrupted fitting. This does not qualify sustained
reliability, physical OOM, SSE aborts or live replacement safety.
"""
import argparse
import copy
import http.client
import json
from pathlib import Path
import re
import socket
import struct
import sys
import time

sys.path.insert(0,str(Path.cwd()/'zig/pkg/inference/scripts/gemma4'))
from native_checkpoint_transport import CONTENT_TYPE, envelope_prefix
from qualify_native_checkpoint_http import (REPO, canonical_choices, git_provenance,
    requests, response_headers, resume, server, sha256_file)


class AbortableRequest:
    """Retain a duplicate descriptor so response-owned sockets can be reset."""
    def __init__(self, host, port, timeout):
        self.connection=http.client.HTTPConnection(host,port,timeout=timeout)
        self.response=None
        self.transport=None
        try:
            self.connection.connect()
            self.transport=self.connection.sock.dup()
        except BaseException:
            self.close();raise
    def getresponse(self):
        self.response=self.connection.getresponse()
        return self.response
    def abort(self):
        if self.transport is None: raise RuntimeError('request already closed')
        self.transport.setsockopt(socket.SOL_SOCKET,socket.SO_LINGER,struct.pack('ii',1,0))
        self.close()
    def close(self):
        try:
            if self.response is not None: self.response.close();self.response=None
        finally:
            self.connection.close()
            if self.transport is not None: self.transport.close();self.transport=None


def probe(args):
    connection=http.client.HTTPConnection('127.0.0.1',args.port,timeout=2)
    try:
        connection.request('POST','/ai/v1/chat/context/resume',b'{}',{'Content-Type':'application/json'})
        response=connection.getresponse();payload=json.loads(response.read(65536))
        if response.status==400:
            if payload!={'error':'INVALID_KV_CHECKPOINT','message':'InvalidNativeKvFrame'}:
                raise AssertionError(f'unexpected released-guard response: {payload}')
        elif response.status==503:
            if payload.get('error')!='MODEL_RESOURCE_BUSY': raise AssertionError(payload)
        else: raise AssertionError(f'guard probe HTTP {response.status}: {payload}')
        return {'status':response.status,'body':payload}
    finally: connection.close()


def observe_guard(args,case,expected,timeout):
    started=time.monotonic();deadline=started+timeout
    observations=case.setdefault('guard_observations',[])
    while True:
        row=probe(args);row['elapsed_seconds']=time.monotonic()-started
        row['waiting_for_status']=expected;observations.append(row)
        if row['elapsed_seconds']>timeout: raise TimeoutError(f'guard observation exceeded {timeout}s')
        if row['status']==expected: return row['elapsed_seconds']
        if time.monotonic()>=deadline: raise TimeoutError(f'guard did not reach HTTP {expected}')
        time.sleep(.01)


def diagnostic_messages(data):
    """Read structured or Zig stderr records, never bare/embedded text."""
    messages=[]
    for line in data.splitlines():
        try: text=line.decode('utf-8').rstrip('\r\n')
        except UnicodeError: continue
        try:
            record=json.loads(text)
        except ValueError:
            match=re.fullmatch(r'(?:debug|info|warn|warning|err|error)(?:\([^\r\n()]+\))?: (.*)',text)
            if match: messages.append(match[1])
        else:
            if isinstance(record,dict) and isinstance(record.get('msg'),str):
                messages.append(record['msg'])
    return messages


def wait_prefill_route(log,offset,timeout):
    deadline=time.monotonic()+timeout
    while time.monotonic()<deadline:
        with log.open('rb') as source:
            if source.seek(0,2)<offset: raise RuntimeError('worker log truncated during request')
            source.seek(offset);data=source.read(1024*1024)
        if len(data)==1024*1024: raise RuntimeError('unexpected excessive prefill diagnostics')
        for message in diagnostic_messages(data):
            if message.startswith('cuda_gqa_prefill_profile: status=active '): return message
        time.sleep(.01)
    raise TimeoutError('no fresh active CUDA prefill route marker')


def fitting_cancellation_evidence(data):
    messages=diagnostic_messages(data)
    errors=[message for message in messages if message.startswith('attention_match_fit_error ')]
    phases=[message for message in messages if message.startswith('attention_match_fit_rejected ')]
    if len(errors)!=1 or len(phases)!=1:
        raise AssertionError('missing/ambiguous fresh fitting error and phase evidence')
    if re.fullmatch(r'attention_match_fit_error error=(Cancelled|Canceled|CompactionCancelled)',errors[0]) is None:
        raise AssertionError('fitting failed for a reason other than cancellation')
    fields=dict(field.split('=',1) for field in phases[0].split()[1:] if '=' in field)
    if fields.get('phase')!='fit': raise AssertionError('cancellation did not interrupt fitting')
    try:
        attempted=int(fields['attempted_chunks']);completed=int(fields['completed_chunks'])
        elapsed=int(fields['elapsed_ns'])
    except (ValueError,KeyError) as error:
        raise AssertionError('missing fitting chunk/timing evidence') from error
    if attempted<=0 or not 0<=completed<=attempted or elapsed<=0:
        raise AssertionError('fitting chunks did not start before cancellation')
    return {'error_event':errors[0],'phase_event':phases[0],
            'attempted_chunks':attempted,'completed_chunks':completed,'elapsed_ns':elapsed}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--server',type=Path,required=True)
    parser.add_argument('--server-kind',choices=['inference','standalone'],default='standalone')
    parser.add_argument('--models-dir',type=Path,default=Path('.models'))
    parser.add_argument('--model',default='google/gemma-4-E4B-it-qat-q4_0-gguf')
    parser.add_argument('--reference',type=Path,required=True)
    parser.add_argument('--checkpoint',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--padding-lines',type=int,default=742)
    parser.add_argument('--port',type=int,default=8514)
    parser.add_argument('--timeout',type=float,default=1800)
    parser.add_argument('--recovery-limit-seconds',type=float,default=5)
    parser.add_argument('--experimental-rotated4',action='store_true')
    args=parser.parse_args();args.fixture='fact'
    if not 0<args.recovery_limit_seconds<=5: parser.error('recovery bound must be positive and at most 5s')
    if args.output.exists(): raise FileExistsError(args.output)
    args.output.parent.mkdir(parents=True,exist_ok=True)
    evidence={'passed':False,'production_ready':False,'fitting_cancellation_qualified':False,
              'live_replacement_cancellation_qualified':False,'server_kind':args.server_kind,
              'scope':'Actual TCP reset during partial native upload, prefill, export, and evidenced fitting; same-worker recovery',
              'source':git_provenance(REPO),'binary_sha256':sha256_file(args.server),
              'checkpoint_sha256':sha256_file(args.checkpoint),'reference_sha256':sha256_file(args.reference),
              'runner_sha256':sha256_file(Path(__file__)),'cases':[]}
    def publish():
        temporary=args.output.with_suffix('.json.part');temporary.write_text(json.dumps(evidence,indent=2)+'\n')
        temporary.replace(args.output)
    try:
        reference=json.loads(args.reference.read_text())
        if not reference.get('passed'): raise AssertionError('reference did not pass')
        exports=[r['result'] for r in reference['exporter']['requests'] if r['kind']=='prepare']
        if len(exports)!=1: raise AssertionError('ambiguous checkpoint reference')
        prepared=exports[0]
        encoding=prepared['metadata'].get('encoding','f32')
        if encoding not in ('f32','f16','fp8','rotated4'): raise AssertionError('unsupported reference encoding')
        if encoding=='rotated4':
            from qualify_attention_match_queries import validate_quantization_evidence
            validate_quantization_evidence([prepared['metadata']], 'rotated4')
        evidence['encoding']=encoding
        if prepared['file_sha256']!=evidence['checkpoint_sha256']: raise AssertionError('checkpoint changed')
        body=copy.deepcopy(next(iter(requests(args.model,'f16',[args.padding_lines],False,'fact'))))
        body.pop('context_management');body.pop('cache_dtype')
        with server(args,'worker',evidence) as record:
            baseline=resume(args,body,record,prepared,'before_cancellation')
            log=args.output.with_suffix('.worker.log')
            export_ready_seconds=None
            for kind in ['partial_upload','prefill','export','fitting']:
                case={'kind':kind,'passed':False};evidence['cases'].append(case);publish()
                request=AbortableRequest('127.0.0.1',args.port,args.timeout)
                try:
                    connection=request.connection
                    if kind=='partial_upload':
                        size=args.checkpoint.stat().st_size;prefix=envelope_prefix(body,size)
                        connection.putrequest('POST','/ai/v1/chat/context/resume')
                        connection.putheader('Content-Type',CONTENT_TYPE)
                        connection.putheader('Content-Length',str(len(prefix)+size));connection.endheaders()
                        connection.send(prefix)
                        with args.checkpoint.open('rb') as source: part=source.read(64*1024)
                        if len(part)>=size: raise AssertionError('fixture too small for a partial upload')
                        connection.send(part);case['checkpoint_bytes_sent']=len(part)
                    else:
                        offset=log.stat().st_size
                        case['request_log_offset']=offset
                        publish()
                        request_started=time.monotonic()
                        connection.request('POST','/ai/v1/chat/context/checkpoint',json.dumps(body).encode(),
                                           {'Content-Type':'application/json','X-Antfly-KV-Encoding':encoding})
                        if kind=='prefill':
                            case['active_route_marker']=wait_prefill_route(log,offset,30)
                        elif kind=='fitting':
                            if export_ready_seconds is None: raise AssertionError('missing warm export calibration')
                            # This aims the reset near the end of warm preparation.
                            # Only the fresh cancellation+fit+chunk diagnostics
                            # below qualify its phase, never elapsed time alone.
                            delay=export_ready_seconds*.9
                            if not 0<delay<120: raise AssertionError('invalid fitting-abort calibration')
                            case['calibrated_delay_seconds']=delay
                            observe_guard(args,case,503,2)
                            remaining=request_started+delay-time.monotonic()
                            if remaining>0: time.sleep(remaining)
                        else:
                            response=request.getresponse()
                            if response.status!=200: raise AssertionError(f'export HTTP {response.status}: {response.read(65536)!r}')
                            export_ready_seconds=time.monotonic()-request_started
                            case['export_ready_seconds']=export_ready_seconds
                            response_headers(response,record)
                            part=response.read(64*1024)
                            if not part.startswith(b'AFATT001') or len(part)!=64*1024:
                                raise AssertionError('export did not reach actual checkpoint transfer')
                            case['export_bytes_received']=len(part)
                    # Prove the model request owns the native guard before reset.
                    observe_guard(args,case,503,2)
                    reset_started=time.monotonic()
                    request.abort();case['tcp_reset_sent']=True
                    case['guard_recovery_seconds']=observe_guard(args,case,400,args.recovery_limit_seconds)
                    case['reset_to_released_seconds']=time.monotonic()-reset_started
                    if case['reset_to_released_seconds']>args.recovery_limit_seconds:
                        raise AssertionError('reset-to-release deadline exceeded')
                    if kind=='fitting':
                        with log.open('rb') as source:
                            source.seek(offset);fresh=source.read(1024*1024)
                        if len(fresh)==1024*1024: raise AssertionError('excessive fitting diagnostics')
                        case['fitting_evidence']=fitting_cancellation_evidence(fresh)
                finally: request.close()
                # This valid request gets no retries. Header attestation requires
                # the original worker PID; correct output alone is insufficient.
                recovered=resume(args,body,record,prepared,'recovery_'+kind)
                if canonical_choices(recovered['choices'])!=canonical_choices(baseline['choices']) or recovered['usage']!=baseline['usage']:
                    raise AssertionError('recovery changed the saved-prefix continuation')
                case['passed']=True;publish();print(json.dumps(case),flush=True)
                if kind=='fitting': evidence['fitting_cancellation_qualified']=True
        memory=evidence['worker']['memory']
        if memory['samples']<2 or memory['errors'] or not memory['gpu_uuids']:
            raise AssertionError('incomplete memory evidence')
        if not 0<memory['peak_host_rss_bytes']<=8*1024**3 or not 0<memory['peak_gpu_memory_bytes']<=18*1024**3:
            raise AssertionError('memory cap exceeded')
        if git_provenance(REPO)!=evidence['source']: raise AssertionError('source changed')
        for path,key in [(args.server,'binary_sha256'),(args.checkpoint,'checkpoint_sha256'),
                         (args.reference,'reference_sha256'),(Path(__file__),'runner_sha256')]:
            if sha256_file(path)!=evidence[key]: raise AssertionError(f'evidence changed: {path}')
        evidence['passed']=True
    except Exception as error:
        evidence['error']=f'{type(error).__name__}: {error}'
    finally: publish()
    print(json.dumps({'passed':evidence['passed'],'error':evidence.get('error')}),flush=True)
    return 0 if evidence['passed'] else 1


if __name__=='__main__': raise SystemExit(main())
