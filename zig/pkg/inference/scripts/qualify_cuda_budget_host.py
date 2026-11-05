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

"""Require real allocation rejection, quarantine/reload and owned NVML evidence.

Uses the production engine-host test executable with its 64 MiB cap. This is a
scaled guard exercise on the local GPU, not 38 GiB exhaustion or A100 testing.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import threading
import time
import qualify_read_pool_40g as q


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--binary',type=Path,required=True)
    p.add_argument('--output',type=Path,required=True)
    a=p.parse_args(); a.output.mkdir(parents=True,exist_ok=True)
    try: gpu=q.Gpu()
    except Exception as error:
        (a.output/'report.json').write_text(json.dumps({'status':'incomplete','error':str(error)},indent=2)+'\n')
        raise q.EvidenceUnavailable(str(error)) from error
    child=subprocess.Popen([str(a.binary.resolve())],env={**os.environ,'ANTFLY_QUALIFY_CUDA_DEVICE_BUDGET':'1'},stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True)
    lock=threading.Lock(); current={}; events=[]; samples=[]; errors=[]
    phases={'guard-rejected','failed-free','quarantined','reloaded'}
    def read():
        with (a.output/'test.log').open('w') as out:
            for line in child.stdout:
                out.write(line); out.flush()
                if 'CUDA_PRESSURE ' in line:
                    values=dict(word.split('=',1) for word in line.split('CUDA_PRESSURE ',1)[1].split() if '=' in word)
                    values={k:int(v) if v.isdigit() else v for k,v in values.items()}
                    with lock:
                        current.clear(); current.update(values); events.append(dict(values,time=time.time()))
    reader=threading.Thread(target=read,daemon=True); reader.start()
    try:
        while child.poll() is None:
            with lock: state=dict(current)
            if state.get('phase') in phases:
                try:
                    raw=gpu.sample()
                    with lock: unchanged=current==state
                    if unchanged:
                        owned=[x for x in raw['nvml_compute_processes'] or [] if x['pid']==child.pid]
                        if not owned or any(not isinstance(x['bytes'],int) or x['bytes']<=0 for x in owned):
                            raise q.EvidenceUnavailable('no valid NVML process bytes for owned pressure test')
                        row={**state,'time':time.time(),'pid':child.pid,'service_nvml_bytes':sum(x['bytes'] for x in owned)}
                        assert 0 < row['managed_live'] <= row['managed_peak'] <= row['managed_limit'] == 64*(1<<20)
                        # At these stable barriers, the difference provides an
                        # observed bound for excluded driver/vendor allocation.
                        row['excluded_bytes_observed']=max(0,row['service_nvml_bytes']-row['managed_live'])
                        assert row['excluded_bytes_observed'] <= 2*(1<<30), '2 GiB excluded-allocation allowance exceeded'
                        samples.append(row)
                except Exception as error: errors.append({'phase':state['phase'],'error':str(error),'type':type(error).__name__})
            time.sleep(.1)
        reader.join(timeout=10)
        assert child.returncode==0, f'host test failed with exit {child.returncode}'
        assert not reader.is_alive(), 'test log reader did not finish'
        if errors: raise q.EvidenceUnavailable(f'invalid pressure telemetry: {errors}')
        for phase in phases:
            if sum(s['phase']==phase for s in samples)<2: raise q.EvidenceUnavailable(f'insufficient valid pressure samples: {phase}')
        by_phase={e['phase']:e for e in events}
        assert by_phase['guard-rejected']['managed_live']==48*(1<<20)
        assert by_phase['guard-rejected']['rejected_bytes']==32*(1<<20)
        assert by_phase['failed-free']['managed_live']==48*(1<<20)
        assert by_phase['quarantined']['managed_live']==48*(1<<20)
        assert by_phase['confirmed-free']['managed_live']==0
        assert by_phase['reloaded']['managed_live']==64*(1<<20)
        status,error='passed',None
    except BaseException as failure:
        status,error=('incomplete' if isinstance(failure,q.EvidenceUnavailable) else 'failed'),str(failure)
        raise
    finally:
        if child.poll() is None: child.terminate(); child.wait(timeout=10)
        reader.join(timeout=10)
        (a.output/'memory.jsonl').write_text(''.join(json.dumps(s)+'\n' for s in samples))
        report={'status':status,'error':error,'scope':'GB10 production engine-host path; scaled 64 MiB physical guard, failed free and retained primary lifetime and confirmed free/reload; no A100 or 38 GiB exhaustion claim','binary_sha256':hashlib.sha256(a.binary.read_bytes()).hexdigest(),'pid':child.pid,'exit_code':child.returncode,'headroom_bytes':2*(1<<30),'events':events,'sample_counts':{phase:sum(s['phase']==phase for s in samples) for phase in sorted(phases)},'sampling_errors':errors,'service_nvml_peak_bytes':max((s['service_nvml_bytes'] for s in samples),default=None),'excluded_bytes_observed_peak':max((s['excluded_bytes_observed'] for s in samples),default=None)}
        (a.output/'report.json').write_text(json.dumps(report,indent=2)+'\n')
        print(json.dumps(report,indent=2),flush=True)

if __name__=='__main__': main()
