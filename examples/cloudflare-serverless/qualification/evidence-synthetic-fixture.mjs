// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Explicitly synthetic, known-complete populations. Never repair or overwrite
// a historical capture to make it pass. Feed these into the same full reader
// rehearsal and unchanged strict reconciler used for preserved real captures.
import{mkdir,writeFile}from'node:fs/promises';import{resolve}from'node:path';import{createHash}from'node:crypto';import{minuteOf,windowPrefix,canonicalLayout}from'./canonical-windows.mjs';
const arg=(k,f)=>process.argv.find(a=>a.startsWith('--'+k+'='))?.slice(k.length+3)??f,out=resolve(arg('out','dist/qualification/evidence-synthetic-'+Date.now())),count=Number(arg('terminals','2048'));if(!Number.isInteger(count)||count<128||count>4096)throw Error('SyntheticSizeInvalid');await mkdir(out,{recursive:true});
const sha=v=>createHash('sha256').update(v).digest('hex'),now=Date.now(),base=minuteOf(now-32*60000),before=base+1000,after=base+27*60000+59000,counterVersion=crypto.randomUUID(),collectorVersion=crypto.randomUUID(),observer={id:'antfly-1278-observe-synthetic',observation_capture_id:crypto.randomUUID(),scopes:[]},report={qualification:false,fixture_kind:'synthetic-known-complete',sides:[],captures:[],witnesses:{before:{},after:{}}};
for(const side of['b','c']){const run='antfly-1278-g-syntheticq-1-'+side,versions=Object.fromEntries(['app','writer','reader','builder'].map(role=>[role,crypto.randomUUID()])),ledger={schema:1,overflow:false,attempts:[]};report.sides.push({run,source_versions:versions,invocation_ledger:ledger});
 for(const role of['writer','reader','builder','app']){
  const source=run+(role==='app'?'':'-'+role),captureId=crypto.randomUUID(),scope={source,capture_id:captureId,run,role,binding:'PRODUCER_'+observer.scopes.length};observer.scopes.push(scope);const objects=new Map();
  const counter=(timestamp,attempt)=>{const event={outcome:'ok',cpuTime:1,wallTime:5},id=crypto.randomUUID(),minute=minuteOf(timestamp),key=windowPrefix(captureId,timestamp)+'counter/'+id+'.json';objects.set(key,{schema:2,qualification:false,evidence_layout:canonicalLayout,source:'provider-tail-dispatch',producer:source,collector_version:counterVersion,received_at:now-1,delivery_id:id,dispatch_id:crypto.randomUUID(),archive_sha256:sha(key),minute,count:1,records:[{script:source,version_id:versions[role],timestamp,...attempt?{attempt_id_sha256:attempt}:{},event}]});return key;};
  const boundary=[];for(const[name,timestamp]of[['before',before],['after',after]]){const id=sha(crypto.randomUUID());report.witnesses[name][source]={attempt_id_sha256:id};boundary.push(counter(timestamp,id));}
  for(let i=0;i<count;i++){
   const start=before+500+Math.floor(i*(after-before-1000)/count),id=crypto.randomUUID(),attempt=role==='app'?sha(id):undefined,event={outcome:'ok',cpuTime:1,wallTime:5};counter(start,attempt);
   const raw={signal:'traces',script:source,version_id:versions[role],start,end:start+5,handler:role==='app'?'fetch':'rpc',outcome:'ok',terminal:true,invocation_id_sha256:sha(source+'/invocation/'+i),trace_id_sha256:sha(source+'/trace/'+i),span_id_sha256:sha(source+'/span/'+i),...attempt?{attempt_id_sha256:attempt}:{},event},row={...raw,source_row_sha256:sha(JSON.stringify(raw))},key=windowPrefix(captureId,start)+'batches/'+sha(source+'/'+i)+'.json';objects.set(key,{schema:2,qualification:false,evidence_layout:canonicalLayout,signal:'traces',collector_version:counterVersion,received_at:now-1,raw_batch_sha256:sha(key+'/raw'),archive_sha256:sha(key+'/archive'),minute:minuteOf(start),rows:[row]});
   if(role==='app')ledger.attempts.push({id,phase:'read',op:'lookup',tenant:'a',started_at:start,response_at:start+4,finished_at:start+5,status:200,version_id:versions[role],transport_error:false});
  }
  const dir=resolve(out,source);await mkdir(dir);const catalog={schema:1,qualification:false,evidence_layout:canonicalLayout,window_partition:'utc-minute',capture_id:captureId,collector_version:collectorVersion,timeframe:{from:before+1,to:after-1},snapshot_started_at:now,snapshot_finished_at:now,windows:[]};
  for(let minute=minuteOf(before);minute<=minuteOf(after);minute+=60000){const timeframe={from:Math.max(before+1,minute),to:Math.min(after-1,minute+59999)},selected=new Map([...objects].filter(([key])=>key.startsWith(windowPrefix(captureId,minute))));for(const key of boundary)selected.set(key,objects.get(key));const values=[...selected].map(([key,value])=>({key,value})).sort((a,b)=>a.key.localeCompare(b.key)),snapshot={qualification:false,evidence_layout:canonicalLayout,capture_id:captureId,collector_version:collectorVersion,timeframe,snapshot_started_at:now,snapshot_finished_at:now,pages:1,bytes:values.reduce((n,v)=>n+Buffer.byteLength(JSON.stringify(v)),0),objects:values},body=JSON.stringify(snapshot),path=resolve(dir,String(minute)+'.json');await writeFile(path,body);catalog.windows.push({timeframe,path,sha256:sha(body),bytes:Buffer.byteLength(body)});}
  const body=JSON.stringify(catalog),path=resolve(out,source+'.json');await writeFile(path,body);report.captures.push({source,pass:0,path,sha256:sha(body),bytes:Buffer.byteLength(body)});console.log(JSON.stringify({source,terminals:count,objects:objects.size,windows:catalog.windows.length}));
 }
}
await writeFile(resolve(out,'report.json'),JSON.stringify(report,null,2));await writeFile(resolve(out,'observer.json'),JSON.stringify(observer,null,2));await writeFile(resolve(out,'postmortem.json'),JSON.stringify({captures:[]}));await writeFile(resolve(out,'fixture.json'),JSON.stringify({qualification:false,fixture_kind:'synthetic-known-complete',terminals_per_source:count,sources:8,windows_per_source:28,original_hosted_reports_changed:false},null,2));console.log(JSON.stringify({directory:out,qualification:false}));
