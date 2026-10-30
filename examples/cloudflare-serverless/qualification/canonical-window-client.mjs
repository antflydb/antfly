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

import {readFile,writeFile,mkdir,stat} from 'node:fs/promises';
import {resolve} from 'node:path';
import {createHash} from 'node:crypto';
import {minuteOf,windowPrefix,proofPrefix,canonicalLayout} from './canonical-windows.mjs';
const sha=v=>createHash('sha256').update(v).digest('hex');

// Align logical cuts with physical UTC partitions so a minute is fetched once.
// Legacy catalogs remain readable through their explicit/default elapsed layout.
export function partitionCanonicalTimeframe(timeframe,partition='utc-minute'){
 if(!Number.isSafeInteger(timeframe?.from)||!Number.isSafeInteger(timeframe?.to)||timeframe.from<0||timeframe.from>=timeframe.to||timeframe.to-timeframe.from>3600000||!['utc-minute','elapsed-minute'].includes(partition))throw Error('ObserverWindowScopeInvalid');
 const windows=[];for(let from=timeframe.from;from<=timeframe.to;){const to=Math.min(timeframe.to,(partition==='utc-minute'?minuteOf(from):from)+59999);windows.push({from,to});from=to+1;}return windows;
}

// Store each bounded observation separately. The catalog never accumulates a
// source's terminal history or verbose archive in memory.
export async function collectCanonicalWindows(observer,scope,{witnesses},call) {
 if(!observer.dir||!witnesses?.before||!witnesses?.after)throw Error('ObserverWindowScopeInvalid');
 const base='/source/'+scope.source,version=observer.reader_version;
 const scoped=p=>{if(p.qualification!==false||p.capture_id!==scope.capture_id||p.collector_version!==version||p.evidence_layout!==canonicalLayout)throw Error('ObserverSnapshotScopeInvalid');return p;};
 const started=Date.now();scoped(await call(observer,base+'/layout'));
 const boundary=[];
 for(const hash of[witnesses.before,witnesses.after]){
  const p=scoped(await call(observer,base+'/witnesses',{hash})),body=await call(observer,base+'/objects',{objects:[p.object]});
  if(body.capture_id!==scope.capture_id||body.collector_version!==version||body.objects?.length!==1||body.objects[0].key!==p.pointer.counter_key)throw Error('ObserverWitnessInvalid');
  const object=body.objects[0],matches=object.value?.records?.filter(r=>r.attempt_id_sha256===hash);
  if(matches?.length!==1||matches[0].timestamp!==p.pointer.timestamp)throw Error('ObserverWitnessInvalid');
  boundary.push({object,timestamp:p.pointer.timestamp});
 }
 const timeframe={from:boundary[0].timestamp+1,to:boundary[1].timestamp-1};
 if(timeframe.to<=timeframe.from||timeframe.to-timeframe.from>3600000)throw Error('ObserverWindowScopeInvalid');
 const dir=resolve(observer.dir,'windows-'+scope.source+'-'+crypto.randomUUID());await mkdir(dir,{recursive:true,mode:0o700});
 const windows=[];
 for(const{from,to}of partitionCanonicalTimeframe(timeframe)){
  const start=Date.now(),objects=new Map();let bytes=0,pages=0;
  const add=o=>{if(objects.has(o.key)){if(JSON.stringify(objects.get(o.key))!==JSON.stringify(o))throw Error('ObserverObjectChanged');return;}bytes+=Buffer.byteLength(JSON.stringify(o));if(objects.size>=20000||bytes>64*1024*1024)throw Error('ObserverSnapshotBudgetExceeded');objects.set(o.key,o);};
  const drain=async(mode,input,prefix)=>{let cursor=null;const cursors=new Set();do{if(++pages>2000||Date.now()-start>120000)throw Error('ObserverSnapshotBudgetExceeded');const p=scoped(await call(observer,base+'/'+mode,{...input,...(cursor?{cursor}:{})}));if(!Array.isArray(p.objects)||p.objects.length>32)throw Error('ObserverSnapshotScopeInvalid');for(const o of p.objects){if(typeof o.key!=='string'||!o.key.startsWith(prefix)||objects.has(o.key))throw Error('ObserverSnapshotObjectInvalid');add(o);}cursor=p.cursor;if(cursor!==null){if(typeof cursor!=='string'||!cursor||cursors.has(cursor))throw Error('ObserverCursorInvalid');cursors.add(cursor);}}while(cursor!==null);};
  for(let minute=minuteOf(from);minute<=minuteOf(to);minute+=60000)await drain('window',{minute},windowPrefix(scope.capture_id,minute));
  await drain('rejections',{},proofPrefix(scope.capture_id)+'rejections/');
  for(const b of boundary)add(b.object);
  const snapshot={qualification:false,evidence_layout:canonicalLayout,capture_id:scope.capture_id,collector_version:version,timeframe:{from,to},snapshot_started_at:start,snapshot_finished_at:Date.now(),pages,bytes,objects:[...objects.values()].sort((a,b)=>a.key.localeCompare(b.key))};
  const body=JSON.stringify(snapshot),path=resolve(dir,String(from)+'.json');await writeFile(path,body,{mode:0o600,flag:'wx'});windows.push({timeframe:{from,to},path,sha256:sha(body),bytes:Buffer.byteLength(body)});
 }
 return {schema:1,qualification:false,evidence_layout:canonicalLayout,window_partition:'utc-minute',capture_id:scope.capture_id,collector_version:version,timeframe,snapshot_started_at:started,snapshot_finished_at:Date.now(),windows};
}
export async function readCanonicalSnapshot(entry) {
 if(!entry||typeof entry.path!=='string'||entry.bytes>64*1024*1024+1024*1024)throw Error('ObserverSnapshotBudgetExceeded');
 const meta=await stat(entry.path);if(meta.size!==entry.bytes)throw Error('ObserverSnapshotChanged');
 const body=await readFile(entry.path);if(body.length!==entry.bytes||sha(body)!==entry.sha256)throw Error('ObserverSnapshotChanged');const s=JSON.parse(body);
 if(s.evidence_layout!==canonicalLayout||JSON.stringify(s.timeframe)!==JSON.stringify(entry.timeframe)||!Array.isArray(s.objects)||s.objects.length>20000||s.bytes>64*1024*1024||s.objects.reduce((n,o)=>n+Buffer.byteLength(JSON.stringify(o)),0)!==s.bytes)throw Error('ObserverSnapshotScopeInvalid');return s;
}
