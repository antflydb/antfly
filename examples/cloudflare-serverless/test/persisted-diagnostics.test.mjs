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

import {test} from 'node:test';
import assert from 'node:assert/strict';
import {resolve} from 'node:path';
import {collectPersistedWindow, persistedWindowQuery, sanitizePersistedEvent} from '../qualification/persisted-diagnostics.mjs';

const id='antfly-1278-g-test-1-c',dir=resolve('dist',id),run={id,dir,worker:id,bucket:id,account:'a'.repeat(32),queues:[id,id+'-dead'],token_file:resolve(dir,'api-token'),services:['app','writer','reader','builder'].map(role=>({role,name:id+(role==='app'?'':'-'+role)}))};
const token='test-disposable-token-12345',time={from:1000,to:2000};
const row=(id,extra={})=>({$metadata:{id,service:run.id,type:'cf-worker-event',requestId:'request-'+id},$workers:{scriptName:run.id,outcome:'ok'},timestamp:1500,source:{url:'https://secret.invalid',authorization:'Bearer secret-must-not-persist'},...extra});
function fixture(rows,{changeCount=false,sampleInterval=1,abr=1,pages,transform=x=>x}={}){
 let counts=0,reads=0;const requests=[];
 return{requests,fetch:async(url,options)=>{
  const query=JSON.parse(options.body);requests.push(query);assert.equal(options.headers.Authorization,'Bearer '+token);assert.equal(query.dry,true);assert.equal(query.parameters.view,undefined);assert.ok(['events','calculations'].includes(query.view));
  const n=rows.length+(changeCount&&counts>0?1:0);
  const result={run:{status:'COMPLETED'},statistics:{abr_level:abr},...(query.view==='calculations'?{calculations:[{alias:'events',aggregates:[{count:n,value:n,sampleInterval}]}]}:{events:{count:rows.length,events:pages?pages[reads++]??[]:rows}})};
  if(query.view==='calculations')counts++;
  return Response.json({success:true,result:transform(result)});
 }};
}
test('persisted windows paginate exact scoped counts and retain only sanitized events',async()=>{
 const rows=[row('first'),row('second')],api=fixture(rows,{pages:[[rows[0]],[rows[1]]]}),report=await collectPersistedWindow(run,'app',token,time,api);
 assert.equal(report.qualification,false);assert.equal(report.reconciled,true);assert.equal(report.events,2);assert.equal(report.invocations,2);assert.equal(report.resource_failures,0);assert.equal(report.queries,4);assert.match(report.rows_sha256,/^[a-f0-9]{64}$/);
 assert.equal(api.requests[2].offset,'first');assert.equal(api.requests[2].offsetDirection,'next');assert.equal(api.requests[0].parameters.filters[0].value,run.id);
 const saved=JSON.stringify(report);for(const forbidden of[token,'secret.invalid','secret-must-not-persist','Bearer','request-first'])assert.ok(!saved.includes(forbidden));
 const privateQuery=persistedWindowQuery(run,'writer',time);assert.equal(privateQuery.parameters.filters[0].value,run.id+'-writer');
 assert.throws(()=>persistedWindowQuery(run,'unknown',time),/View/);assert.throws(()=>persistedWindowQuery({...run,id:'production'},'app',time),/Scope/);assert.throws(()=>persistedWindowQuery(run,'app',{from:0,to:60001}),/Window/);
});
test('persisted diagnostics reject sampling, changing counts, repeated cursors and incomplete pages',async()=>{
 const rows=[row('first'),row('second')];
 for(const[options,pattern]of[[{sampleInterval:2},/IngestionSampled/],[{abr:2},/QuerySampled/],[{changeCount:true},/CountChanged/],[{pages:[[rows[0]],[rows[0]]]},/CursorRepeated/],[{pages:[[rows[0]],[]]},/PaginationIncomplete/],[{transform:result=>({...result,run:{status:'STARTED'}})},/QueryIncomplete/]])await assert.rejects(collectPersistedWindow(run,'app',token,time,fixture(rows,options)),pattern);
 const duplicate=row('second');duplicate.$metadata.requestId='request-first';await assert.rejects(collectPersistedWindow(run,'app',token,time,fixture([rows[0],duplicate])),/InvocationRepeated/);
 await assert.rejects(collectPersistedWindow(run,'app',token,time,fixture(rows,{transform:result=>result.events?{...result,events:undefined,calculations:[]}:result})),/EventsSchemaUnknown/);
 const empty=await collectPersistedWindow(run,'app',token,time,fixture([],{sampleInterval:0}));assert.equal(empty.events,0);assert.equal(empty.qualification,false);assert.equal(empty.reconciled,true);
});
test('persisted event identities retain colliding console IDs from separate invocations without dropping errors',async()=>{
 const a=row('shared',{$metadata:{id:'shared',requestId:'invocation-a',service:run.id,type:'cf-worker'},source:{event:'antfly.operation',op:'lookup',elapsed_ms:1}}),b=row('shared',{$metadata:{id:'shared',requestId:'invocation-b',service:run.id,type:'cf-worker'},source:{event:'antfly.resource_failure',failure_code:'MemoryLimitExceeded'}});
 const result=await collectPersistedWindow(run,'app',token,time,fixture([a,b]));assert.equal(result.schema,3);assert.equal(result.events,2);assert.notEqual(result.rows[0].event_id_sha256,result.rows[1].event_id_sha256);assert.ok(result.resource_failures>0);
 await assert.rejects(collectPersistedWindow(run,'app',token,time,fixture([a,{...b,$metadata:{...b.$metadata,requestId:'invocation-a'}}])),/CursorRepeated/);
 await assert.rejects(collectPersistedWindow(run,'app',token,time,fixture([a,b],{pages:[[a],[a]]})),/CursorRepeated/);
});

test('persisted event parsing rejects wrong scope and unknown outcomes and retains late resource failures',async()=>{
 for(const value of[row('wrong',{$metadata:{id:'wrong',service:'production'}}),row('late',{timestamp:2001}),row('unknown',{$workers:{outcome:'new-platform-outcome'}})])assert.throws(()=>sanitizePersistedEvent(value,run.id,time));
 const log=row('late-resource',{$metadata:{id:'late-resource',service:run.id,type:'cf-worker',requestId:'request-first'},$workers:{scriptName:run.id},source:JSON.stringify({event:'antfly.resource_failure',failure_code:'SQLITE_NOMEM secret-must-not-persist'})});
 const report=await collectPersistedWindow(run,'app',token,time,fixture([row('first'),log]));assert.ok(report.resource_failures>0);assert.ok(!JSON.stringify(report).includes('secret-must-not-persist'));
 const draining={...log,$metadata:{...log.$metadata,id:'draining'},source:{event:'antfly.coordinator',failure_code:'Canceled',io_draining:true}};
 assert.equal((await collectPersistedWindow(run,'app',token,time,fixture([draining]))).gap,true);
 const metrics={...log,$metadata:{...log.$metadata,id:'metrics'},source:{cpuTimeMs:1,memory_limit_bytes:8388608}};
 const healthy=await collectPersistedWindow(run,'app',token,time,fixture([metrics]));assert.equal(healthy.resource_failures,0);assert.equal(healthy.rows[0].event.exceptions,undefined);
});
test('persisted query failures expose fixed codes and bound all response and query memory',async()=>{
 await assert.rejects(collectPersistedWindow(run,'app',token,time,{fetch:async()=>Response.json({errors:[{message:'secret'}]},{status:403})}),/DiagnosticQueryStatus403/);
 await assert.rejects(collectPersistedWindow(run,'app',token,time,{fetch:async()=>{throw Error('Bearer secret');}}),error=>error.message==='DiagnosticQueryTransportFailed');
 await assert.rejects(collectPersistedWindow(run,'app',token,time,{fetch:async()=>new Response('x'.repeat(4*1024*1024+1))}),/ResponseExceeded/);
});

test('persisted evidence independently verifies all eight roles and both snapshots and rejects substitution',async()=>{
 const{verifyPersistedProbe}=await import('../qualification/persisted-diagnostic-evidence.mjs'),{mkdtemp,writeFile,readFile,rm}=await import('node:fs/promises'),{createHash}=await import('node:crypto');
 const directory=await mkdtemp(resolve('dist','persisted-evidence-')),otherId=run.id.slice(0,-1)+'b',otherDir=resolve('dist',otherId),other={...run,id:otherId,worker:otherId,bucket:otherId,queues:[otherId,otherId+'-dead'],dir:otherDir,token_file:resolve(otherDir,'api-token'),services:run.services.map(service=>({...service,name:otherId+(service.role==='app'?'':'-'+service.role)}))},runs=[other,run],report={schema:1,qualification:false,stable:true,error:null,timeframe:time,resource_failures:0,invocation_errors:0,gaps:0,artifacts:[]};
 const audit=resolve(directory,'audit.json');
 try{
  for(const target of runs)for(const role of['app','writer','reader','builder'])for(const pass of[0,1]){
   const worker=target.id+(role==='app'?'':'-'+role),event=row('first',{$metadata:{id:'first',service:worker,type:'cf-worker-event',requestId:'request-first'},$workers:{scriptName:worker,outcome:'ok'}}),window=await collectPersistedWindow(target,role,token,time,{...fixture([event]),clock:()=>100000+pass*30000}),path=resolve(directory,target.id+'-'+role+'-'+pass+'.json'),bytes=Buffer.from(JSON.stringify(window));
   await writeFile(path,bytes);report.artifacts.push({worker,role,pass,path,events:1,invocations:1,bytes:bytes.length,sha256:createHash('sha256').update(bytes).digest('hex')});
  }
  await writeFile(audit,JSON.stringify(report));assert.equal(await verifyPersistedProbe(directory,runs),true);
  const original=report.artifacts[0];report.artifacts[0]=report.artifacts[1];await writeFile(audit,JSON.stringify(report));assert.equal(await verifyPersistedProbe(directory,runs),false);report.artifacts[0]=original;
  await writeFile(audit,JSON.stringify(report));const bytes=await readFile(original.path);await writeFile(original.path,'{}');assert.equal(await verifyPersistedProbe(directory,runs),false);await writeFile(original.path,bytes);
  report.resource_failures=1;await writeFile(audit,JSON.stringify(report));assert.equal(await verifyPersistedProbe(directory,runs),false);
 }finally{await rm(directory,{recursive:true,force:true});}
});
