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

import{test}from'node:test';import assert from'node:assert/strict';
import{InvocationLedger,rayIdentity,attemptIdentity,attemptParameter,requestAttemptIdentity,reconcileInvocations}from'../qualification/invocation-accounting.mjs';
import{GateClient}from'../qualification/gate-client.mjs';
const version='01234567-0123-4567-89ab-0123456789ab',ray='0123456789abcdef';
test('attempt accounting records admission retries without bodies headers or raw Ray IDs',async()=>{
 const seen=[],c=new GateClient({origin:'https://fixture.invalid',version},'secret-token',{fetch:async(url,options)=>{seen.push(new URL(url).searchParams.get('__antfly_qualification_attempt'));return Response.json(seen.length===1?{error:'CoordinatorRecovering'}:{count:'1'},{status:seen.length===1?503:200,headers:{'cf-ray':ray.slice(0,-1)+(seen.length===1?'1':'2')+'-SJC','X-Antfly-Version':version,Authorization:'secret-header'}});}});
 await c.call('a',{op:'count'},'read');const ledger=c.report().invocation_ledger;
 assert.equal(ledger.attempts.length,2);assert.notEqual(seen[0],seen[1]);assert.deepEqual(ledger.attempts.map(r=>r.status),[503,200]);assert.ok(ledger.attempts.every(r=>r.finished_at!==null));
 const saved=JSON.stringify(ledger);for(const text of['secret-token','secret-header',ray,'fixture.invalid'])assert.ok(!saved.includes(text));
 assert.equal(rayIdentity(ray+'-SJC'),rayIdentity(ray.toUpperCase()));assert.equal(requestAttemptIdentity('https://fixture.invalid/?__antfly_qualification_attempt='+seen[0]),attemptIdentity(ledger.attempts[0].id));
 const bounded=new InvocationLedger({limit:1});bounded.begin({phase:'read',op:'count',tenant:'a'});assert.equal(bounded.begin({phase:'read',op:'count',tenant:'a'}),null);assert.equal(bounded.report().overflow,true);
});
function fixture(){
 const ledger=new InvocationLedger(),attempt=ledger.begin({phase:'read',op:'count',tenant:'a'});ledger.response(attempt,new Response('',{headers:{'cf-ray':ray,'X-Antfly-Version':version}}));ledger.finish(attempt);
 const timeframe={from:attempt.started_at-1,to:attempt.finished_at+1};
 const run={id:'owned',version,versions:Object.fromEntries(['app','writer','reader','builder'].map(role=>[role,version]))};
 const windows=['app','writer','reader','builder'].map((role,i)=>({schema:2,role,timeframe,worker:run.id+(role==='app'?'':'-'+role),reconciled:true,rows:[{timestamp:attempt.started_at,invocation:true,event_id_sha256:String(i).repeat(64),invocation_id_sha256:String(i+4).repeat(64),version_id:version,event_type:role==='app'?'fetch':role==='builder'?'queue':'rpc',ray_id_sha256:role==='app'?rayIdentity(ray):null,attempt_id_sha256:role==='app'?attemptIdentity(attempt.id):null}]}));
 const roleCounts=Object.fromEntries(windows.map(w=>[w.role,{worker:w.worker,timeframe,count:1,sample_interval:1,source:'independent-platform-counter'}]));return{ledger:ledger.report(),run,windows,roleCounts,timeframe};
}
test('invocation reconciliation binds every role, version and acknowledgement to independent counts',()=>{
 const f=fixture();assert.equal(reconcileInvocations(f.run,f.ledger,f.windows,f).complete,true);
 for(const mutate of[
  f=>f.windows[0].rows.splice(0),f=>f.windows[1].rows[0].version_id=crypto.randomUUID(),f=>f.roleCounts.writer.sample_interval=2,
  f=>delete f.roleCounts.builder,f=>f.roleCounts.app.timeframe={from:0,to:1},f=>f.windows[1].timeframe={from:f.timeframe.from+1,to:f.timeframe.to},f=>f.ledger.attempts.push({...f.ledger.attempts[0]}),f=>f.ledger.attempts[0].version_id=null,f=>f.ledger.attempts[0].ray_id_sha256=null,f=>f.ledger.overflow=true,
  f=>f.windows.push({...f.windows[0],rows:[{...f.windows[0].rows[0],version_id:crypto.randomUUID()}]}),
 ]){const f=fixture();mutate(f);assert.equal(reconcileInvocations(f.run,f.ledger,f.windows,f).complete,false);}
 const missing=fixture();assert.equal(reconcileInvocations(missing.run,missing.ledger,missing.windows).complete,false);
 const overlap=fixture();overlap.windows.push(structuredClone(overlap.windows[0]));assert.equal(reconcileInvocations(overlap.run,overlap.ledger,overlap.windows,overlap).complete,true);
});

test('attempt tags round-trip full UUID entropy while avoiding documented log redaction shapes',()=>{
 for(const id of['00000000-0000-0000-0000-000000000000','ffffffff-ffff-ffff-ffff-ffffffffffff',...Array.from({length:100},()=>crypto.randomUUID())]){const parameter=attemptParameter(id);assert.match(parameter,/^q[0-9a-z]{25}$/);assert.doesNotMatch(parameter,/^[0-9a-f+-_]{32,}$/i);assert.equal(requestAttemptIdentity('https://fixture.invalid/?__antfly_qualification_attempt='+parameter),attemptIdentity(id));}
 for(const parameter of['REDACTED','q'+'z'.repeat(25),'q123','q'+'0'.repeat(24)+'-'])assert.equal(requestAttemptIdentity('https://fixture.invalid/?__antfly_qualification_attempt='+parameter),null);
});

test('completion-window audits retain acknowledged requests that started before the provider log window',()=>{
 const f=fixture(),attempt=f.ledger.attempts[0],from=attempt.started_at+1000;f.timeframe={from,to:from+1000};attempt.response_at=from+100;attempt.finished_at=from+105;
 for(const window of f.windows){window.timeframe=f.timeframe;window.rows[0].timestamp=from+95;f.roleCounts[window.role].timeframe=f.timeframe;}
 assert.equal(reconcileInvocations(f.run,f.ledger,f.windows,f).complete,false);
 const options={...f,ledgerTimeBasis:'response_at'};assert.equal(reconcileInvocations(f.run,f.ledger,f.windows,options).complete,true);
 f.windows[0].rows[0].attempt_id_sha256='f'.repeat(64);assert.equal(reconcileInvocations(f.run,f.ledger,f.windows,options).complete,false);
 assert.throws(()=>reconcileInvocations(f.run,f.ledger,f.windows,{...f,ledgerTimeBasis:'unknown'}),/TimeBasis/);
});
