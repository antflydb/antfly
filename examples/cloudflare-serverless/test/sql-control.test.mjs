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
import {createHmac,createHash} from 'node:crypto';
import {SQLControl,authenticateSQL,consumeSQL,locatorName} from '../sql-control.mjs';
import {Bucket} from './object-fixture.mjs';
const digest=c=>Array(64).fill(c.charCodeAt(0));
const locator={tenant:digest('a'),query:{id:digest('b'),expires_ms:1000000}};
class Storage {
 values=new Map();alarm=null;
 async get(key){return this.values.get(key);}
 async put(key,value){this.values.set(key,structuredClone(value));}
 async delete(key){this.values.delete(key);}
 async setAlarm(time){this.alarm=time;}
 async deleteAlarm(){this.alarm=null;}
 async deleteAll(){this.values.clear();}
}
test('SQL service authentication verifies bounded audience-bound native JWT claims',async()=>{
 const env={SQL_SERVICE_SECRET:'x'.repeat(32),SQL_SERVICE_ISSUER:'sql:cluster'},now=100000;
 const claims={iss:env.SQL_SERVICE_ISSUER,sub:'native',aud:'antfly-internal-v1',principal_kind:'service',admin:true,iat:100,exp:160};
 const request=(changes={},secret=env.SQL_SERVICE_SECRET)=>{
  const bytes=[{alg:'HS256',typ:'JWT'},{...claims,...changes}].map(value=>Buffer.from(JSON.stringify(value)).toString('base64url')).join('.');
  const token=bytes+'.'+createHmac('sha256',secret).update(bytes).digest('base64url');
  return new Request('https://sql/internal/v1/sql/queries/turn',{headers:{'X-Antfly-Trusted-Principal':token}});
 };
 assert.equal(await authenticateSQL(request(),env,now),true);
 for(const invalid of [{aud:'public'},{principal_kind:'user'},{admin:false},{iss:'other'},{iat:101},{exp:100},{exp:161}])assert.equal(await authenticateSQL(request(invalid),env,now),false);
 assert.equal(await authenticateSQL(request({},'z'.repeat(32)),env,now),false);
 assert.equal(await authenticateSQL(new Request('https://sql'),env,now),false);
});
test('SQL alarms precede queue delivery and a replacement coordinator replays durable intent',async()=>{
 const storage=new Storage(),ctx={storage},sent=[];let fail=true,done=false,turns=0;
 const ticket={...locator,stage:0,task:0};
 const env={SQL_TASKS:{async sendBatch(items){assert.ok(storage.alarm);sent.push(...items);if(fail){fail=false;throw Error('queue response lost');}}}};
 const run=async()=>{turns++;return{complete:done,tickets:done?[]:[ticket]};};
 const first=new SQLControl(ctx,env,run,{now:()=>100,owner:'old'});
 await assert.rejects(first.advance(locator),/queue response lost/);
 assert.equal(storage.alarm,2100);assert.equal(sent.length,1);
 const replacement=new SQLControl(ctx,env,run,{now:()=>200,owner:'new'});
 await replacement.alarm();assert.equal(sent.length,2);assert.equal(turns,2);assert.deepEqual(sent[0].body,sent[1].body);
 done=true;assert.deepEqual(await replacement.advance(),{complete:true});assert.equal(storage.alarm,null);assert.equal(storage.values.size,0);
});
test('SQL queues ACK accepted records and retry leased tasks or lost coordinator wakeups',async()=>{
 let mode='running',wakeFailure=false,wakes=0;
 const env={SQL_COORDINATORS:{getByName(name){assert.equal(name,locatorName(locator));return{async kick(){wakes++;if(wakeFailure)throw Error('wake failed');}};}}};
 const run=async()=>({status:mode,until_ms:60100});
 const message=()=>({body:{...locator,stage:1,task:0},ack(){this.accepted=true;},retry(options){this.delay=options.delaySeconds;}});
 const leased=message();await consumeSQL({messages:[leased]},env,run,{now:()=>100});assert.equal(leased.accepted,undefined);assert.equal(leased.delay,90);
 mode='succeeded';wakeFailure=true;const lost=message();await consumeSQL({messages:[lost]},env,run,{now:()=>100});assert.equal(lost.accepted,undefined);assert.equal(lost.delay,2);
 wakeFailure=false;const retry=message();await consumeSQL({messages:[retry]},env,run,{now:()=>100});assert.equal(retry.accepted,true);assert.equal(wakes,2);
 const stale=message();await consumeSQL({messages:[stale]},env,async()=>{throw Error('SqlExecutionFenced');},{now:()=>100});assert.equal(stale.accepted,true);
});
test('SQL discovery advances through early expired queries and authenticates immutable plan bytes',async()=>{
 const bucket=new Bucket(),storage=new Storage(),kicks=[];
 const config={state:{bucket:'execution',prefix:'state'},exchange:{bucket:'execution',prefix:'exchange'},artifacts:{bucket:'execution',prefix:'artifacts'},source_buckets:['execution']};
 const env={SQL_CONFIG:JSON.stringify(config),SQL_BUCKET_BINDINGS:JSON.stringify({execution:'R2'}),R2:bucket,SQL_TASKS:{},SQL_COORDINATORS:{getByName(name){return{async kick(value){kicks.push({name,value});}};}}};
 for(let i=0;i<17;i++){
  const expires_ms=i<16?1:1000000,raw=JSON.stringify({tenant:locator.tenant,expires_ms,index:i}),id=createHash('sha256').update(raw).digest('hex'),expiry=String(expires_ms).padStart(20,'0');
  await bucket.put(`state/sql-query-expiry/${expiry}/${id}.json`,'{}');await bucket.put(`state/sql-execution-v2/${expiry}/${id}/plan.json`,raw);
 }
 const discovery=new SQLControl({storage},env,()=>{throw Error('unexpected kernel call');},{now:()=>100,owner:'discovery'});
 assert.deepEqual(await discovery.recover(),{recovered:0});assert.ok(await storage.get('cursor'));
 assert.deepEqual(await discovery.recover(),{recovered:1});assert.equal(await storage.get('cursor'),undefined);
 assert.equal(kicks.length,1);assert.equal(kicks[0].name,locatorName(kicks[0].value));
});

test('SQL byte caching is opt-in, physical-binding scoped and excludes mutable query records',async()=>{
 const {createImmutableReadCache,isImmutableObjectKey}=await import('../../../zig/lib/objectstore/wasm/cache.mjs');
 const key='state/sql-execution-v2/00000000000001000000/'+'a'.repeat(64)+'/plan.json';
 assert.equal(isImmutableObjectKey(key),false);assert.equal(isImmutableObjectKey(key,{includeSQL:true}),true);
 for(const suffix of ['result-page','result-progress','physical','tiles']){
  const metadata='exchange/sql-exchange/00000000000001000000/'+'a'.repeat(64)+'/'+'b'.repeat(64)+'.'+suffix+'.json';
  assert.equal(isImmutableObjectKey(metadata),false);assert.equal(isImmutableObjectKey(metadata,{includeSQL:true}),true);
 }
 const cache=createImmutableReadCache({includeSQL:true});let reads=0;
 const transport=async request=>{reads++;return{json:{content_length:2,etag:'immutable'},body:new Uint8Array([1,2])};};
 const first=cache.wrap(transport,'physical-a'),other=cache.wrap(transport,'physical-b');
 const request={op:'get',bucket:'source',key};await first(request);await first(request);assert.equal(reads,1);
 await other(request);assert.equal(reads,2);
 const mutable={...request,key:key.replace('plan.json','query.json')};await first(mutable);await first(mutable);assert.equal(reads,4);
});

test('SQL discovery drains completed backlogs through durable alarms after eviction',async()=>{
 const bucket=new Bucket(),storage=new Storage(),kicks=[];
 const initial=100000,expiry=initial+3600000;let now=initial;
 const config={state:{bucket:'execution',prefix:'state'},exchange:{bucket:'execution',prefix:'exchange'},artifacts:{bucket:'execution',prefix:'artifacts'},source_buckets:['execution']};
 const env={SQL_CONFIG:JSON.stringify(config),SQL_BUCKET_BINDINGS:JSON.stringify({execution:'R2'}),R2:bucket,SQL_TASKS:{},SQL_COORDINATORS:{getByName(){return{async kick(value){kicks.push(value);}};}}};
 for(let i=0;i<1001;i++){
  const expires_ms=expiry+(i===1000?1:0),raw=JSON.stringify({tenant:locator.tenant,expires_ms,index:i}),id=createHash('sha256').update(raw).digest('hex'),stamp=String(expires_ms).padStart(20,'0');
  await bucket.put(`state/sql-query-expiry/${stamp}/${id}.json`,'{}');
  await bucket.put(`state/sql-execution-v2/${stamp}/${id}/plan.json`,raw);
  await bucket.put(`state/sql-execution-v2/${stamp}/${id}/query.json`,JSON.stringify({version:2,plan:id,expires_ms,status:i===1000?'pending':['succeeded','failed','cancelled'][i%3]}));
 }
 const control=()=>new SQLControl({storage},env,()=>{throw Error('unexpected kernel call');},{now:()=>now});
 await control().recover();
 let turns=1;
 while(storage.alarm!==null){now=storage.alarm;await control().alarm();assert.ok(++turns<100);}
 assert.equal(turns,63);assert.equal(kicks.length,1);assert.ok(now<initial+60000);
 assert.equal(await storage.get('cursor'),undefined);
 // A failed listing retains a durable retry even with no cron invocation.
 const list=bucket.list.bind(bucket);bucket.list=async()=>{throw Error('transient list failure');};
 await assert.rejects(control().recover(),/transient list failure/);assert.ok(storage.alarm);
 bucket.list=list;now=storage.alarm;await control().alarm();assert.equal(await storage.get('cursor'),undefined);
 assert.equal((await bucket.list({prefix:'state/sql-query-expiry/'})).objects.length,1);
 assert.equal(kicks.length,2);
});
