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
import {SQLControl,authenticateSQL,consumeSQL,locatorName,scheduleSQLMaintenance,sqlErrorStatus} from '../sql-control.mjs';
import {Bucket} from './object-fixture.mjs';
const digest=c=>Array(64).fill(c.charCodeAt(0));
const locator={tenant:digest('a'),query:{id:digest('b'),expires_ms:1000000}};
class Storage {
 values=new Map();alarm=null;inAlarm=false;retrySuppressed=false;
 async get(key){return this.values.get(key);}
 async put(key,value){this.values.set(key,structuredClone(value));}
 async delete(key){this.values.delete(key);}
 async getAlarm(){return this.alarm;}
 async setAlarm(time){this.alarm=time;}
 async deleteAlarm(){this.alarm=null;if(this.inAlarm)this.retrySuppressed=true;}
 async deleteAll(){this.values.clear();this.alarm=null;}
 async fire(control){
  const fired=this.alarm;this.alarm=null;this.inAlarm=true;this.retrySuppressed=false;
  try{return await control.alarm();}
  catch(error){if(!this.retrySuppressed&&this.alarm===null)this.alarm=(fired??control.now())+2000;throw error;}
  finally{this.inAlarm=false;}
 }
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
 assert.equal(storage.alarm,1100);assert.equal(sent.length,1);
 const replacement=new SQLControl(ctx,env,run,{now:()=>200,owner:'new'});
 await storage.fire(replacement);assert.equal(sent.length,2);assert.equal(turns,2);assert.deepEqual(sent[0].body,sent[1].body);
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
 const admission={...request,key:key.replace('plan.json','admission-v1.json')};
 assert.equal(isImmutableObjectKey(admission.key),false);assert.equal(isImmutableObjectKey(admission.key,{includeSQL:true}),true);
 await first(admission);await first(admission);assert.equal(reads,5);
 await other(admission);assert.equal(reads,6);
 assert.equal(isImmutableObjectKey(key.replace('plan.json','admission-v2.json'),{includeSQL:true}),false);
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
 while(storage.alarm!==null){now=storage.alarm;await storage.fire(control());assert.ok(++turns<100);}
 assert.equal(turns,63);assert.equal(kicks.length,1);assert.ok(now<initial+60000);
 assert.equal(await storage.get('cursor'),undefined);
 // A failed listing retains a durable retry even with no cron invocation.
 const list=bucket.list.bind(bucket);bucket.list=async()=>{throw Error('transient list failure');};
 await assert.rejects(control().recover(),/transient list failure/);assert.ok(storage.alarm);
 bucket.list=list;now=storage.alarm;await storage.fire(control());assert.equal(await storage.get('cursor'),undefined);
 assert.equal((await bucket.list({prefix:'state/sql-query-expiry/'})).objects.length,1);
 assert.equal(kicks.length,2);
});

test('SQL notifications retain earliest alarm across sustained traffic and eviction',async()=>{
 const storage=new Storage();let now=0,turns=0;
 const env={SQL_TASKS:{async sendBatch(){}}};
 const run=async()=>{turns++;return{complete:false,tickets:[]};};
 let control=new SQLControl({storage},env,run,{now:()=>now});
 await control.kick(locator);
 await Promise.all(Array.from({length:32},()=>control.kick(locator)));
 assert.equal(storage.alarm,1000);
 for(let i=0;i<64;i++){
  now+=500;
  if(storage.alarm!==null&&storage.alarm<=now)await storage.fire(control);
  control=new SQLControl({storage},env,run,{now:()=>now});
  await control.kick(locator);
 }
 assert.ok(turns>=16);
});
test('SQL alarms rearm during a long turn without starting overlapping turns',async()=>{
 const storage=new Storage();let now=0,release,turns=0;
 const control=new SQLControl({storage},{SQL_TASKS:{async sendBatch(){}}},async()=>{
  turns++;await new Promise(resolve=>{release=resolve;});return{complete:false,tickets:[]};
 },{now:()=>now});
 const pending=control.advance(locator);
 while(!release)await new Promise(resolve=>setImmediate(resolve));
 now=storage.alarm;
 await storage.fire(control);
 assert.equal(turns,1);assert.equal(storage.alarm,now+1000);
 release();await pending;
});

test('SQL alarm failures preserve durable recovery and platform retries before state reads',async()=>{
 for(const failure of ['getAlarm','setAlarm','get','deleteAll']){
  const storage=new Storage();let now=100,turns=0;
  const control=new SQLControl({storage},{SQL_TASKS:{async sendBatch(){}}},async()=>{turns++;return{complete:true,tickets:[]};},{now:()=>now});
  await control.kick(locator);
  const original=storage[failure].bind(storage);let fail=true;
  storage[failure]=async(...args)=>{if(fail){fail=false;throw Error('transient storage failure');}return original(...args);};
  now=storage.alarm;
  await assert.rejects(storage.fire(control),/transient storage failure/);
  assert.equal(storage.retrySuppressed,false);assert.ok(storage.alarm>now);assert.equal(turns,failure==='deleteAll'?1:0);
  now=storage.alarm;await storage.fire(new SQLControl({storage},control.env,control.run,{now:()=>now}));
  assert.equal(turns,failure==='deleteAll'?2:1);assert.equal(storage.alarm,null);
 }
 const empty=new Storage(),control=new SQLControl({storage:empty},{},()=>{throw Error('unexpected turn');});
 await empty.fire(control);assert.equal(empty.alarm,null);
});

test('SQL discovery validates compact records and overlaps bounded reads without loading plans',async()=>{
 const storage=new Storage(),bucket=new Bucket(),kicks=[];let active=0,maximum=0,planReads=0;
 const config={state:{bucket:'execution',prefix:'state'},exchange:{bucket:'execution',prefix:'exchange'},artifacts:{bucket:'execution',prefix:'artifacts'},source_buckets:['execution']};
 const env={SQL_CONFIG:JSON.stringify(config),SQL_BUCKET_BINDINGS:JSON.stringify({execution:'R2'}),R2:bucket,SQL_TASKS:{},SQL_COORDINATORS:{getByName(name){return{async kick(value){kicks.push({name,value});}};}}};
 const get=bucket.get.bind(bucket);
 bucket.get=async(...args)=>{
  if(args[0].endsWith('/plan.json')){planReads++;throw Error('plan unavailable');}
  active++;maximum=Math.max(maximum,active);
  try{await new Promise(resolve=>setImmediate(resolve));return await get(...args);}finally{active--;}
 };
 for(let i=0;i<16;i++){
  const expires_ms=1000000+i,id=createHash('sha256').update(String(i)).digest('hex'),stamp=String(expires_ms).padStart(20,'0');
  await bucket.put(`state/sql-query-expiry/${stamp}/${id}.json`,'{}');
  const record={version:2,plan:id,expires_ms,status:i<8?'pending':'running',tenant:i%2?locator.tenant:'a'.repeat(64)};
  // Malformed scoped records must not silently fall back to a different plan.
  if(i===12)record.plan='b'.repeat(64);
  if(i===13)record.expires_ms++;
  if(i===14)record.tenant='bad';
  if(i===15)record.status='invalid';
  await bucket.put(`state/sql-execution-v2/${stamp}/${id}/query.json`,JSON.stringify(record));
 }
 const control=new SQLControl({storage},env,()=>{throw Error('unexpected kernel');},{now:()=>100});
 assert.deepEqual(await control.recover(),{recovered:12});
 assert.deepEqual(await control.recover(),{recovered:12});
 assert.equal(kicks.length,24);assert.equal(planReads,0);assert.equal(maximum,4);assert.equal(active,0);
 for(const kick of kicks)assert.equal(kick.name,locatorName(kick.value));
});


test('SQL collector drains a single-lane backlog through durable alarms across eviction',async()=>{
 const storage=new Storage();let now=100000,calls=0,remaining=1000;
 const run=async(_env,input)=>{
  assert.equal(input.op,'sql_collect');calls++;
  assert.ok(storage.alarm!==null,'collection must arm recovery before external I/O');
  const deleted=Math.min(64,remaining);remaining-=deleted;
  return{deleted,more:remaining>0,lanes:{state:{deleted,more:remaining>0},discovery:{deleted:0,more:false},exchange:{deleted:0,more:false}}};
 };
 let control=new SQLControl({storage},{},run,{now:()=>now});
 const first=await control.collect();
 assert.equal(first.deleted,64);assert.equal(first.more,true);assert.equal(remaining,936);
 while(storage.alarm!==null){
  now=storage.alarm;
  control=new SQLControl({storage},{},run,{now:()=>now});
  await storage.fire(control);
  assert.ok(calls<=16,'collector must stop when the last nontruncated page drains');
 }
 assert.equal(remaining,0);assert.equal(calls,16);
 assert.equal(await storage.get('collection'),true);
});

test('SQL collector preserves bounded continuation on every alarm metadata failure',async()=>{
 for(const method of ['getAlarm','setAlarm','get','put','delete','deleteAlarm']){
  class FaultStorage extends Storage{
   failure=null;
   check(name){if(this.failure===name){this.failure=null;throw Error('injected '+name);}}
   async getAlarm(){this.check('getAlarm');return super.getAlarm();}
   async setAlarm(time){this.check('setAlarm');return super.setAlarm(time);}
   async get(key){if(key==='collection')this.check('get');return super.get(key);}
   async put(key,value){this.check('put');return super.put(key,value);}
   async delete(key){this.check('delete');return super.delete(key);}
   async deleteAlarm(){this.check('deleteAlarm');return super.deleteAlarm();}
  }
  const storage=new FaultStorage();let now=100000,calls=0;
  const run=async()=>({deleted:++calls===1?64:0,more:calls===1});
  let control=new SQLControl({storage},{},run,{now:()=>now});
  await control.collect();storage.failure=method;now=storage.alarm;
  control=new SQLControl({storage},{},run,{now:()=>now});
  await assert.rejects(storage.fire(control),/injected/);
  assert.notEqual(storage.alarm,null,method+' suppressed recovery');
  now=storage.alarm;control=new SQLControl({storage},{},run,{now:()=>now});
  await storage.fire(control);
  assert.equal(storage.alarm,null,method+' did not retire after recovery');
 }
});

test('SQL collector persists failure backoff and does not overlap racing cron nudges',async()=>{
 const storage=new Storage();let now=100000,calls=0,failing=true;
 const run=async()=>{calls++;if(failing)throw Error('provider unavailable');return{deleted:0,more:false};};
 let control=new SQLControl({storage},{},run,{now:()=>now});
 for(let attempt=0;attempt<3;attempt++){
  const start=now;
  await assert.rejects(control.collect(),/provider unavailable/);
  assert.equal(storage.alarm,start+1000*2**attempt);
  const deferred=new SQLControl({storage},{},run,{now:()=>now});
  assert.equal((await deferred.collect()).deferred,true);
  assert.equal(calls,attempt+1,'a nudge/eviction bypassed durable failure backoff');
  now=storage.alarm;control=new SQLControl({storage},{},run,{now:()=>now});
 }
 failing=false;await storage.fire(control);assert.equal(storage.alarm,null);
 assert.equal(await storage.get('gc_failures'),undefined);
 let entered,release;
 const started=new Promise(resolve=>entered=resolve),gate=new Promise(resolve=>release=resolve);
 let active=0,peak=0;
 control=new SQLControl({storage},{},async()=>{
  active++;peak=Math.max(peak,active);entered();await gate;active--;
  return{deleted:0,more:false};
 },{now:()=>now});
 const first=control.collect();await started;
 assert.equal((await control.collect()).more,true);
 release();assert.equal((await first).more,true,'a racing nudge lost its followup');
 assert.equal(peak,1);assert.notEqual(storage.alarm,null);
 now=storage.alarm;
 const resumed=new SQLControl({storage},{},async()=>({deleted:0,more:false}),{now:()=>now});
 await storage.fire(resumed);assert.equal(storage.alarm,null);
});

test('SQL collector supports old kernel counts and stops on an explicit drained page',async()=>{
 const storage=new Storage();let now=100000,calls=0;
 const run=async()=>({deleted:++calls===1?64:0});
 let control=new SQLControl({storage},{},run,{now:()=>now});
 assert.equal((await control.collect()).more,true);
 now=storage.alarm;await storage.fire(control);assert.equal(storage.alarm,null);
 control=new SQLControl({storage},{},async()=>({deleted:64,more:false}),{now:()=>now});
 assert.equal((await control.collect()).more,false);assert.equal(storage.alarm,null);
});

test('SQL cron starts collection independently of failing or blocked discovery',async()=>{
 const jobs=[];let collected=0,release;
 const gate=new Promise(resolve=>release=resolve);
 const env={SQL_COORDINATORS:{getByName:name=>name==='discovery'?{recover:()=>gate}:{collect:async()=>{collected++;}}}};
 scheduleSQLMaintenance(env,{waitUntil:job=>jobs.push(job)});
 await jobs[1];assert.equal(collected,1);
 release();await Promise.all(jobs);
 jobs.length=0;
 env.SQL_COORDINATORS.getByName=name=>{
  if(name==='discovery')throw Error('discovery unavailable');
  return{collect:async()=>{collected++;}};
 };
 scheduleSQLMaintenance(env,{waitUntil:job=>jobs.push(job)});
 const results=await Promise.allSettled(jobs);
 assert.equal(results[0].status,'rejected');assert.equal(results[1].status,'fulfilled');
 assert.equal(collected,2);
});


test('SQL collector recovery alarms do not keep a slow empty collector alive',async()=>{
 const storage=new Storage();let now=100000,entered,release,calls=0;
 const started=new Promise(resolve=>entered=resolve),gate=new Promise(resolve=>release=resolve);
 const control=new SQLControl({storage},{},async()=>{
  calls++;entered();await gate;return{deleted:0,more:false};
 },{now:()=>now});
 const pass=control.collect();await started;
 now=storage.alarm;await storage.fire(control);
 assert.equal(calls,1,'the recovery alarm overlapped external cleanup');
 release();assert.equal((await pass).more,false);
 assert.equal(storage.alarm,null,'an overlapping recovery alarm caused an idle busy loop');
});


test('SQL collector rotates its first lane durably before successful or failed external work',async()=>{
 const storage=new Storage();let now=100000,fail=true;const lanes=[];
 const run=async(_env,input)=>{
  lanes.push(input.collection_lane);
  assert.equal(await storage.get('gc_lane'),(input.collection_lane+1)%3);
  if(fail){fail=false;throw Error('interrupted pass');}
  return{deleted:1,more:lanes.length<4};
 };
 let control=new SQLControl({storage},{},run,{now:()=>now});
 await assert.rejects(control.collect(),/interrupted/);
 assert.equal(await storage.get('gc_lane'),1);
 assert.equal((await new SQLControl({storage},{},run,{now:()=>now}).collect()).deferred,true);
 assert.deepEqual(lanes,[0]);
 while(storage.alarm!==null){
  now=storage.alarm;control=new SQLControl({storage},{},run,{now:()=>now});
  await storage.fire(control);assert.ok(lanes.length<=4);
 }
 assert.deepEqual(lanes,[0,1,2,0]);
 assert.equal(await storage.get('gc_failures'),undefined);
});

test('SQL HTTP admission status distinguishes permanent requests from infrastructure failures',()=>{
 for(const name of ['InvalidSqlDistributedPlan','InvalidSqlProgram','InvalidSqlParameters','InvalidSqlIdentity','SqlDistributedJoinUnsupported','SqlControlRequestRejected','SqlRequestRequired'])assert.equal(sqlErrorStatus(Error(name)),400);
 assert.equal(sqlErrorStatus(new SyntaxError('deployment configuration')),503);
 for(const [name,status]of [['SqlRequestTooLarge',413],['SqlJoinBudgetExceeded',422],['SqlQueryExpired',410],['Canceled',408],['SqlExecutionCancelled',408],['SqlExecutionUnauthorized',403],['SqlExecutionNotFound',404],['SqlTaskAdmissionUnavailable',429],['DeadlineExceeded',504],['SqlExecutionFailed',409],['InvalidSqlExchange',503],['OutOfMemory',503],['SqlWorkerUnavailable',503]])assert.equal(sqlErrorStatus(Error(name)),status);
});
