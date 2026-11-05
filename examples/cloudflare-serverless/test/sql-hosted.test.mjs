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
import {readFile} from 'node:fs/promises';
import {Bucket} from './object-fixture.mjs';
import {SQLControl} from '../sql-control.mjs';
import {instantiateHost} from '../../../zig/lib/objectstore/wasm/asyncify.mjs';
import {createR2Transport} from '../../../zig/lib/objectstore/wasm/r2.mjs';

// Generate with ANTFLY_SQL_HOSTED_FIXTURE=../examples/cloudflare-serverless/dist/sql-fixture.json
// zig build distributed-sql-test, then instrument the WASM through npm run build.
const module = await WebAssembly.compile(await readFile(new URL('../dist/antfly-serverless.wasm', import.meta.url)));
for (const mode of ['shuffle','bucket','skew','range','wal']) {
const bucketed=mode==='bucket';
const fixture = JSON.parse(await readFile(new URL(mode==='wal'?'../dist/sql-wal-fixture.json':mode==='range'?'../dist/sql-range-fixture.json':mode==='skew'?'../dist/sql-skew-fixture.json':bucketed?'../dist/sql-bucket-fixture.json':'../dist/sql-fixture.json', import.meta.url)));
test(mode==='skew'?'WASM skew kernel finalizes accepted native map tiles with global NULL ownership':`WASM ${mode} joins native retained Parquet cuts with WAL overlays and recovers accepted tasks`, async () => {
  const buckets = Object.fromEntries(['source','execution'].map(name => [name,new Bucket()]));
  for (const object of fixture.objects) await buckets[object.bucket].put(object.key, new Uint8Array(Buffer.from(object.body_base64,'base64')));
  const transport = createR2Transport(buckets,{maxPutBytes:8*1024*1024});
  const host = await instantiateHost(module,transport);
  const request = {config:fixture.config,tenant:fixture.tenant,owner:'wasm-coordinator'};
  const submitted = fixture.query?{query:fixture.query}:await host.request({...request,op:'sql_submit',raw_plan:fixture.raw_plan});
  request.query = submitted.query;
  assert.equal(await host.request({...request,op:'sql_result'}),null);
  let complete=false,accepted=false,lost=false;
  for (let turn=0;turn<512;turn++) {
    const result = await host.request({...request,op:'sql_turn'});
    if (result.complete) { complete=true;break; }
    for (const ticket of result.tickets) {
      const task={...request,op:'sql_execute',owner:'wasm-worker',ticket};
      // Lose an accepted task's final write response. A new instance resolves
      // the durable claim/output instead of rerunning and duplicating rows.
      const fault=async (call,body) => {
        const result=await transport(call,body);
        if (!lost&&call.op==='put'&&(bucketed?call.key?.includes('/stage/0/'):call.key?.includes('/stage/')&&!call.key?.includes('/stage/0/'))&&new TextDecoder().decode(body).includes('"status":"succeeded"')) {
          lost=true;throw Error('lost completion response');
        }
        return result;
      };
      let outcome;
      try { outcome=await host.request(task,{transport:fault,maxOperations:900}); }
      catch(error) {
        if (!lost) throw error;
        const replacement=await instantiateHost(module,transport);
        outcome=await replacement.request(task,{maxOperations:900});
      }
      assert.equal(outcome.status,'succeeded');
      accepted=true;
    }
  }
  assert.ok(complete);assert.ok(accepted);assert.ok(lost);
  const result=await host.request({...request,op:'sql_result'});
  assert.equal(result.rows,fixture.expected_rows??2);
  const pages=[...result.pages];
  const manifests=[...(result.manifests??[])];
  while(manifests.length){
    const ref=manifests.pop();
    const bytes=buckets.execution.objects.get(ref.key).body;
    const digest=Buffer.from(await crypto.subtle.digest('SHA-256',bytes)).toString('hex');
    assert.equal(digest,typeof ref.hash==='string'?ref.hash:String.fromCharCode(...ref.hash));
    const chunk=JSON.parse(new TextDecoder().decode(bytes));assert.ok(chunk.pages.length<=64);pages.push(...chunk.pages);if(chunk.previous)manifests.push(chunk.previous);
  }
  if(mode!=='range'&&mode!=='wal') assert.equal(pages.length,fixture.expected_pages??(bucketed?4:1));
  assert.equal(pages.reduce((n,p)=>n+p.rows,0),result.rows);
  assert.ok(pages[0].index_bytes>0);
  const other=await instantiateHost(module,transport);
  assert.deepEqual(await other.request({...request,op:'sql_result'}),result);
  await assert.rejects(other.request({...request,op:'sql_submit',tenant:Array(64).fill(98),raw_plan:fixture.raw_plan}),/SqlExecutionUnauthorized/);
  const sourceObjects=buckets.source.objects.size;
  const expiry=JSON.parse(fixture.raw_plan).expires_ms;
  const collector=await instantiateHost(module,transport,{clock:()=>expiry+31000});
  let deleted=0;
  for(let turn=0;turn<64&&buckets.execution.objects.size;turn++){
    const collected=await collector.request({...request,op:'sql_collect'});
    deleted+=collected.deleted;
  }
  assert.ok(deleted>0);
  assert.equal(buckets.execution.objects.size,0,'R2 expiry cleanup left execution records or exchanges');
  assert.equal(buckets.source.objects.size,sourceObjects,'execution GC crossed into source retention');
});

}


test('WASM result assembly stays bounded through 1000 pages, lost checkpoint ACK and coordinator takeover', async()=>{
  const fixture=JSON.parse(await readFile(new URL('../dist/sql-skew-fixture.json',import.meta.url)));
  const plan=JSON.parse(fixture.raw_plan);
  plan.strategy='broadcast';plan.join.kind='inner';plan.left.splits=1000;plan.right.splits=1;plan.limits.task_output_bytes=4096;
  const raw=JSON.stringify(plan),id=Buffer.from(await crypto.subtle.digest('SHA-256',new TextEncoder().encode(raw))).toString('hex');
  const query={id,expires_ms:plan.expires_ms},root=`state/sql-execution-v2/${String(query.expires_ms).padStart(20,'0')}/${id}`;
  const buckets={source:new Bucket(),execution:new Bucket()};
  const seed=fixture.objects.find(o=>o.key.includes('/stage/0/task/21.json')&&o.key.includes(typeof fixture.query.id==='string'?fixture.query.id:String.fromCharCode(...fixture.query.id)));
  const accepted=JSON.parse(Buffer.from(seed.body_base64,'base64'));accepted.plan=id;
  const record={...accepted,status:'pending',owner:'',epoch:0,until_ms:0,stage:2,cursor:0,pending:[],reduce_boundaries:[],skew:[],physical:null,adaptive:null,result_progress:null,output:null};
  await buckets.execution.put(root+'/plan.json',raw);await buckets.execution.put(root+'/query.json',JSON.stringify(record));
  for(let i=0;i<1000;i++)await buckets.execution.put(root+`/stage/1/task/${i}.json`,JSON.stringify(accepted));
  const transport=createR2Transport(buckets);
  let now=query.expires_ms-1000000,lost=false,maximum=0,complete=false;
  let host=await instantiateHost(module,transport,{clock:()=>now});
  const request={op:'sql_turn',config:fixture.config,tenant:plan.tenant,query,owner:'first'};
  for(let turn=0;turn<20;turn++){
    const fault=async(call,body)=>{
      const result=await transport(call,body);
      if(!lost&&call.op==='put'&&call.key===root+'/query.json'&&JSON.parse(new TextDecoder().decode(body)).result_progress){lost=true;throw Error('lost result checkpoint ACK');}
      return result;
    };
    try{complete=(await host.request(request,{transport:fault,maxOperations:900,onMetrics:m=>{maximum=Math.max(maximum,m.operations);}})).complete;}
    catch(error){assert.ok(lost,String(error));}
    if(complete)break;
    if(turn===3){now+=92001;request.owner='replacement';host=await instantiateHost(module,transport,{clock:()=>now});}
  }
  assert.ok(complete);assert.ok(lost);assert.ok(maximum<100,`unbounded completion turn: ${maximum}`);
  const result=await host.request({...request,op:'sql_result'},{maxOperations:900});
  assert.equal(result.rows,1000*accepted.output.rows);assert.equal(result.pages.length,0);assert.equal(result.manifests.length,1);
  let next=result.manifests[0],pages=0,rows=0;
  while(next){
    const bytes=buckets.execution.objects.get(next.key).body;
    assert.equal(Buffer.from(await crypto.subtle.digest('SHA-256',bytes)).toString('hex'),typeof next.hash==='string'?next.hash:String.fromCharCode(...next.hash));
    const chunk=JSON.parse(new TextDecoder().decode(bytes));assert.ok(chunk.pages.length<=64);
    pages+=chunk.pages.length;rows+=chunk.rows;next=chunk.previous;
  }
  assert.equal(pages,1000);assert.equal(rows,result.rows);
});

for(const strategy of ['shuffle','broadcast']) test(`WASM ordinary ${strategy} consolidates 320 small build fragments within the production operation cap`,async()=>{
 const fixture=JSON.parse(await readFile(new URL('../dist/sql-skew-fixture.json',import.meta.url)));
 const plan=JSON.parse(fixture.raw_plan);plan.strategy=strategy;if(strategy==='broadcast')plan.join.kind='inner';plan.left.splits=1;plan.right.splits=320;plan.left.estimated_rows=2;plan.right.estimated_rows=320;
 const raw=JSON.stringify(plan),id=Buffer.from(await crypto.subtle.digest('SHA-256',new TextEncoder().encode(raw))).toString('hex'),query={id,expires_ms:plan.expires_ms};
 const root=`state/sql-execution-v2/${String(query.expires_ms).padStart(20,'0')}/${id}`,original=typeof fixture.query.id==='string'?fixture.query.id:String.fromCharCode(...fixture.query.id);
 const decode=i=>JSON.parse(Buffer.from(fixture.objects.find(o=>o.key.includes(original)&&o.key.endsWith('/stage/0/task/'+i+'.json')).body_base64,'base64'));
 const right=decode(301),left=decode(300);right.plan=id;left.plan=id;
 const buckets={source:new Bucket(),execution:new Bucket()};for(const o of fixture.objects)await buckets[o.bucket].put(o.key,Buffer.from(o.body_base64,'base64'));
 const record={...left,status:'pending',owner:'',epoch:0,until_ms:0,stage:1,cursor:0,pending:[],reduce_boundaries:strategy==='broadcast'?[]:[0,1],skew:[],physical:null,adaptive:null,result_progress:null,output:null};
 await buckets.execution.put(root+'/plan.json',raw);await buckets.execution.put(root+'/query.json',JSON.stringify(record));
 for(let i=0;i<321;i++)await buckets.execution.put(root+`/stage/0/task/${i}.json`,JSON.stringify(i<320?right:left));
 const host=await instantiateHost(module,createR2Transport(buckets),{clock:()=>query.expires_ms-100000});
 const request={config:fixture.config,tenant:plan.tenant,query,owner:'consolidator'};let complete=false,maximum=0;
 const options={maxOperations:900,onMetrics:m=>{maximum=Math.max(maximum,m.operations);}};
 for(let turn=0;turn<32;turn++){
  const result=await host.request({...request,op:'sql_turn'},options);
  if(result.complete){complete=true;break;}
  for(const ticket of result.tickets){const accepted=await host.request({...request,op:'sql_execute',owner:'worker',ticket},options);assert.equal(accepted.status,'succeeded');}
 }
 assert.ok(complete);assert.ok(maximum<150,`unbounded fan-in: ${maximum}`);
 const result=await host.request({...request,op:'sql_result'},options);assert.equal(result.rows,strategy==='broadcast'?0:322);
});


test('WASM active task observes durable cancellation after a suspended source read', async()=>{
  const fixture=JSON.parse(await readFile(new URL('../dist/sql-fixture.json',import.meta.url)));
  const buckets=Object.fromEntries(['source','execution'].map(name=>[name,new Bucket()]));
  for(const object of fixture.objects) await buckets[object.bucket].put(object.key,new Uint8Array(Buffer.from(object.body_base64,'base64')));
  const transport=createR2Transport(buckets,{maxPutBytes:8*1024*1024});
  const host=await instantiateHost(module,transport);
  const request={config:fixture.config,tenant:fixture.tenant,owner:'coordinator'};
  request.query=(await host.request({...request,op:'sql_submit',raw_plan:fixture.raw_plan})).query;
  const turn=await host.request({...request,op:'sql_turn'});
  assert.ok(turn.tickets.length);
  const queryKey=[...buckets.execution.objects.keys()].find(key=>key.endsWith('/query.json'));
  let interrupted=false;
  const cancelDuringRead=async(call,body)=>{
    const result=await transport(call,body);
    if(!interrupted&&call.op==='get'&&call.bucket==='source'){
      interrupted=true;
      const record=JSON.parse(new TextDecoder().decode(buckets.execution.objects.get(queryKey).body));
      record.cancel_requested=true;
      await buckets.execution.put(queryKey,JSON.stringify(record));
      await new Promise(resolve=>setTimeout(resolve,1100));
    }
    return result;
  };
  const outcome=await host.request({...request,op:'sql_execute',owner:'worker',ticket:turn.tickets[0]},{transport:cancelDuringRead,maxOperations:900});
  assert.ok(interrupted);
  assert.equal(outcome.status,'cancelled');
  for(const [key,object] of buckets.execution.objects){
    if(key.includes('/stage/')) assert.notEqual(JSON.parse(new TextDecoder().decode(object.body)).status,'succeeded','cancelled task accepted output');
  }
});

test('WASM retry admission survives plan failures without spending epochs during sidecar outages',async()=>{
 const fixture=JSON.parse(await readFile(new URL('../dist/sql-fixture.json',import.meta.url)));
 const buckets=Object.fromEntries(['source','execution'].map(name=>[name,new Bucket()]));
 for(const object of fixture.objects)await buckets[object.bucket].put(object.key,new Uint8Array(Buffer.from(object.body_base64,'base64')));
 const base=createR2Transport(buckets,{maxPutBytes:8*1024*1024});
 let now=Date.now(),failPlan=false,failAdmission=false,planReads=0,admissionReads=0;
 const transport=async(request,body)=>{
  if(request.op==='get'&&request.key.endsWith('/admission-v1.json')){admissionReads++;if(failAdmission)throw Error('injected admission outage');}
  if(request.op==='get'&&request.key.endsWith('/plan.json')&&failPlan){planReads++;throw Error('injected plan outage');}
  return base(request,body);
 };
 const host=await instantiateHost(module,transport,{clock:()=>now});
 const request={config:fixture.config,tenant:fixture.tenant,owner:'coordinator'};
 const spec=JSON.parse(fixture.raw_plan);spec.limits.max_attempts=1;
 const {query}=await host.request({...request,op:'sql_submit',raw_plan:JSON.stringify(spec)});
 const turn=await host.request({...request,query,op:'sql_turn'});
 assert.ok(turn.tickets.length>0);
 const ticket=turn.tickets[0],task={...request,query,op:'sql_execute',owner:'worker',ticket};
 failPlan=true;
 await assert.rejects(host.request(task));
 assert.equal(planReads,1);assert.equal(admissionReads,0);
 now+=92001;failAdmission=true;
 await assert.rejects(host.request(task));
 const id=typeof query.id==='string'?query.id:String.fromCharCode(...query.id),root=`${fixture.config.state.prefix}/sql-execution-v2/${String(query.expires_ms).padStart(20,'0')}/${id}`;
 const key=`${root}/stage/${ticket.stage}/task/${ticket.task}.json`;
 const bucket=buckets[fixture.config.state.bucket];
 assert.equal((await(await bucket.get(key)).json()).epoch,1);
 failAdmission=false;
 assert.equal((await host.request(task)).status,'failed');
 const failed=await(await bucket.get(key)).json();
 assert.equal(failed.epoch,2);assert.equal(failed.failure,'TaskRetryBudgetExceeded');
 assert.equal(failed.task_attempt_limit,undefined);assert.equal(planReads,1);assert.equal(admissionReads,2);
});

test('WASM collection independently drains thousand-object expiry lanes and preserves live/source data',async()=>{
 const fixture=JSON.parse(await readFile(new URL('../dist/sql-fixture.json',import.meta.url)));
 const kinds=['state','discovery','exchange'];
 for(const kind of kinds){
  const buckets={source:new Bucket(),execution:new Bucket()};
  const state=fixture.config.state.prefix,exchange=fixture.config.exchange.prefix;
  const prefix=kind==='exchange'?`${exchange}/sql-exchange/`:kind==='discovery'?`${state}/sql-query-expiry/`:`${state}/sql-execution-v2/`;
  for(let i=0;i<1000;i++){
   const id=i.toString(16).padStart(64,'0');
   await buckets.execution.put(prefix+'00000000000000000001/'+id+(kind==='discovery'?'.json':'/expired.json'),'{}');
  }
  const live=prefix+String(Date.now()+3600000).padStart(20,'0')+'/'+'b'.repeat(64)+(kind==='discovery'?'.json':'/live.json');
  await buckets.execution.put(live,'live');await buckets.source.put('artifacts/keep','source');
  const transport=createR2Transport(buckets),host=await instantiateHost(module,transport);
  let total=0,turns=0,more;
  do{
   const result=await host.request({config:fixture.config,tenant:fixture.tenant,owner:'gc',op:'sql_collect'});
   assert.equal(result.deleted,result.lanes[kind].deleted);
   assert.ok(result.deleted>0&&result.deleted<=64);
   for(const other of kinds)if(other!==kind)assert.deepEqual(result.lanes[other],{deleted:0,more:false});
   total+=result.deleted;more=result.more;turns++;
   assert.ok(turns<=16,'a full single lane must not terminate or spin indefinitely');
  }while(more);
  assert.equal(total,1000);assert.equal(turns,16);
  assert.equal(buckets.execution.objects.size,1);assert.ok(buckets.execution.objects.has(live));
  assert.equal(buckets.source.objects.size,1);
  const idle=await host.request({config:fixture.config,tenant:fixture.tenant,owner:'gc',op:'sql_collect'});
  assert.equal(idle.deleted,0);assert.equal(idle.more,false);
 }
});


function collectionStorage(){
 const values=new Map();let alarm=null;
 return{values,async get(key){return values.get(key);},async put(key,value){values.set(key,structuredClone(value));},async delete(key){values.delete(key);},async getAlarm(){return alarm;},async setAlarm(time){alarm=time;},async deleteAlarm(){alarm=null;}};
}
for(const slow of ['list','delete'])test(`WASM GC ${slow} quantum yields without starving exchange or inducing failure backoff`,async(t)=>{
 const fixture=JSON.parse(await readFile(new URL('../dist/sql-fixture.json',import.meta.url)));
 let now=Date.now();
 // WASM deadlines use the monotonic clock; advancing wall time alone models a
 // clock jump, not slow I/O. Keep both clocks advancing by the provider delay.
 t.mock.method(performance,'now',()=>now);
 const prefix=fixture.config.state.prefix+'/sql-execution-v2/';
 class SlowState extends Bucket{
  async list(options){if(slow==='list'&&options.prefix===prefix)now+=16000;return super.list(options);}
  async delete(key){if(slow==='delete'&&typeof key==='string'&&key.startsWith(prefix))now+=1000;return super.delete(key);}
 }
 const buckets={source:new Bucket(),execution:new SlowState()};
 for(let i=0;i<1000;i++)await buckets.execution.put(prefix+'00000000000000000001/'+i.toString(16).padStart(64,'0')+'/query.json','{}');
 const output=fixture.config.exchange.prefix+'/sql-exchange/00000000000000000001/'+'b'.repeat(64)+'/output.bin';
 await buckets.execution.put(output,'expired');
 const host=await instantiateHost(module,createR2Transport(buckets),{clock:()=>now});
 const run=async(_env,input)=>host.request({...input,config:fixture.config},{deadlineMs:now+55000,maxOperations:900});
 const storage=collectionStorage(),control=new SQLControl({storage},{},run,{now:()=>now});
 const result=await control.collect();
 assert.equal(result.more,true);assert.equal(result.lanes.state.more,true);
 assert.equal(result.lanes.exchange.deleted,1);assert.equal(buckets.execution.objects.has(output),false);
 assert.equal(await storage.get('gc_failures'),undefined,'yielding a quantum was misclassified as an outage');
 assert.equal(await storage.get('gc_not_before'),undefined);
 assert.ok((await storage.getAlarm())<=now+1,'the yielded pass lost its prompt continuation');
 assert.equal(await storage.get('gc_lane'),1);
 if(slow==='list')assert.equal(result.lanes.state.deleted,0);
 else assert.ok(result.lanes.state.deleted>0&&result.lanes.state.deleted<64);
});

test('WASM GC durable lane rotation serves healthy storage even after an unabortable call exhausts a pass',async(t)=>{
 const fixture=JSON.parse(await readFile(new URL('../dist/sql-fixture.json',import.meta.url)));
 let now=Date.now();t.mock.method(performance,'now',()=>now);const prefix=fixture.config.state.prefix+'/sql-execution-v2/';
 class SlowState extends Bucket{
  async delete(key){if(typeof key==='string'&&key.startsWith(prefix))now+=60000;return super.delete(key);}
 }
 const buckets={source:new Bucket(),execution:new SlowState()};
 for(let i=0;i<100;i++)await buckets.execution.put(prefix+'00000000000000000001/'+i.toString(16).padStart(64,'0')+'/query.json','{}');
 const output=fixture.config.exchange.prefix+'/sql-exchange/00000000000000000001/'+'b'.repeat(64)+'/output.bin';
 await buckets.execution.put(output,'expired');
 const host=await instantiateHost(module,createR2Transport(buckets),{clock:()=>now});
 const run=async(_env,input)=>host.request({...input,config:fixture.config},{deadlineMs:now+55000,maxOperations:900});
 const storage=collectionStorage();
 let control=new SQLControl({storage},{},run,{now:()=>now});
 await assert.rejects(control.collect(),/Canceled|DeadlineExceeded/);
 assert.equal(await storage.get('gc_lane'),1);
 assert.equal(buckets.execution.objects.has(output),true);
 now=await storage.getAlarm();
 control=new SQLControl({storage},{},run,{now:()=>now});
 await assert.rejects(control.collect(),/Canceled|DeadlineExceeded/);
 assert.equal(await storage.get('gc_lane'),2);
 assert.equal(buckets.execution.objects.has(output),false,'the slow first lane pinned healthy exchange collection after eviction');
});

test('WASM scalar V2 retains exact literal identity and prepares typed array parameters', async()=>{
 const fixture=JSON.parse(await readFile(new URL('../dist/sql-fixture.json',import.meta.url)));
 const buckets=Object.fromEntries(['source','execution'].map(name=>[name,new Bucket()]));
 for(const object of fixture.objects)await buckets[object.bucket].put(object.key,new Uint8Array(Buffer.from(object.body_base64,'base64')));
 const plan=JSON.parse(fixture.raw_plan);
 const numeric={kind:'number',element_type:'numeric'},boolean={kind:'boolean'},integer={kind:'integer',element_type:'int64'};
 plan.join.condition={version:2,instructions:[
  {type:numeric,operation:{parameter:0}},
  {type:numeric,operation:{literal:'EXACT_NUMERIC_LITERAL'}},
  {type:boolean,operation:{binary:{op:'eq',left:0,right:1}}},
  {type:{kind:'array',element_type:'int64'},operation:{parameter:1}},
  {type:integer,operation:{literal:1}},
  {type:integer,operation:{call:{function:'array_lower',args:[3,4]}}},
  {type:integer,operation:{literal:-2}},
  {type:boolean,operation:{binary:{op:'eq',left:5,right:6}}},
  {type:boolean,operation:{binary:{op:'and',left:2,right:7}}},
 ],root:8,output:boolean,parameters:[],parameter_values:[
  {value:'9007199254740993.1200',text:true},
  {value:{dimensions:[{length:3,lower_bound:-2}],values:['9007199254740993',null,'2'],sql_nulls:[false,true,false]}},
 ]};
 // Inject the original numeric token without a JavaScript Number conversion.
 const raw=JSON.stringify(plan).replace('"EXACT_NUMERIC_LITERAL"','9007199254740993.1200');
 const host=await instantiateHost(module,createR2Transport(buckets));
 const request={config:fixture.config,tenant:fixture.tenant,owner:'typed-parameters'};
 const submitted=await host.request({...request,op:'sql_submit',raw_plan:raw});
 const id=typeof submitted.query.id==='string'?submitted.query.id:String.fromCharCode(...submitted.query.id);
 const stored=buckets.execution.objects.get(`${fixture.config.state.prefix}/sql-execution-v2/${String(submitted.query.expires_ms).padStart(20,'0')}/${id}/plan.json`).body;
 assert.match(new TextDecoder().decode(stored),/"literal":9007199254740993\.1200/);
 assert.equal(id,Buffer.from(await crypto.subtle.digest('SHA-256',stored)).toString('hex'));
 request.query=submitted.query;
 let complete=false;
 for(let turn=0;turn<128;turn++){
  const result=await host.request({...request,op:'sql_turn'});
  if(result.complete){complete=true;break;}
  for(const ticket of result.tickets)assert.equal((await host.request({...request,op:'sql_execute',owner:'typed-worker',ticket})).status,'succeeded');
 }
 assert.ok(complete);
 assert.equal((await host.request({...request,op:'sql_result'})).rows,fixture.expected_rows??2);
});
