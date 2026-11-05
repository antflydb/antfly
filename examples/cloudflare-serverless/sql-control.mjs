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

const utf8=new TextEncoder();
const verificationKeys=new Map();
export function digestText(value){
 if(typeof value==='string'){if(!/^[a-f0-9]{64}$/.test(value))throw Error('InvalidSqlIdentity');return value;}
 if(!Array.isArray(value)||value.length!==64||value.some(c=>!Number.isInteger(c)||!(c>=48&&c<=57||c>=97&&c<=102)))throw Error('InvalidSqlIdentity');
 return String.fromCharCode(...value);
}
export function locatorName({tenant,query}){
 if(!Number.isSafeInteger(query?.expires_ms)||query.expires_ms<=0)throw Error('InvalidSqlIdentity');
 return `query:${digestText(tenant)}:${query.expires_ms}:${digestText(query.id)}`;
}
export function sqlConfig(env){
 const config=JSON.parse(env.SQL_CONFIG??'null');
 const names=JSON.parse(env.SQL_BUCKET_BINDINGS??'null');
 if(!config||!names||!env.SQL_TASKS||!env.SQL_COORDINATORS)throw Error('SqlDeploymentRequired');
 const bindings=Object.create(null);
 for(const[bucket,name]of Object.entries(names)){
  if(typeof name!=='string'||!env[name]?.get||!env[name]?.put)throw Error('InvalidSqlBinding');
  bindings[bucket]=env[name];
 }
 for(const lane of [config.state,config.exchange,config.artifacts])if(!lane||typeof lane.prefix!=='string'||!lane.prefix||lane.prefix.length>512||lane.prefix.includes('..')||!bindings[lane.bucket])throw Error('InvalidSqlBinding');
 if(!Array.isArray(config.source_buckets)||config.source_buckets.some(bucket=>!bindings[bucket]))throw Error('InvalidSqlBinding');
 return {config,bindings};
}
const decode=value=>Uint8Array.from(atob(value.replaceAll('-','+').replaceAll('_','/')),c=>c.charCodeAt(0));
export async function authenticateSQL(request,env,now=Date.now()){
 try{
  if(typeof env.SQL_SERVICE_SECRET!=='string'||utf8.encode(env.SQL_SERVICE_SECRET).length<32||!env.SQL_SERVICE_ISSUER)return false;
  const token=request.headers.get('X-Antfly-Trusted-Principal');
  if(!token||token.length>4096)return false;
  const parts=token.split('.');if(parts.length!==3||parts.some(p=>!p||!/^[A-Za-z0-9_-]+$/.test(p)))return false;
  const header=JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(decode(parts[0])));
  if(header.alg!=='HS256'||header.typ!=='JWT')return false;
  const signature=decode(parts[2]);if(signature.length!==32)return false;
  if(!verificationKeys.has(env.SQL_SERVICE_SECRET)){if(verificationKeys.size>=8)verificationKeys.delete(verificationKeys.keys().next().value);verificationKeys.set(env.SQL_SERVICE_SECRET,crypto.subtle.importKey('raw',utf8.encode(env.SQL_SERVICE_SECRET),{name:'HMAC',hash:'SHA-256'},false,['verify']));}
  const key=await verificationKeys.get(env.SQL_SERVICE_SECRET);
  if(!await crypto.subtle.verify('HMAC',key,signature,utf8.encode(parts[0]+'.'+parts[1])))return false;
  const p=JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(decode(parts[1]))),seconds=Math.floor(now/1000);
  return p.iss===env.SQL_SERVICE_ISSUER&&p.aud==='antfly-internal-v1'&&p.principal_kind==='service'&&p.admin===true&&typeof p.sub==='string'&&p.sub.length>0&&p.sub.length<=256&&Number.isSafeInteger(p.iat)&&Number.isSafeInteger(p.exp)&&p.iat<=seconds&&p.exp>seconds&&p.exp>p.iat&&p.exp-p.iat<=60;
 }catch{return false;}
}
export async function boundedSQLBody(request,reservation){
 if(Number(request.headers.get('content-length')??0)>1024*1024)throw Error('SqlRequestTooLarge');
 const reader=request.body?.getReader();if(!reader)throw Error('SqlRequestRequired');
 const chunks=[];let size=0;
 try{while(true){const{done,value}=await reader.read();if(done)break;size+=value.length;if(size>1024*1024){await reader.cancel();throw Error('SqlRequestTooLarge');}reservation?.add(value.length);chunks.push(value);}}catch(error){try{await reader.cancel();}catch{}throw error;}finally{reader.releaseLock();}
 const bytes=new Uint8Array(size);let at=0;for(const chunk of chunks){bytes.set(chunk,at);at+=chunk.length;}
 return new TextDecoder('utf-8',{fatal:true}).decode(bytes);
}
export const retiredSQLDelivery=error=>/SqlQueryExpired|SqlExecutionFenced|SqlExecutionNotFound|SqlExecutionFailed/.test(String(error));
export class SQLControl {
 constructor(ctx,env,run,{now=()=>Date.now(),owner=crypto.randomUUID()}={}){this.ctx=ctx;this.env=env;this.run=run;this.now=now;this.owner=owner;this.active=false;this.collectAgain=false;this.updates=Promise.resolve();}
 // Serialize only storage metadata, never a turn or external network I/O.
 update(action){const next=this.updates.then(action);this.updates=next.catch(()=>{});return next;}
 async schedule(time){return this.update(async()=>{const previous=await this.ctx.storage.getAlarm();if(previous===null||time<previous)await this.ctx.storage.setAlarm(time);});}
 async retire(){return this.update(async()=>{await this.ctx.storage.deleteAll();await this.ctx.storage.deleteAlarm();});}
 async kick(locator){
  locatorName(locator);
  return this.update(async()=>{
   const previous=await this.ctx.storage.get('query');
   if(previous&&locatorName(previous)!==locatorName(locator))throw Error('SqlCoordinatorIdentityMismatch');
   await this.ctx.storage.put('query',locator);
   const alarm=await this.ctx.storage.getAlarm(),next=this.now()+1000;
   if(alarm===null||next<alarm)await this.ctx.storage.setAlarm(next);
  });
 }
 async advance(locator){
  if(locator)await this.kick(locator);
  if(this.active){await this.schedule(this.now()+1000);return{complete:false};}
  this.active=true;
  try{
   locator=await this.ctx.storage.get('query');
   if(!locator){await this.retire();return{complete:true};}
   if(locator.query.expires_ms<=this.now()){await this.retire();return{complete:true};}
   // Schedule recovery before entering WASM or delivering any queue message.
   await this.schedule(this.now()+2000);
   const result=await this.run(this.env,{op:'sql_turn',...locator,owner:this.owner});
   for(let i=0;i<result.tickets.length;i+=100)await this.env.SQL_TASKS.sendBatch(result.tickets.slice(i,i+100).map(body=>({body,contentType:'json'})));
   if(result.complete)await this.retire();
   return{complete:result.complete};
  }catch(error){if(retiredSQLDelivery(error)){await this.retire();return{complete:true};}throw error;}
  finally{this.active=false;}
 }
 async alarm(){
  // The runtime consumes the firing alarm. Deleting it here suppresses its
  // automatic retries. Arm recovery before even the first durable-state read.
  await this.schedule(this.now()+2000);
  if(await this.ctx.storage.get('collection'))await this.collect(true);
  else if(await this.ctx.storage.get('discovery'))await this.recover();else await this.advance();
 }
 // The named collector DO shares the deployed class/binding, but owns separate
 // durable state, alarms, and an active guard from query/discovery objects.
 async collect(fromAlarm=false){
  // A recovery alarm overlapping its own slow pass is not new work. Only an
  // external cron/RPC nudge requests a followup after a drained result.
  if(this.active){if(!fromAlarm)this.collectAgain=true;await this.schedule(this.now()+1000);return{deleted:0,more:true};}
  this.active=true;this.collectAgain=false;
  try{
   await this.ctx.storage.put('collection',true);
   // Retain a continuation before kernel/R2 I/O, including on eviction/failure.
   await this.schedule(this.now()+2000);
   const notBefore=await this.ctx.storage.get('gc_not_before')??0;
   if(notBefore>this.now()){
    await this.update(()=>this.ctx.storage.setAlarm(notBefore));
    return{deleted:0,more:true,deferred:true};
   }
   const lane=await this.ctx.storage.get('gc_lane')??0;
   if(!Number.isInteger(lane)||lane<0||lane>=3)throw Error('InvalidSqlCollectionLane');
   // Advance durably before external I/O, even if an unabortable operation
   // consumes the whole invocation. Eviction/failure cannot pin a slow lane.
   await this.ctx.storage.put('gc_lane',(lane+1)%3);
   const result=await this.run(this.env,{op:'sql_collect',tenant:Array(64).fill(97),owner:this.owner,collection_lane:lane});
   if(!Number.isSafeInteger(result?.deleted)||result.deleted<0||result.more!==undefined&&typeof result.more!=='boolean')throw Error('InvalidSqlCollectionResult');
   // Compatibility with a pre-progress kernel is conservative: a nonempty
   // pass may have filled only one of its independently bounded lanes.
   const more=(result.more??result.deleted>0)||this.collectAgain;
   await this.update(async()=>{
    await this.ctx.storage.delete('gc_failures');
    await this.ctx.storage.delete('gc_not_before');
    if(!more)await this.ctx.storage.deleteAlarm();
   });
   if(more)await this.schedule(this.now()+1);
   return{...result,more};
  }catch(error){
   // Backoff is durable: a cron nudge or eviction cannot turn a failing store
   // into a hot loop. If metadata itself fails, the prearmed alarm/platform
   // retry or the next cron repairs it; never delete a firing alarm on failure.
   try{await this.update(async()=>{
    const old=await this.ctx.storage.get('gc_failures');
    const failures=Math.min(6,(Number.isSafeInteger(old)&&old>=0?old:0)+1);
    const next=this.now()+Math.min(30000,1000*2**(failures-1));
    await this.ctx.storage.put('collection',true);
    await this.ctx.storage.put('gc_failures',failures);
    await this.ctx.storage.put('gc_not_before',next);
    await this.ctx.storage.setAlarm(next);
   });}catch{}
   throw error;
  }finally{this.active=false;}
 }
 async recover(){
  if(this.active){await this.schedule(this.now()+1000);return{recovered:0};}
  this.active=true;
  try{
   // Persist the continuation before I/O: eviction or a failed list/kick must
   // not turn the minute cron into the throughput limit for discovery.
   await this.ctx.storage.put('discovery',true);
   await this.schedule(this.now()+1000);
   const {config,bindings}=sqlConfig(this.env),bucket=bindings[config.state.bucket],prefix=config.state.prefix+'/sql-query-expiry/';
   const cursor=await this.ctx.storage.get('cursor');
   const page=await bucket.list({prefix,limit:16,...(cursor?{cursor}:{})});
   let recovered=0,next=0;
   // Bound both state reads and locator RPCs. One slow query does not serialize
   // the entire page, and a continuation never fans out beyond four queries.
   await Promise.all(Array.from({length:Math.min(4,page.objects.length)},async()=>{
    while(next<page.objects.length){
     const count=await this.recoverQuery(page.objects[next++],prefix,config,bucket);
     recovered+=count;
    }
   }));
   if(page.truncated){
    await this.ctx.storage.put('cursor',page.cursor);
    // Drain at the speed of storage, with a new bounded invocation per page.
    await this.schedule(this.now()+1);
   }else{await this.ctx.storage.delete('cursor');await this.ctx.storage.deleteAlarm();}
   return{recovered};
  }finally{this.active=false;}
 }
 async recoverQuery(object,prefix,config,bucket){
  try{
   const suffix=object.key.slice(prefix.length),match=/^(\d{20})\/([a-f0-9]{64})\.json$/.exec(suffix);if(!match)return 0;
   const expires_ms=Number(match[1]);if(!Number.isSafeInteger(expires_ms)||expires_ms<=this.now())return 0;
   const base=`${config.state.prefix}/sql-execution-v2/${match[1]}/${match[2]}`;
   const query={id:match[2],expires_ms};
   const record=await bucket.get(`${base}/query.json`);
   if(record){
    if(record.size>16384)throw Error('InvalidSqlExecutionState');
    const state=await record.json();
    if(state.version!==2||digestText(state.plan)!==query.id||state.expires_ms!==expires_ms||!['pending','running','succeeded','failed','cancelled'].includes(state.status))throw Error('InvalidSqlExecutionState');
    if(['succeeded','failed','cancelled'].includes(state.status)){await bucket.delete(object.key);return 0;}
    // Compact records are trusted execution metadata, as on native workers.
    // Validate the scope before waking its DO; only legacy records need a plan.
    if(state.tenant!=null){
     const locator={tenant:digestText(state.tenant),query};
     await this.env.SQL_COORDINATORS.getByName(locatorName(locator)).kick(locator);return 1;
    }
   }
   const raw=await bucket.get(`${base}/plan.json`);if(!raw)return 0;
   if(raw.size>1024*1024)throw Error('InvalidSqlDistributedPlan');
   const bytes=new Uint8Array(await new Response(raw.body).arrayBuffer());
   const digest=Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',bytes)),b=>b.toString(16).padStart(2,'0')).join('');
   if(digest!==query.id)throw Error('InvalidSqlDistributedPlan');
   const plan=JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(bytes));
   if(plan.expires_ms!==expires_ms)throw Error('InvalidSqlDistributedPlan');
   const locator={tenant:digestText(plan.tenant),query};
   await this.env.SQL_COORDINATORS.getByName(locatorName(locator)).kick(locator);return 1;
  }catch(error){console.warn('SQL query recovery deferred',String(error));return 0;}
 }
}
export async function consumeSQL(batch,env,run,{now=()=>Date.now()}={}){
 for(const message of batch.messages){
  try{
   const ticket=message.body,locator={tenant:ticket.tenant,query:ticket.query};locatorName(locator);
   if(ticket.query.expires_ms<=now()){message.ack();continue;}
   const result=await run(env,{op:'sql_execute',...locator,ticket,owner:crypto.randomUUID()});
   if(['succeeded','failed','cancelled'].includes(result.status)){
    // ACK only the durable accepted/terminal record, then wake orchestration.
    await env.SQL_COORDINATORS.getByName(locatorName(locator)).kick(locator);message.ack();
   }else message.retry({delaySeconds:Math.min(120,Math.max(1,Math.ceil((result.until_ms+30000-now())/1000)))});
  }catch(error){if(retiredSQLDelivery(error))message.ack();else message.retry({delaySeconds:2});}
 }
}

// Separate waitUntil jobs prevent discovery failures or slow pages from
// suppressing collector admission. Each owner persists its own continuation.
export function scheduleSQLMaintenance(env,ctx){
 ctx.waitUntil(Promise.resolve().then(()=>env.SQL_COORDINATORS.getByName('discovery').recover()));
 ctx.waitUntil(Promise.resolve().then(()=>env.SQL_COORDINATORS.getByName('collector').collect()));
}
