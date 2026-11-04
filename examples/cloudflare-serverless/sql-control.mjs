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
const retired=error=>/SqlQueryExpired|SqlExecutionFenced|SqlExecutionNotFound|SqlExecutionFailed/.test(String(error));
export class SQLControl {
 constructor(ctx,env,run,{now=()=>Date.now(),owner=crypto.randomUUID()}={}){this.ctx=ctx;this.env=env;this.run=run;this.now=now;this.owner=owner;this.active=false;}
 async retire(){await this.ctx.storage.deleteAlarm();await this.ctx.storage.deleteAll();}
 async kick(locator){
  locatorName(locator);
  const previous=await this.ctx.storage.get('query');
  if(previous&&locatorName(previous)!==locatorName(locator))throw Error('SqlCoordinatorIdentityMismatch');
  await this.ctx.storage.put('query',locator);
  await this.ctx.storage.setAlarm(this.now()+1000);
 }
 async advance(locator){
  if(locator)await this.kick(locator);
  if(this.active)return{complete:false};
  this.active=true;
  try{
   locator=await this.ctx.storage.get('query');
   if(!locator)return{complete:true};
   if(locator.query.expires_ms<=this.now()){await this.retire();return{complete:true};}
   // Schedule recovery before entering WASM or delivering any queue message.
   await this.ctx.storage.setAlarm(this.now()+2000);
   const result=await this.run(this.env,{op:'sql_turn',...locator,owner:this.owner});
   for(let i=0;i<result.tickets.length;i+=100)await this.env.SQL_TASKS.sendBatch(result.tickets.slice(i,i+100).map(body=>({body,contentType:'json'})));
   if(result.complete)await this.retire();
   return{complete:result.complete};
  }catch(error){if(retired(error)){await this.retire();return{complete:true};}throw error;}
  finally{this.active=false;}
 }
 async alarm(){await this.advance();}
 async recover(){
  const {config,bindings}=sqlConfig(this.env),bucket=bindings[config.state.bucket],prefix=config.state.prefix+'/sql-query-expiry/';
  const cursor=await this.ctx.storage.get('cursor');
  const page=await bucket.list({prefix,limit:16,...(cursor?{cursor}:{})});
  let recovered=0;
  for(const object of page.objects){
   try{
    const suffix=object.key.slice(prefix.length),match=/^(\d{20})\/([a-f0-9]{64})\.json$/.exec(suffix);if(!match)continue;
    const expires_ms=Number(match[1]);if(!Number.isSafeInteger(expires_ms)||expires_ms<=this.now())continue;
    const raw=await bucket.get(`${config.state.prefix}/sql-execution-v2/${match[1]}/${match[2]}/plan.json`);if(!raw)continue;
    if(raw.size>1024*1024)throw Error('InvalidSqlDistributedPlan');
    const bytes=new Uint8Array(await new Response(raw.body).arrayBuffer());
    const digest=Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',bytes)),b=>b.toString(16).padStart(2,'0')).join('');
    if(digest!==match[2])throw Error('InvalidSqlDistributedPlan');
    const plan=JSON.parse(new TextDecoder().decode(bytes));
    if(plan.expires_ms!==expires_ms)throw Error('InvalidSqlDistributedPlan');
    const locator={tenant:plan.tenant,query:{id:Array.from(match[2],c=>c.charCodeAt(0)),expires_ms}};
    await this.env.SQL_COORDINATORS.getByName(locatorName(locator)).kick(locator);recovered++;
   }catch(error){console.warn('SQL query recovery deferred',String(error));}
  }
  if(page.truncated)await this.ctx.storage.put('cursor',page.cursor);else await this.ctx.storage.delete('cursor');
  return{recovered};
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
  }catch(error){if(retired(error))message.ack();else message.retry({delaySeconds:2});}
 }
}
