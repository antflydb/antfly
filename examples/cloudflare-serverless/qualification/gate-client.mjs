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

import{Agent}from'undici';
import{assertGateRun}from'./gate-scope.mjs';
import{verifySealedRun}from'./gate-provenance.mjs';
import {readFile} from 'node:fs/promises';
// Keep connections across the fixed 30-second arrivals, identically for both sides.
const transportPolicy=Object.freeze({keep_alive_ms:60000,max_connections:128});
const dispatcher=new Agent({connections:transportPolicy.max_connections,keepAliveTimeout:transportPolicy.keep_alive_ms,keepAliveMaxTimeout:transportPolicy.keep_alive_ms});
const pooledFetch=(url,options)=>globalThis.fetch(url,{...options,dispatcher});
// Replaying an acknowledged setup is safe only before any workload mutation.
// This avoids adding another corpus-sized WAL prefix after a setup interruption.
export function resumeSeedExecution(report,baseline,candidate){
 if(report?.schema!==1||report.documents!==512||report.soak_ms!==0||report.workload||!report.fatal||report.baseline?.converged===true&&report.candidate?.converged===true||!/^[a-f0-9]{8}-(?:[a-f0-9]{4}-){3}[a-f0-9]{12}$/.test(report.execution_id??'')||report.resumed_seed_execution_id&&!/^[a-f0-9]{8}-(?:[a-f0-9]{4}-){3}[a-f0-9]{12}$/.test(report.resumed_seed_execution_id))throw Error('invalid setup resume evidence');
 for(const[name,run]of[['baseline',baseline],['candidate',candidate]]){
  if(run.last_execution_id!==report.execution_id||run.last_phase!=='setup')throw Error('setup is no longer the latest corpus execution');
  const side=report[name];
  if(side?.run!==run.id||side.execution_id!==report.execution_id||side.wasm_sha256!==run.wasm_sha256||side.reader_wasm_sha256!==run.reader_wasm_sha256||!Array.isArray(side.cold)||side.cold.length||side.verification||!Array.isArray(side.resource_errors)||side.resource_errors.length||!Array.isArray(side.errors)||side.errors.some(e=>e.phase!=='converge')||side.samples?.some(s=>!['seed','converge'].includes(s.phase)))throw Error('setup has workload or incompatible evidence');
  const seeds=side.samples?.filter(s=>s.phase==='seed')??[];
  if(seeds.length!==32||!['a','b','c','d'].every(tenant=>seeds.filter(s=>s.tenant===tenant&&s.op==='mutate').length===8))throw Error('setup acknowledgements incomplete');
 }
 return report.resumed_seed_execution_id??report.execution_id;
}
export class GateClient {
 static async open(path){const run=JSON.parse(await readFile(path));assertGateRun(run);const errors=await verifySealedRun(run);if(errors.length)throw Error(errors.join('; '));if(run.deployed!==true||run.deleted||!run.token_installed||!run.origin||!run.version||!/^antfly-1278-g-/.test(run.id)||!new URL(run.origin).hostname.startsWith(run.id+'.')||!new URL(run.origin).hostname.endsWith('.workers.dev'))throw Error('invalid ready gate profile');return new GateClient(run,(await readFile(run.token_file,'utf8')).trim());}
 constructor(run,token,{fetch:fetchImpl=pooledFetch}={}){this.fetch=fetchImpl;this.run=run;this.token=token;this.executionId=crypto.randomUUID();this.samples=[];this.errors=[];this.receipts={};this.seeds={};this.expected=new Map();this.documentLsns=new Map();this.lag=[];this.queue=[];this.cold=[];this.resourceErrors=[];}
 async call(tenant,input,phase,{epoch=this.physicalEpochs?.[tenant]??0,scheduled=Date.now(),path='/api'}={}){
  let attempts=0,last;const started=Date.now();
  const idempotent=["lookup","list","search","count","status","latest"].includes(input.op)||input.op==="mutate"&&typeof input.operation_id==="string"&&input.operation_id.length>0||path==="/_qualification/queue";
  const networkFailure=error=>["TimeoutError","AbortError"].includes(error?.name)||error instanceof TypeError&&/^fetch failed$|^network error$/i.test(error.message)||["ECONNRESET","ETIMEDOUT","EAI_AGAIN","UND_ERR_SOCKET","UND_ERR_CONNECT_TIMEOUT","UND_ERR_HEADERS_TIMEOUT"].includes(error?.cause?.code);
  try{for(;attempts<20&&Date.now()-scheduled<60000;attempts++){
   let response,body;try{response=await this.fetch(new URL(path,this.run.origin),{method:'POST',headers:{Authorization:'Bearer '+this.token,'Content-Type':'application/json','X-Antfly-Tenant':tenant,'X-Antfly-Physical-Epoch':String(epoch)},body:JSON.stringify(input),signal:AbortSignal.timeout(Math.max(1,Math.min(35000,scheduled+60000-Date.now())))});
   body=await response.text();}catch(error){if(phase!=="cold"&&idempotent&&networkFailure(error)){await new Promise(r=>setTimeout(r,250));continue;}throw error;}let value;try{value=JSON.parse(body);}catch{throw Error('non-JSON response: '+response.status);}
   last=value;if(/OutOfMemory|SQLITE_NOMEM|MemoryLimitExceeded/.test(value.error??''))this.resourceErrors.push({phase,error:value.error});
   if(phase!=="cold"&&(response.status===503||response.status===409&&/WorkLeaseLost|WalCommitContended|PublicationConflict|PreconditionFailed/.test(value.error??''))){await new Promise(r=>setTimeout(r,250));continue;}
   if(!response.ok)throw Error('HTTP '+response.status+': '+(value.error??'unknown'));
   const version=response.headers.get('X-Antfly-Version');if(version!==this.run.version)throw Error('mixed version: '+version);
   const finished=Date.now(),sample={tenant,phase,op:input.op??path,ms:finished-scheduled,scheduled_at:scheduled,started_at:started,finished_at:finished,scheduling_delay_ms:Math.max(0,started-scheduled),execution_ms:finished-started,attempts:attempts+1,version,epoch};this.samples.push(sample);return{value,sample};
  }throw Error('admission exhausted: '+JSON.stringify(last));}catch(error){this.errors.push({phase,tenant,ms:Date.now()-scheduled,attempts:Math.min(attempts+1,20),error:String(error)});throw error;}
 }
 body(tenant,id,text='seed '+id){return JSON.stringify({tenant,text,payload:'x'.repeat(1024)});}
 recordCommit(tenant,id,body,input,value){
  if(value.committed!==true||typeof value.visibility!=='string'||!/[a-f0-9]{16}$/.test(value.visibility))throw Error('write was not durably committed');
  const lsn=BigInt('0x'+value.visibility.slice(-16)),key=tenant+'/'+id;
  // Fixed arrivals can overlap. HTTP completion order need not match durable
  // commit order; preserve each document's newest acknowledged write, and the
  // tenant's highest visibility fence, independently.
  if(!this.documentLsns.has(key)||lsn>this.documentLsns.get(key)){this.documentLsns.set(key,lsn);this.expected.set(key,body);}
  if(!this.receipts[tenant]||lsn>BigInt('0x'+this.receipts[tenant].slice(-16))){this.receipts[tenant]=value.visibility;this.latest??={};this.latest[tenant]={input,visibility:value.visibility};}
 }
 async seed(documents,{executionId=this.executionId}={}){
  if(!/^[a-f0-9]{8}-(?:[a-f0-9]{4}-){3}[a-f0-9]{12}$/.test(executionId))throw Error('invalid seed execution identity');
  const seeds=await Promise.allSettled(['a','b','c','d'].map(async tenant=>{
   for(let start=0;start<documents;start+=64){const input={op:'mutate',operation_id:'gate/'+executionId+'/seed/'+start,mutations:Array.from({length:Math.min(64,documents-start)},(_,i)=>{const id=start+i,body=this.body(tenant,id);this.expected.set(tenant+'/'+id,body);return{kind:'upsert',doc_id:'gate/'+id,body};})};const {value}=await this.call(tenant,input,'seed');this.receipts[tenant]=value.visibility;this.seeds[tenant]={input,visibility:value.visibility};}
  }));for(const result of seeds)if(result.status==='rejected')throw result.reason;await this.converge(1800000);
 }
 async round(index,documents,scheduled){
  const outcomes=await Promise.allSettled(['a','b','c','d'].map(async tenant=>{
   const id=index%documents,body=this.body(tenant,id,'round '+this.executionId+' '+index),receipt=this.receipts[tenant];
   const input={op:'mutate',operation_id:'gate/'+this.executionId+'/round/'+index,mutations:[{kind:'upsert',doc_id:'gate/'+id,body}]};
   const jobs=[this.call(tenant,input,'write',{scheduled}).then(({value})=>this.recordCommit(tenant,id,body,input,value))];
   for(let i=0;i<3;i++)jobs.push(this.call(tenant,{op:'lookup',doc_id:'gate/'+((index*37+i)%documents),required_visibility:receipt},'read',{scheduled}).then(({value})=>{const body=JSON.parse(value.body);if(body.tenant!==tenant||body.payload!=='x'.repeat(1024))throw Error('fenced read mismatch');}));
   if(index%4===0)jobs.push(this.call(tenant,{op:'compact'},'compact',{scheduled}));
   const results=await Promise.allSettled(jobs);for(const r of results)if(r.status==='rejected')throw r.reason;
  }));
  const failed=outcomes.find(outcome=>outcome.status==='rejected');if(failed){this.errors.push({phase:'round-correctness',round:index,error:String(failed.reason)});throw failed.reason;}
 }
 async progress(minute){
  await Promise.all(['a','b','c','d'].map(async tenant=>{const{value}=await this.call(tenant,{op:'status'},'lag');const latest=BigInt('0x'+value.visibility.slice(-16)),published=BigInt(value.published_lsn??0);this.lag.push({minute,tenant,lag:Number(latest-published),at:Date.now()});}));
  const{value}=await this.call('a',{},'queue',{path:'/_qualification/queue'});this.queue.push({minute,at:Date.now(),...value});
 }
 async converge(timeout=120000){
  const started=Date.now();for(;;){let done=true;for(const tenant of ['a','b','c','d']){const{value}=await this.call(tenant,{op:'status'},'converge');if(BigInt(value.published_lsn??0)<BigInt('0x'+this.receipts[tenant].slice(-16)))done=false;}if(done){this.converged=true;return;}if(Date.now()-started>timeout){this.converged=false;throw Error('publication did not converge');}await new Promise(r=>setTimeout(r,500));}
 }
 async verify(documents){
  let bodies=0,receipts=0;
  const outcomes=await Promise.allSettled(['a','b','c','d'].map(async tenant=>{
   const count=(await this.call(tenant,{op:'count',required_visibility:this.receipts[tenant]},'verify')).value;if(count.count!==String(documents))throw Error('count mismatch');bodies++;
   for(let base=0;base<documents;base+=4){
    const results=await Promise.allSettled(Array.from({length:Math.min(4,documents-base)},async(_,offset)=>{const id=base+offset,{value}=await this.call(tenant,{op:'lookup',doc_id:'gate/'+id,required_visibility:this.receipts[tenant]},'verify');if(value.body!==this.expected.get(tenant+'/'+id))throw Error('exact body mismatch: '+tenant+'/'+id);bodies++;}));
    for(const result of results)if(result.status==='rejected')throw result.reason;
   }
   for(const prior of[this.seeds[tenant],this.latest?.[tenant]].filter(Boolean)){const{value}=await this.call(tenant,prior.input,'receipt-replay');if(value.visibility!==prior.visibility)throw Error('receipt replay mismatch');receipts++;}
  }));
  for(const outcome of outcomes)if(outcome.status==='rejected')throw outcome.reason;
  this.verification={passed:true,bodies,receipts};
 }
 report(){return{execution_id:this.executionId,version:this.run.version,run:this.run.id,wasm_sha256:this.run.wasm_sha256,reader_wasm_sha256:this.run.reader_wasm_sha256,bundle_hashes:this.run.bundle_hashes,runtime_placement:this.run.runtime_placement??null,transport_policy:transportPolicy,samples:this.samples,errors:this.errors,resource_errors:this.resourceErrors,verification:this.verification,lag:this.lag,queue:this.queue,cold:this.cold,converged:this.converged??false};}
}
