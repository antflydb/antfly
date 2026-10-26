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

// Qualification-only routing: fresh physical actors over the SAME fenced R2
// authority. Epochs are bounded; platform isolate eviction is not implied.
import{placementEnv}from"../runtime-placement.mjs";
import{tenantEnv,tenantEnvs}from"../tenants.mjs";
import{runtimeName,readerSlots}from"../runtime-routing.mjs";
const encode=(epoch,name)=>epoch===0?name:JSON.stringify(["qualification-physical-v1",epoch,name]);
export function epochEnv(env,epoch=0){
 env=placementEnv(env);
 if(!env.ANTFLY_INCARNATION.startsWith("qualification-")||!Number.isInteger(epoch)||epoch<0||epoch>100)throw Error("InvalidQualificationEpoch");
 const result={...env,__ANTFLY_PHYSICAL_EPOCH:epoch};for(const name of["COMMITS","READERS","BUILDERS"]){const binding=env[name];if(!binding)continue;result[name]=new Proxy(binding,{get(target,key){if(key==="idFromName"||key==="getByName")return (value,...args)=>target[key](encode(epoch,value),...args);const value=target[key];return typeof value==="function"?value.bind(target):value;}});}
 return result;
}
export function actorEpochEnv(ctx,env,role){
 env=placementEnv(env);
 const binding=env[{writer:"COMMITS",reader:"READERS",builder:"BUILDERS"}[role]],names=tenantEnvs(env).flatMap(candidate=>role==="writer"?[candidate.ANTFLY_INCARNATION]:Array.from({length:role==="reader"?readerSlots(candidate):1},(_,slot)=>runtimeName(candidate.ANTFLY_INCARNATION,slot)));
 for(let epoch=0;epoch<=100;epoch++)for(const name of names)if(binding.idFromName(encode(epoch,name)).equals(ctx.id))return epochEnv(env,epoch);
 throw Error("UnknownQualificationActor");
}
// One bounded timing snapshot and one error, with no request bodies or tokens.
export function withPublicationDiagnostics(Base){return class extends Base{
 async execute(input,collection,meta){
  if(input?.op!=="publish")return super.execute(input,collection,meta);
  const start=performance.now();try{return await super.execute(input,collection,meta);}catch(error){this.qualificationLastError={at:Date.now(),elapsed_ms:performance.now()-start,error:String(error),trace_id:meta?.trace_id??null};throw error;}
 }
 async perform(input,collection,signal,trace){
  if(input?.op!=="publish")return super.perform(input,collection,signal,trace);
  const start=performance.now();let summary;
  try{const result=await super.perform(input,collection,signal,trace);summary={published:result.published,consumed_records:result.consumed_records,wal_end_lsn:result.wal_end_lsn,has_more:result.has_more};return result;}
  catch(error){summary={error:String(error)};throw error;}
  finally{this.qualificationLastExecution={at:Date.now(),elapsed_ms:performance.now()-start,result:summary,trace:trace.report()};}
 }
 publicationDiagnostics(){return{last_execution:this.qualificationLastExecution??null,last_error:this.qualificationLastError??null};}
};}
export function qualificationApp(app){return{...app,async fetch(request,env,ctx){
 const path=new URL(request.url).pathname;
 if(path.startsWith("/_qualification/")&&(!env.API_TOKEN||request.headers.get("Authorization")!=="Bearer "+env.API_TOKEN))return new Response("unauthorized",{status:401});
 if(path==="/_qualification/publication"){
  if(request.method!=="POST")return new Response("method not allowed",{status:405});
  const selected=tenantEnv(epochEnv(env,Number(request.headers.get("X-Antfly-Physical-Epoch")??0)),request.headers.get("X-Antfly-Tenant")??undefined);
  const[budget,publication]=await Promise.all([selected.COMMITS.getByName(selected.ANTFLY_INCARNATION).publicationBudget(Number(selected.PUBLICATION_RECORDS??16)),selected.BUILDERS.getByName(runtimeName(selected.ANTFLY_INCARNATION,0)).publicationDiagnostics()]);
  return Response.json({budget,publication},{headers:{"X-Antfly-Version":env.WORKER_VERSION.id}});
 }
 if(path==="/_qualification/queue"){
  if(request.method!=="POST"||!env.ANTFLY_INCARNATION.startsWith("qualification-")||request.headers.get("Authorization")!=="Bearer "+env.API_TOKEN)return new Response("unauthorized",{status:401});
  const[m,dead]=await Promise.all([env.MAINTENANCE.metrics(),env.DEAD_LETTERS.metrics()]);return Response.json({dead_letter_count:dead.backlogCount,backlog_count:m.backlogCount,backlog_bytes:m.backlogBytes,oldest_ms:m.oldestMessageTimestamp?Math.max(0,Date.now()-m.oldestMessageTimestamp.getTime()):0},{headers:{"X-Antfly-Version":env.WORKER_VERSION.id}});
 }
 return app.fetch(request,epochEnv(env,Number(request.headers.get("X-Antfly-Physical-Epoch")??0)),ctx);
},queue(batch,env,ctx){return app.queue(batch,epochEnv(env,0),ctx);},scheduled(controller,env,ctx){return app.scheduled(controller,epochEnv(env,0),ctx);}};}
