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

import{readFile,writeFile,mkdir}from"node:fs/promises";
import{resolve}from"node:path";
import{fileURLToPath}from"node:url";
import{randomBytes,createHash}from"node:crypto";
import{spawnSync}from"node:child_process";
const account=process.argv[2],baseline=resolve(process.argv[3]??""),candidate=resolve(fileURLToPath(new URL("../",import.meta.url))),count=Number(process.argv[4]??3);
if(!/^[a-f0-9]{32}$/.test(account??"")||!Number.isInteger(count)||count<1||count>3)throw Error("usage: prepare-gate ACCOUNT BASELINE_WORKTREE [1..3]");
const tag=Date.now().toString(36)+randomBytes(2).toString("hex"),dir=resolve(candidate,"dist/qualification/gate-"+tag);await mkdir(dir,{recursive:true});
const gate={schema:1,id:"gate-"+tag,dir,account,trials:[]};
for(let trial=1;trial<=count;trial++){
 const pair={trial};
 for(const kind of["baseline","candidate"]){
  const source=kind==="baseline"?resolve(baseline,"examples/cloudflare-serverless"):candidate,id="antfly-1278-g-"+tag+"-"+trial+"-"+kind[0],runDir=resolve(dir,id);await mkdir(runDir,{recursive:true});
  const names={app:id,writer:id+"-writer",reader:id+"-reader",builder:id+"-builder"},classes={writer:"R2CommitCoordinator",reader:"R2ReadRuntime",builder:"R2BuildRuntime"},services=[];
  const incarnation="qualification-"+id,vars={EDGE_IMMUTABLE_CACHE_SCOPE:crypto.randomUUID(),ANTFLY_INCARNATION:incarnation,COORDINATOR_BACKEND:"r2",WAL_INLINE_TAIL:"false",SPLIT_RUNTIME:"true",AUTO_GC:"true",READERS_PER_TENANT:"2",PAGED_TEXT:"true",PAGED_PROJECTIONS:"true",PUBLICATION_RECORDS:"64",PUBLICATION_SLICE_MS:"2000",ANN_COARSE_PROBES:"0",LOG_METRICS:"true",ANTFLY_TENANTS:JSON.stringify(Object.fromEntries(["a","b","c","d"].map(tenant=>[tenant,{incarnation:incarnation+"-"+tenant,storage_id:incarnation+"-store-"+tenant}])))};
  for(const role of["writer","reader","builder","app"]){
   const config=JSON.parse(await readFile(resolve(source,"wrangler.split-"+role+".jsonc"))),entry=resolve(runDir,role+".mjs"),helper=resolve(candidate,"qualification/physical-epochs.mjs");let body;
   if(role==="app")body='import app from '+JSON.stringify(resolve(source,"qualification/worker.mjs"))+';import{configureKernels}from '+JSON.stringify(resolve(source,"kernels.mjs"))+';import{qualificationApp}from '+JSON.stringify(helper)+';configureKernels("app");export default qualificationApp(app);';
   else body='import handler,{'+classes[role]+' as Base}from '+JSON.stringify(resolve(source,role+"-worker.mjs"))+';import{actorEpochEnv,withPublicationDiagnostics}from '+JSON.stringify(helper)+';export class '+classes[role]+' extends '+(role==='builder'?'withPublicationDiagnostics(Base)':'Base')+'{constructor(ctx,env){super(ctx,actorEpochEnv(ctx,env,'+JSON.stringify(role)+'));}}export default handler;';
   await writeFile(entry,body);Object.assign(config,{name:names[role],account_id:account,main:entry,vars,observability:{enabled:true,head_sampling_rate:1}});config.r2_buckets[0].bucket_name=id;if(config.assets)config.assets.directory=resolve(source,"public");
   for(const binding of config.durable_objects.bindings)if(binding.script_name)binding.script_name=names[{COMMITS:"writer",READERS:"reader",BUILDERS:"builder"}[binding.name]];
   for(const p of config.queues?.producers??[])p.queue=id;if(role==="app")config.queues.producers.push({binding:"DEAD_LETTERS",queue:id+"-dead"});for(const c of config.queues?.consumers??[]){c.queue=id;c.dead_letter_queue=id+"-dead";}
   const path=resolve(runDir,role+".json");await writeFile(path,JSON.stringify(config,null,2));services.push({role,name:names[role],config:path,entry});
  }
  const token=resolve(runDir,"api-token");await writeFile(token,randomBytes(32).toString("hex"),{mode:0o600});const revision=spawnSync("git",["-C",source,"rev-parse","HEAD"],{encoding:"utf8"});if(revision.status!==0)throw Error("source revision unavailable");
  const run={id,account,dir:runDir,worker:id,bucket:id,queues:[id,id+"-dead"],token_file:token,config:services.find(s=>s.role==="app").config,entry:services.find(s=>s.role==="app").entry,services,trial,kind,source,revision:revision.stdout.trim(),reader_wasm_sha256:createHash("sha256").update(await readFile(resolve(source,"dist/antfly-serverless-compact.wasm"))).digest("hex"),wasm_sha256:createHash("sha256").update(await readFile(resolve(source,"dist/antfly-serverless.wasm"))).digest("hex")};
  if(kind==="candidate")run.observation_wasm_sha256=createHash("sha256").update(await readFile(resolve(source,"dist/antfly-serverless-observe.wasm"))).digest("hex");
  const path=resolve(runDir,"run.json");await writeFile(path,JSON.stringify(run,null,2));pair[kind]=path;
 }
 gate.trials.push(pair);
}
await writeFile(resolve(dir,"gate.json"),JSON.stringify(gate,null,2));console.log(resolve(dir,"gate.json"));
