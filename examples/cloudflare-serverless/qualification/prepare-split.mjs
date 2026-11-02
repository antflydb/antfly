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

// Reuse only the approved disposable app identity/token; private services have
// no public API credential and no workers.dev endpoint.
import{readFile,writeFile,mkdir}from"node:fs/promises";import{resolve}from"node:path";
const base=JSON.parse(await readFile(process.argv[2],"utf8"));if(!/^antfly-1278-[a-z0-9-]+$/.test(base.id)||base.worker!==base.id||base.bucket!==base.id||!/^[a-f0-9]{32}$/.test(base.account))throw Error("invalid disposable manifest");
const suffix=Date.now().toString(36),dir=resolve("dist/qualification",base.id+"-split-"+suffix);await mkdir(dir,{recursive:true});
const names={app:base.id,writer:base.id+"-writer",reader:base.id+"-reader",builder:base.id+"-builder"},services=[];
const entry=dir+"/app.mjs";await writeFile(entry,`import app from ${JSON.stringify(resolve("qualification/worker.mjs"))};import{configureKernels}from ${JSON.stringify(resolve("kernels.mjs"))};configureKernels("app");export default app;`);
for(const role of["writer","reader","builder","app"]){
 const config=JSON.parse(await readFile("wrangler.split-"+role+".jsonc","utf8"));Object.assign(config,{name:names[role],account_id:base.account,main:role==="app"?entry:resolve(config.main),observability:{enabled:true,head_sampling_rate:1}});
 if(config.assets)config.assets.directory=resolve("public");config.r2_buckets[0].bucket_name=base.id;delete config.triggers;
 const incarnation="qualification-"+base.id+"-"+suffix;Object.assign(config.vars,{ANTFLY_INCARNATION:incarnation,ANTFLY_TENANTS:JSON.stringify(Object.fromEntries(["a","b","c","d"].map(t=>[t,{incarnation:incarnation+"-"+t,storage_id:incarnation+"-store-"+t}]))),PAGED_TEXT:"true",PUBLICATION_RECORDS:"64",LOG_METRICS:"true",NATIVE_WAL_COMMIT:"true",AUTO_GC:"false"});
 for(const binding of config.durable_objects.bindings)if(binding.script_name)binding.script_name=Object.values(names).find(name=>name===names[{COMMITS:"writer",READERS:"reader",BUILDERS:"builder"}[binding.name]]);
 for(const producer of config.queues?.producers??[])producer.queue=base.id;for(const consumer of config.queues?.consumers??[]){consumer.queue=base.id;consumer.dead_letter_queue=base.id+"-dead";}
 const path=dir+"/"+role+".json";await writeFile(path,JSON.stringify(config,null,2));services.push({role,name:names[role],config:path});
}
const run={...base,dir,config:services.find(s=>s.role==="app").config,entry,services};await writeFile(dir+"/run.json",JSON.stringify(run,null,2));console.log(dir+"/run.json");
