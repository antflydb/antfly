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

// Controlled actual-workerd/WASM experiment. Injected delays characterize
// dependency depth, including preparation; they do not measure cloud SLOs.
// Use a new report directory to preserve the earlier sealed latency evidence.
import{build}from'esbuild';import{Miniflare,convertV4MiniflareOptions}from'miniflare';
import{readFile,writeFile,mkdir,mkdtemp,rm}from'node:fs/promises';import{resolve,join}from'node:path';import{createHash}from'node:crypto';import assert from'node:assert/strict';
const model=process.argv.find(a=>a.startsWith('--model='))?.slice(8)??'uniform';
if(!['uniform','bursty'].includes(model))throw Error('InvalidLifecycleDelayModel');
const root=resolve('.'),out=resolve('dist/qualification/runtime-plan-lab'),delay=Number(process.argv.find(a=>a.startsWith('--delay='))?.slice(8)??20),documents=Number(process.argv.find(a=>a.startsWith('--documents='))?.slice(12)??128);
if(!Number.isInteger(delay)||delay<0||delay>100||![64,128,512].includes(documents))throw Error('InvalidWorkerLatencyExperiment');await mkdir(out,{recursive:true});
const variants=[{name:'canonical',CANONICAL_POINT_BODIES:'true'},{name:'canonical-combined',CANONICAL_POINT_BODIES:'true',FUSED_READS:'true',OPTIMISTIC_WAL:'true',NATIVE_COMPACTION_PLAN:'true'},{name:'before',baseline:true},{name:'current'},{name:'fused',FUSED_READS:'true'},{name:'optimistic',OPTIMISTIC_WAL:'true'},{name:'inline',INLINE_POINT_BODIES:'true'},{name:'planned-compact',NATIVE_COMPACTION_PLAN:'true'},{name:'combined',FUSED_READS:'true',OPTIMISTIC_WAL:'true',INLINE_POINT_BODIES:'true',NATIVE_COMPACTION_PLAN:'true'}].filter(v=>!process.argv.some(a=>a.startsWith('--only='))||process.argv.find(a=>a.startsWith('--only=')).slice(7).split(',').includes(v.name));
if(!variants.length)throw Error('NoSelectedLifecycleVariants');
const outputName='lifecycle-latency-'+Date.now()+'.json';
const report={qualification:false,output_file:outputName,kind:'actual workerd + native WASM + local R2 binding',storage_delay_model:model,injected_io_delay_ms:delay,delay_includes_preparation:true,documents_per_tenant:documents,tenants:4,cases:[]},stats=a=>{a.sort((a,b)=>a-b);return{n:a.length,p50:a[Math.ceil(a.length*.5)-1]??0,p95:a[Math.ceil(a.length*.95)-1]??0,max:a.at(-1)??0};};
const moduleSets=new Map();
for(const baseline of new Set(variants.map(v=>!!v.baseline)))moduleSets.set(baseline,await Promise.all(['antfly-serverless.wasm','antfly-serverless-compact.wasm','antfly-serverless-observe.wasm'].map(async name=>({type:'CompiledWasm',path:join(root,'dist',name),contents:await readFile(join(baseline?join(out,'before'):join(root,'dist'),name))}))));
for(const variant of variants){
 const nativeModules=moduleSets.get(!!variant.baseline);
 const nativeHashes=Object.fromEntries(nativeModules.map(m=>[m.path.split('/').at(-1),createHash('sha256').update(m.contents).digest('hex')]));
 const bindings={API_TOKEN:'local-latency',ANTFLY_INCARNATION:'latency-'+variant.name,COORDINATOR_BACKEND:'r2',SPLIT_RUNTIME:'true',DIRECT_READS:'true',LOCAL_METADATA_READS:'true',INLINE_METADATA_ROOTS:'true',WAL_INLINE_TAIL:'true',PAGED_TEXT:'true',PAGED_PROJECTIONS:'true',PUBLICATION_RECORDS:'64',WORKER_VERSION:{id:'01234567-0123-4567-89ab-0123456789ab'},ANTFLY_TENANTS:JSON.stringify(Object.fromEntries(['a','b','c','d'].map(t=>[t,{incarnation:'latency-'+variant.name+'-'+t,storage_id:'latency-'+variant.name+'-'+t}]))),...Object.fromEntries(Object.entries(variant).filter(([k])=>k===k.toUpperCase()))};
 const classes={writer:'R2CommitCoordinator',reader:'R2ReadRuntime',builder:'R2BuildRuntime'},durableObjects=Object.fromEntries(Object.entries({COMMITS:'writer',READERS:'reader',BUILDERS:'builder'}).map(([binding,role])=>[binding,{className:classes[role],scriptName:role,useSQLite:true}]));
 const shim=`const delayModel=${JSON.stringify(model)};let ioDelay=0,ops=0,gets=0,puts=0,active=0,peak=0;const proxies=new WeakMap();function slow(env){let bucket=proxies.get(env.DATABASE);if(!bucket){bucket=new Proxy(env.DATABASE,{get(target,key){const value=target[key];if(typeof value!=='function')return value;return async(...args)=>{ops++;gets+=key==='get';puts+=key==='put';peak=Math.max(peak,++active);if(ioDelay){const wait=delayModel==='bursty'?ioDelay+(Math.floor(ops/16)%4===3?ioDelay*4:0)+(ops*17)%Math.max(1,ioDelay):ioDelay;await new Promise(r=>setTimeout(r,wait));}try{return await value.apply(target,args);}finally{active--;}};}});proxies.set(env.DATABASE,bucket);}return{...env,DATABASE:bucket,MAINTENANCE:{async send(){}},DEAD_LETTERS:{}};}function counters(){return{ops,gets,puts,active,peak};}`;
 const workers=[],bundleHashes={};
 for(const role of['app','writer','reader','builder']){
  const source=role==='app'?`${shim}import{configureKernels}from'./kernels.mjs';configureKernels('app');import worker from'./worker.mjs';import{tenantEnv}from'./tenants.mjs';export default{async fetch(request,env,ctx){env=slow(env);if(new URL(request.url).pathname==='/_lab'){const input=await request.json();if(input.counters){const e=tenantEnv(env,'a'),name=JSON.stringify(['runtime-v1',e.ANTFLY_INCARNATION,0]);return Response.json({app:counters(),writer:await e.COMMITS.getByName(e.ANTFLY_INCARNATION).ioCounters(),builder:await e.BUILDERS.getByName(name).ioCounters()});}if(input.inventory){let cursor,objects=0,bytes=0;do{const page=await env.DATABASE.list({cursor,limit:1000});for(const object of page.objects){objects++;bytes+=object.size;}cursor=page.truncated?page.cursor:undefined;}while(cursor);return Response.json({objects,bytes});}if(input.delay!==undefined){ioDelay=input.delay;for(const t of['a','b','c','d']){const e=tenantEnv(env,t),name=JSON.stringify(['runtime-v1',e.ANTFLY_INCARNATION,0]);await e.COMMITS.getByName(e.ANTFLY_INCARNATION).setDelay(input.delay);await e.BUILDERS.getByName(name).setDelay(input.delay);}}return Response.json(counters());}return worker.fetch(request,env,ctx);}};`:`${shim}import{${classes[role]} as Base}from'./${role==='writer'?'r2-coordinator':'runtime-actors'}.mjs';import{configureKernels}from'./kernels.mjs';configureKernels('${role}');export class ${classes[role]} extends Base{constructor(ctx,env){super(ctx,slow(env));}setDelay(value){ioDelay=value;return counters();}ioCounters(){return counters();}}export default{fetch(){return new Response('private');}};`;
  const result=await build({stdin:{contents:source,resolveDir:root,sourcefile:'latency-'+role+'.mjs'},outfile:join(root,'latency-'+role+'.js'),write:false,bundle:true,format:'esm',platform:'neutral',target:'es2022',external:['cloudflare:workers','*.wasm']});
  bundleHashes[role]=createHash('sha256').update(result.outputFiles[0].text).digest('hex');
  workers.push({name:role,modulesRoot:root,modules:[{type:'ESModule',path:join(root,'latency-'+role+'.js'),contents:result.outputFiles[0].text},...nativeModules],compatibilityDate:'2026-10-23',bindings,durableObjects,r2Buckets:{DATABASE:'latency'}});
 }
 const dir=await mkdtemp(join(out,'worker-'+variant.name+'-')),mf=new Miniflare(convertV4MiniflareOptions({workers,resourcePersistencePath:join(dir,'resources'),isolatedResourcePersistencePath:join(dir,'isolated')}));
 let phase='preparation';
 const call=async(tenant,input,path='/api')=>{const started=performance.now(),response=await mf.dispatchFetch('https://latency.invalid'+path,{method:'POST',headers:{Authorization:'Bearer local-latency','X-Antfly-Tenant':tenant},body:JSON.stringify(input)}),value=await response.json();assert.equal(response.status,200,JSON.stringify(value));return{value,ms:performance.now()-started};};
 try{
  const body=(t,i)=>JSON.stringify({tenant:t,text:'document '+i,payload:'x'.repeat(1024)});
  await call('a',{delay},'/_lab');const preparationStarted=performance.now(),seedWrites=[],publications=[];
  for(const tenant of['a','b','c','d'])for(let base=0;base<documents;base+=64){seedWrites.push((await call(tenant,{op:'mutate',operation_id:'seed/'+base,mutations:Array.from({length:Math.min(64,documents-base)},(_,j)=>({kind:'upsert',doc_id:'doc/'+(base+j),body:body(tenant,base+j)}))})).ms);for(let steps=0;;steps++){if(steps>=128)throw Error('PublicationDidNotConverge');const published=await call(tenant,{op:'publish'});publications.push(published.ms);if(!published.value.has_more)break;}}
  const preparationMs=performance.now()-preparationStarted;phase='steady';const reads=[],writes=[],compacts=[];
  for(let i=0;i<8;i++)for(const tenant of['a','b','c','d']){const r=await call(tenant,{op:'lookup',doc_id:'doc/'+((i*37)%documents)});assert.equal(r.value.body,body(tenant,(i*37)%documents));reads.push(r.ms);}
  for(let i=0;i<4;i++)for(const tenant of['a','b','c','d']){const value=await call(tenant,{op:'mutate',operation_id:'update/'+i,mutations:[{kind:'upsert',doc_id:'doc/0',body:JSON.stringify({text:'new '+i})}]});writes.push(value.ms);}
  for(const tenant of['a','b','c','d']){for(let steps=0;;steps++){if(steps>=128)throw Error('PublicationDidNotConverge');if(!(await call(tenant,{op:'publish'})).value.has_more)break;}const r=await call(tenant,{op:'compact'});assert.equal(r.value.published,false);compacts.push(r.ms);assert.equal((await call(tenant,{op:'count'})).value.count,String(documents));assert.equal((await call(tenant,{op:'lookup',doc_id:'doc/0'})).value.body,'{"text":"new 3"}');}
  const physical=(await call('a',{counters:true},'/_lab')).value,retained=(await call('a',{inventory:true},'/_lab')).value;
  report.cases.push({native_sha256:nativeHashes,bundle_sha256:bundleHashes,physical_io:physical,retained,variant:variant.name,preparation_ms:preparationMs,seed_writes_ms:stats(seedWrites),publication_steps_ms:stats(publications),reads_ms:stats(reads),writes_ms:stats(writes),paged_noop_ms:stats(compacts),verified:true,wasm_sha256:nativeHashes['antfly-serverless.wasm']});
  await writeFile(join(out,outputName),JSON.stringify(report,null,2));console.log(JSON.stringify(report.cases.at(-1)));
 }catch(error){report.cases.push({phase,native_sha256:nativeHashes,bundle_sha256:bundleHashes,variant:variant.name,verified:false,error:String(error?.message??error).slice(0,2048)});await writeFile(join(out,outputName),JSON.stringify(report,null,2));console.error(JSON.stringify({variant:variant.name,verified:false,error:String(error?.message??error).slice(0,2048)}));process.exitCode=1;}finally{await mf.dispose();await rm(dir,{recursive:true,force:true});}
}
