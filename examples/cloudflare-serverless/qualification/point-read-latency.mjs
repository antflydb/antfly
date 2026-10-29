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
if(!['uniform','bursty','fixed-bursty'].includes(model))throw Error('InvalidLifecycleDelayModel');
const root=resolve('.'),out=resolve('dist/qualification/point-read-lab'),delay=Number(process.argv.find(a=>a.startsWith('--delay='))?.slice(8)??20),documents=Number(process.argv.find(a=>a.startsWith('--documents='))?.slice(12)??128);
if(!Number.isInteger(delay)||delay<0||delay>100||![64,128,512].includes(documents))throw Error('InvalidWorkerLatencyExperiment');await mkdir(out,{recursive:true});
const variants=[{name:'point-current',baseline:true,NATIVE_READ_PLAN:'true',NATIVE_WRITE_PLAN:'true',PUBLICATION_CONTINUATIONS:'true',PUBLICATION_PREPARED_SLICES:'true',PUBLICATION_ADAPTIVE_SLICES:'true'},{name:'point-staging',NATIVE_READ_DETACHED:'true',NATIVE_READ_PLAN:'true',NATIVE_WRITE_PLAN:'true',PUBLICATION_CONTINUATIONS:'true',PUBLICATION_PREPARED_SLICES:'true',PUBLICATION_ADAPTIVE_SLICES:'true'},{name:'point-compact',NATIVE_READ_PLAN:'true',NATIVE_WRITE_PLAN:'true',PUBLICATION_CONTINUATIONS:'true',PUBLICATION_PREPARED_SLICES:'true',PUBLICATION_ADAPTIVE_SLICES:'true',CANONICAL_POINT_BODIES:'true'}].filter(v=>!process.argv.some(a=>a.startsWith('--only='))||process.argv.find(a=>a.startsWith('--only=')).slice(7).split(',').includes(v.name));
if(!variants.length)throw Error('NoSelectedLifecycleVariants');
if(process.argv.includes('--reverse'))variants.reverse();
const rounds=Number(process.argv.find(a=>a.startsWith('--rounds='))?.slice(9)??16);if(!Number.isInteger(rounds)||rounds<8||rounds>64)throw Error('InvalidPointRounds');
const outputName='lifecycle-latency-'+Date.now()+'.json';
const report={qualification:false,output_file:outputName,kind:'actual workerd + native WASM + local R2 binding',storage_delay_model:model,injected_io_delay_ms:delay,delay_includes_preparation:true,documents_per_tenant:documents,tenants:4,rounds,burst_policy:model==='fixed-bursty'?'every fourth offered round uses five times the base binding delay; independent of I/O count':'original ordinal/uniform diagnostic',cases:[]},stats=a=>{a.sort((a,b)=>a-b);return{n:a.length,p50:a[Math.ceil(a.length*.5)-1]??0,p95:a[Math.ceil(a.length*.95)-1]??0,max:a.at(-1)??0};};
const moduleSets=new Map(),bundleCache=new Map();
for(const baseline of new Set(variants.map(v=>!!v.baseline)))moduleSets.set(baseline,await Promise.all(['antfly-serverless.wasm','antfly-serverless-compact.wasm','antfly-serverless-observe.wasm'].map(async name=>({type:'CompiledWasm',path:join(root,'dist',name),contents:await readFile(join(baseline?join(out,'before'):join(root,'dist'),name))}))));
for(const variant of variants){
 const nativeModules=moduleSets.get(!!variant.baseline);
 const sealed=join(out,outputName.replace('.json','')+'-'+variant.name);await mkdir(sealed,{recursive:true});
 for(const module of nativeModules)await writeFile(join(sealed,module.path.split('/').at(-1)),module.contents);
 const nativeHashes=Object.fromEntries(nativeModules.map(m=>[m.path.split('/').at(-1),createHash('sha256').update(m.contents).digest('hex')]));
 const bindings={API_TOKEN:'local-latency',ANTFLY_INCARNATION:'latency-'+variant.name,COORDINATOR_BACKEND:'r2',SPLIT_RUNTIME:'true',DIRECT_READS:'true',LOCAL_METADATA_READS:'true',INLINE_METADATA_ROOTS:'true',WAL_INLINE_TAIL:'true',PAGED_TEXT:'true',PAGED_PROJECTIONS:'true',PUBLICATION_RECORDS:'64',LOG_METRICS:'true',WORKER_VERSION:{id:'01234567-0123-4567-89ab-0123456789ab'},ANTFLY_TENANTS:JSON.stringify(Object.fromEntries(['a','b','c','d'].map(t=>[t,{incarnation:'latency-'+variant.name+'-'+t,storage_id:'latency-'+variant.name+'-'+t}]))),...Object.fromEntries(Object.entries(variant).filter(([k])=>k===k.toUpperCase()))};
 await writeFile(join(sealed,'bindings.json'),JSON.stringify(bindings,null,2));
 const classes={writer:'R2CommitCoordinator',reader:'R2ReadRuntime',builder:'R2BuildRuntime'},durableObjects=Object.fromEntries(Object.entries({COMMITS:'writer',READERS:'reader',BUILDERS:'builder'}).map(([binding,role])=>[binding,{className:classes[role],scriptName:role,useSQLite:true}]));
 const shim=`const phases={};console.log=(value)=>{try{const sample=typeof value==='string'?JSON.parse(value):value;if(sample?.event==='antfly.trace')for(const span of sample.spans??[]){if(!Number.isFinite(span.duration_ms))continue;const key=sample.scope+'/'+span.name,values=phases[key]??=[];if(values.length<4096)values.push(span.duration_ms);}}catch{}};const delayModel=${JSON.stringify(model)};let ioDelay=0,ops=0,gets=0,puts=0,active=0,peak=0;const proxies=new WeakMap();function slow(env){let bucket=proxies.get(env.DATABASE);if(!bucket){bucket=new Proxy(env.DATABASE,{get(target,key){const value=target[key];if(typeof value!=='function')return value;return async(...args)=>{ops++;gets+=key==='get';puts+=key==='put';peak=Math.max(peak,++active);if(ioDelay){const wait=delayModel==='bursty'?ioDelay+(Math.floor(ops/16)%4===3?ioDelay*4:0)+(ops*17)%Math.max(1,ioDelay):ioDelay;await new Promise(r=>setTimeout(r,wait));}try{return await value.apply(target,args);}finally{active--;}};}});proxies.set(env.DATABASE,bucket);}return{...env,DATABASE:bucket,MAINTENANCE:{async send(){}},DEAD_LETTERS:{}};}function counters(){const percentile=(a,p)=>a.length?a.toSorted((l,r)=>l-r)[Math.ceil(a.length*p)-1]:null;return{ops,gets,puts,active,peak,phases:Object.fromEntries(Object.entries(phases).map(([name,values])=>[name,{n:values.length,p50:percentile(values,.5),p95:percentile(values,.95)}]))};}`;
 const workers=[],bundleHashes={};
 for(const role of['app','writer','reader','builder']){
  const source=role==='app'?`${shim}import{configureKernels}from'./kernels.mjs';configureKernels('app');import worker from'./worker.mjs';import{tenantEnv}from'./tenants.mjs';export default{async fetch(request,env,ctx){env=slow(env);if(new URL(request.url).pathname==='/_lab'){const input=await request.json();if(input.counters){const e=tenantEnv(env,'a'),name=JSON.stringify(['runtime-v1',e.ANTFLY_INCARNATION,0]);return Response.json({app:counters(),writer:await e.COMMITS.getByName(e.ANTFLY_INCARNATION).ioCounters(),builder:await e.BUILDERS.getByName(name).ioCounters()});}if(input.inventory){let cursor,objects=0,bytes=0;do{const page=await env.DATABASE.list({cursor,limit:1000});for(const object of page.objects){objects++;bytes+=object.size;}cursor=page.truncated?page.cursor:undefined;}while(cursor);return Response.json({objects,bytes});}if(input.delay!==undefined){ioDelay=input.delay;for(const t of['a','b','c','d']){const e=tenantEnv(env,t),name=JSON.stringify(['runtime-v1',e.ANTFLY_INCARNATION,0]);await e.COMMITS.getByName(e.ANTFLY_INCARNATION).setDelay(input.delay);await e.BUILDERS.getByName(name).setDelay(input.delay);}}return Response.json(counters());}return worker.fetch(request,env,ctx);}};`:`${shim}import{${classes[role]} as Base}from'./${role==='writer'?'r2-coordinator':'runtime-actors'}.mjs';import{configureKernels}from'./kernels.mjs';configureKernels('${role}');export class ${classes[role]} extends Base{constructor(ctx,env){super(ctx,slow(env));}setDelay(value){ioDelay=value;return counters();}ioCounters(){return counters();}}export default{fetch(){return new Response('private');}};`;
  const result=bundleCache.get(role)??await build({stdin:{contents:source,resolveDir:root,sourcefile:'latency-'+role+'.mjs'},outfile:join(root,'latency-'+role+'.js'),write:false,bundle:true,format:'esm',platform:'neutral',target:'es2022',external:['cloudflare:workers','*.wasm']});
  bundleCache.set(role,result);await writeFile(join(sealed,role+'.js'),result.outputFiles[0].text);
  bundleHashes[role]=createHash('sha256').update(result.outputFiles[0].text).digest('hex');
  workers.push({name:role,modulesRoot:root,modules:[{type:'ESModule',path:join(root,'latency-'+role+'.js'),contents:result.outputFiles[0].text},...nativeModules],compatibilityDate:'2026-10-23',bindings,durableObjects,r2Buckets:{DATABASE:'latency'}});
 }
 const dir=await mkdtemp(join(out,'worker-'+variant.name+'-')),mf=new Miniflare(convertV4MiniflareOptions({workers,resourcePersistencePath:join(dir,'resources'),isolatedResourcePersistencePath:join(dir,'isolated')}));
 let phase='preparation';
 const call=async(tenant,input,path='/api')=>{const started=performance.now(),response=await mf.dispatchFetch('https://latency.invalid'+path,{method:'POST',headers:{Authorization:'Bearer local-latency','X-Antfly-Tenant':tenant},body:JSON.stringify(input)}),value=await response.json();assert.equal(response.status,200,JSON.stringify(value));return{value,ms:performance.now()-started};};
 try{
  const body=(t,i,text='document '+i)=>JSON.stringify({tenant:t,text,payload:'x'.repeat(1024)});
  await call('a',{delay},'/_lab');const preparationStarted=performance.now(),seedWrites=[],publications=[];
  for(const tenant of['a','b','c','d'])for(let base=0;base<documents;base+=64){seedWrites.push((await call(tenant,{op:'mutate',operation_id:'seed/'+base,mutations:Array.from({length:Math.min(64,documents-base)},(_,j)=>({kind:'upsert',doc_id:'doc/'+(base+j),body:body(tenant,base+j)}))})).ms);for(let steps=0;;steps++){if(steps>=128)throw Error('PublicationDidNotConverge');const published=await call(tenant,{op:'publish'});publications.push(published.ms);if(!published.value.has_more)break;}}
  const preparationMs=performance.now()-preparationStarted;phase='steady';const reads=[],writes=[],compacts=[],publicationUpdates=[],samples=[],receipts={},expected={};
  for(const tenant of['a','b','c','d'])receipts[tenant]=(await call(tenant,{op:'status'})).value.visibility;
  for(let round=0;round<rounds;round++){
   if(model==='fixed-bursty')await call('a',{delay:round%4===3?delay*5:delay},'/_lab');
   const jobs=[];
   for(const tenant of['a','b','c','d']){
    const currentBody=body(tenant,0,'round '+round),fence=receipts[tenant];
    jobs.push(call(tenant,{op:'mutate',operation_id:'update/'+round,mutations:[{kind:'upsert',doc_id:'doc/0',body:currentBody}]}).then(result=>{writes.push(result.ms);receipts[tenant]=result.value.visibility;expected[tenant]=currentBody;samples.push({round,tenant,op:'write',ms:result.ms});}));
    for(let i=0;i<3;i++){const id=1+(round*37+i)%(documents-1);jobs.push(call(tenant,{op:'lookup',doc_id:'doc/'+id,required_visibility:fence}).then(result=>{assert.equal(result.value.body,body(tenant,id));assert.ok(BigInt('0x'+result.value.visibility.slice(-16))>=BigInt('0x'+fence.slice(-16)));reads.push(result.ms);samples.push({round,tenant,op:'read',ms:result.ms});}));}
   }
   const results=await Promise.allSettled(jobs);for(const result of results)if(result.status==='rejected')throw result.reason;
   if(round%4===3)for(const tenant of['a','b','c','d'])for(let steps=0;;steps++){if(steps>=128)throw Error('PublicationDidNotConverge');const published=await call(tenant,{op:'publish'});publicationUpdates.push(published.ms);if(!published.value.has_more)break;}
  }
  if(model==='fixed-bursty')await call('a',{delay},'/_lab');
  for(const tenant of['a','b','c','d']){for(let steps=0;;steps++){if(steps>=128)throw Error('PublicationDidNotConverge');if(!(await call(tenant,{op:'publish'})).value.has_more)break;}const result=await call(tenant,{op:'compact'});assert.equal(result.value.published,false);compacts.push(result.ms);assert.equal((await call(tenant,{op:'count'})).value.count,String(documents));assert.equal((await call(tenant,{op:'lookup',doc_id:'doc/0',required_visibility:receipts[tenant]})).value.body,expected[tenant]);}
  const physical=(await call('a',{counters:true},'/_lab')).value,retained=(await call('a',{inventory:true},'/_lab')).value;
  report.cases.push({sealed_artifacts:sealed,native_sha256:nativeHashes,bundle_sha256:bundleHashes,physical_io:physical,retained,variant:variant.name,preparation_ms:preparationMs,seed_writes_ms:stats(seedWrites),publication_steps_ms:stats(publications),reads_ms:stats(reads),writes_ms:stats(writes),samples,publication_updates_ms:stats(publicationUpdates),paged_noop_ms:stats(compacts),verified:true,wasm_sha256:nativeHashes['antfly-serverless.wasm']});
  await writeFile(join(out,outputName),JSON.stringify(report,null,2));console.log(JSON.stringify(report.cases.at(-1)));
 }catch(error){report.cases.push({phase,sealed_artifacts:sealed,native_sha256:nativeHashes,bundle_sha256:bundleHashes,variant:variant.name,verified:false,error:String(error?.message??error).slice(0,2048)});await writeFile(join(out,outputName),JSON.stringify(report,null,2));console.error(JSON.stringify({variant:variant.name,verified:false,error:String(error?.message??error).slice(0,2048)}));process.exitCode=1;}finally{await mf.dispose();await rm(dir,{recursive:true,force:true});}
}
