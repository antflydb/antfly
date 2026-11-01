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

import{test}from'node:test';import assert from'node:assert/strict';
import{ReadByteBudget}from'../../../zig/lib/objectstore/wasm/read-byte-budget.mjs';
import{ReadGraph}from'../../../zig/lib/objectstore/wasm/read-graph.mjs';
import{CommittedHeaders}from'../../../zig/lib/objectstore/wasm/committed-headers.mjs';
import{DurableServingBootstrap}from'../durable-serving-bootstrap.mjs';
import{conditionalReadLeasePolicy}from'../../../zig/lib/objectstore/wasm/read-view-lease.mjs';
const request=i=>({op:'get',bucket:'database',key:'antfly/artifacts/sha256/aa/'+String(i).repeat(62),max_response_bytes:1024});
test('native graph discovery deduplicates before parallel physical loads within byte bounds',async()=>{
 const budget=new ReadByteBudget({maxBytes:16384});let active=0,peak=0,calls=0,retained=0;
 const g=new ReadGraph({deadlineMs:Date.now()+1000,budget,fairKey:'one',reserve:n=>{retained+=n;return()=>retained-=n;}}),read=g.wrap(async()=>{calls++;peak=Math.max(peak,++active);await new Promise(r=>setTimeout(r,5));active--;return{body:Uint8Array.of(7)};});
 for(const i of[1,2,1])await assert.rejects(read(request(i)),e=>e.status===10);assert.equal(calls,0);assert.equal(g.pending.size,2);await g.load();assert.equal(calls,2);assert.equal(peak,2);assert.ok(budget.peakBytes<=16384);assert.equal((await read(request(1))).body[0],7);assert.ok(retained>0);g.clear();assert.equal(retained,0);assert.equal(budget.bytes,0);
});
test('graph deadlines retain physical reservations and preserve late resource errors',async()=>{
 let reject,retained=0;const budget=new ReadByteBudget({maxBytes:8192}),g=new ReadGraph({deadlineMs:Date.now()+20,budget,fairKey:'slow',reserve:n=>{retained+=n;return()=>retained-=n;}}),read=g.wrap(()=>new Promise((_,r)=>reject=r));await assert.rejects(read(request(1)),e=>e.status===10);await assert.rejects(read(request(2)),e=>e.status===10);
 let failure;try{await g.load();}catch(e){failure=e;}assert.equal(failure.message,'DeadlineExceeded');assert.ok(failure.ioSettled);assert.ok(budget.bytes>0);assert.ok(retained>0);reject(Error('SQLITE_NOMEM'));await assert.rejects(failure.ioSettled,/SQLITE_NOMEM/);g.clear();assert.equal(retained,0);assert.equal(budget.bytes,0);
});
test('compressed bootstrap round trips and rejects corrupt payloads',async()=>{
 const h=new CommittedHeaders('ns','2'),key='antfly/manifests/ns/manifests/2.bin',bytes=new TextEncoder().encode('repeat '.repeat(800));h.offer({op:'get',bucket:'database',key},{body:bytes,json:{etag:'one'}});const pack=await h.encodeCompressed();assert.equal(pack.format,2);const read=CommittedHeaders.wrap(()=>{throw Error('unexpected I/O');},pack,'ns','2');assert.deepEqual((await read({op:'get',bucket:'database',key})).body,bytes);await assert.rejects(CommittedHeaders.wrap(()=>{}, {...pack,payload:'broken'},'ns','2')({op:'get',bucket:'database',key}));
});
const owner='d'.repeat(64),env={ANTFLY_INCARNATION:'native-durable'},authority={descriptor:{format:2,namespace:env.ANTFLY_INCARNATION,mode:'r2-bounded',storage_id:env.ANTFLY_INCARNATION,serving:{protocol:'serialized-owner-v3',owner_id:owner,authority_storage:'provider-native-v1'},read_lease_policy:conditionalReadLeasePolicy}};
const storage=()=>({value:null,writes:[],async get(){return structuredClone(this.value);},async put(key,value){this.value=structuredClone(value);this.writes.push(value.dirty?'intent':'clean');},async sync(){this.writes.push('sync');}});
test('durable bootstrap confirms dirty intent and explicitly checkpoints clean native bytes',async()=>{
 const s=storage(),b=new DurableServingBootstrap(s,env,owner);await b.adopt(authority);assert.deepEqual(s.writes,['intent','sync']);await b.begin();let exported=false;await b.finish({owner:[]},async action=>{assert.equal(action,'serving_export');exported=true;return{serving_capsule:'native bytes'};});assert.equal(exported,true);assert.deepEqual(s.writes.slice(-2),['clean','sync']);
 const restored=new DurableServingBootstrap(s,env,owner);await restored.authority();assert.deepEqual(await restored.restore(async(action,data)=>{assert.equal(action,'serving_import');assert.equal(data.serving_capsule,'native bytes');return{state:'restored'};}),{state:'restored'});
 await restored.begin();const dirty=new DurableServingBootstrap(s,env,owner);await dirty.authority();assert.equal(await dirty.restore(()=>{throw Error('must recover R2');}),null);
 await dirty.freeze();await assert.rejects(new DurableServingBootstrap(s,env,owner).authority(),/MigrationRequired/);
});
test('invalid capsule falls back dirty while failed durable writes never confirm state',async()=>{
 const s=storage(),b=new DurableServingBootstrap(s,env,owner);await b.adopt(authority);await b.finish({owner:[]},async()=>({serving_capsule:'bad'}));const restart=new DurableServingBootstrap(s,env,owner);await restart.authority();assert.equal(await restart.restore(async()=>{throw Error('InvalidServingCapsule');}),null);assert.equal(s.value.dirty,true);
 s.sync=async()=>{throw Error('LocalStorageFailure');};await assert.rejects(restart.finish({owner:[]},async()=>({serving_capsule:'native'})),/LocalStorageFailure/);
});
test('byte admission lets small responses progress and cancels queued reservations',async()=>{
 let queued=0;const budget=new ReadByteBudget({maxBytes:16384,reserveQueued:()=>{queued++;return()=>queued--;}}),held=budget.acquire('held',10000),release=await held.ready,big=budget.acquire('big',12000),small=budget.acquire('small',4000),releaseSmall=await small.ready;
 assert.equal(budget.bytes,14000);assert.equal(queued,1);releaseSmall();big.cancel();await assert.rejects(big.ready,/ReadAheadCanceled/);release();assert.equal(budget.bytes,0);assert.equal(queued,0);
});
test('app read-view adapters preserve durable protocol selection in their cache identity',async()=>{
 const{AppReadViews}=await import('../app-read-views.mjs');const configs=[],cache=new AppReadViews(config=>{configs.push(config);return{open(){}};}),configured={LOCAL_METADATA_READS:'true',RESIDENT_SERVING:'true',RESIDENT_MVCC:'true',RESIDENT_COLD_PIPELINE:'true',RESIDENT_DURABLE_BOOTSTRAP:'true',ANTFLY_INCARNATION:'native-durable',COMMITS:{idFromName:()=>({toString:()=>owner})}};
 cache.get(configured);cache.get({...configured,RESIDENT_DURABLE_BOOTSTRAP:'false'});assert.equal(configs.length,2);assert.equal(configs[0].RESIDENT_DURABLE_BOOTSTRAP,'true');assert.equal(configs[0].RESIDENT_COLD_PIPELINE,'true');
});
test('compressed bootstrap carries bounded native routing headers with the manifest',async()=>{
 const h=new CommittedHeaders('ns','3',{routing:true}),manifest='antfly/manifests/ns/manifests/3.bin',route='antfly/artifacts/sha256/aa/'+'e'.repeat(62),body=new TextEncoder().encode('native routing '.repeat(100));h.offer({op:'get',bucket:'database',key:manifest},{body:Uint8Array.of(1),json:{etag:'manifest'}});h.offer({op:'get',bucket:'database',key:route},{body,json:{etag:'route'}});assert.equal(h.encode().format,3);const read=CommittedHeaders.wrap(()=>{throw Error('unexpected I/O');},await h.encodeCompressed(),'ns','3');assert.deepEqual((await read({op:'get',bucket:'database',key:route})).body,body);
});

test('dirty intervals coalesce intents but uncertain clean durability requires a new intent',async()=>{
 const s=storage(),b=new DurableServingBootstrap(s,env,owner);await b.adopt(authority);
 await b.begin();await b.begin();assert.equal(b.generation,2);assert.deepEqual(s.writes,['intent','sync']);
 const sync=s.sync;s.sync=async()=>{throw Error('UncertainCleanSync');};
 await assert.rejects(b.finish({owner:[]},async()=>({serving_capsule:'native'})),/UncertainCleanSync/);
 assert.equal(s.value.dirty,false);assert.equal(b.dirtyConfirmed,false);
 s.sync=sync;await b.begin();assert.equal(s.value.dirty,true);assert.equal(b.dirtyConfirmed,true);
 assert.deepEqual(s.writes.slice(-2),['intent','sync']);
 await b.freeze();assert.equal(s.value.blocked,true);
 await assert.rejects(new DurableServingBootstrap(s,env,owner).authority(),/MigrationRequired/);
});
test('qualification native artifacts share immutable bytes without linking mutable build outputs',async()=>{
 const{mkdtemp,mkdir,readFile,rm,stat,writeFile}=await import('node:fs/promises');const{join}=await import('node:path');const{retainNativeModules}=await import('../qualification/native-artifacts.mjs');
 const dir=await mkdtemp(new URL('../dist/qualification/native-artifact-test-',import.meta.url));
 try{
  const one=join(dir,'one'),two=join(dir,'two'),cache=join(dir,'cache');await mkdir(one);await mkdir(two);
  const contents=Buffer.from('immutable wasm fixture'),modules=[{path:'build/module.wasm',contents}];
  const first=await retainNativeModules(modules,one,cache),second=await retainNativeModules(modules,two,cache);assert.deepEqual(first,second);assert.equal((await stat(join(one,'module.wasm'))).ino,(await stat(join(two,'module.wasm'))).ino);
  contents.fill(0);assert.equal((await readFile(join(one,'module.wasm'))).toString(),'immutable wasm fixture');
  const hash=first['module.wasm'];await rm(join(cache,hash+'.wasm'));await writeFile(join(cache,hash+'.wasm'),'corrupt');const three=join(dir,'three');await mkdir(three);await assert.rejects(retainNativeModules([{path:'module.wasm',contents:Buffer.from('immutable wasm fixture')}],three,cache),/NativeArtifactHashMismatch/);
 }finally{await rm(dir,{recursive:true,force:true});}
});

test('prepared bootstrap reuses decoded owned bytes across transports while preserving request checks',async()=>{
 const h=new CommittedHeaders('ns','2'),key='antfly/manifests/ns/manifests/2.bin',bytes=new TextEncoder().encode('repeat '.repeat(800));h.offer({op:'get',bucket:'database',key},{body:bytes,json:{etag:'one'}});
 const pack=await h.encodeCompressed(),stages=[],prepare=await CommittedHeaders.prepare(pack,'ns','2',{onStage:s=>stages.push(s)});pack.payload='mutated after preparation';
 let first=0,second=0;const a=prepare(async()=>{first++;return{fallback:1};}),b=prepare(async()=>{second++;return{fallback:2};});
 const request={op:'get',bucket:'database',key};(await a(request)).body.fill(0);assert.deepEqual((await b(request)).body,bytes);assert.equal(stages.filter(s=>s.name==='bootstrap_decode').length,1);
 assert.deepEqual((await a({...request,range:{offset:2,length:5}})).body,bytes.slice(2,7));assert.equal((await b({...request,op:'head',max_response_bytes:0})).body,undefined);
 await assert.rejects(a({...request,if_match_etag:'wrong'}),/condition/);await assert.rejects(b({...request,max_response_bytes:1}),/budget/);await assert.rejects(b({...request,range:{offset:-1}}),/range/);
 assert.equal((await a({...request,version_id:'other'})).fallback,1);assert.equal((await b({...request,bucket:'other'})).fallback,2);assert.equal(first,1);assert.equal(second,1);
 const plain=h.encode(),prepared=await CommittedHeaders.prepare(plain,'ns','2');plain.records[0].body='';assert.deepEqual((await prepared(a)(request)).body,bytes);
 assert.equal((await (await CommittedHeaders.prepare(h.encode(),'ns','3'))(async()=>42)(request)),42);
 await assert.rejects(CommittedHeaders.prepare(h.encode(),'other','2'),/InvalidCommittedHeaders/);
});
test('prepared compressed bootstrap rejects decompression expansion beyond the original bound',async()=>{
 const stream=new Blob([new TextEncoder().encode('x'.repeat(18000))]).stream().pipeThrough(new CompressionStream('gzip')),bytes=new Uint8Array(await new Response(stream).arrayBuffer());
 await assert.rejects(CommittedHeaders.prepare({format:2,namespace:'ns',version:'2',encoding:'gzip',payload:btoa(String.fromCharCode(...bytes))},'ns','2'),/InvalidCommittedHeaders/);
});
