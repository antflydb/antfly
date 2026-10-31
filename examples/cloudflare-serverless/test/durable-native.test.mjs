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

import{test}from'node:test';import assert from'node:assert/strict';import{readFile}from'node:fs/promises';
import{R2CoordinatorStore}from'../r2-coordinator-store.mjs';import{DurableServingBootstrap}from'../durable-serving-bootstrap.mjs';import{openAuthority}from'../coordinator-authority.mjs';import{Bucket}from'./object-fixture.mjs';import{createR2Transport}from'../../../zig/lib/objectstore/wasm/r2.mjs';
const module=await WebAssembly.compile(await readFile(new URL('../dist/antfly-serverless.wasm',import.meta.url)));
const actor='e'.repeat(64);
async function fixture(){
 const bucket=new Bucket(),env={DATABASE:bucket,ANTFLY_INCARNATION:'durable-native-test',COORDINATOR_BACKEND:'r2',METADATA_ROOT_BACKEND:'bounded-journal',INLINE_METADATA_ROOTS:'true',RESIDENT_SERVING:'true',RESIDENT_MVCC:'true',RESIDENT_COLD_PIPELINE:'true',RESIDENT_DURABLE_BOOTSTRAP:'true',COMMITS:{idFromName:()=>({toString:()=>actor})}},raw=createR2Transport({database:bucket});let calls=0,intercept;
 const local={value:null,failClean:false,async get(){return structuredClone(this.value);},async put(key,value){if(this.failClean&&!value.dirty)throw Error('InjectedLocalCommitFailure');this.value=structuredClone(value);},async sync(){}};
 const reopen=async()=>{const store=new R2CoordinatorStore(module,bucket,env.ANTFLY_INCARNATION,{exclusiveOwnerId:actor,inlineReadSet:{keys:['value']},transport:async(...args)=>{calls++;await intercept?.(...args);return raw(...args);}});store.durableBootstrap=new DurableServingBootstrap(local,env,actor);const authority=await store.durableBootstrap.authority()??await openAuthority(env,'r2-bounded');await store.durableBootstrap.adopt(authority);store.configureAuthority(authority.descriptor);await store.restoreDurableRead();return store;};
 return{bucket,env,local,reopen,calls:()=>calls,intercept:fn=>intercept=fn};
}
test('native durable capsule restores cold reads with zero R2 I/O and no append authority',async()=>{
 const f=await fixture(),first=await f.reopen();await first.transaction(tx=>tx.put('value',1));await first.flushDurableCheckpoint();const capsule=f.local.value.capsule;
 const cold=await f.reopen(),before=f.calls();assert.equal(await cold.ownerRead(tx=>tx.get('value')),1);assert.equal(f.calls(),before);assert.equal(cold.acquired,false);assert.equal(cold.state,null);assert.equal(cold.journalProof,null);
 await assert.rejects(cold.servingCodec('serving_import',{serving_scope:'f'.repeat(64),serving_capsule:capsule}),/InvalidServingCapsule/);
 const corrupt=JSON.parse(capsule);corrupt.checksum[0]^=1;await assert.rejects(cold.servingCodec('serving_import',{serving_capsule:JSON.stringify(corrupt)}),/InvalidServingCapsule/);await assert.rejects(cold.servingCodec('serving_import',{serving_capsule:'x'.repeat(65537)}),/InvalidServingCapsule/);
 await cold.transaction(tx=>tx.put('value',2));assert.ok(f.calls()>before);assert.equal(await cold.ownerRead(tx=>tx.get('value')),2);assert.deepEqual(cold.state.owner,first.state.owner);assert.notDeepEqual(cold.journalProof.auth,first.journalProof.auth);
});
test('R2 commit followed by local durability failure remains dirty and recovers the committed value',async()=>{
 const f=await fixture(),first=await f.reopen();await first.transaction(tx=>tx.put('value',1));await first.flushDurableCheckpoint();f.local.failClean=true;await first.transaction(tx=>tx.put('value',2));await assert.rejects(first.flushDurableCheckpoint(),/InjectedLocalCommitFailure/);assert.equal(f.local.value.dirty,true);
 f.local.failClean=false;const recovered=await f.reopen(),before=f.calls();assert.equal(recovered.durableReadState,undefined);assert.equal(await recovered.ownerRead(tx=>tx.get('value')),2);assert.ok(f.calls()>before);
});
test('dirty precommit crash keeps the last value and corrupt clean capsules fall back to recovery',async()=>{
 const f=await fixture(),first=await f.reopen();await first.transaction(tx=>tx.put('value',1));await first.flushDurableCheckpoint();await first.durableBootstrap.begin();const dirty=await f.reopen();assert.equal(await dirty.ownerRead(tx=>tx.get('value')),1);
 await dirty.transaction(tx=>tx.put('value',3));await dirty.flushDurableCheckpoint();f.local.value.capsule='invalid';const corrupted=await f.reopen();assert.equal(f.local.value.dirty,true);assert.equal(await corrupted.ownerRead(tx=>tx.get('value')),3);
});
test('a clean confirmed snapshot remains readable while first-write fresh admission waits on R2',async()=>{
 const f=await fixture(),first=await f.reopen();await first.transaction(tx=>tx.put('value',1));await first.flushDurableCheckpoint();const cold=await f.reopen();let armed=true,atIO,release;const ready=new Promise(r=>atIO=r),gate=new Promise(r=>release=r);f.intercept(async()=>{if(armed){armed=false;atIO();await gate;}});
 const write=cold.transaction(tx=>tx.put('value',2));await ready;
 try{const before=f.calls();assert.equal(await cold.ownerRead(tx=>tx.get('value')),1);assert.equal(f.calls(),before);}finally{release();await write;}
 assert.equal(await cold.ownerRead(tx=>tx.get('value')),2);
});
test('incompatible WASM rejects durable activation before accepted metadata writes',async()=>{
 const f=await fixture(),empty=await WebAssembly.compile(Uint8Array.of(0,97,115,109,1,0,0,0)),store=new R2CoordinatorStore(empty,f.bucket,f.env.ANTFLY_INCARNATION,{exclusiveOwnerId:actor});store.durableBootstrap=new DurableServingBootstrap(f.local,f.env,actor);const authority=await openAuthority(f.env,'r2-bounded');await store.durableBootstrap.adopt(authority);store.configureAuthority(authority.descriptor);await assert.rejects(store.restoreDurableRead(),/ServingCapsuleModuleRequired/);assert.equal((await f.bucket.list({prefix:'antfly/coord-v1/'})).objects.length,0);
});
test('an intent durability deadline cannot advance accepted R2 metadata',async()=>{
 const f=await fixture(),store=await f.reopen(),clock=store.clock;await store.transaction(tx=>tx.put('value',1));await store.flushDurableCheckpoint();const before=f.calls();f.local.sync=async function(){if(this.value.dirty)store.clock=()=>clock()+31000;};
 await assert.rejects(store.transaction(tx=>tx.put('value',9),{deadlineMs:clock()+10000}),/DeadlineExceeded/);assert.equal(f.calls(),before);assert.equal(f.local.value.dirty,true);
 const recovered=await f.reopen();assert.equal(await recovered.ownerRead(tx=>tx.get('value')),1);
});
test('accepted writes survive a dirty restart and clean checkpoint sync serializes the next writer',async()=>{
 const f=await fixture(),store=await f.reopen();await store.transaction(tx=>tx.put('value',1));assert.equal(f.local.value.dirty,true);
 const restarted=await f.reopen();assert.equal(await restarted.ownerRead(tx=>tx.get('value')),1);await restarted.transaction(tx=>tx.put('value',1));
 let atSync,release;const ready=new Promise(r=>atSync=r),held=new Promise(r=>release=r);
 f.local.sync=async function(){if(!this.value.dirty){atSync();await held;}};
 const checkpoint=restarted.flushDurableCheckpoint();await ready;const before=f.calls();
 const write=restarted.transaction(tx=>tx.put('value',2));
 try{await new Promise(r=>setTimeout(r,10));assert.equal(f.calls(),before);assert.equal(f.local.value.dirty,false);}finally{release();}
 await checkpoint;await write;assert.equal(f.local.value.dirty,true);
 const recovered=await f.reopen();assert.equal(await recovered.ownerRead(tx=>tx.get('value')),2);
});

test('retirement suppresses new recovery hints before physical I/O',async()=>{
 const f=await fixture(),store=await f.reopen();await store.transaction(tx=>tx.put('value',1));const before=f.calls();store.freezeRequested=true;
 assert.deepEqual(await store.checkpointHint(),{busy:true});assert.equal(f.calls(),before);
 await assert.rejects(store.flushDurableCheckpoint(),/CoordinatorMigrationRequired/);
});
