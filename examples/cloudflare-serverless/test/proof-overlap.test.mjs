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

import {test} from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {AsyncLocalStorage} from 'node:async_hooks';
import {Bucket} from './object-fixture.mjs';
import {R2CoordinatorStore} from '../r2-coordinator-store.mjs';
import {RequestPool} from '../request-pool.mjs';
import {instantiateHost} from '../../../zig/lib/objectstore/wasm/asyncify.mjs';
import {createR2Transport} from '../../../zig/lib/objectstore/wasm/r2.mjs';
import {ReadAheadBudget,proofPrefetchBytes} from '../../../zig/lib/objectstore/wasm/read-ahead.mjs';
const module=await WebAssembly.compile(await readFile(new URL('../dist/antfly-serverless-observe.wasm',import.meta.url)));
const poolFor=clock=>new RequestPool(()=>instantiateHost(module,null,{clock}),1,{clock,reservationBytes:8*1024*1024,maxReservedBytes:8*1024*1024});

test('detached proofs release execution capacity and keep I/O in the submitting reader context', {timeout:5000},async()=>{
 const bucket=new Bucket(),raw=createR2Transport({database:bucket}),contexts=new AsyncLocalStorage(),budget=new ReadAheadBudget(1),backgrounds=[];
 let now=Date.now(),hold=false,release,entered;const held=new Promise(r=>release=r),started=new Promise(r=>entered=r),starts=[];
 const writer=new R2CoordinatorStore(module,bucket,'detached-proof-context',{clock:()=>now});await writer.transaction(tx=>tx.put('value',1));
 const pool=poolFor(()=>now),observer=new R2CoordinatorStore(module,bucket,'detached-proof-context',{clock:()=>now,readKernel:pool,proofBudget:budget,transport:async(request,body)=>{
  if(hold)starts.push({key:request.key,context:contexts.getStore()});
  if(hold&&request.key.endsWith('/CHECKPOINT')){entered();await held;}
  return raw(request,body);
 }}),background=p=>backgrounds.push(p);
 assert.equal(await observer.readOnly(tx=>tx.get('value'),{background}),1);const writes=bucket.writes;hold=true;
 const reading=contexts.run('reader-A',()=>observer.readOnly(tx=>tx.get('value'),{background}));await started;
 try{
  assert.equal(budget.active,1);assert.equal(budget.bytes,proofPrefetchBytes);assert.equal(pool.reservedBytes,0,'proof storage wait must not hold WASM admission');
  assert.equal(await contexts.run('reader-B',()=>pool.withHost(()=>contexts.getStore())),'reader-B');
  assert.ok(starts.length>=3);assert.ok(starts.every(row=>row.context==='reader-A'));
 }finally{hold=false;release();}
 assert.equal(await reading,1);assert.equal(bucket.writes,writes);assert.equal(budget.active,0);assert.equal(budget.bytes,0);assert.equal(observer.snapshotReaders,0);
 await writer.transaction(tx=>tx.put('value',2));assert.equal(await observer.readOnly(tx=>tx.get('value'),{background}),2,'negative inputs cannot survive the next observation');
 const replacement=new R2CoordinatorStore(module,bucket,'detached-proof-context',{clock:()=>now});await replacement.transaction(tx=>tx.put('value',3));
 assert.equal(await observer.readOnly(tx=>tx.get('value'),{background}),3,'native recovery must follow the new owner');assert.equal(observer.acquired,false);
 await Promise.all(backgrounds);
});

test('detached proof deadlines retain input permits and report late resource errors until physical settlement', {timeout:5000},async()=>{
 const bucket=new Bucket(),raw=createR2Transport({database:bucket}),budget=new ReadAheadBudget(1),backgrounds=[],metrics=[];
 let now=Date.now(),hold=false,release,entered;const held=new Promise(r=>release=r),started=new Promise(r=>entered=r);
 const writer=new R2CoordinatorStore(module,bucket,'detached-proof-deadline',{clock:()=>now});await writer.transaction(tx=>tx.put('value',1));
 const pool=poolFor(()=>now),observer=new R2CoordinatorStore(module,bucket,'detached-proof-deadline',{clock:()=>now,readKernel:pool,proofBudget:budget,onMetrics:sample=>metrics.push(sample),transport:async(request,body)=>{
  if(hold&&request.key.endsWith('/CHECKPOINT')){entered();now+=50;await held;throw Error('out of memory: SQLITE_NOMEM');}
  return raw(request,body);
 }}),background=p=>backgrounds.push(p);
 assert.equal(await observer.readOnly(tx=>tx.get('value'),{background}),1);hold=true;
 const failed=observer.readOnly(tx=>tx.get('value'),{background,deadlineMs:now+25}).then(()=>{throw Error('deadline unexpectedly succeeded');},error=>error);await started;
 let error;
 try{
  error=await Promise.race([failed,new Promise((_,reject)=>setTimeout(()=>reject(Error('proof deadline did not return')),1000))]);
  assert.equal(error.message,'DeadlineExceeded');assert.ok(error.ioSettled);assert.equal(backgrounds.length,1);
  assert.equal(budget.active,1);assert.equal(budget.tryAcquire(),null,'timed-out input buffers still own their allowance');assert.equal(observer.snapshotReaders,1);
  assert.equal(pool.reservedBytes,0);assert.equal(pool.pending.length,0);
 }finally{hold=false;release();await Promise.allSettled(backgrounds);}
 await error?.ioSettled;assert.equal(budget.active,0);assert.equal(budget.bytes,0);assert.equal(observer.snapshotReaders,0);
 const sample=metrics.filter(row=>row.event==='antfly.coordinator_snapshot'&&row.proof_prefetch).at(-1);
 assert.equal(sample.outcome,'error');assert.equal(sample.physical_r2.resource_failures,1);assert.ok(sample.operations<=254);assert.ok(sample.transferred_bytes<=8*1024*1024);
 const permitted=budget.tryAcquire();assert.ok(permitted);permitted();permitted();assert.equal(budget.active,0);
});

test('an exhausted detached-proof allowance waits without reserving WASM or starting storage', {timeout:5000},async()=>{
 const bucket=new Bucket(),raw=createR2Transport({database:bucket}),budget=new ReadAheadBudget(1),backgrounds=[];
 let now=Date.now(),hold=false,release,entered;const held=new Promise(r=>release=r),started=new Promise(r=>entered=r);
 const writer=new R2CoordinatorStore(module,bucket,'detached-proof-capacity',{clock:()=>now});await writer.transaction(tx=>tx.put('value',1));
 const pool=poolFor(()=>now),observer=new R2CoordinatorStore(module,bucket,'detached-proof-capacity',{clock:()=>now,readKernel:pool,proofBudget:budget,transport:async(request,body)=>{
  if(hold&&request.key.endsWith('/CHECKPOINT')){entered();await held;}
  return raw(request,body);
 }}),background=p=>backgrounds.push(p);
 assert.equal(await observer.readOnly(tx=>tx.get('value'),{background}),1);const releasePermit=budget.tryAcquire();hold=true;
 const reads=bucket.reads,reading=observer.readOnly(tx=>tx.get('value'),{background});
 try{
  for(let i=0;budget.pending.length===0;i++){assert.ok(i<100);await new Promise(r=>setTimeout(r,1));}
  assert.equal(budget.active,1);assert.equal(budget.peakBytes,proofPrefetchBytes);assert.equal(pool.reservedBytes,0);assert.equal(pool.pending.length,0);assert.equal(bucket.reads,reads);
  releasePermit();await started;assert.equal(budget.active,1);assert.equal(pool.reservedBytes,0);
 }finally{hold=false;release();releasePermit();}
 assert.equal(await reading,1);assert.equal(budget.active,0);assert.equal(pool.reservedBytes,0);await Promise.all(backgrounds);
});


test('a stalled proof and its queued followers leave another namespace admitted in its own context', {timeout:5000},async()=>{
 const bucket=new Bucket(),raw=createR2Transport({database:bucket}),budget=new ReadAheadBudget(2),contexts=new AsyncLocalStorage(),backgrounds=[],background=p=>backgrounds.push(p);
 let now=Date.now(),hold=false,phase=false,release,entered;const held=new Promise(r=>release=r),started=new Promise(r=>entered=r),calls=[];
 for(const [namespace,value]of[['proof-fair-a',1],['proof-fair-b',2]]){const writer=new R2CoordinatorStore(module,bucket,namespace,{clock:()=>now});await writer.transaction(tx=>tx.put('value',value));}
 const pool=new RequestPool(()=>instantiateHost(module,null,{clock:()=>now}),2,{clock:()=>now,reservationBytes:8*1024*1024,maxReservedBytes:16*1024*1024});
 const a=new R2CoordinatorStore(module,bucket,'proof-fair-a',{clock:()=>now,readKernel:pool,proofBudget:budget,transport:async(request,body)=>{
  if(phase)calls.push(contexts.getStore());
  if(hold&&request.key.endsWith('/CHECKPOINT')){entered();await held;}
  return raw(request,body);
 }}),b=new R2CoordinatorStore(module,bucket,'proof-fair-b',{clock:()=>now,readKernel:pool,proofBudget:budget,transport:raw});
 assert.equal(await a.readOnly(tx=>tx.get('value'),{background}),1);assert.equal(await b.readOnly(tx=>tx.get('value'),{background}),2);hold=true;phase=true;
 const first=contexts.run('A1',()=>a.readOnly(tx=>tx.get('value'),{background}));await started;
 const second=contexts.run('A2',()=>a.readOnly(tx=>tx.get('value'),{background}));
 try{
  for(let i=0;budget.pending.length===0;i++){assert.ok(i<100);await new Promise(r=>setTimeout(r,1));}
  assert.equal(budget.active,1);assert.equal(budget.pending.length,1);assert.equal(pool.reservedBytes,0);
  let timer;try{assert.equal(await Promise.race([contexts.run('B',()=>b.readOnly(tx=>tx.get('value'),{background})),new Promise((_,reject)=>timer=setTimeout(()=>reject(Error('independent namespace was blocked')),500))]),2);}finally{clearTimeout(timer);}
  assert.ok(calls.every(value=>value==='A1'));assert.equal(budget.active,1);
 }finally{hold=false;release();await Promise.allSettled([first,second]);}
 assert.equal(await first,1);assert.equal(await second,1);assert.ok(calls.includes('A2'));assert.ok(calls.every(value=>['A1','A2'].includes(value)));
 assert.equal(budget.active,0);assert.equal(budget.pending.length,0);assert.ok(budget.peakBytes<=2*proofPrefetchBytes);await Promise.all(backgrounds);
});

test('an already expired queued proof removes its waiter before any physical request', {timeout:5000},async()=>{
 const bucket=new Bucket(),budget=new ReadAheadBudget(1);let now=Date.now();
 const writer=new R2CoordinatorStore(module,bucket,'proof-queued-deadline',{clock:()=>now});await writer.transaction(tx=>tx.put('value',1));
 const pool=poolFor(()=>now),observer=new R2CoordinatorStore(module,bucket,'proof-queued-deadline',{clock:()=>now,readKernel:pool,proofBudget:budget}),backgrounds=[],background=p=>backgrounds.push(p);
 assert.equal(await observer.readOnly(tx=>tx.get('value'),{background}),1);const release=budget.tryAcquire(),reads=bucket.reads;
 try{
  await assert.rejects(observer.readOnly(tx=>tx.get('value'),{background,deadlineMs:now-1}),/DeadlineExceeded/);
  assert.equal(budget.pending.length,0);assert.equal(budget.active,1);assert.equal(observer.snapshotReaders,0);assert.equal(pool.reservedBytes,0);assert.equal(bucket.reads,reads);assert.equal(backgrounds.length,0);
 }finally{release();}
 assert.equal(budget.active,0);
});

test('proof waiters and native jobs share the existing queue ceiling', {timeout:5000},async()=>{
 let release,entered;const held=new Promise(r=>release=r),started=new Promise(r=>entered=r);
 const pool=new RequestPool(()=>({usable:()=>true,memoryBytes:()=>0}),1),budget=new ReadAheadBudget(1,{reserveQueued:()=>pool.reserveQueuedAdmission()});
 const running=pool.withHost(async()=>{entered();await held;});await started;
 const queued=Array.from({length:31},()=>pool.withHost(()=>1)),releaseProof=budget.tryAcquire('busy');
 const waiting=budget.acquire('busy'),canceled=assert.rejects(waiting.ready,/ReadAheadCanceled/);
 try{
  assert.equal(pool.pending.length,31);assert.equal(pool.externalPending,1);assert.equal(pool.pending.length+pool.externalPending,pool.maxQueued);
  await assert.rejects(pool.withHost(()=>2),/CoordinatorCommitBusy/);
  waiting.cancel();await canceled;assert.equal(pool.externalPending,0);assert.equal(budget.pending.length,0);
  queued.push(pool.withHost(()=>3));assert.equal(pool.pending.length,32);
 }finally{waiting.cancel();releaseProof();release();await Promise.allSettled([running,...queued]);}
 assert.equal(pool.pending.length,0);assert.equal(pool.externalPending,0);assert.equal(budget.active,0);
});
