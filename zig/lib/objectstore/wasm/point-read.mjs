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

import{ReadGraph}from'./read-graph.mjs';
import{readByteBudgetFor}from'./read-byte-budget.mjs';
import {ReadDependencies,pointDependencyBudgetFor} from './read-dependencies.mjs';

// A miss may retain an immutable transcript while its native stack unwinds.
// Acquire that retention permit only after a miss: warm reads must not queue
// behind external I/O. The caller supplies its ordinary bounded native lane.
export async function withPointReadDependencies({graphRead=false,reserveTranscript,kernel,acquire,execute,transport,wrapTransport=t=>t,fairKey,deadlineMs,remainingMs=()=>deadlineMs-Date.now(),signal,background,onMetrics=()=>{},onNativeMetrics=()=>{},onUsage=()=>{},onStage=()=>{},onLateError=error=>console.error('Antfly detached point I/O failed',String(error))}){
 if(typeof background!=='function')throw Error('PointReadLifetimeRequired');
 const dependencies=graphRead?new ReadGraph({deadlineMs,signal,onMetrics,reserve:reserveTranscript,budget:readByteBudgetFor(kernel),fairKey}):new ReadDependencies({deadlineMs,signal,onMetrics});
 const usage={operations:0,bytes:0};let ticket,release,draining=false;
 const check=()=>{const remaining=remainingMs();if(!Number.isFinite(remaining)||remaining<=0||signal?.aborted)throw Error('DeadlineExceeded');dependencies.deadlineMs=Math.min(dependencies.deadlineMs,Date.now()+remaining);dependencies.check();return remaining;};
 const clear=()=>{dependencies.clear();if(release)release();else ticket?.cancel();};
 try{
  for(let step=0;step<=64;step++){
   check();dependencies.operations=usage.operations;dependencies.transferred=usage.bytes;
   if(usage.operations>=1024||usage.bytes>=32*1024*1024)throw Error('ObjectIoBudgetExceeded');
   const value=await acquire(host=>{
    check();const enabled=host.readDependencyABI===1;
    return execute(host,{transport:wrapTransport(dependencies.wrap(transport,{enabled})),nativeReadYield:enabled,nativeReadGraph:graphRead&&host.readGraphABI===1,maxOperations:1024-usage.operations,maxTransferBytes:32*1024*1024-usage.bytes,onMetrics:sample=>{usage.operations+=sample.operations;usage.bytes+=sample.transferred_bytes;onNativeMetrics(sample);}});
   });
   check();if(value?.__read_dependency!==true)return value;
   if(!graphRead&&!release){
    const queuedAt=performance.now();ticket=pointDependencyBudgetFor(kernel).acquire(fairKey,{concurrent:true});
    let timer,cancel;
    try{release=await Promise.race([ticket.ready,new Promise((_,reject)=>{cancel=()=>reject(Error('DeadlineExceeded'));timer=setTimeout(cancel,check());signal?.addEventListener('abort',cancel,{once:true});if(signal?.aborted)cancel();})]);}
    finally{clearTimeout(timer);signal?.removeEventListener('abort',cancel);}
    release.start();onStage({name:"point_dependency_queue",duration_ms:performance.now()-queuedAt});
   }
   check();dependencies.operations=usage.operations;dependencies.transferred=usage.bytes;
   const ioStarted=performance.now();try{await dependencies.load();}finally{onStage({name:"point_dependency_io",duration_ms:performance.now()-ioStarted});}usage.operations=dependencies.operations;usage.bytes=dependencies.transferred;
  }
  throw Error('ObjectIoBudgetExceeded');
 }catch(error){
  if(error.ioSettled&&(release||graphRead)){
   draining=true;const id=crypto.randomUUID();onMetrics({io_draining:true,io_lifecycle_id:id});
   const task=error.ioSettled.catch(onLateError).finally(()=>{clear();onMetrics({event:'antfly.io_settled',io_lifecycle_id:id});});
   try{background(task);}catch{await task;}
  }
  throw error;
 }finally{try{onUsage({operations:Math.max(usage.operations,dependencies.operations),transferred_bytes:Math.max(usage.bytes,dependencies.transferred)});}finally{if(!draining)clear();}}
}
