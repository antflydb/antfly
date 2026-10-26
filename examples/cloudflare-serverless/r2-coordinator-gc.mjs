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

// This collector collects the coordinator's own native page tree. Its progress
// is in newer tree revisions, protected by the same monotonic commit protocol.
import {metadataTreeRoot} from "./metadata-root.mjs";
import {maintenanceMutationBudget} from "./maintenance-policy.mjs";
const STATE = "__antfly_coord_gc/state", TASK = "__antfly_coord_gc/tasks/", MARK = "__antfly_coord_gc/marks/";
// Zig JSON emits valid UTF-8 byte arrays as strings; both forms identify
// the same immutable page. Encode strings as UTF-8 before constructing keys.
const hex = bytes => Array.from(typeof bytes === "string" ? new TextEncoder().encode(bytes) : bytes, n => n.toString(16).padStart(2,"0")).join("");
const pageId = ref => `${hex(ref.attempt)}/${hex(ref.digest)}/${ref.pack?.offset ?? 0}`;
export async function collectCoordinator(store, bucket, now = Date.now, expectedJobId) {
  let sweep;
  const result=await store.transaction(async tx => {
    let state = await tx.get(STATE);
    if (expectedJobId != null && (!state || state.id !== expectedJobId)) return {done:true,job_id:expectedJobId};
    if (!state) state = {id:crypto.randomUUID(),phase:"checkpoint"};
    if(!state.id){state.id=crypto.randomUUID();tx.put(STATE,state);}
    if (state.phase === "checkpoint") {
      if (!await tx.checkpoint()) {tx.put(STATE,state);return {has_more:true,job_id:state.id,delay_seconds:1};}
      const snapshot = tx.snapshot(), root = metadataTreeRoot(snapshot);
      state = {id:state.id,phase: "mark", barrier: String(snapshot.revision), epoch: hex(snapshot.epoch), not_before: now()+60000, cursor: null};
      if (root) tx.put(TASK + pageId(root), {page:root,offset:0});
      tx.put(STATE,state);
      return {has_more:true,job_id:state.id,phase:state.phase};
    }
    if (state.phase === "mark") {
      for (let step = 0; step < 8; step++) {
        if(step>0&&(tx.shouldYieldMaintenance?.()||tx.mutationCount()>=maintenanceMutationBudget-4)) break;
        const task = [...await tx.list({prefix: TASK, limit: 1})][0];
        if (!task) {state.phase = "wait"; break;}
        const work=task[1].page?task[1]:{page:task[1],offset:0};
        const result = await tx.trace(work.page);
        tx.put(MARK + result.key, true);
        const children=result.children.slice(work.offset,work.offset+Math.max(1,maintenanceMutationBudget-4-tx.mutationCount()));
        const marked=await tx.getMany(children.map(child=>MARK+`${store.pagePrefix}${hex(child.attempt)}/${hex(child.digest)}`));
        for(let i=0;i<children.length;i++)if(!marked[i])tx.put(TASK+pageId(children[i]),{page:children[i],offset:0});
        if(work.offset+children.length<result.children.length)tx.put(task[0],{page:work.page,offset:work.offset+children.length});else tx.delete(task[0]);
      }
    } else if (state.phase === "wait") {
      if (now() < state.not_before) return {has_more: true, job_id:state.id, delay_seconds: Math.ceil((state.not_before-now())/1000)};
      state.phase = "pages";
    } else if (["pages","logs","delete"].includes(state.phase)) {
      const phase=state.phase==="delete"?state.pending?.phase:state.phase;
      if(!["pages","logs"].includes(phase))throw Error("InvalidCoordinatorCollection");
      sweep={state,phase,prefix:phase==="pages"?store.pagePrefix:`${store.base}/epochs/`};
      return {has_more:true,job_id:state.id,phase:state.phase};
    } else if (state.phase === "cleanup") {
      const marks = [...await tx.list({prefix: MARK, limit: maintenanceMutationBudget-4})];
      for (const [key] of marks) tx.delete(key);
      if (!(await tx.list({prefix: MARK, limit: 1})).size) {tx.delete(STATE); return {done: true,job_id:state.id,stats:state.stats??{scanned:0,deleted:0,reclaimed_bytes:0}};}
    } else throw new Error("InvalidCoordinatorCollection");
    tx.put(STATE,state);
    return {has_more: true, job_id:state.id, phase: state.phase};
  });
  if(!sweep)return result;
  return sweepCoordinator(store,bucket,sweep);
}

const progress=(state,id)=>state?{has_more:true,job_id:state.id,phase:state.phase}:{done:true,job_id:id};
const sameBoundary=(state,plan)=>state&&state.id===plan.state.id&&state.barrier===plan.state.barrier&&(state.cursor??null)===(plan.state.cursor??null)&&(state.phase==="delete"?state.pending?.phase:state.phase)===plan.phase;
function finishSweep(state,intent){
  state.stats??={scanned:0,deleted:0,reclaimed_bytes:0};
  for(const key of["scanned","deleted","reclaimed_bytes"])state.stats[key]+=intent[key];
  state.cursor=intent.next_cursor;state.phase=intent.next_phase;delete state.pending;
}
function validateIntent(state,plan){
  const intent=state.pending,limit=plan.phase==="pages"?128:32;
  if(state.phase!=="delete"||![1,2].includes(intent?.format)||typeof intent.id!=="string"||intent.phase!==plan.phase||intent.barrier!==state.barrier||(intent.cursor??null)!==(state.cursor??null)||!Array.isArray(intent.keys)||intent.keys.length>limit||new Set(intent.keys).size!==intent.keys.length||intent.deleted!==intent.keys.length||!Number.isSafeInteger(intent.scanned)||intent.scanned<intent.deleted||intent.scanned>limit||!Number.isSafeInteger(intent.reclaimed_bytes)||intent.reclaimed_bytes<0||intent.next_cursor!==null&&typeof intent.next_cursor!=="string"||![plan.phase,plan.phase==="pages"?"logs":"cleanup"].includes(intent.next_phase))throw Error("InvalidCoordinatorCollectionIntent");
  const relative=[];
  for(const key of intent.keys){
    if(typeof key!=="string")throw Error("InvalidCoordinatorCollectionIntent");
    if(intent.format===1&&!key.startsWith(plan.prefix))throw Error("InvalidCoordinatorCollectionIntent");
    const suffix=intent.format===1?key.slice(plan.prefix.length):key;relative.push(suffix);
    if(plan.phase==="pages"?!/^[a-f0-9]{32}\/[a-f0-9]{64}$/.test(suffix)||BigInt("0x"+suffix.slice(0,16))>=BigInt(state.barrier):!/^[a-f0-9]{32}\/(?:START|SEALED|[0-9]{20}\.json)$/.test(suffix))throw Error("InvalidCoordinatorCollectionIntent");
  }
  return intent.format===1?{...intent,format:2,keys:relative}:intent;
}
async function sweepCoordinator(store,bucket,plan){
  // Listing and bulk deletion may take seconds. Neither holds the coordinator
  // transaction or a WASM host. Proof is persisted before physical deletion.
  let page;
  if(plan.state.phase!=="delete")page=await bucket.list({prefix:plan.prefix,cursor:plan.state.cursor??undefined,limit:plan.phase==="pages"?128:32});
  const prepared=await store.transaction(async tx=>{
    const state=await tx.get(STATE);
    if(!sameBoundary(state,plan))return{result:progress(state,plan.state.id)};
    if(state.phase==="delete")return{intent:validateIntent(state,plan)};
    const keys=[];
    if(plan.phase==="pages"){
      const marked=await tx.getMany(page.objects.map(({key})=>MARK+key));
      for(let i=0;i<page.objects.length;i++){
        const key=page.objects[i].key,suffix=key.slice(plan.prefix.length);
        if(key.startsWith(plan.prefix)&&/^[a-f0-9]{32}\/[a-f0-9]{64}$/.test(suffix)&&BigInt("0x"+suffix.slice(0,16))<BigInt(state.barrier)&&!marked[i])keys.push(key);
      }
    }else keys.push(...await tx.retirableLogs(page.objects.map(item=>item.key),state.barrier));
    const deleted=new Set(keys),intent={format:2,id:crypto.randomUUID(),phase:plan.phase,barrier:state.barrier,cursor:state.cursor??null,keys:keys.map(key=>key.slice(plan.prefix.length)),scanned:page.objects.length,deleted:keys.length,reclaimed_bytes:page.objects.reduce((n,item)=>n+(deleted.has(item.key)?item.size:0),0),next_cursor:page.truncated?page.cursor:null,next_phase:page.truncated?plan.phase:plan.phase==="pages"?"logs":"cleanup"};
    if(!keys.length){finishSweep(state,intent);tx.put(STATE,state);return{result:progress(state,state.id)};}
    state.phase="delete";state.pending=intent;validateIntent(state,plan);tx.put(STATE,state);return{intent};
  });
  if(prepared.result)return prepared.result;
  await bucket.delete(prepared.intent.keys.map(suffix=>plan.prefix+suffix));
  return store.transaction(async tx=>{
    const state=await tx.get(STATE);
    // Another delivery may have acknowledged the same immutable deletion, or
    // advanced the job. The cursor and statistics advance only once per intent.
    if(!sameBoundary(state,plan)||state.phase!=="delete"||state.pending?.id!==prepared.intent.id)return progress(state,plan.state.id);
    const intent=validateIntent(state,plan);finishSweep(state,intent);tx.put(STATE,state);return progress(state,state.id);
  });
}
