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
const STATE = "__antfly_coord_gc/state", TASK = "__antfly_coord_gc/tasks/", MARK = "__antfly_coord_gc/marks/";
// Zig JSON emits valid UTF-8 byte arrays as strings; both forms identify
// the same immutable page. Encode strings as UTF-8 before constructing keys.
const hex = bytes => Array.from(typeof bytes === "string" ? new TextEncoder().encode(bytes) : bytes, n => n.toString(16).padStart(2,"0")).join("");
const pageId = ref => `${hex(ref.attempt)}/${hex(ref.digest)}/${ref.pack?.offset ?? 0}`;
export async function collectCoordinator(store, bucket, now = Date.now, expectedJobId) {
  return store.transaction(async tx => {
    let state = await tx.get(STATE);
    if (expectedJobId != null && (!state || state.id !== expectedJobId)) return {done:true,job_id:expectedJobId};
    if (!state) state = {id:crypto.randomUUID(),phase:"checkpoint"};
    state.id ??= crypto.randomUUID();
    if (state.phase === "checkpoint") {
      if (!await tx.checkpoint()) {tx.put(STATE,state);return {has_more:true,job_id:state.id,delay_seconds:1};}
      const snapshot = tx.snapshot(), root = snapshot.root ? JSON.parse(snapshot.root) : null;
      state = {id:state.id,phase: "mark", barrier: String(snapshot.revision), epoch: hex(snapshot.epoch), not_before: now()+60000, cursor: null};
      if (root) tx.put(TASK + pageId(root), {page:root,offset:0});
    }
    if (state.phase === "mark") {
      for (let step = 0; step < 8; step++) {
        if(tx.mutationCount()>280) break;
        const task = [...await tx.list({prefix: TASK, limit: 1})][0];
        if (!task) {state.phase = "wait"; break;}
        const work=task[1].page?task[1]:{page:task[1],offset:0};
        const result = await tx.trace(work.page);
        tx.put(MARK + result.key, true);
        const children=result.children.slice(work.offset,work.offset+128);
        const marked=await tx.getMany(children.map(child=>MARK+`${store.pagePrefix}${hex(child.attempt)}/${hex(child.digest)}`));
        for(let i=0;i<children.length;i++)if(!marked[i])tx.put(TASK+pageId(children[i]),{page:children[i],offset:0});
        if(work.offset+children.length<result.children.length)tx.put(task[0],{page:work.page,offset:work.offset+children.length});else tx.delete(task[0]);
      }
    } else if (state.phase === "wait") {
      if (now() < state.not_before) return {has_more: true, job_id:state.id, delay_seconds: Math.ceil((state.not_before-now())/1000)};
      state.phase = "pages";
    } else if (state.phase === "pages" || state.phase === "logs") {
      const prefix = state.phase === "pages" ? store.pagePrefix : `${store.base}/epochs/`;
      const page = await bucket.list({prefix, cursor: state.cursor ?? undefined, limit: state.phase === "pages" ? 128 : 32});
      const deletable = [];
      if (state.phase === "pages") {
        const marked = await tx.getMany(page.objects.map(({key}) => MARK+key));
        for (let i=0; i<page.objects.length; i++) {
          const suffix = page.objects[i].key.slice(prefix.length);
          if (/^[a-f0-9]{32}\/[a-f0-9]{64}$/.test(suffix) && BigInt(`0x${suffix.slice(0,16)}`) < BigInt(state.barrier) && !marked[i]) deletable.push(page.objects[i].key);
        }
      } else {
        deletable.push(...await tx.retirableLogs(page.objects.map(item => item.key), state.barrier));
      }
      // Physical deletion precedes the cursor commit. Repeating this bounded
      // page after an ambiguous outcome is safe; only retired identities qualify.
      if (deletable.length) await bucket.delete(deletable);
      state.stats??={scanned:0,deleted:0,reclaimed_bytes:0};state.stats.scanned+=page.objects.length;state.stats.deleted+=deletable.length;const deleted=new Set(deletable);state.stats.reclaimed_bytes+=page.objects.reduce((n,item)=>n+(deleted.has(item.key)?item.size:0),0);
      state.cursor = page.truncated ? page.cursor : null;
      if (!page.truncated) state.phase = state.phase === "pages" ? "logs" : "cleanup";
    } else if (state.phase === "cleanup") {
      const marks = [...await tx.list({prefix: MARK, limit: 128})];
      for (const [key] of marks) tx.delete(key);
      if (!(await tx.list({prefix: MARK, limit: 1})).size) {tx.delete(STATE); return {done: true,job_id:state.id,stats:state.stats??{scanned:0,deleted:0,reclaimed_bytes:0}};}
    } else throw new Error("InvalidCoordinatorCollection");
    tx.put(STATE,state);
    return {has_more: true, job_id:state.id, phase: state.phase};
  });
}
