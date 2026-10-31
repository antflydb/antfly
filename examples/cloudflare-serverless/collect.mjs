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

// One bounded durable collection step. Never retain a bucket/stub after return.
export async function collect(env, execute, expected) {
  const coordinator = env.COMMITS.getByName(env.ANTFLY_INCARNATION);
  const claim = await coordinator.collection("claim", { expected });
  if (claim.done) return { done: true, job_id: expected };
  if (claim.busy) return { has_more: true, job_id: claim.id, delay_seconds: 10 };
  const { token, state } = claim;
  let done = false, delay = 0;
  const call = (op, args = {}) => coordinator.collection(op, { token, ...args });
  try {
    switch (state.phase) {
      case "prepare": await call("prepared", { plan: await execute({ op: "gc_prepare" }, token) }); break;
      case "pins": await call("pins"); break;
      case "mark_artifacts": case "mark_wal": {
        const stopAt = Date.now()+10000;
        if (env.COORDINATOR_BACKEND === "r2") {
          let nodes;
          try {nodes=await call("nextBatch");} catch(error) {if(!/UnsupportedCollectionOperation/.test(String(error)))throw error;}
          if(nodes){
          if(!nodes.length){await call("marked");break;}
          const flush=async entries=>{try{await call("tracedBatch",{entries});}catch(error){if(!/UnsupportedCollectionOperation/.test(String(error)))throw error;for(const entry of entries)await call("traced",entry);}};
          let entries=[],children=0;
          for(const node of nodes){
            const result=await execute({op:"gc_trace",gc_task:node.task},token);
            if(entries.length&&children+result.children.length>192){await flush(entries);entries=[];children=0;}
            entries.push({id:node.id,result});children+=result.children.length;
            if(Date.now()>=stopAt)break;
          }
          if(entries.length)await flush(entries);
          break;
          }
        }
        for (let i = 0; i < 8; i++) {
          const node = await call("next");
          if (!node) { await call("marked"); break; }
          await call("traced", { id: node.id, result: await execute({ op: "gc_trace", gc_task: node.task }, token) });
          if (Date.now() >= stopAt) break;
        }
        break;
      }
      case "compact_wal": await call("walPrepared", { result: await execute({ op: "gc_wal", gc_id: token.id, gc_keep_from: state.wal_keep_from ?? "1" }, token) }); break;
      case "wait": delay = await call("ready"); break;
      case "sweep_artifacts": case "sweep_wal": case "sweep_manifests": {
        const prefix = { sweep_artifacts: state.artifact_prefix, sweep_wal: state.wal_prefix, sweep_manifests: state.manifest_prefix }[state.phase];
        const page = await env.DATABASE.list({ prefix, limit: 128, cursor: state.cursor ?? undefined });
        const keys = await call("deletable", { keys: page.objects.map(item => item.key) });
        // An authorization remains safe after lease expiry: retired attempt
        // identities cannot be published/reused by any future writer. Late
        // uploads from fenced writers are rediscovered by the next pass.
        if (keys.length) await env.DATABASE.delete(keys);
        await call("swept", { cursor: page.truncated ? page.cursor : null });
        break;
      }
      case "cleanup": done = await call("cleanup"); break;
      default: throw new Error("InvalidCollectionPhase");
    }
  } finally { if (!done) await call("release"); }
  return { done, phase: state.phase, has_more: !done, job_id: token.id, delay_seconds: delay };
}
