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

// The frontier, exact mark set and state are native Antfly collections in R2.
// Completed-parent, children and mark changes share one native commit.
const STATE = "__antfly_collection";
export const TASKS = "__antfly_gc/tasks/", PENDING = "__antfly_gc/pending/", MARKS = "__antfly_gc/marks/";
export class R2CollectionJournal {
  constructor(store, now = Date.now) {this.store = store; this.now = now;}
  async state(tx) {return tx.get(STATE);}
  save(tx, state) {state.clock = this.now(); tx.put(STATE, state);}
  async valid(tx, token) {
    const state = await this.state(tx);
    if (state && this.now() < state.clock) throw new Error("CollectionClockInvalid");
    if (!state || state.id !== token?.id || state.lease !== token?.lease || state.expires <= this.now()) throw new Error("CollectionLeaseLost");
    return state;
  }
  async insert(tx, task) {
    const id = JSON.stringify(task);
    if (id === undefined) throw new Error("InvalidCollectionTask");
    if (!await tx.get(TASKS + id)) {tx.put(TASKS + id, {done: false}); tx.put(PENDING + id, task);}
  }
  rpc(op, args = {}) {return this.store.transaction(tx => this.execute(tx, op, args));}
  async traceBatch(tx,state,entries) {
    if (!["mark_artifacts","mark_wal"].includes(state.phase)) throw new Error("CollectionPhaseMismatch");
    if (!Array.isArray(entries)||entries.length>16||entries.reduce((n,entry)=>n+entry.result.children.length,0)>192) throw new Error("CollectionBatchExceeded");
    const unique=[...new Map(entries.map(entry=>[entry.id,entry])).values()];
    const parents=await tx.getMany(unique.map(entry=>TASKS+entry.id));
    const pending=unique.filter((_,i)=>parents[i]&&!parents[i].done);
    const children=new Map();
    for(const {result} of pending) for(const task of result.children) {
      const id=JSON.stringify(task);if(id===undefined)throw new Error("InvalidCollectionTask");children.set(id,task);
    }
    const ids=[...children.keys()],existing=await tx.getMany(ids.map(id=>TASKS+id));
    for(let i=0;i<ids.length;i++)if(!existing[i]){tx.put(TASKS+ids[i],{done:false});tx.put(PENDING+ids[i],children.get(ids[i]));}
    for(const {id,result} of pending){
      tx.put(MARKS+result.key,true);tx.put(TASKS+id,{done:true});tx.delete(PENDING+id);
      if(result.wal_keep_from!=null&&(state.wal_keep_from==null||BigInt(result.wal_keep_from)<BigInt(state.wal_keep_from)))state.wal_keep_from=result.wal_keep_from;
    }
    if(pending.length)this.save(tx,state);
  }
  async execute(tx, op, args) {
    if (op === "claim") {
      let state = await this.state(tx);
      if (state && this.now() < state.clock) throw new Error("CollectionClockInvalid");
      if (args.expected && state?.id !== args.expected) return {done: true};
      state ??= {id: crypto.randomUUID().replaceAll("-", ""), phase: "prepare", wal_keep_from: null};
      if (state.expires > this.now()) return {busy: true, id: state.id};
      state.lease = crypto.randomUUID(); state.expires = this.now()+30000; this.save(tx, state);
      return {token: {id: state.id, lease: state.lease}, state};
    }
    const state = await this.valid(tx, args.token);
    const next = async () => {const first = [...await tx.list({prefix: PENDING, limit: 1})][0]; return first ? {id: first[0].slice(PENDING.length), task: first[1]} : null;};
    switch (op) {
      case "release": state.expires = 0; break;
      case "prepared":
        if (state.phase !== "prepare") throw new Error("CollectionPhaseMismatch");
        for (const task of args.plan.roots) await this.insert(tx, task);
        Object.assign(state, args.plan, {phase: "pins"}); break;
      case "pins": {
        if (state.phase !== "pins") throw new Error("CollectionPhaseMismatch");
        const pins = [...await tx.list({prefix: state.pin_prefix, startAfter: state.pin_cursor, limit: 128})];
        for (const [key, record] of pins) {
          const version = key.slice(state.pin_prefix.length);
          if (!/^\d+$/.test(version)) throw new Error("InvalidManifestReadPin");
          const deadline = BigInt(new TextDecoder().decode(record.body).trim());
          if (deadline + 30000000000n >= BigInt(this.now())*1000000n) await this.insert(tx, {kind: "manifest", version});
          else if (BigInt(version) < BigInt(state.floor)) tx.delete(key);
          state.pin_cursor = key;
        }
        if (pins.length < 128) state.phase = "mark_artifacts"; break;
      }
      case "next": return next();
      case "nextBatch": return [...await tx.list({prefix:PENDING,limit:16})].map(([key,task])=>({id:key.slice(PENDING.length),task}));
      case "traced": return this.traceBatch(tx,state,[{id:args.id,result:args.result}]);
      case "tracedBatch": return this.traceBatch(tx,state,args.entries);
      case "marked":
        if (await next()) throw new Error("CollectionMarkIncomplete");
        if (state.phase === "mark_artifacts") state.phase = "compact_wal";
        else if (state.phase === "mark_wal") state.phase = "wait";
        else throw new Error("CollectionPhaseMismatch"); break;
      case "walPrepared":
        if (state.phase !== "compact_wal") throw new Error("CollectionPhaseMismatch");
        if (!args.result.has_more) {
          Object.assign(state, {wal_prefix: args.result.wal_prefix, wal_retired_before: args.result.retired_before, not_before: this.now()+60000, phase: "mark_wal"});
          for (const task of args.result.roots) await this.insert(tx, task);
        } break;
      case "ready":
        if (state.phase !== "wait") throw new Error("CollectionPhaseMismatch");
        if (this.now() < state.not_before) return Math.ceil((state.not_before-this.now())/1000);
        state.phase = "sweep_artifacts"; this.save(tx, state); return 0;
      case "deletable": {
        if (args.keys.length > 128) throw new Error("CollectionBatchExceeded");
        const marked = await tx.getMany(args.keys.map(key => MARKS + key));
        return args.keys.filter((key, i) => {
          let eligible = false;
          if (state.phase === "sweep_artifacts" && key.startsWith(state.artifact_prefix)) {
            const suffix = key.slice(state.artifact_prefix.length);
            eligible = /^[a-f0-9]{32}\/[a-f0-9]{64}$/.test(suffix) && BigInt(`0x${suffix.slice(0,16)}`) < BigInt(state.retired_before);
          } else if (state.phase === "sweep_wal" && key.startsWith(state.wal_prefix)) {
            const suffix = key.slice(state.wal_prefix.length);
            eligible = /^[a-f0-9]{64}$/.test(suffix) || (/^[a-f0-9]{32}\/[a-f0-9]{64}$/.test(suffix) && BigInt(`0x${suffix.slice(0,16)}`) < BigInt(state.wal_retired_before));
          } else if (state.phase === "sweep_manifests" && key.startsWith(state.manifest_prefix)) {
            const suffix = key.slice(state.manifest_prefix.length);
            eligible = /^\d+\.bin$/.test(suffix) && BigInt(suffix.slice(0,-4)) < BigInt(state.floor);
          }
          return eligible && !marked[i];
        });
      }
      case "swept":
        if (!["sweep_artifacts", "sweep_wal", "sweep_manifests"].includes(state.phase)) throw new Error("CollectionPhaseMismatch");
        state.cursor = args.cursor;
        if (args.cursor == null) state.phase = {sweep_artifacts: "sweep_wal", sweep_wal: "sweep_manifests", sweep_manifests: "cleanup"}[state.phase]; break;
      case "cleanup": {
        if (state.phase !== "cleanup") throw new Error("CollectionPhaseMismatch");
        const tasks = [...await tx.list({prefix: TASKS, limit: 128})], marks = [...await tx.list({prefix: MARKS, limit: 128})];
        for (const [key] of [...tasks, ...marks]) tx.delete(key);
        const remaining = (await tx.list({prefix: TASKS, limit: 1})).size || (await tx.list({prefix: MARKS, limit: 1})).size;
        if (!remaining) tx.delete(STATE);
        return !remaining;
      }
      default: throw new Error("UnsupportedCollectionOperation");
    }
    this.save(tx, state);
  }
}
