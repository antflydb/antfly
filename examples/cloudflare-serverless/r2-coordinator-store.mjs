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

// Cloudflare-independent adapter for the shared native ordered metadata store.
import {instantiateHost} from "../../zig/lib/objectstore/wasm/asyncify.mjs";
import {createR2Transport} from "../../zig/lib/objectstore/wasm/r2.mjs";
import {createImmutableReadCache} from "../../zig/lib/objectstore/wasm/cache.mjs";
const utf8 = new TextEncoder();
const compare = (l, r) => {const a = utf8.encode(l), b = utf8.encode(r); for (let i=0; i<Math.min(a.length,b.length); i++) if (a[i]!==b[i]) return a[i]-b[i]; return a.length-b.length;};
export const encode = value => JSON.stringify(value, (_, v) => v instanceof Uint8Array || v instanceof ArrayBuffer ? {__antfly_bytes: Array.from(new Uint8Array(v.buffer ?? v, v.byteOffset ?? 0, v.byteLength))} : v);
const decode = value => JSON.parse(value, (_, v) => v && Object.keys(v).length === 1 && Array.isArray(v.__antfly_bytes) ? Uint8Array.from(v.__antfly_bytes) : v);
export class R2CoordinatorStore {
  constructor(module, bucket, namespace, {prefix = "antfly", clock = Date.now, onMetrics, owner, transport} = {}) {
    this.module = module; this.bucket = bucket; this.namespace = namespace; this.prefix = prefix; this.clock = clock; this.onMetrics = onMetrics;
    this.owner = owner ?? Array.from(crypto.getRandomValues(new Uint8Array(16)));
    this.cache = createImmutableReadCache({includeCommitLog:true});
    this.transport = this.cache.wrap(transport ?? createR2Transport({database: bucket}, {maxPutBytes: 4 * 1024 * 1024}), `coordinator/${namespace}`);
    this.pending = []; this.running = false; this.state = null;
  }
  async native(coordinate) {
    this.host ??= await instantiateHost(this.module, this.transport, {clock: this.clock, onMetrics: sample => {
      this.usedOperations = (this.usedOperations ?? 0)+sample.operations;
      this.usedBytes = (this.usedBytes ?? 0)+sample.transferred_bytes;
      this.onMetrics?.(sample);
    }});
    try {
      if ((this.usedOperations ?? 0) >= 1024 || (this.usedBytes ?? 0) >= 32*1024*1024) throw new Error("CoordinatorIoBudgetExceeded");
      return await this.host.request({op: "coordinate", bucket: "database", prefix: this.prefix, namespace: this.namespace, coordinate: {owner: this.owner, state: this.state, ...coordinate}}, {deadlineMs: this.deadline ?? this.clock() + 30000,maxOperations:1024-(this.usedOperations??0),maxTransferBytes:32*1024*1024-(this.usedBytes??0)});
    } finally {if (!this.host.usable() || this.host.memoryBytes() >= 24 * 1024 * 1024) this.host = null;}
  }
  transaction(run) {
    if (this.pending.length >= 64) return Promise.reject(new Error("CoordinatorCommitBusy"));
    return new Promise((resolve,reject) => {
      this.pending.push({run,resolve,reject});
      if (!this.running) {this.running=true; queueMicrotask(()=>this.drain());}
    });
  }
  async drain() {
    while (this.pending.length) await this.batch();
    this.running=false;
  }
  async batch() {
    this.deadline = this.clock()+30000;
    this.usedOperations=0; this.usedBytes=0;
    const completed=[];
    try {
      const recovered = await this.native({action: "begin"});
      this.state = recovered.state; this.base = recovered.base; this.pagePrefix = recovered.page_prefix;
      if (recovered.has_more) throw new Error("CoordinatorRecovering");
      const overlay = new Map();
      const tx = {
        getMany: async keys => {
          const missing = keys.filter(key => !overlay.has(key));
          const rows = missing.length ? (await this.native({action: "read", commands: missing.map(key => ({kind: "get", key}))})).results : [];
          const found = new Map(missing.map((key, i) => [key, rows[i][0] ? decode(rows[i][0].value) : undefined]));
          return keys.map(key => structuredClone(overlay.has(key) ? overlay.get(key) : found.get(key)));
        },
        get: async key => (await tx.getMany([key]))[0],
        put: (key, value) => {if (value === undefined) throw new Error("CoordinatorValueRequired"); overlay.set(key, structuredClone(value));},
        delete: key => overlay.set(key, undefined),
        list: async ({prefix = "", startAfter = "", limit = 128} = {}) => {
          if (!Number.isSafeInteger(limit) || limit < 1 || limit > 128) throw new Error("CoordinatorBatchExceeded");
          const values = new Map(); let after = startAfter;
          for (let scan = 0; scan < 8; scan++) {
            const page = (await this.native({action: "read", commands: [{kind: "list", key: prefix, after, limit: 128}]})).results[0];
            for (const row of page) values.set(row.key, decode(row.value));
            for (const [key, value] of overlay) if (key.startsWith(prefix) && compare(key, startAfter) > 0) {
              if (value === undefined) values.delete(key); else values.set(key, value);
            }
            const sorted = [...values].sort(([a],[b]) => compare(a,b));
            if (page.length < 128 || (sorted.length >= limit && compare(sorted[limit-1][0], page.at(-1).key) <= 0)) return structuredClone(new Map(sorted.slice(0, limit)));
            after = page.at(-1).key;
          }
          throw new Error("CoordinatorBatchExceeded");
        },
        mutationCount: () => overlay.size,
        snapshot: () => structuredClone(this.state),
        checkpoint: async () => (await this.native({action: "checkpoint"})).checkpointed,
        retirableLogs: async (keys, before) => (await this.native({action: "retire_logs", keys, before})).keys,
        trace: async page => this.native({action: "trace", page}),
      };
      // Independent RPCs serialize through one overlay and share a durable
      // commit. Every result waits for that commit. A failing callback rolls
      // back only its own writes; reads return copies to isolate mutations.
      for (let count=0;count<16&&this.pending.length&&overlay.size<=80;count++) {
        const item=this.pending.shift(), previous=new Map(overlay);
        try {
          const result=await item.run(tx);
          if (overlay.size>512) throw new Error("CoordinatorCommitBusy");
          completed.push({...item,result:structuredClone(result)});
        } catch(error) {
          overlay.clear(); for(const [key,value] of previous) overlay.set(key,value);
          item.reject(error);
        }
      }
      if (overlay.size) {
        if (overlay.size > 512) throw new Error("CoordinatorBatchExceeded");
        const committed = await this.native({action: "commit", commands: [...overlay].map(([key,value]) => ({kind: value === undefined ? "delete" : "put", key, value: value === undefined ? null : encode(value)}))});
        this.state = committed.state;
      }
      for(const item of completed) item.resolve(item.result);
    } catch(error) {
      for(const item of completed) item.reject(error);
      // Recovery advances a bounded cursor before returning backpressure.
      // Reject queued callers rather than retaining an unbounded request set.
      if (!completed.length) for(const item of this.pending.splice(0,16)) item.reject(error);
    } finally {this.deadline = null;}
  }
}
