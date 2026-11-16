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

// SQLite owns the exact mark set and frontier, not an isolate-sized JS Set.
// A checkpoint publishes children and completes their parent in one transaction.
const STATE = "__antfly_collection";
export class CollectionJournal {
  constructor(storage, now = Date.now) {
    this.storage = storage; this.now = now;
    storage.sql.exec("CREATE TABLE IF NOT EXISTS collection_tasks (id TEXT PRIMARY KEY, task TEXT NOT NULL, done INTEGER NOT NULL DEFAULT 0)").toArray();
    storage.sql.exec("CREATE TABLE IF NOT EXISTS collection_marks (key TEXT PRIMARY KEY)").toArray();
    storage.sql.exec("CREATE INDEX IF NOT EXISTS collection_pending ON collection_tasks(done, id)").toArray();
  }
  rpc(op, args) {
    switch (op) {
      case "claim": return this.claim(args.expected);
      case "prepared": return this.prepared(args.token, args.plan);
      case "pins": return this.pins(args.token);
      case "next": return this.next(args.token);
      case "traced": return this.traced(args.token, args.id, args.result);
      case "marked": return this.marked(args.token);
      case "walPrepared": return this.walPrepared(args.token, args.result);
      case "ready": return this.ready(args.token);
      case "deletable": return this.deletable(args.token, args.keys);
      case "swept": return this.swept(args.token, args.cursor);
      case "cleanup": return this.cleanup(args.token);
      case "release": return this.release(args.token);
      default: throw new Error("UnsupportedCollectionOperation");
    }
  }
  state() { return this.storage.kv.get(STATE); }
  save(state) { state.clock = this.now(); this.storage.kv.put(STATE, state); }
  transaction(fn) { return this.storage.transactionSync(fn); }
  valid(token) {
    const state = this.state();
    if (state && this.now() < state.clock) throw new Error("CollectionClockInvalid");
    if (!state || state.id !== token.id || state.lease !== token.lease || state.expires <= this.now()) throw new Error("CollectionLeaseLost");
    return state;
  }
  claim(expected) {
    return this.transaction(() => {
      let state = this.state();
      if (state && this.now() < state.clock) throw new Error("CollectionClockInvalid");
      if (expected && state?.id !== expected) return { done: true };
      if (!state) state = { id: crypto.randomUUID().replaceAll("-", ""), phase: "prepare", wal_keep_from: null };
      if (state.expires > this.now()) return { busy: true, id: state.id };
      state.lease = crypto.randomUUID(); state.expires = this.now() + 30000;
      this.save(state);
      return { token: { id: state.id, lease: state.lease }, state };
    });
  }
  release(token) {
    this.transaction(() => { const state = this.valid(token); state.expires = 0; this.save(state); });
  }
  // Exhaust mutation cursors before issuing the next statement so the SQLite
  // host can release statement/binding resources during a long mark pass.
  insert(task) {
    const id = JSON.stringify(task);
    this.storage.sql.exec("INSERT OR IGNORE INTO collection_tasks(id, task) VALUES (?, ?)", id, id).toArray();
  }
  prepared(token, plan) {
    return this.transaction(() => {
      const state = this.valid(token);
      if (state.phase !== "prepare") throw new Error("CollectionPhaseMismatch");
      for (const task of plan.roots) this.insert(task);
      Object.assign(state, plan, { phase: "pins" }); this.save(state);
    });
  }
  pins(token) {
    return this.transaction(() => {
      const state = this.valid(token);
      if (state.phase !== "pins") throw new Error("CollectionPhaseMismatch");
      const pins = Array.from(this.storage.kv.list({ prefix: state.pin_prefix, startAfter: state.pin_cursor, limit: 128 }));
      for (const [key, record] of pins) {
        const version = key.slice(state.pin_prefix.length);
        if (!/^\d+$/.test(version)) throw new Error("InvalidManifestReadPin");
        const deadline = BigInt(new TextDecoder().decode(record.body).trim());
        if (deadline + 30000000000n >= BigInt(this.now()) * 1000000n) this.insert({ kind: "manifest", version });
        else if (BigInt(version) < BigInt(state.floor)) this.storage.kv.delete(key);
        state.pin_cursor = key;
      }
      if (pins.length < 128) state.phase = "mark_artifacts";
      this.save(state);
    });
  }
  next(token) {
    this.valid(token);
    const row = this.storage.sql.exec("SELECT id, task FROM collection_tasks WHERE done = 0 ORDER BY id LIMIT 1").toArray()[0];
    return row ? { id: row.id, task: JSON.parse(row.task) } : null;
  }
  traced(token, id, result) {
    this.transaction(() => {
      const state = this.valid(token);
      if (!["mark_artifacts", "mark_wal"].includes(state.phase)) throw new Error("CollectionPhaseMismatch");
      const row = this.storage.sql.exec("SELECT done FROM collection_tasks WHERE id = ?", id).toArray()[0];
      if (!row || row.done) return;
      this.storage.sql.exec("INSERT OR IGNORE INTO collection_marks(key) VALUES (?)", result.key).toArray();
      for (const child of result.children) this.insert(child);
      if (result.wal_keep_from != null && (state.wal_keep_from == null || BigInt(result.wal_keep_from) < BigInt(state.wal_keep_from))) state.wal_keep_from = result.wal_keep_from;
      this.storage.sql.exec("UPDATE collection_tasks SET done = 1 WHERE id = ?", id).toArray();
      this.save(state);
    });
  }
  marked(token) {
    this.transaction(() => {
      const state = this.valid(token);
      if (this.next(token)) throw new Error("CollectionMarkIncomplete");
      if (state.phase === "mark_artifacts") state.phase = "compact_wal";
      else if (state.phase === "mark_wal") state.phase = "wait";
      else throw new Error("CollectionPhaseMismatch");
      this.save(state);
    });
  }
  walPrepared(token, result) {
    this.transaction(() => {
      const state = this.valid(token);
      if (state.phase !== "compact_wal") throw new Error("CollectionPhaseMismatch");
      if (!result.has_more) {
        state.wal_prefix = result.wal_prefix;
        state.wal_retired_before = result.retired_before;
        // All HTTP/queue WASM executions have a hard 30-second host deadline.
        // This grace includes another 30 seconds for bounded clock skew.
        state.not_before = this.now() + 60000;
        for (const task of result.roots) this.insert(task);
        state.phase = "mark_wal";
      }
      this.save(state);
    });
  }
  ready(token) {
    return this.transaction(() => {
      const state = this.valid(token);
      if (state.phase !== "wait") throw new Error("CollectionPhaseMismatch");
      if (this.now() < state.not_before) return Math.ceil((state.not_before - this.now()) / 1000);
      state.phase = "sweep_artifacts"; this.save(state); return 0;
    });
  }
  deletable(token, keys) {
    if (keys.length > 128) throw new Error("CollectionBatchExceeded");
    return this.transaction(() => {
      const state = this.valid(token);
      return keys.filter(key => {
        let eligible = false;
        if (state.phase === "sweep_artifacts" && key.startsWith(state.artifact_prefix)) {
          const suffix = key.slice(state.artifact_prefix.length);
          eligible = /^[a-f0-9]{32}\/[a-f0-9]{64}$/.test(suffix) && BigInt(`0x${suffix.slice(0, 16)}`) < BigInt(state.retired_before);
        } else if (state.phase === "sweep_wal" && key.startsWith(state.wal_prefix)) {
          const suffix = key.slice(state.wal_prefix.length);
          eligible = /^[a-f0-9]{64}$/.test(suffix) || (/^[a-f0-9]{32}\/[a-f0-9]{64}$/.test(suffix) && BigInt(`0x${suffix.slice(0, 16)}`) < BigInt(state.wal_retired_before));
        } else if (state.phase === "sweep_manifests" && key.startsWith(state.manifest_prefix)) {
          const suffix = key.slice(state.manifest_prefix.length);
          // Below the irreversible native retirement floor, a manifest name
          // can never become a new valid publication or reader root.
          eligible = /^\d+\.bin$/.test(suffix) && BigInt(suffix.slice(0, -4)) < BigInt(state.floor);
        }
        return eligible && this.storage.sql.exec("SELECT key FROM collection_marks WHERE key = ?", key).toArray().length === 0;
      });
    });
  }
  swept(token, cursor) {
    this.transaction(() => {
      const state = this.valid(token);
      if (!["sweep_artifacts", "sweep_wal", "sweep_manifests"].includes(state.phase)) throw new Error("CollectionPhaseMismatch");
      state.cursor = cursor;
      if (cursor == null) state.phase = { sweep_artifacts: "sweep_wal", sweep_wal: "sweep_manifests", sweep_manifests: "cleanup" }[state.phase];
      this.save(state);
    });
  }
  cleanup(token) {
    return this.transaction(() => {
      const state = this.valid(token);
      if (state.phase !== "cleanup") throw new Error("CollectionPhaseMismatch");
      this.storage.sql.exec("DELETE FROM collection_tasks WHERE id IN (SELECT id FROM collection_tasks LIMIT 128)").toArray();
      this.storage.sql.exec("DELETE FROM collection_marks WHERE key IN (SELECT key FROM collection_marks LIMIT 128)").toArray();
      const remaining = this.storage.sql.exec("SELECT 1 FROM collection_tasks UNION ALL SELECT 1 FROM collection_marks LIMIT 1").toArray().length;
      if (!remaining) this.storage.kv.delete(STATE);
      return !remaining;
    });
  }
}
