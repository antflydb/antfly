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

import {DurableObject} from "cloudflare:workers";
import {CollectionJournal} from "../collection-journal.mjs";

// Disposable fixture only: no native WASM, R2, app data, or model binding.
export class CommitCoordinator extends DurableObject {
  step({rows = 4096, steps = 32, mode = "drained"}) {
    if (!Number.isSafeInteger(rows) || rows < 128 || rows > 32768 || !Number.isSafeInteger(steps) || steps < 1 || steps > 64 || !["drained", "legacy"].includes(mode)) throw new Error("InvalidSoakProfile");
    const real = this.ctx.storage;
    const meta = real.kv.get("__soak") ?? {rows, mode, rounds: 0, calls: 0, statements: 0, undrained: 0, phase: "mark", peak_database_bytes: 0};
    if (meta.rows !== rows || meta.mode !== mode) throw new Error("SoakProfileChanged");
    let stage = "constructor";
    const sql = {exec: (query, ...args) => {
      meta.statements++;
      const cursor = real.sql.exec(query, ...args);
      if (mode === "legacy" && /^(CREATE|INSERT|UPDATE|DELETE)/.test(query)) {
        // Reproduce the original ignored mutation-cursor lifetime. Reads and
        // all SQL/transaction contents remain identical to the drained path.
        meta.undrained++;
        return {toArray() {void cursor; return [];}};
      }
      return cursor;
    }};
    const storage = {sql, kv: real.kv, transactionSync: fn => real.transactionSync(fn)};
    try {
      const j = new CollectionJournal(storage);
      meta.calls++;
      if (meta.phase === "cleanup") {
        stage = "cleanup";
        for (let i = 0; i < steps; i++) {
          sql.exec("DELETE FROM collection_tasks WHERE rowid IN (SELECT rowid FROM collection_tasks LIMIT 128)").toArray();
          sql.exec("DELETE FROM collection_marks WHERE rowid IN (SELECT rowid FROM collection_marks LIMIT 128)").toArray();
          if (!sql.exec("SELECT 1 FROM collection_tasks UNION ALL SELECT 1 FROM collection_marks LIMIT 1").toArray().length) {
            real.kv.delete("__antfly_collection"); meta.rounds++; meta.phase = "mark"; break;
          }
        }
      } else {
        stage = "claim";
        const claim = j.claim();
        if (claim.busy) return {ok: true, busy: true, ...meta};
        const token = claim.token;
        try {
          if (claim.state.phase === "prepare") {
            stage = "prepare";
            j.prepared(token, {roots: [{kind: "manifest", version: "0"}], floor: "1", retired_before: "1", pin_prefix: "fixture/", artifact_prefix: "artifacts/", manifest_prefix: "manifests/"}); j.pins(token);
          }
          for (let i = 0; i < steps; i++) {
            stage = "next"; const next = j.next(token);
            if (!next) {
              stage = "marked"; j.marked(token);
              const marks = sql.exec("SELECT COUNT(*) AS count FROM collection_marks").toArray()[0].count;
              if (marks !== rows + Math.ceil(rows / 128)) throw new Error("SoakMarkCountMismatch");
              meta.phase = "cleanup"; break;
            }
            let children = [], key;
            if (next.task.kind === "manifest") {
              const offset = Number(next.task.version); key = "frontier/" + offset;
              children = Array.from({length: Math.min(128, rows-offset)}, (_, n) => ({kind: "blob", artifact: {artifact_id: "a".repeat(175), checksum: "b".repeat(64), byte_len: offset+n+1, metadata_version: 0}}));
              if (offset + 128 < rows) children.push({kind: "manifest", version: String(offset+128)});
            } else key = "body/" + next.task.artifact.byte_len;
            stage = "traced"; j.traced(token, next.id, {key, children});
          }
        } finally { const prior = stage; stage = "release"; j.release(token); stage = prior; }
      }
      meta.peak_database_bytes = Math.max(meta.peak_database_bytes, real.sql.databaseSize ?? 0);
      real.kv.put("__soak", meta);
      return {ok: true, ...meta};
    } catch (error) {
      return {ok: false, stage, error: String(error.message ?? error), ...meta};
    }
  }
  async destroyFixture() {await this.ctx.storage.deleteAll();}
}
export default {
  async fetch(request, env) {
    if (request.method !== "POST" || !env.API_TOKEN || request.headers.get("Authorization") !== `Bearer ${env.API_TOKEN}` || !env.ANTFLY_INCARNATION?.startsWith("qualification-")) return new Response("unauthorized", {status: 401});
    const input = await request.json();
    if (!/^[a-zA-Z0-9-]{1,64}$/.test(input.run ?? "") || !["legacy", "drained"].includes(input.mode)) return new Response("invalid fixture", {status: 400});
    const stub = env.COMMITS.getByName(`${env.ANTFLY_INCARNATION}/sqlite-soak/${input.run}/${input.mode}`);
    if (input.op === "destroy") {await stub.destroyFixture(); return Response.json({destroyed: true});}
    return Response.json(await stub.step(input));
  },
};
