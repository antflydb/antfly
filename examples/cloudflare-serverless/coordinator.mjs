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

import {tenantEnvs} from "./tenants.mjs";
import {encode as encodeCoordinatorValue} from "./r2-coordinator-store.mjs";
import {openAuthority} from "./coordinator-authority.mjs";
import { DurableObject } from "cloudflare:workers";
import { CollectionJournal } from "./collection-journal.mjs";
// Small WAL/progress metadata and the bounded collection journal live here.
// Immutable database pages remain in R2, avoiding a hot R2 HEAD overwrite.
export class CommitCoordinator extends DurableObject {
  constructor(ctx, env) { super(ctx, env); if(env.ANTFLY_TENANTS){const binding=env.LEGACY_COMMITS??env.COMMITS;env=tenantEnvs(env).find(candidate=>binding.idFromName(candidate.ANTFLY_INCARNATION).equals(ctx.id));if(!env)throw new Error("UnknownTenant");} this.journal = new CollectionJournal(ctx.storage); this.namespace = env.ANTFLY_INCARNATION;
    ctx.blockConcurrencyWhile(async () => {await openAuthority(env, "sqlite", {allowRetired: !!ctx.storage.kv.get("__antfly_r2_migration"), allowExistingSqlite: ctx.storage.kv.list({limit:1}).size > 0});});
  }
  assertNotMigrating() {
    if (this.ctx.storage.kv.get("__antfly_r2_migration")) throw new Error("CoordinatorMigrationFrozen");
  }
  frozenR2Status() {return this.ctx.storage.kv.get("__antfly_r2_migration") ?? null;}
  freezeForR2(namespace) {
    if (namespace !== this.namespace) throw new Error("CoordinatorMigrationNamespaceMismatch");
    return this.ctx.storage.transactionSync(() => {
      let state = this.ctx.storage.kv.get("__antfly_r2_migration");
      if (!state) {state = {token: crypto.randomUUID(), namespace, phase: "frozen", ready_at: Date.now()+60000}; this.ctx.storage.kv.put("__antfly_r2_migration", state);}
      return state;
    });
  }
  exportFrozenR2({token, kind, after = ""}) {
    const frozen = this.ctx.storage.kv.get("__antfly_r2_migration");
    if (!frozen || frozen.token !== token || Date.now() < frozen.ready_at) throw new Error("CoordinatorMigrationNotReady");
    let rows;
    if (kind === "kv") rows = [...this.ctx.storage.kv.list({startAfter: after, limit: 128})];
    else if (kind === "tasks") rows = this.ctx.storage.sql.exec("SELECT id, task, done FROM collection_tasks WHERE id > ? ORDER BY id LIMIT 128", after).toArray().map(row => [row.id, {task: JSON.parse(row.task), done: !!row.done}]);
    else if (kind === "marks") rows = this.ctx.storage.sql.exec("SELECT key FROM collection_marks WHERE key > ? ORDER BY key LIMIT 128", after).toArray().map(row => [row.key, true]);
    else throw new Error("CoordinatorMigrationPhaseInvalid");
    const selected=[];let bytes=0,next=after;
    for(const row of rows) {
      const size=new TextEncoder().encode(JSON.stringify({kind:"put",key:row[0],value:encodeCoordinatorValue(row[1])})).byteLength;
      if(bytes+size>384*1024) {if(!selected.length)throw new Error("CoordinatorMigrationValueExceeded");break;}
      bytes+=size;selected.push(row);next=row[0];
    }
    return {rows:selected.filter(([key])=>key!=="__antfly_r2_migration"),complete:selected.length===rows.length&&rows.length<128,next};
  }
  completeR2Migration(token) {
    return this.ctx.storage.transactionSync(() => {
      const state = this.ctx.storage.kv.get("__antfly_r2_migration");
      if (!state || state.token !== token) throw new Error("CoordinatorMigrationTokenMismatch");
      state.phase = "r2-active"; this.ctx.storage.kv.put("__antfly_r2_migration", state); return true;
    });
  }
  publicationBudget(maximum) {
    this.assertNotMigrating();
    if (!Number.isSafeInteger(maximum) || maximum < 1 || maximum > 256) throw new Error("InvalidPublicationBudget");
    return Math.min(maximum, this.ctx.storage.kv.get("__antfly_publication_budget") ?? maximum);
  }
  lowerPublicationBudget(maximum) {
    this.assertNotMigrating();
    return this.ctx.storage.transactionSync(() => {
      const next = Math.max(1, Math.floor(this.publicationBudget(maximum) / 2));
      this.ctx.storage.kv.put("__antfly_publication_budget", next); return next;
    });
  }
  ensureEmbeddingProfile(profile) {
    this.assertNotMigrating();
    if (!profile.model?.startsWith("@cf/") || !Number.isSafeInteger(profile.dimensions) || profile.dimensions < 1 || profile.dimensions > 1536 || typeof (profile.revision ?? "1") !== "string" || (profile.revision ?? "1").length < 1 || (profile.revision ?? "1").length > 256) return false;
    return this.ctx.storage.transactionSync(() => {
      const encoded = JSON.stringify(profile), key = "__antfly_embedding_profile";
      const previous = this.ctx.storage.kv.get(key);
      if (previous != null) {
        const old = JSON.parse(previous);
        return old.model === profile.model && old.dimensions === profile.dimensions && (old.revision ?? "1") === (profile.revision ?? "1");
      }
      this.ctx.storage.kv.put(key, encoded); return true;
    });
  }
  pruneEmbeddingCheckpointMetadata(namespace, today) {
    this.assertNotMigrating();
    if (typeof namespace !== "string" || !namespace.length || !Number.isSafeInteger(today) || today < 0) throw new Error("InvalidEmbeddingCheckpointSweep");
    return this.ctx.storage.transactionSync(() => {
      const stateKey = `__antfly_embedding_gc/${namespace}`;
      const state = this.ctx.storage.kv.get(stateKey) ?? {};
      const prefix = `antfly/progress/${namespace}/EMBEDDING_CACHE/`;
      const entries = Array.from(this.ctx.storage.kv.list({prefix, startAfter: state.metadata_cursor, limit: 128}));
      for (const [key] of entries) {
        const day = key.slice(prefix.length).split("/")[0];
        if (/^[a-f0-9]{8}$/.test(day) && parseInt(day, 16) + 1 < today) this.ctx.storage.kv.delete(key);
      }
      state.metadata_cursor = entries.length === 128 ? entries.at(-1)[0] : undefined;
      this.ctx.storage.kv.put(stateKey, state);
      return state.payload_cursor ?? null;
    });
  }
  saveEmbeddingCheckpointCursor(namespace, expected, next) {
    this.assertNotMigrating();
    return this.ctx.storage.transactionSync(() => {
      const key = `__antfly_embedding_gc/${namespace}`, state = this.ctx.storage.kv.get(key) ?? {};
      if ((state.payload_cursor ?? null) !== expected) return false;
      state.payload_cursor = next; this.ctx.storage.kv.put(key, state); return true;
    });
  }
  reportPressure(error, op) {
    if (/\bSQLITE_NOMEM\b/.test(String(error))) {
      // Observe without writing into an already pressured SQLite instance.
      const sample = {event: "antfly.sqlite.pressure", op, failure_code: "SQLITE_NOMEM", phase: null, database_bytes: null};
      try { sample.phase = this.journal.state()?.phase ?? null; } catch {}
      try { sample.database_bytes = this.ctx.storage.sql.databaseSize ?? null; } catch {}
      try { console.error(JSON.stringify(sample)); } catch {}
    }
  }
  collection(op, args) {
    this.assertNotMigrating();
    try { return this.journal.rpc(op, args); }
    catch (error) { this.reportPressure(error, op); throw error; }
  }

  async objectCall(request, body) {
    this.assertNotMigrating();
    if (!(/\/wal-v1\/[a-f0-9]{64}\/HEAD$/.test(request.key ?? "") || /\/progress\//.test(request.key ?? ""))) return { status: 3 };
    if (body.byteLength > 4096) return { status: 4 };
    try { return this.ctx.storage.transactionSync(() => {
      if (request.collection != null) {
        try { this.journal.valid(request.collection); } catch { return { status: 5 }; }
      }
      const previous = this.ctx.storage.kv.get(request.key);
      if (request.op === "put") {
        if (request.guard_key != null || request.guard_etag != null) {
          // Job progress and HEAD must share this transaction. Comparing a
          // lease in a preceding RPC leaves a takeover/collection race.
          const parent = request.key.slice(0, request.key.lastIndexOf("/"));
          if (!request.key.endsWith("/REINDEX") || request.guard_key !== `${parent}/HEAD` || typeof request.guard_etag !== "string") return {status: 3};
          if (this.ctx.storage.kv.get(request.guard_key)?.etag !== request.guard_etag) return {status: 2};
        }
        if ((request.if_none_match && previous) ||
          (request.if_match_etag != null && previous?.etag !== request.if_match_etag)) return { status: 2 };
        const etag = crypto.randomUUID();
        this.ctx.storage.kv.put(request.key, { etag, body, contentType: request.content_type ?? null });
        return { status: 0, json: { etag } };
      }
      if (request.op === "get" || request.op === "head") {
        if (!previous) return { status: 1 };
        if (request.if_match_etag != null && previous.etag !== request.if_match_etag) return { status: 2 };
        return { status: 0, json: { bucket: request.bucket, key: request.key,
          etag: previous.etag, content_length: previous.body.byteLength, content_type: previous.contentType }, body: request.op === "get" ? previous.body : undefined };
      }
      return { status: 3 };
    }); } catch (error) { this.reportPressure(error, request.op); throw error; }
  }
}
