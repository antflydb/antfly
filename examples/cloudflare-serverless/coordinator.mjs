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

import { DurableObject } from "cloudflare:workers";
import { CollectionJournal } from "./collection-journal.mjs";
// Small WAL/progress metadata and the bounded collection journal live here.
// Immutable database pages remain in R2, avoiding a hot R2 HEAD overwrite.
export class CommitCoordinator extends DurableObject {
  constructor(ctx, env) { super(ctx, env); this.journal = new CollectionJournal(ctx.storage); }
  publicationBudget(maximum) {
    if (!Number.isSafeInteger(maximum) || maximum < 1 || maximum > 256) throw new Error("InvalidPublicationBudget");
    return Math.min(maximum, this.ctx.storage.kv.get("__antfly_publication_budget") ?? maximum);
  }
  lowerPublicationBudget(maximum) {
    return this.ctx.storage.transactionSync(() => {
      const next = Math.max(1, Math.floor(this.publicationBudget(maximum) / 2));
      this.ctx.storage.kv.put("__antfly_publication_budget", next); return next;
    });
  }
  ensureEmbeddingProfile(profile) {
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
    return this.ctx.storage.transactionSync(() => {
      const key = `__antfly_embedding_gc/${namespace}`, state = this.ctx.storage.kv.get(key) ?? {};
      if ((state.payload_cursor ?? null) !== expected) return false;
      state.payload_cursor = next; this.ctx.storage.kv.put(key, state); return true;
    });
  }
  collection(op, args) {
    return this.journal.rpc(op, args);
  }

  async objectCall(request, body) {
    if (!(/\/wal-v1\/[a-f0-9]{64}\/HEAD$/.test(request.key ?? "") || /\/progress\//.test(request.key ?? ""))) return { status: 3 };
    if (body.byteLength > 4096) return { status: 4 };
    return this.ctx.storage.transactionSync(() => {
      if (request.collection != null) {
        try { this.journal.valid(request.collection); } catch { return { status: 5 }; }
      }
      const previous = this.ctx.storage.kv.get(request.key);
      if (request.op === "put") {
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
    });
  }
}
