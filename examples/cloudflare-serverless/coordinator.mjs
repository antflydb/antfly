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
    if (!profile.model?.startsWith("@cf/") || !Number.isSafeInteger(profile.dimensions) || profile.dimensions < 1 || profile.dimensions > 1536) return false;
    return this.ctx.storage.transactionSync(() => {
      const encoded = JSON.stringify(profile), key = "__antfly_embedding_profile";
      const previous = this.ctx.storage.kv.get(key);
      if (previous != null) return previous === encoded;
      this.ctx.storage.kv.put(key, encoded); return true;
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
