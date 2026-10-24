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

import { ObjectStoreError } from "./r2.mjs";
// Cache only immutable object names, never catalog/progress/WAL HEAD records.
// Values contain copied bytes and plain metadata, not request-owned I/O handles.
export function createImmutableReadCache({ maxBytes = 8 * 1024 * 1024, maxEntries = 128, includeCommitLog = false } = {}) {
  if (!Number.isSafeInteger(maxBytes) || maxBytes < 0 || !Number.isSafeInteger(maxEntries) || maxEntries < 0) throw new TypeError("invalid cache budget");
  const entries = new Map(); let retained = 0;
  // Commit log names are immutable within the native checkpoint floor.
  // Only the coordinator opts in; it reads that mutable floor uncached before
  // accepting cached cursors or acknowledging a write to a retired epoch.
  const eligible = key => (includeCommitLog && /\/coord-v1\/[a-f0-9]{64}\/epochs\/[a-f0-9]{32}\/(?:[0-9]{20}\.json|START|SEALED)$/.test(key)) || /\/wal-v1\/[a-f0-9]{64}\/pages\/(?:[a-f0-9]{32}\/)?[a-f0-9]{64}$/.test(key) ||
    /\/artifacts\/(?:sha256\/[a-f0-9]{2}\/[a-f0-9]{62}|graph\/[a-f0-9]{64}\/[a-f0-9]{32}\/[a-f0-9]{64})$/.test(key);
  function evict(key) { const item = entries.get(key); if (item) retained -= item.charge; entries.delete(key); }
  return {
    clear() { entries.clear(); retained = 0; },
    retainedBytes: () => retained,
    wrap(transport, scope) {
      return async (request, body) => {
        if (request.op !== "get" || !eligible(request.key ?? "")) return transport(request, body);
        const key = JSON.stringify([scope, request.bucket, request.key, request.range ?? null, request.if_match_etag ?? null]);
        let item = entries.get(key);
        if (item) {
          if (request.max_response_bytes != null && item.body.byteLength > request.max_response_bytes) throw new ObjectStoreError(4, "cached response exceeds budget");
          entries.delete(key); entries.set(key, item);
          return { json: { ...item.json }, body: item.body.slice() };
        }
        const result = await transport(request, body);
        const bytes = result.body;
        if (!bytes) return result;
        const charge = bytes.byteLength + key.length * 2 + JSON.stringify(result.json).length * 2 + 256;
        if (charge > maxBytes || maxEntries < 1) return result;
        while (entries.size >= maxEntries || charge > maxBytes - retained) evict(entries.keys().next().value);
        item = { body: bytes.slice(), json: { ...result.json }, charge };
        entries.set(key, item); retained += charge;
        return result;
      };
    },
  };
}
