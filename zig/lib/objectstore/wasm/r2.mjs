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

// Transport for the existing objectstore.Client WASM ABI. A binding map is
// supplied by the host, never resolved from untrusted account/bucket names.
export class ObjectStoreError extends Error {
  constructor(status, message, options) { super(message, options); this.status = status; }
}
// Normalize provider failures at every binding boundary. This performs one
// physical attempt: a lost write response remains indeterminate, and only the
// native receipt/conditional-commit protocol may recover it.
export function r2BindingFailure(error) {
  if (error instanceof ObjectStoreError) return error;
  const match = /\((10001|10043|10054|10058)\)\s*$/.exec(String(error));
  if (!match) return error;
  const limited = match[1] === "10058";
  const failure = new ObjectStoreError(limited ? 6 : 7, limited ? "ObjectStoreRateLimited" : "ObjectStoreTransportFailed", {cause:error});
  failure.retryable = true;
  failure.provider_code = Number(match[1]);
  return failure;
}
export async function callR2Binding(run) {
  try { return await run(); } catch (error) { throw r2BindingFailure(error); }
}
const fail = (status, message) => { throw new ObjectStoreError(status, message); };
function integer(value, name) {
  if (!Number.isSafeInteger(value) || value < 0) fail(3, `invalid ${name}`);
  return value;
}
function metadata(bucket, object) {
  return { bucket, key: object.key, etag: object.etag, content_length: object.size,
    content_type: object.httpMetadata?.contentType ?? null,
    last_modified_unix_ms: object.uploaded?.getTime() ?? null };
}
async function boundedBody(stream, limit) {
  const reader = stream.getReader();
  const chunks = [];
  let size = 0;
  try {
    while (true) {
      const item = await reader.read();
      if (item.done) break;
      if (item.value.byteLength > limit - size) {
        await reader.cancel();
        fail(4, "object response exceeds budget");
      }
      size += item.value.byteLength;
      chunks.push(item.value);
    }
  } finally { reader.releaseLock(); }
  const body = new Uint8Array(size);
  let offset = 0;
  for (const chunk of chunks) { body.set(chunk, offset); offset += chunk.byteLength; }
  return body;
}

export function createR2Transport(bindings, { maxResponseBytes = 8 * 1024 * 1024, maxPutBytes = 1024 * 1024 } = {}) {
  integer(maxResponseBytes, "response limit"); integer(maxPutBytes, "put limit");
  return async function objectCall(request, body = new Uint8Array()) {
    try {
    if (request.guard_key != null || request.guard_etag != null) fail(3, "guarded writes require a transactional coordinator");
    const binding = Object.hasOwn(bindings, request.bucket) ? bindings[request.bucket] : undefined;
    if (!binding) fail(3, "unbound bucket");
    if (request.op === "bucket_exists") return { json: { exists: true } };
    const key = request.key;
    if (request.op !== "list" && (typeof key !== "string" || new TextEncoder().encode(key).length > 1024)) fail(3, "invalid key");
    switch (request.op) {
      case "put": {
        if (body.byteLength > maxPutBytes) fail(4, "object write exceeds budget");
        if (request.if_none_match && request.if_match_etag != null) fail(3, "ambiguous condition");
        const onlyIf = request.if_none_match ? { etagDoesNotMatch: "*" }
          : request.if_match_etag != null ? { etagMatches: request.if_match_etag } : undefined;
        const options = { onlyIf,
          httpMetadata: request.content_type ? { contentType: request.content_type } : undefined };
        if (request.checksum_sha256_base64 != null) {
          const raw = Uint8Array.from(atob(request.checksum_sha256_base64), c => c.charCodeAt(0));
          if (raw.byteLength !== 32) fail(3, "invalid SHA256");
          options.sha256 = raw.buffer;
        }
        const result = await binding.put(key, body, options);
        if (result === null) fail(2, "conditional write conflict");
        return { json: { etag: result.etag } };
      }
      case "get": {
        const limit = Math.min(maxResponseBytes, request.max_response_bytes == null
          ? maxResponseBytes : integer(request.max_response_bytes, "response limit"));
        let range;
        if (request.range != null) {
          range = { offset: integer(request.range.offset, "range offset") };
          if (request.range.length != null) range.length = integer(request.range.length, "range length");
          if (range.length === 0) fail(3, "empty range");
        }
        const result = await binding.get(key, { range,
          onlyIf: request.if_match_etag != null ? { etagMatches: request.if_match_etag } : undefined });
        if (result === null) fail(1, "object missing");
        if (!result.body) fail(2, "conditional read conflict");
        // Avoid buffering a known oversized object. The stream bound also
        // protects against a provider returning more bytes than advertised.
        const expected = result.range?.length ?? result.size;
        if (expected > limit) { await result.body.cancel(); fail(4, "object response exceeds budget"); }
        const bytes = await boundedBody(result.body, limit);
        return { json: metadata(request.bucket, result), body: bytes };
      }
      case "head": {
        const result = await binding.head(key);
        if (result === null) fail(1, "object missing");
        return { json: metadata(request.bucket, result) };
      }
      case "delete":
        // R2 bindings do not expose an atomic conditional DELETE. A HEAD then
        // DELETE would violate fencing. Never silently weaken that contract.
        if (request.if_match_etag != null) fail(3, "conditional delete unsupported");
        await binding.delete(key);
        return { json: {} };
      case "list": {
        const limit = integer(request.max_keys, "list limit");
        if (limit === 0 || limit > 1000) fail(3, "invalid list limit");
        if (request.start_after != null && request.continuation_token != null) fail(3, "ambiguous continuation");
        const result = await binding.list({ prefix: request.prefix ?? "", limit,
          cursor: request.continuation_token ?? undefined, startAfter: request.start_after ?? undefined,
          delimiter: request.recursive ? undefined : request.delimiter ?? "/" });
        return { json: {
          entries: result.objects.map(o => ({ key: o.key, etag: o.etag, size: o.size, last_modified_unix_ms: o.uploaded?.getTime() ?? null })),
          common_prefixes: result.delimitedPrefixes ?? [],
          next_continuation_token: result.truncated ? result.cursor : null,
        } };
      }
      default: fail(3, "unsupported object operation");
    }
    } catch (error) { throw r2BindingFailure(error); }
  };
}
