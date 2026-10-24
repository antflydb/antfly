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

// A single suspended stack per instance. I/O completes exactly once; rewind
// restores the original computation instead of re-executing writes or queries.
import { ObjectStoreError } from "./r2.mjs";

export function validatePutBatch(request, bodyLength) {
  if (!Array.isArray(request.items) || request.items.length < 1 || request.items.length > 8) throw new ObjectStoreError(3, "invalid upload batch");
  let end = 0;
  for (const item of request.items) {
    if (typeof item.key !== "string" || !Number.isSafeInteger(item.offset) || item.offset !== end || !Number.isSafeInteger(item.length) || item.length < 0 || item.length > bodyLength - end) throw new ObjectStoreError(3, "invalid upload batch extent");
    end += item.length;
  }
  if (end !== bodyLength) throw new ObjectStoreError(3, "unclaimed upload batch bytes");
  return request.items.length;
}

// Join every started upload before returning an error. Partially successful
// immutable uploads are orphans; callers never publish a partial batch.
export async function executePutBatch(transport, request, body, check = () => {}) {
  validatePutBatch(request, body.byteLength);
  const results = Array(request.items.length);
  let next = 0, failure;
  async function upload() {
    while (!failure && next < request.items.length) {
      const index = next++, item = request.items[index];
      try {
        check();
        const value = await transport({...item, op: "put", bucket: request.bucket}, body.subarray(item.offset, item.offset + item.length));
        results[index] = value.json;
      } catch (error) { failure ??= error instanceof Error ? error : new Error(String(error)); }
    }
  }
  await Promise.all(Array.from({length: Math.min(4, request.items.length)}, upload));
  if (failure) throw failure;
  check();
  return {json: {results}};
}
