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

import {test} from "node:test";
import assert from "node:assert/strict";
import {executePutBatch, validatePutBatch} from "../../../zig/lib/objectstore/wasm/batch.mjs";
const request = {op: "put_many", bucket: "database", items: Array.from({length: 8}, (_, i) => ({key: String(i), offset: i, length: 1}))};
test("upload batches bound concurrency and preserve result ordering", async () => {
  let active = 0, peak = 0;
  const result = await executePutBatch(async (item, body) => {
    peak = Math.max(peak, ++active); assert.equal(body[0], Number(item.key));
    await new Promise(resolve => setTimeout(resolve, 2)); active--;
    return {json: {etag: item.key}};
  }, request, Uint8Array.from({length: 8}, (_, i) => i));
  assert.equal(peak, 4); assert.equal(active, 0);
  assert.deepEqual(result.json.results.map(item => item.etag), request.items.map(item => item.key));
});
test("failed upload batches join in-flight writes and stop scheduling new work", async () => {
  const started = [], finished = [];
  await assert.rejects(executePutBatch(async item => {
    started.push(item.key);
    if (item.key === "0") throw new Error("injected upload loss");
    await new Promise(resolve => setTimeout(resolve, 5)); finished.push(item.key);
    return {json: {}};
  }, request, new Uint8Array(8)), /injected upload loss/);
  assert.deepEqual(started, ["0", "1", "2", "3"]);
  assert.deepEqual(finished, ["1", "2", "3"]);
});
test("upload batch extents cannot alias bytes or amplify an admitted request", () => {
  assert.throws(() => validatePutBatch({...request, items: request.items.map(item => ({...item, offset: 0}))}, 8), /extent/);
  assert.throws(() => validatePutBatch(request, 9), /unclaimed/);
  assert.throws(() => validatePutBatch({...request, items: [...request.items, request.items[0]]}, 8), /invalid upload batch/);
});
