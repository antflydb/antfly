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

import { test } from "node:test";
import assert from "node:assert/strict";
import { fixture } from "./collection-fixture.mjs";

const plan = {roots: [{kind: "manifest", version: "10"}], floor: "9", retired_before: "5", pin_prefix: "pins/", artifact_prefix: "artifacts/", manifest_prefix: "manifests/"};
const scoped = epoch => `artifacts/${epoch.toString(16).padStart(16, "0")}${"1".repeat(16)}/${"a".repeat(64)}`;
test("durable collection resumes marks, protects pins, and fences stale collectors", () => {
  const f = fixture();
  try {
    let j = f.journal(); const old = j.claim().token;
    assert.equal(j.claim().busy, true); j.prepared(old, plan);
    f.storage.kv.put("pins/8", {body: new TextEncoder().encode("10000000000000")}); j.pins(old);
    const first = j.next(old);
    assert.throws(() => j.traced(old, first.id, { key: "must-rollback", children: [undefined] }));
    assert.equal(j.next(old).id, first.id);
    assert.equal(f.storage.sql.exec("SELECT key FROM collection_marks WHERE key = ?", "must-rollback").toArray().length, 0);
    assert.throws(() => j.marked(old), /MarkIncomplete/);
    j.traced(old, first.id, {key: "manifest-first", children: [{kind: "blob", version: "child"}], wal_keep_from: "42"});
    j.traced(old, first.id, {key: "wrong-duplicate", children: []}); j.release(old);
    j = f.journal(); const token = j.claim(old.id).token;
    assert.throws(() => j.traced(old, first.id, {}), /LeaseLost/);
    let node; while ((node = j.next(token))) j.traced(token, node.id, {key: node.task.version === "8" ? scoped(1) : node.id, children: [], wal_keep_from: "17"});
    assert.equal(j.state().wal_keep_from, "17"); j.marked(token);
    j.walPrepared(token, {has_more: true}); assert.equal(j.state().phase, "compact_wal");
    j.walPrepared(token, {has_more: false, roots: [], wal_prefix: "wal/pages/", retired_before: "3"});
    j.marked(token); assert.equal(j.ready(token), 60); j.release(token); f.tick(60001);
    const resumed = j.claim(old.id).token; assert.equal(j.ready(resumed), 0);
    assert.deepEqual(j.deletable(resumed, [scoped(1), scoped(2), scoped(5), "other/key"]), [scoped(2)]); j.swept(resumed, null);
    const legacy = `wal/pages/${"a".repeat(64)}`, newer = `wal/pages/${"0".repeat(15)}3${"0".repeat(16)}/${"b".repeat(64)}`;
    assert.deepEqual(j.deletable(resumed, [legacy, newer]), [legacy]); j.swept(resumed, null);
    assert.deepEqual(j.deletable(resumed, ["manifests/8.bin", "manifests/9.bin", "manifests/100.bin"]), ["manifests/8.bin"]); j.swept(resumed, null);
    while (!j.cleanup(resumed)) {} assert.equal(j.claim(old.id).done, true);
  } finally { f.db.close(); }
});
test("expired collectors cannot checkpoint or authorize deletion", () => {
  const f = fixture(); try {
    const j = f.journal(), old = j.claim().token; f.tick(30001); const next = j.claim(old.id).token;
    assert.notEqual(old.lease, next.lease); assert.throws(() => j.prepared(old, plan), /LeaseLost/);
    assert.throws(() => j.deletable(old, [scoped(1)]), /LeaseLost/); j.prepared(next, plan);
  } finally { f.db.close(); }
});


test("embedding scratch pruning stays scoped, bounded and resumable after deleting a listed page", async () => {
  const {Bucket} = await import("./object-fixture.mjs");
  const {pruneEmbeddingCheckpoints} = await import("../embedding-checkpoints.mjs");
  const bucket = new Bucket(), namespace = "checkpoint-prune";
  const today = Math.floor(Date.now()/86400000);
  const domain = Buffer.from(await crypto.subtle.digest("SHA-256", new TextEncoder().encode(namespace))).toString("hex");
  const prefix = `antfly/embedding-cache/${domain}/`;
  for (let i = 0; i < 130; i++) await bucket.put(`${prefix}${(today-3).toString(16).padStart(8,"0")}/${String(i).padStart(64,"0")}`, new Uint8Array([1]));
  const current = `${prefix}${today.toString(16).padStart(8,"0")}/${"a".repeat(64)}`;
  const foreign = `antfly/embedding-cache/other/${"0".repeat(8)}/${"b".repeat(64)}`;
  await bucket.put(current, new Uint8Array([2])); await bucket.put(foreign, new Uint8Array([3]));
  let cursor = null;
  const coordinator = {async pruneEmbeddingCheckpointMetadata(ns, day) {assert.equal(ns, namespace); assert.equal(day, today); return cursor;},
    async saveEmbeddingCheckpointCursor(_ns, expected, next) {assert.equal(expected, cursor); cursor = next;}};
  const env = {ANTFLY_INCARNATION: namespace, DATABASE: bucket, COMMITS: {getByName: () => coordinator}};
  assert.equal((await pruneEmbeddingCheckpoints(env)).deleted, 128);
  assert.equal((await pruneEmbeddingCheckpoints(env)).deleted, 2);
  assert.deepEqual([...bucket.objects.keys()].sort(), [current, foreign].sort());
});
