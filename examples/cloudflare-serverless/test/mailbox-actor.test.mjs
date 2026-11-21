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

// The real Mailbox actor methods (receive, ingest, delete, purge, sync) over
// an in-memory transactional store, R2 bucket and search engine. The workerd
// integration test (test:mail-workerd) covers the same paths on the native
// engine but needs the serverless WASM build.
import { register } from "node:module";
import { test } from "node:test";
import assert from "node:assert/strict";
import { MailOutbox, MailSyncLog, MailDeletion, MailTrash, DAY_MS, digest, objectPrefix } from "../mail-core.mjs";

register(new URL("./support/workers-node-hooks.mjs", import.meta.url));
const { Mailbox } = await import("../mailbox-actor.mjs");

class Store {
  rows = new Map();
  pending = Promise.resolve();
  transaction(fn) {
    const task = this.pending.then(async () => {
      const rows = structuredClone(this.rows),
        result = await fn({
          get: async (k) => structuredClone(rows.get(k)),
          put: (k, v) => rows.set(k, structuredClone(v)),
          delete: (k) => rows.delete(k),
          list: async ({ prefix, startAfter = "", limit }) =>
            new Map([...rows].filter(([k]) => k.startsWith(prefix) && k > startAfter).sort(([a], [b]) => (a < b ? -1 : 1)).slice(0, limit)),
        });
      this.rows = rows;
      return result;
    });
    this.pending = task.then(() => {}, () => {});
    return task;
  }
}

function bucket() {
  const objects = new Map();
  const object = (bytes) => ({
    size: bytes.length,
    arrayBuffer: async () => bytes.slice().buffer,
    text: async () => new TextDecoder().decode(bytes),
  });
  return {
    objects,
    async put(key, value, options = {}) {
      if (options.onlyIf?.etagDoesNotMatch === "*" && objects.has(key)) return null;
      objects.set(key, typeof value === "string" ? new TextEncoder().encode(value) : new Uint8Array(value));
      return {};
    },
    async get(key) {
      return objects.has(key) ? object(objects.get(key)) : null;
    },
    async delete(keys) {
      for (const key of [keys].flat()) objects.delete(key);
    },
  };
}

// Search engine stand-in: idempotent by operation id, like the native WAL.
function engine() {
  const docs = new Map(),
    operations = new Set(),
    hooks = {};
  return {
    docs,
    hooks,
    async run(input) {
      if (input.op === "mutate") {
        await hooks.beforeMutate?.(input);
        if (operations.has(input.operation_id)) return { duplicate: true };
        operations.add(input.operation_id);
        for (const mutation of input.mutations)
          if (mutation.kind === "upsert") docs.set(mutation.doc_id, mutation.body);
          else if (mutation.kind === "delete") docs.delete(mutation.doc_id);
        return { applied: input.mutations.length };
      }
      if (input.op === "lookup") return docs.has(input.doc_id) ? { body: docs.get(input.doc_id) } : {};
      if (input.op === "publish") return { has_more: false };
      if (input.op === "search")
        return { hits: [...docs].filter(([, body]) => body.includes(input.text)).map(([doc_id]) => ({ doc_id })) };
      throw Error("unexpected op " + input.op);
    },
  };
}

function mailbox() {
  const id = crypto.randomUUID(),
    store = new Store(),
    objects = bucket(),
    search = engine();
  const actor = Object.create(Mailbox.prototype);
  Object.assign(actor, {
    ctx: { id: {} },
    env: { MAILBOXES: { idFromName: () => ({ equals: () => true }) }, MAIL_OBJECTS: objects },
    serviceStore: store,
    transaction: (fn) => store.transaction(fn),
    run: (input) => search.run(input),
  });
  actor.outbox = new MailOutbox(store);
  actor.syncLog = new MailSyncLog(store);
  actor.deletion = new MailDeletion(store, actor.syncLog);
  const clock = { now: 1_700_000_000_000 };
  actor.trashBin = new MailTrash(store, actor.syncLog, { clock: () => clock.now, retentionDays: 30 });
  return { id, actor, store, objects, search, clock, prefix: objectPrefix(id) };
}

const raw = (subject) =>
  "From: Customer <customer@example.net>\r\nTo: me@example.test\r\nSubject: " + subject +
  '\r\nMIME-Version: 1.0\r\nContent-Type: multipart/mixed; boundary="p"\r\n\r\n--p\r\nContent-Type: text/plain\r\n\r\n' +
  subject + " body needleword\r\n--p\r\nContent-Type: text/plain\r\nContent-Disposition: attachment; filename=\"a.txt\"\r\n\r\nsecret\r\n--p--\r\n";

// What the Worker's email() handler does, then the queue's ingest.
async function deliver(m, subject) {
  const bytes = new TextEncoder().encode(raw(subject)),
    receipt = crypto.randomUUID(),
    key = m.prefix + "raw/" + receipt;
  await m.objects.put(key, bytes, { onlyIf: { etagDoesNotMatch: "*" } });
  await m.actor.receive(m.id, { id: receipt, sha256: await digest(bytes), key, to: "me@example.test", from: "customer@example.net" });
  return receipt;
}
const keysOf = (m, receipt) => [...m.objects.objects.keys()].filter((k) => k.includes(receipt));

test("delete hides a message everywhere and purges only what it owns", async () => {
  const m = mailbox();
  const keep = await deliver(m, "Keep me");
  const doomed = await deliver(m, "Delete me");
  for (const receipt of [keep, doomed]) assert.deepEqual(await m.actor.ingest(m.id, receipt), { done: true });
  assert.equal((await m.actor.list(m.id)).messages.length, 2);
  assert.equal(keysOf(m, doomed).length, 2, "raw MIME and attachment stored per delivery");
  const projections = [...m.objects.objects.keys()].filter((k) => k.includes("/projections/"));
  assert.equal(projections.length, 2);

  assert.deepEqual(await m.actor.delete(m.id, doomed), { id: doomed, deleted: true });
  assert.deepEqual((await m.actor.list(m.id)).messages.map((x) => x.id), [keep]);
  assert.equal(await m.actor.message(m.id, doomed), null);
  assert.equal(await m.actor.attachment(m.id, doomed, 0), null);
  assert.deepEqual((await m.actor.search(m.id, "needleword")).messages.map((x) => x.id), [keep]);
  assert.equal(m.search.docs.has("message/" + doomed), false, "search document purged");
  assert.deepEqual(keysOf(m, doomed), [], "owned objects purged");
  assert.equal([...m.objects.objects.keys()].filter((k) => k.includes("/projections/")).length, 1);
  assert.equal(keysOf(m, keep).length, 2, "the other message is untouched");
  assert.deepEqual(await m.actor.pending(m.id), { inbound: [], outbound: [], purge: [], expire: false, publish: false }, "purged and published");

  const synced = await m.actor.sync(m.id, "");
  assert.deepEqual(synced.deleted, [doomed]);
  assert.deepEqual(synced.messages.map((x) => x.id), [keep]);
  assert.deepEqual(await m.actor.delete(m.id, doomed), { id: doomed, deleted: true }, "idempotent");
  await assert.rejects(m.actor.delete(m.id, crypto.randomUUID()), /UnknownMessage/);
});

test("a purge that fails part-way is completed by a retry", async () => {
  const m = mailbox();
  const doomed = await deliver(m, "Flaky");
  await m.actor.ingest(m.id, doomed);
  const remove = m.objects.delete;
  m.objects.delete = async () => {
    throw Error("R2 unavailable");
  };
  await assert.rejects(m.actor.delete(m.id, doomed), /R2 unavailable/);
  assert.equal(await m.actor.message(m.id, doomed), null, "hidden even though the purge failed");
  assert.deepEqual((await m.actor.pending(m.id)).purge, [doomed], "the sweep will retry");
  m.objects.delete = remove;
  await m.actor.purge(m.id, doomed);
  assert.deepEqual(keysOf(m, doomed), []);
  assert.deepEqual((await m.actor.pending(m.id)).purge, []);
});

test("a delete racing an ingest never resurrects the message or orphans objects", async () => {
  const m = mailbox();
  const doomed = await deliver(m, "Racing");
  // The user deletes while the ingest is about to upsert the search document:
  // the delete's purge runs first, then the ingest's upsert lands.
  m.search.hooks.beforeMutate = async (input) => {
    if (input.operation_id !== "mail/ingest/" + doomed) return;
    m.search.hooks.beforeMutate = null;
    await m.actor.delete(m.id, doomed);
  };
  const result = await m.actor.ingest(m.id, doomed);
  assert.deepEqual(result, { done: true, purge: true }, "the ingest re-arms the purge");
  assert.equal(m.search.docs.has("message/" + doomed), true, "the late upsert landed");
  await m.actor.purge(m.id, doomed); // what the queue hint does
  assert.equal(m.search.docs.has("message/" + doomed), false, "the re-armed purge removed it (new operation id)");
  assert.deepEqual(keysOf(m, doomed), [], "objects written by the ingest were purged too");
  assert.deepEqual((await m.actor.list(m.id)).messages, []);
  assert.deepEqual(await m.actor.ingest(m.id, doomed), { done: true }, "a replayed ingest stays deleted");
  assert.equal(m.search.docs.has("message/" + doomed), false);
});

test("deleting before ingest leaves nothing behind", async () => {
  const m = mailbox();
  const doomed = await deliver(m, "Never indexed");
  await m.actor.delete(m.id, doomed);
  assert.deepEqual((await m.actor.pending(m.id)).inbound, [], "no ingest is left pending");
  assert.deepEqual(keysOf(m, doomed), [], "the raw MIME is purged");
  assert.deepEqual(await m.actor.ingest(m.id, doomed), { done: true });
  assert.equal(m.search.docs.size, 0);
});

test("Trash hides a message from the inbox, search and devices, keeps it openable, and restores it", async () => {
  const m = mailbox();
  const keep = await deliver(m, "Keep me");
  const message = await deliver(m, "Trash me");
  for (const receipt of [keep, message]) await m.actor.ingest(m.id, receipt);
  const cursor = (await m.actor.sync(m.id, "")).cursor;

  const trashed = await m.actor.trash(m.id, message);
  assert.deepEqual(trashed, { id: message, trashed: true, delete_at: m.clock.now + 30 * DAY_MS });
  assert.deepEqual(await m.actor.trash(m.id, message), trashed, "trashing again changes nothing");
  assert.deepEqual((await m.actor.list(m.id)).messages.map((x) => x.id), [keep]);
  assert.deepEqual((await m.actor.search(m.id, "needleword")).messages.map((x) => x.id), [keep], "search excludes Trash");
  const opened = await m.actor.message(m.id, message);
  assert.equal(opened.trashed_at, m.clock.now, "trashed messages can still be opened");
  assert.equal(opened.delete_at, m.clock.now + 30 * DAY_MS);
  assert.ok(await m.actor.attachment(m.id, message, 0), "and their attachments downloaded");
  const trash = await m.actor.trashList(m.id);
  assert.deepEqual([trash.messages.map((x) => x.id), trash.retention_days], [[message], 30]);
  assert.equal(keysOf(m, message).length, 2, "nothing is purged while in Trash");
  assert.deepEqual((await m.actor.sync(m.id, cursor)).deleted, [message], "devices drop it");

  // Restore puts it back in the inbox and re-announces it to devices.
  const afterTrash = (await m.actor.sync(m.id, cursor)).cursor;
  assert.deepEqual(await m.actor.restore(m.id, message), { id: message, restored: true });
  assert.deepEqual(new Set((await m.actor.list(m.id)).messages.map((x) => x.id)), new Set([keep, message]));
  assert.equal((await m.actor.message(m.id, message)).trashed_at, undefined);
  assert.deepEqual((await m.actor.trashList(m.id)).messages, []);
  const resynced = await m.actor.sync(m.id, afterTrash);
  assert.deepEqual(resynced.messages.map((x) => x.id), [message], "devices fetch and embed it again");
  // A message restored then trashed again before a device syncs is not re-sent.
  await m.actor.trash(m.id, message);
  const replay = await m.actor.sync(m.id, afterTrash);
  assert.deepEqual([replay.messages, replay.deleted], [[], [message]]);
  await assert.rejects(m.actor.trash(m.id, crypto.randomUUID()), /UnknownMessage/);
});

test("retention deletes Trash older than the period forever; Empty Trash deletes it all", async () => {
  const m = mailbox();
  const [old, recent, inbox] = [await deliver(m, "Old"), await deliver(m, "Recent"), await deliver(m, "Inbox")];
  for (const receipt of [old, recent, inbox]) await m.actor.ingest(m.id, receipt);
  const pending = await deliver(m, "Still arriving");
  await assert.rejects(m.actor.trash(m.id, pending), /MessageNotReady/);

  await m.actor.trash(m.id, old);
  m.clock.now += 20 * DAY_MS;
  await m.actor.trash(m.id, recent);
  assert.equal((await m.actor.pending(m.id)).expire, false, "nothing has outlived 30 days yet");
  m.clock.now += 10 * DAY_MS + 1;
  assert.equal((await m.actor.pending(m.id)).expire, true, "the sweep will expire Trash");
  assert.deepEqual(await m.actor.expire(m.id), { deleted: 1, done: true });
  assert.equal(await m.actor.message(m.id, old), null, "the expired message is deleted forever");
  assert.deepEqual(keysOf(m, old), [], "with its objects");
  assert.ok(await m.actor.message(m.id, recent), "younger Trash is kept");
  assert.equal((await m.actor.pending(m.id)).expire, false);

  // Delete forever straight from Trash removes its Trash entries too.
  await m.actor.trash(m.id, inbox);
  await m.actor.delete(m.id, inbox);
  assert.deepEqual((await m.actor.trashList(m.id)).messages.map((x) => x.id), [recent]);
  assert.deepEqual(await m.actor.emptyTrash(m.id), { deleted: 1, done: true });
  assert.deepEqual((await m.actor.trashList(m.id)).messages, []);
  assert.equal(await m.actor.message(m.id, recent), null);
  assert.equal(m.search.docs.size, 0, "only the never-indexed delivery remains, unindexed");
});
