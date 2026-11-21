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

// On-device search: sync log, Worker routes, local document schema, model
// publishing and the opt-in UI. Browser-only pieces (OPFS, WASM inference)
// are covered by the embedded WASM smoke test and @antfly/web runtime tests.
import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, readFile, rm, writeFile, mkdir } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { JSDOM } from "jsdom";
import { MailSyncLog, MailDeletion, ownedObjects, syncMessage, syncLimits } from "../mail-core.mjs";
import worker, { contentSecurityPolicy } from "../mail-worker.mjs";
import { localSchema, documentFor, searchRequest, hitsFrom, embeddingText } from "../mail-public/local-schema.js";
import { stripSafetensors, prepare, MEDIA_TENSOR } from "../publish-local-model.mjs";

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
            new Map(
              [...rows]
                .filter(([k]) => k.startsWith(prefix) && k > startAfter)
                .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
                .slice(0, limit)
            ),
        });
      this.rows = rows;
      return result;
    });
    this.pending = task.then(
      () => {},
      () => {}
    );
    return task;
  }
}

test("sync log assigns one ordered sequence per message and backfills old mailboxes once", async () => {
  const store = new Store(),
    log = new MailSyncLog(store),
    old = [crypto.randomUUID(), crypto.randomUUID(), crypto.randomUUID()];
  // Messages indexed before the log existed only have received-order entries.
  await store.transaction(async (tx) => {
    old.forEach((id, i) => tx.put("mail/v1/order/" + String(9e12 - i).padStart(13, "0") + "/" + id, id));
  });
  const fresh = crypto.randomUUID();
  await store.transaction((tx) => log.record(tx, fresh));
  assert.equal(await store.transaction((tx) => log.record(tx, fresh)), false, "a message is logged once");
  assert.equal(await log.backfill(2), false);
  assert.equal(await log.backfill(2), true);
  assert.equal(await log.backfill(2), true, "completed backfill is a no-op");
  // The fresh message was already logged and must not appear twice.
  await store.transaction((tx) => tx.put("mail/v1/order/0000000000001/" + fresh, fresh));
  const seen = [];
  let cursor = "";
  for (;;) {
    const page = await log.page(cursor, 2);
    seen.push(...page.ids);
    assert.ok(Number(page.cursor) >= Number(cursor || 0));
    cursor = page.cursor;
    if (!page.more) break;
  }
  assert.equal(seen[0], fresh);
  assert.deepEqual(new Set(seen), new Set([fresh, ...old]));
  assert.equal(seen.length, 4);
  const empty = await log.page(cursor);
  assert.deepEqual(empty.ids, []);
  assert.equal(empty.cursor, cursor);
  for (const bad of ["x", "-1", "99999999999999", "1".repeat(14)])
    await assert.rejects(log.page(bad), /InvalidSyncCursor/);
  // A replica ahead of the log (e.g. after a restore) is told to rebuild.
  const ahead = await log.page(String(Number(cursor) + 1));
  assert.deepEqual([ahead.reset, ahead.cursor, ahead.ids.length, ahead.more], [true, "", 0, true]);
});

test("deletion hides a message at once, tombstones it for replicas and purges what it owns", async () => {
  const store = new Store(),
    log = new MailSyncLog(store),
    deletion = new MailDeletion(store, log, { clock: () => 42 }),
    id = crypto.randomUUID(),
    other = crypto.randomUUID(),
    prefix = "mail/" + crypto.randomUUID() + "/";
  const receipt = {
    id,
    state: "indexed",
    received_at: 1_700_000_000_000,
    key: prefix + "raw/" + id,
    projection: { key: prefix + "projections/" + "a".repeat(64), sha256: "a".repeat(64) },
    prepared: {
      attachments: [
        { index: 0, key: prefix + "attachments/" + id + "/0" },
        { index: 1, key: prefix + "attachments/" + "b".repeat(64) }, // legacy, may be shared
      ],
    },
  };
  const order = "mail/v1/order/" + String(9999999999999 - receipt.received_at).padStart(13, "0") + "/" + id;
  await store.transaction(async (tx) => {
    tx.put("mail/v1/receipt/" + id, receipt);
    tx.put(order, id);
    await log.record(tx, id);
    await log.record(tx, other);
  });
  const before = await log.page("");
  assert.deepEqual(before.ids, [id, other]);

  const marked = await deletion.mark(id);
  assert.equal(marked.key, receipt.key, "mark returns the receipt to purge");
  assert.equal(await deletion.isDeleted(id), true);
  assert.equal(await deletion.isDeleted(other), false);
  assert.equal(await store.transaction((tx) => tx.get(order)), undefined, "gone from the listing at once");
  assert.equal(await deletion.mark(id), null, "deleting twice is a no-op");
  await assert.rejects(deletion.mark(crypto.randomUUID()), /UnknownMessage/);

  // Replicas get exactly one tombstone after the earlier upsert entries.
  const after = await log.page(before.cursor);
  assert.deepEqual([after.ids, after.deleted], [[], [id]]);
  assert.deepEqual((await log.page("")).deleted, [id]);

  // Only per-message objects are purged; legacy content-addressed ones may be shared.
  assert.deepEqual(ownedObjects(prefix, await deletion.purging(id)), [
    prefix + "raw/" + id,
    prefix + "projections/" + "a".repeat(64),
    prefix + "attachments/" + id + "/0",
  ]);
  assert.deepEqual(ownedObjects(prefix, { ...receipt, key: prefix + "raw/" + "c".repeat(64) }).includes(prefix + "raw/" + "c".repeat(64)), false);
  assert.deepEqual(ownedObjects("mail/other/", receipt), [], "never another mailbox's objects");
  assert.deepEqual(await deletion.pending(), [id]);

  await deletion.finish(id);
  assert.equal(await deletion.purging(id), null);
  assert.deepEqual(await deletion.pending(), []);
  const tombstone = await store.transaction((tx) => tx.get("mail/v1/receipt/" + id));
  assert.deepEqual(tombstone, { id, state: "deleted", deleted_at: 42, purge_attempt: 0 }, "object keys are dropped");

  // An ingest that indexed the message after its purge re-arms it with a new
  // attempt number, since purge mutations are idempotent by operation id.
  await store.transaction((tx) => deletion.requirePurge(tx, id));
  assert.equal((await deletion.purging(id)).purge_attempt, 1);
  await deletion.finish(id);
  await store.transaction((tx) => deletion.requirePurge(tx, id));
  assert.equal((await deletion.purging(id)).purge_attempt, 2, "attempts never repeat");
});

test("sync export is text-only, bounded and drops attachment storage details", () => {
  const long = "é".repeat(syncLimits.textBytes); // two bytes each
  const message = syncMessage({
    id: crypto.randomUUID(),
    subject: "Plan",
    from: "a@example.test",
    to: "b@example.test",
    text: long,
    received_at: 5,
    attachments: [{ index: 0, filename: "x.pdf", size: 3, key: "mail/secret/attachments/1", sha256: "f".repeat(64) }],
  });
  assert.equal(message.text_truncated, true);
  assert.ok(new TextEncoder().encode(message.text).length <= syncLimits.textBytes);
  assert.ok(!message.text.includes("�"));
  assert.deepEqual(message.attachments, [{ index: 0, filename: "x.pdf", size: 3 }]);
  assert.equal(message.thread_id, message.id);
});

function env(overrides = {}) {
  const mailbox = { id: crypto.randomUUID(), address: "me@example.test" };
  const calls = [];
  return {
    mailbox,
    calls,
    AUTH_ORIGIN: "https://mail.example.test",
    AUTH_SERVICE: { session: async () => ({ session: { user: { id: "alice" }, mailbox }, cookies: [] }) },
    MAILBOXES: {
      getByName: (id) => ({
        sync: async (box, cursor) => {
          calls.push({ id, box, cursor });
          return { messages: [], cursor: "0", more: false };
        },
      }),
    },
    LOCAL_MODEL_GRANT_SECRET: "test-grant-secret",
    MAIL_MODELS: {
      async get(key) {
        calls.push({ model: key });
        const objects = {
          "local-models/catalog.json": JSON.stringify({
            id: "m",
            files: [{ path: "model.safetensors", url: "/local/models/model.safetensors", sha256: "a".repeat(64), size_bytes: 4 }],
          }),
          "local-models/model.safetensors": "wgts",
        };
        return key in objects ? { body: objects[key], size: objects[key].length } : null;
      },
    },
    MAIL_ASSETS: {
      fetch: async (request) =>
        new Response("asset", { headers: { "Content-Type": "text/plain", "X-Path": new URL(request.url).pathname } }),
    },
    ...overrides,
  };
}
const get = (path, e, headers = {}) => worker.fetch(new Request("https://mail.example.test" + path, { headers }), e);

test("sync and model routes are scoped to the signed-in mailbox", async () => {
  const e = env();
  const sync = await get("/mail/sync?cursor=12", e);
  assert.equal(sync.status, 200);
  assert.equal(sync.headers.get("Cache-Control"), "no-store");
  assert.deepEqual(e.calls[0], { id: e.mailbox.id, box: e.mailbox.id, cursor: "12" });
  // A browser cannot pick another mailbox; the session alone selects it.
  await get("/mail/sync?mailbox=" + crypto.randomUUID(), e, { "X-Antfly-Tenant": "bob" });
  assert.equal(e.calls[1].id, e.mailbox.id);
  const catalog = await get("/local/models/catalog.json", e);
  assert.equal(catalog.status, 200);
  assert.equal(catalog.headers.get("Cache-Control"), "private, no-store");
  assert.ok(e.calls.some((c) => c.model === "local-models/catalog.json"));
  // The browser model cache downloads without cookies: the signed-in catalog
  // hands out short-lived grant URLs, valid only for their own file.
  const [pin] = (await catalog.json()).files;
  assert.match(pin.url, /^\/local\/models\/model\.safetensors\?grant=\d+\.[a-f0-9]{64}$/);
  const anonymous = env({ AUTH_SERVICE: { session: async () => ({ session: null, cookies: [] }) } });
  const granted = await get(pin.url, anonymous);
  assert.equal(granted.status, 200);
  assert.equal(await granted.text(), "wgts");
  const grant = new URL(pin.url, "https://x").searchParams.get("grant");
  assert.equal((await get("/local/models/model.safetensors", anonymous)).status, 401, "no grant, no cookie");
  assert.equal((await get("/local/models/other.bin?grant=" + grant, anonymous)).status, 401, "grant is per file");
  const tampered = grant.slice(0, -1) + (grant.endsWith("0") ? "1" : "0");
  assert.equal((await get("/local/models/model.safetensors?grant=" + tampered, anonymous)).status, 401);
  const expired = "1." + grant.split(".")[1];
  assert.equal((await get("/local/models/model.safetensors?grant=" + expired, anonymous)).status, 401);
  assert.equal(
    (await get(pin.url, env({ ...anonymous, LOCAL_MODEL_GRANT_SECRET: "rotated" }))).status,
    401,
    "rotating the secret revokes outstanding grants"
  );
  assert.equal((await get("/local/models/catalog.json?grant=" + grant, anonymous)).status, 401, "the catalog needs a session");
  const noSecret = env({ LOCAL_MODEL_GRANT_SECRET: undefined });
  assert.equal((await get("/local/models/catalog.json", noSecret)).status, 503);
  for (const path of ["/local/models/a//b", "/local/models/", "/local/models/a/./b", "/local/models/missing.bin"])
    assert.equal((await get(path, e)).status, 404, path);
  // URL parsing resolves dot segments before routing, so they never reach R2.
  await get("/local/models/../secrets", e);
  await get("/local/models/%2e%2e/x", e);
  assert.ok(e.calls.every((c) => !c.model || !c.model.includes("..")));
  const signedOut = env({ AUTH_SERVICE: { session: async () => ({ session: null, cookies: [] }) } });
  assert.equal((await get("/mail/sync", signedOut)).status, 401);
  assert.equal((await get("/local/models/catalog.json", signedOut)).status, 401);
  const noModels = env({ MAIL_MODELS: undefined });
  assert.equal((await get("/local/models/catalog.json", noModels)).status, 404);
});

test("DELETE removes a message through the session's mailbox and queues its purge", async () => {
  const mailbox = { id: crypto.randomUUID(), address: "me@example.test" },
    calls = [],
    hints = [];
  const e = {
    AUTH_ORIGIN: "https://mail.example.test",
    AUTH_SERVICE: { session: async () => ({ session: { user: { id: "alice" }, mailbox }, cookies: [] }) },
    MAIL_QUEUE: { send: async (body) => hints.push(body) },
    MAILBOXES: {
      getByName: (actor) => ({
        delete: async (box, id) => {
          calls.push({ actor, box, id });
          if (id === "00000000-0000-4000-8000-000000000000") throw Error("UnknownMessage");
          return { id, deleted: true };
        },
        purge: async (box, id) => {
          calls.push({ purge: id });
          return { done: true };
        },
        pending: async () => ({ inbound: [], outbound: [], purge: ["11111111-1111-4111-8111-111111111111"], publish: false }),
      }),
    },
  };
  const id = crypto.randomUUID();
  const del = (path, origin = e.AUTH_ORIGIN) =>
    worker.fetch(new Request(e.AUTH_ORIGIN + path, { method: "DELETE", headers: { Origin: origin } }), e);
  const response = await del("/mail/messages/" + id);
  assert.equal(response.status, 200);
  assert.deepEqual(await response.json(), { id, deleted: true });
  assert.deepEqual(calls[0], { actor: mailbox.id, box: mailbox.id, id });
  assert.deepEqual(hints[0], { mailbox: mailbox.id, kind: "purge", id });
  assert.equal((await del("/mail/messages/" + id, "https://attacker.test")).status, 403, "cross-site deletes are refused");
  assert.equal((await del("/mail/messages/00000000-0000-4000-8000-000000000000")).status, 404);
  assert.equal((await del("/mail/messages/" + id + "/attachments/0")).status, 404, "attachments are not deleted alone");
  // Queue and the scheduled sweep drive outstanding purges.
  const acked = [];
  await worker.queue({ messages: [{ body: { mailbox: mailbox.id, kind: "purge", id }, ack: () => acked.push(1), retry() {} }] }, e);
  assert.deepEqual(calls.at(-1), { purge: id });
  assert.equal(acked.length, 1);
  hints.length = 0;
  await worker.scheduled({}, { ...e, AUTH_SERVICE: { ...e.AUTH_SERVICE, mailboxes: async () => [mailbox.id] } });
  assert.deepEqual(hints, [{ mailbox: mailbox.id, kind: "purge", id: "11111111-1111-4111-8111-111111111111" }]);
});

test("static assets allow WASM and workers but stay same-origin; runtimes are immutable", async () => {
  const e = env();
  const page = await get("/", e);
  assert.equal(page.headers.get("Content-Security-Policy"), contentSecurityPolicy);
  assert.match(contentSecurityPolicy, /script-src 'self' 'wasm-unsafe-eval'/);
  assert.match(contentSecurityPolicy, /worker-src 'self'/);
  assert.match(contentSecurityPolicy, /connect-src 'self';/);
  assert.doesNotMatch(contentSecurityPolicy, /unsafe-inline|\*/);
  assert.equal((await get("/sw.js", e)).headers.get("Cache-Control"), "no-cache");
  assert.equal((await get("/local/config.json", e)).headers.get("Cache-Control"), "no-cache");
  assert.match(
    (await get("/local/0123456789abcdef/antfly/antfly.wasm", e)).headers.get("Cache-Control"),
    /immutable/
  );
});

test("runtime files over the asset limit are served from R2 at their versioned path", async () => {
  const keys = [];
  const e = env({
    MAIL_ASSETS: { fetch: async () => new Response("missing", { status: 404 }) },
    MAIL_MODELS: {
      async get(key) {
        keys.push(key);
        return key === "local-runtime/0123456789abcdef/antfly/antfly.wasm" ? { body: "wasm", size: 4 } : null;
      },
    },
  });
  const wasm = await get("/local/0123456789abcdef/antfly/antfly.wasm", e);
  assert.equal(wasm.status, 200);
  assert.equal(wasm.headers.get("Content-Type"), "application/wasm");
  assert.match(wasm.headers.get("Cache-Control"), /immutable/);
  assert.equal(wasm.headers.get("Content-Security-Policy"), contentSecurityPolicy);
  // Unversioned paths and other prefixes never reach the bucket.
  assert.equal((await get("/local/config.json", e)).status, 404);
  assert.equal((await get("/local/0123456789abcdef/missing.js", e)).status, 404);
  assert.equal((await get("/antfly.wasm", e)).status, 404);
  assert.deepEqual(keys, [
    "local-runtime/0123456789abcdef/antfly/antfly.wasm",
    "local-runtime/0123456789abcdef/missing.js",
  ]);
});

test("local documents carry display fields, one lexical field and the vector", () => {
  const message = {
    id: crypto.randomUUID(),
    thread_id: "t",
    subject: "Invoice",
    from: "plumber@example.test",
    to: "me@example.test",
    received_at: 9,
    text: "Pipes fixed.",
    attachments: [],
  };
  assert.equal(embeddingText(message), "Invoice\n\nPipes fixed.");
  const doc = documentFor(message, [0.1, 0.2]);
  assert.deepEqual(doc._embeddings, { mail_semantic: [0.1, 0.2] });
  assert.match(doc.content, /plumber@example.test/);
  assert.throws(() => documentFor(message, [NaN]), /InvalidLocalVector/);
  assert.throws(() => documentFor(message, []), /InvalidLocalVector/);
  assert.deepEqual(JSON.parse(localSchema.vectorIndex(256).config_json), { field: "embedding", dims: 256, metric: "cosine", external: true });

  const hybrid = searchRequest("  invoice ", [1, 0], 5);
  assert.deepEqual(hybrid.full_text_search, { match: { field: "content", text: "invoice" } });
  assert.deepEqual(hybrid.indexes, ["mail_semantic"]);
  assert.deepEqual(hybrid.merge_config, { strategy: "rrf" });
  assert.equal(searchRequest("x", null).merge_config, undefined);
  assert.equal(searchRequest("", [1]).full_text_search, undefined);
  assert.throws(() => searchRequest(" ", null), /InvalidLocalQuery/);

  const hits = hitsFrom({
    responses: [
      { hits: { hits: [{ _id: "message/a", _score: 2, _source: { id: "a", subject: "A" } }, { _id: "message/a" }] } },
      { hits: { hits: [{ _id: "message/b", _source: {} }] } },
    ],
  });
  assert.deepEqual(hits.map((h) => [h.id, h.local]), [["a", true], ["b", true]]);
  assert.deepEqual(hitsFrom(null), []);
});

function safetensors(tensors) {
  let offset = 0;
  const header = { __metadata__: { format: "pt" } };
  const data = [];
  for (const [name, bytes] of tensors) {
    header[name] = { dtype: "BF16", shape: [bytes.length / 2], data_offsets: [offset, offset + bytes.length] };
    offset += bytes.length;
    data.push(bytes);
  }
  const json = Buffer.from(JSON.stringify(header));
  const prefix = Buffer.alloc(8);
  prefix.writeBigUInt64LE(BigInt(json.length));
  return Buffer.concat([prefix, json, ...data]);
}
function parseSafetensors(bytes) {
  const length = Number(bytes.readBigUInt64LE(0));
  const header = JSON.parse(bytes.subarray(8, 8 + length).toString());
  const start = 8 + length;
  const tensors = {};
  for (const [name, t] of Object.entries(header))
    if (name !== "__metadata__") tensors[name] = bytes.subarray(start + t.data_offsets[0], start + t.data_offsets[1]);
  return { header, tensors, length };
}

test("model publishing strips exactly the browser-skipped media towers and pins every file", async (t) => {
  const dir = await mkdtemp(join(tmpdir(), "antfly-local-model-"));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const model = join(dir, "bundle"),
    out = join(dir, "out");
  await mkdir(join(model, "1_Pooling"), { recursive: true });
  const text = Buffer.from([1, 2, 3, 4]),
    norm = Buffer.from([5, 6]);
  await writeFile(
    join(model, "model.safetensors"),
    safetensors([
      ["vision_tower.encoder.weight", Buffer.alloc(64, 9)],
      ["model.embed_tokens.weight", text],
      ["audio_tower.layers.0.weight", Buffer.alloc(32, 7)],
      ["embed_vision.proj", Buffer.alloc(8, 1)],
      ["model.norm.weight", norm],
    ])
  );
  await writeFile(join(model, "config.json"), JSON.stringify({ model_type: "embedding_gemma2" }));
  for (const name of ["tokenizer.json", "tokenizer_config.json", "processor_config.json", "config_sentence_transformers.json", "1_Pooling/config.json"])
    await writeFile(join(model, name), "{}");
  await writeFile(join(model, "antfly_inference_bundle.json"), JSON.stringify({ files: [] }));
  await writeFile(join(model, "notes.bin"), Buffer.alloc(4));

  const catalog = await prepare({ modelDir: model, out });
  const stripped = parseSafetensors(await readFile(join(out, "model.safetensors")));
  assert.deepEqual(Object.keys(stripped.tensors).sort(), ["model.embed_tokens.weight", "model.norm.weight"]);
  assert.deepEqual(stripped.tensors["model.embed_tokens.weight"], text);
  assert.deepEqual(stripped.tensors["model.norm.weight"], norm);
  assert.equal(stripped.length % 8, 0, "tensor data stays 8-byte aligned");
  assert.deepEqual(stripped.header.__metadata__, { format: "pt" });
  assert.ok(Object.keys(stripped.header).every((name) => !MEDIA_TENSOR.test(name)));

  const paths = catalog.files.map((f) => f.path);
  assert.ok(paths.includes("1_Pooling/config.json") && paths.includes("model.safetensors"));
  assert.ok(!paths.includes("antfly_inference_bundle.json"), "the original receipt pins unstripped weights");
  assert.ok(!paths.includes("notes.bin"));
  for (const file of catalog.files) {
    assert.match(file.sha256, /^[a-f0-9]{64}$/);
    assert.equal(file.url, "/local/models/" + file.path);
  }
  assert.deepEqual(catalog.tasks, ["embed", "decide"]);
  assert.equal(catalog.precision, "bf16");
  assert.deepEqual(JSON.parse(await readFile(join(out, "catalog.json"), "utf8")), catalog);

  await writeFile(join(model, "config.json"), JSON.stringify({ model_type: "gemma3_text" }));
  await assert.rejects(prepare({ modelDir: model, out }), /EmbeddingGemma 2/);
  const small = await stripSafetensors(join(model, "model.safetensors"), join(dir, "copy.safetensors"), /^$/);
  assert.equal(small.removed, 0);
});

async function settled($, pattern) {
  for (let i = 0; i < 200 && !pattern.test($("local-status").textContent); i++)
    await new Promise((resolve) => setTimeout(resolve, 5));
  assert.match($("local-status").textContent, pattern);
}
const html = await readFile(new URL("../mail-public/index.html", import.meta.url), "utf8"),
  script = await readFile(new URL("../mail-public/mail.js", import.meta.url), "utf8");
async function ui({ supported = true, flags = null, online = true, failRemoves = 0, storage = null } = {}) {
  const dom = new JSDOM(html, { url: "https://mail.example.test/", runScripts: "outside-only" }),
    { window } = dom,
    removed = [];
  const me = {
    user: { id: "alice", email: "alice@example.test" },
    mailbox: { id: crypto.randomUUID(), address: "me@example.test" },
  };
  for (const [key, value] of Object.entries(storage ?? {})) window.localStorage.setItem(key, value);
  if (flags) window.localStorage.setItem("antfly-mail-local-v1", JSON.stringify(flags(me)));
  if (supported) {
    window.Worker = class {};
    window.WebAssembly = WebAssembly;
    Object.defineProperty(window.navigator, "storage", {
      value: {
        getDirectory: async () => ({
          removeEntry: async (name, options) => {
            if (failRemoves-- > 0)
              throw Object.assign(new Error("locked by another tab"), { name: "NoModificationAllowedError" });
            removed.push([name, options]);
          },
        }),
      },
    });
  }
  let session = true;
  window.fetch = async (url) => {
    if (!online) throw new window.TypeError("Failed to fetch");
    if (url === "/api/me")
      return Response.json(session ? me : { error: "SignInRequired" }, { status: session ? 200 : 401 });
    if (url === "/api/auth/sign-out") {
      session = false;
      return Response.json({ success: true });
    }
    if (url === "/mail/messages") return Response.json({ messages: [], cursor: null });
    throw Error("UnexpectedUiRequest " + url);
  };
  await window.eval("(async()=>{" + script + "\n})()");
  return { dom, window, me, removed, $: (id) => window.document.getElementById(id) };
}

test("on-device search is opt-in, hidden where unsupported, and wiped on sign-out", async () => {
  const unsupported = await ui({ supported: false });
  try {
    assert.equal(unsupported.$("mail").hidden, false);
    assert.equal(unsupported.$("local").hidden, true);
    assert.equal(unsupported.$("search-where").hidden, true);
  } finally {
    unsupported.dom.window.close();
  }
  const f = await ui();
  try {
    assert.equal(f.$("local").hidden, false);
    assert.equal(f.$("local-toggle").checked, false, "off until the user opts in");
    assert.equal(f.$("search-where").hidden, true, "no device search before a copy exists");
    await f.$("logout").onclick();
    assert.deepEqual(f.removed, [], "nothing to wipe when never enabled");
    assert.equal(f.$("account").hidden, false);
  } finally {
    f.dom.window.close();
  }
  // A previously enabled copy (here: for another mailbox too) is wiped on sign-out.
  const enabled = await ui({ flags: (me) => ({ [me.mailbox.id]: true, [crypto.randomUUID()]: true }) });
  try {
    assert.equal(enabled.$("local-toggle").checked, true);
    // The runtime is not installed in this fixture; reopening reports that.
    await settled(enabled.$, /unavailable/);
    await enabled.$("logout").onclick();
    assert.equal(JSON.stringify(enabled.removed), JSON.stringify([["antfly-mail-local", { recursive: true }]]));
    assert.equal(enabled.window.localStorage.getItem("antfly-mail-local-v1"), null);
    assert.equal(enabled.window.localStorage.getItem("antfly-mail-offline-identity-v1"), null);
  } finally {
    enabled.dom.window.close();
  }
});

test("sign-out always completes; a failed on-device wipe is retried before any reopen", async () => {
  // A transient failure is retried by the post-sign-out refresh.
  const transient = await ui({ flags: (me) => ({ [me.mailbox.id]: true }), failRemoves: 1 });
  try {
    await settled(transient.$, /unavailable/);
    await transient.$("logout").onclick();
    assert.equal(transient.$("status").textContent, "Signed out.");
    assert.equal(transient.removed.length, 1);
    assert.equal(transient.window.localStorage.getItem("antfly-mail-local-wipe-pending-v1"), null);
  } finally {
    transient.dom.window.close();
  }
  // A persistent failure (another tab holds the copy) is reported and kept pending.
  const f = await ui({ flags: (me) => ({ [me.mailbox.id]: true }), failRemoves: 2 });
  let carried;
  try {
    f.window.localStorage.setItem(
      "antfly-mail-offline-identity-v1",
      JSON.stringify({ user: { email: "alice@example.test" }, mailbox: { id: f.me.mailbox.id, address: "me@example.test" } })
    );
    await settled(f.$, /unavailable/);
    await f.$("logout").onclick();
    assert.equal(f.$("account").hidden, false, "the session ended despite the failed wipe");
    assert.match(f.$("status").textContent, /could not be removed/);
    assert.equal(f.removed.length, 0);
    // Offline reopening is impossible immediately; the opt-in stays for the retry.
    assert.equal(f.window.localStorage.getItem("antfly-mail-offline-identity-v1"), null);
    assert.equal(f.window.localStorage.getItem("antfly-mail-local-wipe-pending-v1"), "1");
    assert.notEqual(f.window.localStorage.getItem("antfly-mail-local-v1"), null);
    carried = Object.fromEntries(
      ["antfly-mail-local-v1", "antfly-mail-local-wipe-pending-v1"].map((k) => [k, f.window.localStorage.getItem(k)])
    );
  } finally {
    f.dom.window.close();
  }
  // The next load (here signed in again) finishes the wipe before reopening.
  const next = await ui({ storage: carried });
  try {
    assert.equal(JSON.stringify(next.removed), JSON.stringify([["antfly-mail-local", { recursive: true }]]));
    assert.equal(next.window.localStorage.getItem("antfly-mail-local-wipe-pending-v1"), null);
    assert.equal(next.window.localStorage.getItem("antfly-mail-local-v1"), null);
    assert.equal(next.$("local-toggle").checked, false, "the wiped copy is not reopened");
  } finally {
    next.dom.window.close();
  }
});

test("offline start opens an opted-in copy instead of the sign-in screen", async () => {
  const mailbox = crypto.randomUUID();
  const dom = new JSDOM(html, { url: "https://mail.example.test/", runScripts: "outside-only" });
  const { window } = dom;
  try {
    window.localStorage.setItem("antfly-mail-local-v1", JSON.stringify({ [mailbox]: true }));
    window.localStorage.setItem(
      "antfly-mail-offline-identity-v1",
      JSON.stringify({ user: { email: "alice@example.test" }, mailbox: { id: mailbox, address: "me@example.test" } })
    );
    window.fetch = async () => {
      throw new window.TypeError("Failed to fetch");
    };
    await window.eval("(async()=>{" + script + "\n})()");
    const $ = (id) => window.document.getElementById(id);
    assert.equal($("mail").hidden, false);
    assert.equal($("identity").textContent, "alice@example.test");
    assert.match($("status").textContent, /offline/i);
    await settled($, /unavailable/);
  } finally {
    window.close();
  }
  const signedOut = new JSDOM(html, { url: "https://mail.example.test/", runScripts: "outside-only" });
  try {
    signedOut.window.localStorage.setItem(
      "antfly-mail-offline-identity-v1",
      JSON.stringify({ user: { email: "x@example.test" }, mailbox: { id: mailbox, address: "me@example.test" } })
    );
    signedOut.window.fetch = async () => {
      throw new signedOut.window.TypeError("Failed to fetch");
    };
    await signedOut.window.eval("(async()=>{" + script + "\n})()");
    assert.equal(signedOut.window.document.getElementById("account").hidden, false);
  } finally {
    signedOut.window.close();
  }
});

test("service worker never caches mail, auth or model weights", async () => {
  const source = await readFile(new URL("../mail-public/sw.js", import.meta.url), "utf8");
  const listeners = {},
    named = new Map(),
    fetched = [];
  const keyOf = (key) => (typeof key === "string" ? key : new URL(key.url).pathname);
  const open = async (name) => {
    if (!named.has(name)) {
      const entries = new Map();
      named.set(name, {
        entries,
        match: async (key) => entries.get(keyOf(key)),
        put: async (key, response) => entries.set(keyOf(key), response),
        addAll: async () => {},
        keys: async () => [...entries.keys()].map((path) => ({ url: "https://mail.example.test" + path })),
        delete: async (key) => entries.delete(keyOf(key)),
      });
    }
    return named.get(name);
  };
  // Every path stored in any cache.
  const stored = {
    keys: () => [...named.values()].flatMap((cache) => [...cache.entries.keys()]),
    has: (path) => stored.keys().includes(path),
  };
  const self = {
    location: new URL("https://mail.example.test/sw.js"),
    addEventListener: (type, fn) => (listeners[type] = fn),
    skipWaiting: () => {},
    clients: { claim: () => {} },
  };
  let online = true;
  new Function("self", "caches", "fetch", source)(
    self,
    { open, keys: async () => [...named.keys()], delete: async (name) => named.delete(name) },
    async (request) => {
      const path = new URL(request.url).pathname;
      fetched.push(path);
      if (!online) throw new TypeError("offline");
      const response = new Response(path === "/local/config.json" ? JSON.stringify({ build: "0123456789abcdef" }) : "ok");
      Object.defineProperty(response, "type", { value: "basic" });
      return response;
    }
  );
  async function request(path, mode = "cors") {
    let responded = null;
    const background = [];
    listeners.fetch({
      request: { method: "GET", url: "https://mail.example.test" + path, mode },
      respondWith: (value) => (responded = value),
      waitUntil: (promise) => background.push(promise),
    });
    const result = responded ? await responded : null;
    await Promise.all(background);
    return result;
  }
  for (const path of ["/mail/messages", "/mail/sync?cursor=1", "/api/me", "/local/models/model.safetensors"])
    assert.equal(await request(path), null, path + " must bypass the service worker");
  await request("/local/ffffffffffffffff/antfly/antfly.wasm"); // a previous deploy
  await request("/local/0123456789abcdef/antfly/antfly.wasm");
  assert.ok(stored.has("/local/ffffffffffffffff/antfly/antfly.wasm"));
  await request("/local/config.json"); // names the current build: older ones are pruned
  await request("/local/models/catalog.json");
  await request("/mail.js");
  assert.deepEqual([...stored.keys()].sort(), [
    "/local/0123456789abcdef/antfly/antfly.wasm",
    "/local/config.json",
    "/local/models/catalog.json",
    "/mail.js",
  ]);
  // Offline: versioned runtime and descriptors come from cache.
  online = false;
  fetched.length = 0;
  assert.equal(await (await request("/local/0123456789abcdef/antfly/antfly.wasm")).text(), "ok");
  assert.deepEqual(fetched, [], "immutable runtime is cache-first");
  assert.ok(await request("/local/config.json"));
  assert.ok(await request("/local/models/catalog.json"));
});

test("a message can be deleted from the UI after confirmation", async () => {
  const dom = new JSDOM(html, { url: "https://mail.example.test/", runScripts: "outside-only" });
  const { window } = dom;
  const id = crypto.randomUUID(),
    requests = [];
  let present = true,
    confirmAnswer = false;
  window.confirm = () => confirmAnswer;
  const message = { id, subject: "Invoice", from: "a@example.net", to: "me@example.test", text: "x", received_at: 1, attachments: [] };
  window.fetch = async (url, options = {}) => {
    requests.push([options.method ?? "GET", url]);
    if (url === "/api/me")
      return Response.json({ user: { id: "u", email: "me@example.test" }, mailbox: { id: crypto.randomUUID(), address: "me@example.test" } });
    if (url === "/mail/messages") return Response.json({ messages: present ? [message] : [], cursor: null });
    if (url === "/mail/messages/" + id && options.method === "DELETE") {
      present = false;
      return Response.json({ id, deleted: true });
    }
    if (url === "/mail/messages/" + id) return Response.json(message);
    throw Error("UnexpectedUiRequest " + url);
  };
  try {
    await window.eval("(async()=>{" + script + "\n})()");
    const $ = (name) => window.document.getElementById(name);
    await $("messages").querySelector("button").onclick();
    const remove = [...$("message").querySelectorAll("button")].find((b) => b.textContent === "Delete");
    assert.ok(remove, "server-backed messages offer Delete");
    await remove.onclick();
    assert.ok(!requests.some(([method]) => method === "DELETE"), "nothing is deleted without confirmation");
    confirmAnswer = true;
    await remove.onclick();
    assert.deepEqual(requests.filter(([method]) => method === "DELETE"), [["DELETE", "/mail/messages/" + id]]);
    assert.equal($("message").textContent, "Message deleted.");
    assert.equal($("messages").textContent, "No messages.", "the list is refreshed");
  } finally {
    window.close();
  }
});
