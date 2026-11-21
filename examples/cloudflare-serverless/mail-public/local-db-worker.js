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

// Dedicated module worker that owns this device's copy of one mailbox: the
// embedded Antfly WASM database plus its OPFS checkpoints. Requests arrive one
// at a time from local-index.js and are answered in order.
import { localSchema, documentFor, searchRequest, hitsFrom } from "/local-schema.js";

const ROOT = "antfly-mail-local";
let api = null;
let store = null;
let mailbox = null;
let runtime = null;

async function root() {
  return (await navigator.storage.getDirectory()).getDirectoryHandle(ROOT, { create: true });
}

// Runtime URLs come from /local/config.json and are versioned by content.
async function module(urls) {
  if (runtime?.key !== urls.wasm) {
    const [client, opfs, response] = await Promise.all([
      import(urls.client),
      import(urls.store),
      fetch(urls.wasm),
    ]);
    if (!response.ok) throw Error(`Local database runtime unavailable (${response.status})`);
    runtime = { key: urls.wasm, client, opfs, bytes: new Uint8Array(await response.arrayBuffer()) };
  }
  return runtime.client.instantiateAntflyEmbeddedApiFromBytes(runtime.bytes, {});
}
const openOpfsStore = (directory) => runtime.opfs.openOpfsStore(directory);
const destroyOpfsStore = async (parent, name) => {
  try {
    await parent.removeEntry(name, { recursive: true });
  } catch (error) {
    if (error?.name !== "NotFoundError") throw error;
  }
};

function close() {
  try {
    api?.close();
  } catch {
    /* The instance is discarded either way. */
  }
  api = null;
  store = null;
  mailbox = null;
}

const handlers = {
  // Open (or create) this mailbox's database. A copy built for another model
  // or vector size is discarded: vectors from different spaces never mix.
  async open({ mailboxId, runtime: urls, space }) {
    if (!/^[a-f0-9-]{36}$/.test(mailboxId ?? "")) throw Error("InvalidMailbox");
    close();
    const directory = await (await root()).getDirectoryHandle(mailboxId, { create: true });
    api = await module(urls);
    store = await openOpfsStore(directory);
    const loaded = await store.load(api);
    const current = loaded.metadata?.space;
    if (loaded.restored && current && JSON.stringify(current) !== JSON.stringify(space)) {
      close();
      await destroyOpfsStore(await root(), mailboxId);
      return handlers.open({ mailboxId, runtime: urls, space });
    }
    if (!loaded.restored) {
      api.addIndex(localSchema.vectorIndex(space.dimensions));
      await store.checkpoint(api, { space, cursor: "", messages: 0 });
    }
    mailbox = mailboxId;
    return { ...store.metadata, bytes: store.size() };
  },

  // Apply one synced page and persist it together with the next cursor, so a
  // reload resumes exactly after the last page that reached OPFS.
  async apply({ messages, vectors, cursor }) {
    if (!api) throw Error("LocalIndexClosed");
    if (messages.length !== vectors.length) throw Error("LocalVectorMismatch");
    const inserts = {};
    messages.forEach((message, i) => {
      inserts[localSchema.key(message.id)] = documentFor(message, vectors[i]);
    });
    if (messages.length) {
      api.batch({ inserts });
      api.runUntilIdle();
    }
    const previous = store.metadata ?? {};
    const result = await store.checkpoint(api, {
      ...previous,
      cursor,
      messages: (previous.messages ?? 0) + messages.length,
      synced_at: Date.now(),
    });
    return { ...store.metadata, bytes: store.size(), written: result.bytesWritten };
  },

  async search({ text, vector, limit = 20 }) {
    if (!api) throw Error("LocalIndexClosed");
    return hitsFrom(api.search(searchRequest(text, vector, limit)));
  },

  async message({ id }) {
    if (!api) throw Error("LocalIndexClosed");
    try {
      return api.lookup(localSchema.key(id))._source ?? null;
    } catch {
      return null;
    }
  },

  async status() {
    return store ? { mailbox, ...store.metadata, bytes: store.size() } : null;
  },

  // Remove one mailbox's copy, or every copy on this device.
  async destroy({ mailboxId } = {}) {
    if (!mailboxId || mailboxId === mailbox) close();
    const parent = await navigator.storage.getDirectory();
    if (mailboxId) await destroyOpfsStore(await root(), mailboxId);
    else await destroyOpfsStore(parent, ROOT);
    return true;
  },
};

let queue = Promise.resolve();
self.onmessage = (event) => {
  const { id, op, args } = event.data ?? {};
  queue = queue.then(async () => {
    try {
      const handler = Object.hasOwn(handlers, op) ? handlers[op] : null;
      if (!handler) throw Error("UnknownLocalOperation");
      self.postMessage({ id, result: await handler(args ?? {}) });
    } catch (error) {
      self.postMessage({ id, error: String(error?.message ?? error) });
    }
  });
};
