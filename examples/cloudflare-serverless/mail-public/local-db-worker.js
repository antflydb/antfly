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
// embedded Antfly WASM database, whose files live directly in OPFS through a
// pool of synchronous access handles (antfly_embedded_opfs_fs.mjs). Requests
// arrive one at a time from local-index.js and are answered in order.
import { localSchema, documentFor, searchRequest, hitsFrom } from "/local-schema.js";

const ROOT = "antfly-mail-local";
const DATABASE = "/mail";
// Sync position and vector space, committed in the same batch as each page.
const STATE = "_local/state";
let api = null;
let pool = null;
let mailbox = null;
let runtime = null;

async function root() {
  return (await navigator.storage.getDirectory()).getDirectoryHandle(ROOT, { create: true });
}

// Runtime URLs come from /local/config.json and are versioned by content.
async function loadRuntime(urls) {
  if (runtime?.key !== urls.wasm) {
    const [client, fs, response] = await Promise.all([import(urls.client), import(urls.fs), fetch(urls.wasm)]);
    if (!response.ok) throw Error(`Local database runtime unavailable (${response.status})`);
    runtime = { key: urls.wasm, client, fs, bytes: new Uint8Array(await response.arrayBuffer()) };
  }
  return runtime;
}

async function removeEntry(parent, name) {
  try {
    await parent.removeEntry(name, { recursive: true });
  } catch (error) {
    if (error?.name !== "NotFoundError") throw error;
  }
}

function close() {
  try {
    api?.close();
  } catch {
    /* The instance is discarded either way. */
  }
  // Releasing the handles lets another tab (or a later open) take the pool.
  pool?.close();
  api = null;
  pool = null;
  mailbox = null;
}

// Database calls are synchronous and can create many files; keep the OPFS
// pool topped up before each. If one still runs out, an index may have been
// interrupted mid-build, which the hosted profile cannot repair: rebuild this
// cache from scratch and tell the page to resync from the start.
async function work(fn) {
  try {
    return await runtime.fs.withCapacity(pool, fn);
  } catch (error) {
    if (error?.name !== "OpfsPoolExhausted" || !lastOpen) throw error;
    await handlers.reset();
    throw Error("LocalIndexRebuilt");
  }
}

function state() {
  try {
    return api.lookup(STATE)._source ?? null;
  } catch {
    return null;
  }
}
function status(current = state()) {
  return { ...(current ?? {}), mailbox, ...pool.stats() };
}

let lastOpen = null;
const handlers = {
  // Open (or create) this mailbox's database. A copy built for another model
  // or vector size is discarded: vectors from different spaces never mix.
  async open({ mailboxId, runtime: urls, space }) {
    if (!/^[a-f0-9-]{36}$/.test(mailboxId ?? "")) throw Error("InvalidMailbox");
    close();
    lastOpen = { mailboxId, runtime: urls, space };
    const { client, fs } = await loadRuntime(urls);
    const directory = await (await root()).getDirectoryHandle(mailboxId, { create: true });
    try {
      pool = await fs.openOpfsFileSystem(directory);
    } catch (error) {
      if (error?.message === "OpfsBusy") throw Error("On-device search is open in another tab");
      throw error;
    }
    api = await client.instantiateAntflyEmbeddedApiFromBytes(runtime.bytes, { fileSystem: pool });
    await work(() => api.openFileSystem(DATABASE));
    mailbox = mailboxId;
    const current = state();
    if (current && JSON.stringify(current.space) !== JSON.stringify(space)) {
      close();
      await removeEntry(await root(), mailboxId);
      return handlers.open({ mailboxId, runtime: urls, space });
    }
    if (!current) {
      // Setup is two commits; an interrupted first open may have added the
      // index already, so only add what is missing.
      const index = localSchema.vectorIndex(space.dimensions);
      if (!api.listIndexes().some((existing) => existing.name === index.name)) await work(() => api.addIndex(index));
      await work(() => api.put(STATE, { space, cursor: "", messages: 0 }));
    }
    return status();
  },

  // Apply one synced page. Messages and the next cursor are one batch, so a
  // reload resumes exactly after the last page the database committed.
  async apply({ messages, vectors, cursor }) {
    if (!api) throw Error("LocalIndexClosed");
    if (messages.length !== vectors.length) throw Error("LocalVectorMismatch");
    const previous = state() ?? {};
    const next = { ...previous, cursor, messages: (previous.messages ?? 0) + messages.length, synced_at: Date.now() };
    const inserts = { [STATE]: next };
    messages.forEach((message, i) => {
      inserts[localSchema.key(message.id)] = documentFor(message, vectors[i]);
    });
    await work(() => api.batch({ inserts }));
    await work(() => api.runUntilIdle());
    return status(next);
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

  // The server's log no longer matches this copy: rebuild it from scratch.
  async reset() {
    if (!lastOpen) throw Error("LocalIndexClosed");
    const args = lastOpen;
    close();
    await removeEntry(await root(), args.mailboxId);
    return handlers.open(args);
  },

  // Release the database and the OPFS handles before the worker is stopped.
  async close() {
    close();
    return true;
  },

  async status() {
    return api ? status() : null;
  },

  // Remove one mailbox's copy, or every copy on this device.
  async destroy({ mailboxId } = {}) {
    if (!mailboxId || mailboxId === mailbox) close();
    if (mailboxId) await removeEntry(await root(), mailboxId);
    else await removeEntry(await navigator.storage.getDirectory(), ROOT);
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
