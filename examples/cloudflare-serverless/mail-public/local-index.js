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

// Optional on-device mailbox index. Loaded only after the user opts in.
//
//   server  --/mail/sync (text only)-->  page  --EmbeddingGemma2 (WASM CPU)-->
//   database worker (Antfly WASM)  --files (sync access handle pool)-->  OPFS
//
// Message text and vectors never leave the device. The server copy remains
// authoritative; the local copy is a rebuildable cache keyed by mailbox.
import { embeddingText } from "/local-schema.js";

const FLAGS = "antfly-mail-local-v1";
const ROOT = "antfly-mail-local";
const EMBED_BATCH = 4;

function readFlags() {
  try {
    return JSON.parse(localStorage.getItem(FLAGS) ?? "{}") ?? {};
  } catch {
    return {};
  }
}
function writeFlags(flags) {
  try {
    if (Object.keys(flags).length) localStorage.setItem(FLAGS, JSON.stringify(flags));
    else localStorage.removeItem(FLAGS);
  } catch {
    /* Private windows may refuse storage; the feature then stays off. */
  }
}
export function isEnabled(mailboxId) {
  return readFlags()[mailboxId] === true;
}
export function anyEnabled() {
  return Object.keys(readFlags()).length > 0;
}

/** Remove every mailbox copy on this device (sign-out on a shared machine). */
export async function wipeAll() {
  writeFlags({});
  try {
    await (await navigator.storage.getDirectory()).removeEntry(ROOT, { recursive: true });
  } catch (error) {
    if (error?.name !== "NotFoundError") throw error;
  }
}

/** Remove one mailbox copy without opening it (e.g. after a failed open). */
export async function removeMailbox(mailboxId) {
  const flags = readFlags();
  delete flags[mailboxId];
  writeFlags(flags);
  try {
    const root = await (await navigator.storage.getDirectory()).getDirectoryHandle(ROOT);
    await root.removeEntry(mailboxId, { recursive: true });
  } catch (error) {
    if (error?.name !== "NotFoundError") throw error;
  }
}

export async function localConfig() {
  const response = await fetch("/local/config.json", { cache: "no-cache" });
  if (!response.ok) throw Error("On-device search is not installed on this server");
  const config = await response.json();
  const catalog = await fetch(config.model.catalog, { cache: "no-cache" });
  if (!catalog.ok) throw Error("The on-device model has not been published on this server");
  config.catalog = await catalog.json();
  config.downloadBytes = config.catalog.files.reduce((sum, file) => sum + file.size_bytes, 0);
  return config;
}

export function supported() {
  return (
    typeof Worker !== "undefined" &&
    typeof WebAssembly !== "undefined" &&
    typeof navigator.storage?.getDirectory === "function"
  );
}

class DatabaseWorker {
  constructor() {
    this.worker = new Worker("/local-db-worker.js", { type: "module", name: "antfly-mail-db" });
    this.next = 0;
    this.pending = new Map();
    this.worker.onmessage = ({ data }) => {
      const call = this.pending.get(data.id);
      if (!call) return;
      this.pending.delete(data.id);
      data.error ? call.reject(Error(data.error)) : call.resolve(data.result);
    };
    this.worker.onerror = (event) => {
      for (const call of this.pending.values()) call.reject(Error(event.message || "Local database failed"));
      this.pending.clear();
    };
  }
  call(op, args) {
    const id = ++this.next;
    return new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
      this.worker.postMessage({ id, op, args });
    });
  }
  terminate() {
    this.worker.terminate();
  }
  /** Close the database (releasing its OPFS handles), then stop the worker. */
  async shutdown() {
    await this.call("close").catch(() => {});
    this.terminate();
  }
}

export class LocalMailIndex {
  /**
   * Download (once) and load the model, then open this mailbox's copy.
   * `onStatus` receives `{ stage, loaded?, total?, message? }`.
   */
  static async open(mailboxId, { onStatus = () => {}, config } = {}) {
    if (!supported()) throw Error("This browser cannot run on-device search");
    config ??= await localConfig();
    const space = {
      model: config.catalog.id,
      files: config.catalog.files.map((f) => f.sha256).join(":"),
      dimensions: config.embedding.dimensions,
      max_tokens: config.embedding.max_tokens,
    };
    // Open the database first: it is cheap and fails fast (for example when
    // another tab holds the copy) before a large model download or load.
    const database = new DatabaseWorker();
    let inference = null;
    try {
      const state = await database.call("open", { mailboxId, runtime: config.runtime.antfly, space });
      // Ask the browser not to evict the copy under storage pressure.
      await navigator.storage.persist?.().catch(() => false);
      const web = await import(config.runtime.web);
      inference = await web.Inference.create({ assets: config.runtime.inference });
      onStatus({ stage: "model-download", loaded: 0, total: config.downloadBytes });
      const files = await web.downloadCatalogModel(config.catalog, {
        assets: config.runtime.inference,
        onProgress: (p) => onStatus({ stage: "model-" + p.stage, loaded: p.loaded, total: p.total }),
      });
      onStatus({ stage: "model-load" });
      const model = await inference.loadModel(files, {
        backend: "wasm",
        precision: config.catalog.precision,
        onProgress: (p) => onStatus({ stage: "model-" + p.stage, loaded: p.loaded, total: p.total }),
      });
      if (!model.execution.tasks.includes("embed"))
        throw Error("The published model cannot produce embeddings in this browser");
      // Record the opt-in only once the copy is usable.
      const flags = readFlags();
      flags[mailboxId] = true;
      writeFlags(flags);
      return new LocalMailIndex(mailboxId, inference, database, config, state, space);
    } catch (error) {
      // Release the model memory and the worker's OPFS handles.
      inference?.dispose();
      await database.shutdown();
      throw error;
    }
  }

  constructor(mailboxId, inference, database, config, state, space) {
    this.mailboxId = mailboxId;
    this.space = space;
    this.inference = inference;
    this.database = database;
    this.config = config;
    this.state = state;
    // The inference runtime runs one operation at a time: sync and search share it.
    this.queue = Promise.resolve();
    this.syncing = null;
  }

  serial(fn) {
    const run = this.queue.then(fn, fn);
    this.queue = run.catch(() => {});
    return run;
  }

  async embed(inputs, taskType) {
    const result = await this.inference.embed({
      inputs,
      task_type: taskType,
      dimensions: this.config.embedding.dimensions,
      max_tokens: this.config.embedding.max_tokens,
    });
    return result.value.embeddings;
  }

  /** Pull and embed everything new since the stored cursor; resumable. */
  sync({ signal, onStatus = () => {} } = {}) {
    this.syncing ??= (async () => {
      try {
        let more = true;
        while (more) {
          signal?.throwIfAborted();
          const response = await fetch("/mail/sync?cursor=" + encodeURIComponent(this.state.cursor ?? ""), {
            signal,
          });
          if (response.status === 401) throw Error("Sign in again to continue syncing");
          if (!response.ok) throw Error("Sync is temporarily unavailable");
          const page = await response.json();
          if (page.reset) {
            onStatus({ stage: "reset" });
            this.state = await this.database.call("reset");
            continue;
          }
          const vectors = [];
          for (let at = 0; at < page.messages.length; at += EMBED_BATCH) {
            signal?.throwIfAborted();
            const batch = page.messages.slice(at, at + EMBED_BATCH);
            vectors.push(
              ...(await this.serial(() =>
                // The encoder reads at most max_tokens; ~16 chars/token is a safe upper bound.
                this.embed(
                  batch.map((m) => embeddingText(m).slice(0, this.config.embedding.max_tokens * 16)),
                  "RETRIEVAL_DOCUMENT"
                )
              ))
            );
            onStatus({ stage: "embedding", loaded: (this.state.messages ?? 0) + vectors.length });
          }
          this.state = await this.database.call("apply", {
            messages: page.messages,
            vectors,
            cursor: page.cursor,
          });
          onStatus({ stage: "synced", loaded: this.state.messages, bytes: this.state.bytes, more: page.more });
          more = page.more;
        }
        return this.state;
      } finally {
        this.syncing = null;
      }
    })();
    return this.syncing;
  }

  /** Hybrid keyword + semantic search over this device's copy. */
  async search(text, limit = 20) {
    const [vector] = await this.serial(() => this.embed([text], "RETRIEVAL_QUERY"));
    return this.database.call("search", { text, vector, limit });
  }

  /** The locally stored message, for reading offline. */
  message(id) {
    return this.database.call("message", { id });
  }

  /** Delete every mailbox copy on this device (sign-out). Call close() after. */
  async wipeAll() {
    await this.database.call("destroy", {});
  }

  /** Stop, delete this mailbox's copy and opt out. Keeps the cached model. */
  async remove() {
    await this.database.call("destroy", { mailboxId: this.mailboxId });
    await this.close();
    const flags = readFlags();
    delete flags[this.mailboxId];
    writeFlags(flags);
  }

  close() {
    this.inference.dispose();
    return this.database.shutdown();
  }
}
