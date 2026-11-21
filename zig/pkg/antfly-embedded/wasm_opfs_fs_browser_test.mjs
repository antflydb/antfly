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

// Runs the OPFS slot pool against the real Origin Private File System in
// headless Chromium (Playwright). With ANTFLY_WASM pointing at an installed
// zig-out/antfly-wasm directory, it also runs a full database through the
// pool, reloads the page and searches again.
//
//   node pkg/antfly-embedded/wasm_opfs_fs_browser_test.mjs
//   ANTFLY_WASM=zig-out/antfly-wasm node pkg/antfly-embedded/wasm_opfs_fs_browser_test.mjs
import { createServer } from "node:http";
import { readFile } from "node:fs/promises";
import { extname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import assert from "node:assert/strict";
import { createRequire } from "node:module";
// Resolve Playwright the CommonJS way so a globally installed copy works too.
const { chromium } = createRequire(import.meta.url)("playwright");

const here = fileURLToPath(new URL(".", import.meta.url));
const wasmDir = process.env.ANTFLY_WASM ? resolve(process.env.ANTFLY_WASM) : null;
const types = { ".mjs": "text/javascript", ".js": "text/javascript", ".html": "text/html", ".wasm": "application/wasm" };

const worker = `
const results = [];
const check = (ok, name) => { if (!ok) throw new Error("failed: " + name); results.push(name); };
self.onmessage = async ({ data }) => {
  try {
    const { openOpfsFileSystem, status } = await import("/fs/wasm_opfs_fs.mjs");
    const root = await navigator.storage.getDirectory();
    const dir = await root.getDirectoryHandle("pool-test", { create: true });
    const enc = (t) => new TextEncoder().encode(t);
    const text = (fs, p) => { const n = fs.size(p); const out = new Uint8Array(n); fs.read(p, 0, out); return new TextDecoder().decode(out); };
    if (data.phase === "write") {
      const fs = await openOpfsFileSystem(dir, { initialSlots: 8, reserve: 4 });
      check(fs.write("/db/CURRENT", enc("v1"), false) === 0, "create");
      check(fs.write("/db/wal", enc("abc"), false) === 0 && fs.write("/db/wal", enc("def"), true) === 0, "append");
      check(fs.sync("/db/wal") === 0, "sync");
      check(fs.write("/db/CURRENT.tmp", enc("v2"), false) === 0 && fs.sync("/db/CURRENT.tmp") === 0, "temp");
      check(fs.rename("/db/CURRENT.tmp", "/db/CURRENT") === 0, "rename over existing");
      check(text(fs, "/db/CURRENT") === "v2", "renamed contents");
      const big = new Uint8Array(3 << 20).map((_, i) => i & 255);
      check(fs.write("/db/seg", big, false) === 0 && fs.sync("/db/seg") === 0, "large write");
      let busy = null;
      try { await openOpfsFileSystem(dir); } catch (e) { busy = e.message; }
      check(busy === "OpfsBusy", "exclusive while open");
      await fs.ensureCapacity(6);
      fs.close();
      check(fs.size("/db/wal") === status.io, "closed pool refuses work");
    } else {
      const fs = await openOpfsFileSystem(dir);
      check(JSON.stringify(fs.paths()) === JSON.stringify(["/db/CURRENT", "/db/seg", "/db/wal"]), "namespace survives reload");
      check(text(fs, "/db/CURRENT") === "v2" && text(fs, "/db/wal") === "abcdef", "data survives reload");
      const out = new Uint8Array(16); fs.read("/db/seg", (2 << 20) + 5, out);
      check(out[0] === (((2 << 20) + 5) & 255), "large read at offset");
      check(fs.remove("/db", true) === 0 && fs.paths().length === 0, "tree delete");
      fs.close();
      await root.removeEntry("pool-test", { recursive: true });
      check(true, "directory removable after close");
    }
    self.postMessage({ ok: true, results });
  } catch (error) {
    self.postMessage({ ok: false, error: String(error && error.stack || error), results });
  }
};`;

const dbWorker = `
self.onmessage = async ({ data }) => {
  try {
    const { openOpfsFileSystem } = await import("/fs/wasm_opfs_fs.mjs");
    const { instantiateAntflyEmbeddedApiFromBytes } = await import("/wasm/antfly_embedded_wasm_client.mjs");
    const bytes = new Uint8Array(await (await fetch("/wasm/antfly.wasm")).arrayBuffer());
    const root = await navigator.storage.getDirectory();
    const dir = await root.getDirectoryHandle("db-test", { create: true });
    const fs = await openOpfsFileSystem(dir);
    const api = await instantiateAntflyEmbeddedApiFromBytes(bytes, { fileSystem: fs });
    api.openFileSystem("/mail");
    if (data.phase === "write") {
      api.addIndex({ name: "v", kind: "dense_vector", config_json: JSON.stringify({ field: "embedding", dims: 3, metric: "cosine", external: true }) });
      for (let i = 0; i < 20; i++) {
        await fs.ensureCapacity();
        api.putMany({
          ["m" + i + "a"]: { content: "plumber invoice " + i, _embeddings: { v: [1, 0, 0] } },
          ["m" + i + "b"]: { content: "team offsite " + i, _embeddings: { v: [0, 1, 0] } },
        });
        api.runUntilIdle();
      }
    }
    const dense = JSON.stringify(api.search({ embeddings: { v: [1, 0, 0] }, indexes: ["v"], limit: 5 }));
    const text = JSON.stringify(api.search({ full_text_search: { match: { field: "content", text: "offsite" } }, limit: 50 }));
    const result = { dense: dense.includes("a\\"") && !/m\\d+b/.test(dense), text: (text.match(/m\\d+b/g) || []).length, stats: fs.stats() };
    api.close();
    fs.close();
    if (data.phase === "read") await root.removeEntry("db-test", { recursive: true });
    self.postMessage({ ok: true, result });
  } catch (error) {
    self.postMessage({ ok: false, error: String(error && error.stack || error) });
  }
};`;

const server = createServer(async (request, response) => {
  const url = new URL(request.url, "http://localhost");
  let body = null;
  if (url.pathname === "/") body = "<!doctype html><title>opfs</title>";
  else if (url.pathname === "/worker.js") body = worker;
  else if (url.pathname === "/db-worker.js") body = dbWorker;
  else if (url.pathname.startsWith("/fs/")) body = await readFile(join(here, url.pathname.slice(4))).catch(() => null);
  else if (wasmDir && url.pathname.startsWith("/wasm/")) body = await readFile(join(wasmDir, url.pathname.slice(6))).catch(() => null);
  if (body === null) {
    response.writeHead(404).end();
    return;
  }
  const type = url.pathname.endsWith("worker.js") ? "text/javascript" : (types[extname(url.pathname)] ?? "text/html");
  response.writeHead(200, { "Content-Type": type }).end(body);
});
await new Promise((ok) => server.listen(0, "127.0.0.1", ok));
const origin = `http://127.0.0.1:${server.address().port}`;
const browser = await chromium.launch();
try {
  const context = await browser.newContext();
  const run = async (script, phase) => {
    // Each phase gets a fresh page and worker, like a reload.
    const page = await context.newPage();
    await page.goto(origin + "/");
    const result = await page.evaluate(
      ([s, p]) =>
        new Promise((resolve) => {
          const w = new Worker(s, { type: "module" });
          w.onmessage = ({ data }) => {
            w.terminate();
            resolve(data);
          };
          w.onerror = (e) => resolve({ ok: false, error: e.message });
          w.postMessage({ phase: p });
        }),
      [script, phase]
    );
    await page.close();
    if (!result.ok) throw new Error(`${script} ${phase}: ${result.error}\npassed: ${result.results}`);
    return result;
  };
  const written = await run("/worker.js", "write");
  const reread = await run("/worker.js", "read");
  console.log(`OPFS pool in Chromium: ${[...written.results, ...reread.results].length} checks passed`);
  if (wasmDir) {
    await run("/db-worker.js", "write");
    const { result } = await run("/db-worker.js", "read");
    assert.equal(result.dense, true, "dense search after reload");
    assert.equal(result.text, 20, "full-text search after reload");
    console.log(`Antfly database on OPFS in Chromium: reload + search passed (${result.stats.files} files, ${result.stats.bytes} bytes)`);
  }
} finally {
  await browser.close();
  server.close();
}
