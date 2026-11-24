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

// End-to-end on-device search in headless Chromium: the real mail-public app
// with the Worker's CSP, the built runtime (npm run build:local) and a real
// published model (npm run publish:local-model -- --out <dir>). Only the
// server APIs are stubbed. Opt-in because it downloads and runs a 552 MiB
// model on CPU:
//
//   LOCAL_SEARCH_MODEL=dist/local-model node test/local-search-browser.mjs
import { createServer } from "node:http";
import { readFile, stat } from "node:fs/promises";
import { extname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { createRequire } from "node:module";
import assert from "node:assert/strict";
import { contentSecurityPolicy } from "../mail-worker.mjs";
import { syncMessage } from "../mail-core.mjs";

const { chromium } = createRequire(import.meta.url)("playwright");
const root = fileURLToPath(new URL("../", import.meta.url));
const modelDir = process.env.LOCAL_SEARCH_MODEL && resolve(process.env.LOCAL_SEARCH_MODEL);
if (!modelDir) {
  console.log("skipped: set LOCAL_SEARCH_MODEL to a published model directory");
  process.exit(0);
}
const types = {
  ".html": "text/html", ".js": "text/javascript", ".mjs": "text/javascript", ".css": "text/css",
  ".json": "application/json", ".wasm": "application/wasm", ".svg": "image/svg+xml",
  ".webmanifest": "application/manifest+json", ".wgsl": "text/plain",
};
const mailbox = { id: "0b5c1f4e-7d0a-4c3b-9a1e-2f6d8c4b5a10", address: "me@example.test" };
const mail = [
  ["Invoice #4471 from Rapid Plumbing", "Thanks for having us out to fix the burst pipe under the kitchen sink. The total is $340, due in 30 days."],
  ["Team offsite agenda", "We will hike in the morning, then strategy sessions after lunch and dinner at the lodge. Bring boots."],
  ["Your flight to Lisbon is confirmed", "TAP Air Portugal flight TP 202 departs Friday at 18:40. Check in opens 24 hours before departure."],
  ["Quarterly security training reminder", "Please complete the phishing awareness module before the end of the month."],
  ["Dinner on Saturday?", "We are trying the new ramen place downtown at 7. Let me know if you can make it!"],
].map(([subject, text], i) =>
  syncMessage({
    id: `00000000-0000-4000-8000-00000000000${i}`,
    subject, text, from: `sender${i}@example.net`, to: mailbox.address, received_at: 1_700_000_000_000 + i,
  })
);
let syncCalls = 0;
// The server's sync log: message upserts, then deletion tombstones appended later.
const log = mail.map((message) => ({ message }));
const deletedIds = new Set();
const trashedIds = new Set();

async function file(path) {
  try {
    if ((await stat(path)).isFile()) return readFile(path);
  } catch {}
  return null;
}
const server = createServer(async (request, response) => {
  const url = new URL(request.url, "http://localhost");
  const send = (status, body, type = "application/json", extra = {}) =>
    response.writeHead(status, { "Content-Type": type, "Content-Security-Policy": contentSecurityPolicy, ...extra }).end(body);
  if (url.pathname === "/api/me") return send(200, JSON.stringify({ user: { id: "u", email: "me@example.test" }, mailbox }));
  if (url.pathname === "/mail/messages") return send(200, JSON.stringify({ messages: [], cursor: null }));
  if (url.pathname === "/mail/sync") {
    syncCalls++;
    const after = Number(url.searchParams.get("cursor") || 0);
    const entries = log.slice(after, after + 3);
    return send(200, JSON.stringify({
      messages: entries.filter((e) => e.message && !deletedIds.has(e.message.id) && !trashedIds.has(e.message.id)).map((e) => e.message),
      deleted: entries.filter((e) => e.deleted).map((e) => e.deleted),
      cursor: String(after + entries.length),
      more: after + entries.length < log.length,
    }));
  }
  const trashRoute = /^\/mail\/messages\/([a-f0-9-]{36})\/(trash|restore)$/.exec(url.pathname);
  if (trashRoute && request.method === "POST") {
    const message = mail.find((m) => m.id === trashRoute[1] && !deletedIds.has(m.id));
    if (!message) return send(404, JSON.stringify({ error: "NotFound" }));
    if (trashRoute[2] === "trash") {
      trashedIds.add(message.id);
      log.push({ deleted: message.id }); // devices hold the inbox only
      return send(200, JSON.stringify({ id: message.id, trashed: true }));
    }
    trashedIds.delete(message.id);
    log.push({ message }); // re-announced: devices fetch and embed it again
    return send(200, JSON.stringify({ id: message.id, restored: true }));
  }
  const messageRoute = /^\/mail\/messages\/([a-f0-9-]{36})$/.exec(url.pathname);
  if (messageRoute) {
    const message = mail.find((m) => m.id === messageRoute[1] && !deletedIds.has(m.id));
    if (!message) return send(404, JSON.stringify({ error: "NotFound" }));
    if (request.method === "DELETE") {
      deletedIds.add(message.id);
      log.push({ deleted: message.id });
      return send(200, JSON.stringify({ id: message.id, deleted: true }));
    }
    return send(200, JSON.stringify(message));
  }
  if (url.pathname.startsWith("/local/models/")) {
    const body = await file(join(modelDir, url.pathname.slice("/local/models/".length)));
    return body ? send(200, body, "application/octet-stream") : send(404, "");
  }
  let path = url.pathname === "/" ? "/index.html" : url.pathname;
  let body = await file(join(root, "mail-public", path));
  // Runtime files over the Workers asset limit are staged for R2.
  if (!body && path.startsWith("/local/")) body = await file(join(root, "dist", "local-r2", path.slice("/local/".length)));
  if (!body) return send(404, "not found", "text/plain");
  send(200, body, types[extname(path)] ?? "application/octet-stream");
});
await new Promise((ok) => server.listen(0, "127.0.0.1", ok));
const origin = `http://127.0.0.1:${server.address().port}`;
const browser = await chromium.launch();
const started = Date.now();
const elapsed = () => ((Date.now() - started) / 1000).toFixed(0) + "s";
try {
  const context = await browser.newContext();
  const page = await context.newPage();
  page.on("dialog", (dialog) => dialog.accept());
  const errors = [];
  page.on("pageerror", (error) => errors.push(error.message));
  page.on("console", (message) => message.type() === "error" && errors.push(message.text()));
  await page.goto(origin + "/");
  await page.waitForSelector("#mail:not([hidden])");
  assert.equal(await page.isHidden("#local"), false, "on-device search is offered");
  await page.evaluate(() => (document.getElementById("local").open = true));
  await page.check("#local-toggle");
  await page.waitForFunction(() => /Up to date|unavailable/.test(document.getElementById("local-status").textContent), null, { timeout: 30 * 60_000 });
  const status = await page.textContent("#local-status");
  assert.match(status, /Up to date/, `sync finished: ${status} ${errors.join(" | ")}`);
  assert.match(await page.textContent("#local-badge"), /5 messages on this device/);
  console.log(`[${elapsed()}] opted in, downloaded the model and indexed ${mail.length} messages: ${status}`);

  async function deviceSearch(query) {
    await page.selectOption("#search-where", "device");
    await page.fill("#search input[name=query]", query);
    // Clear the previous results so the wait below sees this search's rows.
    await page.evaluate(() => document.getElementById("messages").replaceChildren());
    await page.click("#search button");
    await page.waitForFunction(() => document.querySelector("#messages .mail-row .local"), null, { timeout: 120_000 });
    return page.$$eval("#messages .mail-row strong", (rows) => rows.map((row) => row.textContent));
  }
  const queries = [
    ["how much do I owe the plumber", "Invoice #4471 from Rapid Plumbing"],
    ["when does my plane leave", "Your flight to Lisbon is confirmed"],
    ["weekend noodle plans with friends", "Dinner on Saturday?"],
  ];
  for (const [query, expected] of queries) {
    const results = await deviceSearch(query);
    assert.equal(results[0], expected, `${query} -> ${results.join(" / ")}`);
    console.log(`[${elapsed()}] on-device "${query}" -> ${results[0]}`);
  }

  // Reload: the copy reopens from OPFS without re-indexing anything.
  const callsBefore = syncCalls;
  await page.reload();
  await page.waitForFunction(() => /Up to date|unavailable/.test(document.getElementById("local-status").textContent), null, { timeout: 15 * 60_000 });
  assert.equal(syncCalls - callsBefore, 1, "a reload only checks for new mail");
  const [first] = await deviceSearch("do I have mandatory compliance training");
  assert.equal(first, "Quarterly security training reminder");
  console.log(`[${elapsed()}] after reload, from OPFS: "do I have mandatory compliance training" -> ${first}`);
  // Delete moves to Trash: the message leaves this device's copy at once.
  await deviceSearch("how much do I owe the plumber");
  await page.click("#messages .mail-row");
  await page.waitForSelector("#message button.danger");
  await page.click("#message button.danger");
  await page.waitForFunction(() => document.getElementById("message").textContent === "Message moved to Trash.");
  let results = await deviceSearch("how much do I owe the plumber");
  assert.ok(!results.includes("Invoice #4471 from Rapid Plumbing"), `trashed message is gone: ${results.join(" / ")}`);
  assert.match(await page.textContent("#local-badge"), /4 messages on this device/);
  console.log(`[${elapsed()}] moved to Trash in the UI -> gone from on-device search (${results.length} left)`);

  // Undo restores it; the device fetches and embeds it again.
  await page.click("#status button"); // Undo
  await page.waitForFunction(() => /5 messages on this device/.test(document.getElementById("local-badge").textContent), null, { timeout: 120_000 });
  results = await deviceSearch("how much do I owe the plumber");
  assert.equal(results[0], "Invoice #4471 from Rapid Plumbing", `restored message is back: ${results.join(" / ")}`);
  console.log(`[${elapsed()}] restored from Trash -> re-embedded and found again`);

  // A deletion made elsewhere (another device) arrives as a sync tombstone.
  await page.evaluate(() => (document.getElementById("local").open = true)); // collapsed after the reload
  const flight = mail.find((m) => m.subject.startsWith("Your flight")).id;
  deletedIds.add(flight);
  log.push({ deleted: flight });
  await page.click("#local-sync");
  await page
    .waitForFunction(() => /4 messages on this device/.test(document.getElementById("local-badge").textContent), null, { timeout: 120_000 })
    .catch(async (error) => {
      const debug = await page.evaluate(() => [document.getElementById("local-status").textContent, document.getElementById("status").textContent, document.getElementById("local-badge").textContent]);
      throw new Error(`tombstone sync did not finish: ${JSON.stringify(debug)} ${errors.join(" | ")}`, { cause: error });
    });
  results = await deviceSearch("when does my plane leave");
  assert.ok(!results.includes("Your flight to Lisbon is confirmed"), `tombstoned message is gone: ${results.join(" / ")}`);
  console.log(`[${elapsed()}] deleted on another device -> removed here by sync (${results.length} left)`);
  assert.deepEqual(errors.filter((e) => !/favicon/.test(e)), [], "no page or console errors");
} finally {
  await browser.close();
  server.close();
}
