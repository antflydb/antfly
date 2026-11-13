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

import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import vm from "node:vm";

class Element {
  children = [];
  style = {};
  attributes = {};
  textContent = "";
  value = "";
  readyState = 1;
  playCount = 0;
  constructor(tag = "div") { this.tagName = tag; }
  append(...nodes) { this.children.push(...nodes); }
  replaceChildren(...nodes) { this.children = nodes; }
  setAttribute(key, value) { this.attributes[key] = value; }
  set innerHTML(_) { throw Error("Record content must never become HTML"); }
  play() { this.playCount++; return Promise.resolve(); }
  load() { this.readyState = 1; this.onloadedmetadata?.(); }
}
const flush = () => new Promise(resolve => setImmediate(resolve));
const descendants = node => [node, ...node.children.flatMap(descendants)];
const reply = body => ({ ok: true, json: async () => body });
const collections = [
  { id: "pdf", label: "PDF records", profile: "pdf" },
  { id: "audio", label: "Audio recordings", profile: "transcription" },
];
function hit(title, provenance, metadata = {}) {
  return {
    _id: "source/fixture",
    _source: { title, metadata },
    hierarchy: { matches: provenance.map(p => ({
      _source: { text: "Verbatim <script> transcript evidence" },
      hierarchy: { ancestors: { unit: { document: { provenance: p } } } },
    })) },
  };
}
function fixture(search) {
  const ids = new Map(["status", "results", "collection", "search", "query", "mode"].map(id => ["#" + id, new Element()]));
  ids.get("#collection").value = "all";
  ids.get("#mode").value = "text";
  ids.get("#query").value = "records";
  const calls = [];
  const document = { querySelector: s => ids.get(s), createElement: tag => new Element(tag), createElementNS: (_, tag) => new Element(tag) };
  const context = vm.createContext({ document, URLSearchParams, AbortController, fetch: async (url, options) => {
    if (url === "/api/collections") return reply(collections);
    calls.push({ url, options }); return search(url, options);
  } });
  vm.runInContext(readFileSync(new URL("./explorer.js", import.meta.url), "utf8"), context);
  return { ids, calls, submit: () => ids.get("#search").onsubmit({ preventDefault() {} }) };
}
test("PDF and audio retain collection identity, safe text, page and temporal citations", async () => {
  const f = fixture(async () => reply({ responses: [
    { collection: collections[0], hits: { hits: [hit("PDF fixture", [{ page_number: 2 }], { page_start: 10 })] } },
    { collection: collections[1], hits: { hits: [hit("<script>Audio fixture", [{ start_ms: 0, end_ms: 1000 }, { start_ms: 2000, end_ms: 5000 }])] } },
  ] }));
  await flush();
  assert.deepEqual(f.ids.get("#collection").children.map(x => x.textContent), ["PDF records", "Audio recordings"]);
  await f.submit();
  assert.equal(new URL(f.calls[0].url, "https://explorer.test").searchParams.get("collection"), "all");
  const [pdf, audio] = f.ids.get("#results").children;
  const pdfLink = descendants(pdf).find(x => x.tagName === "a");
  assert.match(pdfLink.href, /collection=pdf#page=11$/);
  const audioLink = descendants(audio).find(x => x.tagName === "a");
  assert.match(audioLink.href, /collection=audio#t=0$/);
  const player = descendants(audio).find(x => x.tagName === "audio");
  assert.equal(player.preload, "none");
  assert.equal(player.controls, true);
  assert.match(player.src, /^\/original\?/);
  const buttons = descendants(audio).filter(x => x.tagName === "button");
  assert.equal(buttons[1].textContent, "Play 0:02–0:05");
  buttons[1].onclick();
  assert.equal(player.currentTime, 2);
  assert.equal(player.playCount, 1);
  assert.equal(descendants(audio).find(x => x.tagName === "h3").textContent, "<script>Audio fixture");
  assert.match(descendants(audio).find(x => x.tagName === "p").textContent, /<script>/);
});
test("a stale response cannot replace a newer collection search", async () => {
  const pending = [];
  const f = fixture(() => new Promise(resolve => pending.push(resolve)));
  await flush();
  const old = f.submit(); await flush();
  f.ids.get("#collection").value = "audio";
  const latest = f.submit(); await flush();
  assert.equal(f.calls[0].options.signal.aborted, true);
  assert.equal(new URL(f.calls[1].url, "https://explorer.test").searchParams.get("collection"), "audio");
  const response = title => reply({ responses: [{ collection: collections[1], hits: { hits: [hit(title, [])] } }] });
  pending[1](response("Latest")); await latest;
  pending[0](response("Stale")); await old;
  assert.equal(descendants(f.ids.get("#results")).find(x => x.tagName === "h3").textContent, "Latest");
});
