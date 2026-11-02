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

import {readFile, writeFile, mkdir, rm} from "node:fs/promises";
import {resolve} from "node:path";
import assert from "node:assert/strict";
import {randomBytes} from "node:crypto";
const local = process.argv.includes("--local");
const rounds = Number(process.argv.find(x => x.startsWith("--rounds="))?.slice(9) ?? 8);
const rows = Number(process.argv.find(x => x.startsWith("--rows="))?.slice(7) ?? 4096);
assert.ok(Number.isSafeInteger(rounds) && rounds >= 1 && rounds <= 64);
assert.ok(Number.isSafeInteger(rows) && rows >= 128 && rows <= 32768);
const id = `soak-${Date.now().toString(36)}-${randomBytes(3).toString("hex")}`;
let mf, dispatch, output, resourcePath;
if (local) {
  const {Miniflare, convertV4MiniflareOptions} = await import("miniflare");
  resourcePath = resolve("dist/qualification", id, "resources");
  const worker = resolve("qualification/sqlite-soak-worker.mjs");
  const journal = resolve("collection-journal.mjs");
  mf = new Miniflare(convertV4MiniflareOptions({modulesRoot: resolve("."), modules: [{type: "ESModule", path: worker}, {type: "ESModule", path: journal}], compatibilityDate: "2026-10-23", bindings: {API_TOKEN: "local-soak", ANTFLY_INCARNATION: "qualification-sqlite-local"}, durableObjects: {COMMITS: {className: "CommitCoordinator", useSQLite: true}}, resourcePersistencePath: resourcePath}));
  dispatch = input => mf.dispatchFetch("https://example.test/", {method: "POST", headers: {Authorization: "Bearer local-soak"}, body: JSON.stringify(input)});
  output = resolve("dist/qualification", id, "report.json");
} else {
  const run = JSON.parse(await readFile(process.argv[2], "utf8")), origin = process.argv[3];
  const url = new URL(origin);
  assert.ok(run.id.startsWith("antfly-1278-") && url.protocol === "https:" && url.hostname.startsWith(run.id + ".") && url.hostname.endsWith(".workers.dev"));
  const token = (await readFile(run.token_file, "utf8")).trim();
  dispatch = input => fetch(origin, {method: "POST", headers: {Authorization: `Bearer ${token}`}, body: JSON.stringify(input), signal: AbortSignal.timeout(35000)});
  output = resolve(run.dir, `${id}-report.json`);
}
const report = {profile: local ? "workerd SQLite" : "live Cloudflare SQLite", rows, rounds, total_isolate_memory: null, modes: []};
try {
  for (const mode of ["legacy", "drained"]) {
    const started = performance.now(); let result, completed = 0;
    for (let step = 0; step < rounds * (Math.ceil(rows / 64) + Math.ceil(rows / 128) + 16); step++) {
      const response = await dispatch({run: id, rows, steps: 64, mode});
      assert.equal(response.status, 200); result = await response.json();
      if (!result.ok) break;
      if (result.rounds !== completed) {completed = result.rounds; console.log(JSON.stringify({mode, rounds: completed, statements: result.statements, peak_database_bytes: result.peak_database_bytes}));}
      if (completed === rounds) break;
    }
    report.modes.push({mode, duration_ms: performance.now()-started, ...result});
    const destroyed = await dispatch({op: "destroy", run: id, mode}); assert.equal(destroyed.status, 200);
    if (mode === "drained") assert.ok(result?.ok && completed === rounds, JSON.stringify(result));
  }
} finally {
  await mkdir(resolve(output, ".."), {recursive: true}); await writeFile(output, JSON.stringify(report, null, 2));
  if (mf) await mf.dispose();
  if (resourcePath) await rm(resourcePath, {recursive: true, force: true});
}
console.log(JSON.stringify(report)); console.log(`Saved ${output}`);
