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

import { readFile, writeFile, readdir, rm } from "node:fs/promises";
import { resolve, join } from "node:path";
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
const run = JSON.parse(await readFile(process.argv[2], "utf8"));
if (!run.id.startsWith("antfly-1278-")) throw new Error("invalid qualification manifest");
const config = JSON.parse(await readFile(run.config, "utf8"));
config.main = resolve("qualification/worker.mjs");
await writeFile(run.config, JSON.stringify(config, null, 2));
await rm(`${run.dir}/bundle`, {recursive: true, force: true});
const result = spawnSync(process.execPath, ["node_modules/wrangler/bin/wrangler.js", "deploy", "--dry-run", "--config", run.config, "--outdir", `${run.dir}/bundle`], {stdio: "inherit", env: {...process.env, WRANGLER_SEND_METRICS: "false", WRANGLER_LOG_PATH: `${run.dir}/snapshot.log`}});
if (result.status !== 0) throw new Error("qualification snapshot failed");
const hashes = {};
for (const file of await readdir(`${run.dir}/bundle`)) {
  if (!file.endsWith(".wasm") && !file.endsWith(".js")) continue;
  const bytes = await readFile(join(run.dir, "bundle", file));
  if (file.endsWith(".wasm") && !WebAssembly.validate(bytes)) throw new Error("invalid snapshot WASM");
  hashes[file] = createHash("sha256").update(bytes).digest("hex");
}
await writeFile(`${run.dir}/bundle-hashes.json`, JSON.stringify(hashes, null, 2));
config.main = `${run.dir}/bundle/worker.js`;
await writeFile(run.config, JSON.stringify(config, null, 2));
console.log("Sealed qualification bundle:", config.main);
