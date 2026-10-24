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

import { readFile, writeFile, mkdir } from "node:fs/promises";
import { randomBytes } from "node:crypto";
import { resolve } from "node:path";
const account = process.argv[2];
if (!/^[a-f0-9]{32}$/.test(account ?? "")) throw new Error("usage: node qualification/prepare.mjs ACCOUNT_ID");
const id = `antfly-1278-${Date.now().toString(36)}-${randomBytes(3).toString("hex")}`;
const dir = resolve("dist/qualification", id); await mkdir(dir, {recursive: true});
const config = JSON.parse(await readFile(process.argv.includes("--r2-coordinator") ? "wrangler.r2.jsonc" : "wrangler.jsonc", "utf8"));
Object.assign(config, { name: id, account_id: account, main: resolve("qualification/worker.mjs"),
  assets: {...config.assets, directory: resolve("public")},
  vars: {ANTFLY_INCARNATION: `qualification-${id}`, AI_MODEL: "@cf/baai/bge-small-en-v1.5", AI_GATEWAY: "default", AUTO_GC: "false"},
  version_metadata: {binding: "WORKER_VERSION"}, observability: {enabled: true, head_sampling_rate: 1} });
if (process.argv.includes("--paged-text")) Object.assign(config.vars, {PAGED_TEXT: "true", PUBLICATION_RECORDS: "64", LOG_METRICS: "true"});
if (process.argv.includes("--r2-coordinator")) Object.assign(config.vars, {COORDINATOR_BACKEND: "r2", PAGED_PROJECTIONS: "true", LOG_METRICS: "true"});
delete config.triggers;
config.r2_buckets[0].bucket_name = id;
config.queues.producers[0].queue = id;
config.queues.consumers[0].queue = id;
config.queues.consumers[0].dead_letter_queue = `${id}-dead`;
await writeFile(`${dir}/wrangler.json`, JSON.stringify(config, null, 2));
await writeFile(`${dir}/api-token`, randomBytes(32).toString("hex"), {mode: 0o600});
const run = {id, account, dir, config: `${dir}/wrangler.json`, token_file: `${dir}/api-token`, worker: id, bucket: id, queues: [id, `${id}-dead`]};
await writeFile(`${dir}/run.json`, JSON.stringify(run, null, 2));
console.log(`${dir}/run.json`);
