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

// Build the optional on-device search runtime into mail-public/local/:
//
//   local/config.json                   (no-cache; points at the versioned dir)
//   local/<build>/antfly/               embedded Antfly WASM + client + OPFS store
//   local/<build>/web/                  @antfly/web (browser inference client)
//   local/<build>/inference/<runtime>/  inference WASM, workers and shaders
//
// <build> is a content hash, so the service worker may cache it forever.
// Files over 25 MiB are moved to dist/local-r2/ for the R2 bucket; pass
// --upload (and --local for `npm run dev:mail`) to upload them here.
// The model is published separately with publish-local-model.mjs.
import { cp, mkdir, readdir, readFile, rename, rm, stat, writeFile } from "node:fs/promises";
import { createHash } from "node:crypto";
import { spawnSync } from "node:child_process";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("../../", import.meta.url));
const here = fileURLToPath(new URL("./", import.meta.url));
const local = join(here, "mail-public", "local");
const staging = join(here, "dist", "local-staging");
const zig = process.env.ANTFLY_ZIG ?? "zig";
// Workers static assets accept files up to 25 MiB; larger files go to R2.
const ASSET_LIMIT = 25 * 1024 ** 2;
const embedding = {
  dimensions: Number(process.env.LOCAL_EMBEDDING_DIMENSIONS ?? 256),
  max_tokens: Number(process.env.LOCAL_EMBEDDING_MAX_TOKENS ?? 256),
};
if (![128, 256, 512, 768].includes(embedding.dimensions)) throw Error("LOCAL_EMBEDDING_DIMENSIONS must be 128, 256, 512 or 768");
if (!(embedding.max_tokens >= 8 && embedding.max_tokens <= 2048)) throw Error("LOCAL_EMBEDDING_MAX_TOKENS must be 8-2048");

function run(cmd, args, cwd = root) {
  const result = spawnSync(cmd, args, { cwd, stdio: "inherit" });
  if (result.error) throw result.error;
  if (result.status !== 0) throw Error(`${cmd} ${args.join(" ")} exited ${result.status}`);
}
const version = spawnSync(zig, ["version"], { encoding: "utf8" });
if (version.status !== 0 || !version.stdout.startsWith("0.17."))
  throw Error("Antfly requires its pinned Zig 0.17 toolchain; set ANTFLY_ZIG to that compiler");

async function files(dir, base = dir) {
  const out = [];
  for (const entry of await readdir(dir, { withFileTypes: true })) {
    const path = join(dir, entry.name);
    if (entry.isDirectory()) out.push(...(await files(path, base)));
    else out.push(path);
  }
  return out.sort();
}

await rm(staging, { recursive: true, force: true });
await mkdir(staging, { recursive: true });

// 1. Embedded database WASM (hosted profile, change-tracked storage exports).
if (!process.env.LOCAL_SKIP_ZIG) run(zig, ["build", "wasm", "-j1"], join(root, "zig"));
const antflyOut = join(root, "zig", "zig-out", "antfly-wasm");
await mkdir(join(staging, "antfly"), { recursive: true });
for (const name of ["antfly.wasm", "antfly_embedded_wasm_client.mjs", "antfly_embedded_opfs_store.mjs", "LICENSE", "THIRD_PARTY_NOTICES.md"])
  await cp(join(antflyOut, name), join(staging, "antfly", name)).catch((error) => {
    if (!["LICENSE", "THIRD_PARTY_NOTICES.md"].includes(name)) throw error;
  });

// 2. Browser inference client and its matching runtime assets.
if (!process.env.LOCAL_SKIP_WEB) {
  run("pnpm", ["--dir", "ts", "install", "--frozen-lockfile", "--filter", "@antfly/web..."]);
  run("pnpm", ["--dir", "ts", "--filter", "@antfly/web", "build"]);
  run("node", [
    join(root, "ts/packages/web/scripts/prepare-runtime.mjs"),
    "--zig-root", join(root, "zig"),
    "--out", join(staging, "inference"),
    "--zig", zig,
  ]);
}
await cp(join(root, "ts/packages/web/dist"), join(staging, "web"), { recursive: true });
const [runtimeDir] = await readdir(join(staging, "inference"));
if (!runtimeDir) throw Error("prepare-runtime produced no runtime directory");

// 3. Content-addressed build directory and config. Files over the Workers
// asset limit (the WASM modules) go to the private R2 bucket under the same
// versioned path; the Worker serves /local/<build>/ from R2 when assets miss.
const hash = createHash("sha256");
const oversized = [];
for (const path of await files(staging)) {
  hash.update(path.slice(staging.length)).update("\0").update(await readFile(path));
  if ((await stat(path)).size > ASSET_LIMIT) oversized.push(path.slice(staging.length + 1));
}
const build = hash.digest("hex").slice(0, 16);
await rm(local, { recursive: true, force: true });
await mkdir(local, { recursive: true });
await cp(staging, join(local, build), { recursive: true });
const r2 = join(here, "dist", "local-r2");
await rm(r2, { recursive: true, force: true });
for (const path of oversized) {
  await mkdir(dirname(join(r2, build, path)), { recursive: true });
  await rename(join(local, build, path), join(r2, build, path));
}
const bucket = process.env.LOCAL_RUNTIME_BUCKET ?? "antfly-mail-models";
for (const path of oversized) {
  const key = `${bucket}/local-runtime/${build}/${path}`;
  if (process.argv.includes("--upload")) {
    const where = process.argv.includes("--local") ? "--local" : "--remote";
    run("npx", ["wrangler", "r2", "object", "put", key, "--file", join(r2, build, path), where], here);
  } else console.log(`upload to R2: npx wrangler r2 object put ${key} --file dist/local-r2/${build}/${path} --remote`);
}
const base = `/local/${build}/`;
const config = {
  build,
  runtime: {
    web: `${base}web/inference.js`,
    inference: `${base}inference/${runtimeDir}/`,
    antfly: {
      wasm: `${base}antfly/antfly.wasm`,
      client: `${base}antfly/antfly_embedded_wasm_client.mjs`,
      store: `${base}antfly/antfly_embedded_opfs_store.mjs`,
    },
  },
  model: { catalog: "/local/models/catalog.json" },
  embedding,
};
await writeFile(join(local, "config.json"), JSON.stringify(config, null, 2) + "\n");
await rm(staging, { recursive: true, force: true });
console.log(`on-device runtime ${build} written to mail-public/local/`);
