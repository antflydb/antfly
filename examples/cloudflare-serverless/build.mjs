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

import { mkdir, readFile, writeFile, copyFile, cp } from "node:fs/promises";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
const root = fileURLToPath(new URL("../../", import.meta.url));
const out = fileURLToPath(new URL("./dist/", import.meta.url));
await mkdir(out, { recursive: true });
function run(cmd, args, cwd = root) {
  const result = spawnSync(cmd, args, { cwd, stdio: "inherit" });
  if (result.error) throw result.error;
  if (result.status !== 0) throw new Error(`${cmd} exited ${result.status}`);
}
const compiler = process.env.ANTFLY_ZIG ?? "zig";
const version = spawnSync(compiler, ["version"], { encoding: "utf8" });
if (version.status !== 0 || !version.stdout.startsWith("0.17.")) throw new Error("Antfly requires its pinned Zig 0.17 toolchain; set ANTFLY_ZIG to that compiler");
if (process.argv.includes("--wal-only")) {
  run(compiler, ["build-exe", "-target", "wasm32-freestanding", "-OReleaseSafe",
    "-fno-entry", "-rdynamic", "--export-memory", "--stack", "1048576", "--max-memory=67108864",
    "--dep", "objectstore", "-Mroot=zig/pkg/antfly/src/serverless_host_wasm.zig",
    "--dep", "antfly_platform", "-Mobjectstore=zig/lib/objectstore/src/root.zig",
    "-Mantfly_platform=zig/lib/platform/src/root.zig", `-femit-bin=${out}host.raw.wasm`]);
} else {
  run(compiler, ["build", "serverless-wasm", "-Dmetal=false", "-j2", ...(process.env.ANTFLY_HOST_DEBUG ? ["-Dserverless-debug=true"] : [])], `${root}zig`);
  await copyFile(`${root}zig/zig-out/antfly-serverless/host.raw.wasm`, `${out}host.raw.wasm`);
}
// Preserve the license closure for the query runtime, which includes server
// sources as well as Apache embedded/storage components.
await cp(`${root}LICENSES`, `${out}LICENSES`, { recursive: true });
for (const name of ["LICENSE", "LICENSING.md", "THIRD_PARTY_NOTICES.md"]) await copyFile(`${root}${name}`, `${out}${name}`);
// Binaryen is pinned in package-lock.json. Asyncify instruments only the one
// asynchronous object-store import; no native cloud transport is linked.
if (process.env.ANTFLY_WASM_OPT) {
  const tool = process.env.ANTFLY_WASM_OPT;
  const version = spawnSync(tool, ["--version"], { encoding: "utf8" });
  if (version.status !== 0 || !version.stdout.includes("version 132 ")) throw new Error("ANTFLY_WASM_OPT must point to Binaryen 132");
  run(tool, [`${out}host.raw.wasm`, "--strip-dwarf", "--asyncify", "--pass-arg=asyncify-imports@antfly_host.object_call", "--enable-simd", "--enable-bulk-memory", "--enable-mutable-globals", "--enable-sign-ext", "--enable-nontrapping-float-to-int", "--enable-threads", "-O1", "--coalesce-locals", ...(process.env.ANTFLY_HOST_DEBUG ? ["-g"] : []), "-o", `${out}antfly-serverless.wasm`]);
} else {
  const binaryen = (await import("binaryen")).default;
  const module = binaryen.readBinary(await readFile(`${out}host.raw.wasm`));
  const features = binaryen.Features;
  module.setFeatures(features.MVP | features.Atomics | features.MutableGlobals | features.NontrappingFPToInt |
    features.SIMD128 | features.BulkMemory | features.BulkMemoryOpt | features.SignExt);
  binaryen.setOptimizeLevel(1);
  binaryen.setPassArgument("asyncify-imports", "antfly_host.object_call");
  module.runPasses(["strip-dwarf", "asyncify"]);
  module.optimize();
  module.runPasses(["coalesce-locals"]);
  if (!module.validate()) throw new Error("invalid instrumented WASM");
  await writeFile(`${out}antfly-serverless.wasm`, module.emitBinary());
  module.dispose();
}
