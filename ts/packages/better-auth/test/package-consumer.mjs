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

import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { cp, mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

// Test the package boundary with no workspace links or native-library override.
// The supplied library must come from the same qualified source as the packages.
const root = resolve(dirname(fileURLToPath(import.meta.url)), "../../../..");
const packages = join(root, "ts/packages");
const library = process.env.ANTFLY_LIBRARY;
assert.ok(library, "Set ANTFLY_LIBRARY to the qualified native library");
const suffix = `${process.platform}-${process.arch}`;
assert.ok(["darwin-arm64", "linux-x64", "linux-arm64"].includes(suffix));
const authManifest = JSON.parse(await readFile(join(packages, "better-auth/package.json")));
const nativeVersion = authManifest.peerDependencies["@antfly/embedded"].replace(/^\^/, "");
assert.match(nativeVersion, /^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?$/);
const directory = await mkdtemp(join(tmpdir(), "antfly-auth-packages-"));
const tarballs = join(directory, "tarballs");
const consumer = join(directory, "consumer");
function run(command, args, cwd, env = process.env) {
  execFileSync(command, args, { cwd, env, stdio: "inherit" });
}
try {
  await mkdir(tarballs);
  await mkdir(consumer);
  for (const name of ["sdk", "better-auth"])
    run("pnpm", ["pack", "--pack-destination", tarballs], join(packages, name));
  for (const name of ["embedded", `embedded-${suffix}`]) {
    const source = join(packages, name);
    const stage = join(directory, name);
    await mkdir(stage);
    const manifest = JSON.parse(await readFile(join(source, "package.json")));
    manifest.version = nativeVersion;
    if (manifest.optionalDependencies)
      for (const key of Object.keys(manifest.optionalDependencies))
        manifest.optionalDependencies[key] = nativeVersion;
    // Pack the built binding and the qualified library, without install/build hooks.
    await writeFile(join(stage, "package.json"), JSON.stringify(manifest));
    await cp(join(source, "README.md"), join(stage, "README.md"));
    await cp(join(source, "LICENSE"), join(stage, "LICENSE"));
    if (name === "embedded")
      await cp(join(source, "dist"), join(stage, "dist"), { recursive: true });
    else {
      await mkdir(join(stage, "lib"));
      await cp(library, join(stage, "lib", basename(library)));
    }
    run("npm", ["pack", "--ignore-scripts", "--pack-destination", tarballs], stage);
  }
  const sdkManifest = JSON.parse(await readFile(join(packages, "sdk/package.json")));
  await writeFile(
    join(consumer, "package.json"),
    JSON.stringify({ name: "auth-consumer", private: true, type: "module" })
  );
  run(
    "npm",
    [
      "install",
      "--no-audit",
      "--no-fund",
      join(tarballs, `antfly-sdk-${sdkManifest.version}.tgz`),
      join(tarballs, `antfly-better-auth-${authManifest.version}.tgz`),
      join(tarballs, `antfly-embedded-${nativeVersion}.tgz`),
      join(tarballs, `antfly-embedded-${suffix}-${nativeVersion}.tgz`),
      `better-auth@${authManifest.devDependencies["better-auth"]}`,
    ],
    consumer
  );
  await cp(new URL("./installed-smoke.mjs", import.meta.url), join(consumer, "smoke.mjs"));
  const env = { ...process.env };
  delete env.ANTFLY_LIBRARY;
  delete env.ANTFLY_LIB_DIR;
  run(process.execPath, ["smoke.mjs"], consumer, env);
} finally {
  await rm(directory, { recursive: true, force: true });
}
