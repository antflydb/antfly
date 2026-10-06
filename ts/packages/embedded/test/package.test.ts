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

import { execFileSync } from "node:child_process";
import {
  mkdirSync,
  mkdtempSync,
  readdirSync,
  realpathSync,
  rmSync,
  symlinkSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

const packageDir = dirname(dirname(fileURLToPath(import.meta.url)));
const platforms = [
  ["linux", "x64", "libantfly.so"],
  ["linux", "arm64", "libantfly.so"],
  ["darwin", "arm64", "libantfly.dylib"],
] as const;
let installation: string;
let consumer: string;

beforeAll(() => {
  installation = realpathSync(mkdtempSync(join(tmpdir(), "antfly-embedded-package-")));
  consumer = realpathSync(mkdtempSync(join(tmpdir(), "antfly-embedded-consumer-")));
  execFileSync("pnpm", ["run", "build"], { cwd: packageDir });
  execFileSync(
    "npm",
    ["pack", "--ignore-scripts", "--json", "--pack-destination", installation, "."],
    { cwd: packageDir }
  );
  const archives = readdirSync(installation).filter((name) => name.endsWith(".tgz"));
  const archive = archives[0];
  if (archives.length !== 1 || !archive) throw new Error("npm pack must produce one archive");
  const selector = join(installation, "node_modules", "@antfly", "embedded");
  mkdirSync(selector, { recursive: true });
  execFileSync("tar", [
    "-xzf",
    join(installation, archive),
    "-C",
    selector,
    "--strip-components=1",
  ]);
  symlinkSync(
    realpathSync(join(packageDir, "node_modules", "koffi")),
    join(installation, "node_modules", "koffi"),
    "junction"
  );
  for (const [platform, arch, library] of platforms) {
    const name = `@antfly/embedded-${platform}-${arch}`;
    const directory = join(installation, "node_modules", name);
    mkdirSync(join(directory, "lib"), { recursive: true });
    writeFileSync(join(directory, "package.json"), JSON.stringify({ name, version: "0.1.0" }));
    // Discovery only: these files must never be loaded through the C ABI.
    writeFileSync(join(directory, "lib", library), "native discovery fixture");
  }
  const sourceLibrary = join(installation, "zig", "zig-out", "lib");
  mkdirSync(sourceLibrary, { recursive: true });
  writeFileSync(join(sourceLibrary, "libantfly.so"), "source discovery fixture");
}, 60_000);

afterAll(() => {
  if (installation) rmSync(installation, { recursive: true, force: true });
  if (consumer) rmSync(consumer, { recursive: true, force: true });
});

function discover(format: "cjs" | "esm", platform: string, arch: string) {
  const entry = join(installation, `consumer.${format === "cjs" ? "cjs" : "mjs"}`);
  const load =
    format === "cjs"
      ? 'const { resolveLibrary } = require("@antfly/embedded");'
      : 'import { resolveLibrary } from "@antfly/embedded";';
  writeFileSync(
    entry,
    `${load}\nconsole.log(JSON.stringify(resolveLibrary(${JSON.stringify({ platform, arch, env: {} })})));\n`
  );
  return JSON.parse(execFileSync(process.execPath, [entry], { cwd: consumer, encoding: "utf8" }));
}

// Load real packed exports through Node's conditional exports, without a
// resolver mock, an explicit library override, or a checkout working directory.
describe.each(["cjs", "esm"] as const)("packed %s entry point", (format) => {
  it.each(platforms)("discovers the installed %s/%s native package", (platform, arch, library) => {
    expect(discover(format, platform, arch)).toEqual({
      path: join(
        installation,
        "node_modules",
        `@antfly/embedded-${platform}-${arch}`,
        "lib",
        library
      ),
      source: "embedded-package",
    });
  });

  it("finds a source library relative to the package rather than the working directory", () => {
    expect(discover(format, "linux", "riscv64")).toEqual({
      path: join(installation, "zig", "zig-out", "lib", "libantfly.so"),
      source: "source-tree",
    });
  });
});
