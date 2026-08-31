// Copyright 2026 Antfly, Inc.
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

import { spawnSync } from "node:child_process";
import { readFileSync } from "node:fs";

const args = process.argv.slice(2);
if (args.length === 0) {
  console.error("usage: node scripts/run-pinned-toolchain.mjs <command> [args...]");
  process.exit(2);
}

const packageJson = JSON.parse(readFileSync(new URL("../package.json", import.meta.url), "utf8"));
const nodeVersion = packageJson.volta?.node;
const pnpmVersion = packageJson.volta?.pnpm;
const packageManagerVersion = packageJson.packageManager?.match(/^pnpm@([^+]+)(?:\+.*)?$/)?.[1];
if (!nodeVersion || !pnpmVersion || pnpmVersion !== packageManagerVersion) {
  console.error(
    "ts/package.json must pin matching Volta and packageManager versions for Node and pnpm"
  );
  process.exit(2);
}

function run(command, commandArgs, stdio = "inherit") {
  const result = spawnSync(command, commandArgs, { stdio });
  if (result.error) {
    console.error(`${command}: ${result.error.message}`);
    return 127;
  }
  return result.status ?? 1;
}

const hasVolta = run("volta", ["--version"], "ignore") === 0;
if (hasVolta) {
  process.exit(run("volta", ["run", "--node", nodeVersion, "--pnpm", pnpmVersion, ...args]));
}

if (process.versions.node !== nodeVersion) {
  console.error(
    `committed TypeScript artifacts require Node ${nodeVersion}; found ${process.versions.node}. ` +
      "Install Volta or activate the pinned Node version from ts/package.json."
  );
  process.exit(2);
}
if (args[0] === "pnpm") {
  const version = spawnSync("pnpm", ["--version"], { encoding: "utf8" });
  if (version.status !== 0 || version.stdout.trim() !== pnpmVersion) {
    console.error(
      `committed TypeScript artifacts require pnpm ${pnpmVersion}; found ${version.stdout?.trim() || "unavailable"}.`
    );
    process.exit(2);
  }
}
process.exit(run(args[0], args.slice(1)));
