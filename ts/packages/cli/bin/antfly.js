#!/usr/bin/env node

import { spawnSync } from "node:child_process";
import { existsSync } from "node:fs";
import { createRequire } from "node:module";
import { join } from "node:path";

const require = createRequire(import.meta.url);

const packageByPlatform = {
  "darwin-arm64": "@antfly/cli-darwin-arm64",
  "linux-arm64": "@antfly/cli-linux-arm64",
  "linux-x64": "@antfly/cli-linux-x64"
};

const platformKey = `${process.platform}-${process.arch}`;
const packageName = packageByPlatform[platformKey];

if (!packageName) {
  console.error(`Unsupported Antfly CLI platform: ${platformKey}`);
  process.exit(1);
}

let packageJsonPath;
try {
  packageJsonPath = require.resolve(`${packageName}/package.json`);
} catch {
  console.error(`Missing Antfly CLI platform package: ${packageName}`);
  console.error("Reinstall @antfly/cli and verify optional dependencies were not disabled.");
  process.exit(1);
}

const executable = join(packageJsonPath, "..", "bin", process.platform === "win32" ? "antfly.exe" : "antfly");
if (!existsSync(executable)) {
  console.error(`Antfly CLI executable is missing: ${executable}`);
  process.exit(1);
}

const result = spawnSync(executable, process.argv.slice(2), { stdio: "inherit" });
if (result.error) {
  console.error(result.error.message);
  process.exit(1);
}

process.exit(result.status ?? 1);
