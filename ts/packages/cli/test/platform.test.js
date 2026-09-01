import assert from "node:assert/strict";
import test from "node:test";

import { detectLinuxLibc, resolvePlatformPackage } from "../bin/platform.js";

const report = (header) => ({ getReport: () => ({ header }) });

test("detects GNU libc and musl", () => {
  assert.equal(detectLinuxLibc(report({ glibcVersionRuntime: "2.36" })), "glibc");
  assert.equal(detectLinuxLibc(report({})), "musl");
  assert.equal(
    detectLinuxLibc({
      getReport: () => {
        throw new Error("disabled");
      },
    }),
    "musl"
  );
});

test("selects a package by operating system, CPU, and libc", () => {
  const glibc = report({ glibcVersionRuntime: "2.36" });
  const musl = report({});

  assert.equal(resolvePlatformPackage("darwin", "arm64", glibc), "@antfly/cli-darwin-arm64");
  assert.equal(resolvePlatformPackage("linux", "arm64", glibc), "@antfly/cli-linux-arm64-gnu");
  assert.equal(resolvePlatformPackage("linux", "x64", glibc), "@antfly/cli-linux-x64-gnu");
  assert.equal(resolvePlatformPackage("linux", "arm64", musl), "@antfly/cli-linux-arm64");
  assert.equal(resolvePlatformPackage("linux", "x64", musl), "@antfly/cli-linux-x64");
  assert.equal(resolvePlatformPackage("darwin", "x64", glibc), undefined);
  assert.equal(resolvePlatformPackage("win32", "x64", glibc), undefined);
});
