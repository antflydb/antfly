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

// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

import { describe, expect, it } from "vitest";
import {
  cliPlatformPackageName,
  LibraryNotFoundError,
  platformLibraryFileName,
  resolveLibrary,
  UnsupportedPlatformError,
} from "../src/discovery.js";

// Pure discovery-logic tests: no native library required, and none loaded.

describe("platformLibraryFileName", () => {
  it("maps known platforms", () => {
    expect(platformLibraryFileName("darwin")).toBe("libantfly.dylib");
    expect(platformLibraryFileName("linux")).toBe("libantfly.so");
    expect(platformLibraryFileName("win32")).toBe("antfly.dll");
  });

  it("returns undefined for unsupported platforms", () => {
    expect(platformLibraryFileName("aix")).toBeUndefined();
  });
});

describe("cliPlatformPackageName", () => {
  it("maps darwin/arm64", () => {
    expect(cliPlatformPackageName("darwin", "arm64")).toBe("@antfly/cli-darwin-arm64");
  });
  it("maps linux/arm64 and linux/x64", () => {
    expect(cliPlatformPackageName("linux", "arm64")).toBe("@antfly/cli-linux-arm64");
    expect(cliPlatformPackageName("linux", "x64")).toBe("@antfly/cli-linux-x64");
  });
  it("returns undefined for unsupported combinations", () => {
    expect(cliPlatformPackageName("darwin", "x64")).toBeUndefined();
    expect(cliPlatformPackageName("win32", "x64")).toBeUndefined();
  });
});

describe("resolveLibrary", () => {
  const noneExist = () => false;

  it("uses ANTFLY_LIBRARY when set and the file exists", () => {
    const result = resolveLibrary({
      platform: "darwin",
      arch: "arm64",
      env: { ANTFLY_LIBRARY: "/some/explicit/path.dylib" },
      existsSync: (p) => p === "/some/explicit/path.dylib",
    });
    expect(result).toEqual({ path: "/some/explicit/path.dylib", source: "ANTFLY_LIBRARY" });
  });

  it("throws LibraryNotFoundError when ANTFLY_LIBRARY does not exist", () => {
    expect(() =>
      resolveLibrary({
        platform: "darwin",
        arch: "arm64",
        env: { ANTFLY_LIBRARY: "/nonexistent" },
        existsSync: noneExist,
      })
    ).toThrow(LibraryNotFoundError);
  });

  it("uses ANTFLY_LIB_DIR joined with the platform file name", () => {
    const result = resolveLibrary({
      platform: "linux",
      arch: "x64",
      env: { ANTFLY_LIB_DIR: "/opt/antfly/lib" },
      existsSync: (p) => p === "/opt/antfly/lib/libantfly.so",
    });
    expect(result).toEqual({ path: "/opt/antfly/lib/libantfly.so", source: "ANTFLY_LIB_DIR" });
  });

  it("throws LibraryNotFoundError when ANTFLY_LIB_DIR lacks the file", () => {
    expect(() =>
      resolveLibrary({
        platform: "linux",
        arch: "x64",
        env: { ANTFLY_LIB_DIR: "/opt/antfly/lib" },
        existsSync: noneExist,
      })
    ).toThrow(LibraryNotFoundError);
  });

  it("falls back to the @antfly/cli-<platform> package's lib/ directory", () => {
    const result = resolveLibrary({
      platform: "darwin",
      arch: "arm64",
      env: {},
      existsSync: (p) => p === "/pkg/cli-darwin-arm64/lib/libantfly.dylib",
      resolve: (specifier) => {
        expect(specifier).toBe("@antfly/cli-darwin-arm64/package.json");
        return "/pkg/cli-darwin-arm64/package.json";
      },
    });
    expect(result).toEqual({
      path: "/pkg/cli-darwin-arm64/lib/libantfly.dylib",
      source: "cli-package",
    });
  });

  it("falls through past a missing cli package without throwing", () => {
    const result = resolveLibrary({
      platform: "darwin",
      arch: "arm64",
      env: {},
      existsSync: noneExist,
      resolve: () => {
        throw new Error("module not found");
      },
      startDir: "/nowhere",
    });
    expect(result.source).toBe("system");
    expect(result.path).toBe("libantfly.dylib");
  });

  it("finds zig/zig-out/lib by walking up from startDir", () => {
    const sourceTreeLib = "/repo/antfly/zig/zig-out/lib/libantfly.dylib";
    const result = resolveLibrary({
      platform: "darwin",
      arch: "arm64",
      env: {},
      startDir: "/repo/antfly/ts/packages/lite/dist",
      existsSync: (p) => p === sourceTreeLib,
      resolve: () => {
        throw new Error("not installed");
      },
    });
    expect(result).toEqual({ path: sourceTreeLib, source: "source-tree" });
  });

  it("falls back to the bare system loader name when nothing else resolves", () => {
    const result = resolveLibrary({
      platform: "linux",
      arch: "x64",
      env: {},
      startDir: "/",
      existsSync: noneExist,
      resolve: () => {
        throw new Error("not installed");
      },
    });
    expect(result).toEqual({ path: "libantfly.so", source: "system" });
  });

  it("throws UnsupportedPlatformError for an unsupported platform", () => {
    expect(() => resolveLibrary({ platform: "aix", arch: "ppc64" })).toThrow(
      UnsupportedPlatformError
    );
  });

  it("prefers ANTFLY_LIBRARY over ANTFLY_LIB_DIR", () => {
    const result = resolveLibrary({
      platform: "darwin",
      arch: "arm64",
      env: { ANTFLY_LIBRARY: "/explicit.dylib", ANTFLY_LIB_DIR: "/opt/lib" },
      existsSync: (p) => p === "/explicit.dylib" || p === "/opt/lib/libantfly.dylib",
    });
    expect(result.source).toBe("ANTFLY_LIBRARY");
  });
});
