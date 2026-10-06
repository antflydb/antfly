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

import { existsSync as nodeExistsSync } from "node:fs";
import { createRequire } from "node:module";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

/** Where a resolved libantfly path came from, for diagnostics/logging. */
export type LibrarySource =
  | "ANTFLY_LIBRARY"
  | "ANTFLY_LIB_DIR"
  | "cli-package"
  | "source-tree"
  | "system";

export interface ResolvedLibrary {
  /** Absolute path to the shared library, or a bare loader name for "system". */
  path: string;
  source: LibrarySource;
}

export class LibraryNotFoundError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "LibraryNotFoundError";
  }
}

export class UnsupportedPlatformError extends Error {
  constructor(platform: string) {
    super(`@antfly/lite does not support platform ${platform}`);
    this.name = "UnsupportedPlatformError";
  }
}

/** The libantfly shared library file name for a platform, e.g. "libantfly.dylib". */
export function platformLibraryFileName(platform: NodeJS.Platform): string | undefined {
  switch (platform) {
    case "darwin":
      return "libantfly.dylib";
    case "linux":
      return "libantfly.so";
    case "win32":
      return "antfly.dll";
    default:
      return undefined;
  }
}

/** The @antfly/cli-<platform> package that ships a matching prebuilt libantfly, if any. */
export function cliPlatformPackageName(
  platform: NodeJS.Platform,
  arch: string
): string | undefined {
  if (platform === "darwin" && arch === "arm64") {
    return "@antfly/cli-darwin-arm64";
  }
  if (platform === "linux" && (arch === "arm64" || arch === "x64")) {
    return `@antfly/cli-linux-${arch}`;
  }
  return undefined;
}

export interface ResolveLibraryOptions {
  platform?: NodeJS.Platform;
  arch?: string;
  env?: NodeJS.ProcessEnv;
  /** Directory to walk up from when looking for zig/zig-out/lib (a source checkout). Defaults to this package's own directory. */
  startDir?: string;
  existsSync?: (path: string) => boolean;
  /** Like require.resolve; used to locate an installed @antfly/cli-<platform> package. */
  resolve?: (specifier: string) => string;
}

function defaultStartDir(): string {
  try {
    // import.meta.url is only available in ESM; the CJS build gets an
    // equivalent shim from esbuild. Fall back to cwd if neither works.
    return dirname(fileURLToPath(import.meta.url));
  } catch {
    return process.cwd();
  }
}

function findUp(
  startDir: string,
  relative: string,
  existsSync: (path: string) => boolean
): string | undefined {
  let dir = startDir;
  for (let i = 0; i < 16; i++) {
    const candidate = join(dir, relative);
    if (existsSync(candidate)) {
      return candidate;
    }
    const parent = dirname(dir);
    if (parent === dir) {
      return undefined;
    }
    dir = parent;
  }
  return undefined;
}

/**
 * Resolves the libantfly shared library to load, in order:
 *
 * 1. `ANTFLY_LIBRARY` env var: an exact file path.
 * 2. `ANTFLY_LIB_DIR` env var: a directory containing the platform library file.
 * 3. The installed `@antfly/cli-<platform>` package's `lib/` directory.
 * 4. `zig/zig-out/lib/` found by walking up from this package (a source checkout).
 * 5. The bare system loader name, resolved by the OS's shared library search
 *    path (LD_LIBRARY_PATH / DYLD_LIBRARY_PATH / PATH / rpath).
 *
 * Throws LibraryNotFoundError if an explicit env var names a path that does
 * not exist, so misconfiguration fails loudly instead of silently falling
 * through to another tier.
 */
export function resolveLibrary(options: ResolveLibraryOptions = {}): ResolvedLibrary {
  const platform = options.platform ?? process.platform;
  const arch = options.arch ?? process.arch;
  const env = options.env ?? process.env;
  const existsSync = options.existsSync ?? nodeExistsSync;

  const fileName = platformLibraryFileName(platform);
  if (!fileName) {
    throw new UnsupportedPlatformError(platform);
  }

  const explicitLibrary = env.ANTFLY_LIBRARY;
  if (explicitLibrary) {
    if (!existsSync(explicitLibrary)) {
      throw new LibraryNotFoundError(`ANTFLY_LIBRARY=${explicitLibrary} does not exist`);
    }
    return { path: explicitLibrary, source: "ANTFLY_LIBRARY" };
  }

  const libDir = env.ANTFLY_LIB_DIR;
  if (libDir) {
    const candidate = join(libDir, fileName);
    if (!existsSync(candidate)) {
      throw new LibraryNotFoundError(`ANTFLY_LIB_DIR=${libDir} does not contain ${fileName}`);
    }
    return { path: candidate, source: "ANTFLY_LIB_DIR" };
  }

  const packageName = cliPlatformPackageName(platform, arch);
  if (packageName) {
    try {
      const resolve = options.resolve ?? createRequire(import.meta.url).resolve;
      const packageJsonPath = resolve(`${packageName}/package.json`);
      const candidate = join(dirname(packageJsonPath), "lib", fileName);
      if (existsSync(candidate)) {
        return { path: candidate, source: "cli-package" };
      }
    } catch {
      // Platform package not installed (it's an optionalDependency of
      // @antfly/cli, not of this package); fall through to other tiers.
    }
  }

  const startDir = options.startDir ?? defaultStartDir();
  const sourceTreeLibrary = findUp(startDir, join("zig", "zig-out", "lib", fileName), existsSync);
  if (sourceTreeLibrary) {
    return { path: sourceTreeLibrary, source: "source-tree" };
  }

  return { path: fileName, source: "system" };
}
