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

import { validateAbi } from "./abi.js";
import { checkCode, InvalidArgumentError } from "./errors.js";
import {
  callAsync,
  newBufferOut,
  parseJson,
  sliceOf,
  stringSlice,
  takeBuffer,
  toRawBytes,
} from "./marshal.js";
import { loadNative, type NativeLibrary, storageKind } from "./native.js";
import {
  type CheckReport,
  type RestoreOptions,
  type StableSnapshotReport,
  Storage,
} from "./types.js";

/**
 * Builds the antfly_open_options struct for a restore destination: its
 * storage_kind selects the kind of database created at dest_path (NULL means
 * directory per antfly.h, so this binding always passes an explicit struct
 * defaulting to Storage.Lite, matching the Go binding's restoreCOptions).
 */
function restoreOpenOptions(native: NativeLibrary, storage: Storage | undefined): object {
  const options: Record<string, unknown> = {};
  checkCode(native.openOptionsInit(options));
  options.storage_kind = storageKind(storage);
  return options;
}

/** Runs Lite integrity checks for path without opening a database handle and returns the raw JSON result. */
export async function checkFileRaw(path: string): Promise<Buffer> {
  validateAbi();
  const native = loadNative();
  const out = newBufferOut();
  const code = await callAsync(native.liteCheckFileJson, path, out);
  checkCode(code);
  return takeBuffer(native, out);
}

/** Runs Lite integrity checks for path without opening a database handle and returns the typed result. */
export async function checkFile(path: string): Promise<CheckReport> {
  return parseJson(await checkFileRaw(path)) as CheckReport;
}

/**
 * Opens srcPath read-only, copies a stable Lite snapshot to destPath, and
 * returns the raw JSON result. Neither path needs an open handle.
 */
export async function copyStableSnapshotFileRaw(
  srcPath: string,
  destPath: string,
  replace: boolean
): Promise<Buffer> {
  if (!srcPath.endsWith(".aflite") || !destPath.endsWith(".aflite")) {
    throw new InvalidArgumentError("srcPath and destPath must end with .aflite");
  }
  const native = loadNative();
  const out = newBufferOut();
  const code = await callAsync(
    native.liteCopyStableSnapshotFileJson,
    srcPath,
    destPath,
    replace,
    out
  );
  checkCode(code);
  return takeBuffer(native, out);
}

/** Typed form of copyStableSnapshotFileRaw. */
export async function copyStableSnapshotFile(
  srcPath: string,
  destPath: string,
  replace: boolean
): Promise<StableSnapshotReport> {
  return parseJson(
    await copyStableSnapshotFileRaw(srcPath, destPath, replace)
  ) as StableSnapshotReport;
}

/**
 * Creates a database at path from a portable Antfly backup archive, of the
 * storage kind opts.storage selects (default Storage.Lite). A backup of
 * either kind restores into either kind. For Lite destinations path must end
 * with .aflite (checked client-side only). OutcomeUnknown means the
 * destination was published but crash durability could not be confirmed;
 * inspect it and do not retry automatically.
 */
export async function restore(
  path: string,
  backup: Uint8Array,
  opts: RestoreOptions = {}
): Promise<void> {
  if ((opts.storage ?? Storage.Lite) === Storage.Lite && !path.endsWith(".aflite")) {
    throw new InvalidArgumentError("path must end with .aflite for Storage.Lite");
  }
  if (backup.length === 0) {
    throw new InvalidArgumentError("backup must be non-empty");
  }
  const native = loadNative();
  const options = restoreOpenOptions(native, opts.storage);
  const out = newBufferOut();
  const code = await callAsync(
    native.restoreBackupJson,
    path,
    options,
    sliceOf(toRawBytes(backup)),
    Boolean(opts.replace),
    out
  );
  checkCode(code);
  takeBuffer(native, out); // discard the JSON result, matching go/pkg/lite/files.go
}

/**
 * restore() reading the archive from backupPath. For Lite destinations it
 * streams with bounded memory use. Busy means the source changed during
 * streaming or the source/destination is concurrently locked; retry after
 * the files are stable and no writer is active. Unsupported means the source
 * filesystem lacks required advisory locking; copy the archive to a
 * supported local filesystem.
 */
export async function restoreFile(
  path: string,
  backupPath: string,
  opts: RestoreOptions = {}
): Promise<void> {
  if (!backupPath.endsWith(".afb")) {
    throw new InvalidArgumentError("backupPath must end with .afb");
  }
  if ((opts.storage ?? Storage.Lite) === Storage.Lite && !path.endsWith(".aflite")) {
    throw new InvalidArgumentError("path must end with .aflite for Storage.Lite");
  }
  const native = loadNative();
  const options = restoreOpenOptions(native, opts.storage);
  const out = newBufferOut();
  const code = await callAsync(
    native.restoreBackupFileJson,
    path,
    options,
    backupPath,
    Boolean(opts.replace),
    out
  );
  checkCode(code);
  takeBuffer(native, out); // discard the JSON result, matching go/pkg/lite/files.go
}

/** Decodes a base64 artifact ID without opening a database. */
export async function decodeArtifactIdRaw(artifactIdBase64: string): Promise<Buffer> {
  const native = loadNative();
  const out = newBufferOut();
  const code = await callAsync(native.decodeArtifactIdJson, stringSlice(artifactIdBase64), out);
  checkCode(code);
  return takeBuffer(native, out);
}

/** Typed form of decodeArtifactIdRaw. */
export async function decodeArtifactId(artifactIdBase64: string): Promise<unknown> {
  return parseJson(await decodeArtifactIdRaw(artifactIdBase64));
}
