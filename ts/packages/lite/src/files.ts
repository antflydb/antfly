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
  type AnyNativeFn,
  callAsync,
  newBufferOut,
  parseJson,
  sliceOf,
  stringSlice,
  takeBuffer,
  toRawBytes,
} from "./marshal.js";
import { loadNative } from "./native.js";
import type { CheckReport, StableSnapshotReport } from "./types.js";

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

async function restoreViaFn(fn: AnyNativeFn, args: unknown[]): Promise<void> {
  const native = loadNative();
  const out = newBufferOut();
  const code = await callAsync(fn, ...args, out);
  checkCode(code);
  takeBuffer(native, out); // discard the JSON result, matching go/pkg/lite/files.go
}

/**
 * Creates or replaces a Lite database at path from an in-memory portable
 * Antfly backup archive. OutcomeUnknown means the destination was published
 * but crash durability could not be confirmed; inspect it and do not retry
 * automatically.
 */
export function restoreBackup(path: string, backup: Uint8Array, replace: boolean): Promise<void> {
  if (!path.endsWith(".aflite") || backup.length === 0) {
    return Promise.reject(
      new InvalidArgumentError("path must end with .aflite and backup must be non-empty")
    );
  }
  const native = loadNative();
  return restoreViaFn(native.liteRestoreBackupJson, [path, sliceOf(toRawBytes(backup)), replace]);
}

/**
 * Creates or replaces a Lite database at path from an in-memory portable
 * Antfly backup archive (antfly_lite_restore_json). See restoreBackup.
 */
export function restore(path: string, backup: Uint8Array, replace: boolean): Promise<void> {
  if (!path.endsWith(".aflite") || backup.length === 0) {
    return Promise.reject(
      new InvalidArgumentError("path must end with .aflite and backup must be non-empty")
    );
  }
  const native = loadNative();
  return restoreViaFn(native.liteRestoreJson, [path, sliceOf(toRawBytes(backup)), replace]);
}

/**
 * Creates or replaces a Lite database by streaming a portable Antfly backup
 * archive file with bounded memory use. Busy means the source changed during
 * streaming or the source/destination is concurrently locked; retry after
 * the files are stable and no writer is active. Unsupported means the source
 * filesystem lacks required advisory locking; copy the archive to a
 * supported local filesystem. OutcomeUnknown means the destination was
 * published but crash durability could not be confirmed; inspect it and do
 * not retry automatically.
 */
export function restoreBackupFile(
  path: string,
  backupPath: string,
  replace: boolean
): Promise<void> {
  if (!path.endsWith(".aflite") || !backupPath.endsWith(".afb")) {
    return Promise.reject(
      new InvalidArgumentError("path must end with .aflite and backupPath with .afb")
    );
  }
  const native = loadNative();
  return restoreViaFn(native.liteRestoreBackupFileJson, [path, backupPath, replace]);
}

/** Alias of restoreBackupFile, matching go/pkg/lite/files.go's RestoreFile. */
export function restoreFile(path: string, backupPath: string, replace: boolean): Promise<void> {
  return restoreBackupFile(path, backupPath, replace);
}

/** Decodes a base64 artifact ID without opening a database. */
export async function decodeArtifactIdRaw(artifactIdBase64: string): Promise<Buffer> {
  const native = loadNative();
  const out = newBufferOut();
  const code = await callAsync(native.dbDecodeArtifactIdJson, stringSlice(artifactIdBase64), out);
  checkCode(code);
  return takeBuffer(native, out);
}

/** Typed form of decodeArtifactIdRaw. */
export async function decodeArtifactId(artifactIdBase64: string): Promise<unknown> {
  return parseJson(await decodeArtifactIdRaw(artifactIdBase64));
}
