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

/**
 * @antfly/lite -- Node.js binding for embedded Antfly Lite (the libantfly C
 * ABI), via koffi. Node-only: see README.md for browser/WASM notes.
 *
 * @example
 * ```ts
 * import { create, THREADING_SERIALIZED } from "@antfly/lite";
 *
 * await using db = await create("my.aflite");
 * await db.batch([{ key: "doc:1", value: { title: "hello" } }], BigInt(Date.now()) * 1_000_000n);
 * const hit = await db.lookup("doc:1");
 * console.log(hit);
 * ```
 */

export { AbiMismatchError, abiVersion, threadingMode, validateAbi } from "./abi.js";
export {
  create,
  createHosted,
  createWithOptions,
  Database,
  open,
  openHosted,
  openReadonly,
  openStatusOnly,
  openWithOptions,
} from "./database.js";
export {
  cliPlatformPackageName,
  LibraryNotFoundError,
  type LibrarySource,
  platformLibraryFileName,
  type ResolvedLibrary,
  resolveLibrary,
  UnsupportedPlatformError,
} from "./discovery.js";
export {
  AntflyError,
  BusyError,
  ErrorCode,
  errorCodeDescription,
  errorCodeName,
  IntentConflictError,
  InternalError,
  InvalidArgumentError,
  NotFoundError,
  OutcomeUnknownError,
  StalledError,
  TxnNotFoundError,
  UnsupportedError,
  VersionConflictError,
} from "./errors.js";
export {
  checkFile,
  checkFileRaw,
  copyStableSnapshotFile,
  copyStableSnapshotFileRaw,
  decodeArtifactId,
  decodeArtifactIdRaw,
  restore,
  restoreBackup,
  restoreBackupFile,
  restoreFile,
} from "./files.js";
export type { JsonInput, Uint64Like } from "./marshal.js";
/** Native stack, in bytes, every libantfly call runs on (ANTFLY_MIN_THREAD_STACK_SIZE). */
export { NATIVE_STACK_SIZE as MIN_THREAD_STACK_SIZE } from "./native.js";
export type {
  Capabilities,
  CheckReport,
  CompactReport,
  InferenceStatus,
  OpenOptions,
  PendingWorkStatus,
  ReplayGeneratedEnrichmentsResult,
  StableSnapshotReport,
  Status,
  StorageStatus,
  TTLCleanupOptions,
  VacuumReport,
  WriteIntent,
} from "./types.js";
export {
  GraphDirection,
  InferenceMode,
  OpenMode,
  Profile,
  SUPPORTED_ABI_VERSION,
  THREADING_SERIALIZED,
  TxnStatus,
} from "./types.js";
