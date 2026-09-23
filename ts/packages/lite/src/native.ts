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
 * koffi struct/function declarations for the libantfly C ABI (antfly.h), and
 * a lazily-loaded singleton library handle. Everything above this module
 * works with plain JS values (Buffer, string, number/bigint, plain objects);
 * koffi specifics stay contained here.
 */
import koffi, { type KoffiFunc, type LibraryHandle } from "koffi";
import { type ResolvedLibrary, resolveLibrary } from "./discovery.js";
import { InvalidArgumentError } from "./errors.js";
import { Storage } from "./types.js";

// antfly_storage_kind_* values from antfly.h. These do NOT match this
// binding's public Storage enum numbering (Storage.Lite is 0 for ergonomic
// defaulting); storageKind() below translates between them, mirroring the Go
// binding's Storage.cKind().
const ANTFLY_STORAGE_KIND_DIRECTORY = 0;
const ANTFLY_STORAGE_KIND_LITE = 1;

/** Translates the public Storage enum to the antfly_storage_kind_* wire value. */
export function storageKind(storage: Storage | undefined): number {
  switch (storage ?? Storage.Lite) {
    case Storage.Lite:
      return ANTFLY_STORAGE_KIND_LITE;
    case Storage.Directory:
      return ANTFLY_STORAGE_KIND_DIRECTORY;
    default:
      throw new InvalidArgumentError(`invalid Storage value: ${storage}`);
  }
}

// --- Struct types (must match zig/pkg/antfly/include/antfly.h exactly) ----

export const AntflySlice = koffi.struct("antfly_slice", {
  ptr: "const uint8_t *",
  len: "size_t",
});

export const AntflyBuffer = koffi.struct("antfly_buffer", {
  ptr: "uint8_t *",
  len: "size_t",
});

export const AntflyWriteIntent = koffi.struct("antfly_write_intent", {
  key: AntflySlice,
  value: AntflySlice,
  is_delete: "bool",
});

// antfly_version_predicate is part of the ABI surface but never populated by
// this binding (see antfly_db_batch/write_transaction below): predicates are
// always passed as (null, 0), matching the Go binding's public surface.
export const AntflyVersionPredicate = koffi.struct("antfly_version_predicate", {
  key: AntflySlice,
  expected_version: "uint64_t",
});

// antfly_db is an opaque handle (see "Naming" in antfly.h): a typed pointer,
// not void *. Field order and types below must match antfly_open_options in
// antfly.h exactly; ABI size agreement is checked at runtime against
// antfly_open_options_size() (see validateAbi in abi.ts).
const AntflyDbOpaque = koffi.opaque("antfly_db");
export const PAntflyDb = koffi.pointer(AntflyDbOpaque);
const PAntflyDbOut = koffi.out(koffi.pointer(AntflyDbOpaque, 2));

export const AntflyOpenOptions = koffi.struct("antfly_open_options", {
  abi_size: "uint32_t",
  storage_kind: "uint32_t",
  open_mode: "uint32_t",
  profile: "uint32_t",
  flags: "uint32_t",
  reserved0: "uint32_t",
  map_size: "uint64_t",
  ttl_cleanup_enabled: "bool",
  ttl_cleanup_lease_owned: "bool",
  ttl_cleanup_batch_size: "uint32_t",
  ttl_cleanup_owner_id: AntflySlice,
  ttl_cleanup_lease_ttl_ms: "uint64_t",
  ttl_cleanup_interval_ms: "uint64_t",
  ttl_cleanup_grace_period_ns: "uint64_t",
  inference_host_budget_mb: "uint32_t",
  inference_backend_budget_mb: "uint32_t",
  inference_process_memory_budget_mb: "uint32_t",
  inference_combined_budget_mb: "uint32_t",
  inference_kv_budget_mb: "uint32_t",
  inference_scratch_budget_mb: "uint32_t",
  busy_timeout_ms: "uint64_t",
  reserved: koffi.array("uint64_t", 8),
});

const PAntflyOpenOptions = koffi.pointer(AntflyOpenOptions);
const PAntflyOpenOptionsOut = koffi.out(PAntflyOpenOptions);
const PAntflyBufferOut = koffi.out(koffi.pointer(AntflyBuffer));
const PAntflyWriteIntentArray = koffi.pointer(AntflyWriteIntent);
const PAntflyVersionPredicateArray = koffi.pointer(AntflyVersionPredicate);
const PAntflySliceArray = koffi.pointer(AntflySlice);
const PUint8Out = koffi.out(koffi.pointer("uint8_t"));
const PUint64Out = koffi.out(koffi.pointer("uint64_t"));
const PBoolOut = koffi.out(koffi.pointer("bool"));

export interface NativeLibrary {
  handle: LibraryHandle;
  resolved: ResolvedLibrary;

  abiVersion: KoffiFunc<() => number>;
  threadingMode: KoffiFunc<() => number>;
  openOptionsSize: KoffiFunc<() => number>;
  errorCodeName: KoffiFunc<(code: number) => string>;
  errorCodeDescription: KoffiFunc<(code: number) => string>;
  openOptionsInit: KoffiFunc<(options: object) => number>;

  dbOpenWithOptions: KoffiFunc<(path: string, options: object, outHandle: unknown[]) => number>;
  dbCreateWithOptions: KoffiFunc<(path: string, options: object, outHandle: unknown[]) => number>;
  liteOpenHosted: KoffiFunc<(path: string, outHandle: unknown[]) => number>;
  liteCreateHosted: KoffiFunc<(path: string, outHandle: unknown[]) => number>;

  dbClose: KoffiFunc<(handle: unknown) => void>;
  bufferFree: KoffiFunc<(buffer: object) => void>;

  dbStatusJson: KoffiFunc<(handle: unknown, out: object) => number>;
  dbCapabilitiesJson: KoffiFunc<(handle: unknown, out: object) => number>;
  dbBackup: KoffiFunc<(handle: unknown, out: object) => number>;
  dbImportBackup: KoffiFunc<(handle: unknown, backup: object) => number>;
  restoreBackupJson: KoffiFunc<
    (
      destPath: string,
      options: object | null,
      backup: object,
      replace: boolean,
      out: object
    ) => number
  >;
  restoreBackupFileJson: KoffiFunc<
    (
      destPath: string,
      options: object | null,
      backupPath: string,
      replace: boolean,
      out: object
    ) => number
  >;
  liteCheckJson: KoffiFunc<(handle: unknown, out: object) => number>;
  liteCheckFileJson: KoffiFunc<(path: string, out: object) => number>;
  liteCopyStableSnapshotJson: KoffiFunc<
    (handle: unknown, destPath: string, replace: boolean, out: object) => number
  >;
  liteCopyStableSnapshotFileJson: KoffiFunc<
    (srcPath: string, destPath: string, replace: boolean, out: object) => number
  >;
  liteCompactJson: KoffiFunc<(handle: unknown, out: object) => number>;
  liteVacuumJson: KoffiFunc<(handle: unknown, out: object) => number>;
  dbRunUntilIdle: KoffiFunc<(handle: unknown) => number>;
  dbRunUntilIdleJson: KoffiFunc<(handle: unknown, out: object) => number>;
  dbReplayGeneratedEnrichmentsJson: KoffiFunc<(handle: unknown, out: object) => number>;
  dbPendingWorkStatsJson: KoffiFunc<(handle: unknown, out: object) => number>;

  dbBatch: KoffiFunc<
    (
      handle: unknown,
      writes: object[] | null,
      writeCount: number,
      predicates: object[] | null,
      predicateCount: number,
      timestampNs: number | bigint,
      syncLevel: number
    ) => number
  >;
  dbBatchJson: KoffiFunc<(handle: unknown, request: object, out: object) => number>;
  dbBeginTransactionWithId: KoffiFunc<
    (
      handle: unknown,
      txnId: Uint8Array,
      timestampNs: number | bigint,
      participants: object[] | null,
      participantCount: number
    ) => number
  >;
  dbWriteTransaction: KoffiFunc<
    (
      handle: unknown,
      txnId: Uint8Array,
      writes: object[] | null,
      writeCount: number,
      predicates: object[] | null,
      predicateCount: number
    ) => number
  >;
  dbResolveIntents: KoffiFunc<
    (handle: unknown, txnId: Uint8Array, status: number, commitVersion: number | bigint) => number
  >;
  dbGetTransactionStatus: KoffiFunc<
    (handle: unknown, txnId: Uint8Array, outStatus: number[]) => number
  >;
  dbGetCommitVersion: KoffiFunc<
    (handle: unknown, txnId: Uint8Array, outVersion: (number | bigint)[]) => number
  >;

  dbLookupJson: KoffiFunc<(handle: unknown, key: object, out: object) => number>;
  dbGetRaw: KoffiFunc<(handle: unknown, key: object, out: object) => number>;
  dbGetSchemaJson: KoffiFunc<(handle: unknown, out: object) => number>;
  dbSetSchemaJson: KoffiFunc<(handle: unknown, schema: object) => number>;
  dbListIndexesJson: KoffiFunc<(handle: unknown, out: object) => number>;
  dbAddIndexJson: KoffiFunc<(handle: unknown, config: object) => number>;
  dbDeleteIndex: KoffiFunc<(handle: unknown, name: object, outDeleted: boolean[]) => number>;
  dbListEnrichmentsJson: KoffiFunc<(handle: unknown, out: object) => number>;
  dbAddEnrichmentJson: KoffiFunc<(handle: unknown, config: object) => number>;
  dbDeleteEnrichment: KoffiFunc<
    (handle: unknown, kind: object, name: object, outDeleted: boolean[]) => number
  >;
  dbScanJson: KoffiFunc<(handle: unknown, request: object, out: object) => number>;
  dbStatsJson: KoffiFunc<(handle: unknown, out: object) => number>;
  dbSearchJson: KoffiFunc<(handle: unknown, request: object, out: object) => number>;
  dbSearchDenseWire: KoffiFunc<(handle: unknown, request: object, out: object) => number>;
  dbSearchTextMatchWire: KoffiFunc<(handle: unknown, request: object, out: object) => number>;
  dbSearchTextTermWire: KoffiFunc<(handle: unknown, request: object, out: object) => number>;
  dbSearchTextMatchPhraseWire: KoffiFunc<(handle: unknown, request: object, out: object) => number>;
  dbAggregateHitsJson: KoffiFunc<(handle: unknown, request: object, out: object) => number>;
  dbLookupArtifactJson: KoffiFunc<
    (handle: unknown, artifactIdBase64: object, out: object) => number
  >;
  decodeArtifactIdJson: KoffiFunc<(artifactIdBase64: object, out: object) => number>;
  dbExtractEnrichmentsJson: KoffiFunc<(handle: unknown, request: object, out: object) => number>;
  dbComputeEnrichmentsJson: KoffiFunc<(handle: unknown, request: object, out: object) => number>;

  dbGetEdgesJson: KoffiFunc<
    (
      handle: unknown,
      indexName: object,
      key: object,
      edgeType: object,
      direction: number,
      out: object
    ) => number
  >;
  dbTraverseEdgesJson: KoffiFunc<(handle: unknown, request: object, out: object) => number>;
  dbExecuteGraphQueriesJson: KoffiFunc<(handle: unknown, request: object, out: object) => number>;
  dbGetNeighborsJson: KoffiFunc<
    (
      handle: unknown,
      indexName: object,
      key: object,
      edgeType: object,
      direction: number,
      out: object
    ) => number
  >;
  dbFindShortestPathJson: KoffiFunc<(handle: unknown, request: object, out: object) => number>;
  dbFindKShortestPathsJson: KoffiFunc<(handle: unknown, request: object, out: object) => number>;
  dbMatchPatternJson: KoffiFunc<(handle: unknown, request: object, out: object) => number>;
}

let cached: NativeLibrary | undefined;
let cachedError: unknown;

/** Loads (once) and returns the libantfly bindings, throwing if unavailable. */
export function loadNative(): NativeLibrary {
  if (cached) {
    return cached;
  }
  if (cachedError) {
    throw cachedError;
  }
  try {
    cached = buildNative();
    return cached;
  } catch (err) {
    cachedError = err;
    throw err;
  }
}

/** Resets the cached library handle; for tests only. */
export function resetNativeForTests(): void {
  cached = undefined;
  cachedError = undefined;
}

/**
 * Native stack for every libantfly call. koffi runs foreign calls on its own
 * stacks (128 KiB for async calls by default), but libantfly needs the
 * minimum documented in zig/CAPI.md "Thread Safety" (ANTFLY_MIN_THREAD_STACK_SIZE
 * in antfly.h): 8 MiB. Smaller stacks crash inside the storage engine.
 */
export const NATIVE_STACK_SIZE = 8 * 1024 * 1024;

function configureKoffiStacks(): void {
  const current = koffi.config();
  if (
    (current.sync_stack_size ?? 0) >= NATIVE_STACK_SIZE &&
    (current.async_stack_size ?? 0) >= NATIVE_STACK_SIZE
  ) {
    return;
  }
  koffi.config({
    ...current,
    sync_stack_size: Math.max(current.sync_stack_size ?? 0, NATIVE_STACK_SIZE),
    async_stack_size: Math.max(current.async_stack_size ?? 0, NATIVE_STACK_SIZE),
  });
}

function buildNative(): NativeLibrary {
  const resolved = resolveLibrary();
  configureKoffiStacks();
  const handle = koffi.load(resolved.path);

  const f = handle.func.bind(handle);

  return {
    handle,
    resolved,

    abiVersion: f("antfly_abi_version", "uint32_t", []),
    threadingMode: f("antfly_threading_mode", "uint32_t", []),
    openOptionsSize: f("antfly_open_options_size", "uint32_t", []),
    errorCodeName: f("antfly_error_code_name", "str", ["uint32_t"]),
    errorCodeDescription: f("antfly_error_code_description", "str", ["uint32_t"]),
    openOptionsInit: f("antfly_open_options_init", "uint32_t", [PAntflyOpenOptionsOut]),

    dbOpenWithOptions: f("antfly_db_open_with_options", "uint32_t", [
      "str",
      PAntflyOpenOptions,
      PAntflyDbOut,
    ]),
    dbCreateWithOptions: f("antfly_db_create_with_options", "uint32_t", [
      "str",
      PAntflyOpenOptions,
      PAntflyDbOut,
    ]),
    liteOpenHosted: f("antfly_lite_open_hosted", "uint32_t", ["str", PAntflyDbOut]),
    liteCreateHosted: f("antfly_lite_create_hosted", "uint32_t", ["str", PAntflyDbOut]),

    dbClose: f("antfly_db_close", "void", [PAntflyDb]),
    bufferFree: f("antfly_buffer_free", "void", [koffi.pointer(AntflyBuffer)]),

    dbStatusJson: f("antfly_db_status_json", "uint32_t", [PAntflyDb, PAntflyBufferOut]),
    dbCapabilitiesJson: f("antfly_db_capabilities_json", "uint32_t", [PAntflyDb, PAntflyBufferOut]),
    dbBackup: f("antfly_db_backup", "uint32_t", [PAntflyDb, PAntflyBufferOut]),
    dbImportBackup: f("antfly_db_import_backup", "uint32_t", [PAntflyDb, AntflySlice]),
    restoreBackupJson: f("antfly_restore_backup_json", "uint32_t", [
      "str",
      PAntflyOpenOptions,
      AntflySlice,
      "bool",
      PAntflyBufferOut,
    ]),
    restoreBackupFileJson: f("antfly_restore_backup_file_json", "uint32_t", [
      "str",
      PAntflyOpenOptions,
      "str",
      "bool",
      PAntflyBufferOut,
    ]),
    liteCheckJson: f("antfly_lite_check_json", "uint32_t", [PAntflyDb, PAntflyBufferOut]),
    liteCheckFileJson: f("antfly_lite_check_file_json", "uint32_t", ["str", PAntflyBufferOut]),
    liteCopyStableSnapshotJson: f("antfly_lite_copy_stable_snapshot_json", "uint32_t", [
      PAntflyDb,
      "str",
      "bool",
      PAntflyBufferOut,
    ]),
    liteCopyStableSnapshotFileJson: f("antfly_lite_copy_stable_snapshot_file_json", "uint32_t", [
      "str",
      "str",
      "bool",
      PAntflyBufferOut,
    ]),
    liteCompactJson: f("antfly_lite_compact_json", "uint32_t", [PAntflyDb, PAntflyBufferOut]),
    liteVacuumJson: f("antfly_lite_vacuum_json", "uint32_t", [PAntflyDb, PAntflyBufferOut]),
    dbRunUntilIdle: f("antfly_db_run_until_idle", "uint32_t", [PAntflyDb]),
    dbRunUntilIdleJson: f("antfly_db_run_until_idle_json", "uint32_t", [
      PAntflyDb,
      PAntflyBufferOut,
    ]),
    dbReplayGeneratedEnrichmentsJson: f("antfly_db_replay_generated_enrichments_json", "uint32_t", [
      PAntflyDb,
      PAntflyBufferOut,
    ]),
    dbPendingWorkStatsJson: f("antfly_db_pending_work_stats_json", "uint32_t", [
      PAntflyDb,
      PAntflyBufferOut,
    ]),

    dbBatch: f("antfly_db_batch", "uint32_t", [
      PAntflyDb,
      PAntflyWriteIntentArray,
      "size_t",
      PAntflyVersionPredicateArray,
      "size_t",
      "uint64_t",
      "uint8_t",
    ]),
    dbBatchJson: f("antfly_db_batch_json", "uint32_t", [PAntflyDb, AntflySlice, PAntflyBufferOut]),
    dbBeginTransactionWithId: f("antfly_db_begin_transaction_with_id", "uint32_t", [
      PAntflyDb,
      "const uint8_t *",
      "uint64_t",
      PAntflySliceArray,
      "size_t",
    ]),
    dbWriteTransaction: f("antfly_db_write_transaction", "uint32_t", [
      PAntflyDb,
      "const uint8_t *",
      PAntflyWriteIntentArray,
      "size_t",
      PAntflyVersionPredicateArray,
      "size_t",
    ]),
    dbResolveIntents: f("antfly_db_resolve_intents", "uint32_t", [
      PAntflyDb,
      "const uint8_t *",
      "uint8_t",
      "uint64_t",
    ]),
    dbGetTransactionStatus: f("antfly_db_get_transaction_status", "uint32_t", [
      PAntflyDb,
      "const uint8_t *",
      PUint8Out,
    ]),
    dbGetCommitVersion: f("antfly_db_get_commit_version", "uint32_t", [
      PAntflyDb,
      "const uint8_t *",
      PUint64Out,
    ]),

    dbLookupJson: f("antfly_db_lookup_json", "uint32_t", [
      PAntflyDb,
      AntflySlice,
      PAntflyBufferOut,
    ]),
    dbGetRaw: f("antfly_db_get_raw", "uint32_t", [PAntflyDb, AntflySlice, PAntflyBufferOut]),
    dbGetSchemaJson: f("antfly_db_get_schema_json", "uint32_t", [PAntflyDb, PAntflyBufferOut]),
    dbSetSchemaJson: f("antfly_db_set_schema_json", "uint32_t", [PAntflyDb, AntflySlice]),
    dbListIndexesJson: f("antfly_db_list_indexes_json", "uint32_t", [PAntflyDb, PAntflyBufferOut]),
    dbAddIndexJson: f("antfly_db_add_index_json", "uint32_t", [PAntflyDb, AntflySlice]),
    dbDeleteIndex: f("antfly_db_delete_index", "uint32_t", [PAntflyDb, AntflySlice, PBoolOut]),
    dbListEnrichmentsJson: f("antfly_db_list_enrichments_json", "uint32_t", [
      PAntflyDb,
      PAntflyBufferOut,
    ]),
    dbAddEnrichmentJson: f("antfly_db_add_enrichment_json", "uint32_t", [PAntflyDb, AntflySlice]),
    dbDeleteEnrichment: f("antfly_db_delete_enrichment", "uint32_t", [
      PAntflyDb,
      AntflySlice,
      AntflySlice,
      PBoolOut,
    ]),
    dbScanJson: f("antfly_db_scan_json", "uint32_t", [PAntflyDb, AntflySlice, PAntflyBufferOut]),
    dbStatsJson: f("antfly_db_stats_json", "uint32_t", [PAntflyDb, PAntflyBufferOut]),
    dbSearchJson: f("antfly_db_search_json", "uint32_t", [
      PAntflyDb,
      AntflySlice,
      PAntflyBufferOut,
    ]),
    dbSearchDenseWire: f("antfly_db_search_dense_wire", "uint32_t", [
      PAntflyDb,
      AntflySlice,
      PAntflyBufferOut,
    ]),
    dbSearchTextMatchWire: f("antfly_db_search_text_match_wire", "uint32_t", [
      PAntflyDb,
      AntflySlice,
      PAntflyBufferOut,
    ]),
    dbSearchTextTermWire: f("antfly_db_search_text_term_wire", "uint32_t", [
      PAntflyDb,
      AntflySlice,
      PAntflyBufferOut,
    ]),
    dbSearchTextMatchPhraseWire: f("antfly_db_search_text_match_phrase_wire", "uint32_t", [
      PAntflyDb,
      AntflySlice,
      PAntflyBufferOut,
    ]),
    dbAggregateHitsJson: f("antfly_db_aggregate_hits_json", "uint32_t", [
      PAntflyDb,
      AntflySlice,
      PAntflyBufferOut,
    ]),
    dbLookupArtifactJson: f("antfly_db_lookup_artifact_json", "uint32_t", [
      PAntflyDb,
      AntflySlice,
      PAntflyBufferOut,
    ]),
    decodeArtifactIdJson: f("antfly_decode_artifact_id_json", "uint32_t", [
      AntflySlice,
      PAntflyBufferOut,
    ]),
    dbExtractEnrichmentsJson: f("antfly_db_extract_enrichments_json", "uint32_t", [
      PAntflyDb,
      AntflySlice,
      PAntflyBufferOut,
    ]),
    dbComputeEnrichmentsJson: f("antfly_db_compute_enrichments_json", "uint32_t", [
      PAntflyDb,
      AntflySlice,
      PAntflyBufferOut,
    ]),

    dbGetEdgesJson: f("antfly_db_get_edges_json", "uint32_t", [
      PAntflyDb,
      AntflySlice,
      AntflySlice,
      AntflySlice,
      "uint8_t",
      PAntflyBufferOut,
    ]),
    dbTraverseEdgesJson: f("antfly_db_traverse_edges_json", "uint32_t", [
      PAntflyDb,
      AntflySlice,
      PAntflyBufferOut,
    ]),
    dbExecuteGraphQueriesJson: f("antfly_db_execute_graph_queries_json", "uint32_t", [
      PAntflyDb,
      AntflySlice,
      PAntflyBufferOut,
    ]),
    dbGetNeighborsJson: f("antfly_db_get_neighbors_json", "uint32_t", [
      PAntflyDb,
      AntflySlice,
      AntflySlice,
      AntflySlice,
      "uint8_t",
      PAntflyBufferOut,
    ]),
    dbFindShortestPathJson: f("antfly_db_find_shortest_path_json", "uint32_t", [
      PAntflyDb,
      AntflySlice,
      PAntflyBufferOut,
    ]),
    dbFindKShortestPathsJson: f("antfly_db_find_k_shortest_paths_json", "uint32_t", [
      PAntflyDb,
      AntflySlice,
      PAntflyBufferOut,
    ]),
    dbMatchPatternJson: f("antfly_db_match_pattern_json", "uint32_t", [
      PAntflyDb,
      AntflySlice,
      PAntflyBufferOut,
    ]),
  };
}
