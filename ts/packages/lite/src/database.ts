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

import * as fsp from "node:fs/promises";
import { basename, dirname, join } from "node:path";
import { validateAbi } from "./abi.js";
import { checkCode, InvalidArgumentError } from "./errors.js";
import {
  callAsync,
  type DecodedBuffer,
  type JsonInput,
  jsonSlice,
  newBufferOut,
  parseJson,
  type SliceArg,
  sliceOf,
  stringSlice,
  takeBuffer,
  toRawBytes,
  txnIdBytes,
  type Uint64Like,
} from "./marshal.js";
import { loadNative, type NativeLibrary, storageKind } from "./native.js";
import {
  type Capabilities,
  type CheckReport,
  type CompactReport,
  OpenMode,
  type OpenOptions,
  type PendingWorkStatus,
  Profile,
  type ReplayGeneratedEnrichmentsResult,
  type StableSnapshotReport,
  type Status,
  type TxnStatus,
  type VacuumReport,
  type WriteIntent,
} from "./types.js";

const OPEN_FLAG_NO_SYNC = 1 << 0;
const OPEN_FLAG_TTL_CLEANUP = 1 << 1;
const OPEN_FLAG_REMOTE_PROVIDER_CONFIGURED = 1 << 2;
const OPEN_FLAG_LOCAL_RUNTIME_CONFIGURED = 1 << 3;
const OPEN_FLAG_GENERATED_ENRICHMENT_REPLAY = 1 << 4;

type NativeOptionsStruct = Record<string, unknown>;

function buildOpenOptionsStruct(native: NativeLibrary, opts: OpenOptions): NativeOptionsStruct {
  const options: NativeOptionsStruct = {};
  checkCode(native.openOptionsInit(options));

  options.storage_kind = storageKind(opts.storage);
  options.open_mode = opts.mode ?? OpenMode.Writer;
  options.profile = opts.profile ?? Profile.Native;

  let flags = 0;
  if (opts.noSync) flags |= OPEN_FLAG_NO_SYNC;
  if (opts.remoteProviderConfigured) flags |= OPEN_FLAG_REMOTE_PROVIDER_CONFIGURED;
  if (opts.localRuntimeConfigured) flags |= OPEN_FLAG_LOCAL_RUNTIME_CONFIGURED;
  if (opts.generatedEnrichmentReplay) flags |= OPEN_FLAG_GENERATED_ENRICHMENT_REPLAY;
  if (opts.ttlCleanup) flags |= OPEN_FLAG_TTL_CLEANUP;
  options.flags = flags;

  options.map_size = opts.mapSize ?? 0;
  options.inference_host_budget_mb = opts.inferenceHostBudgetMb ?? 0;
  options.inference_backend_budget_mb = opts.inferenceBackendBudgetMb ?? 0;
  options.inference_combined_budget_mb = opts.inferenceCombinedBudgetMb ?? 0;
  options.inference_kv_budget_mb = opts.inferenceKvBudgetMb ?? 0;
  options.inference_scratch_budget_mb = opts.inferenceScratchBudgetMb ?? 0;
  options.inference_process_memory_budget_mb = opts.inferenceProcessMemoryBudgetMb ?? 0;
  options.busy_timeout_ms = opts.busyTimeoutMs ?? 0;

  if (opts.ttlCleanup) {
    const ttl = opts.ttlCleanup;
    options.ttl_cleanup_enabled = ttl.enabled;
    options.ttl_cleanup_lease_owned = ttl.leaseOwned;
    options.ttl_cleanup_batch_size = ttl.batchSize ?? 0;
    options.ttl_cleanup_owner_id = stringSlice(ttl.ownerId ?? "");
    options.ttl_cleanup_lease_ttl_ms = ttl.leaseTtlMs ?? 0;
    options.ttl_cleanup_interval_ms = ttl.intervalMs ?? 0;
    options.ttl_cleanup_grace_period_ns = ttl.gracePeriodNs ?? 0;
  }

  return options;
}

function keySlice(key: string | Uint8Array): SliceArg {
  return typeof key === "string" ? stringSlice(key) : sliceOf(toRawBytes(key));
}

function writeIntentValueBytes(w: WriteIntent): Buffer {
  if (w.delete || w.value == null) {
    return Buffer.alloc(0);
  }
  if (w.value instanceof Uint8Array) {
    return toRawBytes(w.value);
  }
  return Buffer.from(JSON.stringify(w.value), "utf8");
}

function toNativeWriteIntent(w: WriteIntent): NativeOptionsStruct {
  return {
    key: stringSlice(w.key),
    value: sliceOf(writeIntentValueBytes(w)),
    is_delete: Boolean(w.delete),
  };
}

/**
 * An embedded Antfly Lite database handle.
 *
 * A Database is safe for concurrent use from any number of in-flight
 * promises: libantfly runs in serialized threading mode (see
 * threadingMode()) -- reads run in parallel, writes on one handle queue
 * behind each other, and schema/index changes wait for in-flight calls (see
 * zig/CAPI.md "Thread Safety"). All handle operations are async and run on
 * koffi's worker thread pool so they never block the event loop; see the
 * package README for a note on koffi's threadpool size.
 *
 * close() waits for every call this binding has dispatched (queued or
 * in-flight) to finish before freeing the native handle, and rejects new
 * calls immediately once close() has started. Node's own async FFI queueing
 * means a call can be queued but not yet "entered" the C library when
 * close() runs; libantfly's own close only drains calls that already entered
 * it, so this extra bookkeeping is required to avoid a use-after-free.
 */
export class Database implements AsyncDisposable {
  #native: NativeLibrary;
  #handle: unknown;
  #state: "open" | "closing" | "closed" = "open";
  #pending = new Set<Promise<unknown>>();
  #closeTask: Promise<void> | undefined;
  #finalizerToken: object = {};

  /** @internal use open()/create()/openHosted()/etc. instead of the constructor. */
  constructor(native: NativeLibrary, handle: unknown) {
    this.#native = native;
    this.#handle = handle;
    finalizationRegistry.register(this, { native, handle }, this.#finalizerToken);
  }

  #run<T>(op: (handle: unknown) => Promise<T>): Promise<T> {
    if (this.#state !== "open") {
      return Promise.reject(new InvalidArgumentError("database handle is closed"));
    }
    const handle = this.#handle;
    const promise = op(handle);
    this.#pending.add(promise);
    const cleanup = () => this.#pending.delete(promise);
    promise.then(cleanup, cleanup);
    return promise;
  }

  // biome-ignore lint/suspicious/noExplicitAny: koffi function signatures vary per call site
  #invokeCode(fn: any, ...args: unknown[]): Promise<void> {
    return this.#run(async (handle) => {
      const code = await callAsync(fn, handle, ...args);
      checkCode(code);
    });
  }

  // biome-ignore lint/suspicious/noExplicitAny: koffi function signatures vary per call site
  #invokeBuffer(fn: any, ...args: unknown[]): Promise<Buffer> {
    return this.#run(async (handle) => {
      const out: DecodedBuffer = newBufferOut();
      const code = await callAsync(fn, handle, ...args, out);
      checkCode(code);
      return takeBuffer(this.#native, out);
    });
  }

  // --- Lifecycle ---

  /** Idempotent; waits for in-flight calls before freeing the native handle. Calls made after close() rejects with InvalidArgumentError. */
  async close(): Promise<void> {
    if (this.#state === "closed") return;
    if (this.#state === "closing") {
      await this.#closeTask;
      return;
    }
    this.#state = "closing";
    finalizationRegistry.unregister(this.#finalizerToken);
    const handle = this.#handle;
    this.#closeTask = (async () => {
      while (this.#pending.size > 0) {
        await Promise.allSettled([...this.#pending]);
      }
      if (handle != null) {
        await callAsync(this.#native.dbClose, handle);
      }
      this.#handle = null;
      this.#state = "closed";
    })();
    await this.#closeTask;
  }

  async [Symbol.asyncDispose](): Promise<void> {
    await this.close();
  }

  // --- Status / capabilities / maintenance ---

  statusRaw(): Promise<Buffer> {
    return this.#invokeBuffer(this.#native.dbStatusJson);
  }
  async status(): Promise<Status> {
    return parseJson(await this.statusRaw()) as Status;
  }

  capabilitiesRaw(): Promise<Buffer> {
    return this.#invokeBuffer(this.#native.dbCapabilitiesJson);
  }
  async capabilities(): Promise<Capabilities> {
    return parseJson(await this.capabilitiesRaw()) as Capabilities;
  }

  checkRaw(): Promise<Buffer> {
    return this.#invokeBuffer(this.#native.liteCheckJson);
  }
  async check(): Promise<CheckReport> {
    return parseJson(await this.checkRaw()) as CheckReport;
  }

  vacuumRaw(): Promise<Buffer> {
    return this.#invokeBuffer(this.#native.liteVacuumJson);
  }
  async vacuum(): Promise<VacuumReport> {
    return parseJson(await this.vacuumRaw()) as VacuumReport;
  }

  compactRaw(): Promise<Buffer> {
    return this.#invokeBuffer(this.#native.liteCompactJson);
  }
  async compact(): Promise<CompactReport> {
    return parseJson(await this.compactRaw()) as CompactReport;
  }

  copyStableSnapshotRaw(destPath: string, replace: boolean): Promise<Buffer> {
    if (!destPath.endsWith(".aflite")) {
      return Promise.reject(new InvalidArgumentError("destPath must end with .aflite"));
    }
    return this.#invokeBuffer(this.#native.liteCopyStableSnapshotJson, destPath, replace);
  }
  async copyStableSnapshot(destPath: string, replace: boolean): Promise<StableSnapshotReport> {
    return parseJson(await this.copyStableSnapshotRaw(destPath, replace)) as StableSnapshotReport;
  }

  pendingWorkStatsRaw(): Promise<Buffer> {
    return this.#invokeBuffer(this.#native.dbPendingWorkStatsJson);
  }
  async pendingWorkStats(): Promise<PendingWorkStatus> {
    return parseJson(await this.pendingWorkStatsRaw()) as PendingWorkStatus;
  }

  /** Drains pending enrichment and index work; returns no result (see runUntilIdleStatus for the post-drain report). */
  runUntilIdle(): Promise<void> {
    return this.#invokeCode(this.#native.dbRunUntilIdle);
  }
  runUntilIdleStatusRaw(): Promise<Buffer> {
    return this.#invokeBuffer(this.#native.dbRunUntilIdleJson);
  }
  async runUntilIdleStatus(): Promise<PendingWorkStatus> {
    return parseJson(await this.runUntilIdleStatusRaw()) as PendingWorkStatus;
  }

  replayGeneratedEnrichmentsRaw(): Promise<Buffer> {
    return this.#invokeBuffer(this.#native.dbReplayGeneratedEnrichmentsJson);
  }
  async replayGeneratedEnrichments(): Promise<ReplayGeneratedEnrichmentsResult> {
    return parseJson(
      await this.replayGeneratedEnrichmentsRaw()
    ) as ReplayGeneratedEnrichmentsResult;
  }

  // --- Backup / import ---

  /** Returns a portable Antfly backup archive (.afb) of this database, which restores or imports into either storage kind. */
  backup(): Promise<Buffer> {
    return this.#invokeBuffer(this.#native.dbBackup);
  }

  /** Imports a portable Antfly backup archive into this empty database. OutcomeUnknown means the live handle adopted the imported generation, but crash durability could not be confirmed; inspect the handle and do not retry automatically. */
  importBackup(backup: Uint8Array): Promise<void> {
    return this.#invokeCode(this.#native.dbImportBackup, sliceOf(toRawBytes(backup)));
  }

  async backupToFile(path: string): Promise<void> {
    if (!path.endsWith(".afb")) throw new InvalidArgumentError("path must end with .afb");
    await writeFileAtomically(path, await this.backup());
  }

  // --- Documents ---

  batch(writes: WriteIntent[], timestampNs: Uint64Like): Promise<void> {
    return this.#run(async (handle) => {
      const cWrites = writes.length ? writes.map(toNativeWriteIntent) : null;
      const code = await callAsync(
        this.#native.dbBatch,
        handle,
        cWrites,
        writes.length,
        null,
        0,
        timestampNs,
        0
      );
      checkCode(code);
    });
  }

  batchJsonRaw(request: JsonInput): Promise<Buffer> {
    return this.#invokeBuffer(this.#native.dbBatchJson, jsonSlice(request));
  }
  async batchJson(request: JsonInput): Promise<unknown> {
    return parseJson(await this.batchJsonRaw(request));
  }

  lookupRaw(key: string | Uint8Array): Promise<Buffer> {
    return this.#invokeBuffer(this.#native.dbLookupJson, keySlice(key));
  }
  async lookup(key: string | Uint8Array): Promise<unknown> {
    return parseJson(await this.lookupRaw(key));
  }

  /** Raw stored bytes for key (antfly_db_get_raw), not JSON. */
  getRaw(key: string | Uint8Array): Promise<Buffer> {
    return this.#invokeBuffer(this.#native.dbGetRaw, keySlice(key));
  }

  getSchemaRaw(): Promise<Buffer> {
    return this.#invokeBuffer(this.#native.dbGetSchemaJson);
  }
  async getSchema(): Promise<unknown> {
    return parseJson(await this.getSchemaRaw());
  }
  setSchema(schema: JsonInput): Promise<void> {
    return this.#invokeCode(this.#native.dbSetSchemaJson, jsonSlice(schema));
  }

  listIndexesRaw(): Promise<Buffer> {
    return this.#invokeBuffer(this.#native.dbListIndexesJson);
  }
  async listIndexes(): Promise<unknown> {
    return parseJson(await this.listIndexesRaw());
  }
  addIndex(config: JsonInput): Promise<void> {
    return this.#invokeCode(this.#native.dbAddIndexJson, jsonSlice(config));
  }
  deleteIndex(name: string): Promise<boolean> {
    return this.#run(async (handle) => {
      const out = [false];
      const code = await callAsync(this.#native.dbDeleteIndex, handle, stringSlice(name), out);
      checkCode(code);
      return Boolean(out[0]);
    });
  }

  listEnrichmentsRaw(): Promise<Buffer> {
    return this.#invokeBuffer(this.#native.dbListEnrichmentsJson);
  }
  async listEnrichments(): Promise<unknown> {
    return parseJson(await this.listEnrichmentsRaw());
  }
  addEnrichment(config: JsonInput): Promise<void> {
    return this.#invokeCode(this.#native.dbAddEnrichmentJson, jsonSlice(config));
  }
  deleteEnrichment(kind: string, name: string): Promise<boolean> {
    return this.#run(async (handle) => {
      const out = [false];
      const code = await callAsync(
        this.#native.dbDeleteEnrichment,
        handle,
        stringSlice(kind),
        stringSlice(name),
        out
      );
      checkCode(code);
      return Boolean(out[0]);
    });
  }

  scanRaw(request: JsonInput): Promise<Buffer> {
    return this.#invokeBuffer(this.#native.dbScanJson, jsonSlice(request));
  }
  async scan(request: JsonInput): Promise<unknown> {
    return parseJson(await this.scanRaw(request));
  }

  statsRaw(): Promise<Buffer> {
    return this.#invokeBuffer(this.#native.dbStatsJson);
  }
  async stats(): Promise<unknown> {
    return parseJson(await this.statsRaw());
  }

  searchRaw(request: JsonInput): Promise<Buffer> {
    return this.#invokeBuffer(this.#native.dbSearchJson, jsonSlice(request));
  }
  async search(request: JsonInput): Promise<unknown> {
    return parseJson(await this.searchRaw(request));
  }

  // --- Packed wire searches (binary wire format, never JSON: request/response are raw bytes) ---

  denseSearchWire(request: Uint8Array): Promise<Buffer> {
    return this.#invokeBuffer(this.#native.dbSearchDenseWire, sliceOf(toRawBytes(request)));
  }
  textMatchWire(request: Uint8Array): Promise<Buffer> {
    return this.#invokeBuffer(this.#native.dbSearchTextMatchWire, sliceOf(toRawBytes(request)));
  }
  textTermWire(request: Uint8Array): Promise<Buffer> {
    return this.#invokeBuffer(this.#native.dbSearchTextTermWire, sliceOf(toRawBytes(request)));
  }
  textMatchPhraseWire(request: Uint8Array): Promise<Buffer> {
    return this.#invokeBuffer(
      this.#native.dbSearchTextMatchPhraseWire,
      sliceOf(toRawBytes(request))
    );
  }

  aggregateHitsRaw(request: JsonInput): Promise<Buffer> {
    return this.#invokeBuffer(this.#native.dbAggregateHitsJson, jsonSlice(request));
  }
  async aggregateHits(request: JsonInput): Promise<unknown> {
    return parseJson(await this.aggregateHitsRaw(request));
  }

  lookupArtifactRaw(artifactIdBase64: string): Promise<Buffer> {
    return this.#invokeBuffer(this.#native.dbLookupArtifactJson, stringSlice(artifactIdBase64));
  }
  async lookupArtifact(artifactIdBase64: string): Promise<unknown> {
    return parseJson(await this.lookupArtifactRaw(artifactIdBase64));
  }

  extractEnrichmentsRaw(request: JsonInput): Promise<Buffer> {
    return this.#invokeBuffer(this.#native.dbExtractEnrichmentsJson, jsonSlice(request));
  }
  async extractEnrichments(request: JsonInput): Promise<unknown> {
    return parseJson(await this.extractEnrichmentsRaw(request));
  }

  computeEnrichmentsRaw(request: JsonInput): Promise<Buffer> {
    return this.#invokeBuffer(this.#native.dbComputeEnrichmentsJson, jsonSlice(request));
  }
  async computeEnrichments(request: JsonInput): Promise<unknown> {
    return parseJson(await this.computeEnrichmentsRaw(request));
  }

  // --- Graph ---

  edgesRaw(indexName: string, key: string, edgeType: string, direction: number): Promise<Buffer> {
    return this.#invokeBuffer(
      this.#native.dbGetEdgesJson,
      stringSlice(indexName),
      stringSlice(key),
      stringSlice(edgeType),
      direction
    );
  }
  async edges(
    indexName: string,
    key: string,
    edgeType: string,
    direction: number
  ): Promise<unknown> {
    return parseJson(await this.edgesRaw(indexName, key, edgeType, direction));
  }

  neighborsRaw(
    indexName: string,
    key: string,
    edgeType: string,
    direction: number
  ): Promise<Buffer> {
    return this.#invokeBuffer(
      this.#native.dbGetNeighborsJson,
      stringSlice(indexName),
      stringSlice(key),
      stringSlice(edgeType),
      direction
    );
  }
  async neighbors(
    indexName: string,
    key: string,
    edgeType: string,
    direction: number
  ): Promise<unknown> {
    return parseJson(await this.neighborsRaw(indexName, key, edgeType, direction));
  }

  traverseEdgesRaw(request: JsonInput): Promise<Buffer> {
    return this.#invokeBuffer(this.#native.dbTraverseEdgesJson, jsonSlice(request));
  }
  async traverseEdges(request: JsonInput): Promise<unknown> {
    return parseJson(await this.traverseEdgesRaw(request));
  }

  executeGraphQueriesRaw(request: JsonInput): Promise<Buffer> {
    return this.#invokeBuffer(this.#native.dbExecuteGraphQueriesJson, jsonSlice(request));
  }
  async executeGraphQueries(request: JsonInput): Promise<unknown> {
    return parseJson(await this.executeGraphQueriesRaw(request));
  }

  findShortestPathRaw(request: JsonInput): Promise<Buffer> {
    return this.#invokeBuffer(this.#native.dbFindShortestPathJson, jsonSlice(request));
  }
  async findShortestPath(request: JsonInput): Promise<unknown> {
    return parseJson(await this.findShortestPathRaw(request));
  }

  findKShortestPathsRaw(request: JsonInput): Promise<Buffer> {
    return this.#invokeBuffer(this.#native.dbFindKShortestPathsJson, jsonSlice(request));
  }
  async findKShortestPaths(request: JsonInput): Promise<unknown> {
    return parseJson(await this.findKShortestPathsRaw(request));
  }

  matchPatternRaw(request: JsonInput): Promise<Buffer> {
    return this.#invokeBuffer(this.#native.dbMatchPatternJson, jsonSlice(request));
  }
  async matchPattern(request: JsonInput): Promise<unknown> {
    return parseJson(await this.matchPatternRaw(request));
  }

  // --- Transactions ---

  beginTransaction(
    txnId: Uint8Array | string,
    timestampNs: Uint64Like,
    participants: string[] = []
  ): Promise<void> {
    return this.#run(async (handle) => {
      const id = txnIdBytes(txnId);
      const cParticipants = participants.length ? participants.map(stringSlice) : null;
      const code = await callAsync(
        this.#native.dbBeginTransactionWithId,
        handle,
        id,
        timestampNs,
        cParticipants,
        participants.length
      );
      checkCode(code);
    });
  }

  writeTransaction(txnId: Uint8Array | string, writes: WriteIntent[]): Promise<void> {
    return this.#run(async (handle) => {
      const id = txnIdBytes(txnId);
      const cWrites = writes.length ? writes.map(toNativeWriteIntent) : null;
      const code = await callAsync(
        this.#native.dbWriteTransaction,
        handle,
        id,
        cWrites,
        writes.length,
        null,
        0
      );
      checkCode(code);
    });
  }

  resolveTransaction(
    txnId: Uint8Array | string,
    status: TxnStatus,
    commitVersion: Uint64Like = 0
  ): Promise<void> {
    return this.#invokeCode(
      this.#native.dbResolveIntents,
      txnIdBytes(txnId),
      status,
      commitVersion
    );
  }

  transactionStatus(txnId: Uint8Array | string): Promise<TxnStatus> {
    return this.#run(async (handle) => {
      const id = txnIdBytes(txnId);
      const out = [0];
      const code = await callAsync(this.#native.dbGetTransactionStatus, handle, id, out);
      checkCode(code);
      return out[0] as TxnStatus;
    });
  }

  commitVersion(txnId: Uint8Array | string): Promise<bigint> {
    return this.#run(async (handle) => {
      const id = txnIdBytes(txnId);
      const out: Array<number | bigint> = [0];
      const code = await callAsync(this.#native.dbGetCommitVersion, handle, id, out);
      checkCode(code);
      return BigInt(out[0] ?? 0);
    });
  }
}

const finalizationRegistry = new FinalizationRegistry<{ native: NativeLibrary; handle: unknown }>(
  ({ native, handle }) => {
    if (handle != null) {
      try {
        native.dbClose(handle);
      } catch {
        // Best-effort only: the process may be tearing down.
      }
    }
  }
);

async function writeFileAtomically(path: string, data: Buffer, mode = 0o600): Promise<void> {
  const dir = dirname(path);
  const tmpPath = join(
    dir,
    `.${basename(path)}.${process.pid}.${Date.now()}.${Math.random().toString(36).slice(2)}.tmp`
  );
  const handle = await fsp.open(tmpPath, "w", mode);
  try {
    await handle.writeFile(data);
    await handle.sync();
  } finally {
    await handle.close();
  }
  await fsp.rename(tmpPath, path);
}

async function openOrCreate(path: string, opts: OpenOptions, create: boolean): Promise<Database> {
  validateAbi();
  const native = loadNative();
  const options = buildOpenOptionsStruct(native, opts);
  const outHandle: unknown[] = [null];
  const fn = create ? native.dbCreateWithOptions : native.dbOpenWithOptions;
  const code = await callAsync(fn, path, options, outHandle);
  checkCode(code);
  return new Database(native, outHandle[0]);
}

/** Opens an existing native Antfly Lite database file for writing. */
export function open(path: string): Promise<Database> {
  return openWithOptions(path, { mode: OpenMode.Writer, profile: Profile.Native });
}

/** Creates a new native Antfly Lite database file for writing. */
export function create(path: string): Promise<Database> {
  return createWithOptions(path, { mode: OpenMode.Writer, profile: Profile.Native });
}

/** Opens an existing Antfly Lite database file read-only. */
export function openReadonly(path: string): Promise<Database> {
  return openWithOptions(path, { mode: OpenMode.Readonly, profile: Profile.Native });
}

/** Opens enough of an Antfly Lite database to read status. */
export function openStatusOnly(path: string): Promise<Database> {
  return openWithOptions(path, { mode: OpenMode.StatusOnly, profile: Profile.Native });
}

/** Opens an Antfly Lite database using explicit C ABI options. */
export function openWithOptions(path: string, options: OpenOptions = {}): Promise<Database> {
  return openOrCreate(path, options, false);
}

/** Creates an Antfly Lite database using explicit C ABI options. */
export function createWithOptions(path: string, options: OpenOptions = {}): Promise<Database> {
  return openOrCreate(path, options, true);
}

/**
 * Opens an existing Antfly Lite database in hosted/manual maintenance mode.
 * In this profile callers drive pending work explicitly with runUntilIdle.
 */
export async function openHosted(path: string): Promise<Database> {
  validateAbi();
  const native = loadNative();
  const outHandle: unknown[] = [null];
  const code = await callAsync(native.liteOpenHosted, path, outHandle);
  checkCode(code);
  return new Database(native, outHandle[0]);
}

/**
 * Creates a new Antfly Lite database in hosted/manual maintenance mode. In
 * this profile callers drive pending work explicitly with runUntilIdle.
 */
export async function createHosted(path: string): Promise<Database> {
  validateAbi();
  const native = loadNative();
  const outHandle: unknown[] = [null];
  const code = await callAsync(native.liteCreateHosted, path, outHandle);
  checkCode(code);
  return new Database(native, outHandle[0]);
}
