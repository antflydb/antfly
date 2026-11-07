import { createHash, randomUUID } from "node:crypto";
import { mkdir, rename, rm, writeFile } from "node:fs/promises";
import { join, posix } from "node:path";
import type { AntflyClient } from "@antfly/sdk";
import type { SourceSyncJob, StandaloneSourcesClient } from "@antfly/sdk/sources";
import localMediaTypes from "../../../../../go/pkg/sources/local_media_types.json" with {
  type: "json",
};
import { abortable, combinedSignal } from "../abort.js";
import { type AntflyAssetReference, encodeAssetReference } from "./reference.js";

/**
 * `published`: the sync job that ingests the uploaded bytes has committed a
 * catalog row for them (`uploadFile`'s default). `searchable`: additionally
 * wait for the row to turn up in a search query against the same table.
 *
 * antfly#1505 is explicit that "published doesn't mean searchable": sync
 * commits the catalog row and its `content.resource` immediately, but
 * enrichment (OCR, chunking, embeddings) that makes a row *findable* by
 * search runs independently and has no durable completion signal this
 * helper can poll. `searchable` here is therefore a best-effort client-side
 * poll against a search query, not a server-guaranteed state -- document
 * that to callers rather than implying it is as solid as `published`.
 */
export type AssetPublicationState = "published" | "searchable";

export interface UploadFileOptions {
  /** Database client, used to confirm (and, for `searchable`, search for) the resulting catalog row. */
  client: AntflyClient;
  /** Source companion client, used to trigger and poll the ingesting run. */
  sources: StandaloneSourcesClient;
  /** The registered source to upload under. Must have `publication: "media"` and a `local` connection. */
  sourceId: string;
  /**
   * Filesystem root of that source's `local` connection (its `local_root`).
   * This PR only writes new objects for the `local` antfly-sources provider
   * (antfly#1505's decision for local/embedded deployments); a bucket-backed
   * connection (gcs/s3) needs the equivalent provider-specific PUT, which is
   * not implemented here -- see the package README for this gap.
   */
  localRoot: string;
  /** Must match the source's `config.prefix`, if it has one. */
  prefix?: string;
  /** Catalog table the source publishes into (the source's `table`). */
  table: string;
  data: Uint8Array;
  mimeType: string;
  waitFor?: AssetPublicationState;
  /** Bounded retries when sync returns 409 (another source's run is active). Default 5. */
  maxSyncConflictRetries?: number;
  /** Delay between 409 retries. Default 2000ms. */
  syncConflictRetryDelayMs?: number;
  /** Overall deadline, including requests, file writing, sync and publication polling. Default 120000ms. */
  timeoutMs?: number;
  pollIntervalMs?: number;
  signal?: AbortSignal;
}

export interface UploadFileResult {
  reference: AntflyAssetReference;
  /** base64url `providerReference.antfly` id for this asset; `assetProviderReference(result.reference)` also builds this. */
  providerReferenceId: string;
  digest: string;
  state: AssetPublicationState;
}

const MIME_EXTENSIONS: Readonly<Record<string, string>> = localMediaTypes;
const MIME_ALIASES: Readonly<Record<string, string>> = {
  "audio/x-wav": "audio/wav",
  "audio/x-flac": "audio/flac",
  "image/jpg": "image/jpeg",
};

export class SourceSyncConflictError extends Error {
  constructor(readonly sourceId: string) {
    super(`Source ${sourceId} sync stayed busy after retrying; another run is still active`);
    this.name = "SourceSyncConflictError";
  }
}

export class SourceSyncFailedError extends Error {
  constructor(readonly job: SourceSyncJob) {
    super(`Source sync ${job.id} ended in state '${job.state}'${job.code ? ` (${job.code})` : ""}`);
    this.name = "SourceSyncFailedError";
  }
}

export class AssetUploadTimeoutError extends Error {
  constructor(readonly state: AssetPublicationState) {
    super(`Timed out waiting for the uploaded asset to become '${state}'`);
    this.name = "AssetUploadTimeoutError";
  }
}

/**
 * Writes `data` under the source's local content-addressed store, triggers
 * a per-source sync run (`source_id` on the companion's `/v1/sync`, which
 * `@antfly/sdk/sources`'s `StandaloneSourcesClient.sync()` now exposes), and
 * waits for the requested publication state (default `published`). A
 * concurrent run on the companion (sync is serialized service-wide) is
 * retried a bounded number of times before failing with
 * `SourceSyncConflictError` -- antfly#1505 asks for a defined outcome here
 * rather than an unhandled 409.
 *
 * `FilesV4` (the AI SDK's generic multi-provider files interface) has no way
 * to register a byte-identical object that already exists at the target
 * provider; Antfly is not registered as an AI SDK files provider at all for
 * this reason. Existing Antfly-owned assets are made visible to models only
 * through source registration and sync, which is what this function does
 * for a newly-uploaded object.
 */
export async function uploadFile(options: UploadFileOptions): Promise<UploadFileResult> {
  const timeoutMs = options.timeoutMs ?? 120_000;
  if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 2_147_483_647) {
    throw new RangeError("timeoutMs must be an integer between 1 and 2147483647");
  }
  const cancellation = new AbortController();
  const signal = combinedSignal(options.signal, cancellation.signal) ?? cancellation.signal;
  let state: AssetPublicationState = "published";
  const deadline = Date.now() + timeoutMs;
  const check = () => {
    signal.throwIfAborted();
    if (Date.now() >= deadline) {
      cancellation.abort(new AssetUploadTimeoutError(state));
      throw signal.reason;
    }
  };
  const timer = setTimeout(() => cancellation.abort(new AssetUploadTimeoutError(state)), timeoutMs);
  try {
    check();
    return await abortable(
      uploadWithinDeadline({ ...options, signal }, check, () => {
        state = "searchable";
      }),
      signal
    );
  } finally {
    clearTimeout(timer);
  }
}

async function uploadWithinDeadline(
  options: UploadFileOptions,
  check: () => void,
  searching: () => void
): Promise<UploadFileResult> {
  const suppliedMime = options.mimeType.split(";", 1)[0].trim().toLowerCase();
  const mimeType = Object.hasOwn(MIME_ALIASES, suppliedMime)
    ? MIME_ALIASES[suppliedMime]
    : suppliedMime;
  if (!Object.hasOwn(MIME_EXTENSIONS, mimeType))
    throw new Error(`Unsupported local upload MIME type: ${mimeType}`);
  if (options.data.byteLength === 0)
    throw new RangeError("Local media uploads must contain at least one byte");
  if (options.data.byteLength > 256 * 1024 * 1024)
    throw new RangeError("Local uploads may not exceed 256 MiB");
  const profile = await options.sources.profile(options.signal);
  check();
  const source = profile.sources.find((source) => source.id === options.sourceId);
  if (
    !source ||
    source.table !== options.table ||
    source.publication !== "media" ||
    profile.connections[source.connection]?.provider !== "local"
  ) {
    throw new Error("Upload requires a registered local media source in the specified table");
  }
  const prefix = source.config.prefix ?? "";
  if (
    typeof prefix !== "string" ||
    prefix !== (options.prefix ?? "") ||
    (prefix !== "" &&
      (prefix.startsWith("/") ||
        prefix.includes("\\") ||
        prefix.includes("\0") ||
        posix.normalize(prefix) !== prefix ||
        prefix === ".." ||
        prefix.startsWith("../")))
  ) {
    throw new Error("Upload prefix must match the source's local prefix");
  }
  const digest = createHash("sha256").update(options.data).digest("hex");
  const ext = MIME_EXTENSIONS[mimeType];
  const objectsDir = join(options.localRoot, options.prefix ?? "", "objects", "sha256");
  const filename = `${digest}.${ext}`;
  const relativeKey = `objects/sha256/${filename}`;
  const identity = source.identity?.namespace
    ? `${source.identity.namespace}/${relativeKey}`
    : `local://job/${options.sourceId}/${posix.join(prefix, relativeKey)}`;
  const keyDigest = createHash("sha256").update(identity).digest("hex");
  const reference = {
    table: options.table,
    key: `source://job/${options.sourceId}/records/${keyDigest}`,
  };
  const finalPath = join(objectsDir, filename);
  check();
  await mkdir(objectsDir, { recursive: true });
  check();
  const tempPath = join(objectsDir, `.upload-${randomUUID()}`);
  try {
    await writeFile(tempPath, options.data, { signal: options.signal });
    check();
    await rename(tempPath, finalPath);
  } finally {
    await rm(tempPath, { force: true });
  }
  check();
  const job = await runSourceSync(options, check);
  if (job.state !== "completed") throw new SourceSyncFailedError(job);

  const pollIntervalMs = options.pollIntervalMs ?? 500;
  await pollForCatalogRow(options, reference, relativeKey, digest, check, pollIntervalMs);
  if ((options.waitFor ?? "published") === "published") {
    return {
      reference,
      providerReferenceId: encodeAssetReference(reference),
      digest,
      state: "published",
    };
  }
  searching();
  await pollForSearchable(options, reference, digest, check, pollIntervalMs);
  return {
    reference,
    providerReferenceId: encodeAssetReference(reference),
    digest,
    state: "searchable",
  };
}

async function runSourceSync(
  options: UploadFileOptions,
  check: () => void
): Promise<SourceSyncJob> {
  const maxRetries = options.maxSyncConflictRetries ?? 5;
  const retryDelayMs = options.syncConflictRetryDelayMs ?? 2000;
  let job: SourceSyncJob | undefined;
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    check();
    try {
      job = await options.sources.sync({ sourceId: options.sourceId, signal: options.signal });
      break;
    } catch (error) {
      const status = (error as { status?: number } | undefined)?.status;
      if (status !== 409 || attempt === maxRetries) {
        if (status === 409) throw new SourceSyncConflictError(options.sourceId);
        throw error;
      }
      await delay(retryDelayMs, options.signal);
    }
  }
  if (!job) throw new SourceSyncConflictError(options.sourceId);
  while (job.state === "running") {
    check();
    await delay(options.pollIntervalMs ?? 500, options.signal);
    job = await options.sources.job(job.id, options.signal);
  }
  check();
  return job;
}

/** Direct identity lookup is independent of table size and search indexing. */
async function pollForCatalogRow(
  options: UploadFileOptions,
  reference: AntflyAssetReference,
  relativeKey: string,
  digest: string,
  check: () => void,
  pollIntervalMs: number
): Promise<void> {
  while (true) {
    check();
    const { data, response } = await options.client
      .getRawClient()
      .GET("/db/v1/tables/{tableName}/documents/{key}", {
        params: {
          path: { tableName: reference.table, key: reference.key },
          query: { consistency: "read_index" },
        },
        signal: options.signal,
      });
    check();
    if (response.ok) {
      const row = data as {
        source?: { id?: string; relative_key?: string };
        metadata?: { sha256?: string };
        content?: { resource?: { type?: string; version?: string } };
      };
      if (
        row?.source?.id === options.sourceId &&
        row.source.relative_key === relativeKey &&
        row.metadata?.sha256 === digest &&
        row.content?.resource?.type === "source" &&
        row.content.resource.version === `"${digest}"`
      )
        return;
    } else if (response.status !== 404) {
      throw new Error(`Upload catalog lookup failed: HTTP ${response.status}`);
    }
    check();
    await delay(pollIntervalMs, options.signal);
  }
}

async function pollForSearchable(
  options: UploadFileOptions,
  reference: AntflyAssetReference,
  digest: string,
  check: () => void,
  pollIntervalMs: number
): Promise<void> {
  while (true) {
    check();
    const response = await options.client.tables.query(
      options.table,
      {
        full_text_search: { query: digest },
        filter_query: { ids: [reference.key] },
        limit: 1,
      },
      { signal: options.signal }
    );
    check();
    const envelope = response as { responses?: { hits?: { hits?: unknown[] } }[] };
    if ((envelope.responses?.[0]?.hits?.hits?.length ?? 0) > 0) return;
    check();
    await delay(pollIntervalMs, options.signal);
  }
}

function delay(ms: number, signal: AbortSignal | undefined): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) return reject(signal.reason);
    const onAbort = () => {
      clearTimeout(timer);
      reject(signal?.reason);
    };
    const timer = setTimeout(() => {
      signal?.removeEventListener("abort", onAbort);
      resolve();
    }, ms);
    signal?.addEventListener("abort", onAbort, { once: true });
  });
}
