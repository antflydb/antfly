import type {
  FilesV4,
  LanguageModelV4,
  LanguageModelV4FilePart,
  LanguageModelV4Message,
  ProviderV4,
} from "@ai-sdk/provider";
import type { AntflyClient } from "@antfly/sdk";
import { type LanguageModelMiddleware, uploadFile, wrapLanguageModel } from "ai";
import { abortable, combinedSignal } from "../abort.js";
import { type CatalogResource, resolveAuthorizedAsset } from "./catalog.js";
import { type AuthorizeAsset, antflyAssetReferenceFrom } from "./reference.js";

export interface AssetUploadCacheEntry {
  reference: Record<string, string>;
  expiresAt?: Date;
}

/** Caches an uploaded vendor file by content digest and target provider. */
export interface AssetUploadCache {
  get(
    digest: string,
    provider: string
  ): AssetUploadCacheEntry | undefined | Promise<AssetUploadCacheEntry | undefined>;
  set(digest: string, provider: string, entry: AssetUploadCacheEntry): void | Promise<void>;
}

/** A bounded LRU cache. Scope a shared cache to one vendor account. */
export function createInMemoryAssetUploadCache(maxEntries = 1024): AssetUploadCache {
  if (!Number.isSafeInteger(maxEntries) || maxEntries < 1)
    throw new RangeError("maxEntries must be a positive integer");
  const entries = new Map<string, AssetUploadCacheEntry>();
  const cacheKey = (digest: string, provider: string) => `${provider}\u0000${digest}`;
  return {
    get: (digest, provider) => {
      const key = cacheKey(digest, provider);
      const entry = entries.get(key);
      if (!entry) return undefined;
      entries.delete(key);
      if (entry.expiresAt && entry.expiresAt.getTime() <= Date.now()) return undefined;
      entries.set(key, entry);
      return entry;
    },
    set: (digest, provider, entry) => {
      const key = cacheKey(digest, provider);
      entries.delete(key);
      entries.set(key, entry);
      while (entries.size > maxEntries) {
        const oldest = entries.keys().next();
        if (oldest.done) break;
        entries.delete(oldest.value);
      }
    },
  };
}

type UploadFlight = {
  controller: AbortController;
  promise: Promise<AssetUploadCacheEntry>;
  waiters: number;
  settled: boolean;
};
// Sharing a cache also shares in-flight work, even across middleware instances.
// Each reference is authorized before joining; each waiter keeps its own signal.
const uploadFlights = new WeakMap<AssetUploadCache, Map<string, UploadFlight>>();

async function uploadedOnce(
  cache: AssetUploadCache,
  key: string,
  signal: AbortSignal | undefined,
  upload: (signal: AbortSignal) => Promise<AssetUploadCacheEntry>
): Promise<AssetUploadCacheEntry> {
  signal?.throwIfAborted();
  const flights = uploadFlights.get(cache) ?? new Map<string, UploadFlight>();
  uploadFlights.set(cache, flights);
  let flight = flights.get(key);
  if (!flight) {
    const controller = new AbortController();
    const promise = Promise.resolve().then(() => upload(controller.signal));
    const created: UploadFlight = { controller, waiters: 0, settled: false, promise };
    created.promise = promise.finally(() => {
      created.settled = true;
      if (flights.get(key) === created) flights.delete(key);
    });
    flights.set(key, created);
    flight = created;
  }
  flight.waiters++;
  try {
    return await abortable(flight.promise, signal);
  } finally {
    flight.waiters--;
    if (flight.waiters === 0 && !flight.settled) {
      flight.controller.abort();
      if (flights.get(key) === flight) flights.delete(key);
    }
  }
}

export interface AssetMiddlewareOptions {
  client: AntflyClient;
  /** The app's decision on each referenced asset, bound to the current user. Required. */
  authorize: AuthorizeAsset;
  /**
   * Upload the asset to the external provider's own files API, once per
   * provider and content digest. Default `false`: vendor uploads are
   * opt-in, because revoking access in Antfly does not remove a vendor's
   * copy (antfly#1505). When `false` (or the provider has no files API),
   * the asset is always fetched and sent inline instead.
   */
  allowVendorUpload?: boolean;
  /** The provider's files API, required when `allowVendorUpload` is true. */
  files?: FilesV4 | ProviderV4;
  /** Shared across calls for one vendor account. Defaults to a bounded in-memory cache. */
  cache?: AssetUploadCache;
  /** Defaults to `globalThis.fetch`. */
  fetch?: typeof fetch;
  /** Abort signal for the server-side fetch of the asset's bytes. */
  abortSignal?: AbortSignal;
}

/** Thrown when fetching an asset's pinned bytes fails. Never includes the capability URL. */
export class AssetFetchError extends Error {
  constructor(
    readonly status: number,
    readonly reason: "version-mismatch" | "fetch-failed"
  ) {
    super(
      reason === "version-mismatch"
        ? "Asset fetch failed: the pinned version no longer matches (If-Match failed)."
        : `Asset fetch failed with HTTP ${status}.`
    );
    this.name = "AssetFetchError";
  }
}

async function fetchResourceBytes(
  resource: CatalogResource,
  fetchFn: typeof fetch,
  signal: AbortSignal | undefined
): Promise<Uint8Array> {
  // `resource.uri` is a bearer capability URL (antfly#1505: "treat source
  // media URLs as secrets in logs and errors too"); it is used exactly
  // once, here, server-side, and must never appear in a thrown message,
  // a prompt, or a tool result.
  const response = await fetchFn(resource.uri, {
    headers: { "If-Match": resource.version },
    signal,
  });
  if (response.status === 412 || response.status === 409) {
    await response.body?.cancel();
    throw new AssetFetchError(response.status, "version-mismatch");
  }
  if (!response.ok) {
    await response.body?.cancel();
    throw new AssetFetchError(response.status, "fetch-failed");
  }
  return new Uint8Array(await response.arrayBuffer());
}

/**
 * Rewrites `providerReference: { antfly: <id> }` file parts before an
 * external (non-Antfly) model call, per antfly#1505's proposed asset flow:
 * upload once per provider and cache it by digest (opt-in, respecting
 * `expiresAt`), or fetch the pinned bytes with `If-Match` and send them
 * inline. Calls to Antfly's own provider need no rewrite: `AntflyChatLanguageModel`
 * resolves the same reference natively into the typed `source` content part.
 *
 * `resolveProviderReference` (`@ai-sdk/provider-utils`) throws
 * `NoSuchProviderReferenceError` when a provider looks up its own name in a
 * reference map and finds nothing there. This middleware always leaves the
 * wrapped provider's own entry present (by uploading and adding it, or by
 * removing `type: 'reference'` entirely in favor of inline bytes), so that
 * lookup never fails for a part this middleware has touched.
 */
export function createAssetMiddleware(options: AssetMiddlewareOptions): LanguageModelMiddleware {
  const fetchFn = options.fetch ?? globalThis.fetch;
  const cache = options.cache ?? createInMemoryAssetUploadCache();

  async function rewriteFilePart(
    part: LanguageModelV4FilePart,
    provider: string,
    signal: AbortSignal | undefined
  ): Promise<LanguageModelV4FilePart> {
    if (part.data.type !== "reference") return part;
    const reference = antflyAssetReferenceFrom(part.data.reference);
    if (!reference) return part;
    const resource = await resolveAuthorizedAsset(
      options.client,
      options.authorize,
      reference,
      signal
    );

    if (options.allowVendorUpload && options.files) {
      const files = options.files;
      const entry = await uploadedOnce(
        cache,
        `${provider}\u0000${resource.version}`,
        signal,
        async (sharedSignal) => {
          const cached = await cache.get(resource.version, provider);
          sharedSignal.throwIfAborted();
          if (cached && (!cached.expiresAt || cached.expiresAt.getTime() > Date.now()))
            return cached;
          const bytes = await fetchResourceBytes(resource, fetchFn, sharedSignal);
          const uploaded = await uploadFile({
            api: files,
            data: bytes,
            mediaType: resource.mime_type,
            abortSignal: sharedSignal,
          });
          sharedSignal.throwIfAborted();
          const result = { reference: uploaded.providerReference, expiresAt: uploaded.expiresAt };
          await cache.set(resource.version, provider, result);
          return result;
        }
      );
      return {
        ...part,
        data: { type: "reference", reference: { ...part.data.reference, ...entry.reference } },
      };
    }

    const bytes = await fetchResourceBytes(resource, fetchFn, signal);
    return { ...part, data: { type: "data", data: bytes } };
  }

  async function rewriteMessage(
    message: LanguageModelV4Message,
    provider: string,
    signal: AbortSignal | undefined
  ): Promise<LanguageModelV4Message> {
    if (message.role !== "user" && message.role !== "assistant") return message;
    const content = await Promise.all(
      message.content.map((part) =>
        part.type === "file" ? rewriteFilePart(part, provider, signal) : part
      )
    );
    return { ...message, content } as LanguageModelV4Message;
  }

  return {
    specificationVersion: "v4",
    transformParams: async ({ params, model }) => {
      if (model.provider === "antfly") return params;
      const signal = combinedSignal(options.abortSignal, params.abortSignal);
      const prompt = await abortable(
        Promise.all(
          params.prompt.map((message) => rewriteMessage(message, model.provider, signal))
        ),
        signal
      );
      return { ...params, prompt };
    },
  };
}

/** `wrapLanguageModel({ model, middleware: createAssetMiddleware(options) })`. */
export function withAssetMiddleware(
  model: LanguageModelV4,
  options: AssetMiddlewareOptions
): LanguageModelV4 {
  return wrapLanguageModel({ model, middleware: createAssetMiddleware(options) });
}
