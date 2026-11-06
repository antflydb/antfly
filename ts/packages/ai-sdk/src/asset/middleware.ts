import type {
  FilesV4,
  LanguageModelV4,
  LanguageModelV4FilePart,
  LanguageModelV4Message,
  ProviderV4,
} from "@ai-sdk/provider";
import type { AntflyClient } from "@antfly/sdk";
import { type LanguageModelMiddleware, uploadFile, wrapLanguageModel } from "ai";
import { type CatalogResource, resolveCatalogResource } from "./catalog.js";
import { antflyAssetReferenceFrom } from "./reference.js";

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

/** An in-memory cache, scoped to this process. Good enough for a single app server. */
export function createInMemoryAssetUploadCache(): AssetUploadCache {
  const entries = new Map<string, AssetUploadCacheEntry>();
  const cacheKey = (digest: string, provider: string) => `${provider}\u0000${digest}`;
  return {
    get: (digest, provider) => entries.get(cacheKey(digest, provider)),
    set: (digest, provider, entry) => {
      entries.set(cacheKey(digest, provider), entry);
    },
  };
}

export interface AssetMiddlewareOptions {
  client: AntflyClient;
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
  /** Shared across calls so the same asset is not re-uploaded. Defaults to an in-memory cache. */
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
    provider: string
  ): Promise<LanguageModelV4FilePart> {
    if (part.data.type !== "reference") return part;
    const reference = antflyAssetReferenceFrom(part.data.reference);
    if (!reference) return part;
    const resource = await resolveCatalogResource(options.client, reference);

    if (options.allowVendorUpload && options.files) {
      const cached = await cache.get(resource.version, provider);
      if (cached && (!cached.expiresAt || cached.expiresAt.getTime() > Date.now())) {
        return {
          ...part,
          data: { type: "reference", reference: { ...part.data.reference, ...cached.reference } },
        };
      }
      const bytes = await fetchResourceBytes(resource, fetchFn, options.abortSignal);
      const uploaded = await uploadFile({
        api: options.files,
        data: bytes,
        mediaType: resource.mime_type,
        abortSignal: options.abortSignal,
      });
      await cache.set(resource.version, provider, {
        reference: uploaded.providerReference,
        expiresAt: uploaded.expiresAt,
      });
      return {
        ...part,
        data: {
          type: "reference",
          reference: { ...part.data.reference, ...uploaded.providerReference },
        },
      };
    }

    const bytes = await fetchResourceBytes(resource, fetchFn, options.abortSignal);
    return { ...part, data: { type: "data", data: bytes } };
  }

  async function rewriteMessage(
    message: LanguageModelV4Message,
    provider: string
  ): Promise<LanguageModelV4Message> {
    if (message.role !== "user" && message.role !== "assistant") return message;
    const content = await Promise.all(
      message.content.map((part) => (part.type === "file" ? rewriteFilePart(part, provider) : part))
    );
    return { ...message, content } as LanguageModelV4Message;
  }

  return {
    specificationVersion: "v4",
    transformParams: async ({ params, model }) => {
      if (model.provider === "antfly") return params;
      const prompt = await Promise.all(
        params.prompt.map((message) => rewriteMessage(message, model.provider))
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
