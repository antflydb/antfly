import type { AntflyClient } from "@antfly/sdk";
import type { AntflyAssetReference, AuthorizeAsset } from "./reference.js";

/** `document-v3.json`'s `content.resource`: the typed, version-pinned source descriptor. */
export interface CatalogResource {
  type: "source";
  uri: string;
  mime_type: string;
  version: string;
  version_kind?: "strong_etag";
  length: number;
  /** Verified original-byte digest from source metadata, separate from the representation ETag. */
  content_sha256?: string;
}

export class AssetNotFoundError extends Error {
  constructor(readonly reference: AntflyAssetReference) {
    super(`Asset not found: ${reference.table}/${reference.key}`);
    this.name = "AssetNotFoundError";
  }
}

export class AssetAccessDeniedError extends Error {
  constructor(readonly reference: AntflyAssetReference) {
    super("Access to this asset was denied.");
    this.name = "AssetAccessDeniedError";
  }
}

/** The only path from a client-suppliable reference to a catalog row. A thrown decision denies. */
export async function resolveAuthorizedAsset(
  client: AntflyClient,
  authorize: AuthorizeAsset,
  reference: AntflyAssetReference,
  signal?: AbortSignal
): Promise<CatalogResource> {
  signal?.throwIfAborted();
  let allowed = false;
  try {
    allowed = await authorize(reference);
  } catch {
    allowed = false;
  }
  signal?.throwIfAborted();
  if (!allowed) throw new AssetAccessDeniedError(reference);
  return resolveCatalogResource(client, reference, signal);
}

export class AssetMissingResourceError extends Error {
  constructor(readonly reference: AntflyAssetReference) {
    super(
      `Catalog row ${reference.table}/${reference.key} has no content.resource; it is not a byte-backed asset`
    );
    this.name = "AssetMissingResourceError";
  }
}

/**
 * Resolves an `AntflyAssetReference` to its catalog row's `content.resource`.
 * Projects only the descriptor and verified original-byte digest used for
 * cache identity. Other provenance stays on the catalog row.
 */
export async function resolveCatalogResource(
  client: AntflyClient,
  reference: AntflyAssetReference,
  signal?: AbortSignal
): Promise<CatalogResource> {
  const { data, response } = await client
    .getRawClient()
    .GET("/db/v1/tables/{tableName}/documents/{key}", {
      signal,
      params: {
        path: { tableName: reference.table, key: reference.key },
        query: { consistency: "read_index" },
      },
    });
  if (response.status === 404) throw new AssetNotFoundError(reference);
  if (!response.ok)
    throw new Error(
      `Catalog lookup failed for ${reference.table}/${reference.key}: HTTP ${response.status}`
    );
  const row = data as
    | { content?: { resource?: unknown }; metadata?: { sha256?: unknown } }
    | undefined;
  const resource = row?.content?.resource;
  if (!isCatalogResource(resource)) throw new AssetMissingResourceError(reference);
  const digest = row?.metadata?.sha256;
  return {
    type: resource.type,
    uri: resource.uri,
    mime_type: resource.mime_type,
    version: resource.version,
    ...(resource.version_kind ? { version_kind: resource.version_kind } : {}),
    length: resource.length,
    ...(typeof digest === "string" && /^[a-fA-F0-9]{64}$/.test(digest)
      ? { content_sha256: digest.toLowerCase() }
      : {}),
  };
}

function isCatalogResource(value: unknown): value is CatalogResource {
  if (typeof value !== "object" || value === null) return false;
  const resource = value as Record<string, unknown>;
  return (
    resource.type === "source" &&
    typeof resource.uri === "string" &&
    typeof resource.mime_type === "string" &&
    typeof resource.version === "string" &&
    typeof resource.length === "number"
  );
}
