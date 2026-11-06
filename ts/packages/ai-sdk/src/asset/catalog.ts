import type { AntflyClient } from "@antfly/sdk";
import type { AntflyAssetReference } from "./reference.js";

/** `document-v3.json`'s `content.resource`: the typed, version-pinned source descriptor. */
export interface CatalogResource {
  type: "source";
  uri: string;
  mime_type: string;
  version: string;
  version_kind?: "strong_etag";
  length: number;
}

export class AssetNotFoundError extends Error {
  constructor(readonly reference: AntflyAssetReference) {
    super(`Asset not found: ${reference.table}/${reference.key}`);
    this.name = "AssetNotFoundError";
  }
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
 * Never returns the row's other fields: callers that need provenance should
 * read the row themselves rather than growing this helper's return shape.
 */
export async function resolveCatalogResource(
  client: AntflyClient,
  reference: AntflyAssetReference
): Promise<CatalogResource> {
  const { data, response } = await client
    .getRawClient()
    .GET("/db/v1/tables/{tableName}/documents/{key}", {
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
  const resource = (data as { content?: { resource?: unknown } } | undefined)?.content?.resource;
  if (!isCatalogResource(resource)) throw new AssetMissingResourceError(reference);
  return resource;
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
