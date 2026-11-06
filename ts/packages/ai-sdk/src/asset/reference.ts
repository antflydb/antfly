import type { SharedV4ProviderReference } from "@ai-sdk/provider";

/**
 * What an `{ antfly: <opaque id> }` provider reference actually names: one
 * row in an Antfly table whose `content.resource` is the asset's typed
 * source descriptor (`document-v3.json`'s `content.resource`: `type`,
 * `uri`, `mime_type`, `version`, `version_kind`, `length`).
 *
 * The opaque id is base64url JSON of this shape. It is not a capability URL
 * and is not fetchable on its own -- resolving it always goes through an
 * authenticated `AntflyClient`, which is itself scoped to the caller's
 * tenant, so there is no separate tenant check to apply here (antfly#1505's
 * "clients only send opaque IDs, checked against the tenant" requirement is
 * met by routing resolution through that authenticated client rather than
 * letting a browser- or model-supplied URL be fetched directly).
 */
export interface AntflyAssetReference {
  table: string;
  key: string;
  database?: string;
  namespace?: string;
}

export function encodeAssetReference(reference: AntflyAssetReference): string {
  return Buffer.from(JSON.stringify(reference), "utf8").toString("base64url");
}

export function decodeAssetReference(id: string): AntflyAssetReference {
  let parsed: unknown;
  try {
    parsed = JSON.parse(Buffer.from(id, "base64url").toString("utf8"));
  } catch (error) {
    throw new InvalidAssetReferenceError(id, error);
  }
  if (
    typeof parsed !== "object" ||
    parsed === null ||
    typeof (parsed as { table?: unknown }).table !== "string" ||
    typeof (parsed as { key?: unknown }).key !== "string"
  ) {
    throw new InvalidAssetReferenceError(id);
  }
  const reference = parsed as AntflyAssetReference;
  if (reference.database !== undefined && typeof reference.database !== "string")
    throw new InvalidAssetReferenceError(id);
  if (reference.namespace !== undefined && typeof reference.namespace !== "string")
    throw new InvalidAssetReferenceError(id);
  return reference;
}

/** Builds the `providerReference` a `FilePart`/`FileUIPart` should carry. */
export function assetProviderReference(reference: AntflyAssetReference): SharedV4ProviderReference {
  return { antfly: encodeAssetReference(reference) };
}

/** Reads the `antfly` entry from a provider reference, if present. */
export function antflyAssetReferenceFrom(
  providerReference: SharedV4ProviderReference | undefined
): AntflyAssetReference | undefined {
  const id = providerReference?.antfly;
  return typeof id === "string" ? decodeAssetReference(id) : undefined;
}

export class InvalidAssetReferenceError extends Error {
  constructor(
    readonly id: string,
    override readonly cause?: unknown
  ) {
    super("Invalid Antfly asset provider reference");
    this.name = "InvalidAssetReferenceError";
  }
}
