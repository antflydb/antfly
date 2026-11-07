import type { SharedV4ProviderReference } from "@ai-sdk/provider";

/**
 * What an `{ antfly: <opaque id> }` provider reference actually names: one
 * row in an Antfly table whose `content.resource` is the asset's typed
 * source descriptor (`document-v3.json`'s `content.resource`: `type`,
 * `uri`, `mime_type`, `version`, `version_kind`, `length`).
 *
 * The opaque id is base64url JSON of this shape, and it isn't signed: anyone can
 * write one for any table and key, and file parts can originate in a browser.
 * The server resolves it with the app's own credential, which reaches further
 * than one end user should, so every resolution needs the app's own decision
 * (`AuthorizeAsset`) and nothing resolves without one.
 */
export interface AntflyAssetReference {
  table: string;
  key: string;
}

/** The app's decision on whether the current user may use this asset. */
export type AuthorizeAsset = (reference: AntflyAssetReference) => boolean | Promise<boolean>;

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
  return parsed as AntflyAssetReference;
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
