import { describe, expect, it } from "vitest";
import {
  antflyAssetReferenceFrom,
  assetProviderReference,
  decodeAssetReference,
  encodeAssetReference,
  InvalidAssetReferenceError,
} from "../src/asset/reference.js";

describe("asset reference encoding", () => {
  it("round-trips table/key through the opaque id", () => {
    const reference = { table: "files", key: "abc123", database: "default", namespace: "public" };
    const id = encodeAssetReference(reference);
    expect(decodeAssetReference(id)).toEqual(reference);
  });

  it("builds a providerReference carrying only the antfly entry", () => {
    const reference = { table: "files", key: "abc123" };
    expect(assetProviderReference(reference)).toEqual({ antfly: encodeAssetReference(reference) });
  });

  it("reads the antfly entry back out of a provider reference", () => {
    const reference = { table: "files", key: "abc123" };
    const providerReference = { antfly: encodeAssetReference(reference), openai: "file-xyz" };
    expect(antflyAssetReferenceFrom(providerReference)).toEqual(reference);
  });

  it("returns undefined when there is no antfly entry", () => {
    expect(antflyAssetReferenceFrom({ openai: "file-xyz" })).toBeUndefined();
    expect(antflyAssetReferenceFrom(undefined)).toBeUndefined();
  });

  it("rejects a malformed id instead of silently returning garbage", () => {
    expect(() => decodeAssetReference("not-valid-base64url-json")).toThrow(
      InvalidAssetReferenceError
    );
    expect(() => decodeAssetReference(Buffer.from("{}").toString("base64url"))).toThrow(
      InvalidAssetReferenceError
    );
    expect(() =>
      decodeAssetReference(Buffer.from(JSON.stringify({ table: "t" })).toString("base64url"))
    ).toThrow(InvalidAssetReferenceError);
  });
});
