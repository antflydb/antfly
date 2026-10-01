import { describe, expect, it } from "vitest";
import { extractionUnavailableReason } from "./extraction-model";

describe("extraction availability", () => {
  it("blocks execution while model discovery is loading", () => {
    expect(
      extractionUnavailableReason("invoice-extractor", ["invoice-extractor"], true)
    ).toBeTruthy();
  });
  it("blocks unadvertised IDs regardless of their names", () => {
    for (const name of ["invoice-extractor", "gliner2.5-base"]) {
      expect(extractionUnavailableReason(name, ["gliner2"], false)).toContain("Not yet available");
    }
  });
  it("accepts a model advertised for extraction", () => {
    expect(
      extractionUnavailableReason("invoice-extractor", ["invoice-extractor"], false)
    ).toBeNull();
  });
});
