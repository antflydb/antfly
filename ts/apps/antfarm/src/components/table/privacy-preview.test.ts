import { describe, expect, it } from "vitest";
import { privacyHighlights } from "./privacy-preview";

describe("privacy source highlighting", () => {
  it("preserves Unicode and repeated occurrences using UTF-8 byte offsets", () => {
    expect(
      privacyHighlights("Éva 🔐 Éva", [
        { start: 0, end: 4, category: "person" },
        { start: 10, end: 14, category: "person" },
      ])
    ).toEqual([
      { text: "Éva", categories: ["person"] },
      { text: " 🔐 ", categories: [] },
      { text: "Éva", categories: ["person"] },
    ]);
  });
  it("highlights the complete union of overlapping findings", () => {
    expect(
      privacyHighlights("abcdef", [
        { start: 0, end: 3, category: "person" },
        { start: 2, end: 6, category: "secret" },
      ])
    ).toEqual([{ text: "abcdef", categories: ["person", "secret"] }]);
  });
  it("rejects invalid Unicode boundaries and out-of-range spans", () => {
    expect(() => privacyHighlights("Éva", [{ start: 1, end: 2, category: "person" }])).toThrow();
    expect(() => privacyHighlights("abc", [{ start: 0, end: 9, category: "person" }])).toThrow();
  });
});
