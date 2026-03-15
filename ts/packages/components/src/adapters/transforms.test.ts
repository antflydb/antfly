import type { QueryHit } from "@antfly/sdk";
import { describe, expect, it } from "vitest";
import { confidenceLabel, hitToSourceProps, turnToStatus } from "./transforms";

describe("hitToSourceProps", () => {
  it("extracts title and url from _source", () => {
    const hit = {
      _id: "doc-1",
      _score: 0.95,
      _source: { title: "My Doc", url: "https://example.com/doc" },
    };
    const result = hitToSourceProps(hit, 0);
    expect(result).toEqual({
      key: "doc-1",
      href: "https://example.com/doc",
      title: "My Doc",
    });
  });

  it("falls back to _source.name for title", () => {
    const hit = {
      _id: "doc-2",
      _score: 0.8,
      _source: { name: "Named Doc" },
    };
    const result = hitToSourceProps(hit, 1);
    expect(result.title).toBe("Named Doc");
    expect(result.href).toBeUndefined();
  });

  it("falls back to _source.link for href", () => {
    const hit = {
      _id: "doc-3",
      _score: 0.7,
      _source: { title: "Link Doc", link: "https://example.com/link" },
    };
    const result = hitToSourceProps(hit, 2);
    expect(result.href).toBe("https://example.com/link");
  });

  it("falls back to _id for title when no title/name in source", () => {
    const hit = {
      _id: "fallback-id",
      _score: 0.5,
      _source: { data: "some data" },
    };
    const result = hitToSourceProps(hit, 3);
    expect(result.title).toBe("fallback-id");
  });

  it("falls back to index-based title when _id is missing and no source fields", () => {
    const hit = {
      _id: undefined as unknown as string,
      _score: 0.3,
      _source: {},
    };
    const result = hitToSourceProps(hit, 5);
    expect(result.title).toBe("Result 6");
    expect(result.key).toBe("5");
  });

  it("uses empty _id as key and title when _id is empty string", () => {
    const hit = { _id: "", _score: 0.3, _source: {} };
    const result = hitToSourceProps(hit, 5);
    // ?? only triggers on null/undefined, not empty string
    expect(result.key).toBe("");
    expect(result.title).toBe("");
  });

  it("uses _id as key", () => {
    const hit = { _id: "abc", _score: 1, _source: {} };
    expect(hitToSourceProps(hit, 0).key).toBe("abc");
  });

  it("handles undefined _source", () => {
    const hit: QueryHit = { _id: "no-source", _score: 0.5 };
    const result = hitToSourceProps(hit, 0);
    expect(result.title).toBe("no-source");
    expect(result.href).toBeUndefined();
  });
});

describe("turnToStatus", () => {
  it("returns 'awaiting-message' when not streaming", () => {
    expect(turnToStatus(false)).toBe("awaiting-message");
  });

  it("returns 'streaming' when streaming with content (default)", () => {
    expect(turnToStatus(true)).toBe("streaming");
  });

  it("returns 'streaming' when streaming with content explicitly true", () => {
    expect(turnToStatus(true, true)).toBe("streaming");
  });

  it("returns 'submitted' when streaming but no content yet", () => {
    expect(turnToStatus(true, false)).toBe("submitted");
  });

  it("returns 'awaiting-message' when not streaming regardless of hasContent", () => {
    expect(turnToStatus(false, false)).toBe("awaiting-message");
    expect(turnToStatus(false, true)).toBe("awaiting-message");
  });
});

describe("confidenceLabel", () => {
  it("formats confidence as percentage", () => {
    expect(confidenceLabel({ generation_confidence: 0.87, context_relevance: 0.92 })).toBe(
      "87% confident"
    );
  });

  it("rounds to nearest integer", () => {
    expect(confidenceLabel({ generation_confidence: 0.456, context_relevance: 0.5 })).toBe(
      "46% confident"
    );
  });

  it("handles 0 confidence", () => {
    expect(confidenceLabel({ generation_confidence: 0, context_relevance: 0 })).toBe(
      "0% confident"
    );
  });

  it("handles 100% confidence", () => {
    expect(confidenceLabel({ generation_confidence: 1, context_relevance: 1 })).toBe(
      "100% confident"
    );
  });
});
