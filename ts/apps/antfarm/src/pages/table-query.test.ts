import { describe, expect, it } from "vitest";
import { buildTableQueryRequest, parseTableQueryRequest } from "./table-query";

describe("buildTableQueryRequest", () => {
  it("builds a match-all request when the builder query is empty", () => {
    expect(
      buildTableQueryRequest({
        query: "",
        queryIndexes: [],
        selectedFields: [],
        semanticQuery: "{}",
        filterQuery: "{}",
        includeProfile: true,
      })
    ).toEqual({
      limit: 10,
      profile: true,
    });
  });

  it("does not send blank text as a semantic query", () => {
    expect(
      buildTableQueryRequest({
        query: "   ",
        queryIndexes: ["messages"],
        selectedFields: [],
        semanticQuery: '{"limit": 5, "offset": 2}',
        filterQuery: "{}",
        includeProfile: false,
      })
    ).toEqual({
      limit: 5,
      offset: 2,
      profile: false,
    });
  });

  it("preserves semantic and filter searches", () => {
    expect(
      buildTableQueryRequest({
        query: "beetles",
        queryIndexes: ["messages"],
        selectedFields: ["text"],
        semanticQuery: '{"limit": 7, "offset": 2}',
        filterQuery: '{"match": {"text": "session"}}',
        includeProfile: true,
      })
    ).toEqual({
      indexes: ["messages"],
      semantic_search: "beetles",
      fields: ["text"],
      limit: 7,
      filter_query: { match: { text: "session" } },
      profile: true,
    });
  });

  it("rejects non-object JSON request bodies", () => {
    expect(parseTableQueryRequest("null")).toBeNull();
    expect(parseTableQueryRequest("[]")).toBeNull();
    expect(parseTableQueryRequest('"query"')).toBeNull();
    expect(parseTableQueryRequest('{"limit": 5}')).toEqual({ limit: 5 });
  });

  it("ignores non-object builder options and filters", () => {
    expect(
      buildTableQueryRequest({
        query: "",
        queryIndexes: [],
        selectedFields: [],
        semanticQuery: "[]",
        filterQuery: '["not", "a", "filter"]',
        includeProfile: false,
      })
    ).toEqual({
      limit: 10,
      profile: false,
    });
  });
});
