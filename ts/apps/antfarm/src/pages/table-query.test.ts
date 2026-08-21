import type { IndexStatus } from "@antfly/sdk";
import { describe, expect, it } from "vitest";
import {
  artifactRetrievalDefaults,
  buildTableQueryRequest,
  parseTableQueryRequest,
  tableQueryErrorMessage,
  tableQueryInput,
} from "./table-query";

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
      limit: 3,
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
    });
  });

  it("uses full-text search when query text has no vector index", () => {
    expect(
      buildTableQueryRequest({
        query: "singularity",
        queryIndexes: [],
        selectedFields: [],
        semanticQuery: "{}",
        filterQuery: "{}",
        includeProfile: false,
      })
    ).toEqual({
      full_text_search: { query: "singularity" },
      limit: 3,
    });
  });

  it("returns projected direct matches for artifact-backed full-text search", () => {
    expect(
      buildTableQueryRequest({
        query: "singularity",
        queryIndexes: [],
        selectedFields: [],
        semanticQuery: "{}",
        filterQuery: "{}",
        includeProfile: false,
        artifactSearchField: "text",
        returnArtifactMatches: true,
      })
    ).toEqual({
      full_text_search: { query: "text:singularity" },
      fields: ["text"],
      hierarchy: {},
      limit: 3,
    });
  });

  it("returns projected direct matches for artifact-backed semantic search", () => {
    expect(
      buildTableQueryRequest({
        query: "singularity",
        queryIndexes: ["document_vectors"],
        selectedFields: [],
        semanticQuery: "{}",
        filterQuery: "{}",
        includeProfile: false,
        artifactSearchField: "text",
        returnArtifactMatches: true,
      })
    ).toEqual({
      indexes: ["document_vectors"],
      semantic_search: "singularity",
      fields: ["text"],
      hierarchy: {},
      limit: 3,
    });
  });

  it("quotes multi-word artifact queries without exposing query syntax", () => {
    expect(
      buildTableQueryRequest({
        query: '  event horizon "image"  ',
        queryIndexes: [],
        selectedFields: ["text", "title"],
        semanticQuery: "{}",
        filterQuery: "{}",
        includeProfile: false,
        artifactSearchField: "text",
        returnArtifactMatches: true,
      })
    ).toMatchObject({
      full_text_search: { query: 'text:"event horizon \\"image\\""' },
      fields: ["text", "title"],
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
      limit: 3,
    });
  });
});

describe("table query builder UX", () => {
  it("detects artifact-backed full-text and vector indexes", () => {
    const indexes = [
      {
        config: {
          name: "document_text",
          type: "full_text",
          field: "text",
          artifact_name: "document_chunks_v1",
          enrichments: [
            {
              name: "document_chunks_v1",
              kind: "chunk",
              field: "text",
              source_artifact_name: "document_units_v1",
              full_text_index: true,
            },
          ],
        },
        shard_status: {},
        status: { index_type: "full_text" },
      },
      {
        config: {
          name: "document_vectors",
          type: "embeddings",
          field: "text",
          source_artifact_name: "document_chunks_v1",
        },
        shard_status: {},
        status: { index_type: "embeddings" },
      },
    ] as IndexStatus[];

    expect(artifactRetrievalDefaults(indexes, [])).toEqual({
      field: "text",
      returnMatches: true,
    });
    expect(artifactRetrievalDefaults(indexes, ["document_vectors"])).toEqual({
      field: "text",
      returnMatches: true,
    });
  });

  it("does not apply hierarchy defaults to ordinary indexes", () => {
    const indexes = [
      {
        config: { name: "products", type: "full_text" },
        shard_status: {},
        status: { index_type: "full_text" },
      },
    ] as IndexStatus[];

    expect(artifactRetrievalDefaults(indexes, [])).toBeNull();
  });

  it("round-trips field-scoped artifact text into the builder", () => {
    expect(tableQueryInput({ full_text_search: { query: "text:singularity" } }, "text")).toBe(
      "singularity"
    );
    expect(tableQueryInput({ full_text_search: { query: 'text:"event horizon"' } }, "text")).toBe(
      "event horizon"
    );
  });

  it("shows Problem Details and Error messages instead of undefined", () => {
    expect(
      tableQueryErrorMessage(
        { title: "Bad Gateway", detail: "upstream request failed" },
        "fallback"
      )
    ).toBe("upstream request failed");
    expect(tableQueryErrorMessage(new Error("Table query failed: invalid query"), "fallback")).toBe(
      "Table query failed: invalid query"
    );
  });
});
