import type { IndexStatus, TableStatus } from "@antfly/sdk";
import { describe, expect, it } from "vitest";
import {
  artifactRetrievalDefaults,
  builderArtifactRetrievalDefaults,
  buildTableQueryRequest,
  parseTableQueryRequest,
  requestArtifactRetrievalDefaults,
  tableQueryBuilderConversionBlocker,
  tableQueryErrorMessage,
  tableQueryInput,
  tableQueryJsonSafetyBlocker,
  tableQueryMetadataBlocker,
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
      limit: 10,
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
        artifactSearchFields: ["text"],
        artifactProjectionFields: ["text"],
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
        artifactSearchFields: ["text"],
        artifactProjectionFields: ["text", "caption", "text"],
        returnArtifactMatches: true,
      })
    ).toEqual({
      indexes: ["document_vectors"],
      semantic_search: "singularity",
      fields: ["text", "caption"],
      hierarchy: {},
      limit: 3,
    });
  });

  it("uses a structured match for natural multi-word artifact queries", () => {
    expect(
      buildTableQueryRequest({
        query: '  event horizon "image"  ',
        queryIndexes: [],
        selectedFields: ["text", "title"],
        semanticQuery: "{}",
        filterQuery: "{}",
        includeProfile: false,
        artifactSearchFields: ["text"],
        returnArtifactMatches: true,
      })
    ).toMatchObject({
      full_text_search: { field: "text", match: 'event horizon "image"' },
      fields: ["text", "title"],
    });
  });

  it("searches every artifact field with a structured disjunction", () => {
    expect(
      buildTableQueryRequest({
        query: "event horizon",
        queryIndexes: [],
        selectedFields: [],
        semanticQuery: "{}",
        filterQuery: "{}",
        includeProfile: false,
        artifactSearchFields: ["text", "caption"],
        artifactProjectionFields: ["text", "caption", "text"],
        returnArtifactMatches: true,
      })
    ).toEqual({
      full_text_search: {
        disjuncts: [
          { field: "text", match: "event horizon" },
          { field: "caption", match: "event horizon" },
        ],
      },
      fields: ["text", "caption"],
      hierarchy: {},
      limit: 3,
    });
  });

  it("searches schema-unknown asset output without returning source payloads", () => {
    expect(
      buildTableQueryRequest({
        query: "crimson harbor",
        queryIndexes: [],
        selectedFields: [],
        semanticQuery: "{}",
        filterQuery: "{}",
        includeProfile: false,
        artifactSearchFields: ["_all"],
        artifactProjectionFields: [],
        returnArtifactMatches: true,
      })
    ).toEqual({
      full_text_search: { field: "_all", match: "crimson harbor" },
      fields: [],
      hierarchy: {},
      limit: 3,
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
    });
  });
});

describe("table query builder UX", () => {
  it("blocks the builder until both metadata sources are ready", () => {
    expect(tableQueryMetadataBlocker("loading", "ready")).toBe(
      "Loading query metadata before enabling safe queries."
    );
    expect(tableQueryMetadataBlocker("ready", "loading")).toBe(
      "Loading query metadata before enabling safe queries."
    );
    expect(tableQueryMetadataBlocker("error", "ready")).toBe(
      "Query metadata could not be loaded safely. Retry before using the query builder, or use JSON mode with an explicit fields projection."
    );
    expect(tableQueryMetadataBlocker("ready", "ready")).toBeNull();
  });

  it("requires an explicit JSON projection while metadata is unavailable", () => {
    const metadataBlocker = tableQueryMetadataBlocker("error", "ready");
    expect(tableQueryJsonSafetyBlocker(metadataBlocker, false, { limit: 3 })).toBe(
      'Query metadata is not ready. Add an explicit "fields" array before running this JSON query; use "fields": [] for identity-only results.'
    );
    expect(
      tableQueryJsonSafetyBlocker(metadataBlocker, false, { fields: [], limit: 3 })
    ).toBeNull();
    expect(
      tableQueryJsonSafetyBlocker(metadataBlocker, false, { fields: ["text"], limit: 3 })
    ).toBeNull();
    expect(
      tableQueryJsonSafetyBlocker(metadataBlocker, false, {
        fields: ["text", 42] as unknown as string[],
        limit: 3,
      })
    ).not.toBeNull();
    expect(tableQueryJsonSafetyBlocker(null, false, { limit: 3 })).toBeNull();
    expect(tableQueryJsonSafetyBlocker(metadataBlocker, false, null)).toBeNull();
  });

  it("keeps known artifact-backed JSON queries projected after metadata becomes ready", () => {
    expect(tableQueryJsonSafetyBlocker(null, true, { limit: 3 })).toBe(
      'Artifact-backed JSON queries require an explicit "fields" array; use "fields": [] for identity-only results.'
    );
    expect(tableQueryJsonSafetyBlocker(null, true, { fields: ["text"], limit: 3 })).toBeNull();
    expect(tableQueryJsonSafetyBlocker(null, false, { limit: 3 })).toBeNull();
  });

  it("applies artifact defaults only to requests that retrieve search artifacts", () => {
    const indexes = [
      {
        config: { name: "full_text_index_v0", type: "full_text" },
        shard_status: {},
        status: { index_type: "full_text" },
      },
    ] as IndexStatus[];
    const tableStatus = {
      name: "docs",
      indexes: {
        full_text_index_v0: { name: "full_text_index_v0", type: "full_text" },
      },
      shards: {},
      storage_status: {},
      artifact_enrichments: [
        {
          name: "document_chunks_v1",
          kind: "chunk",
          field: "text",
          full_text_index: true,
        },
      ],
    } as TableStatus;

    expect(builderArtifactRetrievalDefaults(indexes, "", [], tableStatus)).toBeNull();
    expect(builderArtifactRetrievalDefaults(indexes, "singularity", [], tableStatus)).toEqual({
      searchFields: ["text"],
      projectionFields: ["text"],
      returnMatches: true,
    });
    expect(
      requestArtifactRetrievalDefaults(
        indexes,
        { semantic_search: "singularity", limit: 3 },
        tableStatus
      )
    ).toBeNull();
    expect(requestArtifactRetrievalDefaults(indexes, { limit: 10 }, tableStatus)).toBeNull();
    expect(
      requestArtifactRetrievalDefaults(
        indexes,
        { filter_query: { match: "singularity", field: "text" }, limit: 10 },
        tableStatus
      )
    ).toBeNull();
    expect(
      requestArtifactRetrievalDefaults(
        indexes,
        { full_text_search: { query: "singularity" }, limit: 3 },
        tableStatus
      )
    ).toEqual({
      searchFields: ["text"],
      projectionFields: ["text"],
      returnMatches: true,
    });
    const hybridIndexes = [
      ...indexes,
      {
        config: { name: "document_vectors", type: "embeddings" },
        shard_status: {},
        status: { index_type: "embeddings" },
      },
    ] as IndexStatus[];
    expect(
      requestArtifactRetrievalDefaults(
        hybridIndexes,
        {
          semantic_search: "singularity",
          indexes: ["document_vectors"],
          full_text_search: { query: "singularity" },
          limit: 3,
        },
        tableStatus
      )
    ).toEqual({
      searchFields: ["text"],
      projectionFields: ["text"],
      returnMatches: true,
    });
    expect(
      requestArtifactRetrievalDefaults(indexes, { hierarchy: {}, limit: 3 }, tableStatus)
    ).not.toBeNull();
  });

  it("detects artifact-backed full-text and vector indexes", () => {
    const indexes = [
      {
        config: {
          name: "document_text",
          type: "full_text",
          artifact_name: "document_chunks_v1",
          enrichments: ["document_units_v1", "document_chunks_v1"],
        },
        shard_status: {},
        status: { index_type: "full_text" },
      },
      {
        config: {
          name: "document_vectors",
          type: "embeddings",
          field: "embedding",
          embedding_name: "document_chunk_dense_v1",
          source_artifact_name: "document_chunks_v1",
          enrichments: ["document_chunk_dense_v1"],
        },
        shard_status: {},
        status: { index_type: "embeddings" },
      },
    ] as unknown as IndexStatus[];

    const tableStatus = {
      name: "docs",
      indexes: {
        document_text: {
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
        document_vectors: {
          name: "document_vectors",
          type: "embeddings",
          field: "embedding",
          embedding_name: "document_chunk_dense_v1",
          source_artifact_name: "document_chunks_v1",
          enrichments: [
            {
              name: "document_chunk_dense_v1",
              kind: "embedding",
              field: "text",
              source_artifact_name: "document_chunks_v1",
            },
          ],
        },
      },
      shards: {},
      storage_status: {},
    } as TableStatus;

    expect(artifactRetrievalDefaults(indexes, [], tableStatus)).toEqual({
      searchFields: ["text"],
      projectionFields: ["text"],
      returnMatches: true,
    });
    expect(artifactRetrievalDefaults(indexes, ["document_vectors"], tableStatus)).toEqual({
      searchFields: ["text"],
      projectionFields: ["text"],
      returnMatches: true,
    });
  });

  it("detects separately registered chunk enrichments routed to full-text search", () => {
    const indexes = [
      {
        config: { name: "full_text_index_v0", type: "full_text" },
        shard_status: {},
        status: { index_type: "full_text" },
      },
    ] as IndexStatus[];
    const tableStatus = {
      name: "docs",
      indexes: {
        full_text_index_v0: { name: "full_text_index_v0", type: "full_text" },
      },
      shards: {},
      storage_status: {},
      artifact_enrichments: [
        {
          name: "document_chunks_v1",
          kind: "chunk",
          field: "text",
          full_text_index: true,
        },
      ],
    } as TableStatus;

    expect(artifactRetrievalDefaults(indexes, [], tableStatus)).toEqual({
      searchFields: ["text"],
      projectionFields: ["text"],
      returnMatches: true,
    });
  });

  it("uses all-fields search when generated asset output has no declared schema", () => {
    const indexes = [
      {
        config: { name: "full_text_index_v0", type: "full_text" },
        shard_status: {},
        status: { index_type: "full_text" },
      },
    ] as IndexStatus[];
    const tableStatus = {
      name: "docs",
      indexes: {
        full_text_index_v0: { name: "full_text_index_v0", type: "full_text" },
      },
      shards: {},
      storage_status: {},
      artifact_enrichments: [
        {
          name: "document_chunks_v1",
          kind: "chunk",
          field: "text",
          full_text_index: true,
        },
        {
          name: "image_captions_v1",
          kind: "asset",
          field: "caption",
          full_text_index: true,
        },
      ],
    } as TableStatus;

    expect(artifactRetrievalDefaults(indexes, [], tableStatus)).toEqual({
      searchFields: ["_all"],
      projectionFields: ["text"],
      returnMatches: true,
    });
  });

  it("keeps asset-only retrieval identity-only by default", () => {
    const indexes = [
      {
        config: { name: "full_text_index_v0", type: "full_text" },
        shard_status: {},
        status: { index_type: "full_text" },
      },
    ] as IndexStatus[];
    const tableStatus = {
      name: "docs",
      indexes: {
        full_text_index_v0: { name: "full_text_index_v0", type: "full_text" },
      },
      shards: {},
      storage_status: {},
      artifact_enrichments: [
        {
          name: "image_captions_v1",
          kind: "asset",
          field: "inline_url",
          full_text_index: true,
        },
      ],
    } as TableStatus;

    expect(artifactRetrievalDefaults(indexes, [], tableStatus)).toEqual({
      searchFields: ["_all"],
      projectionFields: [],
      returnMatches: true,
    });
  });

  it("keeps explicitly named full-text artifact indexes isolated", () => {
    const indexes = [
      {
        config: {
          name: "document_text",
          type: "full_text",
          artifact_name: "document_chunks_v1",
        },
        shard_status: {},
        status: { index_type: "full_text" },
      },
    ] as unknown as IndexStatus[];
    const tableStatus = {
      name: "docs",
      indexes: {
        document_text: {
          name: "document_text",
          type: "full_text",
          artifact_name: "document_chunks_v1",
        },
      },
      shards: {},
      storage_status: {},
      artifact_enrichments: [
        {
          name: "document_chunks_v1",
          kind: "chunk",
          field: "text",
          full_text_index: true,
        },
        {
          name: "image_captions_v1",
          kind: "asset",
          field: "caption",
          full_text_index: true,
        },
      ],
    } as unknown as TableStatus;

    expect(artifactRetrievalDefaults(indexes, [], tableStatus)).toEqual({
      searchFields: ["text"],
      projectionFields: ["text"],
      returnMatches: true,
    });
  });

  it("does not treat an ordinary vector index as artifact-backed because of table enrichments", () => {
    const indexes = [
      {
        config: { name: "product_vectors", type: "embeddings", field: "embedding" },
        shard_status: {},
        status: { index_type: "embeddings" },
      },
    ] as IndexStatus[];
    const tableStatus = {
      name: "products",
      indexes: {
        product_vectors: {
          name: "product_vectors",
          type: "embeddings",
          field: "embedding",
          embedding_name: "stale_dense_v1",
          source_artifact_name: "document_chunks_v1",
          enrichments: [
            {
              name: "stale_dense_v1",
              kind: "embedding",
              field: "text",
              source_artifact_name: "document_chunks_v1",
            },
          ],
        },
      },
      shards: {},
      storage_status: {},
      artifact_enrichments: [
        {
          name: "document_chunks_v1",
          kind: "chunk",
          field: "text",
          full_text_index: true,
        },
      ],
    } as TableStatus;

    expect(artifactRetrievalDefaults(indexes, ["product_vectors"], tableStatus)).toBeNull();
  });

  it("uses live index membership instead of stale table details", () => {
    const tableStatus = {
      name: "docs",
      indexes: {
        removed_vectors: {
          name: "removed_vectors",
          type: "embeddings",
          source_artifact_name: "document_chunks_v1",
        },
      },
      shards: {},
      storage_status: {},
    } as TableStatus;

    expect(artifactRetrievalDefaults([], ["removed_vectors"], tableStatus)).toBeNull();
  });

  it("projects every artifact field and rejects mixed vector result types", () => {
    const indexes = [
      {
        config: {
          name: "text_vectors",
          type: "embeddings",
          source_artifact_name: "text_chunks_v1",
        },
        shard_status: {},
        status: { index_type: "embeddings" },
      },
      {
        config: {
          name: "caption_vectors",
          type: "embeddings",
          source_artifact_name: "image_assets_v1",
        },
        shard_status: {},
        status: { index_type: "embeddings" },
      },
      {
        config: { name: "product_vectors", type: "embeddings", field: "embedding" },
        shard_status: {},
        status: { index_type: "embeddings" },
      },
    ] as IndexStatus[];
    const tableStatus = {
      name: "docs",
      indexes: {
        text_vectors: {
          name: "text_vectors",
          type: "embeddings",
          embedding_name: "text_dense_v1",
          source_artifact_name: "text_chunks_v1",
        },
        caption_vectors: {
          name: "caption_vectors",
          type: "embeddings",
          embedding_name: "caption_dense_v1",
          source_artifact_name: "image_assets_v1",
        },
        product_vectors: { name: "product_vectors", type: "embeddings", field: "embedding" },
      },
      shards: {},
      storage_status: {},
      artifact_enrichments: [
        {
          name: "text_dense_v1",
          kind: "embedding",
          field: "text",
          source_artifact_name: "text_chunks_v1",
        },
        {
          name: "caption_dense_v1",
          kind: "embedding",
          field: "caption",
          source_artifact_name: "image_assets_v1",
        },
      ],
    } as TableStatus;

    expect(
      artifactRetrievalDefaults(indexes, ["text_vectors", "caption_vectors"], tableStatus)
    ).toEqual({
      searchFields: ["text", "caption"],
      projectionFields: ["text", "caption"],
      returnMatches: true,
    });
    expect(
      artifactRetrievalDefaults(indexes, ["text_vectors", "product_vectors"], tableStatus)
    ).toEqual({
      searchFields: ["text"],
      projectionFields: ["text"],
      returnMatches: true,
      selectionError:
        "Artifact-backed and document-backed vector indexes cannot be searched together. Select indexes that return the same result type.",
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
    expect(tableQueryInput({ full_text_search: { query: "text:singularity" } }, ["text"])).toBe(
      "singularity"
    );
    expect(tableQueryInput({ full_text_search: { query: 'text:"event horizon"' } }, ["text"])).toBe(
      "event horizon"
    );
    expect(
      tableQueryInput({ full_text_search: { match: "event horizon", field: "text" } }, ["text"])
    ).toBe("event horizon");
  });

  it("round-trips generated multi-field artifact disjunctions into the builder", () => {
    const request = buildTableQueryRequest({
      query: "event horizon",
      queryIndexes: [],
      selectedFields: [],
      semanticQuery: "{}",
      filterQuery: "{}",
      includeProfile: false,
      artifactSearchFields: ["text", "caption"],
      artifactProjectionFields: ["text", "caption"],
      returnArtifactMatches: true,
    });

    expect(tableQueryInput(request, ["text", "caption"])).toBe("event horizon");
  });

  it("does not flatten heterogeneous or custom full-text disjunctions", () => {
    expect(
      tableQueryInput(
        {
          full_text_search: {
            disjuncts: [
              { field: "text", match: "event horizon" },
              { field: "caption", match: "singularity" },
            ],
          },
        },
        ["text", "caption"]
      )
    ).toBe("");
    expect(
      tableQueryInput(
        {
          full_text_search: {
            disjuncts: [
              { field: "text", match: "event horizon" },
              { field: "summary", match: "event horizon" },
            ],
          },
        },
        ["text", "caption"]
      )
    ).toBe("");
  });

  it("allows JSON-to-Builder conversion only when the request is lossless", () => {
    const source = {
      hierarchy: {},
      fields: ["text"],
      full_text_search: { field: "text", match: "event horizon" },
      limit: 3,
    };
    const reordered = {
      limit: 3,
      full_text_search: { match: "event horizon", field: "text" },
      fields: ["text"],
      hierarchy: {},
    };

    expect(tableQueryBuilderConversionBlocker(source, reordered)).toBeNull();
    expect(
      tableQueryBuilderConversionBlocker(
        {
          ...source,
          full_text_search: {
            disjuncts: [
              { field: "text", match: "event horizon" },
              { field: "caption", match: "singularity" },
            ],
          },
        },
        reordered
      )
    ).toContain("without changing it");
    expect(
      tableQueryBuilderConversionBlocker(
        { ...source, order_by: [{ field: "created_at", direction: "desc" }] } as typeof source,
        reordered
      )
    ).toContain("without changing it");
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
