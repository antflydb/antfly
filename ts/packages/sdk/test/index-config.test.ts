import { describe, expect, it } from "vitest";

import {
  artifactEmbeddingIndexConfig,
  artifactIndexSources,
  graphIndexSources,
} from "../src/index-config.js";

describe("artifact index configuration", () => {
  it("builds a multi-source embedding index with automatic vector-space validation", () => {
    const config = artifactEmbeddingIndexConfig("document_vectors", {
      sources: [
        { artifact: "title_dense_v1", sourceArtifact: "title_chunks_v1" },
        { artifact: "body_dense_v1", sourceArtifact: "body_chunks_v1" },
      ],
      dimension: 384,
      embedder: { provider: "antfly", model: "antflydb/clipclap" },
    });

    expect(config.sources).toEqual([
      { artifact: "title_dense_v1" },
      { artifact: "body_dense_v1" },
    ]);
    expect(config.enrichments).toHaveLength(2);
    expect(config.enrichments?.[0]).not.toHaveProperty("vector_space");
    expect(config).not.toHaveProperty("field");
    expect(config).not.toHaveProperty("embedding_name");
  });

  it("preserves graph-specific source interpretation", () => {
    expect(
      graphIndexSources(
        { artifact: "relations_v1", path: "$.relations[*]", format: "extraction_relation" },
        { artifact: "graph_v1", path: "$.graph", format: "extraction_graph" }
      )
    ).toHaveLength(2);
  });

  it("rejects duplicates and source-count overflow", () => {
    expect(() => artifactIndexSources("same", "same")).toThrow(/duplicate/);
    expect(() => artifactIndexSources(...Array.from({ length: 65 }, (_, i) => `a${i}`))).toThrow(
      /at most 64/
    );
    expect(() =>
      graphIndexSources({ artifact: "relations_v1", format: "unknown" as "extraction_graph" })
    ).toThrow(/format/);
    expect(() =>
      artifactEmbeddingIndexConfig("vectors", {
        sources: [{ artifact: "dense_v1" }],
        embedder: {} as never,
      })
    ).toThrow(/provider/);
  });
});
