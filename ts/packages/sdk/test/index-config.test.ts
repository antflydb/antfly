import { describe, expect, it } from "vitest";
import {
  artifactEmbeddingIndexConfig,
  artifactFullTextIndexConfig,
  artifactIndexSources,
  graphIndexSources,
} from "../src/index-config.js";

describe("artifact embedding index configuration", () => {
  it("builds a full-text index over multiple artifact streams", () => {
    expect(
      artifactFullTextIndexConfig("document_text", "document_text_v1", "document_chunks_v1")
    ).toEqual({
      name: "document_text",
      type: "full_text",
      sources: [{ artifact: "document_text_v1" }, { artifact: "document_chunks_v1" }],
    });
  });

  it("combines document- and chunk-backed embedding streams", () => {
    const config = artifactEmbeddingIndexConfig("document_vectors", {
      sources: [
        { artifact: "document_dense_v1", field: "semantic_content" },
        {
          artifact: "document_chunk_dense_v1",
          sourceArtifact: "document_chunks_v1",
          field: "text",
        },
      ],
      embedder: { provider: "antfly", model: "antflydb/clipclap" },
      dimension: 384,
    });

    expect(config.sources).toEqual([
      { artifact: "document_dense_v1" },
      { artifact: "document_chunk_dense_v1" },
    ]);
    expect(config.enrichments).toHaveLength(2);
    expect(config.enrichments?.[1]).toMatchObject({
      source_artifact_name: "document_chunks_v1",
    });
    expect(config).not.toHaveProperty("embedding_name");
  });

  it("rejects duplicate sources and invalid sparse options", () => {
    expect(() => artifactIndexSources("same", "same")).toThrow(/duplicate/);
    expect(() =>
      artifactEmbeddingIndexConfig("sparse", {
        sources: [{ artifact: "tokens_v1" }],
        embedder: { provider: "antfly", model: "splade" },
        sparse: true,
        dimension: 384,
      })
    ).toThrow(/dimension/);
  });

  it("preserves graph source mappings and defensively copies metadata", () => {
    const metadata = { origin: "extractor", nested: { score: 1 } };
    const sources = graphIndexSources(
      {
        artifact: "relations_v1",
        path: "$.relations[*]",
        nodes: { source: "{{source}}", target: "{{target}}" },
        edge: { type: "{{relation}}", metadata },
        context: { doc_fields: ["title", "url"] },
      },
      { artifact: "graph_v1", path: "$.graph", format: "extraction_graph" }
    );
    metadata.nested.score = 2;
    expect(sources[0]?.edge?.metadata).toEqual({ origin: "extractor", nested: { score: 1 } });
    expect(sources[1]?.format).toBe("extraction_graph");
  });

  it("rejects invalid graph source sets", () => {
    expect(() => graphIndexSources({ artifact: "same" }, { artifact: "same" })).toThrow(
      /duplicate/
    );
    expect(() =>
      graphIndexSources({ artifact: "relations", edge: { weight: Number.NaN } })
    ).toThrow(/finite/);
  });
});
