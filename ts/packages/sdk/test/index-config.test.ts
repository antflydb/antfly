import { describe, expect, it } from "vitest";
import {
  artifactEmbeddingIndexConfig,
  artifactFullTextIndexConfig,
  artifactIndexSources,
  graphIndexSources,
  validateCreateIndexRequestRelationships,
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

    expect(
      artifactFullTextIndexConfig("document_text", {
        artifacts: ["document_text_v1", "document_chunks_v1"],
        field: " text ",
      })
    ).toEqual({
      name: "document_text",
      type: "full_text",
      field: "text",
      sources: [{ artifact: "document_text_v1" }, { artifact: "document_chunks_v1" }],
    });

    expect(
      artifactFullTextIndexConfig("document_text", {
        sources: [
          { artifact: "document_text_v1", field: " summary " },
          { artifact: "document_chunks_v1", field: "text" },
        ],
      })
    ).toEqual({
      name: "document_text",
      type: "full_text",
      sources: [
        { artifact: "document_text_v1", field: "summary" },
        { artifact: "document_chunks_v1", field: "text" },
      ],
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

  it("canonicalizes template-only embedding sources without a no-op field", () => {
    const config = artifactEmbeddingIndexConfig("templated_vectors", {
      sources: [{ artifact: "templated_v1", template: "{{ title }}: {{ body }}" }],
      embedder: { provider: "antfly", model: "antflydb/clipclap" },
    });
    expect(config.enrichments?.[0]).toMatchObject({ template: "{{ title }}: {{ body }}" });
    expect(config.enrichments?.[0]).not.toHaveProperty("field");
  });

  it("rejects duplicate sources and invalid sparse options", () => {
    expect(() => artifactIndexSources("same", "same")).toThrow(/duplicate/);
    expect(() =>
      // @ts-expect-error JavaScript callers still require runtime validation.
      artifactIndexSources(42)
    ).toThrow(/non-empty string/);
    expect(() =>
      // @ts-expect-error Sparse configurations reject dense-only dimensions.
      artifactEmbeddingIndexConfig("sparse", {
        sources: [{ artifact: "tokens_v1" }],
        embedder: { provider: "antfly", model: "splade" },
        sparse: true,
        dimension: 384,
      })
    ).toThrow(/dimension/);
  });

  it("enforces OpenAPI index request relationships before transport", () => {
    expect(() =>
      validateCreateIndexRequestRelationships({
        type: "embeddings",
        source_artifact_name: "chunks_v1",
      })
    ).toThrow(/requires a non-empty embedding_name/);
    expect(() =>
      validateCreateIndexRequestRelationships({
        type: "embeddings",
        external: true,
        sources: [{ artifact: "dense_v1" }],
      })
    ).toThrow(/external/);
    expect(() =>
      validateCreateIndexRequestRelationships({
        type: "embeddings",
        embedding_name: "dense_v1",
        source_artifact_name: "wrong_chunks_v1",
        enrichments: [
          {
            name: "dense_v1",
            kind: "embedding",
            source_artifact_name: "chunks_v1",
          },
        ],
      })
    ).toThrow(/authoritative embedding enrichment/);
    expect(() =>
      validateCreateIndexRequestRelationships({
        type: "full_text",
        artifact_name: "chunks_v1",
        sources: [{ artifact: "chunks_v2" }],
      })
    ).toThrow(/artifact_name/);
    expect(() =>
      validateCreateIndexRequestRelationships({
        type: "graph",
        source: { artifact: "relations_v1" },
        sources: [{ artifact: "relations_v2" }],
      })
    ).toThrow(/source/);
    expect(() =>
      validateCreateIndexRequestRelationships({
        type: "embeddings",
        external: false,
        sources: [{ artifact: "dense_v1" }],
      })
    ).not.toThrow();
  });

  it("preserves graph source mappings and defensively copies metadata", () => {
    const metadata = { origin: "extractor", nested: { score: 1 } };
    const sources = graphIndexSources(
      {
        artifact: "relations_v1",
        path: "$.relations[*]",
        nodes: { target: 42 },
        edge: { type: "{{relation}}", metadata },
        context: { doc_fields: ["title", "url"] },
      },
      { artifact: "graph_v1", path: "$.graph", format: "extraction_graph" }
    );
    metadata.nested.score = 2;
    expect(sources[0]?.edge?.metadata).toEqual({ origin: "extractor", nested: { score: 1 } });
    expect(sources[0]?.nodes?.target).toBe(42);
    expect(sources[1]?.format).toBe("extraction_graph");
  });

  it("rejects invalid graph source sets", () => {
    expect(() => graphIndexSources({ artifact: "same" }, { artifact: "same" })).toThrow(
      /duplicate/
    );
    expect(() =>
      graphIndexSources({ artifact: "relations", edge: { weight: Number.NaN } })
    ).toThrow(/finite/);
    expect(() => graphIndexSources({ artifact: "relations", path: "$.relations[0]" })).toThrow(
      /path/
    );
    expect(() =>
      graphIndexSources({ artifact: "relations", nodes: { source: "{{ _doc.key }}" } } as never)
    ).toThrow(/not supported/);
    expect(() =>
      graphIndexSources({ artifact: "relations", nodes: { target: Number.POSITIVE_INFINITY } })
    ).toThrow(/nodes.target/);
    expect(() =>
      graphIndexSources({ artifact: "relations", edge: { type: true } } as never)
    ).toThrow(/string or finite number/);
    expect(() => graphIndexSources({ artifact: "relations", kind: "artifact" } as never)).toThrow(
      /not supported/
    );
  });
});
