/**
 * Type tests for Antfly query integration
 * These tests verify that the Antfly query types are properly integrated
 */
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, expectTypeOf, it } from "vitest";
import type { GraphPathEdge } from "../src/index.js";
import type { components, operations } from "../src/public-api.js";
import { match as matchQuery } from "../src/query-helpers.js";
import type {
  AntflyQuery,
  BatchRequest,
  BooleanQuery,
  BoolFieldQuery,
  ConjunctionQuery,
  CreateIndexRequest,
  DisjunctionQuery,
  GraphAggregatesResult,
  GraphBindingsResult,
  GraphDocumentFilter,
  GraphMatchQuery,
  GraphNodesResult,
  LegacyGraphQueryResult,
  MatchQuery,
  NumericRangeQuery,
  QueryRequest,
  QueryResult,
  QueryStringQuery,
  SortProfile,
  TermQuery,
} from "../src/types.js";
import {
  formatQueryHitsTotal,
  queryHitsTotalIsExact,
  queryResultHitsTotal,
  queryResultTotalHits,
} from "../src/types.js";

const __dirname = dirname(fileURLToPath(import.meta.url));

function generatedSortProfileDeclaration(): string {
  const generatedApi = readFileSync(join(__dirname, "../src/public-api.d.ts"), "utf8");
  const match = generatedApi.match(/SortProfile: \{[\s\S]*?\n {8}\};/);
  if (!match) {
    throw new Error("generated SortProfile declaration not found");
  }
  return match[0];
}

describe("Antfly Query Type Integration", () => {
  describe("Backup metadata availability responses", () => {
    it("types both retryable 503 variants", () => {
      type BackupUnavailable = components["schemas"]["BackupMetadataUnavailableError"];
      type Backup503 = operations["backup"]["responses"][503]["content"]["application/json"];
      type BackupTable503 =
        operations["backupTable"]["responses"][503]["content"]["application/json"];
      type BackupTable409 =
        operations["backupTable"]["responses"][409]["content"]["application/json"];
      type ClusterBackup = components["schemas"]["ClusterBackupResponse"];

      const capability: BackupUnavailable = {
        code: "metadata_capability_unavailable",
        error: "metadata capability unavailable",
        message: "upgrade metadata nodes",
        required_capability: "linearizable_snapshot",
        retryable: true,
        retry_after_ms: 5000,
      };
      const leader: BackupUnavailable = {
        code: "metadata_leader_unavailable",
        error: "metadata leader unavailable",
        message: "metadata leader unavailable",
        retryable: true,
        retry_after_ms: 1000,
      };
      const ambiguous: BackupTable409 = {
        code: "backup_outcome_ambiguous",
        error: "backup outcome is ambiguous; inspect the backup id before retrying",
        message:
          "backup outcome is ambiguous; inspect the backup id and artifact id before retrying",
        retryable: false,
        backup_id: "snap",
        artifact_backup_id: "generation-7",
      };
      const ambiguousCluster: ClusterBackup = {
        backup_id: "nightly",
        status: "ambiguous",
        tables: [
          {
            name: "docs",
            status: "ambiguous",
            code: "backup_outcome_ambiguous",
            retryable: false,
            backup_id: "attempt-t-0",
            artifact_backup_id: "attempt-a-0",
          },
        ],
      };

      expectTypeOf<Backup503>().toEqualTypeOf<BackupUnavailable>();
      expectTypeOf<BackupTable503>().toEqualTypeOf<BackupUnavailable>();
      expect(capability.required_capability).toBe("linearizable_snapshot");
      expect(leader.error).toBe("metadata leader unavailable");
      expect(ambiguous.code).toBe("backup_outcome_ambiguous");
      expect(ambiguousCluster.tables[0]?.artifact_backup_id).toBe("attempt-a-0");
    });
  });

  describe("CreateIndexRequest type safety", () => {
    it("discriminates index-specific fields", () => {
      const embeddings: CreateIndexRequest = {
        type: "embeddings",
        dimension: 512,
      };
      const fullText: CreateIndexRequest = {
        type: "full_text",
        mem_only: true,
      };

      expect(embeddings.type).toBe("embeddings");
      expect(fullText.type).toBe("full_text");
    });

    it("rejects fields from a different index kind", () => {
      // @ts-expect-error dimension is embeddings-only.
      const invalid: CreateIndexRequest = { type: "full_text", dimension: 512 };
      expect(invalid.type).toBe("full_text");
    });

    it("types artifact graph mappings and algebraic planning", () => {
      const graph: CreateIndexRequest = {
        type: "graph",
        source: {
          kind: "artifact",
          artifact: "relations_v1",
          format: "extraction_graph",
        },
        artifact: {
          name: "relations_v1",
          kind: "asset",
          source: { type: "template", value: "{{ body }}" },
          execution: { batch_items: 8 },
        },
        nodes: {
          model: "document",
          source: "{{ _doc.key }}",
          target: "{{ _item.target.text }}",
        },
        edge: {
          type: "{{ _item.type }}",
          weight: 0.75,
          metadata: { source: "extractor" },
        },
        context: { doc_fields: ["title", "body"] },
        algebraic_planning: {
          bounded_traversal: {
            law: "provenance_semiring",
          },
        },
      };

      expect(graph.edge?.weight).toBe(0.75);
      expect(graph.artifact?.execution?.batch_items).toBe(8);
      expect(graph.algebraic_planning?.bounded_traversal?.law).toBe("provenance_semiring");
    });
  });

  describe("Batch transform types", () => {
    it("accepts the numeric $min transform operator", () => {
      const request: BatchRequest = {
        transforms: [
          {
            key: "doc-1",
            operations: [{ op: "$min", path: "priority", value: 4 }],
          },
        ],
      };

      expect(request.transforms?.[0]?.operations[0]?.op).toBe("$min");
    });
  });

  describe("QueryRequest type safety", () => {
    it("keeps graph filters in the stored-document predicate subset", () => {
      const filter: GraphDocumentFilter = { term: "active", field: "status" };
      const numeric: GraphDocumentFilter = {
        numeric_range: { field: "score", min: 0 },
      };
      const graph: GraphMatchQuery = {
        index: "social",
        match: { anchor: "person", nodes: { person: { filter } }, edges: [] },
        return: { aggregates: { count: { count: "*" } } },
      };
      const request: QueryRequest = { graph_queries: { people: graph } };

      expect(request.graph_queries?.people).toBeDefined();
      expect(numeric).toEqual({ numeric_range: { field: "score", min: 0 } });
    });

    it("types pre-discriminator graph responses during the compatibility window", () => {
      const legacy: LegacyGraphQueryResult = {
        type: "neighbors",
        total: 0,
      };

      expect(legacy.kind).toBeUndefined();
      expectTypeOf(legacy.kind).toEqualTypeOf<"legacy" | undefined>();
    });

    it("exports each canonical graph result variant", () => {
      const bindings: GraphBindingsResult = {
        kind: "bindings",
        rows: [],
        stats: { returned_items: 0, truncated: false },
        took: 0,
      };
      const aggregates: GraphAggregatesResult = {
        kind: "aggregates",
        aggregates: { count: { value: "0", exact: true } },
        stats: { returned_items: 1, truncated: false },
        took: 0,
      };
      const nodes: GraphNodesResult = {
        kind: "nodes",
        nodes: [],
        paths: [],
        stats: { returned_items: 0, truncated: false },
        took: 0,
      };

      expect(bindings.kind).toBe("bindings");
      expect(aggregates.aggregates.count?.exact).toBe(true);
      expect(nodes.kind).toBe("nodes");
    });

    it("exports table-qualified canonical path edges", () => {
      const edge: GraphPathEdge = {
        from: { key: "shared" },
        to: { key: "shared", table: "entities" },
        type: "references",
        weight: 1,
      };

      expect(edge.from.table).toBeUndefined();
      expect(edge.to.table).toBe("entities");
    });

    it("rejects analyzer-backed and full-text range shapes in graph filters", () => {
      // @ts-expect-error match requires a text index and is not a stored-document predicate.
      const analyzerBacked: GraphDocumentFilter = { match: "active", field: "status" };
      // @ts-expect-error graph ranges use an explicit operator wrapper.
      const ambiguousRange: GraphDocumentFilter = { field: "score", min: 0 };

      expect(analyzerBacked).toBeDefined();
      expect(ambiguousRange).toBeDefined();
    });

    it("should accept valid MatchQuery in full_text_search", () => {
      const query: QueryRequest = {
        table: "products",
        full_text_search: {
          match: "laptop",
          field: "name",
        } as MatchQuery,
        limit: 10,
      };

      expect(query.full_text_search).toBeDefined();
      expectTypeOf(query.full_text_search).toMatchTypeOf<AntflyQuery | undefined>();
    });

    it("should accept valid BooleanQuery in full_text_search", () => {
      const query: QueryRequest = {
        table: "products",
        full_text_search: {
          must: {
            conjuncts: [{ match: "laptop", field: "name" } as MatchQuery],
          },
          should: {
            disjuncts: [{ term: "gaming", field: "category" } as TermQuery],
          },
        } as BooleanQuery,
      };

      expect(query.full_text_search).toBeDefined();
      expectTypeOf(query.full_text_search).toMatchTypeOf<AntflyQuery | undefined>();
    });

    it("should accept valid query in filter_query", () => {
      const query: QueryRequest = {
        table: "products",
        filter_query: {
          min: 100,
          max: 1000,
          field: "price",
        } as NumericRangeQuery,
      };

      expect(query.filter_query).toBeDefined();
      expectTypeOf(query.filter_query).toMatchTypeOf<AntflyQuery | undefined>();
    });

    it("should accept valid query in exclusion_query", () => {
      const query: QueryRequest = {
        table: "products",
        exclusion_query: {
          term: "discontinued",
          field: "status",
        } as TermQuery,
      };

      expect(query.exclusion_query).toBeDefined();
      expectTypeOf(query.exclusion_query).toMatchTypeOf<AntflyQuery | undefined>();
    });
  });

  describe("SortProfile diagnostics", () => {
    it("keeps the public diagnostic surface closed to stable fields", () => {
      const profile: SortProfile = {
        plan: "native_doc_values_top_n",
        candidate_count: 7,
      };

      expect(profile.plan).toBe("native_doc_values_top_n");
      expectTypeOf(profile.plan).toEqualTypeOf<string | undefined>();
      expectTypeOf(profile.candidate_count).toEqualTypeOf<number | undefined>();

      const declaration = generatedSortProfileDeclaration();
      expect(declaration).toContain("plan?: string");
      expect(declaration).toContain("candidate_count?: number");
      expect(declaration).not.toContain("native_doc_value_load_us");
      expect(declaration).not.toContain("collector_heap_peak");
    });
  });

  describe("Query String Query", () => {
    it("should create valid query string query", () => {
      const query: QueryStringQuery = {
        query: "laptop AND (gaming OR professional)",
      };

      expect(query.query).toBe("laptop AND (gaming OR professional)");
    });

    it("should support boost parameter", () => {
      const query: QueryStringQuery = {
        query: "laptop",
        boost: 2.0,
      };

      expect(query.boost).toBe(2.0);
    });
  });

  describe("Match Query", () => {
    it("should create valid match query", () => {
      const query: MatchQuery = {
        match: "laptop",
        field: "name",
      };

      expect(query.match).toBe("laptop");
      expect(query.field).toBe("name");
    });

    it("should keep the match helper aligned with the generated schema", () => {
      const query = matchQuery("laptop computer", "description", {
        analyzer: "standard",
        boost: 1.5,
      });

      expectTypeOf(query).toEqualTypeOf<MatchQuery>();
      expect(query.analyzer).toBe("standard");
      expect(query.boost).toBe(1.5);
    });
  });

  describe("Boolean Query", () => {
    it("should create complex boolean query", () => {
      const query: BooleanQuery = {
        must: {
          conjuncts: [
            { match: "laptop", field: "name" } as MatchQuery,
            { bool: true, field: "in_stock" } as BoolFieldQuery,
          ],
        } as ConjunctionQuery,
        should: {
          disjuncts: [
            { term: "gaming", field: "category" } as TermQuery,
            { term: "professional", field: "category" } as TermQuery,
          ],
          min: 1,
        } as DisjunctionQuery,
        must_not: {
          disjuncts: [{ term: "discontinued", field: "status" } as TermQuery],
        } as DisjunctionQuery,
      };

      expect(query.must).toBeDefined();
      expect(query.should).toBeDefined();
      expect(query.must_not).toBeDefined();
    });

    it("should support boost in boolean query", () => {
      const query: BooleanQuery = {
        must: {
          conjuncts: [{ match: "laptop" } as MatchQuery],
        } as ConjunctionQuery,
        boost: 1.5,
      };

      expect(query.boost).toBe(1.5);
    });
  });

  describe("Numeric Range Query", () => {
    it("should create valid numeric range query", () => {
      const query: NumericRangeQuery = {
        min: 100,
        max: 1000,
        field: "price",
      };

      expect(query.min).toBe(100);
      expect(query.max).toBe(1000);
      expect(query.field).toBe("price");
    });

    it("should support inclusive bounds", () => {
      const query: NumericRangeQuery = {
        min: 100,
        max: 1000,
        inclusive_min: true,
        inclusive_max: false,
        field: "price",
      };

      expect(query.inclusive_min).toBe(true);
      expect(query.inclusive_max).toBe(false);
    });

    it("should allow null values", () => {
      const query: NumericRangeQuery = {
        min: null,
        max: 1000,
        field: "price",
      };

      expect(query.min).toBeNull();
    });
  });

  describe("Term Query", () => {
    it("should create valid term query", () => {
      const query: TermQuery = {
        term: "electronics",
        field: "category",
      };

      expect(query.term).toBe("electronics");
      expect(query.field).toBe("category");
    });

    it("should support boost", () => {
      const query: TermQuery = {
        term: "premium",
        field: "tags",
        boost: 2.0,
      };

      expect(query.boost).toBe(2.0);
    });
  });

  describe("Bool Field Query", () => {
    it("should create valid bool field query", () => {
      const query: BoolFieldQuery = {
        bool: true,
        field: "in_stock",
      };

      expect(query.bool).toBe(true);
      expect(query.field).toBe("in_stock");
    });

    it("should work with false value", () => {
      const query: BoolFieldQuery = {
        bool: false,
        field: "discontinued",
      };

      expect(query.bool).toBe(false);
    });
  });

  describe("Complex nested queries", () => {
    it("should support deeply nested boolean queries", () => {
      const query: QueryRequest = {
        table: "products",
        full_text_search: {
          must: {
            conjuncts: [
              {
                disjuncts: [
                  { match: "laptop", field: "name" } as MatchQuery,
                  { match: "notebook", field: "name" } as MatchQuery,
                ],
              } as DisjunctionQuery,
              {
                min: 500,
                max: 2000,
                field: "price",
              } as NumericRangeQuery,
            ],
          } as ConjunctionQuery,
        } as BooleanQuery,
        filter_query: {
          bool: true,
          field: "in_stock",
        } as BoolFieldQuery,
        limit: 50,
      };

      expect(query.full_text_search).toBeDefined();
      expect(query.filter_query).toBeDefined();
    });

    it("should combine multiple query types", () => {
      const query: QueryRequest = {
        table: "products",
        full_text_search: {
          query: "laptop OR desktop",
        } as QueryStringQuery,
        filter_query: {
          min: 500,
          field: "price",
        } as NumericRangeQuery,
        exclusion_query: {
          term: "refurbished",
          field: "condition",
        } as TermQuery,
        fields: ["name", "price", "category"],
        limit: 100,
        offset: 0,
      };

      expect(query.full_text_search).toBeDefined();
      expect(query.filter_query).toBeDefined();
      expect(query.exclusion_query).toBeDefined();
      expect(query.fields).toHaveLength(3);
    });
  });

  describe("QueryRequest with all features", () => {
    it("should create comprehensive query request", () => {
      const query: QueryRequest = {
        table: "products",
        full_text_search: {
          match: "laptop",
          field: "description",
        } as MatchQuery,
        semantic_search: "high-performance computing device",
        indexes: ["bleve_index", "vector_index"],
        filter_query: {
          min: 1000,
          max: 3000,
          field: "price",
        } as NumericRangeQuery,
        exclusion_query: {
          term: "discontinued",
          field: "status",
        } as TermQuery,
        fields: ["name", "price", "specs", "reviews"],
        limit: 50,
        offset: 10,
        order_by: { price: true, rating: false },
        count: true,
      };

      expect(query.table).toBe("products");
      expect(query.full_text_search).toBeDefined();
      expect(query.semantic_search).toBeDefined();
      expect(query.indexes).toHaveLength(2);
      expect(query.filter_query).toBeDefined();
      expect(query.exclusion_query).toBeDefined();
      expect(query.fields).toHaveLength(4);
      expect(query.limit).toBe(50);
      expect(query.offset).toBe(10);
      expect(query.order_by).toBeDefined();
      expect(query.count).toBe(true);
    });
  });
});

describe("Query total helpers", () => {
  it("preserves lower-bound total semantics for display", () => {
    const result: QueryResult = {
      hits: {
        total: { value: 42, relation: "gte" },
        hits: [],
      },
    };

    expect(queryResultHitsTotal(result)).toEqual({ value: 42, relation: "gte" });
    expect(queryHitsTotalIsExact(result.hits?.total)).toBe(false);
    expect(formatQueryHitsTotal(result.hits?.total)).toBe(">= 42 hits");
    expect(formatQueryHitsTotal({ value: 1, relation: "exact" })).toBe("1 hit");
    expect(queryResultTotalHits(result)).toBe(42);
  });
});
