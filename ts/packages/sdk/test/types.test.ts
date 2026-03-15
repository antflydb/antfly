/**
 * Type tests for Bleve query integration
 * These tests verify that the Bleve query types are properly integrated
 */
import { describe, expect, expectTypeOf, it } from "vitest";
import type {
  BleveQuery,
  BooleanQuery,
  BoolFieldQuery,
  ConjunctionQuery,
  DisjunctionQuery,
  MatchQuery,
  NumericRangeQuery,
  QueryRequest,
  QueryStringQuery,
  TermQuery,
} from "../src/types.js";

describe("Bleve Query Type Integration", () => {
  describe("QueryRequest type safety", () => {
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
      expectTypeOf(query.full_text_search).toMatchTypeOf<BleveQuery | undefined>();
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
      expectTypeOf(query.full_text_search).toMatchTypeOf<BleveQuery | undefined>();
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
      expectTypeOf(query.filter_query).toMatchTypeOf<BleveQuery | undefined>();
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
      expectTypeOf(query.exclusion_query).toMatchTypeOf<BleveQuery | undefined>();
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

    it("should support fuzziness", () => {
      const query: MatchQuery = {
        match: "laptop",
        field: "name",
        fuzziness: "auto",
      };

      expect(query.fuzziness).toBe("auto");
    });

    it("should support operator", () => {
      const query: MatchQuery = {
        match: "laptop computer",
        field: "description",
        operator: "and",
      };

      expect(query.operator).toBe("and");
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
