/**
 * Type tests for Antfly query integration
 * These tests verify that the Antfly query types are properly integrated
 */
import { describe, expect, expectTypeOf, it } from "vitest";
import type {
  AntflyQuery,
  BooleanQuery,
  BoolFieldQuery,
  ConjunctionQuery,
  DisjunctionQuery,
  MatchQuery,
  NumericRangeQuery,
  QueryRequest,
  QueryStringQuery,
  RowOperation,
  RowsArrayUpdateTransform,
  RowsAggregateHaving,
  RowsAggregateHavingPredicate,
  RowsAggregatePlanRequest,
  RowsAggregateRequest,
  RowsArrayLengthProjection,
  RowsBatchRequest,
  RowsCoalesceOperand,
  RowsCoalesceProjection,
  RowsConflictTarget,
  RowsDocKeyRange,
  RowsExpression,
  RowsExpressionAssignmentMap,
  RowsExpressionCaseBranch,
  RowsFieldPatch,
  RowsFieldAliasProjection,
  RowsJoinRequest,
  RowsJoinPlanRequest,
  RowsJsonExtractProjection,
  RowsJsonSetTransform,
  RowsLateralRequest,
  RowsLateralPlanRequest,
  RowsMutationSourceRequest,
  RowsMutationSourceResultSet,
  RowsNumericIncrement,
  RowsOnConflict,
  RowsQueryRequest,
  RowsRowClaim,
  RowsUniquePredicateGroup,
  TermQuery,
} from "../src/types.js";

describe("Antfly Query Type Integration", () => {
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

describe("Relational row plan type integration", () => {
  it("should create a CTE-backed typed join plan", () => {
    const join: RowsJoinRequest = {
      left: { source_cte: "open_orders", select: ["tenant_id", "order_id"] },
      right: {
        where: { field: "kind", op: "eq", value: "customer" },
        expression_any: [{ field: "status", op: "eq", value: "active" }],
        select: ["tenant_id", "customer_id"],
      },
      on: [{ left_field: "tenant_id", right_field: "tenant_id" }],
      join_type: "left",
      select: [
        { as: "order_id", side: "left", field: "order_id" },
        { as: "customer_id", side: "right", field: "customer_id" },
      ],
      order_by: [{ field: "order_id", direction: "asc" }],
      limit: 25,
    };
    const plan: RowsJoinPlanRequest = {
      ctes: [
        {
          name: "open_orders",
          query: { where: { field: "status", op: "eq", value: "open" } },
        },
      ],
      join,
    };

    expect(plan.ctes?.[0]?.name).toBe("open_orders");
    expect(plan.join?.left.source_cte).toBe("open_orders");
    expect(plan.join?.select?.[1]?.side).toBe("right");
  });

  it("should create a bounded lateral row plan", () => {
    const lateral: RowsLateralRequest = {
      left: { source_cte: "tenants", select: ["tenant_id"] },
      right: {
        where: { field: "kind", op: "eq", value: "event" },
        order_by: [{ field: "created_at", direction: "desc" }],
        limit: 3,
      },
      correlations: [{ left_field: "tenant_id", right_field: "tenant_id" }],
      select: [
        { as: "tenant_id", side: "left", field: "tenant_id" },
        { as: "event_id", side: "right", field: "id" },
      ],
    };
    const plan: RowsLateralPlanRequest = {
      ctes: [{ name: "tenants", query: { select: ["tenant_id"] } }],
      lateral,
    };

    expect(plan.lateral?.right.limit).toBe(3);
    expect(plan.lateral?.correlations[0]?.right_field).toBe("tenant_id");
  });

  it("should create an aggregate plan with typed having predicates", () => {
    const predicate: RowsAggregateHavingPredicate = {
      field: "amount_sum",
      op: "gt",
      value: 100,
    };
    const having: RowsAggregateHaving = { all: [predicate] };
    const aggregate: RowsAggregateRequest = {
      source: { where: { field: "status", op: "eq", value: "open" } },
      group_by: ["customer_id"],
      aggregations: [{ name: "amount_sum", op: "sum", field: "amount" }],
      having,
      order_by: [{ field: "amount_sum", direction: "desc" }],
      limit: 10,
    };
    const plan: RowsAggregatePlanRequest = { aggregate };

    expect(plan.aggregate.having?.all[0]?.field).toBe("amount_sum");
    expectTypeOf(plan.aggregate.having?.all[0]?.op).toMatchTypeOf<
      | "is_null"
      | "is_not_null"
      | "is_distinct"
      | "is_not_distinct"
      | "eq"
      | "ne"
      | "gt"
      | "gte"
      | "lt"
      | "lte"
      | undefined
    >();
  });

  it("should create a row query with typed compact projections", () => {
    const jsonExtract: RowsJsonExtractProjection = {
      as: "plan",
      field: "metadata",
      path: ["billing", "plan"],
      as_text: true,
    };
    const arrayLength: RowsArrayLengthProjection = { as: "tag_count", field: "tags" };
    const coalesceOperand: RowsCoalesceOperand = { field: "display_name" };
    const coalesce: RowsCoalesceProjection = {
      as: "name_or_email",
      operands: [coalesceOperand, { field: "email" }, { value: "unknown" }],
    };
    const alias: RowsFieldAliasProjection = { as: "raw_id", field: "id" };
    const query: RowsQueryRequest = {
      select: ["id"],
      json_extract: [jsonExtract],
      array_length: [arrayLength],
      coalesce: [coalesce],
      field_aliases: [alias],
    };

    expect(query.json_extract?.[0]?.as_text).toBe(true);
    expect(query.array_length?.[0]?.field).toBe("tags");
    expect(query.coalesce?.[0]?.operands[2]?.value).toBe("unknown");
    expect(query.field_aliases?.[0]?.as).toBe("raw_id");
  });

  it("should create a claimed mutation-source request and result", () => {
    const caseBranch: RowsExpressionCaseBranch = {
      when: { lhs: { field: "status" }, op: "eq", rhs: { value: "ready" } },
      then: { value: "claimed:ready" },
    };
    const statusExpr: RowsExpression = { field: "status" };
    const caseExpr: RowsExpression = { op: "case", cases: [caseBranch], else: statusExpr };
    const rowClaim: RowsRowClaim = {
      mode: "for_update",
      owner_id: "worker:1",
      transaction_id: "00112233445566778899aabbccddeeff",
      skip_locked: true,
      lease_ms: 45000,
    };
    const range: RowsDocKeyRange = { start: "row:a", end: "row:z" };
    const request: RowsMutationSourceRequest = {
      op: "update",
      source: {
        where: { field: "status", op: "eq", value: "ready" },
        row_claim: rowClaim,
        order_by: [{ field: "created_at", direction: "asc" }],
        limit: 5,
      },
      patch_expr: {
        status: caseExpr,
      },
      returning: ["id", "status"],
      returning_expressions: [{ as: "status_label", expr: statusExpr }],
    };
    const result: RowsMutationSourceResultSet = {
      matched: 2,
      staged: 1,
      returning: [{ id: "r1", status: "claimed:ready" }],
    };

    expect(request.source.row_claim?.skip_locked).toBe(true);
    expect(range.end).toBe("row:z");
    expect(result.returning?.[0]?.status).toBe("claimed:ready");
  });

  it("should create row batch operations with typed conflict and transform plans", () => {
    const patch: RowsFieldPatch = { status: "active" };
    const increment: RowsNumericIncrement = { amount: 2.5 };
    const patchExpr: RowsExpressionAssignmentMap = {
      status_key: { op: "lower", args: [{ field: "status", source: "proposed" }] },
    };
    const jsonSet: RowsJsonSetTransform = {
      field: "metadata",
      path: ["billing", "plan"],
      value: "pro",
    };
    const arrayUpdate: RowsArrayUpdateTransform = {
      field: "tags",
      op: "add_to_set",
      value: "paid",
    };
    const partialTarget: RowsUniquePredicateGroup = {
      all: [{ field: "status", op: "eq", value: "active" }],
    };
    const target: RowsConflictTarget = {
      unique: { name: "usage_records_active_email_key", where: partialTarget },
    };
    const onConflict: RowsOnConflict = {
      target,
      action: "update",
      patch,
      increment,
      patch_expr: patchExpr,
      json_set: [jsonSet],
      array_update: [arrayUpdate],
      where_expression: {
        lhs: { field: "status", source: "proposed" },
        op: "is_not_null",
      },
    };
    const op: RowOperation = {
      op: "insert",
      row: { id: "u2", email: "ada@example.test", status: "active" },
      on_conflict: onConflict,
      returning: ["id", "status"],
      returning_expressions: [{ as: "status_label", expr: { field: "status" } }],
    };
    const batch: RowsBatchRequest = { operations: [op] };

    expect(batch.operations[0]?.on_conflict?.target.unique?.where?.all[0]?.field).toBe("status");
    expect(batch.operations[0]?.on_conflict?.json_set?.[0]?.path).toEqual(["billing", "plan"]);
    expect(batch.operations[0]?.on_conflict?.array_update?.[0]?.op).toBe("add_to_set");
  });
});
