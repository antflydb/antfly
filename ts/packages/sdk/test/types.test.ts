/**
 * Type tests for Antfly query integration
 * These tests verify that the Antfly query types are properly integrated
 */
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, expectTypeOf, it } from "vitest";
import type {
  AntflyQuery,
  BooleanQuery,
  BoolFieldQuery,
  ConjunctionQuery,
  DisjunctionQuery,
  GraphIndexStats,
  GraphMetricRuntimeStats,
  MatchQuery,
  NumericRangeQuery,
  QueryRequest,
  QueryResult,
  QueryStringQuery,
  RowFilterEntry,
  RowOperation,
  RowsAggregateHaving,
  RowsAggregateHavingPredicate,
  RowsAggregatePlanRequest,
  RowsAggregateRequest,
  RowsArrayLengthProjection,
  RowsArrayUpdateTransform,
  RowsBatchRequest,
  RowsCoalesceFieldOperand,
  RowsCoalesceOperand,
  RowsCoalesceProjection,
  RowsCoalesceValueOperand,
  RowsConflictTarget,
  RowsDocKeyRange,
  RowsExpression,
  RowsExpressionAssignmentMap,
  RowsExpressionCaseBranch,
  RowsExpressionField,
  RowsExpressionOperator,
  RowsExpressionValue,
  RowsFieldAliasProjection,
  RowsFieldPatch,
  RowsGetRequest,
  RowsGetResult,
  RowsGetResultSet,
  RowsInsertSourceAssignment,
  RowsInsertSourceRequest,
  RowsJoinedMutationSourceAssignment,
  RowsJoinedMutationSourceRequest,
  RowsJoinPlanRequest,
  RowsJoinRequest,
  RowsJsonExtractProjection,
  RowsJsonSetTransform,
  RowsLateralPlanRequest,
  RowsLateralRequest,
  RowsMutationSourceRequest,
  RowsMutationSourceResultSet,
  RowsNumericIncrement,
  RowsOnConflict,
  RowsQueryOrderExpression,
  RowsQueryOrderField,
  RowsQueryRequest,
  RowsRowClaim,
  RowsUniquePredicateGroup,
  RowsWhere,
  RowsWhereAtom,
  RowsWhereBranchAll,
  RowsWhereBranchAtom,
  RowsWindowFrame,
  RowsWindowRequest,
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
  const match = generatedApi.match(/SortProfile: \{[\s\S]*?\n        \};/);
  if (!match) {
    throw new Error("generated SortProfile declaration not found");
  }
  return match[0];
}

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

  describe("Graph index stats", () => {
    it("should expose graph metric runtime telemetry", () => {
      const runtime: GraphMetricRuntimeStats = {
        enabled: true,
        role: "worker_pool",
        owner_id_hash: 17,
        worker_count: 3,
        takeover_count: 2,
        lost_leases: 1,
        total_pages_claimed: 6,
        last_pages_completed: 3,
        last_budget_exhausted: true,
      };
      const stats: GraphIndexStats = {
        index_type: "graph",
        total_edges: 4,
        graph_metric_runtime: runtime,
      };

      expect(stats.graph_metric_runtime?.role).toBe("worker_pool");
      expect(stats.graph_metric_runtime?.owner_id_hash).toBe(17);
      expectTypeOf(stats.graph_metric_runtime).toMatchTypeOf<GraphMetricRuntimeStats | undefined>();
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

  it("should expose relational get, ordering, expression, and operand schema variants", () => {
    const get: RowsGetRequest = {
      keys: [{ primary: { tenant_id: "t1", user_id: "u1" } }],
      include_physical_key: true,
    };
    const getRow: RowsGetResult = {
      identity: get.keys[0],
      found: true,
      row: { tenant_id: "t1", user_id: "u1", status: "Ready" },
      version: 7,
      physical_key: "row:t1:u1",
    };
    const resultSet: RowsGetResultSet = { rows: [getRow] };

    const fieldOrder: RowsQueryOrderField = { field: "status", direction: "asc" };
    const fieldExpr: RowsExpressionField = { field: "status", source: "row" };
    const valueExpr: RowsExpressionValue = { value: "ready" };
    const operatorExpr: RowsExpressionOperator = { op: "lower", args: [fieldExpr] };
    const exprOrder: RowsQueryOrderExpression = { expr: operatorExpr, direction: "desc" };
    const fieldOperand: RowsCoalesceFieldOperand = { field: "display_name" };
    const valueOperand: RowsCoalesceValueOperand = { value: "unknown" };
    const coalesceOperand: RowsCoalesceOperand = fieldOperand;
    const expression: RowsExpression = operatorExpr;

    expect(resultSet.rows?.[0]?.physical_key).toBe("row:t1:u1");
    expect(exprOrder.expr).toMatchObject({ op: "lower" });
    expect(valueExpr.value).toBe("ready");
    expect(coalesceOperand).toBe(fieldOperand);
    expect(valueOperand.value).toBe("unknown");
    expectTypeOf(fieldOrder).toMatchTypeOf<RowsQueryOrderField>();
    expectTypeOf(expression).toMatchTypeOf<RowsExpression>();
  });

  it("should create a framed typed window plan", () => {
    const frame: RowsWindowFrame = {
      unit: "rows",
      start: "offset_preceding",
      start_offset: 1,
      end: "offset_following",
      end_offset: 1,
    };
    const window: RowsWindowRequest = {
      source: { where: { field: "status", op: "eq", value: "open" } },
      windows: [
        {
          as: "last_amount",
          function: "last_value",
          expr: { field: "amount" },
          order_by: [{ field: "amount", direction: "desc" }],
          frame,
        },
        {
          as: "partition_count",
          function: "count",
          order_by: [{ field: "amount", direction: "desc" }],
          frame,
        },
      ],
      select: ["tenant_id", "id"],
    };

    expect(window.windows[0]?.frame?.end).toBe("offset_following");
    expect(window.windows[0]?.frame?.end_offset).toBe(1);
    expect(window.windows[1]?.function).toBe("count");
    expectTypeOf(window.windows[0]?.frame).toMatchTypeOf<RowsWindowFrame | undefined>();
  });

  it("should create an aggregate plan with typed having predicates", () => {
    const predicate: RowsAggregateHavingPredicate = {
      field: "amount_sum",
      op: "gt",
      value: 100,
    };
    const nullPredicate: RowsAggregateHavingPredicate = {
      field: "optional_amount",
      op: "is_not_distinct",
      value: null,
    };
    const having: RowsAggregateHaving = { all: [predicate, nullPredicate] };
    const aggregate: RowsAggregateRequest = {
      source: { where: { field: "status", op: "eq", value: "open" } },
      group_by: ["customer_id"],
      aggregations: [
        { name: "amount_sum", op: "sum", field: "amount" },
        {
          name: "recent_statuses",
          op: "array_agg",
          field: "status",
          filter_any: [{ all: [{ lhs: { field: "status" }, op: "eq", rhs: { value: "open" } }] }],
        },
      ],
      having,
      order_by: [{ field: "amount_sum", direction: "desc" }],
      limit: 10,
    };
    const plan: RowsAggregatePlanRequest = {
      ranges: [{ start: "row:a", end: "row:z" }],
      aggregate,
    };

    expect(plan.aggregate.having?.all[0]?.field).toBe("amount_sum");
    expect(plan.ranges?.[0]?.start).toBe("row:a");
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

  it("should create row composition plans with declared ranges", () => {
    const leftRange: RowsDocKeyRange = { start: "row:orders:", end: "row:orders;" };
    const rightRange: RowsDocKeyRange = { start: "row:customers:", end: "row:customers;" };
    const joinPlan: RowsJoinPlanRequest = {
      left_ranges: [leftRange],
      right_ranges: [rightRange],
      join: {
        left: {},
        right: {},
        on: [{ left_field: "customer_id", right_field: "id" }],
      },
    };
    const lateralPlan: RowsLateralPlanRequest = {
      left_ranges: [{ start: "row:org:", end: "row:org;" }],
      right_ranges: [{ start: "row:bal:", end: "row:bal;" }],
      lateral: {
        left: {},
        right: { limit: 1 },
        correlations: [{ left_field: "id", right_field: "organization_id" }],
      },
    };

    expect(joinPlan.left_ranges?.[0]?.end).toBe("row:orders;");
    expect(joinPlan.right_ranges?.[0]?.start).toBe("row:customers:");
    expect(lateralPlan.left_ranges?.[0]?.start).toBe("row:org:");
    expect(lateralPlan.right_ranges?.[0]?.end).toBe("row:bal;");
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

  it("should create canonical typed row predicates", () => {
    const statusAtom: RowsWhereAtom = { field: "status", op: "eq", value: "ready" };
    const anyBranch: RowsWhereBranchAtom = { field: "priority", op: "gt", value: 10 };
    const structuredAnyBranch: RowsWhereBranchAtom = {
      field: "tags",
      op: "array_contains",
      value: "paid",
    };
    const conjunctionBranch: RowsWhereBranchAll = {
      all: [
        { field: "tier", op: "eq", value: "enterprise" },
        { field: "metadata", op: "json_path_eq", path: ["billing", "plan"], value: "pro" },
      ],
    };
    const where: RowsWhere = {
      all: [
        statusAtom,
        { field: "tags", op: "array_contains", value: "paid" },
        { field: "metadata", op: "json_path_eq", path: ["billing", "plan"], value: "pro" },
        { field: "email", op: "text_pattern", pattern: "%@example.test", case_insensitive: true },
      ],
      any: [anyBranch, structuredAnyBranch, conjunctionBranch],
      not: [{ field: "tags", op: "array_contains", value: "cold" }],
    };
    const query: RowsQueryRequest = { where };

    expect(query.where?.all?.[1]?.op).toBe("array_contains");
    expect(query.where?.all?.[2]?.path).toEqual(["billing", "plan"]);
    expect(query.where?.any?.[1]?.op).toBe("array_contains");
    expect(query.where?.any?.[2]?.all?.[0]?.value).toBe("enterprise");
    expect(query.where?.any?.[2]?.all?.[1]?.path).toEqual(["billing", "plan"]);
    expect(query.where?.not?.[0]?.op).toBe("array_contains");
    expectTypeOf(anyBranch).toMatchTypeOf<RowsWhereBranchAtom>();
    expectTypeOf(structuredAnyBranch).toMatchTypeOf<RowsWhereBranchAtom>();
    expectTypeOf(conjunctionBranch).toMatchTypeOf<RowsWhereBranchAll>();
  });

  it("should expose row filter policy entries through the handwritten facade", () => {
    const filter: RowFilterEntry = {
      table: "usage_records",
      filter: { term: { tenant_id: "t1" } },
    };

    expect(filter.table).toBe("usage_records");
    expectTypeOf(filter.filter).toMatchTypeOf<Record<string, unknown>>();
  });

  it("should create a claimed mutation-source request and result", () => {
    const caseBranch: RowsExpressionCaseBranch = {
      when: { lhs: { field: "status" }, op: "eq", rhs: { value: "ready" } },
      // biome-ignore lint/suspicious/noThenProperty: This mirrors the generated CASE branch API field.
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
      json_set: [
        {
          field: "metadata",
          path: ["claim", "status_key"],
          expr: { op: "lower", args: [{ field: "status", source: "existing" }] },
        },
      ],
      returning: ["id", "status"],
      returning_expressions: [{ as: "status_label", expr: statusExpr }],
    };
    const result: RowsMutationSourceResultSet = {
      matched: 2,
      staged: 1,
      returning: [{ id: "r1", status: "claimed:ready" }],
    };

    expect(request.source.row_claim?.skip_locked).toBe(true);
    expect(request.json_set?.[0]?.expr).toMatchObject({ op: "lower" });
    expect(request.json_set?.[0]?.path).toEqual(["claim", "status_key"]);
    expect(range.end).toBe("row:z");
    expect(result.returning?.[0]?.status).toBe("claimed:ready");
  });

  it("should create a joined mutation-source request", () => {
    const assignment: RowsJoinedMutationSourceAssignment = {
      target_field: "quantity",
      side: "right",
      field: "source_quantity",
    };
    const request: RowsJoinedMutationSourceRequest = {
      op: "update",
      source_table: "source_records",
      target_side: "left",
      join: {
        left: {
          where: { field: "status", op: "eq", value: "ready" },
          row_claim: {
            mode: "for_update",
            owner_id: "worker:joined",
            transaction_id: "00112233445566778899aabbccddeeff",
            skip_locked: true,
          },
        },
        right: { where: { field: "source_status", op: "eq", value: "source" } },
        on: [{ left_field: "source_id", right_field: "source_pk" }],
        order_by: [{ field: "amount", direction: "desc" }],
        limit: 5,
      },
      source_assignments: [assignment],
      patch: { status: "synced" },
      patch_expr: {
        status_key: { op: "lower", args: [{ field: "status" }] },
      },
      returning: ["id", "quantity"],
      returning_expressions: [{ as: "status_key", expr: { field: "status_key" } }],
    };

    expect(request.source_table).toBe("source_records");
    expect(request.join.left.row_claim?.skip_locked).toBe(true);
    expect(request.source_assignments?.[0]?.field).toBe("source_quantity");
    expect(request.returning_expressions?.[0]?.as).toBe("status_key");
  });

  it("should create an insert-source request", () => {
    const assignments: RowsInsertSourceAssignment[] = [
      { target_field: "id", expr: { field: "source_id" } },
      { target_field: "status", expr: { op: "lower", args: [{ field: "status" }] } },
      { target_field: "amount", expr: { op: "add", args: [{ field: "amount" }, { value: 1 }] } },
    ];
    const request: RowsInsertSourceRequest = {
      op: "insert",
      source_table: "archived_records",
      source: {
        where: { field: "status", op: "eq", value: "ready" },
        order_by: [{ field: "amount", direction: "desc" }],
        limit: 5,
      },
      assignments,
      on_conflict: {
        target: { primary: true },
        action: "nothing",
      },
      returning: ["id", "status"],
      returning_expressions: [
        { as: "amount_plus_one", expr: { op: "add", args: [{ field: "amount" }, { value: 1 }] } },
      ],
    };

    expect(request.source_table).toBe("archived_records");
    expect(request.assignments[0]?.target_field).toBe("id");
    expect(request.on_conflict?.target.primary).toBe(true);
    expect(request.returning_expressions?.[0]?.as).toBe("amount_plus_one");
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
    const jsonSetExpr: RowsJsonSetTransform = {
      field: "metadata",
      path: ["billing", "status_key"],
      expr: { op: "lower", args: [{ field: "status", source: "proposed" }] },
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
      json_set: [jsonSet, jsonSetExpr],
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
    expect(batch.operations[0]?.on_conflict?.json_set?.[1]?.expr).toMatchObject({ op: "lower" });
    expect(batch.operations[0]?.on_conflict?.array_update?.[0]?.op).toBe("add_to_set");
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
