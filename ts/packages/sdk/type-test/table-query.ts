import type { QueryRequest, TableQueryRequest } from "../src/types.js";

const tableQuery: TableQueryRequest = { limit: 10 };
void tableQuery;

// A table-scoped request has exactly one table authority: its route argument.
// @ts-expect-error table must not be repeated in the request body.
const ambiguousLiteral: TableQueryRequest = { table: "other", limit: 10 };
void ambiguousLiteral;

declare const globalQuery: QueryRequest;
// @ts-expect-error a global request may carry table, so callers must narrow it explicitly.
const ambiguousVariable: TableQueryRequest = globalQuery;
void ambiguousVariable;
