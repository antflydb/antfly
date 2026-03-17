/**
 * Examples demonstrating type-safe Bleve query construction
 * This file shows how to use the integrated Bleve query types
 */

import type {
  BooleanQuery,
  BoolFieldQuery,
  ConjunctionQuery,
  DisjunctionQuery,
  MatchQuery,
  NumericRangeQuery,
  QueryRequest,
  TermQuery,
} from "../src/types.js";

// Example 1: Simple match query
const simpleMatchQuery: QueryRequest = {
  table: "products",
  full_text_search: {
    match: "laptop",
    field: "name",
  } as MatchQuery,
  limit: 10,
};

// Example 2: Boolean query with must/should/must_not
const booleanQuery: QueryRequest = {
  table: "products",
  full_text_search: {
    must: {
      conjuncts: [
        { match: "laptop", field: "name" } as MatchQuery,
        { bool: true, field: "in_stock" } as BoolFieldQuery,
      ],
    },
    should: {
      disjuncts: [
        { term: "premium", field: "category" } as TermQuery,
        { term: "pro", field: "tags" } as TermQuery,
      ],
      min: 1,
    },
    must_not: {
      disjuncts: [{ term: "refurbished", field: "condition" } as TermQuery],
    },
  } as BooleanQuery,
  limit: 20,
};

// Example 3: Query with filters
const filteredQuery: QueryRequest = {
  table: "products",
  full_text_search: {
    match: "laptop",
    field: "description",
  } as MatchQuery,
  filter_query: {
    min: 500,
    max: 2000,
    field: "price",
  } as NumericRangeQuery,
  exclusion_query: {
    term: "discontinued",
    field: "status",
  } as TermQuery,
  limit: 50,
};

// Example 4: Complex nested boolean query
const complexQuery: QueryRequest = {
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
          min: 1000,
          max: 3000,
          field: "price",
        } as NumericRangeQuery,
      ],
    } as ConjunctionQuery,
    should: {
      disjuncts: [
        { term: "gaming", field: "category" } as TermQuery,
        { term: "professional", field: "category" } as TermQuery,
      ],
      min: 1,
    } as DisjunctionQuery,
  } as BooleanQuery,
  fields: ["name", "price", "category", "specs"],
  limit: 100,
};

// Example 5: Query string query (simplest form)
const queryStringQuery: QueryRequest = {
  table: "products",
  full_text_search: {
    query: "laptop AND (gaming OR professional) -discontinued",
  },
  limit: 25,
};

// Export examples for use in other files
export { booleanQuery, complexQuery, filteredQuery, queryStringQuery, simpleMatchQuery };
