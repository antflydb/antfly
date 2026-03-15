/**
 * Helper functions for constructing Bleve queries with type safety
 *
 * These helpers make it easier to build complex Bleve queries while
 * maintaining full TypeScript type checking.
 */

import type { components } from "./antfly-api.js";

type Query = components["schemas"]["Query"];
type ConjunctionQuery = components["schemas"]["ConjunctionQuery"];
type DisjunctionQuery = components["schemas"]["DisjunctionQuery"];

/**
 * Create a QueryString query - uses Bleve's query string syntax
 *
 * @example
 * queryString("body:computer AND category:technology")
 * queryString("title:golang", 1.5)
 */
export function queryString(query: string, boost?: number): Query {
  return {
    query,
    boost: boost ?? undefined,
  } as Query;
}

/**
 * Create a Term query - exact term match
 *
 * @example
 * term("published", "status")
 * term("draft", "status", 0.5)
 */
export function term(term: string, field?: string, boost?: number): Query {
  return {
    term,
    field,
    boost: boost ?? undefined,
  } as Query;
}

/**
 * Create a Match query - analyzed text match
 *
 * @example
 * match("golang tutorial")
 * match("computer science", "title", { fuzziness: "auto" })
 */
export function match(
  match: string,
  field?: string,
  options?: {
    analyzer?: string;
    boost?: number;
    prefix_length?: number;
    fuzziness?: number | "auto";
    operator?: "or" | "and";
  }
): Query {
  return {
    match,
    field,
    analyzer: options?.analyzer,
    boost: options?.boost,
    prefix_length: options?.prefix_length,
    fuzziness: options?.fuzziness,
    operator: options?.operator,
  } as Query;
}

/**
 * Create a MatchPhrase query - phrase match
 *
 * @example
 * matchPhrase("golang tutorial")
 * matchPhrase("distributed systems", "body")
 */
export function matchPhrase(
  matchPhrase: string,
  field?: string,
  options?: {
    analyzer?: string;
    boost?: number;
    fuzziness?: number | "auto";
  }
): Query {
  return {
    match_phrase: matchPhrase,
    field,
    analyzer: options?.analyzer,
    boost: options?.boost,
    fuzziness: options?.fuzziness,
  } as Query;
}

/**
 * Create a Prefix query - prefix match
 *
 * @example
 * prefix("comp", "title")
 */
export function prefix(prefix: string, field?: string, boost?: number): Query {
  return {
    prefix,
    field,
    boost: boost ?? undefined,
  } as Query;
}

/**
 * Create a Fuzzy query - fuzzy match
 *
 * @example
 * fuzzy("golang", "title", { fuzziness: 2 })
 */
export function fuzzy(
  term: string,
  field?: string,
  options?: {
    fuzziness?: number | "auto";
    prefix_length?: number;
    boost?: number;
  }
): Query {
  return {
    term,
    field,
    fuzziness: options?.fuzziness,
    prefix_length: options?.prefix_length,
    boost: options?.boost,
  } as Query;
}

/**
 * Create a NumericRange query
 *
 * @example
 * numericRange("price", { min: 0, max: 1000 })
 * numericRange("year", { min: 2020, inclusive_min: true })
 */
export function numericRange(
  field: string,
  options: {
    min?: number | null;
    max?: number | null;
    inclusive_min?: boolean | null;
    inclusive_max?: boolean | null;
    boost?: number;
  }
): Query {
  return {
    field,
    min: options.min,
    max: options.max,
    inclusive_min: options.inclusive_min,
    inclusive_max: options.inclusive_max,
    boost: options.boost,
  } as Query;
}

/**
 * Create a DateRange query
 *
 * @example
 * dateRange("created_at", {
 *   start: "2024-01-01T00:00:00Z",
 *   end: "2024-12-31T23:59:59Z"
 * })
 */
export function dateRange(
  field: string,
  options: {
    start?: string;
    end?: string;
    inclusive_start?: boolean | null;
    inclusive_end?: boolean | null;
    datetime_parser?: string;
    boost?: number;
  }
): Query {
  return {
    field,
    start: options.start,
    end: options.end,
    inclusive_start: options.inclusive_start,
    inclusive_end: options.inclusive_end,
    datetime_parser: options.datetime_parser,
    boost: options.boost,
  } as Query;
}

/**
 * Create a MatchAll query
 *
 * @example
 * matchAll()
 * matchAll(0.5)
 */
export function matchAll(boost?: number): Query {
  return {
    match_all: {},
    boost: boost ?? undefined,
  } as Query;
}

/**
 * Create a MatchNone query
 *
 * @example
 * matchNone()
 */
export function matchNone(boost?: number): Query {
  return {
    match_none: {},
    boost: boost ?? undefined,
  } as Query;
}

/**
 * Create a Boolean query - combine multiple queries with boolean logic
 *
 * @example
 * boolean({
 *   must: [
 *     term("published", "status"),
 *     match("technology", "category")
 *   ],
 *   mustNot: [
 *     term("archived", "status")
 *   ]
 * })
 */
export function boolean(options: {
  must?: Query[];
  should?: Query[];
  mustNot?: Query[];
  filter?: Query;
  boost?: number;
  minShouldMatch?: number;
}): Query {
  const result: {
    boost?: number;
    must?: ConjunctionQuery;
    should?: DisjunctionQuery;
    must_not?: DisjunctionQuery;
    filter?: Query;
  } = {
    boost: options.boost,
  };

  if (options.must && options.must.length > 0) {
    result.must = {
      conjuncts: options.must,
    };
  }

  if (options.should && options.should.length > 0) {
    result.should = {
      disjuncts: options.should,
      min: options.minShouldMatch,
    };
  }

  if (options.mustNot && options.mustNot.length > 0) {
    result.must_not = {
      disjuncts: options.mustNot,
    };
  }

  if (options.filter) {
    result.filter = options.filter;
  }

  return result as Query;
}

/**
 * Create a Conjunction query (AND) - all must match
 *
 * @example
 * conjunction([
 *   term("published", "status"),
 *   match("golang", "title")
 * ])
 */
export function conjunction(queries: Query[]): ConjunctionQuery {
  return {
    conjuncts: queries,
  };
}

/**
 * Create a Disjunction query (OR) - at least min must match
 *
 * @example
 * disjunction([
 *   term("draft", "status"),
 *   term("pending", "status")
 * ])
 *
 * // At least 2 must match
 * disjunction([...queries], 2)
 */
export function disjunction(queries: Query[], min?: number): DisjunctionQuery {
  return {
    disjuncts: queries,
    min,
  };
}

/**
 * Create a DocID query - match by document IDs
 *
 * @example
 * docIds(["doc1", "doc2", "doc3"])
 */
export function docIds(ids: string[], boost?: number): Query {
  return {
    ids,
    boost: boost ?? undefined,
  } as Query;
}

/**
 * Create a GeoDistance query
 *
 * @example
 * geoDistance("location", { lon: -122.4, lat: 37.8 }, "5km")
 */
export function geoDistance(
  field: string,
  location: { lon: number; lat: number },
  distance: string,
  boost?: number
): Query {
  return {
    field,
    location,
    distance,
    boost: boost ?? undefined,
  } as Query;
}

/**
 * Create a GeoBoundingBox query
 *
 * @example
 * geoBoundingBox("location", {
 *   top_left: { lon: -122.5, lat: 37.9 },
 *   bottom_right: { lon: -122.3, lat: 37.7 }
 * })
 */
export function geoBoundingBox(
  field: string,
  bounds: {
    top_left: { lon: number; lat: number };
    bottom_right: { lon: number; lat: number };
  },
  boost?: number
): Query {
  return {
    field,
    top_left: bounds.top_left,
    bottom_right: bounds.bottom_right,
    boost: boost ?? undefined,
  } as Query;
}
