/**
 * Normalizes simplified DSL queries (returned by the AI query builder agent)
 * into standard Bleve query format.
 *
 * The AI agent returns: { "and": [...] }, { "or": [...] }, { "not": {...} }
 * Bleve expects: { "conjuncts": [...] }, { "disjuncts": [...] }, { "must_not": { "disjuncts": [...] } }
 */

type QueryObject = Record<string, unknown>;

const MAX_DEPTH = 50;

export function normalizeSimplifiedDSL(query: unknown, depth = 0): QueryObject {
  if (depth > MAX_DEPTH) {
    return {};
  }

  if (!query || typeof query !== "object" || Array.isArray(query)) {
    return {};
  }

  const q = query as QueryObject;
  const result: QueryObject = {};
  const recurse = (v: unknown) => normalizeSimplifiedDSL(v, depth + 1);

  // Transform "and" → "conjuncts"
  if ("and" in q) {
    const children = Array.isArray(q.and) ? q.and.map(recurse) : [];
    return { ...copyExtraKeys(q, ["and"]), conjuncts: children };
  }

  // Transform "or" → "disjuncts"
  if ("or" in q) {
    const children = Array.isArray(q.or) ? q.or.map(recurse) : [];
    return { ...copyExtraKeys(q, ["or"]), disjuncts: children };
  }

  // Transform "not" → "must_not" wrapper
  if ("not" in q) {
    const inner = recurse(q.not);
    return { ...copyExtraKeys(q, ["not"]), must_not: { disjuncts: [inner] } };
  }

  // Recurse into known compound keys
  for (const key of Object.keys(q)) {
    const val = q[key];
    if (key === "conjuncts" || key === "disjuncts") {
      result[key] = Array.isArray(val) ? val.map(recurse) : val;
    } else if (key === "must" || key === "should" || key === "must_not") {
      result[key] = recurse(val);
    } else {
      result[key] = val;
    }
  }

  return result;
}

/** Copy all keys from obj except the ones in `exclude` */
function copyExtraKeys(obj: QueryObject, exclude: string[]): QueryObject {
  const result: QueryObject = {};
  for (const key of Object.keys(obj)) {
    if (!exclude.includes(key)) {
      result[key] = obj[key];
    }
  }
  return result;
}

/**
 * Checks if a query uses simplified DSL format (and/or/not keys)
 */
export function usesSimplifiedDSL(query: unknown): boolean {
  if (!query || typeof query !== "object" || Array.isArray(query)) return false;
  const q = query as QueryObject;
  if ("and" in q || "or" in q || "not" in q) return true;
  // Check nested
  for (const val of Object.values(q)) {
    if (Array.isArray(val)) {
      for (const item of val) {
        if (usesSimplifiedDSL(item)) return true;
      }
    } else if (typeof val === "object" && val !== null) {
      if (usesSimplifiedDSL(val)) return true;
    }
  }
  return false;
}
