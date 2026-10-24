import type { QueryRequest } from "@antfly/sdk";
import type { Filters, Manifest, Permit, SearchResult } from "../src/types.ts";
import { client, generator, METRICS, PERMITS, queryAntfly } from "./antfly.ts";

export const PAGE_SIZE = 40;
export class InputError extends Error {}
export function readFilters(params: Record<string, unknown>): Filters {
  const str = (key: string, max = 200) => {
    if (params[key] !== undefined && typeof params[key] !== "string")
      throw new InputError(`Invalid ${key}`);
    const s = String(params[key] || "").trim();
    if (s.length > max) throw new InputError(`${key} is too long`);
    return s;
  };
  const from = str("from"),
    to = str("to");
  for (const d of [from, to])
    if (
      d &&
      (!/^\d{4}-\d{2}-\d{2}$/.test(d) ||
        !Number.isFinite(Date.parse(d)) ||
        new Date(d).toISOString().slice(0, 10) !== d)
    )
      throw new InputError("Use valid YYYY-MM-DD dates");
  if (from && to && from > to)
    throw new InputError("Start date must precede end date");
  const offset = Number(params.offset || 0);
  if (!Number.isSafeInteger(offset) || offset < 0 || offset > 10000)
    throw new InputError("Invalid page offset");
  const mode = str("mode");
  if (mode && mode !== "keyword" && mode !== "hybrid")
    throw new InputError("Unknown search mode");
  return {
    q: str("q", 500),
    neighborhood: str("neighborhood", 500),
    status: str("status"),
    permit_type: str("permit_type"),
    from,
    to,
    mode: mode === "hybrid" ? "hybrid" : "keyword",
    offset,
  };
}
export function buildQuery(f: Filters, m: Manifest): QueryRequest {
  if (f.mode === "hybrid" && !m.semantic)
    throw new InputError(
      "This snapshot has no semantic index. Use keyword search.",
    );
  const conjuncts: Record<string, unknown>[] = [
    { term: m.snapshot_id, field: "snapshot_id" },
  ];
  for (const field of ["neighborhood", "status", "permit_type"] as const)
    if (f[field]) conjuncts.push({ term: f[field], field });
  if (f.from || f.to)
    conjuncts.push({
      field: "created_at",
      ...(f.from
        ? { start: `${f.from}T00:00:00Z`, inclusive_start: true }
        : {}),
      ...(f.to ? { end: `${f.to}T23:59:59.999Z`, inclusive_end: true } : {}),
    });
  const hybrid = f.mode === "hybrid" && Boolean(f.q);
  return {
    table: PERMITS,
    filter_query: { conjuncts },
    full_text_search: f.q
      ? { match: f.q, field: "search_text" }
      : { match_all: {} },
    ...(hybrid ? { semantic_search: f.q, indexes: ["permit_embeddings"] } : {}),
    limit: PAGE_SIZE,
    offset: f.offset,
    ...(!hybrid
      ? {
          aggregations: {
            neighborhoods: { type: "terms", field: "neighborhood", size: 6 },
          },
        }
      : {}),
  };
}
export async function manifest(): Promise<Manifest> {
  const m = (await client.tables.lookup(
    METRICS,
    "current",
  )) as unknown as Manifest;
  if (!m?.snapshot_id)
    throw new Error("No snapshot has been ingested. Run pnpm ingest first.");
  return m;
}
export async function search(f: Filters, m?: Manifest): Promise<SearchResult> {
  m ||= await manifest();
  const start = performance.now();
  const result = await queryAntfly(buildQuery(f, m), {
    signal: AbortSignal.timeout(30_000),
  });
  if (!result || result.error || result.status >= 400)
    throw new Error(result?.error || "Antfly returned no search result");
  const total = result.hits?.total;
  const hybrid = f.mode === "hybrid" && Boolean(f.q);
  // Some older Antfly releases returned a numeric total rather than {value, relation}.
  const count = typeof total === "number" ? total : total?.value || 0;
  return {
    permits: (result.hits?.hits || []).map(
      (h) => h._source as unknown as Permit,
    ),
    total: count,
    exact:
      !hybrid && (typeof total === "number" || total?.relation === "exact"),
    took: Math.round(performance.now() - start),
    scope: hybrid ? "ranked_candidates" : "all_matches",
    neighborhoods: result.aggregations?.neighborhoods?.buckets || [],
    manifest: m,
    generation_enabled: Boolean(generator),
  };
}
