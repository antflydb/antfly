import { mkdir, readFile, rename, writeFile } from "node:fs/promises";
import { dirname } from "node:path";
import { parseArgs } from "node:util";
import {
  FIELDS,
  HOUSING_SOURCE,
  normalizeHousing,
  normalizePermit,
  PERMIT_SOURCE,
  type Feature,
} from "./normalize.ts";
import type { Housing, Snapshot } from "../src/types.ts";

const { values } = parseArgs({
  options: {
    since: { type: "string", default: "2023-01-01" },
    limit: { type: "string", default: "2000" },
    out: { type: "string", default: "data/snapshot.json" },
  },
});
const since = values.since!;
if (
  !/^\d{4}-\d{2}-\d{2}$/.test(since) ||
  new Date(since).toISOString().slice(0, 10) !== since
)
  throw new Error("--since must be a valid YYYY-MM-DD date");
const limit = Number(values.limit);
if (!Number.isSafeInteger(limit) || limit < 0)
  throw new Error("--limit must be a nonnegative integer (0 means all)");
async function json(url: string) {
  for (let attempt = 0; ; attempt++) {
    const response = await fetch(url, { signal: AbortSignal.timeout(60_000) });
    if ((response.status === 429 || response.status >= 500) && attempt < 3) {
      await new Promise((r) => setTimeout(r, 1500 * 2 ** attempt));
      continue;
    }
    if (!response.ok) throw new Error(`${response.status}: ${url}`);
    const body = await response.json();
    if (body.error) throw new Error(JSON.stringify(body.error));
    return body;
  }
}
async function arcgis(params: Record<string, string>) {
  return json(
    `${PERMIT_SOURCE}/query?${new URLSearchParams({ f: "json", ...params })}`,
  );
}
await mkdir("data", { recursive: true });
const fetchedAt = new Date().toISOString();
const idResponse = await arcgis({
  where: `CREATEDATE >= DATE '${since}' AND PERMIT IN ('Residential 1 & 2 Family Permit', 'Commercial Building Permit', 'Facility Permit')`,
  returnIdsOnly: "true",
});
if (!Array.isArray(idResponse.objectIds))
  throw new Error("Source did not return permit IDs");
const ids: number[] = [...new Set<number>(idResponse.objectIds)].sort(
  (a, b) => b - a,
);
const selected = limit ? ids.slice(0, limit) : ids;
if (!selected.length)
  throw new Error("No permits in the requested coverage period");
const permits = new Map<string, ReturnType<typeof normalizePermit>>();
for (let offset = 0; offset < selected.length; offset += 200) {
  const page = await arcgis({
    objectIds: selected.slice(offset, offset + 200).join(","),
    outFields: FIELDS.join(","),
    outSR: "4326",
    returnGeometry: "true",
  });
  if (!Array.isArray(page.features) || page.exceededTransferLimit)
    throw new Error("Incomplete ArcGIS page; snapshot not published");
  if (page.features.length !== selected.slice(offset, offset + 200).length)
    throw new Error("Source changed during export; retry snapshot");
  for (const feature of page.features as Feature[]) {
    const p = normalizePermit(feature);
    if (permits.has(p.id))
      throw new Error(`Duplicate stable permit ID: ${p.id}`);
    permits.set(p.id, p);
  }
  console.log(`Fetched ${permits.size} / ${selected.length} permits`);
}
let housing: Housing | null = null;
try {
  try {
    housing = JSON.parse(await readFile("data/housing-cache.json", "utf8"));
  } catch {
    /* first fetch */
  }
  if (!housing || Date.now() - Date.parse(housing.fetched_at) >= 3_600_000) {
    // Civic Lab requests at most one poll per hour. Its response is cached on disk.
    housing = normalizeHousing(
      await json(HOUSING_SOURCE),
      new Date().toISOString(),
    );
    await writeFile(
      "data/housing-cache.json",
      JSON.stringify(housing, null, 2),
    );
  }
} catch (error) {
  console.warn(
    "Housing context unavailable; retaining cached context if present:",
    error,
  );
}
const snapshot: Snapshot = {
  version: 1,
  fetched_at: fetchedAt,
  since,
  source_url: PERMIT_SOURCE,
  source_count: ids.length,
  selection: `Residential 1 & 2 Family, Commercial Building, and Facility permits only. ${selected.length < ids.length ? `Bounded sample: highest ${selected.length} source OBJECTIDs among permits created since ${since}. Not a random or citywide sample.` : `All source IDs returned for permits created since ${since}.`}`,
  permits: [...permits.values()],
  housing,
};
await mkdir(dirname(values.out!), { recursive: true });
await writeFile(`${values.out}.tmp`, JSON.stringify(snapshot, null, 2) + "\n");
await rename(`${values.out}.tmp`, values.out!);
console.log(
  `Saved ${snapshot.permits.length} real permits to ${values.out} (${ids.length} source records match the period)`,
);
