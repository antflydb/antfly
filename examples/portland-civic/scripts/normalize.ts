import type { Housing, Permit, Point } from "../src/types.ts";

export const PERMIT_SOURCE =
  "https://www.portlandmaps.com/arcgis/rest/services/Public/BDS_Permit/FeatureServer/22";
export const HOUSING_SOURCE =
  "https://www.portlandciviclab.org/api/dashboard/housing";
export const FIELDS = [
  "OBJECTID",
  "FOLDERKEY",
  "APPLICATION",
  "HOUSE",
  "DIRECTION",
  "PROPSTREET",
  "STREETTYPE",
  "DESCRIPTION",
  "NEIGHBORHOOD",
  "PERMIT",
  "STATUS",
  "CREATEDATE",
  "ISSUED",
  "FINALED",
  "SUBMITTEDVALUATION",
  "NUMNEWUNITS",
  "PORTLAND_MAPS_URL",
];
export interface Feature {
  attributes: Record<string, unknown>;
  geometry?: { x: number; y: number };
}
const text = (v: unknown) =>
  typeof v === "string" ? v.replace(/\s+/g, " ").trim() : "";
const number = (v: unknown) =>
  typeof v === "number" && Number.isFinite(v) && v >= 0 ? v : undefined;
export function date(v: unknown): string | undefined {
  if (typeof v !== "number" || !Number.isFinite(v)) return undefined;
  const d = new Date(v);
  // ArcGIS sometimes returns sentinel dates in 1899.
  return d.getUTCFullYear() >= 1990 && v <= Date.now() + 86_400_000
    ? d.toISOString()
    : undefined;
}
export function sourceLink(v: unknown): string | undefined {
  try {
    const u = new URL(String(v));
    if (u.protocol === "https:" && u.hostname === "www.portlandmaps.com")
      return u.href;
  } catch {
    /* invalid source link */
  }
}
export function normalizePermit(feature: Feature): Permit {
  const a = feature.attributes;
  if (!Number.isSafeInteger(a.FOLDERKEY) || Number(a.FOLDERKEY) <= 0)
    throw new Error("Permit is missing its stable FOLDERKEY");
  const address = [a.HOUSE, a.DIRECTION, a.PROPSTREET, a.STREETTYPE]
    .map(text)
    .filter(Boolean)
    .join(" ");
  const p: Permit = {
    id: `permit:${a.FOLDERKEY}`,
    source_id: Number(a.FOLDERKEY),
    application: text(a.APPLICATION) || String(a.FOLDERKEY),
    address: address || "Address not recorded",
    description: text(a.DESCRIPTION),
    neighborhood: text(a.NEIGHBORHOOD) || "Not recorded",
    permit_type: text(a.PERMIT) || "Not recorded",
    status: text(a.STATUS) || "Not recorded",
    created_at: date(a.CREATEDATE),
    issued_at: date(a.ISSUED),
    finaled_at: date(a.FINALED),
    valuation: number(a.SUBMITTEDVALUATION),
    new_units: number(a.NUMNEWUNITS),
    source_url:
      sourceLink(a.PORTLAND_MAPS_URL) ||
      `https://www.portlandmaps.com/detail/permit/${a.FOLDERKEY}_did/`,
    search_text: "",
  };
  const g = feature.geometry;
  if (
    g &&
    Number.isFinite(g.x) &&
    Number.isFinite(g.y) &&
    Math.abs(g.x) <= 180 &&
    Math.abs(g.y) <= 90 &&
    (g.x !== 0 || g.y !== 0)
  )
    p.location = { lon: g.x, lat: g.y };
  p.search_text = [
    p.description,
    p.address,
    p.neighborhood,
    p.permit_type,
    p.application,
  ].join(". ");
  return p;
}
export function normalizeHousing(
  raw: Record<string, unknown>,
  fetchedAt: string,
): Housing {
  const specs = [
    ["permitPipeline", "Permits filed", "permits"],
    ["processingDays", "Average days to issue", "days"],
    ["medianRent", "Zillow rent index (ZORI)", "USD"],
  ];
  return {
    fetched_at: fetchedAt,
    source_url: HOUSING_SOURCE,
    data_status: text(raw.dataStatus) || "unknown",
    series: specs.map(([id, title, unit]) => ({
      id,
      title,
      unit,
      points: (Array.isArray(raw[id]) ? raw[id] : [])
        .filter((p): p is Point =>
          Boolean(
            p &&
              typeof p.date === "string" &&
              /^\d{4}-\d{2}(-\d{2})?$/.test(p.date) &&
              typeof p.value === "number" &&
              Number.isFinite(p.value),
          ),
        )
        .map((p) => ({ date: p.date, value: p.value }))
        .sort((a, b) => a.date.localeCompare(b.date)),
    })),
  };
}
