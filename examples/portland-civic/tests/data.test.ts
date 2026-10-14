import assert from "node:assert/strict";
import { test } from "node:test";
import { readFile } from "node:fs/promises";
import {
  date,
  normalizeHousing,
  normalizePermit,
  sourceLink,
} from "../scripts/normalize.ts";
import { buildQuery, readFilters } from "../server/search.ts";
import type { Manifest, Snapshot } from "../src/types.ts";

test("normalization keeps stable IDs, missing values, valid coordinates, and safe source links", () => {
  const p = normalizePermit({
    attributes: {
      FOLDERKEY: 123,
      HOUSE: "42",
      PROPSTREET: "MAIN",
      DESCRIPTION: "New\r\n apartment",
      CREATEDATE: Date.UTC(1899, 11, 30),
      SUBMITTEDVALUATION: null,
      NUMNEWUNITS: 0,
      PORTLAND_MAPS_URL: "javascript:alert(1)",
    },
    geometry: { x: -122.6, y: 45.5 },
  });
  assert.equal(p.id, "permit:123");
  assert.equal(p.description, "New apartment");
  assert.equal(p.created_at, undefined);
  assert.equal(p.valuation, undefined);
  assert.equal(p.new_units, 0);
  assert.deepEqual(p.location, { lon: -122.6, lat: 45.5 });
  assert.equal(
    p.source_url,
    "https://www.portlandmaps.com/detail/permit/123_did/",
  );
  assert.equal(sourceLink("https://attacker.example/"), undefined);
  assert.equal(date(null), undefined);
  assert.throws(() => normalizePermit({ attributes: {} }), /stable FOLDERKEY/);
});

test("housing context preserves zero values and partial status without inventing missing observations", () => {
  const h = normalizeHousing(
    {
      dataStatus: "partial",
      permitPipeline: [
        { date: "2026-02", value: 0 },
        { date: "2026-01", value: 1 },
        { date: "2026-03", value: null },
      ],
    },
    "2026-03-01T00:00:00Z",
  );
  assert.equal(h.data_status, "partial");
  assert.deepEqual(h.series[0].points, [
    { date: "2026-01", value: 1 },
    { date: "2026-02", value: 0 },
  ]);
  assert.deepEqual(h.series[1].points, []);
});

test("filters reject ambiguous types, invalid dates, reverse ranges and unbounded pagination", () => {
  for (const input of [
    { q: ["a", "b"] },
    { from: "2026-02-30" },
    { from: "2026-02-01", to: "2026-01-01" },
    { offset: "-1" },
    { offset: "Infinity" },
    { mode: "anything" },
  ])
    assert.throws(() => readFilters(input));
});

test("search keeps user values literal and pins all queries to a snapshot", () => {
  const m = { snapshot_id: "test-generation", semantic: true } as Manifest;
  const q = buildQuery(
    readFilters({
      q: "garage +status:all",
      neighborhood: 'x" OR *',
      from: "2025-01-01",
      to: "2025-12-31",
    }),
    m,
  );
  assert.deepEqual(q.full_text_search, {
    match: "garage +status:all",
    field: "search_text",
  });
  const filter = q.filter_query as { conjuncts: Record<string, unknown>[] };
  assert.deepEqual(filter.conjuncts[0], {
    field: "snapshot_id",
    term: "test-generation",
  });
  assert.deepEqual(filter.conjuncts[1], {
    field: "neighborhood",
    term: 'x" OR *',
  });
  assert.equal(filter.conjuncts[2].end, "2025-12-31T23:59:59.999Z");
  // count=true is a count-only API call and would suppress the actual permits.
  assert.notEqual(q.count, true);
  assert.ok(q.aggregations);
  const hybrid = buildQuery(
    readFilters({ q: "apartments", mode: "hybrid" }),
    m,
  );
  assert.equal(hybrid.semantic_search, "apartments");
  assert.equal(hybrid.aggregations, undefined);
  assert.throws(
    () =>
      buildQuery(readFilters({ mode: "hybrid" }), { ...m, semantic: false }),
    /no semantic index/,
  );
});

test("checked-in snapshot contains unique, source-linked building records and explicit sample coverage", async () => {
  const s: Snapshot = JSON.parse(
    await readFile(new URL("../data/sample.json", import.meta.url), "utf8"),
  );
  assert.equal(s.version, 1);
  assert.ok(s.source_count >= s.permits.length);
  assert.equal(new Set(s.permits.map((p) => p.id)).size, s.permits.length);
  for (const p of s.permits) {
    assert.ok(sourceLink(p.source_url));
    assert.ok(
      [
        "Residential 1 & 2 Family Permit",
        "Commercial Building Permit",
        "Facility Permit",
      ].includes(p.permit_type),
    );
    assert.ok(p.created_at && p.created_at >= s.since);
  }
  assert.match(s.selection, /sample/i);
});
