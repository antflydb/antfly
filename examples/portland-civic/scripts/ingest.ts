import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { parseArgs } from "node:util";
import {
  client,
  embedder,
  exists,
  METRICS,
  queryAntfly,
  PERMITS,
} from "../server/antfly.ts";
import type { Manifest, Snapshot } from "../src/types.ts";

const { values } = parseArgs({
  options: { file: { type: "string", default: "data/sample.json" } },
});
const raw = await readFile(values.file!, "utf8");
const snapshot: Snapshot = JSON.parse(raw);
if (
  snapshot.version !== 1 ||
  !Array.isArray(snapshot.permits) ||
  !snapshot.permits.length ||
  !snapshot.fetched_at ||
  snapshot.permits.some((p) => !p.id || !p.source_url || !p.search_text)
)
  throw new Error("Invalid or empty permit snapshot");
if (new Set(snapshot.permits.map((p) => p.id)).size !== snapshot.permits.length)
  throw new Error("Duplicate permit IDs in snapshot");
const snapshotId = createHash("sha256").update(raw).digest("hex").slice(0, 16);
const keyword = { type: "string", "x-antfly-types": ["keyword"] };
if (!(await exists(PERMITS)))
  await client.tables.create(PERMITS, {
    num_shards: 1,
    schema: {
      default_type: "permit",
      document_schemas: {
        permit: {
          schema: {
            type: "object",
            additionalProperties: true,
            properties: {
              search_text: { type: "string", "x-antfly-types": ["text"] },
              snapshot_id: keyword,
              neighborhood: keyword,
              permit_type: keyword,
              status: keyword,
              created_at: {
                type: "string",
                format: "date-time",
                "x-antfly-types": ["datetime"],
              },
            },
          },
        },
      },
    },
    indexes: embedder
      ? {
          permit_embeddings: {
            type: "embeddings",
            field: "search_text",
            embedder,
          },
        }
      : undefined,
  });
if (!(await exists(METRICS)))
  await client.tables.create(METRICS, { num_shards: 1 });
let indexes = await client.indexes.list(PERMITS);
if (embedder && !indexes.some((i) => i.config.name === "permit_embeddings")) {
  await client.indexes.create(PERMITS, "permit_embeddings", {
    type: "embeddings",
    field: "search_text",
    embedder,
  });
  indexes = await client.indexes.list(PERMITS);
}
// A generation prefix lets the current manifest remain readable until every batch succeeds.
// Reimporting an identical file is idempotent; older generations can be retained for demos.
for (let offset = 0; offset < snapshot.permits.length; offset += 100) {
  const inserts = Object.fromEntries(
    snapshot.permits
      .slice(offset, offset + 100)
      .map((p) => [`${snapshotId}:${p.id}`, { ...p, snapshot_id: snapshotId }]),
  );
  await client.tables.batch(PERMITS, { inserts });
  console.log(
    `Indexed ${Math.min(offset + 100, snapshot.permits.length)} / ${snapshot.permits.length}`,
  );
}
const semantic = indexes.some((i) => i.config.name === "permit_embeddings");
const observations: Record<string, unknown> = {};
for (const series of snapshot.housing?.series || [])
  for (const p of series.points) {
    observations[`${snapshotId}:housing:${series.id}:${p.date}`] = {
      topic: "housing",
      metric: series.id,
      title: series.title,
      unit: series.unit,
      date: p.date,
      value: p.value,
      snapshot_id: snapshotId,
      source_url: snapshot.housing!.source_url,
      fetched_at: snapshot.housing!.fetched_at,
      data_status: snapshot.housing!.data_status,
      geography: "Upstream dashboard coverage; see source methodology",
    };
  }
const entries = Object.entries(observations);
for (let i = 0; i < entries.length; i += 100)
  await client.tables.batch(METRICS, {
    inserts: Object.fromEntries(entries.slice(i, i + 100)),
  });
const unique = (field: "neighborhood" | "status" | "permit_type") =>
  [...new Set(snapshot.permits.map((p) => p[field]))].sort();
// Publish only after every record in this generation is searchable.
let visible = false;
for (let attempt = 0; attempt < 30; attempt++) {
  const result = await queryAntfly({
    table: PERMITS,
    count: true,
    filter_query: { term: snapshotId, field: "snapshot_id" },
  });
  if (
    result?.hits?.total?.relation === "exact" &&
    result.hits.total.value === snapshot.permits.length
  ) {
    visible = true;
    break;
  }
  await new Promise((resolve) => setTimeout(resolve, 1000));
}
if (!visible)
  throw new Error(
    "Snapshot indexing has not become visible; the previous manifest is unchanged. Retry ingestion.",
  );
const { permits, ...metadata } = snapshot;
const manifest: Manifest = {
  ...metadata,
  snapshot_id: snapshotId,
  imported_count: permits.length,
  semantic,
  neighborhoods: unique("neighborhood"),
  statuses: unique("status"),
  permit_types: unique("permit_type"),
};
await client.tables.batch(METRICS, { inserts: { current: manifest } });
console.log(
  `Published snapshot ${snapshotId}. Search mode: ${semantic ? "hybrid available (wait for embedding enrichment)" : "keyword"}.`,
);
