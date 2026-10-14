import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { client, exists, queryAntfly } from "../server/antfly.ts";
import { governanceEmbedder as embedder } from "../server/governance-models.ts";
import {
  EVIDENCE,
  GOVERNANCE_META,
  GRAPH,
  EMBEDDINGS,
  readSnapshot,
  relations,
} from "../server/governance-data.ts";

const { data, id } = await readSnapshot();
for (const doc of data.documents.filter((d) => d.extraction === "pdf-text")) {
  const bytes = await readFile(`data/governance/sources/${doc.sha256}.pdf`);
  if (createHash("sha256").update(bytes).digest("hex") !== doc.sha256)
    throw new Error(`Checksum mismatch: ${doc.title}`);
}
const keyword = { type: "string", "x-antfly-types": ["keyword"] };
if (!(await exists(EVIDENCE)))
  await client.tables.create(EVIDENCE, {
    num_shards: 1,
    schema: {
      default_type: "evidence",
      document_schemas: {
        evidence: {
          schema: {
            type: "object",
            additionalProperties: true,
            properties: {
              search_text: { type: "string", "x-antfly-types": ["text"] },
              kind: keyword,
              snapshot_id: keyword,
              document_id: keyword,
            },
          },
        },
      },
    },
  });
if (!(await exists(GOVERNANCE_META)))
  await client.tables.create(GOVERNANCE_META, { num_shards: 1 });
let indexes = await client.indexes.list(EVIDENCE);
if (!indexes.some((i) => i.config.name === GRAPH))
  await client.indexes.create(EVIDENCE, GRAPH, {
    type: "graph",
    edge_types: relations.map((name) => ({
      name,
      field: name,
      topology: "graph",
    })),
  });
if (embedder && !indexes.some((i) => i.config.name === EMBEDDINGS))
  await client.indexes.create(EVIDENCE, EMBEDDINGS, {
    type: "embeddings",
    field: "search_text",
    embedder,
  });
const passages = new Map(data.passages.map((p) => [p.id, p]));
const records = data.nodes.map((node) => {
  const p = node.passage_id ? passages.get(node.passage_id) : undefined;
  const doc = p
    ? data.documents.find((d) => d.id === p.document_id)
    : undefined;
  const record: Record<string, unknown> = {
    ...node,
    ...p,
    snapshot_id: id,
    ...(doc
      ? {
          source_title: doc.title,
          source_url: doc.source_url,
          source_version: doc.version,
          source_sha256: doc.sha256,
          published: doc.published,
          review: doc.review,
        }
      : {}),
  };
  for (const relation of relations)
    record[relation] = (node[relation] || []).map(
      (target) => `${id}:${target}`,
    );
  return [`${id}:${node.id}`, record] as const;
});
for (const page of data.pages.filter((p) => p.caption)) {
  const doc = data.documents.find((d) => d.id === page.document_id)!;
  records.push([
    `${id}:${doc.sha256.slice(0, 16)}-p${page.page}-caption`,
    {
      kind: "caption",
      document_id: doc.id,
      page: page.page,
      caption: page.caption,
      search_text: page.caption,
      snapshot_id: id,
      source_sha256: doc.sha256,
      reader_model: page.reader_model,
      review: "machine-description-unreviewed",
    },
  ]);
}
for (let offset = 0; offset < records.length; offset += 100) {
  await client.tables.batch(EVIDENCE, {
    inserts: Object.fromEntries(records.slice(offset, offset + 100)),
  });
  console.log(
    `Indexed ${Math.min(offset + 100, records.length)} / ${records.length} evidence nodes`,
  );
}
let visible = false;
for (let attempt = 0; attempt < 60; attempt++) {
  const result = await queryAntfly({
    table: EVIDENCE,
    count: true,
    filter_query: { term: id, field: "snapshot_id" },
  });
  if (
    result?.hits?.total?.relation === "exact" &&
    result.hits.total.value === records.length
  ) {
    visible = true;
    break;
  }
  await new Promise((r) => setTimeout(r, 1000));
}
if (!visible)
  throw new Error(
    "New snapshot not yet fully searchable; prior manifest unchanged. Retry ingestion.",
  );
indexes = await client.indexes.list(EVIDENCE);
await client.tables.batch(GOVERNANCE_META, {
  inserts: {
    current: {
      snapshot_id: id,
      semantic: indexes.some((i) => i.config.name === EMBEDDINGS),
    },
  },
});
console.log(
  `Published ${id}. Embedding enrichment may still be running; the UI checks readiness.`,
);
