import { createHash } from "node:crypto";
import { writeFile, rename, mkdir } from "node:fs/promises";
import { client, exists, baseUrl, headers } from "../server/antfly.ts";
import { readSnapshot, GOVERNANCE_META } from "../server/governance-data.ts";
import {
  extractionModel,
  extractionSchemaVersion,
  extractionSchema,
  inference,
} from "../server/governance-models.ts";
import { cachedInference } from "../server/governance-extraction.ts";
import { normalizeSpan } from "../server/governance-spans.ts";

const TABLE = "oregon_governance_mentions_v1",
  ENTITIES = "oregon_governance_entities_v1",
  GRAPH = "autograph_v1";
const { data, id } = await readSnapshot();
type Entity = {
  label: string;
  text: string;
  start: number;
  end: number;
  score?: number;
};
type Endpoint = Partial<Entity> & { entity_index?: number; id?: string };
type Relation = {
  type: string;
  source: Endpoint;
  target: Endpoint;
  score?: number;
};
type Extraction = { entities: Entity[]; relations?: Relation[] };
let completed = 0;
async function status(state: string, error?: string) {
  await mkdir("data/governance/cache", { recursive: true });
  const record = {
    state,
    model: extractionModel,
    snapshot_id: id,
    completed,
    total: data.passages.length,
    error,
    updated_at: new Date().toISOString(),
  };
  await writeFile(
    "data/governance/cache/extraction-status.json.partial",
    JSON.stringify(record),
  );
  await rename(
    "data/governance/cache/extraction-status.json.partial",
    "data/governance/cache/extraction-status.json",
  );
  if (await exists(GOVERNANCE_META))
    await client.tables.batch(GOVERNANCE_META, {
      inserts: { extraction: record },
    });
}
try {
  for (const table of [TABLE, ENTITIES])
    if (!(await exists(table)))
      await client.tables.create(table, { num_shards: 1 });
  const indexes = await client.indexes.list(TABLE);
  if (!indexes.some((i) => i.config.name === GRAPH))
    await client.indexes.create(TABLE, GRAPH, {
      type: "graph",
      source: {
        artifact: "extraction_v1",
        path: "$.relations[*]",
        format: "extraction_relation",
        mention_edge_type: "mentions",
      },
      artifact: {
        name: "extraction_v1",
        kind: "asset",
        source: { type: "field", value: "extraction" },
        content_type: "application/json",
      },
      edge_types: [
        { name: "mentions" },
        ...extractionSchema.relations.map((r) => ({ name: r.type })),
      ],
      resolvers: [
        {
          name: "oregon",
          table: ENTITIES,
          source_artifact: "extraction_v1",
          resolution_artifact: "resolution_v1",
          key_template:
            "oregon/2017R1/{{ lower _entity.label }}/{{ slug _entity.text }}",
          candidate_search: "prefix",
          config_generation: 1,
        },
      ],
    });
  await status("running");
  for (let offset = 0; offset < data.passages.length; offset += 8) {
    const batch = data.passages.slice(offset, offset + 8);
    const request = {
      schema_version: extractionSchemaVersion,
      model: extractionModel,
      inputs: batch.map((passage) => ({
        id: passage.id,
        content: passage.text,
      })),
      schema: extractionSchema,
      options: {
        include_spans: true,
        include_confidence: true,
        ...(extractionSchemaVersion === 2
          ? { offset_unit: "utf16_codeunits" }
          : {}),
        threshold: 0.6,
      },
    };
    const response = await cachedInference("native-gliner-v1", request, () =>
      inference<{ data: Extraction[] }>("extract", request),
    );
    if (response.data?.length !== batch.length)
      throw new Error("Extraction returned an incomplete batch");
    for (const [position, passage] of batch.entries()) {
      const extraction = structuredClone(response.data[position]);
      if (!extraction || !Array.isArray(extraction.entities))
        throw new Error(`Missing extraction result: ${passage.id}`);
      // GLiNER2's v1 API reports UTF-8 bytes. Citation slices use JS UTF-16.
      // Decode only exact scalar boundaries; never silently repair bad offsets.
      extraction.entities = extraction.entities.map((entity) =>
        normalizeSpan(
          passage.text,
          entity,
          extractionSchemaVersion === 1 ? "utf8_bytes" : "utf16_codeunits",
        ),
      );
      const doc = data.documents.find((d) => d.id === passage.document_id)!;
      const normalized = {
        ...extraction,
        entities: extraction.entities.map((entity, i) => ({
          ...entity,
          id: `e${i}`,
        })),
      };
      const resolveEndpoint = (endpoint: Endpoint) => {
        if (endpoint.entity_index !== undefined) {
          if (!normalized.entities[endpoint.entity_index])
            throw new Error("Invalid relation endpoint");
          return { ...endpoint, id: `e${endpoint.entity_index}` };
        }
        if (
          typeof endpoint.start !== "number" ||
          typeof endpoint.end !== "number" ||
          passage.text.slice(endpoint.start, endpoint.end) !== endpoint.text
        )
          throw new Error("Unanchored relation endpoint");
        const i = normalized.entities.findIndex(
          (e) =>
            e.start === endpoint.start &&
            e.end === endpoint.end &&
            e.label === endpoint.label,
        );
        if (i >= 0) return { ...endpoint, id: `e${i}` };
        const entity = {
          ...endpoint,
          label: endpoint.label || "mention",
          id: `e${normalized.entities.length}`,
        } as Entity & { id: string };
        normalized.entities.push(entity);
        return { ...endpoint, id: entity.id };
      };
      normalized.relations = (extraction.relations || [])
        .filter((r) => (r.score ?? 0) >= 0.6)
        .map((r) => ({
          ...r,
          source: resolveEndpoint(r.source),
          target: resolveEndpoint(r.target),
        }));
      const provenance = {
        snapshot_id: id,
        passage_id: passage.id,
        document_id: doc.id,
        source_sha256: doc.sha256,
        source_version: doc.version,
        published: doc.published,
        page: passage.page,
        start: passage.start,
        end: passage.end,
        text: passage.text,
        extractor: extractionModel,
        extraction_schema_version: extractionSchemaVersion,
        offset_unit: "utf16_codeunits",
        schema_sha256: createHash("sha256")
          .update(JSON.stringify(extractionSchema))
          .digest("hex"),
        review: "machine-extracted-unreviewed",
        interpretation:
          "Candidate relationships asserted in this source version; not verified real-world outcomes.",
      };
      const inserts: Record<string, Record<string, unknown>> = {
        [`${id}:${passage.id}`]: {
          ...provenance,
          record_type: "passage",
          // The graph materializer owns edges by their source document. A
          // relation is therefore a cited assertion node, not an edge whose
          // local endpoint ID could be mistaken for a global document key.
          extraction: { ...normalized, relations: [] },
          candidate_relations: normalized.relations,
        },
      };
      for (const [i, relation] of normalized.relations.entries()) {
        const subject = normalized.entities.find(
          (e) => e.id === relation.source.id,
        )!;
        const object = normalized.entities.find(
          (e) => e.id === relation.target.id,
        )!;
        inserts[`${id}:${passage.id}:relation:${i}`] = {
          ...provenance,
          record_type: "relation",
          relation_type: relation.type,
          subject,
          object,
          score: relation.score,
          extraction: {
            entities: [
              { ...subject, id: "subject" },
              { ...object, id: "object" },
            ],
            relations: [],
          },
        };
      }
      const previous = await fetch(
        `${baseUrl}/db/v1/tables/${TABLE}/documents/${encodeURIComponent(`${id}:${passage.id}`)}`,
        {
          headers,
          signal: AbortSignal.timeout(10_000),
        },
      );
      if (!previous.ok && previous.status !== 404)
        throw new Error(
          `Previous extraction lookup failed: ${previous.status}`,
        );
      const oldCount = previous.ok
        ? (await previous.json()).candidate_relations?.length || 0
        : 0;
      const relationCount = normalized.relations.length;
      const deletes = Array.from(
        { length: Math.max(0, oldCount - relationCount) },
        (_, i) => `${id}:${passage.id}:relation:${relationCount + i}`,
      );
      await client.tables.batch(TABLE, { inserts, deletes });
      completed++;
      if (completed % 25 === 0) {
        await status("running");
        console.log(`Extracted ${completed}/${data.passages.length} passages`);
      }
    }
  }
  await status("extracted");
  console.log(
    "Native extraction finished. Autograph resolution may still be indexing; check /api/governance/pipeline.",
  );
} catch (error) {
  await status("blocked", String(error));
  throw error;
}
