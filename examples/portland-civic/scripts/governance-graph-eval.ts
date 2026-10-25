import assert from "node:assert/strict";
import { client, queryAntfly } from "../server/antfly.ts";
import { readSnapshot } from "../server/governance-data.ts";
import { extractionModel } from "../server/governance-models.ts";

const { data, id } = await readSnapshot();
const table = "oregon_governance_mentions_v1";
const assertions = await queryAntfly({
  table,
  filter_query: {
    conjuncts: [
      { term: id, field: "snapshot_id" },
      { term: "relation", field: "record_type" },
    ],
  },
  limit: 10,
});
assert.ok(!assertions?.error, assertions?.error);
assert.ok(
  assertions?.hits?.hits?.length,
  "No native relation assertions indexed",
);
for (const hit of assertions.hits.hits) {
  const record = hit._source!;
  const passage = data.passages.find((p) => p.id === record.passage_id)!;
  assert.ok(passage);
  assert.equal(record.extractor, extractionModel);
  assert.ok(Number(record.score) >= 0.6);
  for (const name of ["subject", "object"]) {
    const entity = record[name] as { text: string; start: number; end: number };
    assert.equal(passage.text.slice(entity.start, entity.end), entity.text);
  }
  const result = await queryAntfly({
    table,
    graph_queries: {
      entities: {
        index: "autograph_v1",
        traverse: {
          start: { keys: [hit._id] },
          direction: "out",
          max_depth: 1,
          limit: 10,
          include_paths: true,
          include_documents: true,
        },
      },
    },
  });
  assert.ok(!result?.error, result?.error);
  const graph = result?.graph_results?.entities;
  const nodes = graph?.kind === "nodes" ? graph.nodes : [];
  assert.ok(nodes.length, "Assertion has no canonical entity links");
  for (const node of nodes) {
    assert.match(node.key, /^oregon\/2017R1\//);
    assert.ok(
      node.document?.canonical_name,
      "Canonical entity was not promoted",
    );
    assert.ok(node.path_edges?.every((edge) => edge.type === "mentions"));
  }
}
console.log(
  "Native GLiNER2 relation assertions, exact mention spans and canonical Autograph traversal passed.",
);
