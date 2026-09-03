import type { CreateIndexRequest } from "../src/types.js";

const graph: CreateIndexRequest = {
  type: "graph",
  source: {
    artifact: "relations_v1",
    format: "extraction_graph",
    nodes: {
      model: "document",
      target: "{{ _item.target.text }}",
    },
    edge: {
      type: "{{ _item.type }}",
      weight: 0.75,
      metadata: { source: "extractor" },
    },
    context: { doc_fields: ["title", "body"] },
  },
};
void graph;

const graphSourceWithKind: CreateIndexRequest = {
  type: "graph",
  source: {
    artifact: "relations_v1",
    // @ts-expect-error Graph artifact sources do not use a redundant kind discriminator.
    kind: "artifact",
  },
};
void graphSourceWithKind;

const graphWithRootMapping: CreateIndexRequest = {
  type: "graph",
  // @ts-expect-error Graph mappings are scoped to each source.
  nodes: { model: "document" },
};
void graphWithRootMapping;
