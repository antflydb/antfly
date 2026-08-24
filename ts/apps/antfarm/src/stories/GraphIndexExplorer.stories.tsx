import type { AntflyClient, GraphQueryResult, IndexStatus, QueryRequest } from "@antfly/sdk";
import type { Meta, StoryObj } from "@storybook/react-vite";
import { GraphIndexExplorer } from "../components/GraphIndexExplorer";
import { ApiConfigContext } from "../contexts/api-config-context";

const documents = {
  "paper:graph-rag": {
    title: "Graph-Aware Retrieval",
    author: "Antfly Labs",
    year: 2026,
    topic: "retrieval",
  },
  "paper:vector-db": {
    title: "Vector Index Internals",
    author: "M. Chen",
    year: 2025,
    topic: "indexing",
  },
  "paper:path-ranking": {
    title: "Weighted Path Ranking",
    author: "R. Patel",
    year: 2024,
    topic: "graph algorithms",
  },
  "paper:agent-memory": {
    title: "Agent Memory Graphs",
    author: "S. Okafor",
    year: 2026,
    topic: "agents",
  },
  "paper:entity-links": {
    title: "Entity Linking at Scale",
    author: "L. Torres",
    year: 2023,
    topic: "knowledge graphs",
  },
};

const graphIndex = {
  config: {
    name: "citation_graph",
    type: "graph",
    edge_types: [
      { name: "cites", source_field: "id", target_field: "references", weight_field: "score" },
      { name: "extends", source_field: "id", target_field: "related", weight_field: "score" },
      { name: "mentions", source_field: "id", target_field: "entities", weight_field: "score" },
    ],
    max_edges_per_document: 64,
    ttl_duration: "30d",
  },
  status: {
    total_edges: 1842,
    edge_types: {
      cites: 912,
      extends: 385,
      mentions: 545,
    },
    rebuilding: false,
  },
} as unknown as IndexStatus;

const traversalResult: GraphQueryResult = {
  kind: "nodes",
  nodes: [
    {
      key: "paper:vector-db",
      depth: 1,
      distance: 0.92,
      document: documents["paper:vector-db"],
      path: [{ key: "paper:graph-rag" }, { key: "paper:vector-db" }],
      path_edges: [
        {
          from: { key: "paper:graph-rag" },
          to: { key: "paper:vector-db" },
          type: "cites",
          weight: 0.92,
        },
      ],
    },
    {
      key: "paper:path-ranking",
      depth: 2,
      distance: 1.7,
      document: documents["paper:path-ranking"],
      path: [{ key: "paper:graph-rag" }, { key: "paper:vector-db" }, { key: "paper:path-ranking" }],
      path_edges: [
        {
          from: { key: "paper:graph-rag" },
          to: { key: "paper:vector-db" },
          type: "cites",
          weight: 0.92,
        },
        {
          from: { key: "paper:vector-db" },
          to: { key: "paper:path-ranking" },
          type: "extends",
          weight: 0.78,
        },
      ],
    },
    {
      key: "paper:agent-memory",
      depth: 1,
      distance: 0.88,
      document: documents["paper:agent-memory"],
      path: [{ key: "paper:graph-rag" }, { key: "paper:agent-memory" }],
      path_edges: [
        {
          from: { key: "paper:graph-rag" },
          to: { key: "paper:agent-memory" },
          type: "extends",
          weight: 0.88,
        },
      ],
    },
    {
      key: "paper:entity-links",
      depth: 2,
      distance: 1.6,
      document: documents["paper:entity-links"],
      path: [
        { key: "paper:graph-rag" },
        { key: "paper:agent-memory" },
        { key: "paper:entity-links" },
      ],
      path_edges: [
        {
          from: { key: "paper:graph-rag" },
          to: { key: "paper:agent-memory" },
          type: "extends",
          weight: 0.88,
        },
        {
          from: { key: "paper:agent-memory" },
          to: { key: "paper:entity-links" },
          type: "mentions",
          weight: 0.72,
        },
      ],
    },
  ],
  paths: [],
  stats: { returned_items: 4, truncated: false },
};

const shortestPathResult: GraphQueryResult = {
  kind: "nodes",
  nodes: [],
  paths: [
    {
      nodes: [
        { key: "paper:graph-rag" },
        { key: "paper:agent-memory" },
        { key: "paper:entity-links" },
      ],
      edges: [
        {
          from: { key: "paper:graph-rag" },
          to: { key: "paper:agent-memory" },
          type: "extends",
          weight: 0.88,
        },
        {
          from: { key: "paper:agent-memory" },
          to: { key: "paper:entity-links" },
          type: "mentions",
          weight: 0.72,
        },
      ],
      total_weight: 0.63,
      length: 2,
    },
  ],
  stats: { returned_items: 1, truncated: false },
};

function graphResultFor(request: QueryRequest): GraphQueryResult {
  const query = request.graph_queries?.explorer;
  if (query && "shortest_path" in query) return shortestPathResult;
  return traversalResult;
}

const fakeClient = {
  tables: {
    query: async (_tableName: string, request: QueryRequest) => {
      if (!request.graph_queries) {
        return {
          responses: [
            {
              hits: {
                hits: Object.keys(documents).map((key) => ({
                  _id: key,
                  _source: documents[key as keyof typeof documents],
                })),
              },
            },
          ],
        };
      }

      return {
        responses: [
          {
            graph_results: {
              explorer: graphResultFor(request),
            },
          },
        ],
      };
    },
  },
} as unknown as AntflyClient;

const meta: Meta<typeof GraphIndexExplorer> = {
  title: "Antfarm/Graph Index Explorer",
  component: GraphIndexExplorer,
  parameters: {
    layout: "fullscreen",
  },
  decorators: [
    (Story) => (
      <ApiConfigContext.Provider
        value={{
          apiUrl: "storybook://fake-antfly",
          setApiUrl: () => undefined,
          client: fakeClient,
          resetToDefault: () => undefined,
          inferenceApiUrl: "",
          setInferenceApiUrl: () => undefined,
          resetInferenceApiUrl: () => undefined,
          inferenceConnectionId: "local-inference",
          setInferenceConnectionId: () => undefined,
          inferenceUrl: (operation) => `/ai/v1/${operation}`,
        }}
      >
        <div className="af-dashboard min-h-screen bg-background p-6">
          <Story />
        </div>
      </ApiConfigContext.Provider>
    ),
  ],
  args: {
    tableName: "research_papers",
    indexes: [graphIndex],
    onRefreshIndexes: () => undefined,
    initialStartKey: "paper:graph-rag",
    initialTargetKey: "paper:entity-links",
    initialResult: traversalResult,
  },
};

export default meta;
type Story = StoryObj<typeof meta>;

export const TraversalWithFakeCitationData: Story = {};

export const ShortestPathWithFakeCitationData: Story = {
  args: {
    initialMode: "shortest_path",
    initialResult: shortestPathResult,
  },
};
