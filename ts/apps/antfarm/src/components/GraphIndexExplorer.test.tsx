import type { GraphQueryResult, IndexStatus } from "@antfly/sdk";
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { GraphIndexExplorer } from "./GraphIndexExplorer";

const mocks = vi.hoisted(() => ({
  query: vi.fn(),
}));

vi.mock("@/hooks/use-api-config", () => ({
  useApi: () => ({
    tables: {
      query: mocks.query,
    },
  }),
}));

vi.mock("@antfly/graph", () => ({
  ForceGraph: ({ data }: { data: { nodes: unknown[]; edges: unknown[] } }) => (
    <div data-testid="force-graph">
      {data.nodes.length} nodes / {data.edges.length} edges
    </div>
  ),
}));

vi.mock("./JsonViewer", () => ({
  default: ({ json }: { json: unknown }) => (
    <pre data-testid="json-viewer">{JSON.stringify(json)}</pre>
  ),
}));

const graphIndex = {
  config: {
    name: "graph_idx",
    type: "graph",
    edge_types: [{ name: "cites" }],
  },
  status: {
    total_edges: 2,
    edge_types: {
      cites: 2,
    },
  },
} as unknown as IndexStatus;

describe("GraphIndexExplorer", () => {
  beforeEach(() => {
    mocks.query.mockReset();
  });

  afterEach(() => {
    cleanup();
  });

  it("renders a graph index without update loops", async () => {
    render(
      <GraphIndexExplorer
        tableName="papers"
        indexes={[graphIndex]}
        onRefreshIndexes={() => undefined}
      />
    );

    expect(await screen.findByText("Graph Explorer")).toBeTruthy();
    expect(screen.getAllByText("graph_idx").length).toBeGreaterThan(0);
    expect(screen.getByText("cites")).toBeTruthy();
    expect(screen.getByTestId("force-graph").textContent).toContain("0 nodes");
  });

  it("requests documents and edges for visualization queries", async () => {
    mocks.query.mockResolvedValue({
      responses: [
        {
          graph_results: {
            explorer: {
              type: "traverse",
              total: 1,
              nodes: [
                {
                  key: "bob",
                  depth: 1,
                  path_edges: [{ source: "alice", target: "bob", type: "cites", weight: 0.8 }],
                },
              ],
            },
          },
        },
      ],
    });

    render(
      <GraphIndexExplorer
        tableName="papers"
        indexes={[graphIndex]}
        onRefreshIndexes={() => undefined}
      />
    );

    fireEvent.change(screen.getByLabelText("Start key"), { target: { value: "alice" } });
    fireEvent.click(screen.getByRole("button", { name: /run graph query/i }));

    await waitFor(() => expect(mocks.query).toHaveBeenCalledTimes(1));
    expect(mocks.query).toHaveBeenCalledWith(
      "papers",
      expect.objectContaining({
        graph_queries: {
          explorer: expect.objectContaining({
            index: "graph_idx",
            traverse: expect.objectContaining({
              start: { keys: ["alice"] },
              include_paths: true,
              deduplicate_nodes: true,
              max_depth: 2,
            }),
          }),
        },
      })
    );
  });

  it("summarizes a minimal pre-discriminator legacy result", async () => {
    render(
      <GraphIndexExplorer
        tableName="papers"
        indexes={[graphIndex]}
        onRefreshIndexes={() => undefined}
        initialResult={{ type: "neighbors", total: 12 } as unknown as GraphQueryResult}
      />
    );

    expect(await screen.findByText("Graph Explorer")).toBeTruthy();
    expect(screen.getByText("12")).toBeTruthy();
    expect(screen.getByTestId("force-graph").textContent).toContain("0 nodes");
  });

  it("keeps same-key path endpoints distinct across tables", async () => {
    render(
      <GraphIndexExplorer
        tableName="papers"
        indexes={[graphIndex]}
        onRefreshIndexes={() => undefined}
        initialResult={{
          kind: "nodes",
          nodes: [],
          paths: [
            {
              nodes: [{ key: "shared" }, { key: "shared", table: "entities" }],
              edges: [{ source: "shared", target: "shared", type: "mentions", weight: 1 }],
              total_weight: 1,
              length: 1,
            },
          ],
          stats: { returned_items: 1, truncated: false },
          took: 1,
        }}
      />
    );

    expect(await screen.findByText("Graph Explorer")).toBeTruthy();
    expect(screen.getByTestId("force-graph").textContent).toContain("2 nodes / 1 edges");
  });
});
