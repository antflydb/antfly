import { type GraphCatalogEntry, GraphExecutionError, type GraphQueryResult } from "@antfly/sdk";
import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { AuthContext, type AuthContextType } from "../contexts/auth-context";
import GraphQueryWorkbench, { graphQueryStatement } from "./GraphQueryWorkbench";

const mocks = vi.hoisted(() => ({
  client: { graphs: { execute: vi.fn(), writeStatus: vi.fn() }, tables: { get: vi.fn() } },
}));
vi.mock("../hooks/use-api-config", () => ({ useApi: () => mocks.client }));
const graphs: GraphCatalogEntry[] = [
  {
    graph_id: "1",
    revision: "1",
    name: "knowledge",
    definition: { labels: [{ name: "Document", table: "docs" }], relationships: [] },
  },
  {
    graph_id: "2",
    revision: "1",
    name: "other",
    definition: { labels: [{ name: "Person", table: "people" }], relationships: [] },
  },
];
function response(): GraphQueryResult {
  return {
    wire_version: 1,
    columns: ["n"],
    rows: [
      [
        {
          type: "node",
          value: {
            identity: { table: "docs", key: "a" },
            labels: ["Document"],
            properties: {
              type: "map",
              value: { count: { type: "integer", value: "9223372036854775807" } },
            },
          },
        },
      ],
    ],
    plan: [],
    stats: {},
    graph_incarnation: "1",
    graph_revision: "1",
    complete: true,
    read_only: true,
  };
}
const show = (selected = "knowledge") => (
  <MemoryRouter>
    <GraphQueryWorkbench graphs={graphs} selected={selected} onSelect={vi.fn()} />
  </MemoryRouter>
);
describe("graph query workbench", () => {
  it("removes modes after nested comments without inspecting statement keywords", () => {
    expect(graphQueryStatement("/* a /* b */ c */ PROFILE/* mode */ MATCH (n) RETURN n")).toBe(
      "/* mode */ MATCH (n) RETURN n"
    );
    expect(graphQueryStatement("/* a /* b */ c */ MATCH (n) RETURN n")).toBe(
      "/* a /* b */ c */ MATCH (n) RETURN n"
    );
  });
  beforeEach(() => {
    localStorage.clear();
    mocks.client.graphs.execute.mockReset().mockResolvedValue(response());
    mocks.client.graphs.writeStatus.mockReset();
    mocks.client.tables.get.mockReset();
  });
  afterEach(cleanup);
  it("isolates persisted write receipts by the authenticated principal", async () => {
    const authentication = (username: string): AuthContextType => ({
      user: { username, permissions: [] },
      isAuthenticated: true,
      isLoading: false,
      error: null,
      authEnabled: true,
      login: async () => {},
      logout: vi.fn(),
      hasPermission: () => true,
      refreshUser: async () => {},
    });
    const alice = authentication("alice");
    localStorage.setItem(
      `antfly:graph-write:${window.location.origin}:principal:alice`,
      JSON.stringify({ id: "a".repeat(32), graph: "knowledge", disposition: "outcome_unknown" })
    );
    const view = render(
      <AuthContext.Provider value={authentication("bob")}>{show()}</AuthContext.Provider>
    );
    expect((screen.getByRole("button", { name: "Run" }) as HTMLButtonElement).disabled).toBe(false);
    view.rerender(<AuthContext.Provider value={alice}>{show()}</AuthContext.Provider>);
    await waitFor(() =>
      expect((screen.getByRole("button", { name: "Run" }) as HTMLButtonElement).disabled).toBe(true)
    );
    expect(mocks.client.graphs.execute).not.toHaveBeenCalled();
  });
  it("loads schema property suggestions from explicitly scoped mapped tables", async () => {
    const scoped: GraphCatalogEntry[] = [
      {
        graph_id: "3",
        revision: "1",
        name: "scoped",
        definition: {
          labels: [
            { name: "Document", table: { database: "sales", namespace: "reports", table: "docs" } },
          ],
          relationships: [],
        },
      },
    ];
    mocks.client.tables.get.mockResolvedValue({
      schema: {
        document_schemas: {
          row: { schema: { properties: { "display name": { type: "string" } } } },
        },
      },
    });
    render(
      <MemoryRouter>
        <GraphQueryWorkbench graphs={scoped} selected="scoped" onSelect={vi.fn()} />
      </MemoryRouter>
    );
    fireEvent.click(screen.getByRole("button", { name: "Load properties" }));
    await screen.findByRole("option", { name: "n.`display name`" });
    expect(mocks.client.tables.get).toHaveBeenCalledWith("docs", {
      database: "sales",
      namespace: "reports",
    });
    fireEvent.change(screen.getByLabelText("Insert Cypher syntax"), {
      target: { value: "n.`display name`" },
    });
    expect((screen.getByLabelText("Cypher statement") as HTMLTextAreaElement).value).toContain(
      "n.`display name`"
    );
  });
  it("runs against the selected graph and keeps typed integer properties lossless", async () => {
    render(show());
    fireEvent.click(screen.getByRole("button", { name: "Run" }));
    await screen.findByText(":Document a");
    expect(mocks.client.graphs.execute.mock.calls[0][0]).toBe("knowledge");
    expect(mocks.client.graphs.execute.mock.calls[0][1]).toMatchObject({ parameters: {} });
    fireEvent.click(screen.getByRole("tab", { name: "Graph" }));
    fireEvent.click(screen.getByRole("button", { name: "Inspect a" }));
    expect(screen.getByText(/9223372036854775807/).textContent).toContain("9223372036854775807");
  });
  it("prefixes Explain without executing a normal statement", async () => {
    render(show());
    fireEvent.click(screen.getByRole("button", { name: "Explain" }));
    await waitFor(() => expect(mocks.client.graphs.execute).toHaveBeenCalled());
    expect(mocks.client.graphs.execute.mock.calls[0][1].query).toBe(
      "EXPLAIN MATCH (n) RETURN n LIMIT 25"
    );
  });
  it("discards a late response after switching graph scope", async () => {
    let resolve!: (value: GraphQueryResult) => void;
    mocks.client.graphs.execute.mockReturnValueOnce(
      new Promise<GraphQueryResult>((done) => {
        resolve = done;
      })
    );
    const view = render(show());
    fireEvent.click(screen.getByRole("button", { name: "Run" }));
    const signal = mocks.client.graphs.execute.mock.calls[0][2].signal as AbortSignal;
    view.rerender(show("other"));
    expect(signal.aborted).toBe(true);
    await act(async () => {
      resolve(response());
    });
    expect(screen.queryByText(":Document a")).toBeNull();
  });
  it("selects UTF-8 diagnostics in the Unicode editor after an Explain prefix", async () => {
    const source = "RETURN '😀', nope";
    const start = source.indexOf("nope");
    const byteStart = new TextEncoder().encode(`EXPLAIN ${source.slice(0, start)}`).length;
    mocks.client.graphs.execute.mockRejectedValue(
      new GraphExecutionError(400, {
        code: "UndefinedGraphVariable",
        message: "Unknown variable nope",
        span: { start: byteStart, end: byteStart + 4 },
      })
    );
    render(show());
    const editor = screen.getByLabelText("Cypher statement") as HTMLTextAreaElement;
    fireEvent.change(editor, { target: { value: source } });
    fireEvent.click(screen.getByRole("button", { name: "Run" }));
    await screen.findByText("Unknown variable nope at line 1, column 13");
    expect(editor.selectionStart).toBe(start);
    expect(editor.selectionEnd).toBe(start + 4);
  });
  it("keeps runtime availability errors out of the editor source location", async () => {
    mocks.client.graphs.execute.mockRejectedValue(
      new GraphExecutionError(503, {
        code: "LeaderUnavailable",
        message: "The owner is unavailable",
        span: { start: 0, end: 0 },
      })
    );
    render(show());
    const editor = screen.getByLabelText("Cypher statement") as HTMLTextAreaElement;
    fireEvent.change(editor, { target: { value: "RETURN 1" } });
    editor.setSelectionRange(4, 6);
    fireEvent.click(screen.getByRole("button", { name: "Run" }));
    await screen.findByText("The owner is unavailable");
    expect(screen.queryByText(/at line/)).toBeNull();
    expect(editor.selectionStart).toBe(4);
    expect(editor.selectionEnd).toBe(6);
  });

  it("hydrates a relationship endpoint and keeps isolated neighbor expansion bounded", async () => {
    const entry: GraphCatalogEntry = {
      ...graphs[0],
      definition: {
        ...graphs[0].definition,
        relationships: [
          {
            name: "CITES",
            index: "links",
            source_label: "Document",
            target_label: "Document",
            stored_type: "cites",
          },
        ],
      },
    };
    mocks.client.graphs.execute.mockResolvedValueOnce(response()).mockResolvedValueOnce({
      ...response(),
      rows: [
        [
          {
            type: "relationship",
            value: {
              identity: { table: "docs", key: "edge" },
              index: "links",
              type: "CITES",
              source: { table: "docs", key: "a" },
              target: { table: "docs", key: "b" },
              ownership: "explicit",
              properties: { type: "map", value: {} },
            },
          },
        ],
      ],
    });
    render(
      <MemoryRouter>
        <GraphQueryWorkbench graphs={[entry]} selected="knowledge" onSelect={vi.fn()} />
      </MemoryRouter>
    );
    fireEvent.click(screen.getByRole("button", { name: "Run" }));
    await screen.findByText(":CITES");
    fireEvent.click(screen.getByRole("tab", { name: "Graph" }));
    fireEvent.click(screen.getByRole("button", { name: "Inspect b" }));
    const hydrated = response();
    if (hydrated.rows[0][0].type === "node") hydrated.rows[0][0].value.identity.key = "b";
    mocks.client.graphs.execute.mockResolvedValueOnce(hydrated);
    fireEvent.click(screen.getByRole("button", { name: "Expand neighbors" }));
    await screen.findByText(/Added up to 50 neighbor rows/);
    const request = mocks.client.graphs.execute.mock.calls[2][1];
    expect(request.query).toContain("OPTIONAL MATCH");
    expect(request.query).toContain("LIMIT 50");
    expect(request.parameters).toEqual({ key: "b" });
    expect(request.limits).toMatchObject({ explored_edges: 10000 });
    expect(screen.getByText(/9223372036854775807/).textContent).toContain("9223372036854775807");
  });

  it("retains the write ID after nested-comment writes are canceled and inspects without replay", async () => {
    mocks.client.graphs.execute
      .mockResolvedValueOnce({ ...response(), read_only: false })
      .mockImplementationOnce(
        (_graph, _request, options) =>
          new Promise((_resolve, reject) => {
            options.signal.addEventListener(
              "abort",
              () => reject(new DOMException("Aborted", "AbortError")),
              { once: true }
            );
          })
      );
    render(show());
    fireEvent.change(screen.getByLabelText("Cypher statement"), {
      target: { value: "/* outer /* inner */ ' */ CREATE (n:Document {id:'new'}) RETURN n // '" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Run" }));
    await waitFor(() => expect(mocks.client.graphs.execute).toHaveBeenCalledTimes(2));
    const request = mocks.client.graphs.execute.mock.calls[1][1];
    expect(request.write_request_id).toMatch(/^[0-9a-f]{32}$/);
    fireEvent.click(screen.getByRole("button", { name: "Cancel request" }));
    await screen.findByText(/Write result delivery stopped/);
    expect((screen.getByRole("button", { name: "Run" }) as HTMLButtonElement).disabled).toBe(true);
    mocks.client.graphs.writeStatus.mockResolvedValue({ disposition: "committed" });
    fireEvent.click(screen.getByRole("button", { name: "Inspect transaction" }));
    await screen.findByText(`Transaction ${request.write_request_id}: committed.`);
    expect(mocks.client.graphs.writeStatus).toHaveBeenCalledWith(request.write_request_id);
    expect(mocks.client.graphs.execute).toHaveBeenCalledTimes(2);
    expect((screen.getByRole("button", { name: "Run" }) as HTMLButtonElement).disabled).toBe(false);
  });

  it("restores a canceled write receipt after remount without replaying", async () => {
    localStorage.setItem(
      `antfly:graph-write:${window.location.origin}`,
      JSON.stringify({ id: "a".repeat(32), graph: "knowledge", disposition: "outcome_unknown" })
    );
    render(show());
    expect((screen.getByRole("button", { name: "Run" }) as HTMLButtonElement).disabled).toBe(true);
    mocks.client.graphs.writeStatus.mockResolvedValue({ disposition: "committed" });
    fireEvent.click(screen.getByRole("button", { name: "Inspect transaction" }));
    await screen.findByText(`Transaction ${"a".repeat(32)}: committed.`);
    expect(mocks.client.graphs.execute).not.toHaveBeenCalled();
  });

  it("allows another statement after a definite permission rejection", async () => {
    mocks.client.graphs.execute
      .mockResolvedValueOnce({ ...response(), read_only: false })
      .mockRejectedValueOnce(
        new GraphExecutionError(403, { code: "Forbidden", message: "Write permission denied" })
      );
    render(show());
    fireEvent.change(screen.getByLabelText("Cypher statement"), {
      target: { value: "CREATE (n:Document) RETURN n" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Run" }));
    await screen.findByText("Write permission denied");
    expect((screen.getByRole("button", { name: "Run" }) as HTMLButtonElement).disabled).toBe(false);
    expect(mocks.client.graphs.execute).toHaveBeenCalledTimes(2);
  });

  it("unblocks a write explicitly rejected before admission", async () => {
    mocks.client.graphs.execute
      .mockResolvedValueOnce({ ...response(), read_only: false })
      .mockRejectedValueOnce(
        new GraphExecutionError(503, {
          code: "Backpressured",
          message: "Owners busy",
          write_outcome: "not_started",
        })
      );
    render(show());
    fireEvent.change(screen.getByLabelText("Cypher statement"), {
      target: { value: "CREATE (n:Document) RETURN n" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Run" }));
    await screen.findByText("Owners busy");
    expect((screen.getByRole("button", { name: "Run" }) as HTMLButtonElement).disabled).toBe(false);
  });

  it("retains a committed outcome when result encoding fails", async () => {
    mocks.client.graphs.execute
      .mockResolvedValueOnce({ ...response(), read_only: false })
      .mockRejectedValueOnce(
        new GraphExecutionError(429, {
          code: "OutOfMemory",
          message: "Encoding failed",
          write_outcome: "committed",
        })
      );
    render(show());
    fireEvent.change(screen.getByLabelText("Cypher statement"), {
      target: { value: "CREATE (n:Document) RETURN n" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Run" }));
    await screen.findByText(/The write committed, but its result could not be delivered/);
    const stored = JSON.parse(
      localStorage.getItem(`antfly:graph-write:${window.location.origin}`) ?? "{}"
    );
    expect(stored.disposition).toBe("committed");
    expect(mocks.client.graphs.execute).toHaveBeenCalledTimes(2);
  });

  it("inserts graph functions at the editor selection and offers scoped search syntax", () => {
    render(show());
    const editor = screen.getByLabelText("Cypher statement") as HTMLTextAreaElement;
    editor.setSelectionRange(0, editor.value.length);
    fireEvent.change(screen.getByLabelText("Insert Cypher syntax"), {
      target: { value: "elementId(n)" },
    });
    expect(editor.value).toBe("elementId(n)");
    const search = screen
      .getAllByRole("option", { name: /CALL antfly.search/ })
      .map((option) => option.getAttribute("value"));
    expect(search).toHaveLength(3);
    for (const mode of ["text", "semantic", "hybrid"])
      expect(
        search.some(
          (text) => text?.includes(`mode:'${mode}'`) && text.includes('antfly.search("docs"')
        )
      ).toBe(true);
    expect(mocks.client.graphs.execute).not.toHaveBeenCalled();
  });

  it("inspects relationship properties and separates parallel edges and self loops", async () => {
    const edge = (key: string, target = "b"): GraphQueryResult["rows"][number][number] => ({
      type: "relationship",
      value: {
        identity: { table: "docs", key },
        index: "links",
        type: "CITES",
        source: { table: "docs", key: "a" },
        target: { table: "docs", key: target },
        ownership: "explicit",
        properties: {
          type: "map",
          value: { weight: { type: "integer", value: "9223372036854775807" } },
        },
      },
    });
    mocks.client.graphs.execute.mockResolvedValue({
      ...response(),
      columns: ["r"],
      rows: [[edge("edge-1")], [edge("edge-2")], [edge("loop", "a")]],
    });
    render(show());
    fireEvent.click(screen.getByRole("button", { name: "Run" }));
    await screen.findAllByRole("button", { name: ":CITES" });
    fireEvent.click(screen.getAllByRole("button", { name: ":CITES" })[0]);
    expect(screen.getByText(/9223372036854775807/).textContent).toContain("9223372036854775807");
    expect(screen.queryByRole("button", { name: "Expand neighbors" })).toBeNull();
    fireEvent.click(screen.getByRole("tab", { name: "Graph" }));
    const first = screen
      .getByRole("button", { name: "Inspect relationship edge-1" })
      .querySelector("path")
      ?.getAttribute("d");
    const second = screen
      .getByRole("button", { name: "Inspect relationship edge-2" })
      .querySelector("path")
      ?.getAttribute("d");
    const loop = screen
      .getByRole("button", { name: "Inspect relationship loop" })
      .querySelector("path")
      ?.getAttribute("d");
    expect(first).not.toBe(second);
    expect(loop).toContain(" C ");
    fireEvent.change(screen.getByLabelText("Graph zoom"), { target: { value: "2" } });
    expect(screen.getByRole("img", { name: "Query graph" }).getAttribute("viewBox")).toBe(
      "200 105 400 210"
    );
    fireEvent.click(screen.getByRole("button", { name: "Pan right" }));
    expect(screen.getByRole("img", { name: "Query graph" }).getAttribute("viewBox")).toBe(
      "240 105 400 210"
    );
    fireEvent.click(screen.getByRole("button", { name: "Reset view" }));
    expect(screen.getByRole("img", { name: "Query graph" }).getAttribute("viewBox")).toBe(
      "0 0 800 420"
    );
  });
});
