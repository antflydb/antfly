import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import GraphCatalogPage from "./GraphCatalogPage";

const mocks = vi.hoisted(() => ({
  client: {
    graphs: { list: vi.fn(), create: vi.fn(), update: vi.fn(), get: vi.fn(), execute: vi.fn() },
    tables: { list: vi.fn() },
    indexes: { list: vi.fn() },
  },
}));
vi.mock("../hooks/use-api-config", () => ({ useApi: () => mocks.client }));
vi.mock("../components/GraphQueryWorkbench", () => ({ default: () => null }));
afterEach(cleanup);
beforeEach(() => vi.clearAllMocks());

it("creates explicitly scoped graph mappings and discovers indexes in the same scope", async () => {
  mocks.client.graphs.list.mockResolvedValue([]);
  mocks.client.tables.list.mockResolvedValue([{ name: "docs" }]);
  mocks.client.indexes.list.mockResolvedValue([{ config: { type: "graph", name: "links" } }]);
  mocks.client.graphs.create.mockResolvedValue({
    graph_id: "4",
    revision: "1",
    name: "knowledge",
    definition: { labels: [], relationships: [] },
  });
  render(<GraphCatalogPage />);
  const create = await screen.findByRole("button", { name: "Create graph" });
  await waitFor(() => expect(create.hasAttribute("disabled")).toBe(false));
  fireEvent.change(screen.getByLabelText("Graph name"), { target: { value: "knowledge" } });
  fireEvent.change(screen.getByLabelText("Document table"), { target: { value: "docs" } });
  fireEvent.change(screen.getByLabelText("Database"), { target: { value: "sales" } });
  fireEvent.change(screen.getByLabelText("Namespace"), { target: { value: "reports" } });
  await waitFor(() =>
    expect(mocks.client.indexes.list).toHaveBeenCalledWith("docs", {
      database: "sales",
      namespace: "reports",
    })
  );
  fireEvent.click(create);
  await waitFor(() =>
    expect(mocks.client.graphs.create).toHaveBeenCalledWith("knowledge", {
      labels: [
        { name: "Document", table: { database: "sales", namespace: "reports", table: "docs" } },
      ],
      relationships: [],
    })
  );
});

it("keeps the original edit revision through refresh and requires explicit reload after conflict", async () => {
  const original = {
    graph_id: "4",
    revision: "1",
    name: "knowledge",
    definition: { labels: [{ name: "Document", table: "docs" }], relationships: [] },
  };
  const concurrent = {
    ...original,
    revision: "2",
    definition: { ...original.definition, labels: [{ name: "Other", table: "docs" }] },
  };
  mocks.client.graphs.list.mockResolvedValue([original]);
  mocks.client.tables.list.mockResolvedValue([{ name: "docs" }]);
  mocks.client.indexes.list.mockResolvedValue([]);
  mocks.client.graphs.get.mockResolvedValue(concurrent);
  mocks.client.graphs.update.mockRejectedValue(
    new Error("Graph definition changed; reload the catalog before saving your edits.")
  );
  render(<GraphCatalogPage />);
  fireEvent.click(await screen.findByRole("button", { name: "knowledge" }));
  fireEvent.click(screen.getByRole("button", { name: "Edit definition" }));
  fireEvent.change(screen.getByLabelText("Label"), { target: { value: "Draft" } });
  mocks.client.graphs.list.mockResolvedValue([concurrent]);
  fireEvent.click(screen.getByRole("button", { name: "Refresh" }));
  await waitFor(() => expect(screen.getByText("Definition revision 2")).toBeTruthy());
  expect((screen.getByLabelText("Label") as HTMLInputElement).value).toBe("Draft");
  fireEvent.click(screen.getByRole("button", { name: "Save definition" }));
  await waitFor(() =>
    expect(mocks.client.graphs.update).toHaveBeenCalledWith("knowledge", "1", {
      labels: [{ name: "Draft", table: "docs" }],
      relationships: [],
    })
  );
  await waitFor(() =>
    expect(screen.getByRole("status").textContent).toContain("reload the catalog")
  );
  expect(mocks.client.graphs.create).not.toHaveBeenCalled();
  fireEvent.click(screen.getByRole("button", { name: "Reload definition" }));
  await waitFor(() =>
    expect((screen.getByLabelText("Label") as HTMLInputElement).value).toBe("Other")
  );
  const updated = { ...concurrent, revision: "3" };
  mocks.client.graphs.update.mockResolvedValue(updated);
  mocks.client.graphs.list.mockResolvedValue([updated]);
  fireEvent.click(screen.getByRole("button", { name: "Save definition" }));
  await waitFor(() =>
    expect(mocks.client.graphs.update).toHaveBeenLastCalledWith(
      "knowledge",
      "2",
      concurrent.definition
    )
  );
});

it("demands relationship projections for readiness and clears it when the definition changes", async () => {
  const original = {
    graph_id: "4",
    revision: "1",
    name: "knowledge",
    definition: { labels: [{ name: "Document", table: "docs" }] },
  };
  mocks.client.graphs.list.mockResolvedValue([original]);
  mocks.client.tables.list.mockResolvedValue([{ name: "docs" }]);
  mocks.client.indexes.list.mockResolvedValue([]);
  mocks.client.graphs.execute.mockResolvedValue({ complete: true, graph_revision: "1", rows: [] });
  render(<GraphCatalogPage />);
  fireEvent.click(await screen.findByRole("button", { name: "knowledge" }));
  fireEvent.click(await screen.findByRole("button", { name: "Check readiness" }));
  await waitFor(() =>
    expect(mocks.client.graphs.execute).toHaveBeenCalledWith(
      "knowledge",
      { query: "MATCH ()-[r]->() RETURN r LIMIT 0" },
      { signal: expect.any(AbortSignal) }
    )
  );
  expect(await screen.findByText(/Ready at definition revision 1/)).toBeTruthy();
  mocks.client.graphs.list.mockResolvedValue([{ ...original, revision: "2" }]);
  fireEvent.click(screen.getByRole("button", { name: "Refresh" }));
  await waitFor(() => expect(screen.queryByText(/Ready at definition revision 1/)).toBeNull());
  mocks.client.graphs.execute.mockRejectedValue(new Error("CatalogGenerationChanged"));
  fireEvent.click(screen.getByRole("button", { name: "Check readiness" }));
  expect(await screen.findByText(/Graph unavailable.*CatalogGenerationChanged/)).toBeTruthy();
});
