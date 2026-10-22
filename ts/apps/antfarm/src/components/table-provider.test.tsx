import { act, cleanup, render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter, useLocation } from "react-router-dom";
import { afterEach, describe, expect, it, vi } from "vitest";
import { useTable } from "@/hooks/use-table";
import { TableProvider } from "./table-provider";

const api = vi.hoisted(() => ({
  tables: { list: vi.fn() },
  indexes: { list: vi.fn() },
}));
vi.mock("@/hooks/use-api-config", () => ({ useApi: () => api }));
vi.mock("@/config/products", () => ({ isProductEnabled: () => true }));

afterEach(() => {
  cleanup();
  vi.resetAllMocks();
});
function Consumer() {
  const context = useTable();
  const location = useLocation();
  return (
    <>
      <output data-testid="context">
        {JSON.stringify({
          index: context.selectedIndex,
          indexes: context.embeddingIndexes,
          loading: context.isLoadingIndexes,
          search: location.search,
          pathname: location.pathname,
          table: context.selectedTable,
        })}
      </output>
      <button type="button" onClick={() => context.setSelectedTable("other")}>
        Switch table
      </button>
    </>
  );
}
function setup() {
  api.tables.list.mockResolvedValue([{ name: "docs" }, { name: "other" }]);
  return render(
    <MemoryRouter initialEntries={["/data/playground/chat?table=docs&index=second"]}>
      <TableProvider>
        <Consumer />
      </TableProvider>
    </MemoryRouter>
  );
}
function state() {
  return JSON.parse(screen.getByTestId("context").textContent || "{}");
}
function index(name: string) {
  return { config: { name, type: "aknn" } };
}

describe("table request context", () => {
  it("preserves a compatible index deep link without reloading indexes", async () => {
    api.indexes.list.mockResolvedValue([index("first"), index("second")]);
    setup();
    await waitFor(() => expect(state().indexes).toEqual(["first", "second"]));
    expect(state().index).toBe("second");
    expect(api.indexes.list).toHaveBeenCalledTimes(1);
  });

  it("returns to the table list when the selected table no longer exists", async () => {
    api.tables.list.mockResolvedValue([]);
    api.indexes.list.mockResolvedValue([]);
    render(
      <MemoryRouter initialEntries={["/tables/deleted?section=overview"]}>
        <TableProvider>
          <Consumer />
        </TableProvider>
      </MemoryRouter>
    );
    await waitFor(() => expect(state().pathname).toBe("/"));
    expect(state().table).toBe("");
    expect(state().search).not.toContain("table=");
  });

  it("discards a slow response after switching tables and selects a compatible index", async () => {
    let resolveOld: (value: unknown) => void = () => {};
    api.indexes.list.mockImplementation((table: string) =>
      table === "docs"
        ? new Promise((resolve) => {
            resolveOld = resolve;
          })
        : Promise.resolve([index("other-embedding")])
    );
    setup();
    await waitFor(() => expect(api.indexes.list).toHaveBeenCalledWith("docs"));
    act(() => screen.getByRole("button", { name: "Switch table" }).click());
    await waitFor(() => expect(state().index).toBe("other-embedding"));
    await act(async () => resolveOld([index("stale")]));
    expect(state().indexes).toEqual(["other-embedding"]);
    expect(state().search).toContain("table=other");
    expect(state().search).toContain("index=other-embedding");
    expect(api.indexes.list).toHaveBeenCalledTimes(2);
  });
});
