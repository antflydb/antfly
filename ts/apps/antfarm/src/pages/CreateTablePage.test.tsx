import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import CreateTablePage from "./CreateTablePage";

const mocks = vi.hoisted(() => ({
  create: vi.fn(),
  navigate: vi.fn(),
  refreshTables: vi.fn(),
  setSelectedTable: vi.fn(),
}));

vi.mock("../hooks/use-api-config", () => ({
  useApi: () => ({ tables: { create: mocks.create }, indexes: { create: vi.fn() } }),
}));

vi.mock("../hooks/use-table", () => ({
  useTable: () => ({
    refreshTables: mocks.refreshTables,
    setSelectedTable: mocks.setSelectedTable,
  }),
}));

vi.mock("react-router-dom", () => ({
  useNavigate: () => mocks.navigate,
}));

vi.mock("../components/schema-builder/TableSchemaForm", () => ({
  default: ({ onSubmit }: { onSubmit: (data: unknown) => void }) => (
    <button
      type="button"
      onClick={() =>
        onSubmit({
          name: "docs",
          num_shards: 1,
          schema: { version: 0, document_schemas: {} },
          indexes: [],
        })
      }
    >
      Submit table
    </button>
  ),
}));

describe("CreateTablePage", () => {
  beforeEach(() => {
    mocks.create.mockReset();
    mocks.navigate.mockReset();
    mocks.refreshTables.mockReset();
    mocks.setSelectedTable.mockReset();
  });

  afterEach(() => {
    cleanup();
    vi.restoreAllMocks();
  });

  it("sends a defaulted create request and displays API failures", async () => {
    vi.spyOn(console, "error").mockImplementation(() => undefined);
    mocks.create.mockRejectedValue(
      new Error("Failed to create table: invalid create table request")
    );

    render(<CreateTablePage />);
    fireEvent.click(screen.getByRole("button", { name: "Submit table" }));

    await waitFor(() => expect(mocks.create).toHaveBeenCalledWith("docs", { num_shards: 1 }));
    expect(
      await screen.findByText("Failed to create table: invalid create table request")
    ).toBeTruthy();
    expect(mocks.navigate).not.toHaveBeenCalled();
  });
});
