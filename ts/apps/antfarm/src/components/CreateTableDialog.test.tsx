import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import CreateTableDialog from "./CreateTableDialog";

const mocks = vi.hoisted(() => ({
  create: vi.fn(),
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

vi.mock("./schema-builder/TableSchemaForm", () => ({
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

describe("CreateTableDialog", () => {
  beforeEach(() => {
    mocks.create.mockReset();
    mocks.refreshTables.mockReset();
    mocks.setSelectedTable.mockReset();
  });

  afterEach(() => {
    cleanup();
    vi.restoreAllMocks();
  });

  it("keeps the dialog open and displays API failures", async () => {
    vi.spyOn(console, "error").mockImplementation(() => undefined);
    mocks.create.mockRejectedValue(
      new Error("Failed to create table: invalid create table request")
    );
    const onClose = vi.fn();

    render(<CreateTableDialog open onClose={onClose} onTableCreated={vi.fn()} theme="light" />);
    fireEvent.click(screen.getByRole("button", { name: "Submit table" }));

    await waitFor(() => expect(mocks.create).toHaveBeenCalledWith("docs", { num_shards: 1 }));
    expect(
      await screen.findByText("Failed to create table: invalid create table request")
    ).toBeTruthy();
    expect(onClose).not.toHaveBeenCalled();
  });
});
