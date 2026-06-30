import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import * as React from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import SqlWorkbenchPage from "./SqlWorkbenchPage";

const mocks = vi.hoisted(() => ({
  execute: vi.fn(),
  setSelectedTable: vi.fn(),
}));

vi.mock("@/hooks/use-api-config", () => ({
  useApiConfig: () => ({
    apiUrl: "/db/v1",
  }),
  useApi: () => ({
    sql: {
      execute: mocks.execute,
    },
  }),
}));

vi.mock("@/hooks/use-table", () => ({
  useTable: () => ({
    tables: [{ name: "docs" }],
    isLoadingTables: false,
    selectedTable: "docs",
    setSelectedTable: mocks.setSelectedTable,
  }),
}));

vi.mock("@antfly/design-system", () => {
  const Div = ({ children, ...props }: React.HTMLAttributes<HTMLDivElement>) => (
    <div {...props}>{children}</div>
  );
  const Button = ({ children, ...props }: React.ButtonHTMLAttributes<HTMLButtonElement>) => (
    <button {...props}>{children}</button>
  );
  const Label = ({ children, ...props }: React.LabelHTMLAttributes<HTMLLabelElement>) => (
    // biome-ignore lint/a11y/noLabelWithoutControl: this test double preserves label lookup semantics.
    <label {...props}>{children}</label>
  );
  const Input = (props: React.InputHTMLAttributes<HTMLInputElement>) => <input {...props} />;
  const Textarea = React.forwardRef<
    HTMLTextAreaElement,
    React.TextareaHTMLAttributes<HTMLTextAreaElement>
  >((props, ref) => <textarea ref={ref} {...props} />);
  const Switch = ({
    checked,
    onCheckedChange,
    ...props
  }: React.InputHTMLAttributes<HTMLInputElement> & {
    onCheckedChange?: (checked: boolean) => void;
  }) => (
    <input
      {...props}
      type="checkbox"
      checked={Boolean(checked)}
      onChange={(event) => onCheckedChange?.(event.currentTarget.checked)}
    />
  );
  const Table = ({ children, ...props }: React.TableHTMLAttributes<HTMLTableElement>) => (
    <table {...props}>{children}</table>
  );
  const TableHeader = ({ children, ...props }: React.HTMLAttributes<HTMLTableSectionElement>) => (
    <thead {...props}>{children}</thead>
  );
  const TableBody = ({ children, ...props }: React.HTMLAttributes<HTMLTableSectionElement>) => (
    <tbody {...props}>{children}</tbody>
  );
  const TableRow = ({ children, ...props }: React.HTMLAttributes<HTMLTableRowElement>) => (
    <tr {...props}>{children}</tr>
  );
  const TableHead = ({ children, ...props }: React.ThHTMLAttributes<HTMLTableCellElement>) => (
    <th {...props}>{children}</th>
  );
  const TableCell = ({ children, ...props }: React.TdHTMLAttributes<HTMLTableCellElement>) => (
    <td {...props}>{children}</td>
  );

  return {
    Alert: Div,
    AlertDescription: Div,
    Badge: Div,
    Button,
    DashboardPage: Div,
    DashboardPageActions: Div,
    DashboardPageDescription: Div,
    DashboardPageHeader: Div,
    DashboardPageTitle: Div,
    DashboardToolbar: Div,
    Input,
    Label,
    Select: Div,
    SelectContent: Div,
    SelectItem: Div,
    SelectTrigger: Div,
    SelectValue: Div,
    Switch,
    Table,
    TableBody,
    TableCell,
    TableHead,
    TableHeader,
    TableRow,
    Tabs: Div,
    TabsContent: Div,
    TabsList: Div,
    TabsTrigger: Div,
    Textarea,
  };
});

describe("SqlWorkbenchPage", () => {
  beforeEach(() => {
    mocks.execute.mockReset();
    mocks.setSelectedTable.mockReset();
    window.localStorage.clear();
  });

  afterEach(() => {
    cleanup();
  });

  it("executes SQL through the SDK and stores the returned session", async () => {
    mocks.execute.mockResolvedValue({
      kind: "read",
      session_id: 42,
      statement_kind: "select",
      result: {
        rows: [{ id: "doc-1", title: "First document" }],
      },
    });

    render(<SqlWorkbenchPage />);

    fireEvent.click(screen.getAllByRole("button", { name: /^Run$/ })[0]);

    await waitFor(() => expect(mocks.execute).toHaveBeenCalledTimes(1));
    expect(mocks.execute).toHaveBeenCalledWith({
      sql: 'SELECT * FROM "docs" LIMIT 100;',
      session_id: null,
      database: null,
      namespace: null,
      read_only: true,
    });
    expect(await screen.findByText("Session 42")).toBeTruthy();
    expect(await screen.findByText("doc-1")).toBeTruthy();
  });

  it("blocks mutating statements while read-only mode is enabled", async () => {
    render(<SqlWorkbenchPage />);

    fireEvent.change(screen.getByLabelText("Editor"), {
      target: { value: "CREATE TABLE blocked (id TEXT);" },
    });
    fireEvent.click(screen.getAllByRole("button", { name: /^Run$/ })[0]);

    expect(mocks.execute).not.toHaveBeenCalled();
    expect(screen.getAllByText(/Read-only mode blocks this statement/).length).toBeGreaterThan(0);
  });

  it("blocks additional statements while read-only mode is enabled", async () => {
    render(<SqlWorkbenchPage />);

    fireEvent.change(screen.getByLabelText("Editor"), {
      target: { value: "SELECT 1; DROP TABLE docs;" },
    });
    fireEvent.click(screen.getAllByRole("button", { name: /^Run$/ })[0]);

    expect(mocks.execute).not.toHaveBeenCalled();
    expect(screen.getAllByText(/Read-only mode blocks this statement/).length).toBeGreaterThan(0);
  });

  it("blocks mutating CTE bodies while read-only mode is enabled", async () => {
    render(<SqlWorkbenchPage />);

    fireEvent.change(screen.getByLabelText("Editor"), {
      target: {
        value: "WITH deleted AS (DELETE FROM docs RETURNING *) SELECT * FROM deleted;",
      },
    });
    fireEvent.click(screen.getAllByRole("button", { name: /^Run$/ })[0]);

    expect(mocks.execute).not.toHaveBeenCalled();
    expect(screen.getAllByText(/Read-only mode blocks this statement/).length).toBeGreaterThan(0);
  });

  it("allows mutating words inside string literals while read-only mode is enabled", async () => {
    mocks.execute.mockResolvedValue({
      kind: "read",
      session_id: 43,
      statement_kind: "select",
      result: {
        rows: [{ value: "DROP TABLE docs" }],
      },
    });
    render(<SqlWorkbenchPage />);

    fireEvent.change(screen.getByLabelText("Editor"), {
      target: { value: "SELECT 'DROP TABLE docs' AS value;" },
    });
    fireEvent.click(screen.getAllByRole("button", { name: /^Run$/ })[0]);

    await waitFor(() => expect(mocks.execute).toHaveBeenCalledTimes(1));
    expect(mocks.execute).toHaveBeenCalledWith({
      sql: "SELECT 'DROP TABLE docs' AS value;",
      session_id: null,
      database: null,
      namespace: null,
      read_only: true,
    });
  });
});
