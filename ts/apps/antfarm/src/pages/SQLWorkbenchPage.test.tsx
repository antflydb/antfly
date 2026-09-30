import { SQLExecutionError, type SQLResponse } from "@antfly/sdk";
import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import SQLWorkbenchPage from "./SQLWorkbenchPage";

const mocks = vi.hoisted(() => ({ client: { executeSQL: vi.fn() } }));
vi.mock("../hooks/use-api-config", () => ({ useApi: () => mocks.client }));

function response(overrides: Partial<SQLResponse> = {}): SQLResponse {
  return {
    columns: [],
    rows: [],
    rows_affected: 0,
    command_tag: "SELECT",
    transaction_status: "idle",
    ...overrides,
  };
}

function button(name: string): HTMLButtonElement {
  return screen.getByRole("button", { name });
}

function deferred() {
  let resolve!: (value: SQLResponse) => void;
  let reject!: (error: Error) => void;
  const promise = new Promise<SQLResponse>((ok, fail) => {
    resolve = ok;
    reject = fail;
  });
  return { promise, resolve, reject };
}

describe("SQL workbench mounted lifecycle", () => {
  beforeEach(() => {
    mocks.client = { executeSQL: vi.fn().mockResolvedValue(response()) };
  });
  afterEach(() => cleanup());

  it("owns BEGIN, statement and COMMIT within the same scope and session", async () => {
    mocks.client.executeSQL
      .mockResolvedValueOnce(
        response({
          command_tag: "BEGIN",
          session_id: "session-a",
          transaction_status: "in_transaction",
        })
      )
      .mockResolvedValueOnce(
        response({ session_id: "session-a", transaction_status: "in_transaction" })
      )
      .mockResolvedValueOnce(response({ command_tag: "COMMIT", transaction_id: "receipt-a" }));
    render(<SQLWorkbenchPage />);
    fireEvent.change(screen.getByLabelText("Database"), { target: { value: "analytics" } });
    fireEvent.click(button("Begin transaction"));
    await screen.findByText("session-a");
    expect((screen.getByLabelText("Database") as HTMLInputElement).disabled).toBe(true);
    expect((screen.getByLabelText("Namespace") as HTMLInputElement).disabled).toBe(true);
    fireEvent.click(button("Run statement"));
    await waitFor(() => expect(button("Commit session").disabled).toBe(false));
    fireEvent.click(button("Commit session"));
    await screen.findByText("receipt-a");
    expect(screen.queryByText("session-a")).toBeNull();
    expect((screen.getByLabelText("Database") as HTMLInputElement).disabled).toBe(false);
    const requests = mocks.client.executeSQL.mock.calls.map(([request]) => request);
    expect(requests.map((request) => request.session_id)).toEqual([
      undefined,
      "session-a",
      "session-a",
    ]);
    expect(requests.map((request) => request.statement)).toEqual([
      "BEGIN ISOLATION LEVEL READ COMMITTED",
      "SELECT 1",
      "COMMIT",
    ]);
    expect(
      requests.every(
        (request) => request.database === "analytics" && request.namespace === "public"
      )
    ).toBe(true);
  });

  it("rejects malformed parameters before sending and isolates transaction button parameters", async () => {
    mocks.client.executeSQL.mockResolvedValue(
      response({
        command_tag: "BEGIN",
        session_id: "session-b",
        transaction_status: "in_transaction",
      })
    );
    render(<SQLWorkbenchPage />);
    fireEvent.change(screen.getByLabelText("Statement"), {
      target: { value: "BEGIN ISOLATION LEVEL READ COMMITTED" },
    });
    fireEvent.change(screen.getByLabelText(/Positional parameters/), { target: { value: "{}" } });
    fireEvent.click(button("Run statement"));
    expect(screen.getByRole("alert").textContent).toContain("JSON array");
    expect(mocks.client.executeSQL).not.toHaveBeenCalled();
    fireEvent.click(button("Begin transaction"));
    await screen.findByText("session-b");
    expect(mocks.client.executeSQL.mock.calls[0][0].parameters).toEqual([]);
    expect(screen.queryByRole("alert")).toBeNull();
  });

  it("keeps an aborted session available for rollback and displays its receipt", async () => {
    mocks.client.executeSQL
      .mockResolvedValueOnce(
        response({
          command_tag: "BEGIN",
          session_id: "session-c",
          transaction_status: "in_transaction",
        })
      )
      .mockRejectedValueOnce(
        new SQLExecutionError(409, {
          code: "40001",
          message: "serialization conflict",
          transaction_status: "failed",
          transaction_id: "receipt-c",
        })
      )
      .mockResolvedValueOnce(response({ command_tag: "ROLLBACK" }));
    render(<SQLWorkbenchPage />);
    fireEvent.click(button("Begin transaction"));
    await screen.findByText("session-c");
    fireEvent.click(button("Run statement"));
    expect((await screen.findByRole("alert")).textContent).toContain("receipt-c");
    expect(screen.getByText("session-c")).toBeTruthy();
    fireEvent.click(button("Roll back session"));
    await waitFor(() => expect(screen.queryByText("session-c")).toBeNull());
    expect(mocks.client.executeSQL.mock.calls[2][0]).toMatchObject({
      statement: "ROLLBACK",
      session_id: "session-c",
      parameters: [],
    });
  });

  it("discards a completed session after an idle diagnostic", async () => {
    mocks.client.executeSQL
      .mockResolvedValueOnce(
        response({
          command_tag: "BEGIN",
          session_id: "expired",
          transaction_status: "in_transaction",
        })
      )
      .mockRejectedValueOnce(
        new SQLExecutionError(409, {
          code: "25P01",
          message: "session expired",
          transaction_status: "idle",
        })
      );
    render(<SQLWorkbenchPage />);
    fireEvent.click(button("Begin transaction"));
    await screen.findByText("expired");
    fireEvent.click(button("Run statement"));
    await screen.findByRole("alert");
    expect(screen.queryByText("expired")).toBeNull();
    expect(button("Begin transaction").disabled).toBe(false);
  });

  it("cancels transport without replay and requires reconciliation of an unknown outcome", async () => {
    const pending = deferred();
    mocks.client.executeSQL.mockReturnValueOnce(pending.promise);
    render(<SQLWorkbenchPage />);
    fireEvent.click(button("Run statement"));
    expect(button("Run statement").disabled).toBe(true);
    const signal = mocks.client.executeSQL.mock.calls[0][1].signal as AbortSignal;
    fireEvent.click(button("Cancel request"));
    expect(signal.aborted).toBe(true);
    await act(async () => pending.reject(new DOMException("Canceled", "AbortError")));
    expect(screen.getByRole("alert").textContent).toContain("do not replay");
    expect(button("Run statement").disabled).toBe(true);
    expect(mocks.client.executeSQL).toHaveBeenCalledTimes(1);
    fireEvent.click(button("I have reconciled the outcome; enable new statements"));
    expect(button("Run statement").disabled).toBe(false);
    expect(mocks.client.executeSQL).toHaveBeenCalledTimes(1);
  });

  it("requires reconciliation for a durable unknown-outcome diagnostic", async () => {
    mocks.client.executeSQL.mockRejectedValueOnce(
      new SQLExecutionError(409, {
        code: "40003",
        message: "commit outcome unknown",
        transaction_id: "receipt-unknown",
      })
    );
    render(<SQLWorkbenchPage />);
    fireEvent.click(button("Run statement"));
    expect((await screen.findByRole("alert")).textContent).toContain("receipt-unknown");
    expect(button("Begin transaction").disabled).toBe(true);
    expect(button("Run statement").disabled).toBe(true);
    expect(mocks.client.executeSQL).toHaveBeenCalledTimes(1);
  });

  it("aborts old-client requests and ignores their late success after a connection change", async () => {
    const old = deferred();
    mocks.client.executeSQL.mockReturnValueOnce(old.promise);
    const view = render(<SQLWorkbenchPage />);
    fireEvent.click(button("Run statement"));
    const signal = mocks.client.executeSQL.mock.calls[0][1].signal as AbortSignal;
    mocks.client = { executeSQL: vi.fn().mockResolvedValue(response()) };
    view.rerender(<SQLWorkbenchPage />);
    expect(signal.aborted).toBe(true);
    await act(async () =>
      old.resolve(response({ session_id: "old-session", command_tag: "OLD RESULT" }))
    );
    expect(screen.queryByText("old-session")).toBeNull();
    expect(screen.queryByRole("status")).toBeNull();
    fireEvent.click(button("Run statement"));
    await screen.findByRole("status");
    expect(mocks.client.executeSQL.mock.calls[0][0].session_id).toBeUndefined();
  });

  it("clears old-client errors and aborts outstanding transport on unmount", async () => {
    mocks.client.executeSQL.mockRejectedValueOnce(new Error("old endpoint failed"));
    const view = render(<SQLWorkbenchPage />);
    fireEvent.click(button("Run statement"));
    await screen.findByRole("alert");
    const pending = deferred();
    mocks.client = { executeSQL: vi.fn().mockReturnValueOnce(pending.promise) };
    view.rerender(<SQLWorkbenchPage />);
    expect(screen.queryByRole("alert")).toBeNull();
    expect(button("Run statement").disabled).toBe(false);
    fireEvent.click(button("Run statement"));
    const signal = mocks.client.executeSQL.mock.calls[0][1].signal as AbortSignal;
    view.unmount();
    expect(signal.aborted).toBe(true);
    await act(async () => pending.resolve(response({ session_id: "late-unmounted" })));
    expect(mocks.client.executeSQL).toHaveBeenCalledTimes(1);
  });
});
