// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import type { AntflyClient } from "@antfly/sdk";
import { describe, expect, it, vi } from "vitest";
import { antflyAdapter, predicate } from "../src/adapter.js";
import { remoteDatabase } from "../src/remote.js";
import { type AuthDatabase, assertReady } from "../src/transport.js";

describe("transport safety", () => {
  it("reconciles committed visibility by receipt without repeating a write", async () => {
    const executeSQL = vi.fn().mockResolvedValue({
      rows: [],
      columns: [],
      rows_affected: 0,
      transaction_status: "idle",
      mutation_outcome: "committed_pending",
      transaction_id: "a".repeat(32),
    });
    const getSQLTransactionOutcome = vi
      .fn()
      .mockResolvedValueOnce({ disposition: "committed_pending" })
      .mockResolvedValueOnce({ disposition: "committed" });
    const db = remoteDatabase({
      openSQLConnection: async () => ({ connection_id: "connection" }),
      executeSQL,
      getSQLTransactionOutcome,
      closeSQLConnection: async () => {},
    } as unknown as AntflyClient);
    const session = await db.connect();
    expect((await session.execute("COMMIT")).mutationOutcome).toBe("committed");
    expect(executeSQL).toHaveBeenCalledTimes(1);
    expect(getSQLTransactionOutcome).toHaveBeenCalledTimes(2);
    getSQLTransactionOutcome.mockResolvedValueOnce({ disposition: "committed_repair_required" });
    await expect(session.execute("COMMIT")).rejects.toThrow("operator reconciliation");
    expect(executeSQL).toHaveBeenCalledTimes(2);
    await session.close();
  });
  it("accepts DDL readiness only for the exact receipt identity and generation", async () => {
    const list = vi
      .fn()
      .mockResolvedValue([{ name: "session", table_id: "17", schema: { version: 2 } }]);
    const status = vi.fn().mockResolvedValue({ schema_version: 2, state: "enforced" });
    const db = remoteDatabase({
      tables: { list, constraints: { status } },
    } as unknown as AntflyClient);
    const receipt = {
      database: "default",
      namespace: "public",
      table: "session",
      table_id: "17",
      schema_version: 2,
      state: "pending",
    };
    await expect(db.waitForSchema!(receipt)).resolves.toBeUndefined();
    await expect(db.waitForSchema!({ ...receipt, table_id: "18" })).rejects.toThrow(
      "generation changed"
    );
    status.mockResolvedValue({ schema_version: 2, state: "invalid" });
    await expect(db.waitForSchema!(receipt)).rejects.toThrow("operator review");
  });
  it("preserves remote connection and transaction session identity without replay", async () => {
    const executeSQL = vi
      .fn()
      .mockResolvedValueOnce({
        rows: [],
        columns: [],
        rows_affected: 0,
        session_id: "txn",
        transaction_status: "in_transaction",
      })
      .mockResolvedValueOnce({
        rows: [["9007199254740993"]],
        columns: [{ name: "n", type: "integer" }],
        rows_affected: 1,
        session_id: "txn",
        transaction_status: "in_transaction",
      })
      .mockResolvedValueOnce({
        rows: [],
        columns: [],
        rows_affected: 0,
        transaction_status: "idle",
        mutation_outcome: "committed",
      });
    const closeSQLConnection = vi.fn();
    const db = remoteDatabase({
      openSQLConnection: async () => ({ connection_id: "connection", owner_node_id: "0" }),
      executeSQL,
      closeSQLConnection,
    } as unknown as AntflyClient);
    const session = await db.connect();
    await session.execute("BEGIN");
    expect((await session.execute("UPDATE example")).rows[0]?.n).toBe(9007199254740993n);
    await session.execute("COMMIT");
    expect(executeSQL.mock.calls[1]?.[0]).toMatchObject({
      connection_id: "connection",
      session_id: "txn",
    });
    expect(executeSQL.mock.calls[2]?.[0]).toMatchObject({
      connection_id: "connection",
      session_id: "txn",
    });
    await session.close();
    await session.close();
    expect(closeSQLConnection).toHaveBeenCalledTimes(1);
  });
  it("rejects incomplete committed writes and pending schema receipts", () => {
    expect(() =>
      assertReady({ rows: [], rowsAffected: 0n, mutationOutcome: "committed_pending" })
    ).toThrow("do not repeat");
    expect(() =>
      assertReady({ rows: [], rowsAffected: 0n, ddlReceipt: { state: "pending" } })
    ).toThrow("Schema is not ready");
  });
  it("sanitizes storage errors and never retries writes", async () => {
    const execute = vi.fn().mockRejectedValue(new Error("OUTCOME_UNKNOWN password=secret-token"));
    const close = vi.fn();
    const db = { connect: async () => ({ execute, close }) } as unknown as AuthDatabase;
    const adapter = antflyAdapter(db)({});
    await expect(
      adapter.create({
        model: "user",
        data: {
          name: "secret",
          email: "private@example.test",
          emailVerified: false,
          createdAt: new Date(),
          updatedAt: new Date(),
        },
      })
    ).rejects.toThrow("Authentication storage is unavailable");
    expect(execute).toHaveBeenCalledTimes(1);
    expect(close).toHaveBeenCalledTimes(1);
  });
  it("parameterizes values and groups owner guards outside OR predicates", () => {
    const values: unknown[] = [];
    expect(
      predicate(
        [
          {
            field: "email",
            operator: "eq",
            value: "x' OR TRUE",
            connector: "AND",
            mode: "sensitive",
          },
          { field: "email", operator: "eq", value: "b", connector: "OR", mode: "sensitive" },
          { field: "id", operator: "eq", value: "owner", connector: "AND", mode: "sensitive" },
        ],
        values
      )
    ).toBe('(("email" = $1 OR "email" = $2) AND "id" = $3)');
    expect(values).toEqual(["x' OR TRUE", "b", "owner"]);
  });
  it("does not replay a callback after an uncertain commit", async () => {
    const statements: string[] = [];
    const execute = vi.fn(async (statement: string) => {
      statements.push(statement);
      if (statement === "COMMIT") throw new Error("OUTCOME_UNKNOWN secret-token");
      return { rows: [], rowsAffected: 0n };
    });
    const db = {
      connect: async () => ({ execute, close: async () => {} }),
    } as unknown as AuthDatabase;
    const callback = vi.fn(async () => "done");
    await expect(antflyAdapter(db)({}).transaction(callback)).rejects.toThrow(
      "do not repeat an uncertain write"
    );
    expect(callback).toHaveBeenCalledTimes(1);
    expect(statements).toEqual(["BEGIN ISOLATION LEVEL READ COMMITTED", "COMMIT", "ROLLBACK"]);
  });
});
