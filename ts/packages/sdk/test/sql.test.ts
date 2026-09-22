// Copyright 2026 Antfly, Inc.
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

import { afterEach, describe, expect, it, vi } from "vitest";
import { AntflyClient, SQLExecutionError } from "../src/client.js";

afterEach(() => vi.unstubAllGlobals());

describe("SQL client", () => {
  it.each([
    Number.NaN,
    Number.POSITIVE_INFINITY,
    Number.NEGATIVE_INFINITY,
    9007199254740992,
  ])("rejects nonfinite and unsafe integer parameter %s before dispatch", async (value) => {
    const fetch = vi.fn();
    vi.stubGlobal("fetch", fetch);
    const client = new AntflyClient({ baseUrl: "http://localhost:8080" });
    await expect(
      client.executeSQL({ statement: "SELECT $1", parameters: [{ nested: [value] }] })
    ).rejects.toThrow("Relational numbers");
    expect(fetch).not.toHaveBeenCalled();
  });
  it("does not let caller options raise the SQL request ceiling", async () => {
    const fetch = vi.fn();
    vi.stubGlobal("fetch", fetch);
    const client = new AntflyClient({ baseUrl: "http://localhost:8080" });
    await expect(
      client.executeSQL(
        { statement: "SELECT $1", parameters: ["x".repeat(4 << 20)] },
        { maxRequestBytes: 64 << 20 }
      )
    ).rejects.toThrow("4194304 bytes");
    expect(fetch).not.toHaveBeenCalled();
  });
  it("defaults SQL response admission to 16 MiB even with undefined options", async () => {
    const result = {
      columns: [{ name: "v", type: "string" }],
      rows: [["x".repeat(2 << 20)]],
      rows_affected: 0,
      command_tag: "SELECT 1",
    };
    const fetch = vi.fn().mockResolvedValue(new Response(JSON.stringify(result)));
    vi.stubGlobal("fetch", fetch);
    const client = new AntflyClient({ baseUrl: "http://localhost:8080" });
    expect(
      await client.executeSQL({ statement: "SELECT v FROM docs" }, { maxResponseBytes: undefined })
    ).toEqual(result);
    expect(fetch).toHaveBeenCalledWith(
      expect.any(String),
      expect.objectContaining({ redirect: "error" })
    );
  });
  it("does not let caller options raise the SQL response ceiling", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response(" ".repeat((16 << 20) + 1))));
    const client = new AntflyClient({ baseUrl: "http://localhost:8080" });
    await expect(
      client.executeSQL({ statement: "SELECT v FROM docs" }, { maxResponseBytes: 32 << 20 })
    ).rejects.toThrow("16777216 bytes");
  });
  it("preserves committed repair outcomes and their reconciliation receipts", async () => {
    const result = {
      columns: [],
      rows: [],
      rows_affected: 1,
      command_tag: "DELETE 1",
      mutation_outcome: "committed_repair_required",
      transaction_id: "0123456789abcdef0123456789abcdef",
    };
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response(JSON.stringify(result))));
    const client = new AntflyClient({ baseUrl: "http://localhost:8080" });
    expect(await client.executeSQL({ statement: "DELETE FROM docs WHERE _id = 'a'" })).toEqual(
      result
    );
  });
  it("retains SQLSTATE and ambiguous transaction reconciliation receipts", async () => {
    const diagnostic = {
      code: "40003",
      message: "do not replay",
      retryable: false,
      transaction_id: "0123456789abcdef0123456789abcdef",
    };
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(new Response(JSON.stringify(diagnostic), { status: 409 }))
    );
    const client = new AntflyClient({ baseUrl: "http://localhost:8080" });
    const error = await client
      .executeSQL({ statement: "DELETE FROM docs" })
      .catch((error: unknown) => error);
    expect(error).toBeInstanceOf(SQLExecutionError);
    expect((error as SQLExecutionError).diagnostic).toEqual(diagnostic);
  });
  it("preserves decimal integers, duplicate labels, bound parameters, and cancellation", async () => {
    const result = {
      columns: [
        { name: "id", type: "integer" },
        { name: "id", type: "string" },
      ],
      rows: [["9223372036854775807", "second"]],
      rows_affected: 0,
      command_tag: "SELECT 1",
    };
    const fetch = vi.fn().mockResolvedValue(new Response(JSON.stringify(result)));
    vi.stubGlobal("fetch", fetch);
    const client = new AntflyClient({ baseUrl: "http://localhost:8080" });
    const signal = new AbortController().signal;
    expect(
      await client.executeSQL(
        { statement: "SELECT $1", parameters: ["9223372036854775807"] },
        { signal }
      )
    ).toEqual(result);
    expect(fetch).toHaveBeenCalledWith(
      "http://localhost:8080/db/v1/sql",
      expect.objectContaining({ signal, method: "POST" })
    );
    expect(JSON.parse(fetch.mock.calls[0][1].body).parameters).toEqual(["9223372036854775807"]);
  });

  it("rejects malformed row widths", async () => {
    vi.stubGlobal(
      "fetch",
      vi
        .fn()
        .mockResolvedValue(
          new Response(
            JSON.stringify({ columns: [], rows: [[1]], rows_affected: 0, command_tag: "SELECT 1" })
          )
        )
    );
    const client = new AntflyClient({ baseUrl: "http://localhost:8080" });
    await expect(client.executeSQL({ statement: "SELECT 1" })).rejects.toThrow("row width");
  });

  it("enforces the transport response bound", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response(" ".repeat(65))));
    const client = new AntflyClient({ baseUrl: "http://localhost:8080" });
    await expect(
      client.executeSQL({ statement: "SELECT 1" }, { maxResponseBytes: 64 })
    ).rejects.toThrow("exceeded 64 bytes");
  });
});
