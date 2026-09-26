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

describe("durable SQL prepared client", () => {
  const prepared = {
    prepared_id: "opaque-resource",
    expires_at_ms: 1800000000000,
    owner_node_id: "9007199254740993",
    parameter_types: ["integer"],
    columns: [{ name: "value", type: "integer" }],
  };
  const result = {
    columns: [{ name: "value", type: "integer" }],
    rows: [["9223372036854775807"]],
    sql_nulls: [[false]],
    rows_affected: 1,
    command_tag: "INSERT 1",
    mutation_outcome: "committed_repair_required",
    transaction_id: "receipt",
  };

  it("prepares, executes and closes with explicit owner metadata and bounded no-replay transport", async () => {
    const fetch = vi
      .fn()
      .mockResolvedValueOnce(new Response(JSON.stringify(prepared)))
      .mockResolvedValueOnce(new Response(JSON.stringify(result)))
      .mockResolvedValueOnce(new Response("{}"));
    vi.stubGlobal("fetch", fetch);
    const client = new AntflyClient({
      baseUrl: "http://localhost:8080",
      auth: { type: "token", token: "credential" },
    });
    const signal = new AbortController().signal;
    expect(
      await client.prepareSQL(
        {
          statement: "INSERT INTO items (n) VALUES ($1) RETURNING n",
          database: "analytics",
          namespace: "reports",
        },
        { signal }
      )
    ).toEqual(prepared);
    expect(
      await client.executePreparedSQL(
        prepared.prepared_id,
        { parameters: ["9223372036854775807"], session_id: "session", limit: 10 },
        { signal }
      )
    ).toEqual(result);
    await client.closePreparedSQL(prepared.prepared_id, { signal });
    expect(fetch).toHaveBeenCalledTimes(3);
    for (const [, options] of fetch.mock.calls) {
      expect(options).toMatchObject({
        redirect: "error",
        credentials: "omit",
        signal,
        headers: { Authorization: "Bearer credential" },
      });
    }
    expect(fetch.mock.calls.map(([url]) => url)).toEqual([
      "http://localhost:8080/db/v1/sql/prepared",
      "http://localhost:8080/db/v1/sql/prepared/opaque-resource/execute",
      "http://localhost:8080/db/v1/sql/prepared/opaque-resource",
    ]);
    expect(JSON.parse(fetch.mock.calls[1][1].body)).toEqual({
      parameters: ["9223372036854775807"],
      session_id: "session",
      limit: 10,
    });
    expect(fetch.mock.calls[2][1]).toMatchObject({ method: "DELETE", body: undefined });
  });

  it("encodes opaque resource paths and permits parameterless execution", async () => {
    const fetch = vi
      .fn()
      .mockResolvedValueOnce(new Response(JSON.stringify(result)))
      .mockResolvedValueOnce(new Response("{}"));
    vi.stubGlobal("fetch", fetch);
    const client = new AntflyClient({ baseUrl: "http://localhost:8080" });
    await client.executePreparedSQL("a/b?c#d");
    await client.closePreparedSQL("a/b?c#d");
    expect(fetch.mock.calls[0][0]).toBe(
      "http://localhost:8080/db/v1/sql/prepared/a%2Fb%3Fc%23d/execute"
    );
    expect(fetch.mock.calls[0][1].body).toBe("{}");
    expect(fetch.mock.calls[1][0]).toBe("http://localhost:8080/db/v1/sql/prepared/a%2Fb%3Fc%23d");
  });

  it.each([
    "prepare",
    "execute",
    "close",
  ])("preserves SQL diagnostics without retries for %s", async (operation) => {
    const diagnostic = {
      code: "40003",
      message: "outcome unknown",
      retryable: false,
      transaction_id: "reconcile-me",
    };
    const fetch = vi
      .fn()
      .mockResolvedValue(new Response(JSON.stringify(diagnostic), { status: 503 }));
    vi.stubGlobal("fetch", fetch);
    const client = new AntflyClient({ baseUrl: "http://localhost:8080" });
    const work =
      operation === "prepare"
        ? client.prepareSQL({ statement: "SELECT 1" })
        : operation === "execute"
          ? client.executePreparedSQL("id")
          : client.closePreparedSQL("id");
    const error = await work.catch((error: unknown) => error);
    expect(error).toBeInstanceOf(SQLExecutionError);
    expect((error as SQLExecutionError).diagnostic).toEqual(diagnostic);
    expect(fetch).toHaveBeenCalledTimes(1);
  });

  it.each([
    "prepare",
    "execute",
    "close",
  ])("does not replay transport failure for %s", async (operation) => {
    const fetch = vi.fn().mockRejectedValue(new TypeError("network response lost"));
    vi.stubGlobal("fetch", fetch);
    const client = new AntflyClient({ baseUrl: "http://localhost:8080" });
    const work =
      operation === "prepare"
        ? client.prepareSQL({ statement: "SELECT 1" })
        : operation === "execute"
          ? client.executePreparedSQL("id")
          : client.closePreparedSQL("id");
    await expect(work).rejects.toThrow("network response lost");
    expect(fetch).toHaveBeenCalledTimes(1);
    expect(fetch.mock.calls[0][1].redirect).toBe("error");
  });

  it("rejects unsafe nested parameters, missing IDs and oversized requests before dispatch", async () => {
    const fetch = vi.fn();
    vi.stubGlobal("fetch", fetch);
    const client = new AntflyClient({ baseUrl: "http://localhost:8080" });
    await expect(
      client.executePreparedSQL("id", { parameters: [{ nested: [9007199254740992] }] })
    ).rejects.toThrow("Relational numbers");
    await expect(client.executePreparedSQL("")).rejects.toThrow("resource ID");
    await expect(client.closePreparedSQL("")).rejects.toThrow("resource ID");
    await expect(
      client.prepareSQL({ statement: "x".repeat(4 << 20) }, { maxRequestBytes: 64 << 20 })
    ).rejects.toThrow("4194304 bytes");
    await expect(
      client.executePreparedSQL(
        "id",
        { parameters: ["x".repeat(4 << 20)] },
        { maxRequestBytes: 64 << 20 }
      )
    ).rejects.toThrow("4194304 bytes");
    expect(fetch).not.toHaveBeenCalled();
  });

  it("bounds prepare and close responses and validates execution row widths", async () => {
    const fetch = vi
      .fn()
      .mockResolvedValueOnce(new Response(" ".repeat(65)))
      .mockResolvedValueOnce(new Response(" ".repeat(65)))
      .mockResolvedValueOnce(new Response(JSON.stringify({ ...result, rows: [[1, 2]] })));
    vi.stubGlobal("fetch", fetch);
    const client = new AntflyClient({ baseUrl: "http://localhost:8080" });
    await expect(
      client.prepareSQL({ statement: "SELECT 1" }, { maxResponseBytes: 64 })
    ).rejects.toThrow("exceeded 64 bytes");
    await expect(client.closePreparedSQL("id", { maxResponseBytes: 64 })).rejects.toThrow(
      "exceeded 64 bytes"
    );
    await expect(client.executePreparedSQL("id")).rejects.toThrow("row width");
  });

  it("rejects malformed prepared metadata and close acknowledgments", async () => {
    const fetch = vi
      .fn()
      .mockResolvedValueOnce(new Response("{}"))
      .mockResolvedValueOnce(new Response("[]"));
    vi.stubGlobal("fetch", fetch);
    const client = new AntflyClient({ baseUrl: "http://localhost:8080" });
    await expect(client.prepareSQL({ statement: "SELECT 1" })).rejects.toThrow(
      "Invalid prepared SQL response"
    );
    await expect(client.closePreparedSQL("id")).rejects.toThrow(
      "Invalid prepared SQL close response"
    );
  });

  it.each([
    9007199254740992,
    "9e18",
    "-1",
  ])("rejects an inexact or malformed owner identity %s", async (owner) => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(new Response(JSON.stringify({ ...prepared, owner_node_id: owner })))
    );
    const client = new AntflyClient({ baseUrl: "http://localhost:8080" });
    await expect(client.prepareSQL({ statement: "SELECT 1" })).rejects.toThrow(
      "Invalid prepared SQL response"
    );
  });
});

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
