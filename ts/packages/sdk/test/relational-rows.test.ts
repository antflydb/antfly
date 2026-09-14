import { afterEach, describe, expect, it, vi } from "vitest";
import { AntflyClient } from "../src/client.js";

afterEach(() => vi.restoreAllMocks());

describe("typed relational rows", () => {
  it("reports retirement acceptance without implying table deletion", async () => {
    const fetch = vi
      .spyOn(globalThis, "fetch")
      .mockResolvedValueOnce(new Response('{"status":"accepted"}', { status: 202 }));
    const client = new AntflyClient({ baseUrl: "http://localhost:8080" });
    expect(await client.tables.constraints.retire("t", { schema_version: 2, drop: true })).toEqual({
      status: "accepted",
    });
    expect(fetch).toHaveBeenCalledOnce();
    expect(fetch.mock.calls[0]?.[0]).toBe(
      "http://localhost:8080/db/v1/tables/t/constraints/retire"
    );
    expect(JSON.parse(fetch.mock.calls[0]?.[1]?.body as string)).toEqual({
      schema_version: 2,
      drop: true,
    });
  });
  it("keeps administrative repair precision and pending outcomes", async () => {
    const fetch = vi
      .spyOn(globalThis, "fetch")
      .mockResolvedValueOnce(
        new Response('{"status":"committed_pending","inserted":1,"deleted":0}', { status: 202 })
      );
    const client = new AntflyClient({ baseUrl: "http://localhost:8080" });
    const outcome = await client.tables.constraints.repair("table name", {
      schema_version: 2,
      mutations: [{ key: "b", expected_version: "18446744073709551615", row: { id: 2 } }],
    });
    expect(outcome.status).toBe("committed_pending");
    expect(fetch).toHaveBeenCalledOnce();
    expect(fetch.mock.calls[0]?.[0]).toBe(
      "http://localhost:8080/db/v1/tables/table%20name/constraints/repair"
    );
    await expect(
      client.tables.constraints.repair("t", {
        schema_version: 2,
        mutations: [{ key: "b", expected_version: "1", row: { id: 9007199254740992 } }],
      })
    ).rejects.toThrow("safe JavaScript integers");
    expect(fetch).toHaveBeenCalledOnce();
  });

  it("reports constraint retry acceptance separately from completion", async () => {
    const fetch = vi
      .spyOn(globalThis, "fetch")
      .mockResolvedValueOnce(new Response('{"status":"accepted"}', { status: 202 }));
    const client = new AntflyClient({ baseUrl: "http://localhost:8080" });
    expect(await client.tables.constraints.retry("t", { schema_version: 2 })).toEqual({
      status: "accepted",
    });
    expect(fetch).toHaveBeenCalledOnce();
    expect(fetch.mock.calls[0]?.[0]).toBe("http://localhost:8080/db/v1/tables/t/constraints/retry");
  });

  it("rejects unsafe integer operands and row values before sending", async () => {
    const fetch = vi.spyOn(globalThis, "fetch");
    const client = new AntflyClient({ baseUrl: "http://localhost:8080" });
    await expect(
      client.tables.rows.queryRaw("t", {
        fields: ["id"],
        conditions: [{ column: "id", op: "eq", value: 9007199254740992 }],
      })
    ).rejects.toThrow("safe JavaScript integers");
    await expect(
      client.tables.rows.mutate("t", {
        schema_version: 7,
        mutations: [{ key: "a", expected_version: "0", row: { id: 9007199254740992 } }],
      })
    ).rejects.toThrow("safe JavaScript integers");
    expect(fetch).not.toHaveBeenCalled();
  });

  it("rejects nonfinite cells rather than serializing them as null", async () => {
    const fetch = vi.spyOn(globalThis, "fetch");
    const client = new AntflyClient({ baseUrl: "http://localhost:8080" });
    await expect(
      client.tables.rows.mutate("t", {
        schema_version: 7,
        mutations: [{ key: "a", expected_version: "0", row: { number: Number.NaN } }],
      })
    ).rejects.toThrow("finite");
    expect(fetch).not.toHaveBeenCalled();
  });

  it("validates the value actually emitted by custom JSON serialization", async () => {
    const fetch = vi.spyOn(globalThis, "fetch");
    const client = new AntflyClient({ baseUrl: "http://localhost:8080" });
    await expect(
      client.tables.rows.mutate("t", {
        schema_version: 7,
        mutations: [
          { key: "a", expected_version: "0", row: { id: { toJSON: () => 9007199254740992 } } },
        ],
      })
    ).rejects.toThrow("safe JavaScript integers");
    expect(fetch).not.toHaveBeenCalled();
  });
  it("preserves exact int64 cells and versions as raw NDJSON", async () => {
    const page =
      '{"_id":"a","row":{"n":9223372036854775807},"version":"18446744073709551615","schema_version":7}\n';
    const fetch = vi.spyOn(globalThis, "fetch").mockResolvedValueOnce(new Response(page));
    const client = new AntflyClient({
      baseUrl: "http://localhost:8080",
      auth: { apiKey: "secret" },
    });
    expect(
      await client.tables.rows.queryRaw("table name", { fields: ["n"], schema_version: 7 })
    ).toBe(page);
    expect(fetch).toHaveBeenCalledOnce();
    expect(fetch.mock.calls[0]?.[0]).toBe(
      "http://localhost:8080/db/v1/tables/table%20name/rows/query"
    );
  });

  it("preserves committed-pending mutation outcomes without retrying", async () => {
    const outcome = { status: "committed_pending", inserted: 1, deleted: 0 };
    const fetch = vi
      .spyOn(globalThis, "fetch")
      .mockResolvedValueOnce(new Response(JSON.stringify(outcome), { status: 202 }));
    const client = new AntflyClient({ baseUrl: "http://localhost:8080" });
    const request = {
      schema_version: 7,
      mutations: [{ key: "a", expected_version: "18446744073709551615", row: { name: "next" } }],
    };
    expect(await client.tables.rows.mutate("t", request)).toEqual(outcome);
    expect(fetch).toHaveBeenCalledOnce();
    expect(JSON.parse(fetch.mock.calls[0]?.[1]?.body as string)).toEqual(request);
  });

  it("does not retry a conditional mutation conflict", async () => {
    const fetch = vi
      .spyOn(globalThis, "fetch")
      .mockResolvedValueOnce(
        new Response('{"error":"foreign key parent missing"}', { status: 409 })
      );
    const client = new AntflyClient({ baseUrl: "http://localhost:8080" });
    await expect(
      client.tables.rows.mutate("t", {
        schema_version: 7,
        mutations: [{ key: "a", expected_version: "0", row: {} }],
      })
    ).rejects.toThrow("409");
    expect(fetch).toHaveBeenCalledOnce();
  });

  it("rejects an empty successful mutation response as an unknown outcome", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValueOnce(new Response(null, { status: 201 }));
    const client = new AntflyClient({ baseUrl: "http://localhost:8080" });
    await expect(
      client.tables.rows.mutate("t", {
        schema_version: 7,
        mutations: [{ key: "a", expected_version: "1" }],
      })
    ).rejects.toThrow("no commit outcome");
  });
});
