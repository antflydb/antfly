import { afterEach, describe, expect, it, vi } from "vitest";
import { AntflyClient } from "../src/client.js";

const token = "A".repeat(182);
afterEach(() => vi.unstubAllGlobals());

describe("schema validation recovery", () => {
  it("reads exact scoped jobs and rejects foreign, oversized or incomplete completion without DDL replay", async () => {
    const calls: Request[] = [];
    let mode = "valid";
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
        const request = input instanceof Request ? input : new Request(input, init);
        calls.push(request);
        if (request.signal.aborted) throw request.signal.reason;
        if (mode === "oversize") return new Response("x".repeat(16385));
        return Response.json({
          schema_validation_token: mode === "foreign" ? "B".repeat(182) : token,
          table_id: "18446744073709551615",
          active_schema_version: 2,
          state: ["incomplete", "negative"].includes(mode) ? "succeeded" : "admission_unknown",
          ...(mode === "incomplete"
            ? { phase: "succeeded", owner_count: 1, ready: 1, installed: 1, released: 0 }
            : {}),
          ...(mode === "negative"
            ? { phase: "succeeded", owner_count: -1, ready: -1, installed: -1, released: -1 }
            : {}),
        });
      })
    );
    const client = new AntflyClient({ baseUrl: "https://antfly.test" });
    const result = await client.tables.schemaPublication("items/a", token);
    expect(result.state).toBe("admission_unknown");
    expect(result.table_id).toBe("18446744073709551615");
    await client.tables.schemaPublication("items/a", token, {
      scope: { database: "warehouse", namespace: "sales" },
    });
    expect(calls.map((r) => r.url)).toEqual([
      "https://antfly.test/db/v1/tables/items%2Fa/schema/publication",
      "https://antfly.test/db/v1/databases/warehouse/namespaces/sales/tables/items%2Fa/schema/publication",
    ]);
    for (mode of ["foreign", "oversize", "incomplete", "negative"]) {
      const before = calls.length;
      await expect(client.tables.schemaPublication("items/a", token)).rejects.toThrow();
      expect(calls).toHaveLength(before + 1);
    }
    expect(
      calls.every(
        (r) => r.method === "GET" && r.headers.get("X-Antfly-Schema-Validation-Token") === token
      )
    ).toBe(true);
    const before = calls.length;
    await expect(client.tables.schemaPublication("items/a", "bad")).rejects.toThrow("token");
    expect(calls).toHaveLength(before);
    const controller = new AbortController();
    controller.abort();
    await expect(
      client.tables.schemaPublication("items/a", token, { signal: controller.signal })
    ).rejects.toBeDefined();
  });
});
