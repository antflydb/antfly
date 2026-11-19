import { afterEach, describe, expect, it, vi } from "vitest";
import { AntflyClient } from "../src/client.js";

const token = "A".repeat(171);
function observation() {
  return {
    index_type: "relational",
    index_name: "by id/#",
    publication_token: token,
    table_id: "18446744073709551615",
    publication_id: "2".repeat(32),
    plan_digest: "3".repeat(64),
    metadata_incarnation: "1".repeat(32),
    state: "admission_unknown",
    replication_pending: true,
    phase: "published",
  };
}
afterEach(() => vi.unstubAllGlobals());

describe("index publication recovery", () => {
  it("reads exact scoped identities with cancellation and no DDL replay", async () => {
    const calls: Request[] = [];
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
        const request = input instanceof Request ? input : new Request(input, init);
        calls.push(request);
        if (request.signal.aborted) throw request.signal.reason;
        return Response.json(observation());
      })
    );
    const client = new AntflyClient({ baseUrl: "https://antfly.test" });
    const status = await client.indexes.publication("items/a", "by id/#", token);
    expect(status.state).toBe("admission_unknown");
    await client.indexes.publication("items/a", "by id/#", token, {
      scope: { database: "warehouse", namespace: "sales" },
    });
    expect(calls.map((r) => r.url)).toEqual([
      "https://antfly.test/db/v1/tables/items%2Fa/indexes/by%20id%2F%23/publication",
      "https://antfly.test/db/v1/databases/warehouse/namespaces/sales/tables/items%2Fa/indexes/by%20id%2F%23/publication",
    ]);
    expect(
      calls.every(
        (r) => r.method === "GET" && r.headers.get("X-Antfly-Index-Publication-Token") === token
      )
    ).toBe(true);
    await expect(client.indexes.publication("items/a", "by id/#", "bad")).rejects.toThrow("token");
    await expect(client.indexes.publication("items/a", "by id/#", `${token}\n`)).rejects.toThrow(
      "token"
    );
    expect(calls).toHaveLength(2);
    const controller = new AbortController();
    controller.abort();
    await expect(
      client.indexes.publication("items/a", "by id/#", token, { signal: controller.signal })
    ).rejects.toBeDefined();
    expect(calls).toHaveLength(3);
  });

  it.each([
    "foreign",
    "unconfirmed",
    "invalid_state",
    "oversize",
  ])("rejects %s responses without retries", async (mode) => {
    const fetch = vi.fn(async () => {
      if (mode === "oversize") return new Response("x".repeat(16385));
      const body = observation();
      if (mode === "foreign") body.publication_token = "B".repeat(171);
      if (mode === "unconfirmed") body.state = "published";
      if (mode === "invalid_state") body.state = "unknown";
      return Response.json(body);
    });
    vi.stubGlobal("fetch", fetch);
    const client = new AntflyClient({ baseUrl: "https://antfly.test" });
    await expect(client.indexes.publication("items/a", "by id/#", token)).rejects.toThrow();
    expect(fetch).toHaveBeenCalledTimes(1);
  });
});
