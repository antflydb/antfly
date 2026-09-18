import { afterEach, describe, expect, it, vi } from "vitest";
import { AntflyClient } from "../src/client.js";
import type { IndexMaintenanceRequest } from "../src/index.js";

function fixture(): IndexMaintenanceRequest {
  return {
    table_id: "9007199254740999",
    schema_version: 7,
    owners: ["9007199254740993", "9007199254740994"].map((group_id) => ({
      group_id,
      generation: "9007199254740995",
      slot: 0,
      maintenance_epoch: "0",
      owner: "a".repeat(64),
      comparison: "b".repeat(64),
      progress_digest: "c".repeat(64),
    })),
  };
}

afterEach(() => vi.unstubAllGlobals());

describe("shared index maintenance", () => {
  it("preserves proofs, escapes path segments, and propagates cancellation", async () => {
    const controller = new AbortController();
    const calls: Request[] = [];
    const bodies: unknown[] = [];
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
        const request = input instanceof Request ? input : new Request(input, init);
        calls.push(request);
        if (request.signal.aborted) throw request.signal.reason;
        bodies.push(await request.json());
        return Response.json({ acknowledged_groups: ["9007199254740994", "9007199254740993"] });
      })
    );
    const client = new AntflyClient({ baseUrl: "https://antfly.test" });
    const request = fixture();
    request.schema_version = 0;
    const before = JSON.stringify(request);
    await client.indexes.retry("wiki/media", "by id/#", request, { signal: controller.signal });
    await client.indexes.repair("wiki/media", "by id/#", request);
    expect(calls.map((call) => call.url)).toEqual([
      "https://antfly.test/db/v1/tables/wiki%2Fmedia/indexes/by%20id%2F%23/retry",
      "https://antfly.test/db/v1/tables/wiki%2Fmedia/indexes/by%20id%2F%23/repair",
    ]);
    expect(bodies).toEqual([request, request]);
    expect(JSON.stringify(request)).toBe(before);
    controller.abort();
    await expect(
      client.indexes.retry("rows", "by_id", request, { signal: controller.signal })
    ).rejects.toThrow();
    expect(calls).toHaveLength(3);
  });

  it.each([
    {},
    { acknowledged_groups: [] },
    { acknowledged_groups: ["9007199254740993"] },
    { acknowledged_groups: ["9007199254740993", "9007199254740993"] },
    { acknowledged_groups: ["9007199254740993", "12"] },
    { acknowledged_groups: [42, "9007199254740994"] },
    { acknowledged_groups: null },
  ])("rejects incomplete or malformed acknowledgement %j without automatic retry", async (response) => {
    const fetch = vi.fn(async () => Response.json(response));
    vi.stubGlobal("fetch", fetch);
    const client = new AntflyClient({ baseUrl: "https://antfly.test" });
    await expect(client.indexes.retry("rows", "by_id", fixture())).rejects.toThrow(
      "acknowledgement"
    );
    expect(fetch).toHaveBeenCalledTimes(1);
  });

  it("rejects invalid or excessive proofs before network admission", async () => {
    const fetch = vi.fn();
    vi.stubGlobal("fetch", fetch);
    const client = new AntflyClient({ baseUrl: "https://antfly.test" });
    const oversized = fixture();
    oversized.owners = Array.from({ length: 129 }, () => ({ ...oversized.owners[0] }));
    await expect(client.indexes.repair("rows", "by_id", oversized)).rejects.toThrow("128");
    const duplicate = fixture();
    duplicate.owners[1].group_id = duplicate.owners[0].group_id;
    await expect(client.indexes.retry("rows", "by_id", duplicate)).rejects.toThrow("duplicate");
    const invalid = fixture();
    invalid.owners[0].maintenance_epoch = "18446744073709551616";
    await expect(client.indexes.retry("rows", "by_id", invalid)).rejects.toThrow("invalid");
    expect(fetch).not.toHaveBeenCalled();
  });

  it("bounds acknowledgement bytes and requires the canonical HTTP outcome", async () => {
    const body = { acknowledged_groups: ["9007199254740993", "9007199254740994"] };
    const fetch = vi
      .fn()
      .mockResolvedValueOnce(Response.json({ ...body, padding: "x".repeat(33 * 1024) }))
      .mockResolvedValueOnce(Response.json(body, { status: 202 }));
    vi.stubGlobal("fetch", fetch);
    const client = new AntflyClient({ baseUrl: "https://antfly.test" });
    await expect(client.indexes.retry("rows", "by_id", fixture())).rejects.toThrow("exceeded");
    await expect(client.indexes.repair("rows", "by_id", fixture())).rejects.toThrow("status");
    expect(fetch).toHaveBeenCalledTimes(2);
  });
});
