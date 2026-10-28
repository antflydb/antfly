import { afterEach, expect, it, vi } from "vitest";
import { AntflyClient } from "../src/client.js";
import { AntflyHTTPError, AntflyResponseError } from "../src/errors.js";
import { InferenceClient } from "../src/inference-client.js";

afterEach(() => vi.unstubAllGlobals());
it("does not invent counts for an empty batch response", async () => {
  vi.stubGlobal(
    "fetch",
    vi.fn(async () => new Response(null, { status: 204 }))
  );
  const client = new AntflyClient({ baseUrl: "http://localhost" });
  await expect(client.tables.batch("t", { inserts: { a: {} } })).rejects.toBeInstanceOf(
    AntflyResponseError
  );
});
it.each([404, 401, 500])("preserves lookup HTTP status %i", async (status) => {
  vi.stubGlobal(
    "fetch",
    vi.fn(
      async () =>
        new Response(JSON.stringify({ error: "failure" }), {
          status,
          headers: { "content-type": "application/json" },
        })
    )
  );
  const client = new AntflyClient({ baseUrl: "http://localhost" });
  await expect(client.tables.lookup("t", "a")).rejects.toMatchObject({
    status,
    body: { error: "failure" },
  });
  await expect(client.tables.get("t")).rejects.toBeInstanceOf(AntflyHTTPError);
  await expect(client.indexes.get("t", "v")).rejects.toMatchObject({ status });
});
it("preserves batch statuses", async () => {
  vi.stubGlobal(
    "fetch",
    vi.fn(
      async () =>
        new Response(
          JSON.stringify({ status: "committed_repair_required", failure: { retryable: false } })
        )
    )
  );
  const client = new AntflyClient({ baseUrl: "http://localhost" });
  expect(await client.tables.batch("t", { inserts: { a: {} } })).toMatchObject({
    status: "committed_repair_required",
  });
});
it.each(["embedDense", "rerank"])("forwards %s cancellation", async (method) => {
  const controller = new AbortController();
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const signal = input instanceof Request ? input.signal : init?.signal;
      expect(signal).toBeTruthy();
      controller.abort();
      expect(signal?.aborted).toBe(true);
      throw signal?.reason;
    })
  );
  const client = new InferenceClient({ baseUrl: "http://localhost" });
  await expect(
    method === "embedDense"
      ? client.embedDense("model", ["text"], { signal: controller.signal })
      : client.rerank("model", "query", ["text"], { signal: controller.signal })
  ).rejects.toThrow();
});
