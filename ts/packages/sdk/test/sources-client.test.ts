import { describe, expect, it, vi } from "vitest";
import { SourceServiceError, StandaloneSourcesClient } from "../src/sources-client.js";

describe("StandaloneSourcesClient", () => {
  it("uses the companion protocol and prevents credential forwarding through redirects", async () => {
    const fetcher = vi.fn<typeof fetch>().mockResolvedValue(
      new Response(JSON.stringify({ version: 1, id: "job", state: "running", started_at: 1 }), {
        status: 202,
      })
    );
    const client = new StandaloneSourcesClient({
      baseUrl: "https://sources.example",
      token: "operator-token",
      fetch: fetcher,
    });
    expect((await client.sync({ restart: true })).id).toBe("job");
    const [url, init] = fetcher.mock.calls[0] ?? [];
    expect(url).toBe("https://sources.example/v1/sync");
    expect(init?.redirect).toBe("error");
    expect(init?.headers).toEqual({
      Authorization: "Bearer operator-token",
      "Content-Type": "application/json",
    });
    expect(JSON.parse(init?.body as string)).toEqual({ restart: true });
  });
  it("syncs a single source by source_id, for a per-source run after an upload", async () => {
    const fetcher = vi.fn<typeof fetch>().mockResolvedValue(
      new Response(JSON.stringify({ version: 1, id: "job", state: "running", started_at: 1 }), {
        status: 202,
      })
    );
    const client = new StandaloneSourcesClient({
      baseUrl: "https://sources.example",
      token: "operator-token",
      fetch: fetcher,
    });
    await client.sync({ sourceId: "11111111-1111-1111-1111-111111111111", requestKey: "key-1" });
    const [, init] = fetcher.mock.calls[0] ?? [];
    expect(JSON.parse(init?.body as string)).toEqual({
      source_id: "11111111-1111-1111-1111-111111111111",
      restart: false,
      request_key: "key-1",
    });
  });
  it("preserves service errors without exposing tokens", async () => {
    const fetcher = vi
      .fn<typeof fetch>()
      .mockImplementation(
        async () => new Response('{"version":1,"code":"sync_in_progress"}', { status: 409 })
      );
    const client = new StandaloneSourcesClient({
      baseUrl: "https://sources.example",
      token: "operator-token",
      fetch: fetcher,
    });
    await expect(client.sync()).rejects.toMatchObject({ status: 409, code: "sync_in_progress" });
    await expect(client.sync()).rejects.toBeInstanceOf(SourceServiceError);
  });
  it("encodes identifiers and sends secrets only in request bodies", async () => {
    const fetcher = vi
      .fn<typeof fetch>()
      .mockImplementation(async () => new Response('{"version":1,"state":"updated"}'));
    const client = new StandaloneSourcesClient({
      baseUrl: "https://sources.example",
      token: "operator-token",
      fetch: fetcher,
    });
    await client.putSecret("bucket.secret", "private-value");
    expect(fetcher.mock.calls[0]?.[0]).toBe("https://sources.example/v1/secrets/bucket.secret");
    expect(fetcher.mock.calls[0]?.[1]?.body).toBe('{"value":"private-value"}');
    await client.job("a/b");
    expect(fetcher.mock.calls[1]?.[0]).toBe("https://sources.example/v1/jobs/a%2Fb");
    await client.purgeSource("a/b");
    expect(fetcher.mock.calls[2]?.[0]).toBe("https://sources.example/v1/sources/a%2Fb?purge=true");
    await client.runPurges();
    expect(fetcher.mock.calls[3]?.[0]).toBe("https://sources.example/v1/sync");
    expect(fetcher.mock.calls[3]?.[1]?.body).toBe('{"purge_only":true}');
  });
  it.each([
    "http://remote.example",
    "https://user:pass@example.com",
    "https://example.com?token=x",
  ])("rejects unsafe endpoint %s", (baseUrl) => {
    expect(() => new StandaloneSourcesClient({ baseUrl, token: "token" })).toThrow();
  });
});

it("targets the common hosted mount and sends a stable idempotency key", async () => {
  const { SourcesClient } = await import("../src/sources-client.js");
  const fetcher = vi
    .fn<typeof fetch>()
    .mockImplementation(async () => new Response('{"id":"run"}'));
  const baseUrl =
    "https://colony.example/api/v1/organizations/org/cloud/instances/instance/sources/v1";
  const client = new SourcesClient({ baseUrl, token: "hosted-token", fetch: fetcher });
  await client.startRun("source", "submission-1");
  await client.runs("source", { cursor: "opaque/cursor" });
  expect(fetcher.mock.calls[0]?.[0]).toBe(`${baseUrl}/sources/source/runs`);
  expect(fetcher.mock.calls[0]?.[1]?.headers).toMatchObject({ "Idempotency-Key": "submission-1" });
  expect(fetcher.mock.calls[1]?.[0]).toBe(`${baseUrl}/sources/source/runs?cursor=opaque%2Fcursor`);
  expect(fetcher).toHaveBeenCalledTimes(2);
});
