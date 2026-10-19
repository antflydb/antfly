import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { afterEach, expect, it, vi } from "vitest";
import { AntflyClient } from "../src/client.js";
import { RequestAdmission } from "../src/request-admission.js";

const baseUrl = "http://localhost:8080";
afterEach(() => vi.unstubAllGlobals());

it.each([
  "scan",
  "scanAll",
] as const)("refuses an already aborted %s without sending", async (method) => {
  const fetch = vi.fn();
  vi.stubGlobal("fetch", fetch);
  const client = new AntflyClient({ baseUrl });
  const abort = new AbortController();
  const reason = new Error("original scan canceled");
  abort.abort(reason);
  const options = { signal: abort.signal };
  const pending =
    method === "scan"
      ? client.tables.scan("items", undefined, options).next()
      : client.tables.scanAll("items", undefined, options);
  await expect(pending).rejects.toBe(reason);
  expect(fetch).not.toHaveBeenCalled();
});

it.each([
  "scan",
  "scanAll",
] as const)("unlinks a canceled queued %s and preserves its peer", async (method) => {
  const sent: string[] = [];
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL) => {
      sent.push(input instanceof Request ? input.url : String(input));
      return new Response(new ReadableStream({}, { highWaterMark: 0 }));
    })
  );
  const owner = new RequestAdmission({ maxConcurrent: 1, maxQueued: 2 });
  const client = new AntflyClient({ baseUrl, requestAdmission: owner });
  const held = await owner.fetch(`${baseUrl}/held`);
  const abort = new AbortController();
  const options = { signal: abort.signal };
  const pending = (
    method === "scan"
      ? client.tables.scan("items", undefined, options).next()
      : client.tables.scanAll("items", undefined, options)
  ).catch((error: unknown) => error);
  const peer = owner.fetch(`${baseUrl}/peer`);
  expect(owner.stats.queued).toBe(2);
  const reason = new Error("original queued scan canceled");
  abort.abort(reason);
  expect(await pending).toBe(reason);
  expect(owner.stats).toEqual({ active: 1, queued: 1, closed: false });
  expect(sent).toEqual([`${baseUrl}/held`]);
  await held.body?.cancel();
  const survivor = await peer;
  await survivor.body?.cancel();
  expect(sent).toEqual([`${baseUrl}/held`, `${baseUrl}/peer`]);
  expect(owner.stats).toEqual({ active: 0, queued: 0, closed: false });
});

it.each([
  1, 2,
])("stops a scan with %i buffered rows after cancellation and retires the body", async (count) => {
  const cancel = vi.fn();
  vi.stubGlobal(
    "fetch",
    vi.fn(
      async () =>
        new Response(
          new ReadableStream({
            start(controller) {
              controller.enqueue(
                new TextEncoder().encode(
                  count === 1 ? '{"_id":"one"}\n' : '{"_id":"one"}\n{"_id":"two"}\n'
                )
              );
            },
            cancel,
          })
        )
    )
  );
  const owner = new RequestAdmission({ maxConcurrent: 1 });
  const client = new AntflyClient({ baseUrl, requestAdmission: owner });
  const abort = new AbortController();
  const scan = client.tables.scan("items", undefined, { signal: abort.signal });
  expect(await scan.next()).toMatchObject({ value: { _id: "one" }, done: false });
  const reason = new Error("original buffered scan canceled");
  abort.abort(reason);
  await expect(scan.next()).rejects.toBe(reason);
  expect(cancel).toHaveBeenCalledOnce();
  expect(owner.stats).toEqual({ active: 0, queued: 0, closed: false });
});

it.each([
  "scan",
  "scanAll",
] as const)("aborts a real pending %s body and admits a peer", async (method) => {
  let reach!: () => void;
  const reached = new Promise<void>((resolve) => {
    reach = resolve;
  });
  const sent: string[] = [];
  const server = createServer((input, output) => {
    sent.push(input.url ?? "");
    if (input.url === "/db/v1/status") {
      output.setHeader("Content-Type", "application/json");
      output.end("{}");
    } else {
      output.setHeader("Content-Type", "application/x-ndjson");
      output.write('{"_id":');
      reach();
    }
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const owner = new RequestAdmission({ maxConcurrent: 1 });
  const client = new AntflyClient({
    baseUrl: `http://127.0.0.1:${(server.address() as AddressInfo).port}`,
    requestAdmission: owner,
  });
  const abort = new AbortController();
  const options = { signal: abort.signal };
  const pending = (
    method === "scan"
      ? client.tables.scan("items", undefined, options).next()
      : client.tables.scanAll("items", undefined, options)
  ).catch((error: unknown) => error);
  try {
    await reached;
    expect(owner.stats.active).toBe(1);
    abort.abort(new Error("original pending read canceled"));
    expect(await pending).toBeInstanceOf(Error);
    await vi.waitFor(() => expect(owner.stats.active).toBe(0));
    await client.api.GET("/db/v1/status");
    expect(sent).toEqual(["/db/v1/tables/items/documents", "/db/v1/status"]);
    expect(owner.stats).toEqual({ active: 0, queued: 0, closed: false });
  } finally {
    abort.abort();
    owner.close();
    server.closeAllConnections();
    await new Promise<void>((resolve, reject) =>
      server.close((error) => (error ? reject(error) : resolve()))
    );
    await pending;
  }
});
