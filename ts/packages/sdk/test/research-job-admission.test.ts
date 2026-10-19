import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { afterEach, expect, it, vi } from "vitest";
import { AntflyClient } from "../src/client.js";
import { RequestAdmission } from "../src/request-admission.js";
import type { ResearchAgentRequest } from "../src/types.js";

const request: ResearchAgentRequest = { query: "bounded research", queries: [{ table: "docs" }] };
const job = { job_id: "job1", state: "running", phase: "plan", advances: 0 };
const baseUrl = "http://localhost:8080";
afterEach(() => {
  vi.unstubAllGlobals();
  vi.useRealTimers();
});

it.each([
  "create",
  "advance",
  "status",
] as const)("aborts a real research %s response body and makes capacity available to a peer", async (stage) => {
  const sent: string[] = [];
  let reach!: () => void;
  const reached = new Promise<void>((resolve) => {
    reach = resolve;
  });
  const server = createServer((input, output) => {
    sent.push(input.url ?? "");
    output.setHeader("Content-Type", "application/json");
    if (input.url === "/db/v1/status") {
      output.end("{}");
      return;
    }
    const current = input.url?.endsWith("/advance")
      ? "advance"
      : input.method === "GET"
        ? "status"
        : "create";
    if (current === stage) {
      output.write("{");
      reach();
    } else if (current === "advance") {
      output.writeHead(409, { "Retry-After": "1" });
      output.end('{"error":"conflict"}');
    } else output.end(JSON.stringify(job));
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const owner = new RequestAdmission({ maxConcurrent: 1 });
  const client = new AntflyClient({
    baseUrl: `http://127.0.0.1:${(server.address() as AddressInfo).port}`,
    requestAdmission: owner,
  });
  const abort = new AbortController();
  const pending = client
    .runResearchJob(request, { signal: abort.signal })
    .catch((error: unknown) => error);
  try {
    await reached;
    expect(owner.stats.active).toBe(1);
    abort.abort(new Error("original body lifetime ended"));
    expect(await pending).toBeInstanceOf(Error);
    await vi.waitFor(() => expect(owner.stats.active).toBe(0));
    await client.api.GET("/db/v1/status");
    expect(sent.at(-1)).toBe("/db/v1/status");
    expect(sent.some((url) => url.endsWith("/cancel"))).toBe(false);
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

it("refuses an already canceled research workflow before creating a durable job", async () => {
  const fetch = vi.fn();
  vi.stubGlobal("fetch", fetch);
  const client = new AntflyClient({ baseUrl });
  const abort = new AbortController();
  const reason = new Error("original operation expired");
  abort.abort(reason);
  await expect(client.runResearchJob(request, { signal: abort.signal })).rejects.toBe(reason);
  expect(fetch).not.toHaveBeenCalled();
});

it.each([
  "create",
  "advance",
  "status",
] as const)("unlinks a canceled queued research %s and permits a surviving peer", async (stage) => {
  const sent: string[] = [];
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL) => {
      const url = input instanceof Request ? input.url : String(input);
      sent.push(url);
      return new Response(new ReadableStream({}, { highWaterMark: 0 }));
    })
  );
  const owner = new RequestAdmission({ maxConcurrent: 1, maxQueued: 2 });
  const client = new AntflyClient({ baseUrl, requestAdmission: owner });
  const held = await owner.fetch(`${baseUrl}/held`);
  const abort = new AbortController();
  const options = { signal: abort.signal };
  const pending = (
    stage === "create"
      ? client.startResearchJob({ request }, options)
      : stage === "advance"
        ? client.advanceResearchJob("job1", undefined, options)
        : client.getResearchJob("job1", options)
  ).catch((error: unknown) => error);
  const peer = owner.fetch(`${baseUrl}/peer`);
  expect(owner.stats.queued).toBe(2);
  const reason = new Error("original queued operation canceled");
  abort.abort(reason);
  expect(await pending).toBe(reason);
  expect(owner.stats).toEqual({ active: 1, queued: 1, closed: false });
  expect(sent).toEqual([`${baseUrl}/held`]);
  await held.body?.cancel();
  const survivor = await peer;
  expect(sent).toEqual([`${baseUrl}/held`, `${baseUrl}/peer`]);
  await survivor.body?.cancel();
  expect(owner.stats).toEqual({ active: 0, queued: 0, closed: false });
});

it.each([
  new Error("original deadline"),
  "original caller reason",
])("stops conflict backoff under the original signal and preserves reason %s", async (reason) => {
  vi.useFakeTimers();
  const sent: string[] = [];
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL) => {
      if (!(input instanceof Request)) throw new Error("expected generated request");
      sent.push(input.url);
      return input.url.endsWith("/advance")
        ? Response.json({ error: "conflict" }, { status: 409, headers: { "Retry-After": "2" } })
        : Response.json(job);
    })
  );
  const owner = new RequestAdmission({ maxConcurrent: 1 });
  const client = new AntflyClient({ baseUrl, requestAdmission: owner });
  const abort = new AbortController();
  const pending = client
    .runResearchJob(request, { signal: abort.signal })
    .catch((error: unknown) => error);
  await vi.waitFor(() => expect(sent).toHaveLength(2));
  expect(owner.stats.active).toBe(0);
  abort.abort(reason);
  expect(await pending).toBe(reason);
  await vi.advanceTimersByTimeAsync(2000);
  expect(sent).toHaveLength(2);
  expect(owner.stats).toEqual({ active: 0, queued: 0, closed: false });
});

it.each([
  "create",
  "advance",
  "status",
] as const)("carries the original workflow cancellation through active %s and retires admission", async (stage) => {
  vi.useFakeTimers();
  const sent: string[] = [];
  let captured: AbortSignal | undefined;
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL) => {
      if (!(input instanceof Request)) throw new Error("expected generated request");
      sent.push(input.url);
      const current = input.url.endsWith("/advance")
        ? "advance"
        : input.method === "GET"
          ? "status"
          : "create";
      if (current === stage) {
        captured = input.signal;
        return new Promise<Response>((_resolve, reject) => {
          input.signal.addEventListener("abort", () => reject(input.signal.reason), { once: true });
          if (input.signal.aborted) reject(input.signal.reason);
        });
      }
      if (current === "advance")
        return Response.json(
          { error: "conflict" },
          { status: 409, headers: { "Retry-After": "1" } }
        );
      return Response.json(job);
    })
  );
  const owner = new RequestAdmission({ maxConcurrent: 1 });
  const client = new AntflyClient({ baseUrl, requestAdmission: owner });
  const abort = new AbortController();
  const pending = client
    .runResearchJob(request, { signal: abort.signal })
    .catch((error: unknown) => error);
  if (stage === "status") {
    await vi.waitFor(() => expect(sent).toHaveLength(2));
    await vi.advanceTimersByTimeAsync(1000);
  }
  await vi.waitFor(() => expect(captured).toBeDefined());
  expect(owner.stats.active).toBe(1);
  const reason = new Error("same operation canceled");
  abort.abort(reason);
  expect(await pending).toBe(reason);
  expect(captured?.aborted).toBe(true);
  expect(owner.stats).toEqual({ active: 0, queued: 0, closed: false });
  expect(sent.some((url) => url.endsWith("/cancel"))).toBe(false);
});
