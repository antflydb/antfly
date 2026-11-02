import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { afterEach, describe, expect, it, vi } from "vitest";
import { AntflyClient } from "../src/client.js";
import { InferenceClient } from "../src/inference-client.js";
import { ClientCapacityError, RequestAdmission } from "../src/request-admission.js";

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((ok) => {
    resolve = ok;
  });
  return { promise, resolve };
}
function heldResponse(cancel?: () => Promise<void> | void): Response {
  return new Response(new ReadableStream({ cancel }, { highWaterMark: 0 }));
}
function requiredBody(response: Response): ReadableStream<Uint8Array> {
  if (!response.body) throw new Error("fixture requires a response body");
  return response.body;
}
function stubHeld() {
  const sent: string[] = [];
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL) => {
      sent.push(input instanceof Request ? input.url : String(input));
      return heldResponse();
    })
  );
  return sent;
}
const base = "http://localhost:8080";
afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
  vi.useRealTimers();
});

describe("bounded request ownership", () => {
  it.each([
    { maxConcurrent: 0 },
    { maxConcurrent: -1 },
    { maxQueued: -1 },
    { maxWaitMs: -1 },
    { maxWaitMs: 0x80000000 },
    { maxQueued: Number.NaN },
    { maxConcurrent: 1.5 },
  ])("rejects invalid limits %j", (options) => {
    expect(() => new RequestAdmission(options)).toThrow(RangeError);
  });
  it("bounds default active and queued work and refuses before sending", async () => {
    const sent = stubHeld();
    const owner = new RequestAdmission();
    const active = await Promise.all(
      Array.from({ length: 100 }, (_, i) => owner.fetch(`${base}/${i}`))
    );
    const queued = Array.from({ length: 100 }, (_, i) =>
      owner.fetch(`${base}/queued-${i}`).catch((error) => error)
    );
    expect(owner.stats).toEqual({ active: 100, queued: 100, closed: false });
    await expect(owner.fetch(`${base}/overflow`)).rejects.toMatchObject({
      reason: "queue_full",
      requestSent: false,
    });
    expect(sent).toHaveLength(100);
    owner.close();
    expect(
      (await Promise.all(queued)).every(
        (error) => error instanceof ClientCapacityError && error.reason === "closed"
      )
    ).toBe(true);
    await Promise.all(active.map((response) => requiredBody(response).cancel()));
    expect(owner.stats).toEqual({ active: 0, queued: 0, closed: true });
    expect(sent).toHaveLength(100);
  });
  it.each([0, 1, 2])("unlinks canceled waiter %i and preserves survivor FIFO", async (canceled) => {
    const sent = stubHeld();
    const owner = new RequestAdmission({ maxConcurrent: 1, maxQueued: 3 });
    const first = await owner.fetch(`${base}/first`);
    const controllers = Array.from({ length: 3 }, () => new AbortController());
    const queued = controllers.map((controller, i) =>
      owner.fetch(`${base}/${i}`, { signal: controller.signal }).catch((error) => error)
    );
    const reason = new Error("caller canceled");
    controllers.at(canceled)?.abort(reason);
    await expect(queued[canceled]).resolves.toBe(reason);
    expect(owner.stats.queued).toBe(2);
    await requiredBody(first).cancel();
    for (let i = 0; i < 3; i++) {
      if (i === canceled) continue;
      const response = (await queued[i]) as Response;
      expect(sent.at(-1)).toBe(`${base}/${i}`);
      await requiredBody(response).cancel();
    }
    expect(owner.stats.active).toBe(0);
    expect(sent).toHaveLength(3);
  });
  it("expires the original queue wait and restores queue capacity", async () => {
    const sent = stubHeld();
    const owner = new RequestAdmission({ maxConcurrent: 1, maxQueued: 1, maxWaitMs: 20 });
    const first = await owner.fetch(`${base}/first`);
    await expect(owner.fetch(`${base}/expired`)).rejects.toMatchObject({
      reason: "wait_timeout",
      requestSent: false,
    });
    expect(owner.stats.queued).toBe(0);
    expect(sent).toEqual([`${base}/first`]);
    const fresh = owner.fetch(`${base}/fresh`);
    await requiredBody(first).cancel();
    await requiredBody(await fresh).cancel();
    expect(sent).toEqual([`${base}/first`, `${base}/fresh`]);
  });
  it("checks elapsed queue time before delayed timer callbacks", async () => {
    vi.useFakeTimers({ toFake: ["performance", "setTimeout", "clearTimeout"] });
    stubHeld();
    const owner = new RequestAdmission({ maxConcurrent: 1, maxWaitMs: 10 });
    const first = await owner.fetch(`${base}/first`);
    const expired = owner.fetch(`${base}/expired`).catch((error) => error);
    vi.spyOn(performance, "now").mockReturnValue(20);
    await requiredBody(first).cancel();
    expect(await expired).toMatchObject({ reason: "wait_timeout" });
    expect(owner.stats.active).toBe(0);
  });
  it.each([{ maxQueued: 0 }, { maxWaitMs: 0 }])("supports fail-fast %j", async (options) => {
    stubHeld();
    const owner = new RequestAdmission({ maxConcurrent: 1, ...options });
    const first = await owner.fetch(base);
    await expect(owner.fetch(base)).rejects.toMatchObject({ reason: "queue_full" });
    expect(owner.stats.queued).toBe(0);
    await requiredBody(first).cancel();
  });
  it("does not send after cancellation between grant and dispatch", async () => {
    const sent = stubHeld();
    const owner = new RequestAdmission({ maxConcurrent: 1 });
    const abort = new AbortController();
    const request = owner.fetch(base, { signal: abort.signal });
    abort.abort();
    await expect(request).rejects.toMatchObject({ name: "AbortError" });
    expect(sent).toEqual([]);
    expect(owner.stats.active).toBe(0);
  });
  it("keeps unread bodies active until physical cancellation completes", async () => {
    const canceled = deferred();
    const started = deferred();
    const fetchMock = vi.fn(async () =>
      heldResponse(() => {
        started.resolve();
        return canceled.promise;
      })
    );
    vi.stubGlobal("fetch", fetchMock);
    const owner = new RequestAdmission({ maxConcurrent: 1 });
    const first = await owner.fetch(base);
    const queued = owner.fetch(base);
    expect(owner.stats).toMatchObject({ active: 1, queued: 1 });
    const canceling = requiredBody(first).cancel();
    await started.promise;
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(owner.stats.active).toBe(1);
    canceled.resolve();
    await canceling;
    await requiredBody(await queued).cancel();
    expect(owner.stats.active).toBe(0);
  });
  it("retires an unread response when the transport fails before another read", async () => {
    const failure = new Error("transport aborted after headers");
    let source!: ReadableStreamDefaultController<Uint8Array>;
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(
        new Response(
          new ReadableStream<Uint8Array>(
            {
              start(controller) {
                source = controller;
              },
            },
            { highWaterMark: 0 }
          )
        )
      )
      .mockImplementation(async () => heldResponse());
    vi.stubGlobal("fetch", fetchMock);
    const owner = new RequestAdmission({ maxConcurrent: 1 });
    const first = await owner.fetch(base);
    const queued = owner.fetch(base);
    expect(owner.stats).toMatchObject({ active: 1, queued: 1 });
    source.error(failure);
    await new Promise<void>((resolve) => setImmediate(resolve));
    const callsAfterFailure = fetchMock.mock.calls.length;
    const queuedAfterFailure = owner.stats.queued;
    // Cleanup both outcomes before assertions, including the leaking version.
    await expect(first.text()).rejects.toBe(failure);
    await requiredBody(await queued).cancel();
    expect(callsAfterFailure).toBe(2);
    expect(queuedAfterFailure).toBe(0);
    expect(owner.stats.active).toBe(0);
  });
  it("retires read failures and rejected cancellation exactly once", async () => {
    const failure = new Error("read failed");
    vi.stubGlobal(
      "fetch",
      vi.fn(
        async () =>
          new Response(
            new ReadableStream(
              {
                pull() {
                  throw failure;
                },
              },
              { highWaterMark: 0 }
            )
          )
      )
    );
    const owner = new RequestAdmission({ maxConcurrent: 1 });
    const first = await owner.fetch(base);
    await expect(first.text()).rejects.toBe(failure);
    expect(owner.stats.active).toBe(0);
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => heldResponse(() => Promise.reject(failure)))
    );
    const second = await owner.fetch(base);
    await expect(requiredBody(second).cancel()).rejects.toBe(failure);
    expect(owner.stats.active).toBe(0);
  });
  it("releases transport failures and empty bodies, and never retries 503", async () => {
    const failure = new Error("transport failed");
    const fetchMock = vi
      .fn()
      .mockRejectedValueOnce(failure)
      .mockResolvedValueOnce(new Response(null, { status: 204 }))
      .mockResolvedValueOnce(new Response("busy", { status: 503 }));
    vi.stubGlobal("fetch", fetchMock);
    const owner = new RequestAdmission({ maxConcurrent: 1 });
    await expect(owner.fetch(base)).rejects.toBe(failure);
    expect(owner.stats.active).toBe(0);
    await owner.fetch(base);
    expect(owner.stats.active).toBe(0);
    const busy = await owner.fetch(base);
    expect(owner.stats.active).toBe(1);
    expect(await busy.text()).toBe("busy");
    expect(owner.stats.active).toBe(0);
    expect(fetchMock).toHaveBeenCalledTimes(3);
  });
  it("close wakes queued callers without retiring active bodies", async () => {
    const sent = stubHeld();
    const owner = new RequestAdmission({ maxConcurrent: 1 });
    const first = await owner.fetch(base);
    const queued = owner.fetch(base);
    owner.close();
    owner.close();
    await expect(queued).rejects.toMatchObject({ reason: "closed", requestSent: false });
    await expect(owner.fetch(base)).rejects.toMatchObject({ reason: "closed" });
    expect(owner.stats.active).toBe(1);
    expect(sent).toHaveLength(1);
    await requiredBody(first).cancel();
    expect(owner.stats.active).toBe(0);
  });
});

it("shares generated database routes, raw writes and inference over real TCP", async () => {
  const received: string[] = [];
  const headers = deferred();
  const release = deferred();
  const server = createServer(async (request, response) => {
    received.push(request.url ?? "");
    response.setHeader("Content-Type", "application/json");
    if (request.url === "/db/v1/status") {
      response.write('{"status":');
      headers.resolve();
      await release.promise;
      response.end('"ok"}');
    } else if (request.url === "/db/v1/tables/items/batch") {
      response.end('{"inserted":1,"deleted":0,"transformed":0}');
    } else {
      response.end('{"choices":[]}');
    }
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const owner = new RequestAdmission({ maxConcurrent: 1, maxQueued: 2 });
  const url = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
  const db = new AntflyClient({ baseUrl: url, requestAdmission: owner });
  const ai = new InferenceClient({ baseUrl: url, requestAdmission: owner });
  try {
    const first = db.api.GET("/db/v1/status");
    await headers.promise;
    const write = db.tables.batch("items", { inserts: { one: { id: 1 } } });
    const generation = ai.generate({ model: "model", messages: [] });
    await vi.waitFor(() => expect(owner.stats.queued).toBe(2));
    await expect(db.api.GET("/db/v1/status")).rejects.toMatchObject({ reason: "queue_full" });
    expect(received).toEqual(["/db/v1/status"]);
    release.resolve();
    await first;
    await write;
    await generation;
    expect(received).toEqual(["/db/v1/status", "/db/v1/tables/items/batch", "/ai/v1/generate"]);
    expect(owner.stats.active).toBe(0);
    ai.close();
    await expect(db.api.GET("/db/v1/status")).rejects.toMatchObject({ reason: "closed" });
  } finally {
    release.resolve();
    owner.close();
    server.closeAllConnections();
    await new Promise<void>((resolve, reject) =>
      server.close((error) => (error ? reject(error) : resolve()))
    );
  }
});
it("reuses capacity after a real fetch aborts an unread response", async () => {
  const server = createServer((_request, response) => {
    response.setHeader("Content-Type", "text/plain");
    response.write("retained response");
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const owner = new RequestAdmission({ maxConcurrent: 1 });
  const abort = new AbortController();
  const url = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
  let first: Response | undefined;
  try {
    first = await owner.fetch(url, { signal: abort.signal });
    const queued = owner.fetch(url);
    expect(owner.stats).toMatchObject({ active: 1, queued: 1 });
    abort.abort();
    // The queued request must start without consuming/canceling the first body.
    const next = await queued;
    expect(owner.stats).toMatchObject({ active: 1, queued: 0 });
    await expect(first.text()).rejects.toMatchObject({ name: "AbortError" });
    await requiredBody(next).cancel();
    expect(owner.stats.active).toBe(0);
  } finally {
    abort.abort();
    await first?.body?.cancel().catch(() => {});
    owner.close();
    server.closeAllConnections();
    await new Promise<void>((resolve, reject) =>
      server.close((error) => (error ? reject(error) : resolve()))
    );
  }
});

it("returns capacity when a caller stops an NDJSON scan early", async () => {
  const canceled = vi.fn();
  vi.stubGlobal(
    "fetch",
    vi.fn(
      async () =>
        new Response(
          new ReadableStream({
            start(controller) {
              controller.enqueue(new TextEncoder().encode('{"_id":"one"}\n'));
            },
            cancel: canceled,
          })
        )
    )
  );
  const owner = new RequestAdmission({ maxConcurrent: 1 });
  const db = new AntflyClient({ baseUrl: base, requestAdmission: owner });
  for await (const row of db.tables.scan("items")) {
    expect(row._id).toBe("one");
    break;
  }
  expect(canceled).toHaveBeenCalledOnce();
  expect(owner.stats.active).toBe(0);
});

it("keeps capacity through cancellation even when a pending read reports EOF early", async () => {
  const pulled = deferred();
  const cancelStarted = deferred();
  const canceled = deferred();
  const fetchMock = vi.fn(
    async () =>
      new Response(
        new ReadableStream(
          {
            pull() {
              pulled.resolve();
            },
            cancel() {
              cancelStarted.resolve();
              return canceled.promise;
            },
          },
          { highWaterMark: 0 }
        )
      )
  );
  vi.stubGlobal("fetch", fetchMock);
  const owner = new RequestAdmission({ maxConcurrent: 1 });
  const first = await owner.fetch(base);
  const reader = requiredBody(first).getReader();
  const read = reader.read();
  await pulled.promise;
  const queued = owner.fetch(base);
  const canceling = reader.cancel();
  await cancelStarted.promise;
  await read;
  await new Promise<void>((resolve) => setImmediate(resolve));
  const activeDuringCancel = owner.stats.active;
  const callsDuringCancel = fetchMock.mock.calls.length;
  canceled.resolve();
  await canceling;
  await requiredBody(await queued).cancel();
  reader.releaseLock();
  expect(activeDuringCancel).toBe(1);
  expect(callsDuringCancel).toBe(1);
  expect(owner.stats.active).toBe(0);
});

it("retires a streaming inference response when its generator is closed early", async () => {
  const canceled = vi.fn();
  vi.stubGlobal(
    "fetch",
    vi.fn(
      async () =>
        new Response(
          new ReadableStream({
            start(controller) {
              controller.enqueue(new TextEncoder().encode('data: {"choices":[]}\n\n'));
            },
            cancel: canceled,
          }),
          { headers: { "Content-Type": "text/event-stream" } }
        )
    )
  );
  const owner = new RequestAdmission({ maxConcurrent: 1 });
  const ai = new InferenceClient({ baseUrl: base, requestAdmission: owner });
  for await (const chunk of ai.generateStream({ model: "model", messages: [] })) {
    expect(chunk).toEqual({ choices: [] });
    expect(owner.stats.active).toBe(1);
    break;
  }
  expect(canceled).toHaveBeenCalledOnce();
  expect(owner.stats.active).toBe(0);
});

it("preserves the caller signal and response metadata without resetting its deadline", async () => {
  const abort = new AbortController();
  const response = new Response("ok");
  Object.defineProperties(response, {
    url: { value: `${base}/redirected` },
    redirected: { value: true },
  });
  const fetchMock = vi.fn(async (_input, init) => {
    expect(init.signal).toBe(abort.signal);
    return response;
  });
  vi.stubGlobal("fetch", fetchMock);
  const owner = new RequestAdmission();
  const result = await owner.fetch(base, { signal: abort.signal });
  expect(result.url).toBe(`${base}/redirected`);
  expect(result.redirected).toBe(true);
  expect(await result.text()).toBe("ok");
  expect(owner.stats.active).toBe(0);
});

it("honors an explicit null signal override on a Request", async () => {
  const sent = stubHeld();
  const abort = new AbortController();
  abort.abort();
  const request = new Request(base, { signal: abort.signal });
  const owner = new RequestAdmission();
  const response = await owner.fetch(request, { signal: null });
  expect(sent).toHaveLength(1);
  await requiredBody(response).cancel();
  expect(owner.stats.active).toBe(0);
});
