import { afterEach, describe, expect, it, vi } from "vitest";
import { AdmissionPool, ClientBusyError } from "../src/admission.js";
import { AntflyClient } from "../src/client.js";
import { InferenceClient } from "../src/inference-client.js";

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

describe("client workload admission", () => {
  it("holds the slot until response EOF or cancellation and queues FIFO", async () => {
    const pool = new AdmissionPool({ maxInFlight: 1, maxQueued: 2, maxWaitMs: 1000 });
    const network = vi.fn<typeof fetch>().mockImplementation(async () => new Response("payload"));
    const fetch = pool.wrap(network);
    const first = await fetch("http://localhost/first");
    const second = fetch("http://localhost/second");
    const third = fetch("http://localhost/third");
    expect(pool.stats).toEqual({ active: 1, queued: 2 });
    expect(network).toHaveBeenCalledTimes(1);
    await expect(fetch("http://localhost/fourth")).rejects.toBeInstanceOf(ClientBusyError);
    await first.body!.cancel();
    const response = await second;
    expect(await response.text()).toBe("payload");
    await (await third).body!.cancel();
    expect(network.mock.calls.map(([url]) => url)).toEqual([
      "http://localhost/first",
      "http://localhost/second",
      "http://localhost/third",
    ]);
    expect(pool.stats).toEqual({ active: 0, queued: 0 });
  });

  it("retires queued cancellation without dispatch or disturbing the active response", async () => {
    const pool = new AdmissionPool({ maxInFlight: 1, maxQueued: 1, maxWaitMs: 1000 });
    const network = vi.fn<typeof fetch>().mockResolvedValue(new Response("active"));
    const fetch = pool.wrap(network);
    const active = await fetch("http://localhost/active");
    const controller = new AbortController();
    const queued = fetch("http://localhost/queued", { signal: controller.signal });
    controller.abort();
    await expect(queued).rejects.toMatchObject({ name: "AbortError" });
    expect(pool.stats).toEqual({ active: 1, queued: 0 });
    expect(network).toHaveBeenCalledTimes(1);
    await active.body!.cancel();
    expect(pool.stats.active).toBe(0);
  });

  it("expires waiting at the configured ceiling", async () => {
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "performance"] });
    const pool = new AdmissionPool({ maxInFlight: 1, maxQueued: 1, maxWaitMs: 10 });
    const network = vi.fn<typeof fetch>().mockResolvedValue(new Response("active"));
    const fetch = pool.wrap(network);
    const active = await fetch("http://localhost/active");
    const queued = fetch("http://localhost/queued");
    const rejected = expect(queued).rejects.toBeInstanceOf(ClientBusyError);
    await vi.advanceTimersByTimeAsync(10);
    await rejected;
    await active.body!.cancel();
    expect(network).toHaveBeenCalledTimes(1);
    expect(pool.stats).toEqual({ active: 0, queued: 0 });
  });

  it("joins response cancellation before reusing a slot", async () => {
    const pool = new AdmissionPool({ maxInFlight: 1, maxQueued: 1, maxWaitMs: 1000 });
    let finishCancel!: () => void;
    const network = vi.fn<typeof fetch>().mockResolvedValue(
      new Response(
        new ReadableStream({
          cancel: () =>
            new Promise<void>((resolve) => {
              finishCancel = resolve;
            }),
        })
      )
    );
    const controller = new AbortController();
    const response = await pool.wrap(network)("http://localhost/stream", {
      signal: controller.signal,
    });
    controller.abort();
    expect(pool.stats.active).toBe(1);
    finishCancel();
    await expect(response.text()).rejects.toMatchObject({ name: "AbortError" });
    expect(pool.stats.active).toBe(0);
  });

  it("does not retry a write whose network outcome is unknown", async () => {
    const pool = new AdmissionPool({ maxInFlight: 1, maxQueued: 0, maxWaitMs: 0 });
    const network = vi.fn<typeof fetch>().mockRejectedValue(new TypeError("connection lost"));
    await expect(
      pool.wrap(network)("http://localhost/batch", { method: "POST", body: "{}" })
    ).rejects.toThrow("connection lost");
    expect(network).toHaveBeenCalledTimes(1);
    expect(pool.stats).toEqual({ active: 0, queued: 0 });
  });

  it("shares one pool across generated database and inference calls", async () => {
    const pool = new AdmissionPool({ maxInFlight: 1, maxQueued: 1, maxWaitMs: 1000 });
    let finish!: (response: Response) => void;
    const network = vi
      .fn<typeof fetch>()
      .mockImplementationOnce(
        () =>
          new Promise<Response>((resolve) => {
            finish = resolve;
          })
      )
      .mockResolvedValueOnce(new Response('{"embeddings":[]}'));
    vi.stubGlobal("fetch", network);
    const db = new AntflyClient({ baseUrl: "http://localhost", admission: pool });
    const inference = new InferenceClient({ baseUrl: "http://localhost", admission: pool });
    const database = db.api.GET("/status");
    // openapi-fetch builds its Request before calling the transport.
    await vi.waitFor(() => expect(network).toHaveBeenCalledTimes(1));
    const embedding = inference.embed({ model: "test", input: "test" });
    await vi.waitFor(() => expect(pool.stats.queued).toBe(1));
    finish(new Response("{}"));
    await database;
    await embedding;
    expect(network).toHaveBeenCalledTimes(2);
    expect(pool.stats).toEqual({ active: 0, queued: 0 });
  });

  it("validates and snapshots configuration", () => {
    for (const maxInFlight of [0, -1, 1.5, Number.POSITIVE_INFINITY]) {
      expect(() => new AdmissionPool({ maxInFlight, maxQueued: 0, maxWaitMs: 0 })).toThrow(
        TypeError
      );
    }
    expect(() => new AdmissionPool({ maxInFlight: 1, maxQueued: 1, maxWaitMs: 0 })).toThrow(
      TypeError
    );
  });
});
