import { describe, expect, it, vi } from "vitest";
import { AdmissionPool } from "../src/admission.js";
import { readRetryFetch } from "../src/read-retries.js";

const rejected = JSON.stringify({
  reason: "instance_busy",
  stage: "admission",
  execution_started: false,
});
const policy = { maxAttempts: 3, maxElapsedMs: 1_000, initialBackoffMs: 1, maxBackoffMs: 10 };
const response = (status: number, body: string, after?: string) =>
  new Response(body, {
    status,
    headers: { "Content-Length": String(body.length), ...(after ? { "Retry-After": after } : {}) },
  });
const url = "http://test/db/v1/tables/docs/query";

describe("query read retries", () => {
  it("includes the shortest body timeout in the original backoff budget", async () => {
    const base = vi.fn<typeof fetch>().mockImplementation(async (input) => {
      const lines = (await (input as Request).text()).trim().split("\n");
      expect(lines.map((line) => JSON.parse(line).timeout_ms)).toEqual([
        expect.any(Number),
        expect.any(Number),
      ]);
      expect(JSON.parse(lines[0]).timeout_ms).toBeLessThanOrEqual(80);
      expect(JSON.parse(lines[1]).timeout_ms).toBeLessThanOrEqual(80);
      return response(429, rejected);
    });
    const result = await readRetryFetch(base, {
      ...policy,
      initialBackoffMs: 100,
      maxBackoffMs: 100,
    })(url, {
      method: "POST",
      headers: { "Content-Type": "application/x-ndjson" },
      body: '{"timeout_ms":800}\n{"timeout_ms":80}\n',
    });
    expect(result.status).toBe(429);
    expect(base).toHaveBeenCalledTimes(1);
    await result.body?.cancel();
  });

  it("forwards only remaining query time while preserving unknown JSON fields", async () => {
    const submitted =
      '{ "large": 12345678901234567890123456789, "decimal": 1.0000000000000000001, "nested": {"timeout_ms": 900}, "timeout_ms": 200 }';
    const requests: string[] = [];
    const base = vi.fn<typeof fetch>().mockImplementation(async (input) => {
      expect((input as Request).headers.has("Content-Length")).toBe(false);
      requests.push(await (input as Request).text());
      return response(requests.length === 1 ? 429 : 200, requests.length === 1 ? rejected : "ok");
    });
    const result = await readRetryFetch(base, {
      ...policy,
      initialBackoffMs: 40,
      maxBackoffMs: 40,
    })(url, {
      method: "POST",
      headers: { "Content-Length": String(submitted.length) },
      body: submitted,
    });
    expect(result.status).toBe(200);
    expect(requests).toHaveLength(2);
    const first = JSON.parse(requests[0]).timeout_ms as number;
    const second = JSON.parse(requests[1]).timeout_ms as number;
    expect(first).toBeGreaterThan(0);
    expect(first).toBeLessThanOrEqual(200);
    expect(second).toBeGreaterThan(0);
    expect(second).toBeLessThan(first - 20);
    for (const request of requests)
      expect(request).toContain(
        '"large": 12345678901234567890123456789, "decimal": 1.0000000000000000001, "nested": {"timeout_ms": 900}'
      );
  });

  it("does not retry duplicate top-level timeout fields", async () => {
    const body = '{"timeout_ms":200,"timeout_ms":50}';
    const base = vi.fn<typeof fetch>().mockResolvedValue(response(429, rejected));
    const result = await readRetryFetch(base, policy)(url, { method: "POST", body });
    expect(result.status).toBe(429);
    expect(base).toHaveBeenCalledTimes(1);
    expect(await (base.mock.calls[0][0] as Request).text()).toBe(body);
  });
  it("preserves oversized or chunked error bodies without retrying", async () => {
    for (const length of [undefined, "5"]) {
      const body = `${rejected}${" ".repeat(16_384)}`;
      const base = vi
        .fn<typeof fetch>()
        .mockResolvedValue(
          new Response(body, { status: 429, headers: length ? { "Content-Length": length } : {} })
        );
      const result = await readRetryFetch(base, policy)(url, { method: "POST", body: "{}" });
      expect(await result.text()).toBe(body);
      expect(base).toHaveBeenCalledTimes(1);
    }
  });

  it("bypasses large input without changing its bytes", async () => {
    const body = " ".repeat((1 << 20) + 1);
    const base = vi.fn<typeof fetch>().mockImplementation(async (input) => {
      expect(await (input as Request).text()).toBe(body);
      return response(429, rejected);
    });
    const result = await readRetryFetch(base, policy)(url, { method: "POST", body });
    expect(await result.text()).toBe(rejected);
    expect(base).toHaveBeenCalledTimes(1);
  });
  it("replays input and reacquires admission, retaining the final body slot", async () => {
    const base = vi
      .fn<typeof fetch>()
      .mockImplementationOnce(async (input) => {
        expect(await (input as Request).text()).toBe("{}");
        return response(429, rejected);
      })
      .mockImplementationOnce(async (input) => {
        expect(await (input as Request).text()).toBe("{}");
        return response(200, "result");
      });
    const pool = new AdmissionPool({ maxInFlight: 1, maxQueued: 0, maxWaitMs: 0 });
    const fetch = readRetryFetch(pool.wrap(base), policy);
    const result = await fetch(new Request(url, { method: "POST", body: "{}" }));
    expect(base).toHaveBeenCalledTimes(2);
    expect(pool.stats.active).toBe(1);
    expect(await result.text()).toBe("result");
    expect(pool.stats.active).toBe(0);
  });

  it.each([
    ["http://test/db/v1/tables/docs/batch", 429, rejected, undefined],
    [url, 429, '{"reason":"instance_busy"}', undefined],
    [url, 429, rejected.replace("false", "true"), undefined],
    [url, 503, rejected, undefined],
    [url, 429, rejected, "1"],
    [
      url,
      429,
      '{"reason":"instance_busy","stage":"admission","execution_started":true,"execution_started":false}',
      undefined,
    ],
    [
      url,
      429,
      '{"reason":"instance_busy","stage":"worker","stage":"admission","execution_started":false}',
      undefined,
    ],
    [
      url,
      429,
      '{"reason":"instance_busy","stage":"admission","execution_started":true,"execution_\\u0073tarted":false}',
      undefined,
    ],
    [url, 429, '{"reason":"instance_busy","stage":"admission","protocol_version":2}', undefined],
    [
      url,
      429,
      '{"reason":"instance_busy","stage":"admission","execution_started":false,"protocol_version":1,"protocol_version":2}',
      undefined,
    ],
    [url, 429, '{"reason":"instance_busy","stage":"worker","execution_started":false}', undefined],
    [
      url,
      429,
      '{"Reason":"instance_busy","Stage":"admission","Execution_Started":false}',
      undefined,
    ],
  ])("does not retry writes or ambiguous/long-delay errors (%s %s)", async (path, status, body, after) => {
    const base = vi.fn<typeof fetch>().mockResolvedValue(response(status, body, after));
    const result = await readRetryFetch(base, policy)(path, { method: "POST", body: "{}" });
    expect(await result.text()).toBe(body);
    expect(base).toHaveBeenCalledTimes(1);
  });

  it("does not retry malformed UTF-8 in otherwise explicit evidence", async () => {
    const body = new Uint8Array([
      ...new TextEncoder().encode(
        '{"reason":"instance_busy","stage":"admission","execution_started":false,"trace":"'
      ),
      0xff,
      ...new TextEncoder().encode('"}'),
    ]);
    const base = vi.fn<typeof fetch>().mockResolvedValue(
      new Response(body, {
        status: 429,
        headers: { "Content-Length": String(body.length) },
      })
    );
    const result = await readRetryFetch(base, policy)(url, { method: "POST", body: "{}" });
    expect(new Uint8Array(await result.arrayBuffer())).toEqual(body);
    expect(base).toHaveBeenCalledTimes(1);
  });

  it("retries explicit pre-execution evidence with future extension fields", async () => {
    const extended = JSON.stringify({ ...JSON.parse(rejected), protocol_version: 2 });
    const base = vi
      .fn<typeof fetch>()
      .mockResolvedValueOnce(response(429, extended))
      .mockResolvedValueOnce(response(200, "ok"));
    const result = await readRetryFetch(base, policy)(url, { method: "POST", body: "{}" });
    expect(result.status).toBe(200);
    expect(base).toHaveBeenCalledTimes(2);
  });

  it("cancels backoff without another dispatch", async () => {
    const controller = new AbortController();
    const base = vi.fn<typeof fetch>().mockImplementation(async () => {
      setTimeout(() => controller.abort(), 2);
      return response(429, rejected);
    });
    const fetch = readRetryFetch(base, { ...policy, initialBackoffMs: 50, maxBackoffMs: 50 });
    await expect(
      fetch(url, { method: "POST", body: "{}", signal: controller.signal })
    ).rejects.toThrow();
    expect(base).toHaveBeenCalledTimes(1);
  });

  it("caps attempts and never retries transport failures", async () => {
    const base = vi.fn<typeof fetch>().mockImplementation(async () => response(429, rejected));
    const result = await readRetryFetch(base, policy)(url, { method: "POST", body: "{}" });
    expect(await result.text()).toBe(rejected);
    expect(base).toHaveBeenCalledTimes(3);
    base.mockClear().mockRejectedValue(new Error("unknown outcome"));
    await expect(readRetryFetch(base, policy)(url, { method: "POST", body: "{}" })).rejects.toThrow(
      "unknown outcome"
    );
    expect(base).toHaveBeenCalledTimes(1);
  });
});

describe("query retry transport cancellation", () => {
  it("keeps the original deadline while a successful response is streaming", async () => {
    let closed = 0;
    const base = vi.fn<typeof fetch>().mockImplementation(
      async () =>
        new Response(
          new ReadableStream<Uint8Array>({
            start(controller) {
              controller.enqueue(new TextEncoder().encode("first"));
            },
            cancel() {
              closed++;
            },
          }),
          { status: 200 }
        )
    );
    const result = await readRetryFetch(base, { ...policy, maxElapsedMs: 30 })(url, {
      method: "POST",
      body: "{}",
    });
    if (!result.body) throw new Error("missing response body");
    const reader = result.body.getReader();
    expect(new TextDecoder().decode((await reader.read()).value)).toBe("first");
    await expect(reader.read()).rejects.toMatchObject({ name: "TimeoutError" });
    expect(closed).toBe(1);
    expect(base).toHaveBeenCalledTimes(1);
  });

  it("cancels a returned response when its caller aborts", async () => {
    let closed = 0;
    const controller = new AbortController();
    const base = vi.fn<typeof fetch>().mockImplementation(
      async () =>
        new Response(
          new ReadableStream<Uint8Array>({
            cancel() {
              closed++;
            },
          }),
          { status: 200 }
        )
    );
    const result = await readRetryFetch(base, policy)(url, {
      method: "POST",
      body: "{}",
      signal: controller.signal,
    });
    if (!result.body) throw new Error("missing response body");
    const pending = result.body.getReader().read();
    controller.abort(new DOMException("cancelled", "AbortError"));
    await expect(pending).rejects.toMatchObject({ name: "AbortError" });
    expect(closed).toBe(1);
    expect(base).toHaveBeenCalledTimes(1);
  });

  it("rejects late successful headers and closes the underlying response", async () => {
    let closed = 0;
    const base = vi.fn<typeof fetch>().mockImplementation(async () => {
      // A custom transport may finish even after its input signal was aborted.
      await new Promise((resolve) => setTimeout(resolve, 40));
      return new Response(
        new ReadableStream({
          cancel: () => {
            closed++;
          },
        }),
        { status: 200 }
      );
    });
    await expect(
      readRetryFetch(base, { ...policy, maxElapsedMs: 20 })(url, { method: "POST", body: "{}" })
    ).rejects.toMatchObject({ name: "TimeoutError" });
    expect(base).toHaveBeenCalledTimes(1);
    expect(closed).toBe(1);
  });

  it("cancels both bounded-error tee branches when a reply stalls", async () => {
    let closed = 0;
    const base = vi.fn<typeof fetch>().mockImplementation(async () => {
      const body = new ReadableStream<Uint8Array>({
        start(controller) {
          controller.enqueue(new TextEncoder().encode("{"));
        },
        cancel() {
          closed++;
        },
      });
      return new Response(body, { status: 429, headers: { "Content-Length": "100" } });
    });
    await expect(
      readRetryFetch(base, { ...policy, maxElapsedMs: 20 })(url, { method: "POST", body: "{}" })
    ).rejects.toMatchObject({ name: "TimeoutError" });
    expect(base).toHaveBeenCalledTimes(1);
    expect(closed).toBe(1);
  }, 500);
});

it("retains error-stream admission through asynchronous deadline cleanup", async () => {
  let closed = 0;
  let finishClose!: () => void;
  const closing = new Promise<void>((resolve) => {
    finishClose = resolve;
  });
  const base = vi.fn<typeof fetch>().mockImplementation(
    async () =>
      new Response(
        new ReadableStream<Uint8Array>({
          start(controller) {
            controller.enqueue(new TextEncoder().encode("{"));
          },
          cancel() {
            closed++;
            return closing;
          },
        }),
        { status: 429, headers: { "Content-Length": "100" } }
      )
  );
  const pool = new AdmissionPool({ maxInFlight: 1, maxQueued: 0, maxWaitMs: 0 });
  await expect(
    readRetryFetch(pool.wrap(base), { ...policy, maxElapsedMs: 20 })(url, {
      method: "POST",
      body: "{}",
    })
  ).rejects.toMatchObject({ name: "TimeoutError" });
  expect(closed).toBe(1);
  expect(pool.stats.active).toBe(1);
  finishClose();
  await new Promise((resolve) => setTimeout(resolve, 0));
  expect(pool.stats.active).toBe(0);
  expect(base).toHaveBeenCalledTimes(1);
  expect(closed).toBe(1);
});
