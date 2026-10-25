import { afterEach, describe, expect, it, vi } from "vitest";
import {
  beginAbortableRequest,
  fetchInferenceJson,
  fetchWithRetry,
  finishAbortableRequest,
  InferenceHttpError,
} from "./utils";

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

describe("fetchWithRetry", () => {
  it("retries only a server-declared transient capacity response", async () => {
    vi.useFakeTimers();
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            error: "MODEL_RESOURCE_BUSY",
            message: "busy",
            retryable: true,
            retry_after_ms: 100,
          }),
          { status: 503, headers: { "content-type": "application/json" } }
        )
      )
      .mockResolvedValueOnce(new Response(JSON.stringify({ ok: true }), { status: 200 }));
    vi.stubGlobal("fetch", fetchMock);

    const pending = fetchWithRetry("/ai/v1/embed", undefined, { maxRetries: 1 });
    await vi.advanceTimersByTimeAsync(100);

    expect((await pending).status).toBe(200);
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  it.each([
    [502, { error: "BAD_GATEWAY", retryable: true }],
    [503, { error: "MODEL_RESOURCE_BUSY", retryable: false }],
    [503, { error: "OTHER", retryable: true }],
  ])("does not retry status %s without the exact capacity contract", async (status, body) => {
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(JSON.stringify(body), {
        status,
        headers: { "content-type": "application/json" },
      })
    );
    vi.stubGlobal("fetch", fetchMock);

    expect((await fetchWithRetry("/ai/v1/embed")).status).toBe(status);
    expect(fetchMock).toHaveBeenCalledOnce();
  });

  it("cancels a pending retry immediately", async () => {
    vi.useFakeTimers();
    const controller = new AbortController();
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(
        new Response(
          JSON.stringify({
            error: "MODEL_RESOURCE_BUSY",
            retryable: true,
            retry_after_ms: 5_000,
          }),
          { status: 503, headers: { "content-type": "application/json" } }
        )
      )
    );

    const pending = fetchWithRetry("/ai/v1/embed", { signal: controller.signal });
    controller.abort();
    await expect(pending).rejects.toMatchObject({ name: "AbortError" });
  });
});

describe("fetchInferenceJson", () => {
  it("throws a typed error without consuming an unstructured response", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(
        new Response(JSON.stringify({ error: "INVALID_MODEL", message: "Choose another model" }), {
          status: 400,
          headers: { "content-type": "application/json" },
        })
      )
    );

    const error = await fetchInferenceJson("/ai/v1/embed").catch((reason) => reason);
    expect(error).toBeInstanceOf(InferenceHttpError);
    expect(error).toMatchObject({
      status: 400,
      code: "INVALID_MODEL",
      retryable: false,
      message: "Choose another model",
    });
  });
});

describe("abortable request ownership", () => {
  it("prevents an older request from clearing a newer request", () => {
    const slot = { current: null as AbortController | null };
    const first = beginAbortableRequest(slot);
    const second = beginAbortableRequest(slot);

    expect(first.signal.aborted).toBe(true);
    expect(finishAbortableRequest(slot, first)).toBe(false);
    expect(slot.current).toBe(second);
    expect(finishAbortableRequest(slot, second)).toBe(true);
    expect(slot.current).toBeNull();
  });
});
