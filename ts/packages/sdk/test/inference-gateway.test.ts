import { afterEach, describe, expect, it, vi } from "vitest";
import { AntflyClient } from "../src/client.js";

afterEach(() => vi.unstubAllGlobals());

describe("named inference gateway", () => {
  it("shares database authentication and normalizes the public gateway origin", async () => {
    const fetcher = vi.fn().mockResolvedValue(
      new Response(
        JSON.stringify({
          object: "list",
          data: [{ id: "chains/support", task: "generate", availability: "unknown" }],
        }),
        { headers: { "Content-Type": "application/json" } }
      )
    );
    vi.stubGlobal("fetch", fetcher);
    const client = new AntflyClient({
      baseUrl: "https://antfly.test/db/v1",
      auth: { type: "token", token: "tenant-key" },
    });
    const catalog = await client.inference.models();
    expect(catalog.data[0]?.id).toBe("chains/support");
    expect(fetcher).toHaveBeenCalledWith(
      "https://antfly.test/ai/v1/models",
      expect.objectContaining({
        headers: expect.objectContaining({ Authorization: "Bearer tenant-key" }),
      })
    );
  });
  it("returns the live stream with its cancellation signal and never retries a mutation", async () => {
    const signal = new AbortController().signal;
    const response = new Response(
      new ReadableStream({
        start(controller) {
          controller.enqueue(new TextEncoder().encode('data: {"delta":{"tool_calls":[]}}\n\n'));
        },
      }),
      { headers: { "Content-Type": "text/event-stream" } }
    );
    const fetcher = vi
      .fn()
      .mockResolvedValueOnce(response)
      .mockResolvedValueOnce(new Response("busy", { status: 503 }));
    vi.stubGlobal("fetch", fetcher);
    const client = new AntflyClient({ baseUrl: "https://antfly.test" });
    const stream = await client.inference.invoke(
      "chat/completions",
      { model: "chains/support", stream: true },
      signal
    );
    const reader = stream.body?.getReader();
    expect(new TextDecoder().decode((await reader?.read())?.value)).toContain("tool_calls");
    await reader?.cancel();
    expect(fetcher.mock.calls[0]?.[1]?.signal).toBe(signal);
    expect(
      (await client.inference.invoke("predict", { model: "predictors/risk", input: [[1, 2]] }))
        .status
    ).toBe(503);
    expect(fetcher).toHaveBeenCalledTimes(2);
    expect(fetcher.mock.calls[1]?.[0]).toBe("https://antfly.test/ml/v1/predict");
  });
});
