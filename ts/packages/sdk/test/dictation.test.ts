import { afterEach, describe, expect, it, vi } from "vitest";
import { InferenceClient } from "../src/inference-client.js";

const request = {
  model: "transcribers/apple-speech",
  audio: "YXVkaW8=",
  dictionary: ["Antfly"],
  cleanup_model: "cleanup",
};
afterEach(() => vi.unstubAllGlobals());

describe("dictation", () => {
  it("forwards provider options and cancellation for JSON requests", async () => {
    const abort = new AbortController();
    const fetch = vi.fn(async (input: Request) => {
      expect(input.url).toBe("http://localhost:8080/ai/v1/dictate");
      expect(await input.json()).toEqual({ ...request, stream: false });
      expect(input.signal.aborted).toBe(false);
      return Response.json({
        object: "dictation",
        text: "Antfly",
        transcript: { text: "ant fly" },
      });
    });
    vi.stubGlobal("fetch", fetch);
    const client = new InferenceClient({ baseUrl: "http://localhost:8080" });
    expect((await client.dictate(request, { signal: abort.signal })).text).toBe("Antfly");
    expect(fetch).toHaveBeenCalledOnce();
  });

  it("streams transcript, deltas and completion in order", async () => {
    const events = [
      { type: "dictation.transcript", id: "d", transcript: { text: "ant fly" } },
      { type: "dictation.delta", id: "d", delta: "Antfly" },
      { type: "dictation.completed", id: "d", text: "Antfly" },
    ];
    vi.stubGlobal(
      "fetch",
      vi.fn(
        async () =>
          new Response(
            `${events.map((event) => `data: ${JSON.stringify(event)}\n\n`).join("")}data: [DONE]\n\n`,
            { headers: { "Content-Type": "text/event-stream" } }
          )
      )
    );
    const client = new InferenceClient({ baseUrl: "http://localhost:8080" });
    const received = [];
    for await (const event of client.dictateStream(request)) received.push(event);
    expect(received).toEqual(events);
  });

  it.each([
    ["data: [DONE]\n\n", "before completion"],
    ['data: {"type":"dictation.delta","id":"d","delta":"partial"}\n\n', "without [DONE]"],
    ['data: {"type":"error","id":"d","message":"Apple busy"}\n\n', "Apple busy"],
  ])("rejects incomplete or failed streams", async (body, message) => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => new Response(body, { headers: { "Content-Type": "text/event-stream" } }))
    );
    const client = new InferenceClient({ baseUrl: "http://localhost:8080" });
    const consume = async () => {
      for await (const _event of client.dictateStream(request)) {
        /* consume */
      }
    };
    await expect(consume()).rejects.toThrow(message);
  });

  it("rejects JSON disguised as a successful stream", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => Response.json({ error: "unavailable" }))
    );
    const client = new InferenceClient({ baseUrl: "http://localhost:8080" });
    await expect(client.dictateStream(request).next()).rejects.toThrow("event stream");
  });
});
