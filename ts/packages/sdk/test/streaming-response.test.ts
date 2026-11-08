import { afterEach, expect, it, vi } from "vitest";
import { InferenceClient } from "../src/inference-client.js";

afterEach(() => vi.unstubAllGlobals());
const request = { model: "m", messages: [] };
const client = () => new InferenceClient({ baseUrl: "http://inference.test" });

it("opens on SSE headers without consuming a token and owns cancellation immediately", async () => {
  const pull = vi.fn();
  const cancel = vi.fn();
  vi.stubGlobal(
    "fetch",
    async () =>
      new Response(new ReadableStream({ pull, cancel }, { highWaterMark: 0 }), {
        headers: { "content-type": "text/event-stream" },
      })
  );
  const stream = await client().openGenerateStream(request);
  expect(pull).not.toHaveBeenCalled();
  await stream.cancel();
  expect(cancel).toHaveBeenCalledOnce();
});

it.each([200, 503])("interrupts an uncooperative HTTP %s body on caller abort", async (status) => {
  const controller = new AbortController();
  const cancel = vi.fn(() => new Promise<void>(() => {}));
  const pull = vi.fn(() => new Promise<void>(() => {}));
  vi.stubGlobal(
    "fetch",
    async () =>
      new Response(new ReadableStream({ pull, cancel }, { highWaterMark: 0 }), {
        status,
        headers: { "content-type": "text/event-stream" },
      })
  );
  const opening = client().openGenerateStream(request, { signal: controller.signal });
  let reading: Promise<unknown>;
  if (status === 200) reading = (await opening).getReader().read();
  else reading = opening;
  const rejection = expect(reading).rejects.toThrow("caller stopped");
  await vi.waitFor(() => expect(pull).toHaveBeenCalledOnce());
  controller.abort(new Error("caller stopped"));
  await rejection;
  expect(cancel).toHaveBeenCalledOnce();
});

it("settles cancellation while awaiting headers and discards the eventual response", async () => {
  const controller = new AbortController();
  const cancel = vi.fn();
  let respond!: (response: Response) => void;
  vi.stubGlobal(
    "fetch",
    () =>
      new Promise<Response>((resolve) => {
        respond = resolve;
      })
  );
  const opening = client().openGenerateStream(request, { signal: controller.signal });
  controller.abort(new Error("stopped before headers"));
  await expect(opening).rejects.toThrow("stopped before headers");
  respond(new Response(new ReadableStream({ cancel }, { highWaterMark: 0 })));
  await vi.waitFor(() => expect(cancel).toHaveBeenCalledOnce());
});

it("discards responses when abort races the header continuation", async () => {
  for (let depth = 0; depth < 12; depth++) {
    const controller = new AbortController();
    const cancel = vi.fn();
    vi.stubGlobal("fetch", async () => {
      let later = () => controller.abort();
      for (let i = 0; i < depth; i++) {
        const previous = later;
        later = () => queueMicrotask(previous);
      }
      queueMicrotask(later);
      return new Response(new ReadableStream({ cancel }, { highWaterMark: 0 }), {
        headers: { "content-type": "text/event-stream" },
      });
    });
    let stream: ReadableStream | undefined;
    try {
      stream = await client().openGenerateStream(request, { signal: controller.signal });
    } catch {
      /* expected abort */
    }
    await new Promise((resolve) => setTimeout(resolve, 0));
    await stream?.cancel().catch(() => {});
    expect(cancel.mock.calls.length, `microtask depth ${depth}`).toBe(1);
  }
});
