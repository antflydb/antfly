import { afterEach, expect, it, vi } from "vitest";
import { InferenceClient } from "../src/inference-client.js";

const plan = "1".repeat(64);
const item = {
  descriptor: {
    id: `${plan}:0`,
    ordinal: 0,
    coordinates: { kind: "text", start_byte: 0, end_byte: 7 },
    input_tokens: 4,
  },
  mime_type: "text/plain",
  text: "hé🐜",
  embedding: [0.6, 0.8],
};
const terminal = {
  object: "chunk.embedding.list",
  data: [],
  status: "partial",
  source_fingerprint: "2".repeat(64),
  source_version: "v1",
  plan_id: plan,
  model: "model",
  model_identity: "3".repeat(64),
  model_revision: "4".repeat(64),
  task_type: "RETRIEVAL_DOCUMENT",
  dimensions: 2,
  policy: {},
  start_ordinal: 0,
  covered_through_ordinal: 1,
  next_continuation: `${plan}:1`,
  gaps: [],
};
const context = { ...terminal };
const start = { object: "chunk.embedding.start", context };
afterEach(() => vi.unstubAllGlobals());

it.each([
  true,
  false,
])("requires terminal coverage with split UTF8 records: %s", async (includeTerminal) => {
  const events = [
    start,
    { object: "chunk.embedding", data: item },
    ...(includeTerminal ? [{ object: "chunk.embedding.complete", response: terminal }] : []),
  ];
  const bytes = new TextEncoder().encode(
    `${events.map((event) => JSON.stringify(event)).join("\n")}\n`
  );
  vi.stubGlobal(
    "fetch",
    vi.fn(
      async () =>
        new Response(
          new ReadableStream({
            start(controller) {
              for (const byte of bytes) controller.enqueue(Uint8Array.of(byte));
              controller.close();
            },
          }),
          { headers: { "Content-Type": "application/x-ndjson" } }
        )
    )
  );
  const client = new InferenceClient({ baseUrl: "http://test" });
  const received = [];
  const consume = async () => {
    for await (const event of client.chunkAndEmbedStream({
      input: "hé🐜",
      embedding: { model: "model" },
    }))
      received.push(event);
  };
  if (includeTerminal) {
    await consume();
    expect(received).toHaveLength(3);
    expect(received[2]).toEqual({ object: "chunk.embedding.complete", response: terminal });
  } else {
    await expect(consume()).rejects.toThrow("before terminal coverage");
    expect(received).toHaveLength(2);
  }
  expect(received[1]).toEqual({ object: "chunk.embedding", data: item });
});

it("rejects terminal coverage from another plan", async () => {
  const bytes =
    JSON.stringify(start) +
    "\n" +
    JSON.stringify({ object: "chunk.embedding", data: item }) +
    "\n" +
    JSON.stringify({
      object: "chunk.embedding.complete",
      response: { ...terminal, plan_id: "5".repeat(64) },
    }) +
    "\n";
  vi.stubGlobal(
    "fetch",
    vi.fn(async () => new Response(bytes, { headers: { "Content-Type": "application/x-ndjson" } }))
  );
  const client = new InferenceClient({ baseUrl: "http://test" });
  await expect(async () => {
    for await (const _ of client.chunkAndEmbedStream({
      input: "hello",
      embedding: { model: "model" },
    })) {
      /* drain */
    }
  }).rejects.toThrow("coverage does not match");
});

it.each(["missing", "changed-weights"])("rejects an unsafe stream context: %s", async (variant) => {
  const events = [
    ...(variant === "missing" ? [] : [start]),
    { object: "chunk.embedding", data: item },
    {
      object: "chunk.embedding.complete",
      response: { ...terminal, model_identity: "5".repeat(64) },
    },
  ];
  vi.stubGlobal(
    "fetch",
    vi.fn(
      async () =>
        new Response(`${events.map((value) => JSON.stringify(value)).join("\n")}\n`, {
          headers: { "Content-Type": "application/x-ndjson" },
        })
    )
  );
  const client = new InferenceClient({ baseUrl: "http://test" });
  await expect(async () => {
    for await (const _ of client.chunkAndEmbedStream({
      input: "hello",
      embedding: { model: "model" },
    })) {
    }
  }).rejects.toThrow(
    variant === "missing" ? "Missing chunk stream context" : "coverage does not match"
  );
});

it("releases the response when the consumer stops after stream context", async () => {
  let cancelled = false;
  const bytes = new TextEncoder().encode(
    `${JSON.stringify(start)}\n${JSON.stringify({ object: "chunk.embedding", data: item })}\n`
  );
  vi.stubGlobal(
    "fetch",
    vi.fn(
      async () =>
        new Response(
          new ReadableStream({
            start(controller) {
              controller.enqueue(bytes);
            },
            cancel() {
              cancelled = true;
            },
          }),
          { headers: { "Content-Type": "application/x-ndjson" } }
        )
    )
  );
  const iterator = new InferenceClient({ baseUrl: "http://test" }).chunkAndEmbedStream({
    input: "hello",
    embedding: { model: "model" },
  });
  expect((await iterator.next()).value).toEqual(start);
  await iterator.return(undefined);
  expect(cancelled).toBe(true);
});

it.each([
  undefined,
  null,
])("accepts planning-only context without a known vector width: %s", async (dimensions) => {
  const events = [
    { object: "chunk.embedding.start", context: { ...context, dimensions } },
    { object: "chunk.embedding", data: { ...item, embedding: undefined } },
    { object: "chunk.embedding.complete", response: terminal },
  ];
  vi.stubGlobal(
    "fetch",
    vi.fn(
      async () =>
        new Response(`${events.map((event) => JSON.stringify(event)).join("\n")}\n`, {
          headers: { "Content-Type": "application/x-ndjson" },
        })
    )
  );
  const received = [];
  for await (const event of new InferenceClient({ baseUrl: "http://test" }).chunkAndEmbedStream({
    input: "hello",
    embedding: { model: "model", execute: false },
  }))
    received.push(event);
  expect(received).toHaveLength(3);
});
