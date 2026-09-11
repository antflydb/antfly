import { afterEach, expect, it, vi } from "vitest";
import { InferenceAPIError, InferenceClient } from "../src/index.js";

afterEach(() => vi.unstubAllGlobals());

it.each([undefined, "auto", "exact", "beam"] as const)("decoder preserves omitted and explicit algorithm %s", async (algorithm) => {
  const decoder = algorithm === undefined ? { beam_width: 16 } : { algorithm, beam_width: 16 };
  vi.stubGlobal("fetch", vi.fn(async (request: Request) => {
    const wire = await request.json();
    expect(wire.options.decoder).toEqual(decoder);
    if (algorithm === undefined) expect(wire.options.decoder).not.toHaveProperty("algorithm");
    return new Response(JSON.stringify({ object: "extraction", model: "m", schema_version: 2, data: [{}] }), {
      status: 200, headers: { "Content-Type": "application/json" },
    });
  }));
  const client = new InferenceClient({ baseUrl: "http://test" });
  await client.extractV2({ model: "m", schema: { entities: ["person"] }, inputs: [{ content: "Ada" }], options: { decoder } });
});

it("extraction v2 preserves null, false, and whole empty replacements", async () => {
  let body: unknown;
  vi.stubGlobal("fetch", vi.fn(async (request: Request) => {
    body = await request.json();
    return new Response(JSON.stringify({
      object: "extraction", model: "m", schema_version: 2,
      data: [{ offset_unit: "utf8_bytes" }, { offset_unit: "utf8_bytes" }],
    }), { status: 200, headers: { "Content-Type": "application/json" } });
  }));
  const request = {
    model: "m",
    schema: { classifications: [{ name: "t", labels: ["a", "b"], max_labels: null, ordered: false }] },
    options: { threshold: 0, include_spans: false, word_splitter: "char" as const },
    inputs: [{ content: "Ada", options: {} }, { content: "Bob" }],
  };
  const client = new InferenceClient({ baseUrl: "http://test" });
  await client.extractV2(request);
  expect(body).toEqual({ ...request, schema_version: 2 });
  expect(request).not.toHaveProperty("schema_version");
});

it("extraction v2 errors retain atomic input zero and stage", async () => {
  vi.stubGlobal("fetch", vi.fn(async () => new Response(JSON.stringify({
    error: "EXTRACTION_SEARCH_EXHAUSTED", message: "no accepted witness", input_index: 0, stage: "decode",
  }), { status: 422, headers: { "Content-Type": "application/json" } })));
  const client = new InferenceClient({ baseUrl: "http://test" });
  const call = client.extractV2({ model: "m", schema: { entities: ["p"] }, inputs: [{ content: "Ada" }] });
  await expect(call).rejects.toBeInstanceOf(InferenceAPIError);
  await expect(call).rejects.toMatchObject({ status: 422, code: "EXTRACTION_SEARCH_EXHAUSTED", inputIndex: 0, stage: "decode" });
});

it("extraction v2 preserves long-document identity and record solver metadata", async () => {
  const output = {
    offset_unit: "utf8_bytes",
    long_document: {
      version: 1, window_count: 3, window_policy: "source_words_midpoint_ownership",
      classification_aggregation: "owned_word_weighted_mean_raw_logits",
      duplicate_score: "maximum_calibrated_score", natural_record_identity: "exact_source_anchor",
      other_record_identity: "semantic", solver_optimality_scope: "retained_candidate_graph",
    },
    solvers: { records: { status: "feasible", utility: 1.25, visited_nodes: 0, exhausted: true } },
  };
  vi.stubGlobal("fetch", vi.fn(async (request: Request) => {
    expect((await request.json()).options.long_document).toEqual({ mode: "window", record_identity: "semantic" });
    return new Response(JSON.stringify({ object: "extraction", model: "m", schema_version: 2, data: [output] }),
      { status: 200, headers: { "Content-Type": "application/json" } });
  }));
  const client = new InferenceClient({ baseUrl: "http://test" });
  const response = await client.extractV2({
    model: "m", schema: { entities: ["person"] }, inputs: [{ content: "Ada" }],
    options: { long_document: { mode: "window", record_identity: "semantic" } },
  });
  expect(response.data?.[0]).toEqual(output);
  expect(response.data?.[0].long_document?.version).toBe(1);
  expect(response.data?.[0].solvers?.records?.exhausted).toBe(true);
});

const identifiedRequest = {
  model: "m", schema: { entities: ["person"] },
  inputs: [{ id: "a", content: "Ada" }, { id: "b", content: "Bob" }],
};

function extractionEnvelope(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    object: "extraction", model: "m", schema_version: 2,
    data: [{ id: "a", offset_unit: "utf8_bytes" }, { id: "b", offset_unit: "utf8_bytes" }],
    ...overrides,
  };
}

function respondWith(payload: unknown): void {
  vi.stubGlobal("fetch", vi.fn(async () => new Response(JSON.stringify(payload), {
    status: 200, headers: { "Content-Type": "application/json" },
  })));
}

const invalidEnvelopes: [string, unknown][] = [
  ["null envelope", null],
  ["array envelope", []],
  ["string envelope", "extraction"],
  ["boolean envelope", true],
  ["missing object", extractionEnvelope({ object: undefined })],
  ["wrong object", extractionEnvelope({ object: "embedding" })],
  ["null object", extractionEnvelope({ object: null })],
  ["missing version", extractionEnvelope({ schema_version: undefined })],
  ["wrong version", extractionEnvelope({ schema_version: 1 })],
  ["string version", extractionEnvelope({ schema_version: "2" })],
  ["null version", extractionEnvelope({ schema_version: null })],
  ["missing model", extractionEnvelope({ model: undefined })],
  ["wrong model", extractionEnvelope({ model: "other/model" })],
  ["null model", extractionEnvelope({ model: null })],
  ["numeric model", extractionEnvelope({ model: 4 })],
  ["missing data", extractionEnvelope({ data: undefined })],
  ["null data", extractionEnvelope({ data: null })],
  ["object data", extractionEnvelope({ data: {} })],
  ["empty data", extractionEnvelope({ data: [] })],
  ["partial data", extractionEnvelope({ data: [{ id: "a" }] })],
  ["extra data", extractionEnvelope({ data: [{ id: "a" }, { id: "b" }, {}] })],
  ["null row", extractionEnvelope({ data: [null, { id: "b" }] })],
  ["array row", extractionEnvelope({ data: [[], { id: "b" }] })],
  ["string row", extractionEnvelope({ data: ["a", { id: "b" }] })],
  ["missing explicit id", extractionEnvelope({ data: [{}, { id: "b" }] })],
  ["wrong id", extractionEnvelope({ data: [{ id: "other" }, { id: "b" }] })],
  ["reordered ids", extractionEnvelope({ data: [{ id: "b" }, { id: "a" }] })],
  ["null explicit id", extractionEnvelope({ data: [{ id: null }, { id: "b" }] })],
  ["numeric id", extractionEnvelope({ data: [{ id: 0 }, { id: "b" }] })],
];

it.each(invalidEnvelopes)("extraction v2 rejects %s", async (_name, payload) => {
  respondWith(payload);
  const client = new InferenceClient({ baseUrl: "http://test" });
  await expect(client.extractV2(identifiedRequest)).rejects.toThrow();
  // Explicit V2 callers of the raw API receive the same validation.
  await expect(client.extractRaw({ ...identifiedRequest, schema_version: 2 })).rejects.toThrow();
});

it("extraction v2 retains repeated IDs by position, empty IDs, offsets, and raw extensions", async () => {
  const response = extractionEnvelope({
    future: { enabled: true },
    data: [
      { id: "repeat", offset_unit: "utf8_bytes", future_row: { index: 0 }, entities: [
        { label: "person", text: "Ada", start: 0, end: 3, future_entity: 7 },
      ] },
      { id: "repeat", offset_unit: "utf8_bytes", entities: [{ label: "person", text: "Bob" }] },
      { id: "", offset_unit: "utf8_bytes" },
      { offset_unit: "utf8_bytes" },
    ],
  });
  respondWith(response);
  const client = new InferenceClient({ baseUrl: "http://test" });
  const result = await client.extractV2({
    ...identifiedRequest,
    inputs: [{ id: "repeat", content: "Ada" }, { id: "repeat", content: "Bob" },
      { id: "", content: "Eve" }, { content: "Max" }],
  });
  expect(result).toEqual(response);
  expect(result.data[0].entities?.[0].start).toBe(0);
  expect(result.data[1].entities?.[0]).not.toHaveProperty("start");
  expect(result.data[3]).not.toHaveProperty("id");
});

it.each(["unexpected", null, 0, false, [], {}].map((id) => ({ id })))("extraction v2 rejects a present anonymous id $id", async ({ id }) => {
  respondWith(extractionEnvelope({ data: [{ id }] }));
  const client = new InferenceClient({ baseUrl: "http://test" });
  await expect(client.extractV2({ ...identifiedRequest, inputs: [{ content: "Ada" }] }))
    .rejects.toThrow("item 0 id");
});

it("extraction v2 does not treat an empty explicit ID as anonymous", async () => {
  respondWith(extractionEnvelope({ data: [{}] }));
  const client = new InferenceClient({ baseUrl: "http://test" });
  await expect(client.extractV2({ ...identifiedRequest, inputs: [{ id: "", content: "Ada" }] }))
    .rejects.toThrow("item 0 id");
});

it("extraction v2 binds response IDs to the submitted request, retaining legacy behavior", async () => {
  const request = { ...identifiedRequest, inputs: [{ id: "a", content: "Ada" }] };
  vi.stubGlobal("fetch", vi.fn(async (httpRequest: Request) => {
    expect((await httpRequest.json()).inputs[0].id).toBe("a");
    request.inputs[0].id = "changed after submission";
    return new Response(JSON.stringify(extractionEnvelope({ data: [{ id: "a" }] })), {
      status: 200, headers: { "Content-Type": "application/json" },
    });
  }));
  const client = new InferenceClient({ baseUrl: "http://test" });
  expect((await client.extractV2(request)).data[0].id).toBe("a");

  const legacy = { object: "extraction", model: "legacy-model", data: [{ entities: [] }] };
  respondWith(legacy);
  expect(await client.extractRaw(identifiedRequest)).toEqual(legacy);
});
