// Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Apache-2.0
import assert from "node:assert/strict";
import { test } from "node:test";
import { resolveAdapter, inspectModel } from "../dist/runtime-assets/runtime/model-adapters.js";
const marker = {
  model_type: "extractor",
  architecture: "span",
  config_version: 3,
  architecture_version: 1,
  span_head: { span_mode: "markerV0" },
};
test("configuration adapters preserve metadata and restrict executable tasks", () => {
  const model = resolveAdapter(
    marker,
    { model_type: "deberta-v2" },
    { tasks: ["extract", "decide", "future"], capabilities: ["classification", "future"] }
  );
  assert.deepEqual(model.tasks, ["extract", "decide", "future"]);
  assert.deepEqual(model.execution.tasks, ["extract"]);
  assert.deepEqual(model.execution.capabilities, ["classification"]);
  assert.deepEqual(model.execution.decisionKinds, []);
  assert.throws(() => resolveAdapter(marker, {}, { tasks: "extract" }), /metadata/);
});
test("native decider routes expose their executable capabilities", () => {
  for (const [config, encoder, kinds] of [
    [marker, { model_type: "modernbert" }, ["choice", "score", "predicate"]],
    [{ architecture: "boundary" }, { model_type: "modernbert" }, ["choice", "score", "predicate"]],
    [
      { model_type: "modernbert", vocab_size: 262144, laya: {} },
      undefined,
      ["choice", "score", "predicate"],
    ],
    [{ model_type: "embedding_gemma2_text" }, undefined, ["choice", "multi_choice"]],
  ]) {
    const result = resolveAdapter(config, encoder);
    assert.equal(result.availability.available, true);
    assert(result.execution.tasks.includes("decide"));
    assert.deepEqual(result.execution.decisionKinds, kinds);
  }
});
test("unsupported geometry and unknown embedding encoders report reasons", () => {
  for (const [config, encoder, manifest, reason] of [
    [marker, { model_type: "modernbert", hidden_size: 4096 }, undefined, /geometry/],
    [
      { architecture: "boundary" },
      { model_type: "modernbert", num_hidden_layers: 100 },
      undefined,
      /geometry/,
    ],
    [
      { model_type: "modernbert", vocab_size: 262145, laya: {} },
      undefined,
      undefined,
      /vocabulary/,
    ],
    [
      { model_type: "unknown_embedding_encoder" },
      undefined,
      { tasks: ["embed", "decide"], capabilities: ["embedding_similarity"] },
      /encoder adapter/,
    ],
  ]) {
    const result = resolveAdapter(config, encoder, manifest);
    assert.equal(result.availability.available, false);
    assert.match(result.availability.reason, reason);
    assert.deepEqual(result.execution.tasks, []);
  }
});
test("metadata inspection needs no weights and supports upstream Laya folders", async (t) => {
  // The npm package ships JS assets; preparation supplies the CPU WASM.
  const previousFetch = globalThis.fetch;
  const previousCompile = WebAssembly.compile;
  const previousInstantiate = WebAssembly.instantiate;
  t.after(() => {
    globalThis.fetch = previousFetch;
    WebAssembly.compile = previousCompile;
    WebAssembly.instantiate = previousInstantiate;
  });
  globalThis.fetch = async () => ({ ok: true, arrayBuffer: async () => new ArrayBuffer(0) });
  WebAssembly.compile = async () => ({});
  let reason = "";
  WebAssembly.instantiate = async () => {
    const memory = { buffer: new ArrayBuffer(65536) };
    return {
      exports: {
        memory,
        extraction_abi_version: () => 2,
        wasm_alloc: () => 1024,
        wasm_dealloc: () => {},
        extraction_create: (ptr, len) => {
          const metadata = JSON.parse(
            new TextDecoder().decode(new Uint8Array(memory.buffer, ptr, len))
          );
          assert.equal(JSON.parse(metadata.config).model_type, "modernbert");
          new Uint8Array(memory.buffer, 0, reason.length).set(new TextEncoder().encode(reason));
          return reason ? 0 : 1;
        },
        extraction_unload: () => {},
        extraction_error_ptr: () => 0,
        extraction_error_len: () => reason.length,
      },
    };
  };
  const files = new Map([
    [
      "encoder/config.json",
      new Blob([JSON.stringify({ model_type: "modernbert", vocab_size: 100 })]),
    ],
    ["rl_agent_config.json", new Blob(["{}"])],
    [
      "model_manifest.json",
      new Blob([JSON.stringify({ tasks: ["decide"], capabilities: ["typed_decisions"] })]),
    ],
  ]);
  const result = await inspectModel(files);
  assert.equal(result.family, "laya");
  assert.deepEqual(result.execution.tasks, ["decide"]);
  assert.deepEqual(result.execution.capabilities, ["typed_decisions"]);
  assert(!files.has("config.json"));
  reason = "UnsupportedLayaConfig";
  const unsupported = await inspectModel(files);
  assert.equal(unsupported.availability.available, false);
  assert.equal(unsupported.availability.reason, reason);
  assert.deepEqual(unsupported.execution.tasks, []);
  assert.deepEqual(unsupported.execution.capabilities, []);
  assert.deepEqual(unsupported.execution.decisionKinds, []);
});

test("native role corrections keep older Laya metadata usable as a decider", () => {
  const result = resolveAdapter({ model_type: "modernbert", laya: {} }, undefined, {
    tasks: ["extract", "classify"],
    capabilities: ["classification", "extraction", "typed_decisions"],
  });
  assert.deepEqual(result.tasks, ["decide"]);
  assert.deepEqual(result.capabilities, ["typed_decisions"]);
  assert.deepEqual(result.execution.tasks, ["decide"]);
});

test("explicit empty or decision-only capabilities cannot inherit extraction tasks", () => {
  for (const capabilities of [[], ["typed_decisions"], ["future"]]) {
    const model = resolveAdapter(
      marker,
      { model_type: "modernbert" },
      {
        tasks: ["extract", "decide"],
        capabilities,
      }
    );
    assert.equal(model.execution.tasks.includes("extract"), false);
    assert.equal(
      model.execution.tasks.includes("decide"),
      capabilities.includes("typed_decisions")
    );
  }
});
