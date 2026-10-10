// Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Apache-2.0

import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { test } from "node:test";
import { chromium } from "@playwright/test";
import {
  layaFixture,
  layaRequest,
} from "../../../../../zig/pkg/inference/web/laya-test-fixture.mjs";
import { ExtractionSession } from "../../../../../zig/pkg/inference/web/runtime/extraction-session.js";
import "../../../../../zig/pkg/inference/web/configuration-test-runtime.mjs";
import { createWasmAbi } from "../../../../../zig/pkg/inference/web/runtime/wasm-abi.js";
import { startRuntimeServer } from "./server.mjs";

const packageUrl = "/client/index.js";
const gpu = process.env.EXTRACTION_GPU === "1";
const wasmPath = new URL("../../../../../zig/zig-out/antfly-extraction-cpu.wasm", import.meta.url);
const launch = () => chromium.launch({ args: ["--enable-unsafe-webgpu", "--use-angle=metal"] });

test("public Inference decisions run through the browser worker on WASM", {
  timeout: 30000,
}, async () => {
  const server = await startRuntimeServer();
  const browser = await chromium.launch();
  try {
    const page = await browser.newPage();
    await page.goto(server.url);
    const payload = await Promise.all(
      [...layaFixture()].map(async ([p, b]) => [
        p,
        Array.from(new Uint8Array(await b.arrayBuffer())),
      ])
    );
    const result = await page.evaluate(async (payload) => {
      const { Inference } = await import("/client/inference.js");
      const inference = await Inference.create();
      const files = new Map(payload.map(([p, b]) => [p, new Blob([new Uint8Array(b)])]));
      try {
        const inspection = await inference.inspectModel(files);
        const model = await inference.loadModel(files, { backend: "wasm", precision: "fp32" });
        const request = {
          model: "local",
          input: "Please search.",
          questions: [{ name: "act", type: "predicate", instructions: "Should we act?" }],
        };
        const validation = await inference.validateDecision(request);
        const decision = await inference.decide(request);
        let unsupported;
        try {
          await inference.extract({
            schema_version: 2,
            model: "local",
            inputs: [{ content: "x" }],
            schema: { entities: ["person"] },
          });
        } catch (error) {
          unsupported = error.code;
        }
        return {
          inspection,
          model,
          validation,
          decision,
          unsupported,
          status: inference.state.status,
        };
      } finally {
        inference.dispose();
      }
    }, payload);
    assert.deepEqual(result.inspection.execution.tasks, ["decide"]);
    assert.equal(result.model.family, "laya");
    assert.equal(result.model.architecture, "modernbert");
    assert.equal(result.model.backend, "wasm");
    assert(result.validation.value.encoded_tokens > 0);
    assert.equal(result.decision.value.answers[0].type, "predicate");
    assert.equal(result.decision.value.answers[0].decision_method, "typed");
    assert.equal(result.unsupported, "UNSUPPORTED_TASK");
    assert.equal(result.status, "ready");
  } finally {
    await browser.close();
    await server.close();
  }
});

test("Laya WebGPU pointer/Q8 parity and explicit packed CPU fallback", {
  skip: !gpu,
  timeout: 180000,
}, async () => {
  const server = await startRuntimeServer();
  const browser = await launch();
  const { instance } = await WebAssembly.instantiate(await readFile(wasmPath), { env: {} });
  const cpu = new ExtractionSession(instance.exports, createWasmAbi(instance.exports));
  try {
    const page = await browser.newPage();
    await page.goto(server.url);
    for (const [extra, precision] of [
      [{}, "fp16"],
      [{ decision_head: "pointer", pointer_dim: 32 }, "fp16"],
      [{ weight_quantization: "q8_0" }, "fp16"],
      [{ packing: { mode: "candidate", max_packed_len: 2048, two_stage: { top_k: 2 } } }, "fp16"],
      [{ format: "opendecider" }, "fp16"],
      [{ format: "opendecider" }, "bf16"],
    ]) {
      const files = layaFixture(extra, precision);
      await cpu.load(files, precision);
      const expected = cpu.run(layaRequest).value;
      cpu.unload();
      const payload = await Promise.all(
        [...files].map(async ([p, b]) => [p, Array.from(new Uint8Array(await b.arrayBuffer()))])
      );
      const output = await page.evaluate(
        async ({ packageUrl, payload, request, precision }) => {
          const { InferenceClient } = await import(packageUrl),
            client = new InferenceClient();
          try {
            const model = await client.loadModel(
              new Map(payload.map(([p, b]) => [p, new Blob([new Uint8Array(b)])])),
              { backend: "webgpu", precision }
            );
            const result = await client.run(request);
            return { model, result };
          } finally {
            client.dispose();
          }
        },
        { packageUrl, payload, request: layaRequest, precision }
      );
      assert.equal(
        output.model.backend,
        extra.packing || precision === "bf16" ? "wasm" : "webgpu",
        JSON.stringify(extra)
      );
      if (extra.packing) assert.match(output.model.fallbackReason, /segment attention/);
      for (let i = 0; i < expected.data[0].decisions.length; i++) {
        const want = expected.data[0].decisions[i],
          got = output.result.value.data[0].decisions[i];
        assert.equal(got.label, want.label);
        for (let j = 0; j < want.probabilities.length; j++)
          assert(
            Math.abs(got.probabilities[j].probability - want.probabilities[j].probability) < 2e-5,
            JSON.stringify(extra)
          );
        if (extra.format === "opendecider") assert.equal(got.act_probability, undefined);
      }
    }
  } finally {
    cpu.unload();
    await browser.close();
    await server.close();
  }
});

test("ModernBERT marker and boundary run through the public browser worker", {
  timeout: 60000,
}, async () => {
  const { modernFixture } = await import(
    "../../../../../zig/pkg/inference/web/model-backend-test-fixtures.mjs"
  );
  const server = await startRuntimeServer();
  const browser = await chromium.launch();
  try {
    const page = await browser.newPage();
    await page.goto(server.url);
    for (const boundary of [false, true]) {
      const payload = await Promise.all(
        [...(await modernFixture(boundary))].map(async ([path, file]) => [
          path,
          Array.from(new Uint8Array(await file.arrayBuffer())),
        ])
      );
      const result = await page.evaluate(async (payload) => {
        const { Inference } = await import("/client/inference.js");
        const inference = await Inference.create();
        try {
          const files = new Map(
            payload.map(([path, bytes]) => [path, new Blob([new Uint8Array(bytes)])])
          );
          const model = await inference.loadModel(files, { backend: "wasm", precision: "fp32" });
          const request = {
            model: "local",
            input: "state state",
            questions: [
              {
                name: "tool",
                type: "choice",
                instructions: "question",
                choices: [{ value: "search" }, { value: "fetch" }],
              },
            ],
          };
          const validation = await inference.validateDecision(request);
          const decision = await inference.decide(request);
          const extraction = await inference.extract({
            schema_version: 2,
            model: "local",
            inputs: [{ content: "state state" }],
            schema: {
              classifications: [{ name: "tool", mode: "single", labels: ["search", "fetch"] }],
            },
          });
          return { model, validation, decision, extraction };
        } finally {
          await inference.dispose();
        }
      }, payload);
      assert.equal(result.validation.value.valid, true);
      assert.equal(result.decision.value.answers[0].choice, "search");
      assert.equal(result.extraction.value.data[0].classifications[0].label, "search");
      assert.deepEqual(result.model.execution.tasks, ["extract", "decide"]);
    }
  } finally {
    await browser.close();
    await server.close();
  }
});
