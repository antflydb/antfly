// Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Apache-2.0
import assert from "node:assert/strict";
import { test } from "node:test";
import { chromium } from "@playwright/test";
import { startRuntimeServer } from "./server.mjs";

// Opt-in released DeBERTa Decide checkpoint test. Tiny ModernBERT fixtures do
// not exercise its exact-GELU FFN or disentangled-attention residency path.
test("Decide keeps encoder activations on WebGPU with CPU output parity", {
  skip: process.env.EXTRACTION_GPU !== "1" || !process.env.DECIDE_MODEL,
  timeout: 300000,
}, async () => {
  const server = await startRuntimeServer();
  const browser = await chromium.launch({ args: ["--enable-unsafe-webgpu", "--use-angle=metal"] });
  try {
    const page = await browser.newPage();
    await page.goto(server.url);
    await page.setContent('<input type="file" webkitdirectory multiple>');
    await page.locator("input").setInputFiles(process.env.DECIDE_MODEL);
    const result = await page.evaluate(async () => {
      const { Inference } = await import("/client/inference.js");
      const { WebGPUOps } = await import("/inference/webgpu-ops.js");
      const original = WebGPUOps.prototype.handleWorkerCommand;
      let stats;
      WebGPUOps.prototype.handleWorkerCommand = function (msg, sab) {
        if (stats) {
          stats.commands[msg.cmd] = (stats.commands[msg.cmd] ?? 0) + 1;
          if (msg.cmd === "download") stats.downloadBytes += Number(msg.size ?? msg.sizeBytes);
          if (msg.cmd === "upload" || msg.cmd === "write_buffer_at_offset")
            stats.uploadBytes += Number(msg.size ?? msg.sizeBytes);
        }
        return original.call(this, msg, sab);
      };
      const files = new Map(
        [...document.querySelector("input").files].map((file) => [
          file.webkitRelativePath.split("/").slice(1).join("/"),
          file,
        ]),
      );
      const requests = [
        [
          "Please search for the latest documentation about browser inference.",
          "Which tool is needed to handle this request?",
          ["search", "fetch", "none"],
        ],
        [
          "My subscription renewed for ¥5,400 after the service went down. Please refund it.",
          "What does the customer want?",
          ["refund", "cancel", "support"],
        ],
        [
          "Bonjour — merci! No search is needed; just say hello.",
          "Does this request require searching for information?",
          ["false", "true"],
        ],
      ].map(([content, instruction, labels]) => ({
        schema_version: 2,
        model: "decide",
        inputs: [{ content }],
        schema: { classifications: [{ name: "decision", mode: "single", instruction, labels }] },
        options: { include_confidence: true },
      }));
      const runs = {};
      try {
        for (const backend of ["wasm", "webgpu"]) {
          const client = new Inference("/inference/");
          try {
            const model = await client.loadModel(files, { backend, precision: "q8_0" });
            if (model.backend !== backend)
              throw new Error(model.fallbackReason ?? "Unexpected backend");
            runs[backend] = [];
            for (const request of [...requests, requests[0], requests[0], requests[0]]) {
              // CPU only needs one reference for each distinct request.
              if (backend === "wasm" && runs[backend].length === requests.length) break;
              stats = { commands: {}, uploadBytes: 0, downloadBytes: 0 };
              const output = await client.run(request);
              runs[backend].push({ ms: output.elapsedMs, value: output.value, ...stats });
            }
          } finally {
            client.dispose();
          }
        }
        return runs;
      } finally {
        WebGPUOps.prototype.handleWorkerCommand = original;
      }
    });
    const near = (actual, expected) => {
      if (typeof expected === "number") {
        assert(Number.isFinite(actual));
        assert(Math.abs(actual - expected) < 5e-4, `${actual} != ${expected}`);
      } else if (expected && typeof expected === "object") {
        assert.deepEqual(Object.keys(actual), Object.keys(expected));
        for (const key of Object.keys(expected)) near(actual[key], expected[key]);
      } else assert.equal(actual, expected);
    };
    for (const [i, run] of result.webgpu.entries()) {
      near(run.value, result.wasm[i < 3 ? i : 0].value);
      assert((run.commands.deberta_disentangled_attention ?? 0) > 0, "DeBERTa must run on GPU");
      assert(
        (run.commands.modern_op ?? 0) >= run.commands.deberta_disentangled_attention,
        "Each encoder layer must dispatch erf GELU on GPU",
      );
      assert((run.commands.download ?? 0) <= 2, JSON.stringify(run));
      assert(run.downloadBytes < 1024 * 1024, JSON.stringify(run));
    }
    // Relative-position embeddings and masks still upload per layer. Record
    // those costs separately from the eliminated activation readbacks.
    console.log(JSON.stringify(result));
  } finally {
    await browser.close();
    await server.close();
  }
});
