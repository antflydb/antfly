// Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Apache-2.0
import assert from "node:assert/strict";
import { test } from "node:test";
import { chromium } from "@playwright/test";
import { startRuntimeServer } from "./server.mjs";

test("a WebGPU WASM artifact loads in direct and worker modes without a GPU", {
  timeout: 90000,
}, async () => {
  const server = await startRuntimeServer();
  const browser = await chromium.launch();
  try {
    const page = await browser.newPage();
    await page.goto(server.url);
    const result = await page.evaluate(async () => {
      const { InferenceWeb } = await import("/inference/inference-web.js");
      const wasm = "/inference/antfly-extraction-webgpu.wasm";
      for (const worker of [false, true]) {
        const runtime = new InferenceWeb();
        try {
          await runtime.init(wasm, {
            worker,
            workerUrl: "/inference/inference-worker.js",
            wasmMemoryModel: "wasm32",
          });
        } finally {
          runtime.destroy();
        }
      }
      return true;
    });
    assert.equal(result, true);
  } finally {
    await browser.close();
    await server.close();
  }
});
