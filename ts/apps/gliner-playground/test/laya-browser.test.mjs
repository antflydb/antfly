// Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Apache-2.0
import { test } from "node:test";
import assert from "node:assert/strict";
import { chromium } from "@playwright/test";
import { readFile } from "node:fs/promises";
const gpu = process.env.EXTRACTION_GPU === "1";
const model = process.env.LAYA_MODEL;
const base = process.env.PLAYGROUND_URL || "http://127.0.0.1:3101/";
async function ready(page, expected) {
  await page.waitForFunction(text => document.querySelector(".status")?.textContent.includes(text) || document.querySelector("[role=alert]"), expected, { timeout: 120000 });
  assert.deepEqual(await page.locator("[role=alert]").allTextContents(), []);
}
test("Laya browser decisions, oracle parity and cancellation/reload", { skip: !model, timeout: 300000 }, async () => {
  const browser = await chromium.launch(gpu ? { args: ["--enable-unsafe-webgpu", "--use-angle=metal"] } : {});
  try {
    const page = await browser.newPage();
    const requests = [], errors = [];
    page.on("request", request => requests.push({ url: request.url(), method: request.method(), body: request.postData() }));
    page.on("pageerror", error => errors.push(error.message));
    if (!gpu) await page.route(base, async route => {
      const response = await route.fetch(), headers = { ...response.headers() };
      delete headers["cross-origin-opener-policy"]; delete headers["cross-origin-embedder-policy"];
      await route.fulfill({ response, headers });
    });
    await page.goto(base);
    if (gpu) await page.evaluate(async () => {
      const { WebGPUOps } = await import("/inference/webgpu-ops.js");
      const create = WebGPUOps.prototype.createBuffer;
      const dispatch = WebGPUOps.prototype._dispatchMatmul;
      globalThis.gpuProbe = { peakBytes: 0, matmuls: 0, instance: null };
      WebGPUOps.prototype.createBuffer = function (...args) {
        const id = create.apply(this, args);
        globalThis.gpuProbe.instance = this;
        const live = [...this.buffers.values()].reduce((sum, b) => sum + b.size, 0);
        globalThis.gpuProbe.peakBytes = Math.max(globalThis.gpuProbe.peakBytes, live);
        return id;
      };
      WebGPUOps.prototype._dispatchMatmul = function (...args) {
        globalThis.gpuProbe.matmuls++;
        return dispatch.apply(this, args);
      };
    });
    assert.equal(await page.evaluate(() => crossOriginIsolated), gpu);
    await page.getByLabel("Model", { exact: true }).selectOption("laya-english-fp16");
    await page.getByLabel("Backend", { exact: true }).selectOption("webgpu");
    await page.locator("input[webkitdirectory]").setInputFiles(model);
    await page.getByRole("button", { name: /Load local bundle/ }).click();
    await ready(page, "Ready");
    if (!gpu) assert.match(await page.locator(".notice").innerText(), /isolation|SharedArrayBuffer|adapter/i);
    assert.equal(await page.getByRole("button", { name: "entities", exact: true }).count(), 0);
    await page.getByRole("button", { name: "Use example", exact: true }).click();
    for (const [mode, type] of [["single", "choice"], ["ordinal", "score"], ["boolean", "boolean"]]) {
      await page.getByLabel("Decision type", { exact: true }).selectOption(mode);
      await page.getByRole("button", { name: /Run extraction/ }).click();
      await ready(page, "Complete");
      const output = JSON.parse(await page.locator(".results details > pre").last().innerText());
      assert.equal(output.data[0].decisions[0].type, type);
      assert.equal(await page.locator(".decision").count(), 1);
      assert.match(await page.locator(".metrics").innerText(), gpu ? /WEBGPU/i : /WASM/);
    }
    await page.getByLabel("Advanced JSON").check();
    if (process.env.LAYA_ORACLE) {
      const expected = JSON.parse(await readFile(process.env.LAYA_ORACLE, "utf8"));
      const request = {
        schema_version: 2, model: "laya", inputs: [{ id: "local", content: expected[0].text ?? "Please search for the latest documentation about browser inference." }],
        schema: { classifications: [
          { name: "tool", mode: "single", instruction: "Which tool is needed to handle this request?", labels: ["search", "fetch", "none"] },
          { name: "urgency", mode: "ordinal", instruction: "How urgent is this request?", labels: ["low", "medium", "high"] },
          { name: "search_needed", mode: "boolean", instruction: "Does this request require searching for information?", labels: ["false", "true"] },
        ] },
      };
      await page.getByLabel("Request JSON").fill(JSON.stringify(request));
      let previous;
      for (let repeat = 0; repeat < 2; repeat++) {
        await page.getByRole("button", { name: /Run extraction/ }).click();
        await ready(page, "Complete");
        const output = JSON.parse(await page.locator(".results details > pre").last().innerText());
        let maxError = 0;
        for (const [i, decision] of output.data[0].decisions.entries()) {
          const ps = expected[i].probabilities;
          for (const [j, p] of decision.probabilities.entries()) maxError = Math.max(maxError, Math.abs(p.probability - ps[j]));
          const confidence = decision.type === "boolean" ? Math.max(ps[1], 1 - ps[1]) : 1 + ps.reduce((sum, p) => sum + p * Math.log(Math.max(p, 1e-12)), 0) / Math.log(ps.length);
          assert(Math.abs(decision.confidence - confidence) < 5e-5, `${decision.name}: confidence ${decision.confidence}, expected ${confidence}`);
          assert(Math.abs(decision.act_probability - expected[i].act_probability) < 5e-5);
          if (decision.type === "score") assert(Math.abs(decision.expected_value - ps.reduce((sum, p, j) => sum + p * j, 0)) < 5e-5);
        }
        console.log({ gpu, repeat, maxProbabilityError: maxError });
        assert(maxError < 5e-5);
        if (previous) assert.deepEqual(output, previous);
        previous = output;
        if (gpu) {
          const stats = await page.evaluate(() => ({ peakBytes: gpuProbe.peakBytes, matmuls: gpuProbe.matmuls, remaining: gpuProbe.instance.buffers.size }));
          console.log(stats);
          assert(stats.peakBytes < 1024 ** 3, "Resident weights and activations must stay within the GPU budget");
          assert(stats.matmuls > 0, "Must actually dispatch GPU projections");
          assert(stats.remaining > 0, "Model weights remain resident between requests");
        }
      }
    }
    const original = await page.getByLabel("Request JSON").inputValue();
    await page.getByLabel("Advanced JSON").uncheck(); await page.getByLabel("Advanced JSON").check();
    assert.equal(await page.getByLabel("Request JSON").inputValue(), original);
    await page.getByRole("button", { name: /Run extraction/ }).click();
    await page.getByRole("button", { name: "Cancel", exact: true }).click();
    await page.waitForFunction(() => !document.querySelector(".spinner"));
    await page.getByRole("button", { name: /Run extraction/ }).click();
    await ready(page, "Complete");
    if (gpu) {
      const before = await page.evaluate(() => gpuProbe.matmuls);
      await page.getByRole("button", { name: /Run extraction/ }).click();
      await page.waitForFunction(n => gpuProbe.matmuls > n, before);
      await page.evaluate(() => gpuProbe.instance.device.destroy());
      await page.getByRole("alert").waitFor();
      assert.match(await page.getByRole("alert").innerText(), /lost|destroy|WebGPU/i);
      await page.getByRole("button", { name: /Run extraction/ }).click();
      await ready(page, "Complete");
    }
    await page.setViewportSize({ width: 390, height: 844 });
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth > innerWidth), false);
    if (process.env.LAYA_SCREENSHOT) await page.screenshot({ path: process.env.LAYA_SCREENSHOT, fullPage: true });
    await page.getByRole("button", { name: "Unload", exact: true }).click();
    assert.deepEqual(errors, []);
    assert(!requests.some(r => r.body || !["GET", "HEAD"].includes(r.method) || new URL(r.url).origin !== new URL(base).origin));
  } finally { await browser.close(); }
});
