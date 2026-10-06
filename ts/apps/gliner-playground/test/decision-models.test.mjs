// Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Apache-2.0
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { chromium } from '@playwright/test';
import { layaFixture, layaRequest } from '../../../../zig/pkg/inference/web/laya-test-fixture.mjs';
import { ExtractionSession } from '../../../../zig/pkg/inference/web/runtime/extraction-session.js';
import { createWasmAbi } from '../../../../zig/pkg/inference/web/runtime/wasm-abi.js';
const base = process.env.PLAYGROUND_URL || 'http://127.0.0.1:3101/';
const packageUrl = `/@fs${new URL('../../../packages/inference-web/src/index.ts', import.meta.url).pathname}`;
const gpu = process.env.EXTRACTION_GPU === '1';
const wasmPath = new URL('../../../../zig/zig-out/antfly-extraction-cpu.wasm', import.meta.url);
const launch = () => chromium.launch({ args: ['--enable-unsafe-webgpu', '--use-angle=metal'] });

test('Laya WebGPU pointer/Q8 parity and explicit packed CPU fallback', { skip: !gpu, timeout: 180000 }, async () => {
  const browser = await launch();
  const { instance } = await WebAssembly.instantiate(await readFile(wasmPath), { env: {} });
  const cpu = new ExtractionSession(instance.exports, createWasmAbi(instance.exports));
  try {
    const page = await browser.newPage(); await page.goto(base);
    for (const [extra, precision] of [[{}, 'fp16'], [{ decision_head: 'pointer', pointer_dim: 32 }, 'fp16'], [{ weight_quantization: 'q8_0' }, 'fp16'], [{ packing: { mode: 'candidate', max_packed_len: 2048, two_stage: { top_k: 2 } } }, 'fp16'], [{ format: 'opendecider' }, 'fp16'], [{ format: 'opendecider' }, 'bf16']]) {
      const files = layaFixture(extra, precision);
      await cpu.load(files, precision); const expected = cpu.run(layaRequest).value; cpu.unload();
      const payload = await Promise.all([...files].map(async ([p, b]) => [p, Array.from(new Uint8Array(await b.arrayBuffer()))]));
      const output = await page.evaluate(async ({ packageUrl, payload, request, precision }) => {
        const { InferenceClient } = await import(packageUrl), client = new InferenceClient();
        try {
          const model = await client.loadModel(new Map(payload.map(([p, b]) => [p, new Blob([new Uint8Array(b)])])), { backend: 'webgpu', precision });
          const result = await client.run(request);
          return { model, result };
        } finally { client.dispose(); }
      }, { packageUrl, payload, request: layaRequest, precision });
      assert.equal(output.model.backend, extra.packing || precision === 'bf16' ? 'wasm' : 'webgpu', JSON.stringify(extra));
      if (extra.packing) assert.match(output.model.fallbackReason, /segment attention/);
      for (let i = 0; i < expected.data[0].decisions.length; i++) {
        const want = expected.data[0].decisions[i], got = output.result.value.data[0].decisions[i];
        assert.equal(got.label, want.label);
        for (let j = 0; j < want.probabilities.length; j++) assert(Math.abs(got.probabilities[j].probability - want.probabilities[j].probability) < 2e-5, JSON.stringify(extra));
        if (extra.format === 'opendecider') assert.equal(got.act_probability, undefined);
      }
    }
  } finally { cpu.unload(); await browser.close(); }
});

test('Decide browser UI loads Q8, builds classification requests and matches reference', { skip: !process.env.DECIDE_MODEL, timeout: 300000 }, async () => {
  const browser = await launch();
  try {
    const page = await browser.newPage(); await page.goto(base);
    await page.getByLabel('Model', { exact: true }).selectOption('gliner25-decide-q8');
    await page.getByLabel('Backend', { exact: true }).selectOption(gpu ? 'webgpu' : 'wasm');
    await page.locator('input[webkitdirectory]').setInputFiles(process.env.DECIDE_MODEL);
    await page.getByRole('button', { name: /Load local bundle/ }).click();
    const ready = async expected => {
      await page.waitForFunction(expected => document.querySelector('.status')?.textContent.includes(expected) || document.querySelector('[role=alert]'), expected, { timeout: 180000 });
      assert.deepEqual(await page.locator('[role=alert]').allTextContents(), []);
    };
    await ready('Ready');
    assert.equal(await page.getByRole('button', { name: 'entities', exact: true }).count(), 0);
    await page.getByLabel('Decision type', { exact: true }).selectOption('boolean');
    await page.getByLabel('Input text').fill('Please search the documentation.');
    await page.getByRole('button', { name: /Run extraction/ }).click(); await ready('Complete');
    assert.match(await page.locator('.metrics').innerText(), gpu ? /WEBGPU/i : /WASM/i);
    const fixture = JSON.parse(await readFile(new URL('../../../../zig/pkg/inference/testdata/gliner25/decide/cases.json', import.meta.url)));
    await page.getByLabel('Advanced JSON').check();
    for (const item of [fixture.classification[0], fixture.classification[2], fixture.classification[4]]) {
      await page.getByLabel('Request JSON').fill(JSON.stringify({ schema_version: 2, model: 'decide', schema: item.v2_schema, inputs: [{ content: item.text }], options: { include_confidence: true } }));
      await page.getByRole('button', { name: /Run extraction/ }).click(); await ready('Complete');
      const result = JSON.parse(await page.locator('.results details > pre').last().innerText());
      assert(result.data[0].classifications.some(c => c.label === item.result[item.v2_schema.classifications[0].name].label), item.name);
    }
    assert.equal(await page.locator('.score meter').count() > 0, true);
  } finally { await browser.close(); }
});
