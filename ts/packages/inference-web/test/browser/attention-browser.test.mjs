// Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Apache-2.0

import assert from "node:assert/strict";
import { test } from "node:test";
import { chromium } from "@playwright/test";
import { startRuntimeServer } from "./server.mjs";

test("WebGPU attention reduction synchronizes across SIMD groups and 256-key boundary", {
  skip: process.env.EXTRACTION_GPU !== "1",
  timeout: 90000,
}, async () => {
  const server = await startRuntimeServer();
  const browser = await chromium.launch({ args: ["--enable-unsafe-webgpu", "--use-angle=metal"] });
  try {
    const page = await browser.newPage();
    await page.goto(server.url);
    const cases = await page.evaluate(async () => {
      const { WebGPUOps } = await import("/inference/webgpu-ops.js");
      const gpu = new WebGPUOps();
      if (!(await gpu.init())) throw new Error(gpu.lastInitError || "WebGPU unavailable");
      const results = [];
      try {
        for (const seq of [33, 256, 257, 284, 512]) {
          const batch = 2,
            heads = 2,
            dim = 64,
            count = batch * seq * heads * dim;
          const zero = new Float32Array(count),
            values = new Float32Array(count);
          const mask = new Uint32Array(batch * seq);
          const means = new Float64Array(batch * heads * dim);
          for (let b = 0; b < batch; b++) {
            let valid = 0;
            for (let s = 0; s < seq; s++) {
              mask[b * seq + s] = (s + b) % 7 !== 0 ? 1 : 0;
              valid += mask[b * seq + s];
              for (let h = 0; h < heads; h++)
                for (let d = 0; d < dim; d++) {
                  const i = ((b * seq + s) * heads + h) * dim + d;
                  values[i] = Math.sin(s * 0.19 + h + d * 0.1 + b);
                  if (mask[b * seq + s]) means[(b * heads + h) * dim + d] += values[i];
                }
            }
            for (let i = b * heads * dim; i < (b + 1) * heads * dim; i++) means[i] /= valid;
          }
          const ids = [zero, zero, values, mask].map((data) => {
            const id = gpu.createBuffer(data.byteLength);
            gpu.device.queue.writeBuffer(gpu.buffers.get(id), 0, data);
            return id;
          });
          const output = gpu.createBuffer(values.byteLength);
          try {
            for (const window of [0, 65])
              for (let repeat = 0; repeat < 3; repeat++) {
                gpu._dispatchAttention(...ids, output, batch, seq, heads, dim, window);
                const staging = gpu.device.createBuffer({
                  size: values.byteLength,
                  usage: GPUBufferUsage.COPY_DST | GPUBufferUsage.MAP_READ,
                });
                try {
                  const encoder = gpu.device.createCommandEncoder();
                  encoder.copyBufferToBuffer(
                    gpu.buffers.get(output),
                    0,
                    staging,
                    0,
                    values.byteLength
                  );
                  gpu.device.queue.submit([encoder.finish()]);
                  await staging.mapAsync(GPUMapMode.READ);
                  const actual = new Float32Array(staging.getMappedRange());
                  let maxError = 0;
                  for (let b = 0; b < batch; b++)
                    for (let s = 0; s < seq; s++)
                      for (let h = 0; h < heads; h++)
                        for (let d = 0; d < dim; d++) {
                          let expected = means[(b * heads + h) * dim + d];
                          if (window) {
                            let sum = 0,
                              count = 0;
                            for (
                              let k = Math.max(0, s - window + 1);
                              k < Math.min(seq, s + window);
                              k++
                            )
                              if (mask[b * seq + k]) {
                                sum += values[((b * seq + k) * heads + h) * dim + d];
                                count++;
                              }
                            expected = sum / count;
                          }
                          maxError = Math.max(
                            maxError,
                            Math.abs(actual[((b * seq + s) * heads + h) * dim + d] - expected)
                          );
                        }
                  results.push({ seq, window, repeat, maxError });
                  staging.unmap();
                } finally {
                  staging.destroy();
                }
              }
          } finally {
            for (const id of [...ids, output]) gpu.freeBuffer(id);
          }
        }
      } finally {
        gpu.destroy();
      }
      return results;
    });
    for (const result of cases) assert(result.maxError < 1e-5, JSON.stringify(result));
    console.log({
      attentionCases: cases.length,
      maxError: Math.max(...cases.map((c) => c.maxError)),
    });
  } finally {
    await browser.close();
    await server.close();
  }
});
