import { test } from "node:test";
import assert from "node:assert/strict";
import { chromium } from "@playwright/test";

test("Laya GPU FP16 matmul and resident activation kernels", { skip: process.env.EXTRACTION_GPU !== "1", timeout: 90000 }, async () => {
  const browser = await chromium.launch({ args: ["--enable-unsafe-webgpu", "--use-angle=metal"] });
  try {
    const page = await browser.newPage();
    await page.goto(process.env.PLAYGROUND_URL || "http://127.0.0.1:3102/");
    const errors = await page.evaluate(async () => {
      const { WebGPUOps } = await import("/inference/webgpu-ops.js");
      const gpu = new WebGPUOps();
      if (!await gpu.init()) throw new Error(gpu.lastInitError);
      const errors = [];
      const put = data => { const id = gpu.createBuffer(data.byteLength); gpu.device.queue.writeBuffer(gpu.buffers.get(id), 0, data); return id; };
      const check = async (id, expected, name) => {
        const staging = gpu.device.createBuffer({ size: expected.length * 4, usage: GPUBufferUsage.MAP_READ | GPUBufferUsage.COPY_DST });
        try {
          const encoder = gpu.device.createCommandEncoder();
          encoder.copyBufferToBuffer(gpu.buffers.get(id), 0, staging, 0, expected.length * 4);
          gpu.device.queue.submit([encoder.finish()]);
          await staging.mapAsync(GPUMapMode.READ);
          const actual = new Float32Array(staging.getMappedRange());
          errors.push({ name, error: Math.max(...expected.map((v, i) => Math.abs(v - actual[i]))) });
          staging.unmap();
        } finally { staging.destroy(); }
      };
      try {
        for (const [m, n, k] of [[3, 5, 7], [17, 19, 33], [1, 1, 1]]) {
          const a = Float32Array.from({ length: m * k }, (_, i) => (i % 11 - 5) / 4);
          const values = [1, -0.5, 0.25], bits = [0x3c00, 0xb800, 0x3400];
          const w = new Uint16Array(Math.ceil(n * k / 2) * 2);
          for (let i = 0; i < n * k; i++) w[i] = bits[i % 3];
          const ai = put(a), wi = put(w), out = gpu.createBuffer(m * n * 4);
          gpu._dispatchMatmul("matmulTransBF16", ai, wi, out, m, n, k);
          const expected = new Float32Array(m * n);
          for (let r = 0; r < m; r++) for (let c = 0; c < n; c++) for (let j = 0; j < k; j++) expected[r * n + c] += a[r * k + j] * values[(c * k + j) % 3];
          await check(out, expected, `fp16 ${m}x${n}x${k}`);
          for (const id of [ai, wi, out]) gpu.freeBuffer(id);
        }
        const x = Float32Array.from({ length: 16 }, (_, i) => (i - 8) / 4), input = put(x);
        for (const paired of [0, 1]) {
          const out = gpu.createBuffer(64), expected = new Float32Array(16);
          gpu._dispatchModern(input, 0, out, 16, 0, 4, 0, paired, 2, 10000);
          for (let i = 0; i < 16; i++) {
            const d = i % 4, pos = Math.floor(i / 8), pair = paired ? Math.floor(d / 2) : d % 2;
            const other = paired ? d ^ 1 : (d + 2) % 4, negative = paired ? d % 2 === 0 : d < 2;
            const angle = pos / Math.pow(10000, 2 * pair / 4);
            expected[i] = x[i] * Math.cos(angle) + x[i - d + other] * (negative ? -1 : 1) * Math.sin(angle);
          }
          await check(out, expected, `rope ${paired}`); gpu.freeBuffer(out);
        }
        const slice = gpu.createBuffer(32);
        gpu._dispatchModern(input, 0, slice, 8, 3, 2, 4, 1, 0, 0);
        await check(slice, Float32Array.from([x[1], x[2], x[5], x[6], x[9], x[10], x[13], x[14]]), "slice");
        const ids = put(new Uint32Array([3, 0])), gather = gpu.createBuffer(32);
        gpu._dispatchModern(input, ids, gather, 8, 4, 4, 0, 0, 0, 0);
        await check(gather, Float32Array.from([...x.slice(12), ...x.slice(0, 4)]), "gather");
        const geluInput = put(new Float32Array([-10, -2, -1, 0, 1, 2, 10])), gelu = gpu.createBuffer(28);
        gpu._dispatchModern(geluInput, 0, gelu, 7, 1, 0, 0, 0, 0, 0);
        await check(gelu, [-0, -0.0455002639, -0.1586552539, 0, 0.8413447461, 1.9544997361, 10], "erf GELU");
      } finally { gpu.destroy(); }
      return errors;
    });
    for (const error of errors) assert(error.error < 1e-5, JSON.stringify(error));
    console.log(errors);
  } finally { await browser.close(); }
});
