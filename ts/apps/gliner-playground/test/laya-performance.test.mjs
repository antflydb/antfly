import { test } from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { chromium } from "@playwright/test";
const packageUrl = `/@fs${new URL("../../../packages/inference-web/src/index.ts", import.meta.url).pathname}`;

test("Laya fixed-input warm latency and transfer accounting", { skip: !process.env.LAYA_MODEL, timeout: 300000 }, async () => {
  const browser = await chromium.launch({ args: ["--enable-unsafe-webgpu", "--use-angle=metal"] });
  try {
    const page = await browser.newPage();
    await page.goto(process.env.PLAYGROUND_URL || "http://127.0.0.1:3102/");
    await page.locator("input[webkitdirectory]").setInputFiles(process.env.LAYA_MODEL);
    const oracle = JSON.parse(await readFile(process.env.LAYA_ORACLE, "utf8"));
    const result = await page.evaluate(async ({ oracle, resident, packageUrl }) => {
      const { WebGPUOps } = await import("/inference/webgpu-ops.js");
      const handle = WebGPUOps.prototype.handleWorkerCommand;
      let stats, instance;
      WebGPUOps.prototype.handleWorkerCommand = function (msg, sab) {
        instance = this;
        if (stats) {
          if (msg.cmd === "upload" || msg.cmd === "write_buffer_at_offset") stats.uploadBytes += Number(msg.size ?? msg.sizeBytes);
          if (msg.cmd === "download") { stats.downloadBytes += Number(msg.size ?? msg.sizeBytes); stats.downloads++; }
          stats.commands++;
        }
        return handle.call(this, msg, sab);
      };
      const { InferenceClient } = await import(packageUrl);
      const files = new Map([...document.querySelector("input[webkitdirectory]").files].map(f => [f.webkitRelativePath.split("/").slice(1).join("/"), f]));
      const client = new InferenceClient();
      try {
        const info = await client.loadModel(files, { backend: "webgpu", precision: "fp16" });
        if (info.backend !== "webgpu") throw new Error(info.fallbackReason);
        const request = { schema_version: 2, model: "laya", inputs: [{ content: oracle[0].text ?? "Please search for the latest documentation about browser inference." }], schema: { classifications: [{ name: "tool", mode: "single", instruction: "Which tool is needed to handle this request?", labels: ["search", "fetch", "none"] }] } };
        const runs = [];
        for (let i = 0; i < 4; i++) {
          stats = { uploadBytes: 0, downloadBytes: 0, downloads: 0, commands: 0 };
          const result = await client.run(request);
          const ps = result.value.data[0].decisions[0].probabilities.map(p => p.probability);
          runs.push({ ms: result.elapsedMs, ...stats, gpuBytes: [...instance.buffers.values()].reduce((sum, b) => sum + b.size, 0), maxError: Math.max(...ps.map((p, j) => Math.abs(p - oracle[0].probabilities[j]))) });
          // Token and type embedding rows are gathered on CPU; bulk model
          // weights must not be uploaded again. Allow their two FP32 activations.
          if (resident && i > 0 && stats.uploadBytes > oracle[0].ids.length * 1024 * 8 + 65536) throw new Error(`Warm run uploaded ${stats.uploadBytes} bytes`);
        }
        return runs;
      } finally { client.dispose(); }
    }, { oracle, resident: process.env.LAYA_RESIDENT === "1", packageUrl });
    for (const run of result) assert(run.maxError < 5e-5, JSON.stringify(run));
    if (process.env.LAYA_RESIDENT === "1") for (const run of result.slice(1)) {
      assert.equal(run.gpuBytes, result[0].gpuBytes, "Resident allocations must plateau");
      assert(run.downloads <= 3, "Only CLS features, decision logits and action logits cross to CPU");
    }
    console.log(JSON.stringify({ runs: result, warmMedianMs: result.slice(1).map(r => r.ms).sort((a,b) => a-b)[1] }));
  } finally { await browser.close(); }
});
