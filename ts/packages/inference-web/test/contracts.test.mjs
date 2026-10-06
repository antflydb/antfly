import assert from "node:assert/strict";
import { test } from "node:test";
import { InferenceClient } from "../dist/index.js";

// Browser orchestration contracts that do not need a model or GPU.
globalThis.location = new URL("http://localhost:3101/");
test("client rejects missing models, concurrent operations and use after disposal", async () => {
  const client = new InferenceClient();
  await assert.rejects(client.run({ schema_version: 2, model: "missing" }), /Load a model/);
  client.busy = true;
  await assert.rejects(client.loadModel([]), /one model operation/);
  client.busy = false;
  client.dispose();
  await assert.rejects(client.run({ schema_version: 2, model: "disposed" }), /disposed/);
});
test("hard cancellation rejects all pending work and releases the device", () => {
  const client = new InferenceClient();
  let workerDestroyed = 0,
    gpuDestroyed = 0;
  client.runtime = {
    destroy() {
      workerDestroyed++;
    },
  };
  client.gpu = {
    destroy() {
      gpuDestroyed++;
    },
  };
  client.cancel();
  client.cancel();
  assert.equal(workerDestroyed, 1);
  assert.equal(gpuDestroyed, 1);
  assert.equal(client.model, null);
});

test("request errors preserve a loaded model, while fatal runtime errors tear it down", async () => {
  const client = new InferenceClient();
  const request = { schema_version: 2, model: "loaded" };
  const model = { backend: "wasm" };
  let destroyed = 0,
    calls = 0;
  client.model = model;
  client.runtime = {
    async runExtraction() {
      calls++;
      if (calls === 1) throw new Error("InvalidExtractionRequest");
      if (calls === 3) throw Object.assign(new Error("WASM trapped"), { fatal: true });
      return { value: { ok: true }, elapsedMs: 1, wasmBytes: 1024 };
    },
    destroy() {
      destroyed++;
    },
  };
  await assert.rejects(client.validateRequest(request), /InvalidExtractionRequest/);
  assert.equal(client.model, model);
  assert.equal(destroyed, 0);
  assert.deepEqual((await client.run(request)).value, { ok: true });
  await assert.rejects(client.run(request), /WASM trapped/);
  assert.equal(client.model, null);
  assert.equal(destroyed, 1);
});
