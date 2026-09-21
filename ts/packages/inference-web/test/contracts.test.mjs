import assert from "node:assert/strict";
import { test } from "node:test";
import { InferenceClient } from "../src/index.ts";
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
