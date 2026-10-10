import assert from "node:assert/strict";
import { beforeEach, test } from "node:test";
import { InferenceClient, RUNTIME_COMPATIBILITY } from "../dist/index.js";

globalThis.location = new URL("http://localhost:3101/");
const assets = new URL("./fixtures/", import.meta.url).href;
const request = { schema_version: 1, model: "response-label", text: "hello" };
const bundle = () => new Map([["model", new Blob(["fixture"])]]);
const deferred = () => {
  let resolve, reject;
  const promise = new Promise((ok, fail) => {
    resolve = ok;
    reject = fail;
  });
  return { promise, resolve, reject };
};
const code = (expected) => (error) => {
  assert.equal(error.code, expected);
  return true;
};
beforeEach(() => {
  globalThis.fetch = async () => Response.json(RUNTIME_COMPATIBILITY);
  Object.defineProperty(globalThis, "navigator", { configurable: true, value: { storage: {} } });
  globalThis.crossOriginIsolated = false;
  globalThis.inferenceFixture = { runtimes: [], gpus: [], loads: [], initGpu: async () => false };
});
function gpuAvailable() {
  globalThis.crossOriginIsolated = true;
  navigator.gpu = { requestAdapter: async () => ({ limits: {} }) };
}

test("client rejects missing models, concurrent operations and use after disposal", async () => {
  const client = new InferenceClient(assets);
  await assert.rejects(client.run(request), code("MODEL_NOT_LOADED"));
  client.busy = true;
  await assert.rejects(client.loadModel([]), code("BUSY"));
  client.busy = false;
  client.dispose();
  await assert.rejects(client.run(request), code("DISPOSED"));
  assert.equal(client.state.status, "disposed");
});
test("hard cancellation releases the runtime/device once and exposes recovery", async () => {
  gpuAvailable();
  inferenceFixture.initGpu = async () => true;
  const client = new InferenceClient(assets);
  await client.loadModel(bundle());
  client.cancel();
  client.cancel();
  assert.equal(inferenceFixture.runtimes[0].destroyed, 1);
  assert.equal(inferenceFixture.gpus[0].destroyed, 1);
  assert.equal(client.model, null);
  assert.equal(client.state.status, "cancelled");
  assert.equal(client.state.error.code, "CANCELLED");
  assert.equal(client.state.recovery, "reload-on-next-run");
});
test("request errors preserve a loaded model; fatal errors tear it down", async () => {
  const client = new InferenceClient(assets);
  await client.loadModel(bundle());
  const model = client.model;
  let calls = 0;
  inferenceFixture.run = async () => {
    calls++;
    if (calls === 1) throw new Error("InvalidExtractionRequest");
    if (calls === 3) throw Object.assign(new Error("WASM trapped"), { fatal: true });
    return { schema_version: 1, entities: [] };
  };
  await assert.rejects(client.validateRequest(request), code("INVALID_REQUEST"));
  assert.equal(client.model, model);
  assert.equal(client.state.error.message, "InvalidExtractionRequest");
  assert.equal(inferenceFixture.runtimes[0].destroyed, undefined);
  assert.deepEqual((await client.run(request)).value.entities, []);
  await assert.rejects(client.run(request), code("RUNTIME_FAILED"));
  assert.equal(client.model, null);
  assert.equal(inferenceFixture.runtimes[0].destroyed, 1);
  assert.equal(client.state.recovery, "reload-on-next-run");
});
test("a failed explicit model switch cannot silently reload the old model", async () => {
  const client = new InferenceClient(assets),
    a = bundle(),
    b = bundle();
  await client.loadModel(a);
  inferenceFixture.load = async (files) => {
    if (files === b) throw new Error("Invalid model B");
  };
  await assert.rejects(client.loadModel(b), code("MODEL_LOAD_FAILED"));
  assert.equal(client.state.recovery, "none");
  assert.equal(client.files, null);
  await assert.rejects(client.run({ ...request, model: "B" }), code("MODEL_NOT_LOADED"));
  assert.deepEqual(inferenceFixture.loads, [a, b]);
  assert.equal(inferenceFixture.runtimes[0].destroyed, 1);
  assert.equal(inferenceFixture.runtimes[1].destroyed, 1);
});
test("even inspection failure or an already aborted switch clears the reload recipe", async () => {
  const client = new InferenceClient(assets);
  await client.loadModel(bundle());
  inferenceFixture.inspect = async () => {
    throw new Error("Invalid bundle");
  };
  await assert.rejects(client.loadModel(bundle()), code("MODEL_LOAD_FAILED"));
  await assert.rejects(client.run(request), code("MODEL_NOT_LOADED"));
  inferenceFixture.inspect = undefined;
  await client.loadModel(bundle());
  await assert.rejects(
    client.loadModel(bundle(), { signal: AbortSignal.abort() }),
    code("CANCELLED")
  );
  await assert.rejects(client.run(request), code("MODEL_NOT_LOADED"));
});
for (const action of ["cancel", "abort", "unload", "dispose"]) {
  test(`a device created after ${action} during GPU initialization is destroyed`, async () => {
    gpuAvailable();
    const entered = deferred(),
      finish = deferred(),
      controller = new AbortController();
    let deviceDestroyed = 0;
    inferenceFixture.initGpu = async (gpu) => {
      entered.resolve();
      await finish.promise;
      gpu.device = {
        destroy: () => {
          deviceDestroyed++;
        },
        lost: new Promise(() => {}),
        addEventListener() {},
      };
      return true;
    };
    const client = new InferenceClient(assets);
    const loading = client.loadModel(bundle(), { signal: controller.signal });
    const rejected = assert.rejects(loading, code("CANCELLED"));
    await entered.promise;
    assert.equal(client.gpu, null, "initializing GPU remains locally owned");
    if (action === "abort") controller.abort();
    else if (action === "unload") client.unloadModel();
    else client[action]();
    finish.resolve();
    await rejected;
    assert.equal(deviceDestroyed, 1);
    assert.equal(inferenceFixture.gpus[0].destroyed, 1);
    assert.equal(client.gpu, null);
    assert.equal(client.model, null);
    assert.equal(inferenceFixture.runtimes.length, 0);
    assert.equal(
      client.state.status,
      action === "dispose" ? "disposed" : action === "unload" ? "idle" : "cancelled"
    );
  });
}
test("GPU initialization exceptions release any partially created device", async () => {
  gpuAvailable();
  let destroyed = 0;
  inferenceFixture.initGpu = async (gpu) => {
    gpu.device = {
      destroy() {
        destroyed++;
      },
    };
    throw new Error("init failed");
  };
  const client = new InferenceClient(assets);
  await assert.rejects(client.loadModel(bundle()), code("MODEL_LOAD_FAILED"));
  assert.equal(destroyed, 1);
  assert.equal(client.state.error.message, "init failed");
});
test("GPU initialization returning false destroys local ownership and reports CPU fallback", async () => {
  gpuAvailable();
  inferenceFixture.initGpu = async (gpu) => {
    gpu.lastInitError = "GPU unavailable";
    return false;
  };
  const client = new InferenceClient(assets);
  const model = await client.loadModel(bundle());
  assert.equal(inferenceFixture.gpus[0].destroyed, 1);
  assert.equal(model.backend, "wasm");
  assert.equal(model.fallbackReason, "GPU unavailable");
  client.dispose();
  assert.equal(inferenceFixture.gpus[0].destroyed, 1);
});
test("device loss is observable, rejects pending inference and reloads visibly", async () => {
  gpuAvailable();
  const lost = deferred(),
    pending = deferred(),
    entered = deferred();
  inferenceFixture.initGpu = async (gpu) => {
    gpu.device = { lost: lost.promise, destroy() {}, addEventListener() {} };
    return true;
  };
  const client = new InferenceClient(assets),
    states = [],
    progress = [];
  client.subscribe((state) => states.push(state));
  await client.loadModel(bundle());
  inferenceFixture.run = async () => {
    entered.resolve();
    return pending.promise;
  };
  const running = client.run(request);
  const rejected = assert.rejects(running, code("DEVICE_LOST"));
  await entered.promise;
  lost.resolve({ message: "removed" });
  await Promise.resolve();
  assert.equal(client.state.error.code, "DEVICE_LOST");
  pending.resolve({ schema_version: 1, entities: [] });
  await rejected;
  inferenceFixture.run = undefined;
  inferenceFixture.initGpu = async () => false;
  const result = await client.run(request, { onProgress: (p) => progress.push(p.stage) });
  assert.equal(result.backend, "wasm");
  assert(states.some((s) => s.operation === "reload" && s.status === "loading"));
  assert(progress.includes("reload"));
  assert.equal(client.state.status, "ready");
  assert.equal(client.state.error, null);
});
test("failed automatic reloads retain only their own recovery configuration", async () => {
  const client = new InferenceClient(assets),
    files = bundle();
  await client.loadModel(files);
  client.cancel();
  inferenceFixture.load = async () => {
    throw new Error("retry failed");
  };
  await assert.rejects(client.run(request), code("MODEL_LOAD_FAILED"));
  assert.equal(client.state.recovery, "reload-on-next-run");
  inferenceFixture.load = undefined;
  await client.run(request);
  assert.deepEqual(inferenceFixture.loads, [files, files, files]);
});
test("snapshots, model and errors are readonly, notifications can unsubscribe", async () => {
  const client = new InferenceClient(assets),
    states = [];
  const unsubscribe = client.subscribe((state) => states.push(state));
  const initial = client.state;
  await client.loadModel(bundle());
  assert.equal(initial.status, "idle");
  assert(Object.isFrozen(client.state));
  assert(Object.isFrozen(client.model));
  assert.throws(() => {
    client.model = null;
  }, TypeError);
  client.cancel();
  assert(Object.isFrozen(client.state.error));
  assert.equal(client.state.error.message, "Inference cancelled");
  const count = states.length;
  unsubscribe();
  client.unloadModel();
  assert.equal(states.length, count);
});
test("validation has its own result and observable operation", async () => {
  const client = new InferenceClient(assets),
    states = [];
  await client.loadModel(bundle());
  client.subscribe((state) => states.push(state));
  assert.deepEqual((await client.validateRequest(request)).value, {
    valid: true,
    encoded_tokens: 4,
  });
  assert(states.some((s) => s.status === "validating" && s.operation === "validate"));
});
test("mismatched client/runtime manifests fail before allocating a GPU or worker", async () => {
  globalThis.fetch = async () => Response.json({ ...RUNTIME_COMPATIBILITY, protocolVersion: 2 });
  const client = new InferenceClient(assets);
  await assert.rejects(client.loadModel(bundle()), code("RUNTIME_INCOMPATIBLE"));
  assert.equal(inferenceFixture.runtimes.length, 0);
  assert.equal(inferenceFixture.gpus.length, 0);
  assert.equal(client.state.error.code, "RUNTIME_INCOMPATIBLE");
});

test("a stale runtime module fails even if its manifest advertises the matching version", async () => {
  const client = new InferenceClient(new URL("./fixtures/incompatible/", import.meta.url).href);
  await assert.rejects(client.loadModel(bundle()), code("RUNTIME_INCOMPATIBLE"));
  assert.equal(inferenceFixture.runtimes.length, 0);
});
test("worker compatibility errors retain their stable public error code", async () => {
  inferenceFixture.initRuntime = async () => {
    throw Object.assign(new Error("incompatible worker"), {
      code: "RUNTIME_INCOMPATIBLE",
      fatal: true,
    });
  };
  const client = new InferenceClient(assets);
  await assert.rejects(client.loadModel(bundle()), code("RUNTIME_INCOMPATIBLE"));
  assert.equal(inferenceFixture.runtimes[0].destroyed, 1);
});

test("ready notifications permit the next operation without clearing its concurrency lock", async () => {
  const client = new InferenceClient(assets),
    entered = deferred(),
    finish = deferred();
  inferenceFixture.run = async () => {
    entered.resolve();
    await finish.promise;
    return { schema_version: 1, entities: [] };
  };
  let running;
  const unsubscribe = client.subscribe((state) => {
    if (state.status === "ready" && !running) running = client.run(request);
  });
  await client.loadModel(bundle());
  await entered.promise;
  await assert.rejects(client.run(request), code("BUSY"));
  unsubscribe();
  finish.resolve();
  await running;
  assert.equal(client.state.status, "ready");
});
