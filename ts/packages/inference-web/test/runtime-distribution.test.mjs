import assert from "node:assert/strict";
import { test } from "node:test";
import { chmod, cp, mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";
import { runInNewContext } from "node:vm";
import { RUNTIME_COMPATIBILITY } from "../dist/index.js";
import { prepareRuntime } from "../scripts/prepare-runtime.mjs";
import { digest, sourceFingerprint, verifyWasm } from "../scripts/runtime-support.mjs";
const assets = fileURLToPath(new URL("../dist/runtime-assets/", import.meta.url));
function abiWasm(version = 1) {
  const name = Buffer.from("extraction_abi_version");
  const exports = [1, name.length, ...name, 0, 0];
  return new Uint8Array([
    0,
    97,
    115,
    109,
    1,
    0,
    0,
    0,
    1,
    5,
    1,
    96,
    0,
    1,
    127,
    3,
    2,
    1,
    0,
    7,
    exports.length,
    ...exports,
    10,
    6,
    1,
    4,
    0,
    65,
    version,
    11,
  ]);
}
test("packaged runtime assets match the manifest and the client identity", async () => {
  const manifest = JSON.parse(await readFile(join(assets, "runtime-manifest.json")));
  for (const [key, value] of Object.entries(RUNTIME_COMPATIBILITY))
    assert.equal(manifest[key], value);
  for (const pin of manifest.files) {
    const bytes = await readFile(join(assets, pin.path));
    assert.equal(digest(bytes), pin.sha256, pin.path);
    assert.equal(bytes.length, pin.size_bytes, pin.path);
  }
  for (const name of [
    "inference-web.js",
    "inference-worker.js",
    "webgpu-ops.js",
    "runtime/extraction-bundle.js",
  ])
    assert(
      (await readFile(join(assets, name), "utf8")).includes(RUNTIME_COMPATIBILITY.runtimeId),
      name
    );
});
test("a mismatched worker rejects initialization before fetching WASM", async () => {
  const source = (await readFile(join(assets, "inference-worker.js"), "utf8")).replace(
    /^import .*;\n/gm,
    ""
  );
  const messages = [],
    self = { postMessage: (value) => messages.push(value) };
  runInNewContext(source, { self, TextEncoder, TextDecoder });
  await self.onmessage({ data: { type: "init", id: 7, expectedRuntimeId: "old-client" } });
  assert.equal(messages.length, 1);
  assert.equal(messages[0].type, "error");
  assert.equal(messages[0].code, "RUNTIME_INCOMPATIBLE");
  assert.equal(messages[0].fatal, true);
});
test("preparation builds both backends, checks ABI and publishes a complete immutable directory", async () => {
  const root = await mkdtemp(join(tmpdir(), "antfly-runtime-prepare-"));
  try {
    const zigRoot = join(root, "zig"),
      output = join(root, "public/inference"),
      copy = join(root, "assets");
    for (const dir of [
      "pkg/inference/src",
      "pkg/inference/build",
      "lib",
      "build_support",
      "zig-out",
    ])
      await mkdir(join(zigRoot, dir), { recursive: true });
    await writeFile(join(zigRoot, "build.zig"), "// fixture compiler input\n");
    await writeFile(join(zigRoot, "build.zig.zon"), ".{}\n");
    await writeFile(join(zigRoot, "pkg/inference/build.zig"), "// fixture inference build\n");
    await writeFile(join(zigRoot, "pkg/inference/build.zig.zon"), ".{}\n");
    await cp(assets, copy, { recursive: true });
    const manifest = JSON.parse(await readFile(join(copy, "runtime-manifest.json")));
    manifest.sourceFingerprint = await sourceFingerprint(zigRoot);
    await writeFile(join(copy, "runtime-manifest.json"), JSON.stringify(manifest));
    const compiler = join(root, "fake-zig.mjs");
    await writeFile(
      compiler,
      `#!/usr/bin/env node\nimport { appendFile, writeFile } from "node:fs/promises";\nconst args = process.argv.slice(2);\nif (args[0] === "version") { console.log("0.17.0"); process.exit(0); }\nawait appendFile("builds.jsonl", JSON.stringify(args) + "\\n");\nawait writeFile("zig-out/antfly-extraction-" + (args.includes("-Dwebgpu=true") ? "webgpu" : "cpu") + ".wasm", new Uint8Array(${JSON.stringify([...abiWasm()])}));\n`
    );
    await chmod(compiler, 0o755);
    const destination = await prepareRuntime({ zigRoot, out: output, zig: compiler, assets: copy });
    const final = JSON.parse(await readFile(join(destination, "runtime-manifest.json")));
    assert.equal(final.files.length, manifest.files.length + 2);
    for (const pin of final.files)
      assert.equal(digest(await readFile(join(destination, pin.path))), pin.sha256);
    const builds = (await readFile(join(zigRoot, "builds.jsonl"), "utf8"))
      .trim()
      .split("\n")
      .map(JSON.parse);
    assert.deepEqual(builds, [
      ["build", "inference-wasm", "-Dmetal=false", "-j1"],
      ["build", "inference-wasm", "-Dmetal=false", "-j1", "-Dwebgpu=true"],
    ]);
    await assert.rejects(
      prepareRuntime({ zigRoot, out: output, zig: compiler, assets: copy }),
      /already exists/
    );
    await writeFile(join(zigRoot, "build.zig"), "// different revision\n");
    await assert.rejects(
      prepareRuntime({ zigRoot, out: output, zig: compiler, assets: copy }),
      /do not match/
    );
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
test("preparation rejects WASM with an incompatible extraction ABI", async () => {
  const root = await mkdtemp(join(tmpdir(), "antfly-runtime-abi-"));
  try {
    const path = join(root, "wrong.wasm");
    await writeFile(path, abiWasm(2));
    await assert.rejects(verifyWasm(path, 1), /ABI mismatch/);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("the runtime rejects older workers that omit or mismatch the identity reply", async (t) => {
  const { InferenceWeb } = await import("../dist/runtime-assets/inference-web.js");
  const previousWorker = globalThis.Worker,
    previousLocation = globalThis.location;
  t.after(() => {
    globalThis.Worker = previousWorker;
    globalThis.location = previousLocation;
  });
  globalThis.location = new URL("http://localhost/");
  const workers = [];
  globalThis.Worker = class {
    constructor() {
      workers.push(this);
    }
    terminate() {
      this.terminated = true;
    }
  };
  for (const runtimeId of [undefined, "older-runtime", RUNTIME_COMPATIBILITY.runtimeId]) {
    const runtime = new InferenceWeb();
    runtime._workerCall = async () => ({ type: "init-done", runtimeId });
    const initialized = runtime.init("http://localhost/inference/antfly-extraction-cpu.wasm", {
      worker: true,
      wasmMemoryModel: "wasm32",
      expectedRuntimeId: RUNTIME_COMPATIBILITY.runtimeId,
    });
    if (runtimeId === RUNTIME_COMPATIBILITY.runtimeId) await initialized;
    else {
      await assert.rejects(
        initialized,
        (error) => error.code === "RUNTIME_INCOMPATIBLE" && error.fatal === true
      );
      assert.equal(workers.at(-1).terminated, true);
    }
    runtime.destroy();
  }
});
