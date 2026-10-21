// Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Apache-2.0
import assert from "node:assert/strict";
import { test } from "node:test";
import { createHash } from "node:crypto";
import { downloadCatalogModel } from "../dist/model-cache.js";
import { readModelManifest } from "../dist/runtime-assets/runtime/extraction-bundle.js";
import { resolveAdapter } from "../dist/runtime-assets/runtime/model-adapters.js";
const json = (value) => new Blob([JSON.stringify(value)]);
test("catalog downloads retain restrictions without modifying pinned manifests", async (t) => {
  const previous = {
    fetch: globalThis.fetch,
    Worker: globalThis.Worker,
    location: globalThis.location,
  };
  t.after(() => Object.assign(globalThis, previous));
  globalThis.location = new URL("https://localhost/");
  const blobs = new Map();
  globalThis.fetch = async (url) => new Response(blobs.get(String(url)));
  globalThis.Worker = class {
    postMessage({ file }) {
      file.arrayBuffer().then((bytes) =>
        this.onmessage({
          data: {
            hash: createHash("sha256").update(Buffer.from(bytes)).digest("hex"),
          },
        })
      );
    }
    terminate() {}
  };
  const download = async (manifest, declarations) => {
    const file = json(manifest ?? {}),
      url = "https://models.example/file";
    blobs.set(url, file);
    return downloadCatalogModel(
      {
        ...declarations,
        files: [
          {
            path: manifest ? "model_manifest.json" : "config.json",
            url,
            size_bytes: file.size,
            sha256: createHash("sha256")
              .update(Buffer.from(await file.arrayBuffer()))
              .digest("hex"),
          },
        ],
      },
      { cache: false }
    );
  };
  const restriction = { tasks: ["extract"], capabilities: ["classification"] };
  for (const manifest of [
    undefined,
    {
      tasks: ["extract", "decide"],
      capabilities: ["extraction", "classification", "typed_decisions"],
    },
  ]) {
    const files = await download(manifest, restriction);
    const metadata = await readModelManifest(files);
    const model = resolveAdapter(
      { architecture: "boundary" },
      { model_type: "modernbert" },
      metadata
    );
    assert.deepEqual(model.execution.tasks, ["extract"]);
    assert.deepEqual(model.execution.capabilities, ["classification"]);
    assert.deepEqual(model.execution.decisionKinds, []);
    if (manifest)
      assert.deepEqual(JSON.parse(await files.get("model_manifest.json").text()), manifest);
  }
  const files = await download(
    { tasks: ["decide"], capabilities: ["typed_decisions"] },
    restriction
  );
  assert.deepEqual(await readModelManifest(files), { tasks: [], capabilities: [] });
  assert.deepEqual(
    await readModelManifest(await download(undefined, { tasks: [], capabilities: [] })),
    { tasks: [], capabilities: [] }
  );
  await assert.rejects(download(undefined, { capabilities: "classification" }), /metadata/);
});
