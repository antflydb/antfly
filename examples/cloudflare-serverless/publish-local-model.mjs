// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Prepare the on-device search model for Antfly Mail and (optionally) upload it.
//
//   antfly inference pull hf:google/embeddinggemma-2:bf16-safetensors-bundle-v1
//   node publish-local-model.mjs --model-dir ~/.antfly/inference/models/google/embeddinggemma-2--antfly-... \
//     --out dist/local-model [--upload --bucket antfly-mail-models]
//
// The browser embeds text only, so the media towers are removed from the
// weights. That cuts the download roughly in half and keeps the file under the
// browser catalog's 1 GiB per-file limit. The bundle's own integrity receipt
// pins the original weights, so it is replaced by the catalog's SHA-256 pins,
// which the browser verifies before loading. The weights keep their upstream
// license; you are responsible for complying with it when serving them.
import { createReadStream, createWriteStream } from "node:fs";
import { copyFile, mkdir, open, readdir, readFile, stat, writeFile } from "node:fs/promises";
import { createHash } from "node:crypto";
import { join, relative, dirname, resolve } from "node:path";
import { spawnSync } from "node:child_process";
import { pathToFileURL } from "node:url";

// Must match the tensors the browser runtime skips for embedding similarity
// (zig/pkg/inference/web/runtime/extraction-session.js).
export const MEDIA_TENSOR = /^(vision_tower|audio_tower|embed_vision|embed_audio)\./;
const SIDECAR = /\.(json|txt|model)$/;
const RECEIPT = "antfly_inference_bundle.json";

async function readHeader(path) {
  const file = await open(path, "r");
  try {
    const prefix = Buffer.alloc(8);
    await file.read(prefix, 0, 8, 0);
    const length = Number(prefix.readBigUInt64LE(0));
    if (length <= 0 || length > 16 * 1024 ** 2) throw Error(`invalid safetensors header in ${path}`);
    const bytes = Buffer.alloc(length);
    await file.read(bytes, 0, length, 8);
    return { header: JSON.parse(bytes.toString("utf8")), dataStart: 8 + length };
  } finally {
    await file.close();
  }
}

/** Copy a safetensors file without tensors whose names match `drop`. */
export async function stripSafetensors(source, target, drop = MEDIA_TENSOR) {
  const { header, dataStart } = await readHeader(source);
  const kept = Object.entries(header)
    .filter(([name]) => name !== "__metadata__" && !drop.test(name))
    .sort((a, b) => a[1].data_offsets[0] - b[1].data_offsets[0]);
  const removed = Object.keys(header).filter((name) => name !== "__metadata__" && drop.test(name));
  const next = header.__metadata__ ? { __metadata__: header.__metadata__ } : {};
  let offset = 0;
  for (const [name, tensor] of kept) {
    const [begin, end] = tensor.data_offsets;
    if (!(end >= begin)) throw Error(`invalid offsets for ${name}`);
    next[name] = { ...tensor, data_offsets: [offset, offset + (end - begin)] };
    offset += end - begin;
  }
  let json = Buffer.from(JSON.stringify(next), "utf8");
  // Pad the header with spaces so tensor data stays 8-byte aligned.
  const padded = Math.ceil(json.length / 8) * 8;
  json = Buffer.concat([json, Buffer.alloc(padded - json.length, 0x20)]);
  const prefix = Buffer.alloc(8);
  prefix.writeBigUInt64LE(BigInt(json.length));
  await mkdir(dirname(target), { recursive: true });
  const out = createWriteStream(target);
  let failure = null;
  out.on("error", (error) => (failure = error));
  const write = async (chunk) => {
    if (failure) throw failure;
    if (!out.write(chunk)) await new Promise((ok) => out.once("drain", ok));
  };
  await write(prefix);
  await write(json);
  for (const [name] of kept) {
    const [begin, end] = header[name].data_offsets;
    if (end === begin) continue;
    for await (const chunk of createReadStream(source, { start: dataStart + begin, end: dataStart + end - 1 }))
      await write(chunk);
  }
  await new Promise((ok, fail) => out.end((error) => (error ? fail(error) : ok())));
  return { kept: kept.length, removed: removed.length, bytes: 8 + json.length + offset };
}

async function sha256(path) {
  const hash = createHash("sha256");
  for await (const chunk of createReadStream(path)) hash.update(chunk);
  return hash.digest("hex");
}

async function walk(root, dir = root) {
  const files = [];
  for (const entry of await readdir(dir, { withFileTypes: true })) {
    if (entry.name.startsWith(".")) continue;
    const path = join(dir, entry.name);
    if (entry.isDirectory()) files.push(...(await walk(root, path)));
    else if (entry.isFile()) files.push(relative(root, path).replaceAll("\\", "/"));
  }
  return files.sort();
}

/** Build `out/` (stripped weights + sidecars + catalog.json); returns the catalog. */
export async function prepare({ modelDir, out, urlBase = "/local/models/", id, name }) {
  const files = await walk(modelDir);
  if (!files.includes("model.safetensors") || !files.includes("config.json"))
    throw Error("expected an EmbeddingGemma 2 SafeTensors bundle (model.safetensors + config.json)");
  const config = JSON.parse(await readFile(join(modelDir, "config.json"), "utf8"));
  if (!["embedding_gemma2", "embedding_gemma2_text"].includes(config.model_type))
    throw Error(`unsupported model_type ${config.model_type}; on-device mail search needs EmbeddingGemma 2`);
  const published = [];
  for (const path of files) {
    if (path === RECEIPT) continue;
    if (path === "model.safetensors") {
      const result = await stripSafetensors(join(modelDir, path), join(out, path));
      console.log(`model.safetensors: kept ${result.kept} text tensors, removed ${result.removed} media tensors`);
      published.push(path);
    } else if (SIDECAR.test(path)) {
      await mkdir(dirname(join(out, path)), { recursive: true });
      await copyFile(join(modelDir, path), join(out, path));
      published.push(path);
    }
  }
  const pins = [];
  for (const path of published) {
    const size = (await stat(join(out, path))).size;
    if (size > 1024 ** 3) throw Error(`${path} exceeds the browser's 1 GiB catalog file limit`);
    pins.push({ path, url: urlBase + path, sha256: await sha256(join(out, path)), size_bytes: size });
  }
  const weights = pins.find((p) => p.path === "model.safetensors");
  const catalog = {
    id: id ?? `embeddinggemma-2-text-bf16-${weights.sha256.slice(0, 12)}`,
    name: name ?? "EmbeddingGemma 2 (text, BF16)",
    family: "embedding",
    architecture: "embedding",
    tasks: ["embed", "decide"],
    capabilities: ["embedding_similarity"],
    precision: "bf16",
    files: pins,
    qualification: "pending",
    license: "Gemma Terms of Use (upstream)",
  };
  await writeFile(join(out, "catalog.json"), JSON.stringify(catalog, null, 2) + "\n");
  return catalog;
}

function upload(out, catalog, { bucket, prefix, local }) {
  // Weights first, catalog last: browsers only see a catalog whose files exist.
  for (const path of [...catalog.files.map((f) => f.path), "catalog.json"]) {
    const result = spawnSync(
      "npx",
      ["wrangler", "r2", "object", "put", `${bucket}/${prefix}${path}`, "--file", join(out, path), local ? "--local" : "--remote"],
      { stdio: "inherit" }
    );
    if (result.status !== 0) throw Error(`upload failed: ${path}`);
  }
}

if (import.meta.url === pathToFileURL(process.argv[1] ?? "").href) {
  const args = Object.fromEntries(
    process.argv.slice(2).reduce((pairs, value, i, all) => {
      if (value.startsWith("--")) pairs.push([value.slice(2), all[i + 1]?.startsWith("--") ? true : (all[i + 1] ?? true)]);
      return pairs;
    }, [])
  );
  if (!args["model-dir"]) {
    console.error("usage: node publish-local-model.mjs --model-dir <bundle> [--out dist/local-model] [--upload [--local] --bucket <r2 bucket> --prefix local-models/]");
    process.exit(2);
  }
  const out = resolve(args.out ?? "dist/local-model");
  const catalog = await prepare({ modelDir: resolve(args["model-dir"]), out });
  const total = catalog.files.reduce((sum, f) => sum + f.size_bytes, 0);
  console.log(`prepared ${catalog.files.length} files (${(total / 1024 ** 2).toFixed(0)} MiB) in ${out}`);
  if (args.upload)
    upload(out, catalog, {
      bucket: args.bucket ?? "antfly-mail-models",
      prefix: args.prefix ?? "local-models/",
      local: args.local === true,
    });
}
