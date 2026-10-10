#!/usr/bin/env node
// Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Apache-2.0
import { execFile, spawn } from "node:child_process";
import { promisify } from "node:util";
import { access, cp, mkdir, mkdtemp, readFile, rename, rm, writeFile } from "node:fs/promises";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { digest, sourceFingerprint, verifyWasm } from "./runtime-support.mjs";
async function build(zig, zigRoot, gpu) {
  const args = [
    "build",
    "inference-wasm",
    "-Dmetal=false",
    "-j1",
    ...(gpu ? ["-Dwebgpu=true"] : []),
  ];
  await new Promise((done, reject) => {
    const child = spawn(zig, args, { cwd: zigRoot, stdio: "inherit" });
    child.once("error", reject);
    child.once("exit", (code) =>
      code === 0 ? done() : reject(new Error(`Zig build failed (${code})`))
    );
  });
}
export async function prepareRuntime({
  zigRoot,
  out,
  zig = "zig",
  assets = fileURLToPath(new URL("../dist/runtime-assets/", import.meta.url)),
}) {
  const manifest = JSON.parse(await readFile(join(assets, "runtime-manifest.json"), "utf8"));
  if ((await sourceFingerprint(zigRoot)) !== manifest.sourceFingerprint)
    throw new Error(
      "Zig sources do not match this client package. Build the package from the same checkout."
    );
  for (const pin of manifest.files) {
    const bytes = await readFile(join(assets, pin.path));
    if (bytes.length !== pin.size_bytes || digest(bytes) !== pin.sha256)
      throw new Error(`Bundled runtime integrity mismatch: ${pin.path}`);
  }
  const destination = join(
    resolve(out),
    `${manifest.clientVersion}-${manifest.runtimeId.slice(0, 16)}`
  );
  try {
    await access(destination);
    throw new Error(`Versioned asset directory already exists: ${destination}`);
  } catch (error) {
    if (error.code !== "ENOENT") throw error;
  }
  const { stdout } = await promisify(execFile)(zig, ["version"]);
  if (stdout.trim() !== manifest.zigVersion)
    throw new Error(
      `This runtime requires Zig ${manifest.zigVersion}; found ${stdout.trim()}. Pass the pinned compiler with --zig.`
    );
  await build(zig, zigRoot, false);
  await build(zig, zigRoot, true);
  // Fail if the checkout changed while compilation was running.
  if ((await sourceFingerprint(zigRoot)) !== manifest.sourceFingerprint)
    throw new Error("Zig sources changed during preparation");
  const binaries = [];
  for (const backend of ["cpu", "webgpu"]) {
    const name = `antfly-extraction-${backend}.wasm`;
    binaries.push([
      name,
      await verifyWasm(join(zigRoot, "zig-out", name), manifest.extractionAbiVersion),
    ]);
  }
  await mkdir(resolve(out), { recursive: true });
  const staging = await mkdtemp(join(resolve(out), ".prepare-"));
  try {
    await cp(assets, staging, { recursive: true });
    for (const [path, bytes] of binaries) {
      await writeFile(join(staging, path), bytes);
      manifest.files.push({ path, sha256: digest(bytes), size_bytes: bytes.length });
    }
    await writeFile(
      join(staging, "runtime-manifest.json"),
      JSON.stringify(manifest, null, 2) + "\n"
    );
    await rename(staging, destination);
    console.log(
      `Prepared ${destination}\nServe this directory as an immutable asset URL and pass that URL to InferenceClient.`
    );
    return destination;
  } finally {
    await rm(staging, { recursive: true, force: true });
  }
}
if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const options = {};
  for (let i = 2; i < process.argv.length; i += 2) {
    const key = { "--zig-root": "zigRoot", "--out": "out", "--zig": "zig" }[process.argv[i]];
    if (!key || !process.argv[i + 1])
      throw new Error(
        "Usage: antfly-inference-prepare --zig-root <matching checkout/zig> --out <public/inference> [--zig <compiler>]"
      );
    options[key] = key === "zig" ? process.argv[i + 1] : resolve(process.argv[i + 1]);
  }
  if (!options.zigRoot || !options.out) throw new Error("--zig-root and --out are required");
  try {
    await prepareRuntime(options);
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}
