// Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Apache-2.0
import { createHash } from "node:crypto";
import { readdir, readFile } from "node:fs/promises";
import { join, relative } from "node:path";
export const digest = (bytes) => createHash("sha256").update(bytes).digest("hex");
export async function walk(root, extensions) {
  const result = [];
  async function visit(dir) {
    for (const entry of await readdir(dir, { withFileTypes: true })) {
      if (entry.name.startsWith(".") || ["node_modules", "zig-out"].includes(entry.name)) continue;
      const path = join(dir, entry.name);
      if (entry.isDirectory()) await visit(path);
      else if (entry.isFile() && extensions.some((ext) => path.endsWith(ext))) result.push(path);
    }
  }
  await visit(root);
  return result.sort();
}
export async function sourceFingerprint(zigRoot) {
  const paths = [];
  for (const dir of ["pkg/inference/src", "pkg/inference/build", "lib", "build_support"])
    paths.push(...(await walk(join(zigRoot, dir), [".zig", ".zon"])));
  for (const name of [
    "build.zig",
    "build.zig.zon",
    "pkg/inference/build.zig",
    "pkg/inference/build.zig.zon",
  ])
    paths.push(join(zigRoot, name));
  const hash = createHash("sha256");
  for (const path of paths.sort()) {
    hash.update(relative(zigRoot, path).replaceAll("\\", "/") + "\0");
    hash.update(await readFile(path));
  }
  return hash.digest("hex");
}
export async function verifyWasm(path, expectedAbi) {
  const bytes = await readFile(path);
  const module = new WebAssembly.Module(bytes);
  const imports = {};
  for (const item of WebAssembly.Module.imports(module)) {
    if (item.kind !== "function")
      throw new Error(`Unsupported WASM import ${item.module}/${item.name}`);
    (imports[item.module] ??= {})[item.name] = () => 0;
  }
  const instance = new WebAssembly.Instance(module, imports);
  if (instance.exports.extraction_abi_version?.() !== expectedAbi)
    throw new Error(`Extraction WASM ABI mismatch: ${path}`);
  return bytes;
}
