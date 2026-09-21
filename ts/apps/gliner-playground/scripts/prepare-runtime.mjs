// Copies the authoritative runtime; do not maintain a second browser engine.
import { cp, mkdir, stat } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { resolve } from "node:path";
const app = fileURLToPath(new URL("..", import.meta.url));
const zig = resolve(app, "../../../zig");
const destination = resolve(app, "public/inference");
await mkdir(destination, { recursive: true });
for (const item of [
  "inference-web.js",
  "inference-worker.js",
  "webgpu-ops.js",
  "runtime",
  "shaders",
]) {
  await cp(resolve(zig, "pkg/inference/web", item), resolve(destination, item), {
    recursive: true,
  });
}
for (const backend of ["cpu", "webgpu"]) {
  const file = `antfly-extraction-${backend}.wasm`;
  const source = resolve(zig, "zig-out", file);
  try {
    await stat(source);
  } catch {
    throw new Error(
      `Missing ${file}. From zig/: zig build inference-wasm -j1${backend === "webgpu" ? " -Dwebgpu=true" : ""}`
    );
  }
  await cp(source, resolve(destination, file));
}
