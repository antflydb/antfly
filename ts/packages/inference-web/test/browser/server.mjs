// Minimal same-origin runtime harness; the product UI lives in Colony.

import { readFile } from "node:fs/promises";
import { createServer } from "node:http";
import { extname, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";
export async function startRuntimeServer() {
  const root = fileURLToPath(new URL("../../../../../", import.meta.url));
  const types = { ".js": "text/javascript", ".wasm": "application/wasm", ".wgsl": "text/plain" };
  const server = createServer(async (req, res) => {
    const headers = {
      "Cross-Origin-Opener-Policy": "same-origin",
      "Cross-Origin-Embedder-Policy": "require-corp",
    };
    try {
      const path = decodeURIComponent(new URL(req.url, "http://localhost").pathname);
      if (path === "/") {
        res.writeHead(200, { ...headers, "Content-Type": "text/html" });
        res.end("<!doctype html><title>Antfly runtime tests</title>");
        return;
      }
      const client = path.startsWith("/client/");
      if (!client && !path.startsWith("/inference/")) throw new Error("Unknown asset");
      const name = path.slice(client ? 8 : 11);
      const dir = resolve(
        root,
        client
          ? "ts/packages/inference-web/dist"
          : name.endsWith(".wasm")
            ? "zig/zig-out"
            : "ts/packages/inference-web/dist/runtime-assets"
      );
      const file = resolve(dir, name);
      if (!file.startsWith(dir + sep)) throw new Error("Invalid path");
      const bytes = await readFile(file);
      res.writeHead(200, {
        ...headers,
        "Content-Type": types[extname(file)] ?? "application/octet-stream",
      });
      res.end(bytes);
    } catch {
      res.writeHead(404, headers);
      res.end("Not found");
    }
  });
  await new Promise((done, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", done);
  });
  return {
    url: `http://127.0.0.1:${server.address().port}/`,
    close: () =>
      new Promise((done, reject) => server.close((error) => (error ? reject(error) : done()))),
  };
}
