// @vitest-environment node
import http from "node:http";
import { fileURLToPath } from "node:url";
import { createServer, type ViteDevServer } from "vite";
import { expect, it, vi } from "vitest";

it("preserves browser origin and host for local personal connections", async () => {
  const backend = http.createServer((request, response) => {
    response.setHeader("Content-Type", "application/json");
    response.end(JSON.stringify({ host: request.headers.host, origin: request.headers.origin }));
  });
  let vite: ViteDevServer | undefined;
  await new Promise<void>((resolve) => backend.listen(0, "127.0.0.1", resolve));
  try {
    const address = backend.address();
    if (!address || typeof address === "string") throw new Error("Missing backend address");
    vi.stubEnv("ANTFARM_API_PROXY_TARGET", `http://127.0.0.1:${address.port}`);
    const root = fileURLToPath(new URL("../../", import.meta.url));
    vite = await createServer({
      root,
      configFile: `${root}/vite.config.ts`,
      server: { host: "127.0.0.1", port: 0 },
      optimizeDeps: { noDiscovery: true, include: [] },
      logLevel: "silent",
    });
    await vite.listen();
    const frontend = vite.httpServer?.address();
    if (!frontend || typeof frontend === "string") throw new Error("Missing frontend address");
    const origin = `http://127.0.0.1:${frontend.port}`;
    for (const [method, path] of [
      ["GET", "/db/v1/connections/chatgpt/accounts"],
      ["POST", "/db/v1/connections/chatgpt/authorize"],
      ["POST", "/db/v1/connections/one/chatgpt/disconnect"],
      ["POST", "/db/v1/retrieval"],
    ]) {
      const response = await fetch(`${origin}${path}`, {
        method,
        headers: { Origin: origin, "Sec-Fetch-Site": "same-origin" },
      });
      expect(await response.json()).toEqual({ host: `127.0.0.1:${frontend.port}`, origin });
    }
    // A hostile origin must remain visible to the backend's origin check.
    const response = await fetch(`${origin}/db/v1/connections/chatgpt/authorize`, {
      method: "POST",
      headers: { Origin: "https://untrusted.example", "Sec-Fetch-Site": "cross-site" },
    });
    expect(await response.json()).toEqual({
      host: `127.0.0.1:${frontend.port}`,
      origin: "https://untrusted.example",
    });
  } finally {
    if (vite) await vite.close();
    await new Promise<void>((resolve, reject) =>
      backend.close((error) => (error ? reject(error) : resolve()))
    );
    vi.unstubAllEnvs();
  }
}, 20_000);
