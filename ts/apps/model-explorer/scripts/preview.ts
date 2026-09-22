import { createReadStream, existsSync, realpathSync, statSync } from "node:fs";
import { createServer } from "node:http";
import { extname, relative, resolve, sep } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const mimeTypes: Record<string, string> = {
  ".html": "text/html; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".txt": "text/plain; charset=utf-8",
  ".svg": "image/svg+xml",
  ".ico": "image/x-icon",
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".webp": "image/webp",
  ".ttf": "font/ttf",
  ".otf": "font/otf",
  ".woff": "font/woff",
  ".woff2": "font/woff2",
  ".map": "application/json; charset=utf-8",
  ".wasm": "application/wasm",
  ".webmanifest": "application/manifest+json; charset=utf-8",
};

function inside(root: string, path: string): boolean {
  const rel = relative(root, path);
  return rel === "" || (!rel.startsWith(`..${sep}`) && rel !== ".." && !rel.startsWith(sep));
}

/** Resolve exported Next pages and assets without directory listing or SPA fallback. */
export function exportFile(root: string, pathname: string): string | undefined {
  if (pathname.includes("\0") || pathname.includes("\\") || pathname.split("/").includes(".."))
    return undefined;
  const base = resolve(root, `.${pathname}`);
  if (!inside(root, base)) return undefined;
  for (const candidate of [base, `${base}.html`, resolve(base, "index.html")]) {
    try {
      const actual = realpathSync(candidate);
      if (inside(root, actual) && statSync(actual).isFile()) return actual;
    } catch {
      // Missing candidates are normal for extensionless static routes.
    }
  }
  return undefined;
}

export function previewServer(directory: string) {
  const root = realpathSync(directory);
  return createServer((req, res) => {
    res.setHeader("X-Content-Type-Options", "nosniff");
    res.setHeader("Cache-Control", "no-cache");
    if (req.method !== "GET" && req.method !== "HEAD") {
      res.writeHead(405, { Allow: "GET, HEAD" }).end();
      return;
    }
    let pathname: string;
    try {
      pathname = decodeURIComponent(new URL(req.url ?? "/", "http://localhost").pathname);
    } catch {
      res.writeHead(400).end("Bad request");
      return;
    }
    let file = exportFile(root, pathname);
    let status = 200;
    if (!file) {
      status = 404;
      file = exportFile(root, "/404.html");
    }
    if (!file) {
      res
        .writeHead(404, { "Content-Type": "text/plain; charset=utf-8" })
        .end(req.method === "HEAD" ? undefined : "Not found");
      return;
    }
    res.writeHead(status, {
      "Content-Type": mimeTypes[extname(file)] ?? "application/octet-stream",
      "Content-Length": statSync(file).size,
    });
    if (req.method === "HEAD") res.end();
    else
      createReadStream(file)
        .on("error", () => res.destroy())
        .pipe(res);
  });
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  const directory = fileURLToPath(new URL("../out", import.meta.url));
  if (!existsSync(directory)) throw new Error("Static export missing. Run pnpm build first.");
  const port = Number(process.env.PORT ?? 3000);
  if (!Number.isInteger(port) || port < 1 || port > 65535)
    throw new Error("PORT must be an integer from 1 to 65535");
  const host = process.env.HOST ?? "127.0.0.1";
  previewServer(directory).listen(port, host, () => {
    console.log(`Model Explorer static preview: http://${host}:${port}`);
  });
}
