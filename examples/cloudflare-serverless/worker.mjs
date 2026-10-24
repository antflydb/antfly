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

import module from "./dist/antfly-serverless.wasm";
import { instantiateHost } from "../../zig/lib/objectstore/wasm/asyncify.mjs";
import { createR2Transport } from "../../zig/lib/objectstore/wasm/r2.mjs";
import { withCommitCoordinator } from "../../zig/lib/objectstore/wasm/coordinated.mjs";
import { createImmutableReadCache } from "../../zig/lib/objectstore/wasm/cache.mjs";
export { CommitCoordinator } from "./coordinator.mjs";
let lane, busy = false;
const cache = createImmutableReadCache();
class Busy extends Error {}
async function run(env, input, signal) {
  if (busy) throw new Busy("database is busy");
  busy = true;
  try {
    if (!env.ANTFLY_INCARNATION) throw new Error("database incarnation is not configured");
    if (!lane) lane = await instantiateHost(module, null, {
      onTransportError: (error, op) => console.error("Antfly object operation failed", op, String(error)),
      onLog: message => console.error("Antfly runtime", message),
    });
    const transport = cache.wrap(withCommitCoordinator(createR2Transport({ database: env.DATABASE }),
      env.COMMITS.getByName(env.ANTFLY_INCARNATION)), env.ANTFLY_INCARNATION);
    return await lane.request({ ...input, bucket: "database", prefix: "antfly", namespace: env.ANTFLY_INCARNATION }, { transport, signal });
  } catch (error) {
    if (/IntegrityMismatch/.test(String(error))) cache.clear();
    throw error;
  } finally {
    if (lane && !lane.usable()) lane = null;
    busy = false;
  }
}
async function readJson(request) {
  if (Number(request.headers.get("Content-Length") ?? 0) > 1024 * 1024) throw new Error("request too large");
  const reader = request.body?.getReader();
  if (!reader) throw new Error("body required");
  const chunks = []; let size = 0;
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      size += value.byteLength;
      if (size > 1024 * 1024) { await reader.cancel(); throw new Error("request too large"); }
      chunks.push(value);
    }
  } finally { reader.releaseLock(); }
  const input = new Uint8Array(size); let offset = 0;
  for (const chunk of chunks) { input.set(chunk, offset); offset += chunk.byteLength; }
  return JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(input));
}
async function enqueue(env) {
  if (!env.MAINTENANCE) return false;
  await env.MAINTENANCE.send({ incarnation: env.ANTFLY_INCARNATION });
  return true;
}
export default {
  async fetch(request, env) {
    const path = new URL(request.url).pathname;
    if (request.method === "GET" && path === "/" && env.ASSETS) return env.ASSETS.fetch(request);
    if (!env.API_TOKEN || request.headers.get("Authorization") !== `Bearer ${env.API_TOKEN}`)
      return new Response("unauthorized", { status: 401 });
    if (request.method !== "POST" || !["/api", "/ai"].includes(path)) return new Response("not found", { status: 404 });
    try {
      const args = await readJson(request);
      if (path === "/ai") {
        // Model and gateway selection are deployment policy. Native typed task
        // adapters remain the responsibility of #1271; this passes their model
        // input to Workers AI without assuming chat-completions semantics.
        if (!env.AI_MODEL?.startsWith("@cf/") || !env.AI_GATEWAY) return new Response("AI model and gateway are not configured", { status: 503 });
        return Response.json(await env.AI.run(env.AI_MODEL, args, { gateway: { id: env.AI_GATEWAY } }));
      }
      if (!["mutate", "lookup", "list", "search", "publish", "latest"].includes(args.op)) return new Response("unsupported operation", { status: 400 });
      const result = await run(env, args, request.signal);
      if ((args.op === "mutate" && result.committed) || (args.op === "publish" && result.has_more)) {
        try { result.maintenance_pending = !await enqueue(env); }
        catch (error) {
          // Preserve the accepted write's durable outcome if scheduling fails.
          result.maintenance_pending = true;
          console.error("Antfly maintenance enqueue failed", String(error));
        }
      }
      return Response.json(result);
    } catch (error) {
      return Response.json({ error: String(error.message ?? error) }, { status: error instanceof Busy ? 503 : 409,
        headers: error instanceof Busy ? { "Retry-After": "1" } : undefined });
    }
  },
  async queue(batch, env) {
    for (const message of batch.messages) {
      if (message.body?.incarnation !== env.ANTFLY_INCARNATION) { message.ack(); continue; }
      try {
        const result = await run(env, { op: "publish" });
        if (result.has_more && !await enqueue(env)) throw new Error("maintenance queue is not configured");
        message.ack();
      } catch (error) {
        console.error("Antfly publication deferred", String(error));
        message.retry({ delaySeconds: 10 });
      }
    }
  },
  // Recovery also covers a crash between WAL acceptance and queue admission.
  async scheduled(_controller, env) { await enqueue(env); },
};
