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

import {takeRpcData} from "./rpc-data.mjs";
import { boundedBytes, digest, address, uuid, objectPrefix } from "./mail-core.mjs";
const publicMessage = (value) =>
  value
    ? {
        ...value,
        attachments: (value.attachments ?? []).map(({ index, filename, size }) => ({
          index,
          filename,
          size,
        })),
      }
    : value;
const publicPage = (value) => ({ ...value, messages: value.messages.map(publicMessage) });
const response = (body, status = 200) =>
  Response.json(body, { status, headers: { "Cache-Control": "no-store" } });
const actor = (env, id) => {
  if (!uuid(id)) throw Error("InvalidMailbox");
  return env.MAILBOXES.getByName(id);
};
const hint = (env, mailbox, kind, id) =>
  env.MAIL_QUEUE.send({ mailbox, kind, ...(id ? { id } : {}) });
// WASM compilation and module workers back the optional on-device index.
// Everything, including model downloads, stays same-origin.
export const contentSecurityPolicy =
  "default-src 'self'; script-src 'self' 'wasm-unsafe-eval'; worker-src 'self'; " +
  "connect-src 'self'; img-src 'self'; manifest-src 'self'; object-src 'none'; " +
  "base-uri 'none'; frame-ancestors 'none'; form-action 'self'";
// Content-addressed public runtime code (no user data), so no session needed.
async function localRuntime(env, pathname) {
  const object = await env.MAIL_MODELS.get("local-runtime/" + pathname.slice("/local/".length));
  if (!object) return null;
  const type = pathname.endsWith(".wasm")
    ? "application/wasm"
    : /\.m?js$/.test(pathname)
      ? "text/javascript"
      : "application/octet-stream";
  return new Response(object.body, { headers: { "Content-Type": type, "Content-Length": String(object.size) } });
}

// The on-device embedding model, published by the operator to a private R2
// bucket (see publish-local-model.mjs). It never contains user data, but the
// deployment must not become an open mirror of licensed weights. The catalog
// is served to signed-in users with short-lived grant URLs, because the
// browser model cache downloads pinned files without cookies.
const GRANT_SECONDS = 3600;
const hex = (bytes) => Array.from(new Uint8Array(bytes), (b) => b.toString(16).padStart(2, "0")).join("");
async function grantSignature(env, path, expires) {
  const key = await crypto.subtle.importKey(
    "raw",
    new TextEncoder().encode(env.LOCAL_MODEL_GRANT_SECRET),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"]
  );
  return hex(await crypto.subtle.sign("HMAC", key, new TextEncoder().encode(path + "\n" + expires)));
}
export async function modelGrant(env, path, now = Date.now()) {
  const expires = Math.floor(now / 1000) + GRANT_SECONDS;
  return expires + "." + (await grantSignature(env, path, expires));
}
async function validGrant(env, path, grant, now = Date.now()) {
  const match = /^(\d{1,12})\.([a-f0-9]{64})$/.exec(grant ?? "");
  if (!match || !env.LOCAL_MODEL_GRANT_SECRET || Number(match[1]) < now / 1000) return false;
  const expected = await grantSignature(env, path, match[1]);
  // Constant-time comparison of equal-length hex strings.
  let difference = 0;
  for (let i = 0; i < expected.length; i++) difference |= expected.charCodeAt(i) ^ match[2].charCodeAt(i);
  return difference === 0;
}
function modelPathOf(pathname) {
  const path = pathname.slice("/local/models/".length);
  return path &&
    path.length <= 512 &&
    !path.split("/").some((part) => !part || part === "." || part === "..") &&
    /^[A-Za-z0-9._/-]+$/.test(path)
    ? path
    : null;
}
async function localModel(env, url, method, signedIn) {
  const path = modelPathOf(url.pathname);
  if (method !== "GET" || !env.MAIL_MODELS || !path) return new Response("not found", { status: 404 });
  if (path === "catalog.json") {
    if (!signedIn) return Response.json({ error: "SignInRequired" }, { status: 401 });
    if (!env.LOCAL_MODEL_GRANT_SECRET)
      return Response.json({ error: "LocalModelGrantSecretMissing" }, { status: 503 });
  } else if (!signedIn && !(await validGrant(env, path, url.searchParams.get("grant"))))
    return Response.json({ error: "SignInRequired" }, { status: 401 });
  const object = await env.MAIL_MODELS.get((env.MAIL_MODEL_PREFIX ?? "local-models/") + path);
  if (!object) return new Response("not found", { status: 404 });
  const headers = {
    "Content-Type": path.endsWith(".json") ? "application/json" : "application/octet-stream",
    "X-Content-Type-Options": "nosniff",
  };
  if (path === "catalog.json") {
    const catalog = JSON.parse(await new Response(object.body).text());
    for (const file of catalog.files ?? [])
      if (typeof file.path === "string" && modelPathOf("/local/models/" + file.path))
        file.url = "/local/models/" + file.path + "?grant=" + (await modelGrant(env, file.path));
    return Response.json(catalog, { headers: { ...headers, "Cache-Control": "private, no-store" } });
  }
  // Pinned weight files are content-checked by the browser before use.
  return new Response(object.body, {
    headers: { ...headers, "Content-Length": String(object.size), "Cache-Control": "private, max-age=86400" },
  });
}
export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (request.method === "GET" && env.AUTH_ORIGIN && url.origin !== env.AUTH_ORIGIN) {
      const target = new URL(env.AUTH_ORIGIN);
      target.pathname = url.pathname;
      target.search = url.search;
      return Response.redirect(target.href, 307);
    }
    if (url.pathname.startsWith("/api/auth/")) return env.AUTH_SERVICE.fetch(request);
    const modelPath = url.pathname.startsWith("/local/models/");
    // Granted weight downloads carry no cookies; they are authorized by URL.
    if (modelPath && url.pathname !== "/local/models/catalog.json" && url.searchParams.has("grant"))
      return localModel(env, url, request.method, false);
    if (!url.pathname.startsWith("/mail/") && url.pathname !== "/api/me" && !modelPath) {
      if (request.method !== "GET" || !env.MAIL_ASSETS)
        return new Response("not found", { status: 404 });
      const runtime = /^\/local\/[a-f0-9]{16}\/[A-Za-z0-9._/-]+$/.test(url.pathname);
      let asset = await env.MAIL_ASSETS.fetch(request);
      // Runtime files over the 25 MiB asset limit live in R2 at the same path.
      if (runtime && asset.status === 404 && env.MAIL_MODELS && !url.pathname.includes(".."))
        asset = (await localRuntime(env, url.pathname)) ?? asset;
      const headers = new Headers(asset.headers);
      headers.set("Content-Security-Policy", contentSecurityPolicy);
      headers.set("X-Content-Type-Options", "nosniff");
      // The service worker must update promptly; versioned runtimes never change.
      if (url.pathname === "/sw.js" || url.pathname === "/local/config.json")
        headers.set("Cache-Control", "no-cache");
      else if (asset.ok && runtime)
        headers.set("Cache-Control", "public, max-age=31536000, immutable");
      return new Response(asset.body, { status: asset.status, headers });
    }
    let cookies = [];
    const withCookies = (result) => {
      for (const cookie of cookies) result.headers.append("Set-Cookie", cookie);
      return result;
    };
    const reply = (body, status = 200) => withCookies(response(body, status));
    try {
      const result = await takeRpcData(env.AUTH_SERVICE.session([...request.headers]));
      cookies = result.cookies;
      const session = result.session;
      if (!session) return reply({ error: "SignInRequired" }, 401);
      const { mailbox } = session,
        stub = actor(env, mailbox.id);
      if (request.method !== "GET" && request.headers.get("Origin") !== env.AUTH_ORIGIN)
        return reply({ error: "InvalidOrigin" }, 403);
      if (url.pathname === "/api/me" && request.method === "GET") return reply(session);
      if (modelPath) return withCookies(await localModel(env, url, request.method, true));
      if (url.pathname === "/mail/messages" && request.method === "GET")
        return reply(
          publicPage(await takeRpcData(stub.list(mailbox.id, url.searchParams.get("cursor") ?? "")))
        );
      if (url.pathname === "/mail/sync" && request.method === "GET")
        return reply(await takeRpcData(stub.sync(mailbox.id, url.searchParams.get("cursor") ?? "")));
      if (url.pathname === "/mail/search" && request.method === "GET")
        return reply(publicPage(await takeRpcData(stub.search(mailbox.id, url.searchParams.get("q")))));
      const match = /^\/mail\/messages\/([a-f0-9-]{36})(?:\/attachments\/(\d{1,2}))?$/.exec(
        url.pathname
      );
      if (match && !match[2] && request.method === "DELETE") {
        const result = await takeRpcData(stub.delete(mailbox.id, match[1]));
        // The purge ran inline; a hint covers a failure part-way through it.
        await hint(env, mailbox.id, "purge", match[1]);
        return reply(result);
      }
      if (match && request.method === "GET") {
        if (match[2]) {
          const part = await takeRpcData(stub.attachment(mailbox.id, match[1], Number(match[2])));
          if (!part) return reply({ error: "NotFound" }, 404);
          return withCookies(
            new Response(part.bytes, {
              headers: {
                "Content-Type": "application/octet-stream",
                "Content-Disposition":
                  "attachment; filename=attachment; filename*=UTF-8''" +
                  encodeURIComponent(part.filename),
                "X-Content-Type-Options": "nosniff",
                "Cache-Control": "private, no-store",
              },
            })
          );
        }
        const message = await takeRpcData(stub.message(mailbox.id, match[1]));
        return message ? reply(publicMessage(message)) : reply({ error: "NotFound" }, 404);
      }
      if (url.pathname === "/mail/outbox" && request.method === "GET")
        return reply(await takeRpcData(stub.outboundList(mailbox.id, url.searchParams.get("cursor") ?? "")));
      if (url.pathname === "/mail/outbox" && request.method === "POST") {
        const body = JSON.parse(
          new TextDecoder().decode(await boundedBytes(request.body, 600 * 1024))
        );
        if (
          !uuid(body.id) ||
          Object.keys(body).some((k) => !["id", "to", "subject", "text", "reply_to"].includes(k))
        )
          return reply({ error: "InvalidOutgoingMail" }, 400);
        const mail = { to: body.to, subject: body.subject, text: body.text, from: mailbox.address };
        if (body.reply_to) {
          const prior = await takeRpcData(stub.message(mailbox.id, body.reply_to));
          if (!prior) return reply({ error: "NotFound" }, 404);
          mail.to = prior.from;
          if (
            typeof prior.message_id === "string" &&
            /^<[^<>\s]+@[^<>\s]+>$/.test(prior.message_id)
          )
            mail.headers = { "In-Reply-To": prior.message_id, References: prior.message_id };
        }
        const queued = await takeRpcData(stub.enqueue(mailbox.id, body.id, mail));
        await hint(env, mailbox.id, "send", body.id);
        return reply({ id: queued.id, state: queued.state }, 202);
      }
      const outbox = /^\/mail\/outbox\/([a-f0-9-]{36})$/.exec(url.pathname);
      if (outbox && request.method === "GET") {
        const row = await takeRpcData(stub.outbound(mailbox.id, outbox[1]));
        return row ? reply(row) : reply({ error: "NotFound" }, 404);
      }
      return reply({ error: "NotFound" }, 404);
    } catch (error) {
      if (error?.message === "UnknownMessage") return reply({ error: "NotFound" }, 404);
      if (error?.message === "MailSendRateLimited")
        return reply({ error: "MailSendRateLimited" }, 429);
      const invalid =
        error instanceof SyntaxError || /^Invalid|Exceeded$/.test(error?.message ?? "");
      return reply(
        { error: invalid ? "InvalidRequest" : "MailTemporarilyUnavailable" },
        invalid ? 400 : 503
      );
    }
  },
  async email(message, env) {
    const to = address(message.to),
      from = message.from === "" ? "" : address(message.from),
      mailbox = await takeRpcData(env.AUTH_SERVICE.recipient(to));
    if (!mailbox) {
      message.setReject("Unknown recipient");
      return;
    }
    if (Number.isFinite(message.rawSize) && message.rawSize > 5 * 1024 * 1024) {
      message.setReject("Message too large");
      return;
    }
    // Each SMTP invocation receives a receipt. Content hashes deduplicate bytes,
    // never distinct deliveries; a repeated Message-ID cannot erase a message.
    // Raw MIME is stored per delivery (not by content hash) so that deleting
    // one message can remove its bytes without affecting identical deliveries.
    const raw = await boundedBytes(message.raw),
      checksum = await digest(raw),
      id = crypto.randomUUID(),
      key = objectPrefix(mailbox.id) + "raw/" + id;
    await env.MAIL_OBJECTS.put(key, raw, {
      onlyIf: { etagDoesNotMatch: "*" },
      httpMetadata: { contentType: "message/rfc822" },
    });
    await takeRpcData(actor(env, mailbox.id).receive(mailbox.id, { id, sha256: checksum, key, to, from }));
    await hint(env, mailbox.id, "ingest", id);
  },
  async queue(batch, env) {
    for (const message of batch.messages) {
      const body = message.body;
      if (
        !uuid(body?.mailbox) ||
        !["ingest", "send", "publish", "purge"].includes(body.kind) ||
        (body.kind !== "publish" && !uuid(body.id))
      ) {
        message.ack();
        continue;
      }
      try {
        const stub = actor(env, body.mailbox),
          result =
            body.kind === "ingest"
              ? await takeRpcData(stub.ingest(body.mailbox, body.id))
              : body.kind === "send"
                ? await takeRpcData(stub.dispatch(body.mailbox, body.id))
                : body.kind === "purge"
                  ? await takeRpcData(stub.purge(body.mailbox, body.id))
                  : await takeRpcData(stub.publish(body.mailbox));
        if (result.purge) await hint(env, body.mailbox, "purge", body.id);
        if (result.done === false) await hint(env, body.mailbox, "publish");
        message.ack();
      } catch {
        message.retry({ delaySeconds: 60 });
      }
    }
  },
  async scheduled(_controller, env) {
    const failures = [];
    for (const mailbox of await takeRpcData(env.AUTH_SERVICE.mailboxes())) {
      try {
        const pending = await takeRpcData(actor(env, mailbox).pending(mailbox));
        for (const id of pending.inbound) await hint(env, mailbox, "ingest", id);
        for (const id of pending.outbound) await hint(env, mailbox, "send", id);
        for (const id of pending.purge ?? []) await hint(env, mailbox, "purge", id);
        if (pending.publish) await hint(env, mailbox, "publish");
      } catch (error) {
        failures.push(error);
      }
    }
    // Visit the entire page before reporting failures. Durable pending intents
    // remain available for later sweeps; one unavailable actor cannot starve its
    // healthy neighbors, and operators still see an unsuccessful invocation.
    if (failures.length) throw new AggregateError(failures, "MailboxRecoveryFailed");
  },
};
