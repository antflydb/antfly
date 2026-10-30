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
    if (!url.pathname.startsWith("/mail/") && url.pathname !== "/api/me") {
      if (request.method !== "GET" || !env.MAIL_ASSETS)
        return new Response("not found", { status: 404 });
      const asset = await env.MAIL_ASSETS.fetch(request),
        headers = new Headers(asset.headers);
      headers.set(
        "Content-Security-Policy",
        "default-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'"
      );
      headers.set("X-Content-Type-Options", "nosniff");
      return new Response(asset.body, { status: asset.status, headers });
    }
    let cookies = [];
    const withCookies = (result) => {
      for (const cookie of cookies) result.headers.append("Set-Cookie", cookie);
      return result;
    };
    const reply = (body, status = 200) => withCookies(response(body, status));
    try {
      const result = await env.AUTH_SERVICE.session([...request.headers]);
      cookies = result.cookies;
      const session = result.session;
      if (!session) return reply({ error: "SignInRequired" }, 401);
      const { mailbox } = session,
        stub = actor(env, mailbox.id);
      if (request.method !== "GET" && request.headers.get("Origin") !== env.AUTH_ORIGIN)
        return reply({ error: "InvalidOrigin" }, 403);
      if (url.pathname === "/api/me" && request.method === "GET") return reply(session);
      if (url.pathname === "/mail/messages" && request.method === "GET")
        return reply(
          publicPage(await stub.list(mailbox.id, url.searchParams.get("cursor") ?? ""))
        );
      if (url.pathname === "/mail/search" && request.method === "GET")
        return reply(publicPage(await stub.search(mailbox.id, url.searchParams.get("q"))));
      const match = /^\/mail\/messages\/([a-f0-9-]{36})(?:\/attachments\/(\d{1,2}))?$/.exec(
        url.pathname
      );
      if (match && request.method === "GET") {
        if (match[2]) {
          const part = await stub.attachment(mailbox.id, match[1], Number(match[2]));
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
        const message = await stub.message(mailbox.id, match[1]);
        return message ? reply(publicMessage(message)) : reply({ error: "NotFound" }, 404);
      }
      if (url.pathname === "/mail/outbox" && request.method === "GET")
        return reply(await stub.outboundList(mailbox.id, url.searchParams.get("cursor") ?? ""));
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
          const prior = await stub.message(mailbox.id, body.reply_to);
          if (!prior) return reply({ error: "NotFound" }, 404);
          mail.to = prior.from;
          if (
            typeof prior.message_id === "string" &&
            /^<[^<>\s]+@[^<>\s]+>$/.test(prior.message_id)
          )
            mail.headers = { "In-Reply-To": prior.message_id, References: prior.message_id };
        }
        const queued = await stub.enqueue(mailbox.id, body.id, mail);
        await hint(env, mailbox.id, "send", body.id);
        return reply({ id: queued.id, state: queued.state }, 202);
      }
      const outbox = /^\/mail\/outbox\/([a-f0-9-]{36})$/.exec(url.pathname);
      if (outbox && request.method === "GET") {
        const row = await stub.outbound(mailbox.id, outbox[1]);
        return row ? reply(row) : reply({ error: "NotFound" }, 404);
      }
      return reply({ error: "NotFound" }, 404);
    } catch (error) {
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
      mailbox = await env.AUTH_SERVICE.recipient(to);
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
    const raw = await boundedBytes(message.raw),
      checksum = await digest(raw),
      key = objectPrefix(mailbox.id) + "raw/" + checksum,
      id = crypto.randomUUID();
    await env.MAIL_OBJECTS.put(key, raw, {
      onlyIf: { etagDoesNotMatch: "*" },
      httpMetadata: { contentType: "message/rfc822" },
    });
    await actor(env, mailbox.id).receive(mailbox.id, { id, sha256: checksum, key, to, from });
    await hint(env, mailbox.id, "ingest", id);
  },
  async queue(batch, env) {
    for (const message of batch.messages) {
      const body = message.body;
      if (
        !uuid(body?.mailbox) ||
        !["ingest", "send", "publish"].includes(body.kind) ||
        (body.kind !== "publish" && !uuid(body.id))
      ) {
        message.ack();
        continue;
      }
      try {
        const stub = actor(env, body.mailbox),
          result =
            body.kind === "ingest"
              ? await stub.ingest(body.mailbox, body.id)
              : body.kind === "send"
                ? await stub.dispatch(body.mailbox, body.id)
                : await stub.publish(body.mailbox);
        if (result.done === false) await hint(env, body.mailbox, "publish");
        message.ack();
      } catch {
        message.retry({ delaySeconds: 60 });
      }
    }
  },
  async scheduled(_controller, env) {
    const failures = [];
    for (const mailbox of await env.AUTH_SERVICE.mailboxes()) {
      try {
        const pending = await actor(env, mailbox).pending(mailbox);
        for (const id of pending.inbound) await hint(env, mailbox, "ingest", id);
        for (const id of pending.outbound) await hint(env, mailbox, "send", id);
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
