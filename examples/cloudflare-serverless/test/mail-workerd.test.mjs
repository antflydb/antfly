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

import { test } from "node:test";
import assert from "node:assert/strict";
import { Miniflare, convertV4MiniflareOptions } from "miniflare";
import { mkdtemp, readdir, rm } from "node:fs/promises";
import { resolve, join } from "node:path";
const cwd = resolve(import.meta.dirname, "..");
async function modules(dir, harness) {
  const names = await readdir(dir);
  return [
    { type: "ESModule", path: join(dir, "harness.mjs"), contents: harness },
    ...names
      .filter((n) => n.endsWith(".js") || n.endsWith(".wasm"))
      .map((name) => ({
        type: name.endsWith(".wasm") ? "CompiledWasm" : "ESModule",
        path: join(dir, name),
      })),
  ];
}
async function fixture(persist) {
  const vars = {
    AUTH_ORIGIN: "http://localhost:3000",
    MAIL_DOMAIN: "mail.example.test",
    AUTH_STORAGE_ID: "workerd-mail-auth-v1",
    BETTER_AUTH_SECRET: "workerd-local-disposable-auth-secret-over-32-characters",
    WORKER_VERSION: { id: "11111111-1111-4111-8111-111111111111" },
  };
  const authDir = resolve(cwd, "dist/mail-auth"),
    appDir = resolve(cwd, "dist/mail-app");
  const guard =
    "for(const name of['sql','kv'])Object.defineProperty(ctx.storage,name,{get(){throw Error('UnexpectedPlatformDatabaseAccess');}});";
  const auth = await modules(
    authDir,
    `import Service,{MailAuth as Base}from'./mail-auth-worker.js';export class TestMailAuth extends Base{constructor(ctx,env){${guard}super(ctx,env);const transaction=this.store.transaction.bind(this.store);this.store.transaction=fn=>transaction(async tx=>{let userWrite=false;const wrapped={...tx,put:(key,value)=>{if(key.startsWith('better-auth/v1/row/user/'))userWrite=true;return tx.put(key,value);}};const result=await fn(wrapped);if(this.failSignup&&userWrite){this.failSignup=false;throw Error('InjectedSignupCommitFailure');}return result;});}injectSignupFailure(){this.failSignup=true;}async ageSession(headers){const session=await this.auth.api.getSession({headers:new Headers(headers)});await(await this.auth.$context).adapter.update({model:'session',where:[{field:'id',value:session.session.id}],update:{expiresAt:new Date(Date.now()+5*86400000),updatedAt:new Date(Date.now()-2*86400000)}});}async userCount(){return(await this.auth.$context).adapter.count({model:'user'});}}export default class extends Service{failSignup(){return this.actor().injectSignupFailure();}ageSession(headers){return this.actor().ageSession(headers);}countUsers(){return this.actor().userCount();}async flush(){const actor=this.actor();for(const id of await actor.retryAuthMail())await actor.dispatch(id);}}`
  );
  const app = await modules(
    appDir,
    `import app,{Mailbox as Base}from'./mail-worker-entry.js';import{WorkerEntrypoint}from'cloudflare:workers';export class TestMailbox extends Base{constructor(ctx,env){${guard}super(ctx,env);}}export default class extends WorkerEntrypoint{fetch(request){return app.fetch(request,this.env,this.ctx);}async inject(to,from,raw){let rejected=null;await app.email({to,from,rawSize:new TextEncoder().encode(raw).length,raw:new Blob([raw]).stream(),setReject(value){rejected=value;}},this.env);return rejected??'accepted';}async duplicateQueued(){const messages=await this.env.MAIL_QUEUE.take();for(const body of messages){await this.env.MAIL_QUEUE.send(body);await this.env.MAIL_QUEUE.send(body);}return messages.length;}async flush(){const messages=await this.env.MAIL_QUEUE.take(),states=[];await app.queue({messages:messages.map(body=>({body,ack(){states.push('ack');},retry(){states.push('retry');}}))},this.env);return JSON.stringify(states);}}`
  );
  const capture = `import{WorkerEntrypoint}from'cloudflare:workers';export default class extends WorkerEntrypoint{async send(value){const id=crypto.randomUUID();await this.env.CAPTURE.put(id,JSON.stringify(value));return{messageId:id};}async take(){const objects=await this.env.CAPTURE.list({limit:1000}),values=[];for(const o of objects.objects){values.push(await(await this.env.CAPTURE.get(o.key)).json());await this.env.CAPTURE.delete(o.key);}return values;}async takeJSON(){return JSON.stringify(await this.take());}}`;
  const workers = [
    {
      name: "mail",
      modulesRoot: appDir,
      modules: app,
      compatibilityDate: "2026-10-23",
      compatibilityFlags: ["nodejs_compat"],
      bindings: vars,
      durableObjects: { MAILBOXES: { className: "TestMailbox", useSQLite: true } },
      r2Buckets: { DATABASE: "database", MAIL_OBJECTS: "objects" },
      serviceBindings: { AUTH_SERVICE: "auth", MAIL_QUEUE: "queue", EMAIL: "capture" },
    },
    {
      name: "auth",
      modulesRoot: authDir,
      modules: auth,
      compatibilityDate: "2026-10-23",
      compatibilityFlags: ["nodejs_compat"],
      bindings: vars,
      durableObjects: { AUTH: { className: "TestMailAuth", useSQLite: true } },
      r2Buckets: { AUTH_DATA: "auth" },
      serviceBindings: { AUTH_MAIL_QUEUE: "authqueue", EMAIL: "capture" },
    },
    ...["capture", "queue", "authqueue"].map((name) => ({
      name,
      modules: [
        { type: "ESModule", path: resolve(cwd, "dist/" + name + ".mjs"), contents: capture },
      ],
      compatibilityDate: "2026-10-23",
      r2Buckets: { CAPTURE: name },
    })),
  ];
  return new Miniflare(convertV4MiniflareOptions({ workers, resourcePersistencePath: persist }));
}
test("actual workerd auth and mailbox isolation, native search, attachments and restart", {
  timeout: 180000,
}, async () => {
  const persist = await mkdtemp(resolve(cwd, "dist/mail-workerd-"));
  let mf = await fixture(persist);
  const cookies = {};
  const call = async (path, body, user, ip = "127.0.0.1") => {
    const worker = await mf.getWorker("mail");
    return worker.fetch("http://localhost:3000" + path, {
      method: body ? "POST" : "GET",
      headers: {
        Origin: "http://localhost:3000",
        "CF-Connecting-IP": ip,
        ...(body ? { "Content-Type": "application/json" } : {}),
        ...(user ? { Cookie: cookies[user] } : {}),
      },
      ...(body ? { body: JSON.stringify(body) } : {}),
    });
  };
  const json = async (path, body, user) => {
    const r = await call(path, body, user),
      value = await r.json();
    assert.equal(r.status, 200, JSON.stringify(value));
    return value;
  };
  try {
    assert.equal((await call("/mail/messages")).status, 401);
    const invalidSignup = await call("/api/auth/sign-up/email", {
      email: "invalid@example.test", name: "Invalid", password: "short",
    }, null, "127.0.0.2");
    assert.equal(invalidSignup.status, 400);
    assert.equal((await invalidSignup.json()).code, "PASSWORD_TOO_SHORT");
    for (const name of ["alice", "bob"]) {
      const signup = await call("/api/auth/sign-up/email", {
        email: name + "@example.test",
        name,
        password: "correct horse battery staple",
      });
      assert.equal(signup.status, 200, await signup.text());
      await (await mf.getWorker("auth")).flush();
      const emails = JSON.parse(await (await mf.getWorker("capture")).takeJSON());
      assert.equal(emails.length, 1);
      const verify = emails[0].text.match(/https?:\/\/\S+/)[0];
      assert.ok(
        [200, 302].includes(
          (
            await (
              await mf.getWorker("mail")
            ).fetch(verify, { redirect: "manual", headers: { "CF-Connecting-IP": "127.0.0.1" } })
          ).status
        )
      );
      const login = await call("/api/auth/sign-in/email", {
        email: name + "@example.test",
        password: "correct horse battery staple",
      });
      assert.equal(login.status, 200, await login.text());
      cookies[name] = login.headers.get("set-cookie").split(";")[0];
    }
    await (await mf.getWorker("auth")).ageSession([["cookie", cookies.alice]]);
    const renewed = await call("/api/me", null, "alice");
    assert.equal(renewed.status, 200);
    assert.ok(
      renewed.headers.getSetCookie().some((cookie) =>
        cookie.startsWith(cookies.alice.split("=")[0] + "=") && /Max-Age=604800/i.test(cookie)
      )
    );
    assert.equal(Object.hasOwn(await renewed.json(), "cookies"), false);
    await (await mf.getWorker("auth")).failSignup();
    const failed = await call("/api/auth/sign-up/email", {
      email: "rollback@example.test",
      name: "Rollback",
      password: "correct horse battery staple",
    });
    assert.equal(failed.status, 500);
    assert.equal(await (await mf.getWorker("auth")).countUsers(), 2);
    assert.equal(JSON.parse(await (await mf.getWorker("capture")).takeJSON()).length, 0);
    const alice = await json("/api/me", null, "alice"),
      bob = await json("/api/me", null, "bob");
    assert.notEqual(alice.mailbox.id, bob.mailbox.id);
    const raw =
      'From: Customer <customer@example.net>\r\nTo: hidden@example.test\r\nSubject: Searchable invoice\r\nMessage-ID: <invoice@example.net>\r\nMIME-Version: 1.0\r\nContent-Type: multipart/mixed; boundary="parts"\r\n\r\n--parts\r\nContent-Type: text/plain; charset=utf-8\r\n\r\nInvoice needleword for Alice.\r\n--parts\r\nContent-Type: text/plain\r\nContent-Disposition: attachment; filename="receipt.txt"\r\n\r\nprivate attachment\r\n--parts--\r\n';
    const mail = await mf.getWorker("mail");
    assert.equal(await mail.inject(alice.mailbox.address, "customer@example.net", raw), "accepted");
    assert.equal(await mail.duplicateQueued(), 1);
    assert.equal(await mail.flush(), '["ack","ack"]');
    const listing = await json("/mail/messages", null, "alice");
    assert.equal(listing.messages.length, 1);
    const id = listing.messages[0].id;
    assert.equal(listing.messages[0].to, alice.mailbox.address);
    assert.equal((await json("/mail/messages", null, "bob")).messages.length, 0);
    assert.equal((await json("/mail/search?q=needleword", null, "alice")).messages[0].id, id);
    assert.equal((await json("/mail/search?q=needleword", null, "bob")).messages.length, 0);
    assert.equal((await call("/mail/messages/" + id, null, "bob")).status, 404);
    assert.equal((await call("/mail/messages/" + id + "/attachments/0", null, "bob")).status, 404);
    const attachment = await call("/mail/messages/" + id + "/attachments/0", null, "alice");
    assert.equal(attachment.status, 200);
    assert.equal(attachment.headers.get("content-type"), "application/octet-stream");
    assert.match(await attachment.text(), /private attachment/);
    assert.equal(await mail.inject(alice.mailbox.address, "customer@example.net", raw), "accepted");
    assert.equal(await mail.flush(), '["ack"]');
    const repeated = await json("/mail/messages", null, "alice");
    assert.equal(repeated.messages.length, 2);
    assert.notEqual(repeated.messages[0].id, repeated.messages[1].id);
    const outboxId = crypto.randomUUID(),
      sent = await call(
        "/mail/outbox",
        {
          id: outboxId,
          reply_to: id,
          to: "ignored@example.net",
          subject: "Re: Searchable invoice",
          text: "Thank you.",
        },
        "alice"
      );
    assert.equal(sent.status, 202);
    assert.equal(await mail.flush(), '["ack"]');
    const outgoing = JSON.parse(await (await mf.getWorker("capture")).takeJSON());
    assert.equal(outgoing.length, 1);
    assert.equal(outgoing[0].to, "customer@example.net");
    assert.equal(outgoing[0].from, alice.mailbox.address);
    assert.equal(outgoing[0].headers["In-Reply-To"], "<invoice@example.net>");
    assert.equal((await json("/mail/outbox/" + outboxId, null, "alice")).state, "accepted");
    assert.equal((await json("/mail/outbox", null, "alice")).messages[0].id, outboxId);
    assert.equal((await json("/mail/outbox", null, "bob")).messages.length, 0);
    assert.equal((await call("/mail/outbox/" + outboxId, null, "bob")).status, 404);
    const oversizedHeader = "From: customer@example.net\r\nX-Padding: " +
      "a".repeat(70000) + "\r\n\r\nbody";
    const excessiveParts = "MIME-Version: 1.0\r\nContent-Type: multipart/mixed; boundary=x\r\n\r\n" +
      Array.from({ length: 130 }, () => "--x\r\nContent-Type: text/plain\r\n\r\nhello\r\n").join("") +
      "--x--\r\n";
    for (const invalid of [oversizedHeader, excessiveParts])
      assert.equal(await mail.inject(alice.mailbox.address, "customer@example.net", invalid), "accepted");
    assert.equal(await mail.duplicateQueued(), 2);
    assert.equal(await mail.flush(), '["ack","ack","ack","ack"]');
    const quarantined = (await json("/mail/messages", null, "alice")).messages
      .filter((message) => message.quarantined);
    assert.equal(quarantined.length, 2);
    for (const message of quarantined) {
      assert.equal(message.subject, "Message quarantined");
      assert.deepEqual(message.attachments, []);
      assert.match(message.text, /could not be decoded within the mailbox limits/);
      assert.equal((await call("/mail/messages/" + message.id, null, "bob")).status, 404);
    }
    await mf.dispose();
    mf = await fixture(persist);
    assert.equal((await json("/api/me", null, "alice")).mailbox.id, alice.mailbox.id);
    assert.equal((await json("/mail/messages/" + id, null, "alice")).id, id);
    await json("/api/auth/sign-out", {}, "alice");
    assert.equal((await call("/mail/messages", null, "alice")).status, 401);
  } finally {
    await mf.dispose();
    await rm(persist, { recursive: true, force: true });
  }
});
