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

import { DurableObject, WorkerEntrypoint } from "cloudflare:workers";
import {
  createWorkersAuth,
  prepareWorkersAuth,
  consumerAuthOptions,
} from "../../ts/packages/better-auth/src/workers.ts";
import { R2CoordinatorStore } from "./r2-coordinator-store.mjs";
import { RequestPool } from "./request-pool.mjs";
import { instantiateHost } from "../../zig/lib/objectstore/wasm/asyncify.mjs";
import { actorOwner } from "./actor-owner.mjs";
import { openAuthority, coordinatorAuthorityMode } from "./coordinator-authority.mjs";
import { AuthorityAdmission } from "./authority-admission.mjs";
import module from "./dist/antfly-serverless-observe.wasm";
import { MailOutbox, digest, address, uuid } from "./mail-core.mjs";
const kernel = new RequestPool(() => instantiateHost(module, null), 1, {
  maxReservedBytes: 8 * 1024 * 1024,
  reservationBytes: 8 * 1024 * 1024,
});
export class MailAuth extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    const origin = new URL(env.AUTH_ORIGIN);
    if (
      !env.BETTER_AUTH_SECRET ||
      env.BETTER_AUTH_SECRET.length < 32 ||
      origin.origin !== env.AUTH_ORIGIN ||
      !env.AUTH_STORAGE_ID
    )
      throw Error("AuthConfigurationRequired");
    const nativeStore = new R2CoordinatorStore(module, env.AUTH_DATA, env.AUTH_STORAGE_ID, {
        kernel,
        owner: actorOwner(ctx.id.toString(), env.WORKER_VERSION?.id, env.AUTH_STORAGE_ID),
      }),
      authority = new AuthorityAdmission(() =>
        openAuthority(
          {
            ...env,
            DATABASE: env.AUTH_DATA,
            ANTFLY_INCARNATION: env.AUTH_STORAGE_ID,
            COORDINATOR_STORAGE_ID: env.AUTH_STORAGE_ID,
          },
          coordinatorAuthorityMode(env)
        )
      );
    this.store = {
      transaction: async (callback) => {
        nativeStore.configureAuthority((await authority.get()).descriptor);
        return nativeStore.transaction(callback);
      },
    };
    this.outbox = new MailOutbox(this.store, { prefix: "auth-mail/v1/" });
    const local =
      env.AUTH_DEVELOPMENT === "true" &&
      origin.protocol === "http:" &&
      ["localhost", "127.0.0.1"].includes(origin.hostname);
    if (
      origin.protocol !== "https:" &&
      !(origin.protocol === "http:" && ["localhost", "127.0.0.1"].includes(origin.hostname))
    )
      throw Error("AuthSecureOriginRequired");
    address("accounts@" + env.MAIL_DOMAIN);
    const options = {
      ...consumerAuthOptions({
        remote: true,
        sendMail: async (mail) => {
          const id = crypto.randomUUID();
          await this.outbox.enqueue(id, { ...mail, from: address("accounts@" + env.MAIL_DOMAIN) });
          await env.AUTH_MAIL_QUEUE.send({ kind: "auth-mail", id });
        },
      }),
      secret: env.BETTER_AUTH_SECRET,
      baseURL: env.AUTH_ORIGIN,
      trustedOrigins: [env.AUTH_ORIGIN],
      advanced: {
        defaultCookieAttributes: { httpOnly: true, sameSite: "lax" },
        ipAddress: { ipAddressHeaders: ["cf-connecting-ip"] },
      },
    };
    this.local = local;
    if (local) {
      options.emailAndPassword.requireEmailVerification = false;
    }
    options.user.deleteUser.enabled = false;
    this.auth = createWorkersAuth(this.store, options);
    this.authOptions = options;
    this.ready = null;
  }
  async ensureReady() {
    this.ready ??= prepareWorkersAuth(this.store, this.authOptions);
    try {
      await this.ready;
    } catch (error) {
      this.ready = null;
      throw error;
    }
  }
  async fetch(request) {
    await this.ensureReady();
    return this.auth.handler(request);
  }
  async session(headerEntries) {
    await this.ensureReady();
    const { response: session, headers } = await this.auth.api.getSession({
      headers: new Headers(headerEntries),
      returnHeaders: true,
    });
    const cookies = headers.getSetCookie();
    if (!session || (!session.user.emailVerified && !this.local))
      return { session: null, cookies };
    const user = { id: session.user.id, name: session.user.name, email: session.user.email },
      key = "mail-membership/v1/" + (await digest(user.id)),
      domain = new URL("https://" + this.env.MAIL_DOMAIN).hostname;
    const mailbox = await this.store.transaction(async (tx) => {
      let mailbox = await tx.get(key);
      if (!mailbox) {
        const id = crypto.randomUUID();
        mailbox = { id, address: address("u-" + id + "@" + domain) };
        await tx.put(key, mailbox);
        await tx.put("mail-alias/v1/" + mailbox.address, mailbox);
        await tx.put("mail-directory/v1/" + id, id);
      }
      return mailbox;
    });
    return { session: { user, mailbox }, cookies };
  }
  async recipient(to) {
    await this.ensureReady();
    return this.store.transaction((tx) => tx.get("mail-alias/v1/" + address(to)));
  }
  async mailboxes() {
    await this.ensureReady();
    return this.store.transaction(async (tx) => {
      const cursor = (await tx.get("mail-directory-cursor/v1")) ?? "",
        page = await tx.list({ prefix: "mail-directory/v1/", startAfter: cursor, limit: 16 });
      await tx.put("mail-directory-cursor/v1", page.size === 16 ? [...page.keys()].at(-1) : "");
      return [...page.values()];
    });
  }
  async dispatch(id) {
    await this.ensureReady();
    return this.outbox.dispatch(id, (mail) => this.env.EMAIL.send(mail));
  }
  async retryAuthMail() {
    await this.ensureReady();
    return this.outbox.pending();
  }
}
export default class AuthService extends WorkerEntrypoint {
  actor() {
    return this.env.AUTH.getByName(this.env.AUTH_STORAGE_ID);
  }
  fetch(request) {
    return this.actor().fetch(request);
  }
  session(headers) {
    return this.actor().session(headers);
  }
  recipient(to) {
    return this.actor().recipient(to);
  }
  mailboxes() {
    return this.actor().mailboxes();
  }
  async queue(batch) {
    for (const message of batch.messages) {
      if (message.body?.kind !== "auth-mail" || !uuid(message.body.id)) {
        message.ack();
        continue;
      }
      try {
        await this.actor().dispatch(message.body.id);
        message.ack();
      } catch {
        message.retry({ delaySeconds: 60 });
      }
    }
  }
  async scheduled() {
    for (const id of await this.actor().retryAuthMail())
      await this.env.AUTH_MAIL_QUEUE.send({ kind: "auth-mail", id });
  }
}
