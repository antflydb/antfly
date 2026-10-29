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

import { betterAuth } from "better-auth";
import { APIError } from "better-auth/api";
import { describe, expect, it } from "vitest";
import {
  authDefaults,
  consumerAuthOptions,
  prepareWorkersAuth,
  workersAdapter,
} from "../src/workers.js";
import { WorkersAuthRows } from "../src/workers-store.js";
import { MemoryAuthStore } from "./workers-fixture.js";

const base = {
  ...authDefaults,
  secret: "local-test-secret-with-over-thirty-two-characters",
  baseURL: "http://localhost:3000",
  trustedOrigins: ["http://localhost:3000"],
  rateLimit: { enabled: true, storage: "database" as const },
};
async function fixture(consumer = false) {
  const store = new MemoryAuthStore(),
    mail: { to: string; text: string }[] = [],
    options = {
      ...base,
      ...(consumer
        ? consumerAuthOptions({
            remote: true,
            sendMail: async (item) => {
              mail.push(item);
            },
          })
        : {}),
      database: workersAdapter(store),
    };
  await prepareWorkersAuth(store, options);
  return { store, options, auth: betterAuth(options), mail };
}
const signup = (
  auth: {
    api: {
      signUpEmail(input: {
        body: { name: string; email: string; password: string };
        asResponse: true;
      }): Promise<Response>;
    };
  },
  email = "alice@example.test"
) =>
  auth.api.signUpEmail({
    body: { name: "Alice", email, password: "correct horse battery staple" },
    asResponse: true,
  });
const headers = (r: Response) =>
  new Headers({ cookie: r.headers.get("set-cookie")?.split(";")[0] ?? "" });
function mailLink(mail: { text: string } | undefined): string {
  const url = mail?.text.match(/https?:\/\/\S+/)?.[0];
  if (!url) throw Error("Missing fixture email link");
  return url;
}
describe("Workers structured Antfly auth adapter", () => {
  it("uses real password hashing, indexed sessions, restart and logout", async () => {
    const f = await fixture(),
      r = await signup(f.auth);
    expect(r.status).toBe(200);
    const user = ((await r.json()) as { user: { id: string } }).user,
      request = headers(r);
    expect(await f.auth.api.getSession({ headers: request })).toMatchObject({
      user: { id: user.id },
    });
    const accounts = await f.store.transaction((tx) =>
      new WorkersAuthRows(f.options).find(tx, "account")
    );
    expect(accounts[0]?.password).toBeTypeOf("string");
    expect(accounts[0]?.password).not.toContain("correct horse");
    const lists = f.store.lists;
    const reopened = betterAuth({ ...f.options, database: workersAdapter(f.store) });
    expect(await reopened.api.getSession({ headers: request })).toMatchObject({
      user: { id: user.id },
    });
    expect(f.store.lists - lists).toBeLessThan(10);
    await reopened.api.signOut({ headers: request });
    expect(await reopened.api.getSession({ headers: request })).toBeNull();
  });
  it("guards unique identities atomically and rolls back transaction callbacks without replay", async () => {
    const f = await fixture();
    await Promise.all([signup(f.auth), signup(f.auth)]);
    const rows = new WorkersAuthRows(f.options);
    expect(await f.store.transaction((tx) => rows.find(tx, "user"))).toHaveLength(1);
    expect(await f.store.transaction((tx) => rows.find(tx, "account"))).toHaveLength(1);
    const before = structuredClone(f.store.data),
      db = workersAdapter(f.store)(f.options);
    let callbacks = 0;
    await expect(
      db.transaction(async (tx) => {
        callbacks++;
        await tx.deleteMany({ model: "account", where: [] });
        throw Error("rollback fixture");
      })
    ).rejects.toThrow();
    expect(callbacks).toBe(1);
    expect(f.store.data).toEqual(before);
  });
  it("preserves signup validation errors without writing an account", async () => {
    const f = await fixture();
    const response = await f.auth.api.signUpEmail({
      body: { name: "Alice", email: "alice@example.test", password: "short" },
      asResponse: true,
    });
    expect(response.status).toBe(400);
    expect(await response.json()).toMatchObject({ code: "PASSWORD_TOO_SHORT" });
    expect(
      await f.store.transaction((tx) => new WorkersAuthRows(f.options).find(tx, "user"))
    ).toEqual([]);
  });
  it("preserves application errors after rollback without replaying the callback", async () => {
    const f = await fixture();
    await signup(f.auth);
    const before = structuredClone(f.store.data);
    const db = workersAdapter(f.store)(f.options);
    const error = new APIError("BAD_REQUEST", { message: "Rejected by application" });
    let calls = 0;
    await expect(
      db.transaction(async (tx) => {
        calls++;
        await tx.deleteMany({ model: "account", where: [] });
        throw error;
      })
    ).rejects.toBe(error);
    expect(calls).toBe(1);
    expect(f.store.data).toEqual(before);
  });
  it("reports a rollback failure as storage uncertainty instead of an application error", async () => {
    const f = await fixture();
    const transaction = f.store.transaction.bind(f.store);
    f.store.transaction = async (fn) => {
      try {
        return await transaction(fn);
      } catch {
        throw Error("Injected rollback failure");
      }
    };
    const db = workersAdapter(f.store)(f.options);
    await expect(
      db.transaction(async () => {
        throw new APIError("BAD_REQUEST", { message: "Rejected by application" });
      })
    ).rejects.toMatchObject({
      status: "INTERNAL_SERVER_ERROR",
      body: { code: "ANTFLY_AUTH_DATABASE_ERROR" },
    });
  });
  it("does not release queued verification email after failed commit", async () => {
    const f = await fixture(true);
    f.store.failCommit = true;
    expect((await signup(f.auth)).status).toBe(500);
    expect(f.mail).toEqual([]);
    expect(
      await f.store.transaction((tx) => new WorkersAuthRows(f.options).find(tx, "user"))
    ).toEqual([]);
  });
  it("supports verified signup and recovery with one-time guarded tokens", async () => {
    const f = await fixture(true);
    expect((await signup(f.auth)).status).toBe(200);
    expect(f.mail).toHaveLength(1);
    const url = mailLink(f.mail[0]);
    await f.auth.handler(new Request(url));
    const signin = await f.auth.api.signInEmail({
      body: { email: "alice@example.test", password: "correct horse battery staple" },
      asResponse: true,
    });
    expect(signin.status).toBe(200);
    expect((await f.auth.api.getSession({ headers: headers(signin) }))?.user.emailVerified).toBe(
      true
    );
    await f.auth.api.requestPasswordReset({
      body: { email: "alice@example.test", redirectTo: "http://localhost:3000/reset" },
    });
    const reset = mailLink(f.mail.at(-1)),
      token = new URL(reset).pathname.split("/").at(-1) ?? "";
    await f.auth.api.resetPassword({
      body: { token, newPassword: "a different correct long password" },
    });
    await expect(
      f.auth.api.resetPassword({ body: { token, newPassword: "yet another correct password" } })
    ).rejects.toThrow();
    expect(await f.auth.api.getSession({ headers: headers(signin) })).toBeNull();
  });
  it("fails closed on schema changes, missing schema and unknown commit outcomes", async () => {
    const f = await fixture(),
      db = workersAdapter(f.store)(f.options);
    f.store.failCommit = true;
    await expect(db.findMany({ model: "user" })).rejects.toThrow("Authentication storage");
    await expect(
      prepareWorkersAuth(f.store, {
        ...f.options,
        user: { additionalFields: { role: { type: "string" } } },
      })
    ).rejects.toThrow("schema changed");
    await expect(
      workersAdapter(new MemoryAuthStore())(f.options).findMany({ model: "user" })
    ).rejects.toThrow("Authentication storage");
  });
});
