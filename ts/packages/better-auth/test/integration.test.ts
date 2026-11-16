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

import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { betterAuth } from "better-auth";
import { afterEach, describe, expect, it, vi } from "vitest";
import { antflyAdapter } from "../src/adapter.js";
import { createUserContent, prepareContentSchema } from "../src/content.js";
import { getDatabase } from "../src/embedded.js";
import { authDefaults, consumerAuthOptions } from "../src/options.js";
import { checkAuthSchema, prepareAuthSchema } from "../src/schema.js";
import { createTestAuth } from "../src/testing.js";
import { execute } from "../src/transport.js";

const fixtures: Awaited<ReturnType<typeof createTestAuth>>[] = [];
afterEach(async () => {
  await Promise.all(fixtures.splice(0).map((f) => f.close()));
});
async function fixture(consumer = false) {
  const f = await createTestAuth({}, consumer);
  fixtures.push(f);
  return f;
}
async function signup(
  auth: {
    api: {
      signUpEmail(input: {
        body: { name: string; email: string; password: string };
        asResponse: true;
      }): Promise<Response>;
    };
  },
  email = "a@example.test"
) {
  const response = await auth.api.signUpEmail({
    body: { name: "Example", email, password: "correct horse battery staple" },
    asResponse: true,
  });
  expect(response.status).toBe(200);
  const body = (await response.json()) as { user: { id: string } };
  const cookie = response.headers.get("set-cookie")?.split(";")[0];
  return { body, headers: new Headers(cookie ? { cookie } : {}), response };
}
describe("native auth integration", () => {
  it("hashes passwords, persists sessions across restart, and revokes logout", async () => {
    const directory = await mkdtemp(join(tmpdir(), "antfly-auth-restart-"));
    const path = join(directory, "app.aflite");
    let db = await getDatabase(path);
    expect(await getDatabase(path)).toBe(db);
    const base = {
      ...authDefaults,
      secret: "test-secret-with-more-than-thirty-two-characters",
      baseURL: "http://localhost:3000",
      trustedOrigins: ["http://localhost:3000"],
    };
    try {
      const options = { ...base, database: antflyAdapter(db) };
      await prepareAuthSchema(db, options);
      const auth = betterAuth(options);
      const { body, headers } = await signup(auth);
      const password = (await execute(db, 'SELECT password FROM "account"')).rows[0]?.password;
      expect(password).toBeTypeOf("string");
      expect(password).not.toContain("correct horse");
      expect(await auth.api.getSession({ headers })).toMatchObject({ user: { id: body.user.id } });
      await db.close();
      db = await getDatabase(path);
      await prepareAuthSchema(db, options);
      const reopened = betterAuth({ ...base, database: antflyAdapter(db) });
      expect(await reopened.api.getSession({ headers })).toMatchObject({
        user: { id: body.user.id },
      });
      await reopened.api.signOut({ headers });
      expect(await reopened.api.getSession({ headers })).toBeNull();
      await expect(
        reopened.api.signInEmail({
          body: { email: "a@example.test", password: "incorrect-password" },
        })
      ).rejects.toThrow();
      expect(
        (
          await reopened.api.signInEmail({
            body: { email: "a@example.test", password: "correct horse battery staple" },
          })
        ).user.id
      ).toBe(body.user.id);
    } finally {
      await db.close();
      await rm(directory, { recursive: true, force: true });
    }
  }, 30000);
  it("rolls back a complete transaction when a later account insert fails", async () => {
    const { db, auth } = await fixture();
    const adapter = antflyAdapter(db)(auth.options);
    await expect(
      adapter.transaction(async (tx) => {
        await tx.create({
          model: "user",
          data: {
            id: "rollback",
            name: "Rollback",
            email: "rollback@example.test",
            emailVerified: false,
            createdAt: new Date(),
            updatedAt: new Date(),
          },
          forceAllowId: true,
        });
        await tx.create({
          model: "account",
          data: {
            id: "bad",
            userId: "missing-user",
            providerId: "credential",
            accountId: "bad",
            createdAt: new Date(),
            updatedAt: new Date(),
          },
          forceAllowId: true,
        });
      })
    ).rejects.toThrow();
    expect(await adapter.count({ model: "user" })).toBe(0);
  });
  it("has one winner for concurrent unique signup and token consumption", async () => {
    const { db, auth } = await fixture();
    const results = await Promise.allSettled(
      Array.from({ length: 4 }, () =>
        auth.api.signUpEmail({
          body: { name: "Race", email: "race@example.test", password: "long-enough-password" },
        })
      )
    );
    expect(results.filter((r) => r.status === "fulfilled")).toHaveLength(1);
    const adapter = antflyAdapter(db)(auth.options);
    await adapter.create({
      model: "verification",
      data: {
        identifier: "once",
        value: "token",
        expiresAt: new Date(Date.now() + 60000),
        createdAt: new Date(),
        updatedAt: new Date(),
      },
    });
    const consumed = await Promise.all(
      Array.from({ length: 4 }, () =>
        adapter.consumeOne({
          model: "verification",
          where: [{ field: "identifier", value: "once" }],
        })
      )
    );
    expect(consumed.filter(Boolean)).toHaveLength(1);
  }, 30000);
  it("enforces owner CRUD and top-one search; fails closed on raw query shapes", async () => {
    const { db, auth } = await fixture();
    const tables = { documents: { fields: ["title", "body"] } };
    await prepareContentSchema(db, tables, auth.options);
    const requireUser = createUserContent({ auth, db, tables });
    const a = await signup(auth);
    const b = await signup(auth, "b@example.test");
    const mine = await requireUser({ headers: a.headers });
    const theirs = await requireUser({ headers: b.headers });
    const own = await mine.documents.create({ title: "own", body: "needle" });
    const other = await theirs.documents.create({ title: "secret", body: "needle ".repeat(20) });
    expect(await mine.documents.get(String(other?._id))).toBeNull();
    expect(await mine.documents.update(String(other?._id), { body: "stolen" })).toBeNull();
    expect(await mine.documents.delete(String(other?._id))).toBe(false);
    expect(await mine.documents.searchText({ query: "needle", limit: 1 })).toEqual([
      expect.objectContaining({ _id: own?._id }),
    ]);
    await expect(
      mine.documents.create({ owner_id: b.body.user.id, body: "spoof" })
    ).rejects.toThrow();
    await expect(
      mine.documents.searchText({ query: "needle", filter_query: { match_all: {} } } as never)
    ).rejects.toThrow();
    await expect(requireUser({ headers: new Headers() })).rejects.toThrow();
    expect(await checkAuthSchema(db, auth.options)).toEqual([]);
  }, 30000);
  it("blocks expired sessions and checks schema without modifying it", async () => {
    const { db, auth } = await fixture();
    const { headers } = await signup(auth);
    await execute(db, 'UPDATE "session" SET "expiresAt"=$1', [new Date(0).toISOString()]);
    expect(await auth.api.getSession({ headers })).toBeNull();
    expect(await checkAuthSchema(db, auth.options)).toEqual([]);
    const changed = {
      ...auth.options,
      user: { additionalFields: { extra: { type: "string" as const, required: false } } },
    };
    await expect(prepareAuthSchema(db, changed)).rejects.toThrow("antfly-auth apply");
    await prepareAuthSchema(db, changed, { apply: true });
    expect(await checkAuthSchema(db, changed)).toEqual([]);
  });
  it("verifies email, resets password, and deletes the account", async () => {
    const { auth, mail, db } = await fixture(true);
    await signup(auth);
    expect(mail).toHaveLength(1);
    const verifyURL = new URL(mail[0]!.text.match(/https?:\/\/\S+/)![0]);
    const verification = await auth.handler(new Request(verifyURL));
    expect([200, 302]).toContain(verification.status);
    const signed = await auth.api.signInEmail({
      body: { email: "a@example.test", password: "correct horse battery staple" },
      asResponse: true,
    });
    expect(signed.status).toBe(200);
    const headers = new Headers({ cookie: signed.headers.get("set-cookie")!.split(";")[0]! });
    await auth.api.requestPasswordReset({
      body: { email: "a@example.test", redirectTo: "http://localhost:3000/reset" },
    });
    const resetURL = new URL(mail.at(-1)!.text.match(/https?:\/\/\S+/)![0]);
    const token = resetURL.pathname.split("/").at(-1)!;
    await auth.api.resetPassword({ body: { token, newPassword: "new-correct-password" } });
    expect(await auth.api.getSession({ headers })).toBeNull();
    await expect(
      auth.api.resetPassword({ body: { token, newPassword: "replayed-password" } })
    ).rejects.toThrow();
    const login = await auth.api.signInEmail({
      body: { email: "a@example.test", password: "new-correct-password" },
      asResponse: true,
    });
    const newHeaders = new Headers({ cookie: login.headers.get("set-cookie")!.split(";")[0]! });
    await auth.api.deleteUser({ headers: newHeaders, body: { password: "new-correct-password" } });
    expect((await execute(db, 'SELECT id FROM "user"')).rows).toEqual([]);
  }, 30000);
  it("never sends verification mail after a failed commit", async () => {
    const { db, auth } = await fixture();
    const mail: unknown[] = [];
    const failing = {
      ...db,
      async connect() {
        const session = await db.connect();
        return {
          ...session,
          execute: async (statement: string, args?: readonly unknown[]) => {
            if (statement === "COMMIT") throw new Error("injected commit failure");
            return session.execute(statement, args);
          },
          close: () => session.close(),
        };
      },
    };
    const failingAuth = betterAuth({
      ...auth.options,
      ...consumerAuthOptions({
        sendMail: async (item) => {
          mail.push(item);
        },
      }),
      database: antflyAdapter(failing),
    });
    await expect(
      failingAuth.api.signUpEmail({
        body: { name: "Rollback", email: "mail@example.test", password: "correct-long-password" },
      })
    ).rejects.toThrow();
    expect(mail).toEqual([]);
    expect((await execute(db, 'SELECT id FROM "user"')).rows).toEqual([]);
  });
  it("requires confirmation of an email change and preserves the user ID", async () => {
    const { auth, mail } = await fixture(true);
    const created = await signup(auth);
    const follow = async () =>
      auth.handler(new Request(mail.at(-1)!.text.match(/https?:\/\/\S+/)![0]));
    await follow();
    const response = await auth.api.signInEmail({
      body: { email: "a@example.test", password: "correct horse battery staple" },
      asResponse: true,
    });
    const headers = new Headers({ cookie: response.headers.get("set-cookie")!.split(";")[0]! });
    await auth.api.changeEmail({
      headers,
      body: { newEmail: "changed@example.test", callbackURL: "http://localhost:3000" },
    });
    expect((await auth.api.getSession({ headers }))?.user.email).toBe("a@example.test");
    await follow();
    expect(mail.at(-1)?.to).toBe("changed@example.test");
    await follow();
    const session = await auth.api.getSession({ headers });
    expect(session?.user).toMatchObject({
      id: created.body.user.id,
      email: "changed@example.test",
      emailVerified: true,
    });
  }, 30000);
  it("persists a Google OAuth callback and rejects state replay", async () => {
    let identity = { subject: "google-subject", email: "google@example.test", verified: true };
    const f = await createTestAuth(
      {
        socialProviders: {
          google: {
            clientId: "test-client",
            clientSecret: "test-secret",
            getUserInfo: async () => ({
              user: {
                name: "Google Test",
                email: identity.email,
                emailVerified: identity.verified,
              },
              data: {
                sub: identity.subject,
                aud: "test-client",
                azp: "test-client",
                email: identity.email,
                email_verified: identity.verified,
                exp: Math.floor(Date.now() / 1000) + 3600,
                iat: Math.floor(Date.now() / 1000),
                iss: "https://accounts.google.com",
                family_name: "Test",
                given_name: "Google",
                name: "Google Test",
                picture: "https://example.test/avatar.png",
              },
            }),
          },
        },
      },
      true
    );
    fixtures.push(f);
    const original = globalThis.fetch;
    const spy = vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
      const url = input instanceof Request ? input.url : String(input);
      if (url === "https://oauth2.googleapis.com/token")
        return Response.json({
          access_token: "test-access-token",
          token_type: "Bearer",
          expires_in: 3600,
        });
      return original(input, init);
    });
    try {
      const start = await f.auth.api.signInSocial({
        body: { provider: "google", callbackURL: "http://localhost:3000" },
        asResponse: true,
      });
      const body = (await start.json()) as { url: string };
      const state = new URL(body.url).searchParams.get("state")!;
      const cookies = start.headers
        .getSetCookie()
        .map((cookie) => cookie.split(";")[0])
        .join("; ");
      const callback = `http://localhost:3000/api/auth/callback/google?code=test-code&state=${encodeURIComponent(state)}`;
      const response = await f.auth.handler(
        new Request(callback, { headers: { cookie: cookies } })
      );
      expect(response.status).toBe(302);
      expect((await execute(f.db, 'SELECT "providerId" FROM "account"')).rows).toEqual([
        { providerId: "google" },
      ]);
      expect((await execute(f.db, 'SELECT id FROM "session"')).rows).toHaveLength(1);
      await f.auth.handler(new Request(callback, { headers: { cookie: cookies } }));
      expect((await execute(f.db, 'SELECT id FROM "session"')).rows).toHaveLength(1);
      await f.auth.api.signUpEmail({
        body: { name: "Victim", email: "victim@example.test", password: "long-victim-password" },
      });
      identity = {
        subject: "different-provider-subject",
        email: "victim@example.test",
        verified: false,
      };
      const unsafeStart = await f.auth.api.signInSocial({
        body: { provider: "google", callbackURL: "http://localhost:3000" },
        asResponse: true,
      });
      const unsafeBody = (await unsafeStart.json()) as { url: string };
      const unsafeState = new URL(unsafeBody.url).searchParams.get("state")!;
      const unsafeCookies = unsafeStart.headers
        .getSetCookie()
        .map((cookie) => cookie.split(";")[0])
        .join("; ");
      await f.auth.handler(
        new Request(
          `http://localhost:3000/api/auth/callback/google?code=test-code&state=${encodeURIComponent(unsafeState)}`,
          { headers: { cookie: unsafeCookies } }
        )
      );
      expect(
        (await execute(f.db, 'SELECT id FROM "account" WHERE "providerId"=$1', ["google"])).rows
      ).toHaveLength(1);
      expect((await execute(f.db, 'SELECT id FROM "session"')).rows).toHaveLength(1);
    } finally {
      spy.mockRestore();
    }
  }, 30000);
  it("increments nullable counters atomically and preserves fractional numeric fields", async () => {
    const f = await createTestAuth({
      plugins: [
        {
          id: "counter-test",
          schema: {
            counter: {
              fields: {
                value: { type: "number", required: false },
                marker: { type: "string", required: false },
              },
            },
          },
        },
      ],
    });
    fixtures.push(f);
    const adapter = antflyAdapter(f.db)(f.auth.options);
    await adapter.create({
      model: "counter",
      data: { id: "counter", value: null, marker: "before" },
      forceAllowId: true,
    });
    const outcomes = await Promise.all(
      Array.from({ length: 4 }, () =>
        adapter.incrementOne({
          model: "counter",
          where: [
            { field: "id", value: "counter" },
            { field: "value", value: null },
          ],
          increment: { value: 1.25 },
          set: { marker: "after" },
        })
      )
    );
    expect(outcomes.filter(Boolean)).toEqual([
      expect.objectContaining({ value: 1.25, marker: "after" }),
    ]);
    expect(
      await adapter.incrementOne({
        model: "counter",
        where: [{ field: "id", value: "counter" }],
        increment: { value: -0.25 },
      })
    ).toMatchObject({ value: 1 });
  });
});
