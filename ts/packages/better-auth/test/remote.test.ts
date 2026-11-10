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

import { AntflyClient } from "@antfly/sdk";
import { betterAuth } from "better-auth";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { antflyAdapter } from "../src/adapter.js";
import { createUserContent, prepareContentSchema } from "../src/content.js";
import { type AuthMail, authDefaults, consumerAuthOptions } from "../src/options.js";
import { remoteDatabase } from "../src/remote.js";
import { checkAuthSchema, prepareAuthSchema } from "../src/schema.js";
import { execute } from "../src/transport.js";
import { createRemoteServer } from "./remote-server.js";

const binary = process.env.ANTFLY_AUTH_TEST_BINARY;
if (!binary) throw new Error("Set ANTFLY_AUTH_TEST_BINARY to a built Antfly server");
const server = await createRemoteServer(binary);
const client = new AntflyClient({ baseUrl: server.url });
const db = remoteDatabase(client);
const options = {
  ...authDefaults,
  secret: "remote-test-secret-at-least-thirty-two-characters",
  baseURL: "http://localhost:3000",
  trustedOrigins: ["http://localhost:3000"],
  rateLimit: { ...authDefaults.rateLimit, storage: "database" as const },
  database: antflyAdapter(db),
};
const auth = betterAuth(options);
const tables = { documents: { fields: ["title", "body"] } };
beforeAll(async () => {
  expect((await db.listTables()).filter((name) => name !== "default")).toEqual([]);
  await prepareAuthSchema(db, options);
  await prepareContentSchema(db, tables, options);
}, 30000);
afterAll(async () => {
  await server.close();
}, 30000);

describe("real remote auth", () => {
  it("preserves connection directory entries during concurrent requests", async () => {
    for (let round = 0; round < 3; round++) {
      await Promise.all(
        Array.from({ length: 24 }, async () => {
          const connection = await client.openSQLConnection({});
          try {
            expect(
              (
                await client.executeSQL({
                  connection_id: connection.connection_id,
                  statement: "SELECT 1",
                })
              ).rows
            ).toHaveLength(1);
          } finally {
            await client.closeSQLConnection(connection.connection_id);
          }
        })
      );
    }
  });
  it("runs signup, persisted session, owner search and revocation", async () => {
    const request = async (email: string) => {
      const response = await auth.api.signUpEmail({
        body: { name: "Remote", email, password: "long-remote-password" },
        asResponse: true,
      });
      expect(response.status).toBe(200);
      return new Headers({ cookie: response.headers.get("set-cookie")!.split(";")[0]! });
    };
    const a = await request("remote-a@example.test"),
      b = await request("remote-b@example.test");
    const requireUser = createUserContent({ auth, db, tables });
    const mine = await requireUser({ headers: a }),
      other = await requireUser({ headers: b });
    await mine.documents.create({ title: "mine", body: "remote needle" });
    await other.documents.create({ title: "secret", body: "needle ".repeat(20) });
    // Remote indexing is asynchronous; poll reads without replaying writes.
    let found: unknown[] = [];
    for (let attempt = 0; attempt < 50 && !found.length; attempt++) {
      found = await mine.documents.searchText({ query: "needle", limit: 1 });
      if (!found.length) await new Promise((resolve) => setTimeout(resolve, 100));
    }
    expect(found).toEqual([expect.objectContaining({ title: "mine" })]);
    const restarted = betterAuth({ ...options, database: antflyAdapter(remoteDatabase(client)) });
    expect(await restarted.api.getSession({ headers: a })).not.toBeNull();
    await restarted.api.signOut({ headers: a });
    expect(await auth.api.getSession({ headers: a })).toBeNull();
    expect(await checkAuthSchema(db, options)).toEqual([]);
  }, 30000);
  it("verifies email, resets passwords and deletes remote accounts", async () => {
    const mail: AuthMail[] = [];
    const flow = betterAuth({
      ...options,
      ...consumerAuthOptions({
        remote: true,
        sendMail: async (item) => {
          mail.push(item);
        },
      }),
    });
    const email = "remote-consumer@example.test";
    await flow.api.signUpEmail({
      body: { name: "Consumer", email, password: "consumer-long-password" },
    });
    expect(mail).toHaveLength(1);
    const verification = await flow.handler(new Request(mail[0]!.text.match(/https?:\/\/\S+/)![0]));
    expect([200, 302]).toContain(verification.status);
    const signed = await flow.api.signInEmail({
      body: { email, password: "consumer-long-password" },
      asResponse: true,
    });
    expect(signed.status).toBe(200);
    const headers = new Headers({
      cookie: signed.headers
        .getSetCookie()
        .map((value) => value.split(";")[0])
        .join("; "),
    });
    await flow.api.requestPasswordReset({
      body: { email, redirectTo: "http://localhost:3000/reset" },
    });
    const resetURL = new URL(mail.at(-1)!.text.match(/https?:\/\/\S+/)![0]);
    const token = resetURL.pathname.split("/").at(-1)!;
    await flow.api.resetPassword({ body: { token, newPassword: "new-consumer-password" } });
    expect(await flow.api.getSession({ headers })).toBeNull();
    await expect(
      flow.api.resetPassword({ body: { token, newPassword: "replayed-password" } })
    ).rejects.toThrow();
    const login = await flow.api.signInEmail({
      body: { email, password: "new-consumer-password" },
      asResponse: true,
    });
    expect(login.status).toBe(200);
    const newHeaders = new Headers({
      cookie: login.headers
        .getSetCookie()
        .map((value) => value.split(";")[0])
        .join("; "),
    });
    await flow.api.deleteUser({ headers: newHeaders, body: { password: "new-consumer-password" } });
    expect((await execute(db, 'SELECT id FROM "user" WHERE email=$1', [email])).rows).toEqual([]);
    expect(await flow.api.getSession({ headers: newHeaders })).toBeNull();
  }, 30000);
  it("rolls back all transaction writes and consumes a token once", async () => {
    const adapter = antflyAdapter(db)(options);
    await expect(
      adapter.transaction(async (tx) => {
        await tx.create({
          model: "verification",
          data: {
            identifier: "rolled-back",
            value: "value",
            expiresAt: new Date(Date.now() + 60000),
          },
        });
        throw new Error("abort");
      })
    ).rejects.toThrow("abort");
    expect(
      await adapter.count({
        model: "verification",
        where: [{ field: "identifier", value: "rolled-back" }],
      })
    ).toBe(0);
    await adapter.create({
      model: "verification",
      data: { identifier: "once", value: "value", expiresAt: new Date(Date.now() + 60000) },
    });
    // Competing remote writers may fail closed; only one may consume the credential.
    const outcomes = await Promise.allSettled(
      Array.from({ length: 4 }, () =>
        adapter.consumeOne({
          model: "verification",
          where: [{ field: "identifier", value: "once" }],
        })
      )
    );
    expect(outcomes.filter((result) => result.status === "fulfilled" && result.value)).toHaveLength(
      1
    );
    for (const result of outcomes)
      if (result.status === "rejected")
        expect(result.reason.body?.code).toBe("ANTFLY_AUTH_DATABASE_ERROR");
    expect(
      await adapter.count({
        model: "verification",
        where: [{ field: "identifier", value: "once" }],
      })
    ).toBe(0);
    await execute(db, 'INSERT INTO "rateLimit" (id,key,count,"lastRequest") VALUES ($1,$2,$3,$4)', [
      "rate",
      "rate",
      0,
      Date.now(),
    ]);
    const increments = await Promise.allSettled(
      Array.from({ length: 4 }, () =>
        adapter.incrementOne({
          model: "rateLimit",
          where: [
            { field: "id", value: "rate" },
            { field: "count", operator: "lt", value: 1 },
          ],
          increment: { count: 1 },
        })
      )
    );
    expect(
      increments.filter((result) => result.status === "fulfilled" && result.value)
    ).toHaveLength(1);
    for (const result of increments)
      if (result.status === "rejected")
        expect(result.reason.body?.code).toBe("ANTFLY_AUTH_DATABASE_ERROR");
    expect(
      (await execute(db, 'SELECT count FROM "rateLimit" WHERE id=$1', ["rate"])).rows[0]?.count
    ).toBe(1);
  });
});
