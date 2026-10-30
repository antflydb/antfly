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

import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  antflyAdapter,
  authDefaults,
  createUserContent,
  getDatabase,
  prepareAuthSchema,
  prepareContentSchema,
} from "@antfly/better-auth/embedded";
import { AntflyClient } from "@antfly/sdk";
import { betterAuth } from "better-auth";

// Copy this file into a fresh npm project to test installed packages, not workspace links.
assert.equal(typeof AntflyClient.prototype.getSQLTransactionOutcome, "function");
const directory = await mkdtemp(join(tmpdir(), "antfly-installed-auth-"));
const path = join(directory, "app.aflite");
let db;
const tables = { documents: { fields: ["title", "body"] } };
async function open() {
  db = await getDatabase(path);
  const options = {
    ...authDefaults,
    secret: "installed-test-only-secret-at-least-thirty-two-characters",
    baseURL: "http://localhost:3000",
    trustedOrigins: ["http://localhost:3000"],
    database: antflyAdapter(db),
  };
  await prepareAuthSchema(db, options);
  await prepareContentSchema(db, tables, options);
  const auth = betterAuth(options);
  return { auth, requireUser: createUserContent({ auth, db, tables }) };
}
try {
  const { auth, requireUser } = await open();
  async function signup(email) {
    const response = await auth.api.signUpEmail({
      body: { name: "Installed consumer", email, password: "installed-smoke-long-password" },
      asResponse: true,
    });
    assert.equal(response.status, 200);
    return new Headers({
      cookie: response.headers
        .getSetCookie()
        .map((value) => value.split(";")[0])
        .join("; "),
    });
  }
  const a = await signup("a@example.test");
  const b = await signup("b@example.test");
  const mine = await requireUser({ headers: a });
  const other = await requireUser({ headers: b });
  const saved = await mine.documents.create({ title: "Mine", body: "installed needle" });
  await other.documents.create({ title: "Private", body: "needle ".repeat(20) });
  assert.equal((await mine.documents.searchText({ query: "needle", limit: 1 }))[0]._id, saved._id);
  assert.equal(await other.documents.get(saved._id), null);
  await db.close();
  const restarted = await open();
  assert.equal((await restarted.auth.api.getSession({ headers: a })).user.email, "a@example.test");
  await restarted.auth.api.signOut({ headers: a });
  assert.equal(await restarted.auth.api.getSession({ headers: a }), null);
  console.log("PASS installed packages: signup, private search, reopen, logout");
} finally {
  await db?.close();
  await rm(directory, { recursive: true, force: true });
}
