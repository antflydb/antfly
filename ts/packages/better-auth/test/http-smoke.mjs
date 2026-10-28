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
import { randomUUID } from "node:crypto";

const base = process.env.AUTH_APP_URL ?? "http://localhost:13030";
const request = (path, body, cookie) =>
  fetch(base + path, {
    method: body ? "POST" : "GET",
    headers: {
      ...(body ? { "content-type": "application/json", origin: base } : {}),
      ...(cookie ? { cookie } : {}),
    },
    ...(body ? { body: JSON.stringify(body) } : {}),
  });
assert.equal((await request("/")).status, 200);
assert.equal((await request("/api/documents")).status, 401);
const signup = async (email) => {
  const response = await request("/api/auth/sign-up/email", {
    email,
    password: "next-http-long-password",
    name: "Next",
  });
  assert.equal(response.status, 200, await response.clone().text());
  return response.headers
    .getSetCookie()
    .map((value) => value.split(";")[0])
    .join("; ");
};
const run = randomUUID();
const a = await signup(`smoke-a-${run}@example.test`);
const b = await signup(`smoke-b-${run}@example.test`);
assert.equal(
  (await request("/api/documents", { title: "mine", body: "remote needle" }, a)).status,
  200
);
assert.equal(
  (await request("/api/documents", { title: "secret", body: "needle ".repeat(20) }, b)).status,
  200
);
let hits = [];
for (let attempt = 0; attempt < 50 && !hits.length; attempt++) {
  const response = await request("/api/documents?q=needle", undefined, a);
  assert.equal(response.status, 200, await response.clone().text());
  hits = await response.json();
  if (!hits.length) await new Promise((resolve) => setTimeout(resolve, 100));
}
assert.equal(hits.length, 1);
assert.equal(hits[0].title, "mine");
assert.equal((await request("/api/auth/get-session", undefined, a)).status, 200);
assert.equal((await request("/api/auth/sign-out", {}, a)).status, 200);
assert.equal((await request("/api/documents", undefined, a)).status, 401);
console.log(
  "PASS production HTTP: protected route, signup, private create/search, session, logout"
);
