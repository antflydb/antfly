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
import test from "node:test";
import { accessIdentity } from "../mail-access.mjs";

test("Access requires a valid signed user token for this application", async (t) => {
  const config = { issuer: "https://demo.cloudflareaccess.com", audience: "mail-app" };
  const algorithm = { name: "RSASSA-PKCS1-v1_5", hash: "SHA-256" };
  const keys = await crypto.subtle.generateKey({
    ...algorithm, modulusLength: 2048, publicExponent: Uint8Array.of(1, 0, 1),
  }, true, ["sign", "verify"]);
  const jwk = await crypto.subtle.exportKey("jwk", keys.publicKey);
  t.mock.method(globalThis, "fetch", async (url) => {
    assert.equal(url, config.issuer + "/cdn-cgi/access/certs");
    return Response.json({ keys: [{ ...jwk, kid: "test" }] });
  });
  const payload = {
    iss: config.issuer, aud: [config.audience], exp: Date.now() / 1000 + 60,
    sub: "user-one", email: "one@example.com", type: "app",
  };
  async function sign(overrides = {}) {
    const encode = (value) => Buffer.from(JSON.stringify(value)).toString("base64url");
    const data = encode({ alg: "RS256", kid: "test" }) + "." + encode({ ...payload, ...overrides });
    const signature = await crypto.subtle.sign(algorithm, keys.privateKey, Buffer.from(data));
    return data + "." + Buffer.from(signature).toString("base64url");
  }
  const valid = await sign();
  assert.equal((await accessIdentity(valid, config)).email, payload.email);
  assert.equal(await accessIdentity(await sign({ aud: ["other"] }), config), null);
  assert.equal(await accessIdentity(await sign({ exp: 1 }), config), null);
  assert.equal(await accessIdentity(await sign({ type: "service" }), config), null);
  assert.equal(await accessIdentity(valid.replace(/\.[^.]+$/, ".AAAA"), config), null);
});
