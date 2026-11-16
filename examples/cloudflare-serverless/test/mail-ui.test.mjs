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
import { readFile } from "node:fs/promises";
import { JSDOM } from "jsdom";
const html = await readFile(new URL("../mail-public/index.html", import.meta.url), "utf8"),
  script = await readFile(new URL("../mail-public/mail.js", import.meta.url), "utf8");
async function fixture(path = "/") {
  const dom = new JSDOM(html, {
      url: "https://mail.example.test" + path,
      runScripts: "outside-only",
    }),
    { window } = dom,
    calls = [];
  let session = false;
  const mailbox = {
      user: { id: "alice", email: "alice@example.test" },
      mailbox: { id: crypto.randomUUID(), address: "support@example.test" },
    },
    message = {
      id: crypto.randomUUID(),
      subject: "<img src=x onerror=alert(1)>",
      from: "customer@example.net",
      to: "support@example.test",
      text: "<script>alert(1)</script>",
      received_at: 1234,
      attachments: [{ index: 0, filename: "<b>file.txt</b>", size: 3 }],
    };
  window.fetch = async (url, options = {}) => {
    const body = options.body ? JSON.parse(options.body) : null;
    calls.push({ url, body });
    if (url === "/api/me")
      return Response.json(session ? mailbox : { error: "SignInRequired" }, {
        status: session ? 200 : 401,
      });
    if (url === "/api/auth/sign-up/email" || url === "/api/auth/sign-in/email") {
      session = true;
      return Response.json({ user: mailbox.user });
    }
    if (url === "/api/auth/sign-out") {
      session = false;
      return Response.json({ success: true });
    }
    if (url === "/mail/messages" || url.startsWith("/mail/search?"))
      return Response.json({ messages: [message], cursor: null });
    if (url === "/mail/messages/" + message.id) return Response.json(message);
    if (url === "/mail/outbox")
      return Response.json({ messages: [{ ...message, created_at: 1234, state: "unknown" }] });
    if (url === "/api/auth/request-password-reset" || url === "/api/auth/reset-password")
      return Response.json({ status: true });
    throw Error("UnexpectedUiRequest");
  };
  await window.eval("(async()=>{" + script + "\n})()");
  return { dom, window, calls, message, $: (id) => window.document.getElementById(id) };
}
test("mail UI wires signup, private inbox, search, outbox and logout without interpreting email HTML", async () => {
  const f = await fixture();
  try {
    assert.equal(f.$("account").hidden, false);
    const form = f.$("login");
    form.elements.email.value = "alice@example.test";
    form.elements.password.value = "long enough test password";
    form.elements.name.value = "Alice";
    await form.onsubmit({
      preventDefault() {},
      currentTarget: form,
      submitter: { value: "signup" },
    });
    assert.equal(f.$("mail").hidden, false);
    assert.equal(f.$("identity").textContent, "alice@example.test");
    assert.equal(f.$("messages").querySelector("img"), null);
    await f.$("messages").querySelector("button").onclick();
    assert.equal(f.$("message").querySelector("script"), null);
    assert.equal(f.$("message").querySelector("b"), null);
    assert.equal(f.$("message").querySelector("pre").textContent, "<script>alert(1)</script>");
    assert.equal(
      f.$("message").querySelector("a").getAttribute("href"),
      "/mail/messages/" + f.message.id + "/attachments/0"
    );
    f.$("search").elements.query.value = "needle word";
    await f.$("search").onsubmit({ preventDefault() {}, currentTarget: f.$("search") });
    assert.ok(f.calls.some((c) => c.url === "/mail/search?q=needle%20word"));
    await f.$("outbox").onclick();
    await f.$("messages").querySelector("button").onclick();
    assert.match(f.$("message").textContent, /will not be resent automatically/);
    await f.$("logout").onclick();
    assert.equal(f.$("account").hidden, false);
    assert.equal(f.$("mail").hidden, true);
  } finally {
    f.dom.window.close();
  }
});
test("password recovery UI submits the scoped reset token and returns to sign-in", async () => {
  const f = await fixture("/reset-password?token=disposable-ui-fixture-token");
  try {
    assert.equal(f.$("reset").hidden, false);
    f.$("reset-form").elements.password.value = "a different correct password";
    await f.$("reset-form").onsubmit({ preventDefault() {}, currentTarget: f.$("reset-form") });
    assert.equal(f.window.location.pathname, "/");
    assert.equal(f.$("account").hidden, false);
    assert.ok(
      f.calls.some(
        (c) =>
          c.url === "/api/auth/reset-password" && c.body.token === "disposable-ui-fixture-token"
      )
    );
    assert.match(f.$("status").textContent, /Password changed/);
  } finally {
    f.dom.window.close();
  }
});
