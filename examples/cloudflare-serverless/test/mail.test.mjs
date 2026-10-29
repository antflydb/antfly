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
import {
  MailOutbox,
  parseMail,
  boundedBytes,
  safeFilename,
  address,
  mailLimits,
} from "../mail-core.mjs";
import worker from "../mail-worker.mjs";
class Store {
  rows = new Map();
  pending = Promise.resolve();
  transaction(fn) {
    const task = this.pending.then(async () => {
      const rows = structuredClone(this.rows),
        result = await fn({
          get: async (k) => structuredClone(rows.get(k)),
          put: (k, v) => rows.set(k, structuredClone(v)),
          delete: (k) => rows.delete(k),
          list: async ({ prefix, limit }) =>
            new Map(
              [...rows]
                .filter(([k]) => k.startsWith(prefix))
                .sort()
                .slice(0, limit)
            ),
        });
      this.rows = rows;
      return result;
    });
    this.pending = task.then(
      () => {},
      () => {}
    );
    return task;
  }
}
const mail = {
  from: "support@example.test",
  to: "customer@example.net",
  subject: "A receipt",
  text: "Hello.",
};
test("outbox persists before send, serializes competing deliveries and replays completed receipts", async () => {
  const store = new Store(),
    outbox = new MailOutbox(store),
    id = crypto.randomUUID();
  await outbox.enqueue(id, mail);
  assert.equal((await outbox.get(id)).state, "pending");
  assert.deepEqual(await outbox.pending(), [id]);
  let sent = 0;
  const results = await Promise.allSettled([
    outbox.dispatch(id, async () => {
      sent++;
      await new Promise((r) => setTimeout(r, 5));
      return { messageId: "provider-id" };
    }),
    outbox.dispatch(id, async () => {
      sent++;
      return { messageId: "duplicate" };
    }),
  ]);
  assert.equal(results[0].status, "fulfilled");
  assert.equal(sent, 1);
  assert.equal((await outbox.get(id)).state, "accepted");
  await outbox.dispatch(id, async () => {
    throw Error("must not send");
  });
  assert.deepEqual(await outbox.pending(), []);
  await assert.rejects(outbox.enqueue(id, { ...mail, text: "different" }), /Conflict/);
});
test("ambiguous sends and expired claimed sends are never automatically resubmitted", async () => {
  let now = 1000;
  const store = new Store(),
    outbox = new MailOutbox(store, { clock: () => now }),
    id = crypto.randomUUID();
  await outbox.enqueue(id, mail);
  assert.equal(
    (
      await outbox.dispatch(id, async () => {
        throw Error("lost acknowledgement");
      })
    ).state,
    "unknown"
  );
  let called = false;
  await outbox.dispatch(id, async () => {
    called = true;
  });
  assert.equal(called, false);
  const other = crypto.randomUUID();
  await outbox.enqueue(other, mail);
  await store.transaction(async (tx) => {
    const key = outbox.key(other),
      row = await tx.get(key);
    tx.put(key, { ...row, state: "sending", claim_until: now + 60000 });
  });
  now += 61000;
  assert.equal(
    (
      await outbox.dispatch(other, async () => {
        called = true;
      })
    ).state,
    "unknown"
  );
  assert.equal(called, false);
});
test("known provider rejection and accepted are distinct from recipient delivery", async () => {
  const outbox = new MailOutbox(new Store()),
    id = crypto.randomUUID();
  await outbox.enqueue(id, mail);
  assert.equal(
    (
      await outbox.dispatch(id, async () => {
        throw Object.assign(Error("rejected"), { code: "E_RECIPIENT_SUPPRESSED" });
      })
    ).state,
    "rejected"
  );
  assert.equal((await outbox.get(id)).provider_id, null);
});
test("MIME parsing preserves envelope delivery and treats sender HTML as untrusted data", async () => {
  const raw = new TextEncoder().encode(
    "From: forged@example.net\r\nTo: visible@example.net\r\nSubject: Receipt\r\nContent-Type: text/html; charset=utf-8\r\n\r\n<script>alert(1)</script>"
  );
  const result = await parseMail(
    raw,
    { from: "envelope@example.net", to: "actual@example.test" },
    123
  );
  assert.equal(result.to, "actual@example.test");
  assert.equal(result.from, "envelope@example.net");
  assert.equal(result.html_available, true);
  assert.ok(!Object.hasOwn(result, "html"));
  assert.ok(!JSON.stringify(result).includes("<script>"));
});
test("mail boundaries reject oversized input, header injection and invalid recipients", async () => {
  await assert.rejects(boundedBytes(new Blob(["12345"]).stream(), 4), /SizeExceeded/);
  const outbox = new MailOutbox(new Store());
  for (const value of [
    { ...mail, from: "bad\r\n@example.test" },
    { ...mail, subject: "hello\r\nBcc: bad@example.test" },
    { ...mail, text: "x".repeat(mailLimits.outgoingTextBytes + 1) },
    { ...mail, headers: { From: "spoof@example.test" } },
    { ...mail, headers: { References: "x\r\ny" } },
  ])
    await assert.rejects(outbox.enqueue(crypto.randomUUID(), value));
  assert.throws(() => address("foo@example.test\n"));
  assert.equal(safeFilename("../a\u202e.exe"), ".._a_.exe");
});
test("mail HTTP authorization never accepts browser-selected ownership or bypasses origin checks", async () => {
  const mailbox = { id: crypto.randomUUID(), address: "support@example.test" },
    calls = [],
    env = {
      AUTH_ORIGIN: "https://mail.example.test",
      AUTH_SERVICE: { session: async () => ({ user: { id: "alice" }, mailbox }) },
      MAILBOXES: {
        getByName(id) {
          calls.push(id);
          return { list: async (id) => ({ messages: [], mailbox: id }), message: async () => null };
        },
      },
    };
  const req = (path, method = "GET", body, origin = "https://mail.example.test") =>
    new Request("https://mail.example.test" + path, {
      method,
      headers: { Origin: origin, "X-Antfly-Tenant": "bob", "Content-Type": "application/json" },
      ...(body ? { body: JSON.stringify(body) } : {}),
    });
  const listing = await worker.fetch(req("/mail/messages"), env);
  assert.equal(listing.status, 200);
  assert.deepEqual(calls, [mailbox.id]);
  assert.equal((await worker.fetch(req("/mail/messages/" + crypto.randomUUID()), env)).status, 404);
  assert.equal(
    (
      await worker.fetch(
        req("/mail/outbox", "POST", { id: crypto.randomUUID(), owner_id: "bob" }),
        env
      )
    ).status,
    400
  );
  assert.equal(
    (await worker.fetch(req("/mail/outbox", "POST", {}, "https://attacker.test"), env)).status,
    403
  );
  assert.equal(
    (
      await worker.fetch(req("/mail/messages"), {
        ...env,
        AUTH_SERVICE: { session: async () => null },
      })
    ).status,
    401
  );
});

test("per-mailbox outbound quota is atomic, bounded and preserves idempotent enqueue", async () => {
  let now = 0;
  const outbox = new MailOutbox(new Store(), { maxPerHour: 1, clock: () => now }),
    id = crypto.randomUUID();
  await outbox.enqueue(id, mail);
  await outbox.enqueue(id, mail);
  await assert.rejects(outbox.enqueue(crypto.randomUUID(), mail), /MailSendRateLimited/);
  now = 3600000;
  assert.equal((await outbox.enqueue(crypto.randomUUID(), mail)).state, "pending");
});
