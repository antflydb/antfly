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

import { R2CommitCoordinator } from "./r2-coordinator.mjs";
import { createRuntime } from "./runtime.mjs";
import { buildKernel, sharedCache } from "./kernels.mjs";
import {
  MailOutbox,
  uuid,
  objectPrefix,
  parseMail,
  digest,
  address,
  safeFilename,
} from "./mail-core.mjs";
const sha = (v) => typeof v === "string" && /^[a-f0-9]{64}$/.test(v);
export class Mailbox extends R2CommitCoordinator {
  constructor(ctx, env) {
    const namespace = "mailbox-v1-" + ctx.id.toString();
    super(ctx, {
      ...env,
      ANTFLY_INCARNATION: namespace,
      COORDINATOR_STORAGE_ID: namespace,
      ANTFLY_TENANTS: undefined,
      COMMITS: env.MAILBOXES,
      COORDINATOR_BACKEND: "r2",
      SPLIT_RUNTIME: "false",
      AUTO_GC: "false",
    });
    this.env = { ...this.env, COMMITS: { getByName: () => this } };
    this.runtime = createRuntime({
      kernel: buildKernel,
      sharedCache,
      background: (promise) => ctx.waitUntil(promise),
    });
    this.outbox = new MailOutbox(this.serviceStore, {
      maxPerHour: Number(env.MAIL_SENDS_PER_HOUR ?? 20),
    });
  }
  scope(id) {
    if (!uuid(id) || !this.env.MAILBOXES.idFromName(id).equals(this.ctx.id))
      throw Error("MailboxScopeMismatch");
    return objectPrefix(id);
  }
  async run(input) {
    if (input.op === "mutate") return this.mutateBatch(input);
    return this.runtime.run(this.env, input, AbortSignal.timeout(30000));
  }
  receiptKey(id) {
    if (!uuid(id)) throw Error("InvalidDeliveryReceipt");
    return "mail/v1/receipt/" + id;
  }
  async receive(mailbox, receipt) {
    const prefix = this.scope(mailbox);
    if (
      !uuid(receipt.id) ||
      !sha(receipt.sha256) ||
      receipt.key !== prefix + "raw/" + receipt.sha256
    )
      throw Error("InvalidInboundReceipt");
    if (receipt.from !== "") address(receipt.from);
    address(receipt.to);
    return this.transaction(async (tx) => {
      const key = this.receiptKey(receipt.id),
        old = await tx.get(key);
      if (old) {
        if (old.sha256 !== receipt.sha256 || old.from !== receipt.from || old.to !== receipt.to)
          throw Error("InboundReceiptConflict");
        return { id: old.id };
      }
      await tx.put(key, { ...receipt, state: "pending", received_at: Date.now() });
      await tx.put("mail/v1/pending/" + receipt.id, receipt.id);
      return { id: receipt.id };
    });
  }
  async ingest(mailbox, id) {
    const prefix = this.scope(mailbox);
    let receipt = await this.transaction((tx) => tx.get(this.receiptKey(id)));
    if (!receipt) throw Error("UnknownInboundReceipt");
    if (receipt.state === "indexed") return { done: true };
    if (!receipt.projection) {
      const object = await this.env.MAIL_OBJECTS.get(receipt.key);
      if (!object || object.size > 5 * 1024 * 1024) throw Error("MailRawObjectMissing");
      const raw = new Uint8Array(await object.arrayBuffer());
      if ((await digest(raw)) !== receipt.sha256) throw Error("MailRawDigestMismatch");
      let parsed;
      try {
        parsed = await parseMail(raw, receipt, receipt.received_at);
      } catch (error) {
        if (
          ![
            "MailContentBudgetExceeded",
            "MailAttachmentBudgetExceeded",
            "MailSizeExceeded",
          ].includes(error?.message) &&
          ![
            "EMAXNESTINGDEPTH",
            "EMAXHEADERSIZE",
            "EMAXPARTCOUNT",
            "EMAXFORWARDEDADDRESSES",
          ].includes(error?.code)
        )
          throw error;
        parsed = {
          subject: "Message quarantined",
          text: "This message could not be decoded within the mailbox limits. The original remains in private storage for review.",
          from: receipt.from,
          to: receipt.to,
          received_at: receipt.received_at,
          message_id: null,
          in_reply_to: null,
          html_available: false,
          attachments: [],
          quarantined: true,
        };
      }
      const attachments = [];
      for (const part of parsed.attachments) {
        const checksum = await digest(part.bytes),
          key = prefix + "attachments/" + checksum;
        await this.env.MAIL_OBJECTS.put(key, part.bytes, {
          onlyIf: { etagDoesNotMatch: "*" },
          httpMetadata: { contentType: "application/octet-stream" },
        });
        attachments.push({
          index: part.index,
          filename: safeFilename(part.filename),
          size: part.bytes.length,
          key,
          sha256: checksum,
        });
      }
      // Persist only small metadata under the transactional root. Large immutable
      // projections are prepared in R2 outside its critical section, then adopted.
      const metadata = await this.transaction(async (tx) => {
        const current = await tx.get(this.receiptKey(id));
        if (current.prepared) return current.prepared;
        let thread = id;
        if (parsed.in_reply_to) {
          const parent = await tx.get("mail/v1/thread/" + (await digest(parsed.in_reply_to)));
          if (parent && !parent.ambiguous) thread = parent.id;
        }
        if (parsed.message_id) {
          const key = "mail/v1/thread/" + (await digest(parsed.message_id)),
            old = await tx.get(key);
          await tx.put(key, old && old.id !== thread ? { ambiguous: true } : { id: thread });
        }
        const metadata = {
          id,
          direction: "inbound",
          thread_id: thread,
          subject: parsed.subject,
          from: parsed.from,
          to: parsed.to,
          received_at: parsed.received_at,
          message_id: parsed.message_id,
          in_reply_to: parsed.in_reply_to,
          html_available: parsed.html_available,
          quarantined: parsed.quarantined === true,
          attachments,
        };
        await tx.put(this.receiptKey(id), { ...current, prepared: metadata });
        return metadata;
      });
      const body = JSON.stringify({ ...metadata, text: parsed.text }),
        checksum = await digest(body),
        key = prefix + "projections/" + checksum;
      await this.env.MAIL_OBJECTS.put(key, body, {
        onlyIf: { etagDoesNotMatch: "*" },
        httpMetadata: { contentType: "application/json" },
      });
      receipt = await this.transaction(async (tx) => {
        const current = await tx.get(this.receiptKey(id));
        if (current.projection) return current;
        const next = { ...current, projection: { key, sha256: checksum } };
        await tx.put(this.receiptKey(id), next);
        return next;
      });
    }
    if (!receipt.projection.key.startsWith(prefix + "projections/"))
      throw Error("InvalidProjectionScope");
    const object = await this.env.MAIL_OBJECTS.get(receipt.projection.key);
    if (!object || object.size > 4 * 1024 * 1024) throw Error("MailProjectionMissing");
    const body = await object.text();
    if ((await digest(body)) !== receipt.projection.sha256)
      throw Error("MailProjectionDigestMismatch");
    await this.run({
      op: "mutate",
      operation_id: "mail/ingest/" + id,
      mutations: [{ kind: "upsert", doc_id: "message/" + id, body }],
    });
    await this.transaction(async (tx) => {
      const current = await tx.get(this.receiptKey(id));
      await tx.put(this.receiptKey(id), { ...current, state: "indexed" });
      await tx.delete("mail/v1/pending/" + id);
      await tx.put("mail/v1/publish", crypto.randomUUID());
      await tx.put(
        "mail/v1/order/" + String(9999999999999 - current.received_at).padStart(13, "0") + "/" + id,
        id
      );
    });
    return this.publish(mailbox);
  }
  async publish(mailbox) {
    this.scope(mailbox);
    const intent = await this.transaction((tx) => tx.get("mail/v1/publish"));
    const result = await this.run({ op: "publish" });
    if (!result.has_more)
      await this.transaction(async (tx) => {
        if ((await tx.get("mail/v1/publish")) === intent) await tx.delete("mail/v1/publish");
      });
    return { done: !result.has_more };
  }
  async message(mailbox, id) {
    this.scope(mailbox);
    if (!uuid(id)) throw Error("InvalidMessageIdentity");
    const result = await this.run({ op: "lookup", doc_id: "message/" + id });
    return result.body ? JSON.parse(result.body) : null;
  }
  async list(mailbox, cursor = "") {
    this.scope(mailbox);
    if (
      typeof cursor !== "string" ||
      cursor.length > 128 ||
      (cursor && !/^mail\/v1\/order\/\d{13}\/[a-f0-9-]{36}$/.test(cursor))
    )
      throw Error("InvalidMailCursor");
    const page = await this.transaction((tx) =>
      tx.list({ prefix: "mail/v1/order/", startAfter: cursor, limit: 10 })
    );
    const messages = [];
    for (const id of page.values()) {
      const message = await this.message(mailbox, id);
      if (message) messages.push(message);
    }
    return { messages, cursor: page.size === 10 ? [...page.keys()].at(-1) : null };
  }
  async search(mailbox, text) {
    this.scope(mailbox);
    if (typeof text !== "string" || !text.trim() || text.length > 1024)
      throw Error("InvalidMailQuery");
    const found = await this.run({ op: "search", text, limit: 10, fresh: true }),
      messages = [];
    for (const hit of found.hits ?? []) {
      if (!hit.doc_id?.startsWith("message/")) throw Error("InvalidMailSearchResult");
      const message = await this.message(mailbox, hit.doc_id.slice(8));
      if (message) messages.push(message);
    }
    return { messages };
  }
  async attachment(mailbox, id, index) {
    const prefix = this.scope(mailbox),
      message = await this.message(mailbox, id),
      part = message?.attachments?.find((a) => a.index === index);
    if (!part || !part.key.startsWith(prefix + "attachments/")) return null;
    const object = await this.env.MAIL_OBJECTS.get(part.key);
    if (!object || object.size !== part.size) throw Error("MailAttachmentMissing");
    const bytes = new Uint8Array(await object.arrayBuffer());
    if ((await digest(bytes)) !== part.sha256) throw Error("MailAttachmentDigestMismatch");
    return { bytes, filename: safeFilename(part.filename) };
  }
  async enqueue(mailbox, id, mail) {
    this.scope(mailbox);
    return this.outbox.enqueue(id, mail);
  }
  async dispatch(mailbox, id) {
    this.scope(mailbox);
    return this.outbox.dispatch(id, (mail) => this.env.EMAIL.send(mail));
  }
  async outboundList(mailbox, cursor = "") {
    this.scope(mailbox);
    return this.outbox.list(cursor);
  }
  async outbound(mailbox, id) {
    this.scope(mailbox);
    const row = await this.outbox.get(id);
    return row
      ? {
          id: row.id,
          state: row.state,
          provider_id: row.provider_id ?? null,
          created_at: row.created_at,
        }
      : null;
  }
  async pending(mailbox) {
    this.scope(mailbox);
    return {
      inbound: await this.transaction(async (tx) => [
        ...(await tx.list({ prefix: "mail/v1/pending/", limit: 16 })).values(),
      ]),
      outbound: await this.outbox.pending(),
      publish: !!(await this.transaction((tx) => tx.get("mail/v1/publish"))),
    };
  }
}
