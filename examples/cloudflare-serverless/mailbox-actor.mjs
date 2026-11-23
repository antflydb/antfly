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
  MailSyncLog,
  MailDeletion,
  MailTrash,
  ownedObjects,
  syncMessage,
  syncLimits,
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
    this.syncLog = new MailSyncLog(this.serviceStore);
    this.deletion = new MailDeletion(this.serviceStore, this.syncLog);
    this.trashBin = new MailTrash(this.serviceStore, this.syncLog, {
      retentionDays: Number(env.MAIL_TRASH_DAYS ?? 30),
    });
  }
  scope(id) {
    if (!uuid(id) || !this.env.MAILBOXES.idFromName(id).equals(this.ctx.id))
      throw Error("MailboxScopeMismatch");
    return objectPrefix(id);
  }
  async run(input, signal = AbortSignal.timeout(30000)) {
    if (input.op === "mutate") return this.mutateBatch(input);
    return this.runtime.run(this.env, input, signal);
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
      // Raw MIME is stored per delivery so deleting a message can remove it.
      receipt.key !== prefix + "raw/" + receipt.id
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
    this.receiptKey(id);
    this.ingests ??= new Map();
    const active = this.ingests.get(id);
    if (active) return active;
    // Purge may mark the receipt at any time, but must wait until no ingest
    // can still write its objects or search document. The marker survives a
    // restart, and every object key is recorded before starting its write.
    const task = this.ingestReceipt(mailbox, prefix, id);
    this.ingests.set(id, task);
    try {
      return await task;
    } finally {
      this.ingests.delete(id);
    }
  }
  async ingestReceipt(mailbox, prefix, id) {
    let receipt = await this.transaction((tx) => tx.get(this.receiptKey(id)));
    if (!receipt) throw Error("UnknownInboundReceipt");
    if (receipt.state === "indexed" || receipt.state === "deleted") return { done: true };
    const projection = receipt.projection;
    if (projection && !projection.key.startsWith(prefix + "projections/")) throw Error("InvalidProjectionScope");
    const prepared = projection ? await this.env.MAIL_OBJECTS.get(projection.key) : null;
    let body;
    if (prepared) {
      if (prepared.size > 4 * 1024 * 1024) throw Error("MailProjectionTooLarge");
      body = await prepared.text();
      if ((await digest(body)) !== projection.sha256) throw Error("MailProjectionDigestMismatch");
    } else {
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
      const attachments = await Promise.all(parsed.attachments.map(async (part) => ({
        index: part.index,
        filename: safeFilename(part.filename),
        size: part.bytes.length,
        key: prefix + "attachments/" + id + "/" + part.index,
        sha256: await digest(part.bytes),
      })));
      const parentKey = parsed.in_reply_to ? "mail/v1/thread/" + (await digest(parsed.in_reply_to)) : null;
      const messageKey = parsed.message_id ? "mail/v1/thread/" + (await digest(parsed.message_id)) : null;
      // Record attachment ownership before any object writes. A worker that
      // stops halfway through its puts leaves enough metadata for purge.
      const metadata = await this.transaction(async (tx) => {
        const current = await tx.get(this.receiptKey(id));
        if (current.state === "deleted") return null;
        if (current.prepared) return current.prepared;
        let thread = id;
        if (parentKey) {
          const parent = await tx.get(parentKey);
          if (parent && !parent.ambiguous) thread = parent.id;
        }
        if (messageKey) {
          const old = await tx.get(messageKey);
          await tx.put(messageKey, old && old.id !== thread ? { ambiguous: true } : { id: thread });
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
      if (!metadata) return { done: true, purge: true };
      body = JSON.stringify({ ...metadata, text: parsed.text });
      const checksum = await digest(body);
      const key = prefix + "projections/" + checksum;
      if (projection && (projection.key !== key || projection.sha256 !== checksum))
        throw Error("MailProjectionDigestMismatch");
      receipt = await this.transaction(async (tx) => {
        const current = await tx.get(this.receiptKey(id));
        if (current.state === "deleted") return current;
        const next = { ...current, projection: { key, sha256: checksum } };
        await tx.put(this.receiptKey(id), next);
        return next;
      });
      if (receipt.state === "deleted") return { done: true, purge: true };
      for (const part of parsed.attachments) {
        await this.env.MAIL_OBJECTS.put(metadata.attachments[part.index].key, part.bytes, {
          onlyIf: { etagDoesNotMatch: "*" },
          httpMetadata: { contentType: "application/octet-stream" },
        });
      }
      // The projection is the completion marker for the attachment puts.
      await this.env.MAIL_OBJECTS.put(key, body, {
        onlyIf: { etagDoesNotMatch: "*" },
        httpMetadata: { contentType: "application/json" },
      });
    }
    await this.run({
      op: "mutate",
      operation_id: "mail/ingest/" + id,
      mutations: [{ kind: "upsert", doc_id: "message/" + id, body }],
    });
    const deletedMeanwhile = await this.transaction(async (tx) => {
      const current = await tx.get(this.receiptKey(id));
      if (current.state === "deleted") {
        // A late index commit needs its own delete operation if an older
        // deployment already submitted a purge for this receipt.
        await this.deletion.requirePurge(tx, id);
        return true;
      }
      await tx.put(this.receiptKey(id), { ...current, state: "indexed" });
      await tx.delete("mail/v1/pending/" + id);
      await tx.put("mail/v1/publish", crypto.randomUUID());
      await tx.put(
        "mail/v1/order/" + String(9999999999999 - current.received_at).padStart(13, "0") + "/" + id,
        id
      );
      // Offline replicas learn about the message in the same commit that lists it.
      await this.syncLog.record(tx, id);
      return false;
    });
    if (deletedMeanwhile) return { done: true, purge: true };
    return this.publish(mailbox);
  }
  async trash(mailbox, id) {
    this.scope(mailbox);
    return { id, trashed: true, delete_at: await this.trashBin.trash(id) };
  }
  async restore(mailbox, id) {
    this.scope(mailbox);
    await this.trashBin.restore(id);
    return { id, restored: true };
  }
  async trashList(mailbox, cursor = "") {
    this.scope(mailbox);
    const page = await this.trashBin.list(cursor);
    const messages = [];
    for (const id of page.ids) {
      const message = await this.message(mailbox, id);
      if (message) messages.push(message);
    }
    return { messages, cursor: page.cursor, retention_days: this.trashBin.retentionDays };
  }
  // Delete forever, in bounded steps: everything in Trash, or only what has
  // outlived the retention period. `done: false` asks the caller to continue.
  async emptyTrash(mailbox, { continuation = false } = {}) {
    return this.purgeTrash(mailbox, continuation ? null : this.trashBin.clock(), true);
  }
  async expire(mailbox) {
    return this.purgeTrash(mailbox, this.trashBin.clock() - this.trashBin.retentionMs, false);
  }
  async purgeTrash(mailbox, cutoff, empty) {
    this.scope(mailbox);
    const { ids, done } = await this.deletion.markTrash(25, cutoff, empty);
    // Submit bounded groups together so the coordinator can combine their
    // WAL mutations. Every marked receipt has an independent retry marker.
    for (let offset = 0; offset < ids.length; offset += 8) {
      const results = await Promise.allSettled(ids.slice(offset, offset + 8).map((id) => this.purgeObjects(mailbox, id)));
      const failed = results.find((result) => result.status === "rejected");
      if (failed) throw failed.reason;
    }
    if (ids.length) await this.publish(mailbox);
    return { deleted: ids.length, done };
  }
  // Hide the message at once (one transaction), then purge it. A failed or
  // interrupted purge is retried by the queue and the scheduled sweep.
  async delete(mailbox, id) {
    this.scope(mailbox);
    if (!uuid(id)) throw Error("InvalidMessageIdentity");
    const marked = await this.deletion.mark(id);
    if (marked) await this.purge(mailbox, id);
    return { id, deleted: true };
  }
  async purge(mailbox, id) {
    const finished = await this.purgeObjects(mailbox, id);
    if (finished === false) return { deferred: true };
    if (finished === null) return { done: true };
    return this.publish(mailbox);
  }
  async purgeObjects(mailbox, id) {
    const prefix = this.scope(mailbox);
    const receipt = await this.deletion.purging(id);
    if (!receipt) return null;
    if (this.ingests?.has(id)) return false;
    const attempt = receipt.purge_attempt ?? 0;
    await this.run({
      op: "mutate",
      operation_id: "mail/delete/" + id + "/" + attempt,
      mutations: [{ kind: "delete", doc_id: "message/" + id }],
    });
    await this.transaction((tx) => tx.put("mail/v1/publish", crypto.randomUUID()));
    const keys = ownedObjects(prefix, receipt);
    if (keys.length) await this.env.MAIL_OBJECTS.delete(keys);
    return this.deletion.finish(id, attempt);
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
    const receipt = await this.transaction((tx) => tx.get(this.receiptKey(id)));
    return this.loadMessage(id, receipt);
  }
  async loadMessage(id, receipt, signal) {
    if (receipt?.state !== "indexed") return null;
    const result = await this.run({ op: "lookup", doc_id: "message/" + id }, signal);
    if (!result.body) return null;
    const message = JSON.parse(result.body);
    const trashedAt = receipt.trashed_at;
    return trashedAt == null ? message : { ...message, trashed_at: trashedAt, delete_at: this.trashBin.expiresAt(trashedAt) };
  }
  async inboxMessages(ids, limit = ids.length, signal) {
    if (!ids.length) return [];
    const receipts = await this.transaction((tx) => tx.getMany(ids.map((id) => this.receiptKey(id))));
    const messages = [];
    for (const [index, id] of ids.entries()) {
      if (messages.length === limit) break;
      const receipt = receipts[index];
      if (receipt?.state !== "indexed" || receipt.trashed_at != null) continue;
      const message = await this.loadMessage(id, receipt, signal);
      if (message) messages.push(message);
    }
    return messages;
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
    const messages = await this.inboxMessages([...page.values()]);
    return { messages, cursor: page.size === 10 ? [...page.keys()].at(-1) : null };
  }
  async search(mailbox, text) {
    this.scope(mailbox);
    if (typeof text !== "string" || !text.trim() || text.length > 1024)
      throw Error("InvalidMailQuery");
    const messages = [];
    let cursor = null;
    const signal = AbortSignal.timeout(30000);
    do {
      const page = await this.run({ op: "search", text, limit: 32, search_ids_only: true, cursor }, signal);
      const ids = (page.hits ?? []).map(({ doc_id }) => {
        if (!doc_id?.startsWith("message/") || !uuid(doc_id.slice(8))) throw Error("InvalidMailSearchResult");
        return doc_id.slice(8);
      });
      messages.push(...await this.inboxMessages(ids, 10 - messages.length, signal));
      cursor = page.cursor;
    } while (messages.length < 10 && cursor);
    return { messages };
  }
  // Incremental, text-only export of this mailbox for the PWA's local index.
  // Session membership has already selected this actor; the cursor is only a
  // position in this mailbox's log and grants nothing on its own.
  async sync(mailbox, cursor = "") {
    this.scope(mailbox);
    const backfilled = await this.syncLog.backfill();
    const page = await this.syncLog.page(cursor);
    const messages = (await this.inboxMessages(page.ids)).map(syncMessage);
    return {
      messages,
      deleted: page.deleted ?? [],
      cursor: page.cursor,
      ...(page.reset ? { reset: true } : {}),
      more: page.more || !backfilled,
      limits: { text_bytes: syncLimits.textBytes },
    };
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
      purge: await this.deletion.pending(),
      expire: (await this.trashBin.expired(1)).length > 0,
      ...(await this.deletion.emptyTrashPending() ? { empty: true } : {}),
      publish: !!(await this.transaction((tx) => tx.get("mail/v1/publish"))),
    };
  }
}
