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

import PostalMime from "postal-mime";
export const mailLimits = Object.freeze({
  rawBytes: 5 * 1024 * 1024,
  textBytes: 512 * 1024,
  outgoingTextBytes: 64 * 1024,
  attachments: 16,
  attachmentBytes: 4 * 1024 * 1024,
});
export const uuid = (value) =>
  typeof value === "string" && /^[a-f0-9]{8}-(?:[a-f0-9]{4}-){3}[a-f0-9]{12}$/.test(value);
export const digest = async (bytes) =>
  Array.from(
    new Uint8Array(
      await crypto.subtle.digest(
        "SHA-256",
        typeof bytes === "string" ? new TextEncoder().encode(bytes) : bytes
      )
    ),
    (b) => b.toString(16).padStart(2, "0")
  ).join("");
export function address(value) {
  if (
    typeof value !== "string" ||
    value.length > 254 ||
    !/^[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]+@[A-Za-z0-9](?:[A-Za-z0-9.-]*[A-Za-z0-9])?$/.test(value)
  )
    throw Error("InvalidMailAddress");
  const at = value.lastIndexOf("@");
  return value.slice(0, at) + "@" + value.slice(at + 1).toLowerCase();
}
export async function boundedBytes(stream, maximum = mailLimits.rawBytes) {
  let size = 0;
  const chunks = [];
  for await (const chunk of stream) {
    const bytes = new Uint8Array(chunk);
    size += bytes.length;
    if (size > maximum) throw Error("MailSizeExceeded");
    chunks.push(bytes);
  }
  const bytes = new Uint8Array(size);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.length;
  }
  return bytes;
}
export const objectPrefix = (id) => {
  if (!uuid(id)) throw Error("InvalidMailbox");
  return "mail/" + id + "/";
};
export async function parseMail(raw, envelope, receivedAt) {
  if (raw.byteLength > mailLimits.rawBytes) throw Error("MailSizeExceeded");
  const parsed = await PostalMime.parse(raw, {
    maxNestingDepth: 12,
    maxPartCount: 128,
    maxHeadersSize: 64 * 1024,
    maxRfc822NestingDepth: 2,
    maxForwardedAddresses: 32,
  });
  const text = parsed.text ?? "";
  if (
    new TextEncoder().encode(text).length > mailLimits.textBytes ||
    parsed.attachments.length > mailLimits.attachments
  )
    throw Error("MailContentBudgetExceeded");
  let attachmentBytes = 0;
  const attachments = parsed.attachments.map((part, index) => {
    const bytes = new Uint8Array(part.content);
    attachmentBytes += bytes.length;
    if (attachmentBytes > mailLimits.attachmentBytes || part.rfc822DepthExceeded)
      throw Error("MailAttachmentBudgetExceeded");
    return {
      index,
      filename: typeof part.filename === "string" ? part.filename.slice(0, 255) : "attachment",
      mime_type: part.mimeType,
      bytes,
    };
  });
  const messageId = typeof parsed.messageId === "string" ? parsed.messageId.slice(0, 512) : null,
    inReplyTo = typeof parsed.inReplyTo === "string" ? parsed.inReplyTo.slice(0, 512) : null;
  return {
    message_id: messageId,
    in_reply_to: inReplyTo,
    subject: (parsed.subject ?? "").slice(0, 998),
    from: envelope.from,
    to: envelope.to,
    text,
    received_at: receivedAt,
    attachments,
    html_available: !!parsed.html,
  };
}
export function safeFilename(value) {
  return (
    String(value ?? "attachment")
      .replace(/[\x00-\x1f\x7f/\\\u202a-\u202e\u2066-\u2069]/g, "_")
      .slice(0, 255) || "attachment"
  );
}
const outgoingMessage = (value) => {
  if (
    value?.headers &&
    (Object.keys(value.headers).some((k) => !["In-Reply-To", "References"].includes(k)) ||
      Object.values(value.headers).some(
        (v) => typeof v !== "string" || v.length > 1024 || /[\r\n]/.test(v)
      ))
  )
    throw Error("InvalidOutgoingHeaders");
  if (
    !value ||
    typeof value.subject !== "string" ||
    value.subject.length > 998 ||
    /[\r\n]/.test(value.subject) ||
    typeof value.text !== "string" ||
    new TextEncoder().encode(value.text).length > mailLimits.outgoingTextBytes
  )
    throw Error("InvalidOutgoingMail");
  return {
    from: address(value.from),
    to: address(value.to),
    subject: value.subject,
    text: value.text,
    ...(value.headers ? { headers: value.headers } : {}),
  };
};
// Antfly native metadata transactions protect outbox claims. Provider acceptance
// and durable acknowledgement are separate; an unknown send is never replayed.
export class MailOutbox {
  constructor(store, { clock = Date.now, prefix = "mail/v1/outbox/", maxPerHour } = {}) {
    this.store = store;
    this.clock = clock;
    this.prefix = prefix;
    this.maxPerHour = maxPerHour;
    if (
      maxPerHour != null &&
      (!Number.isInteger(maxPerHour) || maxPerHour < 1 || maxPerHour > 1000)
    )
      throw Error("InvalidMailQuota");
  }
  key(id) {
    if (!uuid(id)) throw Error("InvalidOutboxIdentity");
    return this.prefix + id;
  }
  async enqueue(id, message) {
    const payload = outgoingMessage(message),
      fingerprint = await digest(JSON.stringify(payload));
    return this.store.transaction(async (tx) => {
      const key = this.key(id),
        old = await tx.get(key);
      if (old) {
        if (old.fingerprint !== fingerprint) throw Error("OutboxIdentityConflict");
        return old;
      }
      if (this.maxPerHour != null) {
        const key = this.prefix + "quota",
          hour = Math.floor(this.clock() / 3600000),
          prior = await tx.get(key),
          count = prior?.hour === hour ? prior.count : 0;
        if (count >= this.maxPerHour) throw Error("MailSendRateLimited");
        await tx.put(key, { hour, count: count + 1 });
      }
      const row = { id, payload, fingerprint, state: "pending", created_at: this.clock() };
      await tx.put(key, row);
      await tx.put(this.prefix + "pending/" + id, id);
      return row;
    });
  }
  async get(id) {
    return this.store.transaction((tx) => tx.get(this.key(id)));
  }
  async dispatch(id, send) {
    const attempt = crypto.randomUUID(),
      now = this.clock(),
      claim = await this.store.transaction(async (tx) => {
        const key = this.key(id),
          row = await tx.get(key);
        if (!row) throw Error("UnknownOutboxMessage");
        if (row.state === "sending") {
          if (row.claim_until > now) return { busy: true };
          row.state = "unknown";
          await tx.put(key, row);
          await tx.delete(this.prefix + "pending/" + id);
          return { state: "unknown" };
        }
        if (row.state !== "pending") return { state: row.state };
        const claimed = { ...row, state: "sending", attempt, claim_until: now + 60000 };
        await tx.put(key, claimed);
        return { payload: row.payload };
      });
    if (claim.busy) throw Error("OutboxSendInProgress");
    if (!claim.payload) return claim;
    let state = "unknown",
      provider_id = null;
    try {
      const result = await send(claim.payload);
      if (typeof result?.messageId === "string" && result.messageId.length <= 512) {
        state = "accepted";
        provider_id = result.messageId;
      }
    } catch (error) {
      if (
        [
          "E_VALIDATION_ERROR",
          "E_FIELD_MISSING",
          "E_SENDER_NOT_VERIFIED",
          "E_RECIPIENT_NOT_ALLOWED",
          "E_RECIPIENT_SUPPRESSED",
          "E_SENDER_DOMAIN_NOT_AVAILABLE",
          "E_ACCOUNT_SUSPENDED",
          "E_CONTENT_TOO_LARGE",
        ].includes(error?.code)
      )
        state = "rejected";
    }
    return this.store.transaction(async (tx) => {
      const key = this.key(id),
        row = await tx.get(key);
      if (row?.attempt !== attempt || !["sending", "unknown"].includes(row.state))
        throw Error("OutboxClaimChanged");
      const done = { ...row, state, provider_id, finished_at: this.clock() };
      await tx.put(key, done);
      await tx.delete(this.prefix + "pending/" + id);
      return { state, provider_id };
    });
  }
  async list(cursor = "") {
    if (
      typeof cursor !== "string" ||
      (cursor && (!cursor.startsWith(this.prefix) || !uuid(cursor.slice(this.prefix.length))))
    )
      throw Error("InvalidOutboxCursor");
    return this.store.transaction(async (tx) => {
      const page = await tx.list({ prefix: this.prefix, startAfter: cursor, limit: 11 }),
        rows = [...page].filter(([key]) => uuid(key.slice(this.prefix.length)));
      return {
        messages: rows
          .slice(0, 10)
          .map(([, r]) => ({
            id: r.id,
            state: r.state,
            created_at: r.created_at,
            provider_id: r.provider_id ?? null,
            ...r.payload,
          })),
        cursor: rows.length > 10 ? rows[9][0] : null,
      };
    });
  }
  async pending(limit = 16) {
    return this.store.transaction(async (tx) => [
      ...(await tx.list({ prefix: this.prefix + "pending/", limit })).values(),
    ]);
  }
}
// Append-only per-mailbox change log for offline replicas (the PWA's local
// index). Every indexed message gets exactly one monotonically increasing
// sequence; a replica stores the last sequence it applied as its cursor.
// Messages indexed before the log existed are appended by a bounded backfill
// that walks the received-order index, so the log converges for old mailboxes.
export const syncLimits = Object.freeze({ pageMessages: 25, textBytes: 64 * 1024, backfill: 50 });
const SEQUENCE_DIGITS = 13;
export class MailSyncLog {
  constructor(store, { prefix = "mail/v1/sync/" } = {}) {
    this.store = store;
    this.prefix = prefix;
  }
  entryKey(sequence) {
    return this.prefix + "log/" + String(sequence).padStart(SEQUENCE_DIGITS, "0");
  }
  cursorSequence(cursor) {
    if (cursor === "" || cursor == null) return 0;
    if (typeof cursor !== "string" || !/^\d{1,13}$/.test(cursor)) throw Error("InvalidSyncCursor");
    return Number(cursor);
  }
  /** Append `id` once. Call inside the transaction that makes it searchable. */
  async record(tx, id) {
    if (!uuid(id)) throw Error("InvalidMessageIdentity");
    const marker = this.prefix + "id/" + id;
    if (await tx.get(marker)) return false;
    const sequence = ((await tx.get(this.prefix + "head")) ?? 0) + 1;
    await tx.put(this.prefix + "head", sequence);
    await tx.put(this.entryKey(sequence), id);
    await tx.put(marker, sequence);
    return true;
  }
  /** Append `id` again (e.g. restored from Trash) so replicas re-add it. */
  async recordUpsert(tx, id) {
    if (!uuid(id)) throw Error("InvalidMessageIdentity");
    const sequence = ((await tx.get(this.prefix + "head")) ?? 0) + 1;
    await tx.put(this.prefix + "head", sequence);
    await tx.put(this.entryKey(sequence), id);
    await tx.put(this.prefix + "id/" + id, sequence);
    return sequence;
  }
  /** Append a deletion tombstone for `id`. Call inside the deleting transaction. */
  async recordDelete(tx, id) {
    if (!uuid(id)) throw Error("InvalidMessageIdentity");
    const sequence = ((await tx.get(this.prefix + "head")) ?? 0) + 1;
    await tx.put(this.prefix + "head", sequence);
    await tx.put(this.entryKey(sequence), { id, deleted: true });
    return sequence;
  }
  /** Append up to `limit` pre-existing messages; true when none remain. */
  async backfill(limit = syncLimits.backfill) {
    return this.store.transaction(async (tx) => {
      const state = (await tx.get(this.prefix + "backfill")) ?? { after: "", done: false };
      if (state.done) return true;
      const page = await tx.list({ prefix: "mail/v1/order/", startAfter: state.after, limit });
      for (const [key, id] of page) {
        await this.record(tx, id);
        state.after = key;
      }
      state.done = page.size < limit;
      await tx.put(this.prefix + "backfill", state);
      return state.done;
    });
  }
  /** Message identities after `cursor`, oldest first, with the next cursor. */
  async page(cursor = "", limit = syncLimits.pageMessages) {
    const after = this.cursorSequence(cursor);
    if (!Number.isInteger(limit) || limit < 1 || limit > 100) throw Error("InvalidSyncLimit");
    return this.store.transaction(async (tx) => {
      const head = (await tx.get(this.prefix + "head")) ?? 0;
      // A cursor past the head means the log was restored or rebuilt since the
      // replica synced: tell it to rebuild from the start, not fail forever.
      if (after > head) return { ids: [], cursor: "", head, more: head > 0, reset: true };
      const page = await tx.list({
        prefix: this.prefix + "log/",
        startAfter: after ? this.entryKey(after) : "",
        limit,
      });
      const ids = [];
      const deleted = [];
      let last = after;
      for (const [key, entry] of page) {
        if (typeof entry === "string") ids.push(entry);
        else if (entry?.deleted && uuid(entry.id)) deleted.push(entry.id);
        last = Number(key.slice(this.prefix.length + 4));
      }
      return { ids, deleted, cursor: String(last), head, more: last < head };
    });
  }
}
/** The replica view of a message: text only, bounded, no attachment bytes. */
export function syncMessage(message) {
  const bytes = new TextEncoder().encode(message.text ?? "");
  const truncated = bytes.length > syncLimits.textBytes;
  const text = truncated
    ? new TextDecoder().decode(bytes.slice(0, syncLimits.textBytes)).replace(/�$/, "")
    : (message.text ?? "");
  return {
    id: message.id,
    thread_id: message.thread_id ?? message.id,
    subject: message.subject ?? "",
    from: message.from ?? "",
    to: message.to ?? "",
    received_at: message.received_at ?? 0,
    quarantined: message.quarantined === true,
    attachments: (message.attachments ?? []).map(({ index, filename, size }) => ({ index, filename, size })),
    text,
    text_truncated: truncated,
  };
}

// Objects in private R2 that belong to exactly one message and can be removed
// with it. Deliveries now store raw MIME and attachments under the receipt id;
// older content-addressed objects ("raw/<sha256>", "attachments/<sha256>") may
// be shared by identical deliveries, so deletion leaves them in place.
export function ownedObjects(prefix, receipt) {
  if (!uuid(receipt?.id)) return [];
  const keys = [];
  if (receipt.key === prefix + "raw/" + receipt.id) keys.push(receipt.key);
  const projection = receipt.projection?.key;
  if (typeof projection === "string" && projection.startsWith(prefix + "projections/")) keys.push(projection);
  for (const part of receipt.prepared?.attachments ?? [])
    if (typeof part?.key === "string" && part.key.startsWith(prefix + "attachments/" + receipt.id + "/")) keys.push(part.key);
  return keys;
}
const orderKey = (receipt) =>
  "mail/v1/order/" + String(9999999999999 - receipt.received_at).padStart(13, "0") + "/" + receipt.id;
const pad = (n) => String(n).padStart(13, "0");
// Trash is listed newest-trashed first and expires oldest-first.
const trashKeys = (receipt) => [
  "mail/v1/trash/" + pad(9999999999999 - receipt.trashed_at) + "/" + receipt.id,
  "mail/v1/trash-expiry/" + pad(receipt.trashed_at) + "/" + receipt.id,
];
export const DAY_MS = 24 * 60 * 60 * 1000;

// Gmail-style Trash. Moving a message to Trash hides it from the inbox, search
// and device copies (a sync tombstone) but keeps it, with its search document
// and objects, so it can be opened or restored. Messages left in Trash longer
// than the retention period are deleted forever by the scheduled sweep, through
// MailDeletion. Restoring puts the message back in the inbox and re-announces it
// to replicas, which embed it again.
export class MailTrash {
  constructor(store, syncLog, { clock = Date.now, retentionDays = 30, prefix = "mail/v1/" } = {}) {
    if (!Number.isInteger(retentionDays) || retentionDays < 1 || retentionDays > 3650)
      throw Error("InvalidTrashRetention");
    this.store = store;
    this.syncLog = syncLog;
    this.clock = clock;
    this.retentionMs = retentionDays * DAY_MS;
    this.retentionDays = retentionDays;
    this.prefix = prefix;
  }
  receiptKey(id) {
    if (!uuid(id)) throw Error("InvalidMessageIdentity");
    return this.prefix + "receipt/" + id;
  }
  /** Move an inbox message to Trash; returns its expiry time. Idempotent. */
  async trash(id) {
    return this.store.transaction(async (tx) => {
      const receipt = await tx.get(this.receiptKey(id));
      if (!receipt || receipt.state === "deleted") throw Error("UnknownMessage");
      if (receipt.state !== "indexed") throw Error("MessageNotReady");
      if (Number.isFinite(receipt.trashed_at)) return receipt.trashed_at + this.retentionMs;
      const trashed = { ...receipt, trashed_at: this.clock() };
      await tx.delete(orderKey(receipt));
      for (const key of trashKeys(trashed)) await tx.put(key, id);
      await tx.put(this.receiptKey(id), trashed);
      await this.syncLog.recordDelete(tx, id);
      return trashed.trashed_at + this.retentionMs;
    });
  }
  /** Return a trashed message to the inbox. Idempotent. */
  async restore(id) {
    return this.store.transaction(async (tx) => {
      const receipt = await tx.get(this.receiptKey(id));
      if (!receipt || receipt.state === "deleted") throw Error("UnknownMessage");
      if (!Number.isFinite(receipt.trashed_at)) return false;
      for (const key of trashKeys(receipt)) await tx.delete(key);
      const { trashed_at: _, ...restored } = receipt;
      await tx.put(this.receiptKey(id), restored);
      await tx.put(orderKey(restored), id);
      await this.syncLog.recordUpsert(tx, id);
      return true;
    });
  }
  /** Trashed-at time, or null if the message is not in Trash. */
  async trashedAt(id) {
    if (!uuid(id)) return null;
    const receipt = await this.store.transaction((tx) => tx.get(this.receiptKey(id)));
    return receipt?.state !== "deleted" && Number.isFinite(receipt?.trashed_at) ? receipt.trashed_at : null;
  }
  expiresAt(trashedAt) {
    return trashedAt + this.retentionMs;
  }
  /** One page of Trash, most recently trashed first. */
  async list(cursor = "", limit = 10) {
    if (typeof cursor !== "string" || (cursor && !/^mail\/v1\/trash\/\d{13}\/[a-f0-9-]{36}$/.test(cursor)))
      throw Error("InvalidMailCursor");
    return this.store.transaction(async (tx) => {
      const page = await tx.list({ prefix: this.prefix + "trash/", startAfter: cursor, limit: limit + 1 });
      const entries = [...page].slice(0, limit);
      return { ids: entries.map(([, id]) => id), cursor: page.size > limit ? entries.at(-1)[0] : null };
    });
  }
  /** Trashed ids whose retention has elapsed, oldest first. */
  async expired(limit = 25, now = this.clock()) {
    return this.store.transaction(async (tx) => {
      const page = await tx.list({ prefix: this.prefix + "trash-expiry/", limit });
      const cutoff = now - this.retentionMs;
      return [...page].filter(([key]) => Number(key.split("/")[3]) <= cutoff).map(([, id]) => id);
    });
  }
  /** Any trashed ids (oldest first), for emptying Trash in bounded steps. */
  async all(limit = 25) {
    return this.store.transaction(async (tx) => [
      ...(await tx.list({ prefix: this.prefix + "trash-expiry/", limit })).values(),
    ]);
  }
}

// Deleting a message is a transactional mark followed by an idempotent purge.
// The mark hides the message everywhere at once (list order, lookups, search
// results) and appends a sync tombstone so on-device copies drop it too; the
// purge removes the search document and the message's own R2 objects, and is
// retried from a durable marker until it completes.
export class MailDeletion {
  constructor(store, syncLog, { clock = Date.now, prefix = "mail/v1/" } = {}) {
    this.store = store;
    this.syncLog = syncLog;
    this.clock = clock;
    this.prefix = prefix;
  }
  receiptKey(id) {
    if (!uuid(id)) throw Error("InvalidMessageIdentity");
    return this.prefix + "receipt/" + id;
  }
  /** Returns the receipt (with object keys) to purge, or null if already deleted. */
  async mark(id) {
    return this.store.transaction(async (tx) => {
      const receipt = await tx.get(this.receiptKey(id));
      if (!receipt) throw Error("UnknownMessage");
      if (receipt.state === "deleted") return null;
      if (Number.isFinite(receipt.received_at)) await tx.delete(orderKey(receipt));
      if (Number.isFinite(receipt.trashed_at)) for (const key of trashKeys(receipt)) await tx.delete(key);
      await tx.delete(this.prefix + "pending/" + id);
      await tx.put(this.receiptKey(id), { ...receipt, state: "deleted", deleted_at: this.clock() });
      await tx.put(this.prefix + "purge/" + id, id);
      await this.syncLog.recordDelete(tx, id);
      return receipt;
    });
  }
  async isDeleted(id) {
    if (!uuid(id)) return true;
    const receipt = await this.store.transaction((tx) => tx.get(this.receiptKey(id)));
    return receipt?.state === "deleted";
  }
  /** Receipt still holding object keys, while its purge is outstanding. */
  async purging(id) {
    return this.store.transaction(async (tx) => {
      if (!(await tx.get(this.prefix + "purge/" + id))) return null;
      return (await tx.get(this.receiptKey(id))) ?? null;
    });
  }
  /**
   * Re-arm the purge (e.g. an ingest indexed a message after it was deleted).
   * Each attempt gets a new number: purge mutations are idempotent by
   * operation id, so a repeated id would not delete a re-indexed document.
   */
  async requirePurge(tx, id) {
    const receipt = await tx.get(this.receiptKey(id));
    if (receipt) await tx.put(this.receiptKey(id), { ...receipt, purge_attempt: (receipt.purge_attempt ?? 0) + 1 });
    await tx.put(this.prefix + "purge/" + id, id);
  }
  async pending(limit = 16) {
    return this.store.transaction(async (tx) => [
      ...(await tx.list({ prefix: this.prefix + "purge/", limit })).values(),
    ]);
  }
  /** Drop the marker and keep only a small tombstone receipt. */
  async finish(id) {
    return this.store.transaction(async (tx) => {
      const receipt = await tx.get(this.receiptKey(id));
      await tx.delete(this.prefix + "purge/" + id);
      if (receipt)
        await tx.put(this.receiptKey(id), {
          id,
          state: "deleted",
          deleted_at: receipt.deleted_at,
          purge_attempt: receipt.purge_attempt ?? 0,
        });
    });
  }
}
