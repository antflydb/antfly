// Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Elastic-2.0
//! Durable producer work, independent of a worker lease or local queue. Rows
//! touched while the baseline scan runs create obligations in the same write
//! transaction, so moving the scan cursor never certifies a missed mutation.
const std = @import("std");
const publication = @import("artifact_publication.zig");
const Position = publication.Position;
pub const key = "\x00\x00__artifact_publication__:obligations";
pub const document_prefix = "\x00\x00__artifact_publication__:dirty:";
pub const max_cursor_bytes = 1024 * 1024;

pub const State = struct {
    namespace: publication.Namespace,
    epoch: u64,
    pending_documents: u64 = 0,
    /// Owner-local monotonic work identity, not a replicated source position.
    /// Never reset on completion: dependency-only and delete/recreate work
    /// must not admit an older scheduler/completion observation.
    work_revision: u64 = 0,
    baseline_complete: bool = false,
    /// A primary store key, not a logical document key. Borrowed from `load`.
    cursor: []const u8 = "",
    sealed_attempt: ?[32]u8 = null,

    pub fn requireAuthority(self: State, authority: publication.Authority) !void {
        if (self.epoch != authority.epoch or !std.mem.eql(u8, &self.namespace, &authority.namespace)) return error.ArtifactCatalogDrift;
    }
};

pub fn load(txn: anytype) !?State {
    const raw = txn.get(key) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    if (raw.len < 122 or raw.len > 122 + max_cursor_bytes or !std.mem.eql(u8, raw[0..4], "APO2") or raw[44] > 1 or raw[45] > 1) return error.ArtifactCatalogCorrupt;
    const cursor_len = std.mem.readInt(u32, raw[78..82], .little);
    if (raw.len != 122 + @as(usize, cursor_len)) return error.ArtifactCatalogCorrupt;
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(raw[0 .. raw.len - 32], &digest, .{});
    if (!std.mem.eql(u8, &digest, raw[raw.len - 32 ..])) return error.ArtifactCatalogCorrupt;
    const result: State = .{ .namespace = raw[4..28].*, .epoch = std.mem.readInt(u64, raw[28..36], .little), .pending_documents = std.mem.readInt(u64, raw[36..44], .little), .baseline_complete = raw[44] == 1, .sealed_attempt = if (raw[45] == 1) raw[46..78].* else null, .work_revision = std.mem.readInt(u64, raw[82..90], .little), .cursor = raw[90 .. 90 + cursor_len] };
    if (result.epoch == 0 or result.pending_documents > result.work_revision or std.mem.allEqual(u8, &result.namespace, 0) or
        (result.sealed_attempt != null and (!result.baseline_complete or result.pending_documents != 0)) or
        (raw[45] == 0 and !std.mem.allEqual(u8, raw[46..78], 0))) return error.ArtifactCatalogCorrupt;
    return result;
}

pub fn store(alloc: std.mem.Allocator, txn: anytype, state: State) !void {
    if (state.cursor.len > max_cursor_bytes or state.epoch == 0) return error.InvalidBatchRequest;
    const raw = try alloc.alloc(u8, 122 + state.cursor.len);
    defer alloc.free(raw);
    @memset(raw, 0);
    @memcpy(raw[0..4], "APO2");
    @memcpy(raw[4..28], &state.namespace);
    std.mem.writeInt(u64, raw[28..36], state.epoch, .little);
    std.mem.writeInt(u64, raw[36..44], state.pending_documents, .little);
    raw[44] = @intFromBool(state.baseline_complete);
    if (state.sealed_attempt) |attempt| {
        raw[45] = 1;
        @memcpy(raw[46..78], &attempt);
    }
    std.mem.writeInt(u32, raw[78..82], @intCast(state.cursor.len), .little);
    std.mem.writeInt(u64, raw[82..90], state.work_revision, .little);
    @memcpy(raw[90 .. 90 + state.cursor.len], state.cursor);
    std.crypto.hash.sha2.Sha256.hash(raw[0 .. raw.len - 32], raw[raw.len - 32 ..][0..32], .{});
    try txn.put(key, raw);
}

pub fn begin(alloc: std.mem.Allocator, txn: anytype, authority: publication.Authority) !void {
    if (try load(txn)) |existing| {
        if (existing.epoch == authority.epoch and std.mem.eql(u8, &existing.namespace, &authority.namespace)) return;
        if (existing.sealed_attempt != null) return error.ArtifactCatalogDrift;
    }
    try store(alloc, txn, .{ .namespace = authority.namespace, .epoch = authority.epoch });
}

pub fn workPrefix(authority: publication.Authority) [document_prefix.len + 32]u8 {
    var prefix: [document_prefix.len + 32]u8 = undefined;
    @memcpy(prefix[0..document_prefix.len], document_prefix);
    @memcpy(prefix[document_prefix.len..][0..24], &authority.namespace);
    std.mem.writeInt(u64, prefix[prefix.len - 8 ..], authority.epoch, .big);
    return prefix;
}

fn documentKey(alloc: std.mem.Allocator, authority: publication.Authority, document: []const u8) ![]u8 {
    if (document.len > max_cursor_bytes) return error.InvalidBatchRequest;
    return std.mem.concat(alloc, u8, &.{ &workPrefix(authority), document });
}

const Record = struct { epoch: u64, position: ?Position, revision: u64, dispatch_sequence: u64 = 0, next_template: u32 = 0, dispatch_complete: bool = false, retry_round: u64 = 0, retry_sequence: u64 = 0, retry_next_template: u32 = 0 };
fn decodeRecord(raw: []const u8) !Record {
    if (raw.len != 116 or raw[61] > 1 or !std.mem.allEqual(u8, raw[62..64], 0)) return error.ArtifactCatalogCorrupt;
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(raw[0..84], &digest, .{});
    if (!std.mem.eql(u8, &digest, raw[84..116])) return error.ArtifactCatalogCorrupt;
    const epoch = std.mem.readInt(u64, raw[0..8], .little);
    const revision = std.mem.readInt(u64, raw[49..57], .little);
    const sequence = std.mem.readInt(u64, raw[41..49], .little);
    const next = std.mem.readInt(u32, raw[57..61], .little);
    if (epoch == 0 or revision == 0 or (sequence == 0) != (next == 0) or (raw[61] == 1 and next == 0)) return error.ArtifactCatalogCorrupt;
    const round = std.mem.readInt(u64, raw[64..72], .little);
    const retry_sequence = std.mem.readInt(u64, raw[72..80], .little);
    const retry_next = std.mem.readInt(u32, raw[80..84], .little);
    if ((round == 0 and (retry_sequence != 0 or retry_next != 0)) or (round != 0 and raw[61] == 0) or
        (retry_next != 0 and retry_next >= next) or (retry_sequence != 0 and retry_sequence <= sequence)) return error.ArtifactCatalogCorrupt;
    return .{ .epoch = epoch, .revision = revision, .position = if (std.mem.allEqual(u8, raw[8..41], 0)) null else Position.decode(raw[8..41]) catch return error.ArtifactCatalogCorrupt, .dispatch_sequence = sequence, .next_template = next, .dispatch_complete = raw[61] == 1, .retry_round = round, .retry_sequence = retry_sequence, .retry_next_template = retry_next };
}

fn encodeRecord(record: Record) ![116]u8 {
    var raw: [116]u8 = @splat(0);
    std.mem.writeInt(u64, raw[0..8], record.epoch, .little);
    if (record.position) |position| @memcpy(raw[8..41], &(try position.encode()));
    std.mem.writeInt(u64, raw[41..49], record.dispatch_sequence, .little);
    std.mem.writeInt(u64, raw[49..57], record.revision, .little);
    std.mem.writeInt(u32, raw[57..61], record.next_template, .little);
    raw[61] = @intFromBool(record.dispatch_complete);
    std.mem.writeInt(u64, raw[64..72], record.retry_round, .little);
    std.mem.writeInt(u64, raw[72..80], record.retry_sequence, .little);
    std.mem.writeInt(u32, raw[80..84], record.retry_next_template, .little);
    std.crypto.hash.sha2.Sha256.hash(raw[0..84], raw[84..116], .{});
    return raw;
}

/// Returns false only when this owner has not activated obligation capture.
/// No old value/artifact scan is performed; one document causes two point IOs.
pub fn mark(alloc: std.mem.Allocator, txn: anytype, authority: publication.Authority, document: []const u8, position: ?Position) !bool {
    var state = (try load(txn)) orelse return false;
    try state.requireAuthority(authority);
    if (state.sealed_attempt != null) return error.RetainedEffectsFenceMismatch;
    const dirty = try documentKey(alloc, authority, document);
    defer alloc.free(dirty);
    const previous = txn.get(dirty) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
    if (previous == null or (try decodeRecord(previous.?)).epoch != authority.epoch)
        state.pending_documents = std.math.add(u64, state.pending_documents, 1) catch return error.ArtifactCatalogCorrupt;
    state.work_revision = std.math.add(u64, state.work_revision, 1) catch return error.ResourceLimitExceeded;
    const record = try encodeRecord(.{ .epoch = authority.epoch, .position = position, .revision = state.work_revision });
    // Serialize the borrowed state before replacing any backend values.
    try store(alloc, txn, state);
    try txn.put(dirty, &record);
    return true;
}

/// Local replay scheduling is not producer acceptance. The caller must append
/// the journal record at `sequence` in this SAME transaction. A new primary or
/// dependency obligation resets this marker, even when the primary is unchanged.
pub const Dispatch = struct { next_template: u32, complete: bool };

const retry_round_key = "\x00\x00__artifact_publication__:retry-round";

fn retryRoundState(txn: anytype, authority: publication.Authority) !?struct { number: u64, started_ns: u64 } {
    const raw = txn.get(retry_round_key) catch |err| if (err == error.NotFound) return null else return err;
    if (raw.len != 84 or !std.mem.eql(u8, raw[0..4], "ARR2")) return error.ArtifactCatalogCorrupt;
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(raw[0..52], &digest, .{});
    if (!std.mem.eql(u8, &digest, raw[52..84])) return error.ArtifactCatalogCorrupt;
    const round = std.mem.readInt(u64, raw[36..44], .little);
    if (round == 0 or std.mem.readInt(u64, raw[28..36], .little) == 0 or std.mem.allEqual(u8, raw[4..28], 0)) return error.ArtifactCatalogCorrupt;
    if (!std.mem.eql(u8, &authority.namespace, raw[4..28]) or authority.epoch != std.mem.readInt(u64, raw[28..36], .little)) return null;
    return .{ .number = round, .started_ns = std.mem.readInt(u64, raw[44..52], .little) };
}

/// Persistent scheduling time prevents repeated cold-owner eviction from
/// granting an endless new grace interval. Clock failure/regression permits
/// an early retry, never acceptance or obligation discharge.
pub fn retryDelay(txn: anytype, authority: publication.Authority, now_ns: u64, interval_ns: u64) !u64 {
    const previous = (try retryRoundState(txn, authority)) orelse return 0;
    if (now_ns == 0 or previous.started_ns == 0 or now_ns < previous.started_ns) return 0;
    return interval_ns -| (now_ns - previous.started_ns);
}

/// One durable scheduling generation per retry sweep, not a receipt or a
/// completion epoch. Local clocks only select when to begin; this counter
/// prevents restart/clock changes from reusing a completed scheduling round.
pub fn beginRetryRound(txn: anytype, authority: publication.Authority) !u64 {
    return beginRetryRoundAt(txn, authority, 0);
}

pub fn beginRetryRoundAt(txn: anytype, authority: publication.Authority, now_ns: u64) !u64 {
    if (!std.meta.eql((try publication.authority(txn)) orelse return error.ArtifactCatalogDrift, authority)) return error.ArtifactCatalogDrift;
    const state = (try load(txn)) orelse return error.ArtifactCatalogCorrupt;
    try state.requireAuthority(authority);
    if (state.sealed_attempt != null) return error.RetainedEffectsFenceMismatch;
    const previous = try retryRoundState(txn, authority);
    const next = std.math.add(u64, if (previous) |value| value.number else 0, 1) catch return error.ResourceLimitExceeded;
    var raw: [84]u8 = undefined;
    @memcpy(raw[0..4], "ARR2");
    @memcpy(raw[4..28], &authority.namespace);
    std.mem.writeInt(u64, raw[28..36], authority.epoch, .little);
    std.mem.writeInt(u64, raw[36..44], next, .little);
    std.mem.writeInt(u64, raw[44..52], now_ns, .little);
    std.crypto.hash.sha2.Sha256.hash(raw[0..52], raw[52..84], .{});
    try txn.put(retry_round_key, &raw);
    return next;
}

/// Atomically advance a bounded retry page alongside its journal append.
/// A null sequence is permitted only for a page with no pending requests;
/// its caller records metadata alone. Completion evidence is never modified.
pub fn stageRetry(alloc: std.mem.Allocator, txn: anytype, authority: publication.Authority, item: WorkPage.Item, round: u64, sequence: ?u64, page: Dispatch) !bool {
    const first = if (item.retry_round == round) item.retry_next_template else 0;
    if (round == 0 or !item.dispatch_complete or (item.retry_round == round and item.retry_next_template == 0) or page.next_template <= first or (sequence != null and sequence.? == 0)) return error.InvalidBatchRequest;
    const current = (try lookupWork(alloc, txn, authority, item.document)) orelse return false;
    const active_round = (try retryRoundState(txn, authority)) orelse return false;
    if (active_round.number != round or current.revision != item.revision or !std.meta.eql(current.position, item.position) or
        !current.dispatch_complete or current.retry_round != item.retry_round or current.retry_sequence != item.retry_sequence or current.retry_next_template != item.retry_next_template) return false;
    // The initial cursor covers the same immutable template inventory.
    if (page.next_template > current.next_template or page.complete != (page.next_template == current.next_template)) return error.InvalidBatchRequest;
    if (sequence) |value| if (value <= current.retry_sequence or value <= current.dispatch_sequence) return error.InvalidBatchRequest;
    const selected = try documentKey(alloc, authority, item.document);
    defer alloc.free(selected);
    var record = try decodeRecord(try txn.get(selected));
    record.retry_round = round;
    record.retry_sequence = sequence orelse record.retry_sequence;
    record.retry_next_template = if (page.complete) 0 else page.next_template;
    try txn.put(selected, &try encodeRecord(record));
    return true;
}

pub fn stageDispatch(alloc: std.mem.Allocator, txn: anytype, authority: publication.Authority, item: WorkPage.Item, sequence: u64, page: Dispatch) !bool {
    if (sequence == 0 or page.next_template <= item.next_template) return error.InvalidBatchRequest;
    const current = (try publication.authority(txn)) orelse return error.ArtifactCatalogDrift;
    if (!std.meta.eql(current, authority)) return error.ArtifactCatalogDrift;
    const state = (try load(txn)) orelse return error.ArtifactCatalogCorrupt;
    try state.requireAuthority(authority);
    if (state.sealed_attempt != null) return error.RetainedEffectsFenceMismatch;
    const dirty = try documentKey(alloc, authority, item.document);
    defer alloc.free(dirty);
    const raw = txn.get(dirty) catch |err| switch (err) {
        error.NotFound => return false,
        else => return err,
    };
    var record = try decodeRecord(raw);
    if (record.epoch != authority.epoch or record.revision != item.revision or !std.meta.eql(record.position, item.position) or
        record.dispatch_complete or record.next_template != item.next_template or record.dispatch_sequence != item.dispatch_sequence) return false;
    record.dispatch_sequence = sequence;
    record.next_template = page.next_template;
    record.dispatch_complete = page.complete;
    try txn.put(dirty, &try encodeRecord(record));
    return true;
}

/// Caller has checked every required producer receipt in this same writer
/// snapshot. This token is owner-local: a replicated completion must capture
/// and validate each receiver's current work, never copy a leader's revision.
/// Stale completion cannot clear later primary OR dependency-only work.
pub fn complete(alloc: std.mem.Allocator, txn: anytype, authority: publication.Authority, item: WorkPage.Item) !void {
    const active = (try publication.authority(txn)) orelse return error.ArtifactCatalogDrift;
    if (!std.meta.eql(active, authority)) return error.ArtifactCatalogDrift;
    var state = (try load(txn)) orelse return error.ArtifactCatalogDrift;
    try state.requireAuthority(authority);
    if (state.sealed_attempt != null) return error.ArtifactCatalogDrift;
    const dirty = try documentKey(alloc, authority, item.document);
    defer alloc.free(dirty);
    const raw = txn.get(dirty) catch |err| switch (err) {
        error.NotFound => return,
        else => return err,
    };
    const record = try decodeRecord(raw);
    if (record.epoch != authority.epoch or record.revision != item.revision or !std.meta.eql(record.position, item.position)) return;
    state.pending_documents = std.math.sub(u64, state.pending_documents, 1) catch return error.ArtifactCatalogCorrupt;
    try store(alloc, txn, state);
    try txn.delete(dirty);
}

pub fn requireDrained(txn: anytype, authority: publication.Authority) !State {
    const state = (try load(txn)) orelse return error.ArtifactCatalogDrift;
    try state.requireAuthority(authority);
    if (!state.baseline_complete or state.pending_documents != 0) return error.ArtifactCatalogDrift;
    return state;
}

/// Owned scheduler input, never a completion certificate. Release the read
/// snapshot before dispatching work; workers still revalidate each exact
/// input and clear obligations only through the ordered completion path.
pub const WorkPage = struct {
    arena: std.heap.ArenaAllocator,
    authority: publication.Authority,
    items: []const Item,
    next_document: ?[]const u8,
    at_end: bool,

    pub const Item = struct { document: []const u8, position: ?Position, revision: u64, dispatch_sequence: u64 = 0, next_template: u32 = 0, dispatch_complete: bool = false, retry_round: u64 = 0, retry_sequence: u64 = 0, retry_next_template: u32 = 0 };
    pub fn deinit(self: *WorkPage) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

pub const WorkCursor = struct { authority: publication.Authority, document: []const u8 };

/// Point lookup for completion admission. The returned document identity is
/// borrowed from the caller, not the transaction. Dispatch bookkeeping is not
/// acceptance and does not change the underlying obligation revision.
pub fn lookupWork(alloc: std.mem.Allocator, txn: anytype, authority: publication.Authority, document: []const u8) !?WorkPage.Item {
    const active = (try publication.authority(txn)) orelse return error.ArtifactCatalogDrift;
    if (!std.meta.eql(active, authority)) return error.ArtifactCatalogDrift;
    const state = (try load(txn)) orelse return error.ArtifactCatalogDrift;
    try state.requireAuthority(authority);
    if (state.sealed_attempt != null) return error.ArtifactCatalogDrift;
    const selected = try documentKey(alloc, authority, document);
    defer alloc.free(selected);
    const raw = txn.get(selected) catch |err| if (err == error.NotFound) return null else return err;
    const record = try decodeRecord(raw);
    if (record.epoch != authority.epoch or record.revision > state.work_revision) return error.ArtifactCatalogCorrupt;
    if (record.position) |value| value.requireNamespace(publication.namespaceFromBytes(authority.namespace)) catch return error.ArtifactCatalogCorrupt;
    return .{ .document = document, .position = record.position, .revision = record.revision, .dispatch_sequence = record.dispatch_sequence, .next_template = record.next_template, .dispatch_complete = record.dispatch_complete, .retry_round = record.retry_round, .retry_sequence = record.retry_sequence, .retry_next_template = record.retry_next_template };
}

/// Old work is never consumed by a new epoch. Retire it locally in bounded
/// transactions; this changes neither active counts nor any completion proof.
/// Readers pin immutable snapshots, so reclamation cannot invalidate a reader
/// that captured the preceding authority. No history-sized allocation or
/// replicated GC cursor is required: each committed page removes its prefix.
pub fn collectObsoleteWorkPage(alloc: std.mem.Allocator, store_handle: anytype) !bool {
    return collectObsoleteEpochPage(alloc, store_handle, document_prefix, 0, max_cursor_bytes);
}

/// Shared bounded reclamation for receiver-local registries whose key is
/// prefix + namespace + big-endian epoch + identity. Active rows and reader
/// snapshots remain untouched; no history-sized metadata allocation is needed.
pub fn collectObsoleteEpochPage(alloc: std.mem.Allocator, store_handle: anytype, comptime registry_prefix: []const u8, comptime min_identity_bytes: usize, comptime max_identity_bytes: usize) !bool {
    return collectObsoleteEpochPageForIdentity(alloc, store_handle, registry_prefix, min_identity_bytes, max_identity_bytes, null);
}

/// An optional receiver identity prefix also retires imported checkpoints in
/// the current epoch. Seek over the entire live identity, rather than scanning
/// it on every cleanup page. Registries without local identity keep all current
/// epoch rows, preserving the ordinary obligation-GC contract.
pub fn collectObsoleteEpochPageForIdentity(alloc: std.mem.Allocator, store_handle: anytype, comptime registry_prefix: []const u8, comptime min_identity_bytes: usize, comptime max_identity_bytes: usize, active_identity: ?[]const u8) !bool {
    comptime std.debug.assert(std.mem.startsWith(u8, registry_prefix, "\x00\x00__artifact_publication__:"));
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const scratch = arena.allocator();
    var retired: std.ArrayList([]const u8) = .empty;
    var at_end = false;
    const authority = blk: {
        var read = try store_handle.beginReadTxnWithBlockCacheAdmission(.transient);
        defer read.abort();
        const authority = (try publication.authority(&read)) orelse return true;
        const prefix = try std.mem.concat(scratch, u8, &.{ registry_prefix, &authority.namespace });
        var cursor = try read.openPhysicalCursorAdapter();
        defer cursor.close();
        var entry = try cursor.seekAtOrAfter(prefix);
        var bytes: usize = 0;
        const deadline = @import("antfly_platform").time.monotonicNs() +| 2 * std.time.ns_per_ms;
        while (entry) |item| {
            if (!std.mem.startsWith(u8, item.key, prefix)) {
                entry = null;
                break;
            }
            if (item.key.len < prefix.len + 8 + min_identity_bytes or item.key.len > prefix.len + 8 + max_identity_bytes) return error.ArtifactCatalogCorrupt;
            const epoch = std.mem.readInt(u64, item.key[prefix.len..][0..8], .big);
            if (epoch == 0) return error.ArtifactCatalogCorrupt;
            if (epoch > authority.epoch or (epoch == authority.epoch and active_identity == null)) {
                entry = null;
                break;
            }
            if (epoch == authority.epoch) if (active_identity) |identity| {
                if (identity.len == 0 or identity.len > min_identity_bytes) return error.InvalidArgument;
                if (std.mem.startsWith(u8, item.key[prefix.len + 8 ..], identity)) {
                    const upper = try @import("../internal_keys.zig").nextPrefixAlloc(scratch, item.key[0 .. prefix.len + 8 + identity.len]);
                    entry = if (upper) |next| try cursor.seekAtOrAfter(next) else null;
                    continue;
                }
            };
            if (retired.items.len != 0 and (retired.items.len >= 128 or bytes >= 64 * 1024 or item.key.len > 64 * 1024 - bytes or @import("antfly_platform").time.monotonicNs() >= deadline)) break;
            try retired.append(scratch, try scratch.dupe(u8, item.key));
            bytes += item.key.len;
            entry = try cursor.next();
        }
        at_end = entry == null;
        break :blk authority;
    };
    if (retired.items.len == 0) return at_end;
    var write = try store_handle.beginWriteTxn();
    errdefer write.abort();
    const current = (try publication.authority(&write)) orelse return error.ArtifactCatalogDrift;
    if (!std.meta.eql(current, authority)) return error.ArtifactCatalogDrift;
    for (retired.items) |retired_key| write.delete(retired_key) catch |err| switch (err) {
        error.NotFound => {},
        else => return err,
    };
    try write.commit();
    return at_end;
}

/// One prefix seek, at most 128 records and 64 KiB of document keys (one
/// oversized key may advance). Old epochs have disjoint prefixes, so catalog
/// churn cannot make current scheduling scan a history-sized dirty queue.
/// `after` is exclusive and belongs to this exact authority. At end the caller
/// wraps to null so a pending low key cannot starve later keys, or vice versa.
pub fn scanWork(alloc: std.mem.Allocator, txn: anytype, authority: publication.Authority, after: ?WorkCursor) !WorkPage {
    const active = (try publication.authority(txn)) orelse return error.ArtifactCatalogDrift;
    if (!std.meta.eql(active, authority)) return error.ArtifactCatalogDrift;
    const state = (try load(txn)) orelse return error.ArtifactCatalogCorrupt;
    try state.requireAuthority(authority);
    if (after) |value| {
        if (!std.meta.eql(value.authority, authority)) return error.ArtifactCatalogDrift;
        if (value.document.len > max_cursor_bytes) return error.InvalidBatchRequest;
    }
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    const owned = arena.allocator();
    const prefix = workPrefix(authority);
    const seek = if (after) |value| try documentKey(owned, authority, value.document) else &prefix;
    var cursor = try txn.openPhysicalCursorAdapter();
    defer cursor.close();
    var entry = try cursor.seekAtOrAfter(seek);
    if (after != null) if (entry) |item| if (std.mem.eql(u8, item.key, seek)) {
        entry = try cursor.next();
    };
    var items: std.ArrayList(WorkPage.Item) = .empty;
    var bytes: usize = 0;
    const deadline = @import("antfly_platform").time.monotonicNs() +| 2 * std.time.ns_per_ms;
    while (entry) |item| {
        if (!std.mem.startsWith(u8, item.key, &prefix)) {
            entry = null;
            break;
        }
        const document = item.key[prefix.len..];
        if (document.len > max_cursor_bytes) return error.ArtifactCatalogCorrupt;
        if (items.items.len != 0 and (items.items.len >= 128 or bytes >= 64 * 1024 or document.len > 64 * 1024 - bytes or @import("antfly_platform").time.monotonicNs() >= deadline)) break;
        const record = try decodeRecord(item.value);
        if (record.epoch != authority.epoch or record.revision > state.work_revision) return error.ArtifactCatalogCorrupt;
        try items.append(owned, .{ .document = try owned.dupe(u8, document), .position = record.position, .revision = record.revision, .dispatch_sequence = record.dispatch_sequence, .next_template = record.next_template, .dispatch_complete = record.dispatch_complete, .retry_round = record.retry_round, .retry_sequence = record.retry_sequence, .retry_next_template = record.retry_next_template });
        bytes += document.len;
        entry = try cursor.next();
    }
    return .{ .arena = arena, .authority = authority, .items = items.items, .next_document = if (items.items.len == 0) null else items.items[items.items.len - 1].document, .at_end = entry == null };
}

/// Advance only the cursor captured by this bounded scan page. Foreground
/// mutations may change the pending count in between; preserve that live
/// count instead of overwriting it with a stale scan snapshot.
pub fn advanceBaseline(alloc: std.mem.Allocator, txn: anytype, authority: publication.Authority, expected_cursor: []const u8, next_cursor: []const u8, complete_scan: bool) !void {
    var state = (try load(txn)) orelse return error.ArtifactCatalogDrift;
    try state.requireAuthority(authority);
    if (state.baseline_complete or state.sealed_attempt != null or !std.mem.eql(u8, state.cursor, expected_cursor)) return error.ArtifactCatalogDrift;
    if (!complete_scan and std.mem.order(u8, next_cursor, expected_cursor) != .gt) return error.InvalidBatchRequest;
    state.baseline_complete = complete_scan;
    state.cursor = if (complete_scan) "" else next_cursor;
    try store(alloc, txn, state);
}

pub fn stageSeal(alloc: std.mem.Allocator, txn: anytype, authority: publication.Authority, attempt: [32]u8) !void {
    if (std.mem.allEqual(u8, &attempt, 0)) return error.InvalidBatchRequest;
    var state = try requireDrained(txn, authority);
    // Empty work queues alone cannot certify cross-document read sets. Pin
    // the completed, unchanged validation epoch in this same transaction.
    try @import("artifact_producer_validation.zig").requireComplete(txn, authority);
    if (state.sealed_attempt) |previous| if (!std.mem.eql(u8, &previous, &attempt)) return error.ArtifactCatalogDrift;
    state.sealed_attempt = attempt;
    try store(alloc, txn, state);
}

/// Only an authenticated cancellation of the same attempt may reopen writes.
pub fn cancelSeal(alloc: std.mem.Allocator, txn: anytype, authority: publication.Authority, attempt: [32]u8) !void {
    var state = (try load(txn)) orelse return error.ArtifactCatalogDrift;
    try state.requireAuthority(authority);
    const previous = state.sealed_attempt orelse return;
    if (!std.mem.eql(u8, &previous, &attempt)) return error.ArtifactCatalogDrift;
    state.sealed_attempt = null;
    try store(alloc, txn, state);
}

test "ordered artifact inventory retry records authenticate every cursor and sequence byte" {
    const record: Record = .{ .epoch = 1, .position = null, .revision = 2, .dispatch_sequence = 3, .next_template = 4, .dispatch_complete = true, .retry_round = 5, .retry_sequence = 6, .retry_next_template = 2 };
    const raw = try encodeRecord(record);
    try std.testing.expectEqualDeep(record, try decodeRecord(&raw));
    for (0..raw.len) |i| {
        var corrupt = raw;
        corrupt[i] ^= 1;
        try std.testing.expectError(error.ArtifactCatalogCorrupt, decodeRecord(&corrupt));
        try std.testing.expectError(error.ArtifactCatalogCorrupt, decodeRecord(raw[0..i]));
    }
    var invalid = record;
    invalid.retry_round = 0;
    try std.testing.expectError(error.ArtifactCatalogCorrupt, decodeRecord(&try encodeRecord(invalid)));
    invalid = record;
    invalid.dispatch_complete = false;
    try std.testing.expectError(error.ArtifactCatalogCorrupt, decodeRecord(&try encodeRecord(invalid)));
}

test "ordered artifact inventory obligations survive stale completion and seal only the drained exact attempt" {
    const alloc = std.testing.allocator;
    const Txn = struct {
        values: std.StringHashMap([]u8),
        pub fn get(self: *@This(), k: []const u8) anyerror![]const u8 {
            return self.values.get(k) orelse error.NotFound;
        }
        pub fn put(self: *@This(), k: []const u8, value: []const u8) !void {
            const owned = try std.testing.allocator.dupe(u8, value);
            errdefer std.testing.allocator.free(owned);
            if (self.values.getPtr(k)) |old| {
                std.testing.allocator.free(old.*);
                old.* = owned;
            } else {
                const owned_key = try std.testing.allocator.dupe(u8, k);
                errdefer std.testing.allocator.free(owned_key);
                try self.values.put(owned_key, owned);
            }
        }
        pub fn delete(self: *@This(), k: []const u8) !void {
            const removed = self.values.fetchRemove(k) orelse return error.NotFound;
            std.testing.allocator.free(removed.key);
            std.testing.allocator.free(removed.value);
        }
        fn deinit(self: *@This()) void {
            var iterator = self.values.iterator();
            while (iterator.next()) |entry| {
                std.testing.allocator.free(entry.key_ptr.*);
                std.testing.allocator.free(entry.value_ptr.*);
            }
            self.values.deinit();
        }
    };
    var txn: Txn = .{ .values = std.StringHashMap([]u8).init(alloc) };
    defer txn.deinit();
    const authority: publication.Authority = .{ .namespace = @splat(1), .epoch = 2, .catalog_digest = @splat(3) };
    const first: Position = .{ .raft = .{ .term = 1, .index = 8 } };
    const later: Position = .{ .raft = .{ .term = 1, .index = 9 } };
    try publication.stageAuthority(&txn, .{ .mode = .activate, .namespace = authority.namespace, .authority_epoch = authority.epoch, .catalog_digest = authority.catalog_digest, .producer_name = "", .producer_generation = 0, .sources = &.{}, .mutations = &.{}, .publication_digest = @splat(0) });
    try begin(alloc, &txn, authority);
    try std.testing.expect(try mark(alloc, &txn, authority, "doc\xff", first));
    const item: WorkPage.Item = .{ .document = "doc\xff", .position = first, .revision = 1 };
    try std.testing.expect(try stageDispatch(alloc, &txn, authority, item, 7, .{ .next_template = 1, .complete = false }));
    try std.testing.expect(!try stageDispatch(alloc, &txn, authority, item, 8, .{ .next_template = 1, .complete = false }));
    const resumed: WorkPage.Item = .{ .document = item.document, .position = first, .revision = 1, .dispatch_sequence = 7, .next_template = 1 };
    try std.testing.expect(try stageDispatch(alloc, &txn, authority, resumed, 8, .{ .next_template = 2, .complete = true }));
    try std.testing.expect(!try stageDispatch(alloc, &txn, authority, resumed, 9, .{ .next_template = 2, .complete = true }));
    const dispatched = (try lookupWork(alloc, &txn, authority, item.document)).?;
    const round = try beginRetryRoundAt(&txn, authority, 100);
    try std.testing.expectEqual(@as(u64, 1), round);
    try std.testing.expectEqual(@as(u64, 40), try retryDelay(&txn, authority, 110, 50));
    try std.testing.expectEqual(@as(u64, 0), try retryDelay(&txn, authority, 150, 50));
    try std.testing.expectEqual(@as(u64, 0), try retryDelay(&txn, authority, 99, 50));
    try std.testing.expectEqual(@as(u64, 0), try retryDelay(&txn, authority, 0, 50));
    // An accepted prefix advances only scheduling metadata, without a replay
    // append. A missing suffix is re-enqueued atomically with its sequence.
    try std.testing.expect(try stageRetry(alloc, &txn, authority, dispatched, round, null, .{ .next_template = 1, .complete = false }));
    try std.testing.expect(!try stageRetry(alloc, &txn, authority, dispatched, round, 9, .{ .next_template = 1, .complete = false }));
    const retry_suffix = (try lookupWork(alloc, &txn, authority, item.document)).?;
    try std.testing.expect(try stageRetry(alloc, &txn, authority, retry_suffix, round, 10, .{ .next_template = 2, .complete = true }));
    const retried = (try lookupWork(alloc, &txn, authority, item.document)).?;
    try std.testing.expectEqual(@as(u32, 0), retried.retry_next_template);
    try std.testing.expectEqual(@as(u64, 10), retried.retry_sequence);
    try std.testing.expectEqual(dispatched.revision, retried.revision);
    try std.testing.expectEqual(dispatched.dispatch_sequence, retried.dispatch_sequence);
    try std.testing.expectError(error.InvalidBatchRequest, stageRetry(alloc, &txn, authority, retried, round, 11, .{ .next_template = 2, .complete = true }));
    const next_round = try beginRetryRound(&txn, authority);
    try std.testing.expectEqual(round + 1, next_round);
    try std.testing.expect(!try stageRetry(alloc, &txn, authority, retry_suffix, round, 12, .{ .next_template = 2, .complete = true }));
    // Dispatch does not discharge work. Dependency changes must requeue even
    // when the primary input position has not changed.
    try std.testing.expectEqual(@as(u64, 1), (try load(&txn)).?.pending_documents);
    try std.testing.expect(try mark(alloc, &txn, authority, "doc\xff", first));
    try std.testing.expect(!try stageRetry(alloc, &txn, authority, retried, next_round, 12, .{ .next_template = 2, .complete = true }));
    try std.testing.expect(!try stageDispatch(alloc, &txn, authority, item, 9, .{ .next_template = 1, .complete = true }));
    try complete(alloc, &txn, authority, item);
    try std.testing.expectEqual(@as(u64, 1), (try load(&txn)).?.pending_documents);
    const refreshed: WorkPage.Item = .{ .document = item.document, .position = first, .revision = 2 };
    try std.testing.expect(try stageDispatch(alloc, &txn, authority, refreshed, 9, .{ .next_template = 1, .complete = true }));
    try advanceBaseline(alloc, &txn, authority, "", "primary:a", false);
    try std.testing.expect(try mark(alloc, &txn, authority, "doc\xff", later));
    try std.testing.expect(!try stageDispatch(alloc, &txn, authority, item, 10, .{ .next_template = 1, .complete = true }));
    try complete(alloc, &txn, authority, item);
    try std.testing.expectEqual(@as(u64, 1), (try load(&txn)).?.pending_documents);
    try std.testing.expectError(error.ArtifactCatalogDrift, advanceBaseline(alloc, &txn, authority, "", "primary:b", true));
    try advanceBaseline(alloc, &txn, authority, "primary:a", "", true);
    try std.testing.expectError(error.ArtifactCatalogDrift, stageSeal(alloc, &txn, authority, @splat(4)));
    const current: WorkPage.Item = .{ .document = item.document, .position = later, .revision = 3 };
    try complete(alloc, &txn, authority, current);
    try complete(alloc, &txn, authority, current);
    // Removing a work record cannot recycle its identity if dependency work
    // is recreated with the same primary position (including absence).
    try std.testing.expect(try mark(alloc, &txn, authority, item.document, later));
    try complete(alloc, &txn, authority, current);
    try std.testing.expectEqual(@as(u64, 1), (try load(&txn)).?.pending_documents);
    try std.testing.expect(!try stageDispatch(alloc, &txn, authority, current, 10, .{ .next_template = 1, .complete = true }));
    try complete(alloc, &txn, authority, .{ .document = item.document, .position = later, .revision = 4 });
    const validation = @import("artifact_producer_validation.zig");
    try std.testing.expectError(error.ArtifactCatalogDrift, stageSeal(alloc, &txn, authority, @splat(4)));
    try validation.begin(alloc, &txn, authority);
    try std.testing.expectError(error.ArtifactCatalogDrift, stageSeal(alloc, &txn, authority, @splat(4)));
    try validation.advance(alloc, &txn, authority, (try validation.load(&txn)).?, "", true);
    try validation.invalidate(alloc, &txn, authority);
    // A changed dependency invalidates seal even if no primary row became
    // dirty. The restarted validation must discover and re-dirty its users.
    try std.testing.expectError(error.ArtifactCatalogDrift, stageSeal(alloc, &txn, authority, @splat(4)));
    try validation.advance(alloc, &txn, authority, (try validation.load(&txn)).?, "", true);
    try stageSeal(alloc, &txn, authority, @splat(4));
    try std.testing.expectError(error.RetainedEffectsFenceMismatch, mark(alloc, &txn, authority, "next", later));
    try std.testing.expectError(error.ArtifactCatalogDrift, cancelSeal(alloc, &txn, authority, @splat(5)));
    try cancelSeal(alloc, &txn, authority, @splat(4));
    try std.testing.expect(try mark(alloc, &txn, authority, "next", later));
    try std.testing.expectEqual(@as(u64, 1), (try load(&txn)).?.pending_documents);
    var exhausted = (try load(&txn)).?;
    exhausted.work_revision = std.math.maxInt(u64);
    try store(alloc, &txn, exhausted);
    try std.testing.expectError(error.ResourceLimitExceeded, mark(alloc, &txn, authority, "overflow", later));
    try std.testing.expectEqual(@as(u64, 1), (try load(&txn)).?.pending_documents);
    try std.testing.expectEqual(std.math.maxInt(u64), (try load(&txn)).?.work_revision);
}
