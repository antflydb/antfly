// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Owner-local inverse-reference generation retirement for TRUNCATE. Pending
//! records are invisible to integrity reads. Activated generations retain an
//! immutable point-addressable tombstone even after physical reference GC:
//! deleting that tombstone would let a delayed old-generation attach resurrect
//! an FK after its child table has been truncated.
const std = @import("std");
const topology = @import("relational_integrity_topology.zig");
const integrity = @import("relational_integrity_contract.zig");

pub const key = "\x00\x00__metadata__:relational_integrity_generation_retirement";
pub const active_prefix = "\x00\x00__metadata__:relational_integrity_retired_generation:";
pub const gc_progress_key = "\x00\x00__metadata__:relational_integrity_retired_generation_gc";
pub const max_entries = 128;
const header_len = 184;
const max_name_len = 256;
const entry_len = 28 + 2 * max_name_len;
const active_len = 4 + 1 + 3 + entry_len + 32 + 136 + 32;
const max_cursor_len = integrity.key_len + 32;

pub const Entry = @import("relational_integrity_topology_contract.zig").ParentRetirementEntry;

pub fn activeKey(generation: integrity.Generation) [active_prefix.len + 16]u8 {
    var result: [active_prefix.len + 16]u8 = undefined;
    @memcpy(result[0..active_prefix.len], active_prefix);
    @memcpy(result[active_prefix.len..], &generation);
    return result;
}

pub const Active = struct {
    /// Borrowed from the encoded tombstone.
    entry: []const u8,
    publication_digest: integrity.Digest,
    fence: topology.Fence,

    pub fn matchesReference(self: Active, reference: integrity.Reference) bool {
        return entryMatchesReference(self.entry, reference);
    }

    pub fn decode(bytes: []const u8, generation: integrity.Generation) !Active {
        if (bytes.len != active_len or !std.mem.eql(u8, bytes[0..4], "AIG3") or bytes[4] != 1 or
            !std.mem.allEqual(u8, bytes[5..8], 0) or !std.mem.eql(u8, bytes[16..32], &generation) or
            std.mem.allEqual(u8, bytes[8 + entry_len ..][0..32], 0)) return error.InvalidGenerationRetirement;
        var digest: integrity.Digest = undefined;
        std.crypto.hash.Blake3.hash(bytes[0 .. bytes.len - 32], &digest, .{});
        if (!std.mem.eql(u8, &digest, bytes[bytes.len - 32 ..])) return error.InvalidGenerationRetirement;
        const entry = bytes[8..][0..entry_len];
        try validateEntry(entry);
        const fence = topology.Fence.decode(bytes[8 + entry_len + 32 ..][0..136]) catch return error.InvalidGenerationRetirement;
        if (fence.role != .truncate_parent) return error.InvalidGenerationRetirement;
        return .{ .entry = entry, .publication_digest = bytes[8 + entry_len ..][0..32].*, .fence = fence };
    }
};

/// Each activation resets one sequential scan. The cursor is a physical
/// reference key, not a child row key; its exact bytes make restart and Raft
/// replay deterministic. Tombstones outlive this physical cleanup progress.
pub const GcProgress = struct {
    revision: u64,
    cursor: []const u8 = "",
    complete: bool = false,

    pub fn decode(bytes: []const u8) !GcProgress {
        if (bytes.len < 18 + 32 or !std.mem.eql(u8, bytes[0..4], "AIGC") or bytes[4] != 1 or
            bytes[5] > 1 or !std.mem.allEqual(u8, bytes[6..8], 0)) return error.InvalidGenerationRetirement;
        const cursor_len = std.mem.readInt(u16, bytes[16..18], .little);
        if (cursor_len > max_cursor_len or bytes.len != 18 + @as(usize, cursor_len) + 32) return error.InvalidGenerationRetirement;
        var digest: integrity.Digest = undefined;
        std.crypto.hash.Blake3.hash(bytes[0 .. bytes.len - 32], &digest, .{});
        if (!std.mem.eql(u8, &digest, bytes[bytes.len - 32 ..])) return error.InvalidGenerationRetirement;
        const revision = std.mem.readInt(u64, bytes[8..16], .little);
        const cursor = bytes[18 .. bytes.len - 32];
        if (revision == 0 or (cursor.len != 0 and (try integrity.parseKey(cursor)).kind != .reference)) return error.InvalidGenerationRetirement;
        return .{ .revision = revision, .cursor = cursor, .complete = bytes[5] != 0 };
    }

    pub fn encode(self: GcProgress, alloc: std.mem.Allocator) ![]u8 {
        if (self.revision == 0 or self.cursor.len > max_cursor_len or
            (self.cursor.len != 0 and (try integrity.parseKey(self.cursor)).kind != .reference)) return error.InvalidGenerationRetirement;
        const bytes = try alloc.alloc(u8, 18 + self.cursor.len + 32);
        @memcpy(bytes[0..4], "AIGC");
        bytes[4] = 1;
        bytes[5] = @intFromBool(self.complete);
        @memset(bytes[6..8], 0);
        std.mem.writeInt(u64, bytes[8..16], self.revision, .little);
        std.mem.writeInt(u16, bytes[16..18], @intCast(self.cursor.len), .little);
        @memcpy(bytes[18..][0..self.cursor.len], self.cursor);
        std.crypto.hash.Blake3.hash(bytes[0 .. bytes.len - 32], bytes[bytes.len - 32 ..][0..32], .{});
        return bytes;
    }
};

fn encodeActive(alloc: std.mem.Allocator, encoded_entry: []const u8, publication_digest: integrity.Digest, fence: topology.Fence) ![]u8 {
    try validateEntry(encoded_entry);
    if (std.mem.allEqual(u8, &publication_digest, 0) or fence.role != .truncate_parent) return error.InvalidGenerationRetirement;
    const encoded_fence = try fence.encode();
    const bytes = try alloc.alloc(u8, active_len);
    @memcpy(bytes[0..4], "AIG3");
    bytes[4] = 1;
    @memset(bytes[5..8], 0);
    @memcpy(bytes[8..][0..entry_len], encoded_entry);
    @memcpy(bytes[8 + entry_len ..][0..32], &publication_digest);
    @memcpy(bytes[8 + entry_len + 32 ..][0..136], &encoded_fence);
    std.crypto.hash.Blake3.hash(bytes[0 .. bytes.len - 32], bytes[bytes.len - 32 ..][0..32], .{});
    return bytes;
}

fn validateEntry(entry: []const u8) !void {
    if (entry.len != entry_len) return error.InvalidGenerationRetirement;
    const child_len = std.mem.readInt(u16, entry[24..26], .little);
    const constraint_len = std.mem.readInt(u16, entry[26..28], .little);
    if (std.mem.readInt(u64, entry[0..8], .little) == 0 or std.mem.allEqual(u8, entry[8..24], 0) or
        child_len == 0 or child_len > max_name_len or constraint_len == 0 or constraint_len > max_name_len or
        !std.mem.allEqual(u8, entry[28 + child_len .. 28 + max_name_len], 0) or
        !std.mem.allEqual(u8, entry[28 + max_name_len + constraint_len .. entry_len], 0)) return error.InvalidGenerationRetirement;
}

fn entryMatchesReference(entry: []const u8, reference: integrity.Reference) bool {
    const child_len = std.mem.readInt(u16, entry[24..26], .little);
    const constraint_len = std.mem.readInt(u16, entry[26..28], .little);
    return std.mem.eql(u8, entry[8..24], &reference.constraint_generation) and
        std.mem.eql(u8, entry[28..][0..child_len], reference.child_table) and
        std.mem.eql(u8, entry[28 + max_name_len ..][0..constraint_len], reference.constraint_name);
}

pub const Pending = struct {
    fence: topology.Fence,
    plan_digest: integrity.Digest,
    /// Borrowed from the encoded record after decode.
    entries: []const u8,

    pub fn entryCount(self: Pending) usize {
        return self.entries.len / entry_len;
    }

    pub fn contains(self: Pending, child_table_id: u64, generation: integrity.Generation) bool {
        var offset: usize = 0;
        while (offset < self.entries.len) : (offset += entry_len) {
            if (std.mem.readInt(u64, self.entries[offset..][0..8], .little) == child_table_id and
                std.mem.eql(u8, self.entries[offset + 8 ..][0..16], &generation)) return true;
        }
        return false;
    }

    /// The physical inverse record does not encode the child table ID. The
    /// plan pins the name-to-ID bridge, and owner reads compare every encoded
    /// field present in a reference before treating it as retired.
    pub fn matchesReference(self: Pending, reference: integrity.Reference) bool {
        var offset: usize = 0;
        while (offset < self.entries.len) : (offset += entry_len) {
            if (entryMatchesReference(self.entries[offset..][0..entry_len], reference)) return true;
        }
        return false;
    }

    pub fn decode(bytes: []const u8) !Pending {
        if (bytes.len < header_len + entry_len + 32 or bytes.len > header_len + max_entries * entry_len + 32 or
            !std.mem.eql(u8, bytes[0..4], "AIG2") or bytes[4] != 1 or
            !std.mem.allEqual(u8, bytes[5..8], 0) or !std.mem.allEqual(u8, bytes[178..184], 0))
            return error.InvalidGenerationRetirement;
        const count = std.mem.readInt(u16, bytes[176..178], .little);
        if (count == 0 or count > max_entries or bytes.len != header_len + @as(usize, count) * entry_len + 32) return error.InvalidGenerationRetirement;
        var digest: integrity.Digest = undefined;
        std.crypto.hash.Blake3.hash(bytes[0 .. bytes.len - 32], &digest, .{});
        if (!std.mem.eql(u8, &digest, bytes[bytes.len - 32 ..])) return error.InvalidGenerationRetirement;
        const fence = topology.Fence.decode(bytes[8..144]) catch return error.InvalidGenerationRetirement;
        if (fence.role != .truncate_parent or std.mem.allEqual(u8, bytes[144..176], 0)) return error.InvalidGenerationRetirement;
        const entries = bytes[header_len .. bytes.len - 32];
        for (0..count) |index| {
            const entry = entries[index * entry_len ..][0..entry_len];
            try validateEntry(entry);
            for (0..index) |prior| {
                const previous = entries[prior * entry_len ..][0..entry_len];
                if (std.mem.eql(u8, previous[0..24], entry[0..24])) return error.InvalidGenerationRetirement;
            }
        }
        return .{ .fence = fence, .plan_digest = bytes[144..176].*, .entries = entries };
    }
};

pub fn encodePending(alloc: std.mem.Allocator, fence: topology.Fence, plan_digest: integrity.Digest, entries: []const Entry) ![]u8 {
    if (fence.role != .truncate_parent or std.mem.allEqual(u8, &plan_digest, 0) or entries.len == 0 or entries.len > max_entries)
        return error.InvalidGenerationRetirement;
    const encoded_fence = try fence.encode();
    const bytes = try alloc.alloc(u8, header_len + entries.len * entry_len + 32);
    errdefer alloc.free(bytes);
    @memset(bytes, 0);
    @memcpy(bytes[0..4], "AIG2");
    bytes[4] = 1; // pending; no activation command exists yet
    @memcpy(bytes[8..144], &encoded_fence);
    @memcpy(bytes[144..176], &plan_digest);
    std.mem.writeInt(u16, bytes[176..178], @intCast(entries.len), .little);
    for (entries, 0..) |entry, index| {
        if (entry.child_table_id == 0 or std.mem.allEqual(u8, &entry.generation, 0) or
            entry.child_table_name.len == 0 or entry.child_table_name.len > max_name_len or
            entry.constraint_name.len == 0 or entry.constraint_name.len > max_name_len) return error.InvalidGenerationRetirement;
        const offset = header_len + index * entry_len;
        std.mem.writeInt(u64, bytes[offset..][0..8], entry.child_table_id, .little);
        @memcpy(bytes[offset + 8 ..][0..16], &entry.generation);
        std.mem.writeInt(u16, bytes[offset + 24 ..][0..2], @intCast(entry.child_table_name.len), .little);
        std.mem.writeInt(u16, bytes[offset + 26 ..][0..2], @intCast(entry.constraint_name.len), .little);
        @memcpy(bytes[offset + 28 ..][0..entry.child_table_name.len], entry.child_table_name);
        @memcpy(bytes[offset + 28 + max_name_len ..][0..entry.constraint_name.len], entry.constraint_name);
        for (entries[0..index]) |previous| if (previous.child_table_id == entry.child_table_id and
            std.mem.eql(u8, &previous.generation, &entry.generation)) return error.InvalidGenerationRetirement;
    }
    std.crypto.hash.Blake3.hash(bytes[0 .. bytes.len - 32], bytes[bytes.len - 32 ..][0..32], .{});
    return bytes;
}

fn optional(txn: anytype) !?[]const u8 {
    return txn.get(key) catch |err| {
        if (err == error.NotFound) return null;
        return err;
    };
}

pub fn current(txn: anytype) !?Pending {
    return if (try optional(txn)) |bytes| try Pending.decode(bytes) else null;
}

/// Stage only after the parent owner is fenced and its old participants drain.
/// Replaying the identical stage after an unknown reply is idempotent.
pub fn stagePending(alloc: std.mem.Allocator, txn: anytype, manager: *@import("../transactions.zig").TxnManager, fence: topology.Fence, plan_digest: integrity.Digest, entries: []const Entry) !void {
    try topology.requireDrained(txn, manager, fence);
    const encoded = try encodePending(alloc, fence, plan_digest, entries);
    defer alloc.free(encoded);
    if (try optional(txn)) |before| {
        _ = try Pending.decode(before);
        if (!std.mem.eql(u8, before, encoded)) return error.GenerationRetirementChanged;
        return;
    }
    try txn.put(key, encoded);
}

/// Cancellation may remove pending state under its exact fence. A missing
/// record is an idempotent retry, but another generation is never removed.
pub fn stageCancel(txn: anytype, fence: topology.Fence) !void {
    const actual = (try topology.current(txn)) orelse return error.IntegrityTopologyFenceMissing;
    if (!actual.eql(fence)) return error.IntegrityTopologyChanged;
    if (try current(txn)) |pending| {
        if (!pending.fence.eql(fence)) return error.GenerationRetirementChanged;
        try txn.delete(key);
    }
}

pub fn requireClear(txn: anytype) !void {
    if (try current(txn) != null) return error.GenerationRetirementPending;
}

/// Range handoff currently transfers claims/references/jobs, not permanent
/// generation tombstones. Until the handoff manifest carries this authority,
/// never let a new owner reinterpret old inverse records as live.
pub fn requireNoActive(txn: anytype) !void {
    var cursor = try txn.openCursor();
    defer cursor.close();
    if (try cursor.seekAtOrAfter(active_prefix)) |entry| {
        if (std.mem.startsWith(u8, entry.key, active_prefix)) return error.GenerationRetirementHandoffRequired;
    }
}

/// The authenticated topology owner must have already verified a linearizable
/// metadata publication decision and exact plan/child generations before it
/// calls this helper. This helper deliberately is not wired to the generic
/// topology command: an arbitrary control message is not publication proof.
/// The caller stages this together with topology release in ONE owner txn.
pub fn stageVerifiedActivation(alloc: std.mem.Allocator, txn: anytype, fence: topology.Fence, plan_digest: integrity.Digest, publication_digest: integrity.Digest) !void {
    const actual = (try topology.current(txn)) orelse return error.IntegrityTopologyFenceMissing;
    if (!actual.eql(fence)) return error.IntegrityTopologyChanged;
    const pending = (try current(txn)) orelse return error.GenerationRetirementPending;
    if (!pending.fence.eql(fence) or !std.mem.eql(u8, &pending.plan_digest, &plan_digest)) return error.GenerationRetirementChanged;
    const entries = try alloc.dupe(u8, pending.entries);
    defer alloc.free(entries);
    var offset: usize = 0;
    while (offset < entries.len) : (offset += entry_len) {
        const entry = entries[offset..][0..entry_len];
        const physical_key = activeKey(entry[8..24].*);
        const next = try encodeActive(alloc, entry, publication_digest, fence);
        defer alloc.free(next);
        if (try optionalKey(txn, &physical_key)) |previous| {
            _ = try Active.decode(previous, entry[8..24].*);
            if (!std.mem.eql(u8, previous, next)) return error.GenerationRetirementChanged;
        } else try txn.put(&physical_key, next);
    }
    const prior = if (try optionalKey(txn, gc_progress_key)) |bytes| try GcProgress.decode(bytes) else null;
    const progress: GcProgress = .{ .revision = if (prior) |old| std.math.add(u64, old.revision, 1) catch return error.GenerationRetirementRevisionExhausted else 1 };
    const encoded_progress = try progress.encode(alloc);
    defer alloc.free(encoded_progress);
    try txn.put(gc_progress_key, encoded_progress);
    try txn.delete(key);
}

fn optionalKey(txn: anytype, physical_key: []const u8) !?[]const u8 {
    return txn.get(physical_key) catch |err| {
        if (err == error.NotFound) return null;
        return err;
    };
}

/// Point lookup stays O(1) regardless of the number of historic truncates.
/// A same-generation identity mismatch is corruption, not a live reference.
pub fn isRetired(txn: anytype, reference: integrity.Reference) !bool {
    const physical_key = activeKey(reference.constraint_generation);
    const bytes = (try optionalKey(txn, &physical_key)) orelse return false;
    const active = try Active.decode(bytes, reference.constraint_generation);
    if (!active.matchesReference(reference)) return error.GenerationRetirementChanged;
    return true;
}

pub const GcRecord = struct { key: []const u8, value: []const u8 };
pub const GcPage = struct {
    arena: std.heap.ArenaAllocator,
    expected: []const u8,
    next: []const u8,
    deletions: []const GcRecord,
    inspected: usize,
    pub fn deinit(self: *GcPage) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

/// Preparing a page never mutates the owner. An apply command must carry the
/// exact expected/next progress and records into replicated apply; a timeout
/// can safely reprepare from durable progress. The scan is bounded by both
/// inspected records and bytes, including live records that are not deleted.
pub fn prepareGcPage(alloc: std.mem.Allocator, txn: anytype, max_records: usize, max_bytes: usize) !?GcPage {
    if (max_records == 0 or max_records > 4096 or max_bytes == 0 or max_bytes > 16 * 1024 * 1024) return error.InvalidIntegrityBudget;
    const raw = (try optionalKey(txn, gc_progress_key)) orelse return null;
    if ((try GcProgress.decode(raw)).complete) return null;
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    const owned = arena.allocator();
    const expected = try owned.dupe(u8, raw);
    const progress = try GcProgress.decode(expected);
    var deletions: std.ArrayList(GcRecord) = .empty;
    var prefix: [integrity.namespace.len + 1]u8 = undefined;
    @memcpy(prefix[0..integrity.namespace.len], integrity.namespace);
    prefix[integrity.namespace.len] = @intFromEnum(integrity.Kind.reference);
    var cursor = try txn.openCursor();
    defer cursor.close();
    var item = try cursor.seekAtOrAfter(if (progress.cursor.len == 0) &prefix else progress.cursor);
    if (item) |entry| {
        if (progress.cursor.len != 0 and std.mem.eql(u8, entry.key, progress.cursor)) item = try cursor.next();
    }
    var inspected: usize = 0;
    var bytes: usize = 0;
    var after: []const u8 = progress.cursor;
    var complete = true;
    while (item) |entry| : (item = try cursor.next()) {
        if (!std.mem.startsWith(u8, entry.key, &prefix)) break;
        const size = std.math.add(usize, entry.key.len, entry.value.len) catch return error.IntegrityRecordTooLarge;
        if (inspected == max_records or size > max_bytes - bytes) {
            if (inspected == 0) return error.IntegrityRecordTooLarge;
            complete = false;
            break;
        }
        const reference = try integrity.Reference.decode(entry.key, entry.value);
        if (try isRetired(txn, reference)) try deletions.append(owned, .{
            .key = try owned.dupe(u8, entry.key),
            .value = try owned.dupe(u8, entry.value),
        });
        after = try owned.dupe(u8, entry.key);
        inspected += 1;
        bytes += size;
    }
    const next = try (GcProgress{ .revision = progress.revision, .cursor = after, .complete = complete }).encode(owned);
    return .{ .arena = arena, .expected = expected, .next = next, .deletions = try deletions.toOwnedSlice(owned), .inspected = inspected };
}

/// Called only in deterministic owner apply. Exact progress and value checks
/// make a stale page harmless rather than deleting a newly rewritten record.
pub fn applyGcPage(txn: anytype, page: GcPage) !void {
    const raw = (try optionalKey(txn, gc_progress_key)) orelse return error.GenerationRetirementChanged;
    if (!std.mem.eql(u8, raw, page.expected)) return error.GenerationRetirementChanged;
    const before = try GcProgress.decode(page.expected);
    const after = try GcProgress.decode(page.next);
    if (before.complete or before.revision != after.revision or
        std.mem.order(u8, after.cursor, before.cursor) == .lt or
        (!after.complete and std.mem.eql(u8, after.cursor, before.cursor))) return error.InvalidGenerationRetirement;
    var prefix: [integrity.namespace.len + 1]u8 = undefined;
    @memcpy(prefix[0..integrity.namespace.len], integrity.namespace);
    prefix[integrity.namespace.len] = @intFromEnum(integrity.Kind.reference);
    var cursor = try txn.openCursor();
    defer cursor.close();
    var item = try cursor.seekAtOrAfter(if (before.cursor.len == 0) &prefix else before.cursor);
    if (item) |entry| {
        if (before.cursor.len != 0 and std.mem.eql(u8, entry.key, before.cursor)) item = try cursor.next();
    }
    var observed: usize = 0;
    var observed_bytes: usize = 0;
    var deletion_index: usize = 0;
    var last_matches = std.mem.eql(u8, before.cursor, after.cursor);
    while (item) |entry| : (item = try cursor.next()) {
        if (!std.mem.startsWith(u8, entry.key, &prefix) or
            (after.cursor.len != 0 and std.mem.order(u8, entry.key, after.cursor) == .gt)) break;
        observed += 1;
        observed_bytes = std.math.add(usize, observed_bytes, entry.key.len + entry.value.len) catch return error.IntegrityRecordTooLarge;
        if (observed > 4096 or observed_bytes > 16 * 1024 * 1024) return error.InvalidIntegrityBudget;
        const reference = try integrity.Reference.decode(entry.key, entry.value);
        if (try isRetired(txn, reference)) {
            if (deletion_index == page.deletions.len or
                !std.mem.eql(u8, entry.key, page.deletions[deletion_index].key) or
                !std.mem.eql(u8, entry.value, page.deletions[deletion_index].value)) return error.GenerationRetirementChanged;
            deletion_index += 1;
        }
        last_matches = std.mem.eql(u8, entry.key, after.cursor);
    }
    if (deletion_index != page.deletions.len or observed != page.inspected or
        !last_matches or
        (after.complete and item != null and std.mem.startsWith(u8, item.?.key, &prefix))) return error.GenerationRetirementChanged;
    var previous = before.cursor;
    for (page.deletions) |record| {
        if (std.mem.order(u8, record.key, previous) != .gt or std.mem.order(u8, record.key, after.cursor) == .gt) return error.InvalidGenerationRetirement;
        const reference = try integrity.Reference.decode(record.key, record.value);
        if (!(try isRetired(txn, reference))) return error.GenerationRetirementChanged;
        const present = (try optionalKey(txn, record.key)) orelse return error.GenerationRetirementChanged;
        if (!std.mem.eql(u8, present, record.value)) return error.GenerationRetirementChanged;
        try txn.delete(record.key);
        previous = record.key;
    }
    try txn.put(gc_progress_key, page.next);
}
