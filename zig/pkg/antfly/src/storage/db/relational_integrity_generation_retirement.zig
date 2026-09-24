// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Owner-local inverse-reference generation retirement. Pending TRUNCATE
//! records are invisible to integrity reads. Activation atomically installs a
//! permanent accepted-generation scope; bounded GC removes both physical
//! references and historical tombstones only after metadata acknowledgement.
const std = @import("std");
const topology = @import("relational_integrity_topology.zig");
const integrity = @import("relational_integrity_contract.zig");

pub const key = "\x00\x00__metadata__:relational_integrity_generation_retirement";
pub const active_prefix = "\x00\x00__metadata__:relational_integrity_retired_generation:";
pub const gc_progress_key = "\x00\x00__metadata__:relational_integrity_retired_generation_gc";
pub const activation_receipt_key = "\x00\x00__metadata__:relational_integrity_generation_activation_receipt";
pub const completed_pending_key = "\x00\x00__metadata__:relational_integrity_generation_completed_pending";
pub const acknowledged_receipt_key = "\x00\x00__metadata__:relational_integrity_generation_acknowledged_receipt";
pub const max_entries = 128;
const header_len = 184;
const max_name_len = 256;
const entry_len = 44 + 2 * max_name_len;
const active_len = 4 + 1 + 3 + entry_len + 32 + 136 + 32;
const max_cursor_len = integrity.key_len + 32;

pub const Entry = @import("relational_integrity_topology_contract.zig").ParentRetirementEntry;

pub fn publicationDigest(plan_id: [16]u8, plan_digest: integrity.Digest) integrity.Digest {
    var state = std.crypto.hash.Blake3.init(.{});
    state.update("antfly external parent generation publication v1");
    state.update(&plan_id);
    state.update(&plan_digest);
    var result: integrity.Digest = undefined;
    state.final(&result);
    return result;
}

pub fn activationReceipt(fence: topology.Fence, plan_digest: integrity.Digest, publication_digest: integrity.Digest) !integrity.Digest {
    var state = std.crypto.hash.Blake3.init(.{});
    state.update("antfly external parent owner activation receipt v1");
    state.update(&try fence.encode());
    state.update(&plan_digest);
    state.update(&publication_digest);
    var result: integrity.Digest = undefined;
    state.final(&result);
    return result;
}

pub fn completedActivation(txn: anytype, fence: topology.Fence, plan_digest: integrity.Digest, publication_digest: integrity.Digest) !bool {
    const bytes = (try optionalKey(txn, activation_receipt_key)) orelse return false;
    const expected = try activationReceipt(fence, plan_digest, publication_digest);
    return std.mem.eql(u8, bytes, &expected);
}

/// An owner that has made old references invisible cannot change topology
/// until metadata has durably recorded that exact publication. The ACK is
/// itself replicated; a lost ACK reply can be retried without reopening the
/// admission window.
pub fn requireActivationAcknowledged(txn: anytype) !void {
    if (try optionalKey(txn, completed_pending_key) == null) return;
    const receipt = (try optionalKey(txn, activation_receipt_key)) orelse return error.GenerationRetirementAcknowledgementPending;
    const acknowledged = (try optionalKey(txn, acknowledged_receipt_key)) orelse return error.GenerationRetirementAcknowledgementPending;
    if (receipt.len != 32 or acknowledged.len != 32 or !std.mem.eql(u8, receipt, acknowledged))
        return error.GenerationRetirementAcknowledgementPending;
}

/// Caller has obtained a fresh metadata leader read-index response proving
/// child publication and the exact parent_activated receipt. Parent admission
/// remains fenced from activation until this publication is durable.
pub fn stageAcknowledgement(txn: anytype, fence: topology.Fence, plan_digest: integrity.Digest, publication_digest: integrity.Digest) !void {
    if (!try completedActivation(txn, fence, plan_digest, publication_digest)) return error.GenerationRetirementChanged;
    const pending = (try completedPending(txn)) orelse return error.GenerationRetirementChanged;
    if (!pending.fence.eql(fence) or !std.mem.eql(u8, &pending.plan_digest, &plan_digest)) return error.GenerationRetirementChanged;
    const expected = try activationReceipt(fence, plan_digest, publication_digest);
    if (try optionalKey(txn, acknowledged_receipt_key)) |ack| {
        if (!std.mem.eql(u8, ack, &expected)) return error.GenerationRetirementChanged;
        return;
    }
    if (try topology.current(txn)) |active| {
        if (!active.eql(fence)) return error.IntegrityTopologyChanged;
        try topology.stageRelease(txn, fence);
    } else {
        const completed = (try topology.completed(txn)) orelse return error.IntegrityTopologyFenceMissing;
        if (!completed.eql(fence)) return error.IntegrityTopologyChanged;
    }
    try txn.put(acknowledged_receipt_key, &expected);
}

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

    /// Different parent ranges receive the same irreversible publication but
    /// have distinct owner fences. A merge may retain either physical proof
    /// only when the semantic generation and publication are identical.
    pub fn samePublication(self: Active, other: Active) bool {
        return std.mem.eql(u8, self.entry, other.entry) and
            std.mem.eql(u8, &self.publication_digest, &other.publication_digest);
    }

    pub fn decode(bytes: []const u8, generation: integrity.Generation) !Active {
        if (bytes.len != active_len or !std.mem.eql(u8, bytes[0..4], "AIG3") or bytes[4] != 2 or
            !std.mem.allEqual(u8, bytes[5..8], 0) or !std.mem.eql(u8, bytes[16..32], &generation) or
            std.mem.allEqual(u8, bytes[8 + entry_len ..][0..32], 0)) return error.InvalidGenerationRetirement;
        var digest: integrity.Digest = undefined;
        std.crypto.hash.Blake3.hash(bytes[0 .. bytes.len - 32], &digest, .{});
        if (!std.mem.eql(u8, &digest, bytes[bytes.len - 32 ..])) return error.InvalidGenerationRetirement;
        const entry = bytes[8..][0..entry_len];
        try validateEntry(entry);
        const fence = topology.Fence.decode(bytes[8 + entry_len + 32 ..][0..136]) catch return error.InvalidGenerationRetirement;
        if (fence.role != .truncate_parent and fence.role != .child_generation_parent) return error.InvalidGenerationRetirement;
        return .{ .entry = entry, .publication_digest = bytes[8 + entry_len ..][0..32].*, .fence = fence };
    }
};

/// Each activation resets a two-phase sequential scan: inverse references,
/// then historical tombstones. Cursor CAS makes restart and replay exact.
pub const GcProgress = struct {
    revision: u64,
    cursor: []const u8 = "",
    tombstones: bool = false,
    complete: bool = false,

    pub fn decode(bytes: []const u8) !GcProgress {
        if (bytes.len < 18 + 32 or !std.mem.eql(u8, bytes[0..4], "AIGC") or bytes[4] != 2 or
            bytes[5] > 2 or !std.mem.allEqual(u8, bytes[6..8], 0)) return error.InvalidGenerationRetirement;
        const cursor_len = std.mem.readInt(u16, bytes[16..18], .little);
        if (cursor_len > max_cursor_len or bytes.len != 18 + @as(usize, cursor_len) + 32) return error.InvalidGenerationRetirement;
        var digest: integrity.Digest = undefined;
        std.crypto.hash.Blake3.hash(bytes[0 .. bytes.len - 32], &digest, .{});
        if (!std.mem.eql(u8, &digest, bytes[bytes.len - 32 ..])) return error.InvalidGenerationRetirement;
        const revision = std.mem.readInt(u64, bytes[8..16], .little);
        const cursor = bytes[18 .. bytes.len - 32];
        const tombstones = bytes[5] != 0;
        if (revision == 0 or (cursor.len != 0 and (if (tombstones)
            cursor.len != active_prefix.len + 16 or !std.mem.startsWith(u8, cursor, active_prefix)
        else
            (try integrity.parseKey(cursor)).kind != .reference))) return error.InvalidGenerationRetirement;
        return .{ .revision = revision, .cursor = cursor, .tombstones = tombstones, .complete = bytes[5] == 2 };
    }

    pub fn encode(self: GcProgress, alloc: std.mem.Allocator) ![]u8 {
        if (self.revision == 0 or self.cursor.len > max_cursor_len or
            (self.cursor.len != 0 and (if (self.tombstones)
                self.cursor.len != active_prefix.len + 16 or !std.mem.startsWith(u8, self.cursor, active_prefix)
            else
                (try integrity.parseKey(self.cursor)).kind != .reference))) return error.InvalidGenerationRetirement;
        const bytes = try alloc.alloc(u8, 18 + self.cursor.len + 32);
        @memcpy(bytes[0..4], "AIGC");
        bytes[4] = 2;
        bytes[5] = if (self.complete) 2 else if (self.tombstones) 1 else 0;
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
    if (std.mem.allEqual(u8, &publication_digest, 0) or
        (fence.role != .truncate_parent and fence.role != .child_generation_parent)) return error.InvalidGenerationRetirement;
    const encoded_fence = try fence.encode();
    const bytes = try alloc.alloc(u8, active_len);
    @memcpy(bytes[0..4], "AIG3");
    bytes[4] = 2;
    @memset(bytes[5..8], 0);
    @memcpy(bytes[8..][0..entry_len], encoded_entry);
    @memcpy(bytes[8 + entry_len ..][0..32], &publication_digest);
    @memcpy(bytes[8 + entry_len + 32 ..][0..136], &encoded_fence);
    std.crypto.hash.Blake3.hash(bytes[0 .. bytes.len - 32], bytes[bytes.len - 32 ..][0..32], .{});
    return bytes;
}

fn validateEntry(entry: []const u8) !void {
    if (entry.len != entry_len) return error.InvalidGenerationRetirement;
    const child_len = std.mem.readInt(u16, entry[40..42], .little);
    const constraint_len = std.mem.readInt(u16, entry[42..44], .little);
    if (std.mem.readInt(u64, entry[0..8], .little) == 0 or std.mem.allEqual(u8, entry[8..24], 0) or
        (!std.mem.allEqual(u8, entry[24..40], 0) and std.mem.eql(u8, entry[8..24], entry[24..40])) or
        child_len == 0 or child_len > max_name_len or constraint_len == 0 or constraint_len > max_name_len or
        !std.mem.allEqual(u8, entry[44 + child_len .. 44 + max_name_len], 0) or
        !std.mem.allEqual(u8, entry[44 + max_name_len + constraint_len .. entry_len], 0)) return error.InvalidGenerationRetirement;
}

fn entryMatchesReference(entry: []const u8, reference: integrity.Reference) bool {
    const child_len = std.mem.readInt(u16, entry[40..42], .little);
    const constraint_len = std.mem.readInt(u16, entry[42..44], .little);
    return std.mem.eql(u8, entry[8..24], &reference.constraint_generation) and
        std.mem.eql(u8, entry[44..][0..child_len], reference.child_table) and
        std.mem.eql(u8, entry[44 + max_name_len ..][0..constraint_len], reference.constraint_name);
}

pub const Pending = struct {
    fence: topology.Fence,
    plan_digest: integrity.Digest,
    /// Borrowed from the encoded record after decode.
    entries: []const u8,

    pub fn entryCount(self: Pending) usize {
        return self.entries.len / entry_len;
    }

    pub fn entryAt(self: Pending, index: usize) !Entry {
        if (index >= self.entryCount()) return error.InvalidGenerationRetirement;
        const bytes = self.entries[index * entry_len ..][0..entry_len];
        try validateEntry(bytes);
        const child_len = std.mem.readInt(u16, bytes[40..42], .little);
        const constraint_len = std.mem.readInt(u16, bytes[42..44], .little);
        return .{
            .child_table_id = std.mem.readInt(u64, bytes[0..8], .little),
            .generation = bytes[8..24].*,
            .next_generation = bytes[24..40].*,
            .child_table_name = bytes[44..][0..child_len],
            .constraint_name = bytes[44 + max_name_len ..][0..constraint_len],
        };
    }

    pub fn contains(self: Pending, child_table_id: u64, generation: integrity.Generation) bool {
        var offset: usize = 0;
        while (offset < self.entries.len) : (offset += entry_len) {
            if (std.mem.readInt(u64, self.entries[offset..][0..8], .little) == child_table_id and
                std.mem.eql(u8, self.entries[offset + 8 ..][0..16], &generation)) return true;
        }
        return false;
    }

    pub fn containsTransition(self: Pending, child_table_id: u64, generation: integrity.Generation, next_generation: integrity.Generation) bool {
        var offset: usize = 0;
        while (offset < self.entries.len) : (offset += entry_len) {
            if (std.mem.readInt(u64, self.entries[offset..][0..8], .little) == child_table_id and
                std.mem.eql(u8, self.entries[offset + 8 ..][0..16], &generation) and
                std.mem.eql(u8, self.entries[offset + 24 ..][0..16], &next_generation)) return true;
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
            !std.mem.eql(u8, bytes[0..4], "AIG2") or bytes[4] != 2 or
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
            // A TRUNCATE successor must always have a nonzero replacement;
            // only child-schema drop/reparent tombstones may encode none.
            if (std.mem.allEqual(u8, entry[24..40], 0)) return error.InvalidGenerationRetirement;
            for (0..index) |prior| {
                const previous = entries[prior * entry_len ..][0..entry_len];
                if (std.mem.eql(u8, previous[0..24], entry[0..24]) or
                    (std.mem.eql(u8, previous[44..][0..max_name_len], entry[44..][0..max_name_len]) and
                        std.mem.eql(u8, previous[44 + max_name_len ..][0..max_name_len], entry[44 + max_name_len ..][0..max_name_len]))) return error.InvalidGenerationRetirement;
            }
        }
        return .{ .fence = fence, .plan_digest = bytes[144..176].*, .entries = entries };
    }
};

pub const OwnerStatus = struct {
    fence: topology.Fence,
    plan_digest: integrity.Digest,
    entries: []const Entry,
    completed: bool,
    acknowledged: bool,
    receipt: ?integrity.Digest,
};

pub fn ownerStatus(alloc: std.mem.Allocator, txn: anytype) !?OwnerStatus {
    const active_pending = try current(txn);
    const pending = active_pending orelse (try completedPending(txn)) orelse return null;
    if (active_pending == null) {
        // A later topology transition may begin only after the metadata ACK.
        // Keep the old receipt addressable for a lost ACK response even then.
        if (try optionalKey(txn, acknowledged_receipt_key) == null) {
            if (try topology.current(txn)) |active| {
                if (!active.eql(pending.fence)) return error.IntegrityTopologyChanged;
            } else {
                const completed = (try topology.completed(txn)) orelse return error.IntegrityTopologyFenceMissing;
                if (!completed.eql(pending.fence)) return error.IntegrityTopologyChanged;
            }
        }
    }
    const owner_fence = if (active_pending != null)
        (try topology.current(txn)) orelse return error.IntegrityTopologyFenceMissing
    else
        pending.fence;
    if (!owner_fence.eql(pending.fence)) return error.GenerationRetirementChanged;
    const entries = try alloc.alloc(Entry, pending.entryCount());
    errdefer alloc.free(entries);
    for (entries, 0..) |*entry, index| entry.* = try pending.entryAt(index);
    const receipt = if (active_pending == null) blk: {
        const raw = (try optionalKey(txn, activation_receipt_key)) orelse return error.GenerationRetirementChanged;
        if (raw.len != 32) return error.GenerationRetirementChanged;
        break :blk raw[0..32].*;
    } else null;
    const acknowledged = if (receipt) |digest| blk: {
        const raw = (try optionalKey(txn, acknowledged_receipt_key)) orelse break :blk false;
        break :blk std.mem.eql(u8, raw, &digest);
    } else false;
    return .{ .fence = owner_fence, .plan_digest = pending.plan_digest, .entries = entries, .completed = active_pending == null, .acknowledged = acknowledged, .receipt = receipt };
}

pub fn encodePending(alloc: std.mem.Allocator, fence: topology.Fence, plan_digest: integrity.Digest, entries: []const Entry) ![]u8 {
    if (fence.role != .truncate_parent or std.mem.allEqual(u8, &plan_digest, 0) or entries.len == 0 or entries.len > max_entries)
        return error.InvalidGenerationRetirement;
    const encoded_fence = try fence.encode();
    const bytes = try alloc.alloc(u8, header_len + entries.len * entry_len + 32);
    errdefer alloc.free(bytes);
    @memset(bytes, 0);
    @memcpy(bytes[0..4], "AIG2");
    bytes[4] = 2; // pending; invisible until owner activation commits
    @memcpy(bytes[8..144], &encoded_fence);
    @memcpy(bytes[144..176], &plan_digest);
    std.mem.writeInt(u16, bytes[176..178], @intCast(entries.len), .little);
    for (entries, 0..) |entry, index| {
        if (entry.child_table_id == 0 or std.mem.allEqual(u8, &entry.generation, 0) or
            std.mem.allEqual(u8, &entry.next_generation, 0) or std.mem.eql(u8, &entry.generation, &entry.next_generation) or
            entry.child_table_name.len == 0 or entry.child_table_name.len > max_name_len or
            entry.constraint_name.len == 0 or entry.constraint_name.len > max_name_len) return error.InvalidGenerationRetirement;
        const offset = header_len + index * entry_len;
        std.mem.writeInt(u64, bytes[offset..][0..8], entry.child_table_id, .little);
        @memcpy(bytes[offset + 8 ..][0..16], &entry.generation);
        @memcpy(bytes[offset + 24 ..][0..16], &entry.next_generation);
        std.mem.writeInt(u16, bytes[offset + 40 ..][0..2], @intCast(entry.child_table_name.len), .little);
        std.mem.writeInt(u16, bytes[offset + 42 ..][0..2], @intCast(entry.constraint_name.len), .little);
        @memcpy(bytes[offset + 44 ..][0..entry.child_table_name.len], entry.child_table_name);
        @memcpy(bytes[offset + 44 + max_name_len ..][0..entry.constraint_name.len], entry.constraint_name);
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

pub fn completedPending(txn: anytype) !?Pending {
    return if (try optionalKey(txn, completed_pending_key)) |bytes| try Pending.decode(bytes) else null;
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
    } else if (try completedPending(txn)) |completed| {
        // Metadata's activating decision is irreversible. A generic cancel
        // must not lift its fence before child publication is proven by ACK.
        // Older acknowledged completions are retained through later fences.
        if (completed.fence.eql(fence) and (try optionalKey(txn, acknowledged_receipt_key)) == null)
            return error.GenerationAdmissionActivationRequired;
    }
}

pub fn requireClear(txn: anytype) !void {
    if (try current(txn) != null) return error.GenerationRetirementPending;
}

/// Rewrite handoff does not yet transfer permanent generation tombstones.
/// Split and merge use the integrity handoff manifest to copy them before
/// publication; rewrite must remain fenced until it has the same guarantee.
pub fn requireNoActive(txn: anytype) !void {
    var cursor = try txn.openCursor();
    defer cursor.close();
    const admission = @import("relational_integrity_generation_admission.zig");
    if (try cursor.seekAtOrAfter(admission.prefix)) |entry| {
        if (std.mem.startsWith(u8, entry.key, admission.prefix)) return error.GenerationRetirementHandoffRequired;
    }
    if (try cursor.seekAtOrAfter(active_prefix)) |entry| {
        if (std.mem.startsWith(u8, entry.key, active_prefix)) return error.GenerationRetirementHandoffRequired;
    }
}

/// The authenticated topology owner must have already verified a linearizable
/// metadata publication decision and exact plan/child generations before it
/// calls this helper. The generic topology batch is not publication proof;
/// only the private owner activation endpoint may submit its command.
/// The caller stages this together with topology release in ONE owner txn.
pub fn stageVerifiedActivation(alloc: std.mem.Allocator, txn: anytype, fence: topology.Fence, plan_id: [16]u8, plan_digest: integrity.Digest, publication_digest: integrity.Digest) !void {
    const actual = (try topology.current(txn)) orelse return error.IntegrityTopologyFenceMissing;
    if (!actual.eql(fence)) return error.IntegrityTopologyChanged;
    const pending = (try current(txn)) orelse return error.GenerationRetirementPending;
    if (!pending.fence.eql(fence) or !std.mem.eql(u8, &pending.plan_digest, &plan_digest)) return error.GenerationRetirementChanged;
    const entries = try alloc.dupe(u8, pending.entries);
    defer alloc.free(entries);
    for (0..pending.entryCount()) |index| {
        try @import("relational_integrity_generation_admission.zig").activateTruncate(alloc, txn, try pending.entryAt(index), plan_id, publication_digest);
    }
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
    const receipt = try activationReceipt(fence, plan_digest, publication_digest);
    try txn.put(activation_receipt_key, &receipt);
    try txn.delete(acknowledged_receipt_key);
    const encoded_pending = try alloc.dupe(u8, (try optional(txn)) orelse return error.GenerationRetirementPending);
    defer alloc.free(encoded_pending);
    try txn.put(completed_pending_key, encoded_pending);
    try txn.delete(key);
}

/// A child-schema publication retires each replaced or dropped FK generation
/// in the same parent transaction that switches accepted generations and
/// releases the parent fence. The durable marker survives split/merge/HA and
/// restarts bounded physical inverse-reference GC. New-only FKs have no old
/// inverse records to retire. The accepted scope remains even after GC, so a
/// delayed old-generation attach can never resurrect deleted references.
pub fn stageChildGenerationRetirements(alloc: std.mem.Allocator, txn: anytype, fence: topology.Fence, transitions: []const @import("relational_integrity_generation_admission.zig").Transition) !void {
    const admission = @import("relational_integrity_generation_admission.zig");
    if (fence.role != .child_generation_parent) return error.InvalidGenerationRetirement;
    try admission.validateTransitions(transitions);
    var added = false;
    for (transitions) |transition| {
        const generation = transition.expected_generation orelse continue;
        var entry: [entry_len]u8 = @splat(0);
        std.mem.writeInt(u64, entry[0..8], transition.child_table_id, .little);
        @memcpy(entry[8..24], &generation);
        if (transition.next_generation) |next| @memcpy(entry[24..40], &next);
        std.mem.writeInt(u16, entry[40..42], @intCast(transition.child_table_name.len), .little);
        std.mem.writeInt(u16, entry[42..44], @intCast(transition.constraint_name.len), .little);
        @memcpy(entry[44..][0..transition.child_table_name.len], transition.child_table_name);
        @memcpy(entry[44 + max_name_len ..][0..transition.constraint_name.len], transition.constraint_name);
        const physical_key = activeKey(generation);
        const value = try encodeActive(alloc, &entry, transition.decision_digest, fence);
        defer alloc.free(value);
        if (try optionalKey(txn, &physical_key)) |previous| {
            const active = try Active.decode(previous, generation);
            if (!active.samePublication(try Active.decode(value, generation))) return error.GenerationRetirementChanged;
        } else {
            try txn.put(&physical_key, value);
            added = true;
        }
    }
    if (added) {
        const prior = if (try optionalKey(txn, gc_progress_key)) |bytes| try GcProgress.decode(bytes) else null;
        const progress: GcProgress = .{ .revision = if (prior) |old| std.math.add(u64, old.revision, 1) catch return error.GenerationRetirementRevisionExhausted else 1 };
        const encoded = try progress.encode(alloc);
        defer alloc.free(encoded);
        try txn.put(gc_progress_key, encoded);
    }
}

/// A split or merge copies immutable tombstone authority before exposing the
/// new owner. A newly copied generation may retire references already copied
/// on earlier pages, so restart its bounded physical GC scan. The authority
/// itself is never garbage-collected here.
pub fn stageTransferredActive(alloc: std.mem.Allocator, txn: anytype, physical_key: []const u8, value: []const u8) !void {
    if (physical_key.len != active_prefix.len + 16 or !std.mem.startsWith(u8, physical_key, active_prefix)) return error.InvalidGenerationRetirement;
    const generation = physical_key[active_prefix.len..][0..16].*;
    const incoming = try Active.decode(value, generation);
    if (try optionalKey(txn, physical_key)) |existing| {
        const prior = try Active.decode(existing, generation);
        if (!prior.samePublication(incoming)) return error.IntegrityHandoffCollision;
        return;
    }
    const prior = if (try optionalKey(txn, gc_progress_key)) |bytes| try GcProgress.decode(bytes) else null;
    const progress: GcProgress = .{ .revision = if (prior) |old| std.math.add(u64, old.revision, 1) catch return error.GenerationRetirementRevisionExhausted else 1 };
    const encoded = try progress.encode(alloc);
    defer alloc.free(encoded);
    try txn.put(physical_key, value);
    try txn.put(gc_progress_key, encoded);
}

fn optionalKey(txn: anytype, physical_key: []const u8) !?[]const u8 {
    return txn.get(physical_key) catch |err| {
        if (err == error.NotFound) return null;
        return err;
    };
}

/// One point lookup per child constraint, independent of the number of
/// truncates. Activation commits the permanent accepted scope and physical
/// tombstone atomically; handoff/HA transfer both before serving. The scope
/// therefore owns admission, while tombstones supply physical GC provenance.
pub fn isRetired(txn: anytype, reference: integrity.Reference) !bool {
    return @import("relational_integrity_generation_admission.zig").excludes(txn, reference);
}

pub const GcRecord = struct { key: []const u8, value: []const u8 };
pub const GcCommand = struct {
    owner_group_id: u64,
    namespace: @import("doc_identity.zig").Namespace,
    expected: []const u8,
    next: []const u8,
    deletions: []const GcRecord,
    inspected: usize,

    pub fn validate(self: GcCommand) !void {
        if (self.owner_group_id == 0 or self.namespace.table_id == 0 or self.inspected > 4096 or
            self.deletions.len > self.inspected or self.deletions.len > 4096) return error.InvalidIntegrityBudget;
        const before = try GcProgress.decode(self.expected);
        const after = try GcProgress.decode(self.next);
        if (before.complete or before.revision != after.revision or
            (before.tombstones and !after.tombstones) or
            (!before.tombstones and after.complete) or
            (after.tombstones != before.tombstones and after.cursor.len != 0) or
            (after.tombstones == before.tombstones and !after.complete and
                (std.mem.order(u8, after.cursor, before.cursor) == .lt or
                    std.mem.eql(u8, after.cursor, before.cursor)))) return error.InvalidGenerationRetirement;
        var bytes: usize = 0;
        var previous = before.cursor;
        for (self.deletions) |record| {
            bytes = std.math.add(usize, bytes, record.key.len +| record.value.len) catch return error.InvalidIntegrityBudget;
            if (bytes > 16 * 1024 * 1024 or std.mem.order(u8, record.key, previous) != .gt or
                (after.tombstones == before.tombstones and !after.complete and std.mem.order(u8, record.key, after.cursor) == .gt)) return error.InvalidGenerationRetirement;
            if (before.tombstones) {
                if (record.key.len != active_prefix.len + 16 or !std.mem.startsWith(u8, record.key, active_prefix)) return error.InvalidGenerationRetirement;
                _ = try Active.decode(record.value, record.key[active_prefix.len..][0..16].*);
            } else _ = try integrity.Reference.decode(record.key, record.value);
            previous = record.key;
        }
    }

    pub fn jsonStringify(self: GcCommand, jw: anytype) @TypeOf(jw.*).Error!void {
        try @import("relational_integrity_json.zig").write(self, jw);
    }
};
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

    pub fn command(self: *const GcPage, owner_group_id: u64, namespace: @import("doc_identity.zig").Namespace) GcCommand {
        return .{ .owner_group_id = owner_group_id, .namespace = namespace, .expected = self.expected, .next = self.next, .deletions = self.deletions, .inspected = self.inspected };
    }
};

fn tombstoneRetirable(txn: anytype, physical_key: []const u8, value: []const u8) !bool {
    if (physical_key.len != active_prefix.len + 16 or !std.mem.startsWith(u8, physical_key, active_prefix)) return error.InvalidGenerationRetirement;
    const generation = physical_key[active_prefix.len..][0..16].*;
    const active = try Active.decode(value, generation);
    const entry = active.entry;
    const child_len = std.mem.readInt(u16, entry[40..42], .little);
    const constraint_len = std.mem.readInt(u16, entry[42..44], .little);
    const child = entry[44..][0..child_len];
    const constraint = entry[44 + max_name_len ..][0..constraint_len];
    const scope = (try @import("relational_integrity_generation_admission.zig").load(txn, child, constraint)) orelse return error.GenerationAdmissionChanged;
    if (scope.phase != .active or
        (scope.active_generation != null and std.mem.eql(u8, &scope.active_generation.?, &generation))) return error.GenerationAdmissionChanged;
    return true;
}

/// Preparing a page never mutates the owner. An apply command must carry the
/// exact expected/next progress and records into replicated apply; a timeout
/// can safely reprepare from durable progress. The scan is bounded by both
/// inspected records and bytes, including live records that are not deleted.
pub fn prepareGcPage(alloc: std.mem.Allocator, txn: anytype, max_records: usize, max_bytes: usize) !?GcPage {
    if (max_records == 0 or max_records > 4096 or max_bytes == 0 or max_bytes > 16 * 1024 * 1024) return error.InvalidIntegrityBudget;
    const raw = (try optionalKey(txn, gc_progress_key)) orelse return null;
    const progress = try GcProgress.decode(raw);
    if (progress.complete) return null;
    if (progress.tombstones) {
        requireActivationAcknowledged(txn) catch |err| switch (err) {
            error.GenerationRetirementAcknowledgementPending => return null,
            else => return err,
        };
        if (try topology.current(txn) != null) return null;
    }
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    const owned = arena.allocator();
    const expected = try owned.dupe(u8, raw);
    var deletions: std.ArrayList(GcRecord) = .empty;
    var prefix: [integrity.namespace.len + 1]u8 = undefined;
    @memcpy(prefix[0..integrity.namespace.len], integrity.namespace);
    prefix[integrity.namespace.len] = @intFromEnum(integrity.Kind.reference);
    var cursor = try txn.openCursor();
    defer cursor.close();
    const scan_prefix: []const u8 = if (progress.tombstones) active_prefix else &prefix;
    var item = try cursor.seekAtOrAfter(if (progress.cursor.len == 0) scan_prefix else progress.cursor);
    if (item) |entry| {
        if (progress.cursor.len != 0 and std.mem.eql(u8, entry.key, progress.cursor)) item = try cursor.next();
    }
    var inspected: usize = 0;
    var bytes: usize = 0;
    var after: []const u8 = progress.cursor;
    var complete = true;
    while (item) |entry| : (item = try cursor.next()) {
        if (!std.mem.startsWith(u8, entry.key, scan_prefix)) break;
        const size = std.math.add(usize, entry.key.len, entry.value.len) catch return error.IntegrityRecordTooLarge;
        if (inspected == max_records or size > max_bytes - bytes) {
            if (inspected == 0) return error.IntegrityRecordTooLarge;
            complete = false;
            break;
        }
        const retired = if (progress.tombstones)
            try tombstoneRetirable(txn, entry.key, entry.value)
        else
            try isRetired(txn, try integrity.Reference.decode(entry.key, entry.value));
        if (retired) try deletions.append(owned, .{
            .key = try owned.dupe(u8, entry.key),
            .value = try owned.dupe(u8, entry.value),
        });
        after = try owned.dupe(u8, entry.key);
        inspected += 1;
        bytes += size;
    }
    const next = try (GcProgress{
        .revision = progress.revision,
        .cursor = if (complete) "" else after,
        .tombstones = progress.tombstones or complete,
        .complete = complete and progress.tombstones,
    }).encode(owned);
    return .{ .arena = arena, .expected = expected, .next = next, .deletions = try deletions.toOwnedSlice(owned), .inspected = inspected };
}

/// Called only in deterministic owner apply. Exact progress and value checks
/// make a stale page harmless rather than deleting a newly rewritten record.
pub fn applyGcPage(txn: anytype, page: anytype) !void {
    try page.validate();
    const raw = (try optionalKey(txn, gc_progress_key)) orelse return error.GenerationRetirementChanged;
    if (!std.mem.eql(u8, raw, page.expected)) return error.GenerationRetirementChanged;
    const before = try GcProgress.decode(page.expected);
    const after = try GcProgress.decode(page.next);
    if (before.tombstones) {
        try requireActivationAcknowledged(txn);
        if (try topology.current(txn) != null) return error.IntegrityTopologyChanged;
    }
    var prefix: [integrity.namespace.len + 1]u8 = undefined;
    @memcpy(prefix[0..integrity.namespace.len], integrity.namespace);
    prefix[integrity.namespace.len] = @intFromEnum(integrity.Kind.reference);
    var cursor = try txn.openCursor();
    defer cursor.close();
    const scan_prefix: []const u8 = if (before.tombstones) active_prefix else &prefix;
    var item = try cursor.seekAtOrAfter(if (before.cursor.len == 0) scan_prefix else before.cursor);
    if (item) |entry| {
        if (before.cursor.len != 0 and std.mem.eql(u8, entry.key, before.cursor)) item = try cursor.next();
    }
    var observed: usize = 0;
    var observed_bytes: usize = 0;
    var deletion_index: usize = 0;
    const switched = before.tombstones != after.tombstones or after.complete;
    var last_matches = switched or std.mem.eql(u8, before.cursor, after.cursor);
    while (item) |entry| : (item = try cursor.next()) {
        if (!std.mem.startsWith(u8, entry.key, scan_prefix) or
            (!switched and after.cursor.len != 0 and std.mem.order(u8, entry.key, after.cursor) == .gt)) break;
        observed += 1;
        observed_bytes = std.math.add(usize, observed_bytes, entry.key.len + entry.value.len) catch return error.IntegrityRecordTooLarge;
        if (observed > 4096 or observed_bytes > 16 * 1024 * 1024) return error.InvalidIntegrityBudget;
        const retired = if (before.tombstones)
            try tombstoneRetirable(txn, entry.key, entry.value)
        else
            try isRetired(txn, try integrity.Reference.decode(entry.key, entry.value));
        if (retired) {
            if (deletion_index == page.deletions.len or
                !std.mem.eql(u8, entry.key, page.deletions[deletion_index].key) or
                !std.mem.eql(u8, entry.value, page.deletions[deletion_index].value)) return error.GenerationRetirementChanged;
            deletion_index += 1;
        }
        last_matches = switched or std.mem.eql(u8, entry.key, after.cursor);
    }
    if (deletion_index != page.deletions.len or observed != page.inspected or
        !last_matches or
        (switched and item != null and std.mem.startsWith(u8, item.?.key, scan_prefix))) return error.GenerationRetirementChanged;
    var previous = before.cursor;
    for (page.deletions) |record| {
        if (std.mem.order(u8, record.key, previous) != .gt or
            (!switched and std.mem.order(u8, record.key, after.cursor) == .gt)) return error.InvalidGenerationRetirement;
        const retired = if (before.tombstones)
            try tombstoneRetirable(txn, record.key, record.value)
        else
            try isRetired(txn, try integrity.Reference.decode(record.key, record.value));
        if (!retired) return error.GenerationRetirementChanged;
        const present = (try optionalKey(txn, record.key)) orelse return error.GenerationRetirementChanged;
        if (!std.mem.eql(u8, present, record.value)) return error.GenerationRetirementChanged;
        try txn.delete(record.key);
        previous = record.key;
    }
    try txn.put(gc_progress_key, page.next);
}

test "retired generation handoff accepts one publication across owner fences" {
    const alloc = std.testing.allocator;
    const first: topology.Fence = .{ .role = .truncate_parent, .transition_id = 11, .attempt = 1, .peer_group_id = 20, .owner_group_id = 31, .namespace = .{ .table_id = 41, .shard_id = 31, .range_id = 31 }, .catalog_digest = @splat(2) };
    var second = first;
    second.owner_group_id = 32;
    second.namespace.shard_id = 32;
    second.namespace.range_id = 32;
    const pending = try encodePending(alloc, first, @splat(5), &.{.{ .child_table_id = 51, .child_table_name = "children", .constraint_name = "fk", .generation = @splat(3), .next_generation = @splat(4) }});
    defer alloc.free(pending);
    const entry = (try Pending.decode(pending)).entries[0..entry_len];
    const first_bytes = try encodeActive(alloc, entry, @splat(6), first);
    defer alloc.free(first_bytes);
    const second_bytes = try encodeActive(alloc, entry, @splat(6), second);
    defer alloc.free(second_bytes);
    const conflict_bytes = try encodeActive(alloc, entry, @splat(7), second);
    defer alloc.free(conflict_bytes);
    const initial = try Active.decode(first_bytes, @splat(3));
    try std.testing.expect(initial.samePublication(try Active.decode(second_bytes, @splat(3))));
    try std.testing.expect(!initial.samePublication(try Active.decode(conflict_bytes, @splat(3))));
}
