// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Owner-local pending inverse-reference generation retirement for TRUNCATE.
//! A pending record is deliberately invisible to integrity reads. Activation
//! needs a durable publication proof and is not exposed by this module yet.
const std = @import("std");
const topology = @import("relational_integrity_topology.zig");
const integrity = @import("relational_integrity_contract.zig");

pub const key = "\x00\x00__metadata__:relational_integrity_generation_retirement";
pub const max_entries = 128;
const header_len = 184;
const max_name_len = 256;
const entry_len = 28 + 2 * max_name_len;

pub const Entry = @import("relational_integrity_topology_contract.zig").ParentRetirementEntry;

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
            const entry = self.entries[offset..][0..entry_len];
            const child_len = std.mem.readInt(u16, entry[24..26], .little);
            const constraint_len = std.mem.readInt(u16, entry[26..28], .little);
            if (std.mem.eql(u8, entry[8..24], &reference.constraint_generation) and
                std.mem.eql(u8, entry[28..][0..child_len], reference.child_table) and
                std.mem.eql(u8, entry[28 + max_name_len ..][0..constraint_len], reference.constraint_name)) return true;
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
            const child_len = std.mem.readInt(u16, entry[24..26], .little);
            const constraint_len = std.mem.readInt(u16, entry[26..28], .little);
            if (std.mem.readInt(u64, entry[0..8], .little) == 0 or std.mem.allEqual(u8, entry[8..24], 0) or
                child_len == 0 or child_len > max_name_len or constraint_len == 0 or constraint_len > max_name_len or
                !std.mem.allEqual(u8, entry[28 + child_len .. 28 + max_name_len], 0) or
                !std.mem.allEqual(u8, entry[28 + max_name_len + constraint_len .. entry_len], 0)) return error.InvalidGenerationRetirement;
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
