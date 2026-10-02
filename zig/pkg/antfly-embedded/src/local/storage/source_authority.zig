// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Durable owner authority and the non-Raft source cut clock. The native
//! sequence is advanced in the same transaction as control or retained row
//! effects; it is not a fabricated Raft watermark.
const std = @import("std");
const keys = @import("internal_keys.zig");
pub const Kind = enum(u8) { raft = 1, native = 2 };
pub const key = "\x00\x00__metadata__:source_authority";
pub const encoded_size = 72;
pub const State = struct {
    kind: Kind,
    namespace: [24]u8,
    sequence: u64 = 0,

    pub fn encode(self: State) [encoded_size]u8 {
        var bytes: [encoded_size]u8 = @splat(0);
        @memcpy(bytes[0..4], "ASA1");
        bytes[4] = @intFromEnum(self.kind);
        @memcpy(bytes[8..32], &self.namespace);
        std.mem.writeInt(u64, bytes[32..40], self.sequence, .little);
        std.crypto.hash.sha2.Sha256.hash(bytes[0..40], bytes[40..72], .{});
        return bytes;
    }

    pub fn decode(bytes: []const u8) !State {
        if (bytes.len != encoded_size or !std.mem.eql(u8, bytes[0..4], "ASA1") or !std.mem.allEqual(u8, bytes[5..8], 0)) return error.OnlineSourceCorrupt;
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(bytes[0..40], &digest, .{});
        if (!std.mem.eql(u8, &digest, bytes[40..72])) return error.OnlineSourceCorrupt;
        const result: State = .{ .kind = std.enums.fromInt(Kind, bytes[4]) orelse return error.OnlineSourceCorrupt, .namespace = bytes[8..32].*, .sequence = std.mem.readInt(u64, bytes[32..40], .little) };
        if (std.mem.allEqual(u8, &result.namespace, 0) or (result.kind == .raft and result.sequence != 0)) return error.OnlineSourceCorrupt;
        return result;
    }
};

pub fn load(txn: anytype) !?State {
    const raw = txn.get(key) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    return try State.decode(raw);
}

pub fn require(txn: anytype, kind: Kind, namespace: [24]u8) !State {
    const state = try load(txn) orelse return error.OnlineSourceScopeChanged;
    if (state.kind != kind or !std.mem.eql(u8, &state.namespace, &namespace)) return error.OnlineSourceScopeChanged;
    if (kind == .native) try requireNoRaftMarker(txn);
    return state;
}

fn requireNoRaftMarker(txn: anytype) !void {
    const marker = txn.get(&keys.raft_document_applied_entry_key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
    if (marker != null) return error.OnlineSourceScopeChanged;
}

/// Called only from authenticated owner provisioning, not a caller's Scope.
pub fn bind(txn: anytype, kind: Kind, namespace: [24]u8) !void {
    // Legacy unscoped library owners cannot issue a valid source Scope. Do
    // not persist an invalid all-zero binding merely because the C ABI's
    // conservative default is Raft ownership.
    if (std.mem.allEqual(u8, &namespace, 0)) {
        if (kind == .native) return error.IdentityNamespaceMismatch;
        return;
    }
    if (try load(txn)) |existing| {
        if (std.mem.eql(u8, &existing.namespace, &namespace)) {
            _ = try require(txn, kind, namespace);
            return;
        }
        // An authenticated generation adoption may change the logical
        // namespace. Require its durable identity publication first; never
        // reset the clock merely because a caller supplied different fields.
        const current = txn.get(&keys.identity_namespace_key) catch |err| switch (err) {
            error.NotFound => return error.OnlineSourceScopeChanged,
            else => return err,
        };
        if (!std.mem.eql(u8, current, &namespace)) return error.OnlineSourceScopeChanged;
    }
    if (kind == .native) try requireNoRaftMarker(txn);
    try txn.put(key, &(State{ .kind = kind, .namespace = namespace }).encode());
}

pub fn advance(txn: anytype, namespace: [24]u8, forwarded: ?u64) !u64 {
    var state = try require(txn, .native, namespace);
    const next = std.math.add(u64, state.sequence, 1) catch return error.OnlineSourceCorrupt;
    if (forwarded) |value| if (value != next) return error.OnlineSourceScopeChanged;
    state.sequence = next;
    try txn.put(key, &state.encode());
    return next;
}

/// One retained frame represents one atomic logical mutation on both primary
/// and standby. No clock lookup is paid by owners without active retention.
pub fn advanceCaptured(txn: anytype, namespace: [24]u8) !void {
    const state = try load(txn) orelse return;
    if (!std.mem.eql(u8, &state.namespace, &namespace)) return error.OnlineSourceScopeChanged;
    if (state.kind == .native) _ = try advance(txn, namespace, null);
}

pub fn requireRaftMarkerAllowed(txn: anytype) !void {
    if (try load(txn)) |state| if (state.kind == .native) return error.OnlineSourceScopeChanged;
}
