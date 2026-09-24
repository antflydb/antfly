// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! A deliberately narrow physical classifier for retained-owner controls.
//! Classification does not grant acceptance: the pool still fences every
//! transition until one owner can pay for all WAL writes and one checkpoint.
const std = @import("std");
const shape = @import("../completion_control_budget.zig");
const begin = @import("completion_control_begin.zig");
const record = @import("completion_control_record.zig");
const slot = @import("completion_slot.zig");

pub const Transition = union(enum) {
    decision: record.Progress.Decision,
    acknowledgement: struct { count: u32, resolved_digest: [32]u8 },
};

pub fn inspect(
    operations: []const slot.Operation,
    declaration: begin.Declaration,
    old_record: []const u8,
    participants: []const u8,
    old_resolved: ?[]const u8,
) !Transition {
    if (!declaration.coordinator or declaration.participants.count == 0 or
        old_record.len != shape.txn_record_v6_size or old_record[50] != @intFromBool(declaration.coordinator) or
        old_record[51] != @intFromBool(declaration.retain_terminal) or
        !std.meta.eql(try record.Participants.fromEncodedList(participants), declaration.participants))
        return error.UnsupportedCompletionProfile;
    var record_write: ?[]const u8 = null;
    var resolved_write: ?[]const u8 = null;
    var intent_admission = false;
    var intent_keys = false;
    var schema_lease = false;
    var read_admission = false;
    var completion_delete = false;
    var summary_write: ?[]const u8 = null;
    for (operations) |op| {
        if (op.bindings.len != 0) return error.UnsupportedCompletionProfile;
        if (matches(op.key, shape.records_prefix, declaration.txn_id)) {
            if (record_write != null or op.kind != .put) return error.UnsupportedCompletionProfile;
            record_write = op.value;
        } else if (matches(op.key, shape.resolved_participants_prefix, declaration.txn_id)) {
            if (resolved_write != null or op.kind != .put) return error.UnsupportedCompletionProfile;
            resolved_write = op.value;
        } else if (matches(op.key, shape.intent_admission_prefix, declaration.txn_id)) {
            try deleted(op, &intent_admission);
        } else if (matches(op.key, shape.intent_keys_prefix, declaration.txn_id)) {
            try deleted(op, &intent_keys);
        } else if (matches(op.key, shape.schema_leases_prefix, declaration.txn_id)) {
            try deleted(op, &schema_lease);
        } else if (matches(op.key, "\x00\x00__txn_read_admission__:", declaration.txn_id)) {
            try deleted(op, &read_admission);
        } else if (matches(op.key, shape.completion_prefix, declaration.txn_id)) {
            try deleted(op, &completion_delete);
        } else if (std.mem.eql(u8, op.key, shape.completion_summary_key)) {
            if (summary_write != null or op.kind != .put or op.value.len != 16) return error.UnsupportedCompletionProfile;
            summary_write = op.value;
        } else return error.UnsupportedCompletionProfile;
    }
    if (record_write) |next| {
        if (resolved_write != null or old_resolved != null or
            !intent_admission or !intent_keys or !schema_lease or !read_admission or
            completion_delete or summary_write != null or operations.len != 5 or
            old_record[0] != 0 or old_record[49] != 0 or old_record[52] != 0 or
            next.len != shape.txn_record_v6_size or next[52] != 1 or
            !std.mem.eql(u8, old_record[1..9], next[1..9]) or
            !std.mem.eql(u8, old_record[17..25], next[17..25]) or
            !std.mem.eql(u8, old_record[33..52], next[33..52]) or
            std.mem.readInt(u64, next[25..33], .little) == 0)
            return error.UnsupportedCompletionProfile;
        const decision: record.Progress.Decision = switch (next[0]) {
            1 => .committed,
            2 => .aborted,
            else => return error.UnsupportedCompletionProfile,
        };
        const commit = std.mem.readInt(u64, next[9..17], .little);
        const finalized = std.mem.readInt(u64, next[25..33], .little);
        if (std.mem.readInt(u64, old_record[9..17], .little) != 0 or
            std.mem.readInt(u64, old_record[25..33], .little) != 0 or
            (decision == .committed and commit != finalized) or
            (decision == .aborted and commit != 0)) return error.UnsupportedCompletionProfile;
        return .{ .decision = decision };
    }
    const next_resolved = resolved_write orelse return error.UnsupportedCompletionProfile;
    if (intent_admission or intent_keys or schema_lease or read_admission or
        old_record[0] == 0 or old_record[0] > 2 or old_record[52] != 1 or
        (completion_delete != (summary_write != null))) return error.UnsupportedCompletionProfile;
    var previous = if (old_resolved) |bytes| try shape.ParticipantIterator.init(bytes) else null;
    const previous_count: u32 = if (previous) |iterator| iterator.count else 0;
    var next = try shape.ParticipantIterator.init(next_resolved);
    const expected_count = std.math.add(u32, previous_count, 1) catch return error.UnsupportedCompletionProfile;
    if (next.count != expected_count or next.count > declaration.participants.count or
        completion_delete != (next.count == declaration.participants.count) or
        operations.len != (if (completion_delete) @as(usize, 3) else 1)) return error.UnsupportedCompletionProfile;
    for (0..previous_count) |_| {
        const old_name = (try previous.?.next()) orelse return error.UnsupportedCompletionProfile;
        const new_name = (try next.next()) orelse return error.UnsupportedCompletionProfile;
        if (!std.mem.eql(u8, old_name, new_name)) return error.UnsupportedCompletionProfile;
    }
    const name = (try next.next()) orelse return error.UnsupportedCompletionProfile;
    if ((try next.next()) != null) return error.UnsupportedCompletionProfile;
    if (old_resolved) |encoded| {
        var old_names = try shape.ParticipantIterator.init(encoded);
        while (try old_names.next()) |old_name|
            if (std.mem.eql(u8, name, old_name)) return error.UnsupportedCompletionProfile;
    }
    var enlisted = false;
    var names = try shape.ParticipantIterator.init(participants);
    while (try names.next()) |candidate| {
        if (std.mem.eql(u8, name, candidate)) enlisted = true;
    }
    if (!enlisted) return error.UnsupportedCompletionProfile;
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly-completion-control-resolved-v1\x00");
    hash.update(next_resolved);
    return .{ .acknowledgement = .{ .count = next.count, .resolved_digest = hash.finalResult() } };
}

fn matches(key: []const u8, comptime prefix: []const u8, txn_id: [16]u8) bool {
    return key.len == prefix.len + 16 and std.mem.startsWith(u8, key, prefix) and
        std.mem.eql(u8, key[prefix.len..], &txn_id);
}

fn deleted(op: slot.Operation, seen: *bool) !void {
    if (seen.* or op.kind != .delete or op.value.len != 0) return error.UnsupportedCompletionProfile;
    seen.* = true;
}
