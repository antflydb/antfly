// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Disjoint native output identities. Control ownership outlives document
//! cohort rearm; its persisted IDs must never be inferred from a new cohort.
const std = @import("std");
const capacity = @import("completion_capacity.zig");
const control_guard = @import("completion_control_guard.zig");
const control_record = @import("completion_control_record.zig");

pub const Held = struct {
    ids: [capacity.control_outputs]?u64 = @splat(null),
    mask: u8 = 0,
};

/// Guard framing/installation is checked here, not consensus authority or BEGIN
/// semantics. Actual owner restoration remains required before qualification.
pub fn loadHeld(alloc: std.mem.Allocator, storage: anytype, root: []const u8, authority: control_record.Authority) !Held {
    var result: Held = .{};
    for (0..capacity.control_outputs) |i| {
        if (try control_guard.load(alloc, storage, root, i, authority)) |owned_value| {
            var owned = owned_value;
            defer owned.deinit();
            const id = owned.guard.record.output_run_id;
            for (result.ids[0..i]) |previous| if (previous == id) return error.InvalidCompletionSlot;
            result.ids[i] = id;
            result.mask |= @as(u8, 1) << @intCast(i);
        }
    }
    return result;
}

pub const Plan = struct {
    document_base: u64,
    control_ids: [capacity.control_outputs]u64,
    next_run_id: u64,
};

fn successor(value: u64, amount: u64) !u64 {
    return std.math.add(u64, value, amount) catch error.UnsupportedCompletionProfile;
}

pub fn plan(next_run_id: u64, saved_document_base: ?u64, held: Held) !Plan {
    if (next_run_id == 0) return error.InvalidCompletionSlot;
    var next = next_run_id;
    for (held.ids, 0..) |id, i| {
        const marked = held.mask & (@as(u8, 1) << @intCast(i)) != 0;
        if (marked != (id != null)) return error.InvalidCompletionSlot;
        if (id) |value| {
            if (value == 0) return error.InvalidCompletionSlot;
            for (held.ids[0..i]) |previous| if (previous == value) return error.InvalidCompletionSlot;
            next = @max(next, try successor(value, 1));
        }
    }
    const doc_base = saved_document_base orelse next;
    if (doc_base == 0) return error.InvalidCompletionSlot;
    const doc_end = try successor(doc_base, capacity.document_outputs);
    for (held.ids) |id| if (id) |value| {
        if (value >= doc_base and value < doc_end) return error.InvalidCompletionSlot;
    };
    next = @max(next, doc_end);
    var result = Plan{ .document_base = doc_base, .control_ids = undefined, .next_run_id = undefined };
    for (held.ids, 0..) |id, i| {
        if (id) |value| {
            result.control_ids[i] = value;
        } else {
            result.control_ids[i] = next;
            next = try successor(next, 1);
        }
    }
    result.next_run_id = next;
    return result;
}

/// Only document-range outputs consume document drain positions. Control SSTs
/// can complete in any order without shifting the next document's output path.
pub fn nextDocumentIndex(runs: anytype, base: u64, slots: usize) !usize {
    if (base == 0 or slots == 0 or slots > capacity.document_outputs) return error.InvalidCompletionSlot;
    const end = try successor(base, slots);
    var mask: u8 = 0;
    for (0..runs.count()) |i| {
        const id = runs.at(i).id;
        if (id < base or id >= end) continue;
        const bit = @as(u8, 1) << @as(u3, @intCast(id - base));
        if (mask & bit != 0) return error.InvalidCompletionSlot;
        mask |= bit;
    }
    var count: usize = 0;
    for (0..slots) |i| {
        if (mask & (@as(u8, 1) << @intCast(i)) == 0) continue;
        if (i != count) return error.InvalidCompletionSlot;
        count += 1;
    }
    if (count == slots) return error.CompletionPlanCapacityExceeded;
    return count;
}

test "workload admission completion output layout retains control identity across document rearm" {
    const held: Held = .{ .ids = .{ 71, null, 90, null }, .mask = 0b0101 };
    const restored = try plan(64, 65, held);
    try std.testing.expectEqual(@as(u64, 65), restored.document_base);
    try std.testing.expectEqual([_]u64{ 71, 91, 90, 92 }, restored.control_ids);
    try std.testing.expectEqual(@as(u64, 93), restored.next_run_id);
    const rearmed = try plan(restored.next_run_id, null, held);
    try std.testing.expectEqual(@as(u64, 93), rearmed.document_base);
    try std.testing.expectEqual([_]u64{ 71, 97, 90, 98 }, rearmed.control_ids);
    try std.testing.expectError(error.InvalidCompletionSlot, plan(64, 70, held));
    try std.testing.expectError(error.InvalidCompletionSlot, plan(64, null, .{ .ids = .{ 71, null, 71, null }, .mask = 0b0101 }));
    try std.testing.expectError(error.InvalidCompletionSlot, plan(64, null, .{ .ids = held.ids, .mask = 0 }));
    try std.testing.expectError(error.UnsupportedCompletionProfile, plan(std.math.maxInt(u64) - 6, null, .{}));
    try std.testing.expectError(error.UnsupportedCompletionProfile, plan(1, null, .{ .ids = .{ std.math.maxInt(u64), null, null, null }, .mask = 1 }));
}

test "workload admission completion output layout ignores intervening control runs and rejects document holes" {
    const Runs = struct {
        ids: []const u64,
        fn count(self: @This()) usize {
            return self.ids.len;
        }
        fn at(self: @This(), index: usize) struct { id: u64 } {
            return .{ .id = self.ids[index] };
        }
    };
    try std.testing.expectEqual(@as(usize, 2), try nextDocumentIndex(Runs{ .ids = &.{ 1, 2, 100, 101, 104, 105, 106, 107 } }, 100, 4));
    try std.testing.expectEqual(@as(usize, 3), try nextDocumentIndex(Runs{ .ids = &.{ 100, 101, 102, 104, 105, 106, 107 } }, 100, 4));
    try std.testing.expectError(error.CompletionPlanCapacityExceeded, nextDocumentIndex(Runs{ .ids = &.{ 100, 101, 102, 103, 104, 105, 106, 107 } }, 100, 4));
    try std.testing.expectError(error.InvalidCompletionSlot, nextDocumentIndex(Runs{ .ids = &.{ 100, 102, 104 } }, 100, 4));
}
