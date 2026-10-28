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

const admission = @import("../common/read_admission.zig");

/// The caller aborts its owned batch on rejection. Every check precedes commit;
/// backend callback internals and accepted commit retain their existing policy.
pub fn apply(batch: anytype, writes: anytype, deletes: anytype, replay: anytype, control: ?admission.Port) !void {
    try stage(batch, writes, deletes, replay, control);
    try batch.commit();
}

/// Stage only, allowing the owner to attach resource grants in this same batch.
/// A failed grant must abort the batch; it must never publish these writes alone.
pub fn stage(batch: anytype, writes: anytype, deletes: anytype, replay: anytype, control: ?admission.Port) !void {
    if (control) |owner| try owner.checkpoint();
    for (deletes) |key| {
        if (control) |owner| try owner.checkpoint();
        batch.delete(key) catch |err| switch (err) {
            error.NotFound => {},
            else => return err,
        };
    }
    for (writes) |kv| {
        if (control) |owner| try owner.checkpoint();
        try batch.put(kv.key, kv.value);
    }
    if (replay) |entry| {
        if (control) |owner| try owner.checkpoint();
        try batch.setReplayOpaque(entry.sequence, entry.payload);
    }
    if (control) |owner| try owner.checkpoint();
}

const std = @import("std");
const KV = @import("docstore.zig").KVPair;
pub const Replay = struct { sequence: u64, payload: []const u8 };

/// Detached physical mutation bytes. This owns data, not the caller's apply,
/// catalog, policy or native commit-participant leases. Capturing it does not
/// publish a decision or qualify an executable recovery bundle.
pub const OwnedBatch = struct {
    allocator: std.mem.Allocator,
    backing: []u8,
    writes: []const KV,
    deletes: []const []const u8,
    replay: ?Replay,

    pub fn deinit(self: *@This()) void {
        self.allocator.free(self.backing);
        self.* = undefined;
    }

    /// One allocation, including descriptor arrays and worst-case alignment
    /// for an arbitrary byte-aligned backing allocator. Payload bytes are exact;
    /// allocator overhead and backend staging remain separate resource owners.
    pub fn allocationBytes(writes: []const KV, deletes: []const []const u8, replay: ?Replay, control: ?admission.Port) !usize {
        var total: usize = @alignOf(KV) - 1 + @alignOf([]const u8) - 1;
        total = try sum(total, std.math.mul(usize, writes.len, @sizeOf(KV)) catch return error.TransactionTooLarge);
        total = try sum(total, std.math.mul(usize, deletes.len, @sizeOf([]const u8)) catch return error.TransactionTooLarge);
        for (writes) |write| {
            if (control) |owner| try owner.checkpoint();
            total = try sum(total, write.key.len);
            total = try sum(total, write.value.len);
        }
        for (deletes) |key| {
            if (control) |owner| try owner.checkpoint();
            total = try sum(total, key.len);
        }
        if (replay) |entry| total = try sum(total, entry.payload.len);
        if (control) |owner| try owner.checkpoint();
        return total;
    }

    pub fn capture(allocator: std.mem.Allocator, writes: []const KV, deletes: []const []const u8, replay: ?Replay, control: ?admission.Port) !OwnedBatch {
        const bytes = try allocationBytes(writes, deletes, replay, control);
        const backing = try allocator.alloc(u8, bytes);
        errdefer allocator.free(backing);
        var arena = std.heap.FixedBufferAllocator.init(backing);
        const a = arena.allocator();
        const owned_writes = try a.alloc(KV, writes.len);
        const owned_deletes = try a.alloc([]const u8, deletes.len);
        for (writes, owned_writes) |write, *owned| {
            owned.* = .{ .key = try copy(a, write.key, control), .value = try copy(a, write.value, control) };
        }
        for (deletes, owned_deletes) |key, *owned| owned.* = try copy(a, key, control);
        const owned_replay: ?Replay = if (replay) |entry| .{ .sequence = entry.sequence, .payload = try copy(a, entry.payload, control) } else null;
        if (control) |owner| try owner.checkpoint();
        return .{ .allocator = allocator, .backing = backing, .writes = owned_writes, .deletes = owned_deletes, .replay = owned_replay };
    }

    fn sum(left: usize, right: usize) !usize {
        return std.math.add(usize, left, right) catch error.TransactionTooLarge;
    }
    fn copy(a: std.mem.Allocator, source: []const u8, control: ?admission.Port) ![]const u8 {
        if (control) |owner| try owner.checkpoint();
        const target = try a.alloc(u8, source.len);
        var offset: usize = 0;
        while (offset < source.len) {
            if (control) |owner| try owner.checkpoint();
            const end = offset + @min(@as(usize, 4096), source.len - offset);
            @memcpy(target[offset..end], source[offset..end]);
            offset = end;
        }
        return target;
    }
};

test "prepared transaction effects owned mutation bytes outlive every input buffer and preserve order" {
    const a = std.testing.allocator;
    var captured = blk: {
        const key = try a.dupe(u8, "row\x00id");
        defer a.free(key);
        const value = try a.dupe(u8, "original");
        defer a.free(value);
        const deleted = try a.dupe(u8, "removed");
        defer a.free(deleted);
        const replay = try a.dupe(u8, "opaque-replay");
        defer a.free(replay);
        const writes = [_]KV{ .{ .key = key, .value = value }, .{ .key = "same", .value = "first" }, .{ .key = "same", .value = "last" } };
        const result = try OwnedBatch.capture(a, &writes, &.{ deleted, "" }, .{ .sequence = 42, .payload = replay }, null);
        @memset(key, 'k');
        @memset(value, 'v');
        @memset(deleted, 'd');
        @memset(replay, 'r');
        break :blk result;
    };
    defer captured.deinit();
    try std.testing.expectEqualStrings("row\x00id", captured.writes[0].key);
    try std.testing.expectEqualStrings("original", captured.writes[0].value);
    try std.testing.expectEqualStrings("first", captured.writes[1].value);
    try std.testing.expectEqualStrings("last", captured.writes[2].value);
    try std.testing.expectEqualStrings("removed", captured.deletes[0]);
    try std.testing.expectEqualStrings("", captured.deletes[1]);
    try std.testing.expectEqual(@as(u64, 42), captured.replay.?.sequence);
    try std.testing.expectEqualStrings("opaque-replay", captured.replay.?.payload);
}

test "prepared transaction effects owned mutation bound includes alignment with one backing allocation" {
    const a = std.testing.allocator;
    const value: [8193]u8 = @splat('v');
    const writes = [_]KV{ .{ .key = "k", .value = &value }, .{ .key = "", .value = "" } };
    const deletes = [_][]const u8{ "removed", "binary\x00key" };
    const replay: Replay = .{ .sequence = 9, .payload = "replay" };
    const bytes = try OwnedBatch.allocationBytes(&writes, &deletes, replay, null);
    const storage = try a.alloc(u8, bytes + 1);
    defer a.free(storage);
    // Force an unaligned byte backing. Exactly the measured span is available:
    // any second allocation from this allocator would fail.
    var bounded = std.heap.FixedBufferAllocator.init(storage[1..]);
    var owned = try OwnedBatch.capture(bounded.allocator(), &writes, &deletes, replay, null);
    defer owned.deinit();
    try std.testing.expectEqual(bytes, owned.backing.len);
    try std.testing.expectEqualSlices(u8, &value, owned.writes[0].value);
    try std.testing.expectEqualStrings("binary\x00key", owned.deletes[1]);
    try std.testing.expectEqualStrings("replay", owned.replay.?.payload);
}

test "prepared transaction effects owned mutation capture unwinds allocation and every original checkpoint" {
    const Checks = struct {
        calls: usize = 0,
        stop: usize = std.math.maxInt(usize),
        cause: anyerror = error.DeadlineExceeded,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            if (self.calls == self.stop) return self.cause;
        }
        fn run(a: std.mem.Allocator, checks: ?*@This()) !void {
            const value: [13000]u8 = @splat('v');
            const writes = [_]KV{.{ .key = "row", .value = &value }};
            var payload: admission.Payload = .{ .ptr = checks orelse undefined, .vtable = &.{ .checkpoint = check } };
            var captured = try OwnedBatch.capture(a, &writes, &.{"removed"}, .{ .sequence = 1, .payload = &value }, if (checks != null) admission.Port.init(&payload) else null);
            defer captured.deinit();
            try std.testing.expectEqualSlices(u8, &value, captured.replay.?.payload);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Checks.run, .{@as(?*Checks, null)});
    var baseline: Checks = .{};
    try Checks.run(std.testing.allocator, &baseline);
    try std.testing.expect(baseline.calls > 10);
    for ([_]anyerror{ error.DeadlineExceeded, error.NotFound }) |cause| for (1..baseline.calls + 1) |stop| {
        var checks: Checks = .{ .stop = stop, .cause = cause };
        try std.testing.expectError(cause, Checks.run(std.testing.allocator, &checks));
    };
}
