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

//! Retained directory summary computation. Runs and child summary operands
//! remain immutable and pinned through completion. Caller prepays the fixed
//! cursor header. No allocation, root mutation or original-port retention.
const std = @import("std");
const Run = @import("repository.zig").Run;
const Comparison = @import("run_comparison.zig").Cursor;
const admission = @import("../../common/read_admission.zig");
pub const Summary = struct {
    smallest: ?*const Run = null,
    largest: ?*const Run = null,
    non_async_runs: usize = 0,
    tombstone_runs: usize = 0,
    unknown_tombstone_runs: usize = 0,
    gc_requested: bool = false,
    oldest_tombstone: u64 = std.math.maxInt(u64),
    newest_tombstone: u64 = 0,
};
pub const UpperSummary = struct { largest: ?*const Run = null };
pub const Full = Preparation(Summary, true);
pub const Upper = Preparation(UpperSummary, false);

fn Preparation(comptime Result: type, comptime full: bool) type {
    return struct {
        left: Result,
        right: Result,
        value: Result,
        phase: usize = 0,
        comparison: ?Comparison = null,
        candidate: ?*const Run = null,
        compared_bytes: usize = 0,
        pub fn init(entry: *const Run, left: Result, right: Result) @This() {
            const deletes = (entry.tombstone_count orelse 0) != 0;
            const value: Result = if (full) .{
                .smallest = entry,
                .largest = entry,
                .non_async_runs = left.non_async_runs + right.non_async_runs + @intFromBool(entry.path == null or entry.state != null),
                .tombstone_runs = left.tombstone_runs + right.tombstone_runs + @intFromBool(deletes),
                .unknown_tombstone_runs = left.unknown_tombstone_runs + right.unknown_tombstone_runs + @intFromBool(entry.tombstone_count == null),
                .gc_requested = left.gc_requested or right.gc_requested or entry.gc_requested,
                .oldest_tombstone = @min(left.oldest_tombstone, right.oldest_tombstone, if (deletes) entry.oldest_tombstone_unix_ns else std.math.maxInt(u64)),
                .newest_tombstone = @max(left.newest_tombstone, right.newest_tombstone, if (deletes) entry.oldest_tombstone_unix_ns else 0),
            } else .{ .largest = entry };
            return .{ .left = left, .right = right, .value = value };
        }
        pub fn resumeWithAdmission(self: *@This(), original: ?admission.Port) !Result {
            if (original) |port| try port.checkpoint();
            while (self.phase < if (full) @as(usize, 4) else 2) {
                if (original) |port| try port.checkpoint();
                const smallest = full and self.phase < 2;
                if (self.comparison == null) {
                    const child = if (self.phase % 2 == 0) self.left else self.right;
                    const candidate = if (full) (if (smallest) child.smallest else child.largest) else child.largest;
                    if (candidate) |run| {
                        self.candidate = run;
                        if (full) {
                            if (smallest) {
                                const current = self.value.smallest.?;
                                self.comparison = .bound(run.smallest_namespace_name, run.smallest_key, current.smallest_namespace_name, current.smallest_key);
                            } else self.beginUpper(run);
                        } else self.beginUpper(run);
                    } else {
                        self.phase += 1;
                        continue;
                    }
                }
                const before = self.comparison.?.compared_bytes;
                const order = self.comparison.?.resumeWithAdmission(original) catch |cause| {
                    self.compared_bytes += self.comparison.?.compared_bytes - before;
                    return cause;
                };
                self.compared_bytes += self.comparison.?.compared_bytes - before;
                if (order == .lt) {
                    if (full) {
                        if (smallest) self.value.smallest = self.candidate else self.value.largest = self.candidate;
                    } else self.value.largest = self.candidate;
                }
                self.comparison = null;
                self.candidate = null;
                self.phase += 1;
            }
            return self.value;
        }
        fn beginUpper(self: *@This(), run: *const Run) void {
            const current = self.value.largest.?;
            self.comparison = .bound(current.largest_namespace_name, current.largest_key, run.largest_namespace_name, run.largest_key);
        }
    };
}

fn boundOracle(a_ns: ?[]const u8, a: []const u8, b_ns: ?[]const u8, b: []const u8) std.math.Order {
    if (a_ns == null and b_ns != null) return .lt;
    if (a_ns != null and b_ns == null) return .gt;
    if (a_ns) |name| {
        const order = std.mem.order(u8, name, b_ns.?);
        if (order != .eq) return order;
    }
    return std.mem.order(u8, a, b);
}
fn oracle(entry: *const Run, left: Summary, right: Summary) Summary {
    const deletes = (entry.tombstone_count orelse 0) != 0;
    var out: Summary = .{
        .smallest = entry,
        .largest = entry,
        .non_async_runs = left.non_async_runs + right.non_async_runs + @intFromBool(entry.path == null or entry.state != null),
        .tombstone_runs = left.tombstone_runs + right.tombstone_runs + @intFromBool(deletes),
        .unknown_tombstone_runs = left.unknown_tombstone_runs + right.unknown_tombstone_runs + @intFromBool(entry.tombstone_count == null),
        .gc_requested = left.gc_requested or right.gc_requested or entry.gc_requested,
        .oldest_tombstone = @min(left.oldest_tombstone, right.oldest_tombstone, if (deletes) entry.oldest_tombstone_unix_ns else std.math.maxInt(u64)),
        .newest_tombstone = @max(left.newest_tombstone, right.newest_tombstone, if (deletes) entry.oldest_tombstone_unix_ns else 0),
    };
    for ([_]?*const Run{ left.smallest, right.smallest }) |candidate| if (candidate) |run| {
        if (boundOracle(run.smallest_namespace_name, run.smallest_key, out.smallest.?.smallest_namespace_name, out.smallest.?.smallest_key) == .lt) out.smallest = run;
    };
    for ([_]?*const Run{ left.largest, right.largest }) |candidate| if (candidate) |run| {
        if (boundOracle(run.largest_namespace_name, run.largest_key, out.largest.?.largest_namespace_name, out.largest.?.largest_key) == .gt) out.largest = run;
    };
    return out;
}
fn makeRun(id: u64, key: []u8) Run {
    return .{ .id = id, .level = 1, .size_bytes = 1, .path = @constCast("/summary.tbl"), .smallest_namespace_name = null, .smallest_key = key, .largest_namespace_name = null, .largest_key = key, .entry_count = 1, .bloom_filter = null, .state = null, .owns_metadata = false };
}

test "controlled physical sync retained run summaries preserve bound tie order and all scalar aggregates" {
    var runs: [12]Run = undefined;
    const names = [_]?[]u8{ null, @constCast(""), @constCast("docs") };
    const keys = [_][]u8{ @constCast("a"), @constCast("z"), @constCast("") };
    for (&runs, 0..) |*value, i| {
        value.* = makeRun(i, keys[i % 3]);
        value.smallest_namespace_name = names[i % 3];
        value.largest_namespace_name = names[(i / 3) % 3];
        value.tombstone_count = if (i % 3 == 0) null else @as(u32, @intCast(i % 3));
        value.oldest_tombstone_unix_ns = i * 11;
        value.gc_requested = i % 2 == 1;
        if (i % 4 == 0) value.path = null;
        if (i % 5 == 0) value.state = .{};
    }
    for (&runs) |*entry| for (&runs) |*child| {
        const left: Summary = .{ .smallest = child, .largest = child, .tombstone_runs = 3, .unknown_tombstone_runs = 7, .non_async_runs = 4, .oldest_tombstone = 9, .newest_tombstone = 117 };
        const right: Summary = .{ .smallest = entry, .largest = entry, .tombstone_runs = 5, .non_async_runs = 2, .unknown_tombstone_runs = 2, .gc_requested = true, .oldest_tombstone = 23, .newest_tombstone = 91 };
        var full = Full.init(entry, left, right);
        const expected = oracle(entry, left, right);
        try std.testing.expectEqualDeep(expected, try full.resumeWithAdmission(null));
        var upper = Upper.init(entry, .{ .largest = child }, .{ .largest = entry });
        try std.testing.expectEqual(expected.largest, (try upper.resumeWithAdmission(null)).largest);
    };
    var empty = Full.init(&runs[0], .{}, .{});
    try std.testing.expectEqualDeep(oracle(&runs[0], .{}, .{}), try empty.resumeWithAdmission(null));
}

test "controlled physical sync retained summary comparisons retain every original rejection and avoid prefix replay" {
    const a = std.testing.allocator;
    const bytes = try a.alloc(u8, 2 * Comparison.quantum + 9);
    defer a.free(bytes);
    @memset(bytes, 'n');
    const other = try a.dupe(u8, bytes);
    defer a.free(other);
    other[other.len - 1] = 'm';
    var entry = makeRun(1, bytes);
    var child = makeRun(2, other);
    entry.smallest_namespace_name = bytes;
    entry.largest_namespace_name = bytes;
    child.smallest_namespace_name = bytes;
    child.largest_namespace_name = bytes;
    inline for (.{ true, false }) |full| {
        const Cursor = if (full) Full else Upper;
        const Result = if (full) Summary else UpperSummary;
        const Fault = struct {
            checks: usize = 0,
            at: usize = std.math.maxInt(usize),
            failure: anyerror = error.Canceled,
            fn check(raw: *anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                const at = self.checks;
                self.checks += 1;
                if (at == self.at) return self.failure;
            }
        };
        const left: Result = if (full) .{ .smallest = &child, .largest = &child } else .{ .largest = &child };
        const right = left;
        var checks: usize = 1;
        var target: usize = 0;
        var compared: usize = 0;
        while (target <= checks) : (target += 1) {
            var cursor = Cursor.init(&entry, left, right);
            var fault: Fault = .{ .at = if (target == 0) std.math.maxInt(usize) else target - 1, .failure = switch (target % 3) {
                0 => error.Canceled,
                1 => error.DeadlineExceeded,
                else => error.FileNotFound,
            } };
            const payload: admission.Payload = .{ .ptr = &fault, .vtable = &.{ .checkpoint = Fault.check } };
            const original = admission.Port.init(&payload);
            const result = cursor.resumeWithAdmission(original);
            if (target == 0) {
                _ = try result;
                checks = fault.checks;
                compared = cursor.compared_bytes;
            } else {
                try std.testing.expectError(fault.failure, result);
                _ = try cursor.resumeWithAdmission(original);
            }
            const expected = oracle(&entry, .{ .smallest = &child, .largest = &child }, .{ .smallest = &child, .largest = &child });
            const got = try cursor.resumeWithAdmission(original);
            if (full) try std.testing.expectEqualDeep(expected, got) else try std.testing.expectEqual(expected.largest, got.largest);
            try std.testing.expectEqual(compared, cursor.compared_bytes);
            fault.at = fault.checks;
            try std.testing.expectError(fault.failure, cursor.resumeWithAdmission(original));
            try std.testing.expectEqual(compared, cursor.compared_bytes);
        }
    }
}
