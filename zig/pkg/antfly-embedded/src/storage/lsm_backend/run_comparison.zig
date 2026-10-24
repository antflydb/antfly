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

//! Allocation-free retained directory ordering. Operands' metadata/domain bytes
//! remain immutable and pinned until completion. Fixed caller-owned header;
//! no original port is stored. One byte comparison examines at most 16 KiB
//! between checkpoints. Null namespaces sort before present-empty namespaces.
const std = @import("std");
const Run = @import("repository.zig").Run;
const admission = @import("../../common/read_admission.zig");
pub const Mode = enum { read, id, lower, upper, domain };
pub const Operand = struct { run: *const Run, domain: []const u8 = "" };
pub const Cursor = struct {
    pub const quantum = 16 * 1024;
    const Part = union(enum) {
        number: struct { a: u64, b: u64 },
        bytes: struct { a: ?[]const u8, b: ?[]const u8 },
    };
    parts: [8]Part = undefined,
    count: usize = 0,
    part: usize = 0,
    offset: usize = 0,
    result: ?std.math.Order = null,
    compared_bytes: usize = 0,

    fn number(self: *Cursor, a: u64, b: u64) void {
        self.parts[self.count] = .{ .number = .{ .a = a, .b = b } };
        self.count += 1;
    }
    fn bytes(self: *Cursor, a: ?[]const u8, b: ?[]const u8) void {
        self.parts[self.count] = .{ .bytes = .{ .a = a, .b = b } };
        self.count += 1;
    }
    fn readPrefix(self: *Cursor, a: *const Run, b: *const Run) void {
        self.number(a.level, b.level);
        if (a.level == 0 and b.level == 0) self.number(if (b.visibility_id == 0) b.id else b.visibility_id, if (a.visibility_id == 0) a.id else a.visibility_id);
    }
    pub fn init(mode: Mode, a: Operand, b: Operand) Cursor {
        var out: Cursor = .{};
        switch (mode) {
            .id => out.number(a.run.id, b.run.id),
            .read => {
                out.readPrefix(a.run, b.run);
                out.bytes(a.run.smallest_namespace_name, b.run.smallest_namespace_name);
                out.bytes(a.run.smallest_key, b.run.smallest_key);
                out.number(a.run.id, b.run.id);
            },
            .lower => {
                out.bytes(a.run.smallest_namespace_name, b.run.smallest_namespace_name);
                out.bytes(a.run.smallest_key, b.run.smallest_key);
                out.readPrefix(a.run, b.run);
                // The primary comparison already established namespace/key equality.
                out.number(a.run.id, b.run.id);
            },
            .upper => {
                out.bytes(a.run.largest_namespace_name, b.run.largest_namespace_name);
                out.bytes(a.run.largest_key, b.run.largest_key);
                out.readPrefix(a.run, b.run);
                out.bytes(a.run.smallest_namespace_name, b.run.smallest_namespace_name);
                out.bytes(a.run.smallest_key, b.run.smallest_key);
                out.number(a.run.id, b.run.id);
            },
            .domain => {
                out.bytes(a.run.smallest_namespace_name, b.run.smallest_namespace_name);
                out.bytes(a.domain, b.domain);
                out.readPrefix(a.run, b.run);
                out.bytes(a.run.smallest_key, b.run.smallest_key);
                out.number(a.run.id, b.run.id);
            },
        }
        return out;
    }
    pub fn bound(a_ns: ?[]const u8, a: []const u8, b_ns: ?[]const u8, b: []const u8) Cursor {
        var out: Cursor = .{};
        out.bytes(a_ns, b_ns);
        out.bytes(a, b);
        return out;
    }
    fn checkpoint(original: ?admission.Port) !void {
        if (original) |port| try port.checkpoint();
    }
    pub fn resumeWithAdmission(self: *Cursor, original: ?admission.Port) !std.math.Order {
        try checkpoint(original);
        if (self.result) |result| return result;
        while (self.part < self.count) {
            try checkpoint(original);
            const order: std.math.Order = switch (self.parts[self.part]) {
                .number => |value| std.math.order(value.a, value.b),
                .bytes => |value| bytes: {
                    if (value.a == null) break :bytes if (value.b == null) .eq else .lt;
                    if (value.b == null) break :bytes .gt;
                    const a = value.a.?;
                    const b = value.b.?;
                    const len = @min(a.len, b.len);
                    while (self.offset < len) {
                        try checkpoint(original);
                        const end = self.offset + @min(quantum, len - self.offset);
                        const result = std.mem.order(u8, a[self.offset..end], b[self.offset..end]);
                        self.compared_bytes += end - self.offset;
                        self.offset = end;
                        if (result != .eq) break :bytes result;
                    }
                    break :bytes std.math.order(a.len, b.len);
                },
            };
            if (order != .eq) {
                self.result = order;
                return order;
            }
            self.part += 1;
            self.offset = 0;
        }
        self.result = .eq;
        return .eq;
    }
};
pub fn compare(mode: Mode, a: Operand, b: Operand) std.math.Order {
    var cursor = Cursor.init(mode, a, b);
    return cursor.resumeWithAdmission(null) catch unreachable;
}

fn nsOracle(a: ?[]const u8, b: ?[]const u8) std.math.Order {
    if (a == null) return if (b == null) .eq else .lt;
    if (b == null) return .gt;
    return std.mem.order(u8, a.?, b.?);
}
fn readOracle(a: *const Run, b: *const Run) std.math.Order {
    if (a.level != b.level) return std.math.order(a.level, b.level);
    if (a.level == 0) {
        const av = if (a.visibility_id == 0) a.id else a.visibility_id;
        const bv = if (b.visibility_id == 0) b.id else b.visibility_id;
        if (av != bv) return std.math.order(bv, av);
    }
    const ns = nsOracle(a.smallest_namespace_name, b.smallest_namespace_name);
    if (ns != .eq) return ns;
    const key = std.mem.order(u8, a.smallest_key, b.smallest_key);
    return if (key != .eq) key else std.math.order(a.id, b.id);
}
fn oracle(mode: Mode, a: Operand, b: Operand) std.math.Order {
    switch (mode) {
        .id => return std.math.order(a.run.id, b.run.id),
        .read => return readOracle(a.run, b.run),
        .lower, .upper => {
            const ns = if (mode == .lower) nsOracle(a.run.smallest_namespace_name, b.run.smallest_namespace_name) else nsOracle(a.run.largest_namespace_name, b.run.largest_namespace_name);
            if (ns != .eq) return ns;
            const key = if (mode == .lower) std.mem.order(u8, a.run.smallest_key, b.run.smallest_key) else std.mem.order(u8, a.run.largest_key, b.run.largest_key);
            return if (key != .eq) key else readOracle(a.run, b.run);
        },
        .domain => {
            const ns = nsOracle(a.run.smallest_namespace_name, b.run.smallest_namespace_name);
            if (ns != .eq) return ns;
            const domain = std.mem.order(u8, a.domain, b.domain);
            return if (domain != .eq) domain else readOracle(a.run, b.run);
        },
    }
}
fn run(id: u64, key: []u8) Run {
    return .{ .id = id, .level = 1, .size_bytes = 1, .path = @constCast("/comparison.tbl"), .smallest_namespace_name = null, .smallest_key = key, .largest_namespace_name = null, .largest_key = key, .entry_count = 1, .bloom_filter = null, .state = null, .owns_metadata = false };
}

test "controlled physical sync retained run comparisons match independent directory ordering for all key modes" {
    const a = std.testing.allocator;
    const long = try a.alloc(u8, 2 * Cursor.quantum + 3);
    defer a.free(long);
    @memset(long, 'm');
    const other = try a.dupe(u8, long);
    defer a.free(other);
    other[other.len - 1] = 'n';
    const names = [_]?[]u8{ null, @constCast(""), @constCast("docs"), long };
    const keys = [_][]u8{ @constCast(""), @constCast("a"), @constCast("aa"), long, other };
    const domains = [_][]const u8{ "", "partition", long, other };
    var runs: [24]Run = undefined;
    for (&runs, 0..) |*value, i| {
        value.* = run(i % 7 + 1, keys[(i / 2) % keys.len]);
        value.level = @intCast(i % 3);
        value.visibility_id = if (i % 4 == 0) 0 else 11;
        value.smallest_namespace_name = names[i % names.len];
        value.largest_namespace_name = names[(i / 4) % names.len];
        value.largest_key = keys[i % keys.len];
    }
    for (std.enums.values(Mode)) |mode| for (&runs, 0..) |*lhs, i| for (&runs, 0..) |*rhs, j| {
        const left: Operand = .{ .run = lhs, .domain = domains[i % domains.len] };
        const right: Operand = .{ .run = rhs, .domain = domains[j % domains.len] };
        try std.testing.expectEqual(oracle(mode, left, right), compare(mode, left, right));
    };
}

test "controlled physical sync retained comparison rejects every original checkpoint without replaying large prefixes" {
    const a = std.testing.allocator;
    const long = try a.alloc(u8, 2 * Cursor.quantum + 7);
    defer a.free(long);
    @memset(long, 'x');
    const other = try a.dupe(u8, long);
    defer a.free(other);
    other[other.len - 1] = 'y';
    var left = run(7, long);
    var right = run(9, other);
    left.smallest_namespace_name = long;
    right.smallest_namespace_name = long;
    left.largest_namespace_name = long;
    right.largest_namespace_name = long;
    const Fault = struct {
        cursor: *Cursor,
        checks: usize = 0,
        at: usize = std.math.maxInt(usize),
        last_bytes: usize = 0,
        failure: anyerror = error.Canceled,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expect(self.cursor.compared_bytes - self.last_bytes <= Cursor.quantum);
            self.last_bytes = self.cursor.compared_bytes;
            const at = self.checks;
            self.checks += 1;
            if (at == self.at) return self.failure;
        }
    };
    for (0..6) |case| {
        const mode: Mode = if (case < 5) @fromBackingInt(@intCast(case)) else .read;
        const l: Operand = .{ .run = &left, .domain = long };
        const r: Operand = .{ .run = &right, .domain = other };
        var checkpoints: usize = 1;
        var target: usize = 0;
        var successful_bytes: usize = 0;
        while (target <= checkpoints) : (target += 1) {
            var cursor = if (case == 5) Cursor.bound(left.smallest_namespace_name, left.smallest_key, right.smallest_namespace_name, right.smallest_key) else Cursor.init(mode, l, r);
            var fault: Fault = .{ .cursor = &cursor, .at = if (target == 0) std.math.maxInt(usize) else target - 1, .failure = switch (target % 3) {
                0 => error.Canceled,
                1 => error.DeadlineExceeded,
                else => error.FileNotFound,
            } };
            const payload: admission.Payload = .{ .ptr = &fault, .vtable = &.{ .checkpoint = Fault.check } };
            const original = admission.Port.init(&payload);
            const result = cursor.resumeWithAdmission(original);
            if (target == 0) {
                try std.testing.expectEqual(oracle(mode, l, r), try result);
                checkpoints = fault.checks;
                successful_bytes = cursor.compared_bytes;
            } else {
                try std.testing.expectError(fault.failure, result);
                try std.testing.expectEqual(oracle(mode, l, r), try cursor.resumeWithAdmission(original));
            }
            try std.testing.expectEqual(successful_bytes, cursor.compared_bytes);
            try std.testing.expect(cursor.compared_bytes - fault.last_bytes <= Cursor.quantum);
            try std.testing.expectEqual(oracle(mode, l, r), try cursor.resumeWithAdmission(original));
            // Cached completion still honors the next original's lifetime.
            const completed_bytes = cursor.compared_bytes;
            fault.at = fault.checks;
            try std.testing.expectError(fault.failure, cursor.resumeWithAdmission(original));
            try std.testing.expectEqual(completed_bytes, cursor.compared_bytes);
            try std.testing.expectEqual(oracle(mode, l, r), try cursor.resumeWithAdmission(original));
        }
    }
}
