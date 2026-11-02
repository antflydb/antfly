// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Compute a stable request capsule's decoded-input charge once, under its
//! existing execution owner. Values and keys must remain immutable throughout
//! traversal; the capsule retains the result across all subsequent phases.
const std = @import("std");
const Control = @import("sql_json_source.zig").Control;

/// A borrowed immutable input view and its once-computed admission charge.
/// The capsule owning values/keys must outlive this view and every consumer.
pub const Captured = struct {
    values: []const std.json.Value,
    bytes: u64,

    pub fn capture(allocator: std.mem.Allocator, values: []const std.json.Value, control: Control) !Captured {
        return .{ .values = values, .bytes = try parameterBytes(allocator, values, control) };
    }

    pub fn requestBytes(self: Captured, statement_bytes: usize) u64 {
        return @as(u64, statement_bytes) +| self.bytes +| 1024;
    }
};

/// Preserve bounded_admission.parameterBytes' conservative accounting while
/// replacing recursive traversal with a checked, allocator-owned frame stack.
/// The caller supplies a charged allocator and already admitted authority;
/// this function never starts a root or refreshes a cancellation/deadline fence.
pub fn parameterBytes(allocator: std.mem.Allocator, values: []const std.json.Value, control: Control) !u64 {
    try control.check(control.ptr);
    var frames: Frames = .{};
    defer frames.deinit(allocator);
    try frames.push(allocator, .{ .values = values }, control);
    var bytes = @as(u64, values.len) *| @sizeOf(std.json.Value);
    var work: usize = 0;
    while (frames.len != 0) {
        if (work % 64 == 0) try control.check(control.ptr);
        work +%= 1;
        const frame = &frames.storage()[frames.len - 1];
        if (frame.index == frame.values.len) {
            frames.len -= 1;
            continue;
        }
        const value = frame.values[frame.index];
        if (frame.keys) |keys| bytes +|= keys[frame.index].len;
        frame.index += 1;
        // No frame pointer is used after push can replace its storage.
        switch (value) {
            .string, .number_string => |text| bytes +|= text.len,
            .array => |array| {
                bytes +|= @as(u64, array.capacity) *| @sizeOf(std.json.Value);
                if (array.items.len != 0) try frames.push(allocator, .{ .values = array.items }, control);
            },
            .object => |object| {
                // Existing admission counts each object value both in map
                // metadata and in its singleton parameter traversal.
                bytes +|= @as(u64, object.count()) *| (2 * @sizeOf(std.json.Value) + @sizeOf([]const u8));
                if (object.count() != 0) try frames.push(allocator, .{ .values = object.values(), .keys = object.keys() }, control);
            },
            else => {},
        }
    }
    try control.check(control.ptr);
    return bytes;
}

const Frame = struct {
    values: []const std.json.Value,
    keys: ?[]const []const u8 = null,
    index: usize = 0,
};

const Frames = struct {
    small: [16]Frame = undefined,
    allocated: ?[]Frame = null,
    len: usize = 0,

    fn storage(self: *Frames) []Frame {
        return self.allocated orelse &self.small;
    }

    fn deinit(self: *Frames, allocator: std.mem.Allocator) void {
        if (self.allocated) |owned| allocator.free(owned);
        self.* = undefined;
    }

    fn push(self: *Frames, allocator: std.mem.Allocator, frame: Frame, control: Control) !void {
        try control.check(control.ptr);
        const old = self.storage();
        if (self.len == old.len) {
            const capacity = std.math.mul(usize, old.len, 2) catch return error.OutOfMemory;
            const replacement = try allocator.alloc(Frame, capacity);
            errdefer allocator.free(replacement);
            const chunk = @max(@as(usize, 1), 4096 / @sizeOf(Frame));
            var offset: usize = 0;
            while (offset < self.len) {
                try control.check(control.ptr);
                const end = offset + @min(chunk, self.len - offset);
                @memcpy(replacement[offset..end], old[offset..end]);
                offset = end;
            }
            try control.check(control.ptr);
            if (self.allocated) |owned| allocator.free(owned);
            self.allocated = replacement;
        }
        self.storage()[self.len] = frame;
        self.len += 1;
    }
};

const TestControl = struct {
    checks: usize = 0,
    fail_at: usize = 0,
    failure: anyerror = error.Canceled,
    fn check(raw: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.checks += 1;
        if (self.fail_at != 0 and self.checks == self.fail_at) return self.failure;
    }
    fn control(self: *@This()) Control {
        return .{ .ptr = self, .check = check };
    }
};

test "SQL parameter accounting preserves existing arrays objects keys and exact-number charges" {
    var parsed = try std.json.parseFromSlice([]const std.json.Value, std.testing.allocator, "[\"hello\",9007199254740993,null,[true,\"nested\"],{\"key\":\"value\",\"nested\":[false,null]}]", .{ .parse_numbers = false });
    defer parsed.deinit();
    var control: TestControl = .{};
    const actual = try parameterBytes(std.testing.allocator, parsed.value, control.control());
    const expected = @import("antfly_local_sources").common_bounded_admission.parameterBytes(parsed.value);
    try std.testing.expectEqual(expected, actual);
    try std.testing.expect(control.checks > 1);
}

test "SQL captured parameter charge is reused without another traversal or authority check" {
    var parsed = try std.json.parseFromSlice([]const std.json.Value, std.testing.allocator, "[[9007199254740993,{\"key\":\"retained value\"}],null]", .{ .parse_numbers = false });
    defer parsed.deinit();
    var control: TestControl = .{};
    const captured = try Captured.capture(std.testing.allocator, parsed.value, control.control());
    const checks = control.checks;
    try std.testing.expect(captured.values.ptr == parsed.value.ptr);
    try std.testing.expectEqual(@import("antfly_local_sources").common_bounded_admission.parameterBytes(parsed.value), captured.bytes);
    // Later phases read the immutable charge even when the original authority
    // has expired. They still apply their original admission/lifetime fences;
    // accounting never creates a new root or refreshes that authority.
    control.fail_at = checks + 1;
    for ([_]usize{ 0, 8, 4096, 8 }) |statement_bytes|
        try std.testing.expectEqual(@as(u64, statement_bytes) +| captured.bytes +| 1024, captured.requestBytes(statement_bytes));
    try std.testing.expectEqual(checks, control.checks);
    const saturated: Captured = .{ .values = &.{}, .bytes = std.math.maxInt(u64) };
    try std.testing.expectEqual(std.math.maxInt(u64), saturated.requestBytes(1));
}

test "SQL parameter accounting deep traversal is iterative and preserves original interruption" {
    const depth = 4096;
    const nodes = try std.testing.allocator.alloc(std.json.Value, depth + 1);
    defer std.testing.allocator.free(nodes);
    for (0..depth) |index| nodes[index] = .{ .array = .{
        .items = nodes[index + 1 .. index + 2],
        .capacity = 1,
        .allocator = std.testing.allocator,
        .pointer_stability = .{},
    } };
    nodes[depth] = .null;
    var control: TestControl = .{};
    try std.testing.expectEqual(@as(u64, depth + 1) * @sizeOf(std.json.Value), try parameterBytes(std.testing.allocator, nodes[0..1], control.control()));
    try std.testing.expect(control.checks > depth);
    for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.RunnableWaitTimeout, error.SchedulerClosed }) |failure| {
        var interrupted: TestControl = .{ .fail_at = 64, .failure = failure };
        try std.testing.expectError(failure, parameterBytes(std.testing.allocator, nodes[0..1], interrupted.control()));
        try std.testing.expectEqual(@as(usize, 64), interrupted.checks);
    }
}

test "SQL parameter accounting frame allocation faults retire all traversal storage" {
    const Fixture = struct {
        fn run(allocator: std.mem.Allocator, nodes: []const std.json.Value) !void {
            var control: TestControl = .{};
            try std.testing.expectEqual(@as(u64, 257) * @sizeOf(std.json.Value), try parameterBytes(allocator, nodes[0..1], control.control()));
        }
    };
    const depth = 256;
    const nodes = try std.testing.allocator.alloc(std.json.Value, depth + 1);
    defer std.testing.allocator.free(nodes);
    for (0..depth) |index| nodes[index] = .{ .array = .{
        .items = nodes[index + 1 .. index + 2],
        .capacity = 1,
        .allocator = std.testing.allocator,
        .pointer_stability = .{},
    } };
    nodes[depth] = .null;
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{nodes});
}

test "SQL parameter accounting NoResize frame growth retains old storage through interrupted copy" {
    const Owner = struct {
        live_bytes: usize = 0,
        live_allocations: usize = 0,
        interrupted_growth: bool = false,
        fn alloc(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const bytes = std.testing.allocator.rawAlloc(len, alignment, ra) orelse return null;
            self.live_bytes += len;
            self.live_allocations += 1;
            return bytes;
        }
        fn free(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ra: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.testing.allocator.rawFree(bytes, alignment, ra);
            self.live_bytes -= bytes.len;
            self.live_allocations -= 1;
        }
        fn allocator(self: *@This()) std.mem.Allocator {
            return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = std.mem.Allocator.noResize, .remap = std.mem.Allocator.noRemap, .free = free } };
        }
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.live_allocations == 2) {
                self.interrupted_growth = true;
                return error.DeadlineExceeded;
            }
        }
    };
    const nodes = try std.testing.allocator.alloc(std.json.Value, 129);
    defer std.testing.allocator.free(nodes);
    for (0..128) |index| nodes[index] = .{ .array = .{
        .items = nodes[index + 1 .. index + 2],
        .capacity = 1,
        .allocator = std.testing.allocator,
        .pointer_stability = .{},
    } };
    nodes[128] = .null;
    var owner: Owner = .{};
    try std.testing.expectError(error.DeadlineExceeded, parameterBytes(owner.allocator(), nodes[0..1], .{ .ptr = &owner, .check = Owner.check }));
    try std.testing.expect(owner.interrupted_growth);
    try std.testing.expectEqual(@as(usize, 0), owner.live_bytes);
    try std.testing.expectEqual(@as(usize, 0), owner.live_allocations);
}
