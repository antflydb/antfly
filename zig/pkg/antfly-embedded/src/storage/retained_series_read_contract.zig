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

//! Private native retained-read admission; no physical storage dependencies.
pub const ArchiveStart = struct {
    sequence: u64,
    complete_from_ns: i128,
    closed_at_ns: i128,
    first_key: @import("../retained_dataset_types.zig").RowKey,
};

pub const Query = struct {
    fence: @import("retained_series_control.zig").Fence,
    start_ns: i128,
    end_ns: i128,
    admitted_ns: i128,
    archive: ?ArchiveStart = null,
    dimensions_json: ?[]const u8 = null,
    after: ?@import("../retained_dataset_types.zig").RowKey = null,
};
pub const Request = union(enum) {
    status,
    revocation: @import("retained_series_control.zig").Fence,
    poll: struct {
        fence: @import("retained_series_control.zig").Fence,
        root_incarnation: u128,
        cursor: @import("retained_series.zig").Cursor,
        limit: u16 = 8,
        include_latest: bool = false,
    },
};

/// Borrowed for one synchronous call only; never retained by a read session.
/// Boolean callbacks keep cancellation portable across compiled owners.
pub const Control = struct {
    deadline_ns: u64 = @import("std").math.maxInt(u64),
    cancellation_ctx: ?*const anyopaque = null,
    is_cancelled_fn: ?*const fn (*const anyopaque) bool = null,

    pub fn check(self: @This()) !void {
        if (self.is_cancelled_fn) |probe| if (probe(self.cancellation_ctx orelse return error.InvalidRetainedReadQuery)) return error.Canceled;
        if (self.deadline_ns != @import("std").math.maxInt(u64) and @import("antfly_platform").time.monotonicNs() >= self.deadline_ns) return error.Timeout;
    }
};

/// Buffered JSON serialization checks admission between bounded output chunks,
/// including large strings. A writer failure preserves its original identity.
pub fn stringifyAlloc(a: @import("std").mem.Allocator, value: anytype, options: @import("std").json.Stringify.Options, operation: Control) ![]u8 {
    const std = @import("std");
    try operation.check();
    const Writer = struct {
        output: std.Io.Writer.Allocating,
        operation: Control,
        buffer: [4096]u8 = undefined,
        writer: std.Io.Writer = undefined,
        failure: ?anyerror = null,
        fn send(self: *@This(), bytes: []const u8) !void {
            var position: usize = 0;
            while (position < bytes.len) {
                try self.operation.check();
                const end = @min(bytes.len, position + self.buffer.len);
                self.output.writer.writeAll(bytes[position..end]) catch return error.OutOfMemory;
                position = end;
            }
        }
        fn drain(w: *std.Io.Writer, data: []const []const u8, splat: usize) std.Io.Writer.Error!usize {
            const self: *@This() = @alignCast(@fieldParentPtr("writer", w));
            return self.drainInner(w, data, splat) catch |err| {
                self.failure = err;
                return error.WriteFailed;
            };
        }
        fn drainInner(self: *@This(), w: *std.Io.Writer, data: []const []const u8, splat: usize) !usize {
            try self.send(w.buffered());
            w.end = 0;
            var count: usize = 0;
            for (data[0 .. data.len - 1]) |bytes| {
                try self.send(bytes);
                count += bytes.len;
            }
            const pattern = data[data.len - 1];
            for (0..splat) |_| try self.send(pattern);
            return count + pattern.len * splat;
        }
    };
    var writer: Writer = .{ .output = .init(a), .operation = operation };
    defer writer.output.deinit();
    writer.writer = .{ .vtable = &.{ .drain = Writer.drain }, .buffer = &writer.buffer };
    std.json.Stringify.value(value, options, &writer.writer) catch |err| return writer.failure orelse err;
    writer.writer.flush() catch |err| return writer.failure orelse err;
    try operation.check();
    return writer.output.toOwnedSlice() catch return error.OutOfMemory;
}

test "retained series controlled serialization interrupts large strings and releases allocation failures" {
    const std = @import("std");
    const a = std.testing.allocator;
    const bytes = try a.alloc(u8, 128 * 1024);
    defer a.free(bytes);
    @memset(bytes, 'a');
    const Probe = struct {
        calls: usize = 0,
        fn canceled(raw: *const anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw)));
            self.calls += 1;
            return self.calls == 5;
        }
    };
    var probe: Probe = .{};
    try std.testing.expectError(error.Canceled, stringifyAlloc(a, .{ .value = bytes }, .{}, .{ .cancellation_ctx = &probe, .is_cancelled_fn = Probe.canceled }));
    try std.testing.expectEqual(@as(usize, 5), probe.calls);
    const actual = try stringifyAlloc(a, .{ .value = bytes }, .{}, .{});
    defer a.free(actual);
    const expected = try std.json.Stringify.valueAlloc(a, .{ .value = bytes }, .{});
    defer a.free(expected);
    try std.testing.expectEqualStrings(expected, actual);
    const Scenario = struct {
        fn run(backing: std.mem.Allocator) !void {
            const result = try stringifyAlloc(backing, .{ .values = [_][]const u8{ "a", "b" }, .n = 7 }, .{}, .{});
            backing.free(result);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Scenario.run, .{});
}
