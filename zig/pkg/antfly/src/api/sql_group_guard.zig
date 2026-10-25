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

//! Keep live authorization checks around finalized group pulls, including
//! consumers which bypass the ordinary row cursor after negotiation.
const std = @import("std");
const operators = @import("antfly_local_sources").sql_operators;
pub const Check = struct { ptr: *anyopaque, call: *const fn (*anyopaque, std.mem.Allocator) anyerror!void };
const Guard = struct {
    a: std.mem.Allocator,
    source: operators.Grouped.Finalized,
    check: Check,
    fn next(ptr: *anyopaque, a: std.mem.Allocator, maximum: usize, exact: bool) !?operators.GroupBatch {
        const self: *Guard = @ptrCast(@alignCast(ptr));
        try self.check.call(self.check.ptr, a);
        const result = try self.source.next(self.source.ptr, a, maximum, exact);
        try self.check.call(self.check.ptr, a);
        return result;
    }
    fn close(ptr: *anyopaque) void {
        const self: *Guard = @ptrCast(@alignCast(ptr));
        const a = self.a;
        self.source.close(self.source.ptr);
        a.destroy(self);
    }
};
pub fn wrap(a: std.mem.Allocator, grouped: *operators.Grouped, check: Check) !void {
    const source = grouped.finalized orelse return error.InvalidSqlBackendResponse;
    const guard = try a.create(Guard);
    guard.* = .{ .a = a, .source = source, .check = check };
    grouped.finalized.?.ptr = guard;
    grouped.finalized.?.next = Guard.next;
    grouped.finalized.?.close = Guard.close;
}

test "sql.finalized group guard checks authority before and after every pull" {
    const Source = struct {
        allowed: bool = true,
        revoke_on_pull: bool = false,
        pulls: usize = 0,
        closes: usize = 0,
        fn check(ptr: *anyopaque, _: std.mem.Allocator) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (!self.allowed) return error.TestUnexpectedResult;
        }
        fn next(ptr: *anyopaque, _: std.mem.Allocator, _: usize, _: bool) !?operators.GroupBatch {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.pulls += 1;
            if (self.revoke_on_pull) self.allowed = false;
            return null;
        }
        fn close(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.closes += 1;
        }
    };
    var source: Source = .{};
    const grouped = try operators.Grouped.create(std.testing.allocator, &.{.{ .kind = .count }}, .{});
    errdefer grouped.deinit();
    try grouped.adoptFinalized(.{ .ptr = &source, .count = 1, .next = Source.next, .close = Source.close }, 1);
    try wrap(std.testing.allocator, grouped, .{ .ptr = &source, .call = Source.check });
    const final = grouped.finalized.?;
    source.allowed = false;
    try std.testing.expectError(error.TestUnexpectedResult, final.next(final.ptr, std.testing.allocator, 1, false));
    try std.testing.expectEqual(@as(usize, 0), source.pulls);
    source.allowed = true;
    source.revoke_on_pull = true;
    try std.testing.expectError(error.TestUnexpectedResult, final.next(final.ptr, std.testing.allocator, 1, false));
    try std.testing.expectEqual(@as(usize, 1), source.pulls);
    source.allowed = true;
    source.revoke_on_pull = false;
    try std.testing.expectEqual(@as(?operators.GroupBatch, null), try final.next(final.ptr, std.testing.allocator, 1, false));
    grouped.deinit();
    try std.testing.expectEqual(@as(usize, 1), source.closes);
}
