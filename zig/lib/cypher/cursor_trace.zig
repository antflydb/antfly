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

//! Shared merge positions plus one update per row. Expand only at a consumer's
//! actual page boundary; nested owner pages never copy all positions per row.
const std = @import("std");
const A = std.mem.Allocator;
pub const Position = struct { exact_keys: bool = false, coarse: bool = false, owner: usize, batched: bool = false, after: ?[]const u8 = null, finished: bool = false };
pub const Step = struct { slot: usize, after: ?[]const u8 };
pub const Trace = struct {
    base: []const Position,
    steps: []const Step,
    /// Singleton coordinator envelopes, innermost first.
    wrappers: []const Position = &.{},
    pub fn validate(self: Trace, rows: usize) !void {
        if (self.base.len == 0 or self.base.len > 1024 or self.steps.len != rows or self.wrappers.len > 16) return error.CypherInvalidBackendPage;
        for (self.wrappers) |wrapper| if (wrapper.after != null or wrapper.finished) return error.CypherInvalidBackendPage;
        var finished: [1024]bool = undefined;
        for (self.base, 0..) |position, index| {
            if (position.owner >= 1024 or (index != 0 and self.base[index - 1].owner >= position.owner)) return error.CypherInvalidBackendPage;
            finished[index] = position.finished;
        }
        for (self.steps) |step| {
            if (step.slot >= self.base.len or finished[step.slot]) return error.CypherInvalidBackendPage;
            finished[step.slot] = step.after == null;
        }
    }
    pub fn create(a: A, value: Trace) !*const Trace {
        const out = try a.create(Trace);
        out.* = value;
        return out;
    }
    pub fn clone(self: Trace, a: A) !*const Trace {
        const base = try a.dupe(Position, self.base);
        for (base) |*position| if (position.after) |bytes| {
            position.after = try a.dupe(u8, bytes);
        };
        const steps = try a.dupe(Step, self.steps);
        for (steps) |*step| if (step.after) |bytes| {
            step.after = try a.dupe(u8, bytes);
        };
        return create(a, .{ .base = base, .steps = steps, .wrappers = try a.dupe(Position, self.wrappers) });
    }
    pub fn wrap(self: Trace, a: A, position: Position) !*const Trace {
        if (self.wrappers.len >= 16) return error.CypherInvalidBackendPage;
        const wrappers = try a.alloc(Position, self.wrappers.len + 1);
        @memcpy(wrappers[0..self.wrappers.len], self.wrappers);
        wrappers[self.wrappers.len] = position;
        return create(a, .{ .base = self.base, .steps = self.steps, .wrappers = wrappers });
    }
    pub fn materialize(self: Trace, a: A, consumed: usize, work: anytype) !?[]const u8 {
        try self.validate(self.steps.len);
        if (consumed > self.steps.len) return error.CypherInvalidBackendCursor;
        if (work) |meter| try meter.charge(meter.ptr, self.base.len + consumed + self.wrappers.len);
        const positions = try a.dupe(Position, self.base);
        defer a.free(positions);
        for (self.steps[0..consumed]) |step| {
            positions[step.slot].after = step.after;
            positions[step.slot].finished = step.after == null;
        }
        const more = for (positions) |position| {
            if (!position.finished) break true;
        } else false;
        if (!more) return null;
        var cursor = try std.json.Stringify.valueAlloc(a, positions, .{});
        errdefer a.free(cursor);
        for (self.wrappers) |wrapper| {
            var position = wrapper;
            position.after = cursor;
            position.finished = false;
            const next = try std.json.Stringify.valueAlloc(a, &.{position}, .{});
            a.free(cursor);
            cursor = next;
        }
        if (work) |meter| try meter.charge(meter.ptr, cursor.len / 64 + 1);
        return cursor;
    }
};

test "cursor traces share positions and validate nested resumable boundaries" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const trace: Trace = .{ .base = &.{ .{ .owner = 0, .after = "a" }, .{ .owner = 1, .after = "b" } }, .steps = &.{ .{ .slot = 1, .after = "c" }, .{ .slot = 0, .after = null }, .{ .slot = 1, .after = null } } };
    try trace.validate(3);
    const first = (try trace.materialize(a, 1, @as(?@import("backend.zig").Work, null))).?;
    const positions = try std.json.parseFromSliceLeaky([]Position, a, first, .{});
    try std.testing.expectEqualStrings("a", positions[0].after.?);
    try std.testing.expectEqualStrings("c", positions[1].after.?);
    const wrapped = try trace.wrap(a, .{ .owner = 7 });
    const outer = try std.json.parseFromSliceLeaky([]Position, a, (try wrapped.materialize(a, 2, @as(?@import("backend.zig").Work, null))).?, .{});
    try std.testing.expectEqual(@as(usize, 7), outer[0].owner);
    const inner = try std.json.parseFromSliceLeaky([]Position, a, outer[0].after.?, .{});
    try std.testing.expect(inner[0].finished);
    try std.testing.expectEqualStrings("c", inner[1].after.?);
    try std.testing.expect((try trace.materialize(a, 3, @as(?@import("backend.zig").Work, null))) == null);
    const invalid: Trace = .{ .base = trace.base, .steps = &.{.{ .slot = 2, .after = null }} };
    try std.testing.expectError(error.CypherInvalidBackendPage, invalid.validate(1));
    try std.testing.expectError(error.CypherInvalidBackendPage, trace.validate(4));
    const duplicate: Trace = .{ .base = &.{ .{ .owner = 0 }, .{ .owner = 0 } }, .steps = &.{} };
    try std.testing.expectError(error.CypherInvalidBackendPage, duplicate.validate(0));
    const resurrected: Trace = .{ .base = &.{.{ .owner = 0, .finished = true }}, .steps = &.{.{ .slot = 0, .after = "again" }} };
    try std.testing.expectError(error.CypherInvalidBackendPage, resurrected.validate(1));
    try std.testing.expectError(error.CypherInvalidBackendCursor, trace.materialize(a, 4, @as(?@import("backend.zig").Work, null)));
    const clone = try wrapped.clone(a);
    try std.testing.expectEqualStrings((try wrapped.materialize(a, 1, @as(?@import("backend.zig").Work, null))).?, (try clone.materialize(a, 1, @as(?@import("backend.zig").Work, null))).?);
}
