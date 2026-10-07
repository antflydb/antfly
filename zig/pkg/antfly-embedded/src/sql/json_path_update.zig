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

//! Immutable JSONB path replacement. Resolve first, then copy only containers
//! on the changed spine. Unchanged branches and path keys borrow the enclosing
//! evaluation owner; missing intermediate steps do not allocate or create gaps.
const std = @import("std");
const Json = std.json.Value;
const arrays = @import("array_value.zig");
const Work = @import("json_order.zig").Budget;
const Memory = @import("memory_budget.zig");
const Frame = struct { parent: Json, key: []const u8, slot: usize = 0, insert: bool = false };
pub const Result = struct { value: Json, allocated_bytes: usize };

pub fn set(owner: std.mem.Allocator, target: Json, path: *const arrays.Value, replacement: Json, create: bool, bytes: usize, work: *Work) !Result {
    if (path.element_type != .text) return error.SqlTypeMismatch;
    if (path.dimensions.len > 1) return error.SqlArraySubscriptError;
    if (target != .object and target != .array) return error.InvalidSqlParameters;
    if (path.elements.len == 0) return .{ .value = target, .allocated_bytes = 0 };
    if (!create and (if (target == .object) target.object.count() else target.array.items.len) == 0) return .{ .value = target, .allocated_bytes = 0 };
    var frames: [128]Frame = undefined;
    var count: usize = 0;
    var current = target;
    for (path.elements, 0..) |element, level| {
        try work.consume(1);
        if (element.sql_null) return error.SqlNullValueNotAllowed;
        if (element.array != null or element.value != .string) return error.SqlTypeMismatch;
        const key = element.value.string;
        try work.consume(key.len);
        if (count == frames.len) return error.SqlProgramLimitExceeded;
        const final = level == path.elements.len - 1;
        var frame: Frame = .{ .parent = current, .key = key };
        switch (current) {
            .object => |object| {
                const child = object.get(key);
                if (child == null and (!final or !create)) return .{ .value = target, .allocated_bytes = 0 };
                frame.insert = child == null;
                if (!final) current = child.?;
            },
            .array => |array| {
                const index = try ordinal(key);
                const position: i64 = if (index < 0) @as(i64, @intCast(array.items.len)) + index else index;
                const outside = position < 0 or position >= array.items.len;
                if (outside and (!final or !create)) return .{ .value = target, .allocated_bytes = 0 };
                frame.slot = if (outside) (if (index < 0) 0 else array.items.len) else @intCast(position);
                frame.insert = outside;
                if (!final) current = array.items[frame.slot];
            },
            else => return .{ .value = target, .allocated_bytes = 0 },
        }
        frames[count] = frame;
        count += 1;
    }
    var memory: Memory = .{ .backing = owner, .limit = bytes };
    const result = rebuild(memory.allocator(), owner, frames[0..count], replacement, work) catch |err| {
        if (err == error.OutOfMemory and memory.exhausted) return error.SqlProgramLimitExceeded;
        return err;
    };
    return .{ .value = result, .allocated_bytes = memory.peak };
}

fn ordinal(key: []const u8) !i32 {
    // PostgreSQL's strtoint accepts leading ASCII whitespace and a sign, but
    // neither trailing whitespace nor Zig's digit separators/base prefixes.
    const digits = std.mem.trimStart(u8, key, " \t\n\r\x0b\x0c");
    const start: usize = if (digits.len > 0 and (digits[0] == '+' or digits[0] == '-')) 1 else 0;
    if (digits.len == start) return error.SqlInvalidTextRepresentation;
    for (digits[start..]) |c| if (c < '0' or c > '9') return error.SqlInvalidTextRepresentation;
    return std.fmt.parseInt(i32, digits, 10) catch return error.SqlInvalidTextRepresentation;
}

fn rebuild(a: std.mem.Allocator, owner: std.mem.Allocator, frames: []const Frame, replacement: Json, work: *Work) !Json {
    var made: [128]Json = undefined;
    var count: usize = 0;
    errdefer for (made[0..count]) |value| {
        // Free only new containers, never their shared children or keys.
        switch (value) {
            .object => |object| {
                var copy = object;
                copy.deinit(a);
            },
            .array => |array| a.free(array.items),
            else => unreachable,
        }
    };
    var value = replacement;
    var i = frames.len;
    while (i != 0) {
        i -= 1;
        const frame = frames[i];
        switch (frame.parent) {
            .object => |old| {
                const size = std.math.add(usize, old.count(), @intFromBool(frame.insert)) catch return error.SqlProgramLimitExceeded;
                try work.consume(size);
                for (old.keys()) |key| try work.consume(key.len);
                try work.consume(frame.key.len);
                var object: std.json.ObjectMap = .empty;
                errdefer object.deinit(a);
                try object.ensureTotalCapacity(a, size);
                for (old.keys(), old.values()) |key, child| object.putAssumeCapacity(key, child);
                object.putAssumeCapacity(frame.key, value);
                value = .{ .object = object };
            },
            .array => |old| {
                const size = std.math.add(usize, old.items.len, @intFromBool(frame.insert)) catch return error.SqlProgramLimitExceeded;
                try work.consume(size);
                const items = try a.alloc(Json, size);
                @memcpy(items[0..frame.slot], old.items[0..frame.slot]);
                items[frame.slot] = value;
                const next = frame.slot + @intFromBool(!frame.insert);
                @memcpy(items[frame.slot + 1 ..], old.items[next..]);
                value = .{ .array = std.array_list.Managed(Json).fromOwnedSlice(owner, items) };
            },
            else => unreachable,
        }
        made[count] = value;
        count += 1;
    }
    return value;
}

test "SQL JSONB path replacement copies only its spine and unwinds allocation faults" {
    const Faults = struct {
        fn run(a: std.mem.Allocator) !void {
            const parsed = try std.json.parseFromSlice(Json, a, "{\"a\":[{\"b\":1,\"cold\":[1,2,3]}],\"untouched\":{\"x\":2}}", .{});
            defer parsed.deinit();
            const path: arrays.Value = .{ .element_type = .text, .dimensions = &.{.{ .length = 3 }}, .elements = &.{ .json(.{ .string = "a" }), .json(.{ .string = "0" }), .json(.{ .string = "b" }) } };
            var work: Work = .{};
            const changed = try set(a, parsed.value, &path, .{ .integer = 9 }, true, 4096, &work);
            var root = changed.value.object;
            defer root.deinit(a);
            var array = root.get("a").?.array;
            defer array.deinit();
            var leaf = array.items[0].object;
            defer leaf.deinit(a);
            const old_leaf = parsed.value.object.get("a").?.array.items[0].object;
            try std.testing.expectEqual(@as(i64, 9), leaf.get("b").?.integer);
            try std.testing.expectEqual(@as(i64, 1), old_leaf.get("b").?.integer);
            try std.testing.expectEqual(old_leaf.get("cold").?.array.items.ptr, leaf.get("cold").?.array.items.ptr);
            try std.testing.expectEqual(parsed.value.object.get("untouched").?.object.keys().ptr, root.get("untouched").?.object.keys().ptr);
            try std.testing.expectEqual(a.ptr, array.allocator.ptr);
            try std.testing.expect(changed.allocated_bytes > 0);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Faults.run, .{});
}

test "SQL JSONB path replacement admits bytes work and depth before copying" {
    const a = std.testing.allocator;
    const parsed = try std.json.parseFromSlice(Json, a, "{\"a\":1}", .{});
    defer parsed.deinit();
    const path: arrays.Value = .{ .element_type = .text, .dimensions = &.{.{ .length = 1 }}, .elements = &.{.json(.{ .string = "a" })} };
    const missing: arrays.Value = .{ .element_type = .text, .dimensions = &.{.{ .length = 1 }}, .elements = &.{.json(.{ .string = "missing" })} };
    var denied = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    var work: Work = .{};
    const unchanged = try set(denied.allocator(), parsed.value, &missing, .null, false, 0, &work);
    try std.testing.expectEqual(parsed.value.object.keys().ptr, unchanged.value.object.keys().ptr);
    try std.testing.expectEqual(@as(usize, 0), unchanged.allocated_bytes);
    work = .{};
    try std.testing.expectError(error.SqlProgramLimitExceeded, set(denied.allocator(), parsed.value, &path, .null, true, 0, &work));
    work = .{ .remaining = 0 };
    try std.testing.expectError(error.SqlProgramLimitExceeded, set(denied.allocator(), parsed.value, &path, .null, true, 4096, &work));
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var nested: Json = .{ .object = .empty };
    var elements: [129]arrays.Element = undefined;
    for (&elements) |*element| {
        var object: std.json.ObjectMap = .empty;
        try object.put(arena.allocator(), "a", nested);
        nested = .{ .object = object };
        element.* = .json(.{ .string = "a" });
    }
    const deep: arrays.Value = .{ .element_type = .text, .dimensions = &.{.{ .length = elements.len }}, .elements = &elements };
    work = .{};
    try std.testing.expectError(error.SqlProgramLimitExceeded, set(denied.allocator(), nested, &deep, .null, true, 4096, &work));
    try std.testing.expectEqual(@as(usize, 0), denied.allocations);
}

test "SQL JSONB path replacement bounds copied bytes independently of cold subtree size" {
    var fixture = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer fixture.deinit();
    const a = fixture.allocator();
    const cold = try a.alloc(Json, 4096);
    for (cold, 0..) |*cell, i| cell.* = .{ .integer = @intCast(i) };
    var billing: std.json.ObjectMap = .empty;
    try billing.put(a, "plan", .{ .string = "basic" });
    var root: std.json.ObjectMap = .empty;
    try root.put(a, "billing", .{ .object = billing });
    try root.put(a, "cold", .{ .array = std.array_list.Managed(Json).fromOwnedSlice(a, cold) });
    const target: Json = .{ .object = root };
    const path: arrays.Value = .{ .element_type = .text, .dimensions = &.{.{ .length = 2 }}, .elements = &.{ .json(.{ .string = "billing" }), .json(.{ .string = "plan" }) } };
    const buffer = try a.alloc(u8, 1024 * 1024);
    var scratch = std.heap.FixedBufferAllocator.init(buffer);
    var peaks: [2]usize = .{ 0, 0 };
    var elapsed: [2]i128 = undefined;
    for (0..2) |mode| {
        const start = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
        for (0..500) |_| {
            scratch.reset();
            const result: Json = if (mode == 0) changed: {
                var work: Work = .{};
                break :changed (try set(scratch.allocator(), target, &path, .{ .string = "pro" }, true, buffer.len, &work)).value;
            } else cloned: {
                var copy = try @import("operators.zig").cloneDatum(scratch.allocator(), .json(target));
                try copy.value.object.getPtr("billing").?.object.put(scratch.allocator(), "plan", .{ .string = "pro" });
                break :cloned copy.value;
            };
            try std.testing.expectEqualStrings("pro", result.object.get("billing").?.object.get("plan").?.string);
            try std.testing.expectEqual(@as(i64, 4095), result.object.get("cold").?.array.items[4095].integer);
            try std.testing.expectEqual(mode == 0, result.object.get("cold").?.array.items.ptr == cold.ptr);
            peaks[mode] = @max(peaks[mode], scratch.end_index);
        }
        elapsed[mode] = std.Io.Clock.now(.awake, std.testing.io).nanoseconds - start;
    }
    try std.testing.expect(peaks[0] < 4096);
    try std.testing.expect(peaks[1] > 100 * peaks[0]);
    std.debug.print("SQL JSONB path copy: rows=500 cold_elements=4096 spine_bytes={} clone_bytes={} spine_ns={} clone_ns={}\n", .{ peaks[0], peaks[1], elapsed[0], elapsed[1] });
}
