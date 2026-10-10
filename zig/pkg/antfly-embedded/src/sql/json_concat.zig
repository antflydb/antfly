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

//! Shallow PostgreSQL JSONB concatenation. Only the new outer container is
//! allocated; immutable children and keys borrow the enclosing evaluation's
//! lifetime. No serialization, recursive merge, or SQL NULL conversion.
const std = @import("std");
const Json = std.json.Value;
const Work = @import("json_order.zig").Budget;
const Memory = @import("memory_budget.zig");

pub const Result = struct { value: Json, allocated_bytes: usize };

pub fn concat(owner: std.mem.Allocator, left: Json, right: Json, bytes: usize, work: *Work) !Result {
    var memory: Memory = .{ .backing = owner, .limit = bytes };
    const value = build(memory.allocator(), owner, left, right, work) catch |err| {
        if (err == error.OutOfMemory and memory.exhausted) return error.SqlProgramLimitExceeded;
        return err;
    };
    return .{ .value = value, .allocated_bytes = memory.peak };
}

fn build(a: std.mem.Allocator, owner: std.mem.Allocator, left: Json, right: Json, work: *Work) !Json {
    if (left == .object and right == .object) {
        const count = std.math.add(usize, left.object.count(), right.object.count()) catch return error.SqlProgramLimitExceeded;
        // Admit all key hashing/comparison work before creating the map.
        try work.consume(count);
        for (left.object.keys()) |key| try work.consume(key.len);
        for (right.object.keys()) |key| try work.consume(key.len);
        var object: std.json.ObjectMap = .empty;
        errdefer object.deinit(a);
        try object.ensureTotalCapacity(a, count);
        for (left.object.keys(), left.object.values()) |key, value| object.putAssumeCapacity(key, value);
        for (right.object.keys(), right.object.values()) |key, value| object.putAssumeCapacity(key, value);
        return .{ .object = object };
    }
    const n_left = if (left == .array) left.array.items.len else 1;
    const n_right = if (right == .array) right.array.items.len else 1;
    const count = std.math.add(usize, n_left, n_right) catch return error.SqlProgramLimitExceeded;
    try work.consume(count);
    const items = try a.alloc(Json, count);
    if (left == .array) @memcpy(items[0..n_left], left.array.items) else items[0] = left;
    if (right == .array) @memcpy(items[n_left..], right.array.items) else items[n_left] = right;
    // Managed JSON arrays must never retain the stack-local quota allocator.
    return .{ .array = std.array_list.Managed(Json).fromOwnedSlice(owner, items) };
}

test "SQL JSONB concatenation admits capacity before allocating and unwinds faults" {
    const Faults = struct {
        fn run(a: std.mem.Allocator) !void {
            const left = try std.json.parseFromSlice(Json, a, "{\"x\":1,\"nested\":{\"a\":2}}", .{});
            defer left.deinit();
            const right = try std.json.parseFromSlice(Json, a, "{\"x\":3,\"nested\":{\"b\":4}}", .{});
            defer right.deinit();
            var work: Work = .{};
            const result = try concat(a, left.value, right.value, 4096, &work);
            var object = result.value.object;
            defer object.deinit(a);
            try std.testing.expectEqual(@as(i64, 3), object.get("x").?.integer);
            try std.testing.expect(object.get("nested").?.object.get("a") == null);
            try std.testing.expectEqual(@as(i64, 4), object.get("nested").?.object.get("b").?.integer);
            try std.testing.expect(result.allocated_bytes > 0);
            work = .{};
            const array_result = try concat(a, .null, right.value, 4096, &work);
            var array = array_result.value.array;
            defer array.deinit();
            try std.testing.expectEqual(@as(usize, 2), array.items.len);
            try std.testing.expect(array.items[0] == .null);
            try std.testing.expectEqual(a.ptr, array.allocator.ptr);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Faults.run, .{});
    var work: Work = .{};
    var denied = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    try std.testing.expectError(error.SqlProgramLimitExceeded, concat(denied.allocator(), .null, .null, 0, &work));
    try std.testing.expectEqual(@as(usize, 0), denied.allocations);
    work = .{ .remaining = 1 };
    try std.testing.expectError(error.SqlProgramLimitExceeded, concat(denied.allocator(), .null, .null, 4096, &work));
    try std.testing.expectEqual(@as(usize, 0), denied.allocations);
}
