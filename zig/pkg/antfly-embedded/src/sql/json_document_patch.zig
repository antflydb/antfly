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

//! Immutable document patches: validate all operations before building a
//! candidate and copy only changed object spines. Payloads and cold branches
//! borrow the evaluation owner. Native admission still validates the schema.
const std = @import("std");
const Json = std.json.Value;
const Work = @import("json_order.zig").Budget;
const Memory = @import("memory_budget.zig");
pub const Result = struct { value: Json, allocated_bytes: usize };

pub fn apply(owner: std.mem.Allocator, target: Json, patch: Json, bytes: usize, work: *Work) !Result {
    if (target != .object or patch != .object or patch.object.count() != 1) return error.InvalidSqlParameters;
    const ops = patch.object.get("ops") orelse return error.InvalidSqlParameters;
    if (ops != .array) return error.InvalidSqlParameters;
    if (ops.array.items.len > 1024) return error.SqlProgramLimitExceeded;
    for (ops.array.items) |op| {
        try work.consume(1);
        try validate(op, work);
    }
    // The evaluation owner is a region. Replaced map buffers can remain
    // retained there even after free; never refund their admission.
    var memory: Memory = .{ .backing = owner, .limit = bytes, .monotonic = true };
    var result = target;
    for (ops.array.items) |op| {
        result = evaluate(memory.allocator(), result, op, work) catch |err| {
            return if (err == error.OutOfMemory and memory.isExhausted()) error.SqlProgramLimitExceeded else err;
        };
    }
    return .{ .value = result, .allocated_bytes = memory.footprint() };
}

fn validate(op: Json, work: *Work) !void {
    if (op != .object) return error.InvalidSqlParameters;
    const verb = op.object.get("op") orelse return error.InvalidSqlParameters;
    if (verb != .string) return error.InvalidSqlParameters;
    const remove = std.mem.eql(u8, verb.string, "remove");
    const merging = std.mem.eql(u8, verb.string, "merge");
    if (!remove and !merging and !std.mem.eql(u8, verb.string, "set")) return error.InvalidSqlParameters;
    if (op.object.count() != @as(usize, if (remove or merging) 2 else 3)) return error.InvalidSqlParameters;
    if (merging) {
        const value = op.object.get("value") orelse return error.InvalidSqlParameters;
        if (value != .object) return error.InvalidSqlParameters;
        return;
    }
    if (!remove and !op.object.contains("value")) return error.InvalidSqlParameters;
    const path = op.object.get("path") orelse return error.InvalidSqlParameters;
    if (path != .string or path.string.len == 0) return error.InvalidSqlParameters;
    try work.consume(path.string.len);
    var count: usize = 0;
    var parts = std.mem.splitScalar(u8, path.string, '.');
    while (parts.next()) |part| {
        if (part.len == 0 or std.mem.indexOfScalar(u8, part, 0) != null) return error.InvalidSqlParameters;
        count += 1;
        if (count > 64) return error.SqlProgramLimitExceeded;
    }
    if (!remove) try admitDepth(op.object.get("value").?, count, work);
}

fn admitDepth(value: Json, depth: usize, work: *Work) anyerror!void {
    try work.consume(1);
    switch (value) {
        .object => |object| {
            if (depth >= 64) return error.SqlProgramLimitExceeded;
            for (object.keys(), object.values()) |key, child| {
                try work.consume(key.len);
                try admitDepth(child, depth + 1, work);
            }
        },
        .array => |array| {
            if (depth >= 64) return error.SqlProgramLimitExceeded;
            for (array.items) |child| try admitDepth(child, depth + 1, work);
        },
        else => {},
    }
}

fn evaluate(a: std.mem.Allocator, target: Json, op: Json, work: *Work) !Json {
    if (std.mem.eql(u8, op.object.get("op").?.string, "merge")) return merge(a, target, op.object.get("value").?, work, 0);
    var segments: [64][]const u8 = undefined;
    var count: usize = 0;
    var parts = std.mem.splitScalar(u8, op.object.get("path").?.string, '.');
    while (parts.next()) |part| {
        segments[count] = part;
        count += 1;
    }
    return change(a, target, segments[0..count], op.object.get("value"), work);
}

fn copyObject(a: std.mem.Allocator, source: Json, extra: usize, work: *Work) !std.json.ObjectMap {
    if (source != .object) return error.InvalidSqlParameters;
    try work.consume(source.object.count());
    var result: std.json.ObjectMap = .empty;
    errdefer result.deinit(a);
    const capacity = std.math.cast(u32, @as(usize, source.object.count()) + extra) orelse return error.SqlProgramLimitExceeded;
    try result.ensureTotalCapacity(a, capacity);
    for (source.object.keys(), source.object.values()) |key, value| {
        try work.consume(key.len);
        result.putAssumeCapacity(key, value);
    }
    return result;
}

fn change(a: std.mem.Allocator, source: Json, path: []const []const u8, replacement: ?Json, work: *Work) anyerror!Json {
    try work.consume(1);
    if (source != .object) return error.InvalidSqlParameters;
    const prior = source.object.get(path[0]);
    if (replacement == null and prior == null) return source;
    const child: ?Json = if (path.len == 1) replacement else try change(a, prior orelse Json{ .object = .empty }, path[1..], replacement, work);
    var result = try copyObject(a, source, @intFromBool(prior == null and child != null), work);
    errdefer result.deinit(a);
    if (child) |value| result.putAssumeCapacity(path[0], value) else _ = result.orderedRemove(path[0]);
    return .{ .object = result };
}

fn merge(a: std.mem.Allocator, source: Json, overlay: Json, work: *Work, depth: usize) anyerror!Json {
    try work.consume(1);
    if (source != .object or overlay != .object) return overlay;
    if (depth >= 64) return error.SqlProgramLimitExceeded;
    if (overlay.object.count() == 0) return source;
    // Reserve the final shape once. Rehashing a growing merged object in an
    // arena retains replaced buffers and can dominate a small patch's cost.
    var extra: usize = 0;
    for (overlay.object.keys()) |key| {
        try work.consume(key.len + 1);
        extra += @intFromBool(!source.object.contains(key));
    }
    var result = try copyObject(a, source, extra, work);
    errdefer result.deinit(a);
    for (overlay.object.keys(), overlay.object.values()) |key, value| {
        try work.consume(key.len + 1);
        const next = if (source.object.get(key)) |prior| try merge(a, prior, value, work, depth + 1) else value;
        result.putAssumeCapacity(key, next);
    }
    return .{ .object = result };
}

test "SQL immutable document patches preserve cold branches and validate every operation" {
    const F = struct {
        fn run(backing: std.mem.Allocator) !void {
            var arena = std.heap.ArenaAllocator.init(backing);
            defer arena.deinit();
            const a = arena.allocator();
            const source = try std.json.parseFromSliceLeaky(Json, a, "{\"a\":{\"n\":1},\"cold\":[1,2,3],\"obsolete\":true}", .{});
            const patch = try std.json.parseFromSliceLeaky(Json, a, "{\"ops\":[{\"op\":\"set\",\"path\":\"a.n\",\"value\":2},{\"op\":\"remove\",\"path\":\"obsolete\"},{\"op\":\"merge\",\"value\":{\"a\":{\"explicit\":null}}}]}", .{});
            var work: Work = .{};
            const result = try apply(a, source, patch, 8192, &work);
            try std.testing.expectEqual(@as(i64, 1), source.object.get("a").?.object.get("n").?.integer);
            try std.testing.expectEqual(@as(i64, 2), result.value.object.get("a").?.object.get("n").?.integer);
            try std.testing.expect(result.value.object.get("a").?.object.get("explicit").? == .null);
            try std.testing.expect(!result.value.object.contains("obsolete"));
            try std.testing.expect(source.object.get("cold").?.array.items.ptr == result.value.object.get("cold").?.array.items.ptr);
            var denied = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
            work = .{};
            try std.testing.expectError(error.SqlProgramLimitExceeded, apply(denied.allocator(), source, patch, 0, &work));
            const invalid = try std.json.parseFromSliceLeaky(Json, a, "{\"ops\":[{\"op\":\"set\",\"path\":\"a\",\"value\":2},{\"op\":\"remove\",\"path\":\"bad..path\"}]}", .{});
            work = .{};
            try std.testing.expectError(error.InvalidSqlParameters, apply(denied.allocator(), source, invalid, 8192, &work));
            try std.testing.expectEqual(@as(usize, 0), denied.allocations);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, F.run, .{});
}

test "SQL immutable document patch output depth accounts for inserted subtrees" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const parts: [64][]const u8 = @splat("x");
    const path = try std.mem.join(a, ".", &parts);
    for ([_][]const u8{ "null", "{\"n\":1}", "[1]" }, 0..) |value, index| {
        const raw = try std.fmt.allocPrint(a, "{{\"ops\":[{{\"op\":\"set\",\"path\":\"{s}\",\"value\":{s}}}]}}", .{ path, value });
        const patch = try std.json.parseFromSliceLeaky(Json, a, raw, .{});
        var work: Work = .{};
        if (index != 0) {
            var denied = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
            try std.testing.expectError(error.SqlProgramLimitExceeded, apply(denied.allocator(), .{ .object = .empty }, patch, 65536, &work));
            try std.testing.expectEqual(@as(usize, 0), denied.allocations);
        } else {
            var cursor = (try apply(a, .{ .object = .empty }, patch, 65536, &work)).value;
            for (0..64) |_| cursor = cursor.object.get("x").?;
            try std.testing.expect(cursor == .null);
        }
    }
}
