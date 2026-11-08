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

//! Owned request coalescing and transform effects. This module has no DB,
//! Store, writer admission, clock or runtime dependency. Captured-only callers
//! can resolve postimages before entering either writer gate.
const std = @import("std");
const Allocator = std.mem.Allocator;
const types = @import("types.zig");

/// Immutable request inputs independent of the caller's buffers. Both
/// publication cohorts and transform preparation retain this same representation.
pub const OwnedRequest = struct {
    arena: std.heap.ArenaAllocator,
    writes: []const types.BatchWrite,
    deletes: []const []const u8,
    transforms: []const types.DocumentTransform,
    fingerprint: [32]u8,

    pub fn init(a: Allocator, writes: []const types.BatchWrite, deletes: []const []const u8, transforms: []const types.DocumentTransform) !OwnedRequest {
        var arena = std.heap.ArenaAllocator.init(a);
        errdefer arena.deinit();
        const owned = arena.allocator();
        const owned_writes = try ownValue(owned, @TypeOf(writes), writes);
        const owned_deletes = try ownValue(owned, @TypeOf(deletes), deletes);
        const owned_transforms = try ownValue(owned, @TypeOf(transforms), transforms);
        return .{ .arena = arena, .writes = owned_writes, .deletes = owned_deletes, .transforms = owned_transforms, .fingerprint = requestFingerprint(owned_writes, owned_deletes, owned_transforms) };
    }
    pub fn matches(self: *const OwnedRequest, writes: []const types.BatchWrite, deletes: []const []const u8, transforms: []const types.DocumentTransform) bool {
        return std.mem.eql(u8, &self.fingerprint, &requestFingerprint(writes, deletes, transforms));
    }
    pub fn deinit(self: *OwnedRequest) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

fn ownValue(a: Allocator, comptime T: type, value: T) !T {
    return switch (@typeInfo(T)) {
        .pointer => |pointer| blk: {
            if (pointer.size != .slice) @compileError("owned mutation effects require explicit ownership for non-slice pointers");
            const result = try a.alloc(pointer.child, value.len);
            for (value, result) |item, *out| out.* = try ownValue(a, pointer.child, item);
            break :blk result;
        },
        .optional => |optional| if (value) |item| try ownValue(a, optional.child, item) else null,
        .@"struct" => |record| blk: {
            var result = value;
            inline for (record.field_names, record.field_types, record.field_attrs) |name, FieldType, attributes| if (!attributes.@"comptime") {
                @field(result, name) = try ownValue(a, FieldType, @field(value, name));
            };
            break :blk result;
        },
        .bool, .int, .@"enum", .float => value,
        else => @compileError("unsupported owned mutation effect field: " ++ @typeName(T)),
    };
}

pub fn requestFingerprint(writes: []const types.BatchWrite, deletes: []const []const u8, transforms: []const types.DocumentTransform) [32]u8 {
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly owned mutation effects v1");
    hashValue(&hash, @TypeOf(writes), writes);
    hashValue(&hash, @TypeOf(deletes), deletes);
    hashValue(&hash, @TypeOf(transforms), transforms);
    var result: [32]u8 = undefined;
    hash.final(&result);
    return result;
}

fn hashValue(hash: *std.crypto.hash.Blake3, comptime T: type, value: T) void {
    switch (@typeInfo(T)) {
        .pointer => |pointer| {
            if (pointer.size != .slice) @compileError("mutation fingerprint requires slices");
            hash.update(&std.mem.toBytes(std.mem.nativeToLittle(u64, value.len)));
            if (pointer.child == u8) hash.update(value) else for (value) |item| hashValue(hash, pointer.child, item);
        },
        .optional => |optional| {
            hash.update(&.{@intFromBool(value != null)});
            if (value) |item| hashValue(hash, optional.child, item);
        },
        .@"struct" => |record| inline for (record.field_names, record.field_types, record.field_attrs) |name, FieldType, attributes| {
            if (!attributes.@"comptime") hashValue(hash, FieldType, @field(value, name));
        },
        .bool => hash.update(&.{@intFromBool(value)}),
        .@"enum" => hashValue(hash, []const u8, @tagName(value)),
        .int => hash.update(&std.mem.toBytes(std.mem.nativeToLittle(T, value))),
        else => @compileError("unsupported mutation fingerprint field: " ++ @typeName(T)),
    }
}

// Share the concrete request-only planner with foreground, recovery and owned
// publication. The ownership layer adds retained buffers and exact read proofs;
// it does not maintain a second transform or graph-effects implementation.
const planner = @import("mutation_plan.zig");
pub const TransformReadSnapshot = planner.ReadSnapshot;

pub fn CoalescedKeyValueRequest(comptime T: type) type {
    if (T != types.BatchWrite) @compileError("mutation writes must use the canonical write type");
    return planner.PreparedMutation;
}

pub fn coalesceCapturedKeyValueRequest(
    alloc: Allocator,
    comptime T: type,
    writes: []const T,
    deletes: []const []const u8,
    transforms: []const types.DocumentTransform,
    snapshot: *const TransformReadSnapshot,
) !CoalescedKeyValueRequest(T) {
    // A missing capture is different from a captured absent document. Owned
    // preparation must never silently turn the former into an upsert base.
    var reads: @import("transform_read_plan.zig").Plan = .{};
    defer reads.deinit(alloc);
    for (writes) |write| try reads.establish(alloc, write.key);
    for (deletes) |key| try reads.establish(alloc, key);
    for (transforms) |transform| try reads.transform(alloc, transform.key);
    for (reads.durable_bases.items) |key| if (!snapshot.positions.contains(key)) return error.PreparedReadSetChanged;
    return planner.prepare(alloc, writes, deletes, transforms, snapshot);
}

test "owned request coalescing refuses uncaptured bases instead of treating them as missing" {
    const a = std.testing.allocator;
    var snapshot: TransformReadSnapshot = .{};
    defer snapshot.deinit(a);
    try std.testing.expectError(error.PreparedReadSetChanged, coalesceCapturedKeyValueRequest(a, types.BatchWrite, &.{}, &.{}, &.{.{ .key = "counter", .upsert = true, .operations = &.{.{ .op = .inc, .path = "n", .value_json = "1" }} }}, &snapshot));
}

fn testOwnedRequestCoalescingAllocation(a: Allocator) !void {
    var snapshot: TransformReadSnapshot = .{};
    defer snapshot.deinit(a);
    const entries = try a.alloc(TransformReadSnapshot.Entry, 1);
    const body = a.dupe(u8, "{\"n\":2}") catch |err| {
        a.free(entries);
        return err;
    };
    entries[0] = .{ .key = "counter", .value = body, .expected_version = 9 };
    snapshot.entries = entries;
    // Transfer ownership before the next fallible allocation.
    try snapshot.positions.put(a, "counter", 0);
    var result = try coalesceCapturedKeyValueRequest(a, types.BatchWrite, &.{}, &.{}, &.{.{ .key = "counter", .operations = &.{
        .{ .op = .push, .path = "$._edges.graph.FRIEND", .value_json = "{\"target\":\"b\"}" },
        .{ .op = .pull, .path = "$._edges.graph.FRIEND", .value_json = "{\"target\":\"b\"}" },
        .{ .op = .push, .path = "$._edges.graph.FRIEND", .value_json = "{\"target\":\"c\",\"metadata\":{\"order\":3}}" },
        .{ .op = .inc, .path = "n", .value_json = "1" },
    } }}, &snapshot);
    defer result.deinit(a);
    try std.testing.expectEqual(@as(usize, 1), result.writes.len);
    try std.testing.expectEqual(@as(usize, 1), result.graph_writes.items.len);
    try std.testing.expectEqual(@as(usize, 1), result.graph_deletes.items.len);
    try std.testing.expectEqualStrings("c", result.graph_writes.items[0].target);
    try std.testing.expectEqualStrings("b", result.graph_deletes.items[0].target);
    var parsed = try std.json.parseFromSlice(std.json.Value, a, result.writes[0].value, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(i64, 3), parsed.value.object.get("n").?.integer);
}

test "owned request coalescing retains ordered transform and graph effects across every allocation failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, testOwnedRequestCoalescingAllocation, .{});
}

test "owned request coalescing preserves final deletes and transform upserts without a runtime" {
    const a = std.testing.allocator;
    var snapshot: TransformReadSnapshot = .{};
    defer snapshot.deinit(a);
    var result = try coalesceCapturedKeyValueRequest(a, types.BatchWrite, &.{.{ .key = "counter", .value = "{\"n\":20}" }}, &.{"counter"}, &.{.{ .key = "counter", .upsert = true, .operations = &.{.{ .op = .inc, .path = "n", .value_json = "1" }} }}, &snapshot);
    defer result.deinit(a);
    try std.testing.expectEqual(@as(usize, 0), result.deletes.len);
    try std.testing.expectEqual(@as(usize, 1), result.writes.len);
    var parsed = try std.json.parseFromSlice(std.json.Value, a, result.writes[0].value, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(i64, 1), parsed.value.object.get("n").?.integer);
}

fn testOwnedRequestAllocation(a: Allocator) !void {
    var body = [_]u8{ '{', '}', ' ' };
    var operand = [_]u8{'1'};
    var request = try OwnedRequest.init(a, &.{.{ .key = "counter", .value = &body }}, &.{"deleted"}, &.{.{ .key = "counter", .upsert = true, .operations = &.{.{ .op = .inc, .path = "n", .value_json = &operand }} }});
    defer request.deinit();
    body[0] = '[';
    operand[0] = '9';
    try std.testing.expectEqualStrings("{} ", request.writes[0].value);
    try std.testing.expectEqualStrings("1", request.transforms[0].operations[0].value_json.?);
    try std.testing.expect(request.matches(request.writes, request.deletes, request.transforms));
    try std.testing.expect(!request.matches(&.{.{ .key = "counter", .value = &body }}, request.deletes, request.transforms));
    try std.testing.expect(!request.matches(request.writes, request.deletes, &.{.{ .key = "counter", .upsert = false, .operations = request.transforms[0].operations }}));
}

test "owned request coalescing inputs retain buffers and exact operation semantics across every allocation failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, testOwnedRequestAllocation, .{});
}
