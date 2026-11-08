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

//! Concrete request-only mutation planning. Durable reads are supplied as an
//! explicit snapshot; this module cannot open, mutate, or schedule a DB.
const std = @import("std");
const Allocator = std.mem.Allocator;
const types = @import("mutation_values.zig");
const transform_mod = @import("transform.zig");

test "mutation plan preserves delete and transform ordering without a database" {
    const alloc = std.testing.allocator;
    var plan = try prepare(alloc, &.{.{ .key = "a", .value = "{\"count\":1}" }}, &.{"a"}, &.{
        .{ .key = "a", .operations = &.{.{ .op = .inc, .path = "count", .value_json = "1" }} },
        .{ .key = "b", .upsert = true, .operations = &.{.{ .op = .set, .path = "count", .value_json = "2" }} },
    }, &.{});
    defer plan.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), plan.writes.len);
    try std.testing.expectEqualStrings("b", plan.writes[0].key);
    try std.testing.expectEqualStrings("{\"count\":2}", plan.writes[0].value);
    try std.testing.expectEqualStrings("a", plan.deletes[0]);
}

test "mutation plan resolves repeated transforms from one immutable durable snapshot" {
    const alloc = std.testing.allocator;
    const original = "{\"count\":10}";
    var entries = [_]ReadSnapshot.Entry{.{ .key = "a", .value = @constCast(original), .expected_version = 73 }};
    var snapshot: ReadSnapshot = .{ .entries = &entries };
    defer snapshot.positions.deinit(alloc);
    try snapshot.positions.put(alloc, "a", 0);
    var plan = try prepare(alloc, &.{}, &.{}, &.{
        .{ .key = "a", .operations = &.{.{ .op = .inc, .path = "count", .value_json = "1" }} },
        .{ .key = "a", .operations = &.{.{ .op = .inc, .path = "count", .value_json = "1" }} },
    }, &snapshot);
    defer plan.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), plan.writes.len);
    try std.testing.expectEqualStrings("{\"count\":12}", plan.writes[0].value);
    try std.testing.expectEqualStrings(original, snapshot.valueFor("a").?);
    try std.testing.expectEqual(@as(u64, 73), snapshot.entries[0].expected_version);
}

test "mutation plan retains JSON null provenance for batch and transaction writes" {
    try std.testing.expect(types.BatchWrite == types.TransactionWrite);
    const alloc = std.testing.allocator;
    const writes = [_]types.TransactionWrite{.{ .key = "row", .value = "{\"payload\":null}", .json_null_fields = &.{"payload"} }};
    var plan = try prepare(alloc, &writes, &.{}, &.{}, &.{});
    defer plan.deinit(alloc);
    try std.testing.expectEqualStrings("payload", plan.writes[0].json_null_fields[0]);
    try std.testing.expectError(error.UnsupportedTransformOperation, prepare(alloc, &writes, &.{}, &.{.{ .key = "row", .operations = &.{} }}, &.{}));
}

test "mutation plan frees graph and document effects at every allocation failure" {
    const Check = struct {
        fn run(alloc: Allocator) !void {
            var plan = try prepare(alloc, &.{.{ .key = "a", .value = "{\"count\":1}" }}, &.{"deleted"}, &.{.{ .key = "a", .operations = &.{
                .{ .op = .inc, .path = "count", .value_json = "1" },
                .{ .op = .push, .path = "$._edges.graph.FRIEND", .value_json = "{\"target\":\"b\",\"metadata\":{\"since\":2026}}" },
                .{ .op = .pull, .path = "$._edges.graph.FRIEND", .value_json = "{\"target\":\"b\"}" },
            } }}, &.{});
            defer plan.deinit(alloc);
            try std.testing.expectEqual(@as(usize, 0), plan.graph_writes.items.len);
            try std.testing.expectEqual(@as(usize, 1), plan.graph_deletes.items.len);
        }
    };
    // A backing allocator may remap in place depending on the address it
    // returned. Deny resizing here so every fault sweep visits the same
    // allocation sites in both Debug and ReleaseFast.
    const Stable = struct {
        fn allocate(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret: usize) ?[*]u8 {
            const backing: *Allocator = @ptrCast(@alignCast(ctx));
            return backing.rawAlloc(len, alignment, ret);
        }
        fn free(ctx: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ret: usize) void {
            const backing: *Allocator = @ptrCast(@alignCast(ctx));
            backing.rawFree(bytes, alignment, ret);
        }
    };
    var backing = std.testing.allocator;
    const stable: Allocator = .{ .ptr = &backing, .vtable = &.{ .alloc = Stable.allocate, .resize = Allocator.noResize, .remap = Allocator.noRemap, .free = Stable.free } };
    try std.testing.checkAllAllocationFailures(stable, Check.run, .{});
}

const coalescing = @import("mutation_coalescing.zig");
pub const PreparedMutation = coalescing.CoalescedKeyValueRequest(types.BatchWrite);
pub const ReadSnapshot = coalescing.TransformReadSnapshot;

pub noinline fn prepare(alloc: Allocator, writes: []const types.BatchWrite, deletes: []const []const u8, transforms: []const types.DocumentTransform, read_snapshot: *const ReadSnapshot) !PreparedMutation {
    const CapturedOnly = struct {
        pub fn isRecoveryExecution(_: @This()) bool {
            return true;
        }
        pub fn get(_: @This(), _: Allocator, _: []const u8) !?[]u8 {
            return error.PreparedReadSetChanged;
        }
    };
    return coalescing.coalesceKeyValueRequest(CapturedOnly{}, alloc, types.BatchWrite, writes, deletes, transforms, read_snapshot);
}

pub fn resetCoalescedEntryToDelete(entry: *PreparedMutation.Entry) void {
    return coalescing.resetCoalescedEntryToDelete(types.BatchWrite, entry);
}
pub fn setCoalescedEntryToBorrowedWrite(entry: *PreparedMutation.Entry, write: types.BatchWrite) void {
    return coalescing.setCoalescedEntryToBorrowedWrite(types.BatchWrite, entry, write);
}
pub fn setCoalescedEntryToOwnedWrite(alloc: Allocator, entry: *PreparedMutation.Entry, key: []const u8, value: []u8) !void {
    return coalescing.setCoalescedEntryToOwnedWrite(types.BatchWrite, alloc, entry, key, value);
}
pub const appendGraphTransformDelete = coalescing.appendGraphTransformDelete;
pub const appendGraphTransformWrite = coalescing.appendGraphTransformWrite;
pub const deinitOwnedGraphEdgeDelete = coalescing.deinitOwnedGraphEdgeDelete;
pub const deinitOwnedGraphEdgeWrite = coalescing.deinitOwnedGraphEdgeWrite;
pub const graphMutationIdentityEql = coalescing.graphMutationIdentityEql;
