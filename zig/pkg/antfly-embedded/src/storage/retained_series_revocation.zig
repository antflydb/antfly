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

//! Durable install cancellation fences. Keep these after removing a producer:
//! an RPC admitted before retirement can arrive after its dependency is gone.
//! Keys are per immutable generation so a later generation remains admissible.
const std = @import("std");
const A = std.mem.Allocator;
const prefix = "\x00\x00__metadata__:retained_series_revoked_v1:";
const magic = "AFRSREV1";
const value_len = 120;
pub const Id = [32]u8;
pub const Fence = struct {
    id: Id,
    generation: u64,
    definition: Id,
    partition: Id,
    incarnation: u64,

    pub fn jsonStringify(self: Fence, writer: anytype) !void {
        try @import("../retained_dataset_types.zig").writeFields(self, .{}, writer);
    }
    pub fn validate(self: Fence) !void {
        if (self.generation == 0 or self.incarnation == 0 or
            std.mem.allEqual(u8, &self.id, 0) or std.mem.allEqual(u8, &self.definition, 0) or
            std.mem.allEqual(u8, &self.partition, 0)) return error.InvalidRetainedSeries;
    }
};
fn key(buffer: []u8, id: Id, generation: u64) ![]const u8 {
    return std.fmt.bufPrint(buffer, "{s}{s}:{x:0>16}", .{ prefix, std.fmt.bytesToHex(id, .lower), generation });
}
fn encode(fence: Fence) [value_len]u8 {
    var bytes: [value_len]u8 = undefined;
    @memcpy(bytes[0..8], magic);
    @memcpy(bytes[8..40], &fence.id);
    std.mem.writeInt(u64, bytes[40..48], fence.generation, .big);
    @memcpy(bytes[48..80], &fence.definition);
    @memcpy(bytes[80..112], &fence.partition);
    std.mem.writeInt(u64, bytes[112..120], fence.incarnation, .big);
    return bytes;
}
fn read(txn: anytype, id: Id, generation: u64) !?Fence {
    var buffer: [192]u8 = undefined;
    const bytes = txn.get(try key(&buffer, id, generation)) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    if (bytes.len != value_len or !std.mem.eql(u8, bytes[0..8], magic)) return error.RetainedSeriesCorrupt;
    const fence: Fence = .{ .id = bytes[8..40].*, .generation = std.mem.readInt(u64, bytes[40..48], .big), .definition = bytes[48..80].*, .partition = bytes[80..112].*, .incarnation = std.mem.readInt(u64, bytes[112..120], .big) };
    fence.validate() catch return error.RetainedSeriesCorrupt;
    if (!std.mem.eql(u8, &id, &fence.id) or generation != fence.generation) return error.RetainedSeriesCorrupt;
    return fence;
}
pub fn isRevoked(txn: anytype, fence: Fence) !bool {
    try fence.validate();
    const previous = (try read(txn, fence.id, fence.generation)) orelse return false;
    if (!std.meta.eql(previous, fence)) return error.RetainedSeriesDefinitionMismatch;
    return true;
}
pub fn requireInstall(txn: anytype, fence: Fence) !void {
    try fence.validate();
    if (try read(txn, fence.id, fence.generation) != null) return error.RetainedSeriesSourceRevoked;
}
fn headKey(buffer: []u8, id: Id, partition: Id) ![]const u8 {
    return std.fmt.bufPrint(buffer, "\x00\x00__metadata__:retained_series_revoked_head_v1:{s}:{s}", .{ std.fmt.bytesToHex(id, .lower), std.fmt.bytesToHex(partition, .lower) });
}
pub fn latestRevoked(txn: anytype, id: Id, partition: Id) !u64 {
    var buffer: [192]u8 = undefined;
    const bytes = txn.get(try headKey(&buffer, id, partition)) catch |err| switch (err) {
        error.NotFound => return 0,
        else => return err,
    };
    if (bytes.len != 8) return error.RetainedSeriesCorrupt;
    return std.mem.readInt(u64, bytes[0..8], .big);
}

/// Call only in the same transaction as stopping the installed producer and
/// draining its prepared intents. Missing producers are deliberately accepted.
pub fn stage(txn: anytype, fence: Fence) !void {
    try fence.validate();
    if (try read(txn, fence.id, fence.generation)) |previous| {
        if (!std.meta.eql(previous, fence)) return error.RetainedSeriesDefinitionMismatch;
    } else {
        var buffer: [192]u8 = undefined;
        const bytes = encode(fence);
        try txn.put(try key(&buffer, fence.id, fence.generation), &bytes);
    }
    if (try latestRevoked(txn, fence.id, fence.partition) < fence.generation) {
        var buffer: [192]u8 = undefined;
        var bytes: [8]u8 = undefined;
        std.mem.writeInt(u64, &bytes, fence.generation, .big);
        try txn.put(try headKey(&buffer, fence.id, fence.partition), &bytes);
    }
}

test "retained revocation survives missing installs and rejects stale retries without blocking successors" {
    const Store = struct {
        a: A,
        entries: std.StringHashMapUnmanaged([]const u8) = .empty,
        fn get(self: *@This(), name: []const u8) anyerror![]const u8 {
            return self.entries.get(name) orelse error.NotFound;
        }
        fn put(self: *@This(), name: []const u8, bytes: []const u8) !void {
            const entry = try self.entries.getOrPut(self.a, name);
            if (!entry.found_existing) entry.key_ptr.* = try self.a.dupe(u8, name);
            entry.value_ptr.* = try self.a.dupe(u8, bytes);
        }
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var store: Store = .{ .a = arena.allocator() };
    const fence: Fence = .{ .id = @splat(1), .generation = 1, .definition = @splat(2), .partition = @splat(3), .incarnation = 4 };
    try requireInstall(&store, fence);
    try std.testing.expect(!try isRevoked(&store, fence));
    try stage(&store, fence);
    try std.testing.expect(try isRevoked(&store, fence));
    try stage(&store, fence);
    try std.testing.expectError(error.RetainedSeriesSourceRevoked, requireInstall(&store, fence));
    var other = fence;
    other.definition = @splat(9);
    try std.testing.expectError(error.RetainedSeriesDefinitionMismatch, stage(&store, other));
    try std.testing.expectError(error.RetainedSeriesDefinitionMismatch, isRevoked(&store, other));
    try std.testing.expectError(error.RetainedSeriesSourceRevoked, requireInstall(&store, other));
    other = fence;
    other.generation += 1;
    try requireInstall(&store, other);
    // A persisted malformed receipt fails closed rather than permitting a
    // late install. The fixed binary format bounds every read allocation.
    var buffer: [192]u8 = undefined;
    try store.put(try key(&buffer, fence.id, fence.generation), "broken");
    try std.testing.expectError(error.RetainedSeriesCorrupt, requireInstall(&store, fence));
}
