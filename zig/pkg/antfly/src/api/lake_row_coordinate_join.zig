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

//! Private bounded physical-to-logical coordinates from an authenticated ID join.
//! Two sequential remote cursors replace random per-payload-tile cloud seeks.
const std = @import("std");
const local = @import("antfly_local_sources");
const identity = @import("lake_row_identity.zig");
const tree = @import("../serverless/graph_segment/page_tree.zig");
const Mutations = @import("lake_index_page_mutations.zig").Mutations;
const A = std.mem.Allocator;

pub fn build(a: A, manager: *local.sql_spill.Manager, source: identity.Space, target: identity.Space, remote: tree.Store, destination: tree.Store) !?tree.Ref {
    if (source.rows != target.rows) return error.InvalidLogicalRowIdentity;
    var physical = try tree.Cursor.init(a, remote, source.by_id, "", null);
    defer physical.deinit();
    var logical = try tree.Cursor.init(a, remote, target.by_id, "", null);
    defer logical.deinit();
    var edits = Mutations.init(a, manager, destination);
    defer edits.deinit();
    var count: u64 = 0;
    while (try physical.next()) |row| {
        try manager.check();
        const other = (try logical.next()) orelse return error.InvalidLogicalRowIdentity;
        if (!std.mem.eql(u8, row.key, other.key)) return error.InvalidLogicalRowIdentity;
        const from = try identity.decodeState(source, row.key, row.value);
        const to = try identity.decodeState(target, other.key, other.value);
        if (!from.state.unchanged(to.state) or from.coordinate.file >= source.file_count or to.coordinate.file >= target.file_count) return error.InvalidLogicalRowIdentity;
        const key = from.coordinate.encode();
        const value = to.coordinate.encode();
        try edits.addRecord(&key, &value);
        count += 1;
    }
    if (count != source.rows or (try logical.next()) != null) return error.InvalidLogicalRowIdentity;
    const root = try edits.finish(null);
    // The reducer merges duplicate keys; a verified bijection must retain every
    // physical coordinate. Reject duplicates rather than silently losing rows.
    if ((if (root) |ref| ref.records else 0) != count) return error.InvalidLogicalRowIdentity;
    return root;
}

test "external lake execution coordinate join is bounded and rejects invalid bijections" {
    const a = std.testing.allocator;
    const Check = struct {
        fn call(_: *anyopaque) !void {}
    };
    var marker: u8 = 0;
    var manager: local.sql_spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &marker, .checkpoint = Check.call, .max_bytes = 32 * 1024 * 1024, .async_writes = false };
    defer manager.deinit();
    var memory: tree.testing.MemoryStore = .{ .alloc = a };
    defer memory.deinit();
    var from_edits = Mutations.init(a, &manager, memory.store());
    defer from_edits.deinit();
    var location_edits = Mutations.init(a, &manager, memory.store());
    defer location_edits.deinit();
    var to_edits = Mutations.init(a, &manager, memory.store());
    defer to_edits.deinit();
    var changed_edits = Mutations.init(a, &manager, memory.store());
    defer changed_edits.deinit();
    var duplicate_edits = Mutations.init(a, &manager, memory.store());
    defer duplicate_edits.deinit();
    const count = 8192;
    for (0..count) |i| {
        var key: [32]u8 = @splat(0);
        std.mem.writeInt(u64, key[24..32], i + 1, .big);
        var from: [56]u8 = @splat(7);
        var to: [56]u8 = @splat(7);
        from[0..16].* = (identity.logical.Coordinate{ .file = 9, .group = 3, .row = (i * 2053) % count }).encode();
        to[0..16].* = (identity.logical.Coordinate{ .file = @intCast(i % 13), .group = 0, .row = i / 13 }).encode();
        std.mem.writeInt(u64, from[48..56], 0, .big);
        std.mem.writeInt(u64, to[48..56], 0, .big);
        try from_edits.addRecord(&key, &from);
        const location = identity.locationKey("physical", 3, (i * 2053) % count);
        var member: [64]u8 = undefined;
        member[0..32].* = key;
        member[32..64].* = from[16..48].*;
        try location_edits.addRecord(&location, &member);
        try to_edits.addRecord(&key, &to);
        to[16] ^= 1;
        try changed_edits.addRecord(&key, &to);
        if (i == count - 1) from[0..16].* = (identity.logical.Coordinate{ .file = 9, .group = 3, .row = ((count - 2) * 2053) % count }).encode();
        try duplicate_edits.addRecord(&key, &from);
    }
    const from_root = try from_edits.finish(null);
    const to_root = try to_edits.finish(null);
    const location_root = try location_edits.finish(null);
    const changed_root = try changed_edits.finish(null);
    const duplicate_root = try duplicate_edits.finish(null);
    const from: identity.Space = .{ .source = "source", .snapshot = "physical", .rows = count, .file_count = 10, .files = null, .by_id = from_root, .by_location = null };
    var to = from;
    to.by_id = to_root;
    to.file_count = 13;
    var measured = struct {
        backing: tree.Store,
        reads: u64 = 0,
        fn read(raw: *anyopaque, alloc: A, ref: tree.Ref) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.reads += 1;
            return self.backing.get(self.backing.ptr, alloc, ref);
        }
        fn write(_: *anyopaque, _: tree.Ref, _: []const u8) !void {
            return error.AccessDenied;
        }
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.backing.check(self.backing.ptr);
        }
    }{ .backing = memory.store() };
    const remote: tree.Store = .{ .domain = measured.backing.domain, .ptr = &measured, .get = @TypeOf(measured).read, .put = @TypeOf(measured).write, .check = @TypeOf(measured).check };
    const root = try build(a, &manager, from, to, remote, memory.store());
    const sequential_reads = measured.reads;
    try std.testing.expect(sequential_reads < count / 8);
    measured.reads = 0;
    var point_cache: tree.Cache = .{ .alloc = a, .underlying = remote, .max_bytes = 2 * 1024 * 1024 };
    defer point_cache.deinit();
    var first: usize = 0;
    while (first < count) : (first += 256) {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const ta = arena.allocator();
        const keys = try ta.alloc([44]u8, 256);
        const pointers = try ta.alloc([]const u8, 256);
        for (keys, pointers, first..) |*key, *pointer, row| {
            key.* = identity.locationKey("physical", 3, row);
            pointer.* = key;
        }
        const members = try tree.lookupMany(ta, point_cache.store(), location_root, pointers);
        for (members, pointers) |entry, *pointer| pointer.* = (entry orelse return error.InvalidLogicalRowIdentity).value[0..32];
        const locations = try tree.lookupMany(ta, point_cache.store(), to_root, pointers);
        for (locations) |entry| try std.testing.expect(entry != null);
    }
    const point_reads = measured.reads;
    try std.testing.expect(point_reads > sequential_reads);
    std.debug.print("COORDINATE_JOIN rows={} sequential_remote_reads={} tiled_remote_reads={}\n", .{ count, sequential_reads, point_reads });

    try std.testing.expectEqual(@as(u64, count), root.?.records);
    for ([_]usize{ 0, 31, 256, count - 1 }) |i| {
        const key = (identity.logical.Coordinate{ .file = 9, .group = 3, .row = (i * 2053) % count }).encode();
        const entries = try tree.lookupMany(a, memory.store(), root, &.{&key});
        defer a.free(entries);
        const entry = entries[0].?;
        defer entry.deinit(a);
        const coordinate = try identity.logical.Coordinate.decode(entry.value);
        try std.testing.expectEqual(@as(u32, @intCast(i % 13)), coordinate.file);
        try std.testing.expectEqual(@as(u64, i / 13), coordinate.row);
    }
    var changed = to;
    changed.by_id = changed_root;
    try std.testing.expectError(error.InvalidLogicalRowIdentity, build(a, &manager, from, changed, memory.store(), memory.store()));
    var missing = to;
    missing.rows -= 1;
    try std.testing.expectError(error.InvalidLogicalRowIdentity, build(a, &manager, from, missing, memory.store(), memory.store()));
    var duplicate = from;
    duplicate.by_id = duplicate_root;
    try std.testing.expectError(error.InvalidLogicalRowIdentity, build(a, &manager, duplicate, to, memory.store(), memory.store()));
}
