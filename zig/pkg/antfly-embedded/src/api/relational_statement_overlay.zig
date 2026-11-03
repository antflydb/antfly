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

//! Owned, indexed effects of already validated SQL statements. This is a
//! speculative read overlay, never commit authority. Its caller must fence the
//! complete input state and invalidate it after rollback or an interrupted add.
const std = @import("std");
const contract = @import("distributed_txn_contract.zig");
const storage = @import("../storage/db/relational_integrity_contract.zig");

pub const Overlay = struct {
    pub const Rows = std.StringHashMapUnmanaged(?[]const u8);
    const Table = struct {
        rows: Rows = .empty,
        commands: std.AutoHashMapUnmanaged(storage.Address, std.ArrayList(storage.Command)) = .empty,
    };
    arena: std.heap.ArenaAllocator,
    tables: std.StringHashMapUnmanaged(*Table) = .empty,

    pub fn init(backing: std.mem.Allocator) Overlay {
        return .{ .arena = .init(backing) };
    }
    pub fn deinit(self: *Overlay) void {
        self.arena.deinit();
        self.* = undefined;
    }
    fn table(self: *Overlay, name: []const u8) !*Table {
        if (self.tables.get(name)) |found| return found;
        const a = self.arena.allocator();
        const owned = try a.dupe(u8, name);
        const result = try a.create(Table);
        result.* = .{};
        try self.tables.put(a, owned, result);
        return result;
    }
    /// Input arenas may disappear immediately afterwards. Retain only rows and
    /// canonical constraint effects, not source callbacks or prepared runtimes.
    /// An allocation failure invalidates the partially extended overlay.
    pub fn add(self: *Overlay, requests: []const contract.TableCommitRequest) !void {
        const a = self.arena.allocator();
        for (requests) |request| {
            const target = try self.table(request.table_name);
            for (request.writes) |write| {
                const entry = try target.rows.getOrPut(a, write.key);
                if (!entry.found_existing) entry.key_ptr.* = try a.dupe(u8, write.key);
                entry.value_ptr.* = try a.dupe(u8, write.value);
            }
            for (request.deletes) |key| {
                const entry = try target.rows.getOrPut(a, key);
                if (!entry.found_existing) entry.key_ptr.* = try a.dupe(u8, key);
                entry.value_ptr.* = null;
            }
            for (request.integrity_commands) |command| {
                const entry = try target.commands.getOrPut(a, command.address);
                if (!entry.found_existing) entry.value_ptr.* = .empty;
                try entry.value_ptr.append(a, try own(storage.Command, a, command));
            }
        }
    }
    pub fn rows(self: *const Overlay, name: []const u8) ?*const Rows {
        const found = self.tables.get(name) orelse return null;
        return &found.rows;
    }
    pub fn commands(self: *const Overlay, name: []const u8, address: storage.Address) []const storage.Command {
        const found = self.tables.get(name) orelse return &.{};
        return if (found.commands.get(address)) |list| list.items else &.{};
    }
};

// Command payloads contain tagged values and owned byte slices. Keep cloning
// structural so every variant preserves its exact interpretation and no JSON
// encoding/decoding or second constraint interpreter enters this cache.
fn own(comptime T: type, a: std.mem.Allocator, value: T) std.mem.Allocator.Error!T {
    return switch (@typeInfo(T)) {
        .pointer => |p| blk: {
            if (p.size != .slice) @compileError("overlay command contains a borrowed pointer");
            const out = try a.alloc(p.child, value.len);
            for (value, out) |item, *target| target.* = try own(p.child, a, item);
            break :blk out;
        },
        .optional => |o| if (value) |item| try own(o.child, a, item) else null,
        .@"struct" => |s| blk: {
            var out: T = undefined;
            inline for (s.field_names, s.field_types) |name, Field| @field(out, name) = try own(Field, a, @field(value, name));
            break :blk out;
        },
        .@"union" => |u| blk: {
            inline for (u.field_names, u.field_types) |name, Field| if (std.meta.activeTag(value) == @field(u.tag_type.?, name))
                break :blk @unionInit(T, name, try own(Field, a, @field(value, name)));
            unreachable;
        },
        .array => |array| blk: {
            var out: T = undefined;
            for (0..array.len) |i| out[i] = try own(array.child, a, value[i]);
            break :blk out;
        },
        else => value,
    };
}

test "relational statement overlay owns deltas and indexes only the affected row and claim" {
    const a = std.testing.allocator;
    const Fault = struct {
        fn run(alloc: std.mem.Allocator) !void {
            var overlay = Overlay.init(alloc);
            defer overlay.deinit();
            const first = try storage.Address.init(@splat(1), "tuple-first");
            const second = try storage.Address.init(@splat(1), "tuple-second");
            {
                var input = std.heap.ArenaAllocator.init(a);
                defer input.deinit();
                const ia = input.allocator();
                const name = try ia.dupe(u8, "table");
                const key = try ia.dupe(u8, "row");
                const value = try ia.dupe(u8, "{\"n\":1}");
                const claim: storage.Claim = .{ .tuple = try ia.dupe(u8, "tuple-first"), .parent_table = name, .parent_key = key, .schema_version = 1 };
                try overlay.add(&.{.{ .table_name = name, .writes = &.{.{ .key = key, .value = value }}, .integrity_commands = &.{.{ .address = first, .operation = .{ .establish = claim } }} }});
            }
            try std.testing.expectEqualStrings("{\"n\":1}", overlay.rows("table").?.get("row").?.?);
            try std.testing.expectEqualStrings("tuple-first", overlay.commands("table", first)[0].operation.establish.tuple);
            try overlay.add(&.{.{ .table_name = "table", .deletes = &.{"row"}, .integrity_commands = &.{ .{ .address = first, .operation = .{ .release = .{ .parent_table = "table", .parent_key = "row" } } }, .{ .address = second, .operation = .{ .check_owner = .{ .parent_table = "table", .parent_key = "another" } } } } }});
            try std.testing.expect(overlay.rows("table").?.contains("row"));
            try std.testing.expect(overlay.rows("table").?.get("row").? == null);
            try std.testing.expectEqual(@as(usize, 2), overlay.commands("table", first).len);
            try std.testing.expectEqual(@as(usize, 1), overlay.commands("table", second).len);
            try std.testing.expectEqual(@as(usize, 0), overlay.commands("other", first).len);
            try overlay.add(&.{.{ .table_name = "table", .writes = &.{.{ .key = "row", .value = "new" }} }});
            try std.testing.expectEqualStrings("new", overlay.rows("table").?.get("row").?.?);
        }
    };
    try Fault.run(a);
    try std.testing.checkAllAllocationFailures(a, Fault.run, .{});
}
