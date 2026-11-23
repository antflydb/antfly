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

//! Source-ordered effects compiled into canonical native per-table mutations.
//! A full delete discards preceding transforms for that key; later transforms
//! keep their own upsert semantics and execute against the deleted base.
const std = @import("std");
const values = @import("db/mutation_values.zig");
const A = std.mem.Allocator;
pub const Limits = struct { keys: usize = 65536, operations: usize = 1048576, bytes: usize = 256 * 1024 * 1024 };
pub const Table = struct {
    table_name: []const u8,
    deletes: []const []const u8,
    transforms: []const values.DocumentTransform,
};
pub const Plan = struct {
    arena: std.heap.ArenaAllocator,
    tables: std.StringArrayHashMapUnmanaged(TablePlan) = .empty,
    limits: Limits,
    keys: usize = 0,
    operations: usize = 0,
    bytes: usize = 0,
    failed: bool = false,
    result: ?[]const Table = null,

    const KeyPlan = struct {
        reset: bool = false,
        transforms: std.ArrayList(values.DocumentTransform) = .empty,
    };
    const TablePlan = struct { keys: std.StringArrayHashMapUnmanaged(KeyPlan) = .empty };

    pub fn init(allocator: A, limits: Limits) Plan {
        return .{ .arena = .init(allocator), .limits = limits };
    }
    pub fn deinit(self: *Plan) void {
        self.arena.deinit();
        self.* = undefined;
    }
    fn charge(self: *Plan, count: usize) !void {
        self.bytes = std.math.add(usize, self.bytes, count) catch return error.TransactionTooLarge;
        if (self.bytes > self.limits.bytes) return error.TransactionTooLarge;
    }
    fn entry(self: *Plan, table: []const u8, key: []const u8) !*KeyPlan {
        if (self.failed or self.result != null) return error.InvalidSourceTransaction;
        if (table.len == 0 or key.len == 0) return error.InvalidSourceTransaction;
        const a = self.arena.allocator();
        const found_table = try self.tables.getOrPut(a, table);
        if (!found_table.found_existing) {
            try self.charge(table.len);
            found_table.key_ptr.* = try a.dupe(u8, table);
            found_table.value_ptr.* = .{};
        }
        const found = try found_table.value_ptr.keys.getOrPut(a, key);
        if (!found.found_existing) {
            self.keys = std.math.add(usize, self.keys, 1) catch return error.TransactionTooLarge;
            if (self.keys > self.limits.keys) return error.TransactionTooLarge;
            try self.charge(key.len);
            found.key_ptr.* = try a.dupe(u8, key);
            found.value_ptr.* = .{};
        }
        return found.value_ptr;
    }
    pub fn delete(self: *Plan, table: []const u8, key: []const u8) !void {
        errdefer self.failed = true;
        self.operations = std.math.add(usize, self.operations, 1) catch return error.TransactionTooLarge;
        if (self.operations > self.limits.operations) return error.TransactionTooLarge;
        const found = try self.entry(table, key);
        // Clearing retains arena capacity, and all input operations remain
        // charged. Alternating writes/deletes cannot evade admission bounds.
        found.transforms.clearRetainingCapacity();
        found.reset = true;
    }
    pub fn transform(self: *Plan, table: []const u8, transform_value: values.DocumentTransform) !void {
        errdefer self.failed = true;
        const found = try self.entry(table, transform_value.key);
        self.operations = std.math.add(usize, self.operations, (std.math.add(usize, transform_value.operations.len, 1) catch return error.TransactionTooLarge)) catch return error.TransactionTooLarge;
        if (self.operations > self.limits.operations) return error.TransactionTooLarge;
        const a = self.arena.allocator();
        const operation_bytes = std.math.mul(usize, transform_value.operations.len, @sizeOf(values.TransformOp)) catch return error.TransactionTooLarge;
        try self.charge(operation_bytes);
        try self.charge(@sizeOf(values.DocumentTransform));
        try self.charge(transform_value.key.len);
        const ops = try a.alloc(values.TransformOp, transform_value.operations.len);
        for (transform_value.operations, ops) |op, *out| {
            try self.charge(op.path.len);
            if (op.value_json) |json| try self.charge(json.len);
            out.* = .{
                .op = op.op,
                .path = try a.dupe(u8, op.path),
                .value_json = if (op.value_json) |json| try a.dupe(u8, json) else null,
            };
        }
        try found.transforms.append(a, .{
            .key = try a.dupe(u8, transform_value.key),
            .operations = ops,
            .upsert = transform_value.upsert,
        });
    }
    pub fn finish(self: *Plan) ![]const Table {
        if (self.failed) return error.InvalidSourceTransaction;
        if (self.result) |result| return result;
        errdefer self.failed = true;
        const a = self.arena.allocator();
        const result = try a.alloc(Table, self.tables.count());
        for (self.tables.keys(), self.tables.values(), result) |table_name, table, *out| {
            var deletes: std.ArrayList([]const u8) = .empty;
            var transforms: std.ArrayList(values.DocumentTransform) = .empty;
            for (table.keys.keys(), table.keys.values()) |key, value| {
                if (value.reset) try deletes.append(a, key);
                try transforms.appendSlice(a, value.transforms.items);
            }
            out.* = .{ .table_name = table_name, .deletes = deletes.items, .transforms = transforms.items };
        }
        self.result = result;
        return result;
    }
};

test "source effects preserve repeated key order reset and individual upsert semantics" {
    const a = std.testing.allocator;
    var plan = Plan.init(a, .{});
    defer plan.deinit();
    const inc: values.DocumentTransform = .{ .key = "counter", .upsert = true, .operations = &.{.{ .op = .inc, .path = "count", .value_json = "1" }} };
    try plan.transform("docs", inc);
    try plan.transform("docs", inc);
    try plan.delete("docs", "counter");
    var no_upsert = inc;
    no_upsert.upsert = false;
    try plan.transform("docs", no_upsert);
    try plan.transform("docs", inc);
    try plan.transform("docs", inc);
    try plan.transform("other", inc);
    const tables = try plan.finish();
    try std.testing.expectEqual(@as(usize, 2), tables.len);
    var resolved = try @import("db/mutation_plan.zig").prepare(a, &.{}, tables[0].deletes, tables[0].transforms, &.{});
    defer resolved.deinit(a);
    try std.testing.expectEqual(@as(usize, 1), resolved.writes.len);
    try std.testing.expectEqualStrings("{\"count\":2}", resolved.writes[0].value);
    try std.testing.expectEqual(@as(usize, 3), tables[0].transforms.len);
    var deleted = Plan.init(a, .{});
    defer deleted.deinit();
    try deleted.transform("docs", inc);
    try deleted.delete("docs", "counter");
    const after = try deleted.finish();
    try std.testing.expectEqual(@as(usize, 0), after[0].transforms.len);
    try std.testing.expectEqualStrings("counter", after[0].deletes[0]);
}

test "source effects enforce cumulative budgets and poison rejected partial plans" {
    var plan = Plan.init(std.testing.allocator, .{ .operations = 3 });
    defer plan.deinit();
    const transform_value: values.DocumentTransform = .{ .key = "a", .operations = &.{.{ .op = .set, .path = "x", .value_json = "1" }} };
    try plan.transform("docs", transform_value);
    try plan.delete("docs", "a");
    try std.testing.expectError(error.TransactionTooLarge, plan.transform("docs", transform_value));
    try std.testing.expectError(error.InvalidSourceTransaction, plan.finish());
}
