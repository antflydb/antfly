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

//! Inherited declarations are finalized at the native catalog's occupied-name
//! cut BEFORE initial-child schema/AIC digests are assigned. Physical owner IDs
//! come from the existing initial-child compiler; this module allocates none.
const std = @import("std");
const core = @import("../system_catalog/table_partitions.zig");
const naming = @import("../system_catalog/constraint_names.zig");
const A = std.mem.Allocator;
const Json = std.json.Value;
pub const Plan = struct {
    arena: std.heap.ArenaAllocator,
    schema_json: []const u8,
    mappings: []const core.ConstraintBinding,
    pub fn deinit(self: *Plan) void {
        self.arena.deinit();
        self.* = undefined;
    }
};
fn rules(root: *Json, field: []const u8) ![]Json {
    const list = root.object.getPtr(field) orelse return &.{};
    if (list.* != .array or list.array.items.len > 1024) return error.InvalidPartitionDescriptor;
    return list.array.items;
}
fn label(rule: *Json) !*Json {
    if (rule.* != .object) return error.InvalidPartitionDescriptor;
    const name = rule.object.getPtr("name") orelse return error.InvalidPartitionDescriptor;
    if (name.* != .string or name.string.len == 0 or name.string.len > 256) return error.InvalidPartitionDescriptor;
    return name;
}
fn keys(a: A, rule: Json) ![]const []const u8 {
    var result: std.ArrayList([]const u8) = .empty;
    if (rule.object.get("columns")) |columns| {
        if (columns != .null) {
            if (columns != .array) return error.InvalidPartitionDescriptor;
            for (columns.array.items) |column| {
                if (column != .string) return error.InvalidPartitionDescriptor;
                try result.append(a, column.string);
            }
        }
    }
    if (result.items.len == 0) if (rule.object.get("keys")) |entries| {
        if (entries != .array) return error.InvalidPartitionDescriptor;
        for (entries.array.items) |entry| {
            if (entry != .object) return error.InvalidPartitionDescriptor;
            const column = entry.object.get("column");
            try result.append(a, if (column != null and column.? == .string) column.?.string else "expr");
        }
    };
    if (rule.object.get("include_columns")) |includes| if (includes != .null) {
        if (includes != .array) return error.InvalidPartitionDescriptor;
        for (includes.array.items) |column| {
            if (column != .string) return error.InvalidPartitionDescriptor;
            try result.append(a, column.string);
        }
    };
    if (result.items.len == 0 or result.items.len > 256) return error.InvalidPartitionDescriptor;
    return result.toOwnedSlice(a);
}
/// namespace supplies checkpoint(), occupied(name,kind), relationOccupied(name)
/// from ONE current catalog writer. Its caller reserves all selected relation
/// names with initial child + parent membership in that same durable journal.
pub fn prepare(allocator: A, parent_schema_json: []const u8, child_name: []const u8, namespace: anytype, max_attempts: u32) !Plan {
    if (parent_schema_json.len > 16 * 1024 * 1024 or max_attempts == 0) return error.ProgramLimitExceeded;
    var arena = std.heap.ArenaAllocator.init(allocator);
    errdefer arena.deinit();
    const a = arena.allocator();
    var root = try std.json.parseFromSliceLeaky(Json, a, parent_schema_json, .{ .parse_numbers = false });
    if (root != .object) return error.InvalidPartitionDescriptor;
    const mode = root.object.get("storage_mode") orelse return error.InvalidPartitionDescriptor;
    if (mode != .string or !std.mem.eql(u8, mode.string, "relational")) return error.InvalidPartitionDescriptor;
    _ = root.object.orderedRemove("sql_partition");
    try root.object.put(a, "version", .{ .integer = 1 });
    const Namespace = struct {
        allocator: A,
        reader: @TypeOf(namespace),
        local: std.StringHashMapUnmanaged(void) = .empty,
        remaining: u32,
        pub fn occupied(self: *@This(), name: []const u8, kind: naming.Kind) !bool {
            try self.reader.checkpoint();
            if (self.remaining == 0) return error.ProgramLimitExceeded;
            self.remaining -= 1;
            return self.local.contains(name) or try self.reader.occupied(name, kind) or (kind.includesRelationNames() and try self.reader.relationOccupied(name));
        }
        fn reserve(self: *@This(), name: []const u8) !void {
            try self.local.put(self.allocator, name, {});
        }
    };
    var occupied: Namespace = .{ .allocator = a, .reader = namespace, .remaining = max_attempts };
    try occupied.reserve(child_name);
    var mappings: std.ArrayList(core.ConstraintBinding) = .empty;
    // PostgreSQL preserves inherited CHECK and FOREIGN KEY names. Its child
    // primary/unique/index relations receive independent physical names.
    for ([_][]const u8{ "checks", "foreign_keys" }) |field| {
        for (try rules(&root, field)) |*rule| {
            try namespace.checkpoint();
            const name = (try label(rule)).string;
            try occupied.reserve(name);
            try mappings.append(a, .{ .kind = if (std.mem.eql(u8, field, "checks")) .check else .foreign_key, .parent_name = name, .child_name = name });
        }
    }
    const uniques = try rules(&root, "unique_constraints");
    for ([_]bool{ true, false }) |primary| for (uniques) |*rule| {
        const flag = rule.object.get("primary");
        if (flag != null and flag.? != .null and flag.? != .bool) return error.InvalidPartitionDescriptor;
        const is_primary = flag != null and flag.? == .bool and flag.?.bool;
        if (is_primary != primary) continue;
        const name = try label(rule);
        const original = name.string;
        const origin = rule.object.get("origin");
        const is_index = origin != null and origin.? == .string and std.mem.eql(u8, origin.?.string, "index");
        const columns = if (is_primary) &.{} else try keys(a, rule.*);
        const candidate = if (is_index) try naming.Candidate.initIndex(child_name, columns) else try naming.Candidate.init(child_name, columns, is_primary);
        const selected = try naming.choose(a, &candidate, &occupied, max_attempts);
        try occupied.reserve(selected);
        name.* = .{ .string = selected };
        try mappings.append(a, .{ .kind = .unique, .parent_name = original, .child_name = selected });
    };
    for (try rules(&root, "relational_indexes")) |*rule| {
        try namespace.checkpoint();
        const name = try label(rule);
        const original = name.string;
        const selected = for (mappings.items) |binding| {
            if (binding.kind == .unique and std.mem.eql(u8, binding.parent_name, original)) break binding.child_name;
        } else blk: {
            const candidate = try naming.Candidate.initIndex(child_name, try keys(a, rule.*));
            const chosen = try naming.choose(a, &candidate, &occupied, max_attempts);
            try occupied.reserve(chosen);
            break :blk chosen;
        };
        name.* = .{ .string = selected };
        try mappings.append(a, .{ .kind = .index, .parent_name = original, .child_name = selected });
    }
    try core.validateInheritance(mappings.items);
    return .{ .arena = arena, .schema_json = try std.json.Stringify.valueAlloc(a, root, .{}), .mappings = try mappings.toOwnedSlice(a) };
}

test "partition inheritance allocates child index relations and preserves parent checks foreign keys and counter defaults" {
    const Reader = struct {
        pub fn checkpoint(_: *@This()) !void {}
        pub fn occupied(_: *@This(), name: []const u8, _: naming.Kind) !bool {
            return std.mem.eql(u8, name, "c_pkey");
        }
        pub fn relationOccupied(_: *@This(), name: []const u8) !bool {
            return std.mem.eql(u8, name, "c_pkey");
        }
    };
    const Runner = struct {
        fn run(a: A) !void {
            var reader: Reader = .{};
            var plan = try prepare(a,
                \\{"version":7,"storage_mode":"relational","sql_partition":{"parent":{}},"checks":[{"name":"explicit_check","expression":{"op":"literal","value":true}}],"foreign_keys":[{"name":"explicit_fk","parent_table":"p","child_columns":["id"]}],"unique_constraints":[{"name":"explicit_pk","primary":true,"columns":["at","id"]}],"relational_indexes":[{"name":"explicit_idx","keys":[{"column":"id"}]}],"column_defaults":[{"column":"id","expression":{"op":"sequence","parent_sequence":42}}]}
            , "c", &reader, 64);
            defer plan.deinit();
            try std.testing.expectEqualStrings("c_pkey1", plan.mappings[2].child_name);
            try std.testing.expectEqualStrings("c_id_idx", plan.mappings[3].child_name);
            try std.testing.expectEqualStrings("explicit_fk", plan.mappings[1].child_name);
            var parsed = try std.json.parseFromSlice(Json, a, plan.schema_json, .{});
            defer parsed.deinit();
            try std.testing.expect(parsed.value.object.get("sql_partition") == null);
            try std.testing.expectEqual(@as(i64, 1), parsed.value.object.get("version").?.integer);
            const expression = parsed.value.object.get("column_defaults").?.array.items[0].object.get("expression").?;
            try std.testing.expectEqual(@as(i64, 42), expression.object.get("parent_sequence").?.integer);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{});
}
