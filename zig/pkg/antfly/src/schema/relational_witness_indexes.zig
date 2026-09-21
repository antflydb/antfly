// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! MATCH PARTIAL uses linear-size ordered support, never mask enumeration or
//! an unbounded primary-row scan. Shared by DDL dependency admission and the
//! transaction witness planner, so an accepted mask is always seekable.
const std = @import("std");
const wire = @import("antfly_schema_openapi");

pub const owned_prefix = "__fk_partial_";

pub fn supportName(column: []const u8) [owned_prefix.len + 64]u8 {
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly partial witness support v1");
    hash.update(column);
    var digest: [32]u8 = undefined;
    hash.final(&digest);
    return owned_prefix.* ++ std.fmt.bytesToHex(digest, .lower);
}

/// Produce an ordinary schema-index definition update, leaving the version to
/// the authoritative schema CAS. Reuse existing suitable indexes; one new
/// leading-key index per uncovered column is sufficient for every null mask.
/// This helper does not grant ownership or publication authority: its caller
/// must persist the dependency and perform normal parent-schema admission.
pub fn ensureCoverage(alloc: std.mem.Allocator, schema_json: []const u8, columns: []const []const u8) !?[]u8 {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const owned = arena.allocator();
    var parent = try @import("mod.zig").parseValidatedTableSchema(owned, schema_json);
    defer parent.deinit(owned);
    var parsed = try std.json.parseFromSlice(std.json.Value, owned, schema_json, .{ .parse_numbers = false });
    if (parsed.value != .object) return error.InvalidSchemaUpdateRequest;
    var definitions: std.array_list.Managed(std.json.Value) = .init(owned);
    if (parsed.value.object.get("relational_indexes")) |existing| {
        if (existing != .null) {
            if (existing != .array) return error.InvalidSchemaUpdateRequest;
            try definitions.appendSlice(existing.array.items);
        }
    }
    var added = std.StringHashMapUnmanaged(void).empty;
    for (columns) |column| {
        if (added.contains(column)) continue;
        if (parent.relational_indexes) |indexes| {
            for (indexes.value) |index| {
                if (eligible(index) and std.mem.eql(u8, index.keys[0].column.?, column)) break;
            } else {
                try appendSupport(owned, &definitions, column, columns);
                try added.put(owned, column, {});
            }
        } else {
            try appendSupport(owned, &definitions, column, columns);
            try added.put(owned, column, {});
        }
    }
    if (added.count() == 0) return null;
    try parsed.value.object.put(owned, "relational_indexes", .{ .array = definitions });
    const encoded = try std.json.Stringify.valueAlloc(alloc, parsed.value, .{});
    errdefer alloc.free(encoded);
    var validated = try @import("mod.zig").parseValidatedTableSchema(owned, encoded);
    defer validated.deinit(owned);
    try requireCoverage(validated, columns);
    return encoded;
}

fn appendSupport(alloc: std.mem.Allocator, definitions: *std.array_list.Managed(std.json.Value), column: []const u8, columns: []const []const u8) !void {
    const name = try alloc.dupe(u8, &supportName(column));
    for (definitions.items) |definition| {
        const existing = definition.object.get("name") orelse return error.InvalidSchemaUpdateRequest;
        if (existing == .string and std.mem.eql(u8, existing.string, name)) return error.ForeignKeyPartialSupportIndexConflict;
    }
    var includes = std.ArrayList([]const u8).empty;
    for (columns) |other| if (!std.mem.eql(u8, other, column)) {
        var found = false;
        for (includes.items) |existing| if (std.mem.eql(u8, existing, other)) {
            found = true;
            break;
        };
        if (!found) try includes.append(alloc, other);
    };
    const definition: wire.RelationalIndexDefinition = .{ .name = name, .keys = &.{.{ .column = column }}, .include_columns = includes.items };
    const json = try std.json.Stringify.valueAlloc(alloc, definition, .{ .emit_null_optional_fields = false });
    const parsed = try std.json.parseFromSlice(std.json.Value, alloc, json, .{});
    try definitions.append(parsed.value);
}

fn eligible(index: anytype) bool {
    if (index.keys.len == 0 or (if (index.where) |conditions| conditions.len != 0 else false)) return false;
    for (index.keys) |key| {
        if (key.column == null or key.expression != null or (key.direction orelse .asc) != .asc or key.collation != null) return false;
    }
    return true;
}

pub fn requireCoverage(parent: anytype, columns: []const []const u8) !void {
    const indexes = parent.relational_indexes orelse return error.ForeignKeyPartialSupportIndexRequired;
    for (columns) |column| {
        for (indexes.value) |index| {
            if (eligible(index) and std.mem.eql(u8, index.keys[0].column.?, column)) break;
        } else return error.ForeignKeyPartialSupportIndexRequired;
    }
}

pub const Selection = struct { name: []const u8, values: []const std.json.Value };

/// Choose the longest exact equality prefix; residual typed predicates verify
/// every other non-null component. Definitions are borrowed from the pinned
/// parent schema, while the bound values belong to the request arena.
pub fn select(alloc: std.mem.Allocator, parent: anytype, conditions: anytype) !Selection {
    const indexes = parent.relational_indexes orelse return error.ForeignKeyPartialSupportIndexRequired;
    var selected: ?[]const u8 = null;
    var best_values: []const std.json.Value = &.{};
    errdefer alloc.free(best_values);
    for (indexes.value) |index| {
        if (!eligible(index)) continue;
        var values = std.ArrayList(std.json.Value).empty;
        defer values.deinit(alloc);
        for (index.keys) |key| {
            const value = for (conditions) |condition| {
                if (std.mem.eql(u8, key.column.?, condition.column)) break condition.value;
            } else break;
            try values.append(alloc, value);
        }
        if (values.items.len > best_values.len) {
            const replacement = try values.toOwnedSlice(alloc);
            selected = index.name;
            alloc.free(best_values);
            best_values = replacement;
        }
    }
    return .{ .name = selected orelse return error.ForeignKeyPartialSupportIndexRequired, .values = best_values };
}

test "distributed txn partial witness support selects a bounded equality prefix and pins every mask" {
    const Key = struct { column: ?[]const u8, expression: ?[]const u8 = null, direction: ?enum { asc, desc } = null, collation: ?[]const u8 = null };
    const Index = struct { name: []const u8, keys: []const Key, where: ?[]const u8 = null };
    const Parent = struct { relational_indexes: ?struct { value: []const Index } };
    const Condition = struct { column: []const u8, value: std.json.Value };
    const indexes = [_]Index{
        .{ .name = "filtered", .keys = &.{.{ .column = "a" }}, .where = "not a universal witness index" },
        .{ .name = "a", .keys = &.{.{ .column = "a" }} },
        .{ .name = "ab", .keys = &.{ .{ .column = "a" }, .{ .column = "b" } } },
        .{ .name = "b", .keys = &.{.{ .column = "b" }} },
    };
    const parent = Parent{ .relational_indexes = .{ .value = &indexes } };
    try requireCoverage(parent, &.{ "a", "b" });
    try std.testing.expectError(error.ForeignKeyPartialSupportIndexRequired, requireCoverage(Parent{ .relational_indexes = .{ .value = indexes[0..3] } }, &.{ "a", "b" }));
    const selected = try select(std.testing.allocator, parent, &[_]Condition{
        .{ .column = "b", .value = .{ .integer = 2 } },
        .{ .column = "a", .value = .{ .integer = 1 } },
    });
    defer std.testing.allocator.free(selected.values);
    try std.testing.expectEqualStrings("ab", selected.name);
    try std.testing.expectEqual(@as(i64, 1), selected.values[0].integer);
    try std.testing.expectEqual(@as(i64, 2), selected.values[1].integer);
    const partial = try select(std.testing.allocator, parent, &[_]Condition{.{ .column = "b", .value = .{ .integer = 2 } }});
    defer std.testing.allocator.free(partial.values);
    try std.testing.expectEqualStrings("b", partial.name);
}

test "distributed txn partial witness automatic support uses one normal covering index per missing column" {
    const alloc = std.testing.allocator;
    const schema = "{\"version\":7,\"storage_mode\":\"relational\",\"default_type\":\"row\",\"relational_indexes\":[{\"name\":\"existing_a\",\"keys\":[{\"column\":\"a\"}]}],\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"a\":{\"type\":\"integer\"},\"b\":{\"type\":\"integer\"}},\"additionalProperties\":false}}}}";
    const updated = (try ensureCoverage(alloc, schema, &.{ "a", "b", "b" })).?;
    defer alloc.free(updated);
    var parsed = try @import("mod.zig").parseValidatedTableSchema(alloc, updated);
    defer parsed.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 7), parsed.version);
    try std.testing.expectEqual(@as(usize, 2), parsed.relational_indexes.?.value.len);
    const added = parsed.relational_indexes.?.value[1];
    try std.testing.expectEqualStrings(&supportName("b"), added.name);
    try std.testing.expectEqualStrings("b", added.keys[0].column.?);
    try std.testing.expectEqualStrings("a", added.include_columns.?[0]);
    try std.testing.expect((try ensureCoverage(alloc, updated, &.{ "a", "b" })) == null);
    const occupied = try std.mem.replaceOwned(u8, alloc, schema, "existing_a", &supportName("b"));
    defer alloc.free(occupied);
    try std.testing.expectError(error.ForeignKeyPartialSupportIndexConflict, ensureCoverage(alloc, occupied, &.{ "a", "b" }));
}
