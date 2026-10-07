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

pub const std = @import("std");
pub const metadata_table_manager = @import("../metadata/catalog.zig");
pub const schema_mod = @import("../schema/mod.zig");
pub const full_text_indexes = @import("full_text_indexes.zig");
pub const table_create_contract = @import("table_create_contract.zig");

pub const default_full_text_index_name = full_text_indexes.default_full_text_index_name;
pub const default_indexes_json = "{\"full_text_index_v0\":{\"name\":\"full_text_index_v0\",\"type\":\"full_text\"}}";
pub const default_schema_json = "{\"version\":0,\"default_type\":\"doc\",\"enforce_types\":false,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"additionalProperties\":true,\"x-antfly-dynamic-indexing\":{\"mode\":\"infer_types\"}}}}}";

pub fn effectiveSchemaJson(schema_json: ?[]const u8) []const u8 {
    if (schema_json) |value| {
        if (value.len > 0) return value;
    }
    return default_schema_json;
}

pub const ParsedTableSchema = schema_mod.ParsedTableSchema;
pub const CreateTableRequest = table_create_contract.CreateTableRequest;

pub fn deriveTableRecord(table_name: []const u8, req: CreateTableRequest) metadata_table_manager.TableRecord {
    const min_ranges = req.num_shards orelse 1;
    return .{
        .storage = req.storage orelse .{},
        .table_id = deriveId(table_name, 0x54424c45),
        .name = table_name,
        .description = req.description orelse "",
        .schema_json = effectiveSchemaJson(req.schema_json),
        .indexes_json = req.indexes_json orelse default_indexes_json,
        .replication_sources_json = req.replication_sources_json orelse "[]",
        .placement_role = "data",
        .desired_replica_count = 3,
        .min_ranges = min_ranges,
    };
}

pub fn parseValidatedTableSchema(alloc: std.mem.Allocator, schema_json: []const u8) !ParsedTableSchema {
    return try schema_mod.parseValidatedTableSchema(alloc, schema_json);
}

pub fn validateWritesAgainstTableSchema(
    alloc: std.mem.Allocator,
    schema: ParsedTableSchema,
    writes: anytype,
) !void {
    try schema_mod.validateWritesAgainstTableSchema(alloc, schema, writes);
}

pub fn deriveRuntimeTableSchema(alloc: std.mem.Allocator, schema: ParsedTableSchema) !@import("../storage/schema.zig").TableSchema {
    return try schema_mod.deriveRuntimeTableSchema(alloc, schema);
}

pub fn deriveId(name: []const u8, seed: u64) u64 {
    const id = std.hash.Wyhash.hash(seed, name);
    return if (id == 0) 1 else id;
}

pub const coverage_policy_mod = @import("coverage_policy.zig");
pub fn validateIndexesValue(value: std.json.Value, comptime trusted_catalog: bool) !void {
    if (value != .object) return error.InvalidCreateTableRequest;
    var index_it = value.object.iterator();
    while (index_it.next()) |entry| {
        if (std.mem.eql(u8, entry.key_ptr.*, "resolvers") or std.mem.eql(u8, entry.key_ptr.*, "enrichments")) continue;
        if (trusted_catalog) {
            coverage_policy_mod.validateStoredIndexConfig(entry.value_ptr.*) catch return error.InvalidCreateTableRequest;
        } else {
            coverage_policy_mod.validateIndexConfig(entry.value_ptr.*) catch return error.InvalidCreateTableRequest;
        }
    }
}

pub fn validateStoredIndexesJson(alloc: std.mem.Allocator, indexes_json: []const u8) !void {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, indexes_json, .{}) catch return error.InvalidCreateTableRequest;
    defer parsed.deinit();
    try validateIndexesValue(parsed.value, true);
}

pub const algebraic_mod = @import("../storage/db/algebraic/mod.zig");
pub const json_helpers = @import("json_helpers.zig");
pub fn expandSchemaDerivedAlgebraicIndexesAlloc(
    alloc: std.mem.Allocator,
    table_name: []const u8,
    indexes_json: []const u8,
    schema_json: []const u8,
) ![]u8 {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, if (indexes_json.len > 0) indexes_json else default_indexes_json, .{});
    defer parsed.deinit();
    const root = switch (parsed.value) {
        .object => |object| object,
        else => return try alloc.dupe(u8, indexes_json),
    };

    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();
    var object = std.json.ObjectMap.empty;
    var changed = false;
    var it = root.iterator();
    while (it.next()) |entry| {
        const value = if (isSchemaDerivedAlgebraicIndex(entry.value_ptr.*)) blk: {
            if (schema_json.len == 0) return error.InvalidCreateTableRequest;
            changed = true;
            break :blk try schemaDerivedAlgebraicIndexValueAlloc(arena, table_name, schema_json, entry.value_ptr.*);
        } else try cloneJsonValueAlloc(arena, entry.value_ptr.*);
        try object.put(arena, try arena.dupe(u8, entry.key_ptr.*), value);
    }
    if (!changed) return try alloc.dupe(u8, indexes_json);
    return try std.json.Stringify.valueAlloc(alloc, std.json.Value{ .object = object }, .{ .emit_null_optional_fields = false });
}

pub fn expandSchemaDerivedAlgebraicIndexAlloc(
    alloc: std.mem.Allocator,
    table_name: []const u8,
    index_json: []const u8,
    schema_json: []const u8,
) ![]u8 {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, index_json, .{});
    defer parsed.deinit();
    if (!isSchemaDerivedAlgebraicIndex(parsed.value)) return try alloc.dupe(u8, index_json);
    if (schema_json.len == 0) return error.InvalidCreateTableRequest;
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const value = try schemaDerivedAlgebraicIndexValueAlloc(arena_impl.allocator(), table_name, schema_json, parsed.value);
    return try std.json.Stringify.valueAlloc(alloc, value, .{ .emit_null_optional_fields = false });
}

pub fn isSchemaDerivedAlgebraicIndex(value: std.json.Value) bool {
    if (value != .object) return false;
    const type_value = value.object.get("type") orelse return false;
    if (type_value != .string or !std.mem.eql(u8, type_value.string, "algebraic")) return false;
    const derive_value = value.object.get("derive_from_schema") orelse return false;
    return derive_value == .bool and derive_value.bool;
}

pub fn schemaDerivedAlgebraicIndexValueAlloc(
    alloc: std.mem.Allocator,
    table_name: []const u8,
    schema_json: []const u8,
    source: std.json.Value,
) !std.json.Value {
    const config_json = try algebraic_mod.schema_capability.configJsonFromSchemaJsonAlloc(alloc, table_name, schema_json);
    defer alloc.free(config_json);
    var derived = try parseJsonValueAlloc(alloc, config_json);
    if (derived != .object) return error.InvalidCreateTableRequest;
    try derived.object.put(alloc, try alloc.dupe(u8, "type"), .{ .string = try alloc.dupe(u8, "algebraic") });

    var it = source.object.iterator();
    while (it.next()) |entry| {
        if (std.mem.eql(u8, entry.key_ptr.*, "derive_from_schema")) continue;
        if (isAlgebraicInternalConfigField(entry.key_ptr.*)) continue;
        try derived.object.put(
            alloc,
            try alloc.dupe(u8, entry.key_ptr.*),
            try cloneJsonValueAlloc(alloc, entry.value_ptr.*),
        );
    }
    if (source.object.get("aggregates")) |aggregates| {
        try validatePublicAggregateRecipes(aggregates);
        var canonical: std.json.Array = .init(alloc);
        for (aggregates.array.items) |recipe| {
            var object: std.json.ObjectMap = .empty;
            var fields = recipe.object.iterator();
            while (fields.next()) |entry| {
                if (entry.value_ptr.* == .null) continue;
                try object.put(alloc, try alloc.dupe(u8, entry.key_ptr.*), try cloneJsonValueAlloc(alloc, entry.value_ptr.*));
            }
            try canonical.append(.{ .object = object });
        }
        try derived.object.put(alloc, "aggregates", .{ .array = canonical });
        try derived.object.put(alloc, "materializations", .{ .array = canonical });
    }
    return derived;
}

/// Regenerate the schema-derived config for every algebraic index in
/// `indexes_json` from `schema_json`. Used on schema update so that a
/// dynamic-template change refreshes the durable algebraic `dynamic_field_rules`
/// (and capability fingerprint) without requiring the table to be recreated.
///
/// Public algebraic indexes are always schema-derived, so each is regenerated
/// in full. User-managed runtime policy and materialization definitions are
/// preserved from the stored config, then revalidated against the regenerated
/// schema before publication. Returns the original bytes when there are no
/// algebraic indexes to refresh.
/// Public recipes describe SQL semantics, never physical reducer state or
/// private planner policy. Schema derivation validates the referenced fields.
pub fn validatePublicAggregateRecipes(value: std.json.Value) !void {
    if (value != .array or value.array.items.len > 64) return error.InvalidCreateTableRequest;
    for (value.array.items, 0..) |recipe, index| {
        if (recipe != .object) return error.InvalidCreateTableRequest;
        var fields = recipe.object.iterator();
        while (fields.next()) |entry| {
            if (!std.mem.eql(u8, entry.key_ptr.*, "name") and !std.mem.eql(u8, entry.key_ptr.*, "op") and !std.mem.eql(u8, entry.key_ptr.*, "group_by") and !std.mem.eql(u8, entry.key_ptr.*, "measure")) return error.InvalidCreateTableRequest;
        }
        const name = recipe.object.get("name") orelse return error.InvalidCreateTableRequest;
        const op = recipe.object.get("op") orelse return error.InvalidCreateTableRequest;
        if (name != .string or name.string.len == 0 or name.string.len > 128 or op != .string) return error.InvalidCreateTableRequest;
        const known = for ([_][]const u8{ "count", "sum", "avg", "min", "max" }) |kind| {
            if (std.mem.eql(u8, op.string, kind)) break true;
        } else false;
        if (!known) return error.InvalidCreateTableRequest;
        const measure = recipe.object.get("measure");
        if (measure != null and measure.? != .null) {
            if (measure.? != .string or measure.?.string.len == 0) return error.InvalidCreateTableRequest;
        } else if (!std.mem.eql(u8, op.string, "count")) return error.InvalidCreateTableRequest;
        if (recipe.object.get("group_by")) |groups| {
            if (groups != .null) {
                if (groups != .array or groups.array.items.len > 32) return error.InvalidCreateTableRequest;
                for (groups.array.items, 0..) |key, key_index| {
                    if (key != .string or key.string.len == 0) return error.InvalidCreateTableRequest;
                    for (groups.array.items[0..key_index]) |previous| if (std.mem.eql(u8, previous.string, key.string)) return error.InvalidCreateTableRequest;
                }
            }
        }
        for (value.array.items[0..index]) |previous| if (std.mem.eql(u8, previous.object.get("name").?.string, name.string)) return error.InvalidCreateTableRequest;
    }
}

pub fn isAlgebraicInternalConfigField(field: []const u8) bool {
    const internal_fields = [_][]const u8{
        "materializations",
        "group_fields",
        "measure_fields",
        "time_fields",
        "dynamic_field_rules",
        "dynamic_rules_backfill_pending",
        "joins",
        "laws",
        "capability_fingerprint",
        "capability_lifecycle_status",
        "capability_change_added_fields",
        "capability_change_removed_fields",
        "capability_change_changed_type_fields",
    };
    for (internal_fields) |internal| {
        if (std.mem.eql(u8, field, internal)) return true;
    }
    return false;
}

pub fn parseJsonValueAlloc(alloc: std.mem.Allocator, body: []const u8) !std.json.Value {
    return try json_helpers.parseOwnedJsonValueAllocAlways(alloc, body);
}

pub fn cloneJsonValueAlloc(alloc: std.mem.Allocator, value: std.json.Value) !std.json.Value {
    return try json_helpers.cloneJsonValue(alloc, value);
}

/// Admission and publication share a table-wide declaration ceiling. Public
/// recipes must not commit definitions that can never fit their artifact directory.
pub fn validateLakeIndexCapacity(a: std.mem.Allocator, schema_json: []const u8, indexes_json: []const u8) !void {
    var schema = try std.json.parseFromSlice(std.json.Value, a, schema_json, .{});
    defer schema.deinit();
    if (schema.value != .object) return;
    const base_source = schema.value.object.get("base_source") orelse return;
    if (base_source != .object) return;
    const kind = base_source.object.get("kind") orelse return;
    if (kind != .string or !std.mem.eql(u8, kind.string, "external")) return;
    var parsed = try std.json.parseFromSlice(std.json.Value, a, indexes_json, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidCreateTableRequest;
    var count: usize = 0;
    if (schema.value.object.get("relational_indexes")) |rows| {
        if (rows != .array) return error.InvalidCreateTableRequest;
        count = rows.array.items.len;
    }
    var it = parsed.value.object.iterator();
    while (it.next()) |entry| {
        const config = entry.value_ptr.*;
        if (config != .object) return error.InvalidCreateTableRequest;
        const index_kind = config.object.get("type") orelse continue;
        if (index_kind != .string) return error.InvalidCreateTableRequest;
        if (std.mem.eql(u8, index_kind.string, "algebraic")) {
            const recipes = config.object.get("materializations") orelse config.object.get("aggregates") orelse continue;
            if (recipes != .array) return error.InvalidCreateTableRequest;
            count += recipes.array.items.len;
        } else {
            count += 1;
            if (config.object.get("metrics")) |metrics| {
                if (metrics == .object) count += metrics.object.count();
            }
        }
        if (count > @import("../metadata/lake_index_catalog.zig").max_directory_artifacts) return error.InvalidCreateTableRequest;
    }
}

test "lake catalog admission bounds recipes across indexes before commit" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const aa = arena.allocator();
    var configs = std.json.ObjectMap.empty;
    var recipes: std.json.Array = .init(aa);
    for (0..64) |_| try recipes.append(.null);
    for (0..65) |index| {
        var config = std.json.ObjectMap.empty;
        try config.put(aa, "type", .{ .string = "algebraic" });
        try config.put(aa, "materializations", .{ .array = recipes });
        try configs.put(aa, try std.fmt.allocPrint(aa, "index{d}", .{index}), .{ .object = config });
    }
    const schema = "{\"base_source\":{\"kind\":\"external\"}}";
    const too_many = try std.json.Stringify.valueAlloc(aa, std.json.Value{ .object = configs }, .{});
    try std.testing.expectError(error.InvalidCreateTableRequest, validateLakeIndexCapacity(a, schema, too_many));
    _ = configs.orderedRemove("index64");
    const accepted = try std.json.Stringify.valueAlloc(aa, std.json.Value{ .object = configs }, .{});
    try validateLakeIndexCapacity(a, schema, accepted);
}
