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

const std = @import("std");

const binder = @import("binder.zig");
const ddl_plan = @import("ddl_plan.zig");
const expr_type = @import("expr/type.zig");
const lower_expr = @import("lower_expr.zig");
const runtime_schema = @import("../storage/schema.zig");
const schema_mod = @import("../schema/mod.zig");
const value_mod = @import("value.zig");

pub fn alterRelationalColumnTypeAlloc(
    alloc: std.mem.Allocator,
    schema: *runtime_schema.TableSchema,
    operation: ddl_plan.AlterColumnTypeOperation,
) !void {
    const index = binder.relationalColumnIndex(schema.relational_columns, operation.column_name) orelse return error.InvalidSqlCatalog;
    const columns = @constCast(schema.relational_columns);
    if (columns[index].generated != null) return error.UnsupportedSqlShape;
    const supports_collation = binder.relationalColumnTypeSupportsCollation(operation.field_type, operation.array_item_type);
    if (operation.collation != null and !supports_collation) return error.UnsupportedSqlShape;
    if (!supports_collation and columns[index].collation != null) return error.UnsupportedSqlShape;
    const new_array_item_type = if (operation.field_type == .array) operation.array_item_type orelse return error.InvalidSqlCatalog else null;
    const new_collation = if (operation.collation) |collation| try alloc.dupe(u8, collation) else null;
    columns[index].field_type = operation.field_type;
    columns[index].array_item_type = new_array_item_type;
    if (new_collation) |collation| {
        if (columns[index].collation) |existing| alloc.free(existing);
        columns[index].collation = collation;
    }
    if (columns[index].default_value) |default_value| try value_mod.validateDefaultValueForColumnAlloc(alloc, columns[index], default_value);
    if (columns[index].on_update_value) |on_update_value| try value_mod.validateDefaultValueForColumnAlloc(alloc, columns[index], on_update_value);
    try expr_type.validateRelationalColumnCatalog(schema.relational_columns);
    try binder.validateRelationalPeriodCatalog(schema.relational_columns, schema.periods);
    if (schema.primary_key) |primary_key| {
        try expr_type.validatePrimaryKeyColumns(schema.relational_columns, primary_key);
        try binder.validatePrimaryKeyTemporalCatalog(schema.periods, primary_key);
        if (primary_key.name) |name| {
            if (binder.uniqueConstraintNameExists(schema.unique_constraints, name) or
                binder.foreignKeyNameExists(schema.foreign_keys, name) or
                binder.relationalCheckNameExists(schema.checks, name))
            {
                return error.InvalidSqlCatalog;
            }
        }
    }
    try expr_type.validateUniqueConstraintCatalog(schema.relational_columns, schema.periods, schema.unique_constraints);
    try expr_type.validateForeignKeyCatalog(schema.relational_columns, schema.periods, schema.foreign_keys);
    try expr_type.validateRelationalCheckCatalog(schema.relational_columns, schema.checks);
}

fn foreignKeyValidationStateString(state: runtime_schema.ForeignKeyValidationState) ![]const u8 {
    return switch (state) {
        .enforced => "enforced",
        .unvalidated => "unvalidated",
        .validating, .invalid => error.InvalidSchemaUpdateRequest,
    };
}

fn uniqueConstraintValidationStateString(state: runtime_schema.UniqueConstraintValidationState) ![]const u8 {
    return switch (state) {
        .enforced => "enforced",
        .unvalidated => "unvalidated",
        .validating, .invalid => error.InvalidSchemaUpdateRequest,
    };
}

pub fn schemaWithForeignKeyValidationStateAlloc(
    alloc: std.mem.Allocator,
    schema_json: []const u8,
    constraint_name: []const u8,
    state: runtime_schema.ForeignKeyValidationState,
) ![]u8 {
    const state_text = try foreignKeyValidationStateString(state);
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, schema_json, .{});
    defer parsed.deinit();

    const root = switch (parsed.value) {
        .object => |*object| object,
        else => return error.InvalidSchemaUpdateRequest,
    };
    const foreign_keys = root.getPtr("foreign_keys") orelse return error.ForeignKeyNotFound;
    const foreign_key_items = switch (foreign_keys.*) {
        .array => |*array| array.items,
        else => return error.InvalidSchemaUpdateRequest,
    };

    var found = false;
    for (foreign_key_items) |*foreign_key| {
        const object = switch (foreign_key.*) {
            .object => |*object| object,
            else => return error.InvalidSchemaUpdateRequest,
        };
        const name = object.get("name") orelse return error.InvalidSchemaUpdateRequest;
        if (name != .string) return error.InvalidSchemaUpdateRequest;
        if (!std.mem.eql(u8, name.string, constraint_name)) continue;

        const validation_state = object.getPtr("validation_state") orelse return error.InvalidSchemaUpdateRequest;
        validation_state.* = .{ .string = state_text };
        found = true;
        break;
    }
    if (!found) return error.ForeignKeyNotFound;

    const updated = try std.json.Stringify.valueAlloc(alloc, parsed.value, .{});
    errdefer alloc.free(updated);
    var validated = try schema_mod.parseValidatedTableSchema(alloc, updated);
    validated.deinit(alloc);
    return updated;
}

pub fn schemaWithUniqueConstraintValidationStateAlloc(
    alloc: std.mem.Allocator,
    schema_json: []const u8,
    constraint_name: []const u8,
    state: runtime_schema.UniqueConstraintValidationState,
) ![]u8 {
    const state_text = try uniqueConstraintValidationStateString(state);
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, schema_json, .{});
    defer parsed.deinit();

    const root = switch (parsed.value) {
        .object => |*object| object,
        else => return error.InvalidSchemaUpdateRequest,
    };
    const unique_constraints = root.getPtr("unique_constraints") orelse return error.UniqueConstraintNotFound;
    const constraint_items = switch (unique_constraints.*) {
        .array => |*array| array.items,
        else => return error.InvalidSchemaUpdateRequest,
    };

    var found = false;
    for (constraint_items) |*constraint| {
        const object = switch (constraint.*) {
            .object => |*object| object,
            else => return error.InvalidSchemaUpdateRequest,
        };
        const name = object.get("name") orelse return error.InvalidSchemaUpdateRequest;
        if (name != .string) return error.InvalidSchemaUpdateRequest;
        if (!std.mem.eql(u8, name.string, constraint_name)) continue;

        const validation_state = object.getPtr("validation_state") orelse return error.InvalidSchemaUpdateRequest;
        validation_state.* = .{ .string = state_text };
        found = true;
        break;
    }
    if (!found) return error.UniqueConstraintNotFound;

    const updated = try std.json.Stringify.valueAlloc(alloc, parsed.value, .{});
    errdefer alloc.free(updated);
    var validated = try schema_mod.parseValidatedTableSchema(alloc, updated);
    validated.deinit(alloc);
    return updated;
}

pub fn schemaWithSecondaryIndexReadyAlloc(
    alloc: std.mem.Allocator,
    schema_json: []const u8,
    index_name: []const u8,
    expected_generation: u64,
) ![]u8 {
    return try schemaWithSecondaryIndexReadyCheckedAlloc(alloc, schema_json, index_name, .{
        .generation = expected_generation,
    });
}

pub const SecondaryIndexReadyExpectation = struct {
    generation: u64,
    access_method: ?runtime_schema.RelationalIndexAccessMethod = null,
    schema_fingerprint: ?[]const u8 = null,
};

pub fn schemaWithSecondaryIndexReadyCheckedAlloc(
    alloc: std.mem.Allocator,
    schema_json: []const u8,
    index_name: []const u8,
    expected: SecondaryIndexReadyExpectation,
) ![]u8 {
    const expected_generation = expected.generation;
    if (expected_generation == 0) return error.InvalidSchemaUpdateRequest;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, schema_json, .{});
    defer parsed.deinit();

    const root = switch (parsed.value) {
        .object => |*object| object,
        else => return error.InvalidSchemaUpdateRequest,
    };
    const default_type_value = root.get("default_type") orelse return error.InvalidSchemaUpdateRequest;
    if (default_type_value != .string) return error.InvalidSchemaUpdateRequest;
    const document_schemas = root.getPtr("document_schemas") orelse return error.InvalidSchemaUpdateRequest;
    if (document_schemas.* != .object) return error.InvalidSchemaUpdateRequest;
    const document_schema = document_schemas.object.getPtr(default_type_value.string) orelse return error.InvalidSchemaUpdateRequest;
    if (document_schema.* != .object) return error.InvalidSchemaUpdateRequest;
    const schema = document_schema.object.getPtr("schema") orelse return error.InvalidSchemaUpdateRequest;
    if (schema.* != .object) return error.InvalidSchemaUpdateRequest;
    const properties = schema.object.getPtr("properties") orelse return error.InvalidSchemaUpdateRequest;
    if (properties.* != .object) return error.InvalidSchemaUpdateRequest;
    const property = schemaPropertyForSecondaryIndex(properties, index_name) orelse return error.SecondaryIndexNotFound;
    if (property.* != .object) return error.InvalidSchemaUpdateRequest;

    const generation_value = property.object.get("x-antfly-index-generation") orelse return error.SecondaryIndexGenerationMismatch;
    if (generation_value != .integer or generation_value.integer <= 0) return error.InvalidSchemaUpdateRequest;
    const generation: u64 = @intCast(generation_value.integer);
    if (generation != expected_generation) return error.SecondaryIndexGenerationMismatch;

    if (expected.access_method) |expected_method| {
        const access_method_value = property.object.get("x-antfly-index-access-method") orelse return error.SecondaryIndexAccessMethodMismatch;
        if (access_method_value != .string) return error.InvalidSchemaUpdateRequest;
        const actual_method = runtime_schema.RelationalIndexAccessMethod.fromString(access_method_value.string) orelse return error.InvalidSchemaUpdateRequest;
        if (actual_method != expected_method) return error.SecondaryIndexAccessMethodMismatch;
    }

    if (expected.schema_fingerprint) |expected_fingerprint| {
        if (expected_fingerprint.len == 0) return error.InvalidSchemaUpdateRequest;
        const fingerprint_value = property.object.get("x-antfly-index-schema-fingerprint") orelse return error.SecondaryIndexSchemaFingerprintMismatch;
        if (fingerprint_value != .string or fingerprint_value.string.len == 0) return error.InvalidSchemaUpdateRequest;
        if (!std.mem.eql(u8, fingerprint_value.string, expected_fingerprint)) return error.SecondaryIndexSchemaFingerprintMismatch;
    }

    const lifecycle_value = property.object.getPtr("x-antfly-index-lifecycle") orelse return error.SecondaryIndexNotBuilding;
    if (lifecycle_value.* != .string) return error.InvalidSchemaUpdateRequest;
    if (!std.mem.eql(u8, lifecycle_value.string, "building") and !std.mem.eql(u8, lifecycle_value.string, "catching_up")) return error.SecondaryIndexNotBuilding;
    lifecycle_value.* = .{ .string = "ready" };

    const updated = try std.json.Stringify.valueAlloc(alloc, parsed.value, .{});
    errdefer alloc.free(updated);
    var validated = try schema_mod.parseValidatedTableSchema(alloc, updated);
    validated.deinit(alloc);
    return updated;
}

fn schemaPropertyForSecondaryIndex(properties: *std.json.Value, index_name: []const u8) ?*std.json.Value {
    if (properties.* != .object) return null;
    var it = properties.object.iterator();
    while (it.next()) |entry| {
        if (entry.value_ptr.* != .object) continue;
        const declared = entry.value_ptr.object.get("x-antfly-index-name") orelse continue;
        if (declared == .string and std.mem.eql(u8, declared.string, index_name)) return entry.value_ptr;
    }
    return properties.object.getPtr(index_name);
}

pub fn schemaVersion(schema_json: []const u8) !u32 {
    if (schema_json.len == 0) return 0;
    var parsed = try std.json.parseFromSlice(std.json.Value, std.heap.page_allocator, schema_json, .{});
    defer parsed.deinit();

    const root = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidSchemaUpdateRequest,
    };
    const version_value = root.get("version") orelse return 0;
    return switch (version_value) {
        .integer => |value| std.math.cast(u32, value) orelse error.InvalidSchemaUpdateRequest,
        else => error.InvalidSchemaUpdateRequest,
    };
}

pub fn documentSchemasChanged(alloc: std.mem.Allocator, current_schema_json: []const u8, next_schema_json: []const u8) !bool {
    const current = try extractCanonicalObjectField(alloc, current_schema_json, "document_schemas");
    defer if (current) |value| alloc.free(value);
    const next = try extractCanonicalObjectField(alloc, next_schema_json, "document_schemas");
    defer if (next) |value| alloc.free(value);

    if (current == null and next == null) return false;
    if (current == null or next == null) return true;
    return !std.mem.eql(u8, current.?, next.?);
}

fn extractCanonicalObjectField(alloc: std.mem.Allocator, schema_json: []const u8, field_name: []const u8) !?[]u8 {
    if (schema_json.len == 0) return null;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, schema_json, .{});
    defer parsed.deinit();

    const root = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidSchemaUpdateRequest,
    };
    const value = root.get(field_name) orelse return null;
    return try stringifyJsonValue(alloc, value);
}

pub fn normalizeSchemaVersion(alloc: std.mem.Allocator, schema_json: []const u8, version: u32) ![]u8 {
    const source = if (schema_json.len > 0) schema_json else "{}";
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, source, .{});
    defer parsed.deinit();

    const root = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidSchemaUpdateRequest,
    };

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.append(alloc, '{');
    try appendJsonString(alloc, &out, "version");
    try out.append(alloc, ':');
    const encoded_version = try std.fmt.allocPrint(alloc, "{d}", .{version});
    defer alloc.free(encoded_version);
    try out.appendSlice(alloc, encoded_version);

    const relational_storage = blk: {
        const storage_mode = root.get("storage_mode") orelse break :blk false;
        break :blk storage_mode == .string and std.mem.eql(u8, storage_mode.string, "relational");
    };
    if (relational_storage) {
        try out.append(alloc, ',');
        try appendJsonString(alloc, &out, "enforce_types");
        try out.appendSlice(alloc, ":true");
    }

    var it = root.iterator();
    while (it.next()) |entry| {
        if (std.mem.eql(u8, entry.key_ptr.*, "version")) continue;
        if (relational_storage and std.mem.eql(u8, entry.key_ptr.*, "enforce_types")) continue;
        try out.append(alloc, ',');
        try appendJsonString(alloc, &out, entry.key_ptr.*);
        try out.append(alloc, ':');
        const encoded = try stringifyJsonValue(alloc, entry.value_ptr.*);
        defer alloc.free(encoded);
        try out.appendSlice(alloc, encoded);
    }

    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

fn appendJsonString(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), value: []const u8) !void {
    const escaped = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})});
    defer alloc.free(escaped);
    try out.appendSlice(alloc, escaped);
}

fn stringifyJsonValue(alloc: std.mem.Allocator, value: std.json.Value) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})});
}
