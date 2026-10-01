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
pub const metadata_table_manager = @import("../metadata/local_catalog.zig");
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

pub fn applySchemaUpdateRecordWithIncarnation(
    alloc: std.mem.Allocator,
    table: *const metadata_table_manager.TableRecord,
    schema_json: []const u8,
    incarnation: u64,
) !metadata_table_manager.TableRecord {
    if (!@import("../storage/coverage_identity.zig").isValid(incarnation)) return error.InvalidIndexConfig;
    return applySchemaRecord(alloc, table, schema_json, false, false, incarnation);
}

pub fn foreignKeyPublicationIndexesValid(alloc: std.mem.Allocator, table_name: []const u8, before_indexes_json: []const u8, after_indexes_json: []const u8, after_schema_json: []const u8, next_version: u32) !bool {
    if (next_version == 0) return false;
    var candidate = try std.json.parseFromSlice(std.json.Value, alloc, after_indexes_json, .{});
    defer candidate.deinit();
    if (candidate.value != .object) return false;
    const next_name = try std.fmt.allocPrint(alloc, "full_text_index_v{d}", .{next_version});
    defer alloc.free(next_name);
    const incarnation = if (candidate.value.object.get(next_name)) |value|
        coverage_policy_mod.incarnation(value) orelse return false
    else
        null;
    if (incarnation) |next_incarnation| {
        var prior = try std.json.parseFromSlice(std.json.Value, alloc, before_indexes_json, .{});
        defer prior.deinit();
        if (prior.value != .object) return false;
        var entries = prior.value.object.iterator();
        while (entries.next()) |entry| {
            if (coverage_policy_mod.incarnation(entry.value_ptr.*)) |old_incarnation|
                if (old_incarnation == next_incarnation) return false;
        }
    }
    const regenerated = try regenerateAlgebraicIndexesFromSchemaAlloc(alloc, table_name, before_indexes_json, after_schema_json);
    defer alloc.free(regenerated);
    const expected = try upsertVersionedFullTextIndex(alloc, regenerated, next_version - 1, next_version, incarnation);
    defer alloc.free(expected);
    return std.mem.eql(u8, expected, after_indexes_json);
}

pub fn applySchemaRecord(alloc: std.mem.Allocator, table: *const metadata_table_manager.TableRecord, schema_json: []const u8, rewrite: bool, fk_publication: bool, pinned_incarnation: ?u64) !metadata_table_manager.TableRecord {
    try @import("../schema/relational_index_namespace.zig").validate(alloc, schema_json, table.indexes_json);
    const current_version = try schemaVersion(table.schema_json);
    const schema_changed = !try schemasSemanticallyEqual(alloc, table.schema_json, schema_json);
    if (schema_changed and !rewrite) {
        try @import("../schema/relational_expression.zig").validateSchemaUpdate(alloc, table.schema_json, schema_json);
        if (!fk_publication and !try foreignKeyDefinitionsUnchanged(alloc, table.schema_json, schema_json))
            return error.ForeignKeyGenerationPublicationRequired;
    }
    const next_version = if (schema_changed)
        std.math.add(u32, current_version, 1) catch return error.SchemaVersionExhausted
    else
        current_version;

    // Validate and compare before cloning the catalog record so rejected
    // updates do not duplicate potentially large schema/index metadata.
    var updated = try metadata_table_manager.cloneTable(alloc, table.*);
    errdefer metadata_table_manager.freeTable(alloc, updated);

    const normalized_schema_json = try normalizeSchemaVersion(alloc, schema_json, next_version);
    var normalized_schema_json_owned = true;
    errdefer if (normalized_schema_json_owned) alloc.free(normalized_schema_json);
    // schemasSemanticallyEqual derives both runtime schemas, so the candidate
    // has already passed the same validation. Avoid parsing and deriving it a
    // second time on the mutation path.
    alloc.free(updated.schema_json);
    updated.schema_json = normalized_schema_json;
    normalized_schema_json_owned = false;

    // Refresh schema-derived algebraic configs (dynamic_field_rules + capability
    // fingerprint) on every accepted schema update so the algebraic sidecar
    // tracks dynamic templates without a recreate.
    const refreshed_indexes_json = try regenerateAlgebraicIndexesFromSchemaAlloc(alloc, table.name, updated.indexes_json, updated.schema_json);
    alloc.free(updated.indexes_json);
    updated.indexes_json = refreshed_indexes_json;

    if (!schema_changed) return updated;

    if (table.read_schema_json.len == 0) {
        const normalized_read_schema_json = if (table.schema_json.len > 0)
            try normalizeSchemaVersion(alloc, table.schema_json, current_version)
        else
            try normalizeSchemaVersion(alloc, "{}", 0);
        alloc.free(updated.read_schema_json);
        updated.read_schema_json = normalized_read_schema_json;
    }

    const next_indexes_json = try upsertVersionedFullTextIndex(alloc, updated.indexes_json, current_version, next_version, pinned_incarnation);
    alloc.free(updated.indexes_json);
    updated.indexes_json = next_indexes_json;
    return updated;
}

pub fn foreignKeyDefinitionsUnchanged(alloc: std.mem.Allocator, previous_json: []const u8, next_json: []const u8) !bool {
    const schema_api = @import("../schema/mod.zig");
    const native = @import("../storage/schema.zig");
    const declarations = @import("../schema/relational_declarations.zig");
    var scratch = std.heap.ArenaAllocator.init(alloc);
    defer scratch.deinit();
    const a = scratch.allocator();
    var previous = try schema_api.parseValidatedTableSchema(a, if (previous_json.len == 0) "{}" else previous_json);
    defer previous.deinit(a);
    var next = try schema_api.parseValidatedTableSchema(a, if (next_json.len == 0) "{}" else next_json);
    defer next.deinit(a);
    const previous_fk_count = if (previous.foreign_keys) |list| list.value.len else 0;
    const next_fk_count = if (next.foreign_keys) |list| list.value.len else 0;
    if (previous_fk_count == 0 and next_fk_count == 0) return true;
    const previous_runtime = try schema_api.deriveRuntimeTableSchema(a, previous);
    defer native.freeSchema(a, previous_runtime);
    const next_runtime = try schema_api.deriveRuntimeTableSchema(a, next);
    defer native.freeSchema(a, next_runtime);
    const prior = try declarations.definitionFingerprints(a, previous, previous_runtime);
    defer declarations.freeDefinitions(a, prior);
    const candidate = try declarations.definitionFingerprints(a, next, next_runtime);
    defer declarations.freeDefinitions(a, candidate);
    var prior_count: usize = 0;
    var next_count: usize = 0;
    for (prior) |definition| {
        if (definition.kind != .foreign_key) continue;
        prior_count += 1;
        const matching = for (candidate) |other| {
            if (other.kind == .foreign_key and std.mem.eql(u8, definition.name, other.name)) break other;
        } else return false;
        if (!std.mem.eql(u8, &definition.fingerprint, &matching.fingerprint)) return false;
    }
    for (candidate) |definition| if (definition.kind == .foreign_key) {
        next_count += 1;
    };
    return prior_count == next_count;
}

pub fn regenerateAlgebraicIndexesFromSchemaAlloc(
    alloc: std.mem.Allocator,
    table_name: []const u8,
    indexes_json: []const u8,
    schema_json: []const u8,
) ![]u8 {
    if (indexes_json.len == 0) return try alloc.dupe(u8, indexes_json);
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, indexes_json, .{});
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
        const value = if (isAlgebraicIndexValue(entry.value_ptr.*)) blk: {
            if (schema_json.len == 0) return error.InvalidSchemaUpdateRequest;
            changed = true;
            break :blk try regenerateAlgebraicIndexValueAlloc(arena, table_name, schema_json, entry.value_ptr.*);
        } else try cloneJsonValueAlloc(arena, entry.value_ptr.*);
        try object.put(arena, try arena.dupe(u8, entry.key_ptr.*), value);
    }
    if (!changed) return try alloc.dupe(u8, indexes_json);
    return try std.json.Stringify.valueAlloc(alloc, std.json.Value{ .object = object }, .{ .emit_null_optional_fields = false });
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

pub fn regenerateAlgebraicIndexValueAlloc(
    alloc: std.mem.Allocator,
    table_name: []const u8,
    schema_json: []const u8,
    source: std.json.Value,
) !std.json.Value {
    const config_json = try algebraic_mod.schema_capability.configJsonFromSchemaJsonAlloc(alloc, table_name, schema_json);
    defer alloc.free(config_json);
    var derived = try parseJsonValueAlloc(alloc, config_json);
    if (derived != .object) return error.InvalidSchemaUpdateRequest;
    try derived.object.put(alloc, try alloc.dupe(u8, "type"), .{ .string = try alloc.dupe(u8, "algebraic") });

    // Schema-derived fields stay authoritative; only carry forward user knobs.
    var it = source.object.iterator();
    while (it.next()) |entry| {
        if (!isAlgebraicUserTunableField(entry.key_ptr.*)) continue;
        try derived.object.put(
            alloc,
            try alloc.dupe(u8, entry.key_ptr.*),
            try cloneJsonValueAlloc(alloc, entry.value_ptr.*),
        );
    }

    var source_config = try std.json.parseFromValue(algebraic_mod.index.Config, alloc, source, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = true,
    });
    defer source_config.deinit();
    var derived_config = try std.json.parseFromValue(algebraic_mod.index.Config, alloc, derived, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = true,
    });
    defer derived_config.deinit();
    algebraic_mod.index.validateConfig(derived_config.value) catch return error.InvalidSchemaUpdateRequest;
    const delta = algebraic_mod.index.schemaCapabilityDelta(source_config.value, derived_config.value);

    // Static projection changes can leave existing docfacts encoded under the
    // old type/layout. Keep the whole algebraic planner fail-closed until a
    // generation rebuild proves coverage. Do not erase an existing non-ready
    // lifecycle on an otherwise identical schema refresh.
    const source_status = source_config.value.capability_lifecycle_status;
    if (delta.static_fields_changed or !algebraic_mod.index.capabilityLifecycleStatusReady(source_status)) {
        const status = if (delta.static_fields_changed) "rebuild_required" else source_status;
        try derived.object.put(alloc, try alloc.dupe(u8, "capability_lifecycle_status"), .{
            .string = try alloc.dupe(u8, status),
        });
    }

    // Dynamic rules are gated independently so unchanged static fields remain
    // available while only the dynamic projection is awaiting a rebuild. Use
    // structural rule comparison rather than fingerprint presence; legacy
    // configs without a fingerprint must fail closed too.
    const has_dynamic_rules = blk: {
        const rules = derived.object.get("dynamic_field_rules") orelse break :blk false;
        break :blk rules == .array and rules.array.items.len > 0;
    };
    if (has_dynamic_rules and (delta.dynamic_rules_changed or source_config.value.dynamic_rules_backfill_pending)) {
        try derived.object.put(alloc, try alloc.dupe(u8, "dynamic_rules_backfill_pending"), .{ .bool = true });
    }
    return derived;
}

pub fn normalizeSchemaVersion(alloc: std.mem.Allocator, schema_json: []const u8, version: u32) ![]u8 {
    const source = if (schema_json.len > 0) schema_json else "{}";
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, source, .{ .parse_numbers = false });
    defer parsed.deinit();
    try @import("relational_expression_contract.zig").canonicalizeSchemaValue(parsed.arena.allocator(), &parsed.value);

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

    var it = root.iterator();
    while (it.next()) |entry| {
        if (std.mem.eql(u8, entry.key_ptr.*, "version")) continue;
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

pub fn isAlgebraicIndexValue(value: std.json.Value) bool {
    if (value != .object) return false;
    const type_value = value.object.get("type") orelse return false;
    return type_value == .string and std.mem.eql(u8, type_value.string, "algebraic");
}

pub fn appendJsonString(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), value: []const u8) !void {
    const escaped = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})});
    defer alloc.free(escaped);
    try out.appendSlice(alloc, escaped);
}

pub fn upsertVersionedFullTextIndex(
    alloc: std.mem.Allocator,
    current_indexes_json: []const u8,
    current_version: u32,
    next_version: u32,
    pinned_incarnation: ?u64,
) ![]u8 {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, current_indexes_json, .{});
    defer parsed.deinit();

    const root = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidTableIndexMetadata,
    };

    const next_name = try std.fmt.allocPrint(alloc, "full_text_index_v{d}", .{next_version});
    defer alloc.free(next_name);

    // A table may contain any number of named full-text indexes with artifact
    // or field-selective sources. Only the index selected for the current read
    // schema is the primary document index and may seed the next schema
    // version. Picking the first full-text config silently promotes an
    // unrelated named index when catalog insertion order changes.
    const active_name = try selectFullTextIndexNameForVersion(alloc, current_indexes_json, current_version);
    defer if (active_name) |name| alloc.free(name);
    const active_config = if (active_name) |name| root.get(name) else null;

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.append(alloc, '{');

    var first = true;
    var it = root.iterator();
    while (it.next()) |entry| {
        if (std.mem.eql(u8, entry.key_ptr.*, next_name)) continue;

        if (!first) try out.append(alloc, ',');
        first = false;
        try appendJsonString(alloc, &out, entry.key_ptr.*);
        try out.append(alloc, ':');
        const encoded = try stringifyJsonValue(alloc, entry.value_ptr.*);
        defer alloc.free(encoded);
        try out.appendSlice(alloc, encoded);
    }

    if (active_config) |config| {
        var next_config = try buildCanonicalIndexConfigValue(alloc, next_name, config);
        defer deinitJsonValue(alloc, &next_config);
        // A versioned index is a distinct desired incarnation. Retaining the
        // previous private token would let stale runtime observations satisfy
        // readiness for the newly built index.
        removeOwnedJsonObjectField(alloc, &next_config.object, coverage_policy_mod.incarnation_field);
        removeOwnedJsonObjectField(alloc, &next_config.object, coverage_policy_mod.legacy_coverage_incarnation_field);
        const encoded_next_config = if (pinned_incarnation) |incarnation|
            try coverage_policy_mod.withIncarnationAlloc(alloc, next_config, incarnation)
        else
            try coverage_policy_mod.withFreshIncarnationAlloc(alloc, next_config);
        defer alloc.free(encoded_next_config);

        if (!first) try out.append(alloc, ',');
        try appendJsonString(alloc, &out, next_name);
        try out.append(alloc, ':');
        try out.appendSlice(alloc, encoded_next_config);
    }

    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

pub fn buildCanonicalIndexConfigValue(
    alloc: std.mem.Allocator,
    index_name: []const u8,
    config: std.json.Value,
) !std.json.Value {
    if (config != .object) return error.InvalidTableIndexMetadata;
    const index_type = inferIndexType(index_name, config) orelse return error.InvalidTableIndexMetadata;

    var object = std.json.ObjectMap.empty;
    errdefer {
        var value: std.json.Value = .{ .object = object };
        deinitJsonValue(alloc, &value);
    }

    try object.put(alloc, try alloc.dupe(u8, "name"), .{ .string = try alloc.dupe(u8, index_name) });
    if (config.object.get("type") == null) {
        try object.put(alloc, try alloc.dupe(u8, "type"), .{ .string = try alloc.dupe(u8, switch (index_type) {
            .full_text => "full_text",
            .embeddings => "embeddings",
            .graph => "graph",
            .algebraic => "algebraic",
        }) });
    }

    var it = config.object.iterator();
    while (it.next()) |entry| {
        if (std.mem.eql(u8, entry.key_ptr.*, "name")) continue;
        try object.put(alloc, try alloc.dupe(u8, entry.key_ptr.*), try cloneJsonValueAlloc(alloc, entry.value_ptr.*));
    }
    return .{ .object = object };
}

pub fn schemasSemanticallyEqual(alloc: std.mem.Allocator, current_schema_json: []const u8, next_schema_json: []const u8) !bool {
    // Version is backend-owned, so derive both schemas at the same synthetic
    // generation and compare their canonical runtime behavior. This treats
    // omitted fields and their explicit defaults as equal, ignores object key
    // order, and still preserves meaningful ordering such as template rules.
    const current_normalized = try normalizeSchemaVersion(alloc, current_schema_json, 0);
    defer alloc.free(current_normalized);
    const next_normalized = try normalizeSchemaVersion(alloc, next_schema_json, 0);
    defer alloc.free(next_normalized);

    const next_runtime_json = try compileRuntimeSchemaJson(alloc, next_normalized);
    defer alloc.free(next_runtime_json);
    const current_runtime_json = compileRuntimeSchemaJson(alloc, current_normalized) catch |err| switch (err) {
        // Let a valid update repair catalog state written by an older schema
        // implementation. Allocation failure remains operational and must not
        // be mistaken for a semantic difference.
        error.OutOfMemory => return err,
        else => return false,
    };
    defer alloc.free(current_runtime_json);

    if (!try sourceSchemasSemanticallyEqual(alloc, current_normalized, next_normalized)) return false;

    var current = try json_helpers.parseJsonValueAlloc(alloc, current_runtime_json);
    defer current.deinit();
    var next = try json_helpers.parseJsonValueAlloc(alloc, next_runtime_json);
    defer next.deinit();

    const current_canonical = try canonicalSchemaJsonAlloc(alloc, current.value, null, .runtime);
    defer alloc.free(current_canonical);
    const next_canonical = try canonicalSchemaJsonAlloc(alloc, next.value, null, .runtime);
    defer alloc.free(next_canonical);
    return std.mem.eql(u8, current_canonical, next_canonical);
}

pub fn inferIndexType(index_name: []const u8, config: std.json.Value) ?ApiIndexType {
    if (config != .object) return null;
    if (config.object.get("type")) |type_value| {
        if (type_value != .string) return null;
        if (std.mem.eql(u8, type_value.string, "full_text")) return .full_text;
        if (std.mem.eql(u8, type_value.string, "embeddings")) return .embeddings;
        if (std.mem.eql(u8, type_value.string, "graph")) return .graph;
        if (std.mem.eql(u8, type_value.string, "algebraic")) return .algebraic;
        return null;
    }
    if (std.mem.eql(u8, index_name, default_full_text_index_name)) return .full_text;
    if (std.mem.startsWith(u8, index_name, "full_text_index_v")) return .full_text;
    if (std.mem.eql(u8, index_name, "default")) return .full_text;
    return null;
}

pub fn isAlgebraicUserTunableField(field: []const u8) bool {
    const tunable = [_][]const u8{
        "adaptive",
        "pathfact_policy",
        "max_result_buckets",
        "max_planner_scan_rows",
        "max_batch_accumulator_entries",
        "max_cardinality_cache_bytes",
        "max_hll_contributions_per_document",
        "max_hll_contribution_bytes_per_document",
        "max_distributed_hll_partial_bytes",
        "max_hll_maintenance_rows_per_tick",
        "max_pending_hll_observation_entries",
        "max_pending_hll_observation_bytes",
        "hll_cardinalities",
        "min_max_candidate_cache_size",
        "enable_temporal_range_pruning",
    };
    for (tunable) |name| {
        if (std.mem.eql(u8, field, name)) return true;
    }
    return false;
}

pub fn selectFullTextIndexNameForVersion(
    alloc: std.mem.Allocator,
    indexes_json: []const u8,
    version: u32,
) !?[]u8 {
    return try full_text_indexes.selectFullTextIndexNameForVersionAlloc(alloc, indexes_json, version);
}

pub fn deinitJsonValue(alloc: std.mem.Allocator, value: *std.json.Value) void {
    json_helpers.deinitJsonValue(alloc, value);
    value.* = .null;
}

pub fn removeOwnedJsonObjectField(
    alloc: std.mem.Allocator,
    object: *std.json.ObjectMap,
    field: []const u8,
) void {
    if (object.fetchOrderedRemove(field)) |removed| {
        alloc.free(@constCast(removed.key));
        var removed_value = removed.value;
        deinitJsonValue(alloc, &removed_value);
    }
}

pub fn compileRuntimeSchemaJson(alloc: std.mem.Allocator, schema_json: []const u8) ![]u8 {
    var parsed_schema = try schema_mod.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try schema_mod.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer runtime_schema_mod.freeSchema(alloc, runtime_schema);

    return try runtimeSchemaJsonAlloc(alloc, runtime_schema);
}

pub fn stringifyJsonValue(alloc: std.mem.Allocator, value: std.json.Value) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})});
}

pub fn sourceSchemasSemanticallyEqual(
    alloc: std.mem.Allocator,
    current_schema_json: []const u8,
    next_schema_json: []const u8,
) !bool {
    var current = try json_helpers.parseJsonValueAlloc(alloc, current_schema_json);
    defer current.deinit();
    var next = try json_helpers.parseJsonValueAlloc(alloc, next_schema_json);
    defer next.deinit();
    normalizeSourceSchemaTopLevelDefaults(&current.value);
    normalizeSourceSchemaTopLevelDefaults(&next.value);

    const current_canonical = try canonicalSchemaJsonAlloc(alloc, current.value, null, .source);
    defer alloc.free(current_canonical);
    const next_canonical = try canonicalSchemaJsonAlloc(alloc, next.value, null, .source);
    defer alloc.free(next_canonical);
    return std.mem.eql(u8, current_canonical, next_canonical);
}

pub fn normalizeSourceSchemaTopLevelDefaults(value: *std.json.Value) void {
    if (value.* != .object) return;
    const object = &value.object;
    const removable = [_][]const u8{
        "default_type",
        "ttl_duration_ns",
        "ttl_field",
        "enforce_types",
        "document_schemas",
        "dynamic_templates",
        "index_sort",
    };
    for (removable) |name| {
        const field = object.get(name) orelse continue;
        const is_default = if (field == .null)
            true
        else if (std.mem.eql(u8, name, "default_type"))
            field == .string and field.string.len == 0
        else if (std.mem.eql(u8, name, "ttl_duration_ns"))
            field == .integer and field.integer == 0
        else if (std.mem.eql(u8, name, "ttl_field"))
            field == .string and std.mem.eql(u8, field.string, "_timestamp")
        else if (std.mem.eql(u8, name, "enforce_types"))
            field == .bool and !field.bool
        else if (std.mem.eql(u8, name, "document_schemas"))
            field == .object and field.object.count() == 0
        else
            field == .array and field.array.items.len == 0;
        if (is_default) _ = object.orderedRemove(name);
    }
}

pub fn runtimeSchemaJsonAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema_mod.TableSchema,
) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try appendRuntimeSchemaObject(alloc, &out, schema);
    return try out.toOwnedSlice(alloc);
}

pub fn canonicalSchemaJsonAlloc(
    alloc: std.mem.Allocator,
    value: std.json.Value,
    field_name: ?[]const u8,
    kind: CanonicalSchemaKind,
) std.mem.Allocator.Error![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendCanonicalSchemaJson(alloc, &out, value, field_name, kind);
    return try out.toOwnedSlice(alloc);
}

pub fn appendCanonicalSchemaJson(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    value: std.json.Value,
    field_name: ?[]const u8,
    kind: CanonicalSchemaKind,
) std.mem.Allocator.Error!void {
    switch (value) {
        .object => |object| {
            const keys = try alloc.alloc([]const u8, object.count());
            defer alloc.free(keys);
            var key_index: usize = 0;
            var it = object.iterator();
            while (it.next()) |entry| : (key_index += 1) keys[key_index] = entry.key_ptr.*;
            std.mem.sort([]const u8, keys, {}, struct {
                fn lessThan(_: void, lhs: []const u8, rhs: []const u8) bool {
                    return std.mem.order(u8, lhs, rhs) == .lt;
                }
            }.lessThan);

            try out.append(alloc, '{');
            for (keys, 0..) |key, index| {
                if (index > 0) try out.append(alloc, ',');
                try appendJsonString(alloc, out, key);
                try out.append(alloc, ':');
                try appendCanonicalSchemaJson(alloc, out, object.get(key).?, key, kind);
            }
            try out.append(alloc, '}');
        },
        .array => |array| {
            try out.append(alloc, '[');
            if (schemaArrayIsUnordered(kind, field_name)) {
                // Canonicalize and sort once instead of performing quadratic
                // pairwise matching for schemas with many derived fields.
                const items = try alloc.alloc([]u8, array.items.len);
                var initialized: usize = 0;
                defer {
                    for (items[0..initialized]) |item| alloc.free(item);
                    alloc.free(items);
                }
                for (array.items) |item| {
                    items[initialized] = try canonicalSchemaJsonAlloc(alloc, item, null, kind);
                    initialized += 1;
                }
                std.mem.sort([]u8, items, {}, struct {
                    fn lessThan(_: void, lhs: []const u8, rhs: []const u8) bool {
                        return std.mem.order(u8, lhs, rhs) == .lt;
                    }
                }.lessThan);
                for (items, 0..) |item, index| {
                    if (index > 0) try out.append(alloc, ',');
                    try out.appendSlice(alloc, item);
                }
            } else {
                for (array.items, 0..) |item, index| {
                    if (index > 0) try out.append(alloc, ',');
                    try appendCanonicalSchemaJson(alloc, out, item, null, kind);
                }
            }
            try out.append(alloc, ']');
        },
        else => {
            const encoded = try stringifyJsonValue(alloc, value);
            defer alloc.free(encoded);
            try out.appendSlice(alloc, encoded);
        },
    }
}

pub fn appendRuntimeSchemaObject(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    schema: runtime_schema_mod.TableSchema,
) !void {
    try out.append(alloc, '{');
    try appendJsonString(alloc, out, "version");
    try out.append(alloc, ':');
    const version_text = try std.fmt.allocPrint(alloc, "{d}", .{schema.version});
    defer alloc.free(version_text);
    try out.appendSlice(alloc, version_text);
    try out.appendSlice(alloc, ",\"default_type\":");
    try appendJsonString(alloc, out, schema.default_type);
    try out.appendSlice(alloc, ",\"ttl_field\":");
    try appendJsonString(alloc, out, schema.ttl_field);
    try out.appendSlice(alloc, ",\"ttl_duration_ns\":");
    const ttl_text = try std.fmt.allocPrint(alloc, "{d}", .{schema.ttl_duration_ns});
    defer alloc.free(ttl_text);
    try out.appendSlice(alloc, ttl_text);
    try out.appendSlice(alloc, ",\"enforce_types\":");
    try out.appendSlice(alloc, if (schema.enforce_types) "true" else "false");
    try out.appendSlice(alloc, ",\"index_sort\":[");
    for (schema.index_sort, 0..) |field, i| {
        if (i > 0) try out.append(alloc, ',');
        try out.append(alloc, '{');
        try appendJsonString(alloc, out, "field");
        try out.append(alloc, ':');
        try appendJsonString(alloc, out, field.field);
        try out.appendSlice(alloc, ",\"order\":");
        try appendJsonString(alloc, out, if (field.desc) "desc" else "asc");
        try out.append(alloc, '}');
    }
    try out.appendSlice(alloc, "],\"dynamic_templates\":[");
    for (schema.dynamic_templates, 0..) |tmpl, i| {
        if (i > 0) try out.append(alloc, ',');
        try out.append(alloc, '{');
        try appendJsonString(alloc, out, "name");
        try out.append(alloc, ':');
        try appendJsonString(alloc, out, tmpl.name);
        if (tmpl.match_pattern) |value| {
            try out.appendSlice(alloc, ",\"match\":");
            try appendJsonString(alloc, out, value);
        }
        if (tmpl.unmatch_pattern) |value| {
            try out.appendSlice(alloc, ",\"unmatch\":");
            try appendJsonString(alloc, out, value);
        }
        if (tmpl.path_match) |value| {
            try out.appendSlice(alloc, ",\"path_match\":");
            try appendJsonString(alloc, out, value);
        }
        if (tmpl.path_unmatch) |value| {
            try out.appendSlice(alloc, ",\"path_unmatch\":");
            try appendJsonString(alloc, out, value);
        }
        if (tmpl.match_mapping_type) |value| {
            try out.appendSlice(alloc, ",\"match_mapping_type\":");
            try appendJsonString(alloc, out, value);
        }
        try out.appendSlice(alloc, ",\"mapping\":{");
        try appendJsonString(alloc, out, "type");
        try out.append(alloc, ':');
        try appendJsonString(alloc, out, antflyTypeName(tmpl.mapping.field_type));
        try out.appendSlice(alloc, ",\"index\":");
        try out.appendSlice(alloc, if (tmpl.mapping.do_index) "true" else "false");
        try out.appendSlice(alloc, ",\"store\":");
        try out.appendSlice(alloc, if (tmpl.mapping.store) "true" else "false");
        try out.appendSlice(alloc, ",\"sortable\":");
        try out.appendSlice(alloc, if (tmpl.mapping.sortable) "true" else "false");
        try out.appendSlice(alloc, ",\"missing_null_policy\":");
        try appendJsonString(alloc, out, runtime_schema_mod.missingNullPolicyName(tmpl.mapping.missing_null_policy));
        try out.appendSlice(alloc, ",\"include_in_all\":");
        try out.appendSlice(alloc, if (tmpl.mapping.include_in_all) "true" else "false");
        try out.appendSlice(alloc, ",\"analyzer\":");
        try appendJsonString(alloc, out, tmpl.mapping.analyzer);
        try out.appendSlice(alloc, "}}");
    }
    try out.appendSlice(alloc, "],\"exact_fields\":[");
    for (schema.exact_fields, 0..) |field, i| {
        if (i > 0) try out.append(alloc, ',');
        try out.append(alloc, '{');
        try appendJsonString(alloc, out, "source_field");
        try out.append(alloc, ':');
        try appendJsonString(alloc, out, field.source_field);
        try out.append(alloc, ',');
        try appendJsonString(alloc, out, "field");
        try out.append(alloc, ':');
        try appendJsonString(alloc, out, field.field);
        try out.appendSlice(alloc, ",\"mapping\":{");
        try appendJsonString(alloc, out, "type");
        try out.append(alloc, ':');
        try appendJsonString(alloc, out, antflyTypeName(field.mapping.field_type));
        try out.appendSlice(alloc, ",\"index\":");
        try out.appendSlice(alloc, if (field.mapping.do_index) "true" else "false");
        try out.appendSlice(alloc, ",\"store\":");
        try out.appendSlice(alloc, if (field.mapping.store) "true" else "false");
        try out.appendSlice(alloc, ",\"doc_values\":");
        try out.appendSlice(alloc, if (field.mapping.doc_values) "true" else "false");
        try out.appendSlice(alloc, ",\"sortable\":");
        try out.appendSlice(alloc, if (field.mapping.sortable) "true" else "false");
        try out.appendSlice(alloc, ",\"missing_null_policy\":");
        try appendJsonString(alloc, out, runtime_schema_mod.missingNullPolicyName(field.mapping.missing_null_policy));
        try out.appendSlice(alloc, ",\"include_in_all\":");
        try out.appendSlice(alloc, if (field.mapping.include_in_all) "true" else "false");
        try out.appendSlice(alloc, ",\"analyzer\":");
        try appendJsonString(alloc, out, field.mapping.analyzer);
        try out.appendSlice(alloc, "}}");
    }
    try out.appendSlice(alloc, "],\"declared_fields\":[");
    for (schema.declared_fields, 0..) |field, i| {
        if (i > 0) try out.append(alloc, ',');
        try out.append(alloc, '{');
        try appendJsonString(alloc, out, "field");
        try out.append(alloc, ':');
        try appendJsonString(alloc, out, field.field);
        try out.appendSlice(alloc, ",\"mapping\":{");
        try appendJsonString(alloc, out, "type");
        try out.append(alloc, ':');
        try appendJsonString(alloc, out, antflyTypeName(field.mapping.field_type));
        try out.appendSlice(alloc, ",\"index\":");
        try out.appendSlice(alloc, if (field.mapping.do_index) "true" else "false");
        try out.appendSlice(alloc, ",\"store\":");
        try out.appendSlice(alloc, if (field.mapping.store) "true" else "false");
        try out.appendSlice(alloc, ",\"sortable\":");
        try out.appendSlice(alloc, if (field.mapping.sortable) "true" else "false");
        try out.appendSlice(alloc, ",\"missing_null_policy\":");
        try appendJsonString(alloc, out, runtime_schema_mod.missingNullPolicyName(field.mapping.missing_null_policy));
        try out.appendSlice(alloc, ",\"analyzer\":");
        try appendJsonString(alloc, out, field.mapping.analyzer);
        try out.appendSlice(alloc, "}}");
    }
    try out.appendSlice(alloc, "],\"field_capabilities\":[");
    try appendRuntimeFieldCapabilities(alloc, out, schema);
    try out.appendSlice(alloc, "],\"full_text_documents\":[");
    for (schema.full_text_documents, 0..) |doc, doc_idx| {
        if (doc_idx > 0) try out.append(alloc, ',');
        try out.append(alloc, '{');
        try appendJsonString(alloc, out, "name");
        try out.append(alloc, ':');
        try appendJsonString(alloc, out, doc.name);
        try out.appendSlice(alloc, ",\"fields\":[");
        for (doc.fields, 0..) |field, field_idx| {
            if (field_idx > 0) try out.append(alloc, ',');
            try out.append(alloc, '{');
            try appendJsonString(alloc, out, "path");
            try out.append(alloc, ':');
            try appendJsonString(alloc, out, field.path);
            try out.appendSlice(alloc, ",\"emitted_name\":");
            try appendJsonString(alloc, out, field.emitted_name);
            try out.appendSlice(alloc, ",\"analyzer\":");
            try appendJsonString(alloc, out, field.analyzer);
            try out.appendSlice(alloc, ",\"include_in_all\":");
            try out.appendSlice(alloc, if (field.include_in_all) "true" else "false");
            try out.append(alloc, '}');
        }
        try out.appendSlice(alloc, "],\"dynamic_rules\":[");
        for (doc.dynamic_rules, 0..) |rule, rule_idx| {
            if (rule_idx > 0) try out.append(alloc, ',');
            try out.append(alloc, '{');
            try appendJsonString(alloc, out, "parent_path");
            try out.append(alloc, ':');
            try appendJsonString(alloc, out, rule.parent_path);
            if (rule.segment_pattern) |segment_pattern| {
                try out.appendSlice(alloc, ",\"segment_pattern\":");
                try appendJsonString(alloc, out, segment_pattern);
            }
            try out.appendSlice(alloc, ",\"relative_path\":");
            try appendJsonString(alloc, out, rule.relative_path);
            try out.appendSlice(alloc, ",\"variants\":[");
            for (rule.variants, 0..) |variant, variant_idx| {
                if (variant_idx > 0) try out.append(alloc, ',');
                try out.append(alloc, '{');
                try appendJsonString(alloc, out, "suffix");
                try out.append(alloc, ':');
                try appendJsonString(alloc, out, variant.suffix);
                try out.appendSlice(alloc, ",\"analyzer\":");
                try appendJsonString(alloc, out, variant.analyzer);
                try out.appendSlice(alloc, ",\"include_in_all\":");
                try out.appendSlice(alloc, if (variant.include_in_all) "true" else "false");
                try out.append(alloc, '}');
            }
            try out.appendSlice(alloc, "]}");
        }
        try out.appendSlice(alloc, "],\"open_dynamic_paths\":[");
        for (doc.open_dynamic_paths, 0..) |path, open_idx| {
            if (open_idx > 0) try out.append(alloc, ',');
            try appendJsonString(alloc, out, path);
        }
        try out.appendSlice(alloc, "],\"infer_type_dynamic_paths\":[");
        for (doc.infer_type_dynamic_paths, 0..) |path, infer_idx| {
            if (infer_idx > 0) try out.append(alloc, ',');
            try appendJsonString(alloc, out, path);
        }
        try out.appendSlice(alloc, "],\"declared_paths\":[");
        for (doc.declared_paths, 0..) |path, declared_idx| {
            if (declared_idx > 0) try out.append(alloc, ',');
            try appendJsonString(alloc, out, path);
        }
        try out.appendSlice(alloc, "],\"unindexed_paths\":[");
        for (doc.unindexed_paths, 0..) |path, unindexed_idx| {
            if (unindexed_idx > 0) try out.append(alloc, ',');
            try appendJsonString(alloc, out, path);
        }
        try out.appendSlice(alloc, "]}");
    }
    try out.appendSlice(alloc, "]}");
}

pub fn appendRuntimeFieldCapabilities(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    schema: runtime_schema_mod.TableSchema,
) !void {
    const capabilities = try runtime_schema_mod.fieldCapabilitiesAlloc(alloc, schema);
    defer runtime_schema_mod.freeFieldCapabilities(alloc, capabilities);

    for (capabilities, 0..) |capability, i| {
        try appendRuntimeFieldCapability(alloc, out, i > 0, capability);
    }
}

pub fn schemaArrayIsUnordered(kind: CanonicalSchemaKind, field_name: ?[]const u8) bool {
    const name = field_name orelse return false;
    return switch (kind) {
        .source => std.mem.eql(u8, name, "required") or
            std.mem.eql(u8, name, "enum") or
            std.mem.eql(u8, name, "type") or
            std.mem.eql(u8, name, "x-antfly-types") or
            std.mem.eql(u8, name, "x-antfly-include-in-all") or
            std.mem.eql(u8, name, "allOf") or
            std.mem.eql(u8, name, "anyOf") or
            std.mem.eql(u8, name, "oneOf"),
        // Keep dynamic_rules ordered: the document mapper uses first-match
        // precedence, so swapping two overlapping rules changes indexing.
        .runtime => std.mem.eql(u8, name, "field_capabilities") or
            std.mem.eql(u8, name, "full_text_documents") or
            std.mem.eql(u8, name, "fields") or
            std.mem.eql(u8, name, "variants") or
            std.mem.eql(u8, name, "open_dynamic_paths") or
            std.mem.eql(u8, name, "infer_type_dynamic_paths") or
            std.mem.eql(u8, name, "declared_paths") or
            std.mem.eql(u8, name, "unindexed_paths"),
    };
}

pub fn antflyTypeName(value: runtime_schema_mod.AntflyType) []const u8 {
    return switch (value) {
        .text => "text",
        .keyword => "keyword",
        .numeric => "numeric",
        .embedding => "embedding",
        .link => "link",
        .boolean => "boolean",
        .datetime => "datetime",
        .geopoint => "geopoint",
        .geoshape => "geoshape",
        .blob => "blob",
        .html => "html",
        .search_as_you_type => "search_as_you_type",
        .substring => "substring",
    };
}

pub fn appendRuntimeFieldCapability(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    needs_comma: bool,
    capability: runtime_schema_mod.FieldCapability,
) !void {
    if (needs_comma) try out.append(alloc, ',');
    try out.append(alloc, '{');

    var field_count: usize = 0;
    try appendOptionalJsonStringField(alloc, out, "name", capability.name, &field_count);
    try appendOptionalJsonStringField(alloc, out, "field", capability.field, &field_count);
    try appendOptionalJsonStringField(alloc, out, "path_pattern", capability.path_pattern, &field_count);
    try appendOptionalJsonStringField(alloc, out, "field_pattern", capability.field_pattern, &field_count);
    try appendOptionalJsonStringField(alloc, out, "match_mapping_type", capability.match_mapping_type, &field_count);
    try appendOptionalJsonStringField(alloc, out, "emitted_name", capability.emitted_name, &field_count);
    try appendOptionalJsonStringField(alloc, out, "document_schema", capability.document_schema, &field_count);
    if (field_count > 0) try out.append(alloc, ',');
    try appendJsonString(alloc, out, "type");
    try out.append(alloc, ':');
    try appendJsonString(alloc, out, antflyTypeName(capability.field_type));
    try out.appendSlice(alloc, ",\"query_modes\":[");
    for (queryModesForFieldCapability(capability), 0..) |mode, mode_idx| {
        if (mode_idx > 0) try out.append(alloc, ',');
        try appendJsonString(alloc, out, mode);
    }
    try out.append(alloc, ']');
    try out.appendSlice(alloc, ",\"sortable\":");
    try appendJsonBool(out, alloc, capability.sortable);
    try out.appendSlice(alloc, ",\"doc_value_coverage\":");
    try appendJsonString(alloc, out, capability.doc_value_coverage);
    try out.appendSlice(alloc, ",\"provenance\":");
    try appendJsonString(alloc, out, capability.provenance);
    try out.appendSlice(alloc, ",\"missing_null_policy\":");
    try appendJsonString(alloc, out, capability.missing_null_policy);
    try out.appendSlice(alloc, ",\"queryability_state\":");
    try appendJsonString(alloc, out, capability.queryability_state);
    try out.appendSlice(alloc, ",\"sort_lifecycle_state\":");
    try appendJsonString(alloc, out, capability.sort_lifecycle_state);
    if (capability.analyzer) |analyzer| {
        try out.appendSlice(alloc, ",\"analyzer\":");
        try appendJsonString(alloc, out, analyzer);
    }
    if (capability.index_sort) |membership| {
        try out.appendSlice(alloc, ",\"index_sort_position\":");
        const text = try std.fmt.allocPrint(alloc, "{d}", .{membership.position});
        defer alloc.free(text);
        try out.appendSlice(alloc, text);
        try out.appendSlice(alloc, ",\"index_sort_order\":");
        try appendJsonString(alloc, out, if (membership.desc) "desc" else "asc");
    }
    try out.append(alloc, '}');
}

pub fn appendOptionalJsonStringField(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    name: []const u8,
    maybe_value: ?[]const u8,
    field_count: *usize,
) !void {
    const value = maybe_value orelse return;
    if (field_count.* > 0) try out.append(alloc, ',');
    field_count.* += 1;
    try appendJsonString(alloc, out, name);
    try out.append(alloc, ':');
    try appendJsonString(alloc, out, value);
}

pub fn queryModesForFieldCapability(capability: runtime_schema_mod.FieldCapability) []const []const u8 {
    return switch (capability.field_type) {
        .text, .html => if (capability.searchable) &.{"full_text"} else &.{},
        .search_as_you_type => if (capability.searchable) &.{ "full_text", "autocomplete" } else &.{"autocomplete"},
        .substring => if (capability.searchable) &.{ "full_text", "substring" } else &.{"substring"},
        .keyword, .link => if (capability.filterable) &.{"exact"} else &.{},
        .numeric, .datetime => if (capability.filterable) &.{ "exact", "range" } else &.{},
        .boolean => if (capability.filterable) &.{"exact"} else &.{},
        .geopoint, .geoshape => if (capability.filterable) &.{"geo"} else &.{},
        .embedding, .blob => &.{},
    };
}

pub fn appendJsonBool(out: *std.ArrayListUnmanaged(u8), alloc: std.mem.Allocator, value: bool) !void {
    try out.appendSlice(alloc, if (value) "true" else "false");
}

pub const runtime_schema_mod = @import("../storage/schema.zig");

pub const ApiIndexType = enum {
    full_text,
    embeddings,
    graph,
    algebraic,
};

pub const CanonicalSchemaKind = enum {
    source,
    runtime,
};
