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
const group_ids = @import("../common/group_ids.zig");
const metadata_api = @import("../metadata/api.zig");
const metadata_admin = @import("../metadata/admin.zig");
const metadata_table_manager = @import("../metadata/table_manager.zig");
const metadata_transition_state = @import("../metadata/transition_state.zig");
const raft_reconciler = @import("../raft/reconciler.zig");
const db_mod = @import("../storage/db/mod.zig");
const indexes_openapi = @import("antfly_indexes_openapi");
const metadata_openapi = @import("antfly_metadata_openapi");
const schema_openapi = @import("antfly_schema_openapi");
const schema_mod = @import("../schema/mod.zig");
const runtime_schema_mod = @import("../storage/schema.zig");
const algebraic_mod = @import("../storage/db/algebraic/mod.zig");
const full_text_indexes = @import("full_text_indexes.zig");
const json_helpers = @import("json_helpers.zig");

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
pub const TableStorageStatus = struct {
    table_name: []const u8,
    empty: bool,
};

const RuntimeSchemaDebugBinding = struct {
    index_name: []const u8,
    status: []const u8,
    schema_version: ?u32 = null,
    schema_slot: ?[]const u8 = null,
    runtime_schema: ?std.json.Value = null,
};

const RuntimeSchemaDebugSchemaEntry = struct {
    slot: []const u8,
    status: []const u8,
    @"error": ?[]const u8 = null,
    schema_version: ?u32 = null,
    runtime_schema: ?std.json.Value = null,
};

const AlgebraicCapabilityDebugEntry = struct {
    slot: []const u8,
    status: []const u8,
    @"error": ?[]const u8 = null,
    schema_version: ?u32 = null,
    capability_fingerprint: ?[]const u8 = null,
    lifecycle_status: ?[]const u8 = null,
    change_added_fields: ?u32 = null,
    change_removed_fields: ?u32 = null,
    change_changed_type_fields: ?u32 = null,
    compatible_additive: ?bool = null,
    requires_rebuild: ?bool = null,
    group_field_count: u32 = 0,
    measure_field_count: u32 = 0,
    time_field_count: u32 = 0,
    skipped_dynamic_fields: u32 = 0,
    skipped_complex_fields: u32 = 0,
    skipped_unbounded_fields: u32 = 0,
    config: ?std.json.Value = null,
};

const TableRuntimeSchemaDebug = struct {
    runtime_schemas: []const RuntimeSchemaDebugSchemaEntry,
    full_text_index_bindings: []const RuntimeSchemaDebugBinding,
    algebraic_capabilities: []const AlgebraicCapabilityDebugEntry,
};

const IndexRuntimeSchemaDebug = struct {
    binding: RuntimeSchemaDebugBinding,
};

pub const TableStatusWithRuntimeSchemaDebug = struct {
    name: []const u8,
    description: ?[]const u8 = null,
    indexes: std.json.ArrayHashMap(indexes_openapi.IndexConfig),
    shards: std.json.ArrayHashMap(metadata_openapi.ShardConfig),
    schema: ?schema_openapi.TableSchema = null,
    migration: ?metadata_openapi.TableMigration = null,
    replication_sources: ?[]const metadata_openapi.ReplicationSource = null,
    storage_status: metadata_openapi.StorageStatus,
    debug: TableRuntimeSchemaDebug,
};

pub fn encodeTableList(
    alloc: std.mem.Allocator,
    snapshot: *const metadata_api.AdminSnapshot,
    prefix: ?[]const u8,
) ![]u8 {
    return try encodeTableListWithStorageStatuses(alloc, snapshot, prefix, null);
}

pub fn encodeTableListWithStorageStatuses(
    alloc: std.mem.Allocator,
    snapshot: *const metadata_api.AdminSnapshot,
    prefix: ?[]const u8,
    storage_statuses: ?[]const TableStorageStatus,
) ![]u8 {
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const listed = try buildTableListWithStorageStatuses(arena_impl.allocator(), snapshot, prefix, storage_statuses);
    return try std.json.Stringify.valueAlloc(alloc, listed, .{ .emit_null_optional_fields = false });
}

pub fn encodeSingleTableStatus(
    alloc: std.mem.Allocator,
    snapshot: *const metadata_api.AdminSnapshot,
    table_name: []const u8,
) !?[]u8 {
    return try encodeSingleTableStatusWithStorageStatuses(alloc, snapshot, table_name, null);
}

pub fn encodeSingleTableStatusWithStorageStatuses(
    alloc: std.mem.Allocator,
    snapshot: *const metadata_api.AdminSnapshot,
    table_name: []const u8,
    storage_statuses: ?[]const TableStorageStatus,
) !?[]u8 {
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const status = (try buildSingleTableStatusWithStorageStatuses(arena_impl.allocator(), snapshot, table_name, storage_statuses)) orelse return null;
    return try std.json.Stringify.valueAlloc(alloc, status, .{ .emit_null_optional_fields = false });
}

pub fn buildTableListWithStorageStatuses(
    alloc: std.mem.Allocator,
    snapshot: *const metadata_api.AdminSnapshot,
    prefix: ?[]const u8,
    storage_statuses: ?[]const TableStorageStatus,
) ![]metadata_openapi.TableStatus {
    var count: usize = 0;
    for (snapshot.tables) |*table| {
        if (prefix) |pfx| {
            if (!std.mem.startsWith(u8, table.name, pfx)) continue;
        }
        count += 1;
    }

    const listed = try alloc.alloc(metadata_openapi.TableStatus, count);
    var index: usize = 0;
    for (snapshot.tables) |*table| {
        if (prefix) |pfx| {
            if (!std.mem.startsWith(u8, table.name, pfx)) continue;
        }
        listed[index] = try buildTableStatus(alloc, snapshot, table, findTableStorageStatus(storage_statuses, table.name), false);
        index += 1;
    }
    return listed;
}

pub fn buildSingleTableStatusWithStorageStatuses(
    alloc: std.mem.Allocator,
    snapshot: *const metadata_api.AdminSnapshot,
    table_name: []const u8,
    storage_statuses: ?[]const TableStorageStatus,
) !?metadata_openapi.TableStatus {
    const table = findTableByName(snapshot, table_name) orelse return null;
    return try buildTableStatus(alloc, snapshot, table, findTableStorageStatus(storage_statuses, table.name), true);
}

pub fn encodeSingleTableStatusWithRuntimeSchemaDebug(
    alloc: std.mem.Allocator,
    snapshot: *const metadata_api.AdminSnapshot,
    table_name: []const u8,
    storage_statuses: ?[]const TableStorageStatus,
) !?[]u8 {
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const response = (try buildSingleTableStatusWithRuntimeSchemaDebug(arena_impl.allocator(), snapshot, table_name, storage_statuses)) orelse return null;
    return try std.json.Stringify.valueAlloc(alloc, response, .{ .emit_null_optional_fields = false });
}

pub fn buildSingleTableStatusWithRuntimeSchemaDebug(
    alloc: std.mem.Allocator,
    snapshot: *const metadata_api.AdminSnapshot,
    table_name: []const u8,
    storage_statuses: ?[]const TableStorageStatus,
) !?TableStatusWithRuntimeSchemaDebug {
    const table = findTableByName(snapshot, table_name) orelse return null;
    const base = (try buildSingleTableStatusWithStorageStatuses(alloc, snapshot, table_name, storage_statuses)) orelse return null;
    const debug = try buildTableRuntimeSchemaDebug(alloc, table);
    return .{
        .name = base.name,
        .description = base.description,
        .indexes = base.indexes,
        .shards = base.shards,
        .schema = base.schema,
        .migration = base.migration,
        .replication_sources = base.replication_sources,
        .storage_status = base.storage_status,
        .debug = debug,
    };
}

pub fn encodeSingleTableIndexWithRuntimeSchemaDebug(
    alloc: std.mem.Allocator,
    snapshot: *const metadata_api.AdminSnapshot,
    table_name: []const u8,
    index_name: []const u8,
) !?[]u8 {
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const value = (try buildSingleTableIndexWithRuntimeSchemaDebugValue(arena_impl.allocator(), snapshot, table_name, index_name)) orelse return null;
    return try std.json.Stringify.valueAlloc(alloc, value, .{ .emit_null_optional_fields = false });
}

pub fn buildSingleTableIndexWithRuntimeSchemaDebugValue(
    alloc: std.mem.Allocator,
    snapshot: *const metadata_api.AdminSnapshot,
    table_name: []const u8,
    index_name: []const u8,
) !?std.json.Value {
    const table = findTableByName(snapshot, table_name) orelse return null;
    var value = (try buildSingleTableIndexValue(alloc, table, index_name)) orelse return null;
    errdefer deinitJsonValue(alloc, &value);
    if (value != .object) return error.InvalidTableIndexMetadata;
    try value.object.put(alloc, try alloc.dupe(u8, "debug"), try buildTableIndexRuntimeSchemaDebugValue(alloc, table, index_name));
    return value;
}

pub const CreateTableRequest = struct {
    num_shards: ?u32 = null,
    description: ?[]u8 = null,
    indexes_json: ?[]u8 = null,
    schema_json: ?[]u8 = null,
    replication_sources_json: ?[]u8 = null,

    pub fn deinit(self: *CreateTableRequest, alloc: std.mem.Allocator) void {
        if (self.description) |value| alloc.free(value);
        if (self.indexes_json) |value| alloc.free(value);
        if (self.schema_json) |value| alloc.free(value);
        if (self.replication_sources_json) |value| alloc.free(value);
        self.* = undefined;
    }
};

pub fn parseCreateTableRequest(alloc: std.mem.Allocator, body: []const u8) !CreateTableRequest {
    if (body.len == 0) return .{};
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{});
    defer parsed.deinit();

    const root = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidCreateTableRequest,
    };

    var req: CreateTableRequest = .{};
    errdefer req.deinit(alloc);

    if (root.get("num_shards")) |value| {
        if (value != .null) req.num_shards = try parseU32Field(value);
    }
    if (root.get("description")) |value| {
        req.description = switch (value) {
            .null => null,
            .string => |str| try alloc.dupe(u8, str),
            else => return error.InvalidCreateTableRequest,
        };
    }
    if (root.get("indexes")) |value| {
        if (value != .null)
            req.indexes_json = try stringifyJsonValue(alloc, value)
        else
            req.indexes_json = try alloc.dupe(u8, default_indexes_json);
    } else {
        req.indexes_json = try alloc.dupe(u8, default_indexes_json);
    }
    if (root.get("schema")) |value| {
        if (value != .null) {
            const encoded_schema = try stringifyJsonValue(alloc, value);
            defer alloc.free(encoded_schema);
            const validated_schema = parseSchemaUpdateRequest(alloc, encoded_schema) catch |err| switch (err) {
                error.InvalidSchemaUpdateRequest => return error.InvalidCreateTableRequest,
                else => return err,
            };
            defer alloc.free(validated_schema);
            req.schema_json = try normalizeSchemaVersion(alloc, validated_schema, 0);
        }
    }
    if (root.get("replication_sources")) |value| {
        if (value != .null) {
            const encoded_replication_sources = try stringifyJsonValue(alloc, value);
            defer alloc.free(encoded_replication_sources);
            req.replication_sources_json = try validateReplicationSourcesJson(alloc, encoded_replication_sources);
        }
    }

    if (req.num_shards) |num_shards| {
        if (num_shards == 0) return error.InvalidCreateTableRequest;
    }
    return req;
}

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

pub fn validatePublicAlgebraicIndexesJson(alloc: std.mem.Allocator, indexes_json: []const u8) !void {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, indexes_json, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidCreateTableRequest;

    var it = parsed.value.object.iterator();
    while (it.next()) |entry| {
        try validatePublicAlgebraicIndexValue(entry.value_ptr.*);
    }
}

pub fn validatePublicAlgebraicIndexJson(alloc: std.mem.Allocator, index_json: []const u8) !void {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, index_json, .{});
    defer parsed.deinit();
    try validatePublicAlgebraicIndexValue(parsed.value);
}

fn isSchemaDerivedAlgebraicIndex(value: std.json.Value) bool {
    if (value != .object) return false;
    const type_value = value.object.get("type") orelse return false;
    if (type_value != .string or !std.mem.eql(u8, type_value.string, "algebraic")) return false;
    const derive_value = value.object.get("derive_from_schema") orelse return false;
    return derive_value == .bool and derive_value.bool;
}

fn validatePublicAlgebraicIndexValue(value: std.json.Value) !void {
    if (value != .object) return;
    const type_value = value.object.get("type") orelse return;
    if (type_value != .string) return error.InvalidCreateTableRequest;
    if (!std.mem.eql(u8, type_value.string, "algebraic")) return;

    const derive_value = value.object.get("derive_from_schema") orelse return error.InvalidCreateTableRequest;
    if (derive_value != .bool or !derive_value.bool) return error.InvalidCreateTableRequest;

    var it = value.object.iterator();
    while (it.next()) |entry| {
        if (isAlgebraicInternalConfigField(entry.key_ptr.*)) return error.InvalidCreateTableRequest;
    }
}

fn schemaDerivedAlgebraicIndexValueAlloc(
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

fn isAlgebraicInternalConfigField(field: []const u8) bool {
    const internal_fields = [_][]const u8{
        "materializations",
        "group_fields",
        "measure_fields",
        "time_fields",
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

pub fn deriveTableRecord(table_name: []const u8, req: CreateTableRequest) metadata_table_manager.TableRecord {
    const min_ranges = req.num_shards orelse 1;
    return .{
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

pub fn deriveInitialRange(table: metadata_table_manager.TableRecord) metadata_table_manager.RangeRecord {
    const group_id = deriveDataGroupId(table.name, 0x47525031);
    return .{
        .group_id = group_id,
        .range_id = group_id,
        .table_id = table.table_id,
        .start_key = "",
        .end_key = null,
    };
}

pub fn deriveInitialRanges(
    alloc: std.mem.Allocator,
    table: metadata_table_manager.TableRecord,
) ![]metadata_table_manager.RangeRecord {
    if (table.min_ranges <= 1) {
        const initial_range = deriveInitialRange(table);
        const out = try alloc.alloc(metadata_table_manager.RangeRecord, 1);
        out[0] = .{
            .group_id = initial_range.group_id,
            .range_id = initial_range.range_id,
            .table_id = table.table_id,
            .start_key = try alloc.dupe(u8, ""),
            .end_key = null,
        };
        return out;
    }

    const shard_count = table.min_ranges;
    const out = try alloc.alloc(metadata_table_manager.RangeRecord, shard_count);

    var i: u32 = 0;
    errdefer {
        var cleanup_index: u32 = 0;
        while (cleanup_index < i) : (cleanup_index += 1) {
            metadata_table_manager.freeRange(alloc, out[cleanup_index]);
        }
        alloc.free(out);
    }
    while (i < shard_count) : (i += 1) {
        const start_key = if (i == 0)
            try alloc.dupe(u8, "")
        else
            try deriveShardBoundaryKey(alloc, i, shard_count);
        errdefer alloc.free(start_key);

        const end_key = if (i + 1 == shard_count)
            null
        else
            try deriveShardBoundaryKey(alloc, i + 1, shard_count);
        errdefer if (end_key) |value| alloc.free(value);

        const group_id = deriveShardGroupId(table.name, i);
        out[i] = .{
            .group_id = group_id,
            .range_id = group_id,
            .table_id = table.table_id,
            .start_key = start_key,
            .end_key = end_key,
        };
    }

    return out;
}

pub fn parseSchemaUpdateRequest(alloc: std.mem.Allocator, body: []const u8) ![]u8 {
    return try schema_mod.parseSchemaUpdateRequest(alloc, body);
}

pub fn parseValidatedTableSchema(alloc: std.mem.Allocator, schema_json: []const u8) !ParsedTableSchema {
    return try schema_mod.parseValidatedTableSchema(alloc, schema_json);
}

pub fn validateBatchWritesAgainstTableSchema(
    alloc: std.mem.Allocator,
    schema: ParsedTableSchema,
    writes: []const db_mod.types.BatchWrite,
) !void {
    try schema_mod.validateBatchWritesAgainstTableSchema(alloc, schema, writes);
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

pub fn applySchemaUpdateRecord(
    alloc: std.mem.Allocator,
    table: *const metadata_table_manager.TableRecord,
    schema_json: []const u8,
) !metadata_table_manager.TableRecord {
    var updated = try metadata_table_manager.cloneTable(alloc, table.*);
    errdefer metadata_table_manager.freeTable(alloc, updated);

    const current_version = try schemaVersion(table.schema_json);
    const doc_schemas_changed = try documentSchemasChanged(alloc, table.schema_json, schema_json);
    const next_version = if (doc_schemas_changed) current_version + 1 else current_version;

    const normalized_schema_json = try normalizeSchemaVersion(alloc, schema_json, next_version);
    alloc.free(updated.schema_json);
    updated.schema_json = normalized_schema_json;

    if (!doc_schemas_changed) return updated;

    if (table.read_schema_json.len == 0) {
        const normalized_read_schema_json = if (table.schema_json.len > 0)
            try normalizeSchemaVersion(alloc, table.schema_json, current_version)
        else
            try normalizeSchemaVersion(alloc, "{}", 0);
        alloc.free(updated.read_schema_json);
        updated.read_schema_json = normalized_read_schema_json;
    }

    const next_indexes_json = try upsertVersionedFullTextIndex(alloc, table.indexes_json, current_version, next_version);
    alloc.free(updated.indexes_json);
    updated.indexes_json = next_indexes_json;
    return updated;
}

pub fn routeQueryRequestToActiveReadIndex(
    alloc: std.mem.Allocator,
    table: *const metadata_table_manager.TableRecord,
    req: *db_mod.types.SearchRequest,
) !void {
    if (!queryNeedsPrimaryTextIndex(req.*)) return;

    const active_name = (try selectActiveFullTextIndexName(alloc, table)) orelse return;
    errdefer alloc.free(active_name);

    if (req.primary_text_index_name == null) {
        req.primary_text_index_name = try alloc.dupe(u8, active_name);
    }
    if (req.index_name == null) {
        req.index_name = active_name;
    } else {
        alloc.free(active_name);
    }
}

fn buildTableStatus(
    alloc: std.mem.Allocator,
    snapshot: *const metadata_api.AdminSnapshot,
    table: *const metadata_table_manager.TableRecord,
    storage_status: ?TableStorageStatus,
    include_replication_runtime: bool,
) !metadata_openapi.TableStatus {
    const ranges = try metadata_admin.listTableRanges(alloc, snapshot, table.table_id);
    defer metadata_admin.freeRangeRefs(alloc, ranges);

    var shards = std.json.ArrayHashMap(metadata_openapi.ShardConfig){};
    for (ranges) |range_ref| {
        const key = try std.fmt.allocPrint(alloc, "{d}", .{range_ref.group_id});
        const byte_range = try alloc.alloc([]const u8, 2);
        byte_range[0] = range_ref.start_key;
        byte_range[1] = range_ref.end_key orelse "";
        try shards.map.put(alloc, key, .{ .byte_range = byte_range });
    }

    const empty = if (storage_status) |status| status.empty else ranges.len == 0;
    return .{
        .name = table.name,
        .description = if (table.description.len > 0) table.description else null,
        .indexes = try parseTableIndexes(alloc, table.indexes_json),
        .shards = shards,
        .schema = try parseOptionalTableSchema(alloc, table.schema_json),
        .migration = if (table.read_schema_json.len > 0) .{
            .state = "rebuilding",
            .read_schema = try parseTableSchema(alloc, table.read_schema_json),
        } else null,
        .replication_sources = try parseReplicationSources(alloc, snapshot, table, include_replication_runtime),
        .storage_status = .{
            .disk_usage = 0,
            .empty = empty,
        },
    };
}

fn parseOptionalTableSchema(alloc: std.mem.Allocator, schema_json: []const u8) !?schema_openapi.TableSchema {
    if (schema_json.len == 0) return null;
    return try parseTableSchema(alloc, schema_json);
}

fn parseTableSchema(alloc: std.mem.Allocator, schema_json: []const u8) !schema_openapi.TableSchema {
    return try std.json.parseFromSliceLeaky(schema_openapi.TableSchema, alloc, schema_json, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = true,
    });
}

fn parseTableIndexes(
    alloc: std.mem.Allocator,
    indexes_json: []const u8,
) !std.json.ArrayHashMap(indexes_openapi.IndexConfig) {
    const canonical_json = try encodeTableIndexesObject(alloc, indexes_json);
    defer alloc.free(canonical_json);
    return try std.json.parseFromSliceLeaky(std.json.ArrayHashMap(indexes_openapi.IndexConfig), alloc, canonical_json, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = true,
    });
}

fn parseReplicationSources(
    alloc: std.mem.Allocator,
    snapshot: *const metadata_api.AdminSnapshot,
    table: *const metadata_table_manager.TableRecord,
    include_replication_runtime: bool,
) ![]const metadata_openapi.ReplicationSource {
    const raw = if (include_replication_runtime)
        try encodeTableReplicationSourcesAlloc(alloc, snapshot, table)
    else
        table.replication_sources_json;
    return try std.json.parseFromSliceLeaky([]metadata_openapi.ReplicationSource, alloc, raw, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = true,
    });
}

fn encodeTableReplicationSourcesAlloc(
    alloc: std.mem.Allocator,
    snapshot: *const metadata_api.AdminSnapshot,
    table: *const metadata_table_manager.TableRecord,
) ![]u8 {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, table.replication_sources_json, .{});
    defer parsed.deinit();
    if (parsed.value != .array) return try alloc.dupe(u8, table.replication_sources_json);

    for (parsed.value.array.items, 0..) |*item, source_ordinal| {
        if (item.* != .object) continue;
        if (findReplicationSourceStatus(snapshot, table.table_id, @intCast(source_ordinal))) |status| {
            try item.object.put(alloc, "status", try replicationSourceStatusJsonValueAlloc(alloc, status.*));
        }
        if (findReplicationSourceActionHint(snapshot, table.table_id, @intCast(source_ordinal))) |hint| {
            try item.object.put(alloc, "action_hint", try replicationSourceActionHintJsonValueAlloc(alloc, hint.*));
        }
    }
    return try std.json.Stringify.valueAlloc(alloc, parsed.value, .{});
}

fn replicationSourceStatusJsonValueAlloc(
    alloc: std.mem.Allocator,
    status: metadata_table_manager.ReplicationSourceStatusRecord,
) !std.json.Value {
    var object = std.json.ObjectMap.empty;
    errdefer {
        var it = object.iterator();
        while (it.next()) |entry| {
            alloc.free(@constCast(entry.key_ptr.*));
            deinitJsonValue(alloc, entry.value_ptr);
        }
        object.deinit(alloc);
    }
    try putJsonStringField(alloc, &object, "source_kind", status.source_kind);
    try putJsonStringField(alloc, &object, "external_table", status.external_table);
    try putJsonStringField(alloc, &object, "cutover_mode", status.cutover_mode);
    try putJsonStringField(alloc, &object, "slot_name", status.slot_name);
    try putJsonStringField(alloc, &object, "publication_name", status.publication_name);
    try putJsonStringField(alloc, &object, "phase", status.phase);
    try putJsonStringField(alloc, &object, "checkpoint", status.checkpoint);
    try putJsonIntegerField(alloc, &object, "snapshot_offset", status.snapshot_offset);
    try putJsonStringField(alloc, &object, "prepared_checkpoint", status.prepared_checkpoint);
    try putJsonStringField(alloc, &object, "stream_checkpoint", status.stream_checkpoint);
    try putJsonStringField(alloc, &object, "last_error", status.last_error);
    try putJsonStringField(alloc, &object, "failure_class", status.failure_class);
    try putJsonIntegerField(alloc, &object, "lag_records", status.lag_records);
    try putJsonIntegerField(alloc, &object, "lag_millis", status.lag_millis);
    try putJsonIntegerField(alloc, &object, "consecutive_failures", status.consecutive_failures);
    try putJsonIntegerField(alloc, &object, "last_source_commit_at_ms", status.last_source_commit_at_ms);
    try putJsonIntegerField(alloc, &object, "last_success_at_ms", status.last_success_at_ms);
    try putJsonIntegerField(alloc, &object, "last_change_applied_at_ms", status.last_change_applied_at_ms);
    try putJsonIntegerField(alloc, &object, "updated_at_ms", status.updated_at_ms);
    return .{ .object = object };
}

fn replicationSourceActionHintJsonValueAlloc(
    alloc: std.mem.Allocator,
    hint: metadata_api.ReplicationSourceActionHint,
) !std.json.Value {
    var object = std.json.ObjectMap.empty;
    errdefer {
        var it = object.iterator();
        while (it.next()) |entry| {
            alloc.free(@constCast(entry.key_ptr.*));
            deinitJsonValue(alloc, entry.value_ptr);
        }
        object.deinit(alloc);
    }
    try putJsonStringField(alloc, &object, "action", hint.action);
    try putJsonStringField(alloc, &object, "reason", hint.reason);
    try putJsonStringField(alloc, &object, "reseed_exact_cutover_path", hint.reseed_exact_cutover_path);
    return .{ .object = object };
}

fn putJsonStringField(
    alloc: std.mem.Allocator,
    object: *std.json.ObjectMap,
    key: []const u8,
    value: []const u8,
) !void {
    try object.put(alloc, try alloc.dupe(u8, key), .{ .string = try alloc.dupe(u8, value) });
}

fn putJsonIntegerField(
    alloc: std.mem.Allocator,
    object: *std.json.ObjectMap,
    key: []const u8,
    value: u64,
) !void {
    try object.put(alloc, try alloc.dupe(u8, key), .{ .integer = @intCast(value) });
}

fn findReplicationSourceStatus(
    snapshot: *const metadata_api.AdminSnapshot,
    table_id: u64,
    source_ordinal: u32,
) ?*const metadata_table_manager.ReplicationSourceStatusRecord {
    for (snapshot.replication_source_statuses) |*status| {
        if (status.table_id == table_id and status.source_ordinal == source_ordinal) return status;
    }
    return null;
}

fn findReplicationSourceActionHint(
    snapshot: *const metadata_api.AdminSnapshot,
    table_id: u64,
    source_ordinal: u32,
) ?*const metadata_api.ReplicationSourceActionHint {
    for (snapshot.replication_source_action_hints) |*hint| {
        if (hint.table_id == table_id and hint.source_ordinal == source_ordinal) return hint;
    }
    return null;
}

fn findTableStorageStatus(
    storage_statuses: ?[]const TableStorageStatus,
    table_name: []const u8,
) ?TableStorageStatus {
    const items = storage_statuses orelse return null;
    for (items) |status| {
        if (std.mem.eql(u8, status.table_name, table_name)) return status;
    }
    return null;
}

fn appendJsonString(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), value: []const u8) !void {
    const escaped = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})});
    defer alloc.free(escaped);
    try out.appendSlice(alloc, escaped);
}

fn deinitJsonValue(alloc: std.mem.Allocator, value: *std.json.Value) void {
    json_helpers.deinitJsonValue(alloc, value);
    value.* = .null;
}

fn stringifyJsonValue(alloc: std.mem.Allocator, value: std.json.Value) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})});
}

fn encodeTableIndexesObject(alloc: std.mem.Allocator, indexes_json: []const u8) ![]u8 {
    const source = if (indexes_json.len > 0) indexes_json else default_indexes_json;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, source, .{});
    defer parsed.deinit();
    const root = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidTableIndexMetadata,
    };

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.append(alloc, '{');
    var first = true;
    var it = root.iterator();
    while (it.next()) |entry| {
        if (!first) try out.append(alloc, ',');
        first = false;
        try appendJsonString(alloc, &out, entry.key_ptr.*);
        try out.append(alloc, ':');
        try appendCanonicalIndexConfig(alloc, &out, entry.key_ptr.*, entry.value_ptr.*);
    }
    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

fn encodeSingleTableIndex(
    alloc: std.mem.Allocator,
    table: *const metadata_table_manager.TableRecord,
    index_name: []const u8,
) !?[]u8 {
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const value = (try buildSingleTableIndexValue(arena_impl.allocator(), table, index_name)) orelse return null;
    return try std.json.Stringify.valueAlloc(alloc, value, .{});
}

fn buildSingleTableIndexValue(
    alloc: std.mem.Allocator,
    table: *const metadata_table_manager.TableRecord,
    index_name: []const u8,
) !?std.json.Value {
    const source = if (table.indexes_json.len > 0) table.indexes_json else default_indexes_json;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, source, .{});
    defer parsed.deinit();
    const root = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidTableIndexMetadata,
    };
    const config = root.get(index_name) orelse return null;
    return try buildCanonicalIndexConfigValue(alloc, index_name, config);
}

const ApiIndexType = enum {
    full_text,
    embeddings,
    graph,
    algebraic,
};

fn appendCanonicalIndexConfig(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    index_name: []const u8,
    config: std.json.Value,
) !void {
    if (config != .object) return error.InvalidTableIndexMetadata;
    const index_type = inferIndexType(index_name, config) orelse return error.InvalidTableIndexMetadata;

    try out.append(alloc, '{');
    try appendJsonString(alloc, out, "name");
    try out.append(alloc, ':');
    try appendJsonString(alloc, out, index_name);
    if (config.object.get("type") == null) {
        try out.append(alloc, ',');
        try appendJsonString(alloc, out, "type");
        try out.append(alloc, ':');
        try appendJsonString(alloc, out, switch (index_type) {
            .full_text => "full_text",
            .embeddings => "embeddings",
            .graph => "graph",
            .algebraic => "algebraic",
        });
    }

    var it = config.object.iterator();
    while (it.next()) |entry| {
        if (std.mem.eql(u8, entry.key_ptr.*, "name")) continue;
        try out.append(alloc, ',');
        try appendJsonString(alloc, out, entry.key_ptr.*);
        try out.append(alloc, ':');
        const encoded = try stringifyJsonValue(alloc, entry.value_ptr.*);
        defer alloc.free(encoded);
        try out.appendSlice(alloc, encoded);
    }
    try out.append(alloc, '}');
}

fn buildCanonicalIndexConfigValue(
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

fn inferIndexType(index_name: []const u8, config: std.json.Value) ?ApiIndexType {
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

fn parseJsonValueAlloc(alloc: std.mem.Allocator, body: []const u8) !std.json.Value {
    return try json_helpers.parseOwnedJsonValueAllocAlways(alloc, body);
}

fn cloneJsonValueAlloc(alloc: std.mem.Allocator, value: std.json.Value) !std.json.Value {
    return try json_helpers.cloneJsonValue(alloc, value);
}

fn buildTableRuntimeSchemaDebug(
    alloc: std.mem.Allocator,
    table: *const metadata_table_manager.TableRecord,
) !TableRuntimeSchemaDebug {
    const runtime_schemas = try alloc.alloc(RuntimeSchemaDebugSchemaEntry, 2);
    runtime_schemas[0] = try buildTableSchemaDebugEntry(alloc, "active", table.schema_json);
    runtime_schemas[1] = try buildTableSchemaDebugEntry(alloc, "read", table.read_schema_json);
    const algebraic_capabilities = try alloc.alloc(AlgebraicCapabilityDebugEntry, 2);
    algebraic_capabilities[0] = try buildAlgebraicCapabilityDebugEntry(alloc, "active", table.name, table.schema_json);
    algebraic_capabilities[1] = try buildAlgebraicCapabilityDebugEntry(alloc, "read", table.name, table.read_schema_json);
    try annotateAlgebraicCapabilityLifecycle(alloc, table.schema_json, table.read_schema_json, algebraic_capabilities);
    return .{
        .runtime_schemas = runtime_schemas,
        .full_text_index_bindings = try buildFullTextIndexBindings(alloc, table),
        .algebraic_capabilities = algebraic_capabilities,
    };
}

fn encodeTableRuntimeSchemaDebug(
    alloc: std.mem.Allocator,
    table: *const metadata_table_manager.TableRecord,
) ![]u8 {
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const debug = try buildTableRuntimeSchemaDebug(arena_impl.allocator(), table);
    return try std.json.Stringify.valueAlloc(alloc, debug, .{ .emit_null_optional_fields = false });
}

fn buildTableIndexRuntimeSchemaDebug(
    alloc: std.mem.Allocator,
    table: *const metadata_table_manager.TableRecord,
    index_name: []const u8,
) !IndexRuntimeSchemaDebug {
    return .{
        .binding = try buildSingleIndexBinding(alloc, table, index_name),
    };
}

pub fn buildTableIndexRuntimeSchemaDebugValue(
    alloc: std.mem.Allocator,
    table: *const metadata_table_manager.TableRecord,
    index_name: []const u8,
) !std.json.Value {
    const debug = try buildTableIndexRuntimeSchemaDebug(alloc, table, index_name);
    var object = std.json.ObjectMap.empty;
    errdefer {
        var value: std.json.Value = .{ .object = object };
        deinitJsonValue(alloc, &value);
    }
    try object.put(alloc, try alloc.dupe(u8, "binding"), try jsonValueFromRuntimeSchemaDebugBinding(alloc, debug.binding));
    return .{ .object = object };
}

fn encodeTableIndexRuntimeSchemaDebug(
    alloc: std.mem.Allocator,
    table: *const metadata_table_manager.TableRecord,
    index_name: []const u8,
) ![]u8 {
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const debug = try buildTableIndexRuntimeSchemaDebug(arena_impl.allocator(), table, index_name);
    return try std.json.Stringify.valueAlloc(alloc, debug, .{ .emit_null_optional_fields = false });
}

fn jsonValueFromRuntimeSchemaDebugBinding(
    alloc: std.mem.Allocator,
    binding: RuntimeSchemaDebugBinding,
) !std.json.Value {
    var object = std.json.ObjectMap.empty;
    errdefer {
        var value: std.json.Value = .{ .object = object };
        deinitJsonValue(alloc, &value);
    }
    try object.put(alloc, try alloc.dupe(u8, "index_name"), .{ .string = try alloc.dupe(u8, binding.index_name) });
    try object.put(alloc, try alloc.dupe(u8, "status"), .{ .string = try alloc.dupe(u8, binding.status) });
    if (binding.schema_version) |schema_version| {
        try object.put(alloc, try alloc.dupe(u8, "schema_version"), .{ .integer = schema_version });
    }
    if (binding.schema_slot) |schema_slot| {
        try object.put(alloc, try alloc.dupe(u8, "schema_slot"), .{ .string = try alloc.dupe(u8, schema_slot) });
    }
    if (binding.runtime_schema) |runtime_schema| {
        try object.put(alloc, try alloc.dupe(u8, "runtime_schema"), try cloneJsonValueAlloc(alloc, runtime_schema));
    }
    return .{ .object = object };
}

fn buildTableSchemaDebugEntry(
    alloc: std.mem.Allocator,
    slot: []const u8,
    schema_json: []const u8,
) !RuntimeSchemaDebugSchemaEntry {
    if (schema_json.len == 0) {
        return .{
            .slot = slot,
            .status = "missing",
        };
    }

    var parsed_schema = schema_mod.parseValidatedTableSchema(alloc, schema_json) catch |err| {
        return .{
            .slot = slot,
            .status = "error",
            .@"error" = @errorName(err),
        };
    };
    defer parsed_schema.deinit(alloc);

    const runtime_schema = schema_mod.deriveRuntimeTableSchema(alloc, parsed_schema) catch |err| {
        return .{
            .slot = slot,
            .status = "error",
            .@"error" = @errorName(err),
        };
    };
    defer runtime_schema_mod.freeSchema(alloc, runtime_schema);

    const runtime_schema_json = try runtimeSchemaJsonAlloc(alloc, runtime_schema);
    defer alloc.free(runtime_schema_json);
    return .{
        .slot = slot,
        .status = "ok",
        .schema_version = runtime_schema.version,
        .runtime_schema = try parseJsonValueAlloc(alloc, runtime_schema_json),
    };
}

fn buildAlgebraicCapabilityDebugEntry(
    alloc: std.mem.Allocator,
    slot: []const u8,
    table_name: []const u8,
    schema_json: []const u8,
) !AlgebraicCapabilityDebugEntry {
    if (schema_json.len == 0) {
        return .{
            .slot = slot,
            .status = "missing",
        };
    }

    var parsed_schema = schema_mod.parseValidatedTableSchema(alloc, schema_json) catch |err| {
        return .{
            .slot = slot,
            .status = "error",
            .@"error" = @errorName(err),
        };
    };
    defer parsed_schema.deinit(alloc);

    var plan = algebraic_mod.schema_capability.compilePlanAlloc(alloc, parsed_schema) catch |err| {
        return .{
            .slot = slot,
            .status = "error",
            .@"error" = @errorName(err),
        };
    };
    defer plan.deinit(alloc);
    const fingerprint = algebraic_mod.schema_capability.capabilityFingerprintAlloc(alloc, plan) catch |err| {
        return .{
            .slot = slot,
            .status = "error",
            .@"error" = @errorName(err),
        };
    };

    const config_json = algebraic_mod.schema_capability.configJsonFromPlanAlloc(alloc, table_name, plan) catch |err| {
        alloc.free(fingerprint);
        return .{
            .slot = slot,
            .status = "error",
            .@"error" = @errorName(err),
        };
    };
    defer alloc.free(config_json);

    return .{
        .slot = slot,
        .status = "ok",
        .schema_version = plan.schema_version,
        .capability_fingerprint = fingerprint,
        .lifecycle_status = "current",
        .group_field_count = countAlgebraicRole(plan, .group),
        .measure_field_count = countAlgebraicRole(plan, .measure),
        .time_field_count = countAlgebraicRole(plan, .time),
        .skipped_dynamic_fields = plan.skipped_dynamic_fields,
        .skipped_complex_fields = plan.skipped_complex_fields,
        .skipped_unbounded_fields = plan.skipped_unbounded_fields,
        .config = try parseJsonValueAlloc(alloc, config_json),
    };
}

fn annotateAlgebraicCapabilityLifecycle(
    alloc: std.mem.Allocator,
    active_schema_json: []const u8,
    read_schema_json: []const u8,
    entries: []AlgebraicCapabilityDebugEntry,
) !void {
    if (entries.len < 2 or active_schema_json.len == 0 or read_schema_json.len == 0) return;
    if (!std.mem.eql(u8, entries[0].status, "ok") or !std.mem.eql(u8, entries[1].status, "ok")) return;

    var active_schema = try schema_mod.parseValidatedTableSchema(alloc, active_schema_json);
    defer active_schema.deinit(alloc);
    var read_schema = try schema_mod.parseValidatedTableSchema(alloc, read_schema_json);
    defer read_schema.deinit(alloc);
    var active_plan = try algebraic_mod.schema_capability.compilePlanAlloc(alloc, active_schema);
    defer active_plan.deinit(alloc);
    var read_plan = try algebraic_mod.schema_capability.compilePlanAlloc(alloc, read_schema);
    defer read_plan.deinit(alloc);

    const impact = algebraic_mod.schema_capability.classifyChange(active_plan, read_plan);
    entries[1].change_added_fields = impact.added_fields;
    entries[1].change_removed_fields = impact.removed_fields;
    entries[1].change_changed_type_fields = impact.changed_type_fields;
    entries[1].compatible_additive = impact.compatible_additive;
    entries[1].requires_rebuild = impact.requires_rebuild;
    entries[1].lifecycle_status = if (impact.requires_rebuild)
        "rebuild_required"
    else if (impact.added_fields > 0)
        "compatible_additive"
    else
        "current";
}

fn countAlgebraicRole(
    plan: algebraic_mod.schema_capability.Plan,
    role: algebraic_mod.schema_capability.FieldRole,
) u32 {
    var count: u32 = 0;
    for (plan.fields) |field| {
        if (field.role == role) count += 1;
    }
    return count;
}

fn buildFullTextIndexBindings(
    alloc: std.mem.Allocator,
    table: *const metadata_table_manager.TableRecord,
) ![]RuntimeSchemaDebugBinding {
    const source = if (table.indexes_json.len > 0) table.indexes_json else default_indexes_json;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, source, .{});
    defer parsed.deinit();
    const root = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidTableIndexMetadata,
    };

    var count: usize = 0;
    var it = root.iterator();
    while (it.next()) |entry| {
        if (!isFullTextIndexConfig(entry.value_ptr.*)) continue;
        count += 1;
    }
    const bindings = try alloc.alloc(RuntimeSchemaDebugBinding, count);
    it = root.iterator();
    var index: usize = 0;
    while (it.next()) |entry| {
        if (!isFullTextIndexConfig(entry.value_ptr.*)) continue;
        bindings[index] = try buildSingleIndexBinding(alloc, table, entry.key_ptr.*);
        index += 1;
    }
    return bindings;
}

fn buildSingleIndexBinding(
    alloc: std.mem.Allocator,
    table: *const metadata_table_manager.TableRecord,
    index_name: []const u8,
) !RuntimeSchemaDebugBinding {
    const binding = try resolveFullTextIndexBinding(alloc, table, index_name);
    defer if (binding.runtime_schema_json) |value| alloc.free(value);
    return .{
        .index_name = index_name,
        .status = binding.status,
        .schema_version = binding.schema_version,
        .schema_slot = binding.schema_slot,
        .runtime_schema = if (binding.runtime_schema_json) |runtime_schema_json|
            try parseJsonValueAlloc(alloc, runtime_schema_json)
        else
            null,
    };
}

const FullTextIndexBinding = struct {
    status: []const u8,
    schema_version: ?u32 = null,
    schema_slot: ?[]const u8 = null,
    runtime_schema_json: ?[]u8 = null,
};

fn resolveFullTextIndexBinding(
    alloc: std.mem.Allocator,
    table: *const metadata_table_manager.TableRecord,
    index_name: []const u8,
) !FullTextIndexBinding {
    const source = if (table.indexes_json.len > 0) table.indexes_json else default_indexes_json;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, source, .{});
    defer parsed.deinit();
    const root = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidTableIndexMetadata,
    };
    const config = root.get(index_name) orelse return .{ .status = "missing_index" };
    if (!isFullTextIndexConfig(config)) return .{ .status = "not_full_text" };

    const schema_version = fullTextSchemaVersionForIndex(alloc, table, index_name) orelse {
        return .{ .status = "unavailable" };
    };
    const compiled = try compileRuntimeSchemaJsonForVersion(alloc, table, schema_version);
    if (compiled) |entry| {
        return .{
            .status = "ok",
            .schema_version = schema_version,
            .schema_slot = entry.slot,
            .runtime_schema_json = entry.runtime_schema_json,
        };
    }
    return .{
        .status = "unavailable",
        .schema_version = schema_version,
    };
}

fn fullTextSchemaVersionForIndex(
    alloc: std.mem.Allocator,
    table: *const metadata_table_manager.TableRecord,
    index_name: []const u8,
) ?u32 {
    if (std.mem.eql(u8, index_name, "default") or std.mem.eql(u8, index_name, "full_text_index")) return 0;
    const prefix = "full_text_index_v";
    if (std.mem.startsWith(u8, index_name, prefix)) {
        return std.fmt.parseInt(u32, index_name[prefix.len..], 10) catch null;
    }

    const active_name = full_text_indexes.selectActiveFullTextIndexNameAlloc(
        alloc,
        table.schema_json,
        table.read_schema_json,
        table.indexes_json,
    ) catch return null;
    defer if (active_name) |value| alloc.free(value);
    if (active_name == null or !std.mem.eql(u8, active_name.?, index_name)) return null;
    return if (table.read_schema_json.len > 0)
        schemaVersion(table.read_schema_json) catch null
    else
        schemaVersion(table.schema_json) catch null;
}

const CompiledRuntimeSchemaJson = struct {
    slot: []const u8,
    runtime_schema_json: []u8,
};

fn compileRuntimeSchemaJsonForVersion(
    alloc: std.mem.Allocator,
    table: *const metadata_table_manager.TableRecord,
    version: u32,
) !?CompiledRuntimeSchemaJson {
    if (table.schema_json.len > 0) {
        const active_version = schemaVersion(table.schema_json) catch null;
        if (active_version != null and active_version.? == version) {
            const runtime_schema_json = try compileRuntimeSchemaJson(alloc, table.schema_json);
            return .{ .slot = "active", .runtime_schema_json = runtime_schema_json };
        }
    }
    if (table.read_schema_json.len > 0) {
        const read_version = schemaVersion(table.read_schema_json) catch null;
        if (read_version != null and read_version.? == version) {
            const runtime_schema_json = try compileRuntimeSchemaJson(alloc, table.read_schema_json);
            return .{ .slot = "read", .runtime_schema_json = runtime_schema_json };
        }
    }
    return null;
}

fn compileRuntimeSchemaJson(alloc: std.mem.Allocator, schema_json: []const u8) ![]u8 {
    var parsed_schema = try schema_mod.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try schema_mod.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer runtime_schema_mod.freeSchema(alloc, runtime_schema);

    return try runtimeSchemaJsonAlloc(alloc, runtime_schema);
}

fn runtimeSchemaJsonAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema_mod.TableSchema,
) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try appendRuntimeSchemaObject(alloc, &out, schema);
    return try out.toOwnedSlice(alloc);
}

fn appendRuntimeSchemaObject(
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
    try out.appendSlice(alloc, ",\"dynamic_templates\":[");
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
        try out.appendSlice(alloc, ",\"doc_values\":");
        try out.appendSlice(alloc, if (tmpl.mapping.doc_values) "true" else "false");
        try out.appendSlice(alloc, ",\"include_in_all\":");
        try out.appendSlice(alloc, if (tmpl.mapping.include_in_all) "true" else "false");
        try out.appendSlice(alloc, ",\"analyzer\":");
        try appendJsonString(alloc, out, tmpl.mapping.analyzer);
        try out.appendSlice(alloc, "}}");
    }
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
        try out.appendSlice(alloc, "]}");
    }
    try out.appendSlice(alloc, "]}");
}

fn antflyTypeName(value: runtime_schema_mod.AntflyType) []const u8 {
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
    };
}

fn queryNeedsPrimaryTextIndex(req: db_mod.types.SearchRequest) bool {
    if (req.full_text != null) return true;
    if (req.full_text_queries.len > 0) return false;

    return switch (req.query) {
        .match_none,
        .match_all,
        .phrase,
        .multi_phrase,
        .term,
        .fuzzy,
        .numeric_range,
        .date_range,
        .doc_id,
        .bool_field,
        .geo_distance,
        .geo_bbox,
        .term_range,
        .ip_range,
        .geo_shape,
        .match,
        .match_phrase,
        .prefix,
        .wildcard,
        .regexp,
        => true,
        else => false,
    };
}

fn schemaVersion(schema_json: []const u8) !u32 {
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

fn documentSchemasChanged(alloc: std.mem.Allocator, current_schema_json: []const u8, next_schema_json: []const u8) !bool {
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

fn normalizeSchemaVersion(alloc: std.mem.Allocator, schema_json: []const u8, version: u32) ![]u8 {
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

fn upsertVersionedFullTextIndex(
    alloc: std.mem.Allocator,
    current_indexes_json: []const u8,
    current_version: u32,
    next_version: u32,
) ![]u8 {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, current_indexes_json, .{});
    defer parsed.deinit();

    const root = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidTableIndexMetadata,
    };

    const stale_name = try std.fmt.allocPrint(alloc, "full_text_index_v{d}", .{current_version});
    defer alloc.free(stale_name);
    const next_name = try std.fmt.allocPrint(alloc, "full_text_index_v{d}", .{next_version});
    defer alloc.free(next_name);

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.append(alloc, '{');

    var full_text_config: ?std.json.Value = null;
    var first = true;
    var it = root.iterator();
    while (it.next()) |entry| {
        if (full_text_config == null and isFullTextIndexConfig(entry.value_ptr.*)) {
            full_text_config = entry.value_ptr.*;
        }
        if (std.mem.eql(u8, entry.key_ptr.*, next_name)) continue;

        if (!first) try out.append(alloc, ',');
        first = false;
        try appendJsonString(alloc, &out, entry.key_ptr.*);
        try out.append(alloc, ':');
        const encoded = try stringifyJsonValue(alloc, entry.value_ptr.*);
        defer alloc.free(encoded);
        try out.appendSlice(alloc, encoded);
    }

    if (full_text_config) |config| {
        if (!first) try out.append(alloc, ',');
        try appendJsonString(alloc, &out, next_name);
        try out.append(alloc, ':');
        try appendCanonicalIndexConfig(alloc, &out, next_name, config);
    }

    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

fn selectActiveFullTextIndexName(
    alloc: std.mem.Allocator,
    table: *const metadata_table_manager.TableRecord,
) !?[]u8 {
    return try full_text_indexes.selectActiveFullTextIndexNameAlloc(
        alloc,
        table.schema_json,
        table.read_schema_json,
        table.indexes_json,
    );
}

fn selectFullTextIndexNameForVersion(
    alloc: std.mem.Allocator,
    indexes_json: []const u8,
    version: u32,
) !?[]u8 {
    return try full_text_indexes.selectFullTextIndexNameForVersionAlloc(alloc, indexes_json, version);
}

fn isFullTextIndexConfig(value: std.json.Value) bool {
    return full_text_indexes.isFullTextIndexConfig(value);
}

fn validateReplicationSourcesJson(alloc: std.mem.Allocator, replication_sources_json: []const u8) ![]u8 {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, replication_sources_json, .{});
    defer parsed.deinit();
    const items = switch (parsed.value) {
        .array => |array| array.items,
        else => return error.InvalidCreateTableRequest,
    };
    for (items) |item| {
        const object = switch (item) {
            .object => |object| object,
            else => return error.InvalidCreateTableRequest,
        };
        try requireStringField(object, "type");
        try requireStringField(object, "dsn");
        try requireStringField(object, "postgres_table");
        if (object.get("key_template")) |value| if (value != .string) return error.InvalidCreateTableRequest;
        if (object.get("slot_name")) |value| if (value != .string) return error.InvalidCreateTableRequest;
        if (object.get("publication_name")) |value| if (value != .string) return error.InvalidCreateTableRequest;
        if (object.get("require_exact_cutover")) |value| if (value != .bool) return error.InvalidCreateTableRequest;
    }
    return try alloc.dupe(u8, replication_sources_json);
}

fn requireStringField(object: std.json.ObjectMap, field_name: []const u8) !void {
    const value = object.get(field_name) orelse return error.InvalidCreateTableRequest;
    if (value != .string) return error.InvalidCreateTableRequest;
}

fn parseU32Field(value: std.json.Value) !u32 {
    return switch (value) {
        .integer => |int_value| std.math.cast(u32, int_value) orelse error.InvalidCreateTableRequest,
        else => error.InvalidCreateTableRequest,
    };
}

pub fn findTableByName(snapshot: *const metadata_api.AdminSnapshot, table_name: []const u8) ?*const metadata_table_manager.TableRecord {
    for (snapshot.tables) |*record| {
        if (std.mem.eql(u8, record.name, table_name)) return record;
    }
    return null;
}

fn deriveId(name: []const u8, seed: u64) u64 {
    const id = std.hash.Wyhash.hash(seed, name);
    return if (id == 0) 1 else id;
}

fn deriveDataGroupId(name: []const u8, seed: u64) u64 {
    return group_ids.dataGroupIdFromHash(std.hash.Wyhash.hash(seed, name));
}

fn deriveShardGroupId(table_name: []const u8, shard_index: u32) u64 {
    var hasher = std.hash.Wyhash.init(0x47525031);
    hasher.update(table_name);
    hasher.update(&[_]u8{0});
    hasher.update(std.mem.asBytes(&shard_index));
    return group_ids.dataGroupIdFromHash(hasher.final());
}

fn deriveShardBoundaryKey(alloc: std.mem.Allocator, shard_index: u32, shard_count: u32) ![]u8 {
    const pos = (@as(u32, shard_index) * 65536) / shard_count;
    const hi: u8 = @truncate(pos >> 8);
    const lo: u8 = @truncate(pos & 0xff);
    if (lo == 0) {
        return try std.fmt.allocPrint(alloc, "{x:0>2}", .{hi});
    }
    return try std.fmt.allocPrint(alloc, "{x:0>2}{x:0>2}", .{ hi, lo });
}

test "metadata.table status encoder emits antfly-style shard map" {
    const snapshot: metadata_api.AdminSnapshot = .{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{ .table_id = 7, .name = "docs", .description = "docs table", .schema_json = "{\"kind\":\"demo\"}", .read_schema_json = "{\"version\":0}", .indexes_json = "{\"full_text_index_v0\":{}}", .replication_sources_json = "[{\"type\":\"postgres\",\"dsn\":\"postgres://seed\",\"postgres_table\":\"seed_docs\"}]", .placement_role = "data" }})[0..]),
        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{ .group_id = 7001, .table_id = 7, .start_key = "doc:a", .end_key = "doc:z" }})[0..]),
        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
    };

    const encoded = (try encodeSingleTableStatus(std.testing.allocator, &snapshot, "docs")).?;
    defer std.testing.allocator.free(encoded);
    var parsed = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, encoded, .{});
    defer parsed.deinit();
    const root = parsed.value.object;
    try std.testing.expectEqualStrings("docs", root.get("name").?.string);
    try std.testing.expectEqualStrings("docs table", root.get("description").?.string);
    try std.testing.expect(root.get("schema") != null);
    try std.testing.expect(root.get("migration") != null);
    try std.testing.expect(root.get("indexes") != null);
    try std.testing.expect(root.get("replication_sources") != null);
    try std.testing.expect(root.get("shards") != null);

    const shards = root.get("shards").?.object;
    const shard = shards.get("7001").?.object;
    const byte_range = shard.get("byte_range").?.array.items;
    try std.testing.expectEqualStrings("doc:a", byte_range[0].string);
    try std.testing.expectEqualStrings("doc:z", byte_range[1].string);

    const replication_sources = root.get("replication_sources").?.array.items;
    try std.testing.expectEqual(@as(usize, 1), replication_sources.len);
    try std.testing.expectEqualStrings("postgres", replication_sources[0].object.get("type").?.string);
}

test "metadata.table detail encoder includes replication source status and action hint" {
    const snapshot: metadata_api.AdminSnapshot = .{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{ .table_id = 7, .name = "docs", .indexes_json = default_indexes_json, .replication_sources_json = "[{\"type\":\"postgres\",\"dsn\":\"postgres://db\",\"postgres_table\":\"users\"}]", .placement_role = "data" }})[0..]),
        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null }})[0..]),
        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
        .replication_source_statuses = @constCast((&[_]metadata_table_manager.ReplicationSourceStatusRecord{.{
            .table_id = 7,
            .source_ordinal = 0,
            .source_kind = "postgres",
            .external_table = "users",
            .cutover_mode = "slot_resumed",
            .slot_name = "slot_old",
            .publication_name = "pub_old",
            .phase = "streaming",
            .checkpoint = "lsn:0/10",
            .last_error = "",
        }})[0..]),
        .replication_source_action_hints = @constCast((&[_]metadata_api.ReplicationSourceActionHint{.{
            .table_id = 7,
            .table_name = @constCast("docs"),
            .source_ordinal = 0,
            .action = "reseed_exact_cutover",
            .reason = "existing_slot_non_exact_cutover",
            .reseed_exact_cutover_path = @constCast("/internal/v1/tables/docs/replication-sources/0/reseed-exact-cutover"),
        }})[0..]),
    };

    const encoded = (try encodeSingleTableStatus(std.testing.allocator, &snapshot, "docs")).?;
    defer std.testing.allocator.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"replication_sources\":[{\"type\":\"postgres\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"status\":{\"source_kind\":\"postgres\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"cutover_mode\":\"slot_resumed\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"action_hint\":{\"action\":\"reseed_exact_cutover\"") != null);

    const listed = try encodeTableList(std.testing.allocator, &snapshot, null);
    defer std.testing.allocator.free(listed);
    try std.testing.expect(std.mem.indexOf(u8, listed, "\"action_hint\"") == null);
    try std.testing.expect(std.mem.indexOf(u8, listed, "\"status\":{\"source_kind\":\"postgres\"") == null);
}

test "metadata.table status encoder honors storage status overrides" {
    const snapshot: metadata_api.AdminSnapshot = .{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{ .table_id = 7, .name = "docs", .indexes_json = default_indexes_json, .replication_sources_json = "[]", .placement_role = "data" }})[0..]),
        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null }})[0..]),
        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
    };
    const storage_statuses = [_]TableStorageStatus{.{ .table_name = "docs", .empty = true }};

    const encoded = (try encodeSingleTableStatusWithStorageStatuses(std.testing.allocator, &snapshot, "docs", storage_statuses[0..])).?;
    defer std.testing.allocator.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"storage_status\":{\"disk_usage\":0,\"empty\":true}") != null);
}

test "metadata.table status encoder canonicalizes embeddings indexes without inline names" {
    const snapshot: metadata_api.AdminSnapshot = .{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{ .table_id = 7, .name = "docs", .indexes_json = "{\"semantic_kg\":{\"type\":\"embeddings\",\"field\":\"body\",\"dimension\":3,\"embedder\":{\"provider\":\"openai\",\"model\":\"text-embedding-3-small\",\"url\":\"http://127.0.0.1:11434/v1\"}}}", .replication_sources_json = "[]", .placement_role = "data" }})[0..]),
        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null }})[0..]),
        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
    };

    const encoded = (try encodeSingleTableStatus(std.testing.allocator, &snapshot, "docs")).?;
    defer std.testing.allocator.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"semantic_kg\":{\"name\":\"semantic_kg\",\"type\":\"embeddings\"") != null);
}

test "metadata.table debug encoder emits runtime schemas and index bindings" {
    const schema_v1 =
        \\{"version":1,"default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","properties":{"title":{"type":"string","x-antfly-types":["text"],"x-antfly-analyzer":"french"}}}}}}
    ;
    const schema_v0 =
        \\{"version":0,"default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","properties":{"name":{"type":"string","x-antfly-types":["search_as_you_type"]}}}}}}
    ;
    const snapshot: metadata_api.AdminSnapshot = .{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{
            .table_id = 7,
            .name = "docs",
            .schema_json = schema_v1,
            .read_schema_json = schema_v0,
            .indexes_json = "{\"full_text_index_v0\":{\"type\":\"full_text\"},\"full_text_index_v1\":{\"type\":\"full_text\"}}",
            .replication_sources_json = "[]",
            .placement_role = "data",
        }})[0..]),
        .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{.{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null }})[0..]),
        .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
        .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
        .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
        .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
    };

    const encoded = (try encodeSingleTableStatusWithRuntimeSchemaDebug(std.testing.allocator, &snapshot, "docs", null)).?;
    defer std.testing.allocator.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"debug\":{\"runtime_schemas\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"slot\":\"active\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"slot\":\"read\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"index_name\":\"full_text_index_v0\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"schema_slot\":\"read\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"schema_slot\":\"active\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"analyzer\":\"french\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"algebraic_capabilities\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"capability_fingerprint\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"lifecycle_status\":\"rebuild_required\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"requires_rebuild\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"materializations\":[]") != null);
}

test "create table parser preserves supported metadata fields" {
    var parsed = try parseCreateTableRequest(std.testing.allocator, "{\"num_shards\":1,\"description\":\"docs table\",\"schema\":{\"kind\":\"demo\"},\"indexes\":{\"default\":{}},\"replication_sources\":[{\"type\":\"postgres\",\"dsn\":\"postgres://db\",\"postgres_table\":\"users\"}]}");
    defer parsed.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(?u32, 1), parsed.num_shards);
    try std.testing.expectEqualStrings("docs table", parsed.description.?);
    try std.testing.expectEqualStrings("{\"version\":0,\"kind\":\"demo\"}", parsed.schema_json.?);
    try std.testing.expectEqualStrings("{\"default\":{}}", parsed.indexes_json.?);
    try std.testing.expectEqualStrings("[{\"type\":\"postgres\",\"dsn\":\"postgres://db\",\"postgres_table\":\"users\"}]", parsed.replication_sources_json.?);
}

test "schema-derived algebraic indexes expand into explicit capability config" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","properties":{"customer":{"type":"keyword"},"amount":{"type":"number"},"created_at":{"type":"datetime"}}}}}}
    ;
    const indexes_json =
        \\{"sales_rollup":{"type":"algebraic","derive_from_schema":true}}
    ;

    try validatePublicAlgebraicIndexesJson(alloc, indexes_json);
    const expanded = try expandSchemaDerivedAlgebraicIndexesAlloc(alloc, "orders", indexes_json, schema_json);
    defer alloc.free(expanded);

    try std.testing.expect(std.mem.indexOf(u8, expanded, "\"derive_from_schema\"") == null);
    try std.testing.expect(std.mem.indexOf(u8, expanded, "\"type\":\"algebraic\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, expanded, "\"table\":\"orders\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, expanded, "\"group_fields\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, expanded, "\"measure_fields\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, expanded, "\"time_fields\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, expanded, "\"materializations\":[]") != null);
    try std.testing.expect(std.mem.indexOf(u8, expanded, "\"sum_by_customer\"") == null);
}

test "schema-derived algebraic indexes require a schema" {
    try std.testing.expectError(
        error.InvalidCreateTableRequest,
        expandSchemaDerivedAlgebraicIndexesAlloc(
            std.testing.allocator,
            "orders",
            "{\"sales_rollup\":{\"type\":\"algebraic\",\"derive_from_schema\":true}}",
            "",
        ),
    );
}

test "single schema-derived algebraic index expands into explicit capability config" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","properties":{"customer":{"type":"keyword"},"amount":{"type":"number"}}}}}}
    ;
    const index_json =
        \\{"type":"algebraic","derive_from_schema":true}
    ;
    try validatePublicAlgebraicIndexJson(alloc, index_json);
    const expanded = try expandSchemaDerivedAlgebraicIndexAlloc(
        alloc,
        "orders",
        index_json,
        schema_json,
    );
    defer alloc.free(expanded);

    try std.testing.expect(std.mem.indexOf(u8, expanded, "\"derive_from_schema\"") == null);
    try std.testing.expect(std.mem.indexOf(u8, expanded, "\"group_fields\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, expanded, "\"measure_fields\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, expanded, "\"materializations\":[]") != null);
    try std.testing.expect(std.mem.indexOf(u8, expanded, "\"sum_by_customer\"") == null);
}

test "public algebraic index definitions cannot declare internal materializations" {
    const alloc = std.testing.allocator;
    try std.testing.expectError(
        error.InvalidCreateTableRequest,
        validatePublicAlgebraicIndexJson(alloc, "{\"type\":\"algebraic\",\"materializations\":[]}"),
    );
    try std.testing.expectError(
        error.InvalidCreateTableRequest,
        validatePublicAlgebraicIndexJson(alloc, "{\"type\":\"algebraic\",\"derive_from_schema\":true,\"materializations\":[]}"),
    );
    try std.testing.expectError(
        error.InvalidCreateTableRequest,
        validatePublicAlgebraicIndexJson(alloc, "{\"type\":\"algebraic\",\"derive_from_schema\":true,\"group_fields\":[]}"),
    );
}

test "create table parser preserves postgres slot and publication metadata fields" {
    var parsed = try parseCreateTableRequest(
        std.testing.allocator,
        "{\"replication_sources\":[{\"type\":\"postgres\",\"dsn\":\"postgres://db\",\"postgres_table\":\"users\",\"slot_name\":\"custom_slot\",\"publication_name\":\"custom_pub\"}]}",
    );
    defer parsed.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings(
        "[{\"type\":\"postgres\",\"dsn\":\"postgres://db\",\"postgres_table\":\"users\",\"slot_name\":\"custom_slot\",\"publication_name\":\"custom_pub\"}]",
        parsed.replication_sources_json.?,
    );
}

test "create table parser preserves postgres exact cutover requirement metadata field" {
    var parsed = try parseCreateTableRequest(
        std.testing.allocator,
        "{\"replication_sources\":[{\"type\":\"postgres\",\"dsn\":\"postgres://db\",\"postgres_table\":\"users\",\"require_exact_cutover\":true}]}",
    );
    defer parsed.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings(
        "[{\"type\":\"postgres\",\"dsn\":\"postgres://db\",\"postgres_table\":\"users\",\"require_exact_cutover\":true}]",
        parsed.replication_sources_json.?,
    );
}

test "derive initial ranges honors shard count" {
    const table = deriveTableRecord("docs", .{ .num_shards = 4 });
    const ranges = try deriveInitialRanges(std.testing.allocator, table);
    defer {
        for (ranges) |record| metadata_table_manager.freeRange(std.testing.allocator, record);
        std.testing.allocator.free(ranges);
    }

    try std.testing.expectEqual(@as(usize, 4), ranges.len);
    try std.testing.expectEqual(table.table_id, ranges[0].table_id);
    try std.testing.expectEqualStrings("", ranges[0].start_key);
    try std.testing.expectEqualStrings("40", ranges[0].end_key.?);
    try std.testing.expectEqualStrings("40", ranges[1].start_key);
    try std.testing.expectEqualStrings("80", ranges[1].end_key.?);
    try std.testing.expectEqualStrings("80", ranges[2].start_key);
    try std.testing.expectEqualStrings("c0", ranges[2].end_key.?);
    try std.testing.expectEqualStrings("c0", ranges[3].start_key);
    try std.testing.expect(ranges[3].end_key == null);
}

test "create table parser rejects zero shards" {
    try std.testing.expectError(
        error.InvalidCreateTableRequest,
        parseCreateTableRequest(std.testing.allocator, "{\"num_shards\":0}"),
    );
}

test "create table parser rejects malformed replication sources" {
    try std.testing.expectError(
        error.InvalidCreateTableRequest,
        parseCreateTableRequest(std.testing.allocator, "{\"replication_sources\":{\"type\":\"postgres\"}}"),
    );
    try std.testing.expectError(
        error.InvalidCreateTableRequest,
        parseCreateTableRequest(std.testing.allocator, "{\"replication_sources\":[{\"type\":\"postgres\",\"dsn\":123,\"postgres_table\":\"users\"}]}"),
    );
}

test "schema update parser preserves object payload" {
    const parsed = try parseSchemaUpdateRequest(std.testing.allocator, "{\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\"}}}}");
    defer std.testing.allocator.free(parsed);
    try std.testing.expectEqualStrings("{\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\"}}}}", parsed);
}

test "schema update parser rejects malformed document schema payloads" {
    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        parseSchemaUpdateRequest(std.testing.allocator, "{\"document_schemas\":{\"doc\":{}}}"),
    );
    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        parseSchemaUpdateRequest(std.testing.allocator, "{\"document_schemas\":{\"doc\":{\"schema\":true}}}"),
    );
    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        parseSchemaUpdateRequest(std.testing.allocator, "{\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"string\"}}}}"),
    );
}

test "schema update parser rejects invalid top-level schema fields" {
    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        parseSchemaUpdateRequest(std.testing.allocator, "{\"version\":-1}"),
    );
    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        parseSchemaUpdateRequest(std.testing.allocator, "{\"ttl_duration_ns\":-1}"),
    );
    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        parseSchemaUpdateRequest(std.testing.allocator, "{\"dynamic_templates\":true}"),
    );
    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        parseSchemaUpdateRequest(std.testing.allocator, "{\"dynamic_templates\":{\"body\":{\"mapping\":true}}}"),
    );
}

test "validated table schema parses default type and dynamic templates" {
    var parsed = try parseValidatedTableSchema(
        std.testing.allocator,
        "{\"default_type\":\"doc\",\"enforce_types\":true,\"dynamic_templates\":{\"meta\":{\"match\":\"meta_*\",\"mapping\":{\"type\":\"keyword\"}}},\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"title\":{\"type\":\"text\"},\"published\":{\"type\":\"boolean\"}}}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    try std.testing.expectEqualStrings("doc", parsed.default_type);
    try std.testing.expect(parsed.enforce_types);
    try std.testing.expectEqual(@as(usize, 1), parsed.document_schemas.len);
    try std.testing.expectEqual(@as(usize, 1), parsed.dynamic_templates.len);
}

test "table schema write validation rejects unknown fields when enforce_types is enabled" {
    var parsed = try parseValidatedTableSchema(
        std.testing.allocator,
        "{\"default_type\":\"doc\",\"enforce_types\":true,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"title\":{\"type\":\"text\"}}}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateBatchWritesAgainstTableSchema(std.testing.allocator, parsed, &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"unexpected\"}" }}),
    );
}

test "table schema write validation uses explicit type and basic field type checks" {
    var parsed = try parseValidatedTableSchema(
        std.testing.allocator,
        "{\"enforce_types\":true,\"document_schemas\":{\"article\":{\"schema\":{\"type\":\"object\",\"properties\":{\"title\":{\"type\":\"text\"}}}},\"event\":{\"schema\":{\"type\":\"object\",\"properties\":{\"starts_at\":{\"type\":\"datetime\"}}}}}}",
    );
    defer parsed.deinit(std.testing.allocator);

    try validateBatchWritesAgainstTableSchema(std.testing.allocator, parsed, &.{.{ .key = "doc:a", .value = "{\"_type\":\"article\",\"title\":\"alpha\"}" }});
    try std.testing.expectError(
        error.InvalidBatchRequest,
        validateBatchWritesAgainstTableSchema(std.testing.allocator, parsed, &.{.{ .key = "doc:b", .value = "{\"title\":\"missing type\"}" }}),
    );
}

test "metadata.schema update preserves read schema and adds versioned full-text index" {
    const table: metadata_table_manager.TableRecord = .{
        .table_id = 7,
        .name = "docs",
        .description = "docs table",
        .schema_json = "{\"version\":0,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\"}}},\"dynamic_templates\":{\"body\":{\"mapping\":{\"type\":\"text\"}}}}",
        .indexes_json = "{\"full_text_index_v0\":{\"type\":\"full_text\"},\"embed_idx\":{\"type\":\"embeddings\",\"dimension\":384}}",
        .replication_sources_json = "[\"seed\"]",
        .placement_role = "data",
    };

    const updated = try applySchemaUpdateRecord(
        std.testing.allocator,
        &table,
        "{\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"title\":{\"type\":\"string\"}}}}},\"dynamic_templates\":{\"body\":{\"mapping\":{\"type\":\"text\"}}}}",
    );
    defer metadata_table_manager.freeTable(std.testing.allocator, updated);

    try std.testing.expect(std.mem.indexOf(u8, updated.schema_json, "\"version\":1") != null);
    try std.testing.expect(std.mem.indexOf(u8, updated.read_schema_json, "\"version\":0") != null);
    try std.testing.expect(std.mem.indexOf(u8, updated.read_schema_json, "\"document_schemas\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, updated.indexes_json, "\"full_text_index_v0\":{\"type\":\"full_text\"}") != null);
    try std.testing.expect(std.mem.indexOf(u8, updated.indexes_json, "\"embed_idx\":{\"type\":\"embeddings\",\"dimension\":384}") != null);
    try std.testing.expect(std.mem.indexOf(u8, updated.indexes_json, "\"full_text_index_v1\":{\"name\":\"full_text_index_v1\",\"type\":\"full_text\"}") != null);
}

test "metadata.schema update keeps version for template-only changes" {
    const table: metadata_table_manager.TableRecord = .{
        .table_id = 7,
        .name = "docs",
        .schema_json = "{\"version\":2,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\"}}},\"dynamic_templates\":{\"body\":{\"mapping\":{\"type\":\"text\"}}}}",
        .read_schema_json = "{\"version\":1,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\",\"properties\":{\"title\":{\"type\":\"string\"}}}}}}",
        .indexes_json = "{\"full_text_index_v1\":{\"type\":\"full_text\"},\"full_text_index_v2\":{\"type\":\"full_text\"}}",
        .replication_sources_json = "[]",
        .placement_role = "data",
    };

    const updated = try applySchemaUpdateRecord(
        std.testing.allocator,
        &table,
        "{\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\"}}},\"dynamic_templates\":{\"body\":{\"mapping\":{\"type\":\"keyword\"}}}}",
    );
    defer metadata_table_manager.freeTable(std.testing.allocator, updated);

    try std.testing.expect(std.mem.indexOf(u8, updated.schema_json, "\"version\":2") != null);
    try std.testing.expectEqualStrings(table.read_schema_json, updated.read_schema_json);
    try std.testing.expect(std.mem.indexOf(u8, updated.indexes_json, "\"full_text_index_v2\":{\"type\":\"full_text\"}") != null);
    try std.testing.expect(std.mem.indexOf(u8, updated.indexes_json, "\"full_text_index_v3\"") == null);
}

test "metadata.query routing selects read schema full text index" {
    const table: metadata_table_manager.TableRecord = .{
        .table_id = 7,
        .name = "docs",
        .schema_json = "{\"version\":3}",
        .read_schema_json = "{\"version\":0}",
        .indexes_json = "{\"full_text_index_v0\":{\"type\":\"full_text\"},\"full_text_index_v3\":{\"type\":\"full_text\"}}",
        .placement_role = "data",
    };
    var req: db_mod.types.SearchRequest = .{
        .query = .{ .match = .{ .field = "body", .text = "hello" } },
    };
    defer if (req.index_name) |index_name| std.testing.allocator.free(index_name);
    defer if (req.primary_text_index_name) |index_name| std.testing.allocator.free(index_name);
    defer {
        std.testing.allocator.free(req.query.match.field);
        std.testing.allocator.free(req.query.match.text);
    }
    req.query.match.field = try std.testing.allocator.dupe(u8, req.query.match.field);
    req.query.match.text = try std.testing.allocator.dupe(u8, req.query.match.text);

    try routeQueryRequestToActiveReadIndex(std.testing.allocator, &table, &req);
    try std.testing.expectEqualStrings("full_text_index_v0", req.index_name.?);
    try std.testing.expectEqualStrings("full_text_index_v0", req.primary_text_index_name.?);
}

test "metadata.query routing selects current versioned full text index" {
    const table: metadata_table_manager.TableRecord = .{
        .table_id = 7,
        .name = "docs",
        .schema_json = "{\"version\":2}",
        .indexes_json = "{\"default\":{\"type\":\"full_text\"},\"full_text_index_v2\":{\"type\":\"full_text\"}}",
        .placement_role = "data",
    };
    var req: db_mod.types.SearchRequest = .{
        .full_text = .{ .match = .{ .field = try std.testing.allocator.dupe(u8, "body"), .text = try std.testing.allocator.dupe(u8, "hello") } },
    };
    defer if (req.index_name) |index_name| std.testing.allocator.free(index_name);
    defer if (req.primary_text_index_name) |index_name| std.testing.allocator.free(index_name);
    defer {
        std.testing.allocator.free(req.full_text.?.match.field);
        std.testing.allocator.free(req.full_text.?.match.text);
    }

    try routeQueryRequestToActiveReadIndex(std.testing.allocator, &table, &req);
    try std.testing.expectEqualStrings("full_text_index_v2", req.index_name.?);
    try std.testing.expectEqualStrings("full_text_index_v2", req.primary_text_index_name.?);
}

test "metadata.query routing preserves vector index and records read schema text index for filters" {
    const table: metadata_table_manager.TableRecord = .{
        .table_id = 7,
        .name = "docs",
        .schema_json = "{\"version\":3}",
        .read_schema_json = "{\"version\":0}",
        .indexes_json = "{\"dense_idx\":{\"type\":\"embeddings\",\"dimension\":3},\"full_text_index_v0\":{\"type\":\"full_text\"},\"full_text_index_v3\":{\"type\":\"full_text\"}}",
        .placement_role = "data",
    };
    var req: db_mod.types.SearchRequest = .{
        .index_name = try std.testing.allocator.dupe(u8, "dense_idx"),
        .full_text = .{ .match_all = {} },
        .filter_query_json = try std.testing.allocator.dupe(u8, "{\"term\":{\"status\":\"active\"}}"),
    };
    defer if (req.index_name) |index_name| std.testing.allocator.free(index_name);
    defer if (req.primary_text_index_name) |index_name| std.testing.allocator.free(index_name);
    defer std.testing.allocator.free(req.filter_query_json);

    try routeQueryRequestToActiveReadIndex(std.testing.allocator, &table, &req);
    try std.testing.expectEqualStrings("dense_idx", req.index_name.?);
    try std.testing.expectEqualStrings("full_text_index_v0", req.primary_text_index_name.?);
}
