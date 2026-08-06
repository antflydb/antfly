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
const metadata_openapi = @import("antfly_metadata_openapi");
const tables_api = @import("../metadata/catalog/table_ddl.zig");

fn stringifyJsonAlloc(alloc: std.mem.Allocator, value: anytype) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})});
}

pub const CreateTableRequestErrorDisposition = enum {
    bad_request,
    internal_failure,
};

/// Keep the HTTP boundary fail-closed: only errors that are known to describe
/// malformed client input become 400 responses. Allocation, entropy, I/O, and
/// any future operational failures must retain their error identity so the
/// server can surface a 500 and preserve operational observability.
pub fn classifyCreateTableRequestError(err: anyerror) CreateTableRequestErrorDisposition {
    return switch (err) {
        error.InvalidCreateTableRequest,
        error.InvalidCreateTableSchemaRequest,
        error.SchemaVersionManagedByBackend,
        error.InvalidSchemaUpdateRequest,
        error.SyntaxError,
        error.UnexpectedEndOfInput,
        error.UnexpectedToken,
        error.InvalidNumber,
        error.Overflow,
        error.InvalidEnumTag,
        error.DuplicateField,
        error.UnknownField,
        error.MissingField,
        error.LengthMismatch,
        error.InvalidCharacter,
        error.ValueTooLong,
        => .bad_request,
        else => .internal_failure,
    };
}

pub fn parseCreateTableRequest(alloc: std.mem.Allocator, body: []const u8) !tables_api.CreateTableRequest {
    if (body.len == 0) return .{};

    // Validate and normalize indexes from the raw request before invoking the
    // generated parser. The generated OpenAPI parser rejects unknown enum
    // values, and this function historically fell back to the more permissive
    // internal parser on any generated-parser error. That allowed an unknown
    // index type to reach catalog publication before local admission rejected
    // it. Performing public index validation first keeps the compatibility
    // fallback without making it an admission bypass.
    var raw_parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{});
    defer raw_parsed.deinit();
    const raw_root = switch (raw_parsed.value) {
        .object => |object| object,
        else => return error.InvalidCreateTableRequest,
    };
    if (raw_root.get("indexes")) |indexes_value| {
        if (indexes_value != .null) try validateCreateTableIndexesValue(indexes_value);
    }
    if (raw_root.get("schema")) |schema_value| {
        if (schema_value != .null) try tables_api.validateCreateSchemaVersion(schema_value, false);
    }

    // Use typed OpenAPI parsing for scalar fields (num_shards, description, schema,
    // replication_sources). For indexes, parse from the raw body to preserve
    // type-specific fields (external, dimension, edge_types, etc.) that the
    // generated IndexConfig struct doesn't capture.
    var parsed = metadata_openapi.server.parseCreateTableBody(alloc, body) catch {
        var fallback = try tables_api.parseCreateTableRequest(alloc, body);
        errdefer fallback.deinit(alloc);
        try validateSupportedPublicIndexesJson(
            alloc,
            fallback.indexes_json orelse tables_api.default_indexes_json,
        );
        return fallback;
    };
    defer parsed.deinit();

    var req: tables_api.CreateTableRequest = .{};
    errdefer req.deinit(alloc);

    if (parsed.value.num_shards) |num_shards| {
        req.num_shards = std.math.cast(u32, num_shards) orelse return error.InvalidCreateTableRequest;
    }
    if (parsed.value.description) |description| {
        req.description = try alloc.dupe(u8, description);
    }

    if (raw_root.get("indexes")) |indexes_value| {
        if (indexes_value != .null)
            req.indexes_json = try normalizeCreateTableIndexesFromValue(alloc, indexes_value)
        else
            req.indexes_json = try alloc.dupe(u8, tables_api.default_indexes_json);
    } else {
        req.indexes_json = try alloc.dupe(u8, tables_api.default_indexes_json);
    }
    if (raw_root.get("typed_paths")) |typed_paths_value| {
        if (typed_paths_value == .null) {
            // Optional public metadata field; explicit null is equivalent to
            // absence, matching the core table parser's optional-field rules.
        } else {
            const indexes_with_typed_paths = try tables_api.mergeTypedPathsIntoIndexesJsonAlloc(alloc, req.indexes_json.?, typed_paths_value);
            alloc.free(req.indexes_json.?);
            req.indexes_json = indexes_with_typed_paths;
        }
    }

    if (raw_root.get("schema")) |schema_value| {
        if (schema_value != .null) {
            const raw_schema = try stringifyJsonAlloc(alloc, schema_value);
            defer alloc.free(raw_schema);
            if (publicSchemaMentionsInternalDocValues(raw_schema)) return error.InvalidCreateTableSchemaRequest;
            const validated_schema = tables_api.parseSchemaUpdateRequest(alloc, raw_schema) catch |err| switch (err) {
                error.InvalidSchemaUpdateRequest => return error.InvalidCreateTableSchemaRequest,
                else => return err,
            };
            defer alloc.free(validated_schema);
            req.schema_json = tables_api.normalizeSchemaVersion(alloc, validated_schema, 0) catch |err| switch (err) {
                error.InvalidSchemaUpdateRequest => return error.InvalidCreateTableSchemaRequest,
                else => return err,
            };
        }
    } else if (parsed.value.schema) |schema| {
        const raw_schema = try stringifyJsonAlloc(alloc, schema);
        defer alloc.free(raw_schema);
        if (publicSchemaMentionsInternalDocValues(raw_schema)) return error.InvalidCreateTableSchemaRequest;
        const validated_schema = tables_api.parseSchemaUpdateRequest(alloc, raw_schema) catch |err| switch (err) {
            error.InvalidSchemaUpdateRequest => return error.InvalidCreateTableRequest,
            else => return err,
        };
        defer alloc.free(validated_schema);
        req.schema_json = tables_api.normalizeSchemaVersion(alloc, validated_schema, 0) catch |err| switch (err) {
            error.InvalidSchemaUpdateRequest => return error.InvalidCreateTableRequest,
            else => return err,
        };
    }
    if (parsed.value.replication_sources) |replication_sources| {
        req.replication_sources_json = try stringifyJsonAlloc(alloc, replication_sources);
    }

    if (req.num_shards) |num_shards| {
        if (num_shards == 0) return error.InvalidCreateTableRequest;
    }
    return req;
}

pub fn normalizeCreateTableIndexesValueAlloc(alloc: std.mem.Allocator, value: std.json.Value) ![]u8 {
    return try normalizeCreateTableIndexesFromValue(alloc, value);
}

pub fn normalizeTableDefinitionIndexesValueAlloc(alloc: std.mem.Allocator, value: std.json.Value) ![]u8 {
    const object = switch (value) {
        .object => |object| object,
        else => return error.InvalidCreateTableRequest,
    };

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.append(alloc, '{');
    var first = true;
    var saw_full_text = false;

    var it = object.iterator();
    while (it.next()) |entry| {
        if (entry.value_ptr.* != .object) return error.InvalidCreateTableRequest;
        const index_type = extractPublicIndexType(entry.value_ptr.object) orelse "full_text";
        const is_full_text = isPublicFullTextType(index_type);

        if (is_full_text) {
            if (!isReservedFullTextIndexName(entry.key_ptr.*)) continue;
            saw_full_text = true;
        } else if (isReservedFullTextIndexName(entry.key_ptr.*)) {
            return error.InvalidCreateTableRequest;
        }

        if (entry.value_ptr.object.get("name")) |name_value| {
            if (name_value != .string) return error.InvalidCreateTableRequest;
            if (!std.mem.eql(u8, name_value.string, entry.key_ptr.*)) return error.InvalidCreateTableRequest;
        }

        const normalized = normalizeIndexConfigJson(alloc, entry.value_ptr.object, entry.key_ptr.*, .{
            .include_name = true,
            .default_type = true,
        }) catch |err| switch (err) {
            error.InvalidCreateIndexRequest => return error.InvalidCreateTableRequest,
            else => return err,
        };
        defer alloc.free(normalized);

        if (!first) try out.append(alloc, ',');
        first = false;
        try appendJsonString(alloc, &out, entry.key_ptr.*);
        try out.append(alloc, ':');
        try out.appendSlice(alloc, normalized);
    }

    if (!saw_full_text) {
        if (!first) try out.append(alloc, ',');
        try appendJsonString(alloc, &out, tables_api.default_full_text_index_name);
        try out.append(alloc, ':');
        try out.appendSlice(alloc, "{\"name\":\"");
        try out.appendSlice(alloc, tables_api.default_full_text_index_name);
        try out.appendSlice(alloc, "\",\"type\":\"full_text\"}");
    }

    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

pub fn encodeCreateTableRequest(alloc: std.mem.Allocator, req: tables_api.CreateTableRequest) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);

    try out.append(alloc, '{');
    var first = true;

    if (req.num_shards) |num_shards| {
        try appendField(alloc, &out, "num_shards", .{ .integer = num_shards }, &first);
    }
    if (req.description) |description| {
        try appendField(alloc, &out, "description", .{ .string = description }, &first);
    }
    if (req.indexes_json) |indexes_json| {
        try appendRawJsonField(alloc, &out, "indexes", indexes_json, &first);
    }
    if (req.schema_json) |schema_json| {
        try appendRawJsonField(alloc, &out, "schema", schema_json, &first);
    }
    if (req.replication_sources_json) |replication_sources_json| {
        try appendRawJsonField(alloc, &out, "replication_sources", replication_sources_json, &first);
    }

    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

pub fn parseSchemaUpdateRequest(alloc: std.mem.Allocator, body: []const u8) ![]u8 {
    if (body.len == 0) return error.InvalidSchemaUpdateRequest;
    if (publicSchemaMentionsInternalDocValues(body)) return error.InvalidSchemaUpdateRequest;
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, body, .{}) catch return error.InvalidSchemaUpdateRequest;
    defer parsed.deinit();
    if (parsed.value == .object) {
        if (parsed.value.object.get("version")) |version| {
            if (version != .null) return error.SchemaVersionManagedByBackend;
        }
    }
    // Pass the raw body directly to preserve x-antfly-* extension properties
    // that would be lost if round-tripped through the typed OpenAPI TableSchema struct.
    return try tables_api.parseSchemaUpdateRequest(alloc, body);
}

fn publicSchemaMentionsInternalDocValues(body: []const u8) bool {
    return std.mem.indexOf(u8, body, "\"doc_values\"") != null;
}

pub fn schemaUpdateRequestErrorMessage(err: anyerror, body: []const u8) []const u8 {
    if (err == error.SchemaVersionManagedByBackend) {
        return "schema.version is managed by Antfly; omit it";
    }
    if (publicSchemaMentionsInternalDocValues(body)) {
        return "invalid schema update request: doc_values is internal; use sortable: true on scalar mappings";
    }
    return "invalid schema update request";
}

pub fn createTableRequestErrorMessage(err: anyerror, body: []const u8) []const u8 {
    if (err == error.SchemaVersionManagedByBackend) {
        return "schema.version is managed by Antfly; omit it";
    }
    if (publicSchemaMentionsInternalDocValues(body)) {
        return "invalid create table request: schema doc_values is internal; use sortable: true on scalar mappings";
    }
    return "invalid create table request";
}

pub fn parseCreateIndexRequest(alloc: std.mem.Allocator, index_name: []const u8, body: []const u8) ![]u8 {
    if (body.len == 0) return error.InvalidCreateIndexRequest;

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{});
    defer parsed.deinit();
    const root = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidCreateIndexRequest,
    };

    if (root.get("name")) |name_value| {
        if (name_value != .string) return error.InvalidCreateIndexRequest;
        if (!std.mem.eql(u8, name_value.string, index_name)) return error.InvalidCreateIndexRequest;
    }
    if (isReservedPublicCreateIndexName(index_name)) return error.InvalidCreateIndexRequest;

    return normalizeIndexConfigJson(alloc, root, index_name, .{
        .include_name = true,
        .default_type = true,
    }) catch |err| switch (err) {
        error.InvalidCreateIndexRequest => error.InvalidCreateIndexRequest,
        else => err,
    };
}

pub fn parseArtifactEnrichmentRequest(alloc: std.mem.Allocator, artifact_name: []const u8, body: []const u8) ![]u8 {
    if (body.len == 0) return error.InvalidArtifactEnrichmentRequest;

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{});
    defer parsed.deinit();
    const root = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidArtifactEnrichmentRequest,
    };

    if (root.get("name")) |name_value| {
        if (name_value != .string) return error.InvalidArtifactEnrichmentRequest;
        if (!std.mem.eql(u8, name_value.string, artifact_name)) return error.InvalidArtifactEnrichmentRequest;
    }
    if (root.get("full_text_index")) |value| {
        if (value != .bool) return error.InvalidArtifactEnrichmentRequest;
        if (value.bool) {
            const kind = root.get("kind") orelse return error.InvalidArtifactEnrichmentRequest;
            if (kind != .string or (!std.mem.eql(u8, kind.string, "chunk") and !std.mem.eql(u8, kind.string, "asset"))) return error.InvalidArtifactEnrichmentRequest;
        }
    }

    return normalizeArtifactEnrichmentConfigJson(alloc, root, artifact_name) catch |err| switch (err) {
        error.InvalidArtifactEnrichmentRequest => error.InvalidArtifactEnrichmentRequest,
        else => err,
    };
}

const NormalizeIndexOptions = struct {
    include_name: bool,
    default_type: bool,
};

fn normalizeIndexConfigJson(
    alloc: std.mem.Allocator,
    object: anytype,
    index_name: []const u8,
    options: NormalizeIndexOptions,
) ![]u8 {
    try validatePublicIndexObject(object);

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);

    try out.append(alloc, '{');
    var first = true;
    const Object = @TypeOf(object);
    const contains_name = if (@hasField(Object, "map")) object.map.contains("name") else object.contains("name");
    const contains_type = if (@hasField(Object, "map")) object.map.contains("type") else object.contains("type");

    if (options.include_name and !contains_name) {
        try appendField(alloc, &out, "name", .{ .string = index_name }, &first);
    }
    if (options.default_type and !contains_type) {
        try appendField(alloc, &out, "type", .{ .string = "full_text" }, &first);
    }

    if (@hasField(Object, "map")) {
        var it = object.map.iterator();
        while (it.next()) |entry| {
            if (!options.include_name and std.mem.eql(u8, entry.key_ptr.*, "name")) continue;
            try appendField(alloc, &out, entry.key_ptr.*, entry.value_ptr.*, &first);
        }
    } else {
        var it = object.iterator();
        while (it.next()) |entry| {
            if (!options.include_name and std.mem.eql(u8, entry.key_ptr.*, "name")) continue;
            try appendField(alloc, &out, entry.key_ptr.*, entry.value_ptr.*, &first);
        }
    }
    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

fn validatePublicIndexObject(object: anytype) !void {
    const Object = @TypeOf(object);
    const explicit_type = if (@hasField(Object, "map"))
        object.map.get("type")
    else
        object.get("type");
    if (explicit_type) |value| {
        if (value != .string) return error.InvalidCreateIndexRequest;
    }
    const index_type = extractPublicIndexType(object) orelse "full_text";
    if (!isSupportedPublicIndexType(index_type)) return error.InvalidCreateIndexRequest;
    if (std.mem.eql(u8, index_type, "full_text")) {
        try validatePublicFullTextIndexObject(object);
        return;
    }
    if (std.mem.eql(u8, index_type, "embeddings") or
        std.mem.eql(u8, index_type, "graph") or
        std.mem.eql(u8, index_type, "algebraic"))
    {
        return;
    }
    return error.InvalidCreateIndexRequest;
}

fn validateCreateTableIndexesValue(value: std.json.Value) !void {
    const object = switch (value) {
        .object => |object| object,
        else => return error.InvalidCreateTableRequest,
    };

    var it = object.iterator();
    while (it.next()) |entry| {
        if (entry.value_ptr.* != .object) return error.InvalidCreateTableRequest;
        try validateCreateTableIndexName(entry.key_ptr.*);
        validatePublicIndexObject(entry.value_ptr.object) catch return error.InvalidCreateTableRequest;
    }
}

fn validateSupportedPublicIndexesJson(alloc: std.mem.Allocator, indexes_json: []const u8) !void {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, indexes_json, .{}) catch
        return error.InvalidCreateTableRequest;
    defer parsed.deinit();
    const object = switch (parsed.value) {
        .object => |value| value,
        else => return error.InvalidCreateTableRequest,
    };
    var it = object.iterator();
    while (it.next()) |entry| {
        if (entry.value_ptr.* != .object) return error.InvalidCreateTableRequest;
        validatePublicIndexObject(entry.value_ptr.object) catch return error.InvalidCreateTableRequest;
    }
}

fn normalizeArtifactEnrichmentConfigJson(
    alloc: std.mem.Allocator,
    object: anytype,
    artifact_name: []const u8,
) ![]u8 {
    try validatePublicArtifactEnrichmentObject(object);

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);

    try out.append(alloc, '{');
    var first = true;
    const Object = @TypeOf(object);
    const contains_name = if (@hasField(Object, "map")) object.map.contains("name") else object.contains("name");
    if (!contains_name) {
        try appendField(alloc, &out, "name", .{ .string = artifact_name }, &first);
    }

    if (@hasField(Object, "map")) {
        var it = object.map.iterator();
        while (it.next()) |entry| try appendField(alloc, &out, entry.key_ptr.*, entry.value_ptr.*, &first);
    } else {
        var it = object.iterator();
        while (it.next()) |entry| try appendField(alloc, &out, entry.key_ptr.*, entry.value_ptr.*, &first);
    }
    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

fn validatePublicArtifactEnrichmentObject(object: anytype) !void {
    const Object = @TypeOf(object);
    if (@hasField(Object, "map")) {
        var it = object.map.iterator();
        while (it.next()) |entry| {
            if (!isAllowedPublicArtifactEnrichmentField(entry.key_ptr.*)) return error.InvalidArtifactEnrichmentRequest;
        }
        return;
    }

    var it = object.iterator();
    while (it.next()) |entry| {
        if (!isAllowedPublicArtifactEnrichmentField(entry.key_ptr.*)) return error.InvalidArtifactEnrichmentRequest;
    }
}

fn isAllowedPublicArtifactEnrichmentField(field_name: []const u8) bool {
    return std.mem.eql(u8, field_name, "name") or
        std.mem.eql(u8, field_name, "kind") or
        std.mem.eql(u8, field_name, "field") or
        std.mem.eql(u8, field_name, "template") or
        std.mem.eql(u8, field_name, "source_artifact_name") or
        std.mem.eql(u8, field_name, "expected_dims") or
        std.mem.eql(u8, field_name, "chunk_size") or
        std.mem.eql(u8, field_name, "chunk_overlap") or
        std.mem.eql(u8, field_name, "chunker_json") or
        std.mem.eql(u8, field_name, "full_text_index") or
        std.mem.eql(u8, field_name, "content_type") or
        std.mem.eql(u8, field_name, "producer_json") or
        std.mem.eql(u8, field_name, "execution");
}

fn extractPublicIndexType(object: anytype) ?[]const u8 {
    const Object = @TypeOf(object);
    if (@hasField(Object, "map")) {
        const value = object.map.get("type") orelse return null;
        return switch (value) {
            .string => |str| str,
            else => null,
        };
    }
    const value = object.get("type") orelse return null;
    return switch (value) {
        .string => |str| str,
        else => null,
    };
}

fn isPublicScalarIndexType(index_type: []const u8) bool {
    return std.mem.eql(u8, index_type, "scalar") or
        std.mem.eql(u8, index_type, "path") or
        std.mem.eql(u8, index_type, "secondary") or
        std.mem.eql(u8, index_type, "keyword") or
        std.mem.eql(u8, index_type, "numeric") or
        std.mem.eql(u8, index_type, "boolean") or
        std.mem.eql(u8, index_type, "datetime") or
        std.mem.eql(u8, index_type, "term");
}

fn validatePublicFullTextIndexObject(object: anytype) !void {
    const Object = @TypeOf(object);
    if (@hasField(Object, "map")) {
        var it = object.map.iterator();
        while (it.next()) |entry| {
            if (!isAllowedPublicFullTextField(entry.key_ptr.*)) return error.InvalidCreateIndexRequest;
        }
        return;
    }

    var it = object.iterator();
    while (it.next()) |entry| {
        if (!isAllowedPublicFullTextField(entry.key_ptr.*)) return error.InvalidCreateIndexRequest;
    }
}

fn isAllowedPublicFullTextField(field_name: []const u8) bool {
    return std.mem.eql(u8, field_name, "name") or
        std.mem.eql(u8, field_name, "type") or
        std.mem.eql(u8, field_name, "description") or
        std.mem.eql(u8, field_name, "mem_only") or
        std.mem.eql(u8, field_name, "field") or
        std.mem.eql(u8, field_name, "path") or
        std.mem.eql(u8, field_name, "fields") or
        std.mem.eql(u8, field_name, "paths") or
        std.mem.eql(u8, field_name, "scalar_field") or
        std.mem.eql(u8, field_name, "scalar_path") or
        std.mem.eql(u8, field_name, "scalar_fields") or
        std.mem.eql(u8, field_name, "scalar_paths") or
        std.mem.eql(u8, field_name, "indexed_scalar_field") or
        std.mem.eql(u8, field_name, "indexed_scalar_path") or
        std.mem.eql(u8, field_name, "indexed_scalar_fields") or
        std.mem.eql(u8, field_name, "indexed_scalar_paths");
}

fn normalizeCreateTableIndexesFromValue(alloc: std.mem.Allocator, value: std.json.Value) ![]u8 {
    const object = switch (value) {
        .object => |object| object,
        else => return error.InvalidCreateTableRequest,
    };

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.append(alloc, '{');
    var first = true;
    try appendDefaultFullTextIndexEntry(alloc, &out, &first);
    var it = object.iterator();
    while (it.next()) |entry| {
        if (entry.value_ptr.* != .object) return error.InvalidCreateTableRequest;
        try validateCreateTableIndexName(entry.key_ptr.*);
        const normalized = normalizeIndexConfigJson(alloc, entry.value_ptr.object, entry.key_ptr.*, .{
            .include_name = true,
            .default_type = true,
        }) catch |err| switch (err) {
            error.InvalidCreateIndexRequest => return error.InvalidCreateTableRequest,
            else => return err,
        };
        defer alloc.free(normalized);
        if (isPublicFullTextType(extractPublicIndexType(entry.value_ptr.object) orelse "full_text")) continue;
        if (!first) try out.append(alloc, ',');
        first = false;
        try appendJsonString(alloc, &out, entry.key_ptr.*);
        try out.append(alloc, ':');
        try out.appendSlice(alloc, normalized);
    }
    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

fn appendDefaultFullTextIndexEntry(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
) !void {
    if (!first.*) try out.append(alloc, ',');
    first.* = false;
    try appendJsonString(alloc, out, tables_api.default_full_text_index_name);
    try out.append(alloc, ':');
    try out.appendSlice(alloc, "{\"name\":\"");
    try out.appendSlice(alloc, tables_api.default_full_text_index_name);
    try out.appendSlice(alloc, "\",\"type\":\"full_text\"}");
}

fn validateCreateTableIndexName(index_name: []const u8) !void {
    if (index_name.len == 0) return error.InvalidCreateTableRequest;
    if (isReservedFullTextIndexName(index_name)) return error.InvalidCreateTableRequest;
}

fn isReservedFullTextIndexName(index_name: []const u8) bool {
    return std.mem.startsWith(u8, index_name, "full_text_index");
}

fn isReservedPublicCreateIndexName(index_name: []const u8) bool {
    return isReservedFullTextIndexName(index_name) or
        std.mem.eql(u8, index_name, "default");
}

fn isPublicFullTextType(index_type: []const u8) bool {
    return std.mem.eql(u8, index_type, "full_text");
}

fn isSupportedPublicIndexType(index_type: []const u8) bool {
    return isPublicFullTextType(index_type) or
        std.mem.eql(u8, index_type, "embeddings") or
        std.mem.eql(u8, index_type, "graph") or
        std.mem.eql(u8, index_type, "algebraic");
}

fn appendField(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    key: []const u8,
    value: std.json.Value,
    first: *bool,
) !void {
    if (!first.*) try out.append(alloc, ',');
    first.* = false;
    const encoded_key = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(key, .{})});
    defer alloc.free(encoded_key);
    try out.appendSlice(alloc, encoded_key);
    try out.append(alloc, ':');
    const encoded_value = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})});
    defer alloc.free(encoded_value);
    try out.appendSlice(alloc, encoded_value);
}

fn appendRawJsonField(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    key: []const u8,
    raw_json: []const u8,
    first: *bool,
) !void {
    if (!first.*) try out.append(alloc, ',');
    first.* = false;
    try appendJsonString(alloc, out, key);
    try out.append(alloc, ':');
    try out.appendSlice(alloc, raw_json);
}

fn appendJsonString(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), value: []const u8) !void {
    const encoded = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})});
    defer alloc.free(encoded);
    try out.appendSlice(alloc, encoded);
}

test "table contract parses create table via generated openapi type" {
    var req = try parseCreateTableRequest(
        std.testing.allocator,
        "{\"num_shards\":1,\"description\":\"docs\",\"indexes\":{\"default\":{\"name\":\"default\",\"type\":\"full_text\"}},\"schema\":{\"default_type\":\"doc\"}}",
    );
    defer req.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(?u32, 1), req.num_shards);
    try std.testing.expectEqualStrings("docs", req.description.?);
    try std.testing.expectEqualStrings(tables_api.default_indexes_json, req.indexes_json.?);
    try std.testing.expect(std.mem.indexOf(u8, req.schema_json.?, "\"default_type\":\"doc\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, req.schema_json.?, "\"version\":0") != null);
    try std.testing.expect(std.mem.indexOf(u8, req.schema_json.?, "\"version\":null") == null);
}

test "table contract preserves multi shard create table requests" {
    var req = try parseCreateTableRequest(
        std.testing.allocator,
        "{\"num_shards\":4,\"description\":\"docs\"}",
    );
    defer req.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(?u32, 4), req.num_shards);
}

test "table contract rejects zero shard create table requests" {
    try std.testing.expectError(
        error.InvalidCreateTableRequest,
        parseCreateTableRequest(std.testing.allocator, "{\"num_shards\":0}"),
    );
}

test "table contract encodes internal create table request back to public json" {
    var req: tables_api.CreateTableRequest = .{
        .num_shards = 1,
        .description = try std.testing.allocator.dupe(u8, "docs"),
        .indexes_json = try std.testing.allocator.dupe(u8, "{\"full_text_index_v0\":{\"name\":\"full_text_index_v0\",\"type\":\"full_text\"}}"),
        .schema_json = try std.testing.allocator.dupe(
            u8,
            "{\"version\":0,\"document_schemas\":{\"doc\":{\"schema\":{\"type\":\"object\"}}}}",
        ),
    };
    defer req.deinit(std.testing.allocator);

    const body = try encodeCreateTableRequest(std.testing.allocator, req);
    defer std.testing.allocator.free(body);
    var parsed = try metadata_openapi.server.parseCreateTableBody(std.testing.allocator, body);
    defer parsed.deinit();
    var raw = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, body, .{});
    defer raw.deinit();
    const root = raw.value.object;

    try std.testing.expectEqual(@as(i64, 1), parsed.value.num_shards.?);
    try std.testing.expectEqualStrings("docs", parsed.value.description.?);
    try std.testing.expect(parsed.value.indexes != null);
    try std.testing.expect(parsed.value.indexes.?.map.count() == 1);
    try std.testing.expect(parsed.value.indexes.?.map.get("full_text_index_v0") != null);
    try std.testing.expect(parsed.value.schema != null);
    try std.testing.expect(root.get("indexes_json") == null);
    try std.testing.expect(root.get("schema_json") == null);
}

test "table contract accepts create table with explicit null optional fields" {
    var req = try parseCreateTableRequest(
        std.testing.allocator,
        "{\"num_shards\":1,\"description\":null,\"indexes\":null,\"typed_paths\":null,\"schema\":null,\"replication_sources\":null}",
    );
    defer req.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(?u32, 1), req.num_shards);
    try std.testing.expectEqual(@as(?[]u8, null), req.description);
    try std.testing.expect(req.indexes_json != null);
}

test "table contract rejects malformed schema payloads" {
    try std.testing.expectError(
        error.InvalidSchemaUpdateRequest,
        parseSchemaUpdateRequest(std.testing.allocator, "{\"document_schemas\":{\"doc\":{}}}"),
    );
    try std.testing.expectError(
        error.InvalidCreateTableSchemaRequest,
        parseCreateTableRequest(std.testing.allocator, "{\"schema\":{\"ttl_duration_ns\":-1}}"),
    );
    try std.testing.expectError(
        error.InvalidCreateTableSchemaRequest,
        parseCreateTableRequest(std.testing.allocator, "{\"schema\":{\"dynamic_templates\":[{\"name\":\"rank\",\"path_match\":\"rank\",\"mapping\":{\"type\":\"numeric\",\"doc_values\":true}}]}}"),
    );
}

test "table contract rejects caller-managed schema update versions" {
    try std.testing.expectError(
        error.SchemaVersionManagedByBackend,
        parseSchemaUpdateRequest(std.testing.allocator, "{\"version\":7,\"document_schemas\":{}}"),
    );
}

test "table contract rejects caller-managed create schema versions" {
    try std.testing.expectError(
        error.SchemaVersionManagedByBackend,
        parseCreateTableRequest(std.testing.allocator, "{\"schema\":{\"version\":7}}"),
    );
    try std.testing.expectEqual(
        CreateTableRequestErrorDisposition.bad_request,
        classifyCreateTableRequestError(error.SchemaVersionManagedByBackend),
    );
    try std.testing.expectEqualStrings(
        "schema.version is managed by Antfly; omit it",
        createTableRequestErrorMessage(error.SchemaVersionManagedByBackend, "{\"schema\":{\"version\":7}}"),
    );
}

test "table contract normalizes index create request against path name" {
    const config_json = try parseCreateIndexRequest(
        std.testing.allocator,
        "embed_idx",
        "{\"type\":\"embeddings\",\"name\":\"embed_idx\",\"dimension\":3}",
    );
    defer std.testing.allocator.free(config_json);
    try std.testing.expect(std.mem.indexOf(u8, config_json, "\"name\":\"embed_idx\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, config_json, "\"type\":\"embeddings\"") != null);
}

test "table contract preserves embeddings create request fields" {
    const config_json = try parseCreateIndexRequest(
        std.testing.allocator,
        "embed_idx",
        "{\"name\":\"embed_idx\",\"type\":\"embeddings\",\"external\":true,\"dimension\":384}",
    );
    defer std.testing.allocator.free(config_json);
    try std.testing.expect(std.mem.indexOf(u8, config_json, "\"external\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, config_json, "\"dimension\":384") != null);
    try std.testing.expect(std.mem.indexOf(u8, config_json, "\"name\":\"embed_idx\"") != null);
}

test "table contract rejects unsupported index kinds before admission" {
    const supported = [_][]const u8{
        "{\"indexes\":{\"search\":{\"type\":\"full_text\"}}}",
        "{\"indexes\":{\"semantic\":{\"type\":\"embeddings\",\"dimension\":3}}}",
        "{\"indexes\":{\"relations\":{\"type\":\"graph\"}}}",
        "{\"indexes\":{\"features\":{\"type\":\"algebraic\"}}}",
    };
    for (supported) |body| {
        var req = try parseCreateTableRequest(std.testing.allocator, body);
        req.deinit(std.testing.allocator);
    }

    try std.testing.expectError(
        error.InvalidCreateTableRequest,
        parseCreateTableRequest(
            std.testing.allocator,
            "{\"indexes\":{\"legacy\":{\"type\":\"aknn_v0\"}}}",
        ),
    );
    try std.testing.expectError(
        error.InvalidCreateIndexRequest,
        parseCreateIndexRequest(
            std.testing.allocator,
            "legacy",
            "{\"type\":\"aknn_v0\"}",
        ),
    );
    try std.testing.expectError(
        error.InvalidCreateTableRequest,
        parseCreateTableRequest(
            std.testing.allocator,
            "{\"indexes\":{\"bad\":{\"type\":7}}}",
        ),
    );
}

test "table contract rejects non-go full text fields" {
    try std.testing.expectError(
        error.InvalidCreateIndexRequest,
        parseCreateIndexRequest(
            std.testing.allocator,
            "search_idx",
            "{\"name\":\"search_idx\",\"type\":\"full_text\",\"chunk_name\":\"serverless_chunk_preview\"}",
        ),
    );
    try std.testing.expectError(
        error.InvalidCreateTableRequest,
        parseCreateTableRequest(
            std.testing.allocator,
            "{\"indexes\":{\"full_text_index_v1\":{\"type\":\"full_text\",\"chunk_name\":\"serverless_chunk_preview\"}}}",
        ),
    );
}

test "table contract ignores create-table full text entries and preserves typed path metadata" {
    var req = try parseCreateTableRequest(
        std.testing.allocator,
        "{\"description\":\"docs\",\"indexes\":{\"default\":{},\"embed_idx\":{\"type\":\"embeddings\",\"dimension\":384}},\"typed_paths\":{\"numeric\":[\"metrics.score\"],\"keyword\":\"status\"}}",
    );
    defer req.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("docs", req.description.?);
    var indexes = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, req.indexes_json.?, .{});
    defer indexes.deinit();
    try std.testing.expect(indexes.value.object.get("default") == null);
    const full_text = indexes.value.object.get("full_text_index_v0").?.object;
    try std.testing.expectEqualStrings("full_text_index_v0", full_text.get("name").?.string);
    try std.testing.expectEqualStrings("full_text", full_text.get("type").?.string);
    const embedding = indexes.value.object.get("embed_idx").?.object;
    try std.testing.expect(embedding.get("name") == null);
    try std.testing.expectEqualStrings("embeddings", embedding.get("type").?.string);
    try std.testing.expectEqual(@as(i64, 384), embedding.get("dimension").?.integer);
    const typed_paths = indexes.value.object.get("typed_paths").?.object;
    try std.testing.expectEqualStrings("metrics.score", typed_paths.get("numeric").?.array.items[0].string);
    try std.testing.expectEqualStrings("status", typed_paths.get("keyword").?.string);
}

test "table contract accepts public full text create index" {
    const config_json = try parseCreateIndexRequest(
        std.testing.allocator,
        "search_idx",
        "{\"type\":\"full_text\"}",
    );
    defer std.testing.allocator.free(config_json);
    try std.testing.expect(std.mem.indexOf(u8, config_json, "\"name\":\"search_idx\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, config_json, "\"type\":\"full_text\"") != null);

    try std.testing.expectError(
        error.InvalidCreateIndexRequest,
        parseCreateIndexRequest(
            std.testing.allocator,
            "default",
            "{}",
        ),
    );
}

test "table contract accepts public field scoped full text create index" {
    const config_json = try parseCreateIndexRequest(
        std.testing.allocator,
        "category_fts",
        "{\"type\":\"full_text\",\"field\":\"category\",\"fields\":[\"title\",\"body\"]}",
    );
    defer std.testing.allocator.free(config_json);
    try std.testing.expect(std.mem.indexOf(u8, config_json, "\"name\":\"category_fts\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, config_json, "\"field\":\"category\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, config_json, "\"fields\":[\"title\",\"body\"]") != null);
}

test "table contract rejects scalar path metadata as a public index" {
    try std.testing.expectError(
        error.InvalidCreateIndexRequest,
        parseCreateIndexRequest(
            std.testing.allocator,
            "score_idx",
            "{\"type\":\"numeric\",\"path\":\"metrics.score\"}",
        ),
    );
    try std.testing.expectError(
        error.InvalidCreateTableRequest,
        parseCreateTableRequest(
            std.testing.allocator,
            "{\"indexes\":{\"score_idx\":{\"type\":\"keyword\",\"fields\":[\"status\",\"metadata.plan\"]}}}",
        ),
    );
}

test "table contract rejects artifact-backed public full text create index" {
    try std.testing.expectError(
        error.InvalidCreateIndexRequest,
        parseCreateIndexRequest(
            std.testing.allocator,
            "document_text",
            "{\"type\":\"full_text\",\"artifact_name\":\"document_chunks_v1\",\"enrichments\":[{\"name\":\"document_units_v1\",\"kind\":\"asset\",\"field\":\"url\",\"content_type\":\"application/json\",\"producer_json\":\"{\\\"type\\\":\\\"document_extraction\\\",\\\"config\\\":{}}\"},{\"name\":\"document_chunks_v1\",\"kind\":\"chunk\",\"source_artifact_name\":\"document_units_v1\",\"field\":\"text\",\"chunk_size\":512,\"chunk_overlap\":50}]}",
        ),
    );
    try std.testing.expectError(
        error.InvalidCreateIndexRequest,
        parseCreateIndexRequest(
            std.testing.allocator,
            "full_text_index_v1",
            "{}",
        ),
    );
}

test "table contract normalizes public artifact enrichment request" {
    const config_json = try parseArtifactEnrichmentRequest(
        std.testing.allocator,
        "document_chunks_v1",
        "{\"kind\":\"chunk\",\"source_artifact_name\":\"document_units_v1\",\"field\":\"text\",\"chunk_size\":512,\"chunk_overlap\":50,\"full_text_index\":true}",
    );
    defer std.testing.allocator.free(config_json);
    try std.testing.expect(std.mem.indexOf(u8, config_json, "\"name\":\"document_chunks_v1\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, config_json, "\"kind\":\"chunk\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, config_json, "\"full_text_index\":true") != null);

    try std.testing.expectError(
        error.InvalidArtifactEnrichmentRequest,
        parseArtifactEnrichmentRequest(
            std.testing.allocator,
            "document_chunks_v1",
            "{\"name\":\"other\",\"kind\":\"chunk\",\"field\":\"text\",\"chunk_size\":512}",
        ),
    );
    const asset_config_json = try parseArtifactEnrichmentRequest(
        std.testing.allocator,
        "document_units_v1",
        "{\"kind\":\"asset\",\"field\":\"url\",\"full_text_index\":true}",
    );
    defer std.testing.allocator.free(asset_config_json);
    try std.testing.expect(std.mem.indexOf(u8, asset_config_json, "\"name\":\"document_units_v1\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, asset_config_json, "\"kind\":\"asset\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, asset_config_json, "\"full_text_index\":true") != null);
}

test "table contract rejects reserved full text index names on create table" {
    try std.testing.expectError(
        error.InvalidCreateTableRequest,
        parseCreateTableRequest(
            std.testing.allocator,
            "{\"indexes\":{\"full_text_index_v1\":{\"type\":\"embeddings\",\"dimension\":3}}}",
        ),
    );
}

test "table contract rejects unsupported index kinds before catalog admission" {
    try std.testing.expectError(
        error.InvalidCreateTableRequest,
        parseCreateTableRequest(
            std.testing.allocator,
            "{\"indexes\":{\"unsupported_idx\":{\"type\":\"unsupported\"}}}",
        ),
    );
}

test "table contract keeps operational create request failures on the internal error path" {
    try std.testing.expectEqual(
        CreateTableRequestErrorDisposition.bad_request,
        classifyCreateTableRequestError(error.InvalidCreateTableRequest),
    );
    try std.testing.expectEqual(
        CreateTableRequestErrorDisposition.bad_request,
        classifyCreateTableRequestError(error.SyntaxError),
    );
    try std.testing.expectEqual(
        CreateTableRequestErrorDisposition.internal_failure,
        classifyCreateTableRequestError(error.OutOfMemory),
    );
    try std.testing.expectEqual(
        CreateTableRequestErrorDisposition.internal_failure,
        classifyCreateTableRequestError(error.EntropyUnavailable),
    );
    try std.testing.expectEqual(
        CreateTableRequestErrorDisposition.internal_failure,
        classifyCreateTableRequestError(error.Canceled),
    );

    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    try std.testing.expectError(
        error.OutOfMemory,
        parseCreateTableRequest(failing.allocator(), "{}"),
    );
}

test "table contract normalizes table-definition indexes with versioned full text entries" {
    var parsed = try std.json.parseFromSlice(
        std.json.Value,
        std.testing.allocator,
        "{\"full_text_index_v0\":{\"type\":\"full_text\"},\"full_text_index_v1\":{\"type\":\"full_text\"},\"semantic_idx\":{\"type\":\"embeddings\",\"dimension\":3}}",
        .{},
    );
    defer parsed.deinit();

    const normalized = try normalizeTableDefinitionIndexesValueAlloc(std.testing.allocator, parsed.value);
    defer std.testing.allocator.free(normalized);

    try std.testing.expect(std.mem.indexOf(u8, normalized, "\"full_text_index_v0\":{\"name\":\"full_text_index_v0\",\"type\":\"full_text\"}") != null);
    try std.testing.expect(std.mem.indexOf(u8, normalized, "\"full_text_index_v1\":{\"name\":\"full_text_index_v1\",\"type\":\"full_text\"}") != null);
    try std.testing.expect(std.mem.indexOf(u8, normalized, "\"semantic_idx\":{\"name\":\"semantic_idx\",\"type\":\"embeddings\",\"dimension\":3}") != null);
}

test "table contract ignores full text indexes with name field in create table request" {
    // Matches e2e test_table_create_table_ignores_user_full_text_index_entries payload
    var req = try parseCreateTableRequest(
        std.testing.allocator,
        "{\"num_shards\":1,\"indexes\":{\"search_idx\":{\"name\":\"search_idx\",\"type\":\"full_text\"},\"embed_idx\":{\"name\":\"embed_idx\",\"type\":\"embeddings\",\"external\":true,\"dimension\":3}}}",
    );
    defer req.deinit(std.testing.allocator);
    try std.testing.expect(std.mem.indexOf(u8, req.indexes_json.?, "\"full_text_index_v0\":{\"name\":\"full_text_index_v0\",\"type\":\"full_text\"}") != null);
    try std.testing.expect(std.mem.indexOf(u8, req.indexes_json.?, "\"search_idx\"") == null);
    try std.testing.expect(std.mem.indexOf(u8, req.indexes_json.?, "\"embed_idx\"") != null);
}

test "table contract skips arbitrary public full text names in table-definition indexes" {
    var parsed = try std.json.parseFromSlice(
        std.json.Value,
        std.testing.allocator,
        "{\"search_idx\":{\"type\":\"full_text\"}}",
        .{},
    );
    defer parsed.deinit();

    const normalized = try normalizeTableDefinitionIndexesValueAlloc(std.testing.allocator, parsed.value);
    defer std.testing.allocator.free(normalized);
    try std.testing.expect(std.mem.indexOf(u8, normalized, "\"search_idx\"") == null);
    try std.testing.expect(std.mem.indexOf(u8, normalized, "\"full_text_index_v0\"") != null);
}

test "table contract schema update error message explains public sortable replacement for doc values" {
    try std.testing.expectEqualStrings(
        "invalid schema update request: doc_values is internal; use sortable: true on scalar mappings",
        schemaUpdateRequestErrorMessage(error.InvalidSchemaUpdateRequest, "{\"dynamic_templates\":[{\"mapping\":{\"type\":\"keyword\",\"doc_values\":true}}]}"),
    );
    try std.testing.expectEqualStrings(
        "invalid schema update request",
        schemaUpdateRequestErrorMessage(error.InvalidSchemaUpdateRequest, "{\"dynamic_templates\":[{\"mapping\":{\"type\":\"keyword\",\"sortable\":true}}]}"),
    );
    try std.testing.expectEqualStrings(
        "invalid create table request: schema doc_values is internal; use sortable: true on scalar mappings",
        createTableRequestErrorMessage(error.InvalidCreateTableSchemaRequest, "{\"schema\":{\"dynamic_templates\":[{\"mapping\":{\"type\":\"keyword\",\"doc_values\":true}}]}}"),
    );
    try std.testing.expectEqualStrings(
        "invalid create table request",
        createTableRequestErrorMessage(error.InvalidCreateTableSchemaRequest, "{\"schema\":{\"dynamic_templates\":[{\"mapping\":{\"type\":\"keyword\",\"sortable\":true}}]}}"),
    );
}
