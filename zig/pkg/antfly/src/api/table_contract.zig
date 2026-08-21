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
const ant_json = @import("antfly-json");
const metadata_openapi = @import("antfly_metadata_openapi");
const tables_api = @import("tables.zig");
const indexes_api = @import("indexes.zig");
const coverage_policy = @import("coverage_policy.zig");
const public_index_contract = @import("public_index_contract.zig");

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
        // Raw public fields were validated above. The compatibility parser
        // adds private coverage incarnations, so re-running the public
        // allow-list against its normalized output would reject trusted
        // metadata that the caller never supplied.
        try validateCreateTableArtifactEnrichments(
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
    try validateCreateTableArtifactEnrichments(alloc, req.indexes_json.?);

    if (raw_root.get("schema")) |schema_value| {
        if (schema_value != .null) {
            const raw_schema = try stringifyJsonAlloc(alloc, schema_value);
            defer alloc.free(raw_schema);
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
            if (isArtifactBackedFullTextIndex(entry.value_ptr.object)) {
                if (isReservedFullTextIndexName(entry.key_ptr.*)) return error.InvalidCreateTableRequest;
            } else {
                if (!isReservedFullTextIndexName(entry.key_ptr.*)) continue;
                saw_full_text = true;
            }
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

pub fn schemaUpdateRequestErrorMessage(err: anyerror, body: []const u8) []const u8 {
    if (err == error.SchemaVersionManagedByBackend) {
        return "schema.version is managed by Antfly; omit it";
    }
    if (std.mem.indexOf(u8, body, "\"doc_values\"") != null) {
        return "invalid schema update request: doc_values is internal; use sortable: true on scalar mappings";
    }
    return "invalid schema update request";
}

pub fn createTableRequestErrorMessage(err: anyerror, body: []const u8) []const u8 {
    if (err == error.SchemaVersionManagedByBackend) {
        return "schema.version is managed by Antfly; omit it";
    }
    if (std.mem.indexOf(u8, body, "\"doc_values\"") != null) {
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

    const normalized = normalizeIndexConfigJson(alloc, root, index_name, .{
        .include_name = true,
        .default_type = true,
    }) catch |err| switch (err) {
        error.InvalidCreateIndexRequest => return error.InvalidCreateIndexRequest,
        else => return err,
    };
    errdefer alloc.free(normalized);
    indexes_api.validateArtifactEnrichmentsForTableIndexesJson(alloc, normalized) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.InvalidCreateIndexRequest,
    };
    return normalized;
}

fn validateCreateTableArtifactEnrichments(alloc: std.mem.Allocator, indexes_json: []const u8) !void {
    indexes_api.validateArtifactEnrichmentsForTableIndexesJson(alloc, indexes_json) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.InvalidCreateTableRequest,
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
    const index_type = public_index_contract.parseKind(extractPublicIndexType(object) orelse "full_text") orelse
        return error.InvalidCreateIndexRequest;
    try validatePublicInlineArtifactEnrichments(object);
    try validatePublicIndexFields(object, index_type);
    try validatePublicNestedIndexFields(object, index_type);
}

fn validatePublicNestedIndexFields(object: anytype, index_type: public_index_contract.Kind) !void {
    const Object = @TypeOf(object);
    if (index_type == .embeddings) {
        const chunker = if (@hasField(Object, "map")) object.map.get("chunker") else object.get("chunker");
        if (chunker) |value| {
            if (value != .null) try validatePublicCreatedShape(value, .chunker);
        }
        const execution = if (@hasField(Object, "map")) object.map.get("execution") else object.get("execution");
        if (execution) |value| {
            if (value != .null) try validatePublicCreatedShape(value, .index_execution);
        }
        return;
    }
    if (index_type != .graph) return;

    const source = if (@hasField(Object, "map")) object.map.get("source") else object.get("source");
    if (source) |value| {
        if (value != .null) try validatePublicCreatedShape(value, .graph_source);
    }

    const artifact = if (@hasField(Object, "map")) object.map.get("artifact") else object.get("artifact");
    if (artifact) |value| {
        if (value != .null) try validatePublicGraphArtifact(value);
    }

    const graph_shapes = .{
        .{ "nodes", public_index_contract.CreatedObjectShape.graph_nodes },
        .{ "edge", public_index_contract.CreatedObjectShape.graph_edge },
        .{ "context", public_index_contract.CreatedObjectShape.graph_context },
        .{ "algebraic_planning", public_index_contract.CreatedObjectShape.graph_algebraic_planning },
    };
    inline for (graph_shapes) |field_shape| {
        const value = if (@hasField(Object, "map")) object.map.get(field_shape[0]) else object.get(field_shape[0]);
        if (value) |nested| {
            if (nested != .null) try validatePublicCreatedShape(nested, field_shape[1]);
        }
    }

    const edge_types = if (@hasField(Object, "map")) object.map.get("edge_types") else object.get("edge_types");
    if (edge_types) |value| {
        if (value != .null) try validatePublicCreatedShape(value, .edge_types);
    }

    const resolvers = if (@hasField(Object, "map")) object.map.get("resolvers") else object.get("resolvers");
    if (resolvers) |value| {
        if (value != .null) try validatePublicCreatedShape(value, .graph_resolvers);
    }
}

fn validatePublicCreatedShape(value: std.json.Value, shape: public_index_contract.CreatedObjectShape) !void {
    if (!public_index_contract.createdValueMatchesShape(shape, value)) return error.InvalidCreateIndexRequest;
    switch (value) {
        .object => |object| {
            var it = object.iterator();
            while (it.next()) |entry| {
                if (!public_index_contract.isAllowedCreatedObjectField(shape, entry.key_ptr.*))
                    return error.InvalidCreateIndexRequest;
                // Generated SDKs use null for absent optional members. The
                // canonicalizer drops them, so admission must treat them as
                // omission before applying the non-null wire type contract.
                if (entry.value_ptr.* == .null) continue;
                if (!public_index_contract.createdFieldValueMatches(shape, entry.key_ptr.*, entry.value_ptr.*))
                    return error.InvalidCreateIndexRequest;
                if (shape == .execution_policy and entry.value_ptr.integer <= 0)
                    return error.InvalidCreateIndexRequest;
                const child_shape = public_index_contract.createdObjectShapeForChild(shape, entry.key_ptr.*);
                if (child_shape != .unrestricted) try validatePublicCreatedShape(entry.value_ptr.*, child_shape);
            }
        },
        .array => |array| {
            const item_shape = public_index_contract.createdObjectShapeForArrayItem(shape);
            for (array.items) |item| try validatePublicCreatedShape(item, item_shape);
        },
        else => unreachable,
    }
}

fn validatePublicGraphArtifact(value: std.json.Value) !void {
    if (!public_index_contract.createdValueMatchesShape(.graph_artifact, value))
        return error.InvalidCreateIndexRequest;
    var it = value.object.iterator();
    while (it.next()) |entry| {
        if (!public_index_contract.isAllowedGraphArtifactRequestField(entry.key_ptr.*))
            return error.InvalidCreateIndexRequest;
        if (entry.value_ptr.* == .null) continue;
        if (std.mem.eql(u8, entry.key_ptr.*, "producer_json")) {
            if (entry.value_ptr.* != .object) return error.InvalidCreateIndexRequest;
        } else if (!public_index_contract.createdFieldValueMatches(.graph_artifact, entry.key_ptr.*, entry.value_ptr.*)) {
            return error.InvalidCreateIndexRequest;
        }
    }
}

fn validatePublicInlineArtifactEnrichments(object: anytype) !void {
    const Object = @TypeOf(object);
    const enrichments = if (@hasField(Object, "map"))
        object.map.get("enrichments")
    else
        object.get("enrichments");
    const value = enrichments orelse return;
    if (value == .null) return;
    if (value != .array) return error.InvalidCreateIndexRequest;
    for (value.array.items) |item| {
        if (!public_index_contract.createdValueMatchesShape(.enrichment, item))
            return error.InvalidCreateIndexRequest;
        if (item != .object) return error.InvalidCreateIndexRequest;
        validatePublicArtifactEnrichmentObject(item.object) catch
            return error.InvalidCreateIndexRequest;
    }
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
            try validatePublicArtifactEnrichmentField(entry.key_ptr.*, entry.value_ptr.*);
        }
    } else {
        var it = object.iterator();
        while (it.next()) |entry| {
            try validatePublicArtifactEnrichmentField(entry.key_ptr.*, entry.value_ptr.*);
        }
    }
}

fn validatePublicArtifactEnrichmentField(field: []const u8, value: std.json.Value) !void {
    if (!public_index_contract.isAllowedEnrichmentRequestField(field)) return error.InvalidArtifactEnrichmentRequest;
    if (value == .null) return;
    if (std.mem.eql(u8, field, "producer_json")) {
        if (value != .string) return error.InvalidArtifactEnrichmentRequest;
        return;
    }
    if (!public_index_contract.createdFieldValueMatches(.enrichment, field, value))
        return error.InvalidArtifactEnrichmentRequest;
    if (std.mem.eql(u8, field, "execution")) {
        validatePublicCreatedShape(value, .execution_policy) catch return error.InvalidArtifactEnrichmentRequest;
    }
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

fn validatePublicIndexFields(object: anytype, index_type: public_index_contract.Kind) !void {
    const Object = @TypeOf(object);
    if (@hasField(Object, "map")) {
        var it = object.map.iterator();
        while (it.next()) |entry| {
            if (!public_index_contract.isAllowedConfigField(index_type, entry.key_ptr.*)) return error.InvalidCreateIndexRequest;
            if (entry.value_ptr.* != .null and
                !public_index_contract.rootFieldValueMatches(index_type, entry.key_ptr.*, entry.value_ptr.*))
                return error.InvalidCreateIndexRequest;
        }
        return;
    }

    var it = object.iterator();
    while (it.next()) |entry| {
        if (!public_index_contract.isAllowedConfigField(index_type, entry.key_ptr.*)) return error.InvalidCreateIndexRequest;
        if (entry.value_ptr.* != .null and
            !public_index_contract.rootFieldValueMatches(index_type, entry.key_ptr.*, entry.value_ptr.*))
            return error.InvalidCreateIndexRequest;
    }
}

fn isArtifactBackedFullTextIndex(object: anytype) bool {
    const Object = @TypeOf(object);
    if (@hasField(Object, "map")) {
        return object.map.contains("artifact_name") or object.map.contains("enrichments");
    }
    return object.contains("artifact_name") or object.contains("enrichments");
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
        if (isPublicFullTextType(extractPublicIndexType(entry.value_ptr.object) orelse "full_text") and
            !isArtifactBackedFullTextIndex(entry.value_ptr.object)) continue;
        if (!first) try out.append(alloc, ',');
        first = false;
        try appendJsonString(alloc, &out, entry.key_ptr.*);
        try out.append(alloc, ':');
        try out.appendSlice(alloc, normalized);
    }
    try out.append(alloc, '}');
    const normalized = try out.toOwnedSlice(alloc);
    defer alloc.free(normalized);
    // The typed OpenAPI path must establish the same durable incarnation
    // contract as the compatibility parser in tables.zig. Without it, inline
    // embedding indexes are provisioned with a derived runtime identity while
    // the catalog retains an identity-less config; readiness then correctly
    // rejects every otherwise healthy shard as a config mismatch.
    return try coverage_policy.withMissingIncarnationsAlloc(alloc, normalized);
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

fn appendField(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    key: []const u8,
    value: std.json.Value,
    first: *bool,
) !void {
    // Generated SDKs encode absent optional fields as JSON null. Treat those
    // exactly like omission so typed and raw callers converge on one stored
    // configuration.
    if (value == .null) return;
    if (!first.*) try out.append(alloc, ',');
    first.* = false;
    const encoded_key = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(key, .{})});
    defer alloc.free(encoded_key);
    try out.appendSlice(alloc, encoded_key);
    try out.append(alloc, ':');
    try appendCanonicalPublicValue(alloc, out, value, key);
}

fn appendCanonicalPublicValue(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    value: std.json.Value,
    field_name: ?[]const u8,
) !void {
    // Opaque producer documents are write-only and provider-significant, so
    // preserve their internal null values while canonicalizing public config.
    if (field_name) |name| {
        if (std.mem.eql(u8, name, "producer_json")) {
            const encoded = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})});
            defer alloc.free(encoded);
            return out.appendSlice(alloc, encoded);
        }
    }

    switch (value) {
        .object => |object| {
            try out.append(alloc, '{');
            var first = true;
            var it = object.iterator();
            while (it.next()) |entry| {
                if (entry.value_ptr.* == .null) continue;
                if (!first) try out.append(alloc, ',');
                first = false;
                try appendJsonString(alloc, out, entry.key_ptr.*);
                try out.append(alloc, ':');
                try appendCanonicalPublicValue(alloc, out, entry.value_ptr.*, entry.key_ptr.*);
            }
            try out.append(alloc, '}');
        },
        .array => |array| {
            try out.append(alloc, '[');
            for (array.items, 0..) |item, index| {
                if (index > 0) try out.append(alloc, ',');
                try appendCanonicalPublicValue(alloc, out, item, null);
            }
            try out.append(alloc, ']');
        },
        else => {
            const encoded = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})});
            defer alloc.free(encoded);
            try out.appendSlice(alloc, encoded);
        },
    }
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
        "{\"num_shards\":1,\"description\":null,\"indexes\":null,\"schema\":null,\"replication_sources\":null}",
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
    try ant_json.testing.expectEqualJsonText(
        std.testing.allocator,
        "{\"name\":\"embed_idx\",\"type\":\"embeddings\",\"dimension\":3}",
        config_json,
    );
}

test "table contract preserves embeddings create request fields" {
    const config_json = try parseCreateIndexRequest(
        std.testing.allocator,
        "embed_idx",
        "{\"name\":\"embed_idx\",\"type\":\"embeddings\",\"external\":true,\"dimension\":384}",
    );
    defer std.testing.allocator.free(config_json);
    try ant_json.testing.expectEqualJsonText(
        std.testing.allocator,
        "{\"name\":\"embed_idx\",\"type\":\"embeddings\",\"external\":true,\"dimension\":384}",
        config_json,
    );
}

test "table contract public response omits unknown nested provider fields" {
    const config_json = try parseCreateIndexRequest(
        std.testing.allocator,
        "embed_idx",
        "{\"type\":\"embeddings\",\"dimension\":384,\"embedder\":{\"provider\":\"openai\",\"model\":\"text-embedding-3-small\",\"client_value\":\"private-unknown-field\",\"settings\":{\"opaque\":\"private-nested-value\"}}}",
    );
    defer std.testing.allocator.free(config_json);

    const response = try indexes_api.encodeCreatedIndexConfig(std.testing.allocator, "embed_idx", config_json);
    defer std.testing.allocator.free(response);
    try ant_json.testing.expectEqualJsonText(
        std.testing.allocator,
        "{\"name\":\"embed_idx\",\"type\":\"embeddings\",\"dimension\":384,\"embedder\":{\"provider\":\"openai\",\"model\":\"text-embedding-3-small\"}}",
        response,
    );
}

test "table contract canonicalizes generated optional null fields" {
    var request = try parseCreateTableRequest(
        std.testing.allocator,
        "{\"num_shards\":1,\"description\":null,\"indexes\":{\"title_body\":{\"description\":null,\"version\":null,\"enrichments\":null,\"coverage_policy\":null,\"external\":null,\"sparse\":null,\"dimension\":3,\"field\":null,\"template\":\"{{title}} {{body}}\",\"embedder\":{\"provider\":\"antfly\",\"model\":\"antfly-embed-v1\",\"api_url\":\"http://127.0.0.1:8080/ai/v1\",\"dimensions\":null},\"summarizer\":null,\"chunker\":null,\"execution\":null,\"type\":\"embeddings\"}},\"schema\":null,\"replication_sources\":null}",
    );
    defer request.deinit(std.testing.allocator);
    var indexes = try std.json.parseFromSlice(std.json.Value, std.testing.allocator, request.indexes_json.?, .{});
    defer indexes.deinit();
    const embedding = indexes.value.object.getPtr("title_body") orelse return error.TestUnexpectedResult;
    switch (embedding.*) {
        .object => |*object| _ = object.swapRemove(coverage_policy.incarnation_field),
        else => return error.TestUnexpectedResult,
    }
    try ant_json.testing.expectEqualJsonValue(
        std.testing.allocator,
        "{\"full_text_index_v0\":{\"name\":\"full_text_index_v0\",\"type\":\"full_text\"},\"title_body\":{\"name\":\"title_body\",\"type\":\"embeddings\",\"dimension\":3,\"template\":\"{{title}} {{body}}\",\"embedder\":{\"provider\":\"antfly\",\"model\":\"antfly-embed-v1\",\"api_url\":\"http://127.0.0.1:8080/ai/v1\"}}}",
        indexes.value,
    );
}

test "table contract preserves typed artifact-backed graph configuration" {
    const body =
        \\{"type":"graph","source":{"kind":"artifact","artifact":"relations_v1","path":"$.relations[*]","format":"extraction_relation","mention_edge_type":"mentions"},"artifact":{"name":"relations_v1","kind":"asset","field":"relations","content_type":"application/json","producer_json":{"type":"document_extraction","api_key":"write-only"}},"nodes":{"model":"document","source":"{{ _doc.key }}","target":"{{ _item.target.text }}"},"edge":{"type":"{{ _item.predicate }}","weight":0.75,"metadata":{"source":"{{ _item.source }}"}},"context":{"doc_fields":["title","body"]},"algebraic_planning":{"bounded_traversal":{"law":"provenance_semiring","enabled":true}},"edge_types":[{"name":"mentions"}],"resolvers":[{"name":"kg","table":"entities","source_artifact":"relations_v1","resolution_artifact":"resolution_v1","key_template":"{{ lower _entity.label }}/{{ slug _entity.text }}","candidate_search":"prefix","config_generation":1}]}
    ;
    const config_json = try parseCreateIndexRequest(std.testing.allocator, "relations_graph", body);
    defer std.testing.allocator.free(config_json);
    try ant_json.testing.expectSubsetJsonText(
        std.testing.allocator,
        "{\"name\":\"relations_graph\",\"type\":\"graph\",\"source\":{\"artifact\":\"relations_v1\"},\"artifact\":{\"name\":\"relations_v1\"},\"nodes\":{\"model\":\"document\",\"target\":\"{{ _item.target.text }}\"},\"edge\":{\"weight\":0.75},\"context\":{\"doc_fields\":[\"title\",\"body\"]},\"algebraic_planning\":{\"bounded_traversal\":{\"law\":\"provenance_semiring\"}},\"resolvers\":[{\"name\":\"kg\",\"candidate_search\":\"prefix\"}]}",
        config_json,
    );

    var table_req = try parseCreateTableRequest(
        std.testing.allocator,
        "{\"indexes\":{\"relations_graph\":" ++ body ++ "}}",
    );
    defer table_req.deinit(std.testing.allocator);
    try ant_json.testing.expectSubsetJsonText(
        std.testing.allocator,
        "{\"relations_graph\":{\"type\":\"graph\",\"source\":{\"artifact\":\"relations_v1\"},\"nodes\":{\"model\":\"document\"},\"edge\":{\"weight\":0.75},\"context\":{\"doc_fields\":[\"title\",\"body\"]},\"algebraic_planning\":{\"bounded_traversal\":{\"law\":\"provenance_semiring\"}},\"resolvers\":[{\"name\":\"kg\"}]}}",
        table_req.indexes_json.?,
    );
}

test "table contract rejects unknown fields in closed nested index objects" {
    const invalid_requests = [_][]const u8{
        \\{"type":"graph","source":{"kind":"artifact","artifact":"relations_v1","client_value":"private"}}
        ,
        \\{"type":"graph","artifact":{"name":"relations_v1","kind":"asset","client_value":"private"}}
        ,
        \\{"type":"graph","resolvers":[{"name":"kg","table":"entities","source_artifact":"relations_v1","resolution_artifact":"resolution_v1","key_template":"{{label}}","client_value":"private"}]}
        ,
        \\{"type":"graph","edge_types":[{"name":"mentions","client_value":"private"}]}
        ,
        \\{"type":"graph","edge_types":[{"name":"mentions","required_metadata":{"client_value":"private"}}]}
        ,
        \\{"type":"graph","edge_types":[{"topology":"graph"}]}
        ,
        \\{"type":"graph","source":{"kind":"artifact","artifact":"relations_v1","path":{"client_value":"private"}}}
        ,
        \\{"type":"graph","resolvers":[{"name":"kg","table":"entities","source_artifact":"relations_v1","resolution_artifact":"resolution_v1","key_template":"{{label}}","candidate_limit":{"client_value":"private"}}]}
        ,
        \\{"type":"graph","nodes":{"model":"document","client_value":"private"}}
        ,
        \\{"type":"graph","nodes":{"model":"unsupported"}}
        ,
        \\{"type":"graph","edge":{"weight":{"client_value":"private"}}}
        ,
        \\{"type":"graph","context":{"doc_fields":["title",""]}}
        ,
        \\{"type":"graph","algebraic_planning":{"bounded_traversal":{"law":"min_plus_semiring"}}}
        ,
        \\{"type":"graph","algebraic_planning":{"bounded_traversal":{"law":"provenance_semiring","client_value":"private"}}}
        ,
        \\{"type":"embeddings","dimension":384,"chunker":{"provider":"antfly","model":"fixed","client_value":"private"}}
        ,
        \\{"type":"embeddings","dimension":384,"chunker":{"provider":"antfly","model":"fixed","text":{"target_tokens":500,"client_value":"private"}}}
        ,
        \\{"type":"embeddings","dimension":384,"chunker":{"provider":"antfly","model":{"client_value":"private"}}}
        ,
        \\{"type":"embeddings","dimension":384,"chunker":{"model":"fixed"}}
        ,
        \\{"type":"embeddings","dimension":384,"execution":{"embedding":{"batch_items":32,"client_value":"private"}}}
        ,
        \\{"type":"embeddings","dimension":384,"execution":{"embedding":{"batch_items":{"client_value":"private"}}}}
        ,
        \\{"type":"embeddings","dimension":384,"execution":{"client_value":{"batch_items":32}}}
        ,
        \\{"type":"embeddings","dimension":384,"template":{"client_value":"private"}}
        ,
    };
    for (invalid_requests) |request| {
        try std.testing.expectError(
            error.InvalidCreateIndexRequest,
            parseCreateIndexRequest(std.testing.allocator, "test_idx", request),
        );
    }
}

test "table contract treats nullable nested index fields as omitted" {
    const config_json = try parseCreateIndexRequest(
        std.testing.allocator,
        "relations_graph",
        "{\"type\":\"graph\",\"source\":null,\"artifact\":null,\"nodes\":null,\"edge\":null,\"context\":null,\"algebraic_planning\":null,\"edge_types\":null,\"resolvers\":null}",
    );
    defer std.testing.allocator.free(config_json);
    try ant_json.testing.expectEqualJsonText(
        std.testing.allocator,
        "{\"name\":\"relations_graph\",\"type\":\"graph\"}",
        config_json,
    );

    const embedding_json = try parseCreateIndexRequest(
        std.testing.allocator,
        "semantic_chunks",
        "{\"type\":\"embeddings\",\"dimension\":384,\"chunker\":{\"provider\":\"antfly\",\"model\":\"fixed\",\"text\":null,\"audio\":null},\"execution\":null}",
    );
    defer std.testing.allocator.free(embedding_json);
    try ant_json.testing.expectEqualJsonText(
        std.testing.allocator,
        "{\"name\":\"semantic_chunks\",\"type\":\"embeddings\",\"dimension\":384,\"chunker\":{\"provider\":\"antfly\",\"model\":\"fixed\"}}",
        embedding_json,
    );
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

test "table contract ignores create-table full text entries and preserves non-full-text indexes" {
    var req = try parseCreateTableRequest(
        std.testing.allocator,
        "{\"description\":\"docs\",\"indexes\":{\"default\":{},\"embed_idx\":{\"type\":\"embeddings\",\"dimension\":384}}}",
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
    try std.testing.expect(coverage_policy.incarnation(.{ .object = embedding }) != null);
}

test "table contract accepts public full text create index" {
    const config_json = try parseCreateIndexRequest(
        std.testing.allocator,
        "search_idx",
        "{\"type\":\"full_text\"}",
    );
    defer std.testing.allocator.free(config_json);
    try ant_json.testing.expectEqualJsonText(
        std.testing.allocator,
        "{\"name\":\"search_idx\",\"type\":\"full_text\"}",
        config_json,
    );

    try std.testing.expectError(
        error.InvalidCreateIndexRequest,
        parseCreateIndexRequest(
            std.testing.allocator,
            "default",
            "{}",
        ),
    );
}

test "table contract preserves artifact-backed public full text indexes" {
    const artifact_index =
        "{\"type\":\"full_text\",\"field\":\"text\",\"artifact_name\":\"document_chunks_v1\",\"enrichments\":[{\"name\":\"document_units_v1\",\"kind\":\"asset\",\"field\":\"url\",\"content_type\":\"application/json\",\"producer_json\":\"{\\\"type\\\":\\\"document_extraction\\\",\\\"config\\\":{}}\"},{\"name\":\"document_chunks_v1\",\"kind\":\"chunk\",\"source_artifact_name\":\"document_units_v1\",\"field\":\"text\",\"chunk_size\":512,\"chunk_overlap\":50}]}";
    const config_json = try parseCreateIndexRequest(std.testing.allocator, "document_text", artifact_index);
    defer std.testing.allocator.free(config_json);
    try std.testing.expect(std.mem.indexOf(u8, config_json, "\"artifact_name\":\"document_chunks_v1\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, config_json, "\"enrichments\"") != null);

    var table_req = try parseCreateTableRequest(
        std.testing.allocator,
        "{\"indexes\":{\"document_text\":" ++ artifact_index ++ "}}",
    );
    defer table_req.deinit(std.testing.allocator);
    try std.testing.expect(std.mem.indexOf(u8, table_req.indexes_json.?, "\"document_text\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, table_req.indexes_json.?, "\"artifact_name\":\"document_chunks_v1\"") != null);

    try std.testing.expectError(
        error.InvalidCreateIndexRequest,
        parseCreateIndexRequest(
            std.testing.allocator,
            "full_text_index_v1",
            "{}",
        ),
    );
}

test "table contract rejects invalid inline artifact enrichments before admission" {
    try std.testing.expectError(
        error.InvalidCreateTableRequest,
        parseCreateTableRequest(
            std.testing.allocator,
            "{\"indexes\":{\"document_text\":{\"type\":\"full_text\",\"artifact_name\":\"chunks\",\"enrichments\":[{\"name\":\"chunks\",\"kind\":\"chunk\",\"field\":\"text\"}]}}}",
        ),
    );
    try std.testing.expectError(
        error.InvalidCreateTableRequest,
        parseCreateTableRequest(
            std.testing.allocator,
            "{\"indexes\":{\"document_text\":{\"type\":\"full_text\",\"enrichments\":\"not-an-array\"}}}",
        ),
    );
    try std.testing.expectError(
        error.InvalidCreateIndexRequest,
        parseCreateIndexRequest(
            std.testing.allocator,
            "document_text",
            "{\"type\":\"full_text\",\"enrichments\":[false]}",
        ),
    );
    try std.testing.expectError(
        error.InvalidCreateIndexRequest,
        parseCreateIndexRequest(
            std.testing.allocator,
            "document_text",
            "{\"type\":\"full_text\",\"enrichments\":[{\"name\":\"chunks\",\"kind\":\"chunk\",\"field\":\"text\",\"chunk_size\":512,\"chunk_overalp\":50}]}",
        ),
    );
    try std.testing.expectError(
        error.InvalidCreateIndexRequest,
        parseCreateIndexRequest(
            std.testing.allocator,
            "document_text",
            "{\"type\":\"full_text\",\"enrichments\":[{\"name\":\"chunks\",\"kind\":\"chunk\",\"field\":\"text\",\"chunk_size\":512,\"execution\":{\"batch_itmes\":4}}]}",
        ),
    );
    try std.testing.expectError(
        error.InvalidCreateIndexRequest,
        parseCreateIndexRequest(
            std.testing.allocator,
            "document_text",
            "{\"type\":\"full_text\",\"enrichments\":[{\"name\":\"units\",\"kind\":\"asset\",\"field\":\"url\",\"producer_json\":\"{\"}]}",
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

test "table contract rejects unknown fields for every public index variant" {
    const invalid_requests = [_][]const u8{
        "{\"type\":\"full_text\",\"unknown\":true}",
        "{\"type\":\"embeddings\",\"external\":true,\"dimension\":384,\"producer_json\":\"{}\"}",
        "{\"type\":\"graph\",\"unknown\":true}",
        "{\"type\":\"algebraic\",\"unknown\":true}",
    };
    for (invalid_requests) |request| {
        try std.testing.expectError(
            error.InvalidCreateIndexRequest,
            parseCreateIndexRequest(std.testing.allocator, "test_idx", request),
        );
    }

    const valid_with_common_fields = try parseCreateIndexRequest(
        std.testing.allocator,
        "embed_idx",
        "{\"type\":\"embeddings\",\"description\":\"semantic search\",\"version\":1,\"external\":true,\"dimension\":384}",
    );
    defer std.testing.allocator.free(valid_with_common_fields);
    try ant_json.testing.expectEqualJsonText(
        std.testing.allocator,
        "{\"name\":\"embed_idx\",\"type\":\"embeddings\",\"description\":\"semantic search\",\"version\":1,\"external\":true,\"dimension\":384}",
        valid_with_common_fields,
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
