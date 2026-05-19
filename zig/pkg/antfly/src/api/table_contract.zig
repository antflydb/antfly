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
const tables_api = @import("tables.zig");

fn stringifyJsonAlloc(alloc: std.mem.Allocator, value: anytype) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})});
}

pub fn parseCreateTableRequest(alloc: std.mem.Allocator, body: []const u8) !tables_api.CreateTableRequest {
    if (body.len == 0) return .{};

    // Use typed OpenAPI parsing for scalar fields (num_shards, description, schema,
    // replication_sources). For indexes, parse from the raw body to preserve
    // type-specific fields (external, dimension, edge_types, etc.) that the
    // generated IndexConfig struct doesn't capture.
    var parsed = metadata_openapi.server.parseCreateTableBody(alloc, body) catch {
        return tables_api.parseCreateTableRequest(alloc, body);
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

    // Extract indexes from raw body to preserve all fields.
    var raw_parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{});
    defer raw_parsed.deinit();
    const raw_root = switch (raw_parsed.value) {
        .object => |object| object,
        else => return error.InvalidCreateTableRequest,
    };
    if (raw_root.get("indexes")) |indexes_value| {
        if (indexes_value != .null)
            req.indexes_json = try normalizeCreateTableIndexesFromValue(alloc, indexes_value)
        else
            req.indexes_json = try alloc.dupe(u8, tables_api.default_indexes_json);
    } else {
        req.indexes_json = try alloc.dupe(u8, tables_api.default_indexes_json);
    }

    if (parsed.value.schema) |schema| {
        const raw_schema = try stringifyJsonAlloc(alloc, schema);
        defer alloc.free(raw_schema);
        req.schema_json = tables_api.parseSchemaUpdateRequest(alloc, raw_schema) catch |err| switch (err) {
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
    // Pass the raw body directly to preserve x-antfly-* extension properties
    // that would be lost if round-tripped through the typed OpenAPI TableSchema struct.
    return try tables_api.parseSchemaUpdateRequest(alloc, body);
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
    if (isReservedFullTextIndexName(index_name)) return error.InvalidCreateIndexRequest;
    if (std.mem.eql(u8, extractPublicIndexType(root) orelse "full_text", "full_text")) {
        return error.InvalidCreateIndexRequest;
    }

    return normalizeIndexConfigJson(alloc, root, index_name, .{
        .include_name = true,
        .default_type = true,
    }) catch |err| switch (err) {
        error.InvalidCreateIndexRequest => error.InvalidCreateIndexRequest,
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
    const index_type = extractPublicIndexType(object) orelse "full_text";
    if (std.mem.eql(u8, index_type, "full_text")) {
        try validatePublicFullTextIndexObject(object);
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
        std.mem.eql(u8, field_name, "mem_only");
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
        .indexes_json = try std.testing.allocator.dupe(u8, "{\"full_text_index_v0\":{\"type\":\"full_text\"}}"),
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
        error.InvalidCreateTableRequest,
        parseCreateTableRequest(std.testing.allocator, "{\"schema\":{\"ttl_duration_ns\":-1}}"),
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
    try std.testing.expect(std.mem.indexOf(u8, req.indexes_json.?, "\"full_text_index_v0\":{\"type\":\"full_text\"}") != null);
    try std.testing.expect(std.mem.indexOf(u8, req.indexes_json.?, "\"default\"") == null);
    try std.testing.expect(std.mem.indexOf(u8, req.indexes_json.?, "\"embed_idx\":{\"type\":\"embeddings\",\"dimension\":384}") != null);
}

test "table contract rejects public full text create index" {
    try std.testing.expectError(
        error.InvalidCreateIndexRequest,
        parseCreateIndexRequest(
            std.testing.allocator,
            "search_idx",
            "{\"type\":\"full_text\"}",
        ),
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

test "table contract rejects reserved full text index names on create table" {
    try std.testing.expectError(
        error.InvalidCreateTableRequest,
        parseCreateTableRequest(
            std.testing.allocator,
            "{\"indexes\":{\"full_text_index_v1\":{\"type\":\"embeddings\",\"dimension\":3}}}",
        ),
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
