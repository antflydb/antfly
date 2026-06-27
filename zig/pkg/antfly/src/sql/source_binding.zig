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

const metadata_table_manager = @import("../metadata/table_manager.zig");
const runtime_schema = @import("../storage/schema.zig");

pub const SqlSourceFamily = enum {
    relational,
    document,
    lake,
};

pub const CatalogTableRef = struct {
    database_name: []const u8 = metadata_table_manager.default_database_name,
    namespace_name: []const u8 = metadata_table_manager.default_namespace_name,
    table_name: []const u8,
};

pub const BoundedScanPolicy = struct {
    max_rows: ?u32 = null,
    max_bytes: ?u64 = null,
};

pub const default_document_sql_bounded_scan_rows: u32 = 10_000;

pub const RelationalBinding = struct {
    target: CatalogTableRef,
    schema: runtime_schema.TableSchema,
    table_id: u64 = 0,
    schema_generation: u64 = 0,
};

pub const DocumentSqlVirtualFieldSource = enum {
    declared_schema,
    index_definition,
    /// Shape/type metadata only. This can expose a SQL-visible JSON path root,
    /// but it must not satisfy index readiness or choose a storage producer.
    typed_path_metadata,
};

pub const DocumentSqlVirtualField = struct {
    name: []const u8,
    path: []const u8,
    source: DocumentSqlVirtualFieldSource,
    field_type: ?runtime_schema.AntflyType = null,
};

pub const DocumentSqlTypedPath = struct {
    path: []const u8,
    field_type: runtime_schema.AntflyType,
};

pub const DocumentSqlSchema = struct {
    exposes_doc_id: bool = true,
    exposes_doc: bool = true,
    fields: []const DocumentSqlVirtualField = &.{},
    owns_fields: bool = false,
    typed_paths: []const DocumentSqlTypedPath = &.{},
    owns_typed_paths: bool = false,
};

pub const DocumentSqlFullTextIndex = struct {
    name: []const u8,
    paths: []const []const u8 = &.{},
};

pub const DocumentSqlCapabilities = struct {
    doc_id_lookup: bool = true,
    indexed_scalar_filters: bool = false,
    indexed_scalar_filter_paths: []const []const u8 = &.{},
    owns_indexed_scalar_filter_paths: bool = false,
    runtime_schema_scalar_filters: ?runtime_schema.TableSchema = null,
    full_text_filters: bool = false,
    full_text_indexes: []const DocumentSqlFullTextIndex = &.{},
    owns_full_text_indexes: bool = false,
    semantic_filters: bool = false,
    vector_filters: bool = false,
    hybrid_filters: bool = false,
    graph_filters: bool = false,
    graph_metric_filters: bool = false,
    algebraic_aggregates: bool = false,
    bounded_scan: ?BoundedScanPolicy = null,
};

pub const DocumentBinding = struct {
    target: CatalogTableRef,
    schema: runtime_schema.TableSchema,
    table_id: u64 = 0,
    schema_generation: u64 = 0,
    indexes_json: ?[]const u8 = null,
    virtual_schema: DocumentSqlSchema = .{},
    capabilities: DocumentSqlCapabilities = .{},
};

pub const LakeBinding = struct {
    target: CatalogTableRef,
    schema: runtime_schema.TableSchema,
    table_id: u64 = 0,
    schema_generation: u64 = 0,
};

pub const SqlSourceBinding = union(SqlSourceFamily) {
    relational: RelationalBinding,
    document: DocumentBinding,
    lake: LakeBinding,

    pub fn family(self: SqlSourceBinding) SqlSourceFamily {
        return switch (self) {
            .relational => .relational,
            .document => .document,
            .lake => .lake,
        };
    }

    pub fn target(self: SqlSourceBinding) CatalogTableRef {
        return switch (self) {
            .relational => |binding| binding.target,
            .document => |binding| binding.target,
            .lake => |binding| binding.target,
        };
    }

    pub fn schema(self: SqlSourceBinding) runtime_schema.TableSchema {
        return switch (self) {
            .relational => |binding| binding.schema,
            .document => |binding| binding.schema,
            .lake => |binding| binding.schema,
        };
    }

    pub fn tableId(self: SqlSourceBinding) u64 {
        return switch (self) {
            .relational => |binding| binding.table_id,
            .document => |binding| binding.table_id,
            .lake => |binding| binding.table_id,
        };
    }

    pub fn schemaGeneration(self: SqlSourceBinding) u64 {
        return switch (self) {
            .relational => |binding| binding.schema_generation,
            .document => |binding| binding.schema_generation,
            .lake => |binding| binding.schema_generation,
        };
    }
};

pub fn familyForRuntimeSchema(schema: runtime_schema.TableSchema) SqlSourceFamily {
    if (schema.external_base_source != null) return .lake;
    return switch (schema.storage_mode) {
        .relational => .relational,
        .document => .document,
    };
}

pub fn bindingForRuntimeSchema(target: CatalogTableRef, schema: runtime_schema.TableSchema) SqlSourceBinding {
    return switch (familyForRuntimeSchema(schema)) {
        .relational => .{ .relational = .{
            .target = target,
            .schema = schema,
        } },
        .document => .{ .document = .{
            .target = target,
            .schema = schema,
            .capabilities = documentCapabilitiesForRuntimeSchema(schema),
        } },
        .lake => .{ .lake = .{
            .target = target,
            .schema = schema,
        } },
    };
}

pub fn documentCapabilitiesForRuntimeSchema(schema: runtime_schema.TableSchema) DocumentSqlCapabilities {
    var capabilities = DocumentSqlCapabilities{
        .bounded_scan = .{ .max_rows = default_document_sql_bounded_scan_rows },
        .runtime_schema_scalar_filters = schema,
    };
    const schema_driven_text = documentSchemaHasSchemaDrivenText(schema);
    for (schema.relational_columns) |column| {
        if (!column.indexed or column.index_lifecycle != .ready) continue;
        switch (column.field_type) {
            .text, .html, .search_as_you_type => {
                capabilities.full_text_filters = true;
            },
            .embedding => {
                capabilities.semantic_filters = true;
                capabilities.vector_filters = true;
            },
            .blob => {},
            .keyword,
            .numeric,
            .link,
            .boolean,
            .datetime,
            .geopoint,
            .geoshape,
            .json,
            .array,
            => {},
        }
        if (documentColumnSupportsAlgebraicAggregate(column)) {
            capabilities.algebraic_aggregates = true;
        }
    }
    if (schema_driven_text) capabilities.full_text_filters = true;
    return capabilities;
}

pub fn documentCapabilitiesForRuntimeSchemaAndIndexesJsonAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    indexes_json: []const u8,
) !DocumentSqlCapabilities {
    var capabilities = documentCapabilitiesForRuntimeSchema(schema);
    if (indexes_json.len == 0) return capabilities;
    capabilities.indexed_scalar_filters = false;
    var scalar_paths = std.ArrayListUnmanaged([]const u8).empty;
    var full_text_indexes = std.ArrayListUnmanaged(DocumentSqlFullTextIndex).empty;
    errdefer {
        for (scalar_paths.items) |path| alloc.free(@constCast(path));
        scalar_paths.deinit(alloc);
        deinitDocumentSqlFullTextIndexList(alloc, &full_text_indexes);
    }
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, indexes_json, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidSqlCatalog;

    try mergeDocumentCapabilitiesFromIndexesJsonValue(alloc, &capabilities, &scalar_paths, &full_text_indexes, parsed.value);
    if (scalar_paths.items.len > 0) {
        capabilities.indexed_scalar_filter_paths = try scalar_paths.toOwnedSlice(alloc);
        capabilities.owns_indexed_scalar_filter_paths = true;
    }
    if (full_text_indexes.items.len > 0) {
        capabilities.full_text_indexes = try full_text_indexes.toOwnedSlice(alloc);
        capabilities.owns_full_text_indexes = true;
    }
    return capabilities;
}

pub fn documentSqlSchemaForRuntimeSchemaAndIndexesJsonAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    indexes_json: []const u8,
) !DocumentSqlSchema {
    var fields = std.ArrayListUnmanaged(DocumentSqlVirtualField).empty;
    var typed_paths = std.ArrayListUnmanaged(DocumentSqlTypedPath).empty;
    errdefer deinitDocumentSqlVirtualFieldList(alloc, &fields);
    errdefer deinitDocumentSqlTypedPathList(alloc, &typed_paths);

    for (schema.relational_columns) |column| {
        try appendDocumentSqlVirtualFieldAlloc(alloc, &fields, column.name, column.path, .declared_schema, column.field_type);
    }

    if (indexes_json.len > 0) {
        var parsed = try std.json.parseFromSlice(std.json.Value, alloc, indexes_json, .{});
        defer parsed.deinit();
        if (parsed.value != .object) return error.InvalidSqlCatalog;
        try appendDocumentSqlVirtualFieldsFromCatalogMetadataValue(alloc, &fields, parsed.value);
        try appendDocumentSqlTypedPathsFromCatalogMetadataValue(alloc, &typed_paths, parsed.value);
    }

    var virtual_schema = DocumentSqlSchema{};
    errdefer deinitDocumentSqlSchema(alloc, &virtual_schema);
    if (fields.items.len > 0) {
        virtual_schema.fields = try fields.toOwnedSlice(alloc);
        virtual_schema.owns_fields = true;
    }
    if (typed_paths.items.len > 0) {
        virtual_schema.typed_paths = try typed_paths.toOwnedSlice(alloc);
        virtual_schema.owns_typed_paths = true;
    }
    return virtual_schema;
}

pub fn deinitDocumentSqlSchema(alloc: std.mem.Allocator, schema: *DocumentSqlSchema) void {
    if (schema.owns_fields) {
        for (schema.fields) |field| {
            alloc.free(@constCast(field.name));
            alloc.free(@constCast(field.path));
        }
        alloc.free(schema.fields);
    }
    if (schema.owns_typed_paths) {
        for (schema.typed_paths) |path| {
            alloc.free(@constCast(path.path));
        }
        alloc.free(schema.typed_paths);
    }
    schema.* = .{};
}

pub fn documentSqlTypedPathType(schema: DocumentSqlSchema, path: []const u8) ?runtime_schema.AntflyType {
    for (schema.typed_paths) |candidate| {
        if (documentScalarFilterPathEqual(candidate.path, path)) return candidate.field_type;
    }
    return null;
}

pub fn deinitDocumentSqlCapabilities(alloc: std.mem.Allocator, capabilities: *DocumentSqlCapabilities) void {
    if (capabilities.owns_indexed_scalar_filter_paths) {
        for (capabilities.indexed_scalar_filter_paths) |path| alloc.free(@constCast(path));
        alloc.free(capabilities.indexed_scalar_filter_paths);
    }
    if (capabilities.owns_full_text_indexes) {
        for (capabilities.full_text_indexes) |index| {
            alloc.free(@constCast(index.name));
            for (index.paths) |path| alloc.free(@constCast(path));
            if (index.paths.len > 0) alloc.free(index.paths);
        }
        alloc.free(capabilities.full_text_indexes);
    }
    capabilities.* = .{};
}

pub fn documentScalarFilterPathReady(capabilities: DocumentSqlCapabilities, path: []const u8) bool {
    if (capabilities.indexed_scalar_filters) return true;
    for (capabilities.indexed_scalar_filter_paths) |candidate| {
        if (documentScalarFilterPathEqual(candidate, path)) return true;
    }
    if (capabilities.runtime_schema_scalar_filters) |schema| {
        return documentRuntimeSchemaScalarPathReady(schema, path);
    }
    return false;
}

fn appendDocumentSqlVirtualFieldsFromCatalogMetadataValue(
    alloc: std.mem.Allocator,
    fields: *std.ArrayListUnmanaged(DocumentSqlVirtualField),
    value: std.json.Value,
) !void {
    if (value != .object) return error.InvalidSqlCatalog;

    if (value.object.get("indexes")) |indexes| {
        if (indexes != .array) return error.InvalidSqlCatalog;
        for (indexes.array.items) |index_value| {
            try appendDocumentSqlVirtualFieldsFromIndexDefinitionValue(alloc, fields, index_value);
        }
    }

    var it = value.object.iterator();
    while (it.next()) |entry| {
        if (std.mem.eql(u8, entry.key_ptr.*, "indexes") or
            std.mem.eql(u8, entry.key_ptr.*, "typed_paths") or
            std.mem.eql(u8, entry.key_ptr.*, "resolvers") or
            std.mem.eql(u8, entry.key_ptr.*, "enrichments")) continue;
        try appendDocumentSqlVirtualFieldsFromIndexDefinitionValue(alloc, fields, entry.value_ptr.*);
    }

    if (value.object.get("typed_paths")) |typed_paths| {
        try appendDocumentSqlVirtualFieldsFromTypedPathsValue(alloc, fields, typed_paths);
    }
}

fn appendDocumentSqlVirtualFieldsFromIndexDefinitionValue(
    alloc: std.mem.Allocator,
    fields: *std.ArrayListUnmanaged(DocumentSqlVirtualField),
    value: std.json.Value,
) !void {
    if (value != .object) return;
    const type_value = value.object.get("type") orelse value.object.get("kind") orelse return;
    if (type_value != .string) return;
    if (!std.mem.eql(u8, type_value.string, "full_text")) return;
    try appendDocumentSqlIndexDefinitionVirtualFieldsFromNamedConfigValue(alloc, fields, value, "field");
    try appendDocumentSqlIndexDefinitionVirtualFieldsFromNamedConfigValue(alloc, fields, value, "path");
    try appendDocumentSqlIndexDefinitionVirtualFieldsFromNamedConfigValue(alloc, fields, value, "fields");
    try appendDocumentSqlIndexDefinitionVirtualFieldsFromNamedConfigValue(alloc, fields, value, "paths");
}

fn appendDocumentSqlIndexDefinitionVirtualFieldsFromNamedConfigValue(
    alloc: std.mem.Allocator,
    fields: *std.ArrayListUnmanaged(DocumentSqlVirtualField),
    value: std.json.Value,
    name: []const u8,
) !void {
    const field_value = value.object.get(name) orelse return;
    switch (field_value) {
        .string => |path| try appendDocumentSqlVirtualFieldFromIndexDefinitionPathAlloc(alloc, fields, path),
        .array => |array| {
            for (array.items) |item| {
                if (item != .string) return error.InvalidSqlCatalog;
                try appendDocumentSqlVirtualFieldFromIndexDefinitionPathAlloc(alloc, fields, item.string);
            }
        },
        else => return error.InvalidSqlCatalog,
    }
}

fn appendDocumentSqlVirtualFieldFromIndexDefinitionPathAlloc(
    alloc: std.mem.Allocator,
    fields: *std.ArrayListUnmanaged(DocumentSqlVirtualField),
    path: []const u8,
) !void {
    const top_level = documentSqlTopLevelPathSegment(path) orelse return error.InvalidSqlCatalog;
    try appendDocumentSqlVirtualFieldAlloc(alloc, fields, top_level, top_level, .index_definition, null);
}

fn appendDocumentSqlVirtualFieldsFromTypedPathsValue(
    alloc: std.mem.Allocator,
    fields: *std.ArrayListUnmanaged(DocumentSqlVirtualField),
    value: std.json.Value,
) !void {
    switch (value) {
        .array => |array| {
            for (array.items) |item| try appendDocumentSqlVirtualFieldsFromTypedPathConfigValue(alloc, fields, item);
        },
        .object => |object| {
            if (object.get("type") != null or object.get("kind") != null) {
                try appendDocumentSqlVirtualFieldsFromTypedPathConfigValue(alloc, fields, value);
                return;
            }
            var it = object.iterator();
            while (it.next()) |entry| {
                const field_type = documentSqlFieldTypeFromTypedPathTypeName(entry.key_ptr.*) orelse return error.InvalidSqlCatalog;
                try appendDocumentSqlVirtualFieldsFromPathValue(alloc, fields, entry.value_ptr.*, field_type);
            }
        },
        else => return error.InvalidSqlCatalog,
    }
}

fn appendDocumentSqlVirtualFieldsFromPathValue(
    alloc: std.mem.Allocator,
    fields: *std.ArrayListUnmanaged(DocumentSqlVirtualField),
    value: std.json.Value,
    field_type: runtime_schema.AntflyType,
) !void {
    switch (value) {
        .string => |path| try appendDocumentSqlVirtualFieldFromTypedPathAlloc(alloc, fields, path, field_type),
        .array => |array| {
            for (array.items) |item| {
                if (item != .string) return error.InvalidSqlCatalog;
                try appendDocumentSqlVirtualFieldFromTypedPathAlloc(alloc, fields, item.string, field_type);
            }
        },
        else => return error.InvalidSqlCatalog,
    }
}

fn appendDocumentSqlVirtualFieldsFromTypedPathConfigValue(
    alloc: std.mem.Allocator,
    fields: *std.ArrayListUnmanaged(DocumentSqlVirtualField),
    value: std.json.Value,
) !void {
    if (value != .object) return;
    const field_type = documentSqlFieldTypeFromTypedPathConfigValue(value);
    try appendDocumentSqlVirtualFieldsFromNamedConfigValue(alloc, fields, value, "field", field_type);
    try appendDocumentSqlVirtualFieldsFromNamedConfigValue(alloc, fields, value, "path", field_type);
    try appendDocumentSqlVirtualFieldsFromNamedConfigValue(alloc, fields, value, "fields", field_type);
    try appendDocumentSqlVirtualFieldsFromNamedConfigValue(alloc, fields, value, "paths", field_type);
}

fn appendDocumentSqlVirtualFieldsFromNamedConfigValue(
    alloc: std.mem.Allocator,
    fields: *std.ArrayListUnmanaged(DocumentSqlVirtualField),
    value: std.json.Value,
    name: []const u8,
    field_type: ?runtime_schema.AntflyType,
) !void {
    const field_value = value.object.get(name) orelse return;
    switch (field_value) {
        .string => |path| try appendDocumentSqlVirtualFieldFromTypedPathAlloc(alloc, fields, path, field_type),
        .array => |array| {
            for (array.items) |item| {
                if (item != .string) return error.InvalidSqlCatalog;
                try appendDocumentSqlVirtualFieldFromTypedPathAlloc(alloc, fields, item.string, field_type);
            }
        },
        else => return error.InvalidSqlCatalog,
    }
}

fn documentSqlFieldTypeFromTypedPathConfigValue(value: std.json.Value) ?runtime_schema.AntflyType {
    const type_value = value.object.get("type") orelse value.object.get("kind") orelse return null;
    if (type_value != .string) return null;
    return documentSqlFieldTypeFromTypedPathTypeName(type_value.string);
}

fn documentSqlFieldTypeFromTypedPathTypeName(type_name: []const u8) ?runtime_schema.AntflyType {
    if (std.mem.eql(u8, type_name, "keyword") or std.mem.eql(u8, type_name, "term")) return .keyword;
    if (std.mem.eql(u8, type_name, "numeric")) return .numeric;
    if (std.mem.eql(u8, type_name, "boolean")) return .boolean;
    if (std.mem.eql(u8, type_name, "datetime")) return .datetime;
    return null;
}

fn appendDocumentSqlVirtualFieldFromTypedPathAlloc(
    alloc: std.mem.Allocator,
    fields: *std.ArrayListUnmanaged(DocumentSqlVirtualField),
    path: []const u8,
    field_type: ?runtime_schema.AntflyType,
) !void {
    const top_level = documentSqlTopLevelPathSegment(path) orelse return error.InvalidSqlCatalog;
    try appendDocumentSqlVirtualFieldAlloc(alloc, fields, top_level, top_level, .typed_path_metadata, documentSqlVirtualFieldTypeForTypedPath(path, field_type));
}

fn appendDocumentSqlTypedPathsFromCatalogMetadataValue(
    alloc: std.mem.Allocator,
    typed_paths: *std.ArrayListUnmanaged(DocumentSqlTypedPath),
    value: std.json.Value,
) !void {
    if (value != .object) return error.InvalidSqlCatalog;

    if (value.object.get("typed_paths")) |typed_paths_value| {
        try appendDocumentSqlTypedPathsFromTypedPathsValue(alloc, typed_paths, typed_paths_value);
    }
}

fn appendDocumentSqlTypedPathsFromTypedPathsValue(
    alloc: std.mem.Allocator,
    typed_paths: *std.ArrayListUnmanaged(DocumentSqlTypedPath),
    value: std.json.Value,
) !void {
    switch (value) {
        .array => |array| {
            for (array.items) |item| try appendDocumentSqlTypedPathsFromTypedPathConfigValue(alloc, typed_paths, item);
        },
        .object => |object| {
            if (object.get("type") != null or object.get("kind") != null) {
                try appendDocumentSqlTypedPathsFromTypedPathConfigValue(alloc, typed_paths, value);
                return;
            }
            var it = object.iterator();
            while (it.next()) |entry| {
                const field_type = documentSqlFieldTypeFromTypedPathTypeName(entry.key_ptr.*) orelse return error.InvalidSqlCatalog;
                try appendDocumentSqlTypedPathsFromPathValue(alloc, typed_paths, entry.value_ptr.*, field_type);
            }
        },
        else => return error.InvalidSqlCatalog,
    }
}

fn appendDocumentSqlTypedPathsFromPathValue(
    alloc: std.mem.Allocator,
    typed_paths: *std.ArrayListUnmanaged(DocumentSqlTypedPath),
    value: std.json.Value,
    field_type: runtime_schema.AntflyType,
) !void {
    switch (value) {
        .string => |path| try appendDocumentSqlTypedPathAlloc(alloc, typed_paths, path, field_type),
        .array => |array| {
            for (array.items) |item| {
                if (item != .string) return error.InvalidSqlCatalog;
                try appendDocumentSqlTypedPathAlloc(alloc, typed_paths, item.string, field_type);
            }
        },
        else => return error.InvalidSqlCatalog,
    }
}

fn appendDocumentSqlTypedPathsFromTypedPathConfigValue(
    alloc: std.mem.Allocator,
    typed_paths: *std.ArrayListUnmanaged(DocumentSqlTypedPath),
    value: std.json.Value,
) !void {
    if (value != .object) return;
    const field_type = documentSqlFieldTypeFromTypedPathConfigValue(value) orelse return;
    try appendDocumentSqlTypedPathsFromNamedConfigValue(alloc, typed_paths, value, "field", field_type);
    try appendDocumentSqlTypedPathsFromNamedConfigValue(alloc, typed_paths, value, "path", field_type);
    try appendDocumentSqlTypedPathsFromNamedConfigValue(alloc, typed_paths, value, "fields", field_type);
    try appendDocumentSqlTypedPathsFromNamedConfigValue(alloc, typed_paths, value, "paths", field_type);
}

fn appendDocumentSqlTypedPathsFromNamedConfigValue(
    alloc: std.mem.Allocator,
    typed_paths: *std.ArrayListUnmanaged(DocumentSqlTypedPath),
    value: std.json.Value,
    name: []const u8,
    field_type: runtime_schema.AntflyType,
) !void {
    const field_value = value.object.get(name) orelse return;
    switch (field_value) {
        .string => |path| try appendDocumentSqlTypedPathAlloc(alloc, typed_paths, path, field_type),
        .array => |array| {
            for (array.items) |item| {
                if (item != .string) return error.InvalidSqlCatalog;
                try appendDocumentSqlTypedPathAlloc(alloc, typed_paths, item.string, field_type);
            }
        },
        else => return error.InvalidSqlCatalog,
    }
}

fn appendDocumentSqlTypedPathAlloc(
    alloc: std.mem.Allocator,
    typed_paths: *std.ArrayListUnmanaged(DocumentSqlTypedPath),
    path: []const u8,
    field_type: runtime_schema.AntflyType,
) !void {
    if (path.len == 0) return error.InvalidSqlCatalog;
    for (typed_paths.items) |existing| {
        if (documentScalarFilterPathEqual(existing.path, path)) return;
    }
    try typed_paths.append(alloc, .{
        .path = try alloc.dupe(u8, path),
        .field_type = field_type,
    });
}

fn documentSqlVirtualFieldTypeForTypedPath(path: []const u8, field_type: ?runtime_schema.AntflyType) ?runtime_schema.AntflyType {
    const top_level = documentSqlTopLevelPathSegment(path) orelse return null;
    const normalized = if (path.len > 0 and path[0] == '/') path[1..] else path;
    if (normalized.len != top_level.len) return null;
    return field_type;
}

fn appendDocumentSqlVirtualFieldAlloc(
    alloc: std.mem.Allocator,
    fields: *std.ArrayListUnmanaged(DocumentSqlVirtualField),
    name: []const u8,
    path: []const u8,
    source: DocumentSqlVirtualFieldSource,
    field_type: ?runtime_schema.AntflyType,
) !void {
    if (name.len == 0 or path.len == 0) return error.InvalidSqlCatalog;
    for (fields.items) |existing| {
        if (std.ascii.eqlIgnoreCase(existing.name, name)) return;
    }
    const owned_name = try alloc.dupe(u8, name);
    errdefer alloc.free(owned_name);
    const owned_path = try alloc.dupe(u8, path);
    errdefer alloc.free(owned_path);
    try fields.append(alloc, .{
        .name = owned_name,
        .path = owned_path,
        .source = source,
        .field_type = field_type,
    });
}

fn deinitDocumentSqlVirtualFieldList(
    alloc: std.mem.Allocator,
    fields: *std.ArrayListUnmanaged(DocumentSqlVirtualField),
) void {
    for (fields.items) |field| {
        alloc.free(@constCast(field.name));
        alloc.free(@constCast(field.path));
    }
    fields.deinit(alloc);
}

fn deinitDocumentSqlTypedPathList(
    alloc: std.mem.Allocator,
    paths: *std.ArrayListUnmanaged(DocumentSqlTypedPath),
) void {
    for (paths.items) |path| {
        alloc.free(@constCast(path.path));
    }
    paths.deinit(alloc);
}

fn documentSqlTopLevelPathSegment(path: []const u8) ?[]const u8 {
    if (path.len == 0) return null;
    const start: usize = if (path[0] == '/') 1 else 0;
    if (start >= path.len) return null;
    var end = start;
    while (end < path.len and path[end] != '/' and path[end] != '.') : (end += 1) {}
    if (end == start) return null;
    return path[start..end];
}

fn mergeDocumentCapabilitiesFromIndexesJsonValue(
    alloc: std.mem.Allocator,
    capabilities: *DocumentSqlCapabilities,
    scalar_paths: *std.ArrayListUnmanaged([]const u8),
    full_text_indexes: *std.ArrayListUnmanaged(DocumentSqlFullTextIndex),
    value: std.json.Value,
) !void {
    if (value != .object) return error.InvalidSqlCatalog;

    if (value.object.get("indexes")) |indexes| {
        if (indexes != .array) return error.InvalidSqlCatalog;
        for (indexes.array.items) |index_value| {
            try mergeDocumentCapabilitiesFromIndexConfigValue(alloc, capabilities, scalar_paths, full_text_indexes, null, index_value);
        }
    }

    var it = value.object.iterator();
    while (it.next()) |entry| {
        if (std.mem.eql(u8, entry.key_ptr.*, "indexes") or
            std.mem.eql(u8, entry.key_ptr.*, "typed_paths") or
            std.mem.eql(u8, entry.key_ptr.*, "resolvers") or
            std.mem.eql(u8, entry.key_ptr.*, "enrichments")) continue;
        try mergeDocumentCapabilitiesFromIndexConfigValue(alloc, capabilities, scalar_paths, full_text_indexes, entry.key_ptr.*, entry.value_ptr.*);
    }
}

fn mergeDocumentCapabilitiesFromIndexConfigValue(
    alloc: std.mem.Allocator,
    capabilities: *DocumentSqlCapabilities,
    scalar_paths: *std.ArrayListUnmanaged([]const u8),
    full_text_indexes: *std.ArrayListUnmanaged(DocumentSqlFullTextIndex),
    index_name: ?[]const u8,
    value: std.json.Value,
) !void {
    if (value != .object) return;
    const type_value = value.object.get("type") orelse value.object.get("kind") orelse return;
    if (type_value != .string) return;
    if (std.mem.eql(u8, type_value.string, "full_text")) {
        capabilities.full_text_filters = true;
        try appendDocumentFullTextIndexFromConfigValue(alloc, full_text_indexes, index_name, value);
        try appendDocumentScalarFilterPathsFromIndexConfigValue(alloc, scalar_paths, value);
    } else if (std.mem.eql(u8, type_value.string, "embeddings") or
        std.mem.eql(u8, type_value.string, "semantic") or
        std.mem.eql(u8, type_value.string, "aknn"))
    {
        capabilities.semantic_filters = true;
        capabilities.vector_filters = true;
    } else if (std.mem.eql(u8, type_value.string, "hybrid")) {
        capabilities.hybrid_filters = true;
        capabilities.vector_filters = true;
    } else if (std.mem.eql(u8, type_value.string, "graph")) {
        capabilities.graph_filters = true;
    } else if (std.mem.eql(u8, type_value.string, "graph_metric")) {
        capabilities.graph_metric_filters = true;
    } else if (std.mem.eql(u8, type_value.string, "algebraic")) {
        capabilities.algebraic_aggregates = true;
    }
}

fn appendDocumentScalarFilterPathsFromIndexConfigValue(
    alloc: std.mem.Allocator,
    scalar_paths: *std.ArrayListUnmanaged([]const u8),
    value: std.json.Value,
) !void {
    try appendDocumentScalarFilterPathsFromNamedConfigValue(alloc, scalar_paths, value, "field");
    try appendDocumentScalarFilterPathsFromNamedConfigValue(alloc, scalar_paths, value, "path");
    try appendDocumentScalarFilterPathsFromNamedConfigValue(alloc, scalar_paths, value, "fields");
    try appendDocumentScalarFilterPathsFromNamedConfigValue(alloc, scalar_paths, value, "paths");
    try appendDocumentScalarFilterPathsFromNamedConfigValue(alloc, scalar_paths, value, "scalar_field");
    try appendDocumentScalarFilterPathsFromNamedConfigValue(alloc, scalar_paths, value, "scalar_path");
    try appendDocumentScalarFilterPathsFromNamedConfigValue(alloc, scalar_paths, value, "scalar_fields");
    try appendDocumentScalarFilterPathsFromNamedConfigValue(alloc, scalar_paths, value, "scalar_paths");
}

fn appendDocumentScalarFilterPathsFromNamedConfigValue(
    alloc: std.mem.Allocator,
    scalar_paths: *std.ArrayListUnmanaged([]const u8),
    value: std.json.Value,
    name: []const u8,
) !void {
    const field_value = value.object.get(name) orelse return;
    switch (field_value) {
        .string => |path| try appendDocumentScalarFilterPathAlloc(alloc, scalar_paths, path),
        .array => |array| {
            for (array.items) |item| {
                if (item != .string) return error.InvalidSqlCatalog;
                try appendDocumentScalarFilterPathAlloc(alloc, scalar_paths, item.string);
            }
        },
        else => return error.InvalidSqlCatalog,
    }
}

fn appendDocumentScalarFilterPathAlloc(
    alloc: std.mem.Allocator,
    scalar_paths: *std.ArrayListUnmanaged([]const u8),
    path: []const u8,
) !void {
    if (path.len == 0) return error.InvalidSqlCatalog;
    for (scalar_paths.items) |existing| {
        if (documentScalarFilterPathEqual(existing, path)) return;
    }
    try scalar_paths.append(alloc, try alloc.dupe(u8, path));
}

fn appendDocumentFullTextIndexFromConfigValue(
    alloc: std.mem.Allocator,
    indexes: *std.ArrayListUnmanaged(DocumentSqlFullTextIndex),
    index_name: ?[]const u8,
    value: std.json.Value,
) !void {
    const name = index_name orelse blk: {
        const name_value = value.object.get("name") orelse return;
        if (name_value != .string or name_value.string.len == 0) return error.InvalidSqlCatalog;
        break :blk name_value.string;
    };
    for (indexes.items) |existing| {
        if (std.mem.eql(u8, existing.name, name)) return;
    }

    var paths = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (paths.items) |path| alloc.free(@constCast(path));
        paths.deinit(alloc);
    }
    try appendDocumentScalarFilterPathsFromIndexConfigValue(alloc, &paths, value);

    const owned_name = try alloc.dupe(u8, name);
    errdefer alloc.free(owned_name);
    try indexes.append(alloc, .{
        .name = owned_name,
        .paths = if (paths.items.len > 0) try paths.toOwnedSlice(alloc) else &.{},
    });
}

fn deinitDocumentSqlFullTextIndexList(
    alloc: std.mem.Allocator,
    indexes: *std.ArrayListUnmanaged(DocumentSqlFullTextIndex),
) void {
    for (indexes.items) |index| {
        alloc.free(@constCast(index.name));
        for (index.paths) |path| alloc.free(@constCast(path));
        if (index.paths.len > 0) alloc.free(index.paths);
    }
    indexes.deinit(alloc);
}

fn documentScalarFilterPathEqual(a: []const u8, b: []const u8) bool {
    var ai: usize = if (a.len > 0 and a[0] == '/') 1 else 0;
    var bi: usize = if (b.len > 0 and b[0] == '/') 1 else 0;
    while (ai < a.len and bi < b.len) : ({
        ai += 1;
        bi += 1;
    }) {
        const ac = if (a[ai] == '.') '/' else a[ai];
        const bc = if (b[bi] == '.') '/' else b[bi];
        if (ac != bc) return false;
    }
    return ai == a.len and bi == b.len;
}

fn documentRuntimeSchemaScalarPathReady(schema: runtime_schema.TableSchema, path: []const u8) bool {
    if (documentColumnForPath(schema, path)) |column| {
        return documentColumnIndexReady(column);
    }
    for (schema.relational_columns) |column| {
        if (column.field_type != .json) continue;
        if (!documentColumnIndexReady(column)) continue;
        if (documentPathContainsPath(column.path, path)) return true;
    }
    return false;
}

fn documentColumnForPath(schema: runtime_schema.TableSchema, path: []const u8) ?runtime_schema.RelationalColumn {
    for (schema.relational_columns) |column| {
        if (documentScalarFilterPathEqual(column.path, path)) return column;
    }
    return null;
}

fn documentColumnIndexReady(column: runtime_schema.RelationalColumn) bool {
    return column.indexed and column.index_lifecycle == .ready;
}

fn documentPathContainsPath(parent_path: []const u8, child_path: []const u8) bool {
    if (documentScalarFilterPathEqual(parent_path, child_path)) return true;
    var parent_i: usize = if (parent_path.len > 0 and parent_path[0] == '/') 1 else 0;
    var child_i: usize = if (child_path.len > 0 and child_path[0] == '/') 1 else 0;
    while (parent_i < parent_path.len and child_i < child_path.len) : ({
        parent_i += 1;
        child_i += 1;
    }) {
        const parent_char = if (parent_path[parent_i] == '.') '/' else parent_path[parent_i];
        const child_char = if (child_path[child_i] == '.') '/' else child_path[child_i];
        if (parent_char != child_char) return false;
    }
    if (parent_i != parent_path.len or child_i >= child_path.len) return false;
    return child_path[child_i] == '/' or child_path[child_i] == '.';
}

fn documentSchemaHasSchemaDrivenText(schema: runtime_schema.TableSchema) bool {
    if (schema.dynamic_templates.len > 0) return true;
    for (schema.full_text_documents) |document| {
        if (document.fields.len > 0) return true;
        if (document.dynamic_rules.len > 0) return true;
        if (document.open_dynamic_paths.len > 0) return true;
        if (document.infer_type_dynamic_paths.len > 0) return true;
    }
    return false;
}

fn documentColumnSupportsAlgebraicAggregate(column: runtime_schema.RelationalColumn) bool {
    if (!column.indexed or column.index_lifecycle != .ready) return false;
    return switch (column.field_type) {
        .keyword, .numeric, .boolean, .datetime, .geopoint, .geoshape => true,
        else => false,
    };
}

test "source binding classifies relational document and lake schemas" {
    const schema_api = @import("../schema/mod.zig");

    const alloc = std.testing.allocator;
    const target: CatalogTableRef = .{ .table_name = "events" };

    var relational_parsed = try schema_api.parseValidatedTableSchema(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"amount":{"type":"numeric"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    );
    defer relational_parsed.deinit(alloc);
    const relational_schema = try schema_api.deriveRuntimeTableSchema(alloc, relational_parsed);
    defer runtime_schema.freeSchema(alloc, relational_schema);
    const relational = bindingForRuntimeSchema(target, relational_schema);
    try std.testing.expectEqual(SqlSourceFamily.relational, relational.family());
    try std.testing.expectEqualStrings("events", relational.target().table_name);
    try std.testing.expectEqual(runtime_schema.StorageMode.relational, relational.schema().storage_mode);

    var document_parsed = try schema_api.parseValidatedTableSchema(alloc,
        \\{"version":1,"default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","properties":{"title":{"type":"text"}},"additionalProperties":true}}}}
    );
    defer document_parsed.deinit(alloc);
    const document_schema = try schema_api.deriveRuntimeTableSchema(alloc, document_parsed);
    defer runtime_schema.freeSchema(alloc, document_schema);
    const document = bindingForRuntimeSchema(target, document_schema);
    try std.testing.expectEqual(SqlSourceFamily.document, document.family());
    switch (document) {
        .document => |binding| {
            try std.testing.expect(binding.capabilities.doc_id_lookup);
            try std.testing.expect(binding.virtual_schema.exposes_doc);
            try std.testing.expect(binding.capabilities.full_text_filters);
            try std.testing.expect(!binding.capabilities.indexed_scalar_filters);
            try std.testing.expect(documentScalarFilterPathReady(binding.capabilities, "/title"));
            try std.testing.expect(!documentScalarFilterPathReady(binding.capabilities, "/status"));
            try std.testing.expect(!binding.capabilities.semantic_filters);
            try std.testing.expect(!binding.capabilities.vector_filters);
            try std.testing.expect(!binding.capabilities.hybrid_filters);
            try std.testing.expect(!binding.capabilities.graph_filters);
            try std.testing.expect(!binding.capabilities.graph_metric_filters);
            try std.testing.expect(!binding.capabilities.algebraic_aggregates);
            try std.testing.expectEqual(default_document_sql_bounded_scan_rows, binding.capabilities.bounded_scan.?.max_rows.?);
        },
        else => return error.TestExpectedEqual,
    }

    const capable_schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "title", .path = "title", .field_type = .text },
            .{ .name = "status", .path = "status", .field_type = .keyword },
            .{ .name = "embedding", .path = "embedding", .field_type = .embedding },
        },
    };
    const capable = bindingForRuntimeSchema(target, capable_schema);
    switch (capable) {
        .document => |binding| {
            try std.testing.expect(binding.capabilities.full_text_filters);
            try std.testing.expect(!binding.capabilities.indexed_scalar_filters);
            try std.testing.expect(documentScalarFilterPathReady(binding.capabilities, "/title"));
            try std.testing.expect(documentScalarFilterPathReady(binding.capabilities, "/status"));
            try std.testing.expect(!documentScalarFilterPathReady(binding.capabilities, "/category"));
            try std.testing.expect(binding.capabilities.semantic_filters);
            try std.testing.expect(binding.capabilities.vector_filters);
            try std.testing.expect(!binding.capabilities.hybrid_filters);
            try std.testing.expect(!binding.capabilities.graph_filters);
            try std.testing.expect(!binding.capabilities.graph_metric_filters);
            try std.testing.expect(binding.capabilities.algebraic_aggregates);
            try std.testing.expectEqual(default_document_sql_bounded_scan_rows, binding.capabilities.bounded_scan.?.max_rows.?);
        },
        else => return error.TestExpectedEqual,
    }

    const unavailable_schema = runtime_schema.TableSchema{
        .storage_mode = .document,
        .relational_columns = &.{
            .{ .name = "status", .path = "status", .field_type = .keyword, .index_lifecycle = .building },
            .{ .name = "body", .path = "body", .field_type = .text, .indexed = false },
        },
    };
    const unavailable = bindingForRuntimeSchema(target, unavailable_schema);
    switch (unavailable) {
        .document => |binding| {
            try std.testing.expect(!binding.capabilities.full_text_filters);
            try std.testing.expect(!binding.capabilities.indexed_scalar_filters);
            try std.testing.expect(!binding.capabilities.semantic_filters);
            try std.testing.expect(!binding.capabilities.vector_filters);
            try std.testing.expect(!binding.capabilities.hybrid_filters);
            try std.testing.expect(!binding.capabilities.graph_filters);
            try std.testing.expect(!binding.capabilities.graph_metric_filters);
            try std.testing.expect(!binding.capabilities.algebraic_aggregates);
            try std.testing.expectEqual(default_document_sql_bounded_scan_rows, binding.capabilities.bounded_scan.?.max_rows.?);
        },
        else => return error.TestExpectedEqual,
    }

    var index_backed_capabilities = try documentCapabilitiesForRuntimeSchemaAndIndexesJsonAlloc(
        alloc,
        unavailable_schema,
        "{\"fts\":{\"type\":\"full_text\"},\"embedding_idx\":{\"type\":\"embeddings\",\"dimension\":384},\"alg\":{\"type\":\"algebraic\"}}",
    );
    defer deinitDocumentSqlCapabilities(alloc, &index_backed_capabilities);
    try std.testing.expect(index_backed_capabilities.full_text_filters);
    try std.testing.expectEqual(@as(usize, 1), index_backed_capabilities.full_text_indexes.len);
    try std.testing.expectEqualStrings("fts", index_backed_capabilities.full_text_indexes[0].name);
    try std.testing.expect(!index_backed_capabilities.indexed_scalar_filters);
    try std.testing.expect(index_backed_capabilities.semantic_filters);
    try std.testing.expect(index_backed_capabilities.vector_filters);
    try std.testing.expect(!index_backed_capabilities.hybrid_filters);
    try std.testing.expect(!index_backed_capabilities.graph_filters);
    try std.testing.expect(!index_backed_capabilities.graph_metric_filters);
    try std.testing.expect(index_backed_capabilities.algebraic_aggregates);
    try std.testing.expectEqual(default_document_sql_bounded_scan_rows, index_backed_capabilities.bounded_scan.?.max_rows.?);

    var list_backed_capabilities = try documentCapabilitiesForRuntimeSchemaAndIndexesJsonAlloc(
        alloc,
        unavailable_schema,
        "{\"indexes\":[{\"name\":\"fts\",\"kind\":\"full_text\"},{\"name\":\"embedding_idx\",\"type\":\"embeddings\",\"dimension\":384},{\"name\":\"alg\",\"type\":\"algebraic\"}]}",
    );
    defer deinitDocumentSqlCapabilities(alloc, &list_backed_capabilities);
    try std.testing.expect(list_backed_capabilities.full_text_filters);
    try std.testing.expectEqual(@as(usize, 1), list_backed_capabilities.full_text_indexes.len);
    try std.testing.expectEqualStrings("fts", list_backed_capabilities.full_text_indexes[0].name);
    try std.testing.expect(!list_backed_capabilities.indexed_scalar_filters);
    try std.testing.expect(list_backed_capabilities.semantic_filters);
    try std.testing.expect(list_backed_capabilities.vector_filters);
    try std.testing.expect(list_backed_capabilities.algebraic_aggregates);

    var derived_index_capabilities = try documentCapabilitiesForRuntimeSchemaAndIndexesJsonAlloc(
        alloc,
        unavailable_schema,
        "{\"semantic_idx\":{\"type\":\"semantic\"},\"hybrid_idx\":{\"type\":\"hybrid\"},\"graph_idx\":{\"type\":\"graph\"},\"pagerank_idx\":{\"type\":\"graph_metric\"},\"aknn_idx\":{\"type\":\"aknn\"}}",
    );
    defer deinitDocumentSqlCapabilities(alloc, &derived_index_capabilities);
    try std.testing.expect(derived_index_capabilities.semantic_filters);
    try std.testing.expect(derived_index_capabilities.vector_filters);
    try std.testing.expect(derived_index_capabilities.hybrid_filters);
    try std.testing.expect(derived_index_capabilities.graph_filters);
    try std.testing.expect(derived_index_capabilities.graph_metric_filters);
    try std.testing.expect(!derived_index_capabilities.algebraic_aggregates);

    var typed_path_capabilities = try documentCapabilitiesForRuntimeSchemaAndIndexesJsonAlloc(
        alloc,
        unavailable_schema,
        "{\"typed_paths\":{\"keyword\":[\"status\",\"metadata.plan\"],\"numeric\":[\"/metadata/score\"]},\"future_text\":{\"type\":\"full_text\",\"scalar_paths\":[\"tenant\"]}}",
    );
    defer deinitDocumentSqlCapabilities(alloc, &typed_path_capabilities);
    try std.testing.expect(!typed_path_capabilities.indexed_scalar_filters);
    try std.testing.expect(typed_path_capabilities.full_text_filters);
    try std.testing.expectEqual(@as(usize, 1), typed_path_capabilities.indexed_scalar_filter_paths.len);
    try std.testing.expect(!documentScalarFilterPathReady(typed_path_capabilities, "/status"));
    try std.testing.expect(!documentScalarFilterPathReady(typed_path_capabilities, "/metadata/plan"));
    try std.testing.expect(!documentScalarFilterPathReady(typed_path_capabilities, "metadata.score"));
    try std.testing.expect(documentScalarFilterPathReady(typed_path_capabilities, "/tenant"));
    try std.testing.expect(!documentScalarFilterPathReady(typed_path_capabilities, "/body"));

    var legacy_scalar_config_capabilities = try documentCapabilitiesForRuntimeSchemaAndIndexesJsonAlloc(
        alloc,
        unavailable_schema,
        "{\"fts\":{\"type\":\"full_text\",\"indexed_scalar_paths\":[\"status\"],\"scalar_paths\":[\"tenant\"]},\"score_idx\":{\"type\":\"numeric\",\"path\":\"metrics.score\"}}",
    );
    defer deinitDocumentSqlCapabilities(alloc, &legacy_scalar_config_capabilities);
    try std.testing.expect(legacy_scalar_config_capabilities.full_text_filters);
    try std.testing.expect(!legacy_scalar_config_capabilities.indexed_scalar_filters);
    try std.testing.expectEqual(@as(usize, 1), legacy_scalar_config_capabilities.indexed_scalar_filter_paths.len);
    try std.testing.expect(documentScalarFilterPathReady(legacy_scalar_config_capabilities, "/tenant"));
    try std.testing.expect(!documentScalarFilterPathReady(legacy_scalar_config_capabilities, "/status"));
    try std.testing.expect(!documentScalarFilterPathReady(legacy_scalar_config_capabilities, "/metrics/score"));

    var full_text_field_capabilities = try documentCapabilitiesForRuntimeSchemaAndIndexesJsonAlloc(
        alloc,
        unavailable_schema,
        "{\"body_fts\":{\"type\":\"full_text\",\"field\":\"body\"},\"title_fts\":{\"type\":\"full_text\",\"fields\":[\"title\",\"metadata.summary\"]}}",
    );
    defer deinitDocumentSqlCapabilities(alloc, &full_text_field_capabilities);
    try std.testing.expect(full_text_field_capabilities.full_text_filters);
    try std.testing.expect(!full_text_field_capabilities.indexed_scalar_filters);
    try std.testing.expectEqual(@as(usize, 3), full_text_field_capabilities.indexed_scalar_filter_paths.len);
    try std.testing.expect(documentScalarFilterPathReady(full_text_field_capabilities, "/body"));
    try std.testing.expect(documentScalarFilterPathReady(full_text_field_capabilities, "title"));
    try std.testing.expect(documentScalarFilterPathReady(full_text_field_capabilities, "/metadata/summary"));
    try std.testing.expect(!documentScalarFilterPathReady(full_text_field_capabilities, "/status"));

    var full_text_virtual_schema = try documentSqlSchemaForRuntimeSchemaAndIndexesJsonAlloc(
        alloc,
        unavailable_schema,
        "{\"category_fts\":{\"type\":\"full_text\",\"field\":\"category\"},\"metadata_fts\":{\"type\":\"full_text\",\"fields\":[\"metadata.summary\"]}}",
    );
    defer deinitDocumentSqlSchema(alloc, &full_text_virtual_schema);
    try std.testing.expectEqual(@as(usize, 4), full_text_virtual_schema.fields.len);
    try std.testing.expectEqual(@as(usize, 0), full_text_virtual_schema.typed_paths.len);
    var saw_category_index_field = false;
    var saw_metadata_index_root = false;
    for (full_text_virtual_schema.fields) |field| {
        if (std.mem.eql(u8, field.name, "category")) {
            saw_category_index_field = true;
            try std.testing.expectEqual(DocumentSqlVirtualFieldSource.index_definition, field.source);
            try std.testing.expect(field.field_type == null);
        }
        if (std.mem.eql(u8, field.name, "metadata")) {
            saw_metadata_index_root = true;
            try std.testing.expectEqual(DocumentSqlVirtualFieldSource.index_definition, field.source);
            try std.testing.expect(field.field_type == null);
        }
    }
    try std.testing.expect(saw_category_index_field);
    try std.testing.expect(saw_metadata_index_root);
    try std.testing.expect(documentSqlTypedPathType(full_text_virtual_schema, "/category") == null);
    try std.testing.expect(documentSqlTypedPathType(full_text_virtual_schema, "/metadata/summary") == null);

    var catalog_virtual_schema = try documentSqlSchemaForRuntimeSchemaAndIndexesJsonAlloc(
        alloc,
        unavailable_schema,
        "{\"body_fts\":{\"type\":\"full_text\",\"field\":\"body\"},\"typed_paths\":{\"numeric\":[\"score\",\"/metrics/score\"],\"keyword\":[\"metadata.plan\",\"/tenant/id\"]}}",
    );
    defer deinitDocumentSqlSchema(alloc, &catalog_virtual_schema);
    try std.testing.expect(catalog_virtual_schema.exposes_doc_id);
    try std.testing.expect(catalog_virtual_schema.exposes_doc);
    try std.testing.expectEqual(@as(usize, 6), catalog_virtual_schema.fields.len);
    try std.testing.expectEqual(@as(usize, 4), catalog_virtual_schema.typed_paths.len);
    try std.testing.expectEqual(runtime_schema.AntflyType.numeric, documentSqlTypedPathType(catalog_virtual_schema, "/score").?);
    try std.testing.expectEqual(runtime_schema.AntflyType.numeric, documentSqlTypedPathType(catalog_virtual_schema, "/metrics/score").?);
    try std.testing.expectEqual(runtime_schema.AntflyType.keyword, documentSqlTypedPathType(catalog_virtual_schema, "/metadata/plan").?);
    try std.testing.expectEqualStrings("status", catalog_virtual_schema.fields[0].name);
    try std.testing.expectEqual(DocumentSqlVirtualFieldSource.declared_schema, catalog_virtual_schema.fields[0].source);
    try std.testing.expectEqual(runtime_schema.AntflyType.keyword, catalog_virtual_schema.fields[0].field_type.?);
    try std.testing.expectEqualStrings("body", catalog_virtual_schema.fields[1].name);
    try std.testing.expectEqual(DocumentSqlVirtualFieldSource.declared_schema, catalog_virtual_schema.fields[1].source);
    try std.testing.expectEqual(runtime_schema.AntflyType.text, catalog_virtual_schema.fields[1].field_type.?);
    try std.testing.expectEqualStrings("score", catalog_virtual_schema.fields[2].name);
    try std.testing.expectEqual(DocumentSqlVirtualFieldSource.typed_path_metadata, catalog_virtual_schema.fields[2].source);
    try std.testing.expectEqual(runtime_schema.AntflyType.numeric, catalog_virtual_schema.fields[2].field_type.?);
    var saw_metadata = false;
    var saw_metrics = false;
    var saw_tenant = false;
    for (catalog_virtual_schema.fields) |field| {
        if (std.mem.eql(u8, field.name, "metadata")) {
            saw_metadata = true;
            try std.testing.expectEqual(DocumentSqlVirtualFieldSource.typed_path_metadata, field.source);
            try std.testing.expect(field.field_type == null);
        }
        if (std.mem.eql(u8, field.name, "metrics")) {
            saw_metrics = true;
            try std.testing.expectEqual(DocumentSqlVirtualFieldSource.typed_path_metadata, field.source);
            try std.testing.expect(field.field_type == null);
        }
        if (std.mem.eql(u8, field.name, "tenant")) {
            saw_tenant = true;
            try std.testing.expectEqual(DocumentSqlVirtualFieldSource.typed_path_metadata, field.source);
            try std.testing.expect(field.field_type == null);
        }
    }
    try std.testing.expect(saw_metadata);
    try std.testing.expect(saw_metrics);
    try std.testing.expect(saw_tenant);

    var explicit_only_virtual_schema = try documentSqlSchemaForRuntimeSchemaAndIndexesJsonAlloc(
        alloc,
        unavailable_schema,
        "{\"legacy_score_idx\":{\"type\":\"numeric\",\"path\":\"legacy.score\"},\"typed_paths\":{\"numeric\":[\"metrics.score\"]}}",
    );
    defer deinitDocumentSqlSchema(alloc, &explicit_only_virtual_schema);
    try std.testing.expectEqual(@as(usize, 1), explicit_only_virtual_schema.typed_paths.len);
    try std.testing.expectEqual(runtime_schema.AntflyType.numeric, documentSqlTypedPathType(explicit_only_virtual_schema, "/metrics/score").?);
    try std.testing.expect(documentSqlTypedPathType(explicit_only_virtual_schema, "/legacy/score") == null);
    for (explicit_only_virtual_schema.fields) |field| {
        try std.testing.expect(!std.mem.eql(u8, field.name, "legacy"));
    }

    var lake_parsed = try schema_api.parseValidatedTableSchema(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"base_source":{"kind":"external","table_id":"events","format":"iceberg","uri":"s3://bucket/warehouse/events","snapshot":{"mode":"snapshot_id","id":"snap-1"},"schema_fingerprint":"schema-v1","write_policy":"read_only"},"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    );
    defer lake_parsed.deinit(alloc);
    const lake_schema = try schema_api.deriveRuntimeTableSchema(alloc, lake_parsed);
    defer runtime_schema.freeSchema(alloc, lake_schema);
    const lake = bindingForRuntimeSchema(target, lake_schema);
    try std.testing.expectEqual(SqlSourceFamily.lake, lake.family());
    try std.testing.expect(lake.schema().external_base_source != null);
}
