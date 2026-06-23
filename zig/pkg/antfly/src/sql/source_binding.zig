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
};

pub const DocumentSqlVirtualFieldSource = enum {
    declared_schema,
    index_definition,
};

pub const DocumentSqlVirtualField = struct {
    name: []const u8,
    path: []const u8,
    source: DocumentSqlVirtualFieldSource,
};

pub const DocumentSqlSchema = struct {
    exposes_doc_id: bool = true,
    exposes_doc: bool = true,
    fields: []const DocumentSqlVirtualField = &.{},
    owns_fields: bool = false,
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
    indexes_json: ?[]const u8 = null,
    virtual_schema: DocumentSqlSchema = .{},
    capabilities: DocumentSqlCapabilities = .{},
};

pub const LakeBinding = struct {
    target: CatalogTableRef,
    schema: runtime_schema.TableSchema,
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
    errdefer deinitDocumentSqlVirtualFieldList(alloc, &fields);

    for (schema.relational_columns) |column| {
        try appendDocumentSqlVirtualFieldAlloc(alloc, &fields, column.name, column.path, .declared_schema);
    }

    if (indexes_json.len > 0) {
        var parsed = try std.json.parseFromSlice(std.json.Value, alloc, indexes_json, .{});
        defer parsed.deinit();
        if (parsed.value != .object) return error.InvalidSqlCatalog;
        try appendDocumentSqlVirtualFieldsFromIndexesJsonValue(alloc, &fields, parsed.value);
    }

    var virtual_schema = DocumentSqlSchema{};
    if (fields.items.len > 0) {
        virtual_schema.fields = try fields.toOwnedSlice(alloc);
        virtual_schema.owns_fields = true;
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
    schema.* = .{};
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

fn appendDocumentSqlVirtualFieldsFromIndexesJsonValue(
    alloc: std.mem.Allocator,
    fields: *std.ArrayListUnmanaged(DocumentSqlVirtualField),
    value: std.json.Value,
) !void {
    if (value != .object) return error.InvalidSqlCatalog;

    if (value.object.get("indexes")) |indexes| {
        if (indexes != .array) return error.InvalidSqlCatalog;
        for (indexes.array.items) |index_value| {
            try appendDocumentSqlVirtualFieldsFromIndexConfigValue(alloc, fields, index_value);
        }
    }

    var it = value.object.iterator();
    while (it.next()) |entry| {
        if (std.mem.eql(u8, entry.key_ptr.*, "indexes")) continue;
        try appendDocumentSqlVirtualFieldsFromIndexConfigValue(alloc, fields, entry.value_ptr.*);
    }
}

fn appendDocumentSqlVirtualFieldsFromIndexConfigValue(
    alloc: std.mem.Allocator,
    fields: *std.ArrayListUnmanaged(DocumentSqlVirtualField),
    value: std.json.Value,
) !void {
    if (value != .object) return;
    try appendDocumentSqlVirtualFieldsFromNamedConfigValue(alloc, fields, value, "field");
    try appendDocumentSqlVirtualFieldsFromNamedConfigValue(alloc, fields, value, "path");
    try appendDocumentSqlVirtualFieldsFromNamedConfigValue(alloc, fields, value, "fields");
    try appendDocumentSqlVirtualFieldsFromNamedConfigValue(alloc, fields, value, "paths");
    try appendDocumentSqlVirtualFieldsFromNamedConfigValue(alloc, fields, value, "scalar_field");
    try appendDocumentSqlVirtualFieldsFromNamedConfigValue(alloc, fields, value, "scalar_path");
    try appendDocumentSqlVirtualFieldsFromNamedConfigValue(alloc, fields, value, "scalar_fields");
    try appendDocumentSqlVirtualFieldsFromNamedConfigValue(alloc, fields, value, "scalar_paths");
    try appendDocumentSqlVirtualFieldsFromNamedConfigValue(alloc, fields, value, "indexed_scalar_field");
    try appendDocumentSqlVirtualFieldsFromNamedConfigValue(alloc, fields, value, "indexed_scalar_path");
    try appendDocumentSqlVirtualFieldsFromNamedConfigValue(alloc, fields, value, "indexed_scalar_fields");
    try appendDocumentSqlVirtualFieldsFromNamedConfigValue(alloc, fields, value, "indexed_scalar_paths");
}

fn appendDocumentSqlVirtualFieldsFromNamedConfigValue(
    alloc: std.mem.Allocator,
    fields: *std.ArrayListUnmanaged(DocumentSqlVirtualField),
    value: std.json.Value,
    name: []const u8,
) !void {
    const field_value = value.object.get(name) orelse return;
    switch (field_value) {
        .string => |path| try appendDocumentSqlVirtualFieldFromIndexPathAlloc(alloc, fields, path),
        .array => |array| {
            for (array.items) |item| {
                if (item != .string) return error.InvalidSqlCatalog;
                try appendDocumentSqlVirtualFieldFromIndexPathAlloc(alloc, fields, item.string);
            }
        },
        else => return error.InvalidSqlCatalog,
    }
}

fn appendDocumentSqlVirtualFieldFromIndexPathAlloc(
    alloc: std.mem.Allocator,
    fields: *std.ArrayListUnmanaged(DocumentSqlVirtualField),
    path: []const u8,
) !void {
    const top_level = documentSqlTopLevelPathSegment(path) orelse return error.InvalidSqlCatalog;
    try appendDocumentSqlVirtualFieldAlloc(alloc, fields, top_level, top_level, .index_definition);
}

fn appendDocumentSqlVirtualFieldAlloc(
    alloc: std.mem.Allocator,
    fields: *std.ArrayListUnmanaged(DocumentSqlVirtualField),
    name: []const u8,
    path: []const u8,
    source: DocumentSqlVirtualFieldSource,
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
        if (std.mem.eql(u8, entry.key_ptr.*, "indexes")) continue;
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
    } else if (documentIndexConfigTypeIsScalar(type_value.string)) {
        try appendDocumentScalarFilterPathsFromIndexConfigValue(alloc, scalar_paths, value);
    }
    try appendDocumentScalarFilterPathsFromNamedConfigValue(alloc, scalar_paths, value, "scalar_field");
    try appendDocumentScalarFilterPathsFromNamedConfigValue(alloc, scalar_paths, value, "scalar_path");
    try appendDocumentScalarFilterPathsFromNamedConfigValue(alloc, scalar_paths, value, "scalar_fields");
    try appendDocumentScalarFilterPathsFromNamedConfigValue(alloc, scalar_paths, value, "scalar_paths");
    try appendDocumentScalarFilterPathsFromNamedConfigValue(alloc, scalar_paths, value, "indexed_scalar_field");
    try appendDocumentScalarFilterPathsFromNamedConfigValue(alloc, scalar_paths, value, "indexed_scalar_path");
    try appendDocumentScalarFilterPathsFromNamedConfigValue(alloc, scalar_paths, value, "indexed_scalar_fields");
    try appendDocumentScalarFilterPathsFromNamedConfigValue(alloc, scalar_paths, value, "indexed_scalar_paths");
}

fn documentIndexConfigTypeIsScalar(type_name: []const u8) bool {
    return std.mem.eql(u8, type_name, "scalar") or
        std.mem.eql(u8, type_name, "path") or
        std.mem.eql(u8, type_name, "secondary") or
        std.mem.eql(u8, type_name, "keyword") or
        std.mem.eql(u8, type_name, "numeric") or
        std.mem.eql(u8, type_name, "boolean") or
        std.mem.eql(u8, type_name, "datetime") or
        std.mem.eql(u8, type_name, "term");
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

    var scalar_path_capabilities = try documentCapabilitiesForRuntimeSchemaAndIndexesJsonAlloc(
        alloc,
        unavailable_schema,
        "{\"status_idx\":{\"type\":\"scalar\",\"field\":\"status\"},\"metadata_idx\":{\"type\":\"path\",\"paths\":[\"metadata.plan\",\"/metadata/score\"]},\"future_text\":{\"type\":\"full_text\",\"scalar_paths\":[\"tenant\"]}}",
    );
    defer deinitDocumentSqlCapabilities(alloc, &scalar_path_capabilities);
    try std.testing.expect(!scalar_path_capabilities.indexed_scalar_filters);
    try std.testing.expect(scalar_path_capabilities.full_text_filters);
    try std.testing.expectEqual(@as(usize, 4), scalar_path_capabilities.indexed_scalar_filter_paths.len);
    try std.testing.expect(documentScalarFilterPathReady(scalar_path_capabilities, "/status"));
    try std.testing.expect(documentScalarFilterPathReady(scalar_path_capabilities, "/metadata/plan"));
    try std.testing.expect(documentScalarFilterPathReady(scalar_path_capabilities, "metadata.score"));
    try std.testing.expect(documentScalarFilterPathReady(scalar_path_capabilities, "/tenant"));
    try std.testing.expect(!documentScalarFilterPathReady(scalar_path_capabilities, "/body"));

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

    var index_backed_virtual_schema = try documentSqlSchemaForRuntimeSchemaAndIndexesJsonAlloc(
        alloc,
        unavailable_schema,
        "{\"body_fts\":{\"type\":\"full_text\",\"field\":\"body\"},\"metadata_idx\":{\"type\":\"path\",\"paths\":[\"metadata.plan\",\"/tenant/id\"]}}",
    );
    defer deinitDocumentSqlSchema(alloc, &index_backed_virtual_schema);
    try std.testing.expect(index_backed_virtual_schema.exposes_doc_id);
    try std.testing.expect(index_backed_virtual_schema.exposes_doc);
    try std.testing.expectEqual(@as(usize, 4), index_backed_virtual_schema.fields.len);
    try std.testing.expectEqualStrings("status", index_backed_virtual_schema.fields[0].name);
    try std.testing.expectEqual(DocumentSqlVirtualFieldSource.declared_schema, index_backed_virtual_schema.fields[0].source);
    try std.testing.expectEqualStrings("body", index_backed_virtual_schema.fields[1].name);
    try std.testing.expectEqual(DocumentSqlVirtualFieldSource.declared_schema, index_backed_virtual_schema.fields[1].source);
    try std.testing.expectEqualStrings("metadata", index_backed_virtual_schema.fields[2].name);
    try std.testing.expectEqual(DocumentSqlVirtualFieldSource.index_definition, index_backed_virtual_schema.fields[2].source);
    try std.testing.expectEqualStrings("tenant", index_backed_virtual_schema.fields[3].name);

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
