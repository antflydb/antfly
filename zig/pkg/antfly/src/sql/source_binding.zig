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
pub const default_document_sql_bounded_scan_bytes: u64 = 8 * 1024 * 1024;

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
    /// Catalog document-to-SQL view mapping. This exposes an aliased SQL column
    /// backed by a concrete document path.
    view_mapping,
};

pub const DocumentSqlVirtualField = struct {
    name: []const u8,
    path: []const u8,
    source: DocumentSqlVirtualFieldSource,
    field_type: ?runtime_schema.AntflyType = null,
    array_item_type: ?runtime_schema.AntflyType = null,
    nullable: ?bool = null,
};

pub const DocumentSqlTypedPath = struct {
    path: []const u8,
    field_type: runtime_schema.AntflyType,
};

pub const DocumentSqlViewMappingSummary = struct {
    name: []const u8,
    source_table: []const u8 = "",
    required_indexes: usize = 0,
    required_indexes_ready: bool = false,
    source_generation_fresh: bool = false,
    source_schema_fingerprint_fresh: bool = false,
};

pub const DocumentSqlSchema = struct {
    exposes_doc_id: bool = true,
    exposes_doc: bool = true,
    fields: []const DocumentSqlVirtualField = &.{},
    owns_fields: bool = false,
    typed_paths: []const DocumentSqlTypedPath = &.{},
    owns_typed_paths: bool = false,
    view_mappings: []const DocumentSqlViewMappingSummary = &.{},
    owns_view_mappings: bool = false,
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
    indexed_array_element_paths: []const []const u8 = &.{},
    owns_indexed_array_element_paths: bool = false,
    runtime_schema_scalar_filters: ?runtime_schema.TableSchema = null,
    full_text_filters: bool = false,
    full_text_indexes: []const DocumentSqlFullTextIndex = &.{},
    owns_full_text_indexes: bool = false,
    semantic_filters: bool = false,
    semantic_index_names: []const []const u8 = &.{},
    owns_semantic_index_names: bool = false,
    vector_filters: bool = false,
    vector_index_names: []const []const u8 = &.{},
    owns_vector_index_names: bool = false,
    hybrid_filters: bool = false,
    graph_filters: bool = false,
    graph_index_names: []const []const u8 = &.{},
    owns_graph_index_names: bool = false,
    graph_metric_filters: bool = false,
    graph_metric_index_names: []const []const u8 = &.{},
    owns_graph_metric_index_names: bool = false,
    algebraic_aggregates: bool = false,
    bounded_scan: ?BoundedScanPolicy = null,
};

pub fn documentSqlSourceSchemaFingerprintAlloc(alloc: std.mem.Allocator, schema_json: []const u8) ![]u8 {
    const value = std.hash.Wyhash.hash(0x53514c46, schema_json);
    return try std.fmt.allocPrint(alloc, "schema:{x}", .{if (value == 0) @as(u64, 1) else value});
}

pub fn documentSqlIndexesJsonHasViewMappingForSourceTableAlloc(
    alloc: std.mem.Allocator,
    indexes_json: []const u8,
    view_name: []const u8,
    source_table: []const u8,
) !bool {
    if (indexes_json.len == 0) return false;
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, indexes_json, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidSqlCatalog;
    const view_mappings = parsed.value.object.get("view_mappings") orelse return false;
    return try documentSqlViewMappingsContainViewForSourceTable(view_mappings, view_name, source_table);
}

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
    return try documentCapabilitiesForRuntimeSchemaAndIndexesJsonWithBindingAlloc(alloc, schema, indexes_json, null, null);
}

pub fn documentCapabilitiesForRuntimeSchemaAndIndexesJsonWithBindingAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    indexes_json: []const u8,
    expected_source_generation: ?u64,
    expected_source_schema_fingerprint: ?[]const u8,
) !DocumentSqlCapabilities {
    var capabilities = documentCapabilitiesForRuntimeSchema(schema);
    if (indexes_json.len == 0) return capabilities;
    capabilities.indexed_scalar_filters = false;
    var scalar_paths = std.ArrayListUnmanaged([]const u8).empty;
    var array_element_paths = std.ArrayListUnmanaged([]const u8).empty;
    var full_text_indexes = std.ArrayListUnmanaged(DocumentSqlFullTextIndex).empty;
    var semantic_index_names = std.ArrayListUnmanaged([]const u8).empty;
    var vector_index_names = std.ArrayListUnmanaged([]const u8).empty;
    var graph_index_names = std.ArrayListUnmanaged([]const u8).empty;
    var graph_metric_index_names = std.ArrayListUnmanaged([]const u8).empty;
    errdefer {
        for (scalar_paths.items) |path| alloc.free(@constCast(path));
        scalar_paths.deinit(alloc);
        for (array_element_paths.items) |path| alloc.free(@constCast(path));
        array_element_paths.deinit(alloc);
        deinitDocumentSqlFullTextIndexList(alloc, &full_text_indexes);
        deinitDocumentSqlIndexNameList(alloc, &semantic_index_names);
        deinitDocumentSqlIndexNameList(alloc, &vector_index_names);
        deinitDocumentSqlIndexNameList(alloc, &graph_index_names);
        deinitDocumentSqlIndexNameList(alloc, &graph_metric_index_names);
    }
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, indexes_json, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidSqlCatalog;

    try mergeDocumentCapabilitiesFromIndexesJsonValue(alloc, &capabilities, &scalar_paths, &array_element_paths, &full_text_indexes, &semantic_index_names, &vector_index_names, &graph_index_names, &graph_metric_index_names, expected_source_generation, expected_source_schema_fingerprint, parsed.value);
    if (scalar_paths.items.len > 0) {
        capabilities.indexed_scalar_filter_paths = try scalar_paths.toOwnedSlice(alloc);
        capabilities.owns_indexed_scalar_filter_paths = true;
    }
    if (array_element_paths.items.len > 0) {
        capabilities.indexed_array_element_paths = try array_element_paths.toOwnedSlice(alloc);
        capabilities.owns_indexed_array_element_paths = true;
    }
    if (full_text_indexes.items.len > 0) {
        capabilities.full_text_indexes = try full_text_indexes.toOwnedSlice(alloc);
        capabilities.owns_full_text_indexes = true;
    }
    if (semantic_index_names.items.len > 0) {
        capabilities.semantic_index_names = try semantic_index_names.toOwnedSlice(alloc);
        capabilities.owns_semantic_index_names = true;
    }
    if (vector_index_names.items.len > 0) {
        capabilities.vector_index_names = try vector_index_names.toOwnedSlice(alloc);
        capabilities.owns_vector_index_names = true;
    }
    if (graph_index_names.items.len > 0) {
        capabilities.graph_index_names = try graph_index_names.toOwnedSlice(alloc);
        capabilities.owns_graph_index_names = true;
    }
    if (graph_metric_index_names.items.len > 0) {
        capabilities.graph_metric_index_names = try graph_metric_index_names.toOwnedSlice(alloc);
        capabilities.owns_graph_metric_index_names = true;
    }
    return capabilities;
}

pub fn documentSqlSchemaForRuntimeSchemaAndIndexesJsonAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    indexes_json: []const u8,
) !DocumentSqlSchema {
    return try documentSqlSchemaForRuntimeSchemaAndIndexesJsonWithSourceTableAlloc(alloc, schema, indexes_json, null);
}

pub fn documentSqlSchemaForRuntimeSchemaAndIndexesJsonWithSourceTableAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    indexes_json: []const u8,
    expected_source_table: ?[]const u8,
) !DocumentSqlSchema {
    return try documentSqlSchemaForRuntimeSchemaAndIndexesJsonWithBindingAlloc(alloc, schema, indexes_json, expected_source_table, null, null);
}

pub fn documentSqlSchemaForRuntimeSchemaAndIndexesJsonWithBindingAlloc(
    alloc: std.mem.Allocator,
    schema: runtime_schema.TableSchema,
    indexes_json: []const u8,
    expected_source_table: ?[]const u8,
    expected_source_generation: ?u64,
    expected_source_schema_fingerprint: ?[]const u8,
) !DocumentSqlSchema {
    var fields = std.ArrayListUnmanaged(DocumentSqlVirtualField).empty;
    var typed_paths = std.ArrayListUnmanaged(DocumentSqlTypedPath).empty;
    var view_mapping_summaries = std.ArrayListUnmanaged(DocumentSqlViewMappingSummary).empty;
    errdefer deinitDocumentSqlVirtualFieldList(alloc, &fields);
    errdefer deinitDocumentSqlTypedPathList(alloc, &typed_paths);
    errdefer deinitDocumentSqlViewMappingSummaryList(alloc, &view_mapping_summaries);

    for (schema.relational_columns) |column| {
        try appendDocumentSqlVirtualFieldWithNullabilityAlloc(alloc, &fields, column.name, column.path, .declared_schema, column.field_type, column.array_item_type, column.nullable);
    }

    if (indexes_json.len > 0) {
        var parsed = try std.json.parseFromSlice(std.json.Value, alloc, indexes_json, .{});
        defer parsed.deinit();
        if (parsed.value != .object) return error.InvalidSqlCatalog;
        if (expected_source_table) |source_table| {
            if (parsed.value.object.get("view_mappings")) |view_mappings| {
                try validateDocumentSqlViewMappingsSourceTableValue(view_mappings, source_table);
            }
        }
        if (expected_source_generation) |source_generation| {
            if (parsed.value.object.get("view_mappings")) |view_mappings| {
                try validateDocumentSqlViewMappingsSourceGenerationValue(view_mappings, source_generation);
            }
        }
        if (parsed.value.object.get("view_mappings")) |view_mappings| {
            try validateDocumentSqlViewMappingsSourceSchemaFingerprintValue(view_mappings, expected_source_schema_fingerprint);
        }
        if (parsed.value.object.get("view_mappings")) |view_mappings| {
            try validateDocumentSqlViewMappingsRequiredIndexesValue(view_mappings, parsed.value);
            try validateDocumentSqlViewMappingsDeclaredShapeValue(view_mappings, schema);
        }
        try appendDocumentSqlVirtualFieldsFromCatalogMetadataValue(alloc, &fields, parsed.value);
        try appendDocumentSqlTypedPathsFromCatalogMetadataValue(alloc, &typed_paths, parsed.value);
        if (parsed.value.object.get("view_mappings")) |mapping_value| {
            try appendDocumentSqlViewMappingSummariesValue(
                alloc,
                &view_mapping_summaries,
                mapping_value,
                parsed.value,
                expected_source_generation,
                expected_source_schema_fingerprint,
            );
        }
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
    if (view_mapping_summaries.items.len > 0) {
        virtual_schema.view_mappings = try view_mapping_summaries.toOwnedSlice(alloc);
        virtual_schema.owns_view_mappings = true;
    }
    return virtual_schema;
}

fn validateDocumentSqlViewMappingsSourceTableValue(value: std.json.Value, expected_source_table: []const u8) !void {
    if (value == .array) {
        for (value.array.items) |item| try validateDocumentSqlViewMappingSourceTableValue(item, expected_source_table);
        return;
    }
    if (value != .object) return error.InvalidSqlCatalog;
    if (value.object.get("fields") != null) {
        try validateDocumentSqlViewMappingSourceTableValue(value, expected_source_table);
        return;
    }
    var it = value.object.iterator();
    while (it.next()) |entry| try validateDocumentSqlViewMappingSourceTableValue(entry.value_ptr.*, expected_source_table);
}

fn validateDocumentSqlViewMappingSourceTableValue(value: std.json.Value, expected_source_table: []const u8) !void {
    if (value != .object) return error.InvalidSqlCatalog;
    const source_table = documentSqlStringField(value, "source_table") orelse return error.InvalidSqlCatalog;
    if (!std.ascii.eqlIgnoreCase(source_table, expected_source_table)) return error.InvalidSqlCatalog;
}

fn documentSqlViewMappingsContainViewForSourceTable(value: std.json.Value, view_name: []const u8, source_table: []const u8) !bool {
    if (value == .array) {
        for (value.array.items) |item| {
            if (try documentSqlViewMappingMatchesViewForSourceTable(item, view_name, source_table)) return true;
        }
        return false;
    }
    if (value != .object) return error.InvalidSqlCatalog;
    if (value.object.get("fields") != null) {
        return try documentSqlViewMappingMatchesViewForSourceTable(value, view_name, source_table);
    }
    var it = value.object.iterator();
    while (it.next()) |entry| {
        if (!std.ascii.eqlIgnoreCase(entry.key_ptr.*, view_name)) continue;
        return documentSqlViewMappingHasSourceTable(entry.value_ptr.*, source_table);
    }
    return false;
}

fn documentSqlViewMappingMatchesViewForSourceTable(value: std.json.Value, view_name: []const u8, source_table: []const u8) !bool {
    if (value != .object) return error.InvalidSqlCatalog;
    const name =
        documentSqlStringField(value, "name") orelse
        documentSqlStringField(value, "view") orelse
        documentSqlStringField(value, "view_name") orelse
        return false;
    if (!std.ascii.eqlIgnoreCase(name, view_name)) return false;
    return documentSqlViewMappingHasSourceTable(value, source_table);
}

fn documentSqlViewMappingHasSourceTable(value: std.json.Value, source_table: []const u8) bool {
    if (value != .object) return false;
    const actual = documentSqlStringField(value, "source_table") orelse return false;
    return std.ascii.eqlIgnoreCase(actual, source_table);
}

fn appendDocumentSqlViewMappingSummariesValue(
    alloc: std.mem.Allocator,
    summaries: *std.ArrayListUnmanaged(DocumentSqlViewMappingSummary),
    value: std.json.Value,
    catalog_metadata: std.json.Value,
    expected_source_generation: ?u64,
    expected_source_schema_fingerprint: ?[]const u8,
) !void {
    if (value == .array) {
        for (value.array.items) |item| {
            try appendDocumentSqlViewMappingSummaryValue(alloc, summaries, item, null, catalog_metadata, expected_source_generation, expected_source_schema_fingerprint);
        }
        return;
    }
    if (value != .object) return error.InvalidSqlCatalog;
    if (value.object.get("fields") != null) {
        try appendDocumentSqlViewMappingSummaryValue(alloc, summaries, value, null, catalog_metadata, expected_source_generation, expected_source_schema_fingerprint);
        return;
    }
    var it = value.object.iterator();
    while (it.next()) |entry| {
        try appendDocumentSqlViewMappingSummaryValue(alloc, summaries, entry.value_ptr.*, entry.key_ptr.*, catalog_metadata, expected_source_generation, expected_source_schema_fingerprint);
    }
}

fn appendDocumentSqlViewMappingSummaryValue(
    alloc: std.mem.Allocator,
    summaries: *std.ArrayListUnmanaged(DocumentSqlViewMappingSummary),
    value: std.json.Value,
    fallback_name: ?[]const u8,
    catalog_metadata: std.json.Value,
    expected_source_generation: ?u64,
    expected_source_schema_fingerprint: ?[]const u8,
) !void {
    if (value != .object) return error.InvalidSqlCatalog;
    const name =
        documentSqlStringField(value, "name") orelse
        documentSqlStringField(value, "view") orelse
        documentSqlStringField(value, "view_name") orelse
        fallback_name orelse
        return;
    const source_table = documentSqlStringField(value, "source_table") orelse "";
    const required_indexes = try documentSqlViewMappingRequiredIndexCount(value, catalog_metadata);
    const source_generation_fresh = if (expected_source_generation) |expected|
        documentSqlViewMappingOptionalSourceGenerationMatches(value, expected)
    else
        false;
    const source_schema_fingerprint_fresh = if (expected_source_schema_fingerprint) |expected|
        try documentSqlViewMappingOptionalSourceSchemaFingerprintMatches(value, expected)
    else
        false;
    const owned_name = try alloc.dupe(u8, name);
    errdefer alloc.free(owned_name);
    const owned_source_table = try alloc.dupe(u8, source_table);
    errdefer alloc.free(owned_source_table);
    try summaries.append(alloc, .{
        .name = owned_name,
        .source_table = owned_source_table,
        .required_indexes = required_indexes,
        .required_indexes_ready = required_indexes > 0,
        .source_generation_fresh = source_generation_fresh,
        .source_schema_fingerprint_fresh = source_schema_fingerprint_fresh,
    });
}

fn documentSqlViewMappingRequiredIndexCount(value: std.json.Value, catalog_metadata: std.json.Value) !usize {
    const required_indexes = value.object.get("required_indexes") orelse return 0;
    if (required_indexes != .array) return error.InvalidSqlCatalog;
    for (required_indexes.array.items) |item| {
        const name = try documentSqlRequiredIndexName(item);
        _ = documentSqlCatalogIndexConfigByName(catalog_metadata, name) orelse return error.InvalidSqlCatalog;
    }
    return required_indexes.array.items.len;
}

fn documentSqlViewMappingOptionalSourceGenerationMatches(value: std.json.Value, expected: u64) bool {
    const actual = documentSqlOptionalU64Field(value, "source_generation") catch return false;
    return if (actual) |generation| generation == expected else false;
}

fn documentSqlViewMappingOptionalSourceSchemaFingerprintMatches(value: std.json.Value, expected: []const u8) !bool {
    const actual =
        try documentSqlOptionalStringField(value, "source_schema_fingerprint") orelse
        try documentSqlOptionalStringField(value, "schema_fingerprint") orelse
        return false;
    return std.mem.eql(u8, actual, expected);
}

fn validateDocumentSqlViewMappingsSourceGenerationValue(value: std.json.Value, expected_source_generation: u64) !void {
    if (value == .array) {
        for (value.array.items) |item| try validateDocumentSqlViewMappingSourceGenerationValue(item, expected_source_generation);
        return;
    }
    if (value != .object) return error.InvalidSqlCatalog;
    if (value.object.get("fields") != null) {
        try validateDocumentSqlViewMappingSourceGenerationValue(value, expected_source_generation);
        return;
    }
    var it = value.object.iterator();
    while (it.next()) |entry| try validateDocumentSqlViewMappingSourceGenerationValue(entry.value_ptr.*, expected_source_generation);
}

fn validateDocumentSqlViewMappingSourceGenerationValue(value: std.json.Value, expected_source_generation: u64) !void {
    if (value != .object) return error.InvalidSqlCatalog;
    const actual = try documentSqlOptionalU64Field(value, "source_generation") orelse return;
    if (actual != expected_source_generation) return error.InvalidSqlCatalog;
}

fn validateDocumentSqlViewMappingsSourceSchemaFingerprintValue(value: std.json.Value, expected_source_schema_fingerprint: ?[]const u8) !void {
    if (value == .array) {
        for (value.array.items) |item| try validateDocumentSqlViewMappingSourceSchemaFingerprintValue(item, expected_source_schema_fingerprint);
        return;
    }
    if (value != .object) return error.InvalidSqlCatalog;
    if (value.object.get("fields") != null) {
        try validateDocumentSqlViewMappingSourceSchemaFingerprintValue(value, expected_source_schema_fingerprint);
        return;
    }
    var it = value.object.iterator();
    while (it.next()) |entry| try validateDocumentSqlViewMappingSourceSchemaFingerprintValue(entry.value_ptr.*, expected_source_schema_fingerprint);
}

fn validateDocumentSqlViewMappingSourceSchemaFingerprintValue(value: std.json.Value, expected_source_schema_fingerprint: ?[]const u8) !void {
    if (value != .object) return error.InvalidSqlCatalog;
    const actual =
        try documentSqlOptionalStringField(value, "source_schema_fingerprint") orelse
        try documentSqlOptionalStringField(value, "schema_fingerprint") orelse
        return;
    if (actual.len == 0) return error.InvalidSqlCatalog;
    if (expected_source_schema_fingerprint) |expected| {
        if (!std.mem.eql(u8, actual, expected)) return error.InvalidSqlCatalog;
    }
}

fn validateDocumentSqlViewMappingsRequiredIndexesValue(value: std.json.Value, catalog_metadata: std.json.Value) !void {
    if (value == .array) {
        for (value.array.items) |item| try validateDocumentSqlViewMappingRequiredIndexesValue(item, catalog_metadata);
        return;
    }
    if (value != .object) return error.InvalidSqlCatalog;
    if (value.object.get("fields") != null) {
        try validateDocumentSqlViewMappingRequiredIndexesValue(value, catalog_metadata);
        return;
    }
    var it = value.object.iterator();
    while (it.next()) |entry| try validateDocumentSqlViewMappingRequiredIndexesValue(entry.value_ptr.*, catalog_metadata);
}

fn validateDocumentSqlViewMappingRequiredIndexesValue(value: std.json.Value, catalog_metadata: std.json.Value) !void {
    if (value != .object) return error.InvalidSqlCatalog;
    const required_indexes = value.object.get("required_indexes") orelse return;
    if (required_indexes != .array) return error.InvalidSqlCatalog;
    for (required_indexes.array.items) |item| {
        try validateDocumentSqlViewMappingRequiredIndexValue(item, catalog_metadata);
    }
}

fn validateDocumentSqlViewMappingRequiredIndexValue(value: std.json.Value, catalog_metadata: std.json.Value) !void {
    const name = try documentSqlRequiredIndexName(value);
    const index = documentSqlCatalogIndexConfigByName(catalog_metadata, name) orelse return error.InvalidSqlCatalog;
    try validateDocumentSqlCatalogIndexReady(index);
    if (value == .object) {
        try validateDocumentSqlRequiredIndexStringMetadata(value, index, "lifecycle", &.{ "index_lifecycle", "status", "index_status" });
        try validateDocumentSqlRequiredIndexU64Metadata(value, index, "generation", &.{"index_generation"});
        try validateDocumentSqlRequiredIndexU64Metadata(value, index, "source_generation", &.{"source_schema_generation"});
    }
}

fn documentSqlRequiredIndexName(value: std.json.Value) ![]const u8 {
    switch (value) {
        .string => |name| {
            if (name.len == 0) return error.InvalidSqlCatalog;
            return name;
        },
        .object => {
            const name =
                try documentSqlOptionalStringField(value, "name") orelse
                try documentSqlOptionalStringField(value, "index") orelse
                try documentSqlOptionalStringField(value, "index_name") orelse
                return error.InvalidSqlCatalog;
            if (name.len == 0) return error.InvalidSqlCatalog;
            return name;
        },
        else => return error.InvalidSqlCatalog,
    }
}

fn validateDocumentSqlCatalogIndexReady(value: std.json.Value) !void {
    if (try documentSqlOptionalStringFieldAny(value, "lifecycle", &.{ "index_lifecycle", "status", "index_status" })) |lifecycle| {
        if (!std.mem.eql(u8, lifecycle, "ready")) return error.InvalidSqlCatalog;
    }
}

fn validateDocumentSqlDerivedIndexCurrent(
    value: std.json.Value,
    expected_source_generation: ?u64,
    expected_source_schema_fingerprint: ?[]const u8,
) !void {
    if (try documentSqlOptionalStringFieldAny(value, "lifecycle", &.{ "index_lifecycle", "status", "index_status" })) |lifecycle| {
        if (!std.mem.eql(u8, lifecycle, "ready")) return error.InvalidSqlCatalog;
    }
    if (try documentSqlOptionalStringFieldAny(value, "capability_lifecycle_status", &.{ "capability_status", "freshness", "freshness_status" })) |status| {
        if (!std.mem.eql(u8, status, "current")) return error.InvalidSqlCatalog;
    }
    try validateDocumentSqlDerivedIndexGeneration(value);
    if (expected_source_generation) |expected| {
        if (try documentSqlOptionalU64FieldAny(value, "source_generation", &.{"source_schema_generation"})) |actual| {
            if (actual != expected) return error.InvalidSqlCatalog;
        }
    }
    if (expected_source_schema_fingerprint) |expected| {
        if (try documentSqlOptionalStringFieldAny(value, "source_schema_fingerprint", &.{"schema_fingerprint"})) |actual| {
            if (!std.mem.eql(u8, actual, expected)) return error.InvalidSqlCatalog;
        }
    }
}

fn validateDocumentSqlDerivedIndexGeneration(value: std.json.Value) !void {
    const index_generation = try documentSqlOptionalU64Field(value, "index_generation");
    const generation = try documentSqlOptionalU64Field(value, "generation");
    if (index_generation != null and generation != null and index_generation.? != generation.?) {
        return error.InvalidSqlCatalog;
    }
    const actual_generation = index_generation orelse generation;
    const expected_generation = try documentSqlOptionalU64FieldAny(
        value,
        "expected_index_generation",
        &.{ "required_index_generation", "expected_generation" },
    ) orelse return;
    if (actual_generation == null or actual_generation.? != expected_generation) {
        return error.InvalidSqlCatalog;
    }
}

fn validateDocumentSqlRequiredIndexStringMetadata(
    required: std.json.Value,
    index: std.json.Value,
    primary_name: []const u8,
    alternate_names: []const []const u8,
) !void {
    const expected = try documentSqlOptionalStringFieldAny(required, primary_name, alternate_names) orelse return;
    if (!std.mem.eql(u8, expected, "ready")) return error.InvalidSqlCatalog;
    const actual = try documentSqlOptionalStringFieldAny(index, primary_name, alternate_names) orelse return error.InvalidSqlCatalog;
    if (!std.mem.eql(u8, actual, expected)) return error.InvalidSqlCatalog;
}

fn validateDocumentSqlRequiredIndexU64Metadata(
    required: std.json.Value,
    index: std.json.Value,
    primary_name: []const u8,
    alternate_names: []const []const u8,
) !void {
    const expected = try documentSqlOptionalU64FieldAny(required, primary_name, alternate_names) orelse return;
    const actual = try documentSqlOptionalU64FieldAny(index, primary_name, alternate_names) orelse return error.InvalidSqlCatalog;
    if (actual != expected) return error.InvalidSqlCatalog;
}

fn documentSqlCatalogIndexNameExists(catalog_metadata: std.json.Value, name: []const u8) bool {
    return documentSqlCatalogIndexConfigByName(catalog_metadata, name) != null;
}

fn documentSqlCatalogIndexConfigByName(catalog_metadata: std.json.Value, name: []const u8) ?std.json.Value {
    if (catalog_metadata != .object) return null;
    if (catalog_metadata.object.get("indexes")) |indexes| {
        if (indexes == .array) {
            for (indexes.array.items) |index| {
                if (documentSqlIndexConfigNameMatches(index, null, name)) return index;
            }
        }
    }
    var it = catalog_metadata.object.iterator();
    while (it.next()) |entry| {
        if (std.mem.eql(u8, entry.key_ptr.*, "indexes") or
            std.mem.eql(u8, entry.key_ptr.*, "typed_paths") or
            std.mem.eql(u8, entry.key_ptr.*, "view_mappings") or
            std.mem.eql(u8, entry.key_ptr.*, "resolvers") or
            std.mem.eql(u8, entry.key_ptr.*, "enrichments")) continue;
        if (documentSqlIndexConfigNameMatches(entry.value_ptr.*, entry.key_ptr.*, name)) return entry.value_ptr.*;
    }
    return null;
}

fn documentSqlIndexConfigNameMatches(value: std.json.Value, object_key: ?[]const u8, name: []const u8) bool {
    if (object_key) |key| {
        if (std.mem.eql(u8, key, name)) return true;
    }
    if (value != .object) return false;
    if (documentSqlStringField(value, "name")) |candidate| {
        if (std.mem.eql(u8, candidate, name)) return true;
    }
    if (documentSqlStringField(value, "index")) |candidate| {
        if (std.mem.eql(u8, candidate, name)) return true;
    }
    if (documentSqlStringField(value, "index_name")) |candidate| {
        if (std.mem.eql(u8, candidate, name)) return true;
    }
    return false;
}

fn validateDocumentSqlViewMappingsDeclaredShapeValue(value: std.json.Value, schema: runtime_schema.TableSchema) !void {
    if (value == .array) {
        for (value.array.items) |item| try validateDocumentSqlViewMappingDeclaredShapeValue(item, schema);
        return;
    }
    if (value != .object) return error.InvalidSqlCatalog;
    if (value.object.get("fields") != null) {
        try validateDocumentSqlViewMappingDeclaredShapeValue(value, schema);
        return;
    }
    var it = value.object.iterator();
    while (it.next()) |entry| try validateDocumentSqlViewMappingDeclaredShapeValue(entry.value_ptr.*, schema);
}

fn validateDocumentSqlViewMappingDeclaredShapeValue(value: std.json.Value, schema: runtime_schema.TableSchema) !void {
    if (value != .object) return error.InvalidSqlCatalog;
    const field_values = value.object.get("fields") orelse return error.InvalidSqlCatalog;
    try validateDocumentSqlViewMappingFieldsDeclaredShapeValue(field_values, schema);
}

fn validateDocumentSqlViewMappingFieldsDeclaredShapeValue(value: std.json.Value, schema: runtime_schema.TableSchema) !void {
    switch (value) {
        .array => |array| {
            for (array.items) |item| try validateDocumentSqlViewMappingFieldDeclaredShapeValue(item, schema);
        },
        .object => |object| {
            var it = object.iterator();
            while (it.next()) |entry| try validateDocumentSqlViewMappingNamedFieldDeclaredShapeValue(entry.value_ptr.*, schema);
        },
        else => return error.InvalidSqlCatalog,
    }
}

fn validateDocumentSqlViewMappingNamedFieldDeclaredShapeValue(value: std.json.Value, schema: runtime_schema.TableSchema) !void {
    if (value == .string) {
        try validateDocumentSqlViewMappingPathAgainstDeclaredShapeValue(value.string, null, null, null, schema);
        return;
    }
    try validateDocumentSqlViewMappingFieldDeclaredShapeValue(value, schema);
}

fn validateDocumentSqlViewMappingFieldDeclaredShapeValue(value: std.json.Value, schema: runtime_schema.TableSchema) !void {
    if (value != .object) return error.InvalidSqlCatalog;
    const raw_path = documentSqlStringField(value, "path") orelse documentSqlStringField(value, "field") orelse return error.InvalidSqlCatalog;
    const field_type = try documentSqlFieldTypeFromViewMappingFieldValue(value);
    const array_item_type = try documentSqlArrayItemTypeFromViewMappingFieldValue(value);
    const nullable = try documentSqlOptionalBoolField(value, "nullable");
    try validateDocumentSqlViewMappingPathAgainstDeclaredShapeValue(raw_path, field_type, array_item_type, nullable, schema);
}

fn validateDocumentSqlViewMappingPathAgainstDeclaredShapeValue(
    path: []const u8,
    field_type: ?runtime_schema.AntflyType,
    array_item_type: ?runtime_schema.AntflyType,
    nullable: ?bool,
    schema: runtime_schema.TableSchema,
) !void {
    if (documentColumnForPath(schema, path)) |column| {
        if (column.default_value != null or column.on_update_value != null or column.generated != null) return error.InvalidSqlCatalog;
        if (field_type) |declared_type| {
            if (declared_type != column.field_type) return error.InvalidSqlCatalog;
        }
        if (array_item_type) |declared_item_type| {
            if (column.array_item_type != declared_item_type) return error.InvalidSqlCatalog;
        }
        if (nullable) |declared_nullable| {
            if (declared_nullable != column.nullable) return error.InvalidSqlCatalog;
        }
    }
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
    if (schema.owns_view_mappings) {
        for (schema.view_mappings) |mapping| {
            alloc.free(@constCast(mapping.name));
            if (mapping.source_table.len > 0) alloc.free(@constCast(mapping.source_table));
        }
        alloc.free(schema.view_mappings);
    }
    schema.* = .{};
}

pub fn documentSqlTypedPathType(schema: DocumentSqlSchema, path: []const u8) ?runtime_schema.AntflyType {
    for (schema.typed_paths) |candidate| {
        if (documentScalarFilterPathEqual(candidate.path, path)) return candidate.field_type;
    }
    return null;
}

pub fn documentSqlViewMappingSummaryForView(schema: DocumentSqlSchema, view_name: []const u8) ?DocumentSqlViewMappingSummary {
    for (schema.view_mappings) |mapping| {
        if (std.ascii.eqlIgnoreCase(mapping.name, view_name)) return mapping;
    }
    return null;
}

pub fn deinitDocumentSqlCapabilities(alloc: std.mem.Allocator, capabilities: *DocumentSqlCapabilities) void {
    if (capabilities.owns_indexed_scalar_filter_paths) {
        for (capabilities.indexed_scalar_filter_paths) |path| alloc.free(@constCast(path));
        alloc.free(capabilities.indexed_scalar_filter_paths);
    }
    if (capabilities.owns_indexed_array_element_paths) {
        for (capabilities.indexed_array_element_paths) |path| alloc.free(@constCast(path));
        alloc.free(capabilities.indexed_array_element_paths);
    }
    if (capabilities.owns_full_text_indexes) {
        for (capabilities.full_text_indexes) |index| {
            alloc.free(@constCast(index.name));
            for (index.paths) |path| alloc.free(@constCast(path));
            if (index.paths.len > 0) alloc.free(index.paths);
        }
        alloc.free(capabilities.full_text_indexes);
    }
    if (capabilities.owns_semantic_index_names) {
        for (capabilities.semantic_index_names) |name| alloc.free(@constCast(name));
        alloc.free(capabilities.semantic_index_names);
    }
    if (capabilities.owns_vector_index_names) {
        for (capabilities.vector_index_names) |name| alloc.free(@constCast(name));
        alloc.free(capabilities.vector_index_names);
    }
    if (capabilities.owns_graph_index_names) {
        for (capabilities.graph_index_names) |name| alloc.free(@constCast(name));
        alloc.free(capabilities.graph_index_names);
    }
    if (capabilities.owns_graph_metric_index_names) {
        for (capabilities.graph_metric_index_names) |name| alloc.free(@constCast(name));
        alloc.free(capabilities.graph_metric_index_names);
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
            std.mem.eql(u8, entry.key_ptr.*, "view_mappings") or
            std.mem.eql(u8, entry.key_ptr.*, "resolvers") or
            std.mem.eql(u8, entry.key_ptr.*, "enrichments")) continue;
        try appendDocumentSqlVirtualFieldsFromIndexDefinitionValue(alloc, fields, entry.value_ptr.*);
    }

    if (value.object.get("typed_paths")) |typed_paths| {
        try appendDocumentSqlVirtualFieldsFromTypedPathsValue(alloc, fields, typed_paths);
    }
    if (value.object.get("view_mappings")) |view_mappings| {
        try appendDocumentSqlVirtualFieldsFromViewMappingsValue(alloc, fields, view_mappings);
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

fn appendDocumentSqlVirtualFieldsFromViewMappingsValue(
    alloc: std.mem.Allocator,
    fields: *std.ArrayListUnmanaged(DocumentSqlVirtualField),
    value: std.json.Value,
) !void {
    if (value == .array) {
        for (value.array.items) |item| try appendDocumentSqlVirtualFieldsFromViewMappingValue(alloc, fields, item);
        return;
    }
    if (value != .object) return error.InvalidSqlCatalog;
    if (value.object.get("fields") != null) {
        try appendDocumentSqlVirtualFieldsFromViewMappingValue(alloc, fields, value);
        return;
    }
    var it = value.object.iterator();
    while (it.next()) |entry| {
        try appendDocumentSqlVirtualFieldsFromViewMappingValue(alloc, fields, entry.value_ptr.*);
    }
}

fn appendDocumentSqlVirtualFieldsFromViewMappingValue(
    alloc: std.mem.Allocator,
    fields: *std.ArrayListUnmanaged(DocumentSqlVirtualField),
    value: std.json.Value,
) !void {
    if (value != .object) return error.InvalidSqlCatalog;
    const field_values = value.object.get("fields") orelse return error.InvalidSqlCatalog;
    try appendDocumentSqlVirtualFieldsFromViewMappingFieldsValue(alloc, fields, field_values);
}

fn appendDocumentSqlVirtualFieldsFromViewMappingFieldsValue(
    alloc: std.mem.Allocator,
    fields: *std.ArrayListUnmanaged(DocumentSqlVirtualField),
    value: std.json.Value,
) !void {
    switch (value) {
        .array => |array| {
            for (array.items) |item| try appendDocumentSqlVirtualFieldFromViewMappingFieldValue(alloc, fields, item);
        },
        .object => |object| {
            var it = object.iterator();
            while (it.next()) |entry| {
                try appendDocumentSqlVirtualFieldFromViewMappingNamedFieldValue(alloc, fields, entry.key_ptr.*, entry.value_ptr.*);
            }
        },
        else => return error.InvalidSqlCatalog,
    }
}

fn appendDocumentSqlVirtualFieldFromViewMappingNamedFieldValue(
    alloc: std.mem.Allocator,
    fields: *std.ArrayListUnmanaged(DocumentSqlVirtualField),
    name: []const u8,
    value: std.json.Value,
) !void {
    if (value == .string) {
        const path = try documentSqlViewMappingPathAlloc(alloc, value.string);
        defer alloc.free(path);
        try appendDocumentSqlViewMappingVirtualFieldAlloc(alloc, fields, name, path, null, null, null);
        return;
    }
    if (value != .object) return error.InvalidSqlCatalog;
    const raw_path = documentSqlStringField(value, "path") orelse documentSqlStringField(value, "field") orelse return error.InvalidSqlCatalog;
    const path = try documentSqlViewMappingPathAlloc(alloc, raw_path);
    defer alloc.free(path);
    const field_type = try documentSqlFieldTypeFromViewMappingFieldValue(value);
    const array_item_type = try documentSqlArrayItemTypeFromViewMappingFieldValue(value);
    const nullable = try documentSqlOptionalBoolField(value, "nullable");
    try appendDocumentSqlViewMappingVirtualFieldAlloc(alloc, fields, name, path, field_type, array_item_type, nullable);
}

fn appendDocumentSqlVirtualFieldFromViewMappingFieldValue(
    alloc: std.mem.Allocator,
    fields: *std.ArrayListUnmanaged(DocumentSqlVirtualField),
    value: std.json.Value,
) !void {
    if (value != .object) return error.InvalidSqlCatalog;
    const name = documentSqlStringField(value, "name") orelse documentSqlStringField(value, "alias") orelse return error.InvalidSqlCatalog;
    const raw_path = documentSqlStringField(value, "path") orelse documentSqlStringField(value, "field") orelse return error.InvalidSqlCatalog;
    const path = try documentSqlViewMappingPathAlloc(alloc, raw_path);
    defer alloc.free(path);
    const field_type = try documentSqlFieldTypeFromViewMappingFieldValue(value);
    const array_item_type = try documentSqlArrayItemTypeFromViewMappingFieldValue(value);
    const nullable = try documentSqlOptionalBoolField(value, "nullable");
    try appendDocumentSqlViewMappingVirtualFieldAlloc(alloc, fields, name, path, field_type, array_item_type, nullable);
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

fn documentSqlFieldTypeFromViewMappingFieldValue(value: std.json.Value) !?runtime_schema.AntflyType {
    const type_value = value.object.get("type") orelse value.object.get("kind") orelse return null;
    if (type_value != .string) return error.InvalidSqlCatalog;
    return documentSqlFieldTypeFromTypedPathTypeName(type_value.string) orelse error.InvalidSqlCatalog;
}

fn documentSqlFieldTypeFromTypedPathTypeName(type_name: []const u8) ?runtime_schema.AntflyType {
    if (std.mem.eql(u8, type_name, "keyword") or std.mem.eql(u8, type_name, "term")) return .keyword;
    if (std.mem.eql(u8, type_name, "text")) return .text;
    if (std.mem.eql(u8, type_name, "numeric")) return .numeric;
    if (std.mem.eql(u8, type_name, "boolean")) return .boolean;
    if (std.mem.eql(u8, type_name, "datetime")) return .datetime;
    if (std.mem.eql(u8, type_name, "array")) return .array;
    return null;
}

fn documentSqlArrayItemTypeFromViewMappingFieldValue(value: std.json.Value) !?runtime_schema.AntflyType {
    const item_value = value.object.get("item_type") orelse
        value.object.get("item_kind") orelse
        value.object.get("array_item_type") orelse
        value.object.get("array_item_kind") orelse
        value.object.get("items") orelse return null;
    switch (item_value) {
        .string => |item_type| return documentSqlFieldTypeFromTypedPathTypeName(item_type) orelse error.InvalidSqlCatalog,
        .object => |object| {
            const type_value = object.get("type") orelse object.get("kind") orelse return error.InvalidSqlCatalog;
            if (type_value != .string) return error.InvalidSqlCatalog;
            return documentSqlFieldTypeFromTypedPathTypeName(type_value.string) orelse error.InvalidSqlCatalog;
        },
        else => return error.InvalidSqlCatalog,
    }
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
    if (value.object.get("view_mappings")) |view_mappings| {
        try appendDocumentSqlTypedPathsFromViewMappingsValue(alloc, typed_paths, view_mappings);
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

fn appendDocumentSqlTypedPathsFromViewMappingsValue(
    alloc: std.mem.Allocator,
    typed_paths: *std.ArrayListUnmanaged(DocumentSqlTypedPath),
    value: std.json.Value,
) !void {
    if (value == .array) {
        for (value.array.items) |item| try appendDocumentSqlTypedPathsFromViewMappingValue(alloc, typed_paths, item);
        return;
    }
    if (value != .object) return error.InvalidSqlCatalog;
    if (value.object.get("fields") != null) {
        try appendDocumentSqlTypedPathsFromViewMappingValue(alloc, typed_paths, value);
        return;
    }
    var it = value.object.iterator();
    while (it.next()) |entry| {
        try appendDocumentSqlTypedPathsFromViewMappingValue(alloc, typed_paths, entry.value_ptr.*);
    }
}

fn appendDocumentSqlTypedPathsFromViewMappingValue(
    alloc: std.mem.Allocator,
    typed_paths: *std.ArrayListUnmanaged(DocumentSqlTypedPath),
    value: std.json.Value,
) !void {
    if (value != .object) return error.InvalidSqlCatalog;
    const field_values = value.object.get("fields") orelse return error.InvalidSqlCatalog;
    try appendDocumentSqlTypedPathsFromViewMappingFieldsValue(alloc, typed_paths, field_values);
}

fn appendDocumentSqlTypedPathsFromViewMappingFieldsValue(
    alloc: std.mem.Allocator,
    typed_paths: *std.ArrayListUnmanaged(DocumentSqlTypedPath),
    value: std.json.Value,
) !void {
    switch (value) {
        .array => |array| {
            for (array.items) |item| try appendDocumentSqlTypedPathFromViewMappingFieldValue(alloc, typed_paths, item);
        },
        .object => |object| {
            var it = object.iterator();
            while (it.next()) |entry| try appendDocumentSqlTypedPathFromViewMappingFieldValue(alloc, typed_paths, entry.value_ptr.*);
        },
        else => return error.InvalidSqlCatalog,
    }
}

fn appendDocumentSqlTypedPathFromViewMappingFieldValue(
    alloc: std.mem.Allocator,
    typed_paths: *std.ArrayListUnmanaged(DocumentSqlTypedPath),
    value: std.json.Value,
) !void {
    if (value != .object) return;
    const field_type = (try documentSqlFieldTypeFromViewMappingFieldValue(value)) orelse return;
    const raw_path = documentSqlStringField(value, "path") orelse documentSqlStringField(value, "field") orelse return error.InvalidSqlCatalog;
    const path = try documentSqlViewMappingPathAlloc(alloc, raw_path);
    defer alloc.free(path);
    try appendDocumentSqlTypedPathAlloc(alloc, typed_paths, path, field_type);
}

fn documentSqlViewMappingPathAlloc(alloc: std.mem.Allocator, path: []const u8) ![]u8 {
    if (path.len == 0) return error.InvalidSqlCatalog;
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.append(alloc, '/');
    var start: usize = if (path[0] == '/') 1 else 0;
    if (start >= path.len) return error.InvalidSqlCatalog;
    var previous_was_separator = false;
    while (start < path.len) : (start += 1) {
        const c = path[start];
        if (c == '/' or c == '.') {
            if (previous_was_separator) return error.InvalidSqlCatalog;
            previous_was_separator = true;
            try out.append(alloc, '/');
            continue;
        }
        previous_was_separator = false;
        try out.append(alloc, c);
    }
    if (previous_was_separator) return error.InvalidSqlCatalog;
    return try out.toOwnedSlice(alloc);
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
    return try appendDocumentSqlVirtualFieldWithNullabilityAlloc(alloc, fields, name, path, source, field_type, null, null);
}

fn appendDocumentSqlViewMappingVirtualFieldAlloc(
    alloc: std.mem.Allocator,
    fields: *std.ArrayListUnmanaged(DocumentSqlVirtualField),
    name: []const u8,
    path: []const u8,
    field_type: ?runtime_schema.AntflyType,
    array_item_type: ?runtime_schema.AntflyType,
    nullable: ?bool,
) !void {
    if (documentSqlDeclaredVirtualFieldCanBecomeViewMapping(fields, name, path, field_type, array_item_type, nullable)) |index| {
        fields.items[index].source = .view_mapping;
        if (field_type) |field_type_value| fields.items[index].field_type = field_type_value;
        if (array_item_type) |array_item_type_value| fields.items[index].array_item_type = array_item_type_value;
        if (nullable) |nullable_value| fields.items[index].nullable = nullable_value;
        return;
    }
    if (documentSqlVirtualFieldNameExists(fields.*, name)) return error.InvalidSqlCatalog;
    try appendDocumentSqlVirtualFieldWithNullabilityAlloc(alloc, fields, name, path, .view_mapping, field_type, array_item_type, nullable);
}

fn documentSqlDeclaredVirtualFieldCanBecomeViewMapping(
    fields: *std.ArrayListUnmanaged(DocumentSqlVirtualField),
    name: []const u8,
    path: []const u8,
    field_type: ?runtime_schema.AntflyType,
    array_item_type: ?runtime_schema.AntflyType,
    nullable: ?bool,
) ?usize {
    for (fields.items, 0..) |field, index| {
        if (!std.ascii.eqlIgnoreCase(field.name, name)) continue;
        if (field.source != .declared_schema) return null;
        if (!documentScalarFilterPathEqual(field.path, path)) return null;
        if (field_type) |expected| {
            if (field.field_type == null or field.field_type.? != expected) return null;
        }
        if (array_item_type) |expected| {
            if (field.array_item_type == null or field.array_item_type.? != expected) return null;
        }
        if (nullable) |expected| {
            if (field.nullable == null or field.nullable.? != expected) return null;
        }
        return index;
    }
    return null;
}

fn appendDocumentSqlVirtualFieldWithNullabilityAlloc(
    alloc: std.mem.Allocator,
    fields: *std.ArrayListUnmanaged(DocumentSqlVirtualField),
    name: []const u8,
    path: []const u8,
    source: DocumentSqlVirtualFieldSource,
    field_type: ?runtime_schema.AntflyType,
    array_item_type: ?runtime_schema.AntflyType,
    nullable: ?bool,
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
        .array_item_type = array_item_type,
        .nullable = nullable,
    });
}

fn documentSqlVirtualFieldNameExists(fields: std.ArrayListUnmanaged(DocumentSqlVirtualField), name: []const u8) bool {
    for (fields.items) |existing| {
        if (std.ascii.eqlIgnoreCase(existing.name, name)) return true;
    }
    return false;
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

fn deinitDocumentSqlViewMappingSummaryList(
    alloc: std.mem.Allocator,
    summaries: *std.ArrayListUnmanaged(DocumentSqlViewMappingSummary),
) void {
    for (summaries.items) |summary| {
        alloc.free(@constCast(summary.name));
        if (summary.source_table.len > 0) alloc.free(@constCast(summary.source_table));
    }
    summaries.deinit(alloc);
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
    array_element_paths: *std.ArrayListUnmanaged([]const u8),
    full_text_indexes: *std.ArrayListUnmanaged(DocumentSqlFullTextIndex),
    semantic_index_names: *std.ArrayListUnmanaged([]const u8),
    vector_index_names: *std.ArrayListUnmanaged([]const u8),
    graph_index_names: *std.ArrayListUnmanaged([]const u8),
    graph_metric_index_names: *std.ArrayListUnmanaged([]const u8),
    expected_source_generation: ?u64,
    expected_source_schema_fingerprint: ?[]const u8,
    value: std.json.Value,
) !void {
    if (value != .object) return error.InvalidSqlCatalog;

    if (value.object.get("indexes")) |indexes| {
        if (indexes != .array) return error.InvalidSqlCatalog;
        for (indexes.array.items) |index_value| {
            try mergeDocumentCapabilitiesFromIndexConfigValue(alloc, capabilities, scalar_paths, array_element_paths, full_text_indexes, semantic_index_names, vector_index_names, graph_index_names, graph_metric_index_names, expected_source_generation, expected_source_schema_fingerprint, null, index_value);
        }
    }

    var it = value.object.iterator();
    while (it.next()) |entry| {
        if (std.mem.eql(u8, entry.key_ptr.*, "indexes") or
            std.mem.eql(u8, entry.key_ptr.*, "typed_paths") or
            std.mem.eql(u8, entry.key_ptr.*, "resolvers") or
            std.mem.eql(u8, entry.key_ptr.*, "enrichments")) continue;
        try mergeDocumentCapabilitiesFromIndexConfigValue(alloc, capabilities, scalar_paths, array_element_paths, full_text_indexes, semantic_index_names, vector_index_names, graph_index_names, graph_metric_index_names, expected_source_generation, expected_source_schema_fingerprint, entry.key_ptr.*, entry.value_ptr.*);
    }
}

fn mergeDocumentCapabilitiesFromIndexConfigValue(
    alloc: std.mem.Allocator,
    capabilities: *DocumentSqlCapabilities,
    scalar_paths: *std.ArrayListUnmanaged([]const u8),
    array_element_paths: *std.ArrayListUnmanaged([]const u8),
    full_text_indexes: *std.ArrayListUnmanaged(DocumentSqlFullTextIndex),
    semantic_index_names: *std.ArrayListUnmanaged([]const u8),
    vector_index_names: *std.ArrayListUnmanaged([]const u8),
    graph_index_names: *std.ArrayListUnmanaged([]const u8),
    graph_metric_index_names: *std.ArrayListUnmanaged([]const u8),
    expected_source_generation: ?u64,
    expected_source_schema_fingerprint: ?[]const u8,
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
        try validateDocumentSqlDerivedIndexCurrent(value, expected_source_generation, expected_source_schema_fingerprint);
        capabilities.semantic_filters = true;
        capabilities.vector_filters = true;
        try appendDocumentSqlIndexNameFromConfigValue(alloc, semantic_index_names, index_name, value);
        try appendDocumentSqlIndexNameFromConfigValue(alloc, vector_index_names, index_name, value);
    } else if (std.mem.eql(u8, type_value.string, "hybrid")) {
        try validateDocumentSqlDerivedIndexCurrent(value, expected_source_generation, expected_source_schema_fingerprint);
        capabilities.hybrid_filters = true;
        capabilities.vector_filters = true;
    } else if (std.mem.eql(u8, type_value.string, "graph")) {
        try validateDocumentSqlDerivedIndexCurrent(value, expected_source_generation, expected_source_schema_fingerprint);
        capabilities.graph_filters = true;
        try appendDocumentSqlIndexNameFromConfigValue(alloc, graph_index_names, index_name, value);
    } else if (std.mem.eql(u8, type_value.string, "graph_metric")) {
        try validateDocumentSqlDerivedIndexCurrent(value, expected_source_generation, expected_source_schema_fingerprint);
        capabilities.graph_metric_filters = true;
        try appendDocumentSqlGraphMetricIndexNameFromConfigValue(alloc, graph_metric_index_names, value);
    } else if (std.mem.eql(u8, type_value.string, "array_element") or
        std.mem.eql(u8, type_value.string, "array_elements"))
    {
        try appendDocumentScalarFilterPathsFromIndexConfigValue(alloc, array_element_paths, value);
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

fn documentSqlStringField(value: std.json.Value, name: []const u8) ?[]const u8 {
    if (value != .object) return null;
    const field = value.object.get(name) orelse return null;
    if (field != .string) return null;
    return field.string;
}

fn documentSqlOptionalStringField(value: std.json.Value, name: []const u8) !?[]const u8 {
    if (value != .object) return null;
    const field = value.object.get(name) orelse return null;
    if (field != .string) return error.InvalidSqlCatalog;
    return field.string;
}

fn documentSqlOptionalStringFieldAny(value: std.json.Value, primary_name: []const u8, alternate_names: []const []const u8) !?[]const u8 {
    if (try documentSqlOptionalStringField(value, primary_name)) |field| return field;
    for (alternate_names) |name| {
        if (try documentSqlOptionalStringField(value, name)) |field| return field;
    }
    return null;
}

fn documentSqlOptionalBoolField(value: std.json.Value, name: []const u8) !?bool {
    if (value != .object) return null;
    const field = value.object.get(name) orelse return null;
    if (field != .bool) return error.InvalidSqlCatalog;
    return field.bool;
}

fn documentSqlOptionalU64Field(value: std.json.Value, name: []const u8) !?u64 {
    if (value != .object) return null;
    const field = value.object.get(name) orelse return null;
    if (field != .integer) return error.InvalidSqlCatalog;
    return std.math.cast(u64, field.integer) orelse error.InvalidSqlCatalog;
}

fn documentSqlOptionalU64FieldAny(value: std.json.Value, primary_name: []const u8, alternate_names: []const []const u8) !?u64 {
    if (try documentSqlOptionalU64Field(value, primary_name)) |field| return field;
    for (alternate_names) |name| {
        if (try documentSqlOptionalU64Field(value, name)) |field| return field;
    }
    return null;
}

fn appendDocumentSqlIndexNameFromConfigValue(
    alloc: std.mem.Allocator,
    names: *std.ArrayListUnmanaged([]const u8),
    index_name: ?[]const u8,
    value: std.json.Value,
) !void {
    const name = index_name orelse
        documentSqlStringField(value, "name") orelse
        documentSqlStringField(value, "index") orelse
        documentSqlStringField(value, "index_name") orelse
        return;
    if (name.len == 0) return error.InvalidSqlCatalog;
    for (names.items) |existing| {
        if (std.mem.eql(u8, existing, name)) return;
    }
    try names.append(alloc, try alloc.dupe(u8, name));
}

fn appendDocumentSqlGraphMetricIndexNameFromConfigValue(
    alloc: std.mem.Allocator,
    names: *std.ArrayListUnmanaged([]const u8),
    value: std.json.Value,
) !void {
    const name = documentSqlStringField(value, "graph_index") orelse
        documentSqlStringField(value, "index") orelse
        documentSqlStringField(value, "index_name") orelse
        return;
    if (name.len == 0) return error.InvalidSqlCatalog;
    for (names.items) |existing| {
        if (std.mem.eql(u8, existing, name)) return;
    }
    try names.append(alloc, try alloc.dupe(u8, name));
}

fn deinitDocumentSqlIndexNameList(
    alloc: std.mem.Allocator,
    names: *std.ArrayListUnmanaged([]const u8),
) void {
    for (names.items) |name| alloc.free(@constCast(name));
    names.deinit(alloc);
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
    try std.testing.expectEqual(@as(usize, 2), derived_index_capabilities.semantic_index_names.len);
    try std.testing.expectEqualStrings("semantic_idx", derived_index_capabilities.semantic_index_names[0]);
    try std.testing.expectEqualStrings("aknn_idx", derived_index_capabilities.semantic_index_names[1]);
    try std.testing.expectEqual(@as(usize, 2), derived_index_capabilities.vector_index_names.len);
    try std.testing.expectEqualStrings("semantic_idx", derived_index_capabilities.vector_index_names[0]);
    try std.testing.expectEqualStrings("aknn_idx", derived_index_capabilities.vector_index_names[1]);
    try std.testing.expectEqual(@as(usize, 1), derived_index_capabilities.graph_index_names.len);
    try std.testing.expectEqualStrings("graph_idx", derived_index_capabilities.graph_index_names[0]);
    try std.testing.expectEqual(@as(usize, 0), derived_index_capabilities.graph_metric_index_names.len);

    var constrained_graph_metric_capabilities = try documentCapabilitiesForRuntimeSchemaAndIndexesJsonAlloc(
        alloc,
        unavailable_schema,
        "{\"pagerank_idx\":{\"type\":\"graph_metric\",\"graph_index\":\"docs_edge_graph\"}}",
    );
    defer deinitDocumentSqlCapabilities(alloc, &constrained_graph_metric_capabilities);
    try std.testing.expect(constrained_graph_metric_capabilities.graph_metric_filters);
    try std.testing.expectEqual(@as(usize, 1), constrained_graph_metric_capabilities.graph_metric_index_names.len);
    try std.testing.expectEqualStrings("docs_edge_graph", constrained_graph_metric_capabilities.graph_metric_index_names[0]);

    var constrained_hybrid_capabilities = try documentCapabilitiesForRuntimeSchemaAndIndexesJsonAlloc(
        alloc,
        unavailable_schema,
        "{\"docs_body_fts\":{\"type\":\"full_text\"},\"docs_body_semantic\":{\"type\":\"semantic\"},\"docs_hybrid\":{\"type\":\"hybrid\"}}",
    );
    defer deinitDocumentSqlCapabilities(alloc, &constrained_hybrid_capabilities);
    try std.testing.expect(constrained_hybrid_capabilities.hybrid_filters);
    try std.testing.expectEqual(@as(usize, 1), constrained_hybrid_capabilities.full_text_indexes.len);
    try std.testing.expectEqualStrings("docs_body_fts", constrained_hybrid_capabilities.full_text_indexes[0].name);
    try std.testing.expectEqual(@as(usize, 1), constrained_hybrid_capabilities.semantic_index_names.len);
    try std.testing.expectEqualStrings("docs_body_semantic", constrained_hybrid_capabilities.semantic_index_names[0]);

    var current_derived_index_capabilities = try documentCapabilitiesForRuntimeSchemaAndIndexesJsonAlloc(
        alloc,
        unavailable_schema,
        "{\"semantic_idx\":{\"type\":\"semantic\",\"lifecycle\":\"ready\",\"capability_lifecycle_status\":\"current\",\"index_generation\":4,\"expected_index_generation\":4},\"graph_idx\":{\"type\":\"graph\",\"status\":\"ready\",\"generation\":5,\"required_index_generation\":5},\"pagerank_idx\":{\"type\":\"graph_metric\",\"graph_index\":\"docs_edge_graph\",\"capability_status\":\"current\",\"index_generation\":6,\"generation\":6,\"expected_generation\":6}}",
    );
    defer deinitDocumentSqlCapabilities(alloc, &current_derived_index_capabilities);
    try std.testing.expect(current_derived_index_capabilities.semantic_filters);
    try std.testing.expect(current_derived_index_capabilities.graph_filters);
    try std.testing.expect(current_derived_index_capabilities.graph_metric_filters);

    try std.testing.expectError(
        error.InvalidSqlCatalog,
        documentCapabilitiesForRuntimeSchemaAndIndexesJsonAlloc(
            alloc,
            unavailable_schema,
            "{\"semantic_idx\":{\"type\":\"semantic\",\"lifecycle\":\"building\"}}",
        ),
    );
    try std.testing.expectError(
        error.InvalidSqlCatalog,
        documentCapabilitiesForRuntimeSchemaAndIndexesJsonAlloc(
            alloc,
            unavailable_schema,
            "{\"pagerank_idx\":{\"type\":\"graph_metric\",\"graph_index\":\"docs_edge_graph\",\"capability_lifecycle_status\":\"stale\"}}",
        ),
    );
    try std.testing.expectError(
        error.InvalidSqlCatalog,
        documentCapabilitiesForRuntimeSchemaAndIndexesJsonAlloc(
            alloc,
            unavailable_schema,
            "{\"semantic_idx\":{\"type\":\"semantic\",\"index_generation\":4,\"generation\":5}}",
        ),
    );
    try std.testing.expectError(
        error.InvalidSqlCatalog,
        documentCapabilitiesForRuntimeSchemaAndIndexesJsonAlloc(
            alloc,
            unavailable_schema,
            "{\"graph_idx\":{\"type\":\"graph\",\"expected_index_generation\":5}}",
        ),
    );
    try std.testing.expectError(
        error.InvalidSqlCatalog,
        documentCapabilitiesForRuntimeSchemaAndIndexesJsonAlloc(
            alloc,
            unavailable_schema,
            "{\"pagerank_idx\":{\"type\":\"graph_metric\",\"graph_index\":\"docs_edge_graph\",\"index_generation\":5,\"expected_index_generation\":6}}",
        ),
    );
    var bound_derived_index_capabilities = try documentCapabilitiesForRuntimeSchemaAndIndexesJsonWithBindingAlloc(
        alloc,
        unavailable_schema,
        "{\"semantic_idx\":{\"type\":\"semantic\",\"source_generation\":7,\"source_schema_fingerprint\":\"schema-v7\"}}",
        7,
        "schema-v7",
    );
    defer deinitDocumentSqlCapabilities(alloc, &bound_derived_index_capabilities);
    try std.testing.expect(bound_derived_index_capabilities.semantic_filters);
    try std.testing.expectError(
        error.InvalidSqlCatalog,
        documentCapabilitiesForRuntimeSchemaAndIndexesJsonWithBindingAlloc(
            alloc,
            unavailable_schema,
            "{\"semantic_idx\":{\"type\":\"semantic\",\"source_generation\":8}}",
            7,
            "schema-v7",
        ),
    );
    try std.testing.expectError(
        error.InvalidSqlCatalog,
        documentCapabilitiesForRuntimeSchemaAndIndexesJsonWithBindingAlloc(
            alloc,
            unavailable_schema,
            "{\"semantic_idx\":{\"type\":\"semantic\",\"source_schema_fingerprint\":\"schema-v8\"}}",
            7,
            "schema-v7",
        ),
    );

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

    var array_element_capabilities = try documentCapabilitiesForRuntimeSchemaAndIndexesJsonAlloc(
        alloc,
        unavailable_schema,
        "{\"tags_array_element\":{\"type\":\"array_element\",\"path\":\"tags\"}}",
    );
    defer deinitDocumentSqlCapabilities(alloc, &array_element_capabilities);
    try std.testing.expectEqual(@as(usize, 1), array_element_capabilities.indexed_array_element_paths.len);
    try std.testing.expectEqualStrings("tags", array_element_capabilities.indexed_array_element_paths[0]);
    try std.testing.expect(!documentScalarFilterPathReady(array_element_capabilities, "/tags"));

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

    var view_mapping_virtual_schema = try documentSqlSchemaForRuntimeSchemaAndIndexesJsonAlloc(
        alloc,
        unavailable_schema,
        "{\"view_mappings\":{\"support_view\":{\"source_table\":\"docs\",\"fields\":[{\"name\":\"title_text\",\"path\":\"title\",\"type\":\"text\"},{\"name\":\"plan\",\"path\":\"metadata.plan\",\"type\":\"keyword\",\"nullable\":false},{\"name\":\"region\",\"path\":\"metadata.region\",\"type\":\"keyword\",\"nullable\":true},{\"name\":\"score\",\"path\":\"metrics.score\",\"type\":\"numeric\",\"nullable\":true},{\"name\":\"published\",\"path\":\"published\",\"type\":\"boolean\",\"nullable\":true},{\"name\":\"tag_list\",\"path\":\"tags\",\"type\":\"array\",\"item_type\":\"keyword\"}]}}}",
    );
    defer deinitDocumentSqlSchema(alloc, &view_mapping_virtual_schema);
    try std.testing.expectEqual(@as(usize, 8), view_mapping_virtual_schema.fields.len);
    try std.testing.expectEqual(@as(usize, 6), view_mapping_virtual_schema.typed_paths.len);
    try std.testing.expectEqual(runtime_schema.AntflyType.text, documentSqlTypedPathType(view_mapping_virtual_schema, "/title").?);
    try std.testing.expectEqual(runtime_schema.AntflyType.keyword, documentSqlTypedPathType(view_mapping_virtual_schema, "/metadata/plan").?);
    try std.testing.expectEqual(runtime_schema.AntflyType.keyword, documentSqlTypedPathType(view_mapping_virtual_schema, "/metadata/region").?);
    try std.testing.expectEqual(runtime_schema.AntflyType.numeric, documentSqlTypedPathType(view_mapping_virtual_schema, "/metrics/score").?);
    try std.testing.expectEqual(runtime_schema.AntflyType.boolean, documentSqlTypedPathType(view_mapping_virtual_schema, "/published").?);
    try std.testing.expectEqual(runtime_schema.AntflyType.array, documentSqlTypedPathType(view_mapping_virtual_schema, "/tags").?);
    try std.testing.expectEqual(@as(usize, 1), view_mapping_virtual_schema.view_mappings.len);
    try std.testing.expectEqualStrings("support_view", view_mapping_virtual_schema.view_mappings[0].name);
    try std.testing.expectEqual(@as(usize, 0), view_mapping_virtual_schema.view_mappings[0].required_indexes);
    try std.testing.expect(!view_mapping_virtual_schema.view_mappings[0].required_indexes_ready);
    try std.testing.expect(documentSqlViewMappingSummaryForView(view_mapping_virtual_schema, "support_view") != null);
    var saw_title_text_view_field = false;
    var saw_plan_view_field = false;
    var saw_region_view_field = false;
    var saw_score_view_field = false;
    var saw_published_view_field = false;
    var saw_tags_view_field = false;
    for (view_mapping_virtual_schema.fields) |field| {
        if (std.mem.eql(u8, field.name, "title_text")) {
            saw_title_text_view_field = true;
            try std.testing.expectEqual(DocumentSqlVirtualFieldSource.view_mapping, field.source);
            try std.testing.expectEqualStrings("/title", field.path);
            try std.testing.expectEqual(runtime_schema.AntflyType.text, field.field_type.?);
        }
        if (std.mem.eql(u8, field.name, "plan")) {
            saw_plan_view_field = true;
            try std.testing.expectEqual(DocumentSqlVirtualFieldSource.view_mapping, field.source);
            try std.testing.expectEqualStrings("/metadata/plan", field.path);
            try std.testing.expectEqual(runtime_schema.AntflyType.keyword, field.field_type.?);
            try std.testing.expectEqual(false, field.nullable.?);
        }
        if (std.mem.eql(u8, field.name, "region")) {
            saw_region_view_field = true;
            try std.testing.expectEqual(DocumentSqlVirtualFieldSource.view_mapping, field.source);
            try std.testing.expectEqualStrings("/metadata/region", field.path);
            try std.testing.expectEqual(runtime_schema.AntflyType.keyword, field.field_type.?);
            try std.testing.expectEqual(true, field.nullable.?);
        }
        if (std.mem.eql(u8, field.name, "score")) {
            saw_score_view_field = true;
            try std.testing.expectEqual(DocumentSqlVirtualFieldSource.view_mapping, field.source);
            try std.testing.expectEqualStrings("/metrics/score", field.path);
            try std.testing.expectEqual(runtime_schema.AntflyType.numeric, field.field_type.?);
            try std.testing.expectEqual(true, field.nullable.?);
        }
        if (std.mem.eql(u8, field.name, "published")) {
            saw_published_view_field = true;
            try std.testing.expectEqual(DocumentSqlVirtualFieldSource.view_mapping, field.source);
            try std.testing.expectEqualStrings("/published", field.path);
            try std.testing.expectEqual(runtime_schema.AntflyType.boolean, field.field_type.?);
            try std.testing.expectEqual(true, field.nullable.?);
        }
        if (std.mem.eql(u8, field.name, "tag_list")) {
            saw_tags_view_field = true;
            try std.testing.expectEqual(DocumentSqlVirtualFieldSource.view_mapping, field.source);
            try std.testing.expectEqualStrings("/tags", field.path);
            try std.testing.expectEqual(runtime_schema.AntflyType.array, field.field_type.?);
            try std.testing.expectEqual(runtime_schema.AntflyType.keyword, field.array_item_type.?);
        }
    }
    try std.testing.expect(saw_title_text_view_field);
    try std.testing.expect(saw_plan_view_field);
    try std.testing.expect(saw_region_view_field);
    try std.testing.expect(saw_score_view_field);
    try std.testing.expect(saw_published_view_field);
    try std.testing.expect(saw_tags_view_field);
    try std.testing.expectError(error.InvalidSqlCatalog, documentSqlSchemaForRuntimeSchemaAndIndexesJsonWithSourceTableAlloc(
        alloc,
        unavailable_schema,
        "{\"view_mappings\":{\"support_view\":{\"source_table\":\"other_docs\",\"fields\":[{\"name\":\"plan\",\"path\":\"metadata.plan\",\"type\":\"keyword\"}]}}}",
        "docs",
    ));
    var source_generation_view_schema = try documentSqlSchemaForRuntimeSchemaAndIndexesJsonWithBindingAlloc(
        alloc,
        unavailable_schema,
        "{\"view_mappings\":{\"support_view\":{\"source_table\":\"docs\",\"source_generation\":7,\"fields\":[{\"name\":\"plan\",\"path\":\"metadata.plan\",\"type\":\"keyword\"}]}}}",
        "docs",
        7,
        null,
    );
    defer deinitDocumentSqlSchema(alloc, &source_generation_view_schema);
    try std.testing.expectEqual(runtime_schema.AntflyType.keyword, documentSqlTypedPathType(source_generation_view_schema, "/metadata/plan").?);
    try std.testing.expect(source_generation_view_schema.view_mappings[0].source_generation_fresh);
    try std.testing.expectError(error.InvalidSqlCatalog, documentSqlSchemaForRuntimeSchemaAndIndexesJsonWithBindingAlloc(
        alloc,
        unavailable_schema,
        "{\"view_mappings\":{\"support_view\":{\"source_table\":\"docs\",\"source_generation\":8,\"fields\":[{\"name\":\"plan\",\"path\":\"metadata.plan\",\"type\":\"keyword\"}]}}}",
        "docs",
        7,
        null,
    ));
    var source_schema_fingerprint_view_schema = try documentSqlSchemaForRuntimeSchemaAndIndexesJsonWithBindingAlloc(
        alloc,
        unavailable_schema,
        "{\"view_mappings\":{\"support_view\":{\"source_table\":\"docs\",\"source_schema_fingerprint\":\"schema-v7\",\"fields\":[{\"name\":\"plan\",\"path\":\"metadata.plan\",\"type\":\"keyword\"}]}}}",
        "docs",
        null,
        "schema-v7",
    );
    defer deinitDocumentSqlSchema(alloc, &source_schema_fingerprint_view_schema);
    try std.testing.expectEqual(runtime_schema.AntflyType.keyword, documentSqlTypedPathType(source_schema_fingerprint_view_schema, "/metadata/plan").?);
    try std.testing.expect(source_schema_fingerprint_view_schema.view_mappings[0].source_schema_fingerprint_fresh);
    try std.testing.expectError(error.InvalidSqlCatalog, documentSqlSchemaForRuntimeSchemaAndIndexesJsonWithBindingAlloc(
        alloc,
        unavailable_schema,
        "{\"view_mappings\":{\"support_view\":{\"source_table\":\"docs\",\"source_schema_fingerprint\":\"schema-v8\",\"fields\":[{\"name\":\"plan\",\"path\":\"metadata.plan\",\"type\":\"keyword\"}]}}}",
        "docs",
        null,
        "schema-v7",
    ));
    try std.testing.expectError(error.InvalidSqlCatalog, documentSqlSchemaForRuntimeSchemaAndIndexesJsonWithBindingAlloc(
        alloc,
        unavailable_schema,
        "{\"view_mappings\":{\"support_view\":{\"source_table\":\"docs\",\"source_schema_fingerprint\":7,\"fields\":[{\"name\":\"plan\",\"path\":\"metadata.plan\",\"type\":\"keyword\"}]}}}",
        "docs",
        null,
        "schema-v7",
    ));
    var required_index_view_schema = try documentSqlSchemaForRuntimeSchemaAndIndexesJsonWithSourceTableAlloc(
        alloc,
        unavailable_schema,
        "{\"view_mappings\":{\"support_view\":{\"source_table\":\"docs\",\"required_indexes\":[\"plan_fts\"],\"fields\":[{\"name\":\"plan\",\"path\":\"metadata.plan\",\"type\":\"keyword\"}]}},\"plan_fts\":{\"type\":\"full_text\",\"field\":\"body\"}}",
        "docs",
    );
    defer deinitDocumentSqlSchema(alloc, &required_index_view_schema);
    try std.testing.expectEqual(runtime_schema.AntflyType.keyword, documentSqlTypedPathType(required_index_view_schema, "/metadata/plan").?);
    try std.testing.expectEqual(@as(usize, 1), required_index_view_schema.view_mappings[0].required_indexes);
    try std.testing.expect(required_index_view_schema.view_mappings[0].required_indexes_ready);
    try std.testing.expectError(error.InvalidSqlCatalog, documentSqlSchemaForRuntimeSchemaAndIndexesJsonWithSourceTableAlloc(
        alloc,
        unavailable_schema,
        "{\"view_mappings\":{\"support_view\":{\"source_table\":\"docs\",\"required_indexes\":[\"missing_fts\"],\"fields\":[{\"name\":\"plan\",\"path\":\"metadata.plan\",\"type\":\"keyword\"}]}}}",
        "docs",
    ));
    var required_index_metadata_view_schema = try documentSqlSchemaForRuntimeSchemaAndIndexesJsonWithSourceTableAlloc(
        alloc,
        unavailable_schema,
        "{\"view_mappings\":{\"support_view\":{\"source_table\":\"docs\",\"required_indexes\":[{\"name\":\"plan_fts\",\"lifecycle\":\"ready\",\"generation\":4,\"source_generation\":7}],\"fields\":[{\"name\":\"plan\",\"path\":\"metadata.plan\",\"type\":\"keyword\"}]}},\"plan_fts\":{\"type\":\"full_text\",\"field\":\"body\",\"lifecycle\":\"ready\",\"generation\":4,\"source_generation\":7}}",
        "docs",
    );
    defer deinitDocumentSqlSchema(alloc, &required_index_metadata_view_schema);
    try std.testing.expectEqual(runtime_schema.AntflyType.keyword, documentSqlTypedPathType(required_index_metadata_view_schema, "/metadata/plan").?);
    try std.testing.expectEqual(@as(usize, 1), required_index_metadata_view_schema.view_mappings[0].required_indexes);
    try std.testing.expect(required_index_metadata_view_schema.view_mappings[0].required_indexes_ready);
    try std.testing.expectError(error.InvalidSqlCatalog, documentSqlSchemaForRuntimeSchemaAndIndexesJsonWithSourceTableAlloc(
        alloc,
        unavailable_schema,
        "{\"view_mappings\":{\"support_view\":{\"source_table\":\"docs\",\"required_indexes\":[{\"name\":\"plan_fts\",\"lifecycle\":\"ready\"}],\"fields\":[{\"name\":\"plan\",\"path\":\"metadata.plan\",\"type\":\"keyword\"}]}},\"plan_fts\":{\"type\":\"full_text\",\"field\":\"body\",\"lifecycle\":\"building\"}}",
        "docs",
    ));
    try std.testing.expectError(error.InvalidSqlCatalog, documentSqlSchemaForRuntimeSchemaAndIndexesJsonWithSourceTableAlloc(
        alloc,
        unavailable_schema,
        "{\"view_mappings\":{\"support_view\":{\"source_table\":\"docs\",\"required_indexes\":[{\"name\":\"plan_fts\",\"generation\":5}],\"fields\":[{\"name\":\"plan\",\"path\":\"metadata.plan\",\"type\":\"keyword\"}]}},\"plan_fts\":{\"type\":\"full_text\",\"field\":\"body\",\"generation\":4}}",
        "docs",
    ));
    try std.testing.expectError(error.InvalidSqlCatalog, documentSqlSchemaForRuntimeSchemaAndIndexesJsonWithSourceTableAlloc(
        alloc,
        unavailable_schema,
        "{\"view_mappings\":{\"support_view\":{\"source_table\":\"docs\",\"required_indexes\":[{\"name\":\"plan_fts\",\"source_generation\":8}],\"fields\":[{\"name\":\"plan\",\"path\":\"metadata.plan\",\"type\":\"keyword\"}]}},\"plan_fts\":{\"type\":\"full_text\",\"field\":\"body\",\"source_generation\":7}}",
        "docs",
    ));
    try std.testing.expectError(error.InvalidSqlCatalog, documentSqlSchemaForRuntimeSchemaAndIndexesJsonWithSourceTableAlloc(
        alloc,
        unavailable_schema,
        "{\"view_mappings\":{\"support_view\":{\"source_table\":\"docs\",\"fields\":[{\"name\":\"plan\",\"path\":\"metadata.plan\",\"type\":\"keyword\",\"nullable\":\"sometimes\"}]}}}",
        "docs",
    ));
    try std.testing.expectError(error.InvalidSqlCatalog, documentSqlSchemaForRuntimeSchemaAndIndexesJsonWithSourceTableAlloc(
        alloc,
        unavailable_schema,
        "{\"view_mappings\":{\"support_view\":{\"source_table\":\"docs\",\"fields\":[{\"name\":\"plan\",\"path\":\"metadata.plan\",\"type\":\"object\"}]}}}",
        "docs",
    ));
    try std.testing.expectError(error.InvalidSqlCatalog, documentSqlSchemaForRuntimeSchemaAndIndexesJsonWithSourceTableAlloc(
        alloc,
        unavailable_schema,
        "{\"view_mappings\":{\"support_view\":{\"source_table\":\"docs\",\"fields\":[{\"name\":\"plan\",\"path\":\"metadata.plan\",\"type\":\"keyword\"},{\"name\":\"PLAN\",\"path\":\"metadata.alt_plan\",\"type\":\"keyword\"}]}}}",
        "docs",
    ));
    try std.testing.expectError(error.InvalidSqlCatalog, documentSqlSchemaForRuntimeSchemaAndIndexesJsonWithSourceTableAlloc(
        alloc,
        unavailable_schema,
        "{\"view_mappings\":{\"support_view\":{\"source_table\":\"docs\",\"fields\":[{\"path\":\"metadata.plan\",\"type\":\"keyword\"}]}}}",
        "docs",
    ));
    try std.testing.expectError(error.InvalidSqlCatalog, documentSqlSchemaForRuntimeSchemaAndIndexesJsonWithSourceTableAlloc(
        alloc,
        unavailable_schema,
        "{\"view_mappings\":{\"support_view\":{\"source_table\":\"docs\",\"fields\":[{\"name\":\"plan\",\"path\":\"metadata..plan\",\"type\":\"keyword\"}]}}}",
        "docs",
    ));
    var declared_path_view_schema = try documentSqlSchemaForRuntimeSchemaAndIndexesJsonWithSourceTableAlloc(
        alloc,
        unavailable_schema,
        "{\"view_mappings\":{\"support_view\":{\"source_table\":\"docs\",\"fields\":[{\"name\":\"mapped_status\",\"path\":\"status\",\"type\":\"keyword\",\"nullable\":true}]}}}",
        "docs",
    );
    defer deinitDocumentSqlSchema(alloc, &declared_path_view_schema);
    var saw_mapped_status = false;
    for (declared_path_view_schema.fields) |field| {
        if (std.mem.eql(u8, field.name, "mapped_status")) {
            saw_mapped_status = true;
            try std.testing.expectEqual(runtime_schema.AntflyType.keyword, field.field_type.?);
            try std.testing.expectEqual(true, field.nullable.?);
        }
    }
    try std.testing.expect(saw_mapped_status);
    var same_name_declared_path_view_schema = try documentSqlSchemaForRuntimeSchemaAndIndexesJsonWithSourceTableAlloc(
        alloc,
        unavailable_schema,
        "{\"view_mappings\":{\"support_view\":{\"source_table\":\"docs\",\"fields\":[{\"name\":\"status\",\"path\":\"status\",\"type\":\"keyword\",\"nullable\":true}]}}}",
        "docs",
    );
    defer deinitDocumentSqlSchema(alloc, &same_name_declared_path_view_schema);
    var saw_status_view_overlay = false;
    for (same_name_declared_path_view_schema.fields) |field| {
        if (std.mem.eql(u8, field.name, "status")) {
            saw_status_view_overlay = true;
            try std.testing.expectEqual(DocumentSqlVirtualFieldSource.view_mapping, field.source);
            try std.testing.expectEqualStrings("status", field.path);
            try std.testing.expectEqual(runtime_schema.AntflyType.keyword, field.field_type.?);
            try std.testing.expectEqual(true, field.nullable.?);
        }
    }
    try std.testing.expect(saw_status_view_overlay);
    try std.testing.expectError(error.InvalidSqlCatalog, documentSqlSchemaForRuntimeSchemaAndIndexesJsonWithSourceTableAlloc(
        alloc,
        unavailable_schema,
        "{\"view_mappings\":{\"support_view\":{\"source_table\":\"docs\",\"fields\":[{\"name\":\"status\",\"path\":\"body\",\"type\":\"text\",\"nullable\":true}]}}}",
        "docs",
    ));
    try std.testing.expectError(error.InvalidSqlCatalog, documentSqlSchemaForRuntimeSchemaAndIndexesJsonWithSourceTableAlloc(
        alloc,
        unavailable_schema,
        "{\"view_mappings\":{\"support_view\":{\"source_table\":\"docs\",\"fields\":[{\"name\":\"mapped_status\",\"path\":\"status\",\"type\":\"numeric\",\"nullable\":true}]}}}",
        "docs",
    ));
    try std.testing.expectError(error.InvalidSqlCatalog, documentSqlSchemaForRuntimeSchemaAndIndexesJsonWithSourceTableAlloc(
        alloc,
        unavailable_schema,
        "{\"view_mappings\":{\"support_view\":{\"source_table\":\"docs\",\"fields\":[{\"name\":\"mapped_status\",\"path\":\"status\",\"type\":\"keyword\",\"nullable\":false}]}}}",
        "docs",
    ));
    var generated_default_parsed = try schema_api.parseValidatedTableSchema(alloc,
        \\{"version":1,"storage_mode":"document","default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","properties":{"status":{"type":"keyword","default":"new"},"status_lower":{"type":"keyword","generated":{"op":"lower","field":"status"}}},"required":[],"additionalProperties":false}}}}
    );
    defer generated_default_parsed.deinit(alloc);
    const generated_default_schema = try schema_api.deriveRuntimeTableSchema(alloc, generated_default_parsed);
    defer runtime_schema.freeSchema(alloc, generated_default_schema);
    try std.testing.expectError(error.InvalidSqlCatalog, documentSqlSchemaForRuntimeSchemaAndIndexesJsonWithSourceTableAlloc(
        alloc,
        generated_default_schema,
        "{\"view_mappings\":{\"support_view\":{\"source_table\":\"docs\",\"fields\":[{\"name\":\"status\",\"path\":\"status\",\"type\":\"keyword\"}]}}}",
        "docs",
    ));
    try std.testing.expectError(error.InvalidSqlCatalog, documentSqlSchemaForRuntimeSchemaAndIndexesJsonWithSourceTableAlloc(
        alloc,
        generated_default_schema,
        "{\"view_mappings\":{\"support_view\":{\"source_table\":\"docs\",\"fields\":[{\"name\":\"status_lower\",\"path\":\"status_lower\",\"type\":\"keyword\"}]}}}",
        "docs",
    ));

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
