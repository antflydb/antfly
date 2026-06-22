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

pub const RelationalBinding = struct {
    target: CatalogTableRef,
    schema: runtime_schema.TableSchema,
};

pub const DocumentSqlSchema = struct {
    exposes_doc_id: bool = true,
    exposes_doc: bool = true,
};

pub const DocumentSqlCapabilities = struct {
    doc_id_lookup: bool = true,
    indexed_scalar_filters: bool = false,
    full_text_filters: bool = false,
    vector_filters: bool = false,
    bounded_scan: ?BoundedScanPolicy = null,
};

pub const DocumentBinding = struct {
    target: CatalogTableRef,
    schema: runtime_schema.TableSchema,
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
        } },
        .lake => .{ .lake = .{
            .target = target,
            .schema = schema,
        } },
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
        },
        else => return error.TestExpectedEqual,
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
