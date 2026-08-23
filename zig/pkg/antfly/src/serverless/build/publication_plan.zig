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
const Allocator = std.mem.Allocator;
const catalog_types = @import("../catalog/types.zig");
const builder_mod = @import("builder.zig");
const search_sources = @import("../search_sources.zig");
const full_text_indexes = @import("../../api/full_text_indexes.zig");
const external_binding = @import("../external_source/catalog_binding.zig");
const external_source_manifest = @import("external_source_manifest.zig");
const external_source_plan_resolver_api = @import("external_source_plan_resolver_api.zig");
const manifest_base_source = @import("../manifest/base_source.zig");

pub const OwnedExternalTableBinding = struct {
    binding: external_binding.Binding,
    table_id: []u8,
    source_uri: []u8,
    credential_ref_id: ?[]u8 = null,
    credential_scope: ?[]u8 = null,
    snapshot_value: ?[]u8 = null,
    schema_fingerprint: []u8,

    pub fn deinit(self: *OwnedExternalTableBinding, alloc: Allocator) void {
        alloc.free(self.table_id);
        alloc.free(self.source_uri);
        if (self.credential_ref_id) |value| alloc.free(value);
        if (self.credential_scope) |value| alloc.free(value);
        if (self.snapshot_value) |value| alloc.free(value);
        alloc.free(self.schema_fingerprint);
        self.* = undefined;
    }
};

pub const ExternalSourcePlanResolveRequest = external_source_plan_resolver_api.ResolveRequest;
pub const ExternalSourcePlanResolver = external_source_plan_resolver_api.Resolver;

pub const ArtifactAction = enum {
    reuse,
    rebuild,
    drop,
};

pub const DerivedOutputAction = enum {
    reuse,
    recompute,
    drop,
};

pub const FullTextIndexAction = struct {
    name: []u8,
    action: ArtifactAction,
    source_mode: full_text_indexes.FullTextSourceMode = .document,
    chunked_source_count: usize = 0,

    pub fn deinit(self: *FullTextIndexAction, alloc: Allocator) void {
        alloc.free(self.name);
        self.* = undefined;
    }
};

pub const NamedArtifactAction = struct {
    name: []u8,
    action: ArtifactAction,

    pub fn deinit(self: *NamedArtifactAction, alloc: Allocator) void {
        alloc.free(self.name);
        self.* = undefined;
    }
};

pub const MetadataRepublishReasons = struct {
    read_schema_migration: bool = false,
    published_search_sources_changed: bool = false,
    artifact_families_changed: bool = false,
    chunk_preview_policy_changed: bool = false,
    chunk_embeddings_policy_changed: bool = false,
    rerank_terms_policy_changed: bool = false,
    graph_metric_policy_changed: bool = false,

    pub fn any(self: MetadataRepublishReasons) bool {
        return self.read_schema_migration or
            self.published_search_sources_changed or
            self.artifact_families_changed or
            self.chunk_preview_policy_changed or
            self.chunk_embeddings_policy_changed or
            self.rerank_terms_policy_changed or
            self.graph_metric_policy_changed;
    }
};

pub const ArtifactActions = struct {
    document_segment: ArtifactAction = .rebuild,
    full_text: ArtifactAction = .rebuild,
    dense_vector: ArtifactAction = .rebuild,
    sparse_vector: ArtifactAction = .rebuild,
    graph: ArtifactAction = .rebuild,

    pub fn any(self: ArtifactActions) bool {
        return self.document_segment != .reuse or
            self.full_text != .reuse or
            self.dense_vector != .reuse or
            self.sparse_vector != .reuse or
            self.graph != .reuse;
    }
};

pub const DerivedOutputActions = struct {
    chunk_preview: DerivedOutputAction = .reuse,
    chunk_embeddings: DerivedOutputAction = .reuse,
    rerank_terms: DerivedOutputAction = .reuse,

    pub fn any(self: DerivedOutputActions) bool {
        return self.chunk_preview != .reuse or
            self.chunk_embeddings != .reuse or
            self.rerank_terms != .reuse;
    }
};

pub const TableDefinitionSnapshot = struct {
    schema_json: []u8 = &.{},
    read_schema_json: []u8 = &.{},
    indexes_json: []u8 = &.{},
    base_source: ?manifest_base_source.BaseSourceDescriptor = null,

    pub fn deinit(self: *TableDefinitionSnapshot, alloc: Allocator) void {
        if (self.schema_json.len > 0) alloc.free(self.schema_json);
        if (self.read_schema_json.len > 0) alloc.free(self.read_schema_json);
        if (self.indexes_json.len > 0) alloc.free(self.indexes_json);
        if (self.base_source) |*descriptor| manifest_base_source.freeOwnedDescriptor(alloc, descriptor);
        self.* = undefined;
    }
};

pub fn tableDefinitionSnapshotAlloc(
    alloc: Allocator,
    schema_json: []const u8,
    read_schema_json: []const u8,
    indexes_json: []const u8,
) !TableDefinitionSnapshot {
    var snapshot = TableDefinitionSnapshot{};
    errdefer snapshot.deinit(alloc);
    snapshot.schema_json = try alloc.dupe(u8, schema_json);
    snapshot.read_schema_json = try alloc.dupe(u8, read_schema_json);
    snapshot.indexes_json = try alloc.dupe(u8, indexes_json);
    snapshot.base_source = try pinnedExternalBaseSourceFromSchemaJsonAlloc(alloc, schema_json);
    return snapshot;
}

pub const TablePublicationPlan = struct {
    targets: builder_mod.Builder.PublicationTargets,
    policy: catalog_types.NamespacePolicy = .{},
    table_definition: TableDefinitionSnapshot = .{},
    external_source_plan: ?external_source_manifest.Plan = null,
    metadata_republish: MetadataRepublishReasons = .{},
    artifact_actions: ArtifactActions = .{},
    full_text_index_actions: []FullTextIndexAction = &.{},
    vector_index_actions: []NamedArtifactAction = &.{},
    sparse_index_actions: []NamedArtifactAction = &.{},
    graph_index_actions: []NamedArtifactAction = &.{},
    derived_output_actions: DerivedOutputActions = .{},

    pub fn deinit(self: *TablePublicationPlan, alloc: Allocator) void {
        search_sources.deinitPublishedSearchSources(alloc, &self.targets.published_search_sources);
        self.table_definition.deinit(alloc);
        if (self.external_source_plan) |*plan| plan.deinit(alloc);
        for (self.full_text_index_actions) |*entry| entry.deinit(alloc);
        if (self.full_text_index_actions.len > 0) alloc.free(self.full_text_index_actions);
        for (self.vector_index_actions) |*entry| entry.deinit(alloc);
        if (self.vector_index_actions.len > 0) alloc.free(self.vector_index_actions);
        for (self.sparse_index_actions) |*entry| entry.deinit(alloc);
        if (self.sparse_index_actions.len > 0) alloc.free(self.sparse_index_actions);
        for (self.graph_index_actions) |*entry| entry.deinit(alloc);
        if (self.graph_index_actions.len > 0) alloc.free(self.graph_index_actions);
        self.* = undefined;
    }

    pub fn forceRepublishFromHead(self: TablePublicationPlan) bool {
        return self.metadata_republish.any();
    }

    pub fn effectiveFullTextAction(self: TablePublicationPlan, text_artifact_present: bool) ArtifactAction {
        return collapseFullTextArtifactAction(self.full_text_index_actions, text_artifact_present, self.artifact_actions.full_text);
    }
};

pub fn collapseFullTextArtifactAction(
    items: []const FullTextIndexAction,
    text_artifact_present: bool,
    fallback: ArtifactAction,
) ArtifactAction {
    if (items.len == 0) {
        if (fallback == .rebuild and text_artifact_present) return .reuse;
        return fallback;
    }
    var has_rebuild = false;
    var has_reuse = false;
    for (items) |item| switch (item.action) {
        .rebuild => has_rebuild = true,
        .reuse => has_reuse = true,
        .drop => {},
    };
    if (has_rebuild) return .rebuild;
    if (has_reuse) return .reuse;
    return if (text_artifact_present) .drop else .drop;
}

pub fn collapseNamedArtifactAction(
    items: []const NamedArtifactAction,
    artifact_present: bool,
    fallback: ArtifactAction,
) ArtifactAction {
    if (items.len == 0) return fallback;
    var has_rebuild = false;
    var has_reuse = false;
    for (items) |item| switch (item.action) {
        .rebuild => has_rebuild = true,
        .reuse => has_reuse = true,
        .drop => {},
    };
    if (has_rebuild) return .rebuild;
    if (has_reuse) return .reuse;
    return if (artifact_present) .drop else .drop;
}

pub fn pinnedExternalBaseSourceFromSchemaJsonAlloc(
    alloc: Allocator,
    schema_json: []const u8,
) !?manifest_base_source.BaseSourceDescriptor {
    var owned_binding = (try externalBindingFromSchemaJsonAlloc(alloc, schema_json)) orelse return null;
    defer owned_binding.deinit(alloc);
    const binding = owned_binding.binding;
    try binding.validateReadOnlyMvp();
    const pinned_snapshot_id = binding.snapshot_mode.pinnedSnapshotId() orelse return null;
    const descriptor = try binding.toManifestBaseSource(pinned_snapshot_id, null);
    return try manifest_base_source.cloneDescriptorAlloc(alloc, descriptor);
}

pub fn externalBindingFromSchemaJsonAlloc(
    alloc: Allocator,
    schema_json: []const u8,
) !?OwnedExternalTableBinding {
    if (schema_json.len == 0) return null;

    var parsed = std.json.parseFromSlice(std.json.Value, alloc, schema_json, .{}) catch
        return error.InvalidExternalTableBinding;
    defer parsed.deinit();
    const root = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidExternalTableBinding,
    };
    const source_value = root.get("base_source") orelse return null;
    const source = switch (source_value) {
        .object => |object| object,
        else => return error.InvalidExternalTableBinding,
    };
    const kind = try requiredJsonString(source, "kind");
    if (!std.mem.eql(u8, kind, "external")) return null;

    const table_id = try alloc.dupe(u8, try requiredJsonString(source, "table_id"));
    errdefer alloc.free(table_id);
    const source_uri = try alloc.dupe(u8, try requiredJsonString(source, "uri"));
    errdefer alloc.free(source_uri);
    const format: @import("../external_source/types.zig").Format = blk: {
        const value = try requiredJsonString(source, "format");
        if (std.mem.eql(u8, value, "parquet")) break :blk .parquet;
        if (std.mem.eql(u8, value, "iceberg")) break :blk .iceberg;
        if (std.mem.eql(u8, value, "lance")) break :blk .lance;
        return error.InvalidExternalTableBinding;
    };

    const credentials = if (source.get("credentials")) |value| switch (value) {
        .object => |object| object,
        .null => null,
        else => return error.InvalidExternalTableBinding,
    } else null;
    const credential_ref_id = if (credentials) |value|
        try alloc.dupe(u8, try requiredJsonString(value, "ref"))
    else
        null;
    errdefer if (credential_ref_id) |value| alloc.free(value);
    const credential_scope = if (credentials) |value|
        if (value.get("scope")) |scope| switch (scope) {
            .string => |string| try alloc.dupe(u8, string),
            else => return error.InvalidExternalTableBinding,
        } else try alloc.dupe(u8, "")
    else
        null;
    errdefer if (credential_scope) |value| alloc.free(value);

    var snapshot_tag: enum { current, snapshot_id, object_version_digest } = .current;
    var snapshot_borrowed: ?[]const u8 = null;
    if (source.get("snapshot")) |snapshot| switch (snapshot) {
        .string => |value| {
            if (!std.mem.eql(u8, value, "current")) return error.InvalidExternalTableBinding;
        },
        .object => |object| {
            const mode = try requiredJsonString(object, "mode");
            if (std.mem.eql(u8, mode, "snapshot_id")) {
                snapshot_tag = .snapshot_id;
                snapshot_borrowed = try requiredJsonString(object, "id");
            } else if (std.mem.eql(u8, mode, "object_version_digest")) {
                snapshot_tag = .object_version_digest;
                snapshot_borrowed = try requiredJsonString(object, "digest");
            } else if (!std.mem.eql(u8, mode, "current")) {
                return error.InvalidExternalTableBinding;
            }
        },
        else => return error.InvalidExternalTableBinding,
    };
    const snapshot_value = if (snapshot_borrowed) |value| try alloc.dupe(u8, value) else null;
    errdefer if (snapshot_value) |value| alloc.free(value);
    const schema_fingerprint = try alloc.dupe(u8, try requiredJsonString(source, "schema_fingerprint"));
    errdefer alloc.free(schema_fingerprint);
    const write_policy: external_binding.WritePolicy = if (source.get("write_policy")) |value| blk: {
        const string = switch (value) {
            .string => |item| item,
            else => return error.InvalidExternalTableBinding,
        };
        if (std.mem.eql(u8, string, "read_only")) break :blk .read_only;
        if (std.mem.eql(u8, string, "materialized_overlay")) break :blk .materialized_overlay;
        if (std.mem.eql(u8, string, "iceberg_writer")) break :blk .iceberg_writer;
        if (std.mem.eql(u8, string, "lake_native_relational")) break :blk .lake_native_relational;
        return error.InvalidExternalTableBinding;
    } else .read_only;

    const binding = external_binding.Binding{
        .table_id = table_id,
        .format = format,
        .source_uri = source_uri,
        .credential_ref = if (credential_ref_id) |ref_id| .{
            .ref_id = ref_id,
            .scope = credential_scope orelse &.{},
        } else null,
        .snapshot_mode = switch (snapshot_tag) {
            .current => .current,
            .snapshot_id => .{ .snapshot_id = snapshot_value orelse return error.InvalidExternalTableBinding },
            .object_version_digest => .{ .object_version_digest = snapshot_value.? },
        },
        .schema_fingerprint = schema_fingerprint,
        .write_policy = write_policy,
    };
    try binding.validateReadOnlyMvp();

    return .{
        .binding = binding,
        .table_id = table_id,
        .source_uri = source_uri,
        .credential_ref_id = credential_ref_id,
        .credential_scope = credential_scope,
        .snapshot_value = snapshot_value,
        .schema_fingerprint = schema_fingerprint,
    };
}

fn requiredJsonString(object: anytype, name: []const u8) ![]const u8 {
    const value = object.get(name) orelse return error.InvalidExternalTableBinding;
    return switch (value) {
        .string => |string| string,
        else => error.InvalidExternalTableBinding,
    };
}

test "metadata republish reasons report when any flag is set" {
    try std.testing.expect(!(MetadataRepublishReasons{}).any());
    try std.testing.expect((MetadataRepublishReasons{ .published_search_sources_changed = true }).any());
}

test "artifact actions default to rebuild" {
    const actions = ArtifactActions{};
    try std.testing.expectEqual(ArtifactAction.rebuild, actions.document_segment);
    try std.testing.expectEqual(ArtifactAction.rebuild, actions.full_text);
    try std.testing.expectEqual(ArtifactAction.rebuild, actions.dense_vector);
    try std.testing.expectEqual(ArtifactAction.rebuild, actions.sparse_vector);
    try std.testing.expectEqual(ArtifactAction.rebuild, actions.graph);
}

test "derived output actions default to reuse" {
    const actions = DerivedOutputActions{};
    try std.testing.expectEqual(DerivedOutputAction.reuse, actions.chunk_preview);
    try std.testing.expectEqual(DerivedOutputAction.reuse, actions.chunk_embeddings);
    try std.testing.expectEqual(DerivedOutputAction.reuse, actions.rerank_terms);
}

test "table publication plan deinit frees full text actions" {
    var plan = TablePublicationPlan{
        .targets = .{ .published_search_sources = .{} },
        .full_text_index_actions = try std.testing.allocator.alloc(FullTextIndexAction, 1),
    };
    plan.full_text_index_actions[0] = .{
        .name = try std.testing.allocator.dupe(u8, "full_text_index_v1"),
        .action = .rebuild,
    };
    plan.deinit(std.testing.allocator);
}

test "publication plan republish follows explicit metadata reasons" {
    try std.testing.expect(!(TablePublicationPlan{
        .targets = .{ .published_search_sources = .{} },
        .artifact_actions = .{ .dense_vector = .rebuild, .document_segment = .reuse, .full_text = .reuse, .sparse_vector = .reuse, .graph = .reuse },
    }).forceRepublishFromHead());
    try std.testing.expect((TablePublicationPlan{
        .targets = .{ .published_search_sources = .{} },
        .metadata_republish = .{ .artifact_families_changed = true },
    }).forceRepublishFromHead());
}

test "table definition snapshot deinit handles empty fields" {
    var snapshot = TableDefinitionSnapshot{};
    snapshot.deinit(std.testing.allocator);
}

test "table definition snapshot owns pinned external base source" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":5,"storage_mode":"relational","default_type":"row","enforce_types":true,"base_source":{"kind":"external","table_id":"events","format":"parquet","uri":"s3://bucket/events","snapshot":{"mode":"object_version_digest","digest":"sha256:objects"},"schema_fingerprint":"schema-v5","write_policy":"read_only"},"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var snapshot = try tableDefinitionSnapshotAlloc(
        alloc,
        schema_json,
        "",
        "{}",
    );
    defer snapshot.deinit(alloc);

    try std.testing.expect(snapshot.base_source != null);
    try std.testing.expectEqual(manifest_base_source.BaseSourceKind.external_parquet, std.meta.activeTag(snapshot.base_source.?));
    try std.testing.expectEqualStrings("sha256:objects", snapshot.base_source.?.external_parquet.snapshot_id);
}

test "publication plan derives pinned external lake base source from schema" {
    const alloc = std.testing.allocator;
    var descriptor = (try pinnedExternalBaseSourceFromSchemaJsonAlloc(alloc,
        \\{"version":5,"storage_mode":"relational","default_type":"row","enforce_types":true,"base_source":{"kind":"external","table_id":"events","format":"iceberg","uri":"s3://bucket/warehouse/events","credentials":{"ref":"prod-lake-read","scope":"warehouse/events"},"snapshot":{"mode":"snapshot_id","id":"iceberg-123"},"schema_fingerprint":"schema-v5","write_policy":"read_only"},"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    )).?;
    defer manifest_base_source.freeOwnedDescriptor(alloc, &descriptor);

    try std.testing.expectEqual(manifest_base_source.BaseSourceKind.external_iceberg, std.meta.activeTag(descriptor));
    try std.testing.expectEqualStrings("s3://bucket/warehouse/events", descriptor.external_iceberg.source_uri);
    try std.testing.expectEqualStrings("iceberg-123", descriptor.external_iceberg.snapshot_id);
    try std.testing.expectEqualStrings("schema-v5", descriptor.external_iceberg.schema_fingerprint);
    try std.testing.expectEqual(@as(?[]const u8, null), descriptor.external_iceberg.file_inventory_artifact);
}

test "publication plan leaves current external lake base source unpinned" {
    const alloc = std.testing.allocator;
    const descriptor = try pinnedExternalBaseSourceFromSchemaJsonAlloc(alloc,
        \\{"version":5,"storage_mode":"relational","default_type":"row","enforce_types":true,"base_source":{"kind":"external","table_id":"events","format":"parquet","uri":"s3://bucket/events","snapshot":"current","schema_fingerprint":"schema-v5","write_policy":"read_only"},"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    );
    try std.testing.expectEqual(@as(?manifest_base_source.BaseSourceDescriptor, null), descriptor);
}

test "collapse full text artifact action uses per-index actions when present" {
    const alloc = std.testing.allocator;
    var actions = try alloc.alloc(FullTextIndexAction, 2);
    defer {
        for (actions) |*entry| entry.deinit(alloc);
        alloc.free(actions);
    }
    actions[0] = .{ .name = try alloc.dupe(u8, "full_text_index_v0"), .action = .reuse };
    actions[1] = .{ .name = try alloc.dupe(u8, "full_text_index_v1"), .action = .rebuild };
    try std.testing.expectEqual(ArtifactAction.rebuild, collapseFullTextArtifactAction(actions, true, .reuse));
}

test "collapse full text artifact action reuses implicit default text artifact" {
    try std.testing.expectEqual(ArtifactAction.reuse, collapseFullTextArtifactAction(&.{}, true, .rebuild));
    try std.testing.expectEqual(ArtifactAction.rebuild, collapseFullTextArtifactAction(&.{}, false, .rebuild));
}

test "collapse named artifact action uses per-index actions when present" {
    const alloc = std.testing.allocator;
    var actions = try alloc.alloc(NamedArtifactAction, 2);
    defer {
        for (actions) |*entry| entry.deinit(alloc);
        alloc.free(actions);
    }
    actions[0] = .{ .name = try alloc.dupe(u8, "semantic_a"), .action = .reuse };
    actions[1] = .{ .name = try alloc.dupe(u8, "semantic_b"), .action = .rebuild };
    try std.testing.expectEqual(ArtifactAction.rebuild, collapseNamedArtifactAction(actions, true, .reuse));
}
