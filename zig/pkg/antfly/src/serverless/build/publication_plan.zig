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

    pub fn any(self: MetadataRepublishReasons) bool {
        return self.read_schema_migration or
            self.published_search_sources_changed or
            self.artifact_families_changed or
            self.chunk_preview_policy_changed or
            self.chunk_embeddings_policy_changed or
            self.rerank_terms_policy_changed;
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

    pub fn deinit(self: *TableDefinitionSnapshot, alloc: Allocator) void {
        if (self.schema_json.len > 0) alloc.free(self.schema_json);
        if (self.read_schema_json.len > 0) alloc.free(self.read_schema_json);
        if (self.indexes_json.len > 0) alloc.free(self.indexes_json);
        self.* = undefined;
    }
};

pub const TablePublicationPlan = struct {
    targets: builder_mod.Builder.PublicationTargets,
    policy: catalog_types.NamespacePolicy = .{},
    table_definition: TableDefinitionSnapshot = .{},
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
