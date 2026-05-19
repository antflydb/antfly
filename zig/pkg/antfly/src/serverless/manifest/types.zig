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
const search_sources = @import("../search_sources.zig");

pub const ArtifactKind = enum(u8) {
    text_segment = 1,
    vector_segment = 2,
    doc_values = 3,
    stored_fields = 4,
    mutation_segment = 5,
    document_segment = 6,
    sparse_segment = 7,
    graph_segment = 8,
};

pub const ArtifactRef = struct {
    kind: ArtifactKind,
    name: []const u8 = &.{},
    artifact_id: []const u8,
    byte_len: u64,
    checksum: []const u8,
};

pub const PublishedGenerationStats = struct {
    document_count: u64 = 0,
    document_base_version: u64 = 0,
    document_publish_mode: catalog_types.DocumentPublishMode = .append_mutation_tail,
    text_segment_count: u32 = 0,
    vector_segment_count: u32 = 0,
    sparse_segment_count: u32 = 0,
    graph_segment_count: u32 = 0,
    published_search_sources: search_sources.PublishedSearchSources = .{},
    derived_outputs: search_sources.MaterializedDerivedOutputs = .{},
    policy: catalog_types.NamespacePolicy = .{},
    schema_json: []u8 = &.{},
    read_schema_json: []u8 = &.{},
    indexes_json: []u8 = &.{},
};

pub const PublishedGeneration = struct {
    namespace: []const u8,
    version: u64,
    built_at_ns: u64,
    wal_start_lsn: u64,
    wal_end_lsn: u64,
    stats: PublishedGenerationStats,
    artifacts: []ArtifactRef,

    pub fn deinit(self: *Manifest, alloc: Allocator) void {
        alloc.free(self.namespace);
        search_sources.deinitPublishedSearchSources(alloc, &self.stats.published_search_sources);
        search_sources.deinitMaterializedDerivedOutputs(alloc, &self.stats.derived_outputs);
        if (self.stats.schema_json.len > 0) alloc.free(self.stats.schema_json);
        if (self.stats.read_schema_json.len > 0) alloc.free(self.stats.read_schema_json);
        if (self.stats.indexes_json.len > 0) alloc.free(self.stats.indexes_json);
        for (self.artifacts) |artifact| {
            if (artifact.name.len > 0) alloc.free(artifact.name);
            alloc.free(artifact.artifact_id);
            alloc.free(artifact.checksum);
        }
        alloc.free(self.artifacts);
        self.* = undefined;
    }
};

pub const ManifestStats = PublishedGenerationStats;
pub const Manifest = PublishedGeneration;

pub fn freeManifest(alloc: Allocator, manifest: *PublishedGeneration) void {
    manifest.deinit(alloc);
}

pub fn cloneManifest(alloc: Allocator, src: PublishedGeneration) !PublishedGeneration {
    const namespace = try alloc.dupe(u8, src.namespace);
    errdefer alloc.free(namespace);

    const artifacts = try alloc.alloc(ArtifactRef, src.artifacts.len);
    errdefer alloc.free(artifacts);

    var initialized: usize = 0;
    errdefer {
        for (artifacts[0..initialized]) |artifact| {
            alloc.free(artifact.artifact_id);
            alloc.free(artifact.checksum);
        }
    }

    for (src.artifacts, 0..) |artifact, idx| {
        const name = if (artifact.name.len == 0) &.{} else try alloc.dupe(u8, artifact.name);
        errdefer if (name.len > 0) alloc.free(name);
        const artifact_id = try alloc.dupe(u8, artifact.artifact_id);
        errdefer alloc.free(artifact_id);
        artifacts[idx] = .{
            .kind = artifact.kind,
            .name = name,
            .artifact_id = artifact_id,
            .byte_len = artifact.byte_len,
            .checksum = try alloc.dupe(u8, artifact.checksum),
        };
        initialized += 1;
    }

    return .{
        .namespace = namespace,
        .version = src.version,
        .built_at_ns = src.built_at_ns,
        .wal_start_lsn = src.wal_start_lsn,
        .wal_end_lsn = src.wal_end_lsn,
        .stats = .{
            .document_count = src.stats.document_count,
            .document_base_version = src.stats.document_base_version,
            .document_publish_mode = src.stats.document_publish_mode,
            .text_segment_count = src.stats.text_segment_count,
            .vector_segment_count = src.stats.vector_segment_count,
            .sparse_segment_count = src.stats.sparse_segment_count,
            .graph_segment_count = src.stats.graph_segment_count,
            .published_search_sources = try search_sources.clonePublishedSearchSourcesAlloc(
                alloc,
                src.stats.published_search_sources,
            ),
            .derived_outputs = try search_sources.cloneMaterializedDerivedOutputsAlloc(
                alloc,
                src.stats.derived_outputs,
            ),
            .policy = src.stats.policy,
            .schema_json = if (src.stats.schema_json.len == 0) &.{} else try alloc.dupe(u8, src.stats.schema_json),
            .read_schema_json = if (src.stats.read_schema_json.len == 0) &.{} else try alloc.dupe(u8, src.stats.read_schema_json),
            .indexes_json = if (src.stats.indexes_json.len == 0) &.{} else try alloc.dupe(u8, src.stats.indexes_json),
        },
        .artifacts = artifacts,
    };
}

test "cloneManifest duplicates owned storage" {
    const alloc = std.testing.allocator;
    var original = Manifest{
        .namespace = try alloc.dupe(u8, "docs"),
        .version = 7,
        .built_at_ns = 100,
        .wal_start_lsn = 10,
        .wal_end_lsn = 20,
        .stats = .{
            .document_count = 9,
            .document_base_version = 7,
            .document_publish_mode = .inline_rebase,
            .text_segment_count = 1,
            .vector_segment_count = 1,
        },
        .artifacts = try alloc.alloc(ArtifactRef, 1),
    };
    defer original.deinit(alloc);

    original.artifacts[0] = .{
        .kind = .text_segment,
        .name = try alloc.dupe(u8, "full_text_index_v0"),
        .artifact_id = try alloc.dupe(u8, "artifact-a"),
        .byte_len = 123,
        .checksum = try alloc.dupe(u8, "sha256:abc"),
    };

    var cloned = try cloneManifest(alloc, original);
    defer cloned.deinit(alloc);

    try std.testing.expectEqualStrings("docs", cloned.namespace);
    try std.testing.expectEqual(@as(u64, 7), cloned.version);
    try std.testing.expectEqual(@as(u64, 7), cloned.stats.document_base_version);
    try std.testing.expectEqual(catalog_types.DocumentPublishMode.inline_rebase, cloned.stats.document_publish_mode);
    try std.testing.expectEqual(@as(usize, 1), cloned.artifacts.len);
    try std.testing.expect(cloned.namespace.ptr != original.namespace.ptr);
    try std.testing.expect(cloned.artifacts[0].name.ptr != original.artifacts[0].name.ptr);
    try std.testing.expect(cloned.artifacts[0].artifact_id.ptr != original.artifacts[0].artifact_id.ptr);
}
