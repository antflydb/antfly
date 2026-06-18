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

//! Manifest-level declarations for RowSource-derived sidecar artifacts.
//!
//! The text/vector/sparse/graph payload codecs should not need to know how a
//! lake snapshot is pinned. This layer binds those artifacts to the RowSource
//! snapshot, schema, row-ref kind, source columns, and index configuration they
//! were built from.

const std = @import("std");
const artifact_ref = @import("../manifest/artifact_ref.zig");
const rowsource = @import("../../storage/rowsource/types.zig");
const source_binding = @import("source_binding.zig");

pub const DeclaredArtifact = struct {
    name: []const u8,
    binding: source_binding.Binding,
    artifact: artifact_ref.ArtifactRef,

    pub fn validate(self: DeclaredArtifact) !void {
        if (self.name.len == 0) return error.InvalidSidecarArtifactDeclaration;
        try validateArtifactBinding(self.binding, self.artifact);
        if (self.artifact.name.len != 0 and !std.mem.eql(u8, self.name, self.artifact.name)) {
            return error.InvalidSidecarArtifactDeclaration;
        }
    }
};

pub const Manifest = struct {
    artifacts: []const DeclaredArtifact,

    pub fn validate(self: Manifest) !void {
        for (self.artifacts, 0..) |artifact, index| {
            try artifact.validate();
            for (self.artifacts[0..index]) |previous| {
                if (std.mem.eql(u8, previous.name, artifact.name)) {
                    return error.DuplicateSidecarArtifactDeclaration;
                }
                if (std.mem.eql(u8, previous.artifact.artifact_id, artifact.artifact.artifact_id)) {
                    return error.DuplicateSidecarArtifactDeclaration;
                }
            }
        }
    }

    pub fn find(self: Manifest, name: []const u8) ?DeclaredArtifact {
        for (self.artifacts) |artifact| {
            if (std.mem.eql(u8, artifact.name, name)) return artifact;
        }
        return null;
    }
};

pub fn artifactKindForSidecarKind(kind: source_binding.SidecarKind) artifact_ref.ArtifactKind {
    return switch (kind) {
        .text => .text_segment,
        .vector => .vector_segment,
        .sparse => .sparse_segment,
        .graph => .graph_segment,
        .algebraic => .algebraic_segment,
    };
}

pub fn sidecarKindForArtifactKind(kind: artifact_ref.ArtifactKind) ?source_binding.SidecarKind {
    return switch (kind) {
        .text_segment => .text,
        .vector_segment => .vector,
        .sparse_segment => .sparse,
        .graph_segment => .graph,
        .algebraic_segment => .algebraic,
        else => null,
    };
}

pub fn validateArtifactBinding(
    binding: source_binding.Binding,
    artifact: artifact_ref.ArtifactRef,
) !void {
    try binding.validate();
    if (artifact.artifact_id.len == 0) return error.InvalidSidecarArtifactDeclaration;
    if (artifact.kind != artifactKindForSidecarKind(binding.sidecar_kind)) {
        return error.SidecarArtifactKindMismatch;
    }
}

pub fn validateBatchAgainstDeclaredArtifact(
    declaration: DeclaredArtifact,
    batch: rowsource.ColumnBatch,
) !void {
    try declaration.validate();
    try source_binding.validateBatchAgainstBinding(declaration.binding, batch);
}

test "sidecar manifest declares text vector sparse and graph source bindings" {
    const text_binding = source_binding.Binding{
        .sidecar_kind = .text,
        .source_kind = .serverless_fragment,
        .row_ref_kind = .serverless,
        .source_id = "docs",
        .snapshot_id = "manifest-1",
        .schema_fingerprint = "schema-v1",
        .column_bindings = &[_][]const u8{"body"},
        .index_config_hash = "sha256:text",
    };
    const vector_binding = source_binding.Binding{
        .sidecar_kind = .vector,
        .source_kind = .external_lance,
        .row_ref_kind = .external,
        .source_id = "events",
        .snapshot_id = "lance-9",
        .schema_fingerprint = "schema-v2",
        .column_bindings = &[_][]const u8{"embedding"},
        .index_config_hash = "sha256:vector",
    };
    const sparse_binding = source_binding.Binding{
        .sidecar_kind = .sparse,
        .source_kind = .external_iceberg,
        .row_ref_kind = .external,
        .source_id = "events",
        .snapshot_id = "iceberg-9",
        .schema_fingerprint = "schema-v2",
        .column_bindings = &[_][]const u8{"keywords"},
        .index_config_hash = "sha256:sparse",
    };
    const graph_binding = source_binding.Binding{
        .sidecar_kind = .graph,
        .source_kind = .relational_store,
        .row_ref_kind = .relational_key,
        .source_id = "links",
        .snapshot_id = "rel-4",
        .schema_fingerprint = "schema-v3",
        .column_bindings = &[_][]const u8{ "source_id", "target_id", "edge_type" },
        .index_config_hash = "sha256:graph",
    };
    const declarations = [_]DeclaredArtifact{
        .{
            .name = "docs.body.text",
            .binding = text_binding,
            .artifact = .{ .kind = .text_segment, .name = "docs.body.text", .artifact_id = "text-1", .byte_len = 128, .checksum = "len:128" },
        },
        .{
            .name = "events.embedding.vector",
            .binding = vector_binding,
            .artifact = .{ .kind = .vector_segment, .name = "events.embedding.vector", .artifact_id = "vector-1", .byte_len = 256, .checksum = "len:256" },
        },
        .{
            .name = "events.keywords.sparse",
            .binding = sparse_binding,
            .artifact = .{ .kind = .sparse_segment, .name = "events.keywords.sparse", .artifact_id = "sparse-1", .byte_len = 96, .checksum = "len:96" },
        },
        .{
            .name = "links.graph",
            .binding = graph_binding,
            .artifact = .{ .kind = .graph_segment, .name = "links.graph", .artifact_id = "graph-1", .byte_len = 64, .checksum = "len:64" },
        },
    };

    const manifest = Manifest{ .artifacts = &declarations };
    try manifest.validate();
    try std.testing.expectEqual(artifact_ref.ArtifactKind.vector_segment, artifactKindForSidecarKind(.vector));
    try std.testing.expectEqual(source_binding.SidecarKind.graph, sidecarKindForArtifactKind(.graph_segment).?);
    try std.testing.expectEqualStrings("vector-1", manifest.find("events.embedding.vector").?.artifact.artifact_id);
}

test "sidecar manifest rejects mismatched artifact kinds and duplicate ids" {
    const binding = source_binding.Binding{
        .sidecar_kind = .text,
        .source_kind = .serverless_fragment,
        .row_ref_kind = .serverless,
        .snapshot_id = "manifest-1",
        .schema_fingerprint = "schema-v1",
        .column_bindings = &[_][]const u8{"body"},
        .index_config_hash = "sha256:text",
    };
    try std.testing.expectError(
        error.SidecarArtifactKindMismatch,
        validateArtifactBinding(binding, .{
            .kind = .vector_segment,
            .artifact_id = "vector-1",
            .byte_len = 128,
            .checksum = "len:128",
        }),
    );

    const duplicate = [_]DeclaredArtifact{
        .{
            .name = "docs.body.text",
            .binding = binding,
            .artifact = .{ .kind = .text_segment, .artifact_id = "text-1", .byte_len = 128, .checksum = "len:128" },
        },
        .{
            .name = "docs.title.text",
            .binding = binding,
            .artifact = .{ .kind = .text_segment, .artifact_id = "text-1", .byte_len = 128, .checksum = "len:128" },
        },
    };
    try std.testing.expectError(
        error.DuplicateSidecarArtifactDeclaration,
        (Manifest{ .artifacts = &duplicate }).validate(),
    );
}

test "sidecar manifest validates declared artifacts against RowSource batches" {
    const row_refs = [_]rowsource.RowRef{
        .{ .external = .{
            .source_id = "events",
            .snapshot_id = "iceberg-7",
            .file_id = "file-a.parquet",
            .row_group_ordinal = 1,
            .row_ordinal = 4,
        } },
    };
    const embeddings = [_][]const f32{&[_]f32{ 0.25, 0.75 }};
    const columns = [_]rowsource.ColumnVector{
        .{ .name = "embedding", .values = .{ .vector_f32 = &embeddings } },
    };
    const batch = rowsource.ColumnBatch{
        .snapshot = .{ .table_id = "events", .snapshot_id = "iceberg-7" },
        .row_refs = &row_refs,
        .columns = &columns,
    };
    const declaration = DeclaredArtifact{
        .name = "events.embedding.vector",
        .binding = source_binding.bindingFromSnapshot(
            .vector,
            .external_iceberg,
            batch.snapshot,
            "schema-v2",
            &[_][]const u8{"embedding"},
            "sha256:vector",
        ),
        .artifact = .{
            .kind = .vector_segment,
            .name = "events.embedding.vector",
            .artifact_id = "vector-iceberg-7",
            .byte_len = 512,
            .checksum = "len:512",
        },
    };

    try validateBatchAgainstDeclaredArtifact(declaration, batch);

    var stale_batch = batch;
    stale_batch.snapshot = .{ .table_id = "events", .snapshot_id = "iceberg-8" };
    try std.testing.expectError(
        error.SidecarSourceBindingMismatch,
        validateBatchAgainstDeclaredArtifact(declaration, stale_batch),
    );
}
