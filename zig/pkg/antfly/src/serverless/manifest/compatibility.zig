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

//! Compatibility checks for lake-native manifest descriptors. This keeps older
//! serverless generations fail-closed when a query runtime sees a base source,
//! artifact family, or required stats sidecar it cannot safely interpret.

const std = @import("std");
const artifact_ref = @import("artifact_ref.zig");
const base_source = @import("base_source.zig");

pub const Policy = struct {
    require_row_fragment_stats: bool = true,
    allow_external_parquet: bool = true,
    allow_external_iceberg: bool = true,
    allow_external_lance: bool = true,
};

pub const Report = struct {
    source_kind: base_source.BaseSourceKind,
    row_fragment_count: u32 = 0,
    row_fragment_stats_count: u32 = 0,
    algebraic_segment_count: u32 = 0,
    external_metadata_count: u32 = 0,
    sidecar_count: u32 = 0,
};

pub fn checkLakeBaseSource(
    descriptor: base_source.BaseSourceDescriptor,
    artifacts: []const artifact_ref.ArtifactRef,
    policy: Policy,
) !Report {
    try descriptor.validate();
    try validateArtifacts(artifacts);

    var report = countArtifacts(descriptor);
    for (artifacts) |artifact| {
        switch (artifact.kind) {
            .row_fragment => report.row_fragment_count += 1,
            .row_fragment_stats => report.row_fragment_stats_count += 1,
            .algebraic_segment => report.algebraic_segment_count += 1,
            .external_base_source => report.external_metadata_count += 1,
            .text_segment, .vector_segment, .sparse_segment, .graph_segment => report.sidecar_count += 1,
            .doc_values, .stored_fields, .mutation_segment, .document_segment => {},
        }
    }

    try checkSourceSupport(descriptor, policy);
    try checkSourceArtifacts(descriptor, artifacts, policy);
    return report;
}

fn countArtifacts(descriptor: base_source.BaseSourceDescriptor) Report {
    return .{ .source_kind = switch (descriptor) {
        .antfly_document_segments => .antfly_document_segments,
        .antfly_row_fragments => .antfly_row_fragments,
        .antfly_lsm_overlay => .antfly_lsm_overlay,
        .external_parquet => .external_parquet,
        .external_iceberg => .external_iceberg,
        .external_lance => .external_lance,
    } };
}

fn validateArtifacts(artifacts: []const artifact_ref.ArtifactRef) !void {
    for (artifacts, 0..) |artifact, idx| {
        if (artifact.artifact_id.len == 0) return error.IncompatibleLakeManifest;
        if (artifact.checksum.len == 0) return error.IncompatibleLakeManifest;
        for (artifacts[0..idx]) |previous| {
            if (std.mem.eql(u8, previous.artifact_id, artifact.artifact_id)) {
                return error.DuplicateLakeManifestArtifact;
            }
        }
    }
}

fn checkSourceSupport(descriptor: base_source.BaseSourceDescriptor, policy: Policy) !void {
    switch (descriptor) {
        .external_parquet => if (!policy.allow_external_parquet) return error.UnsupportedLakeManifestFeature,
        .external_iceberg => if (!policy.allow_external_iceberg) return error.UnsupportedLakeManifestFeature,
        .external_lance => if (!policy.allow_external_lance) return error.UnsupportedLakeManifestFeature,
        .antfly_document_segments, .antfly_row_fragments, .antfly_lsm_overlay => {},
    }
}

fn checkSourceArtifacts(
    descriptor: base_source.BaseSourceDescriptor,
    artifacts: []const artifact_ref.ArtifactRef,
    policy: Policy,
) !void {
    switch (descriptor) {
        .antfly_row_fragments => |source| {
            for (source.row_fragment_artifacts) |artifact_id| {
                if (!hasArtifact(artifacts, artifact_id, .row_fragment)) return error.IncompatibleLakeManifest;
            }
            if (policy.require_row_fragment_stats) {
                if (source.row_fragment_stats_artifacts.len < source.row_fragment_artifacts.len) {
                    return error.IncompatibleLakeManifest;
                }
            }
            for (source.row_fragment_stats_artifacts) |artifact_id| {
                if (!hasArtifact(artifacts, artifact_id, .row_fragment_stats)) return error.IncompatibleLakeManifest;
            }
        },
        .external_parquet, .external_iceberg, .external_lance => |source| {
            if (source.file_inventory_artifact) |artifact_id| {
                if (!hasArtifact(artifacts, artifact_id, .external_base_source)) return error.IncompatibleLakeManifest;
            }
            if (source.row_group_metadata_artifact) |artifact_id| {
                if (!hasArtifact(artifacts, artifact_id, .external_base_source)) return error.IncompatibleLakeManifest;
            }
            if (source.delete_metadata_artifact) |artifact_id| {
                if (!hasArtifact(artifacts, artifact_id, .external_base_source)) return error.IncompatibleLakeManifest;
            }
        },
        .antfly_document_segments, .antfly_lsm_overlay => {},
    }
}

fn hasArtifact(
    artifacts: []const artifact_ref.ArtifactRef,
    artifact_id: []const u8,
    kind: artifact_ref.ArtifactKind,
) bool {
    for (artifacts) |artifact| {
        if (artifact.kind == kind and std.mem.eql(u8, artifact.artifact_id, artifact_id)) return true;
    }
    return false;
}

test "lake manifest compatibility accepts row fragments with stats" {
    const row_fragments = [_][]const u8{"rows-1"};
    const stats = [_][]const u8{"rows-1.stats"};
    const descriptor = base_source.BaseSourceDescriptor{ .antfly_row_fragments = .{
        .snapshot_id = "manifest-7",
        .schema_fingerprint = "schema-v1",
        .row_fragment_artifacts = &row_fragments,
        .row_fragment_stats_artifacts = &stats,
    } };
    const artifacts = [_]artifact_ref.ArtifactRef{
        .{ .kind = .row_fragment, .artifact_id = "rows-1", .byte_len = 1024, .checksum = "len:1024" },
        .{ .kind = .row_fragment_stats, .artifact_id = "rows-1.stats", .byte_len = 128, .checksum = "len:128" },
        .{ .kind = .vector_segment, .artifact_id = "vec-1", .byte_len = 256, .checksum = "len:256" },
    };

    const report = try checkLakeBaseSource(descriptor, &artifacts, .{});
    try std.testing.expectEqual(base_source.BaseSourceKind.antfly_row_fragments, report.source_kind);
    try std.testing.expectEqual(@as(u32, 1), report.row_fragment_count);
    try std.testing.expectEqual(@as(u32, 1), report.row_fragment_stats_count);
    try std.testing.expectEqual(@as(u32, 1), report.sidecar_count);
}

test "lake manifest compatibility rejects missing row-fragment stats" {
    const row_fragments = [_][]const u8{"rows-1"};
    const descriptor = base_source.BaseSourceDescriptor{ .antfly_row_fragments = .{
        .snapshot_id = "manifest-7",
        .schema_fingerprint = "schema-v1",
        .row_fragment_artifacts = &row_fragments,
    } };
    const artifacts = [_]artifact_ref.ArtifactRef{
        .{ .kind = .row_fragment, .artifact_id = "rows-1", .byte_len = 1024, .checksum = "len:1024" },
    };

    try std.testing.expectError(error.IncompatibleLakeManifest, checkLakeBaseSource(descriptor, &artifacts, .{}));
    _ = try checkLakeBaseSource(descriptor, &artifacts, .{ .require_row_fragment_stats = false });
}

test "lake manifest compatibility rejects duplicate artifact ids and unsupported sources" {
    const descriptor = base_source.BaseSourceDescriptor{ .external_lance = .{
        .format = .lance,
        .source_uri = "s3://bucket/lance/events",
        .snapshot_id = "lance-1",
        .schema_fingerprint = "schema-v1",
        .file_inventory_artifact = "files-1",
    } };
    const artifacts = [_]artifact_ref.ArtifactRef{
        .{ .kind = .external_base_source, .artifact_id = "files-1", .byte_len = 128, .checksum = "len:128" },
        .{ .kind = .text_segment, .artifact_id = "files-1", .byte_len = 64, .checksum = "len:64" },
    };

    try std.testing.expectError(error.DuplicateLakeManifestArtifact, checkLakeBaseSource(descriptor, &artifacts, .{}));

    const deduped = [_]artifact_ref.ArtifactRef{
        .{ .kind = .external_base_source, .artifact_id = "files-1", .byte_len = 128, .checksum = "len:128" },
    };
    try std.testing.expectError(error.UnsupportedLakeManifestFeature, checkLakeBaseSource(
        descriptor,
        &deduped,
        .{ .allow_external_lance = false },
    ));
}
