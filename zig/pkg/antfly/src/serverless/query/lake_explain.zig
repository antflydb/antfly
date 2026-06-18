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

//! Explain-plan scaffold for lake-native RowSource execution. The intent is to
//! make query planning auditable before we have a full lake optimizer: every
//! query pins one base source, declares the artifact families it can consult,
//! accounts for manifest-referenced bytes, and can surface adaptive promotion
//! advice from the serving observation stream.

const std = @import("std");
const artifact_ref = @import("../manifest/artifact_ref.zig");
const base_source = @import("../manifest/base_source.zig");
const lake_promotion = @import("../build/lake_promotion.zig");
const sidecar_manifest = @import("../segment/sidecar_manifest.zig");
const lake_sidecar_selection = @import("lake_sidecar_selection.zig");

pub const Operation = enum {
    scan,
    hydrate,
    group_by,
};

pub const CacheClass = enum {
    none,
    row_fragment_data,
    row_fragment_stats,
    algebraic_segment,
    external_metadata,
};

pub const Request = struct {
    base_source: base_source.BaseSourceDescriptor,
    artifacts: []const artifact_ref.ArtifactRef,
    sidecars: []const sidecar_manifest.DeclaredArtifact = &.{},
    desired_sidecars: []const lake_sidecar_selection.DesiredSidecar = &.{},
    sidecar_policy: lake_sidecar_selection.Policy = .{},
    operation: Operation,
    projected_column_count: u16 = 0,
    observation: ?lake_promotion.Observation = null,
    thresholds: lake_promotion.Thresholds = .{},
};

pub const ArtifactAccounting = struct {
    row_fragment_count: u32 = 0,
    row_fragment_stats_count: u32 = 0,
    algebraic_segment_count: u32 = 0,
    search_sidecar_count: u32 = 0,
    external_metadata_count: u32 = 0,
    manifest_accounted_bytes: u64 = 0,
};

pub const SidecarSelectionAccounting = struct {
    declared_count: u32 = 0,
    selected_count: u32 = 0,
    stale_ignored_count: u32 = 0,
    not_requested_count: u32 = 0,
};

pub const Plan = struct {
    operation: Operation,
    source_kind: base_source.BaseSourceKind,
    snapshot_id: []const u8 = &.{},
    schema_fingerprint: []const u8 = &.{},
    projected_column_count: u16 = 0,
    cache_class: CacheClass,
    accounting: ArtifactAccounting,
    sidecar_selection: SidecarSelectionAccounting = .{},
    recommendation: lake_promotion.Recommendation = .{ .kind = .none },
};

pub fn explain(request: Request) !Plan {
    try request.base_source.validate();
    var accounting = ArtifactAccounting{};
    for (request.artifacts) |artifact| {
        try accountArtifact(&accounting, artifact);
    }
    try validateBaseSourceArtifacts(request.base_source, request.artifacts);
    try validateSidecarDeclarations(request.artifacts, request.sidecars);
    const sidecar_selection = try summarizeSidecarSelection(
        request.base_source,
        request.sidecars,
        request.desired_sidecars,
        request.sidecar_policy,
    );

    const source_info = sourceInfo(request.base_source);
    return .{
        .operation = request.operation,
        .source_kind = source_info.kind,
        .snapshot_id = source_info.snapshot_id,
        .schema_fingerprint = source_info.schema_fingerprint,
        .projected_column_count = request.projected_column_count,
        .cache_class = chooseCacheClass(request.operation, request.base_source, accounting),
        .accounting = accounting,
        .sidecar_selection = sidecar_selection,
        .recommendation = if (request.observation) |observation|
            lake_promotion.recommend(observation, request.thresholds)
        else
            .{ .kind = .none },
    };
}

fn validateSidecarDeclarations(
    artifacts: []const artifact_ref.ArtifactRef,
    sidecars: []const sidecar_manifest.DeclaredArtifact,
) !void {
    if (sidecars.len == 0) return;
    const manifest = sidecar_manifest.Manifest{ .artifacts = sidecars };
    try manifest.validate();
    for (sidecars) |declaration| {
        if (!hasArtifact(artifacts, declaration.artifact.artifact_id, declaration.artifact.kind)) {
            return error.LakeExplainMissingArtifact;
        }
    }
}

fn summarizeSidecarSelection(
    descriptor: base_source.BaseSourceDescriptor,
    sidecars: []const sidecar_manifest.DeclaredArtifact,
    desired: []const lake_sidecar_selection.DesiredSidecar,
    policy: lake_sidecar_selection.Policy,
) !SidecarSelectionAccounting {
    if (sidecars.len == 0) return .{};
    const summary = try lake_sidecar_selection.summarize(descriptor, sidecars, desired, policy);
    return .{
        .declared_count = @intCast(sidecars.len),
        .selected_count = summary.selected_count,
        .stale_ignored_count = summary.stale_ignored_count,
        .not_requested_count = summary.not_requested_count,
    };
}

fn accountArtifact(accounting: *ArtifactAccounting, artifact: artifact_ref.ArtifactRef) !void {
    if (artifact.artifact_id.len == 0) return error.InvalidLakeExplainPlan;
    if (artifact.checksum.len == 0) return error.InvalidLakeExplainPlan;
    accounting.manifest_accounted_bytes += artifact.byte_len;
    switch (artifact.kind) {
        .row_fragment => accounting.row_fragment_count += 1,
        .row_fragment_stats => accounting.row_fragment_stats_count += 1,
        .algebraic_segment => accounting.algebraic_segment_count += 1,
        .external_base_source => accounting.external_metadata_count += 1,
        .text_segment, .vector_segment, .sparse_segment, .graph_segment => accounting.search_sidecar_count += 1,
        .doc_values, .stored_fields, .mutation_segment, .document_segment => {},
    }
}

fn validateBaseSourceArtifacts(
    descriptor: base_source.BaseSourceDescriptor,
    artifacts: []const artifact_ref.ArtifactRef,
) !void {
    switch (descriptor) {
        .antfly_row_fragments => |source| {
            if (source.row_fragment_artifacts.len == 0) return error.InvalidLakeExplainPlan;
            for (source.row_fragment_artifacts) |artifact_id| {
                if (!hasArtifact(artifacts, artifact_id, .row_fragment)) return error.LakeExplainMissingArtifact;
            }
            for (source.row_fragment_stats_artifacts) |artifact_id| {
                if (!hasArtifact(artifacts, artifact_id, .row_fragment_stats)) return error.LakeExplainMissingArtifact;
            }
        },
        .external_parquet, .external_iceberg, .external_lance => |source| {
            if (source.file_inventory_artifact) |artifact_id| {
                if (!hasArtifact(artifacts, artifact_id, .external_base_source)) return error.LakeExplainMissingArtifact;
            }
            if (source.row_group_metadata_artifact) |artifact_id| {
                if (!hasArtifact(artifacts, artifact_id, .external_base_source)) return error.LakeExplainMissingArtifact;
            }
            if (source.delete_metadata_artifact) |artifact_id| {
                if (!hasArtifact(artifacts, artifact_id, .external_base_source)) return error.LakeExplainMissingArtifact;
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

const SourceInfo = struct {
    kind: base_source.BaseSourceKind,
    snapshot_id: []const u8 = &.{},
    schema_fingerprint: []const u8 = &.{},
};

fn sourceInfo(descriptor: base_source.BaseSourceDescriptor) SourceInfo {
    return switch (descriptor) {
        .antfly_document_segments => .{ .kind = .antfly_document_segments },
        .antfly_lsm_overlay => .{ .kind = .antfly_lsm_overlay },
        .antfly_row_fragments => |source| .{
            .kind = .antfly_row_fragments,
            .snapshot_id = source.snapshot_id,
            .schema_fingerprint = source.schema_fingerprint,
        },
        .external_parquet => |source| .{
            .kind = .external_parquet,
            .snapshot_id = source.snapshot_id,
            .schema_fingerprint = source.schema_fingerprint,
        },
        .external_iceberg => |source| .{
            .kind = .external_iceberg,
            .snapshot_id = source.snapshot_id,
            .schema_fingerprint = source.schema_fingerprint,
        },
        .external_lance => |source| .{
            .kind = .external_lance,
            .snapshot_id = source.snapshot_id,
            .schema_fingerprint = source.schema_fingerprint,
        },
    };
}

fn chooseCacheClass(
    operation: Operation,
    descriptor: base_source.BaseSourceDescriptor,
    accounting: ArtifactAccounting,
) CacheClass {
    return switch (operation) {
        .group_by => if (accounting.algebraic_segment_count > 0) .algebraic_segment else .row_fragment_data,
        .hydrate => switch (descriptor) {
            .external_parquet, .external_iceberg, .external_lance => .external_metadata,
            else => .row_fragment_data,
        },
        .scan => if (accounting.row_fragment_stats_count > 0)
            .row_fragment_stats
        else switch (descriptor) {
            .external_parquet, .external_iceberg, .external_lance => .external_metadata,
            else => .row_fragment_data,
        },
    };
}

test "lake explain accounts for Antfly row fragments and stats" {
    const row_fragment_ids = [_][]const u8{"rows-1"};
    const row_fragment_stats_ids = [_][]const u8{"rows-1.stats"};
    const descriptor = base_source.BaseSourceDescriptor{ .antfly_row_fragments = .{
        .snapshot_id = "manifest-7",
        .schema_fingerprint = "schema-v1",
        .row_fragment_artifacts = &row_fragment_ids,
        .row_fragment_stats_artifacts = &row_fragment_stats_ids,
    } };
    const artifacts = [_]artifact_ref.ArtifactRef{
        .{ .kind = .row_fragment, .artifact_id = "rows-1", .byte_len = 1024, .checksum = "len:1024" },
        .{ .kind = .row_fragment_stats, .artifact_id = "rows-1.stats", .byte_len = 128, .checksum = "len:128" },
        .{ .kind = .algebraic_segment, .artifact_id = "agg-1", .byte_len = 256, .checksum = "len:256" },
    };

    const plan = try explain(.{
        .base_source = descriptor,
        .artifacts = &artifacts,
        .operation = .scan,
        .projected_column_count = 2,
    });

    try std.testing.expectEqual(base_source.BaseSourceKind.antfly_row_fragments, plan.source_kind);
    try std.testing.expectEqual(CacheClass.row_fragment_stats, plan.cache_class);
    try std.testing.expectEqual(@as(u32, 1), plan.accounting.row_fragment_count);
    try std.testing.expectEqual(@as(u32, 1), plan.accounting.row_fragment_stats_count);
    try std.testing.expectEqual(@as(u32, 1), plan.accounting.algebraic_segment_count);
    try std.testing.expectEqual(@as(u64, 1408), plan.accounting.manifest_accounted_bytes);
    try std.testing.expectEqual(@as(u16, 2), plan.projected_column_count);
}

test "lake explain validates external metadata artifacts and promotion advice" {
    const descriptor = base_source.BaseSourceDescriptor{ .external_iceberg = .{
        .format = .iceberg,
        .source_uri = "s3://bucket/warehouse/events",
        .snapshot_id = "iceberg-9",
        .schema_fingerprint = "schema-v2",
        .file_inventory_artifact = "files-1",
    } };
    const artifacts = [_]artifact_ref.ArtifactRef{
        .{ .kind = .external_base_source, .artifact_id = "files-1", .byte_len = 4096, .checksum = "len:4096" },
        .{ .kind = .text_segment, .artifact_id = "text-1", .byte_len = 512, .checksum = "len:512" },
    };

    const plan = try explain(.{
        .base_source = descriptor,
        .artifacts = &artifacts,
        .operation = .hydrate,
        .projected_column_count = 3,
        .observation = .{
            .source_kind = .external_iceberg,
            .scanned_rows = 20_000,
            .returned_rows = 500,
            .projected_column_count = 3,
            .repeated_scan_count = 3,
        },
    });

    try std.testing.expectEqual(base_source.BaseSourceKind.external_iceberg, plan.source_kind);
    try std.testing.expectEqual(CacheClass.external_metadata, plan.cache_class);
    try std.testing.expectEqual(@as(u32, 1), plan.accounting.external_metadata_count);
    try std.testing.expectEqual(@as(u32, 1), plan.accounting.search_sidecar_count);
    try std.testing.expectEqual(lake_promotion.RecommendationKind.promote_external_to_row_fragment, plan.recommendation.kind);
}

test "lake explain rejects missing manifest artifacts" {
    const row_fragment_ids = [_][]const u8{"rows-1"};
    const descriptor = base_source.BaseSourceDescriptor{ .antfly_row_fragments = .{
        .snapshot_id = "manifest-7",
        .schema_fingerprint = "schema-v1",
        .row_fragment_artifacts = &row_fragment_ids,
    } };

    try std.testing.expectError(error.LakeExplainMissingArtifact, explain(.{
        .base_source = descriptor,
        .artifacts = &.{},
        .operation = .scan,
    }));
}

test "lake explain rejects stale declared sidecars" {
    const descriptor = base_source.BaseSourceDescriptor{ .external_iceberg = .{
        .format = .iceberg,
        .source_uri = "s3://bucket/warehouse/events",
        .snapshot_id = "iceberg-9",
        .schema_fingerprint = "schema-v2",
        .file_inventory_artifact = "files-1",
    } };
    const artifacts = [_]artifact_ref.ArtifactRef{
        .{ .kind = .external_base_source, .artifact_id = "files-1", .byte_len = 4096, .checksum = "len:4096" },
        .{ .kind = .vector_segment, .name = "events.embedding.vector", .artifact_id = "vector-1", .byte_len = 512, .checksum = "len:512" },
    };
    const stale_sidecar = sidecar_manifest.DeclaredArtifact{
        .name = "events.embedding.vector",
        .binding = .{
            .sidecar_kind = .vector,
            .source_kind = .external_iceberg,
            .row_ref_kind = .external,
            .source_id = "events",
            .snapshot_id = "iceberg-8",
            .schema_fingerprint = "schema-v2",
            .column_bindings = &[_][]const u8{"embedding"},
            .index_config_hash = "sha256:vector",
        },
        .artifact = .{
            .kind = .vector_segment,
            .name = "events.embedding.vector",
            .artifact_id = "vector-1",
            .byte_len = 512,
            .checksum = "len:512",
        },
    };

    try std.testing.expectError(error.StaleLakeSidecar, explain(.{
        .base_source = descriptor,
        .artifacts = &artifacts,
        .sidecars = &[_]sidecar_manifest.DeclaredArtifact{stale_sidecar},
        .operation = .scan,
    }));
}

test "lake explain reports sidecar selection fallback" {
    const descriptor = base_source.BaseSourceDescriptor{ .external_iceberg = .{
        .format = .iceberg,
        .source_uri = "s3://bucket/warehouse/events",
        .snapshot_id = "iceberg-9",
        .schema_fingerprint = "schema-v2",
        .file_inventory_artifact = "files-1",
    } };
    const artifacts = [_]artifact_ref.ArtifactRef{
        .{ .kind = .external_base_source, .artifact_id = "files-1", .byte_len = 4096, .checksum = "len:4096" },
        .{ .kind = .vector_segment, .name = "events.embedding.vector", .artifact_id = "vector-1", .byte_len = 512, .checksum = "len:512" },
        .{ .kind = .text_segment, .name = "events.body.text", .artifact_id = "text-1", .byte_len = 256, .checksum = "len:256" },
    };
    const sidecars = [_]sidecar_manifest.DeclaredArtifact{
        .{
            .name = "events.embedding.vector",
            .binding = .{
                .sidecar_kind = .vector,
                .source_kind = .external_iceberg,
                .row_ref_kind = .external,
                .source_id = "events",
                .snapshot_id = "iceberg-8",
                .schema_fingerprint = "schema-v2",
                .column_bindings = &[_][]const u8{"embedding"},
                .index_config_hash = "sha256:vector",
            },
            .artifact = .{
                .kind = .vector_segment,
                .name = "events.embedding.vector",
                .artifact_id = "vector-1",
                .byte_len = 512,
                .checksum = "len:512",
            },
        },
        .{
            .name = "events.body.text",
            .binding = .{
                .sidecar_kind = .text,
                .source_kind = .external_iceberg,
                .row_ref_kind = .external,
                .source_id = "events",
                .snapshot_id = "iceberg-9",
                .schema_fingerprint = "schema-v2",
                .column_bindings = &[_][]const u8{"body"},
                .index_config_hash = "sha256:text",
            },
            .artifact = .{
                .kind = .text_segment,
                .name = "events.body.text",
                .artifact_id = "text-1",
                .byte_len = 256,
                .checksum = "len:256",
            },
        },
    };

    const plan = try explain(.{
        .base_source = descriptor,
        .artifacts = &artifacts,
        .sidecars = &sidecars,
        .desired_sidecars = &[_]lake_sidecar_selection.DesiredSidecar{.{ .kind = .vector }},
        .sidecar_policy = .{ .stale = .ignore },
        .operation = .scan,
    });

    try std.testing.expectEqual(@as(u32, 2), plan.sidecar_selection.declared_count);
    try std.testing.expectEqual(@as(u32, 0), plan.sidecar_selection.selected_count);
    try std.testing.expectEqual(@as(u32, 1), plan.sidecar_selection.stale_ignored_count);
    try std.testing.expectEqual(@as(u32, 1), plan.sidecar_selection.not_requested_count);
}
