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
const lake_cache = @import("lake_cache.zig");
const lake_rows = @import("lake_rows.zig");
const lake_parquet_rowgroup = @import("lake_parquet_rowgroup.zig");
const lake_range_io = @import("lake_range_io.zig");
const lake_sidecar_selection = @import("lake_sidecar_selection.zig");
const rowsource = @import("../../storage/rowsource/types.zig");
const source_binding = @import("../segment/source_binding.zig");

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
    candidate_sets: []const lake_rows.SidecarCandidateSet = &.{},
    operation: Operation,
    projected_column_count: u16 = 0,
    cache_budget: lake_cache.Budget = .{},
    range_cache_stats: ?lake_parquet_rowgroup.ObjectRangeCacheStats = null,
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

pub const SidecarCandidateAccounting = struct {
    supplied_set_count: u32 = 0,
    supplied_ref_count: u64 = 0,
    usable_set_count: u32 = 0,
    usable_ref_count: u64 = 0,
    intersected_ref_count: u64 = 0,
    empty_usable_set_count: u32 = 0,
    selected_without_candidates_count: u32 = 0,
    stale_ignored_candidate_set_count: u32 = 0,
    not_requested_candidate_set_count: u32 = 0,
    missing_declaration_candidate_set_count: u32 = 0,
    hydration_possible: bool = false,
    candidate_intersection_empty: bool = false,
};

pub const RangeCacheLaneAccounting = struct {
    hits: usize = 0,
    misses: usize = 0,
    stored_bytes: usize = 0,
    evicted_bytes: usize = 0,
    rejected_bytes: usize = 0,

    pub fn hadActivity(self: RangeCacheLaneAccounting) bool {
        return self.hits != 0 or
            self.misses != 0 or
            self.stored_bytes != 0 or
            self.evicted_bytes != 0 or
            self.rejected_bytes != 0;
    }
};

pub const RangeCacheAccounting = struct {
    total: RangeCacheLaneAccounting = .{},
    metadata: RangeCacheLaneAccounting = .{},
    compressed_range: RangeCacheLaneAccounting = .{},
    decoded_column: RangeCacheLaneAccounting = .{},
    projected_batch: RangeCacheLaneAccounting = .{},
    serving_sidecar: RangeCacheLaneAccounting = .{},
    broad_scan_scratch: RangeCacheLaneAccounting = .{},

    pub fn anyRejected(self: RangeCacheAccounting) bool {
        return self.total.rejected_bytes != 0 or
            self.metadata.rejected_bytes != 0 or
            self.compressed_range.rejected_bytes != 0 or
            self.decoded_column.rejected_bytes != 0 or
            self.projected_batch.rejected_bytes != 0 or
            self.serving_sidecar.rejected_bytes != 0 or
            self.broad_scan_scratch.rejected_bytes != 0;
    }
};

pub const Plan = struct {
    operation: Operation,
    source_kind: base_source.BaseSourceKind,
    snapshot_id: []const u8 = &.{},
    schema_fingerprint: []const u8 = &.{},
    projected_column_count: u16 = 0,
    cache_class: CacheClass,
    accounting: ArtifactAccounting,
    cache_accounting: lake_cache.Accounting = .{},
    range_cache_accounting: RangeCacheAccounting = .{},
    sidecar_selection: SidecarSelectionAccounting = .{},
    sidecar_candidates: SidecarCandidateAccounting = .{},
    recommendation: lake_promotion.Recommendation = .{ .kind = .none },
};

pub fn explain(request: Request) !Plan {
    try request.base_source.validate();
    var accounting = ArtifactAccounting{};
    for (request.artifacts) |artifact| {
        try accountArtifact(&accounting, artifact);
    }
    const cache_accounting = try lake_cache.accountArtifacts(request.artifacts, request.cache_budget);
    try validateBaseSourceArtifacts(request.base_source, request.artifacts);
    try validateSidecarDeclarations(request.artifacts, request.sidecars);
    const sidecar_selection = try summarizeSidecarSelection(
        request.base_source,
        request.sidecars,
        request.desired_sidecars,
        request.sidecar_policy,
    );
    const sidecar_candidates = try summarizeSidecarCandidates(
        request.base_source,
        request.sidecars,
        request.desired_sidecars,
        request.sidecar_policy,
        request.candidate_sets,
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
        .cache_accounting = cache_accounting,
        .range_cache_accounting = summarizeRangeCache(request.range_cache_stats),
        .sidecar_selection = sidecar_selection,
        .sidecar_candidates = sidecar_candidates,
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

fn summarizeSidecarCandidates(
    descriptor: base_source.BaseSourceDescriptor,
    sidecars: []const sidecar_manifest.DeclaredArtifact,
    desired: []const lake_sidecar_selection.DesiredSidecar,
    policy: lake_sidecar_selection.Policy,
    candidate_sets: []const lake_rows.SidecarCandidateSet,
) !SidecarCandidateAccounting {
    var accounting = SidecarCandidateAccounting{};
    for (candidate_sets) |candidate_set| {
        if (candidate_set.sidecar_name.len == 0) return error.InvalidLakeSidecarSelection;
        accounting.supplied_set_count += 1;
        accounting.supplied_ref_count += @intCast(candidate_set.row_refs.len);

        const declaration = findSidecarDeclaration(sidecars, candidate_set.sidecar_name) orelse {
            accounting.missing_declaration_candidate_set_count += 1;
            continue;
        };
        const requested = try lake_sidecar_selection.declarationMatchesDesired(declaration, desired);
        if (!requested) {
            accounting.not_requested_candidate_set_count += 1;
            continue;
        }

        const fresh = try lake_sidecar_selection.declarationMatchesBaseSource(descriptor, declaration.binding);
        if (!fresh) {
            switch (policy.stale) {
                .reject => return error.StaleLakeSidecar,
                .ignore => {
                    accounting.stale_ignored_candidate_set_count += 1;
                    continue;
                },
            }
        }

        try source_binding.validateCandidateRowRefsAgainstBinding(declaration.binding, candidate_set.row_refs);
        accounting.usable_set_count += 1;
        accounting.usable_ref_count += @intCast(candidate_set.row_refs.len);
        if (candidate_set.row_refs.len == 0) accounting.empty_usable_set_count += 1;
    }

    for (sidecars) |declaration| {
        if (!(try lake_sidecar_selection.declarationMatchesDesired(declaration, desired))) continue;
        const fresh = try lake_sidecar_selection.declarationMatchesBaseSource(descriptor, declaration.binding);
        if (!fresh) continue;
        if (findCandidateSet(candidate_sets, declaration.name) == null) {
            accounting.selected_without_candidates_count += 1;
        }
    }

    accounting.hydration_possible = accounting.usable_set_count != 0;
    accounting.intersected_ref_count = try countIntersectedCandidateRefs(
        descriptor,
        sidecars,
        desired,
        policy,
        candidate_sets,
    );
    accounting.candidate_intersection_empty =
        accounting.usable_set_count != 0 and accounting.intersected_ref_count == 0;
    return accounting;
}

fn countIntersectedCandidateRefs(
    descriptor: base_source.BaseSourceDescriptor,
    sidecars: []const sidecar_manifest.DeclaredArtifact,
    desired: []const lake_sidecar_selection.DesiredSidecar,
    policy: lake_sidecar_selection.Policy,
    candidate_sets: []const lake_rows.SidecarCandidateSet,
) !u64 {
    const first = try firstUsableCandidateSet(descriptor, sidecars, desired, policy, candidate_sets) orelse return 0;
    var total: u64 = 0;
    for (first.row_refs, 0..) |row_ref, row_idx| {
        if (containsRowRef(first.row_refs[0..row_idx], row_ref)) continue;
        if (!try rowRefInEveryUsableCandidateSet(descriptor, sidecars, desired, policy, candidate_sets, row_ref)) continue;
        total += 1;
    }
    return total;
}

fn firstUsableCandidateSet(
    descriptor: base_source.BaseSourceDescriptor,
    sidecars: []const sidecar_manifest.DeclaredArtifact,
    desired: []const lake_sidecar_selection.DesiredSidecar,
    policy: lake_sidecar_selection.Policy,
    candidate_sets: []const lake_rows.SidecarCandidateSet,
) !?lake_rows.SidecarCandidateSet {
    for (candidate_sets) |candidate_set| {
        if (try candidateSetUsable(descriptor, sidecars, desired, policy, candidate_set)) {
            return candidate_set;
        }
    }
    return null;
}

fn rowRefInEveryUsableCandidateSet(
    descriptor: base_source.BaseSourceDescriptor,
    sidecars: []const sidecar_manifest.DeclaredArtifact,
    desired: []const lake_sidecar_selection.DesiredSidecar,
    policy: lake_sidecar_selection.Policy,
    candidate_sets: []const lake_rows.SidecarCandidateSet,
    row_ref: rowsource.RowRef,
) !bool {
    for (candidate_sets) |candidate_set| {
        if (!(try candidateSetUsable(descriptor, sidecars, desired, policy, candidate_set))) continue;
        if (!containsRowRef(candidate_set.row_refs, row_ref)) return false;
    }
    return true;
}

fn candidateSetUsable(
    descriptor: base_source.BaseSourceDescriptor,
    sidecars: []const sidecar_manifest.DeclaredArtifact,
    desired: []const lake_sidecar_selection.DesiredSidecar,
    policy: lake_sidecar_selection.Policy,
    candidate_set: lake_rows.SidecarCandidateSet,
) !bool {
    const declaration = findSidecarDeclaration(sidecars, candidate_set.sidecar_name) orelse return false;
    if (!(try lake_sidecar_selection.declarationMatchesDesired(declaration, desired))) return false;
    const fresh = try lake_sidecar_selection.declarationMatchesBaseSource(descriptor, declaration.binding);
    if (!fresh) {
        return switch (policy.stale) {
            .reject => error.StaleLakeSidecar,
            .ignore => false,
        };
    }
    try source_binding.validateCandidateRowRefsAgainstBinding(declaration.binding, candidate_set.row_refs);
    return true;
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

fn findCandidateSet(
    candidate_sets: []const lake_rows.SidecarCandidateSet,
    sidecar_name: []const u8,
) ?lake_rows.SidecarCandidateSet {
    for (candidate_sets) |candidate_set| {
        if (std.mem.eql(u8, candidate_set.sidecar_name, sidecar_name)) return candidate_set;
    }
    return null;
}

fn findSidecarDeclaration(
    sidecars: []const sidecar_manifest.DeclaredArtifact,
    sidecar_name: []const u8,
) ?sidecar_manifest.DeclaredArtifact {
    for (sidecars) |declaration| {
        if (std.mem.eql(u8, declaration.name, sidecar_name)) return declaration;
    }
    return null;
}

fn containsRowRef(haystack: []const rowsource.RowRef, needle: rowsource.RowRef) bool {
    for (haystack) |candidate| {
        if (rowRefsEqual(candidate, needle)) return true;
    }
    return false;
}

fn rowRefsEqual(a: rowsource.RowRef, b: rowsource.RowRef) bool {
    if (std.meta.activeTag(a) != std.meta.activeTag(b)) return false;
    return switch (a) {
        .relational_key => |key| std.mem.eql(u8, key, b.relational_key),
        .serverless => |value| blk: {
            const other = b.serverless;
            break :blk std.mem.eql(u8, value.fragment_id, other.fragment_id) and
                value.row_ordinal == other.row_ordinal;
        },
        .external => |value| blk: {
            const other = b.external;
            break :blk std.mem.eql(u8, value.source_id, other.source_id) and
                std.mem.eql(u8, value.snapshot_id, other.snapshot_id) and
                std.mem.eql(u8, value.file_id, other.file_id) and
                value.row_group_ordinal == other.row_group_ordinal and
                value.row_ordinal == other.row_ordinal;
        },
    };
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

fn summarizeRangeCache(stats: ?lake_parquet_rowgroup.ObjectRangeCacheStats) RangeCacheAccounting {
    const snapshot = stats orelse return .{};
    return .{
        .total = rangeLaneAccounting(.{
            .hits = snapshot.hits,
            .misses = snapshot.misses,
            .stored_bytes = snapshot.stored_bytes,
            .evicted_bytes = snapshot.evicted_bytes,
            .rejected_bytes = snapshot.rejected_bytes,
        }),
        .metadata = rangeLaneAccounting(snapshot.lane(.metadata)),
        .compressed_range = rangeLaneAccounting(snapshot.lane(.compressed_range)),
        .decoded_column = rangeLaneAccounting(snapshot.lane(.decoded_column)),
        .projected_batch = rangeLaneAccounting(snapshot.lane(.projected_batch)),
        .serving_sidecar = rangeLaneAccounting(snapshot.lane(.serving_sidecar)),
        .broad_scan_scratch = rangeLaneAccounting(snapshot.lane(.broad_scan_scratch)),
    };
}

fn rangeLaneAccounting(stats: lake_parquet_rowgroup.ObjectRangeCacheLaneStats) RangeCacheLaneAccounting {
    return .{
        .hits = stats.hits,
        .misses = stats.misses,
        .stored_bytes = stats.stored_bytes,
        .evicted_bytes = stats.evicted_bytes,
        .rejected_bytes = stats.rejected_bytes,
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
    try std.testing.expectEqual(@as(u64, 384), plan.cache_accounting.pinned_bytes);
    try std.testing.expectEqual(@as(u64, 1024), plan.cache_accounting.payload_bytes);
    try std.testing.expectEqual(@as(u64, 1408), plan.cache_accounting.total_bytes);
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
        .cache_budget = .{
            .max_pinned_bytes = 2048,
            .max_payload_bytes = 256,
            .max_total_bytes = 4096,
        },
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
    try std.testing.expectEqual(@as(u64, 4096), plan.cache_accounting.external_metadata_bytes);
    try std.testing.expectEqual(@as(u64, 512), plan.cache_accounting.search_sidecar_bytes);
    try std.testing.expect(plan.cache_accounting.over_pinned_budget);
    try std.testing.expect(plan.cache_accounting.over_payload_budget);
    try std.testing.expect(plan.cache_accounting.over_total_budget);
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

test "lake explain reports sidecar candidate hydration accounting" {
    const descriptor = base_source.BaseSourceDescriptor{ .external_iceberg = .{
        .format = .iceberg,
        .source_uri = "s3://bucket/warehouse/events",
        .snapshot_id = "iceberg-9",
        .schema_fingerprint = "schema-v2",
        .file_inventory_artifact = "files-1",
    } };
    const artifacts = [_]artifact_ref.ArtifactRef{
        .{ .kind = .external_base_source, .artifact_id = "files-1", .byte_len = 4096, .checksum = "len:4096" },
        .{ .kind = .text_segment, .name = "events.body.text", .artifact_id = "text-1", .byte_len = 256, .checksum = "len:256" },
        .{ .kind = .vector_segment, .name = "events.embedding.vector", .artifact_id = "vector-1", .byte_len = 512, .checksum = "len:512" },
    };
    const sidecars = [_]sidecar_manifest.DeclaredArtifact{
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
        .{
            .name = "events.embedding.vector",
            .binding = .{
                .sidecar_kind = .vector,
                .source_kind = .external_iceberg,
                .row_ref_kind = .external,
                .source_id = "events",
                .snapshot_id = "iceberg-9",
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
    };
    const text_candidates = [_]rowsource.RowRef{
        .{ .external = .{ .source_id = "events", .snapshot_id = "iceberg-9", .file_id = "part-a", .row_group_ordinal = 0, .row_ordinal = 7 } },
        .{ .external = .{ .source_id = "events", .snapshot_id = "iceberg-9", .file_id = "part-a", .row_group_ordinal = 0, .row_ordinal = 9 } },
    };
    const candidates = [_]lake_rows.SidecarCandidateSet{
        .{ .sidecar_name = "events.body.text", .row_refs = &text_candidates },
    };

    const plan = try explain(.{
        .base_source = descriptor,
        .artifacts = &artifacts,
        .sidecars = &sidecars,
        .candidate_sets = &candidates,
        .operation = .hydrate,
    });

    try std.testing.expectEqual(@as(u32, 2), plan.sidecar_selection.selected_count);
    try std.testing.expectEqual(@as(u32, 1), plan.sidecar_candidates.supplied_set_count);
    try std.testing.expectEqual(@as(u64, 2), plan.sidecar_candidates.supplied_ref_count);
    try std.testing.expectEqual(@as(u32, 1), plan.sidecar_candidates.usable_set_count);
    try std.testing.expectEqual(@as(u64, 2), plan.sidecar_candidates.usable_ref_count);
    try std.testing.expectEqual(@as(u64, 2), plan.sidecar_candidates.intersected_ref_count);
    try std.testing.expectEqual(@as(u32, 1), plan.sidecar_candidates.selected_without_candidates_count);
    try std.testing.expect(plan.sidecar_candidates.hydration_possible);
    try std.testing.expect(!plan.sidecar_candidates.candidate_intersection_empty);
}

test "lake explain reports intersected sidecar candidate hydration refs" {
    const descriptor = base_source.BaseSourceDescriptor{ .external_iceberg = .{
        .format = .iceberg,
        .source_uri = "s3://bucket/warehouse/events",
        .snapshot_id = "iceberg-9",
        .schema_fingerprint = "schema-v2",
        .file_inventory_artifact = "files-1",
    } };
    const artifacts = [_]artifact_ref.ArtifactRef{
        .{ .kind = .external_base_source, .artifact_id = "files-1", .byte_len = 4096, .checksum = "len:4096" },
        .{ .kind = .text_segment, .name = "events.body.text", .artifact_id = "text-1", .byte_len = 256, .checksum = "len:256" },
        .{ .kind = .vector_segment, .name = "events.embedding.vector", .artifact_id = "vector-1", .byte_len = 512, .checksum = "len:512" },
    };
    const sidecars = [_]sidecar_manifest.DeclaredArtifact{
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
        .{
            .name = "events.embedding.vector",
            .binding = .{
                .sidecar_kind = .vector,
                .source_kind = .external_iceberg,
                .row_ref_kind = .external,
                .source_id = "events",
                .snapshot_id = "iceberg-9",
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
    };
    const row_7 = rowsource.RowRef{ .external = .{ .source_id = "events", .snapshot_id = "iceberg-9", .file_id = "part-a", .row_group_ordinal = 0, .row_ordinal = 7 } };
    const row_9 = rowsource.RowRef{ .external = .{ .source_id = "events", .snapshot_id = "iceberg-9", .file_id = "part-a", .row_group_ordinal = 0, .row_ordinal = 9 } };
    const row_11 = rowsource.RowRef{ .external = .{ .source_id = "events", .snapshot_id = "iceberg-9", .file_id = "part-a", .row_group_ordinal = 0, .row_ordinal = 11 } };
    const text_candidates = [_]rowsource.RowRef{ row_7, row_9, row_9 };
    const vector_candidates = [_]rowsource.RowRef{ row_9, row_11 };
    const candidates = [_]lake_rows.SidecarCandidateSet{
        .{ .sidecar_name = "events.body.text", .row_refs = &text_candidates },
        .{ .sidecar_name = "events.embedding.vector", .row_refs = &vector_candidates },
    };

    const plan = try explain(.{
        .base_source = descriptor,
        .artifacts = &artifacts,
        .sidecars = &sidecars,
        .candidate_sets = &candidates,
        .operation = .hydrate,
    });

    try std.testing.expectEqual(@as(u32, 2), plan.sidecar_candidates.usable_set_count);
    try std.testing.expectEqual(@as(u64, 5), plan.sidecar_candidates.usable_ref_count);
    try std.testing.expectEqual(@as(u64, 1), plan.sidecar_candidates.intersected_ref_count);
    try std.testing.expectEqual(@as(u32, 0), plan.sidecar_candidates.selected_without_candidates_count);
    try std.testing.expect(plan.sidecar_candidates.hydration_possible);
    try std.testing.expect(!plan.sidecar_candidates.candidate_intersection_empty);
}

test "lake explain reports object range cache lane accounting" {
    const descriptor = base_source.BaseSourceDescriptor{ .external_parquet = .{
        .format = .parquet_prefix,
        .source_uri = "s3://bucket/events",
        .snapshot_id = "parquet-3",
        .schema_fingerprint = "schema-v3",
        .file_inventory_artifact = "files-3",
    } };
    const artifacts = [_]artifact_ref.ArtifactRef{
        .{ .kind = .external_base_source, .artifact_id = "files-3", .byte_len = 2048, .checksum = "len:2048" },
    };
    var stats = lake_parquet_rowgroup.ObjectRangeCacheStats{
        .hits = 4,
        .misses = 7,
        .stored_bytes = 2048,
        .evicted_bytes = 128,
        .rejected_bytes = 64,
    };
    stats.lanes[@intFromEnum(lake_range_io.CacheLane.metadata)] = .{
        .hits = 2,
        .misses = 1,
        .stored_bytes = 512,
    };
    stats.lanes[@intFromEnum(lake_range_io.CacheLane.compressed_range)] = .{
        .misses = 5,
        .stored_bytes = 1024,
        .evicted_bytes = 128,
    };
    stats.lanes[@intFromEnum(lake_range_io.CacheLane.serving_sidecar)] = .{
        .hits = 2,
        .stored_bytes = 512,
    };
    stats.lanes[@intFromEnum(lake_range_io.CacheLane.broad_scan_scratch)] = .{
        .misses = 1,
        .rejected_bytes = 64,
    };

    const plan = try explain(.{
        .base_source = descriptor,
        .artifacts = &artifacts,
        .operation = .scan,
        .range_cache_stats = stats,
    });

    try std.testing.expectEqual(@as(usize, 4), plan.range_cache_accounting.total.hits);
    try std.testing.expectEqual(@as(usize, 7), plan.range_cache_accounting.total.misses);
    try std.testing.expectEqual(@as(usize, 2048), plan.range_cache_accounting.total.stored_bytes);
    try std.testing.expectEqual(@as(usize, 512), plan.range_cache_accounting.metadata.stored_bytes);
    try std.testing.expectEqual(@as(usize, 128), plan.range_cache_accounting.compressed_range.evicted_bytes);
    try std.testing.expect(plan.range_cache_accounting.serving_sidecar.hadActivity());
    try std.testing.expect(plan.range_cache_accounting.anyRejected());
    try std.testing.expectEqual(@as(usize, 64), plan.range_cache_accounting.broad_scan_scratch.rejected_bytes);
}
