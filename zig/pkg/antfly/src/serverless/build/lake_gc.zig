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

//! Dry-run lake artifact garbage-collection planning. Store-level retention can
//! delete artifacts, but lake mode needs an auditable snapshot-aware plan that
//! explains why row fragments, stats, algebraic folds, and external metadata are
//! retained or collectible before destructive cleanup runs.

const std = @import("std");
const Allocator = std.mem.Allocator;
const artifact_ref = @import("../manifest/artifact_ref.zig");
const base_source = @import("../manifest/base_source.zig");

pub const Snapshot = struct {
    snapshot_id: []const u8,
    base_source: base_source.BaseSourceDescriptor,
    artifacts: []const artifact_ref.ArtifactRef,
};

pub const Candidate = struct {
    artifact_id: []const u8,
    kind: artifact_ref.ArtifactKind,
    byte_len: u64 = 0,
};

pub const Plan = struct {
    retained_artifact_ids: [][]u8,
    collectible_artifact_ids: [][]u8,
    retained_bytes: u64,
    collectible_bytes: u64,

    pub fn deinit(self: *Plan, alloc: Allocator) void {
        for (self.retained_artifact_ids) |artifact_id| alloc.free(artifact_id);
        alloc.free(self.retained_artifact_ids);
        for (self.collectible_artifact_ids) |artifact_id| alloc.free(artifact_id);
        alloc.free(self.collectible_artifact_ids);
        self.* = undefined;
    }

    pub fn isRetained(self: Plan, artifact_id: []const u8) bool {
        for (self.retained_artifact_ids) |retained| {
            if (std.mem.eql(u8, retained, artifact_id)) return true;
        }
        return false;
    }

    pub fn isCollectible(self: Plan, artifact_id: []const u8) bool {
        for (self.collectible_artifact_ids) |collectible| {
            if (std.mem.eql(u8, collectible, artifact_id)) return true;
        }
        return false;
    }
};

pub fn planAlloc(
    alloc: Allocator,
    retained_snapshots: []const Snapshot,
    candidates: []const Candidate,
) !Plan {
    var retained = std.StringHashMapUnmanaged(Candidate).empty;
    defer freeOwnedKeys(alloc, &retained);

    for (retained_snapshots) |snapshot| {
        if (snapshot.snapshot_id.len == 0) return error.InvalidLakeGcPlan;
        try snapshot.base_source.validate();
        try collectBaseSourceArtifactIds(alloc, &retained, snapshot.base_source, snapshot.artifacts);
        for (snapshot.artifacts) |artifact| {
            try collectArtifact(alloc, &retained, .{
                .artifact_id = artifact.artifact_id,
                .kind = artifact.kind,
                .byte_len = artifact.byte_len,
            });
        }
    }

    var retained_ids = std.ArrayListUnmanaged([]u8).empty;
    errdefer freeStringList(alloc, &retained_ids);
    var collectible_ids = std.ArrayListUnmanaged([]u8).empty;
    errdefer freeStringList(alloc, &collectible_ids);

    var retained_bytes: u64 = 0;
    var collectible_bytes: u64 = 0;

    for (candidates) |candidate| {
        if (candidate.artifact_id.len == 0) return error.InvalidLakeGcPlan;
        if (retained.get(candidate.artifact_id)) |retained_candidate| {
            try appendUnique(alloc, &retained_ids, candidate.artifact_id);
            retained_bytes += if (candidate.byte_len != 0) candidate.byte_len else retained_candidate.byte_len;
        } else if (isLakeArtifact(candidate.kind)) {
            try appendUnique(alloc, &collectible_ids, candidate.artifact_id);
            collectible_bytes += candidate.byte_len;
        }
    }

    sortStrings(retained_ids.items);
    sortStrings(collectible_ids.items);

    return .{
        .retained_artifact_ids = try retained_ids.toOwnedSlice(alloc),
        .collectible_artifact_ids = try collectible_ids.toOwnedSlice(alloc),
        .retained_bytes = retained_bytes,
        .collectible_bytes = collectible_bytes,
    };
}

fn collectBaseSourceArtifactIds(
    alloc: Allocator,
    retained: *std.StringHashMapUnmanaged(Candidate),
    descriptor: base_source.BaseSourceDescriptor,
    artifacts: []const artifact_ref.ArtifactRef,
) !void {
    switch (descriptor) {
        .antfly_row_fragments => |source| {
            for (source.row_fragment_artifacts) |artifact_id| {
                try collectReferencedArtifact(alloc, retained, artifacts, artifact_id, .row_fragment);
            }
            for (source.row_fragment_stats_artifacts) |artifact_id| {
                try collectReferencedArtifact(alloc, retained, artifacts, artifact_id, .row_fragment_stats);
            }
        },
        .external_parquet, .external_iceberg, .external_lance => |source| {
            if (source.file_inventory_artifact) |artifact_id| {
                try collectReferencedArtifact(alloc, retained, artifacts, artifact_id, .external_base_source);
            }
            if (source.row_group_metadata_artifact) |artifact_id| {
                try collectReferencedArtifact(alloc, retained, artifacts, artifact_id, .external_base_source);
            }
            if (source.delete_metadata_artifact) |artifact_id| {
                try collectReferencedArtifact(alloc, retained, artifacts, artifact_id, .external_base_source);
            }
        },
        .antfly_document_segments, .antfly_lsm_overlay => {},
    }
}

fn collectReferencedArtifact(
    alloc: Allocator,
    retained: *std.StringHashMapUnmanaged(Candidate),
    artifacts: []const artifact_ref.ArtifactRef,
    artifact_id: []const u8,
    kind: artifact_ref.ArtifactKind,
) !void {
    const artifact = findArtifact(artifacts, artifact_id, kind) orelse return error.LakeGcMissingReferencedArtifact;
    try collectArtifact(alloc, retained, .{
        .artifact_id = artifact.artifact_id,
        .kind = artifact.kind,
        .byte_len = artifact.byte_len,
    });
}

fn collectArtifact(
    alloc: Allocator,
    retained: *std.StringHashMapUnmanaged(Candidate),
    candidate: Candidate,
) !void {
    if (candidate.artifact_id.len == 0) return error.InvalidLakeGcPlan;
    if (retained.contains(candidate.artifact_id)) return;
    const owned_id = try alloc.dupe(u8, candidate.artifact_id);
    errdefer alloc.free(owned_id);
    try retained.put(alloc, owned_id, candidate);
}

fn findArtifact(
    artifacts: []const artifact_ref.ArtifactRef,
    artifact_id: []const u8,
    kind: artifact_ref.ArtifactKind,
) ?artifact_ref.ArtifactRef {
    for (artifacts) |artifact| {
        if (artifact.kind == kind and std.mem.eql(u8, artifact.artifact_id, artifact_id)) return artifact;
    }
    return null;
}

fn isLakeArtifact(kind: artifact_ref.ArtifactKind) bool {
    return switch (kind) {
        .row_fragment, .row_fragment_stats, .algebraic_segment, .external_base_source => true,
        .text_segment, .vector_segment, .sparse_segment, .graph_segment => true,
        .doc_values, .stored_fields, .mutation_segment, .document_segment => false,
    };
}

fn appendUnique(
    alloc: Allocator,
    list: *std.ArrayListUnmanaged([]u8),
    artifact_id: []const u8,
) !void {
    for (list.items) |existing| {
        if (std.mem.eql(u8, existing, artifact_id)) return;
    }
    try list.append(alloc, try alloc.dupe(u8, artifact_id));
}

fn sortStrings(items: [][]u8) void {
    std.mem.sort([]u8, items, {}, struct {
        fn lessThan(_: void, lhs: []u8, rhs: []u8) bool {
            return std.mem.lessThan(u8, lhs, rhs);
        }
    }.lessThan);
}

fn freeStringList(alloc: Allocator, list: *std.ArrayListUnmanaged([]u8)) void {
    for (list.items) |item| alloc.free(item);
    list.deinit(alloc);
}

fn freeOwnedKeys(alloc: Allocator, map: *std.StringHashMapUnmanaged(Candidate)) void {
    var it = map.iterator();
    while (it.next()) |entry| alloc.free(entry.key_ptr.*);
    map.deinit(alloc);
}

test "lake gc retains current row fragments stats and folds" {
    const alloc = std.testing.allocator;
    const row_fragments = [_][]const u8{"rows-new"};
    const row_fragment_stats = [_][]const u8{"rows-new.stats"};
    const artifacts = [_]artifact_ref.ArtifactRef{
        .{ .kind = .row_fragment, .artifact_id = "rows-new", .byte_len = 100, .checksum = "len:100" },
        .{ .kind = .row_fragment_stats, .artifact_id = "rows-new.stats", .byte_len = 10, .checksum = "len:10" },
        .{ .kind = .algebraic_segment, .artifact_id = "agg-new", .byte_len = 20, .checksum = "len:20" },
    };
    const snapshots = [_]Snapshot{.{
        .snapshot_id = "manifest-2",
        .base_source = .{ .antfly_row_fragments = .{
            .snapshot_id = "manifest-2",
            .schema_fingerprint = "schema-v1",
            .row_fragment_artifacts = &row_fragments,
            .row_fragment_stats_artifacts = &row_fragment_stats,
        } },
        .artifacts = &artifacts,
    }};
    const candidates = [_]Candidate{
        .{ .kind = .row_fragment, .artifact_id = "rows-old", .byte_len = 90 },
        .{ .kind = .row_fragment_stats, .artifact_id = "rows-old.stats", .byte_len = 9 },
        .{ .kind = .row_fragment, .artifact_id = "rows-new", .byte_len = 100 },
        .{ .kind = .row_fragment_stats, .artifact_id = "rows-new.stats", .byte_len = 10 },
        .{ .kind = .algebraic_segment, .artifact_id = "agg-new", .byte_len = 20 },
    };

    var plan = try planAlloc(alloc, &snapshots, &candidates);
    defer plan.deinit(alloc);

    try std.testing.expect(plan.isRetained("rows-new"));
    try std.testing.expect(plan.isRetained("rows-new.stats"));
    try std.testing.expect(plan.isRetained("agg-new"));
    try std.testing.expect(plan.isCollectible("rows-old"));
    try std.testing.expect(plan.isCollectible("rows-old.stats"));
    try std.testing.expectEqual(@as(u64, 130), plan.retained_bytes);
    try std.testing.expectEqual(@as(u64, 99), plan.collectible_bytes);
}

test "lake gc retains external metadata referenced by live snapshots" {
    const alloc = std.testing.allocator;
    const artifacts = [_]artifact_ref.ArtifactRef{
        .{ .kind = .external_base_source, .artifact_id = "files-new", .byte_len = 1000, .checksum = "len:1000" },
        .{ .kind = .external_base_source, .artifact_id = "deletes-new", .byte_len = 200, .checksum = "len:200" },
        .{ .kind = .text_segment, .artifact_id = "text-new", .byte_len = 50, .checksum = "len:50" },
    };
    const snapshots = [_]Snapshot{.{
        .snapshot_id = "iceberg-2",
        .base_source = .{ .external_iceberg = .{
            .format = .iceberg,
            .source_uri = "s3://bucket/warehouse/events",
            .snapshot_id = "iceberg-2",
            .schema_fingerprint = "schema-v2",
            .file_inventory_artifact = "files-new",
            .delete_metadata_artifact = "deletes-new",
        } },
        .artifacts = &artifacts,
    }};
    const candidates = [_]Candidate{
        .{ .kind = .external_base_source, .artifact_id = "files-old", .byte_len = 900 },
        .{ .kind = .external_base_source, .artifact_id = "files-new", .byte_len = 1000 },
        .{ .kind = .external_base_source, .artifact_id = "deletes-new", .byte_len = 200 },
        .{ .kind = .text_segment, .artifact_id = "text-new", .byte_len = 50 },
    };

    var plan = try planAlloc(alloc, &snapshots, &candidates);
    defer plan.deinit(alloc);

    try std.testing.expect(plan.isCollectible("files-old"));
    try std.testing.expect(plan.isRetained("files-new"));
    try std.testing.expect(plan.isRetained("deletes-new"));
    try std.testing.expect(plan.isRetained("text-new"));
    try std.testing.expectEqual(@as(u64, 1250), plan.retained_bytes);
    try std.testing.expectEqual(@as(u64, 900), plan.collectible_bytes);
}

test "lake gc fails closed when live snapshot references missing metadata" {
    const alloc = std.testing.allocator;
    const snapshots = [_]Snapshot{.{
        .snapshot_id = "manifest-1",
        .base_source = .{ .external_parquet = .{
            .format = .parquet_prefix,
            .source_uri = "s3://bucket/events",
            .snapshot_id = "digest-1",
            .schema_fingerprint = "schema-v1",
            .file_inventory_artifact = "missing-files",
        } },
        .artifacts = &.{},
    }};

    try std.testing.expectError(error.LakeGcMissingReferencedArtifact, planAlloc(alloc, &snapshots, &.{}));
}
