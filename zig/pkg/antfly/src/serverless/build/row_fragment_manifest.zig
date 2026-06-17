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

//! Manifest planning helpers for Antfly-owned row-fragment publication.

const std = @import("std");
const Allocator = std.mem.Allocator;
const manifest_artifact = @import("../manifest/artifact_ref.zig");
const manifest_base_source = @import("../manifest/base_source.zig");

pub const PublishedArtifact = struct {
    artifact_id: []const u8,
    byte_len: u64,
    checksum: []const u8,
    name: []const u8 = &.{},
};

pub const Plan = struct {
    base_source: manifest_base_source.BaseSourceDescriptor,
    artifacts: []manifest_artifact.ArtifactRef,

    pub fn deinit(self: *Plan, alloc: Allocator) void {
        for (self.artifacts) |artifact| {
            if (artifact.name.len != 0) alloc.free(artifact.name);
            alloc.free(artifact.artifact_id);
            alloc.free(artifact.checksum);
        }
        alloc.free(self.artifacts);
        alloc.free(self.base_source.antfly_row_fragments.snapshot_id);
        alloc.free(self.base_source.antfly_row_fragments.schema_fingerprint);
        alloc.free(self.base_source.antfly_row_fragments.row_fragment_artifacts);
        alloc.free(self.base_source.antfly_row_fragments.row_fragment_stats_artifacts);
        self.* = undefined;
    }
};

pub fn planAlloc(
    alloc: Allocator,
    snapshot_id: []const u8,
    schema_fingerprint: []const u8,
    row_fragments: []const PublishedArtifact,
    row_fragment_stats: []const PublishedArtifact,
) !Plan {
    if (snapshot_id.len == 0) return error.InvalidRowFragmentManifestPlan;
    if (schema_fingerprint.len == 0) return error.InvalidRowFragmentManifestPlan;
    if (row_fragments.len == 0) return error.InvalidRowFragmentManifestPlan;

    const artifact_count = row_fragments.len + row_fragment_stats.len;
    const artifacts = try alloc.alloc(manifest_artifact.ArtifactRef, artifact_count);
    errdefer alloc.free(artifacts);
    var initialized_artifacts: usize = 0;
    errdefer {
        for (artifacts[0..initialized_artifacts]) |artifact| {
            if (artifact.name.len != 0) alloc.free(artifact.name);
            alloc.free(artifact.artifact_id);
            alloc.free(artifact.checksum);
        }
    }

    const row_fragment_ids = try alloc.alloc([]const u8, row_fragments.len);
    errdefer alloc.free(row_fragment_ids);
    const row_fragment_stats_ids = try alloc.alloc([]const u8, row_fragment_stats.len);
    errdefer alloc.free(row_fragment_stats_ids);

    for (row_fragments, 0..) |artifact, idx| {
        artifacts[initialized_artifacts] = try cloneArtifactRef(alloc, .row_fragment, artifact);
        row_fragment_ids[idx] = artifacts[initialized_artifacts].artifact_id;
        initialized_artifacts += 1;
    }
    for (row_fragment_stats, 0..) |artifact, idx| {
        artifacts[initialized_artifacts] = try cloneArtifactRef(alloc, .row_fragment_stats, artifact);
        row_fragment_stats_ids[idx] = artifacts[initialized_artifacts].artifact_id;
        initialized_artifacts += 1;
    }

    const snapshot_id_copy = try alloc.dupe(u8, snapshot_id);
    errdefer alloc.free(snapshot_id_copy);
    const schema_fingerprint_copy = try alloc.dupe(u8, schema_fingerprint);
    errdefer alloc.free(schema_fingerprint_copy);

    const base_source = manifest_base_source.BaseSourceDescriptor{ .antfly_row_fragments = .{
        .snapshot_id = snapshot_id_copy,
        .schema_fingerprint = schema_fingerprint_copy,
        .row_fragment_artifacts = row_fragment_ids,
        .row_fragment_stats_artifacts = row_fragment_stats_ids,
    } };
    try base_source.validate();

    return .{
        .base_source = base_source,
        .artifacts = artifacts,
    };
}

fn cloneArtifactRef(
    alloc: Allocator,
    kind: manifest_artifact.ArtifactKind,
    artifact: PublishedArtifact,
) !manifest_artifact.ArtifactRef {
    if (artifact.artifact_id.len == 0) return error.InvalidRowFragmentManifestPlan;
    if (artifact.checksum.len == 0) return error.InvalidRowFragmentManifestPlan;
    return .{
        .kind = kind,
        .name = if (artifact.name.len == 0) &.{} else try alloc.dupe(u8, artifact.name),
        .artifact_id = try alloc.dupe(u8, artifact.artifact_id),
        .byte_len = artifact.byte_len,
        .checksum = try alloc.dupe(u8, artifact.checksum),
    };
}

test "row fragment manifest plan creates artifacts and base source" {
    const alloc = std.testing.allocator;
    const fragments = [_]PublishedArtifact{
        .{
            .artifact_id = "rows-0001",
            .byte_len = 1024,
            .checksum = "sha256:rows",
            .name = "orders.rows",
        },
    };
    const stats = [_]PublishedArtifact{
        .{
            .artifact_id = "rows-0001.stats",
            .byte_len = 128,
            .checksum = "sha256:stats",
        },
    };

    var plan = try planAlloc(alloc, "manifest-7", "schema-v1", &fragments, &stats);
    defer plan.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), plan.artifacts.len);
    try std.testing.expectEqual(manifest_artifact.ArtifactKind.row_fragment, plan.artifacts[0].kind);
    try std.testing.expectEqual(manifest_artifact.ArtifactKind.row_fragment_stats, plan.artifacts[1].kind);
    try std.testing.expectEqualStrings("rows-0001", plan.base_source.antfly_row_fragments.row_fragment_artifacts[0]);
    try plan.base_source.validate();
}
