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

//! Manifest planning helpers for serverless algebraic materialization artifacts.

const std = @import("std");
const Allocator = std.mem.Allocator;
const manifest_artifact = @import("../manifest/artifact_ref.zig");

pub const PublishedArtifact = struct {
    artifact_id: []const u8,
    byte_len: u64,
    checksum: []const u8,
    name: []const u8 = &.{},
};

pub const Plan = struct {
    artifacts: []manifest_artifact.ArtifactRef,

    pub fn deinit(self: *Plan, alloc: Allocator) void {
        for (self.artifacts) |artifact| {
            if (artifact.name.len != 0) alloc.free(artifact.name);
            alloc.free(artifact.artifact_id);
            alloc.free(artifact.checksum);
        }
        alloc.free(self.artifacts);
        self.* = undefined;
    }
};

pub fn planAlloc(
    alloc: Allocator,
    algebraic_segments: []const PublishedArtifact,
) !Plan {
    if (algebraic_segments.len == 0) return error.InvalidAlgebraicManifestPlan;

    const artifacts = try alloc.alloc(manifest_artifact.ArtifactRef, algebraic_segments.len);
    errdefer alloc.free(artifacts);
    var initialized: usize = 0;
    errdefer {
        for (artifacts[0..initialized]) |artifact| {
            if (artifact.name.len != 0) alloc.free(artifact.name);
            alloc.free(artifact.artifact_id);
            alloc.free(artifact.checksum);
        }
    }

    for (algebraic_segments, artifacts) |artifact, *out| {
        out.* = try cloneArtifactRef(alloc, artifact);
        initialized += 1;
    }

    return .{ .artifacts = artifacts };
}

fn cloneArtifactRef(
    alloc: Allocator,
    artifact: PublishedArtifact,
) !manifest_artifact.ArtifactRef {
    if (artifact.artifact_id.len == 0) return error.InvalidAlgebraicManifestPlan;
    if (artifact.checksum.len == 0) return error.InvalidAlgebraicManifestPlan;
    return .{
        .kind = .algebraic_segment,
        .name = if (artifact.name.len == 0) &.{} else try alloc.dupe(u8, artifact.name),
        .artifact_id = try alloc.dupe(u8, artifact.artifact_id),
        .byte_len = artifact.byte_len,
        .checksum = try alloc.dupe(u8, artifact.checksum),
    };
}

test "algebraic manifest plan creates algebraic segment artifacts" {
    const alloc = std.testing.allocator;
    const segments = [_]PublishedArtifact{
        .{
            .artifact_id = "folds-0001",
            .byte_len = 512,
            .checksum = "sha256:folds",
            .name = "orders.amount_by_tenant",
        },
    };

    var plan = try planAlloc(alloc, &segments);
    defer plan.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), plan.artifacts.len);
    try std.testing.expectEqual(manifest_artifact.ArtifactKind.algebraic_segment, plan.artifacts[0].kind);
    try std.testing.expectEqualStrings("folds-0001", plan.artifacts[0].artifact_id);
}
