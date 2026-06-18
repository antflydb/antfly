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

//! Apply an external lake manifest plan to an owned published generation.
//! This is the bridge from "inventory artifact was published" to "the manifest
//! head durably points at that pinned external source snapshot".

const std = @import("std");
const Allocator = std.mem.Allocator;
const external_source_manifest = @import("external_source_manifest.zig");
const manifest_base_source = @import("../manifest/base_source.zig");
const manifest_compatibility = @import("../manifest/compatibility.zig");
const manifest_types = @import("../manifest/types.zig");

pub fn attachPlanToOwnedManifestAlloc(
    alloc: Allocator,
    manifest: *manifest_types.Manifest,
    plan: external_source_manifest.Plan,
    policy: manifest_compatibility.Policy,
) !manifest_compatibility.Report {
    const artifacts = try manifest_types.cloneAppendedArtifactRefsAlloc(alloc, manifest.artifacts, plan.artifacts);
    errdefer {
        manifest_types.freeArtifactRefs(alloc, artifacts);
        alloc.free(artifacts);
    }
    var base_source = try manifest_base_source.cloneDescriptorAlloc(alloc, plan.base_source);
    errdefer manifest_base_source.freeOwnedDescriptor(alloc, &base_source);

    const report = try manifest_compatibility.checkLakeBaseSource(base_source, artifacts, policy);

    manifest_types.freeArtifactRefs(alloc, manifest.artifacts);
    alloc.free(manifest.artifacts);
    if (manifest.base_source) |*old_base_source| manifest_base_source.freeOwnedDescriptor(alloc, old_base_source);

    manifest.artifacts = artifacts;
    manifest.base_source = base_source;
    return report;
}

test "external source publication attaches plan to owned manifest" {
    const alloc = std.testing.allocator;
    var manifest = manifest_types.Manifest{
        .namespace = try alloc.dupe(u8, "events"),
        .version = 7,
        .built_at_ns = 123456,
        .wal_start_lsn = 10,
        .wal_end_lsn = 20,
        .stats = .{ .document_count = 0, .document_base_version = 7 },
        .artifacts = try alloc.alloc(manifest_types.ArtifactRef, 1),
    };
    defer manifest.deinit(alloc);
    manifest.artifacts[0] = .{
        .kind = .vector_segment,
        .name = try alloc.dupe(u8, "semantic_idx"),
        .artifact_id = try alloc.dupe(u8, "vec-1"),
        .byte_len = 128,
        .checksum = try alloc.dupe(u8, "len:128"),
    };

    var plan = try external_source_manifest.planAlloc(
        alloc,
        .iceberg,
        "s3://bucket/warehouse/events",
        "iceberg-123",
        "schema-v1",
        .{
            .artifact_id = "external-files-0001",
            .byte_len = 4096,
            .checksum = "sha256:files",
            .name = "events.files",
        },
    );
    defer plan.deinit(alloc);

    const report = try attachPlanToOwnedManifestAlloc(alloc, &manifest, plan, .{});

    try std.testing.expect(manifest.base_source != null);
    try std.testing.expectEqual(manifest_types.BaseSourceKind.external_iceberg, std.meta.activeTag(manifest.base_source.?));
    try std.testing.expectEqual(@as(usize, 2), manifest.artifacts.len);
    try std.testing.expectEqual(manifest_types.ArtifactKind.external_base_source, manifest.artifacts[1].kind);
    try std.testing.expectEqual(manifest_types.BaseSourceKind.external_iceberg, report.source_kind);
    try std.testing.expectEqual(@as(u32, 1), report.external_metadata_count);
}
