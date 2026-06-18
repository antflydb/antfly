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

//! Manifest planning helpers for user-owned external lake sources.

const std = @import("std");
const Allocator = std.mem.Allocator;
const catalog_binding = @import("../external_source/catalog_binding.zig");
const external_source = @import("../external_source/types.zig");
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
        switch (self.base_source) {
            .external_parquet, .external_iceberg, .external_lance => |source| {
                alloc.free(source.source_uri);
                alloc.free(source.snapshot_id);
                alloc.free(source.schema_fingerprint);
            },
            else => {},
        }
        self.* = undefined;
    }
};

pub fn planAlloc(
    alloc: Allocator,
    format: manifest_base_source.ExternalBaseFormat,
    source_uri: []const u8,
    snapshot_id: []const u8,
    schema_fingerprint: []const u8,
    file_inventory: PublishedArtifact,
) !Plan {
    if (source_uri.len == 0) return error.InvalidExternalSourceManifestPlan;
    if (snapshot_id.len == 0) return error.InvalidExternalSourceManifestPlan;
    if (schema_fingerprint.len == 0) return error.InvalidExternalSourceManifestPlan;

    const artifacts = try alloc.alloc(manifest_artifact.ArtifactRef, 1);
    errdefer alloc.free(artifacts);
    artifacts[0] = try cloneArtifactRef(alloc, .external_base_source, file_inventory);
    errdefer {
        if (artifacts[0].name.len != 0) alloc.free(artifacts[0].name);
        alloc.free(artifacts[0].artifact_id);
        alloc.free(artifacts[0].checksum);
    }

    const source_uri_copy = try alloc.dupe(u8, source_uri);
    errdefer alloc.free(source_uri_copy);
    const snapshot_id_copy = try alloc.dupe(u8, snapshot_id);
    errdefer alloc.free(snapshot_id_copy);
    const schema_fingerprint_copy = try alloc.dupe(u8, schema_fingerprint);
    errdefer alloc.free(schema_fingerprint_copy);

    const source = manifest_base_source.ExternalBaseSource{
        .format = format,
        .source_uri = source_uri_copy,
        .snapshot_id = snapshot_id_copy,
        .schema_fingerprint = schema_fingerprint_copy,
        .file_inventory_artifact = artifacts[0].artifact_id,
    };
    const base_source = switch (format) {
        .parquet_prefix => manifest_base_source.BaseSourceDescriptor{ .external_parquet = source },
        .iceberg => manifest_base_source.BaseSourceDescriptor{ .external_iceberg = source },
        .lance => manifest_base_source.BaseSourceDescriptor{ .external_lance = source },
    };
    try base_source.validate();

    return .{
        .base_source = base_source,
        .artifacts = artifacts,
    };
}

pub fn planFromBindingAndInventoryAlloc(
    alloc: Allocator,
    binding: catalog_binding.Binding,
    inventory: external_source.Inventory,
    file_inventory: PublishedArtifact,
) !Plan {
    try binding.validateReadOnlyMvp();
    try inventory.validate();
    try validateBindingMatchesInventory(binding, inventory);

    return planAlloc(
        alloc,
        binding.manifestFormat(),
        binding.source_uri,
        inventory.snapshot_id,
        binding.schema_fingerprint,
        file_inventory,
    );
}

fn validateBindingMatchesInventory(
    binding: catalog_binding.Binding,
    inventory: external_source.Inventory,
) !void {
    if (binding.format != inventory.format) return error.ExternalSourceInventoryMismatch;
    if (!std.mem.eql(u8, binding.table_id, inventory.source_id)) return error.ExternalSourceInventoryMismatch;
    if (!std.mem.eql(u8, binding.source_uri, inventory.source_uri)) return error.ExternalSourceInventoryMismatch;
    if (!std.mem.eql(u8, binding.schema_fingerprint, inventory.schema_fingerprint)) return error.ExternalSourceInventoryMismatch;

    switch (binding.snapshot_mode) {
        .current => {},
        .snapshot_id => |snapshot_id| {
            if (!std.mem.eql(u8, snapshot_id, inventory.snapshot_id)) return error.ExternalSourceInventoryMismatch;
        },
        .object_version_digest => |digest| {
            if (!std.mem.eql(u8, digest, inventory.snapshot_id)) return error.ExternalSourceInventoryMismatch;
        },
    }
}

fn cloneArtifactRef(
    alloc: Allocator,
    kind: manifest_artifact.ArtifactKind,
    artifact: PublishedArtifact,
) !manifest_artifact.ArtifactRef {
    if (artifact.artifact_id.len == 0) return error.InvalidExternalSourceManifestPlan;
    if (artifact.checksum.len == 0) return error.InvalidExternalSourceManifestPlan;
    return .{
        .kind = kind,
        .name = if (artifact.name.len == 0) &.{} else try alloc.dupe(u8, artifact.name),
        .artifact_id = try alloc.dupe(u8, artifact.artifact_id),
        .byte_len = artifact.byte_len,
        .checksum = try alloc.dupe(u8, artifact.checksum),
    };
}

test "external source manifest plan creates inventory artifact and base source" {
    const alloc = std.testing.allocator;
    var plan = try planAlloc(
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

    try std.testing.expectEqual(@as(usize, 1), plan.artifacts.len);
    try std.testing.expectEqual(manifest_artifact.ArtifactKind.external_base_source, plan.artifacts[0].kind);
    try std.testing.expectEqualStrings("external-files-0001", plan.base_source.external_iceberg.file_inventory_artifact.?);
    try plan.base_source.validate();
}

test "external source manifest plan pins current catalog binding to discovered inventory" {
    const alloc = std.testing.allocator;
    var inventory = external_source.Inventory{
        .format = .iceberg,
        .source_id = try alloc.dupe(u8, "events"),
        .source_uri = try alloc.dupe(u8, "s3://bucket/warehouse/events"),
        .snapshot_id = try alloc.dupe(u8, "iceberg-123"),
        .schema_fingerprint = try alloc.dupe(u8, "schema-v1"),
        .files = try alloc.alloc(external_source.FileEntry, 1),
    };
    defer inventory.deinit(alloc);
    inventory.files[0] = .{
        .file_id = try alloc.dupe(u8, "file-a.parquet"),
        .object_uri = try alloc.dupe(u8, "s3://bucket/warehouse/events/file-a.parquet"),
        .etag = try alloc.dupe(u8, "etag-file-a"),
        .byte_len = 1024,
        .row_count = 0,
        .row_groups = &.{},
    };

    var plan = try planFromBindingAndInventoryAlloc(
        alloc,
        .{
            .table_id = "events",
            .format = .iceberg,
            .source_uri = "s3://bucket/warehouse/events",
            .snapshot_mode = .current,
            .schema_fingerprint = "schema-v1",
        },
        inventory,
        .{
            .artifact_id = "external-files-0001",
            .byte_len = 4096,
            .checksum = "sha256:files",
        },
    );
    defer plan.deinit(alloc);

    try std.testing.expectEqualStrings("iceberg-123", plan.base_source.external_iceberg.snapshot_id);
    try std.testing.expectEqualStrings("external-files-0001", plan.base_source.external_iceberg.file_inventory_artifact.?);
    try plan.base_source.validate();
}

test "external source manifest plan rejects stale explicit catalog binding" {
    const alloc = std.testing.allocator;
    var inventory = external_source.Inventory{
        .format = .parquet,
        .source_id = try alloc.dupe(u8, "logs"),
        .source_uri = try alloc.dupe(u8, "s3://bucket/logs"),
        .snapshot_id = try alloc.dupe(u8, "sha256:new"),
        .schema_fingerprint = try alloc.dupe(u8, "schema-v1"),
        .files = try alloc.alloc(external_source.FileEntry, 1),
    };
    defer inventory.deinit(alloc);
    inventory.files[0] = .{
        .file_id = try alloc.dupe(u8, "part-a.parquet"),
        .object_uri = try alloc.dupe(u8, "s3://bucket/logs/part-a.parquet"),
        .etag = try alloc.dupe(u8, "etag-file-a"),
        .byte_len = 1024,
        .row_count = 0,
        .row_groups = &.{},
    };

    try std.testing.expectError(error.ExternalSourceInventoryMismatch, planFromBindingAndInventoryAlloc(
        alloc,
        .{
            .table_id = "logs",
            .format = .parquet,
            .source_uri = "s3://bucket/logs",
            .snapshot_mode = .{ .object_version_digest = "sha256:old" },
            .schema_fingerprint = "schema-v1",
        },
        inventory,
        .{
            .artifact_id = "external-files-0001",
            .byte_len = 4096,
            .checksum = "sha256:files",
        },
    ));
}
