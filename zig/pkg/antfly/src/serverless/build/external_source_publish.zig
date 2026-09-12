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

//! Publication bridge for external lake inventories: encode the pinned file
//! inventory, write it through the serverless artifact store, and return manifest
//! metadata that pins the external source snapshot for a published generation.

const std = @import("std");
const Allocator = std.mem.Allocator;
const artifact_store = @import("../artifacts/store.zig");
const catalog_binding = @import("../external_source/catalog_binding.zig");
const external_source = @import("../external_source/types.zig");
const external_source_codec = @import("../external_source/codec.zig");
const external_source_manifest = @import("external_source_manifest.zig");

pub const PublishOptions = struct {
    artifact_name: []const u8 = &.{},
    previous_artifacts: []const @import("../manifest/artifact_ref.zig").ArtifactRef = &.{},
    cancellation: @import("../../common/cancellation.zig").CancellationToken = .none,
};

pub const PublishResult = struct {
    plan: external_source_manifest.Plan,

    pub fn deinit(self: *PublishResult, alloc: Allocator) void {
        self.plan.deinit(alloc);
        self.* = undefined;
    }
};

pub fn publishInventoryAlloc(
    alloc: Allocator,
    artifacts: *artifact_store.ArtifactStore,
    binding: catalog_binding.Binding,
    inventory: external_source.Inventory,
    options: PublishOptions,
) !PublishResult {
    const authority = artifacts.upload_scope orelse return error.ExternalInventoryPublicationScopeRequired;
    try authority.validate();
    try options.cancellation.check();
    var operation_artifacts = artifacts.*;
    operation_artifacts.allocator = alloc;
    try binding.validateReadOnlyMvp();
    try inventory.validate();

    const encoded = try external_source_codec.encodeAlloc(alloc, inventory);
    defer alloc.free(encoded);

    // Only the pinned current source may supply reusable identities. Never
    // reconstruct an old global content ID from its checksum: retired attempts
    // may still be undergoing collection while this publication commits.
    var metadata = reuse: {
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(encoded, &digest, .{});
        const checksum = std.fmt.bytesToHex(digest, .lower);
        for (options.previous_artifacts) |prior| {
            if (prior.kind != .external_base_source or prior.byte_len != encoded.len or !std.mem.eql(u8, prior.checksum, &checksum)) continue;
            const prior_scope = (try artifact_store.uploadScopeFromArtifactId(prior.artifact_id)) orelse continue;
            if (!std.mem.eql(u8, &prior_scope.domain, &authority.domain)) continue;
            try options.cancellation.check();
            artifacts.verifyContentWithCancellationUsingAllocator(alloc, prior.artifact_id, prior.byte_len, prior.checksum, options.cancellation) catch |err| switch (err) {
                error.FileNotFound, error.ArtifactNotFound, error.InvalidArtifactId, error.ArtifactIntegrityMismatch => continue,
                else => return err,
            };
            const id = try alloc.dupe(u8, prior.artifact_id);
            errdefer alloc.free(id);
            break :reuse artifact_store.ArtifactMetadata{ .artifact_id = id, .byte_len = prior.byte_len, .checksum = try alloc.dupe(u8, prior.checksum) };
        }
        break :reuse try operation_artifacts.putWithCancellation(encoded, options.cancellation);
    };
    defer metadata.deinit(alloc);

    return .{
        .plan = try external_source_manifest.planFromBindingAndInventoryAlloc(
            alloc,
            binding,
            inventory,
            .{
                .artifact_id = metadata.artifact_id,
                .byte_len = metadata.byte_len,
                .checksum = metadata.checksum,
                .name = options.artifact_name,
            },
        ),
    };
}

test "serverless external source publisher writes inventory artifact and returns manifest plan" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/publish", .{tmp.sub_path});
    defer alloc.free(path);
    var fs = try @import("../artifacts/fs_store.zig").FsStore.init(alloc, path);
    var artifacts = fs.artifactStore();
    defer artifacts.deinit();
    artifacts.upload_scope = .{ .domain = @import("../graph_segment/page_store.zig").PageStore.namespaceDomain("events"), .attempt = @splat(1) };

    var inventory = external_source.Inventory{
        .format = .iceberg,
        .source_id = try alloc.dupe(u8, "events"),
        .source_uri = try alloc.dupe(u8, "s3://bucket/events"),
        .snapshot_id = try alloc.dupe(u8, "12"),
        .schema_fingerprint = try alloc.dupe(u8, "iceberg-schema:7"),
        .files = try alloc.alloc(external_source.FileEntry, 1),
    };
    defer inventory.deinit(alloc);
    inventory.files[0] = .{
        .file_id = try alloc.dupe(u8, "data/a.parquet"),
        .object_uri = try alloc.dupe(u8, "s3://bucket/events/data/a.parquet"),
        .version_id = try alloc.dupe(u8, "iceberg:v1:snapshot=12:file_seq=1"),
        .byte_len = 4096,
        .row_count = 0,
        .row_groups = &.{},
    };

    var result = try publishInventoryAlloc(alloc, &artifacts, .{
        .table_id = "events",
        .format = .iceberg,
        .source_uri = "s3://bucket/events",
        .snapshot_mode = .current,
        .schema_fingerprint = "iceberg-schema:7",
    }, inventory, .{
        .artifact_name = "events.external-files",
    });
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), result.plan.artifacts.len);
    const artifact = result.plan.artifacts[0];
    try std.testing.expectEqualStrings("events.external-files", artifact.name);
    try std.testing.expectEqual(artifacts.upload_scope.?, (try artifact_store.uploadScopeFromArtifactId(artifact.artifact_id)).?);
    try std.testing.expectEqualStrings(artifact.artifact_id, result.plan.base_source.external_iceberg.file_inventory_artifact.?);
    try std.testing.expectEqualStrings("12", result.plan.base_source.external_iceberg.snapshot_id);

    const encoded = try artifacts.getAlloc(artifact.artifact_id);
    defer alloc.free(encoded);
    var decoded = try external_source_codec.decodeAlloc(alloc, encoded);
    defer decoded.deinit(alloc);
    try std.testing.expectEqual(external_source.Format.iceberg, decoded.format);
    try std.testing.expectEqualStrings("events", decoded.source_id);
    try std.testing.expectEqualStrings("12", decoded.snapshot_id);
    try std.testing.expectEqualStrings("data/a.parquet", decoded.files[0].file_id);
}

test "serverless external inventory X Y X publication survives a delayed retired inventory sweep" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/inventories", .{tmp.sub_path});
    defer a.free(path);
    var fs = try @import("../artifacts/fs_store.zig").FsStore.init(a, path);
    var artifacts = fs.artifactStore();
    defer artifacts.deinit();
    const domain = @import("../graph_segment/page_store.zig").PageStore.namespaceDomain("events");
    var inventory = external_source.Inventory{
        .format = .iceberg,
        .source_id = @constCast("events"),
        .source_uri = @constCast("s3://bucket/events"),
        .snapshot_id = @constCast("X"),
        .schema_fingerprint = @constCast("schema:1"),
        .files = &.{},
    };
    const binding = catalog_binding.Binding{ .table_id = "events", .format = .iceberg, .source_uri = "s3://bucket/events", .snapshot_mode = .current, .schema_fingerprint = "schema:1" };
    artifacts.upload_scope = .{ .domain = domain, .attempt = @splat(1) };
    var old_x = try publishInventoryAlloc(a, &artifacts, binding, inventory, .{});
    defer old_x.deinit(a);
    inventory.snapshot_id = @constCast("Y");
    artifacts.upload_scope = .{ .domain = domain, .attempt = @splat(2) };
    var current_y = try publishInventoryAlloc(a, &artifacts, binding, inventory, .{ .previous_artifacts = old_x.plan.artifacts });
    defer current_y.deinit(a);
    // A collector has already selected old X from obsolete HEAD history.
    // Publication now restores the exact same inventory bytes after its fence.
    inventory.snapshot_id = @constCast("X");
    artifacts.upload_scope = .{ .domain = domain, .attempt = @splat(3) };
    var new_x = try publishInventoryAlloc(a, &artifacts, binding, inventory, .{ .previous_artifacts = current_y.plan.artifacts });
    defer new_x.deinit(a);
    try std.testing.expectEqualStrings(old_x.plan.artifacts[0].checksum, new_x.plan.artifacts[0].checksum);
    try std.testing.expect(!std.mem.eql(u8, old_x.plan.artifacts[0].artifact_id, new_x.plan.artifacts[0].artifact_id));
    try artifacts.delete(old_x.plan.artifacts[0].artifact_id);
    var retained = try artifacts.stat(new_x.plan.artifacts[0].artifact_id);
    defer retained.deinit(a);
    // A later unchanged publication preserves source identity instead of
    // creating a fresh scoped inventory and an endless metadata republish.
    artifacts.upload_scope = .{ .domain = domain, .attempt = @splat(4) };
    var unchanged = try publishInventoryAlloc(a, &artifacts, binding, inventory, .{ .previous_artifacts = new_x.plan.artifacts });
    defer unchanged.deinit(a);
    try std.testing.expectEqualStrings(new_x.plan.artifacts[0].artifact_id, unchanged.plan.artifacts[0].artifact_id);
}
