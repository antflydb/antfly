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
    try binding.validateReadOnlyMvp();
    try inventory.validate();

    const encoded = try external_source_codec.encodeAlloc(alloc, inventory);
    defer alloc.free(encoded);

    var metadata = try artifacts.put(encoded);
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

const MemoryArtifactStore = struct {
    alloc: Allocator,
    bytes: ?[]u8 = null,

    fn init(alloc: Allocator) MemoryArtifactStore {
        return .{ .alloc = alloc };
    }

    fn deinit(self: *MemoryArtifactStore) void {
        if (self.bytes) |bytes| self.alloc.free(bytes);
        self.* = undefined;
    }

    fn artifactStore(self: *MemoryArtifactStore) artifact_store.ArtifactStore {
        return .{
            .allocator = self.alloc,
            .ptr = self,
            .vtable = &vtable,
        };
    }

    fn put(self: *MemoryArtifactStore, alloc: Allocator, contents: []const u8) !artifact_store.ArtifactMetadata {
        if (self.bytes) |bytes| self.alloc.free(bytes);
        self.bytes = try self.alloc.dupe(u8, contents);
        return .{
            .artifact_id = try alloc.dupe(u8, "mem:external-files"),
            .byte_len = @intCast(contents.len),
            .checksum = try std.fmt.allocPrint(alloc, "len:{d}", .{contents.len}),
        };
    }

    fn getAlloc(self: *MemoryArtifactStore, alloc: Allocator, artifact_id: []const u8) ![]u8 {
        if (!std.mem.eql(u8, artifact_id, "mem:external-files")) return error.ArtifactNotFound;
        const bytes = self.bytes orelse return error.ArtifactNotFound;
        return try alloc.dupe(u8, bytes);
    }

    fn getRangeAlloc(self: *MemoryArtifactStore, alloc: Allocator, artifact_id: []const u8, offset: u64, len: usize) ![]u8 {
        const bytes = try self.getAlloc(alloc, artifact_id);
        defer alloc.free(bytes);
        if (offset > bytes.len) return error.InvalidRange;
        const start: usize = @intCast(offset);
        const end = @min(bytes.len, start + len);
        return try alloc.dupe(u8, bytes[start..end]);
    }

    fn stat(self: *MemoryArtifactStore, alloc: Allocator, artifact_id: []const u8) !artifact_store.ArtifactMetadata {
        const bytes = try self.getAlloc(alloc, artifact_id);
        defer alloc.free(bytes);
        return .{
            .artifact_id = try alloc.dupe(u8, "mem:external-files"),
            .byte_len = @intCast(bytes.len),
            .checksum = try std.fmt.allocPrint(alloc, "len:{d}", .{bytes.len}),
        };
    }

    fn delete(self: *MemoryArtifactStore, artifact_id: []const u8) !void {
        if (!std.mem.eql(u8, artifact_id, "mem:external-files")) return error.ArtifactNotFound;
        if (self.bytes) |bytes| self.alloc.free(bytes);
        self.bytes = null;
    }

    const vtable: artifact_store.ArtifactStore.VTable = .{
        .deinit = erasedDeinit,
        .put = erasedPut,
        .get_alloc = erasedGetAlloc,
        .get_range_alloc = erasedGetRangeAlloc,
        .stat = erasedStat,
        .delete = erasedDelete,
    };

    fn erasedDeinit(_: Allocator, ptr: *anyopaque) void {
        const self: *MemoryArtifactStore = @ptrCast(@alignCast(ptr));
        self.deinit();
    }

    fn erasedPut(ptr: *anyopaque, alloc: Allocator, contents: []const u8) !artifact_store.ArtifactMetadata {
        const self: *MemoryArtifactStore = @ptrCast(@alignCast(ptr));
        return try self.put(alloc, contents);
    }

    fn erasedGetAlloc(ptr: *anyopaque, alloc: Allocator, artifact_id: []const u8) ![]u8 {
        const self: *MemoryArtifactStore = @ptrCast(@alignCast(ptr));
        return try self.getAlloc(alloc, artifact_id);
    }

    fn erasedGetRangeAlloc(ptr: *anyopaque, alloc: Allocator, artifact_id: []const u8, offset: u64, len: usize) ![]u8 {
        const self: *MemoryArtifactStore = @ptrCast(@alignCast(ptr));
        return try self.getRangeAlloc(alloc, artifact_id, offset, len);
    }

    fn erasedStat(ptr: *anyopaque, alloc: Allocator, artifact_id: []const u8) !artifact_store.ArtifactMetadata {
        const self: *MemoryArtifactStore = @ptrCast(@alignCast(ptr));
        return try self.stat(alloc, artifact_id);
    }

    fn erasedDelete(ptr: *anyopaque, artifact_id: []const u8) !void {
        const self: *MemoryArtifactStore = @ptrCast(@alignCast(ptr));
        try self.delete(artifact_id);
    }
};

test "external source publisher writes inventory artifact and returns manifest plan" {
    const alloc = std.testing.allocator;
    var memory = MemoryArtifactStore.init(alloc);
    var artifacts = memory.artifactStore();
    defer artifacts.deinit();

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
    try std.testing.expectEqualStrings("mem:external-files", artifact.artifact_id);
    try std.testing.expectEqualStrings("mem:external-files", result.plan.base_source.external_iceberg.file_inventory_artifact.?);
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
