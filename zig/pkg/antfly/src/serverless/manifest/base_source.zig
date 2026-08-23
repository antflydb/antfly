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

//! Manifest-side descriptors for authoritative lake-native row sources.

const std = @import("std");
const Allocator = std.mem.Allocator;

pub const BaseSourceKind = enum(u8) {
    antfly_document_segments = 1,
    antfly_row_fragments = 2,
    antfly_lsm_overlay = 3,
    external_parquet = 4,
    external_iceberg = 5,
    external_lance = 6,
};

pub const ExternalBaseFormat = enum(u8) {
    parquet_prefix = 1,
    iceberg = 2,
    lance = 3,
};

pub const AntflyFragmentBaseSource = struct {
    snapshot_id: []const u8,
    schema_fingerprint: []const u8,
    row_fragment_artifacts: []const []const u8 = &.{},
    row_fragment_stats_artifacts: []const []const u8 = &.{},

    pub fn validate(self: AntflyFragmentBaseSource) !void {
        if (self.snapshot_id.len == 0) return error.InvalidManifestBaseSource;
        if (self.schema_fingerprint.len == 0) return error.InvalidManifestBaseSource;
        for (self.row_fragment_artifacts) |artifact_id| {
            if (artifact_id.len == 0) return error.InvalidManifestBaseSource;
        }
        for (self.row_fragment_stats_artifacts) |artifact_id| {
            if (artifact_id.len == 0) return error.InvalidManifestBaseSource;
        }
    }
};

pub const ExternalBaseSource = struct {
    format: ExternalBaseFormat,
    source_uri: []const u8,
    snapshot_id: []const u8,
    schema_fingerprint: []const u8,
    file_inventory_artifact: ?[]const u8 = null,
    row_group_metadata_artifact: ?[]const u8 = null,
    delete_metadata_artifact: ?[]const u8 = null,

    pub fn validate(self: ExternalBaseSource) !void {
        if (self.source_uri.len == 0) return error.InvalidManifestBaseSource;
        if (self.snapshot_id.len == 0) return error.InvalidManifestBaseSource;
        if (self.schema_fingerprint.len == 0) return error.InvalidManifestBaseSource;
        if (self.file_inventory_artifact) |artifact_id| {
            if (artifact_id.len == 0) return error.InvalidManifestBaseSource;
        }
        if (self.row_group_metadata_artifact) |artifact_id| {
            if (artifact_id.len == 0) return error.InvalidManifestBaseSource;
        }
        if (self.delete_metadata_artifact) |artifact_id| {
            if (artifact_id.len == 0) return error.InvalidManifestBaseSource;
        }
    }
};

pub const BaseSourceDescriptor = union(BaseSourceKind) {
    antfly_document_segments: void,
    antfly_row_fragments: AntflyFragmentBaseSource,
    antfly_lsm_overlay: void,
    external_parquet: ExternalBaseSource,
    external_iceberg: ExternalBaseSource,
    external_lance: ExternalBaseSource,

    pub fn validate(self: BaseSourceDescriptor) !void {
        switch (self) {
            .antfly_document_segments, .antfly_lsm_overlay => {},
            .antfly_row_fragments => |source| try source.validate(),
            .external_parquet => |source| {
                if (source.format != .parquet_prefix) return error.InvalidManifestBaseSource;
                try source.validate();
            },
            .external_iceberg => |source| {
                if (source.format != .iceberg) return error.InvalidManifestBaseSource;
                try source.validate();
            },
            .external_lance => |source| {
                if (source.format != .lance) return error.InvalidManifestBaseSource;
                try source.validate();
            },
        }
    }
};

pub fn cloneDescriptorAlloc(alloc: Allocator, src: BaseSourceDescriptor) !BaseSourceDescriptor {
    return switch (src) {
        .antfly_document_segments => .{ .antfly_document_segments = {} },
        .antfly_lsm_overlay => .{ .antfly_lsm_overlay = {} },
        .antfly_row_fragments => |source| .{ .antfly_row_fragments = try cloneAntflyFragmentBaseSourceAlloc(alloc, source) },
        .external_parquet => |source| .{ .external_parquet = try cloneExternalBaseSourceAlloc(alloc, source) },
        .external_iceberg => |source| .{ .external_iceberg = try cloneExternalBaseSourceAlloc(alloc, source) },
        .external_lance => |source| .{ .external_lance = try cloneExternalBaseSourceAlloc(alloc, source) },
    };
}

pub fn freeOwnedDescriptor(alloc: Allocator, descriptor: *BaseSourceDescriptor) void {
    switch (descriptor.*) {
        .antfly_document_segments, .antfly_lsm_overlay => {},
        .antfly_row_fragments => |source| {
            alloc.free(source.snapshot_id);
            alloc.free(source.schema_fingerprint);
            freeStringList(alloc, source.row_fragment_artifacts);
            freeStringList(alloc, source.row_fragment_stats_artifacts);
        },
        .external_parquet, .external_iceberg, .external_lance => |source| {
            freeExternalBaseSource(alloc, source);
        },
    }
    descriptor.* = undefined;
}

fn cloneAntflyFragmentBaseSourceAlloc(alloc: Allocator, source: AntflyFragmentBaseSource) !AntflyFragmentBaseSource {
    const snapshot_id = try alloc.dupe(u8, source.snapshot_id);
    errdefer alloc.free(snapshot_id);
    const schema_fingerprint = try alloc.dupe(u8, source.schema_fingerprint);
    errdefer alloc.free(schema_fingerprint);
    const row_fragment_artifacts = try cloneStringListAlloc(alloc, source.row_fragment_artifacts);
    errdefer freeStringList(alloc, row_fragment_artifacts);
    const row_fragment_stats_artifacts = try cloneStringListAlloc(alloc, source.row_fragment_stats_artifacts);
    errdefer freeStringList(alloc, row_fragment_stats_artifacts);

    return .{
        .snapshot_id = snapshot_id,
        .schema_fingerprint = schema_fingerprint,
        .row_fragment_artifacts = row_fragment_artifacts,
        .row_fragment_stats_artifacts = row_fragment_stats_artifacts,
    };
}

fn cloneExternalBaseSourceAlloc(alloc: Allocator, source: ExternalBaseSource) !ExternalBaseSource {
    const source_uri = try alloc.dupe(u8, source.source_uri);
    errdefer alloc.free(source_uri);
    const snapshot_id = try alloc.dupe(u8, source.snapshot_id);
    errdefer alloc.free(snapshot_id);
    const schema_fingerprint = try alloc.dupe(u8, source.schema_fingerprint);
    errdefer alloc.free(schema_fingerprint);
    const file_inventory_artifact = if (source.file_inventory_artifact) |artifact_id| try alloc.dupe(u8, artifact_id) else null;
    errdefer if (file_inventory_artifact) |artifact_id| alloc.free(artifact_id);
    const row_group_metadata_artifact = if (source.row_group_metadata_artifact) |artifact_id| try alloc.dupe(u8, artifact_id) else null;
    errdefer if (row_group_metadata_artifact) |artifact_id| alloc.free(artifact_id);
    const delete_metadata_artifact = if (source.delete_metadata_artifact) |artifact_id| try alloc.dupe(u8, artifact_id) else null;
    errdefer if (delete_metadata_artifact) |artifact_id| alloc.free(artifact_id);

    return .{
        .format = source.format,
        .source_uri = source_uri,
        .snapshot_id = snapshot_id,
        .schema_fingerprint = schema_fingerprint,
        .file_inventory_artifact = file_inventory_artifact,
        .row_group_metadata_artifact = row_group_metadata_artifact,
        .delete_metadata_artifact = delete_metadata_artifact,
    };
}

fn freeExternalBaseSource(alloc: Allocator, source: ExternalBaseSource) void {
    alloc.free(source.source_uri);
    alloc.free(source.snapshot_id);
    alloc.free(source.schema_fingerprint);
    if (source.file_inventory_artifact) |artifact_id| alloc.free(artifact_id);
    if (source.row_group_metadata_artifact) |artifact_id| alloc.free(artifact_id);
    if (source.delete_metadata_artifact) |artifact_id| alloc.free(artifact_id);
}

fn cloneStringListAlloc(alloc: Allocator, strings: []const []const u8) ![]const []const u8 {
    if (strings.len == 0) return &.{};
    const out = try alloc.alloc([]const u8, strings.len);
    errdefer alloc.free(out);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |item| alloc.free(item);
    }
    for (strings, 0..) |item, idx| {
        out[idx] = try alloc.dupe(u8, item);
        initialized += 1;
    }
    return out;
}

fn freeStringList(alloc: Allocator, strings: []const []const u8) void {
    for (strings) |item| alloc.free(item);
    if (strings.len != 0) alloc.free(strings);
}

test "base source descriptors validate Antfly and external row sources" {
    const row_fragments = [_][]const u8{ "rows-0001", "rows-0002" };
    try (BaseSourceDescriptor{ .antfly_row_fragments = .{
        .snapshot_id = "manifest-7",
        .schema_fingerprint = "schema-v1",
        .row_fragment_artifacts = &row_fragments,
    } }).validate();

    try (BaseSourceDescriptor{ .external_iceberg = .{
        .format = .iceberg,
        .source_uri = "s3://bucket/warehouse/events",
        .snapshot_id = "123456",
        .schema_fingerprint = "schema-v2",
        .file_inventory_artifact = "files-0001",
        .delete_metadata_artifact = "deletes-0001",
    } }).validate();

    try std.testing.expectError(error.InvalidManifestBaseSource, (BaseSourceDescriptor{ .external_iceberg = .{
        .format = .parquet_prefix,
        .source_uri = "s3://bucket/warehouse/events",
        .snapshot_id = "123456",
        .schema_fingerprint = "schema-v2",
    } }).validate());
}

test "base source descriptors clone and free owned storage" {
    const alloc = std.testing.allocator;
    const row_fragments = [_][]const u8{ "rows-0001", "rows-0002" };
    var cloned = try cloneDescriptorAlloc(alloc, .{ .antfly_row_fragments = .{
        .snapshot_id = "manifest-7",
        .schema_fingerprint = "schema-v1",
        .row_fragment_artifacts = &row_fragments,
    } });
    defer freeOwnedDescriptor(alloc, &cloned);

    try cloned.validate();
    try std.testing.expectEqualStrings("manifest-7", cloned.antfly_row_fragments.snapshot_id);
    try std.testing.expectEqualStrings("rows-0002", cloned.antfly_row_fragments.row_fragment_artifacts[1]);

    var external = try cloneDescriptorAlloc(alloc, .{ .external_iceberg = .{
        .format = .iceberg,
        .source_uri = "s3://bucket/warehouse/events",
        .snapshot_id = "123456",
        .schema_fingerprint = "schema-v2",
        .file_inventory_artifact = "files-0001",
    } });
    defer freeOwnedDescriptor(alloc, &external);

    try external.validate();
    try std.testing.expectEqualStrings("files-0001", external.external_iceberg.file_inventory_artifact.?);
}
