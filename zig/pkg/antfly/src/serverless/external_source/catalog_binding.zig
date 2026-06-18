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

//! Catalog-facing external lake table bindings.
//!
//! A binding describes the authoritative external table attached to a logical
//! Antfly table. It is intentionally separate from external_source.Inventory:
//! the binding is durable catalog metadata, while an inventory is a pinned,
//! discovered snapshot of files and row groups.

const std = @import("std");
const external_source = @import("types.zig");
const manifest_base_source = @import("../manifest/base_source.zig");
const rowsource = @import("../../storage/rowsource/types.zig");

pub const CredentialRef = struct {
    /// Name of an Antfly-managed credential. Raw cloud keys must not be stored
    /// in catalog bindings.
    ref_id: []const u8,
    scope: []const u8 = &.{},

    pub fn validate(self: CredentialRef) !void {
        if (self.ref_id.len == 0) return error.InvalidExternalTableBinding;
    }
};

pub const SnapshotMode = union(enum) {
    /// Resolve the provider's current snapshot during planning, then pin the
    /// resolved snapshot id for the lifetime of the query/build.
    current,
    /// Bind to an explicit Iceberg/Lance/provider snapshot id.
    snapshot_id: []const u8,
    /// Bind a raw Parquet prefix to a digest over object keys, sizes, and
    /// ETags/version IDs.
    object_version_digest: []const u8,

    pub fn validate(self: SnapshotMode) !void {
        switch (self) {
            .current => {},
            .snapshot_id => |value| if (value.len == 0) return error.InvalidExternalTableBinding,
            .object_version_digest => |value| if (value.len == 0) return error.InvalidExternalTableBinding,
        }
    }

    pub fn pinnedSnapshotId(self: SnapshotMode) ?[]const u8 {
        return switch (self) {
            .current => null,
            .snapshot_id => |value| value,
            .object_version_digest => |value| value,
        };
    }

    pub fn requiresDiscoveryPin(self: SnapshotMode) bool {
        return self == .current;
    }
};

pub const WritePolicy = enum(u8) {
    /// MVP and default: Antfly reads, indexes, caches, and materializes sidecars
    /// but does not mutate the user's lake table.
    read_only = 1,
    /// Antfly writes mutable or materialized overlay rows in Antfly-owned
    /// storage while preserving the external lake as the base copy.
    materialized_overlay = 2,
    /// Future mode where Antfly can commit Parquet data and Iceberg metadata.
    iceberg_writer = 3,
    /// Future Antfly-owned LTAP path where hot relational writes are committed
    /// into Antfly lake-native fragments and optionally exported.
    lake_native_relational = 4,

    pub fn mutatesExternalLake(self: WritePolicy) bool {
        return self == .iceberg_writer;
    }

    pub fn usesAntflyOwnedWrites(self: WritePolicy) bool {
        return self == .materialized_overlay or self == .lake_native_relational;
    }
};

pub const Binding = struct {
    table_id: []const u8,
    format: external_source.Format,
    source_uri: []const u8,
    credential_ref: ?CredentialRef = null,
    snapshot_mode: SnapshotMode = .current,
    schema_fingerprint: []const u8,
    write_policy: WritePolicy = .read_only,

    pub fn validate(self: Binding) !void {
        if (self.table_id.len == 0) return error.InvalidExternalTableBinding;
        if (self.source_uri.len == 0) return error.InvalidExternalTableBinding;
        if (self.schema_fingerprint.len == 0) return error.InvalidExternalTableBinding;
        if (self.credential_ref) |credential| try credential.validate();
        try self.snapshot_mode.validate();
    }

    pub fn validateReadOnlyMvp(self: Binding) !void {
        try self.validate();
        if (self.write_policy != .read_only) return error.UnsupportedExternalTableWritePolicy;
    }

    pub fn rowSourceKind(self: Binding) rowsource.SourceKind {
        return sourceKindForFormat(self.format);
    }

    pub fn manifestFormat(self: Binding) manifest_base_source.ExternalBaseFormat {
        return manifestFormatForExternalFormat(self.format);
    }

    pub fn toManifestBaseSource(
        self: Binding,
        pinned_snapshot_id: []const u8,
        file_inventory_artifact: ?[]const u8,
    ) !manifest_base_source.BaseSourceDescriptor {
        try self.validate();
        if (pinned_snapshot_id.len == 0) return error.InvalidExternalTableBinding;
        if (file_inventory_artifact) |artifact_id| {
            if (artifact_id.len == 0) return error.InvalidExternalTableBinding;
        }

        const source = manifest_base_source.ExternalBaseSource{
            .format = self.manifestFormat(),
            .source_uri = self.source_uri,
            .snapshot_id = pinned_snapshot_id,
            .schema_fingerprint = self.schema_fingerprint,
            .file_inventory_artifact = file_inventory_artifact,
        };
        const descriptor = switch (self.format) {
            .parquet => manifest_base_source.BaseSourceDescriptor{ .external_parquet = source },
            .iceberg => manifest_base_source.BaseSourceDescriptor{ .external_iceberg = source },
            .lance => manifest_base_source.BaseSourceDescriptor{ .external_lance = source },
        };
        try descriptor.validate();
        return descriptor;
    }
};

pub fn sourceKindForFormat(format: external_source.Format) rowsource.SourceKind {
    return switch (format) {
        .parquet => .external_parquet,
        .iceberg => .external_iceberg,
        .lance => .external_lance,
    };
}

pub fn manifestFormatForExternalFormat(format: external_source.Format) manifest_base_source.ExternalBaseFormat {
    return switch (format) {
        .parquet => .parquet_prefix,
        .iceberg => .iceberg,
        .lance => .lance,
    };
}

test "external table binding validates read-only iceberg source" {
    const binding = Binding{
        .table_id = "events",
        .format = .iceberg,
        .source_uri = "s3://bucket/warehouse/events",
        .credential_ref = .{ .ref_id = "prod-lake-read", .scope = "events" },
        .snapshot_mode = .{ .snapshot_id = "iceberg-123" },
        .schema_fingerprint = "schema-v2",
    };

    try binding.validateReadOnlyMvp();
    try std.testing.expectEqual(rowsource.SourceKind.external_iceberg, binding.rowSourceKind());
    try std.testing.expectEqual(manifest_base_source.ExternalBaseFormat.iceberg, binding.manifestFormat());
    try std.testing.expectEqualStrings("iceberg-123", binding.snapshot_mode.pinnedSnapshotId().?);
    try std.testing.expect(!binding.snapshot_mode.requiresDiscoveryPin());

    const descriptor = try binding.toManifestBaseSource("iceberg-123", "files-0001");
    try descriptor.validate();
    try std.testing.expectEqualStrings("files-0001", descriptor.external_iceberg.file_inventory_artifact.?);
}

test "external table binding supports raw parquet object-version snapshots" {
    const binding = Binding{
        .table_id = "logs",
        .format = .parquet,
        .source_uri = "s3://bucket/logs/",
        .snapshot_mode = .{ .object_version_digest = "sha256:objects" },
        .schema_fingerprint = "schema-v1",
    };

    try binding.validateReadOnlyMvp();
    try std.testing.expectEqual(rowsource.SourceKind.external_parquet, binding.rowSourceKind());
    try std.testing.expectEqualStrings("sha256:objects", binding.snapshot_mode.pinnedSnapshotId().?);

    const descriptor = try binding.toManifestBaseSource("sha256:objects", null);
    try descriptor.validate();
    try std.testing.expectEqual(manifest_base_source.ExternalBaseFormat.parquet_prefix, descriptor.external_parquet.format);
}

test "external table binding rejects empty fields and non-mvp writes" {
    try std.testing.expectError(error.InvalidExternalTableBinding, (Binding{
        .table_id = "",
        .format = .lance,
        .source_uri = "s3://bucket/lance/events",
        .schema_fingerprint = "schema-v1",
    }).validate());

    try std.testing.expectError(error.InvalidExternalTableBinding, (Binding{
        .table_id = "events",
        .format = .iceberg,
        .source_uri = "s3://bucket/warehouse/events",
        .credential_ref = .{ .ref_id = "" },
        .schema_fingerprint = "schema-v1",
    }).validate());

    const future_write = Binding{
        .table_id = "events",
        .format = .iceberg,
        .source_uri = "s3://bucket/warehouse/events",
        .snapshot_mode = .current,
        .schema_fingerprint = "schema-v1",
        .write_policy = .iceberg_writer,
    };
    try future_write.validate();
    try std.testing.expect(future_write.write_policy.mutatesExternalLake());
    try std.testing.expectError(error.UnsupportedExternalTableWritePolicy, future_write.validateReadOnlyMvp());
    try std.testing.expect(future_write.snapshot_mode.requiresDiscoveryPin());
}
