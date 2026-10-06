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
    /// Object-prefix path, relative to the bucket (or filesystem connection
    /// root), within which this binding's source must remain.
    scope: []const u8 = &.{},

    pub fn validate(self: CredentialRef) !void {
        if (self.ref_id.len == 0) return error.InvalidExternalTableBinding;
        if (self.scope.len == 0) return;
        if (self.scope.len > 4096 or self.scope[0] == '/' or self.scope[self.scope.len - 1] == '/' or
            std.mem.indexOfAny(u8, self.scope, "\\\x00") != null)
        {
            return error.InvalidExternalTableBinding;
        }
        var components = std.mem.splitScalar(u8, self.scope, '/');
        while (components.next()) |component| {
            if (component.len == 0 or std.mem.eql(u8, component, ".") or std.mem.eql(u8, component, "..")) {
                return error.InvalidExternalTableBinding;
            }
        }
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
        if (self.credential_ref) |credential| {
            try credential.validate();
            if (credential.scope.len != 0) {
                const source_prefix = sourcePrefix(self.source_uri) orelse return error.InvalidExternalTableBinding;
                if (!isPrefixWithinCredentialScope(source_prefix, credential.scope)) {
                    return error.ExternalLakeCredentialScopeMismatch;
                }
            }
        }
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

fn sourcePrefix(uri: []const u8) ?[]const u8 {
    const rest = if (std.mem.startsWith(u8, uri, "s3://"))
        uri["s3://".len..]
    else if (std.mem.startsWith(u8, uri, "gs://"))
        uri["gs://".len..]
    else if (std.mem.startsWith(u8, uri, "file://"))
        return std.mem.trim(u8, uri["file://".len..], "/")
    else
        return null;
    const slash = std.mem.indexOfScalar(u8, rest, '/') orelse return "";
    return std.mem.trim(u8, rest[slash + 1 ..], "/");
}

pub fn isPrefixWithinCredentialScope(prefix: []const u8, scope: []const u8) bool {
    const requested = std.mem.trimEnd(u8, prefix, "/");
    const allowed = std.mem.trimEnd(u8, scope, "/");
    if (allowed.len == 0 or std.mem.eql(u8, requested, allowed)) return true;
    return requested.len > allowed.len and
        std.mem.startsWith(u8, requested, allowed) and
        requested[allowed.len] == '/';
}

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

pub fn bindingFromRuntimeExternalBaseSource(source: anytype) Binding {
    return .{
        .table_id = source.table_id,
        .format = externalFormatFromRuntime(source.format),
        .source_uri = source.source_uri,
        .credential_ref = if (source.credential_ref) |credential| .{
            .ref_id = credential.ref_id,
            .scope = credential.scope,
        } else null,
        .snapshot_mode = snapshotModeFromRuntime(source.snapshot_mode),
        .schema_fingerprint = source.schema_fingerprint,
        .write_policy = writePolicyFromRuntime(source.write_policy),
    };
}

fn externalFormatFromRuntime(format: anytype) external_source.Format {
    return switch (format) {
        .parquet => .parquet,
        .iceberg => .iceberg,
        .lance => .lance,
    };
}

fn snapshotModeFromRuntime(snapshot_mode: anytype) SnapshotMode {
    return switch (snapshot_mode) {
        .current => .current,
        .snapshot_id => |snapshot_id| .{ .snapshot_id = snapshot_id },
        .object_version_digest => |digest| .{ .object_version_digest = digest },
    };
}

fn writePolicyFromRuntime(write_policy: anytype) WritePolicy {
    return switch (write_policy) {
        .read_only => .read_only,
        .materialized_overlay => .materialized_overlay,
        .iceberg_writer => .iceberg_writer,
        .lake_native_relational => .lake_native_relational,
    };
}

test "external table binding validates read-only iceberg source" {
    const binding = Binding{
        .table_id = "events",
        .format = .iceberg,
        .source_uri = "s3://bucket/warehouse/events",
        .credential_ref = .{ .ref_id = "prod-lake-read", .scope = "warehouse/events" },
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

test "external source catalog admission rejects credential scope outside source prefix" {
    try std.testing.expectError(error.ExternalLakeCredentialScopeMismatch, (Binding{
        .table_id = "events",
        .format = .iceberg,
        .source_uri = "s3://bucket/warehouse/events",
        .credential_ref = .{ .ref_id = "prod-lake-read", .scope = "events" },
        .schema_fingerprint = "schema-v1",
    }).validate());
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

test "external table binding converts from runtime schema base source" {
    const RuntimeFormat = enum { parquet, iceberg, lance };
    const RuntimeSnapshotMode = union(enum) {
        current,
        snapshot_id: []const u8,
        object_version_digest: []const u8,
    };
    const RuntimeCredentialRef = struct {
        ref_id: []const u8,
        scope: []const u8 = "",
    };
    const RuntimeWritePolicy = enum {
        read_only,
        materialized_overlay,
        iceberg_writer,
        lake_native_relational,
    };
    const RuntimeSource = struct {
        table_id: []const u8,
        format: RuntimeFormat,
        source_uri: []const u8,
        credential_ref: ?RuntimeCredentialRef = null,
        snapshot_mode: RuntimeSnapshotMode = .current,
        schema_fingerprint: []const u8,
        write_policy: RuntimeWritePolicy = .read_only,
    };

    const source = RuntimeSource{
        .table_id = "events",
        .format = .iceberg,
        .source_uri = "s3://bucket/warehouse/events",
        .credential_ref = .{ .ref_id = "prod-lake-read", .scope = "warehouse/events" },
        .snapshot_mode = .{ .snapshot_id = "iceberg-123" },
        .schema_fingerprint = "schema-v5",
    };

    const binding = bindingFromRuntimeExternalBaseSource(source);
    try binding.validateReadOnlyMvp();
    try std.testing.expectEqual(external_source.Format.iceberg, binding.format);
    try std.testing.expectEqualStrings("events", binding.table_id);
    try std.testing.expectEqualStrings("s3://bucket/warehouse/events", binding.source_uri);
    try std.testing.expect(binding.credential_ref != null);
    try std.testing.expectEqualStrings("prod-lake-read", binding.credential_ref.?.ref_id);
    try std.testing.expectEqualStrings("warehouse/events", binding.credential_ref.?.scope);
    try std.testing.expectEqualStrings("iceberg-123", binding.snapshot_mode.snapshot_id);
    try std.testing.expectEqualStrings("schema-v5", binding.schema_fingerprint);
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
