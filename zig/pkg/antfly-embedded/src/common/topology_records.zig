// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Runtime-independent topology record wire types shared by metadata and
//! portable storage artifacts. Keep this module below both layers so decoding
//! a seed never imports the metadata control loop into storage-only binaries.

const std = @import("std");

pub const TableRecord = struct {
    /// Opaque identity on compact internal routing projections. Full durable
    /// records omit this field; raw restore locations remain outside routing.
    routing_storage_binding: ?[32]u8 = null,
    /// Internal metadata lifecycle intent; never accepted as public schema.
    relational_retirement_json: []const u8 = "",
    /// Native, CAS-published generations over authorized external lake sources.
    /// Internal control-plane state; never a user-supplied index configuration.
    lake_index_catalog_json: []const u8 = "",
    storage: @import("table_storage.zig").Settings = .{},
    /// Durable incarnation fence for native object-backed WAL and artifacts.
    object_storage_generation: u64 = 0,
    object_storage_identity: [32]u8 = @splat(0),
    storage_migration: ?@import("vector_migration.zig").Admission = null,
    table_id: u64,
    name: []const u8,
    description: []const u8 = "",
    schema_json: []const u8 = "",
    read_schema_json: []const u8 = "",
    indexes_json: []const u8 = "{}",
    replication_sources_json: []const u8 = "[]",
    placement_role: []const u8 = "data",
    restore_backup_id: []const u8 = "",
    restore_location: []const u8 = "",
    desired_replica_count: u16 = 3,
    min_ranges: u32 = 1,

    pub fn jsonStringify(self: TableRecord, jw: anytype) !void {
        try jw.beginObject();
        inline for (@typeInfo(TableRecord).@"struct".field_names) |field_name| {
            const routing_field = comptime std.mem.eql(u8, field_name, "routing_storage_binding");
            const object_field = comptime std.mem.eql(u8, field_name, "object_storage_generation") or std.mem.eql(u8, field_name, "object_storage_identity");
            if ((!routing_field or self.routing_storage_binding != null) and (!object_field or self.storage.engine == .object) and (!std.mem.eql(u8, field_name, "lake_index_catalog_json") or self.lake_index_catalog_json.len != 0)) {
                try jw.objectField(field_name);
                try jw.write(@field(self, field_name));
            }
        }
        try jw.endObject();
    }

    /// Default legacy tables retain their exact durable record bytes. Storage
    /// ownership or an admitted migration requires the versioned extension.
    pub fn requiresStorageMetadataExtension(self: TableRecord) bool {
        return self.storage.engine != .local or self.storage.dense_embeddings != .primary_lsm or self.storage_migration != null;
    }

    pub fn migrationState(self: *const TableRecord) TableMigrationState {
        return .{
            .schema_json = self.schema_json,
            .read_schema_json = self.read_schema_json,
        };
    }

    pub fn indexCatalog(self: *const TableRecord) TableIndexCatalog {
        return .{ .indexes_json = self.indexes_json };
    }
};

pub const TableMigrationState = struct {
    schema_json: []const u8,
    read_schema_json: []const u8,

    pub fn migrating(self: TableMigrationState) bool {
        return self.read_schema_json.len > 0;
    }
};

pub const TableIndexCatalog = struct {
    indexes_json: []const u8,
};

pub const RangeRecord = struct {
    group_id: u64,
    range_id: u64 = 0,
    table_id: u64,
    start_key: []const u8,
    end_key: ?[]const u8 = null,
    doc_identity_shard_id: u64 = 0,
    doc_identity_range_id: u64 = 0,
    /// Monotonic source-local split attempt allocator. Durable metadata
    /// advances this only in the CAS command that admits the corresponding
    /// transition, so an epoch cannot be consumed without recovery state.
    split_attempt_epoch: u64 = 0,
    restore_backup_id: []const u8 = "",
    restore_artifact_backup_id: []const u8 = "",
    restore_location: []const u8 = "",
    restore_snapshot_path: []const u8 = "",
    /// Cluster-local authority used to resolve `restore_location`. This is an
    /// identifier only; credentials remain in each node's secret/config store.
    restore_connection: []const u8 = "",
    /// Content identity captured from the immutable backup manifest at
    /// admission. New restores require a SHA-256 binding.
    restore_artifact_size_bytes: u64 = 0,
    restore_artifact_sha256: []const u8 = "",
    /// Separately pinned identity for the native generation inventory. A
    /// distributed restore may classify projection-local damage only when this
    /// exact manifest remains authenticated across admission and execution.
    restore_native_manifest_size_bytes: u64 = 0,
    restore_native_manifest_sha256: []const u8 = "",
    /// Durable, bounded idempotency provenance for the most recently
    /// completed restore. Active replica progress can be garbage-collected
    /// without making an exact job retry ambiguous.
    completed_restore_fingerprint: RestoreCompletionFingerprint =
        empty_restore_completion_fingerprint,
};

pub const RestoreCompletionFingerprint =
    [std.crypto.hash.sha2.Sha256.digest_length]u8;
pub const empty_restore_completion_fingerprint: RestoreCompletionFingerprint =
    @as([std.crypto.hash.sha2.Sha256.digest_length]u8, @splat(0));

/// Preserve the established changefeed table-binding digest for full records.
/// A compact projection carries that exact digest without private path strings.
pub fn tableStorageBinding(table: anytype) [32]u8 {
    if (@hasField(@TypeOf(table), "routing_storage_binding")) if (table.routing_storage_binding) |binding| return binding;
    const Output = struct {
        writer: std.Io.Writer = .{ .vtable = &.{ .drain = drain, .flush = std.Io.Writer.noopFlush }, .buffer = &.{} },
        sha: std.crypto.hash.sha2.Sha256 = .init(.{}),
        fn drain(writer: *std.Io.Writer, data: []const []const u8, splat: usize) std.Io.Writer.Error!usize {
            const self: *@This() = @alignCast(@fieldParentPtr("writer", writer));
            for (data[0 .. data.len - 1]) |bytes| self.sha.update(bytes);
            for (0..splat) |_| self.sha.update(data[data.len - 1]);
            return std.Io.Writer.countSplat(data, splat);
        }
    };
    var output: Output = .{};
    std.json.Stringify.value(.{ .domain = "antfly:changefeed-table-binding:v1", .table_id = table.table_id, .engine = table.storage.engine, .object_generation = table.object_storage_generation, .object_identity = table.object_storage_identity, .restore_backup_id = table.restore_backup_id, .restore_location = table.restore_location }, .{}, &output.writer) catch unreachable;
    return output.sha.finalResult();
}
