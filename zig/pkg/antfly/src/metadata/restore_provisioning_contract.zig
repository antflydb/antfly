// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Owned restore provisioning DTO shared across runtime archives and seed
//! artifacts. No metadata service or physical storage implementation belongs
//! in this contract.
const std = @import("std");
const records = @import("../common/topology_records.zig");

pub const SourceArtifact = struct {
    target_group_id: u64,
    source_namespace: @import("../storage/db/doc_identity_namespace.zig").Namespace,
    format: enum { native, portable },
    snapshot_path: []const u8,
    artifact_size_bytes: u64,
    artifact_sha256: [32]u8,
    native_manifest_size_bytes: u64 = 0,
    native_manifest_sha256: []const u8 = "",
    cohort_seal: ?@import("../storage/db/native_backup_seal_contract.zig").Handle = null,
    rewrite: ?@import("../storage/db/relational_rewrite_contract.zig").Binding = null,

    pub fn digest(self: SourceArtifact, alloc: std.mem.Allocator) ![32]u8 {
        const encoded = try std.json.Stringify.valueAlloc(alloc, self, .{});
        defer alloc.free(encoded);
        var result: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(encoded, &result, .{});
        return result;
    }
};

pub const ProvisioningProjection = struct {
    tables: []records.TableRecord,
    ranges: []records.RangeRecord,
    /// Immutable plans prove the authority for each hidden owner.
    jobs_json: []const []const u8 = &.{},

    pub fn jsonStringify(self: ProvisioningProjection, stream: anytype) @TypeOf(stream.*).Error!void {
        try stream.beginObject();
        try stream.objectField("tables");
        try stream.write(self.tables);
        try stream.objectField("ranges");
        try @import("../storage/db/relational_integrity_json.zig").write(self.ranges, stream);
        try stream.objectField("jobs_json");
        try stream.write(self.jobs_json);
        try stream.endObject();
    }

    /// Values here are individually owned, unlike JSON Parsed projections,
    /// whose arena is released by Parsed.deinit instead.
    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        for (self.tables) |table| freeTable(alloc, table);
        alloc.free(self.tables);
        for (self.ranges) |range| freeRange(alloc, range);
        alloc.free(self.ranges);
        for (self.jobs_json) |job| alloc.free(job);
        if (self.jobs_json.len != 0) alloc.free(self.jobs_json);
        self.* = undefined;
    }
};

pub fn freeTable(alloc: std.mem.Allocator, record: records.TableRecord) void {
    alloc.free(record.relational_retirement_json);
    alloc.free(record.name);
    alloc.free(record.description);
    alloc.free(record.schema_json);
    alloc.free(record.read_schema_json);
    alloc.free(record.indexes_json);
    alloc.free(record.replication_sources_json);
    alloc.free(record.placement_role);
    alloc.free(record.restore_backup_id);
    alloc.free(record.restore_location);
}

pub fn freeRange(alloc: std.mem.Allocator, record: records.RangeRecord) void {
    alloc.free(record.start_key);
    if (record.end_key) |key| alloc.free(key);
    alloc.free(record.restore_backup_id);
    alloc.free(record.restore_artifact_backup_id);
    alloc.free(record.restore_location);
    alloc.free(record.restore_snapshot_path);
    alloc.free(record.restore_connection);
    alloc.free(record.restore_artifact_sha256);
    alloc.free(record.restore_native_manifest_sha256);
}
