// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

//! Owned restore provisioning DTO shared across runtime archives and seed
//! artifacts. No metadata service or physical storage implementation belongs
//! in this contract.
const std = @import("std");
const records = @import("antfly_local_sources").common_topology_records;

pub const SourceArtifact = struct {
    target_group_id: u64,
    source_namespace: @import("antfly_local_sources").storage_db_doc_identity_namespace.Namespace,
    format: enum { native, portable },
    snapshot_path: []const u8,
    artifact_size_bytes: u64,
    artifact_sha256: [32]u8,
    native_manifest_size_bytes: u64 = 0,
    native_manifest_sha256: []const u8 = "",
    cohort_seal: ?@import("antfly_local_sources").storage_db_native_backup_seal_contract.Handle = null,
    rewrite: ?@import("antfly_local_sources").storage_db_relational_rewrite_contract.Binding = null,

    pub fn digest(self: SourceArtifact, alloc: std.mem.Allocator) ![32]u8 {
        const encoded = try std.json.Stringify.valueAlloc(alloc, self, .{});
        defer alloc.free(encoded);
        var result: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(encoded, &result, .{});
        return result;
    }
};

pub const ProvisioningProjection = struct {
    pub const InitialFkOwner = struct {
        plan_id: [16]u8,
        plan_digest: [32]u8,
        child_table_id: u64,
        child_group_id: u64,
        namespace: @import("antfly_local_sources").storage_db_doc_identity_namespace.Namespace,
        schema_version: u32,
        schema_digest: [32]u8,
        public_schema_json_digest: [32]u8,
        catalog_digest: [32]u8,
    };
    tables: []records.TableRecord,
    ranges: []records.RangeRecord,
    /// Immutable plans prove the authority for each hidden owner.
    jobs_json: []const []const u8 = &.{},
    /// Plan-bound owner admission marker. A projected hidden child is never
    /// created as an ordinary writable group before this gate is installed.
    initial_fk_owners: []const InitialFkOwner = &.{},

    pub fn jsonStringify(self: ProvisioningProjection, stream: anytype) @TypeOf(stream.*).Error!void {
        try stream.beginObject();
        try stream.objectField("tables");
        try stream.write(self.tables);
        try stream.objectField("ranges");
        try @import("antfly_local_sources").storage_db_relational_integrity_json.write(self.ranges, stream);
        try stream.objectField("jobs_json");
        try stream.write(self.jobs_json);
        try stream.objectField("initial_fk_owners");
        try stream.write(self.initial_fk_owners);
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
        if (self.initial_fk_owners.len != 0) alloc.free(self.initial_fk_owners);
        self.* = undefined;
    }
};

pub const freeTable = @import("antfly_local_sources").metadata_record_memory.freeTable;
pub const freeRange = @import("antfly_local_sources").metadata_record_memory.freeRange;
