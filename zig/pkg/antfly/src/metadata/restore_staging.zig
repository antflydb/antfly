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

//! Shared document/relational restore reservations. Staging descriptors never inhabit ordinary
//! table/range namespaces. A fixed-size owner receipt proves completion under
//! the immutable target plan; publication is one metadata-store transaction.
const std = @import("std");
const records = @import("../common/topology_records.zig");
const tables = @import("table_manager.zig");
pub const Id = [16]u8;
/// The existing restore job/attempt owns this private metadata reservation.
/// There is no independent scheduler or user-visible restore job here.
pub fn idForAttempt(job_id: u64, attempt_id: u64) !Id {
    if (job_id == 0 or attempt_id == 0) return error.InvalidRestoreStaging;
    var id: Id = undefined;
    std.mem.writeInt(u64, id[0..8], job_id, .little);
    std.mem.writeInt(u64, id[8..16], attempt_id, .little);
    return id;
}
pub const Digest = [32]u8;
pub const max_encoded_bytes = 32 * 1024 * 1024;
pub const max_active_attempts = 8;
pub const ProvisioningProjection = struct {
    tables: []records.TableRecord,
    ranges: []records.RangeRecord,
    /// Immutable active plans accompany private descriptors; receivers never
    /// accept a hidden table on the authority of its name alone.
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
    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        for (self.tables) |table| tables.freeTable(alloc, table);
        alloc.free(self.tables);
        for (self.ranges) |range| tables.freeRange(alloc, range);
        alloc.free(self.ranges);
        for (self.jobs_json) |job| alloc.free(job);
        if (self.jobs_json.len != 0) alloc.free(self.jobs_json);
        self.* = undefined;
    }
};
pub const ProvisioningRequest = struct { node_id: u64 };

pub fn scopeProvisioningForNode(alloc: std.mem.Allocator, projection: ProvisioningProjection, node_id: u64, placements: []const @import("../raft/reconciler.zig").PlacementIntent) !ProvisioningProjection {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    var assigned: std.AutoHashMapUnmanaged(u64, void) = .empty;
    for (placements) |placement| if (placement.record.local_node_id == node_id) try assigned.put(a, placement.record.group_id, {});
    var hidden_groups: std.AutoHashMapUnmanaged(u64, void) = .empty;
    var owned_tables: std.AutoHashMapUnmanaged(u64, void) = .empty;
    var jobs = std.ArrayListUnmanaged([]const u8).empty;
    var selected_tables = std.ArrayListUnmanaged(records.TableRecord).empty;
    var selected_ranges = std.ArrayListUnmanaged(records.RangeRecord).empty;
    errdefer {
        for (jobs.items) |job| alloc.free(job);
        jobs.deinit(alloc);
        for (selected_tables.items) |table| tables.freeTable(alloc, table);
        selected_tables.deinit(alloc);
        for (selected_ranges.items) |range| tables.freeRange(alloc, range);
        selected_ranges.deinit(alloc);
    }
    if (projection.jobs_json.len > max_active_attempts) return error.InvalidRestoreStaging;
    for (projection.jobs_json) |bytes| {
        const parsed = try std.json.parseFromSlice(Job, a, bytes, .{});
        var owned = false;
        for (parsed.value.plan.targets) |target| {
            for (target.ranges) |range| {
                try hidden_groups.put(a, range.group_id, {});
                if (assigned.contains(range.group_id)) {
                    owned = true;
                    try owned_tables.put(a, target.table.table_id, {});
                }
            }
        }
        if (owned) {
            try jobs.ensureUnusedCapacity(alloc, 1);
            jobs.appendAssumeCapacity(try alloc.dupe(u8, bytes));
        }
    }
    for (projection.tables) |table| if (owned_tables.contains(table.table_id)) {
        try selected_tables.ensureUnusedCapacity(alloc, 1);
        selected_tables.appendAssumeCapacity(try tables.cloneTable(alloc, table));
    };
    for (projection.ranges) |range| if (hidden_groups.contains(range.group_id) and assigned.contains(range.group_id)) {
        try selected_ranges.ensureUnusedCapacity(alloc, 1);
        selected_ranges.appendAssumeCapacity(try tables.cloneRange(alloc, range));
    };
    const owned_table_slice = try selected_tables.toOwnedSlice(alloc);
    errdefer {
        for (owned_table_slice) |table| tables.freeTable(alloc, table);
        alloc.free(owned_table_slice);
    }
    const owned_range_slice = try selected_ranges.toOwnedSlice(alloc);
    errdefer {
        for (owned_range_slice) |range| tables.freeRange(alloc, range);
        alloc.free(owned_range_slice);
    }
    return .{ .tables = owned_table_slice, .ranges = owned_range_slice, .jobs_json = try jobs.toOwnedSlice(alloc) };
}
pub const ProvisioningSnapshot = struct {
    node_id: u64,
    metadata_group_id: u64,
    metadata_incarnation: @import("incarnation.zig").MetadataClusterIncarnation,
    metadata_epoch: u64,
    catalog: ProvisioningProjection,
    pub fn jsonStringify(self: ProvisioningSnapshot, stream: anytype) @TypeOf(stream.*).Error!void {
        try stream.beginObject();
        try stream.objectField("node_id");
        try stream.write(self.node_id);
        try stream.objectField("metadata_group_id");
        try stream.write(self.metadata_group_id);
        try stream.objectField("metadata_incarnation");
        try stream.write(self.metadata_incarnation);
        try stream.objectField("metadata_epoch");
        try stream.write(self.metadata_epoch);
        try stream.objectField("catalog");
        try stream.write(self.catalog);
        try stream.endObject();
    }
};
pub const State = enum { importing, validating, cutover, published, canceling, canceled };
pub const SourceArtifact = struct {
    target_group_id: u64,
    source_namespace: @import("../storage/db/doc_identity.zig").Namespace,
    format: enum { native, portable },
    snapshot_path: []const u8,
    artifact_size_bytes: u64,
    artifact_sha256: [32]u8,
    native_manifest_size_bytes: u64 = 0,
    native_manifest_sha256: []const u8 = "",
    cohort_seal: ?@import("../storage/db/native_backup_seal.zig").Handle = null,

    pub fn digest(self: SourceArtifact, alloc: std.mem.Allocator) !Digest {
        const encoded = try std.json.Stringify.valueAlloc(alloc, self, .{});
        defer alloc.free(encoded);
        var result: Digest = undefined;
        std.crypto.hash.Blake3.hash(encoded, &result, .{});
        return result;
    }
};
pub const Target = struct {
    source_table_id: u64,
    table: records.TableRecord,
    ranges: []const records.RangeRecord,
    /// Native owner reservations bind these authenticated source identities
    /// before accepting an import RPC. No full-plan transfer per row page.
    source_artifacts: []const SourceArtifact = &.{},
    /// Explicit overwrite pins the exact old generation, which remains live
    /// until all new targets validate and the old-owner cutover fence drains.
    replace: ?struct {
        table: records.TableRecord,
        ranges: []const records.RangeRecord,
        /// Planned before reservation, so a cancellation can tombstone even
        /// a cutover fence whose begin acknowledgement was lost.
        fences: []const @import("../storage/db/relational_integrity_topology.zig").Fence = &.{},
    } = null,
};
pub const Plan = struct {
    id: Id,
    /// Authenticated server-issued aggregate manifest identity, not an
    /// independent set of user-selected table snapshot timestamps.
    cohort_digest: Digest,
    source_location: []const u8 = "",
    source_connection: []const u8 = "",
    skipped_tables: []const []const u8 = &.{},
    targets: []const Target,

    pub fn jsonStringify(self: Plan, jw: anytype) @TypeOf(jw.*).Error!void {
        try @import("../storage/db/relational_integrity_json.zig").write(self, jw);
    }

    pub fn validate(self: Plan, alloc: std.mem.Allocator) !void {
        if (std.mem.allEqual(u8, &self.id, 0) or std.mem.allEqual(u8, &self.cohort_digest, 0) or
            self.targets.len == 0 or self.targets.len > 128) return error.InvalidRestoreStaging;
        if (self.skipped_tables.len > 128 or self.skipped_tables.len + self.targets.len > 128) return error.InvalidRestoreStaging;
        for (self.skipped_tables, 0..) |name, index| {
            if (name.len == 0 or name.len > 4096) return error.InvalidRestoreStaging;
            for (self.skipped_tables[0..index]) |previous| if (std.mem.eql(u8, previous, name)) return error.InvalidRestoreStaging;
            for (self.targets) |target| if (std.mem.eql(u8, target.table.name, name)) return error.InvalidRestoreStaging;
        }
        var range_count: usize = 0;
        var schema_bytes: usize = 0;
        var old_range_count: usize = 0;
        var group_ids: std.AutoHashMapUnmanaged(u64, void) = .empty;
        defer group_ids.deinit(alloc);
        // Receipt keys use physical owner identity. Old/new overlap or an
        // owner repeated across targets would let one receipt satisfy two.
        for (self.targets) |target| if (target.replace) |old| {
            for (old.ranges) |range| {
                try @import("../common/group_ids.zig").requireDataGroupId(range.group_id);
                const existing = try group_ids.getOrPut(alloc, range.group_id);
                if (existing.found_existing) return error.InvalidRestoreStaging;
            }
        };
        for (self.targets, 0..) |target, index| {
            const table = target.table;
            if (target.source_table_id == 0 or table.table_id == 0 or table.table_id == target.source_table_id or
                table.name.len == 0 or table.name.len > 255 or table.relational_retirement_json.len != 0 or
                target.ranges.len == 0 or target.ranges.len != table.min_ranges) return error.InvalidRestoreStaging;
            for (table.name) |byte| if (std.ascii.isControl(byte)) return error.InvalidRestoreStaging;
            for (self.targets[0..index]) |previous| {
                if (previous.source_table_id == target.source_table_id or previous.table.table_id == table.table_id or
                    std.mem.eql(u8, previous.table.name, table.name)) return error.InvalidRestoreStaging;
            }
            if (target.replace) |old| {
                if (old.table.table_id == 0 or old.table.table_id == table.table_id or !std.mem.eql(u8, old.table.name, table.name) or
                    old.table.relational_retirement_json.len != 0 or old.table.restore_backup_id.len != 0) return error.InvalidRestoreStaging;
                tables.validateCompleteKeyspaceRanges(old.ranges) catch return error.InvalidRestoreStaging;
                old_range_count += old.ranges.len;
                if (old_range_count > 4096 or old.ranges.len == 0) return error.InvalidRestoreStaging;
                for (old.ranges) |range| if (range.table_id != old.table.table_id or range.restore_backup_id.len != 0) return error.InvalidRestoreStaging;
                if (old.fences.len != 0) {
                    if (old.fences.len != old.ranges.len) return error.InvalidRestoreStaging;
                    for (old.fences, 0..) |fence, fence_index| {
                        _ = try fence.encode();
                        if (fence.namespace.table_id != old.table.table_id) return error.InvalidRestoreStaging;
                        for (old.fences[0..fence_index]) |previous| if (previous.owner_group_id == fence.owner_group_id) return error.InvalidRestoreStaging;
                        for (old.ranges) |range| {
                            if (range.group_id == fence.owner_group_id and tables.rangeDocIdentityShardId(range) == fence.namespace.shard_id and tables.rangeDocIdentityRangeId(range) == fence.namespace.range_id) break;
                        } else return error.InvalidRestoreStaging;
                    }
                }
                for (self.targets) |candidate| if (candidate.table.table_id == old.table.table_id) return error.InvalidRestoreStaging;
            }
            // New target identities must be disjoint from the whole source
            // cohort, not merely from their corresponding source table.
            for (self.targets) |source| if (source.source_table_id == table.table_id) return error.InvalidRestoreStaging;
            range_count = std.math.add(usize, range_count, target.ranges.len) catch return error.InvalidRestoreStaging;
            schema_bytes = std.math.add(usize, schema_bytes, table.schema_json.len +| table.read_schema_json.len +| table.indexes_json.len) catch return error.InvalidRestoreStaging;
            if (range_count > 4096 or schema_bytes > 4 * 1024 * 1024) return error.InvalidRestoreStaging;
            tables.validateCompleteKeyspaceRanges(target.ranges) catch return error.InvalidRestoreStaging;
            if (target.source_artifacts.len != 0) {
                if (target.source_artifacts.len != target.ranges.len) return error.InvalidRestoreStaging;
                for (target.source_artifacts, 0..) |artifact, artifact_index| {
                    if (artifact.source_namespace.table_id != target.source_table_id or artifact.source_namespace.shard_id == 0 or artifact.source_namespace.range_id == 0 or
                        artifact.snapshot_path.len == 0 or artifact.snapshot_path.len > 4096 or std.mem.allEqual(u8, &artifact.artifact_sha256, 0)) return error.InvalidRestoreStaging;
                    for (target.source_artifacts[0..artifact_index]) |previous| if (previous.target_group_id == artifact.target_group_id) return error.InvalidRestoreStaging;
                    for (target.ranges) |range| {
                        if (range.group_id == artifact.target_group_id) break;
                    } else return error.InvalidRestoreStaging;
                }
            }
            for (target.ranges) |range| {
                try @import("../common/group_ids.zig").requireDataGroupId(range.group_id);
                // Fresh restore owners use the canonical new-group namespace;
                // aliases imported from a source split must never survive.
                if (range.table_id != table.table_id or range.range_id != range.group_id or
                    range.doc_identity_shard_id != range.group_id or range.doc_identity_range_id != range.group_id) return error.InvalidRestoreStaging;
                const existing = try group_ids.getOrPut(alloc, range.group_id);
                if (existing.found_existing) return error.InvalidRestoreStaging;
            }
            try @import("../schema/restore_migration.zig").validate(alloc, table.schema_json, table.read_schema_json);
            for ([_][]const u8{ table.schema_json, table.read_schema_json }) |schema_json| {
                if (schema_json.len == 0) continue;
                var schema = try @import("../schema/mod.zig").parseValidatedTableSchema(alloc, schema_json);
                defer schema.deinit(alloc);
                if (schema.foreign_keys) |foreign_keys| for (foreign_keys.value) |fk| {
                    const parent = for (self.targets) |candidate| {
                        if (std.mem.eql(u8, candidate.table.name, fk.parent_table)) break candidate.table;
                    } else return error.RestoreDependencyMissing;
                    try @import("../schema/relational_foreign_key_target.zig").validate(alloc, schema_json, parent.name, parent.schema_json);
                };
            }
        }
    }

    pub fn digest(self: Plan, alloc: std.mem.Allocator) !Digest {
        const bytes = try std.json.Stringify.valueAlloc(alloc, self, .{});
        defer alloc.free(bytes);
        if (bytes.len > max_encoded_bytes) return error.InvalidRestoreStaging;
        var value: Digest = undefined;
        std.crypto.hash.Blake3.hash(bytes, &value, .{});
        return value;
    }
};

pub const Job = struct {
    plan: Plan,
    plan_digest: Digest,
    state: State = .importing,
    revision: u64 = 1,
    completed_owners: u32 = 0,

    pub fn jsonStringify(self: Job, jw: anytype) @TypeOf(jw.*).Error!void {
        try @import("../storage/db/relational_integrity_json.zig").write(self, jw);
    }
};

/// One canonical derivation shared by metadata provisioning and the restore
/// coordinator. Hash the encoded runtime schema, not the public JSON spelling.
pub fn ownerScope(alloc: std.mem.Allocator, plan: Plan, plan_digest: Digest, target: Target, range: records.RangeRecord) !@import("../storage/db/restore_staging.zig").Scope {
    if (range.table_id != target.table.table_id) return error.InvalidRestoreStaging;
    const artifact = for (target.source_artifacts) |source| {
        if (source.target_group_id == range.group_id) break source;
    } else return error.RestoreSourceProofMissing;
    const api_tables = @import("../api/tables.zig");
    const runtime_schema = @import("../storage/schema.zig");
    var schema = try api_tables.parseValidatedTableSchema(alloc, target.table.schema_json);
    defer schema.deinit(alloc);
    const typed = try api_tables.deriveRuntimeTableSchema(alloc, schema);
    defer runtime_schema.freeSchema(alloc, typed);
    const encoded = try runtime_schema.serializeSchema(alloc, typed);
    defer alloc.free(encoded);
    return .{
        .plan_id = plan.id,
        .plan_digest = plan_digest,
        .source_artifact_digest = artifact.artifact_sha256,
        .source_descriptor_digest = try artifact.digest(alloc),
        .source_namespace = artifact.source_namespace,
        .target_namespace = .{ .table_id = target.table.table_id, .shard_id = tables.rangeDocIdentityShardId(range), .range_id = tables.rangeDocIdentityRangeId(range) },
        .target_schema_digest = @import("../storage/db/restore_staging.zig").digest(encoded),
    };
}

/// Progress is separate from the immutable plan so per-owner acknowledgements
/// update only a small record, independent of schema/target count.
pub const Progress = struct { state: State = .importing, revision: u64 = 1, completed_owners: u32 = 0 };

pub fn progressKey(buf: []u8, metadata_group_id: u64, id: Id) ![]const u8 {
    return std.fmt.bufPrint(buf, "\x00\x00__metadata__:restore_staging:{d}:progress:{s}", .{ metadata_group_id, std.fmt.bytesToHex(id, .lower) });
}

pub const OwnerReceipt = struct {
    group_id: u64,
    range_id: u64,
    plan_digest: Digest,
    /// Opaque durable owner completion identity. Native import/constraint
    /// validation must commit this identity before metadata reports it.
    completion_digest: Digest,
};

pub const Command = struct {
    id: Id,
    expected_revision: u64 = 0,
    action: enum { reserve, cancel_reservation, imported, validated, begin_cutover, old_fenced, publish, begin_cancel, canceled, finish_cancel },
    plan: ?Plan = null,
    receipt: ?OwnerReceipt = null,

    pub fn jsonStringify(self: Command, jw: anytype) @TypeOf(jw.*).Error!void {
        try @import("../storage/db/relational_integrity_json.zig").write(self, jw);
    }

    pub fn validate(self: Command, alloc: std.mem.Allocator) !void {
        if (std.mem.allEqual(u8, &self.id, 0)) return error.InvalidRestoreStaging;
        if (self.action == .reserve) {
            const plan = self.plan orelse return error.InvalidRestoreStaging;
            if (self.expected_revision != 0 or self.receipt != null or !std.mem.eql(u8, &plan.id, &self.id)) return error.InvalidRestoreStaging;
            try plan.validate(alloc);
        } else if (self.action == .cancel_reservation) {
            if (self.plan != null or self.expected_revision != 0) return error.InvalidRestoreStaging;
        } else if (self.plan != null or self.expected_revision == 0) return error.InvalidRestoreStaging;
        const needs_receipt = self.action == .imported or self.action == .validated or self.action == .old_fenced or self.action == .canceled;
        if (needs_receipt != (self.receipt != null)) return error.InvalidRestoreStaging;
        if (self.receipt) |receipt| {
            if (receipt.group_id == 0 or receipt.range_id == 0 or std.mem.allEqual(u8, &receipt.plan_digest, 0) or
                std.mem.allEqual(u8, &receipt.completion_digest, 0)) return error.InvalidRestoreStaging;
        }
    }
};

pub fn parseCommand(alloc: std.mem.Allocator, bytes: []const u8) !std.json.Parsed(Command) {
    if (bytes.len == 0 or bytes.len > max_encoded_bytes) return error.InvalidRestoreStaging;
    var parsed = try std.json.parseFromSlice(Command, alloc, bytes, .{ .allocate = .alloc_always });
    errdefer parsed.deinit();
    try parsed.value.validate(alloc);
    return parsed;
}

pub fn jobKey(buf: []u8, metadata_group_id: u64, id: Id) ![]const u8 {
    return std.fmt.bufPrint(buf, "\x00\x00__metadata__:restore_staging:{d}:job:{s}", .{ metadata_group_id, std.fmt.bytesToHex(id, .lower) });
}

pub fn prefix(buf: []u8, metadata_group_id: u64) ![]const u8 {
    return std.fmt.bufPrint(buf, "\x00\x00__metadata__:restore_staging:{d}:", .{metadata_group_id});
}

pub fn activePrefix(buf: []u8, metadata_group_id: u64) ![]const u8 {
    return std.fmt.bufPrint(buf, "\x00\x00__metadata__:restore_staging:{d}:active:", .{metadata_group_id});
}

pub fn activeKey(buf: []u8, metadata_group_id: u64, id: Id) ![]const u8 {
    return std.fmt.bufPrint(buf, "\x00\x00__metadata__:restore_staging:{d}:active:{s}", .{ metadata_group_id, std.fmt.bytesToHex(id, .lower) });
}

pub fn nameKey(buf: []u8, metadata_group_id: u64, name: []const u8) ![]const u8 {
    return std.fmt.bufPrint(buf, "\x00\x00__metadata__:restore_staging:{d}:name:{s}", .{ metadata_group_id, name });
}

pub fn identityKey(buf: []u8, metadata_group_id: u64, kind: enum { table, group, old_table }, id: u64) ![]const u8 {
    return std.fmt.bufPrint(buf, "\x00\x00__metadata__:restore_staging:{d}:{s}:{d}", .{ metadata_group_id, @tagName(kind), id });
}

pub fn receiptKey(buf: []u8, metadata_group_id: u64, id: Id, state: State, owner: u64) ![]const u8 {
    return std.fmt.bufPrint(buf, "\x00\x00__metadata__:restore_staging:{d}:receipt:{s}:{s}:{d}", .{ metadata_group_id, std.fmt.bytesToHex(id, .lower), @tagName(state), owner });
}

test "relational integrity restore staging shares one plan across document and typed tables" {
    const parent_schema =
        \\{"version":1,"storage_mode":"relational","default_type":"row","unique_constraints":[{"name":"id_unique","columns":["id"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}}}
    ;
    const child_schema =
        \\{"version":1,"storage_mode":"relational","default_type":"row","foreign_keys":[{"name":"parent_fk","child_columns":["parent_id"],"parent_table":"restored_parent","parent_columns":["id"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"parent_id":{"type":"integer"}},"additionalProperties":false}}}}
    ;
    const targets = [_]Target{
        .{ .source_table_id = 1, .table = .{ .table_id = 11, .name = "restored_documents", .schema_json = "{}" }, .ranges = &.{.{ .table_id = 11, .group_id = 701, .range_id = 701, .doc_identity_shard_id = 701, .doc_identity_range_id = 701, .start_key = "" }} },
        .{ .source_table_id = 2, .table = .{ .table_id = 12, .name = "restored_parent", .schema_json = parent_schema }, .ranges = &.{.{ .table_id = 12, .group_id = 702, .range_id = 702, .doc_identity_shard_id = 702, .doc_identity_range_id = 702, .start_key = "" }} },
        .{ .source_table_id = 3, .table = .{ .table_id = 13, .name = "restored_child", .schema_json = child_schema }, .ranges = &.{.{ .table_id = 13, .group_id = 703, .range_id = 703, .doc_identity_shard_id = 703, .doc_identity_range_id = 703, .start_key = "" }} },
    };
    const plan: Plan = .{ .id = @splat(7), .cohort_digest = @splat(9), .targets = &targets };
    try plan.validate(std.testing.allocator);
    const command: Command = .{ .id = plan.id, .action = .reserve, .plan = plan };
    const bytes = try std.json.Stringify.valueAlloc(std.testing.allocator, command, .{});
    defer std.testing.allocator.free(bytes);
    var decoded = try parseCommand(std.testing.allocator, bytes);
    defer decoded.deinit();
    try std.testing.expectEqual(@as(usize, 3), decoded.value.plan.?.targets.len);
    const missing_parent = [_]Target{ targets[0], targets[2] };
    var incomplete = plan;
    incomplete.targets = &missing_parent;
    try std.testing.expectError(error.RestoreDependencyMissing, incomplete.validate(std.testing.allocator));
}
