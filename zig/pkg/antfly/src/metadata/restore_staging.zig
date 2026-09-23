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
pub const ProvisioningProjection = @import("restore_provisioning_contract.zig").ProvisioningProjection;
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
pub const State = enum { importing, validating, cutover, published, canceling, canceled, preparing_sources };
pub const SourceArtifact = @import("restore_provisioning_contract.zig").SourceArtifact;
pub const Target = struct {
    pub fn nativeJsonSkipField(self: @This(), comptime name: []const u8) bool {
        return std.mem.eql(u8, name, "empty_generation") and !self.empty_generation;
    }
    source_table_id: u64,
    empty_generation: bool = false,
    table: records.TableRecord,
    /// Logical publication is part of the same transaction as the new owner
    /// generation. The namespace ID is pinned, not reinterpreted by name.
    catalog_binding: ?@import("../system_catalog/domain.zig").Resource = null,
    ranges: []const records.RangeRecord,
    /// Native owner reservations bind these authenticated source identities
    /// before accepting an import RPC. No full-plan transfer per row page.
    source_artifacts: []const SourceArtifact = &.{},
    /// Explicitly distinct from restoration of the authenticated source
    /// definition. It must be serviced by the snapshot+retained-tail rewrite
    /// driver, never by the preservation-only backup restore worker.
    rewrite: ?@import("../storage/db/relational_rewrite_contract.zig").Intent = null,
    /// Allocated before any source pin is admitted. Source artifact receipts
    /// may fill cuts/checksums later, but never choose a different owner scope.
    rewrite_sources: []const @import("../storage/db/online_source_contract.zig").Scope = &.{},
    /// Explicit overwrite pins the exact old generation, which remains live
    /// until all new targets validate and the old-owner cutover fence drains.
    replace: ?struct {
        table: records.TableRecord,
        ranges: []const records.RangeRecord,
        /// Planned before reservation, so a cancellation can tombstone even
        /// a cutover fence whose begin acknowledgement was lost.
        fences: []const @import("../storage/db/relational_integrity_topology_contract.zig").Fence = &.{},
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
    preparing_sources: bool = false,

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
            if (target.empty_generation) {
                const old = target.replace orelse return error.InvalidRestoreStaging;
                if (self.preparing_sources or target.rewrite != null or target.rewrite_sources.len != 0 or target.source_artifacts.len != 0 or old.table.table_id != target.source_table_id or old.fences.len != old.ranges.len or old.ranges.len != target.ranges.len) return error.InvalidRestoreStaging;
                if (!std.mem.eql(u8, old.table.schema_json, table.schema_json) or !std.mem.eql(u8, old.table.read_schema_json, table.read_schema_json) or !std.mem.eql(u8, old.table.indexes_json, table.indexes_json)) return error.InvalidRestoreStaging;
                for (old.ranges, target.ranges) |source, destination| {
                    if (!std.mem.eql(u8, source.start_key, destination.start_key) or !std.mem.eql(u8, source.end_key orelse "", destination.end_key orelse "")) return error.InvalidRestoreStaging;
                    const fence = for (old.fences) |item| {
                        if (item.owner_group_id == source.group_id) break item;
                    } else return error.InvalidRestoreStaging;
                    if (fence.role != .rewrite_source or fence.peer_group_id != destination.group_id or fence.transition_id != std.mem.readInt(u64, self.id[0..8], .little) or fence.attempt != std.mem.readInt(u64, self.id[8..16], .little)) return error.InvalidRestoreStaging;
                }
            }
            if (target.catalog_binding) |binding| {
                if (binding.kind != .table or binding.id != table.table_id or binding.parent_id == 0 or
                    !std.mem.eql(u8, binding.storage_name, table.name)) return error.InvalidRestoreStaging;
                try @import("../system_catalog/domain.zig").validateTableName(binding.name);
                for (self.targets[0..index]) |previous| if (previous.catalog_binding) |other| {
                    if (binding.parent_id == other.parent_id and std.mem.eql(u8, binding.name, other.name)) return error.InvalidRestoreStaging;
                };
            }
            if (target.rewrite) |rewrite| {
                try rewrite.validate();
                if (!std.mem.eql(u8, rewrite.target_schema, table.schema_json) or !std.mem.eql(u8, rewrite.target_read_schema, table.read_schema_json) or target.replace == null or
                    (if (self.preparing_sources) target.source_artifacts.len > target.ranges.len else target.source_artifacts.len != target.ranges.len))
                    return error.InvalidRestoreStaging;
                const original = target.replace.?.table;
                if (original.table_id != target.source_table_id) return error.InvalidRestoreStaging;
                if (rewrite.preserve_document and (!std.mem.eql(u8, @import("../api/tables.zig").effectiveSchemaJson(original.schema_json), @import("../api/tables.zig").effectiveSchemaJson(table.schema_json)) or !std.mem.eql(u8, original.read_schema_json, table.read_schema_json))) return error.InvalidRestoreStaging;
                // The authenticated live source definitions are retained, not
                // replaced by target JSON under the old cohort identity.
                for ([_][]const u8{ original.schema_json, original.read_schema_json }) |required| {
                    if (required.len == 0) continue;
                    for (rewrite.source_schemas) |source| {
                        if (std.mem.eql(u8, source, required)) break;
                    } else return error.InvalidRestoreStaging;
                }
                for (rewrite.source_schemas) |source| {
                    schema_bytes = std.math.add(usize, schema_bytes, source.len) catch return error.InvalidRestoreStaging;
                    if (schema_bytes > 4 * 1024 * 1024) return error.InvalidRestoreStaging;
                    var source_schema = try @import("../schema/mod.zig").parseValidatedTableSchema(alloc, source);
                    defer source_schema.deinit(alloc);
                    const required_mode: @TypeOf(source_schema.storage_mode) = if (rewrite.preserve_document) .document else .relational;
                    if (source_schema.storage_mode != required_mode) return error.InvalidRestoreStaging;
                }
                if (self.preparing_sources or target.rewrite_sources.len != 0) {
                    if (target.rewrite_sources.len != target.ranges.len or target.replace.?.ranges.len != target.ranges.len or target.replace.?.fences.len != target.ranges.len) return error.InvalidRestoreStaging;
                    for (target.rewrite_sources, target.ranges, target.replace.?.ranges) |scope, destination, original_range| {
                        try scope.validate();
                        for (target.replace.?.fences) |fence| {
                            if (fence.eql(scope.fence)) break;
                        } else return error.InvalidRestoreStaging;
                        if (scope.fence.role != .rewrite_source or scope.fence.namespace.table_id != target.source_table_id or
                            scope.fence.owner_group_id != original_range.group_id or scope.fence.namespace.shard_id != tables.rangeDocIdentityShardId(original_range) or
                            scope.fence.namespace.range_id != tables.rangeDocIdentityRangeId(original_range) or scope.fence.peer_group_id != destination.group_id or
                            scope.receiver_namespace.table_id != table.table_id or scope.receiver_namespace.shard_id != destination.group_id or scope.receiver_namespace.range_id != destination.group_id or
                            scope.fence.transition_id != std.mem.readInt(u64, self.id[0..8], .little) or scope.fence.attempt != std.mem.readInt(u64, self.id[8..16], .little) or
                            !std.mem.eql(u8, original_range.start_key, destination.start_key) or !std.mem.eql(u8, original_range.end_key orelse "", destination.end_key orelse "")) return error.InvalidRestoreStaging;
                    }
                }
            } else if (self.preparing_sources or target.rewrite_sources.len != 0) {
                return error.InvalidRestoreStaging;
            }
            if (target.source_table_id == 0 or table.table_id == 0 or table.table_id == target.source_table_id or
                table.name.len == 0 or (table.name.len > 255 and !(try @import("../system_catalog/domain.zig").isRestoreTarget(table.name))) or table.relational_retirement_json.len != 0 or
                target.ranges.len == 0 or target.ranges.len != table.min_ranges) return error.InvalidRestoreStaging;
            for (table.name) |byte| if (std.ascii.isControl(byte)) return error.InvalidRestoreStaging;
            for (self.targets[0..index]) |previous| {
                if (previous.source_table_id == target.source_table_id or previous.table.table_id == table.table_id or
                    std.mem.eql(u8, previous.table.name, table.name)) return error.InvalidRestoreStaging;
            }
            if (target.replace) |old| {
                if (old.table.table_id == 0 or old.table.table_id == table.table_id or (target.catalog_binding == null and !std.mem.eql(u8, old.table.name, table.name)) or
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
                if (!self.preparing_sources and target.source_artifacts.len != target.ranges.len) return error.InvalidRestoreStaging;
                for (target.source_artifacts, 0..) |artifact, artifact_index| {
                    if (target.rewrite) |rewrite| {
                        if (artifact.format != .portable or artifact.cohort_seal != null) return error.InvalidRestoreStaging;
                        const binding = artifact.rewrite orelse return error.InvalidRestoreStaging;
                        try binding.validate();
                        const source_scope = binding.source_scope orelse return error.InvalidRestoreStaging;
                        if (!source_scope.fence.namespace.eql(artifact.source_namespace) or source_scope.receiver_namespace.table_id != table.table_id or source_scope.receiver_namespace.shard_id != artifact.target_group_id or source_scope.receiver_namespace.range_id != artifact.target_group_id)
                            return error.InvalidRestoreStaging;
                        if (source_scope.fence.transition_id != std.mem.readInt(u64, self.id[0..8], .little) or
                            source_scope.fence.attempt != std.mem.readInt(u64, self.id[8..16], .little)) return error.InvalidRestoreStaging;
                        if (!std.mem.eql(u8, &rewrite.program_digest, &binding.program_digest)) return error.InvalidRestoreStaging;
                        if (target.rewrite_sources.len != 0) {
                            for (target.rewrite_sources) |expected| {
                                if (std.mem.eql(u8, &expected.pin(), &source_scope.pin())) break;
                            } else return error.InvalidRestoreStaging;
                        }
                    } else if (artifact.rewrite != null) return error.InvalidRestoreStaging;
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
                if (target.rewrite) |rewrite| {
                    const required_mode: @TypeOf(schema.storage_mode) = if (rewrite.preserve_document) .document else .relational;
                    if (schema.storage_mode != required_mode) return error.InvalidRestoreStaging;
                }
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

    /// Identity before source pin acquisition. Only authenticated artifact
    /// receipts and their aggregate digest may change when freezing the plan.
    pub fn rewriteIntentDigest(self: Plan, alloc: std.mem.Allocator) !Digest {
        const targets = try alloc.dupe(Target, self.targets);
        defer alloc.free(targets);
        for (targets) |*target| {
            if (target.rewrite == null or target.rewrite_sources.len != target.ranges.len) return error.InvalidRestoreStaging;
            target.source_artifacts = &.{};
        }
        var draft = self;
        draft.targets = targets;
        draft.preparing_sources = true;
        draft.cohort_digest = @splat(0);
        return draft.digest(alloc);
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
pub fn ownerScope(alloc: std.mem.Allocator, plan: Plan, plan_digest: Digest, target: Target, range: records.RangeRecord) !@import("../storage/db/restore_staging_contract.zig").Scope {
    if (range.table_id != target.table.table_id) return error.InvalidRestoreStaging;
    const api_tables = @import("../api/tables.zig");
    const runtime_schema = @import("../storage/schema.zig");
    var schema = try api_tables.parseValidatedTableSchema(alloc, target.table.schema_json);
    defer schema.deinit(alloc);
    const typed = try api_tables.deriveRuntimeTableSchema(alloc, schema);
    defer runtime_schema.freeSchema(alloc, typed);
    const encoded = try runtime_schema.serializeSchema(alloc, typed);
    defer alloc.free(encoded);
    if (target.empty_generation) {
        const old = target.replace orelse return error.InvalidRestoreStaging;
        const source = for (old.ranges) |item| {
            if (std.mem.eql(u8, item.start_key, range.start_key) and std.mem.eql(u8, item.end_key orelse "", range.end_key orelse "")) break item;
        } else return error.InvalidRestoreStaging;
        return .{
            .plan_id = plan.id,
            .plan_digest = plan_digest,
            .source_artifact_digest = @splat(0),
            .source_namespace = .{ .table_id = old.table.table_id, .shard_id = tables.rangeDocIdentityShardId(source), .range_id = tables.rangeDocIdentityRangeId(source) },
            .target_namespace = .{ .table_id = target.table.table_id, .shard_id = tables.rangeDocIdentityShardId(range), .range_id = tables.rangeDocIdentityRangeId(range) },
            .target_schema_digest = @import("../storage/db/restore_staging_contract.zig").digest(encoded),
            .empty_generation = true,
        };
    }
    const artifact = for (target.source_artifacts) |source| {
        if (source.target_group_id == range.group_id) break source;
    } else return error.RestoreSourceProofMissing;
    return .{
        .plan_id = plan.id,
        .plan_digest = plan_digest,
        .source_artifact_digest = artifact.artifact_sha256,
        .source_descriptor_digest = try artifact.digest(alloc),
        .source_namespace = artifact.source_namespace,
        .target_namespace = .{ .table_id = target.table.table_id, .shard_id = tables.rangeDocIdentityShardId(range), .range_id = tables.rangeDocIdentityRangeId(range) },
        .target_schema_digest = @import("../storage/db/restore_staging_contract.zig").digest(encoded),
        .preserve_artifacts = artifact.rewrite == null,
        .rewrite = artifact.rewrite,
    };
}

/// Progress is separate from the immutable plan so per-owner acknowledgements
/// update only a small record, independent of schema/target count.
pub const Progress = struct { state: State = .importing, revision: u64 = 1, completed_owners: u32 = 0 };

/// Private node-scoped authority reads are separate from the active-only
/// provisioning projection: terminal publication/cancellation still needs an
/// authoritative answer after its hidden placement has disappeared.
pub const AuthorityRequest = struct {
    node_id: u64,
    plan_id: Id,
    include_plan: bool = false,
    receipt: ?struct { state: State, owner_group: u64 } = null,

    pub fn jsonStringify(self: AuthorityRequest, stream: anytype) @TypeOf(stream.*).Error!void {
        try writeAuthority(self, stream);
    }

    pub fn validate(self: AuthorityRequest) !void {
        if (self.node_id == 0 or std.mem.allEqual(u8, &self.plan_id, 0)) return error.InvalidArgument;
        if (self.receipt) |receipt| {
            if (receipt.owner_group == 0) return error.InvalidArgument;
            switch (receipt.state) {
                .importing, .validating, .cutover, .canceling => {},
                .published, .canceled, .preparing_sources => return error.InvalidArgument,
            }
        }
    }
};

pub const AuthorityResponse = struct {
    node_id: u64,
    plan_id: Id,
    metadata_group_id: u64,
    metadata_incarnation: @import("incarnation.zig").MetadataClusterIncarnation,
    metadata_epoch: u64,
    progress: ?Progress,
    job_json: ?[]const u8 = null,
    receipt: ?Digest = null,

    pub fn jsonStringify(self: AuthorityResponse, stream: anytype) @TypeOf(stream.*).Error!void {
        try writeAuthority(self, stream);
    }

    pub fn deinit(self: *AuthorityResponse, alloc: std.mem.Allocator) void {
        if (self.job_json) |json| alloc.free(json);
        self.* = undefined;
    }

    pub fn validate(self: AuthorityResponse, request: AuthorityRequest) !void {
        try request.validate();
        if (self.node_id != request.node_id or !std.mem.eql(u8, &self.plan_id, &request.plan_id) or self.metadata_group_id == 0 or !@import("incarnation.zig").isValid(self.metadata_incarnation)) return error.InvalidRestoreStaging;
        if (self.progress) |progress| {
            if (progress.revision == 0) return error.InvalidRestoreStaging;
        } else if (self.job_json != null or self.receipt != null) return error.InvalidRestoreStaging;
        if ((!request.include_plan and self.job_json != null) or (request.receipt == null and self.receipt != null)) return error.InvalidRestoreStaging;
        if (self.job_json) |json| if (json.len == 0 or json.len > max_encoded_bytes) return error.InvalidRestoreStaging;
    }
};

fn writeAuthority(value: anytype, stream: anytype) @TypeOf(stream.*).Error!void {
    try stream.beginObject();
    inline for (@typeInfo(@TypeOf(value)).@"struct".fields) |field| {
        try stream.objectField(field.name);
        if (comptime std.mem.eql(u8, field.name, "plan_id") or std.mem.eql(u8, field.name, "receipt")) {
            try @import("../storage/db/relational_integrity_json.zig").write(@field(value, field.name), stream);
        } else try stream.write(@field(value, field.name));
    }
    try stream.endObject();
}

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
    action: enum { reserve, cancel_reservation, imported, validated, begin_cutover, old_fenced, publish, begin_cancel, canceled, finish_cancel, freeze_rewrite, rewrite_source_ready },
    plan: ?Plan = null,
    receipt: ?OwnerReceipt = null,
    source_artifact: ?SourceArtifact = null,

    pub fn jsonStringify(self: Command, jw: anytype) @TypeOf(jw.*).Error!void {
        try @import("../storage/db/relational_integrity_json.zig").write(self, jw);
    }

    pub fn validate(self: Command, alloc: std.mem.Allocator) !void {
        if (std.mem.allEqual(u8, &self.id, 0)) return error.InvalidRestoreStaging;
        if ((self.action == .rewrite_source_ready) != (self.source_artifact != null)) return error.InvalidRestoreStaging;
        if (self.action == .reserve) {
            const plan = self.plan orelse return error.InvalidRestoreStaging;
            if (self.expected_revision != 0 or self.receipt != null or !std.mem.eql(u8, &plan.id, &self.id)) return error.InvalidRestoreStaging;
            try plan.validate(alloc);
            for (plan.targets) |target| if (target.rewrite != null and (!plan.preparing_sources or target.source_artifacts.len != 0)) return error.InvalidRestoreStaging;
        } else if (self.action == .freeze_rewrite) {
            const plan = self.plan orelse return error.InvalidRestoreStaging;
            if (self.expected_revision == 0 or self.receipt != null or plan.preparing_sources or !std.mem.eql(u8, &plan.id, &self.id)) return error.InvalidRestoreStaging;
            try plan.validate(alloc);
            _ = try plan.rewriteIntentDigest(alloc);
        } else if (self.action == .cancel_reservation) {
            if (self.plan != null or self.expected_revision != 0) return error.InvalidRestoreStaging;
        } else if (self.plan != null or self.expected_revision == 0) return error.InvalidRestoreStaging;
        if (self.source_artifact) |artifact| {
            const binding = artifact.rewrite orelse return error.InvalidRestoreStaging;
            try binding.validate();
            const scope = binding.source_scope orelse return error.InvalidRestoreStaging;
            if (artifact.format != .portable or artifact.cohort_seal != null or artifact.artifact_size_bytes == 0 or
                !std.mem.eql(u8, &artifact.artifact_sha256, &binding.snapshot_certificate) or artifact.snapshot_path.len == 0 or artifact.snapshot_path.len > 4096 or
                !artifact.source_namespace.eql(scope.fence.namespace) or artifact.target_group_id != scope.fence.peer_group_id or
                scope.fence.transition_id != std.mem.readInt(u64, self.id[0..8], .little) or scope.fence.attempt != std.mem.readInt(u64, self.id[8..16], .little)) return error.InvalidRestoreStaging;
        }
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

/// Bounded per-source publication receipt. It shares the existing job's
/// snapshot/tombstone lifetime; the immutable draft is never rewritten for
/// each owner, and compact Progress never parses these artifact descriptors.
pub fn sourceArtifactKey(buf: []u8, metadata_group_id: u64, id: Id, target_group: u64) ![]const u8 {
    return std.fmt.bufPrint(buf, "\x00\x00__metadata__:restore_staging:{d}:source:{s}:{d}", .{ metadata_group_id, std.fmt.bytesToHex(id, .lower), target_group });
}

/// One point-read index per plan/node; the value is a group whose existing
/// permanent placement-version fence proves this node really owned the plan.
/// This shares the plan's snapshot/tombstone lifetime, not a separate ledger.
pub fn authorityNodeKey(buf: []u8, metadata_group_id: u64, id: Id, node_id: u64) ![]const u8 {
    return std.fmt.bufPrint(buf, "\x00\x00__metadata__:restore_staging:{d}:authority:{s}:{d}", .{ metadata_group_id, std.fmt.bytesToHex(id, .lower), node_id });
}

test "relational integrity restore staging empty generation binds old fences without source artifacts" {
    const alloc = std.testing.allocator;
    const id = try idForAttempt(7, 1);
    const old: records.TableRecord = .{ .table_id = 9, .name = "docs", .schema_json = "{}" };
    const old_range: records.RangeRecord = .{ .table_id = 9, .group_id = 301, .start_key = "" };
    const fence: @import("../storage/db/relational_integrity_topology_contract.zig").Fence = .{ .role = .rewrite_source, .transition_id = 7, .attempt = 1, .owner_group_id = 301, .peer_group_id = 401, .namespace = .{ .table_id = 9, .shard_id = 301, .range_id = 301 }, .catalog_digest = @splat(4) };
    var targets = [_]Target{.{ .source_table_id = 9, .empty_generation = true, .table = .{ .table_id = 10, .name = "docs", .schema_json = "{}" }, .ranges = &.{.{ .table_id = 10, .group_id = 401, .range_id = 401, .doc_identity_shard_id = 401, .doc_identity_range_id = 401, .start_key = "" }}, .replace = .{ .table = old, .ranges = &.{old_range}, .fences = &.{fence} } }};
    const plan: Plan = .{ .id = id, .cohort_digest = @splat(3), .targets = &targets };
    try plan.validate(alloc);
    const scope = try ownerScope(alloc, plan, try plan.digest(alloc), targets[0], targets[0].ranges[0]);
    try scope.validate();
    try std.testing.expect(scope.empty_generation);
    try std.testing.expect(!scope.preserve_artifacts);
    try std.testing.expect(std.mem.allEqual(u8, &scope.source_artifact_digest, 0));
    try std.testing.expectEqual(@as(u64, 9), scope.source_namespace.table_id);
    targets[0].replace.?.fences = &.{};
    try std.testing.expectError(error.InvalidRestoreStaging, plan.validate(alloc));
    targets[0].replace.?.fences = &.{fence};
    targets[0].table.schema_json = "{\"version\":2}";
    try std.testing.expectError(error.InvalidRestoreStaging, plan.validate(alloc));
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

test "relational integrity restore staging rewrite intent binds original schema source pin and immutable target" {
    const alloc = std.testing.allocator;
    const old_schema =
        \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"x":{"type":"integer"}},"additionalProperties":false}}}}
    ;
    const new_schema =
        \\{"version":2,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"x":{"type":"integer"}},"additionalProperties":false}}}}
    ;
    const id = try idForAttempt(7, 1);
    const source: @import("../storage/db/online_source_contract.zig").Scope = .{
        .fence = .{ .role = .rewrite_source, .transition_id = 7, .attempt = 1, .owner_group_id = 301, .peer_group_id = 401, .namespace = .{ .table_id = 9, .shard_id = 301, .range_id = 301 }, .catalog_digest = @splat(4) },
        .receiver_namespace = .{ .table_id = 10, .shard_id = 401, .range_id = 401 },
        .consumer_epoch = 1,
        .copy_attempt = .{ .donor_term = 1, .sequence = 1 },
    };
    var artifacts = [_]SourceArtifact{.{ .target_group_id = 401, .source_namespace = source.fence.namespace, .format = .portable, .snapshot_path = "cut/source.afb2", .artifact_size_bytes = 100, .artifact_sha256 = @splat(5), .rewrite = .{ .program_digest = @splat(6), .retained_pin = source.pin(), .snapshot_certificate = @splat(7), .retained_epoch = 1, .retained_start = 8, .source_applied_index = 20, .source_scope = source } }};
    var targets = [_]Target{.{ .source_table_id = 9, .table = .{ .table_id = 10, .name = "rows", .schema_json = new_schema }, .ranges = &.{.{ .table_id = 10, .group_id = 401, .range_id = 401, .doc_identity_shard_id = 401, .doc_identity_range_id = 401, .start_key = "" }}, .source_artifacts = &artifacts, .replace = .{ .table = .{ .table_id = 9, .name = "rows", .schema_json = old_schema }, .ranges = &.{.{ .table_id = 9, .group_id = 301, .start_key = "" }} }, .rewrite = .{ .source_schemas = &.{old_schema}, .target_schema = new_schema, .program_digest = @splat(6) } }};
    const plan: Plan = .{ .id = id, .cohort_digest = @splat(7), .targets = &targets };
    try plan.validate(alloc);
    const original_digest = try plan.digest(alloc);
    const scope = try ownerScope(alloc, plan, original_digest, targets[0], targets[0].ranges[0]);
    try std.testing.expectEqualSlices(u8, &source.pin(), &scope.rewrite.?.retained_pin);
    targets[0].rewrite.?.apply_defaults_to_absent = true;
    try std.testing.expect(!std.mem.eql(u8, &original_digest, &try plan.digest(alloc)));
    targets[0].rewrite.?.apply_defaults_to_absent = false;
    targets[0].rewrite.?.source_schemas = &.{new_schema};
    try std.testing.expectError(error.InvalidRestoreStaging, plan.validate(alloc));
    targets[0].rewrite.?.source_schemas = &.{old_schema};
    artifacts[0].rewrite.?.source_scope = null;
    try std.testing.expectError(error.InvalidRestoreStaging, plan.validate(alloc));
    artifacts[0].rewrite.?.source_scope = source;
    artifacts[0].rewrite.?.program_digest = @splat(9);
    try std.testing.expectError(error.InvalidRestoreStaging, plan.validate(alloc));
}
