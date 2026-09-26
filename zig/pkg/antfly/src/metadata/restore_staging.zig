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

/// Graph declarations must be recognized at the durable-plan boundary, not
/// inferred from the first index or from the old owner's mutable runtime.
pub fn hasGraphIndex(alloc: std.mem.Allocator, indexes_json: []const u8) !bool {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, indexes_json, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidRestoreStaging;
    for (parsed.value.object.values()) |declaration| {
        if (declaration != .object) return error.InvalidRestoreStaging;
        const kind = declaration.object.get("type") orelse return error.InvalidRestoreStaging;
        if (kind != .string) return error.InvalidRestoreStaging;
        if (std.mem.eql(u8, kind.string, "graph")) return true;
    }
    return false;
}

/// Owner-comparable semantic graph declaration digest. Graph plans are new in
/// this feature and have no legacy raw-JSON digest compatibility mode.
pub fn graphRetirementDigest(alloc: std.mem.Allocator, source_table_id: u64, target_table_id: u64, indexes_json: []const u8) !?Digest {
    const config_digest = (try @import("../storage/db/graph_retirement_config.zig").fromMetadata(alloc, indexes_json)) orelse return null;
    return @import("../storage/db/graph_retirement_config.zig").retirementDigest(source_table_id, target_table_id, config_digest);
}

test "graph retirement digest matches production index config extraction for explicit and legacy incarnations" {
    const alloc = std.testing.allocator;
    const graph_config = @import("../storage/db/graph_retirement_config.zig");
    const table_index_config = @import("../api/table_index_config.zig");
    for ([_][]const u8{
        "{\"links\":{\"type\":\"graph\",\"_index_incarnation\":17,\"edge_types\":[{\"name\":\"knows\"}],\"settings\":{\"b\":2,\"a\":1}}}",
        "{\"links\":{\"type\":\"graph\",\"edge_types\":[{\"name\":\"knows\"}],\"settings\":{\"b\":2,\"a\":1}}}",
    }) |indexes_json| {
        var parsed = try std.json.parseFromSlice(std.json.Value, alloc, indexes_json, .{});
        defer parsed.deinit();
        const raw = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(parsed.value.object.get("links").?, .{})});
        defer alloc.free(raw);
        var loaded = try table_index_config.parseIndexConfig(alloc, "links", raw);
        defer loaded.deinit(alloc);
        try std.testing.expectEqualDeep((try graph_config.fromMetadata(alloc, indexes_json)).?, (try graph_config.fromLoaded(alloc, &.{loaded})).?);
        try std.testing.expectEqualDeep((try graphRetirementDigest(alloc, 100, 200, indexes_json)).?, graph_config.retirementDigest(100, 200, (try graph_config.fromLoaded(alloc, &.{loaded})).?));
    }
}
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
    var initial_fk_owners = std.ArrayListUnmanaged(ProvisioningProjection.InitialFkOwner).empty;
    var selected_tables = std.ArrayListUnmanaged(records.TableRecord).empty;
    var selected_ranges = std.ArrayListUnmanaged(records.RangeRecord).empty;
    errdefer {
        for (jobs.items) |job| alloc.free(job);
        jobs.deinit(alloc);
        initial_fk_owners.deinit(alloc);
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
    for (projection.initial_fk_owners) |owner| {
        if (owner.child_group_id == 0 or owner.child_table_id == 0 or
            owner.namespace.table_id != owner.child_table_id) return error.InvalidGenerationPublication;
        try hidden_groups.put(a, owner.child_group_id, {});
        if (assigned.contains(owner.child_group_id)) {
            try owned_tables.put(a, owner.child_table_id, {});
            try initial_fk_owners.append(alloc, owner);
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
    return .{ .tables = owned_table_slice, .ranges = owned_range_slice, .jobs_json = try jobs.toOwnedSlice(alloc), .initial_fk_owners = try initial_fk_owners.toOwnedSlice(alloc) };
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
/// `activating` is a durable, irreversible publication decision. The old
/// child owners are fenced and the new child owners remain hidden while each
/// external parent activates its generation tombstone. Cancellation after
/// this point would revive old children without their inverse references.
pub const State = enum { importing, validating, cutover, activating, published, canceling, canceled, preparing_sources };
pub const SourceArtifact = @import("restore_provisioning_contract.zig").SourceArtifact;
pub const SourceRangeGenerationAdmissions = struct {
    target_group_id: u64,
    source_namespace: @import("../storage/db/doc_identity_namespace.zig").Namespace,
    entries: []const @import("../storage/portable_backup.zig").SourceGenerationAdmissionSummaryEntry,
    digest: Digest,
    /// Derived once from the complete selected cohort and authenticated by
    /// the immutable Plan digest. Worker ticks borrow these canonical values.
    mappings: []const @import("../storage/db/restore_staging_contract.zig").GenerationAdmissionMapping = &.{},
};
/// Exact old-owner state observed before an empty-generation rewrite is
/// admitted. The source owner later fences and seals this same state; only
/// active, dependency-closed scopes are remapped into the fresh parent.
/// Null scopes and historical retirement tombstones stay on the fenced old
/// physical owner, but their digests remain part of the publication proof.
pub const GenerationHandoffRange = struct {
    source_group_id: u64,
    target_group_id: u64,
    source_namespace: @import("../storage/db/doc_identity_namespace.zig").Namespace,
    admissions: []const @import("../storage/portable_backup.zig").SourceGenerationAdmissionSummaryEntry,
    admissions_digest: Digest,
    retired_digest: Digest,
    retired_count: u64,
    mappings: []const @import("../storage/db/restore_staging_contract.zig").GenerationAdmissionMapping = &.{},
};
pub const Target = struct {
    pub fn nativeJsonSkipField(self: @This(), comptime name: []const u8) bool {
        return (std.mem.eql(u8, name, "empty_generation") and !self.empty_generation) or
            (std.mem.eql(u8, name, "graph_retirement_digest") and self.graph_retirement_digest == null) or
            (std.mem.eql(u8, name, "source_table_name") and self.source_table_name.len == 0) or
            (std.mem.eql(u8, name, "target_schema_digest") and self.target_schema_digest == null) or
            (std.mem.eql(u8, name, "generation_handoffs") and self.generation_handoffs.len == 0) or
            (std.mem.eql(u8, name, "source_generation_admissions") and self.source_generation_admissions.len == 0);
    }
    source_table_id: u64,
    source_table_name: []const u8 = "",
    target_schema_digest: ?Digest = null,
    empty_generation: bool = false,
    /// Binds graph artifact retirement to the exact old/new physical table
    /// incarnations and unchanged index declarations. Old edge/metric stores
    /// remain with the fenced old groups; fresh groups must prove pristine
    /// graph stores before acknowledging the hidden owner reservation.
    graph_retirement_digest: ?Digest = null,
    table: records.TableRecord,
    /// Logical publication is part of the same transaction as the new owner
    /// generation. The namespace ID is pinned, not reinterpreted by name.
    catalog_binding: ?@import("../system_catalog/domain.zig").Resource = null,
    ranges: []const records.RangeRecord,
    /// Native owner reservations bind these authenticated source identities
    /// before accepting an import RPC. No full-plan transfer per row page.
    source_artifacts: []const SourceArtifact = &.{},
    /// Source-parent proof only. Never copy an archived accepted scope into a
    /// target owner: child IDs and FK generations change with incarnation.
    source_generation_admissions: []const SourceRangeGenerationAdmissions = &.{},
    /// Every old physical range of an empty-generation rewrite has an exact
    /// namespace-bound accepted-scope and retirement summary, even if both
    /// sets are empty. Generic rewrites never carry this authority.
    generation_handoffs: []const GenerationHandoffRange = &.{},
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
/// An untouched FK parent participates in an empty-generation cutover only to
/// retire inverse references to the selected old child generations. Its rows
/// and logical table identity remain unchanged.
pub const ExternalFkParent = struct {
    table: records.TableRecord,
    ranges: []const records.RangeRecord,
    fences: []const @import("../storage/db/relational_integrity_topology_contract.zig").Fence,
    foreign_keys: []const struct {
        child_table_id: u64,
        child_table_name: []const u8,
        constraint_name: []const u8,
        generation: @import("../storage/db/relational_integrity_contract.zig").Generation,
        /// Exact fresh child generation installed by the replacement table
        /// incarnation. Parent owners accept it before child publication.
        next_generation: @import("../storage/db/relational_integrity_contract.zig").Generation,
    },
};

pub fn plannedForeignGeneration(alloc: std.mem.Allocator, child: Target, name: []const u8) !@import("../storage/db/relational_integrity_contract.zig").Generation {
    return foreignGenerationForTableId(alloc, child, child.table.table_id, name);
}

fn foreignGenerationForTableId(alloc: std.mem.Allocator, child: Target, table_id: u64, name: []const u8) !@import("../storage/db/relational_integrity_contract.zig").Generation {
    const schema_api = @import("../schema/mod.zig");
    const native = @import("../storage/schema.zig");
    const declarations = @import("../schema/relational_declarations.zig");
    const catalog = @import("../storage/db/relational_integrity_catalog.zig");
    var parsed = try schema_api.parseValidatedTableSchema(alloc, child.table.schema_json);
    defer parsed.deinit(alloc);
    const runtime = try schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer native.freeSchema(alloc, runtime);
    const serialized = try native.serializeSchema(alloc, runtime);
    defer alloc.free(serialized);
    var schema_digest: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(serialized, &schema_digest, .{});
    const definitions = try declarations.definitionFingerprints(alloc, parsed, runtime);
    defer declarations.freeDefinitions(alloc, definitions);
    var update = try catalog.prepare(alloc, null, try catalog.incarnationFromTableId(table_id), runtime.version, schema_digest, definitions);
    defer update.deinit();
    return (update.catalog.find(.foreign_key, name) orelse return error.InvalidRestoreStaging).generation;
}

pub fn runtimeSchemaDigestForTable(alloc: std.mem.Allocator, schema_json: []const u8) !Digest {
    const api_tables = @import("../api/tables.zig");
    const runtime_schema = @import("../storage/schema.zig");
    var schema = try api_tables.parseValidatedTableSchema(alloc, schema_json);
    defer schema.deinit(alloc);
    const typed = try api_tables.deriveRuntimeTableSchema(alloc, schema);
    defer runtime_schema.freeSchema(alloc, typed);
    const encoded = try runtime_schema.serializeSchema(alloc, typed);
    defer alloc.free(encoded);
    return @import("../storage/db/restore_staging_contract.zig").digest(encoded);
}

/// A build-time projection: expensive schema/catalog derivation occurs once,
/// before the immutable Plan is hashed. Each worker tick then only hashes its
/// compact owner scope and already validated per-range mapping.
pub fn prepareTargetProjectionsAlloc(alloc: std.mem.Allocator, targets: []Target) !void {
    const contract = @import("../storage/db/restore_staging_contract.zig");
    for (targets) |*target| target.target_schema_digest = try runtimeSchemaDigestForTable(alloc, target.table.schema_json);
    for (targets) |*parent| {
        if (parent.source_generation_admissions.len == 0) continue;
        const proofs = try alloc.alloc(SourceRangeGenerationAdmissions, parent.source_generation_admissions.len);
        for (parent.source_generation_admissions, proofs) |source, *proof| {
            proof.* = source;
            const mappings = try alloc.alloc(contract.GenerationAdmissionMapping, source.entries.len);
            for (source.entries, mappings) |entry, *mapping| {
                const child: Target = for (targets) |candidate| {
                    if (candidate.source_table_id == entry.child_table_id and
                        std.mem.eql(u8, candidate.source_table_name, entry.child_table_name)) break candidate;
                } else return error.RestoreDependencyMissing;
                mapping.* = .{
                    .source_child_table_id = entry.child_table_id,
                    .source_child_table_name = entry.child_table_name,
                    .target_child_table_id = child.table.table_id,
                    .target_child_table_name = child.table.name,
                    .constraint_name = entry.constraint_name,
                    .source_generation = entry.active_generation,
                    .target_generation = if (entry.active_generation != null) try plannedForeignGeneration(alloc, child, entry.constraint_name) else null,
                    .source_scope_digest = entry.source_scope_digest,
                };
            }
            proof.mappings = mappings;
        }
        parent.source_generation_admissions = proofs;
    }
}

/// Build immutable, active-only destination admissions from the complete
/// selected TRUNCATE cohort. The old-owner summary includes null tombstones,
/// but a fresh namespace cannot inherit their old physical identity.
pub fn prepareEmptyGenerationHandoffMappingsAlloc(alloc: std.mem.Allocator, targets: []Target) !void {
    const Mapping = @import("../storage/db/restore_staging_contract.zig").GenerationAdmissionMapping;
    for (targets) |*parent| {
        if (!parent.empty_generation) continue;
        const handoffs = try alloc.dupe(GenerationHandoffRange, parent.generation_handoffs);
        for (handoffs) |*handoff| {
            var active_count: usize = 0;
            for (handoff.admissions) |entry| if (entry.active_generation != null) {
                active_count += 1;
            };
            const mappings = try alloc.alloc(Mapping, active_count);
            var index: usize = 0;
            for (handoff.admissions) |entry| {
                const generation = entry.active_generation orelse continue;
                const child: Target = for (targets) |candidate| {
                    if (candidate.empty_generation and candidate.source_table_id == entry.child_table_id and
                        candidate.replace != null and std.mem.eql(u8, candidate.replace.?.table.name, entry.child_table_name)) break candidate;
                } else return error.RestoreDependencyMissing;
                mappings[index] = .{
                    .source_child_table_id = entry.child_table_id,
                    .source_child_table_name = entry.child_table_name,
                    .target_child_table_id = child.table.table_id,
                    .target_child_table_name = child.table.name,
                    .constraint_name = entry.constraint_name,
                    .source_generation = generation,
                    .target_generation = try plannedForeignGeneration(alloc, child, entry.constraint_name),
                    .source_scope_digest = entry.source_scope_digest,
                };
                index += 1;
            }
            handoff.mappings = mappings;
        }
        parent.generation_handoffs = handoffs;
    }
}

pub const Plan = struct {
    pub fn nativeJsonSkipField(self: @This(), comptime name: []const u8) bool {
        // Preserve the canonical digest of in-flight plans created before
        // external parents became an optional part of the envelope.
        return std.mem.eql(u8, name, "external_fk_parents") and self.external_fk_parents.len == 0;
    }

    id: Id,
    /// Authenticated server-issued aggregate manifest identity, not an
    /// independent set of user-selected table snapshot timestamps.
    cohort_digest: Digest,
    source_location: []const u8 = "",
    source_connection: []const u8 = "",
    skipped_tables: []const []const u8 = &.{},
    targets: []const Target,
    external_fk_parents: []const ExternalFkParent = &.{},
    preparing_sources: bool = false,

    pub fn jsonStringify(self: Plan, jw: anytype) @TypeOf(jw.*).Error!void {
        try @import("../storage/db/relational_integrity_json.zig").write(self, jw);
    }

    pub fn validate(self: Plan, alloc: std.mem.Allocator) !void {
        if (std.mem.allEqual(u8, &self.id, 0) or std.mem.allEqual(u8, &self.cohort_digest, 0) or
            self.targets.len == 0 or self.targets.len > 128 or self.external_fk_parents.len > 128) return error.InvalidRestoreStaging;
        if (self.skipped_tables.len > 128 or self.skipped_tables.len + self.targets.len > 128) return error.InvalidRestoreStaging;
        for (self.skipped_tables, 0..) |name, index| {
            if (name.len == 0 or name.len > 4096) return error.InvalidRestoreStaging;
            for (self.skipped_tables[0..index]) |previous| if (std.mem.eql(u8, previous, name)) return error.InvalidRestoreStaging;
            for (self.targets) |target| if (std.mem.eql(u8, target.table.name, name)) return error.InvalidRestoreStaging;
        }
        var range_count: usize = 0;
        var schema_bytes: usize = 0;
        var old_range_count: usize = 0;
        var handoff_entry_count: usize = 0;
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
        for (self.external_fk_parents, 0..) |parent, parent_index| {
            if (self.preparing_sources or parent.table.table_id == 0 or parent.table.name.len == 0 or parent.table.relational_retirement_json.len != 0 or
                parent.table.restore_backup_id.len != 0 or parent.ranges.len == 0 or parent.ranges.len > 4096 or parent.fences.len != parent.ranges.len or
                parent.foreign_keys.len == 0 or parent.foreign_keys.len > 128) return error.InvalidRestoreStaging;
            for (self.external_fk_parents[0..parent_index]) |previous| if (previous.table.table_id == parent.table.table_id or std.mem.eql(u8, previous.table.name, parent.table.name)) return error.InvalidRestoreStaging;
            for (self.targets) |target| {
                if (target.table.table_id == parent.table.table_id or target.source_table_id == parent.table.table_id or
                    std.mem.eql(u8, target.table.name, parent.table.name)) return error.InvalidRestoreStaging;
                if (target.replace) |old| if (old.table.table_id == parent.table.table_id) return error.InvalidRestoreStaging;
            }
            tables.validateCompleteKeyspaceRanges(parent.ranges) catch return error.InvalidRestoreStaging;
            old_range_count = std.math.add(usize, old_range_count, parent.ranges.len) catch return error.InvalidRestoreStaging;
            if (old_range_count > 4096) return error.InvalidRestoreStaging;
            schema_bytes = std.math.add(usize, schema_bytes, parent.table.schema_json.len +| parent.table.read_schema_json.len +| parent.table.indexes_json.len) catch return error.InvalidRestoreStaging;
            if (schema_bytes > 4 * 1024 * 1024) return error.InvalidRestoreStaging;
            for (parent.ranges) |range| {
                if (range.table_id != parent.table.table_id or range.restore_backup_id.len != 0) return error.InvalidRestoreStaging;
                try @import("../common/group_ids.zig").requireDataGroupId(range.group_id);
                if ((try group_ids.getOrPut(alloc, range.group_id)).found_existing) return error.InvalidRestoreStaging;
                const fence = for (parent.fences) |item| {
                    if (item.owner_group_id == range.group_id) break item;
                } else return error.InvalidRestoreStaging;
                _ = try fence.encode();
                if (fence.role != .truncate_parent or fence.namespace.table_id != parent.table.table_id or
                    fence.namespace.shard_id != tables.rangeDocIdentityShardId(range) or fence.namespace.range_id != tables.rangeDocIdentityRangeId(range) or
                    fence.peer_group_id != range.group_id or fence.transition_id != std.mem.readInt(u64, self.id[0..8], .little) or
                    fence.attempt != std.mem.readInt(u64, self.id[8..16], .little)) return error.InvalidRestoreStaging;
            }
            for (parent.fences, 0..) |fence, index| for (parent.fences[0..index]) |prior| if (prior.owner_group_id == fence.owner_group_id) return error.InvalidRestoreStaging;
            for (parent.foreign_keys, 0..) |foreign, index| {
                if (foreign.child_table_id == 0 or foreign.child_table_name.len == 0 or foreign.child_table_name.len > 256 or
                    foreign.constraint_name.len == 0 or foreign.constraint_name.len > 256 or std.mem.allEqual(u8, &foreign.generation, 0) or
                    std.mem.allEqual(u8, &foreign.next_generation, 0) or std.mem.eql(u8, &foreign.generation, &foreign.next_generation)) return error.InvalidRestoreStaging;
                for (parent.foreign_keys[0..index]) |prior| if ((prior.child_table_id == foreign.child_table_id and std.mem.eql(u8, prior.constraint_name, foreign.constraint_name)) or
                    std.mem.eql(u8, &prior.generation, &foreign.generation) or std.mem.eql(u8, &prior.next_generation, &foreign.next_generation)) return error.InvalidRestoreStaging;
                for (self.external_fk_parents[0..parent_index]) |previous_parent| for (previous_parent.foreign_keys) |prior| if (std.mem.eql(u8, &prior.generation, &foreign.generation)) return error.InvalidRestoreStaging;
                const child = for (self.targets) |target| {
                    if (!target.empty_generation or target.source_table_id != foreign.child_table_id) continue;
                    break target.replace.?.table;
                } else return error.InvalidRestoreStaging;
                if (!std.mem.eql(u8, child.name, foreign.child_table_name)) return error.InvalidRestoreStaging;
                const planned_child = for (self.targets) |target| {
                    if (target.source_table_id == foreign.child_table_id) break target;
                } else return error.InvalidRestoreStaging;
                if (!std.mem.eql(u8, &foreign.next_generation, &try plannedForeignGeneration(alloc, planned_child, foreign.constraint_name))) return error.InvalidRestoreStaging;
                var found = false;
                for ([_][]const u8{ child.schema_json, child.read_schema_json }) |definition| {
                    if (definition.len == 0) continue;
                    var schema = try @import("../schema/mod.zig").parseValidatedTableSchema(alloc, definition);
                    defer schema.deinit(alloc);
                    if (schema.foreign_keys) |fks| for (fks.value) |fk| if (std.mem.eql(u8, fk.name, foreign.constraint_name) and std.mem.eql(u8, fk.parent_table, parent.table.name)) {
                        found = true;
                    };
                }
                if (!found) return error.InvalidRestoreStaging;
            }
        }
        for (self.targets, 0..) |target, index| {
            const table = target.table;
            if (target.target_schema_digest) |cached| {
                if (!std.mem.eql(u8, &cached, &try runtimeSchemaDigestForTable(alloc, table.schema_json))) return error.InvalidRestoreStaging;
            }
            if (target.empty_generation) {
                const old = target.replace orelse return error.InvalidRestoreStaging;
                if (self.preparing_sources or target.rewrite != null or target.rewrite_sources.len != 0 or target.source_artifacts.len != 0 or old.table.table_id != target.source_table_id or old.fences.len != old.ranges.len or old.ranges.len != target.ranges.len) return error.InvalidRestoreStaging;
                if (target.generation_handoffs.len != old.ranges.len) return error.InvalidRestoreStaging;
                if (!std.mem.eql(u8, old.table.schema_json, table.schema_json) or !std.mem.eql(u8, old.table.read_schema_json, table.read_schema_json) or !std.mem.eql(u8, old.table.indexes_json, table.indexes_json)) return error.InvalidRestoreStaging;
                const graph_index = try hasGraphIndex(alloc, table.indexes_json);
                if (graph_index != (target.graph_retirement_digest != null)) return error.InvalidRestoreStaging;
                if (target.graph_retirement_digest) |retirement_digest| {
                    const expected = (try graphRetirementDigest(alloc, old.table.table_id, table.table_id, table.indexes_json)) orelse return error.InvalidRestoreStaging;
                    if (!std.mem.eql(u8, &retirement_digest, &expected)) return error.InvalidRestoreStaging;
                }
                for (old.ranges, target.ranges, target.generation_handoffs) |source, destination, handoff| {
                    if (!std.mem.eql(u8, source.start_key, destination.start_key) or !std.mem.eql(u8, source.end_key orelse "", destination.end_key orelse "")) return error.InvalidRestoreStaging;
                    const fence = for (old.fences) |item| {
                        if (item.owner_group_id == source.group_id) break item;
                    } else return error.InvalidRestoreStaging;
                    if (fence.role != .rewrite_source or fence.peer_group_id != destination.group_id or fence.transition_id != std.mem.readInt(u64, self.id[0..8], .little) or fence.attempt != std.mem.readInt(u64, self.id[8..16], .little)) return error.InvalidRestoreStaging;
                    if (handoff.source_group_id != source.group_id or handoff.target_group_id != destination.group_id or
                        !handoff.source_namespace.eql(fence.namespace) or
                        std.mem.allEqual(u8, &handoff.retired_digest, 0) or
                        handoff.mappings.len > handoff.admissions.len or
                        !std.mem.eql(u8, &handoff.admissions_digest, &try @import("../storage/portable_backup.zig").sourceGenerationAdmissionSummaryDigest(handoff.source_namespace, handoff.admissions))) return error.InvalidRestoreStaging;
                    handoff_entry_count = std.math.add(usize, handoff_entry_count, handoff.admissions.len) catch return error.InvalidRestoreStaging;
                    if (handoff_entry_count > 4096) return error.InvalidRestoreStaging;
                    var mapped_index: usize = 0;
                    for (handoff.admissions) |entry| {
                        if (std.mem.allEqual(u8, &entry.source_scope_digest, 0)) return error.InvalidRestoreStaging;
                        const generation = entry.active_generation orelse continue;
                        const child: Target = for (self.targets) |candidate| {
                            if (candidate.empty_generation and candidate.source_table_id == entry.child_table_id and
                                candidate.replace != null and std.mem.eql(u8, candidate.replace.?.table.name, entry.child_table_name)) break candidate;
                        } else return error.RestoreDependencyMissing;
                        const old_generation = try foreignGenerationForTableId(alloc, child, child.source_table_id, entry.constraint_name);
                        if (!std.mem.eql(u8, &generation, &old_generation)) return error.InvalidRestoreStaging;
                        var declared = false;
                        var child_schema = try @import("../schema/mod.zig").parseValidatedTableSchema(alloc, child.table.schema_json);
                        defer child_schema.deinit(alloc);
                        if (child_schema.foreign_keys) |fks| for (fks.value) |fk| {
                            if (std.mem.eql(u8, fk.name, entry.constraint_name) and std.mem.eql(u8, fk.parent_table, old.table.name)) declared = true;
                        };
                        if (!declared or mapped_index >= handoff.mappings.len) return error.InvalidRestoreStaging;
                        const mapping = handoff.mappings[mapped_index];
                        try mapping.validate();
                        const next_generation = try plannedForeignGeneration(alloc, child, entry.constraint_name);
                        if (mapping.source_child_table_id != entry.child_table_id or
                            !std.mem.eql(u8, mapping.source_child_table_name, entry.child_table_name) or
                            mapping.target_child_table_id != child.table.table_id or
                            !std.mem.eql(u8, mapping.target_child_table_name, child.table.name) or
                            !std.mem.eql(u8, mapping.constraint_name, entry.constraint_name) or
                            !std.meta.eql(mapping.source_generation, entry.active_generation) or
                            mapping.target_generation == null or
                            !std.mem.eql(u8, &mapping.target_generation.?, &next_generation) or
                            !std.mem.eql(u8, &mapping.source_scope_digest, &entry.source_scope_digest)) return error.InvalidRestoreStaging;
                        mapped_index += 1;
                    }
                    if (mapped_index != handoff.mappings.len) return error.InvalidRestoreStaging;
                }
            }
            if (!target.empty_generation and (target.graph_retirement_digest != null or target.generation_handoffs.len != 0)) return error.InvalidRestoreStaging;
            if (target.source_generation_admissions.len != 0) {
                if (target.source_table_name.len == 0 or target.source_table_name.len > 256 or
                    target.target_schema_digest == null or
                    target.source_generation_admissions.len != target.ranges.len or
                    target.source_artifacts.len != target.ranges.len)
                    return error.InvalidRestoreStaging;
                for (target.source_artifacts) |artifact| if (artifact.format != .portable) return error.InvalidRestoreStaging;
                for (target.source_generation_admissions, 0..) |range_proof, proof_index| {
                    const artifact = target.source_artifacts[proof_index];
                    if (range_proof.target_group_id != artifact.target_group_id or
                        !range_proof.source_namespace.eql(artifact.source_namespace) or
                        range_proof.mappings.len != range_proof.entries.len or
                        !std.mem.eql(u8, &range_proof.digest, &try @import("../storage/portable_backup.zig").sourceGenerationAdmissionSummaryDigest(range_proof.source_namespace, range_proof.entries)))
                        return error.InvalidRestoreStaging;
                    for (range_proof.entries, range_proof.mappings) |entry, mapping| {
                        try mapping.validate();
                        if (std.mem.allEqual(u8, &entry.source_scope_digest, 0)) return error.InvalidRestoreStaging;
                        const child: Target = for (self.targets) |candidate| {
                            if (candidate.source_table_id == entry.child_table_id) break candidate;
                        } else return error.RestoreDependencyMissing;
                        if (!std.mem.eql(u8, child.source_table_name, entry.child_table_name)) return error.InvalidRestoreStaging;
                        var child_schema = try @import("../schema/mod.zig").parseValidatedTableSchema(alloc, child.table.schema_json);
                        defer child_schema.deinit(alloc);
                        var declared = false;
                        if (child_schema.foreign_keys) |fks| for (fks.value) |fk| {
                            if (std.mem.eql(u8, fk.name, entry.constraint_name) and std.mem.eql(u8, fk.parent_table, target.source_table_name)) declared = true;
                        };
                        if (entry.active_generation) |generation| {
                            if (!declared or !std.mem.eql(u8, &generation, &try foreignGenerationForTableId(alloc, child, child.source_table_id, entry.constraint_name)))
                                return error.InvalidRestoreStaging;
                        } else if (declared) return error.InvalidRestoreStaging;
                        if (mapping.source_child_table_id != entry.child_table_id or
                            !std.mem.eql(u8, mapping.source_child_table_name, entry.child_table_name) or
                            mapping.target_child_table_id != child.table.table_id or
                            !std.mem.eql(u8, mapping.target_child_table_name, child.table.name) or
                            !std.mem.eql(u8, mapping.constraint_name, entry.constraint_name) or
                            !std.meta.eql(mapping.source_generation, entry.active_generation) or
                            !std.mem.eql(u8, &mapping.source_scope_digest, &entry.source_scope_digest) or
                            !std.meta.eql(mapping.target_generation, if (entry.active_generation != null) try plannedForeignGeneration(alloc, child, entry.constraint_name) else null))
                            return error.InvalidRestoreStaging;
                    }
                }
            } else if (target.rewrite == null and target.source_artifacts.len != 0 and target.source_artifacts[0].format == .portable) return error.InvalidRestoreStaging;
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
                    const selected_parent: ?Target = for (self.targets) |candidate| {
                        const lookup_name = if (target.source_generation_admissions.len != 0) candidate.source_table_name else candidate.table.name;
                        if (std.mem.eql(u8, lookup_name, fk.parent_table)) break candidate;
                    } else null;
                    if (selected_parent) |planned_parent| if (planned_parent.empty_generation) {
                        if (!target.empty_generation or target.replace == null) return error.RestoreDependencyMissing;
                        // Every physical parent range must attest the old
                        // active generation. A selected child declaration
                        // without a corresponding accepted scope is not a
                        // dependency-closed handoff, even if its schema is
                        // otherwise valid.
                        const source_generation = try foreignGenerationForTableId(alloc, target, target.source_table_id, fk.name);
                        for (planned_parent.generation_handoffs) |handoff| {
                            const present = for (handoff.admissions) |entry| {
                                if (entry.child_table_id == target.source_table_id and
                                    std.mem.eql(u8, entry.child_table_name, target.replace.?.table.name) and
                                    std.mem.eql(u8, entry.constraint_name, fk.name) and
                                    entry.active_generation != null and
                                    std.mem.eql(u8, &entry.active_generation.?, &source_generation)) break true;
                            } else false;
                            if (!present) return error.RestoreDependencyMissing;
                        }
                    };
                    const parent = if (selected_parent) |selected| selected.table else blk: {
                        for (self.external_fk_parents) |external| if (std.mem.eql(u8, external.table.name, fk.parent_table)) {
                            for (external.foreign_keys) |foreign| if (foreign.child_table_id == target.source_table_id and std.mem.eql(u8, foreign.constraint_name, fk.name)) break :blk external.table;
                            return error.RestoreDependencyMissing;
                        };
                        return error.RestoreDependencyMissing;
                    };
                    // Portable artifacts contain the source FK schema. A
                    // destination parent rename needs an explicit schema
                    // rewrite, not merely a remapped admission identity.
                    if (target.source_generation_admissions.len != 0 and
                        !std.mem.eql(u8, parent.name, fk.parent_table)) return error.RestoreDependencyMissing;
                    try @import("../schema/relational_foreign_key_target.zig").validate(alloc, schema_json, parent.name, parent.schema_json);
                    if (target.source_artifacts.len != 0 and target.source_artifacts[0].format == .portable) {
                        const selected = selected_parent orelse return error.RestoreDependencyMissing;
                        if (selected.source_generation_admissions.len == 0) return error.RestoreSourceProofMissing;
                        const source_generation = try foreignGenerationForTableId(alloc, target, target.source_table_id, fk.name);
                        for (selected.source_generation_admissions) |range_proof| {
                            const found = for (range_proof.entries) |entry| {
                                if (entry.child_table_id == target.source_table_id and
                                    std.mem.eql(u8, entry.child_table_name, target.source_table_name) and
                                    std.mem.eql(u8, entry.constraint_name, fk.name) and
                                    entry.active_generation != null and
                                    std.mem.eql(u8, &entry.active_generation.?, &source_generation)) break true;
                            } else false;
                            if (!found) return error.RestoreSourceProofMissing;
                        }
                        _ = try plannedForeignGeneration(alloc, target, fk.name);
                    }
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

/// Recompute one exact old-owner graph scope from immutable metadata.
pub fn graphSealScopeForOldRange(
    alloc: std.mem.Allocator,
    plan: Plan,
    plan_digest: Digest,
    target: Target,
    old_range: records.RangeRecord,
) !?@import("../storage/db/graph_retirement_seal.zig").Scope {
    const declared = target.graph_retirement_digest orelse return null;
    const old = target.replace orelse return error.InvalidRestoreStaging;
    if (!target.empty_generation or old_range.table_id != old.table.table_id or
        target.source_table_id != old.table.table_id) return error.InvalidRestoreStaging;
    const config_digest = (try @import("../storage/db/graph_retirement_config.zig").fromMetadata(alloc, old.table.indexes_json)) orelse return error.InvalidRestoreStaging;
    const v2 = @import("../storage/db/graph_retirement_config.zig").retirementDigest(old.table.table_id, target.table.table_id, config_digest);
    if (!std.mem.eql(u8, &declared, &v2)) return error.InvalidRestoreStaging;
    const fence = for (old.fences) |candidate| {
        if (candidate.owner_group_id == old_range.group_id) break candidate;
    } else return error.InvalidRestoreStaging;
    const range_id = if (old_range.range_id == 0) old_range.group_id else old_range.range_id;
    if (fence.namespace.range_id != tables.rangeDocIdentityRangeId(old_range) or
        fence.owner_group_id != old_range.group_id or range_id == 0) return error.InvalidRestoreStaging;
    const scope: @import("../storage/db/graph_retirement_seal.zig").Scope = .{
        .fence = fence,
        .plan_id = plan.id,
        .plan_digest = plan_digest,
        .target_table_id = target.table.table_id,
        .graph_config_digest = config_digest,
    };
    try scope.validate();
    return scope;
}

pub fn generationHandoffForOldRange(target: Target, old_range: records.RangeRecord) !?GenerationHandoffRange {
    if (!target.empty_generation) return null;
    const old = target.replace orelse return error.InvalidRestoreStaging;
    if (old.ranges.len != target.generation_handoffs.len) return error.InvalidRestoreStaging;
    for (old.ranges, target.generation_handoffs) |candidate, handoff| {
        if (candidate.group_id != old_range.group_id) continue;
        if (candidate.table_id != old_range.table_id or handoff.source_group_id != candidate.group_id or
            handoff.source_namespace.table_id != old.table.table_id) return error.InvalidRestoreStaging;
        return handoff;
    }
    return error.InvalidRestoreStaging;
}

/// Exact logical metadata identity of a fenced, read-indexed source preview.
/// The durable Raft seal and applied watermark are checked separately before
/// old_fenced. This receipt only allows the reversible validating phase to
/// enter cutover after all source summaries match the immutable Plan.
pub fn generationHandoffPreflightDigest(fence: @import("../storage/db/relational_integrity_topology_contract.zig").Fence, plan_digest: Digest, handoff: GenerationHandoffRange) !Digest {
    if (!handoff.source_namespace.eql(fence.namespace) or handoff.source_group_id != fence.owner_group_id) return error.InvalidRestoreStaging;
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly empty generation source preflight v1");
    hash.update(&try fence.encode());
    hash.update(&plan_digest);
    hash.update(&handoff.admissions_digest);
    hash.update(&handoff.retired_digest);
    var number: [8]u8 = undefined;
    std.mem.writeInt(u64, &number, handoff.retired_count, .little);
    hash.update(&number);
    var result: Digest = undefined;
    hash.final(&result);
    return result;
}

pub fn generationHandoffSealDigest(fence: @import("../storage/db/relational_integrity_topology_contract.zig").Fence, plan_digest: Digest, handoff: GenerationHandoffRange) !Digest {
    if (!handoff.source_namespace.eql(fence.namespace) or handoff.source_group_id != fence.owner_group_id) return error.InvalidRestoreStaging;
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly empty generation source sealed v1");
    hash.update(&try fence.encode());
    hash.update(&plan_digest);
    hash.update(&handoff.admissions_digest);
    hash.update(&handoff.retired_digest);
    var number: [8]u8 = undefined;
    std.mem.writeInt(u64, &number, handoff.retired_count, .little);
    hash.update(&number);
    var result: Digest = undefined;
    hash.final(&result);
    return result;
}

/// One old_fenced receipt must prove both protocols when a graph index and
/// accepted FK generations coexist on the same retired physical owner.
pub fn generationHandoffOldFenceDigest(fence: @import("../storage/db/relational_integrity_topology_contract.zig").Fence, handoff_seal_digest: Digest, graph_seal_digest: ?Digest) !Digest {
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly old owner graph and generation handoff v1");
    hash.update(&try fence.encode());
    hash.update(&handoff_seal_digest);
    hash.update(if (graph_seal_digest) |digest| &digest else &([_]u8{0} ** 32));
    var result: Digest = undefined;
    hash.final(&result);
    return result;
}

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
    const schema_digest = target.target_schema_digest orelse try runtimeSchemaDigestForTable(alloc, target.table.schema_json);
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
            .target_schema_digest = schema_digest,
            .empty_generation = true,
            .graph_retirement_digest = target.graph_retirement_digest,
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
        .target_schema_digest = schema_digest,
        .preserve_artifacts = artifact.rewrite == null,
        .rewrite = artifact.rewrite,
    };
}

pub const MappedGenerationAdmission = struct {
    command: @import("../storage/db/restore_staging_contract.zig").InstallGenerationAdmissions,
    expected_receipt_digest: Digest,

    pub fn deinit(self: *@This(), _: std.mem.Allocator) void {
        self.* = undefined;
    }
};

pub const MappedEmptyGenerationHandoff = struct {
    command: @import("../storage/db/relational_integrity_topology_contract.zig").GenerationHandoffInstall,
    expected_receipt_digest: Digest,
};

pub fn mappedEmptyGenerationHandoffForGroup(alloc: std.mem.Allocator, plan: Plan, plan_digest: Digest, group_id: u64) !?MappedEmptyGenerationHandoff {
    for (plan.targets) |target| {
        if (!target.empty_generation) continue;
        if (target.ranges.len != target.generation_handoffs.len) return error.InvalidRestoreStaging;
        for (target.ranges, target.generation_handoffs) |range, handoff| {
            if (range.group_id != group_id) continue;
            if (handoff.target_group_id != group_id) return error.InvalidRestoreStaging;
            const scope = try ownerScope(alloc, plan, plan_digest, target, range);
            const command: @import("../storage/db/relational_integrity_topology_contract.zig").GenerationHandoffInstall = .{
                .scope = scope.digest(),
                .source_summary_digest = handoff.admissions_digest,
                .retired_digest = handoff.retired_digest,
                .retired_count = handoff.retired_count,
                .mappings = handoff.mappings,
            };
            return .{ .command = command, .expected_receipt_digest = try @import("../storage/db/empty_generation_handoff.zig").installReceiptDigest(command) };
        }
    }
    return null;
}

pub fn emptyGenerationDestinationFence(plan: Plan, target: Target, range: records.RangeRecord, handoff: GenerationHandoffRange, scope: @import("../storage/db/restore_staging_contract.zig").Scope) !@import("../storage/db/relational_integrity_topology_contract.zig").Fence {
    if (!target.empty_generation or range.group_id != handoff.target_group_id or
        handoff.source_group_id == 0 or !scope.target_namespace.eql(.{
        .table_id = target.table.table_id,
        .shard_id = tables.rangeDocIdentityShardId(range),
        .range_id = tables.rangeDocIdentityRangeId(range),
    })) return error.InvalidRestoreStaging;
    const fence: @import("../storage/db/relational_integrity_topology_contract.zig").Fence = .{
        .transition_id = std.mem.readInt(u64, plan.id[0..8], .little),
        .attempt = std.mem.readInt(u64, plan.id[8..16], .little),
        .peer_group_id = handoff.source_group_id,
        .owner_group_id = range.group_id,
        .role = .rewrite_destination,
        .namespace = scope.target_namespace,
        .catalog_digest = scope.target_schema_digest,
    };
    _ = try fence.encode();
    return fence;
}

/// The same immutable staged plan drives hidden-owner bootstrap, coordinator
/// requests, and metadata receipt validation. Source scopes are proof only;
/// fresh child IDs and FK generations are recomputed from the target plan.
pub fn mappedGenerationAdmissionsForGroupAlloc(alloc: std.mem.Allocator, plan: Plan, plan_digest: Digest, group_id: u64) !?MappedGenerationAdmission {
    const contract = @import("../storage/db/restore_staging_contract.zig");
    const parent: Target = parent: {
        for (plan.targets) |target| for (target.ranges) |range| {
            if (range.group_id == group_id) break :parent target;
        };
        return error.InvalidRestoreStaging;
    };
    if (parent.source_generation_admissions.len == 0) return null;
    const range = for (parent.ranges) |candidate| {
        if (candidate.group_id == group_id) break candidate;
    } else return error.InvalidRestoreStaging;
    const range_proof = for (parent.source_generation_admissions) |candidate| {
        if (candidate.target_group_id == group_id) break candidate;
    } else return error.InvalidRestoreStaging;
    if (range_proof.entries.len == 0) return null;
    const scope = try ownerScope(alloc, plan, plan_digest, parent, range);
    if (range_proof.mappings.len != range_proof.entries.len) return error.InvalidRestoreStaging;
    const command: contract.InstallGenerationAdmissions = .{ .scope = scope.digest(), .source_summary_digest = range_proof.digest, .mappings = range_proof.mappings };
    return .{ .command = command, .expected_receipt_digest = try contract.admissionReceiptDigest(command) };
}

pub const ExpectedGenerationAdmission = @import("../storage/db/restore_staging_contract.zig").GenerationAdmissionExpectation;

/// The source-proof check is independent of installing a target admission:
/// an empty sealed portable range still has an authenticated summary digest.
pub fn sourceGenerationProofDigestForGroup(plan: Plan, group_id: u64) !?Digest {
    for (plan.targets) |target| {
        for (target.ranges) |range| {
            if (range.group_id != group_id) continue;
            if (target.source_generation_admissions.len == 0) return null;
            for (target.source_generation_admissions) |proof| {
                if (proof.target_group_id == group_id) return proof.digest;
            }
            return error.InvalidRestoreStaging;
        }
    }
    return error.InvalidRestoreStaging;
}

pub fn expectedGenerationAdmissionReceiptForGroup(alloc: std.mem.Allocator, plan: Plan, plan_digest: Digest, group_id: u64) !?ExpectedGenerationAdmission {
    var mapped = (try mappedGenerationAdmissionsForGroupAlloc(alloc, plan, plan_digest, group_id)) orelse return null;
    defer mapped.deinit(alloc);
    return .{ .source_summary_digest = mapped.command.source_summary_digest, .expected_receipt_digest = mapped.expected_receipt_digest };
}

test "relational integrity restore staging sealed portable empty source admission still projects namespace-bound proof" {
    const namespace: @import("../storage/db/doc_identity_namespace.zig").Namespace = .{ .table_id = 9, .shard_id = 10, .range_id = 11 };
    const proof_digest = try @import("../storage/portable_backup.zig").sourceGenerationAdmissionSummaryDigest(namespace, &.{});
    const ranges = [_]records.RangeRecord{.{ .table_id = 20, .group_id = 30, .range_id = 30, .doc_identity_shard_id = 30, .doc_identity_range_id = 30, .start_key = "" }};
    const proofs = [_]SourceRangeGenerationAdmissions{.{ .target_group_id = 30, .source_namespace = namespace, .entries = &.{}, .digest = proof_digest }};
    const targets = [_]Target{.{ .source_table_id = 9, .source_table_name = "parent", .table = .{ .table_id = 20, .name = "parent", .schema_json = "{}" }, .ranges = &ranges, .source_generation_admissions = &proofs }};
    const plan: Plan = .{ .id = @splat(1), .cohort_digest = @splat(2), .targets = &targets };
    try std.testing.expectEqualDeep(proof_digest, (try sourceGenerationProofDigestForGroup(plan, 30)).?);
    try std.testing.expect((try expectedGenerationAdmissionReceiptForGroup(std.testing.allocator, plan, @splat(3), 30)) == null);
    try std.testing.expectError(error.InvalidRestoreStaging, sourceGenerationProofDigestForGroup(plan, 31));
    const plain_targets = [_]Target{.{ .source_table_id = 9, .table = .{ .table_id = 20, .name = "parent", .schema_json = "{}" }, .ranges = &ranges }};
    try std.testing.expect((try sourceGenerationProofDigestForGroup(.{ .id = @splat(1), .cohort_digest = @splat(2), .targets = &plain_targets }, 30)) == null);
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
    /// Required for an unchanged external parent owner: unlike a staged
    /// target it has no hidden-placement authority row.
    owner_group: ?u64 = null,
    receipt: ?struct { state: State, owner_group: u64 } = null,

    pub fn jsonStringify(self: AuthorityRequest, stream: anytype) @TypeOf(stream.*).Error!void {
        try writeAuthority(self, stream);
    }

    pub fn validate(self: AuthorityRequest) !void {
        if (self.node_id == 0 or std.mem.allEqual(u8, &self.plan_id, 0)) return error.InvalidArgument;
        if (self.owner_group) |group| if (group == 0 or (self.receipt != null and self.receipt.?.owner_group != group)) return error.InvalidArgument;
        if (self.receipt) |receipt| {
            if (receipt.owner_group == 0) return error.InvalidArgument;
            switch (receipt.state) {
                .importing, .validating, .cutover, .activating, .canceling => {},
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

    /// Validate a parent activation decision from a *direct*, authenticated
    /// leader read-index response. These fields are not a signed bearer token:
    /// passing a coordinator-provided copy to the storage apply path would be
    /// forgeable. The final owner leader must perform this read itself before
    /// proposing its local activation transaction.
    pub fn parentActivationDecision(
        self: AuthorityResponse,
        alloc: std.mem.Allocator,
        request: AuthorityRequest,
        fence: @import("../storage/db/relational_integrity_topology_contract.zig").Fence,
        pending: @import("../storage/db/relational_integrity_generation_retirement.zig").Pending,
    ) !ParentActivationDecision {
        try self.validate(request);
        const acknowledging = request.receipt != null;
        const expected_state: State = if (acknowledging) .published else .activating;
        if (!request.include_plan or request.owner_group != fence.owner_group_id or
            (request.receipt != null and (request.receipt.?.state != .activating or request.receipt.?.owner_group != fence.owner_group_id)) or
            self.progress == null or self.progress.?.state != expected_state or
            self.job_json == null or !pending.fence.eql(fence) or fence.role != .truncate_parent)
            return error.RestoreActivationDecisionMissing;
        var job = try std.json.parseFromSlice(Job, alloc, self.job_json.?, .{});
        defer job.deinit();
        if (job.value.state != expected_state or job.value.revision != self.progress.?.revision or
            job.value.completed_owners != self.progress.?.completed_owners or
            !std.mem.eql(u8, &job.value.plan.id, &self.plan_id) or
            !std.mem.eql(u8, &job.value.plan_digest, &pending.plan_digest) or
            !std.mem.eql(u8, &job.value.plan_digest, &try job.value.plan.digest(alloc)))
            return error.RestoreActivationDecisionMissing;
        try job.value.plan.validate(alloc);
        const parent = for (job.value.plan.external_fk_parents) |candidate| {
            if (candidate.table.table_id == fence.namespace.table_id) break candidate;
        } else return error.RestoreActivationDecisionMissing;
        const planned_fence = for (parent.fences) |candidate| {
            if (candidate.owner_group_id == fence.owner_group_id) break candidate;
        } else return error.RestoreActivationDecisionMissing;
        if (!planned_fence.eql(fence) or parent.foreign_keys.len != pending.entryCount())
            return error.RestoreActivationDecisionMissing;
        for (parent.foreign_keys) |fk| {
            if (!pending.containsTransition(fk.child_table_id, fk.generation, fk.next_generation)) return error.RestoreActivationDecisionMissing;
            const reference: @import("../storage/db/relational_integrity_contract.zig").Reference = .{
                .child_table = fk.child_table_name,
                .child_key = "not used for generation match",
                .constraint_name = fk.constraint_name,
                .constraint_generation = fk.generation,
            };
            if (!pending.matchesReference(reference)) return error.RestoreActivationDecisionMissing;
        }
        if (acknowledging) {
            const retirement = @import("../storage/db/relational_integrity_generation_retirement.zig");
            const expected = try retirement.activationReceipt(fence, pending.plan_digest, retirement.publicationDigest(self.plan_id, pending.plan_digest));
            if (self.receipt == null or !std.mem.eql(u8, &self.receipt.?, &expected)) return error.RestoreActivationDecisionMissing;
        }
        return .{
            .metadata_group_id = self.metadata_group_id,
            .metadata_incarnation = self.metadata_incarnation,
            .metadata_epoch = self.metadata_epoch,
            .revision = self.progress.?.revision,
            .plan_digest = pending.plan_digest,
            .owner_group_id = fence.owner_group_id,
        };
    }
};

pub const ParentActivationDecision = struct {
    metadata_group_id: u64,
    metadata_incarnation: @import("incarnation.zig").MetadataClusterIncarnation,
    metadata_epoch: u64,
    revision: u64,
    plan_digest: Digest,
    owner_group_id: u64,
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
    action: enum { reserve, cancel_reservation, imported, validated, source_handoff_checked, begin_cutover, old_fenced, begin_activation, parent_activated, target_admissions_activated, publish, begin_cancel, canceled, finish_cancel, freeze_rewrite, rewrite_source_ready, parent_fenced },
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
        const needs_receipt = self.action == .imported or self.action == .validated or self.action == .source_handoff_checked or self.action == .old_fenced or self.action == .canceled or self.action == .parent_fenced or self.action == .parent_activated or self.action == .target_admissions_activated;
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

pub fn identityKey(buf: []u8, metadata_group_id: u64, kind: enum { table, group, old_table, parent_table, parent_group }, id: u64) ![]const u8 {
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
    const empty_handoff: GenerationHandoffRange = .{ .source_group_id = 301, .target_group_id = 401, .source_namespace = fence.namespace, .admissions = &.{}, .admissions_digest = try @import("../storage/portable_backup.zig").sourceGenerationAdmissionSummaryDigest(fence.namespace, &.{}), .retired_digest = @splat(8), .retired_count = 0 };
    var targets = [_]Target{.{ .source_table_id = 9, .empty_generation = true, .table = .{ .table_id = 10, .name = "docs", .schema_json = "{}" }, .ranges = &.{.{ .table_id = 10, .group_id = 401, .range_id = 401, .doc_identity_shard_id = 401, .doc_identity_range_id = 401, .start_key = "" }}, .generation_handoffs = &.{empty_handoff}, .replace = .{ .table = old, .ranges = &.{old_range}, .fences = &.{fence} } }};
    const plan: Plan = .{ .id = id, .cohort_digest = @splat(3), .targets = &targets };
    try plan.validate(alloc);
    targets[0].generation_handoffs = &.{};
    try std.testing.expectError(error.InvalidRestoreStaging, plan.validate(alloc));
    targets[0].generation_handoffs = &.{empty_handoff};
    var forged_handoff = empty_handoff;
    forged_handoff.admissions_digest[0] ^= 1;
    targets[0].generation_handoffs = &.{forged_handoff};
    try std.testing.expectError(error.InvalidRestoreStaging, plan.validate(alloc));
    forged_handoff = empty_handoff;
    forged_handoff.target_group_id = 402;
    targets[0].generation_handoffs = &.{forged_handoff};
    try std.testing.expectError(error.InvalidRestoreStaging, plan.validate(alloc));
    targets[0].generation_handoffs = &.{empty_handoff};
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
    targets[0].table.schema_json = "{}";
    targets[0].table.indexes_json = "{\"graph_idx\":{\"type\":\"graph\"}}";
    targets[0].replace.?.table.indexes_json = targets[0].table.indexes_json;
    try std.testing.expectError(error.InvalidRestoreStaging, plan.validate(alloc));
    targets[0].graph_retirement_digest = try graphRetirementDigest(alloc, 9, 10, targets[0].table.indexes_json);
    try plan.validate(alloc);
    const graph_scope = try ownerScope(alloc, plan, try plan.digest(alloc), targets[0], targets[0].ranges[0]);
    try std.testing.expectEqualDeep(targets[0].graph_retirement_digest, graph_scope.graph_retirement_digest);
    var wrong_digest = targets[0].graph_retirement_digest.?;
    wrong_digest[0] ^= 1;
    targets[0].graph_retirement_digest = wrong_digest;
    try std.testing.expectError(error.InvalidRestoreStaging, graphSealScopeForOldRange(alloc, plan, try plan.digest(alloc), targets[0], old_range));
    targets[0].graph_retirement_digest = try graphRetirementDigest(alloc, 9, 10, targets[0].table.indexes_json);
    try plan.validate(alloc);
    const seal_scope = (try graphSealScopeForOldRange(alloc, plan, try plan.digest(alloc), targets[0], old_range)).?;
    try std.testing.expect(seal_scope.fence.eql(fence));
    try std.testing.expectEqualDeep(targets[0].graph_retirement_digest.?, seal_scope.retirementDigest());
    try std.testing.expectEqualDeep(try plan.digest(alloc), seal_scope.plan_digest);
    targets[0].graph_retirement_digest = try graphRetirementDigest(alloc, 9, 11, targets[0].table.indexes_json);
    try std.testing.expectError(error.InvalidRestoreStaging, plan.validate(alloc));
}

test "relational integrity restore staging empty-generation graph declaration guard is independent of index position" {
    const alloc = std.testing.allocator;
    try std.testing.expect(!try hasGraphIndex(alloc, "{}"));
    try std.testing.expect(!try hasGraphIndex(alloc, "{\"ordered\":{\"type\":\"relational\"}}"));
    try std.testing.expect(try hasGraphIndex(alloc, "{\"ordered\":{\"type\":\"relational\"},\"links\":{\"type\":\"graph\"}}"));
    try std.testing.expectError(error.InvalidRestoreStaging, hasGraphIndex(alloc, "{\"links\":{}}"));
}

test "relational integrity restore staging pins an untouched FK parent and exact child generation" {
    const alloc = std.testing.allocator;
    const id = try idForAttempt(7, 1);
    const child_schema =
        \\{"version":1,"storage_mode":"relational","default_type":"row","foreign_keys":[{"name":"fk","child_columns":["id"],"parent_table":"parents","parent_columns":["id"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}}}
    ;
    const parent_schema =
        \\{"version":1,"storage_mode":"relational","default_type":"row","unique_constraints":[{"name":"pk","columns":["id"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}}}
    ;
    const old: records.TableRecord = .{ .table_id = 9, .name = "children", .schema_json = child_schema };
    const old_range: records.RangeRecord = .{ .table_id = 9, .group_id = 301, .start_key = "" };
    const old_fence: @import("../storage/db/relational_integrity_topology_contract.zig").Fence = .{ .role = .rewrite_source, .transition_id = 7, .attempt = 1, .owner_group_id = 301, .peer_group_id = 401, .namespace = .{ .table_id = 9, .shard_id = 301, .range_id = 301 }, .catalog_digest = @splat(4) };
    const empty_handoff: GenerationHandoffRange = .{ .source_group_id = 301, .target_group_id = 401, .source_namespace = old_fence.namespace, .admissions = &.{}, .admissions_digest = try @import("../storage/portable_backup.zig").sourceGenerationAdmissionSummaryDigest(old_fence.namespace, &.{}), .retired_digest = @splat(8), .retired_count = 0 };
    const target: Target = .{ .source_table_id = 9, .empty_generation = true, .table = .{ .table_id = 10, .name = "children", .schema_json = child_schema }, .ranges = &.{.{ .table_id = 10, .group_id = 401, .range_id = 401, .doc_identity_shard_id = 401, .doc_identity_range_id = 401, .start_key = "" }}, .generation_handoffs = &.{empty_handoff}, .replace = .{ .table = old, .ranges = &.{old_range}, .fences = &.{old_fence} } };
    const parent_range: records.RangeRecord = .{ .table_id = 11, .group_id = 501, .start_key = "" };
    const parent_fence: @import("../storage/db/relational_integrity_topology_contract.zig").Fence = .{ .role = .truncate_parent, .transition_id = 7, .attempt = 1, .owner_group_id = 501, .peer_group_id = 501, .namespace = .{ .table_id = 11, .shard_id = 501, .range_id = 501 }, .catalog_digest = @splat(6) };
    const next_generation = try plannedForeignGeneration(alloc, target, "fk");
    const parent: ExternalFkParent = .{ .table = .{ .table_id = 11, .name = "parents", .schema_json = parent_schema }, .ranges = &.{parent_range}, .fences = &.{parent_fence}, .foreign_keys = &.{.{ .child_table_id = 9, .child_table_name = "children", .constraint_name = "fk", .generation = @splat(5), .next_generation = next_generation }} };
    const plan: Plan = .{ .id = id, .cohort_digest = @splat(3), .targets = &.{target}, .external_fk_parents = &.{parent} };
    try plan.validate(alloc);
    // min_ranges is a floor, not the current split count. The complete
    // physical range set and exact owner fences are the retirement proof.
    var split_parent = parent;
    const split_ranges = [_]records.RangeRecord{
        .{ .table_id = 11, .group_id = 501, .start_key = "", .end_key = "m" },
        .{ .table_id = 11, .group_id = 502, .start_key = "m" },
    };
    var second_parent_fence = parent_fence;
    second_parent_fence.owner_group_id = 502;
    second_parent_fence.peer_group_id = 502;
    second_parent_fence.namespace.shard_id = 502;
    second_parent_fence.namespace.range_id = 502;
    const split_fences = [_]@TypeOf(parent_fence){ parent_fence, second_parent_fence };
    split_parent.ranges = &split_ranges;
    split_parent.fences = &split_fences;
    var split_plan = plan;
    split_plan.external_fk_parents = &.{split_parent};
    try split_plan.validate(alloc);
    const digest = try plan.digest(alloc);
    const pending_bytes = try @import("../storage/db/relational_integrity_generation_retirement.zig").encodePending(alloc, parent_fence, digest, &.{.{ .child_table_id = 9, .child_table_name = "children", .constraint_name = "fk", .generation = @splat(5), .next_generation = next_generation }});
    defer alloc.free(pending_bytes);
    const pending = try @import("../storage/db/relational_integrity_generation_retirement.zig").Pending.decode(pending_bytes);
    const request: AuthorityRequest = .{ .node_id = 7, .plan_id = id, .include_plan = true, .owner_group = 501 };
    var conflicting_request = request;
    conflicting_request.receipt = .{ .state = .cutover, .owner_group = 502 };
    try std.testing.expectError(error.InvalidArgument, conflicting_request.validate());
    const job_json = try std.json.Stringify.valueAlloc(alloc, Job{ .plan = plan, .plan_digest = digest, .state = .activating, .revision = 4 }, .{});
    defer alloc.free(job_json);
    var response: AuthorityResponse = .{ .node_id = 7, .plan_id = id, .metadata_group_id = 1, .metadata_incarnation = "0123456789abcdef0123456789abcdef".*, .metadata_epoch = 9, .progress = .{ .state = .activating, .revision = 4 }, .job_json = job_json };
    const decision = try response.parentActivationDecision(alloc, request, parent_fence, pending);
    try std.testing.expectEqual(@as(u64, 501), decision.owner_group_id);
    var ack_request = request;
    ack_request.receipt = .{ .state = .activating, .owner_group = 501 };
    const receipt = try @import("../storage/db/relational_integrity_generation_retirement.zig").activationReceipt(parent_fence, digest, @import("../storage/db/relational_integrity_generation_retirement.zig").publicationDigest(id, digest));
    response.receipt = receipt;
    try std.testing.expectError(error.RestoreActivationDecisionMissing, response.parentActivationDecision(alloc, ack_request, parent_fence, pending));
    const published_json = try std.json.Stringify.valueAlloc(alloc, Job{ .plan = plan, .plan_digest = digest, .state = .published, .revision = 5 }, .{});
    defer alloc.free(published_json);
    response.job_json = published_json;
    response.progress.?.state = .published;
    response.progress.?.revision = 5;
    _ = try response.parentActivationDecision(alloc, ack_request, parent_fence, pending);
    response.job_json = job_json;
    response.progress.?.state = .activating;
    response.progress.?.revision = 4;
    response.receipt = null;
    response.progress.?.state = .cutover;
    try std.testing.expectError(error.RestoreActivationDecisionMissing, response.parentActivationDecision(alloc, request, parent_fence, pending));
    response.progress.?.state = .activating;
    response.progress.?.revision = 5;
    try std.testing.expectError(error.RestoreActivationDecisionMissing, response.parentActivationDecision(alloc, request, parent_fence, pending));
    response.progress.?.revision = 4;
    var wrong_parent_fence = parent_fence;
    wrong_parent_fence.owner_group_id = 502;
    try std.testing.expectError(error.RestoreActivationDecisionMissing, response.parentActivationDecision(alloc, request, wrong_parent_fence, pending));
    const wrong_pending_bytes = try @import("../storage/db/relational_integrity_generation_retirement.zig").encodePending(alloc, parent_fence, @splat(8), &.{.{ .child_table_id = 9, .child_table_name = "children", .constraint_name = "fk", .generation = @splat(5), .next_generation = next_generation }});
    defer alloc.free(wrong_pending_bytes);
    const wrong_pending = try @import("../storage/db/relational_integrity_generation_retirement.zig").Pending.decode(wrong_pending_bytes);
    try std.testing.expectError(error.RestoreActivationDecisionMissing, response.parentActivationDecision(alloc, request, parent_fence, wrong_pending));
    const wrong_fk_bytes = try @import("../storage/db/relational_integrity_generation_retirement.zig").encodePending(alloc, parent_fence, digest, &.{.{ .child_table_id = 9, .child_table_name = "children", .constraint_name = "other", .generation = @splat(5), .next_generation = next_generation }});
    defer alloc.free(wrong_fk_bytes);
    const wrong_fk = try @import("../storage/db/relational_integrity_generation_retirement.zig").Pending.decode(wrong_fk_bytes);
    try std.testing.expectError(error.RestoreActivationDecisionMissing, response.parentActivationDecision(alloc, request, parent_fence, wrong_fk));
    const wrong_next_bytes = try @import("../storage/db/relational_integrity_generation_retirement.zig").encodePending(alloc, parent_fence, digest, &.{.{ .child_table_id = 9, .child_table_name = "children", .constraint_name = "fk", .generation = @splat(5), .next_generation = @splat(7) }});
    defer alloc.free(wrong_next_bytes);
    const wrong_next = try @import("../storage/db/relational_integrity_generation_retirement.zig").Pending.decode(wrong_next_bytes);
    try std.testing.expectError(error.RestoreActivationDecisionMissing, response.parentActivationDecision(alloc, request, parent_fence, wrong_next));
    var missing = plan;
    missing.external_fk_parents = &.{};
    try std.testing.expectError(error.RestoreDependencyMissing, missing.validate(alloc));
    var wrong = parent;
    wrong.foreign_keys = &.{.{ .child_table_id = 9, .child_table_name = "children", .constraint_name = "fk", .generation = @splat(5), .next_generation = @splat(7) }};
    var bad = plan;
    bad.external_fk_parents = &.{wrong};
    try std.testing.expectError(error.InvalidRestoreStaging, bad.validate(alloc));
    wrong.foreign_keys = &.{.{ .child_table_id = 9, .child_table_name = "children", .constraint_name = "other", .generation = @splat(5), .next_generation = next_generation }};
    bad.external_fk_parents = &.{wrong};
    try std.testing.expectError(error.InvalidRestoreStaging, bad.validate(alloc));
    wrong = parent;
    wrong.foreign_keys = &.{.{ .child_table_id = 9, .child_table_name = "renamed_children", .constraint_name = "fk", .generation = @splat(5), .next_generation = next_generation }};
    bad.external_fk_parents = &.{wrong};
    try std.testing.expectError(error.InvalidRestoreStaging, bad.validate(alloc));
    wrong = parent;
    var wrong_fence = parent_fence;
    wrong_fence.namespace.table_id = 12;
    wrong.fences = &.{wrong_fence};
    bad.external_fk_parents = &.{wrong};
    try std.testing.expectError(error.InvalidRestoreStaging, bad.validate(alloc));
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
