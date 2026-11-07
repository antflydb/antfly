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

//! Construct an FK-generation candidate from read-index owner identities and
//! one linearizable metadata topology. Begin rechecks every descriptor inside
//! the metadata transaction; child fence receipts attest the proposed AIC.
const std = @import("std");
const server_mod = @import("http_server.zig");
const operation = @import("antfly_local_sources").api_operation;
const records = @import("antfly_local_sources").common_topology_records;
const metadata = @import("../metadata/table_manager.zig");
const publication = @import("../metadata/fk_generation_publication.zig");
const topology = @import("antfly_local_sources").storage_db_relational_integrity_topology_contract;
const tables = @import("tables.zig");

/// The returned publication plan belongs to the caller's request arena, not
/// to the short-lived metadata snapshot used to derive it.
pub fn ownedTableForPlan(alloc: std.mem.Allocator, table: records.TableRecord) !records.TableRecord {
    return metadata.cloneTable(alloc, table);
}

const CatalogCut = struct { tables: []const records.TableRecord, ranges: []const records.RangeRecord };

pub fn rangesFor(alloc: std.mem.Allocator, snapshot: @import("../metadata/api.zig").AdminSnapshot, table_id: u64) ![]records.RangeRecord {
    return rangesForCut(alloc, .{ .tables = snapshot.tables, .ranges = snapshot.ranges }, table_id);
}

fn rangesForCut(alloc: std.mem.Allocator, snapshot: CatalogCut, table_id: u64) ![]records.RangeRecord {
    var selected: std.ArrayList(records.RangeRecord) = .empty;
    errdefer {
        for (selected.items) |range| metadata.freeRange(alloc, range);
        selected.deinit(alloc);
    }
    // Publication plans outlive the linearizable snapshot. Preserve every
    // range-owned field, not only the routing key used by ownerFences.
    for (snapshot.ranges) |range| if (range.table_id == table_id) {
        const owned = try metadata.cloneRange(alloc, range);
        selected.append(alloc, owned) catch |err| {
            metadata.freeRange(alloc, owned);
            return err;
        };
    };
    if (selected.items.len == 0 or selected.items.len > publication.max_owners) return error.InvalidGenerationPublication;
    metadata.sortKeyspaceRanges(records.RangeRecord, selected.items);
    try metadata.validateCompleteKeyspaceRanges(selected.items);
    return selected.toOwnedSlice(alloc);
}

pub fn ownerFences(server: *server_mod.ApiHttpServer, alloc: std.mem.Allocator, context: operation.RequestContext, id: publication.Id, table: records.TableRecord, ranges: []const records.RangeRecord, role: topology.Role) ![]topology.Fence {
    return ownerFencesWithCoverage(server, alloc, context, id, table, ranges, role, .legacy);
}

pub fn ownerFencesWithCoverage(server: *server_mod.ApiHttpServer, alloc: std.mem.Allocator, context: operation.RequestContext, id: publication.Id, table: records.TableRecord, ranges: []const records.RangeRecord, role: topology.Role, coverage: publication.CatalogCoverage) ![]topology.Fence {
    return ownerFencesCaptured(server, alloc, context, id, table, ranges, role, coverage, null);
}

fn ownerFencesCaptured(server: *server_mod.ApiHttpServer, alloc: std.mem.Allocator, context: operation.RequestContext, id: publication.Id, table: records.TableRecord, ranges: []const records.RangeRecord, role: topology.Role, coverage: publication.CatalogCoverage, captures: ?*RetirementCaptures) ![]topology.Fence {
    const reads = server.table_reads orelse return error.UnsupportedOperation;
    const fences = try alloc.alloc(topology.Fence, ranges.len);
    for (ranges, fences) |range, *fence| {
        try context.ensureActive();
        const Native = struct { namespace: @import("antfly_local_sources").storage_db_doc_identity.Namespace, catalog_digest: [32]u8, next_epoch: u64, catalog_checks_identity_supported: bool = false };
        const native: Native = if (captures != null and table.sql_persistence != null) blk: {
            const captured = try captures.?.capture(table, range);
            break :blk .{ .namespace = captured.identity.namespace, .catalog_digest = captured.identity.catalog_digest, .next_epoch = captured.identity.next_epoch, .catalog_checks_identity_supported = captured.identity.catalog_checks_identity_supported };
        } else blk: {
            var response = (try reads.lookup(alloc, table.name, range.start_key, .{
                .relational_topology_json = "{\"mode\":\"identity\"}",
                .execution_deadline_ns = (try context.platformDeadline()).deadline_ns,
                .cancellation = context.cancellation,
            }, .read_index)) orelse return error.TableNotFound;
            defer response.deinit(alloc);
            break :blk try std.json.parseFromSliceLeaky(Native, alloc, response.json, .{ .ignore_unknown_fields = true });
        };
        if (coverage == .checks_v1 and !native.catalog_checks_identity_supported) return error.TableTopologyProtocolUpgradeRequired;
        if (native.namespace.table_id != table.table_id or
            native.namespace.shard_id != metadata.rangeDocIdentityShardId(range) or
            native.namespace.range_id != metadata.rangeDocIdentityRangeId(range)) return error.TableGenerationChanged;
        fence.* = .{
            .transition_id = std.mem.readInt(u64, id[0..8], .little),
            .attempt = std.mem.readInt(u64, id[8..16], .little),
            .admission_epoch = native.next_epoch,
            .owner_group_id = range.group_id,
            .peer_group_id = range.group_id,
            .role = role,
            .namespace = native.namespace,
            .catalog_digest = native.catalog_digest,
        };
    }
    return fences;
}

fn childCatalogB64(server: *server_mod.ApiHttpServer, alloc: std.mem.Allocator, table: records.TableRecord) ![]const u8 {
    var response = (try (server.table_reads orelse return error.UnsupportedOperation).integrityCatalog(alloc, table.name)) orelse return error.IntegrityCatalogUnavailable;
    defer response.deinit(alloc);
    const Envelope = struct { catalog: []const u8, schema_version: u32, table_id: []const u8 };
    const observed = try std.json.parseFromSliceLeaky(Envelope, alloc, response.json, .{ .ignore_unknown_fields = true });
    if ((std.fmt.parseInt(u64, observed.table_id, 10) catch return error.InvalidIntegrityCatalog) != table.table_id or
        observed.schema_version != try tables.schemaVersion(table.schema_json)) return error.CatalogGenerationChanged;
    const size = std.base64.standard.Decoder.calcSizeForSlice(observed.catalog) catch return error.InvalidIntegrityCatalog;
    if (size == 0 or size > @import("antfly_local_sources").storage_db_relational_integrity_catalog.max_catalog_bytes) return error.InvalidIntegrityCatalog;
    return alloc.dupe(u8, observed.catalog);
}

pub fn build(server: *server_mod.ApiHttpServer, alloc: std.mem.Allocator, context: operation.RequestContext, identity: ?server_mod.AuthenticatedIdentity, before: records.TableRecord, proposed_schema_json: []const u8) !publication.Plan {
    return buildWithConstraintNameGuard(server, alloc, context, identity, before, proposed_schema_json, null);
}

pub fn buildWithConstraintNameGuard(server: *server_mod.ApiHttpServer, alloc: std.mem.Allocator, context: operation.RequestContext, identity: ?server_mod.AuthenticatedIdentity, before: records.TableRecord, proposed_schema_json: []const u8, guard: ?@import("antfly_local_sources").system_catalog_domain.RelationMutationGuard) !publication.Plan {
    return buildControlled(server, alloc, context, identity, before, proposed_schema_json, guard, null);
}

pub fn buildSqlDrop(server: *server_mod.ApiHttpServer, alloc: std.mem.Allocator, context: operation.RequestContext, identity: ?server_mod.AuthenticatedIdentity, before: records.TableRecord, guard: @import("antfly_local_sources").system_catalog_domain.RelationMutationGuard) !publication.Plan {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, before.schema_json, .{ .parse_numbers = false });
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidGenerationPublication;
    _ = parsed.value.object.swapRemove("foreign_keys");
    const target = try std.json.Stringify.valueAlloc(alloc, parsed.value, .{});
    defer alloc.free(target);
    return buildControlled(server, alloc, context, identity, before, target, null, guard);
}

fn buildControlled(server: *server_mod.ApiHttpServer, alloc: std.mem.Allocator, context: operation.RequestContext, identity: ?server_mod.AuthenticatedIdentity, before: records.TableRecord, proposed_schema_json: []const u8, guard: ?@import("antfly_local_sources").system_catalog_domain.RelationMutationGuard, sql_drop: ?@import("antfly_local_sources").system_catalog_domain.RelationMutationGuard) !publication.Plan {
    try context.ensureActive();
    var snapshot = (try server.source.linearizableSnapshot(context)) orelse return error.MetadataCapabilityUnavailable;
    defer server.source.freeAdminSnapshot(&snapshot);
    return buildFromCut(server, alloc, context, identity, .{ .tables = snapshot.tables, .ranges = snapshot.ranges }, before, proposed_schema_json, guard, sql_drop, null);
}

const retirement_preparation = @import("table_retirement_preparation.zig");
const RetirementCaptures = struct {
    const Key = struct { table: u64, owner_group: u64, namespace: @import("antfly_local_sources").storage_db_doc_identity_namespace.Namespace };
    const Facts = struct {
        identity: topology.Identity,
        native_binding: records.RelationPersistence.NativeBinding,
        boot_generation: u64,
        metadata_incarnation: [32]u8,
        name_digest: [32]u8,
        schema_digest: [32]u8,
        indexes_digest: [32]u8,
    };
    server: *server_mod.ApiHttpServer,
    alloc: std.mem.Allocator,
    context: operation.RequestContext,
    head: @import("../metadata/table_retirement_ledger.zig").Head,
    entries: std.AutoHashMapUnmanaged(Key, Facts) = .empty,
    root_catalog: ?struct { key: Key, base64: []u8 } = null,
    fn deinit(self: *@This()) void {
        if (self.root_catalog) |root| self.alloc.free(root.base64);
        self.entries.deinit(self.alloc);
    }
    fn digest(bytes: []const u8) [32]u8 {
        var result: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(bytes, &result, .{});
        return result;
    }
    fn capture(self: *@This(), table: records.TableRecord, range: records.RangeRecord) !Facts {
        return self.captureInternal(table, range, false);
    }
    fn captureInternal(self: *@This(), table: records.TableRecord, range: records.RangeRecord, keep_root_catalog: bool) !Facts {
        try self.context.ensureActive();
        if (range.table_id != table.table_id) return error.IdentityNamespaceMismatch;
        const descriptor = table.sql_persistence orelse return error.InvalidSqlRelationPersistence;
        const namespace: @import("antfly_local_sources").storage_db_doc_identity_namespace.Namespace = .{ .table_id = table.table_id, .shard_id = metadata.rangeDocIdentityShardId(range), .range_id = metadata.rangeDocIdentityRangeId(range) };
        const key: Key = .{ .table = table.table_id, .owner_group = range.group_id, .namespace = namespace };
        if (keep_root_catalog and table.table_id != self.head.lookup.table) return error.InvalidCatalogRecord;
        if (keep_root_catalog) if (self.root_catalog) |root| if (!std.meta.eql(root.key, key)) return error.InvalidCatalogRecord;
        const existing = self.entries.get(key);
        if (existing) |facts| {
            if (!facts.native_binding.descriptor.eql(descriptor) or !std.mem.eql(u8, &facts.name_digest, &digest(table.name)) or
                !std.mem.eql(u8, &facts.schema_digest, &digest(table.schema_json)) or !std.mem.eql(u8, &facts.indexes_digest, &digest(table.indexes_json))) return error.CatalogGenerationChanged;
            if (!keep_root_catalog or self.root_catalog != null) return facts;
        } else if (self.entries.count() >= publication.max_owners) return error.InvalidGenerationPublication;
        // The caller is normally a preparation arena. Transport and parsing
        // must be reclaimed independently for every owner, before map growth.
        var temporary = std.heap.ArenaAllocator.init(self.server.alloc);
        defer temporary.deinit();
        const scratch = temporary.allocator();
        const request: retirement_preparation.Request = .{ .lookup = self.head.lookup, .expected_revision = self.head.revision, .effects = self.head.effects, .scope_parent = self.head.scope.parent, .metadata_incarnation = self.head.incarnation, .table_id = table.table_id, .owner_group_id = range.group_id, .namespace = namespace };
        try request.requireReservedHead(self.head);
        try self.context.ensureActive();
        const normalized = try self.context.platformDeadline();
        const local_owner = self.server.cfg.table_retirement_preparation != null and
            try @import("group_work_ownership.zig").mayCoordinate(self.server.cfg.relational_maintenance_leadership, scratch, table.table_id, range.group_id, descriptor, .retirement, normalized);
        // Both pre-seal and sealed cleanup use the exact captured store identity.
        const bytes = if (local_owner)
            try self.server.prepareTableRetirementOwner(scratch, range.group_id, table.name, request, normalized)
        else remote: {
            const uri = (try self.server.source.nativeOwnerDataUri(scratch, descriptor.owner_node_id, descriptor.owner_store_id, normalized)) orelse return error.SqlRelationOwnerUnavailable;
            defer scratch.free(uri);
            var client = @import("http_client.zig").ApiHttpClient.init(scratch, self.server.cfg.session_executor orelse return error.HttpExecutorUnavailable);
            _ = client.withInternalServiceAuth(self.server.cfg.internal_service_secret, self.server.cfg.internal_service_issuer);
            break :remote try client.fetchTableRetirementPreparation(uri, range.group_id, table.name, request, normalized);
        };
        defer scratch.free(bytes);
        if (bytes.len == 0 or bytes.len > retirement_preparation.max_response_bytes) return error.InvalidCatalogRecord;
        const response = try std.json.parseFromSliceLeaky(retirement_preparation.Response, scratch, bytes, .{});
        try response.requireFor(request, descriptor);
        const facts: Facts = .{ .identity = response.identity, .native_binding = response.native_binding, .boot_generation = response.boot_generation, .metadata_incarnation = response.metadata_incarnation, .name_digest = digest(table.name), .schema_digest = digest(table.schema_json), .indexes_digest = digest(table.indexes_json) };
        if (existing) |previous| if (!std.meta.eql(previous, facts)) return error.CatalogGenerationChanged;
        var root_base64: ?[]u8 = null;
        errdefer if (root_base64) |owned| self.alloc.free(owned);
        if (keep_root_catalog) {
            const Envelope = struct { catalog: []const u8, schema_version: u32, table_id: []const u8 };
            const envelope = try std.json.parseFromSliceLeaky(Envelope, scratch, response.catalog_json, .{ .ignore_unknown_fields = true });
            if ((std.fmt.parseInt(u64, envelope.table_id, 10) catch return error.InvalidIntegrityCatalog) != table.table_id or envelope.schema_version != try tables.schemaVersion(table.schema_json)) return error.CatalogGenerationChanged;
            root_base64 = try self.alloc.dupe(u8, envelope.catalog);
        }
        try self.entries.put(self.alloc, key, facts);
        if (root_base64) |owned| {
            self.root_catalog = .{ .key = key, .base64 = owned };
            root_base64 = null;
        }
        return facts;
    }
    fn catalog(self: *@This(), table: records.TableRecord, ranges: []const records.RangeRecord) ![]const u8 {
        if (ranges.len == 0) return error.GenerationAdmissionPending;
        _ = try self.captureInternal(table, ranges[0], true);
        // This copy belongs to the resulting Plan and survives cache teardown.
        return try self.alloc.dupe(u8, (self.root_catalog orelse return error.InvalidCatalogRecord).base64);
    }
};

const RetirementRoot = struct {
    id: publication.Id,
    effects: @import("../metadata/table_retirement_manifest.zig").Reference,
    retiring_parents: []const @import("../metadata/scope_retirement_ledger.zig").Child = &.{},
    captures: ?*RetirementCaptures = null,
    fn parent(self: @This(), id: u64) ?@import("../metadata/scope_retirement_ledger.zig").Child {
        for (self.retiring_parents) |retiring| if (retiring.table == id) return retiring;
        return null;
    }
};

/// Plan only the accepted root's immutable source and outgoing parents. No
/// whole-catalog snapshot is needed; seal rechecks this cut in the root txn.
/// Allocations belong to the caller's preparation arena.
pub fn buildTableRetirement(server: *server_mod.ApiHttpServer, alloc: std.mem.Allocator, context: operation.RequestContext, id: publication.Id, manifest: @import("../metadata/table_retirement_manifest.zig").Manifest) !publication.Plan {
    return buildTableRetirementImpl(server, alloc, context, id, manifest, null);
}

pub fn buildReservedTableRetirement(server: *server_mod.ApiHttpServer, alloc: std.mem.Allocator, context: operation.RequestContext, head: @import("../metadata/table_retirement_ledger.zig").Head, manifest: @import("../metadata/table_retirement_manifest.zig").Manifest) !publication.Plan {
    var captures: RetirementCaptures = .{ .server = server, .alloc = alloc, .context = context, .head = head };
    defer captures.deinit();
    return buildTableRetirementImpl(server, alloc, context, head.lookup.id, manifest, &captures);
}

fn buildTableRetirementImpl(server: *server_mod.ApiHttpServer, alloc: std.mem.Allocator, context: operation.RequestContext, id: publication.Id, manifest: @import("../metadata/table_retirement_manifest.zig").Manifest, captures: ?*RetirementCaptures) !publication.Plan {
    try context.ensureActive();
    try manifest.validate(alloc);
    if (manifest.view_backings.len != 0) return error.UnsupportedOperation;
    var root_schema = try @import("antfly_local_sources").schema_mod.parseValidatedTableSchema(alloc, if (manifest.root.schema_json.len == 0) "{}" else manifest.root.schema_json);
    defer root_schema.deinit(alloc);
    if (root_schema.storage_mode == .document) {
        const document_ranges = try rangesForCut(alloc, .{ .tables = &.{manifest.root}, .ranges = manifest.root_ranges }, manifest.root.table_id);
        const plan: publication.Plan = .{
            .historical_reference_coverage = true,
            .catalog_coverage = .checks_v1,
            .table_retirement = try manifest.reference(),
            .sql_drop = try manifest.guard.clone(alloc),
            .id = id,
            .child_before = try ownedTableForPlan(alloc, manifest.root),
            .child_after = try ownedTableForPlan(alloc, manifest.root),
            .child_catalog_before_b64 = "",
            .parents = &.{},
            .child_ranges = document_ranges,
            .child_fences = try ownerFencesCaptured(server, alloc, context, id, manifest.root, document_ranges, .document_retirement_source, .checks_v1, captures),
        };
        try plan.validate(alloc);
        const physical = @import("../metadata/table_retirement_physical.zig");
        try physical.requireManifest(alloc, .{ .group = manifest.metadata_group_id, .table = manifest.root.table_id, .id = id }, try physical.prepare(alloc, alloc, plan), manifest);
        return plan;
    }
    var selected: std.ArrayList(records.TableRecord) = .empty;
    var ranges: std.ArrayList(records.RangeRecord) = .empty;
    try selected.append(alloc, manifest.root);
    try ranges.appendSlice(alloc, manifest.root_ranges);
    for (manifest.survivors) |pair| try selected.append(alloc, pair.before);
    try ranges.appendSlice(alloc, manifest.survivor_ranges);
    for (manifest.external_parents) |parent| {
        try selected.append(alloc, parent.table);
        try ranges.appendSlice(alloc, parent.ranges);
    }
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, if (manifest.root.schema_json.len == 0) "{}" else manifest.root.schema_json, .{ .parse_numbers = false });
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidGenerationPublication;
    _ = parsed.value.object.swapRemove("foreign_keys");
    const target = try std.json.Stringify.valueAlloc(alloc, parsed.value, .{});
    const plan = try buildFromCut(server, alloc, context, null, .{ .tables = selected.items, .ranges = ranges.items }, manifest.root, target, null, manifest.guard, .{ .id = id, .effects = try manifest.reference(), .retiring_parents = manifest.retiring_parents, .captures = captures });
    const physical = @import("../metadata/table_retirement_physical.zig");
    const prepared = try physical.prepare(alloc, alloc, plan);
    try physical.requireManifest(alloc, .{ .group = manifest.metadata_group_id, .table = manifest.root.table_id, .id = id }, prepared, manifest);
    return plan;
}

fn buildFromCut(server: *server_mod.ApiHttpServer, alloc: std.mem.Allocator, context: operation.RequestContext, identity: ?server_mod.AuthenticatedIdentity, snapshot: CatalogCut, before: records.TableRecord, proposed_schema_json: []const u8, guard: ?@import("antfly_local_sources").system_catalog_domain.RelationMutationGuard, sql_drop: ?@import("antfly_local_sources").system_catalog_domain.RelationMutationGuard, root: ?RetirementRoot) !publication.Plan {
    const current = for (snapshot.tables) |table| {
        if (table.table_id == before.table_id and std.mem.eql(u8, table.name, before.name)) break table;
    } else return error.CatalogGenerationChanged;
    if (!metadata.tableDefinitionsEqual(current, before)) return error.CatalogGenerationChanged;
    const after = if (sql_drop != null)
        try tables.prepareSqlDropPublicationRecord(alloc, &current, proposed_schema_json)
    else
        try tables.prepareForeignKeyPublicationRecord(alloc, &current, proposed_schema_json);
    const capture = if (root) |accepted| accepted.captures else null;
    const catalog_b64 = if (capture != null and current.sql_persistence != null)
        try capture.?.catalog(current, try rangesForCut(alloc, snapshot, current.table_id))
    else
        try childCatalogB64(server, alloc, current);
    const derived = try publication.deriveTransitions(alloc, current.table_id, current.name, current.schema_json, after.schema_json, catalog_b64);
    if (derived.len == 0 and sql_drop == null) return error.InvalidGenerationPublication;
    var id: publication.Id = undefined;
    if (root) |accepted| {
        id = accepted.id;
    } else {
        const io = server.restore_job_store.io orelse return error.AsyncRestoreUnavailable;
        while (true) {
            try io.randomSecure(&id);
            if (std.mem.readInt(u64, id[0..8], .little) != 0 and std.mem.readInt(u64, id[8..16], .little) != 0) break;
        }
    }
    const child_ranges = try rangesForCut(alloc, snapshot, current.table_id);
    const retiring_self = if (root) |accepted| accepted.parent(current.table_id) != null else false;
    const self_parent = for (derived) |item| {
        if (!retiring_self and std.mem.eql(u8, item.parent_table_name, current.name)) break true;
    } else false;
    const child_fences = try ownerFencesCaptured(server, alloc, context, id, current, child_ranges, if (self_parent) .child_generation_dual else .child_generation_source, .checks_v1, capture);
    var parents: std.ArrayList(publication.Parent) = .empty;
    for (derived) |item| {
        const parent_table = for (snapshot.tables) |table| {
            if (std.mem.eql(u8, table.name, item.parent_table_name)) break table;
        } else return error.ForeignKeyParentTableNotFound;
        if (parent_table.storage_migration != null or
            parent_table.relational_retirement_json.len != 0 or parent_table.restore_backup_id.len != 0) return error.TableTransitionActive;
        var found: ?*publication.Parent = null;
        for (parents.items) |*parent| if (parent.table.table_id == parent_table.table_id) {
            found = parent;
            break;
        };
        if (found == null) {
            // A child owner may retire its own outgoing references without
            // parent administration. The DROP-only plan validates every edge
            // against the exact child schema and permits no successor grant.
            // All ordinary FK schema changes retain parent authorization.
            if (sql_drop == null) {
                const names = try server.logicalTableNamesInArena(alloc, context, &.{parent_table.name});
                if (names.len != 1 or !try server_mod.tablePermissionCurrentlyAllowed(identity, names[0], .admin) or
                    try server_mod.resolveEffectiveRowFilterJson(alloc, identity, names[0]) != null) return error.Forbidden;
            }
            const dual = parent_table.table_id == current.table_id;
            const parent_ranges = if (dual) child_ranges else try rangesForCut(alloc, snapshot, parent_table.table_id);
            const retirement = if (root) |accepted| accepted.parent(parent_table.table_id) else null;
            try parents.append(alloc, .{
                .scope_retirement = retirement,
                .table = try ownedTableForPlan(alloc, parent_table),
                .ranges = parent_ranges,
                .fences = if (retirement != null) &.{} else if (dual) child_fences else try ownerFencesCaptured(server, alloc, context, id, parent_table, parent_ranges, .child_generation_parent, .checks_v1, capture),
                .transitions = &.{},
            });
            found = &parents.items[parents.items.len - 1];
        }
        const existing = found.?.transitions;
        const next = try alloc.alloc(publication.Transition, existing.len + 1);
        @memcpy(next[0..existing.len], existing);
        next[existing.len] = item.transition;
        found.?.transitions = next;
    }
    std.mem.sort(publication.Parent, parents.items, {}, struct {
        fn less(_: void, lhs: publication.Parent, rhs: publication.Parent) bool {
            return lhs.table.table_id < rhs.table.table_id;
        }
    }.less);
    for (parents.items) |*parent| {
        const sorted = try alloc.dupe(publication.Transition, parent.transitions);
        std.mem.sort(publication.Transition, sorted, {}, struct {
            fn less(_: void, lhs: publication.Transition, rhs: publication.Transition) bool {
                return std.mem.lessThan(u8, lhs.constraint_name, rhs.constraint_name);
            }
        }.less);
        parent.transitions = sorted;
    }
    const result: publication.Plan = .{
        .historical_reference_coverage = true,
        .table_retirement = if (root) |accepted| accepted.effects else null,
        .id = id,
        .child_before = try ownedTableForPlan(alloc, current),
        .child_after = after,
        .child_catalog_before_b64 = catalog_b64,
        .child_ranges = child_ranges,
        .child_fences = child_fences,
        .parents = try parents.toOwnedSlice(alloc),
        .constraint_name_guard = if (guard) |value| if (value.object_renames.len == 0) try value.clone(alloc) else null else null,
        .object_rename_guard = if (guard) |value| if (value.object_renames.len != 0) try value.clone(alloc) else null else null,
        .sql_drop = if (sql_drop) |value| try value.clone(alloc) else null,
        .catalog_coverage = .checks_v1,
    };
    try result.validate(alloc);
    return result;
}

test "FK plan table and range descriptors survive snapshot release" {
    const alloc = std.testing.allocator;
    const source_table = try metadata.cloneTable(alloc, .{
        .table_id = 7,
        .name = "parent",
        .schema_json = "{\"version\":1}",
        .indexes_json = "{\"parent_key\":{}}",
    });
    const owned_table = try ownedTableForPlan(alloc, source_table);
    defer metadata.freeTable(alloc, owned_table);
    metadata.freeTable(alloc, source_table);
    try std.testing.expectEqualStrings("parent", owned_table.name);
    try std.testing.expectEqualStrings("{\"version\":1}", owned_table.schema_json);
    try std.testing.expectEqualStrings("{\"parent_key\":{}}", owned_table.indexes_json);

    const source_range = try metadata.cloneRange(alloc, .{
        .group_id = 91,
        .range_id = 91,
        .table_id = 7,
        .start_key = "",
        .restore_backup_id = "snapshot-backup",
    });
    var snapshot_ranges = [_]records.RangeRecord{source_range};
    const snapshot: @import("../metadata/api.zig").AdminSnapshot = .{
        .status = undefined,
        .tables = &.{},
        .ranges = &snapshot_ranges,
        .stores = &.{},
        .placement_intents = &.{},
        .split_transitions = &.{},
        .merge_transitions = &.{},
    };
    const owned = try rangesFor(alloc, snapshot, 7);
    defer {
        for (owned) |range| metadata.freeRange(alloc, range);
        alloc.free(owned);
    }
    metadata.freeRange(alloc, source_range);
    try std.testing.expectEqualStrings("snapshot-backup", owned[0].restore_backup_id);
    try std.testing.expectEqual(@as(u64, 91), owned[0].group_id);
}

test "SQL relation lifetime reserved native planner captures once without generic reads or live connection owner" {
    const local = @import("antfly_local_sources");
    const a = std.testing.allocator;
    const descriptor: records.RelationPersistence.Descriptor = .{ .kind = .unlogged, .owner_node_id = 9, .owner_store_id = 11, .generation_instance = @splat(3), .data_generation = 1 };
    const table: records.TableRecord = .{ .table_id = 7, .name = "table:seven", .schema_json = "{\"version\":0}", .sql_persistence = descriptor };
    const range: records.RangeRecord = .{ .group_id = 72, .table_id = 7, .doc_identity_shard_id = 172, .doc_identity_range_id = 173, .start_key = "" };
    const head: @import("../metadata/table_retirement_ledger.zig").Head = .{ .lookup = .{ .group = 2188, .table = 7, .id = @splat(1) }, .incarnation = "11111111111111111111111111111111".*, .effects = .{ .digest = @splat(2), .root_table = 7, .survivors = 0, .counters = 0, .views = 0 }, .scope = .{ .database = 1, .namespace = 2, .creator = .bootstrap }, .participant_count = 1, .range_count = 1, .participants_digest = @splat(3) };
    var prepared = try local.storage_db_relational_integrity_catalog.prepare(a, null, @splat(1), 0, @splat(2), &.{});
    defer prepared.deinit();
    const base64 = try a.alloc(u8, std.base64.standard.Encoder.calcSize(prepared.value.len));
    defer a.free(base64);
    _ = std.base64.standard.Encoder.encode(base64, prepared.value);
    const envelope = try std.json.Stringify.valueAlloc(a, .{ .catalog = base64, .schema_version = @as(u32, 0), .table_id = "7" }, .{});
    defer a.free(envelope);
    var digest: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(prepared.value, &digest, .{});
    const Spy = struct {
        catalog: []const u8,
        digest: [32]u8,
        descriptor: records.RelationPersistence.Descriptor,
        captures: usize = 0,
        generic_reads: usize = 0,
        bad_echo: bool = false,
        fn localOwner(_: *anyopaque, _: std.mem.Allocator, selected: @import("group_work_ownership.zig").NativeSelection, _: operation.RequestContext) anyerror!bool {
            if (selected.descriptor.owner_node_id != 9 or selected.descriptor.owner_store_id != 11 or selected.table_id != 7 or selected.purpose != .retirement) return error.InvalidCatalogRecord;
            return true;
        }
        fn raft(_: *anyopaque, _: u64) bool {
            return false;
        }
        fn status(_: *anyopaque) anyerror!@import("../metadata/api.zig").MetadataStatus {
            return error.TestUnexpectedStatusRead;
        }
        fn lookup(raw: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: local.storage_db_types.LookupOptions, _: local.storage_read_consistency.ReadConsistency) anyerror!?local.api_table_read_source.LookupResponse {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.generic_reads += 1;
            return error.TestUnexpectedGenericRead;
        }
        fn scan(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: []const u8, _: local.storage_db_types.ScanOptions, _: local.storage_read_consistency.ReadConsistency) anyerror!?local.api_table_read_source.ScanResponse {
            return error.TestUnexpectedGenericRead;
        }
        fn query(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: local.storage_db_types.SearchRequest, _: local.storage_read_consistency.ReadConsistency) anyerror!?local.api_query_response.QueryResponse {
            return error.TestUnexpectedGenericRead;
        }
        fn capture(raw: *anyopaque, alloc: std.mem.Allocator, group: u64, name: []const u8, request: retirement_preparation.Request, context: operation.RequestContext) anyerror![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.captures += 1;
            try std.testing.expect(context.sql_relation_owner == null);
            try std.testing.expectEqual(@as(u64, 72), group);
            try std.testing.expectEqualStrings("table:seven", name);
            const response: retirement_preparation.Response = .{ .request = request, .identity = .{ .namespace = request.namespace, .catalog_digest = self.digest, .next_epoch = 1, .catalog_checks_identity_supported = true }, .native_binding = .{ .descriptor = self.descriptor, .native_instance = @splat(4), .table_id = 7, .shard_id = 172, .range_id = 173, .physical_root_incarnation = 1, .schema_version = 0, .schema_digest = @splat(2), .catalog_generation = 1, .catalog_fingerprint = @splat(6) }, .boot_generation = 1, .metadata_incarnation = request.metadata_incarnation, .catalog_json = self.catalog };
            var result = response;
            if (self.bad_echo) result.request.expected_revision += 1;
            return result.encodeAlloc(alloc);
        }
    };
    var spy: Spy = .{ .catalog = envelope, .digest = digest, .descriptor = descriptor };
    const reader: local.api_table_read_source.TableReadSource = .{ .ptr = &spy, .vtable = &.{ .lookup = Spy.lookup, .scan = Spy.scan, .query = Spy.query } };
    var server = server_mod.ApiHttpServer.init(a, .{ .deployment_mode = .standalone, .relational_maintenance_leadership = .{ .ptr = &spy, .vtable = &.{ .is_local_leader = Spy.raft, .native_local = Spy.localOwner } }, .table_retirement_preparation = .{ .ptr = &spy, .prepare_fn = Spy.capture } }, .{ .ptr = &spy, .vtable = &.{ .status = Spy.status } }, reader, null);
    defer server.deinit();
    const context: operation.RequestContext = .{ .setting_admin = true, .schema_validation_authority = true, .fk_generation_publication_authority = true };
    {
        var captures: RetirementCaptures = .{ .server = &server, .alloc = a, .context = context, .head = head };
        defer captures.deinit();
        const catalog = try captures.catalog(table, &.{range});
        defer a.free(catalog);
        try std.testing.expectEqualStrings(base64, catalog);
        const fences = try ownerFencesCaptured(&server, a, context, head.lookup.id, table, &.{range}, .child_generation_source, .checks_v1, &captures);
        defer a.free(fences);
        try std.testing.expectEqual(@as(usize, 1), spy.captures);
        try std.testing.expectEqual(@as(u64, 72), fences[0].owner_group_id);
        try std.testing.expectEqual(@as(u64, 172), fences[0].namespace.shard_id);
        try std.testing.expectEqual(@as(usize, 0), spy.generic_reads);
    }
    const owner = server.cfg.relational_maintenance_leadership;
    server.cfg.relational_maintenance_leadership = null;
    {
        var captures: RetirementCaptures = .{ .server = &server, .alloc = a, .context = context, .head = head };
        defer captures.deinit();
        try std.testing.expectError(error.SqlRelationOwnerUnavailable, captures.capture(table, range));
        try std.testing.expectEqual(@as(usize, 1), spy.captures);
    }
    server.cfg.relational_maintenance_leadership = owner;
    spy.bad_echo = true;
    {
        var captures: RetirementCaptures = .{ .server = &server, .alloc = a, .context = context, .head = head };
        defer captures.deinit();
        try std.testing.expectError(error.InvalidCatalogRecord, captures.capture(table, range));
    }
    server.cfg.table_retirement_preparation = null;
    {
        var captures: RetirementCaptures = .{ .server = &server, .alloc = a, .context = context, .head = head };
        defer captures.deinit();
        try std.testing.expectError(error.SqlRelationOwnerUnavailable, captures.capture(table, range));
        try std.testing.expectEqual(@as(usize, 0), spy.generic_reads);
    }
}

test "SQL relation lifetime retirement capture metadata cache releases many owner catalogs with an arena caller" {
    const local = @import("antfly_local_sources");
    const a = std.testing.allocator;
    var fixture_region = std.heap.ArenaAllocator.init(a);
    defer fixture_region.deinit();
    const fixture = fixture_region.allocator();
    const native_catalog = local.storage_db_relational_integrity_catalog;
    const definitions = try fixture.alloc(native_catalog.Definition, 256);
    for (definitions, 0..) |*definition, i| {
        const name = try std.fmt.allocPrint(fixture, "u_{d:0>3}_abcdefghijklmnopqrstuvwxyz_abcdefghijklmnopqrstuvwxyz_abcdefghijklmnopqrstuvwxyz_abcdefghijklmnopqrstuvwxyz", .{i});
        var fingerprint: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(name, &fingerprint, .{});
        definition.* = .{ .kind = .unique, .name = name, .fingerprint = fingerprint };
    }
    var prepared = try native_catalog.prepare(a, null, @splat(1), 0, @splat(2), definitions);
    defer prepared.deinit();
    try std.testing.expect(prepared.value.len > 32768);
    const base64 = try fixture.alloc(u8, std.base64.standard.Encoder.calcSize(prepared.value.len));
    _ = std.base64.standard.Encoder.encode(base64, prepared.value);
    const envelope = try std.json.Stringify.valueAlloc(fixture, .{ .catalog = base64, .schema_version = @as(u32, 0), .table_id = "7" }, .{});
    var catalog_digest: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(prepared.value, &catalog_digest, .{});
    const descriptor: records.RelationPersistence.Descriptor = .{ .kind = .unlogged, .owner_node_id = 9, .owner_store_id = 11, .generation_instance = @splat(3), .data_generation = 1 };
    const table: records.TableRecord = .{ .table_id = 7, .name = "table:seven", .schema_json = "{\"version\":0}", .sql_persistence = descriptor };
    const root_range: records.RangeRecord = .{ .group_id = 72, .table_id = 7, .doc_identity_shard_id = 172, .doc_identity_range_id = 173, .start_key = "" };
    const head: @import("../metadata/table_retirement_ledger.zig").Head = .{ .lookup = .{ .group = 2188, .table = 7, .id = @splat(1) }, .incarnation = "11111111111111111111111111111111".*, .effects = .{ .digest = @splat(2), .root_table = 7, .survivors = 0, .counters = 0, .views = 0 }, .scope = .{ .database = 1, .namespace = 2, .creator = .bootstrap }, .participant_count = 1, .range_count = 513, .participants_digest = @splat(3) };
    const Spy = struct {
        catalog: []const u8,
        digest: [32]u8,
        descriptor: records.RelationPersistence.Descriptor,
        calls: usize = 0,
        fn localOwner(_: *anyopaque, _: std.mem.Allocator, selected: @import("group_work_ownership.zig").NativeSelection, _: operation.RequestContext) anyerror!bool {
            if (selected.descriptor.owner_node_id != 9 or selected.descriptor.owner_store_id != 11 or selected.table_id != 7 or selected.purpose != .retirement) return error.InvalidCatalogRecord;
            return true;
        }
        fn raft(_: *anyopaque, _: u64) bool {
            return false;
        }
        fn status(_: *anyopaque) anyerror!@import("../metadata/api.zig").MetadataStatus {
            return error.TestUnexpectedStatusRead;
        }
        fn capture(raw: *anyopaque, alloc: std.mem.Allocator, _: u64, _: []const u8, request: retirement_preparation.Request, _: operation.RequestContext) anyerror![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            const response: retirement_preparation.Response = .{ .request = request, .identity = .{ .namespace = request.namespace, .catalog_digest = self.digest, .next_epoch = 1, .catalog_checks_identity_supported = true }, .native_binding = .{ .descriptor = self.descriptor, .native_instance = @splat(4), .table_id = request.namespace.table_id, .shard_id = request.namespace.shard_id, .range_id = request.namespace.range_id, .physical_root_incarnation = 1, .schema_version = 0, .schema_digest = @splat(2), .catalog_generation = 1, .catalog_fingerprint = @splat(6) }, .boot_generation = 1, .metadata_incarnation = request.metadata_incarnation, .catalog_json = self.catalog };
            return response.encodeAlloc(alloc);
        }
    };
    var spy: Spy = .{ .catalog = envelope, .digest = catalog_digest, .descriptor = descriptor };
    var accounting = std.testing.FailingAllocator.init(a, .{});
    const backing = accounting.allocator();
    var server = server_mod.ApiHttpServer.init(backing, .{ .deployment_mode = .standalone, .relational_maintenance_leadership = .{ .ptr = &spy, .vtable = &.{ .is_local_leader = Spy.raft, .native_local = Spy.localOwner } }, .table_retirement_preparation = .{ .ptr = &spy, .prepare_fn = Spy.capture } }, .{ .ptr = &spy, .vtable = &.{ .status = Spy.status } }, null, null);
    defer server.deinit();
    const baseline = accounting.allocated_bytes - accounting.freed_bytes;
    // Reproduce the actual coordinator: its Plan lives in an arena. Response
    // buffers must not be allocated there even when free/deinit are called.
    var plan = std.heap.ArenaAllocator.init(backing);
    defer plan.deinit();
    var captures: RetirementCaptures = .{ .server = &server, .alloc = plan.allocator(), .context = .{ .setting_admin = true, .schema_validation_authority = true, .fk_generation_publication_authority = true }, .head = head };
    defer captures.deinit();
    const root_catalog = try captures.catalog(table, &.{root_range});
    try std.testing.expectEqualStrings(base64, root_catalog);
    for (0..512) |i| {
        const range: records.RangeRecord = .{ .group_id = 1000 + i, .table_id = 7, .doc_identity_shard_id = 2000 + i, .doc_identity_range_id = 3000 + i, .start_key = "" };
        _ = try captures.capture(table, range);
        const retained = accounting.allocated_bytes - accounting.freed_bytes - baseline;
        try std.testing.expect(retained < 4 * 1024 * 1024);
    }
    try std.testing.expect(captures.entries.count() == 513);
    try std.testing.expectEqual(@as(usize, 513), spy.calls);
    try std.testing.expectEqual(@as(u64, 72), captures.root_catalog.?.key.owner_group);
    try std.testing.expectEqual(base64.len, captures.root_catalog.?.base64.len);
    for (0..512) |i| {
        const range: records.RangeRecord = .{ .group_id = 1000 + i, .table_id = 7, .doc_identity_shard_id = 2000 + i, .doc_identity_range_id = 3000 + i, .start_key = "" };
        _ = try captures.capture(table, range);
    }
    _ = try captures.capture(table, root_range);
    try std.testing.expectEqual(@as(usize, 513), spy.calls);
}
