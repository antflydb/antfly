// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Construct an FK-generation candidate from read-index owner identities and
//! one linearizable metadata topology. Begin rechecks every descriptor inside
//! the metadata transaction; child fence receipts attest the proposed AIC.
const std = @import("std");
const server_mod = @import("http_server.zig");
const operation = @import("operation.zig");
const records = @import("../common/topology_records.zig");
const metadata = @import("../metadata/table_manager.zig");
const publication = @import("../metadata/fk_generation_publication.zig");
const topology = @import("../storage/db/relational_integrity_topology_contract.zig");
const tables = @import("tables.zig");

pub fn rangesFor(alloc: std.mem.Allocator, snapshot: @import("../metadata/api.zig").AdminSnapshot, table_id: u64) ![]records.RangeRecord {
    var selected: std.ArrayList(records.RangeRecord) = .empty;
    for (snapshot.ranges) |range| if (range.table_id == table_id) try selected.append(alloc, range);
    if (selected.items.len == 0 or selected.items.len > publication.max_owners) return error.InvalidGenerationPublication;
    metadata.sortKeyspaceRanges(records.RangeRecord, selected.items);
    try metadata.validateCompleteKeyspaceRanges(selected.items);
    return selected.toOwnedSlice(alloc);
}

pub fn ownerFences(server: *server_mod.ApiHttpServer, alloc: std.mem.Allocator, context: operation.RequestContext, id: publication.Id, table: records.TableRecord, ranges: []const records.RangeRecord, role: topology.Role) ![]topology.Fence {
    const reads = server.table_reads orelse return error.UnsupportedOperation;
    const fences = try alloc.alloc(topology.Fence, ranges.len);
    for (ranges, fences) |range, *fence| {
        try context.ensureActive();
        var response = (try reads.lookup(alloc, table.name, range.start_key, .{
            .relational_topology_json = "{\"mode\":\"identity\"}",
            .execution_deadline_ns = (try context.platformDeadline()).deadline_ns,
            .cancellation = context.cancellation,
        }, .read_index)) orelse return error.TableNotFound;
        defer response.deinit(alloc);
        const Native = struct { namespace: @import("../storage/db/doc_identity.zig").Namespace, catalog_digest: [32]u8, next_epoch: u64 };
        const native = try std.json.parseFromSliceLeaky(Native, alloc, response.json, .{ .ignore_unknown_fields = true });
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
    if (size == 0 or size > @import("../storage/db/relational_integrity_catalog.zig").max_catalog_bytes) return error.InvalidIntegrityCatalog;
    return alloc.dupe(u8, observed.catalog);
}

pub fn build(server: *server_mod.ApiHttpServer, alloc: std.mem.Allocator, context: operation.RequestContext, identity: ?server_mod.AuthenticatedIdentity, before: records.TableRecord, proposed_schema_json: []const u8) !publication.Plan {
    try context.ensureActive();
    var snapshot = (try server.source.linearizableSnapshot(context)) orelse return error.MetadataCapabilityUnavailable;
    defer server.source.freeAdminSnapshot(&snapshot);
    const current = for (snapshot.tables) |table| {
        if (table.table_id == before.table_id and std.mem.eql(u8, table.name, before.name)) break table;
    } else return error.CatalogGenerationChanged;
    if (!metadata.tableDefinitionsEqual(current, before)) return error.CatalogGenerationChanged;
    const after = try tables.prepareForeignKeyPublicationRecord(alloc, &current, proposed_schema_json);
    const catalog_b64 = try childCatalogB64(server, alloc, current);
    const derived = try publication.deriveTransitions(alloc, current.table_id, current.name, current.schema_json, after.schema_json, catalog_b64);
    if (derived.len == 0) return error.InvalidGenerationPublication;
    var id: publication.Id = undefined;
    const io = server.restore_job_store.io orelse return error.AsyncRestoreUnavailable;
    while (true) {
        try io.randomSecure(&id);
        if (std.mem.readInt(u64, id[0..8], .little) != 0 and std.mem.readInt(u64, id[8..16], .little) != 0) break;
    }
    const child_ranges = try rangesFor(alloc, snapshot, current.table_id);
    const child_fences = try ownerFences(server, alloc, context, id, current, child_ranges, .child_generation_source);
    var parents: std.ArrayList(publication.Parent) = .empty;
    for (derived) |item| {
        const parent_table = for (snapshot.tables) |table| {
            if (std.mem.eql(u8, table.name, item.parent_table_name)) break table;
        } else return error.ForeignKeyParentTableNotFound;
        if (parent_table.table_id == current.table_id or parent_table.storage_migration != null or
            parent_table.relational_retirement_json.len != 0 or parent_table.restore_backup_id.len != 0) return error.TableTransitionActive;
        var found: ?*publication.Parent = null;
        for (parents.items) |*parent| if (parent.table.table_id == parent_table.table_id) {
            found = parent;
            break;
        };
        if (found == null) {
            const names = try server.logicalTableNamesInArena(alloc, context, &.{parent_table.name});
            if (names.len != 1 or !try server_mod.tablePermissionCurrentlyAllowed(identity, names[0], .admin) or
                try server_mod.resolveEffectiveRowFilterJson(alloc, identity, names[0]) != null) return error.Forbidden;
            const parent_ranges = try rangesFor(alloc, snapshot, parent_table.table_id);
            try parents.append(alloc, .{
                .table = parent_table,
                .ranges = parent_ranges,
                .fences = try ownerFences(server, alloc, context, id, parent_table, parent_ranges, .child_generation_parent),
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
        .id = id,
        .child_before = current,
        .child_after = after,
        .child_catalog_before_b64 = catalog_b64,
        .child_ranges = child_ranges,
        .child_fences = child_fences,
        .parents = try parents.toOwnedSlice(alloc),
    };
    try result.validate(alloc);
    return result;
}
