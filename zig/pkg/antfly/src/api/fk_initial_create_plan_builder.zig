// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Build an initial FK table publication from one metadata-assigned hidden
//! identity. Begin rechecks the catalog revision and every owner descriptor.
const std = @import("std");
const server_mod = @import("http_server.zig");
const operation = @import("operation.zig");
const domain = @import("../system_catalog/domain.zig");
const records = @import("../common/topology_records.zig");
const publication = @import("../metadata/fk_generation_publication.zig");
const topology = @import("../storage/db/relational_integrity_topology_contract.zig");
const tables = @import("tables.zig");
const platform_time = @import("antfly_platform").time;
const existing = @import("fk_generation_plan_builder.zig");

fn parentsSettled(tables_snapshot: []const records.TableRecord, child_name: []const u8, derived: []const publication.DerivedTransition) !bool {
    var pending = false;
    for (derived) |item| {
        if (std.mem.eql(u8, item.parent_table_name, child_name)) continue;
        const parent = for (tables_snapshot) |table| {
            if (std.mem.eql(u8, table.name, item.parent_table_name)) break table;
        } else return error.ForeignKeyParentTableNotFound;
        if (parent.read_schema_json.len != 0) pending = true;
    }
    return !pending;
}

/// The support-index DDL is committed before initial CREATE planning. Its
/// schema migration can still replace the parent's exact TableRecord while
/// owner read-index fences are being collected. Pin only a finalized parent
/// definition; the owner stage separately waits for physical index readiness.
fn settledParentSnapshot(
    server: *server_mod.ApiHttpServer,
    context: operation.RequestContext,
    child_name: []const u8,
    derived: []const publication.DerivedTransition,
) !@import("../metadata/api.zig").AdminSnapshot {
    const io = server.restore_job_store.io orelse return error.AsyncRestoreUnavailable;
    const started = platform_time.monotonicNs();
    while (true) {
        try context.ensureActive();
        var snapshot = (try server.source.linearizableSnapshot(context)) orelse return error.MetadataCapabilityUnavailable;
        var release = true;
        defer if (release) server.source.freeAdminSnapshot(&snapshot);
        if (try parentsSettled(snapshot.tables, child_name, derived)) {
            release = false;
            return snapshot;
        }
        if (platform_time.monotonicNs() -| started >= 15 * std.time.ns_per_s)
            return error.ForeignKeyParentSchemaPending;
        try io.sleep(.fromMilliseconds(25), .awake);
    }
}

/// Resolve only the self edge after metadata has reserved the candidate's
/// physical identity. External edges have already been bound at DDL ingress.
fn bindReservedSelf(alloc: std.mem.Allocator, schema_json: []const u8, logical_name: []const u8, physical_name: []const u8) ![]const u8 {
    var value = try std.json.parseFromSliceLeaky(std.json.Value, alloc, schema_json, .{ .allocate = .alloc_always, .parse_numbers = false });
    if (value != .object) return error.InvalidGenerationPublication;
    const fks = value.object.getPtr("foreign_keys") orelse return schema_json;
    if (fks.* != .array) return error.InvalidGenerationPublication;
    var changed = false;
    for (fks.array.items) |*fk| {
        if (fk.* != .object) return error.InvalidGenerationPublication;
        const parent = fk.object.getPtr("parent_table") orelse return error.InvalidGenerationPublication;
        if (parent.* != .string) return error.InvalidGenerationPublication;
        if (!std.mem.eql(u8, parent.string, logical_name)) continue;
        parent.* = .{ .string = physical_name };
        changed = true;
    }
    return if (changed) try std.json.Stringify.valueAlloc(alloc, value, .{}) else schema_json;
}

fn namespaceId(server: *server_mod.ApiHttpServer, alloc: std.mem.Allocator, context: operation.RequestContext, target: domain.Target) !u64 {
    if (std.mem.eql(u8, target.database, domain.default_database_name) and
        std.mem.eql(u8, target.namespace, domain.default_namespace_name))
        return domain.default_namespace_id;
    const bytes = try server.source.systemCatalog(alloc, context, .{ .read = .{
        .kind = .namespace,
        .database = target.database,
        .name = target.namespace,
    } });
    const resources = try std.json.parseFromSliceLeaky([]domain.Resource, alloc, bytes, .{ .allocate = .alloc_always });
    for (resources) |resource| {
        if (resource.kind == .namespace and std.mem.eql(u8, resource.name, target.namespace)) return resource.id;
    }
    return error.NamespaceNotFound;
}

pub fn build(
    server: *server_mod.ApiHttpServer,
    alloc: std.mem.Allocator,
    context: operation.RequestContext,
    identity: ?server_mod.AuthenticatedIdentity,
    target: domain.Target,
    request: tables.CreateTableRequest,
) !publication.InitialCreatePlan {
    try context.ensureActive();
    const logical_child = try target.resourceNameAlloc(alloc);
    defer alloc.free(logical_child);
    if (!try server_mod.tablePermissionCurrentlyAllowed(identity, logical_child, .admin)) return error.Forbidden;
    if (request.tablespace_name) |tablespace| {
        if (identity) |authenticated| {
            if (!server_mod.permissionsAllow(authenticated.permissions, .tablespace, tablespace, .read)) return error.Forbidden;
        }
    }
    var trusted = context;
    trusted.setting_admin = true;
    trusted.fk_generation_publication_authority = true;
    const namespace_id = try namespaceId(server, alloc, trusted, target);
    var candidate = tables.deriveTableRecord("", request);
    candidate.table_id = 0;
    const prepare_bytes = try server.source.systemCatalog(alloc, trusted, .{ .fk_initial_create_prepare = .{
        .namespace_id = namespace_id,
        .logical_name = target.table,
        .tablespace_name = request.tablespace_name,
        .min_ranges_explicit = request.num_shards != null,
        .candidate = candidate,
    } });
    const prepared = try std.json.parseFromSliceLeaky(publication.InitialCreatePrepare, alloc, prepare_bytes, .{ .allocate = .alloc_always });
    var child = prepared.child;
    child.schema_json = try bindReservedSelf(alloc, child.schema_json, target.table, child.name);
    const derived = try publication.deriveInitialTransitions(alloc, child.table_id, child.name, child.schema_json);
    if (derived.len == 0) return error.InvalidGenerationPublication;
    var snapshot = try settledParentSnapshot(server, context, child.name, derived);
    defer server.source.freeAdminSnapshot(&snapshot);
    var id: publication.Id = undefined;
    const io = server.restore_job_store.io orelse return error.AsyncRestoreUnavailable;
    while (true) {
        try io.randomSecure(&id);
        if (std.mem.readInt(u64, id[0..8], .little) != 0 and std.mem.readInt(u64, id[8..16], .little) != 0) break;
    }
    var parents: std.ArrayList(publication.Parent) = .empty;
    var self_transitions: std.ArrayList(publication.Transition) = .empty;
    var self_target_checked = false;
    for (derived) |item| {
        if (std.mem.eql(u8, item.parent_table_name, child.name)) {
            if (!self_target_checked) {
                try @import("../schema/relational_foreign_key_target.zig").validate(alloc, child.schema_json, child.name, child.schema_json);
                self_target_checked = true;
            }
            try self_transitions.append(alloc, item.transition);
            continue;
        }
        const parent_table: records.TableRecord = for (snapshot.tables) |table| {
            if (std.mem.eql(u8, table.name, item.parent_table_name)) break table;
        } else return error.ForeignKeyParentTableNotFound;
        if (parent_table.table_id == prepared.child.table_id or parent_table.storage_migration != null or
            parent_table.relational_retirement_json.len != 0 or parent_table.restore_backup_id.len != 0)
            return error.TableTransitionActive;
        var found: ?*publication.Parent = null;
        for (parents.items) |*parent| if (parent.table.table_id == parent_table.table_id) {
            found = parent;
            break;
        };
        if (found == null) {
            const names = try server.logicalTableNamesInArena(alloc, context, &.{parent_table.name});
            if (names.len != 1 or !try server_mod.tablePermissionCurrentlyAllowed(identity, names[0], .admin) or
                try server_mod.resolveEffectiveRowFilterJson(alloc, identity, names[0]) != null)
                return error.Forbidden;
            const ranges = try existing.rangesFor(alloc, snapshot, parent_table.table_id);
            try parents.append(alloc, .{
                .table = try existing.ownedTableForPlan(alloc, parent_table),
                .ranges = ranges,
                .fences = try existing.ownerFences(server, alloc, context, id, parent_table, ranges, .child_generation_parent),
                .transitions = &.{},
            });
            found = &parents.items[parents.items.len - 1];
        }
        const old = found.?.transitions;
        const next = try alloc.alloc(publication.Transition, old.len + 1);
        @memcpy(next[0..old.len], old);
        next[old.len] = item.transition;
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
    std.mem.sort(publication.Transition, self_transitions.items, {}, struct {
        fn less(_: void, lhs: publication.Transition, rhs: publication.Transition) bool {
            return std.mem.lessThan(u8, lhs.constraint_name, rhs.constraint_name);
        }
    }.less);
    const plan: publication.InitialCreatePlan = .{
        .id = id,
        .catalog_id = prepared.catalog_id,
        .expected_catalog_revision = prepared.expected_catalog_revision,
        .tablespace_id = prepared.tablespace_id,
        .min_ranges_explicit = prepared.min_ranges_explicit,
        .child = child,
        .child_ranges = prepared.child_ranges,
        .parents = try parents.toOwnedSlice(alloc),
        .self_transitions = try self_transitions.toOwnedSlice(alloc),
        .logical_name = try alloc.dupe(u8, target.table),
        .namespace_id = namespace_id,
    };
    try plan.validate(alloc);
    return plan;
}

test "initial FK plan waits for all external parent schema migrations" {
    const parents = [_]records.TableRecord{
        .{ .table_id = 11, .name = "parent_a", .schema_json = "{\"version\":2}", .read_schema_json = "{\"version\":1}" },
        .{ .table_id = 12, .name = "parent_b", .schema_json = "{\"version\":3}" },
    };
    const derived = [_]publication.DerivedTransition{
        .{ .parent_table_name = "parent_a", .transition = undefined },
        .{ .parent_table_name = "parent_b", .transition = undefined },
        .{ .parent_table_name = "child", .transition = undefined },
    };
    try std.testing.expect(!try parentsSettled(&parents, "child", &derived));
    var finalized = parents;
    finalized[0].read_schema_json = "";
    try std.testing.expect(try parentsSettled(&finalized, "child", &derived));
    finalized[1].name = "renamed";
    try std.testing.expectError(error.ForeignKeyParentTableNotFound, parentsSettled(&finalized, "child", &derived));
}
