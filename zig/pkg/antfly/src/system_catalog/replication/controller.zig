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

//! Native publication admission planning. All metadata facts are read through
//! the caller's actual transaction; physical AIC is supplied later by an
//! enrolled owner observation. No metadata mutation or physical grant occurs
//! until the complete draft and pending Resource cut have been validated.
const std = @import("std");
const local = @import("antfly_local_sources");
const domain = local.system_catalog_domain;
const records = local.system_catalog_logical_replication_catalog;
const publisher = local.system_catalog_logical_replication_publisher;
const capture = local.system_catalog_logical_replication_capture;
const storage = @import("../storage.zig");
const membership = @import("membership.zig");
const ddl = @import("logical_replication.zig");
const installation = @import("installation.zig");
const roster_reader = @import("roster.zig");
const A = std.mem.Allocator;

pub const Planned = struct {
    draft: installation.Draft,
    /// Inactive CREATE placeholder or retained active ALTER/DROP definition.
    /// Pending refs and this definition are committed with the draft itself.
    resources: domain.Delta,
};

fn resource(a: A, value: records.Publication) !domain.Resource {
    try value.validate(a);
    return .{ .kind = .publication, .id = value.id, .parent_id = value.database_id, .name = value.name, .source_json = try std.json.Stringify.valueAlloc(a, value, .{}) };
}
/// host.table(a,id), host.ranges(a,id) must point-join actual physical metadata
/// in this same transaction. They may not use a cache, placement guess or a
/// caller-provided schema. Ordinary publication relation OIDs come from SCAT.
pub fn prepare(a: A, view: storage.View, realm: publisher.Realm, request: ddl.Request, command_digest: [32]u8, host: anytype) !?Planned {
    try request.validate();
    try realm.validate();
    if (realm.group_id != view.group_id or realm.database_id != request.database_id or
        request.kind() != .publication or !std.mem.eql(u8, &realm.incarnation, &std.fmt.bytesToHex(request.incarnation, .lower))) return error.InvalidLogicalCaptureControl;
    const existing = try view.lookup(.publication, request.database_id, request.name);
    const previous = if (existing) |value| try records.decodePublication(a, value) else null;
    if (previous) |record| {
        if (record.capture_operation != null or !record.active) return error.LogicalCapturePublicationPending;
    }
    const planned = try ddl.plan(a, view, request);
    if (planned.result.no_op) return null;
    const desired = if (planned.delta.upserts.len != 0) try records.decodePublication(a, planned.delta.upserts[0]) else null;
    if (previous == null and desired == null) return error.InvalidLogicalCaptureControl;
    const id = if (desired) |value| value.id else previous.?.id;
    const generation = if (desired) |value| value.generation else std.math.add(u64, previous.?.generation, 1) catch return error.CatalogGenerationExhausted;
    const scope: capture.Scope = .{ .realm = realm, .subject = .{ .publication = .{ .id = id, .generation = generation } }, .operation_id = request.admission.nonce, .change_digest = command_digest };
    try scope.validate();
    const Membership = struct { table: domain.Resource, old: bool = false, new: bool = false };
    var selected: std.AutoHashMapUnmanaged(u64, Membership) = .empty;
    defer selected.deinit(a);
    for ([_]?records.Publication{ previous, desired }, 0..) |maybe, side| if (maybe) |value| {
        for (value.tables) |table| {
            const actual = (try view.tableByLogicalRelationId(table.id)) orelse return error.CatalogSourceUntracked;
            if (actual.parent_id != table.namespace_id) return error.LogicalCaptureControlChanged;
            const entry = try selected.getOrPut(a, actual.id);
            if (!entry.found_existing) entry.value_ptr.* = .{ .table = actual };
            if (side == 0) entry.value_ptr.old = true else entry.value_ptr.new = true;
        }
    };
    const all_tables = (previous != null and previous.?.all_tables) or (desired != null and desired.?.all_tables);
    if (all_tables) {
        // Bound the complete database scan across namespace pages.
        const namespaces = try view.childrenBounded(.namespace, realm.database_id, publisher.max_relations, records.max_record_bytes);
        var bytes: usize = 0;
        var scanned: usize = 0;
        for (namespaces) |namespace| {
            const tables = try view.childrenBounded(.table, namespace.id, publisher.max_relations - scanned, records.max_record_bytes);
            scanned += tables.len;
            for (tables) |table| {
                const encoded = try std.json.Stringify.valueAlloc(a, table, .{});
                defer a.free(encoded);
                bytes = std.math.add(usize, bytes, encoded.len) catch return error.CatalogCommandTooLarge;
                if (bytes > records.max_record_bytes) return error.CatalogCommandTooLarge;
                const physical = try host.table(a, table.id);
                if (!try @import("roster.zig").eligibleForAllTables(a, physical, table)) continue;
                _ = table.sql_relation_id orelse return error.CatalogSourceUntracked;
                const entry = try selected.getOrPut(a, table.id);
                if (!entry.found_existing) entry.value_ptr.* = .{ .table = table };
                if (selected.count() > publisher.max_relations) return error.CatalogCommandTooLarge;
            }
        }
    }
    if (selected.count() > publisher.max_relations) return error.CatalogCommandTooLarge;
    const affected = try a.alloc(u64, selected.count());
    defer a.free(affected);
    var iter = selected.keyIterator();
    for (affected) |*table_id| table_id.* = iter.next().?.*;
    std.mem.sort(u64, affected, {}, std.sort.asc(u64));
    const database = (try view.byId(.database, realm.database_id)) orelse return error.DatabaseNotFound;
    const previous_stamp = if (previous) |value| try value.stamp(a) else null;
    const desired_stamp = if (desired) |value| try value.stamp(a) else null;
    const all_scope: membership.Scope = .{ .database = database.id };
    const next_all = try membership.project(a, view, all_scope, database.sql_all_publications, id, if (previous != null and previous.?.all_tables) previous_stamp else null, if (desired != null and desired.?.all_tables) desired_stamp else null);
    const database_head = try membership.head(all_scope, next_all);
    var members: std.ArrayList(installation.DraftMember) = .empty;
    var budget: publisher.RosterBudget = .{};
    var installed_cache: installation.LookupCache = .empty;
    defer installed_cache.deinit(a);
    var parameter_cache: std.AutoHashMapUnmanaged(u64, publisher.Membership) = .empty;
    defer parameter_cache.deinit(a);
    if (desired) |value| try parameter_cache.put(a, id, .{ .publication = desired_stamp.?, .parameters = value.parameters });
    for (affected) |table_id| {
        const member = selected.get(table_id).?;
        const table = member.table;
        if (table.native_service != null) return error.UnsupportedReplicationType;
        const logical = table.sql_relation_id orelse return error.CatalogSourceUntracked;
        const namespace = (try view.byId(.namespace, table.parent_id)) orelse return error.InvalidCatalogRecord;
        if (namespace.parent_id != realm.database_id) return error.LogicalCaptureControlChanged;
        const physical = try host.table(a, table.id);
        if (physical.sql_persistence != null or physical.sql_materialized_view_id != null or
            physical.read_schema_json.len != 0 or physical.storage_migration != null) return error.UnsupportedReplicationType;
        const table_scope: membership.Scope = .{ .relation = .{ .database = database.id, .logical_id = logical } };
        const next_table = try membership.project(a, view, table_scope, table.sql_publications, id, if (member.old) previous_stamp else null, if (member.new) desired_stamp else null);
        const heads: capture.Heads = .{ .table = try membership.head(table_scope, next_table), .database = database_head };
        const description = try roster_reader.describeTable(a, physical, table, namespace);
        const roster: ?publisher.PublishedRelation = if (heads.empty()) null else .{ .relation = description, .memberships = try projectedMemberships(a, view, &parameter_cache, next_table, next_all) };
        if (roster) |value| try budget.add(value);
        const ranges = try host.ranges(a, table.id, publisher.max_relations - members.items.len);
        if (ranges.len == 0) return error.CatalogSourceUntracked;
        for (ranges) |range| {
            if (range.table_id != table.id or range.group_id == 0 or members.items.len == publisher.max_relations) return error.InvalidLogicalCaptureControl;
            const target: capture.ObservationRequest = .{ .scope = scope, .logical_relation_id = logical, .physical_table_id = table.id, .owner_group_id = range.group_id, .namespace = .{ .table_id = table.id, .shard_id = @TypeOf(host).shard(range), .range_id = @TypeOf(host).rangeId(range) }, .schema_version = description.schema_version, .schema_digest = description.schema_digest, .catalog_revision = view.meta.revision, .previous = .{ .table = table.sql_publications, .database = database.sql_all_publications }, .next = heads };
            try members.append(a, .{ .request = target, .roster = roster, .previous = try installation.installed(a, view.txn, view.group_id, target, &installed_cache) });
        }
    }
    std.mem.sort(installation.DraftMember, members.items, {}, struct {
        fn less(_: void, x: installation.DraftMember, y: installation.DraftMember) bool {
            return x.request.physical_table_id < y.request.physical_table_id or
                (x.request.physical_table_id == y.request.physical_table_id and x.request.owner_group_id < y.request.owner_group_id);
        }
    }.less);
    const draft: installation.Draft = .{ .scope = scope, .previous = previous, .desired = desired, .members = try members.toOwnedSlice(a) };
    var placeholder = previous orelse desired.?;
    placeholder.active = previous != null;
    placeholder.capture_operation = .{ .id = scope.operation_id, .body_digest = try draft.digest(a) };
    return .{ .draft = draft, .resources = .{ .upserts = try a.dupe(domain.Resource, &.{try resource(a, placeholder)}), .removes = &.{}, .next_id = planned.delta.next_id } };
}

pub fn stage(a: A, view: storage.View, planned: Planned) !installation.Address {
    const address = try installation.begin(a, view.txn, view.group_id, planned.draft);
    try storage.applyDelta(a, view.txn, view.group_id, planned.resources, view.meta, planned.draft.scope.change_digest);
    return address;
}

/// Publish under the same metadata transaction as the acknowledged journal
/// transition. Only the exact reserved preimage and complete owner cut qualify.
pub fn publish(a: A, view: storage.View, current: installation.Record) !void {
    if (current.phase != .prepared or current.plan.owner() != .publication) return error.LogicalCaptureControlChanged;
    const plan = current.plan;
    const id = plan.owner().id();
    const stored = (try view.byId(.publication, id)) orelse return error.LogicalCaptureControlChanged;
    const reservation = try records.decodePublication(a, stored);
    const operation = reservation.capture_operation orelse return error.LogicalCaptureControlChanged;
    const draft_digest = current.draft_digest orelse return error.LogicalCaptureControlChanged;
    if (!std.mem.eql(u8, &operation.id, &plan.scope.operation_id) or
        !std.mem.eql(u8, &operation.body_digest, &draft_digest) or
        reservation.active != (plan.previous != null))
        return error.LogicalCaptureControlChanged;
    const expected = plan.previous orelse plan.desired.?;
    if (!std.meta.eql(try expected.stamp(a), try reservation.stamp(a))) return error.LogicalCaptureControlChanged;
    var change: [1]domain.Resource = undefined;
    var delta: domain.Delta = .{ .upserts = &.{}, .removes = &.{}, .next_id = view.meta.next_id };
    if (plan.desired) |desired| {
        change[0] = try resource(a, desired);
        delta.upserts = &change;
    } else {
        change[0] = stored;
        delta.removes = &change;
    }
    try installation.requireUnpublished(view.txn, view.group_id, plan);
    try storage.applyDelta(a, view.txn, view.group_id, delta, view.meta, plan.scope.change_digest);
    try installation.putPublishedRevision(view.txn, view.group_id, plan, view.meta.revision + 1);
}

fn projectedMemberships(a: A, view: storage.View, cache: *std.AutoHashMapUnmanaged(u64, publisher.Membership), table: []const records.Stamp, all: []const records.Stamp) ![]const publisher.Membership {
    if (table.len + all.len > publisher.max_publications) return error.CatalogCommandTooLarge;
    const result = try a.alloc(publisher.Membership, table.len + all.len);
    var left: usize = 0;
    var right: usize = 0;
    for (result) |*out| {
        if (left < table.len and right < all.len and table[left].id == all[right].id) return error.InvalidLogicalReplicationRecord;
        const stamp = if (right == all.len or (left < table.len and table[left].id < all[right].id)) selected: {
            defer left += 1;
            break :selected table[left];
        } else selected: {
            defer right += 1;
            break :selected all[right];
        };
        const entry = try cache.getOrPut(a, stamp.id);
        if (!entry.found_existing) {
            const record = try records.decodePublication(a, (try view.byId(.publication, stamp.id)) orelse return error.InvalidLogicalReplicationRecord);
            entry.value_ptr.* = .{ .publication = try record.stamp(a), .parameters = record.parameters };
        }
        if (!std.meta.eql(entry.value_ptr.publication, stamp)) return error.LogicalCaptureControlChanged;
        out.* = entry.value_ptr.*;
    }
    return result;
}
