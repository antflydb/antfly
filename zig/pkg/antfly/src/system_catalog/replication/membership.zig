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

//! Derived publication membership indexes. The catalog Resource heads commit
//! the exact sorted stamps, so a missing index cannot silently mean no capture.
const std = @import("std");
const local = @import("antfly_local_sources");
const domain = local.system_catalog_domain;
const records = local.system_catalog_logical_replication_catalog;
const storage = @import("../storage.zig");
const A = std.mem.Allocator;
const Stamp = records.Stamp;
pub const max_publications = 4096;

pub const Scope = union(enum) { database: u64, relation: struct { database: u64, logical_id: u64 } };
pub fn prefix(buffer: []u8, group: u64, scope: Scope) ![]const u8 {
    if (group == 0) return error.InvalidLogicalReplicationRecord;
    return switch (scope) {
        .database => |database| if (database == 0) error.InvalidLogicalReplicationRecord else std.fmt.bufPrint(buffer, "\x00\x00__metadata__:system_catalog:{d}:publication-members:all:{d}:", .{ group, database }),
        .relation => |relation| if (relation.database == 0 or relation.logical_id == 0) error.InvalidLogicalReplicationRecord else std.fmt.bufPrint(buffer, "\x00\x00__metadata__:system_catalog:{d}:publication-members:table:{d}:{d}:", .{ group, relation.database, relation.logical_id }),
    };
}
fn key(buffer: []u8, group: u64, scope: Scope, publication: u64) ![]const u8 {
    if (publication == 0) return error.InvalidLogicalReplicationRecord;
    var base: [192]u8 = undefined;
    return std.fmt.bufPrint(buffer, "{s}{x:0>16}", .{ try prefix(&base, group, scope), publication });
}
fn encode(stamp: Stamp) [40]u8 {
    var result: [40]u8 = undefined;
    std.mem.writeInt(u64, result[0..8], stamp.generation, .big);
    @memcpy(result[8..40], &stamp.digest);
    return result;
}
fn decode(id: u64, bytes: []const u8) !Stamp {
    if (id == 0 or bytes.len != 40) return error.InvalidLogicalReplicationRecord;
    const generation = std.mem.readInt(u64, bytes[0..8], .big);
    var digest: [32]u8 = undefined;
    @memcpy(&digest, bytes[8..40]);
    if (generation == 0 or std.mem.allEqual(u8, &digest, 0)) return error.InvalidLogicalReplicationRecord;
    return .{ .id = id, .generation = generation, .digest = digest };
}
fn matches(record: records.Publication, scope: Scope) bool {
    if (!record.active) return false;
    return switch (scope) {
        .database => |database| record.database_id == database and record.all_tables,
        .relation => |relation| record.database_id == relation.database and !record.all_tables and for (record.tables) |table| {
            if (table.id == relation.logical_id) break true;
        } else false,
    };
}
pub fn head(scope: Scope, stamps: []const Stamp) !?domain.PublicationMembership {
    if (stamps.len == 0) return null;
    if (stamps.len > max_publications) return error.InvalidLogicalReplicationRecord;
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly/sql-publication-membership/v1\x00");
    var identity: [17]u8 = @splat(0);
    switch (scope) {
        .database => |database| {
            identity[0] = 1;
            std.mem.writeInt(u64, identity[1..9], database, .big);
        },
        .relation => |relation| {
            identity[0] = 2;
            std.mem.writeInt(u64, identity[1..9], relation.database, .big);
            std.mem.writeInt(u64, identity[9..17], relation.logical_id, .big);
        },
    }
    hash.update(&identity);
    var previous: u64 = 0;
    for (stamps) |stamp| {
        if (stamp.id <= previous or stamp.generation == 0 or std.mem.allEqual(u8, &stamp.digest, 0)) return error.InvalidLogicalReplicationRecord;
        previous = stamp.id;
        var id: [8]u8 = undefined;
        std.mem.writeInt(u64, &id, stamp.id, .big);
        hash.update(&id);
        hash.update(&encode(stamp));
    }
    var digest: [32]u8 = undefined;
    hash.final(&digest);
    return .{ .count = @intCast(stamps.len), .digest = digest };
}

/// Called only by ordinary catalog resource write/remove in its native txn.
pub fn write(a: A, txn: anytype, group: u64, resource: domain.Resource, remove: bool) !void {
    if (resource.kind != .publication) return;
    var region = std.heap.ArenaAllocator.init(a);
    defer region.deinit();
    const record = try records.decodePublication(region.allocator(), resource);
    if (!record.active) return;
    const stamp = try record.stamp(region.allocator());
    var buffer: [256]u8 = undefined;
    if (record.all_tables) {
        const selected = try key(&buffer, group, .{ .database = record.database_id }, record.id);
        if (remove) try txn.delete(selected) else try txn.put(selected, &encode(stamp));
    } else {
        for (record.tables) |table| {
            const selected = try key(&buffer, group, .{ .relation = .{ .database = record.database_id, .logical_id = table.id } }, record.id);
            if (remove) try txn.delete(selected) else try txn.put(selected, &encode(stamp));
        }
    }
}

/// One bounded range read per requested relation/database, never per row.
/// Each index value is rejoined to its actual canonical publication Resource.
pub fn read(a: A, view: storage.View, scope: Scope, expected: ?domain.PublicationMembership) ![]const Stamp {
    return readWithKnown(a, view, scope, expected, null);
}

const KnownEdge = struct { id: u64, stamp: ?Stamp };

fn readWithKnown(a: A, view: storage.View, scope: Scope, expected: ?domain.PublicationMembership, known: ?KnownEdge) ![]const Stamp {
    var buffer: [192]u8 = undefined;
    const selected = try prefix(&buffer, view.group_id, scope);
    var cursor = try view.txn.openCursor();
    defer cursor.close();
    var entries: std.ArrayList(Stamp) = .empty;
    errdefer entries.deinit(a);
    var region = std.heap.ArenaAllocator.init(a);
    defer region.deinit();
    var found_known = false;
    var row = try cursor.seekAtOrAfter(selected);
    while (row) |entry| : (row = try cursor.next()) {
        if (!std.mem.startsWith(u8, entry.key, selected)) break;
        if (entries.items.len == max_publications or entry.key.len != selected.len + 16) return error.InvalidLogicalReplicationRecord;
        const id = std.fmt.parseInt(u64, entry.key[selected.len..], 16) catch return error.InvalidLogicalReplicationRecord;
        var canonical: [256]u8 = undefined;
        if (!std.mem.eql(u8, entry.key, try key(&canonical, view.group_id, scope, id))) return error.InvalidLogicalReplicationRecord;
        const retained = try decode(id, entry.value);
        if (known != null and known.?.id == id) {
            if (known.?.stamp == null or !std.meta.eql(retained, known.?.stamp.?)) return error.InvalidLogicalReplicationRecord;
            found_known = true;
        } else {
            _ = region.reset(.retain_capacity);
            var bounded = view;
            bounded.alloc = region.allocator();
            const resource = (try bounded.byId(.publication, id)) orelse return error.InvalidLogicalReplicationRecord;
            const record = try records.decodePublication(bounded.alloc, resource);
            if (!matches(record, scope) or !std.meta.eql(retained, try record.stamp(bounded.alloc))) return error.InvalidLogicalReplicationRecord;
        }
        try entries.append(a, retained);
    }
    if (known != null and (known.?.stamp != null) != found_known) return error.InvalidLogicalReplicationRecord;
    if (!std.meta.eql(try head(scope, entries.items), expected)) return error.InvalidLogicalReplicationRecord;
    return entries.toOwnedSlice(a);
}

/// Project the same membership transition that the catalog writer will apply.
pub fn project(a: A, view: storage.View, scope: Scope, expected: ?domain.PublicationMembership, changed: u64, previous: ?Stamp, next: ?Stamp) ![]const Stamp {
    const before = try readWithKnown(a, view, scope, expected, .{ .id = changed, .stamp = previous });
    defer a.free(before);
    return replace(a, before, changed, next);
}

fn replace(a: A, before: []const Stamp, id: u64, next: ?Stamp) ![]const Stamp {
    var result: std.ArrayList(Stamp) = .empty;
    try result.ensureTotalCapacity(a, before.len + @intFromBool(next != null));
    var pending = next;
    for (before) |stamp| {
        if (pending != null and stamp.id >= id) {
            result.appendAssumeCapacity(pending.?);
            pending = null;
        }
        if (stamp.id != id) result.appendAssumeCapacity(stamp);
    }
    if (pending) |stamp| result.appendAssumeCapacity(stamp);
    return result.toOwnedSlice(a);
}

/// Publication definition and authoritative affected-object membership heads
/// are one Delta. Source rows and table schema/placement remain untouched.
pub fn augment(a: A, view: storage.View, delta: domain.Delta) !domain.Delta {
    var publication: ?domain.Resource = null;
    var removed = false;
    for (delta.upserts) |resource| if (resource.kind == .publication) {
        if (publication != null) return error.InvalidCatalogMutation;
        publication = resource;
    };
    for (delta.removes) |resource| if (resource.kind == .publication) {
        if (publication != null) return error.InvalidCatalogMutation;
        publication = resource;
        removed = true;
    };
    const changed = publication orelse return delta;
    if (delta.upserts.len + delta.removes.len != 1) return error.InvalidCatalogMutation;
    const old_resource = try view.byId(.publication, changed.id);
    const previous = if (old_resource) |old| try records.decodePublication(a, old) else null;
    if (removed) {
        const persisted = old_resource orelse return error.CatalogGenerationChanged;
        if (!std.mem.eql(u8, persisted.source_json, changed.source_json)) return error.CatalogGenerationChanged;
    }
    if (previous) |old| {
        if (old.database_id != changed.parent_id or !std.mem.eql(u8, old.name, changed.name)) return error.InvalidCatalogMutation;
    }
    const next = if (removed) null else try records.decodePublication(a, changed);
    const previous_stamp = if (previous != null and previous.?.active) try previous.?.stamp(a) else null;
    const next_stamp = if (next != null and next.?.active) try next.?.stamp(a) else null;
    var upserts: std.ArrayList(domain.Resource) = .empty;
    try upserts.appendSlice(a, delta.upserts);
    const Membership = struct { previous: bool = false, next: bool = false };
    var ids: std.AutoHashMapUnmanaged(u64, Membership) = .empty;
    defer ids.deinit(a);
    if (previous) |record| for (record.tables) |table| try ids.put(a, table.id, .{ .previous = true });
    if (next) |record| for (record.tables) |table| {
        const entry = try ids.getOrPut(a, table.id);
        if (!entry.found_existing) entry.value_ptr.* = .{};
        entry.value_ptr.next = true;
    };
    const ordered = try a.alloc(u64, ids.count());
    defer a.free(ordered);
    var iterator = ids.keyIterator();
    var index: usize = 0;
    while (iterator.next()) |logical| : (index += 1) ordered[index] = logical.*;
    std.mem.sort(u64, ordered, {}, std.sort.asc(u64));
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    for (ordered) |logical| {
        _ = scratch.reset(.retain_capacity);
        var table = (try view.tableByLogicalRelationId(logical)) orelse return error.CatalogGenerationChanged;
        var scoped_view = view;
        scoped_view.alloc = scratch.allocator();
        const namespace = (try scoped_view.byId(.namespace, table.parent_id)) orelse return error.CatalogGenerationChanged;
        if (namespace.parent_id != changed.parent_id) return error.CatalogGenerationChanged;
        const scope: Scope = .{ .relation = .{ .database = changed.parent_id, .logical_id = logical } };
        const member = ids.get(logical).?;
        const known: KnownEdge = .{ .id = changed.id, .stamp = if (member.previous) previous_stamp else null };
        const before = try readWithKnown(scoped_view.alloc, scoped_view, scope, table.sql_publications, known);
        const updated = try head(scope, try replace(scoped_view.alloc, before, changed.id, if (member.next) next_stamp else null));
        if (!std.meta.eql(table.sql_publications, updated)) {
            table.sql_publications = updated;
            try upserts.append(a, table);
        }
    }
    if ((previous != null and previous.?.all_tables) or (next != null and next.?.all_tables)) {
        _ = scratch.reset(.retain_capacity);
        var database = (try view.byId(.database, changed.parent_id)) orelse return error.CatalogGenerationChanged;
        const scope: Scope = .{ .database = database.id };
        const known: KnownEdge = .{ .id = changed.id, .stamp = if (previous != null and previous.?.all_tables) previous_stamp else null };
        const before = try readWithKnown(scratch.allocator(), view, scope, database.sql_all_publications, known);
        const updated = try head(scope, try replace(scratch.allocator(), before, changed.id, if (next != null and next.?.all_tables) next_stamp else null));
        if (!std.meta.eql(database.sql_all_publications, updated)) {
            database.sql_all_publications = updated;
            try upserts.append(a, database);
        }
    }
    var result = delta;
    result.upserts = try upserts.toOwnedSlice(a);
    return result;
}

fn directoryPrefix(buffer: []u8, group: u64) ![]const u8 {
    if (group == 0) return error.InvalidLogicalReplicationRecord;
    return std.fmt.bufPrint(buffer, "\x00\x00__metadata__:system_catalog:{d}:publication-members:", .{group});
}
fn address(group: u64, bytes: []const u8) !struct { scope: Scope, publication: u64 } {
    var buffer: [192]u8 = undefined;
    const base = try directoryPrefix(&buffer, group);
    if (!std.mem.startsWith(u8, bytes, base)) return error.InvalidLogicalReplicationRecord;
    var parts = std.mem.splitScalar(u8, bytes[base.len..], ':');
    const kind = parts.next() orelse return error.InvalidLogicalReplicationRecord;
    const database = std.fmt.parseInt(u64, parts.next() orelse return error.InvalidLogicalReplicationRecord, 10) catch return error.InvalidLogicalReplicationRecord;
    const scope: Scope = if (std.mem.eql(u8, kind, "all")) .{ .database = database } else if (std.mem.eql(u8, kind, "table")) .{ .relation = .{
        .database = database,
        .logical_id = std.fmt.parseInt(u64, parts.next() orelse return error.InvalidLogicalReplicationRecord, 10) catch return error.InvalidLogicalReplicationRecord,
    } } else return error.InvalidLogicalReplicationRecord;
    const id = std.fmt.parseInt(u64, parts.next() orelse return error.InvalidLogicalReplicationRecord, 16) catch return error.InvalidLogicalReplicationRecord;
    if (parts.next() != null) return error.InvalidLogicalReplicationRecord;
    var canonical: [256]u8 = undefined;
    if (!std.mem.eql(u8, bytes, try key(&canonical, group, scope, id))) return error.InvalidLogicalReplicationRecord;
    return .{ .scope = scope, .publication = id };
}
fn ownerHead(a: A, reader: anytype, group: u64, scope: Scope) !?domain.PublicationMembership {
    return switch (scope) {
        .database => |id| blk: {
            var resource = (try storage.getById(a, reader, group, .database, id)) orelse return error.InvalidLogicalReplicationRecord;
            defer resource.deinit();
            try resource.value.validateLogicalRelationId();
            break :blk resource.value.sql_all_publications;
        },
        .relation => |relation| blk: {
            var resource = (try storage.getTableByLogicalRelationId(a, reader, group, relation.logical_id)) orelse return error.InvalidLogicalReplicationRecord;
            defer resource.deinit();
            try resource.value.validateLogicalRelationId();
            var namespace = (try storage.getById(a, reader, group, .namespace, resource.value.parent_id)) orelse return error.InvalidLogicalReplicationRecord;
            defer namespace.deinit();
            if (namespace.value.parent_id != relation.database) return error.InvalidLogicalReplicationRecord;
            break :blk resource.value.sql_publications;
        },
    };
}

/// Snapshot/checkpoint verification is linear in real resources and edges.
/// A forward point-check proves every required edge exists; a second ordered
/// pass streams each exact scope and joins its authoritative object head.
/// Independent edge/head counts detect orphans and stripped empty directories.
pub fn verifyDirectory(backing: A, reader: anytype, group: u64, rows: anytype, canceled: ?*const std.atomic.Value(bool)) !void {
    var region = std.heap.ArenaAllocator.init(backing);
    defer region.deinit();
    var record_buffer: [160]u8 = undefined;
    const record_prefix = try std.fmt.bufPrint(&record_buffer, "\x00\x00__metadata__:system_catalog:{d}:record:", .{group});
    var index_buffer: [192]u8 = undefined;
    const index_prefix = try directoryPrefix(&index_buffer, group);
    var expected_edges: u64 = 0;
    var expected_heads: u64 = 0;
    try rows.reset();
    while (try rows.next()) |row| {
        if (canceled) |flag| if (flag.load(.acquire)) return error.SnapshotBuildCancelled;
        if (!std.mem.startsWith(u8, row.key, record_prefix)) continue;
        _ = region.reset(.retain_capacity);
        const a = region.allocator();
        const Header = struct {
            kind: domain.Kind,
            id: u64,
            sql_publications: ?domain.PublicationMembership = null,
            sql_all_publications: ?domain.PublicationMembership = null,
        };
        const resource = try std.json.parseFromSliceLeaky(Header, a, row.value, .{ .ignore_unknown_fields = true });
        if (resource.sql_publications) |bound| {
            if (resource.kind != .table) return error.InvalidLogicalReplicationRecord;
            try bound.validate();
            expected_heads = try std.math.add(u64, expected_heads, 1);
        }
        if (resource.sql_all_publications) |bound| {
            if (resource.kind != .database) return error.InvalidLogicalReplicationRecord;
            try bound.validate();
            expected_heads = try std.math.add(u64, expected_heads, 1);
        }
        if (resource.kind != .publication) continue;
        const full = try std.json.parseFromSliceLeaky(domain.Resource, a, row.value, .{ .allocate = .alloc_always });
        const canonical = try storage.recordKeyAlloc(a, group, .publication, resource.id);
        if (!std.mem.eql(u8, canonical, row.key)) return error.InvalidLogicalReplicationRecord;
        const publication = try records.decodePublication(a, full);
        if (!publication.active) continue;
        const stamp = try publication.stamp(a);
        var buffer: [256]u8 = undefined;
        if (publication.all_tables) {
            const stored = reader.get(try key(&buffer, group, .{ .database = publication.database_id }, publication.id)) catch |err| return if (err == error.NotFound) error.InvalidLogicalReplicationRecord else err;
            if (!std.mem.eql(u8, stored, &encode(stamp))) return error.InvalidLogicalReplicationRecord;
            expected_edges = try std.math.add(u64, expected_edges, 1);
        } else for (publication.tables) |table| {
            const stored = reader.get(try key(&buffer, group, .{ .relation = .{ .database = publication.database_id, .logical_id = table.id } }, publication.id)) catch |err| return if (err == error.NotFound) error.InvalidLogicalReplicationRecord else err;
            if (!std.mem.eql(u8, stored, &encode(stamp))) return error.InvalidLogicalReplicationRecord;
            expected_edges = try std.math.add(u64, expected_edges, 1);
        }
    }
    var actual_edges: u64 = 0;
    var actual_heads: u64 = 0;
    var scope: ?Scope = null;
    var stamps: std.ArrayList(Stamp) = .empty;
    defer stamps.deinit(backing);
    try rows.reset();
    while (try rows.next()) |row| {
        if (canceled) |flag| if (flag.load(.acquire)) return error.SnapshotBuildCancelled;
        if (!std.mem.startsWith(u8, row.key, index_prefix)) continue;
        const current = try address(group, row.key);
        if (scope != null and !std.meta.eql(scope.?, current.scope)) {
            _ = region.reset(.retain_capacity);
            if (!std.meta.eql(try head(scope.?, stamps.items), try ownerHead(region.allocator(), reader, group, scope.?))) return error.InvalidLogicalReplicationRecord;
            actual_heads = try std.math.add(u64, actual_heads, 1);
            stamps.clearRetainingCapacity();
        }
        scope = current.scope;
        if (stamps.items.len == max_publications) return error.InvalidLogicalReplicationRecord;
        try stamps.append(backing, try decode(current.publication, row.value));
        actual_edges = try std.math.add(u64, actual_edges, 1);
    }
    if (scope) |last| {
        _ = region.reset(.retain_capacity);
        if (!std.meta.eql(try head(last, stamps.items), try ownerHead(region.allocator(), reader, group, last))) return error.InvalidLogicalReplicationRecord;
        actual_heads = try std.math.add(u64, actual_heads, 1);
    }
    if (actual_edges != expected_edges or actual_heads != expected_heads) return error.InvalidLogicalReplicationRecord;
}

const MembershipTestRows = struct {
    txn: *local.storage_docstore.DocStore.Txn,
    cursor: ?local.storage_docstore.DocStore.Txn.CursorAdapter = null,
    started: bool = false,
    fn reset(self: *@This()) !void {
        if (self.cursor) |*value| value.close();
        self.cursor = null;
        self.started = false;
    }
    fn next(self: *@This()) !?local.storage_docstore.KVPair {
        if (self.cursor == null) self.cursor = try self.txn.openCursor();
        const row = if (!self.started) first: {
            self.started = true;
            break :first try self.cursor.?.seekAtOrAfter("");
        } else try self.cursor.?.next();
        return if (row) |value| .{ .key = value.key, .value = value.value } else null;
    }
    fn verify(a: A, txn: *local.storage_docstore.DocStore.Txn) !void {
        var rows: @This() = .{ .txn = txn };
        defer rows.reset() catch unreachable;
        try verifyDirectory(a, txn, 7, &rows, null);
    }
};

test "SQL logical replication membership joins authoritative heads and refuses stripped orphan and stale cold edges" {
    const backing = std.testing.allocator;
    var region = std.heap.ArenaAllocator.init(backing);
    defer region.deinit();
    const a = region.allocator();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/publication-membership", .{tmp.sub_path}, 0);
    var db = try local.storage_docstore.DocStore.open(backing, path.ptr, .{});
    var open = true;
    defer if (open) db.close();
    const table: domain.Resource = .{ .kind = .table, .id = 3, .sql_relation_id = 100, .parent_id = 2, .name = "docs", .storage_name = "physical:docs" };
    var explicit: records.Publication = .{ .id = 101, .generation = 1, .database_id = 1, .name = "usage_pub", .owner = .auth_disabled, .all_tables = false, .tables = &.{.{ .id = 100, .namespace_id = 2, .definition_digest = @splat(1) }}, .parameters = .{} };
    var all = explicit;
    all.id = 102;
    all.name = "usage_pub_all";
    all.all_tables = true;
    all.tables = &.{};
    var overlap = explicit;
    overlap.id = 103;
    overlap.name = "usage_pub_overlap";
    const Helper = struct {
        fn resource(a2: A, value: records.Publication) !domain.Resource {
            return .{ .kind = .publication, .id = value.id, .parent_id = value.database_id, .name = value.name, .source_json = try std.json.Stringify.valueAlloc(a2, value, .{}) };
        }
        fn publish(a2: A, txn: *local.storage_docstore.DocStore.Txn, value: records.Publication) !void {
            const meta = try storage.readMeta(a2, txn, 7);
            const delta: domain.Delta = .{ .upserts = try a2.dupe(domain.Resource, &.{try resource(a2, value)}), .removes = &.{}, .next_id = @max(meta.next_id, value.id + 1) };
            try storage.applyDelta(a2, txn, 7, delta, meta, @splat(1));
        }
    };
    {
        var txn = try db.beginWriteTxn();
        errdefer txn.abort();
        try std.testing.expectError(error.LogicalReplicationCatalogStateRequired, storage.importState(a, &txn, 7, .{
            .next_id = 103,
            .resources = &.{try Helper.resource(a, all)},
        }));
        const meta = try storage.readMeta(a, &txn, 7);
        try storage.applyDelta(a, &txn, 7, .{ .upserts = try a.dupe(domain.Resource, &.{table}), .removes = &.{}, .next_id = 101 }, meta, @splat(1));
        try Helper.publish(a, &txn, explicit);
        try Helper.publish(a, &txn, all);
        try Helper.publish(a, &txn, overlap);
        try MembershipTestRows.verify(backing, &txn);
        // Altered options change the publication stamp and affected head in
        // the same actual writer; logical table identity remains unchanged.
        explicit.generation += 1;
        explicit.parameters.publish.delete = false;
        try Helper.publish(a, &txn, explicit);
        try MembershipTestRows.verify(backing, &txn);
        try txn.commit();
    }
    try db.sync(true);
    db.close();
    open = false;
    db = try local.storage_docstore.DocStore.open(backing, path.ptr, .{});
    open = true;
    {
        var txn = try db.beginWriteTxn();
        defer txn.abort();
        try MembershipTestRows.verify(backing, &txn);
        const view: storage.View = .{ .alloc = a, .txn = &txn, .group_id = 7, .meta = try storage.readMeta(a, &txn, 7) };
        const actual_table = (try view.tableByLogicalRelationId(100)).?;
        const actual_database = (try view.byId(.database, 1)).?;
        try std.testing.expectEqual(@as(usize, 2), (try read(a, view, .{ .relation = .{ .database = 1, .logical_id = 100 } }, actual_table.sql_publications)).len);
        try std.testing.expectEqual(@as(usize, 1), (try read(a, view, .{ .database = 1 }, actual_database.sql_all_publications)).len);
        var buffer: [256]u8 = undefined;
        const edge = try a.dupe(u8, try key(&buffer, 7, .{ .relation = .{ .database = 1, .logical_id = 100 } }, 101));
        const retained = try a.dupe(u8, try txn.get(edge));
        try txn.delete(edge);
        try std.testing.expectError(error.InvalidLogicalReplicationRecord, MembershipTestRows.verify(backing, &txn));
        try std.testing.expectError(error.InvalidLogicalReplicationRecord, read(a, view, .{ .relation = .{ .database = 1, .logical_id = 100 } }, actual_table.sql_publications));
        try txn.put(edge, retained);
        const orphan = try a.dupe(u8, try key(&buffer, 7, .{ .relation = .{ .database = 1, .logical_id = 100 } }, 99));
        try txn.put(orphan, retained);
        try std.testing.expectError(error.InvalidLogicalReplicationRecord, MembershipTestRows.verify(backing, &txn));
        try txn.delete(orphan);
        var stale = try explicit.stamp(a);
        stale.generation -= 1;
        try txn.put(edge, &encode(stale));
        try std.testing.expectError(error.InvalidLogicalReplicationRecord, MembershipTestRows.verify(backing, &txn));
        try txn.put(edge, retained);
        var stripped = actual_table;
        stripped.sql_publications = null;
        try storage.writeResource(a, &txn, 7, stripped);
        try std.testing.expectError(error.InvalidLogicalReplicationRecord, MembershipTestRows.verify(backing, &txn));
        try std.testing.expectError(error.InvalidLogicalReplicationRecord, read(a, view, .{ .relation = .{ .database = 1, .logical_id = 100 } }, null));

        // Alter must detect a missing authoritative edge even when the owner
        // head was also stripped. Repair both before testing a genuine drop.
        try txn.delete(edge);
        var attempted = explicit;
        attempted.generation += 1;
        try std.testing.expectError(error.InvalidLogicalReplicationRecord, Helper.publish(a, &txn, attempted));
        try txn.put(edge, retained);
        try storage.writeResource(a, &txn, 7, actual_table);

        const removal = try a.dupe(domain.Resource, &.{(try view.byId(.publication, explicit.id)).?});
        try storage.applyDelta(a, &txn, 7, .{ .upserts = &.{}, .removes = removal, .next_id = view.meta.next_id }, view.meta, @splat(9));
        try MembershipTestRows.verify(backing, &txn);
        const remaining = (try view.tableByLogicalRelationId(100)).?;
        const stamps = try read(a, view, .{ .relation = .{ .database = 1, .logical_id = 100 } }, remaining.sql_publications);
        try std.testing.expectEqualSlices(Stamp, &.{try overlap.stamp(a)}, stamps);
        try std.testing.expect((try view.byId(.publication, explicit.id)) == null);
    }
}
