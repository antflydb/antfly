// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Rejoin the existing initial-create/restore reservation before installing a
//! reserved SQL OID. No allocation, actor grant or second journal lives here.
const std = @import("std");
const domain = @import("antfly_local_sources").system_catalog_domain;
const initial = @import("../metadata/fk_generation_publication.zig");
const stages = @import("../metadata/restore_staging.zig");
const A = std.mem.Allocator;

pub const Cache = struct {
    // The enclosing statement arena owns parsed immutable jobs. One point
    // lookup/parse per selected Job, irrespective of its target count.
    jobs: std.AutoHashMapUnmanaged(stages.Id, *stages.Job) = .empty,
    pub fn reserved(self: *Cache, a: A, txn: anytype, group: u64, record: domain.Resource, next_id: u64) !bool {
        const oid = record.sql_relation_id orelse return false;
        if (oid >= next_id) return false;
        try domain.requireRelationOid(oid);
        var key: [256]u8 = undefined;
        if (try optional(txn, try initial.initialKey(&key, group, record.id))) |bytes| {
            if (bytes.len > initial.max_bytes) return error.InvalidCatalogRecord;
            const publication = try std.json.parseFromSliceLeaky(initial.InitialPublication, a, bytes, .{ .allocate = .alloc_always });
            if (!std.mem.eql(u8, &publication.plan_digest, &try publication.plan.digest(a))) return error.InvalidCatalogRecord;
            const plan = publication.plan;
            if (publication.phase == .publishing_child and plan.catalog_id == oid and plan.child.table_id == record.id and plan.namespace_id == record.parent_id and std.mem.eql(u8, plan.logical_name, record.name) and std.mem.eql(u8, plan.child.name, record.storage_name)) return true;
        }
        const owner = (try optional(txn, try stages.identityKey(&key, group, .table, record.id))) orelse return false;
        if (owner.len != 16) return error.InvalidRestoreStaging;
        const id: stages.Id = owner[0..16].*;
        var job = self.jobs.get(id);
        if (job == null) {
            const bytes = (try optional(txn, try stages.jobKey(&key, group, id))) orelse return error.InvalidRestoreStaging;
            if (bytes.len > stages.max_encoded_bytes) return error.InvalidRestoreStaging;
            const stored = try a.create(stages.Job);
            stored.* = try std.json.parseFromSliceLeaky(stages.Job, a, bytes, .{ .allocate = .alloc_always });
            if (!std.mem.eql(u8, &stored.plan.id, &id) or !std.mem.eql(u8, &stored.plan_digest, &try stored.plan.digest(a))) return error.InvalidRestoreStaging;
            const progress_bytes = (try optional(txn, try stages.progressKey(&key, group, id))) orelse return error.InvalidRestoreStaging;
            const progress = try std.json.parseFromSliceLeaky(stages.Progress, a, progress_bytes, .{});
            // Only actual publication can install the new binding. A canceled
            // or merely admitted plan cannot donate its burned counter slot.
            if (!stored.plan.catalogPublicationReady(progress)) return false;
            try self.jobs.put(a, id, stored);
            job = stored;
        }
        for (job.?.plan.targets) |target| {
            if (target.table.table_id != record.id or target.materialized_view != null) continue;
            const binding = target.catalog_binding orelse return false;
            if (binding.sql_relation_id == null or binding.sql_relation_id.? != oid or binding.parent_id != record.parent_id or !std.mem.eql(u8, binding.name, record.name) or !std.mem.eql(u8, target.table.name, record.storage_name)) return false;
            return true;
        }
        return false;
    }
};
fn optional(txn: anytype, key: []const u8) !?[]const u8 {
    return txn.get(key) catch |err| if (err == error.NotFound) null else err;
}

/// Full checkpoint discovery includes plain hidden CTAS/restore reservations,
/// even when neither a Resource nor a callable lease has been published.
pub fn verifyDirectory(backing: A, reader: anytype, group: u64, rows: anytype, canceled: ?*const std.atomic.Value(bool)) !bool {
    const storage = @import("storage.zig");
    const meta = try storage.readMeta(backing, reader, group);
    var prefix_buffer: [160]u8 = undefined;
    const prefix = try stages.jobPrefix(&prefix_buffer, group);
    var selected: std.AutoHashMapUnmanaged(u64, void) = .empty;
    defer selected.deinit(backing);
    var fresh_history: std.AutoHashMapUnmanaged(u64, void) = .empty;
    defer fresh_history.deinit(backing);
    var present = false;
    try rows.reset();
    while (try rows.next()) |row| {
        if (canceled) |flag| if (flag.load(.acquire)) return error.SnapshotBuildCancelled;
        if (!std.mem.startsWith(u8, row.key, prefix)) continue;
        if (row.value.len == 0 or row.value.len > stages.max_encoded_bytes) return error.InvalidRestoreStaging;
        var region = std.heap.ArenaAllocator.init(backing);
        defer region.deinit();
        const a = region.allocator();
        const job = try std.json.parseFromSliceLeaky(stages.Job, a, row.value, .{ .allocate = .alloc_always });
        if (job.plan.targets.len == 0 or job.plan.targets.len > 128) return error.InvalidRestoreStaging;
        const has_oid = for (job.plan.targets) |target| {
            if (target.catalog_binding) |binding| if (binding.sql_relation_id != null) break true;
        } else false;
        if (!has_oid) continue;
        present = true;
        var key: [256]u8 = undefined;
        if (!std.mem.eql(u8, row.key, try stages.jobKey(&key, group, job.plan.id)) or !std.mem.eql(u8, &job.plan_digest, &try job.plan.digest(a))) return error.InvalidRestoreStaging;
        const progress_bytes = (try optional(reader, try stages.progressKey(&key, group, job.plan.id))) orelse return error.InvalidRestoreStaging;
        if (progress_bytes.len == 0 or progress_bytes.len > 64 * 1024) return error.InvalidRestoreStaging;
        const progress = try std.json.parseFromSliceLeaky(stages.Progress, a, progress_bytes, .{});
        if (progress.revision == 0) return error.InvalidRestoreStaging;
        for (job.plan.targets) |target| if (target.catalog_binding) |binding| {
            const oid = binding.sql_relation_id orelse continue;
            try binding.validateLogicalRelationId();
            if (oid >= meta.next_id or binding.id != target.table.table_id or !std.mem.eql(u8, binding.storage_name, target.table.name)) return error.InvalidRestoreStaging;
            const same_object = target.replace != null and target.schema_only_snapshot == null and (target.empty_generation or target.rewrite != null);
            // A canceled independent destination still burned its allocation.
            // Same-object generation histories legitimately reuse one OID.
            if (!same_object and (try fresh_history.getOrPut(backing, oid)).found_existing) return error.InvalidRestoreStaging;
            if (progress.state == .published or progress.state == .canceled) continue;
            if ((try selected.getOrPut(backing, oid)).found_existing) return error.InvalidRestoreStaging;
            const owner = (try optional(reader, try stages.identityKey(&key, group, .table, binding.id))) orelse return error.InvalidRestoreStaging;
            if (!std.mem.eql(u8, owner, &job.plan.id)) return error.InvalidRestoreStaging;
            if (try storage.getTableByLogicalRelationId(a, reader, group, oid)) |current| {
                if (current.value.id == binding.id) continue;
                const replacement = target.replace orelse return error.InvalidRestoreStaging;
                if (!same_object or current.value.id != replacement.table.table_id) return error.InvalidRestoreStaging;
            }
        };
    }
    return present;
}

test "SQL relation OID reservation rejoins exact restore phase binding and immutable plan" {
    var region = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer region.deinit();
    const a = region.allocator();
    const Memory = struct {
        a: A,
        rows: std.StringHashMapUnmanaged([]const u8) = .empty,
        fn put(self: *@This(), key: []const u8, value: anytype) !void {
            try self.rows.put(self.a, try self.a.dupe(u8, key), try std.json.Stringify.valueAlloc(self.a, value, .{}));
        }
        pub fn get(self: *@This(), key: []const u8) anyerror![]const u8 {
            return self.rows.get(key) orelse error.NotFound;
        }
    };
    var db: Memory = .{ .a = a };
    const physical = std.math.maxInt(u64) - 1;
    const record: domain.Resource = .{ .kind = .table, .id = physical, .sql_relation_id = 7, .parent_id = 2, .name = "restored", .storage_name = "hidden:restored" };
    const target: stages.Target = .{ .source_table_id = 42, .table = .{ .table_id = physical, .name = record.storage_name, .schema_json = "{}" }, .catalog_binding = record, .ranges = &.{.{ .table_id = physical, .group_id = 301, .range_id = 301, .start_key = "" }} };
    const plan: stages.Plan = .{ .id = @splat(1), .cohort_digest = @splat(2), .targets = &.{target} };
    const job: stages.Job = .{ .plan = plan, .plan_digest = try plan.digest(a) };
    var key: [256]u8 = undefined;
    try db.rows.put(a, try a.dupe(u8, try stages.identityKey(&key, 17, .table, physical)), &plan.id);
    try db.put(try stages.jobKey(&key, 17, plan.id), job);
    try db.put(try stages.progressKey(&key, 17, plan.id), stages.Progress{ .state = .importing });
    var cache: Cache = .{};
    try std.testing.expect(!try cache.reserved(a, &db, 17, record, 8));
    // A fresh target has no activation owners. Neither the wrong phase
    // nor merely entering validation can donate its reserved OID.
    try db.put(try stages.progressKey(&key, 17, plan.id), stages.Progress{ .state = .activating, .completed_owners = 1 });
    cache = .{};
    try std.testing.expect(!try cache.reserved(a, &db, 17, record, 8));
    try db.put(try stages.progressKey(&key, 17, plan.id), stages.Progress{ .state = .validating });
    cache = .{};
    try std.testing.expect(!try cache.reserved(a, &db, 17, record, 8));
    try db.put(try stages.progressKey(&key, 17, plan.id), stages.Progress{ .state = .validating, .completed_owners = 1 });
    cache = .{};
    try std.testing.expect(try cache.reserved(a, &db, 17, record, 8));
    var forged = record;
    forged.sql_relation_id = 6;
    try std.testing.expect(!try cache.reserved(a, &db, 17, forged, 8));
    forged = record;
    forged.name = "renamed";
    try std.testing.expect(!try cache.reserved(a, &db, 17, forged, 8));
    forged = record;
    forged.parent_id += 1;
    try std.testing.expect(!try cache.reserved(a, &db, 17, forged, 8));
    forged = record;
    forged.storage_name = "another-image-target";
    try std.testing.expect(!try cache.reserved(a, &db, 17, forged, 8));
    try std.testing.expect(!try cache.reserved(a, &db, 18, record, 8));
    try db.put(try stages.progressKey(&key, 17, plan.id), stages.Progress{ .state = .canceled });
    cache = .{};
    try std.testing.expect(!try cache.reserved(a, &db, 17, record, 8));
    var no_oid_target = target;
    no_oid_target.table.table_id = 7;
    no_oid_target.ranges = &.{.{ .table_id = 7, .group_id = 301, .range_id = 301, .start_key = "" }};
    no_oid_target.catalog_binding.?.id = 7;
    no_oid_target.catalog_binding.?.sql_relation_id = null;
    var no_oid_plan = plan;
    no_oid_plan.targets = &.{no_oid_target};
    var no_oid_record = record;
    no_oid_record.id = 7;
    try db.rows.put(a, try a.dupe(u8, try stages.identityKey(&key, 17, .table, 7)), &plan.id);
    try db.put(try stages.jobKey(&key, 17, plan.id), stages.Job{ .plan = no_oid_plan, .plan_digest = try no_oid_plan.digest(a) });
    try db.put(try stages.progressKey(&key, 17, plan.id), stages.Progress{ .state = .validating, .completed_owners = 1 });
    cache = .{};
    try std.testing.expect(!try cache.reserved(a, &db, 17, no_oid_record, 8));
    no_oid_target.catalog_binding.?.sql_relation_id = 7;
    no_oid_plan.targets = &.{no_oid_target};
    try db.put(try stages.jobKey(&key, 17, plan.id), stages.Job{ .plan = no_oid_plan, .plan_digest = try no_oid_plan.digest(a) });
    cache = .{};
    try std.testing.expect(try cache.reserved(a, &db, 17, no_oid_record, 8));
    var corrupt = job;
    corrupt.plan_digest = @splat(9);
    try db.put(try stages.jobKey(&key, 17, plan.id), corrupt);
    cache = .{};
    try std.testing.expectError(error.InvalidRestoreStaging, cache.reserved(a, &db, 17, record, 8));
}

test "SQL relation OID cold reservations include hidden jobs and reject missing counter and duplicate claims" {
    var region = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer region.deinit();
    const a = region.allocator();
    const Memory = struct {
        const Self = @This();
        const Map = std.StringHashMapUnmanaged([]const u8);
        a: A,
        values: Map = .empty,
        pub fn get(self: *@This(), key: []const u8) anyerror![]const u8 {
            return self.values.get(key) orelse error.NotFound;
        }
        pub fn put(self: *@This(), key: []const u8, bytes: []const u8) !void {
            try self.values.put(self.a, try self.a.dupe(u8, key), try self.a.dupe(u8, bytes));
        }
        fn json(self: *@This(), key: []const u8, value: anytype) !void {
            try self.put(key, try std.json.Stringify.valueAlloc(self.a, value, .{}));
        }
        const Rows = struct {
            owner: *Self,
            iterator: Map.Iterator = undefined,
            pub fn reset(self: *@This()) !void {
                self.iterator = self.owner.values.iterator();
            }
            pub fn next(self: *@This()) !?struct { key: []const u8, value: []const u8 } {
                const entry = self.iterator.next() orelse return null;
                return .{ .key = entry.key_ptr.*, .value = entry.value_ptr.* };
            }
        };
    };
    var db: Memory = .{ .a = a };
    var rows: Memory.Rows = .{ .owner = &db };
    var key: [256]u8 = undefined;
    const binding: domain.Resource = .{ .kind = .table, .id = 99, .sql_relation_id = 7, .parent_id = 2, .name = "restored", .storage_name = "hidden:restored" };
    const target: stages.Target = .{ .source_table_id = 42, .table = .{ .table_id = 99, .name = binding.storage_name, .schema_json = "{}" }, .catalog_binding = binding, .ranges = &.{} };
    var plan: stages.Plan = .{ .id = @splat(1), .cohort_digest = @splat(2), .targets = &.{target} };
    const meta_key = "\x00\x00__metadata__:system_catalog:17:meta";
    try db.json(meta_key, domain.Meta{ .next_id = 8 });
    try db.json(try stages.jobKey(&key, 17, plan.id), stages.Job{ .plan = plan, .plan_digest = try plan.digest(a) });
    try db.json(try stages.progressKey(&key, 17, plan.id), stages.Progress{ .state = .importing });
    try db.put(try stages.identityKey(&key, 17, .table, 99), &plan.id);
    try std.testing.expect(try verifyDirectory(a, &db, 17, &rows, null));
    try db.json(meta_key, domain.Meta{ .next_id = 7 });
    try std.testing.expectError(error.InvalidRestoreStaging, verifyDirectory(a, &db, 17, &rows, null));
    try db.json(meta_key, domain.Meta{ .next_id = 8 });
    var second = target;
    second.table.table_id = 100;
    second.catalog_binding.?.id = 100;
    plan.id = @splat(3);
    plan.targets = &.{second};
    try db.json(try stages.jobKey(&key, 17, plan.id), stages.Job{ .plan = plan, .plan_digest = try plan.digest(a) });
    try db.json(try stages.progressKey(&key, 17, plan.id), stages.Progress{ .state = .importing });
    try db.put(try stages.identityKey(&key, 17, .table, 100), &plan.id);
    try std.testing.expectError(error.InvalidRestoreStaging, verifyDirectory(a, &db, 17, &rows, null));
    try db.json(try stages.progressKey(&key, 17, plan.id), stages.Progress{ .state = .canceled });
    try std.testing.expectError(error.InvalidRestoreStaging, verifyDirectory(a, &db, 17, &rows, null));
    second.catalog_binding.?.sql_relation_id = 6;
    plan.targets = &.{second};
    try db.json(try stages.jobKey(&key, 17, plan.id), stages.Job{ .plan = plan, .plan_digest = try plan.digest(a) });
    try std.testing.expect(try verifyDirectory(a, &db, 17, &rows, null));
}
