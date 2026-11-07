// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Pending restore plans pin current callable authority until publish/cancel.
//! These are bounded, indexed dependency leases, not executable schema bodies.
const std = @import("std");
const local = @import("antfly_local_sources");
const domain = local.system_catalog_domain;
const routines = local.sql_catalog.routines.records;
const storage = @import("../system_catalog/storage.zig");
const stages = @import("restore_staging.zig");
const A = std.mem.Allocator;
const leases = @import("../system_catalog/trigger_restore_leases.zig");
const Resource = leases.Resource;
const leaseKey = leases.leaseKey;
fn resources(a: A, txn: anytype, group: u64, plan: stages.Plan) ![]Resource {
    var seen: std.AutoHashMap(Resource, void) = .init(a);
    defer seen.deinit();
    var claims: std.AutoHashMap(u64, local.sql_catalog.trigger_restore.RoutineMove) = .init(a);
    defer claims.deinit();
    for (plan.targets) |target| if (target.trigger_handoff) |handoff| for (handoff.routines) |claim| {
        if (claim.destination_generation == 0) continue;
        const slot = try claims.getOrPut(claim.destination.id);
        if (slot.found_existing) {
            const previous = slot.value_ptr.*;
            if (previous.destination_generation != claim.destination_generation or previous.destination.namespace_id != claim.destination.namespace_id) return error.InvalidRestoreStaging;
            continue;
        }
        if (claims.count() > 256) return error.SqlProgramLimitExceeded;
        slot.value_ptr.* = claim;
        var function = (try storage.getById(a, txn, group, .routine, claim.destination.id)) orelse return error.CatalogGenerationChanged;
        defer function.deinit();
        try storage.requireScopeResourceWritable(a, txn, group, function.value, try storage.readMeta(a, txn, group));
        var record = try routines.Record.decode(a, function.value.source_json);
        defer record.deinit();
        if (function.value.parent_id != claim.destination.namespace_id or record.value.id != claim.destination.id or record.value.generation != claim.destination_generation or record.value.kind != .function or !record.value.definition.returns_trigger or record.value.definition.arguments.len != 0) return error.CatalogGenerationChanged;
        var namespace = (try storage.getById(a, txn, group, .namespace, function.value.parent_id)) orelse return error.CatalogGenerationChanged;
        defer namespace.deinit();
        var database = (try storage.getById(a, txn, group, .database, namespace.value.parent_id)) orelse return error.CatalogGenerationChanged;
        defer database.deinit();
        try seen.put(.{ .kind = .routine, .id = function.value.id }, {});
        try seen.put(.{ .kind = .namespace, .id = namespace.value.id }, {});
        try seen.put(.{ .kind = .database, .id = database.value.id }, {});
    };
    const result = try a.alloc(Resource, seen.count());
    var iterator = seen.keyIterator();
    var index: usize = 0;
    while (iterator.next()) |resource| : (index += 1) result[index] = resource.*;
    return result;
}
/// Validate before any reservation writes. Every later lease operation uses
/// the same transaction and immutable metadata plan, never a cached catalog.
pub fn validate(a: A, txn: anytype, group: u64, plan: stages.Plan) !void {
    const items = try resources(a, txn, group, plan);
    a.free(items);
}
pub fn acquire(a: A, txn: anytype, group: u64, plan: stages.Plan) !void {
    const items = try resources(a, txn, group, plan);
    defer a.free(items);
    var buffer: [256]u8 = undefined;
    for (items) |resource| try txn.put(try leaseKey(&buffer, group, resource, plan.id), &.{1});
}
pub fn requireHeld(a: A, txn: anytype, group: u64, plan: stages.Plan) !void {
    const items = try resources(a, txn, group, plan);
    defer a.free(items);
    var buffer: [256]u8 = undefined;
    for (items) |resource| {
        const bytes = txn.get(try leaseKey(&buffer, group, resource, plan.id)) catch |failure| return if (failure == error.NotFound) error.InvalidRestoreStaging else failure;
        if (!std.mem.eql(u8, bytes, &.{1})) return error.InvalidRestoreStaging;
    }
}
pub fn release(a: A, txn: anytype, group: u64, plan: stages.Plan) !void {
    const items = try resources(a, txn, group, plan);
    defer a.free(items);
    var buffer: [256]u8 = undefined;
    for (items) |resource| try txn.delete(try leaseKey(&buffer, group, resource, plan.id));
}
/// A forward exact-key proof and canonical reverse cardinality establish a
/// bijection between pending immutable plans and stored leases. Each bounded
/// plan is decoded once, in its own arena; a routine does not clone or scan the
/// catalog or repeatedly decode its owning restore job. Terminal plans retain
/// history but must retain no execution-authority lease.
pub fn verifyDirectory(backing: A, reader: anytype, group: u64, rows: anytype, canceled: ?*const std.atomic.Value(bool)) !bool {
    var job_buffer: [160]u8 = undefined;
    const job_prefix = try stages.jobPrefix(&job_buffer, group);
    var expected: usize = 0;
    var present = false;
    try rows.reset();
    while (try rows.next()) |row| {
        if (canceled) |flag| if (flag.load(.acquire)) return error.SnapshotBuildCancelled;
        if (!std.mem.startsWith(u8, row.key, job_prefix)) continue;
        if (row.value.len == 0 or row.value.len > stages.max_encoded_bytes) return error.InvalidRestoreStaging;
        var arena: std.heap.ArenaAllocator = .init(backing);
        defer arena.deinit();
        const a = arena.allocator();
        const job = try std.json.parseFromSliceLeaky(stages.Job, a, row.value, .{ .allocate = .alloc_always });
        var key_buffer: [256]u8 = undefined;
        if (!std.mem.eql(u8, row.key, try stages.jobKey(&key_buffer, group, job.plan.id))) return error.InvalidRestoreStaging;
        const claims_present = found: for (job.plan.targets) |target| {
            if (target.trigger_handoff) |handoff| for (handoff.routines) |claim| {
                if (claim.destination_generation != 0) break :found true;
            };
        } else false;
        if (!claims_present) continue;
        present = true;
        if (!std.mem.eql(u8, &job.plan_digest, &try job.plan.digest(a))) return error.InvalidRestoreStaging;
        const progress_bytes = reader.get(try stages.progressKey(&key_buffer, group, job.plan.id)) catch |failure| return if (failure == error.NotFound) error.InvalidRestoreStaging else failure;
        if (progress_bytes.len == 0 or progress_bytes.len > 64 * 1024) return error.InvalidRestoreStaging;
        const progress = try std.json.parseFromSliceLeaky(stages.Progress, a, progress_bytes, .{});
        if (progress.revision == 0) return error.InvalidRestoreStaging;
        if (progress.state == .published or progress.state == .canceled) continue;
        const items = try resources(a, reader, group, job.plan);
        for (items) |resource| {
            const bytes = reader.get(try leaseKey(&key_buffer, group, resource, job.plan.id)) catch |failure| return if (failure == error.NotFound) error.InvalidRestoreStaging else failure;
            if (!std.mem.eql(u8, bytes, &.{1})) return error.InvalidRestoreStaging;
        }
        expected = std.math.add(usize, expected, items.len) catch return error.InvalidRestoreStaging;
    }
    var actual: usize = 0;
    try rows.reset();
    while (try rows.next()) |row| {
        if (canceled) |flag| if (flag.load(.acquire)) return error.SnapshotBuildCancelled;
        if (try leases.address(group, row.key)) |_| {
            if (!std.mem.eql(u8, row.value, &.{1})) return error.InvalidRestoreStaging;
            actual = std.math.add(usize, actual, 1) catch return error.InvalidRestoreStaging;
        }
    }
    // Every expected key was independently observed above. Canonical unique
    // reverse keys therefore cannot hide an orphan, terminal or extra lease,
    // even when its count happens to compensate for a missing expected key.
    if (actual != expected) return error.InvalidRestoreStaging;
    return present;
}

pub const requireUnleasedDelta = @import("../system_catalog/trigger_restore_leases.zig").requireUnleasedDelta;

test "trigger restore callable leases pin generation namespace and enclosing authority until confirmed publication or cancellation" {
    const Fake = struct {
        const Self = @This();
        a: A,
        values: std.StringHashMapUnmanaged([]const u8) = .empty,
        pub fn get(self: *@This(), key: []const u8) ![]const u8 {
            return self.values.get(key) orelse error.NotFound;
        }
        fn put(self: *@This(), key: []const u8, value: []const u8) !void {
            try self.values.put(self.a, try self.a.dupe(u8, key), try self.a.dupe(u8, value));
        }
        fn delete(self: *@This(), key: []const u8) !void {
            _ = self.values.remove(key);
        }
        const Row = struct { key: []const u8, value: []const u8 };
        const Cursor = struct {
            owner: *Self,
            keys: []const []const u8,
            index: usize = 0,
            pub fn close(_: *@This()) void {}
            pub fn seekAtOrAfter(self: *@This(), key: []const u8) !?Row {
                self.index = 0;
                while (self.index < self.keys.len and std.mem.order(u8, self.keys[self.index], key) == .lt) self.index += 1;
                return self.current();
            }
            fn current(self: *@This()) ?Row {
                if (self.index == self.keys.len) return null;
                return .{ .key = self.keys[self.index], .value = self.owner.values.get(self.keys[self.index]).? };
            }
            fn next(self: *@This()) !?Row {
                if (self.index < self.keys.len) self.index += 1;
                return self.current();
            }
        };
        pub fn openCursor(self: *@This()) !Cursor {
            const keys = try self.a.alloc([]const u8, self.values.count());
            var iterator = self.values.keyIterator();
            var i: usize = 0;
            while (iterator.next()) |key| : (i += 1) keys[i] = key.*;
            std.mem.sort([]const u8, keys, {}, struct {
                fn less(_: void, x: []const u8, y: []const u8) bool {
                    return std.mem.order(u8, x, y) == .lt;
                }
            }.less);
            return .{ .owner = self, .keys = keys };
        }
    };

    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var txn: Fake = .{ .a = a };
    var record: routines.Record = .{ .id = 9, .generation = 1, .definition = .{ .returns_trigger = true, .arguments = &.{}, .language = .plpgsql, .body = "BEGIN RETURN NEW; END", .owner = .auth_disabled } };
    var resource: domain.Resource = .{ .id = 9, .kind = .routine, .parent_id = 2, .name = "touch", .signature = "", .source_json = try record.encode(a) };
    const namespace: domain.Resource = .{ .id = 2, .kind = .namespace, .parent_id = 1, .name = "public" };
    const database: domain.Resource = .{ .id = 1, .kind = .database, .parent_id = 0, .name = "default" };
    for ([_]domain.Resource{ resource, namespace, database }) |value| try txn.put(try storage.recordKeyAlloc(a, 1, value.kind, value.id), try std.json.Stringify.valueAlloc(a, value, .{}));
    const claim: local.sql_catalog.trigger_restore.RoutineMove = .{ .source_id = 90, .destination = .{ .id = 9, .namespace_id = 2, .database = "default", .namespace = "public", .name = "touch" }, .destination_generation = 1 };
    const handoff: stages.TriggerHandoff = .{ .source_schema_json = "{}", .source_columns = .{ .format = 1, .table_id = 70, .epoch = .{ .version = 1, .digest = @splat(1) }, .next_id = 1, .columns = &.{} }, .destination_incarnation = @splat(1), .routines = &.{claim} };
    const target: stages.Target = .{ .source_table_id = 70, .table = .{ .table_id = 71, .name = "restored" }, .ranges = &.{}, .trigger_handoff = handoff };
    const plan: stages.Plan = .{ .id = @splat(1), .cohort_digest = @splat(2), .targets = &.{target} };
    try validate(a, &txn, 1, plan);
    try acquire(a, &txn, 1, plan);
    try requireHeld(a, &txn, 1, plan);
    for ([_]domain.Resource{ resource, namespace, database }) |value| {
        var changed = [_]domain.Resource{value};
        try std.testing.expectError(if (value.kind == .routine) error.SqlDependentCallable else error.TableTransitionActive, requireUnleasedDelta(&txn, 1, .{ .upserts = &changed, .removes = &.{}, .next_id = 10 }));
        try std.testing.expectError(if (value.kind == .routine) error.SqlDependentCallable else error.TableTransitionActive, requireUnleasedDelta(&txn, 1, .{ .upserts = &.{}, .removes = &changed, .next_id = 10 }));
    }
    // Even if an unguarded/internal caller corrupts a claimed record, the
    // predecision/finalpublication validator refuses the changed generation.
    record.generation = 2;
    resource.source_json = try record.encode(a);
    try txn.put(try storage.recordKeyAlloc(a, 1, .routine, 9), try std.json.Stringify.valueAlloc(a, resource, .{}));
    try std.testing.expectError(error.CatalogGenerationChanged, requireHeld(a, &txn, 1, plan));
    record.generation = 1;
    resource.source_json = try record.encode(a);
    try txn.put(try storage.recordKeyAlloc(a, 1, .routine, 9), try std.json.Stringify.valueAlloc(a, resource, .{}));
    resource.parent_id = 3;
    try txn.put(try storage.recordKeyAlloc(a, 1, .routine, 9), try std.json.Stringify.valueAlloc(a, resource, .{}));
    try std.testing.expectError(error.CatalogGenerationChanged, requireHeld(a, &txn, 1, plan));
    resource.parent_id = 2;
    try txn.put(try storage.recordKeyAlloc(a, 1, .routine, 9), try std.json.Stringify.valueAlloc(a, resource, .{}));
    try release(a, &txn, 1, plan);
    var replacement = [_]domain.Resource{resource};
    try requireUnleasedDelta(&txn, 1, .{ .upserts = &replacement, .removes = &.{}, .next_id = 10 });
    try std.testing.expectError(error.InvalidRestoreStaging, requireHeld(a, &txn, 1, plan));
}

test "trigger restore cold lease directory rejects missing orphan terminal and forged progress authority" {
    const Memory = struct {
        const Self = @This();
        a: A,
        values: std.StringHashMapUnmanaged([]const u8) = .empty,
        pub fn get(self: *Self, key: []const u8) ![]const u8 {
            return self.values.get(key) orelse error.NotFound;
        }
        fn put(self: *Self, key: []const u8, value: []const u8) !void {
            try self.values.put(self.a, try self.a.dupe(u8, key), try self.a.dupe(u8, value));
        }
        fn remove(self: *Self, key: []const u8) void {
            _ = self.values.remove(key);
        }
        pub fn delete(self: *Self, key: []const u8) !void {
            self.remove(key);
        }
        const Rows = struct {
            owner: *Self,
            iterator: std.StringHashMapUnmanaged([]const u8).Iterator = undefined,
            pub fn reset(self: *@This()) !void {
                self.iterator = self.owner.values.iterator();
            }
            pub fn next(self: *@This()) !?struct { key: []const u8, value: []const u8 } {
                const row = self.iterator.next() orelse return null;
                return .{ .key = row.key_ptr.*, .value = row.value_ptr.* };
            }
        };
    };
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var reader: Memory = .{ .a = a };
    const routine: routines.Record = .{ .id = 9, .generation = 3, .definition = .{ .returns_trigger = true, .arguments = &.{}, .language = .plpgsql, .body = "BEGIN RETURN NEW; END", .owner = .auth_disabled } };
    const definitions = [_]domain.Resource{
        .{ .id = 9, .kind = .routine, .parent_id = 2, .name = "touch", .signature = "", .source_json = try routine.encode(a) },
        .{ .id = 2, .kind = .namespace, .parent_id = 1, .name = "public" },
        .{ .id = 1, .kind = .database, .parent_id = 0, .name = "default" },
    };
    for (definitions) |record| try reader.put(try storage.recordKeyAlloc(a, 1, record.kind, record.id), try std.json.Stringify.valueAlloc(a, record, .{}));
    const claim: local.sql_catalog.trigger_restore.RoutineMove = .{ .source_id = 90, .destination = .{ .id = 9, .namespace_id = 2, .database = "default", .namespace = "public", .name = "touch" }, .destination_generation = 3 };
    const target: stages.Target = .{ .source_table_id = 70, .table = .{ .table_id = 71, .name = "restored" }, .ranges = &.{}, .trigger_handoff = .{ .source_schema_json = "{}", .source_columns = .{ .format = 1, .table_id = 70, .epoch = .{ .version = 1, .digest = @splat(1) }, .next_id = 1, .columns = &.{} }, .destination_incarnation = @splat(1), .routines = &.{claim} } };
    const plan: stages.Plan = .{ .id = @splat(1), .cohort_digest = @splat(2), .targets = &.{target} };
    const job: stages.Job = .{ .plan = plan, .plan_digest = try plan.digest(a) };
    var buffer: [256]u8 = undefined;
    try reader.put(try stages.jobKey(&buffer, 1, plan.id), try std.json.Stringify.valueAlloc(a, job, .{}));
    const progress_key = try a.dupe(u8, try stages.progressKey(&buffer, 1, plan.id));
    try reader.put(progress_key, try std.json.Stringify.valueAlloc(a, stages.Progress{ .state = .validating, .revision = 2 }, .{}));
    try acquire(a, &reader, 1, plan);
    var rows: Memory.Rows = .{ .owner = &reader };
    try std.testing.expect(try verifyDirectory(a, &reader, 1, &rows, null));
    const routine_key = try a.dupe(u8, try leaseKey(&buffer, 1, .{ .kind = .routine, .id = 9 }, plan.id));
    reader.remove(routine_key);
    try std.testing.expectError(error.InvalidRestoreStaging, verifyDirectory(a, &reader, 1, &rows, null));
    try acquire(a, &reader, 1, plan);
    const orphan = try a.dupe(u8, try leaseKey(&buffer, 1, .{ .kind = .routine, .id = 99 }, @splat(7)));
    try reader.put(orphan, &.{1});
    try std.testing.expectError(error.InvalidRestoreStaging, verifyDirectory(a, &reader, 1, &rows, null));
    reader.remove(orphan);
    // A malformed key cannot evade the reverse directory proof.
    const noncanonical = try std.fmt.allocPrint(a, "{s}0", .{routine_key});
    try reader.put(noncanonical, &.{1});
    try std.testing.expectError(error.InvalidRestoreStaging, verifyDirectory(a, &reader, 1, &rows, null));
    reader.remove(noncanonical);
    // The immutable Job retains its original state; only current Progress
    // decides whether the lease must exist or must have been released.
    try reader.put(progress_key, try std.json.Stringify.valueAlloc(a, stages.Progress{ .state = .published, .revision = 3 }, .{}));
    try std.testing.expectError(error.InvalidRestoreStaging, verifyDirectory(a, &reader, 1, &rows, null));
    try release(a, &reader, 1, plan);
    try std.testing.expect(try verifyDirectory(a, &reader, 1, &rows, null));
    try reader.put(progress_key, try std.json.Stringify.valueAlloc(a, stages.Progress{ .state = .validating, .revision = 0 }, .{}));
    try std.testing.expectError(error.InvalidRestoreStaging, verifyDirectory(a, &reader, 1, &rows, null));
    reader.remove(progress_key);
    try std.testing.expectError(error.InvalidRestoreStaging, verifyDirectory(a, &reader, 1, &rows, null));
    var canceled = std.atomic.Value(bool).init(true);
    try std.testing.expectError(error.SnapshotBuildCancelled, verifyDirectory(a, &reader, 1, &rows, &canceled));
}
