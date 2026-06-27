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

const std = @import("std");

const catalog_resources = @import("catalog_resources.zig");
const table_catalog = @import("table_catalog.zig");
const metadata_api = @import("../metadata/api.zig");
const metadata_table_manager = @import("../metadata/table_manager.zig");
const runtime_schema_mod = @import("../storage/schema.zig");
const sql_adapter = @import("../sql/mod.zig");
const tables_api = @import("tables.zig");

// Catalog job planning owns deterministic durable job records. Execution stays
// with the table/range write path that can claim and drain range-local work.
pub fn scheduleSchemaRewriteJobsForAppliedDdlOnService(
    svc: anytype,
    alloc: std.mem.Allocator,
    applied: tables_api.AppliedRelationalSqlDdlRecord,
) !void {
    if (applied.dropped_table or applied.work_items.len == 0) return;
    const ServiceType = @TypeOf(svc);
    const ServiceDeclType = switch (@typeInfo(ServiceType)) {
        .pointer => |pointer| pointer.child,
        else => ServiceType,
    };
    if (!@hasDecl(ServiceDeclType, "adminSnapshot") or
        !@hasDecl(ServiceDeclType, "freeAdminSnapshot") or
        !@hasDecl(ServiceDeclType, "upsertSchemaRewriteJob"))
    {
        return error.UnsupportedOperation;
    }
    var snapshot = try svc.adminSnapshot();
    defer svc.freeAdminSnapshot(&snapshot);
    try scheduleSchemaRewriteJobsForAppliedDdlSnapshot(svc, alloc, snapshot.ranges, applied);
}

pub fn scheduleSchemaRewriteJobsForAppliedDdlSnapshot(
    scheduler: anytype,
    alloc: std.mem.Allocator,
    ranges: []const metadata_table_manager.RangeRecord,
    applied: tables_api.AppliedRelationalSqlDdlRecord,
) !void {
    if (applied.dropped_table or applied.work_items.len == 0) return;
    var ordinal: u32 = 0;
    for (applied.work_items) |item| {
        switch (item.action) {
            .validate => if (item.subject != .table or item.reason != .constraints) return error.UnsupportedSqlShape,
            .rewrite => if (item.rewrite_expression == null and item.row_rewrite_plan.empty() and !item.full_row_rewrite) return error.UnsupportedSqlShape,
            else => continue,
        }
        ordinal += 1;
        for (ranges) |range| {
            if (range.table_id != applied.table.table_id) continue;
            const job = try schemaRewriteJobForAppliedDdlWorkItem(alloc, applied.table, range, item, ordinal);
            try scheduler.upsertSchemaRewriteJob(job);
        }
    }
}

pub fn schemaRewriteJobForAppliedDdlWorkItem(
    alloc: std.mem.Allocator,
    table: metadata_table_manager.TableRecord,
    range: metadata_table_manager.RangeRecord,
    item: sql_adapter.AppliedDdlWorkItem,
    ordinal: u32,
) !metadata_table_manager.SchemaRewriteJobRecord {
    var hasher = std.hash.Wyhash.init(0x5352514a);
    hasher.update(std.mem.asBytes(&table.table_id));
    hasher.update(&[_]u8{0});
    hasher.update(table.schema_json);
    hasher.update(&[_]u8{0});
    hasher.update(@tagName(item.action));
    hasher.update(&[_]u8{0});
    hasher.update(@tagName(item.subject));
    hasher.update(&[_]u8{0});
    hasher.update(@tagName(item.reason));
    hasher.update(&[_]u8{0});
    hasher.update(std.mem.asBytes(&range.group_id));
    hasher.update(&[_]u8{0});
    hasher.update(range.start_key);
    hasher.update(&[_]u8{0});
    if (range.end_key) |end_key| hasher.update(end_key);
    hasher.update(&[_]u8{0});
    hasher.update(std.mem.asBytes(&ordinal));
    hasher.update(&[_]u8{0});
    if (item.rewrite_expression) |rewrite| {
        hasher.update(rewrite.target_column);
        hasher.update(&[_]u8{0});
        const expression = try sql_adapter.rowRewriteExpressionFingerprintAlloc(alloc, rewrite.expression);
        defer alloc.free(expression);
        hasher.update(expression);
    }
    if (item.full_row_rewrite) {
        hasher.update("full_row_rewrite");
        hasher.update(&[_]u8{0});
    }
    for (item.row_rewrite_plan.renames) |rename| {
        hasher.update("rename");
        hasher.update(&[_]u8{0});
        hasher.update(rename.old_path);
        hasher.update(&[_]u8{0});
        hasher.update(rename.new_path);
        hasher.update(&[_]u8{0});
    }
    for (item.row_rewrite_plan.drops) |drop| {
        hasher.update("drop");
        hasher.update(&[_]u8{0});
        hasher.update(drop);
        hasher.update(&[_]u8{0});
    }
    const job_id = nonZeroId(hasher.final());
    const schema_generation = metadata_table_manager.schemaRewriteGenerationForSchemaJson(table.schema_json);
    return .{
        .job_id = job_id,
        .table_id = table.table_id,
        .group_id = range.group_id,
        .schema_generation = schema_generation,
        .action = @tagName(item.action),
        .reason = @tagName(item.reason),
        .start_row_key = range.start_key,
        .end_row_key = range.end_key,
        .target_column = if (item.rewrite_expression) |rewrite| rewrite.target_column else "",
        .expression = if (item.rewrite_expression) |rewrite| rewrite.expression else null,
        .full_row_rewrite = item.full_row_rewrite,
        .rewrite_renames = schemaRewriteRenamesForAppliedRowRewritePlan(item.row_rewrite_plan),
        .rewrite_drops = item.row_rewrite_plan.drops,
    };
}

pub fn appliedDdlHasSchemaRewriteWork(applied: tables_api.AppliedRelationalSqlDdlRecord) bool {
    if (applied.dropped_table) return false;
    for (applied.work_items) |item| {
        switch (item.action) {
            .rewrite => if (item.subject == .table and item.reason == .row_images) return true,
            .validate => if (item.subject == .table and item.reason == .constraints) return true,
            else => {},
        }
    }
    return false;
}

pub fn tableEmptyingJobForRange(
    table: metadata_table_manager.TableRecord,
    range: metadata_table_manager.RangeRecord,
    affected_table_ids: []const u64,
    restart_identity: bool,
    cascade: bool,
) metadata_table_manager.TableEmptyingJobRecord {
    const schema_generation = metadata_table_manager.schemaRewriteGenerationForSchemaJson(table.schema_json);
    var record = metadata_table_manager.TableEmptyingJobRecord{
        .job_id = 0,
        .table_id = table.table_id,
        .group_id = range.group_id,
        .schema_generation = schema_generation,
        .start_row_key = range.start_key,
        .end_row_key = range.end_key,
        .affected_table_ids = affected_table_ids,
        .restart_identity = restart_identity,
        .cascade = cascade,
    };
    record.job_id = metadata_table_manager.stableTableEmptyingJobId(record);
    return record;
}

pub fn scheduleTableEmptyingJobsForTableOnService(
    svc: anytype,
    table: metadata_table_manager.TableRecord,
    affected_table_ids: []const u64,
    restart_identity: bool,
    cascade: bool,
) !usize {
    const ServiceType = @TypeOf(svc);
    const ServiceDeclType = switch (@typeInfo(ServiceType)) {
        .pointer => |pointer| pointer.child,
        else => ServiceType,
    };
    if (!@hasDecl(ServiceDeclType, "adminSnapshot") or
        !@hasDecl(ServiceDeclType, "freeAdminSnapshot") or
        !@hasDecl(ServiceDeclType, "upsertTableEmptyingJob"))
    {
        return error.UnsupportedOperation;
    }
    var snapshot = try svc.adminSnapshot();
    defer svc.freeAdminSnapshot(&snapshot);
    return try scheduleTableEmptyingJobsForTableSnapshot(svc, snapshot.ranges, table, affected_table_ids, restart_identity, cascade);
}

pub fn scheduleTableEmptyingJobsForTableSnapshot(
    scheduler: anytype,
    ranges: []const metadata_table_manager.RangeRecord,
    table: metadata_table_manager.TableRecord,
    affected_table_ids: []const u64,
    restart_identity: bool,
    cascade: bool,
) !usize {
    var scheduled: usize = 0;
    for (ranges) |range| {
        if (range.table_id != table.table_id) continue;
        const job = tableEmptyingJobForRange(table, range, affected_table_ids, restart_identity, cascade);
        try scheduler.upsertTableEmptyingJob(job);
        scheduled += 1;
    }
    return scheduled;
}

fn findTableById(snapshot: *const metadata_api.AdminSnapshot, table_id: u64) ?metadata_table_manager.TableRecord {
    for (snapshot.tables) |table| {
        if (table.table_id == table_id) return table;
    }
    return null;
}

fn findTableEmptyingJobById(
    snapshot: *const metadata_api.AdminSnapshot,
    job_id: u64,
) ?metadata_table_manager.TableEmptyingJobRecord {
    for (snapshot.table_emptying_jobs) |record| {
        if (record.job_id == job_id) return record;
    }
    return null;
}

fn tableEmptyingJobInvolvesTable(record: metadata_table_manager.TableEmptyingJobRecord, table_id: u64) bool {
    if (record.table_id == table_id) return true;
    for (record.affected_table_ids) |affected_table_id| {
        if (affected_table_id == table_id) return true;
    }
    return false;
}

fn appendUniqueTableEmptyingJobId(alloc: std.mem.Allocator, job_ids: *std.ArrayListUnmanaged(u64), job_id: u64) !void {
    for (job_ids.items) |existing| {
        if (existing == job_id) return;
    }
    try job_ids.append(alloc, job_id);
}

fn completedTableEmptyingBarrierJobIdsForCandidateAlloc(
    alloc: std.mem.Allocator,
    snapshot: *const metadata_api.AdminSnapshot,
    candidate: metadata_table_manager.TableEmptyingJobRecord,
) !?[]u64 {
    if (!metadata_table_manager.tableEmptyingJobComplete(candidate)) return null;
    if (candidate.affected_table_ids.len == 0) return error.InvalidTableEmptyingJob;

    var completed_job_ids = std.ArrayListUnmanaged(u64).empty;
    errdefer completed_job_ids.deinit(alloc);

    for (candidate.affected_table_ids) |affected_table_id| {
        const table = findTableById(snapshot, affected_table_id) orelse return error.TableNotFound;
        var range_count: usize = 0;
        for (snapshot.ranges) |range| {
            if (range.table_id != affected_table_id) continue;
            range_count += 1;
            const expected = tableEmptyingJobForRange(
                table,
                range,
                candidate.affected_table_ids,
                candidate.restart_identity,
                candidate.cascade,
            );
            const record = findTableEmptyingJobById(snapshot, expected.job_id) orelse return null;
            if (std.mem.eql(u8, record.state, metadata_table_manager.table_emptying_invalid)) return error.TableEmptyingJobInvalid;
            if (!metadata_table_manager.tableEmptyingJobComplete(record)) return null;
            try appendUniqueTableEmptyingJobId(alloc, &completed_job_ids, record.job_id);
        }
        if (range_count == 0) return error.InvalidTableEmptyingJob;
    }

    return try completed_job_ids.toOwnedSlice(alloc);
}

pub fn completedTableEmptyingBarrierJobIdsForTableAlloc(
    alloc: std.mem.Allocator,
    snapshot: *const metadata_api.AdminSnapshot,
    table_id: u64,
) ![]u64 {
    var completed_job_ids = std.ArrayListUnmanaged(u64).empty;
    errdefer completed_job_ids.deinit(alloc);

    for (snapshot.table_emptying_jobs) |candidate| {
        if (!tableEmptyingJobInvolvesTable(candidate, table_id)) continue;
        var barrier_job_ids = (try completedTableEmptyingBarrierJobIdsForCandidateAlloc(alloc, snapshot, candidate)) orelse continue;
        defer alloc.free(barrier_job_ids);
        for (barrier_job_ids) |job_id| try appendUniqueTableEmptyingJobId(alloc, &completed_job_ids, job_id);
    }

    return try completed_job_ids.toOwnedSlice(alloc);
}

pub fn promoteCompletedTableEmptyingBarriersForTableOnServiceAlloc(
    alloc: std.mem.Allocator,
    service: anytype,
    table_name: []const u8,
) !usize {
    const ServiceType = @TypeOf(service);
    const ServiceDeclType = switch (@typeInfo(ServiceType)) {
        .pointer => |pointer| pointer.child,
        else => ServiceType,
    };
    if (!@hasDecl(ServiceDeclType, "adminSnapshot") or
        !@hasDecl(ServiceDeclType, "freeAdminSnapshot") or
        !@hasDecl(ServiceDeclType, "removeTableEmptyingJob"))
    {
        return error.UnsupportedOperation;
    }

    var snapshot = try service.adminSnapshot();
    defer service.freeAdminSnapshot(&snapshot);
    const table = tables_api.findTableByName(&snapshot, table_name) orelse return error.TableNotFound;
    const job_ids = try completedTableEmptyingBarrierJobIdsForTableAlloc(alloc, &snapshot, table.table_id);
    defer alloc.free(job_ids);

    for (job_ids) |job_id| try service.removeTableEmptyingJob(job_id);
    return job_ids.len;
}

pub fn promoteCompletedTableEmptyingBarriersForTableAlloc(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
) !usize {
    return try promoteCompletedTableEmptyingBarriersForTableOnServiceAlloc(alloc, catalog, table_name);
}

fn schemaRewriteRenamesForAppliedRowRewritePlan(plan: sql_adapter.AppliedDdlRowRewritePlan) []const metadata_table_manager.SchemaRewriteRename {
    if (plan.renames.len == 0) return &.{};
    const ptr: [*]const metadata_table_manager.SchemaRewriteRename = @ptrCast(@alignCast(plan.renames.ptr));
    return ptr[0..plan.renames.len];
}

fn nonZeroId(value: u64) u64 {
    return if (value == 0) 1 else value;
}

test "catalog jobs schedules typed schema rewrite jobs from applied SQL DDL work" {
    const alloc = std.testing.allocator;
    const FakeService = struct {
        ranges: [2]metadata_table_manager.RangeRecord = .{
            .{ .group_id = 9001, .range_id = 9101, .table_id = 77, .start_key = "", .end_key = "m" },
            .{ .group_id = 9002, .range_id = 9102, .table_id = 77, .start_key = "m", .end_key = null },
        },
        jobs: std.ArrayListUnmanaged(metadata_table_manager.SchemaRewriteJobRecord) = .empty,

        fn deinit(self: *@This(), allocator: std.mem.Allocator) void {
            for (self.jobs.items) |record| metadata_table_manager.freeSchemaRewriteJob(allocator, record);
            self.jobs.deinit(allocator);
            self.* = undefined;
        }

        pub fn adminSnapshot(self: *@This()) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = &.{},
                .ranges = self.ranges[0..],
                .stores = &.{},
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }

        fn freeAdminSnapshot(_: *@This(), _: *metadata_api.AdminSnapshot) void {}

        fn upsertSchemaRewriteJob(self: *@This(), record: metadata_table_manager.SchemaRewriteJobRecord) !void {
            const owned = try metadata_table_manager.cloneSchemaRewriteJob(std.testing.allocator, record);
            errdefer metadata_table_manager.freeSchemaRewriteJob(std.testing.allocator, owned);
            for (self.jobs.items) |*existing| {
                if (existing.job_id != record.job_id) continue;
                metadata_table_manager.freeSchemaRewriteJob(std.testing.allocator, existing.*);
                existing.* = owned;
                return;
            }
            try self.jobs.append(std.testing.allocator, owned);
        }
    };

    const table = try metadata_table_manager.cloneTable(alloc, .{
        .table_id = 77,
        .name = "events",
        .schema_json = "{\"version\":2,\"storage_mode\":\"relational\"}",
    });
    errdefer metadata_table_manager.freeTable(alloc, table);
    const target_column = try alloc.dupe(u8, "status_key");
    errdefer alloc.free(target_column);
    const expression = try runtime_schema_mod.cloneRelationalRowsExpressionAlloc(alloc, .{
        .kind = .lower,
        .operands = &.{.{ .kind = .field, .field = "status" }},
    });
    errdefer runtime_schema_mod.freeRelationalRowsExpression(alloc, expression);
    const work_items = try alloc.alloc(sql_adapter.AppliedDdlWorkItem, 1);
    work_items[0] = .{
        .action = .rewrite,
        .subject = .table,
        .reason = .row_images,
        .rewrite_expression = .{
            .target_column = target_column,
            .expression = expression,
        },
    };
    var applied: tables_api.AppliedRelationalSqlDdlRecord = .{
        .table = table,
        .rewrite_required = true,
        .work_items = work_items,
    };

    var service = FakeService{};
    defer service.deinit(alloc);
    try scheduleSchemaRewriteJobsForAppliedDdlOnService(&service, alloc, applied);
    try scheduleSchemaRewriteJobsForAppliedDdlOnService(&service, alloc, applied);

    const same_expression_job = try schemaRewriteJobForAppliedDdlWorkItem(alloc, applied.table, service.ranges[0], applied.work_items[0], 1);
    const alternate_expression = try runtime_schema_mod.cloneRelationalRowsExpressionAlloc(alloc, .{
        .kind = .upper,
        .operands = &.{.{ .kind = .field, .field = "status" }},
    });
    defer runtime_schema_mod.freeRelationalRowsExpression(alloc, alternate_expression);
    const alternate_item: sql_adapter.AppliedDdlWorkItem = .{
        .action = .rewrite,
        .subject = .table,
        .reason = .row_images,
        .rewrite_expression = .{
            .target_column = applied.work_items[0].rewrite_expression.?.target_column,
            .expression = alternate_expression,
        },
    };
    const alternate_expression_job = try schemaRewriteJobForAppliedDdlWorkItem(alloc, applied.table, service.ranges[0], alternate_item, 1);
    try std.testing.expect(same_expression_job.job_id != alternate_expression_job.job_id);

    applied.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), service.jobs.items.len);
    const job = service.jobs.items[0];
    try std.testing.expectEqual(@as(u64, 77), job.table_id);
    try std.testing.expectEqual(@as(u64, 9001), job.group_id);
    try std.testing.expect(job.job_id != 0);
    try std.testing.expect(job.schema_generation != 0);
    try std.testing.expectEqualStrings("", job.start_row_key);
    try std.testing.expectEqualStrings("m", job.end_row_key.?);
    try std.testing.expectEqualStrings("rewrite", job.action);
    try std.testing.expectEqualStrings("row_images", job.reason);
    try std.testing.expectEqualStrings("status_key", job.target_column);
    const job_expression = job.expression orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(runtime_schema_mod.RelationalRowsExpressionKind.lower, job_expression.kind);
    try std.testing.expectEqualStrings("status", job_expression.operands[0].field);
    try std.testing.expectEqual(@as(u64, 9002), service.jobs.items[1].group_id);
    try std.testing.expectEqualStrings("m", service.jobs.items[1].start_row_key);
    try std.testing.expect(service.jobs.items[1].end_row_key == null);
}

test "catalog jobs schedules durable schema validation jobs from applied SQL DDL work" {
    const alloc = std.testing.allocator;
    const FakeService = struct {
        ranges: [2]metadata_table_manager.RangeRecord = .{
            .{ .group_id = 9101, .range_id = 9201, .table_id = 88, .start_key = "", .end_key = "n" },
            .{ .group_id = 9102, .range_id = 9202, .table_id = 88, .start_key = "n", .end_key = null },
        },
        jobs: std.ArrayListUnmanaged(metadata_table_manager.SchemaRewriteJobRecord) = .empty,

        fn deinit(self: *@This(), allocator: std.mem.Allocator) void {
            for (self.jobs.items) |record| metadata_table_manager.freeSchemaRewriteJob(allocator, record);
            self.jobs.deinit(allocator);
            self.* = undefined;
        }

        pub fn adminSnapshot(self: *@This()) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = &.{},
                .ranges = self.ranges[0..],
                .stores = &.{},
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }

        fn freeAdminSnapshot(_: *@This(), _: *metadata_api.AdminSnapshot) void {}

        fn upsertSchemaRewriteJob(self: *@This(), record: metadata_table_manager.SchemaRewriteJobRecord) !void {
            const owned = try metadata_table_manager.cloneSchemaRewriteJob(std.testing.allocator, record);
            errdefer metadata_table_manager.freeSchemaRewriteJob(std.testing.allocator, owned);
            try self.jobs.append(std.testing.allocator, owned);
        }
    };

    const table = try metadata_table_manager.cloneTable(alloc, .{
        .table_id = 88,
        .name = "events",
        .schema_json = "{\"version\":3,\"storage_mode\":\"relational\"}",
    });
    errdefer metadata_table_manager.freeTable(alloc, table);
    const work_items = try alloc.alloc(sql_adapter.AppliedDdlWorkItem, 1);
    work_items[0] = .{
        .action = .validate,
        .subject = .table,
        .reason = .constraints,
    };
    var applied: tables_api.AppliedRelationalSqlDdlRecord = .{
        .table = table,
        .validation_required = true,
        .work_items = work_items,
    };
    defer applied.deinit(alloc);

    var service = FakeService{};
    defer service.deinit(alloc);
    try scheduleSchemaRewriteJobsForAppliedDdlOnService(&service, alloc, applied);

    try std.testing.expectEqual(@as(usize, 2), service.jobs.items.len);
    try std.testing.expectEqual(@as(u64, 88), service.jobs.items[0].table_id);
    try std.testing.expectEqual(@as(u64, 9101), service.jobs.items[0].group_id);
    try std.testing.expectEqualStrings("validate", service.jobs.items[0].action);
    try std.testing.expectEqualStrings("constraints", service.jobs.items[0].reason);
    try std.testing.expectEqualStrings("", service.jobs.items[0].target_column);
    try std.testing.expect(service.jobs.items[0].expression == null);
    try std.testing.expect(!service.jobs.items[0].full_row_rewrite);
    try std.testing.expectEqual(@as(usize, 0), service.jobs.items[0].rewrite_renames.len);
    try std.testing.expectEqual(@as(usize, 0), service.jobs.items[0].rewrite_drops.len);
    try std.testing.expectEqual(@as(u64, 9102), service.jobs.items[1].group_id);
}

test "catalog jobs schedules row-plan schema rewrite jobs from applied SQL DDL work" {
    const alloc = std.testing.allocator;
    const FakeService = struct {
        ranges: [1]metadata_table_manager.RangeRecord = .{
            .{ .group_id = 9001, .range_id = 9101, .table_id = 77, .start_key = "", .end_key = null },
        },
        jobs: std.ArrayListUnmanaged(metadata_table_manager.SchemaRewriteJobRecord) = .empty,

        fn deinit(self: *@This(), allocator: std.mem.Allocator) void {
            for (self.jobs.items) |record| metadata_table_manager.freeSchemaRewriteJob(allocator, record);
            self.jobs.deinit(allocator);
            self.* = undefined;
        }

        pub fn adminSnapshot(self: *@This()) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = &.{},
                .ranges = self.ranges[0..],
                .stores = &.{},
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }

        fn freeAdminSnapshot(_: *@This(), _: *metadata_api.AdminSnapshot) void {}

        fn upsertSchemaRewriteJob(self: *@This(), record: metadata_table_manager.SchemaRewriteJobRecord) !void {
            const owned = try metadata_table_manager.cloneSchemaRewriteJob(std.testing.allocator, record);
            errdefer metadata_table_manager.freeSchemaRewriteJob(std.testing.allocator, owned);
            try self.jobs.append(std.testing.allocator, owned);
        }
    };

    const table = try metadata_table_manager.cloneTable(alloc, .{
        .table_id = 77,
        .name = "events",
        .schema_json = "{\"version\":2,\"storage_mode\":\"relational\",\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"state\":{\"type\":\"keyword\"}}}}}}",
    });
    errdefer metadata_table_manager.freeTable(alloc, table);
    const renames = try alloc.alloc(sql_adapter.AppliedDdlRowRewriteRename, 1);
    renames[0] = .{
        .old_path = try alloc.dupe(u8, "status"),
        .new_path = try alloc.dupe(u8, "state"),
    };
    const drops = try alloc.alloc([]const u8, 1);
    drops[0] = try alloc.dupe(u8, "legacy_status");
    const work_items = try alloc.alloc(sql_adapter.AppliedDdlWorkItem, 1);
    work_items[0] = .{
        .action = .rewrite,
        .subject = .table,
        .reason = .row_images,
        .row_rewrite_plan = .{ .renames = renames, .drops = drops },
    };
    var applied: tables_api.AppliedRelationalSqlDdlRecord = .{
        .table = table,
        .rewrite_required = true,
        .work_items = work_items,
    };

    var service = FakeService{};
    defer service.deinit(alloc);
    try scheduleSchemaRewriteJobsForAppliedDdlOnService(&service, alloc, applied);

    const same_row_plan_job = try schemaRewriteJobForAppliedDdlWorkItem(alloc, applied.table, service.ranges[0], applied.work_items[0], 1);
    const alternate_item: sql_adapter.AppliedDdlWorkItem = .{
        .action = .rewrite,
        .subject = .table,
        .reason = .row_images,
        .row_rewrite_plan = .{ .drops = &.{"legacy_status"} },
    };
    const alternate_row_plan_job = try schemaRewriteJobForAppliedDdlWorkItem(alloc, applied.table, service.ranges[0], alternate_item, 1);
    try std.testing.expect(same_row_plan_job.job_id != alternate_row_plan_job.job_id);

    applied.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), service.jobs.items.len);
    const job = service.jobs.items[0];
    try std.testing.expectEqual(@as(usize, 1), job.rewrite_renames.len);
    try std.testing.expectEqualStrings("status", job.rewrite_renames[0].old_path);
    try std.testing.expectEqualStrings("state", job.rewrite_renames[0].new_path);
    try std.testing.expectEqual(@as(usize, 1), job.rewrite_drops.len);
    try std.testing.expectEqualStrings("legacy_status", job.rewrite_drops[0]);
}

test "catalog jobs schedules full row schema rewrite jobs from applied SQL DDL work" {
    const alloc = std.testing.allocator;
    const FakeService = struct {
        ranges: [1]metadata_table_manager.RangeRecord = .{
            .{ .group_id = 9001, .range_id = 9101, .table_id = 77, .start_key = "", .end_key = null },
        },
        jobs: std.ArrayListUnmanaged(metadata_table_manager.SchemaRewriteJobRecord) = .empty,

        fn deinit(self: *@This(), allocator: std.mem.Allocator) void {
            for (self.jobs.items) |record| metadata_table_manager.freeSchemaRewriteJob(allocator, record);
            self.jobs.deinit(allocator);
            self.* = undefined;
        }

        pub fn adminSnapshot(self: *@This()) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = &.{},
                .ranges = self.ranges[0..],
                .stores = &.{},
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }

        fn freeAdminSnapshot(_: *@This(), _: *metadata_api.AdminSnapshot) void {}

        fn upsertSchemaRewriteJob(self: *@This(), record: metadata_table_manager.SchemaRewriteJobRecord) !void {
            const owned = try metadata_table_manager.cloneSchemaRewriteJob(std.testing.allocator, record);
            errdefer metadata_table_manager.freeSchemaRewriteJob(std.testing.allocator, owned);
            try self.jobs.append(std.testing.allocator, owned);
        }
    };

    const table = try metadata_table_manager.cloneTable(alloc, .{
        .table_id = 77,
        .name = "events",
        .schema_json = "{\"version\":2,\"storage_mode\":\"relational\",\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"state\":{\"type\":\"keyword\"}}}}}}",
    });
    errdefer metadata_table_manager.freeTable(alloc, table);
    const work_items = try alloc.alloc(sql_adapter.AppliedDdlWorkItem, 1);
    work_items[0] = .{
        .action = .rewrite,
        .subject = .table,
        .reason = .row_images,
        .full_row_rewrite = true,
    };
    var applied: tables_api.AppliedRelationalSqlDdlRecord = .{
        .table = table,
        .rewrite_required = true,
        .work_items = work_items,
    };

    var service = FakeService{};
    defer service.deinit(alloc);
    try scheduleSchemaRewriteJobsForAppliedDdlOnService(&service, alloc, applied);

    const same_full_job = try schemaRewriteJobForAppliedDdlWorkItem(alloc, applied.table, service.ranges[0], applied.work_items[0], 1);
    const alternate_item: sql_adapter.AppliedDdlWorkItem = .{
        .action = .rewrite,
        .subject = .table,
        .reason = .row_images,
        .row_rewrite_plan = .{ .drops = &.{"legacy_status"} },
    };
    const alternate_row_plan_job = try schemaRewriteJobForAppliedDdlWorkItem(alloc, applied.table, service.ranges[0], alternate_item, 1);
    try std.testing.expect(same_full_job.job_id != alternate_row_plan_job.job_id);

    applied.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), service.jobs.items.len);
    const job = service.jobs.items[0];
    try std.testing.expect(job.full_row_rewrite);
    try std.testing.expectEqual(@as(usize, 0), job.rewrite_renames.len);
    try std.testing.expectEqual(@as(usize, 0), job.rewrite_drops.len);
}

test "catalog jobs rejects schema rewrite jobs without typed row operation" {
    const alloc = std.testing.allocator;
    const FakeService = struct {
        ranges: [1]metadata_table_manager.RangeRecord = .{
            .{ .group_id = 9001, .range_id = 9101, .table_id = 77, .start_key = "", .end_key = null },
        },
        upsert_count: usize = 0,

        pub fn adminSnapshot(self: *@This()) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = &.{},
                .ranges = self.ranges[0..],
                .stores = &.{},
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }

        fn freeAdminSnapshot(_: *@This(), _: *metadata_api.AdminSnapshot) void {}

        fn upsertSchemaRewriteJob(self: *@This(), _: metadata_table_manager.SchemaRewriteJobRecord) !void {
            self.upsert_count += 1;
        }
    };

    const table = try metadata_table_manager.cloneTable(alloc, .{
        .table_id = 77,
        .name = "events",
        .schema_json = "{\"version\":2,\"storage_mode\":\"relational\"}",
    });
    errdefer metadata_table_manager.freeTable(alloc, table);
    const work_items = try alloc.alloc(sql_adapter.AppliedDdlWorkItem, 1);
    work_items[0] = .{
        .action = .rewrite,
        .subject = .table,
        .reason = .row_images,
    };
    var applied: tables_api.AppliedRelationalSqlDdlRecord = .{
        .table = table,
        .rewrite_required = true,
        .work_items = work_items,
    };
    defer applied.deinit(alloc);

    var service = FakeService{};
    try std.testing.expectError(error.UnsupportedSqlShape, scheduleSchemaRewriteJobsForAppliedDdlOnService(&service, alloc, applied));
    try std.testing.expectEqual(@as(usize, 0), service.upsert_count);
}

test "catalog jobs detects schema rewrite wakeable applied DDL work" {
    const alloc = std.testing.allocator;
    const table = try metadata_table_manager.cloneTable(alloc, .{
        .table_id = 77,
        .name = "events",
        .schema_json = "{\"version\":2,\"storage_mode\":\"relational\"}",
    });
    errdefer metadata_table_manager.freeTable(alloc, table);
    const work_items = try alloc.alloc(sql_adapter.AppliedDdlWorkItem, 1);
    work_items[0] = .{
        .action = .validate,
        .subject = .table,
        .reason = .constraints,
    };
    var applied: tables_api.AppliedRelationalSqlDdlRecord = .{
        .table = table,
        .validation_required = true,
        .work_items = work_items,
    };
    defer applied.deinit(alloc);

    try std.testing.expect(appliedDdlHasSchemaRewriteWork(applied));
    applied.dropped_table = true;
    try std.testing.expect(!appliedDdlHasSchemaRewriteWork(applied));
}

test "catalog jobs builds deterministic table emptying jobs for ranges" {
    const table = metadata_table_manager.TableRecord{
        .table_id = 77,
        .name = "events",
        .schema_json = "{\"version\":2,\"storage_mode\":\"relational\"}",
    };
    const range = metadata_table_manager.RangeRecord{
        .group_id = 9001,
        .range_id = 9101,
        .table_id = 77,
        .start_key = "",
        .end_key = null,
    };
    const affected = [_]u64{ 77, 88 };
    const first = tableEmptyingJobForRange(table, range, affected[0..], true, true);
    const second = tableEmptyingJobForRange(table, range, affected[0..], true, true);
    try std.testing.expect(first.job_id != 0);
    try std.testing.expectEqual(first.job_id, second.job_id);
    try std.testing.expectEqual(@as(u64, 77), first.table_id);
    try std.testing.expectEqual(@as(u64, 9001), first.group_id);
    try std.testing.expectEqual(metadata_table_manager.schemaRewriteGenerationForSchemaJson(table.schema_json), first.schema_generation);
    try std.testing.expect(first.restart_identity);
    try std.testing.expect(first.cascade);

    var next_range = range;
    next_range.group_id = 9002;
    const different_group = tableEmptyingJobForRange(table, next_range, affected[0..], true, true);
    try std.testing.expect(first.job_id != different_group.job_id);

    const different_flags = tableEmptyingJobForRange(table, range, affected[0..], false, true);
    try std.testing.expect(first.job_id != different_flags.job_id);
}

test "catalog jobs schedules table emptying jobs from snapshot ranges" {
    const Scheduler = struct {
        jobs: std.ArrayListUnmanaged(metadata_table_manager.TableEmptyingJobRecord) = .empty,

        fn deinit(self: *@This(), allocator: std.mem.Allocator) void {
            for (self.jobs.items) |record| metadata_table_manager.freeTableEmptyingJob(allocator, record);
            self.jobs.deinit(allocator);
        }

        fn upsertTableEmptyingJob(self: *@This(), record: metadata_table_manager.TableEmptyingJobRecord) !void {
            const owned = try metadata_table_manager.cloneTableEmptyingJob(std.testing.allocator, record);
            errdefer metadata_table_manager.freeTableEmptyingJob(std.testing.allocator, owned);
            try self.jobs.append(std.testing.allocator, owned);
        }
    };
    const table = metadata_table_manager.TableRecord{
        .table_id = 91,
        .name = "events",
        .schema_json = "{\"version\":2,\"storage_mode\":\"relational\"}",
    };
    const ranges = [_]metadata_table_manager.RangeRecord{
        .{ .group_id = 8101, .range_id = 8102, .table_id = 91, .start_key = "", .end_key = "m" },
        .{ .group_id = 8103, .range_id = 8104, .table_id = 91, .start_key = "m", .end_key = null },
        .{ .group_id = 9999, .range_id = 9998, .table_id = 92, .start_key = "", .end_key = null },
    };
    const affected = [_]u64{ 91, 92 };
    var scheduler = Scheduler{};
    defer scheduler.deinit(std.testing.allocator);

    const scheduled = try scheduleTableEmptyingJobsForTableSnapshot(&scheduler, ranges[0..], table, affected[0..], true, true);

    try std.testing.expectEqual(@as(usize, 2), scheduled);
    try std.testing.expectEqual(@as(usize, 2), scheduler.jobs.items.len);
    try std.testing.expectEqual(@as(u64, 8101), scheduler.jobs.items[0].group_id);
    try std.testing.expectEqual(@as(u64, 8103), scheduler.jobs.items[1].group_id);
    for (scheduler.jobs.items) |job| {
        try std.testing.expectEqual(@as(u64, 91), job.table_id);
        try std.testing.expect(job.restart_identity);
        try std.testing.expect(job.cascade);
        try std.testing.expectEqual(@as(usize, 2), job.affected_table_ids.len);
    }
}

test "catalog jobs table emptying barrier waits for every affected table range" {
    const alloc = std.testing.allocator;
    const tables = [_]metadata_table_manager.TableRecord{
        .{ .table_id = 91, .name = "events", .schema_json = "{\"version\":2,\"storage_mode\":\"relational\"}" },
        .{ .table_id = 92, .name = "events_archive", .schema_json = "{\"version\":2,\"storage_mode\":\"relational\",\"name\":\"archive\"}" },
    };
    const ranges = [_]metadata_table_manager.RangeRecord{
        .{ .group_id = 8101, .range_id = 8102, .table_id = 91, .start_key = "", .end_key = null },
        .{ .group_id = 8201, .range_id = 8202, .table_id = 92, .start_key = "", .end_key = null },
    };
    const affected = [_]u64{ 91, 92 };
    var ready = tableEmptyingJobForRange(tables[0], ranges[0], affected[0..], true, true);
    ready.state = metadata_table_manager.table_emptying_ready;
    const jobs = [_]metadata_table_manager.TableEmptyingJobRecord{ready};
    const snapshot = metadata_api.AdminSnapshot{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = tables[0..],
        .ranges = ranges[0..],
        .stores = &.{},
        .placement_intents = &.{},
        .table_emptying_jobs = jobs[0..],
    };

    const completed = try completedTableEmptyingBarrierJobIdsForTableAlloc(alloc, &snapshot, 91);
    defer alloc.free(completed);
    try std.testing.expectEqual(@as(usize, 0), completed.len);
}

test "catalog jobs table emptying barrier rejects invalid affected range job" {
    const alloc = std.testing.allocator;
    const tables = [_]metadata_table_manager.TableRecord{
        .{ .table_id = 91, .name = "events", .schema_json = "{\"version\":2,\"storage_mode\":\"relational\"}" },
        .{ .table_id = 92, .name = "events_archive", .schema_json = "{\"version\":2,\"storage_mode\":\"relational\",\"name\":\"archive\"}" },
    };
    const ranges = [_]metadata_table_manager.RangeRecord{
        .{ .group_id = 8101, .range_id = 8102, .table_id = 91, .start_key = "", .end_key = null },
        .{ .group_id = 8201, .range_id = 8202, .table_id = 92, .start_key = "", .end_key = null },
    };
    const affected = [_]u64{ 91, 92 };
    var first = tableEmptyingJobForRange(tables[0], ranges[0], affected[0..], true, true);
    first.state = metadata_table_manager.table_emptying_ready;
    var second = tableEmptyingJobForRange(tables[1], ranges[1], affected[0..], true, true);
    second.state = metadata_table_manager.table_emptying_invalid;
    const jobs = [_]metadata_table_manager.TableEmptyingJobRecord{ first, second };
    const snapshot = metadata_api.AdminSnapshot{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = tables[0..],
        .ranges = ranges[0..],
        .stores = &.{},
        .placement_intents = &.{},
        .table_emptying_jobs = jobs[0..],
    };

    try std.testing.expectError(error.TableEmptyingJobInvalid, completedTableEmptyingBarrierJobIdsForTableAlloc(alloc, &snapshot, 91));
}

test "catalog jobs promotes completed table emptying barrier by removing durable jobs" {
    const alloc = std.testing.allocator;
    const FakeService = struct {
        tables: [2]metadata_table_manager.TableRecord = .{
            .{ .table_id = 91, .name = "events", .schema_json = "{\"version\":2,\"storage_mode\":\"relational\"}" },
            .{ .table_id = 92, .name = "events_archive", .schema_json = "{\"version\":2,\"storage_mode\":\"relational\",\"name\":\"archive\"}" },
        },
        ranges: [2]metadata_table_manager.RangeRecord = .{
            .{ .group_id = 8101, .range_id = 8102, .table_id = 91, .start_key = "", .end_key = null },
            .{ .group_id = 8201, .range_id = 8202, .table_id = 92, .start_key = "", .end_key = null },
        },
        affected: [2]u64 = .{ 91, 92 },
        jobs: [2]metadata_table_manager.TableEmptyingJobRecord = undefined,
        removed: std.ArrayListUnmanaged(u64) = .empty,

        fn seed(self: *@This()) void {
            self.jobs[0] = tableEmptyingJobForRange(self.tables[0], self.ranges[0], self.affected[0..], true, true);
            self.jobs[0].state = metadata_table_manager.table_emptying_ready;
            self.jobs[1] = tableEmptyingJobForRange(self.tables[1], self.ranges[1], self.affected[0..], true, true);
            self.jobs[1].state = metadata_table_manager.table_emptying_ready;
        }

        fn deinit(self: *@This(), allocator: std.mem.Allocator) void {
            self.removed.deinit(allocator);
        }

        pub fn adminSnapshot(self: *@This()) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = self.tables[0..],
                .ranges = self.ranges[0..],
                .stores = &.{},
                .placement_intents = &.{},
                .table_emptying_jobs = self.jobs[0..],
            };
        }

        pub fn freeAdminSnapshot(_: *@This(), _: *metadata_api.AdminSnapshot) void {}

        pub fn removeTableEmptyingJob(self: *@This(), job_id: u64) !void {
            try self.removed.append(std.testing.allocator, job_id);
        }
    };

    var service = FakeService{};
    service.seed();
    defer service.deinit(alloc);

    const promoted = try promoteCompletedTableEmptyingBarriersForTableOnServiceAlloc(alloc, &service, "events");

    try std.testing.expectEqual(@as(usize, 2), promoted);
    try std.testing.expectEqual(@as(usize, 2), service.removed.items.len);
    try std.testing.expectEqual(service.jobs[0].job_id, service.removed.items[0]);
    try std.testing.expectEqual(service.jobs[1].job_id, service.removed.items[1]);
}

test "catalog jobs snapshot scheduler does not require HTTP service surface" {
    const alloc = std.testing.allocator;
    const Scheduler = struct {
        jobs: std.ArrayListUnmanaged(metadata_table_manager.SchemaRewriteJobRecord) = .empty,

        fn deinit(self: *@This(), allocator: std.mem.Allocator) void {
            for (self.jobs.items) |record| metadata_table_manager.freeSchemaRewriteJob(allocator, record);
            self.jobs.deinit(allocator);
        }

        fn upsertSchemaRewriteJob(self: *@This(), record: metadata_table_manager.SchemaRewriteJobRecord) !void {
            const owned = try metadata_table_manager.cloneSchemaRewriteJob(std.testing.allocator, record);
            errdefer metadata_table_manager.freeSchemaRewriteJob(std.testing.allocator, owned);
            try self.jobs.append(std.testing.allocator, owned);
        }
    };
    const table = try metadata_table_manager.cloneTable(alloc, .{
        .table_id = 91,
        .name = "events",
        .database_name = catalog_resources.default_database_name,
        .namespace_name = catalog_resources.default_namespace_name,
        .schema_json = "{\"version\":2,\"storage_mode\":\"relational\"}",
    });
    errdefer metadata_table_manager.freeTable(alloc, table);
    const work_items = try alloc.alloc(sql_adapter.AppliedDdlWorkItem, 1);
    work_items[0] = .{
        .action = .validate,
        .subject = .table,
        .reason = .constraints,
    };
    var applied: tables_api.AppliedRelationalSqlDdlRecord = .{
        .table = table,
        .validation_required = true,
        .work_items = work_items,
    };
    defer applied.deinit(alloc);
    const ranges = [_]metadata_table_manager.RangeRecord{
        .{ .group_id = 8101, .range_id = 8102, .table_id = 91, .start_key = "", .end_key = null },
    };
    var scheduler = Scheduler{};
    defer scheduler.deinit(alloc);

    try scheduleSchemaRewriteJobsForAppliedDdlSnapshot(&scheduler, alloc, ranges[0..], applied);

    try std.testing.expectEqual(@as(usize, 1), scheduler.jobs.items.len);
    try std.testing.expectEqual(@as(u64, 8101), scheduler.jobs.items[0].group_id);
}
