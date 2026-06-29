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

const catalog_resources = @import("resources.zig");
const table_catalog = @import("source.zig");
const metadata_api = @import("snapshot.zig");
const metadata_table_manager = @import("../table_manager.zig");
const runtime_schema_mod = @import("../../storage/schema.zig");
const sql_adapter = @import("../../sql/mod.zig");
const table_ddl = @import("table_ddl.zig");

// Catalog job planning owns deterministic durable job records. Execution stays
// with the table/range write path that can claim and drain range-local work.
pub fn scheduleSchemaRewriteJobsForAppliedDdlOnService(
    svc: anytype,
    alloc: std.mem.Allocator,
    applied: table_ddl.AppliedRelationalSqlDdlRecord,
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
    applied: table_ddl.AppliedRelationalSqlDdlRecord,
) !void {
    const jobs = try schemaRewriteJobsForAppliedDdlSnapshotAlloc(alloc, ranges, applied);
    defer {
        for (jobs) |record| metadata_table_manager.freeSchemaRewriteJob(alloc, record);
        alloc.free(jobs);
    }
    for (jobs) |job| try scheduler.upsertSchemaRewriteJob(job);
}

pub fn schemaRewriteJobsForAppliedDdlSnapshotAlloc(
    alloc: std.mem.Allocator,
    ranges: []const metadata_table_manager.RangeRecord,
    applied: table_ddl.AppliedRelationalSqlDdlRecord,
) ![]metadata_table_manager.SchemaRewriteJobRecord {
    var jobs = std.ArrayListUnmanaged(metadata_table_manager.SchemaRewriteJobRecord).empty;
    errdefer {
        for (jobs.items) |record| metadata_table_manager.freeSchemaRewriteJob(alloc, record);
        jobs.deinit(alloc);
    }

    if (applied.dropped_table or applied.work_items.len == 0) return try alloc.alloc(metadata_table_manager.SchemaRewriteJobRecord, 0);
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
            errdefer metadata_table_manager.freeSchemaRewriteJob(alloc, job);
            try jobs.append(alloc, job);
        }
    }
    return try jobs.toOwnedSlice(alloc);
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
    hasher.update(std.mem.asBytes(&range.range_id));
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
    return try metadata_table_manager.cloneSchemaRewriteJob(alloc, .{
        .job_id = job_id,
        .table_id = table.table_id,
        .group_id = range.group_id,
        .range_id = range.range_id,
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
    });
}

pub fn appliedDdlHasSchemaRewriteWork(applied: table_ddl.AppliedRelationalSqlDdlRecord) bool {
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

pub fn promoteCompletedSchemaRewriteForTableIdAlloc(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    table_id: u64,
) !bool {
    var snapshot = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&snapshot);
    const table = findTableById(&snapshot, table_id) orelse return error.TableNotFound;
    if (table.read_schema_json.len == 0) return false;
    try validateSchemaRewriteGenerationReadyForPromotion(&snapshot, table);

    var promoted = try metadata_table_manager.cloneTable(alloc, table);
    defer metadata_table_manager.freeTable(alloc, promoted);
    alloc.free(@constCast(promoted.read_schema_json));
    promoted.read_schema_json = try alloc.dupe(u8, "");

    try catalog.compareAndSwapTableSchema(.{
        .table_id = table.table_id,
        .expected_schema_json = table.schema_json,
        .promoted_table = promoted,
    });
    return true;
}

fn validateSchemaRewriteGenerationReadyForPromotion(
    snapshot: *const metadata_api.AdminSnapshot,
    table: metadata_table_manager.TableRecord,
) !void {
    const schema_generation = metadata_table_manager.schemaRewriteGenerationForSchemaJson(table.schema_json);
    for (snapshot.schema_rewrite_jobs) |record| {
        if (record.table_id != table.table_id) continue;
        if (record.schema_generation != schema_generation) continue;
        if (!metadata_table_manager.schemaRewriteJobComplete(record)) return error.SchemaRewriteJobsIncomplete;
    }
}

pub fn promoteCompletedSchemaRewriteForTableNameAlloc(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
) !bool {
    var snapshot = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&snapshot);
    const table = table_ddl.findTableByName(&snapshot, table_name) orelse return error.TableNotFound;
    return try promoteCompletedSchemaRewriteForTableIdAlloc(alloc, catalog, table.table_id);
}

fn hashTableEmptyingBarrierU64(hasher: *std.hash.Wyhash, value: u64) void {
    var raw: [8]u8 = undefined;
    std.mem.writeInt(u64, &raw, value, .little);
    hasher.update(&raw);
}

fn hashTableEmptyingBarrierBool(hasher: *std.hash.Wyhash, value: bool) void {
    hasher.update(if (value) "\x01" else "\x00");
}

fn validateTableEmptyingAffectedTableIdsForTable(
    table: metadata_table_manager.TableRecord,
    affected_table_ids: []const u64,
) !void {
    if (!metadata_table_manager.tableEmptyingAffectedTableIdsCanonicalValid(table.table_id, affected_table_ids)) {
        return error.InvalidTableEmptyingJob;
    }
}

pub fn tableEmptyingBarrierIdForSnapshot(
    snapshot: *const metadata_api.AdminSnapshot,
    affected_table_ids: []const u64,
    restart_identity: bool,
    cascade: bool,
) !u64 {
    if (!metadata_table_manager.tableEmptyingAffectedTableIdsCanonicalSetValid(affected_table_ids)) return error.InvalidTableEmptyingJob;
    var hasher = std.hash.Wyhash.init(0x5445_4241_5252_2026);
    hashTableEmptyingBarrierU64(&hasher, @intCast(affected_table_ids.len));
    for (affected_table_ids) |affected_table_id| {
        const table = findTableById(snapshot, affected_table_id) orelse return error.TableNotFound;
        hashTableEmptyingBarrierU64(&hasher, table.table_id);
        hashTableEmptyingBarrierU64(&hasher, metadata_table_manager.schemaRewriteGenerationForSchemaJson(table.schema_json));
        hashTableEmptyingBarrierU64(&hasher, table.data_generation);
    }
    hashTableEmptyingBarrierBool(&hasher, restart_identity);
    hashTableEmptyingBarrierBool(&hasher, cascade);
    return nonZeroId(hasher.final());
}

pub fn tableEmptyingJobForRange(
    table: metadata_table_manager.TableRecord,
    range: metadata_table_manager.RangeRecord,
    affected_table_ids: []const u64,
    restart_identity: bool,
    cascade: bool,
) metadata_table_manager.TableEmptyingJobRecord {
    const schema_generation = metadata_table_manager.schemaRewriteGenerationForSchemaJson(table.schema_json);
    var hasher = std.hash.Wyhash.init(0x5445_4241_5252_5349);
    hashTableEmptyingBarrierU64(&hasher, @intCast(affected_table_ids.len));
    for (affected_table_ids) |affected_table_id| hashTableEmptyingBarrierU64(&hasher, affected_table_id);
    hashTableEmptyingBarrierU64(&hasher, table.table_id);
    hashTableEmptyingBarrierU64(&hasher, schema_generation);
    hashTableEmptyingBarrierU64(&hasher, table.data_generation);
    hashTableEmptyingBarrierBool(&hasher, restart_identity);
    hashTableEmptyingBarrierBool(&hasher, cascade);
    return tableEmptyingJobForRangeWithBarrierId(table, range, affected_table_ids, restart_identity, cascade, nonZeroId(hasher.final()));
}

pub fn tableEmptyingJobForRangeWithBarrierId(
    table: metadata_table_manager.TableRecord,
    range: metadata_table_manager.RangeRecord,
    affected_table_ids: []const u64,
    restart_identity: bool,
    cascade: bool,
    barrier_id: u64,
) metadata_table_manager.TableEmptyingJobRecord {
    const schema_generation = metadata_table_manager.schemaRewriteGenerationForSchemaJson(table.schema_json);
    var record = metadata_table_manager.TableEmptyingJobRecord{
        .job_id = 0,
        .table_id = table.table_id,
        .group_id = range.group_id,
        .range_id = range.range_id,
        .schema_generation = schema_generation,
        .data_generation = table.data_generation,
        .barrier_id = barrier_id,
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
    try validateTableEmptyingAffectedTableIdsForTable(table, affected_table_ids);
    const barrier_id = try tableEmptyingBarrierIdForSnapshot(&snapshot, affected_table_ids, restart_identity, cascade);
    return try scheduleTableEmptyingJobsForTableSnapshotWithBarrierId(svc, snapshot.ranges, table, affected_table_ids, restart_identity, cascade, barrier_id);
}

pub fn scheduleTableEmptyingJobsForTableSnapshot(
    scheduler: anytype,
    ranges: []const metadata_table_manager.RangeRecord,
    table: metadata_table_manager.TableRecord,
    affected_table_ids: []const u64,
    restart_identity: bool,
    cascade: bool,
) !usize {
    try validateTableEmptyingAffectedTableIdsForTable(table, affected_table_ids);
    var scheduled: usize = 0;
    for (ranges) |range| {
        if (range.table_id != table.table_id) continue;
        const job = tableEmptyingJobForRange(table, range, affected_table_ids, restart_identity, cascade);
        try scheduler.upsertTableEmptyingJob(job);
        scheduled += 1;
    }
    return scheduled;
}

pub fn scheduleTableEmptyingJobsForTableSnapshotWithBarrierId(
    scheduler: anytype,
    ranges: []const metadata_table_manager.RangeRecord,
    table: metadata_table_manager.TableRecord,
    affected_table_ids: []const u64,
    restart_identity: bool,
    cascade: bool,
    barrier_id: u64,
) !usize {
    if (barrier_id == 0) return error.InvalidTableEmptyingJob;
    try validateTableEmptyingAffectedTableIdsForTable(table, affected_table_ids);
    var scheduled: usize = 0;
    for (ranges) |range| {
        if (range.table_id != table.table_id) continue;
        const job = tableEmptyingJobForRangeWithBarrierId(table, range, affected_table_ids, restart_identity, cascade, barrier_id);
        try scheduler.upsertTableEmptyingJob(job);
        scheduled += 1;
    }
    return scheduled;
}

pub const TableEmptyingBarrierAdmission = struct {
    applied: table_ddl.AppliedRelationalSqlDdlRecord,
    affected_tables: []metadata_table_manager.TableRecord = &.{},
    scheduled_jobs: usize = 0,

    pub fn deinitAffectedTables(self: *@This(), alloc: std.mem.Allocator) void {
        for (self.affected_tables) |record| metadata_table_manager.freeTable(alloc, record);
        alloc.free(self.affected_tables);
        self.affected_tables = &.{};
    }
};

const TableEmptyingDuplicatePolicy = enum {
    reject,
    ignore,
};

fn serviceSupportsTableEmptyingIdentityAllocatorReset(service: anytype) bool {
    const ServiceType = @TypeOf(service);
    const ServiceDeclType = switch (@typeInfo(ServiceType)) {
        .pointer => |pointer| pointer.child,
        else => ServiceType,
    };
    if (comptime @hasDecl(ServiceDeclType, "supportsIdentityAllocatorResetForTableEmptyingBarrier")) {
        return service.supportsIdentityAllocatorResetForTableEmptyingBarrier();
    }
    return comptime @hasDecl(ServiceDeclType, "resetIdentityAllocatorsForTableEmptyingBarrier");
}

fn appendTableEmptyingAffectedRecord(
    alloc: std.mem.Allocator,
    table: *const metadata_table_manager.TableRecord,
    affected_table_ids: *std.ArrayListUnmanaged(u64),
    affected_tables: *std.ArrayListUnmanaged(*const metadata_table_manager.TableRecord),
    duplicate_policy: TableEmptyingDuplicatePolicy,
) !bool {
    for (affected_table_ids.items) |table_id| {
        if (table_id == table.table_id) {
            return switch (duplicate_policy) {
                .reject => error.InvalidArgument,
                .ignore => false,
            };
        }
    }
    try affected_table_ids.append(alloc, table.table_id);
    errdefer _ = affected_table_ids.pop();
    try affected_tables.append(alloc, table);
    return true;
}

fn cloneSortedTableEmptyingAffectedTableIdsAlloc(
    alloc: std.mem.Allocator,
    affected_table_ids: []const u64,
) ![]u64 {
    if (affected_table_ids.len == 0) return error.InvalidArgument;
    const out = try alloc.dupe(u64, affected_table_ids);
    errdefer alloc.free(out);
    std.mem.sort(u64, out, {}, comptime std.sort.asc(u64));
    for (out, 0..) |table_id, i| {
        if (table_id == 0) return error.InvalidArgument;
        if (i > 0 and out[i - 1] == table_id) return error.InvalidArgument;
    }
    return out;
}

fn appendTableEmptyingTargetWithSession(
    alloc: std.mem.Allocator,
    snapshot: *const metadata_api.AdminSnapshot,
    table_name: []const u8,
    session: catalog_resources.SqlCatalogSession,
    affected_table_ids: *std.ArrayListUnmanaged(u64),
    affected_tables: *std.ArrayListUnmanaged(*const metadata_table_manager.TableRecord),
    duplicate_policy: TableEmptyingDuplicatePolicy,
) !bool {
    const target = try session.tableTargetFromObjectName(table_name);
    const table = table_ddl.findTableByQualifiedName(snapshot, target.database_name, target.namespace_name, target.table_name) orelse return error.TableNotFound;
    return try appendTableEmptyingAffectedRecord(alloc, table, affected_table_ids, affected_tables, duplicate_policy);
}

fn tableEmptyingForeignKeyReferencesTable(
    snapshot: *const metadata_api.AdminSnapshot,
    child_table: metadata_table_manager.TableRecord,
    child_schema: runtime_schema_mod.TableSchema,
    foreign_key: runtime_schema_mod.ForeignKey,
    parent_table: metadata_table_manager.TableRecord,
) !bool {
    const parent_table_name = if (std.mem.eql(u8, foreign_key.parent_table, child_schema.default_type))
        child_table.name
    else
        foreign_key.parent_table;
    const child_search_path = [_][]const u8{child_table.namespace_name};
    const child_session = catalog_resources.SqlCatalogSession{
        .current_database_name = child_table.database_name,
        .search_path = child_search_path[0..],
    };
    const parent_target = try child_session.tableTargetFromObjectName(parent_table_name);
    const resolved_parent = table_ddl.findTableByQualifiedName(snapshot, parent_target.database_name, parent_target.namespace_name, parent_target.table_name) orelse return error.InvalidSqlCatalog;
    return resolved_parent.table_id == parent_table.table_id;
}

fn appendTableEmptyingCascadeTargets(
    alloc: std.mem.Allocator,
    snapshot: *const metadata_api.AdminSnapshot,
    affected_table_ids: *std.ArrayListUnmanaged(u64),
    affected_tables: *std.ArrayListUnmanaged(*const metadata_table_manager.TableRecord),
) !void {
    var cursor: usize = 0;
    while (cursor < affected_tables.items.len) : (cursor += 1) {
        const parent_table = affected_tables.items[cursor].*;
        for (snapshot.tables) |*candidate_table| {
            if (candidate_table.schema_json.len == 0) continue;
            var parsed_child = try table_ddl.parseValidatedTableSchema(alloc, candidate_table.schema_json);
            defer parsed_child.deinit(alloc);
            const child_schema = try table_ddl.deriveRuntimeTableSchema(alloc, parsed_child);
            defer runtime_schema_mod.freeSchema(alloc, child_schema);
            if (child_schema.storage_mode != .relational) continue;

            for (child_schema.foreign_keys) |foreign_key| {
                if (!(try tableEmptyingForeignKeyReferencesTable(snapshot, candidate_table.*, child_schema, foreign_key, parent_table))) continue;
                _ = try appendTableEmptyingAffectedRecord(
                    alloc,
                    candidate_table,
                    affected_table_ids,
                    affected_tables,
                    .ignore,
                );
                break;
            }
        }
    }
}

fn cloneTableEmptyingAffectedTablesAlloc(
    alloc: std.mem.Allocator,
    affected_tables: []const *const metadata_table_manager.TableRecord,
) ![]metadata_table_manager.TableRecord {
    const out = try alloc.alloc(metadata_table_manager.TableRecord, affected_tables.len);
    var cloned: usize = 0;
    errdefer {
        for (out[0..cloned]) |record| metadata_table_manager.freeTable(alloc, record);
        alloc.free(out);
    }
    for (affected_tables, 0..) |table, i| {
        out[i] = try metadata_table_manager.cloneTable(alloc, table.*);
        cloned += 1;
    }
    return out;
}

pub fn scheduleTableEmptyingBarrierForTargetsOnServiceWithSessionAlloc(
    alloc: std.mem.Allocator,
    svc: anytype,
    primary_table_name: []const u8,
    additional_table_names: []const []const u8,
    restart_identity: bool,
    cascade: bool,
    session: catalog_resources.SqlCatalogSession,
) !TableEmptyingBarrierAdmission {
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
    if (restart_identity) {
        if (!serviceSupportsTableEmptyingIdentityAllocatorReset(svc)) {
            return error.UnsupportedOperation;
        }
    }

    var snapshot = try svc.adminSnapshot();
    defer svc.freeAdminSnapshot(&snapshot);

    var affected_table_ids = std.ArrayListUnmanaged(u64).empty;
    defer affected_table_ids.deinit(alloc);
    var affected_tables = std.ArrayListUnmanaged(*const metadata_table_manager.TableRecord).empty;
    defer affected_tables.deinit(alloc);

    _ = try appendTableEmptyingTargetWithSession(alloc, &snapshot, primary_table_name, session, &affected_table_ids, &affected_tables, .reject);
    for (additional_table_names) |additional_table_name| {
        _ = try appendTableEmptyingTargetWithSession(alloc, &snapshot, additional_table_name, session, &affected_table_ids, &affected_tables, .reject);
    }
    if (cascade) {
        try appendTableEmptyingCascadeTargets(alloc, &snapshot, &affected_table_ids, &affected_tables);
    }

    const canonical_affected_table_ids = try cloneSortedTableEmptyingAffectedTableIdsAlloc(alloc, affected_table_ids.items);
    defer alloc.free(canonical_affected_table_ids);

    const barrier_id = try tableEmptyingBarrierIdForSnapshot(&snapshot, canonical_affected_table_ids, restart_identity, cascade);
    var scheduled_jobs: usize = 0;
    for (affected_tables.items) |table| {
        scheduled_jobs += try scheduleTableEmptyingJobsForTableSnapshotWithBarrierId(
            svc,
            snapshot.ranges,
            table.*,
            canonical_affected_table_ids,
            restart_identity,
            cascade,
            barrier_id,
        );
    }
    if (scheduled_jobs == 0) return error.UnsupportedOperation;

    const owned_affected_tables = try cloneTableEmptyingAffectedTablesAlloc(alloc, affected_tables.items);
    errdefer {
        for (owned_affected_tables) |record| metadata_table_manager.freeTable(alloc, record);
        alloc.free(owned_affected_tables);
    }

    var applied = try table_ddl.emptyAppliedRelationalSqlDdlRecordAlloc(alloc);
    errdefer applied.deinit(alloc);
    metadata_table_manager.freeTable(alloc, applied.table);
    applied.table = try metadata_table_manager.cloneTable(alloc, affected_tables.items[0].*);

    return .{
        .applied = applied,
        .affected_tables = owned_affected_tables,
        .scheduled_jobs = scheduled_jobs,
    };
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

fn optionalStringsEqual(a: ?[]const u8, b: ?[]const u8) bool {
    if (a == null and b == null) return true;
    if (a == null or b == null) return false;
    return std.mem.eql(u8, a.?, b.?);
}

fn tableEmptyingAffectedTablesEqual(a: []const u64, b: []const u64) bool {
    if (a.len != b.len) return false;
    for (a, b) |left, right| {
        if (left != right) return false;
    }
    return true;
}

fn tableEmptyingJobMatchesBarrierRange(
    record: metadata_table_manager.TableEmptyingJobRecord,
    table: metadata_table_manager.TableRecord,
    range: metadata_table_manager.RangeRecord,
    candidate: metadata_table_manager.TableEmptyingJobRecord,
) bool {
    if (candidate.barrier_id == 0 or record.barrier_id != candidate.barrier_id) return false;
    if (record.table_id != range.table_id) return false;
    if (record.group_id != range.group_id) return false;
    if (record.range_id != 0 and record.range_id != range.range_id) return false;
    if (record.schema_generation != metadata_table_manager.schemaRewriteGenerationForSchemaJson(table.schema_json)) return false;
    if (record.data_generation != table.data_generation) return false;
    if (!std.mem.eql(u8, record.start_row_key, range.start_key)) return false;
    if (!optionalStringsEqual(record.end_row_key, range.end_key)) return false;
    if (!tableEmptyingAffectedTablesEqual(record.affected_table_ids, candidate.affected_table_ids)) return false;
    if (record.restart_identity != candidate.restart_identity) return false;
    if (record.cascade != candidate.cascade) return false;
    return true;
}

fn findTableEmptyingBarrierRangeJob(
    snapshot: *const metadata_api.AdminSnapshot,
    table: metadata_table_manager.TableRecord,
    range: metadata_table_manager.RangeRecord,
    candidate: metadata_table_manager.TableEmptyingJobRecord,
) ?metadata_table_manager.TableEmptyingJobRecord {
    for (snapshot.table_emptying_jobs) |record| {
        if (tableEmptyingJobMatchesBarrierRange(record, table, range, candidate)) return record;
    }
    return null;
}

fn tableEmptyingRepairCandidateActive(candidate: metadata_table_manager.TableEmptyingJobRecord) bool {
    if (std.mem.eql(u8, candidate.state, metadata_table_manager.table_emptying_invalid)) {
        return std.mem.eql(u8, candidate.last_error, @errorName(error.TopologyChanged));
    }
    return std.mem.eql(u8, candidate.state, metadata_table_manager.table_emptying_declared) or
        std.mem.eql(u8, candidate.state, metadata_table_manager.table_emptying_running) or
        std.mem.eql(u8, candidate.state, metadata_table_manager.table_emptying_ready);
}

fn repairMissingTableEmptyingBarrierRangeJobsForCandidate(
    service: anytype,
    snapshot: *const metadata_api.AdminSnapshot,
    candidate: metadata_table_manager.TableEmptyingJobRecord,
) !usize {
    if (!tableEmptyingRepairCandidateActive(candidate)) return 0;
    if (!metadata_table_manager.tableEmptyingAffectedTableIdsCanonicalValid(candidate.table_id, candidate.affected_table_ids)) {
        return error.InvalidTableEmptyingJob;
    }
    if (candidate.barrier_id == 0) return error.InvalidTableEmptyingJob;

    const current_barrier_id = tableEmptyingBarrierIdForSnapshot(
        snapshot,
        candidate.affected_table_ids,
        candidate.restart_identity,
        candidate.cascade,
    ) catch return 0;
    if (current_barrier_id != candidate.barrier_id) return 0;

    var repaired: usize = 0;
    for (candidate.affected_table_ids) |affected_table_id| {
        const table = findTableById(snapshot, affected_table_id) orelse return error.TableNotFound;
        var range_count: usize = 0;
        for (snapshot.ranges) |range| {
            if (range.table_id != affected_table_id) continue;
            range_count += 1;
            const expected = tableEmptyingJobForRangeWithBarrierId(
                table,
                range,
                candidate.affected_table_ids,
                candidate.restart_identity,
                candidate.cascade,
                candidate.barrier_id,
            );
            if (findTableEmptyingBarrierRangeJob(snapshot, table, range, expected) != null) continue;
            try service.upsertTableEmptyingJob(expected);
            repaired += 1;
        }
        if (range_count == 0) return error.InvalidTableEmptyingJob;
    }
    return repaired;
}

pub fn repairMissingTableEmptyingBarrierJobsForTableIdOnServiceAlloc(
    alloc: std.mem.Allocator,
    service: anytype,
    table_id: u64,
) !usize {
    const ServiceType = @TypeOf(service);
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

    var snapshot = try service.adminSnapshot();
    defer service.freeAdminSnapshot(&snapshot);
    if (findTableById(&snapshot, table_id) == null) return error.TableNotFound;

    var repaired_barriers = std.ArrayListUnmanaged(u64).empty;
    defer repaired_barriers.deinit(alloc);

    var repaired: usize = 0;
    for (snapshot.table_emptying_jobs) |candidate| {
        if (!tableEmptyingJobInvolvesTable(candidate, table_id)) continue;
        if (candidate.barrier_id == 0) return error.InvalidTableEmptyingJob;
        if (tableEmptyingBarrierAlreadyPromoted(repaired_barriers.items, candidate.barrier_id)) continue;
        repaired += try repairMissingTableEmptyingBarrierRangeJobsForCandidate(service, &snapshot, candidate);
        try repaired_barriers.append(alloc, candidate.barrier_id);
    }
    return repaired;
}

pub fn repairMissingTableEmptyingBarrierJobsForTableIdAlloc(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    table_id: u64,
) !usize {
    return try repairMissingTableEmptyingBarrierJobsForTableIdOnServiceAlloc(alloc, catalog, table_id);
}

fn completedTableEmptyingBarrierJobIdsForCandidateAlloc(
    alloc: std.mem.Allocator,
    snapshot: *const metadata_api.AdminSnapshot,
    candidate: metadata_table_manager.TableEmptyingJobRecord,
) !?[]u64 {
    if (!metadata_table_manager.tableEmptyingJobComplete(candidate)) return null;
    if (!metadata_table_manager.tableEmptyingAffectedTableIdsCanonicalValid(candidate.table_id, candidate.affected_table_ids)) {
        return error.InvalidTableEmptyingJob;
    }
    if (candidate.barrier_id == 0) return error.InvalidTableEmptyingJob;

    var completed_job_ids = std.ArrayListUnmanaged(u64).empty;
    defer completed_job_ids.deinit(alloc);

    for (candidate.affected_table_ids) |affected_table_id| {
        const table = findTableById(snapshot, affected_table_id) orelse return error.TableNotFound;
        var range_count: usize = 0;
        for (snapshot.ranges) |range| {
            if (range.table_id != affected_table_id) continue;
            range_count += 1;
            const record = findTableEmptyingBarrierRangeJob(snapshot, table, range, candidate) orelse return null;
            if (std.mem.eql(u8, record.state, metadata_table_manager.table_emptying_invalid)) return error.TableEmptyingJobInvalid;
            if (!metadata_table_manager.tableEmptyingJobComplete(record)) return null;
            try appendUniqueTableEmptyingJobId(alloc, &completed_job_ids, record.job_id);
        }
        if (range_count == 0) return error.InvalidTableEmptyingJob;
    }
    for (snapshot.table_emptying_jobs) |record| {
        if (!metadata_table_manager.tableEmptyingJobComplete(record)) continue;
        if (record.barrier_id != candidate.barrier_id) continue;
        if (record.restart_identity != candidate.restart_identity) continue;
        if (record.cascade != candidate.cascade) continue;
        if (!tableEmptyingAffectedTablesEqual(record.affected_table_ids, candidate.affected_table_ids)) continue;
        try appendUniqueTableEmptyingJobId(alloc, &completed_job_ids, record.job_id);
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
        const barrier_job_ids = (try completedTableEmptyingBarrierJobIdsForCandidateAlloc(alloc, snapshot, candidate)) orelse continue;
        defer alloc.free(barrier_job_ids);
        for (barrier_job_ids) |job_id| try appendUniqueTableEmptyingJobId(alloc, &completed_job_ids, job_id);
    }

    return try completed_job_ids.toOwnedSlice(alloc);
}

fn appendTableEmptyingGenerationPromotion(
    alloc: std.mem.Allocator,
    promotions: *std.ArrayListUnmanaged(metadata_table_manager.TableEmptyingBarrierPromotion),
    table_id: u64,
    target_generation: u64,
) !void {
    for (promotions.items) |*existing| {
        if (existing.table_id != table_id) continue;
        existing.target_generation = @max(existing.target_generation, target_generation);
        return;
    }
    try promotions.append(alloc, .{
        .table_id = table_id,
        .target_generation = target_generation,
    });
}

fn tableEmptyingGenerationPromotionsForJobIdsAlloc(
    alloc: std.mem.Allocator,
    snapshot: *const metadata_api.AdminSnapshot,
    job_ids: []const u64,
) ![]metadata_table_manager.TableEmptyingBarrierPromotion {
    var promotions = std.ArrayListUnmanaged(metadata_table_manager.TableEmptyingBarrierPromotion).empty;
    errdefer promotions.deinit(alloc);

    for (job_ids) |job_id| {
        const record = findTableEmptyingJobById(snapshot, job_id) orelse continue;
        try appendTableEmptyingGenerationPromotion(alloc, &promotions, record.table_id, record.data_generation +| 1);
    }

    return try promotions.toOwnedSlice(alloc);
}

fn tableEmptyingBarrierAlreadyPromoted(barrier_ids: []const u64, barrier_id: u64) bool {
    for (barrier_ids) |existing| {
        if (existing == barrier_id) return true;
    }
    return false;
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
        !@hasDecl(ServiceDeclType, "freeAdminSnapshot"))
    {
        return error.UnsupportedOperation;
    }

    var snapshot = try service.adminSnapshot();
    defer service.freeAdminSnapshot(&snapshot);
    const table = table_ddl.findTableByName(&snapshot, table_name) orelse return error.TableNotFound;
    return try promoteCompletedTableEmptyingBarriersForTableIdOnServiceAlloc(alloc, service, table.table_id);
}

pub fn promoteCompletedTableEmptyingBarriersForTableIdOnServiceAlloc(
    alloc: std.mem.Allocator,
    service: anytype,
    table_id: u64,
) !usize {
    const ServiceType = @TypeOf(service);
    const ServiceDeclType = switch (@typeInfo(ServiceType)) {
        .pointer => |pointer| pointer.child,
        else => ServiceType,
    };
    if (!@hasDecl(ServiceDeclType, "adminSnapshot") or
        !@hasDecl(ServiceDeclType, "freeAdminSnapshot") or
        !@hasDecl(ServiceDeclType, "promoteTableEmptyingBarrier"))
    {
        return error.UnsupportedOperation;
    }

    var promoted_barriers = std.ArrayListUnmanaged(u64).empty;
    defer promoted_barriers.deinit(alloc);

    var promoted_jobs: usize = 0;
    while (true) {
        var snapshot = try service.adminSnapshot();
        defer service.freeAdminSnapshot(&snapshot);
        if (findTableById(&snapshot, table_id) == null) return error.TableNotFound;

        var promoted_this_pass = false;
        for (snapshot.table_emptying_jobs) |candidate| {
            if (!tableEmptyingJobInvolvesTable(candidate, table_id)) continue;
            if (candidate.barrier_id == 0) return error.InvalidTableEmptyingJob;
            if (tableEmptyingBarrierAlreadyPromoted(promoted_barriers.items, candidate.barrier_id)) continue;
            if (comptime @hasDecl(ServiceDeclType, "upsertTableEmptyingJob")) {
                const repaired = try repairMissingTableEmptyingBarrierRangeJobsForCandidate(service, &snapshot, candidate);
                if (repaired != 0) {
                    promoted_this_pass = true;
                    break;
                }
            }
            const job_ids = (try completedTableEmptyingBarrierJobIdsForCandidateAlloc(alloc, &snapshot, candidate)) orelse continue;
            defer alloc.free(job_ids);
            const promotions = try tableEmptyingGenerationPromotionsForJobIdsAlloc(alloc, &snapshot, job_ids);
            defer alloc.free(promotions);
            if (job_ids.len == 0 or promotions.len == 0) return error.InvalidTableEmptyingBarrierPromotion;

            if (candidate.restart_identity and !serviceSupportsTableEmptyingIdentityAllocatorReset(service)) {
                return error.UnsupportedOperation;
            }
            try service.promoteTableEmptyingBarrier(.{
                .job_ids = job_ids,
                .promotions = promotions,
            });
            try promoted_barriers.append(alloc, candidate.barrier_id);
            promoted_jobs += job_ids.len;
            promoted_this_pass = true;
            break;
        }
        if (!promoted_this_pass) break;
    }
    return promoted_jobs;
}

pub fn promoteCompletedTableEmptyingBarriersForTableAlloc(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
) !usize {
    return try promoteCompletedTableEmptyingBarriersForTableOnServiceAlloc(alloc, catalog, table_name);
}

pub fn promoteCompletedTableEmptyingBarriersForTableIdAlloc(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    table_id: u64,
) !usize {
    return try promoteCompletedTableEmptyingBarriersForTableIdOnServiceAlloc(alloc, catalog, table_id);
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
    var applied: table_ddl.AppliedRelationalSqlDdlRecord = .{
        .table = table,
        .rewrite_required = true,
        .work_items = work_items,
    };

    var service = FakeService{};
    defer service.deinit(alloc);
    try scheduleSchemaRewriteJobsForAppliedDdlOnService(&service, alloc, applied);
    try scheduleSchemaRewriteJobsForAppliedDdlOnService(&service, alloc, applied);

    const same_expression_job = try schemaRewriteJobForAppliedDdlWorkItem(alloc, applied.table, service.ranges[0], applied.work_items[0], 1);
    defer metadata_table_manager.freeSchemaRewriteJob(alloc, same_expression_job);
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
    defer metadata_table_manager.freeSchemaRewriteJob(alloc, alternate_expression_job);
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
    var applied: table_ddl.AppliedRelationalSqlDdlRecord = .{
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
    var applied: table_ddl.AppliedRelationalSqlDdlRecord = .{
        .table = table,
        .rewrite_required = true,
        .work_items = work_items,
    };

    var service = FakeService{};
    defer service.deinit(alloc);
    try scheduleSchemaRewriteJobsForAppliedDdlOnService(&service, alloc, applied);

    const same_row_plan_job = try schemaRewriteJobForAppliedDdlWorkItem(alloc, applied.table, service.ranges[0], applied.work_items[0], 1);
    defer metadata_table_manager.freeSchemaRewriteJob(alloc, same_row_plan_job);
    const alternate_item: sql_adapter.AppliedDdlWorkItem = .{
        .action = .rewrite,
        .subject = .table,
        .reason = .row_images,
        .row_rewrite_plan = .{ .drops = &.{"legacy_status"} },
    };
    const alternate_row_plan_job = try schemaRewriteJobForAppliedDdlWorkItem(alloc, applied.table, service.ranges[0], alternate_item, 1);
    defer metadata_table_manager.freeSchemaRewriteJob(alloc, alternate_row_plan_job);
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
    var applied: table_ddl.AppliedRelationalSqlDdlRecord = .{
        .table = table,
        .rewrite_required = true,
        .work_items = work_items,
    };

    var service = FakeService{};
    defer service.deinit(alloc);
    try scheduleSchemaRewriteJobsForAppliedDdlOnService(&service, alloc, applied);

    const same_full_job = try schemaRewriteJobForAppliedDdlWorkItem(alloc, applied.table, service.ranges[0], applied.work_items[0], 1);
    defer metadata_table_manager.freeSchemaRewriteJob(alloc, same_full_job);
    const alternate_item: sql_adapter.AppliedDdlWorkItem = .{
        .action = .rewrite,
        .subject = .table,
        .reason = .row_images,
        .row_rewrite_plan = .{ .drops = &.{"legacy_status"} },
    };
    const alternate_row_plan_job = try schemaRewriteJobForAppliedDdlWorkItem(alloc, applied.table, service.ranges[0], alternate_item, 1);
    defer metadata_table_manager.freeSchemaRewriteJob(alloc, alternate_row_plan_job);
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
    var applied: table_ddl.AppliedRelationalSqlDdlRecord = .{
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
    var applied: table_ddl.AppliedRelationalSqlDdlRecord = .{
        .table = table,
        .validation_required = true,
        .work_items = work_items,
    };
    defer applied.deinit(alloc);

    try std.testing.expect(appliedDdlHasSchemaRewriteWork(applied));
    applied.dropped_table = true;
    try std.testing.expect(!appliedDdlHasSchemaRewriteWork(applied));
}

test "catalog jobs promotes completed schema rewrite through table schema CAS" {
    const FakeCatalog = struct {
        table: metadata_table_manager.TableRecord = .{
            .table_id = 77,
            .name = "events",
            .schema_json = "{\"version\":2,\"storage_mode\":\"relational\"}",
            .read_schema_json = "{\"version\":1,\"storage_mode\":\"relational\"}",
        },
        job: metadata_table_manager.SchemaRewriteJobRecord = .{
            .job_id = 7701,
            .table_id = 77,
            .group_id = 1,
            .schema_generation = 2,
            .action = "rewrite",
            .reason = "row_images",
            .start_row_key = "",
            .state = metadata_table_manager.schema_rewrite_ready,
        },
        compare_count: usize = 0,

        fn iface(self: *@This()) table_catalog.CatalogSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                    .compare_and_swap_table_schema = compareAndSwapTableSchema,
                },
            };
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.job.schema_generation = metadata_table_manager.schemaRewriteGenerationForSchemaJson(self.table.schema_json);
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @as([*]metadata_table_manager.TableRecord, @ptrCast(&self.table))[0..1],
                .ranges = &.{},
                .schema_rewrite_jobs = @as([*]metadata_table_manager.SchemaRewriteJobRecord, @ptrCast(&self.job))[0..1],
                .stores = &.{},
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}

        fn compareAndSwapTableSchema(
            ptr: *anyopaque,
            request: metadata_table_manager.TableSchemaCompareAndSwapRequest,
        ) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.compare_count += 1;
            try std.testing.expectEqual(@as(u64, 77), request.table_id);
            try std.testing.expectEqualStrings(self.table.schema_json, request.expected_schema_json);
            try std.testing.expectEqualStrings(self.table.schema_json, request.promoted_table.schema_json);
            try std.testing.expectEqualStrings("", request.promoted_table.read_schema_json);
        }
    };

    var catalog = FakeCatalog{};
    try std.testing.expect(try promoteCompletedSchemaRewriteForTableIdAlloc(std.testing.allocator, catalog.iface(), 77));
    try std.testing.expectEqual(@as(usize, 1), catalog.compare_count);
}

test "catalog jobs rejects schema rewrite promotion while durable jobs are incomplete" {
    const FakeCatalog = struct {
        table: metadata_table_manager.TableRecord = .{
            .table_id = 77,
            .name = "events",
            .schema_json = "{\"version\":2,\"storage_mode\":\"relational\"}",
            .read_schema_json = "{\"version\":1,\"storage_mode\":\"relational\"}",
        },
        job: metadata_table_manager.SchemaRewriteJobRecord = .{
            .job_id = 7701,
            .table_id = 77,
            .group_id = 1,
            .schema_generation = 2,
            .action = "rewrite",
            .reason = "row_images",
            .start_row_key = "",
            .state = metadata_table_manager.schema_rewrite_declared,
        },

        fn iface(self: *@This()) table_catalog.CatalogSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                    .compare_and_swap_table_schema = compareAndSwapTableSchema,
                },
            };
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.job.schema_generation = metadata_table_manager.schemaRewriteGenerationForSchemaJson(self.table.schema_json);
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @as([*]metadata_table_manager.TableRecord, @ptrCast(&self.table))[0..1],
                .ranges = &.{},
                .schema_rewrite_jobs = @as([*]metadata_table_manager.SchemaRewriteJobRecord, @ptrCast(&self.job))[0..1],
                .stores = &.{},
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}

        fn compareAndSwapTableSchema(_: *anyopaque, _: metadata_table_manager.TableSchemaCompareAndSwapRequest) !void {
            return error.TestUnexpectedResult;
        }
    };

    var catalog = FakeCatalog{};
    try std.testing.expectError(
        error.SchemaRewriteJobsIncomplete,
        promoteCompletedSchemaRewriteForTableIdAlloc(std.testing.allocator, catalog.iface(), 77),
    );
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

test "catalog jobs rejects malformed table emptying affected tables before scheduling" {
    const Scheduler = struct {
        called: bool = false,

        fn upsertTableEmptyingJob(self: *@This(), _: metadata_table_manager.TableEmptyingJobRecord) !void {
            self.called = true;
            return error.TestUnexpectedResult;
        }
    };
    const table = metadata_table_manager.TableRecord{
        .table_id = 91,
        .name = "events",
        .schema_json = "{\"version\":2,\"storage_mode\":\"relational\"}",
    };
    const archive_table = metadata_table_manager.TableRecord{
        .table_id = 92,
        .name = "events_archive",
        .schema_json = "{\"version\":2,\"storage_mode\":\"relational\",\"name\":\"archive\"}",
    };
    var ranges = [_]metadata_table_manager.RangeRecord{
        .{ .group_id = 8101, .range_id = 8102, .table_id = 91, .start_key = "", .end_key = null },
    };
    var tables = [_]metadata_table_manager.TableRecord{ table, archive_table };
    const snapshot = metadata_api.AdminSnapshot{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = tables[0..],
        .ranges = ranges[0..],
        .stores = &.{},
        .placement_intents = &.{},
        .split_transitions = &.{},
        .merge_transitions = &.{},
    };
    try std.testing.expectError(error.InvalidTableEmptyingJob, tableEmptyingBarrierIdForSnapshot(&snapshot, &.{ 91, 91 }, false, false));
    try std.testing.expectError(error.InvalidTableEmptyingJob, tableEmptyingBarrierIdForSnapshot(&snapshot, &.{0}, false, false));
    try std.testing.expectError(error.InvalidTableEmptyingJob, tableEmptyingBarrierIdForSnapshot(&snapshot, &.{ 92, 91 }, false, false));

    var scheduler = Scheduler{};
    try std.testing.expectError(
        error.InvalidTableEmptyingJob,
        scheduleTableEmptyingJobsForTableSnapshot(&scheduler, ranges[0..], table, &.{92}, false, false),
    );
    try std.testing.expect(!scheduler.called);

    try std.testing.expectError(
        error.InvalidTableEmptyingJob,
        scheduleTableEmptyingJobsForTableSnapshot(&scheduler, ranges[0..], table, &.{ 91, 91 }, false, false),
    );
    try std.testing.expect(!scheduler.called);

    try std.testing.expectError(
        error.InvalidTableEmptyingJob,
        scheduleTableEmptyingJobsForTableSnapshot(&scheduler, ranges[0..], table, &.{ 92, 91 }, false, false),
    );
    try std.testing.expect(!scheduler.called);

    try std.testing.expectError(
        error.InvalidTableEmptyingJob,
        scheduleTableEmptyingJobsForTableSnapshotWithBarrierId(&scheduler, ranges[0..], table, &.{91}, false, false, 0),
    );
    try std.testing.expect(!scheduler.called);
}

test "catalog jobs admits session qualified table emptying barrier with cascade" {
    const alloc = std.testing.allocator;
    const customers_schema =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}}}
    ;
    const tenant_orders_schema =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"customer_id":{"type":"keyword"}},"required":["id","customer_id"],"additionalProperties":false}}},"foreign_keys":[{"name":"orders_customer_id_fkey","columns":["customer_id"],"references":{"table":"customers","columns":["id"]},"on_delete":"cascade"}]}
    ;
    const FakeService = struct {
        tables: [3]metadata_table_manager.TableRecord = .{
            .{ .table_id = 1, .name = "customers", .namespace_name = "public", .schema_json = customers_schema },
            .{ .table_id = 2, .name = "customers", .namespace_name = "tenant", .schema_json = customers_schema },
            .{ .table_id = 3, .name = "orders", .namespace_name = "tenant", .schema_json = tenant_orders_schema },
        },
        ranges: [3]metadata_table_manager.RangeRecord = .{
            .{ .group_id = 8101, .range_id = 8102, .table_id = 1, .start_key = "", .end_key = null },
            .{ .group_id = 8201, .range_id = 8202, .table_id = 2, .start_key = "", .end_key = null },
            .{ .group_id = 8301, .range_id = 8302, .table_id = 3, .start_key = "", .end_key = null },
        },
        jobs: std.ArrayListUnmanaged(metadata_table_manager.TableEmptyingJobRecord) = .empty,

        fn deinit(self: *@This(), allocator: std.mem.Allocator) void {
            for (self.jobs.items) |record| metadata_table_manager.freeTableEmptyingJob(allocator, record);
            self.jobs.deinit(allocator);
        }

        pub fn adminSnapshot(self: *@This()) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = self.tables[0..],
                .ranges = self.ranges[0..],
                .stores = &.{},
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }

        fn freeAdminSnapshot(_: *@This(), _: *metadata_api.AdminSnapshot) void {}

        fn upsertTableEmptyingJob(self: *@This(), record: metadata_table_manager.TableEmptyingJobRecord) !void {
            const owned = try metadata_table_manager.cloneTableEmptyingJob(std.testing.allocator, record);
            errdefer metadata_table_manager.freeTableEmptyingJob(std.testing.allocator, owned);
            try self.jobs.append(std.testing.allocator, owned);
        }
    };

    var service = FakeService{};
    defer service.deinit(alloc);
    const search_path = [_][]const u8{"tenant"};
    try std.testing.expectError(error.UnsupportedOperation, scheduleTableEmptyingBarrierForTargetsOnServiceWithSessionAlloc(
        alloc,
        &service,
        "customers",
        &.{},
        true,
        true,
        .{ .search_path = search_path[0..] },
    ));
    try std.testing.expectEqual(@as(usize, 0), service.jobs.items.len);

    var admission = try scheduleTableEmptyingBarrierForTargetsOnServiceWithSessionAlloc(
        alloc,
        &service,
        "customers",
        &.{},
        false,
        true,
        .{ .search_path = search_path[0..] },
    );
    defer admission.applied.deinit(alloc);
    defer admission.deinitAffectedTables(alloc);

    try std.testing.expectEqual(@as(u64, 2), admission.applied.table.table_id);
    try std.testing.expectEqual(@as(usize, 2), admission.scheduled_jobs);
    try std.testing.expectEqual(@as(usize, 2), admission.affected_tables.len);
    try std.testing.expectEqual(@as(usize, 2), service.jobs.items.len);

    var saw_tenant_parent = false;
    var saw_tenant_child = false;
    for (service.jobs.items) |job| {
        try std.testing.expect(job.table_id != 1);
        try std.testing.expect(!job.restart_identity);
        try std.testing.expect(job.cascade);
        try std.testing.expectEqual(@as(usize, 2), job.affected_table_ids.len);
        try std.testing.expectEqual(@as(u64, 2), job.affected_table_ids[0]);
        try std.testing.expectEqual(@as(u64, 3), job.affected_table_ids[1]);
        saw_tenant_parent = saw_tenant_parent or job.table_id == 2;
        saw_tenant_child = saw_tenant_child or job.table_id == 3;
    }
    try std.testing.expect(saw_tenant_parent);
    try std.testing.expect(saw_tenant_child);

    var ordered_service = FakeService{};
    defer ordered_service.deinit(alloc);
    var ordered_admission = try scheduleTableEmptyingBarrierForTargetsOnServiceWithSessionAlloc(
        alloc,
        &ordered_service,
        "orders",
        &.{"customers"},
        false,
        false,
        .{ .search_path = search_path[0..] },
    );
    defer ordered_admission.applied.deinit(alloc);
    defer ordered_admission.deinitAffectedTables(alloc);

    try std.testing.expectEqual(@as(u64, 3), ordered_admission.applied.table.table_id);
    try std.testing.expectEqual(@as(usize, 2), ordered_admission.scheduled_jobs);
    try std.testing.expectEqual(@as(usize, 2), ordered_service.jobs.items.len);
    for (ordered_service.jobs.items) |job| {
        try std.testing.expect(!job.cascade);
        try std.testing.expectEqual(@as(usize, 2), job.affected_table_ids.len);
        try std.testing.expectEqual(@as(u64, 2), job.affected_table_ids[0]);
        try std.testing.expectEqual(@as(u64, 3), job.affected_table_ids[1]);
    }
}

test "catalog jobs table emptying barrier waits for every affected table range" {
    const alloc = std.testing.allocator;
    var tables = [_]metadata_table_manager.TableRecord{
        .{ .table_id = 91, .name = "events", .schema_json = "{\"version\":2,\"storage_mode\":\"relational\"}" },
        .{ .table_id = 92, .name = "events_archive", .schema_json = "{\"version\":2,\"storage_mode\":\"relational\",\"name\":\"archive\"}" },
    };
    var ranges = [_]metadata_table_manager.RangeRecord{
        .{ .group_id = 8101, .range_id = 8102, .table_id = 91, .start_key = "", .end_key = null },
        .{ .group_id = 8201, .range_id = 8202, .table_id = 92, .start_key = "", .end_key = null },
    };
    const affected = [_]u64{ 91, 92 };
    const barrier_snapshot = metadata_api.AdminSnapshot{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = tables[0..],
        .ranges = ranges[0..],
        .stores = &.{},
        .placement_intents = &.{},
        .split_transitions = &.{},
        .merge_transitions = &.{},
    };
    const barrier_id = try tableEmptyingBarrierIdForSnapshot(&barrier_snapshot, affected[0..], true, true);
    var ready = tableEmptyingJobForRangeWithBarrierId(tables[0], ranges[0], affected[0..], true, true, barrier_id);
    ready.state = metadata_table_manager.table_emptying_ready;
    var jobs = [_]metadata_table_manager.TableEmptyingJobRecord{ready};
    const snapshot = metadata_api.AdminSnapshot{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = tables[0..],
        .ranges = ranges[0..],
        .stores = &.{},
        .placement_intents = &.{},
        .table_emptying_jobs = jobs[0..],
        .split_transitions = &.{},
        .merge_transitions = &.{},
    };

    const completed = try completedTableEmptyingBarrierJobIdsForTableAlloc(alloc, &snapshot, 91);
    defer alloc.free(completed);
    try std.testing.expectEqual(@as(usize, 0), completed.len);
}

test "catalog jobs table emptying barrier rejects invalid affected range job" {
    const alloc = std.testing.allocator;
    var tables = [_]metadata_table_manager.TableRecord{
        .{ .table_id = 91, .name = "events", .schema_json = "{\"version\":2,\"storage_mode\":\"relational\"}" },
        .{ .table_id = 92, .name = "events_archive", .schema_json = "{\"version\":2,\"storage_mode\":\"relational\",\"name\":\"archive\"}" },
    };
    var ranges = [_]metadata_table_manager.RangeRecord{
        .{ .group_id = 8101, .range_id = 8102, .table_id = 91, .start_key = "", .end_key = null },
        .{ .group_id = 8201, .range_id = 8202, .table_id = 92, .start_key = "", .end_key = null },
    };
    const affected = [_]u64{ 91, 92 };
    const barrier_snapshot = metadata_api.AdminSnapshot{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = tables[0..],
        .ranges = ranges[0..],
        .stores = &.{},
        .placement_intents = &.{},
        .split_transitions = &.{},
        .merge_transitions = &.{},
    };
    const barrier_id = try tableEmptyingBarrierIdForSnapshot(&barrier_snapshot, affected[0..], true, true);
    var first = tableEmptyingJobForRangeWithBarrierId(tables[0], ranges[0], affected[0..], true, true, barrier_id);
    first.state = metadata_table_manager.table_emptying_ready;
    var second = tableEmptyingJobForRangeWithBarrierId(tables[1], ranges[1], affected[0..], true, true, barrier_id);
    second.state = metadata_table_manager.table_emptying_invalid;
    var jobs = [_]metadata_table_manager.TableEmptyingJobRecord{ first, second };
    const snapshot = metadata_api.AdminSnapshot{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = tables[0..],
        .ranges = ranges[0..],
        .stores = &.{},
        .placement_intents = &.{},
        .table_emptying_jobs = jobs[0..],
        .split_transitions = &.{},
        .merge_transitions = &.{},
    };

    try std.testing.expectError(error.TableEmptyingJobInvalid, completedTableEmptyingBarrierJobIdsForTableAlloc(alloc, &snapshot, 91));
}

test "catalog jobs repairs table emptying barrier jobs after range topology changes" {
    const alloc = std.testing.allocator;
    const FakeService = struct {
        tables: [1]metadata_table_manager.TableRecord = .{
            .{
                .table_id = 91,
                .name = "events",
                .schema_json = "{\"version\":2,\"storage_mode\":\"relational\"}",
                .data_generation = 4,
            },
        },
        old_ranges: [1]metadata_table_manager.RangeRecord = .{
            .{
                .group_id = 8100,
                .range_id = 8101,
                .table_id = 91,
                .start_key = "",
                .end_key = null,
            },
        },
        ranges: [2]metadata_table_manager.RangeRecord = .{
            .{ .group_id = 8102, .range_id = 8103, .table_id = 91, .start_key = "", .end_key = "m" },
            .{ .group_id = 8104, .range_id = 8105, .table_id = 91, .start_key = "m", .end_key = null },
        },
        affected: [1]u64 = .{91},
        jobs: [4]metadata_table_manager.TableEmptyingJobRecord = undefined,
        job_count: usize = 1,
        promote_calls: usize = 0,

        fn seed(self: *@This()) !void {
            const barrier_snapshot = metadata_api.AdminSnapshot{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = self.tables[0..],
                .ranges = self.old_ranges[0..],
                .stores = &.{},
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
            const barrier_id = try tableEmptyingBarrierIdForSnapshot(&barrier_snapshot, self.affected[0..], false, false);
            self.jobs[0] = tableEmptyingJobForRangeWithBarrierId(self.tables[0], self.old_ranges[0], self.affected[0..], false, false, barrier_id);
            self.jobs[0].state = metadata_table_manager.table_emptying_invalid;
            self.jobs[0].last_error = @errorName(error.TopologyChanged);
        }

        pub fn adminSnapshot(self: *@This()) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = self.tables[0..],
                .ranges = self.ranges[0..],
                .stores = &.{},
                .placement_intents = &.{},
                .table_emptying_jobs = self.jobs[0..self.job_count],
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }

        pub fn freeAdminSnapshot(_: *@This(), _: *metadata_api.AdminSnapshot) void {}

        pub fn upsertTableEmptyingJob(self: *@This(), record: metadata_table_manager.TableEmptyingJobRecord) !void {
            for (self.jobs[0..self.job_count]) |*existing| {
                if (existing.job_id != record.job_id) continue;
                existing.* = record;
                return;
            }
            if (self.job_count == self.jobs.len) return error.TestUnexpectedResult;
            self.jobs[self.job_count] = record;
            self.job_count += 1;
        }

        pub fn promoteTableEmptyingBarrier(self: *@This(), request: metadata_table_manager.TableEmptyingBarrierPromotionRequest) !void {
            self.promote_calls += 1;
            try std.testing.expectEqual(@as(usize, 2), request.job_ids.len);
            try std.testing.expectEqual(@as(usize, 1), request.promotions.len);
            self.tables[0].data_generation = request.promotions[0].target_generation;
            for (request.job_ids) |job_id| {
                var i: usize = 0;
                while (i < self.job_count) : (i += 1) {
                    if (self.jobs[i].job_id != job_id) continue;
                    if (i + 1 < self.job_count) self.jobs[i] = self.jobs[self.job_count - 1];
                    self.job_count -= 1;
                    break;
                } else return error.UnknownTableEmptyingJob;
            }
        }
    };

    var service = FakeService{};
    try service.seed();

    const repaired = try repairMissingTableEmptyingBarrierJobsForTableIdOnServiceAlloc(alloc, &service, 91);
    try std.testing.expectEqual(@as(usize, 2), repaired);
    try std.testing.expectEqual(@as(usize, 3), service.job_count);
    try std.testing.expectEqualStrings(metadata_table_manager.table_emptying_invalid, service.jobs[0].state);
    try std.testing.expectEqualStrings(metadata_table_manager.table_emptying_declared, service.jobs[1].state);
    try std.testing.expectEqualStrings(metadata_table_manager.table_emptying_declared, service.jobs[2].state);
    try std.testing.expectEqual(@as(u64, 8102), service.jobs[1].group_id);
    try std.testing.expectEqual(@as(u64, 8104), service.jobs[2].group_id);

    service.jobs[1].state = metadata_table_manager.table_emptying_ready;
    service.jobs[2].state = metadata_table_manager.table_emptying_ready;
    const promoted = try promoteCompletedTableEmptyingBarriersForTableIdOnServiceAlloc(alloc, &service, 91);
    try std.testing.expectEqual(@as(usize, 2), promoted);
    try std.testing.expectEqual(@as(usize, 1), service.promote_calls);
    try std.testing.expectEqual(@as(u64, 5), service.tables[0].data_generation);
    try std.testing.expectEqual(@as(usize, 1), service.job_count);
    try std.testing.expectEqualStrings(metadata_table_manager.table_emptying_invalid, service.jobs[0].state);
}

test "catalog jobs promotes completed table emptying barrier by removing durable jobs" {
    const alloc = std.testing.allocator;
    const FakeService = struct {
        tables: [2]metadata_table_manager.TableRecord = .{
            .{ .table_id = 91, .name = "events", .schema_json = "{\"version\":2,\"storage_mode\":\"relational\"}", .data_generation = 4 },
            .{ .table_id = 92, .name = "events_archive", .schema_json = "{\"version\":2,\"storage_mode\":\"relational\",\"name\":\"archive\"}", .data_generation = 7 },
        },
        ranges: [2]metadata_table_manager.RangeRecord = .{
            .{ .group_id = 8101, .range_id = 8102, .table_id = 91, .start_key = "", .end_key = null },
            .{ .group_id = 8201, .range_id = 8202, .table_id = 92, .start_key = "", .end_key = null },
        },
        affected: [2]u64 = .{ 91, 92 },
        jobs: [2]metadata_table_manager.TableEmptyingJobRecord = undefined,
        job_count: usize = 2,
        reset_calls: usize = 0,

        fn seed(self: *@This()) !void {
            const barrier_snapshot = metadata_api.AdminSnapshot{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = self.tables[0..],
                .ranges = self.ranges[0..],
                .stores = &.{},
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
            const barrier_id = try tableEmptyingBarrierIdForSnapshot(&barrier_snapshot, self.affected[0..], true, true);
            self.jobs[0] = tableEmptyingJobForRangeWithBarrierId(self.tables[0], self.ranges[0], self.affected[0..], true, true, barrier_id);
            self.jobs[0].state = metadata_table_manager.table_emptying_ready;
            self.jobs[1] = tableEmptyingJobForRangeWithBarrierId(self.tables[1], self.ranges[1], self.affected[0..], true, true, barrier_id);
            self.jobs[1].state = metadata_table_manager.table_emptying_ready;
        }

        fn deinit(_: *@This(), _: std.mem.Allocator) void {}

        pub fn adminSnapshot(self: *@This()) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = self.tables[0..],
                .ranges = self.ranges[0..],
                .stores = &.{},
                .placement_intents = &.{},
                .table_emptying_jobs = self.jobs[0..self.job_count],
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }

        pub fn freeAdminSnapshot(_: *@This(), _: *metadata_api.AdminSnapshot) void {}

        pub fn resetIdentityAllocatorsForTableEmptyingBarrier(self: *@This(), request: metadata_table_manager.TableEmptyingIdentityAllocatorResetRequest) !void {
            if (request.barrier_id != self.jobs[0].barrier_id) return error.InvalidTableEmptyingBarrierPromotion;
            if (request.affected_table_ids.len != self.affected.len) return error.InvalidTableEmptyingBarrierPromotion;
            if (request.job_ids.len != self.job_count) return error.InvalidTableEmptyingBarrierPromotion;
            if (!request.cascade) return error.InvalidTableEmptyingBarrierPromotion;
        }

        pub fn promoteTableEmptyingBarrier(self: *@This(), request: metadata_table_manager.TableEmptyingBarrierPromotionRequest) !void {
            if (self.job_count > 0 and self.jobs[0].restart_identity) self.reset_calls += 1;
            for (request.promotions) |promotion| {
                for (&self.tables) |*table| {
                    if (table.table_id != promotion.table_id) continue;
                    table.data_generation = @max(table.data_generation, promotion.target_generation);
                    break;
                } else return error.TableNotFound;
            }
            for (request.job_ids) |job_id| {
                var i: usize = 0;
                while (i < self.job_count) : (i += 1) {
                    if (self.jobs[i].job_id != job_id) continue;
                    if (i + 1 < self.job_count) self.jobs[i] = self.jobs[self.job_count - 1];
                    self.job_count -= 1;
                    break;
                } else return error.UnknownTableEmptyingJob;
            }
        }
    };

    var service = FakeService{};
    try service.seed();
    defer service.deinit(alloc);

    const promoted = try promoteCompletedTableEmptyingBarriersForTableOnServiceAlloc(alloc, &service, "events");

    try std.testing.expectEqual(@as(usize, 2), promoted);
    try std.testing.expectEqual(@as(usize, 1), service.reset_calls);
    try std.testing.expectEqual(@as(u64, 5), service.tables[0].data_generation);
    try std.testing.expectEqual(@as(u64, 8), service.tables[1].data_generation);
    try std.testing.expectEqual(@as(usize, 0), service.job_count);

    const promoted_again = try promoteCompletedTableEmptyingBarriersForTableOnServiceAlloc(alloc, &service, "events");
    try std.testing.expectEqual(@as(usize, 0), promoted_again);
    try std.testing.expectEqual(@as(u64, 5), service.tables[0].data_generation);
    try std.testing.expectEqual(@as(u64, 8), service.tables[1].data_generation);
}

test "catalog jobs rejects restart identity promotion without allocator reset owner" {
    const alloc = std.testing.allocator;
    const FakeService = struct {
        tables: [1]metadata_table_manager.TableRecord = .{
            .{ .table_id = 91, .name = "events", .schema_json = "{\"version\":2,\"storage_mode\":\"relational\"}", .data_generation = 4 },
        },
        ranges: [1]metadata_table_manager.RangeRecord = .{
            .{ .group_id = 8101, .range_id = 8102, .table_id = 91, .start_key = "", .end_key = null },
        },
        affected: [1]u64 = .{91},
        jobs: [1]metadata_table_manager.TableEmptyingJobRecord = undefined,

        fn seed(self: *@This()) !void {
            const barrier_snapshot = metadata_api.AdminSnapshot{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = self.tables[0..],
                .ranges = self.ranges[0..],
                .stores = &.{},
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
            const barrier_id = try tableEmptyingBarrierIdForSnapshot(&barrier_snapshot, self.affected[0..], true, false);
            self.jobs[0] = tableEmptyingJobForRangeWithBarrierId(self.tables[0], self.ranges[0], self.affected[0..], true, false, barrier_id);
            self.jobs[0].state = metadata_table_manager.table_emptying_ready;
        }

        pub fn adminSnapshot(self: *@This()) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = self.tables[0..],
                .ranges = self.ranges[0..],
                .stores = &.{},
                .placement_intents = &.{},
                .table_emptying_jobs = self.jobs[0..],
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }

        pub fn freeAdminSnapshot(_: *@This(), _: *metadata_api.AdminSnapshot) void {}

        pub fn promoteTableEmptyingBarrier(_: *@This(), _: metadata_table_manager.TableEmptyingBarrierPromotionRequest) !void {
            return error.TestUnexpectedResult;
        }
    };

    var service = FakeService{};
    try service.seed();
    try std.testing.expectError(
        error.UnsupportedOperation,
        promoteCompletedTableEmptyingBarriersForTableIdOnServiceAlloc(alloc, &service, 91),
    );
    try std.testing.expectEqual(@as(u64, 4), service.tables[0].data_generation);
}

test "catalog jobs promotes restart identity barrier through catalog source promotion hook" {
    const alloc = std.testing.allocator;
    const FakeCatalogOwner = struct {
        tables: [1]metadata_table_manager.TableRecord = .{
            .{ .table_id = 91, .name = "events", .schema_json = "{\"version\":2,\"storage_mode\":\"relational\"}", .data_generation = 4 },
        },
        ranges: [1]metadata_table_manager.RangeRecord = .{
            .{ .group_id = 8101, .range_id = 8102, .table_id = 91, .start_key = "", .end_key = null },
        },
        affected: [1]u64 = .{91},
        jobs: [1]metadata_table_manager.TableEmptyingJobRecord = undefined,
        job_count: usize = 1,
        reset_calls: usize = 0,
        promote_calls: usize = 0,

        fn seed(self: *@This()) !void {
            const barrier_snapshot = metadata_api.AdminSnapshot{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = self.tables[0..],
                .ranges = self.ranges[0..],
                .stores = &.{},
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
            const barrier_id = try tableEmptyingBarrierIdForSnapshot(&barrier_snapshot, self.affected[0..], true, false);
            self.jobs[0] = tableEmptyingJobForRangeWithBarrierId(self.tables[0], self.ranges[0], self.affected[0..], true, false, barrier_id);
            self.jobs[0].state = metadata_table_manager.table_emptying_ready;
        }

        fn iface(self: *@This()) table_catalog.CatalogSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                    .promote_table_emptying_barrier = promoteTableEmptyingBarrier,
                    .reset_identity_allocators_for_table_emptying_barrier = resetIdentityAllocatorsForTableEmptyingBarrier,
                },
            };
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = self.tables[0..],
                .ranges = self.ranges[0..],
                .stores = &.{},
                .placement_intents = &.{},
                .table_emptying_jobs = self.jobs[0..self.job_count],
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}

        fn resetIdentityAllocatorsForTableEmptyingBarrier(
            ptr: *anyopaque,
            request: metadata_table_manager.TableEmptyingIdentityAllocatorResetRequest,
        ) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(self.jobs[0].barrier_id, request.barrier_id);
            try std.testing.expectEqualSlices(u64, self.affected[0..], request.affected_table_ids);
            try std.testing.expectEqual(@as(usize, 1), request.job_ids.len);
            try std.testing.expectEqual(self.jobs[0].job_id, request.job_ids[0]);
            try std.testing.expect(!request.cascade);
        }

        fn promoteTableEmptyingBarrier(
            ptr: *anyopaque,
            request: metadata_table_manager.TableEmptyingBarrierPromotionRequest,
        ) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(@as(usize, 1), request.job_ids.len);
            try std.testing.expectEqual(@as(usize, 1), request.promotions.len);
            if (self.job_count > 0 and self.jobs[0].restart_identity) self.reset_calls += 1;
            self.promote_calls += 1;
            self.tables[0].data_generation = request.promotions[0].target_generation;
            self.job_count = 0;
        }
    };

    var owner = FakeCatalogOwner{};
    try owner.seed();
    const promoted = try promoteCompletedTableEmptyingBarriersForTableIdAlloc(alloc, owner.iface(), 91);

    try std.testing.expectEqual(@as(usize, 1), promoted);
    try std.testing.expectEqual(@as(usize, 1), owner.reset_calls);
    try std.testing.expectEqual(@as(usize, 1), owner.promote_calls);
    try std.testing.expectEqual(@as(u64, 5), owner.tables[0].data_generation);
}

test "catalog jobs promotes one completed table emptying barrier per metadata mutation" {
    const alloc = std.testing.allocator;
    const FakeService = struct {
        tables: [2]metadata_table_manager.TableRecord = .{
            .{ .table_id = 91, .name = "events", .schema_json = "{\"version\":2,\"storage_mode\":\"relational\"}", .data_generation = 4 },
            .{ .table_id = 92, .name = "events_archive", .schema_json = "{\"version\":2,\"storage_mode\":\"relational\",\"name\":\"archive\"}", .data_generation = 7 },
        },
        ranges: [2]metadata_table_manager.RangeRecord = .{
            .{ .group_id = 8101, .range_id = 8102, .table_id = 91, .start_key = "", .end_key = null },
            .{ .group_id = 8201, .range_id = 8202, .table_id = 92, .start_key = "", .end_key = null },
        },
        jobs: [3]metadata_table_manager.TableEmptyingJobRecord = undefined,
        job_count: usize = 3,
        promote_calls: usize = 0,
        single_affected: [1]u64 = .{91},
        cascade_affected: [2]u64 = .{ 91, 92 },

        fn seed(self: *@This()) !void {
            const barrier_snapshot = metadata_api.AdminSnapshot{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = self.tables[0..],
                .ranges = self.ranges[0..],
                .stores = &.{},
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
            const single_barrier_id = try tableEmptyingBarrierIdForSnapshot(&barrier_snapshot, self.single_affected[0..], false, false);
            const cascade_barrier_id = try tableEmptyingBarrierIdForSnapshot(&barrier_snapshot, self.cascade_affected[0..], false, true);
            self.jobs[0] = tableEmptyingJobForRangeWithBarrierId(self.tables[0], self.ranges[0], self.single_affected[0..], false, false, single_barrier_id);
            self.jobs[0].state = metadata_table_manager.table_emptying_ready;
            self.jobs[1] = tableEmptyingJobForRangeWithBarrierId(self.tables[0], self.ranges[0], self.cascade_affected[0..], false, true, cascade_barrier_id);
            self.jobs[1].state = metadata_table_manager.table_emptying_ready;
            self.jobs[2] = tableEmptyingJobForRangeWithBarrierId(self.tables[1], self.ranges[1], self.cascade_affected[0..], false, true, cascade_barrier_id);
            self.jobs[2].state = metadata_table_manager.table_emptying_ready;
        }

        pub fn adminSnapshot(self: *@This()) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = self.tables[0..],
                .ranges = self.ranges[0..],
                .stores = &.{},
                .placement_intents = &.{},
                .table_emptying_jobs = self.jobs[0..self.job_count],
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }

        pub fn freeAdminSnapshot(_: *@This(), _: *metadata_api.AdminSnapshot) void {}

        fn jobBarrierId(self: *@This(), job_id: u64) !u64 {
            for (self.jobs[0..self.job_count]) |job| {
                if (job.job_id == job_id) return job.barrier_id;
            }
            return error.UnknownTableEmptyingJob;
        }

        pub fn promoteTableEmptyingBarrier(self: *@This(), request: metadata_table_manager.TableEmptyingBarrierPromotionRequest) !void {
            if (request.job_ids.len == 0) return error.InvalidTableEmptyingBarrierPromotion;
            const barrier_id = try self.jobBarrierId(request.job_ids[0]);
            for (request.job_ids[1..]) |job_id| {
                if (try self.jobBarrierId(job_id) != barrier_id) return error.InvalidTableEmptyingBarrierPromotion;
            }
            self.promote_calls += 1;
            for (request.promotions) |promotion| {
                for (&self.tables) |*table| {
                    if (table.table_id != promotion.table_id) continue;
                    table.data_generation = @max(table.data_generation, promotion.target_generation);
                    break;
                } else return error.TableNotFound;
            }
            for (request.job_ids) |job_id| {
                var i: usize = 0;
                while (i < self.job_count) : (i += 1) {
                    if (self.jobs[i].job_id != job_id) continue;
                    if (i + 1 < self.job_count) self.jobs[i] = self.jobs[self.job_count - 1];
                    self.job_count -= 1;
                    break;
                } else return error.UnknownTableEmptyingJob;
            }
        }
    };

    var service = FakeService{};
    try service.seed();

    const promoted = try promoteCompletedTableEmptyingBarriersForTableIdOnServiceAlloc(alloc, &service, 91);
    try std.testing.expectEqual(@as(usize, 1), promoted);
    try std.testing.expectEqual(@as(usize, 1), service.promote_calls);
    try std.testing.expectEqual(@as(u64, 5), service.tables[0].data_generation);
    try std.testing.expectEqual(@as(u64, 7), service.tables[1].data_generation);
    try std.testing.expectEqual(@as(usize, 2), service.job_count);
}

test "catalog jobs promotes completed table emptying barrier by table id" {
    const alloc = std.testing.allocator;
    const FakeService = struct {
        tables: [2]metadata_table_manager.TableRecord = .{
            .{ .table_id = 91, .name = "events", .namespace_name = "public", .schema_json = "{\"version\":2,\"storage_mode\":\"relational\"}", .data_generation = 4 },
            .{ .table_id = 92, .name = "events", .namespace_name = "tenant", .schema_json = "{\"version\":2,\"storage_mode\":\"relational\"}", .data_generation = 7 },
        },
        ranges: [2]metadata_table_manager.RangeRecord = .{
            .{ .group_id = 8101, .range_id = 8102, .table_id = 91, .start_key = "", .end_key = null },
            .{ .group_id = 8201, .range_id = 8202, .table_id = 92, .start_key = "", .end_key = null },
        },
        affected: [1]u64 = .{92},
        jobs: [1]metadata_table_manager.TableEmptyingJobRecord = undefined,
        job_count: usize = 1,

        fn seed(self: *@This()) !void {
            const barrier_snapshot = metadata_api.AdminSnapshot{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = self.tables[0..],
                .ranges = self.ranges[0..],
                .stores = &.{},
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
            const barrier_id = try tableEmptyingBarrierIdForSnapshot(&barrier_snapshot, self.affected[0..], false, false);
            self.jobs[0] = tableEmptyingJobForRangeWithBarrierId(self.tables[1], self.ranges[1], self.affected[0..], false, false, barrier_id);
            self.jobs[0].state = metadata_table_manager.table_emptying_ready;
        }

        pub fn adminSnapshot(self: *@This()) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = self.tables[0..],
                .ranges = self.ranges[0..],
                .stores = &.{},
                .placement_intents = &.{},
                .table_emptying_jobs = self.jobs[0..self.job_count],
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }

        pub fn freeAdminSnapshot(_: *@This(), _: *metadata_api.AdminSnapshot) void {}

        pub fn promoteTableEmptyingBarrier(self: *@This(), request: metadata_table_manager.TableEmptyingBarrierPromotionRequest) !void {
            for (request.promotions) |promotion| {
                for (&self.tables) |*table| {
                    if (table.table_id != promotion.table_id) continue;
                    table.data_generation = @max(table.data_generation, promotion.target_generation);
                    break;
                } else return error.TableNotFound;
            }
            for (request.job_ids) |job_id| {
                if (self.job_count == 0 or self.jobs[0].job_id != job_id) return error.UnknownTableEmptyingJob;
                self.job_count = 0;
            }
        }
    };

    var service = FakeService{};
    try service.seed();
    const promoted = try promoteCompletedTableEmptyingBarriersForTableIdOnServiceAlloc(alloc, &service, 92);

    try std.testing.expectEqual(@as(usize, 1), promoted);
    try std.testing.expectEqual(@as(u64, 4), service.tables[0].data_generation);
    try std.testing.expectEqual(@as(u64, 8), service.tables[1].data_generation);
    try std.testing.expectEqual(@as(usize, 0), service.job_count);
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
    var applied: table_ddl.AppliedRelationalSqlDdlRecord = .{
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
