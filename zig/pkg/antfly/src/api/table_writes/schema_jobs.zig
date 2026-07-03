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

const metadata_api = @import("../../metadata/api.zig");
const metadata_table_manager = @import("../../metadata/table_manager.zig");
const db_mod = @import("../../storage/db/mod.zig");
const schema_mod = @import("../../schema/mod.zig");
const relational_rows_api = @import("../../sql/relational_rows.zig");
const storage_schema = @import("../../storage/schema.zig");
const table_catalog = @import("../../metadata/catalog/routing.zig");
const tables_api = @import("../../metadata/catalog/table_ddl.zig");
const table_write_relational_mutation = @import("relational_mutation.zig");

const mutateRowsFromSourceAutocommitOnDb = table_write_relational_mutation.mutateRowsFromSourceAutocommitOnDb;

pub const SecondaryIndexRebuildWorkerResult = struct {
    group_id: u64,
    table_id: u64,
    index_generation: u64,
    claimed: bool = false,
    completed: bool = false,
    invalidated: bool = false,
    report: db_mod.relational_store.SecondaryIndexRebuildReport = .{},
};

pub const SecondaryIndexRebuildWorkerPassResult = struct {
    complete: bool = true,
    ranges_scanned: u64 = 0,
    ranges_claimed: u64 = 0,
    ranges_completed: u64 = 0,
    ranges_busy: u64 = 0,
    indexes_promoted: u64 = 0,
    report: db_mod.relational_store.SecondaryIndexRebuildReport = .{},
    groups: []SecondaryIndexRebuildWorkerResult = &.{},

    pub fn deinit(self: *SecondaryIndexRebuildWorkerPassResult, alloc: std.mem.Allocator) void {
        if (self.groups.len > 0) alloc.free(self.groups);
        self.* = undefined;
    }
};

pub const SecondaryIndexRebuildGroupRequest = struct {
    record: metadata_table_manager.SecondaryIndexRebuildRangeRecord,
    worker_id: []const u8,
    lease_ms: u64 = 60_000,
};

pub const SchemaRewriteWorkerResult = struct {
    group_id: u64,
    table_id: u64,
    job_id: u64,
    claimed: bool = false,
    completed: bool = false,
    invalidated: bool = false,
};

pub const SchemaRewriteWorkerPassResult = struct {
    complete: bool = true,
    jobs_scanned: u64 = 0,
    jobs_claimed: u64 = 0,
    jobs_completed: u64 = 0,
    jobs_invalidated: u64 = 0,
    jobs_busy: u64 = 0,
    groups: []SchemaRewriteWorkerResult = &.{},

    pub fn deinit(self: *SchemaRewriteWorkerPassResult, alloc: std.mem.Allocator) void {
        if (self.groups.len > 0) alloc.free(self.groups);
        self.* = undefined;
    }
};

pub const SchemaRewriteGroupRequest = struct {
    record: metadata_table_manager.SchemaRewriteJobRecord,
    worker_id: []const u8,
    lease_ms: u64 = 60_000,
};

pub const TableEmptyingWorkerResult = struct {
    group_id: u64,
    table_id: u64,
    job_id: u64,
    claimed: bool = false,
    completed: bool = false,
    invalidated: bool = false,
    result: db_mod.types.RelationalRowsMutationSourceResult = .{},

    pub fn deinit(self: *TableEmptyingWorkerResult, alloc: std.mem.Allocator) void {
        self.result.deinit(alloc);
        self.* = undefined;
    }
};

pub const TableEmptyingWorkerPassResult = struct {
    complete: bool = true,
    jobs_scanned: u64 = 0,
    jobs_claimed: u64 = 0,
    jobs_completed: u64 = 0,
    jobs_invalidated: u64 = 0,
    jobs_busy: u64 = 0,
    rows_matched: u64 = 0,
    rows_staged: u64 = 0,
    groups: []TableEmptyingWorkerResult = &.{},

    pub fn deinit(self: *TableEmptyingWorkerPassResult, alloc: std.mem.Allocator) void {
        for (self.groups) |*group| group.deinit(alloc);
        if (self.groups.len > 0) alloc.free(self.groups);
        self.* = undefined;
    }
};

pub const TableEmptyingGroupRequest = struct {
    record: metadata_table_manager.TableEmptyingJobRecord,
    worker_id: []const u8,
    lease_ms: u64 = 60_000,
};

pub const SchemaJobWorkerAdmissionPolicy = struct {
    lease_ms: u64 = 60_000,
    max_work_units: usize = 1,
    allow_stale_lease_takeover: bool = true,

    pub fn validate(self: @This(), worker_id: []const u8) !void {
        if (worker_id.len == 0 or self.lease_ms == 0 or self.max_work_units == 0) return error.InvalidSchemaJobWorkerAdmissionPolicy;
    }
};

pub fn mergeSecondaryIndexRebuildReport(
    aggregate: *db_mod.relational_store.SecondaryIndexRebuildReport,
    next: db_mod.relational_store.SecondaryIndexRebuildReport,
) void {
    aggregate.scanned_rows += next.scanned_rows;
    aggregate.indexed_rows += next.indexed_rows;
    aggregate.deleted_entries += next.deleted_entries;
    aggregate.written_entries += next.written_entries;
}

pub fn runSecondaryIndexRebuildRangeGroupLocal(
    db: *db_mod.DB,
    metadata: anytype,
    record: metadata_table_manager.SecondaryIndexRebuildRangeRecord,
    worker_id: []const u8,
    now_ms: u64,
    lease_ms: u64,
) !SecondaryIndexRebuildWorkerResult {
    if (worker_id.len == 0 or lease_ms == 0) return error.InvalidSecondaryIndexRebuildRequest;
    const selector: metadata_table_manager.SecondaryIndexRebuildRangeSelector = .{
        .table_id = record.table_id,
        .index_name = record.index_name,
        .index_generation = record.index_generation,
        .start_row_key = record.start_row_key,
    };
    var result = SecondaryIndexRebuildWorkerResult{
        .group_id = record.group_id,
        .table_id = record.table_id,
        .index_generation = record.index_generation,
    };
    metadata.beginSecondaryIndexRebuildRange(.{
        .selector = selector,
        .lease_owner = worker_id,
        .now_ms = now_ms,
        .lease_expires_at_ms = now_ms +| lease_ms,
    }) catch |err| switch (err) {
        error.SecondaryIndexRebuildRangeClaimBusy,
        error.SecondaryIndexRebuildRangeNotDeclared,
        => return result,
        else => return err,
    };
    result.claimed = true;

    if (!(try secondaryIndexRebuildRangeMatchesCurrentCatalogRange(metadata, record))) {
        metadata.invalidateSecondaryIndexRebuildRange(.{
            .selector = selector,
            .last_error = @errorName(error.TopologyChanged),
        }) catch {};
        result.invalidated = true;
        return error.TopologyChanged;
    }

    const upper = record.end_row_key orelse "";
    result.report = db.rebuildRelationalSecondaryIndexInRange(
        record.index_name,
        record.index_generation,
        record.start_row_key,
        upper,
    ) catch |err| {
        metadata.invalidateSecondaryIndexRebuildRange(.{
            .selector = selector,
            .last_error = @errorName(err),
        }) catch {};
        result.invalidated = true;
        return err;
    };
    try metadata.finishSecondaryIndexRebuildRange(.{
        .selector = selector,
        .completed_row_count = result.report.scanned_rows,
        .progress_row_key = upper,
    });
    result.completed = true;
    return result;
}

fn secondaryIndexRebuildRangeMatchesCurrentCatalogRange(
    metadata: anytype,
    record: metadata_table_manager.SecondaryIndexRebuildRangeRecord,
) !bool {
    if (record.range_id == 0) return true;
    const MetadataType = @TypeOf(metadata);
    const MetadataDeclType = switch (@typeInfo(MetadataType)) {
        .pointer => |pointer| pointer.child,
        else => MetadataType,
    };
    if (comptime !@hasDecl(MetadataDeclType, "adminSnapshot") or !@hasDecl(MetadataDeclType, "freeAdminSnapshot")) {
        return true;
    }

    var snapshot = try metadata.adminSnapshot();
    defer metadata.freeAdminSnapshot(&snapshot);
    return secondaryIndexRebuildRangeMatchesSnapshotRange(&snapshot, record);
}

fn secondaryIndexRebuildRangeMatchesSnapshotRange(
    snapshot: *const metadata_api.AdminSnapshot,
    record: metadata_table_manager.SecondaryIndexRebuildRangeRecord,
) bool {
    if (record.range_id == 0) return true;
    for (snapshot.ranges) |range| {
        if (range.table_id != record.table_id) continue;
        if (range.group_id != record.group_id) continue;
        if (range.range_id != record.range_id) continue;
        if (!std.mem.eql(u8, range.start_key, record.start_row_key)) continue;
        if (!optionalStringsEqual(range.end_key, record.end_row_key)) continue;
        return true;
    }
    return false;
}

pub fn secondaryIndexRebuildRecordPending(record: metadata_table_manager.SecondaryIndexRebuildRangeRecord) bool {
    return std.mem.eql(u8, record.state, metadata_table_manager.secondary_index_rebuild_declared) or
        std.mem.eql(u8, record.state, metadata_table_manager.secondary_index_rebuild_building);
}

pub fn secondaryIndexRebuildRecordReady(record: metadata_table_manager.SecondaryIndexRebuildRangeRecord) bool {
    return std.mem.eql(u8, record.state, metadata_table_manager.secondary_index_rebuild_ready);
}

fn optionalStringsEqual(a: ?[]const u8, b: ?[]const u8) bool {
    if (a == null and b == null) return true;
    if (a == null or b == null) return false;
    return std.mem.eql(u8, a.?, b.?);
}

pub fn findSecondaryIndexRebuildRecordForRange(
    records: []const metadata_table_manager.SecondaryIndexRebuildRangeRecord,
    table_id: u64,
    index_name: []const u8,
    index_generation: u64,
    range: metadata_table_manager.RangeRecord,
) ?metadata_table_manager.SecondaryIndexRebuildRangeRecord {
    for (records) |record| {
        if (record.table_id != table_id) continue;
        if (record.index_generation != index_generation) continue;
        if (!std.mem.eql(u8, record.index_name, index_name)) continue;
        if (record.range_id != 0 and record.range_id != range.range_id) continue;
        if (!std.mem.eql(u8, record.start_row_key, range.start_key)) continue;
        if (!optionalStringsEqual(record.end_row_key, range.end_key)) continue;
        return record;
    }
    return null;
}

pub fn secondaryIndexReadyForPromotion(
    snapshot: *const metadata_api.AdminSnapshot,
    table_id: u64,
    index_name: []const u8,
    index_generation: u64,
) bool {
    var ranges_seen: usize = 0;
    for (snapshot.ranges) |range| {
        if (range.table_id != table_id) continue;
        ranges_seen += 1;
        const record = findSecondaryIndexRebuildRecordForRange(
            snapshot.secondary_index_rebuild_ranges,
            table_id,
            index_name,
            index_generation,
            range,
        ) orelse return false;
        if (!secondaryIndexRebuildRecordReady(record)) return false;
    }
    return ranges_seen > 0;
}

pub fn promoteReadySecondaryIndexesForCatalog(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
) !u64 {
    var snapshot = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&snapshot);
    const table = tables_api.findTableByName(&snapshot, table_name) orelse return 0;
    if (table.schema_json.len == 0) return 0;
    var parsed = try schema_mod.parseValidatedTableSchema(alloc, table.schema_json);
    defer parsed.deinit(alloc);
    if (parsed.storage_mode != .relational) return 0;
    const runtime = try schema_mod.deriveRuntimeTableSchema(alloc, parsed);
    defer storage_schema.freeSchema(alloc, runtime);

    var promoted: u64 = 0;
    for (runtime.relational_columns) |column| {
        if (!column.indexed) continue;
        if (column.index_lifecycle != .building) continue;
        if (column.index_generation == 0) continue;
        if (!secondaryIndexReadyForPromotion(&snapshot, table.table_id, column.name, column.index_generation)) continue;
        const did_promote = catalog.promoteSecondaryIndexReady(
            alloc,
            table_name,
            column.name,
            column.index_generation,
        ) catch |err| switch (err) {
            error.UnsupportedOperation => false,
            else => return err,
        };
        if (did_promote) promoted += 1;
    }
    return promoted;
}

pub fn runSecondaryIndexRebuildWorkerPassForCatalog(
    alloc: std.mem.Allocator,
    source: anytype,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
    worker_id: []const u8,
    lease_ms: u64,
    max_work_units: usize,
) !SecondaryIndexRebuildWorkerPassResult {
    if (worker_id.len == 0 or lease_ms == 0 or max_work_units == 0) return error.InvalidSecondaryIndexRebuildRequest;
    var snapshot = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&snapshot);
    const table = tables_api.findTableByName(&snapshot, table_name) orelse return .{};

    var groups = std.ArrayListUnmanaged(SecondaryIndexRebuildWorkerResult).empty;
    errdefer groups.deinit(alloc);

    var result: SecondaryIndexRebuildWorkerPassResult = .{};
    for (snapshot.secondary_index_rebuild_ranges) |record| {
        if (record.table_id != table.table_id) continue;
        if (!secondaryIndexRebuildRecordPending(record)) continue;
        result.ranges_scanned += 1;
        if (result.ranges_claimed >= max_work_units) {
            result.complete = false;
            continue;
        }

        const one = (try source.secondaryIndexRebuildGroupLocal(
            alloc,
            record.group_id,
            table_name,
            record,
            worker_id,
            lease_ms,
        )) orelse {
            result.complete = false;
            continue;
        };
        if (!one.claimed) {
            result.ranges_busy += 1;
            result.complete = false;
            try groups.append(alloc, one);
            continue;
        }

        result.ranges_claimed += 1;
        if (one.completed) {
            result.ranges_completed += 1;
            mergeSecondaryIndexRebuildReport(&result.report, one.report);
        } else {
            result.complete = false;
        }
        try groups.append(alloc, one);
    }

    result.indexes_promoted = try promoteReadySecondaryIndexesForCatalog(alloc, catalog, table_name);
    result.groups = try groups.toOwnedSlice(alloc);
    return result;
}

pub fn schemaRewriteRecordPending(record: metadata_table_manager.SchemaRewriteJobRecord) bool {
    return schemaRewriteRecordAdmitted(record, .{});
}

pub fn schemaRewriteRecordAdmitted(
    record: metadata_table_manager.SchemaRewriteJobRecord,
    policy: SchemaJobWorkerAdmissionPolicy,
) bool {
    return std.mem.eql(u8, record.state, metadata_table_manager.schema_rewrite_declared) or
        (policy.allow_stale_lease_takeover and std.mem.eql(u8, record.state, metadata_table_manager.schema_rewrite_running));
}

pub fn findSchemaRewriteJobById(
    records: []const metadata_table_manager.SchemaRewriteJobRecord,
    job_id: u64,
) ?metadata_table_manager.SchemaRewriteJobRecord {
    for (records) |record| {
        if (record.job_id == job_id) return record;
    }
    return null;
}

pub fn schemaRewriteJobResultState(
    catalog: table_catalog.CatalogSource,
    result: *SchemaRewriteWorkerResult,
) !void {
    var snapshot = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&snapshot);
    const record = findSchemaRewriteJobById(snapshot.schema_rewrite_jobs, result.job_id) orelse return;
    result.completed = std.mem.eql(u8, record.state, metadata_table_manager.schema_rewrite_ready);
    result.invalidated = std.mem.eql(u8, record.state, metadata_table_manager.schema_rewrite_invalid);
}

pub const SingleSchemaRewriteJobCatalogService = struct {
    catalog: table_catalog.CatalogSource,
    record: metadata_table_manager.SchemaRewriteJobRecord,

    pub fn listProjectedSchemaRewriteJobs(self: *@This(), alloc: std.mem.Allocator) ![]metadata_table_manager.SchemaRewriteJobRecord {
        const out = try alloc.alloc(metadata_table_manager.SchemaRewriteJobRecord, 1);
        errdefer alloc.free(out);
        out[0] = try metadata_table_manager.cloneSchemaRewriteJob(alloc, self.record);
        return out;
    }

    pub fn freeProjectedSchemaRewriteJobs(_: *@This(), alloc: std.mem.Allocator, records: []metadata_table_manager.SchemaRewriteJobRecord) void {
        for (records) |record| metadata_table_manager.freeSchemaRewriteJob(alloc, record);
        alloc.free(records);
    }

    pub fn beginSchemaRewriteJob(self: *@This(), request: metadata_table_manager.SchemaRewriteJobBeginRequest) !void {
        try self.catalog.beginSchemaRewriteJob(request);
        if (!(try schemaRewriteJobMatchesCurrentCatalogRange(self.catalog, self.record))) {
            self.catalog.invalidateSchemaRewriteJob(.{
                .job_id = self.record.job_id,
                .lease_owner = request.lease_owner,
                .last_error = @errorName(error.TopologyChanged),
            }) catch {};
            return error.TopologyChanged;
        }
    }

    pub fn finishSchemaRewriteJob(self: *@This(), request: metadata_table_manager.SchemaRewriteJobFinishRequest) !void {
        return try self.catalog.finishSchemaRewriteJob(request);
    }

    pub fn invalidateSchemaRewriteJob(self: *@This(), request: metadata_table_manager.SchemaRewriteJobInvalidateRequest) !void {
        return try self.catalog.invalidateSchemaRewriteJob(request);
    }
};

fn schemaRewriteJobMatchesCurrentCatalogRange(
    catalog: table_catalog.CatalogSource,
    record: metadata_table_manager.SchemaRewriteJobRecord,
) !bool {
    if (record.range_id == 0) return true;

    var snapshot = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&snapshot);

    for (snapshot.ranges) |range| {
        if (range.table_id != record.table_id) continue;
        if (range.group_id != record.group_id) continue;
        if (range.range_id != record.range_id) continue;
        if (!std.mem.eql(u8, range.start_key, record.start_row_key)) continue;
        if (!optionalStringsEqual(range.end_key, record.end_row_key)) continue;
        return true;
    }
    return false;
}

pub fn runSchemaRewriteJobGroupLocal(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    catalog: table_catalog.CatalogSource,
    record: metadata_table_manager.SchemaRewriteJobRecord,
    worker_id: []const u8,
    now_ms: u64,
    lease_ms: u64,
) !SchemaRewriteWorkerResult {
    if (worker_id.len == 0 or lease_ms == 0) return error.InvalidSchemaRewriteJobLease;
    var result = SchemaRewriteWorkerResult{
        .group_id = record.group_id,
        .table_id = record.table_id,
        .job_id = record.job_id,
    };
    var service = SingleSchemaRewriteJobCatalogService{ .catalog = catalog, .record = record };
    const progressed = try db.drainSchemaRewriteJobsForIdle(alloc, &service, .{
        .worker_id = worker_id,
        .group_id = record.group_id,
        .now_ms = now_ms,
        .lease_ttl_ms = lease_ms,
        .max_jobs = 1,
    });
    result.claimed = progressed != 0;
    try schemaRewriteJobResultState(catalog, &result);
    return result;
}

pub fn runSchemaRewriteWorkerPassForCatalog(
    alloc: std.mem.Allocator,
    source: anytype,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
    worker_id: []const u8,
    lease_ms: u64,
    max_work_units: usize,
) !SchemaRewriteWorkerPassResult {
    return try runSchemaRewriteWorkerPassForCatalogWithAdmission(alloc, source, catalog, table_name, worker_id, .{
        .lease_ms = lease_ms,
        .max_work_units = max_work_units,
    });
}

pub fn runSchemaRewriteWorkerPassForCatalogWithAdmission(
    alloc: std.mem.Allocator,
    source: anytype,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
    worker_id: []const u8,
    policy: SchemaJobWorkerAdmissionPolicy,
) !SchemaRewriteWorkerPassResult {
    policy.validate(worker_id) catch return error.InvalidSchemaRewriteJobLease;
    var snapshot = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&snapshot);
    const table = tables_api.findTableByName(&snapshot, table_name) orelse return .{};

    var groups = std.ArrayListUnmanaged(SchemaRewriteWorkerResult).empty;
    errdefer groups.deinit(alloc);

    var result: SchemaRewriteWorkerPassResult = .{};
    for (snapshot.schema_rewrite_jobs) |record| {
        if (record.table_id != table.table_id) continue;
        if (!schemaRewriteRecordAdmitted(record, policy)) continue;
        result.jobs_scanned += 1;
        if (result.jobs_claimed >= policy.max_work_units) {
            result.complete = false;
            continue;
        }

        const one = (try source.schemaRewriteGroupLocal(
            alloc,
            record.group_id,
            table_name,
            record,
            worker_id,
            policy.lease_ms,
        )) orelse {
            result.complete = false;
            continue;
        };
        if (!one.claimed) {
            if (one.completed) {
                result.jobs_completed += 1;
            } else if (one.invalidated) {
                result.jobs_invalidated += 1;
                result.complete = false;
            } else {
                result.jobs_busy += 1;
                result.complete = false;
            }
            try groups.append(alloc, one);
            continue;
        }

        result.jobs_claimed += 1;
        if (one.completed) {
            result.jobs_completed += 1;
        } else if (one.invalidated) {
            result.jobs_invalidated += 1;
            result.complete = false;
        } else {
            result.complete = false;
        }
        try groups.append(alloc, one);
    }

    result.groups = try groups.toOwnedSlice(alloc);
    return result;
}

pub fn tableEmptyingRecordPending(record: metadata_table_manager.TableEmptyingJobRecord) bool {
    return tableEmptyingRecordAdmitted(record, .{});
}

pub fn tableEmptyingRecordAdmitted(
    record: metadata_table_manager.TableEmptyingJobRecord,
    policy: SchemaJobWorkerAdmissionPolicy,
) bool {
    return std.mem.eql(u8, record.state, metadata_table_manager.table_emptying_declared) or
        (policy.allow_stale_lease_takeover and std.mem.eql(u8, record.state, metadata_table_manager.table_emptying_running));
}

pub fn findTableEmptyingJobById(
    records: []const metadata_table_manager.TableEmptyingJobRecord,
    job_id: u64,
) ?metadata_table_manager.TableEmptyingJobRecord {
    for (records) |record| {
        if (record.job_id == job_id) return record;
    }
    return null;
}

pub fn tableEmptyingJobResultState(
    catalog: table_catalog.CatalogSource,
    result: *TableEmptyingWorkerResult,
) !void {
    var snapshot = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&snapshot);
    const record = findTableEmptyingJobById(snapshot.table_emptying_jobs, result.job_id) orelse return;
    result.completed = std.mem.eql(u8, record.state, metadata_table_manager.table_emptying_ready);
    result.invalidated = std.mem.eql(u8, record.state, metadata_table_manager.table_emptying_invalid);
}

fn tableEmptyingJobTxnId(record: metadata_table_manager.TableEmptyingJobRecord) db_mod.types.TxnId {
    var txn_id: db_mod.types.TxnId = undefined;
    std.mem.writeInt(u64, txn_id[0..8], record.job_id, .big);
    std.mem.writeInt(u64, txn_id[8..16], record.attempts +% 1, .big);
    return txn_id;
}

fn tableEmptyingJobClaimOwnerAlloc(
    alloc: std.mem.Allocator,
    worker_id: []const u8,
    job_id: u64,
) ![]u8 {
    return try std.fmt.allocPrint(alloc, "table-emptying:{s}:{d}", .{ worker_id, job_id });
}

pub fn tableEmptyingMutationRequestForJobAlloc(
    alloc: std.mem.Allocator,
    record: metadata_table_manager.TableEmptyingJobRecord,
    worker_id: []const u8,
    lease_ms: u64,
    doc_key_range: db_mod.types.RelationalRowsDocKeyRange,
) !db_mod.types.RelationalRowsMutationSourceRequest {
    if (worker_id.len == 0 or lease_ms == 0) return error.InvalidTableEmptyingJobLease;
    return .{
        .kind = .delete,
        .restart_identity = record.restart_identity,
        .source = .{
            .select_all = true,
            .row_claim = .{
                .mode = .for_update,
                .wait_policy = .skip_locked,
                .skip_locked = true,
                .lease_ms = lease_ms,
                .owner_id = try tableEmptyingJobClaimOwnerAlloc(alloc, worker_id, record.job_id),
                .txn_id = tableEmptyingJobTxnId(record),
            },
            .doc_key_range = doc_key_range,
        },
    };
}

const DocumentTableEmptyingResult = struct {
    result: db_mod.types.RelationalRowsMutationSourceResult = .{},
    complete: bool = false,
};

fn deleteDocumentTableRangeForEmptyingJobAlloc(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    record: metadata_table_manager.TableEmptyingJobRecord,
    doc_key_range: db_mod.types.RelationalRowsDocKeyRange,
) !DocumentTableEmptyingResult {
    if (record.restart_identity) return error.InvalidTableEmptyingJob;

    var scan = try db.scan(alloc, doc_key_range.start, doc_key_range.end, .{
        .inclusive_from = true,
        .exclusive_to = doc_key_range.end.len > 0,
        .include_documents = false,
    });
    defer scan.deinit(alloc);

    if (scan.hashes.len == 0) {
        return .{
            .result = .{},
            .complete = true,
        };
    }

    const deletes = try alloc.alloc([]const u8, scan.hashes.len);
    defer alloc.free(deletes);
    for (scan.hashes, 0..) |entry, i| {
        deletes[i] = entry.id;
    }

    try db.batch(.{ .deletes = deletes });

    var remaining = try db.scan(alloc, doc_key_range.start, doc_key_range.end, .{
        .inclusive_from = true,
        .exclusive_to = doc_key_range.end.len > 0,
        .include_documents = false,
        .limit = 1,
    });
    defer remaining.deinit(alloc);

    const deleted_count: u32 = @intCast(scan.hashes.len);
    return .{
        .result = .{
            .matched = deleted_count,
            .staged = deleted_count,
        },
        .complete = remaining.hashes.len == 0,
    };
}

pub const SingleTableEmptyingJobCatalogService = struct {
    catalog: table_catalog.CatalogSource,
    record: metadata_table_manager.TableEmptyingJobRecord,

    pub fn listProjectedTableEmptyingJobs(self: *@This(), alloc: std.mem.Allocator) ![]metadata_table_manager.TableEmptyingJobRecord {
        const out = try alloc.alloc(metadata_table_manager.TableEmptyingJobRecord, 1);
        errdefer alloc.free(out);
        out[0] = try metadata_table_manager.cloneTableEmptyingJob(alloc, self.record);
        return out;
    }

    pub fn freeProjectedTableEmptyingJobs(_: *@This(), alloc: std.mem.Allocator, records: []metadata_table_manager.TableEmptyingJobRecord) void {
        for (records) |record| metadata_table_manager.freeTableEmptyingJob(alloc, record);
        alloc.free(records);
    }

    pub fn beginTableEmptyingJob(self: *@This(), request: metadata_table_manager.TableEmptyingJobBeginRequest) !void {
        return try self.catalog.beginTableEmptyingJob(request);
    }

    pub fn finishTableEmptyingJob(self: *@This(), request: metadata_table_manager.TableEmptyingJobFinishRequest) !void {
        return try self.catalog.finishTableEmptyingJob(request);
    }

    pub fn invalidateTableEmptyingJob(self: *@This(), request: metadata_table_manager.TableEmptyingJobInvalidateRequest) !void {
        return try self.catalog.invalidateTableEmptyingJob(request);
    }
};

fn tableEmptyingJobMatchesTableGeneration(
    table: metadata_table_manager.TableRecord,
    record: metadata_table_manager.TableEmptyingJobRecord,
) bool {
    return record.table_id == table.table_id and
        record.schema_generation == metadata_table_manager.schemaRewriteGenerationForSchemaJson(table.schema_json) and
        record.data_generation == table.data_generation;
}

fn tableEmptyingJobMatchesCurrentCatalogRange(
    catalog: table_catalog.CatalogSource,
    table: metadata_table_manager.TableRecord,
    record: metadata_table_manager.TableEmptyingJobRecord,
) !bool {
    if (record.range_id == 0) return true;

    var snapshot = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&snapshot);

    for (snapshot.ranges) |range| {
        if (range.table_id != table.table_id) continue;
        if (range.group_id != record.group_id) continue;
        if (range.range_id != record.range_id) continue;
        if (!std.mem.eql(u8, range.start_key, record.start_row_key)) continue;
        if (!optionalStringsEqual(range.end_key, record.end_row_key)) continue;
        return true;
    }
    return false;
}

pub fn runTableEmptyingJobGroupLocal(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    catalog: table_catalog.CatalogSource,
    table: metadata_table_manager.TableRecord,
    record: metadata_table_manager.TableEmptyingJobRecord,
    worker_id: []const u8,
    now_ms: u64,
    lease_ms: u64,
) !TableEmptyingWorkerResult {
    if (worker_id.len == 0 or lease_ms == 0) return error.InvalidTableEmptyingJobLease;
    var result = TableEmptyingWorkerResult{
        .group_id = record.group_id,
        .table_id = record.table_id,
        .job_id = record.job_id,
    };

    catalog.beginTableEmptyingJob(.{
        .job_id = record.job_id,
        .lease_owner = worker_id,
        .now_ms = now_ms,
        .lease_expires_at_ms = now_ms +| lease_ms,
    }) catch |err| switch (err) {
        error.TableEmptyingJobClaimBusy,
        error.TableEmptyingJobNotDeclared,
        => return result,
        else => return err,
    };
    result.claimed = true;

    if (!tableEmptyingJobMatchesTableGeneration(table, record)) {
        catalog.invalidateTableEmptyingJob(.{
            .job_id = record.job_id,
            .lease_owner = worker_id,
            .last_error = @errorName(error.InvalidTableEmptyingJob),
        }) catch {};
        result.invalidated = true;
        return error.InvalidTableEmptyingJob;
    }
    if (!metadata_table_manager.tableEmptyingAffectedTableIdsCanonicalValid(table.table_id, record.affected_table_ids)) {
        catalog.invalidateTableEmptyingJob(.{
            .job_id = record.job_id,
            .lease_owner = worker_id,
            .last_error = @errorName(error.InvalidTableEmptyingJob),
        }) catch {};
        result.invalidated = true;
        return error.InvalidTableEmptyingJob;
    }
    if (!(try tableEmptyingJobMatchesCurrentCatalogRange(catalog, table, record))) {
        catalog.invalidateTableEmptyingJob(.{
            .job_id = record.job_id,
            .lease_owner = worker_id,
            .last_error = @errorName(error.TopologyChanged),
        }) catch {};
        result.invalidated = true;
        return error.TopologyChanged;
    }
    if (table.schema_json.len == 0) {
        catalog.invalidateTableEmptyingJob(.{
            .job_id = record.job_id,
            .lease_owner = worker_id,
            .last_error = @errorName(error.InvalidTableEmptyingJob),
        }) catch {};
        result.invalidated = true;
        return error.InvalidTableEmptyingJob;
    }
    var parsed = schema_mod.parseValidatedTableSchema(alloc, table.schema_json) catch |err| {
        catalog.invalidateTableEmptyingJob(.{
            .job_id = record.job_id,
            .lease_owner = worker_id,
            .last_error = @errorName(err),
        }) catch {};
        result.invalidated = true;
        return err;
    };
    defer parsed.deinit(alloc);
    const runtime_schema = schema_mod.deriveRuntimeTableSchema(alloc, parsed) catch |err| {
        catalog.invalidateTableEmptyingJob(.{
            .job_id = record.job_id,
            .lease_owner = worker_id,
            .last_error = @errorName(err),
        }) catch {};
        result.invalidated = true;
        return err;
    };
    defer storage_schema.freeSchema(alloc, runtime_schema);

    const doc_key_range: db_mod.types.RelationalRowsDocKeyRange = .{
        .start = record.start_row_key,
        .end = record.end_row_key orelse "",
    };

    const table_emptying_complete = switch (runtime_schema.storage_mode) {
        .relational => blk: {
            var req = try tableEmptyingMutationRequestForJobAlloc(alloc, record, worker_id, lease_ms, doc_key_range);
            defer req.source.deinit(alloc);

            result.result = mutateRowsFromSourceAutocommitOnDb(alloc, db, runtime_schema, req) catch |err| {
                catalog.invalidateTableEmptyingJob(.{
                    .job_id = record.job_id,
                    .lease_owner = worker_id,
                    .last_error = @errorName(err),
                }) catch {};
                result.invalidated = true;
                return err;
            };
            errdefer result.result.deinit(alloc);
            break :blk result.result.staged == result.result.matched;
        },
        .document => blk: {
            const document_result = deleteDocumentTableRangeForEmptyingJobAlloc(alloc, db, record, doc_key_range) catch |err| {
                catalog.invalidateTableEmptyingJob(.{
                    .job_id = record.job_id,
                    .lease_owner = worker_id,
                    .last_error = @errorName(err),
                }) catch {};
                result.invalidated = true;
                return err;
            };
            result.result = document_result.result;
            errdefer result.result.deinit(alloc);
            break :blk document_result.complete;
        },
    };
    if (!table_emptying_complete) return result;

    try catalog.finishTableEmptyingJob(.{
        .job_id = record.job_id,
        .lease_owner = worker_id,
        .completed_row_count = result.result.matched,
        .progress_row_key = doc_key_range.end,
    });
    result.completed = true;
    return result;
}

fn findTableRecordById(snapshot: *const metadata_api.AdminSnapshot, table_id: u64) ?metadata_table_manager.TableRecord {
    for (snapshot.tables) |table| {
        if (table.table_id == table_id) return table;
    }
    return null;
}

pub fn runTableEmptyingWorkerPassForCatalog(
    alloc: std.mem.Allocator,
    source: anytype,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
    worker_id: []const u8,
    lease_ms: u64,
    max_work_units: usize,
) !TableEmptyingWorkerPassResult {
    return try runTableEmptyingWorkerPassForCatalogWithAdmission(alloc, source, catalog, table_name, worker_id, .{
        .lease_ms = lease_ms,
        .max_work_units = max_work_units,
    });
}

pub fn runTableEmptyingWorkerPassForCatalogWithAdmission(
    alloc: std.mem.Allocator,
    source: anytype,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
    worker_id: []const u8,
    policy: SchemaJobWorkerAdmissionPolicy,
) !TableEmptyingWorkerPassResult {
    policy.validate(worker_id) catch return error.InvalidTableEmptyingJobLease;
    var snapshot = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&snapshot);
    const table = tables_api.findTableByName(&snapshot, table_name) orelse return .{};
    return try runTableEmptyingWorkerPassForSnapshotTable(alloc, source, snapshot.table_emptying_jobs, table.*, worker_id, policy);
}

pub fn runTableEmptyingWorkerPassForCatalogTableId(
    alloc: std.mem.Allocator,
    source: anytype,
    catalog: table_catalog.CatalogSource,
    table_id: u64,
    table_name: []const u8,
    worker_id: []const u8,
    lease_ms: u64,
    max_work_units: usize,
) !TableEmptyingWorkerPassResult {
    return try runTableEmptyingWorkerPassForCatalogTableIdWithAdmission(alloc, source, catalog, table_id, table_name, worker_id, .{
        .lease_ms = lease_ms,
        .max_work_units = max_work_units,
    });
}

pub fn runTableEmptyingWorkerPassForCatalogTableIdWithAdmission(
    alloc: std.mem.Allocator,
    source: anytype,
    catalog: table_catalog.CatalogSource,
    table_id: u64,
    table_name: []const u8,
    worker_id: []const u8,
    policy: SchemaJobWorkerAdmissionPolicy,
) !TableEmptyingWorkerPassResult {
    _ = table_name;
    policy.validate(worker_id) catch return error.InvalidTableEmptyingJobLease;
    var snapshot = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&snapshot);
    const table = findTableRecordById(&snapshot, table_id) orelse return .{};
    return try runTableEmptyingWorkerPassForSnapshotTable(alloc, source, snapshot.table_emptying_jobs, table, worker_id, policy);
}

fn runTableEmptyingWorkerPassForSnapshotTable(
    alloc: std.mem.Allocator,
    source: anytype,
    table_emptying_jobs: []const metadata_table_manager.TableEmptyingJobRecord,
    table: metadata_table_manager.TableRecord,
    worker_id: []const u8,
    policy: SchemaJobWorkerAdmissionPolicy,
) !TableEmptyingWorkerPassResult {
    var groups = std.ArrayListUnmanaged(TableEmptyingWorkerResult).empty;
    errdefer {
        for (groups.items) |*group| group.deinit(alloc);
        groups.deinit(alloc);
    }

    var result: TableEmptyingWorkerPassResult = .{};
    for (table_emptying_jobs) |record| {
        if (record.table_id != table.table_id) continue;
        if (!tableEmptyingRecordAdmitted(record, policy)) continue;
        result.jobs_scanned += 1;
        if (result.jobs_claimed >= policy.max_work_units) {
            result.complete = false;
            continue;
        }

        var one = (try source.tableEmptyingGroupLocal(
            alloc,
            record.group_id,
            table.name,
            record,
            worker_id,
            policy.lease_ms,
        )) orelse {
            result.complete = false;
            continue;
        };
        var one_transferred = false;
        errdefer if (!one_transferred) one.deinit(alloc);
        if (!one.claimed) {
            if (one.completed) {
                result.jobs_completed += 1;
                result.rows_matched += one.result.matched;
                result.rows_staged += one.result.staged;
            } else if (one.invalidated) {
                result.jobs_invalidated += 1;
                result.complete = false;
            } else {
                result.jobs_busy += 1;
                result.complete = false;
            }
            try groups.append(alloc, one);
            one_transferred = true;
            continue;
        }

        result.jobs_claimed += 1;
        if (one.completed) {
            result.jobs_completed += 1;
            result.rows_matched += one.result.matched;
            result.rows_staged += one.result.staged;
        } else if (one.invalidated) {
            result.jobs_invalidated += 1;
            result.complete = false;
        } else {
            result.complete = false;
        }
        try groups.append(alloc, one);
        one_transferred = true;
    }

    result.groups = try groups.toOwnedSlice(alloc);
    return result;
}

test "secondary index rebuild worker helper claims repairs and finishes range" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/secondary-index-rebuild-worker", .{tmp.sub_path});
    defer alloc.free(path);

    var db = try db_mod.DB.open(alloc, path, .{});
    defer db.close();

    const building_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"amount":{"type":"numeric","x-antfly-index-lifecycle":"building","x-antfly-index-generation":9,"x-antfly-index-where":{"all":[{"field":"status","op":"eq","value":"active"}]}},"status":{"type":"keyword"}},"required":["id","amount","status"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    try db.applyTableSchemaJson(alloc, building_schema_json, .{});
    try db.batch(.{ .writes = &.{
        .{ .key = "row:active", .value = "{\"id\":\"active\",\"amount\":1,\"status\":\"active\"}" },
        .{ .key = "row:inactive", .value = "{\"id\":\"inactive\",\"amount\":2,\"status\":\"inactive\"}" },
    } });

    const inactive_amount_key = try db_mod.internal_keys.relationalColumnIndexKeyAlloc(alloc, "amount", "row:inactive");
    defer alloc.free(inactive_amount_key);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, inactive_amount_key));
    try db.core.store.put(inactive_amount_key, "");
    const stale_inactive_amount = try db.core.store.get(alloc, inactive_amount_key);
    defer alloc.free(stale_inactive_amount);

    var manager = metadata_table_manager.TableManager.init(alloc);
    defer manager.deinit();
    try manager.upsertTable(.{ .table_id = 77, .name = "orders", .schema_json = building_schema_json });
    try manager.upsertSecondaryIndexRebuildRange(.{
        .table_id = 77,
        .index_name = "amount",
        .index_generation = 9,
        .start_row_key = "",
        .end_row_key = null,
        .group_id = 9001,
    });
    const records = try manager.listSecondaryIndexRebuildRanges(alloc);
    defer manager.freeSecondaryIndexRebuildRanges(alloc, records);
    try std.testing.expectEqual(@as(usize, 1), records.len);

    const first = try runSecondaryIndexRebuildRangeGroupLocal(&db, &manager, records[0], "worker-a", 1000, 500);
    try std.testing.expect(first.claimed);
    try std.testing.expect(first.completed);
    try std.testing.expect(!first.invalidated);
    try std.testing.expectEqual(@as(u64, 2), first.report.scanned_rows);
    try std.testing.expectEqual(@as(u64, 1), first.report.indexed_rows);

    const final_records = try manager.listSecondaryIndexRebuildRanges(alloc);
    defer manager.freeSecondaryIndexRebuildRanges(alloc, final_records);
    try std.testing.expectEqualStrings(metadata_table_manager.secondary_index_rebuild_ready, final_records[0].state);
    try std.testing.expectEqual(@as(u64, 2), final_records[0].completed_row_count);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, inactive_amount_key));
}

test "secondary index rebuild worker rebuilds and promotes expression generated index" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/secondary-index-expression-rebuild-worker", .{tmp.sub_path});
    defer alloc.free(path);

    var db = try db_mod.DB.open(alloc, path, .{});
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"},"users_lower_email_idx":{"type":"keyword","generated":{"op":"lower","field":"email"},"x-antfly-index":true,"x-antfly-index-lifecycle":"building","x-antfly-index-generation":9,"x-antfly-index-name":"users_lower_email_idx"}},"required":["id","email"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    try db.applyTableSchemaJson(alloc, schema_json, .{});
    var parsed_schema = try schema_mod.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try schema_mod.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer storage_schema.freeSchema(alloc, runtime_schema);
    var rows_batch = try relational_rows_api.parseRowsBatchRequest(
        alloc,
        "{\"operations\":[{\"op\":\"insert\",\"row\":{\"id\":\"a\",\"email\":\"Ada@Example.Test\"}},{\"op\":\"insert\",\"row\":{\"id\":\"b\",\"email\":\"Grace@Example.Test\"}}]}",
        runtime_schema,
    );
    defer rows_batch.deinit(alloc);
    const row_a_key = try alloc.dupe(u8, rows_batch.req.writes[0].key);
    defer alloc.free(row_a_key);
    try db.batch(rows_batch.req);

    const valid_generated_key = try db_mod.internal_keys.relationalColumnIndexKeyAlloc(alloc, "users_lower_email_idx", row_a_key);
    defer alloc.free(valid_generated_key);
    const stale_generated_key = try db_mod.internal_keys.relationalColumnIndexKeyAlloc(alloc, "users_lower_email_idx", "row:stale");
    defer alloc.free(stale_generated_key);
    try db.core.store.put(stale_generated_key, "");
    const stale_before = try db.core.store.get(alloc, stale_generated_key);
    defer alloc.free(stale_before);

    var manager = metadata_table_manager.TableManager.init(alloc);
    defer manager.deinit();
    const table = metadata_table_manager.TableRecord{
        .table_id = 77,
        .name = "users",
        .schema_json = schema_json,
    };
    const range = metadata_table_manager.RangeRecord{
        .group_id = 9001,
        .range_id = 9101,
        .table_id = table.table_id,
        .start_key = "",
        .end_key = null,
    };
    try manager.upsertTable(table);
    try manager.upsertSecondaryIndexRebuildRange(.{
        .table_id = table.table_id,
        .index_name = "users_lower_email_idx",
        .index_generation = 9,
        .start_row_key = range.start_key,
        .end_row_key = range.end_key,
        .group_id = range.group_id,
        .range_id = 0,
    });

    const records = try manager.listSecondaryIndexRebuildRanges(alloc);
    defer manager.freeSecondaryIndexRebuildRanges(alloc, records);
    try std.testing.expectEqual(@as(usize, 1), records.len);

    const rebuilt = try runSecondaryIndexRebuildRangeGroupLocal(&db, &manager, records[0], "worker-a", 1000, 500);
    try std.testing.expect(rebuilt.claimed);
    try std.testing.expect(rebuilt.completed);
    try std.testing.expect(!rebuilt.invalidated);
    try std.testing.expectEqual(@as(u64, 2), rebuilt.report.scanned_rows);
    try std.testing.expectEqual(@as(u64, 2), rebuilt.report.indexed_rows);
    try std.testing.expect(rebuilt.report.written_entries >= 2);
    try std.testing.expect(rebuilt.report.deleted_entries >= 1);

    const valid_after = try db.core.store.get(alloc, valid_generated_key);
    defer alloc.free(valid_after);
    try std.testing.expect(valid_after.len > 0);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, stale_generated_key));

    const final_records = try manager.listSecondaryIndexRebuildRanges(alloc);
    defer manager.freeSecondaryIndexRebuildRanges(alloc, final_records);
    try std.testing.expectEqual(@as(usize, 1), final_records.len);
    try std.testing.expectEqualStrings(metadata_table_manager.secondary_index_rebuild_ready, final_records[0].state);

    const Catalog = struct {
        table: metadata_table_manager.TableRecord,
        range: metadata_table_manager.RangeRecord,
        rebuild: metadata_table_manager.SecondaryIndexRebuildRangeRecord,
        promoted: bool = false,

        fn iface(self: *@This()) table_catalog.CatalogSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                    .promote_secondary_index_ready = promoteSecondaryIndexReady,
                },
            };
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @as([*]metadata_table_manager.TableRecord, @ptrCast(&self.table))[0..1],
                .ranges = @as([*]metadata_table_manager.RangeRecord, @ptrCast(&self.range))[0..1],
                .secondary_index_rebuild_ranges = @as([*]metadata_table_manager.SecondaryIndexRebuildRangeRecord, @ptrCast(&self.rebuild))[0..1],
                .stores = &.{},
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}

        fn promoteSecondaryIndexReady(
            ptr: *anyopaque,
            _: std.mem.Allocator,
            table_name: []const u8,
            index_name: []const u8,
            expected_generation: u64,
        ) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("users", table_name);
            try std.testing.expectEqualStrings("users_lower_email_idx", index_name);
            try std.testing.expectEqual(@as(u64, 9), expected_generation);
            self.promoted = true;
            return true;
        }
    };

    var catalog = Catalog{
        .table = table,
        .range = range,
        .rebuild = final_records[0],
    };
    try std.testing.expectEqual(@as(u64, 1), try promoteReadySecondaryIndexesForCatalog(alloc, catalog.iface(), "users"));
    try std.testing.expect(catalog.promoted);
}

test "secondary index rebuild worker invalidates stale range before rebuilding index" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/secondary-index-rebuild-stale-range-worker", .{tmp.sub_path});
    defer alloc.free(path);

    var db = try db_mod.DB.open(alloc, path, .{});
    defer db.close();

    const building_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"amount":{"type":"numeric","x-antfly-index-lifecycle":"building","x-antfly-index-generation":9,"x-antfly-index-where":{"all":[{"field":"status","op":"eq","value":"active"}]}},"status":{"type":"keyword"}},"required":["id","amount","status"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    try db.applyTableSchemaJson(alloc, building_schema_json, .{});
    try db.batch(.{ .writes = &.{
        .{ .key = "row:active", .value = "{\"id\":\"active\",\"amount\":1,\"status\":\"active\"}" },
        .{ .key = "row:inactive", .value = "{\"id\":\"inactive\",\"amount\":2,\"status\":\"inactive\"}" },
    } });

    const inactive_amount_key = try db_mod.internal_keys.relationalColumnIndexKeyAlloc(alloc, "amount", "row:inactive");
    defer alloc.free(inactive_amount_key);
    try db.core.store.put(inactive_amount_key, "");
    const stale_inactive_amount = try db.core.store.get(alloc, inactive_amount_key);
    defer alloc.free(stale_inactive_amount);

    var manager = metadata_table_manager.TableManager.init(alloc);
    defer manager.deinit();
    const table = metadata_table_manager.TableRecord{
        .table_id = 77,
        .name = "orders",
        .schema_json = building_schema_json,
    };
    try manager.upsertTable(table);
    const stale_range = metadata_table_manager.RangeRecord{
        .group_id = 9001,
        .range_id = 9101,
        .table_id = 77,
        .start_key = "",
        .end_key = null,
    };
    const rebuild = metadata_table_manager.SecondaryIndexRebuildRangeRecord{
        .table_id = 77,
        .index_name = "amount",
        .index_generation = 9,
        .start_row_key = stale_range.start_key,
        .end_row_key = stale_range.end_key,
        .group_id = stale_range.group_id,
        .range_id = stale_range.range_id,
    };
    try manager.upsertSecondaryIndexRebuildRange(rebuild);

    const Catalog = struct {
        manager: *metadata_table_manager.TableManager,
        table: metadata_table_manager.TableRecord,
        current_range: metadata_table_manager.RangeRecord,

        fn iface(self: *@This()) table_catalog.CatalogSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                    .begin_secondary_index_rebuild_range = beginSecondaryIndexRebuildRange,
                    .finish_secondary_index_rebuild_range = finishSecondaryIndexRebuildRange,
                    .invalidate_secondary_index_rebuild_range = invalidateSecondaryIndexRebuildRange,
                },
            };
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @as([*]metadata_table_manager.TableRecord, @ptrCast(&self.table))[0..1],
                .ranges = @as([*]metadata_table_manager.RangeRecord, @ptrCast(&self.current_range))[0..1],
                .stores = &.{},
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}

        fn beginSecondaryIndexRebuildRange(ptr: *anyopaque, request: metadata_table_manager.SecondaryIndexRebuildRangeBeginRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try self.manager.beginSecondaryIndexRebuildRange(request);
        }

        fn finishSecondaryIndexRebuildRange(ptr: *anyopaque, request: metadata_table_manager.SecondaryIndexRebuildRangeFinishRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try self.manager.finishSecondaryIndexRebuildRange(request);
        }

        fn invalidateSecondaryIndexRebuildRange(ptr: *anyopaque, request: metadata_table_manager.SecondaryIndexRebuildRangeInvalidateRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try self.manager.invalidateSecondaryIndexRebuildRange(request);
        }
    };

    const records = try manager.listSecondaryIndexRebuildRanges(alloc);
    defer manager.freeSecondaryIndexRebuildRanges(alloc, records);
    try std.testing.expectEqual(@as(usize, 1), records.len);

    var catalog = Catalog{
        .manager = &manager,
        .table = table,
        .current_range = .{
            .group_id = stale_range.group_id,
            .range_id = stale_range.range_id + 1,
            .table_id = stale_range.table_id,
            .start_key = stale_range.start_key,
            .end_key = stale_range.end_key,
        },
    };
    try std.testing.expectError(
        error.TopologyChanged,
        runSecondaryIndexRebuildRangeGroupLocal(&db, catalog.iface(), records[0], "worker-a", 1000, 500),
    );

    const still_stale = try db.core.store.get(alloc, inactive_amount_key);
    defer alloc.free(still_stale);

    const final_records = try manager.listSecondaryIndexRebuildRanges(alloc);
    defer manager.freeSecondaryIndexRebuildRanges(alloc, final_records);
    try std.testing.expectEqual(@as(usize, 1), final_records.len);
    try std.testing.expectEqualStrings(metadata_table_manager.secondary_index_rebuild_invalid, final_records[0].state);
    try std.testing.expectEqualStrings(@errorName(error.TopologyChanged), final_records[0].last_error);
}

test "table emptying worker helper claims deletes rows and finishes job" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/table-emptying-worker", .{tmp.sub_path});
    defer alloc.free(path);

    var db = try db_mod.DB.open(alloc, path, .{});
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    try db.applyTableSchemaJson(alloc, schema_json, .{});
    try db.batch(.{ .writes = &.{
        .{ .key = "row:a", .value = "{\"id\":\"a\",\"status\":\"open\"}" },
        .{ .key = "row:b", .value = "{\"id\":\"b\",\"status\":\"closed\"}" },
    } });

    var manager = metadata_table_manager.TableManager.init(alloc);
    defer manager.deinit();
    const table = metadata_table_manager.TableRecord{
        .table_id = 77,
        .name = "orders",
        .schema_json = schema_json,
    };
    try manager.upsertTable(table);
    var job = metadata_table_manager.TableEmptyingJobRecord{
        .job_id = 0,
        .table_id = 77,
        .group_id = 9001,
        .schema_generation = metadata_table_manager.schemaRewriteGenerationForSchemaJson(schema_json),
        .start_row_key = "",
        .end_row_key = null,
        .affected_table_ids = &.{77},
    };
    job.job_id = metadata_table_manager.stableTableEmptyingJobId(job);
    try manager.upsertTableEmptyingJob(job);

    const Catalog = struct {
        manager: *metadata_table_manager.TableManager,

        fn iface(self: *@This()) table_catalog.CatalogSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                    .begin_table_emptying_job = beginTableEmptyingJob,
                    .finish_table_emptying_job = finishTableEmptyingJob,
                    .invalidate_table_emptying_job = invalidateTableEmptyingJob,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = &.{},
                .ranges = &.{},
                .stores = &.{},
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}

        fn beginTableEmptyingJob(ptr: *anyopaque, request: metadata_table_manager.TableEmptyingJobBeginRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try self.manager.beginTableEmptyingJob(request);
        }

        fn finishTableEmptyingJob(ptr: *anyopaque, request: metadata_table_manager.TableEmptyingJobFinishRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try self.manager.finishTableEmptyingJob(request);
        }

        fn invalidateTableEmptyingJob(ptr: *anyopaque, request: metadata_table_manager.TableEmptyingJobInvalidateRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try self.manager.invalidateTableEmptyingJob(request);
        }
    };

    const records = try manager.listTableEmptyingJobs(alloc);
    defer manager.freeTableEmptyingJobs(alloc, records);
    try std.testing.expectEqual(@as(usize, 1), records.len);

    var catalog = Catalog{ .manager = &manager };
    var result = try runTableEmptyingJobGroupLocal(alloc, &db, catalog.iface(), table, records[0], "worker-a", 1000, 500);
    defer result.deinit(alloc);
    try std.testing.expect(result.claimed);
    try std.testing.expect(result.completed);
    try std.testing.expect(!result.invalidated);
    try std.testing.expectEqual(@as(u32, 2), result.result.matched);
    try std.testing.expectEqual(@as(u32, 2), result.result.staged);

    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, "row:a"));
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, "row:b"));

    const final_jobs = try manager.listTableEmptyingJobs(alloc);
    defer manager.freeTableEmptyingJobs(alloc, final_jobs);
    try std.testing.expectEqual(@as(usize, 1), final_jobs.len);
    try std.testing.expectEqualStrings(metadata_table_manager.table_emptying_ready, final_jobs[0].state);
    try std.testing.expectEqual(@as(u64, 2), final_jobs[0].completed_row_count);
}

test "table emptying worker helper deletes populated and empty document tables" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/document-table-emptying-worker", .{tmp.sub_path});
    defer alloc.free(path);

    var db = try db_mod.DB.open(alloc, path, .{});
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"document","default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","properties":{"title":{"type":"text"},"status":{"type":"keyword"}},"additionalProperties":true}}}}
    ;
    try db.applyTableSchemaJson(alloc, schema_json, .{});
    try db.batch(.{ .writes = &.{
        .{ .key = "doc:truncate-a", .value = "{\"title\":\"alpha\",\"status\":\"drop\"}" },
        .{ .key = "doc:truncate-b", .value = "{\"title\":\"beta\",\"status\":\"drop\"}" },
    } });

    var manager = metadata_table_manager.TableManager.init(alloc);
    defer manager.deinit();
    const table = metadata_table_manager.TableRecord{
        .table_id = 77,
        .name = "docs",
        .schema_json = schema_json,
    };
    try manager.upsertTable(table);

    const Catalog = struct {
        manager: *metadata_table_manager.TableManager,

        fn iface(self: *@This()) table_catalog.CatalogSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                    .begin_table_emptying_job = beginTableEmptyingJob,
                    .finish_table_emptying_job = finishTableEmptyingJob,
                    .invalidate_table_emptying_job = invalidateTableEmptyingJob,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = &.{},
                .ranges = &.{},
                .stores = &.{},
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}

        fn beginTableEmptyingJob(ptr: *anyopaque, request: metadata_table_manager.TableEmptyingJobBeginRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try self.manager.beginTableEmptyingJob(request);
        }

        fn finishTableEmptyingJob(ptr: *anyopaque, request: metadata_table_manager.TableEmptyingJobFinishRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try self.manager.finishTableEmptyingJob(request);
        }

        fn invalidateTableEmptyingJob(ptr: *anyopaque, request: metadata_table_manager.TableEmptyingJobInvalidateRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try self.manager.invalidateTableEmptyingJob(request);
        }
    };

    var populated_job = metadata_table_manager.TableEmptyingJobRecord{
        .job_id = 0,
        .table_id = 77,
        .group_id = 9001,
        .schema_generation = metadata_table_manager.schemaRewriteGenerationForSchemaJson(schema_json),
        .start_row_key = "",
        .end_row_key = null,
        .affected_table_ids = &.{77},
    };
    populated_job.job_id = metadata_table_manager.stableTableEmptyingJobId(populated_job);
    try manager.upsertTableEmptyingJob(populated_job);

    var catalog = Catalog{ .manager = &manager };
    const populated_records = try manager.listTableEmptyingJobs(alloc);
    defer manager.freeTableEmptyingJobs(alloc, populated_records);
    try std.testing.expectEqual(@as(usize, 1), populated_records.len);

    var populated_result = try runTableEmptyingJobGroupLocal(alloc, &db, catalog.iface(), table, populated_records[0], "worker-docs-a", 1000, 500);
    defer populated_result.deinit(alloc);
    try std.testing.expect(populated_result.claimed);
    try std.testing.expect(populated_result.completed);
    try std.testing.expect(!populated_result.invalidated);
    try std.testing.expectEqual(@as(u32, 2), populated_result.result.matched);
    try std.testing.expectEqual(@as(u32, 2), populated_result.result.staged);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, "doc:truncate-a"));
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, "doc:truncate-b"));

    var empty_job = metadata_table_manager.TableEmptyingJobRecord{
        .job_id = 0,
        .table_id = 77,
        .group_id = 9002,
        .schema_generation = metadata_table_manager.schemaRewriteGenerationForSchemaJson(schema_json),
        .start_row_key = "",
        .end_row_key = null,
        .affected_table_ids = &.{77},
    };
    empty_job.job_id = metadata_table_manager.stableTableEmptyingJobId(empty_job);
    try manager.upsertTableEmptyingJob(empty_job);

    const empty_records = try manager.listTableEmptyingJobs(alloc);
    defer manager.freeTableEmptyingJobs(alloc, empty_records);
    var empty_record: ?metadata_table_manager.TableEmptyingJobRecord = null;
    for (empty_records) |record| {
        if (record.job_id == empty_job.job_id) {
            empty_record = record;
            break;
        }
    }
    var empty_result = try runTableEmptyingJobGroupLocal(alloc, &db, catalog.iface(), table, empty_record orelse return error.TestUnexpectedResult, "worker-docs-b", 2000, 500);
    defer empty_result.deinit(alloc);
    try std.testing.expect(empty_result.claimed);
    try std.testing.expect(empty_result.completed);
    try std.testing.expect(!empty_result.invalidated);
    try std.testing.expectEqual(@as(u32, 0), empty_result.result.matched);
    try std.testing.expectEqual(@as(u32, 0), empty_result.result.staged);

    const large_count: usize = 96;
    const large_writes = try alloc.alloc(db_mod.types.BatchWrite, large_count);
    defer alloc.free(large_writes);
    for (large_writes, 0..) |*write, i| {
        write.* = .{
            .key = try std.fmt.allocPrint(alloc, "doc:large-{d:0>3}", .{i}),
            .value = try std.fmt.allocPrint(alloc, "{{\"title\":\"large {d}\",\"status\":\"drop\"}}", .{i}),
        };
    }
    defer for (large_writes) |write| {
        alloc.free(@constCast(write.key));
        alloc.free(@constCast(write.value));
    };
    try db.batch(.{ .writes = large_writes });

    var large_job = metadata_table_manager.TableEmptyingJobRecord{
        .job_id = 0,
        .table_id = 77,
        .group_id = 9003,
        .schema_generation = metadata_table_manager.schemaRewriteGenerationForSchemaJson(schema_json),
        .start_row_key = "doc:large-",
        .end_row_key = "doc:large;",
        .affected_table_ids = &.{77},
    };
    large_job.job_id = metadata_table_manager.stableTableEmptyingJobId(large_job);
    try manager.upsertTableEmptyingJob(large_job);

    const large_records = try manager.listTableEmptyingJobs(alloc);
    defer manager.freeTableEmptyingJobs(alloc, large_records);
    var large_record: ?metadata_table_manager.TableEmptyingJobRecord = null;
    for (large_records) |record| {
        if (record.job_id == large_job.job_id) {
            large_record = record;
            break;
        }
    }
    var large_result = try runTableEmptyingJobGroupLocal(alloc, &db, catalog.iface(), table, large_record orelse return error.TestUnexpectedResult, "worker-docs-c", 3000, 500);
    defer large_result.deinit(alloc);
    try std.testing.expect(large_result.claimed);
    try std.testing.expect(large_result.completed);
    try std.testing.expect(!large_result.invalidated);
    try std.testing.expectEqual(@as(u32, @intCast(large_count)), large_result.result.matched);
    try std.testing.expectEqual(@as(u32, @intCast(large_count)), large_result.result.staged);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, "doc:large-000"));
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, "doc:large-095"));

    const final_jobs = try manager.listTableEmptyingJobs(alloc);
    defer manager.freeTableEmptyingJobs(alloc, final_jobs);
    for (final_jobs) |record| {
        if (record.job_id == populated_job.job_id) {
            try std.testing.expectEqualStrings(metadata_table_manager.table_emptying_ready, record.state);
            try std.testing.expectEqual(@as(u64, 2), record.completed_row_count);
        } else if (record.job_id == empty_job.job_id) {
            try std.testing.expectEqualStrings(metadata_table_manager.table_emptying_ready, record.state);
            try std.testing.expectEqual(@as(u64, 0), record.completed_row_count);
        } else if (record.job_id == large_job.job_id) {
            try std.testing.expectEqualStrings(metadata_table_manager.table_emptying_ready, record.state);
            try std.testing.expectEqual(@as(u64, @intCast(large_count)), record.completed_row_count);
        }
    }
}

test "table emptying worker live lease blocks competing worker without deleting rows" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/table-emptying-worker-busy", .{tmp.sub_path});
    defer alloc.free(path);

    var db = try db_mod.DB.open(alloc, path, .{});
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    try db.applyTableSchemaJson(alloc, schema_json, .{});
    try db.batch(.{ .writes = &.{
        .{ .key = "row:a", .value = "{\"id\":\"a\",\"status\":\"open\"}" },
    } });

    var manager = metadata_table_manager.TableManager.init(alloc);
    defer manager.deinit();
    const table = metadata_table_manager.TableRecord{
        .table_id = 77,
        .name = "orders",
        .schema_json = schema_json,
    };
    try manager.upsertTable(table);
    var job = metadata_table_manager.TableEmptyingJobRecord{
        .job_id = 0,
        .table_id = 77,
        .group_id = 9001,
        .schema_generation = metadata_table_manager.schemaRewriteGenerationForSchemaJson(schema_json),
        .start_row_key = "",
        .end_row_key = null,
        .affected_table_ids = &.{77},
    };
    job.job_id = metadata_table_manager.stableTableEmptyingJobId(job);
    try manager.upsertTableEmptyingJob(job);
    try manager.beginTableEmptyingJob(.{
        .job_id = job.job_id,
        .lease_owner = "worker-a",
        .now_ms = 1000,
        .lease_expires_at_ms = 2000,
    });

    const Catalog = struct {
        manager: *metadata_table_manager.TableManager,

        fn iface(self: *@This()) table_catalog.CatalogSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                    .begin_table_emptying_job = beginTableEmptyingJob,
                    .finish_table_emptying_job = finishTableEmptyingJob,
                    .invalidate_table_emptying_job = invalidateTableEmptyingJob,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = &.{},
                .ranges = &.{},
                .stores = &.{},
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}

        fn beginTableEmptyingJob(ptr: *anyopaque, request: metadata_table_manager.TableEmptyingJobBeginRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try self.manager.beginTableEmptyingJob(request);
        }

        fn finishTableEmptyingJob(ptr: *anyopaque, request: metadata_table_manager.TableEmptyingJobFinishRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try self.manager.finishTableEmptyingJob(request);
        }

        fn invalidateTableEmptyingJob(ptr: *anyopaque, request: metadata_table_manager.TableEmptyingJobInvalidateRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try self.manager.invalidateTableEmptyingJob(request);
        }
    };

    const records = try manager.listTableEmptyingJobs(alloc);
    defer manager.freeTableEmptyingJobs(alloc, records);
    try std.testing.expectEqual(@as(usize, 1), records.len);
    try std.testing.expectEqualStrings(metadata_table_manager.table_emptying_running, records[0].state);

    var catalog = Catalog{ .manager = &manager };
    var result = try runTableEmptyingJobGroupLocal(alloc, &db, catalog.iface(), table, records[0], "worker-b", 1200, 500);
    defer result.deinit(alloc);
    try std.testing.expect(!result.claimed);
    try std.testing.expect(!result.completed);
    try std.testing.expect(!result.invalidated);
    try std.testing.expectEqual(@as(u32, 0), result.result.matched);
    try std.testing.expectEqual(@as(u32, 0), result.result.staged);

    const row = (try db.get(alloc, "row:a")) orelse return error.TestUnexpectedResult;
    defer alloc.free(row);
    try std.testing.expect(std.mem.indexOf(u8, row, "\"status\":\"open\"") != null);

    const final_jobs = try manager.listTableEmptyingJobs(alloc);
    defer manager.freeTableEmptyingJobs(alloc, final_jobs);
    try std.testing.expectEqual(@as(usize, 1), final_jobs.len);
    try std.testing.expectEqualStrings(metadata_table_manager.table_emptying_running, final_jobs[0].state);
    try std.testing.expectEqualStrings("worker-a", final_jobs[0].lease_owner);
    try std.testing.expectEqual(@as(u64, 2000), final_jobs[0].lease_expires_at_ms);
    try std.testing.expectEqual(@as(u32, 1), final_jobs[0].attempts);
}

test "table emptying worker stale lease lets next worker delete rows and finish job" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/table-emptying-worker-takeover", .{tmp.sub_path});
    defer alloc.free(path);

    var db = try db_mod.DB.open(alloc, path, .{});
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    try db.applyTableSchemaJson(alloc, schema_json, .{});
    try db.batch(.{ .writes = &.{
        .{ .key = "row:a", .value = "{\"id\":\"a\",\"status\":\"open\"}" },
    } });

    var manager = metadata_table_manager.TableManager.init(alloc);
    defer manager.deinit();
    const table = metadata_table_manager.TableRecord{
        .table_id = 77,
        .name = "orders",
        .schema_json = schema_json,
    };
    try manager.upsertTable(table);
    var job = metadata_table_manager.TableEmptyingJobRecord{
        .job_id = 0,
        .table_id = 77,
        .group_id = 9001,
        .schema_generation = metadata_table_manager.schemaRewriteGenerationForSchemaJson(schema_json),
        .start_row_key = "",
        .end_row_key = null,
        .affected_table_ids = &.{77},
    };
    job.job_id = metadata_table_manager.stableTableEmptyingJobId(job);
    try manager.upsertTableEmptyingJob(job);
    try manager.beginTableEmptyingJob(.{
        .job_id = job.job_id,
        .lease_owner = "worker-a",
        .now_ms = 1000,
        .lease_expires_at_ms = 2000,
    });

    const Catalog = struct {
        manager: *metadata_table_manager.TableManager,

        fn iface(self: *@This()) table_catalog.CatalogSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                    .begin_table_emptying_job = beginTableEmptyingJob,
                    .finish_table_emptying_job = finishTableEmptyingJob,
                    .invalidate_table_emptying_job = invalidateTableEmptyingJob,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = &.{},
                .ranges = &.{},
                .stores = &.{},
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}

        fn beginTableEmptyingJob(ptr: *anyopaque, request: metadata_table_manager.TableEmptyingJobBeginRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try self.manager.beginTableEmptyingJob(request);
        }

        fn finishTableEmptyingJob(ptr: *anyopaque, request: metadata_table_manager.TableEmptyingJobFinishRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try self.manager.finishTableEmptyingJob(request);
        }

        fn invalidateTableEmptyingJob(ptr: *anyopaque, request: metadata_table_manager.TableEmptyingJobInvalidateRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try self.manager.invalidateTableEmptyingJob(request);
        }
    };

    const running_jobs = try manager.listTableEmptyingJobs(alloc);
    defer manager.freeTableEmptyingJobs(alloc, running_jobs);
    try std.testing.expectEqual(@as(usize, 1), running_jobs.len);
    try std.testing.expectEqualStrings(metadata_table_manager.table_emptying_running, running_jobs[0].state);
    try std.testing.expectEqualStrings("worker-a", running_jobs[0].lease_owner);
    try std.testing.expectEqual(@as(u32, 1), running_jobs[0].attempts);

    var catalog = Catalog{ .manager = &manager };
    var result = try runTableEmptyingJobGroupLocal(alloc, &db, catalog.iface(), table, running_jobs[0], "worker-b", 2000, 500);
    defer result.deinit(alloc);
    try std.testing.expect(result.claimed);
    try std.testing.expect(result.completed);
    try std.testing.expect(!result.invalidated);
    try std.testing.expectEqual(@as(u32, 1), result.result.matched);
    try std.testing.expectEqual(@as(u32, 1), result.result.staged);

    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, "row:a"));

    const final_jobs = try manager.listTableEmptyingJobs(alloc);
    defer manager.freeTableEmptyingJobs(alloc, final_jobs);
    try std.testing.expectEqual(@as(usize, 1), final_jobs.len);
    try std.testing.expectEqualStrings(metadata_table_manager.table_emptying_ready, final_jobs[0].state);
    try std.testing.expectEqualStrings("", final_jobs[0].lease_owner);
    try std.testing.expectEqual(@as(u64, 0), final_jobs[0].lease_expires_at_ms);
    try std.testing.expectEqual(@as(u32, 2), final_jobs[0].attempts);
    try std.testing.expectEqual(@as(u64, 1), final_jobs[0].completed_row_count);
}

test "table emptying worker leaves job running when concurrent row claim skips rows" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/table-emptying-worker-row-claim", .{tmp.sub_path});
    defer alloc.free(path);

    var db = try db_mod.DB.open(alloc, path, .{});
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    try db.applyTableSchemaJson(alloc, schema_json, .{});
    try db.batch(.{ .writes = &.{
        .{ .key = "row:a", .value = "{\"id\":\"a\",\"status\":\"open\"}" },
        .{ .key = "row:b", .value = "{\"id\":\"b\",\"status\":\"open\"}" },
    } });

    const locker_txn = try db.beginTransaction(1_000);
    try db.claimRowsForTransaction(locker_txn, &.{"row:a"}, .{
        .mode = .for_update,
        .owner_id = "writer:row-a",
        .txn_id = locker_txn,
    });
    defer db.abortTransaction(locker_txn, 1_500) catch {};

    var manager = metadata_table_manager.TableManager.init(alloc);
    defer manager.deinit();
    const table = metadata_table_manager.TableRecord{
        .table_id = 77,
        .name = "orders",
        .schema_json = schema_json,
    };
    try manager.upsertTable(table);
    var job = metadata_table_manager.TableEmptyingJobRecord{
        .job_id = 0,
        .table_id = 77,
        .group_id = 9001,
        .schema_generation = metadata_table_manager.schemaRewriteGenerationForSchemaJson(schema_json),
        .start_row_key = "",
        .end_row_key = null,
        .affected_table_ids = &.{77},
    };
    job.job_id = metadata_table_manager.stableTableEmptyingJobId(job);
    try manager.upsertTableEmptyingJob(job);

    const Catalog = struct {
        manager: *metadata_table_manager.TableManager,

        fn iface(self: *@This()) table_catalog.CatalogSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                    .begin_table_emptying_job = beginTableEmptyingJob,
                    .finish_table_emptying_job = finishTableEmptyingJob,
                    .invalidate_table_emptying_job = invalidateTableEmptyingJob,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = &.{},
                .ranges = &.{},
                .stores = &.{},
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}

        fn beginTableEmptyingJob(ptr: *anyopaque, request: metadata_table_manager.TableEmptyingJobBeginRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try self.manager.beginTableEmptyingJob(request);
        }

        fn finishTableEmptyingJob(ptr: *anyopaque, request: metadata_table_manager.TableEmptyingJobFinishRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try self.manager.finishTableEmptyingJob(request);
        }

        fn invalidateTableEmptyingJob(ptr: *anyopaque, request: metadata_table_manager.TableEmptyingJobInvalidateRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try self.manager.invalidateTableEmptyingJob(request);
        }
    };

    var catalog = Catalog{ .manager = &manager };
    {
        const records = try manager.listTableEmptyingJobs(alloc);
        defer manager.freeTableEmptyingJobs(alloc, records);
        try std.testing.expectEqual(@as(usize, 1), records.len);

        var partial = try runTableEmptyingJobGroupLocal(alloc, &db, catalog.iface(), table, records[0], "worker-a", 1_100, 200);
        defer partial.deinit(alloc);
        try std.testing.expect(partial.claimed);
        try std.testing.expect(!partial.completed);
        try std.testing.expect(!partial.invalidated);
        try std.testing.expectEqual(@as(u32, 2), partial.result.matched);
        try std.testing.expectEqual(@as(u32, 1), partial.result.staged);
    }

    const locked = (try db.get(alloc, "row:a")) orelse return error.TestUnexpectedResult;
    defer alloc.free(locked);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, "row:b"));
    {
        const running_jobs = try manager.listTableEmptyingJobs(alloc);
        defer manager.freeTableEmptyingJobs(alloc, running_jobs);
        try std.testing.expectEqual(@as(usize, 1), running_jobs.len);
        try std.testing.expectEqualStrings(metadata_table_manager.table_emptying_running, running_jobs[0].state);
        try std.testing.expectEqualStrings("worker-a", running_jobs[0].lease_owner);
        try std.testing.expectEqual(@as(u64, 1300), running_jobs[0].lease_expires_at_ms);
        try std.testing.expectEqual(@as(u32, 1), running_jobs[0].attempts);
    }

    try db.abortTransaction(locker_txn, 1_500);
    {
        const running_jobs = try manager.listTableEmptyingJobs(alloc);
        defer manager.freeTableEmptyingJobs(alloc, running_jobs);
        var finished = try runTableEmptyingJobGroupLocal(alloc, &db, catalog.iface(), table, running_jobs[0], "worker-b", 1_300, 200);
        defer finished.deinit(alloc);
        try std.testing.expect(finished.claimed);
        try std.testing.expect(finished.completed);
        try std.testing.expect(!finished.invalidated);
        try std.testing.expectEqual(@as(u32, 1), finished.result.matched);
        try std.testing.expectEqual(@as(u32, 1), finished.result.staged);
    }

    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, "row:a"));
    const final_jobs = try manager.listTableEmptyingJobs(alloc);
    defer manager.freeTableEmptyingJobs(alloc, final_jobs);
    try std.testing.expectEqual(@as(usize, 1), final_jobs.len);
    try std.testing.expectEqualStrings(metadata_table_manager.table_emptying_ready, final_jobs[0].state);
    try std.testing.expectEqual(@as(u64, 1), final_jobs[0].completed_row_count);
}

test "table emptying worker invalidates malformed schema after claiming without deleting rows" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/table-emptying-worker-schema-abort", .{tmp.sub_path});
    defer alloc.free(path);

    var db = try db_mod.DB.open(alloc, path, .{});
    defer db.close();

    const storage_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    try db.applyTableSchemaJson(alloc, storage_schema_json, .{});
    try db.batch(.{ .writes = &.{
        .{ .key = "row:a", .value = "{\"id\":\"a\",\"status\":\"open\"}" },
    } });

    const malformed_schema_json = "{\"version\":1";
    var manager = metadata_table_manager.TableManager.init(alloc);
    defer manager.deinit();
    const table = metadata_table_manager.TableRecord{
        .table_id = 77,
        .name = "orders",
        .schema_json = malformed_schema_json,
    };
    try manager.upsertTable(table);
    var job = metadata_table_manager.TableEmptyingJobRecord{
        .job_id = 0,
        .table_id = 77,
        .group_id = 9001,
        .schema_generation = metadata_table_manager.schemaRewriteGenerationForSchemaJson(malformed_schema_json),
        .start_row_key = "",
        .end_row_key = null,
        .affected_table_ids = &.{77},
    };
    job.job_id = metadata_table_manager.stableTableEmptyingJobId(job);
    try manager.upsertTableEmptyingJob(job);

    const Catalog = struct {
        manager: *metadata_table_manager.TableManager,

        fn iface(self: *@This()) table_catalog.CatalogSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                    .begin_table_emptying_job = beginTableEmptyingJob,
                    .finish_table_emptying_job = finishTableEmptyingJob,
                    .invalidate_table_emptying_job = invalidateTableEmptyingJob,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = &.{},
                .ranges = &.{},
                .stores = &.{},
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}

        fn beginTableEmptyingJob(ptr: *anyopaque, request: metadata_table_manager.TableEmptyingJobBeginRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try self.manager.beginTableEmptyingJob(request);
        }

        fn finishTableEmptyingJob(ptr: *anyopaque, request: metadata_table_manager.TableEmptyingJobFinishRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try self.manager.finishTableEmptyingJob(request);
        }

        fn invalidateTableEmptyingJob(ptr: *anyopaque, request: metadata_table_manager.TableEmptyingJobInvalidateRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try self.manager.invalidateTableEmptyingJob(request);
        }
    };

    const records = try manager.listTableEmptyingJobs(alloc);
    defer manager.freeTableEmptyingJobs(alloc, records);
    try std.testing.expectEqual(@as(usize, 1), records.len);

    var catalog = Catalog{ .manager = &manager };
    try std.testing.expectError(
        error.UnexpectedEndOfInput,
        runTableEmptyingJobGroupLocal(alloc, &db, catalog.iface(), table, records[0], "worker-a", 1_000, 500),
    );

    const row = (try db.get(alloc, "row:a")) orelse return error.TestUnexpectedResult;
    defer alloc.free(row);

    const final_jobs = try manager.listTableEmptyingJobs(alloc);
    defer manager.freeTableEmptyingJobs(alloc, final_jobs);
    try std.testing.expectEqual(@as(usize, 1), final_jobs.len);
    try std.testing.expectEqualStrings(metadata_table_manager.table_emptying_invalid, final_jobs[0].state);
    try std.testing.expectEqualStrings("", final_jobs[0].lease_owner);
    try std.testing.expectEqual(@as(u64, 0), final_jobs[0].lease_expires_at_ms);
    try std.testing.expectEqualStrings(@errorName(error.UnexpectedEndOfInput), final_jobs[0].last_error);
}

test "table emptying worker invalidates stale range job before deleting rows" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/table-emptying-stale-topology-worker", .{tmp.sub_path});
    defer alloc.free(path);

    var db = try db_mod.DB.open(alloc, path, .{});
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    try db.applyTableSchemaJson(alloc, schema_json, .{});
    try db.batch(.{ .writes = &.{
        .{ .key = "row:a", .value = "{\"id\":\"a\",\"status\":\"open\"}" },
    } });

    var manager = metadata_table_manager.TableManager.init(alloc);
    defer manager.deinit();
    const table = metadata_table_manager.TableRecord{
        .table_id = 77,
        .name = "orders",
        .schema_json = schema_json,
    };
    try manager.upsertTable(table);
    const stale_range = metadata_table_manager.RangeRecord{
        .group_id = 9001,
        .range_id = 9101,
        .table_id = 77,
        .start_key = "",
        .end_key = null,
    };
    var job = metadata_table_manager.TableEmptyingJobRecord{
        .job_id = 0,
        .table_id = 77,
        .group_id = stale_range.group_id,
        .range_id = stale_range.range_id,
        .schema_generation = metadata_table_manager.schemaRewriteGenerationForSchemaJson(schema_json),
        .data_generation = table.data_generation,
        .start_row_key = stale_range.start_key,
        .end_row_key = stale_range.end_key,
        .affected_table_ids = &.{77},
    };
    job.job_id = metadata_table_manager.stableTableEmptyingJobId(job);
    try manager.upsertTableEmptyingJob(job);

    const Catalog = struct {
        manager: *metadata_table_manager.TableManager,
        table: metadata_table_manager.TableRecord,
        current_range: metadata_table_manager.RangeRecord,

        fn iface(self: *@This()) table_catalog.CatalogSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                    .begin_table_emptying_job = beginTableEmptyingJob,
                    .finish_table_emptying_job = finishTableEmptyingJob,
                    .invalidate_table_emptying_job = invalidateTableEmptyingJob,
                },
            };
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @as([*]metadata_table_manager.TableRecord, @ptrCast(&self.table))[0..1],
                .ranges = @as([*]metadata_table_manager.RangeRecord, @ptrCast(&self.current_range))[0..1],
                .stores = &.{},
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}

        fn beginTableEmptyingJob(ptr: *anyopaque, request: metadata_table_manager.TableEmptyingJobBeginRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try self.manager.beginTableEmptyingJob(request);
        }

        fn finishTableEmptyingJob(ptr: *anyopaque, request: metadata_table_manager.TableEmptyingJobFinishRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try self.manager.finishTableEmptyingJob(request);
        }

        fn invalidateTableEmptyingJob(ptr: *anyopaque, request: metadata_table_manager.TableEmptyingJobInvalidateRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try self.manager.invalidateTableEmptyingJob(request);
        }
    };

    const records = try manager.listTableEmptyingJobs(alloc);
    defer manager.freeTableEmptyingJobs(alloc, records);
    try std.testing.expectEqual(@as(usize, 1), records.len);

    var catalog = Catalog{
        .manager = &manager,
        .table = table,
        .current_range = .{
            .group_id = stale_range.group_id,
            .range_id = stale_range.range_id + 1,
            .table_id = stale_range.table_id,
            .start_key = stale_range.start_key,
            .end_key = stale_range.end_key,
        },
    };
    try std.testing.expectError(
        error.TopologyChanged,
        runTableEmptyingJobGroupLocal(alloc, &db, catalog.iface(), table, records[0], "worker-a", 1000, 500),
    );

    const row = (try db.get(alloc, "row:a")) orelse return error.TestUnexpectedResult;
    defer alloc.free(row);

    const final_jobs = try manager.listTableEmptyingJobs(alloc);
    defer manager.freeTableEmptyingJobs(alloc, final_jobs);
    try std.testing.expectEqual(@as(usize, 1), final_jobs.len);
    try std.testing.expectEqualStrings(metadata_table_manager.table_emptying_invalid, final_jobs[0].state);
    try std.testing.expectEqualStrings(@errorName(error.TopologyChanged), final_jobs[0].last_error);
}

test "table emptying worker invalidates malformed affected table metadata before deleting rows" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/table-emptying-invalid-affected-tables", .{tmp.sub_path});
    defer alloc.free(path);

    var db = try db_mod.DB.open(alloc, path, .{});
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    try db.applyTableSchemaJson(alloc, schema_json, .{});
    try db.batch(.{ .writes = &.{
        .{ .key = "row:a", .value = "{\"id\":\"a\",\"status\":\"open\"}" },
    } });

    const table = metadata_table_manager.TableRecord{
        .table_id = 77,
        .name = "orders",
        .schema_json = schema_json,
    };
    const job = metadata_table_manager.TableEmptyingJobRecord{
        .job_id = 7001,
        .table_id = 77,
        .group_id = 9001,
        .schema_generation = metadata_table_manager.schemaRewriteGenerationForSchemaJson(schema_json),
        .data_generation = table.data_generation,
        .affected_table_ids = &.{ 88, 77 },
    };

    const Catalog = struct {
        invalidated: bool = false,
        last_error: []const u8 = "",

        fn iface(self: *@This()) table_catalog.CatalogSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                    .begin_table_emptying_job = beginTableEmptyingJob,
                    .finish_table_emptying_job = finishTableEmptyingJob,
                    .invalidate_table_emptying_job = invalidateTableEmptyingJob,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = &.{},
                .ranges = &.{},
                .stores = &.{},
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}

        fn beginTableEmptyingJob(_: *anyopaque, request: metadata_table_manager.TableEmptyingJobBeginRequest) !void {
            if (request.job_id != 7001 or request.lease_owner.len == 0) return error.TestUnexpectedResult;
        }

        fn finishTableEmptyingJob(_: *anyopaque, _: metadata_table_manager.TableEmptyingJobFinishRequest) !void {
            return error.TestUnexpectedResult;
        }

        fn invalidateTableEmptyingJob(ptr: *anyopaque, request: metadata_table_manager.TableEmptyingJobInvalidateRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (request.job_id != 7001 or request.lease_owner.len == 0) return error.TestUnexpectedResult;
            self.invalidated = true;
            self.last_error = request.last_error;
        }
    };

    var catalog = Catalog{};
    try std.testing.expectError(
        error.InvalidTableEmptyingJob,
        runTableEmptyingJobGroupLocal(alloc, &db, catalog.iface(), table, job, "worker-a", 1000, 500),
    );
    try std.testing.expect(catalog.invalidated);
    try std.testing.expectEqualStrings(@errorName(error.InvalidTableEmptyingJob), catalog.last_error);

    const row = (try db.get(alloc, "row:a")) orelse return error.TestUnexpectedResult;
    defer alloc.free(row);
}

test "table emptying worker completes restart identity range delete before catalog reset" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/table-emptying-restart-identity-worker", .{tmp.sub_path});
    defer alloc.free(path);

    var db = try db_mod.DB.open(alloc, path, .{});
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    try db.applyTableSchemaJson(alloc, schema_json, .{});
    try db.batch(.{ .writes = &.{
        .{ .key = "row:a", .value = "{\"id\":\"a\",\"status\":\"open\"}" },
    } });

    var manager = metadata_table_manager.TableManager.init(alloc);
    defer manager.deinit();
    const table = metadata_table_manager.TableRecord{
        .table_id = 77,
        .name = "orders",
        .schema_json = schema_json,
    };
    try manager.upsertTable(table);
    var job = metadata_table_manager.TableEmptyingJobRecord{
        .job_id = 0,
        .table_id = 77,
        .group_id = 9001,
        .schema_generation = metadata_table_manager.schemaRewriteGenerationForSchemaJson(schema_json),
        .start_row_key = "",
        .end_row_key = null,
        .affected_table_ids = &.{77},
        .restart_identity = true,
    };
    job.job_id = metadata_table_manager.stableTableEmptyingJobId(job);
    try manager.upsertTableEmptyingJob(job);

    const Catalog = struct {
        manager: *metadata_table_manager.TableManager,

        fn iface(self: *@This()) table_catalog.CatalogSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                    .begin_table_emptying_job = beginTableEmptyingJob,
                    .finish_table_emptying_job = finishTableEmptyingJob,
                    .invalidate_table_emptying_job = invalidateTableEmptyingJob,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = &.{},
                .ranges = &.{},
                .stores = &.{},
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}

        fn beginTableEmptyingJob(ptr: *anyopaque, request: metadata_table_manager.TableEmptyingJobBeginRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try self.manager.beginTableEmptyingJob(request);
        }

        fn finishTableEmptyingJob(ptr: *anyopaque, request: metadata_table_manager.TableEmptyingJobFinishRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try self.manager.finishTableEmptyingJob(request);
        }

        fn invalidateTableEmptyingJob(ptr: *anyopaque, request: metadata_table_manager.TableEmptyingJobInvalidateRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try self.manager.invalidateTableEmptyingJob(request);
        }
    };

    const records = try manager.listTableEmptyingJobs(alloc);
    defer manager.freeTableEmptyingJobs(alloc, records);
    try std.testing.expectEqual(@as(usize, 1), records.len);

    var catalog = Catalog{ .manager = &manager };
    var result = try runTableEmptyingJobGroupLocal(alloc, &db, catalog.iface(), table, records[0], "worker-a", 1000, 500);
    defer result.deinit(alloc);
    try std.testing.expect(result.claimed);
    try std.testing.expect(result.completed);
    try std.testing.expect(!result.invalidated);
    try std.testing.expectEqual(@as(u32, 1), result.result.matched);
    try std.testing.expectEqual(@as(u32, 1), result.result.staged);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, "row:a"));

    const final_jobs = try manager.listTableEmptyingJobs(alloc);
    defer manager.freeTableEmptyingJobs(alloc, final_jobs);
    try std.testing.expectEqual(@as(usize, 1), final_jobs.len);
    try std.testing.expectEqualStrings(metadata_table_manager.table_emptying_ready, final_jobs[0].state);
    try std.testing.expectEqual(@as(u64, 1), final_jobs[0].completed_row_count);
}

test "table emptying and secondary index rebuild converge across chaos and reopen" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/table-emptying-secondary-index-chaos", .{tmp.sub_path});
    defer alloc.free(path);

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"amount":{"type":"numeric","x-antfly-index-lifecycle":"building","x-antfly-index-generation":9},"status":{"type":"keyword"}},"required":["id","amount","status"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    const table = metadata_table_manager.TableRecord{
        .table_id = 77,
        .name = "orders",
        .schema_json = schema_json,
    };
    const range = metadata_table_manager.RangeRecord{
        .group_id = 9001,
        .range_id = 9101,
        .table_id = table.table_id,
        .start_key = "",
        .end_key = null,
    };
    const stale_range = metadata_table_manager.RangeRecord{
        .group_id = range.group_id,
        .range_id = range.range_id + 1,
        .table_id = range.table_id,
        .start_key = range.start_key,
        .end_key = range.end_key,
    };

    var table_emptying_job = metadata_table_manager.TableEmptyingJobRecord{
        .job_id = 0,
        .table_id = table.table_id,
        .group_id = range.group_id,
        .range_id = range.range_id,
        .schema_generation = metadata_table_manager.schemaRewriteGenerationForSchemaJson(schema_json),
        .data_generation = table.data_generation,
        .start_row_key = range.start_key,
        .end_row_key = range.end_key,
        .affected_table_ids = &.{table.table_id},
    };
    table_emptying_job.job_id = metadata_table_manager.stableTableEmptyingJobId(table_emptying_job);
    const rebuild_range = metadata_table_manager.SecondaryIndexRebuildRangeRecord{
        .table_id = table.table_id,
        .index_name = "amount",
        .index_generation = 9,
        .start_row_key = range.start_key,
        .end_row_key = range.end_key,
        .group_id = range.group_id,
        .range_id = range.range_id,
    };

    const stale_amount_key = try db_mod.internal_keys.relationalColumnIndexKeyAlloc(alloc, "amount", "row:b");
    defer alloc.free(stale_amount_key);
    const stale_concurrent_amount_key = try db_mod.internal_keys.relationalColumnIndexKeyAlloc(alloc, "amount", "row:c");
    defer alloc.free(stale_concurrent_amount_key);

    var manager = metadata_table_manager.TableManager.init(alloc);
    defer manager.deinit();
    try manager.upsertTable(table);
    try manager.upsertTableEmptyingJob(table_emptying_job);
    try manager.upsertSecondaryIndexRebuildRange(rebuild_range);

    const Catalog = struct {
        manager: *metadata_table_manager.TableManager,
        table: metadata_table_manager.TableRecord,
        current_range: metadata_table_manager.RangeRecord,

        fn iface(self: *@This()) table_catalog.CatalogSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                    .begin_table_emptying_job = beginTableEmptyingJob,
                    .finish_table_emptying_job = finishTableEmptyingJob,
                    .invalidate_table_emptying_job = invalidateTableEmptyingJob,
                    .begin_secondary_index_rebuild_range = beginSecondaryIndexRebuildRange,
                    .finish_secondary_index_rebuild_range = finishSecondaryIndexRebuildRange,
                    .invalidate_secondary_index_rebuild_range = invalidateSecondaryIndexRebuildRange,
                },
            };
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @as([*]metadata_table_manager.TableRecord, @ptrCast(&self.table))[0..1],
                .ranges = @as([*]metadata_table_manager.RangeRecord, @ptrCast(&self.current_range))[0..1],
                .stores = &.{},
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}

        fn beginTableEmptyingJob(ptr: *anyopaque, request: metadata_table_manager.TableEmptyingJobBeginRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try self.manager.beginTableEmptyingJob(request);
        }

        fn finishTableEmptyingJob(ptr: *anyopaque, request: metadata_table_manager.TableEmptyingJobFinishRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try self.manager.finishTableEmptyingJob(request);
        }

        fn invalidateTableEmptyingJob(ptr: *anyopaque, request: metadata_table_manager.TableEmptyingJobInvalidateRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try self.manager.invalidateTableEmptyingJob(request);
        }

        fn beginSecondaryIndexRebuildRange(ptr: *anyopaque, request: metadata_table_manager.SecondaryIndexRebuildRangeBeginRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try self.manager.beginSecondaryIndexRebuildRange(request);
        }

        fn finishSecondaryIndexRebuildRange(ptr: *anyopaque, request: metadata_table_manager.SecondaryIndexRebuildRangeFinishRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try self.manager.finishSecondaryIndexRebuildRange(request);
        }

        fn invalidateSecondaryIndexRebuildRange(ptr: *anyopaque, request: metadata_table_manager.SecondaryIndexRebuildRangeInvalidateRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try self.manager.invalidateSecondaryIndexRebuildRange(request);
        }
    };

    var catalog = Catalog{ .manager = &manager, .table = table, .current_range = range };
    {
        var db = try db_mod.DB.open(alloc, path, .{});
        defer db.close();

        try db.applyTableSchemaJson(alloc, schema_json, .{});
        try db.batch(.{ .writes = &.{
            .{ .key = "row:a", .value = "{\"id\":\"a\",\"amount\":1,\"status\":\"open\"}" },
            .{ .key = "row:b", .value = "{\"id\":\"b\",\"amount\":2,\"status\":\"open\"}" },
        } });
        try db.core.store.put(stale_amount_key, "");

        const locker_txn = try db.beginTransaction(1_000);
        try db.claimRowsForTransaction(locker_txn, &.{"row:a"}, .{
            .mode = .for_update,
            .owner_id = "writer:row-a",
            .txn_id = locker_txn,
        });

        {
            const jobs = try manager.listTableEmptyingJobs(alloc);
            defer manager.freeTableEmptyingJobs(alloc, jobs);
            var partial = try runTableEmptyingJobGroupLocal(alloc, &db, catalog.iface(), table, jobs[0], "empty-a", 1_000, 200);
            defer partial.deinit(alloc);
            try std.testing.expect(partial.claimed);
            try std.testing.expect(!partial.completed);
            try std.testing.expect(!partial.invalidated);
            try std.testing.expectEqual(@as(u32, 2), partial.result.matched);
            try std.testing.expectEqual(@as(u32, 1), partial.result.staged);
        }

        try std.testing.expectError(error.NotFound, db.core.store.get(alloc, "row:b"));
        const locked = (try db.get(alloc, "row:a")) orelse return error.TestUnexpectedResult;
        defer alloc.free(locked);

        try db.batch(.{ .writes = &.{
            .{ .key = "row:c", .value = "{\"id\":\"c\",\"amount\":3,\"status\":\"open\"}" },
        } });
        try db.core.store.put(stale_concurrent_amount_key, "");

        catalog.current_range = stale_range;
        {
            const rebuilds = try manager.listSecondaryIndexRebuildRanges(alloc);
            defer manager.freeSecondaryIndexRebuildRanges(alloc, rebuilds);
            try std.testing.expectError(
                error.TopologyChanged,
                runSecondaryIndexRebuildRangeGroupLocal(&db, catalog.iface(), rebuilds[0], "index-a", 1_050, 200),
            );
        }
        const stale_after_invalid = try db.core.store.get(alloc, stale_concurrent_amount_key);
        defer alloc.free(stale_after_invalid);

        try db.abortTransaction(locker_txn, 1_150);
    }

    catalog.current_range = range;
    _ = manager.removeSecondaryIndexRebuildRange(rebuild_range.table_id, rebuild_range.index_name, rebuild_range.index_generation, rebuild_range.start_row_key);
    try manager.upsertSecondaryIndexRebuildRange(rebuild_range);
    {
        var db = try db_mod.DB.open(alloc, path, .{});
        defer db.close();

        {
            const running_jobs = try manager.listTableEmptyingJobs(alloc);
            defer manager.freeTableEmptyingJobs(alloc, running_jobs);
            try std.testing.expectEqual(@as(usize, 1), running_jobs.len);
            try std.testing.expectEqualStrings(metadata_table_manager.table_emptying_running, running_jobs[0].state);
            try std.testing.expectEqualStrings("empty-a", running_jobs[0].lease_owner);

            var finished = try runTableEmptyingJobGroupLocal(alloc, &db, catalog.iface(), table, running_jobs[0], "empty-b", 1_200, 200);
            defer finished.deinit(alloc);
            try std.testing.expect(finished.claimed);
            try std.testing.expect(finished.completed);
            try std.testing.expect(!finished.invalidated);
            try std.testing.expectEqual(@as(u32, 2), finished.result.matched);
            try std.testing.expectEqual(@as(u32, 2), finished.result.staged);
        }

        {
            const rebuilds = try manager.listSecondaryIndexRebuildRanges(alloc);
            defer manager.freeSecondaryIndexRebuildRanges(alloc, rebuilds);
            try std.testing.expectEqual(@as(usize, 1), rebuilds.len);
            try std.testing.expectEqualStrings(metadata_table_manager.secondary_index_rebuild_declared, rebuilds[0].state);
            const rebuilt = try runSecondaryIndexRebuildRangeGroupLocal(&db, catalog.iface(), rebuilds[0], "index-b", 1_250, 200);
            try std.testing.expect(rebuilt.claimed);
            try std.testing.expect(rebuilt.completed);
            try std.testing.expect(!rebuilt.invalidated);
            try std.testing.expectEqual(@as(u64, 0), rebuilt.report.scanned_rows);
        }

        try std.testing.expectError(error.NotFound, db.core.store.get(alloc, "row:a"));
        try std.testing.expectError(error.NotFound, db.core.store.get(alloc, "row:b"));
        try std.testing.expectError(error.NotFound, db.core.store.get(alloc, "row:c"));
        try std.testing.expectError(error.NotFound, db.core.store.get(alloc, stale_amount_key));
        try std.testing.expectError(error.NotFound, db.core.store.get(alloc, stale_concurrent_amount_key));
    }

    const final_jobs = try manager.listTableEmptyingJobs(alloc);
    defer manager.freeTableEmptyingJobs(alloc, final_jobs);
    try std.testing.expectEqual(@as(usize, 1), final_jobs.len);
    try std.testing.expectEqualStrings(metadata_table_manager.table_emptying_ready, final_jobs[0].state);
    try std.testing.expectEqualStrings("", final_jobs[0].lease_owner);
    try std.testing.expectEqual(@as(u32, 2), final_jobs[0].attempts);
    try std.testing.expectEqual(@as(u64, 2), final_jobs[0].completed_row_count);

    const final_rebuilds = try manager.listSecondaryIndexRebuildRanges(alloc);
    defer manager.freeSecondaryIndexRebuildRanges(alloc, final_rebuilds);
    try std.testing.expectEqual(@as(usize, 1), final_rebuilds.len);
    try std.testing.expectEqualStrings(metadata_table_manager.secondary_index_rebuild_ready, final_rebuilds[0].state);
    try std.testing.expectEqualStrings("", final_rebuilds[0].lease_owner);
    try std.testing.expectEqual(@as(u32, 1), final_rebuilds[0].attempts);
    try std.testing.expectEqual(@as(u64, 0), final_rebuilds[0].completed_row_count);
}

test "schema rewrite worker invalidates stale range job before rewriting rows" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/schema-rewrite-stale-range-worker", .{tmp.sub_path});
    defer alloc.free(path);

    var db = try db_mod.DB.open(alloc, path, .{});
    defer db.close();

    const schema_v1 =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id","status"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    const schema_v2 =
        \\{"version":2,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"status_key":{"type":"keyword"}},"required":["id","status"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    try db.applyTableSchemaJson(alloc, schema_v1, .{});
    try db.batch(.{ .writes = &.{
        .{ .key = "row:a", .value = "{\"id\":\"a\",\"status\":\"ACTIVE\"}" },
    } });
    try db.applyTableSchemaJson(alloc, schema_v2, .{});

    var manager = metadata_table_manager.TableManager.init(alloc);
    defer manager.deinit();
    const table = metadata_table_manager.TableRecord{
        .table_id = 77,
        .name = "events",
        .schema_json = schema_v2,
    };
    try manager.upsertTable(table);
    const stale_range = metadata_table_manager.RangeRecord{
        .group_id = 9001,
        .range_id = 9101,
        .table_id = 77,
        .start_key = "",
        .end_key = null,
    };
    const job = metadata_table_manager.SchemaRewriteJobRecord{
        .job_id = 8101,
        .table_id = 77,
        .group_id = stale_range.group_id,
        .range_id = stale_range.range_id,
        .schema_generation = metadata_table_manager.schemaRewriteGenerationForSchemaJson(schema_v2),
        .action = "rewrite",
        .reason = "row_images",
        .start_row_key = stale_range.start_key,
        .end_row_key = stale_range.end_key,
        .target_column = "status_key",
        .expression = .{
            .kind = .lower,
            .operands = &.{.{ .kind = .field, .field = "status" }},
        },
    };
    try manager.upsertSchemaRewriteJob(job);

    const Catalog = struct {
        manager: *metadata_table_manager.TableManager,
        table: metadata_table_manager.TableRecord,
        current_range: metadata_table_manager.RangeRecord,

        fn iface(self: *@This()) table_catalog.CatalogSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                    .begin_schema_rewrite_job = beginSchemaRewriteJob,
                    .finish_schema_rewrite_job = finishSchemaRewriteJob,
                    .invalidate_schema_rewrite_job = invalidateSchemaRewriteJob,
                },
            };
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @as([*]metadata_table_manager.TableRecord, @ptrCast(&self.table))[0..1],
                .ranges = @as([*]metadata_table_manager.RangeRecord, @ptrCast(&self.current_range))[0..1],
                .stores = &.{},
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}

        fn beginSchemaRewriteJob(ptr: *anyopaque, request: metadata_table_manager.SchemaRewriteJobBeginRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try self.manager.beginSchemaRewriteJob(request);
        }

        fn finishSchemaRewriteJob(ptr: *anyopaque, request: metadata_table_manager.SchemaRewriteJobFinishRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try self.manager.finishSchemaRewriteJob(request);
        }

        fn invalidateSchemaRewriteJob(ptr: *anyopaque, request: metadata_table_manager.SchemaRewriteJobInvalidateRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try self.manager.invalidateSchemaRewriteJob(request);
        }
    };

    const records = try manager.listSchemaRewriteJobs(alloc);
    defer manager.freeSchemaRewriteJobs(alloc, records);
    try std.testing.expectEqual(@as(usize, 1), records.len);

    var catalog = Catalog{
        .manager = &manager,
        .table = table,
        .current_range = .{
            .group_id = stale_range.group_id,
            .range_id = stale_range.range_id + 1,
            .table_id = stale_range.table_id,
            .start_key = stale_range.start_key,
            .end_key = stale_range.end_key,
        },
    };
    try std.testing.expectError(
        error.TopologyChanged,
        runSchemaRewriteJobGroupLocal(alloc, &db, catalog.iface(), records[0], "worker-a", 1000, 500),
    );

    const row = (try db.get(alloc, "row:a")) orelse return error.TestUnexpectedResult;
    defer alloc.free(row);
    try std.testing.expect(std.mem.indexOf(u8, row, "\"status_key\"") == null);

    const final_jobs = try manager.listSchemaRewriteJobs(alloc);
    defer manager.freeSchemaRewriteJobs(alloc, final_jobs);
    try std.testing.expectEqual(@as(usize, 1), final_jobs.len);
    try std.testing.expectEqualStrings(metadata_table_manager.schema_rewrite_invalid, final_jobs[0].state);
    try std.testing.expectEqualStrings(@errorName(error.TopologyChanged), final_jobs[0].last_error);
}

test "schema rewrite worker pass treats unclaimed terminal jobs as terminal" {
    const alloc = std.testing.allocator;

    const Catalog = struct {
        table: metadata_table_manager.TableRecord = .{
            .table_id = 77,
            .name = "events",
            .placement_role = "data",
            .indexes_json = "{}",
            .schema_json = "{}",
        },
        jobs: [2]metadata_table_manager.SchemaRewriteJobRecord = .{
            .{
                .job_id = 8101,
                .table_id = 77,
                .group_id = 9001,
                .schema_generation = 11,
                .action = "rewrite",
                .reason = "row_images",
                .start_row_key = "",
                .end_row_key = null,
                .target_column = "status_key",
                .expression = .{
                    .kind = .lower,
                    .operands = &.{.{ .kind = .field, .field = "status" }},
                },
            },
            .{
                .job_id = 8102,
                .table_id = 77,
                .group_id = 9002,
                .schema_generation = 11,
                .action = "rewrite",
                .reason = "row_images",
                .start_row_key = "",
                .end_row_key = null,
                .target_column = "status_key",
                .expression = .{
                    .kind = .lower,
                    .operands = &.{.{ .kind = .field, .field = "status" }},
                },
            },
        },

        fn iface(self: *@This()) table_catalog.CatalogSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @as([*]metadata_table_manager.TableRecord, @ptrCast(&self.table))[0..1],
                .ranges = &.{},
                .schema_rewrite_jobs = self.jobs[0..],
                .stores = &.{},
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const Source = struct {
        fn schemaRewriteGroupLocal(
            _: @This(),
            _: std.mem.Allocator,
            group_id: u64,
            _: []const u8,
            record: metadata_table_manager.SchemaRewriteJobRecord,
            _: []const u8,
            _: u64,
        ) !?SchemaRewriteWorkerResult {
            return .{
                .group_id = group_id,
                .table_id = record.table_id,
                .job_id = record.job_id,
                .claimed = false,
                .completed = record.job_id == 8101,
                .invalidated = record.job_id == 8102,
            };
        }
    };

    var catalog = Catalog{};
    var pass = try runSchemaRewriteWorkerPassForCatalog(
        alloc,
        Source{},
        catalog.iface(),
        "events",
        "worker-a",
        500,
        4,
    );
    defer pass.deinit(alloc);

    try std.testing.expect(!pass.complete);
    try std.testing.expectEqual(@as(u64, 2), pass.jobs_scanned);
    try std.testing.expectEqual(@as(u64, 0), pass.jobs_claimed);
    try std.testing.expectEqual(@as(u64, 1), pass.jobs_completed);
    try std.testing.expectEqual(@as(u64, 1), pass.jobs_invalidated);
    try std.testing.expectEqual(@as(u64, 0), pass.jobs_busy);
    try std.testing.expectEqual(@as(usize, 2), pass.groups.len);
    try std.testing.expect(pass.groups[0].completed);
    try std.testing.expect(pass.groups[1].invalidated);
}

test "schema job worker admission policy bounds work and stale takeover" {
    const alloc = std.testing.allocator;

    const Catalog = struct {
        table: metadata_table_manager.TableRecord = .{
            .table_id = 77,
            .name = "events",
            .placement_role = "data",
            .indexes_json = "{}",
            .schema_json = "{}",
            .data_generation = 4,
        },
        schema_jobs: [3]metadata_table_manager.SchemaRewriteJobRecord = .{
            .{
                .job_id = 8101,
                .table_id = 77,
                .group_id = 9001,
                .schema_generation = 11,
                .action = "rewrite",
                .reason = "row_images",
                .start_row_key = "",
            },
            .{
                .job_id = 8102,
                .table_id = 77,
                .group_id = 9002,
                .schema_generation = 11,
                .action = "rewrite",
                .reason = "row_images",
                .start_row_key = "",
                .state = metadata_table_manager.schema_rewrite_running,
                .lease_owner = "stale-worker",
                .lease_expires_at_ms = 1,
            },
            .{
                .job_id = 8103,
                .table_id = 77,
                .group_id = 9003,
                .schema_generation = 11,
                .action = "rewrite",
                .reason = "row_images",
                .start_row_key = "",
            },
        },
        emptying_jobs: [3]metadata_table_manager.TableEmptyingJobRecord = .{
            .{ .job_id = 9101, .table_id = 77, .group_id = 8001, .schema_generation = 11, .data_generation = 4, .affected_table_ids = &.{77} },
            .{
                .job_id = 9102,
                .table_id = 77,
                .group_id = 8002,
                .schema_generation = 11,
                .data_generation = 4,
                .affected_table_ids = &.{77},
                .state = metadata_table_manager.table_emptying_running,
                .lease_owner = "stale-worker",
                .lease_expires_at_ms = 1,
            },
            .{ .job_id = 9103, .table_id = 77, .group_id = 8003, .schema_generation = 11, .data_generation = 4, .affected_table_ids = &.{77} },
        },

        fn iface(self: *@This()) table_catalog.CatalogSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @as([*]metadata_table_manager.TableRecord, @ptrCast(&self.table))[0..1],
                .ranges = &.{},
                .schema_rewrite_jobs = self.schema_jobs[0..],
                .table_emptying_jobs = self.emptying_jobs[0..],
                .stores = &.{},
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const Source = struct {
        schema_calls: usize = 0,
        schema_seen_running: bool = false,
        table_calls: usize = 0,
        table_seen_running: bool = false,

        fn schemaRewriteGroupLocal(
            self: *@This(),
            _: std.mem.Allocator,
            group_id: u64,
            _: []const u8,
            record: metadata_table_manager.SchemaRewriteJobRecord,
            _: []const u8,
            lease_ms: u64,
        ) !?SchemaRewriteWorkerResult {
            try std.testing.expectEqual(@as(u64, 250), lease_ms);
            self.schema_calls += 1;
            if (std.mem.eql(u8, record.state, metadata_table_manager.schema_rewrite_running)) self.schema_seen_running = true;
            return .{
                .group_id = group_id,
                .table_id = record.table_id,
                .job_id = record.job_id,
                .claimed = true,
            };
        }

        fn tableEmptyingGroupLocal(
            self: *@This(),
            _: std.mem.Allocator,
            group_id: u64,
            _: []const u8,
            record: metadata_table_manager.TableEmptyingJobRecord,
            _: []const u8,
            lease_ms: u64,
        ) !?TableEmptyingWorkerResult {
            try std.testing.expectEqual(@as(u64, 250), lease_ms);
            self.table_calls += 1;
            if (std.mem.eql(u8, record.state, metadata_table_manager.table_emptying_running)) self.table_seen_running = true;
            return .{
                .group_id = group_id,
                .table_id = record.table_id,
                .job_id = record.job_id,
                .claimed = true,
            };
        }
    };

    var catalog = Catalog{};

    {
        var source = Source{};
        var pass = try runSchemaRewriteWorkerPassForCatalogWithAdmission(
            alloc,
            &source,
            catalog.iface(),
            "events",
            "worker-a",
            .{ .lease_ms = 250, .max_work_units = 1, .allow_stale_lease_takeover = false },
        );
        defer pass.deinit(alloc);
        try std.testing.expect(!pass.complete);
        try std.testing.expectEqual(@as(u64, 2), pass.jobs_scanned);
        try std.testing.expectEqual(@as(u64, 1), pass.jobs_claimed);
        try std.testing.expectEqual(@as(usize, 1), source.schema_calls);
        try std.testing.expect(!source.schema_seen_running);
    }

    {
        var source = Source{};
        var pass = try runSchemaRewriteWorkerPassForCatalogWithAdmission(
            alloc,
            &source,
            catalog.iface(),
            "events",
            "worker-a",
            .{ .lease_ms = 250, .max_work_units = 2, .allow_stale_lease_takeover = true },
        );
        defer pass.deinit(alloc);
        try std.testing.expect(!pass.complete);
        try std.testing.expectEqual(@as(u64, 3), pass.jobs_scanned);
        try std.testing.expectEqual(@as(u64, 2), pass.jobs_claimed);
        try std.testing.expectEqual(@as(usize, 2), source.schema_calls);
        try std.testing.expect(source.schema_seen_running);
    }

    {
        var source = Source{};
        var pass = try runTableEmptyingWorkerPassForCatalogWithAdmission(
            alloc,
            &source,
            catalog.iface(),
            "events",
            "worker-a",
            .{ .lease_ms = 250, .max_work_units = 1, .allow_stale_lease_takeover = false },
        );
        defer pass.deinit(alloc);
        try std.testing.expect(!pass.complete);
        try std.testing.expectEqual(@as(u64, 2), pass.jobs_scanned);
        try std.testing.expectEqual(@as(u64, 1), pass.jobs_claimed);
        try std.testing.expectEqual(@as(usize, 1), source.table_calls);
        try std.testing.expect(!source.table_seen_running);
    }

    {
        var source = Source{};
        var pass = try runTableEmptyingWorkerPassForCatalogWithAdmission(
            alloc,
            &source,
            catalog.iface(),
            "events",
            "worker-a",
            .{ .lease_ms = 250, .max_work_units = 2, .allow_stale_lease_takeover = true },
        );
        defer pass.deinit(alloc);
        try std.testing.expect(!pass.complete);
        try std.testing.expectEqual(@as(u64, 3), pass.jobs_scanned);
        try std.testing.expectEqual(@as(u64, 2), pass.jobs_claimed);
        try std.testing.expectEqual(@as(usize, 2), source.table_calls);
        try std.testing.expect(source.table_seen_running);
    }
}

test "table emptying worker pass can select same-name table by table id" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    const schema_generation = metadata_table_manager.schemaRewriteGenerationForSchemaJson(schema_json);

    const Catalog = struct {
        tables: [2]metadata_table_manager.TableRecord = .{
            .{ .table_id = 91, .name = "events", .namespace_name = "public", .schema_json = schema_json, .data_generation = 4 },
            .{ .table_id = 92, .name = "events", .namespace_name = "tenant", .schema_json = schema_json, .data_generation = 7 },
        },
        jobs: [2]metadata_table_manager.TableEmptyingJobRecord = undefined,

        fn seed(self: *@This(), generation: u64) void {
            self.jobs = .{
                .{ .job_id = 9101, .table_id = 91, .group_id = 8101, .schema_generation = generation, .data_generation = 4, .affected_table_ids = &.{91} },
                .{ .job_id = 9201, .table_id = 92, .group_id = 8201, .schema_generation = generation, .data_generation = 7, .affected_table_ids = &.{92} },
            };
        }

        fn iface(self: *@This()) table_catalog.CatalogSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = self.tables[0..],
                .ranges = &.{},
                .table_emptying_jobs = self.jobs[0..],
                .stores = &.{},
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const Source = struct {
        calls: usize = 0,
        last_group_id: u64 = 0,
        last_table_id: u64 = 0,

        fn tableEmptyingGroupLocal(
            self: *@This(),
            _: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            record: metadata_table_manager.TableEmptyingJobRecord,
            _: []const u8,
            _: u64,
        ) !?TableEmptyingWorkerResult {
            if (!std.mem.eql(u8, table_name, "events")) return error.TestUnexpectedResult;
            self.calls += 1;
            self.last_group_id = group_id;
            self.last_table_id = record.table_id;
            return .{
                .group_id = group_id,
                .table_id = record.table_id,
                .job_id = record.job_id,
                .claimed = true,
                .completed = true,
                .result = .{ .matched = 3, .staged = 3 },
            };
        }
    };

    var catalog = Catalog{};
    catalog.seed(schema_generation);
    var source = Source{};
    var pass = try runTableEmptyingWorkerPassForCatalogTableId(
        alloc,
        &source,
        catalog.iface(),
        92,
        "events",
        "worker-a",
        500,
        4,
    );
    defer pass.deinit(alloc);

    try std.testing.expect(pass.complete);
    try std.testing.expectEqual(@as(usize, 1), source.calls);
    try std.testing.expectEqual(@as(u64, 8201), source.last_group_id);
    try std.testing.expectEqual(@as(u64, 92), source.last_table_id);
    try std.testing.expectEqual(@as(u64, 1), pass.jobs_scanned);
    try std.testing.expectEqual(@as(u64, 1), pass.jobs_claimed);
    try std.testing.expectEqual(@as(u64, 1), pass.jobs_completed);
    try std.testing.expectEqual(@as(u64, 3), pass.rows_matched);
    try std.testing.expectEqual(@as(u64, 3), pass.rows_staged);
}

test "secondary index promotion ignores stale ready rebuild generation" {
    const alloc = std.testing.allocator;
    const stale_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"amount":{"type":"numeric","x-antfly-index-lifecycle":"building","x-antfly-index-generation":10},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;

    const Catalog = struct {
        table: metadata_table_manager.TableRecord = .{
            .table_id = 77,
            .name = "orders",
            .placement_role = "data",
            .indexes_json = "{}",
            .schema_json = stale_schema_json,
        },
        range: metadata_table_manager.RangeRecord = .{
            .group_id = 9001,
            .range_id = 9101,
            .table_id = 77,
            .start_key = "",
            .end_key = null,
        },
        rebuild: metadata_table_manager.SecondaryIndexRebuildRangeRecord = .{
            .table_id = 77,
            .index_name = "amount",
            .index_generation = 9,
            .start_row_key = "",
            .end_row_key = null,
            .group_id = 9001,
            .state = metadata_table_manager.secondary_index_rebuild_ready,
        },

        fn iface(self: *@This()) table_catalog.CatalogSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @as([*]metadata_table_manager.TableRecord, @ptrCast(&self.table))[0..1],
                .ranges = @as([*]metadata_table_manager.RangeRecord, @ptrCast(&self.range))[0..1],
                .secondary_index_rebuild_ranges = @as([*]metadata_table_manager.SecondaryIndexRebuildRangeRecord, @ptrCast(&self.rebuild))[0..1],
                .stores = &.{},
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var catalog = Catalog{};
    try std.testing.expectEqual(@as(u64, 0), try promoteReadySecondaryIndexesForCatalog(alloc, catalog.iface(), "orders"));
}
