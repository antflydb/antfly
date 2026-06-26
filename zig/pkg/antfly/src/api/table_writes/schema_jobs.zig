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
const storage_schema = @import("../../storage/schema.zig");
const table_catalog = @import("../table_catalog.zig");
const tables_api = @import("../tables.zig");

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
    return std.mem.eql(u8, record.state, metadata_table_manager.schema_rewrite_declared) or
        std.mem.eql(u8, record.state, metadata_table_manager.schema_rewrite_running);
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
        return try self.catalog.beginSchemaRewriteJob(request);
    }

    pub fn finishSchemaRewriteJob(self: *@This(), request: metadata_table_manager.SchemaRewriteJobFinishRequest) !void {
        return try self.catalog.finishSchemaRewriteJob(request);
    }

    pub fn invalidateSchemaRewriteJob(self: *@This(), request: metadata_table_manager.SchemaRewriteJobInvalidateRequest) !void {
        return try self.catalog.invalidateSchemaRewriteJob(request);
    }
};

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
    if (worker_id.len == 0 or lease_ms == 0 or max_work_units == 0) return error.InvalidSchemaRewriteJobLease;
    var snapshot = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&snapshot);
    const table = tables_api.findTableByName(&snapshot, table_name) orelse return .{};

    var groups = std.ArrayListUnmanaged(SchemaRewriteWorkerResult).empty;
    errdefer groups.deinit(alloc);

    var result: SchemaRewriteWorkerPassResult = .{};
    for (snapshot.schema_rewrite_jobs) |record| {
        if (record.table_id != table.table_id) continue;
        if (!schemaRewriteRecordPending(record)) continue;
        result.jobs_scanned += 1;
        if (result.jobs_claimed >= max_work_units) {
            result.complete = false;
            continue;
        }

        const one = (try source.schemaRewriteGroupLocal(
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
