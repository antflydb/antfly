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

const metadata_admin = @import("../../metadata/admin.zig");
const metadata_api = @import("../../metadata/api.zig");
const metadata_table_manager = @import("../../metadata/table_manager.zig");
const platform_clock = @import("../../platform/clock.zig");
const sql_schema_mutation = @import("../../sql/schema_mutation.zig");
const db_mod = @import("../../storage/db/mod.zig");
const storage_schema = @import("../../storage/schema.zig");
const distributed_txn = @import("../distributed_txn.zig");
const table_catalog = @import("../table_catalog.zig");
const tables_api = @import("../tables.zig");
const table_write_core = @import("core.zig");
const integrity_types = @import("integrity_types.zig");
const table_write_managed_db = @import("managed_db.zig");

const ForeignKeyActionJobProgressResult = integrity_types.ForeignKeyActionJobProgressResult;
const ForeignKeyActionJobResult = integrity_types.ForeignKeyActionJobResult;
const ForeignKeyActionJobStatus = integrity_types.ForeignKeyActionJobStatus;
const ForeignKeyActionScheduleProgressResult = integrity_types.ForeignKeyActionScheduleProgressResult;
const ForeignKeyActionScheduleStatus = integrity_types.ForeignKeyActionScheduleStatus;
const ForeignKeyIntegrityAction = integrity_types.ForeignKeyIntegrityAction;
const ForeignKeyIntegrityGroupReport = integrity_types.ForeignKeyIntegrityGroupReport;
const ForeignKeyIntegrityJobStatus = integrity_types.ForeignKeyIntegrityJobStatus;
const ForeignKeyIntegrityProgress = integrity_types.ForeignKeyIntegrityProgress;
const ForeignKeyIntegrityResult = integrity_types.ForeignKeyIntegrityResult;
const ForeignKeyIntegritySchemaControllerOptions = integrity_types.ForeignKeyIntegritySchemaControllerOptions;
const ForeignKeyIntegritySchemaControllerResult = integrity_types.ForeignKeyIntegritySchemaControllerResult;
const ForeignKeyIntegritySchemaControllerTableResult = integrity_types.ForeignKeyIntegritySchemaControllerTableResult;
const ForeignKeyIntegrityTupleValue = integrity_types.ForeignKeyIntegrityTupleValue;
const ForeignKeyIntegrityViolation = integrity_types.ForeignKeyIntegrityViolation;
const ForeignKeyIntegrityWorkClaim = integrity_types.ForeignKeyIntegrityWorkClaim;
const ForeignKeyIntegrityWorkState = integrity_types.ForeignKeyIntegrityWorkState;
const ForeignKeyIntegrityWorkStatus = integrity_types.ForeignKeyIntegrityWorkStatus;
const ForeignKeyIntegrityWorkUnit = integrity_types.ForeignKeyIntegrityWorkUnit;
const UniqueConstraintIntegrityAction = integrity_types.UniqueConstraintIntegrityAction;
const UniqueConstraintIntegrityGroupReport = integrity_types.UniqueConstraintIntegrityGroupReport;
const UniqueConstraintIntegrityProgress = integrity_types.UniqueConstraintIntegrityProgress;
const UniqueConstraintIntegrityResult = integrity_types.UniqueConstraintIntegrityResult;
const UniqueConstraintIntegritySchemaControllerOptions = integrity_types.UniqueConstraintIntegritySchemaControllerOptions;
const UniqueConstraintIntegritySchemaControllerResult = integrity_types.UniqueConstraintIntegritySchemaControllerResult;
const UniqueConstraintIntegritySchemaControllerTableResult = integrity_types.UniqueConstraintIntegritySchemaControllerTableResult;
const UniqueConstraintOwnerRange = integrity_types.UniqueConstraintOwnerRange;
const UniqueConstraintOwnerTopology = integrity_types.UniqueConstraintOwnerTopology;
const TableWriteSource = table_write_core.TableWriteSource;
const applyLocalTableSchemaJson = table_write_managed_db.applyLocalTableSchemaJson;
const loadLocalTableSchemaJson = table_write_managed_db.loadLocalTableSchemaJson;

pub fn runUniqueConstraintIntegritySchemaControllerMaintenanceForTable(
    alloc: std.mem.Allocator,
    source: TableWriteSource,
    table_name: []const u8,
    schema_json: []const u8,
    options: UniqueConstraintIntegritySchemaControllerOptions,
    summary: *UniqueConstraintIntegritySchemaControllerResult,
    results: *std.ArrayListUnmanaged(UniqueConstraintIntegritySchemaControllerTableResult),
) !void {
    const selected = (try selectedUniqueConstraintIntegrityControllerConstraintAlloc(alloc, schema_json)) orelse return;
    defer alloc.free(selected);
    summary.tables_with_pending_constraints += 1;
    if (summary.tables_executed >= options.max_tables) {
        summary.complete = false;
        return;
    }

    var result = (try source.uniqueConstraintIntegrity(
        alloc,
        table_name,
        options.action,
        "",
        "",
    )) orelse return;
    errdefer result.deinit(alloc);

    if (options.action == .repair and result.complete) {
        var validation = (try source.uniqueConstraintIntegrity(
            alloc,
            table_name,
            .validate,
            "",
            "",
        )) orelse return;
        defer validation.deinit(alloc);
        result.valid = validation.valid;
        result.complete = result.complete and validation.complete;
    }

    summary.tables_executed += 1;
    summary.complete = summary.complete and result.complete;
    summary.valid = summary.valid and result.valid;
    if (result.complete and result.valid) summary.terminal_valid_results += 1;
    if (result.complete and !result.valid) summary.terminal_invalid_results += 1;
    try appendUniqueConstraintIntegritySchemaControllerTableResult(alloc, results, table_name, selected, true, result);
}

pub fn promoteLocalUniqueConstraintAfterSchemaControllerResult(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    constraint_name: []const u8,
    result: UniqueConstraintIntegrityResult,
) !void {
    if (!shouldPromoteUniqueConstraintAfterSchemaControllerResult(result)) return;
    const schema_json = (try loadLocalTableSchemaJson(alloc, db)) orelse return;
    defer alloc.free(schema_json);
    const enforced_schema_json = try sql_schema_mutation.schemaWithUniqueConstraintValidationStateAlloc(
        alloc,
        schema_json,
        constraint_name,
        .enforced,
    );
    defer alloc.free(enforced_schema_json);
    try applyLocalTableSchemaJson(alloc, db, enforced_schema_json);
}

pub fn foreignKeyIntegritySchemaControllerPassWithSchemaJson(
    alloc: std.mem.Allocator,
    source: TableWriteSource,
    table_name: []const u8,
    schema_json: []const u8,
    action: ForeignKeyIntegrityAction,
    worker_id: []const u8,
    lease_ms: u64,
    max_work_units: usize,
    constraint_name: ?[]const u8,
    lower_doc_key: []const u8,
    upper_doc_key: []const u8,
    violation_limit: usize,
) !?ForeignKeyIntegrityResult {
    if (worker_id.len == 0 or lease_ms == 0) return error.InvalidForeignKeyIntegrityRequest;
    if (!foreignKeyIntegrityWorkerActionSupported(action)) return error.InvalidForeignKeyIntegrityRequest;

    const selected_constraint = try selectedForeignKeyIntegrityControllerConstraintAlloc(alloc, schema_json, constraint_name);
    defer if (selected_constraint) |value| alloc.free(value);
    if (selected_constraint == null) {
        return try emptyForeignKeyIntegrityControllerResult(alloc, action, violation_limit);
    }

    const job_id = try stableForeignKeyIntegrityJobIdAlloc(alloc, table_name, action, selected_constraint, lower_doc_key, upper_doc_key);
    defer alloc.free(job_id);
    return try source.foreignKeyIntegrityWorkerPass(
        alloc,
        table_name,
        action,
        job_id,
        worker_id,
        lease_ms,
        max_work_units,
        selected_constraint,
        lower_doc_key,
        upper_doc_key,
        violation_limit,
    );
}

pub fn runForeignKeyIntegritySchemaControllerMaintenanceForTable(
    alloc: std.mem.Allocator,
    source: TableWriteSource,
    table_name: []const u8,
    schema_json: []const u8,
    options: ForeignKeyIntegritySchemaControllerOptions,
    summary: *ForeignKeyIntegritySchemaControllerResult,
    results: *std.ArrayListUnmanaged(ForeignKeyIntegritySchemaControllerTableResult),
) !void {
    const selected = (try selectedForeignKeyIntegrityControllerConstraintAlloc(alloc, schema_json, null)) orelse return;
    defer alloc.free(selected);
    summary.tables_with_pending_constraints += 1;
    if (summary.tables_executed >= options.max_tables) {
        summary.complete = false;
        return;
    }

    var result = (try source.foreignKeyIntegritySchemaControllerPass(
        alloc,
        table_name,
        options.action,
        options.worker_id,
        options.lease_ms,
        options.max_work_units_per_table,
        null,
        "",
        "",
        options.violation_limit,
    )) orelse return;
    errdefer result.deinit(alloc);

    summary.tables_executed += 1;
    summary.claim_attempts += result.work_claims.len;
    summary.complete = summary.complete and result.complete;
    summary.valid = summary.valid and result.valid;
    if (result.complete and result.valid) summary.terminal_valid_results += 1;
    if (result.complete and !result.valid) summary.terminal_invalid_results += 1;
    try appendForeignKeyIntegritySchemaControllerTableResult(alloc, results, table_name, true, result);
}

pub fn runForeignKeyIntegrityJobControllerMaintenanceForTable(
    alloc: std.mem.Allocator,
    source: TableWriteSource,
    table_name: []const u8,
    options: ForeignKeyIntegritySchemaControllerOptions,
    summary: *ForeignKeyIntegritySchemaControllerResult,
    results: *std.ArrayListUnmanaged(ForeignKeyIntegritySchemaControllerTableResult),
) !void {
    var progress = (try source.foreignKeyIntegrity(
        alloc,
        table_name,
        .progress,
        null,
        "",
        "",
        0,
    )) orelse return;
    defer progress.deinit(alloc);

    summary.jobs_scanned += progress.jobs.len;
    for (progress.jobs) |job| {
        if (job.completed) continue;
        if (!std.mem.eql(u8, job.table_name, table_name)) continue;
        if (foreignKeyIntegritySchemaControllerResultsContainJobId(results.items, job.job_id)) continue;
        const action = foreignKeyIntegrityActionFromJobStatus(job) orelse continue;
        if (!foreignKeyIntegrityWorkerActionSupported(action)) continue;
        if (summary.jobs_executed >= options.max_jobs) {
            summary.complete = false;
            break;
        }

        var result = (try source.foreignKeyIntegrityWorkerPass(
            alloc,
            table_name,
            action,
            job.job_id,
            options.worker_id,
            options.lease_ms,
            options.max_work_units_per_table,
            job.constraint_name,
            job.lower_doc_key,
            job.upper_doc_key,
            options.violation_limit,
        )) orelse {
            summary.complete = false;
            continue;
        };
        errdefer result.deinit(alloc);

        summary.jobs_executed += 1;
        summary.claim_attempts += result.work_claims.len;
        summary.complete = summary.complete and result.complete;
        summary.valid = summary.valid and result.valid;
        if (result.complete and result.valid) summary.terminal_valid_results += 1;
        if (result.complete and !result.valid) summary.terminal_invalid_results += 1;
        try appendForeignKeyIntegritySchemaControllerTableResult(alloc, results, table_name, false, result);
    }
}

fn foreignKeyActionJobSchemaControllerResultsContainJobId(
    jobs: []const ForeignKeyActionJobStatus,
    job_id: []const u8,
) bool {
    for (jobs) |job| {
        if (std.mem.eql(u8, job.job_id, job_id)) return true;
    }
    return false;
}

pub fn foreignKeyActionCanRunGroupDbLocal(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    group_id: u64,
    child_table_name: []const u8,
    constraint_name: []const u8,
    parent_table_name: []const u8,
    parent_key: []const u8,
) !bool {
    const child_groups = try table_catalog.resolveGroupsForSpan(alloc, catalog, child_table_name, "", "");
    defer if (child_groups.len > 0) alloc.free(child_groups);
    if (child_groups.len != 1 or child_groups[0] != group_id) return false;

    const owner_parent_table_name = try foreignKeyActionOwnerParentTableNameAlloc(
        alloc,
        catalog,
        child_table_name,
        constraint_name,
        parent_table_name,
    );
    defer alloc.free(owner_parent_table_name);

    var owner_resolution = try table_catalog.resolveForeignKeyRefOwnerGroups(
        alloc,
        catalog,
        child_table_name,
        constraint_name,
        owner_parent_table_name,
        parent_key,
    );
    defer owner_resolution.deinit(alloc);
    if (!owner_resolution.configured) return true;
    return owner_resolution.groups.len == 1 and owner_resolution.groups[0] == group_id;
}

pub fn foreignKeyActionOwnerParentTableNameAlloc(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    child_table_name: []const u8,
    constraint_name: []const u8,
    parent_table_name: []const u8,
) ![]u8 {
    const schema_json = (try table_catalog.tableSchemaJsonAlloc(alloc, catalog, child_table_name)) orelse return try alloc.dupe(u8, parent_table_name);
    defer alloc.free(schema_json);
    if (schema_json.len == 0) return try alloc.dupe(u8, parent_table_name);

    var parsed_schema = try tables_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try tables_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer storage_schema.freeSchema(alloc, runtime_schema);
    if (runtime_schema.storage_mode != .relational) return try alloc.dupe(u8, parent_table_name);

    for (runtime_schema.foreign_keys) |foreign_key| {
        if (!std.mem.eql(u8, foreign_key.name, constraint_name)) continue;
        const catalog_parent_table_name = if (std.mem.eql(u8, foreign_key.parent_table, runtime_schema.default_type))
            child_table_name
        else
            foreign_key.parent_table;
        if (!std.mem.eql(u8, parent_table_name, foreign_key.parent_table) and
            !std.mem.eql(u8, parent_table_name, catalog_parent_table_name))
        {
            return error.UnsupportedOperation;
        }
        return try alloc.dupe(u8, catalog_parent_table_name);
    }
    return try alloc.dupe(u8, parent_table_name);
}

pub fn runForeignKeyActionScheduleControllerMaintenanceForTable(
    alloc: std.mem.Allocator,
    source: TableWriteSource,
    table_name: []const u8,
    options: ForeignKeyIntegritySchemaControllerOptions,
    summary: *ForeignKeyIntegritySchemaControllerResult,
    action_schedules: *std.ArrayListUnmanaged(ForeignKeyActionScheduleStatus),
) !void {
    var progress = (try source.foreignKeyActionScheduleProgress(alloc, table_name)) orelse return;
    defer progress.deinit(alloc);

    summary.action_schedules_scanned += progress.schedules.len;
    for (progress.schedules) |schedule| {
        if (schedule.completed) continue;
        if (std.mem.eql(u8, schedule.status, "invalid")) {
            var cloned = try cloneForeignKeyActionScheduleStatus(alloc, schedule);
            errdefer cloned.deinit(alloc);
            try action_schedules.append(alloc, cloned);
            summary.action_schedules_invalid += 1;
            summary.complete = false;
            summary.valid = false;
            continue;
        }
        if (summary.action_schedules_executed >= options.max_action_jobs) {
            summary.complete = false;
            break;
        }

        const page_limit = @min(schedule.page_limit, options.action_job_page_limit);
        var result = if (try source.foreignKeyActionJobGroupLocalSchedule(
            alloc,
            schedule.group_id,
            table_name,
            schedule.action_job_id,
            schedule.action,
            options.worker_id,
            schedule.constraint_name,
            schedule.parent_table,
            schedule.parent_key,
            schedule.updated_parent_key,
            page_limit,
            schedule.cascade_depth,
            schedule.cascade_max_depth,
        )) |status| blk: {
            const groups = try alloc.alloc(ForeignKeyActionJobStatus, 1);
            groups[0] = status;
            break :blk ForeignKeyActionJobResult{
                .complete = status.completed,
                .groups = groups,
            };
        } else (try source.foreignKeyActionJobSchedule(
            alloc,
            table_name,
            schedule.action_job_id,
            schedule.action,
            options.worker_id,
            schedule.constraint_name,
            schedule.parent_table,
            schedule.parent_key,
            schedule.updated_parent_key,
            page_limit,
            schedule.cascade_depth,
            schedule.cascade_max_depth,
        )) orelse {
            summary.complete = false;
            continue;
        };
        defer result.deinit(alloc);

        if (result.groups.len == 0) {
            summary.complete = false;
            if (try source.foreignKeyActionScheduleGroupLocalMarkSeeded(
                alloc,
                schedule.group_id,
                table_name,
                schedule.schedule_id,
                0,
            )) |status_value| {
                var status = status_value;
                defer status.deinit(alloc);
                var cloned = try cloneForeignKeyActionScheduleStatus(alloc, status);
                errdefer cloned.deinit(alloc);
                try action_schedules.append(alloc, cloned);
                if (std.mem.eql(u8, status.status, "invalid")) {
                    summary.action_schedules_invalid += 1;
                    summary.valid = false;
                }
            } else if (try source.foreignKeyActionScheduleMarkSeeded(
                alloc,
                table_name,
                schedule.schedule_id,
                0,
            )) |status_value| {
                var status = status_value;
                defer status.deinit(alloc);
                var cloned = try cloneForeignKeyActionScheduleStatus(alloc, status);
                errdefer cloned.deinit(alloc);
                try action_schedules.append(alloc, cloned);
                if (std.mem.eql(u8, status.status, "invalid")) {
                    summary.action_schedules_invalid += 1;
                    summary.valid = false;
                }
            }
            continue;
        }

        var marked = if (try source.foreignKeyActionScheduleGroupLocalMarkSeeded(
            alloc,
            schedule.group_id,
            table_name,
            schedule.schedule_id,
            @intCast(result.groups.len),
        )) |status| status else (try source.foreignKeyActionScheduleMarkSeeded(
            alloc,
            table_name,
            schedule.schedule_id,
            @intCast(result.groups.len),
        )) orelse {
            summary.complete = false;
            continue;
        };
        defer marked.deinit(alloc);
        summary.action_schedules_executed += 1;
        summary.claim_attempts += result.groups.len;
        summary.complete = summary.complete and result.complete;
        summary.complete = summary.complete and marked.completed;
        if (std.mem.eql(u8, marked.status, "invalid")) {
            summary.action_schedules_invalid += 1;
            summary.valid = false;
        }
        var cloned = try cloneForeignKeyActionScheduleStatus(alloc, marked);
        errdefer cloned.deinit(alloc);
        try action_schedules.append(alloc, cloned);
    }
}

pub fn runForeignKeyActionJobControllerMaintenanceForTable(
    alloc: std.mem.Allocator,
    source: TableWriteSource,
    table_name: []const u8,
    options: ForeignKeyIntegritySchemaControllerOptions,
    summary: *ForeignKeyIntegritySchemaControllerResult,
    action_jobs: *std.ArrayListUnmanaged(ForeignKeyActionJobStatus),
) !void {
    var progress = (try source.foreignKeyActionJobProgress(alloc, table_name)) orelse return;
    defer progress.deinit(alloc);

    summary.action_jobs_scanned += progress.jobs.len;
    for (progress.jobs) |job| {
        if (job.completed) continue;
        if (foreignKeyActionJobSchemaControllerResultsContainJobId(action_jobs.items, job.job_id)) continue;
        if (std.mem.eql(u8, job.status, "invalid")) {
            var cloned = try cloneForeignKeyActionJobStatus(alloc, job);
            errdefer cloned.deinit(alloc);
            try action_jobs.append(alloc, cloned);
            summary.action_jobs_invalid += 1;
            summary.complete = false;
            summary.valid = false;
            continue;
        }
        if (summary.action_jobs_executed >= options.max_action_jobs) {
            summary.complete = false;
            break;
        }

        var result = (source.foreignKeyActionJobPage(
            alloc,
            table_name,
            job.job_id,
            job.action,
            options.worker_id,
            job.constraint_name,
            job.parent_table,
            job.parent_key,
            job.updated_parent_key,
            @min(job.page_limit, options.action_job_page_limit),
            options.lease_ms,
        ) catch |err| switch (err) {
            error.ForeignKeyIntegrityClaimBusy => {
                summary.complete = false;
                continue;
            },
            else => {
                var refreshed = (try source.foreignKeyActionJobProgress(alloc, table_name)) orelse return err;
                defer refreshed.deinit(alloc);
                const appended = try appendForeignKeyActionJobStatusFromProgressByJobId(alloc, action_jobs, refreshed, job.job_id);
                if (appended == 0) return err;
                const invalid = countInvalidForeignKeyActionJobStatusesByJobId(refreshed.jobs, job.job_id);
                summary.action_jobs_invalid += invalid;
                if (invalid > 0) summary.valid = false;
                summary.action_jobs_executed += 1;
                summary.claim_attempts += appended;
                summary.complete = false;
                continue;
            },
        }) orelse {
            summary.complete = false;
            continue;
        };
        defer result.deinit(alloc);

        summary.action_jobs_executed += 1;
        summary.claim_attempts += result.groups.len;
        summary.complete = summary.complete and result.complete;
        const invalid = countInvalidForeignKeyActionJobStatuses(result.groups);
        summary.action_jobs_invalid += invalid;
        if (invalid > 0) summary.valid = false;
        try appendForeignKeyActionJobStatuses(alloc, action_jobs, result.groups);
    }
}

pub fn promoteLocalForeignKeyAfterSchemaControllerResult(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    result: ForeignKeyIntegrityResult,
) !void {
    if (!shouldPromoteForeignKeyAfterSchemaControllerResult(result)) return;
    const constraint_name = resultForeignKeyConstraintName(result) orelse return;
    const schema_json = (try loadLocalTableSchemaJson(alloc, db)) orelse return;
    defer alloc.free(schema_json);
    const enforced_schema_json = try sql_schema_mutation.schemaWithForeignKeyValidationStateAlloc(
        alloc,
        schema_json,
        constraint_name,
        .enforced,
    );
    defer alloc.free(enforced_schema_json);
    try applyLocalTableSchemaJson(alloc, db, enforced_schema_json);
}

pub fn runCatalogForeignKeyIntegritySchemaControllerMaintenancePass(
    alloc: std.mem.Allocator,
    source: TableWriteSource,
    catalog: table_catalog.CatalogSource,
    options: ForeignKeyIntegritySchemaControllerOptions,
) !ForeignKeyIntegritySchemaControllerResult {
    try validateForeignKeyIntegritySchemaControllerOptions(options);
    var snapshot = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&snapshot);

    var summary = ForeignKeyIntegritySchemaControllerResult{};
    var results = std.ArrayListUnmanaged(ForeignKeyIntegritySchemaControllerTableResult).empty;
    var action_schedules = std.ArrayListUnmanaged(ForeignKeyActionScheduleStatus).empty;
    var action_jobs = std.ArrayListUnmanaged(ForeignKeyActionJobStatus).empty;
    errdefer {
        for (results.items) |*result| result.deinit(alloc);
        results.deinit(alloc);
        for (action_schedules.items) |*schedule| schedule.deinit(alloc);
        action_schedules.deinit(alloc);
        for (action_jobs.items) |*job| job.deinit(alloc);
        action_jobs.deinit(alloc);
    }

    for (snapshot.tables) |table| {
        if (table.schema_json.len == 0) continue;
        summary.tables_scanned += 1;
        if (!(try tableSchemaHasForeignKeysAlloc(alloc, table.schema_json))) continue;
        try runForeignKeyIntegritySchemaControllerMaintenanceForTable(
            alloc,
            source,
            table.name,
            table.schema_json,
            options,
            &summary,
            &results,
        );
        try runForeignKeyIntegrityJobControllerMaintenanceForTable(
            alloc,
            source,
            table.name,
            options,
            &summary,
            &results,
        );
        try runForeignKeyActionScheduleControllerMaintenanceForTable(
            alloc,
            source,
            table.name,
            options,
            &summary,
            &action_schedules,
        );
        try runForeignKeyActionJobControllerMaintenanceForTable(
            alloc,
            source,
            table.name,
            options,
            &summary,
            &action_jobs,
        );
        if (summary.tables_executed >= options.max_tables and summary.jobs_executed >= options.max_jobs and summary.action_schedules_executed >= options.max_action_jobs and summary.action_jobs_executed >= options.max_action_jobs) break;
    }

    return try finalizeForeignKeyIntegritySchemaControllerMaintenanceResult(alloc, summary, &results, &action_schedules, &action_jobs);
}

pub fn runLocalForeignKeyIntegritySchemaControllerMaintenancePass(
    alloc: std.mem.Allocator,
    source: TableWriteSource,
    db: *db_mod.DB,
    table_name: []const u8,
    options: ForeignKeyIntegritySchemaControllerOptions,
) !ForeignKeyIntegritySchemaControllerResult {
    try validateForeignKeyIntegritySchemaControllerOptions(options);
    var summary = ForeignKeyIntegritySchemaControllerResult{};
    var results = std.ArrayListUnmanaged(ForeignKeyIntegritySchemaControllerTableResult).empty;
    var action_schedules = std.ArrayListUnmanaged(ForeignKeyActionScheduleStatus).empty;
    var action_jobs = std.ArrayListUnmanaged(ForeignKeyActionJobStatus).empty;
    errdefer {
        for (results.items) |*result| result.deinit(alloc);
        results.deinit(alloc);
        for (action_schedules.items) |*schedule| schedule.deinit(alloc);
        action_schedules.deinit(alloc);
        for (action_jobs.items) |*job| job.deinit(alloc);
        action_jobs.deinit(alloc);
    }

    const schema_json = (try loadLocalTableSchemaJson(alloc, db)) orelse {
        return try finalizeForeignKeyIntegritySchemaControllerMaintenanceResult(alloc, summary, &results, &action_schedules, &action_jobs);
    };
    defer alloc.free(schema_json);

    if (schema_json.len > 0) {
        summary.tables_scanned = 1;
        if (try tableSchemaHasForeignKeysAlloc(alloc, schema_json)) {
            try runForeignKeyIntegritySchemaControllerMaintenanceForTable(
                alloc,
                source,
                table_name,
                schema_json,
                options,
                &summary,
                &results,
            );
            try runForeignKeyIntegrityJobControllerMaintenanceForTable(
                alloc,
                source,
                table_name,
                options,
                &summary,
                &results,
            );
            try runForeignKeyActionScheduleControllerMaintenanceForTable(
                alloc,
                source,
                table_name,
                options,
                &summary,
                &action_schedules,
            );
            try runForeignKeyActionJobControllerMaintenanceForTable(
                alloc,
                source,
                table_name,
                options,
                &summary,
                &action_jobs,
            );
            for (results.items) |entry| {
                if (!entry.schema_adoption) continue;
                try promoteLocalForeignKeyAfterSchemaControllerResult(
                    alloc,
                    db,
                    entry.result,
                );
            }
        }
    }

    return try finalizeForeignKeyIntegritySchemaControllerMaintenanceResult(alloc, summary, &results, &action_schedules, &action_jobs);
}

pub fn runCatalogUniqueConstraintIntegritySchemaControllerMaintenancePass(
    alloc: std.mem.Allocator,
    source: TableWriteSource,
    catalog: table_catalog.CatalogSource,
    options: UniqueConstraintIntegritySchemaControllerOptions,
) !UniqueConstraintIntegritySchemaControllerResult {
    try validateUniqueConstraintIntegritySchemaControllerOptions(options);
    var snapshot = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&snapshot);

    var summary = UniqueConstraintIntegritySchemaControllerResult{};
    var results = std.ArrayListUnmanaged(UniqueConstraintIntegritySchemaControllerTableResult).empty;
    errdefer {
        for (results.items) |*result| result.deinit(alloc);
        results.deinit(alloc);
    }

    for (snapshot.tables) |table| {
        if (table.schema_json.len == 0) continue;
        summary.tables_scanned += 1;
        if (!(try tableSchemaHasUniqueConstraintsAlloc(alloc, table.schema_json))) continue;
        const first_result_index = results.items.len;
        try runUniqueConstraintIntegritySchemaControllerMaintenanceForTable(
            alloc,
            source,
            table.name,
            table.schema_json,
            options,
            &summary,
            &results,
        );
        for (results.items[first_result_index..]) |entry| {
            if (!entry.schema_adoption) continue;
            if (!shouldPromoteUniqueConstraintAfterSchemaControllerResult(entry.result)) continue;
            _ = try table_catalog.promoteUniqueConstraintEnforced(alloc, catalog, entry.table_name, entry.constraint_name);
        }
        if (summary.tables_executed >= options.max_tables) break;
    }

    return try finalizeUniqueConstraintIntegritySchemaControllerMaintenanceResult(alloc, summary, &results);
}

pub fn runLocalUniqueConstraintIntegritySchemaControllerMaintenancePass(
    alloc: std.mem.Allocator,
    source: TableWriteSource,
    db: *db_mod.DB,
    table_name: []const u8,
    options: UniqueConstraintIntegritySchemaControllerOptions,
) !UniqueConstraintIntegritySchemaControllerResult {
    try validateUniqueConstraintIntegritySchemaControllerOptions(options);
    var summary = UniqueConstraintIntegritySchemaControllerResult{};
    var results = std.ArrayListUnmanaged(UniqueConstraintIntegritySchemaControllerTableResult).empty;
    errdefer {
        for (results.items) |*result| result.deinit(alloc);
        results.deinit(alloc);
    }

    const schema_json = (try loadLocalTableSchemaJson(alloc, db)) orelse {
        return try finalizeUniqueConstraintIntegritySchemaControllerMaintenanceResult(alloc, summary, &results);
    };
    defer alloc.free(schema_json);

    if (schema_json.len > 0) {
        summary.tables_scanned = 1;
        if (try tableSchemaHasUniqueConstraintsAlloc(alloc, schema_json)) {
            try runUniqueConstraintIntegritySchemaControllerMaintenanceForTable(
                alloc,
                source,
                table_name,
                schema_json,
                options,
                &summary,
                &results,
            );
            for (results.items) |entry| {
                if (!entry.schema_adoption) continue;
                try promoteLocalUniqueConstraintAfterSchemaControllerResult(
                    alloc,
                    db,
                    entry.constraint_name,
                    entry.result,
                );
            }
        }
    }

    return try finalizeUniqueConstraintIntegritySchemaControllerMaintenanceResult(alloc, summary, &results);
}

pub fn cloneOptionalString(alloc: std.mem.Allocator, value: ?[]const u8) !?[]u8 {
    return if (value) |text| try alloc.dupe(u8, text) else null;
}

pub fn foreignKeyActionJobStatusFromDbRecord(
    alloc: std.mem.Allocator,
    group_id: u64,
    record: db_mod.DB.ForeignKeyActionJobRecord,
) !ForeignKeyActionJobStatus {
    var status = ForeignKeyActionJobStatus{
        .group_id = group_id,
        .version = record.version,
        .job_id = &.{},
        .action = &.{},
        .worker_id = &.{},
        .constraint_name = &.{},
        .parent_table = &.{},
        .parent_key = &.{},
        .updated_parent_key = null,
        .page_limit = record.page_limit,
        .status = &.{},
        .created_at_ns = record.created_at_ns,
        .updated_at_ns = record.updated_at_ns,
        .claimed_at_ns = record.claimed_at_ns,
        .lease_until_ns = record.lease_until_ns,
        .attempts = record.attempts,
        .completed = record.completed,
        .applied_children = record.applied_children,
        .failure_count = record.failure_count,
        .first_failed_at_ns = record.first_failed_at_ns,
        .last_failed_at_ns = record.last_failed_at_ns,
        .requeue_count = record.requeue_count,
        .last_requeued_at_ns = record.last_requeued_at_ns,
        .cascade_depth = record.cascade_depth,
        .cascade_max_depth = record.cascade_max_depth,
    };
    errdefer status.deinit(alloc);
    status.job_id = try alloc.dupe(u8, record.job_id);
    status.action = try alloc.dupe(u8, record.action);
    status.worker_id = try alloc.dupe(u8, record.worker_id);
    status.constraint_name = try alloc.dupe(u8, record.constraint_name);
    status.parent_table = try alloc.dupe(u8, record.parent_table);
    status.parent_key = try alloc.dupe(u8, record.parent_key);
    status.updated_parent_key = try cloneOptionalString(alloc, record.updated_parent_key);
    status.status = try alloc.dupe(u8, record.status);
    status.next_child_table = try cloneOptionalString(alloc, record.next_child_table);
    status.next_child_key = try cloneOptionalString(alloc, record.next_child_key);
    status.last_error = try cloneOptionalString(alloc, record.last_error);
    return status;
}

pub fn cloneForeignKeyActionJobStatus(
    alloc: std.mem.Allocator,
    source: ForeignKeyActionJobStatus,
) !ForeignKeyActionJobStatus {
    var status = ForeignKeyActionJobStatus{
        .group_id = source.group_id,
        .version = source.version,
        .job_id = &.{},
        .action = &.{},
        .worker_id = &.{},
        .constraint_name = &.{},
        .parent_table = &.{},
        .parent_key = &.{},
        .updated_parent_key = null,
        .page_limit = source.page_limit,
        .status = &.{},
        .created_at_ns = source.created_at_ns,
        .updated_at_ns = source.updated_at_ns,
        .claimed_at_ns = source.claimed_at_ns,
        .lease_until_ns = source.lease_until_ns,
        .attempts = source.attempts,
        .completed = source.completed,
        .applied_children = source.applied_children,
        .failure_count = source.failure_count,
        .first_failed_at_ns = source.first_failed_at_ns,
        .last_failed_at_ns = source.last_failed_at_ns,
        .requeue_count = source.requeue_count,
        .last_requeued_at_ns = source.last_requeued_at_ns,
        .cascade_depth = source.cascade_depth,
        .cascade_max_depth = source.cascade_max_depth,
    };
    errdefer status.deinit(alloc);
    status.job_id = try alloc.dupe(u8, source.job_id);
    status.action = try alloc.dupe(u8, source.action);
    status.worker_id = try alloc.dupe(u8, source.worker_id);
    status.constraint_name = try alloc.dupe(u8, source.constraint_name);
    status.parent_table = try alloc.dupe(u8, source.parent_table);
    status.parent_key = try alloc.dupe(u8, source.parent_key);
    status.updated_parent_key = try cloneOptionalString(alloc, source.updated_parent_key);
    status.status = try alloc.dupe(u8, source.status);
    status.next_child_table = try cloneOptionalString(alloc, source.next_child_table);
    status.next_child_key = try cloneOptionalString(alloc, source.next_child_key);
    status.last_error = try cloneOptionalString(alloc, source.last_error);
    return status;
}

pub fn foreignKeyActionJobProgressFromDbRecords(
    alloc: std.mem.Allocator,
    group_id: u64,
    records: []const db_mod.DB.ForeignKeyActionJobRecord,
) !ForeignKeyActionJobProgressResult {
    var jobs = try alloc.alloc(ForeignKeyActionJobStatus, records.len);
    var filled: usize = 0;
    errdefer {
        for (jobs[0..filled]) |*job| job.deinit(alloc);
        alloc.free(jobs);
    }
    for (records, 0..) |record, i| {
        jobs[i] = try foreignKeyActionJobStatusFromDbRecord(alloc, group_id, record);
        filled += 1;
    }
    return .{ .jobs = jobs };
}

pub fn foreignKeyActionScheduleStatusFromDbRecord(
    alloc: std.mem.Allocator,
    group_id: u64,
    record: db_mod.DB.ForeignKeyActionScheduleRecord,
) !ForeignKeyActionScheduleStatus {
    var status = ForeignKeyActionScheduleStatus{
        .group_id = group_id,
        .version = record.version,
        .schedule_id = &.{},
        .action_job_id = &.{},
        .action = &.{},
        .worker_id = &.{},
        .constraint_name = &.{},
        .parent_table = &.{},
        .parent_key = &.{},
        .updated_parent_key = null,
        .page_limit = record.page_limit,
        .status = &.{},
        .created_at_ns = record.created_at_ns,
        .updated_at_ns = record.updated_at_ns,
        .completed = record.completed,
        .scheduled_groups = record.scheduled_groups,
        .cascade_depth = record.cascade_depth,
        .cascade_max_depth = record.cascade_max_depth,
        .requeue_count = record.requeue_count,
        .last_requeued_at_ns = record.last_requeued_at_ns,
        .last_error = null,
    };
    errdefer status.deinit(alloc);
    status.schedule_id = try alloc.dupe(u8, record.schedule_id);
    status.action_job_id = try alloc.dupe(u8, record.action_job_id);
    status.action = try alloc.dupe(u8, record.action);
    status.worker_id = try alloc.dupe(u8, record.worker_id);
    status.constraint_name = try alloc.dupe(u8, record.constraint_name);
    status.parent_table = try alloc.dupe(u8, record.parent_table);
    status.parent_key = try alloc.dupe(u8, record.parent_key);
    status.updated_parent_key = try cloneOptionalString(alloc, record.updated_parent_key);
    status.status = try alloc.dupe(u8, record.status);
    if (record.last_error) |value| status.last_error = try alloc.dupe(u8, value);
    return status;
}

pub fn foreignKeyActionScheduleProgressFromDbRecords(
    alloc: std.mem.Allocator,
    group_id: u64,
    records: []const db_mod.DB.ForeignKeyActionScheduleRecord,
) !ForeignKeyActionScheduleProgressResult {
    var schedules = try alloc.alloc(ForeignKeyActionScheduleStatus, records.len);
    var initialized: usize = 0;
    errdefer {
        for (schedules[0..initialized]) |*schedule| schedule.deinit(alloc);
        if (schedules.len > 0) alloc.free(schedules);
    }
    for (records, 0..) |record, i| {
        schedules[i] = try foreignKeyActionScheduleStatusFromDbRecord(alloc, group_id, record);
        initialized += 1;
    }
    return .{ .schedules = schedules };
}

pub fn appendForeignKeyActionJobProgressFromDb(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(ForeignKeyActionJobStatus),
    db: *db_mod.DB,
    group_id: u64,
) !void {
    const records = try db.listForeignKeyActionJobRecords();
    defer db.freeForeignKeyActionJobRecords(records);
    try out.ensureUnusedCapacity(alloc, records.len);
    for (records) |record| {
        out.appendAssumeCapacity(try foreignKeyActionJobStatusFromDbRecord(alloc, group_id, record));
    }
}

pub fn appendForeignKeyActionJobStatuses(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(ForeignKeyActionJobStatus),
    statuses: []const ForeignKeyActionJobStatus,
) !void {
    try out.ensureUnusedCapacity(alloc, statuses.len);
    for (statuses) |status| {
        out.appendAssumeCapacity(try cloneForeignKeyActionJobStatus(alloc, status));
    }
}

pub fn appendForeignKeyActionJobStatusFromProgressByJobId(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(ForeignKeyActionJobStatus),
    progress: ForeignKeyActionJobProgressResult,
    job_id: []const u8,
) !usize {
    var appended: usize = 0;
    for (progress.jobs) |status| {
        if (!std.mem.eql(u8, status.job_id, job_id)) continue;
        try out.append(alloc, try cloneForeignKeyActionJobStatus(alloc, status));
        appended += 1;
    }
    return appended;
}

pub fn countInvalidForeignKeyActionJobStatusesByJobId(
    statuses: []const ForeignKeyActionJobStatus,
    job_id: []const u8,
) usize {
    var count: usize = 0;
    for (statuses) |status| {
        if (!std.mem.eql(u8, status.job_id, job_id)) continue;
        if (std.mem.eql(u8, status.status, "invalid")) count += 1;
    }
    return count;
}

pub fn countInvalidForeignKeyActionJobStatuses(statuses: []const ForeignKeyActionJobStatus) usize {
    var count: usize = 0;
    for (statuses) |status| {
        if (std.mem.eql(u8, status.status, "invalid")) count += 1;
    }
    return count;
}

pub fn cloneForeignKeyActionScheduleStatus(
    alloc: std.mem.Allocator,
    source: ForeignKeyActionScheduleStatus,
) !ForeignKeyActionScheduleStatus {
    var status = ForeignKeyActionScheduleStatus{
        .group_id = source.group_id,
        .version = source.version,
        .schedule_id = &.{},
        .action_job_id = &.{},
        .action = &.{},
        .worker_id = &.{},
        .constraint_name = &.{},
        .parent_table = &.{},
        .parent_key = &.{},
        .updated_parent_key = null,
        .page_limit = source.page_limit,
        .status = &.{},
        .created_at_ns = source.created_at_ns,
        .updated_at_ns = source.updated_at_ns,
        .completed = source.completed,
        .scheduled_groups = source.scheduled_groups,
        .cascade_depth = source.cascade_depth,
        .cascade_max_depth = source.cascade_max_depth,
        .requeue_count = source.requeue_count,
        .last_requeued_at_ns = source.last_requeued_at_ns,
        .last_error = null,
    };
    errdefer status.deinit(alloc);
    status.schedule_id = try alloc.dupe(u8, source.schedule_id);
    status.action_job_id = try alloc.dupe(u8, source.action_job_id);
    status.action = try alloc.dupe(u8, source.action);
    status.worker_id = try alloc.dupe(u8, source.worker_id);
    status.constraint_name = try alloc.dupe(u8, source.constraint_name);
    status.parent_table = try alloc.dupe(u8, source.parent_table);
    status.parent_key = try alloc.dupe(u8, source.parent_key);
    status.updated_parent_key = try cloneOptionalString(alloc, source.updated_parent_key);
    status.status = try alloc.dupe(u8, source.status);
    if (source.last_error) |value| status.last_error = try alloc.dupe(u8, value);
    return status;
}

pub fn appendForeignKeyActionScheduleStatuses(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(ForeignKeyActionScheduleStatus),
    statuses: []const ForeignKeyActionScheduleStatus,
) !void {
    try out.ensureUnusedCapacity(alloc, statuses.len);
    for (statuses) |status| {
        out.appendAssumeCapacity(try cloneForeignKeyActionScheduleStatus(alloc, status));
    }
}

pub fn cloneForeignKeyIntegrityViolation(
    alloc: std.mem.Allocator,
    group_id: u64,
    violation: db_mod.relational_store.ForeignKeyIntegrityViolation,
) !ForeignKeyIntegrityViolation {
    var cloned = ForeignKeyIntegrityViolation{
        .group_id = group_id,
        .kind = violation.kind,
        .constraint_name = try alloc.dupe(u8, violation.constraint_name),
        .child_table = &.{},
        .child_key = &.{},
        .parent_table = &.{},
        .parent_key = &.{},
        .parent_values = &.{},
        .observed_parent_key = null,
        .observed_parent_values = &.{},
    };
    errdefer cloned.deinit(alloc);
    cloned.child_table = try alloc.dupe(u8, violation.child_table);
    cloned.child_key = try alloc.dupe(u8, violation.child_key);
    cloned.parent_table = try alloc.dupe(u8, violation.parent_table);
    cloned.parent_key = try alloc.dupe(u8, violation.parent_key);
    cloned.parent_values = try cloneStorageForeignKeyIntegrityTupleValues(alloc, violation.parent_values);
    if (violation.observed_parent_key) |value| cloned.observed_parent_key = try alloc.dupe(u8, value);
    cloned.observed_parent_values = try cloneStorageForeignKeyIntegrityTupleValues(alloc, violation.observed_parent_values);
    return cloned;
}

pub fn cloneForeignKeyIntegrityResultViolation(
    alloc: std.mem.Allocator,
    violation: ForeignKeyIntegrityViolation,
) !ForeignKeyIntegrityViolation {
    var cloned = ForeignKeyIntegrityViolation{
        .group_id = violation.group_id,
        .kind = violation.kind,
        .constraint_name = try alloc.dupe(u8, violation.constraint_name),
        .child_table = &.{},
        .child_key = &.{},
        .parent_table = &.{},
        .parent_key = &.{},
        .parent_values = &.{},
        .observed_parent_key = null,
        .observed_parent_values = &.{},
    };
    errdefer cloned.deinit(alloc);
    cloned.child_table = try alloc.dupe(u8, violation.child_table);
    cloned.child_key = try alloc.dupe(u8, violation.child_key);
    cloned.parent_table = try alloc.dupe(u8, violation.parent_table);
    cloned.parent_key = try alloc.dupe(u8, violation.parent_key);
    cloned.parent_values = try cloneForeignKeyIntegrityTupleValues(alloc, violation.parent_values);
    if (violation.observed_parent_key) |value| cloned.observed_parent_key = try alloc.dupe(u8, value);
    cloned.observed_parent_values = try cloneForeignKeyIntegrityTupleValues(alloc, violation.observed_parent_values);
    return cloned;
}

pub fn cloneForeignKeyIntegrityResultViolations(
    alloc: std.mem.Allocator,
    violations: []const ForeignKeyIntegrityViolation,
) ![]ForeignKeyIntegrityViolation {
    var cloned = try alloc.alloc(ForeignKeyIntegrityViolation, violations.len);
    var initialized: usize = 0;
    errdefer {
        for (cloned[0..initialized]) |*violation| violation.deinit(alloc);
        if (cloned.len > 0) alloc.free(cloned);
    }
    for (violations, 0..) |violation, i| {
        cloned[i] = try cloneForeignKeyIntegrityResultViolation(alloc, violation);
        initialized += 1;
    }
    return cloned;
}

pub fn cloneForeignKeyIntegrityProgress(
    alloc: std.mem.Allocator,
    progress: ForeignKeyIntegrityProgress,
) !ForeignKeyIntegrityProgress {
    var cloned = ForeignKeyIntegrityProgress{
        .group_id = progress.group_id,
        .version = progress.version,
        .phase = try alloc.dupe(u8, if (progress.phase.len > 0) progress.phase else "child_range"),
        .mode = try alloc.dupe(u8, progress.mode),
        .constraint_name = null,
        .lower_doc_key = &.{},
        .upper_doc_key = &.{},
        .completed = progress.completed,
        .valid = progress.valid,
        .updated_at_ns = progress.updated_at_ns,
        .report = progress.report,
    };
    errdefer cloned.deinit(alloc);
    if (progress.constraint_name) |value| cloned.constraint_name = try alloc.dupe(u8, value);
    cloned.lower_doc_key = try alloc.dupe(u8, progress.lower_doc_key);
    cloned.upper_doc_key = try alloc.dupe(u8, progress.upper_doc_key);
    return cloned;
}

pub fn cloneForeignKeyIntegrityProgressSlice(
    alloc: std.mem.Allocator,
    entries: []const ForeignKeyIntegrityProgress,
) ![]ForeignKeyIntegrityProgress {
    var cloned = try alloc.alloc(ForeignKeyIntegrityProgress, entries.len);
    var initialized: usize = 0;
    errdefer {
        for (cloned[0..initialized]) |*entry| entry.deinit(alloc);
        if (cloned.len > 0) alloc.free(cloned);
    }
    for (entries, 0..) |entry, i| {
        cloned[i] = try cloneForeignKeyIntegrityProgress(alloc, entry);
        initialized += 1;
    }
    return cloned;
}

pub fn cloneForeignKeyIntegrityProgressRecord(
    alloc: std.mem.Allocator,
    group_id: u64,
    progress: db_mod.DB.ForeignKeyIntegrityProgressRecord,
) !ForeignKeyIntegrityProgress {
    var cloned = ForeignKeyIntegrityProgress{
        .group_id = group_id,
        .version = progress.version,
        .phase = try alloc.dupe(u8, progress.phase),
        .mode = try alloc.dupe(u8, progress.mode),
        .constraint_name = null,
        .lower_doc_key = &.{},
        .upper_doc_key = &.{},
        .completed = progress.completed,
        .valid = progress.valid,
        .updated_at_ns = progress.updated_at_ns,
        .report = progress.report,
    };
    errdefer cloned.deinit(alloc);
    if (progress.constraint_name) |value| cloned.constraint_name = try alloc.dupe(u8, value);
    cloned.lower_doc_key = try alloc.dupe(u8, progress.lower_doc_key);
    cloned.upper_doc_key = try alloc.dupe(u8, progress.upper_doc_key);
    return cloned;
}

pub fn cloneForeignKeyIntegrityWorkUnit(
    alloc: std.mem.Allocator,
    unit: ForeignKeyIntegrityWorkUnit,
) !ForeignKeyIntegrityWorkUnit {
    var cloned = ForeignKeyIntegrityWorkUnit{
        .group_id = unit.group_id,
        .phase = try alloc.dupe(u8, unit.phase),
        .planned_action = &.{},
        .constraint_name = null,
        .lower_doc_key = &.{},
        .upper_doc_key = &.{},
    };
    errdefer cloned.deinit(alloc);
    cloned.planned_action = try alloc.dupe(u8, unit.planned_action);
    if (unit.constraint_name) |value| cloned.constraint_name = try alloc.dupe(u8, value);
    cloned.lower_doc_key = try alloc.dupe(u8, unit.lower_doc_key);
    cloned.upper_doc_key = try alloc.dupe(u8, unit.upper_doc_key);
    return cloned;
}

pub fn cloneForeignKeyIntegrityWorkUnits(
    alloc: std.mem.Allocator,
    units: []const ForeignKeyIntegrityWorkUnit,
) ![]ForeignKeyIntegrityWorkUnit {
    var cloned = try alloc.alloc(ForeignKeyIntegrityWorkUnit, units.len);
    var initialized: usize = 0;
    errdefer {
        for (cloned[0..initialized]) |*unit| unit.deinit(alloc);
        if (cloned.len > 0) alloc.free(cloned);
    }
    for (units, 0..) |unit, i| {
        cloned[i] = try cloneForeignKeyIntegrityWorkUnit(alloc, unit);
        initialized += 1;
    }
    return cloned;
}

pub fn foreignKeyIntegrityPlannedActionName(action: ForeignKeyIntegrityAction) []const u8 {
    return switch (action) {
        .plan, .validate, .progress => "validate",
        .dry_run => "dry_run",
        .repair => "repair",
        .list => "list",
        .explain_delete => "explain_delete",
    };
}

pub fn stableForeignKeyIntegrityJobIdAlloc(
    alloc: std.mem.Allocator,
    table_name: []const u8,
    action: ForeignKeyIntegrityAction,
    constraint_name: ?[]const u8,
    lower_doc_key: []const u8,
    upper_doc_key: []const u8,
) ![]u8 {
    var hasher = std.hash.Wyhash.init(0);
    hasher.update("foreign-key-integrity");
    hasher.update(&.{0});
    hasher.update(table_name);
    hasher.update(&.{0});
    hasher.update(foreignKeyIntegrityPlannedActionName(action));
    hasher.update(&.{0});
    if (constraint_name) |value| hasher.update(value);
    hasher.update(&.{0});
    hasher.update(lower_doc_key);
    hasher.update(&.{0});
    hasher.update(upper_doc_key);
    const hash = hasher.final();
    return try std.fmt.allocPrint(alloc, "fk-integrity:{x}", .{if (hash == 0) 1 else hash});
}

pub fn foreignKeyIntegrityProgressModeNameForPlannedAction(planned_action: []const u8) ?[]const u8 {
    if (std.mem.eql(u8, planned_action, "validate") or std.mem.eql(u8, planned_action, "list")) return "validate";
    if (std.mem.eql(u8, planned_action, "dry_run")) return "dry_run";
    if (std.mem.eql(u8, planned_action, "repair")) return "repair";
    return null;
}

pub fn startForeignKeyIntegrityJobOnDb(
    db: *db_mod.DB,
    job_id: ?[]const u8,
    table_name: []const u8,
    action: ForeignKeyIntegrityAction,
    worker_id: []const u8,
    constraint_name: ?[]const u8,
    lower_doc_key: []const u8,
    upper_doc_key: []const u8,
    lease_ms: u64,
    max_work_units: usize,
) !void {
    const id = job_id orelse return;
    if (id.len == 0) return error.InvalidForeignKeyIntegrityRequest;
    const record = try db.upsertForeignKeyIntegrityJobRecord(
        id,
        table_name,
        foreignKeyIntegrityPlannedActionName(action),
        worker_id,
        constraint_name,
        lower_doc_key,
        upper_doc_key,
        lease_ms,
        max_work_units,
        "running",
    );
    defer db.freeForeignKeyIntegrityJobRecord(record);
}

const foreign_key_integrity_job_violation_sample_limit: usize = 100;

pub const ForeignKeyIntegrityJobDiagnostics = struct {
    samples_json: []u8,
    sample_count: usize,
    truncated: bool,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.samples_json);
        self.* = undefined;
    }
};

pub fn foreignKeyIntegrityJobDiagnosticsAlloc(
    alloc: std.mem.Allocator,
    existing: ?db_mod.DB.ForeignKeyIntegrityJobRecord,
    result: ForeignKeyIntegrityResult,
) !ForeignKeyIntegrityJobDiagnostics {
    var samples = std.ArrayListUnmanaged(ForeignKeyIntegrityViolation).empty;
    errdefer {
        for (samples.items) |*violation| violation.deinit(alloc);
        samples.deinit(alloc);
    }
    var truncated = result.violations_truncated;

    if (existing) |record| {
        truncated = truncated or record.violations_truncated;
        if (record.violation_samples_json.len > 0 and !std.mem.eql(u8, record.violation_samples_json, "[]")) {
            var parsed = try std.json.parseFromSlice([]ForeignKeyIntegrityViolation, alloc, record.violation_samples_json, .{
                .allocate = .alloc_always,
                .ignore_unknown_fields = true,
            });
            defer parsed.deinit();
            for (parsed.value) |violation| {
                if (samples.items.len >= foreign_key_integrity_job_violation_sample_limit) {
                    truncated = true;
                    break;
                }
                if (foreignKeyIntegrityViolationSamplesContain(samples.items, violation)) continue;
                try samples.append(alloc, try cloneForeignKeyIntegrityResultViolation(alloc, violation));
            }
        }
    }

    for (result.violations) |violation| {
        if (foreignKeyIntegrityViolationSamplesContain(samples.items, violation)) continue;
        if (samples.items.len >= foreign_key_integrity_job_violation_sample_limit) {
            truncated = true;
            break;
        }
        try samples.append(alloc, try cloneForeignKeyIntegrityResultViolation(alloc, violation));
    }

    const samples_json = try std.json.Stringify.valueAlloc(
        alloc,
        samples.items,
        .{ .emit_null_optional_fields = false },
    );
    const sample_count = samples.items.len;
    for (samples.items) |*violation| violation.deinit(alloc);
    samples.deinit(alloc);
    return .{
        .samples_json = samples_json,
        .sample_count = sample_count,
        .truncated = truncated,
    };
}

pub fn foreignKeyIntegrityViolationSamplesContain(
    samples: []const ForeignKeyIntegrityViolation,
    needle: ForeignKeyIntegrityViolation,
) bool {
    for (samples) |sample| {
        if (sample.group_id != needle.group_id) continue;
        if (sample.kind != needle.kind) continue;
        if (!std.mem.eql(u8, sample.constraint_name, needle.constraint_name)) continue;
        if (!std.mem.eql(u8, sample.child_table, needle.child_table)) continue;
        if (!std.mem.eql(u8, sample.child_key, needle.child_key)) continue;
        if (!std.mem.eql(u8, sample.parent_table, needle.parent_table)) continue;
        if (!std.mem.eql(u8, sample.parent_key, needle.parent_key)) continue;
        if (!foreignKeyIntegrityTupleValuesEqual(sample.parent_values, needle.parent_values)) continue;
        if (!optionalStringsEqual(sample.observed_parent_key, needle.observed_parent_key)) continue;
        if (!foreignKeyIntegrityTupleValuesEqual(sample.observed_parent_values, needle.observed_parent_values)) continue;
        return true;
    }
    return false;
}

pub fn foreignKeyIntegrityTupleValuesEqual(
    a: []const ForeignKeyIntegrityTupleValue,
    b: []const ForeignKeyIntegrityTupleValue,
) bool {
    if (a.len != b.len) return false;
    for (a, b) |left, right| {
        if (!std.mem.eql(u8, left.column, right.column)) return false;
        if (!std.mem.eql(u8, left.value, right.value)) return false;
    }
    return true;
}

pub fn optionalStringsEqual(a: ?[]const u8, b: ?[]const u8) bool {
    if (a == null or b == null) return a == null and b == null;
    return std.mem.eql(u8, a.?, b.?);
}

pub fn finishForeignKeyIntegrityJobOnDb(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    job_id: ?[]const u8,
    result: ForeignKeyIntegrityResult,
) !void {
    const id = job_id orelse return;
    const existing = try db.loadForeignKeyIntegrityJobRecord(id);
    defer if (existing) |record| db.freeForeignKeyIntegrityJobRecord(record);
    var diagnostics = try foreignKeyIntegrityJobDiagnosticsAlloc(alloc, existing, result);
    defer diagnostics.deinit(alloc);

    const record = if (result.complete) blk: {
        const status = if (result.valid) "complete" else "invalid";
        break :blk try db.completeForeignKeyIntegrityJobRecordWithDiagnostics(
            id,
            status,
            result.valid,
            result.report,
            diagnostics.samples_json,
            diagnostics.sample_count,
            diagnostics.truncated,
        );
    } else try db.updateForeignKeyIntegrityJobDiagnosticsWithReport(
        id,
        result.report,
        diagnostics.samples_json,
        diagnostics.sample_count,
        diagnostics.truncated,
    );
    defer db.freeForeignKeyIntegrityJobRecord(record);
}

pub fn hydrateForeignKeyIntegrityResultDiagnosticsFromJobOnDb(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    group_id: u64,
    result: *ForeignKeyIntegrityResult,
) !void {
    const id = result.job_id orelse return;
    const job = (try db.loadForeignKeyIntegrityJobRecord(id)) orelse return;
    defer db.freeForeignKeyIntegrityJobRecord(job);

    const violations = try decodeForeignKeyIntegrityJobViolationSamples(alloc, job.violation_samples_json);
    errdefer {
        for (violations) |*violation| violation.deinit(alloc);
        if (violations.len > 0) alloc.free(violations);
    }
    const jobs = try alloc.alloc(ForeignKeyIntegrityJobStatus, 1);
    var jobs_initialized: usize = 0;
    errdefer {
        for (jobs[0..jobs_initialized]) |*entry| entry.deinit(alloc);
        alloc.free(jobs);
    }
    jobs[0] = try cloneForeignKeyIntegrityJobRecord(alloc, group_id, job);
    jobs_initialized = 1;

    for (result.violations) |*violation| violation.deinit(alloc);
    if (result.violations.len > 0) alloc.free(result.violations);
    for (result.jobs) |*entry| entry.deinit(alloc);
    if (result.jobs.len > 0) alloc.free(result.jobs);
    result.report = job.aggregate_report;
    result.violations = violations;
    result.violations_truncated = job.violations_truncated;
    result.jobs = jobs;
}

pub fn attachForeignKeyIntegrityJobId(
    alloc: std.mem.Allocator,
    result: *ForeignKeyIntegrityResult,
    job_id: ?[]const u8,
) !void {
    const id = job_id orelse return;
    if (id.len == 0) return error.InvalidForeignKeyIntegrityRequest;
    if (result.job_id) |existing| alloc.free(existing);
    result.job_id = try alloc.dupe(u8, id);
}

pub fn decodeForeignKeyIntegrityJobViolationSamples(
    alloc: std.mem.Allocator,
    samples_json: []const u8,
) ![]ForeignKeyIntegrityViolation {
    if (samples_json.len == 0 or std.mem.eql(u8, samples_json, "[]")) {
        return try alloc.alloc(ForeignKeyIntegrityViolation, 0);
    }
    var parsed = try std.json.parseFromSlice([]ForeignKeyIntegrityViolation, alloc, samples_json, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = true,
    });
    defer parsed.deinit();
    return try cloneForeignKeyIntegrityResultViolations(alloc, parsed.value);
}

pub fn cloneForeignKeyIntegrityJobStatus(
    alloc: std.mem.Allocator,
    job: ForeignKeyIntegrityJobStatus,
) !ForeignKeyIntegrityJobStatus {
    var cloned = ForeignKeyIntegrityJobStatus{
        .group_id = job.group_id,
        .version = job.version,
        .job_id = try alloc.dupe(u8, job.job_id),
        .table_name = &.{},
        .action = &.{},
        .worker_id = &.{},
        .constraint_name = null,
        .lower_doc_key = &.{},
        .upper_doc_key = &.{},
        .lease_ms = job.lease_ms,
        .max_work_units = job.max_work_units,
        .status = &.{},
        .created_at_ns = job.created_at_ns,
        .updated_at_ns = job.updated_at_ns,
        .attempts = job.attempts,
        .completed = job.completed,
        .valid = job.valid,
        .last_report = job.last_report,
        .aggregate_report = job.aggregate_report,
        .violation_sample_count = job.violation_sample_count,
        .violations_truncated = job.violations_truncated,
        .diagnostic_passes = job.diagnostic_passes,
        .violating_passes = job.violating_passes,
        .first_violation_at_ns = job.first_violation_at_ns,
        .last_violation_at_ns = job.last_violation_at_ns,
        .violation_samples = &.{},
    };
    errdefer cloned.deinit(alloc);
    cloned.table_name = try alloc.dupe(u8, job.table_name);
    cloned.action = try alloc.dupe(u8, job.action);
    cloned.worker_id = try alloc.dupe(u8, job.worker_id);
    if (job.constraint_name) |value| cloned.constraint_name = try alloc.dupe(u8, value);
    cloned.lower_doc_key = try alloc.dupe(u8, job.lower_doc_key);
    cloned.upper_doc_key = try alloc.dupe(u8, job.upper_doc_key);
    cloned.status = try alloc.dupe(u8, job.status);
    cloned.violation_samples = try cloneForeignKeyIntegrityResultViolations(alloc, job.violation_samples);
    return cloned;
}

pub fn cloneForeignKeyIntegrityJobRecord(
    alloc: std.mem.Allocator,
    group_id: u64,
    job: db_mod.DB.ForeignKeyIntegrityJobRecord,
) !ForeignKeyIntegrityJobStatus {
    var cloned = ForeignKeyIntegrityJobStatus{
        .group_id = group_id,
        .version = job.version,
        .job_id = try alloc.dupe(u8, job.job_id),
        .table_name = &.{},
        .action = &.{},
        .worker_id = &.{},
        .constraint_name = null,
        .lower_doc_key = &.{},
        .upper_doc_key = &.{},
        .lease_ms = job.lease_ms,
        .max_work_units = job.max_work_units,
        .status = &.{},
        .created_at_ns = job.created_at_ns,
        .updated_at_ns = job.updated_at_ns,
        .attempts = job.attempts,
        .completed = job.completed,
        .valid = job.valid,
        .last_report = job.last_report,
        .aggregate_report = job.aggregate_report,
        .violation_sample_count = job.violation_sample_count,
        .violations_truncated = job.violations_truncated,
        .diagnostic_passes = job.diagnostic_passes,
        .violating_passes = job.violating_passes,
        .first_violation_at_ns = job.first_violation_at_ns,
        .last_violation_at_ns = job.last_violation_at_ns,
        .violation_samples = &.{},
    };
    errdefer cloned.deinit(alloc);
    cloned.table_name = try alloc.dupe(u8, job.table_name);
    cloned.action = try alloc.dupe(u8, job.action);
    cloned.worker_id = try alloc.dupe(u8, job.worker_id);
    if (job.constraint_name) |value| cloned.constraint_name = try alloc.dupe(u8, value);
    cloned.lower_doc_key = try alloc.dupe(u8, job.lower_doc_key);
    cloned.upper_doc_key = try alloc.dupe(u8, job.upper_doc_key);
    cloned.status = try alloc.dupe(u8, job.status);
    cloned.violation_samples = try decodeForeignKeyIntegrityJobViolationSamples(alloc, job.violation_samples_json);
    return cloned;
}

pub fn cloneForeignKeyIntegrityWorkStatus(
    alloc: std.mem.Allocator,
    status: ForeignKeyIntegrityWorkStatus,
) !ForeignKeyIntegrityWorkStatus {
    var cloned = ForeignKeyIntegrityWorkStatus{
        .group_id = status.group_id,
        .phase = try alloc.dupe(u8, status.phase),
        .planned_action = &.{},
        .constraint_name = null,
        .lower_doc_key = &.{},
        .upper_doc_key = &.{},
        .claim_key = &.{},
        .state = status.state,
        .claim_worker_id = null,
        .claim_lease_until_ns = status.claim_lease_until_ns,
        .progress_updated_at_ns = status.progress_updated_at_ns,
    };
    errdefer cloned.deinit(alloc);
    cloned.planned_action = try alloc.dupe(u8, status.planned_action);
    if (status.constraint_name) |value| cloned.constraint_name = try alloc.dupe(u8, value);
    cloned.lower_doc_key = try alloc.dupe(u8, status.lower_doc_key);
    cloned.upper_doc_key = try alloc.dupe(u8, status.upper_doc_key);
    cloned.claim_key = try alloc.dupe(u8, status.claim_key);
    if (status.claim_worker_id) |value| cloned.claim_worker_id = try alloc.dupe(u8, value);
    return cloned;
}

pub fn cloneForeignKeyIntegrityWorkClaim(
    alloc: std.mem.Allocator,
    claim: ForeignKeyIntegrityWorkClaim,
) !ForeignKeyIntegrityWorkClaim {
    var cloned = ForeignKeyIntegrityWorkClaim{
        .group_id = claim.group_id,
        .claim_key = try alloc.dupe(u8, claim.claim_key),
        .worker_id = &.{},
        .phase = &.{},
        .planned_action = &.{},
        .constraint_name = null,
        .lower_doc_key = &.{},
        .upper_doc_key = &.{},
        .claimed_at_ns = claim.claimed_at_ns,
        .lease_until_ns = claim.lease_until_ns,
        .attempts = claim.attempts,
    };
    errdefer cloned.deinit(alloc);
    cloned.worker_id = try alloc.dupe(u8, claim.worker_id);
    cloned.phase = try alloc.dupe(u8, claim.phase);
    cloned.planned_action = try alloc.dupe(u8, claim.planned_action);
    if (claim.constraint_name) |value| cloned.constraint_name = try alloc.dupe(u8, value);
    cloned.lower_doc_key = try alloc.dupe(u8, claim.lower_doc_key);
    cloned.upper_doc_key = try alloc.dupe(u8, claim.upper_doc_key);
    return cloned;
}

pub fn cloneForeignKeyIntegrityWorkClaimSlice(
    alloc: std.mem.Allocator,
    entries: []const ForeignKeyIntegrityWorkClaim,
) ![]ForeignKeyIntegrityWorkClaim {
    var cloned = try alloc.alloc(ForeignKeyIntegrityWorkClaim, entries.len);
    var initialized: usize = 0;
    errdefer {
        for (cloned[0..initialized]) |*entry| entry.deinit(alloc);
        if (cloned.len > 0) alloc.free(cloned);
    }
    for (entries, 0..) |entry, i| {
        cloned[i] = try cloneForeignKeyIntegrityWorkClaim(alloc, entry);
        initialized += 1;
    }
    return cloned;
}

pub fn cloneForeignKeyIntegrityResultForWorkerSnapshot(
    alloc: std.mem.Allocator,
    result: ForeignKeyIntegrityResult,
) !ForeignKeyIntegrityResult {
    const groups = try alloc.alloc(ForeignKeyIntegrityGroupReport, 0);
    errdefer alloc.free(groups);
    const progress = try cloneForeignKeyIntegrityProgressSlice(alloc, result.progress);
    errdefer {
        for (progress) |*entry| entry.deinit(alloc);
        if (progress.len > 0) alloc.free(progress);
    }
    const work_claims = try cloneForeignKeyIntegrityWorkClaimSlice(alloc, result.work_claims);
    errdefer {
        for (work_claims) |*claim| claim.deinit(alloc);
        if (work_claims.len > 0) alloc.free(work_claims);
    }
    var jobs = try alloc.alloc(ForeignKeyIntegrityJobStatus, result.jobs.len);
    var jobs_initialized: usize = 0;
    errdefer {
        for (jobs[0..jobs_initialized]) |*job| job.deinit(alloc);
        if (jobs.len > 0) alloc.free(jobs);
    }
    for (result.jobs, 0..) |job, i| {
        jobs[i] = try cloneForeignKeyIntegrityJobStatus(alloc, job);
        jobs_initialized += 1;
    }
    const violations = try alloc.alloc(ForeignKeyIntegrityViolation, 0);
    errdefer alloc.free(violations);
    var cloned: ForeignKeyIntegrityResult = .{
        .action = result.action,
        .valid = result.valid,
        .complete = result.complete,
        .violation_limit = result.violation_limit,
        .violations_truncated = result.violations_truncated,
        .report = result.report,
        .groups = groups,
        .progress = progress,
        .work_units = &.{},
        .work_claims = work_claims,
        .work_statuses = &.{},
        .jobs = jobs,
        .violations = violations,
    };
    errdefer cloned.deinit(alloc);
    try attachForeignKeyIntegrityJobId(alloc, &cloned, result.job_id);
    return cloned;
}

pub fn cloneForeignKeyIntegrityResultForWorkerExecution(
    alloc: std.mem.Allocator,
    result: ForeignKeyIntegrityResult,
) !ForeignKeyIntegrityResult {
    const groups = try alloc.dupe(ForeignKeyIntegrityGroupReport, result.groups);
    errdefer alloc.free(groups);
    const progress = try cloneForeignKeyIntegrityProgressSlice(alloc, result.progress);
    errdefer {
        for (progress) |*entry| entry.deinit(alloc);
        if (progress.len > 0) alloc.free(progress);
    }
    const work_units = try cloneForeignKeyIntegrityWorkUnits(alloc, result.work_units);
    errdefer {
        for (work_units) |*unit| unit.deinit(alloc);
        if (work_units.len > 0) alloc.free(work_units);
    }
    const work_claims = try cloneForeignKeyIntegrityWorkClaimSlice(alloc, result.work_claims);
    errdefer {
        for (work_claims) |*claim| claim.deinit(alloc);
        if (work_claims.len > 0) alloc.free(work_claims);
    }
    var work_statuses = try alloc.alloc(ForeignKeyIntegrityWorkStatus, result.work_statuses.len);
    var statuses_initialized: usize = 0;
    errdefer {
        for (work_statuses[0..statuses_initialized]) |*status| status.deinit(alloc);
        if (work_statuses.len > 0) alloc.free(work_statuses);
    }
    for (result.work_statuses, 0..) |status, i| {
        work_statuses[i] = try cloneForeignKeyIntegrityWorkStatus(alloc, status);
        statuses_initialized += 1;
    }
    var violations = try alloc.alloc(ForeignKeyIntegrityViolation, result.violations.len);
    var violations_initialized: usize = 0;
    errdefer {
        for (violations[0..violations_initialized]) |*violation| violation.deinit(alloc);
        if (violations.len > 0) alloc.free(violations);
    }
    for (result.violations, 0..) |violation, i| {
        violations[i] = try cloneForeignKeyIntegrityResultViolation(alloc, violation);
        violations_initialized += 1;
    }
    var jobs = try alloc.alloc(ForeignKeyIntegrityJobStatus, result.jobs.len);
    var jobs_initialized: usize = 0;
    errdefer {
        for (jobs[0..jobs_initialized]) |*job| job.deinit(alloc);
        if (jobs.len > 0) alloc.free(jobs);
    }
    for (result.jobs, 0..) |job, i| {
        jobs[i] = try cloneForeignKeyIntegrityJobStatus(alloc, job);
        jobs_initialized += 1;
    }
    var cloned: ForeignKeyIntegrityResult = .{
        .action = result.action,
        .valid = result.valid,
        .complete = result.complete,
        .violation_limit = result.violation_limit,
        .violations_truncated = result.violations_truncated,
        .report = result.report,
        .delete_plan = result.delete_plan,
        .groups = groups,
        .progress = progress,
        .work_units = work_units,
        .work_claims = work_claims,
        .work_statuses = work_statuses,
        .jobs = jobs,
        .violations = violations,
    };
    errdefer cloned.deinit(alloc);
    try attachForeignKeyIntegrityJobId(alloc, &cloned, result.job_id);
    return cloned;
}

pub fn cloneForeignKeyIntegrityWorkClaimRecord(
    alloc: std.mem.Allocator,
    claim: db_mod.DB.ForeignKeyIntegrityClaimRecord,
) !ForeignKeyIntegrityWorkClaim {
    var cloned = ForeignKeyIntegrityWorkClaim{
        .group_id = claim.group_id,
        .claim_key = try alloc.dupe(u8, claim.claim_key),
        .worker_id = &.{},
        .phase = &.{},
        .planned_action = &.{},
        .constraint_name = null,
        .lower_doc_key = &.{},
        .upper_doc_key = &.{},
        .claimed_at_ns = claim.claimed_at_ns,
        .lease_until_ns = claim.lease_until_ns,
        .attempts = claim.attempts,
    };
    errdefer cloned.deinit(alloc);
    cloned.worker_id = try alloc.dupe(u8, claim.worker_id);
    cloned.phase = try alloc.dupe(u8, claim.phase);
    cloned.planned_action = try alloc.dupe(u8, claim.planned_action);
    if (claim.constraint_name) |value| cloned.constraint_name = try alloc.dupe(u8, value);
    cloned.lower_doc_key = try alloc.dupe(u8, claim.lower_doc_key);
    cloned.upper_doc_key = try alloc.dupe(u8, claim.upper_doc_key);
    return cloned;
}

pub fn foreignKeyIntegrityProgressMatchesUnit(
    progress: ForeignKeyIntegrityProgress,
    unit: ForeignKeyIntegrityWorkUnit,
) bool {
    if (progress.group_id != unit.group_id) return false;
    const progress_phase = if (progress.phase.len > 0) progress.phase else "child_range";
    if (!std.mem.eql(u8, progress_phase, unit.phase)) return false;
    const expected_mode = foreignKeyIntegrityProgressModeNameForPlannedAction(unit.planned_action) orelse return false;
    if (!std.mem.eql(u8, progress.mode, expected_mode)) return false;
    if (progress.constraint_name == null and unit.constraint_name != null) return false;
    if (progress.constraint_name != null and unit.constraint_name == null) return false;
    if (progress.constraint_name) |progress_name| {
        if (!std.mem.eql(u8, progress_name, unit.constraint_name.?)) return false;
    }
    return std.mem.eql(u8, progress.lower_doc_key, unit.lower_doc_key) and
        std.mem.eql(u8, progress.upper_doc_key, unit.upper_doc_key);
}

pub fn foreignKeyIntegrityClaimKey(
    alloc: std.mem.Allocator,
    unit: ForeignKeyIntegrityWorkUnit,
) ![]u8 {
    return try std.fmt.allocPrint(
        alloc,
        "fk-integrity:v1:group={d}:phase-len={d}:phase={s}:action-len={d}:action={s}:constraint-len={d}:constraint={s}:lower-len={d}:lower={s}:upper-len={d}:upper={s}",
        .{
            unit.group_id,
            unit.phase.len,
            unit.phase,
            unit.planned_action.len,
            unit.planned_action,
            if (unit.constraint_name) |value| value.len else 0,
            unit.constraint_name orelse "",
            unit.lower_doc_key.len,
            unit.lower_doc_key,
            unit.upper_doc_key.len,
            unit.upper_doc_key,
        },
    );
}

pub fn foreignKeyIntegrityWorkStatusFromUnit(
    alloc: std.mem.Allocator,
    unit: ForeignKeyIntegrityWorkUnit,
    progress_entries: []const ForeignKeyIntegrityProgress,
    claim_entries: []const ForeignKeyIntegrityWorkClaim,
    missing_state: ForeignKeyIntegrityWorkState,
) !ForeignKeyIntegrityWorkStatus {
    var state = missing_state;
    var updated_at_ns: ?u64 = null;
    var claim_worker_id: ?[]const u8 = null;
    var claim_lease_until_ns: ?u64 = null;
    const claim_key = try foreignKeyIntegrityClaimKey(alloc, unit);
    var claim_key_transferred = false;
    errdefer if (!claim_key_transferred) alloc.free(claim_key);
    for (progress_entries) |progress| {
        if (!foreignKeyIntegrityProgressMatchesUnit(progress, unit)) continue;
        updated_at_ns = progress.updated_at_ns;
        state = if (!progress.completed) .incomplete else if (progress.valid) .complete else .invalid;
        break;
    }
    for (claim_entries) |claim| {
        if (!std.mem.eql(u8, claim.claim_key, claim_key)) continue;
        if (state == missing_state) {
            state = .claimed;
        }
        claim_worker_id = claim.worker_id;
        claim_lease_until_ns = claim.lease_until_ns;
        break;
    }
    var status = ForeignKeyIntegrityWorkStatus{
        .group_id = unit.group_id,
        .phase = try alloc.dupe(u8, unit.phase),
        .planned_action = &.{},
        .constraint_name = null,
        .lower_doc_key = &.{},
        .upper_doc_key = &.{},
        .claim_key = claim_key,
        .state = state,
        .claim_worker_id = null,
        .claim_lease_until_ns = claim_lease_until_ns,
        .progress_updated_at_ns = updated_at_ns,
    };
    claim_key_transferred = true;
    errdefer status.deinit(alloc);
    status.planned_action = try alloc.dupe(u8, unit.planned_action);
    if (unit.constraint_name) |value| status.constraint_name = try alloc.dupe(u8, value);
    status.lower_doc_key = try alloc.dupe(u8, unit.lower_doc_key);
    status.upper_doc_key = try alloc.dupe(u8, unit.upper_doc_key);
    if (claim_worker_id) |value| status.claim_worker_id = try alloc.dupe(u8, value);
    return status;
}

pub fn buildForeignKeyIntegrityWorkStatuses(
    alloc: std.mem.Allocator,
    units: []const ForeignKeyIntegrityWorkUnit,
    progress_entries: []const ForeignKeyIntegrityProgress,
    claim_entries: []const ForeignKeyIntegrityWorkClaim,
    missing_state: ForeignKeyIntegrityWorkState,
) ![]ForeignKeyIntegrityWorkStatus {
    var statuses = try alloc.alloc(ForeignKeyIntegrityWorkStatus, units.len);
    var initialized: usize = 0;
    errdefer {
        for (statuses[0..initialized]) |*status| status.deinit(alloc);
        if (statuses.len > 0) alloc.free(statuses);
    }
    for (units, 0..) |unit, i| {
        statuses[i] = try foreignKeyIntegrityWorkStatusFromUnit(alloc, unit, progress_entries, claim_entries, missing_state);
        initialized += 1;
    }
    return statuses;
}

pub fn foreignKeyIntegritySingleWorkUnit(
    alloc: std.mem.Allocator,
    group_id: u64,
    action: ForeignKeyIntegrityAction,
    constraint_name: ?[]const u8,
    lower_doc_key: []const u8,
    upper_doc_key: []const u8,
) ![]ForeignKeyIntegrityWorkUnit {
    return try foreignKeyIntegritySingleWorkUnitForPhase(alloc, group_id, "child_range", action, constraint_name, lower_doc_key, upper_doc_key);
}

pub fn foreignKeyIntegritySingleWorkUnitForPhase(
    alloc: std.mem.Allocator,
    group_id: u64,
    phase: []const u8,
    action: ForeignKeyIntegrityAction,
    constraint_name: ?[]const u8,
    lower_doc_key: []const u8,
    upper_doc_key: []const u8,
) ![]ForeignKeyIntegrityWorkUnit {
    const units = try alloc.alloc(ForeignKeyIntegrityWorkUnit, 1);
    errdefer alloc.free(units);
    units[0] = .{
        .group_id = group_id,
        .phase = try alloc.dupe(u8, phase),
        .planned_action = &.{},
        .constraint_name = null,
        .lower_doc_key = &.{},
        .upper_doc_key = &.{},
    };
    errdefer units[0].deinit(alloc);
    units[0].planned_action = try alloc.dupe(u8, foreignKeyIntegrityPlannedActionName(action));
    if (constraint_name) |value| units[0].constraint_name = try alloc.dupe(u8, value);
    units[0].lower_doc_key = try alloc.dupe(u8, lower_doc_key);
    units[0].upper_doc_key = try alloc.dupe(u8, upper_doc_key);
    return units;
}

pub fn foreignKeyIntegrityWorkerActionSupported(action: ForeignKeyIntegrityAction) bool {
    return switch (action) {
        .validate, .dry_run, .repair => true,
        .plan, .list, .explain_delete, .progress => false,
    };
}

pub fn foreignKeyIntegrityWorkStatusClaimable(status: ForeignKeyIntegrityWorkStatus, now_ns: u64) bool {
    return switch (status.state) {
        .planned, .pending, .incomplete => true,
        .claimed => if (status.claim_lease_until_ns) |lease_until| lease_until <= now_ns else true,
        .complete, .invalid => false,
    };
}

pub fn foreignKeyIntegrityWorkStatusesValid(statuses: []const ForeignKeyIntegrityWorkStatus) bool {
    for (statuses) |status| {
        if (status.state == .invalid) return false;
    }
    return true;
}

pub fn foreignKeyIntegrityWorkStatusesHaveClaimable(statuses: []const ForeignKeyIntegrityWorkStatus, now_ns: u64) bool {
    for (statuses) |status| {
        if (foreignKeyIntegrityWorkStatusClaimable(status, now_ns)) return true;
    }
    return false;
}

pub fn foreignKeyIntegrityPlannedUnitsContainGroupBefore(units: []const ForeignKeyIntegrityWorkUnit, index: usize) bool {
    const group_id = units[index].group_id;
    for (units[0..index]) |unit| {
        if (unit.group_id == group_id) return true;
    }
    return false;
}

pub fn mergeForeignKeyIntegrityReport(
    dst: *db_mod.relational_store.ForeignKeyIntegrityReport,
    src: db_mod.relational_store.ForeignKeyIntegrityReport,
) void {
    dst.scanned_child_rows +|= src.scanned_child_rows;
    dst.referenced_child_rows +|= src.referenced_child_rows;
    dst.scanned_ref_rows +|= src.scanned_ref_rows;
    dst.missing_parent_rows +|= src.missing_parent_rows;
    dst.missing_ref_rows +|= src.missing_ref_rows;
    dst.stale_ref_rows +|= src.stale_ref_rows;
    dst.repaired_ref_rows +|= src.repaired_ref_rows;
    dst.deleted_stale_ref_rows +|= src.deleted_stale_ref_rows;
}

pub fn appendForeignKeyIntegrityResult(
    alloc: std.mem.Allocator,
    result: ForeignKeyIntegrityResult,
    violation_limit: usize,
    aggregate: *db_mod.relational_store.ForeignKeyIntegrityReport,
    group_reports: *std.ArrayListUnmanaged(ForeignKeyIntegrityGroupReport),
    progress_entries: *std.ArrayListUnmanaged(ForeignKeyIntegrityProgress),
    work_units: *std.ArrayListUnmanaged(ForeignKeyIntegrityWorkUnit),
    work_claims: *std.ArrayListUnmanaged(ForeignKeyIntegrityWorkClaim),
    jobs: *std.ArrayListUnmanaged(ForeignKeyIntegrityJobStatus),
    violations: *std.ArrayListUnmanaged(ForeignKeyIntegrityViolation),
    delete_plan: *?db_mod.relational_store.ForeignKeyDeletePlan,
    truncated: *bool,
    valid: *bool,
    complete: *bool,
) !void {
    mergeForeignKeyIntegrityReport(aggregate, result.report);
    try group_reports.ensureUnusedCapacity(alloc, result.groups.len);
    for (result.groups) |group| group_reports.appendAssumeCapacity(group);

    try progress_entries.ensureUnusedCapacity(alloc, result.progress.len);
    for (result.progress) |progress| {
        progress_entries.appendAssumeCapacity(try cloneForeignKeyIntegrityProgress(alloc, progress));
    }

    try work_units.ensureUnusedCapacity(alloc, result.work_units.len);
    for (result.work_units) |unit| {
        work_units.appendAssumeCapacity(try cloneForeignKeyIntegrityWorkUnit(alloc, unit));
    }

    try work_claims.ensureUnusedCapacity(alloc, result.work_claims.len);
    for (result.work_claims) |claim| {
        work_claims.appendAssumeCapacity(try cloneForeignKeyIntegrityWorkClaim(alloc, claim));
    }

    try jobs.ensureUnusedCapacity(alloc, result.jobs.len);
    for (result.jobs) |job| {
        jobs.appendAssumeCapacity(try cloneForeignKeyIntegrityJobStatus(alloc, job));
    }

    const remaining = violation_limit -| violations.items.len;
    const copy_count = @min(remaining, result.violations.len);
    try violations.ensureUnusedCapacity(alloc, copy_count);
    for (result.violations[0..copy_count]) |violation| {
        violations.appendAssumeCapacity(try cloneForeignKeyIntegrityResultViolation(alloc, violation));
    }

    truncated.* = truncated.* or result.violations_truncated or result.violations.len > remaining;
    valid.* = valid.* and result.valid;
    complete.* = complete.* and result.complete;
    if (result.delete_plan) |plan| {
        delete_plan.* = mergeForeignKeyDeletePlans(delete_plan.*, plan);
    }
}

pub fn appendForeignKeyIntegrityProgressAndClaims(
    alloc: std.mem.Allocator,
    result: ForeignKeyIntegrityResult,
    progress_entries: *std.ArrayListUnmanaged(ForeignKeyIntegrityProgress),
    work_claims: *std.ArrayListUnmanaged(ForeignKeyIntegrityWorkClaim),
    jobs: *std.ArrayListUnmanaged(ForeignKeyIntegrityJobStatus),
) !void {
    try progress_entries.ensureUnusedCapacity(alloc, result.progress.len);
    for (result.progress) |progress| {
        progress_entries.appendAssumeCapacity(try cloneForeignKeyIntegrityProgress(alloc, progress));
    }
    try work_claims.ensureUnusedCapacity(alloc, result.work_claims.len);
    for (result.work_claims) |claim| {
        work_claims.appendAssumeCapacity(try cloneForeignKeyIntegrityWorkClaim(alloc, claim));
    }
    try jobs.ensureUnusedCapacity(alloc, result.jobs.len);
    for (result.jobs) |job| {
        jobs.appendAssumeCapacity(try cloneForeignKeyIntegrityJobStatus(alloc, job));
    }
}

pub fn appendForeignKeyIntegrityExecutedResult(
    alloc: std.mem.Allocator,
    result: ForeignKeyIntegrityResult,
    violation_limit: usize,
    aggregate: *db_mod.relational_store.ForeignKeyIntegrityReport,
    group_reports: *std.ArrayListUnmanaged(ForeignKeyIntegrityGroupReport),
    violations: *std.ArrayListUnmanaged(ForeignKeyIntegrityViolation),
    truncated: *bool,
) !void {
    mergeForeignKeyIntegrityReport(aggregate, result.report);
    try group_reports.ensureUnusedCapacity(alloc, result.groups.len);
    for (result.groups) |group| group_reports.appendAssumeCapacity(group);

    const remaining = violation_limit -| violations.items.len;
    const copy_count = @min(remaining, result.violations.len);
    try violations.ensureUnusedCapacity(alloc, copy_count);
    for (result.violations[0..copy_count]) |violation| {
        violations.appendAssumeCapacity(try cloneForeignKeyIntegrityResultViolation(alloc, violation));
    }
    truncated.* = truncated.* or result.violations_truncated or result.violations.len > remaining;
}

pub fn mergeForeignKeyDeletePlans(
    existing: ?db_mod.relational_store.ForeignKeyDeletePlan,
    next: db_mod.relational_store.ForeignKeyDeletePlan,
) db_mod.relational_store.ForeignKeyDeletePlan {
    var merged = existing orelse return next;
    merged.exists = merged.exists or next.exists;
    merged.allowed = merged.allowed and next.allowed;
    if (merged.block_reason == .none and next.block_reason != .none) merged.block_reason = next.block_reason;
    merged.planned_set_null_updates +|= next.planned_set_null_updates;
    merged.planned_cascade_deletes +|= next.planned_cascade_deletes;
    merged.planned_row_deletes +|= next.planned_row_deletes;
    merged.planned_index_deletes +|= next.planned_index_deletes;
    merged.planned_writes +|= next.planned_writes;
    return merged;
}

pub fn cloneUniqueConstraintIntegrityProgress(
    alloc: std.mem.Allocator,
    progress: UniqueConstraintIntegrityProgress,
) !UniqueConstraintIntegrityProgress {
    var cloned = UniqueConstraintIntegrityProgress{
        .group_id = progress.group_id,
        .version = progress.version,
        .mode = try alloc.dupe(u8, progress.mode),
        .lower_doc_key = &.{},
        .upper_doc_key = &.{},
        .completed = progress.completed,
        .valid = progress.valid,
        .updated_at_ns = progress.updated_at_ns,
        .report = progress.report,
    };
    errdefer cloned.deinit(alloc);
    cloned.lower_doc_key = try alloc.dupe(u8, progress.lower_doc_key);
    cloned.upper_doc_key = try alloc.dupe(u8, progress.upper_doc_key);
    return cloned;
}

pub fn cloneUniqueConstraintIntegrityProgressRecord(
    alloc: std.mem.Allocator,
    group_id: u64,
    progress: db_mod.DB.UniqueConstraintIntegrityProgressRecord,
) !UniqueConstraintIntegrityProgress {
    var cloned = UniqueConstraintIntegrityProgress{
        .group_id = group_id,
        .version = progress.version,
        .mode = try alloc.dupe(u8, progress.mode),
        .lower_doc_key = &.{},
        .upper_doc_key = &.{},
        .completed = progress.completed,
        .valid = progress.valid,
        .updated_at_ns = progress.updated_at_ns,
        .report = progress.report,
    };
    errdefer cloned.deinit(alloc);
    cloned.lower_doc_key = try alloc.dupe(u8, progress.lower_doc_key);
    cloned.upper_doc_key = try alloc.dupe(u8, progress.upper_doc_key);
    return cloned;
}

pub fn mergeUniqueConstraintIntegrityReport(
    aggregate: *db_mod.relational_store.UniqueConstraintIntegrityReport,
    next: db_mod.relational_store.UniqueConstraintIntegrityReport,
) void {
    aggregate.scanned_rows +|= next.scanned_rows;
    aggregate.expected_unique_rows +|= next.expected_unique_rows;
    aggregate.scanned_unique_rows +|= next.scanned_unique_rows;
    aggregate.missing_unique_rows +|= next.missing_unique_rows;
    aggregate.stale_unique_rows +|= next.stale_unique_rows;
    aggregate.duplicate_unique_rows +|= next.duplicate_unique_rows;
    aggregate.repaired_unique_rows +|= next.repaired_unique_rows;
    aggregate.deleted_stale_unique_rows +|= next.deleted_stale_unique_rows;
}

pub fn appendUniqueConstraintIntegrityResult(
    alloc: std.mem.Allocator,
    result: UniqueConstraintIntegrityResult,
    aggregate: *db_mod.relational_store.UniqueConstraintIntegrityReport,
    group_reports: *std.ArrayListUnmanaged(UniqueConstraintIntegrityGroupReport),
    progress_entries: *std.ArrayListUnmanaged(UniqueConstraintIntegrityProgress),
    valid: *bool,
    complete: *bool,
) !void {
    mergeUniqueConstraintIntegrityReport(aggregate, result.report);
    try group_reports.ensureUnusedCapacity(alloc, result.groups.len);
    for (result.groups) |group| group_reports.appendAssumeCapacity(group);
    try progress_entries.ensureUnusedCapacity(alloc, result.progress.len);
    for (result.progress) |progress| {
        progress_entries.appendAssumeCapacity(try cloneUniqueConstraintIntegrityProgress(alloc, progress));
    }
    valid.* = valid.* and result.valid;
    complete.* = complete.* and result.complete;
}

pub fn uniqueConstraintIntegrityProgressModeForAction(
    action: UniqueConstraintIntegrityAction,
) ?db_mod.relational_store.ForeignKeyIntegrityMode {
    return switch (action) {
        .validate => .validate,
        .dry_run => .dry_run,
        .repair => .repair,
        .progress => null,
    };
}

pub fn emptyForeignKeyIntegrityControllerResult(
    alloc: std.mem.Allocator,
    action: ForeignKeyIntegrityAction,
    violation_limit: usize,
) !ForeignKeyIntegrityResult {
    const groups = try alloc.alloc(ForeignKeyIntegrityGroupReport, 0);
    errdefer alloc.free(groups);
    const violations = try alloc.alloc(ForeignKeyIntegrityViolation, 0);
    return .{
        .action = action,
        .valid = true,
        .complete = true,
        .violation_limit = violation_limit,
        .violations_truncated = false,
        .report = .{},
        .groups = groups,
        .violations = violations,
    };
}

pub fn selectedForeignKeyIntegrityControllerConstraintAlloc(
    alloc: std.mem.Allocator,
    schema_json: []const u8,
    constraint_name: ?[]const u8,
) !?[]u8 {
    if (schema_json.len == 0) return null;

    var parsed_schema = try tables_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try tables_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer storage_schema.freeSchema(alloc, runtime_schema);

    if (constraint_name) |requested| {
        for (runtime_schema.foreign_keys) |foreign_key| {
            if (std.mem.eql(u8, foreign_key.name, requested)) {
                return try alloc.dupe(u8, foreign_key.name);
            }
        }
        return error.ForeignKeyNotFound;
    }

    for (runtime_schema.foreign_keys) |foreign_key| {
        if (foreign_key.validation_state != .enforced) {
            return try alloc.dupe(u8, foreign_key.name);
        }
    }
    return null;
}

pub fn tableSchemaHasForeignKeysAlloc(alloc: std.mem.Allocator, schema_json: []const u8) !bool {
    if (schema_json.len == 0) return false;

    var parsed_schema = try tables_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    return parsed_schema.foreign_keys.len > 0;
}

pub fn selectedUniqueConstraintIntegrityControllerConstraintAlloc(
    alloc: std.mem.Allocator,
    schema_json: []const u8,
) !?[]u8 {
    if (schema_json.len == 0) return null;

    var parsed_schema = try tables_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try tables_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer storage_schema.freeSchema(alloc, runtime_schema);

    for (runtime_schema.unique_constraints) |constraint| {
        if (constraint.validation_state != .enforced) {
            return try alloc.dupe(u8, constraint.name);
        }
    }
    return null;
}

pub fn tableSchemaHasUniqueConstraintsAlloc(alloc: std.mem.Allocator, schema_json: []const u8) !bool {
    if (schema_json.len == 0) return false;

    var parsed_schema = try tables_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    return parsed_schema.unique_constraints.len > 0;
}

pub fn validateUniqueConstraintIntegritySchemaControllerOptions(options: UniqueConstraintIntegritySchemaControllerOptions) !void {
    if (options.worker_id.len == 0 or options.max_tables == 0) return error.InvalidUniqueIntegrityRequest;
}

pub fn appendUniqueConstraintIntegritySchemaControllerTableResult(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(UniqueConstraintIntegritySchemaControllerTableResult),
    table_name: []const u8,
    constraint_name: []const u8,
    schema_adoption: bool,
    result: UniqueConstraintIntegrityResult,
) !void {
    var entry = UniqueConstraintIntegritySchemaControllerTableResult{
        .table_name = try alloc.dupe(u8, table_name),
        .constraint_name = try alloc.dupe(u8, constraint_name),
        .schema_adoption = schema_adoption,
        .result = result,
    };
    errdefer entry.deinit(alloc);
    try out.append(alloc, entry);
}

pub fn finalizeUniqueConstraintIntegritySchemaControllerMaintenanceResult(
    alloc: std.mem.Allocator,
    summary: UniqueConstraintIntegritySchemaControllerResult,
    results: *std.ArrayListUnmanaged(UniqueConstraintIntegritySchemaControllerTableResult),
) !UniqueConstraintIntegritySchemaControllerResult {
    var out = summary;
    out.results = try results.toOwnedSlice(alloc);
    return out;
}

pub fn shouldPromoteUniqueConstraintAfterSchemaControllerResult(result: UniqueConstraintIntegrityResult) bool {
    return switch (result.action) {
        .validate, .repair => result.complete and result.valid,
        else => false,
    };
}

pub fn validateForeignKeyIntegritySchemaControllerOptions(options: ForeignKeyIntegritySchemaControllerOptions) !void {
    if (!foreignKeyIntegrityWorkerActionSupported(options.action)) return error.InvalidForeignKeyIntegrityRequest;
    if (options.worker_id.len == 0 or options.lease_ms == 0) return error.InvalidForeignKeyIntegrityRequest;
    if (options.max_tables == 0 or options.max_jobs == 0 or options.max_action_jobs == 0 or options.max_work_units_per_table == 0 or options.action_job_page_limit == 0) return error.InvalidForeignKeyIntegrityRequest;
}

pub fn appendForeignKeyIntegritySchemaControllerTableResult(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(ForeignKeyIntegritySchemaControllerTableResult),
    table_name: []const u8,
    schema_adoption: bool,
    result: ForeignKeyIntegrityResult,
) !void {
    var entry = ForeignKeyIntegritySchemaControllerTableResult{
        .table_name = try alloc.dupe(u8, table_name),
        .schema_adoption = schema_adoption,
        .result = result,
    };
    errdefer entry.deinit(alloc);
    try out.append(alloc, entry);
}

pub fn foreignKeyIntegrityActionFromJobStatus(job: ForeignKeyIntegrityJobStatus) ?ForeignKeyIntegrityAction {
    return std.meta.stringToEnum(ForeignKeyIntegrityAction, job.action);
}

pub fn foreignKeyIntegritySchemaControllerResultsContainJobId(
    results: []const ForeignKeyIntegritySchemaControllerTableResult,
    job_id: []const u8,
) bool {
    for (results) |entry| {
        if (entry.result.job_id) |existing| {
            if (std.mem.eql(u8, existing, job_id)) return true;
        }
    }
    return false;
}

pub fn finalizeForeignKeyIntegritySchemaControllerMaintenanceResult(
    alloc: std.mem.Allocator,
    summary: ForeignKeyIntegritySchemaControllerResult,
    results: *std.ArrayListUnmanaged(ForeignKeyIntegritySchemaControllerTableResult),
    action_schedules: *std.ArrayListUnmanaged(ForeignKeyActionScheduleStatus),
    action_jobs: *std.ArrayListUnmanaged(ForeignKeyActionJobStatus),
) !ForeignKeyIntegritySchemaControllerResult {
    var out = summary;
    out.results = try results.toOwnedSlice(alloc);
    out.action_schedules = try action_schedules.toOwnedSlice(alloc);
    out.action_jobs = try action_jobs.toOwnedSlice(alloc);
    return out;
}

pub fn resultForeignKeyConstraintName(result: ForeignKeyIntegrityResult) ?[]const u8 {
    for (result.work_units) |unit| {
        if (unit.constraint_name) |constraint_name| return constraint_name;
    }
    for (result.work_statuses) |status| {
        if (status.constraint_name) |constraint_name| return constraint_name;
    }
    return null;
}

pub fn shouldPromoteForeignKeyAfterSchemaControllerResult(result: ForeignKeyIntegrityResult) bool {
    return switch (result.action) {
        .validate, .repair => result.complete and result.valid,
        else => false,
    };
}

pub fn cloneUniqueConstraintOwnerRange(
    alloc: std.mem.Allocator,
    range: metadata_table_manager.UniqueConstraintRangeRecord,
) !UniqueConstraintOwnerRange {
    var cloned = UniqueConstraintOwnerRange{
        .constraint_name = try alloc.dupe(u8, range.constraint_name),
        .start_encoded_value = &.{},
        .end_encoded_value = null,
        .group_id = range.group_id,
        .topology_epoch = range.topology_epoch,
        .state = &.{},
        .active = metadata_table_manager.uniqueConstraintRangeRoutable(range),
    };
    errdefer cloned.deinit(alloc);
    cloned.start_encoded_value = try alloc.dupe(u8, range.start_encoded_value);
    if (range.end_encoded_value) |value| cloned.end_encoded_value = try alloc.dupe(u8, value);
    cloned.state = try alloc.dupe(u8, range.state);
    return cloned;
}

pub fn inspectUniqueConstraintOwnerTopology(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
) !?UniqueConstraintOwnerTopology {
    var snapshot = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&snapshot);
    const table = tables_api.findTableByName(&snapshot, table_name) orelse return null;

    var parsed_schema = try tables_api.parseValidatedTableSchema(alloc, table.schema_json);
    defer parsed_schema.deinit(alloc);

    var ranges = std.ArrayListUnmanaged(UniqueConstraintOwnerRange).empty;
    var configured_names = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (configured_names.items) |name| alloc.free(name);
        configured_names.deinit(alloc);
    }
    errdefer {
        for (ranges.items) |*range| range.deinit(alloc);
        ranges.deinit(alloc);
    }

    var hasher = std.hash.Wyhash.init(0);
    hasher.update(table.name);
    hasher.update(std.mem.asBytes(&table.table_id));
    var active_ranges: usize = 0;
    var transitional_ranges: usize = 0;

    for (snapshot.unique_constraint_ranges) |range| {
        if (range.table_id != table.table_id) continue;
        hashUniqueConstraintOwnerRangeForInspection(&hasher, range);
        if (metadata_table_manager.uniqueConstraintRangeRoutable(range)) {
            active_ranges += 1;
        } else {
            transitional_ranges += 1;
        }
        if (!stringSliceContains(configured_names.items, range.constraint_name)) {
            const name = try alloc.dupe(u8, range.constraint_name);
            var name_transferred = false;
            errdefer if (!name_transferred) alloc.free(name);
            try configured_names.append(alloc, name);
            name_transferred = true;
        }
        var cloned_range = try cloneUniqueConstraintOwnerRange(alloc, range);
        var cloned_range_transferred = false;
        errdefer if (!cloned_range_transferred) cloned_range.deinit(alloc);
        try ranges.append(alloc, cloned_range);
        cloned_range_transferred = true;
    }

    var all_declared_complete = true;
    const declared_owner_count = countUniqueOwnerConstraints(parsed_schema) + @as(usize, if (parsed_schema.primary_key != null) 1 else 0);
    if (parsed_schema.primary_key != null) {
        if (!uniqueConstraintOwnerCoverageComplete(snapshot.unique_constraint_ranges, table.table_id, db_mod.relational_store.primary_key_constraint_name)) {
            all_declared_complete = false;
        }
    }
    for (parsed_schema.unique_constraints) |constraint| {
        if (constraint.validation_state != .enforced) continue;
        if (!all_declared_complete) break;
        if (!uniqueConstraintOwnerCoverageComplete(snapshot.unique_constraint_ranges, table.table_id, constraint.name)) {
            all_declared_complete = false;
            break;
        }
    }

    const no_stale_configured_constraints = configured_names.items.len <= declared_owner_count and blk: {
        for (configured_names.items) |name| {
            if (!declaredUniqueOwnerName(parsed_schema, name)) break :blk false;
        }
        break :blk true;
    };

    return .{
        .configured = ranges.items.len > 0,
        .complete = all_declared_complete and no_stale_configured_constraints and transitional_ranges == 0,
        .declared_constraints = declared_owner_count,
        .configured_constraints = countUniqueConstraintOwnerNames(ranges.items),
        .active_ranges = active_ranges,
        .transitional_ranges = transitional_ranges,
        .topology_epoch = if (ranges.items.len == 0) 0 else hasher.final(),
        .ranges = try ranges.toOwnedSlice(alloc),
    };
}

pub fn declaredUniqueOwnerName(
    parsed_schema: anytype,
    name: []const u8,
) bool {
    if (std.mem.eql(u8, name, db_mod.relational_store.primary_key_constraint_name)) {
        return parsed_schema.primary_key != null;
    }
    for (parsed_schema.unique_constraints) |constraint| {
        if (constraint.validation_state != .enforced) continue;
        if (std.mem.eql(u8, constraint.name, name)) return true;
    }
    return false;
}

pub fn countUniqueOwnerConstraints(parsed_schema: anytype) usize {
    var count: usize = 0;
    for (parsed_schema.unique_constraints) |constraint| {
        if (constraint.validation_state == .enforced) count += 1;
    }
    return count;
}

pub fn stringSliceContains(values: []const []const u8, needle: []const u8) bool {
    for (values) |value| {
        if (std.mem.eql(u8, value, needle)) return true;
    }
    return false;
}

pub fn countUniqueConstraintOwnerNames(ranges: []const UniqueConstraintOwnerRange) usize {
    var count: usize = 0;
    for (ranges, 0..) |range, i| {
        var seen = false;
        for (ranges[0..i]) |previous| {
            if (std.mem.eql(u8, previous.constraint_name, range.constraint_name)) {
                seen = true;
                break;
            }
        }
        if (!seen) count += 1;
    }
    return count;
}

pub fn uniqueConstraintOwnerCoverageComplete(
    all_ranges: []const metadata_table_manager.UniqueConstraintRangeRecord,
    table_id: u64,
    constraint_name: []const u8,
) bool {
    var current_start: []const u8 = "";
    var matched = false;
    while (true) {
        var next: ?metadata_table_manager.UniqueConstraintRangeRecord = null;
        for (all_ranges) |range| {
            if (range.table_id != table_id) continue;
            if (!std.mem.eql(u8, range.constraint_name, constraint_name)) continue;
            if (!metadata_table_manager.uniqueConstraintRangeRoutable(range)) return false;
            if (!std.mem.eql(u8, range.start_encoded_value, current_start)) continue;
            if (next != null) return false;
            next = range;
        }
        const range = next orelse return false;
        matched = true;
        if (range.end_encoded_value) |end| {
            if (end.len == 0 or std.mem.eql(u8, end, current_start)) return false;
            current_start = end;
            continue;
        }
        return matched;
    }
}

pub fn hashUniqueConstraintOwnerRangeForInspection(
    hasher: *std.hash.Wyhash,
    range: metadata_table_manager.UniqueConstraintRangeRecord,
) void {
    hasher.update(range.constraint_name);
    hasher.update(range.start_encoded_value);
    if (range.end_encoded_value) |end| {
        hasher.update(&[_]u8{1});
        hasher.update(end);
    } else {
        hasher.update(&[_]u8{0});
    }
    hasher.update(std.mem.asBytes(&range.group_id));
    hasher.update(std.mem.asBytes(&range.topology_epoch));
    hasher.update(range.state);
}

pub fn planForeignKeyIntegrityWorkUnits(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
    action: ForeignKeyIntegrityAction,
    constraint_name: ?[]const u8,
    lower_doc_key: []const u8,
    upper_doc_key: []const u8,
) ![]ForeignKeyIntegrityWorkUnit {
    var snapshot = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&snapshot);
    const table = tables_api.findTableByName(&snapshot, table_name) orelse return try alloc.alloc(ForeignKeyIntegrityWorkUnit, 0);
    const ranges = try metadata_admin.listTableRanges(alloc, &snapshot, table.table_id);
    defer metadata_admin.freeRangeRefs(alloc, ranges);
    sortRangeRefsForForeignKeyIntegrityPlan(ranges);

    var units = std.ArrayListUnmanaged(ForeignKeyIntegrityWorkUnit).empty;
    errdefer {
        for (units.items) |*unit| unit.deinit(alloc);
        units.deinit(alloc);
    }

    for (ranges) |range_ref| {
        const range = range_ref.*;
        if (!rangeOverlapsForeignKeyIntegritySpan(range, lower_doc_key, upper_doc_key)) continue;
        const unit_lower = maxLowerDocKey(lower_doc_key, range.start_key);
        const unit_upper = minUpperDocKey(upper_doc_key, range.end_key);
        var unit = ForeignKeyIntegrityWorkUnit{
            .group_id = range.group_id,
            .phase = try alloc.dupe(u8, "child_range"),
            .planned_action = &.{},
            .constraint_name = null,
            .lower_doc_key = &.{},
            .upper_doc_key = &.{},
        };
        var unit_transferred = false;
        errdefer if (!unit_transferred) unit.deinit(alloc);
        unit.planned_action = try alloc.dupe(u8, foreignKeyIntegrityPlannedActionName(action));
        if (constraint_name) |value| unit.constraint_name = try alloc.dupe(u8, value);
        unit.lower_doc_key = try alloc.dupe(u8, unit_lower);
        unit.upper_doc_key = try alloc.dupe(u8, unit_upper);
        try units.append(alloc, unit);
        unit_transferred = true;
    }

    return try units.toOwnedSlice(alloc);
}

pub fn planForeignKeyIntegrityWorkerWorkUnits(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
    action: ForeignKeyIntegrityAction,
    constraint_name: ?[]const u8,
    lower_doc_key: []const u8,
    upper_doc_key: []const u8,
) ![]ForeignKeyIntegrityWorkUnit {
    const child_units = try planForeignKeyIntegrityWorkUnits(alloc, catalog, table_name, action, constraint_name, lower_doc_key, upper_doc_key);
    defer {
        for (child_units) |*unit| unit.deinit(alloc);
        if (child_units.len > 0) alloc.free(child_units);
    }

    var units = std.ArrayListUnmanaged(ForeignKeyIntegrityWorkUnit).empty;
    errdefer {
        for (units.items) |*unit| unit.deinit(alloc);
        units.deinit(alloc);
    }
    try units.ensureUnusedCapacity(alloc, child_units.len);
    for (child_units) |unit| {
        units.appendAssumeCapacity(try cloneForeignKeyIntegrityWorkUnit(alloc, unit));
    }

    var snapshot = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&snapshot);
    const table = tables_api.findTableByName(&snapshot, table_name) orelse return try units.toOwnedSlice(alloc);
    try appendForeignKeyIntegrityOwnerRangeWorkUnits(
        alloc,
        &units,
        &snapshot,
        table.*,
        action,
        constraint_name,
        lower_doc_key,
        upper_doc_key,
    );
    return try units.toOwnedSlice(alloc);
}

pub fn appendForeignKeyIntegrityOwnerRangeWorkUnits(
    alloc: std.mem.Allocator,
    units: *std.ArrayListUnmanaged(ForeignKeyIntegrityWorkUnit),
    snapshot: *const metadata_api.AdminSnapshot,
    table: metadata_table_manager.TableRecord,
    action: ForeignKeyIntegrityAction,
    constraint_name: ?[]const u8,
    lower_doc_key: []const u8,
    upper_doc_key: []const u8,
) !void {
    if (!foreignKeyIntegrityWorkerActionSupported(action)) return;
    if (table.schema_json.len == 0) return;

    var parsed_schema = try tables_api.parseValidatedTableSchema(alloc, table.schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try tables_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer storage_schema.freeSchema(alloc, runtime_schema);

    for (snapshot.foreign_key_ref_ranges) |range| {
        if (range.child_table_id != table.table_id) continue;
        if (constraint_name) |requested| {
            if (!std.mem.eql(u8, requested, range.constraint_name)) continue;
        }
        if (!metadata_table_manager.foreignKeyReferenceRangeRoutable(range)) continue;
        if (!rangeOverlapsForeignKeyOwnerSpan(range.start_parent_key, range.end_parent_key, lower_doc_key, upper_doc_key)) continue;
        if (!foreignKeyReferenceRangeMatchesEnforcedSchema(snapshot, runtime_schema.foreign_keys, range)) continue;

        const unit_lower = maxLowerDocKey(lower_doc_key, range.start_parent_key);
        const unit_upper = minUpperDocKey(upper_doc_key, range.end_parent_key);
        var unit = ForeignKeyIntegrityWorkUnit{
            .group_id = range.group_id,
            .phase = try alloc.dupe(u8, "owner_range"),
            .planned_action = &.{},
            .constraint_name = null,
            .lower_doc_key = &.{},
            .upper_doc_key = &.{},
        };
        var unit_transferred = false;
        errdefer if (!unit_transferred) unit.deinit(alloc);
        unit.planned_action = try alloc.dupe(u8, foreignKeyIntegrityPlannedActionName(action));
        unit.constraint_name = try alloc.dupe(u8, range.constraint_name);
        unit.lower_doc_key = try alloc.dupe(u8, unit_lower);
        unit.upper_doc_key = try alloc.dupe(u8, unit_upper);
        try units.append(alloc, unit);
        unit_transferred = true;
    }
}

pub fn foreignKeyReferenceRangeMatchesEnforcedSchema(
    snapshot: *const metadata_api.AdminSnapshot,
    foreign_keys: []const storage_schema.ForeignKey,
    range: metadata_table_manager.ForeignKeyReferenceRangeRecord,
) bool {
    for (foreign_keys) |foreign_key| {
        if (!std.mem.eql(u8, foreign_key.name, range.constraint_name)) continue;
        if (foreign_key.timing != .immediate or foreign_key.validation_state != .enforced) return false;
        const parent_table = findForeignKeyIntegritySnapshotTableById(snapshot, range.parent_table_id) orelse return false;
        return std.mem.eql(u8, parent_table.name, foreign_key.parent_table);
    }
    return false;
}

pub fn findForeignKeyIntegritySnapshotTableById(
    snapshot: *const metadata_api.AdminSnapshot,
    table_id: u64,
) ?metadata_table_manager.TableRecord {
    for (snapshot.tables) |table| {
        if (table.table_id == table_id) return table;
    }
    return null;
}

pub fn sortRangeRefsForForeignKeyIntegrityPlan(ranges: []const *const metadata_table_manager.RangeRecord) void {
    std.sort.insertion(*const metadata_table_manager.RangeRecord, @constCast(ranges), {}, struct {
        fn lessThan(_: void, a: *const metadata_table_manager.RangeRecord, b: *const metadata_table_manager.RangeRecord) bool {
            return std.mem.order(u8, a.start_key, b.start_key) == .lt;
        }
    }.lessThan);
}

pub fn rangeOverlapsForeignKeyIntegritySpan(
    range: metadata_table_manager.RangeRecord,
    lower_doc_key: []const u8,
    upper_doc_key: []const u8,
) bool {
    if (upper_doc_key.len > 0 and std.mem.order(u8, range.start_key, upper_doc_key) != .lt) return false;
    if (range.end_key) |end_key| {
        if (end_key.len > 0 and lower_doc_key.len > 0 and std.mem.order(u8, end_key, lower_doc_key) != .gt) return false;
    }
    return true;
}

pub fn rangeOverlapsForeignKeyOwnerSpan(
    range_start: []const u8,
    range_end: ?[]const u8,
    lower_parent_key: []const u8,
    upper_parent_key: []const u8,
) bool {
    if (upper_parent_key.len > 0 and std.mem.order(u8, range_start, upper_parent_key) != .lt) return false;
    if (range_end) |end_key| {
        if (end_key.len > 0 and lower_parent_key.len > 0 and std.mem.order(u8, end_key, lower_parent_key) != .gt) return false;
    }
    return true;
}

pub fn maxLowerDocKey(request_lower: []const u8, range_start: []const u8) []const u8 {
    if (request_lower.len == 0) return range_start;
    if (range_start.len == 0) return request_lower;
    return if (std.mem.order(u8, request_lower, range_start) == .lt) range_start else request_lower;
}

pub fn minUpperDocKey(request_upper: []const u8, range_end: ?[]const u8) []const u8 {
    const end = range_end orelse return request_upper;
    if (end.len == 0) return request_upper;
    if (request_upper.len == 0) return end;
    return if (std.mem.order(u8, request_upper, end) == .lt) request_upper else end;
}

pub fn foreignKeyIntegrityNowNs() u64 {
    return platform_clock.Clock.real().nowRealtimeNs();
}

pub fn resolveForeignKeyIntegrityGroupsEventually(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
    action: ForeignKeyIntegrityAction,
    lower_doc_key: []const u8,
    upper_doc_key: []const u8,
) ![]u64 {
    if (action == .explain_delete) {
        const group_id = (try table_catalog.resolveGroupForKey(alloc, catalog, table_name, lower_doc_key)) orelse return try alloc.alloc(u64, 0);
        const groups = try alloc.alloc(u64, 1);
        groups[0] = group_id;
        return groups;
    }
    return try table_catalog.resolveGroupsForSpanEventually(
        alloc,
        catalog,
        table_name,
        lower_doc_key,
        upper_doc_key,
        5 * std.time.ns_per_s,
        10,
    );
}

pub fn foreignKeyIntegrityProgressModeForAction(
    action: ForeignKeyIntegrityAction,
) ?db_mod.relational_store.ForeignKeyIntegrityMode {
    return switch (action) {
        .validate, .list => .validate,
        .dry_run => .dry_run,
        .repair => .repair,
        .plan, .explain_delete, .progress => null,
    };
}

pub fn foreignKeyIntegrityResultFromRoutedExplain(
    alloc: std.mem.Allocator,
    action: ForeignKeyIntegrityAction,
    violation_limit: usize,
    explain: distributed_txn.ForeignKeyDeleteExplain,
) !ForeignKeyIntegrityResult {
    const groups = try alloc.alloc(ForeignKeyIntegrityGroupReport, 1);
    errdefer alloc.free(groups);
    groups[0] = .{
        .group_id = explain.parent_group_id,
        .report = .{},
    };
    return .{
        .action = action,
        .valid = explain.plan.allowed,
        .complete = true,
        .violation_limit = violation_limit,
        .violations_truncated = false,
        .report = .{},
        .delete_plan = explain.plan,
        .groups = groups,
        .progress = &.{},
        .work_units = &.{},
        .work_claims = &.{},
        .work_statuses = &.{},
        .violations = &.{},
    };
}

pub fn runForeignKeyIntegrityOnDb(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    group_id: u64,
    action: ForeignKeyIntegrityAction,
    constraint_name: ?[]const u8,
    lower_doc_key: []const u8,
    upper_doc_key: []const u8,
    violation_limit: usize,
) !ForeignKeyIntegrityResult {
    const report = switch (action) {
        .plan => db_mod.relational_store.ForeignKeyIntegrityReport{},
        .validate, .list => try db.validateForeignKeyRefsInRangeForConstraint(constraint_name, lower_doc_key, upper_doc_key),
        .dry_run => try db.dryRunRepairForeignKeyRefsInRangeForConstraint(constraint_name, lower_doc_key, upper_doc_key),
        .repair => try db.repairForeignKeyRefsInRangeForConstraint(constraint_name, lower_doc_key, upper_doc_key),
        .explain_delete, .progress => db_mod.relational_store.ForeignKeyIntegrityReport{},
    };

    const delete_plan = if (action == .explain_delete)
        try db.explainForeignKeyDeleteForConstraint(constraint_name, lower_doc_key)
    else
        null;

    var raw_violations = if (action == .plan or action == .explain_delete or action == .progress)
        try alloc.alloc(db_mod.relational_store.ForeignKeyIntegrityViolation, 0)
    else
        try db.listForeignKeyViolationsInRangeForConstraint(constraint_name, lower_doc_key, upper_doc_key);
    defer db.freeForeignKeyIntegrityViolations(raw_violations);

    var violations = std.ArrayListUnmanaged(ForeignKeyIntegrityViolation).empty;
    errdefer {
        for (violations.items) |*violation| violation.deinit(alloc);
        violations.deinit(alloc);
    }
    const copy_count = @min(violation_limit, raw_violations.len);
    try violations.ensureTotalCapacity(alloc, copy_count);
    for (raw_violations[0..copy_count]) |violation| {
        violations.appendAssumeCapacity(try cloneForeignKeyIntegrityViolation(alloc, group_id, violation));
    }

    const groups = try alloc.alloc(ForeignKeyIntegrityGroupReport, 1);
    errdefer alloc.free(groups);
    groups[0] = .{ .group_id = group_id, .report = report };

    var progress = std.ArrayListUnmanaged(ForeignKeyIntegrityProgress).empty;
    errdefer {
        for (progress.items) |*entry| entry.deinit(alloc);
        progress.deinit(alloc);
    }
    if (foreignKeyIntegrityProgressModeForAction(action)) |mode| {
        if (try db.loadForeignKeyIntegrityProgressRecord(mode, constraint_name, lower_doc_key, upper_doc_key)) |record| {
            defer db.freeForeignKeyIntegrityProgressRecord(record);
            try progress.append(alloc, try cloneForeignKeyIntegrityProgressRecord(alloc, group_id, record));
        }
    } else if (action == .progress or action == .plan) {
        const records = try db.listForeignKeyIntegrityProgressRecords();
        defer db.freeForeignKeyIntegrityProgressRecords(records);
        try progress.ensureUnusedCapacity(alloc, records.len);
        for (records) |record| {
            progress.appendAssumeCapacity(try cloneForeignKeyIntegrityProgressRecord(alloc, group_id, record));
        }
    }

    var progress_valid = true;
    if (action == .progress) {
        for (progress.items) |entry| progress_valid = progress_valid and entry.valid;
    }

    var work_claims = std.ArrayListUnmanaged(ForeignKeyIntegrityWorkClaim).empty;
    errdefer {
        for (work_claims.items) |*claim| claim.deinit(alloc);
        work_claims.deinit(alloc);
    }
    const claim_records = try db.listForeignKeyIntegrityClaimRecords();
    defer db.freeForeignKeyIntegrityClaimRecords(claim_records);
    try work_claims.ensureUnusedCapacity(alloc, claim_records.len);
    for (claim_records) |claim| {
        work_claims.appendAssumeCapacity(try cloneForeignKeyIntegrityWorkClaimRecord(alloc, claim));
    }

    var jobs = std.ArrayListUnmanaged(ForeignKeyIntegrityJobStatus).empty;
    errdefer {
        for (jobs.items) |*job| job.deinit(alloc);
        jobs.deinit(alloc);
    }
    if (action == .progress) {
        const job_records = try db.listForeignKeyIntegrityJobRecords();
        defer db.freeForeignKeyIntegrityJobRecords(job_records);
        try jobs.ensureUnusedCapacity(alloc, job_records.len);
        for (job_records) |job| {
            jobs.appendAssumeCapacity(try cloneForeignKeyIntegrityJobRecord(alloc, group_id, job));
        }
    }

    const include_work_unit = switch (action) {
        .plan, .validate, .dry_run, .repair, .list => true,
        .explain_delete, .progress => false,
    };
    const work_units: []ForeignKeyIntegrityWorkUnit = if (include_work_unit)
        try foreignKeyIntegritySingleWorkUnit(alloc, group_id, action, constraint_name, lower_doc_key, upper_doc_key)
    else
        try alloc.alloc(ForeignKeyIntegrityWorkUnit, 0);
    errdefer if (work_units.len > 0) {
        for (work_units) |*unit| unit.deinit(alloc);
        alloc.free(work_units);
    };
    const work_statuses = try buildForeignKeyIntegrityWorkStatuses(
        alloc,
        work_units,
        progress.items,
        work_claims.items,
        if (action == .plan) .planned else .pending,
    );
    errdefer {
        for (work_statuses) |*status| status.deinit(alloc);
        if (work_statuses.len > 0) alloc.free(work_statuses);
    }

    return .{
        .action = action,
        .valid = if (action == .plan) true else if (action == .progress) progress_valid else if (delete_plan) |plan| plan.allowed else raw_violations.len == 0,
        .complete = true,
        .violation_limit = violation_limit,
        .violations_truncated = raw_violations.len > violation_limit,
        .report = report,
        .delete_plan = delete_plan,
        .groups = groups,
        .progress = try progress.toOwnedSlice(alloc),
        .work_units = work_units,
        .work_claims = try work_claims.toOwnedSlice(alloc),
        .work_statuses = work_statuses,
        .jobs = try jobs.toOwnedSlice(alloc),
        .violations = try violations.toOwnedSlice(alloc),
    };
}

pub fn runForeignKeyIntegrityClaimedWorkUnitOnDb(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    group_id: u64,
    action: ForeignKeyIntegrityAction,
    phase: []const u8,
    claim_key: []const u8,
    worker_id: []const u8,
    lease_ms: u64,
    constraint_name: ?[]const u8,
    lower_doc_key: []const u8,
    upper_doc_key: []const u8,
    violation_limit: usize,
) !ForeignKeyIntegrityResult {
    const mode = foreignKeyIntegrityProgressModeForAction(action) orelse return error.InvalidForeignKeyIntegrityRequest;
    const report = try db.claimAndRunForeignKeyIntegrityWorkUnit(
        claim_key,
        worker_id,
        group_id,
        phase,
        mode,
        constraint_name,
        lower_doc_key,
        upper_doc_key,
        lease_ms,
    );

    var raw_violations: []db_mod.relational_store.ForeignKeyIntegrityViolation = &.{};
    if (std.mem.eql(u8, phase, "child_range")) {
        raw_violations = try db.listForeignKeyViolationsInRangeForConstraint(constraint_name, lower_doc_key, upper_doc_key);
    }
    defer if (raw_violations.len > 0) db.freeForeignKeyIntegrityViolations(raw_violations);

    var violations = std.ArrayListUnmanaged(ForeignKeyIntegrityViolation).empty;
    errdefer {
        for (violations.items) |*violation| violation.deinit(alloc);
        violations.deinit(alloc);
    }
    const copy_count = @min(violation_limit, raw_violations.len);
    try violations.ensureTotalCapacity(alloc, copy_count);
    for (raw_violations[0..copy_count]) |violation| {
        violations.appendAssumeCapacity(try cloneForeignKeyIntegrityViolation(alloc, group_id, violation));
    }

    const groups = try alloc.alloc(ForeignKeyIntegrityGroupReport, 1);
    errdefer alloc.free(groups);
    groups[0] = .{ .group_id = group_id, .report = report };

    var progress = std.ArrayListUnmanaged(ForeignKeyIntegrityProgress).empty;
    errdefer {
        for (progress.items) |*entry| entry.deinit(alloc);
        progress.deinit(alloc);
    }
    if (try db.loadForeignKeyIntegrityProgressRecordForPhase(phase, mode, constraint_name, lower_doc_key, upper_doc_key)) |record| {
        defer db.freeForeignKeyIntegrityProgressRecord(record);
        try progress.append(alloc, try cloneForeignKeyIntegrityProgressRecord(alloc, group_id, record));
    }

    const claim_records = try db.listForeignKeyIntegrityClaimRecords();
    defer db.freeForeignKeyIntegrityClaimRecords(claim_records);
    var work_claims = std.ArrayListUnmanaged(ForeignKeyIntegrityWorkClaim).empty;
    errdefer {
        for (work_claims.items) |*claim| claim.deinit(alloc);
        work_claims.deinit(alloc);
    }
    try work_claims.ensureUnusedCapacity(alloc, claim_records.len);
    for (claim_records) |claim| {
        work_claims.appendAssumeCapacity(try cloneForeignKeyIntegrityWorkClaimRecord(alloc, claim));
    }

    const work_units = try foreignKeyIntegritySingleWorkUnitForPhase(alloc, group_id, phase, action, constraint_name, lower_doc_key, upper_doc_key);
    errdefer {
        for (work_units) |*unit| unit.deinit(alloc);
        alloc.free(work_units);
    }
    const work_statuses = try buildForeignKeyIntegrityWorkStatuses(
        alloc,
        work_units,
        progress.items,
        work_claims.items,
        .pending,
    );
    errdefer {
        for (work_statuses) |*status| status.deinit(alloc);
        if (work_statuses.len > 0) alloc.free(work_statuses);
    }

    return .{
        .action = action,
        .valid = if (mode == .repair) report.missing_parent_rows == 0 else report.valid(),
        .complete = true,
        .violation_limit = violation_limit,
        .violations_truncated = raw_violations.len > violation_limit,
        .report = report,
        .delete_plan = null,
        .groups = groups,
        .progress = try progress.toOwnedSlice(alloc),
        .work_units = work_units,
        .work_claims = try work_claims.toOwnedSlice(alloc),
        .work_statuses = work_statuses,
        .violations = try violations.toOwnedSlice(alloc),
    };
}

pub fn runUniqueConstraintIntegrityOnDb(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    group_id: u64,
    action: UniqueConstraintIntegrityAction,
    lower_doc_key: []const u8,
    upper_doc_key: []const u8,
) !UniqueConstraintIntegrityResult {
    const report = switch (action) {
        .validate => try db.validateUniqueConstraintRowsInRange(lower_doc_key, upper_doc_key),
        .dry_run => try db.dryRunRepairUniqueConstraintRowsInRange(lower_doc_key, upper_doc_key),
        .repair => try db.repairUniqueConstraintRowsInRange(lower_doc_key, upper_doc_key),
        .progress => db_mod.relational_store.UniqueConstraintIntegrityReport{},
    };

    const groups = try alloc.alloc(UniqueConstraintIntegrityGroupReport, 1);
    errdefer alloc.free(groups);
    groups[0] = .{ .group_id = group_id, .report = report };

    var progress = std.ArrayListUnmanaged(UniqueConstraintIntegrityProgress).empty;
    errdefer {
        for (progress.items) |*entry| entry.deinit(alloc);
        progress.deinit(alloc);
    }
    if (uniqueConstraintIntegrityProgressModeForAction(action)) |mode| {
        if (try db.loadUniqueConstraintIntegrityProgressRecord(mode, lower_doc_key, upper_doc_key)) |record| {
            defer db.freeUniqueConstraintIntegrityProgressRecord(record);
            try progress.append(alloc, try cloneUniqueConstraintIntegrityProgressRecord(alloc, group_id, record));
        }
    } else if (action == .progress) {
        const records = try db.listUniqueConstraintIntegrityProgressRecords();
        defer db.freeUniqueConstraintIntegrityProgressRecords(records);
        try progress.ensureUnusedCapacity(alloc, records.len);
        for (records) |record| {
            progress.appendAssumeCapacity(try cloneUniqueConstraintIntegrityProgressRecord(alloc, group_id, record));
        }
    }

    var progress_valid = true;
    if (action == .progress) {
        for (progress.items) |entry| progress_valid = progress_valid and entry.valid;
    }

    return .{
        .action = action,
        .valid = if (action == .progress) progress_valid else report.valid(),
        .complete = true,
        .report = report,
        .groups = groups,
        .owner_topology = null,
        .progress = try progress.toOwnedSlice(alloc),
    };
}

pub fn stableForeignKeyActionPageTxnId(record: db_mod.DB.ForeignKeyActionJobRecord) db_mod.types.TxnId {
    var hi = std.hash.Wyhash.init(0x464b_4150_5458_4e31);
    var lo = std.hash.Wyhash.init(0x464b_4150_5458_4e32);
    hashForeignKeyActionPageTxnIdentity(&hi, record);
    hashForeignKeyActionPageTxnIdentity(&lo, record);
    var txn_id: db_mod.types.TxnId = undefined;
    std.mem.writeInt(u64, txn_id[0..8], hi.final(), .big);
    std.mem.writeInt(u64, txn_id[8..16], lo.final(), .big);
    return txn_id;
}

pub fn hashForeignKeyActionPageTxnIdentity(hasher: *std.hash.Wyhash, record: db_mod.DB.ForeignKeyActionJobRecord) void {
    hashActionPageTxnField(hasher, "foreign-key-action-page-v1");
    hashActionPageTxnField(hasher, record.job_id);
    hashActionPageTxnField(hasher, record.action);
    hashActionPageTxnField(hasher, record.constraint_name);
    hashActionPageTxnField(hasher, record.parent_table);
    hashActionPageTxnField(hasher, record.parent_key);
    hashActionPageTxnOptionalField(hasher, record.updated_parent_key);
    hashActionPageTxnOptionalField(hasher, record.next_child_table);
    hashActionPageTxnOptionalField(hasher, record.next_child_key);
    hashActionPageTxnU64(hasher, @intCast(record.page_limit));
    hashActionPageTxnU64(hasher, record.requeue_count);
    hashActionPageTxnU64(hasher, @intCast(record.cascade_depth));
    hashActionPageTxnU64(hasher, @intCast(record.cascade_max_depth));
}

pub fn hashActionPageTxnField(hasher: *std.hash.Wyhash, value: []const u8) void {
    hashActionPageTxnU64(hasher, @intCast(value.len));
    hasher.update(value);
}

pub fn hashActionPageTxnOptionalField(hasher: *std.hash.Wyhash, value: ?[]const u8) void {
    if (value) |text| {
        hasher.update(&.{1});
        hashActionPageTxnField(hasher, text);
    } else {
        hasher.update(&.{0});
    }
}

pub fn hashActionPageTxnU64(hasher: *std.hash.Wyhash, value: u64) void {
    var buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &buf, value, .big);
    hasher.update(&buf);
}

pub fn appendUniqueGroupId(
    alloc: std.mem.Allocator,
    group_ids: *std.ArrayListUnmanaged(u64),
    group_id: u64,
) !void {
    for (group_ids.items) |existing| {
        if (existing == group_id) return;
    }
    try group_ids.append(alloc, group_id);
}

pub fn collectForeignKeyActionJobProgressGroupIds(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
) ![]u64 {
    var snapshot = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&snapshot);
    const table = tables_api.findTableByName(&snapshot, table_name) orelse return error.TableNotFound;

    var group_ids = std.ArrayListUnmanaged(u64).empty;
    errdefer group_ids.deinit(alloc);
    for (snapshot.ranges) |range| {
        if (range.table_id == table.table_id) try appendUniqueGroupId(alloc, &group_ids, range.group_id);
    }
    for (snapshot.foreign_key_ref_ranges) |range| {
        if (range.child_table_id == table.table_id) try appendUniqueGroupId(alloc, &group_ids, range.group_id);
    }
    return try group_ids.toOwnedSlice(alloc);
}

pub fn cloneStorageForeignKeyIntegrityTupleValues(
    alloc: std.mem.Allocator,
    values: []const db_mod.relational_store.ForeignKeyIntegrityTupleValue,
) ![]ForeignKeyIntegrityTupleValue {
    var out = try alloc.alloc(ForeignKeyIntegrityTupleValue, values.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*value| value.deinit(alloc);
        if (out.len > 0) alloc.free(out);
    }
    for (values, 0..) |value, i| {
        const column = try alloc.dupe(u8, value.column);
        var column_transferred = false;
        errdefer if (!column_transferred) alloc.free(column);
        const scalar = try alloc.dupe(u8, value.value);
        var scalar_transferred = false;
        errdefer if (!scalar_transferred) alloc.free(scalar);
        out[i] = .{
            .column = column,
            .value = scalar,
        };
        column_transferred = true;
        scalar_transferred = true;
        initialized += 1;
    }
    return out;
}

pub fn cloneForeignKeyIntegrityTupleValues(
    alloc: std.mem.Allocator,
    values: []const ForeignKeyIntegrityTupleValue,
) ![]ForeignKeyIntegrityTupleValue {
    var out = try alloc.alloc(ForeignKeyIntegrityTupleValue, values.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*value| value.deinit(alloc);
        if (out.len > 0) alloc.free(out);
    }
    for (values, 0..) |value, i| {
        const column = try alloc.dupe(u8, value.column);
        var column_transferred = false;
        errdefer if (!column_transferred) alloc.free(column);
        const scalar = try alloc.dupe(u8, value.value);
        var scalar_transferred = false;
        errdefer if (!scalar_transferred) alloc.free(scalar);
        out[i] = .{
            .column = column,
            .value = scalar,
        };
        column_transferred = true;
        scalar_transferred = true;
        initialized += 1;
    }
    return out;
}

test "foreign key integrity stable job id uses planned action and bounds" {
    const alloc = std.testing.allocator;
    const validate = try stableForeignKeyIntegrityJobIdAlloc(
        alloc,
        "orders",
        .validate,
        "orders_customer_id_fkey",
        "order:1",
        "order:9",
    );
    defer alloc.free(validate);
    const plan = try stableForeignKeyIntegrityJobIdAlloc(
        alloc,
        "orders",
        .plan,
        "orders_customer_id_fkey",
        "order:1",
        "order:9",
    );
    defer alloc.free(plan);
    const repair = try stableForeignKeyIntegrityJobIdAlloc(
        alloc,
        "orders",
        .repair,
        "orders_customer_id_fkey",
        "order:1",
        "order:9",
    );
    defer alloc.free(repair);

    try std.testing.expectEqualStrings(validate, plan);
    try std.testing.expect(!std.mem.eql(u8, validate, repair));
}

test "foreign key integrity plan clips requested span to table ranges" {
    const alloc = std.testing.allocator;

    const FakeCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            const tables = struct {
                var items = [_]metadata_table_manager.TableRecord{.{
                    .table_id = 42,
                    .name = "orders",
                }};
            }.items[0..];
            const ranges = struct {
                var items = [_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 10, .table_id = 42, .start_key = "", .end_key = "m" },
                    .{ .group_id = 11, .table_id = 42, .start_key = "m", .end_key = "t" },
                    .{ .group_id = 12, .table_id = 42, .start_key = "t", .end_key = null },
                };
            }.items[0..];
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = tables,
                .ranges = ranges,
                .stores = &.{},
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const units = try planForeignKeyIntegrityWorkUnits(alloc, FakeCatalog.iface(), "orders", .plan, "orders_customer_id_fkey", "c", "w");
    defer {
        for (units) |*unit| unit.deinit(alloc);
        alloc.free(units);
    }

    try std.testing.expectEqual(@as(usize, 3), units.len);
    try std.testing.expectEqual(@as(u64, 10), units[0].group_id);
    try std.testing.expectEqualStrings("c", units[0].lower_doc_key);
    try std.testing.expectEqualStrings("m", units[0].upper_doc_key);
    try std.testing.expectEqual(@as(u64, 11), units[1].group_id);
    try std.testing.expectEqualStrings("m", units[1].lower_doc_key);
    try std.testing.expectEqualStrings("t", units[1].upper_doc_key);
    try std.testing.expectEqual(@as(u64, 12), units[2].group_id);
    try std.testing.expectEqualStrings("t", units[2].lower_doc_key);
    try std.testing.expectEqualStrings("w", units[2].upper_doc_key);
    for (units) |unit| {
        try std.testing.expectEqualStrings("child_range", unit.phase);
        try std.testing.expectEqualStrings("validate", unit.planned_action);
        try std.testing.expectEqualStrings("orders_customer_id_fkey", unit.constraint_name.?);
    }

    const repair_units = try planForeignKeyIntegrityWorkUnits(alloc, FakeCatalog.iface(), "orders", .repair, null, "m", "t");
    defer {
        for (repair_units) |*unit| unit.deinit(alloc);
        alloc.free(repair_units);
    }
    try std.testing.expectEqual(@as(usize, 1), repair_units.len);
    try std.testing.expectEqual(@as(u64, 11), repair_units[0].group_id);
    try std.testing.expectEqualStrings("repair", repair_units[0].planned_action);
    try std.testing.expect(repair_units[0].constraint_name == null);
    try std.testing.expectEqualStrings("m", repair_units[0].lower_doc_key);
    try std.testing.expectEqualStrings("t", repair_units[0].upper_doc_key);
}

test "foreign key integrity worker plan includes active owner ranges" {
    const alloc = std.testing.allocator;

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"customer_id":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"foreign_keys":[{"name":"orders_customer_id_fkey","columns":["customer_id"],"references":{"table":"customers","columns":["_id"]},"on_delete":"restrict"}]}
    ;

    const FakeCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            const tables = struct {
                var items = [_]metadata_table_manager.TableRecord{
                    .{
                        .table_id = 42,
                        .name = "orders",
                        .schema_json = schema_json,
                    },
                    .{
                        .table_id = 43,
                        .name = "customers",
                    },
                };
            }.items[0..];
            const ranges = struct {
                var items = [_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 10, .table_id = 42, .start_key = "", .end_key = null },
                };
            }.items[0..];
            const owner_ranges = struct {
                var items = [_]metadata_table_manager.ForeignKeyReferenceRangeRecord{
                    .{
                        .child_table_id = 42,
                        .constraint_name = "orders_customer_id_fkey",
                        .parent_table_id = 43,
                        .start_parent_key = "",
                        .end_parent_key = "customer:m",
                        .group_id = 200,
                    },
                    .{
                        .child_table_id = 42,
                        .constraint_name = "orders_customer_id_fkey",
                        .parent_table_id = 43,
                        .start_parent_key = "customer:m",
                        .end_parent_key = null,
                        .group_id = 201,
                    },
                    .{
                        .child_table_id = 42,
                        .constraint_name = "orders_customer_id_fkey",
                        .parent_table_id = 43,
                        .start_parent_key = "customer:z",
                        .end_parent_key = null,
                        .group_id = 202,
                        .state = metadata_table_manager.foreign_key_ref_range_rebuilding,
                    },
                };
            }.items[0..];
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = tables,
                .ranges = ranges,
                .foreign_key_ref_ranges = owner_ranges,
                .stores = &.{},
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const public_units = try planForeignKeyIntegrityWorkUnits(alloc, FakeCatalog.iface(), "orders", .validate, "orders_customer_id_fkey", "", "");
    defer {
        for (public_units) |*unit| unit.deinit(alloc);
        alloc.free(public_units);
    }
    try std.testing.expectEqual(@as(usize, 1), public_units.len);
    try std.testing.expectEqualStrings("child_range", public_units[0].phase);

    const worker_units = try planForeignKeyIntegrityWorkerWorkUnits(alloc, FakeCatalog.iface(), "orders", .repair, "orders_customer_id_fkey", "", "");
    defer {
        for (worker_units) |*unit| unit.deinit(alloc);
        alloc.free(worker_units);
    }
    try std.testing.expectEqual(@as(usize, 3), worker_units.len);
    try std.testing.expectEqualStrings("child_range", worker_units[0].phase);
    try std.testing.expectEqualStrings("owner_range", worker_units[1].phase);
    try std.testing.expectEqual(@as(u64, 200), worker_units[1].group_id);
    try std.testing.expectEqualStrings("", worker_units[1].lower_doc_key);
    try std.testing.expectEqualStrings("customer:m", worker_units[1].upper_doc_key);
    try std.testing.expectEqualStrings("owner_range", worker_units[2].phase);
    try std.testing.expectEqual(@as(u64, 201), worker_units[2].group_id);
    try std.testing.expectEqualStrings("customer:m", worker_units[2].lower_doc_key);
    try std.testing.expectEqualStrings("", worker_units[2].upper_doc_key);
    for (worker_units[1..]) |unit| {
        try std.testing.expectEqualStrings("repair", unit.planned_action);
        try std.testing.expectEqualStrings("orders_customer_id_fkey", unit.constraint_name.?);
    }
}

test "unique integrity owner topology inspection reports active and transitional ranges" {
    const alloc = std.testing.allocator;

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"},"username":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"unique_constraints":[{"name":"users_email_key","columns":["email"]},{"name":"users_username_key","columns":["username"]}]}
    ;

    const FakeCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            const tables = struct {
                var items = [_]metadata_table_manager.TableRecord{.{
                    .table_id = 42,
                    .name = "users",
                    .schema_json = schema_json,
                }};
            }.items[0..];
            const unique_ranges = struct {
                var items = [_]metadata_table_manager.UniqueConstraintRangeRecord{
                    .{
                        .table_id = 42,
                        .constraint_name = "users_email_key",
                        .start_encoded_value = "",
                        .end_encoded_value = "m",
                        .group_id = 100,
                        .topology_epoch = 7,
                        .state = metadata_table_manager.unique_constraint_range_active,
                    },
                    .{
                        .table_id = 42,
                        .constraint_name = "users_email_key",
                        .start_encoded_value = "m",
                        .end_encoded_value = null,
                        .group_id = 101,
                        .topology_epoch = 8,
                        .state = metadata_table_manager.unique_constraint_range_active,
                    },
                    .{
                        .table_id = 42,
                        .constraint_name = "users_username_key",
                        .start_encoded_value = "",
                        .end_encoded_value = null,
                        .group_id = 102,
                        .topology_epoch = 9,
                        .state = metadata_table_manager.unique_constraint_range_rebuilding,
                    },
                    .{
                        .table_id = 42,
                        .constraint_name = db_mod.relational_store.primary_key_constraint_name,
                        .start_encoded_value = "",
                        .end_encoded_value = null,
                        .group_id = 103,
                        .topology_epoch = 10,
                        .state = metadata_table_manager.unique_constraint_range_active,
                    },
                };
            }.items[0..];
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = tables,
                .ranges = &.{},
                .unique_constraint_ranges = unique_ranges,
                .stores = &.{},
                .placement_intents = &.{},
                .split_transitions = &.{},
                .merge_transitions = &.{},
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    var topology = (try inspectUniqueConstraintOwnerTopology(alloc, FakeCatalog.iface(), "users")).?;
    defer topology.deinit(alloc);
    try std.testing.expect(topology.configured);
    try std.testing.expect(!topology.complete);
    try std.testing.expectEqual(@as(usize, 3), topology.declared_constraints);
    try std.testing.expectEqual(@as(usize, 3), topology.configured_constraints);
    try std.testing.expectEqual(@as(usize, 3), topology.active_ranges);
    try std.testing.expectEqual(@as(usize, 1), topology.transitional_ranges);
    try std.testing.expectEqual(@as(usize, 4), topology.ranges.len);
    try std.testing.expect(topology.topology_epoch != 0);
    try std.testing.expectEqualStrings("users_email_key", topology.ranges[0].constraint_name);
    try std.testing.expect(topology.ranges[0].active);
    try std.testing.expectEqualStrings("users_username_key", topology.ranges[2].constraint_name);
    try std.testing.expect(!topology.ranges[2].active);
}

test "foreign key integrity job diagnostics merge samples across passes" {
    const alloc = std.testing.allocator;
    const old_samples =
        \\[{"group_id":7,"kind":"missing_parent","constraint_name":"orders_customer_id_fkey","child_table":"row","child_key":"order:old","parent_table":"customers","parent_key":"customer:old","parent_values":[],"observed_parent_values":[]}]
    ;
    const existing: db_mod.DB.ForeignKeyIntegrityJobRecord = .{
        .job_id = "job:fk",
        .table_name = "orders",
        .action = "validate",
        .worker_id = "worker",
        .constraint_name = "orders_customer_id_fkey",
        .lower_doc_key = "",
        .upper_doc_key = "",
        .lease_ms = 60_000,
        .max_work_units = 1,
        .status = "running",
        .created_at_ns = 1,
        .updated_at_ns = 2,
        .violation_samples_json = old_samples,
        .violation_sample_count = 1,
    };

    const old_parent_values = try alloc.alloc(ForeignKeyIntegrityTupleValue, 1);
    old_parent_values[0] = .{
        .column = try alloc.dupe(u8, "email"),
        .value = try alloc.dupe(u8, "old@example.test"),
    };
    var new_violations = [_]ForeignKeyIntegrityViolation{
        .{
            .group_id = 7,
            .kind = .missing_parent,
            .constraint_name = try alloc.dupe(u8, "orders_customer_id_fkey"),
            .child_table = try alloc.dupe(u8, "row"),
            .child_key = try alloc.dupe(u8, "order:old"),
            .parent_table = try alloc.dupe(u8, "customers"),
            .parent_key = try alloc.dupe(u8, "customer:old"),
        },
        .{
            .group_id = 8,
            .kind = .missing_parent,
            .constraint_name = try alloc.dupe(u8, "orders_customer_id_fkey"),
            .child_table = try alloc.dupe(u8, "row"),
            .child_key = try alloc.dupe(u8, "order:new"),
            .parent_table = try alloc.dupe(u8, "customers"),
            .parent_key = try alloc.dupe(u8, "customer:new"),
        },
        .{
            .group_id = 7,
            .kind = .missing_parent,
            .constraint_name = try alloc.dupe(u8, "orders_customer_id_fkey"),
            .child_table = try alloc.dupe(u8, "row"),
            .child_key = try alloc.dupe(u8, "order:old"),
            .parent_table = try alloc.dupe(u8, "customers"),
            .parent_key = try alloc.dupe(u8, "customer:old"),
            .parent_values = old_parent_values,
        },
    };
    defer {
        for (&new_violations) |*violation| violation.deinit(alloc);
    }
    const result: ForeignKeyIntegrityResult = .{
        .action = .validate,
        .valid = false,
        .complete = true,
        .violation_limit = 100,
        .violations_truncated = false,
        .report = .{ .missing_parent_rows = 2 },
        .groups = &.{},
        .violations = new_violations[0..],
    };

    var diagnostics = try foreignKeyIntegrityJobDiagnosticsAlloc(alloc, existing, result);
    defer diagnostics.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 3), diagnostics.sample_count);
    try std.testing.expect(!diagnostics.truncated);
    try std.testing.expect(std.mem.indexOf(u8, diagnostics.samples_json, "\"child_key\":\"order:old\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, diagnostics.samples_json, "\"child_key\":\"order:new\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, diagnostics.samples_json, "old@example.test") != null);
}

test "foreign key integrity job records diagnostics across incomplete passes" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-api-fk-job-pass-diagnostics";
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};

    var db = try db_mod.DB.open(alloc, path, .{});
    defer db.close();

    try startForeignKeyIntegrityJobOnDb(
        &db,
        "job:fk:diagnostics",
        "orders",
        .validate,
        "worker:first",
        "orders_customer_id_fkey",
        "",
        "",
        60_000,
        2,
    );

    var first_violations = [_]ForeignKeyIntegrityViolation{.{
        .group_id = 7,
        .kind = .missing_parent,
        .constraint_name = try alloc.dupe(u8, "orders_customer_id_fkey"),
        .child_table = try alloc.dupe(u8, "row"),
        .child_key = try alloc.dupe(u8, "order:first"),
        .parent_table = try alloc.dupe(u8, "customers"),
        .parent_key = try alloc.dupe(u8, "customer:first"),
    }};
    defer first_violations[0].deinit(alloc);
    const first_result: ForeignKeyIntegrityResult = .{
        .action = .validate,
        .valid = false,
        .complete = false,
        .violation_limit = 100,
        .violations_truncated = false,
        .report = .{ .missing_parent_rows = 1 },
        .groups = &.{},
        .violations = first_violations[0..],
    };
    try finishForeignKeyIntegrityJobOnDb(alloc, &db, "job:fk:diagnostics", first_result);

    {
        const running = (try db.loadForeignKeyIntegrityJobRecord("job:fk:diagnostics")) orelse return error.TestUnexpectedResult;
        defer db.freeForeignKeyIntegrityJobRecord(running);
        try std.testing.expectEqualStrings("running", running.status);
        try std.testing.expect(!running.completed);
        try std.testing.expect(running.valid == null);
        try std.testing.expectEqual(@as(u64, 1), running.last_report.missing_parent_rows);
        try std.testing.expectEqual(@as(usize, 1), running.violation_sample_count);
        try std.testing.expect(std.mem.indexOf(u8, running.violation_samples_json, "order:first") != null);
        try std.testing.expectEqual(@as(u64, 1), running.diagnostic_passes);
        try std.testing.expectEqual(@as(u64, 1), running.violating_passes);
        try std.testing.expect(running.first_violation_at_ns != null);
        try std.testing.expect(running.last_violation_at_ns != null);
    }

    try startForeignKeyIntegrityJobOnDb(
        &db,
        "job:fk:diagnostics",
        "orders",
        .validate,
        "worker:second",
        "orders_customer_id_fkey",
        "",
        "",
        60_000,
        2,
    );
    {
        const resumed = (try db.loadForeignKeyIntegrityJobRecord("job:fk:diagnostics")) orelse return error.TestUnexpectedResult;
        defer db.freeForeignKeyIntegrityJobRecord(resumed);
        try std.testing.expectEqualStrings("worker:second", resumed.worker_id);
        try std.testing.expectEqual(@as(u64, 1), resumed.last_report.missing_parent_rows);
        try std.testing.expectEqual(@as(usize, 1), resumed.violation_sample_count);
        try std.testing.expectEqual(@as(u64, 1), resumed.diagnostic_passes);
        try std.testing.expectEqual(@as(u64, 1), resumed.violating_passes);
    }

    const second_result: ForeignKeyIntegrityResult = .{
        .action = .validate,
        .valid = false,
        .complete = true,
        .violation_limit = 100,
        .violations_truncated = false,
        .report = .{ .missing_ref_rows = 1 },
        .groups = &.{},
        .violations = &.{},
    };
    try finishForeignKeyIntegrityJobOnDb(alloc, &db, "job:fk:diagnostics", second_result);

    const completed = (try db.loadForeignKeyIntegrityJobRecord("job:fk:diagnostics")) orelse return error.TestUnexpectedResult;
    defer db.freeForeignKeyIntegrityJobRecord(completed);
    try std.testing.expectEqualStrings("invalid", completed.status);
    try std.testing.expect(completed.completed);
    try std.testing.expect(!completed.valid.?);
    try std.testing.expectEqual(@as(u64, 0), completed.last_report.missing_parent_rows);
    try std.testing.expectEqual(@as(u64, 1), completed.last_report.missing_ref_rows);
    try std.testing.expectEqual(@as(u64, 1), completed.aggregate_report.missing_parent_rows);
    try std.testing.expectEqual(@as(u64, 1), completed.aggregate_report.missing_ref_rows);
    try std.testing.expectEqual(@as(usize, 1), completed.violation_sample_count);
    try std.testing.expect(std.mem.indexOf(u8, completed.violation_samples_json, "order:first") != null);
    try std.testing.expectEqual(@as(u64, 2), completed.diagnostic_passes);
    try std.testing.expectEqual(@as(u64, 2), completed.violating_passes);
    try std.testing.expect(completed.first_violation_at_ns != null);
    try std.testing.expect(completed.last_violation_at_ns != null);
    try std.testing.expect(completed.last_violation_at_ns.? >= completed.first_violation_at_ns.?);

    var hydrated_result: ForeignKeyIntegrityResult = .{
        .job_id = try alloc.dupe(u8, "job:fk:diagnostics"),
        .action = .validate,
        .valid = false,
        .complete = true,
        .violation_limit = 100,
        .violations_truncated = false,
        .report = .{ .missing_ref_rows = 1 },
        .groups = try alloc.alloc(ForeignKeyIntegrityGroupReport, 0),
        .violations = try alloc.alloc(ForeignKeyIntegrityViolation, 0),
    };
    defer hydrated_result.deinit(alloc);
    try hydrateForeignKeyIntegrityResultDiagnosticsFromJobOnDb(alloc, &db, 7, &hydrated_result);
    try std.testing.expectEqual(@as(u64, 1), hydrated_result.report.missing_parent_rows);
    try std.testing.expectEqual(@as(u64, 1), hydrated_result.report.missing_ref_rows);
    try std.testing.expectEqual(@as(usize, 1), hydrated_result.violations.len);
    try std.testing.expectEqual(db_mod.relational_store.ForeignKeyIntegrityViolationKind.missing_parent, hydrated_result.violations[0].kind);
    try std.testing.expectEqualStrings("order:first", hydrated_result.violations[0].child_key);
    try std.testing.expectEqual(@as(usize, 1), hydrated_result.jobs.len);
    try std.testing.expectEqual(@as(u64, 7), hydrated_result.jobs[0].group_id);
    try std.testing.expectEqual(@as(u64, 2), hydrated_result.jobs[0].diagnostic_passes);
    try std.testing.expectEqual(@as(u64, 2), hydrated_result.jobs[0].violating_passes);
    try std.testing.expect(hydrated_result.jobs[0].first_violation_at_ns != null);
    try std.testing.expect(hydrated_result.jobs[0].last_violation_at_ns != null);
}

test "foreign key integrity diagnostics deduplicates violation samples" {
    const alloc = std.testing.allocator;
    var violations = try alloc.alloc(ForeignKeyIntegrityViolation, 2);
    var initialized: usize = 0;
    errdefer {
        for (violations[0..initialized]) |*violation| violation.deinit(alloc);
        alloc.free(violations);
    }
    violations[0] = try testViolation(alloc, "order:1");
    initialized += 1;
    violations[1] = try cloneForeignKeyIntegrityResultViolation(alloc, violations[0]);
    initialized += 1;

    var result = ForeignKeyIntegrityResult{
        .action = .validate,
        .valid = false,
        .complete = true,
        .violation_limit = 10,
        .violations_truncated = false,
        .report = .{ .missing_parent_rows = 2 },
        .groups = try alloc.alloc(integrity_types.ForeignKeyIntegrityGroupReport, 0),
        .violations = violations,
    };
    defer result.deinit(alloc);

    var diagnostics = try foreignKeyIntegrityJobDiagnosticsAlloc(alloc, null, result);
    defer diagnostics.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), diagnostics.sample_count);
    try std.testing.expect(!diagnostics.truncated);
    try std.testing.expect(std.mem.indexOf(u8, diagnostics.samples_json, "\"child_key\":\"order:1\"") != null);
}

test "foreign key action schedule controller does not seed empty owner schedule" {
    const alloc = std.testing.allocator;

    const EmptySeedSource = struct {
        mark_seeded_calls: usize = 0,
        action_job_schedule_calls: usize = 0,

        fn source(self: *@This()) TableWriteSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .batch = batch,
                    .foreign_key_action_schedule_progress = actionScheduleProgress,
                    .foreign_key_action_job_schedule = actionJobSchedule,
                    .foreign_key_action_schedule_mark_seeded = markSeeded,
                },
            };
        }

        fn dup(alloc_inner: std.mem.Allocator, value: []const u8) ![]u8 {
            return try alloc_inner.dupe(u8, value);
        }

        fn batch(
            ptr: *anyopaque,
            alloc_inner: std.mem.Allocator,
            table_name: []const u8,
            req: db_mod.types.BatchRequest,
        ) !?void {
            _ = ptr;
            _ = alloc_inner;
            _ = table_name;
            _ = req;
            return null;
        }

        fn actionScheduleProgress(
            ptr: *anyopaque,
            alloc_inner: std.mem.Allocator,
            table_name: []const u8,
        ) !?ForeignKeyActionScheduleProgressResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("orders", table_name);
            const schedules = try alloc_inner.alloc(ForeignKeyActionScheduleStatus, 1);
            errdefer alloc_inner.free(schedules);
            const invalid = self.mark_seeded_calls > 0;
            schedules[0] = .{
                .group_id = 0,
                .version = 1,
                .schedule_id = try dup(alloc_inner, "fk-action-schedule:empty"),
                .action_job_id = try dup(alloc_inner, "fk-action:empty"),
                .action = try dup(alloc_inner, "set_null"),
                .worker_id = try dup(alloc_inner, "worker:initial"),
                .constraint_name = try dup(alloc_inner, "orders_customer_id_fkey"),
                .parent_table = try dup(alloc_inner, "customers"),
                .parent_key = try dup(alloc_inner, "customer:empty"),
                .page_limit = 16,
                .status = try dup(alloc_inner, if (invalid) "invalid" else "pending"),
                .created_at_ns = 1,
                .updated_at_ns = if (invalid) 2 else 1,
                .completed = false,
                .scheduled_groups = 0,
                .cascade_depth = 0,
                .cascade_max_depth = 64,
                .last_error = if (invalid) try dup(alloc_inner, "NoForeignKeyActionOwnerGroups") else null,
            };
            return .{ .schedules = schedules };
        }

        fn actionJobSchedule(
            ptr: *anyopaque,
            alloc_inner: std.mem.Allocator,
            table_name: []const u8,
            job_id: []const u8,
            action: []const u8,
            worker_id: []const u8,
            constraint_name: []const u8,
            parent_table: []const u8,
            parent_key: []const u8,
            updated_parent_key: ?[]const u8,
            page_limit: usize,
            cascade_depth: u32,
            cascade_max_depth: u32,
        ) !?ForeignKeyActionJobResult {
            _ = alloc_inner;
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.action_job_schedule_calls += 1;
            try std.testing.expectEqualStrings("orders", table_name);
            try std.testing.expectEqualStrings("fk-action:empty", job_id);
            try std.testing.expectEqualStrings("set_null", action);
            try std.testing.expectEqualStrings("worker:fk-action-controller", worker_id);
            try std.testing.expectEqualStrings("orders_customer_id_fkey", constraint_name);
            try std.testing.expectEqualStrings("customers", parent_table);
            try std.testing.expectEqualStrings("customer:empty", parent_key);
            try std.testing.expect(updated_parent_key == null);
            try std.testing.expectEqual(@as(usize, 16), page_limit);
            try std.testing.expectEqual(@as(u32, 0), cascade_depth);
            try std.testing.expectEqual(@as(u32, 64), cascade_max_depth);
            return ForeignKeyActionJobResult{
                .complete = true,
                .groups = &.{},
            };
        }

        fn markSeeded(
            ptr: *anyopaque,
            alloc_inner: std.mem.Allocator,
            table_name: []const u8,
            schedule_id: []const u8,
            scheduled_groups: u64,
        ) !?ForeignKeyActionScheduleStatus {
            try std.testing.expectEqualStrings("orders", table_name);
            try std.testing.expectEqualStrings("fk-action-schedule:empty", schedule_id);
            try std.testing.expectEqual(@as(u64, 0), scheduled_groups);
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.mark_seeded_calls += 1;
            return .{
                .group_id = 0,
                .version = 1,
                .schedule_id = try dup(alloc_inner, "fk-action-schedule:empty"),
                .action_job_id = try dup(alloc_inner, "fk-action:empty"),
                .action = try dup(alloc_inner, "set_null"),
                .worker_id = try dup(alloc_inner, "worker:initial"),
                .constraint_name = try dup(alloc_inner, "orders_customer_id_fkey"),
                .parent_table = try dup(alloc_inner, "customers"),
                .parent_key = try dup(alloc_inner, "customer:empty"),
                .page_limit = 16,
                .status = try dup(alloc_inner, "invalid"),
                .created_at_ns = 1,
                .updated_at_ns = 2,
                .completed = false,
                .scheduled_groups = 0,
                .last_error = try dup(alloc_inner, "NoForeignKeyActionOwnerGroups"),
            };
        }
    };

    var fake = EmptySeedSource{};
    var summary: ForeignKeyIntegritySchemaControllerResult = .{};
    var action_schedules = std.ArrayListUnmanaged(ForeignKeyActionScheduleStatus).empty;
    defer {
        for (action_schedules.items) |*schedule| schedule.deinit(alloc);
        action_schedules.deinit(alloc);
    }

    try runForeignKeyActionScheduleControllerMaintenanceForTable(
        alloc,
        fake.source(),
        "orders",
        .{
            .action = .repair,
            .worker_id = "worker:fk-action-controller",
            .max_action_jobs = 4,
            .action_job_page_limit = 16,
        },
        &summary,
        &action_schedules,
    );
    try std.testing.expect(!summary.complete);
    try std.testing.expect(!summary.valid);
    try std.testing.expectEqual(@as(usize, 1), summary.action_schedules_scanned);
    try std.testing.expectEqual(@as(usize, 0), summary.action_schedules_executed);
    try std.testing.expectEqual(@as(usize, 1), summary.action_schedules_invalid);
    try std.testing.expectEqual(@as(usize, 1), action_schedules.items.len);
    try std.testing.expectEqualStrings("invalid", action_schedules.items[0].status);
    try std.testing.expect(action_schedules.items[0].last_error != null);
    try std.testing.expectEqualStrings("NoForeignKeyActionOwnerGroups", action_schedules.items[0].last_error.?);
    try std.testing.expectEqual(@as(usize, 1), fake.mark_seeded_calls);
    try std.testing.expectEqual(@as(usize, 1), fake.action_job_schedule_calls);

    var retry_summary: ForeignKeyIntegritySchemaControllerResult = .{};
    var retry_action_schedules = std.ArrayListUnmanaged(ForeignKeyActionScheduleStatus).empty;
    defer {
        for (retry_action_schedules.items) |*schedule| schedule.deinit(alloc);
        retry_action_schedules.deinit(alloc);
    }
    try runForeignKeyActionScheduleControllerMaintenanceForTable(
        alloc,
        fake.source(),
        "orders",
        .{
            .action = .repair,
            .worker_id = "worker:fk-action-controller",
            .max_action_jobs = 4,
            .action_job_page_limit = 16,
        },
        &retry_summary,
        &retry_action_schedules,
    );
    try std.testing.expect(!retry_summary.complete);
    try std.testing.expect(!retry_summary.valid);
    try std.testing.expectEqual(@as(usize, 1), retry_summary.action_schedules_scanned);
    try std.testing.expectEqual(@as(usize, 0), retry_summary.action_schedules_executed);
    try std.testing.expectEqual(@as(usize, 1), retry_summary.action_schedules_invalid);
    try std.testing.expectEqual(@as(usize, 1), retry_action_schedules.items.len);
    try std.testing.expectEqualStrings("invalid", retry_action_schedules.items[0].status);
    try std.testing.expectEqual(@as(usize, 1), fake.mark_seeded_calls);
    try std.testing.expectEqual(@as(usize, 1), fake.action_job_schedule_calls);
}

const LocalUniqueConstraintIntegrityTestSource = struct {
    table_name: []const u8,
    db: *db_mod.DB,

    fn source(self: *@This()) TableWriteSource {
        return .{
            .ptr = self,
            .vtable = &.{
                .batch = batch,
                .unique_constraint_integrity = uniqueConstraintIntegrity,
            },
        };
    }

    fn batch(
        _: *anyopaque,
        _: std.mem.Allocator,
        _: []const u8,
        _: db_mod.types.BatchRequest,
    ) !?void {
        return error.UnsupportedOperation;
    }

    fn uniqueConstraintIntegrity(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        action: UniqueConstraintIntegrityAction,
        lower_doc_key: []const u8,
        upper_doc_key: []const u8,
    ) !?UniqueConstraintIntegrityResult {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        return try runUniqueConstraintIntegrityOnDb(alloc, self.db, 0, action, lower_doc_key, upper_doc_key);
    }
};

const LocalForeignKeyIntegrityTestSource = struct {
    table_name: []const u8,
    db: *db_mod.DB,

    fn source(self: *@This()) TableWriteSource {
        return .{
            .ptr = self,
            .vtable = &.{
                .batch = batch,
                .foreign_key_integrity = foreignKeyIntegrity,
                .foreign_key_integrity_worker_pass = foreignKeyIntegrityWorkerPass,
                .foreign_key_integrity_schema_controller_pass = foreignKeyIntegritySchemaControllerPass,
                .foreign_key_action_job_page = foreignKeyActionJobPage,
                .foreign_key_action_job_schedule = foreignKeyActionJobSchedule,
                .foreign_key_action_job_progress = foreignKeyActionJobProgress,
                .foreign_key_action_schedule_progress = foreignKeyActionScheduleProgress,
                .foreign_key_action_schedule_mark_seeded = foreignKeyActionScheduleMarkSeeded,
            },
        };
    }

    fn batch(
        _: *anyopaque,
        _: std.mem.Allocator,
        _: []const u8,
        _: db_mod.types.BatchRequest,
    ) !?void {
        return error.UnsupportedOperation;
    }

    fn foreignKeyIntegrity(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        action: ForeignKeyIntegrityAction,
        constraint_name: ?[]const u8,
        lower_doc_key: []const u8,
        upper_doc_key: []const u8,
        violation_limit: usize,
    ) !?ForeignKeyIntegrityResult {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        return try runForeignKeyIntegrityOnDb(alloc, self.db, 0, action, constraint_name, lower_doc_key, upper_doc_key, violation_limit);
    }

    fn foreignKeyIntegrityWorkerPass(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        action: ForeignKeyIntegrityAction,
        job_id: ?[]const u8,
        worker_id: []const u8,
        lease_ms: u64,
        max_work_units: usize,
        constraint_name: ?[]const u8,
        lower_doc_key: []const u8,
        upper_doc_key: []const u8,
        violation_limit: usize,
    ) !?ForeignKeyIntegrityResult {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        if (!foreignKeyIntegrityWorkerActionSupported(action)) return error.InvalidForeignKeyIntegrityRequest;
        try startForeignKeyIntegrityJobOnDb(self.db, job_id, table_name, action, worker_id, constraint_name, lower_doc_key, upper_doc_key, lease_ms, max_work_units);

        const planned_units = try foreignKeyIntegritySingleWorkUnit(alloc, 0, action, constraint_name, lower_doc_key, upper_doc_key);
        defer {
            for (planned_units) |*unit| unit.deinit(alloc);
            alloc.free(planned_units);
        }

        var status_snapshot = try runForeignKeyIntegrityOnDb(alloc, self.db, 0, .progress, constraint_name, lower_doc_key, upper_doc_key, violation_limit);
        defer status_snapshot.deinit(alloc);
        const initial_statuses = try buildForeignKeyIntegrityWorkStatuses(alloc, planned_units, status_snapshot.progress, status_snapshot.work_claims, .pending);
        defer {
            for (initial_statuses) |*status| status.deinit(alloc);
            if (initial_statuses.len > 0) alloc.free(initial_statuses);
        }

        var aggregate: db_mod.relational_store.ForeignKeyIntegrityReport = .{};
        var group_reports = std.ArrayListUnmanaged(ForeignKeyIntegrityGroupReport).empty;
        var violations = std.ArrayListUnmanaged(ForeignKeyIntegrityViolation).empty;
        errdefer {
            group_reports.deinit(alloc);
            for (violations.items) |*violation| violation.deinit(alloc);
            violations.deinit(alloc);
        }
        var truncated = false;
        const now_ns = foreignKeyIntegrityNowNs();
        if (max_work_units > 0 and initial_statuses.len > 0 and foreignKeyIntegrityWorkStatusClaimable(initial_statuses[0], now_ns)) {
            var one = try runForeignKeyIntegrityClaimedWorkUnitOnDb(
                alloc,
                self.db,
                0,
                action,
                "child_range",
                initial_statuses[0].claim_key,
                worker_id,
                lease_ms,
                constraint_name,
                lower_doc_key,
                upper_doc_key,
                violation_limit,
            );
            defer one.deinit(alloc);
            mergeForeignKeyIntegrityReport(&aggregate, one.report);
            try group_reports.ensureUnusedCapacity(alloc, one.groups.len);
            for (one.groups) |group| group_reports.appendAssumeCapacity(group);
            const copy_count = @min(violation_limit, one.violations.len);
            try violations.ensureUnusedCapacity(alloc, copy_count);
            for (one.violations[0..copy_count]) |violation| {
                violations.appendAssumeCapacity(try cloneForeignKeyIntegrityResultViolation(alloc, violation));
            }
            truncated = one.violations_truncated or one.violations.len > violation_limit;
        }

        var final_snapshot = try runForeignKeyIntegrityOnDb(alloc, self.db, 0, .progress, constraint_name, lower_doc_key, upper_doc_key, violation_limit);
        defer final_snapshot.deinit(alloc);
        const work_units = try cloneForeignKeyIntegrityWorkUnits(alloc, planned_units);
        errdefer {
            for (work_units) |*unit| unit.deinit(alloc);
            if (work_units.len > 0) alloc.free(work_units);
        }
        const work_statuses = try buildForeignKeyIntegrityWorkStatuses(alloc, planned_units, final_snapshot.progress, final_snapshot.work_claims, .pending);
        errdefer {
            for (work_statuses) |*status| status.deinit(alloc);
            if (work_statuses.len > 0) alloc.free(work_statuses);
        }

        const valid = foreignKeyIntegrityWorkStatusesValid(work_statuses);
        const complete = !foreignKeyIntegrityWorkStatusesHaveClaimable(work_statuses, foreignKeyIntegrityNowNs());
        var result: ForeignKeyIntegrityResult = .{
            .action = action,
            .valid = valid,
            .complete = complete,
            .violation_limit = violation_limit,
            .violations_truncated = truncated,
            .report = aggregate,
            .groups = try group_reports.toOwnedSlice(alloc),
            .progress = try cloneForeignKeyIntegrityProgressSlice(alloc, final_snapshot.progress),
            .work_units = work_units,
            .work_claims = try cloneForeignKeyIntegrityWorkClaimSlice(alloc, final_snapshot.work_claims),
            .work_statuses = work_statuses,
            .violations = try violations.toOwnedSlice(alloc),
        };
        errdefer {
            var cleanup = result;
            cleanup.deinit(alloc);
        }
        try finishForeignKeyIntegrityJobOnDb(alloc, self.db, job_id, result);
        try attachForeignKeyIntegrityJobId(alloc, &result, job_id);
        try hydrateForeignKeyIntegrityResultDiagnosticsFromJobOnDb(alloc, self.db, 0, &result);
        return result;
    }

    fn foreignKeyIntegritySchemaControllerPass(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        action: ForeignKeyIntegrityAction,
        worker_id: []const u8,
        lease_ms: u64,
        max_work_units: usize,
        constraint_name: ?[]const u8,
        lower_doc_key: []const u8,
        upper_doc_key: []const u8,
        violation_limit: usize,
    ) !?ForeignKeyIntegrityResult {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        const schema_json = (try loadLocalTableSchemaJson(alloc, self.db)) orelse {
            return try emptyForeignKeyIntegrityControllerResult(alloc, action, violation_limit);
        };
        defer alloc.free(schema_json);
        return try foreignKeyIntegritySchemaControllerPassWithSchemaJson(
            alloc,
            self.source(),
            table_name,
            schema_json,
            action,
            worker_id,
            lease_ms,
            max_work_units,
            constraint_name,
            lower_doc_key,
            upper_doc_key,
            violation_limit,
        );
    }

    fn foreignKeyActionJobPage(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        job_id: []const u8,
        action: []const u8,
        worker_id: []const u8,
        constraint_name: []const u8,
        parent_table: []const u8,
        parent_key: []const u8,
        updated_parent_key: ?[]const u8,
        page_limit: usize,
        lease_ms: u64,
    ) !?ForeignKeyActionJobResult {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        const record = try self.db.claimAndRunForeignKeyActionJobPageWithUpdatedParentKeyAt(
            job_id,
            action,
            worker_id,
            constraint_name,
            parent_table,
            parent_key,
            updated_parent_key,
            page_limit,
            lease_ms,
            table_write_core.nextTxnTimestamp(),
        );
        defer self.db.freeForeignKeyActionJobRecord(record);
        const groups = try alloc.alloc(ForeignKeyActionJobStatus, 1);
        errdefer alloc.free(groups);
        groups[0] = try foreignKeyActionJobStatusFromDbRecord(alloc, 0, record);
        errdefer groups[0].deinit(alloc);
        return .{
            .complete = groups[0].completed,
            .groups = groups,
        };
    }

    fn foreignKeyActionJobSchedule(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        job_id: []const u8,
        action: []const u8,
        worker_id: []const u8,
        constraint_name: []const u8,
        parent_table: []const u8,
        parent_key: []const u8,
        updated_parent_key: ?[]const u8,
        page_limit: usize,
        cascade_depth: u32,
        cascade_max_depth: u32,
    ) !?ForeignKeyActionJobResult {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        const record = try self.db.scheduleForeignKeyActionJobWithUpdatedParentKeyAndCascadeLineageAt(
            job_id,
            action,
            worker_id,
            constraint_name,
            parent_table,
            parent_key,
            updated_parent_key,
            page_limit,
            cascade_depth,
            cascade_max_depth,
            table_write_core.nextTxnTimestamp(),
        );
        defer self.db.freeForeignKeyActionJobRecord(record);
        const groups = try alloc.alloc(ForeignKeyActionJobStatus, 1);
        errdefer alloc.free(groups);
        groups[0] = try foreignKeyActionJobStatusFromDbRecord(alloc, 0, record);
        errdefer groups[0].deinit(alloc);
        return .{
            .complete = groups[0].completed,
            .groups = groups,
        };
    }

    fn foreignKeyActionJobProgress(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
    ) !?ForeignKeyActionJobProgressResult {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        const records = try self.db.listForeignKeyActionJobRecords();
        defer self.db.freeForeignKeyActionJobRecords(records);
        return try foreignKeyActionJobProgressFromDbRecords(alloc, 0, records);
    }

    fn foreignKeyActionScheduleProgress(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
    ) !?ForeignKeyActionScheduleProgressResult {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        const records = try self.db.listForeignKeyActionScheduleRecords();
        defer self.db.freeForeignKeyActionScheduleRecords(records);
        return try foreignKeyActionScheduleProgressFromDbRecords(alloc, 0, records);
    }

    fn foreignKeyActionScheduleMarkSeeded(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        schedule_id: []const u8,
        scheduled_groups: u64,
    ) !?ForeignKeyActionScheduleStatus {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (!std.mem.eql(u8, table_name, self.table_name)) return null;
        const record = try self.db.markForeignKeyActionScheduleSeeded(schedule_id, scheduled_groups);
        defer self.db.freeForeignKeyActionScheduleRecord(record);
        return try foreignKeyActionScheduleStatusFromDbRecord(alloc, 0, record);
    }
};

test "foreign key local schema controller repairs unvalidated constraint" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/fk-schema-controller-maintenance/table-db", .{tmp.sub_path});
    defer alloc.free(path);

    var db = try db_mod.DB.open(alloc, path, .{});
    defer db.close();
    try db.applyTableSchemaJson(
        alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"customer_id":{"type":"keyword"}},"additionalProperties":false}}},"foreign_keys":[{"name":"orders_customer_id_fkey","columns":["customer_id"],"references":{"table":"customers","columns":["_id"]},"on_delete":"restrict","validation_state":"unvalidated"}]}
    ,
        .{},
    );
    try db.batch(.{
        .writes = &.{
            .{ .key = "customer:a", .value = "{\"_type\":\"customers\"}" },
            .{ .key = "order:1", .value = "{\"customer_id\":\"customer:a\"}" },
        },
        .sync_level = .write,
    });

    const before = try db.validateForeignKeyRefsInRangeForConstraint("orders_customer_id_fkey", "", "");
    try std.testing.expectEqual(@as(u64, 1), before.missing_ref_rows);

    var source = LocalForeignKeyIntegrityTestSource{ .table_name = "orders", .db = &db };
    var summary = try runLocalForeignKeyIntegritySchemaControllerMaintenancePass(
        alloc,
        source.source(),
        &db,
        "orders",
        .{
            .action = .repair,
            .worker_id = "worker:fk-maintenance",
            .max_tables = 4,
            .max_work_units_per_table = 1,
        },
    );
    defer summary.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), summary.tables_scanned);
    try std.testing.expectEqual(@as(usize, 1), summary.tables_with_pending_constraints);
    try std.testing.expectEqual(@as(usize, 1), summary.tables_executed);
    try std.testing.expect(summary.complete);
    try std.testing.expect(summary.valid);
    try std.testing.expectEqual(@as(usize, 1), summary.terminal_valid_results);
    try std.testing.expectEqual(@as(usize, 0), summary.terminal_invalid_results);
    try std.testing.expectEqual(@as(usize, 1), summary.results.len);
    try std.testing.expectEqualStrings("orders", summary.results[0].table_name);
    try std.testing.expect(summary.results[0].result.job_id != null);
    try std.testing.expect(std.mem.startsWith(u8, summary.results[0].result.job_id.?, "fk-integrity:"));
    try std.testing.expectEqual(@as(u64, 1), summary.results[0].result.report.repaired_ref_rows);
    try std.testing.expectEqual(@as(usize, 1), summary.results[0].result.work_units.len);
    try std.testing.expectEqualStrings("orders_customer_id_fkey", summary.results[0].result.work_units[0].constraint_name.?);

    const after = try db.validateForeignKeyRefsInRangeForConstraint("orders_customer_id_fkey", "", "");
    try std.testing.expectEqual(@as(u64, 0), after.missing_ref_rows);
    try std.testing.expectEqual(@as(u64, 0), after.missing_parent_rows);

    const promoted_schema_json = (try loadLocalTableSchemaJson(alloc, &db)) orelse return error.TestUnexpectedResult;
    defer alloc.free(promoted_schema_json);
    try std.testing.expect(std.mem.indexOf(u8, promoted_schema_json, "\"validation_state\":\"enforced\"") != null);

    var second_summary = try runLocalForeignKeyIntegritySchemaControllerMaintenancePass(
        alloc,
        source.source(),
        &db,
        "orders",
        .{
            .action = .repair,
            .worker_id = "worker:fk-maintenance",
            .max_tables = 4,
            .max_work_units_per_table = 1,
        },
    );
    defer second_summary.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), second_summary.tables_scanned);
    try std.testing.expectEqual(@as(usize, 0), second_summary.tables_with_pending_constraints);
    try std.testing.expectEqual(@as(usize, 0), second_summary.tables_executed);
    try std.testing.expect(second_summary.complete);
    try std.testing.expect(second_summary.valid);
    try std.testing.expectEqual(@as(usize, 0), second_summary.terminal_valid_results);
    try std.testing.expectEqual(@as(usize, 0), second_summary.terminal_invalid_results);
    try std.testing.expectEqual(@as(usize, 0), second_summary.results.len);

    const job = (try db.loadForeignKeyIntegrityJobRecord(summary.results[0].result.job_id.?)) orelse return error.TestUnexpectedResult;
    defer db.freeForeignKeyIntegrityJobRecord(job);
    try std.testing.expectEqualStrings("orders", job.table_name);
    try std.testing.expectEqualStrings("repair", job.action);
    try std.testing.expectEqualStrings("worker:fk-maintenance", job.worker_id);
    try std.testing.expectEqualStrings("orders_customer_id_fkey", job.constraint_name.?);
    try std.testing.expectEqualStrings("complete", job.status);
    try std.testing.expect(job.completed);
    try std.testing.expect(job.valid.?);
}

test "foreign key local schema controller resumes incomplete durable job" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/fk-job-controller-maintenance/table-db", .{tmp.sub_path});
    defer alloc.free(path);

    var db = try db_mod.DB.open(alloc, path, .{});
    defer db.close();
    try db.applyTableSchemaJson(
        alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"customer_id":{"type":"keyword"}},"additionalProperties":false}}},"foreign_keys":[{"name":"orders_customer_id_fkey","columns":["customer_id"],"references":{"table":"customers","columns":["_id"]},"on_delete":"restrict","validation_state":"enforced"}]}
    ,
        .{},
    );
    try db.batch(.{
        .writes = &.{
            .{ .key = "customer:present", .value = "{\"_type\":\"customers\"}" },
            .{ .key = "order:pending-job", .value = "{\"customer_id\":\"customer:present\"}" },
        },
        .sync_level = .write,
    });

    var source = LocalForeignKeyIntegrityTestSource{ .table_name = "orders", .db = &db };
    var started = (try source.source().foreignKeyIntegrityWorkerPass(
        alloc,
        "orders",
        .validate,
        "job:fk:orders:resume",
        "worker:manual",
        60_000,
        0,
        "orders_customer_id_fkey",
        "",
        "",
        100,
    )).?;
    defer started.deinit(alloc);
    try std.testing.expect(!started.complete);

    var progress = (try source.source().foreignKeyIntegrity(alloc, "orders", .progress, null, "", "", 0)).?;
    defer progress.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), progress.jobs.len);
    try std.testing.expectEqualStrings("job:fk:orders:resume", progress.jobs[0].job_id);
    try std.testing.expect(!progress.jobs[0].completed);

    var summary = try runLocalForeignKeyIntegritySchemaControllerMaintenancePass(
        alloc,
        source.source(),
        &db,
        "orders",
        .{
            .action = .repair,
            .worker_id = "worker:fk-job-controller",
            .max_tables = 4,
            .max_jobs = 4,
            .max_work_units_per_table = 1,
        },
    );
    defer summary.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), summary.tables_scanned);
    try std.testing.expectEqual(@as(usize, 0), summary.tables_with_pending_constraints);
    try std.testing.expectEqual(@as(usize, 0), summary.tables_executed);
    try std.testing.expectEqual(@as(usize, 1), summary.jobs_scanned);
    try std.testing.expectEqual(@as(usize, 1), summary.jobs_executed);
    try std.testing.expectEqual(@as(usize, 1), summary.results.len);
    try std.testing.expect(!summary.results[0].schema_adoption);
    try std.testing.expectEqualStrings("job:fk:orders:resume", summary.results[0].result.job_id.?);
    try std.testing.expect(summary.results[0].result.complete);
    try std.testing.expect(summary.results[0].result.valid);
    try std.testing.expectEqual(@as(u64, 0), summary.results[0].result.report.missing_parent_rows);

    const job = (try db.loadForeignKeyIntegrityJobRecord("job:fk:orders:resume")) orelse return error.TestUnexpectedResult;
    defer db.freeForeignKeyIntegrityJobRecord(job);
    try std.testing.expectEqualStrings("complete", job.status);
    try std.testing.expect(job.completed);
    try std.testing.expect(job.valid.?);
    try std.testing.expectEqualStrings("worker:fk-job-controller", job.worker_id);
    try std.testing.expectEqual(@as(usize, 0), job.violation_sample_count);
}

test "foreign key local schema controller records invalid unvalidated constraint" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/fk-schema-controller-maintenance-invalid/table-db", .{tmp.sub_path});
    defer alloc.free(path);

    var db = try db_mod.DB.open(alloc, path, .{});
    defer db.close();
    try db.applyTableSchemaJson(
        alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"customer_id":{"type":"keyword"}},"additionalProperties":false}}},"foreign_keys":[{"name":"orders_customer_id_fkey","columns":["customer_id"],"references":{"table":"customers","columns":["_id"]},"on_delete":"restrict","validation_state":"unvalidated"}]}
    ,
        .{},
    );
    try db.batch(.{
        .writes = &.{
            .{ .key = "order:1", .value = "{\"customer_id\":\"customer:missing\"}" },
        },
        .sync_level = .write,
    });

    var source = LocalForeignKeyIntegrityTestSource{ .table_name = "orders", .db = &db };
    var summary = try runLocalForeignKeyIntegritySchemaControllerMaintenancePass(
        alloc,
        source.source(),
        &db,
        "orders",
        .{
            .action = .validate,
            .worker_id = "worker:fk-maintenance",
            .max_tables = 4,
            .max_work_units_per_table = 1,
        },
    );
    defer summary.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), summary.tables_scanned);
    try std.testing.expectEqual(@as(usize, 1), summary.tables_with_pending_constraints);
    try std.testing.expectEqual(@as(usize, 1), summary.tables_executed);
    try std.testing.expect(summary.complete);
    try std.testing.expect(!summary.valid);
    try std.testing.expectEqual(@as(usize, 0), summary.terminal_valid_results);
    try std.testing.expectEqual(@as(usize, 1), summary.terminal_invalid_results);
    try std.testing.expectEqual(@as(usize, 1), summary.results.len);
    try std.testing.expect(summary.results[0].result.job_id != null);
    try std.testing.expectEqual(@as(u64, 1), summary.results[0].result.report.missing_parent_rows);

    const schema_json = (try loadLocalTableSchemaJson(alloc, &db)) orelse return error.TestUnexpectedResult;
    defer alloc.free(schema_json);
    try std.testing.expect(std.mem.indexOf(u8, schema_json, "\"validation_state\":\"unvalidated\"") != null);

    const job = (try db.loadForeignKeyIntegrityJobRecord(summary.results[0].result.job_id.?)) orelse return error.TestUnexpectedResult;
    defer db.freeForeignKeyIntegrityJobRecord(job);
    try std.testing.expectEqualStrings("orders", job.table_name);
    try std.testing.expectEqualStrings("validate", job.action);
    try std.testing.expectEqualStrings("worker:fk-maintenance", job.worker_id);
    try std.testing.expectEqualStrings("orders_customer_id_fkey", job.constraint_name.?);
    try std.testing.expectEqualStrings("invalid", job.status);
    try std.testing.expect(job.completed);
    try std.testing.expect(!job.valid.?);
    try std.testing.expectEqual(@as(usize, 2), job.violation_sample_count);
    try std.testing.expect(!job.violations_truncated);
    try std.testing.expect(std.mem.indexOf(u8, job.violation_samples_json, "\"child_key\":\"order:1\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, job.violation_samples_json, "\"kind\":\"missing_parent\"") != null);

    var progress = (try source.source().foreignKeyIntegrity(alloc, "orders", .progress, null, "", "", 10)) orelse return error.TestUnexpectedResult;
    defer progress.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), progress.jobs.len);
    try std.testing.expectEqualStrings(job.job_id, progress.jobs[0].job_id);
    try std.testing.expectEqual(@as(u64, 1), progress.jobs[0].aggregate_report.missing_parent_rows);
    try std.testing.expectEqual(@as(u64, 1), progress.jobs[0].aggregate_report.missing_ref_rows);
    try std.testing.expectEqual(@as(usize, 2), progress.jobs[0].violation_sample_count);
    try std.testing.expect(!progress.jobs[0].violations_truncated);
    try std.testing.expectEqual(@as(usize, 2), progress.jobs[0].violation_samples.len);
    try std.testing.expectEqual(db_mod.relational_store.ForeignKeyIntegrityViolationKind.missing_parent, progress.jobs[0].violation_samples[0].kind);
    try std.testing.expectEqualStrings("order:1", progress.jobs[0].violation_samples[0].child_key);
}

test "foreign key local action job controller resumes durable action job" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/fk-action-job-controller-maintenance/table-db", .{tmp.sub_path});
    defer alloc.free(path);

    var db = try db_mod.DB.open(alloc, path, .{});
    defer db.close();
    try db.applyTableSchemaJson(
        alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"customer_id":{"type":"keyword"},"status":{"type":"keyword"}},"additionalProperties":false}}},"foreign_keys":[{"name":"orders_customer_id_fkey","columns":["customer_id"],"references":{"table":"customers","columns":["_id"]},"on_delete":"set_null","validation_state":"enforced"}]}
    ,
        .{},
    );
    try db.batch(.{
        .writes = &.{
            .{ .key = "customer:hot", .value = "{\"_type\":\"customers\"}" },
            .{ .key = "order:1", .value = "{\"customer_id\":\"customer:hot\",\"status\":\"open\"}" },
            .{ .key = "order:2", .value = "{\"customer_id\":\"customer:hot\",\"status\":\"open\"}" },
        },
        .sync_level = .write,
    });

    var source = LocalForeignKeyIntegrityTestSource{ .table_name = "orders", .db = &db };
    var first = (try source.source().foreignKeyActionJobPage(
        alloc,
        "orders",
        "fk-action:set-null:controller-resume",
        "set_null",
        "worker:fk-action-controller",
        "orders_customer_id_fkey",
        "customers",
        "customer:hot",
        null,
        1,
        60_000,
    )) orelse return error.TestUnexpectedResult;
    defer first.deinit(alloc);
    try std.testing.expect(!first.complete);
    try std.testing.expectEqual(@as(u64, 1), first.groups[0].applied_children);

    var summary = try runLocalForeignKeyIntegritySchemaControllerMaintenancePass(
        alloc,
        source.source(),
        &db,
        "orders",
        .{
            .action = .repair,
            .worker_id = "worker:fk-action-controller",
            .max_tables = 4,
            .max_jobs = 4,
            .max_action_jobs = 4,
            .action_job_page_limit = 16,
        },
    );
    defer summary.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), summary.action_schedules_scanned);
    try std.testing.expectEqual(@as(usize, 0), summary.action_schedules_executed);
    try std.testing.expectEqual(@as(usize, 0), summary.action_schedules.len);
    try std.testing.expectEqual(@as(usize, 1), summary.action_jobs_scanned);
    try std.testing.expectEqual(@as(usize, 1), summary.action_jobs_executed);
    try std.testing.expectEqual(@as(usize, 1), summary.action_jobs.len);
    try std.testing.expect(summary.action_jobs[0].completed);
    try std.testing.expectEqual(@as(u64, 2), summary.action_jobs[0].applied_children);
    try std.testing.expectEqualStrings("worker:fk-action-controller", summary.action_jobs[0].worker_id);

    const first_child = (try db.get(alloc, "order:1")) orelse return error.TestUnexpectedResult;
    defer alloc.free(first_child);
    const second_child = (try db.get(alloc, "order:2")) orelse return error.TestUnexpectedResult;
    defer alloc.free(second_child);
    try std.testing.expectEqualStrings("{\"status\":\"open\"}", first_child);
    try std.testing.expectEqualStrings("{\"status\":\"open\"}", second_child);
}

test "foreign key local action job controller reports failed durable action job" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/fk-action-job-controller-maintenance-failed/table-db", .{tmp.sub_path});
    defer alloc.free(path);

    var db = try db_mod.DB.open(alloc, path, .{});
    defer db.close();
    try db.applyTableSchemaJson(
        alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"customer_id":{"type":"keyword"},"status":{"type":"keyword"}},"additionalProperties":false}}},"foreign_keys":[{"name":"orders_customer_id_fkey","columns":["customer_id"],"references":{"table":"customers","columns":["_id"]},"on_delete":"set_null","validation_state":"enforced"}]}
    ,
        .{},
    );
    try db.batch(.{
        .writes = &.{
            .{ .key = "customer:hot", .value = "{\"_type\":\"customers\"}" },
            .{ .key = "order:1", .value = "{\"customer_id\":\"customer:hot\",\"status\":\"open\"}" },
        },
        .sync_level = .write,
    });
    const scheduled = try db.scheduleForeignKeyActionJob(
        "fk-action:cascade:controller-failed",
        "cascade",
        "worker:fk-action-controller",
        "orders_customer_id_fkey",
        "customers",
        "customer:hot",
        16,
    );
    defer db.freeForeignKeyActionJobRecord(scheduled);

    var source = LocalForeignKeyIntegrityTestSource{ .table_name = "orders", .db = &db };
    var summary = try runLocalForeignKeyIntegritySchemaControllerMaintenancePass(
        alloc,
        source.source(),
        &db,
        "orders",
        .{
            .action = .repair,
            .worker_id = "worker:fk-action-controller",
            .max_tables = 4,
            .max_jobs = 4,
            .max_action_jobs = 4,
            .action_job_page_limit = 16,
        },
    );
    defer summary.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), summary.action_jobs_scanned);
    try std.testing.expectEqual(@as(usize, 1), summary.action_jobs_executed);
    try std.testing.expectEqual(@as(usize, 1), summary.action_jobs_invalid);
    try std.testing.expect(!summary.valid);
    try std.testing.expectEqual(@as(usize, 1), summary.action_jobs.len);
    try std.testing.expectEqualStrings("fk-action:cascade:controller-failed", summary.action_jobs[0].job_id);
    try std.testing.expectEqualStrings("invalid", summary.action_jobs[0].status);
    try std.testing.expectEqualStrings("ForeignKeyViolation", summary.action_jobs[0].last_error.?);
    try std.testing.expect(!summary.action_jobs[0].completed);

    var retry_summary = try runLocalForeignKeyIntegritySchemaControllerMaintenancePass(
        alloc,
        source.source(),
        &db,
        "orders",
        .{
            .action = .repair,
            .worker_id = "worker:fk-action-controller",
            .max_tables = 4,
            .max_jobs = 4,
            .max_action_jobs = 4,
            .action_job_page_limit = 16,
        },
    );
    defer retry_summary.deinit(alloc);
    try std.testing.expect(!retry_summary.complete);
    try std.testing.expectEqual(@as(usize, 1), retry_summary.action_jobs_scanned);
    try std.testing.expectEqual(@as(usize, 0), retry_summary.action_jobs_executed);
    try std.testing.expectEqual(@as(usize, 1), retry_summary.action_jobs_invalid);
    try std.testing.expect(!retry_summary.valid);
    try std.testing.expectEqual(@as(usize, 1), retry_summary.action_jobs.len);
    try std.testing.expectEqualStrings("fk-action:cascade:controller-failed", retry_summary.action_jobs[0].job_id);
    try std.testing.expectEqualStrings("invalid", retry_summary.action_jobs[0].status);
    try std.testing.expectEqualStrings("ForeignKeyViolation", retry_summary.action_jobs[0].last_error.?);
    try std.testing.expectEqual(@as(u32, summary.action_jobs[0].attempts), retry_summary.action_jobs[0].attempts);
}

test "foreign key local action job controller reports incomplete when lease is busy" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/fk-action-job-controller-maintenance-busy/table-db", .{tmp.sub_path});
    defer alloc.free(path);

    var db = try db_mod.DB.open(alloc, path, .{});
    defer db.close();
    try db.applyTableSchemaJson(
        alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"customer_id":{"type":"keyword"},"status":{"type":"keyword"}},"additionalProperties":false}}},"foreign_keys":[{"name":"orders_customer_id_fkey","columns":["customer_id"],"references":{"table":"customers","columns":["_id"]},"on_delete":"set_null","validation_state":"enforced"}]}
    ,
        .{},
    );
    try db.batch(.{
        .writes = &.{
            .{ .key = "customer:busy", .value = "{\"_type\":\"customers\"}" },
            .{ .key = "order:busy:1", .value = "{\"customer_id\":\"customer:busy\",\"status\":\"open\"}" },
            .{ .key = "order:busy:2", .value = "{\"customer_id\":\"customer:busy\",\"status\":\"open\"}" },
        },
        .sync_level = .write,
    });

    const leased = try db.claimAndRunForeignKeyActionJobPage(
        "fk-action:set-null:controller-busy",
        "set_null",
        "worker:lease-owner",
        "orders_customer_id_fkey",
        "customers",
        "customer:busy",
        1,
        60_000,
    );
    defer db.freeForeignKeyActionJobRecord(leased);
    try std.testing.expect(!leased.completed);
    try std.testing.expectEqualStrings("pending", leased.status);
    try std.testing.expectEqualStrings("worker:lease-owner", leased.worker_id);
    try std.testing.expect(leased.lease_until_ns > foreignKeyIntegrityNowNs());
    try std.testing.expectEqual(@as(u64, 1), leased.applied_children);

    var source = LocalForeignKeyIntegrityTestSource{ .table_name = "orders", .db = &db };
    var summary = try runLocalForeignKeyIntegritySchemaControllerMaintenancePass(
        alloc,
        source.source(),
        &db,
        "orders",
        .{
            .action = .repair,
            .worker_id = "worker:fk-action-controller",
            .max_tables = 4,
            .max_jobs = 4,
            .max_action_jobs = 4,
            .action_job_page_limit = 16,
        },
    );
    defer summary.deinit(alloc);
    try std.testing.expect(!summary.complete);
    try std.testing.expectEqual(@as(usize, 1), summary.action_jobs_scanned);
    try std.testing.expectEqual(@as(usize, 0), summary.action_jobs_executed);
    try std.testing.expectEqual(@as(usize, 0), summary.action_jobs.len);

    const first_child = (try db.get(alloc, "order:busy:1")) orelse return error.TestUnexpectedResult;
    defer alloc.free(first_child);
    const second_child = (try db.get(alloc, "order:busy:2")) orelse return error.TestUnexpectedResult;
    defer alloc.free(second_child);
    try std.testing.expectEqualStrings("{\"status\":\"open\"}", first_child);
    try std.testing.expectEqualStrings("{\"customer_id\":\"customer:busy\",\"status\":\"open\"}", second_child);
}

test "foreign key local action schedule controller seeds durable action schedule" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/fk-action-schedule-controller-maintenance/table-db", .{tmp.sub_path});
    defer alloc.free(path);

    var db = try db_mod.DB.open(alloc, path, .{});
    defer db.close();
    try db.applyTableSchemaJson(
        alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"customer_id":{"type":"keyword"},"status":{"type":"keyword"}},"additionalProperties":false}}},"foreign_keys":[{"name":"orders_customer_id_fkey","columns":["customer_id"],"references":{"table":"customers","columns":["_id"]},"on_delete":"set_null","validation_state":"enforced"}]}
    ,
        .{},
    );
    try db.batch(.{
        .writes = &.{
            .{ .key = "customer:queued", .value = "{\"_type\":\"customers\"}" },
            .{ .key = "order:queued:1", .value = "{\"customer_id\":\"customer:queued\",\"status\":\"open\"}" },
            .{ .key = "order:queued:2", .value = "{\"customer_id\":\"customer:queued\",\"status\":\"open\"}" },
        },
        .sync_level = .write,
    });
    const scheduled = try db.scheduleForeignKeyActionSchedule(
        "fk-action-schedule:set-null:customer-queued",
        "fk-action:set-null:customer-queued",
        "set_null",
        "worker:initial-scheduler",
        "orders_customer_id_fkey",
        "customers",
        "customer:queued",
        16,
    );
    defer db.freeForeignKeyActionScheduleRecord(scheduled);
    try std.testing.expectEqualStrings("pending", scheduled.status);

    var source = LocalForeignKeyIntegrityTestSource{ .table_name = "orders", .db = &db };
    var summary = try runLocalForeignKeyIntegritySchemaControllerMaintenancePass(
        alloc,
        source.source(),
        &db,
        "orders",
        .{
            .action = .repair,
            .worker_id = "worker:fk-action-controller",
            .max_tables = 4,
            .max_jobs = 4,
            .max_action_jobs = 4,
            .action_job_page_limit = 16,
        },
    );
    defer summary.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), summary.action_schedules_scanned);
    try std.testing.expectEqual(@as(usize, 1), summary.action_schedules_executed);
    try std.testing.expectEqual(@as(usize, 1), summary.action_schedules.len);
    try std.testing.expectEqualStrings("fk-action-schedule:set-null:customer-queued", summary.action_schedules[0].schedule_id);
    try std.testing.expectEqualStrings("seeded", summary.action_schedules[0].status);
    try std.testing.expect(summary.action_schedules[0].completed);
    try std.testing.expectEqual(@as(u64, 1), summary.action_schedules[0].scheduled_groups);
    try std.testing.expectEqual(@as(usize, 1), summary.action_jobs_scanned);
    try std.testing.expectEqual(@as(usize, 1), summary.action_jobs_executed);
    try std.testing.expectEqual(@as(usize, 1), summary.action_jobs.len);
    try std.testing.expect(summary.action_jobs[0].completed);
    try std.testing.expectEqualStrings("fk-action:set-null:customer-queued", summary.action_jobs[0].job_id);

    const seeded = (try db.loadForeignKeyActionScheduleRecord("fk-action-schedule:set-null:customer-queued")) orelse return error.TestUnexpectedResult;
    defer db.freeForeignKeyActionScheduleRecord(seeded);
    try std.testing.expectEqualStrings("seeded", seeded.status);
    try std.testing.expect(seeded.completed);
    try std.testing.expectEqual(@as(u64, 1), seeded.scheduled_groups);

    const first_child = (try db.get(alloc, "order:queued:1")) orelse return error.TestUnexpectedResult;
    defer alloc.free(first_child);
    const second_child = (try db.get(alloc, "order:queued:2")) orelse return error.TestUnexpectedResult;
    defer alloc.free(second_child);
    try std.testing.expectEqualStrings("{\"status\":\"open\"}", first_child);
    try std.testing.expectEqualStrings("{\"status\":\"open\"}", second_child);
}

test "foreign key local action schedule controller reports incomplete when budget remains" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/fk-action-schedule-controller-budget/table-db", .{tmp.sub_path});
    defer alloc.free(path);

    var db = try db_mod.DB.open(alloc, path, .{});
    defer db.close();
    try db.applyTableSchemaJson(
        alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"customer_id":{"type":"keyword"},"status":{"type":"keyword"}},"additionalProperties":false}}},"foreign_keys":[{"name":"orders_customer_id_fkey","columns":["customer_id"],"references":{"table":"customers","columns":["_id"]},"on_delete":"set_null","validation_state":"enforced"}]}
    ,
        .{},
    );
    try db.batch(.{
        .writes = &.{
            .{ .key = "customer:budget:a", .value = "{\"_type\":\"customers\"}" },
            .{ .key = "customer:budget:b", .value = "{\"_type\":\"customers\"}" },
            .{ .key = "order:budget:a", .value = "{\"customer_id\":\"customer:budget:a\",\"status\":\"open\"}" },
            .{ .key = "order:budget:b", .value = "{\"customer_id\":\"customer:budget:b\",\"status\":\"open\"}" },
        },
        .sync_level = .write,
    });
    const scheduled_a = try db.scheduleForeignKeyActionSchedule(
        "fk-action-schedule:set-null:budget:a",
        "fk-action:set-null:budget:a",
        "set_null",
        "worker:initial-scheduler",
        "orders_customer_id_fkey",
        "customers",
        "customer:budget:a",
        16,
    );
    defer db.freeForeignKeyActionScheduleRecord(scheduled_a);
    const scheduled_b = try db.scheduleForeignKeyActionSchedule(
        "fk-action-schedule:set-null:budget:b",
        "fk-action:set-null:budget:b",
        "set_null",
        "worker:initial-scheduler",
        "orders_customer_id_fkey",
        "customers",
        "customer:budget:b",
        16,
    );
    defer db.freeForeignKeyActionScheduleRecord(scheduled_b);

    var source = LocalForeignKeyIntegrityTestSource{ .table_name = "orders", .db = &db };
    var summary = try runLocalForeignKeyIntegritySchemaControllerMaintenancePass(
        alloc,
        source.source(),
        &db,
        "orders",
        .{
            .action = .repair,
            .worker_id = "worker:fk-action-controller",
            .max_tables = 4,
            .max_jobs = 4,
            .max_action_jobs = 1,
            .action_job_page_limit = 16,
        },
    );
    defer summary.deinit(alloc);
    try std.testing.expect(!summary.complete);
    try std.testing.expectEqual(@as(usize, 2), summary.action_schedules_scanned);
    try std.testing.expectEqual(@as(usize, 1), summary.action_schedules_executed);
    try std.testing.expectEqual(@as(usize, 1), summary.action_schedules.len);

    var seeded: usize = 0;
    const schedule_a = (try db.loadForeignKeyActionScheduleRecord("fk-action-schedule:set-null:budget:a")) orelse return error.TestUnexpectedResult;
    defer db.freeForeignKeyActionScheduleRecord(schedule_a);
    const schedule_b = (try db.loadForeignKeyActionScheduleRecord("fk-action-schedule:set-null:budget:b")) orelse return error.TestUnexpectedResult;
    defer db.freeForeignKeyActionScheduleRecord(schedule_b);
    if (schedule_a.completed) seeded += 1;
    if (schedule_b.completed) seeded += 1;
    try std.testing.expectEqual(@as(usize, 1), seeded);
}

test "unique schema controller maintenance repairs and promotes unvalidated local constraint" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/unique-schema-controller-maintenance/table-db", .{tmp.sub_path});
    defer alloc.free(path);

    var db = try db_mod.DB.open(alloc, path, .{});
    defer db.close();
    try db.applyTableSchemaJson(
        alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"unique_constraints":[{"name":"users_email_key","columns":["email"],"validation_state":"unvalidated"}]}
    ,
        .{},
    );
    try db.batch(.{
        .writes = &.{
            .{ .key = "user:ada", .value = "{\"id\":\"user:ada\",\"email\":\"ada@example.test\"}" },
            .{ .key = "user:grace", .value = "{\"id\":\"user:grace\",\"email\":\"grace@example.test\"}" },
        },
        .sync_level = .write,
    });

    const before = try db.validateUniqueConstraintRowsInRange("", "");
    try std.testing.expectEqual(@as(u64, 2), before.missing_unique_rows);

    var source = LocalUniqueConstraintIntegrityTestSource{ .table_name = "users", .db = &db };
    var summary = try runLocalUniqueConstraintIntegritySchemaControllerMaintenancePass(
        alloc,
        source.source(),
        &db,
        "users",
        .{
            .action = .repair,
            .worker_id = "worker:unique-maintenance",
            .max_tables = 4,
        },
    );
    defer summary.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), summary.tables_scanned);
    try std.testing.expectEqual(@as(usize, 1), summary.tables_with_pending_constraints);
    try std.testing.expectEqual(@as(usize, 1), summary.tables_executed);
    try std.testing.expect(summary.complete);
    try std.testing.expect(summary.valid);
    try std.testing.expectEqual(@as(usize, 1), summary.terminal_valid_results);
    try std.testing.expectEqual(@as(usize, 0), summary.terminal_invalid_results);
    try std.testing.expectEqual(@as(usize, 1), summary.results.len);
    try std.testing.expectEqualStrings("users", summary.results[0].table_name);
    try std.testing.expectEqualStrings("users_email_key", summary.results[0].constraint_name);
    try std.testing.expectEqual(@as(u64, 2), summary.results[0].result.report.repaired_unique_rows);

    const after = try db.validateUniqueConstraintRowsInRange("", "");
    try std.testing.expect(after.valid());

    const promoted_schema_json = (try loadLocalTableSchemaJson(alloc, &db)) orelse return error.TestUnexpectedResult;
    defer alloc.free(promoted_schema_json);
    try std.testing.expect(std.mem.indexOf(u8, promoted_schema_json, "\"validation_state\":\"enforced\"") != null);

    try std.testing.expectError(error.UniqueConstraintViolation, db.batch(.{
        .writes = &.{.{ .key = "user:duplicate", .value = "{\"id\":\"user:duplicate\",\"email\":\"ada@example.test\"}" }},
        .sync_level = .write,
    }));

    var second_summary = try runLocalUniqueConstraintIntegritySchemaControllerMaintenancePass(
        alloc,
        source.source(),
        &db,
        "users",
        .{
            .action = .repair,
            .worker_id = "worker:unique-maintenance",
            .max_tables = 4,
        },
    );
    defer second_summary.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), second_summary.tables_scanned);
    try std.testing.expectEqual(@as(usize, 0), second_summary.tables_with_pending_constraints);
    try std.testing.expectEqual(@as(usize, 0), second_summary.tables_executed);
    try std.testing.expect(second_summary.complete);
    try std.testing.expect(second_summary.valid);
    try std.testing.expectEqual(@as(usize, 0), second_summary.results.len);
}

test "unique schema controller maintenance keeps invalid unvalidated local constraint pending" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/unique-schema-controller-maintenance-invalid/table-db", .{tmp.sub_path});
    defer alloc.free(path);

    var db = try db_mod.DB.open(alloc, path, .{});
    defer db.close();
    try db.applyTableSchemaJson(
        alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"unique_constraints":[{"name":"users_email_key","columns":["email"],"validation_state":"unvalidated"}]}
    ,
        .{},
    );
    try db.batch(.{
        .writes = &.{
            .{ .key = "user:ada", .value = "{\"id\":\"user:ada\",\"email\":\"same@example.test\"}" },
            .{ .key = "user:grace", .value = "{\"id\":\"user:grace\",\"email\":\"same@example.test\"}" },
        },
        .sync_level = .write,
    });

    var source = LocalUniqueConstraintIntegrityTestSource{ .table_name = "users", .db = &db };
    var summary = try runLocalUniqueConstraintIntegritySchemaControllerMaintenancePass(
        alloc,
        source.source(),
        &db,
        "users",
        .{
            .action = .repair,
            .worker_id = "worker:unique-maintenance",
            .max_tables = 4,
        },
    );
    defer summary.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), summary.tables_scanned);
    try std.testing.expectEqual(@as(usize, 1), summary.tables_with_pending_constraints);
    try std.testing.expectEqual(@as(usize, 1), summary.tables_executed);
    try std.testing.expect(summary.complete);
    try std.testing.expect(!summary.valid);
    try std.testing.expectEqual(@as(usize, 0), summary.terminal_valid_results);
    try std.testing.expectEqual(@as(usize, 1), summary.terminal_invalid_results);
    try std.testing.expectEqual(@as(usize, 1), summary.results.len);
    try std.testing.expectEqualStrings("users_email_key", summary.results[0].constraint_name);
    try std.testing.expectEqual(@as(u64, 1), summary.results[0].result.report.duplicate_unique_rows);

    const schema_json = (try loadLocalTableSchemaJson(alloc, &db)) orelse return error.TestUnexpectedResult;
    defer alloc.free(schema_json);
    try std.testing.expect(std.mem.indexOf(u8, schema_json, "\"validation_state\":\"unvalidated\"") != null);
}

test "foreign key action page transaction id is stable for durable page retry" {
    const base = db_mod.DB.ForeignKeyActionJobRecord{
        .job_id = "fk-action:set-null:customer:stable",
        .action = "set_null",
        .worker_id = "worker:first",
        .constraint_name = "orders_customer_id_fkey",
        .parent_table = "customers",
        .parent_key = "customer:stable",
        .updated_parent_key = null,
        .page_limit = 128,
        .status = "claimed",
        .created_at_ns = 10,
        .updated_at_ns = 20,
        .claimed_at_ns = 20,
        .lease_until_ns = 30,
        .attempts = 1,
        .completed = false,
        .applied_children = 7,
        .requeue_count = 0,
        .cascade_depth = 0,
        .cascade_max_depth = 64,
        .next_child_table = "row",
        .next_child_key = "order:cursor",
    };
    const first = stableForeignKeyActionPageTxnId(base);

    var lease_handoff = base;
    lease_handoff.worker_id = "worker:second";
    lease_handoff.claimed_at_ns = 40;
    lease_handoff.lease_until_ns = 50;
    lease_handoff.attempts = 2;
    const after_handoff = stableForeignKeyActionPageTxnId(lease_handoff);
    try std.testing.expectEqualSlices(u8, first[0..], after_handoff[0..]);

    var next_page = base;
    next_page.next_child_key = "order:next";
    const next_page_id = stableForeignKeyActionPageTxnId(next_page);
    try std.testing.expect(!std.mem.eql(u8, first[0..], next_page_id[0..]));

    var requeued = base;
    requeued.requeue_count = 1;
    const requeued_id = stableForeignKeyActionPageTxnId(requeued);
    try std.testing.expect(!std.mem.eql(u8, first[0..], requeued_id[0..]));
}

fn testViolation(alloc: std.mem.Allocator, child_key: []const u8) !ForeignKeyIntegrityViolation {
    var violation = ForeignKeyIntegrityViolation{
        .group_id = 7,
        .kind = .missing_parent,
        .constraint_name = try alloc.dupe(u8, "orders_customer_id_fkey"),
        .child_table = &.{},
        .child_key = &.{},
        .parent_table = &.{},
        .parent_key = &.{},
    };
    errdefer violation.deinit(alloc);
    violation.child_table = try alloc.dupe(u8, "orders");
    violation.child_key = try alloc.dupe(u8, child_key);
    violation.parent_table = try alloc.dupe(u8, "customers");
    violation.parent_key = try alloc.dupe(u8, "customer:missing");
    return violation;
}
