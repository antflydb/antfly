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
const db_mod = @import("../../storage/db/mod.zig");

pub const ForeignKeyIntegrityAction = enum {
    plan,
    validate,
    dry_run,
    repair,
    list,
    explain_delete,
    progress,
};

pub const UniqueConstraintIntegrityAction = enum {
    validate,
    dry_run,
    repair,
    progress,
};

pub const ForeignKeyIntegrityRequest = struct {
    action: ForeignKeyIntegrityAction = .validate,
    phase: ?[]const u8 = null,
    constraint_name: ?[]const u8 = null,
    doc_key: ?[]const u8 = null,
    lower_doc_key: []const u8 = "",
    upper_doc_key: []const u8 = "",
    violation_limit: usize = 100,
    claim_key: ?[]const u8 = null,
    job_id: ?[]const u8 = null,
    worker_id: ?[]const u8 = null,
    lease_ms: ?u64 = null,
    max_work_units: ?usize = null,
    controller: ?[]const u8 = null,
};

pub const ForeignKeyIntegritySchemaControllerOptions = struct {
    action: ForeignKeyIntegrityAction = .repair,
    worker_id: []const u8 = "fk-schema-controller",
    lease_ms: u64 = 60_000,
    max_tables: usize = 16,
    max_jobs: usize = 16,
    max_action_jobs: usize = 16,
    max_work_units_per_table: usize = 1,
    action_job_page_limit: usize = 1024,
    violation_limit: usize = 100,
};

pub const ForeignKeyIntegritySchemaControllerTableResult = struct {
    table_name: []u8,
    schema_adoption: bool = false,
    result: ForeignKeyIntegrityResult,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.table_name);
        self.result.deinit(alloc);
        self.* = undefined;
    }
};

pub const ForeignKeyIntegritySchemaControllerResult = struct {
    tables_scanned: usize = 0,
    tables_with_pending_constraints: usize = 0,
    tables_executed: usize = 0,
    jobs_scanned: usize = 0,
    jobs_executed: usize = 0,
    action_schedules_scanned: usize = 0,
    action_schedules_executed: usize = 0,
    action_schedules_invalid: usize = 0,
    action_jobs_scanned: usize = 0,
    action_jobs_executed: usize = 0,
    action_jobs_invalid: usize = 0,
    claim_attempts: usize = 0,
    terminal_valid_results: usize = 0,
    terminal_invalid_results: usize = 0,
    complete: bool = true,
    valid: bool = true,
    results: []ForeignKeyIntegritySchemaControllerTableResult = &.{},
    action_schedules: []ForeignKeyActionScheduleStatus = &.{},
    action_jobs: []ForeignKeyActionJobStatus = &.{},

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        for (self.results) |*result| result.deinit(alloc);
        if (self.results.len > 0) alloc.free(self.results);
        for (self.action_schedules) |*schedule| schedule.deinit(alloc);
        if (self.action_schedules.len > 0) alloc.free(self.action_schedules);
        for (self.action_jobs) |*job| job.deinit(alloc);
        if (self.action_jobs.len > 0) alloc.free(self.action_jobs);
        self.* = undefined;
    }
};

pub const UniqueConstraintIntegrityRequest = struct {
    action: UniqueConstraintIntegrityAction = .validate,
    lower_doc_key: []const u8 = "",
    upper_doc_key: []const u8 = "",
};

pub const UniqueConstraintIntegritySchemaControllerOptions = struct {
    action: UniqueConstraintIntegrityAction = .repair,
    worker_id: []const u8 = "unique-schema-controller",
    max_tables: usize = 16,
};

pub const UniqueConstraintIntegrityGroupReport = struct {
    group_id: u64,
    report: db_mod.relational_store.UniqueConstraintIntegrityReport,
};

pub const UniqueConstraintIntegrityResult = struct {
    action: UniqueConstraintIntegrityAction,
    valid: bool,
    complete: bool,
    report: db_mod.relational_store.UniqueConstraintIntegrityReport,
    groups: []UniqueConstraintIntegrityGroupReport,
    owner_topology: ?UniqueConstraintOwnerTopology = null,
    progress: []UniqueConstraintIntegrityProgress = &.{},

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.groups);
        if (self.owner_topology) |*owner_topology| owner_topology.deinit(alloc);
        for (self.progress) |*progress| progress.deinit(alloc);
        if (self.progress.len > 0) alloc.free(self.progress);
        self.* = undefined;
    }
};

pub const UniqueConstraintIntegritySchemaControllerTableResult = struct {
    table_name: []u8,
    constraint_name: []u8,
    schema_adoption: bool = false,
    result: UniqueConstraintIntegrityResult,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.table_name);
        alloc.free(self.constraint_name);
        self.result.deinit(alloc);
        self.* = undefined;
    }
};

pub const UniqueConstraintIntegritySchemaControllerResult = struct {
    tables_scanned: usize = 0,
    tables_with_pending_constraints: usize = 0,
    tables_executed: usize = 0,
    terminal_valid_results: usize = 0,
    terminal_invalid_results: usize = 0,
    complete: bool = true,
    valid: bool = true,
    results: []UniqueConstraintIntegritySchemaControllerTableResult = &.{},

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        for (self.results) |*result| result.deinit(alloc);
        if (self.results.len > 0) alloc.free(self.results);
        self.* = undefined;
    }
};

pub const UniqueConstraintOwnerTopology = struct {
    configured: bool = false,
    complete: bool = true,
    declared_constraints: usize = 0,
    configured_constraints: usize = 0,
    active_ranges: usize = 0,
    transitional_ranges: usize = 0,
    topology_epoch: u64 = 0,
    ranges: []UniqueConstraintOwnerRange = &.{},

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        for (self.ranges) |*range| range.deinit(alloc);
        if (self.ranges.len > 0) alloc.free(self.ranges);
        self.* = undefined;
    }
};

pub const UniqueConstraintOwnerRange = struct {
    constraint_name: []u8,
    start_encoded_value: []u8,
    end_encoded_value: ?[]u8 = null,
    group_id: u64,
    range_id: u64 = 0,
    topology_epoch: u64 = 0,
    state: []u8,
    active: bool,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.constraint_name.len > 0) alloc.free(self.constraint_name);
        if (self.start_encoded_value.len > 0) alloc.free(self.start_encoded_value);
        if (self.end_encoded_value) |value| alloc.free(value);
        if (self.state.len > 0) alloc.free(self.state);
        self.* = undefined;
    }
};

pub const UniqueConstraintIntegrityProgress = struct {
    group_id: u64,
    version: u32 = 1,
    mode: []u8,
    lower_doc_key: []u8,
    upper_doc_key: []u8,
    completed: bool = true,
    valid: bool,
    updated_at_ns: u64,
    report: db_mod.relational_store.UniqueConstraintIntegrityReport,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.mode.len > 0) alloc.free(self.mode);
        if (self.lower_doc_key.len > 0) alloc.free(self.lower_doc_key);
        if (self.upper_doc_key.len > 0) alloc.free(self.upper_doc_key);
        self.* = undefined;
    }
};

pub const ForeignKeyIntegrityGroupReport = struct {
    group_id: u64,
    report: db_mod.relational_store.ForeignKeyIntegrityReport,
};

pub const ForeignKeyIntegrityTupleValue = struct {
    column: []u8,
    value: []u8,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.column);
        alloc.free(self.value);
        self.* = undefined;
    }
};

pub const ForeignKeyIntegrityViolation = struct {
    group_id: u64,
    kind: db_mod.relational_store.ForeignKeyIntegrityViolationKind,
    constraint_name: []u8,
    child_table: []u8,
    child_key: []u8,
    parent_table: []u8,
    parent_key: []u8,
    parent_values: []ForeignKeyIntegrityTupleValue = &.{},
    observed_parent_key: ?[]u8 = null,
    observed_parent_values: []ForeignKeyIntegrityTupleValue = &.{},

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.constraint_name);
        alloc.free(self.child_table);
        alloc.free(self.child_key);
        alloc.free(self.parent_table);
        alloc.free(self.parent_key);
        freeForeignKeyIntegrityTupleValues(alloc, self.parent_values);
        if (self.observed_parent_key) |value| alloc.free(value);
        freeForeignKeyIntegrityTupleValues(alloc, self.observed_parent_values);
        self.* = undefined;
    }
};

fn freeForeignKeyIntegrityTupleValues(alloc: std.mem.Allocator, values: []ForeignKeyIntegrityTupleValue) void {
    for (values) |*value| value.deinit(alloc);
    if (values.len > 0) alloc.free(values);
}

pub const ForeignKeyIntegrityJobStatus = struct {
    group_id: u64,
    version: u32 = 1,
    job_id: []u8,
    table_name: []u8,
    action: []u8,
    worker_id: []u8,
    constraint_name: ?[]u8 = null,
    lower_doc_key: []u8,
    upper_doc_key: []u8,
    lease_ms: u64,
    max_work_units: usize,
    status: []u8,
    created_at_ns: u64,
    updated_at_ns: u64,
    attempts: u32 = 0,
    completed: bool = false,
    valid: ?bool = null,
    last_report: db_mod.relational_store.ForeignKeyIntegrityReport = .{},
    aggregate_report: db_mod.relational_store.ForeignKeyIntegrityReport = .{},
    violation_sample_count: usize = 0,
    violations_truncated: bool = false,
    diagnostic_passes: u64 = 0,
    violating_passes: u64 = 0,
    first_violation_at_ns: ?u64 = null,
    last_violation_at_ns: ?u64 = null,
    violation_samples: []ForeignKeyIntegrityViolation = &.{},

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.job_id.len > 0) alloc.free(self.job_id);
        if (self.table_name.len > 0) alloc.free(self.table_name);
        if (self.action.len > 0) alloc.free(self.action);
        if (self.worker_id.len > 0) alloc.free(self.worker_id);
        if (self.constraint_name) |value| alloc.free(value);
        if (self.lower_doc_key.len > 0) alloc.free(self.lower_doc_key);
        if (self.upper_doc_key.len > 0) alloc.free(self.upper_doc_key);
        if (self.status.len > 0) alloc.free(self.status);
        for (self.violation_samples) |*violation| violation.deinit(alloc);
        if (self.violation_samples.len > 0) alloc.free(self.violation_samples);
        self.* = undefined;
    }
};

pub const ForeignKeyIntegrityResult = struct {
    job_id: ?[]u8 = null,
    action: ForeignKeyIntegrityAction,
    valid: bool,
    complete: bool,
    violation_limit: usize,
    violations_truncated: bool,
    report: db_mod.relational_store.ForeignKeyIntegrityReport,
    delete_plan: ?db_mod.relational_store.ForeignKeyDeletePlan = null,
    groups: []ForeignKeyIntegrityGroupReport,
    progress: []ForeignKeyIntegrityProgress = &.{},
    work_units: []ForeignKeyIntegrityWorkUnit = &.{},
    work_claims: []ForeignKeyIntegrityWorkClaim = &.{},
    work_statuses: []ForeignKeyIntegrityWorkStatus = &.{},
    jobs: []ForeignKeyIntegrityJobStatus = &.{},
    violations: []ForeignKeyIntegrityViolation,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.job_id) |value| alloc.free(value);
        alloc.free(self.groups);
        for (self.progress) |*progress| progress.deinit(alloc);
        if (self.progress.len > 0) alloc.free(self.progress);
        for (self.work_units) |*unit| unit.deinit(alloc);
        if (self.work_units.len > 0) alloc.free(self.work_units);
        for (self.work_claims) |*claim| claim.deinit(alloc);
        if (self.work_claims.len > 0) alloc.free(self.work_claims);
        for (self.work_statuses) |*status| status.deinit(alloc);
        if (self.work_statuses.len > 0) alloc.free(self.work_statuses);
        for (self.jobs) |*job| job.deinit(alloc);
        if (self.jobs.len > 0) alloc.free(self.jobs);
        for (self.violations) |*violation| violation.deinit(alloc);
        alloc.free(self.violations);
        self.* = undefined;
    }
};

pub const ForeignKeyIntegrityWorkUnit = struct {
    group_id: u64,
    phase: []u8,
    planned_action: []u8,
    constraint_name: ?[]u8 = null,
    lower_doc_key: []u8,
    upper_doc_key: []u8,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.phase.len > 0) alloc.free(self.phase);
        if (self.planned_action.len > 0) alloc.free(self.planned_action);
        if (self.constraint_name) |value| alloc.free(value);
        if (self.lower_doc_key.len > 0) alloc.free(self.lower_doc_key);
        if (self.upper_doc_key.len > 0) alloc.free(self.upper_doc_key);
        self.* = undefined;
    }
};

pub const ForeignKeyIntegrityWorkState = enum {
    planned,
    pending,
    claimed,
    incomplete,
    complete,
    invalid,
};

pub const ForeignKeyIntegrityWorkStatus = struct {
    group_id: u64,
    phase: []u8,
    planned_action: []u8,
    constraint_name: ?[]u8 = null,
    lower_doc_key: []u8,
    upper_doc_key: []u8,
    claim_key: []u8,
    state: ForeignKeyIntegrityWorkState,
    claim_worker_id: ?[]u8 = null,
    claim_lease_until_ns: ?u64 = null,
    progress_updated_at_ns: ?u64 = null,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.phase.len > 0) alloc.free(self.phase);
        if (self.planned_action.len > 0) alloc.free(self.planned_action);
        if (self.constraint_name) |value| alloc.free(value);
        if (self.lower_doc_key.len > 0) alloc.free(self.lower_doc_key);
        if (self.upper_doc_key.len > 0) alloc.free(self.upper_doc_key);
        if (self.claim_key.len > 0) alloc.free(self.claim_key);
        if (self.claim_worker_id) |value| alloc.free(value);
        self.* = undefined;
    }
};

pub const ForeignKeyIntegrityWorkClaim = struct {
    group_id: u64,
    claim_key: []u8,
    worker_id: []u8,
    phase: []u8,
    planned_action: []u8,
    constraint_name: ?[]u8 = null,
    lower_doc_key: []u8,
    upper_doc_key: []u8,
    claimed_at_ns: u64,
    lease_until_ns: u64,
    attempts: u32,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.claim_key.len > 0) alloc.free(self.claim_key);
        if (self.worker_id.len > 0) alloc.free(self.worker_id);
        if (self.phase.len > 0) alloc.free(self.phase);
        if (self.planned_action.len > 0) alloc.free(self.planned_action);
        if (self.constraint_name) |value| alloc.free(value);
        if (self.lower_doc_key.len > 0) alloc.free(self.lower_doc_key);
        if (self.upper_doc_key.len > 0) alloc.free(self.upper_doc_key);
        self.* = undefined;
    }
};

pub const ForeignKeyIntegrityProgress = struct {
    group_id: u64,
    version: u32 = 1,
    phase: []u8 = &.{},
    mode: []u8,
    constraint_name: ?[]u8 = null,
    lower_doc_key: []u8,
    upper_doc_key: []u8,
    completed: bool = true,
    valid: bool,
    updated_at_ns: u64,
    report: db_mod.relational_store.ForeignKeyIntegrityReport,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.phase.len > 0) alloc.free(self.phase);
        if (self.mode.len > 0) alloc.free(self.mode);
        if (self.constraint_name) |value| alloc.free(value);
        if (self.lower_doc_key.len > 0) alloc.free(self.lower_doc_key);
        if (self.upper_doc_key.len > 0) alloc.free(self.upper_doc_key);
        self.* = undefined;
    }
};

pub const ForeignKeyActionJobStatus = struct {
    group_id: u64,
    version: u32 = 1,
    job_id: []u8,
    action: []u8,
    worker_id: []u8,
    constraint_name: []u8,
    parent_table: []u8,
    parent_key: []u8,
    updated_parent_key: ?[]u8 = null,
    page_limit: usize,
    status: []u8,
    created_at_ns: u64,
    updated_at_ns: u64,
    claimed_at_ns: u64 = 0,
    lease_until_ns: u64 = 0,
    attempts: u32 = 0,
    completed: bool = false,
    applied_children: u64 = 0,
    failure_count: u64 = 0,
    first_failed_at_ns: ?u64 = null,
    last_failed_at_ns: ?u64 = null,
    requeue_count: u64 = 0,
    last_requeued_at_ns: ?u64 = null,
    cascade_depth: u32 = 0,
    cascade_max_depth: u32 = 64,
    next_child_table: ?[]u8 = null,
    next_child_key: ?[]u8 = null,
    last_error: ?[]u8 = null,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.job_id.len > 0) alloc.free(self.job_id);
        if (self.action.len > 0) alloc.free(self.action);
        if (self.worker_id.len > 0) alloc.free(self.worker_id);
        if (self.constraint_name.len > 0) alloc.free(self.constraint_name);
        if (self.parent_table.len > 0) alloc.free(self.parent_table);
        if (self.parent_key.len > 0) alloc.free(self.parent_key);
        if (self.updated_parent_key) |value| alloc.free(value);
        if (self.status.len > 0) alloc.free(self.status);
        if (self.next_child_table) |value| alloc.free(value);
        if (self.next_child_key) |value| alloc.free(value);
        if (self.last_error) |value| alloc.free(value);
        self.* = undefined;
    }
};

pub const ForeignKeyActionJobResult = struct {
    complete: bool,
    groups: []ForeignKeyActionJobStatus,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        for (self.groups) |*group| group.deinit(alloc);
        if (self.groups.len > 0) alloc.free(self.groups);
        self.* = undefined;
    }
};

pub const ForeignKeyActionJobProgressResult = struct {
    jobs: []ForeignKeyActionJobStatus,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        for (self.jobs) |*job| job.deinit(alloc);
        if (self.jobs.len > 0) alloc.free(self.jobs);
        self.* = undefined;
    }
};

pub const ForeignKeyActionScheduleStatus = struct {
    group_id: u64,
    version: u32 = 1,
    schedule_id: []u8,
    action_job_id: []u8,
    action: []u8,
    worker_id: []u8,
    constraint_name: []u8,
    parent_table: []u8,
    parent_key: []u8,
    updated_parent_key: ?[]u8 = null,
    page_limit: usize,
    status: []u8,
    created_at_ns: u64,
    updated_at_ns: u64,
    completed: bool = false,
    scheduled_groups: u64 = 0,
    cascade_depth: u32 = 0,
    cascade_max_depth: u32 = 64,
    requeue_count: u64 = 0,
    last_requeued_at_ns: ?u64 = null,
    last_error: ?[]u8 = null,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.schedule_id.len > 0) alloc.free(self.schedule_id);
        if (self.action_job_id.len > 0) alloc.free(self.action_job_id);
        if (self.action.len > 0) alloc.free(self.action);
        if (self.worker_id.len > 0) alloc.free(self.worker_id);
        if (self.constraint_name.len > 0) alloc.free(self.constraint_name);
        if (self.parent_table.len > 0) alloc.free(self.parent_table);
        if (self.parent_key.len > 0) alloc.free(self.parent_key);
        if (self.updated_parent_key) |value| alloc.free(value);
        if (self.status.len > 0) alloc.free(self.status);
        if (self.last_error) |value| alloc.free(value);
        self.* = undefined;
    }
};

pub const ForeignKeyActionScheduleProgressResult = struct {
    schedules: []ForeignKeyActionScheduleStatus,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        for (self.schedules) |*schedule| schedule.deinit(alloc);
        if (self.schedules.len > 0) alloc.free(self.schedules);
        self.* = undefined;
    }
};
