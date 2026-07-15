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
const builtin = @import("builtin");

const docstore_mod = @import("../docstore.zig");
const internal_keys = @import("../internal_keys.zig");
const schema_mod = @import("../schema.zig");
const schema_api_mod = @import("../../schema/mod.zig");
const transactions_mod = @import("../transactions.zig");
const mapper = @import("document_mapper.zig");
const db_internal = @import("internal.zig");
const relational_store_mod = @import("relational_store.zig");
const types = @import("types.zig");
const platform_clock = @import("../../platform/clock.zig");
const temporal_typed_dv = @import("../../section/typed_doc_values.zig");

const Allocator = std.mem.Allocator;

const TestHelpers = if (builtin.is_test) @import("test_support.zig") else struct {};

const temporal_bound_neg_infinity_tag: u8 = 0xf0;
const temporal_bound_pos_infinity_tag: u8 = 0xf1;
const foreign_key_integrity_progress_key_prefix = "\x00\x00__metadata__:foreign_key_integrity_progress";
const foreign_key_integrity_claim_key_prefix = "\x00\x00__metadata__:foreign_key_integrity_claim";
const foreign_key_integrity_job_key_prefix = "\x00\x00__metadata__:foreign_key_integrity_job";
const relational_index_repair_job_key_prefix = "\x00\x00__metadata__:relational_index_repair_job";
const relational_index_drop_job_key_prefix = "\x00\x00__metadata__:relational_index_drop_job";
const foreign_key_action_job_key_prefix = "\x00\x00__metadata__:foreign_key_action_job";
const foreign_key_action_schedule_key_prefix = "\x00\x00__metadata__:foreign_key_action_schedule";
const unique_constraint_integrity_progress_key_prefix = "\x00\x00__metadata__:unique_constraint_integrity_progress";
const foreign_key_action_default_cascade_max_depth: u32 = 64;

fn currentTimeNs() u64 {
    return platform_clock.Clock.real().nowRealtimeNs();
}

fn relationalIndexRepairReportAdd(
    a: relational_store_mod.ColumnBackedIndexRepairReport,
    b: relational_store_mod.ColumnBackedIndexRepairReport,
) relational_store_mod.ColumnBackedIndexRepairReport {
    return .{
        .scanned_rows = a.scanned_rows +| b.scanned_rows,
        .indexed_rows = a.indexed_rows +| b.indexed_rows,
        .deleted_orphan_entries = a.deleted_orphan_entries +| b.deleted_orphan_entries,
        .written_entries = a.written_entries +| b.written_entries,
    };
}

pub const findUniqueConstraintMutation = relational_store_mod.findUniqueConstraintMutation;

fn mutationIsTemporal(mutation: types.UniqueConstraintMutation) bool {
    return mutation.temporal_start != null and mutation.temporal_end != null;
}

fn findTemporalUniqueConstraintMutationConflict(
    mutations: []const types.UniqueConstraintMutation,
    needle: types.UniqueConstraintMutation,
) ?types.UniqueConstraintMutation {
    if (!mutationIsTemporal(needle)) return null;
    for (mutations) |mutation| {
        if (!mutationIsTemporal(mutation)) continue;
        if (!std.mem.eql(u8, mutation.constraint_name, needle.constraint_name)) continue;
        if (!std.mem.eql(u8, mutation.encoded_value, needle.encoded_value)) continue;
        const overlaps = relational_store_mod.temporalPeriodSpanBytesOverlap(
            mutation.temporal_start.?,
            mutation.temporal_end.?,
            needle.temporal_start.?,
            needle.temporal_end.?,
        ) catch return mutation;
        if (overlaps) return mutation;
    }
    return null;
}

fn findUniqueConstraintByName(unique_constraints: []const schema_mod.UniqueConstraint, name: []const u8) ?schema_mod.UniqueConstraint {
    for (unique_constraints) |constraint| {
        if (std.mem.eql(u8, constraint.name, name)) return constraint;
    }
    return null;
}

fn expectOrderedUniqueEntryAndNoDedicatedOwner(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    runtime_schema: schema_mod.TableSchema,
    constraint_name: []const u8,
    row_value: []const u8,
    doc_key: []const u8,
) !void {
    const constraint = findUniqueConstraintByName(runtime_schema.unique_constraints, constraint_name) orelse return error.TestUnexpectedResult;
    const index = relational_store_mod.relationalIndexForUniqueConstraint(runtime_schema.relational_indexes, constraint, .ordered_tuple) orelse return error.TestUnexpectedResult;
    const lifecycle = schema_mod.relationalIndexLifecycle(index) orelse return error.TestUnexpectedResult;
    if (lifecycle != .ready or index.keys.len == 0) return error.TestUnexpectedResult;

    const logical_tuple = (try relational_store_mod.uniqueConstraintTupleValueWithColumnsAlloc(alloc, row_value, constraint, runtime_schema.relational_columns)) orelse return error.TestExpectedEqual;
    defer alloc.free(logical_tuple);
    const ordered_tuple = try relational_store_mod.orderedTupleValueForIndexKeysAlloc(alloc, row_value, index.keys, runtime_schema.relational_columns);
    defer alloc.free(ordered_tuple);
    const forward_key = try internal_keys.relationalOrderedTupleIndexKeyAlloc(alloc, index.name, ordered_tuple, doc_key);
    defer alloc.free(forward_key);
    const forward_value = try store.get(alloc, forward_key);
    defer alloc.free(forward_value);

    const dedicated_owner_key = try internal_keys.relationalUniqueKeyAlloc(alloc, constraint.name, logical_tuple);
    defer alloc.free(dedicated_owner_key);
    try std.testing.expectError(error.NotFound, store.get(alloc, dedicated_owner_key));
}

fn expectDedicatedUniqueEntryAndNoOrderedOwner(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    runtime_schema: schema_mod.TableSchema,
    constraint_name: []const u8,
    row_value: []const u8,
    doc_key: []const u8,
) !void {
    const constraint = findUniqueConstraintByName(runtime_schema.unique_constraints, constraint_name) orelse return error.TestUnexpectedResult;
    try std.testing.expect(relational_store_mod.relationalIndexForUniqueConstraint(runtime_schema.relational_indexes, constraint, .ordered_tuple) == null);

    const logical_tuple = (try relational_store_mod.uniqueConstraintTupleValueWithColumnsAlloc(alloc, row_value, constraint, runtime_schema.relational_columns)) orelse return error.TestExpectedEqual;
    defer alloc.free(logical_tuple);

    const dedicated_owner_key = try internal_keys.relationalUniqueKeyAlloc(alloc, constraint.name, logical_tuple);
    defer alloc.free(dedicated_owner_key);
    const owner = try store.get(alloc, dedicated_owner_key);
    defer alloc.free(owner);
    try std.testing.expectEqualStrings(doc_key, owner);
}

fn uniqueOwnerConstraintsAlloc(alloc: Allocator, runtime_schema: schema_mod.TableSchema) ![]schema_mod.UniqueConstraint {
    const extra: usize = if (runtime_schema.primary_key != null) 1 else 0;
    const constraints = try alloc.alloc(schema_mod.UniqueConstraint, runtime_schema.unique_constraints.len + extra);
    var index: usize = 0;
    if (runtime_schema.primary_key) |primary_key| {
        constraints[index] = relational_store_mod.primaryKeyAsUniqueConstraint(primary_key);
        index += 1;
    }
    for (runtime_schema.unique_constraints) |constraint| {
        constraints[index] = constraint;
        index += 1;
    }
    return constraints;
}

pub fn isForeignKeyActionScheduleMetadataKey(key: []const u8) bool {
    return std.mem.startsWith(u8, key, foreign_key_action_schedule_key_prefix);
}

pub const foreign_key_externalized_parent_check_intent_key_prefix = relational_store_mod.foreign_key_externalized_parent_check_intent_key_prefix;
pub const foreign_key_constraint_timing_override_intent_key_prefix = relational_store_mod.foreign_key_constraint_timing_override_intent_key_prefix;
pub const foreignKeyExternalizedParentCheckIntentKeyAlloc = relational_store_mod.foreignKeyExternalizedParentCheckIntentKeyAlloc;
pub const foreignKeyConstraintTimingOverrideIntentKeyAlloc = relational_store_mod.foreignKeyConstraintTimingOverrideIntentKeyAlloc;
pub const encodeForeignKeyExternalizedParentCheckIntentValueAlloc = relational_store_mod.encodeForeignKeyExternalizedParentCheckIntentValueAlloc;
pub const encodeForeignKeyConstraintTimingOverrideIntentValueAlloc = relational_store_mod.encodeForeignKeyConstraintTimingOverrideIntentValueAlloc;
pub const collectTransactionExternalizedForeignKeyParentChecksAlloc = relational_store_mod.collectTransactionExternalizedForeignKeyParentChecksAlloc;
pub const collectTransactionForeignKeyConstraintTimingOverridesAlloc = relational_store_mod.collectTransactionForeignKeyConstraintTimingOverridesAlloc;
pub const freeExternalizedForeignKeyParentChecks = relational_store_mod.freeExternalizedForeignKeyParentChecks;
pub const freeExternalizedForeignKeyParentCheck = relational_store_mod.freeExternalizedForeignKeyParentCheck;
pub const freeRelationalForeignKeyConstraintTimingOverrides = relational_store_mod.freeRelationalForeignKeyConstraintTimingOverrides;
pub const freeRelationalForeignKeyConstraintTimingOverride = relational_store_mod.freeRelationalForeignKeyConstraintTimingOverride;

pub const ForeignKeyIntegrityProgressRecord = struct {
    version: u32 = 1,
    phase: []const u8 = "child_range",
    mode: []const u8,
    constraint_name: ?[]const u8 = null,
    lower_doc_key: []const u8,
    upper_doc_key: []const u8,
    completed: bool = true,
    valid: bool,
    updated_at_ns: u64,
    report: relational_store_mod.ForeignKeyIntegrityReport,
};

pub const ForeignKeyIntegrityClaimRecord = struct {
    version: u32 = 1,
    claim_key: []const u8,
    worker_id: []const u8,
    group_id: u64,
    phase: []const u8 = "child_range",
    planned_action: []const u8,
    constraint_name: ?[]const u8 = null,
    lower_doc_key: []const u8,
    upper_doc_key: []const u8,
    claimed_at_ns: u64,
    lease_until_ns: u64,
    attempts: u32 = 1,
};

pub const ForeignKeyIntegrityJobRecord = struct {
    version: u32 = 1,
    job_id: []const u8,
    table_name: []const u8,
    action: []const u8,
    worker_id: []const u8,
    constraint_name: ?[]const u8 = null,
    lower_doc_key: []const u8,
    upper_doc_key: []const u8,
    lease_ms: u64,
    max_work_units: usize,
    status: []const u8,
    created_at_ns: u64,
    updated_at_ns: u64,
    attempts: u32 = 0,
    completed: bool = false,
    valid: ?bool = null,
    last_report: relational_store_mod.ForeignKeyIntegrityReport = .{},
    aggregate_report: relational_store_mod.ForeignKeyIntegrityReport = .{},
    violation_samples_json: []const u8 = "[]",
    violation_sample_count: usize = 0,
    violations_truncated: bool = false,
    diagnostic_passes: u64 = 0,
    violating_passes: u64 = 0,
    first_violation_at_ns: ?u64 = null,
    last_violation_at_ns: ?u64 = null,
};

pub const RelationalIndexRepairJobRecord = struct {
    version: u32 = 1,
    job_id: []const u8,
    database_name: []const u8 = "default",
    namespace_name: []const u8 = "public",
    table_name: []const u8,
    access_method: []const u8 = "ordered_tuple",
    index_name: []const u8 = "",
    generation: u64 = 0,
    worker_id: []const u8,
    lower_doc_key: []const u8 = "",
    upper_doc_key: []const u8 = "",
    lease_ms: u64,
    max_work_units: usize,
    status: []const u8,
    created_at_ns: u64,
    updated_at_ns: u64,
    attempts: u32 = 0,
    completed: bool = false,
    complete: ?bool = null,
    next_lower_doc_key: []const u8 = "",
    cursor: []const u8 = "",
    failure_reason: ?[]const u8 = null,
    stale_generation: bool = false,
    pass_count: u64 = 0,
    last_units_queued: u64 = 0,
    last_units_running: u64 = 0,
    last_units_throttled: u64 = 0,
    last_units_completed: u64 = 0,
    total_units_queued: u64 = 0,
    total_units_running: u64 = 0,
    total_units_throttled: u64 = 0,
    total_units_completed: u64 = 0,
    last_ranges_scanned: u64 = 0,
    last_ranges_repaired: u64 = 0,
    last_ranges_missing: u64 = 0,
    total_ranges_scanned: u64 = 0,
    total_ranges_repaired: u64 = 0,
    total_ranges_missing: u64 = 0,
    last_report: relational_store_mod.ColumnBackedIndexRepairReport = .{},
    aggregate_report: relational_store_mod.ColumnBackedIndexRepairReport = .{},
    last_error: ?[]const u8 = null,
};

pub const RelationalIndexDropJobRecord = struct {
    version: u32 = 1,
    job_id: []const u8,
    database_name: []const u8 = "default",
    namespace_name: []const u8 = "public",
    table_name: []const u8,
    access_method: []const u8,
    index_name: []const u8,
    generation: u64,
    worker_id: []const u8,
    cursor: []const u8 = "",
    lease_ms: u64,
    max_work_units: usize,
    status: []const u8,
    created_at_ns: u64,
    updated_at_ns: u64,
    attempts: u32 = 0,
    completed: bool = false,
    failure_reason: ?[]const u8 = null,
    stale_generation: bool = false,
    pass_count: u64 = 0,
    last_units_queued: u64 = 0,
    last_units_running: u64 = 0,
    last_units_throttled: u64 = 0,
    last_units_completed: u64 = 0,
    total_units_queued: u64 = 0,
    total_units_running: u64 = 0,
    total_units_throttled: u64 = 0,
    total_units_completed: u64 = 0,
};

pub const ForeignKeyActionJobRecord = struct {
    version: u32 = 1,
    job_id: []const u8,
    action: []const u8,
    worker_id: []const u8,
    constraint_name: []const u8,
    parent_table: []const u8,
    parent_key: []const u8,
    updated_parent_key: ?[]const u8 = null,
    page_limit: usize,
    status: []const u8,
    created_at_ns: u64,
    updated_at_ns: u64,
    claimed_at_ns: u64,
    lease_until_ns: u64,
    attempts: u32 = 0,
    completed: bool = false,
    applied_children: u64 = 0,
    failure_count: u64 = 0,
    first_failed_at_ns: ?u64 = null,
    last_failed_at_ns: ?u64 = null,
    requeue_count: u64 = 0,
    last_requeued_at_ns: ?u64 = null,
    cascade_depth: u32,
    cascade_max_depth: u32,
    next_child_table: ?[]const u8 = null,
    next_child_key: ?[]const u8 = null,
    last_error: ?[]const u8 = null,
};

pub const ForeignKeyActionScheduleRecord = struct {
    version: u32 = 1,
    schedule_id: []const u8,
    action_job_id: []const u8,
    action: []const u8,
    worker_id: []const u8,
    constraint_name: []const u8,
    parent_table: []const u8,
    parent_key: []const u8,
    updated_parent_key: ?[]const u8 = null,
    page_limit: usize,
    status: []const u8,
    created_at_ns: u64,
    updated_at_ns: u64,
    completed: bool = false,
    scheduled_groups: u64 = 0,
    cascade_depth: u32,
    cascade_max_depth: u32,
    requeue_count: u64 = 0,
    last_requeued_at_ns: ?u64 = null,
    last_error: ?[]const u8 = null,
};

pub const UniqueConstraintIntegrityProgressRecord = struct {
    version: u32 = 1,
    mode: []const u8,
    lower_doc_key: []const u8,
    upper_doc_key: []const u8,
    completed: bool = true,
    valid: bool,
    updated_at_ns: u64,
    report: relational_store_mod.UniqueConstraintIntegrityReport,
};

pub fn Impl(comptime DB: type) type {
    return struct {
        const Self = @This();
        const ForeignKeyIntegrityReport = relational_store_mod.ForeignKeyIntegrityReport;
        const ForeignKeyIntegrityViolation = relational_store_mod.ForeignKeyIntegrityViolation;
        const ForeignKeyDeletePlan = relational_store_mod.ForeignKeyDeletePlan;
        const UniqueConstraintIntegrityReport = relational_store_mod.UniqueConstraintIntegrityReport;
        const RelationalTemporalBound = relational_store_mod.TemporalBound;
        const RelationalTemporalSpan = relational_store_mod.TemporalSpan;

        pub fn recordForeignKeyChildWriteReject(self: *DB) void {
            self.foreign_key_stats.recordChildWriteReject();
        }

        pub fn recordForeignKeyParentDeleteReject(self: *DB) void {
            self.foreign_key_stats.recordParentDeleteReject();
        }

        pub fn recordForeignKeyIntegrityReport(
            self: *DB,
            mode: relational_store_mod.ForeignKeyIntegrityMode,
            report: ForeignKeyIntegrityReport,
        ) void {
            self.foreign_key_stats.recordIntegrityReport(mode, report);
        }

        fn shouldSkipIntegrityProgressWrite(self: *DB) bool {
            return DB.LifecycleCallbacks.open_mode_requires_read_only_backends(self.open_mode);
        }

        pub fn validateForeignKeyRefsInRange(
            self: *DB,
            lower_doc_key: []const u8,
            upper_doc_key: []const u8,
        ) !ForeignKeyIntegrityReport {
            return try validateForeignKeyRefsInRangeForConstraint(self, null, lower_doc_key, upper_doc_key);
        }

        pub fn validateForeignKeyRefsInRangeForConstraint(
            self: *DB,
            constraint_name: ?[]const u8,
            lower_doc_key: []const u8,
            upper_doc_key: []const u8,
        ) !ForeignKeyIntegrityReport {
            self.core.lockApply();
            defer self.core.unlockApply();
            return try self.relationalIntegrityReconcileForeignKeyRefsInRangeLocked(constraint_name, lower_doc_key, upper_doc_key, .validate);
        }

        pub fn repairForeignKeyRefsInRange(
            self: *DB,
            lower_doc_key: []const u8,
            upper_doc_key: []const u8,
        ) !ForeignKeyIntegrityReport {
            return try repairForeignKeyRefsInRangeForConstraint(self, null, lower_doc_key, upper_doc_key);
        }

        pub fn repairForeignKeyRefsInRangeForConstraint(
            self: *DB,
            constraint_name: ?[]const u8,
            lower_doc_key: []const u8,
            upper_doc_key: []const u8,
        ) !ForeignKeyIntegrityReport {
            self.core.lockApply();
            defer self.core.unlockApply();
            return try self.relationalIntegrityReconcileForeignKeyRefsInRangeLocked(constraint_name, lower_doc_key, upper_doc_key, .repair);
        }

        pub fn dryRunRepairForeignKeyRefsInRange(
            self: *DB,
            lower_doc_key: []const u8,
            upper_doc_key: []const u8,
        ) !ForeignKeyIntegrityReport {
            return try dryRunRepairForeignKeyRefsInRangeForConstraint(self, null, lower_doc_key, upper_doc_key);
        }

        pub fn dryRunRepairForeignKeyRefsInRangeForConstraint(
            self: *DB,
            constraint_name: ?[]const u8,
            lower_doc_key: []const u8,
            upper_doc_key: []const u8,
        ) !ForeignKeyIntegrityReport {
            self.core.lockApply();
            defer self.core.unlockApply();
            return try self.relationalIntegrityReconcileForeignKeyRefsInRangeLocked(constraint_name, lower_doc_key, upper_doc_key, .dry_run);
        }

        pub fn validateUniqueConstraintRowsInRange(
            self: *DB,
            lower_doc_key: []const u8,
            upper_doc_key: []const u8,
        ) !UniqueConstraintIntegrityReport {
            self.core.lockApply();
            defer self.core.unlockApply();
            return try self.relationalIntegrityReconcileUniqueConstraintRowsInRangeLocked(lower_doc_key, upper_doc_key, .validate);
        }

        pub fn dryRunRepairUniqueConstraintRowsInRange(
            self: *DB,
            lower_doc_key: []const u8,
            upper_doc_key: []const u8,
        ) !UniqueConstraintIntegrityReport {
            self.core.lockApply();
            defer self.core.unlockApply();
            return try self.relationalIntegrityReconcileUniqueConstraintRowsInRangeLocked(lower_doc_key, upper_doc_key, .dry_run);
        }

        pub fn repairUniqueConstraintRowsInRange(
            self: *DB,
            lower_doc_key: []const u8,
            upper_doc_key: []const u8,
        ) !UniqueConstraintIntegrityReport {
            self.core.lockApply();
            defer self.core.unlockApply();
            return try self.relationalIntegrityReconcileUniqueConstraintRowsInRangeLocked(lower_doc_key, upper_doc_key, .repair);
        }

        pub fn validateForeignKeyRefOwnerForParent(
            self: *DB,
            constraint_name: []const u8,
            parent_table: []const u8,
            parent_key: []const u8,
        ) !ForeignKeyIntegrityReport {
            self.core.lockApply();
            defer self.core.unlockApply();
            return try self.relationalIntegrityReconcileForeignKeyRefOwnerForParentLocked(constraint_name, parent_table, parent_key, .validate);
        }

        pub fn dryRunRepairForeignKeyRefOwnerForParent(
            self: *DB,
            constraint_name: []const u8,
            parent_table: []const u8,
            parent_key: []const u8,
        ) !ForeignKeyIntegrityReport {
            self.core.lockApply();
            defer self.core.unlockApply();
            return try self.relationalIntegrityReconcileForeignKeyRefOwnerForParentLocked(constraint_name, parent_table, parent_key, .dry_run);
        }

        pub fn repairForeignKeyRefOwnerForParent(
            self: *DB,
            constraint_name: []const u8,
            parent_table: []const u8,
            parent_key: []const u8,
        ) !ForeignKeyIntegrityReport {
            self.core.lockApply();
            defer self.core.unlockApply();
            return try self.relationalIntegrityReconcileForeignKeyRefOwnerForParentLocked(constraint_name, parent_table, parent_key, .repair);
        }

        pub fn validateForeignKeyRefOwnerRange(
            self: *DB,
            constraint_name: []const u8,
            parent_table: []const u8,
            start_parent_key: []const u8,
            end_parent_key: []const u8,
        ) !ForeignKeyIntegrityReport {
            self.core.lockApply();
            defer self.core.unlockApply();
            return try self.relationalIntegrityReconcileForeignKeyRefOwnerRangeLocked(constraint_name, parent_table, start_parent_key, end_parent_key, .validate);
        }

        pub fn dryRunRepairForeignKeyRefOwnerRange(
            self: *DB,
            constraint_name: []const u8,
            parent_table: []const u8,
            start_parent_key: []const u8,
            end_parent_key: []const u8,
        ) !ForeignKeyIntegrityReport {
            self.core.lockApply();
            defer self.core.unlockApply();
            return try self.relationalIntegrityReconcileForeignKeyRefOwnerRangeLocked(constraint_name, parent_table, start_parent_key, end_parent_key, .dry_run);
        }

        pub fn repairForeignKeyRefOwnerRange(
            self: *DB,
            constraint_name: []const u8,
            parent_table: []const u8,
            start_parent_key: []const u8,
            end_parent_key: []const u8,
        ) !ForeignKeyIntegrityReport {
            self.core.lockApply();
            defer self.core.unlockApply();
            return try self.relationalIntegrityReconcileForeignKeyRefOwnerRangeLocked(constraint_name, parent_table, start_parent_key, end_parent_key, .repair);
        }

        pub fn explainForeignKeyDelete(self: *DB, doc_key: []const u8) !relational_store_mod.ForeignKeyDeletePlan {
            return try self.explainForeignKeyDeleteForConstraint(null, doc_key);
        }

        pub fn explainForeignKeyDeleteForConstraint(self: *DB, constraint_name: ?[]const u8, doc_key: []const u8) !relational_store_mod.ForeignKeyDeletePlan {
            self.core.lockApply();
            defer self.core.unlockApply();

            const runtime_schema = self.core.schema orelse return .{};
            if (runtime_schema.storage_mode != .relational or runtime_schema.foreign_keys.len == 0) return .{};
            const foreign_keys = try foreignKeysForIntegrityConstraint(self.alloc, runtime_schema.foreign_keys, constraint_name);
            defer if (constraint_name == null and foreign_keys.len > 0) self.alloc.free(foreign_keys);
            if (foreign_keys.len == 0) return .{};
            return try relational_store_mod.explainForeignKeyDeleteWithPrimaryKey(
                self.alloc,
                self.core.store,
                runtime_schema.default_type,
                runtime_schema.relational_columns,
                runtime_schema.periods,
                foreign_keys,
                runtime_schema.primary_key,
                runtime_schema.unique_constraints,
                doc_key,
            );
        }

        pub fn listForeignKeyViolationsInRange(
            self: *DB,
            lower_doc_key: []const u8,
            upper_doc_key: []const u8,
        ) ![]relational_store_mod.ForeignKeyIntegrityViolation {
            return try listForeignKeyViolationsInRangeForConstraint(self, null, lower_doc_key, upper_doc_key);
        }

        pub fn listForeignKeyViolationsInRangeForConstraint(
            self: *DB,
            constraint_name: ?[]const u8,
            lower_doc_key: []const u8,
            upper_doc_key: []const u8,
        ) ![]relational_store_mod.ForeignKeyIntegrityViolation {
            self.core.lockApply();
            defer self.core.unlockApply();

            const runtime_schema = self.core.schema orelse return try self.alloc.alloc(relational_store_mod.ForeignKeyIntegrityViolation, 0);
            if (runtime_schema.storage_mode != .relational or runtime_schema.foreign_keys.len == 0) {
                return try self.alloc.alloc(relational_store_mod.ForeignKeyIntegrityViolation, 0);
            }
            const foreign_keys = try foreignKeysForIntegrityConstraint(self.alloc, runtime_schema.foreign_keys, constraint_name);
            defer if (constraint_name == null and foreign_keys.len > 0) self.alloc.free(foreign_keys);
            return try relational_store_mod.listForeignKeyViolationsInRangeWithPrimaryKey(
                self.alloc,
                self.core.store,
                runtime_schema.default_type,
                runtime_schema.relational_columns,
                runtime_schema.periods,
                foreign_keys,
                runtime_schema.primary_key,
                runtime_schema.unique_constraints,
                lower_doc_key,
                upper_doc_key,
            );
        }

        pub fn freeForeignKeyIntegrityViolations(self: *DB, violations: []relational_store_mod.ForeignKeyIntegrityViolation) void {
            relational_store_mod.freeForeignKeyIntegrityViolations(self.alloc, violations);
        }

        pub fn listForeignKeyRefChildrenForParent(
            self: *DB,
            alloc: Allocator,
            constraint_name: []const u8,
            parent_table: []const u8,
            parent_key: []const u8,
            limit: usize,
        ) ![]types.ForeignKeyRefChild {
            var page = try listForeignKeyRefChildrenPageForParent(self, alloc, constraint_name, parent_table, parent_key, null, null, limit);
            errdefer freeForeignKeyRefChildrenPage(self, alloc, &page);
            if (!page.complete) return error.ForeignKeyActionLimitExceeded;
            const children = page.children;
            page.children = &.{};
            freeForeignKeyRefChildrenPage(self, alloc, &page);
            return children;
        }

        pub fn listForeignKeyRefChildrenPageForParent(
            self: *DB,
            alloc: Allocator,
            constraint_name: []const u8,
            parent_table: []const u8,
            parent_key: []const u8,
            start_after_child_table: ?[]const u8,
            start_after_child_key: ?[]const u8,
            limit: usize,
        ) !types.ForeignKeyRefChildrenPage {
            if ((start_after_child_table == null) != (start_after_child_key == null)) return error.ForeignKeyViolation;
            self.core.lockApply();
            defer self.core.unlockApply();

            const runtime_schema = self.core.schema orelse return error.ForeignKeyViolation;
            if (runtime_schema.storage_mode != .relational) return error.ForeignKeyViolation;
            const foreign_key = findRuntimeForeignKeyByName(runtime_schema.foreign_keys, constraint_name) orelse return error.ForeignKeyViolation;
            if (!foreignKeyIsEnforcedImmediate(foreign_key)) return error.ForeignKeyViolation;
            if (!std.mem.eql(u8, foreign_key.parent_table, parent_table)) return error.ForeignKeyViolation;

            const prefix = try internal_keys.relationalForeignKeyRefParentPrefixAlloc(alloc, constraint_name, parent_table, parent_key);
            defer alloc.free(prefix);
            const upper = try internal_keys.relationalForeignKeyRefParentPrefixUpperAlloc(alloc, constraint_name, parent_table, parent_key);
            defer if (upper) |buf| alloc.free(buf);
            const scanned = try self.core.store.scanRange(alloc, prefix, if (upper) |buf| buf else "");
            defer docstore_mod.DocStore.freeResults(alloc, scanned);

            var out = std.ArrayListUnmanaged(types.ForeignKeyRefChild).empty;
            errdefer {
                for (out.items) |child| {
                    alloc.free(@constCast(child.child_table));
                    alloc.free(@constCast(child.child_key));
                }
                out.deinit(alloc);
            }
            var complete = true;
            for (scanned) |entry| {
                var decoded = (try internal_keys.decodeRelationalForeignKeyRefKeyAlloc(alloc, entry.key)) orelse continue;
                defer decoded.deinit(alloc);
                if (!std.mem.eql(u8, decoded.child_table, runtime_schema.default_type)) continue;
                if (start_after_child_table) |cursor_table| {
                    if (compareForeignKeyRefChildCursor(decoded.child_table, decoded.child_key, cursor_table, start_after_child_key.?) != .gt) continue;
                }
                if (limit > 0 and out.items.len == limit) {
                    complete = false;
                    break;
                }
                try out.append(alloc, .{
                    .child_table = try alloc.dupe(u8, decoded.child_table),
                    .child_key = try alloc.dupe(u8, decoded.child_key),
                });
            }
            const children = try out.toOwnedSlice(alloc);
            errdefer {
                for (children) |child| {
                    alloc.free(@constCast(child.child_table));
                    alloc.free(@constCast(child.child_key));
                }
                if (children.len > 0) alloc.free(children);
            }
            var next_child_table: ?[]const u8 = null;
            var next_child_key: ?[]const u8 = null;
            errdefer {
                if (next_child_table) |value| alloc.free(@constCast(value));
                if (next_child_key) |value| alloc.free(@constCast(value));
            }
            if (!complete and children.len > 0) {
                next_child_table = try alloc.dupe(u8, children[children.len - 1].child_table);
                next_child_key = try alloc.dupe(u8, children[children.len - 1].child_key);
            }
            return .{
                .children = children,
                .complete = complete,
                .next_child_table = next_child_table,
                .next_child_key = next_child_key,
            };
        }

        pub fn freeForeignKeyRefChildren(_: *DB, alloc: Allocator, children: []types.ForeignKeyRefChild) void {
            for (children) |child| {
                alloc.free(child.child_table);
                alloc.free(child.child_key);
            }
            if (children.len > 0) alloc.free(children);
        }

        pub fn freeForeignKeyRefChildrenPage(self: *DB, alloc: Allocator, page: *types.ForeignKeyRefChildrenPage) void {
            freeForeignKeyRefChildren(self, alloc, page.children);
            if (page.next_child_table) |value| alloc.free(@constCast(value));
            if (page.next_child_key) |value| alloc.free(@constCast(value));
            page.* = undefined;
        }

        fn compareForeignKeyRefChildCursor(
            lhs_table: []const u8,
            lhs_key: []const u8,
            rhs_table: []const u8,
            rhs_key: []const u8,
        ) std.math.Order {
            const table_order = std.mem.order(u8, lhs_table, rhs_table);
            if (table_order != .eq) return table_order;
            return std.mem.order(u8, lhs_key, rhs_key);
        }

        pub fn reconcileForeignKeyRefsInRangeLocked(
            self: *DB,
            constraint_name: ?[]const u8,
            lower_doc_key: []const u8,
            upper_doc_key: []const u8,
            mode: relational_store_mod.ForeignKeyIntegrityMode,
        ) !ForeignKeyIntegrityReport {
            const runtime_schema = self.core.schema orelse return .{};
            if (runtime_schema.storage_mode != .relational or runtime_schema.foreign_keys.len == 0) return .{};
            const foreign_keys = try foreignKeysForIntegrityConstraint(self.alloc, runtime_schema.foreign_keys, constraint_name);
            defer if (constraint_name == null and foreign_keys.len > 0) self.alloc.free(foreign_keys);
            if (foreign_keys.len == 0) return .{};
            const report = try relational_store_mod.reconcileForeignKeyRefsInRangeWithPrimaryKeyAndIndexes(
                self.alloc,
                self.core.store,
                runtime_schema.default_type,
                runtime_schema.relational_columns,
                runtime_schema.periods,
                foreign_keys,
                runtime_schema.primary_key,
                runtime_schema.unique_constraints,
                runtime_schema.relational_indexes,
                lower_doc_key,
                upper_doc_key,
                mode,
            );
            try recordForeignKeyIntegrityProgressLocked(self, self.alloc, mode, constraint_name, lower_doc_key, upper_doc_key, report);
            return report;
        }

        pub fn reconcileUniqueConstraintRowsInRangeLocked(
            self: *DB,
            lower_doc_key: []const u8,
            upper_doc_key: []const u8,
            mode: relational_store_mod.ForeignKeyIntegrityMode,
        ) !UniqueConstraintIntegrityReport {
            const runtime_schema = self.core.schema orelse return .{};
            if (runtime_schema.storage_mode != .relational or (runtime_schema.primary_key == null and runtime_schema.unique_constraints.len == 0)) return .{};
            const owner_constraints = try uniqueOwnerConstraintsAlloc(self.alloc, runtime_schema);
            defer self.alloc.free(owner_constraints);
            const report = try relational_store_mod.reconcileUniqueConstraintRowsInRange(
                self.alloc,
                self.core.store,
                runtime_schema.relational_columns,
                runtime_schema.periods,
                owner_constraints,
                runtime_schema.relational_indexes,
                lower_doc_key,
                upper_doc_key,
                mode,
            );
            try recordUniqueConstraintIntegrityProgressLocked(self, self.alloc, mode, lower_doc_key, upper_doc_key, report);
            return report;
        }

        pub fn validateUniqueConstraintMutations(
            self: *DB,
            unique_writes: []const types.UniqueConstraintMutation,
            unique_deletes: []const types.UniqueConstraintMutation,
        ) !void {
            if (unique_writes.len == 0 and unique_deletes.len == 0) return;
            const runtime_schema = self.core.schema orelse return error.UniqueConstraintViolation;
            if (runtime_schema.storage_mode != .relational) return error.UniqueConstraintViolation;
            const owner_constraints = try uniqueOwnerConstraintsAlloc(self.alloc, runtime_schema);
            defer self.alloc.free(owner_constraints);
            for (unique_writes) |mutation| {
                const constraint = try Self.validateUniqueConstraintMutation(self, owner_constraints, mutation);
                if (mutationIsTemporal(mutation)) {
                    if (findTemporalUniqueConstraintMutationConflict(unique_writes, mutation)) |existing| {
                        if (!std.mem.eql(u8, existing.owner_key, mutation.owner_key)) {
                            return error.UniqueConstraintViolation;
                        }
                    }
                    const committed_owner = relational_store_mod.lookupTemporalUniqueOverlapOwnerAlloc(
                        self.alloc,
                        self.core.store,
                        mutation.constraint_name,
                        mutation.encoded_value,
                        mutation.temporal_start.?,
                        mutation.temporal_end.?,
                    ) catch |err| switch (err) {
                        error.NotFound => continue,
                        else => return err,
                    };
                    if (committed_owner) |owner| {
                        defer self.alloc.free(owner);
                        if (std.mem.eql(u8, owner, mutation.owner_key)) continue;
                        if (findTemporalUniqueConstraintMutationConflict(unique_deletes, mutation)) |delete_mutation| {
                            if (std.mem.eql(u8, delete_mutation.owner_key, owner)) continue;
                        }
                        return error.UniqueConstraintViolation;
                    }
                    _ = constraint;
                    continue;
                }
                const key = try internal_keys.relationalUniqueKeyAlloc(self.alloc, mutation.constraint_name, mutation.encoded_value);
                defer self.alloc.free(key);
                if (findUniqueConstraintMutation(unique_writes, mutation.constraint_name, mutation.encoded_value)) |existing| {
                    if (!std.mem.eql(u8, existing.owner_key, mutation.owner_key)) {
                        return error.UniqueConstraintViolation;
                    }
                }
                const committed_owner = self.core.store.get(self.alloc, key) catch |err| switch (err) {
                    error.NotFound => continue,
                    else => return err,
                };
                defer self.alloc.free(committed_owner);
                if (std.mem.eql(u8, committed_owner, mutation.owner_key)) continue;
                if (findUniqueConstraintMutation(unique_deletes, mutation.constraint_name, mutation.encoded_value)) |delete_mutation| {
                    if (std.mem.eql(u8, delete_mutation.owner_key, committed_owner)) continue;
                }
                return error.UniqueConstraintViolation;
            }
            for (unique_deletes) |mutation| {
                _ = try Self.validateUniqueConstraintMutation(self, owner_constraints, mutation);
                if (mutationIsTemporal(mutation)) {
                    const committed_owner = relational_store_mod.lookupTemporalUniqueOverlapOwnerAlloc(
                        self.alloc,
                        self.core.store,
                        mutation.constraint_name,
                        mutation.encoded_value,
                        mutation.temporal_start.?,
                        mutation.temporal_end.?,
                    ) catch |err| switch (err) {
                        error.NotFound => continue,
                        else => return err,
                    };
                    if (committed_owner) |owner| {
                        defer self.alloc.free(owner);
                        if (std.mem.eql(u8, owner, mutation.owner_key)) continue;
                        if (findTemporalUniqueConstraintMutationConflict(unique_writes, mutation)) |write_mutation| {
                            if (std.mem.eql(u8, write_mutation.owner_key, owner)) continue;
                        }
                        return error.UniqueConstraintViolation;
                    }
                    continue;
                }
                const key = try internal_keys.relationalUniqueKeyAlloc(self.alloc, mutation.constraint_name, mutation.encoded_value);
                defer self.alloc.free(key);
                const committed_owner = self.core.store.get(self.alloc, key) catch |err| switch (err) {
                    error.NotFound => continue,
                    else => return err,
                };
                defer self.alloc.free(committed_owner);
                if (std.mem.eql(u8, committed_owner, mutation.owner_key)) continue;
                if (findUniqueConstraintMutation(unique_writes, mutation.constraint_name, mutation.encoded_value)) |write_mutation| {
                    if (std.mem.eql(u8, write_mutation.owner_key, committed_owner)) continue;
                }
                return error.UniqueConstraintViolation;
            }
        }

        fn validateUniqueConstraintMutation(
            _: *DB,
            constraints: []const schema_mod.UniqueConstraint,
            mutation: types.UniqueConstraintMutation,
        ) !schema_mod.UniqueConstraint {
            if (mutation.constraint_name.len == 0 or mutation.encoded_value.len == 0 or mutation.owner_key.len == 0) {
                return error.UniqueConstraintViolation;
            }
            if (!isForeignKeyExternalDocKey(mutation.owner_key)) {
                return error.UniqueConstraintViolation;
            }
            const constraint = findUniqueConstraintByName(constraints, mutation.constraint_name) orelse return error.UniqueConstraintViolation;
            const has_start = mutation.temporal_start != null;
            const has_end = mutation.temporal_end != null;
            if (has_start != has_end) return error.UniqueConstraintViolation;
            if (has_start) {
                if (constraint.without_overlaps_period == null) return error.UniqueConstraintViolation;
                if (!(relational_store_mod.temporalPeriodSpanBytesValid(mutation.temporal_start.?, mutation.temporal_end.?) catch return error.UniqueConstraintViolation)) {
                    return error.UniqueConstraintViolation;
                }
            } else if (constraint.without_overlaps_period != null or !relational_store_mod.uniqueConstraintUsesDedicatedOwnerRows(constraint)) {
                return error.UniqueConstraintViolation;
            }
            return constraint;
        }

        pub fn appendUniqueConstraintMutationIntents(
            self: *DB,
            intents: *std.ArrayListUnmanaged(transactions_mod.WriteIntent),
            owned_keys: *std.ArrayListUnmanaged([]u8),
            owned_values: *std.ArrayListUnmanaged([]u8),
            unique_writes: []const types.UniqueConstraintMutation,
            unique_deletes: []const types.UniqueConstraintMutation,
        ) !void {
            if (unique_writes.len == 0 and unique_deletes.len == 0) return;
            const runtime_schema = self.core.schema orelse return error.UniqueConstraintViolation;
            if (runtime_schema.storage_mode != .relational) return error.UniqueConstraintViolation;
            const owner_constraints = try uniqueOwnerConstraintsAlloc(self.alloc, runtime_schema);
            defer self.alloc.free(owner_constraints);
            for (unique_deletes) |mutation| {
                _ = try Self.validateUniqueConstraintMutation(self, owner_constraints, mutation);
                const key = if (mutationIsTemporal(mutation))
                    try internal_keys.relationalTemporalUniqueKeyAlloc(self.alloc, mutation.constraint_name, mutation.encoded_value, mutation.temporal_start.?, mutation.temporal_end.?, mutation.owner_key)
                else
                    try internal_keys.relationalUniqueKeyAlloc(self.alloc, mutation.constraint_name, mutation.encoded_value);
                errdefer self.alloc.free(key);
                try owned_keys.append(self.alloc, key);
                try intents.append(self.alloc, .{ .key = key, .value = null });
            }
            for (unique_writes) |mutation| {
                _ = try Self.validateUniqueConstraintMutation(self, owner_constraints, mutation);
                const key = if (mutationIsTemporal(mutation))
                    try internal_keys.relationalTemporalUniqueKeyAlloc(self.alloc, mutation.constraint_name, mutation.encoded_value, mutation.temporal_start.?, mutation.temporal_end.?, mutation.owner_key)
                else
                    try internal_keys.relationalUniqueKeyAlloc(self.alloc, mutation.constraint_name, mutation.encoded_value);
                errdefer self.alloc.free(key);
                const owner = try self.alloc.dupe(u8, mutation.owner_key);
                errdefer self.alloc.free(owner);
                try owned_keys.append(self.alloc, key);
                try owned_values.append(self.alloc, owner);
                try intents.append(self.alloc, .{ .key = key, .value = owner });
            }
        }

        pub fn appendForeignKeyExternalizedParentCheckIntents(
            self: *DB,
            txn_id: types.TxnId,
            intents: *std.ArrayListUnmanaged(transactions_mod.WriteIntent),
            owned_keys: *std.ArrayListUnmanaged([]u8),
            owned_values: *std.ArrayListUnmanaged([]u8),
            checks: []const types.ForeignKeyParentCheck,
        ) !void {
            for (checks) |check| {
                const key = try foreignKeyExternalizedParentCheckIntentKeyAlloc(self.alloc, txn_id, check);
                errdefer self.alloc.free(key);
                const value = try encodeForeignKeyExternalizedParentCheckIntentValueAlloc(self.alloc, check);
                errdefer self.alloc.free(value);
                try owned_keys.append(self.alloc, key);
                try owned_values.append(self.alloc, value);
                try intents.append(self.alloc, .{ .key = key, .value = value });
            }
        }

        pub fn appendForeignKeyConstraintTimingOverrideIntents(
            self: *DB,
            txn_id: types.TxnId,
            intents: *std.ArrayListUnmanaged(transactions_mod.WriteIntent),
            owned_keys: *std.ArrayListUnmanaged([]u8),
            owned_values: *std.ArrayListUnmanaged([]u8),
            overrides: []const types.ForeignKeyConstraintTimingOverride,
        ) !void {
            for (overrides) |override| {
                const key = try foreignKeyConstraintTimingOverrideIntentKeyAlloc(self.alloc, txn_id, override);
                errdefer self.alloc.free(key);
                const value = try encodeForeignKeyConstraintTimingOverrideIntentValueAlloc(self.alloc, override);
                errdefer self.alloc.free(value);
                try owned_keys.append(self.alloc, key);
                try owned_values.append(self.alloc, value);
                try intents.append(self.alloc, .{ .key = key, .value = value });
            }
        }

        pub fn validateForeignKeyConstraintTimingOverrides(
            self: *DB,
            overrides: []const types.ForeignKeyConstraintTimingOverride,
        ) !void {
            if (overrides.len == 0) return;
            const runtime_schema = self.core.schema orelse return error.ForeignKeyViolation;
            if (runtime_schema.storage_mode != .relational) return error.ForeignKeyViolation;
            for (overrides, 0..) |override, i| {
                if (override.constraint_name.len == 0) return error.ForeignKeyViolation;
                if (override.timing != .immediate and override.timing != .deferred) return error.ForeignKeyViolation;
                for (overrides[0..i]) |previous| {
                    if (std.mem.eql(u8, previous.constraint_name, override.constraint_name)) return error.ForeignKeyViolation;
                }
                const foreign_key = findRuntimeForeignKeyByName(runtime_schema.foreign_keys, override.constraint_name) orelse return error.ForeignKeyViolation;
                if (foreign_key.validation_state != .enforced) return error.ForeignKeyViolation;
                if (!foreign_key.deferrable) return error.ForeignKeyViolation;
            }
        }

        pub fn appendForeignKeyRefMutationIntents(
            self: *DB,
            intents: *std.ArrayListUnmanaged(transactions_mod.WriteIntent),
            owned_keys: *std.ArrayListUnmanaged([]u8),
            ref_writes: []const types.ForeignKeyRefMutation,
            ref_deletes: []const types.ForeignKeyRefMutation,
        ) !void {
            for (ref_writes) |mutation| {
                const key = try internal_keys.relationalForeignKeyRefKeyAlloc(self.alloc, mutation.constraint_name, mutation.parent_table, mutation.parent_key, mutation.child_table, mutation.child_key);
                errdefer self.alloc.free(key);
                try owned_keys.append(self.alloc, key);
                try intents.append(self.alloc, .{ .key = key, .value = "" });
            }
            for (ref_deletes) |mutation| {
                const key = try internal_keys.relationalForeignKeyRefKeyAlloc(self.alloc, mutation.constraint_name, mutation.parent_table, mutation.parent_key, mutation.child_table, mutation.child_key);
                errdefer self.alloc.free(key);
                try owned_keys.append(self.alloc, key);
                try intents.append(self.alloc, .{ .key = key, .value = null });
            }
        }

        pub fn validateForeignKeyRefMutations(self: *DB, mutations: []const types.ForeignKeyRefMutation) !void {
            if (mutations.len == 0) return;
            const runtime_schema = self.core.schema orelse return error.ForeignKeyViolation;
            if (runtime_schema.storage_mode != .relational) return error.ForeignKeyViolation;
            for (mutations) |mutation| {
                if (mutation.constraint_name.len == 0 or mutation.parent_table.len == 0 or mutation.parent_key.len == 0 or mutation.child_table.len == 0 or mutation.child_key.len == 0) return error.ForeignKeyViolation;
                if (!isForeignKeyExternalDocKey(mutation.child_key)) return error.ForeignKeyViolation;
                if (!std.mem.eql(u8, mutation.child_table, runtime_schema.default_type)) return error.ForeignKeyViolation;
                const foreign_key = findRuntimeForeignKeyByName(runtime_schema.foreign_keys, mutation.constraint_name) orelse return error.ForeignKeyViolation;
                if (foreign_key.validation_state != .enforced) return error.ForeignKeyViolation;
                if (!std.mem.eql(u8, foreign_key.parent_table, mutation.parent_table)) return error.ForeignKeyViolation;
                if (foreignKeyReferencesPrimaryKey(foreign_key)) {
                    if (!isForeignKeyExternalDocKey(mutation.parent_key)) return error.ForeignKeyViolation;
                }
            }
        }

        pub fn applyForeignKeyParentDeleteActions(
            self: *DB,
            intents: *std.ArrayListUnmanaged(transactions_mod.WriteIntent),
            owned_keys: *std.ArrayListUnmanaged([]u8),
            owned_values: *std.ArrayListUnmanaged([]u8),
            checks: []const types.ForeignKeyParentDeleteCheck,
        ) !void {
            if (checks.len == 0) return;
            const runtime_schema = self.core.schema orelse return error.ForeignKeyViolation;
            if (runtime_schema.storage_mode != .relational) return error.ForeignKeyViolation;

            var action_writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
            defer action_writes.deinit(self.alloc);
            var action_deletes = std.ArrayListUnmanaged([]const u8).empty;
            defer action_deletes.deinit(self.alloc);
            var participant = relational_store_mod.WriteParticipant.initWithColumnIndexPolicy(
                self.alloc,
                self.core.store,
                &action_writes,
                &action_deletes,
                owned_keys,
                owned_values,
                relationalColumnIndexPolicyForStore(self),
            );
            participant.configureForeignKeys(runtime_schema.default_type, runtime_schema.foreign_keys, &.{});
            participant.configurePrimaryKey(runtime_schema.primary_key);
            participant.configureUniqueConstraints(runtime_schema.unique_constraints);
            participant.configurePeriods(runtime_schema.periods, runtime_schema.relational_columns);
            var prepared = false;
            var closed = false;
            defer if (prepared and !closed) participant.abort(null);

            for (checks) |check| {
                if (check.operation != .delete) continue;
                const foreign_key = findRuntimeForeignKeyByName(runtime_schema.foreign_keys, check.constraint_name) orelse return error.ForeignKeyViolation;
                if (foreign_key.on_delete != .set_null) continue;
                if (check.constraint_name.len == 0 or check.parent_table.len == 0 or check.parent_key.len == 0) return error.ForeignKeyViolation;
                if (!isForeignKeyExternalDocKey(check.parent_key)) return error.ForeignKeyViolation;
                try participant.prepareSetNullForeignKeyParentDelete(check.constraint_name, check.parent_table, check.parent_key);
                prepared = true;
            }
            if (!prepared) return;
            try participant.commit(null, 0);
            closed = true;

            for (action_writes.items) |write| {
                try intents.append(self.alloc, .{ .key = write.key, .value = write.value });
            }
            for (action_deletes.items) |key| {
                try intents.append(self.alloc, .{ .key = key, .value = null });
            }
        }

        pub fn applyForeignKeySetNullChildActions(
            self: *DB,
            intents: *std.ArrayListUnmanaged(transactions_mod.WriteIntent),
            owned_keys: *std.ArrayListUnmanaged([]u8),
            owned_values: *std.ArrayListUnmanaged([]u8),
            actions: []const types.ForeignKeySetNullChildAction,
        ) !void {
            if (actions.len == 0) return;
            const runtime_schema = self.core.schema orelse return error.ForeignKeyViolation;
            if (runtime_schema.storage_mode != .relational) return error.ForeignKeyViolation;

            var action_writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
            defer action_writes.deinit(self.alloc);
            var action_deletes = std.ArrayListUnmanaged([]const u8).empty;
            defer action_deletes.deinit(self.alloc);
            var participant = relational_store_mod.WriteParticipant.initWithColumnIndexPolicy(
                self.alloc,
                self.core.store,
                &action_writes,
                &action_deletes,
                owned_keys,
                owned_values,
                relationalColumnIndexPolicyForStore(self),
            );
            participant.configureForeignKeys(runtime_schema.default_type, runtime_schema.foreign_keys, &.{});
            participant.configurePrimaryKey(runtime_schema.primary_key);
            participant.configureUniqueConstraints(runtime_schema.unique_constraints);
            participant.configurePeriods(runtime_schema.periods, runtime_schema.relational_columns);
            var prepared = false;
            var closed = false;
            defer if (prepared and !closed) participant.abort(null);

            for (actions) |action| {
                if (action.constraint_name.len == 0 or action.parent_table.len == 0 or action.parent_key.len == 0 or action.child_key.len == 0) return error.ForeignKeyViolation;
                if (!isForeignKeyExternalDocKey(action.child_key)) return error.ForeignKeyViolation;
                const foreign_key = findRuntimeForeignKeyByName(runtime_schema.foreign_keys, action.constraint_name) orelse return error.ForeignKeyViolation;
                if (foreignKeyReferencesPrimaryKey(foreign_key) and !isForeignKeyExternalDocKey(action.parent_key)) return error.ForeignKeyViolation;
                switch (action.operation) {
                    .delete => try participant.prepareSetNullForeignKeyChildAction(action.constraint_name, action.parent_table, action.parent_key, action.child_key),
                    .update => try participant.prepareSetNullForeignKeyUpdateChildAction(action.constraint_name, action.parent_table, action.parent_key, action.child_key),
                }
                prepared = true;
            }
            if (!prepared) return;
            try participant.commit(null, 0);
            closed = true;

            for (action_writes.items) |write| {
                try intents.append(self.alloc, .{ .key = write.key, .value = write.value });
            }
            for (action_deletes.items) |key| {
                try intents.append(self.alloc, .{ .key = key, .value = null });
            }
        }

        pub fn applyForeignKeyCascadeChildActions(
            self: *DB,
            intents: *std.ArrayListUnmanaged(transactions_mod.WriteIntent),
            owned_keys: *std.ArrayListUnmanaged([]u8),
            owned_values: *std.ArrayListUnmanaged([]u8),
            actions: []const types.ForeignKeyCascadeChildAction,
        ) !void {
            if (actions.len == 0) return;
            const runtime_schema = self.core.schema orelse return error.ForeignKeyViolation;
            if (runtime_schema.storage_mode != .relational) return error.ForeignKeyViolation;

            var action_writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
            defer action_writes.deinit(self.alloc);
            var action_deletes = std.ArrayListUnmanaged([]const u8).empty;
            defer action_deletes.deinit(self.alloc);
            var participant = relational_store_mod.WriteParticipant.initWithColumnIndexPolicy(
                self.alloc,
                self.core.store,
                &action_writes,
                &action_deletes,
                owned_keys,
                owned_values,
                relationalColumnIndexPolicyForStore(self),
            );
            participant.configureForeignKeys(runtime_schema.default_type, runtime_schema.foreign_keys, &.{});
            participant.configurePrimaryKey(runtime_schema.primary_key);
            participant.configureUniqueConstraints(runtime_schema.unique_constraints);
            participant.configurePeriods(runtime_schema.periods, runtime_schema.relational_columns);
            var prepared = false;
            var closed = false;
            defer if (prepared and !closed) participant.abort(null);

            for (actions) |action| {
                if (action.constraint_name.len == 0 or action.parent_table.len == 0 or action.parent_key.len == 0 or action.child_key.len == 0) return error.ForeignKeyViolation;
                if (!isForeignKeyExternalDocKey(action.child_key)) return error.ForeignKeyViolation;
                const foreign_key = findRuntimeForeignKeyByName(runtime_schema.foreign_keys, action.constraint_name) orelse return error.ForeignKeyViolation;
                if (foreignKeyReferencesPrimaryKey(foreign_key) and !isForeignKeyExternalDocKey(action.parent_key)) return error.ForeignKeyViolation;
                switch (action.operation) {
                    .delete => try participant.prepareCascadeForeignKeyChildAction(action.constraint_name, action.parent_table, action.parent_key, action.child_key),
                    .update => {
                        const updated_parent_key = action.updated_parent_key orelse return error.ForeignKeyViolation;
                        if (foreignKeyReferencesPrimaryKey(foreign_key) and !isForeignKeyExternalDocKey(updated_parent_key)) return error.ForeignKeyViolation;
                        try participant.prepareCascadeForeignKeyUpdateChildAction(action.constraint_name, action.parent_table, action.parent_key, updated_parent_key, action.child_key);
                    },
                }
                prepared = true;
            }
            if (!prepared) return;
            try participant.commit(null, 0);
            closed = true;

            for (action_writes.items) |write| {
                try intents.append(self.alloc, .{ .key = write.key, .value = write.value });
            }
            for (action_deletes.items) |key| {
                try intents.append(self.alloc, .{ .key = key, .value = null });
            }
        }

        pub fn appendForeignKeyConflictIntents(
            self: *DB,
            intents: *std.ArrayListUnmanaged(transactions_mod.WriteIntent),
            owned_keys: *std.ArrayListUnmanaged([]u8),
            writes: []const types.TransactionWrite,
            parent_delete_checks: []const types.ForeignKeyParentDeleteCheck,
            conflict_checks: []const types.ForeignKeyConflictCheck,
            ref_writes: []const types.ForeignKeyRefMutation,
        ) !void {
            if (writes.len == 0 and parent_delete_checks.len == 0 and conflict_checks.len == 0 and ref_writes.len == 0) return;
            if (self.core.schema) |runtime_schema| {
                for (writes) |write| {
                    if (isMetadataOrPhysicalTableDataKey(write.key)) continue;
                    for (runtime_schema.foreign_keys) |foreign_key| {
                        if (!foreignKeyNeedsRestrictConflictKey(foreign_key)) continue;
                        const parent_key = (try foreignKeyParentKeyFromJsonAlloc(self.alloc, runtime_schema, foreign_key, write.value)) orelse continue;
                        defer self.alloc.free(parent_key);
                        try appendForeignKeyConflictIntent(self.alloc, intents, owned_keys, foreign_key.name, foreign_key.parent_table, parent_key);
                    }
                }
            }
            for (ref_writes) |mutation| {
                try appendForeignKeyConflictIntent(self.alloc, intents, owned_keys, mutation.constraint_name, mutation.parent_table, mutation.parent_key);
            }
            for (parent_delete_checks) |check| {
                try appendForeignKeyConflictIntent(self.alloc, intents, owned_keys, check.constraint_name, check.parent_table, check.parent_key);
            }
            for (conflict_checks) |check| {
                try appendForeignKeyConflictIntent(self.alloc, intents, owned_keys, check.constraint_name, check.parent_table, check.parent_key);
            }
        }

        fn appendForeignKeyConflictIntent(
            alloc: Allocator,
            intents: *std.ArrayListUnmanaged(transactions_mod.WriteIntent),
            owned_keys: *std.ArrayListUnmanaged([]u8),
            constraint_name: []const u8,
            parent_table: []const u8,
            parent_key: []const u8,
        ) !void {
            const key = try internal_keys.relationalForeignKeyConflictKeyAlloc(alloc, constraint_name, parent_table, parent_key);
            errdefer alloc.free(key);
            if (containsString(owned_keys.items, key)) {
                alloc.free(key);
                return;
            }
            try owned_keys.append(alloc, key);
            try intents.append(alloc, .{ .key = key, .value = null });
        }

        fn foreignKeyNeedsRestrictConflictKey(foreign_key: schema_mod.ForeignKey) bool {
            return foreign_key.validation_state == .enforced and
                foreignKeyDeleteActionRestricts(foreign_key) and
                foreign_key.parent_columns.len == 1 and
                std.mem.eql(u8, foreign_key.parent_columns[0], "_id");
        }

        pub fn validateForeignKeyParentChecks(
            self: *DB,
            checks: []const types.ForeignKeyParentCheck,
            writes: []const types.TransactionWrite,
            deletes: []const []const u8,
            unique_writes: []const types.UniqueConstraintMutation,
            unique_deletes: []const types.UniqueConstraintMutation,
        ) !void {
            if (checks.len == 0) return;
            const runtime_schema = self.core.schema orelse return error.ForeignKeyViolation;
            if (runtime_schema.storage_mode != .relational) return error.ForeignKeyViolation;
            for (checks) |check| {
                if (check.parent_key.len == 0 or check.parent_table.len == 0 or check.constraint_name.len == 0 or check.child_table.len == 0 or check.child_key.len == 0) return error.ForeignKeyViolation;
                if (!isForeignKeyExternalDocKey(check.child_key)) return error.ForeignKeyViolation;
                if (!std.mem.eql(u8, runtime_schema.default_type, check.parent_table)) return error.ForeignKeyViolation;
                const has_temporal_payload = check.child_period_start_json != null and check.child_period_end_json != null;
                if ((check.child_period_start_json == null) != (check.child_period_end_json == null)) return error.ForeignKeyViolation;
                if (has_temporal_payload) {
                    try validateTemporalForeignKeyParentCheck(self, runtime_schema, check, deletes);
                    continue;
                }
                if (check.parent_constraint_name) |constraint_name| {
                    const constraint = findUniqueConstraintByName(runtime_schema.unique_constraints, constraint_name) orelse return error.ForeignKeyViolation;
                    if (findUniqueConstraintMutation(unique_deletes, constraint_name, check.parent_key) != null and
                        findUniqueConstraintMutation(unique_writes, constraint_name, check.parent_key) == null) return error.ForeignKeyViolation;
                    if (findUniqueConstraintMutation(unique_writes, constraint_name, check.parent_key) != null) return error.ForeignKeyViolation;
                    if (try stagedWriteSatisfiesUniqueParentCheck(self.alloc, runtime_schema, writes, constraint, check.parent_key)) continue;
                    const ordered_index = relational_store_mod.relationalIndexForUniqueConstraint(runtime_schema.relational_indexes, constraint, .ordered_tuple) orelse return error.ForeignKeyViolation;
                    if (!relational_store_mod.orderedTupleUniqueIndexLookupReady(constraint, ordered_index)) return error.ForeignKeyViolation;
                    const ordered_parent_tuple = check.ordered_parent_tuple orelse return error.ForeignKeyViolation;
                    const owners = try relational_store_mod.scanOrderedTupleDocKeysAlloc(self.alloc, self.core.store, ordered_index.name, ordered_parent_tuple, "", "");
                    defer relational_store_mod.freeDocKeys(self.alloc, owners);
                    var found_parent = false;
                    for (owners) |owner_doc_key| {
                        if (containsString(deletes, owner_doc_key)) continue;
                        const parent = try relational_store_mod.getRawAlloc(self.alloc, self.core.store, owner_doc_key);
                        if (parent) |raw| {
                            defer self.alloc.free(raw);
                            const tuple = (try relational_store_mod.uniqueConstraintTupleValueWithColumnsAlloc(
                                self.alloc,
                                raw,
                                constraint,
                                runtime_schema.relational_columns,
                            )) orelse continue;
                            defer self.alloc.free(tuple);
                            if (!std.mem.eql(u8, tuple, check.parent_key)) continue;
                            found_parent = true;
                            break;
                        }
                    }
                    if (!found_parent) return error.ForeignKeyViolation;
                    continue;
                }
                if (check.ordered_parent_tuple != null) return error.ForeignKeyViolation;
                if (!isForeignKeyExternalDocKey(check.parent_key)) return error.ForeignKeyViolation;
                if (containsString(deletes, check.parent_key)) return error.ForeignKeyViolation;
                if (containsTransactionWrite(writes, check.parent_key)) continue;
                const existing = try self.get(self.alloc, check.parent_key);
                defer if (existing) |body| self.alloc.free(body);
                if (existing == null) return error.ForeignKeyViolation;
            }
        }

        fn stagedWriteSatisfiesUniqueParentCheck(
            alloc: Allocator,
            runtime_schema: schema_mod.TableSchema,
            writes: []const types.TransactionWrite,
            constraint: schema_mod.UniqueConstraint,
            parent_key: []const u8,
        ) !bool {
            for (writes) |write| {
                const row_value = mapper.buildRelationalRowValueAlloc(alloc, write.value, runtime_schema.relational_columns) catch continue;
                defer alloc.free(row_value);
                const tuple = (try relational_store_mod.uniqueConstraintTupleValueWithColumnsAlloc(
                    alloc,
                    row_value,
                    constraint,
                    runtime_schema.relational_columns,
                )) orelse continue;
                defer alloc.free(tuple);
                if (std.mem.eql(u8, tuple, parent_key)) return true;
            }
            return false;
        }

        pub fn validateForeignKeyReferenceShapes(
            self: *DB,
            writes: []const types.TransactionWrite,
        ) !void {
            if (writes.len == 0) return;
            const runtime_schema = self.core.schema orelse return;
            if (runtime_schema.storage_mode != .relational or runtime_schema.foreign_keys.len == 0) return;
            for (writes) |write| {
                if (isMetadataOrPhysicalTableDataKey(write.key)) continue;
                for (runtime_schema.foreign_keys) |foreign_key| {
                    if (foreign_key.validation_state != .enforced) continue;
                    const parent_key = try foreignKeyParentKeyFromJsonAlloc(self.alloc, runtime_schema, foreign_key, write.value);
                    defer if (parent_key) |value| self.alloc.free(value);
                }
            }
        }

        fn validateTemporalForeignKeyParentCheck(
            self: *DB,
            runtime_schema: schema_mod.TableSchema,
            check: types.ForeignKeyParentCheck,
            deletes: []const []const u8,
        ) !void {
            const start_json = check.child_period_start_json orelse return error.ForeignKeyViolation;
            const end_json = check.child_period_end_json orelse return error.ForeignKeyViolation;
            const parent_constraint = temporalForeignKeyParentConstraint(runtime_schema, check) orelse return error.ForeignKeyViolation;
            const parent_period_name = parent_constraint.without_overlaps_period orelse return error.ForeignKeyViolation;
            const parent_period = relational_store_mod.findPeriod(runtime_schema.periods, parent_period_name) orelse return error.ForeignKeyViolation;
            const start_column = relational_store_mod.findTemporalColumn(runtime_schema.relational_columns, parent_period.start_column) orelse return error.ForeignKeyViolation;
            const end_column = relational_store_mod.findTemporalColumn(runtime_schema.relational_columns, parent_period.end_column) orelse return error.ForeignKeyViolation;
            const child_span = try relationalTemporalSpanFromBoundJsonAlloc(self.alloc, start_json, end_json, start_column, end_column);
            try requireTemporalForeignKeyParentCoverage(self, parent_constraint, check.parent_key, child_span, deletes);
        }

        fn temporalForeignKeyParentConstraint(
            runtime_schema: schema_mod.TableSchema,
            check: types.ForeignKeyParentCheck,
        ) ?schema_mod.UniqueConstraint {
            const name = check.parent_constraint_name orelse return null;
            if (runtime_schema.primary_key) |primary_key| {
                if (std.mem.eql(u8, name, relational_store_mod.primary_key_constraint_name)) {
                    return relational_store_mod.primaryKeyAsUniqueConstraint(primary_key);
                }
            }
            return findUniqueConstraintByName(runtime_schema.unique_constraints, name);
        }

        fn relationalTemporalSpanFromBoundJsonAlloc(
            alloc: Allocator,
            start_json: []const u8,
            end_json: []const u8,
            start_column: schema_mod.RelationalColumn,
            end_column: schema_mod.RelationalColumn,
        ) !RelationalTemporalSpan {
            const start = try relational_store_mod.temporalStartBoundFromJsonAlloc(alloc, start_json, start_column);
            const end = try relational_store_mod.temporalEndBoundFromJsonAlloc(alloc, end_json, end_column);
            const span: RelationalTemporalSpan = .{ .start = start, .end = end };
            if (!relational_store_mod.temporalSpanValid(span)) return error.ForeignKeyViolation;
            return span;
        }

        fn requireTemporalForeignKeyParentCoverage(
            self: *DB,
            parent_constraint: schema_mod.UniqueConstraint,
            parent_key: []const u8,
            child_span: RelationalTemporalSpan,
            deletes: []const []const u8,
        ) !void {
            var covered_end = child_span.start;
            var matched_any = false;
            while (relational_store_mod.temporalBoundLessThan(covered_end, child_span.end)) {
                const next = try findTemporalForeignKeyParentCoverageEnd(self, parent_constraint, parent_key, covered_end, child_span.end, deletes);
                if (next == null) return error.ForeignKeyViolation;
                if (!relational_store_mod.temporalBoundLessThan(covered_end, next.?)) return error.ForeignKeyViolation;
                covered_end = next.?;
                matched_any = true;
            }
            if (!matched_any or !relational_store_mod.temporalBoundEqual(covered_end, child_span.end)) return error.ForeignKeyViolation;
        }

        fn findTemporalForeignKeyParentCoverageEnd(
            self: *DB,
            parent_constraint: schema_mod.UniqueConstraint,
            parent_key: []const u8,
            needed_start: RelationalTemporalBound,
            child_end: RelationalTemporalBound,
            deletes: []const []const u8,
        ) !?RelationalTemporalBound {
            const prefix = try internal_keys.relationalTemporalUniquePrefixAlloc(self.alloc, parent_constraint.name, parent_key);
            defer self.alloc.free(prefix);
            const upper = try internal_keys.relationalTemporalUniquePrefixUpperAlloc(self.alloc, parent_constraint.name, parent_key);
            defer if (upper) |buf| self.alloc.free(buf);
            const scanned = try self.core.store.scanRange(self.alloc, prefix, if (upper) |buf| buf else "");
            defer docstore_mod.DocStore.freeResults(self.alloc, scanned);
            var best: ?RelationalTemporalBound = null;
            for (scanned) |entry| {
                if (containsString(deletes, entry.value)) continue;
                const span = try decodeTemporalForeignKeyOwnerSpanFromKeyAlloc(self.alloc, entry.key, prefix);
                best = temporalForeignKeyCoverageCandidateEnd(best, needed_start, child_end, span);
            }
            return best;
        }

        fn temporalForeignKeyCoverageCandidateEnd(
            current_best: ?RelationalTemporalBound,
            needed_start: RelationalTemporalBound,
            child_end: RelationalTemporalBound,
            parent_span: RelationalTemporalSpan,
        ) ?RelationalTemporalBound {
            if (relational_store_mod.temporalBoundLessThan(needed_start, parent_span.start)) return current_best;
            if (!relational_store_mod.temporalBoundLessThan(needed_start, parent_span.end)) return current_best;
            const candidate = if (relational_store_mod.temporalBoundLessThan(child_end, parent_span.end)) child_end else parent_span.end;
            if (current_best) |best| {
                return if (relational_store_mod.temporalBoundLessThan(best, candidate)) candidate else best;
            }
            return candidate;
        }

        fn decodeTemporalForeignKeyOwnerSpanFromKeyAlloc(
            alloc: Allocator,
            key: []const u8,
            prefix: []const u8,
        ) !RelationalTemporalSpan {
            if (!std.mem.startsWith(u8, key, prefix)) return error.ForeignKeyViolation;
            var pos: usize = prefix.len;
            const start_term = internal_keys.findComponentTerminator(key, pos) orelse return error.ForeignKeyViolation;
            const start = try internal_keys.decodeBodyAlloc(alloc, key[pos..start_term]);
            defer alloc.free(start);
            pos = start_term + 2;
            const end_term = internal_keys.findComponentTerminator(key, pos) orelse return error.ForeignKeyViolation;
            const end = try internal_keys.decodeBodyAlloc(alloc, key[pos..end_term]);
            defer alloc.free(end);
            return .{
                .start = try relationalTemporalBoundFromOwnerBytes(start),
                .end = try relationalTemporalBoundFromOwnerBytes(end),
            };
        }

        fn relationalTemporalBoundFromOwnerBytes(bytes: []const u8) !RelationalTemporalBound {
            if (bytes.len == 1 and bytes[0] == temporal_bound_neg_infinity_tag) return .neg_infinity;
            if (bytes.len == 1 and bytes[0] == temporal_bound_pos_infinity_tag) return .pos_infinity;
            if (bytes.len != 9) return error.ForeignKeyViolation;
            const raw = std.mem.readInt(u64, bytes[1..9], .big);
            if (bytes[0] == @intFromEnum(temporal_typed_dv.ValueType.f64_val)) return .{ .number = @bitCast(raw) };
            if (bytes[0] == @intFromEnum(temporal_typed_dv.ValueType.u64_val)) return .{ .datetime_ns = @bitCast(raw) };
            return error.ForeignKeyViolation;
        }

        pub fn validateExternalizedForeignKeyParentChecks(
            self: *DB,
            checks: []const types.ForeignKeyParentCheck,
            constraint_timing_overrides: []const types.ForeignKeyConstraintTimingOverride,
            writes: []const types.TransactionWrite,
        ) !void {
            const runtime_schema = self.core.schema orelse {
                if (checks.len > 0) return error.ForeignKeyViolation;
                return;
            };
            if (runtime_schema.storage_mode != .relational or runtime_schema.foreign_keys.len == 0) {
                if (checks.len > 0) return error.ForeignKeyViolation;
                return;
            }
            for (checks) |check| {
                if (check.constraint_name.len == 0 or check.child_table.len == 0 or check.child_key.len == 0 or check.parent_table.len == 0 or check.parent_key.len == 0) return error.ForeignKeyViolation;
                const foreign_key = findRuntimeForeignKeyByName(runtime_schema.foreign_keys, check.constraint_name) orelse return error.ForeignKeyViolation;
                if (foreign_key.validation_state != .enforced) return error.ForeignKeyViolation;
                if (check.timing != effectiveForeignKeyParentCheckTiming(foreign_key, constraint_timing_overrides)) return error.ForeignKeyViolation;
                const has_temporal_payload = check.child_period_start_json != null and check.child_period_end_json != null;
                if ((check.child_period_start_json == null) != (check.child_period_end_json == null)) return error.ForeignKeyViolation;
                if ((foreign_key.child_period != null or foreign_key.parent_period != null) != has_temporal_payload) return error.ForeignKeyViolation;
                if (!std.mem.eql(u8, check.child_table, runtime_schema.default_type)) return error.ForeignKeyViolation;
                if (!std.mem.eql(u8, check.parent_table, foreign_key.parent_table)) return error.ForeignKeyViolation;
                if (!isForeignKeyExternalDocKey(check.child_key)) return error.ForeignKeyViolation;
                if (foreignKeyReferencesPrimaryKey(foreign_key)) {
                    if (!isForeignKeyExternalDocKey(check.parent_key)) return error.ForeignKeyViolation;
                    if (check.parent_constraint_name != null) return error.ForeignKeyViolation;
                    if (check.ordered_parent_tuple != null) return error.ForeignKeyViolation;
                } else {
                    const parent_constraint_name = check.parent_constraint_name orelse return error.ForeignKeyViolation;
                    if (parent_constraint_name.len == 0) return error.ForeignKeyViolation;
                    if (std.mem.eql(u8, foreign_key.parent_table, runtime_schema.default_type)) {
                        if (foreign_key.parent_period) |parent_period| {
                            const parent_constraint = temporalForeignKeyParentConstraint(runtime_schema, check) orelse return error.ForeignKeyViolation;
                            if (!stringSlicesEqual(parent_constraint.columns, foreign_key.parent_columns)) return error.ForeignKeyViolation;
                            if (parent_constraint.without_overlaps_period == null or
                                !std.mem.eql(u8, parent_constraint.without_overlaps_period.?, parent_period))
                            {
                                return error.ForeignKeyViolation;
                            }
                        } else {
                            const parent_constraint = findUniqueConstraintByName(runtime_schema.unique_constraints, parent_constraint_name) orelse return error.ForeignKeyViolation;
                            if (!uniqueConstraintCanBackForeignKey(parent_constraint)) return error.ForeignKeyViolation;
                            if (!stringSlicesEqual(parent_constraint.columns, foreign_key.parent_columns)) return error.ForeignKeyViolation;
                        }
                    }
                }
            }
            if (writes.len == 0) return;
            for (runtime_schema.foreign_keys) |foreign_key| {
                if (foreign_key.validation_state != .enforced or effectiveForeignKeyParentCheckTiming(foreign_key, constraint_timing_overrides) != .deferred) continue;
                for (writes) |write| {
                    if (isMetadataOrPhysicalTableDataKey(write.key)) continue;
                    const parent_key = (try foreignKeyParentKeyFromJsonAlloc(self.alloc, runtime_schema, foreign_key, write.value)) orelse continue;
                    defer self.alloc.free(parent_key);
                    var child_period_bounds = try foreignKeyChildPeriodBoundsFromJsonAlloc(self.alloc, runtime_schema, foreign_key, write.value);
                    defer child_period_bounds.deinit(self.alloc);
                    if (!containsExternalizedForeignKeyParentCheck(checks, foreign_key, runtime_schema.default_type, write.key, parent_key, child_period_bounds.start_json, child_period_bounds.end_json)) return error.ForeignKeyViolation;
                }
            }
        }

        fn containsExternalizedForeignKeyParentCheck(
            checks: []const types.ForeignKeyParentCheck,
            foreign_key: schema_mod.ForeignKey,
            child_table: []const u8,
            child_key: []const u8,
            parent_key: []const u8,
            child_period_start_json: ?[]const u8,
            child_period_end_json: ?[]const u8,
        ) bool {
            for (checks) |check| {
                if (check.timing != .deferred) continue;
                if (!std.mem.eql(u8, check.constraint_name, foreign_key.name)) continue;
                if (!std.mem.eql(u8, check.child_table, child_table)) continue;
                if (!std.mem.eql(u8, check.child_key, child_key)) continue;
                if (!std.mem.eql(u8, check.parent_table, foreign_key.parent_table)) continue;
                if (!std.mem.eql(u8, check.parent_key, parent_key)) continue;
                if (!optionalStringsEqual(check.child_period_start_json, child_period_start_json)) continue;
                if (!optionalStringsEqual(check.child_period_end_json, child_period_end_json)) continue;
                return true;
            }
            return false;
        }

        fn effectiveForeignKeyParentCheckTiming(
            foreign_key: schema_mod.ForeignKey,
            overrides: []const types.ForeignKeyConstraintTimingOverride,
        ) types.ForeignKeyParentCheck.Timing {
            for (overrides) |override| {
                if (std.mem.eql(u8, override.constraint_name, foreign_key.name)) return override.timing;
            }
            return foreignKeyParentCheckTiming(foreign_key);
        }

        fn foreignKeyParentCheckTiming(foreign_key: schema_mod.ForeignKey) types.ForeignKeyParentCheck.Timing {
            return switch (foreign_key.timing) {
                .immediate => .immediate,
                .deferred => .deferred,
            };
        }

        fn foreignKeyDeleteActionRestricts(foreign_key: schema_mod.ForeignKey) bool {
            return foreign_key.on_delete == .restrict or foreign_key.on_delete == .no_action;
        }

        fn foreignKeyUpdateActionRestricts(foreign_key: schema_mod.ForeignKey) bool {
            return foreign_key.on_update == .restrict or foreign_key.on_update == .no_action;
        }

        fn containsTransactionWrite(writes: []const types.TransactionWrite, key: []const u8) bool {
            for (writes) |write| {
                if (std.mem.eql(u8, write.key, key)) return true;
            }
            return false;
        }

        fn containsString(values: []const []const u8, key: []const u8) bool {
            for (values) |value| {
                if (std.mem.eql(u8, value, key)) return true;
            }
            return false;
        }

        pub fn validateForeignKeyParentDeleteChecks(
            self: *DB,
            checks: []const types.ForeignKeyParentDeleteCheck,
            constraint_timing_overrides: []const types.ForeignKeyConstraintTimingOverride,
            writes: []const types.TransactionWrite,
            deletes: []const []const u8,
            ref_writes: []const types.ForeignKeyRefMutation,
            ref_deletes: []const types.ForeignKeyRefMutation,
        ) !void {
            if (checks.len == 0) return;
            const runtime_schema = self.core.schema orelse return error.ForeignKeyViolation;
            if (runtime_schema.storage_mode != .relational) return error.ForeignKeyViolation;
            for (checks) |check| {
                if (check.constraint_name.len == 0 or check.parent_table.len == 0 or check.parent_key.len == 0) return error.ForeignKeyViolation;
                const foreign_key = findRuntimeForeignKeyByName(runtime_schema.foreign_keys, check.constraint_name) orelse return error.ForeignKeyViolation;
                if (foreign_key.validation_state != .enforced) return error.ForeignKeyViolation;
                if (check.timing != effectiveForeignKeyParentCheckTiming(foreign_key, constraint_timing_overrides)) return error.ForeignKeyViolation;
                switch (check.operation) {
                    .delete => if (!foreignKeyDeleteActionRestricts(foreign_key)) continue,
                    .update => if (!foreignKeyUpdateActionRestricts(foreign_key)) continue,
                }
                if (foreignKeyReferencesPrimaryKey(foreign_key) and !isForeignKeyExternalDocKey(check.parent_key)) return error.ForeignKeyViolation;
                if (!std.mem.eql(u8, foreign_key.parent_table, check.parent_table)) return error.ForeignKeyViolation;

                for (writes) |write| {
                    if (isMetadataOrPhysicalTableDataKey(write.key)) continue;
                    if (try foreignKeyWriteReferencesParent(self.alloc, runtime_schema, foreign_key, write.value, check.parent_key)) return error.ForeignKeyViolation;
                }
                for (ref_writes) |mutation| {
                    if (foreignKeyRefMutationMatchesParentDeleteCheck(mutation, check)) return error.ForeignKeyViolation;
                }

                const prefix = try internal_keys.relationalForeignKeyRefParentPrefixAlloc(self.alloc, foreign_key.name, foreign_key.parent_table, check.parent_key);
                defer self.alloc.free(prefix);
                const upper = try internal_keys.relationalForeignKeyRefParentPrefixUpperAlloc(self.alloc, foreign_key.name, foreign_key.parent_table, check.parent_key);
                defer if (upper) |buf| self.alloc.free(buf);
                const scanned = try self.core.store.scanRange(self.alloc, prefix, if (upper) |buf| buf else "");
                defer docstore_mod.DocStore.freeResults(self.alloc, scanned);
                for (scanned) |entry| {
                    var decoded = (try internal_keys.decodeRelationalForeignKeyRefKeyAlloc(self.alloc, entry.key)) orelse continue;
                    defer decoded.deinit(self.alloc);
                    if (!std.mem.eql(u8, decoded.child_table, runtime_schema.default_type)) continue;
                    if (containsString(deletes, decoded.child_key)) continue;
                    if (containsForeignKeyRefDelete(ref_deletes, decoded, check)) continue;
                    if (findTransactionWrite(writes, decoded.child_key)) |write| {
                        if (!try foreignKeyWriteReferencesParent(self.alloc, runtime_schema, foreign_key, write.value, check.parent_key)) continue;
                    }
                    return error.ForeignKeyViolation;
                }
                if (check.ordered_child_tuple) |tuple| {
                    try validateForeignKeyParentDeleteCheckViaChildOrderedTuple(
                        self,
                        runtime_schema,
                        foreign_key,
                        check,
                        tuple,
                        writes,
                        deletes,
                        ref_deletes,
                    );
                }
            }
        }

        fn validateForeignKeyParentDeleteCheckViaChildOrderedTuple(
            self: *DB,
            runtime_schema: schema_mod.TableSchema,
            foreign_key: schema_mod.ForeignKey,
            check: types.ForeignKeyParentDeleteCheck,
            ordered_child_tuple: []const u8,
            writes: []const types.TransactionWrite,
            deletes: []const []const u8,
            ref_deletes: []const types.ForeignKeyRefMutation,
        ) !void {
            const index = try relational_store_mod.readyChildOrderedTupleIndexForForeignKey(runtime_schema.relational_indexes, foreign_key) orelse return error.ForeignKeyViolation;
            const child_keys = try relational_store_mod.scanOrderedTupleDocKeysAlloc(self.alloc, self.core.store, index.name, ordered_child_tuple, "", "");
            defer relational_store_mod.freeDocKeys(self.alloc, child_keys);
            for (child_keys) |child_key| {
                if (containsString(deletes, child_key)) continue;
                if (containsForeignKeyRefDeleteForChild(ref_deletes, check, runtime_schema.default_type, child_key)) continue;
                if (findTransactionWrite(writes, child_key)) |write| {
                    if (!try foreignKeyWriteReferencesParent(self.alloc, runtime_schema, foreign_key, write.value, check.parent_key)) continue;
                    return error.ForeignKeyViolation;
                }
                const row = try relational_store_mod.getRawAlloc(self.alloc, self.core.store, child_key);
                if (row) |raw| {
                    defer self.alloc.free(raw);
                    if (try foreignKeyPackedRowReferencesParent(self.alloc, runtime_schema, foreign_key, raw, check.parent_key)) return error.ForeignKeyViolation;
                }
            }
        }

        fn foreignKeyRefMutationMatchesParentDeleteCheck(mutation: types.ForeignKeyRefMutation, check: types.ForeignKeyParentDeleteCheck) bool {
            return std.mem.eql(u8, mutation.constraint_name, check.constraint_name) and
                std.mem.eql(u8, mutation.parent_table, check.parent_table) and
                std.mem.eql(u8, mutation.parent_key, check.parent_key);
        }

        fn containsForeignKeyRefDelete(
            ref_deletes: []const types.ForeignKeyRefMutation,
            decoded: internal_keys.RelationalForeignKeyRefKey,
            check: types.ForeignKeyParentDeleteCheck,
        ) bool {
            for (ref_deletes) |mutation| {
                if (!foreignKeyRefMutationMatchesParentDeleteCheck(mutation, check)) continue;
                if (!std.mem.eql(u8, mutation.child_table, decoded.child_table)) continue;
                if (!std.mem.eql(u8, mutation.child_key, decoded.child_key)) continue;
                return true;
            }
            return false;
        }

        fn containsForeignKeyRefDeleteForChild(
            ref_deletes: []const types.ForeignKeyRefMutation,
            check: types.ForeignKeyParentDeleteCheck,
            child_table: []const u8,
            child_key: []const u8,
        ) bool {
            for (ref_deletes) |mutation| {
                if (!foreignKeyRefMutationMatchesParentDeleteCheck(mutation, check)) continue;
                if (!std.mem.eql(u8, mutation.child_table, child_table)) continue;
                if (!std.mem.eql(u8, mutation.child_key, child_key)) continue;
                return true;
            }
            return false;
        }

        fn findTransactionWrite(writes: []const types.TransactionWrite, key: []const u8) ?types.TransactionWrite {
            for (writes) |write| {
                if (std.mem.eql(u8, write.key, key)) return write;
            }
            return null;
        }

        fn foreignKeyWriteReferencesParent(
            alloc: Allocator,
            runtime_schema: schema_mod.TableSchema,
            foreign_key: schema_mod.ForeignKey,
            value: []const u8,
            parent_key: []const u8,
        ) !bool {
            const actual_parent_key = (try foreignKeyParentKeyFromJsonAlloc(alloc, runtime_schema, foreign_key, value)) orelse return false;
            defer alloc.free(actual_parent_key);
            return std.mem.eql(u8, actual_parent_key, parent_key);
        }

        fn foreignKeyPackedRowReferencesParent(
            alloc: Allocator,
            runtime_schema: schema_mod.TableSchema,
            foreign_key: schema_mod.ForeignKey,
            row_value: []const u8,
            parent_key: []const u8,
        ) !bool {
            const actual_parent_key = (try relational_store_mod.foreignKeyReferenceValueWithColumnsAlloc(
                alloc,
                row_value,
                foreign_key,
                runtime_schema.primary_key,
                runtime_schema.relational_columns,
            )) orelse return false;
            defer alloc.free(actual_parent_key);
            return std.mem.eql(u8, actual_parent_key, parent_key);
        }

        fn foreignKeyParentKeyFromJsonAlloc(
            alloc: Allocator,
            runtime_schema: schema_mod.TableSchema,
            foreign_key: schema_mod.ForeignKey,
            value: []const u8,
        ) !?[]u8 {
            const row = try mapper.buildRelationalRowValueAlloc(alloc, value, runtime_schema.relational_columns);
            defer alloc.free(row);
            return try relational_store_mod.foreignKeyReferenceValueWithColumnsAlloc(alloc, row, foreign_key, runtime_schema.primary_key, runtime_schema.relational_columns);
        }

        const ForeignKeyChildPeriodJsonBounds = struct {
            start_json: ?[]const u8 = null,
            end_json: ?[]const u8 = null,

            fn deinit(self: *@This(), alloc: Allocator) void {
                if (self.start_json) |json| alloc.free(@constCast(json));
                if (self.end_json) |json| alloc.free(@constCast(json));
                self.* = .{};
            }
        };

        fn foreignKeyChildPeriodBoundsFromJsonAlloc(
            alloc: Allocator,
            runtime_schema: schema_mod.TableSchema,
            foreign_key: schema_mod.ForeignKey,
            value: []const u8,
        ) !ForeignKeyChildPeriodJsonBounds {
            const period_name = foreign_key.child_period orelse return .{};
            const period = relational_store_mod.findPeriod(runtime_schema.periods, period_name) orelse return error.ForeignKeyViolation;
            var parsed = std.json.parseFromSlice(std.json.Value, alloc, value, .{}) catch return error.ForeignKeyViolation;
            defer parsed.deinit();
            const obj = switch (parsed.value) {
                .object => |object| object,
                else => return error.ForeignKeyViolation,
            };
            const start_value = obj.get(period.start_column) orelse return error.ForeignKeyViolation;
            const end_value = obj.get(period.end_column) orelse return error.ForeignKeyViolation;
            const start_json = try std.json.Stringify.valueAlloc(alloc, start_value, .{ .emit_null_optional_fields = false });
            errdefer alloc.free(start_json);
            const end_json = try std.json.Stringify.valueAlloc(alloc, end_value, .{ .emit_null_optional_fields = false });
            errdefer alloc.free(end_json);
            return .{ .start_json = start_json, .end_json = end_json };
        }

        fn optionalStringsEqual(a: ?[]const u8, b: ?[]const u8) bool {
            if (a == null and b == null) return true;
            if (a == null or b == null) return false;
            return std.mem.eql(u8, a.?, b.?);
        }

        fn stringSlicesEqual(a: []const []const u8, b: []const []const u8) bool {
            if (a.len != b.len) return false;
            for (a, b) |left, right| {
                if (!std.mem.eql(u8, left, right)) return false;
            }
            return true;
        }

        fn uniqueConstraintCanBackForeignKey(constraint: schema_mod.UniqueConstraint) bool {
            return constraint.where.len == 0 and
                constraint.where_expressions.len == 0 and
                constraint.expressions.len == 0 and
                constraint.without_overlaps_period == null;
        }

        pub fn reconcileForeignKeyRefOwnerForParentLocked(
            self: *DB,
            constraint_name: []const u8,
            parent_table: []const u8,
            parent_key: []const u8,
            mode: relational_store_mod.ForeignKeyIntegrityMode,
        ) !ForeignKeyIntegrityReport {
            const runtime_schema = self.core.schema orelse return error.ForeignKeyViolation;
            if (runtime_schema.storage_mode != .relational) return error.ForeignKeyViolation;
            const foreign_key = findRuntimeForeignKeyByName(runtime_schema.foreign_keys, constraint_name) orelse return error.ForeignKeyViolation;
            if (!foreignKeyIsEnforcedImmediate(foreign_key)) return error.ForeignKeyViolation;
            if (!std.mem.eql(u8, foreign_key.parent_table, parent_table)) return error.ForeignKeyViolation;
            const report = try relational_store_mod.reconcileForeignKeyRefOwnerForParent(
                self.alloc,
                self.core.store,
                runtime_schema.default_type,
                &.{foreign_key},
                constraint_name,
                parent_table,
                parent_key,
                mode,
            );
            self.relationalIntegrityRecordForeignKeyIntegrityReport(mode, report);
            return report;
        }

        pub fn reconcileForeignKeyRefOwnerRangeLocked(
            self: *DB,
            constraint_name: []const u8,
            parent_table: []const u8,
            start_parent_key: []const u8,
            end_parent_key: []const u8,
            mode: relational_store_mod.ForeignKeyIntegrityMode,
        ) !ForeignKeyIntegrityReport {
            const runtime_schema = self.core.schema orelse return error.ForeignKeyViolation;
            if (runtime_schema.storage_mode != .relational) return error.ForeignKeyViolation;
            const foreign_key = findRuntimeForeignKeyByName(runtime_schema.foreign_keys, constraint_name) orelse return error.ForeignKeyViolation;
            if (!foreignKeyIsEnforcedImmediate(foreign_key)) return error.ForeignKeyViolation;
            if (!std.mem.eql(u8, foreign_key.parent_table, parent_table)) return error.ForeignKeyViolation;
            if (end_parent_key.len > 0 and std.mem.order(u8, start_parent_key, end_parent_key) != .lt) return error.ForeignKeyViolation;
            const report = try relational_store_mod.reconcileForeignKeyRefOwnerRange(
                self.alloc,
                self.core.store,
                runtime_schema.default_type,
                &.{foreign_key},
                constraint_name,
                parent_table,
                start_parent_key,
                end_parent_key,
                mode,
            );
            self.relationalIntegrityRecordForeignKeyIntegrityReport(mode, report);
            return report;
        }

        pub fn reconcileForeignKeyRefOwnerRangeForConstraintLocked(
            self: *DB,
            constraint_name: []const u8,
            start_parent_key: []const u8,
            end_parent_key: []const u8,
            mode: relational_store_mod.ForeignKeyIntegrityMode,
        ) !ForeignKeyIntegrityReport {
            const runtime_schema = self.core.schema orelse return error.ForeignKeyViolation;
            if (runtime_schema.storage_mode != .relational) return error.ForeignKeyViolation;
            const foreign_key = findRuntimeForeignKeyByName(runtime_schema.foreign_keys, constraint_name) orelse return error.ForeignKeyViolation;
            if (!foreignKeyIsEnforcedImmediate(foreign_key)) return error.ForeignKeyViolation;
            if (end_parent_key.len > 0 and std.mem.order(u8, start_parent_key, end_parent_key) != .lt) return error.ForeignKeyViolation;
            const report = try relational_store_mod.reconcileForeignKeyRefOwnerRange(
                self.alloc,
                self.core.store,
                runtime_schema.default_type,
                &.{foreign_key},
                constraint_name,
                foreign_key.parent_table,
                start_parent_key,
                end_parent_key,
                mode,
            );
            try recordForeignKeyIntegrityProgressForPhaseLocked(self, self.alloc, "owner_range", mode, constraint_name, start_parent_key, end_parent_key, report);
            return report;
        }

        fn foreignKeysForIntegrityConstraint(
            alloc: Allocator,
            foreign_keys: []const schema_mod.ForeignKey,
            constraint_name: ?[]const u8,
        ) ![]const schema_mod.ForeignKey {
            if (constraint_name == null) {
                var active = std.ArrayListUnmanaged(schema_mod.ForeignKey).empty;
                errdefer active.deinit(alloc);
                for (foreign_keys) |foreign_key| {
                    if (!foreignKeyIsLocallyEnforced(foreign_key)) continue;
                    try active.append(alloc, foreign_key);
                }
                return try active.toOwnedSlice(alloc);
            }
            const name = constraint_name.?;
            for (foreign_keys, 0..) |foreign_key, i| {
                if (!std.mem.eql(u8, foreign_key.name, name)) continue;
                if (!foreignKeyCanRunIntegrity(foreign_key)) return error.ForeignKeyNotFound;
                return foreign_keys[i .. i + 1];
            }
            return error.ForeignKeyNotFound;
        }

        fn foreignKeyCanRunIntegrity(foreign_key: schema_mod.ForeignKey) bool {
            return foreign_key.timing == .immediate or foreign_key.timing == .deferred;
        }

        fn findRuntimeForeignKeyByName(foreign_keys: []const schema_mod.ForeignKey, name: []const u8) ?schema_mod.ForeignKey {
            for (foreign_keys) |foreign_key| {
                if (std.mem.eql(u8, foreign_key.name, name)) return foreign_key;
            }
            return null;
        }

        fn isForeignKeyExternalDocKey(key: []const u8) bool {
            return !db_internal.isMetadataKey(key) and !internal_keys.isInternalPhysicalTableDataKey(key);
        }

        fn isMetadataOrPhysicalTableDataKey(key: []const u8) bool {
            return db_internal.isMetadataKey(key) or internal_keys.isInternalPhysicalTableDataKey(key);
        }

        fn relationalColumnIndexPolicyForStore(self: *DB) relational_store_mod.ColumnIndexPolicy {
            const schema = self.core.schema orelse return relational_store_mod.ColumnIndexPolicy.empty();
            if (schema.storage_mode != .relational) return relational_store_mod.ColumnIndexPolicy.empty();
            return relational_store_mod.ColumnIndexPolicy.fromSchema(schema);
        }

        fn foreignKeyIsEnforcedImmediate(foreign_key: schema_mod.ForeignKey) bool {
            return foreign_key.validation_state == .enforced and foreign_key.timing == .immediate;
        }

        fn foreignKeyIsLocallyEnforced(foreign_key: schema_mod.ForeignKey) bool {
            return foreign_key.validation_state == .enforced and
                (foreign_key.timing == .immediate or foreign_key.timing == .deferred);
        }

        fn foreignKeyReferencesPrimaryKey(foreign_key: schema_mod.ForeignKey) bool {
            return foreign_key.parent_columns.len == 1 and std.mem.eql(u8, foreign_key.parent_columns[0], "_id");
        }

        pub fn freeForeignKeyIntegrityProgressRecord(self: *DB, record: ForeignKeyIntegrityProgressRecord) void {
            if (record.phase.len > 0) self.alloc.free(record.phase);
            if (record.mode.len > 0) self.alloc.free(record.mode);
            if (record.constraint_name) |value| self.alloc.free(value);
            if (record.lower_doc_key.len > 0) self.alloc.free(record.lower_doc_key);
            if (record.upper_doc_key.len > 0) self.alloc.free(record.upper_doc_key);
        }

        pub fn freeForeignKeyIntegrityProgressRecords(self: *DB, records: []ForeignKeyIntegrityProgressRecord) void {
            for (records) |record| self.freeForeignKeyIntegrityProgressRecord(record);
            if (records.len > 0) self.alloc.free(records);
        }

        pub fn freeForeignKeyIntegrityClaimRecord(self: *DB, record: ForeignKeyIntegrityClaimRecord) void {
            if (record.claim_key.len > 0) self.alloc.free(record.claim_key);
            if (record.worker_id.len > 0) self.alloc.free(record.worker_id);
            if (record.phase.len > 0) self.alloc.free(record.phase);
            if (record.planned_action.len > 0) self.alloc.free(record.planned_action);
            if (record.constraint_name) |value| self.alloc.free(value);
            if (record.lower_doc_key.len > 0) self.alloc.free(record.lower_doc_key);
            if (record.upper_doc_key.len > 0) self.alloc.free(record.upper_doc_key);
        }

        pub fn freeForeignKeyIntegrityClaimRecords(self: *DB, records: []ForeignKeyIntegrityClaimRecord) void {
            for (records) |record| self.freeForeignKeyIntegrityClaimRecord(record);
            if (records.len > 0) self.alloc.free(records);
        }

        pub fn freeForeignKeyIntegrityJobRecord(self: *DB, record: ForeignKeyIntegrityJobRecord) void {
            if (record.job_id.len > 0) self.alloc.free(record.job_id);
            if (record.table_name.len > 0) self.alloc.free(record.table_name);
            if (record.action.len > 0) self.alloc.free(record.action);
            if (record.worker_id.len > 0) self.alloc.free(record.worker_id);
            if (record.constraint_name) |value| self.alloc.free(value);
            if (record.lower_doc_key.len > 0) self.alloc.free(record.lower_doc_key);
            if (record.upper_doc_key.len > 0) self.alloc.free(record.upper_doc_key);
            if (record.status.len > 0) self.alloc.free(record.status);
            if (record.violation_samples_json.len > 0) self.alloc.free(record.violation_samples_json);
        }

        pub fn freeForeignKeyIntegrityJobRecords(self: *DB, records: []ForeignKeyIntegrityJobRecord) void {
            for (records) |record| self.freeForeignKeyIntegrityJobRecord(record);
            if (records.len > 0) self.alloc.free(records);
        }

        pub fn freeRelationalIndexRepairJobRecord(self: *DB, record: RelationalIndexRepairJobRecord) void {
            if (record.job_id.len > 0) self.alloc.free(record.job_id);
            if (record.database_name.len > 0) self.alloc.free(record.database_name);
            if (record.namespace_name.len > 0) self.alloc.free(record.namespace_name);
            if (record.table_name.len > 0) self.alloc.free(record.table_name);
            if (record.access_method.len > 0) self.alloc.free(record.access_method);
            if (record.index_name.len > 0) self.alloc.free(record.index_name);
            if (record.worker_id.len > 0) self.alloc.free(record.worker_id);
            if (record.lower_doc_key.len > 0) self.alloc.free(record.lower_doc_key);
            if (record.upper_doc_key.len > 0) self.alloc.free(record.upper_doc_key);
            if (record.status.len > 0) self.alloc.free(record.status);
            if (record.next_lower_doc_key.len > 0) self.alloc.free(record.next_lower_doc_key);
            if (record.cursor.len > 0) self.alloc.free(record.cursor);
            if (record.failure_reason) |value| self.alloc.free(value);
            if (record.last_error) |value| self.alloc.free(value);
        }

        pub fn freeRelationalIndexRepairJobRecords(self: *DB, records: []RelationalIndexRepairJobRecord) void {
            for (records) |record| self.freeRelationalIndexRepairJobRecord(record);
            if (records.len > 0) self.alloc.free(records);
        }

        pub fn freeRelationalIndexDropJobRecord(self: *DB, record: RelationalIndexDropJobRecord) void {
            if (record.job_id.len > 0) self.alloc.free(record.job_id);
            if (record.database_name.len > 0) self.alloc.free(record.database_name);
            if (record.namespace_name.len > 0) self.alloc.free(record.namespace_name);
            if (record.table_name.len > 0) self.alloc.free(record.table_name);
            if (record.access_method.len > 0) self.alloc.free(record.access_method);
            if (record.index_name.len > 0) self.alloc.free(record.index_name);
            if (record.worker_id.len > 0) self.alloc.free(record.worker_id);
            if (record.cursor.len > 0) self.alloc.free(record.cursor);
            if (record.status.len > 0) self.alloc.free(record.status);
            if (record.failure_reason) |value| self.alloc.free(value);
        }

        pub fn freeRelationalIndexDropJobRecords(self: *DB, records: []RelationalIndexDropJobRecord) void {
            for (records) |record| self.freeRelationalIndexDropJobRecord(record);
            if (records.len > 0) self.alloc.free(records);
        }

        pub fn freeForeignKeyActionJobRecord(self: *DB, record: ForeignKeyActionJobRecord) void {
            if (record.job_id.len > 0) self.alloc.free(record.job_id);
            if (record.action.len > 0) self.alloc.free(record.action);
            if (record.worker_id.len > 0) self.alloc.free(record.worker_id);
            if (record.constraint_name.len > 0) self.alloc.free(record.constraint_name);
            if (record.parent_table.len > 0) self.alloc.free(record.parent_table);
            if (record.parent_key.len > 0) self.alloc.free(record.parent_key);
            if (record.updated_parent_key) |value| self.alloc.free(value);
            if (record.status.len > 0) self.alloc.free(record.status);
            if (record.next_child_table) |value| self.alloc.free(value);
            if (record.next_child_key) |value| self.alloc.free(value);
            if (record.last_error) |value| self.alloc.free(value);
        }

        pub fn freeForeignKeyActionJobRecords(self: *DB, records: []ForeignKeyActionJobRecord) void {
            for (records) |record| self.freeForeignKeyActionJobRecord(record);
            if (records.len > 0) self.alloc.free(records);
        }

        pub fn freeForeignKeyActionScheduleRecord(self: *DB, record: ForeignKeyActionScheduleRecord) void {
            if (record.schedule_id.len > 0) self.alloc.free(record.schedule_id);
            if (record.action_job_id.len > 0) self.alloc.free(record.action_job_id);
            if (record.action.len > 0) self.alloc.free(record.action);
            if (record.worker_id.len > 0) self.alloc.free(record.worker_id);
            if (record.constraint_name.len > 0) self.alloc.free(record.constraint_name);
            if (record.parent_table.len > 0) self.alloc.free(record.parent_table);
            if (record.parent_key.len > 0) self.alloc.free(record.parent_key);
            if (record.updated_parent_key) |value| self.alloc.free(value);
            if (record.status.len > 0) self.alloc.free(record.status);
            if (record.last_error) |value| self.alloc.free(value);
        }

        pub fn freeForeignKeyActionScheduleRecords(self: *DB, records: []ForeignKeyActionScheduleRecord) void {
            for (records) |record| self.freeForeignKeyActionScheduleRecord(record);
            if (records.len > 0) self.alloc.free(records);
        }

        pub fn freeUniqueConstraintIntegrityProgressRecord(self: *DB, record: UniqueConstraintIntegrityProgressRecord) void {
            if (record.mode.len > 0) self.alloc.free(record.mode);
            if (record.lower_doc_key.len > 0) self.alloc.free(record.lower_doc_key);
            if (record.upper_doc_key.len > 0) self.alloc.free(record.upper_doc_key);
        }

        pub fn freeUniqueConstraintIntegrityProgressRecords(self: *DB, records: []UniqueConstraintIntegrityProgressRecord) void {
            for (records) |record| self.freeUniqueConstraintIntegrityProgressRecord(record);
            if (records.len > 0) self.alloc.free(records);
        }

        pub fn listForeignKeyIntegrityProgressRecords(self: *DB) ![]ForeignKeyIntegrityProgressRecord {
            self.core.lockApply();
            defer self.core.unlockApply();

            const upper = try metadataPrefixUpperAlloc(self.alloc, foreign_key_integrity_progress_key_prefix);
            defer if (upper) |buf| self.alloc.free(buf);
            const scanned = try self.core.store.scanRange(self.alloc, foreign_key_integrity_progress_key_prefix, if (upper) |buf| buf else "");
            defer docstore_mod.DocStore.freeResults(self.alloc, scanned);

            var out = std.ArrayListUnmanaged(ForeignKeyIntegrityProgressRecord).empty;
            errdefer {
                for (out.items) |record| self.freeForeignKeyIntegrityProgressRecord(record);
                out.deinit(self.alloc);
            }
            for (scanned) |entry| {
                var parsed = try std.json.parseFromSlice(ForeignKeyIntegrityProgressRecord, self.alloc, entry.value, .{
                    .allocate = .alloc_always,
                    .ignore_unknown_fields = true,
                });
                defer parsed.deinit();

                var cloned = ForeignKeyIntegrityProgressRecord{
                    .version = parsed.value.version,
                    .phase = &.{},
                    .mode = &.{},
                    .constraint_name = null,
                    .lower_doc_key = &.{},
                    .upper_doc_key = &.{},
                    .completed = parsed.value.completed,
                    .valid = parsed.value.valid,
                    .updated_at_ns = parsed.value.updated_at_ns,
                    .report = parsed.value.report,
                };
                errdefer self.freeForeignKeyIntegrityProgressRecord(cloned);
                cloned.phase = try self.alloc.dupe(u8, parsed.value.phase);
                cloned.mode = try self.alloc.dupe(u8, parsed.value.mode);
                if (parsed.value.constraint_name) |value| cloned.constraint_name = try self.alloc.dupe(u8, value);
                cloned.lower_doc_key = try self.alloc.dupe(u8, parsed.value.lower_doc_key);
                cloned.upper_doc_key = try self.alloc.dupe(u8, parsed.value.upper_doc_key);
                try out.append(self.alloc, cloned);
            }
            return try out.toOwnedSlice(self.alloc);
        }

        pub fn listForeignKeyIntegrityClaimRecords(self: *DB) ![]ForeignKeyIntegrityClaimRecord {
            self.core.lockApply();
            defer self.core.unlockApply();

            const upper = try metadataPrefixUpperAlloc(self.alloc, foreign_key_integrity_claim_key_prefix);
            defer if (upper) |buf| self.alloc.free(buf);
            const scanned = try self.core.store.scanRange(self.alloc, foreign_key_integrity_claim_key_prefix, if (upper) |buf| buf else "");
            defer docstore_mod.DocStore.freeResults(self.alloc, scanned);

            var out = std.ArrayListUnmanaged(ForeignKeyIntegrityClaimRecord).empty;
            errdefer {
                for (out.items) |record| self.freeForeignKeyIntegrityClaimRecord(record);
                out.deinit(self.alloc);
            }
            for (scanned) |entry| {
                const cloned = try cloneForeignKeyIntegrityClaimRecordFromJson(self, entry.value);
                errdefer self.freeForeignKeyIntegrityClaimRecord(cloned);
                try out.append(self.alloc, cloned);
            }
            return try out.toOwnedSlice(self.alloc);
        }

        pub fn listForeignKeyIntegrityJobRecords(self: *DB) ![]ForeignKeyIntegrityJobRecord {
            self.core.lockApply();
            defer self.core.unlockApply();

            const upper = try metadataPrefixUpperAlloc(self.alloc, foreign_key_integrity_job_key_prefix);
            defer if (upper) |buf| self.alloc.free(buf);
            const scanned = try self.core.store.scanRange(self.alloc, foreign_key_integrity_job_key_prefix, if (upper) |buf| buf else "");
            defer docstore_mod.DocStore.freeResults(self.alloc, scanned);

            var out = std.ArrayListUnmanaged(ForeignKeyIntegrityJobRecord).empty;
            errdefer {
                for (out.items) |record| self.freeForeignKeyIntegrityJobRecord(record);
                out.deinit(self.alloc);
            }
            for (scanned) |entry| {
                const cloned = try cloneForeignKeyIntegrityJobRecordFromJson(self, entry.value);
                errdefer self.freeForeignKeyIntegrityJobRecord(cloned);
                try out.append(self.alloc, cloned);
            }
            return try out.toOwnedSlice(self.alloc);
        }

        pub fn listRelationalIndexRepairJobRecords(self: *DB) ![]RelationalIndexRepairJobRecord {
            self.core.lockApply();
            defer self.core.unlockApply();

            return try listRelationalIndexRepairJobRecordsLocked(self);
        }

        fn listRelationalIndexRepairJobRecordsLocked(self: *DB) ![]RelationalIndexRepairJobRecord {
            const upper = try metadataPrefixUpperAlloc(self.alloc, relational_index_repair_job_key_prefix);
            defer if (upper) |buf| self.alloc.free(buf);
            const scanned = try self.core.store.scanRange(self.alloc, relational_index_repair_job_key_prefix, if (upper) |buf| buf else "");
            defer docstore_mod.DocStore.freeResults(self.alloc, scanned);

            var out = std.ArrayListUnmanaged(RelationalIndexRepairJobRecord).empty;
            errdefer {
                for (out.items) |record| self.freeRelationalIndexRepairJobRecord(record);
                out.deinit(self.alloc);
            }
            for (scanned) |entry| {
                const cloned = try cloneRelationalIndexRepairJobRecordFromJson(self, entry.value);
                errdefer self.freeRelationalIndexRepairJobRecord(cloned);
                try out.append(self.alloc, cloned);
            }
            return try out.toOwnedSlice(self.alloc);
        }

        pub fn listRelationalIndexDropJobRecords(self: *DB) ![]RelationalIndexDropJobRecord {
            self.core.lockApply();
            defer self.core.unlockApply();

            const upper = try metadataPrefixUpperAlloc(self.alloc, relational_index_drop_job_key_prefix);
            defer if (upper) |buf| self.alloc.free(buf);
            const scanned = try self.core.store.scanRange(self.alloc, relational_index_drop_job_key_prefix, if (upper) |buf| buf else "");
            defer docstore_mod.DocStore.freeResults(self.alloc, scanned);

            var out = std.ArrayListUnmanaged(RelationalIndexDropJobRecord).empty;
            errdefer {
                for (out.items) |record| self.freeRelationalIndexDropJobRecord(record);
                out.deinit(self.alloc);
            }
            for (scanned) |entry| {
                const cloned = try cloneRelationalIndexDropJobRecordFromJson(self, entry.value);
                errdefer self.freeRelationalIndexDropJobRecord(cloned);
                try out.append(self.alloc, cloned);
            }
            return try out.toOwnedSlice(self.alloc);
        }

        pub fn loadForeignKeyActionJobRecord(self: *DB, job_id: []const u8) !?ForeignKeyActionJobRecord {
            self.core.lockApply();
            defer self.core.unlockApply();

            const key = try foreignKeyActionJobKeyAlloc(self.alloc, job_id);
            defer self.alloc.free(key);
            const raw = self.core.store.get(self.alloc, key) catch |err| switch (err) {
                error.NotFound => return null,
                else => return err,
            };
            defer self.alloc.free(raw);
            return try cloneForeignKeyActionJobRecordFromJson(self, raw);
        }

        pub fn listForeignKeyActionJobRecords(self: *DB) ![]ForeignKeyActionJobRecord {
            self.core.lockApply();
            defer self.core.unlockApply();

            const upper = try metadataPrefixUpperAlloc(self.alloc, foreign_key_action_job_key_prefix);
            defer if (upper) |buf| self.alloc.free(buf);
            const scanned = try self.core.store.scanRange(self.alloc, foreign_key_action_job_key_prefix, if (upper) |buf| buf else "");
            defer docstore_mod.DocStore.freeResults(self.alloc, scanned);

            var out = std.ArrayListUnmanaged(ForeignKeyActionJobRecord).empty;
            errdefer {
                for (out.items) |record| self.freeForeignKeyActionJobRecord(record);
                out.deinit(self.alloc);
            }
            for (scanned) |entry| {
                const cloned = try cloneForeignKeyActionJobRecordFromJson(self, entry.value);
                errdefer self.freeForeignKeyActionJobRecord(cloned);
                try out.append(self.alloc, cloned);
            }
            return try out.toOwnedSlice(self.alloc);
        }

        pub fn loadForeignKeyActionScheduleRecord(self: *DB, schedule_id: []const u8) !?ForeignKeyActionScheduleRecord {
            self.core.lockApply();
            defer self.core.unlockApply();

            const key = try foreignKeyActionScheduleKeyAlloc(self.alloc, schedule_id);
            defer self.alloc.free(key);
            const raw = self.core.store.get(self.alloc, key) catch |err| switch (err) {
                error.NotFound => return null,
                else => return err,
            };
            defer self.alloc.free(raw);
            return try cloneForeignKeyActionScheduleRecordFromJson(self, raw);
        }

        pub fn listForeignKeyActionScheduleRecords(self: *DB) ![]ForeignKeyActionScheduleRecord {
            self.core.lockApply();
            defer self.core.unlockApply();

            const upper = try metadataPrefixUpperAlloc(self.alloc, foreign_key_action_schedule_key_prefix);
            defer if (upper) |buf| self.alloc.free(buf);
            const scanned = try self.core.store.scanRange(self.alloc, foreign_key_action_schedule_key_prefix, if (upper) |buf| buf else "");
            defer docstore_mod.DocStore.freeResults(self.alloc, scanned);

            var out = std.ArrayListUnmanaged(ForeignKeyActionScheduleRecord).empty;
            errdefer {
                for (out.items) |record| self.freeForeignKeyActionScheduleRecord(record);
                out.deinit(self.alloc);
            }
            for (scanned) |entry| {
                const cloned = try cloneForeignKeyActionScheduleRecordFromJson(self, entry.value);
                errdefer self.freeForeignKeyActionScheduleRecord(cloned);
                try out.append(self.alloc, cloned);
            }
            return try out.toOwnedSlice(self.alloc);
        }

        pub fn listUniqueConstraintIntegrityProgressRecords(self: *DB) ![]UniqueConstraintIntegrityProgressRecord {
            self.core.lockApply();
            defer self.core.unlockApply();

            const upper = try metadataPrefixUpperAlloc(self.alloc, unique_constraint_integrity_progress_key_prefix);
            defer if (upper) |buf| self.alloc.free(buf);
            const scanned = try self.core.store.scanRange(self.alloc, unique_constraint_integrity_progress_key_prefix, if (upper) |buf| buf else "");
            defer docstore_mod.DocStore.freeResults(self.alloc, scanned);

            var out = std.ArrayListUnmanaged(UniqueConstraintIntegrityProgressRecord).empty;
            errdefer {
                for (out.items) |record| self.freeUniqueConstraintIntegrityProgressRecord(record);
                out.deinit(self.alloc);
            }
            for (scanned) |entry| {
                var parsed = try std.json.parseFromSlice(UniqueConstraintIntegrityProgressRecord, self.alloc, entry.value, .{
                    .allocate = .alloc_always,
                    .ignore_unknown_fields = true,
                });
                defer parsed.deinit();

                var cloned = UniqueConstraintIntegrityProgressRecord{
                    .version = parsed.value.version,
                    .mode = &.{},
                    .lower_doc_key = &.{},
                    .upper_doc_key = &.{},
                    .completed = parsed.value.completed,
                    .valid = parsed.value.valid,
                    .updated_at_ns = parsed.value.updated_at_ns,
                    .report = parsed.value.report,
                };
                errdefer self.freeUniqueConstraintIntegrityProgressRecord(cloned);
                cloned.mode = try self.alloc.dupe(u8, parsed.value.mode);
                cloned.lower_doc_key = try self.alloc.dupe(u8, parsed.value.lower_doc_key);
                cloned.upper_doc_key = try self.alloc.dupe(u8, parsed.value.upper_doc_key);
                try out.append(self.alloc, cloned);
            }
            return try out.toOwnedSlice(self.alloc);
        }

        pub fn loadForeignKeyIntegrityProgressRecord(
            self: *DB,
            mode: relational_store_mod.ForeignKeyIntegrityMode,
            constraint_name: ?[]const u8,
            lower_doc_key: []const u8,
            upper_doc_key: []const u8,
        ) !?ForeignKeyIntegrityProgressRecord {
            return try self.loadForeignKeyIntegrityProgressRecordForPhase("child_range", mode, constraint_name, lower_doc_key, upper_doc_key);
        }

        pub fn loadForeignKeyIntegrityProgressRecordForPhase(
            self: *DB,
            phase: []const u8,
            mode: relational_store_mod.ForeignKeyIntegrityMode,
            constraint_name: ?[]const u8,
            lower_doc_key: []const u8,
            upper_doc_key: []const u8,
        ) !?ForeignKeyIntegrityProgressRecord {
            self.core.lockApply();
            defer self.core.unlockApply();

            const key = try foreignKeyIntegrityProgressKeyAlloc(self.alloc, phase, mode, constraint_name, lower_doc_key, upper_doc_key);
            defer self.alloc.free(key);
            const raw = self.core.store.get(self.alloc, key) catch |err| switch (err) {
                error.NotFound => return null,
                else => return err,
            };
            defer self.alloc.free(raw);

            var parsed = try std.json.parseFromSlice(ForeignKeyIntegrityProgressRecord, self.alloc, raw, .{
                .allocate = .alloc_always,
                .ignore_unknown_fields = true,
            });
            defer parsed.deinit();

            var cloned = ForeignKeyIntegrityProgressRecord{
                .version = parsed.value.version,
                .phase = &.{},
                .mode = &.{},
                .constraint_name = null,
                .lower_doc_key = &.{},
                .upper_doc_key = &.{},
                .completed = parsed.value.completed,
                .valid = parsed.value.valid,
                .updated_at_ns = parsed.value.updated_at_ns,
                .report = parsed.value.report,
            };
            errdefer self.freeForeignKeyIntegrityProgressRecord(cloned);
            cloned.phase = try self.alloc.dupe(u8, parsed.value.phase);
            cloned.mode = try self.alloc.dupe(u8, parsed.value.mode);
            if (parsed.value.constraint_name) |value| cloned.constraint_name = try self.alloc.dupe(u8, value);
            cloned.lower_doc_key = try self.alloc.dupe(u8, parsed.value.lower_doc_key);
            cloned.upper_doc_key = try self.alloc.dupe(u8, parsed.value.upper_doc_key);
            return cloned;
        }

        pub fn loadForeignKeyIntegrityClaimRecord(
            self: *DB,
            claim_key: []const u8,
        ) !?ForeignKeyIntegrityClaimRecord {
            self.core.lockApply();
            defer self.core.unlockApply();

            const key = try foreignKeyIntegrityClaimKeyAlloc(self.alloc, claim_key);
            defer self.alloc.free(key);
            const raw = self.core.store.get(self.alloc, key) catch |err| switch (err) {
                error.NotFound => return null,
                else => return err,
            };
            defer self.alloc.free(raw);
            return try cloneForeignKeyIntegrityClaimRecordFromJson(self, raw);
        }

        pub fn loadForeignKeyIntegrityJobRecord(
            self: *DB,
            job_id: []const u8,
        ) !?ForeignKeyIntegrityJobRecord {
            self.core.lockApply();
            defer self.core.unlockApply();

            const key = try foreignKeyIntegrityJobKeyAlloc(self.alloc, job_id);
            defer self.alloc.free(key);
            const raw = self.core.store.get(self.alloc, key) catch |err| switch (err) {
                error.NotFound => return null,
                else => return err,
            };
            defer self.alloc.free(raw);
            return try cloneForeignKeyIntegrityJobRecordFromJson(self, raw);
        }

        pub fn loadRelationalIndexRepairJobRecord(
            self: *DB,
            job_id: []const u8,
        ) !?RelationalIndexRepairJobRecord {
            self.core.lockApply();
            defer self.core.unlockApply();

            const key = try relationalIndexRepairJobKeyAlloc(self.alloc, job_id);
            defer self.alloc.free(key);
            const raw = self.core.store.get(self.alloc, key) catch |err| switch (err) {
                error.NotFound => return null,
                else => return err,
            };
            defer self.alloc.free(raw);
            return try cloneRelationalIndexRepairJobRecordFromJson(self, raw);
        }

        pub fn loadRelationalIndexDropJobRecord(
            self: *DB,
            job_id: []const u8,
        ) !?RelationalIndexDropJobRecord {
            self.core.lockApply();
            defer self.core.unlockApply();

            const key = try relationalIndexDropJobKeyAlloc(self.alloc, job_id);
            defer self.alloc.free(key);
            const raw = self.core.store.get(self.alloc, key) catch |err| switch (err) {
                error.NotFound => return null,
                else => return err,
            };
            defer self.alloc.free(raw);
            return try cloneRelationalIndexDropJobRecordFromJson(self, raw);
        }

        pub fn loadUniqueConstraintIntegrityProgressRecord(
            self: *DB,
            mode: relational_store_mod.ForeignKeyIntegrityMode,
            lower_doc_key: []const u8,
            upper_doc_key: []const u8,
        ) !?UniqueConstraintIntegrityProgressRecord {
            self.core.lockApply();
            defer self.core.unlockApply();

            const key = try uniqueConstraintIntegrityProgressKeyAlloc(self.alloc, mode, lower_doc_key, upper_doc_key);
            defer self.alloc.free(key);
            const raw = self.core.store.get(self.alloc, key) catch |err| switch (err) {
                error.NotFound => return null,
                else => return err,
            };
            defer self.alloc.free(raw);

            var parsed = try std.json.parseFromSlice(UniqueConstraintIntegrityProgressRecord, self.alloc, raw, .{
                .allocate = .alloc_always,
                .ignore_unknown_fields = true,
            });
            defer parsed.deinit();

            var cloned = UniqueConstraintIntegrityProgressRecord{
                .version = parsed.value.version,
                .mode = &.{},
                .lower_doc_key = &.{},
                .upper_doc_key = &.{},
                .completed = parsed.value.completed,
                .valid = parsed.value.valid,
                .updated_at_ns = parsed.value.updated_at_ns,
                .report = parsed.value.report,
            };
            errdefer self.freeUniqueConstraintIntegrityProgressRecord(cloned);
            cloned.mode = try self.alloc.dupe(u8, parsed.value.mode);
            cloned.lower_doc_key = try self.alloc.dupe(u8, parsed.value.lower_doc_key);
            cloned.upper_doc_key = try self.alloc.dupe(u8, parsed.value.upper_doc_key);
            return cloned;
        }

        pub fn recordForeignKeyIntegrityProgressLocked(
            self: *DB,
            alloc: Allocator,
            mode: relational_store_mod.ForeignKeyIntegrityMode,
            constraint_name: ?[]const u8,
            lower_doc_key: []const u8,
            upper_doc_key: []const u8,
            report: relational_store_mod.ForeignKeyIntegrityReport,
        ) !void {
            return try recordForeignKeyIntegrityProgressForPhaseLocked(self, alloc, "child_range", mode, constraint_name, lower_doc_key, upper_doc_key, report);
        }

        pub fn recordForeignKeyIntegrityProgressForPhaseLocked(
            self: *DB,
            alloc: Allocator,
            phase: []const u8,
            mode: relational_store_mod.ForeignKeyIntegrityMode,
            constraint_name: ?[]const u8,
            lower_doc_key: []const u8,
            upper_doc_key: []const u8,
            report: relational_store_mod.ForeignKeyIntegrityReport,
        ) !void {
            self.relationalIntegrityRecordForeignKeyIntegrityReport(mode, report);
            if (shouldSkipIntegrityProgressWrite(self)) return;
            const key = try foreignKeyIntegrityProgressKeyAlloc(alloc, phase, mode, constraint_name, lower_doc_key, upper_doc_key);
            defer alloc.free(key);
            const payload = try std.json.Stringify.valueAlloc(alloc, ForeignKeyIntegrityProgressRecord{
                .phase = phase,
                .mode = foreignKeyIntegrityModeName(mode),
                .constraint_name = constraint_name,
                .lower_doc_key = lower_doc_key,
                .upper_doc_key = upper_doc_key,
                .valid = foreignKeyIntegrityProgressValid(mode, report),
                .updated_at_ns = currentTimeNs(),
                .report = report,
            }, .{ .emit_null_optional_fields = false });
            defer alloc.free(payload);
            try self.core.store.put(key, payload);
        }

        pub fn recordUniqueConstraintIntegrityProgressLocked(
            self: *DB,
            alloc: Allocator,
            mode: relational_store_mod.ForeignKeyIntegrityMode,
            lower_doc_key: []const u8,
            upper_doc_key: []const u8,
            report: relational_store_mod.UniqueConstraintIntegrityReport,
        ) !void {
            if (shouldSkipIntegrityProgressWrite(self)) return;
            const key = try uniqueConstraintIntegrityProgressKeyAlloc(alloc, mode, lower_doc_key, upper_doc_key);
            defer alloc.free(key);
            const payload = try std.json.Stringify.valueAlloc(alloc, UniqueConstraintIntegrityProgressRecord{
                .mode = foreignKeyIntegrityModeName(mode),
                .lower_doc_key = lower_doc_key,
                .upper_doc_key = upper_doc_key,
                .valid = uniqueConstraintIntegrityProgressValid(mode, report),
                .updated_at_ns = currentTimeNs(),
                .report = report,
            }, .{});
            defer alloc.free(payload);
            try self.core.store.put(key, payload);
        }

        pub fn claimForeignKeyIntegrityWorkUnit(
            self: *DB,
            claim_key: []const u8,
            worker_id: []const u8,
            group_id: u64,
            phase: []const u8,
            planned_action: []const u8,
            constraint_name: ?[]const u8,
            lower_doc_key: []const u8,
            upper_doc_key: []const u8,
            lease_ms: u64,
        ) !ForeignKeyIntegrityClaimRecord {
            return try self.claimForeignKeyIntegrityWorkUnitAt(
                claim_key,
                worker_id,
                group_id,
                phase,
                planned_action,
                constraint_name,
                lower_doc_key,
                upper_doc_key,
                lease_ms,
                currentTimeNs(),
            );
        }

        pub fn claimForeignKeyIntegrityWorkUnitAt(
            self: *DB,
            claim_key: []const u8,
            worker_id: []const u8,
            group_id: u64,
            phase: []const u8,
            planned_action: []const u8,
            constraint_name: ?[]const u8,
            lower_doc_key: []const u8,
            upper_doc_key: []const u8,
            lease_ms: u64,
            now_ns: u64,
        ) !ForeignKeyIntegrityClaimRecord {
            if (claim_key.len == 0 or worker_id.len == 0 or phase.len == 0 or planned_action.len == 0) return error.InvalidForeignKeyIntegrityClaim;
            self.core.lockApply();
            defer self.core.unlockApply();

            const key = try foreignKeyIntegrityClaimKeyAlloc(self.alloc, claim_key);
            defer self.alloc.free(key);

            var attempts: u32 = 1;
            if (self.core.store.get(self.alloc, key)) |raw| {
                defer self.alloc.free(raw);
                const existing = try cloneForeignKeyIntegrityClaimRecordFromJson(self, raw);
                defer self.freeForeignKeyIntegrityClaimRecord(existing);
                const held_by_other = !std.mem.eql(u8, existing.worker_id, worker_id);
                if (held_by_other and existing.lease_until_ns > now_ns) return error.ForeignKeyIntegrityClaimBusy;
                attempts = existing.attempts +| 1;
            } else |err| switch (err) {
                error.NotFound => {},
                else => return err,
            }

            const lease_ns = std.math.mul(u64, lease_ms, std.time.ns_per_ms) catch std.math.maxInt(u64);
            const lease_until_ns = now_ns +| lease_ns;
            const record = ForeignKeyIntegrityClaimRecord{
                .claim_key = claim_key,
                .worker_id = worker_id,
                .group_id = group_id,
                .phase = phase,
                .planned_action = planned_action,
                .constraint_name = constraint_name,
                .lower_doc_key = lower_doc_key,
                .upper_doc_key = upper_doc_key,
                .claimed_at_ns = now_ns,
                .lease_until_ns = lease_until_ns,
                .attempts = attempts,
            };
            const payload = try std.json.Stringify.valueAlloc(self.alloc, record, .{ .emit_null_optional_fields = false });
            defer self.alloc.free(payload);
            try self.core.store.put(key, payload);
            return try cloneForeignKeyIntegrityClaimRecordFromJson(self, payload);
        }

        pub fn upsertForeignKeyIntegrityJobRecord(
            self: *DB,
            job_id: []const u8,
            table_name: []const u8,
            action: []const u8,
            worker_id: []const u8,
            constraint_name: ?[]const u8,
            lower_doc_key: []const u8,
            upper_doc_key: []const u8,
            lease_ms: u64,
            max_work_units: usize,
            status: []const u8,
        ) !ForeignKeyIntegrityJobRecord {
            return try self.upsertForeignKeyIntegrityJobRecordAt(
                job_id,
                table_name,
                action,
                worker_id,
                constraint_name,
                lower_doc_key,
                upper_doc_key,
                lease_ms,
                max_work_units,
                status,
                currentTimeNs(),
            );
        }

        pub fn upsertForeignKeyIntegrityJobRecordAt(
            self: *DB,
            job_id: []const u8,
            table_name: []const u8,
            action: []const u8,
            worker_id: []const u8,
            constraint_name: ?[]const u8,
            lower_doc_key: []const u8,
            upper_doc_key: []const u8,
            lease_ms: u64,
            max_work_units: usize,
            status: []const u8,
            now_ns: u64,
        ) !ForeignKeyIntegrityJobRecord {
            if (job_id.len == 0 or table_name.len == 0 or action.len == 0 or worker_id.len == 0 or status.len == 0) return error.InvalidForeignKeyIntegrityJob;
            self.core.lockApply();
            defer self.core.unlockApply();

            const key = try foreignKeyIntegrityJobKeyAlloc(self.alloc, job_id);
            defer self.alloc.free(key);

            var created_at_ns = now_ns;
            var attempts: u32 = 1;
            var violation_samples_json: []const u8 = "[]";
            var preserved_violation_samples_json: ?[]u8 = null;
            defer if (preserved_violation_samples_json) |value| self.alloc.free(value);
            var violation_sample_count: usize = 0;
            var violations_truncated = false;
            var last_report: relational_store_mod.ForeignKeyIntegrityReport = .{};
            var aggregate_report: relational_store_mod.ForeignKeyIntegrityReport = .{};
            var diagnostic_passes: u64 = 0;
            var violating_passes: u64 = 0;
            var first_violation_at_ns: ?u64 = null;
            var last_violation_at_ns: ?u64 = null;
            if (self.core.store.get(self.alloc, key)) |raw| {
                defer self.alloc.free(raw);
                const existing = try cloneForeignKeyIntegrityJobRecordFromJson(self, raw);
                defer self.freeForeignKeyIntegrityJobRecord(existing);
                created_at_ns = existing.created_at_ns;
                attempts = existing.attempts +| 1;
                preserved_violation_samples_json = try self.alloc.dupe(u8, existing.violation_samples_json);
                violation_samples_json = preserved_violation_samples_json.?;
                violation_sample_count = existing.violation_sample_count;
                violations_truncated = existing.violations_truncated;
                last_report = existing.last_report;
                aggregate_report = existing.aggregate_report;
                diagnostic_passes = existing.diagnostic_passes;
                violating_passes = existing.violating_passes;
                first_violation_at_ns = existing.first_violation_at_ns;
                last_violation_at_ns = existing.last_violation_at_ns;
            } else |err| switch (err) {
                error.NotFound => {},
                else => return err,
            }

            const record = ForeignKeyIntegrityJobRecord{
                .job_id = job_id,
                .table_name = table_name,
                .action = action,
                .worker_id = worker_id,
                .constraint_name = constraint_name,
                .lower_doc_key = lower_doc_key,
                .upper_doc_key = upper_doc_key,
                .lease_ms = lease_ms,
                .max_work_units = max_work_units,
                .status = status,
                .created_at_ns = created_at_ns,
                .updated_at_ns = now_ns,
                .attempts = attempts,
                .completed = false,
                .valid = null,
                .last_report = last_report,
                .aggregate_report = aggregate_report,
                .violation_samples_json = violation_samples_json,
                .violation_sample_count = violation_sample_count,
                .violations_truncated = violations_truncated,
                .diagnostic_passes = diagnostic_passes,
                .violating_passes = violating_passes,
                .first_violation_at_ns = first_violation_at_ns,
                .last_violation_at_ns = last_violation_at_ns,
            };
            const payload = try std.json.Stringify.valueAlloc(self.alloc, record, .{ .emit_null_optional_fields = false });
            defer self.alloc.free(payload);
            try self.core.store.put(key, payload);
            return try cloneForeignKeyIntegrityJobRecordFromJson(self, payload);
        }

        pub fn upsertRelationalIndexRepairJobRecord(
            self: *DB,
            job_id: []const u8,
            database_name: []const u8,
            namespace_name: []const u8,
            table_name: []const u8,
            worker_id: []const u8,
            lower_doc_key: []const u8,
            upper_doc_key: []const u8,
            lease_ms: u64,
            max_work_units: usize,
            status: []const u8,
        ) !RelationalIndexRepairJobRecord {
            return try self.upsertRelationalIndexRepairJobRecordAt(
                job_id,
                database_name,
                namespace_name,
                table_name,
                worker_id,
                lower_doc_key,
                upper_doc_key,
                lease_ms,
                max_work_units,
                status,
                currentTimeNs(),
            );
        }

        pub fn upsertRelationalIndexRepairJobRecordAt(
            self: *DB,
            job_id: []const u8,
            database_name: []const u8,
            namespace_name: []const u8,
            table_name: []const u8,
            worker_id: []const u8,
            lower_doc_key: []const u8,
            upper_doc_key: []const u8,
            lease_ms: u64,
            max_work_units: usize,
            status: []const u8,
            now_ns: u64,
        ) !RelationalIndexRepairJobRecord {
            if (job_id.len == 0 or database_name.len == 0 or namespace_name.len == 0 or table_name.len == 0 or worker_id.len == 0 or lease_ms == 0 or max_work_units == 0 or status.len == 0) return error.InvalidRelationalIndexRepairJob;
            self.core.lockApply();
            defer self.core.unlockApply();

            const key = try relationalIndexRepairJobKeyAlloc(self.alloc, job_id);
            defer self.alloc.free(key);

            var created_at_ns = now_ns;
            var attempts: u32 = 1;
            var next_lower_doc_key: []const u8 = "";
            var preserved_next_lower_doc_key: ?[]u8 = null;
            defer if (preserved_next_lower_doc_key) |value| self.alloc.free(value);
            var cursor: []const u8 = "";
            var preserved_cursor: ?[]u8 = null;
            defer if (preserved_cursor) |value| self.alloc.free(value);
            var failure_reason: ?[]const u8 = null;
            var preserved_failure_reason: ?[]u8 = null;
            defer if (preserved_failure_reason) |value| self.alloc.free(value);
            var stale_generation = false;
            var pass_count: u64 = 0;
            var total_units_queued: u64 = 0;
            var total_units_running: u64 = 0;
            var total_units_throttled: u64 = 0;
            var total_units_completed: u64 = 0;
            var last_ranges_scanned: u64 = 0;
            var last_ranges_repaired: u64 = 0;
            var last_ranges_missing: u64 = 0;
            var total_ranges_scanned: u64 = 0;
            var total_ranges_repaired: u64 = 0;
            var total_ranges_missing: u64 = 0;
            var last_report: relational_store_mod.ColumnBackedIndexRepairReport = .{};
            var aggregate_report: relational_store_mod.ColumnBackedIndexRepairReport = .{};
            var last_error: ?[]const u8 = null;
            var preserved_last_error: ?[]u8 = null;
            defer if (preserved_last_error) |value| self.alloc.free(value);
            if (self.core.store.get(self.alloc, key)) |raw| {
                defer self.alloc.free(raw);
                const existing = try cloneRelationalIndexRepairJobRecordFromJson(self, raw);
                defer self.freeRelationalIndexRepairJobRecord(existing);
                created_at_ns = existing.created_at_ns;
                attempts = existing.attempts +| 1;
                preserved_next_lower_doc_key = try self.alloc.dupe(u8, existing.next_lower_doc_key);
                next_lower_doc_key = preserved_next_lower_doc_key.?;
                preserved_cursor = try self.alloc.dupe(u8, existing.cursor);
                cursor = preserved_cursor.?;
                if (existing.failure_reason) |value| {
                    preserved_failure_reason = try self.alloc.dupe(u8, value);
                    failure_reason = preserved_failure_reason.?;
                }
                stale_generation = existing.stale_generation;
                pass_count = existing.pass_count;
                total_units_queued = existing.total_units_queued;
                total_units_running = existing.total_units_running;
                total_units_throttled = existing.total_units_throttled;
                total_units_completed = existing.total_units_completed;
                last_ranges_scanned = existing.last_ranges_scanned;
                last_ranges_repaired = existing.last_ranges_repaired;
                last_ranges_missing = existing.last_ranges_missing;
                total_ranges_scanned = existing.total_ranges_scanned;
                total_ranges_repaired = existing.total_ranges_repaired;
                total_ranges_missing = existing.total_ranges_missing;
                last_report = existing.last_report;
                aggregate_report = existing.aggregate_report;
                if (existing.last_error) |value| {
                    preserved_last_error = try self.alloc.dupe(u8, value);
                    last_error = preserved_last_error.?;
                }
            } else |err| switch (err) {
                error.NotFound => {},
                else => return err,
            }

            const record = RelationalIndexRepairJobRecord{
                .job_id = job_id,
                .database_name = database_name,
                .namespace_name = namespace_name,
                .table_name = table_name,
                .access_method = "ordered_tuple",
                .index_name = "",
                .generation = 0,
                .worker_id = worker_id,
                .lower_doc_key = lower_doc_key,
                .upper_doc_key = upper_doc_key,
                .lease_ms = lease_ms,
                .max_work_units = max_work_units,
                .status = status,
                .created_at_ns = created_at_ns,
                .updated_at_ns = now_ns,
                .attempts = attempts,
                .completed = false,
                .complete = null,
                .next_lower_doc_key = next_lower_doc_key,
                .cursor = cursor,
                .failure_reason = failure_reason,
                .stale_generation = stale_generation,
                .pass_count = pass_count,
                .total_units_queued = total_units_queued,
                .total_units_running = total_units_running,
                .total_units_throttled = total_units_throttled,
                .total_units_completed = total_units_completed,
                .last_ranges_scanned = last_ranges_scanned,
                .last_ranges_repaired = last_ranges_repaired,
                .last_ranges_missing = last_ranges_missing,
                .total_ranges_scanned = total_ranges_scanned,
                .total_ranges_repaired = total_ranges_repaired,
                .total_ranges_missing = total_ranges_missing,
                .last_report = last_report,
                .aggregate_report = aggregate_report,
                .last_error = last_error,
            };
            const payload = try std.json.Stringify.valueAlloc(self.alloc, record, .{ .emit_null_optional_fields = false });
            defer self.alloc.free(payload);
            try self.core.store.put(key, payload);
            return try cloneRelationalIndexRepairJobRecordFromJson(self, payload);
        }

        pub fn recordRelationalIndexRepairJobPass(
            self: *DB,
            job_id: []const u8,
            status: []const u8,
            complete: bool,
            ranges_scanned: u64,
            ranges_repaired: u64,
            ranges_missing: u64,
            next_lower_doc_key: []const u8,
            report: relational_store_mod.ColumnBackedIndexRepairReport,
            last_error: ?[]const u8,
        ) !RelationalIndexRepairJobRecord {
            return try self.recordRelationalIndexRepairJobPassAt(
                job_id,
                status,
                complete,
                ranges_scanned,
                ranges_repaired,
                ranges_missing,
                next_lower_doc_key,
                report,
                last_error,
                currentTimeNs(),
            );
        }

        pub fn recordRelationalIndexRepairJobPassAt(
            self: *DB,
            job_id: []const u8,
            status: []const u8,
            complete: bool,
            ranges_scanned: u64,
            ranges_repaired: u64,
            ranges_missing: u64,
            next_lower_doc_key: []const u8,
            report: relational_store_mod.ColumnBackedIndexRepairReport,
            last_error: ?[]const u8,
            now_ns: u64,
        ) !RelationalIndexRepairJobRecord {
            if (job_id.len == 0 or status.len == 0) return error.InvalidRelationalIndexRepairJob;
            self.core.lockApply();
            defer self.core.unlockApply();

            const key = try relationalIndexRepairJobKeyAlloc(self.alloc, job_id);
            defer self.alloc.free(key);
            const raw = self.core.store.get(self.alloc, key) catch |err| switch (err) {
                error.NotFound => return error.RelationalIndexRepairJobNotFound,
                else => return err,
            };
            defer self.alloc.free(raw);
            const existing = try cloneRelationalIndexRepairJobRecordFromJson(self, raw);
            defer self.freeRelationalIndexRepairJobRecord(existing);

            const record = RelationalIndexRepairJobRecord{
                .job_id = existing.job_id,
                .database_name = existing.database_name,
                .namespace_name = existing.namespace_name,
                .table_name = existing.table_name,
                .access_method = existing.access_method,
                .index_name = existing.index_name,
                .generation = existing.generation,
                .worker_id = existing.worker_id,
                .lower_doc_key = existing.lower_doc_key,
                .upper_doc_key = existing.upper_doc_key,
                .lease_ms = existing.lease_ms,
                .max_work_units = existing.max_work_units,
                .status = status,
                .created_at_ns = existing.created_at_ns,
                .updated_at_ns = now_ns,
                .attempts = existing.attempts,
                .completed = complete,
                .complete = complete,
                .next_lower_doc_key = next_lower_doc_key,
                .cursor = next_lower_doc_key,
                .failure_reason = if (last_error) |value| value else existing.failure_reason,
                .stale_generation = existing.stale_generation,
                .pass_count = existing.pass_count +| 1,
                .last_units_completed = ranges_scanned,
                .total_units_queued = existing.total_units_queued,
                .total_units_running = existing.total_units_running,
                .total_units_throttled = existing.total_units_throttled,
                .total_units_completed = existing.total_units_completed +| ranges_scanned,
                .last_ranges_scanned = ranges_scanned,
                .last_ranges_repaired = ranges_repaired,
                .last_ranges_missing = ranges_missing,
                .total_ranges_scanned = existing.total_ranges_scanned +| ranges_scanned,
                .total_ranges_repaired = existing.total_ranges_repaired +| ranges_repaired,
                .total_ranges_missing = existing.total_ranges_missing +| ranges_missing,
                .last_report = report,
                .aggregate_report = relationalIndexRepairReportAdd(existing.aggregate_report, report),
                .last_error = last_error,
            };
            const payload = try std.json.Stringify.valueAlloc(self.alloc, record, .{ .emit_null_optional_fields = false });
            defer self.alloc.free(payload);
            try self.core.store.put(key, payload);
            return try cloneRelationalIndexRepairJobRecordFromJson(self, payload);
        }

        pub fn upsertRelationalIndexRepairJobTargetAt(
            self: *DB,
            job_id: []const u8,
            database_name: []const u8,
            namespace_name: []const u8,
            table_name: []const u8,
            access_method: []const u8,
            index_name: []const u8,
            generation: u64,
            worker_id: []const u8,
            cursor: []const u8,
            lease_ms: u64,
            max_work_units: usize,
            status: []const u8,
            now_ns: u64,
        ) !RelationalIndexRepairJobRecord {
            if (job_id.len == 0 or database_name.len == 0 or namespace_name.len == 0 or table_name.len == 0 or access_method.len == 0 or index_name.len == 0 or generation == 0 or worker_id.len == 0 or lease_ms == 0 or status.len == 0) return error.InvalidRelationalIndexRepairJob;
            self.core.lockApply();
            defer self.core.unlockApply();

            const key = try relationalIndexRepairJobKeyAlloc(self.alloc, job_id);
            defer self.alloc.free(key);

            var created_at_ns = now_ns;
            var attempts: u32 = 1;
            var preserved_cursor: ?[]u8 = null;
            defer if (preserved_cursor) |value| self.alloc.free(value);
            var previous_cursor: []const u8 = cursor;
            var failure_reason: ?[]const u8 = null;
            var preserved_failure_reason: ?[]u8 = null;
            defer if (preserved_failure_reason) |value| self.alloc.free(value);
            var stale_generation = false;
            var pass_count: u64 = 0;
            var total_units_queued: u64 = 0;
            var total_units_running: u64 = 0;
            var total_units_throttled: u64 = 0;
            var total_units_completed: u64 = 0;
            if (self.core.store.get(self.alloc, key)) |raw| {
                defer self.alloc.free(raw);
                const existing = try cloneRelationalIndexRepairJobRecordFromJson(self, raw);
                defer self.freeRelationalIndexRepairJobRecord(existing);
                if (existing.generation != 0 and existing.generation != generation) return error.StaleRelationalIndexRepairJobGeneration;
                created_at_ns = existing.created_at_ns;
                attempts = existing.attempts +| 1;
                if (cursor.len == 0) {
                    preserved_cursor = try self.alloc.dupe(u8, existing.cursor);
                    previous_cursor = preserved_cursor.?;
                }
                if (existing.failure_reason) |value| {
                    preserved_failure_reason = try self.alloc.dupe(u8, value);
                    failure_reason = preserved_failure_reason.?;
                }
                stale_generation = existing.stale_generation;
                pass_count = existing.pass_count;
                total_units_queued = existing.total_units_queued;
                total_units_running = existing.total_units_running;
                total_units_throttled = existing.total_units_throttled;
                total_units_completed = existing.total_units_completed;
            } else |err| switch (err) {
                error.NotFound => {},
                else => return err,
            }

            const record = RelationalIndexRepairJobRecord{
                .job_id = job_id,
                .database_name = database_name,
                .namespace_name = namespace_name,
                .table_name = table_name,
                .access_method = access_method,
                .index_name = index_name,
                .generation = generation,
                .worker_id = worker_id,
                .lease_ms = lease_ms,
                .max_work_units = max_work_units,
                .status = status,
                .created_at_ns = created_at_ns,
                .updated_at_ns = now_ns,
                .attempts = attempts,
                .cursor = previous_cursor,
                .failure_reason = failure_reason,
                .stale_generation = stale_generation,
                .pass_count = pass_count,
                .total_units_queued = total_units_queued,
                .total_units_running = total_units_running,
                .total_units_throttled = total_units_throttled,
                .total_units_completed = total_units_completed,
            };
            const payload = try std.json.Stringify.valueAlloc(self.alloc, record, .{ .emit_null_optional_fields = false });
            defer self.alloc.free(payload);
            try self.core.store.put(key, payload);
            return try cloneRelationalIndexRepairJobRecordFromJson(self, payload);
        }

        pub fn recordRelationalIndexRepairJobTargetPassAt(
            self: *DB,
            job_id: []const u8,
            status: []const u8,
            complete: bool,
            cursor: []const u8,
            units_queued: u64,
            units_running: u64,
            units_throttled: u64,
            units_completed: u64,
            failure_reason: ?[]const u8,
            stale_generation: bool,
            now_ns: u64,
        ) !RelationalIndexRepairJobRecord {
            if (job_id.len == 0 or status.len == 0) return error.InvalidRelationalIndexRepairJob;
            self.core.lockApply();
            defer self.core.unlockApply();

            const key = try relationalIndexRepairJobKeyAlloc(self.alloc, job_id);
            defer self.alloc.free(key);
            const raw = self.core.store.get(self.alloc, key) catch |err| switch (err) {
                error.NotFound => return error.RelationalIndexRepairJobNotFound,
                else => return err,
            };
            defer self.alloc.free(raw);
            const existing = try cloneRelationalIndexRepairJobRecordFromJson(self, raw);
            defer self.freeRelationalIndexRepairJobRecord(existing);

            const record = RelationalIndexRepairJobRecord{
                .job_id = existing.job_id,
                .database_name = existing.database_name,
                .namespace_name = existing.namespace_name,
                .table_name = existing.table_name,
                .access_method = existing.access_method,
                .index_name = existing.index_name,
                .generation = existing.generation,
                .worker_id = existing.worker_id,
                .lower_doc_key = existing.lower_doc_key,
                .upper_doc_key = existing.upper_doc_key,
                .lease_ms = existing.lease_ms,
                .max_work_units = existing.max_work_units,
                .status = status,
                .created_at_ns = existing.created_at_ns,
                .updated_at_ns = now_ns,
                .attempts = existing.attempts,
                .completed = complete,
                .complete = complete,
                .next_lower_doc_key = cursor,
                .cursor = cursor,
                .failure_reason = failure_reason,
                .stale_generation = stale_generation,
                .pass_count = existing.pass_count +| 1,
                .last_units_queued = units_queued,
                .last_units_running = units_running,
                .last_units_throttled = units_throttled,
                .last_units_completed = units_completed,
                .total_units_queued = existing.total_units_queued +| units_queued,
                .total_units_running = existing.total_units_running +| units_running,
                .total_units_throttled = existing.total_units_throttled +| units_throttled,
                .total_units_completed = existing.total_units_completed +| units_completed,
                .last_ranges_scanned = existing.last_ranges_scanned,
                .last_ranges_repaired = existing.last_ranges_repaired,
                .last_ranges_missing = existing.last_ranges_missing,
                .total_ranges_scanned = existing.total_ranges_scanned,
                .total_ranges_repaired = existing.total_ranges_repaired,
                .total_ranges_missing = existing.total_ranges_missing,
                .last_report = existing.last_report,
                .aggregate_report = existing.aggregate_report,
                .last_error = failure_reason,
            };
            const payload = try std.json.Stringify.valueAlloc(self.alloc, record, .{ .emit_null_optional_fields = false });
            defer self.alloc.free(payload);
            try self.core.store.put(key, payload);
            return try cloneRelationalIndexRepairJobRecordFromJson(self, payload);
        }

        pub fn runRelationalIndexRepairJobPageAt(
            self: *DB,
            job_id: []const u8,
            database_name: []const u8,
            namespace_name: []const u8,
            table_name: []const u8,
            access_method: []const u8,
            index_name: []const u8,
            generation: u64,
            worker_id: []const u8,
            lease_ms: u64,
            max_work_units: usize,
            now_ns: u64,
        ) !RelationalIndexRepairJobRecord {
            const started = try self.upsertRelationalIndexRepairJobTargetAt(job_id, database_name, namespace_name, table_name, access_method, index_name, generation, worker_id, "", lease_ms, max_work_units, "running", now_ns);
            defer self.freeRelationalIndexRepairJobRecord(started);

            const result = if (std.mem.eql(u8, access_method, "text_search"))
                self.core.index_manager.repairRelationalTextSearchFromRows(self.core.store, index_name, max_work_units)
            else if (std.mem.eql(u8, access_method, "algebraic_filter"))
                self.core.index_manager.repairRelationalAlgebraicFromRows(self.core.store, index_name, max_work_units)
            else
                error.UnsupportedRelationalIndexRepairAccessMethod;

            const page = result catch |err| {
                return try self.recordRelationalIndexRepairJobTargetPassAt(
                    job_id,
                    "failed",
                    false,
                    started.cursor,
                    1,
                    0,
                    0,
                    0,
                    @errorName(err),
                    false,
                    now_ns,
                );
            };

            return try self.recordRelationalIndexRepairJobTargetPassAt(
                job_id,
                if (page.complete) "complete" else "running",
                page.complete,
                page.cursor,
                page.units_queued,
                page.units_running,
                page.units_throttled,
                page.units_completed,
                page.failure_reason,
                page.stale_generation,
                now_ns,
            );
        }

        pub fn scheduleRelationalIndexRepairJobPageAt(
            self: *DB,
            job_id: []const u8,
            database_name: []const u8,
            namespace_name: []const u8,
            table_name: []const u8,
            access_method: []const u8,
            index_name: []const u8,
            generation: u64,
            worker_id: []const u8,
            lease_ms: u64,
            max_work_units: usize,
            now_ns: u64,
        ) !void {
            if (job_id.len == 0 or database_name.len == 0 or namespace_name.len == 0 or table_name.len == 0 or access_method.len == 0 or index_name.len == 0 or generation == 0 or worker_id.len == 0 or lease_ms == 0 or max_work_units == 0) return error.InvalidRelationalIndexRepairJob;

            const RepairPageJob = struct {
                alloc: Allocator,
                db: *DB,
                job_id: []u8,
                database_name: []u8,
                namespace_name: []u8,
                table_name: []u8,
                access_method: []u8,
                index_name: []u8,
                generation: u64,
                worker_id: []u8,
                lease_ms: u64,
                max_work_units: usize,
                now_ns: u64,

                fn init(
                    alloc: Allocator,
                    db: *DB,
                    job_id_arg: []const u8,
                    database_name_arg: []const u8,
                    namespace_name_arg: []const u8,
                    table_name_arg: []const u8,
                    access_method_arg: []const u8,
                    index_name_arg: []const u8,
                    generation_arg: u64,
                    worker_id_arg: []const u8,
                    lease_ms_arg: u64,
                    max_work_units_arg: usize,
                    now_ns_arg: u64,
                ) !*@This() {
                    const job = try alloc.create(@This());
                    errdefer alloc.destroy(job);
                    job.* = .{
                        .alloc = alloc,
                        .db = db,
                        .job_id = try alloc.dupe(u8, job_id_arg),
                        .database_name = &.{},
                        .namespace_name = &.{},
                        .table_name = &.{},
                        .access_method = &.{},
                        .index_name = &.{},
                        .generation = generation_arg,
                        .worker_id = &.{},
                        .lease_ms = lease_ms_arg,
                        .max_work_units = max_work_units_arg,
                        .now_ns = now_ns_arg,
                    };
                    errdefer alloc.free(job.job_id);
                    job.database_name = try alloc.dupe(u8, database_name_arg);
                    errdefer alloc.free(job.database_name);
                    job.namespace_name = try alloc.dupe(u8, namespace_name_arg);
                    errdefer alloc.free(job.namespace_name);
                    job.table_name = try alloc.dupe(u8, table_name_arg);
                    errdefer alloc.free(job.table_name);
                    job.access_method = try alloc.dupe(u8, access_method_arg);
                    errdefer alloc.free(job.access_method);
                    job.index_name = try alloc.dupe(u8, index_name_arg);
                    errdefer alloc.free(job.index_name);
                    job.worker_id = try alloc.dupe(u8, worker_id_arg);
                    return job;
                }

                fn run(ptr: *anyopaque) anyerror!void {
                    const job: *@This() = @ptrCast(@alignCast(ptr));
                    const record = try job.db.runRelationalIndexRepairJobPageAt(
                        job.job_id,
                        job.database_name,
                        job.namespace_name,
                        job.table_name,
                        job.access_method,
                        job.index_name,
                        job.generation,
                        job.worker_id,
                        job.lease_ms,
                        job.max_work_units,
                        job.now_ns,
                    );
                    job.db.freeRelationalIndexRepairJobRecord(record);
                }

                fn deinit(ptr: *anyopaque) void {
                    const job: *@This() = @ptrCast(@alignCast(ptr));
                    job.alloc.free(job.job_id);
                    job.alloc.free(job.database_name);
                    job.alloc.free(job.namespace_name);
                    job.alloc.free(job.table_name);
                    job.alloc.free(job.access_method);
                    job.alloc.free(job.index_name);
                    job.alloc.free(job.worker_id);
                    const alloc = job.alloc;
                    alloc.destroy(job);
                }
            };

            const job = try RepairPageJob.init(
                self.runtime_alloc,
                self,
                job_id,
                database_name,
                namespace_name,
                table_name,
                access_method,
                index_name,
                generation,
                worker_id,
                lease_ms,
                max_work_units,
                now_ns,
            );
            errdefer RepairPageJob.deinit(job);

            try self.backend_runtime.durable_jobs.submit(.{
                .owner_id = self.relational_index_worker_owner_id,
                .class = .maintenance,
                .ptr = job,
                .run = RepairPageJob.run,
                .deinit = RepairPageJob.deinit,
            });
        }

        pub fn snapshotRelationalIndexRepairStats(self: *DB) !types.RelationalIndexRepairStats {
            const records = try self.listRelationalIndexRepairJobRecords();
            defer self.freeRelationalIndexRepairJobRecords(records);

            return aggregateRelationalIndexRepairStats(records);
        }

        pub fn snapshotRelationalIndexRepairStatsAssumeApplyLockHeld(self: *DB) !types.RelationalIndexRepairStats {
            const records = try listRelationalIndexRepairJobRecordsLocked(self);
            defer self.freeRelationalIndexRepairJobRecords(records);

            return aggregateRelationalIndexRepairStats(records);
        }

        fn aggregateRelationalIndexRepairStats(records: []const RelationalIndexRepairJobRecord) types.RelationalIndexRepairStats {
            var stats = types.RelationalIndexRepairStats{};
            for (records) |record| {
                stats.job_count += 1;
                if (record.completed) {
                    stats.completed_job_count += 1;
                } else {
                    stats.active_job_count += 1;
                }
                if (std.mem.eql(u8, record.status, "failed")) stats.failed_job_count += 1;
                if (record.stale_generation) stats.stale_generation_job_count += 1;
                stats.last_units_queued +|= record.last_units_queued;
                stats.last_units_running +|= record.last_units_running;
                stats.last_units_throttled +|= record.last_units_throttled;
                stats.last_units_completed +|= record.last_units_completed;
                stats.total_units_queued +|= record.total_units_queued;
                stats.total_units_running +|= record.total_units_running;
                stats.total_units_throttled +|= record.total_units_throttled;
                stats.total_units_completed +|= record.total_units_completed;
            }
            return stats;
        }

        pub fn upsertRelationalIndexDropJobRecordAt(
            self: *DB,
            job_id: []const u8,
            database_name: []const u8,
            namespace_name: []const u8,
            table_name: []const u8,
            access_method: []const u8,
            index_name: []const u8,
            generation: u64,
            worker_id: []const u8,
            cursor: []const u8,
            lease_ms: u64,
            max_work_units: usize,
            status: []const u8,
            now_ns: u64,
        ) !RelationalIndexDropJobRecord {
            if (job_id.len == 0 or database_name.len == 0 or namespace_name.len == 0 or table_name.len == 0 or access_method.len == 0 or index_name.len == 0 or generation == 0 or worker_id.len == 0 or lease_ms == 0 or max_work_units == 0 or status.len == 0) return error.InvalidRelationalIndexDropJob;
            self.core.lockApply();
            defer self.core.unlockApply();

            const key = try relationalIndexDropJobKeyAlloc(self.alloc, job_id);
            defer self.alloc.free(key);

            var created_at_ns = now_ns;
            var attempts: u32 = 1;
            var preserved_cursor: ?[]u8 = null;
            defer if (preserved_cursor) |value| self.alloc.free(value);
            var previous_cursor: []const u8 = cursor;
            var failure_reason: ?[]const u8 = null;
            var preserved_failure_reason: ?[]u8 = null;
            defer if (preserved_failure_reason) |value| self.alloc.free(value);
            var stale_generation = false;
            var pass_count: u64 = 0;
            var total_units_queued: u64 = 0;
            var total_units_running: u64 = 0;
            var total_units_throttled: u64 = 0;
            var total_units_completed: u64 = 0;
            if (self.core.store.get(self.alloc, key)) |raw| {
                defer self.alloc.free(raw);
                const existing = try cloneRelationalIndexDropJobRecordFromJson(self, raw);
                defer self.freeRelationalIndexDropJobRecord(existing);
                if (existing.generation != generation) return error.StaleRelationalIndexDropJobGeneration;
                created_at_ns = existing.created_at_ns;
                attempts = existing.attempts +| 1;
                if (cursor.len == 0) {
                    preserved_cursor = try self.alloc.dupe(u8, existing.cursor);
                    previous_cursor = preserved_cursor.?;
                }
                if (existing.failure_reason) |value| {
                    preserved_failure_reason = try self.alloc.dupe(u8, value);
                    failure_reason = preserved_failure_reason.?;
                }
                stale_generation = existing.stale_generation;
                pass_count = existing.pass_count;
                total_units_queued = existing.total_units_queued;
                total_units_running = existing.total_units_running;
                total_units_throttled = existing.total_units_throttled;
                total_units_completed = existing.total_units_completed;
            } else |err| switch (err) {
                error.NotFound => {},
                else => return err,
            }

            const record = RelationalIndexDropJobRecord{
                .job_id = job_id,
                .database_name = database_name,
                .namespace_name = namespace_name,
                .table_name = table_name,
                .access_method = access_method,
                .index_name = index_name,
                .generation = generation,
                .worker_id = worker_id,
                .cursor = previous_cursor,
                .lease_ms = lease_ms,
                .max_work_units = max_work_units,
                .status = status,
                .created_at_ns = created_at_ns,
                .updated_at_ns = now_ns,
                .attempts = attempts,
                .failure_reason = failure_reason,
                .stale_generation = stale_generation,
                .pass_count = pass_count,
                .total_units_queued = total_units_queued,
                .total_units_running = total_units_running,
                .total_units_throttled = total_units_throttled,
                .total_units_completed = total_units_completed,
            };
            const payload = try std.json.Stringify.valueAlloc(self.alloc, record, .{ .emit_null_optional_fields = false });
            defer self.alloc.free(payload);
            try self.core.store.put(key, payload);
            return try cloneRelationalIndexDropJobRecordFromJson(self, payload);
        }

        pub fn recordRelationalIndexDropJobPassAt(
            self: *DB,
            job_id: []const u8,
            status: []const u8,
            complete: bool,
            cursor: []const u8,
            units_queued: u64,
            units_running: u64,
            units_throttled: u64,
            units_completed: u64,
            failure_reason: ?[]const u8,
            stale_generation: bool,
            now_ns: u64,
        ) !RelationalIndexDropJobRecord {
            if (job_id.len == 0 or status.len == 0) return error.InvalidRelationalIndexDropJob;
            self.core.lockApply();
            defer self.core.unlockApply();

            const key = try relationalIndexDropJobKeyAlloc(self.alloc, job_id);
            defer self.alloc.free(key);
            const raw = self.core.store.get(self.alloc, key) catch |err| switch (err) {
                error.NotFound => return error.RelationalIndexDropJobNotFound,
                else => return err,
            };
            defer self.alloc.free(raw);
            const existing = try cloneRelationalIndexDropJobRecordFromJson(self, raw);
            defer self.freeRelationalIndexDropJobRecord(existing);

            const record = RelationalIndexDropJobRecord{
                .job_id = existing.job_id,
                .database_name = existing.database_name,
                .namespace_name = existing.namespace_name,
                .table_name = existing.table_name,
                .access_method = existing.access_method,
                .index_name = existing.index_name,
                .generation = existing.generation,
                .worker_id = existing.worker_id,
                .cursor = cursor,
                .lease_ms = existing.lease_ms,
                .max_work_units = existing.max_work_units,
                .status = status,
                .created_at_ns = existing.created_at_ns,
                .updated_at_ns = now_ns,
                .attempts = existing.attempts,
                .completed = complete,
                .failure_reason = failure_reason,
                .stale_generation = stale_generation,
                .pass_count = existing.pass_count +| 1,
                .last_units_queued = units_queued,
                .last_units_running = units_running,
                .last_units_throttled = units_throttled,
                .last_units_completed = units_completed,
                .total_units_queued = existing.total_units_queued +| units_queued,
                .total_units_running = existing.total_units_running +| units_running,
                .total_units_throttled = existing.total_units_throttled +| units_throttled,
                .total_units_completed = existing.total_units_completed +| units_completed,
            };
            const payload = try std.json.Stringify.valueAlloc(self.alloc, record, .{ .emit_null_optional_fields = false });
            defer self.alloc.free(payload);
            try self.core.store.put(key, payload);
            return try cloneRelationalIndexDropJobRecordFromJson(self, payload);
        }

        pub fn completeForeignKeyIntegrityJobRecord(
            self: *DB,
            job_id: []const u8,
            status: []const u8,
            valid: bool,
            report: relational_store_mod.ForeignKeyIntegrityReport,
        ) !ForeignKeyIntegrityJobRecord {
            return try self.completeForeignKeyIntegrityJobRecordAt(job_id, status, valid, report, currentTimeNs());
        }

        pub fn completeForeignKeyIntegrityJobRecordWithDiagnostics(
            self: *DB,
            job_id: []const u8,
            status: []const u8,
            valid: bool,
            report: relational_store_mod.ForeignKeyIntegrityReport,
            violation_samples_json: []const u8,
            violation_sample_count: usize,
            violations_truncated: bool,
        ) !ForeignKeyIntegrityJobRecord {
            return try self.completeForeignKeyIntegrityJobRecordWithDiagnosticsAt(
                job_id,
                status,
                valid,
                report,
                violation_samples_json,
                violation_sample_count,
                violations_truncated,
                currentTimeNs(),
            );
        }

        pub fn completeForeignKeyIntegrityJobRecordAt(
            self: *DB,
            job_id: []const u8,
            status: []const u8,
            valid: bool,
            report: relational_store_mod.ForeignKeyIntegrityReport,
            now_ns: u64,
        ) !ForeignKeyIntegrityJobRecord {
            return try self.completeForeignKeyIntegrityJobRecordWithDiagnosticsAt(
                job_id,
                status,
                valid,
                report,
                null,
                null,
                null,
                now_ns,
            );
        }

        pub fn completeForeignKeyIntegrityJobRecordWithDiagnosticsAt(
            self: *DB,
            job_id: []const u8,
            status: []const u8,
            valid: bool,
            report: relational_store_mod.ForeignKeyIntegrityReport,
            violation_samples_json: ?[]const u8,
            violation_sample_count: ?usize,
            violations_truncated: ?bool,
            now_ns: u64,
        ) !ForeignKeyIntegrityJobRecord {
            if (job_id.len == 0 or status.len == 0) return error.InvalidForeignKeyIntegrityJob;
            self.core.lockApply();
            defer self.core.unlockApply();

            const key = try foreignKeyIntegrityJobKeyAlloc(self.alloc, job_id);
            defer self.alloc.free(key);
            const raw = self.core.store.get(self.alloc, key) catch |err| switch (err) {
                error.NotFound => return error.ForeignKeyIntegrityJobNotFound,
                else => return err,
            };
            defer self.alloc.free(raw);
            const existing = try cloneForeignKeyIntegrityJobRecordFromJson(self, raw);
            defer self.freeForeignKeyIntegrityJobRecord(existing);
            const has_diagnostics = violation_samples_json != null or violation_sample_count != null or violations_truncated != null;
            const pass_has_violations = !valid or
                foreignKeyIntegrityReportHasViolations(report) or
                (violation_sample_count orelse 0) > 0 or
                (violations_truncated orelse false);

            const record = ForeignKeyIntegrityJobRecord{
                .job_id = existing.job_id,
                .table_name = existing.table_name,
                .action = existing.action,
                .worker_id = existing.worker_id,
                .constraint_name = existing.constraint_name,
                .lower_doc_key = existing.lower_doc_key,
                .upper_doc_key = existing.upper_doc_key,
                .lease_ms = existing.lease_ms,
                .max_work_units = existing.max_work_units,
                .status = status,
                .created_at_ns = existing.created_at_ns,
                .updated_at_ns = now_ns,
                .attempts = existing.attempts,
                .completed = true,
                .valid = valid,
                .last_report = report,
                .aggregate_report = foreignKeyIntegrityReportAdd(existing.aggregate_report, report),
                .violation_samples_json = violation_samples_json orelse existing.violation_samples_json,
                .violation_sample_count = violation_sample_count orelse existing.violation_sample_count,
                .violations_truncated = violations_truncated orelse existing.violations_truncated,
                .diagnostic_passes = existing.diagnostic_passes +| @as(u64, if (has_diagnostics) 1 else 0),
                .violating_passes = existing.violating_passes +| @as(u64, if (pass_has_violations) 1 else 0),
                .first_violation_at_ns = foreignKeyIntegrityFirstViolationAt(existing.first_violation_at_ns, pass_has_violations, now_ns),
                .last_violation_at_ns = if (pass_has_violations) now_ns else existing.last_violation_at_ns,
            };
            const payload = try std.json.Stringify.valueAlloc(self.alloc, record, .{ .emit_null_optional_fields = false });
            defer self.alloc.free(payload);
            try self.core.store.put(key, payload);
            return try cloneForeignKeyIntegrityJobRecordFromJson(self, payload);
        }

        pub fn updateForeignKeyIntegrityJobDiagnostics(
            self: *DB,
            job_id: []const u8,
            violation_samples_json: []const u8,
            violation_sample_count: usize,
            violations_truncated: bool,
        ) !ForeignKeyIntegrityJobRecord {
            return try self.updateForeignKeyIntegrityJobDiagnosticsAt(
                job_id,
                violation_samples_json,
                violation_sample_count,
                violations_truncated,
                currentTimeNs(),
            );
        }

        pub fn updateForeignKeyIntegrityJobDiagnosticsWithReport(
            self: *DB,
            job_id: []const u8,
            report: relational_store_mod.ForeignKeyIntegrityReport,
            violation_samples_json: []const u8,
            violation_sample_count: usize,
            violations_truncated: bool,
        ) !ForeignKeyIntegrityJobRecord {
            return try self.updateForeignKeyIntegrityJobDiagnosticsWithReportAt(
                job_id,
                report,
                violation_samples_json,
                violation_sample_count,
                violations_truncated,
                currentTimeNs(),
            );
        }

        pub fn updateForeignKeyIntegrityJobDiagnosticsAt(
            self: *DB,
            job_id: []const u8,
            violation_samples_json: []const u8,
            violation_sample_count: usize,
            violations_truncated: bool,
            now_ns: u64,
        ) !ForeignKeyIntegrityJobRecord {
            return try updateForeignKeyIntegrityJobDiagnosticsMaybeReportAt(
                self,
                job_id,
                null,
                violation_samples_json,
                violation_sample_count,
                violations_truncated,
                now_ns,
            );
        }

        pub fn updateForeignKeyIntegrityJobDiagnosticsWithReportAt(
            self: *DB,
            job_id: []const u8,
            report: relational_store_mod.ForeignKeyIntegrityReport,
            violation_samples_json: []const u8,
            violation_sample_count: usize,
            violations_truncated: bool,
            now_ns: u64,
        ) !ForeignKeyIntegrityJobRecord {
            return try updateForeignKeyIntegrityJobDiagnosticsMaybeReportAt(
                self,
                job_id,
                report,
                violation_samples_json,
                violation_sample_count,
                violations_truncated,
                now_ns,
            );
        }

        fn updateForeignKeyIntegrityJobDiagnosticsMaybeReportAt(
            self: *DB,
            job_id: []const u8,
            report: ?relational_store_mod.ForeignKeyIntegrityReport,
            violation_samples_json: []const u8,
            violation_sample_count: usize,
            violations_truncated: bool,
            now_ns: u64,
        ) !ForeignKeyIntegrityJobRecord {
            if (job_id.len == 0) return error.InvalidForeignKeyIntegrityJob;
            self.core.lockApply();
            defer self.core.unlockApply();

            const key = try foreignKeyIntegrityJobKeyAlloc(self.alloc, job_id);
            defer self.alloc.free(key);
            const raw = self.core.store.get(self.alloc, key) catch |err| switch (err) {
                error.NotFound => return error.ForeignKeyIntegrityJobNotFound,
                else => return err,
            };
            defer self.alloc.free(raw);
            const existing = try cloneForeignKeyIntegrityJobRecordFromJson(self, raw);
            defer self.freeForeignKeyIntegrityJobRecord(existing);
            const pass_has_violations = foreignKeyIntegrityReportHasViolations(report orelse existing.last_report) or
                violation_sample_count > 0 or
                violations_truncated;

            const record = ForeignKeyIntegrityJobRecord{
                .job_id = existing.job_id,
                .table_name = existing.table_name,
                .action = existing.action,
                .worker_id = existing.worker_id,
                .constraint_name = existing.constraint_name,
                .lower_doc_key = existing.lower_doc_key,
                .upper_doc_key = existing.upper_doc_key,
                .lease_ms = existing.lease_ms,
                .max_work_units = existing.max_work_units,
                .status = existing.status,
                .created_at_ns = existing.created_at_ns,
                .updated_at_ns = now_ns,
                .attempts = existing.attempts,
                .completed = existing.completed,
                .valid = existing.valid,
                .last_report = report orelse existing.last_report,
                .aggregate_report = if (report) |value| foreignKeyIntegrityReportAdd(existing.aggregate_report, value) else existing.aggregate_report,
                .violation_samples_json = violation_samples_json,
                .violation_sample_count = violation_sample_count,
                .violations_truncated = violations_truncated,
                .diagnostic_passes = existing.diagnostic_passes +| 1,
                .violating_passes = existing.violating_passes +| @as(u64, if (pass_has_violations) 1 else 0),
                .first_violation_at_ns = foreignKeyIntegrityFirstViolationAt(existing.first_violation_at_ns, pass_has_violations, now_ns),
                .last_violation_at_ns = if (pass_has_violations) now_ns else existing.last_violation_at_ns,
            };
            const payload = try std.json.Stringify.valueAlloc(self.alloc, record, .{ .emit_null_optional_fields = false });
            defer self.alloc.free(payload);
            try self.core.store.put(key, payload);
            return try cloneForeignKeyIntegrityJobRecordFromJson(self, payload);
        }

        pub fn claimAndRunForeignKeyActionJobPage(
            self: *DB,
            job_id: []const u8,
            action: []const u8,
            worker_id: []const u8,
            constraint_name: []const u8,
            parent_table: []const u8,
            parent_key: []const u8,
            page_limit: usize,
            lease_ms: u64,
        ) !ForeignKeyActionJobRecord {
            return try claimAndRunForeignKeyActionJobPageAt(
                self,
                job_id,
                action,
                worker_id,
                constraint_name,
                parent_table,
                parent_key,
                page_limit,
                lease_ms,
                currentTimeNs(),
            );
        }

        pub fn scheduleForeignKeyActionJob(
            self: *DB,
            job_id: []const u8,
            action: []const u8,
            worker_id: []const u8,
            constraint_name: []const u8,
            parent_table: []const u8,
            parent_key: []const u8,
            page_limit: usize,
        ) !ForeignKeyActionJobRecord {
            return try scheduleForeignKeyActionJobWithUpdatedParentKeyAt(
                self,
                job_id,
                action,
                worker_id,
                constraint_name,
                parent_table,
                parent_key,
                null,
                page_limit,
                currentTimeNs(),
            );
        }

        pub fn scheduleForeignKeyActionJobWithUpdatedParentKey(
            self: *DB,
            job_id: []const u8,
            action: []const u8,
            worker_id: []const u8,
            constraint_name: []const u8,
            parent_table: []const u8,
            parent_key: []const u8,
            updated_parent_key: ?[]const u8,
            page_limit: usize,
        ) !ForeignKeyActionJobRecord {
            return try scheduleForeignKeyActionJobWithUpdatedParentKeyAt(
                self,
                job_id,
                action,
                worker_id,
                constraint_name,
                parent_table,
                parent_key,
                updated_parent_key,
                page_limit,
                currentTimeNs(),
            );
        }

        pub fn scheduleForeignKeyActionJobAt(
            self: *DB,
            job_id: []const u8,
            action: []const u8,
            worker_id: []const u8,
            constraint_name: []const u8,
            parent_table: []const u8,
            parent_key: []const u8,
            page_limit: usize,
            now_ns: u64,
        ) !ForeignKeyActionJobRecord {
            return try scheduleForeignKeyActionJobWithUpdatedParentKeyAt(
                self,
                job_id,
                action,
                worker_id,
                constraint_name,
                parent_table,
                parent_key,
                null,
                page_limit,
                now_ns,
            );
        }

        pub fn scheduleForeignKeyActionJobWithUpdatedParentKeyAt(
            self: *DB,
            job_id: []const u8,
            action: []const u8,
            worker_id: []const u8,
            constraint_name: []const u8,
            parent_table: []const u8,
            parent_key: []const u8,
            updated_parent_key: ?[]const u8,
            page_limit: usize,
            now_ns: u64,
        ) !ForeignKeyActionJobRecord {
            return try scheduleForeignKeyActionJobWithUpdatedParentKeyAndCascadeLineageAt(
                self,
                job_id,
                action,
                worker_id,
                constraint_name,
                parent_table,
                parent_key,
                updated_parent_key,
                page_limit,
                0,
                foreign_key_action_default_cascade_max_depth,
                now_ns,
            );
        }

        pub fn scheduleForeignKeyActionJobWithUpdatedParentKeyAndCascadeLineageAt(
            self: *DB,
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
            now_ns: u64,
        ) !ForeignKeyActionJobRecord {
            if (page_limit == 0) return error.InvalidForeignKeyActionJob;
            const canonical_action = foreignKeyActionJobCanonicalAction(action) orelse return error.InvalidForeignKeyActionJob;
            try validateForeignKeyActionLineage(cascade_depth, cascade_max_depth);
            try validateForeignKeyActionJobIdentity(job_id, canonical_action, worker_id, constraint_name, parent_table, parent_key, updated_parent_key);
            self.core.lockApply();
            defer self.core.unlockApply();

            const key = try foreignKeyActionJobKeyAlloc(self.alloc, job_id);
            defer self.alloc.free(key);

            if (self.core.store.get(self.alloc, key)) |raw| {
                defer self.alloc.free(raw);
                const existing = try cloneForeignKeyActionJobRecordFromJson(self, raw);
                errdefer self.freeForeignKeyActionJobRecord(existing);
                try validateForeignKeyActionJobMatches(existing, canonical_action, constraint_name, parent_table, parent_key, updated_parent_key);
                return existing;
            } else |err| switch (err) {
                error.NotFound => {},
                else => return err,
            }

            const record = ForeignKeyActionJobRecord{
                .job_id = job_id,
                .action = canonical_action,
                .worker_id = worker_id,
                .constraint_name = constraint_name,
                .parent_table = parent_table,
                .parent_key = parent_key,
                .updated_parent_key = updated_parent_key,
                .page_limit = page_limit,
                .status = "pending",
                .created_at_ns = now_ns,
                .updated_at_ns = now_ns,
                .claimed_at_ns = 0,
                .lease_until_ns = 0,
                .attempts = 0,
                .completed = false,
                .applied_children = 0,
                .failure_count = 0,
                .first_failed_at_ns = null,
                .last_failed_at_ns = null,
                .requeue_count = 0,
                .last_requeued_at_ns = null,
                .cascade_depth = cascade_depth,
                .cascade_max_depth = cascade_max_depth,
                .next_child_table = null,
                .next_child_key = null,
                .last_error = null,
            };
            const payload = try std.json.Stringify.valueAlloc(self.alloc, record, .{ .emit_null_optional_fields = false });
            defer self.alloc.free(payload);
            try self.core.store.put(key, payload);
            return try cloneForeignKeyActionJobRecordFromJson(self, payload);
        }

        pub fn requeueForeignKeyActionJob(
            self: *DB,
            job_id: []const u8,
            action: []const u8,
            worker_id: []const u8,
            constraint_name: []const u8,
            parent_table: []const u8,
            parent_key: []const u8,
            page_limit: usize,
        ) !ForeignKeyActionJobRecord {
            return try requeueForeignKeyActionJobWithUpdatedParentKeyAt(
                self,
                job_id,
                action,
                worker_id,
                constraint_name,
                parent_table,
                parent_key,
                null,
                page_limit,
                currentTimeNs(),
            );
        }

        pub fn requeueForeignKeyActionJobWithUpdatedParentKey(
            self: *DB,
            job_id: []const u8,
            action: []const u8,
            worker_id: []const u8,
            constraint_name: []const u8,
            parent_table: []const u8,
            parent_key: []const u8,
            updated_parent_key: ?[]const u8,
            page_limit: usize,
        ) !ForeignKeyActionJobRecord {
            return try requeueForeignKeyActionJobWithUpdatedParentKeyAt(
                self,
                job_id,
                action,
                worker_id,
                constraint_name,
                parent_table,
                parent_key,
                updated_parent_key,
                page_limit,
                currentTimeNs(),
            );
        }

        pub fn requeueForeignKeyActionJobAt(
            self: *DB,
            job_id: []const u8,
            action: []const u8,
            worker_id: []const u8,
            constraint_name: []const u8,
            parent_table: []const u8,
            parent_key: []const u8,
            page_limit: usize,
            now_ns: u64,
        ) !ForeignKeyActionJobRecord {
            return try requeueForeignKeyActionJobWithUpdatedParentKeyAt(
                self,
                job_id,
                action,
                worker_id,
                constraint_name,
                parent_table,
                parent_key,
                null,
                page_limit,
                now_ns,
            );
        }

        pub fn requeueForeignKeyActionJobWithUpdatedParentKeyAt(
            self: *DB,
            job_id: []const u8,
            action: []const u8,
            worker_id: []const u8,
            constraint_name: []const u8,
            parent_table: []const u8,
            parent_key: []const u8,
            updated_parent_key: ?[]const u8,
            page_limit: usize,
            now_ns: u64,
        ) !ForeignKeyActionJobRecord {
            if (page_limit == 0) return error.InvalidForeignKeyActionJob;
            const canonical_action = foreignKeyActionJobCanonicalAction(action) orelse return error.InvalidForeignKeyActionJob;
            try validateForeignKeyActionJobIdentity(job_id, canonical_action, worker_id, constraint_name, parent_table, parent_key, updated_parent_key);
            self.core.lockApply();
            defer self.core.unlockApply();

            const key = try foreignKeyActionJobKeyAlloc(self.alloc, job_id);
            defer self.alloc.free(key);
            const raw = self.core.store.get(self.alloc, key) catch |err| switch (err) {
                error.NotFound => return error.ForeignKeyActionJobNotFound,
                else => return err,
            };
            defer self.alloc.free(raw);

            const existing = try cloneForeignKeyActionJobRecordFromJson(self, raw);
            defer self.freeForeignKeyActionJobRecord(existing);
            try validateForeignKeyActionJobMatches(existing, canonical_action, constraint_name, parent_table, parent_key, updated_parent_key);
            if (existing.completed) return error.InvalidForeignKeyActionJob;
            if (!std.mem.eql(u8, existing.status, "invalid") and existing.last_error == null) {
                return error.InvalidForeignKeyActionJob;
            }

            const record = ForeignKeyActionJobRecord{
                .job_id = existing.job_id,
                .action = existing.action,
                .worker_id = worker_id,
                .constraint_name = existing.constraint_name,
                .parent_table = existing.parent_table,
                .parent_key = existing.parent_key,
                .updated_parent_key = existing.updated_parent_key,
                .page_limit = page_limit,
                .status = "pending",
                .created_at_ns = existing.created_at_ns,
                .updated_at_ns = now_ns,
                .claimed_at_ns = 0,
                .lease_until_ns = 0,
                .attempts = existing.attempts,
                .completed = false,
                .applied_children = existing.applied_children,
                .failure_count = existing.failure_count,
                .first_failed_at_ns = existing.first_failed_at_ns,
                .last_failed_at_ns = existing.last_failed_at_ns,
                .requeue_count = existing.requeue_count +| 1,
                .last_requeued_at_ns = now_ns,
                .cascade_depth = existing.cascade_depth,
                .cascade_max_depth = existing.cascade_max_depth,
                .next_child_table = existing.next_child_table,
                .next_child_key = existing.next_child_key,
                .last_error = null,
            };
            const payload = try std.json.Stringify.valueAlloc(self.alloc, record, .{ .emit_null_optional_fields = false });
            defer self.alloc.free(payload);
            try self.core.store.put(key, payload);
            return try cloneForeignKeyActionJobRecordFromJson(self, payload);
        }

        pub fn scheduleForeignKeyActionSchedule(
            self: *DB,
            schedule_id: []const u8,
            action_job_id: []const u8,
            action: []const u8,
            worker_id: []const u8,
            constraint_name: []const u8,
            parent_table: []const u8,
            parent_key: []const u8,
            page_limit: usize,
        ) !ForeignKeyActionScheduleRecord {
            return try scheduleForeignKeyActionScheduleWithUpdatedParentKeyAt(
                self,
                schedule_id,
                action_job_id,
                action,
                worker_id,
                constraint_name,
                parent_table,
                parent_key,
                null,
                page_limit,
                currentTimeNs(),
            );
        }

        pub fn scheduleForeignKeyActionScheduleWithUpdatedParentKey(
            self: *DB,
            schedule_id: []const u8,
            action_job_id: []const u8,
            action: []const u8,
            worker_id: []const u8,
            constraint_name: []const u8,
            parent_table: []const u8,
            parent_key: []const u8,
            updated_parent_key: ?[]const u8,
            page_limit: usize,
        ) !ForeignKeyActionScheduleRecord {
            return try scheduleForeignKeyActionScheduleWithUpdatedParentKeyAt(
                self,
                schedule_id,
                action_job_id,
                action,
                worker_id,
                constraint_name,
                parent_table,
                parent_key,
                updated_parent_key,
                page_limit,
                currentTimeNs(),
            );
        }

        pub fn scheduleForeignKeyActionScheduleAt(
            self: *DB,
            schedule_id: []const u8,
            action_job_id: []const u8,
            action: []const u8,
            worker_id: []const u8,
            constraint_name: []const u8,
            parent_table: []const u8,
            parent_key: []const u8,
            page_limit: usize,
            now_ns: u64,
        ) !ForeignKeyActionScheduleRecord {
            return try scheduleForeignKeyActionScheduleWithUpdatedParentKeyAt(
                self,
                schedule_id,
                action_job_id,
                action,
                worker_id,
                constraint_name,
                parent_table,
                parent_key,
                null,
                page_limit,
                now_ns,
            );
        }

        pub fn scheduleForeignKeyActionScheduleWithUpdatedParentKeyAt(
            self: *DB,
            schedule_id: []const u8,
            action_job_id: []const u8,
            action: []const u8,
            worker_id: []const u8,
            constraint_name: []const u8,
            parent_table: []const u8,
            parent_key: []const u8,
            updated_parent_key: ?[]const u8,
            page_limit: usize,
            now_ns: u64,
        ) !ForeignKeyActionScheduleRecord {
            if (schedule_id.len == 0 or action_job_id.len == 0 or page_limit == 0) return error.InvalidForeignKeyActionJob;
            const canonical_action = foreignKeyActionJobCanonicalAction(action) orelse return error.InvalidForeignKeyActionJob;
            try validateForeignKeyActionJobIdentity(action_job_id, canonical_action, worker_id, constraint_name, parent_table, parent_key, updated_parent_key);
            self.core.lockApply();
            defer self.core.unlockApply();

            const key = try foreignKeyActionScheduleKeyAlloc(self.alloc, schedule_id);
            defer self.alloc.free(key);
            if (self.core.store.get(self.alloc, key)) |raw| {
                defer self.alloc.free(raw);
                const existing = try cloneForeignKeyActionScheduleRecordFromJson(self, raw);
                errdefer self.freeForeignKeyActionScheduleRecord(existing);
                try validateForeignKeyActionScheduleMatches(existing, action_job_id, canonical_action, constraint_name, parent_table, parent_key, updated_parent_key);
                return existing;
            } else |err| switch (err) {
                error.NotFound => {},
                else => return err,
            }

            const record = ForeignKeyActionScheduleRecord{
                .schedule_id = schedule_id,
                .action_job_id = action_job_id,
                .action = canonical_action,
                .worker_id = worker_id,
                .constraint_name = constraint_name,
                .parent_table = parent_table,
                .parent_key = parent_key,
                .updated_parent_key = updated_parent_key,
                .page_limit = page_limit,
                .status = "pending",
                .created_at_ns = now_ns,
                .updated_at_ns = now_ns,
                .completed = false,
                .scheduled_groups = 0,
                .cascade_depth = 0,
                .cascade_max_depth = foreign_key_action_default_cascade_max_depth,
                .requeue_count = 0,
                .last_requeued_at_ns = null,
                .last_error = null,
            };
            const payload = try std.json.Stringify.valueAlloc(self.alloc, record, .{ .emit_null_optional_fields = false });
            defer self.alloc.free(payload);
            try self.core.store.put(key, payload);
            return try cloneForeignKeyActionScheduleRecordFromJson(self, payload);
        }

        pub fn requeueForeignKeyActionSchedule(
            self: *DB,
            schedule_id: []const u8,
            action_job_id: []const u8,
            action: []const u8,
            worker_id: []const u8,
            constraint_name: []const u8,
            parent_table: []const u8,
            parent_key: []const u8,
            page_limit: usize,
        ) !ForeignKeyActionScheduleRecord {
            return try requeueForeignKeyActionScheduleWithUpdatedParentKeyAt(
                self,
                schedule_id,
                action_job_id,
                action,
                worker_id,
                constraint_name,
                parent_table,
                parent_key,
                null,
                page_limit,
                currentTimeNs(),
            );
        }

        pub fn requeueForeignKeyActionScheduleAt(
            self: *DB,
            schedule_id: []const u8,
            action_job_id: []const u8,
            action: []const u8,
            worker_id: []const u8,
            constraint_name: []const u8,
            parent_table: []const u8,
            parent_key: []const u8,
            page_limit: usize,
            now_ns: u64,
        ) !ForeignKeyActionScheduleRecord {
            return try requeueForeignKeyActionScheduleWithUpdatedParentKeyAt(
                self,
                schedule_id,
                action_job_id,
                action,
                worker_id,
                constraint_name,
                parent_table,
                parent_key,
                null,
                page_limit,
                now_ns,
            );
        }

        pub fn requeueForeignKeyActionScheduleWithUpdatedParentKeyAt(
            self: *DB,
            schedule_id: []const u8,
            action_job_id: []const u8,
            action: []const u8,
            worker_id: []const u8,
            constraint_name: []const u8,
            parent_table: []const u8,
            parent_key: []const u8,
            updated_parent_key: ?[]const u8,
            page_limit: usize,
            now_ns: u64,
        ) !ForeignKeyActionScheduleRecord {
            if (schedule_id.len == 0 or action_job_id.len == 0 or page_limit == 0) return error.InvalidForeignKeyActionJob;
            const canonical_action = foreignKeyActionJobCanonicalAction(action) orelse return error.InvalidForeignKeyActionJob;
            try validateForeignKeyActionJobIdentity(action_job_id, canonical_action, worker_id, constraint_name, parent_table, parent_key, updated_parent_key);
            self.core.lockApply();
            defer self.core.unlockApply();

            const key = try foreignKeyActionScheduleKeyAlloc(self.alloc, schedule_id);
            defer self.alloc.free(key);
            const raw = self.core.store.get(self.alloc, key) catch |err| switch (err) {
                error.NotFound => return error.ForeignKeyActionScheduleNotFound,
                else => return err,
            };
            defer self.alloc.free(raw);

            const existing = try cloneForeignKeyActionScheduleRecordFromJson(self, raw);
            defer self.freeForeignKeyActionScheduleRecord(existing);
            try validateForeignKeyActionScheduleMatches(existing, action_job_id, canonical_action, constraint_name, parent_table, parent_key, updated_parent_key);
            if (existing.completed) return error.InvalidForeignKeyActionJob;
            if (!std.mem.eql(u8, existing.status, "invalid") and existing.last_error == null) {
                return error.InvalidForeignKeyActionJob;
            }

            const record = ForeignKeyActionScheduleRecord{
                .schedule_id = existing.schedule_id,
                .action_job_id = existing.action_job_id,
                .action = existing.action,
                .worker_id = worker_id,
                .constraint_name = existing.constraint_name,
                .parent_table = existing.parent_table,
                .parent_key = existing.parent_key,
                .updated_parent_key = existing.updated_parent_key,
                .page_limit = page_limit,
                .status = "pending",
                .created_at_ns = existing.created_at_ns,
                .updated_at_ns = now_ns,
                .completed = false,
                .scheduled_groups = 0,
                .cascade_depth = existing.cascade_depth,
                .cascade_max_depth = existing.cascade_max_depth,
                .requeue_count = existing.requeue_count +| 1,
                .last_requeued_at_ns = now_ns,
                .last_error = null,
            };
            const payload = try std.json.Stringify.valueAlloc(self.alloc, record, .{ .emit_null_optional_fields = false });
            defer self.alloc.free(payload);
            try self.core.store.put(key, payload);
            return try cloneForeignKeyActionScheduleRecordFromJson(self, payload);
        }

        pub fn markForeignKeyActionScheduleSeeded(
            self: *DB,
            schedule_id: []const u8,
            scheduled_groups: u64,
        ) !ForeignKeyActionScheduleRecord {
            return try markForeignKeyActionScheduleSeededAt(self, schedule_id, scheduled_groups, currentTimeNs());
        }

        pub fn markForeignKeyActionScheduleSeededAt(
            self: *DB,
            schedule_id: []const u8,
            scheduled_groups: u64,
            now_ns: u64,
        ) !ForeignKeyActionScheduleRecord {
            if (schedule_id.len == 0) return error.InvalidForeignKeyActionJob;
            self.core.lockApply();
            defer self.core.unlockApply();

            const key = try foreignKeyActionScheduleKeyAlloc(self.alloc, schedule_id);
            defer self.alloc.free(key);
            const raw = self.core.store.get(self.alloc, key) catch |err| switch (err) {
                error.NotFound => return error.ForeignKeyActionScheduleNotFound,
                else => return err,
            };
            defer self.alloc.free(raw);
            const existing = try cloneForeignKeyActionScheduleRecordFromJson(self, raw);
            defer self.freeForeignKeyActionScheduleRecord(existing);
            if (existing.completed) return try cloneForeignKeyActionScheduleRecordOwned(self, existing);
            if (std.mem.eql(u8, existing.status, "invalid") or existing.last_error != null) {
                return error.InvalidForeignKeyActionJob;
            }

            const record = ForeignKeyActionScheduleRecord{
                .schedule_id = existing.schedule_id,
                .action_job_id = existing.action_job_id,
                .action = existing.action,
                .worker_id = existing.worker_id,
                .constraint_name = existing.constraint_name,
                .parent_table = existing.parent_table,
                .parent_key = existing.parent_key,
                .updated_parent_key = existing.updated_parent_key,
                .page_limit = existing.page_limit,
                .status = if (scheduled_groups == 0) "invalid" else "seeded",
                .created_at_ns = existing.created_at_ns,
                .updated_at_ns = now_ns,
                .completed = scheduled_groups != 0,
                .scheduled_groups = scheduled_groups,
                .cascade_depth = existing.cascade_depth,
                .cascade_max_depth = existing.cascade_max_depth,
                .requeue_count = existing.requeue_count,
                .last_requeued_at_ns = existing.last_requeued_at_ns,
                .last_error = if (scheduled_groups == 0) "NoForeignKeyActionOwnerGroups" else null,
            };
            const payload = try std.json.Stringify.valueAlloc(self.alloc, record, .{ .emit_null_optional_fields = false });
            defer self.alloc.free(payload);
            try self.core.store.put(key, payload);
            return try cloneForeignKeyActionScheduleRecordFromJson(self, payload);
        }

        pub fn claimAndRunForeignKeyActionJobPageAt(
            self: *DB,
            job_id: []const u8,
            action: []const u8,
            worker_id: []const u8,
            constraint_name: []const u8,
            parent_table: []const u8,
            parent_key: []const u8,
            page_limit: usize,
            lease_ms: u64,
            now_ns: u64,
        ) !ForeignKeyActionJobRecord {
            return try claimAndRunForeignKeyActionJobPageWithUpdatedParentKeyAt(
                self,
                job_id,
                action,
                worker_id,
                constraint_name,
                parent_table,
                parent_key,
                null,
                page_limit,
                lease_ms,
                now_ns,
            );
        }

        pub fn claimAndRunForeignKeyActionJobPageWithUpdatedParentKeyAt(
            self: *DB,
            job_id: []const u8,
            action: []const u8,
            worker_id: []const u8,
            constraint_name: []const u8,
            parent_table: []const u8,
            parent_key: []const u8,
            updated_parent_key: ?[]const u8,
            page_limit: usize,
            lease_ms: u64,
            now_ns: u64,
        ) !ForeignKeyActionJobRecord {
            return try claimAndRunForeignKeyActionJobPageWithUpdatedParentKeyAndCascadeLineageAt(
                self,
                job_id,
                action,
                worker_id,
                constraint_name,
                parent_table,
                parent_key,
                updated_parent_key,
                page_limit,
                lease_ms,
                0,
                foreign_key_action_default_cascade_max_depth,
                now_ns,
            );
        }

        pub fn claimAndRunForeignKeyActionJobPageWithUpdatedParentKeyAndCascadeLineageAt(
            self: *DB,
            job_id: []const u8,
            action: []const u8,
            worker_id: []const u8,
            constraint_name: []const u8,
            parent_table: []const u8,
            parent_key: []const u8,
            updated_parent_key: ?[]const u8,
            page_limit: usize,
            lease_ms: u64,
            cascade_depth: u32,
            cascade_max_depth: u32,
            now_ns: u64,
        ) !ForeignKeyActionJobRecord {
            if (page_limit == 0 or lease_ms == 0) return error.InvalidForeignKeyActionJob;
            const canonical_action = foreignKeyActionJobCanonicalAction(action) orelse return error.InvalidForeignKeyActionJob;
            try validateForeignKeyActionLineage(cascade_depth, cascade_max_depth);
            try validateForeignKeyActionJobIdentity(job_id, canonical_action, worker_id, constraint_name, parent_table, parent_key, updated_parent_key);

            const claimed = try claimForeignKeyActionJobRecordAt(
                self,
                job_id,
                canonical_action,
                worker_id,
                constraint_name,
                parent_table,
                parent_key,
                updated_parent_key,
                page_limit,
                lease_ms,
                cascade_depth,
                cascade_max_depth,
                now_ns,
            );
            defer self.freeForeignKeyActionJobRecord(claimed);
            if (claimed.completed) return try cloneForeignKeyActionJobRecordOwned(self, claimed);

            return runClaimedForeignKeyActionJobPageAt(self, claimed, canonical_action, constraint_name, parent_table, parent_key, page_limit, now_ns) catch |err| {
                const failed = updateForeignKeyActionJobRecordAfterPageAt(
                    self,
                    claimed,
                    0,
                    false,
                    claimed.next_child_table,
                    claimed.next_child_key,
                    @errorName(err),
                    now_ns +| 1,
                ) catch null;
                if (failed) |record| self.freeForeignKeyActionJobRecord(record);
                return err;
            };
        }

        pub fn claimForeignKeyActionJobPage(
            self: *DB,
            job_id: []const u8,
            action: []const u8,
            worker_id: []const u8,
            constraint_name: []const u8,
            parent_table: []const u8,
            parent_key: []const u8,
            page_limit: usize,
            lease_ms: u64,
        ) !ForeignKeyActionJobRecord {
            return try claimForeignKeyActionJobPageAt(
                self,
                job_id,
                action,
                worker_id,
                constraint_name,
                parent_table,
                parent_key,
                page_limit,
                lease_ms,
                currentTimeNs(),
            );
        }

        pub fn claimForeignKeyActionJobPageAt(
            self: *DB,
            job_id: []const u8,
            action: []const u8,
            worker_id: []const u8,
            constraint_name: []const u8,
            parent_table: []const u8,
            parent_key: []const u8,
            page_limit: usize,
            lease_ms: u64,
            now_ns: u64,
        ) !ForeignKeyActionJobRecord {
            return try claimForeignKeyActionJobPageWithUpdatedParentKeyAt(
                self,
                job_id,
                action,
                worker_id,
                constraint_name,
                parent_table,
                parent_key,
                null,
                page_limit,
                lease_ms,
                now_ns,
            );
        }

        pub fn claimForeignKeyActionJobPageWithUpdatedParentKeyAt(
            self: *DB,
            job_id: []const u8,
            action: []const u8,
            worker_id: []const u8,
            constraint_name: []const u8,
            parent_table: []const u8,
            parent_key: []const u8,
            updated_parent_key: ?[]const u8,
            page_limit: usize,
            lease_ms: u64,
            now_ns: u64,
        ) !ForeignKeyActionJobRecord {
            return try claimForeignKeyActionJobRecordAt(
                self,
                job_id,
                action,
                worker_id,
                constraint_name,
                parent_table,
                parent_key,
                updated_parent_key,
                page_limit,
                lease_ms,
                0,
                foreign_key_action_default_cascade_max_depth,
                now_ns,
            );
        }

        pub fn claimForeignKeyActionJobPageWithUpdatedParentKeyAndCascadeLineageAt(
            self: *DB,
            job_id: []const u8,
            action: []const u8,
            worker_id: []const u8,
            constraint_name: []const u8,
            parent_table: []const u8,
            parent_key: []const u8,
            updated_parent_key: ?[]const u8,
            page_limit: usize,
            lease_ms: u64,
            cascade_depth: u32,
            cascade_max_depth: u32,
            now_ns: u64,
        ) !ForeignKeyActionJobRecord {
            return try claimForeignKeyActionJobRecordAt(
                self,
                job_id,
                action,
                worker_id,
                constraint_name,
                parent_table,
                parent_key,
                updated_parent_key,
                page_limit,
                lease_ms,
                cascade_depth,
                cascade_max_depth,
                now_ns,
            );
        }

        pub fn finishClaimedForeignKeyActionJobPage(
            self: *DB,
            claimed: ForeignKeyActionJobRecord,
            applied_count: usize,
            complete: bool,
            next_child_table: ?[]const u8,
            next_child_key: ?[]const u8,
            last_error: ?[]const u8,
        ) !ForeignKeyActionJobRecord {
            return try finishClaimedForeignKeyActionJobPageAt(
                self,
                claimed,
                applied_count,
                complete,
                next_child_table,
                next_child_key,
                last_error,
                currentTimeNs(),
            );
        }

        pub fn finishClaimedForeignKeyActionJobPageAt(
            self: *DB,
            claimed: ForeignKeyActionJobRecord,
            applied_count: usize,
            complete: bool,
            next_child_table: ?[]const u8,
            next_child_key: ?[]const u8,
            last_error: ?[]const u8,
            now_ns: u64,
        ) !ForeignKeyActionJobRecord {
            return try updateForeignKeyActionJobRecordAfterPageAt(
                self,
                claimed,
                applied_count,
                complete,
                next_child_table,
                next_child_key,
                last_error,
                now_ns,
            );
        }

        fn runClaimedForeignKeyActionJobPageAt(
            self: *DB,
            claimed: ForeignKeyActionJobRecord,
            action: []const u8,
            constraint_name: []const u8,
            parent_table: []const u8,
            parent_key: []const u8,
            page_limit: usize,
            now_ns: u64,
        ) !ForeignKeyActionJobRecord {
            var page = try self.listForeignKeyRefChildrenPageForParent(
                self.alloc,
                constraint_name,
                parent_table,
                parent_key,
                claimed.next_child_table,
                claimed.next_child_key,
                page_limit,
            );
            defer self.freeForeignKeyRefChildrenPage(self.alloc, &page);

            if (page.children.len > 0) {
                const txn_id = try self.beginTransaction(now_ns);
                var txn_open = true;
                errdefer if (txn_open) self.abortTransaction(txn_id, now_ns +| 1) catch {};

                if (std.mem.eql(u8, action, "set_null")) {
                    const actions = try self.alloc.alloc(types.ForeignKeySetNullChildAction, page.children.len);
                    defer self.alloc.free(actions);
                    for (page.children, 0..) |child, i| {
                        actions[i] = .{
                            .constraint_name = constraint_name,
                            .parent_table = parent_table,
                            .parent_key = parent_key,
                            .child_key = child.child_key,
                        };
                    }
                    try self.writeTransaction(txn_id, .{ .foreign_key_set_null_children = actions });
                } else if (std.mem.eql(u8, action, "update_set_null")) {
                    const actions = try self.alloc.alloc(types.ForeignKeySetNullChildAction, page.children.len);
                    defer self.alloc.free(actions);
                    for (page.children, 0..) |child, i| {
                        actions[i] = .{
                            .constraint_name = constraint_name,
                            .parent_table = parent_table,
                            .parent_key = parent_key,
                            .child_key = child.child_key,
                            .operation = .update,
                        };
                    }
                    try self.writeTransaction(txn_id, .{ .foreign_key_set_null_children = actions });
                } else if (std.mem.eql(u8, action, "cascade")) {
                    const actions = try self.alloc.alloc(types.ForeignKeyCascadeChildAction, page.children.len);
                    defer self.alloc.free(actions);
                    for (page.children, 0..) |child, i| {
                        actions[i] = .{
                            .constraint_name = constraint_name,
                            .parent_table = parent_table,
                            .parent_key = parent_key,
                            .child_key = child.child_key,
                        };
                    }
                    try self.writeTransaction(txn_id, .{ .foreign_key_cascade_children = actions });
                } else if (std.mem.eql(u8, action, "update_cascade")) {
                    const updated_parent_key = claimed.updated_parent_key orelse return error.InvalidForeignKeyActionJob;
                    const actions = try self.alloc.alloc(types.ForeignKeyCascadeChildAction, page.children.len);
                    defer self.alloc.free(actions);
                    for (page.children, 0..) |child, i| {
                        actions[i] = .{
                            .constraint_name = constraint_name,
                            .parent_table = parent_table,
                            .parent_key = parent_key,
                            .updated_parent_key = updated_parent_key,
                            .child_key = child.child_key,
                            .operation = .update,
                        };
                    }
                    try self.writeTransaction(txn_id, .{ .foreign_key_cascade_children = actions });
                } else {
                    return error.InvalidForeignKeyActionJob;
                }
                try self.commitTransaction(txn_id, now_ns +| 1);
                txn_open = false;
            }

            return try updateForeignKeyActionJobRecordAfterPageAt(
                self,
                claimed,
                page.children.len,
                page.complete,
                page.next_child_table,
                page.next_child_key,
                null,
                now_ns +| 1,
            );
        }

        fn claimForeignKeyActionJobRecordAt(
            self: *DB,
            job_id: []const u8,
            action: []const u8,
            worker_id: []const u8,
            constraint_name: []const u8,
            parent_table: []const u8,
            parent_key: []const u8,
            updated_parent_key: ?[]const u8,
            page_limit: usize,
            lease_ms: u64,
            cascade_depth: u32,
            cascade_max_depth: u32,
            now_ns: u64,
        ) !ForeignKeyActionJobRecord {
            if (page_limit == 0 or lease_ms == 0) return error.InvalidForeignKeyActionJob;
            const canonical_action = foreignKeyActionJobCanonicalAction(action) orelse return error.InvalidForeignKeyActionJob;
            try validateForeignKeyActionLineage(cascade_depth, cascade_max_depth);
            try validateForeignKeyActionJobIdentity(job_id, canonical_action, worker_id, constraint_name, parent_table, parent_key, updated_parent_key);
            self.core.lockApply();
            defer self.core.unlockApply();

            const key = try foreignKeyActionJobKeyAlloc(self.alloc, job_id);
            defer self.alloc.free(key);

            var created_at_ns = now_ns;
            var attempts: u32 = 1;
            var completed = false;
            var applied_children: u64 = 0;
            var failure_count: u64 = 0;
            var first_failed_at_ns: ?u64 = null;
            var last_failed_at_ns: ?u64 = null;
            var requeue_count: u64 = 0;
            var last_requeued_at_ns: ?u64 = null;
            var record_cascade_depth: u32 = cascade_depth;
            var record_cascade_max_depth: u32 = cascade_max_depth;
            var next_child_table: ?[]const u8 = null;
            var next_child_key: ?[]const u8 = null;
            var existing_next_child_table: ?[]u8 = null;
            var existing_next_child_key: ?[]u8 = null;
            defer {
                if (existing_next_child_table) |value| self.alloc.free(value);
                if (existing_next_child_key) |value| self.alloc.free(value);
            }
            if (self.core.store.get(self.alloc, key)) |raw| {
                defer self.alloc.free(raw);
                const existing = try cloneForeignKeyActionJobRecordFromJson(self, raw);
                defer self.freeForeignKeyActionJobRecord(existing);
                try validateForeignKeyActionJobMatches(existing, canonical_action, constraint_name, parent_table, parent_key, updated_parent_key);
                if (existing.completed) {
                    return try cloneForeignKeyActionJobRecordOwned(self, existing);
                }
                if (!existing.completed and (std.mem.eql(u8, existing.status, "invalid") or existing.last_error != null)) {
                    return error.InvalidForeignKeyActionJob;
                }
                if (!existing.completed and !std.mem.eql(u8, existing.worker_id, worker_id) and existing.lease_until_ns > now_ns) {
                    return error.ForeignKeyIntegrityClaimBusy;
                }
                created_at_ns = existing.created_at_ns;
                attempts = existing.attempts +| 1;
                completed = existing.completed;
                applied_children = existing.applied_children;
                failure_count = existing.failure_count;
                first_failed_at_ns = existing.first_failed_at_ns;
                last_failed_at_ns = existing.last_failed_at_ns;
                requeue_count = existing.requeue_count;
                last_requeued_at_ns = existing.last_requeued_at_ns;
                record_cascade_depth = existing.cascade_depth;
                record_cascade_max_depth = existing.cascade_max_depth;
                if (existing.next_child_table) |value| {
                    existing_next_child_table = try self.alloc.dupe(u8, value);
                    next_child_table = existing_next_child_table.?;
                }
                if (existing.next_child_key) |value| {
                    existing_next_child_key = try self.alloc.dupe(u8, value);
                    next_child_key = existing_next_child_key.?;
                }
            } else |err| switch (err) {
                error.NotFound => {},
                else => return err,
            }

            const lease_ns = std.math.mul(u64, lease_ms, std.time.ns_per_ms) catch std.math.maxInt(u64);
            const lease_until_ns = now_ns +| lease_ns;
            const record = ForeignKeyActionJobRecord{
                .job_id = job_id,
                .action = canonical_action,
                .worker_id = worker_id,
                .constraint_name = constraint_name,
                .parent_table = parent_table,
                .parent_key = parent_key,
                .updated_parent_key = updated_parent_key,
                .page_limit = page_limit,
                .status = if (completed) "complete" else "claimed",
                .created_at_ns = created_at_ns,
                .updated_at_ns = now_ns,
                .claimed_at_ns = now_ns,
                .lease_until_ns = lease_until_ns,
                .attempts = attempts,
                .completed = completed,
                .applied_children = applied_children,
                .failure_count = failure_count,
                .first_failed_at_ns = first_failed_at_ns,
                .last_failed_at_ns = last_failed_at_ns,
                .requeue_count = requeue_count,
                .last_requeued_at_ns = last_requeued_at_ns,
                .cascade_depth = record_cascade_depth,
                .cascade_max_depth = record_cascade_max_depth,
                .next_child_table = next_child_table,
                .next_child_key = next_child_key,
                .last_error = null,
            };
            const payload = try std.json.Stringify.valueAlloc(self.alloc, record, .{ .emit_null_optional_fields = false });
            defer self.alloc.free(payload);
            try self.core.store.put(key, payload);
            return try cloneForeignKeyActionJobRecordFromJson(self, payload);
        }

        fn updateForeignKeyActionJobRecordAfterPageAt(
            self: *DB,
            existing: ForeignKeyActionJobRecord,
            applied_count: usize,
            complete: bool,
            next_child_table: ?[]const u8,
            next_child_key: ?[]const u8,
            last_error: ?[]const u8,
            now_ns: u64,
        ) !ForeignKeyActionJobRecord {
            try validateForeignKeyActionJobPageFinish(applied_count, complete, next_child_table, next_child_key, last_error);
            self.core.lockApply();
            defer self.core.unlockApply();

            const key = try foreignKeyActionJobKeyAlloc(self.alloc, existing.job_id);
            defer self.alloc.free(key);
            const current_raw = self.core.store.get(self.alloc, key) catch |err| switch (err) {
                error.NotFound => return error.ForeignKeyActionJobNotFound,
                else => return err,
            };
            defer self.alloc.free(current_raw);
            const current = try cloneForeignKeyActionJobRecordFromJson(self, current_raw);
            defer self.freeForeignKeyActionJobRecord(current);
            try validateForeignKeyActionJobMatches(current, existing.action, existing.constraint_name, existing.parent_table, existing.parent_key, existing.updated_parent_key);
            if (!foreignKeyActionJobClaimsMatch(current, existing)) return error.ForeignKeyIntegrityClaimBusy;
            const failed = last_error != null;

            const record = ForeignKeyActionJobRecord{
                .job_id = current.job_id,
                .action = current.action,
                .worker_id = current.worker_id,
                .constraint_name = current.constraint_name,
                .parent_table = current.parent_table,
                .parent_key = current.parent_key,
                .updated_parent_key = current.updated_parent_key,
                .page_limit = current.page_limit,
                .status = if (last_error != null) "invalid" else if (complete) "complete" else "pending",
                .created_at_ns = current.created_at_ns,
                .updated_at_ns = now_ns,
                .claimed_at_ns = current.claimed_at_ns,
                .lease_until_ns = current.lease_until_ns,
                .attempts = current.attempts,
                .completed = complete and !failed,
                .applied_children = current.applied_children +| @as(u64, @intCast(applied_count)),
                .failure_count = current.failure_count +| @as(u64, if (failed) 1 else 0),
                .first_failed_at_ns = if (failed and current.first_failed_at_ns == null) now_ns else current.first_failed_at_ns,
                .last_failed_at_ns = if (failed) now_ns else current.last_failed_at_ns,
                .requeue_count = current.requeue_count,
                .last_requeued_at_ns = current.last_requeued_at_ns,
                .cascade_depth = current.cascade_depth,
                .cascade_max_depth = current.cascade_max_depth,
                .next_child_table = if (complete) null else next_child_table,
                .next_child_key = if (complete) null else next_child_key,
                .last_error = last_error,
            };
            const payload = try std.json.Stringify.valueAlloc(self.alloc, record, .{ .emit_null_optional_fields = false });
            defer self.alloc.free(payload);
            try self.core.store.put(key, payload);
            return try cloneForeignKeyActionJobRecordFromJson(self, payload);
        }

        fn validateForeignKeyActionJobPageFinish(
            applied_count: usize,
            complete: bool,
            next_child_table: ?[]const u8,
            next_child_key: ?[]const u8,
            last_error: ?[]const u8,
        ) !void {
            if ((next_child_table == null) != (next_child_key == null)) return error.InvalidForeignKeyActionJob;
            if (next_child_table) |table| if (table.len == 0) return error.InvalidForeignKeyActionJob;
            if (next_child_key) |key| if (key.len == 0) return error.InvalidForeignKeyActionJob;
            if (complete) {
                if (last_error != null or next_child_table != null) return error.InvalidForeignKeyActionJob;
                return;
            }
            if (last_error == null and next_child_table == null) return error.InvalidForeignKeyActionJob;
            if (last_error != null and applied_count > 0 and next_child_table == null) return error.InvalidForeignKeyActionJob;
        }

        pub fn claimAndRunForeignKeyIntegrityWorkUnit(
            self: *DB,
            claim_key: []const u8,
            worker_id: []const u8,
            group_id: u64,
            phase: []const u8,
            mode: relational_store_mod.ForeignKeyIntegrityMode,
            constraint_name: ?[]const u8,
            lower_doc_key: []const u8,
            upper_doc_key: []const u8,
            lease_ms: u64,
        ) !ForeignKeyIntegrityReport {
            return try self.claimAndRunForeignKeyIntegrityWorkUnitAt(
                claim_key,
                worker_id,
                group_id,
                phase,
                mode,
                constraint_name,
                lower_doc_key,
                upper_doc_key,
                lease_ms,
                currentTimeNs(),
            );
        }

        pub fn claimAndRunForeignKeyIntegrityWorkUnitAt(
            self: *DB,
            claim_key: []const u8,
            worker_id: []const u8,
            group_id: u64,
            phase: []const u8,
            mode: relational_store_mod.ForeignKeyIntegrityMode,
            constraint_name: ?[]const u8,
            lower_doc_key: []const u8,
            upper_doc_key: []const u8,
            lease_ms: u64,
            now_ns: u64,
        ) !ForeignKeyIntegrityReport {
            const claim = try claimForeignKeyIntegrityWorkUnitAt(
                self,
                claim_key,
                worker_id,
                group_id,
                phase,
                foreignKeyIntegrityModeName(mode),
                constraint_name,
                lower_doc_key,
                upper_doc_key,
                lease_ms,
                now_ns,
            );
            defer self.freeForeignKeyIntegrityClaimRecord(claim);

            if (std.mem.eql(u8, phase, "owner_range")) {
                const scoped_constraint = constraint_name orelse return error.InvalidForeignKeyIntegrityRequest;
                self.core.lockApply();
                defer self.core.unlockApply();
                return try self.relationalIntegrityReconcileForeignKeyRefOwnerRangeForConstraintLocked(scoped_constraint, lower_doc_key, upper_doc_key, mode);
            }
            if (!std.mem.eql(u8, phase, "child_range")) return error.InvalidForeignKeyIntegrityRequest;
            return switch (mode) {
                .validate => try self.validateForeignKeyRefsInRangeForConstraint(constraint_name, lower_doc_key, upper_doc_key),
                .dry_run => try self.dryRunRepairForeignKeyRefsInRangeForConstraint(constraint_name, lower_doc_key, upper_doc_key),
                .repair => try self.repairForeignKeyRefsInRangeForConstraint(constraint_name, lower_doc_key, upper_doc_key),
            };
        }

        pub fn foreignKeyIntegrityProgressKeyAlloc(
            alloc: Allocator,
            phase: []const u8,
            mode: relational_store_mod.ForeignKeyIntegrityMode,
            constraint_name: ?[]const u8,
            lower_doc_key: []const u8,
            upper_doc_key: []const u8,
        ) ![]u8 {
            const constraint = constraint_name orelse "*";
            return try std.fmt.allocPrint(alloc, "{s}:v2:{d}:{s}:{s}:{d}:{s}:{d}:{s}:{d}:{s}", .{
                foreign_key_integrity_progress_key_prefix,
                phase.len,
                phase,
                foreignKeyIntegrityModeName(mode),
                constraint.len,
                constraint,
                lower_doc_key.len,
                lower_doc_key,
                upper_doc_key.len,
                upper_doc_key,
            });
        }

        pub fn foreignKeyIntegrityClaimKeyAlloc(
            alloc: Allocator,
            claim_key: []const u8,
        ) ![]u8 {
            return try std.fmt.allocPrint(alloc, "{s}:{d}:{s}", .{
                foreign_key_integrity_claim_key_prefix,
                claim_key.len,
                claim_key,
            });
        }

        pub fn foreignKeyIntegrityJobKeyAlloc(
            alloc: Allocator,
            job_id: []const u8,
        ) ![]u8 {
            return try std.fmt.allocPrint(alloc, "{s}:{d}:{s}", .{
                foreign_key_integrity_job_key_prefix,
                job_id.len,
                job_id,
            });
        }

        pub fn relationalIndexRepairJobKeyAlloc(
            alloc: Allocator,
            job_id: []const u8,
        ) ![]u8 {
            return try std.fmt.allocPrint(alloc, "{s}:{d}:{s}", .{
                relational_index_repair_job_key_prefix,
                job_id.len,
                job_id,
            });
        }

        pub fn relationalIndexDropJobKeyAlloc(
            alloc: Allocator,
            job_id: []const u8,
        ) ![]u8 {
            return try std.fmt.allocPrint(alloc, "{s}:{d}:{s}", .{
                relational_index_drop_job_key_prefix,
                job_id.len,
                job_id,
            });
        }

        pub fn foreignKeyActionJobKeyAlloc(
            alloc: Allocator,
            job_id: []const u8,
        ) ![]u8 {
            return try std.fmt.allocPrint(alloc, "{s}:{d}:{s}", .{
                foreign_key_action_job_key_prefix,
                job_id.len,
                job_id,
            });
        }

        pub fn foreignKeyActionScheduleKeyAlloc(
            alloc: Allocator,
            schedule_id: []const u8,
        ) ![]u8 {
            return try std.fmt.allocPrint(alloc, "{s}:{d}:{s}", .{
                foreign_key_action_schedule_key_prefix,
                schedule_id.len,
                schedule_id,
            });
        }

        pub fn uniqueConstraintIntegrityProgressKeyAlloc(
            alloc: Allocator,
            mode: relational_store_mod.ForeignKeyIntegrityMode,
            lower_doc_key: []const u8,
            upper_doc_key: []const u8,
        ) ![]u8 {
            return try std.fmt.allocPrint(alloc, "{s}:{s}:{d}:{s}:{d}:{s}", .{
                unique_constraint_integrity_progress_key_prefix,
                foreignKeyIntegrityModeName(mode),
                lower_doc_key.len,
                lower_doc_key,
                upper_doc_key.len,
                upper_doc_key,
            });
        }

        pub fn cloneForeignKeyIntegrityClaimRecordFromJson(self: *DB, raw: []const u8) !ForeignKeyIntegrityClaimRecord {
            var parsed = try std.json.parseFromSlice(ForeignKeyIntegrityClaimRecord, self.alloc, raw, .{
                .allocate = .alloc_always,
                .ignore_unknown_fields = true,
            });
            defer parsed.deinit();

            var cloned = ForeignKeyIntegrityClaimRecord{
                .version = parsed.value.version,
                .claim_key = &.{},
                .worker_id = &.{},
                .group_id = parsed.value.group_id,
                .phase = &.{},
                .planned_action = &.{},
                .constraint_name = null,
                .lower_doc_key = &.{},
                .upper_doc_key = &.{},
                .claimed_at_ns = parsed.value.claimed_at_ns,
                .lease_until_ns = parsed.value.lease_until_ns,
                .attempts = parsed.value.attempts,
            };
            errdefer self.freeForeignKeyIntegrityClaimRecord(cloned);
            cloned.claim_key = try self.alloc.dupe(u8, parsed.value.claim_key);
            cloned.worker_id = try self.alloc.dupe(u8, parsed.value.worker_id);
            cloned.phase = try self.alloc.dupe(u8, parsed.value.phase);
            cloned.planned_action = try self.alloc.dupe(u8, parsed.value.planned_action);
            if (parsed.value.constraint_name) |value| cloned.constraint_name = try self.alloc.dupe(u8, value);
            cloned.lower_doc_key = try self.alloc.dupe(u8, parsed.value.lower_doc_key);
            cloned.upper_doc_key = try self.alloc.dupe(u8, parsed.value.upper_doc_key);
            return cloned;
        }

        pub fn cloneForeignKeyIntegrityJobRecordFromJson(self: *DB, raw: []const u8) !ForeignKeyIntegrityJobRecord {
            var parsed = try std.json.parseFromSlice(ForeignKeyIntegrityJobRecord, self.alloc, raw, .{
                .allocate = .alloc_always,
                .ignore_unknown_fields = true,
            });
            defer parsed.deinit();

            var cloned = ForeignKeyIntegrityJobRecord{
                .version = parsed.value.version,
                .job_id = &.{},
                .table_name = &.{},
                .action = &.{},
                .worker_id = &.{},
                .constraint_name = null,
                .lower_doc_key = &.{},
                .upper_doc_key = &.{},
                .lease_ms = parsed.value.lease_ms,
                .max_work_units = parsed.value.max_work_units,
                .status = &.{},
                .created_at_ns = parsed.value.created_at_ns,
                .updated_at_ns = parsed.value.updated_at_ns,
                .attempts = parsed.value.attempts,
                .completed = parsed.value.completed,
                .valid = parsed.value.valid,
                .last_report = parsed.value.last_report,
                .aggregate_report = parsed.value.aggregate_report,
                .violation_samples_json = &.{},
                .violation_sample_count = parsed.value.violation_sample_count,
                .violations_truncated = parsed.value.violations_truncated,
                .diagnostic_passes = parsed.value.diagnostic_passes,
                .violating_passes = parsed.value.violating_passes,
                .first_violation_at_ns = parsed.value.first_violation_at_ns,
                .last_violation_at_ns = parsed.value.last_violation_at_ns,
            };
            errdefer self.freeForeignKeyIntegrityJobRecord(cloned);
            cloned.job_id = try self.alloc.dupe(u8, parsed.value.job_id);
            cloned.table_name = try self.alloc.dupe(u8, parsed.value.table_name);
            cloned.action = try self.alloc.dupe(u8, parsed.value.action);
            cloned.worker_id = try self.alloc.dupe(u8, parsed.value.worker_id);
            if (parsed.value.constraint_name) |value| cloned.constraint_name = try self.alloc.dupe(u8, value);
            cloned.lower_doc_key = try self.alloc.dupe(u8, parsed.value.lower_doc_key);
            cloned.upper_doc_key = try self.alloc.dupe(u8, parsed.value.upper_doc_key);
            cloned.status = try self.alloc.dupe(u8, parsed.value.status);
            cloned.violation_samples_json = try self.alloc.dupe(u8, parsed.value.violation_samples_json);
            return cloned;
        }

        pub fn cloneRelationalIndexRepairJobRecordFromJson(self: *DB, raw: []const u8) !RelationalIndexRepairJobRecord {
            var parsed = try std.json.parseFromSlice(RelationalIndexRepairJobRecord, self.alloc, raw, .{
                .allocate = .alloc_always,
                .ignore_unknown_fields = true,
            });
            defer parsed.deinit();

            var cloned = RelationalIndexRepairJobRecord{
                .version = parsed.value.version,
                .job_id = &.{},
                .database_name = &.{},
                .namespace_name = &.{},
                .table_name = &.{},
                .access_method = &.{},
                .index_name = &.{},
                .generation = parsed.value.generation,
                .worker_id = &.{},
                .lower_doc_key = &.{},
                .upper_doc_key = &.{},
                .lease_ms = parsed.value.lease_ms,
                .max_work_units = parsed.value.max_work_units,
                .status = &.{},
                .created_at_ns = parsed.value.created_at_ns,
                .updated_at_ns = parsed.value.updated_at_ns,
                .attempts = parsed.value.attempts,
                .completed = parsed.value.completed,
                .complete = parsed.value.complete,
                .next_lower_doc_key = &.{},
                .cursor = &.{},
                .failure_reason = null,
                .stale_generation = parsed.value.stale_generation,
                .pass_count = parsed.value.pass_count,
                .last_units_queued = parsed.value.last_units_queued,
                .last_units_running = parsed.value.last_units_running,
                .last_units_throttled = parsed.value.last_units_throttled,
                .last_units_completed = parsed.value.last_units_completed,
                .total_units_queued = parsed.value.total_units_queued,
                .total_units_running = parsed.value.total_units_running,
                .total_units_throttled = parsed.value.total_units_throttled,
                .total_units_completed = parsed.value.total_units_completed,
                .last_ranges_scanned = parsed.value.last_ranges_scanned,
                .last_ranges_repaired = parsed.value.last_ranges_repaired,
                .last_ranges_missing = parsed.value.last_ranges_missing,
                .total_ranges_scanned = parsed.value.total_ranges_scanned,
                .total_ranges_repaired = parsed.value.total_ranges_repaired,
                .total_ranges_missing = parsed.value.total_ranges_missing,
                .last_report = parsed.value.last_report,
                .aggregate_report = parsed.value.aggregate_report,
                .last_error = null,
            };
            errdefer self.freeRelationalIndexRepairJobRecord(cloned);
            cloned.job_id = try self.alloc.dupe(u8, parsed.value.job_id);
            cloned.database_name = try self.alloc.dupe(u8, parsed.value.database_name);
            cloned.namespace_name = try self.alloc.dupe(u8, parsed.value.namespace_name);
            cloned.table_name = try self.alloc.dupe(u8, parsed.value.table_name);
            cloned.access_method = try self.alloc.dupe(u8, parsed.value.access_method);
            cloned.index_name = try self.alloc.dupe(u8, parsed.value.index_name);
            cloned.worker_id = try self.alloc.dupe(u8, parsed.value.worker_id);
            cloned.lower_doc_key = try self.alloc.dupe(u8, parsed.value.lower_doc_key);
            cloned.upper_doc_key = try self.alloc.dupe(u8, parsed.value.upper_doc_key);
            cloned.status = try self.alloc.dupe(u8, parsed.value.status);
            cloned.next_lower_doc_key = try self.alloc.dupe(u8, parsed.value.next_lower_doc_key);
            cloned.cursor = try self.alloc.dupe(u8, parsed.value.cursor);
            if (parsed.value.failure_reason) |value| cloned.failure_reason = try self.alloc.dupe(u8, value);
            if (parsed.value.last_error) |value| cloned.last_error = try self.alloc.dupe(u8, value);
            return cloned;
        }

        pub fn cloneRelationalIndexDropJobRecordFromJson(self: *DB, raw: []const u8) !RelationalIndexDropJobRecord {
            var parsed = try std.json.parseFromSlice(RelationalIndexDropJobRecord, self.alloc, raw, .{
                .allocate = .alloc_always,
                .ignore_unknown_fields = true,
            });
            defer parsed.deinit();

            var cloned = RelationalIndexDropJobRecord{
                .version = parsed.value.version,
                .job_id = &.{},
                .database_name = &.{},
                .namespace_name = &.{},
                .table_name = &.{},
                .access_method = &.{},
                .index_name = &.{},
                .generation = parsed.value.generation,
                .worker_id = &.{},
                .cursor = &.{},
                .lease_ms = parsed.value.lease_ms,
                .max_work_units = parsed.value.max_work_units,
                .status = &.{},
                .created_at_ns = parsed.value.created_at_ns,
                .updated_at_ns = parsed.value.updated_at_ns,
                .attempts = parsed.value.attempts,
                .completed = parsed.value.completed,
                .failure_reason = null,
                .stale_generation = parsed.value.stale_generation,
                .pass_count = parsed.value.pass_count,
                .last_units_queued = parsed.value.last_units_queued,
                .last_units_running = parsed.value.last_units_running,
                .last_units_throttled = parsed.value.last_units_throttled,
                .last_units_completed = parsed.value.last_units_completed,
                .total_units_queued = parsed.value.total_units_queued,
                .total_units_running = parsed.value.total_units_running,
                .total_units_throttled = parsed.value.total_units_throttled,
                .total_units_completed = parsed.value.total_units_completed,
            };
            errdefer self.freeRelationalIndexDropJobRecord(cloned);
            cloned.job_id = try self.alloc.dupe(u8, parsed.value.job_id);
            cloned.database_name = try self.alloc.dupe(u8, parsed.value.database_name);
            cloned.namespace_name = try self.alloc.dupe(u8, parsed.value.namespace_name);
            cloned.table_name = try self.alloc.dupe(u8, parsed.value.table_name);
            cloned.access_method = try self.alloc.dupe(u8, parsed.value.access_method);
            cloned.index_name = try self.alloc.dupe(u8, parsed.value.index_name);
            cloned.worker_id = try self.alloc.dupe(u8, parsed.value.worker_id);
            cloned.cursor = try self.alloc.dupe(u8, parsed.value.cursor);
            cloned.status = try self.alloc.dupe(u8, parsed.value.status);
            if (parsed.value.failure_reason) |value| cloned.failure_reason = try self.alloc.dupe(u8, value);
            return cloned;
        }

        pub fn cloneForeignKeyActionJobRecordFromJson(self: *DB, raw: []const u8) !ForeignKeyActionJobRecord {
            var parsed = try std.json.parseFromSlice(ForeignKeyActionJobRecord, self.alloc, raw, .{
                .allocate = .alloc_always,
                .ignore_unknown_fields = true,
            });
            defer parsed.deinit();
            try validateForeignKeyActionLineage(parsed.value.cascade_depth, parsed.value.cascade_max_depth);

            var cloned = ForeignKeyActionJobRecord{
                .version = parsed.value.version,
                .job_id = &.{},
                .action = &.{},
                .worker_id = &.{},
                .constraint_name = &.{},
                .parent_table = &.{},
                .parent_key = &.{},
                .updated_parent_key = null,
                .page_limit = parsed.value.page_limit,
                .status = &.{},
                .created_at_ns = parsed.value.created_at_ns,
                .updated_at_ns = parsed.value.updated_at_ns,
                .claimed_at_ns = parsed.value.claimed_at_ns,
                .lease_until_ns = parsed.value.lease_until_ns,
                .attempts = parsed.value.attempts,
                .completed = parsed.value.completed,
                .applied_children = parsed.value.applied_children,
                .failure_count = parsed.value.failure_count,
                .first_failed_at_ns = parsed.value.first_failed_at_ns,
                .last_failed_at_ns = parsed.value.last_failed_at_ns,
                .requeue_count = parsed.value.requeue_count,
                .last_requeued_at_ns = parsed.value.last_requeued_at_ns,
                .cascade_depth = parsed.value.cascade_depth,
                .cascade_max_depth = parsed.value.cascade_max_depth,
                .next_child_table = null,
                .next_child_key = null,
                .last_error = null,
            };
            errdefer self.freeForeignKeyActionJobRecord(cloned);
            cloned.job_id = try self.alloc.dupe(u8, parsed.value.job_id);
            cloned.action = try self.alloc.dupe(u8, parsed.value.action);
            cloned.worker_id = try self.alloc.dupe(u8, parsed.value.worker_id);
            cloned.constraint_name = try self.alloc.dupe(u8, parsed.value.constraint_name);
            cloned.parent_table = try self.alloc.dupe(u8, parsed.value.parent_table);
            cloned.parent_key = try self.alloc.dupe(u8, parsed.value.parent_key);
            if (parsed.value.updated_parent_key) |value| cloned.updated_parent_key = try self.alloc.dupe(u8, value);
            cloned.status = try self.alloc.dupe(u8, parsed.value.status);
            if (parsed.value.next_child_table) |value| cloned.next_child_table = try self.alloc.dupe(u8, value);
            if (parsed.value.next_child_key) |value| cloned.next_child_key = try self.alloc.dupe(u8, value);
            if (parsed.value.last_error) |value| cloned.last_error = try self.alloc.dupe(u8, value);
            return cloned;
        }

        pub fn cloneForeignKeyActionJobRecordOwned(self: *DB, record: ForeignKeyActionJobRecord) !ForeignKeyActionJobRecord {
            const payload = try std.json.Stringify.valueAlloc(self.alloc, record, .{ .emit_null_optional_fields = false });
            defer self.alloc.free(payload);
            return try cloneForeignKeyActionJobRecordFromJson(self, payload);
        }

        pub fn cloneForeignKeyActionScheduleRecordOwned(self: *DB, record: ForeignKeyActionScheduleRecord) !ForeignKeyActionScheduleRecord {
            const payload = try std.json.Stringify.valueAlloc(self.alloc, record, .{ .emit_null_optional_fields = false });
            defer self.alloc.free(payload);
            return try cloneForeignKeyActionScheduleRecordFromJson(self, payload);
        }

        pub fn cloneForeignKeyActionScheduleRecordFromJson(self: *DB, raw: []const u8) !ForeignKeyActionScheduleRecord {
            var parsed = try std.json.parseFromSlice(ForeignKeyActionScheduleRecord, self.alloc, raw, .{
                .allocate = .alloc_always,
                .ignore_unknown_fields = true,
            });
            defer parsed.deinit();
            try validateForeignKeyActionLineage(parsed.value.cascade_depth, parsed.value.cascade_max_depth);

            var cloned = ForeignKeyActionScheduleRecord{
                .version = parsed.value.version,
                .schedule_id = &.{},
                .action_job_id = &.{},
                .action = &.{},
                .worker_id = &.{},
                .constraint_name = &.{},
                .parent_table = &.{},
                .parent_key = &.{},
                .updated_parent_key = null,
                .page_limit = parsed.value.page_limit,
                .status = &.{},
                .created_at_ns = parsed.value.created_at_ns,
                .updated_at_ns = parsed.value.updated_at_ns,
                .completed = parsed.value.completed,
                .scheduled_groups = parsed.value.scheduled_groups,
                .cascade_depth = parsed.value.cascade_depth,
                .cascade_max_depth = parsed.value.cascade_max_depth,
                .requeue_count = parsed.value.requeue_count,
                .last_requeued_at_ns = parsed.value.last_requeued_at_ns,
                .last_error = null,
            };
            errdefer self.freeForeignKeyActionScheduleRecord(cloned);
            cloned.schedule_id = try self.alloc.dupe(u8, parsed.value.schedule_id);
            cloned.action_job_id = try self.alloc.dupe(u8, parsed.value.action_job_id);
            cloned.action = try self.alloc.dupe(u8, parsed.value.action);
            cloned.worker_id = try self.alloc.dupe(u8, parsed.value.worker_id);
            cloned.constraint_name = try self.alloc.dupe(u8, parsed.value.constraint_name);
            cloned.parent_table = try self.alloc.dupe(u8, parsed.value.parent_table);
            cloned.parent_key = try self.alloc.dupe(u8, parsed.value.parent_key);
            if (parsed.value.updated_parent_key) |value| cloned.updated_parent_key = try self.alloc.dupe(u8, value);
            cloned.status = try self.alloc.dupe(u8, parsed.value.status);
            if (parsed.value.last_error) |value| cloned.last_error = try self.alloc.dupe(u8, value);
            return cloned;
        }

        pub fn metadataPrefixUpperAlloc(alloc: Allocator, prefix: []const u8) !?[]u8 {
            var out = try alloc.dupe(u8, prefix);
            errdefer alloc.free(out);
            var i = out.len;
            while (i > 0) {
                i -= 1;
                if (out[i] != 0xff) {
                    out[i] += 1;
                    return try alloc.realloc(out, i + 1);
                }
            }
            alloc.free(out);
            return null;
        }

        pub fn foreignKeyIntegrityModeName(mode: relational_store_mod.ForeignKeyIntegrityMode) []const u8 {
            return switch (mode) {
                .validate => "validate",
                .dry_run => "dry_run",
                .repair => "repair",
            };
        }

        pub fn foreignKeyActionJobCanonicalAction(action: []const u8) ?[]const u8 {
            if (enumTokenEql(action, "set_null") or enumTokenEql(action, "delete_set_null") or enumTokenEql(action, "on_delete_set_null")) return "set_null";
            if (enumTokenEql(action, "cascade") or enumTokenEql(action, "delete_cascade") or enumTokenEql(action, "on_delete_cascade")) return "cascade";
            if (enumTokenEql(action, "update_set_null") or enumTokenEql(action, "on_update_set_null")) return "update_set_null";
            if (enumTokenEql(action, "update_cascade") or enumTokenEql(action, "on_update_cascade")) return "update_cascade";
            return null;
        }

        pub fn foreignKeyActionJobActionSupported(action: []const u8) bool {
            return foreignKeyActionJobCanonicalAction(action) != null;
        }

        pub fn foreignKeyActionJobIsUpdate(action: []const u8) bool {
            const canonical_action = foreignKeyActionJobCanonicalAction(action) orelse return false;
            return std.mem.eql(u8, canonical_action, "update_set_null") or std.mem.eql(u8, canonical_action, "update_cascade");
        }

        pub fn enumTokenEql(actual: []const u8, expected: []const u8) bool {
            var actual_index: usize = 0;
            var expected_index: usize = 0;
            while (true) {
                while (actual_index < actual.len and enumTokenSeparator(actual[actual_index])) actual_index += 1;
                while (expected_index < expected.len and enumTokenSeparator(expected[expected_index])) expected_index += 1;
                if (actual_index == actual.len or expected_index == expected.len) break;
                if (std.ascii.toLower(actual[actual_index]) != std.ascii.toLower(expected[expected_index])) return false;
                actual_index += 1;
                expected_index += 1;
            }
            while (actual_index < actual.len and enumTokenSeparator(actual[actual_index])) actual_index += 1;
            while (expected_index < expected.len and enumTokenSeparator(expected[expected_index])) expected_index += 1;
            return actual_index == actual.len and expected_index == expected.len;
        }

        pub fn enumTokenSeparator(ch: u8) bool {
            return ch == ' ' or ch == '_' or ch == '-';
        }

        pub fn foreignKeyActionUpdatedParentKeyMatches(existing: ?[]const u8, expected: ?[]const u8) bool {
            if (existing) |existing_value| {
                const expected_value = expected orelse return false;
                return std.mem.eql(u8, existing_value, expected_value);
            }
            return expected == null;
        }

        pub fn validateForeignKeyActionLineage(cascade_depth: u32, cascade_max_depth: u32) !void {
            if (cascade_max_depth == 0 or cascade_depth > cascade_max_depth) {
                return error.InvalidForeignKeyActionJob;
            }
        }

        pub fn validateForeignKeyActionJobIdentity(
            job_id: []const u8,
            action: []const u8,
            worker_id: []const u8,
            constraint_name: []const u8,
            parent_table: []const u8,
            parent_key: []const u8,
            updated_parent_key: ?[]const u8,
        ) !void {
            if (job_id.len == 0 or action.len == 0 or worker_id.len == 0 or constraint_name.len == 0 or parent_table.len == 0 or parent_key.len == 0) {
                return error.InvalidForeignKeyActionJob;
            }
            const canonical_action = foreignKeyActionJobCanonicalAction(action) orelse return error.InvalidForeignKeyActionJob;
            if (std.mem.eql(u8, canonical_action, "update_cascade")) {
                if (updated_parent_key == null or updated_parent_key.?.len == 0) return error.InvalidForeignKeyActionJob;
            } else if (!foreignKeyActionJobIsUpdate(canonical_action) and updated_parent_key != null) {
                return error.InvalidForeignKeyActionJob;
            }
        }

        pub fn validateForeignKeyActionJobMatches(
            existing: ForeignKeyActionJobRecord,
            action: []const u8,
            constraint_name: []const u8,
            parent_table: []const u8,
            parent_key: []const u8,
            updated_parent_key: ?[]const u8,
        ) !void {
            const canonical_action = foreignKeyActionJobCanonicalAction(action) orelse return error.InvalidForeignKeyActionJob;
            if (!std.mem.eql(u8, existing.action, canonical_action) or
                !std.mem.eql(u8, existing.constraint_name, constraint_name) or
                !std.mem.eql(u8, existing.parent_table, parent_table) or
                !std.mem.eql(u8, existing.parent_key, parent_key) or
                !foreignKeyActionUpdatedParentKeyMatches(existing.updated_parent_key, updated_parent_key))
            {
                return error.InvalidForeignKeyActionJob;
            }
        }

        pub fn foreignKeyActionJobClaimsMatch(current: ForeignKeyActionJobRecord, claimed: ForeignKeyActionJobRecord) bool {
            return !current.completed and
                std.mem.eql(u8, current.status, "claimed") and
                std.mem.eql(u8, current.worker_id, claimed.worker_id) and
                current.claimed_at_ns == claimed.claimed_at_ns and
                current.lease_until_ns == claimed.lease_until_ns and
                current.attempts == claimed.attempts;
        }

        pub fn validateForeignKeyActionScheduleMatches(
            existing: ForeignKeyActionScheduleRecord,
            action_job_id: []const u8,
            action: []const u8,
            constraint_name: []const u8,
            parent_table: []const u8,
            parent_key: []const u8,
            updated_parent_key: ?[]const u8,
        ) !void {
            const canonical_action = foreignKeyActionJobCanonicalAction(action) orelse return error.InvalidForeignKeyActionJob;
            if (!std.mem.eql(u8, existing.action_job_id, action_job_id) or
                !std.mem.eql(u8, existing.action, canonical_action) or
                !std.mem.eql(u8, existing.constraint_name, constraint_name) or
                !std.mem.eql(u8, existing.parent_table, parent_table) or
                !std.mem.eql(u8, existing.parent_key, parent_key) or
                !foreignKeyActionUpdatedParentKeyMatches(existing.updated_parent_key, updated_parent_key))
            {
                return error.InvalidForeignKeyActionJob;
            }
        }

        pub fn foreignKeyIntegrityProgressValid(
            mode: relational_store_mod.ForeignKeyIntegrityMode,
            report: relational_store_mod.ForeignKeyIntegrityReport,
        ) bool {
            return switch (mode) {
                .validate, .dry_run => report.valid(),
                .repair => report.missing_parent_rows == 0,
            };
        }

        pub fn foreignKeyIntegrityReportHasViolations(report: relational_store_mod.ForeignKeyIntegrityReport) bool {
            return report.missing_parent_rows != 0 or
                report.missing_ref_rows != 0 or
                report.stale_ref_rows != 0;
        }

        pub fn foreignKeyIntegrityReportAdd(
            left: relational_store_mod.ForeignKeyIntegrityReport,
            right: relational_store_mod.ForeignKeyIntegrityReport,
        ) relational_store_mod.ForeignKeyIntegrityReport {
            return .{
                .scanned_child_rows = left.scanned_child_rows +| right.scanned_child_rows,
                .referenced_child_rows = left.referenced_child_rows +| right.referenced_child_rows,
                .scanned_ref_rows = left.scanned_ref_rows +| right.scanned_ref_rows,
                .missing_parent_rows = left.missing_parent_rows +| right.missing_parent_rows,
                .missing_ref_rows = left.missing_ref_rows +| right.missing_ref_rows,
                .stale_ref_rows = left.stale_ref_rows +| right.stale_ref_rows,
                .repaired_ref_rows = left.repaired_ref_rows +| right.repaired_ref_rows,
                .deleted_stale_ref_rows = left.deleted_stale_ref_rows +| right.deleted_stale_ref_rows,
            };
        }

        pub fn foreignKeyIntegrityFirstViolationAt(existing: ?u64, pass_has_violations: bool, now_ns: u64) ?u64 {
            if (existing) |timestamp| return timestamp;
            return if (pass_has_violations) now_ns else null;
        }

        pub fn uniqueConstraintIntegrityProgressValid(
            mode: relational_store_mod.ForeignKeyIntegrityMode,
            report: relational_store_mod.UniqueConstraintIntegrityReport,
        ) bool {
            return switch (mode) {
                .validate, .dry_run => report.valid(),
                .repair => report.duplicate_unique_rows == 0,
            };
        }
    };
}

test "db schema apply supports unvalidated foreign key then enforced validation flip" {
    const db_mod = @import("mod.zig");
    const DB = db_mod.DB;

    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false });
    defer db.close();

    const schema_v1 =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"customer_id":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}}}
    ;
    const schema_unvalidated =
        \\{"version":2,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"customer_id":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"foreign_keys":[{"name":"orders_customer_id_fkey","columns":["customer_id"],"references":{"table":"customers","columns":["_id"]},"on_delete":"restrict","validation_state":"unvalidated"}]}
    ;
    const schema_enforced =
        \\{"version":3,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"customer_id":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"foreign_keys":[{"name":"orders_customer_id_fkey","columns":["customer_id"],"references":{"table":"customers","columns":["_id"]},"on_delete":"restrict","validation_state":"enforced"}]}
    ;

    try db.applyTableSchemaJson(alloc, schema_v1, .{});
    try db.batch(.{
        .writes = &.{.{ .key = "order:orphan", .value = "{\"id\":\"order:orphan\",\"customer_id\":\"customer:missing\"}" }},
    });

    try db.applyTableSchemaJson(alloc, schema_unvalidated, .{});
    try db.batch(.{
        .writes = &.{.{ .key = "order:still_unvalidated", .value = "{\"id\":\"order:still_unvalidated\",\"customer_id\":\"customer:also_missing\"}" }},
    });
    try db.batch(.{
        .deletes = &.{"customer:missing"},
    });
    const unvalidated_schema = db.core.schema orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(schema_mod.ForeignKeyValidationState.unvalidated, unvalidated_schema.foreign_keys[0].validation_state);

    try std.testing.expectError(error.ForeignKeyViolation, db.applyTableSchemaJson(alloc, schema_enforced, .{}));
    const after_failed_enforce = db.core.schema orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(schema_mod.ForeignKeyValidationState.unvalidated, after_failed_enforce.foreign_keys[0].validation_state);
    const failed_progress = (try db.loadForeignKeyIntegrityProgressRecord(.validate, null, "", "")) orelse return error.TestUnexpectedResult;
    defer db.freeForeignKeyIntegrityProgressRecord(failed_progress);
    try std.testing.expectEqualStrings("validate", failed_progress.mode);
    try std.testing.expect(failed_progress.completed);
    try std.testing.expect(!failed_progress.valid);
    try std.testing.expectEqual(@as(u64, 2), failed_progress.report.missing_parent_rows);

    try db.batch(.{
        .writes = &.{
            .{ .key = "customer:missing", .value = "{\"id\":\"customer:missing\"}" },
            .{ .key = "customer:also_missing", .value = "{\"id\":\"customer:also_missing\"}" },
        },
    });
    try db.applyTableSchemaJson(alloc, schema_enforced, .{});
    const enforced_schema = db.core.schema orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(schema_mod.ForeignKeyValidationState.enforced, enforced_schema.foreign_keys[0].validation_state);

    const fk_ref = try internal_keys.relationalForeignKeyRefKeyAlloc(alloc, "orders_customer_id_fkey", "customers", "customer:missing", "row", "order:orphan");
    defer alloc.free(fk_ref);
    const ref_value = try db.core.store.get(alloc, fk_ref);
    defer alloc.free(ref_value);
    try std.testing.expectEqualStrings("", ref_value);

    try std.testing.expectError(error.ForeignKeyViolation, db.batch(.{
        .writes = &.{.{ .key = "order:new_orphan", .value = "{\"id\":\"order:new_orphan\",\"customer_id\":\"customer:nope\"}" }},
    }));
    try std.testing.expectError(error.ForeignKeyViolation, db.batch(.{
        .deletes = &.{"customer:missing"},
    }));

    const local_schema_json = (try db.getSchemaJson(alloc)) orelse return error.TestUnexpectedResult;
    defer alloc.free(local_schema_json);
    try std.testing.expect(std.mem.indexOf(u8, local_schema_json, "\"validation_state\":\"enforced\"") != null);
}

test "db schema apply drops foreign key refs and stops enforcement" {
    const DB = @import("mod.zig").DB;

    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false });
    defer db.close();

    const schema_enforced =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"customer_id":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"foreign_keys":[{"name":"orders_customer_id_fkey","columns":["customer_id"],"references":{"table":"customers","columns":["_id"]},"on_delete":"restrict"}]}
    ;
    const schema_dropped =
        \\{"version":2,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"customer_id":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}}}
    ;

    try db.applyTableSchemaJson(alloc, schema_enforced, .{});
    try db.batch(.{
        .writes = &.{
            .{ .key = "customer:drop", .value = "{\"id\":\"customer:drop\"}" },
            .{ .key = "order:drop", .value = "{\"id\":\"order:drop\",\"customer_id\":\"customer:drop\"}" },
        },
    });

    const fk_ref = try internal_keys.relationalForeignKeyRefKeyAlloc(alloc, "orders_customer_id_fkey", "customers", "customer:drop", "row", "order:drop");
    defer alloc.free(fk_ref);
    const ref_value = try db.core.store.get(alloc, fk_ref);
    defer alloc.free(ref_value);
    try std.testing.expectEqualStrings("", ref_value);

    try db.applyTableSchemaJson(alloc, schema_dropped, .{});
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, fk_ref));

    try db.batch(.{
        .writes = &.{.{ .key = "order:orphan-after-drop", .value = "{\"id\":\"order:orphan-after-drop\",\"customer_id\":\"customer:missing-after-drop\"}" }},
    });
    try db.batch(.{
        .deletes = &.{"customer:drop"},
    });
    try std.testing.expect((try db.get(alloc, "customer:drop")) == null);

    const durable_schema = (try schema_mod.loadSchema(db.core.store, alloc)) orelse return error.TestUnexpectedResult;
    defer schema_mod.freeSchema(alloc, durable_schema);
    try std.testing.expectEqual(@as(usize, 0), durable_schema.foreign_keys.len);
}

test "db direct schema apply validates and builds added foreign keys" {
    const DB = @import("mod.zig").DB;

    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false });
    defer db.close();

    const schema_v1 =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"customer_id":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}}}
    ;
    const schema_v2 =
        \\{"version":2,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"customer_id":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"foreign_keys":[{"name":"orders_customer_id_fkey","columns":["customer_id"],"references":{"table":"customers","columns":["_id"]},"on_delete":"restrict"}]}
    ;

    try db.applyTableSchemaJson(alloc, schema_v1, .{});
    try db.batch(.{
        .writes = &.{
            .{ .key = "customer:a", .value = "{\"id\":\"customer:a\"}" },
            .{ .key = "order:1", .value = "{\"id\":\"order:1\",\"customer_id\":\"customer:a\"}" },
        },
    });

    try db.applyTableSchemaJson(alloc, schema_v2, .{});

    const fk_ref = try internal_keys.relationalForeignKeyRefKeyAlloc(alloc, "orders_customer_id_fkey", "customers", "customer:a", "row", "order:1");
    defer alloc.free(fk_ref);
    const ref_value = try db.core.store.get(alloc, fk_ref);
    defer alloc.free(ref_value);
    try std.testing.expectEqualStrings("", ref_value);

    try std.testing.expectError(error.ForeignKeyViolation, db.batch(.{
        .deletes = &.{"customer:a"},
    }));
}

test "db direct schema apply builds added unique constraints before foreign keys to unique tuples" {
    const DB = @import("mod.zig").DB;

    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false });
    defer db.close();

    const schema_v1 =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"},"ref_email":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}}}
    ;
    const schema_v2 =
        \\{"version":2,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"},"ref_email":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"unique_constraints":[{"name":"users_email_key","columns":["email"]}],"foreign_keys":[{"name":"users_ref_email_fkey","columns":["ref_email"],"references":{"table":"row","columns":["email"]},"on_delete":"restrict"}],"relational_indexes":[{"name":"users_email_key","owner_kind":"unique_constraint","owner_name":"users_email_key","access_method":"ordered_tuple","unique":true,"columns":["email"],"keys":[{"column":"email"}],"lifecycle":"ready","generation":7,"schema_fingerprint":"unique-index-v1:users_email_key","generation_record":{"generation":7,"owner_ranges":[],"lifecycle":"ready","lag":0,"ready_watermark":0}}]}
    ;

    try db.applyTableSchemaJson(alloc, schema_v1, .{});
    try db.batch(.{
        .writes = &.{
            .{ .key = "user:a", .value = "{\"id\":\"user:a\",\"email\":\"a@example.com\"}" },
            .{ .key = "user:b", .value = "{\"id\":\"user:b\",\"email\":\"b@example.com\",\"ref_email\":\"a@example.com\"}" },
        },
    });

    try db.applyTableSchemaJson(alloc, schema_v2, .{});

    const parent_tuple = try relational_store_mod.bytesTupleValueAlloc(alloc, &.{"a@example.com"});
    defer alloc.free(parent_tuple);
    const parent_row_key = try internal_keys.relationalRowKeyAlloc(alloc, "user:a");
    defer alloc.free(parent_row_key);
    const parent_row = try db.core.store.get(alloc, parent_row_key);
    defer alloc.free(parent_row);
    const users_email_index = relational_store_mod.relationalIndexForUniqueConstraint(db.core.schema.?.relational_indexes, db.core.schema.?.unique_constraints[0], .ordered_tuple) orelse return error.TestUnexpectedResult;
    const parent_ordered_tuple = try relational_store_mod.orderedTupleValueForIndexKeysAlloc(alloc, parent_row, users_email_index.keys, db.core.schema.?.relational_columns);
    defer alloc.free(parent_ordered_tuple);
    const unique_owners = try relational_store_mod.scanOrderedTupleDocKeysAlloc(alloc, db.core.store, "users_email_key", parent_ordered_tuple, "", "");
    defer relational_store_mod.freeDocKeys(alloc, unique_owners);
    try std.testing.expectEqual(@as(usize, 1), unique_owners.len);
    try std.testing.expectEqualStrings("user:a", unique_owners[0]);

    const fk_ref = try internal_keys.relationalForeignKeyRefKeyAlloc(alloc, "users_ref_email_fkey", "row", parent_tuple, "row", "user:b");
    defer alloc.free(fk_ref);
    const ref_value = try db.core.store.get(alloc, fk_ref);
    defer alloc.free(ref_value);
    try std.testing.expectEqualStrings("", ref_value);
}

test "db direct schema apply rejects added foreign keys with orphaned existing rows" {
    const DB = @import("mod.zig").DB;

    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false });
    defer db.close();

    const schema_v1 =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"customer_id":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}}}
    ;
    const schema_v2 =
        \\{"version":2,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"customer_id":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"foreign_keys":[{"name":"orders_customer_id_fkey","columns":["customer_id"],"references":{"table":"customers","columns":["_id"]},"on_delete":"restrict"}]}
    ;

    try db.applyTableSchemaJson(alloc, schema_v1, .{});
    try db.batch(.{
        .writes = &.{.{ .key = "order:orphan", .value = "{\"id\":\"order:orphan\",\"customer_id\":\"customer:missing\"}" }},
    });

    try std.testing.expectError(error.ForeignKeyViolation, db.applyTableSchemaJson(alloc, schema_v2, .{}));
    const durable_schema = (try schema_mod.loadSchema(db.core.store, alloc)) orelse return error.TestUnexpectedResult;
    defer schema_mod.freeSchema(alloc, durable_schema);
    try std.testing.expectEqual(@as(usize, 0), durable_schema.foreign_keys.len);
}

test "db foreign key integrity progress is durable per range" {
    const DB = @import("mod.zig").DB;
    const table_schema_api = @import("../../schema/mod.zig");

    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"customer_id":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"foreign_keys":[{"name":"orders_customer_id_fkey","columns":["customer_id"],"references":{"table":"customers","columns":["_id"]},"on_delete":"restrict"}]}
    ;
    var parsed_schema = try table_schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try table_schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    try db.batch(.{
        .writes = &.{
            .{ .key = "a:customer", .value = "{\"id\":\"a:customer\"}" },
            .{ .key = "a:order", .value = "{\"id\":\"a:order\",\"customer_id\":\"a:customer\"}" },
            .{ .key = "z:customer", .value = "{\"id\":\"z:customer\"}" },
            .{ .key = "z:order", .value = "{\"id\":\"z:order\",\"customer_id\":\"z:customer\"}" },
        },
    });

    const low_report = try db.validateForeignKeyRefsInRange("a:", "m:");
    try std.testing.expect(low_report.valid());
    try std.testing.expectEqual(@as(u64, 1), low_report.referenced_child_rows);
    const high_report = try db.validateForeignKeyRefsInRange("m:", "");
    try std.testing.expect(high_report.valid());
    try std.testing.expectEqual(@as(u64, 1), high_report.referenced_child_rows);

    const low_progress = (try db.loadForeignKeyIntegrityProgressRecord(.validate, null, "a:", "m:")) orelse return error.TestUnexpectedResult;
    defer db.freeForeignKeyIntegrityProgressRecord(low_progress);
    try std.testing.expectEqualStrings("validate", low_progress.mode);
    try std.testing.expectEqualStrings("a:", low_progress.lower_doc_key);
    try std.testing.expectEqualStrings("m:", low_progress.upper_doc_key);
    try std.testing.expect(low_progress.valid);
    try std.testing.expectEqual(@as(u64, 1), low_progress.report.referenced_child_rows);

    const high_progress = (try db.loadForeignKeyIntegrityProgressRecord(.validate, null, "m:", "")) orelse return error.TestUnexpectedResult;
    defer db.freeForeignKeyIntegrityProgressRecord(high_progress);
    try std.testing.expectEqualStrings("validate", high_progress.mode);
    try std.testing.expectEqualStrings("m:", high_progress.lower_doc_key);
    try std.testing.expectEqualStrings("", high_progress.upper_doc_key);
    try std.testing.expect(high_progress.valid);
    try std.testing.expectEqual(@as(u64, 1), high_progress.report.referenced_child_rows);
    try std.testing.expect((try db.loadForeignKeyIntegrityProgressRecord(.validate, null, "", "")) == null);

    const all_progress = try db.listForeignKeyIntegrityProgressRecords();
    defer db.freeForeignKeyIntegrityProgressRecords(all_progress);
    try std.testing.expectEqual(@as(usize, 2), all_progress.len);
    var saw_low = false;
    var saw_high = false;
    for (all_progress) |entry| {
        try std.testing.expectEqualStrings("validate", entry.mode);
        if (std.mem.eql(u8, entry.lower_doc_key, "a:") and std.mem.eql(u8, entry.upper_doc_key, "m:")) saw_low = true;
        if (std.mem.eql(u8, entry.lower_doc_key, "m:") and std.mem.eql(u8, entry.upper_doc_key, "")) saw_high = true;
    }
    try std.testing.expect(saw_low);
    try std.testing.expect(saw_high);

    const worker_report = try db.claimAndRunForeignKeyIntegrityWorkUnitAt(
        "claim:validate-all",
        "worker:validate",
        19,
        "child_range",
        .validate,
        null,
        "",
        "",
        60_000,
        100_000,
    );
    try std.testing.expect(worker_report.valid());
    try std.testing.expectEqual(@as(u64, 2), worker_report.referenced_child_rows);

    const worker_claim = (try db.loadForeignKeyIntegrityClaimRecord("claim:validate-all")) orelse return error.TestUnexpectedResult;
    defer db.freeForeignKeyIntegrityClaimRecord(worker_claim);
    try std.testing.expectEqualStrings("worker:validate", worker_claim.worker_id);
    try std.testing.expectEqualStrings("validate", worker_claim.planned_action);
    try std.testing.expectEqualStrings("", worker_claim.lower_doc_key);
    try std.testing.expectEqualStrings("", worker_claim.upper_doc_key);

    const worker_progress = (try db.loadForeignKeyIntegrityProgressRecord(.validate, null, "", "")) orelse return error.TestUnexpectedResult;
    defer db.freeForeignKeyIntegrityProgressRecord(worker_progress);
    try std.testing.expect(worker_progress.valid);
    try std.testing.expectEqual(@as(u64, 2), worker_progress.report.referenced_child_rows);
}

test "db foreign key integrity work claims are leased and durable" {
    const DB = @import("mod.zig").DB;

    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        const first = try db.claimForeignKeyIntegrityWorkUnitAt(
            "claim:a",
            "worker:a",
            17,
            "child_range",
            "validate",
            "orders_customer_id_fkey",
            "a:",
            "m:",
            1_000,
            10_000,
        );
        defer db.freeForeignKeyIntegrityClaimRecord(first);
        try std.testing.expectEqualStrings("claim:a", first.claim_key);
        try std.testing.expectEqualStrings("worker:a", first.worker_id);
        try std.testing.expectEqual(@as(u64, 17), first.group_id);
        try std.testing.expectEqualStrings("validate", first.planned_action);
        try std.testing.expectEqualStrings("orders_customer_id_fkey", first.constraint_name.?);
        try std.testing.expectEqual(@as(u32, 1), first.attempts);
        try std.testing.expect(first.lease_until_ns > first.claimed_at_ns);

        try std.testing.expectError(error.ForeignKeyIntegrityClaimBusy, db.claimForeignKeyIntegrityWorkUnitAt(
            "claim:a",
            "worker:b",
            17,
            "child_range",
            "validate",
            "orders_customer_id_fkey",
            "a:",
            "m:",
            1_000,
            20_000,
        ));

        const renewed = try db.claimForeignKeyIntegrityWorkUnitAt(
            "claim:a",
            "worker:a",
            17,
            "child_range",
            "validate",
            "orders_customer_id_fkey",
            "a:",
            "m:",
            1_000,
            30_000,
        );
        defer db.freeForeignKeyIntegrityClaimRecord(renewed);
        try std.testing.expectEqualStrings("worker:a", renewed.worker_id);
        try std.testing.expectEqual(@as(u32, 2), renewed.attempts);
        try std.testing.expect(renewed.claimed_at_ns > first.claimed_at_ns);

        const taken = try db.claimForeignKeyIntegrityWorkUnitAt(
            "claim:a",
            "worker:b",
            17,
            "child_range",
            "repair",
            null,
            "a:",
            "m:",
            1_000,
            renewed.lease_until_ns + 1,
        );
        defer db.freeForeignKeyIntegrityClaimRecord(taken);
        try std.testing.expectEqualStrings("worker:b", taken.worker_id);
        try std.testing.expectEqualStrings("repair", taken.planned_action);
        try std.testing.expect(taken.constraint_name == null);
        try std.testing.expectEqual(@as(u32, 3), taken.attempts);

        const loaded = (try db.loadForeignKeyIntegrityClaimRecord("claim:a")) orelse return error.TestUnexpectedResult;
        defer db.freeForeignKeyIntegrityClaimRecord(loaded);
        try std.testing.expectEqualStrings("worker:b", loaded.worker_id);
        try std.testing.expectEqualStrings("repair", loaded.planned_action);

        const all = try db.listForeignKeyIntegrityClaimRecords();
        defer db.freeForeignKeyIntegrityClaimRecords(all);
        try std.testing.expectEqual(@as(usize, 1), all.len);
        try std.testing.expectEqualStrings("claim:a", all[0].claim_key);
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{});
    defer reopened.close();
    const persisted = (try reopened.loadForeignKeyIntegrityClaimRecord("claim:a")) orelse return error.TestUnexpectedResult;
    defer reopened.freeForeignKeyIntegrityClaimRecord(persisted);
    try std.testing.expectEqualStrings("worker:b", persisted.worker_id);
    try std.testing.expectEqualStrings("repair", persisted.planned_action);
    try std.testing.expectEqual(@as(u32, 3), persisted.attempts);
}

test "db foreign key maintenance leases use durable realtime clock" {
    const DB = @import("mod.zig").DB;

    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const before_claim_ns = currentTimeNs();
    const claim = try db.claimForeignKeyIntegrityWorkUnit(
        "claim:realtime",
        "worker:realtime",
        17,
        "child_range",
        "validate",
        "orders_customer_id_fkey",
        "",
        "",
        60_000,
    );
    defer db.freeForeignKeyIntegrityClaimRecord(claim);
    try std.testing.expect(claim.claimed_at_ns >= before_claim_ns);
    try std.testing.expect(claim.lease_until_ns > currentTimeNs());

    const before_action_ns = currentTimeNs();
    const action = try db.scheduleForeignKeyActionJob(
        "fk-action:realtime",
        "set_null",
        "worker:realtime",
        "orders_customer_id_fkey",
        "customers",
        "customer:realtime",
        16,
    );
    defer db.freeForeignKeyActionJobRecord(action);
    try std.testing.expect(action.created_at_ns >= before_action_ns);
    try std.testing.expectEqual(@as(u64, 0), action.lease_until_ns);

    const before_action_claim_ns = currentTimeNs();
    const claimed_action = try db.claimForeignKeyActionJobPage(
        "fk-action:realtime:claim",
        "set_null",
        "worker:realtime",
        "orders_customer_id_fkey",
        "customers",
        "customer:realtime",
        16,
        60_000,
    );
    defer db.freeForeignKeyActionJobRecord(claimed_action);
    try std.testing.expect(claimed_action.claimed_at_ns >= before_action_claim_ns);
    try std.testing.expect(claimed_action.lease_until_ns > currentTimeNs());
}

test "db relational index repair job records persist intent progress and resume cursor" {
    const DB = @import("mod.zig").DB;

    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        const created = try db.upsertRelationalIndexRepairJobRecordAt(
            "job:index-repair:docs",
            "tenant_ops",
            "analytics",
            "docs",
            "worker:index-repair",
            "",
            "",
            60_000,
            2,
            "running",
            10_000,
        );
        defer db.freeRelationalIndexRepairJobRecord(created);
        try std.testing.expectEqualStrings("job:index-repair:docs", created.job_id);
        try std.testing.expectEqualStrings("tenant_ops", created.database_name);
        try std.testing.expectEqualStrings("analytics", created.namespace_name);
        try std.testing.expectEqualStrings("docs", created.table_name);
        try std.testing.expectEqualStrings("worker:index-repair", created.worker_id);
        try std.testing.expectEqual(@as(u64, 60_000), created.lease_ms);
        try std.testing.expectEqual(@as(usize, 2), created.max_work_units);
        try std.testing.expectEqualStrings("running", created.status);
        try std.testing.expectEqual(@as(u64, 10_000), created.created_at_ns);
        try std.testing.expectEqual(@as(u64, 10_000), created.updated_at_ns);
        try std.testing.expectEqual(@as(u32, 1), created.attempts);
        try std.testing.expect(!created.completed);
        try std.testing.expect(created.complete == null);
        try std.testing.expectEqualStrings("", created.next_lower_doc_key);

        const partial = try db.recordRelationalIndexRepairJobPassAt(
            "job:index-repair:docs",
            "running",
            false,
            2,
            1,
            0,
            "row:m",
            .{
                .scanned_rows = 7,
                .indexed_rows = 6,
                .deleted_orphan_entries = 1,
                .written_entries = 9,
            },
            null,
            20_000,
        );
        defer db.freeRelationalIndexRepairJobRecord(partial);
        try std.testing.expect(!partial.completed);
        try std.testing.expect(!partial.complete.?);
        try std.testing.expectEqualStrings("row:m", partial.next_lower_doc_key);
        try std.testing.expectEqual(@as(u64, 2), partial.last_ranges_scanned);
        try std.testing.expectEqual(@as(u64, 1), partial.last_ranges_repaired);
        try std.testing.expectEqual(@as(u64, 2), partial.total_ranges_scanned);
        try std.testing.expectEqual(@as(u64, 1), partial.total_ranges_repaired);
        try std.testing.expectEqual(@as(u64, 7), partial.last_report.scanned_rows);
        try std.testing.expectEqual(@as(u64, 7), partial.aggregate_report.scanned_rows);

        const resumed = try db.upsertRelationalIndexRepairJobRecordAt(
            "job:index-repair:docs",
            "tenant_ops",
            "analytics",
            "docs",
            "worker:index-repair-2",
            "",
            "",
            120_000,
            4,
            "running",
            30_000,
        );
        defer db.freeRelationalIndexRepairJobRecord(resumed);
        try std.testing.expectEqual(@as(u64, 10_000), resumed.created_at_ns);
        try std.testing.expectEqual(@as(u64, 30_000), resumed.updated_at_ns);
        try std.testing.expectEqual(@as(u32, 2), resumed.attempts);
        try std.testing.expectEqualStrings("worker:index-repair-2", resumed.worker_id);
        try std.testing.expectEqual(@as(u64, 120_000), resumed.lease_ms);
        try std.testing.expectEqual(@as(usize, 4), resumed.max_work_units);
        try std.testing.expectEqualStrings("row:m", resumed.next_lower_doc_key);
        try std.testing.expectEqual(@as(u64, 7), resumed.aggregate_report.scanned_rows);

        const completed = try db.recordRelationalIndexRepairJobPassAt(
            "job:index-repair:docs",
            "complete",
            true,
            1,
            1,
            0,
            "",
            .{
                .scanned_rows = 5,
                .indexed_rows = 4,
                .deleted_orphan_entries = 0,
                .written_entries = 8,
            },
            "last-pass-warning",
            40_000,
        );
        defer db.freeRelationalIndexRepairJobRecord(completed);
        try std.testing.expect(completed.completed);
        try std.testing.expect(completed.complete.?);
        try std.testing.expectEqualStrings("complete", completed.status);
        try std.testing.expectEqual(@as(u64, 1), completed.last_ranges_scanned);
        try std.testing.expectEqual(@as(u64, 3), completed.total_ranges_scanned);
        try std.testing.expectEqual(@as(u64, 2), completed.total_ranges_repaired);
        try std.testing.expectEqual(@as(u64, 12), completed.aggregate_report.scanned_rows);
        try std.testing.expectEqual(@as(u64, 10), completed.aggregate_report.indexed_rows);
        try std.testing.expectEqual(@as(u64, 1), completed.aggregate_report.deleted_orphan_entries);
        try std.testing.expectEqual(@as(u64, 17), completed.aggregate_report.written_entries);
        try std.testing.expectEqualStrings("last-pass-warning", completed.last_error.?);

        const all = try db.listRelationalIndexRepairJobRecords();
        defer db.freeRelationalIndexRepairJobRecords(all);
        try std.testing.expectEqual(@as(usize, 1), all.len);
        try std.testing.expectEqualStrings("job:index-repair:docs", all[0].job_id);
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{});
    defer reopened.close();
    const persisted = (try reopened.loadRelationalIndexRepairJobRecord("job:index-repair:docs")) orelse return error.TestUnexpectedResult;
    defer reopened.freeRelationalIndexRepairJobRecord(persisted);
    try std.testing.expect(persisted.completed);
    try std.testing.expect(persisted.complete.?);
    try std.testing.expectEqualStrings("complete", persisted.status);
    try std.testing.expectEqualStrings("tenant_ops", persisted.database_name);
    try std.testing.expectEqualStrings("analytics", persisted.namespace_name);
    try std.testing.expectEqualStrings("docs", persisted.table_name);
    try std.testing.expectEqual(@as(u32, 2), persisted.attempts);
    try std.testing.expectEqual(@as(u64, 3), persisted.total_ranges_scanned);
    try std.testing.expectEqual(@as(u64, 2), persisted.total_ranges_repaired);
    try std.testing.expectEqual(@as(u64, 12), persisted.aggregate_report.scanned_rows);
    try std.testing.expectEqualStrings("last-pass-warning", persisted.last_error.?);
}

test "db relational index target repair job records persist generation counters and stale state" {
    const DB = @import("mod.zig").DB;

    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        const created = try db.upsertRelationalIndexRepairJobTargetAt(
            "job:index-repair:docs:body_text:7",
            "tenant_ops",
            "analytics",
            "docs",
            "text_search",
            "body_text",
            7,
            "worker:repair-a",
            "",
            60_000,
            3,
            "running",
            10_000,
        );
        defer db.freeRelationalIndexRepairJobRecord(created);
        try std.testing.expectEqualStrings("text_search", created.access_method);
        try std.testing.expectEqualStrings("body_text", created.index_name);
        try std.testing.expectEqual(@as(u64, 7), created.generation);
        try std.testing.expectEqual(@as(u32, 1), created.attempts);
        try std.testing.expectEqualStrings("", created.cursor);

        const partial = try db.recordRelationalIndexRepairJobTargetPassAt(
            "job:index-repair:docs:body_text:7",
            "running",
            false,
            "row:m",
            5,
            1,
            2,
            3,
            null,
            false,
            20_000,
        );
        defer db.freeRelationalIndexRepairJobRecord(partial);
        try std.testing.expect(!partial.completed);
        try std.testing.expectEqualStrings("row:m", partial.cursor);
        try std.testing.expectEqual(@as(u64, 1), partial.pass_count);
        try std.testing.expectEqual(@as(u64, 5), partial.total_units_queued);
        try std.testing.expectEqual(@as(u64, 2), partial.total_units_throttled);
        try std.testing.expectEqual(@as(u64, 3), partial.total_units_completed);

        const resumed = try db.upsertRelationalIndexRepairJobTargetAt(
            "job:index-repair:docs:body_text:7",
            "tenant_ops",
            "analytics",
            "docs",
            "text_search",
            "body_text",
            7,
            "worker:repair-b",
            "",
            120_000,
            4,
            "running",
            30_000,
        );
        defer db.freeRelationalIndexRepairJobRecord(resumed);
        try std.testing.expectEqual(@as(u32, 2), resumed.attempts);
        try std.testing.expectEqualStrings("row:m", resumed.cursor);
        try std.testing.expectEqual(@as(u64, 3), resumed.total_units_completed);

        try std.testing.expectError(
            error.StaleRelationalIndexRepairJobGeneration,
            db.upsertRelationalIndexRepairJobTargetAt(
                "job:index-repair:docs:body_text:7",
                "tenant_ops",
                "analytics",
                "docs",
                "text_search",
                "body_text",
                8,
                "worker:repair-c",
                "",
                120_000,
                4,
                "running",
                35_000,
            ),
        );

        const stale = try db.recordRelationalIndexRepairJobTargetPassAt(
            "job:index-repair:docs:body_text:7",
            "stale_generation",
            true,
            "row:z",
            0,
            0,
            0,
            0,
            "target_generation_stale",
            true,
            40_000,
        );
        defer db.freeRelationalIndexRepairJobRecord(stale);
        try std.testing.expect(stale.completed);
        try std.testing.expect(stale.stale_generation);
        try std.testing.expectEqualStrings("target_generation_stale", stale.failure_reason.?);

        const success_created = try db.upsertRelationalIndexRepairJobTargetAt(
            "job:index-repair:docs:body_text:9",
            "tenant_ops",
            "analytics",
            "docs",
            "text_search",
            "body_text",
            9,
            "worker:repair-success",
            "",
            60_000,
            3,
            "running",
            50_000,
        );
        defer db.freeRelationalIndexRepairJobRecord(success_created);
        const success = try db.recordRelationalIndexRepairJobTargetPassAt(
            "job:index-repair:docs:body_text:9",
            "complete",
            true,
            "",
            1,
            1,
            0,
            1,
            null,
            false,
            60_000,
        );
        defer db.freeRelationalIndexRepairJobRecord(success);
        try std.testing.expect(success.completed);
        try std.testing.expect(!success.stale_generation);
        try std.testing.expectEqual(@as(u64, 1), success.total_units_completed);

        const failed_created = try db.upsertRelationalIndexRepairJobTargetAt(
            "job:index-repair:docs:body_text:10",
            "tenant_ops",
            "analytics",
            "docs",
            "text_search",
            "body_text",
            10,
            "worker:repair-failed",
            "",
            60_000,
            3,
            "running",
            70_000,
        );
        defer db.freeRelationalIndexRepairJobRecord(failed_created);
        const failed = try db.recordRelationalIndexRepairJobTargetPassAt(
            "job:index-repair:docs:body_text:10",
            "failed",
            false,
            "row:f",
            1,
            0,
            0,
            0,
            "injected_failure",
            false,
            80_000,
        );
        defer db.freeRelationalIndexRepairJobRecord(failed);
        try std.testing.expect(!failed.completed);
        try std.testing.expectEqualStrings("failed", failed.status);

        const all = try db.listRelationalIndexRepairJobRecords();
        defer db.freeRelationalIndexRepairJobRecords(all);
        try std.testing.expectEqual(@as(usize, 3), all.len);

        const stats = try db.stats(alloc);
        defer types.freeDBStats(alloc, stats);
        try std.testing.expectEqual(@as(u64, 3), stats.relational_index_repair.job_count);
        try std.testing.expectEqual(@as(u64, 1), stats.relational_index_repair.active_job_count);
        try std.testing.expectEqual(@as(u64, 2), stats.relational_index_repair.completed_job_count);
        try std.testing.expectEqual(@as(u64, 1), stats.relational_index_repair.failed_job_count);
        try std.testing.expectEqual(@as(u64, 1), stats.relational_index_repair.stale_generation_job_count);
        try std.testing.expectEqual(@as(u64, 7), stats.relational_index_repair.total_units_queued);
        try std.testing.expectEqual(@as(u64, 2), stats.relational_index_repair.total_units_running);
        try std.testing.expectEqual(@as(u64, 2), stats.relational_index_repair.total_units_throttled);
        try std.testing.expectEqual(@as(u64, 4), stats.relational_index_repair.total_units_completed);
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{});
    defer reopened.close();
    const persisted = (try reopened.loadRelationalIndexRepairJobRecord("job:index-repair:docs:body_text:7")) orelse return error.TestUnexpectedResult;
    defer reopened.freeRelationalIndexRepairJobRecord(persisted);
    try std.testing.expectEqualStrings("text_search", persisted.access_method);
    try std.testing.expectEqualStrings("body_text", persisted.index_name);
    try std.testing.expectEqual(@as(u64, 7), persisted.generation);
    try std.testing.expectEqual(@as(u32, 2), persisted.attempts);
    try std.testing.expectEqual(@as(u64, 2), persisted.pass_count);
    try std.testing.expectEqual(@as(u64, 3), persisted.total_units_completed);
    try std.testing.expect(persisted.stale_generation);
    try std.testing.expectEqualStrings("target_generation_stale", persisted.failure_reason.?);
}

test "db relational index drop job records persist generation counters and stale state" {
    const DB = @import("mod.zig").DB;

    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        const created = try db.upsertRelationalIndexDropJobRecordAt(
            "job:index-drop:docs:alg_docs:11",
            "tenant_ops",
            "analytics",
            "docs",
            "algebraic_filter",
            "alg_docs",
            11,
            "worker:drop-a",
            "",
            60_000,
            2,
            "running",
            10_000,
        );
        defer db.freeRelationalIndexDropJobRecord(created);
        try std.testing.expectEqualStrings("algebraic_filter", created.access_method);
        try std.testing.expectEqualStrings("alg_docs", created.index_name);
        try std.testing.expectEqual(@as(u64, 11), created.generation);

        const partial = try db.recordRelationalIndexDropJobPassAt(
            "job:index-drop:docs:alg_docs:11",
            "running",
            false,
            "artifact:postings:m",
            4,
            1,
            1,
            2,
            null,
            false,
            20_000,
        );
        defer db.freeRelationalIndexDropJobRecord(partial);
        try std.testing.expectEqualStrings("artifact:postings:m", partial.cursor);
        try std.testing.expectEqual(@as(u64, 1), partial.pass_count);
        try std.testing.expectEqual(@as(u64, 2), partial.total_units_completed);

        const resumed = try db.upsertRelationalIndexDropJobRecordAt(
            "job:index-drop:docs:alg_docs:11",
            "tenant_ops",
            "analytics",
            "docs",
            "algebraic_filter",
            "alg_docs",
            11,
            "worker:drop-b",
            "",
            60_000,
            2,
            "running",
            30_000,
        );
        defer db.freeRelationalIndexDropJobRecord(resumed);
        try std.testing.expectEqual(@as(u32, 2), resumed.attempts);
        try std.testing.expectEqualStrings("artifact:postings:m", resumed.cursor);

        try std.testing.expectError(
            error.StaleRelationalIndexDropJobGeneration,
            db.upsertRelationalIndexDropJobRecordAt(
                "job:index-drop:docs:alg_docs:11",
                "tenant_ops",
                "analytics",
                "docs",
                "algebraic_filter",
                "alg_docs",
                12,
                "worker:drop-c",
                "",
                60_000,
                2,
                "running",
                35_000,
            ),
        );

        const complete = try db.recordRelationalIndexDropJobPassAt(
            "job:index-drop:docs:alg_docs:11",
            "complete",
            true,
            "",
            0,
            0,
            0,
            3,
            null,
            false,
            40_000,
        );
        defer db.freeRelationalIndexDropJobRecord(complete);
        try std.testing.expect(complete.completed);
        try std.testing.expectEqual(@as(u64, 5), complete.total_units_completed);

        const all = try db.listRelationalIndexDropJobRecords();
        defer db.freeRelationalIndexDropJobRecords(all);
        try std.testing.expectEqual(@as(usize, 1), all.len);
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{});
    defer reopened.close();
    const persisted = (try reopened.loadRelationalIndexDropJobRecord("job:index-drop:docs:alg_docs:11")) orelse return error.TestUnexpectedResult;
    defer reopened.freeRelationalIndexDropJobRecord(persisted);
    try std.testing.expectEqualStrings("algebraic_filter", persisted.access_method);
    try std.testing.expectEqualStrings("alg_docs", persisted.index_name);
    try std.testing.expectEqual(@as(u64, 11), persisted.generation);
    try std.testing.expectEqual(@as(u32, 2), persisted.attempts);
    try std.testing.expectEqual(@as(u64, 2), persisted.pass_count);
    try std.testing.expectEqual(@as(u64, 5), persisted.total_units_completed);
    try std.testing.expect(persisted.completed);
}

test "db foreign key action jobs canonicalize SQL action aliases" {
    const DB = @import("mod.zig").DB;

    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const scheduled = try db.scheduleForeignKeyActionJobAt(
        "fk-action:sql-set-null",
        "ON DELETE SET NULL",
        "worker:fk-action:scheduler",
        "orders_customer_id_fkey",
        "customers",
        "customer:sql",
        16,
        100_000,
    );
    defer db.freeForeignKeyActionJobRecord(scheduled);
    try std.testing.expectEqualStrings("set_null", scheduled.action);

    const scheduled_again = try db.scheduleForeignKeyActionJobAt(
        "fk-action:sql-set-null",
        "set-null",
        "worker:fk-action:scheduler",
        "orders_customer_id_fkey",
        "customers",
        "customer:sql",
        16,
        100_001,
    );
    defer db.freeForeignKeyActionJobRecord(scheduled_again);
    try std.testing.expectEqualStrings("set_null", scheduled_again.action);
    try std.testing.expectEqual(@as(u64, scheduled.created_at_ns), scheduled_again.created_at_ns);

    const claimed = try db.claimForeignKeyActionJobPageAt(
        "fk-action:sql-set-null",
        "ON DELETE SET NULL",
        "worker:fk-action:claimer",
        "orders_customer_id_fkey",
        "customers",
        "customer:sql",
        16,
        60_000,
        100_010,
    );
    defer db.freeForeignKeyActionJobRecord(claimed);
    try std.testing.expectEqualStrings("set_null", claimed.action);
    try std.testing.expectEqualStrings("claimed", claimed.status);

    const update_scheduled = try db.scheduleForeignKeyActionJobWithUpdatedParentKeyAt(
        "fk-action:sql-update-cascade",
        "ON UPDATE CASCADE",
        "worker:fk-action:scheduler",
        "users_ref_email_fkey",
        "users",
        "email:old",
        "email:new",
        8,
        100_020,
    );
    defer db.freeForeignKeyActionJobRecord(update_scheduled);
    try std.testing.expectEqualStrings("update_cascade", update_scheduled.action);
    try std.testing.expectEqualStrings("email:new", update_scheduled.updated_parent_key.?);

    const schedule = try db.scheduleForeignKeyActionScheduleAt(
        "fk-action-schedule:sql-set-null",
        "fk-action:sql-set-null-scheduled",
        "SET NULL",
        "worker:fk-action:scheduler",
        "orders_customer_id_fkey",
        "customers",
        "customer:scheduled",
        32,
        100_030,
    );
    defer db.freeForeignKeyActionScheduleRecord(schedule);
    try std.testing.expectEqualStrings("set_null", schedule.action);

    const schedule_again = try db.scheduleForeignKeyActionScheduleAt(
        "fk-action-schedule:sql-set-null",
        "fk-action:sql-set-null-scheduled",
        "set-null",
        "worker:fk-action:scheduler",
        "orders_customer_id_fkey",
        "customers",
        "customer:scheduled",
        32,
        100_031,
    );
    defer db.freeForeignKeyActionScheduleRecord(schedule_again);
    try std.testing.expectEqualStrings("set_null", schedule_again.action);
    try std.testing.expectEqual(@as(u64, schedule.created_at_ns), schedule_again.created_at_ns);
}

test "db foreign key action schedule records zero-owner seed failures durably" {
    const DB = @import("mod.zig").DB;

    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const scheduled = try db.scheduleForeignKeyActionSchedule(
        "fk-action-schedule:set-null:no-owners",
        "fk-action:set-null:no-owners",
        "set_null",
        "worker:scheduler",
        "orders_customer_id_fkey",
        "customers",
        "customer:no-owners",
        128,
    );
    defer db.freeForeignKeyActionScheduleRecord(scheduled);

    try std.testing.expectError(error.InvalidForeignKeyActionJob, db.requeueForeignKeyActionScheduleAt(
        "fk-action-schedule:set-null:no-owners",
        "fk-action:set-null:no-owners",
        "set_null",
        "worker:retry",
        "orders_customer_id_fkey",
        "customers",
        "customer:no-owners",
        64,
        31_999,
    ));

    const failed = try db.markForeignKeyActionScheduleSeededAt(
        "fk-action-schedule:set-null:no-owners",
        0,
        32_000,
    );
    defer db.freeForeignKeyActionScheduleRecord(failed);
    try std.testing.expectEqualStrings("invalid", failed.status);
    try std.testing.expect(!failed.completed);
    try std.testing.expectEqual(@as(u64, 0), failed.scheduled_groups);
    try std.testing.expect(failed.last_error != null);
    try std.testing.expectEqualStrings("NoForeignKeyActionOwnerGroups", failed.last_error.?);
    try std.testing.expectEqual(@as(u64, 0), failed.requeue_count);
    try std.testing.expect(failed.last_requeued_at_ns == null);

    try std.testing.expectError(error.InvalidForeignKeyActionJob, db.markForeignKeyActionScheduleSeededAt(
        "fk-action-schedule:set-null:no-owners",
        2,
        32_000,
    ));

    try std.testing.expectError(error.InvalidForeignKeyActionJob, db.requeueForeignKeyActionScheduleAt(
        "fk-action-schedule:set-null:no-owners",
        "fk-action:set-null:no-owners",
        "cascade",
        "worker:scheduler",
        "orders_customer_id_fkey",
        "customers",
        "customer:no-owners",
        64,
        32_000,
    ));

    const requeued = try db.requeueForeignKeyActionScheduleAt(
        "fk-action-schedule:set-null:no-owners",
        "fk-action:set-null:no-owners",
        "ON DELETE SET NULL",
        "worker:retry",
        "orders_customer_id_fkey",
        "customers",
        "customer:no-owners",
        64,
        32_001,
    );
    defer db.freeForeignKeyActionScheduleRecord(requeued);
    try std.testing.expectEqualStrings("pending", requeued.status);
    try std.testing.expect(!requeued.completed);
    try std.testing.expectEqualStrings("worker:retry", requeued.worker_id);
    try std.testing.expectEqual(@as(usize, 64), requeued.page_limit);
    try std.testing.expectEqual(@as(u64, 0), requeued.scheduled_groups);
    try std.testing.expect(requeued.last_error == null);
    try std.testing.expectEqual(@as(u64, 1), requeued.requeue_count);
    try std.testing.expectEqual(@as(u64, 32_001), requeued.last_requeued_at_ns.?);

    const retried = try db.markForeignKeyActionScheduleSeededAt(
        "fk-action-schedule:set-null:no-owners",
        2,
        32_002,
    );
    defer db.freeForeignKeyActionScheduleRecord(retried);
    try std.testing.expectEqualStrings("seeded", retried.status);
    try std.testing.expect(retried.completed);
    try std.testing.expectEqual(@as(u64, 2), retried.scheduled_groups);
    try std.testing.expect(retried.last_error == null);
    try std.testing.expectEqual(@as(u64, 1), retried.requeue_count);
    try std.testing.expectEqual(@as(u64, 32_001), retried.last_requeued_at_ns.?);

    try std.testing.expectError(error.InvalidForeignKeyActionJob, db.requeueForeignKeyActionScheduleAt(
        "fk-action-schedule:set-null:no-owners",
        "fk-action:set-null:no-owners",
        "set_null",
        "worker:retry",
        "orders_customer_id_fkey",
        "customers",
        "customer:no-owners",
        64,
        32_003,
    ));
}

test "db foreign key action job records page execution failures" {
    const DB = @import("mod.zig").DB;
    const table_schema_api = @import("../../schema/mod.zig");

    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"customer_id":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"foreign_keys":[{"name":"orders_customer_id_fkey","columns":["customer_id"],"references":{"table":"customers","columns":["_id"]},"on_delete":"set_null"}]}
    ;
    var parsed_schema = try table_schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try table_schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    try db.batch(.{
        .writes = &.{
            .{ .key = "customer:bad-action", .value = "{\"id\":\"customer:bad-action\"}" },
            .{ .key = "order:bad-action", .value = "{\"id\":\"order:bad-action\",\"customer_id\":\"customer:bad-action\",\"status\":\"open\"}" },
        },
    });

    try std.testing.expectError(error.ForeignKeyViolation, db.claimAndRunForeignKeyActionJobPageAt(
        "fk-action:cascade:set-null-schema",
        "cascade",
        "worker:fk-action:bad",
        "orders_customer_id_fkey",
        "customers",
        "customer:bad-action",
        8,
        60_000,
        300_000,
    ));

    const failed = (try db.loadForeignKeyActionJobRecord("fk-action:cascade:set-null-schema")) orelse return error.TestUnexpectedResult;
    defer db.freeForeignKeyActionJobRecord(failed);
    try std.testing.expectEqualStrings("invalid", failed.status);
    try std.testing.expect(!failed.completed);
    try std.testing.expectEqual(@as(u64, 0), failed.applied_children);
    try std.testing.expect(failed.last_error != null);
    try std.testing.expectEqualStrings("ForeignKeyViolation", failed.last_error.?);
}

test "db foreign key integrity job records persist intent and completion" {
    const DB = @import("mod.zig").DB;

    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        const created = try db.upsertForeignKeyIntegrityJobRecordAt(
            "job:fk:orders:validate",
            "orders",
            "validate",
            "worker:fk-job",
            "orders_customer_id_fkey",
            "a:",
            "z:",
            60_000,
            4,
            "running",
            10_000,
        );
        defer db.freeForeignKeyIntegrityJobRecord(created);
        try std.testing.expectEqualStrings("job:fk:orders:validate", created.job_id);
        try std.testing.expectEqualStrings("orders", created.table_name);
        try std.testing.expectEqualStrings("validate", created.action);
        try std.testing.expectEqualStrings("worker:fk-job", created.worker_id);
        try std.testing.expectEqualStrings("orders_customer_id_fkey", created.constraint_name.?);
        try std.testing.expectEqualStrings("a:", created.lower_doc_key);
        try std.testing.expectEqualStrings("z:", created.upper_doc_key);
        try std.testing.expectEqual(@as(u64, 60_000), created.lease_ms);
        try std.testing.expectEqual(@as(usize, 4), created.max_work_units);
        try std.testing.expectEqualStrings("running", created.status);
        try std.testing.expectEqual(@as(u64, 10_000), created.created_at_ns);
        try std.testing.expectEqual(@as(u64, 10_000), created.updated_at_ns);
        try std.testing.expectEqual(@as(u32, 1), created.attempts);
        try std.testing.expect(!created.completed);
        try std.testing.expect(created.valid == null);
        try std.testing.expectEqualStrings("[]", created.violation_samples_json);
        try std.testing.expectEqual(@as(usize, 0), created.violation_sample_count);
        try std.testing.expect(!created.violations_truncated);
        try std.testing.expectEqual(@as(u64, 0), created.aggregate_report.missing_parent_rows);
        try std.testing.expectEqual(@as(u64, 0), created.aggregate_report.missing_ref_rows);
        try std.testing.expectEqual(@as(u64, 0), created.aggregate_report.stale_ref_rows);
        try std.testing.expectEqual(@as(u64, 0), created.diagnostic_passes);
        try std.testing.expectEqual(@as(u64, 0), created.violating_passes);
        try std.testing.expect(created.first_violation_at_ns == null);
        try std.testing.expect(created.last_violation_at_ns == null);

        const resumed = try db.upsertForeignKeyIntegrityJobRecordAt(
            "job:fk:orders:validate",
            "orders",
            "validate",
            "worker:fk-job-2",
            "orders_customer_id_fkey",
            "a:",
            "z:",
            120_000,
            8,
            "running",
            20_000,
        );
        defer db.freeForeignKeyIntegrityJobRecord(resumed);
        try std.testing.expectEqual(@as(u64, 10_000), resumed.created_at_ns);
        try std.testing.expectEqual(@as(u64, 20_000), resumed.updated_at_ns);
        try std.testing.expectEqual(@as(u32, 2), resumed.attempts);
        try std.testing.expectEqualStrings("worker:fk-job-2", resumed.worker_id);
        try std.testing.expectEqual(@as(u64, 120_000), resumed.lease_ms);
        try std.testing.expectEqual(@as(usize, 8), resumed.max_work_units);

        const diagnosed = try db.updateForeignKeyIntegrityJobDiagnosticsWithReportAt(
            "job:fk:orders:validate",
            .{ .missing_parent_rows = 1 },
            "[{\"kind\":\"missing_parent\",\"child_key\":\"order:1\"}]",
            1,
            true,
            25_000,
        );
        defer db.freeForeignKeyIntegrityJobRecord(diagnosed);
        try std.testing.expectEqualStrings("[{\"kind\":\"missing_parent\",\"child_key\":\"order:1\"}]", diagnosed.violation_samples_json);
        try std.testing.expectEqual(@as(usize, 1), diagnosed.violation_sample_count);
        try std.testing.expect(diagnosed.violations_truncated);
        try std.testing.expectEqual(@as(u64, 1), diagnosed.diagnostic_passes);
        try std.testing.expectEqual(@as(u64, 1), diagnosed.violating_passes);
        try std.testing.expectEqual(@as(u64, 25_000), diagnosed.first_violation_at_ns.?);
        try std.testing.expectEqual(@as(u64, 25_000), diagnosed.last_violation_at_ns.?);
        try std.testing.expectEqual(@as(u64, 1), diagnosed.last_report.missing_parent_rows);
        try std.testing.expectEqual(@as(u64, 1), diagnosed.aggregate_report.missing_parent_rows);

        const completed = try db.completeForeignKeyIntegrityJobRecordAt(
            "job:fk:orders:validate",
            "complete",
            true,
            .{ .referenced_child_rows = 12 },
            30_000,
        );
        defer db.freeForeignKeyIntegrityJobRecord(completed);
        try std.testing.expect(completed.completed);
        try std.testing.expect(completed.valid.?);
        try std.testing.expectEqualStrings("complete", completed.status);
        try std.testing.expectEqual(@as(u64, 12), completed.last_report.referenced_child_rows);
        try std.testing.expectEqual(@as(u64, 12), completed.aggregate_report.referenced_child_rows);
        try std.testing.expectEqual(@as(u64, 1), completed.aggregate_report.missing_parent_rows);
        try std.testing.expectEqualStrings("[{\"kind\":\"missing_parent\",\"child_key\":\"order:1\"}]", completed.violation_samples_json);
        try std.testing.expectEqual(@as(usize, 1), completed.violation_sample_count);
        try std.testing.expect(completed.violations_truncated);
        try std.testing.expectEqual(@as(u64, 1), completed.diagnostic_passes);
        try std.testing.expectEqual(@as(u64, 1), completed.violating_passes);
        try std.testing.expectEqual(@as(u64, 25_000), completed.first_violation_at_ns.?);
        try std.testing.expectEqual(@as(u64, 25_000), completed.last_violation_at_ns.?);

        const all = try db.listForeignKeyIntegrityJobRecords();
        defer db.freeForeignKeyIntegrityJobRecords(all);
        try std.testing.expectEqual(@as(usize, 1), all.len);
        try std.testing.expectEqualStrings("job:fk:orders:validate", all[0].job_id);
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{});
    defer reopened.close();
    const persisted = (try reopened.loadForeignKeyIntegrityJobRecord("job:fk:orders:validate")) orelse return error.TestUnexpectedResult;
    defer reopened.freeForeignKeyIntegrityJobRecord(persisted);
    try std.testing.expect(persisted.completed);
    try std.testing.expect(persisted.valid.?);
    try std.testing.expectEqualStrings("complete", persisted.status);
    try std.testing.expectEqual(@as(u32, 2), persisted.attempts);
    try std.testing.expectEqual(@as(u64, 12), persisted.last_report.referenced_child_rows);
    try std.testing.expectEqual(@as(u64, 12), persisted.aggregate_report.referenced_child_rows);
    try std.testing.expectEqual(@as(u64, 1), persisted.aggregate_report.missing_parent_rows);
    try std.testing.expectEqualStrings("[{\"kind\":\"missing_parent\",\"child_key\":\"order:1\"}]", persisted.violation_samples_json);
    try std.testing.expectEqual(@as(usize, 1), persisted.violation_sample_count);
    try std.testing.expect(persisted.violations_truncated);
    try std.testing.expectEqual(@as(u64, 1), persisted.diagnostic_passes);
    try std.testing.expectEqual(@as(u64, 1), persisted.violating_passes);
    try std.testing.expectEqual(@as(u64, 25_000), persisted.first_violation_at_ns.?);
    try std.testing.expectEqual(@as(u64, 25_000), persisted.last_violation_at_ns.?);
}

test "db direct schema apply validates and builds added unique constraints" {
    const DB = @import("mod.zig").DB;

    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false });
    defer db.close();

    const schema_v1 =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}}}
    ;
    const schema_v2 =
        \\{"version":2,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"unique_constraints":[{"name":"users_email_key","columns":["email"]}]}
    ;

    try db.applyTableSchemaJson(alloc, schema_v1, .{});
    try db.batch(.{
        .writes = &.{
            .{ .key = "user:a", .value = "{\"id\":\"user:a\",\"email\":\"a@example.com\"}" },
            .{ .key = "user:b", .value = "{\"id\":\"user:b\",\"email\":\"b@example.com\"}" },
        },
    });

    try db.applyTableSchemaJson(alloc, schema_v2, .{});

    const row_a = try relational_store_mod.getRawAlloc(alloc, db.core.store, "user:a") orelse return error.TestExpectedEqual;
    defer alloc.free(row_a);
    const runtime_schema = db.core.schema orelse return error.TestExpectedEqual;
    try expectDedicatedUniqueEntryAndNoOrderedOwner(alloc, db.core.store, runtime_schema, "users_email_key", row_a, "user:a");

    try std.testing.expectError(error.UniqueConstraintViolation, db.batch(.{
        .writes = &.{.{ .key = "user:c", .value = "{\"id\":\"user:c\",\"email\":\"a@example.com\"}" }},
    }));
}

test "db direct schema apply rejects added unique constraints with duplicate existing rows" {
    const DB = @import("mod.zig").DB;

    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false });
    defer db.close();

    const schema_v1 =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}}}
    ;
    const schema_v2 =
        \\{"version":2,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"unique_constraints":[{"name":"users_email_key","columns":["email"]}]}
    ;

    try db.applyTableSchemaJson(alloc, schema_v1, .{});
    try db.batch(.{
        .writes = &.{
            .{ .key = "user:a", .value = "{\"id\":\"user:a\",\"email\":\"same@example.com\"}" },
            .{ .key = "user:b", .value = "{\"id\":\"user:b\",\"email\":\"same@example.com\"}" },
        },
    });

    try std.testing.expectError(error.UniqueConstraintViolation, db.applyTableSchemaJson(alloc, schema_v2, .{}));
    const durable_schema = (try schema_mod.loadSchema(db.core.store, alloc)) orelse return error.TestUnexpectedResult;
    defer schema_mod.freeSchema(alloc, durable_schema);
    try std.testing.expectEqual(@as(usize, 0), durable_schema.unique_constraints.len);
}

test "relational expression partial predicates maintain indexes and ordered unique tuples" {
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"},"status":{"type":"keyword"},"name":{"type":"keyword"}},"required":["id","email","status","name"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"unique_constraints":[{"name":"users_active_email_key","columns":["email"],"where_expressions":[{"lhs":{"op":"lower","args":[{"field":"status"}]},"op":"eq","rhs":{"value":"active"}}]}],"relational_indexes":[{"name":"email","owner_kind":"relational_column","owner_name":"email","access_method":"scalar_column","columns":["email"],"where_expressions":[{"lhs":{"op":"lower","args":[{"field":"status"}]},"op":"eq","rhs":{"value":"active"}}]}]}
    ;
    var parsed_schema = try table_schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try table_schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    try db.batch(.{
        .writes = &.{
            .{ .key = "user:active", .value = "{\"id\":\"u1\",\"email\":\"shared@example.test\",\"status\":\"ACTIVE\",\"name\":\"Active\"}" },
            .{ .key = "user:inactive", .value = "{\"id\":\"u2\",\"email\":\"shared@example.test\",\"status\":\"inactive\",\"name\":\"Inactive\"}" },
        },
        .sync_level = .write,
    });

    const active_index_key = try internal_keys.relationalColumnIndexKeyAlloc(alloc, "email", "user:active");
    defer alloc.free(active_index_key);
    const active_index = try db.core.store.get(alloc, active_index_key);
    defer alloc.free(active_index);

    const inactive_index_key = try internal_keys.relationalColumnIndexKeyAlloc(alloc, "email", "user:inactive");
    defer alloc.free(inactive_index_key);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, inactive_index_key));

    const active_row = try relational_store_mod.getRawAlloc(alloc, db.core.store, "user:active") orelse return error.TestExpectedEqual;
    defer alloc.free(active_row);
    try expectDedicatedUniqueEntryAndNoOrderedOwner(alloc, db.core.store, runtime_schema, "users_active_email_key", active_row, "user:active");

    const inactive_row = try relational_store_mod.getRawAlloc(alloc, db.core.store, "user:inactive") orelse return error.TestExpectedEqual;
    defer alloc.free(inactive_row);
    if (try relational_store_mod.uniqueConstraintTupleValueAlloc(alloc, inactive_row, runtime_schema.unique_constraints[0])) |inactive_tuple| {
        alloc.free(inactive_tuple);
        return error.TestExpectedEqual;
    }

    const lower_status_operands = [_]types.RelationalRowsExpression{.{
        .kind = .field,
        .field = "status",
    }};
    const active_expression_rhs = [_]types.RelationalRowsExpression{.{
        .kind = .value,
        .value_json = "\"active\"",
    }};
    const active_expression_predicates = [_]types.RelationalRowsExpressionCondition{.{
        .lhs = .{
            .kind = .lower,
            .operands = lower_status_operands[0..],
        },
        .op = .eq,
        .rhs = active_expression_rhs[0..],
    }};
    const email_column = runtime_schema.relational_columns[1];
    try std.testing.expect(!(try db.searchRuntimeRelationalColumnIndexUsableForQuery(alloc, runtime_schema, email_column, .{
        .predicates = &.{},
        .expressions = &.{},
    })));
    try std.testing.expect(try db.searchRuntimeRelationalColumnIndexUsableForQuery(alloc, runtime_schema, email_column, .{
        .predicates = &.{},
        .expressions = active_expression_predicates[0..],
    }));
    const semantic_index_predicates = [_]schema_mod.RelationalCheck{
        .{
            .name = "",
            .field = "email",
            .op = .eq,
            .value_json = "\"shared@example.test\"",
        },
        .{
            .name = "",
            .field = "status",
            .op = .eq,
            .value_json = "\"ACTIVE\"",
        },
    };
    try std.testing.expect(try db.searchRuntimeRelationalColumnIndexUsableForQuery(alloc, runtime_schema, email_column, .{
        .predicates = semantic_index_predicates[0..],
        .expressions = &.{},
    }));

    try db.core.store.delete(active_index_key);
    const email_predicates = [_]schema_mod.RelationalCheck{.{
        .name = "",
        .field = "email",
        .op = .eq,
        .value_json = "\"shared@example.test\"",
    }};
    const select = [_][]const u8{ "id", "status" };
    var active_query = try db.queryRelationalRows(alloc, runtime_schema, .{
        .predicates = email_predicates[0..],
        .expression_predicates = active_expression_predicates[0..],
        .select = select[0..],
        .select_all = false,
    });
    defer active_query.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 1), active_query.total);
    try std.testing.expectEqual(@as(usize, 1), active_query.rows.len);
    try std.testing.expectEqualStrings("{\"id\":\"u1\",\"status\":\"ACTIVE\"}", active_query.rows[0]);

    var semantic_query = try db.queryRelationalRows(alloc, runtime_schema, .{
        .predicates = semantic_index_predicates[0..],
        .select = select[0..],
        .select_all = false,
    });
    defer semantic_query.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 1), semantic_query.total);
    try std.testing.expectEqual(@as(usize, 1), semantic_query.rows.len);
    try std.testing.expectEqualStrings("{\"id\":\"u1\",\"status\":\"ACTIVE\"}", semantic_query.rows[0]);
}

test "relational text expression partial predicates maintain indexes and ordered unique tuples" {
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"},"status":{"type":"keyword"},"tag":{"type":"keyword"}},"required":["id","email","status","tag"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"unique_constraints":[{"name":"users_hot_email_key","columns":["email"],"where_expressions":[{"lhs":{"op":"replace","args":[{"field":"tag"},{"value":"-"},{"value":""}]},"op":"eq","rhs":{"value":"hot"}},{"lhs":{"op":"regexp_replace","args":[{"field":"status"},{"value":"[0-9]+"},{"value":"#"},{"value":"g"}]},"op":"eq","rhs":{"value":"active-#"}}]}],"relational_indexes":[{"name":"email","owner_kind":"relational_column","owner_name":"email","access_method":"scalar_column","columns":["email"],"where_expressions":[{"lhs":{"op":"split_part","args":[{"field":"status"},{"value":"-"},{"value":1}]},"op":"eq","rhs":{"value":"active"}},{"lhs":{"op":"regexp_replace","args":[{"field":"status"},{"value":"[0-9]+"},{"value":"#"},{"value":"g"}]},"op":"eq","rhs":{"value":"active-#"}},{"lhs":{"op":"starts_with","args":[{"field":"tag"},{"value":"h"}]},"op":"eq","rhs":{"value":true}}]}]}
    ;
    var parsed_schema = try table_schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try table_schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    try db.batch(.{
        .writes = &.{
            .{ .key = "user:active", .value = "{\"id\":\"u1\",\"email\":\"shared@example.test\",\"status\":\"active-2026\",\"tag\":\"h-ot\"}" },
            .{ .key = "user:inactive", .value = "{\"id\":\"u2\",\"email\":\"shared@example.test\",\"status\":\"inactive-open\",\"tag\":\"cold\"}" },
        },
        .sync_level = .write,
    });

    const active_index_key = try internal_keys.relationalColumnIndexKeyAlloc(alloc, "email", "user:active");
    defer alloc.free(active_index_key);
    const active_index = try db.core.store.get(alloc, active_index_key);
    defer alloc.free(active_index);

    const inactive_index_key = try internal_keys.relationalColumnIndexKeyAlloc(alloc, "email", "user:inactive");
    defer alloc.free(inactive_index_key);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, inactive_index_key));

    const active_row = try relational_store_mod.getRawAlloc(alloc, db.core.store, "user:active") orelse return error.TestExpectedEqual;
    defer alloc.free(active_row);
    try expectDedicatedUniqueEntryAndNoOrderedOwner(alloc, db.core.store, runtime_schema, "users_hot_email_key", active_row, "user:active");

    const inactive_row = try relational_store_mod.getRawAlloc(alloc, db.core.store, "user:inactive") orelse return error.TestExpectedEqual;
    defer alloc.free(inactive_row);
    if (try relational_store_mod.uniqueConstraintTupleValueAlloc(alloc, inactive_row, runtime_schema.unique_constraints[0])) |inactive_tuple| {
        alloc.free(inactive_tuple);
        return error.TestExpectedEqual;
    }
}

test "relational scalar expression partial predicates maintain indexes and ordered unique tuples" {
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"},"status":{"type":"keyword"},"amount":{"type":"numeric"},"bonus":{"type":"numeric"},"cap":{"type":"numeric"},"rank_text":{"type":"keyword"}},"required":["id","email","status","amount","bonus","cap","rank_text"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"unique_constraints":[{"name":"users_active_email_key","columns":["email"],"where_expressions":[{"lhs":{"op":"nullif","args":[{"field":"status"},{"value":"blocked"}]},"op":"is_not_null"},{"lhs":{"op":"mod","args":[{"field":"amount"},{"value":2}]},"op":"eq","rhs":{"value":0}}]}],"relational_indexes":[{"name":"email","owner_kind":"relational_column","owner_name":"email","access_method":"scalar_column","columns":["email"],"where_expressions":[{"lhs":{"op":"add","args":[{"field":"amount"},{"field":"bonus"}]},"op":"gte","rhs":{"value":10}},{"lhs":{"op":"cast","to":"numeric","args":[{"field":"rank_text"}]},"op":"eq","rhs":{"value":6}},{"lhs":{"op":"greatest","args":[{"field":"amount"},{"field":"bonus"}]},"op":"gte","rhs":{"value":6}},{"lhs":{"op":"least","args":[{"field":"amount"},{"field":"cap"}]},"op":"lte","rhs":{"value":5}}]}]}
    ;
    var parsed_schema = try table_schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try table_schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    try db.batch(.{
        .writes = &.{
            .{ .key = "user:active", .value = "{\"id\":\"u1\",\"email\":\"shared@example.test\",\"status\":\"active\",\"amount\":6,\"bonus\":4,\"cap\":5,\"rank_text\":\"6\"}" },
            .{ .key = "user:inactive", .value = "{\"id\":\"u2\",\"email\":\"shared@example.test\",\"status\":\"blocked\",\"amount\":3,\"bonus\":1,\"cap\":5,\"rank_text\":\"3\"}" },
        },
        .sync_level = .write,
    });

    const active_index_key = try internal_keys.relationalColumnIndexKeyAlloc(alloc, "email", "user:active");
    defer alloc.free(active_index_key);
    const active_index = try db.core.store.get(alloc, active_index_key);
    defer alloc.free(active_index);

    const inactive_index_key = try internal_keys.relationalColumnIndexKeyAlloc(alloc, "email", "user:inactive");
    defer alloc.free(inactive_index_key);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, inactive_index_key));

    const active_row = try relational_store_mod.getRawAlloc(alloc, db.core.store, "user:active") orelse return error.TestExpectedEqual;
    defer alloc.free(active_row);
    try expectDedicatedUniqueEntryAndNoOrderedOwner(alloc, db.core.store, runtime_schema, "users_active_email_key", active_row, "user:active");

    const inactive_row = try relational_store_mod.getRawAlloc(alloc, db.core.store, "user:inactive") orelse return error.TestExpectedEqual;
    defer alloc.free(inactive_row);
    if (try relational_store_mod.uniqueConstraintTupleValueAlloc(alloc, inactive_row, runtime_schema.unique_constraints[0])) |inactive_tuple| {
        alloc.free(inactive_tuple);
        return error.TestExpectedEqual;
    }
}

test "relational boolean and pattern expression partial predicates maintain indexes and ordered unique tuples" {
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"},"name":{"type":"keyword"},"status":{"type":"keyword"},"enabled":{"type":"boolean"},"archived":{"type":"boolean"}},"required":["id","email","name","status","enabled","archived"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"unique_constraints":[{"name":"users_visible_email_key","columns":["email"],"where_expressions":[{"lhs":{"op":"like","args":[{"field":"status"},{"value":"act%"}]},"op":"eq","rhs":{"value":true}},{"lhs":{"op":"bool_not","args":[{"field":"archived"}]},"op":"eq","rhs":{"value":true}}]}],"relational_indexes":[{"name":"email","owner_kind":"relational_column","owner_name":"email","access_method":"scalar_column","columns":["email"],"where_expressions":[{"lhs":{"op":"ilike","args":[{"field":"name"},{"value":"al%"}]},"op":"eq","rhs":{"value":true}},{"lhs":{"op":"bool_and","args":[{"field":"enabled"},{"value":true}]},"op":"eq","rhs":{"value":true}}]}]}
    ;
    var parsed_schema = try table_schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try table_schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    try db.batch(.{
        .writes = &.{
            .{ .key = "user:visible", .value = "{\"id\":\"u1\",\"email\":\"shared@example.test\",\"name\":\"Alice\",\"status\":\"active\",\"enabled\":true,\"archived\":false}" },
            .{ .key = "user:hidden", .value = "{\"id\":\"u2\",\"email\":\"shared@example.test\",\"name\":\"Bob\",\"status\":\"blocked\",\"enabled\":false,\"archived\":true}" },
        },
        .sync_level = .write,
    });

    const visible_index_key = try internal_keys.relationalColumnIndexKeyAlloc(alloc, "email", "user:visible");
    defer alloc.free(visible_index_key);
    const visible_index = try db.core.store.get(alloc, visible_index_key);
    defer alloc.free(visible_index);

    const hidden_index_key = try internal_keys.relationalColumnIndexKeyAlloc(alloc, "email", "user:hidden");
    defer alloc.free(hidden_index_key);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, hidden_index_key));

    const visible_row = try relational_store_mod.getRawAlloc(alloc, db.core.store, "user:visible") orelse return error.TestExpectedEqual;
    defer alloc.free(visible_row);
    try expectDedicatedUniqueEntryAndNoOrderedOwner(alloc, db.core.store, runtime_schema, "users_visible_email_key", visible_row, "user:visible");

    const hidden_row = try relational_store_mod.getRawAlloc(alloc, db.core.store, "user:hidden") orelse return error.TestExpectedEqual;
    defer alloc.free(hidden_row);
    if (try relational_store_mod.uniqueConstraintTupleValueAlloc(alloc, hidden_row, runtime_schema.unique_constraints[0])) |hidden_tuple| {
        alloc.free(hidden_tuple);
        return error.TestExpectedEqual;
    }
}

test "relational array and json object expression partial predicates maintain indexes and ordered unique tuples" {
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"},"tags":{"type":"array","items":{"type":"keyword"}},"scope":{"type":"keyword"},"metadata":{"type":"json"}},"required":["id","email","tags","scope","metadata"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"unique_constraints":[{"name":"users_array_json_email_key","columns":["email"],"where_expressions":[{"lhs":{"op":"array_to_string","args":[{"op":"array_replace","args":[{"field":"tags"},{"value":"hot"},{"value":"warm"}]},{"value":","}]},"op":"eq","rhs":{"value":"warm"}},{"lhs":{"op":"array_position","args":[{"op":"string_to_array","args":[{"field":"scope"},{"value":" "}]},{"value":"read"}]},"op":"eq","rhs":{"value":1}}]}],"relational_indexes":[{"name":"email","owner_kind":"relational_column","owner_name":"email","access_method":"scalar_column","columns":["email"],"where_expressions":[{"lhs":{"op":"array_position","args":[{"field":"tags"},{"value":"hot"}]},"op":"eq","rhs":{"value":1}},{"lhs":{"op":"array_to_string","args":[{"op":"array_append","args":[{"field":"tags"},{"value":"vip"}]},{"value":","}]},"op":"eq","rhs":{"value":"hot,vip"}},{"lhs":{"op":"json_build_object","args":[{"value":"source"},{"op":"json_extract","args":[{"field":"metadata"}],"path":"source","as_text":true}]},"op":"eq","rhs":{"value":{"source":"api"}}}]}]}
    ;
    var parsed_schema = try table_schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try table_schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    try db.batch(.{
        .writes = &.{
            .{ .key = "user:visible", .value = "{\"id\":\"u1\",\"email\":\"shared@example.test\",\"tags\":[\"hot\"],\"scope\":\"read write\",\"metadata\":{\"source\":\"api\"}}" },
            .{ .key = "user:hidden", .value = "{\"id\":\"u2\",\"email\":\"shared@example.test\",\"tags\":[\"cold\"],\"scope\":\"write\",\"metadata\":{\"source\":\"cli\"}}" },
        },
        .sync_level = .write,
    });

    const visible_index_key = try internal_keys.relationalColumnIndexKeyAlloc(alloc, "email", "user:visible");
    defer alloc.free(visible_index_key);
    const visible_index = try db.core.store.get(alloc, visible_index_key);
    defer alloc.free(visible_index);

    const hidden_index_key = try internal_keys.relationalColumnIndexKeyAlloc(alloc, "email", "user:hidden");
    defer alloc.free(hidden_index_key);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, hidden_index_key));

    const visible_row = try relational_store_mod.getRawAlloc(alloc, db.core.store, "user:visible") orelse return error.TestExpectedEqual;
    defer alloc.free(visible_row);
    try expectDedicatedUniqueEntryAndNoOrderedOwner(alloc, db.core.store, runtime_schema, "users_array_json_email_key", visible_row, "user:visible");

    const hidden_row = try relational_store_mod.getRawAlloc(alloc, db.core.store, "user:hidden") orelse return error.TestExpectedEqual;
    defer alloc.free(hidden_row);
    if (try relational_store_mod.uniqueConstraintTupleValueAlloc(alloc, hidden_row, runtime_schema.unique_constraints[0])) |hidden_tuple| {
        alloc.free(hidden_tuple);
        return error.TestExpectedEqual;
    }
}

test "relational temporal and case expression partial predicates maintain indexes and ordered unique tuples" {
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"},"status":{"type":"keyword"},"created_at_ns":{"type":"datetime"}},"required":["id","email","status","created_at_ns"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"unique_constraints":[{"name":"users_temporal_email_key","columns":["email"],"where_expressions":[{"lhs":{"op":"date_bin","args":[{"op":"interval_ns","args":[{"value":3600000000000}]},{"field":"created_at_ns"},{"value":0}]},"op":"eq","rhs":{"value":3600000000000}},{"lhs":{"op":"date_part","args":[{"value":"hour"},{"field":"created_at_ns"}]},"op":"eq","rhs":{"value":1}}]}],"relational_indexes":[{"name":"email","owner_kind":"relational_column","owner_name":"email","access_method":"scalar_column","columns":["email"],"where_expressions":[{"lhs":{"op":"date_trunc","args":[{"value":"hour"},{"field":"created_at_ns"}]},"op":"eq","rhs":{"value":3600000000000}},{"lhs":{"op":"case","cases":[{"when":{"lhs":{"field":"status"},"op":"eq","rhs":{"value":"active"}},"then":{"value":true}}],"else":{"value":false}},"op":"eq","rhs":{"value":true}}]}]}
    ;
    var parsed_schema = try table_schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try table_schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    try db.batch(.{
        .writes = &.{
            .{ .key = "user:visible", .value = "{\"id\":\"u1\",\"email\":\"shared@example.test\",\"status\":\"active\",\"created_at_ns\":3700000000000}" },
            .{ .key = "user:hidden", .value = "{\"id\":\"u2\",\"email\":\"shared@example.test\",\"status\":\"inactive\",\"created_at_ns\":7300000000000}" },
        },
        .sync_level = .write,
    });

    const visible_index_key = try internal_keys.relationalColumnIndexKeyAlloc(alloc, "email", "user:visible");
    defer alloc.free(visible_index_key);
    const visible_index = try db.core.store.get(alloc, visible_index_key);
    defer alloc.free(visible_index);

    const hidden_index_key = try internal_keys.relationalColumnIndexKeyAlloc(alloc, "email", "user:hidden");
    defer alloc.free(hidden_index_key);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, hidden_index_key));

    const visible_row = try relational_store_mod.getRawAlloc(alloc, db.core.store, "user:visible") orelse return error.TestExpectedEqual;
    defer alloc.free(visible_row);
    try expectDedicatedUniqueEntryAndNoOrderedOwner(alloc, db.core.store, runtime_schema, "users_temporal_email_key", visible_row, "user:visible");

    const hidden_row = try relational_store_mod.getRawAlloc(alloc, db.core.store, "user:hidden") orelse return error.TestExpectedEqual;
    defer alloc.free(hidden_row);
    if (try relational_store_mod.uniqueConstraintTupleValueAlloc(alloc, hidden_row, runtime_schema.unique_constraints[0])) |hidden_tuple| {
        alloc.free(hidden_tuple);
        return error.TestExpectedEqual;
    }
}

test "db foreign key action job applies set-null children in durable pages" {
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        const schema_json =
            \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"customer_id":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"foreign_keys":[{"name":"orders_customer_id_fkey","columns":["customer_id"],"references":{"table":"customers","columns":["_id"]},"on_delete":"set_null"}]}
        ;
        var parsed_schema = try table_schema_api.parseValidatedTableSchema(alloc, schema_json);
        defer parsed_schema.deinit(alloc);
        const runtime_schema = try table_schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
        defer schema_mod.freeSchema(alloc, runtime_schema);
        try db.setSchema(runtime_schema);

        try db.batch(.{
            .writes = &.{
                .{ .key = "customer:wide-job", .value = "{\"id\":\"customer:wide-job\"}" },
                .{ .key = "order:wide-job:1", .value = "{\"id\":\"order:wide-job:1\",\"customer_id\":\"customer:wide-job\",\"status\":\"open\"}" },
                .{ .key = "order:wide-job:2", .value = "{\"id\":\"order:wide-job:2\",\"customer_id\":\"customer:wide-job\",\"status\":\"open\"}" },
                .{ .key = "order:wide-job:3", .value = "{\"id\":\"order:wide-job:3\",\"customer_id\":\"customer:wide-job\",\"status\":\"open\"}" },
            },
        });

        const scheduled = try db.scheduleForeignKeyActionJobAt(
            "fk-action:set-null:customer-wide-job",
            "set_null",
            "worker:fk-action:scheduler",
            "orders_customer_id_fkey",
            "customers",
            "customer:wide-job",
            2,
            100_000,
        );
        defer db.freeForeignKeyActionJobRecord(scheduled);
        try std.testing.expectEqualStrings("pending", scheduled.status);
        try std.testing.expect(!scheduled.completed);
        try std.testing.expectEqual(@as(u32, 0), scheduled.attempts);
        try std.testing.expectEqual(@as(u64, 0), scheduled.claimed_at_ns);
        try std.testing.expectEqual(@as(u64, 0), scheduled.lease_until_ns);

        const scheduled_again = try db.scheduleForeignKeyActionJobAt(
            "fk-action:set-null:customer-wide-job",
            "set_null",
            "worker:fk-action:scheduler",
            "orders_customer_id_fkey",
            "customers",
            "customer:wide-job",
            2,
            150_000,
        );
        defer db.freeForeignKeyActionJobRecord(scheduled_again);
        try std.testing.expectEqualStrings("pending", scheduled_again.status);
        try std.testing.expectEqual(@as(u64, scheduled.created_at_ns), scheduled_again.created_at_ns);

        const first = try db.claimAndRunForeignKeyActionJobPageAt(
            "fk-action:set-null:customer-wide-job",
            "set_null",
            "worker:fk-action:a",
            "orders_customer_id_fkey",
            "customers",
            "customer:wide-job",
            2,
            1,
            200_000,
        );
        defer db.freeForeignKeyActionJobRecord(first);
        try std.testing.expectEqualStrings("pending", first.status);
        try std.testing.expect(!first.completed);
        try std.testing.expectEqual(@as(u64, 2), first.applied_children);
        try std.testing.expect(first.next_child_table != null);
        try std.testing.expect(first.next_child_key != null);

        const row_one = (try db.get(alloc, "order:wide-job:1")) orelse return error.TestExpectedEqual;
        defer alloc.free(row_one);
        const row_two = (try db.get(alloc, "order:wide-job:2")) orelse return error.TestExpectedEqual;
        defer alloc.free(row_two);
        const row_three = (try db.get(alloc, "order:wide-job:3")) orelse return error.TestExpectedEqual;
        defer alloc.free(row_three);
        try std.testing.expect(std.mem.indexOf(u8, row_one, "customer_id") == null);
        try std.testing.expect(std.mem.indexOf(u8, row_two, "customer_id") == null);
        try std.testing.expect(std.mem.indexOf(u8, row_three, "\"customer_id\":\"customer:wide-job\"") != null);
    }

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        const loaded = (try db.loadForeignKeyActionJobRecord("fk-action:set-null:customer-wide-job")) orelse return error.TestExpectedEqual;
        defer db.freeForeignKeyActionJobRecord(loaded);
        try std.testing.expectEqualStrings("pending", loaded.status);
        try std.testing.expectEqual(@as(u64, 2), loaded.applied_children);
        try std.testing.expect(loaded.next_child_table != null);
        try std.testing.expect(loaded.next_child_key != null);

        const second = try db.claimAndRunForeignKeyActionJobPageAt(
            "fk-action:set-null:customer-wide-job",
            "set_null",
            "worker:fk-action:b",
            "orders_customer_id_fkey",
            "customers",
            "customer:wide-job",
            2,
            1,
            2_000_000,
        );
        defer db.freeForeignKeyActionJobRecord(second);
        try std.testing.expectEqualStrings("complete", second.status);
        try std.testing.expect(second.completed);
        try std.testing.expectEqual(@as(u64, 3), second.applied_children);
        try std.testing.expect(second.next_child_table == null);
        try std.testing.expect(second.next_child_key == null);

        const row_three = (try db.get(alloc, "order:wide-job:3")) orelse return error.TestExpectedEqual;
        defer alloc.free(row_three);
        try std.testing.expect(std.mem.indexOf(u8, row_three, "customer_id") == null);
    }
}

test "db foreign key action job applies cascade children in durable pages" {
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        const schema_json =
            \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"customer_id":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"foreign_keys":[{"name":"orders_customer_id_fkey","columns":["customer_id"],"references":{"table":"customers","columns":["_id"]},"on_delete":"cascade"}]}
        ;
        var parsed_schema = try table_schema_api.parseValidatedTableSchema(alloc, schema_json);
        defer parsed_schema.deinit(alloc);
        const runtime_schema = try table_schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
        defer schema_mod.freeSchema(alloc, runtime_schema);
        try db.setSchema(runtime_schema);

        try db.batch(.{
            .writes = &.{
                .{ .key = "customer:cascade-job", .value = "{\"id\":\"customer:cascade-job\"}" },
                .{ .key = "order:cascade-job:1", .value = "{\"id\":\"order:cascade-job:1\",\"customer_id\":\"customer:cascade-job\",\"status\":\"open\"}" },
                .{ .key = "order:cascade-job:2", .value = "{\"id\":\"order:cascade-job:2\",\"customer_id\":\"customer:cascade-job\",\"status\":\"open\"}" },
                .{ .key = "order:cascade-job:3", .value = "{\"id\":\"order:cascade-job:3\",\"customer_id\":\"customer:cascade-job\",\"status\":\"open\"}" },
            },
        });

        const scheduled = try db.scheduleForeignKeyActionJobAt(
            "fk-action:cascade:customer-cascade-job",
            "cascade",
            "worker:fk-action:scheduler",
            "orders_customer_id_fkey",
            "customers",
            "customer:cascade-job",
            2,
            100_000,
        );
        defer db.freeForeignKeyActionJobRecord(scheduled);
        try std.testing.expectEqualStrings("pending", scheduled.status);
        try std.testing.expect(!scheduled.completed);

        const first = try db.claimAndRunForeignKeyActionJobPageAt(
            "fk-action:cascade:customer-cascade-job",
            "cascade",
            "worker:fk-action:a",
            "orders_customer_id_fkey",
            "customers",
            "customer:cascade-job",
            2,
            1,
            200_000,
        );
        defer db.freeForeignKeyActionJobRecord(first);
        try std.testing.expectEqualStrings("pending", first.status);
        try std.testing.expect(!first.completed);
        try std.testing.expectEqual(@as(u64, 2), first.applied_children);
        try std.testing.expect(first.next_child_table != null);
        try std.testing.expect(first.next_child_key != null);

        try std.testing.expect((try db.get(alloc, "order:cascade-job:1")) == null);
        try std.testing.expect((try db.get(alloc, "order:cascade-job:2")) == null);
        const row_three = (try db.get(alloc, "order:cascade-job:3")) orelse return error.TestExpectedEqual;
        defer alloc.free(row_three);
        try std.testing.expect(std.mem.indexOf(u8, row_three, "\"customer_id\":\"customer:cascade-job\"") != null);
    }

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        const loaded = (try db.loadForeignKeyActionJobRecord("fk-action:cascade:customer-cascade-job")) orelse return error.TestExpectedEqual;
        defer db.freeForeignKeyActionJobRecord(loaded);
        try std.testing.expectEqualStrings("pending", loaded.status);
        try std.testing.expectEqual(@as(u64, 2), loaded.applied_children);
        try std.testing.expect(loaded.next_child_table != null);
        try std.testing.expect(loaded.next_child_key != null);

        const second = try db.claimAndRunForeignKeyActionJobPageAt(
            "fk-action:cascade:customer-cascade-job",
            "cascade",
            "worker:fk-action:b",
            "orders_customer_id_fkey",
            "customers",
            "customer:cascade-job",
            2,
            1,
            2_000_000,
        );
        defer db.freeForeignKeyActionJobRecord(second);
        try std.testing.expectEqualStrings("complete", second.status);
        try std.testing.expect(second.completed);
        try std.testing.expectEqual(@as(u64, 3), second.applied_children);
        try std.testing.expect(second.next_child_table == null);
        try std.testing.expect(second.next_child_key == null);

        try std.testing.expect((try db.get(alloc, "order:cascade-job:3")) == null);
        const remaining_refs = try db.listForeignKeyRefChildrenForParent(alloc, "orders_customer_id_fkey", "customers", "customer:cascade-job", 10);
        defer db.freeForeignKeyRefChildren(alloc, remaining_refs);
        try std.testing.expectEqual(@as(usize, 0), remaining_refs.len);
    }
}

test "db foreign key modeled relational identity workload covers repair and actions" {
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"tenant_id":{"type":"keyword"},"id":{"type":"keyword"},"email":{"type":"keyword"},"customer_id":{"type":"keyword"},"customer_email":{"type":"keyword"},"nullable_customer_id":{"type":"keyword"},"deferred_customer_id":{"type":"keyword"},"order_id":{"type":"keyword"},"status":{"type":"keyword"}},"required":["tenant_id","id"],"additionalProperties":false}}},"primary_key":{"columns":["tenant_id","id"]},"unique_constraints":[{"name":"row_tenant_email_key","columns":["tenant_id","email"]}],"foreign_keys":[{"name":"orders_customer_pk_fkey","columns":["tenant_id","customer_id"],"references":{"table":"row","columns":["tenant_id","id"]},"on_delete":"restrict","on_update":"restrict","match":"simple","timing":"immediate","validation_state":"enforced"},{"name":"orders_customer_email_fkey","columns":["tenant_id","customer_email"],"references":{"table":"row","columns":["tenant_id","email"]},"on_delete":"restrict","on_update":"restrict","match":"simple","timing":"immediate","validation_state":"enforced"},{"name":"orders_customer_nullable_fkey","columns":["nullable_customer_id"],"references":{"table":"row","columns":["_id"]},"on_delete":"set_null","on_update":"restrict","match":"simple","timing":"immediate","validation_state":"enforced"},{"name":"orders_deferred_customer_fkey","columns":["tenant_id","deferred_customer_id"],"references":{"table":"row","columns":["tenant_id","id"]},"on_delete":"no_action","on_update":"no_action","match":"simple","timing":"deferred","validation_state":"enforced"},{"name":"lines_order_fkey","columns":["tenant_id","order_id"],"references":{"table":"row","columns":["tenant_id","id"]},"on_delete":"cascade","on_update":"restrict","match":"simple","timing":"immediate","validation_state":"enforced"}]}
    ;

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        var parsed_schema = try table_schema_api.parseValidatedTableSchema(alloc, schema_json);
        defer parsed_schema.deinit(alloc);
        const runtime_schema = try table_schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
        defer schema_mod.freeSchema(alloc, runtime_schema);
        try db.setSchema(runtime_schema);

        try db.batch(.{
            .writes = &.{
                .{ .key = "customer:a", .value = "{\"tenant_id\":\"t1\",\"id\":\"customer:a\",\"email\":\"ada@example.test\"}" },
                .{ .key = "customer:b", .value = "{\"tenant_id\":\"t1\",\"id\":\"customer:b\",\"email\":\"grace@example.test\"}" },
                .{ .key = "customer:c", .value = "{\"tenant_id\":\"t1\",\"id\":\"customer:c\",\"email\":\"linus@example.test\"}" },
                .{ .key = "order:identity", .value = "{\"tenant_id\":\"t1\",\"id\":\"order:identity\",\"customer_id\":\"customer:a\",\"customer_email\":\"ada@example.test\",\"status\":\"open\"}" },
                .{ .key = "line:identity:1", .value = "{\"tenant_id\":\"t1\",\"id\":\"line:identity:1\",\"order_id\":\"order:identity\",\"status\":\"open\"}" },
                .{ .key = "line:identity:2", .value = "{\"tenant_id\":\"t1\",\"id\":\"line:identity:2\",\"order_id\":\"order:identity\",\"status\":\"open\"}" },
                .{ .key = "order:set-null:1", .value = "{\"tenant_id\":\"t1\",\"id\":\"order:set-null:1\",\"nullable_customer_id\":\"customer:b\",\"status\":\"open\"}" },
                .{ .key = "order:set-null:2", .value = "{\"tenant_id\":\"t1\",\"id\":\"order:set-null:2\",\"nullable_customer_id\":\"customer:b\",\"status\":\"open\"}" },
                .{ .key = "order:deferred", .value = "{\"tenant_id\":\"t1\",\"id\":\"order:deferred\",\"deferred_customer_id\":\"customer:c\",\"status\":\"open\"}" },
            },
        });

        try std.testing.expectError(error.ForeignKeyViolation, db.batch(.{
            .writes = &.{.{ .key = "order:missing-parent", .value = "{\"tenant_id\":\"t1\",\"id\":\"order:missing-parent\",\"customer_id\":\"customer:missing\"}" }},
        }));
        try std.testing.expectError(error.ForeignKeyViolation, db.batch(.{
            .writes = &.{.{ .key = "customer:a", .value = "{\"tenant_id\":\"t1\",\"id\":\"customer:a\",\"email\":\"ada-renamed@example.test\"}" }},
        }));

        try db.batch(.{
            .writes = &.{.{ .key = "order:identity", .value = "{\"tenant_id\":\"t1\",\"id\":\"order:identity\",\"customer_id\":\"customer:b\",\"customer_email\":\"grace@example.test\",\"status\":\"open\"}" }},
        });

        const clean_after_move = try db.validateForeignKeyRefsInRange("", "");
        try std.testing.expect(clean_after_move.valid());

        const stale_pk_row = try mapper.buildRelationalRowValueAlloc(alloc, "{\"tenant_id\":\"t1\",\"id\":\"stale-source\",\"customer_id\":\"customer:a\"}", runtime_schema.relational_columns);
        defer alloc.free(stale_pk_row);
        const stale_pk_parent = (try relational_store_mod.foreignKeyReferenceValueAlloc(alloc, stale_pk_row, runtime_schema.foreign_keys[0])) orelse return error.TestExpectedEqual;
        defer alloc.free(stale_pk_parent);
        const current_pk_row = try mapper.buildRelationalRowValueAlloc(alloc, "{\"tenant_id\":\"t1\",\"id\":\"order:identity\",\"customer_id\":\"customer:b\"}", runtime_schema.relational_columns);
        defer alloc.free(current_pk_row);
        const current_pk_parent = (try relational_store_mod.foreignKeyReferenceValueAlloc(alloc, current_pk_row, runtime_schema.foreign_keys[0])) orelse return error.TestExpectedEqual;
        defer alloc.free(current_pk_parent);

        const stale_txn = try db.beginTransaction(44_000);
        try db.writeTransaction(stale_txn, .{
            .foreign_key_ref_writes = &.{
                .{
                    .constraint_name = "orders_customer_pk_fkey",
                    .parent_table = "row",
                    .parent_key = stale_pk_parent,
                    .child_table = "row",
                    .child_key = "order:identity",
                },
                .{
                    .constraint_name = "orders_customer_pk_fkey",
                    .parent_table = "row",
                    .parent_key = stale_pk_parent,
                    .child_table = "row",
                    .child_key = "order:missing",
                },
            },
        });
        try db.commitTransaction(stale_txn, 44_001);

        const stale_owner_report = try db.validateForeignKeyRefOwnerForParent("orders_customer_pk_fkey", "row", stale_pk_parent);
        try std.testing.expectEqual(@as(u64, 2), stale_owner_report.scanned_ref_rows);
        try std.testing.expectEqual(@as(u64, 2), stale_owner_report.stale_ref_rows);
        const repaired_owner_report = try db.repairForeignKeyRefOwnerForParent("orders_customer_pk_fkey", "row", stale_pk_parent);
        try std.testing.expectEqual(@as(u64, 2), repaired_owner_report.deleted_stale_ref_rows);

        const current_ref_key = try internal_keys.relationalForeignKeyRefKeyAlloc(alloc, "orders_customer_pk_fkey", "row", current_pk_parent, "row", "order:identity");
        defer alloc.free(current_ref_key);
        try db.core.store.delete(current_ref_key);
        const missing_ref_report = try db.validateForeignKeyRefsInRangeForConstraint("orders_customer_pk_fkey", "", "");
        try std.testing.expect(!missing_ref_report.valid());
        try std.testing.expectEqual(@as(u64, 1), missing_ref_report.missing_ref_rows);
        const repaired_ref_report = try db.repairForeignKeyRefsInRangeForConstraint("orders_customer_pk_fkey", "", "");
        try std.testing.expectEqual(@as(u64, 1), repaired_ref_report.repaired_ref_rows);

        const set_null_row = try mapper.buildRelationalRowValueAlloc(alloc, "{\"tenant_id\":\"t1\",\"id\":\"set-null-source\",\"nullable_customer_id\":\"customer:b\"}", runtime_schema.relational_columns);
        defer alloc.free(set_null_row);
        const set_null_parent = (try relational_store_mod.foreignKeyReferenceValueAlloc(alloc, set_null_row, runtime_schema.foreign_keys[2])) orelse return error.TestExpectedEqual;
        defer alloc.free(set_null_parent);
        const cascade_row = try mapper.buildRelationalRowValueAlloc(alloc, "{\"tenant_id\":\"t1\",\"id\":\"line-source\",\"order_id\":\"order:identity\"}", runtime_schema.relational_columns);
        defer alloc.free(cascade_row);
        const cascade_parent = (try relational_store_mod.foreignKeyReferenceValueAlloc(alloc, cascade_row, runtime_schema.foreign_keys[4])) orelse return error.TestExpectedEqual;
        defer alloc.free(cascade_parent);

        const set_null_first = try db.claimAndRunForeignKeyActionJobPageAt(
            "fk-action:modeled:set-null",
            "set_null",
            "worker:modeled:a",
            "orders_customer_nullable_fkey",
            "row",
            set_null_parent,
            1,
            1,
            45_000,
        );
        defer db.freeForeignKeyActionJobRecord(set_null_first);
        try std.testing.expectEqualStrings("pending", set_null_first.status);
        try std.testing.expectEqual(@as(u64, 1), set_null_first.applied_children);
        try std.testing.expect(set_null_first.next_child_key != null);

        const cascade_first = try db.claimAndRunForeignKeyActionJobPageAt(
            "fk-action:modeled:cascade",
            "cascade",
            "worker:modeled:a",
            "lines_order_fkey",
            "row",
            cascade_parent,
            1,
            1,
            45_100,
        );
        defer db.freeForeignKeyActionJobRecord(cascade_first);
        try std.testing.expectEqualStrings("pending", cascade_first.status);
        try std.testing.expectEqual(@as(u64, 1), cascade_first.applied_children);
        try std.testing.expect(cascade_first.next_child_key != null);
    }

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        var parsed_schema = try table_schema_api.parseValidatedTableSchema(alloc, schema_json);
        defer parsed_schema.deinit(alloc);
        const runtime_schema = try table_schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
        defer schema_mod.freeSchema(alloc, runtime_schema);
        try db.setSchema(runtime_schema);

        const set_null_row = try mapper.buildRelationalRowValueAlloc(alloc, "{\"tenant_id\":\"t1\",\"id\":\"set-null-source\",\"nullable_customer_id\":\"customer:b\"}", runtime_schema.relational_columns);
        defer alloc.free(set_null_row);
        const set_null_parent = (try relational_store_mod.foreignKeyReferenceValueAlloc(alloc, set_null_row, runtime_schema.foreign_keys[2])) orelse return error.TestExpectedEqual;
        defer alloc.free(set_null_parent);
        const cascade_row = try mapper.buildRelationalRowValueAlloc(alloc, "{\"tenant_id\":\"t1\",\"id\":\"line-source\",\"order_id\":\"order:identity\"}", runtime_schema.relational_columns);
        defer alloc.free(cascade_row);
        const cascade_parent = (try relational_store_mod.foreignKeyReferenceValueAlloc(alloc, cascade_row, runtime_schema.foreign_keys[4])) orelse return error.TestExpectedEqual;
        defer alloc.free(cascade_parent);

        const loaded_set_null = (try db.loadForeignKeyActionJobRecord("fk-action:modeled:set-null")) orelse return error.TestExpectedEqual;
        defer db.freeForeignKeyActionJobRecord(loaded_set_null);
        try std.testing.expectEqualStrings("pending", loaded_set_null.status);
        try std.testing.expectEqual(@as(u64, 1), loaded_set_null.applied_children);
        const loaded_cascade = (try db.loadForeignKeyActionJobRecord("fk-action:modeled:cascade")) orelse return error.TestExpectedEqual;
        defer db.freeForeignKeyActionJobRecord(loaded_cascade);
        try std.testing.expectEqualStrings("pending", loaded_cascade.status);
        try std.testing.expectEqual(@as(u64, 1), loaded_cascade.applied_children);

        const set_null_second = try db.claimAndRunForeignKeyActionJobPageAt(
            "fk-action:modeled:set-null",
            "set_null",
            "worker:modeled:b",
            "orders_customer_nullable_fkey",
            "row",
            set_null_parent,
            1,
            1,
            2_000_000,
        );
        defer db.freeForeignKeyActionJobRecord(set_null_second);
        try std.testing.expectEqualStrings("complete", set_null_second.status);
        try std.testing.expectEqual(@as(u64, 2), set_null_second.applied_children);

        const cascade_second = try db.claimAndRunForeignKeyActionJobPageAt(
            "fk-action:modeled:cascade",
            "cascade",
            "worker:modeled:b",
            "lines_order_fkey",
            "row",
            cascade_parent,
            1,
            1,
            2_100_000,
        );
        defer db.freeForeignKeyActionJobRecord(cascade_second);
        try std.testing.expectEqualStrings("complete", cascade_second.status);
        try std.testing.expectEqual(@as(u64, 2), cascade_second.applied_children);

        const set_null_duplicate = try db.claimAndRunForeignKeyActionJobPageAt(
            "fk-action:modeled:set-null",
            "set_null",
            "worker:modeled:duplicate",
            "orders_customer_nullable_fkey",
            "row",
            set_null_parent,
            1,
            1,
            2_200_000,
        );
        defer db.freeForeignKeyActionJobRecord(set_null_duplicate);
        try std.testing.expectEqualStrings("complete", set_null_duplicate.status);
        try std.testing.expect(set_null_duplicate.completed);
        try std.testing.expectEqual(@as(u64, 2), set_null_duplicate.applied_children);
        try std.testing.expectEqual(@as(u32, set_null_second.attempts), set_null_duplicate.attempts);
        try std.testing.expectEqualStrings(set_null_second.worker_id, set_null_duplicate.worker_id);

        const cascade_duplicate = try db.claimAndRunForeignKeyActionJobPageAt(
            "fk-action:modeled:cascade",
            "cascade",
            "worker:modeled:duplicate",
            "lines_order_fkey",
            "row",
            cascade_parent,
            1,
            1,
            2_300_000,
        );
        defer db.freeForeignKeyActionJobRecord(cascade_duplicate);
        try std.testing.expectEqualStrings("complete", cascade_duplicate.status);
        try std.testing.expect(cascade_duplicate.completed);
        try std.testing.expectEqual(@as(u64, 2), cascade_duplicate.applied_children);
        try std.testing.expectEqual(@as(u32, cascade_second.attempts), cascade_duplicate.attempts);
        try std.testing.expectEqualStrings(cascade_second.worker_id, cascade_duplicate.worker_id);

        const set_null_one = (try db.get(alloc, "order:set-null:1")) orelse return error.TestExpectedEqual;
        defer alloc.free(set_null_one);
        const set_null_two = (try db.get(alloc, "order:set-null:2")) orelse return error.TestExpectedEqual;
        defer alloc.free(set_null_two);
        try std.testing.expect(std.mem.indexOf(u8, set_null_one, "nullable_customer_id") == null);
        try std.testing.expect(std.mem.indexOf(u8, set_null_two, "nullable_customer_id") == null);
        try std.testing.expect((try db.get(alloc, "line:identity:1")) == null);
        try std.testing.expect((try db.get(alloc, "line:identity:2")) == null);

        const final_report = try db.validateForeignKeyRefsInRange("", "");
        try std.testing.expect(final_report.valid());
        try std.testing.expectEqual(@as(u64, 0), final_report.missing_parent_rows);
        try std.testing.expectEqual(@as(u64, 0), final_report.missing_ref_rows);
        try std.testing.expectEqual(@as(u64, 0), final_report.stale_ref_rows);
    }
}

test "db foreign key action job rejects stale page finish after lease handoff" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const scheduled = try db.scheduleForeignKeyActionJobAt(
        "fk-action:set-null:stale-finish",
        "set_null",
        "worker:fk-action:scheduler",
        "orders_customer_id_fkey",
        "customers",
        "customer:stale-finish",
        2,
        100_000,
    );
    defer db.freeForeignKeyActionJobRecord(scheduled);

    const first_claim = try db.claimForeignKeyActionJobPageAt(
        "fk-action:set-null:stale-finish",
        "set_null",
        "worker:fk-action:a",
        "orders_customer_id_fkey",
        "customers",
        "customer:stale-finish",
        2,
        1,
        200_000,
    );
    defer db.freeForeignKeyActionJobRecord(first_claim);
    try std.testing.expectEqualStrings("claimed", first_claim.status);
    try std.testing.expectEqual(@as(u32, 1), first_claim.attempts);

    const second_claim = try db.claimForeignKeyActionJobPageAt(
        "fk-action:set-null:stale-finish",
        "set_null",
        "worker:fk-action:b",
        "orders_customer_id_fkey",
        "customers",
        "customer:stale-finish",
        2,
        60_000,
        first_claim.lease_until_ns +| 1,
    );
    defer db.freeForeignKeyActionJobRecord(second_claim);
    try std.testing.expectEqualStrings("claimed", second_claim.status);
    try std.testing.expectEqualStrings("worker:fk-action:b", second_claim.worker_id);
    try std.testing.expectEqual(@as(u32, 2), second_claim.attempts);

    try std.testing.expectError(error.ForeignKeyIntegrityClaimBusy, db.finishClaimedForeignKeyActionJobPageAt(
        first_claim,
        1,
        false,
        "row",
        "order:stale",
        null,
        second_claim.claimed_at_ns +| 1,
    ));

    const loaded = (try db.loadForeignKeyActionJobRecord("fk-action:set-null:stale-finish")) orelse return error.TestExpectedEqual;
    defer db.freeForeignKeyActionJobRecord(loaded);
    try std.testing.expectEqualStrings("claimed", loaded.status);
    try std.testing.expectEqualStrings("worker:fk-action:b", loaded.worker_id);
    try std.testing.expectEqual(@as(u32, 2), loaded.attempts);
    try std.testing.expectEqual(@as(u64, 0), loaded.applied_children);
    try std.testing.expect(loaded.next_child_table == null);
    try std.testing.expect(loaded.next_child_key == null);

    try std.testing.expectError(error.InvalidForeignKeyActionJob, db.finishClaimedForeignKeyActionJobPageAt(
        second_claim,
        0,
        false,
        null,
        null,
        null,
        second_claim.claimed_at_ns +| 2,
    ));
    try std.testing.expectError(error.InvalidForeignKeyActionJob, db.finishClaimedForeignKeyActionJobPageAt(
        second_claim,
        0,
        false,
        "row",
        null,
        null,
        second_claim.claimed_at_ns +| 2,
    ));
    try std.testing.expectError(error.InvalidForeignKeyActionJob, db.finishClaimedForeignKeyActionJobPageAt(
        second_claim,
        0,
        true,
        "row",
        "order:cursor",
        null,
        second_claim.claimed_at_ns +| 2,
    ));
    try std.testing.expectError(error.InvalidForeignKeyActionJob, db.finishClaimedForeignKeyActionJobPageAt(
        second_claim,
        0,
        true,
        null,
        null,
        "FailedButComplete",
        second_claim.claimed_at_ns +| 2,
    ));
    try std.testing.expectError(error.InvalidForeignKeyActionJob, db.finishClaimedForeignKeyActionJobPageAt(
        second_claim,
        1,
        false,
        null,
        null,
        "FailedWithoutCursor",
        second_claim.claimed_at_ns +| 2,
    ));

    const finished = try db.finishClaimedForeignKeyActionJobPageAt(
        second_claim,
        1,
        false,
        "row",
        "order:cursor",
        null,
        second_claim.claimed_at_ns +| 2,
    );
    defer db.freeForeignKeyActionJobRecord(finished);
    try std.testing.expectEqualStrings("pending", finished.status);
    try std.testing.expectEqual(@as(u64, 1), finished.applied_children);
    try std.testing.expect(finished.next_child_table != null);
    try std.testing.expectEqualStrings("row", finished.next_child_table.?);
    try std.testing.expect(finished.next_child_key != null);
    try std.testing.expectEqualStrings("order:cursor", finished.next_child_key.?);

    try std.testing.expectError(error.ForeignKeyIntegrityClaimBusy, db.finishClaimedForeignKeyActionJobPageAt(
        second_claim,
        1,
        false,
        "row",
        "order:cursor",
        null,
        second_claim.claimed_at_ns +| 3,
    ));
    const loaded_after_duplicate_finish = (try db.loadForeignKeyActionJobRecord("fk-action:set-null:stale-finish")) orelse return error.TestExpectedEqual;
    defer db.freeForeignKeyActionJobRecord(loaded_after_duplicate_finish);
    try std.testing.expectEqualStrings("pending", loaded_after_duplicate_finish.status);
    try std.testing.expectEqual(@as(u64, 1), loaded_after_duplicate_finish.applied_children);
    try std.testing.expect(loaded_after_duplicate_finish.next_child_table != null);
    try std.testing.expectEqualStrings("row", loaded_after_duplicate_finish.next_child_table.?);
    try std.testing.expect(loaded_after_duplicate_finish.next_child_key != null);
    try std.testing.expectEqualStrings("order:cursor", loaded_after_duplicate_finish.next_child_key.?);
}

test "db foreign key action job requeue preserves durable cursor and clears failed claim" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const scheduled = try db.scheduleForeignKeyActionJobAt(
        "fk-action:set-null:requeue",
        "set_null",
        "worker:fk-action:scheduler",
        "orders_customer_id_fkey",
        "customers",
        "customer:requeue",
        8,
        100_000,
    );
    defer db.freeForeignKeyActionJobRecord(scheduled);

    try std.testing.expectError(error.InvalidForeignKeyActionJob, db.requeueForeignKeyActionJobAt(
        "fk-action:set-null:requeue",
        "set_null",
        "worker:fk-action:retry",
        "orders_customer_id_fkey",
        "customers",
        "customer:requeue",
        4,
        199_000,
    ));

    const claimed = try db.claimForeignKeyActionJobPageAt(
        "fk-action:set-null:requeue",
        "set_null",
        "worker:fk-action:failed",
        "orders_customer_id_fkey",
        "customers",
        "customer:requeue",
        8,
        60_000,
        200_000,
    );
    defer db.freeForeignKeyActionJobRecord(claimed);
    try std.testing.expectEqualStrings("claimed", claimed.status);

    const failed = try db.finishClaimedForeignKeyActionJobPageAt(
        claimed,
        3,
        false,
        "row",
        "order:requeue:cursor",
        "OperatorPaused",
        210_000,
    );
    defer db.freeForeignKeyActionJobRecord(failed);
    try std.testing.expectEqualStrings("invalid", failed.status);
    try std.testing.expect(!failed.completed);
    try std.testing.expectEqual(@as(u64, 3), failed.applied_children);
    try std.testing.expectEqual(@as(u32, 1), failed.attempts);
    try std.testing.expectEqualStrings("worker:fk-action:failed", failed.worker_id);
    try std.testing.expect(failed.lease_until_ns > 210_000);
    try std.testing.expect(failed.next_child_table != null);
    try std.testing.expectEqualStrings("row", failed.next_child_table.?);
    try std.testing.expect(failed.next_child_key != null);
    try std.testing.expectEqualStrings("order:requeue:cursor", failed.next_child_key.?);
    try std.testing.expect(failed.last_error != null);
    try std.testing.expectEqualStrings("OperatorPaused", failed.last_error.?);
    try std.testing.expectEqual(@as(u64, 1), failed.failure_count);
    try std.testing.expect(failed.first_failed_at_ns != null);
    try std.testing.expectEqual(@as(u64, 210_000), failed.first_failed_at_ns.?);
    try std.testing.expect(failed.last_failed_at_ns != null);
    try std.testing.expectEqual(@as(u64, 210_000), failed.last_failed_at_ns.?);
    try std.testing.expectEqual(@as(u64, 0), failed.requeue_count);
    try std.testing.expect(failed.last_requeued_at_ns == null);

    try std.testing.expectError(error.InvalidForeignKeyActionJob, db.claimForeignKeyActionJobPageAt(
        "fk-action:set-null:requeue",
        "set_null",
        "worker:fk-action:auto-retry",
        "orders_customer_id_fkey",
        "customers",
        "customer:requeue",
        4,
        60_000,
        failed.lease_until_ns +| 1,
    ));

    try std.testing.expectError(error.InvalidForeignKeyActionJob, db.requeueForeignKeyActionJobAt(
        "fk-action:set-null:requeue",
        "cascade",
        "worker:fk-action:retry",
        "orders_customer_id_fkey",
        "customers",
        "customer:requeue",
        4,
        220_000,
    ));

    const requeued = try db.requeueForeignKeyActionJobAt(
        "fk-action:set-null:requeue",
        "set_null",
        "worker:fk-action:retry",
        "orders_customer_id_fkey",
        "customers",
        "customer:requeue",
        4,
        220_000,
    );
    defer db.freeForeignKeyActionJobRecord(requeued);
    try std.testing.expectEqualStrings("pending", requeued.status);
    try std.testing.expect(!requeued.completed);
    try std.testing.expectEqualStrings("worker:fk-action:retry", requeued.worker_id);
    try std.testing.expectEqual(@as(usize, 4), requeued.page_limit);
    try std.testing.expectEqual(@as(u64, 0), requeued.claimed_at_ns);
    try std.testing.expectEqual(@as(u64, 0), requeued.lease_until_ns);
    try std.testing.expectEqual(@as(u32, 1), requeued.attempts);
    try std.testing.expectEqual(@as(u64, 3), requeued.applied_children);
    try std.testing.expect(requeued.next_child_table != null);
    try std.testing.expectEqualStrings("row", requeued.next_child_table.?);
    try std.testing.expect(requeued.next_child_key != null);
    try std.testing.expectEqualStrings("order:requeue:cursor", requeued.next_child_key.?);
    try std.testing.expect(requeued.last_error == null);
    try std.testing.expectEqual(@as(u64, 1), requeued.failure_count);
    try std.testing.expect(requeued.first_failed_at_ns != null);
    try std.testing.expectEqual(@as(u64, 210_000), requeued.first_failed_at_ns.?);
    try std.testing.expect(requeued.last_failed_at_ns != null);
    try std.testing.expectEqual(@as(u64, 210_000), requeued.last_failed_at_ns.?);
    try std.testing.expectEqual(@as(u64, 1), requeued.requeue_count);
    try std.testing.expect(requeued.last_requeued_at_ns != null);
    try std.testing.expectEqual(@as(u64, 220_000), requeued.last_requeued_at_ns.?);

    const retry_claim = try db.claimForeignKeyActionJobPageAt(
        "fk-action:set-null:requeue",
        "set_null",
        "worker:fk-action:next",
        "orders_customer_id_fkey",
        "customers",
        "customer:requeue",
        4,
        60_000,
        221_000,
    );
    defer db.freeForeignKeyActionJobRecord(retry_claim);
    try std.testing.expectEqualStrings("claimed", retry_claim.status);
    try std.testing.expectEqualStrings("worker:fk-action:next", retry_claim.worker_id);
    try std.testing.expectEqual(@as(u32, 2), retry_claim.attempts);
    try std.testing.expectEqual(@as(u64, 3), retry_claim.applied_children);
    try std.testing.expect(retry_claim.next_child_table != null);
    try std.testing.expectEqualStrings("row", retry_claim.next_child_table.?);
    try std.testing.expect(retry_claim.next_child_key != null);
    try std.testing.expectEqualStrings("order:requeue:cursor", retry_claim.next_child_key.?);
    try std.testing.expectEqual(@as(u64, 1), retry_claim.failure_count);
    try std.testing.expectEqual(@as(u64, 1), retry_claim.requeue_count);

    const completed = try db.finishClaimedForeignKeyActionJobPageAt(
        retry_claim,
        0,
        true,
        null,
        null,
        null,
        222_000,
    );
    defer db.freeForeignKeyActionJobRecord(completed);
    try std.testing.expectEqualStrings("complete", completed.status);
    try std.testing.expect(completed.completed);
    try std.testing.expectEqual(@as(u64, 1), completed.failure_count);
    try std.testing.expectEqual(@as(u64, 1), completed.requeue_count);

    const completed_claim = try db.claimForeignKeyActionJobPageAt(
        "fk-action:set-null:requeue",
        "set_null",
        "worker:fk-action:after-complete",
        "orders_customer_id_fkey",
        "customers",
        "customer:requeue",
        99,
        60_000,
        223_000,
    );
    defer db.freeForeignKeyActionJobRecord(completed_claim);
    try std.testing.expectEqualStrings("complete", completed_claim.status);
    try std.testing.expect(completed_claim.completed);
    try std.testing.expectEqualStrings("worker:fk-action:next", completed_claim.worker_id);
    try std.testing.expectEqual(@as(usize, 4), completed_claim.page_limit);
    try std.testing.expectEqual(@as(u64, 221_000), completed_claim.claimed_at_ns);
    try std.testing.expectEqual(retry_claim.lease_until_ns, completed_claim.lease_until_ns);
    try std.testing.expectEqual(@as(u32, 2), completed_claim.attempts);
    try std.testing.expectEqual(@as(u64, 3), completed_claim.applied_children);
    try std.testing.expectEqual(@as(u64, 1), completed_claim.failure_count);
    try std.testing.expectEqual(@as(u64, 1), completed_claim.requeue_count);

    try std.testing.expectError(error.InvalidForeignKeyActionJob, db.requeueForeignKeyActionJobAt(
        "fk-action:set-null:requeue",
        "set_null",
        "worker:fk-action:retry",
        "orders_customer_id_fkey",
        "customers",
        "customer:requeue",
        4,
        223_000,
    ));
}

test "db foreign key ref children page by child cursor" {
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"customer_id":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"foreign_keys":[{"name":"orders_customer_id_fkey","columns":["customer_id"],"references":{"table":"customers","columns":["_id"]},"on_delete":"restrict"}]}
    ;
    var parsed_schema = try table_schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try table_schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    const owner_write_txn = try db.beginTransaction(28_000);
    try db.writeTransaction(owner_write_txn, .{
        .foreign_key_ref_writes = &.{
            .{
                .constraint_name = "orders_customer_id_fkey",
                .parent_table = "customers",
                .parent_key = "customer:page",
                .child_table = "row",
                .child_key = "order:a",
            },
            .{
                .constraint_name = "orders_customer_id_fkey",
                .parent_table = "customers",
                .parent_key = "customer:page",
                .child_table = "row",
                .child_key = "order:b",
            },
            .{
                .constraint_name = "orders_customer_id_fkey",
                .parent_table = "customers",
                .parent_key = "customer:page",
                .child_table = "row",
                .child_key = "order:c",
            },
        },
    });
    try db.commitTransaction(owner_write_txn, 28_001);

    try std.testing.expectError(error.ForeignKeyActionLimitExceeded, db.listForeignKeyRefChildrenForParent(alloc, "orders_customer_id_fkey", "customers", "customer:page", 2));

    var first_page = try db.listForeignKeyRefChildrenPageForParent(alloc, "orders_customer_id_fkey", "customers", "customer:page", null, null, 2);
    defer db.freeForeignKeyRefChildrenPage(alloc, &first_page);
    try std.testing.expect(!first_page.complete);
    try std.testing.expectEqual(@as(usize, 2), first_page.children.len);
    try std.testing.expectEqualStrings("order:a", first_page.children[0].child_key);
    try std.testing.expectEqualStrings("order:b", first_page.children[1].child_key);
    try std.testing.expectEqualStrings("row", first_page.next_child_table.?);
    try std.testing.expectEqualStrings("order:b", first_page.next_child_key.?);

    var second_page = try db.listForeignKeyRefChildrenPageForParent(
        alloc,
        "orders_customer_id_fkey",
        "customers",
        "customer:page",
        first_page.next_child_table,
        first_page.next_child_key,
        2,
    );
    defer db.freeForeignKeyRefChildrenPage(alloc, &second_page);
    try std.testing.expect(second_page.complete);
    try std.testing.expectEqual(@as(usize, 1), second_page.children.len);
    try std.testing.expectEqualStrings("order:c", second_page.children[0].child_key);
    try std.testing.expect(second_page.next_child_table == null);
    try std.testing.expect(second_page.next_child_key == null);
}

test "db foreign key ref owner validation repairs stale parent prefix rows" {
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"customer_id":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"foreign_keys":[{"name":"orders_customer_id_fkey","columns":["customer_id"],"references":{"table":"customers","columns":["_id"]},"on_delete":"restrict"}]}
    ;
    var parsed_schema = try table_schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try table_schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    try db.batch(.{
        .writes = &.{
            .{ .key = "customer:owner", .value = "{\"id\":\"customer:owner\"}" },
            .{ .key = "customer:other", .value = "{\"id\":\"customer:other\"}" },
            .{ .key = "order:valid", .value = "{\"id\":\"order:valid\",\"customer_id\":\"customer:owner\"}" },
            .{ .key = "order:moved", .value = "{\"id\":\"order:moved\",\"customer_id\":\"customer:other\"}" },
        },
    });

    const stale_write_txn = try db.beginTransaction(29_000);
    try db.writeTransaction(stale_write_txn, .{
        .foreign_key_ref_writes = &.{
            .{
                .constraint_name = "orders_customer_id_fkey",
                .parent_table = "customers",
                .parent_key = "customer:owner",
                .child_table = "row",
                .child_key = "order:missing",
            },
            .{
                .constraint_name = "orders_customer_id_fkey",
                .parent_table = "customers",
                .parent_key = "customer:owner",
                .child_table = "row",
                .child_key = "order:moved",
            },
        },
    });
    try db.commitTransaction(stale_write_txn, 29_001);

    const validate_report = try db.validateForeignKeyRefOwnerForParent("orders_customer_id_fkey", "customers", "customer:owner");
    try std.testing.expectEqual(@as(u64, 3), validate_report.scanned_ref_rows);
    try std.testing.expectEqual(@as(u64, 2), validate_report.stale_ref_rows);
    try std.testing.expectEqual(@as(u64, 0), validate_report.deleted_stale_ref_rows);

    const dry_run_report = try db.dryRunRepairForeignKeyRefOwnerForParent("orders_customer_id_fkey", "customers", "customer:owner");
    try std.testing.expectEqual(@as(u64, 3), dry_run_report.scanned_ref_rows);
    try std.testing.expectEqual(@as(u64, 2), dry_run_report.stale_ref_rows);
    try std.testing.expectEqual(@as(u64, 2), dry_run_report.deleted_stale_ref_rows);

    const before_repair = try db.listForeignKeyRefChildrenForParent(alloc, "orders_customer_id_fkey", "customers", "customer:owner", 10);
    defer db.freeForeignKeyRefChildren(alloc, before_repair);
    try std.testing.expectEqual(@as(usize, 3), before_repair.len);

    const repair_report = try db.repairForeignKeyRefOwnerForParent("orders_customer_id_fkey", "customers", "customer:owner");
    try std.testing.expectEqual(@as(u64, 3), repair_report.scanned_ref_rows);
    try std.testing.expectEqual(@as(u64, 2), repair_report.stale_ref_rows);
    try std.testing.expectEqual(@as(u64, 2), repair_report.deleted_stale_ref_rows);

    const after_repair = try db.listForeignKeyRefChildrenForParent(alloc, "orders_customer_id_fkey", "customers", "customer:owner", 10);
    defer db.freeForeignKeyRefChildren(alloc, after_repair);
    try std.testing.expectEqual(@as(usize, 1), after_repair.len);
    try std.testing.expectEqualStrings("order:valid", after_repair[0].child_key);

    const final_report = try db.validateForeignKeyRefOwnerForParent("orders_customer_id_fkey", "customers", "customer:owner");
    try std.testing.expectEqual(@as(u64, 1), final_report.scanned_ref_rows);
    try std.testing.expectEqual(@as(u64, 0), final_report.stale_ref_rows);
}

test "db foreign key ref owner range validation repairs stale parent-key span rows" {
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"customer_id":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"foreign_keys":[{"name":"orders_customer_id_fkey","columns":["customer_id"],"references":{"table":"customers","columns":["_id"]},"on_delete":"restrict"}]}
    ;
    var parsed_schema = try table_schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try table_schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    try db.batch(.{
        .writes = &.{
            .{ .key = "customer:a", .value = "{\"id\":\"customer:a\"}" },
            .{ .key = "customer:b", .value = "{\"id\":\"customer:b\"}" },
            .{ .key = "customer:z", .value = "{\"id\":\"customer:z\"}" },
            .{ .key = "order:a-valid", .value = "{\"id\":\"order:a-valid\",\"customer_id\":\"customer:a\"}" },
            .{ .key = "order:b-moved", .value = "{\"id\":\"order:b-moved\",\"customer_id\":\"customer:z\"}" },
        },
    });

    const stale_write_txn = try db.beginTransaction(30_000);
    try db.writeTransaction(stale_write_txn, .{
        .foreign_key_ref_writes = &.{
            .{
                .constraint_name = "orders_customer_id_fkey",
                .parent_table = "customers",
                .parent_key = "customer:b",
                .child_table = "row",
                .child_key = "order:missing-b",
            },
            .{
                .constraint_name = "orders_customer_id_fkey",
                .parent_table = "customers",
                .parent_key = "customer:b",
                .child_table = "row",
                .child_key = "order:b-moved",
            },
            .{
                .constraint_name = "orders_customer_id_fkey",
                .parent_table = "customers",
                .parent_key = "customer:z",
                .child_table = "row",
                .child_key = "order:missing-z",
            },
        },
    });
    try db.commitTransaction(stale_write_txn, 30_001);

    const validate_report = try db.validateForeignKeyRefOwnerRange("orders_customer_id_fkey", "customers", "", "customer:m");
    try std.testing.expectEqual(@as(u64, 3), validate_report.scanned_ref_rows);
    try std.testing.expectEqual(@as(u64, 2), validate_report.stale_ref_rows);
    try std.testing.expectEqual(@as(u64, 0), validate_report.deleted_stale_ref_rows);

    const dry_run_report = try db.dryRunRepairForeignKeyRefOwnerRange("orders_customer_id_fkey", "customers", "", "customer:m");
    try std.testing.expectEqual(@as(u64, 3), dry_run_report.scanned_ref_rows);
    try std.testing.expectEqual(@as(u64, 2), dry_run_report.stale_ref_rows);
    try std.testing.expectEqual(@as(u64, 2), dry_run_report.deleted_stale_ref_rows);

    const before_repair_low = try db.validateForeignKeyRefOwnerRange("orders_customer_id_fkey", "customers", "", "customer:m");
    try std.testing.expectEqual(@as(u64, 3), before_repair_low.scanned_ref_rows);
    try std.testing.expectEqual(@as(u64, 2), before_repair_low.stale_ref_rows);

    const worker_report = try db.claimAndRunForeignKeyIntegrityWorkUnitAt(
        "claim:owner-low",
        "worker:owner",
        41,
        "owner_range",
        .repair,
        "orders_customer_id_fkey",
        "",
        "customer:m",
        60_000,
        31_000,
    );
    try std.testing.expectEqual(@as(u64, 3), worker_report.scanned_ref_rows);
    try std.testing.expectEqual(@as(u64, 2), worker_report.stale_ref_rows);
    try std.testing.expectEqual(@as(u64, 2), worker_report.deleted_stale_ref_rows);

    const worker_claim = (try db.loadForeignKeyIntegrityClaimRecord("claim:owner-low")) orelse return error.TestUnexpectedResult;
    defer db.freeForeignKeyIntegrityClaimRecord(worker_claim);
    try std.testing.expectEqualStrings("owner_range", worker_claim.phase);
    try std.testing.expectEqualStrings("orders_customer_id_fkey", worker_claim.constraint_name.?);

    const owner_progress = (try db.loadForeignKeyIntegrityProgressRecordForPhase("owner_range", .repair, "orders_customer_id_fkey", "", "customer:m")) orelse return error.TestUnexpectedResult;
    defer db.freeForeignKeyIntegrityProgressRecord(owner_progress);
    try std.testing.expectEqualStrings("owner_range", owner_progress.phase);
    try std.testing.expectEqual(@as(u64, 2), owner_progress.report.deleted_stale_ref_rows);
    try std.testing.expect((try db.loadForeignKeyIntegrityProgressRecord(.repair, "orders_customer_id_fkey", "", "customer:m")) == null);

    const repaired_low = try db.validateForeignKeyRefOwnerRange("orders_customer_id_fkey", "customers", "", "customer:m");
    try std.testing.expectEqual(@as(u64, 1), repaired_low.scanned_ref_rows);
    try std.testing.expectEqual(@as(u64, 0), repaired_low.stale_ref_rows);

    const moved_child = (try db.get(alloc, "order:b-moved")) orelse return error.TestExpectedEqual;
    defer alloc.free(moved_child);
    try std.testing.expectEqualStrings("{\"id\":\"order:b-moved\",\"customer_id\":\"customer:z\"}", moved_child);

    const untouched_high = try db.validateForeignKeyRefOwnerRange("orders_customer_id_fkey", "customers", "customer:m", "");
    try std.testing.expectEqual(@as(u64, 2), untouched_high.scanned_ref_rows);
    try std.testing.expectEqual(@as(u64, 1), untouched_high.stale_ref_rows);
}

test "db relational integrity constraints relational foreign key on update stays restrictive when delete cascades" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"},"ref_email":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"unique_constraints":[{"name":"users_email_key","columns":["email"]}],"foreign_keys":[{"name":"users_ref_email_fkey","columns":["ref_email"],"references":{"table":"row","columns":["email"]},"on_delete":"cascade","on_update":"no_action"}]}
    ;
    var parsed_schema = try table_schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try table_schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);
    try std.testing.expectEqual(schema_mod.ForeignKeyAction.cascade, runtime_schema.foreign_keys[0].on_delete);
    try std.testing.expectEqual(schema_mod.ForeignKeyAction.no_action, runtime_schema.foreign_keys[0].on_update);

    try db.batch(.{
        .writes = &.{
            .{ .key = "user:parent", .value = "{\"id\":\"user:parent\",\"email\":\"parent@example.com\"}" },
            .{ .key = "user:child", .value = "{\"id\":\"user:child\",\"ref_email\":\"parent@example.com\"}" },
        },
    });

    try std.testing.expectError(error.ForeignKeyViolation, db.batch(.{
        .writes = &.{.{ .key = "user:parent", .value = "{\"id\":\"user:parent\",\"email\":\"updated@example.com\"}" }},
    }));

    try db.batch(.{ .deletes = &.{"user:parent"} });
    try std.testing.expect((try db.get(alloc, "user:parent")) == null);
    try std.testing.expect((try db.get(alloc, "user:child")) == null);
}

test "db relational integrity constraints relational foreign key on update set null rewrites local unique children" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"},"ref_email":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"unique_constraints":[{"name":"users_email_key","columns":["email"]}],"foreign_keys":[{"name":"users_ref_email_fkey","columns":["ref_email"],"references":{"table":"row","columns":["email"]},"on_update":"set_null"}]}
    ;
    var parsed_schema = try table_schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try table_schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);
    try std.testing.expectEqual(schema_mod.ForeignKeyAction.set_null, runtime_schema.foreign_keys[0].on_update);

    try db.batch(.{
        .writes = &.{
            .{ .key = "user:parent", .value = "{\"id\":\"user:parent\",\"email\":\"parent@example.com\",\"status\":\"active\"}" },
            .{ .key = "user:child", .value = "{\"id\":\"user:child\",\"ref_email\":\"parent@example.com\",\"status\":\"open\"}" },
        },
    });
    const old_parent_raw = (try relational_store_mod.getRawAlloc(alloc, db.core.store, "user:parent")) orelse return error.TestExpectedEqual;
    defer alloc.free(old_parent_raw);
    const old_parent_key = (try relational_store_mod.uniqueConstraintTupleValueAlloc(alloc, old_parent_raw, runtime_schema.unique_constraints[0])) orelse return error.TestExpectedEqual;
    defer alloc.free(old_parent_key);

    try db.batch(.{
        .writes = &.{.{ .key = "user:parent", .value = "{\"id\":\"user:parent\",\"email\":\"updated@example.com\",\"status\":\"active\"}" }},
    });

    const child_after = (try db.get(alloc, "user:child")) orelse return error.TestExpectedEqual;
    defer alloc.free(child_after);
    try std.testing.expectEqualStrings("{\"id\":\"user:child\",\"status\":\"open\"}", child_after);

    const old_ref = try internal_keys.relationalForeignKeyRefKeyAlloc(alloc, "users_ref_email_fkey", "row", "parent@example.com", "row", "user:child");
    defer alloc.free(old_ref);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, old_ref));

    const report = try db.validateForeignKeyRefsInRange("", "");
    try std.testing.expect(report.valid());
    try std.testing.expectEqual(@as(u64, 0), report.referenced_child_rows);
    try std.testing.expectEqual(@as(u64, 0), report.scanned_ref_rows);
}

test "db relational integrity constraints relational foreign key on update cascade rewrites local unique children" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"},"ref_email":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"unique_constraints":[{"name":"users_email_key","columns":["email"]}],"foreign_keys":[{"name":"users_ref_email_fkey","columns":["ref_email"],"references":{"table":"row","columns":["email"]},"on_update":"cascade"}]}
    ;
    var parsed_schema = try table_schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try table_schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);
    try std.testing.expectEqual(schema_mod.ForeignKeyAction.cascade, runtime_schema.foreign_keys[0].on_update);

    try db.batch(.{
        .writes = &.{
            .{ .key = "user:parent", .value = "{\"id\":\"user:parent\",\"email\":\"parent@example.com\",\"status\":\"active\"}" },
            .{ .key = "user:child", .value = "{\"id\":\"user:child\",\"ref_email\":\"parent@example.com\",\"status\":\"open\"}" },
        },
    });
    const old_parent_raw = (try relational_store_mod.getRawAlloc(alloc, db.core.store, "user:parent")) orelse return error.TestExpectedEqual;
    defer alloc.free(old_parent_raw);
    const old_parent_key = (try relational_store_mod.uniqueConstraintTupleValueAlloc(alloc, old_parent_raw, runtime_schema.unique_constraints[0])) orelse return error.TestExpectedEqual;
    defer alloc.free(old_parent_key);

    try db.batch(.{
        .writes = &.{.{ .key = "user:parent", .value = "{\"id\":\"user:parent\",\"email\":\"updated@example.com\",\"status\":\"active\"}" }},
    });

    const child_after = (try db.get(alloc, "user:child")) orelse return error.TestExpectedEqual;
    defer alloc.free(child_after);
    try std.testing.expectEqualStrings("{\"id\":\"user:child\",\"ref_email\":\"updated@example.com\",\"status\":\"open\"}", child_after);

    const old_ref = try internal_keys.relationalForeignKeyRefKeyAlloc(alloc, "users_ref_email_fkey", "row", old_parent_key, "row", "user:child");
    defer alloc.free(old_ref);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, old_ref));
    const new_parent_raw = (try relational_store_mod.getRawAlloc(alloc, db.core.store, "user:parent")) orelse return error.TestExpectedEqual;
    defer alloc.free(new_parent_raw);
    const new_parent_key = (try relational_store_mod.uniqueConstraintTupleValueAlloc(alloc, new_parent_raw, runtime_schema.unique_constraints[0])) orelse return error.TestExpectedEqual;
    defer alloc.free(new_parent_key);
    const new_ref = try internal_keys.relationalForeignKeyRefKeyAlloc(alloc, "users_ref_email_fkey", "row", new_parent_key, "row", "user:child");
    defer alloc.free(new_ref);
    const new_ref_value = try db.core.store.get(alloc, new_ref);
    alloc.free(new_ref_value);

    const report = try db.validateForeignKeyRefsInRange("", "");
    try std.testing.expect(report.valid());
    try std.testing.expectEqual(@as(u64, 1), report.referenced_child_rows);
    try std.testing.expectEqual(@as(u64, 1), report.scanned_ref_rows);
}

test "db relational integrity constraints deferred no action foreign key update validates final transaction state" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"},"ref_email":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"unique_constraints":[{"name":"users_email_key","columns":["email"]}],"foreign_keys":[{"name":"users_ref_email_fkey","columns":["ref_email"],"references":{"table":"row","columns":["email"]},"on_update":"no_action","timing":"deferred"}]}
    ;
    var parsed_schema = try table_schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try table_schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    try db.batch(.{
        .writes = &.{
            .{ .key = "user:parent", .value = "{\"id\":\"user:parent\",\"email\":\"old@example.com\"}" },
            .{ .key = "user:child", .value = "{\"id\":\"user:child\",\"ref_email\":\"old@example.com\"}" },
        },
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "user:parent", .value = "{\"id\":\"user:parent\",\"email\":\"new@example.com\"}" },
            .{ .key = "user:child", .value = "{\"id\":\"user:child\",\"ref_email\":\"new@example.com\"}" },
        },
    });

    const parent = (try db.get(alloc, "user:parent")) orelse return error.TestExpectedEqual;
    defer alloc.free(parent);
    try std.testing.expectEqualStrings("{\"id\":\"user:parent\",\"email\":\"new@example.com\"}", parent);

    const child = (try db.get(alloc, "user:child")) orelse return error.TestExpectedEqual;
    defer alloc.free(child);
    try std.testing.expectEqualStrings("{\"id\":\"user:child\",\"ref_email\":\"new@example.com\"}", child);

    const report = try db.validateForeignKeyRefsInRange("", "");
    try std.testing.expect(report.valid());
}

test "db relational integrity constraints deferred restrict foreign key update remains immediate" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"},"ref_email":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"unique_constraints":[{"name":"users_email_key","columns":["email"]}],"foreign_keys":[{"name":"users_ref_email_fkey","columns":["ref_email"],"references":{"table":"row","columns":["email"]},"on_update":"restrict","timing":"deferred"}]}
    ;
    var parsed_schema = try table_schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try table_schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    try db.batch(.{
        .writes = &.{
            .{ .key = "user:parent", .value = "{\"id\":\"user:parent\",\"email\":\"old@example.com\"}" },
            .{ .key = "user:child", .value = "{\"id\":\"user:child\",\"ref_email\":\"old@example.com\"}" },
        },
    });

    try std.testing.expectError(error.ForeignKeyViolation, db.batch(.{
        .writes = &.{
            .{ .key = "user:parent", .value = "{\"id\":\"user:parent\",\"email\":\"new@example.com\"}" },
            .{ .key = "user:child", .value = "{\"id\":\"user:child\",\"ref_email\":\"new@example.com\"}" },
        },
    }));
}

test "db relational integrity constraints deferred no action foreign key delete validates final transaction state" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"customer_id":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"foreign_keys":[{"name":"orders_customer_id_fkey","columns":["customer_id"],"references":{"table":"row","columns":["_id"]},"on_delete":"no_action","timing":"deferred"}]}
    ;
    var parsed_schema = try table_schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try table_schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    try db.batch(.{
        .writes = &.{
            .{ .key = "customer:delete", .value = "{\"id\":\"customer:delete\"}" },
            .{ .key = "order:delete", .value = "{\"id\":\"order:delete\",\"customer_id\":\"customer:delete\"}" },
        },
    });

    const txn = try db.beginTransaction(31_000);
    try db.writeTransaction(txn, .{
        .deletes = &.{"customer:delete"},
    });
    try db.writeTransaction(txn, .{
        .writes = &.{.{ .key = "order:delete", .value = "{\"id\":\"order:delete\"}" }},
    });
    try db.commitTransaction(txn, 31_001);

    try std.testing.expect((try db.get(alloc, "customer:delete")) == null);
    const child = (try db.get(alloc, "order:delete")) orelse return error.TestExpectedEqual;
    defer alloc.free(child);
    try std.testing.expectEqualStrings("{\"id\":\"order:delete\"}", child);

    const report = try db.validateForeignKeyRefsInRange("", "");
    try std.testing.expect(report.valid());
}

test "db relational integrity constraints deferred restrict foreign key delete remains restrictive" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"customer_id":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"foreign_keys":[{"name":"orders_customer_id_fkey","columns":["customer_id"],"references":{"table":"row","columns":["_id"]},"on_delete":"restrict","timing":"deferred"}]}
    ;
    var parsed_schema = try table_schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try table_schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    try db.batch(.{
        .writes = &.{
            .{ .key = "customer:delete", .value = "{\"id\":\"customer:delete\"}" },
            .{ .key = "order:delete", .value = "{\"id\":\"order:delete\",\"customer_id\":\"customer:delete\"}" },
        },
    });

    const txn = try db.beginTransaction(31_100);
    try db.writeTransaction(txn, .{
        .deletes = &.{"customer:delete"},
    });
    try std.testing.expectError(error.ForeignKeyViolation, db.commitTransaction(txn, 31_101));
    try db.abortTransaction(txn, 31_101);
}

test "db relational integrity constraints relational unique constraints enforce committed scalar values" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .mem = .{} },
    });
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"tenant_id":{"type":"keyword"},"email":{"type":"keyword"},"handle":{"type":"keyword"},"age":{"type":"numeric"},"slug":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"unique_constraints":[{"name":"users_email_key","columns":["email"]},{"name":"users_age_key","columns":["age"]},{"name":"users_tenant_handle_key","columns":["tenant_id","handle"]},{"name":"users_active_slug_key","columns":["slug"],"where":{"all":[{"field":"status","op":"eq","value":"active"}]}}]}
    ;
    var parsed_schema = try schema_api_mod.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try schema_api_mod.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    try db.batch(.{
        .writes = &.{
            .{ .key = "user:1", .value = "{\"id\":\"user:1\",\"email\":\"a@example.com\",\"age\":30}" },
            .{ .key = "user:tenant-a", .value = "{\"id\":\"user:tenant-a\",\"tenant_id\":\"tenant:a\",\"handle\":\"sam\"}" },
            .{ .key = "user:tenant-b", .value = "{\"id\":\"user:tenant-b\",\"tenant_id\":\"tenant:b\",\"handle\":\"sam\"}" },
            .{ .key = "user:partial-a", .value = "{\"id\":\"user:partial-a\",\"tenant_id\":\"tenant:a\"}" },
            .{ .key = "user:partial-b", .value = "{\"id\":\"user:partial-b\",\"tenant_id\":\"tenant:a\"}" },
            .{ .key = "user:null-a", .value = "{\"id\":\"user:null-a\"}" },
            .{ .key = "user:null-b", .value = "{\"id\":\"user:null-b\"}" },
            .{ .key = "user:inactive-a", .value = "{\"id\":\"user:inactive-a\",\"slug\":\"sam\",\"status\":\"inactive\"}" },
            .{ .key = "user:inactive-b", .value = "{\"id\":\"user:inactive-b\",\"slug\":\"sam\",\"status\":\"inactive\"}" },
            .{ .key = "user:active-a", .value = "{\"id\":\"user:active-a\",\"slug\":\"sam\",\"status\":\"active\"}" },
        },
    });

    try std.testing.expectError(error.UniqueConstraintViolation, db.batch(.{
        .writes = &.{.{ .key = "user:2", .value = "{\"id\":\"user:2\",\"email\":\"a@example.com\",\"age\":31}" }},
    }));
    try std.testing.expectError(error.UniqueConstraintViolation, db.batch(.{
        .writes = &.{.{ .key = "user:3", .value = "{\"id\":\"user:3\",\"email\":\"b@example.com\",\"age\":30}" }},
    }));
    try std.testing.expectError(error.UniqueConstraintViolation, db.batch(.{
        .writes = &.{.{ .key = "user:tenant-a-dup", .value = "{\"id\":\"user:tenant-a-dup\",\"tenant_id\":\"tenant:a\",\"handle\":\"sam\"}" }},
    }));
    try std.testing.expectError(error.UniqueConstraintViolation, db.batch(.{
        .writes = &.{.{ .key = "user:active-b", .value = "{\"id\":\"user:active-b\",\"slug\":\"sam\",\"status\":\"active\"}" }},
    }));

    try db.batch(.{
        .writes = &.{.{ .key = "user:1", .value = "{\"id\":\"user:1\",\"email\":\"b@example.com\",\"age\":32}" }},
    });
    try db.batch(.{
        .writes = &.{.{ .key = "user:2", .value = "{\"id\":\"user:2\",\"email\":\"a@example.com\",\"age\":30}" }},
    });

    try db.batch(.{
        .deletes = &.{"user:2"},
    });
    const deleted_user_2_row = try relational_store_mod.getRawAlloc(alloc, db.core.store, "user:2");
    defer if (deleted_user_2_row) |row| alloc.free(row);
    try std.testing.expect(deleted_user_2_row == null);
    const reusable_email_value = try relational_store_mod.bytesTupleValueAlloc(alloc, &.{"a@example.com"});
    defer alloc.free(reusable_email_value);
    const reusable_email_key = try internal_keys.relationalUniqueKeyAlloc(alloc, "users_email_key", reusable_email_value);
    defer alloc.free(reusable_email_key);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, reusable_email_key));
    try db.batch(.{
        .writes = &.{.{ .key = "user:3", .value = "{\"id\":\"user:3\",\"email\":\"a@example.com\",\"age\":30}" }},
    });
    try db.batch(.{
        .writes = &.{.{ .key = "user:tenant-a", .value = "{\"id\":\"user:tenant-a\",\"tenant_id\":\"tenant:a\",\"handle\":\"alex\"}" }},
    });
    try db.batch(.{
        .writes = &.{.{ .key = "user:tenant-a-dup", .value = "{\"id\":\"user:tenant-a-dup\",\"tenant_id\":\"tenant:a\",\"handle\":\"sam\"}" }},
    });
    try db.batch(.{
        .deletes = &.{"user:tenant-a-dup"},
    });
    try db.batch(.{
        .writes = &.{.{ .key = "user:tenant-a-reuse", .value = "{\"id\":\"user:tenant-a-reuse\",\"tenant_id\":\"tenant:a\",\"handle\":\"sam\"}" }},
    });
    try db.batch(.{
        .writes = &.{.{ .key = "user:active-a", .value = "{\"id\":\"user:active-a\",\"slug\":\"sam\",\"status\":\"inactive\"}" }},
    });
    try db.batch(.{
        .writes = &.{.{ .key = "user:active-b", .value = "{\"id\":\"user:active-b\",\"slug\":\"sam\",\"status\":\"active\"}" }},
    });

    const txn_id = try db.beginTransaction(20_000);
    try db.writeIntents(txn_id, &.{
        .{ .key = "user:txn", .value = "{\"id\":\"user:txn\",\"email\":\"a@example.com\",\"age\":40}" },
    }, &.{});
    try std.testing.expectError(error.UniqueConstraintViolation, db.commitTransaction(txn_id, 20_001));
    try db.abortTransaction(txn_id, 20_002);
}

test "db relational integrity constraints relational composite primary keys enforce identity and back foreign keys" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .mem = .{} },
    });
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"tenant_id":{"type":"keyword"},"order_id":{"type":"keyword"},"parent_order_id":{"type":"keyword"},"status":{"type":"keyword"}},"required":["tenant_id","order_id"],"additionalProperties":false}}},"primary_key":{"columns":["tenant_id","order_id"]},"foreign_keys":[{"name":"orders_parent_fkey","columns":["tenant_id","parent_order_id"],"references":{"table":"row","columns":["tenant_id","order_id"]},"on_delete":"restrict","on_update":"restrict","match":"full"}]}
    ;
    var parsed_schema = try schema_api_mod.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try schema_api_mod.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    try db.batch(.{
        .writes = &.{
            .{ .key = "order:parent", .value = "{\"tenant_id\":\"tenant:a\",\"order_id\":\"parent\",\"status\":\"open\"}" },
            .{ .key = "order:child", .value = "{\"tenant_id\":\"tenant:a\",\"order_id\":\"child\",\"parent_order_id\":\"parent\",\"status\":\"open\"}" },
        },
    });

    const parent_row_key = try internal_keys.relationalRowKeyAlloc(alloc, "order:parent");
    defer alloc.free(parent_row_key);
    const parent_row = try db.core.store.get(alloc, parent_row_key);
    defer alloc.free(parent_row);
    const parent_tuple = try relational_store_mod.primaryKeyTupleValueAlloc(alloc, parent_row, runtime_schema.primary_key.?);
    defer alloc.free(parent_tuple);
    const parent_owner_key = try internal_keys.relationalUniqueKeyAlloc(alloc, relational_store_mod.primary_key_constraint_name, parent_tuple);
    defer alloc.free(parent_owner_key);
    const parent_owner = try db.core.store.get(alloc, parent_owner_key);
    defer alloc.free(parent_owner);
    try std.testing.expectEqualStrings("order:parent", parent_owner);

    try db.core.store.delete(parent_owner_key);
    const missing_owner_report = try db.validateUniqueConstraintRowsInRange("", "");
    try std.testing.expect(!missing_owner_report.valid());
    try std.testing.expectEqual(@as(u64, 1), missing_owner_report.missing_unique_rows);
    const repair_owner_report = try db.repairUniqueConstraintRowsInRange("", "");
    try std.testing.expectEqual(@as(u64, 1), repair_owner_report.repaired_unique_rows);
    const validate_repaired_owner_report = try db.validateUniqueConstraintRowsInRange("", "");
    try std.testing.expect(validate_repaired_owner_report.valid());
    const repaired_parent_owner = try db.core.store.get(alloc, parent_owner_key);
    defer alloc.free(repaired_parent_owner);
    try std.testing.expectEqualStrings("order:parent", repaired_parent_owner);

    try std.testing.expectError(error.UniqueConstraintViolation, db.batch(.{
        .writes = &.{.{ .key = "order:duplicate", .value = "{\"tenant_id\":\"tenant:a\",\"order_id\":\"parent\",\"status\":\"duplicate\"}" }},
    }));
    try std.testing.expectError(error.InvalidBatchRequest, db.batch(.{
        .writes = &.{.{ .key = "order:missing-key", .value = "{\"tenant_id\":\"tenant:a\",\"status\":\"missing\"}" }},
    }));
    try std.testing.expectError(error.ForeignKeyViolation, db.batch(.{
        .writes = &.{.{ .key = "order:missing-parent", .value = "{\"tenant_id\":\"tenant:a\",\"order_id\":\"missing-parent\",\"parent_order_id\":\"absent\"}" }},
    }));
    try std.testing.expectError(error.ForeignKeyViolation, db.batch(.{
        .deletes = &.{"order:parent"},
    }));
    try std.testing.expectError(error.ForeignKeyViolation, db.batch(.{
        .writes = &.{.{ .key = "order:parent", .value = "{\"tenant_id\":\"tenant:a\",\"order_id\":\"parent-renamed\",\"status\":\"renamed\"}" }},
    }));

    const report = try db.validateForeignKeyRefsInRange("", "");
    try std.testing.expect(report.valid());
}

test "db relational integrity constraints unique constraint integrity repair rebuilds backing rows" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .mem = .{} },
    });
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"unique_constraints":[{"name":"users_email_key","columns":["email"]}]}
    ;
    var parsed_schema = try schema_api_mod.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try schema_api_mod.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    try db.batch(.{ .writes = &.{
        .{ .key = "user:ada", .value = "{\"id\":\"user:ada\",\"email\":\"ada@example.test\"}" },
        .{ .key = "user:grace", .value = "{\"id\":\"user:grace\",\"email\":\"grace@example.test\"}" },
    } });

    const ada_value = try relational_store_mod.bytesTupleValueAlloc(alloc, &.{"ada@example.test"});
    defer alloc.free(ada_value);
    const grace_value = try relational_store_mod.bytesTupleValueAlloc(alloc, &.{"grace@example.test"});
    defer alloc.free(grace_value);
    const stale_value = try relational_store_mod.bytesTupleValueAlloc(alloc, &.{"stale@example.test"});
    defer alloc.free(stale_value);
    const ada_key = try internal_keys.relationalUniqueKeyAlloc(alloc, "users_email_key", ada_value);
    defer alloc.free(ada_key);
    const grace_key = try internal_keys.relationalUniqueKeyAlloc(alloc, "users_email_key", grace_value);
    defer alloc.free(grace_key);
    const stale_key = try internal_keys.relationalUniqueKeyAlloc(alloc, "users_email_key", stale_value);
    defer alloc.free(stale_key);

    try db.core.store.putBatch(&.{
        .{ .key = grace_key, .value = "user:wrong" },
        .{ .key = stale_key, .value = "user:ghost" },
    }, &.{ada_key});

    const validate = try db.validateUniqueConstraintRowsInRange("", "");
    try std.testing.expect(!validate.valid());
    try std.testing.expectEqual(@as(u64, 2), validate.scanned_rows);
    try std.testing.expectEqual(@as(u64, 2), validate.expected_unique_rows);
    try std.testing.expectEqual(@as(u64, 1), validate.missing_unique_rows);
    try std.testing.expectEqual(@as(u64, 2), validate.stale_unique_rows);

    const dry_run = try db.dryRunRepairUniqueConstraintRowsInRange("", "");
    try std.testing.expect(!dry_run.valid());
    try std.testing.expectEqual(@as(u64, 2), dry_run.repaired_unique_rows);
    try std.testing.expectEqual(@as(u64, 1), dry_run.deleted_stale_unique_rows);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, ada_key));

    const repair = try db.repairUniqueConstraintRowsInRange("", "");
    try std.testing.expect(!repair.valid());
    try std.testing.expectEqual(@as(u64, 2), repair.repaired_unique_rows);
    try std.testing.expectEqual(@as(u64, 1), repair.deleted_stale_unique_rows);
    const repair_progress = (try db.loadUniqueConstraintIntegrityProgressRecord(.repair, "", "")) orelse return error.TestUnexpectedResult;
    defer db.freeUniqueConstraintIntegrityProgressRecord(repair_progress);
    try std.testing.expectEqualStrings("repair", repair_progress.mode);
    try std.testing.expect(repair_progress.completed);
    try std.testing.expect(repair_progress.valid);
    try std.testing.expectEqual(@as(u64, 2), repair_progress.report.repaired_unique_rows);
    try std.testing.expectEqual(@as(u64, 1), repair_progress.report.deleted_stale_unique_rows);

    const after = try db.validateUniqueConstraintRowsInRange("", "");
    try std.testing.expect(after.valid());
    try std.testing.expectEqual(@as(u64, 2), after.scanned_rows);
    try std.testing.expectEqual(@as(u64, 2), after.scanned_unique_rows);
    const validate_progress = (try db.loadUniqueConstraintIntegrityProgressRecord(.validate, "", "")) orelse return error.TestUnexpectedResult;
    defer db.freeUniqueConstraintIntegrityProgressRecord(validate_progress);
    try std.testing.expectEqualStrings("validate", validate_progress.mode);
    try std.testing.expect(validate_progress.completed);
    try std.testing.expect(validate_progress.valid);
    try std.testing.expectEqual(@as(u64, 2), validate_progress.report.scanned_unique_rows);
    const dry_run_progress = (try db.loadUniqueConstraintIntegrityProgressRecord(.dry_run, "", "")) orelse return error.TestUnexpectedResult;
    defer db.freeUniqueConstraintIntegrityProgressRecord(dry_run_progress);
    try std.testing.expectEqualStrings("dry_run", dry_run_progress.mode);
    try std.testing.expect(!dry_run_progress.valid);
    try std.testing.expectEqual(@as(u64, 2), dry_run_progress.report.repaired_unique_rows);
    const all_progress = try db.listUniqueConstraintIntegrityProgressRecords();
    defer db.freeUniqueConstraintIntegrityProgressRecords(all_progress);
    try std.testing.expectEqual(@as(usize, 3), all_progress.len);
    const repaired_ada_owner = try db.core.store.get(alloc, ada_key);
    defer alloc.free(repaired_ada_owner);
    try std.testing.expectEqualStrings("user:ada", repaired_ada_owner);
    const repaired_grace_owner = try db.core.store.get(alloc, grace_key);
    defer alloc.free(repaired_grace_owner);
    try std.testing.expectEqualStrings("user:grace", repaired_grace_owner);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, stale_key));
}

test "db relational foreign keys enforce parent existence and restrict deletes" {
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"customer_id":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"foreign_keys":[{"name":"orders_customer_id_fkey","columns":["customer_id"],"references":{"table":"customers","columns":["_id"]},"on_delete":"restrict"}]}
    ;
    var parsed_schema = try table_schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try table_schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    try std.testing.expectError(error.ForeignKeyViolation, db.batch(.{
        .writes = &.{.{ .key = "order:missing", .value = "{\"id\":\"order:missing\",\"customer_id\":\"customer:missing\"}" }},
    }));

    try db.batch(.{
        .writes = &.{
            .{ .key = "order:1", .value = "{\"id\":\"order:1\",\"customer_id\":\"customer:a\"}" },
            .{ .key = "customer:a", .value = "{\"id\":\"customer:a\"}" },
        },
    });

    const fk_ref = try internal_keys.relationalForeignKeyRefKeyAlloc(alloc, "orders_customer_id_fkey", "customers", "customer:a", "row", "order:1");
    defer alloc.free(fk_ref);
    const ref_value = try db.core.store.get(alloc, fk_ref);
    defer alloc.free(ref_value);
    try std.testing.expectEqualStrings("", ref_value);

    try db.core.store.putBatch(&.{}, &.{fk_ref});
    const missing_ref_report = try db.validateForeignKeyRefsInRange("", "");
    try std.testing.expect(!missing_ref_report.valid());
    try std.testing.expectEqual(@as(u64, 1), missing_ref_report.missing_ref_rows);
    try std.testing.expectEqual(@as(u64, 0), missing_ref_report.missing_parent_rows);
    try std.testing.expectEqual(@as(u64, 0), missing_ref_report.repaired_ref_rows);

    const missing_ref_violations = try db.listForeignKeyViolationsInRange("", "");
    defer db.freeForeignKeyIntegrityViolations(missing_ref_violations);
    try std.testing.expectEqual(@as(usize, 1), missing_ref_violations.len);
    try std.testing.expectEqual(relational_store_mod.ForeignKeyIntegrityViolationKind.missing_ref, missing_ref_violations[0].kind);
    try std.testing.expectEqualStrings("orders_customer_id_fkey", missing_ref_violations[0].constraint_name);
    try std.testing.expectEqualStrings("order:1", missing_ref_violations[0].child_key);
    try std.testing.expectEqualStrings("customer:a", missing_ref_violations[0].parent_key);

    const dry_run_ref_report = try db.dryRunRepairForeignKeyRefsInRange("", "");
    try std.testing.expect(!dry_run_ref_report.valid());
    try std.testing.expectEqual(@as(u64, 1), dry_run_ref_report.missing_ref_rows);
    try std.testing.expectEqual(@as(u64, 1), dry_run_ref_report.repaired_ref_rows);

    const after_dry_run_ref_report = try db.validateForeignKeyRefsInRange("", "");
    try std.testing.expect(!after_dry_run_ref_report.valid());
    try std.testing.expectEqual(@as(u64, 1), after_dry_run_ref_report.missing_ref_rows);
    try std.testing.expectEqual(@as(u64, 0), after_dry_run_ref_report.repaired_ref_rows);

    const repair_ref_report = try db.repairForeignKeyRefsInRange("", "");
    try std.testing.expect(!repair_ref_report.valid());
    try std.testing.expectEqual(@as(u64, 1), repair_ref_report.missing_ref_rows);
    try std.testing.expectEqual(@as(u64, 1), repair_ref_report.repaired_ref_rows);

    const repaired_ref_report = try db.validateForeignKeyRefsInRange("", "");
    try std.testing.expect(repaired_ref_report.valid());
    try std.testing.expectEqual(@as(u64, 0), repaired_ref_report.missing_ref_rows);
    try std.testing.expectEqual(@as(u64, 1), repaired_ref_report.scanned_ref_rows);

    const validate_progress = (try db.loadForeignKeyIntegrityProgressRecord(.validate, null, "", "")) orelse return error.TestUnexpectedResult;
    defer db.freeForeignKeyIntegrityProgressRecord(validate_progress);
    try std.testing.expectEqualStrings("validate", validate_progress.mode);
    try std.testing.expect(validate_progress.completed);
    try std.testing.expect(validate_progress.valid);
    try std.testing.expectEqualStrings("", validate_progress.lower_doc_key);
    try std.testing.expectEqualStrings("", validate_progress.upper_doc_key);
    try std.testing.expectEqual(@as(u64, 0), validate_progress.report.missing_ref_rows);
    try std.testing.expectEqual(@as(u64, 1), validate_progress.report.scanned_ref_rows);

    try std.testing.expectError(error.ForeignKeyViolation, db.batch(.{
        .deletes = &.{"customer:a"},
    }));

    try db.batch(.{
        .deletes = &.{ "customer:a", "order:1" },
    });
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, fk_ref));
    try std.testing.expect((try db.get(alloc, "customer:a")) == null);
    try std.testing.expect((try db.get(alloc, "order:1")) == null);

    const missing_txn = try db.beginTransaction(10_000);
    try db.writeIntents(missing_txn, &.{
        .{ .key = "order:txn-missing", .value = "{\"id\":\"order:txn-missing\",\"customer_id\":\"customer:txn-missing\"}" },
    }, &.{});
    try std.testing.expectError(error.ForeignKeyViolation, db.commitTransaction(missing_txn, 10_001));
    try db.abortTransaction(missing_txn, 10_002);

    const create_txn = try db.beginTransaction(11_000);
    try db.writeIntents(create_txn, &.{
        .{ .key = "a:order:txn", .value = "{\"id\":\"a:order:txn\",\"customer_id\":\"z:customer:txn\"}" },
        .{ .key = "z:customer:txn", .value = "{\"id\":\"z:customer:txn\"}" },
    }, &.{});
    try db.commitTransaction(create_txn, 11_001);

    const txn_fk_ref = try internal_keys.relationalForeignKeyRefKeyAlloc(alloc, "orders_customer_id_fkey", "customers", "z:customer:txn", "row", "a:order:txn");
    defer alloc.free(txn_fk_ref);
    const txn_ref_value = try db.core.store.get(alloc, txn_fk_ref);
    defer alloc.free(txn_ref_value);
    try std.testing.expectEqualStrings("", txn_ref_value);

    const delete_txn = try db.beginTransaction(12_000);
    try db.writeTransaction(delete_txn, .{
        .deletes = &.{ "z:customer:txn", "a:order:txn" },
    });
    try db.commitTransaction(delete_txn, 12_001);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, txn_fk_ref));

    const stats = try db.stats(alloc);
    defer types.freeDBStats(alloc, stats);
    try std.testing.expectEqual(@as(u64, 2), stats.foreign_keys.child_write_rejects);
    try std.testing.expectEqual(@as(u64, 1), stats.foreign_keys.parent_delete_rejects);
    try std.testing.expectEqual(@as(u64, 3), stats.foreign_keys.validation_runs);
    try std.testing.expectEqual(@as(u64, 1), stats.foreign_keys.dry_run_runs);
    try std.testing.expectEqual(@as(u64, 1), stats.foreign_keys.repair_runs);
    try std.testing.expectEqual(@as(u64, 4), stats.foreign_keys.missing_ref_rows);
    try std.testing.expectEqual(@as(u64, 2), stats.foreign_keys.repaired_ref_rows);
}

test "db relational foreign keys no_action preserves action and blocks parent deletes" {
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"customer_id":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"foreign_keys":[{"name":"orders_customer_id_fkey","columns":["customer_id"],"references":{"table":"customers","columns":["_id"]},"on_delete":"no_action"}]}
    ;
    var parsed_schema = try table_schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try table_schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);
    try std.testing.expectEqual(schema_mod.ForeignKeyAction.no_action, runtime_schema.foreign_keys[0].on_delete);

    try db.batch(.{
        .writes = &.{
            .{ .key = "customer:no-action", .value = "{\"id\":\"customer:no-action\"}" },
            .{ .key = "order:no-action", .value = "{\"id\":\"order:no-action\",\"customer_id\":\"customer:no-action\"}" },
        },
    });

    try std.testing.expectError(error.ForeignKeyViolation, db.batch(.{
        .deletes = &.{"customer:no-action"},
    }));

    try db.batch(.{ .deletes = &.{ "order:no-action", "customer:no-action" } });
    try std.testing.expect((try db.get(alloc, "customer:no-action")) == null);
}

test "db relational foreign keys setSchema accepts deferred and rejects controller-only states" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const relational_columns = [_]schema_mod.RelationalColumn{
        .{ .name = "id", .path = "id", .field_type = .keyword, .nullable = false },
        .{ .name = "customer_id", .path = "customer_id", .field_type = .keyword, .nullable = true },
    };

    const deferred_foreign_keys = [_]schema_mod.ForeignKey{.{
        .name = "orders_customer_id_fkey",
        .child_columns = &.{"customer_id"},
        .parent_table = "customers",
        .parent_columns = &.{"_id"},
        .on_delete = .restrict,
        .timing = .deferred,
    }};
    try db.setSchema(.{
        .version = 1,
        .default_type = "row",
        .storage_mode = .relational,
        .relational_columns = relational_columns[0..],
        .foreign_keys = deferred_foreign_keys[0..],
    });

    const validating_foreign_keys = [_]schema_mod.ForeignKey{.{
        .name = "orders_customer_id_fkey",
        .child_columns = &.{"customer_id"},
        .parent_table = "customers",
        .parent_columns = &.{"_id"},
        .on_delete = .restrict,
        .validation_state = .validating,
    }};
    try std.testing.expectError(error.InvalidSchemaUpdateRequest, db.setSchema(.{
        .version = 1,
        .default_type = "row",
        .storage_mode = .relational,
        .relational_columns = relational_columns[0..],
        .foreign_keys = validating_foreign_keys[0..],
    }));

    const invalid_foreign_keys = [_]schema_mod.ForeignKey{.{
        .name = "orders_customer_id_fkey",
        .child_columns = &.{"customer_id"},
        .parent_table = "customers",
        .parent_columns = &.{"_id"},
        .on_delete = .restrict,
        .validation_state = .invalid,
    }};
    try std.testing.expectError(error.InvalidSchemaUpdateRequest, db.setSchema(.{
        .version = 1,
        .default_type = "row",
        .storage_mode = .relational,
        .relational_columns = relational_columns[0..],
        .foreign_keys = invalid_foreign_keys[0..],
    }));
}

test "db relational foreign keys integrity ignores unvalidated constraints" {
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"customer_id":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"foreign_keys":[{"name":"orders_customer_id_fkey","columns":["customer_id"],"references":{"table":"customers","columns":["_id"]},"on_delete":"restrict","validation_state":"unvalidated"}]}
    ;
    var parsed_schema = try table_schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try table_schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    try db.batch(.{
        .writes = &.{.{ .key = "order:unvalidated", .value = "{\"id\":\"order:unvalidated\",\"customer_id\":\"customer:missing\"}" }},
    });

    const report = try db.validateForeignKeyRefsInRange("", "");
    try std.testing.expect(report.valid());
    try std.testing.expectEqual(@as(u64, 0), report.scanned_child_rows);
    try std.testing.expectEqual(@as(u64, 0), report.referenced_child_rows);
    try std.testing.expectEqual(@as(u64, 0), report.missing_parent_rows);
    try std.testing.expectEqual(@as(u64, 0), report.missing_ref_rows);

    const dry_run_report = try db.dryRunRepairForeignKeyRefsInRange("", "");
    try std.testing.expect(dry_run_report.valid());
    try std.testing.expectEqual(@as(u64, 0), dry_run_report.scanned_child_rows);
    try std.testing.expectEqual(@as(u64, 0), dry_run_report.repaired_ref_rows);

    const repair_report = try db.repairForeignKeyRefsInRange("", "");
    try std.testing.expect(repair_report.valid());
    try std.testing.expectEqual(@as(u64, 0), repair_report.scanned_child_rows);
    try std.testing.expectEqual(@as(u64, 0), repair_report.repaired_ref_rows);

    const violations = try db.listForeignKeyViolationsInRange("", "");
    defer db.freeForeignKeyIntegrityViolations(violations);
    try std.testing.expectEqual(@as(usize, 0), violations.len);

    const explain = try db.explainForeignKeyDelete("customer:missing");
    try std.testing.expect(!explain.exists);
    try std.testing.expectEqual(@as(u64, 0), explain.planned_row_deletes);
    try std.testing.expectEqual(@as(u64, 0), explain.planned_index_deletes);
    try std.testing.expectEqual(@as(u64, 0), explain.planned_writes);

    try std.testing.expect((try db.loadForeignKeyIntegrityProgressRecord(.validate, null, "", "")) == null);
    try std.testing.expect((try db.loadForeignKeyIntegrityProgressRecord(.dry_run, null, "", "")) == null);
    try std.testing.expect((try db.loadForeignKeyIntegrityProgressRecord(.repair, null, "", "")) == null);

    const stats = try db.stats(alloc);
    defer types.freeDBStats(alloc, stats);
    try std.testing.expectEqual(@as(u64, 0), stats.foreign_keys.validation_runs);
    try std.testing.expectEqual(@as(u64, 0), stats.foreign_keys.dry_run_runs);
    try std.testing.expectEqual(@as(u64, 0), stats.foreign_keys.repair_runs);
    try std.testing.expectEqual(@as(u64, 0), stats.foreign_keys.scanned_child_rows);

    const adoption_report = try db.validateForeignKeyRefsInRangeForConstraint("orders_customer_id_fkey", "", "");
    try std.testing.expect(!adoption_report.valid());
    try std.testing.expectEqual(@as(u64, 1), adoption_report.scanned_child_rows);
    try std.testing.expectEqual(@as(u64, 1), adoption_report.referenced_child_rows);
    try std.testing.expectEqual(@as(u64, 1), adoption_report.missing_parent_rows);
    try std.testing.expectEqual(@as(u64, 1), adoption_report.missing_ref_rows);

    const adoption_violations = try db.listForeignKeyViolationsInRangeForConstraint("orders_customer_id_fkey", "", "");
    defer db.freeForeignKeyIntegrityViolations(adoption_violations);
    try std.testing.expectEqual(@as(usize, 2), adoption_violations.len);
}

test "db relational foreign keys set nullable children null on parent delete" {
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"customer_id":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"foreign_keys":[{"name":"orders_customer_id_fkey","columns":["customer_id"],"references":{"table":"customers","columns":["_id"]},"on_delete":"set_null"}]}
    ;
    var parsed_schema = try table_schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try table_schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    try db.batch(.{
        .writes = &.{
            .{ .key = "customer:set-null", .value = "{\"id\":\"customer:set-null\"}" },
            .{ .key = "order:set-null", .value = "{\"id\":\"order:set-null\",\"customer_id\":\"customer:set-null\",\"status\":\"open\"}" },
        },
    });

    try db.batch(.{ .deletes = &.{"customer:set-null"} });

    const child = (try db.get(alloc, "order:set-null")) orelse return error.TestExpectedEqual;
    defer alloc.free(child);
    try std.testing.expectEqualStrings("{\"id\":\"order:set-null\",\"status\":\"open\"}", child);
    try std.testing.expect((try db.get(alloc, "customer:set-null")) == null);

    const fk_ref = try internal_keys.relationalForeignKeyRefKeyAlloc(alloc, "orders_customer_id_fkey", "customers", "customer:set-null", "row", "order:set-null");
    defer alloc.free(fk_ref);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, fk_ref));

    const report = try db.validateForeignKeyRefsInRange("", "");
    try std.testing.expect(report.valid());
    try std.testing.expectEqual(@as(u64, 0), report.referenced_child_rows);
    try std.testing.expectEqual(@as(u64, 0), report.scanned_ref_rows);
}

test "db relational foreign keys cascade deletes through local children" {
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"parent_id":{"type":"keyword"},"title":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"foreign_keys":[{"name":"nodes_parent_id_fkey","columns":["parent_id"],"references":{"table":"nodes","columns":["_id"]},"on_delete":"cascade"}]}
    ;
    var parsed_schema = try table_schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try table_schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    try db.batch(.{
        .writes = &.{
            .{ .key = "node:root", .value = "{\"id\":\"node:root\",\"title\":\"root\"}" },
            .{ .key = "node:child", .value = "{\"id\":\"node:child\",\"parent_id\":\"node:root\",\"title\":\"child\"}" },
            .{ .key = "node:grandchild", .value = "{\"id\":\"node:grandchild\",\"parent_id\":\"node:child\",\"title\":\"grandchild\"}" },
        },
    });

    try db.batch(.{ .deletes = &.{"node:root"} });
    try std.testing.expect((try db.get(alloc, "node:root")) == null);
    try std.testing.expect((try db.get(alloc, "node:child")) == null);
    try std.testing.expect((try db.get(alloc, "node:grandchild")) == null);

    const root_ref = try internal_keys.relationalForeignKeyRefKeyAlloc(alloc, "nodes_parent_id_fkey", "nodes", "node:root", "row", "node:child");
    defer alloc.free(root_ref);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, root_ref));
    const child_ref = try internal_keys.relationalForeignKeyRefKeyAlloc(alloc, "nodes_parent_id_fkey", "nodes", "node:child", "row", "node:grandchild");
    defer alloc.free(child_ref);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, child_ref));

    const report = try db.validateForeignKeyRefsInRange("", "");
    try std.testing.expect(report.valid());
    try std.testing.expectEqual(@as(u64, 0), report.scanned_child_rows);
    try std.testing.expectEqual(@as(u64, 0), report.scanned_ref_rows);
}

test "db relational foreign keys can reference local unique parent columns" {
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"},"ref_email":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"unique_constraints":[{"name":"users_email_key","columns":["email"]}],"foreign_keys":[{"name":"users_ref_email_fkey","columns":["ref_email"],"references":{"table":"row","columns":["email"]},"on_delete":"restrict"}]}
    ;
    var parsed_schema = try table_schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try table_schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    try std.testing.expectError(error.ForeignKeyViolation, db.batch(.{
        .writes = &.{.{ .key = "user:child-missing", .value = "{\"id\":\"user:child-missing\",\"ref_email\":\"missing@example.com\"}" }},
    }));

    try db.batch(.{
        .writes = &.{
            .{ .key = "user:parent", .value = "{\"id\":\"user:parent\",\"email\":\"parent@example.com\"}" },
            .{ .key = "user:child", .value = "{\"id\":\"user:child\",\"ref_email\":\"parent@example.com\"}" },
        },
    });

    const valid_report = try db.validateForeignKeyRefsInRange("", "");
    try std.testing.expect(valid_report.valid());
    try std.testing.expectEqual(@as(u64, 2), valid_report.scanned_child_rows);
    try std.testing.expectEqual(@as(u64, 1), valid_report.referenced_child_rows);
    try std.testing.expectEqual(@as(u64, 1), valid_report.scanned_ref_rows);

    try std.testing.expectError(error.ForeignKeyViolation, db.batch(.{
        .deletes = &.{"user:parent"},
    }));
    try std.testing.expectError(error.ForeignKeyViolation, db.batch(.{
        .writes = &.{.{ .key = "user:parent", .value = "{\"id\":\"user:parent\",\"email\":\"new-parent@example.com\"}" }},
    }));

    try db.batch(.{
        .writes = &.{.{ .key = "user:child", .value = "{\"id\":\"user:child\"}" }},
    });
    try db.batch(.{
        .writes = &.{.{ .key = "user:parent", .value = "{\"id\":\"user:parent\",\"email\":\"new-parent@example.com\"}" }},
    });

    const create_txn = try db.beginTransaction(30_000);
    try db.writeIntents(create_txn, &.{
        .{ .key = "user:a-txn-parent", .value = "{\"id\":\"user:a-txn-parent\",\"email\":\"txn-parent@example.com\"}" },
        .{ .key = "user:z-txn-child", .value = "{\"id\":\"user:z-txn-child\",\"ref_email\":\"txn-parent@example.com\"}" },
    }, &.{});
    try db.commitTransaction(create_txn, 30_001);

    try db.batch(.{
        .writes = &.{
            .{ .key = "user:diag-parent", .value = "{\"id\":\"user:diag-parent\",\"email\":\"diag@example.com\"}" },
            .{ .key = "user:diag-child", .value = "{\"id\":\"user:diag-child\",\"ref_email\":\"diag@example.com\"}" },
        },
    });
    const diag_parent_row = try relational_store_mod.rowKeyAlloc(alloc, "user:diag-parent");
    defer alloc.free(diag_parent_row);
    try db.core.store.putBatch(&.{}, &.{diag_parent_row});

    const diag_violations = try db.listForeignKeyViolationsInRange("user:diag", "user:diag~");
    defer db.freeForeignKeyIntegrityViolations(diag_violations);
    try std.testing.expectEqual(@as(usize, 1), diag_violations.len);
    try std.testing.expectEqual(relational_store_mod.ForeignKeyIntegrityViolationKind.missing_parent, diag_violations[0].kind);
    try std.testing.expectEqualStrings("users_ref_email_fkey", diag_violations[0].constraint_name);
    try std.testing.expectEqualStrings("user:diag-child", diag_violations[0].child_key);
    try std.testing.expectEqual(@as(usize, 1), diag_violations[0].parent_values.len);
    try std.testing.expectEqualStrings("email", diag_violations[0].parent_values[0].column);
    try std.testing.expectEqualStrings("diag@example.com", diag_violations[0].parent_values[0].value);
    try std.testing.expect(diag_violations[0].observed_parent_key == null);
    try std.testing.expectEqual(@as(usize, 0), diag_violations[0].observed_parent_values.len);
}

test "db relational foreign keys allow multiple constraints on the same child columns" {
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"customer_id":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"foreign_keys":[{"name":"orders_customer_id_row_fkey","columns":["customer_id"],"references":{"table":"row","columns":["_id"]},"on_delete":"restrict"},{"name":"orders_customer_id_customers_fkey","columns":["customer_id"],"references":{"table":"customers","columns":["_id"]},"on_delete":"restrict"}]}
    ;
    var parsed_schema = try table_schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try table_schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    try db.batch(.{
        .writes = &.{
            .{ .key = "parent:shared", .value = "{\"id\":\"parent:shared\"}" },
            .{ .key = "order:shared", .value = "{\"id\":\"order:shared\",\"customer_id\":\"parent:shared\",\"status\":\"open\"}" },
        },
    });

    const row_ref = try internal_keys.relationalForeignKeyRefKeyAlloc(alloc, "orders_customer_id_row_fkey", "row", "parent:shared", "row", "order:shared");
    defer alloc.free(row_ref);
    const row_ref_value = try db.core.store.get(alloc, row_ref);
    defer alloc.free(row_ref_value);
    try std.testing.expectEqualStrings("", row_ref_value);

    const customers_ref = try internal_keys.relationalForeignKeyRefKeyAlloc(alloc, "orders_customer_id_customers_fkey", "customers", "parent:shared", "row", "order:shared");
    defer alloc.free(customers_ref);
    const customers_ref_value = try db.core.store.get(alloc, customers_ref);
    defer alloc.free(customers_ref_value);
    try std.testing.expectEqualStrings("", customers_ref_value);

    try std.testing.expectError(error.ForeignKeyViolation, db.batch(.{
        .deletes = &.{"parent:shared"},
    }));
}

test "db relational foreign keys reject partial match full composite references" {
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"tenant_id":{"type":"keyword"},"email":{"type":"keyword"},"customer_email":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"unique_constraints":[{"name":"users_tenant_email_key","columns":["tenant_id","email"]}],"foreign_keys":[{"name":"users_customer_tenant_email_fkey","columns":["tenant_id","customer_email"],"references":{"table":"row","columns":["tenant_id","email"]},"match":"full"}]}
    ;
    var parsed_schema = try table_schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try table_schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    const all_null_txn = try db.beginTransaction(14_942);
    try db.writeTransaction(all_null_txn, .{
        .writes = &.{.{ .key = "user:all-null", .value = "{\"id\":\"user:all-null\"}" }},
    });
    try db.commitTransaction(all_null_txn, 14_943);

    const partial_txn = try db.beginTransaction(14_944);
    try std.testing.expectError(error.ForeignKeyViolation, db.writeTransaction(partial_txn, .{
        .writes = &.{.{ .key = "user:partial", .value = "{\"id\":\"user:partial\",\"tenant_id\":\"tenant:1\"}" }},
    }));
    try db.abortTransaction(partial_txn, 14_945);
}

test "db relational integrity transaction externalized foreign key parent checks still maintain refs" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"customer_id":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"foreign_keys":[{"name":"orders_customer_id_fkey","columns":["customer_id"],"references":{"table":"customers","columns":["_id"]},"on_delete":"restrict"}]}
    ;
    var parsed_schema = try table_schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try table_schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    const local_txn = try db.beginTransaction(14_000);
    try db.writeTransaction(local_txn, .{
        .writes = &.{.{ .key = "order:local", .value = "{\"id\":\"order:local\",\"customer_id\":\"customer:missing\"}" }},
    });
    try std.testing.expectError(error.ForeignKeyViolation, db.commitTransaction(local_txn, 14_001));
    try db.abortTransaction(local_txn, 14_002);

    const routed_txn = try db.beginTransaction(14_500);
    try db.writeTransaction(routed_txn, .{
        .writes = &.{.{ .key = "order:routed", .value = "{\"id\":\"order:routed\",\"customer_id\":\"customer:routed\"}" }},
        .foreign_key_externalized_parent_checks = &.{.{
            .constraint_name = "orders_customer_id_fkey",
            .child_table = "row",
            .child_key = "order:routed",
            .parent_table = "customers",
            .parent_key = "customer:routed",
            .timing = .immediate,
        }},
    });
    try db.commitTransaction(routed_txn, 14_501);

    const row = (try db.get(alloc, "order:routed")) orelse return error.TestExpectedEqual;
    defer alloc.free(row);
    try std.testing.expectEqualStrings("{\"id\":\"order:routed\",\"customer_id\":\"customer:routed\"}", row);

    const ref_key = try internal_keys.relationalForeignKeyRefKeyAlloc(alloc, "orders_customer_id_fkey", "customers", "customer:routed", "row", "order:routed");
    defer alloc.free(ref_key);
    const ref_value = try db.core.store.get(alloc, ref_key);
    defer alloc.free(ref_value);
    try std.testing.expectEqualStrings("", ref_value);
}

test "db relational integrity transaction externalized foreign key proof records require local fk schema" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const schemaless_txn = try db.beginTransaction(14_600);
    try std.testing.expectError(error.ForeignKeyViolation, db.writeTransaction(schemaless_txn, .{
        .writes = &.{.{ .key = "order:schemaless-proof", .value = "{\"customer_id\":\"customer:proof\"}" }},
        .foreign_key_externalized_parent_checks = &.{.{
            .constraint_name = "orders_customer_id_fkey",
            .child_table = "row",
            .child_key = "order:schemaless-proof",
            .parent_table = "customers",
            .parent_key = "customer:proof",
        }},
    }));
    try db.abortTransaction(schemaless_txn, 14_601);

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"customer_id":{"type":"keyword"}},"additionalProperties":false}}}}
    ;
    var parsed_schema = try table_schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try table_schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    const no_fk_txn = try db.beginTransaction(14_610);
    try std.testing.expectError(error.ForeignKeyViolation, db.writeTransaction(no_fk_txn, .{
        .writes = &.{.{ .key = "order:no-fk-proof", .value = "{\"customer_id\":\"customer:proof\"}" }},
        .foreign_key_externalized_parent_checks = &.{.{
            .constraint_name = "orders_customer_id_fkey",
            .child_table = "row",
            .child_key = "order:no-fk-proof",
            .parent_table = "customers",
            .parent_key = "customer:proof",
        }},
    }));
    try db.abortTransaction(no_fk_txn, 14_611);
}

test "db relational integrity transaction deferred foreign keys validate at local transaction commit and reject externalized checks" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"customer_id":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"foreign_keys":[{"name":"orders_customer_id_fkey","columns":["customer_id"],"references":{"table":"row","columns":["_id"]},"on_delete":"restrict","timing":"deferred"}]}
    ;
    var parsed_schema = try table_schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try table_schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    const missing_txn = try db.beginTransaction(14_750);
    try db.writeTransaction(missing_txn, .{
        .writes = &.{.{ .key = "aa-order:missing", .value = "{\"id\":\"aa-order:missing\",\"customer_id\":\"zz-customer:missing\"}" }},
    });
    try std.testing.expectError(error.ForeignKeyViolation, db.commitTransaction(missing_txn, 14_751));
    try db.abortTransaction(missing_txn, 14_752);

    const ordered_txn = try db.beginTransaction(14_800);
    try db.writeTransaction(ordered_txn, .{
        .writes = &.{
            .{ .key = "aa-order:deferred", .value = "{\"id\":\"aa-order:deferred\",\"customer_id\":\"zz-customer:deferred\"}" },
            .{ .key = "zz-customer:deferred", .value = "{\"id\":\"zz-customer:deferred\"}" },
        },
    });
    try db.commitTransaction(ordered_txn, 14_801);

    const child = (try db.get(alloc, "aa-order:deferred")) orelse return error.TestExpectedEqual;
    defer alloc.free(child);
    try std.testing.expectEqualStrings("{\"id\":\"aa-order:deferred\",\"customer_id\":\"zz-customer:deferred\"}", child);

    const missing_second_write_txn = try db.beginTransaction(14_850);
    try db.writeTransaction(missing_second_write_txn, .{
        .writes = &.{.{ .key = "aa-order:externalized", .value = "{\"id\":\"aa-order:externalized\",\"customer_id\":\"zz-customer:externalized\"}" }},
    });
    try std.testing.expectError(error.ForeignKeyViolation, db.commitTransaction(missing_second_write_txn, 14_851));
    try db.abortTransaction(missing_second_write_txn, 14_852);

    const partial_exact_externalized_txn = try db.beginTransaction(14_875);
    try std.testing.expectError(error.ForeignKeyViolation, db.writeTransaction(partial_exact_externalized_txn, .{
        .writes = &.{
            .{ .key = "aa-order:partial-externalized-a", .value = "{\"id\":\"aa-order:partial-externalized-a\",\"customer_id\":\"zz-customer:partial-externalized-a\"}" },
            .{ .key = "aa-order:partial-externalized-b", .value = "{\"id\":\"aa-order:partial-externalized-b\",\"customer_id\":\"zz-customer:partial-externalized-b\"}" },
        },
        .foreign_key_externalized_parent_checks = &.{.{
            .constraint_name = "orders_customer_id_fkey",
            .child_table = "row",
            .child_key = "aa-order:partial-externalized-a",
            .parent_table = "row",
            .parent_key = "zz-customer:partial-externalized-a",
            .timing = .deferred,
        }},
    }));
    try db.abortTransaction(partial_exact_externalized_txn, 14_876);

    const exact_externalized_txn = try db.beginTransaction(14_900);
    try db.writeTransaction(exact_externalized_txn, .{
        .writes = &.{.{ .key = "aa-order:exact-externalized", .value = "{\"id\":\"aa-order:exact-externalized\",\"customer_id\":\"zz-customer:exact-externalized\"}" }},
        .foreign_key_externalized_parent_checks = &.{.{
            .constraint_name = "orders_customer_id_fkey",
            .child_table = "row",
            .child_key = "aa-order:exact-externalized",
            .parent_table = "row",
            .parent_key = "zz-customer:exact-externalized",
            .timing = .deferred,
        }},
    });
    try db.commitTransaction(exact_externalized_txn, 14_901);

    const exact_child = (try db.get(alloc, "aa-order:exact-externalized")) orelse return error.TestExpectedEqual;
    defer alloc.free(exact_child);
    try std.testing.expectEqualStrings("{\"id\":\"aa-order:exact-externalized\",\"customer_id\":\"zz-customer:exact-externalized\"}", exact_child);

    const forced_immediate_without_proof_txn = try db.beginTransaction(14_925);
    try db.writeTransaction(forced_immediate_without_proof_txn, .{
        .writes = &.{.{ .key = "aa-order:forced-immediate-without-proof", .value = "{\"id\":\"aa-order:forced-immediate-without-proof\",\"customer_id\":\"zz-customer:forced-immediate-without-proof\"}" }},
        .foreign_key_constraint_timing_overrides = &.{.{
            .constraint_name = "orders_customer_id_fkey",
            .timing = .immediate,
        }},
    });
    try std.testing.expectError(error.ForeignKeyViolation, db.commitTransaction(forced_immediate_without_proof_txn, 14_926));
    try db.abortTransaction(forced_immediate_without_proof_txn, 14_927);

    const forced_immediate_exact_txn = try db.beginTransaction(14_950);
    try db.writeTransaction(forced_immediate_exact_txn, .{
        .writes = &.{.{ .key = "aa-order:forced-immediate-exact", .value = "{\"id\":\"aa-order:forced-immediate-exact\",\"customer_id\":\"zz-customer:forced-immediate-exact\"}" }},
        .foreign_key_externalized_parent_checks = &.{.{
            .constraint_name = "orders_customer_id_fkey",
            .child_table = "row",
            .child_key = "aa-order:forced-immediate-exact",
            .parent_table = "row",
            .parent_key = "zz-customer:forced-immediate-exact",
            .timing = .immediate,
        }},
        .foreign_key_constraint_timing_overrides = &.{.{
            .constraint_name = "orders_customer_id_fkey",
            .timing = .immediate,
        }},
    });
    try db.commitTransaction(forced_immediate_exact_txn, 14_951);

    const forced_immediate_exact_child = (try db.get(alloc, "aa-order:forced-immediate-exact")) orelse return error.TestExpectedEqual;
    defer alloc.free(forced_immediate_exact_child);
    try std.testing.expectEqualStrings("{\"id\":\"aa-order:forced-immediate-exact\",\"customer_id\":\"zz-customer:forced-immediate-exact\"}", forced_immediate_exact_child);
}

test "db relational integrity transaction mixed immediate and deferred foreign keys require per-constraint proofs" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"deferred_customer_id":{"type":"keyword"},"immediate_customer_id":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"foreign_keys":[{"name":"orders_deferred_customer_id_fkey","columns":["deferred_customer_id"],"references":{"table":"row","columns":["_id"]},"on_delete":"restrict","timing":"deferred"},{"name":"orders_immediate_customer_id_fkey","columns":["immediate_customer_id"],"references":{"table":"row","columns":["_id"]},"on_delete":"restrict","timing":"immediate"}]}
    ;
    var parsed_schema = try table_schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try table_schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    const partial_proof_txn = try db.beginTransaction(14_970);
    try db.writeTransaction(partial_proof_txn, .{
        .writes = &.{.{ .key = "aa-order:mixed-partial-proof", .value = "{\"id\":\"aa-order:mixed-partial-proof\",\"deferred_customer_id\":\"zz-customer:deferred-proof\",\"immediate_customer_id\":\"zz-customer:immediate-missing\"}" }},
        .foreign_key_externalized_parent_checks = &.{.{
            .constraint_name = "orders_deferred_customer_id_fkey",
            .child_table = "row",
            .child_key = "aa-order:mixed-partial-proof",
            .parent_table = "row",
            .parent_key = "zz-customer:deferred-proof",
            .timing = .deferred,
        }},
    });
    try std.testing.expectError(error.ForeignKeyViolation, db.commitTransaction(partial_proof_txn, 14_971));
    try db.abortTransaction(partial_proof_txn, 14_971);

    const mixed_exact_txn = try db.beginTransaction(14_980);
    try db.writeTransaction(mixed_exact_txn, .{
        .writes = &.{
            .{ .key = "aa-order:mixed-exact", .value = "{\"id\":\"aa-order:mixed-exact\",\"deferred_customer_id\":\"zz-customer:deferred-proof\",\"immediate_customer_id\":\"zz-customer:immediate-present\"}" },
            .{ .key = "zz-customer:immediate-present", .value = "{\"id\":\"zz-customer:immediate-present\"}" },
        },
        .foreign_key_externalized_parent_checks = &.{.{
            .constraint_name = "orders_deferred_customer_id_fkey",
            .child_table = "row",
            .child_key = "aa-order:mixed-exact",
            .parent_table = "row",
            .parent_key = "zz-customer:deferred-proof",
            .timing = .deferred,
        }},
    });
    try db.commitTransaction(mixed_exact_txn, 14_981);

    const child = (try db.get(alloc, "aa-order:mixed-exact")) orelse return error.TestExpectedEqual;
    defer alloc.free(child);
    try std.testing.expectEqualStrings("{\"id\":\"aa-order:mixed-exact\",\"deferred_customer_id\":\"zz-customer:deferred-proof\",\"immediate_customer_id\":\"zz-customer:immediate-present\"}", child);
}

test "db relational integrity transaction foreign key timing override survives intent resolution" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"},"manager_email":{"type":"keyword"}},"additionalProperties":false}}},"unique_constraints":[{"name":"row_email_key","columns":["email"]}],"foreign_keys":[{"name":"row_manager_email_fkey","columns":["manager_email"],"references":{"table":"row","columns":["email"]},"on_update":"no_action","timing":"deferred"}]}
    ;
    var parsed_schema = try table_schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try table_schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    try db.batch(.{
        .writes = &.{
            .{ .key = "aa-parent:deferred", .value = "{\"id\":\"aa-parent:deferred\",\"email\":\"old-a\"}" },
            .{ .key = "zz-child:deferred", .value = "{\"id\":\"zz-child:deferred\",\"manager_email\":\"old-a\"}" },
            .{ .key = "aa-parent:forced", .value = "{\"id\":\"aa-parent:forced\",\"email\":\"old-b\"}" },
            .{ .key = "zz-child:forced", .value = "{\"id\":\"zz-child:forced\",\"manager_email\":\"old-b\"}" },
        },
        .timestamp_ns = 15_000,
    });

    const deferred_txn = try db.beginTransaction(15_100);
    try db.writeTransaction(deferred_txn, .{
        .writes = &.{
            .{ .key = "aa-parent:deferred", .value = "{\"id\":\"aa-parent:deferred\",\"email\":\"new-a\"}" },
            .{ .key = "zz-child:deferred", .value = "{\"id\":\"zz-child:deferred\",\"manager_email\":\"new-a\"}" },
        },
    });
    try db.resolveTransactionIntents(deferred_txn, .committed, 15_101);

    const deferred_child = (try db.get(alloc, "zz-child:deferred")) orelse return error.TestExpectedEqual;
    defer alloc.free(deferred_child);
    try std.testing.expectEqualStrings("{\"id\":\"zz-child:deferred\",\"manager_email\":\"new-a\"}", deferred_child);

    const forced_txn = try db.beginTransaction(15_200);
    try db.writeTransaction(forced_txn, .{
        .writes = &.{
            .{ .key = "aa-parent:forced", .value = "{\"id\":\"aa-parent:forced\",\"email\":\"new-b\"}" },
            .{ .key = "zz-child:forced", .value = "{\"id\":\"zz-child:forced\",\"manager_email\":\"new-b\"}" },
        },
        .foreign_key_constraint_timing_overrides = &.{.{
            .constraint_name = "row_manager_email_fkey",
            .timing = .immediate,
        }},
    });
    try std.testing.expectError(error.ForeignKeyViolation, db.resolveTransactionIntents(forced_txn, .committed, 15_201));
    try db.abortTransaction(forced_txn, 15_202);

    const forced_parent = (try db.get(alloc, "aa-parent:forced")) orelse return error.TestExpectedEqual;
    defer alloc.free(forced_parent);
    try std.testing.expectEqualStrings("{\"id\":\"aa-parent:forced\",\"email\":\"old-b\"}", forced_parent);
}

test "db relational integrity transaction deferrable initially immediate foreign key can be deferred by transaction override" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"},"manager_email":{"type":"keyword"}},"additionalProperties":false}}},"unique_constraints":[{"name":"row_email_key","columns":["email"]}],"foreign_keys":[{"name":"row_manager_email_fkey","columns":["manager_email"],"references":{"table":"row","columns":["email"]},"on_update":"no_action","timing":"immediate","deferrable":true}]}
    ;
    var parsed_schema = try table_schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try table_schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    try db.batch(.{
        .writes = &.{
            .{ .key = "aa-parent:initially-immediate", .value = "{\"id\":\"aa-parent:initially-immediate\",\"email\":\"old-a\"}" },
            .{ .key = "zz-child:initially-immediate", .value = "{\"id\":\"zz-child:initially-immediate\",\"manager_email\":\"old-a\"}" },
        },
        .timestamp_ns = 15_300,
    });

    const deferred_txn = try db.beginTransaction(15_310);
    try db.writeTransaction(deferred_txn, .{
        .writes = &.{
            .{ .key = "aa-parent:initially-immediate", .value = "{\"id\":\"aa-parent:initially-immediate\",\"email\":\"new-a\"}" },
            .{ .key = "zz-child:initially-immediate", .value = "{\"id\":\"zz-child:initially-immediate\",\"manager_email\":\"new-a\"}" },
        },
        .foreign_key_constraint_timing_overrides = &.{.{
            .constraint_name = "row_manager_email_fkey",
            .timing = .deferred,
        }},
    });
    try db.resolveTransactionIntents(deferred_txn, .committed, 15_311);

    const child = (try db.get(alloc, "zz-child:initially-immediate")) orelse return error.TestExpectedEqual;
    defer alloc.free(child);
    try std.testing.expectEqualStrings("{\"id\":\"zz-child:initially-immediate\",\"manager_email\":\"new-a\"}", child);

    const non_deferrable_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"},"manager_email":{"type":"keyword"}},"additionalProperties":false}}},"unique_constraints":[{"name":"row_email_key","columns":["email"]}],"foreign_keys":[{"name":"row_manager_email_fkey","columns":["manager_email"],"references":{"table":"row","columns":["email"]},"on_update":"no_action","timing":"immediate"}]}
    ;
    var path_buf_2: [256]u8 = undefined;
    const path_2 = TestHelpers.tempPath(&path_buf_2);
    defer TestHelpers.cleanupTempDir(path_2);
    var non_deferrable_db = try DB.open(alloc, std.mem.span(path_2), .{});
    defer non_deferrable_db.close();
    var non_deferrable_parsed = try table_schema_api.parseValidatedTableSchema(alloc, non_deferrable_schema_json);
    defer non_deferrable_parsed.deinit(alloc);
    const non_deferrable_runtime = try table_schema_api.deriveRuntimeTableSchema(alloc, non_deferrable_parsed);
    defer schema_mod.freeSchema(alloc, non_deferrable_runtime);
    try non_deferrable_db.setSchema(non_deferrable_runtime);
    const rejected_txn = try non_deferrable_db.beginTransaction(15_320);
    try std.testing.expectError(error.ForeignKeyViolation, non_deferrable_db.writeTransaction(rejected_txn, .{
        .foreign_key_constraint_timing_overrides = &.{.{
            .constraint_name = "row_manager_email_fkey",
            .timing = .deferred,
        }},
    }));
    try non_deferrable_db.abortTransaction(rejected_txn, 15_321);
}

test "db relational integrity transaction deferred foreign keys preserve exact externalized unique parent proofs through commit" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"},"handle":{"type":"keyword"},"ref_email":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"unique_constraints":[{"name":"users_email_key","columns":["email"]},{"name":"users_handle_key","columns":["handle"]}],"foreign_keys":[{"name":"users_ref_email_fkey","columns":["ref_email"],"references":{"table":"row","columns":["email"]},"on_delete":"restrict","timing":"deferred"}]}
    ;
    var parsed_schema = try table_schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try table_schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    const child_row = try mapper.buildRelationalRowValueAlloc(alloc, "{\"id\":\"user:child\",\"ref_email\":\"proof@example.test\"}", runtime_schema.relational_columns);
    defer alloc.free(child_row);
    const encoded_parent = (try relational_store_mod.foreignKeyReferenceValueAlloc(alloc, child_row, runtime_schema.foreign_keys[0])) orelse return error.TestExpectedEqual;
    defer alloc.free(encoded_parent);

    const missing_constraint_txn = try db.beginTransaction(14_920);
    try std.testing.expectError(error.ForeignKeyViolation, db.writeTransaction(missing_constraint_txn, .{
        .writes = &.{.{ .key = "user:missing-constraint", .value = "{\"id\":\"user:missing-constraint\",\"ref_email\":\"proof@example.test\"}" }},
        .foreign_key_externalized_parent_checks = &.{.{
            .constraint_name = "users_ref_email_fkey",
            .child_table = "row",
            .child_key = "user:missing-constraint",
            .parent_table = "row",
            .parent_key = encoded_parent,
            .timing = .deferred,
        }},
    }));
    try db.abortTransaction(missing_constraint_txn, 14_921);

    const wrong_constraint_txn = try db.beginTransaction(14_930);
    try std.testing.expectError(error.ForeignKeyViolation, db.writeTransaction(wrong_constraint_txn, .{
        .writes = &.{.{ .key = "user:wrong-constraint", .value = "{\"id\":\"user:wrong-constraint\",\"ref_email\":\"proof@example.test\"}" }},
        .foreign_key_externalized_parent_checks = &.{.{
            .constraint_name = "users_ref_email_fkey",
            .child_table = "row",
            .child_key = "user:wrong-constraint",
            .parent_table = "row",
            .parent_key = encoded_parent,
            .parent_constraint_name = "users_handle_key",
            .timing = .deferred,
        }},
    }));
    try db.abortTransaction(wrong_constraint_txn, 14_931);

    const exact_txn = try db.beginTransaction(14_940);
    try db.writeTransaction(exact_txn, .{
        .writes = &.{.{ .key = "user:exact-unique", .value = "{\"id\":\"user:exact-unique\",\"ref_email\":\"proof@example.test\"}" }},
        .foreign_key_externalized_parent_checks = &.{.{
            .constraint_name = "users_ref_email_fkey",
            .child_table = "row",
            .child_key = "user:exact-unique",
            .parent_table = "row",
            .parent_key = encoded_parent,
            .parent_constraint_name = "users_email_key",
            .timing = .deferred,
        }},
    });
    try db.commitTransaction(exact_txn, 14_941);

    const child = (try db.get(alloc, "user:exact-unique")) orelse return error.TestExpectedEqual;
    defer alloc.free(child);
    try std.testing.expectEqualStrings("{\"id\":\"user:exact-unique\",\"ref_email\":\"proof@example.test\"}", child);
}

test "db relational integrity transaction deferred foreign keys require named cross-table unique parent proofs" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"customer_email":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"foreign_keys":[{"name":"orders_customer_email_fkey","columns":["customer_email"],"references":{"table":"customers","columns":["email"]},"on_delete":"restrict","timing":"deferred"}]}
    ;
    var parsed_schema = try table_schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try table_schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    const child_row = try mapper.buildRelationalRowValueAlloc(alloc, "{\"id\":\"order:child\",\"customer_email\":\"ada@example.test\"}", runtime_schema.relational_columns);
    defer alloc.free(child_row);
    const encoded_parent = (try relational_store_mod.foreignKeyReferenceValueAlloc(alloc, child_row, runtime_schema.foreign_keys[0])) orelse return error.TestExpectedEqual;
    defer alloc.free(encoded_parent);

    const empty_constraint_txn = try db.beginTransaction(14_950);
    try std.testing.expectError(error.ForeignKeyViolation, db.writeTransaction(empty_constraint_txn, .{
        .writes = &.{.{ .key = "order:empty-proof", .value = "{\"id\":\"order:empty-proof\",\"customer_email\":\"ada@example.test\"}" }},
        .foreign_key_externalized_parent_checks = &.{.{
            .constraint_name = "orders_customer_email_fkey",
            .child_table = "row",
            .child_key = "order:empty-proof",
            .parent_table = "customers",
            .parent_key = encoded_parent,
            .parent_constraint_name = "",
            .timing = .deferred,
        }},
    }));
    try db.abortTransaction(empty_constraint_txn, 14_951);

    const named_constraint_txn = try db.beginTransaction(14_960);
    try db.writeTransaction(named_constraint_txn, .{
        .writes = &.{.{ .key = "order:named-proof", .value = "{\"id\":\"order:named-proof\",\"customer_email\":\"ada@example.test\"}" }},
        .foreign_key_externalized_parent_checks = &.{.{
            .constraint_name = "orders_customer_email_fkey",
            .child_table = "row",
            .child_key = "order:named-proof",
            .parent_table = "customers",
            .parent_key = encoded_parent,
            .parent_constraint_name = "customers_email_key",
            .timing = .deferred,
        }},
    });
    try db.commitTransaction(named_constraint_txn, 14_961);

    const child = (try db.get(alloc, "order:named-proof")) orelse return error.TestExpectedEqual;
    defer alloc.free(child);
    try std.testing.expectEqualStrings("{\"id\":\"order:named-proof\",\"customer_email\":\"ada@example.test\"}", child);
}

test "db relational integrity transaction foreign key parent checks validate final prepared state" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const schemaless_txn = try db.beginTransaction(15_000);
    try std.testing.expectError(error.ForeignKeyViolation, db.writeTransaction(schemaless_txn, .{
        .foreign_key_parent_checks = &.{.{
            .constraint_name = "orders_customer_id_fkey",
            .child_table = "orders",
            .child_key = "order:schemaless",
            .parent_table = "customers",
            .parent_key = "customer:schemaless",
        }},
    }));
    try db.abortTransaction(schemaless_txn, 15_001);

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"customers","enforce_types":true,"document_schemas":{"customers":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"name":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}}}
    ;
    var parsed_schema = try table_schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try table_schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    const internal_parent_key = try internal_keys.documentKeyAlloc(alloc, "customer:internal");
    defer alloc.free(internal_parent_key);
    const internal_parent_txn = try db.beginTransaction(15_250);
    try std.testing.expectError(error.ForeignKeyViolation, db.writeTransaction(internal_parent_txn, .{
        .foreign_key_parent_checks = &.{.{
            .constraint_name = "orders_customer_id_fkey",
            .child_table = "orders",
            .child_key = "order:internal-parent",
            .parent_table = "customers",
            .parent_key = internal_parent_key,
        }},
    }));
    try db.abortTransaction(internal_parent_txn, 15_251);

    const internal_child_key = try internal_keys.documentKeyAlloc(alloc, "order:internal");
    defer alloc.free(internal_child_key);
    const internal_child_txn = try db.beginTransaction(15_500);
    try std.testing.expectError(error.ForeignKeyViolation, db.writeTransaction(internal_child_txn, .{
        .foreign_key_parent_checks = &.{.{
            .constraint_name = "orders_customer_id_fkey",
            .child_table = "orders",
            .child_key = internal_child_key,
            .parent_table = "customers",
            .parent_key = "customer:internal-child",
        }},
    }));
    try db.abortTransaction(internal_child_txn, 15_501);

    const metadata_parent_txn = try db.beginTransaction(15_750);
    try std.testing.expectError(error.ForeignKeyViolation, db.writeTransaction(metadata_parent_txn, .{
        .foreign_key_parent_checks = &.{.{
            .constraint_name = "orders_customer_id_fkey",
            .child_table = "orders",
            .child_key = "order:metadata-parent",
            .parent_table = "customers",
            .parent_key = "\x00\x00__metadata__:fk-parent",
        }},
    }));
    try db.abortTransaction(metadata_parent_txn, 15_751);

    const missing_txn = try db.beginTransaction(16_000);
    try std.testing.expectError(error.ForeignKeyViolation, db.writeTransaction(missing_txn, .{
        .foreign_key_parent_checks = &.{.{
            .constraint_name = "orders_customer_id_fkey",
            .child_table = "orders",
            .child_key = "order:missing",
            .parent_table = "customers",
            .parent_key = "customer:missing",
        }},
    }));
    try db.abortTransaction(missing_txn, 16_001);

    const wrong_table_txn = try db.beginTransaction(16_500);
    try std.testing.expectError(error.ForeignKeyViolation, db.writeTransaction(wrong_table_txn, .{
        .writes = &.{.{ .key = "customer:wrong-table", .value = "{\"id\":\"customer:wrong-table\",\"name\":\"Ada\"}" }},
        .foreign_key_parent_checks = &.{.{
            .constraint_name = "orders_customer_id_fkey",
            .child_table = "orders",
            .child_key = "order:wrong-table",
            .parent_table = "accounts",
            .parent_key = "customer:wrong-table",
        }},
    }));
    try db.abortTransaction(wrong_table_txn, 16_501);

    const create_txn = try db.beginTransaction(17_000);
    try db.writeTransaction(create_txn, .{
        .writes = &.{.{ .key = "customer:1", .value = "{\"id\":\"customer:1\",\"name\":\"Ada\"}" }},
        .foreign_key_parent_checks = &.{.{
            .constraint_name = "orders_customer_id_fkey",
            .child_table = "orders",
            .child_key = "order:1",
            .parent_table = "customers",
            .parent_key = "customer:1",
        }},
    });
    try db.commitTransaction(create_txn, 17_001);

    const parent = (try db.get(alloc, "customer:1")) orelse return error.TestExpectedEqual;
    defer alloc.free(parent);
    try std.testing.expectEqualStrings("{\"id\":\"customer:1\",\"name\":\"Ada\"}", parent);

    const delete_txn = try db.beginTransaction(18_000);
    try std.testing.expectError(error.ForeignKeyViolation, db.writeTransaction(delete_txn, .{
        .deletes = &.{"customer:1"},
        .foreign_key_parent_checks = &.{.{
            .constraint_name = "orders_customer_id_fkey",
            .child_table = "orders",
            .child_key = "order:2",
            .parent_table = "customers",
            .parent_key = "customer:1",
        }},
    }));
    try db.abortTransaction(delete_txn, 18_001);
}

test "db relational integrity transaction foreign key parent checks validate unique tuple state" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"customers","enforce_types":true,"document_schemas":{"customers":{"schema":{"type":"object","properties":{"email":{"type":"keyword"},"name":{"type":"keyword"}},"additionalProperties":false}}},"unique_constraints":[{"name":"customers_email_key","columns":["email"]}]}
    ;
    var parsed_schema = try table_schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try table_schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    const parent_row = try mapper.buildRelationalRowValueAlloc(alloc, "{\"email\":\"ada@example.test\",\"name\":\"Ada\"}", runtime_schema.relational_columns);
    defer alloc.free(parent_row);
    const encoded_parent = try relational_store_mod.uniqueConstraintTupleValueAlloc(alloc, parent_row, runtime_schema.unique_constraints[0]);
    defer if (encoded_parent) |value| alloc.free(value);
    const parent_key = encoded_parent orelse return error.TestExpectedEqual;

    const missing_txn = try db.beginTransaction(18_100);
    try std.testing.expectError(error.ForeignKeyViolation, db.writeTransaction(missing_txn, .{
        .foreign_key_parent_checks = &.{.{
            .constraint_name = "orders_customer_email_fkey",
            .child_table = "orders",
            .child_key = "order:missing-unique",
            .parent_table = "customers",
            .parent_key = parent_key,
            .parent_constraint_name = "customers_email_key",
        }},
    }));
    try db.abortTransaction(missing_txn, 18_101);

    const wrong_table_txn = try db.beginTransaction(18_150);
    try std.testing.expectError(error.ForeignKeyViolation, db.writeTransaction(wrong_table_txn, .{
        .foreign_key_parent_checks = &.{.{
            .constraint_name = "orders_customer_email_fkey",
            .child_table = "orders",
            .child_key = "order:wrong-parent-table",
            .parent_table = "accounts",
            .parent_key = parent_key,
            .parent_constraint_name = "customers_email_key",
        }},
        .unique_constraint_writes = &.{.{
            .constraint_name = "customers_email_key",
            .encoded_value = parent_key,
            .owner_key = "customer:ada",
        }},
    }));
    try db.abortTransaction(wrong_table_txn, 18_151);

    const create_txn = try db.beginTransaction(18_200);
    try db.writeTransaction(create_txn, .{
        .writes = &.{.{
            .key = "customer:ada",
            .value = "{\"email\":\"ada@example.test\",\"name\":\"Ada\"}",
        }},
        .foreign_key_parent_checks = &.{.{
            .constraint_name = "orders_customer_email_fkey",
            .child_table = "orders",
            .child_key = "order:staged-unique",
            .parent_table = "customers",
            .parent_key = parent_key,
            .parent_constraint_name = "customers_email_key",
        }},
    });
    try db.commitTransaction(create_txn, 18_201);

    const delete_txn = try db.beginTransaction(18_300);
    try std.testing.expectError(error.ForeignKeyViolation, db.writeTransaction(delete_txn, .{
        .foreign_key_parent_checks = &.{.{
            .constraint_name = "orders_customer_email_fkey",
            .child_table = "orders",
            .child_key = "order:deleted-unique",
            .parent_table = "customers",
            .parent_key = parent_key,
            .parent_constraint_name = "customers_email_key",
        }},
        .deletes = &.{"customer:ada"},
    }));
    try db.abortTransaction(delete_txn, 18_301);
}

test "db relational integrity transaction foreign key parent delete checks support unique tuple keys" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"email":{"type":"keyword"},"ref_email":{"type":"keyword"},"name":{"type":"keyword"}},"additionalProperties":false}}},"foreign_keys":[{"name":"users_ref_email_fkey","columns":["ref_email"],"references":{"table":"row","columns":["email"]},"on_delete":"restrict"}],"unique_constraints":[{"name":"users_email_key","columns":["email"]}]}
    ;
    var parsed_schema = try table_schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try table_schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    try db.batch(.{
        .writes = &.{
            .{ .key = "user:parent", .value = "{\"email\":\"ada@example.test\",\"name\":\"Ada\"}" },
            .{ .key = "user:child", .value = "{\"ref_email\":\"ada@example.test\",\"name\":\"Child\"}" },
        },
    });

    const parent_row = try mapper.buildRelationalRowValueAlloc(alloc, "{\"email\":\"ada@example.test\",\"name\":\"Ada\"}", runtime_schema.relational_columns);
    defer alloc.free(parent_row);
    const parent_tuple = (try relational_store_mod.uniqueConstraintTupleValueAlloc(alloc, parent_row, runtime_schema.unique_constraints[0])) orelse return error.TestExpectedEqual;
    defer alloc.free(parent_tuple);

    const blocked_txn = try db.beginTransaction(18_400);
    try std.testing.expectError(error.ForeignKeyViolation, db.writeTransaction(blocked_txn, .{
        .foreign_key_parent_delete_checks = &.{.{
            .constraint_name = "users_ref_email_fkey",
            .parent_table = "row",
            .parent_key = parent_tuple,
        }},
    }));
    try db.abortTransaction(blocked_txn, 18_401);

    const allowed_txn = try db.beginTransaction(18_450);
    try db.writeTransaction(allowed_txn, .{
        .foreign_key_parent_delete_checks = &.{.{
            .constraint_name = "users_ref_email_fkey",
            .parent_table = "row",
            .parent_key = parent_tuple,
        }},
        .foreign_key_ref_deletes = &.{.{
            .constraint_name = "users_ref_email_fkey",
            .parent_table = "row",
            .parent_key = parent_tuple,
            .child_table = "row",
            .child_key = "user:child",
        }},
    });
    try db.commitTransaction(allowed_txn, 18_451);
}

test "db relational integrity parent delete checks use ordered child tuple index" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"tenant_id":{"type":"keyword"},"email":{"type":"keyword"},"order_tenant_id":{"type":"keyword"},"order_email":{"type":"keyword"},"name":{"type":"keyword"}},"additionalProperties":false}}},"unique_constraints":[{"name":"users_tenant_email_key","columns":["tenant_id","email"]}],"foreign_keys":[{"name":"orders_user_email_fkey","columns":["order_tenant_id","order_email"],"references":{"table":"row","columns":["tenant_id","email"]},"on_delete":"restrict"}],"relational_indexes":[{"name":"orders_user_lookup_idx","owner_kind":"relational_column","owner_name":"order_tenant_id","access_method":"ordered_tuple","columns":["order_tenant_id","order_email"],"keys":[{"column":"order_tenant_id"},{"column":"order_email"}],"lifecycle":"ready","generation":7,"schema_fingerprint":"secondary-index-v1:orders_user_lookup_idx","generation_record":{"generation":7,"owner_ranges":[],"lifecycle":"ready","lag":0,"ready_watermark":0}}]}
    ;
    var parsed_schema = try table_schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try table_schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    try db.batch(.{
        .writes = &.{
            .{ .key = "user:ada", .value = "{\"tenant_id\":\"tenant:1\",\"email\":\"ada@example.test\",\"name\":\"Ada\"}" },
            .{ .key = "order:ada", .value = "{\"order_tenant_id\":\"tenant:1\",\"order_email\":\"ada@example.test\",\"name\":\"Child\"}" },
        },
    });

    const parent_row = try mapper.buildRelationalRowValueAlloc(alloc, "{\"tenant_id\":\"tenant:1\",\"email\":\"ada@example.test\",\"name\":\"Ada\"}", runtime_schema.relational_columns);
    defer alloc.free(parent_row);
    const parent_tuple = (try relational_store_mod.uniqueConstraintTupleValueAlloc(alloc, parent_row, runtime_schema.unique_constraints[0])) orelse return error.TestExpectedEqual;
    defer alloc.free(parent_tuple);
    const ordered_child_tuple = (try relational_store_mod.orderedTupleChildValueForForeignKeyParentRowAlloc(
        alloc,
        parent_row,
        runtime_schema.foreign_keys[0],
        runtime_schema.relational_indexes,
        runtime_schema.relational_columns,
        runtime_schema.relational_columns,
    )) orelse return error.TestExpectedEqual;
    defer alloc.free(ordered_child_tuple);

    const ref_prefix = try internal_keys.relationalForeignKeyRefParentPrefixAlloc(alloc, runtime_schema.foreign_keys[0].name, runtime_schema.foreign_keys[0].parent_table, parent_tuple);
    defer alloc.free(ref_prefix);
    const ref_upper = try internal_keys.relationalForeignKeyRefParentPrefixUpperAlloc(alloc, runtime_schema.foreign_keys[0].name, runtime_schema.foreign_keys[0].parent_table, parent_tuple);
    defer if (ref_upper) |buf| alloc.free(buf);
    const refs = try db.core.store.scanRange(alloc, ref_prefix, if (ref_upper) |buf| buf else "");
    defer docstore_mod.DocStore.freeResults(alloc, refs);
    try std.testing.expectEqual(@as(usize, 1), refs.len);
    var ref_delete_keys = try alloc.alloc([]const u8, refs.len);
    defer alloc.free(ref_delete_keys);
    for (refs, 0..) |entry, index| ref_delete_keys[index] = entry.key;
    try db.core.store.putBatch(&.{}, ref_delete_keys);

    const blocked_txn = try db.beginTransaction(18_460);
    try std.testing.expectError(error.ForeignKeyViolation, db.writeTransaction(blocked_txn, .{
        .foreign_key_parent_delete_checks = &.{.{
            .constraint_name = "orders_user_email_fkey",
            .parent_table = "row",
            .parent_key = parent_tuple,
            .ordered_child_tuple = ordered_child_tuple,
        }},
    }));
    try db.abortTransaction(blocked_txn, 18_461);

    const allowed_txn = try db.beginTransaction(18_462);
    try db.writeTransaction(allowed_txn, .{
        .deletes = &.{"order:ada"},
        .foreign_key_parent_delete_checks = &.{.{
            .constraint_name = "orders_user_email_fkey",
            .parent_table = "row",
            .parent_key = parent_tuple,
            .ordered_child_tuple = ordered_child_tuple,
        }},
    });
    try db.commitTransaction(allowed_txn, 18_463);
}

test "db relational integrity parent delete ordered child tuple fails closed without child index" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"tenant_id":{"type":"keyword"},"email":{"type":"keyword"},"order_tenant_id":{"type":"keyword"},"order_email":{"type":"keyword"},"name":{"type":"keyword"}},"additionalProperties":false}}},"unique_constraints":[{"name":"users_tenant_email_key","columns":["tenant_id","email"]}],"foreign_keys":[{"name":"orders_user_email_fkey","columns":["order_tenant_id","order_email"],"references":{"table":"row","columns":["tenant_id","email"]},"on_delete":"restrict"}]}
    ;
    try expectParentDeleteOrderedChildTupleCheckFails(schema_json);
}

test "db relational integrity parent delete ordered child tuple fails closed on stale child index" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"tenant_id":{"type":"keyword"},"email":{"type":"keyword"},"order_tenant_id":{"type":"keyword"},"order_email":{"type":"keyword"},"name":{"type":"keyword"}},"additionalProperties":false}}},"unique_constraints":[{"name":"users_tenant_email_key","columns":["tenant_id","email"]}],"foreign_keys":[{"name":"orders_user_email_fkey","columns":["order_tenant_id","order_email"],"references":{"table":"row","columns":["tenant_id","email"]},"on_delete":"restrict"}],"relational_indexes":[{"name":"orders_user_lookup_idx","owner_kind":"relational_column","owner_name":"order_tenant_id","access_method":"ordered_tuple","columns":["order_tenant_id","order_email"],"keys":[{"column":"order_tenant_id"},{"column":"order_email"}],"lifecycle":"stale","generation":7,"schema_fingerprint":"secondary-index-v1:orders_user_lookup_idx","generation_record":{"generation":7,"owner_ranges":[],"lifecycle":"stale","lag":0,"ready_watermark":0}}]}
    ;
    try expectParentDeleteOrderedChildTupleCheckFails(schema_json);
}

test "db relational integrity parent delete ordered child tuple fails closed on unproved partial child index" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"tenant_id":{"type":"keyword"},"email":{"type":"keyword"},"order_tenant_id":{"type":"keyword"},"order_email":{"type":"keyword"},"status":{"type":"keyword"},"name":{"type":"keyword"}},"additionalProperties":false}}},"unique_constraints":[{"name":"users_tenant_email_key","columns":["tenant_id","email"]}],"foreign_keys":[{"name":"orders_user_email_fkey","columns":["order_tenant_id","order_email"],"references":{"table":"row","columns":["tenant_id","email"]},"on_delete":"restrict"}],"relational_indexes":[{"name":"orders_user_lookup_idx","owner_kind":"relational_column","owner_name":"order_tenant_id","access_method":"ordered_tuple","columns":["order_tenant_id","order_email"],"keys":[{"column":"order_tenant_id"},{"column":"order_email"}],"where":{"all":[{"field":"status","op":"eq","value":"open"}]},"lifecycle":"ready","generation":7,"schema_fingerprint":"secondary-index-v1:orders_user_lookup_idx","generation_record":{"generation":7,"owner_ranges":[],"lifecycle":"ready","lag":0,"ready_watermark":0}}]}
    ;
    try expectParentDeleteOrderedChildTupleCheckFails(schema_json);
}

fn expectParentDeleteOrderedChildTupleCheckFails(schema_json: []const u8) !void {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    var parsed_schema = try table_schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try table_schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    const parent_tuple = try relational_store_mod.bytesTupleValueAlloc(alloc, &.{ "tenant:1", "ada@example.test" });
    defer alloc.free(parent_tuple);
    const ordered_child_tuple = try relational_store_mod.bytesTupleValueAlloc(alloc, &.{ "tenant:1", "ada@example.test" });
    defer alloc.free(ordered_child_tuple);

    const blocked_txn = try db.beginTransaction(18_464);
    try std.testing.expectError(error.ForeignKeyViolation, db.writeTransaction(blocked_txn, .{
        .foreign_key_parent_delete_checks = &.{.{
            .constraint_name = "orders_user_email_fkey",
            .parent_table = "row",
            .parent_key = parent_tuple,
            .ordered_child_tuple = ordered_child_tuple,
        }},
    }));
    try db.abortTransaction(blocked_txn, 18_465);
}

test "db relational integrity transaction foreign key parent update checks use update action semantics" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"},"ref_email":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"unique_constraints":[{"name":"users_email_key","columns":["email"]}],"foreign_keys":[{"name":"users_ref_email_fkey","columns":["ref_email"],"references":{"table":"row","columns":["email"]},"on_delete":"set_null","on_update":"restrict"}]}
    ;
    var parsed_schema = try table_schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try table_schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    try db.batch(.{
        .writes = &.{
            .{ .key = "user:parent", .value = "{\"id\":\"user:parent\",\"email\":\"ada@example.test\"}" },
            .{ .key = "user:child", .value = "{\"id\":\"user:child\",\"ref_email\":\"ada@example.test\"}" },
        },
    });

    const parent_row = try mapper.buildRelationalRowValueAlloc(alloc, "{\"id\":\"user:parent\",\"email\":\"ada@example.test\"}", runtime_schema.relational_columns);
    defer alloc.free(parent_row);
    const encoded_parent = (try relational_store_mod.uniqueConstraintTupleValueAlloc(alloc, parent_row, runtime_schema.unique_constraints[0])) orelse return error.TestExpectedEqual;
    defer alloc.free(encoded_parent);

    const update_check_txn = try db.beginTransaction(18_475);
    try std.testing.expectError(error.ForeignKeyViolation, db.writeTransaction(update_check_txn, .{
        .foreign_key_parent_delete_checks = &.{.{
            .constraint_name = "users_ref_email_fkey",
            .parent_table = "row",
            .parent_key = encoded_parent,
            .operation = .update,
        }},
    }));
    try db.abortTransaction(update_check_txn, 18_476);

    const child = (try db.get(alloc, "user:child")) orelse return error.TestExpectedEqual;
    defer alloc.free(child);
    try std.testing.expectEqualStrings("{\"id\":\"user:child\",\"ref_email\":\"ada@example.test\"}", child);
}

test "db relational integrity transaction batch relational identity rewrite uses foreign key update semantics" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const restrict_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"customer_id":{"type":"keyword"},"kind":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"foreign_keys":[{"name":"orders_customer_fkey","columns":["customer_id"],"references":{"table":"row","columns":["id"]},"on_delete":"set_null","on_update":"restrict"}]}
    ;
    var restrict_parsed = try table_schema_api.parseValidatedTableSchema(alloc, restrict_schema_json);
    defer restrict_parsed.deinit(alloc);
    const restrict_schema = try table_schema_api.deriveRuntimeTableSchema(alloc, restrict_parsed);
    defer schema_mod.freeSchema(alloc, restrict_schema);
    try db.setSchema(restrict_schema);

    try db.batch(.{
        .writes = &.{
            .{ .key = "customer:old", .value = "{\"id\":\"customer:old\",\"kind\":\"customer\"}" },
            .{ .key = "order:1", .value = "{\"id\":\"order:1\",\"customer_id\":\"customer:old\",\"kind\":\"order\"}" },
        },
    });

    try std.testing.expectError(error.ForeignKeyViolation, db.batch(.{
        .relational_identity_rewrites = &.{.{
            .old_key = "customer:old",
            .new_key = "customer:new",
            .value = "{\"id\":\"customer:new\",\"kind\":\"customer\"}",
        }},
    }));

    const restrict_child = (try db.get(alloc, "order:1")) orelse return error.TestExpectedEqual;
    defer alloc.free(restrict_child);
    try std.testing.expect(std.mem.indexOf(u8, restrict_child, "\"customer_id\":\"customer:old\"") != null);

    const set_null_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"customer_id":{"type":"keyword"},"kind":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"foreign_keys":[{"name":"orders_customer_fkey","columns":["customer_id"],"references":{"table":"row","columns":["id"]},"on_delete":"restrict","on_update":"set_null"}]}
    ;
    var path_buf_set_null: [256]u8 = undefined;
    const path_set_null = TestHelpers.tempPath(&path_buf_set_null);
    defer TestHelpers.cleanupTempDir(path_set_null);
    var set_null_db = try DB.open(alloc, std.mem.span(path_set_null), .{});
    defer set_null_db.close();
    var set_null_parsed = try table_schema_api.parseValidatedTableSchema(alloc, set_null_schema_json);
    defer set_null_parsed.deinit(alloc);
    const set_null_schema = try table_schema_api.deriveRuntimeTableSchema(alloc, set_null_parsed);
    defer schema_mod.freeSchema(alloc, set_null_schema);
    try set_null_db.setSchema(set_null_schema);

    try set_null_db.batch(.{
        .writes = &.{
            .{ .key = "customer:old", .value = "{\"id\":\"customer:old\",\"kind\":\"customer\"}" },
            .{ .key = "order:1", .value = "{\"id\":\"order:1\",\"customer_id\":\"customer:old\",\"kind\":\"order\"}" },
        },
    });
    try set_null_db.batch(.{
        .relational_identity_rewrites = &.{.{
            .old_key = "customer:old",
            .new_key = "customer:new",
            .value = "{\"id\":\"customer:new\",\"kind\":\"customer\"}",
        }},
    });

    const set_null_child = (try set_null_db.get(alloc, "order:1")) orelse return error.TestExpectedEqual;
    defer alloc.free(set_null_child);
    try std.testing.expect(std.mem.indexOf(u8, set_null_child, "\"customer_id\"") == null);
    const set_null_parent = (try set_null_db.get(alloc, "customer:new")) orelse return error.TestExpectedEqual;
    defer alloc.free(set_null_parent);
    try std.testing.expect(std.mem.indexOf(u8, set_null_parent, "\"id\":\"customer:new\"") != null);
}

test "db relational integrity transaction relational identity rewrite uses foreign key update semantics" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"customer_id":{"type":"keyword"},"kind":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]},"foreign_keys":[{"name":"orders_customer_fkey","columns":["customer_id"],"references":{"table":"row","columns":["id"]},"on_delete":"restrict","on_update":"set_null"}]}
    ;
    var parsed = try table_schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed.deinit(alloc);
    const runtime_schema = try table_schema_api.deriveRuntimeTableSchema(alloc, parsed);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    try db.batch(.{
        .writes = &.{
            .{ .key = "customer:old", .value = "{\"id\":\"customer:old\",\"kind\":\"customer\"}" },
            .{ .key = "order:1", .value = "{\"id\":\"order:1\",\"customer_id\":\"customer:old\",\"kind\":\"order\"}" },
        },
    });

    const txn = try db.beginTransaction(18_462);
    try db.writeTransaction(txn, .{
        .relational_identity_rewrites = &.{.{
            .old_key = "customer:old",
            .new_key = "customer:new",
            .value = "{\"id\":\"customer:new\",\"kind\":\"customer\"}",
        }},
    });
    try db.commitTransaction(txn, 18_463);

    const child = (try db.get(alloc, "order:1")) orelse return error.TestExpectedEqual;
    defer alloc.free(child);
    try std.testing.expect(std.mem.indexOf(u8, child, "\"customer_id\"") == null);
    const parent = (try db.get(alloc, "customer:new")) orelse return error.TestExpectedEqual;
    defer alloc.free(parent);
    try std.testing.expect(std.mem.indexOf(u8, parent, "\"id\":\"customer:new\"") != null);
    try std.testing.expect((try db.get(alloc, "customer:old")) == null);
}

test "db relational integrity transaction foreign key parent delete checks support cross-table unique tuple keys" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"customer_email":{"type":"keyword"},"status":{"type":"keyword"}},"additionalProperties":false}}},"foreign_keys":[{"name":"orders_customer_email_fkey","columns":["customer_email"],"references":{"table":"customers","columns":["email"]},"on_delete":"restrict"}]}
    ;
    var parsed_schema = try table_schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try table_schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    const child_row = try mapper.buildRelationalRowValueAlloc(alloc, "{\"customer_email\":\"ada@example.test\",\"status\":\"open\"}", runtime_schema.relational_columns);
    defer alloc.free(child_row);
    const parent_tuple = (try relational_store_mod.foreignKeyReferenceValueAlloc(alloc, child_row, runtime_schema.foreign_keys[0])) orelse return error.TestExpectedEqual;
    defer alloc.free(parent_tuple);

    const ref_txn = try db.beginTransaction(18_470);
    try db.writeTransaction(ref_txn, .{
        .foreign_key_ref_writes = &.{.{
            .constraint_name = "orders_customer_email_fkey",
            .parent_table = "customers",
            .parent_key = parent_tuple,
            .child_table = "row",
            .child_key = "order:child",
        }},
    });
    try db.commitTransaction(ref_txn, 18_471);

    const blocked_txn = try db.beginTransaction(18_480);
    try std.testing.expectError(error.ForeignKeyViolation, db.writeTransaction(blocked_txn, .{
        .foreign_key_parent_delete_checks = &.{.{
            .constraint_name = "orders_customer_email_fkey",
            .parent_table = "customers",
            .parent_key = parent_tuple,
        }},
    }));
    try db.abortTransaction(blocked_txn, 18_481);

    const allowed_txn = try db.beginTransaction(18_490);
    try db.writeTransaction(allowed_txn, .{
        .foreign_key_parent_delete_checks = &.{.{
            .constraint_name = "orders_customer_email_fkey",
            .parent_table = "customers",
            .parent_key = parent_tuple,
        }},
        .foreign_key_ref_deletes = &.{.{
            .constraint_name = "orders_customer_email_fkey",
            .parent_table = "customers",
            .parent_key = parent_tuple,
            .child_table = "row",
            .child_key = "order:child",
        }},
    });
    try db.commitTransaction(allowed_txn, 18_491);
}

test "db relational integrity transaction foreign key parent delete checks honor deferred timing" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"customer_id":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"foreign_keys":[{"name":"orders_customer_id_fkey","columns":["customer_id"],"references":{"table":"customers","columns":["_id"]},"on_delete":"restrict","timing":"deferred"}]}
    ;
    var parsed_schema = try table_schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try table_schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    const ref_txn = try db.beginTransaction(18_495);
    try db.writeTransaction(ref_txn, .{
        .foreign_key_ref_writes = &.{.{
            .constraint_name = "orders_customer_id_fkey",
            .parent_table = "customers",
            .parent_key = "customer:deferred-delete",
            .child_table = "row",
            .child_key = "order:deferred-delete",
        }},
    });
    try db.commitTransaction(ref_txn, 18_496);

    const timing_mismatch_txn = try db.beginTransaction(18_497);
    try std.testing.expectError(error.ForeignKeyViolation, db.writeTransaction(timing_mismatch_txn, .{
        .foreign_key_parent_delete_checks = &.{.{
            .constraint_name = "orders_customer_id_fkey",
            .parent_table = "customers",
            .parent_key = "customer:deferred-delete",
        }},
    }));
    try db.abortTransaction(timing_mismatch_txn, 18_498);

    const blocked_txn = try db.beginTransaction(18_499);
    try std.testing.expectError(error.ForeignKeyViolation, db.writeTransaction(blocked_txn, .{
        .foreign_key_parent_delete_checks = &.{.{
            .constraint_name = "orders_customer_id_fkey",
            .parent_table = "customers",
            .parent_key = "customer:deferred-delete",
            .timing = .deferred,
        }},
    }));
    try db.abortTransaction(blocked_txn, 18_500);

    const allowed_txn = try db.beginTransaction(18_501);
    try db.writeTransaction(allowed_txn, .{
        .foreign_key_parent_delete_checks = &.{.{
            .constraint_name = "orders_customer_id_fkey",
            .parent_table = "customers",
            .parent_key = "customer:deferred-delete",
            .timing = .deferred,
        }},
        .foreign_key_ref_deletes = &.{.{
            .constraint_name = "orders_customer_id_fkey",
            .parent_table = "customers",
            .parent_key = "customer:deferred-delete",
            .child_table = "row",
            .child_key = "order:deferred-delete",
        }},
    });
    try db.commitTransaction(allowed_txn, 18_502);
}

test "db relational integrity transaction foreign key parent delete checks validate child references" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"customer_id":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"foreign_keys":[{"name":"orders_customer_id_fkey","columns":["customer_id"],"references":{"table":"customers","columns":["_id"]},"on_delete":"restrict"}]}
    ;
    var parsed_schema = try table_schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try table_schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    const internal_delete_parent_key = try internal_keys.documentKeyAlloc(alloc, "customer:internal-delete");
    defer alloc.free(internal_delete_parent_key);
    const internal_delete_txn = try db.beginTransaction(18_500);
    try std.testing.expectError(error.ForeignKeyViolation, db.writeTransaction(internal_delete_txn, .{
        .foreign_key_parent_delete_checks = &.{.{
            .constraint_name = "orders_customer_id_fkey",
            .parent_table = "customers",
            .parent_key = internal_delete_parent_key,
        }},
    }));
    try db.abortTransaction(internal_delete_txn, 18_501);

    const metadata_delete_txn = try db.beginTransaction(18_750);
    try std.testing.expectError(error.ForeignKeyViolation, db.writeTransaction(metadata_delete_txn, .{
        .foreign_key_parent_delete_checks = &.{.{
            .constraint_name = "orders_customer_id_fkey",
            .parent_table = "customers",
            .parent_key = "\x00\x00__metadata__:fk-delete-parent",
        }},
    }));
    try db.abortTransaction(metadata_delete_txn, 18_751);

    try db.batch(.{
        .writes = &.{
            .{ .key = "customer:1", .value = "{\"id\":\"customer:1\"}" },
            .{ .key = "customer:2", .value = "{\"id\":\"customer:2\"}" },
            .{ .key = "customer:3", .value = "{\"id\":\"customer:3\"}" },
            .{ .key = "customer:rewrite-target", .value = "{\"id\":\"customer:rewrite-target\"}" },
            .{ .key = "order:1", .value = "{\"id\":\"order:1\",\"customer_id\":\"customer:1\"}" },
        },
    });

    const blocked_existing_txn = try db.beginTransaction(19_000);
    try std.testing.expectError(error.ForeignKeyViolation, db.writeTransaction(blocked_existing_txn, .{
        .foreign_key_parent_delete_checks = &.{.{
            .constraint_name = "orders_customer_id_fkey",
            .parent_table = "customers",
            .parent_key = "customer:1",
        }},
    }));
    try db.abortTransaction(blocked_existing_txn, 19_001);

    const blocked_write_txn = try db.beginTransaction(20_000);
    try std.testing.expectError(error.ForeignKeyViolation, db.writeTransaction(blocked_write_txn, .{
        .writes = &.{.{ .key = "order:2", .value = "{\"id\":\"order:2\",\"customer_id\":\"customer:1\"}" }},
        .foreign_key_parent_delete_checks = &.{.{
            .constraint_name = "orders_customer_id_fkey",
            .parent_table = "customers",
            .parent_key = "customer:1",
        }},
    }));
    try db.abortTransaction(blocked_write_txn, 20_001);

    const misplaced_ref = try internal_keys.relationalForeignKeyRefKeyAlloc(
        alloc,
        "orders_customer_id_fkey",
        "customers",
        "customer:misplaced",
        "other_child_table",
        "other:child",
    );
    defer alloc.free(misplaced_ref);
    try db.core.store.putBatch(&.{.{ .key = misplaced_ref, .value = "" }}, &.{});
    const ignores_misplaced_ref_txn = try db.beginTransaction(20_250);
    try db.writeTransaction(ignores_misplaced_ref_txn, .{
        .foreign_key_parent_delete_checks = &.{.{
            .constraint_name = "orders_customer_id_fkey",
            .parent_table = "customers",
            .parent_key = "customer:misplaced",
        }},
    });
    try db.abortTransaction(ignores_misplaced_ref_txn, 20_251);

    const allowed_child_rewrite_txn = try db.beginTransaction(20_500);
    try db.writeTransaction(allowed_child_rewrite_txn, .{
        .writes = &.{.{ .key = "order:1", .value = "{\"id\":\"order:1\",\"customer_id\":\"customer:rewrite-target\"}" }},
        .deletes = &.{"customer:1"},
        .foreign_key_parent_delete_checks = &.{.{
            .constraint_name = "orders_customer_id_fkey",
            .parent_table = "customers",
            .parent_key = "customer:1",
        }},
    });
    try db.commitTransaction(allowed_child_rewrite_txn, 20_501);

    try std.testing.expect((try db.get(alloc, "customer:1")) == null);
    const rewritten_child = (try db.get(alloc, "order:1")) orelse return error.TestExpectedEqual;
    defer alloc.free(rewritten_child);
    try std.testing.expectEqualStrings("{\"id\":\"order:1\",\"customer_id\":\"customer:rewrite-target\"}", rewritten_child);

    const old_ref = try internal_keys.relationalForeignKeyRefKeyAlloc(alloc, "orders_customer_id_fkey", "customers", "customer:1", "row", "order:1");
    defer alloc.free(old_ref);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, old_ref));
    const rewritten_ref = try internal_keys.relationalForeignKeyRefKeyAlloc(alloc, "orders_customer_id_fkey", "customers", "customer:rewrite-target", "row", "order:1");
    defer alloc.free(rewritten_ref);
    const rewritten_ref_value = try db.core.store.get(alloc, rewritten_ref);
    defer alloc.free(rewritten_ref_value);
    try std.testing.expectEqualStrings("", rewritten_ref_value);

    try db.batch(.{
        .writes = &.{
            .{ .key = "customer:delete-target", .value = "{\"id\":\"customer:delete-target\"}" },
            .{ .key = "order:delete-target", .value = "{\"id\":\"order:delete-target\",\"customer_id\":\"customer:delete-target\"}" },
        },
    });
    const allowed_child_delete_txn = try db.beginTransaction(21_000);
    try db.writeTransaction(allowed_child_delete_txn, .{
        .deletes = &.{"order:delete-target"},
        .foreign_key_parent_delete_checks = &.{.{
            .constraint_name = "orders_customer_id_fkey",
            .parent_table = "customers",
            .parent_key = "customer:delete-target",
        }},
    });
    try db.commitTransaction(allowed_child_delete_txn, 21_001);

    try std.testing.expect((try db.get(alloc, "order:delete-target")) == null);

    const pending_child_txn = try db.beginTransaction(22_000);
    try db.writeTransaction(pending_child_txn, .{
        .writes = &.{.{ .key = "order:pending", .value = "{\"id\":\"order:pending\",\"customer_id\":\"customer:2\"}" }},
    });
    const blocked_parent_delete_txn = try db.beginTransaction(22_100);
    try std.testing.expectError(error.IntentConflict, db.writeTransaction(blocked_parent_delete_txn, .{
        .foreign_key_parent_delete_checks = &.{.{
            .constraint_name = "orders_customer_id_fkey",
            .parent_table = "customers",
            .parent_key = "customer:2",
        }},
    }));
    try db.abortTransaction(blocked_parent_delete_txn, 22_101);
    try db.abortTransaction(pending_child_txn, 22_102);

    const pending_parent_delete_txn = try db.beginTransaction(23_000);
    try db.writeTransaction(pending_parent_delete_txn, .{
        .foreign_key_parent_delete_checks = &.{.{
            .constraint_name = "orders_customer_id_fkey",
            .parent_table = "customers",
            .parent_key = "customer:3",
        }},
    });
    const blocked_child_txn = try db.beginTransaction(23_100);
    try std.testing.expectError(error.IntentConflict, db.writeTransaction(blocked_child_txn, .{
        .writes = &.{.{ .key = "order:blocked", .value = "{\"id\":\"order:blocked\",\"customer_id\":\"customer:3\"}" }},
    }));
    try db.abortTransaction(blocked_child_txn, 23_101);
    try db.abortTransaction(pending_parent_delete_txn, 23_102);
}

test "db relational integrity transaction foreign key parent delete checks plan set null actions" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"customer_id":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"foreign_keys":[{"name":"orders_customer_id_fkey","columns":["customer_id"],"references":{"table":"customers","columns":["_id"]},"on_delete":"set_null"}]}
    ;
    var parsed_schema = try table_schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try table_schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    try db.batch(.{
        .writes = &.{
            .{ .key = "customer:set-null", .value = "{\"id\":\"customer:set-null\"}" },
            .{ .key = "order:set-null", .value = "{\"id\":\"order:set-null\",\"customer_id\":\"customer:set-null\",\"status\":\"open\"}" },
        },
    });

    const set_null_txn = try db.beginTransaction(25_000);
    try db.writeTransaction(set_null_txn, .{
        .foreign_key_parent_delete_checks = &.{.{
            .constraint_name = "orders_customer_id_fkey",
            .parent_table = "customers",
            .parent_key = "customer:set-null",
        }},
    });
    try db.commitTransaction(set_null_txn, 25_001);

    const child = (try db.get(alloc, "order:set-null")) orelse return error.TestExpectedEqual;
    defer alloc.free(child);
    try std.testing.expectEqualStrings("{\"id\":\"order:set-null\",\"status\":\"open\"}", child);

    const old_ref = try internal_keys.relationalForeignKeyRefKeyAlloc(alloc, "orders_customer_id_fkey", "customers", "customer:set-null", "row", "order:set-null");
    defer alloc.free(old_ref);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, old_ref));

    try db.batch(.{
        .writes = &.{
            .{ .key = "customer:direct-set-null", .value = "{\"id\":\"customer:direct-set-null\"}" },
            .{ .key = "order:direct-set-null", .value = "{\"id\":\"order:direct-set-null\",\"customer_id\":\"customer:direct-set-null\",\"status\":\"open\"}" },
        },
    });
    const direct_set_null_txn = try db.beginTransaction(26_000);
    try db.writeTransaction(direct_set_null_txn, .{
        .foreign_key_set_null_children = &.{.{
            .constraint_name = "orders_customer_id_fkey",
            .parent_table = "customers",
            .parent_key = "customer:direct-set-null",
            .child_key = "order:direct-set-null",
        }},
    });
    try db.commitTransaction(direct_set_null_txn, 26_001);

    const direct_child = (try db.get(alloc, "order:direct-set-null")) orelse return error.TestExpectedEqual;
    defer alloc.free(direct_child);
    try std.testing.expectEqualStrings("{\"id\":\"order:direct-set-null\",\"status\":\"open\"}", direct_child);

    const direct_old_ref = try internal_keys.relationalForeignKeyRefKeyAlloc(alloc, "orders_customer_id_fkey", "customers", "customer:direct-set-null", "row", "order:direct-set-null");
    defer alloc.free(direct_old_ref);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, direct_old_ref));
}

test "db relational integrity transaction foreign key cascade child actions delete exact children" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"customer_id":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"foreign_keys":[{"name":"orders_customer_id_fkey","columns":["customer_id"],"references":{"table":"customers","columns":["_id"]},"on_delete":"cascade"}]}
    ;
    var parsed_schema = try table_schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try table_schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    try db.batch(.{
        .writes = &.{
            .{ .key = "customer:cascade-action", .value = "{\"id\":\"customer:cascade-action\"}" },
            .{ .key = "order:cascade-action", .value = "{\"id\":\"order:cascade-action\",\"customer_id\":\"customer:cascade-action\",\"status\":\"open\"}" },
        },
    });

    const cascade_txn = try db.beginTransaction(27_000);
    try db.writeTransaction(cascade_txn, .{
        .foreign_key_cascade_children = &.{.{
            .constraint_name = "orders_customer_id_fkey",
            .parent_table = "customers",
            .parent_key = "customer:cascade-action",
            .child_key = "order:cascade-action",
        }},
    });
    try db.commitTransaction(cascade_txn, 27_001);

    try std.testing.expect((try db.get(alloc, "order:cascade-action")) == null);
    const old_ref = try internal_keys.relationalForeignKeyRefKeyAlloc(alloc, "orders_customer_id_fkey", "customers", "customer:cascade-action", "row", "order:cascade-action");
    defer alloc.free(old_ref);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, old_ref));
}

test "db relational integrity transaction foreign key set-null child actions support unique tuple parent keys" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"},"ref_email":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"foreign_keys":[{"name":"users_ref_email_fkey","columns":["ref_email"],"references":{"table":"row","columns":["email"]},"on_delete":"set_null"}],"unique_constraints":[{"name":"users_email_key","columns":["email"]}]}
    ;
    var parsed_schema = try table_schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try table_schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    try db.batch(.{
        .writes = &.{
            .{ .key = "user:parent", .value = "{\"id\":\"user:parent\",\"email\":\"ada@example.test\"}" },
            .{ .key = "user:child", .value = "{\"id\":\"user:child\",\"ref_email\":\"ada@example.test\",\"status\":\"open\"}" },
        },
    });

    const parent_row = try mapper.buildRelationalRowValueAlloc(alloc, "{\"id\":\"user:parent\",\"email\":\"ada@example.test\"}", runtime_schema.relational_columns);
    defer alloc.free(parent_row);
    const encoded_parent = (try relational_store_mod.uniqueConstraintTupleValueAlloc(alloc, parent_row, runtime_schema.unique_constraints[0])) orelse return error.TestExpectedEqual;
    defer alloc.free(encoded_parent);

    const set_null_txn = try db.beginTransaction(28_000);
    try db.writeTransaction(set_null_txn, .{
        .foreign_key_set_null_children = &.{.{
            .constraint_name = "users_ref_email_fkey",
            .parent_table = "row",
            .parent_key = encoded_parent,
            .child_key = "user:child",
        }},
    });
    try db.commitTransaction(set_null_txn, 28_001);

    const child = (try db.get(alloc, "user:child")) orelse return error.TestExpectedEqual;
    defer alloc.free(child);
    try std.testing.expectEqualStrings("{\"id\":\"user:child\",\"status\":\"open\"}", child);

    const old_ref = try internal_keys.relationalForeignKeyRefKeyAlloc(alloc, "users_ref_email_fkey", "row", encoded_parent, "row", "user:child");
    defer alloc.free(old_ref);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, old_ref));
}

test "db relational integrity transaction foreign key cascade child actions support unique tuple parent keys" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"email":{"type":"keyword"},"ref_email":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"foreign_keys":[{"name":"users_ref_email_fkey","columns":["ref_email"],"references":{"table":"row","columns":["email"]},"on_delete":"cascade"}],"unique_constraints":[{"name":"users_email_key","columns":["email"]}]}
    ;
    var parsed_schema = try table_schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try table_schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    try db.batch(.{
        .writes = &.{
            .{ .key = "user:parent", .value = "{\"id\":\"user:parent\",\"email\":\"ada@example.test\"}" },
            .{ .key = "user:child", .value = "{\"id\":\"user:child\",\"ref_email\":\"ada@example.test\",\"status\":\"open\"}" },
        },
    });

    const parent_row = try mapper.buildRelationalRowValueAlloc(alloc, "{\"id\":\"user:parent\",\"email\":\"ada@example.test\"}", runtime_schema.relational_columns);
    defer alloc.free(parent_row);
    const encoded_parent = (try relational_store_mod.uniqueConstraintTupleValueAlloc(alloc, parent_row, runtime_schema.unique_constraints[0])) orelse return error.TestExpectedEqual;
    defer alloc.free(encoded_parent);

    const cascade_txn = try db.beginTransaction(29_000);
    try db.writeTransaction(cascade_txn, .{
        .foreign_key_cascade_children = &.{.{
            .constraint_name = "users_ref_email_fkey",
            .parent_table = "row",
            .parent_key = encoded_parent,
            .child_key = "user:child",
        }},
    });
    try db.commitTransaction(cascade_txn, 29_001);

    try std.testing.expect((try db.get(alloc, "user:child")) == null);
    const old_ref = try internal_keys.relationalForeignKeyRefKeyAlloc(alloc, "users_ref_email_fkey", "row", encoded_parent, "row", "user:child");
    defer alloc.free(old_ref);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, old_ref));
}

test "db relational integrity transaction foreign key ref mutations support routed owner participants" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"customer_id":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"foreign_keys":[{"name":"orders_customer_id_fkey","columns":["customer_id"],"references":{"table":"customers","columns":["_id"]},"on_delete":"restrict"}]}
    ;
    var parsed_schema = try table_schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try table_schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    const owner_write_txn = try db.beginTransaction(24_000);
    try db.writeTransaction(owner_write_txn, .{
        .foreign_key_ref_writes = &.{.{
            .constraint_name = "orders_customer_id_fkey",
            .parent_table = "customers",
            .parent_key = "customer:routed",
            .child_table = "row",
            .child_key = "order:routed",
        }},
    });
    try db.commitTransaction(owner_write_txn, 24_001);

    const routed_ref = try internal_keys.relationalForeignKeyRefKeyAlloc(alloc, "orders_customer_id_fkey", "customers", "customer:routed", "row", "order:routed");
    defer alloc.free(routed_ref);
    const routed_ref_value = try db.core.store.get(alloc, routed_ref);
    defer alloc.free(routed_ref_value);
    try std.testing.expectEqualStrings("", routed_ref_value);

    const blocked_delete_txn = try db.beginTransaction(24_500);
    try std.testing.expectError(error.ForeignKeyViolation, db.writeTransaction(blocked_delete_txn, .{
        .foreign_key_parent_delete_checks = &.{.{
            .constraint_name = "orders_customer_id_fkey",
            .parent_table = "customers",
            .parent_key = "customer:routed",
        }},
    }));
    try db.abortTransaction(blocked_delete_txn, 24_501);

    const owner_delete_txn = try db.beginTransaction(25_000);
    try db.writeTransaction(owner_delete_txn, .{
        .foreign_key_parent_delete_checks = &.{.{
            .constraint_name = "orders_customer_id_fkey",
            .parent_table = "customers",
            .parent_key = "customer:routed",
        }},
        .foreign_key_ref_deletes = &.{.{
            .constraint_name = "orders_customer_id_fkey",
            .parent_table = "customers",
            .parent_key = "customer:routed",
            .child_table = "row",
            .child_key = "order:routed",
        }},
    });
    try db.commitTransaction(owner_delete_txn, 25_001);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, routed_ref));

    const staged_ref_txn = try db.beginTransaction(26_000);
    try std.testing.expectError(error.ForeignKeyViolation, db.writeTransaction(staged_ref_txn, .{
        .foreign_key_parent_delete_checks = &.{.{
            .constraint_name = "orders_customer_id_fkey",
            .parent_table = "customers",
            .parent_key = "customer:staged",
        }},
        .foreign_key_ref_writes = &.{.{
            .constraint_name = "orders_customer_id_fkey",
            .parent_table = "customers",
            .parent_key = "customer:staged",
            .child_table = "row",
            .child_key = "order:staged",
        }},
    }));
    try db.abortTransaction(staged_ref_txn, 26_001);

    const wrong_child_table_txn = try db.beginTransaction(27_000);
    try std.testing.expectError(error.ForeignKeyViolation, db.writeTransaction(wrong_child_table_txn, .{
        .foreign_key_ref_writes = &.{.{
            .constraint_name = "orders_customer_id_fkey",
            .parent_table = "customers",
            .parent_key = "customer:wrong-child-table",
            .child_table = "other",
            .child_key = "order:wrong-child-table",
        }},
    }));
    try db.abortTransaction(wrong_child_table_txn, 27_001);
}

test "db relational temporal primary keys enforce without-overlaps intervals" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .mem = .{} },
    });
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"sku":{"type":"keyword"},"valid_from":{"type":"numeric"},"valid_to":{"type":"numeric"},"price":{"type":"numeric"}},"required":["sku","valid_from"],"additionalProperties":false}}},"periods":[{"name":"valid_time","start_column":"valid_from","end_column":"valid_to"}],"primary_key":{"columns":["sku"],"without_overlaps_period":"valid_time"}}
    ;
    var parsed_schema = try schema_api_mod.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try schema_api_mod.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    try db.batch(.{
        .writes = &.{
            .{ .key = "price:a:v1", .value = "{\"sku\":\"sku:a\",\"valid_from\":0,\"valid_to\":10,\"price\":10}" },
            .{ .key = "price:a:v2", .value = "{\"sku\":\"sku:a\",\"valid_from\":10,\"valid_to\":20,\"price\":12}" },
            .{ .key = "price:b:v1", .value = "{\"sku\":\"sku:b\",\"valid_from\":5,\"valid_to\":15,\"price\":7}" },
        },
    });

    try std.testing.expectError(error.UniqueConstraintViolation, db.batch(.{
        .writes = &.{.{ .key = "price:a:overlap", .value = "{\"sku\":\"sku:a\",\"valid_from\":5,\"valid_to\":6,\"price\":11}" }},
    }));
    try std.testing.expectError(error.InvalidColumnValue, db.batch(.{
        .writes = &.{.{ .key = "price:a:invalid", .value = "{\"sku\":\"sku:a\",\"valid_from\":30,\"valid_to\":30,\"price\":15}" }},
    }));
    try std.testing.expectError(error.UniqueConstraintViolation, db.batch(.{
        .writes = &.{.{ .key = "price:a:v2", .value = "{\"sku\":\"sku:a\",\"valid_from\":9,\"valid_to\":20,\"price\":13}" }},
    }));

    try db.batch(.{ .deletes = &.{"price:a:v1"} });
    try db.batch(.{
        .writes = &.{.{ .key = "price:a:replacement", .value = "{\"sku\":\"sku:a\",\"valid_from\":5,\"valid_to\":9,\"price\":14}" }},
    });

    try db.batch(.{
        .writes = &.{
            .{ .key = "price:c:v1", .value = "{\"sku\":\"sku:c\",\"valid_from\":0,\"valid_to\":20,\"price\":8}" },
            .{ .key = "price:c:current", .value = "{\"sku\":\"sku:c\",\"valid_from\":20,\"price\":9}" },
        },
    });
    try std.testing.expectError(error.UniqueConstraintViolation, db.batch(.{
        .writes = &.{.{ .key = "price:c:overlap-open", .value = "{\"sku\":\"sku:c\",\"valid_from\":40,\"valid_to\":50,\"price\":10}" }},
    }));
}

test "db relational temporal foreign keys require covered parent periods" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .mem = .{} },
    });
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"sku":{"type":"keyword"},"parent_sku":{"type":"keyword"},"valid_from":{"type":"numeric"},"valid_to":{"type":"numeric"},"price":{"type":"numeric"}},"required":["sku","valid_from"],"additionalProperties":false}}},"periods":[{"name":"valid_time","start_column":"valid_from","end_column":"valid_to"}],"primary_key":{"columns":["sku"],"without_overlaps_period":"valid_time"},"foreign_keys":[{"name":"price_parent_period_fkey","columns":["parent_sku"],"period":"valid_time","references":{"table":"row","columns":["sku"],"period":"valid_time"},"validation_state":"enforced"}]}
    ;
    var parsed_schema = try schema_api_mod.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try schema_api_mod.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    try db.batch(.{
        .writes = &.{
            .{ .key = "price:parent:v1", .value = "{\"sku\":\"parent\",\"valid_from\":0,\"valid_to\":5,\"price\":10}" },
            .{ .key = "price:parent:v2", .value = "{\"sku\":\"parent\",\"valid_from\":5,\"valid_to\":10,\"price\":12}" },
            .{ .key = "price:gap:v1", .value = "{\"sku\":\"gap\",\"valid_from\":0,\"valid_to\":4,\"price\":7}" },
            .{ .key = "price:gap:v2", .value = "{\"sku\":\"gap\",\"valid_from\":6,\"valid_to\":10,\"price\":8}" },
            .{ .key = "price:open:v1", .value = "{\"sku\":\"open\",\"valid_from\":10,\"price\":14}" },
        },
    });

    try db.batch(.{
        .writes = &.{.{ .key = "price:child:covered", .value = "{\"sku\":\"child:covered\",\"parent_sku\":\"parent\",\"valid_from\":2,\"valid_to\":8,\"price\":99}" }},
    });

    try std.testing.expectError(error.ForeignKeyViolation, db.batch(.{
        .writes = &.{.{ .key = "price:child:gap", .value = "{\"sku\":\"child:gap\",\"parent_sku\":\"gap\",\"valid_from\":3,\"valid_to\":7,\"price\":99}" }},
    }));
    try std.testing.expectError(error.ForeignKeyViolation, db.batch(.{
        .writes = &.{.{ .key = "price:child:missing", .value = "{\"sku\":\"child:missing\",\"parent_sku\":\"missing\",\"valid_from\":1,\"valid_to\":2,\"price\":99}" }},
    }));
    try db.batch(.{
        .writes = &.{.{ .key = "price:child:open-covered", .value = "{\"sku\":\"child:open-covered\",\"parent_sku\":\"open\",\"valid_from\":12,\"valid_to\":100,\"price\":99}" }},
    });

    try db.batch(.{
        .writes = &.{.{ .key = "price:child:covered", .value = "{\"sku\":\"child:covered\",\"parent_sku\":\"parent\",\"valid_from\":1,\"valid_to\":9,\"price\":101}" }},
    });
    try std.testing.expectError(error.ForeignKeyViolation, db.batch(.{
        .writes = &.{.{ .key = "price:child:covered", .value = "{\"sku\":\"child:covered\",\"parent_sku\":\"parent\",\"valid_from\":8,\"valid_to\":12,\"price\":102}" }},
    }));

    const covered_parent_row = try mapper.buildRelationalRowValueAlloc(alloc, "{\"sku\":\"child:proof\",\"parent_sku\":\"parent\",\"valid_from\":2,\"valid_to\":8,\"price\":50}", runtime_schema.relational_columns);
    defer alloc.free(covered_parent_row);
    const covered_parent_key = try relational_store_mod.foreignKeyReferenceValueAlloc(alloc, covered_parent_row, runtime_schema.foreign_keys[0]);
    defer alloc.free(covered_parent_key.?);

    const proof_txn = try db.beginTransaction(3_000);
    try db.writeTransaction(proof_txn, .{
        .foreign_key_parent_checks = &.{.{
            .constraint_name = "price_parent_period_fkey",
            .child_table = "row",
            .child_key = "price:child:proof",
            .parent_table = "row",
            .parent_key = covered_parent_key.?,
            .parent_constraint_name = relational_store_mod.primary_key_constraint_name,
            .child_period_start_json = "2",
            .child_period_end_json = "8",
            .timing = .immediate,
        }},
    });
    try db.abortTransaction(proof_txn, 3_001);

    const gap_txn = try db.beginTransaction(3_010);
    defer db.abortTransaction(gap_txn, 3_011) catch {};
    try std.testing.expectError(error.ForeignKeyViolation, db.writeTransaction(gap_txn, .{
        .foreign_key_parent_checks = &.{.{
            .constraint_name = "price_parent_period_fkey",
            .child_table = "row",
            .child_key = "price:child:proof",
            .parent_table = "row",
            .parent_key = covered_parent_key.?,
            .parent_constraint_name = relational_store_mod.primary_key_constraint_name,
            .child_period_start_json = "8",
            .child_period_end_json = "12",
            .timing = .immediate,
        }},
    }));
}

test "db relational temporal foreign key delete actions respect remaining parent coverage" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var set_null_path_buf: [256]u8 = undefined;
    const set_null_path = TestHelpers.tempPath(&set_null_path_buf);
    defer TestHelpers.cleanupTempDir(set_null_path);

    var set_null_db = try DB.open(alloc, std.mem.span(set_null_path), .{
        .primary_backend = .{ .mem = .{} },
    });
    defer set_null_db.close();

    const set_null_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"sku":{"type":"keyword"},"parent_sku":{"type":"keyword"},"valid_from":{"type":"numeric"},"valid_to":{"type":"numeric"},"price":{"type":"numeric"}},"required":["sku","valid_from","valid_to"],"additionalProperties":false}}},"periods":[{"name":"valid_time","start_column":"valid_from","end_column":"valid_to"}],"primary_key":{"columns":["sku"],"without_overlaps_period":"valid_time"},"foreign_keys":[{"name":"price_parent_period_fkey","columns":["parent_sku"],"period":"valid_time","references":{"table":"row","columns":["sku"],"period":"valid_time"},"on_delete":"set_null","validation_state":"enforced"}]}
    ;
    var set_null_parsed_schema = try schema_api_mod.parseValidatedTableSchema(alloc, set_null_schema_json);
    defer set_null_parsed_schema.deinit(alloc);
    const set_null_runtime_schema = try schema_api_mod.deriveRuntimeTableSchema(alloc, set_null_parsed_schema);
    defer schema_mod.freeSchema(alloc, set_null_runtime_schema);
    try set_null_db.setSchema(set_null_runtime_schema);

    try set_null_db.batch(.{
        .writes = &.{
            .{ .key = "price:parent:v1", .value = "{\"sku\":\"parent\",\"valid_from\":0,\"valid_to\":5,\"price\":10}" },
            .{ .key = "price:parent:v2", .value = "{\"sku\":\"parent\",\"valid_from\":5,\"valid_to\":10,\"price\":12}" },
            .{ .key = "price:child:covered", .value = "{\"sku\":\"child:covered\",\"parent_sku\":\"parent\",\"valid_from\":6,\"valid_to\":8,\"price\":99}" },
        },
    });

    try set_null_db.batch(.{ .deletes = &.{"price:parent:v1"} });
    const covered_child = (try set_null_db.get(alloc, "price:child:covered")) orelse return error.TestExpectedEqual;
    defer alloc.free(covered_child);
    try std.testing.expect(std.mem.indexOf(u8, covered_child, "\"parent_sku\":\"parent\"") != null);

    try set_null_db.batch(.{ .deletes = &.{"price:parent:v2"} });
    const uncovered_child = (try set_null_db.get(alloc, "price:child:covered")) orelse return error.TestExpectedEqual;
    defer alloc.free(uncovered_child);
    try std.testing.expect(std.mem.indexOf(u8, uncovered_child, "\"parent_sku\"") == null);

    var set_null_update_path_buf: [256]u8 = undefined;
    const set_null_update_path = TestHelpers.tempPath(&set_null_update_path_buf);
    defer TestHelpers.cleanupTempDir(set_null_update_path);

    var set_null_update_db = try DB.open(alloc, std.mem.span(set_null_update_path), .{
        .primary_backend = .{ .mem = .{} },
    });
    defer set_null_update_db.close();

    const set_null_update_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"sku":{"type":"keyword"},"parent_sku":{"type":"keyword"},"valid_from":{"type":"numeric"},"valid_to":{"type":"numeric"},"price":{"type":"numeric"}},"required":["sku","valid_from","valid_to"],"additionalProperties":false}}},"periods":[{"name":"valid_time","start_column":"valid_from","end_column":"valid_to"}],"primary_key":{"columns":["sku"],"without_overlaps_period":"valid_time"},"foreign_keys":[{"name":"price_parent_period_fkey","columns":["parent_sku"],"period":"valid_time","references":{"table":"row","columns":["sku"],"period":"valid_time"},"on_update":"set_null","validation_state":"enforced"}]}
    ;
    var set_null_update_parsed_schema = try schema_api_mod.parseValidatedTableSchema(alloc, set_null_update_schema_json);
    defer set_null_update_parsed_schema.deinit(alloc);
    const set_null_update_runtime_schema = try schema_api_mod.deriveRuntimeTableSchema(alloc, set_null_update_parsed_schema);
    defer schema_mod.freeSchema(alloc, set_null_update_runtime_schema);
    try set_null_update_db.setSchema(set_null_update_runtime_schema);

    try set_null_update_db.batch(.{
        .writes = &.{
            .{ .key = "price:parent:v1", .value = "{\"sku\":\"parent\",\"valid_from\":0,\"valid_to\":5,\"price\":10}" },
            .{ .key = "price:parent:v2", .value = "{\"sku\":\"parent\",\"valid_from\":5,\"valid_to\":10,\"price\":12}" },
            .{ .key = "price:child:covered", .value = "{\"sku\":\"child:covered\",\"parent_sku\":\"parent\",\"valid_from\":6,\"valid_to\":8,\"price\":99}" },
        },
    });

    try set_null_update_db.batch(.{
        .writes = &.{.{ .key = "price:parent:v1", .value = "{\"sku\":\"parent\",\"valid_from\":0,\"valid_to\":4,\"price\":10}" }},
    });
    const update_covered_child = (try set_null_update_db.get(alloc, "price:child:covered")) orelse return error.TestExpectedEqual;
    defer alloc.free(update_covered_child);
    try std.testing.expect(std.mem.indexOf(u8, update_covered_child, "\"parent_sku\":\"parent\"") != null);

    try set_null_update_db.batch(.{
        .writes = &.{.{ .key = "price:parent:v2", .value = "{\"sku\":\"parent\",\"valid_from\":5,\"valid_to\":7,\"price\":12}" }},
    });
    const update_uncovered_child = (try set_null_update_db.get(alloc, "price:child:covered")) orelse return error.TestExpectedEqual;
    defer alloc.free(update_uncovered_child);
    try std.testing.expect(std.mem.indexOf(u8, update_uncovered_child, "\"parent_sku\"") == null);

    var cascade_path_buf: [256]u8 = undefined;
    const cascade_path = TestHelpers.tempPath(&cascade_path_buf);
    defer TestHelpers.cleanupTempDir(cascade_path);

    var cascade_db = try DB.open(alloc, std.mem.span(cascade_path), .{
        .primary_backend = .{ .mem = .{} },
    });
    defer cascade_db.close();

    const cascade_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"sku":{"type":"keyword"},"parent_sku":{"type":"keyword"},"valid_from":{"type":"numeric"},"valid_to":{"type":"numeric"},"price":{"type":"numeric"}},"required":["sku","valid_from","valid_to"],"additionalProperties":false}}},"periods":[{"name":"valid_time","start_column":"valid_from","end_column":"valid_to"}],"primary_key":{"columns":["sku"],"without_overlaps_period":"valid_time"},"foreign_keys":[{"name":"price_parent_period_fkey","columns":["parent_sku"],"period":"valid_time","references":{"table":"row","columns":["sku"],"period":"valid_time"},"on_delete":"cascade","validation_state":"enforced"}]}
    ;
    var cascade_parsed_schema = try schema_api_mod.parseValidatedTableSchema(alloc, cascade_schema_json);
    defer cascade_parsed_schema.deinit(alloc);
    const cascade_runtime_schema = try schema_api_mod.deriveRuntimeTableSchema(alloc, cascade_parsed_schema);
    defer schema_mod.freeSchema(alloc, cascade_runtime_schema);
    try cascade_db.setSchema(cascade_runtime_schema);

    try cascade_db.batch(.{
        .writes = &.{
            .{ .key = "price:parent:v1", .value = "{\"sku\":\"parent\",\"valid_from\":0,\"valid_to\":5,\"price\":10}" },
            .{ .key = "price:parent:v2", .value = "{\"sku\":\"parent\",\"valid_from\":5,\"valid_to\":10,\"price\":12}" },
            .{ .key = "price:child:covered", .value = "{\"sku\":\"child:covered\",\"parent_sku\":\"parent\",\"valid_from\":6,\"valid_to\":8,\"price\":99}" },
        },
    });

    try cascade_db.batch(.{ .deletes = &.{"price:parent:v1"} });
    const cascade_child = (try cascade_db.get(alloc, "price:child:covered")) orelse return error.TestExpectedEqual;
    defer alloc.free(cascade_child);
    try std.testing.expect(std.mem.indexOf(u8, cascade_child, "\"parent_sku\":\"parent\"") != null);

    try cascade_db.batch(.{ .deletes = &.{"price:parent:v2"} });
    try std.testing.expect((try cascade_db.get(alloc, "price:child:covered")) == null);
}

test "db relational temporal foreign key repair rebuilds coverage refs" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .mem = .{} },
    });
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"sku":{"type":"keyword"},"parent_sku":{"type":"keyword"},"valid_from":{"type":"numeric"},"valid_to":{"type":"numeric"},"price":{"type":"numeric"}},"required":["sku","valid_from","valid_to"],"additionalProperties":false}}},"periods":[{"name":"valid_time","start_column":"valid_from","end_column":"valid_to"}],"primary_key":{"columns":["sku"],"without_overlaps_period":"valid_time"},"foreign_keys":[{"name":"price_parent_period_fkey","columns":["parent_sku"],"period":"valid_time","references":{"table":"row","columns":["sku"],"period":"valid_time"},"validation_state":"enforced"}]}
    ;
    var parsed_schema = try schema_api_mod.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try schema_api_mod.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    try db.batch(.{
        .writes = &.{
            .{ .key = "price:parent:v1", .value = "{\"sku\":\"parent\",\"valid_from\":0,\"valid_to\":5,\"price\":10}" },
            .{ .key = "price:parent:v2", .value = "{\"sku\":\"parent\",\"valid_from\":5,\"valid_to\":10,\"price\":12}" },
            .{ .key = "price:child:covered", .value = "{\"sku\":\"child:covered\",\"parent_sku\":\"parent\",\"valid_from\":2,\"valid_to\":8,\"price\":99}" },
        },
    });

    const child_row = try mapper.buildRelationalRowValueAlloc(
        alloc,
        "{\"sku\":\"child:covered\",\"parent_sku\":\"parent\",\"valid_from\":2,\"valid_to\":8,\"price\":99}",
        runtime_schema.relational_columns,
    );
    defer alloc.free(child_row);
    const parent_key = (try relational_store_mod.foreignKeyReferenceValueAlloc(alloc, child_row, runtime_schema.foreign_keys[0])) orelse return error.TestExpectedEqual;
    defer alloc.free(parent_key);
    const ref_key = try internal_keys.relationalForeignKeyRefKeyAlloc(
        alloc,
        "price_parent_period_fkey",
        "row",
        parent_key,
        "row",
        "price:child:covered",
    );
    defer alloc.free(ref_key);

    const clean_report = try db.validateForeignKeyRefsInRangeForConstraint("price_parent_period_fkey", "", "");
    try std.testing.expect(clean_report.valid());
    try db.core.store.delete(ref_key);

    const missing_report = try db.validateForeignKeyRefsInRangeForConstraint("price_parent_period_fkey", "", "");
    try std.testing.expect(!missing_report.valid());
    try std.testing.expectEqual(@as(u64, 1), missing_report.missing_ref_rows);

    const repaired_report = try db.repairForeignKeyRefsInRangeForConstraint("price_parent_period_fkey", "", "");
    try std.testing.expectEqual(@as(u64, 1), repaired_report.repaired_ref_rows);
    const validate_repaired_report = try db.validateForeignKeyRefsInRangeForConstraint("price_parent_period_fkey", "", "");
    try std.testing.expect(validate_repaired_report.valid());

    try std.testing.expectError(error.ForeignKeyViolation, db.batch(.{
        .deletes = &.{"price:parent:v1"},
    }));
}

test "db relational temporal portion mutation requires temporal primary identity" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .mem = .{} },
    });
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"sku":{"type":"keyword"},"valid_from":{"type":"numeric"},"valid_to":{"type":"numeric"},"price":{"type":"numeric"}},"required":["sku","valid_from","valid_to"],"additionalProperties":false}}},"periods":[{"name":"valid_time","start_column":"valid_from","end_column":"valid_to"}],"primary_key":{"columns":["sku"]}}
    ;
    var parsed_schema = try schema_api_mod.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try schema_api_mod.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    const txn_id = try db.beginTransaction(2_000);
    defer db.abortTransaction(txn_id, 2_001) catch {};
    const predicates = [_]schema_mod.RelationalCheck{.{
        .name = "",
        .field = "sku",
        .op = .eq,
        .value_json = "\"sku:a\"",
    }};
    const operations = [_]types.TransformOp{.{
        .op = .set,
        .path = "price",
        .value_json = "99",
    }};
    try std.testing.expectError(error.UnsupportedQueryRequest, db.mutateRelationalRowsFromSource(alloc, runtime_schema, .{
        .kind = .update,
        .source = .{
            .predicates = predicates[0..],
            .row_claim = .{
                .mode = .for_update,
                .owner_id = "session:temporal-non-primary",
                .txn_id = txn_id,
            },
        },
        .operations = operations[0..],
        .temporal_portion = .{
            .period = "valid_time",
            .from_json = "3",
            .to_json = "7",
        },
    }));
}

test "db relational temporal mutation source splits portions transactionally" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .mem = .{} },
    });
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"sku":{"type":"keyword"},"valid_from":{"type":"numeric"},"valid_to":{"type":"numeric"},"price":{"type":"numeric"}},"required":["sku","valid_from","valid_to"],"additionalProperties":false}}},"periods":[{"name":"valid_time","start_column":"valid_from","end_column":"valid_to"}],"primary_key":{"columns":["sku"],"without_overlaps_period":"valid_time"},"relational_indexes":[{"name":"sku","owner_kind":"relational_column","owner_name":"sku","access_method":"scalar_column","columns":["sku"]}]}
    ;
    var parsed_schema = try schema_api_mod.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try schema_api_mod.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    try db.batch(.{
        .writes = &.{
            .{ .key = "price:a:v1", .value = "{\"sku\":\"sku:a\",\"valid_from\":0,\"valid_to\":10,\"price\":10}" },
            .{ .key = "price:b:v1", .value = "{\"sku\":\"sku:b\",\"valid_from\":0,\"valid_to\":10,\"price\":20}" },
        },
        .timestamp_ns = 1_000,
    });

    const update_txn = try db.beginTransaction(2_000);
    const update_predicates = [_]schema_mod.RelationalCheck{.{
        .name = "",
        .field = "sku",
        .op = .eq,
        .value_json = "\"sku:a\"",
    }};
    const update_operations = [_]types.TransformOp{.{
        .op = .set,
        .path = "price",
        .value_json = "99",
    }};
    var update = try db.mutateRelationalRowsFromSource(alloc, runtime_schema, .{
        .kind = .update,
        .source = .{
            .predicates = update_predicates[0..],
            .row_claim = .{
                .mode = .for_update,
                .owner_id = "session:temporal-update",
                .txn_id = update_txn,
            },
        },
        .operations = update_operations[0..],
        .temporal_portion = .{
            .period = "valid_time",
            .from_json = "3",
            .to_json = "7",
        },
        .returning_all = true,
    });
    defer update.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 1), update.matched);
    try std.testing.expectEqual(@as(u32, 1), update.staged);
    try std.testing.expectEqual(@as(usize, 1), update.returning_rows.len);
    try TestHelpers.expectRelationalTemporalPriceRow(alloc, update.returning_rows[0], "sku:a", 3, 7, 99);
    try db.commitTransaction(update_txn, 2_010);

    const order_by = [_]types.RelationalRowsQueryOrder{.{
        .field = "valid_from",
        .direction = .asc,
    }};
    var sku_a_rows = try db.queryRelationalRows(alloc, runtime_schema, .{
        .predicates = update_predicates[0..],
        .select_all = true,
        .order_by = order_by[0..],
    });
    defer sku_a_rows.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 3), sku_a_rows.total);
    try std.testing.expectEqual(@as(usize, 3), sku_a_rows.rows.len);
    try TestHelpers.expectRelationalTemporalPriceRow(alloc, sku_a_rows.rows[0], "sku:a", 0, 3, 10);
    try TestHelpers.expectRelationalTemporalPriceRow(alloc, sku_a_rows.rows[1], "sku:a", 3, 7, 99);
    try TestHelpers.expectRelationalTemporalPriceRow(alloc, sku_a_rows.rows[2], "sku:a", 7, 10, 10);
    try TestHelpers.expectRelationalTemporalPrimarySelectorPriceRow(alloc, &db, runtime_schema, "sku:a", "2", 0, 3, 10);
    try TestHelpers.expectRelationalTemporalPrimarySelectorPriceRow(alloc, &db, runtime_schema, "sku:a", "5", 3, 7, 99);
    try TestHelpers.expectRelationalTemporalPrimarySelectorPriceRow(alloc, &db, runtime_schema, "sku:a", "8", 7, 10, 10);

    const repeat_update_txn = try db.beginTransaction(2_012);
    const repeat_update_operations = [_]types.TransformOp{.{
        .op = .set,
        .path = "price",
        .value_json = "77",
    }};
    var repeat_update = try db.mutateRelationalRowsFromSource(alloc, runtime_schema, .{
        .kind = .update,
        .source = .{
            .predicates = update_predicates[0..],
            .row_claim = .{
                .mode = .for_update,
                .owner_id = "session:temporal-repeat-update",
                .txn_id = repeat_update_txn,
            },
        },
        .operations = repeat_update_operations[0..],
        .temporal_portion = .{
            .period = "valid_time",
            .from_json = "8",
            .to_json = "9",
        },
        .returning_all = true,
    });
    defer repeat_update.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 3), repeat_update.matched);
    try std.testing.expectEqual(@as(u32, 1), repeat_update.staged);
    try std.testing.expectEqual(@as(usize, 1), repeat_update.returning_rows.len);
    try TestHelpers.expectRelationalTemporalPriceRow(alloc, repeat_update.returning_rows[0], "sku:a", 8, 9, 77);
    try db.commitTransaction(repeat_update_txn, 2_018);

    var repeated_rows = try db.queryRelationalRows(alloc, runtime_schema, .{
        .predicates = update_predicates[0..],
        .select_all = true,
        .order_by = order_by[0..],
    });
    defer repeated_rows.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 5), repeated_rows.total);
    try std.testing.expectEqual(@as(usize, 5), repeated_rows.rows.len);
    try TestHelpers.expectRelationalTemporalPriceRow(alloc, repeated_rows.rows[0], "sku:a", 0, 3, 10);
    try TestHelpers.expectRelationalTemporalPriceRow(alloc, repeated_rows.rows[1], "sku:a", 3, 7, 99);
    try TestHelpers.expectRelationalTemporalPriceRow(alloc, repeated_rows.rows[2], "sku:a", 7, 8, 10);
    try TestHelpers.expectRelationalTemporalPriceRow(alloc, repeated_rows.rows[3], "sku:a", 8, 9, 77);
    try TestHelpers.expectRelationalTemporalPriceRow(alloc, repeated_rows.rows[4], "sku:a", 9, 10, 10);
    try TestHelpers.expectRelationalTemporalPrimarySelectorPriceRow(alloc, &db, runtime_schema, "sku:a", "8.5", 8, 9, 77);

    const multi_row_update_txn = try db.beginTransaction(2_019);
    const multi_row_update_operations = [_]types.TransformOp{.{
        .op = .set,
        .path = "price",
        .value_json = "55",
    }};
    var multi_row_update = try db.mutateRelationalRowsFromSource(alloc, runtime_schema, .{
        .kind = .update,
        .source = .{
            .predicates = update_predicates[0..],
            .row_claim = .{
                .mode = .for_update,
                .owner_id = "session:temporal-multi-row-update",
                .txn_id = multi_row_update_txn,
            },
        },
        .operations = multi_row_update_operations[0..],
        .temporal_portion = .{
            .period = "valid_time",
            .from_json = "3",
            .to_json = "9",
        },
        .returning_all = true,
    });
    defer multi_row_update.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 5), multi_row_update.matched);
    try std.testing.expectEqual(@as(u32, 3), multi_row_update.staged);
    try std.testing.expectEqual(@as(usize, 3), multi_row_update.returning_rows.len);
    try TestHelpers.expectRelationalTemporalPriceRow(alloc, multi_row_update.returning_rows[0], "sku:a", 3, 7, 55);
    try TestHelpers.expectRelationalTemporalPriceRow(alloc, multi_row_update.returning_rows[1], "sku:a", 7, 8, 55);
    try TestHelpers.expectRelationalTemporalPriceRow(alloc, multi_row_update.returning_rows[2], "sku:a", 8, 9, 55);
    try db.commitTransaction(multi_row_update_txn, 2_019);

    var multi_row_rows = try db.queryRelationalRows(alloc, runtime_schema, .{
        .predicates = update_predicates[0..],
        .select_all = true,
        .order_by = order_by[0..],
    });
    defer multi_row_rows.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 5), multi_row_rows.total);
    try std.testing.expectEqual(@as(usize, 5), multi_row_rows.rows.len);
    try TestHelpers.expectRelationalTemporalPriceRow(alloc, multi_row_rows.rows[0], "sku:a", 0, 3, 10);
    try TestHelpers.expectRelationalTemporalPriceRow(alloc, multi_row_rows.rows[1], "sku:a", 3, 7, 55);
    try TestHelpers.expectRelationalTemporalPriceRow(alloc, multi_row_rows.rows[2], "sku:a", 7, 8, 55);
    try TestHelpers.expectRelationalTemporalPriceRow(alloc, multi_row_rows.rows[3], "sku:a", 8, 9, 55);
    try TestHelpers.expectRelationalTemporalPriceRow(alloc, multi_row_rows.rows[4], "sku:a", 9, 10, 10);
    try TestHelpers.expectRelationalTemporalPrimarySelectorPriceRow(alloc, &db, runtime_schema, "sku:a", "4", 3, 7, 55);
    try TestHelpers.expectRelationalTemporalPrimarySelectorPriceRow(alloc, &db, runtime_schema, "sku:a", "7.5", 7, 8, 55);
    try TestHelpers.expectRelationalTemporalPrimarySelectorPriceRow(alloc, &db, runtime_schema, "sku:a", "8.5", 8, 9, 55);

    const scanned_rows = try relational_store_mod.scanRowsAlloc(alloc, db.core.store, "", "");
    defer relational_store_mod.freeRows(alloc, scanned_rows);
    for (scanned_rows) |row| {
        if (std.mem.indexOf(u8, row.doc_key, "~period:")) |first_period_suffix| {
            const nested_suffix_start = first_period_suffix + "~period:".len;
            try std.testing.expect(std.mem.indexOf(u8, row.doc_key[nested_suffix_start..], "~period:") == null);
        }
    }

    const invalid_txn = try db.beginTransaction(2_020);
    defer db.abortTransaction(invalid_txn, 2_021) catch {};
    const invalid_operations = [_]types.TransformOp{.{
        .op = .set,
        .path = "valid_from",
        .value_json = "4",
    }};
    try std.testing.expectError(error.InvalidQueryRequest, db.mutateRelationalRowsFromSource(alloc, runtime_schema, .{
        .kind = .update,
        .source = .{
            .predicates = update_predicates[0..],
            .row_claim = .{
                .mode = .for_update,
                .owner_id = "session:temporal-invalid",
                .txn_id = invalid_txn,
            },
        },
        .operations = invalid_operations[0..],
        .temporal_portion = .{
            .period = "valid_time",
            .from_json = "4",
            .to_json = "5",
        },
    }));

    const delete_txn = try db.beginTransaction(2_030);
    const delete_predicates = [_]schema_mod.RelationalCheck{.{
        .name = "",
        .field = "sku",
        .op = .eq,
        .value_json = "\"sku:b\"",
    }};
    var delete = try db.mutateRelationalRowsFromSource(alloc, runtime_schema, .{
        .kind = .delete,
        .source = .{
            .predicates = delete_predicates[0..],
            .row_claim = .{
                .mode = .for_update,
                .owner_id = "session:temporal-delete",
                .txn_id = delete_txn,
            },
        },
        .temporal_portion = .{
            .period = "valid_time",
            .from_json = "2",
            .to_json = "8",
        },
        .returning_all = true,
    });
    defer delete.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 1), delete.matched);
    try std.testing.expectEqual(@as(u32, 1), delete.staged);
    try std.testing.expectEqual(@as(usize, 1), delete.returning_rows.len);
    try TestHelpers.expectRelationalTemporalPriceRow(alloc, delete.returning_rows[0], "sku:b", 2, 8, 20);
    try db.commitTransaction(delete_txn, 2_040);

    var sku_b_rows = try db.queryRelationalRows(alloc, runtime_schema, .{
        .predicates = delete_predicates[0..],
        .select_all = true,
        .order_by = order_by[0..],
    });
    defer sku_b_rows.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 2), sku_b_rows.total);
    try std.testing.expectEqual(@as(usize, 2), sku_b_rows.rows.len);
    try TestHelpers.expectRelationalTemporalPriceRow(alloc, sku_b_rows.rows[0], "sku:b", 0, 2, 20);
    try TestHelpers.expectRelationalTemporalPriceRow(alloc, sku_b_rows.rows[1], "sku:b", 8, 10, 20);
}

test "db relational temporal mutation source preserves foreign key coverage" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .mem = .{} },
    });
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"sku":{"type":"keyword"},"parent_sku":{"type":"keyword"},"valid_from":{"type":"numeric"},"valid_to":{"type":"numeric"},"price":{"type":"numeric"}},"required":["sku","valid_from","valid_to"],"additionalProperties":false}}},"periods":[{"name":"valid_time","start_column":"valid_from","end_column":"valid_to"}],"primary_key":{"columns":["sku"],"without_overlaps_period":"valid_time"},"foreign_keys":[{"name":"price_parent_period_fkey","columns":["parent_sku"],"period":"valid_time","references":{"table":"row","columns":["sku"],"period":"valid_time"},"validation_state":"enforced"}],"relational_indexes":[{"name":"sku","owner_kind":"relational_column","owner_name":"sku","access_method":"scalar_column","columns":["sku"]},{"name":"parent_sku","owner_kind":"relational_column","owner_name":"parent_sku","access_method":"scalar_column","columns":["parent_sku"]}]}
    ;
    var parsed_schema = try schema_api_mod.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try schema_api_mod.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    try db.batch(.{
        .writes = &.{
            .{ .key = "price:parent:v1", .value = "{\"sku\":\"parent\",\"valid_from\":0,\"valid_to\":10,\"price\":10}" },
            .{ .key = "price:child:v1", .value = "{\"sku\":\"child\",\"parent_sku\":\"parent\",\"valid_from\":0,\"valid_to\":10,\"price\":20}" },
        },
        .timestamp_ns = 1_000,
    });

    const update_txn = try db.beginTransaction(2_000);
    const child_predicates = [_]schema_mod.RelationalCheck{.{
        .name = "",
        .field = "sku",
        .op = .eq,
        .value_json = "\"child\"",
    }};
    const update_operations = [_]types.TransformOp{.{
        .op = .set,
        .path = "price",
        .value_json = "25",
    }};
    var update = try db.mutateRelationalRowsFromSource(alloc, runtime_schema, .{
        .kind = .update,
        .source = .{
            .predicates = child_predicates[0..],
            .row_claim = .{
                .mode = .for_update,
                .owner_id = "session:temporal-fk-update",
                .txn_id = update_txn,
            },
        },
        .operations = update_operations[0..],
        .temporal_portion = .{
            .period = "valid_time",
            .from_json = "3",
            .to_json = "7",
        },
        .returning_all = true,
    });
    defer update.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 1), update.matched);
    try std.testing.expectEqual(@as(u32, 1), update.staged);
    try std.testing.expectEqual(@as(usize, 1), update.returning_rows.len);
    try TestHelpers.expectRelationalTemporalPriceRow(alloc, update.returning_rows[0], "child", 3, 7, 25);
    try db.commitTransaction(update_txn, 2_010);

    const order_by = [_]types.RelationalRowsQueryOrder{.{
        .field = "valid_from",
        .direction = .asc,
    }};
    var child_rows = try db.queryRelationalRows(alloc, runtime_schema, .{
        .predicates = child_predicates[0..],
        .select_all = true,
        .order_by = order_by[0..],
    });
    defer child_rows.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 3), child_rows.total);
    try std.testing.expectEqual(@as(usize, 3), child_rows.rows.len);
    try TestHelpers.expectRelationalTemporalPriceRow(alloc, child_rows.rows[0], "child", 0, 3, 20);
    try TestHelpers.expectRelationalTemporalPriceRow(alloc, child_rows.rows[1], "child", 3, 7, 25);
    try TestHelpers.expectRelationalTemporalPriceRow(alloc, child_rows.rows[2], "child", 7, 10, 20);

    try std.testing.expectError(error.ForeignKeyViolation, db.batch(.{
        .deletes = &.{"price:parent:v1"},
        .timestamp_ns = 3_000,
    }));
}

test "db relational temporal workload combines portion splits foreign key repair and owner validation" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .mem = .{} },
    });
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"sku":{"type":"keyword"},"parent_sku":{"type":"keyword"},"valid_from":{"type":"numeric"},"valid_to":{"type":"numeric"},"price":{"type":"numeric"}},"required":["sku","valid_from","valid_to"],"additionalProperties":false}}},"periods":[{"name":"valid_time","start_column":"valid_from","end_column":"valid_to"}],"primary_key":{"columns":["sku"],"without_overlaps_period":"valid_time"},"foreign_keys":[{"name":"price_parent_period_fkey","columns":["parent_sku"],"period":"valid_time","references":{"table":"row","columns":["sku"],"period":"valid_time"},"validation_state":"enforced"}],"relational_indexes":[{"name":"sku","owner_kind":"relational_column","owner_name":"sku","access_method":"scalar_column","columns":["sku"]},{"name":"parent_sku","owner_kind":"relational_column","owner_name":"parent_sku","access_method":"scalar_column","columns":["parent_sku"]}]}
    ;
    var parsed_schema = try schema_api_mod.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try schema_api_mod.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    try db.batch(.{
        .writes = &.{
            .{ .key = "price:parent:v1", .value = "{\"sku\":\"parent\",\"valid_from\":0,\"valid_to\":5,\"price\":10}" },
            .{ .key = "price:parent:v2", .value = "{\"sku\":\"parent\",\"valid_from\":5,\"valid_to\":10,\"price\":12}" },
            .{ .key = "price:child:v1", .value = "{\"sku\":\"child\",\"parent_sku\":\"parent\",\"valid_from\":0,\"valid_to\":10,\"price\":20}" },
        },
        .timestamp_ns = 1_000,
    });

    const update_txn = try db.beginTransaction(2_000);
    const child_predicates = [_]schema_mod.RelationalCheck{.{
        .name = "",
        .field = "sku",
        .op = .eq,
        .value_json = "\"child\"",
    }};
    const update_operations = [_]types.TransformOp{.{
        .op = .set,
        .path = "price",
        .value_json = "25",
    }};
    var update = try db.mutateRelationalRowsFromSource(alloc, runtime_schema, .{
        .kind = .update,
        .source = .{
            .predicates = child_predicates[0..],
            .row_claim = .{
                .mode = .for_update,
                .owner_id = "session:temporal-combined",
                .txn_id = update_txn,
            },
        },
        .operations = update_operations[0..],
        .temporal_portion = .{
            .period = "valid_time",
            .from_json = "3",
            .to_json = "7",
        },
        .returning_all = true,
    });
    defer update.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 1), update.matched);
    try std.testing.expectEqual(@as(u32, 1), update.staged);
    try TestHelpers.expectRelationalTemporalPriceRow(alloc, update.returning_rows[0], "child", 3, 7, 25);
    try db.commitTransaction(update_txn, 2_010);

    const order_by = [_]types.RelationalRowsQueryOrder{.{
        .field = "valid_from",
        .direction = .asc,
    }};
    var child_rows = try db.queryRelationalRows(alloc, runtime_schema, .{
        .predicates = child_predicates[0..],
        .select_all = true,
        .order_by = order_by[0..],
    });
    defer child_rows.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 3), child_rows.total);
    try TestHelpers.expectRelationalTemporalPriceRow(alloc, child_rows.rows[0], "child", 0, 3, 20);
    try TestHelpers.expectRelationalTemporalPriceRow(alloc, child_rows.rows[1], "child", 3, 7, 25);
    try TestHelpers.expectRelationalTemporalPriceRow(alloc, child_rows.rows[2], "child", 7, 10, 20);

    const clean_fk_report = try db.validateForeignKeyRefsInRangeForConstraint("price_parent_period_fkey", "", "");
    try std.testing.expect(clean_fk_report.valid());
    const clean_unique_report = try db.validateUniqueConstraintRowsInRange("", "");
    try std.testing.expect(clean_unique_report.valid());

    const middle_child_row = try mapper.buildRelationalRowValueAlloc(
        alloc,
        "{\"sku\":\"child\",\"parent_sku\":\"parent\",\"valid_from\":3,\"valid_to\":7,\"price\":25}",
        runtime_schema.relational_columns,
    );
    defer alloc.free(middle_child_row);
    const parent_key = (try relational_store_mod.foreignKeyReferenceValueAlloc(alloc, middle_child_row, runtime_schema.foreign_keys[0])) orelse return error.TestExpectedEqual;
    defer alloc.free(parent_key);
    const fk_ref_lower = [_]u8{internal_keys.relational_foreign_key_ref_namespace};
    const fk_ref_upper = [_]u8{internal_keys.relational_foreign_key_ref_namespace + 1};
    const fk_refs = try db.core.store.scanRange(alloc, fk_ref_lower[0..], fk_ref_upper[0..]);
    defer docstore_mod.DocStore.freeResults(alloc, fk_refs);
    var ref_key: ?[]u8 = null;
    defer if (ref_key) |key| alloc.free(key);
    for (fk_refs) |entry| {
        var decoded = (try internal_keys.decodeRelationalForeignKeyRefKeyAlloc(alloc, entry.key)) orelse continue;
        defer decoded.deinit(alloc);
        if (!std.mem.eql(u8, decoded.constraint_name, "price_parent_period_fkey")) continue;
        if (!std.mem.eql(u8, decoded.parent_table, "row")) continue;
        if (!std.mem.eql(u8, decoded.parent_key, parent_key)) continue;
        if (!std.mem.eql(u8, decoded.child_table, "row")) continue;
        ref_key = try alloc.dupe(u8, entry.key);
        break;
    }
    const current_ref_key = ref_key orelse return error.TestExpectedEqual;
    try db.core.store.delete(current_ref_key);

    const missing_ref_report = try db.validateForeignKeyRefsInRangeForConstraint("price_parent_period_fkey", "", "");
    try std.testing.expect(!missing_ref_report.valid());
    try std.testing.expectEqual(@as(u64, 1), missing_ref_report.missing_ref_rows);

    const repaired_ref_report = try db.repairForeignKeyRefsInRangeForConstraint("price_parent_period_fkey", "", "");
    try std.testing.expect(!repaired_ref_report.valid());
    try std.testing.expectEqual(@as(u64, 1), repaired_ref_report.repaired_ref_rows);
    const repaired_validate_report = try db.validateForeignKeyRefsInRangeForConstraint("price_parent_period_fkey", "", "");
    try std.testing.expect(repaired_validate_report.valid());

    try std.testing.expectError(error.ForeignKeyViolation, db.batch(.{
        .deletes = &.{"price:parent:v1"},
        .timestamp_ns = 3_000,
    }));
    try std.testing.expectError(error.ForeignKeyViolation, db.batch(.{
        .writes = &.{.{ .key = "price:parent:v1", .value = "{\"sku\":\"parent\",\"valid_from\":0,\"valid_to\":4,\"price\":10}" }},
        .timestamp_ns = 3_100,
    }));
}
