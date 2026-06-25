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

const docstore_mod = @import("../docstore.zig");
const internal_keys = @import("../internal_keys.zig");
const schema_mod = @import("../schema.zig");
const transactions_mod = @import("../transactions.zig");
const mapper = @import("document_mapper.zig");
const db_internal = @import("internal.zig");
const relational_store_mod = @import("relational_store.zig");
const relational_rows = @import("relational_rows.zig");
const types = @import("types.zig");
const platform_clock = @import("../../platform/clock.zig");
const temporal_typed_dv = @import("../../section/typed_doc_values.zig");

const Allocator = std.mem.Allocator;

const temporal_bound_neg_infinity_tag: u8 = 0xf0;
const temporal_bound_pos_infinity_tag: u8 = 0xf1;
const foreign_key_integrity_progress_key_prefix = "\x00\x00__metadata__:foreign_key_integrity_progress";
const foreign_key_integrity_claim_key_prefix = "\x00\x00__metadata__:foreign_key_integrity_claim";
const foreign_key_integrity_job_key_prefix = "\x00\x00__metadata__:foreign_key_integrity_job";
const foreign_key_action_job_key_prefix = "\x00\x00__metadata__:foreign_key_action_job";
const foreign_key_action_schedule_key_prefix = "\x00\x00__metadata__:foreign_key_action_schedule";
const unique_constraint_integrity_progress_key_prefix = "\x00\x00__metadata__:unique_constraint_integrity_progress";
const foreign_key_action_default_cascade_max_depth: u32 = 64;
const foreign_key_externalized_parent_check_intent_key_prefix = "\x00\x00__metadata__:txn_fk_externalized_parent_check:";
const foreign_key_constraint_timing_override_intent_key_prefix = "\x00\x00__metadata__:txn_fk_constraint_timing:";

fn currentTimeNs() u64 {
    return platform_clock.Clock.real().nowRealtimeNs();
}

pub fn findUniqueConstraintMutation(
    mutations: []const types.UniqueConstraintMutation,
    constraint_name: []const u8,
    encoded_value: []const u8,
) ?types.UniqueConstraintMutation {
    for (mutations) |mutation| {
        if (std.mem.eql(u8, mutation.constraint_name, constraint_name) and std.mem.eql(u8, mutation.encoded_value, encoded_value)) return mutation;
    }
    return null;
}

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

pub fn foreignKeyExternalizedParentCheckIntentKeyAlloc(alloc: Allocator, txn_id: transactions_mod.TxnId, check: types.ForeignKeyParentCheck) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.appendSlice(alloc, foreign_key_externalized_parent_check_intent_key_prefix);
    try appendHexBytes(alloc, &out, txn_id[0..]);
    try appendHexField(alloc, &out, check.constraint_name);
    try appendHexField(alloc, &out, check.child_table);
    try appendHexField(alloc, &out, check.child_key);
    try appendHexField(alloc, &out, check.parent_table);
    try appendHexField(alloc, &out, check.parent_key);
    return try out.toOwnedSlice(alloc);
}

pub fn foreignKeyConstraintTimingOverrideIntentKeyAlloc(alloc: Allocator, txn_id: transactions_mod.TxnId, override: types.ForeignKeyConstraintTimingOverride) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.appendSlice(alloc, foreign_key_constraint_timing_override_intent_key_prefix);
    try appendHexBytes(alloc, &out, txn_id[0..]);
    try appendHexField(alloc, &out, override.constraint_name);
    return try out.toOwnedSlice(alloc);
}

pub fn encodeForeignKeyExternalizedParentCheckIntentValueAlloc(alloc: Allocator, check: types.ForeignKeyParentCheck) ![]u8 {
    const timing = switch (check.timing) {
        .immediate => "immediate",
        .deferred => "deferred",
    };
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    const prefix = try std.fmt.allocPrint(
        alloc,
        "{{\"constraint_name\":{f},\"child_table\":{f},\"child_key\":{f},\"parent_table\":{f},\"parent_key\":{f},\"timing\":{f}",
        .{
            std.json.fmt(check.constraint_name, .{}),
            std.json.fmt(check.child_table, .{}),
            std.json.fmt(check.child_key, .{}),
            std.json.fmt(check.parent_table, .{}),
            std.json.fmt(check.parent_key, .{}),
            std.json.fmt(timing, .{}),
        },
    );
    defer alloc.free(prefix);
    try out.appendSlice(alloc, prefix);
    if (check.parent_constraint_name) |name| {
        const encoded = try std.fmt.allocPrint(alloc, ",\"parent_constraint_name\":{f}", .{std.json.fmt(name, .{})});
        defer alloc.free(encoded);
        try out.appendSlice(alloc, encoded);
    }
    if (check.child_period_start_json) |json| {
        const encoded = try std.fmt.allocPrint(alloc, ",\"child_period_start\":{s}", .{json});
        defer alloc.free(encoded);
        try out.appendSlice(alloc, encoded);
    }
    if (check.child_period_end_json) |json| {
        const encoded = try std.fmt.allocPrint(alloc, ",\"child_period_end\":{s}", .{json});
        defer alloc.free(encoded);
        try out.appendSlice(alloc, encoded);
    }
    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

pub fn encodeForeignKeyConstraintTimingOverrideIntentValueAlloc(alloc: Allocator, override: types.ForeignKeyConstraintTimingOverride) ![]u8 {
    const timing = switch (override.timing) {
        .immediate => "immediate",
        .deferred => "deferred",
    };
    return try std.fmt.allocPrint(
        alloc,
        "{{\"constraint_name\":{f},\"timing\":{f}}}",
        .{
            std.json.fmt(override.constraint_name, .{}),
            std.json.fmt(timing, .{}),
        },
    );
}

pub fn collectTransactionExternalizedForeignKeyParentChecksAlloc(
    alloc: Allocator,
    mutations: []const transactions_mod.OwnedIntentMutation,
    skip_keys: *std.ArrayListUnmanaged([]const u8),
) ![]relational_store_mod.ExternalizedForeignKeyParentCheck {
    var out = std.ArrayListUnmanaged(relational_store_mod.ExternalizedForeignKeyParentCheck).empty;
    errdefer {
        for (out.items) |*check| freeExternalizedForeignKeyParentCheck(alloc, check);
        out.deinit(alloc);
    }
    for (mutations) |mutation| {
        if (!isForeignKeyExternalizedParentCheckIntentKey(mutation.key)) continue;
        const raw = mutation.value orelse continue;
        const check = try parseForeignKeyExternalizedParentCheckIntentValueAlloc(alloc, raw);
        var check_owned = true;
        errdefer if (check_owned) {
            var owned = check;
            freeExternalizedForeignKeyParentCheck(alloc, &owned);
        };
        const skip_key = try alloc.dupe(u8, mutation.key);
        var skip_key_owned = true;
        errdefer if (skip_key_owned) alloc.free(skip_key);
        try skip_keys.append(alloc, skip_key);
        skip_key_owned = false;
        try out.append(alloc, check);
        check_owned = false;
    }
    return try out.toOwnedSlice(alloc);
}

pub fn collectTransactionForeignKeyConstraintTimingOverridesAlloc(
    alloc: Allocator,
    mutations: []const transactions_mod.OwnedIntentMutation,
    skip_keys: *std.ArrayListUnmanaged([]const u8),
) ![]relational_store_mod.ForeignKeyConstraintTimingOverride {
    var out = std.ArrayListUnmanaged(relational_store_mod.ForeignKeyConstraintTimingOverride).empty;
    errdefer {
        for (out.items) |*override| freeRelationalForeignKeyConstraintTimingOverride(alloc, override);
        out.deinit(alloc);
    }
    for (mutations) |mutation| {
        if (!isForeignKeyConstraintTimingOverrideIntentKey(mutation.key)) continue;
        const raw = mutation.value orelse continue;
        const override = try parseForeignKeyConstraintTimingOverrideIntentValueAlloc(alloc, raw);
        var override_owned = true;
        errdefer if (override_owned) {
            var owned = override;
            freeRelationalForeignKeyConstraintTimingOverride(alloc, &owned);
        };
        const skip_key = try alloc.dupe(u8, mutation.key);
        var skip_key_owned = true;
        errdefer if (skip_key_owned) alloc.free(skip_key);
        try skip_keys.append(alloc, skip_key);
        skip_key_owned = false;
        try out.append(alloc, override);
        override_owned = false;
    }
    return try out.toOwnedSlice(alloc);
}

pub fn freeExternalizedForeignKeyParentChecks(alloc: Allocator, checks: []relational_store_mod.ExternalizedForeignKeyParentCheck) void {
    for (checks) |*check| freeExternalizedForeignKeyParentCheck(alloc, check);
    if (checks.len > 0) alloc.free(checks);
}

pub fn freeExternalizedForeignKeyParentCheck(alloc: Allocator, check: *relational_store_mod.ExternalizedForeignKeyParentCheck) void {
    alloc.free(@constCast(check.constraint_name));
    alloc.free(@constCast(check.child_table));
    alloc.free(@constCast(check.child_key));
    alloc.free(@constCast(check.parent_table));
    alloc.free(@constCast(check.parent_key));
    if (check.parent_constraint_name) |name| alloc.free(@constCast(name));
    if (check.child_period_start_json) |json| alloc.free(@constCast(json));
    if (check.child_period_end_json) |json| alloc.free(@constCast(json));
    check.* = undefined;
}

pub fn freeRelationalForeignKeyConstraintTimingOverrides(alloc: Allocator, overrides: []relational_store_mod.ForeignKeyConstraintTimingOverride) void {
    for (overrides) |*override| freeRelationalForeignKeyConstraintTimingOverride(alloc, override);
    if (overrides.len > 0) alloc.free(overrides);
}

pub fn freeRelationalForeignKeyConstraintTimingOverride(alloc: Allocator, override: *relational_store_mod.ForeignKeyConstraintTimingOverride) void {
    alloc.free(@constCast(override.constraint_name));
    override.* = undefined;
}

fn parseForeignKeyExternalizedParentCheckIntentValueAlloc(alloc: Allocator, raw: []const u8) !relational_store_mod.ExternalizedForeignKeyParentCheck {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{});
    defer parsed.deinit();
    const obj = switch (parsed.value) {
        .object => |object| object,
        else => return error.ForeignKeyViolation,
    };
    const timing = jsonObjectString(obj, "timing") orelse return error.ForeignKeyViolation;
    if (!std.mem.eql(u8, timing, "deferred") and !std.mem.eql(u8, timing, "immediate")) return error.ForeignKeyViolation;
    const constraint_name = try alloc.dupe(u8, jsonObjectString(obj, "constraint_name") orelse return error.ForeignKeyViolation);
    errdefer alloc.free(constraint_name);
    const child_table = try alloc.dupe(u8, jsonObjectString(obj, "child_table") orelse return error.ForeignKeyViolation);
    errdefer alloc.free(child_table);
    const child_key = try alloc.dupe(u8, jsonObjectString(obj, "child_key") orelse return error.ForeignKeyViolation);
    errdefer alloc.free(child_key);
    const parent_table = try alloc.dupe(u8, jsonObjectString(obj, "parent_table") orelse return error.ForeignKeyViolation);
    errdefer alloc.free(parent_table);
    const parent_key = try alloc.dupe(u8, jsonObjectString(obj, "parent_key") orelse return error.ForeignKeyViolation);
    errdefer alloc.free(parent_key);
    const parent_constraint_name = if (jsonObjectString(obj, "parent_constraint_name")) |name|
        try alloc.dupe(u8, name)
    else
        null;
    errdefer if (parent_constraint_name) |name| alloc.free(name);
    const child_period_start_json = if (obj.get("child_period_start")) |value|
        try std.json.Stringify.valueAlloc(alloc, value, .{ .emit_null_optional_fields = false })
    else
        null;
    errdefer if (child_period_start_json) |json| alloc.free(json);
    const child_period_end_json = if (obj.get("child_period_end")) |value|
        try std.json.Stringify.valueAlloc(alloc, value, .{ .emit_null_optional_fields = false })
    else
        null;
    errdefer if (child_period_end_json) |json| alloc.free(json);
    if ((child_period_start_json == null) != (child_period_end_json == null)) return error.ForeignKeyViolation;
    return .{
        .constraint_name = constraint_name,
        .child_table = child_table,
        .child_key = child_key,
        .parent_table = parent_table,
        .parent_key = parent_key,
        .parent_constraint_name = parent_constraint_name,
        .child_period_start_json = child_period_start_json,
        .child_period_end_json = child_period_end_json,
        .timing = if (std.mem.eql(u8, timing, "deferred")) .deferred else .immediate,
    };
}

fn parseForeignKeyConstraintTimingOverrideIntentValueAlloc(alloc: Allocator, raw: []const u8) !relational_store_mod.ForeignKeyConstraintTimingOverride {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{});
    defer parsed.deinit();
    const obj = switch (parsed.value) {
        .object => |object| object,
        else => return error.ForeignKeyViolation,
    };
    const timing = jsonObjectString(obj, "timing") orelse return error.ForeignKeyViolation;
    if (!std.mem.eql(u8, timing, "deferred") and !std.mem.eql(u8, timing, "immediate")) return error.ForeignKeyViolation;
    const constraint_name = try alloc.dupe(u8, jsonObjectString(obj, "constraint_name") orelse return error.ForeignKeyViolation);
    errdefer alloc.free(constraint_name);
    return .{
        .constraint_name = constraint_name,
        .timing = if (std.mem.eql(u8, timing, "deferred")) .deferred else .immediate,
    };
}

fn isForeignKeyExternalizedParentCheckIntentKey(key: []const u8) bool {
    return std.mem.startsWith(u8, key, foreign_key_externalized_parent_check_intent_key_prefix);
}

fn isForeignKeyConstraintTimingOverrideIntentKey(key: []const u8) bool {
    return std.mem.startsWith(u8, key, foreign_key_constraint_timing_override_intent_key_prefix);
}

fn appendHexField(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: []const u8) !void {
    try out.append(alloc, ':');
    try appendHexBytes(alloc, out, value);
}

fn appendHexBytes(alloc: Allocator, out: *std.ArrayListUnmanaged(u8), value: []const u8) !void {
    const hex = "0123456789abcdef";
    try out.ensureUnusedCapacity(alloc, value.len * 2);
    for (value) |byte| {
        out.appendAssumeCapacity(hex[byte >> 4]);
        out.appendAssumeCapacity(hex[byte & 0x0f]);
    }
}

fn jsonObjectString(obj: std.json.ObjectMap, key: []const u8) ?[]const u8 {
    const value = obj.get(key) orelse return null;
    return switch (value) {
        .string => |text| text,
        else => null,
    };
}

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
        const RelationalTemporalBound = relational_rows.TemporalBound;
        const RelationalTemporalSpan = relational_rows.TemporalSpan;

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
            const report = try relational_store_mod.reconcileForeignKeyRefsInRangeWithPrimaryKey(
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
            } else if (constraint.without_overlaps_period != null) {
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
            for (unique_deletes) |mutation| {
                const key = if (mutationIsTemporal(mutation))
                    try internal_keys.relationalTemporalUniqueKeyAlloc(self.alloc, mutation.constraint_name, mutation.encoded_value, mutation.temporal_start.?, mutation.temporal_end.?, mutation.owner_key)
                else
                    try internal_keys.relationalUniqueKeyAlloc(self.alloc, mutation.constraint_name, mutation.encoded_value);
                errdefer self.alloc.free(key);
                try owned_keys.append(self.alloc, key);
                try intents.append(self.alloc, .{ .key = key, .value = null });
            }
            for (unique_writes) |mutation| {
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
                        const parent_key = (try foreignKeyParentKeyFromJsonAlloc(self.alloc, runtime_schema.relational_columns, foreign_key, write.value)) orelse continue;
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
                    if (findUniqueConstraintByName(runtime_schema.unique_constraints, constraint_name) == null) return error.ForeignKeyViolation;
                    if (findUniqueConstraintMutation(unique_deletes, constraint_name, check.parent_key) != null and
                        findUniqueConstraintMutation(unique_writes, constraint_name, check.parent_key) == null) return error.ForeignKeyViolation;
                    if (findUniqueConstraintMutation(unique_writes, constraint_name, check.parent_key) != null) continue;
                    const unique_key = try internal_keys.relationalUniqueKeyAlloc(self.alloc, constraint_name, check.parent_key);
                    defer self.alloc.free(unique_key);
                    const owner = self.core.store.get(self.alloc, unique_key) catch |err| switch (err) {
                        error.NotFound => return error.ForeignKeyViolation,
                        else => return err,
                    };
                    self.alloc.free(owner);
                    continue;
                }
                if (!isForeignKeyExternalDocKey(check.parent_key)) return error.ForeignKeyViolation;
                if (containsString(deletes, check.parent_key)) return error.ForeignKeyViolation;
                if (containsTransactionWrite(writes, check.parent_key)) continue;
                const existing = try self.get(self.alloc, check.parent_key);
                defer if (existing) |body| self.alloc.free(body);
                if (existing == null) return error.ForeignKeyViolation;
            }
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
                    const parent_key = try foreignKeyParentKeyFromJsonAlloc(self.alloc, runtime_schema.relational_columns, foreign_key, write.value);
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
            const parent_period = relational_rows.findPeriod(runtime_schema.periods, parent_period_name) orelse return error.ForeignKeyViolation;
            const start_column = relational_rows.findTemporalColumn(runtime_schema.relational_columns, parent_period.start_column) orelse return error.ForeignKeyViolation;
            const end_column = relational_rows.findTemporalColumn(runtime_schema.relational_columns, parent_period.end_column) orelse return error.ForeignKeyViolation;
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
            const start = try relational_rows.temporalStartBoundFromJsonAlloc(alloc, start_json, start_column);
            const end = try relational_rows.temporalEndBoundFromJsonAlloc(alloc, end_json, end_column);
            const span: RelationalTemporalSpan = .{ .start = start, .end = end };
            if (!relational_rows.temporalSpanValid(span)) return error.ForeignKeyViolation;
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
            while (relational_rows.temporalBoundLessThan(covered_end, child_span.end)) {
                const next = try findTemporalForeignKeyParentCoverageEnd(self, parent_constraint, parent_key, covered_end, child_span.end, deletes);
                if (next == null) return error.ForeignKeyViolation;
                if (!relational_rows.temporalBoundLessThan(covered_end, next.?)) return error.ForeignKeyViolation;
                covered_end = next.?;
                matched_any = true;
            }
            if (!matched_any or !relational_rows.temporalBoundEqual(covered_end, child_span.end)) return error.ForeignKeyViolation;
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
            if (relational_rows.temporalBoundLessThan(needed_start, parent_span.start)) return current_best;
            if (!relational_rows.temporalBoundLessThan(needed_start, parent_span.end)) return current_best;
            const candidate = if (relational_rows.temporalBoundLessThan(child_end, parent_span.end)) child_end else parent_span.end;
            if (current_best) |best| {
                return if (relational_rows.temporalBoundLessThan(best, candidate)) candidate else best;
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
                    const parent_key = (try foreignKeyParentKeyFromJsonAlloc(self.alloc, runtime_schema.relational_columns, foreign_key, write.value)) orelse continue;
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
                    if (try foreignKeyWriteReferencesParent(self.alloc, runtime_schema.relational_columns, foreign_key, write.value, check.parent_key)) return error.ForeignKeyViolation;
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
                        if (!try foreignKeyWriteReferencesParent(self.alloc, runtime_schema.relational_columns, foreign_key, write.value, check.parent_key)) continue;
                    }
                    return error.ForeignKeyViolation;
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

        fn findTransactionWrite(writes: []const types.TransactionWrite, key: []const u8) ?types.TransactionWrite {
            for (writes) |write| {
                if (std.mem.eql(u8, write.key, key)) return write;
            }
            return null;
        }

        fn foreignKeyWriteReferencesParent(
            alloc: Allocator,
            relational_columns: []const schema_mod.RelationalColumn,
            foreign_key: schema_mod.ForeignKey,
            value: []const u8,
            parent_key: []const u8,
        ) !bool {
            const actual_parent_key = (try foreignKeyParentKeyFromJsonAlloc(alloc, relational_columns, foreign_key, value)) orelse return false;
            defer alloc.free(actual_parent_key);
            return std.mem.eql(u8, actual_parent_key, parent_key);
        }

        fn foreignKeyParentKeyFromJsonAlloc(
            alloc: Allocator,
            relational_columns: []const schema_mod.RelationalColumn,
            foreign_key: schema_mod.ForeignKey,
            value: []const u8,
        ) !?[]u8 {
            const row = try mapper.buildRelationalRowValueAlloc(alloc, value, relational_columns);
            defer alloc.free(row);
            return try relational_store_mod.foreignKeyReferenceValueAlloc(alloc, row, foreign_key);
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
            const period = relational_rows.findPeriod(runtime_schema.periods, period_name) orelse return error.ForeignKeyViolation;
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
            const schema = self.core.schema orelse return relational_store_mod.ColumnIndexPolicy.all();
            if (schema.storage_mode != .relational) return relational_store_mod.ColumnIndexPolicy.all();
            return relational_store_mod.ColumnIndexPolicy.fromColumns(schema.relational_columns);
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
