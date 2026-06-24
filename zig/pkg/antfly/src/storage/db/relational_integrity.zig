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
const relational_store_mod = @import("relational_store.zig");
const types = @import("types.zig");
const platform_clock = @import("../../platform/clock.zig");

const Allocator = std.mem.Allocator;

const foreign_key_integrity_progress_key_prefix = "\x00\x00__metadata__:foreign_key_integrity_progress";
const foreign_key_integrity_claim_key_prefix = "\x00\x00__metadata__:foreign_key_integrity_claim";
const foreign_key_integrity_job_key_prefix = "\x00\x00__metadata__:foreign_key_integrity_job";
const foreign_key_action_job_key_prefix = "\x00\x00__metadata__:foreign_key_action_job";
const foreign_key_action_schedule_key_prefix = "\x00\x00__metadata__:foreign_key_action_schedule";
const unique_constraint_integrity_progress_key_prefix = "\x00\x00__metadata__:unique_constraint_integrity_progress";
const foreign_key_action_default_cascade_max_depth: u32 = 64;

fn currentTimeNs() u64 {
    return platform_clock.Clock.real().nowRealtimeNs();
}

pub fn Impl(comptime DB: type) type {
    return struct {
        const ForeignKeyIntegrityReport = relational_store_mod.ForeignKeyIntegrityReport;
        const ForeignKeyIntegrityViolation = relational_store_mod.ForeignKeyIntegrityViolation;
        const ForeignKeyDeletePlan = relational_store_mod.ForeignKeyDeletePlan;
        const UniqueConstraintIntegrityReport = relational_store_mod.UniqueConstraintIntegrityReport;

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
            if (openModeRequiresReadOnlyBackends(self)) return error.ReadOnly;
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
            if (openModeRequiresReadOnlyBackends(self)) return error.ReadOnly;
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
            if (openModeRequiresReadOnlyBackends(self)) return error.ReadOnly;
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
            if (openModeRequiresReadOnlyBackends(self)) return error.ReadOnly;
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
            const owner_constraints = try self.relationalIntegrityUniqueOwnerConstraintsAlloc(self.alloc, runtime_schema);
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

        fn foreignKeyIsEnforcedImmediate(foreign_key: schema_mod.ForeignKey) bool {
            return foreign_key.validation_state == .enforced and foreign_key.timing == .immediate;
        }

        fn foreignKeyIsLocallyEnforced(foreign_key: schema_mod.ForeignKey) bool {
            return foreign_key.validation_state == .enforced and
                (foreign_key.timing == .immediate or foreign_key.timing == .deferred);
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
            if (openModeRequiresReadOnlyBackends(self)) return;
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
            if (openModeRequiresReadOnlyBackends(self)) return;
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
            if (openModeRequiresReadOnlyBackends(self)) return error.ReadOnly;
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
            if (openModeRequiresReadOnlyBackends(self)) return error.ReadOnly;
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
            if (openModeRequiresReadOnlyBackends(self)) return error.ReadOnly;
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
            if (openModeRequiresReadOnlyBackends(self)) return error.ReadOnly;
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
            if (openModeRequiresReadOnlyBackends(self)) return error.ReadOnly;
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
            if (openModeRequiresReadOnlyBackends(self)) return error.ReadOnly;
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
            if (openModeRequiresReadOnlyBackends(self)) return error.ReadOnly;
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
            if (openModeRequiresReadOnlyBackends(self)) return error.ReadOnly;
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
            if (openModeRequiresReadOnlyBackends(self)) return error.ReadOnly;
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
            if (openModeRequiresReadOnlyBackends(self)) return error.ReadOnly;
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
            if (openModeRequiresReadOnlyBackends(self)) return error.ReadOnly;
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

        fn openModeRequiresReadOnlyBackends(self: *DB) bool {
            return self.open_mode == .query_readonly or self.open_mode == .status_only;
        }
    };
}
