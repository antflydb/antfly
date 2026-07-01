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
const platform = @import("antfly_platform");

const db_internal = @import("internal.zig");
const doc_identity = @import("doc_identity.zig");
const docstore_mod = @import("../docstore.zig");
const internal_keys = @import("../internal_keys.zig");
const mapper = @import("document_mapper.zig");
const platform_clock = @import("../../platform/clock.zig");
const platform_time = @import("../../platform/time.zig");
const relational_store_mod = @import("relational_store.zig");
const schema_api_mod = @import("../../schema/mod.zig");
const schema_mod = @import("../schema.zig");
const transactions_mod = @import("../transactions.zig");
const types = @import("types.zig");

const Allocator = std.mem.Allocator;
const row_claim_intent_key_prefix = relational_store_mod.row_claim_intent_key_prefix;

const TestHelpers = if (builtin.is_test) @import("test_support.zig") else struct {};

pub fn Impl(comptime DB: type) type {
    return struct {
        const ForeignKeyActionScheduleRecord = DB.ForeignKeyActionScheduleRecord;

        pub fn beginTransaction(self: *DB, timestamp_ns: u64) !transactions_mod.TxnId {
            const txn_id = makeTxnId(self);
            return try self.beginTransactionWithIdAndParticipants(txn_id, timestamp_ns, &.{});
        }

        pub fn beginTransactionWithId(self: *DB, txn_id: transactions_mod.TxnId, timestamp_ns: u64) !transactions_mod.TxnId {
            return try self.beginTransactionWithIdAndParticipants(txn_id, timestamp_ns, &.{});
        }

        pub fn beginTransactionWithParticipants(self: *DB, timestamp_ns: u64, participants: []const []const u8) !transactions_mod.TxnId {
            const txn_id = makeTxnId(self);
            return try self.beginTransactionWithIdAndParticipants(txn_id, timestamp_ns, participants);
        }

        pub fn beginTransactionWithIdAndParticipants(self: *DB, txn_id: transactions_mod.TxnId, timestamp_ns: u64, participants: []const []const u8) !transactions_mod.TxnId {
            lockApply(self);
            defer self.core.unlockApply();
            return try self.core.beginTransactionWithParticipants(txn_id, timestamp_ns, participants);
        }

        pub fn writeIntents(
            self: *DB,
            txn_id: transactions_mod.TxnId,
            intents: []const transactions_mod.WriteIntent,
            predicates: []const transactions_mod.VersionPredicate,
        ) !void {
            var relational_intents = std.ArrayListUnmanaged(transactions_mod.WriteIntent).empty;
            defer relational_intents.deinit(self.alloc);
            var owned_relational_values = std.ArrayListUnmanaged([]u8).empty;
            defer {
                for (owned_relational_values.items) |value| self.alloc.free(value);
                owned_relational_values.deinit(self.alloc);
            }
            const effective_intents = blk: {
                const relational_columns = relationalColumnsForStore(self) orelse break :blk intents;
                for (intents) |intent| {
                    const value = if (intent.value) |raw|
                        if (isMetadataKey(intent.key) or internal_keys.isInternalPhysicalTableDataKey(intent.key))
                            raw
                        else
                            try relational_store_mod.relationalStoreRowValueAlloc(self.alloc, raw, relational_columns, &owned_relational_values)
                    else
                        null;
                    try relational_intents.append(self.alloc, .{
                        .key = intent.key,
                        .value = value,
                    });
                }
                break :blk relational_intents.items;
            };
            var identity_upsert_keys = std.ArrayListUnmanaged([]const u8).empty;
            defer identity_upsert_keys.deinit(self.alloc);
            for (effective_intents) |intent| {
                if (intent.value == null or isMetadataKey(intent.key) or internal_keys.isInternalPhysicalTableDataKey(intent.key)) continue;
                try identity_upsert_keys.append(self.alloc, intent.key);
            }

            lockApply(self);
            defer self.core.unlockApply();
            try failIfIdentityOrdinalExhaustedForNewUpserts(self, identity_upsert_keys.items);
            try self.core.writeIntents(txn_id, effective_intents, predicates);
        }

        pub fn writeTransaction(self: *DB, txn_id: types.TxnId, req: types.TransactionIntentRequest) !void {
            var effective_ops = try self.coalesceKeyValueRequest(types.TransactionWrite, req.writes, req.deletes, req.transforms);
            defer effective_ops.deinit(self.alloc);
            try validateForeignKeyConstraintTimingOverrides(self, req.foreign_key_constraint_timing_overrides);
            if (req.foreign_key_externalized_parent_checks.len > 0) {
                try validateExternalizedForeignKeyParentChecks(self, req.foreign_key_externalized_parent_checks, req.foreign_key_constraint_timing_overrides, effective_ops.writes);
            }
            try validateForeignKeyReferenceShapes(self, effective_ops.writes);
            try validateForeignKeyParentChecks(self, req.foreign_key_parent_checks, effective_ops.writes, effective_ops.deletes, req.unique_constraint_writes, req.unique_constraint_deletes);
            try validateForeignKeyRefMutations(self, req.foreign_key_ref_writes);
            try validateForeignKeyRefMutations(self, req.foreign_key_ref_deletes);
            try validateRelationalIdentityRewriteRequest(req.relational_identity_rewrites, effective_ops.writes, effective_ops.deletes);

            var intents = std.ArrayListUnmanaged(transactions_mod.WriteIntent).empty;
            defer intents.deinit(self.alloc);
            var predicates = std.ArrayListUnmanaged(transactions_mod.VersionPredicate).empty;
            defer predicates.deinit(self.alloc);
            var owned_fk_action_keys = std.ArrayListUnmanaged([]u8).empty;
            defer {
                for (owned_fk_action_keys.items) |key| self.alloc.free(key);
                owned_fk_action_keys.deinit(self.alloc);
            }
            var owned_fk_action_values = std.ArrayListUnmanaged([]u8).empty;
            defer {
                for (owned_fk_action_values.items) |value| self.alloc.free(value);
                owned_fk_action_values.deinit(self.alloc);
            }
            var owned_fk_conflict_keys = std.ArrayListUnmanaged([]u8).empty;
            defer {
                for (owned_fk_conflict_keys.items) |key| self.alloc.free(key);
                owned_fk_conflict_keys.deinit(self.alloc);
            }
            var owned_metadata_keys = std.ArrayListUnmanaged([]u8).empty;
            defer {
                for (owned_metadata_keys.items) |key| self.alloc.free(key);
                owned_metadata_keys.deinit(self.alloc);
            }
            var owned_metadata_values = std.ArrayListUnmanaged([]u8).empty;
            defer {
                for (owned_metadata_values.items) |value| self.alloc.free(value);
                owned_metadata_values.deinit(self.alloc);
            }
            var owned_row_claim_predicate_keys = std.ArrayListUnmanaged([]u8).empty;
            defer {
                for (owned_row_claim_predicate_keys.items) |key| self.alloc.free(key);
                owned_row_claim_predicate_keys.deinit(self.alloc);
            }

            for (effective_ops.writes) |write| {
                try intents.append(self.alloc, .{
                    .key = write.key,
                    .value = write.value,
                });
            }
            for (effective_ops.deletes) |key| {
                try intents.append(self.alloc, .{
                    .key = key,
                    .value = null,
                });
            }
            for (req.relational_identity_rewrites) |rewrite| {
                const marker_key = try relationalIdentityRewriteIntentKeyAlloc(self.alloc, txn_id, rewrite);
                var marker_key_owned = true;
                errdefer if (marker_key_owned) self.alloc.free(marker_key);
                const marker_value = try encodeRelationalIdentityRewriteIntentValueAlloc(self.alloc, rewrite);
                var marker_value_owned = true;
                errdefer if (marker_value_owned) self.alloc.free(marker_value);
                try owned_metadata_keys.append(self.alloc, marker_key);
                marker_key_owned = false;
                try owned_metadata_values.append(self.alloc, marker_value);
                marker_value_owned = false;
                try intents.append(self.alloc, .{
                    .key = rewrite.old_key,
                    .value = null,
                });
                try intents.append(self.alloc, .{
                    .key = rewrite.new_key,
                    .value = rewrite.value,
                });
                try intents.append(self.alloc, .{
                    .key = marker_key,
                    .value = marker_value,
                });
            }
            for (req.predicates) |predicate| {
                try predicates.append(self.alloc, .{
                    .key = predicate.key,
                    .expected_version = predicate.expected_version,
                });
            }
            try appendRowClaimPredicatesForMutationKeys(
                self.alloc,
                &predicates,
                &owned_row_claim_predicate_keys,
                effective_ops.writes,
                effective_ops.deletes,
            );
            try appendRowClaimPredicatesForIdentityRewrites(
                self.alloc,
                &predicates,
                &owned_row_claim_predicate_keys,
                req.relational_identity_rewrites,
            );
            _ = try reclaimExpiredRowClaimIntentsForMutationKeys(
                self,
                effective_ops.writes,
                effective_ops.deletes,
                txn_id,
                monotonicTimeNs(),
                false,
            );
            _ = try reclaimExpiredRowClaimIntentsForIdentityRewrites(
                self,
                req.relational_identity_rewrites,
                txn_id,
                monotonicTimeNs(),
                false,
            );
            try applyForeignKeyParentDeleteActions(
                self,
                &intents,
                &owned_fk_action_keys,
                &owned_fk_action_values,
                req.foreign_key_parent_delete_checks,
            );
            try applyForeignKeySetNullChildActions(
                self,
                &intents,
                &owned_fk_action_keys,
                &owned_fk_action_values,
                req.foreign_key_set_null_children,
            );
            try applyForeignKeyCascadeChildActions(
                self,
                &intents,
                &owned_fk_action_keys,
                &owned_fk_action_values,
                req.foreign_key_cascade_children,
            );
            try appendForeignKeyActionScheduleIntents(
                self,
                &intents,
                &predicates,
                &owned_metadata_keys,
                &owned_metadata_values,
                req.foreign_key_action_schedules,
                monotonicTimeNs(),
            );
            try validateUniqueConstraintMutations(self, req.unique_constraint_writes, req.unique_constraint_deletes);
            try validateForeignKeyParentDeleteChecks(self, req.foreign_key_parent_delete_checks, req.foreign_key_constraint_timing_overrides, effective_ops.writes, effective_ops.deletes, req.foreign_key_ref_writes, req.foreign_key_ref_deletes);
            var owned_fk_ref_keys = std.ArrayListUnmanaged([]u8).empty;
            defer {
                for (owned_fk_ref_keys.items) |key| self.alloc.free(key);
                owned_fk_ref_keys.deinit(self.alloc);
            }
            try appendForeignKeyRefMutationIntents(self, &intents, &owned_fk_ref_keys, req.foreign_key_ref_writes, req.foreign_key_ref_deletes);
            var owned_unique_keys = std.ArrayListUnmanaged([]u8).empty;
            defer {
                for (owned_unique_keys.items) |key| self.alloc.free(key);
                owned_unique_keys.deinit(self.alloc);
            }
            var owned_unique_values = std.ArrayListUnmanaged([]u8).empty;
            defer {
                for (owned_unique_values.items) |value| self.alloc.free(value);
                owned_unique_values.deinit(self.alloc);
            }
            try appendUniqueConstraintMutationIntents(self, &intents, &owned_unique_keys, &owned_unique_values, req.unique_constraint_writes, req.unique_constraint_deletes);
            try appendForeignKeyConflictIntents(self, &intents, &owned_fk_conflict_keys, effective_ops.writes, req.foreign_key_parent_delete_checks, req.foreign_key_conflict_checks, req.foreign_key_ref_writes);
            try appendForeignKeyExternalizedParentCheckIntents(
                self,
                txn_id,
                &intents,
                &owned_metadata_keys,
                &owned_metadata_values,
                req.foreign_key_externalized_parent_checks,
            );
            try appendForeignKeyConstraintTimingOverrideIntents(
                self,
                txn_id,
                &intents,
                &owned_metadata_keys,
                &owned_metadata_values,
                req.foreign_key_constraint_timing_overrides,
            );

            try self.writeIntents(txn_id, intents.items, predicates.items);
        }

        pub fn claimRowsForTransaction(
            self: *DB,
            txn_id: types.TxnId,
            row_keys: []const []const u8,
            claim: types.RowClaimRequest,
        ) !void {
            if (!claim.mode.usesDurableIntent()) return error.InvalidQueryRequest;
            if (claim.owner_id.len == 0 or claim.lease_ms == 0) return error.InvalidQueryRequest;
            if (row_keys.len == 0) return;

            const claim_now_ns = monotonicTimeNs();
            const claim_value = try rowClaimIntentValueAlloc(self.alloc, txn_id, claim, claim_now_ns);
            defer self.alloc.free(claim_value);

            var owned_claim_keys = std.ArrayListUnmanaged([]u8).empty;
            defer {
                for (owned_claim_keys.items) |key| self.alloc.free(key);
                owned_claim_keys.deinit(self.alloc);
            }
            var intents = std.ArrayListUnmanaged(transactions_mod.WriteIntent).empty;
            defer intents.deinit(self.alloc);

            for (row_keys) |row_key| {
                if (!isUserRowMutationKey(row_key)) return error.InvalidQueryRequest;
                const claim_key = try rowClaimIntentKeyAlloc(self.alloc, row_key);
                var claim_key_owned = true;
                errdefer if (claim_key_owned) self.alloc.free(claim_key);
                try owned_claim_keys.append(self.alloc, claim_key);
                claim_key_owned = false;
                try intents.append(self.alloc, .{
                    .key = claim_key,
                    .value = claim_value,
                });
            }

            self.writeIntents(txn_id, intents.items, &.{}) catch |err| switch (err) {
                transactions_mod.TxnError.IntentConflict => {
                    const reclaimed = try reclaimExpiredRowClaimIntentsForRows(self, txn_id, row_keys, claim_now_ns);
                    if (reclaimed == 0) return err;
                    try self.writeIntents(txn_id, intents.items, &.{});
                },
                else => return err,
            };
        }

        pub fn tryClaimRowForTransaction(
            self: *DB,
            txn_id: types.TxnId,
            row_key: []const u8,
            claim: types.RowClaimRequest,
        ) !bool {
            self.claimRowsForTransaction(txn_id, &.{row_key}, claim) catch |err| switch (err) {
                transactions_mod.TxnError.IntentConflict => return false,
                else => return err,
            };
            return true;
        }

        fn appendForeignKeyActionScheduleIntents(
            self: *DB,
            intents: *std.ArrayListUnmanaged(transactions_mod.WriteIntent),
            predicates: *std.ArrayListUnmanaged(transactions_mod.VersionPredicate),
            owned_keys: *std.ArrayListUnmanaged([]u8),
            owned_values: *std.ArrayListUnmanaged([]u8),
            schedules: []const types.ForeignKeyActionScheduleMutation,
            now_ns: u64,
        ) !void {
            for (schedules) |schedule| {
                if (schedule.schedule_id.len == 0 or schedule.action_job_id.len == 0 or schedule.page_limit == 0) return error.InvalidForeignKeyActionJob;
                const canonical_action = foreignKeyActionJobCanonicalAction(schedule.action) orelse return error.InvalidForeignKeyActionJob;
                try validateForeignKeyActionLineage(schedule.cascade_depth, schedule.cascade_max_depth);
                try validateForeignKeyActionJobIdentity(schedule.action_job_id, canonical_action, schedule.worker_id, schedule.constraint_name, schedule.parent_table, schedule.parent_key, schedule.updated_parent_key);

                const key = try foreignKeyActionScheduleKeyAlloc(self.alloc, schedule.schedule_id);
                errdefer self.alloc.free(key);
                if (self.core.store.get(self.alloc, key)) |raw| {
                    defer self.alloc.free(raw);
                    const existing = try self.cloneForeignKeyActionScheduleRecordFromJson(raw);
                    defer self.freeForeignKeyActionScheduleRecord(existing);
                    try validateForeignKeyActionScheduleMatches(existing, schedule.action_job_id, canonical_action, schedule.constraint_name, schedule.parent_table, schedule.parent_key, schedule.updated_parent_key);
                    self.alloc.free(key);
                    continue;
                } else |err| switch (err) {
                    error.NotFound => {},
                    else => return err,
                }

                const record = ForeignKeyActionScheduleRecord{
                    .schedule_id = schedule.schedule_id,
                    .action_job_id = schedule.action_job_id,
                    .action = canonical_action,
                    .worker_id = schedule.worker_id,
                    .constraint_name = schedule.constraint_name,
                    .parent_table = schedule.parent_table,
                    .parent_key = schedule.parent_key,
                    .updated_parent_key = schedule.updated_parent_key,
                    .page_limit = schedule.page_limit,
                    .status = "pending",
                    .created_at_ns = now_ns,
                    .updated_at_ns = now_ns,
                    .completed = false,
                    .scheduled_groups = 0,
                    .cascade_depth = schedule.cascade_depth,
                    .cascade_max_depth = schedule.cascade_max_depth,
                    .last_error = null,
                };
                const payload = try std.json.Stringify.valueAlloc(self.alloc, record, .{ .emit_null_optional_fields = false });
                errdefer self.alloc.free(payload);
                try owned_keys.append(self.alloc, key);
                try owned_values.append(self.alloc, payload);
                try predicates.append(self.alloc, .{ .key = key, .expected_version = 0 });
                try intents.append(self.alloc, .{ .key = key, .value = payload });
            }
        }

        fn appendForeignKeyExternalizedParentCheckIntents(
            self: *DB,
            txn_id: types.TxnId,
            intents: *std.ArrayListUnmanaged(transactions_mod.WriteIntent),
            owned_keys: *std.ArrayListUnmanaged([]u8),
            owned_values: *std.ArrayListUnmanaged([]u8),
            checks: []const types.ForeignKeyParentCheck,
        ) !void {
            try self.appendForeignKeyExternalizedParentCheckIntents(txn_id, intents, owned_keys, owned_values, checks);
        }

        fn appendForeignKeyConstraintTimingOverrideIntents(
            self: *DB,
            txn_id: types.TxnId,
            intents: *std.ArrayListUnmanaged(transactions_mod.WriteIntent),
            owned_keys: *std.ArrayListUnmanaged([]u8),
            owned_values: *std.ArrayListUnmanaged([]u8),
            overrides: []const types.ForeignKeyConstraintTimingOverride,
        ) !void {
            try self.appendForeignKeyConstraintTimingOverrideIntents(txn_id, intents, owned_keys, owned_values, overrides);
        }

        fn validateUniqueConstraintMutations(
            self: *DB,
            unique_writes: []const types.UniqueConstraintMutation,
            unique_deletes: []const types.UniqueConstraintMutation,
        ) !void {
            try self.validateUniqueConstraintMutations(unique_writes, unique_deletes);
        }

        const findUniqueConstraintMutation = relational_store_mod.findUniqueConstraintMutation;

        fn appendUniqueConstraintMutationIntents(
            self: *DB,
            intents: *std.ArrayListUnmanaged(transactions_mod.WriteIntent),
            owned_keys: *std.ArrayListUnmanaged([]u8),
            owned_values: *std.ArrayListUnmanaged([]u8),
            unique_writes: []const types.UniqueConstraintMutation,
            unique_deletes: []const types.UniqueConstraintMutation,
        ) !void {
            try self.appendUniqueConstraintMutationIntents(intents, owned_keys, owned_values, unique_writes, unique_deletes);
        }

        fn appendForeignKeyRefMutationIntents(
            self: *DB,
            intents: *std.ArrayListUnmanaged(transactions_mod.WriteIntent),
            owned_keys: *std.ArrayListUnmanaged([]u8),
            ref_writes: []const types.ForeignKeyRefMutation,
            ref_deletes: []const types.ForeignKeyRefMutation,
        ) !void {
            try self.appendForeignKeyRefMutationIntents(intents, owned_keys, ref_writes, ref_deletes);
        }

        fn applyForeignKeyParentDeleteActions(
            self: *DB,
            intents: *std.ArrayListUnmanaged(transactions_mod.WriteIntent),
            owned_keys: *std.ArrayListUnmanaged([]u8),
            owned_values: *std.ArrayListUnmanaged([]u8),
            checks: []const types.ForeignKeyParentDeleteCheck,
        ) !void {
            try self.applyForeignKeyParentDeleteActions(intents, owned_keys, owned_values, checks);
        }

        fn applyForeignKeySetNullChildActions(
            self: *DB,
            intents: *std.ArrayListUnmanaged(transactions_mod.WriteIntent),
            owned_keys: *std.ArrayListUnmanaged([]u8),
            owned_values: *std.ArrayListUnmanaged([]u8),
            actions: []const types.ForeignKeySetNullChildAction,
        ) !void {
            try self.applyForeignKeySetNullChildActions(intents, owned_keys, owned_values, actions);
        }

        fn applyForeignKeyCascadeChildActions(
            self: *DB,
            intents: *std.ArrayListUnmanaged(transactions_mod.WriteIntent),
            owned_keys: *std.ArrayListUnmanaged([]u8),
            owned_values: *std.ArrayListUnmanaged([]u8),
            actions: []const types.ForeignKeyCascadeChildAction,
        ) !void {
            try self.applyForeignKeyCascadeChildActions(intents, owned_keys, owned_values, actions);
        }

        fn appendForeignKeyConflictIntents(
            self: *DB,
            intents: *std.ArrayListUnmanaged(transactions_mod.WriteIntent),
            owned_keys: *std.ArrayListUnmanaged([]u8),
            writes: []const types.TransactionWrite,
            parent_delete_checks: []const types.ForeignKeyParentDeleteCheck,
            conflict_checks: []const types.ForeignKeyConflictCheck,
            ref_writes: []const types.ForeignKeyRefMutation,
        ) !void {
            try self.appendForeignKeyConflictIntents(intents, owned_keys, writes, parent_delete_checks, conflict_checks, ref_writes);
        }

        fn validateForeignKeyParentChecks(
            self: *DB,
            checks: []const types.ForeignKeyParentCheck,
            writes: []const types.TransactionWrite,
            deletes: []const []const u8,
            unique_writes: []const types.UniqueConstraintMutation,
            unique_deletes: []const types.UniqueConstraintMutation,
        ) !void {
            try self.validateForeignKeyParentChecks(checks, writes, deletes, unique_writes, unique_deletes);
        }

        fn validateForeignKeyReferenceShapes(
            self: *DB,
            writes: []const types.TransactionWrite,
        ) !void {
            try self.validateForeignKeyReferenceShapes(writes);
        }

        fn validateExternalizedForeignKeyParentChecks(
            self: *DB,
            checks: []const types.ForeignKeyParentCheck,
            constraint_timing_overrides: []const types.ForeignKeyConstraintTimingOverride,
            writes: []const types.TransactionWrite,
        ) !void {
            try self.validateExternalizedForeignKeyParentChecks(checks, constraint_timing_overrides, writes);
        }

        fn validateForeignKeyConstraintTimingOverrides(
            self: *DB,
            overrides: []const types.ForeignKeyConstraintTimingOverride,
        ) !void {
            try self.validateForeignKeyConstraintTimingOverrides(overrides);
        }

        fn validateForeignKeyParentDeleteChecks(
            self: *DB,
            checks: []const types.ForeignKeyParentDeleteCheck,
            constraint_timing_overrides: []const types.ForeignKeyConstraintTimingOverride,
            writes: []const types.TransactionWrite,
            deletes: []const []const u8,
            ref_writes: []const types.ForeignKeyRefMutation,
            ref_deletes: []const types.ForeignKeyRefMutation,
        ) !void {
            try self.validateForeignKeyParentDeleteChecks(checks, constraint_timing_overrides, writes, deletes, ref_writes, ref_deletes);
        }

        fn validateForeignKeyRefMutations(self: *DB, mutations: []const types.ForeignKeyRefMutation) !void {
            try self.validateForeignKeyRefMutations(mutations);
        }

        fn isForeignKeyExternalDocKey(key: []const u8) bool {
            return !isMetadataKey(key) and !internal_keys.isInternalPhysicalTableDataKey(key);
        }

        fn findRuntimeForeignKeyByName(foreign_keys: []const schema_mod.ForeignKey, name: []const u8) ?schema_mod.ForeignKey {
            for (foreign_keys) |foreign_key| {
                if (std.mem.eql(u8, foreign_key.name, name)) return foreign_key;
            }
            return null;
        }

        // resolveTransactionTransforms, removePendingTransactionWrite, and freeTransactionWritesOwned
        // request coalescing is consolidated into coalesceKeyValueRequest above

        pub fn commitTransaction(self: *DB, txn_id: transactions_mod.TxnId, timestamp_ns: u64) !void {
            try self.resolveTransactionIntents(txn_id, .committed, timestamp_ns);
        }

        pub fn resolveTransactionIntents(self: *DB, txn_id: transactions_mod.TxnId, status: transactions_mod.TxnStatus, commit_version: u64) !void {
            lockApply(self);
            defer self.core.unlockApply();

            var raw_identity_upserts = std.ArrayListUnmanaged([]const u8).empty;
            defer {
                for (raw_identity_upserts.items) |key| self.alloc.free(@constCast(key));
                raw_identity_upserts.deinit(self.alloc);
            }
            var raw_identity_deletes = std.ArrayListUnmanaged([]const u8).empty;
            defer {
                for (raw_identity_deletes.items) |key| self.alloc.free(@constCast(key));
                raw_identity_deletes.deinit(self.alloc);
            }
            var identity_upserts = std.ArrayListUnmanaged([]const u8).empty;
            defer identity_upserts.deinit(self.alloc);
            var identity_deletes = std.ArrayListUnmanaged([]const u8).empty;
            defer identity_deletes.deinit(self.alloc);
            var identity_writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
            defer {
                for (identity_writes.items) |item| {
                    self.alloc.free(@constCast(item.key));
                    self.alloc.free(@constCast(item.value));
                }
                identity_writes.deinit(self.alloc);
            }
            var relational_extra_writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
            defer {
                for (relational_extra_writes.items) |item| {
                    self.alloc.free(@constCast(item.key));
                    self.alloc.free(@constCast(item.value));
                }
                relational_extra_writes.deinit(self.alloc);
            }
            var relational_extra_deletes = std.ArrayListUnmanaged([]const u8).empty;
            defer {
                for (relational_extra_deletes.items) |key| self.alloc.free(@constCast(key));
                relational_extra_deletes.deinit(self.alloc);
            }
            var relational_skip_intent_keys = std.ArrayListUnmanaged([]const u8).empty;
            defer {
                for (relational_skip_intent_keys.items) |key| self.alloc.free(@constCast(key));
                relational_skip_intent_keys.deinit(self.alloc);
            }

            if (status == .committed) {
                try self.core.collectTransactionIntentDocumentKeys(self.alloc, txn_id, &raw_identity_upserts, &raw_identity_deletes);
                for (raw_identity_upserts.items) |key| {
                    if (!isMetadataKey(key) and !internal_keys.isInternalPhysicalTableDataKey(key)) try identity_upserts.append(self.alloc, key);
                }
                for (raw_identity_deletes.items) |key| {
                    if (!isMetadataKey(key) and !internal_keys.isInternalPhysicalTableDataKey(key)) try identity_deletes.append(self.alloc, key);
                }
                const metadata_mutations = try self.core.collectTransactionIntentMutations(self.alloc, txn_id);
                defer {
                    for (metadata_mutations) |*mutation| mutation.deinit(self.alloc);
                    if (metadata_mutations.len > 0) self.alloc.free(metadata_mutations);
                }
                for (metadata_mutations) |mutation| {
                    if (!isRowClaimIntentMetadataKey(mutation.key) and !DB.isForeignKeyActionScheduleMetadataKey(mutation.key)) continue;
                    const skip_key = try self.alloc.dupe(u8, mutation.key);
                    var skip_key_owned = true;
                    errdefer if (skip_key_owned) self.alloc.free(skip_key);
                    if (isRowClaimIntentMetadataKey(mutation.key)) {
                        try relational_skip_intent_keys.append(self.alloc, skip_key);
                        skip_key_owned = false;
                        continue;
                    }
                    try relational_skip_intent_keys.append(self.alloc, skip_key);
                    skip_key_owned = false;

                    const metadata_key = try self.alloc.dupe(u8, mutation.key);
                    var metadata_key_owned = true;
                    errdefer if (metadata_key_owned) self.alloc.free(metadata_key);
                    if (mutation.value) |value| {
                        const metadata_value = try self.alloc.dupe(u8, value);
                        var metadata_value_owned = true;
                        errdefer if (metadata_value_owned) self.alloc.free(metadata_value);
                        try relational_extra_writes.append(self.alloc, .{ .key = metadata_key, .value = metadata_value });
                        metadata_key_owned = false;
                        metadata_value_owned = false;
                    } else {
                        try relational_extra_deletes.append(self.alloc, metadata_key);
                        metadata_key_owned = false;
                    }
                }
                try doc_identity.appendBatchIdentityMetadataForNamespaceAlloc(
                    self.alloc,
                    self.core.store,
                    self.core.identity_namespace,
                    self.core.nextDerivedSequence(),
                    &identity_writes,
                    identity_upserts.items,
                    identity_deletes.items,
                );
                if (relationalColumnsForStore(self) != null) {
                    const mutations = try self.core.collectTransactionIntentMutations(self.alloc, txn_id);
                    defer {
                        for (mutations) |*mutation| mutation.deinit(self.alloc);
                        if (mutations.len > 0) self.alloc.free(mutations);
                    }
                    std.mem.sort(transactions_mod.OwnedIntentMutation, mutations, {}, struct {
                        fn lessThan(_: void, lhs: transactions_mod.OwnedIntentMutation, rhs: transactions_mod.OwnedIntentMutation) bool {
                            const lhs_is_write = lhs.value != null;
                            const rhs_is_write = rhs.value != null;
                            if (lhs_is_write != rhs_is_write) return lhs_is_write;
                            return std.mem.lessThan(u8, lhs.key, rhs.key);
                        }
                    }.lessThan);
                    const externalized_fk_parent_checks = try collectTransactionExternalizedForeignKeyParentChecksAlloc(
                        self.alloc,
                        mutations,
                        &relational_skip_intent_keys,
                    );
                    defer freeExternalizedForeignKeyParentChecks(self.alloc, externalized_fk_parent_checks);
                    const fk_constraint_timing_overrides = try collectTransactionForeignKeyConstraintTimingOverridesAlloc(
                        self.alloc,
                        mutations,
                        &relational_skip_intent_keys,
                    );
                    defer freeRelationalForeignKeyConstraintTimingOverrides(self.alloc, fk_constraint_timing_overrides);
                    const relational_identity_rewrites = try collectTransactionRelationalIdentityRewritesAlloc(
                        self.alloc,
                        mutations,
                        &relational_skip_intent_keys,
                    );
                    defer freeRelationalIdentityRewrites(self.alloc, relational_identity_rewrites);
                    var relational_intent_delete_keys = std.ArrayListUnmanaged([]const u8).empty;
                    defer relational_intent_delete_keys.deinit(self.alloc);
                    for (mutations) |mutation| {
                        if (isMetadataKey(mutation.key) or internal_keys.isInternalPhysicalTableDataKey(mutation.key)) continue;
                        if (isRelationalIdentityRewriteEndpoint(relational_identity_rewrites, mutation.key)) continue;
                        if (mutation.value == null) try relational_intent_delete_keys.append(self.alloc, mutation.key);
                    }
                    var relational_extra_owned_keys = std.ArrayListUnmanaged([]u8).empty;
                    defer {
                        for (relational_extra_owned_keys.items) |key| self.alloc.free(key);
                        relational_extra_owned_keys.deinit(self.alloc);
                    }
                    var relational_extra_owned_values = std.ArrayListUnmanaged([]u8).empty;
                    defer {
                        for (relational_extra_owned_values.items) |value| self.alloc.free(value);
                        relational_extra_owned_values.deinit(self.alloc);
                    }
                    var relational_participant = relational_store_mod.WriteParticipant.initWithColumnIndexPolicy(
                        self.alloc,
                        self.core.store,
                        &relational_extra_writes,
                        &relational_extra_deletes,
                        &relational_extra_owned_keys,
                        &relational_extra_owned_values,
                        relationalColumnIndexPolicyForStore(self),
                    );
                    var relational_table_name: []const u8 = "";
                    if (self.core.schema) |runtime_schema| {
                        relational_table_name = runtime_schema.default_type;
                        relational_participant.configureForeignKeys(runtime_schema.default_type, runtime_schema.foreign_keys, relational_intent_delete_keys.items);
                        relational_participant.configureExternalizedForeignKeyParentChecks(externalized_fk_parent_checks);
                        relational_participant.configureForeignKeyConstraintTimingOverrides(fk_constraint_timing_overrides);
                        relational_participant.configurePrimaryKey(runtime_schema.primary_key);
                        relational_participant.configureUniqueConstraints(runtime_schema.unique_constraints);
                        relational_participant.configurePeriods(runtime_schema.periods, runtime_schema.relational_columns);
                    }
                    var relational_participant_prepared = false;
                    var relational_participant_closed = false;
                    defer if (relational_participant_prepared and !relational_participant_closed)
                        relational_participant.abort(null);
                    for (relational_identity_rewrites) |rewrite| {
                        const skip_old_key = try self.alloc.dupe(u8, rewrite.old_key);
                        var skip_old_key_owned = true;
                        errdefer if (skip_old_key_owned) self.alloc.free(skip_old_key);
                        try relational_skip_intent_keys.append(self.alloc, skip_old_key);
                        skip_old_key_owned = false;
                        const skip_new_key = try self.alloc.dupe(u8, rewrite.new_key);
                        var skip_new_key_owned = true;
                        errdefer if (skip_new_key_owned) self.alloc.free(skip_new_key);
                        try relational_skip_intent_keys.append(self.alloc, skip_new_key);
                        skip_new_key_owned = false;

                        const relational_columns = relationalColumnsForStore(self) orelse return error.UnsupportedOperation;
                        const row_value = try relational_store_mod.relationalStoreRowValueAlloc(self.alloc, rewrite.value, relational_columns, &relational_extra_owned_values);
                        relational_participant_prepared = true;
                        relational_participant.prepareIdentityRewrite(
                            relational_table_name,
                            rewrite.old_key,
                            rewrite.new_key,
                            row_value,
                            txn_id,
                        ) catch |err| {
                            if (err == error.ForeignKeyViolation) recordForeignKeyParentDeleteReject(self);
                            return err;
                        };
                        const old_document_key = try internal_keys.documentKeyAlloc(self.alloc, rewrite.old_key);
                        var old_document_key_owned = true;
                        errdefer if (old_document_key_owned) self.alloc.free(old_document_key);
                        try relational_extra_owned_keys.append(self.alloc, old_document_key);
                        old_document_key_owned = false;
                        try relational_extra_deletes.append(self.alloc, old_document_key);
                        const new_document_key = try internal_keys.documentKeyAlloc(self.alloc, rewrite.new_key);
                        var new_document_key_owned = true;
                        errdefer if (new_document_key_owned) self.alloc.free(new_document_key);
                        try relational_extra_owned_keys.append(self.alloc, new_document_key);
                        new_document_key_owned = false;
                        try relational_extra_deletes.append(self.alloc, new_document_key);
                        if (shouldWriteTimestamp(rewrite.old_key)) {
                            const old_timestamp_key = try makeTimestampKey(self.alloc, rewrite.old_key);
                            var old_timestamp_key_owned = true;
                            errdefer if (old_timestamp_key_owned) self.alloc.free(old_timestamp_key);
                            try relational_extra_owned_keys.append(self.alloc, old_timestamp_key);
                            old_timestamp_key_owned = false;
                            try relational_extra_deletes.append(self.alloc, old_timestamp_key);
                        }
                        if (shouldWriteTimestamp(rewrite.new_key)) {
                            const new_timestamp_key = try makeTimestampKey(self.alloc, rewrite.new_key);
                            var new_timestamp_key_owned = true;
                            errdefer if (new_timestamp_key_owned) self.alloc.free(new_timestamp_key);
                            const timestamp_value = try encodeTimestampValue(self.alloc, commit_version);
                            var timestamp_value_owned = true;
                            errdefer if (timestamp_value_owned) self.alloc.free(timestamp_value);
                            try relational_extra_owned_keys.append(self.alloc, new_timestamp_key);
                            new_timestamp_key_owned = false;
                            try relational_extra_owned_values.append(self.alloc, timestamp_value);
                            timestamp_value_owned = false;
                            try relational_extra_writes.append(self.alloc, .{ .key = new_timestamp_key, .value = timestamp_value });
                        }
                    }
                    for (mutations) |mutation| {
                        if (isMetadataKey(mutation.key) or internal_keys.isInternalPhysicalTableDataKey(mutation.key)) continue;
                        if (isRelationalIdentityRewriteEndpoint(relational_identity_rewrites, mutation.key)) continue;
                        const skip_key = try self.alloc.dupe(u8, mutation.key);
                        var skip_key_owned = true;
                        errdefer if (skip_key_owned) self.alloc.free(skip_key);
                        try relational_skip_intent_keys.append(self.alloc, skip_key);
                        skip_key_owned = false;

                        if (mutation.value) |value| {
                            const row_value = try self.alloc.dupe(u8, value);
                            var row_value_owned = true;
                            errdefer if (row_value_owned) self.alloc.free(row_value);
                            relational_participant_prepared = true;
                            relational_participant.prepareUpsert(
                                relational_table_name,
                                mutation.key,
                                row_value,
                                txn_id,
                            ) catch |err| {
                                if (err == error.ForeignKeyViolation) recordForeignKeyChildWriteReject(self);
                                return err;
                            };
                            try relational_extra_owned_values.append(self.alloc, row_value);
                            row_value_owned = false;
                            if (shouldWriteTimestamp(mutation.key)) {
                                const timestamp_key = try makeTimestampKey(self.alloc, mutation.key);
                                var timestamp_key_owned = true;
                                errdefer if (timestamp_key_owned) self.alloc.free(timestamp_key);
                                const timestamp_value = try encodeTimestampValue(self.alloc, commit_version);
                                var timestamp_value_owned = true;
                                errdefer if (timestamp_value_owned) self.alloc.free(timestamp_value);
                                try relational_extra_owned_keys.append(self.alloc, timestamp_key);
                                timestamp_key_owned = false;
                                try relational_extra_owned_values.append(self.alloc, timestamp_value);
                                timestamp_value_owned = false;
                                try relational_extra_writes.append(self.alloc, .{ .key = timestamp_key, .value = timestamp_value });
                            }
                        } else {
                            relational_participant_prepared = true;
                            relational_participant.prepareDelete(
                                relational_table_name,
                                mutation.key,
                                txn_id,
                            ) catch |err| {
                                if (err == error.ForeignKeyViolation) recordForeignKeyParentDeleteReject(self);
                                return err;
                            };
                            if (shouldWriteTimestamp(mutation.key)) {
                                const timestamp_key = try makeTimestampKey(self.alloc, mutation.key);
                                var timestamp_key_owned = true;
                                errdefer if (timestamp_key_owned) self.alloc.free(timestamp_key);
                                try relational_extra_owned_keys.append(self.alloc, timestamp_key);
                                timestamp_key_owned = false;
                                try relational_extra_deletes.append(self.alloc, timestamp_key);
                            }
                        }

                        const primary_key = try internal_keys.documentKeyAlloc(self.alloc, mutation.key);
                        var primary_key_owned = true;
                        errdefer if (primary_key_owned) self.alloc.free(primary_key);
                        try relational_extra_owned_keys.append(self.alloc, primary_key);
                        primary_key_owned = false;
                        try relational_extra_deletes.append(self.alloc, primary_key);
                    }
                    relational_participant.commit(txn_id, commit_version) catch |err| {
                        if (err == error.ForeignKeyViolation) recordForeignKeyChildWriteReject(self);
                        return err;
                    };
                    relational_participant_closed = true;
                    if (self.core.schema) |runtime_schema| {
                        if (runtime_schema.system_versioned) {
                            try appendSystemVersionedHistoryForTransactionMutations(
                                self,
                                mutations,
                                relational_identity_rewrites,
                                commit_version,
                                &relational_extra_writes,
                                &relational_extra_owned_keys,
                                &relational_extra_owned_values,
                            );
                        }
                    }
                    relational_extra_owned_keys.clearRetainingCapacity();
                    relational_extra_owned_values.clearRetainingCapacity();
                }
            }

            const pending_identity_visibility_summary = try doc_identity.visibilitySummaryFromWrites(identity_writes.items);
            try identity_writes.appendSlice(self.alloc, relational_extra_writes.items);
            relational_extra_writes.clearRetainingCapacity();
            try self.core.resolveTransactionIntentsWithExtraBatch(txn_id, status, commit_version, .{
                .writes = identity_writes.items,
                .deletes = relational_extra_deletes.items,
                .skip_intent_keys = relational_skip_intent_keys.items,
            });
            if (pending_identity_visibility_summary) |summary| {
                self.identity_visibility_summary_cache = summary;
            }
        }

        pub fn abortTransaction(self: *DB, txn_id: transactions_mod.TxnId, timestamp_ns: u64) !void {
            try self.resolveTransactionIntents(txn_id, .aborted, timestamp_ns);
        }

        pub fn getTransactionStatus(self: *DB, txn_id: transactions_mod.TxnId) !transactions_mod.TxnStatus {
            return try self.core.getTransactionStatus(txn_id);
        }

        pub fn getCommitVersion(self: *DB, txn_id: transactions_mod.TxnId) !u64 {
            return try self.core.getCommitVersion(txn_id);
        }

        pub fn markTransactionParticipantResolved(self: *DB, txn_id: transactions_mod.TxnId, participant: []const u8) !void {
            lockApply(self);
            defer self.core.unlockApply();
            try self.core.markTransactionParticipantResolved(txn_id, participant);
        }

        pub fn getTransactionParticipants(self: *DB, alloc: Allocator, txn_id: transactions_mod.TxnId) ![][]u8 {
            return try self.core.getTransactionParticipants(alloc, txn_id);
        }

        pub fn getUnresolvedTransactionParticipants(self: *DB, alloc: Allocator, txn_id: transactions_mod.TxnId) ![][]u8 {
            return try self.core.getUnresolvedTransactionParticipants(alloc, txn_id);
        }

        pub fn recoverTransactions(self: *DB, cutoff_timestamp: u64, resolution_timestamp: u64) !transactions_mod.RecoveryStats {
            lockApply(self);
            defer self.core.unlockApply();
            return try self.core.recoverTransactions(cutoff_timestamp, resolution_timestamp);
        }

        fn lockApply(self: *DB) void {
            self.core.lockApply();
        }

        fn makeTxnId(self: *DB) transactions_mod.TxnId {
            var txn_id: transactions_mod.TxnId = undefined;
            const now = currentTimeNs();
            const seq = self.core.nextDerivedAppendSequence();
            std.mem.writeInt(u64, txn_id[0..8], now, .little);
            std.mem.writeInt(u64, txn_id[8..16], seq, .little);
            return txn_id;
        }

        fn appendSystemVersionedHistoryForTransactionMutations(
            self: *DB,
            mutations: []const transactions_mod.OwnedIntentMutation,
            rewrites: []const types.RelationalIdentityRewrite,
            commit_version: u64,
            writes: *std.ArrayListUnmanaged(docstore_mod.KVPair),
            owned_keys: *std.ArrayListUnmanaged([]u8),
            owned_values: *std.ArrayListUnmanaged([]u8),
        ) !void {
            return try self.appendSystemVersionedHistoryForTransactionMutations(mutations, rewrites, commit_version, writes, owned_keys, owned_values, isUserRowMutationKey);
        }

        fn failIfIdentityOrdinalExhaustedForNewUpserts(self: *DB, doc_ids: []const []const u8) !void {
            if (doc_ids.len == 0) return;

            var txn = try self.core.store.beginProbeTxn();
            defer txn.abort();

            const raw_next = txn.get(internal_keys.identity_next_ordinal_key[0..]) catch |err| switch (err) {
                error.NotFound => return,
                else => return err,
            };
            if (raw_next.len != @sizeOf(doc_identity.DocOrdinal)) return error.InvalidDocIdentity;
            const next_ordinal = std.mem.readInt(doc_identity.DocOrdinal, raw_next[0..4], .big);
            if (next_ordinal != 0 and next_ordinal < std.math.maxInt(doc_identity.DocOrdinal)) return;

            var seen = std.StringHashMapUnmanaged(void).empty;
            defer seen.deinit(self.alloc);
            for (doc_ids) |doc_id| {
                if (seen.contains(doc_id)) continue;
                try seen.put(self.alloc, doc_id, {});
                if (try doc_identity.lookupOrdinalTxn(self.alloc, &txn, doc_id) != null) continue;
                return error.DocOrdinalExhausted;
            }
        }

        fn relationalColumnsForStore(self: *DB) ?[]const schema_mod.RelationalColumn {
            const schema = self.core.schema orelse return null;
            if (schema.storage_mode != .relational) return null;
            return schema.relational_columns;
        }

        fn relationalColumnIndexPolicyForStore(self: *DB) relational_store_mod.ColumnIndexPolicy {
            const columns = relationalColumnsForStore(self) orelse return relational_store_mod.ColumnIndexPolicy.all();
            return relational_store_mod.ColumnIndexPolicy.fromColumns(columns);
        }

        fn recordForeignKeyChildWriteReject(self: *DB) void {
            self.foreign_key_stats.recordChildWriteReject();
        }

        fn recordForeignKeyParentDeleteReject(self: *DB) void {
            self.foreign_key_stats.recordParentDeleteReject();
        }

        fn foreignKeyActionJobCanonicalAction(action: []const u8) ?[]const u8 {
            return DB.foreignKeyActionJobCanonicalAction(action);
        }

        fn foreignKeyActionScheduleKeyAlloc(alloc: Allocator, schedule_id: []const u8) ![]u8 {
            return try DB.foreignKeyActionScheduleKeyAlloc(alloc, schedule_id);
        }

        fn validateForeignKeyActionLineage(cascade_depth: u32, cascade_max_depth: u32) !void {
            try DB.validateForeignKeyActionLineage(cascade_depth, cascade_max_depth);
        }

        fn validateForeignKeyActionJobIdentity(
            job_id: []const u8,
            action: []const u8,
            worker_id: []const u8,
            constraint_name: []const u8,
            parent_table: []const u8,
            parent_key: []const u8,
            updated_parent_key: ?[]const u8,
        ) !void {
            try DB.validateForeignKeyActionJobIdentity(job_id, action, worker_id, constraint_name, parent_table, parent_key, updated_parent_key);
        }

        fn validateForeignKeyActionScheduleMatches(
            existing: ForeignKeyActionScheduleRecord,
            action_job_id: []const u8,
            action: []const u8,
            constraint_name: []const u8,
            parent_table: []const u8,
            parent_key: []const u8,
            updated_parent_key: ?[]const u8,
        ) !void {
            try DB.validateForeignKeyActionScheduleMatches(existing, action_job_id, action, constraint_name, parent_table, parent_key, updated_parent_key);
        }
    };
}

fn currentTimeNs() u64 {
    return platform_clock.Clock.real().nowRealtimeNs();
}

fn monotonicTimeNs() u64 {
    return platform_time.monotonicNs();
}

const isMetadataKey = db_internal.isMetadataKey;

fn isRowClaimIntentMetadataKey(key: []const u8) bool {
    return std.mem.startsWith(u8, key, row_claim_intent_key_prefix);
}

fn isUserRowMutationKey(key: []const u8) bool {
    return !isMetadataKey(key) and !internal_keys.isInternalPhysicalTableDataKey(key);
}

const rowClaimIntentKeyAlloc = relational_store_mod.rowClaimIntentKeyAlloc;
const rowClaimIntentValueAlloc = relational_store_mod.rowClaimIntentValueAlloc;
const relationalIdentityRewriteIntentKeyAlloc = relational_store_mod.relationalIdentityRewriteIntentKeyAlloc;
const encodeRelationalIdentityRewriteIntentValueAlloc = relational_store_mod.encodeRelationalIdentityRewriteIntentValueAlloc;
const collectTransactionRelationalIdentityRewritesAlloc = relational_store_mod.collectTransactionRelationalIdentityRewritesAlloc;
const freeRelationalIdentityRewrites = relational_store_mod.freeRelationalIdentityRewrites;
const isRelationalIdentityRewriteEndpoint = relational_store_mod.isRelationalIdentityRewriteEndpoint;
const collectTransactionExternalizedForeignKeyParentChecksAlloc = relational_store_mod.collectTransactionExternalizedForeignKeyParentChecksAlloc;
const collectTransactionForeignKeyConstraintTimingOverridesAlloc = relational_store_mod.collectTransactionForeignKeyConstraintTimingOverridesAlloc;
const freeExternalizedForeignKeyParentChecks = relational_store_mod.freeExternalizedForeignKeyParentChecks;
const freeRelationalForeignKeyConstraintTimingOverrides = relational_store_mod.freeRelationalForeignKeyConstraintTimingOverrides;

pub fn reclaimExpiredRowClaimIntentsForRows(
    self: anytype,
    claiming_txn_id: types.TxnId,
    row_keys: []const []const u8,
    now_ns: u64,
) !usize {
    return try self.reclaimExpiredRowClaimIntentsForRows(claiming_txn_id, row_keys, now_ns);
}

pub fn reclaimExpiredRowClaimIntentsForMutationKeys(
    self: anytype,
    writes: anytype,
    deletes: []const []const u8,
    exclude_txn_id: ?types.TxnId,
    now_ns: u64,
    comptime already_locked: bool,
) !usize {
    return try self.reclaimExpiredRowClaimIntentsForMutationKeys(writes, deletes, exclude_txn_id, now_ns, already_locked, isUserRowMutationKey);
}

pub fn reclaimExpiredRowClaimIntentsForIdentityRewrites(
    self: anytype,
    rewrites: []const types.RelationalIdentityRewrite,
    exclude_txn_id: ?types.TxnId,
    now_ns: u64,
    comptime already_locked: bool,
) !usize {
    return try self.reclaimExpiredRowClaimIntentsForIdentityRewrites(rewrites, exclude_txn_id, now_ns, already_locked, isUserRowMutationKey);
}

pub fn appendRowClaimPredicatesForMutationKeys(
    alloc: Allocator,
    predicates: *std.ArrayListUnmanaged(transactions_mod.VersionPredicate),
    owned_keys: *std.ArrayListUnmanaged([]u8),
    writes: anytype,
    deletes: []const []const u8,
) !void {
    return try relational_store_mod.appendRowClaimPredicatesForMutationKeys(alloc, predicates, owned_keys, writes, deletes, isUserRowMutationKey);
}

pub fn appendRowClaimPredicatesForIdentityRewrites(
    alloc: Allocator,
    predicates: *std.ArrayListUnmanaged(transactions_mod.VersionPredicate),
    owned_keys: *std.ArrayListUnmanaged([]u8),
    rewrites: []const types.RelationalIdentityRewrite,
) !void {
    return try relational_store_mod.appendRowClaimPredicatesForIdentityRewrites(alloc, predicates, owned_keys, rewrites, isUserRowMutationKey);
}

pub fn validateRelationalIdentityRewriteRequest(
    rewrites: []const types.RelationalIdentityRewrite,
    writes: []const types.TransactionWrite,
    deletes: []const []const u8,
) !void {
    return try relational_store_mod.validateRelationalIdentityRewriteRequest(rewrites, writes, deletes, isUserRowMutationKey);
}

pub fn appendSystemVersionedHistoryForBatch(
    self: anytype,
    req: types.BatchRequest,
    sequence: u64,
    timestamp_ns: u64,
    writes: *std.ArrayListUnmanaged(docstore_mod.KVPair),
    owned_keys: *std.ArrayListUnmanaged([]u8),
    owned_values: *std.ArrayListUnmanaged([]u8),
) !void {
    return try self.appendSystemVersionedHistoryForBatch(req, sequence, timestamp_ns, writes, owned_keys, owned_values, isUserRowMutationKey);
}

fn shouldWriteTimestamp(key: []const u8) bool {
    return !isMetadataKey(key) and !internal_keys.isInternalPhysicalTableDataKey(key);
}

fn makeTimestampKey(alloc: Allocator, key: []const u8) ![]u8 {
    return try internal_keys.ttlKeyAlloc(alloc, key);
}

fn encodeTimestampValue(alloc: Allocator, timestamp_ns: u64) ![]u8 {
    const buf = try alloc.alloc(u8, 8);
    std.mem.writeInt(u64, buf[0..8], timestamp_ns, .little);
    return buf;
}

test "db transactions local lifecycle exposes committed and deleted documents" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const begin_ts: u64 = 1_700_000_000_000_000_000;
    const commit_ts: u64 = begin_ts + 1;
    const txn_id = try db.beginTransaction(begin_ts);
    try std.testing.expectEqual(transactions_mod.TxnStatus.pending, try db.getTransactionStatus(txn_id));

    try db.writeIntents(txn_id, &.{
        .{ .key = "doc:txn", .value = "{\"title\":\"alpha\"}" },
    }, &.{});

    try db.commitTransaction(txn_id, commit_ts);
    try std.testing.expectEqual(transactions_mod.TxnStatus.committed, try db.getTransactionStatus(txn_id));
    try std.testing.expectEqual(commit_ts, try db.getCommitVersion(txn_id));

    const raw = (try db.get(alloc, "doc:txn")) orelse return error.TestExpectedEqual;
    defer alloc.free(raw);
    try std.testing.expectEqualStrings("{\"title\":\"alpha\"}", raw);
    try std.testing.expectEqual(commit_ts, try db.getTimestamp(alloc, "doc:txn"));

    {
        const stats = try db.diagnosticStats(alloc);
        defer types.freeDBStats(alloc, stats);
        try std.testing.expectEqual(@as(u64, 1), stats.doc_identity.state_rows);
        try std.testing.expectEqual(@as(u64, 1), stats.doc_identity.live_ordinals);
        try std.testing.expectEqual(@as(u64, 0), stats.doc_identity.primary_docs_missing_ordinals);
        try std.testing.expectEqual(@as(u64, 0), stats.doc_identity.primary_docs_missing_identity_state);
    }

    const delete_txn = try db.beginTransaction(commit_ts + 1);
    try db.writeIntents(delete_txn, &.{
        .{ .key = "doc:txn", .value = null },
    }, &.{});
    try db.commitTransaction(delete_txn, commit_ts + 2);
    try std.testing.expect((try db.get(alloc, "doc:txn")) == null);

    {
        const stats = try db.diagnosticStats(alloc);
        defer types.freeDBStats(alloc, stats);
        try std.testing.expectEqual(@as(u64, 1), stats.doc_identity.state_rows);
        try std.testing.expectEqual(@as(u64, 0), stats.doc_identity.live_ordinals);
        try std.testing.expectEqual(@as(u64, 1), stats.doc_identity.tombstone_ordinals);
        try std.testing.expectEqual(@as(u64, 0), stats.doc_identity.primary_docs_missing_ordinals);
        try std.testing.expectEqual(@as(u64, 0), stats.doc_identity.primary_docs_missing_identity_state);
    }
}

test "db transactions relational commit writes relational base rows" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"title":{"type":"text"},"amount":{"type":"numeric"}},"required":["title"],"additionalProperties":false}}}}
    ;
    var parsed_schema = try table_schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try table_schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    const begin_ts: u64 = 1_700_000_000_000_100_000;
    const commit_ts: u64 = begin_ts + 1;
    const txn_id = try db.beginTransaction(begin_ts);
    try db.writeIntents(txn_id, &.{
        .{ .key = "row:txn", .value = "{\"title\":\"txn row\",\"amount\":12.5}" },
    }, &.{});
    try db.commitTransaction(txn_id, commit_ts);

    const raw = (try db.get(alloc, "row:txn")) orelse return error.TestExpectedEqual;
    defer alloc.free(raw);
    try std.testing.expect(std.mem.indexOf(u8, raw, "\"title\":\"txn row\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, raw, "\"amount\":12.5") != null);
    try std.testing.expectEqual(commit_ts, try db.getTimestamp(alloc, "row:txn"));

    const relational_key = try relational_store_mod.rowKeyAlloc(alloc, "row:txn");
    defer alloc.free(relational_key);
    const raw_row = try db.core.store.get(alloc, relational_key);
    defer alloc.free(raw_row);
    try std.testing.expect(mapper.isRelationalRowValue(raw_row));

    const primary_key = try internal_keys.documentKeyAlloc(alloc, "row:txn");
    defer alloc.free(primary_key);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, primary_key));

    const delete_txn = try db.beginTransaction(commit_ts + 1);
    try db.writeIntents(delete_txn, &.{
        .{ .key = "row:txn", .value = null },
    }, &.{});
    try db.commitTransaction(delete_txn, commit_ts + 2);
    try std.testing.expect((try db.get(alloc, "row:txn")) == null);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, relational_key));
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, primary_key));
    try std.testing.expectEqual(@as(u64, 0), try db.getTimestamp(alloc, "row:txn"));
}

test "db transactions durable foreign key action schedules are atomic" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const txn_id = try db.beginTransaction(31_000);
    try db.writeTransaction(txn_id, .{
        .foreign_key_action_schedules = &.{.{
            .schedule_id = "fk-action-schedule:orders:customers:customer:txn",
            .action_job_id = "fk-action:orders:customers:customer:txn",
            .action = "SET NULL",
            .worker_id = "worker:txn",
            .constraint_name = "orders_customer_id_fkey",
            .parent_table = "customers",
            .parent_key = "customer:txn",
            .page_limit = 128,
            .cascade_depth = 2,
            .cascade_max_depth = 8,
        }},
    });

    try std.testing.expect((try db.loadForeignKeyActionScheduleRecord("fk-action-schedule:orders:customers:customer:txn")) == null);
    try db.commitTransaction(txn_id, 31_001);

    const loaded = (try db.loadForeignKeyActionScheduleRecord("fk-action-schedule:orders:customers:customer:txn")) orelse return error.TestExpectedEqual;
    defer db.freeForeignKeyActionScheduleRecord(loaded);
    try std.testing.expectEqualStrings("fk-action:orders:customers:customer:txn", loaded.action_job_id);
    try std.testing.expectEqualStrings("set_null", loaded.action);
    try std.testing.expectEqualStrings("worker:txn", loaded.worker_id);
    try std.testing.expectEqualStrings("orders_customer_id_fkey", loaded.constraint_name);
    try std.testing.expectEqualStrings("customers", loaded.parent_table);
    try std.testing.expectEqualStrings("customer:txn", loaded.parent_key);
    try std.testing.expectEqual(@as(usize, 128), loaded.page_limit);
    try std.testing.expectEqual(@as(u32, 2), loaded.cascade_depth);
    try std.testing.expectEqual(@as(u32, 8), loaded.cascade_max_depth);
    try std.testing.expectEqualStrings("pending", loaded.status);
    try std.testing.expect(!loaded.completed);

    const retry_txn = try db.beginTransaction(31_010);
    try db.writeTransaction(retry_txn, .{
        .foreign_key_action_schedules = &.{.{
            .schedule_id = "fk-action-schedule:orders:customers:customer:txn",
            .action_job_id = "fk-action:orders:customers:customer:txn",
            .action = "set-null",
            .worker_id = "worker:txn",
            .constraint_name = "orders_customer_id_fkey",
            .parent_table = "customers",
            .parent_key = "customer:txn",
            .page_limit = 128,
            .cascade_depth = 2,
            .cascade_max_depth = 8,
        }},
    });
    try db.commitTransaction(retry_txn, 31_011);

    const conflicting_txn = try db.beginTransaction(31_020);
    try std.testing.expectError(error.InvalidForeignKeyActionJob, db.writeTransaction(conflicting_txn, .{
        .foreign_key_action_schedules = &.{.{
            .schedule_id = "fk-action-schedule:orders:customers:customer:txn",
            .action_job_id = "fk-action:orders:customers:customer:other",
            .action = "cascade",
            .worker_id = "worker:txn",
            .constraint_name = "orders_customer_id_fkey",
            .parent_table = "customers",
            .parent_key = "customer:txn",
            .page_limit = 128,
            .cascade_depth = 2,
            .cascade_max_depth = 8,
        }},
    }));

    const generated_job = try db.scheduleForeignKeyActionJobWithUpdatedParentKeyAndCascadeLineageAt(
        "fk-action:generated-lineage",
        "cascade",
        "worker:txn",
        "orders_customer_id_fkey",
        "customers",
        "customer:lineage",
        null,
        128,
        3,
        9,
        31_030,
    );
    defer db.freeForeignKeyActionJobRecord(generated_job);
    try std.testing.expectEqual(@as(u32, 3), generated_job.cascade_depth);
    try std.testing.expectEqual(@as(u32, 9), generated_job.cascade_max_depth);

    const generated_schedule = try db.scheduleForeignKeyActionScheduleAt(
        "fk-action-schedule:generated-lineage",
        "fk-action:generated-lineage",
        "cascade",
        "worker:txn",
        "orders_customer_id_fkey",
        "customers",
        "customer:lineage",
        128,
        31_031,
    );
    defer db.freeForeignKeyActionScheduleRecord(generated_schedule);
    try std.testing.expectEqual(@as(u32, 0), generated_schedule.cascade_depth);
    try std.testing.expectEqual(@as(u32, 64), generated_schedule.cascade_max_depth);
}

test "db transactions unique constraint mutations enforce owner handoff" {
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

    const ada_value = try relational_store_mod.bytesTupleValueAlloc(alloc, &.{"ada@example.com"});
    defer alloc.free(ada_value);

    const owner_write_txn = try db.beginTransaction(41_000);
    try db.writeTransaction(owner_write_txn, .{
        .unique_constraint_writes = &.{.{
            .constraint_name = "users_email_key",
            .encoded_value = ada_value,
            .owner_key = "user:1",
        }},
    });
    try db.commitTransaction(owner_write_txn, 41_001);

    const unique_key = try internal_keys.relationalUniqueKeyAlloc(alloc, "users_email_key", ada_value);
    defer alloc.free(unique_key);
    const owner = try db.core.store.get(alloc, unique_key);
    defer alloc.free(owner);
    try std.testing.expectEqualStrings("user:1", owner);

    const conflicting_owner_txn = try db.beginTransaction(41_100);
    try std.testing.expectError(error.UniqueConstraintViolation, db.writeTransaction(conflicting_owner_txn, .{
        .unique_constraint_writes = &.{.{
            .constraint_name = "users_email_key",
            .encoded_value = ada_value,
            .owner_key = "user:2",
        }},
    }));
    try db.abortTransaction(conflicting_owner_txn, 41_101);

    const wrong_delete_txn = try db.beginTransaction(41_200);
    try std.testing.expectError(error.UniqueConstraintViolation, db.writeTransaction(wrong_delete_txn, .{
        .unique_constraint_deletes = &.{.{
            .constraint_name = "users_email_key",
            .encoded_value = ada_value,
            .owner_key = "user:2",
        }},
    }));
    try db.abortTransaction(wrong_delete_txn, 41_201);

    const handoff_txn = try db.beginTransaction(41_300);
    try db.writeTransaction(handoff_txn, .{
        .unique_constraint_deletes = &.{.{
            .constraint_name = "users_email_key",
            .encoded_value = ada_value,
            .owner_key = "user:1",
        }},
        .unique_constraint_writes = &.{.{
            .constraint_name = "users_email_key",
            .encoded_value = ada_value,
            .owner_key = "user:2",
        }},
    });
    try db.commitTransaction(handoff_txn, 41_301);

    const handed_off_owner = try db.core.store.get(alloc, unique_key);
    defer alloc.free(handed_off_owner);
    try std.testing.expectEqualStrings("user:2", handed_off_owner);

    const delete_txn = try db.beginTransaction(41_400);
    try db.writeTransaction(delete_txn, .{
        .unique_constraint_deletes = &.{.{
            .constraint_name = "users_email_key",
            .encoded_value = ada_value,
            .owner_key = "user:2",
        }},
    });
    try db.commitTransaction(delete_txn, 41_401);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, unique_key));
}

test "db transactions doc identity intent writes reject new documents at ordinal exhaustion" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .primary_backend = .{ .mem = .{} },
    });
    defer db.close();

    try db.batch(.{
        .writes = &.{.{ .key = "doc:existing_txn", .value = "{\"name\":\"existing\"}" }},
        .sync_level = .write,
    });

    var value: [4]u8 = undefined;
    std.mem.writeInt(u32, &value, std.math.maxInt(doc_identity.DocOrdinal), .big);
    try db.core.store.put(internal_keys.identity_next_ordinal_key[0..], &value);

    const existing_txn = try db.beginTransaction(11_000);
    try db.writeIntents(existing_txn, &.{
        .{ .key = "doc:existing_txn", .value = "{\"name\":\"updated\"}" },
    }, &.{});
    try db.commitTransaction(existing_txn, 11_001);

    const existing = (try db.get(alloc, "doc:existing_txn")) orelse return error.TestExpectedEqual;
    defer alloc.free(existing);
    try std.testing.expectEqualStrings("{\"name\":\"updated\"}", existing);

    const direct_txn = try db.beginTransaction(12_000);
    try std.testing.expectError(error.DocOrdinalExhausted, db.writeIntents(direct_txn, &.{
        .{ .key = "doc:new_direct_txn", .value = "{\"name\":\"new\"}" },
    }, &.{}));
    try db.abortTransaction(direct_txn, 12_001);
    try std.testing.expect((try db.get(alloc, "doc:new_direct_txn")) == null);

    const request_txn = try db.beginTransaction(13_000);
    try std.testing.expectError(error.DocOrdinalExhausted, db.writeTransaction(request_txn, .{
        .writes = &.{.{ .key = "doc:new_request_txn", .value = "{\"name\":\"new\"}" }},
    }));
    try db.abortTransaction(request_txn, 13_001);
    try std.testing.expect((try db.get(alloc, "doc:new_request_txn")) == null);
}

test "db transactions created identity rows remain visible after reopen" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        const txn_id = try db.beginTransaction(21_000);
        try db.writeTransaction(txn_id, .{
            .writes = &.{.{ .key = "doc:txn_reopen", .value = "{\"title\":\"txn\"}" }},
        });
        try db.commitTransaction(txn_id, 21_001);
    }

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        const raw = (try db.get(alloc, "doc:txn_reopen")) orelse return error.TestExpectedEqual;
        defer alloc.free(raw);
        try std.testing.expectEqualStrings("{\"title\":\"txn\"}", raw);

        const current = try db.searchRequestAtCurrentIdentityGeneration(.{});
        var resolved = try db.internalResolveDocSetForIdsNoLockAtGenerationAlloc(alloc, &.{"doc:txn_reopen"}, current.identity_read_generation);
        defer resolved.deinit(alloc);
        try std.testing.expect(resolved.containsOrdinal(1));

        const stats = try db.diagnosticStats(alloc);
        defer types.freeDBStats(alloc, stats);
        try std.testing.expectEqual(@as(u64, 1), stats.doc_identity.state_rows);
        try std.testing.expectEqual(@as(u64, 1), stats.doc_identity.live_ordinals);
        try std.testing.expectEqual(@as(u64, 0), stats.doc_identity.primary_docs_missing_ordinals);
        try std.testing.expectEqual(@as(u64, 0), stats.doc_identity.primary_docs_missing_identity_state);
    }
}

test "db transactions abort leaves no visible document" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const txn_id = try db.beginTransaction(1_700_000_000_000_000_000);
    try db.writeIntents(txn_id, &.{
        .{ .key = "doc:txn_abort", .value = "{\"title\":\"alpha\"}" },
    }, &.{});
    try db.abortTransaction(txn_id, 1_700_000_000_000_000_001);

    try std.testing.expectEqual(transactions_mod.TxnStatus.aborted, try db.getTransactionStatus(txn_id));
    try std.testing.expect((try db.get(alloc, "doc:txn_abort")) == null);
}

test "db transactions resolve transforms against pending same-transaction writes" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const txn_id = try db.beginTransaction(9_000);
    try db.writeTransaction(txn_id, .{
        .writes = &.{
            .{ .key = "doc:txn_transform", .value = "{\"count\":1}" },
        },
        .transforms = &.{
            .{
                .key = "doc:txn_transform",
                .operations = &.{
                    .{ .op = .inc, .path = "count", .value_json = "4" },
                },
            },
        },
    });
    try db.commitTransaction(txn_id, 9_001);

    const raw = (try db.get(alloc, "doc:txn_transform")) orelse return error.TestExpectedEqual;
    defer alloc.free(raw);

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{});
    defer parsed.deinit();
    const count_value = parsed.value.object.get("count").?;
    switch (count_value) {
        .integer => |value| try std.testing.expectEqual(@as(i64, 5), value),
        .float => |value| try std.testing.expectEqual(@as(f64, 5), value),
        else => return error.TestExpectedEqual,
    }
}

test "db transactions relational transforms read base rows and honor abort" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"title":{"type":"text"},"count":{"type":"numeric"}},"required":["title","count"],"additionalProperties":false}}}}
    ;
    var parsed_schema = try table_schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try table_schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    try db.batch(.{
        .writes = &.{.{ .key = "row:txn_transform", .value = "{\"title\":\"base row\",\"count\":1}" }},
    });

    const primary_key = try internal_keys.documentKeyAlloc(alloc, "row:txn_transform");
    defer alloc.free(primary_key);
    try db.core.store.put(primary_key, "{\"title\":\"stale primary\",\"count\":999}");

    const txn_id = try db.beginTransaction(11_000);
    try db.writeTransaction(txn_id, .{
        .transforms = &.{.{
            .key = "row:txn_transform",
            .operations = &.{.{ .op = .inc, .path = "count", .value_json = "4" }},
        }},
    });
    try db.commitTransaction(txn_id, 11_001);

    const raw = (try db.get(alloc, "row:txn_transform")) orelse return error.TestExpectedEqual;
    defer alloc.free(raw);
    try std.testing.expect(std.mem.indexOf(u8, raw, "\"title\":\"base row\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, raw, "\"count\":5") != null);
    try std.testing.expect(std.mem.indexOf(u8, raw, "stale primary") == null);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, primary_key));

    const abort_txn = try db.beginTransaction(12_000);
    try db.writeTransaction(abort_txn, .{
        .transforms = &.{.{
            .key = "row:txn_transform",
            .operations = &.{.{ .op = .inc, .path = "count", .value_json = "100" }},
        }},
    });
    try db.abortTransaction(abort_txn, 12_001);

    const after_abort = (try db.get(alloc, "row:txn_transform")) orelse return error.TestExpectedEqual;
    defer alloc.free(after_abort);
    try std.testing.expect(std.mem.indexOf(u8, after_abort, "\"count\":5") != null);
}

test "db transactions write request enforces version predicates" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.batch(.{
        .writes = &.{.{ .key = "doc:pred", .value = "{\"title\":\"v1\"}" }},
        .timestamp_ns = 5_000,
    });

    const txn_id = try db.beginTransaction(6_000);
    try std.testing.expectError(transactions_mod.TxnError.VersionConflict, db.writeTransaction(txn_id, .{
        .writes = &.{.{ .key = "doc:pred", .value = "{\"title\":\"v2\"}" }},
        .predicates = &.{.{ .key = "doc:pred", .expected_version = 0 }},
    }));

    try db.writeTransaction(txn_id, .{
        .writes = &.{.{ .key = "doc:pred", .value = "{\"title\":\"v2\"}" }},
        .predicates = &.{.{ .key = "doc:pred", .expected_version = 5_000 }},
    });
    try db.commitTransaction(txn_id, 6_001);

    const raw = (try db.get(alloc, "doc:pred")) orelse return error.TestExpectedEqual;
    defer alloc.free(raw);
    try std.testing.expectEqualStrings("{\"title\":\"v2\"}", raw);
}

test "db transactions write request detects concurrent intent conflicts" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const txn1 = try db.beginTransaction(1_000);
    const txn2 = try db.beginTransaction(1_001);

    try db.writeTransaction(txn1, .{
        .writes = &.{.{ .key = "doc:shared", .value = "{\"title\":\"from1\"}" }},
    });

    try std.testing.expectError(transactions_mod.TxnError.IntentConflict, db.writeTransaction(txn2, .{
        .writes = &.{.{ .key = "doc:shared", .value = "{\"title\":\"from2\"}" }},
    }));
}

test "db transactions row claims block transactional and direct mutations until resolution" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.batch(.{
        .writes = &.{.{ .key = "doc:claim", .value = "{\"title\":\"base\"}" }},
        .timestamp_ns = 1_000,
    });

    const claim_txn = try db.beginTransaction(2_000);
    try std.testing.expectError(error.InvalidQueryRequest, db.claimRowsForTransaction(claim_txn, &.{"doc:claim"}, .{
        .mode = .for_update,
        .txn_id = claim_txn,
    }));
    try std.testing.expectError(error.InvalidQueryRequest, db.claimRowsForTransaction(claim_txn, &.{"doc:claim"}, .{
        .mode = .for_update,
        .owner_id = "session:claim",
        .lease_ms = 0,
        .txn_id = claim_txn,
    }));
    try db.claimRowsForTransaction(claim_txn, &.{"doc:claim"}, .{
        .mode = .for_update,
        .owner_id = "session:claim",
        .txn_id = claim_txn,
    });

    const writer_txn = try db.beginTransaction(2_001);
    try std.testing.expectError(transactions_mod.TxnError.IntentConflict, db.writeTransaction(writer_txn, .{
        .writes = &.{.{ .key = "doc:claim", .value = "{\"title\":\"blocked\"}" }},
    }));
    try std.testing.expectError(transactions_mod.TxnError.IntentConflict, db.batch(.{
        .writes = &.{.{ .key = "doc:claim", .value = "{\"title\":\"direct\"}" }},
        .timestamp_ns = 2_002,
    }));

    try db.commitTransaction(claim_txn, 2_010);
    try db.writeTransaction(writer_txn, .{
        .writes = &.{.{ .key = "doc:claim", .value = "{\"title\":\"updated\"}" }},
    });
    try db.commitTransaction(writer_txn, 2_011);

    const raw = (try db.get(alloc, "doc:claim")) orelse return error.TestExpectedEqual;
    defer alloc.free(raw);
    try std.testing.expectEqualStrings("{\"title\":\"updated\"}", raw);
}

test "db transactions row claims prevent double claim until resolution" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.batch(.{
        .writes = &.{.{ .key = "doc:claim", .value = "{\"title\":\"base\"}" }},
        .timestamp_ns = 1_000,
    });

    const first_txn = try db.beginTransaction(2_000);
    try db.claimRowsForTransaction(first_txn, &.{"doc:claim"}, .{
        .mode = .for_update,
        .owner_id = "session:first",
        .txn_id = first_txn,
    });

    const second_txn = try db.beginTransaction(2_001);
    try std.testing.expectError(transactions_mod.TxnError.IntentConflict, db.claimRowsForTransaction(second_txn, &.{"doc:claim"}, .{
        .mode = .for_update,
        .owner_id = "session:second",
        .txn_id = second_txn,
    }));

    try db.abortTransaction(first_txn, 2_010);
    try db.claimRowsForTransaction(second_txn, &.{"doc:claim"}, .{
        .mode = .for_update,
        .owner_id = "session:second",
        .txn_id = second_txn,
    });

    const blocked_txn = try db.beginTransaction(2_011);
    try std.testing.expectError(transactions_mod.TxnError.IntentConflict, db.writeTransaction(blocked_txn, .{
        .writes = &.{.{ .key = "doc:claim", .value = "{\"title\":\"blocked\"}" }},
    }));

    try db.commitTransaction(second_txn, 2_020);
    try db.writeTransaction(blocked_txn, .{
        .writes = &.{.{ .key = "doc:claim", .value = "{\"title\":\"updated\"}" }},
    });
    try db.commitTransaction(blocked_txn, 2_021);

    const raw = (try db.get(alloc, "doc:claim")) orelse return error.TestExpectedEqual;
    defer alloc.free(raw);
    try std.testing.expectEqualStrings("{\"title\":\"updated\"}", raw);
}

test "db transactions row claim lease expiry aborts stale owner and lets next claimer proceed" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.batch(.{
        .writes = &.{.{ .key = "doc:claim", .value = "{\"title\":\"base\"}" }},
        .timestamp_ns = 1_000,
    });

    const stale_txn = try db.beginTransaction(2_000);
    try db.claimRowsForTransaction(stale_txn, &.{"doc:claim"}, .{
        .mode = .for_update,
        .owner_id = "session:stale",
        .lease_ms = 1,
        .txn_id = stale_txn,
    });
    platform.time.sleepMs(10);

    const next_txn = try db.beginTransaction(2_001);
    try db.claimRowsForTransaction(next_txn, &.{"doc:claim"}, .{
        .mode = .for_update,
        .owner_id = "session:next",
        .txn_id = next_txn,
    });

    try std.testing.expectEqual(transactions_mod.TxnStatus.aborted, try db.getTransactionStatus(stale_txn));
    try std.testing.expectError(transactions_mod.TxnError.DecisionConflict, db.writeTransaction(stale_txn, .{
        .writes = &.{.{ .key = "doc:claim", .value = "{\"title\":\"stale\"}" }},
    }));

    const blocked_txn = try db.beginTransaction(2_002);
    try std.testing.expectError(transactions_mod.TxnError.IntentConflict, db.writeTransaction(blocked_txn, .{
        .writes = &.{.{ .key = "doc:claim", .value = "{\"title\":\"blocked\"}" }},
    }));

    try db.commitTransaction(next_txn, 2_010);
    try db.writeTransaction(blocked_txn, .{
        .writes = &.{.{ .key = "doc:claim", .value = "{\"title\":\"updated\"}" }},
    });
    try db.commitTransaction(blocked_txn, 2_011);

    const raw = (try db.get(alloc, "doc:claim")) orelse return error.TestExpectedEqual;
    defer alloc.free(raw);
    try std.testing.expectEqualStrings("{\"title\":\"updated\"}", raw);
}

test "db transactions row claim survives reopen and expires without mixed state" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    const live_txn = blk: {
        var setup_db = try DB.open(alloc, std.mem.span(path), .{});
        defer setup_db.close();

        try setup_db.batch(.{
            .writes = &.{.{ .key = "doc:claim_reopen", .value = "{\"title\":\"base\"}" }},
            .timestamp_ns = 1_000,
        });

        const txn_id = try setup_db.beginTransaction(2_000);
        try setup_db.claimRowsForTransaction(txn_id, &.{"doc:claim_reopen"}, .{
            .mode = .for_update,
            .owner_id = "session:reopen-live",
            .lease_ms = 60_000,
            .txn_id = txn_id,
        });
        break :blk txn_id;
    };

    {
        var reopened_db = try DB.open(alloc, std.mem.span(path), .{});
        defer reopened_db.close();

        try std.testing.expectEqual(transactions_mod.TxnStatus.pending, try reopened_db.getTransactionStatus(live_txn));

        const competing_txn = try reopened_db.beginTransaction(2_001);
        try std.testing.expectError(transactions_mod.TxnError.IntentConflict, reopened_db.claimRowsForTransaction(competing_txn, &.{"doc:claim_reopen"}, .{
            .mode = .for_update,
            .owner_id = "session:competing",
            .txn_id = competing_txn,
        }));
        try std.testing.expectError(transactions_mod.TxnError.IntentConflict, reopened_db.batch(.{
            .writes = &.{.{ .key = "doc:claim_reopen", .value = "{\"title\":\"direct-blocked\"}" }},
            .timestamp_ns = 2_002,
        }));

        const raw = (try reopened_db.get(alloc, "doc:claim_reopen")) orelse return error.TestExpectedEqual;
        defer alloc.free(raw);
        try std.testing.expectEqualStrings("{\"title\":\"base\"}", raw);

        try reopened_db.abortTransaction(live_txn, 2_010);
    }

    const stale_txn = blk: {
        var setup_db = try DB.open(alloc, std.mem.span(path), .{});
        defer setup_db.close();

        const txn_id = try setup_db.beginTransaction(2_020);
        try setup_db.claimRowsForTransaction(txn_id, &.{"doc:claim_reopen"}, .{
            .mode = .for_update,
            .owner_id = "session:reopen-stale",
            .lease_ms = 1,
            .txn_id = txn_id,
        });
        break :blk txn_id;
    };

    platform.time.sleepMs(10);

    {
        var reopened_db = try DB.open(alloc, std.mem.span(path), .{});
        defer reopened_db.close();

        const next_txn = try reopened_db.beginTransaction(3_000);
        try reopened_db.claimRowsForTransaction(next_txn, &.{"doc:claim_reopen"}, .{
            .mode = .for_update,
            .owner_id = "session:reopen-next",
            .txn_id = next_txn,
        });

        try std.testing.expectEqual(transactions_mod.TxnStatus.aborted, try reopened_db.getTransactionStatus(stale_txn));
        try std.testing.expectError(transactions_mod.TxnError.DecisionConflict, reopened_db.writeTransaction(stale_txn, .{
            .writes = &.{.{ .key = "doc:claim_reopen", .value = "{\"title\":\"stale\"}" }},
        }));

        const blocked_txn = try reopened_db.beginTransaction(3_001);
        try std.testing.expectError(transactions_mod.TxnError.IntentConflict, reopened_db.writeTransaction(blocked_txn, .{
            .writes = &.{.{ .key = "doc:claim_reopen", .value = "{\"title\":\"blocked\"}" }},
        }));

        try reopened_db.commitTransaction(next_txn, 3_010);
        try reopened_db.writeTransaction(blocked_txn, .{
            .writes = &.{.{ .key = "doc:claim_reopen", .value = "{\"title\":\"updated\"}" }},
        });
        try reopened_db.commitTransaction(blocked_txn, 3_011);
    }

    var final_db = try DB.open(alloc, std.mem.span(path), .{});
    defer final_db.close();

    const raw = (try final_db.get(alloc, "doc:claim_reopen")) orelse return error.TestExpectedEqual;
    defer alloc.free(raw);
    try std.testing.expectEqualStrings("{\"title\":\"updated\"}", raw);
}

test "db transactions row claim lease expiry lets direct mutation reclaim stale owner" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.batch(.{
        .writes = &.{.{ .key = "doc:claim", .value = "{\"title\":\"base\"}" }},
        .timestamp_ns = 1_000,
    });

    const stale_txn = try db.beginTransaction(2_000);
    try db.claimRowsForTransaction(stale_txn, &.{"doc:claim"}, .{
        .mode = .for_update,
        .owner_id = "session:stale",
        .lease_ms = 1,
        .txn_id = stale_txn,
    });
    platform.time.sleepMs(10);

    try db.batch(.{
        .writes = &.{.{ .key = "doc:claim", .value = "{\"title\":\"direct\"}" }},
        .timestamp_ns = 2_010,
    });

    try std.testing.expectEqual(transactions_mod.TxnStatus.aborted, try db.getTransactionStatus(stale_txn));
    try std.testing.expectError(transactions_mod.TxnError.DecisionConflict, db.writeTransaction(stale_txn, .{
        .writes = &.{.{ .key = "doc:claim", .value = "{\"title\":\"stale\"}" }},
    }));

    const raw = (try db.get(alloc, "doc:claim")) orelse return error.TestExpectedEqual;
    defer alloc.free(raw);
    try std.testing.expectEqualStrings("{\"title\":\"direct\"}", raw);
}

test "db transactions row claim search skip locked returns claimed subset" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:locked", .value = "{\"title\":\"locked\"}" },
            .{ .key = "doc:free", .value = "{\"title\":\"free\"}" },
        },
        .timestamp_ns = 1_000,
    });

    const locker_txn = try db.beginTransaction(2_000);
    try db.claimRowsForTransaction(locker_txn, &.{"doc:locked"}, .{
        .mode = .for_update,
        .owner_id = "session:locker",
        .txn_id = locker_txn,
    });

    const search_txn = try db.beginTransaction(2_001);
    var claimed = try db.search(alloc, .{
        .row_claim = .{
            .mode = .for_update,
            .skip_locked = true,
            .owner_id = "session:search",
            .txn_id = search_txn,
        },
        .limit = 10,
    });
    defer claimed.deinit();

    try std.testing.expectEqual(@as(u32, 1), claimed.total_hits);
    try std.testing.expectEqualStrings("doc:free", claimed.hits[0].id);

    const blocked_txn = try db.beginTransaction(2_002);
    try std.testing.expectError(transactions_mod.TxnError.IntentConflict, db.writeTransaction(blocked_txn, .{
        .writes = &.{.{ .key = "doc:free", .value = "{\"title\":\"blocked\"}" }},
    }));
}

test "db transactions committed transaction exposes commit timestamp for later predicates" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.batch(.{
        .writes = &.{.{ .key = "doc:txn_pred", .value = "{\"title\":\"base\"}" }},
        .timestamp_ns = 5_000,
    });

    const txn_id = try db.beginTransaction(6_000);
    try db.writeTransaction(txn_id, .{
        .writes = &.{.{ .key = "doc:txn_pred", .value = "{\"title\":\"txn\"}" }},
        .predicates = &.{.{ .key = "doc:txn_pred", .expected_version = 5_000 }},
    });
    try db.commitTransaction(txn_id, 7_000);

    try std.testing.expectEqual(@as(u64, 7_000), try db.getTimestamp(alloc, "doc:txn_pred"));

    try std.testing.expectError(transactions_mod.TxnError.VersionConflict, db.batch(.{
        .writes = &.{.{ .key = "doc:txn_pred", .value = "{\"title\":\"stale\"}" }},
        .predicates = &.{.{ .key = "doc:txn_pred", .expected_version = 6_000 }},
        .timestamp_ns = 8_000,
    }));

    try db.batch(.{
        .writes = &.{.{ .key = "doc:txn_pred", .value = "{\"title\":\"fresh\"}" }},
        .predicates = &.{.{ .key = "doc:txn_pred", .expected_version = 7_000 }},
        .timestamp_ns = 8_001,
    });

    const raw = (try db.get(alloc, "doc:txn_pred")) orelse return error.TestExpectedEqual;
    defer alloc.free(raw);
    try std.testing.expectEqualStrings("{\"title\":\"fresh\"}", raw);
    try std.testing.expectEqual(@as(u64, 8_001), try db.getTimestamp(alloc, "doc:txn_pred"));
}

test "db transactions aborted transaction preserves prior committed state and version" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.batch(.{
        .writes = &.{.{ .key = "doc:txn_abort_preserve", .value = "{\"title\":\"base\"}" }},
        .timestamp_ns = 9_000,
    });

    const txn_id = try db.beginTransaction(9_100);
    try db.writeTransaction(txn_id, .{
        .writes = &.{.{ .key = "doc:txn_abort_preserve", .value = "{\"title\":\"txn\"}" }},
        .predicates = &.{.{ .key = "doc:txn_abort_preserve", .expected_version = 9_000 }},
    });
    try db.abortTransaction(txn_id, 9_200);

    const raw = (try db.get(alloc, "doc:txn_abort_preserve")) orelse return error.TestExpectedEqual;
    defer alloc.free(raw);
    try std.testing.expectEqualStrings("{\"title\":\"base\"}", raw);
    try std.testing.expectEqual(@as(u64, 9_000), try db.getTimestamp(alloc, "doc:txn_abort_preserve"));

    try db.batch(.{
        .writes = &.{.{ .key = "doc:txn_abort_preserve", .value = "{\"title\":\"next\"}" }},
        .predicates = &.{.{ .key = "doc:txn_abort_preserve", .expected_version = 9_000 }},
        .timestamp_ns = 9_300,
    });

    const updated = (try db.get(alloc, "doc:txn_abort_preserve")) orelse return error.TestExpectedEqual;
    defer alloc.free(updated);
    try std.testing.expectEqualStrings("{\"title\":\"next\"}", updated);
    try std.testing.expectEqual(@as(u64, 9_300), try db.getTimestamp(alloc, "doc:txn_abort_preserve"));
}

test "db transactions explicit resolveTransactionIntents applies participant-style commit version" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const txn_id = try db.beginTransaction(12_000);
    try db.writeTransaction(txn_id, .{
        .writes = &.{.{ .key = "doc:participant", .value = "{\"title\":\"replicated\"}" }},
    });

    try db.resolveTransactionIntents(txn_id, .committed, 15_000);

    try std.testing.expectEqual(transactions_mod.TxnStatus.committed, try db.getTransactionStatus(txn_id));
    try std.testing.expectEqual(@as(u64, 15_000), try db.getCommitVersion(txn_id));
    try std.testing.expectEqual(@as(u64, 15_000), try db.getTimestamp(alloc, "doc:participant"));

    const raw = (try db.get(alloc, "doc:participant")) orelse return error.TestExpectedEqual;
    defer alloc.free(raw);
    try std.testing.expectEqualStrings("{\"title\":\"replicated\"}", raw);
}

test "db transactions recoverTransactions auto-aborts stale pending intents" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const txn_id = try db.beginTransaction(1_000);
    try db.writeTransaction(txn_id, .{
        .writes = &.{.{ .key = "doc:recover_pending", .value = "{\"title\":\"pending\"}" }},
    });

    const stats = try db.recoverTransactions(2_000, 3_000);
    try std.testing.expectEqual(@as(u64, 1), stats.auto_aborted);
    try std.testing.expectEqual(transactions_mod.TxnStatus.aborted, try db.getTransactionStatus(txn_id));

    const raw = try db.get(alloc, "doc:recover_pending");
    defer if (raw) |bytes| alloc.free(bytes);
    try std.testing.expect(raw == null);
}

const test_txn_records_prefix = "\x00\x00__txn_records__:";

fn committedTxnRecordKey(txn_id: transactions_mod.TxnId) [test_txn_records_prefix.len + 16]u8 {
    var key: [test_txn_records_prefix.len + 16]u8 = undefined;
    @memcpy(key[0..test_txn_records_prefix.len], test_txn_records_prefix);
    @memcpy(key[test_txn_records_prefix.len..], &txn_id);
    return key;
}

fn committedTxnRecordValueAlloc(alloc: std.mem.Allocator, begin_timestamp: u64, commit_version: u64) ![]u8 {
    const txn_record_v1_size = 33;
    const value = try alloc.alloc(u8, txn_record_v1_size);
    value[0] = @intFromEnum(transactions_mod.TxnStatus.committed);
    std.mem.writeInt(u64, value[1..9], begin_timestamp, .little);
    std.mem.writeInt(u64, value[9..17], commit_version, .little);
    std.mem.writeInt(u64, value[17..25], begin_timestamp, .little);
    std.mem.writeInt(u64, value[25..33], commit_version, .little);
    return value;
}

test "db transactions participant recovery preserves finalized transaction until all participants resolve" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const txn_id = try db.beginTransactionWithParticipants(1_000, &.{ "local", "remote" });
    try db.writeTransaction(txn_id, .{
        .writes = &.{.{ .key = "doc:participant_hold", .value = "{\"title\":\"value\"}" }},
    });
    try db.resolveTransactionIntents(txn_id, .committed, 2_000);
    try db.markTransactionParticipantResolved(txn_id, "local");

    const unresolved_initial = try db.getUnresolvedTransactionParticipants(alloc, txn_id);
    defer transactions_mod.freeParticipantList(alloc, unresolved_initial);
    try std.testing.expectEqual(@as(usize, 1), unresolved_initial.len);
    try std.testing.expectEqualStrings("remote", unresolved_initial[0]);

    const stats = try db.recoverTransactions(3_000, 4_000);
    try std.testing.expectEqual(@as(u64, 1), stats.deferred_unresolved);
    try std.testing.expectEqual(transactions_mod.TxnStatus.committed, try db.getTransactionStatus(txn_id));

    try db.markTransactionParticipantResolved(txn_id, "remote");
    const unresolved_final = try db.getUnresolvedTransactionParticipants(alloc, txn_id);
    defer transactions_mod.freeParticipantList(alloc, unresolved_final);
    try std.testing.expectEqual(@as(usize, 0), unresolved_final.len);

    const cleaned = try db.recoverTransactions(3_000, 4_000);
    try std.testing.expectEqual(@as(u64, 1), cleaned.cleaned_records);
    try std.testing.expectError(transactions_mod.TxnError.TxnNotFound, db.getTransactionStatus(txn_id));
}

test "db transactions recovery resolves committed mutation source row claims after reopen" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed_schema = try schema_api_mod.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try schema_api_mod.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);

    const txn_id: transactions_mod.TxnId = .{ 0xc0, 0xc1, 0xc2, 0xc3, 0xc4, 0xc5, 0xc6, 0xc7, 0xc8, 0xc9, 0xca, 0xcb, 0xcc, 0xcd, 0xce, 0xcf };

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();
        try db.setSchema(runtime_schema);

        try db.batch(.{
            .writes = &.{.{ .key = "row:a", .value = "{\"id\":\"a\",\"status\":\"ready\"}" }},
            .timestamp_ns = 1_000,
        });

        _ = try db.beginTransactionWithId(txn_id, 2_000);
        const predicates = [_]schema_mod.RelationalCheck{.{
            .name = "",
            .field = "status",
            .op = .eq,
            .value_json = "\"ready\"",
        }};
        const operations = [_]types.TransformOp{.{
            .op = .set,
            .path = "status",
            .value_json = "\"done\"",
        }};
        var staged = try db.mutateRelationalRowsFromSource(alloc, runtime_schema, .{
            .kind = .update,
            .source = .{
                .predicates = predicates[0..],
                .row_claim = .{
                    .mode = .for_update,
                    .owner_id = "session:committed-orphan",
                    .lease_ms = 60_000,
                    .txn_id = txn_id,
                },
            },
            .operations = operations[0..],
        });
        defer staged.deinit(alloc);
        try std.testing.expectEqual(@as(u32, 1), staged.matched);
        try std.testing.expectEqual(@as(u32, 1), staged.staged);

        const record_key = committedTxnRecordKey(txn_id);
        const record_value = try committedTxnRecordValueAlloc(alloc, 2_000, 2_100);
        defer alloc.free(record_value);
        try db.core.putStoreBatch(&.{.{ .key = &record_key, .value = record_value }}, &.{});
    }

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();
        try db.setSchema(runtime_schema);

        try std.testing.expectEqual(transactions_mod.TxnStatus.committed, try db.getTransactionStatus(txn_id));
        try std.testing.expectError(transactions_mod.TxnError.IntentConflict, db.batch(.{
            .writes = &.{.{ .key = "row:a", .value = "{\"id\":\"a\",\"status\":\"lost\"}" }},
            .timestamp_ns = 2_200,
        }));

        const contender_txn = try db.beginTransaction(2_300);
        try std.testing.expectError(transactions_mod.TxnError.IntentConflict, db.claimRowsForTransaction(contender_txn, &.{"row:a"}, .{
            .mode = .for_update,
            .owner_id = "session:contender",
            .lease_ms = 60_000,
            .txn_id = contender_txn,
        }));
        try db.abortTransaction(contender_txn, 3_100);

        const stats = try db.recoverTransactions(3_000, 4_000);
        try std.testing.expectEqual(@as(u64, 1), stats.resolved_finalized);
        try std.testing.expectEqual(@as(u64, 1), stats.cleaned_records);
        try std.testing.expectError(transactions_mod.TxnError.TxnNotFound, db.getTransactionStatus(txn_id));

        var final_row = (try db.lookup(alloc, "row:a", .{})) orelse return error.TestUnexpectedResult;
        defer final_row.deinit(alloc);
        try std.testing.expect(std.mem.indexOf(u8, final_row.json, "\"status\":\"done\"") != null);
        try std.testing.expect(std.mem.indexOf(u8, final_row.json, "\"status\":\"lost\"") == null);

        const post_recovery_txn = try db.beginTransaction(4_100);
        try db.claimRowsForTransaction(post_recovery_txn, &.{"row:a"}, .{
            .mode = .for_update,
            .owner_id = "session:post-recovery",
            .lease_ms = 60_000,
            .txn_id = post_recovery_txn,
        });
        try db.abortTransaction(post_recovery_txn, 4_101);

        const again = try db.recoverTransactions(3_000, 4_200);
        try std.testing.expectEqual(@as(u64, 0), again.resolved_finalized);
        try std.testing.expectEqual(@as(u64, 0), again.cleaned_records);
    }
}

test "db transactions recovery runtime resolves participants and unblocks cleanup" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    const TxnResolverRecorder = TestHelpers.TxnResolverRecorder;
    var recorder = TxnResolverRecorder{};
    const txn_id = blk: {
        var setup_db = try DB.open(alloc, std.mem.span(path), .{});
        defer setup_db.close();

        const txn_id = try setup_db.beginTransactionWithParticipants(1_000, &.{ "local", "remote" });
        try setup_db.writeTransaction(txn_id, .{
            .writes = &.{.{ .key = "doc:participant_runtime", .value = "{\"title\":\"value\"}" }},
        });
        try setup_db.resolveTransactionIntents(txn_id, .committed, 2_000);
        try setup_db.markTransactionParticipantResolved(txn_id, "local");
        break :blk txn_id;
    };

    var db = try DB.open(alloc, std.mem.span(path), .{
        .transaction_recovery = .{
            .enabled = true,
            .interval_ms = 10,
            .cutoff_ns = 1,
            .resolver_ctx = &recorder,
            .resolve_participant_fn = TxnResolverRecorder.resolve,
        },
    });
    defer db.close();
    try std.testing.expect(db.transaction_recovery_identity_context != null);
    try std.testing.expect(db.transaction_runtime.?.config.resolution_extra_hooks.build != null);

    var cleared = false;
    var attempts: usize = 0;
    while (attempts < 500) : (attempts += 1) {
        const status = db.getTransactionStatus(txn_id);
        if (status) |_| {} else |err| {
            if (err == transactions_mod.TxnError.TxnNotFound) {
                cleared = true;
                break;
            }
            return err;
        }
        platform.time.sleepMs(10);
    }
    if (!cleared) return error.TransactionRecoveryCleanupTimeout;

    var stats = try db.stats(alloc);
    var stats_ready = stats.transaction_recovery.runs > 0 and
        stats.transaction_recovery.notification_attempts > 0 and
        stats.transaction_recovery.notification_successes > 0 and
        stats.transaction_recovery.cleaned_records > 0;
    var resolver_called = false;
    _ = platform.sync.lockAtomic(&recorder.mutex);
    resolver_called = recorder.calls > 0;
    recorder.mutex.unlock();

    attempts = 0;
    while ((!stats_ready or !resolver_called) and attempts < 500) : (attempts += 1) {
        types.freeDBStats(alloc, stats);
        platform.time.sleepMs(10);
        stats = try db.stats(alloc);
        stats_ready = stats.transaction_recovery.runs > 0 and
            stats.transaction_recovery.notification_attempts > 0 and
            stats.transaction_recovery.notification_successes > 0 and
            stats.transaction_recovery.cleaned_records > 0;
        _ = platform.sync.lockAtomic(&recorder.mutex);
        resolver_called = recorder.calls > 0;
        recorder.mutex.unlock();
    }
    defer types.freeDBStats(alloc, stats);
    try std.testing.expect(stats.transaction_recovery.enabled);
    if (!stats_ready) return error.TransactionRecoveryStatsTimeout;
    try std.testing.expectError(transactions_mod.TxnError.TxnNotFound, db.getTransactionStatus(txn_id));
    if (!resolver_called) return error.TransactionRecoveryResolverTimeout;
}

test "db transactions recovery runtime appends identity rows for committed orphaned intents" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    const txn_id = blk: {
        var setup_db = try DB.open(alloc, std.mem.span(path), .{});
        defer setup_db.close();

        const txn_id = try setup_db.beginTransaction(1_000);
        try setup_db.writeTransaction(txn_id, .{
            .writes = &.{.{ .key = "doc:recovered_orphan", .value = "{\"title\":\"recovered\"}" }},
        });

        const record_key = blk_key: {
            const prefix = "\x00\x00__txn_records__:";
            var key: [prefix.len + @sizeOf(transactions_mod.TxnId)]u8 = undefined;
            @memcpy(key[0..prefix.len], prefix);
            @memcpy(key[prefix.len..], &txn_id);
            break :blk_key key;
        };
        var record_value: [33]u8 = undefined;
        record_value[0] = @intFromEnum(transactions_mod.TxnStatus.committed);
        std.mem.writeInt(u64, record_value[1..9], 1_000, .little);
        std.mem.writeInt(u64, record_value[9..17], 2_000, .little);
        std.mem.writeInt(u64, record_value[17..25], 1_000, .little);
        std.mem.writeInt(u64, record_value[25..33], 2_000, .little);
        try setup_db.core.store.put(record_key[0..], record_value[0..]);
        break :blk txn_id;
    };

    const TxnResolverRecorder = TestHelpers.TxnResolverRecorder;
    var recorder = TxnResolverRecorder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .transaction_recovery = .{
            .enabled = true,
            .interval_ms = 10,
            .cutoff_ns = 1,
            .resolver_ctx = &recorder,
            .resolve_participant_fn = TxnResolverRecorder.resolve,
        },
    });
    defer db.close();

    var cleaned = false;
    var attempts: usize = 0;
    while (attempts < 500) : (attempts += 1) {
        const status = db.getTransactionStatus(txn_id);
        if (status) |_| {} else |err| {
            if (err == transactions_mod.TxnError.TxnNotFound) {
                cleaned = true;
                break;
            }
            return err;
        }
        platform.time.sleepMs(10);
    }
    if (!cleaned) return error.TransactionRecoveryCleanupTimeout;

    const raw = (try db.get(alloc, "doc:recovered_orphan")) orelse return error.TestExpectedEqual;
    defer alloc.free(raw);
    try std.testing.expectEqualStrings("{\"title\":\"recovered\"}", raw);

    const stats = try db.diagnosticStats(alloc);
    defer types.freeDBStats(alloc, stats);
    try std.testing.expectEqual(@as(u64, 1), stats.doc_identity.state_rows);
    try std.testing.expectEqual(@as(u64, 1), stats.doc_identity.live_ordinals);
    try std.testing.expectEqual(@as(u64, 0), stats.doc_identity.primary_docs_missing_ordinals);
    try std.testing.expectEqual(@as(u64, 0), stats.doc_identity.primary_docs_missing_identity_state);
}

test "db transactions relational recovery resolves orphaned intents into base rows" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    const commit_ts: u64 = 2_000;
    const txn_id = blk: {
        var setup_db = try DB.open(alloc, std.mem.span(path), .{});
        defer setup_db.close();

        const schema_json =
            \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"title":{"type":"text"},"amount":{"type":"numeric"}},"required":["title"],"additionalProperties":false}}}}
        ;
        var parsed_schema = try table_schema_api.parseValidatedTableSchema(alloc, schema_json);
        defer parsed_schema.deinit(alloc);
        const runtime_schema = try table_schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
        defer schema_mod.freeSchema(alloc, runtime_schema);
        try setup_db.setSchema(runtime_schema);

        const txn_id = try setup_db.beginTransaction(1_000);
        try setup_db.writeTransaction(txn_id, .{
            .writes = &.{.{ .key = "row:recovered_orphan", .value = "{\"title\":\"recovered row\",\"amount\":14.5}" }},
        });

        const record_key = blk_key: {
            const prefix = "\x00\x00__txn_records__:";
            var key: [prefix.len + @sizeOf(transactions_mod.TxnId)]u8 = undefined;
            @memcpy(key[0..prefix.len], prefix);
            @memcpy(key[prefix.len..], &txn_id);
            break :blk_key key;
        };
        var record_value: [33]u8 = undefined;
        record_value[0] = @intFromEnum(transactions_mod.TxnStatus.committed);
        std.mem.writeInt(u64, record_value[1..9], 1_000, .little);
        std.mem.writeInt(u64, record_value[9..17], commit_ts, .little);
        std.mem.writeInt(u64, record_value[17..25], 1_000, .little);
        std.mem.writeInt(u64, record_value[25..33], commit_ts, .little);
        try setup_db.core.store.put(record_key[0..], record_value[0..]);
        break :blk txn_id;
    };

    const TxnResolverRecorder = TestHelpers.TxnResolverRecorder;
    var recorder = TxnResolverRecorder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .transaction_recovery = .{
            .enabled = true,
            .interval_ms = 10,
            .cutoff_ns = 1,
            .resolver_ctx = &recorder,
            .resolve_participant_fn = TxnResolverRecorder.resolve,
        },
    });
    defer db.close();

    var cleaned = false;
    var attempts: usize = 0;
    while (attempts < 500) : (attempts += 1) {
        const status = db.getTransactionStatus(txn_id);
        if (status) |_| {} else |err| {
            if (err == transactions_mod.TxnError.TxnNotFound) {
                cleaned = true;
                break;
            }
            return err;
        }
        platform.time.sleepMs(10);
    }
    if (!cleaned) return error.TransactionRecoveryCleanupTimeout;

    const raw = (try db.get(alloc, "row:recovered_orphan")) orelse return error.TestExpectedEqual;
    defer alloc.free(raw);
    try std.testing.expect(std.mem.indexOf(u8, raw, "\"title\":\"recovered row\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, raw, "\"amount\":14.5") != null);
    try std.testing.expectEqual(commit_ts, try db.getTimestamp(alloc, "row:recovered_orphan"));

    const relational_key = try relational_store_mod.rowKeyAlloc(alloc, "row:recovered_orphan");
    defer alloc.free(relational_key);
    const raw_row = try db.core.store.get(alloc, relational_key);
    defer alloc.free(raw_row);
    try std.testing.expect(mapper.isRelationalRowValue(raw_row));

    const primary_key = try internal_keys.documentKeyAlloc(alloc, "row:recovered_orphan");
    defer alloc.free(primary_key);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, primary_key));

    const stats = try db.diagnosticStats(alloc);
    defer types.freeDBStats(alloc, stats);
    try std.testing.expectEqual(@as(u64, 1), stats.doc_identity.state_rows);
    try std.testing.expectEqual(@as(u64, 1), stats.doc_identity.live_ordinals);
    try std.testing.expectEqual(@as(u64, 0), stats.doc_identity.primary_docs_missing_ordinals);
    try std.testing.expectEqual(@as(u64, 0), stats.doc_identity.primary_docs_missing_identity_state);
}

test "db transactions one-shot relational recovery resolves orphaned intents into base rows" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;
    const table_schema_api = @import("../../schema/mod.zig");

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"title":{"type":"text"},"amount":{"type":"numeric"}},"required":["title"],"additionalProperties":false}}}}
    ;
    var parsed_schema = try table_schema_api.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try table_schema_api.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer schema_mod.freeSchema(alloc, runtime_schema);
    try db.setSchema(runtime_schema);

    const commit_ts: u64 = 2_000;
    const txn_id = try db.beginTransaction(1_000);
    try db.writeTransaction(txn_id, .{
        .writes = &.{.{ .key = "row:one_shot_recovered", .value = "{\"title\":\"one shot\",\"amount\":21.5}" }},
    });

    const record_key = blk_key: {
        const prefix = "\x00\x00__txn_records__:";
        var key: [prefix.len + @sizeOf(transactions_mod.TxnId)]u8 = undefined;
        @memcpy(key[0..prefix.len], prefix);
        @memcpy(key[prefix.len..], &txn_id);
        break :blk_key key;
    };
    var record_value: [33]u8 = undefined;
    record_value[0] = @intFromEnum(transactions_mod.TxnStatus.committed);
    std.mem.writeInt(u64, record_value[1..9], 1_000, .little);
    std.mem.writeInt(u64, record_value[9..17], commit_ts, .little);
    std.mem.writeInt(u64, record_value[17..25], 1_000, .little);
    std.mem.writeInt(u64, record_value[25..33], commit_ts, .little);
    try db.core.store.put(record_key[0..], record_value[0..]);

    const TxnResolverRecorder = TestHelpers.TxnResolverRecorder;
    var recorder = TxnResolverRecorder{};
    const stats = try db.runTransactionRecoveryOnce(.{
        .enabled = true,
        .cutoff_ns = 1,
        .resolver_ctx = &recorder,
        .resolve_participant_fn = TxnResolverRecorder.resolve,
    });
    try std.testing.expect(stats.resolved_finalized >= 1);

    const raw = (try db.get(alloc, "row:one_shot_recovered")) orelse return error.TestExpectedEqual;
    defer alloc.free(raw);
    try std.testing.expect(std.mem.indexOf(u8, raw, "\"title\":\"one shot\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, raw, "\"amount\":21.5") != null);
    try std.testing.expectEqual(commit_ts, try db.getTimestamp(alloc, "row:one_shot_recovered"));

    const relational_key = try relational_store_mod.rowKeyAlloc(alloc, "row:one_shot_recovered");
    defer alloc.free(relational_key);
    const raw_row = try db.core.store.get(alloc, relational_key);
    defer alloc.free(raw_row);
    try std.testing.expect(mapper.isRelationalRowValue(raw_row));

    const primary_key = try internal_keys.documentKeyAlloc(alloc, "row:one_shot_recovered");
    defer alloc.free(primary_key);
    try std.testing.expectError(error.NotFound, db.core.store.get(alloc, primary_key));
}

test "db transactions batch enforces optimistic version predicates" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.batch(.{
        .writes = &.{.{ .key = "doc:cas", .value = "{\"title\":\"v1\"}" }},
        .timestamp_ns = 10_000,
    });

    try std.testing.expectError(transactions_mod.TxnError.VersionConflict, db.batch(.{
        .writes = &.{.{ .key = "doc:cas", .value = "{\"title\":\"v2\"}" }},
        .predicates = &.{.{ .key = "doc:cas", .expected_version = 0 }},
        .timestamp_ns = 11_000,
    }));

    try db.batch(.{
        .writes = &.{.{ .key = "doc:cas", .value = "{\"title\":\"v2\"}" }},
        .predicates = &.{.{ .key = "doc:cas", .expected_version = 10_000 }},
        .timestamp_ns = 11_000,
    });

    const raw = (try db.get(alloc, "doc:cas")) orelse return error.TestExpectedEqual;
    defer alloc.free(raw);
    try std.testing.expectEqualStrings("{\"title\":\"v2\"}", raw);
    try std.testing.expectEqual(@as(u64, 11_000), try db.getTimestamp(alloc, "doc:cas"));
}
