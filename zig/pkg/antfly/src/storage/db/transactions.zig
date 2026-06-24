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

const db_internal = @import("internal.zig");
const doc_identity = @import("doc_identity.zig");
const docstore_mod = @import("../docstore.zig");
const internal_keys = @import("../internal_keys.zig");
const platform_clock = @import("../../platform/clock.zig");
const platform_time = @import("../../platform/time.zig");
const relational_integrity = @import("relational_integrity.zig");
const relational_rows = @import("relational_rows.zig");
const relational_store_mod = @import("relational_store.zig");
const schema_mod = @import("../schema.zig");
const transactions_mod = @import("../transactions.zig");
const types = @import("types.zig");
const write_path = @import("write_path.zig");

const Allocator = std.mem.Allocator;
const row_claim_intent_key_prefix = relational_rows.row_claim_intent_key_prefix;

pub fn Impl(comptime DB: type) type {
    return struct {
        const relational_integrity_impl = relational_integrity.Impl(DB);
        const write_path_impl = write_path.Impl(DB);
        const ForeignKeyActionScheduleRecord = relational_integrity_impl.ForeignKeyActionScheduleRecord;

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
                            try relational_rows.relationalStoreRowValueAlloc(self.alloc, raw, relational_columns, &owned_relational_values)
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
            var effective_ops = try write_path_impl.coalesceKeyValueRequest(self, types.TransactionWrite, req.writes, req.deletes, req.transforms);
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
            try relational_integrity_impl.appendForeignKeyExternalizedParentCheckIntents(self, txn_id, intents, owned_keys, owned_values, checks);
        }

        fn appendForeignKeyConstraintTimingOverrideIntents(
            self: *DB,
            txn_id: types.TxnId,
            intents: *std.ArrayListUnmanaged(transactions_mod.WriteIntent),
            owned_keys: *std.ArrayListUnmanaged([]u8),
            owned_values: *std.ArrayListUnmanaged([]u8),
            overrides: []const types.ForeignKeyConstraintTimingOverride,
        ) !void {
            try relational_integrity_impl.appendForeignKeyConstraintTimingOverrideIntents(self, txn_id, intents, owned_keys, owned_values, overrides);
        }

        fn validateUniqueConstraintMutations(
            self: *DB,
            unique_writes: []const types.UniqueConstraintMutation,
            unique_deletes: []const types.UniqueConstraintMutation,
        ) !void {
            try relational_integrity_impl.validateUniqueConstraintMutations(self, unique_writes, unique_deletes);
        }

        const findUniqueConstraintMutation = relational_integrity.findUniqueConstraintMutation;

        fn appendUniqueConstraintMutationIntents(
            self: *DB,
            intents: *std.ArrayListUnmanaged(transactions_mod.WriteIntent),
            owned_keys: *std.ArrayListUnmanaged([]u8),
            owned_values: *std.ArrayListUnmanaged([]u8),
            unique_writes: []const types.UniqueConstraintMutation,
            unique_deletes: []const types.UniqueConstraintMutation,
        ) !void {
            try relational_integrity_impl.appendUniqueConstraintMutationIntents(self, intents, owned_keys, owned_values, unique_writes, unique_deletes);
        }

        fn appendForeignKeyRefMutationIntents(
            self: *DB,
            intents: *std.ArrayListUnmanaged(transactions_mod.WriteIntent),
            owned_keys: *std.ArrayListUnmanaged([]u8),
            ref_writes: []const types.ForeignKeyRefMutation,
            ref_deletes: []const types.ForeignKeyRefMutation,
        ) !void {
            try relational_integrity_impl.appendForeignKeyRefMutationIntents(self, intents, owned_keys, ref_writes, ref_deletes);
        }

        fn applyForeignKeyParentDeleteActions(
            self: *DB,
            intents: *std.ArrayListUnmanaged(transactions_mod.WriteIntent),
            owned_keys: *std.ArrayListUnmanaged([]u8),
            owned_values: *std.ArrayListUnmanaged([]u8),
            checks: []const types.ForeignKeyParentDeleteCheck,
        ) !void {
            try relational_integrity_impl.applyForeignKeyParentDeleteActions(self, intents, owned_keys, owned_values, checks);
        }

        fn applyForeignKeySetNullChildActions(
            self: *DB,
            intents: *std.ArrayListUnmanaged(transactions_mod.WriteIntent),
            owned_keys: *std.ArrayListUnmanaged([]u8),
            owned_values: *std.ArrayListUnmanaged([]u8),
            actions: []const types.ForeignKeySetNullChildAction,
        ) !void {
            try relational_integrity_impl.applyForeignKeySetNullChildActions(self, intents, owned_keys, owned_values, actions);
        }

        fn applyForeignKeyCascadeChildActions(
            self: *DB,
            intents: *std.ArrayListUnmanaged(transactions_mod.WriteIntent),
            owned_keys: *std.ArrayListUnmanaged([]u8),
            owned_values: *std.ArrayListUnmanaged([]u8),
            actions: []const types.ForeignKeyCascadeChildAction,
        ) !void {
            try relational_integrity_impl.applyForeignKeyCascadeChildActions(self, intents, owned_keys, owned_values, actions);
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
            try relational_integrity_impl.appendForeignKeyConflictIntents(self, intents, owned_keys, writes, parent_delete_checks, conflict_checks, ref_writes);
        }

        fn validateForeignKeyParentChecks(
            self: *DB,
            checks: []const types.ForeignKeyParentCheck,
            writes: []const types.TransactionWrite,
            deletes: []const []const u8,
            unique_writes: []const types.UniqueConstraintMutation,
            unique_deletes: []const types.UniqueConstraintMutation,
        ) !void {
            try relational_integrity_impl.validateForeignKeyParentChecks(self, checks, writes, deletes, unique_writes, unique_deletes);
        }

        fn validateForeignKeyReferenceShapes(
            self: *DB,
            writes: []const types.TransactionWrite,
        ) !void {
            try relational_integrity_impl.validateForeignKeyReferenceShapes(self, writes);
        }

        fn validateExternalizedForeignKeyParentChecks(
            self: *DB,
            checks: []const types.ForeignKeyParentCheck,
            constraint_timing_overrides: []const types.ForeignKeyConstraintTimingOverride,
            writes: []const types.TransactionWrite,
        ) !void {
            try relational_integrity_impl.validateExternalizedForeignKeyParentChecks(self, checks, constraint_timing_overrides, writes);
        }

        fn validateForeignKeyConstraintTimingOverrides(
            self: *DB,
            overrides: []const types.ForeignKeyConstraintTimingOverride,
        ) !void {
            try relational_integrity_impl.validateForeignKeyConstraintTimingOverrides(self, overrides);
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
            try relational_integrity_impl.validateForeignKeyParentDeleteChecks(self, checks, constraint_timing_overrides, writes, deletes, ref_writes, ref_deletes);
        }

        fn validateForeignKeyRefMutations(self: *DB, mutations: []const types.ForeignKeyRefMutation) !void {
            try relational_integrity_impl.validateForeignKeyRefMutations(self, mutations);
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
                    if (!isRowClaimIntentMetadataKey(mutation.key) and !isForeignKeyActionScheduleMetadataKey(mutation.key)) continue;
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
                        const row_value = try relational_rows.relationalStoreRowValueAlloc(self.alloc, rewrite.value, relational_columns, &relational_extra_owned_values);
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
            return try relational_rows.Impl(DB).appendSystemVersionedHistoryForTransactionMutations(self, mutations, rewrites, commit_version, writes, owned_keys, owned_values, isUserRowMutationKey);
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
            return relational_integrity_impl.foreignKeyActionJobCanonicalAction(action);
        }

        fn foreignKeyActionScheduleKeyAlloc(alloc: Allocator, schedule_id: []const u8) ![]u8 {
            return try relational_integrity_impl.foreignKeyActionScheduleKeyAlloc(alloc, schedule_id);
        }

        fn validateForeignKeyActionLineage(cascade_depth: u32, cascade_max_depth: u32) !void {
            try relational_integrity_impl.validateForeignKeyActionLineage(cascade_depth, cascade_max_depth);
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
            try relational_integrity_impl.validateForeignKeyActionJobIdentity(job_id, action, worker_id, constraint_name, parent_table, parent_key, updated_parent_key);
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
            try relational_integrity_impl.validateForeignKeyActionScheduleMatches(existing, action_job_id, action, constraint_name, parent_table, parent_key, updated_parent_key);
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

const rowClaimIntentKeyAlloc = relational_rows.rowClaimIntentKeyAlloc;
const rowClaimIntentValueAlloc = relational_rows.rowClaimIntentValueAlloc;
const relationalIdentityRewriteIntentKeyAlloc = relational_rows.relationalIdentityRewriteIntentKeyAlloc;
const encodeRelationalIdentityRewriteIntentValueAlloc = relational_rows.encodeRelationalIdentityRewriteIntentValueAlloc;
const collectTransactionRelationalIdentityRewritesAlloc = relational_rows.collectTransactionRelationalIdentityRewritesAlloc;
const freeRelationalIdentityRewrites = relational_rows.freeRelationalIdentityRewrites;
const isRelationalIdentityRewriteEndpoint = relational_rows.isRelationalIdentityRewriteEndpoint;
const isForeignKeyActionScheduleMetadataKey = relational_integrity.isForeignKeyActionScheduleMetadataKey;
const collectTransactionExternalizedForeignKeyParentChecksAlloc = relational_integrity.collectTransactionExternalizedForeignKeyParentChecksAlloc;
const collectTransactionForeignKeyConstraintTimingOverridesAlloc = relational_integrity.collectTransactionForeignKeyConstraintTimingOverridesAlloc;
const freeExternalizedForeignKeyParentChecks = relational_integrity.freeExternalizedForeignKeyParentChecks;
const freeRelationalForeignKeyConstraintTimingOverrides = relational_integrity.freeRelationalForeignKeyConstraintTimingOverrides;

pub fn reclaimExpiredRowClaimIntentsForRows(
    self: anytype,
    claiming_txn_id: types.TxnId,
    row_keys: []const []const u8,
    now_ns: u64,
) !usize {
    const DB = @TypeOf(self.*);
    return try relational_rows.Impl(DB).reclaimExpiredRowClaimIntentsForRows(self, claiming_txn_id, row_keys, now_ns);
}

pub fn reclaimExpiredRowClaimIntentsForMutationKeys(
    self: anytype,
    writes: anytype,
    deletes: []const []const u8,
    exclude_txn_id: ?types.TxnId,
    now_ns: u64,
    comptime already_locked: bool,
) !usize {
    const DB = @TypeOf(self.*);
    return try relational_rows.Impl(DB).reclaimExpiredRowClaimIntentsForMutationKeys(self, writes, deletes, exclude_txn_id, now_ns, already_locked, isUserRowMutationKey);
}

pub fn reclaimExpiredRowClaimIntentsForIdentityRewrites(
    self: anytype,
    rewrites: []const types.RelationalIdentityRewrite,
    exclude_txn_id: ?types.TxnId,
    now_ns: u64,
    comptime already_locked: bool,
) !usize {
    const DB = @TypeOf(self.*);
    return try relational_rows.Impl(DB).reclaimExpiredRowClaimIntentsForIdentityRewrites(self, rewrites, exclude_txn_id, now_ns, already_locked, isUserRowMutationKey);
}

pub fn appendRowClaimPredicatesForMutationKeys(
    alloc: Allocator,
    predicates: *std.ArrayListUnmanaged(transactions_mod.VersionPredicate),
    owned_keys: *std.ArrayListUnmanaged([]u8),
    writes: anytype,
    deletes: []const []const u8,
) !void {
    return try relational_rows.appendRowClaimPredicatesForMutationKeys(alloc, predicates, owned_keys, writes, deletes, isUserRowMutationKey);
}

pub fn appendRowClaimPredicatesForIdentityRewrites(
    alloc: Allocator,
    predicates: *std.ArrayListUnmanaged(transactions_mod.VersionPredicate),
    owned_keys: *std.ArrayListUnmanaged([]u8),
    rewrites: []const types.RelationalIdentityRewrite,
) !void {
    return try relational_rows.appendRowClaimPredicatesForIdentityRewrites(alloc, predicates, owned_keys, rewrites, isUserRowMutationKey);
}

pub fn validateRelationalIdentityRewriteRequest(
    rewrites: []const types.RelationalIdentityRewrite,
    writes: []const types.TransactionWrite,
    deletes: []const []const u8,
) !void {
    return try relational_rows.validateRelationalIdentityRewriteRequest(rewrites, writes, deletes, isUserRowMutationKey);
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
    const DB = @TypeOf(self.*);
    return try relational_rows.Impl(DB).appendSystemVersionedHistoryForBatch(self, req, sequence, timestamp_ns, writes, owned_keys, owned_values, isUserRowMutationKey);
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
