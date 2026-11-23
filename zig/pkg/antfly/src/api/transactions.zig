// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

const std = @import("std");
const platform_sync = @import("antfly_platform").sync;
const batch_api = @import("antfly_local_sources").api_internal_batch;
const db_mod = @import("antfly_local_sources").storage_db_selected_root.db;
const distributed_txn = @import("distributed_txn.zig");
const backend_erased = @import("antfly_local_sources").storage_backend_erased;
const docstore_mod = @import("antfly_local_sources").storage_docstore;
const mem_backend = @import("antfly_local_sources").storage_mem_backend;
const lease_mod = @import("antfly_local_sources").storage_db_lease;
const sql_connection_record = @import("sql/session/connection_record.zig");
const platform_time = @import("antfly_platform").time;

const session_record = @import("../metadata/transaction_session_record.zig");
const session_prefix = session_record.session_prefix;
const session_lease_prefix = "\x00\x00__api_txn_session_leases__:";
const sql_abort_proof_prefix = "\x00\x00__api_txn_completion__:";
const sql_legacy_pin_fence_key = "\x00\x00__api_sql_legacy_pin_fence__";
const tagged_sql_backfill_key = "\x00\x00__api_sql_terminal_backfill_v1__";
const tagged_sql_expiry_prefix = "\x00\x00__api_sql_terminal_expiry__:";
const sql_abort_expiry_prefix = "\x00\x00__api_sql_abort_expiry__:";
const session_expiry_prefix = "\x00\x00__api_txn_session_expiry__:";
const session_recovery_prefix = "\x00\x00__api_txn_session_recovery__:";
var txn_id_nonce: std.atomic.Value(u64) = .init(0);

const AtomicMutex = struct {
    inner: std.atomic.Mutex = .unlocked,

    fn lock(self: *AtomicMutex) void {
        platform_sync.lockYielding(&self.inner);
    }

    fn unlock(self: *AtomicMutex) void {
        self.inner.unlock();
    }
};

pub const TransactionReadItem = struct {
    table_name: []u8,
    key: []u8,
    expected_version: u64,

    pub fn clone(self: TransactionReadItem, alloc: std.mem.Allocator) !TransactionReadItem {
        const table_name = try alloc.dupe(u8, self.table_name);
        errdefer alloc.free(table_name);
        return .{
            .table_name = table_name,
            .key = try alloc.dupe(u8, self.key),
            .expected_version = self.expected_version,
        };
    }

    pub fn deinit(self: *TransactionReadItem, alloc: std.mem.Allocator) void {
        alloc.free(self.table_name);
        alloc.free(self.key);
        self.* = undefined;
    }
};

pub const TableCommitRequest = struct {
    pub const ConflictGuards = struct {
        generation_set: [32]u8,
        commands: []const @import("antfly_local_sources").storage_db_relational_integrity_contract.Command,
        pub fn jsonStringify(self: @This(), stream: anytype) @TypeOf(stream.*).Error!void {
            return @import("antfly_local_sources").storage_db_relational_integrity_json.write(self, stream);
        }
    };
    /// Private statement intent, never decoded from a public commit body.
    /// The final coordinator re-resolves current ACLs and credential limits.
    pub const SqlAuthorization = struct {
        principal_instance: [16]u8,
        table_id: u64,
        required: u16,
        native_rights: u16,
        row_filter_digest: [32]u8,
        native_read: bool = false,
        native_write: bool = false,

        pub fn validate(self: @This()) !void {
            if (std.mem.allEqual(u8, &self.principal_instance, 0) or self.table_id == 0 or self.required == 0 or self.required & ~@as(u16, 255) != 0 or self.native_rights & ~@as(u16, 255) != 0) return error.InvalidTransactionSessionRecord;
        }
        pub fn merge(self: *@This(), other: @This()) !void {
            try other.validate();
            if (self.table_id != other.table_id or !std.mem.eql(u8, &self.principal_instance, &other.principal_instance)) return error.CatalogGenerationChanged;
            if (!std.mem.eql(u8, &self.row_filter_digest, &other.row_filter_digest)) return error.Forbidden;
            self.required |= other.required;
            self.native_rights |= other.native_rights;
            self.native_read = self.native_read or other.native_read;
            self.native_write = self.native_write or other.native_write;
        }
    };
    fn nativeRead(self: TableCommitRequest) bool {
        return self.range_guards != null;
    }
    fn nativeWrite(self: TableCommitRequest) bool {
        return self.batch.writes.len != 0 or self.batch.deletes.len != 0 or self.batch.transforms.len != 0;
    }
    pub fn mergeAuthorization(self: *TableCommitRequest, other: TableCommitRequest) !void {
        if (self.sql_authorization) |*current| {
            if (other.sql_authorization) |value| try current.merge(value) else {
                current.native_read = current.native_read or other.nativeRead();
                current.native_write = current.native_write or other.nativeWrite();
            }
        } else if (other.sql_authorization) |value| {
            var captured = value;
            try captured.validate();
            captured.native_read = captured.native_read or self.nativeRead();
            captured.native_write = captured.native_write or self.nativeWrite();
            self.sql_authorization = captured;
        }
    }

    /// Server-authored arbiter observations survive statement merging,
    /// savepoints, restart and final native dependency expansion.
    sql_authorization: ?SqlAuthorization = null,
    /// Server-authored statement completions. They are durable data, while
    /// user-authority signing callbacks are reconstructed only at commit.
    document_revisions: ?@import("sql/execution/document_revision_custody.zig").Owned = null,
    trigger_receipts: ?std.json.Parsed([]const @import("antfly_local_sources").sql_catalog.trigger_handoff.Receipt) = null,
    conflict_guards: ?std.json.Parsed(ConflictGuards) = null,
    range_guards: ?@import("antfly_local_sources").api_range_read_guards.Owned = null,
    schema_version: ?u32 = null,
    table_name: []u8,
    relational_schema_version: ?u32 = null,
    batch: batch_api.OwnedBatchRequest = .{},
    // The batch field retains only an explicit table override. This floor
    // retains the strongest effective prerequisite already selected by a stage.
    commit_requires_floor: ?db_mod.types.CommitRequires = null,
    predicates: std.ArrayListUnmanaged(db_mod.types.TransactionVersionPredicate) = .empty,

    fn writeCount(self: *const TableCommitRequest) usize {
        return self.batch.writes.len + self.batch.transforms.len + self.batch.graph_writes.len;
    }

    fn deleteCount(self: *const TableCommitRequest) usize {
        return self.batch.deletes.len + self.batch.graph_deletes.len;
    }

    pub fn deinit(self: *TableCommitRequest, alloc: std.mem.Allocator) void {
        if (self.document_revisions) |*revisions| revisions.deinit();
        if (self.trigger_receipts) |*receipts| receipts.deinit();
        if (self.conflict_guards) |*guards| guards.deinit();
        if (self.range_guards) |*guards| guards.deinit();
        alloc.free(self.table_name);
        for (self.predicates.items) |predicate| alloc.free(@constCast(predicate.key));
        self.predicates.deinit(alloc);
        self.batch.deinit(alloc);
        self.* = undefined;
    }

    pub fn clone(self: TableCommitRequest, alloc: std.mem.Allocator) !TableCommitRequest {
        var out: TableCommitRequest = .{
            .table_name = try alloc.dupe(u8, self.table_name),
            .schema_version = self.schema_version,
            .relational_schema_version = self.relational_schema_version,
            .sql_authorization = self.sql_authorization,
            .commit_requires_floor = self.commit_requires_floor,
        };
        errdefer out.deinit(alloc);
        out.batch = try cloneBatchRequest(alloc, self.batch);
        if (self.document_revisions) |revisions| try out.mergeDocumentRevisions(alloc, revisions.value);
        if (self.trigger_receipts) |receipts| try out.mergeTriggerReceipts(alloc, receipts.value);
        if (self.conflict_guards) |guards| try out.mergeConflictGuards(alloc, guards.value);
        if (self.range_guards) |guards| try out.mergeRangeGuards(alloc, guards.value);
        try clonePredicatesInto(alloc, &out.predicates, self.predicates.items);
        return out;
    }

    pub fn mergeFrom(self: *TableCommitRequest, alloc: std.mem.Allocator, other: TableCommitRequest) !void {
        try self.mergeAuthorization(other);
        if (other.document_revisions) |revisions| try self.mergeDocumentRevisions(alloc, revisions.value);
        // Staged statements may strengthen admission, never erase an earlier
        // prerequisite while coalescing the final table postimage.
        self.commit_requires_floor = db_mod.types.CommitRequires.strongest(self.commit_requires_floor, other.commit_requires_floor);
        self.batch.req.commit_requires = db_mod.types.CommitRequires.strongest(self.batch.req.commit_requires, other.batch.req.commit_requires);
        if (other.range_guards) |guards| try self.mergeRangeGuards(alloc, guards.value);
        if (other.trigger_receipts) |receipts| try self.mergeTriggerReceipts(alloc, receipts.value);
        if (other.conflict_guards) |guards| try self.mergeConflictGuards(alloc, guards.value);
        if (self.schema_version != null and other.schema_version != null and self.schema_version != other.schema_version) return error.CatalogGenerationChanged;
        if (self.schema_version == null) self.schema_version = other.schema_version;
        if (self.relational_schema_version != null and other.relational_schema_version != null and self.relational_schema_version != other.relational_schema_version) return error.CatalogGenerationChanged;
        if (self.relational_schema_version == null) self.relational_schema_version = other.relational_schema_version;
        try appendBatchWrites(alloc, &self.batch, other.batch.writes);
        try appendBatchDeletes(alloc, &self.batch, other.batch.deletes);
        try appendBatchTransforms(alloc, &self.batch, other.batch.transforms);
        try @import("transaction_graph_effects.zig").merge(alloc, &self.batch, other.batch.graph_writes, other.batch.graph_deletes);
        try appendPredicates(alloc, &self.predicates, other.predicates.items);
        syncTableBatch(self);
    }

    pub fn mergeDocumentRevisions(self: *TableCommitRequest, a: std.mem.Allocator, incoming: []const @import("sql/execution/document_revision_custody.zig").Handoff) !void {
        const custody = @import("sql/execution/document_revision_custody.zig");
        const previous = if (self.document_revisions) |owned| owned.value else &.{};
        const owned = try custody.Owned.merge(a, previous, incoming);
        if (self.document_revisions) |*old| old.deinit();
        self.document_revisions = owned;
    }

    pub fn mergeTriggerReceipts(self: *TableCommitRequest, a: std.mem.Allocator, incoming: []const @import("antfly_local_sources").sql_catalog.trigger_handoff.Receipt) !void {
        const handoff = @import("antfly_local_sources").sql_catalog.trigger_handoff;
        const previous: []const handoff.Receipt = if (self.trigger_receipts) |owner| owner.value else &.{};
        var merged: std.ArrayList(handoff.Receipt) = .empty;
        defer merged.deinit(a);
        try merged.appendSlice(a, previous);
        for (incoming) |receipt| {
            var duplicate = false;
            for (previous) |prior| if (prior.processed.header.sequence == receipt.processed.header.sequence and std.mem.eql(u8, &prior.processed.header.statement_id, &receipt.processed.header.statement_id)) {
                const old = try handoff.encodeReceipts(a, &.{prior});
                defer a.free(old);
                const next = try handoff.encodeReceipts(a, &.{receipt});
                defer a.free(next);
                if (!std.mem.eql(u8, old, next)) return error.InvalidSqlTriggerManifest;
                duplicate = true;
                break;
            };
            if (!duplicate) try merged.append(a, receipt);
        }
        const bytes = try handoff.encodeReceipts(a, merged.items);
        defer a.free(bytes);
        const owner = try handoff.decodeReceipts(a, bytes);
        if (self.trigger_receipts) |*old| old.deinit();
        self.trigger_receipts = owner;
    }

    pub fn mergeConflictGuards(self: *TableCommitRequest, alloc: std.mem.Allocator, incoming: ConflictGuards) !void {
        const native = @import("antfly_local_sources").storage_db_relational_integrity_contract;
        if (std.mem.allEqual(u8, &incoming.generation_set, 0)) return error.InvalidTransactionSessionRecord;
        for (incoming.commands) |command| if (command.operation != .compare_claim) return error.InvalidTransactionSessionRecord;
        const previous: []const native.Command = if (self.conflict_guards) |guards| blk: {
            if (!std.mem.eql(u8, &guards.value.generation_set, &incoming.generation_set)) return error.CatalogGenerationChanged;
            break :blk guards.value.commands;
        } else &.{};
        if (incoming.commands.len > native.max_commands or previous.len > native.max_commands) return error.TransactionTooLarge;
        const combined = try alloc.alloc(native.Command, previous.len + incoming.commands.len);
        defer alloc.free(combined);
        var seen: std.AutoHashMapUnmanaged(native.Address, void) = .empty;
        defer seen.deinit(alloc);
        var count: usize = 0;
        // Retain the first physical observation across statements. Later
        // statements can observe this transaction's own staged claim changes;
        // their statement validation must not replace the initial commit fence.
        for ([_][]const native.Command{ previous, incoming.commands }) |commands| for (commands) |command| {
            if ((try seen.getOrPut(alloc, command.address)).found_existing) continue;
            if (count >= native.max_commands) return error.TransactionTooLarge;
            combined[count] = command;
            count += 1;
        };
        _ = try native.validateCommandAdmission(combined[0..count]);
        const bytes = try std.json.Stringify.valueAlloc(alloc, ConflictGuards{ .generation_set = incoming.generation_set, .commands = combined[0..count] }, .{});
        defer alloc.free(bytes);
        const owned = try std.json.parseFromSlice(ConflictGuards, alloc, bytes, .{ .allocate = .alloc_always });
        if (self.conflict_guards) |*guards| guards.deinit();
        self.conflict_guards = owned;
    }

    pub fn mergeRangeGuards(self: *TableCommitRequest, alloc: std.mem.Allocator, incoming: []const @import("antfly_local_sources").api_range_read_guards.OwnerRangeProof) !void {
        const merged = try @import("antfly_local_sources").api_range_read_guards.merge(alloc, if (self.range_guards) |guards| guards.value else &.{}, incoming);
        if (self.range_guards) |*guards| guards.deinit();
        self.range_guards = merged;
    }

    pub fn result(self: TableCommitRequest) batch_api.BatchResult {
        return self.batch.result();
    }
};

pub const CatalogBinding = struct {
    logical: []const u8,
    physical: []const u8,
    sql_persistence: ?@import("antfly_local_sources").common_topology_records.RelationPersistence.Descriptor = null,
    pub fn jsonStringify(self: @This(), stream: anytype) @TypeOf(stream.*).Error!void {
        if (self.sql_persistence) |descriptor| return stream.write(.{ .logical = self.logical, .physical = self.physical, .sql_persistence = descriptor });
        return stream.write(.{ .logical = self.logical, .physical = self.physical });
    }
};

pub const OwnedTransactionCommitRequest = struct {
    frozen_births: @import("sql/copy/frozen_births.zig").Ledger = .{},
    // Observation returned only to the preflight caller. Never staged or serialized.
    session_version: ?DurableSessionStore.Version = null,
    bound_catalog_incarnation: ?[16]u8 = null,
    constraint_timing: std.ArrayListUnmanaged(@import("antfly_local_sources").storage_relational_index.ConstraintTiming) = .empty,

    // Server-authored identity bindings are persisted with staged operations.
    // Public JSON cannot supply them. Labels remain stable across renames.
    catalog_bindings: std.ArrayListUnmanaged(CatalogBinding) = .empty,

    read_set: []TransactionReadItem = &.{},
    tables: []TableCommitRequest = &.{},
    sync_level: db_mod.types.SyncLevel = .propose,
    // Retain the request default until integrity expansion has enlisted all
    // mutation participants, including tables absent from the submitted JSON.
    commit_requires: ?db_mod.types.CommitRequires = null,

    pub fn setConstraintTiming(self: *@This(), alloc: std.mem.Allocator, mode: @import("antfly_local_sources").storage_relational_index.ConstraintTiming) !void {
        if (mode.generation == null) self.constraint_timing.clearRetainingCapacity();
        for (self.constraint_timing.items) |*previous| if (std.meta.eql(previous.generation, mode.generation)) {
            previous.* = mode;
            return;
        };
        if (self.constraint_timing.items.len >= 4096) return error.TransactionTooLarge;
        try self.constraint_timing.append(alloc, mode);
    }

    pub fn bind(self: *OwnedTransactionCommitRequest, alloc: std.mem.Allocator, logical: []const u8, physical: []const u8) !void {
        return self.bindWithPersistence(alloc, logical, physical, null);
    }
    pub fn bindWithPersistence(self: *OwnedTransactionCommitRequest, alloc: std.mem.Allocator, logical: []const u8, physical: []const u8, persistence: ?@import("antfly_local_sources").common_topology_records.RelationPersistence.Descriptor) !void {
        if (persistence) |descriptor| try descriptor.validate();
        // Renames and cascades may retain several logical labels for one
        // physical participant. They must all name the same exact image.
        for (self.catalog_bindings.items) |binding| if (std.mem.eql(u8, binding.physical, physical) and
            !std.meta.eql(binding.sql_persistence, persistence)) return error.CatalogGenerationChanged;
        for (self.catalog_bindings.items) |binding| if (std.mem.eql(u8, binding.logical, logical)) {
            if (!std.mem.eql(u8, binding.physical, physical) or !std.meta.eql(binding.sql_persistence, persistence)) return error.CatalogGenerationChanged;
            return;
        };
        const owned_logical = try alloc.dupe(u8, logical);
        errdefer alloc.free(owned_logical);
        const owned_physical = try alloc.dupe(u8, physical);
        errdefer alloc.free(owned_physical);
        try self.catalog_bindings.append(alloc, .{ .logical = owned_logical, .physical = owned_physical, .sql_persistence = persistence });
    }
    pub fn physicalName(self: OwnedTransactionCommitRequest, logical: []const u8) []const u8 {
        for (self.catalog_bindings.items) |binding| if (std.mem.eql(u8, binding.logical, logical)) return binding.physical;
        return logical;
    }
    pub fn observeRanges(self: *OwnedTransactionCommitRequest, alloc: std.mem.Allocator, logical: []const u8, physical: []const u8, schema_version: u32, observations: []const @import("antfly_local_sources").api_range_read_guards.OwnerRangeProof) !void {
        const persistence = for (self.catalog_bindings.items) |binding| {
            if (std.mem.eql(u8, binding.logical, logical)) break binding.sql_persistence;
        } else null;
        try self.bindWithPersistence(alloc, logical, physical, persistence);
        for (self.tables) |*table| if (std.mem.eql(u8, self.physicalName(table.table_name), physical)) {
            if (table.schema_version != null and table.schema_version != schema_version) return error.CatalogGenerationChanged;
            try table.mergeRangeGuards(alloc, observations);
            table.schema_version = schema_version;
            return;
        };
        var guards = try @import("antfly_local_sources").api_range_read_guards.merge(alloc, &.{}, observations);
        defer guards.deinit();
        try appendTable(alloc, self, .{ .table_name = @constCast(logical), .schema_version = schema_version, .range_guards = guards });
    }
    pub fn logicalName(self: OwnedTransactionCommitRequest, physical: []const u8) []const u8 {
        for (self.catalog_bindings.items) |binding| if (std.mem.eql(u8, binding.physical, physical)) return binding.logical;
        return physical;
    }

    /// Split the owned row request after the full payload is durable. Private
    /// rows are delivered by the catalog participant from that original cut.
    fn discardPrivateGenerations(self: *OwnedTransactionCommitRequest, a: std.mem.Allocator, pending: generation_intents.Pending) !void {
        var table_count: usize = 0;
        for (self.tables) |table| {
            if (!pending.ownsPhysical(self.physicalName(table.table_name))) table_count += 1;
        }
        var read_count: usize = 0;
        for (self.read_set) |read| {
            if (!pending.ownsPhysical(self.physicalName(read.table_name))) read_count += 1;
        }
        const tables = try a.alloc(TableCommitRequest, table_count);
        errdefer a.free(tables);
        const reads = try a.alloc(TransactionReadItem, read_count);
        var table_index: usize = 0;
        for (self.tables) |*table| {
            if (pending.ownsPhysical(self.physicalName(table.table_name))) {
                table.deinit(a);
            } else {
                tables[table_index] = table.*;
                table_index += 1;
            }
        }
        var read_index: usize = 0;
        for (self.read_set) |*read| {
            if (pending.ownsPhysical(self.physicalName(read.table_name))) {
                read.deinit(a);
            } else {
                reads[read_index] = read.*;
                read_index += 1;
            }
        }
        a.free(self.tables);
        a.free(self.read_set);
        self.tables = tables;
        self.read_set = reads;
        var i = self.catalog_bindings.items.len;
        while (i != 0) {
            i -= 1;
            if (!pending.ownsPhysical(self.catalog_bindings.items[i].physical)) continue;
            const removed = self.catalog_bindings.orderedRemove(i);
            a.free(removed.physical);
            a.free(removed.logical);
        }
        self.frozen_births.deinit(a);
        self.frozen_births = .{};
    }

    pub fn deinit(self: *OwnedTransactionCommitRequest, alloc: std.mem.Allocator) void {
        self.frozen_births.deinit(alloc);
        self.constraint_timing.deinit(alloc);
        for (self.catalog_bindings.items) |binding| {
            alloc.free(binding.logical);
            alloc.free(binding.physical);
        }
        self.catalog_bindings.deinit(alloc);
        for (self.read_set) |*item| item.deinit(alloc);
        if (self.read_set.len > 0) alloc.free(self.read_set);
        for (self.tables) |*table| table.deinit(alloc);
        if (self.tables.len > 0) alloc.free(self.tables);
        self.* = undefined;
    }

    pub fn clone(self: OwnedTransactionCommitRequest, alloc: std.mem.Allocator) !OwnedTransactionCommitRequest {
        var out: OwnedTransactionCommitRequest = .{
            .bound_catalog_incarnation = self.bound_catalog_incarnation,
            .sync_level = self.sync_level,
            .commit_requires = self.commit_requires,
        };
        errdefer out.deinit(alloc);
        out.frozen_births = try self.frozen_births.clone(alloc);
        try out.constraint_timing.appendSlice(alloc, self.constraint_timing.items);
        for (self.catalog_bindings.items) |binding| try out.bindWithPersistence(alloc, binding.logical, binding.physical, binding.sql_persistence);

        out.read_set = try alloc.alloc(TransactionReadItem, self.read_set.len);
        var read_count: usize = 0;
        errdefer {
            for (out.read_set[0..read_count]) |*item| item.deinit(alloc);
            if (out.read_set.len > 0) alloc.free(out.read_set);
            out.read_set = &.{};
        }
        for (self.read_set) |item| {
            out.read_set[read_count] = try item.clone(alloc);
            read_count += 1;
        }

        out.tables = try alloc.alloc(TableCommitRequest, self.tables.len);
        var table_count: usize = 0;
        errdefer {
            for (out.tables[0..table_count]) |*table| table.deinit(alloc);
            if (out.tables.len > 0) alloc.free(out.tables);
            out.tables = &.{};
        }
        for (self.tables) |table| {
            out.tables[table_count] = try table.clone(alloc);
            table_count += 1;
        }
        return out;
    }

    fn retainCommitRequires(self: *OwnedTransactionCommitRequest) void {
        for (self.tables) |*table| table.commit_requires_floor = db_mod.types.CommitRequires.strongest(
            table.commit_requires_floor,
            table.batch.req.commit_requires orelse self.commit_requires,
        );
    }

    /// Only executable statement effects may end a tuple's lifetime.
    /// Integrity preparation also merges physical postimages, but those
    /// already-admitted rows can be rewritten without a new user mutation.
    pub fn invalidateFrozenBirths(self: *OwnedTransactionCommitRequest, other: *const OwnedTransactionCommitRequest) !void {
        if (self.frozen_births.rows != 0) for (other.tables) |table| {
            const physical_name = other.physicalName(table.table_name);
            if (!@import("../metadata/relation_create.zig").validPhysicalName(physical_name)) continue;
            var physical: [16]u8 = undefined;
            _ = std.fmt.hexToBytes(&physical, physical_name[6..]) catch return error.InvalidTransactionSessionRecord;
            for (table.batch.writes) |write| self.frozen_births.invalidateKey(physical, write.key);
            for (table.batch.deletes) |key| self.frozen_births.invalidateKey(physical, key);
        };
    }

    pub fn mergeFrom(self: *OwnedTransactionCommitRequest, alloc: std.mem.Allocator, other: *const OwnedTransactionCommitRequest) !void {
        if (other.bound_catalog_incarnation) |incarnation| {
            if (self.bound_catalog_incarnation) |bound| if (!std.mem.eql(u8, &bound, &incarnation)) return error.CatalogGenerationChanged;
            self.bound_catalog_incarnation = incarnation;
        }
        try self.frozen_births.merge(alloc, other.frozen_births);
        self.commit_requires = db_mod.types.CommitRequires.strongest(self.commit_requires, other.commit_requires);
        for (other.constraint_timing.items) |mode| try self.setConstraintTiming(alloc, mode);
        for (other.catalog_bindings.items) |binding| try self.bindWithPersistence(alloc, binding.logical, binding.physical, binding.sql_persistence);
        try appendReadSet(alloc, self, other.read_set);
        for (other.tables) |table| {
            // Labels can change (rename) or be server-authored (FK cascade).
            // A participant is identified by its pinned physical generation.
            const physical = other.physicalName(table.table_name);
            const existing: ?usize = for (self.tables, 0..) |current, index| {
                if (std.mem.eql(u8, self.physicalName(current.table_name), physical)) break index;
            } else null;
            if (existing) |idx| {
                try self.tables[idx].mergeFrom(alloc, table);
            } else {
                try appendTable(alloc, self, table);
            }
        }
        // Apply the strengthened default after this statement's explicit
        // overrides, while retaining floors selected by earlier statements.
        self.retainCommitRequires();
    }

    /// Caller-owned descriptors borrowing this request's payloads. The request
    /// must remain alive and unchanged until the view is released. In particular,
    /// statement scratch must never become an allocation owned by the request.
    pub const DistributedTables = struct {
        tables: []distributed_txn.TableCommitRequest,
        writes: []db_mod.types.TransactionWrite,
        allocator: std.mem.Allocator,

        pub fn deinit(self: DistributedTables) void {
            self.allocator.free(self.writes);
            self.allocator.free(self.tables);
        }
    };

    pub fn distributedTables(self: *const OwnedTransactionCommitRequest, alloc: std.mem.Allocator) !DistributedTables {
        var write_count: usize = 0;
        for (self.tables) |table| write_count = std.math.add(usize, write_count, table.batch.writes.len) catch return error.TransactionTooLarge;
        const out = try alloc.alloc(distributed_txn.TableCommitRequest, self.tables.len);
        errdefer alloc.free(out);
        const writes = try alloc.alloc(db_mod.types.TransactionWrite, write_count);
        var offset: usize = 0;
        for (self.tables, 0..) |table, i| {
            const table_writes = writes[offset..][0..table.batch.writes.len];
            for (table.batch.writes, table_writes) |write, *converted| converted.* = .{
                .key = write.key,
                .value = write.value,
                .json_null_fields = write.json_null_fields,
                .prepared_domain = write.prepared_domain,
            };
            offset += table_writes.len;
            out[i] = .{
                .table_name = self.physicalName(table.table_name),
                .trigger_receipts = if (table.trigger_receipts) |owner| owner.value else &.{},
                .document_revision_handoffs = if (table.document_revisions) |owner| owner.value else &.{},
                .sql_persistence = for (self.catalog_bindings.items) |binding| {
                    if (std.mem.eql(u8, binding.logical, table.table_name) or std.mem.eql(u8, binding.physical, table.table_name)) break binding.sql_persistence;
                } else null,

                .source_producer = .ordinary,
                .bound_catalog_incarnation = self.bound_catalog_incarnation,
                .commit_requires = db_mod.types.CommitRequires.strongest(table.commit_requires_floor, table.batch.req.commit_requires orelse self.commit_requires),
                .default_commit_requires = self.commit_requires,
                .schema_version = table.schema_version,
                .relational_schema_version = table.relational_schema_version,
                .relational_integrity_generation_set = if (table.conflict_guards) |guards| guards.value.generation_set else null,
                .integrity_commands = if (table.conflict_guards) |guards| guards.value.commands else &.{},
                .range_guards = if (table.range_guards) |guards| guards.value else &.{},
                .writes = table_writes,
                .deletes = table.batch.deletes,
                .transforms = table.batch.transforms,
                .graph_writes = table.batch.graph_writes,
                .graph_deletes = table.batch.graph_deletes,
                .predicates = table.predicates.items,
            };
        }
        return .{ .tables = out, .writes = writes, .allocator = alloc };
    }

    /// Rollback undoes staged mutations, not observations already exposed to
    /// the client. Retain range dependencies across savepoint rollback so a
    /// later write cannot evade serializable validation using an undone read.
    pub fn retainRangeGuards(self: *OwnedTransactionCommitRequest, alloc: std.mem.Allocator, other: *const OwnedTransactionCommitRequest) !void {
        for (other.tables) |table| if (table.range_guards) |guards| {
            const physical = other.physicalName(table.table_name);
            const persistence = for (other.catalog_bindings.items) |binding| {
                if (std.mem.eql(u8, binding.logical, table.table_name)) break binding.sql_persistence;
            } else null;
            try self.bindWithPersistence(alloc, table.table_name, physical, persistence);
            const existing = for (self.tables) |*entry| {
                if (std.mem.eql(u8, self.physicalName(entry.table_name), physical)) break entry;
            } else null;
            if (existing) |entry| {
                if (entry.schema_version != null and table.schema_version != null and entry.schema_version != table.schema_version) return error.CatalogGenerationChanged;
                try entry.mergeAuthorization(.{ .table_name = table.table_name, .range_guards = guards, .sql_authorization = table.sql_authorization });
                try entry.mergeRangeGuards(alloc, guards.value);
                if (entry.schema_version == null) entry.schema_version = table.schema_version;
            } else try appendTable(alloc, self, .{ .table_name = table.table_name, .schema_version = table.schema_version, .relational_schema_version = table.relational_schema_version, .range_guards = guards, .sql_authorization = table.sql_authorization });
        };
    }
};

pub const ExecutionPlan = struct {
    pub const Publication = struct { version: DurableSessionStore.Version, guard: Mutation };
    const Parsed = std.json.Parsed([]const distributed_txn.TableCommitRequest);
    arena: *std.heap.ArenaAllocator,
    value: []const distributed_txn.TableCommitRequest,
    // Only the caller that durably chose this plan can prove a first dispatch.
    publication: ?ExecutionPlan.Publication = null,

    pub fn firstAttempt(self: @This()) bool {
        return self.publication != null;
    }

    pub fn deinit(self: @This()) void {
        (Parsed{ .arena = self.arena, .value = self.value }).deinit();
    }
};

pub const CatalogExecutionPlan = struct {
    tables: []const distributed_txn.TableCommitRequest,
    catalog: @import("../metadata/restore_staging.zig").Plan,
};

pub fn parseExecutionPlan(alloc: std.mem.Allocator, bytes: []const u8) !ExecutionPlan {
    if (executionPlanIsCatalog(bytes)) {
        const parsed = try parseCatalogExecutionPlan(alloc, bytes);
        return .{ .arena = parsed.arena, .value = parsed.value.tables };
    }
    const parsed = try std.json.parseFromSlice([]const distributed_txn.TableCommitRequest, alloc, bytes, .{ .allocate = .alloc_always });
    return .{ .arena = parsed.arena, .value = parsed.value };
}

fn executionPlanIsCatalog(bytes: []const u8) bool {
    const content = std.mem.trimStart(u8, bytes, " \t\r\n");
    return content.len != 0 and content[0] == '{';
}

pub fn parseCatalogExecutionPlan(alloc: std.mem.Allocator, bytes: []const u8) !std.json.Parsed(CatalogExecutionPlan) {
    return std.json.parseFromSlice(CatalogExecutionPlan, alloc, bytes, .{ .allocate = .alloc_always });
}

pub fn encodeCatalogExecutionPlan(alloc: std.mem.Allocator, plan: CatalogExecutionPlan) ![]u8 {
    const Wire = struct {
        value: CatalogExecutionPlan,
        pub fn jsonStringify(self: @This(), stream: anytype) !void {
            try stream.beginObject();
            try stream.objectField("tables");
            try @import("antfly_local_sources").storage_db_relational_integrity_json.write(self.value.tables, stream);
            try stream.objectField("catalog");
            try stream.write(self.value.catalog);
            try stream.endObject();
        }
    };
    return std.json.Stringify.valueAlloc(alloc, Wire{ .value = plan }, .{});
}

pub fn encodeExecutionPlan(alloc: std.mem.Allocator, tables: []const distributed_txn.TableCommitRequest) ![]u8 {
    const Wire = struct {
        tables: []const distributed_txn.TableCommitRequest,
        pub fn jsonStringify(self: @This(), stream: anytype) !void {
            try @import("antfly_local_sources").storage_db_relational_integrity_json.write(self.tables, stream);
        }
    };
    return std.json.Stringify.valueAlloc(alloc, Wire{ .tables = tables }, .{});
}

pub const CommitConflict = struct {
    table_name: []const u8,
    key: []const u8,
    message: []const u8,
    group_id: ?u64 = null,
    phase: ?distributed_txn.ParticipantPhase = null,
    kind: CommitConflictKind = .transaction_conflict,
    reason: ?CommitConflictReason = null,
    retryable: bool = false,
    retry_after_ms: ?u32 = null,
    retry_scope: ?[]const u8 = null,
    expected_version: ?u64 = null,
    current_version: ?u64 = null,
};

/// Stable validation causes survive participant transport and durable abort.
/// They must not be mistaken for transient contention by activation workers.
pub const CommitConflictReason = @import("antfly_local_sources").api_distributed_txn_contract.CommitConflictReason;

pub const CommitConflictKind = enum {
    version_conflict,
    intent_conflict,
    topology_changed,
    participant_unavailable,
    doc_identity_unavailable,
    session_lease_lost,
    transaction_conflict,
    torn_state,
};

pub const BeginRequest = struct {
    prepared_completion_only: bool = false,
    sync_level: db_mod.types.SyncLevel = .propose,
    sql: ?SqlMetadata = null,
};

const notification_types = @import("antfly_local_sources").sql_ast.notifications;

const generation_intents = @import("../metadata/sql/relation_lifecycle/generation_intents.zig");

pub const SqlMetadata = struct {
    catalog_generations: generation_intents.Pending = .{},
    notifications: notification_types.Pending = .{},
    database: []const u8,
    namespace: []const u8,
    isolation: @import("antfly_local_sources").sql_session.Isolation,
    mode: @import("antfly_local_sources").sql_session.ReadMode,
    deferrable: bool = false,
    failed: bool = false,
    accepted_lake_reads: bool = false,
    /// PostgreSQL forbids named two-phase preparation after any TEMP access
    /// in this transaction, even when that access is rolled back to a savepoint.
    temporary_relation_access: bool = false,
    /// Monotonic across savepoints: an isolation change must never invalidate
    /// an already chosen native snapshot, including a scalar-only query.
    characteristics_locked: bool = false,
    search_path_active: ?@import("../pgwire/search_path.zig").Path = null,
    search_path_committed: ?@import("../pgwire/search_path.zig").Path = null,

    pub fn clone(self: SqlMetadata, alloc: std.mem.Allocator) !SqlMetadata {
        const database = try alloc.dupe(u8, self.database);
        errdefer alloc.free(database);
        var out = self;
        out.database = database;
        out.namespace = try alloc.dupe(u8, self.namespace);
        errdefer alloc.free(out.namespace);
        out.notifications = try self.notifications.clone(alloc);
        errdefer out.notifications.deinit(alloc);
        out.catalog_generations = try self.catalog_generations.clone(alloc);
        return out;
    }

    pub fn jsonStringify(self: SqlMetadata, writer: anytype) !void {
        try writer.beginObject();
        inline for (@typeInfo(SqlMetadata).@"struct".field_names) |field| {
            if ((!(comptime std.mem.eql(u8, field, "catalog_generations")) or self.catalog_generations.entries.items.len != 0) and
                (!(comptime std.mem.eql(u8, field, "notifications")) or self.notifications.commands.items.len != 0) and
                (!(comptime std.mem.eql(u8, field, "temporary_relation_access")) or self.temporary_relation_access))
            {
                try writer.objectField(field);
                try writer.write(@field(self, field));
            }
        }
        try writer.endObject();
    }

    pub fn deinit(self: *SqlMetadata, alloc: std.mem.Allocator) void {
        self.catalog_generations.deinit(alloc);
        self.notifications.deinit(alloc);
        alloc.free(self.database);
        alloc.free(self.namespace);
        self.* = undefined;
    }
};

const setting_catalog = @import("antfly_local_sources").sql_setting_catalog;
const SettingEntries = std.ArrayListUnmanaged(setting_catalog.OverlayEntry);

fn deinitSettingEntries(alloc: std.mem.Allocator, entries: *SettingEntries) void {
    for (entries.items) |entry| if (entry.value == .string) alloc.free(entry.value.string);
    entries.deinit(alloc);
    entries.* = .empty;
}

fn cloneSettingEntries(alloc: std.mem.Allocator, entries: []const setting_catalog.OverlayEntry) !SettingEntries {
    var result: SettingEntries = .empty;
    errdefer deinitSettingEntries(alloc, &result);
    for (entries) |entry| try putSettingEntry(alloc, &result, entry);
    return result;
}

fn putSettingEntry(alloc: std.mem.Allocator, entries: *SettingEntries, entry: setting_catalog.OverlayEntry) !void {
    const max_entries = 128;
    const max_value_bytes = 64 * 1024;
    var old_bytes: usize = 0;
    var found = false;
    var total_bytes: usize = 0;
    for (entries.items) |existing| {
        if (existing.value == .string) total_bytes = std.math.add(usize, total_bytes, existing.value.string.len) catch return error.SettingLimitExceeded;
        if (existing.identity.id == entry.identity.id) {
            found = true;
            if (existing.value == .string) old_bytes = existing.value.string.len;
        }
    }
    const new_bytes: usize = if (entry.value == .string) entry.value.string.len else 0;
    const prospective_bytes = std.math.add(usize, total_bytes - old_bytes, new_bytes) catch return error.SettingLimitExceeded;
    if ((!found and entries.items.len >= max_entries) or prospective_bytes > max_value_bytes) return error.SettingLimitExceeded;
    const value: setting_catalog.Value = switch (entry.value) {
        .boolean => |v| .{ .boolean = v },
        .integer => |v| .{ .integer = v },
        .string => |v| .{ .string = try alloc.dupe(u8, v) },
    };
    for (entries.items) |*existing| if (existing.identity.id == entry.identity.id) {
        if (existing.value == .string) alloc.free(existing.value.string);
        existing.* = .{ .identity = entry.identity, .value = value };
        return;
    };
    errdefer if (value == .string) alloc.free(value.string);
    try entries.append(alloc, .{ .identity = entry.identity, .value = value });
}

fn removeSettingEntry(alloc: std.mem.Allocator, entries: *SettingEntries, id: u64) void {
    for (entries.items, 0..) |entry, i| if (entry.identity.id == id) {
        if (entry.value == .string) alloc.free(entry.value.string);
        _ = entries.swapRemove(i);
        return;
    };
}

fn decodeSettingEntries(alloc: std.mem.Allocator, value: std.json.Value) !SettingEntries {
    const array = switch (value) {
        .array => |items| items,
        else => return error.InvalidTransactionSessionRecord,
    };
    if (array.items.len > 128) return error.InvalidTransactionSessionRecord;
    var result: SettingEntries = .empty;
    errdefer deinitSettingEntries(alloc, &result);
    for (array.items) |item| {
        var parsed = std.json.parseFromValue(setting_catalog.OverlayEntry, alloc, item, .{}) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => return error.InvalidTransactionSessionRecord,
        };
        defer parsed.deinit();
        const entry = parsed.value;
        if (entry.identity.id == 0 or entry.identity.generation == 0 or
            (entry.value == .string and (entry.value.string.len > 4096 or !std.unicode.utf8ValidateSlice(entry.value.string))))
            return error.InvalidTransactionSessionRecord;
        for (result.items) |prior| if (prior.identity.id == entry.identity.id) return error.InvalidTransactionSessionRecord;
        putSettingEntry(alloc, &result, entry) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => return error.InvalidTransactionSessionRecord,
        };
    }
    return result;
}

pub const StageReadRequest = struct {
    table_name: []u8,
    key: []u8,
    version: u64,

    pub fn deinit(self: *StageReadRequest, alloc: std.mem.Allocator) void {
        alloc.free(self.table_name);
        alloc.free(self.key);
        self.* = undefined;
    }
};

pub const StageWriteRequest = struct {
    table_name: []u8,
    key: []u8,
    value_json: []u8,

    pub fn deinit(self: *StageWriteRequest, alloc: std.mem.Allocator) void {
        alloc.free(self.table_name);
        alloc.free(self.key);
        alloc.free(self.value_json);
        self.* = undefined;
    }
};

pub const StageDeleteRequest = struct {
    table_name: []u8,
    key: []u8,

    pub fn deinit(self: *StageDeleteRequest, alloc: std.mem.Allocator) void {
        alloc.free(self.table_name);
        alloc.free(self.key);
        self.* = undefined;
    }
};

pub const SessionInfo = struct {
    terminal_abort: bool,
    terminal_commit: bool = false,
    version: DurableSessionStore.Version,
    // Captured before dispatch under the session lock. A later retry rejection
    // cannot prove that an earlier execution of this ID had no durable effect.
    commit_execution_started: bool = false,
    txn_id: db_mod.types.TxnId,
    begin_timestamp: u64,
    sync_level: db_mod.types.SyncLevel,
};

pub const TerminalCommitStatus = enum {
    committed,
    committed_visibility_pending,
    committed_recovery_pending,

    pub fn text(self: TerminalCommitStatus) []const u8 {
        return @tagName(self);
    }
};

/// Repair debt is terminal and independently persisted. Only live propagation
/// or retryable visibility debt keeps coordinator recovery active.
pub fn terminalCommitStatusForOutcome(
    propagation_pending: bool,
    visibility_pending: bool,
    visibility_retry_pending: bool,
    visibility_repair_required: bool,
) TerminalCommitStatus {
    if (propagation_pending) return .committed_recovery_pending;
    // `visibility_pending` predates the retry/repair split and remains the
    // compatibility contract for private adapters and mixed-version callers.
    // Treat a parent-only outcome as live retryable debt rather than silently
    // upgrading it to committed. A classified repair-only result remains
    // terminal and is rendered by terminalCommitResponseStatus below.
    const unclassified_visibility_pending = visibility_pending and
        !visibility_retry_pending and
        !visibility_repair_required;
    if (visibility_retry_pending or unclassified_visibility_pending)
        return .committed_visibility_pending;
    return .committed;
}

/// Wire status precedence mirrors operational urgency: unresolved participant
/// recovery, then retryable visibility, then terminal repair debt.
pub fn terminalCommitResponseStatus(status: TerminalCommitStatus, repair_required: bool) []const u8 {
    if (status != .committed) return status.text();
    if (repair_required) return "committed_repair_required";
    return status.text();
}

/// A durable API-level terminal result. The coordinator location is retained
/// with the result because current routing may change after the topology fence
/// is released. `coordinator_table_name` is owned by this value.
pub const TerminalCommit = struct {
    status: TerminalCommitStatus,
    /// Stored independently from status so rollback binaries continue to read
    /// the established status enum and simply ignore this additive JSON field.
    repair_required: bool = false,
    coordinator_group_id: ?u64 = null,
    coordinator_table_name: ?[]u8 = null,
    coordinator_acknowledged: bool = false,
    /// Exact decision version when supplied by a durable commit receipt.
    /// Omitted for existing records and producers that do not provide it.
    commit_version: ?u64 = null,

    pub fn deinit(self: *TerminalCommit, alloc: std.mem.Allocator) void {
        if (self.coordinator_table_name) |table_name| alloc.free(table_name);
        self.* = undefined;
    }

    pub fn clone(self: TerminalCommit, alloc: std.mem.Allocator) !TerminalCommit {
        return .{
            .status = self.status,
            .repair_required = self.repair_required,
            .coordinator_group_id = self.coordinator_group_id,
            .coordinator_table_name = if (self.coordinator_table_name) |table_name| try alloc.dupe(u8, table_name) else null,
            .coordinator_acknowledged = self.coordinator_acknowledged,
            .commit_version = self.commit_version,
        };
    }
};

pub const PendingTerminalAcknowledgement = struct {
    txn_id: db_mod.types.TxnId,
    owner_node_id: u64,
    coordinator_group_id: u64,
    coordinator_table_name: []u8,

    pub fn deinit(self: *PendingTerminalAcknowledgement, alloc: std.mem.Allocator) void {
        alloc.free(self.coordinator_table_name);
        self.* = undefined;
    }
};

pub fn deinitPendingTerminalAcknowledgements(
    alloc: std.mem.Allocator,
    acknowledgements: []PendingTerminalAcknowledgement,
) void {
    for (acknowledgements) |*acknowledgement| acknowledgement.deinit(alloc);
    alloc.free(acknowledgements);
}

/// One lease-fenced unit of stable-transaction recovery work. A sealed commit
/// is replayed with its original transaction ID; a terminal commit only needs
/// the coordinator handoff acknowledgement. Both operations are idempotent.
/// Compact seal; row values stay in the existing binary-safe execution plan.
pub const ExternalPreparationSeal = struct {
    admission_head: u64,
    admitted_at_ns: u64,
    standalone: bool,
    identity: @import("../system_catalog/prepared_transactions.zig").Identity,
    actor: @import("../system_catalog/prepared_transactions.zig").Actor,
    participants: []const @import("../system_catalog/prepared_transactions.zig").Participant,

    pub fn admissionRequest(self: @This()) !@import("../system_catalog/prepared_transactions.zig").Mutation {
        var incarnation: [16]u8 = undefined;
        _ = std.fmt.hexToBytes(&incarnation, &self.identity.metadata_incarnation) catch return error.InvalidTransactionSessionRecord;
        const request: @import("../system_catalog/prepared_transactions.zig").Mutation = .{ .notification_control = if (self.identity.notification_plan != null) true else null, .admission = .{ .nonce = self.identity.operation_id, .head = self.admission_head }, .incarnation = incarnation, .actor = self.actor, .standalone = self.standalone, .database_id = self.identity.database_id, .operation = .{ .admit = .{ .identity = self.identity, .participants = self.participants, .admitted_at_ns = self.admitted_at_ns } } };
        try request.validate();
        return request;
    }

    pub fn validate(self: @This(), a: std.mem.Allocator, session: Session, plan: ?[]const u8) !void {
        const registry = @import("../system_catalog/prepared_transactions.zig");
        _ = try self.admissionRequest();
        try self.identity.validate();
        try self.actor.validate();
        if (!self.actor.principal().eql(self.identity.principal)) return error.InvalidTransactionSessionRecord;
        if (self.identity.session == null or !std.meta.eql(self.identity.session.?.connection_id, session.connection_id) or
            !std.mem.eql(u8, &self.identity.txn_id, &session.txn_id) or self.identity.begin_timestamp != session.begin_timestamp or
            !std.mem.eql(u8, &self.identity.manifest_digest, &try registry.manifestDigest(a, self.participants, self.identity.metadata_incarnation))) return error.InvalidTransactionSessionRecord;
        const pending = if (session.sql) |metadata| metadata.notifications.commands.items else &.{};
        if (self.identity.notification_plan) |notifications| {
            if (!notification_types.commandsEqual(notifications.commands, pending)) return error.InvalidTransactionSessionRecord;
        } else if (pending.len != 0) return error.InvalidTransactionSessionRecord;
        const private = self.identity.kind == .session_commit;
        if (private) {
            const effective_sync = if (session.sync_level == .propose) db_mod.types.SyncLevel.write else session.sync_level;
            if (session.external_preparation_prepared or self.identity.commit_sync_level != effective_sync) return error.InvalidTransactionSessionRecord;
            if (session.external_decision) |decision| {
                if (decision.version <= self.identity.begin_timestamp) return error.InvalidTransactionSessionRecord;
                if (decision.status == .committed) {
                    const terminal = session.terminal_commit orelse return error.InvalidTransactionSessionRecord;
                    if (session.terminal_abort or decision.sync_level != self.identity.commit_sync_level or terminal.commit_version != decision.version or
                        terminal.coordinator_group_id != null or terminal.coordinator_table_name != null or !terminal.coordinator_acknowledged or
                        (terminal.repair_required and terminal.status != .committed) or (session.external_receipt_acknowledged and terminal.status != .committed)) return error.InvalidTransactionSessionRecord;
                } else if (!session.terminal_abort or session.terminal_commit != null) return error.InvalidTransactionSessionRecord;
            } else if (session.terminal_commit != null or (session.terminal_abort and !session.external_receipt_acknowledged)) return error.InvalidTransactionSessionRecord;
            if ((session.terminal_commit != null or session.terminal_abort) and (session.execution_plan != null or session.staged != null or session.native_sql_cut != null or session.savepoints.count() != 0 or session.read_snapshots.count() != 0 or session.lake_cuts.count() != 0)) return error.InvalidTransactionSessionRecord;
        } else if (session.terminal_commit != null or session.external_decision != null) return error.InvalidTransactionSessionRecord;
        if (plan) |bytes| {
            if (!std.mem.eql(u8, &self.identity.plan_digest, &try registry.planDigest(bytes))) return error.InvalidTransactionSessionRecord;
            const participant = self.identity.catalog_participant;
            if (executionPlanIsCatalog(bytes) != (participant != null)) return error.InvalidTransactionSessionRecord;
            if (participant) |catalog| {
                var owned = try parseCatalogExecutionPlan(a, bytes);
                defer owned.deinit();
                const requested = owned.value.catalog;
                const binding = requested.catalog_binding orelse return error.InvalidTransactionSessionRecord;
                if (!std.mem.eql(u8, &requested.id, &catalog.staging_id) or
                    !std.mem.eql(u8, &try requested.digest(a), &catalog.plan_digest))
                    return error.InvalidTransactionSessionRecord;
                try binding.requireIdentity(a, binding.fence.group, self.identity, catalog);
                const metadata = session.sql orelse return error.InvalidTransactionSessionRecord;
                if (!std.mem.eql(u8, &metadata.catalog_generations.digest(), &catalog.intent_digest))
                    return error.InvalidTransactionSessionRecord;
            }
        } else if (!session.external_preparation_prepared and !session.terminal_abort and !(private and session.terminal_commit != null)) {
            // A row-free durable native envelope can inspect only the sealed
            // metadata decision graph. Execution-plan consumers separately
            // require the admitted process-local capsule before preparing.
            const reference = session.volatile_payload orelse return error.InvalidTransactionSessionRecord;
            try reference.validate();
            if (session.volatile_payload_available or !session.commit_execution_started or session.commit_body_digest == null)
                return error.InvalidTransactionSessionRecord;
        }
    }
};

pub const PendingSessionRecovery = union(enum) {
    external_preparation: struct { txn_id: db_mod.types.TxnId, seal: []u8, prepared: bool, aborted: bool, committed: bool, volatile_payload_available: bool = true },
    commit: struct {
        txn_id: db_mod.types.TxnId,
        begin_timestamp: u64,
        sync_level: db_mod.types.SyncLevel,
        /// A prior visibility attempt reached durable repair debt. Recovery
        /// should finish participant propagation at write durability without
        /// polling the failed provider again.
        repair_required: bool = false,
        /// The source returned only a terminal error, so coordinator metadata
        /// must be recovered without re-entering the failed visibility barrier.
        repair_handoff_needs_coordinator: bool = false,
        request: ?OwnedTransactionCommitRequest,
        execution_plan: ?[]u8 = null,
    },
    acknowledge: PendingTerminalAcknowledgement,

    pub fn deinit(self: *PendingSessionRecovery, alloc: std.mem.Allocator) void {
        switch (self.*) {
            .external_preparation => |value| alloc.free(value.seal),
            .commit => |*value| {
                if (value.request) |*request| request.deinit(alloc);
                if (value.execution_plan) |bytes| alloc.free(bytes);
            },
            .acknowledge => |*value| value.deinit(alloc),
        }
        self.* = undefined;
    }
};

pub const SessionStatus = struct {
    txn_id: db_mod.types.TxnId,
    owner_node_id: u64,
    begin_timestamp: u64,
    last_touched_timestamp: u64,
    lease_expires_at: u64,
    sync_level: db_mod.types.SyncLevel,
    disposition: SessionDisposition = .active,
    staged_table_count: usize,
    staged_read_count: usize,
    staged_write_count: usize,
    staged_delete_count: usize,
    read_snapshot_count: usize,
    savepoint_count: usize,
    savepoint_limit: ?usize = null,
    remaining_savepoints: ?usize = null,
    durable: bool,
};

/// A read-only reconciliation result. `outcome_unknown` never authorizes a
/// fresh commit with a different transaction ID or an inferred abort.
pub const SessionDisposition = enum {
    active,
    prepared,
    outcome_unknown,
    committed,
    committed_pending,
    committed_repair_required,
    aborted,
};

pub const StageReadSnapshot = struct {
    table_name: []const u8,
    key: []const u8,
    version: u64,
    document_json: ?[]const u8 = null,
};

pub const SessionReadSnapshot = struct {
    table_name: []u8,
    key: []u8,
    version: u64,
    document_json: ?[]u8 = null,

    pub fn clone(self: SessionReadSnapshot, alloc: std.mem.Allocator) !SessionReadSnapshot {
        return ownReadSnapshot(alloc, self.stage());
    }

    pub fn deinit(self: *SessionReadSnapshot, alloc: std.mem.Allocator) void {
        alloc.free(self.table_name);
        alloc.free(self.key);
        if (self.document_json) |document_json| alloc.free(document_json);
        self.* = undefined;
    }

    pub fn stage(self: SessionReadSnapshot) StageReadSnapshot {
        return .{
            .table_name = self.table_name,
            .key = self.key,
            .version = self.version,
            .document_json = self.document_json,
        };
    }
};

pub const SessionTableDetail = struct {
    storage_name: ?[]const u8 = null,
    has_range_reads: bool = false,
    table_name: []u8,
    staged_read_count: usize,
    staged_write_count: usize,
    staged_delete_count: usize,
    staged_predicate_count: usize,

    pub fn deinit(self: *SessionTableDetail, alloc: std.mem.Allocator) void {
        alloc.free(self.table_name);
        if (self.storage_name) |name| alloc.free(name);
        self.* = undefined;
    }
};

pub const SessionDetails = struct {
    prepared_completion_only: bool = false,
    graph_receipt: bool = false,
    status: SessionStatus,
    connection_id: ?[32]u8 = null,
    tables: []SessionTableDetail,
    read_snapshots: []SessionReadSnapshot,
    savepoint_ids: []u64,
    catalog_bindings: []CatalogBinding = &.{},

    pub fn deinit(self: *SessionDetails, alloc: std.mem.Allocator) void {
        for (self.catalog_bindings) |binding| {
            alloc.free(binding.logical);
            alloc.free(binding.physical);
        }
        alloc.free(self.catalog_bindings);
        for (self.tables) |*table| table.deinit(alloc);
        if (self.tables.len > 0) alloc.free(self.tables);
        for (self.read_snapshots) |*snapshot| snapshot.deinit(alloc);
        if (self.read_snapshots.len > 0) alloc.free(self.read_snapshots);
        if (self.savepoint_ids.len > 0) alloc.free(self.savepoint_ids);
        self.* = undefined;
    }
};

pub const SessionStatusResponse = struct {
    transaction_id: []const u8,
    owner_node_id: u64,
    begin_timestamp: u64,
    last_touched_timestamp: u64,
    lease_expires_at: u64,
    lease_state: []const u8,
    sync_level: []const u8,
    disposition: []const u8,
    staged_table_count: usize,
    staged_read_count: usize,
    staged_write_count: usize,
    staged_delete_count: usize,
    read_snapshot_count: usize,
    savepoint_count: usize,
    savepoint_limit: ?usize = null,
    remaining_savepoints: ?usize = null,
    durable: bool,
};

pub const SessionReadSnapshotResponse = struct {
    table: []const u8,
    key: []const u8,
    version: u64,
    document: ?std.json.Value = null,
};

pub const SessionTableDetailResponse = struct {
    table: []const u8,
    staged_read_count: usize,
    staged_write_count: usize,
    staged_delete_count: usize,
    staged_predicate_count: usize,
};

pub const SessionDetailsResponse = struct {
    transaction_id: []const u8,
    owner_node_id: u64,
    begin_timestamp: u64,
    last_touched_timestamp: u64,
    lease_expires_at: u64,
    lease_state: []const u8,
    sync_level: []const u8,
    disposition: []const u8,
    staged_table_count: usize,
    staged_read_count: usize,
    staged_write_count: usize,
    staged_delete_count: usize,
    read_snapshot_count: usize,
    savepoint_count: usize,
    savepoint_limit: ?usize = null,
    remaining_savepoints: ?usize = null,
    durable: bool,
    tables: []const SessionTableDetailResponse,
    read_snapshots: []const SessionReadSnapshotResponse,
    savepoint_ids: []const u64,
};

pub const SessionListResponse = struct {
    session_count: usize,
    lease_held_count: usize,
    lease_expired_count: usize,
    sessions: []const SessionStatusResponse,
};

pub const SessionCleanupResponse = struct {
    removed: usize,
    cutoff_ns: u64,
};

pub const BeginResponse = struct {
    transaction_id: []const u8,
    begin_timestamp: u64,
    sync_level: []const u8,
};

pub const TransactionStatusResponse = struct {
    status: []const u8,
    transaction_id: []const u8,
};

pub const SavepointStatusResponse = struct {
    status: []const u8,
    transaction_id: []const u8,
    savepoint_id: u64,
};

pub const StageReadSnapshotResponse = struct {
    table: []const u8,
    key: []const u8,
    version: []const u8,
    document: std.json.Value,
};

pub const StageReadResponse = struct {
    status: []const u8,
    transaction_id: []const u8,
    snapshot: StageReadSnapshotResponse,
};

pub const CommitConflictParticipantResponse = struct {
    group_id: ?u64 = null,
    phase: ?[]const u8 = null,
};

pub const CommitConflictResponse = struct {
    table: []const u8,
    key: []const u8,
    message: []const u8,
    kind: []const u8,
    reason: ?CommitConflictReason = null,
    retryable: bool,
    retry_after_ms: ?u32 = null,
    retry_scope: ?[]const u8 = null,
    expected_version: ?u64 = null,
    current_version: ?u64 = null,
    participant: ?CommitConflictParticipantResponse = null,
};

pub const CommitTablesResponse = std.json.ArrayHashMap(batch_api.BatchResult);

pub const CommitResponse = struct {
    status: []const u8,
    conflict: ?CommitConflictResponse = null,
    tables: ?CommitTablesResponse = null,
};

pub const MultiBatchResponse = struct {
    status: []const u8 = "committed",
    tables: CommitTablesResponse,
};

pub const SessionCommitResponse = struct {
    status: []const u8,
    transaction_id: []const u8,
    conflict: ?CommitConflictResponse = null,
    tables: ?CommitTablesResponse = null,
};

pub const SavepointInfo = struct {
    txn_id: db_mod.types.TxnId,
    savepoint_id: u64,
};

pub const Savepoint = struct {
    id: u64,
    notification_count: usize = 0,
    generation_count: usize = 0,
    name: ?[]u8 = null,
    snapshot: OwnedTransactionCommitRequest,
    transaction_mode: ?@import("antfly_local_sources").sql_session.ReadMode = null,
    transaction_deferrable: ?bool = null,
    search_path_active: ?@import("../pgwire/search_path.zig").Path = null,
    search_path_committed: ?@import("../pgwire/search_path.zig").Path = null,
    read_snapshots: std.StringArrayHashMapUnmanaged(SessionReadSnapshot) = .empty,
    setting_active: SettingEntries = .empty,
    setting_committed: SettingEntries = .empty,

    pub fn deinit(self: *Savepoint, alloc: std.mem.Allocator) void {
        if (self.name) |name| alloc.free(name);
        self.snapshot.deinit(alloc);
        deinitReadSnapshotMap(alloc, &self.read_snapshots);
        deinitSettingEntries(alloc, &self.setting_active);
        deinitSettingEntries(alloc, &self.setting_committed);
        self.* = undefined;
    }

    pub fn clone(self: Savepoint, alloc: std.mem.Allocator) !Savepoint {
        const name = if (self.name) |name| try alloc.dupe(u8, name) else null;
        const snapshot = self.snapshot.clone(alloc) catch |err| {
            if (name) |value| alloc.free(value);
            return err;
        };
        var out: Savepoint = .{
            .id = self.id,
            .notification_count = self.notification_count,
            .generation_count = self.generation_count,
            .name = name,
            .snapshot = snapshot,
            .transaction_mode = self.transaction_mode,
            .transaction_deferrable = self.transaction_deferrable,
            .search_path_active = self.search_path_active,
            .search_path_committed = self.search_path_committed,
        };
        errdefer out.deinit(alloc);
        out.read_snapshots = try cloneReadSnapshotMap(alloc, self.read_snapshots);
        out.setting_active = try cloneSettingEntries(alloc, self.setting_active.items);
        out.setting_committed = try cloneSettingEntries(alloc, self.setting_committed.items);
        return out;
    }
};

const volatile_capsules = @import("volatile_transaction_capsules.zig");

pub const SqlAbortAcknowledgement = enum {
    rollback,
    conflict,
    row_policy_denied,
    unavailable,

    pub fn fromCause(cause: ?error{ RowPolicyDenied, StorageReadTemporarilyUnavailable }) SqlAbortAcknowledgement {
        return switch (cause orelse return .conflict) {
            error.RowPolicyDenied => .row_policy_denied,
            error.StorageReadTemporarilyUnavailable => .unavailable,
        };
    }

    pub fn diagnostic(self: SqlAbortAcknowledgement) ?anyerror {
        return switch (self) {
            .rollback => null,
            .conflict => error.SqlWriteConflict,
            .row_policy_denied => error.RowPolicyDenied,
            .unavailable => error.StorageReadTemporarilyUnavailable,
        };
    }
};

pub const SqlStoredDecision = struct {
    disposition: SessionDisposition,
    abort_acknowledgement: SqlAbortAcknowledgement,
};

pub const Session = struct {
    sql_abort_acknowledgement: SqlAbortAcknowledgement = .conflict,
    prepared_completion_only: bool = false,
    graph_receipt: bool = false,
    graph_abort_tables: []SessionTableDetail = &.{},
    graph_admission_growth: usize = 0,
    // Owned sessions retain their allocator across registry publication and eviction.
    owner_alloc: ?std.mem.Allocator = null,
    volatile_payload: ?volatile_capsules.Reference = null,
    /// A cold durable envelope retains decision/control facts but never gains
    /// permission to re-prepare missing process-local row payloads.
    volatile_payload_available: bool = true,
    sql: ?SqlMetadata = null,
    sequence_epoch: u64 = 1,
    sequence_state: @import("antfly_local_sources").sql_sequence_session.Snapshot = .{},
    txn_id: db_mod.types.TxnId,
    owner_node_id: u64,
    /// Stable authenticated subject that created this session. `null` is the
    /// anonymous principal used only when authentication is disabled. The
    /// binding is immutable across node-owner lease transfers.
    principal: ?[]u8 = null,
    /// Immutable owner connection for HTTP-created transactions. A caller
    /// cannot bypass its active fence by omitting connection_id later.
    connection_id: ?[32]u8 = null,
    begin_timestamp: u64,
    last_touched_timestamp: u64,
    sync_level: db_mod.types.SyncLevel,
    staged: ?OwnedTransactionCommitRequest = null,
    listing_summary_tail: ?[]u8 = null,
    /// Digest of the optional body supplied to the first commit attempt. Once
    /// present, the effective request is sealed in `staged`; retries must carry
    /// the same body (or omit it if the first attempt omitted it).
    commit_body_digest: ?[32]u8 = null,
    /// Set durably after API/schema/read-set validation and immediately before
    /// invoking 2PC. Once true, background maintenance owns completion even if
    /// the initiating process disappears.
    commit_execution_started: bool = false,
    /// Exact internally prepared participant input, sealed atomically with
    /// execution-start. Recovery must never re-plan against later row state.
    execution_plan: ?[]u8 = null,
    /// Separate, immutable purpose seal. Never an ordinary commit request.
    external_preparation: ?[]u8 = null,
    external_preparation_prepared: bool = false,
    external_receipt_acknowledged: bool = false,
    /// Exact resolved metadata decision for a private COMMIT, including abort.
    /// This remains until its delivery ACK and connection detachment finish.
    external_decision: ?@import("../system_catalog/prepared_transactions.zig").Decision = null,
    /// Persisted before releasing the retained coordinator's topology fence.
    terminal_commit: ?TerminalCommit = null,
    /// Definite post-proposal conflict/abort, retained until any owning
    /// HTTP connection has detached. Never report it as a committed result.
    terminal_abort: bool = false,
    terminal_abort_reason: ?CommitConflictReason = null,
    /// Private immutable accepted-lake cuts. Savepoint rollback deliberately
    /// leaves these transaction read cuts intact.
    // Live capability is intentionally absent from durable session encoding.
    // A locked session recovered without it fails rather than recapturing.
    native_sql_cut: ?*@import("sql/execution/retained_snapshot.zig").Shared = null,
    lake_cuts: std.StringArrayHashMapUnmanaged(SessionReadSnapshot) = .empty,
    read_snapshots: std.StringArrayHashMapUnmanaged(SessionReadSnapshot) = .empty,
    setting_active: SettingEntries = .empty,
    setting_committed: SettingEntries = .empty,
    next_savepoint_id: u64 = 1,
    /// Derived from existing savepoint identities during clone/decode and
    /// maintained by savepoint mutations. Never encoded as another counter.
    current_subtransaction: u64 = 0,
    savepoints: std.AutoHashMapUnmanaged(u64, Savepoint) = .empty,

    fn clearListingSummary(self: *Session, alloc: std.mem.Allocator) void {
        if (self.listing_summary_tail) |json| alloc.free(json);
        self.listing_summary_tail = null;
    }

    fn refreshListingSummary(self: *Session, alloc: std.mem.Allocator, limit: ?usize) !void {
        self.clearListingSummary(alloc);
        const staged = self.staged orelse return;
        // Cap both the input traversal and scratch allocations on the write path.
        // Large records can still be listed from their canonical staged request.
        const budget: usize = @min(if (limit) |max| max / 2 else 32 << 10, 32 << 10);
        var remaining = budget -| 256;
        remaining -|= staged.read_set.len *| 32;
        if (remaining == 0) return;
        for (staged.read_set) |read| {
            remaining -|= read.table_name.len *| 6;
            if (remaining == 0) return;
        }
        for (staged.tables) |table| {
            remaining -|= 256 +| (table.table_name.len *| 6);
            if (remaining == 0) return;
        }
        for (staged.catalog_bindings.items) |binding| {
            remaining -|= 64 +| ((binding.logical.len +| binding.physical.len) *| 6);
            if (remaining == 0) return;
        }
        const tables = try sessionTableDetails(alloc, staged, false);
        defer {
            for (tables) |*table| table.deinit(alloc);
            if (tables.len != 0) alloc.free(tables);
        }
        const json = try std.json.Stringify.valueAlloc(alloc, SessionListingSummary{
            .counts = stagedCounts(self),
            .tables = tables,
            .bindings = staged.catalog_bindings.items,
        }, .{});
        defer alloc.free(json);
        if (std.base64.standard.Encoder.calcSize(json.len) <= budget)
            self.listing_summary_tail = try encodeListingSummaryBytes(alloc, json);
    }

    pub fn info(self: Session) SessionInfo {
        return .{
            .version = DurableSessionStore.Version.of(self),
            .terminal_abort = self.terminal_abort,
            .terminal_commit = self.terminal_commit != null,
            .txn_id = self.txn_id,
            .commit_execution_started = self.commit_execution_started,
            .begin_timestamp = self.begin_timestamp,
            .sync_level = self.sync_level,
        };
    }

    fn deinitOwned(self: *Session) void {
        self.deinit(self.owner_alloc.?);
    }

    pub fn deinit(self: *Session, fallback: std.mem.Allocator) void {
        const alloc = self.owner_alloc orelse fallback;
        for (self.graph_abort_tables) |*table| table.deinit(alloc);
        alloc.free(self.graph_abort_tables);
        if (self.native_sql_cut) |cut| cut.release();
        self.sequence_state.deinit(alloc);
        if (self.sql) |*metadata| metadata.deinit(alloc);
        if (self.principal) |principal| alloc.free(principal);
        if (self.staged) |*staged| staged.deinit(alloc);
        self.clearListingSummary(alloc);
        if (self.execution_plan) |bytes| alloc.free(bytes);
        if (self.external_preparation) |bytes| alloc.free(bytes);
        if (self.terminal_commit) |*terminal| terminal.deinit(alloc);
        deinitReadSnapshotMap(alloc, &self.lake_cuts);
        deinitReadSnapshotMap(alloc, &self.read_snapshots);
        deinitSettingEntries(alloc, &self.setting_active);
        deinitSettingEntries(alloc, &self.setting_committed);
        var it = self.savepoints.iterator();
        while (it.next()) |entry| entry.value_ptr.deinit(alloc);
        self.savepoints.deinit(alloc);
        self.* = undefined;
    }

    pub fn clone(self: Session, alloc: std.mem.Allocator) !Session {
        var out: Session = .{
            .volatile_payload = self.volatile_payload,
            .volatile_payload_available = self.volatile_payload_available,
            .graph_receipt = self.graph_receipt,
            .owner_alloc = alloc,
            .txn_id = self.txn_id,
            .owner_node_id = self.owner_node_id,
            .principal = if (self.principal) |principal| try alloc.dupe(u8, principal) else null,
            .connection_id = self.connection_id,
            .prepared_completion_only = self.prepared_completion_only,
            .sql_abort_acknowledgement = self.sql_abort_acknowledgement,
            .begin_timestamp = self.begin_timestamp,
            .last_touched_timestamp = self.last_touched_timestamp,
            .sync_level = self.sync_level,
            .next_savepoint_id = self.next_savepoint_id,
            .commit_body_digest = self.commit_body_digest,
            .commit_execution_started = self.commit_execution_started,
            .external_preparation_prepared = self.external_preparation_prepared,
            .external_receipt_acknowledged = self.external_receipt_acknowledged,
            .external_decision = self.external_decision,
            .terminal_abort = self.terminal_abort,
            .terminal_abort_reason = self.terminal_abort_reason,
        };
        errdefer out.deinit(alloc);
        out.native_sql_cut = if (self.native_sql_cut) |cut| cut.retain() else null;
        out.sequence_epoch = self.sequence_epoch;
        out.sequence_state = try self.sequence_state.clone(alloc);
        out.graph_abort_tables = try cloneTableDetails(alloc, self.graph_abort_tables);
        if (self.sql) |metadata| out.sql = try metadata.clone(alloc);
        if (self.staged) |staged| out.staged = try staged.clone(alloc);
        if (self.listing_summary_tail) |json| out.listing_summary_tail = try alloc.dupe(u8, json);
        if (self.execution_plan) |bytes| out.execution_plan = try alloc.dupe(u8, bytes);
        if (self.external_preparation) |bytes| out.external_preparation = try alloc.dupe(u8, bytes);
        if (self.terminal_commit) |terminal| out.terminal_commit = try terminal.clone(alloc);
        out.lake_cuts = try cloneReadSnapshotMap(alloc, self.lake_cuts);
        out.read_snapshots = try cloneReadSnapshotMap(alloc, self.read_snapshots);
        out.setting_active = try cloneSettingEntries(alloc, self.setting_active.items);
        out.setting_committed = try cloneSettingEntries(alloc, self.setting_committed.items);
        try out.savepoints.ensureUnusedCapacity(alloc, self.savepoints.count());
        var it = self.savepoints.iterator();
        while (it.next()) |entry| {
            out.savepoints.putAssumeCapacity(entry.key_ptr.*, try entry.value_ptr.clone(alloc));
            out.current_subtransaction = @max(out.current_subtransaction, entry.key_ptr.*);
        }
        return out;
    }
};

const RoutingContext = @import("antfly_local_sources").api_operation.RequestContext;

pub const DurableSessionStore = struct {
    volatile_payloads: volatile_capsules.Store,

    alloc: std.mem.Allocator,
    backend: Backend,
    // Local facade state and fallback for providers whose write transactions
    // already serialize internally. mutationMutex selects shared backend gates.
    mutation_mutex: std.atomic.Mutex = .unlocked,
    reserved_registrations: std.atomic.Value(usize) = .init(0),
    registration_issuer: ?u64 = null,

    // Captured before this store allocates its first reference. Only earlier
    // references can belong to a dead process; never sweep a live collector.
    observation_ack_mutex: std.atomic.Mutex = .unlocked,
    observation_ack_cursor: ?ObservationAckPosition = null,
    registration_recovery_before: ?u64 = null,
    registration_locks: [64]std.atomic.Mutex = @splat(.unlocked),
    sql_abort_proof_position: ?SqlAbortProofPosition = null,
    fail_writes_for_test: bool = false,
    fail_command_finish_for_test: std.atomic.Value(bool) = .init(false),
    fail_command_recovery_once_for_test: std.atomic.Value(bool) = .init(false),
    fail_savepoint_publication_for_test: ?enum { session, source } = null,
    fail_connection_publication_for_test: ?enum { source, resources, connection } = null,
    fail_lease_transition_after_session_write_for_test: bool = false,

    routing_access: ?RoutingAccess = null,

    pub const RoutingAccess = struct {
        ptr: *anyopaque,
        vtable: *const VTable,
        boundary_dispatch: Boundary.Dispatch = Boundary.local_dispatch,

        pub const VTable = struct {
            read: *const fn (*anyopaque, std.mem.Allocator, RoutingContext) anyerror!backend_erased.ReadTxn,
            write: *const fn (*anyopaque, std.mem.Allocator, RoutingContext) anyerror!backend_erased.WriteTxn,
        };
        const Boundary = @import("antfly_local_sources").runtime_callback_abi.Boundary(VTable);

        fn read(self: RoutingAccess, alloc: std.mem.Allocator, context: RoutingContext) !backend_erased.ReadTxn {
            return Boundary.call("read", self.boundary_dispatch, self.vtable.read, .{ self.ptr, alloc, context });
        }
        fn write(self: RoutingAccess, alloc: std.mem.Allocator, context: RoutingContext) !backend_erased.WriteTxn {
            return Boundary.call("write", self.boundary_dispatch, self.vtable.write, .{ self.ptr, alloc, context });
        }
    };

    pub fn beginRoutingRead(self: *DurableSessionStore, store: *backend_erased.Store, context: RoutingContext) !backend_erased.ReadTxn {
        try context.ensureActive();
        if (self.routing_access) |access| return access.read(store.allocator, context);
        return store.beginRead();
    }

    pub fn deinitVolatilePayloads(self: *DurableSessionStore) void {
        self.volatile_payloads.deinit();
    }

    fn encodeDurableSession(self: *DurableSessionStore, session: Session, max_record_bytes: ?usize) ![]u8 {
        if (!sessionContainsNativePayload(session) and session.volatile_payload == null) return encodeSessionRecordWithListingLimit(self.alloc, session, max_record_bytes orelse std.math.maxInt(usize));
        var projection = session;
        if (session.volatile_payload_available) {
            const generations = try nativePayloadGenerations(session, self.alloc);
            const full = try encodeSessionRecordUnchecked(self.alloc, session);
            if (max_record_bytes) |limit| if (full.len > limit) {
                self.alloc.free(full);
                return error.SessionRecordTooLarge;
            };
            projection.volatile_payload = try self.volatile_payloads.prepareOwned(session.txn_id, generations, full);
        } else if (session.volatile_payload == null) return error.SqlRelationRecoveryIncomplete;
        // All row-bearing state (including pre-savepoint copies and mixed
        // logged/native reads) stays in the same owner-local capsule.
        projection.staged = null;
        projection.execution_plan = null;
        projection.read_snapshots = .empty;
        projection.lake_cuts = .empty;
        projection.savepoints = .empty;
        projection.native_sql_cut = null;
        projection.volatile_payload_available = false;
        return encodeSessionRecordUnchecked(self.alloc, projection);
    }

    fn decodeDurableSession(self: *DurableSessionStore, alloc: std.mem.Allocator, id: db_mod.types.TxnId, bytes: []const u8) !Session {
        return self.decodeDurableSessionForOwner(alloc, id, bytes, null);
    }

    fn decodeDurableSessionForOwner(self: *DurableSessionStore, alloc: std.mem.Allocator, id: db_mod.types.TxnId, bytes: []const u8, recovery_owner: ?u64) !Session {
        var envelope = try decodeSessionRecordForOwner(alloc, id, bytes, recovery_owner);
        errdefer envelope.deinit(alloc);
        if (envelope.volatile_payload) |reference| {
            {
                if (try self.volatile_payloads.borrow(id, reference)) |retained| {
                    defer retained.deinit();
                    var hydrated = try decodeSessionRecordForOwner(alloc, id, retained.bytes(), recovery_owner);
                    errdefer hydrated.deinit(alloc);
                    if (!std.mem.eql(u8, &try nativePayloadGenerations(hydrated, alloc), &reference.generations)) return error.SqlRelationOwnerChanged;
                    hydrated.volatile_payload = reference;
                    hydrated.volatile_payload_available = true;
                    envelope.deinit(alloc);
                    return hydrated;
                }
            }
        }
        return envelope;
    }

    fn reconcileVolatileRecord(self: *DurableSessionStore, id: db_mod.types.TxnId, raw: ?[]const u8) !void {
        var reference: ?volatile_capsules.Reference = null;
        if (raw) |bytes| {
            const parsed = try std.json.parseFromSlice(std.json.Value, self.alloc, bytes, .{ .parse_numbers = false });
            defer parsed.deinit();
            if (parsed.value != .object) return error.InvalidTransactionSessionRecord;
            if (parsed.value.object.get("volatile_payload")) |value| {
                const decoded = try std.json.parseFromValue(volatile_capsules.Reference, self.alloc, value, .{});
                defer decoded.deinit();
                try decoded.value.validate();
                reference = decoded.value;
            }
        }
        self.volatile_payloads.reconcile(id, reference);
    }

    fn reconcileVolatilePublication(self: *DurableSessionStore, id: db_mod.types.TxnId) !void {
        const key = try makeSessionKey(self.alloc, id);
        defer self.alloc.free(key);
        switch (self.backend) {
            .docstore => |store| {
                const bytes = store.get(self.alloc, key) catch |err| return if (err == error.NotFound) self.reconcileVolatileRecord(id, null) else err;
                defer self.alloc.free(bytes);
                try self.reconcileVolatileRecord(id, bytes);
            },
            .runtime => |store| {
                var read = try store.beginRead();
                defer read.abort();
                const bytes = read.get(key) catch |err| return if (err == error.NotFound) self.reconcileVolatileRecord(id, null) else err;
                try self.reconcileVolatileRecord(id, bytes);
            },
        }
    }

    fn publishVolatileSession(self: *DurableSessionStore, session: Session) void {
        if (sessionContainsNativePayload(session) or session.volatile_payload != null) self.volatile_payloads.publishPending(session.txn_id) else self.volatile_payloads.forget(session.txn_id);
    }

    fn writeSerialization(self: *const DurableSessionStore) backend_erased.WriteSerialization {
        return switch (self.backend) {
            .docstore => |store| store.writeSerialization(),
            .runtime => |store| store.writeSerialization(),
        };
    }

    pub fn supportsSharedMutations(self: *const DurableSessionStore) bool {
        return self.writeSerialization() != .unsupported;
    }

    /// All read/modify/write operations on a shared runtime namespace use its
    /// backend's gate. A configured Store.write_gate already surrounds writes,
    /// so its caller retains only the local lock and avoids recursive locking.
    pub fn mutationMutex(self: *DurableSessionStore) *std.atomic.Mutex {
        switch (self.writeSerialization()) {
            .gate => |gate| return gate,
            .intrinsic, .unsupported => {},
        }
        return &self.mutation_mutex;
    }

    // A facade owns one issuer for its entire lifetime. Sharing the physical
    // transaction store is supported; rebinding a live facade is not.
    pub fn bindRegistrationIssuer(self: *DurableSessionStore, issuer: u64) !void {
        return self.bindRegistrationIssuerControlled(issuer, .{});
    }

    pub fn bindRegistrationIssuerControlled(self: *DurableSessionStore, issuer: u64, context: RoutingContext) !void {
        // Anonymous node-local stores retain their exclusive-facade contract.
        // Shared issuers must prove transaction-wide coordination before they
        // can advertise SQL completion or allocate a durable consumer.
        if (issuer != 0 and !self.supportsSharedMutations())
            return error.CompletionOwnerUnavailable;
        const mutation = self.mutationMutex();
        while (!mutation.tryLock()) {
            try context.ensureActive();
            platform_time.yieldBriefly();
        }
        defer mutation.unlock();
        try context.ensureActive();
        if (self.registration_issuer) |bound| {
            if (bound != issuer) return error.CompletionWrongOwner;
        } else {
            if (issuer != 0 and (self.registration_recovery_before != null or self.reserved_registrations.load(.monotonic) != 0)) return error.CompletionWrongOwner;
            self.registration_issuer = issuer;
        }
    }
    fn registrationOutbox(self: *DurableSessionStore) @import("completion_outbox.zig").Namespace {
        return .{ .issuer = self.registration_issuer orelse 0 };
    }

    fn registrationOutboxFor(self: *DurableSessionStore, reference: [16]u8) Outbox.Namespace {
        return .{ .issuer = self.registration_issuer orelse 0, .observation = Outbox.isObservation(reference) };
    }

    const Backend = union(enum) {
        docstore: *docstore_mod.DocStore,
        runtime: *backend_erased.Store,
    };

    pub fn init(alloc: std.mem.Allocator, store: *docstore_mod.DocStore) DurableSessionStore {
        return .{
            .alloc = alloc,
            .volatile_payloads = .{ .allocator = alloc },
            .backend = .{ .docstore = store },
        };
    }

    /// Binds session durability to an existing storage-engine namespace. The
    /// runtime store remains owned by the engine and must outlive this value.
    pub fn initRuntime(alloc: std.mem.Allocator, store: *backend_erased.Store) DurableSessionStore {
        return .{ .alloc = alloc, .backend = .{ .runtime = store }, .volatile_payloads = .{ .allocator = alloc } };
    }

    fn checkRecordSize(self: *DurableSessionStore, session: Session, size: usize, configured_limit: ?usize, creating: bool) !void {
        const limit = configured_limit orelse std.math.maxInt(usize);
        if (creating and session.graph_receipt) {
            if (size +| session.graph_admission_growth > @min(limit, self.graphRecordLimit())) return error.SessionRecordTooLarge;
        } else if (!session.graph_receipt and size > limit) return error.SessionRecordTooLarge;
    }

    fn graphRecordLimit(self: *DurableSessionStore) usize {
        return switch (self.backend) {
            .docstore => std.math.maxInt(usize),
            .runtime => |store| if (store.capabilities().max_atomic_compare_write_bytes) |limit|
                // Allow space for keys, framing, lease and index changes as
                // well as the old and new session in the same proposal.
                (limit -| 4096) / 2
            else
                std.math.maxInt(usize),
        };
    }

    pub const Version = struct {
        owner: u64,
        touched: u64,
        executing: bool,

        fn of(session: Session) Version {
            return .{ .owner = session.owner_node_id, .touched = session.last_touched_timestamp, .executing = session.commit_execution_started };
        }
    };

    pub fn saveForTest(self: *DurableSessionStore, session: Session, max_record_bytes: ?usize) !void {
        if (!@import("builtin").is_test) @compileError("unversioned session writes are restricted to fixtures");
        return self.saveRecord(session, max_record_bytes, .{});
    }

    const SaveOptions = struct {
        expected: ?Version = null,
        ttl_ns: ?u64 = null,
        creating: bool = false,
        capacity: ?usize = null,
        external_progress: bool = false,
        unscoped_savepoint: bool = false,
        guard: ?Mutation = null,
    };

    fn saveRecord(self: *DurableSessionStore, session: Session, max_record_bytes: ?usize, options: SaveOptions) !void {
        const mutation = self.mutationMutex();
        platform_sync.lockYielding(mutation);
        defer mutation.unlock();
        if (self.fail_writes_for_test) return error.InjectedSessionStoreFailure;
        const key = try makeSessionKey(self.alloc, session.txn_id);
        defer self.alloc.free(key);
        if (sessionContainsNativePayload(session) or session.volatile_payload != null) try self.reconcileVolatilePublication(session.txn_id);
        const value = try self.encodeDurableSession(session, @min(max_record_bytes orelse std.math.maxInt(usize), self.graphRecordLimit()) -| session.graph_admission_growth);
        defer self.alloc.free(value);
        try self.checkRecordSize(session, value.len, max_record_bytes, options.creating);
        switch (self.backend) {
            .docstore => |store| {
                var txn = try store.beginWriteTxn();
                const result: anyerror!bool = if (putSessionAndExpiryTxn(self, &txn, key, value, session, options)) true else |err| err;
                _ = try finishSessionWrite(&txn, result, options.creating and session.graph_receipt);
            },
            .runtime => |store| {
                var txn = try store.beginWrite();
                const result: anyerror!bool = if (putSessionAndExpiryTxn(self, &txn, key, value, session, options)) true else |err| err;
                _ = try finishSessionWrite(&txn, result, options.creating and session.graph_receipt);
            },
        }
        self.publishVolatileSession(session);
    }

    const SavepointSnapshot = struct { value: Session, digest: [32]u8 };
    fn savepointSnapshot(self: *DurableSessionStore, alloc: std.mem.Allocator, id: db_mod.types.TxnId) !?SavepointSnapshot {
        return switch (self.backend) {
            .docstore => |store| blk: {
                var txn = try store.beginReadTxn();
                defer txn.abort();
                break :blk try self.readSavepointSnapshot(alloc, &txn, id);
            },
            .runtime => |store| blk: {
                var txn = try store.beginRead();
                defer txn.abort();
                break :blk try self.readSavepointSnapshot(alloc, &txn, id);
            },
        };
    }
    fn readSavepointSnapshot(self: *DurableSessionStore, alloc: std.mem.Allocator, txn: anytype, id: db_mod.types.TxnId) !?SavepointSnapshot {
        const key = try makeSessionKey(alloc, id);
        defer alloc.free(key);
        const bytes = txn.get(key) catch |err| return if (err == error.NotFound) null else err;
        var digest: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(bytes, &digest, .{});
        return .{ .value = try self.decodeDurableSession(alloc, id, bytes), .digest = digest };
    }
    fn saveScopedSession(self: *DurableSessionStore, session: Session, expected: ?[32]u8, plan: ?@import("sql/session/command_producer.zig").ControlPlan, control: ?@import("antfly_local_sources").storage_pessimistic_lock_authority.ledger.Control, generation_fence: ?*const @import("antfly_local_sources").storage_pessimistic_lock_authority.CommandFence, max_record_bytes: ?usize, guard: ?Mutation) !void {
        const mutation = self.mutationMutex();
        platform_sync.lockYielding(mutation);
        defer mutation.unlock();
        if (self.fail_writes_for_test) return error.InjectedSessionStoreFailure;
        const key = try makeSessionKey(self.alloc, session.txn_id);
        defer self.alloc.free(key);
        if (sessionContainsNativePayload(session) or session.volatile_payload != null) try self.reconcileVolatilePublication(session.txn_id);
        const value = try self.encodeDurableSession(session, max_record_bytes);
        defer self.alloc.free(value);
        if (max_record_bytes) |limit| if (value.len > limit) return error.SessionRecordTooLarge;
        switch (self.backend) {
            .docstore => |store| {
                var txn = try store.beginWriteTxn();
                errdefer txn.abort();
                try self.saveScopedSessionTxn(&txn, key, value, session, expected, plan, control, generation_fence, guard);
                try txn.commit();
            },
            .runtime => |store| {
                var txn = try store.beginWrite();
                errdefer txn.abort();
                try self.saveScopedSessionTxn(&txn, key, value, session, expected, plan, control, generation_fence, guard);
                try txn.commit();
            },
        }
        self.publishVolatileSession(session);
    }
    fn saveScopedSessionTxn(self: *DurableSessionStore, txn: anytype, key: []const u8, bytes: []const u8, session: Session, expected: ?[32]u8, plan: ?@import("sql/session/command_producer.zig").ControlPlan, control: ?@import("antfly_local_sources").storage_pessimistic_lock_authority.ledger.Control, generation_fence: ?*const @import("antfly_local_sources").storage_pessimistic_lock_authority.CommandFence, guard: ?Mutation) !void {
        if (expected) |want| {
            const previous = txn.get(key) catch |err| return if (err == error.NotFound) error.SqlTransactionNotActive else err;
            var actual: [32]u8 = undefined;
            std.crypto.hash.Blake3.hash(previous, &actual, .{});
            if (!std.mem.eql(u8, &actual, &want)) return error.SqlWriteConflict;
        }
        if (plan) |publication| {
            if (publication.failure) {
                const metadata = session.sql orelse return error.SqlTransactionNotActive;
                const action = control orelse return error.InvalidLockScope;
                if (!metadata.failed or action != .rollback or action.rollback.first_aborted != session.current_subtransaction or
                    !std.mem.eql(u8, &action.rollback.transaction, &session.txn_id) or session.commit_execution_started or session.external_preparation != null) return error.InvalidLockScope;
            }
            if (session.connection_id == null or session.owner_node_id != publication.prior.registration.lease.node or
                !std.mem.eql(u8, session.principal orelse "", publication.prior.principal)) return error.SqlConnectionNotFound;
        } else if (session.connection_id) |connection| {
            const producer = @import("sql/session/command_producer.zig");
            if (generation_fence) |fence| {
                const now_ms = platform_time.realtimeNs() / std.time.ns_per_ms;
                try producer.requireTransactionCommandTxn(self.alloc, txn, connection, session.txn_id, session.principal, session.owner_node_id, fence, now_ms);
            } else try producer.requireCommandTxn(self.alloc, txn, connection, null, 0);
        }
        try self.putSessionAndExpiryTxn(txn, key, bytes, session, .{ .guard = guard });
        if (self.fail_savepoint_publication_for_test == .session) return error.InjectedSessionStoreFailure;
        if (plan) |publication| try @import("sql/session/command_producer.zig").stageSavepointTxn(self.alloc, txn, publication, session.connection_id.?, session.txn_id, control);
        if (self.fail_savepoint_publication_for_test == .source) return error.InjectedSessionStoreFailure;
    }

    /// Atomically publishes a session owner and its fencing lease in the same
    /// storage transaction. Adoption compares the version cloned by the caller. Expired-only
    /// transitions reject an unexpired lease owned by another node.
    pub fn saveWithLease(
        self: *DurableSessionStore,
        session: Session,
        expected: ?Version,
        now_ms: u64,
        ttl_ms: u64,
        require_expired: bool,
        max_record_bytes: ?usize,
        capacity: ?usize,
        tenure: ?*?u128,
    ) !bool {
        return self.saveWithLeaseControlled(session, expected, now_ms, ttl_ms, require_expired, max_record_bytes, capacity, tenure, .{});
    }

    fn saveWithLeaseControlled(
        self: *DurableSessionStore,
        session: Session,
        expected: ?Version,
        now_ms: u64,
        ttl_ms: u64,
        require_expired: bool,
        max_record_bytes: ?usize,
        capacity: ?usize,
        tenure: ?*?u128,
        context: RoutingContext,
    ) !bool {
        const mutation = self.mutationMutex();
        while (!mutation.tryLock()) {
            try context.ensureActive();
            platform_time.yieldBriefly();
        }
        defer mutation.unlock();
        try context.ensureActive();
        if (self.fail_writes_for_test) return error.InjectedSessionStoreFailure;
        return switch (self.backend) {
            .docstore => |store| blk: {
                var txn = try store.beginWriteTxn();
                const result = saveSessionWithLeaseTxn(self, &txn, session, expected, now_ms, ttl_ms, require_expired, max_record_bytes, capacity, tenure);
                context.ensureActive() catch |err| {
                    txn.abort();
                    return err;
                };
                const changed = try finishSessionWrite(&txn, result, session.graph_receipt and expected == null);
                if (changed) self.publishVolatileSession(session);
                break :blk changed;
            },
            .runtime => |store| blk: {
                var txn = if (self.routing_access) |access| try access.write(store.allocator, context) else try store.beginWrite();
                const result = saveSessionWithLeaseTxn(self, &txn, session, expected, now_ms, ttl_ms, require_expired, max_record_bytes, capacity, tenure);
                context.ensureActive() catch |err| {
                    txn.abort();
                    return err;
                };
                const changed = try finishSessionWrite(&txn, result, session.graph_receipt and expected == null);
                if (changed) self.publishVolatileSession(session);
                break :blk changed;
            },
        };
    }

    // Capacity bootstrap is the only write a rejected BEGIN can publish.
    // Both create paths check membership before staging session or lease data.
    fn finishSessionWrite(txn: anytype, result: anyerror!bool, graph_admission: bool) !bool {
        const changed = result catch |err| {
            if (err == error.SessionCountInitializedAtLimit) {
                errdefer txn.abort();
                try txn.commit();
            } else txn.abort();
            return if (err == error.SessionCountInitializedAtLimit) error.SessionLimitExceeded else err;
        };
        if (changed) {
            errdefer txn.abort();
            txn.commit() catch |err| {
                // The replicated store bounds its encoded change set before
                // proposing it. Other commit failures can lose the reply.
                if (err == error.SessionWriteNotLeaderBeforeProposal) return error.NotLeader;
                if (err == error.SessionWriteOutOfMemoryBeforeProposal) return error.OutOfMemory;
                if (err == error.SessionWriteUnavailableBeforeProposal) return error.StorageReadTemporarilyUnavailable;
                if (graph_admission and err == error.SessionLeaseLost) return error.StorageReadTemporarilyUnavailable;
                if (graph_admission and err != error.TransactionTooLarge) return error.GraphReceiptAdmissionUnknown;
                return err;
            };
        } else txn.abort();
        return changed;
    }

    fn saveSessionWithLeaseTxn(
        self: *DurableSessionStore,
        txn: anytype,
        session: Session,
        expected: ?Version,
        now_ms: u64,
        ttl_ms: u64,
        require_expired: bool,
        max_record_bytes: ?usize,
        capacity: ?usize,
        tenure: ?*?u128,
    ) !bool {
        const session_key = try makeSessionKey(self.alloc, session.txn_id);
        defer self.alloc.free(session_key);
        const lease_key = try makeSessionLeaseKey(self.alloc, session.txn_id);
        defer self.alloc.free(lease_key);

        const current_raw = txn.get(session_key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        if (sessionContainsNativePayload(session) or session.volatile_payload != null) try self.reconcileVolatileRecord(session.txn_id, current_raw);
        var previous_needs_recovery = false;
        if (expected) |version| {
            const raw = current_raw orelse return false;
            var current = try self.decodeDurableSession(self.alloc, session.txn_id, raw);
            defer current.deinit(self.alloc);
            if (!std.meta.eql(Version.of(current), version)) return false;
            previous_needs_recovery = sessionNeedsRecovery(current);
        } else {
            if (current_raw != null) return if (session.graph_receipt) error.CypherWriteRequestAlreadyUsed else false;
            try updateSessionCount(txn, true, capacity);
            try self.requireBeginningConnection(txn, session);
        }

        const owner_id = try ownerLeaseId(self.alloc, session.owner_node_id);
        defer self.alloc.free(owner_id);
        const lease_raw = txn.get(lease_key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        var epoch: u64 = 1;
        var expires = now_ms +| ttl_ms;
        if (lease_raw) |raw| {
            const parsed = try std.json.parseFromSlice(lease_mod.LeaseRecord, self.alloc, raw, .{ .allocate = .alloc_always });
            defer parsed.deinit();
            const live = parsed.value.expires_at_ms > now_ms;
            const same_owner = std.mem.eql(u8, parsed.value.owner_id, owner_id);
            if (require_expired and live and !same_owner) return false;
            if (live and same_owner) {
                epoch = @max(1, parsed.value.epoch);
                expires = @max(expires, parsed.value.expires_at_ms);
            } else epoch = std.math.add(u64, parsed.value.epoch, 1) catch return error.LeaseEpochOverflow;
        }

        // Certify every foreground lease on creation. A legacy owner can replace the lease
        // with an uncertified epoch during a rolling upgrade.
        if (expected == null or epoch < certified_lease_epoch) {
            try issueReaderGeneration(self.alloc, txn, session.txn_id);
            epoch = certified_lease_epoch;
        }

        const session_value = try self.encodeDurableSession(session, @min(max_record_bytes orelse std.math.maxInt(usize), self.graphRecordLimit()) -| session.graph_admission_growth);
        defer self.alloc.free(session_value);
        try self.checkRecordSize(session, session_value.len, max_record_bytes, expected == null);
        const lease_value = try std.json.Stringify.valueAlloc(self.alloc, lease_mod.LeaseRecord{
            .owner_id = owner_id,
            .expires_at_ms = expires,
            .epoch = epoch,
        }, .{});
        defer self.alloc.free(lease_value);
        if (expected) |version| {
            if (session.prepared_completion_only) try self.deleteTaggedSqlExpiryKeyTxn(txn, session.txn_id, version.touched);
            const old_expiry_key = try makeSessionExpiryKey(self.alloc, version.touched, session.txn_id);
            defer self.alloc.free(old_expiry_key);
            txn.delete(old_expiry_key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
        }
        const expiry_key = try makeSessionExpiryKey(self.alloc, session.last_touched_timestamp, session.txn_id);
        defer self.alloc.free(expiry_key);
        try self.rejectReferencedResurrection(txn, session);
        try self.preserveCompletionTxn(txn, session, false);
        try self.putConnectionExecutionTxn(txn, session);
        try txn.put(session_key, session_value);
        try txn.put(expiry_key, &.{});
        try self.putTaggedSqlExpiryTxn(txn, session);
        if (sessionNeedsRecovery(session)) {
            try setSessionRecoveryIndexTxn(self, txn, session);
        } else if (previous_needs_recovery) {
            try clearSessionRecoveryIndexTxn(self, txn, session.txn_id);
        }
        if (self.fail_lease_transition_after_session_write_for_test) return error.InjectedLeaseTransitionFailure;
        try txn.put(lease_key, lease_value);
        if (tenure) |out| out.* = try leaseStamp(self.alloc, txn, session.txn_id, epoch);
        return true;
    }

    pub fn load(self: *DurableSessionStore, txn_id: db_mod.types.TxnId) !?Session {
        return self.loadInto(self.alloc, txn_id);
    }

    fn loadForRouting(self: *DurableSessionStore, id: db_mod.types.TxnId, context: RoutingContext) !?Session {
        try context.ensureActive();
        return switch (self.backend) {
            .docstore => self.load(id),
            .runtime => |store| result: {
                var txn = try self.beginRoutingRead(store, context);
                defer txn.abort();
                break :result try self.loadSessionTxn(&txn, id);
            },
        };
    }

    fn loadWithRecoveryOwner(self: *DurableSessionStore, txn_id: db_mod.types.TxnId, recovery_owner: ?u64) !?Session {
        return self.loadIntoForOwner(self.alloc, txn_id, recovery_owner);
    }

    pub fn loadInto(self: *DurableSessionStore, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId) !?Session {
        return self.loadIntoForOwner(alloc, txn_id, null);
    }

    fn loadIntoForOwner(self: *DurableSessionStore, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId, recovery_owner: ?u64) !?Session {
        const key = try makeSessionKey(self.alloc, txn_id);
        defer self.alloc.free(key);
        const value = switch (self.backend) {
            .docstore => |store| store.get(self.alloc, key) catch |err| switch (err) {
                error.NotFound => return null,
                else => return err,
            },
            .runtime => |store| {
                var txn = try store.beginRead();
                defer txn.abort();
                const raw = txn.get(key) catch |err| switch (err) {
                    error.NotFound => return null,
                    else => return err,
                };
                return try self.decodeDurableSessionForOwner(alloc, txn_id, raw, recovery_owner);
            },
        };
        defer self.alloc.free(value);
        return try self.decodeDurableSessionForOwner(alloc, txn_id, value, recovery_owner);
    }

    fn readStatus(self: *DurableSessionStore, id: db_mod.types.TxnId, filter: SessionRegistry.StatusPrincipalFilter, colocated_lease: bool) !?GraphStatusLookup {
        const key = try makeSessionKey(self.alloc, id);
        defer self.alloc.free(key);
        switch (self.backend) {
            .docstore => |store| {
                var txn = try store.beginReadTxn();
                defer txn.abort();
                return readStatusTxn(&txn, self.alloc, key, id, filter, colocated_lease);
            },
            .runtime => |store| {
                var txn = try store.beginRead();
                defer txn.abort();
                return readStatusTxn(&txn, self.alloc, key, id, filter, colocated_lease);
            },
        }
    }

    // Compact completion facts outlive the expiring session payload and its
    // quota/index entries. No TTL may erase a proof still held by a caller.
    pub fn completionSettled(self: *DurableSessionStore, txn_id: db_mod.types.TxnId) !bool {
        const key = try makeSessionCompletionKey(self.alloc, txn_id);
        defer self.alloc.free(key);
        const value = switch (self.backend) {
            .docstore => |store| store.get(self.alloc, key) catch |err| switch (err) {
                error.NotFound => return false,
                else => return err,
            },
            .runtime => |store| blk: {
                var txn = try store.beginRead();
                defer txn.abort();
                const raw = txn.get(key) catch |err| switch (err) {
                    error.NotFound => return false,
                    else => return err,
                };
                break :blk try self.alloc.dupe(u8, raw);
            },
        };
        defer self.alloc.free(value);
        return std.mem.eql(u8, value, "settled-v1");
    }

    fn legacySqlPinsFenced(txn: anytype) !bool {
        return sqlPinFenceExists(txn, sql_legacy_pin_fence_key);
    }

    fn sqlPinFenceExists(txn: anytype, key: []const u8) !bool {
        const version = txn.get(key) catch |err| if (err == error.NotFound) return false else return err;
        if (!std.mem.eql(u8, version, "sealed-v1")) return error.InvalidTransactionSessionRecord;
        return true;
    }

    /// Seal the exclusive issuer and owner namespace on its storage write cut.
    /// The caller must establish that it has no separately routed issuers.
    pub fn sealStandaloneLegacySqlPins(self: *DurableSessionStore) !void {
        try self.sealLegacySqlPins(0);
    }

    pub fn sealSharedLegacySqlPinIssuer(self: *DurableSessionStore, issuer: u64) !void {
        if (issuer == 0) return error.CompletionWrongOwner;
        try self.bindRegistrationIssuer(issuer);
        try self.sealLegacySqlPins(issuer);
    }

    fn sqlPinIssuerFenceKey(issuer: u64, buf: []u8) ![]const u8 {
        if (issuer == 0) return sql_legacy_pin_fence_key;
        return std.fmt.bufPrint(buf, "{s}:{x:0>16}", .{ sql_legacy_pin_fence_key, issuer });
    }

    fn sealLegacySqlPins(self: *DurableSessionStore, issuer: u64) !void {
        const mutation = self.mutationMutex();
        platform_sync.lockYielding(mutation);
        defer mutation.unlock();
        if ((self.registration_issuer orelse 0) != issuer) return error.CompletionWrongOwner;
        var key_buf: [128]u8 = undefined;
        const key = try sqlPinIssuerFenceKey(issuer, &key_buf);
        switch (self.backend) {
            .docstore => |store| {
                var txn = try store.beginWriteTxn();
                errdefer txn.abort();
                if (try self.sealLegacySqlPinsTxn(&txn, key)) try txn.commit() else txn.abort();
            },
            .runtime => |store| {
                var txn = try store.beginWrite();
                errdefer txn.abort();
                if (try self.sealLegacySqlPinsTxn(&txn, key)) try txn.commit() else txn.abort();
            },
        }
    }

    fn sealLegacySqlPinsTxn(self: *DurableSessionStore, txn: anytype, key: []const u8) !bool {
        if (try sqlPinFenceExists(txn, key)) return false;
        const producer = try self.registrationOutbox().producer(txn, self.alloc, @splat(0));
        if (producer.active != 0 or self.reserved_registrations.load(.monotonic) != 0) return error.CompletionIssuerNotDrained;
        try txn.put(key, "sealed-v1");
        return true;
    }

    pub fn reclaimStandaloneSqlAbortProofs(self: *DurableSessionStore) !usize {
        const mutation = self.mutationMutex();
        platform_sync.lockYielding(mutation);
        defer mutation.unlock();
        var next_position: ?SqlAbortProofPosition = null;
        const removed = switch (self.backend) {
            .docstore => |store| result: {
                var txn = try store.beginWriteTxn();
                errdefer txn.abort();
                if (!try legacySqlPinsFenced(&txn)) {
                    txn.abort();
                    break :result 0;
                }
                const count = scan: {
                    var cursor = try txn.openPhysicalKeyCursorAdapter();
                    defer cursor.close();
                    break :scan try reclaimSqlAbortProofsTxn(&txn, &cursor, self.sql_abort_proof_position, &next_position);
                };
                if (count != 0) try txn.commit() else txn.abort();
                break :result count;
            },
            .runtime => |store| result: {
                var txn = try store.beginWrite();
                errdefer txn.abort();
                if (!try legacySqlPinsFenced(&txn)) {
                    txn.abort();
                    break :result 0;
                }
                const count = scan: {
                    var cursor = try txn.openCursor();
                    defer cursor.close();
                    break :scan try reclaimSqlAbortProofsTxn(&txn, &cursor, self.sql_abort_proof_position, &next_position);
                };
                if (count != 0) try txn.commit() else txn.abort();
                break :result count;
            },
        };
        self.sql_abort_proof_position = next_position;
        return removed;
    }

    const completion_retirement_root = "\x00\x00__api_completion_retired__:";
    const completion_reference_root = "\x00\x00__api_completion_reference__:";
    const observation_ack_prefix_len = completion_retirement_root.len + 16 + ":ack:".len;
    const ObservationAckPosition = IndexPosition(observation_ack_prefix_len + 16);

    const ObservationAck = struct { key: [observation_ack_prefix_len + 16]u8, reference: [16]u8 };

    pub fn reclaimLegacyObservationAcks(self: *DurableSessionStore, shared: bool) !bool {
        if (!self.observation_ack_mutex.tryLock()) return true;
        defer self.observation_ack_mutex.unlock();
        var prefix_bytes: [observation_ack_prefix_len + 1]u8 = undefined;
        var prefix: []const u8 = completion_retirement_root;
        var first = prefix;
        if (!shared) {
            const namespace: Outbox.Namespace = .{ .observation = true };
            const producer = switch (self.backend) {
                .docstore => |store| result: {
                    var txn = try store.beginReadTxn();
                    defer txn.abort();
                    break :result try namespace.producer(&txn, self.alloc, @splat(0));
                },
                .runtime => |store| result: {
                    var txn = try store.beginRead();
                    defer txn.abort();
                    break :result try namespace.producer(&txn, self.alloc, @splat(0));
                },
            };
            if (producer.next == 1) return false;
            first = try std.fmt.bufPrint(&prefix_bytes, "{s}{s}:ack:8", .{ completion_retirement_root, &std.fmt.bytesToHex(producer.id, .lower) });
            prefix = first[0..observation_ack_prefix_len];
        }
        const Scan = struct {
            prefix_len: usize,
            acks: [128]ObservationAck = undefined,
            count: usize = 0,
            inspected: usize = 0,
            next: ?ObservationAckPosition = null,

            fn visit(raw: *anyopaque, key: []const u8, _: []const u8) anyerror!bool {
                const scan: *@This() = @ptrCast(@alignCast(raw));
                scan.inspected += 1;
                if (key.len == observation_ack_prefix_len + 16) candidate: {
                    var reference: [16]u8 = undefined;
                    const issuer_end = completion_retirement_root.len + 16;
                    if (!std.mem.eql(u8, key[issuer_end..observation_ack_prefix_len], ":ack:")) break :candidate;
                    _ = std.fmt.hexToBytes(reference[0..8], key[completion_retirement_root.len..issuer_end]) catch break :candidate;
                    _ = std.fmt.hexToBytes(reference[8..], key[observation_ack_prefix_len..]) catch break :candidate;
                    if (!Outbox.isObservation(reference)) {
                        // Ordinary sequences sort before tagged observations
                        // for this producer. Resume directly at the tag.
                        var position: ObservationAckPosition = .{ .len = observation_ack_prefix_len + 1, .exclusive = false };
                        @memcpy(position.bytes[0..observation_ack_prefix_len], key[0..observation_ack_prefix_len]);
                        position.bytes[observation_ack_prefix_len] = '8';
                        scan.next = position;
                        return false;
                    }
                    scan.acks[scan.count] = .{ .key = key[0..][0 .. observation_ack_prefix_len + 16].*, .reference = reference };
                    scan.count += 1;
                }
                if (scan.inspected < scan.acks.len) return true;
                scan.next = ObservationAckPosition.after(key, scan.prefix_len);
                return false;
            }
        };
        var scan: Scan = .{ .prefix_len = prefix.len };
        const after = self.observation_ack_cursor;
        const start = if (after) |*position| position.bytes[0..position.len] else first;
        try self.scanPrefixFromWithContextMode(prefix, start, if (after) |position| position.exclusive else false, &scan, Scan.visit, true);
        const acks = scan.acks[0..scan.count];
        if (acks.len != 0) {
            const mutation = self.mutationMutex();
            platform_sync.lockYielding(mutation);
            defer mutation.unlock();
            switch (self.backend) {
                .docstore => |store| {
                    var txn = try store.beginWriteTxn();
                    errdefer txn.abort();
                    if (try reclaimLegacyObservationAcksTxn(&txn, acks)) try txn.commit() else txn.abort();
                },
                .runtime => |store| {
                    var txn = try store.beginWrite();
                    errdefer txn.abort();
                    if (try reclaimLegacyObservationAcksTxn(&txn, acks)) try txn.commit() else txn.abort();
                },
            }
        }
        self.observation_ack_cursor = scan.next;
        return scan.next != null;
    }

    fn reclaimLegacyObservationAcksTxn(txn: anytype, acks: []const ObservationAck) !bool {
        // A legacy acknowledgement atomically removed the directory member.
        // The marker and this index are its only remaining target-side rows.
        const marker = @import("completion_retirement.zig").marker;
        var changed = false;
        for (acks) |ack| {
            const target = txn.get(&ack.key) catch |err| if (err == error.NotFound) continue else return err;
            const reference_hex = std.fmt.bytesToHex(ack.reference, .lower);
            if (target.len != completion_reference_root.len + 65 or
                !std.mem.startsWith(u8, target, completion_reference_root) or
                target[completion_reference_root.len + 32] != ':' or
                !std.mem.endsWith(u8, target, &reference_hex)) continue;
            const target_key = target[0..][0 .. completion_reference_root.len + 65].*;
            if (txn.get(&target_key)) |value| {
                if (!std.mem.eql(u8, value, marker)) continue;
                try txn.delete(&target_key);
            } else |err| if (err != error.NotFound) return err;
            try txn.delete(&ack.key);
            changed = true;
        }
        return changed;
    }

    const SqlAbortProofPosition = IndexPosition(sql_abort_proof_prefix.len + 32);

    fn reclaimSqlAbortProofsTxn(txn: anytype, cursor: anytype, previous: ?SqlAbortProofPosition, next_position: *?SqlAbortProofPosition) !usize {
        const key_len = sql_abort_proof_prefix.len + 32;
        var keys: [128][key_len]u8 = undefined;
        var count: usize = 0;
        var inspected: usize = 0;
        cursor.setKeysOnly();
        const start = if (previous) |*position| position.bytes[0..position.len] else sql_abort_proof_prefix;
        var next = try cursor.seekAtOrAfter(start);
        while (next) |entry| : (next = try cursor.next()) {
            if (!std.mem.startsWith(u8, entry.key, sql_abort_proof_prefix)) break;
            if (previous) |position| if (position.exclusive and std.mem.eql(u8, entry.key, start)) continue;
            inspected += 1;
            if (entry.key.len == key_len and std.mem.eql(u8, try txn.get(entry.key), "settled-v1")) {
                keys[count] = entry.key[0..key_len].*;
                count += 1;
            }
            if (count == keys.len or inspected == 256) {
                next_position.* = SqlAbortProofPosition.after(entry.key, sql_abort_proof_prefix.len);
                break;
            }
        }
        for (keys[0..count]) |key| try txn.delete(&key);
        return count;
    }

    pub fn sqlCommitDecisionStatus(self: *DurableSessionStore, alloc: std.mem.Allocator, id: db_mod.types.TxnId, connection: ?[32]u8, principal: []const u8) !?SqlStoredDecision {
        return (try self.lookupSqlCommitDecision(alloc, id, connection, principal, false)).decision;
    }

    pub fn probeSqlCommitDecision(self: *DurableSessionStore, alloc: std.mem.Allocator, id: db_mod.types.TxnId, connection: ?[32]u8, principal: []const u8) !SqlCommitProbe {
        return self.lookupSqlCommitDecision(alloc, id, connection, principal, true);
    }

    pub const RoutedSqlCommit = struct {
        decision: ?SqlStoredDecision = null,
        owner: ?u64 = null,
    };

    pub fn routedSqlCommitDecision(self: *DurableSessionStore, alloc: std.mem.Allocator, id: db_mod.types.TxnId, principal: []const u8, capacity: *@import("antfly_local_sources").common_request_admission.RequestAdmission, context: RoutingContext) !RoutedSqlCommit {
        try context.ensureActive();
        return switch (self.backend) {
            .docstore => |store| result: {
                var txn = try store.beginReadTxn();
                defer txn.abort();
                break :result try self.routedSqlCommitDecisionTxn(&txn, alloc, id, principal, capacity, context);
            },
            .runtime => |store| result: {
                var txn = try self.beginRoutingRead(store, context);
                defer txn.abort();
                break :result try self.routedSqlCommitDecisionTxn(&txn, alloc, id, principal, capacity, context);
            },
        };
    }

    fn routedSqlCommitDecisionTxn(self: *DurableSessionStore, txn: anytype, alloc: std.mem.Allocator, id: db_mod.types.TxnId, principal: []const u8, capacity: *@import("antfly_local_sources").common_request_admission.RequestAdmission, context: RoutingContext) !RoutedSqlCommit {
        const probe = try self.sqlCommitDecisionStatusTxn(txn, alloc, id, null, principal, true);
        if (probe.decision) |decision| return .{ .decision = decision };
        if (probe.payload != .none) {
            try context.ensureActive();
            var lease = capacity.tryAcquireLease() orelse return error.SqlWriteCapacityUnavailable;
            defer lease.release();
            if ((try self.sqlCommitDecisionStatusTxn(txn, alloc, id, null, principal, false)).decision) |decision|
                return .{ .decision = decision };
        }
        return .{ .owner = try SessionRegistry.probeOwnerWithFallback(self, txn, id, capacity, context) };
    }

    const SqlCommitProbe = struct {
        decision: ?SqlStoredDecision = null,
        payload: enum { none, terminal, unknown } = .none,
    };

    fn lookupSqlCommitDecision(self: *DurableSessionStore, alloc: std.mem.Allocator, id: db_mod.types.TxnId, connection: ?[32]u8, principal: []const u8, bounded: bool) !SqlCommitProbe {
        return switch (self.backend) {
            .docstore => |store| result: {
                var txn = try store.beginReadTxn();
                defer txn.abort();
                break :result try self.sqlCommitDecisionStatusTxn(&txn, alloc, id, connection, principal, bounded);
            },
            .runtime => |store| result: {
                var txn = try store.beginRead();
                defer txn.abort();
                break :result try self.sqlCommitDecisionStatusTxn(&txn, alloc, id, connection, principal, bounded);
            },
        };
    }

    fn sqlCommitDecisionStatusTxn(self: *DurableSessionStore, txn: anytype, alloc: std.mem.Allocator, id: db_mod.types.TxnId, connection: ?[32]u8, principal: []const u8, bounded: bool) !SqlCommitProbe {
        const key = try makeSqlAbortReceiptKey(alloc, id);
        defer alloc.free(key);
        var receipt = if (txn.get(key)) |raw|
            try decodeSqlTerminalReceipt(alloc, id, raw)
        else |err| switch (err) {
            error.NotFound => live: {
                const session_key = try makeSessionKey(alloc, id);
                defer alloc.free(session_key);
                probe: {
                    var scope = txn.openReadScope(alloc) catch |scope_err| switch (scope_err) {
                        error.Unsupported => break :probe,
                        else => return scope_err,
                    };
                    defer scope.close();
                    const bytes = scope.probePrefix(session_key, connection_prefix_len) catch |probe_err| switch (probe_err) {
                        error.NotFound => return .{},
                        error.Unsupported, error.EndOfStream => break :probe,
                        else => return probe_err,
                    };
                    const bound = try parseConnectionPrefix(bytes);
                    const header = bound orelse try parseSqlPrefix(bytes, sql_field);
                    if (header) |payload| {
                        if (bound != null) {
                            const expected = connection orelse return .{};
                            var decoded: [16]u8 = undefined;
                            _ = std.fmt.hexToBytes(&decoded, &expected) catch return .{};
                            if (!std.mem.eql(u8, &decoded, payload[49..65])) return .{};
                        } else if (connection != null) return .{};
                        if (payload[0] & 4 == 0) return .{};
                        if (bounded) {
                            var digest: [32]u8 = undefined;
                            std.crypto.hash.sha2.Sha256.hash(principal, &digest, .{});
                            return .{ .payload = if (std.mem.eql(u8, &digest, payload[9..41])) .terminal else .none };
                        }
                    }
                }
                if (bounded) return .{ .payload = .unknown };
                var session = (try self.loadSessionTxn(txn, id)) orelse return .{};
                if (session.sql == null or session.graph_receipt) {
                    session.deinit(self.alloc);
                    return .{};
                }
                break :live session;
            },
            else => return err,
        };
        defer receipt.deinitOwned();
        if (!std.meta.eql(receipt.connection_id, connection) or
            !std.mem.eql(u8, receipt.principal orelse "", principal) or receipt.external_preparation_prepared or
            (receipt.terminal_commit == null and !receipt.terminal_abort)) return .{};
        return .{ .decision = .{ .disposition = sessionDisposition(receipt), .abort_acknowledgement = receipt.sql_abort_acknowledgement } };
    }

    fn sqlAbortReceiptAcceptsLatePin(self: *DurableSessionStore, txn: anytype, id: db_mod.types.TxnId) !bool {
        const key = try makeSqlAbortReceiptKey(self.alloc, id);
        defer self.alloc.free(key);
        const raw = txn.get(key) catch |err| if (err == error.NotFound) return false else return err;
        var receipt = try decodeSqlTerminalReceipt(self.alloc, id, raw);
        defer receipt.deinit(self.alloc);
        return !receipt.prepared_completion_only;
    }

    fn sqlAbortProofExists(self: *DurableSessionStore, txn: anytype, id: db_mod.types.TxnId) !bool {
        const key = try makeSessionCompletionKey(self.alloc, id);
        defer self.alloc.free(key);
        const value = txn.get(key) catch |err| if (err == error.NotFound) return false else return err;
        return std.mem.eql(u8, value, "settled-v1");
    }

    fn putSqlAbortProof(self: *DurableSessionStore, txn: anytype, id: db_mod.types.TxnId) !void {
        if (try legacySqlPinsFenced(txn)) return;
        const key = try makeSessionCompletionKey(self.alloc, id);
        defer self.alloc.free(key);
        try txn.put(key, "settled-v1");
    }

    fn rejectReferencedResurrection(self: *DurableSessionStore, txn: anytype, session: Session) !void {
        const key = try makeSessionKey(self.alloc, session.txn_id);
        defer self.alloc.free(key);
        _ = txn.get(key) catch |err| switch (err) {
            error.NotFound => {
                if (try self.sqlAbortProofExists(txn, session.txn_id)) return error.CypherWriteRequestAlreadyUsed;
                const receipt = try makeSqlAbortReceiptKey(self.alloc, session.txn_id);
                defer self.alloc.free(receipt);
                if (txn.get(receipt)) |_| {
                    return error.CypherWriteRequestAlreadyUsed;
                } else |receipt_err| if (receipt_err != error.NotFound) return receipt_err;
                const directory = try completionDirectoryKey(self.alloc, session.txn_id);
                defer self.alloc.free(directory);
                _ = txn.get(directory) catch |missing| switch (missing) {
                    error.NotFound => return,
                    else => return missing,
                };
                return error.SqlTransactionOutcomeUnknown;
            },
            else => return err,
        };
    }

    // References belong to consumers, rather than the lifetime of a session.
    // Only pending receipts allocate these keys. Terminal publication and
    // session retirement settle every registered consumer atomically.
    fn preserveCompletionTxn(self: *DurableSessionStore, txn: anytype, session: Session, retiring: bool) !void {
        if (!sessionCompletionSettled(session) and !(retiring and !session.commit_execution_started)) return;
        const directory = try completionDirectoryKey(self.alloc, session.txn_id);
        defer self.alloc.free(directory);
        const bytes = txn.get(directory) catch |err| switch (err) {
            error.NotFound => return,
            else => return err,
        };
        var refs = try std.json.parseFromSlice([][16]u8, self.alloc, bytes, .{});
        defer refs.deinit();
        for (refs.value) |reference| {
            const key = try completionReferenceKey(self.alloc, session.txn_id, reference);
            defer self.alloc.free(key);
            try txn.put(key, "settled-v1");
        }
    }

    pub fn managedCompletion(self: *DurableSessionStore, id: [16]u8, reference: [16]u8, acknowledge: bool, retired_through: u64) !bool {
        return self.mutateCompletionReference(id, reference, acknowledge, retired_through);
    }
    pub fn lockRegistration(self: *DurableSessionStore, reference: [16]u8) *std.atomic.Mutex {
        const lock = &self.registration_locks[reference[15] % self.registration_locks.len];
        platform_sync.lockYielding(lock);
        return lock;
    }
    const Outbox = @import("completion_outbox.zig");
    const RegistrationMutation = union(enum) {
        create: struct { seed: [8]u8, outcome: @import("../pgwire/completion.zig").Outcome, reserved: bool = false, prepared: bool = false, local_transaction_id: ?[16]u8 = null, observe_only: bool = false, publish_observation: bool = false },
        put: struct { reference: [16]u8, record: Outbox.Record },
        remove: [16]u8,
        join: [16]u8,
        retire_local: struct { reference: [16]u8, transaction_id: [16]u8 },
    };
    fn observationRegistrationPrefix(self: *const DurableSessionStore, buf: []u8) ![]const u8 {
        return std.fmt.bufPrint(buf, "\x00\x00__sql_completion_observation__:{x:0>16}:", .{self.registration_issuer orelse 0});
    }

    fn observationRegistrationKey(self: *const DurableSessionStore, id: [16]u8, buf: []u8) ![]const u8 {
        var prefix_buf: [128]u8 = undefined;
        const prefix = try self.observationRegistrationPrefix(&prefix_buf);
        return std.fmt.bufPrint(buf, "{s}{s}", .{ prefix, &std.fmt.bytesToHex(id, .lower) });
    }

    fn readObservationReference(self: *DurableSessionStore, txn: anytype, key: []const u8, id: [16]u8, joined: ?*bool) !?[16]u8 {
        const value = txn.get(key) catch |err| if (err == error.NotFound) return null else return err;
        if (value.len != 16) return error.InvalidTransactionSessionRecord;
        const reference = value[0..16].*;
        var record = (try self.registrationOutboxFor(reference).load(txn, self.alloc, reference)) orelse return error.InvalidTransactionSessionRecord;
        defer record.deinit();
        if (record.value.observation != true or !record.value.prepared or !record.value.execution_admitted)
            return error.InvalidTransactionSessionRecord;
        const encoded = record.value.outcome.transaction_id orelse return error.InvalidTransactionSessionRecord;
        const target = distributed_txn.parseTxnIdHex(encoded) catch return error.InvalidTransactionSessionRecord;
        if (!std.mem.eql(u8, &target, &id)) return error.InvalidTransactionSessionRecord;
        if (joined) |flag| flag.* = record.value.joined;
        return reference;
    }

    pub fn existingObservationReference(self: *DurableSessionStore, id: [16]u8) !?[16]u8 {
        return self.existingObservationReferenceControlled(id, .{}, false);
    }

    pub fn existingObservationReferenceControlled(self: *DurableSessionStore, id: [16]u8, context: RoutingContext, require_joined: bool) !?[16]u8 {
        try context.ensureActive();
        var joined = false;
        var buf: [128]u8 = undefined;
        const key = try self.observationRegistrationKey(id, &buf);
        const reference = switch (self.backend) {
            .docstore => |store| result: {
                var txn = try store.beginReadTxn();
                defer txn.abort();
                break :result try self.readObservationReference(&txn, key, id, &joined);
            },
            .runtime => |store| result: {
                var txn = try self.beginRoutingRead(store, context);
                defer txn.abort();
                break :result try self.readObservationReference(&txn, key, id, &joined);
            },
        };
        return if (require_joined and !joined) null else reference;
    }

    fn removeObservedRegistrationTxn(self: *DurableSessionStore, txn: anytype, id: [16]u8, reference: [16]u8) !void {
        var buf: [128]u8 = undefined;
        const key = try self.observationRegistrationKey(id, &buf);
        const current = txn.get(key) catch |err| if (err == error.NotFound) return else return err;
        if (std.mem.eql(u8, current, &reference)) try txn.delete(key);
    }

    fn registrationMutationTxn(self: *DurableSessionStore, txn: anytype, action: RegistrationMutation) ![16]u8 {
        if (self.registration_recovery_before == null)
            self.registration_recovery_before = (try self.registrationOutbox().producer(txn, self.alloc, @splat(0))).next;
        switch (action) {
            .create => |value| {
                const reserved = self.reserved_registrations.load(.monotonic);
                if (value.reserved and !value.observe_only and reserved == 0) return error.SessionLimitExceeded;
                const outbox: Outbox.Namespace = .{ .issuer = self.registration_issuer orelse 0, .observation = value.observe_only };
                const held = reserved - @as(usize, if (value.reserved and !value.observe_only) 1 else 0);
                if (!value.observe_only and try outbox.available(txn, self.alloc) <= held) return error.SessionLimitExceeded;
                var observation_buf: [128]u8 = undefined;
                const observation_key = if (value.observe_only) try self.observationRegistrationKey(value.local_transaction_id.?, &observation_buf) else null;
                if (observation_key) |key| {
                    var joined = false;
                    if (try self.readObservationReference(txn, key, value.local_transaction_id.?, &joined)) |reference| {
                        if (value.publish_observation and !joined) {
                            var record = (try outbox.load(txn, self.alloc, reference)) orelse return error.InvalidTransactionSessionRecord;
                            defer record.deinit();
                            record.value.joined = true;
                            try outbox.put(txn, self.alloc, reference, record.value);
                        }
                        return reference;
                    }
                }
                const floor: ?u64 = if (value.observe_only) null else (try outbox.producer(txn, self.alloc, value.seed)).retired_through;
                const reference = try outbox.create(txn, self.alloc, value.seed, value.outcome, value.prepared);
                var issuer_fence_buf: [128]u8 = undefined;
                const issuer_fence = try sqlPinIssuerFenceKey(self.registration_issuer orelse 0, &issuer_fence_buf);
                if (!value.prepared and value.outcome.transaction_id != null and value.outcome.ddl_receipt_json == null and try sqlPinFenceExists(txn, issuer_fence)) {
                    const hex = value.outcome.transaction_id.?;
                    const id = distributed_txn.parseTxnIdHex(hex) catch return error.InvalidArgument;
                    _ = try self.completionReferenceTxn(txn, id, reference, false, floor, .atomic_pin);
                    try self.registrationOutboxFor(reference).put(txn, self.alloc, reference, .{ .outcome = value.outcome, .phase = .registered });
                }
                if (value.local_transaction_id) |id| {
                    // Fold the local handoff into the intent allocation batch:
                    // no remote RPC or intermediate WAL commits are necessary.
                    _ = try self.completionReferenceTxn(txn, id, reference, false, floor, if (value.observe_only) .observe else .prepare);
                    try self.registrationOutboxFor(reference).put(txn, self.alloc, reference, .{ .outcome = value.outcome, .prepared = true, .execution_admitted = true, .phase = .registered, .observation = if (value.observe_only) true else null, .joined = value.publish_observation });
                }
                if (observation_key) |key| try txn.put(key, &reference);
                return reference;
            },
            .put => |value| try self.registrationOutboxFor(value.reference).put(txn, self.alloc, value.reference, value.record),
            .remove => |reference| {
                if (try self.registrationOutboxFor(reference).load(txn, self.alloc, reference)) |loaded| {
                    var previous = loaded;
                    defer previous.deinit();
                    if (previous.value.observation orelse false) {
                        const id = try distributed_txn.parseTxnIdHex(previous.value.outcome.transaction_id.?);
                        try self.removeObservedRegistrationTxn(txn, id, reference);
                    }
                }
                try self.registrationOutboxFor(reference).remove(txn, self.alloc, reference);
            },
            .join => |reference| {
                var record = (try self.registrationOutboxFor(reference).load(txn, self.alloc, reference)) orelse return @splat(0);
                defer record.deinit();
                if (!record.value.joined) {
                    record.value.joined = true;
                    try self.registrationOutboxFor(reference).put(txn, self.alloc, reference, record.value);
                }
                return reference;
            },
            .retire_local => |value| {
                const outbox = self.registrationOutboxFor(value.reference);
                // Observations are created only with the issuer record in this store.
                // No delayed owner mutation can recreate a retired observation.
                const floor: ?u64 = if (outbox.observation) null else (try outbox.producer(txn, self.alloc, @splat(0))).retired_through;
                if (!try self.completionReferenceTxn(txn, value.transaction_id, value.reference, true, floor, .pin)) return error.CompletionUnavailable;
                try self.removeObservedRegistrationTxn(txn, value.transaction_id, value.reference);
                try outbox.remove(txn, self.alloc, value.reference);
            },
        }
        return @splat(0);
    }
    fn registrationMutation(self: *DurableSessionStore, action: RegistrationMutation) ![16]u8 {
        return self.registrationMutationControlled(action, .{});
    }

    fn registrationMutationControlled(self: *DurableSessionStore, action: RegistrationMutation, context: RoutingContext) ![16]u8 {
        const mutation = self.mutationMutex();
        while (!mutation.tryLock()) {
            try context.ensureActive();
            platform_time.yieldBriefly();
        }
        defer mutation.unlock();
        try context.ensureActive();
        if (self.fail_writes_for_test) return error.InjectedSessionStoreFailure;
        const result = switch (self.backend) {
            .docstore => |store| blk: {
                var txn = try store.beginWriteTxn();
                errdefer txn.abort();
                const reference = try self.registrationMutationTxn(&txn, action);
                try context.ensureActive();
                try txn.commit();
                break :blk reference;
            },
            .runtime => |store| blk: {
                var txn = if (self.routing_access) |access| try access.write(store.allocator, context) else try store.beginWrite();
                errdefer txn.abort();
                const reference = try self.registrationMutationTxn(&txn, action);
                try context.ensureActive();
                try txn.commit();
                break :blk reference;
            },
        };
        if (action == .create and action.create.reserved and !action.create.observe_only) self.releaseRegistrations(1);
        return result;
    }
    pub fn reserveRegistrations(self: *DurableSessionStore, count: usize) !void {
        return self.reserveRegistrationsControlled(count, .{});
    }

    pub fn reserveRegistrationsControlled(self: *DurableSessionStore, count: usize, context: RoutingContext) !void {
        const mutation = self.mutationMutex();
        while (!mutation.tryLock()) {
            try context.ensureActive();
            platform_time.yieldBriefly();
        }
        defer mutation.unlock();
        try context.ensureActive();
        const free = switch (self.backend) {
            .docstore => |store| blk: {
                var txn = try store.beginReadTxn();
                defer txn.abort();
                break :blk try self.registrationOutbox().available(&txn, self.alloc);
            },
            .runtime => |store| blk: {
                var txn = try self.beginRoutingRead(store, context);
                defer txn.abort();
                break :blk try self.registrationOutbox().available(&txn, self.alloc);
            },
        };
        try context.ensureActive();
        if (count > free or self.reserved_registrations.load(.monotonic) > free - count) return error.SessionLimitExceeded;
        _ = self.reserved_registrations.fetchAdd(count, .monotonic);
    }
    pub fn releaseRegistrations(self: *DurableSessionStore, count: usize) void {
        // Admission and durable consumption serialize on the mutation gate.
        // Returning unused permits only increases capacity and needs no gate.
        const previous = self.reserved_registrations.fetchSub(count, .monotonic);
        std.debug.assert(count <= previous);
    }
    pub fn createReservedRegistration(self: *DurableSessionStore, seed: [8]u8, outcome: @import("../pgwire/completion.zig").Outcome) ![16]u8 {
        return self.registrationMutation(.{ .create = .{ .seed = seed, .outcome = outcome, .reserved = true } });
    }
    pub fn retireLocalPreparedRegistration(self: *DurableSessionStore, reference: [16]u8, id: [16]u8) !void {
        _ = try self.registrationMutation(.{ .retire_local = .{ .reference = reference, .transaction_id = id } });
    }
    pub fn createLocalPreparedRegistration(self: *DurableSessionStore, seed: [8]u8, outcome: @import("../pgwire/completion.zig").Outcome, id: [16]u8) ![16]u8 {
        return self.registrationMutation(.{ .create = .{ .seed = seed, .outcome = outcome, .reserved = true, .prepared = true, .local_transaction_id = id } });
    }
    pub fn publishObservedRegistration(self: *DurableSessionStore, seed: [8]u8, outcome: @import("../pgwire/completion.zig").Outcome, id: [16]u8, context: RoutingContext) ![16]u8 {
        return self.registrationMutationControlled(.{ .create = .{ .seed = seed, .outcome = outcome, .prepared = true, .local_transaction_id = id, .observe_only = true, .publish_observation = true } }, context);
    }

    pub fn createLocalObservedRegistration(self: *DurableSessionStore, seed: [8]u8, outcome: @import("../pgwire/completion.zig").Outcome, id: [16]u8) ![16]u8 {
        if (try self.existingObservationReference(id)) |reference| return reference;
        return self.registrationMutation(.{ .create = .{ .seed = seed, .outcome = outcome, .prepared = true, .local_transaction_id = id, .observe_only = true } });
    }

    pub fn createPreparedRegistration(self: *DurableSessionStore, seed: [8]u8, outcome: @import("../pgwire/completion.zig").Outcome) ![16]u8 {
        return self.registrationMutation(.{ .create = .{ .seed = seed, .outcome = outcome, .reserved = true, .prepared = true } });
    }
    pub fn createRegistration(self: *DurableSessionStore, seed: [8]u8, outcome: @import("../pgwire/completion.zig").Outcome) ![16]u8 {
        return self.registrationMutation(.{ .create = .{ .seed = seed, .outcome = outcome } });
    }
    pub fn putRegistration(self: *DurableSessionStore, reference: [16]u8, record: Outbox.Record) !void {
        _ = try self.registrationMutation(.{ .put = .{ .reference = reference, .record = record } });
    }
    pub fn joinRegistration(self: *DurableSessionStore, reference: [16]u8) !bool {
        const joined = try self.registrationMutation(.{ .join = reference });
        return std.mem.eql(u8, &joined, &reference);
    }

    pub fn removeRegistration(self: *DurableSessionStore, reference: [16]u8) !void {
        _ = try self.registrationMutation(.{ .remove = reference });
    }
    pub fn loadRegistration(self: *DurableSessionStore, alloc: std.mem.Allocator, reference: [16]u8) !?std.json.Parsed(Outbox.Record) {
        return switch (self.backend) {
            .docstore => |store| blk: {
                var txn = try store.beginReadTxn();
                defer txn.abort();
                break :blk try self.registrationOutboxFor(reference).load(&txn, alloc, reference);
            },
            .runtime => |store| blk: {
                var txn = try store.beginRead();
                defer txn.abort();
                break :blk try self.registrationOutboxFor(reference).load(&txn, alloc, reference);
            },
        };
    }
    pub fn registrationRecoveryPage(self: *DurableSessionStore, alloc: std.mem.Allocator, after: u64, limit: usize) !Outbox.RecoveryPage {
        const mutation = self.mutationMutex();
        platform_sync.lockYielding(mutation);
        defer mutation.unlock();
        return switch (self.backend) {
            .docstore => |store| blk: {
                var txn = try store.beginReadTxn();
                defer txn.abort();
                if (self.registration_recovery_before == null) self.registration_recovery_before = (try self.registrationOutbox().producer(&txn, self.alloc, @splat(0))).next;
                break :blk try self.registrationOutbox().recoveryPage(&txn, alloc, self.registration_recovery_before.?, after, limit);
            },
            .runtime => |store| blk: {
                var txn = try store.beginRead();
                defer txn.abort();
                if (self.registration_recovery_before == null) self.registration_recovery_before = (try self.registrationOutbox().producer(&txn, self.alloc, @splat(0))).next;
                break :blk try self.registrationOutbox().recoveryPage(&txn, alloc, self.registration_recovery_before.?, after, limit);
            },
        };
    }
    pub const ObservationIndexEntry = struct { transaction_id: [16]u8, reference: [16]u8 };
    pub const ObservationRetirementPage = struct { entries: []ObservationIndexEntry, next_after: ?[]u8 };

    pub fn copyObservationRegistrationKey(self: *DurableSessionStore, alloc: std.mem.Allocator, id: [16]u8) ![]u8 {
        var buf: [128]u8 = undefined;
        return alloc.dupe(u8, try self.observationRegistrationKey(id, &buf));
    }

    pub fn observationRetirementPage(self: *DurableSessionStore, alloc: std.mem.Allocator, after: ?[]const u8, limit: usize) !ObservationRetirementPage {
        var entries: std.ArrayList(ObservationIndexEntry) = .empty;
        errdefer entries.deinit(alloc);
        if (limit == 0) return .{ .entries = try entries.toOwnedSlice(alloc), .next_after = null };
        var prefix_buf: [128]u8 = undefined;
        const prefix = try self.observationRegistrationPrefix(&prefix_buf);
        const Scan = struct {
            alloc: std.mem.Allocator,
            entries: *std.ArrayList(ObservationIndexEntry),
            prefix_len: usize,
            limit: usize,
            inspected: usize = 0,
            next_after: ?[]u8 = null,

            fn visit(raw: *anyopaque, key: []const u8, value: []const u8) anyerror!bool {
                const scan: *@This() = @ptrCast(@alignCast(raw));
                scan.inspected += 1;
                const suffix = key[scan.prefix_len..];
                const id = distributed_txn.parseTxnIdHex(suffix) catch null;
                // The maintenance deadline can resume from an entry's encoded key.
                if (id != null and value.len == 16 and std.mem.eql(u8, suffix, &std.fmt.bytesToHex(id.?, .lower))) {
                    try scan.entries.append(scan.alloc, .{ .transaction_id = id.?, .reference = value[0..16].* });
                } else {
                    std.log.warn("malformed SQL observation index row retained key_bytes={d} value_bytes={d}", .{ key.len, value.len });
                }
                if (scan.inspected == scan.limit) {
                    scan.next_after = try scan.alloc.dupe(u8, key);
                    return false;
                }
                return true;
            }
        };
        var scan = Scan{ .alloc = alloc, .entries = &entries, .prefix_len = prefix.len, .limit = limit };
        errdefer if (scan.next_after) |key| alloc.free(key);
        try self.scanPrefixFromWithContext(prefix, after, &scan, Scan.visit);
        return .{ .entries = try entries.toOwnedSlice(alloc), .next_after = scan.next_after };
    }

    pub fn registrationFloor(self: *DurableSessionStore) !u64 {
        return self.registrationFloorFor(@splat(0));
    }

    pub fn registrationFloorFor(self: *DurableSessionStore, reference: [16]u8) !u64 {
        const outbox = self.registrationOutboxFor(reference);
        return switch (self.backend) {
            .docstore => |store| blk: {
                var txn = try store.beginReadTxn();
                defer txn.abort();
                break :blk outbox.ownerFloor((try outbox.producer(&txn, self.alloc, @splat(0))).retired_through);
            },
            .runtime => |store| blk: {
                var txn = try store.beginRead();
                defer txn.abort();
                break :blk outbox.ownerFloor((try outbox.producer(&txn, self.alloc, @splat(0))).retired_through);
            },
        };
    }

    pub fn pinCompletion(self: *DurableSessionStore, id: db_mod.types.TxnId, reference: [16]u8) !void {
        _ = try self.mutateCompletionReference(id, reference, false, null);
    }
    pub fn acknowledgeCompletion(self: *DurableSessionStore, id: db_mod.types.TxnId, reference: [16]u8) !bool {
        return self.mutateCompletionReference(id, reference, true, null);
    }
    fn mutateCompletionReference(self: *DurableSessionStore, id: db_mod.types.TxnId, reference: [16]u8, acknowledge: bool, retired_through: ?u64) !bool {
        return self.changeCompletionReference(id, reference, acknowledge, retired_through, .pin);
    }
    pub const CompletionMode = enum { pin, atomic_pin, prepare, observe, quiesce, abandon, forget };
    pub fn changeCompletionReference(self: *DurableSessionStore, id: [16]u8, reference: [16]u8, acknowledge: bool, retired_through: ?u64, mode: CompletionMode) !bool {
        const mutation = self.mutationMutex();
        platform_sync.lockYielding(mutation);
        defer mutation.unlock();
        if (self.fail_writes_for_test) return error.InjectedSessionStoreFailure;
        return switch (self.backend) {
            .docstore => |store| blk: {
                var txn = try store.beginWriteTxn();
                errdefer txn.abort();
                const result = try self.completionReferenceTxn(&txn, id, reference, acknowledge, retired_through, mode);
                try txn.commit();
                break :blk result;
            },
            .runtime => |store| blk: {
                var txn = try store.beginWrite();
                errdefer txn.abort();
                const result = try self.completionReferenceTxn(&txn, id, reference, acknowledge, retired_through, mode);
                try txn.commit();
                break :blk result;
            },
        };
    }
    fn completionReferenceTxn(self: *DurableSessionStore, txn: anytype, id: db_mod.types.TxnId, reference: [16]u8, acknowledge: bool, retired_through: ?u64, mode: CompletionMode) !bool {
        const retiring = acknowledge or mode == .forget;
        const retirement = @import("completion_retirement.zig");
        if (retired_through) |floor| {
            const observed = try retirement.advance(txn, self.alloc, "\x00\x00__api_completion_retired__:", reference, floor);
            if (retirement.sequence(reference) <= observed) {
                if (retiring or mode == .abandon or mode == .quiesce) return true;
                return error.CompletionAcknowledged;
            }
        }
        const key = try completionReferenceKey(self.alloc, id, reference);
        defer self.alloc.free(key);
        const existing = txn.get(key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        if (existing) |value| if (std.mem.eql(u8, value, retirement.marker)) {
            if (retiring or mode == .abandon or mode == .quiesce) return true;
            return error.CompletionAcknowledged;
        };
        if (acknowledge and existing == null) return true;
        if (acknowledge and !std.mem.eql(u8, existing.?, "settled-v1")) return false;
        if (!retiring and mode == .abandon) {
            // The issuer durably joined without admitting execution. Publish a
            // proof even if the target session was retired before preparation.
            try txn.put(key, "settled-v1");
            return true;
        }
        if (!retiring and mode == .quiesce) {
            if (existing == null) return false;
            if (std.mem.eql(u8, existing.?, "settled-v1")) return true;
            if (std.mem.eql(u8, existing.?, "observing-v1")) return false;
            var session = (try self.loadSessionTxn(txn, id)) orelse return false;
            defer session.deinit(self.alloc);
            if (sessionCompletionSettled(session) or !session.commit_execution_started) {
                try txn.put(key, "settled-v1");
                return true;
            }
            return false;
        }
        if (!retiring and existing != null) return true;
        if (!retiring and mode == .pin and try legacySqlPinsFenced(txn)) return error.CompletionUnavailable;
        const directory = try completionDirectoryKey(self.alloc, id);
        defer self.alloc.free(directory);
        const bytes = txn.get(directory) catch |err| switch (err) {
            error.NotFound => "[]",
            else => return err,
        };
        var parsed = std.json.parseFromSlice([][16]u8, self.alloc, bytes, .{}) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => return error.InvalidTransactionSessionRecord,
        };
        defer parsed.deinit();
        var refs: std.ArrayList([16]u8) = .empty;
        defer refs.deinit(self.alloc);
        for (parsed.value) |old| if (!retiring or !std.mem.eql(u8, &old, &reference)) {
            try refs.append(self.alloc, old);
        };
        if (retiring) {
            if (retired_through != null) try retirement.acknowledge(txn, self.alloc, "\x00\x00__api_completion_retired__:", reference, key) else try txn.delete(key);
        } else {
            if (refs.items.len >= 4096) return error.SessionLimitExceeded;
            const settled = if (try self.loadSessionTxn(txn, id)) |loaded| live: {
                var session = loaded;
                defer session.deinit(self.alloc);
                if (session.prepared_completion_only and mode != .prepare and mode != .observe) return error.CompletionUnavailable;
                break :live sessionCompletionSettled(session) or (mode != .prepare and mode != .observe and !session.commit_execution_started);
            } else receipt: {
                if (mode == .prepare or mode == .atomic_pin) return error.SqlTransactionOutcomeUnknown;
                if (mode == .observe) {
                    const receipt_key = try makeSqlAbortReceiptKey(self.alloc, id);
                    defer self.alloc.free(receipt_key);
                    const raw = txn.get(receipt_key) catch |err| if (err == error.NotFound) return error.SqlTransactionOutcomeUnknown else return err;
                    var terminal = try decodeSqlTerminalReceipt(self.alloc, id, raw);
                    defer terminal.deinit(self.alloc);
                    break :receipt true;
                }
                if (!try self.sqlAbortProofExists(txn, id)) {
                    if (!try self.sqlAbortReceiptAcceptsLatePin(txn, id)) return error.SqlTransactionOutcomeUnknown;
                }
                break :receipt true;
            };
            try refs.append(self.alloc, reference);
            try txn.put(key, if (settled) "settled-v1" else if (mode == .observe) "observing-v1" else "pending-v1");
        }
        if (refs.items.len == 0) {
            try txn.delete(directory);
        } else {
            const encoded = try std.json.Stringify.valueAlloc(self.alloc, refs.items, .{});
            defer self.alloc.free(encoded);
            try txn.put(directory, encoded);
        }
        return true;
    }
    pub fn completionReferenceSettled(self: *DurableSessionStore, id: db_mod.types.TxnId, reference: [16]u8) !bool {
        const key = try completionReferenceKey(self.alloc, id, reference);
        defer self.alloc.free(key);
        const bytes = switch (self.backend) {
            .docstore => |store| store.get(self.alloc, key) catch |err| switch (err) {
                error.NotFound => return false,
                else => return err,
            },
            .runtime => |store| blk: {
                var txn = try store.beginRead();
                defer txn.abort();
                const value = txn.get(key) catch |err| switch (err) {
                    error.NotFound => return false,
                    else => return err,
                };
                break :blk try self.alloc.dupe(u8, value);
            },
        };
        defer self.alloc.free(bytes);
        return std.mem.eql(u8, bytes, "settled-v1");
    }

    fn delete(self: *DurableSessionStore, txn_id: db_mod.types.TxnId, expected: Version, cutoff: ?u64, guard: ?Mutation, sql_commit: ?bool) !void {
        const mutation = self.mutationMutex();
        platform_sync.lockYielding(mutation);
        defer mutation.unlock();
        if (self.fail_writes_for_test) return error.InjectedSessionStoreFailure;
        const key = try makeSessionKey(self.alloc, txn_id);
        defer self.alloc.free(key);
        switch (self.backend) {
            .docstore => |store| {
                var txn = try store.beginWriteTxn();
                errdefer txn.abort();
                try deleteSessionAndExpiryTxn(self, &txn, key, txn_id, expected, cutoff, guard, sql_commit);
                try txn.commit();
            },
            .runtime => |store| {
                var txn = try store.beginWrite();
                errdefer txn.abort();
                try deleteSessionAndExpiryTxn(self, &txn, key, txn_id, expected, cutoff, guard, sql_commit);
                try txn.commit();
            },
        }
        self.volatile_payloads.forget(txn_id);
    }

    pub const DetachCondition = union(enum) {
        beginning,
        active,
        commit,
        expired: struct { state: sql_connection_record.State, cutoff_ms: u64 },

        fn matches(self: DetachCondition, connection: sql_connection_record.Record, owner: u64) bool {
            return switch (self) {
                .beginning => connection.state == .beginning and connection.owner_node_id == owner,
                .active, .commit => connection.state == .active and connection.owner_node_id == owner,
                .expired => |expiry| (expiry.state == .beginning or expiry.state == .active or expiry.state == .uncertain) and
                    connection.state == expiry.state and connection.expires_at_ms <= expiry.cutoff_ms,
            };
        }
    };

    /// A connection-owned rollback has one durable decision: retire the
    /// unexecuted transaction and detach its exact connection in the same
    /// native transaction. Neither side may become visible alone.
    fn deleteAndDetachConnection(self: *DurableSessionStore, txn_id: db_mod.types.TxnId, connection_id: [32]u8, commit_settings: bool, expected: Version, condition: DetachCondition, guard: ?Mutation, separate_receipt: bool) !void {
        const mutation = self.mutationMutex();
        platform_sync.lockYielding(mutation);
        defer mutation.unlock();
        if (self.fail_writes_for_test) return error.InjectedSessionStoreFailure;
        const key = try makeSessionKey(self.alloc, txn_id);
        defer self.alloc.free(key);
        switch (self.backend) {
            .docstore => |store| {
                var txn = try store.beginWriteTxn();
                errdefer txn.abort();
                try self.deleteAndDetachConnectionTxn(&txn, key, txn_id, connection_id, commit_settings, expected, condition, guard, separate_receipt);
                try txn.commit();
            },
            .runtime => |store| {
                var txn = try store.beginWrite();
                errdefer txn.abort();
                try self.deleteAndDetachConnectionTxn(&txn, key, txn_id, connection_id, commit_settings, expected, condition, guard, separate_receipt);
                try txn.commit();
            },
        }
        self.volatile_payloads.forget(txn_id);
    }

    fn deleteAndDetachConnectionTxn(self: *DurableSessionStore, txn: anytype, key: []const u8, txn_id: db_mod.types.TxnId, connection_id: [32]u8, commit_settings: bool, expected: Version, condition: DetachCondition, guard: ?Mutation, separate_receipt: bool) !void {
        const raw = txn.get(key) catch |err| switch (err) {
            error.NotFound => return error.SqlTransactionOutcomeUnknown,
            else => return err,
        };
        var session = try self.decodeDurableSession(self.alloc, txn_id, raw);
        defer session.deinit(self.alloc);
        if (session.connection_id == null or !std.mem.eql(u8, &session.connection_id.?, &connection_id) or
            session.commit_execution_started or session.terminal_commit != null or session.terminal_abort) return error.SqlTransactionOutcomeUnknown;
        var connection = try sql_connection_record.loadTxn(txn, self.alloc, &connection_id);
        defer connection.deinit();
        if (!condition.matches(connection.value, session.owner_node_id) or connection.value.active_txn == null or
            !std.mem.eql(u8, &connection.value.active_txn.?, &txn_id) or
            !std.mem.eql(u8, connection.value.principal, session.principal orelse "")) return error.SqlTransactionOutcomeUnknown;
        var detached = connection.value;
        detached.state = .idle;
        detached.active_txn = null;
        if (detached.revision == std.math.maxInt(u64)) return error.SqlProgramLimitExceeded;
        detached.revision += 1;
        if (commit_settings) {
            detached.overlay = session.setting_committed.items;
            detached.lookup_path = (session.sql orelse return error.InvalidTransactionSessionRecord).search_path_committed;
        }
        try sql_connection_record.putTxn(txn, self.alloc, detached);
        if ((condition == .commit and session.prepared_completion_only) or
            (condition == .expired and connection.value.state != .beginning))
        {
            // The uncertain-state update may itself have lost its acknowledgement.
            // Keep the abort proof for either state and release the connection
            // and lease in the same fenced storage transaction.
            var receipt = sqlCommitDecision(session, commit_settings);
            touchSession(&receipt);
            if (!separate_receipt and !session.prepared_completion_only) {
                const encoded = try encodeSessionRecord(self.alloc, receipt);
                defer self.alloc.free(encoded);
                try self.putSessionAndExpiryTxn(txn, key, encoded, receipt, .{ .expected = expected, .guard = guard });
                try self.clearSessionLeaseTxn(txn, txn_id);
                return;
            }
            try self.deleteLoadedSessionTxn(txn, key, session, expected, null, guard);
            try self.putSqlAbortReceipt(txn, receipt);
        } else {
            try self.deleteLoadedSessionTxn(txn, key, session, expected, null, guard);
        }
    }

    fn sqlCommitDecision(session: Session, committed: bool) Session {
        return .{
            .txn_id = session.txn_id,
            .owner_node_id = session.owner_node_id,
            .principal = session.principal,
            .connection_id = session.connection_id,
            .prepared_completion_only = session.prepared_completion_only,
            .begin_timestamp = session.begin_timestamp,
            .last_touched_timestamp = session.last_touched_timestamp,
            .sync_level = session.sync_level,
            .commit_execution_started = true,
            .terminal_abort = !committed,
            .sql_abort_acknowledgement = if (session.sql != null and session.sql.?.failed and !session.commit_execution_started) .rollback else session.sql_abort_acknowledgement,
            .terminal_commit = if (committed) .{ .status = .committed } else null,
        };
    }

    pub fn compactDetachedSqlSessionTxn(self: *DurableSessionStore, txn: anytype, session: Session) !void {
        // The command loan and recovery observer both use the exact external
        // seal through completion, even after the connection detaches. The
        // tagged expiry queue retires it after ACK and the observation TTL.
        if (session.external_preparation != null) return;
        if (!session.prepared_completion_only or !sessionCompletionSettled(session) or !sessionCanExpire(session)) return;
        const key = try makeSessionKey(self.alloc, session.txn_id);
        defer self.alloc.free(key);
        try self.deleteLoadedSessionTxn(txn, key, session, Version.of(session), null, null);
        try self.putSqlCommitDecision(txn, session, session.terminal_commit != null);
    }

    fn putSqlCommitDecision(self: *DurableSessionStore, txn: anytype, session: Session, committed: bool) !void {
        var receipt = sqlCommitDecision(session, committed);
        touchSession(&receipt);
        try self.putSqlAbortReceipt(txn, receipt);
    }

    fn putSqlAbortReceipt(self: *DurableSessionStore, txn: anytype, receipt: Session) !void {
        const encoded = try encodeSessionRecord(self.alloc, receipt);
        defer self.alloc.free(encoded);
        const receipt_key = try makeSqlAbortReceiptKey(self.alloc, receipt.txn_id);
        defer self.alloc.free(receipt_key);
        const expiry_key = try makeExpiryKey(self.alloc, sql_abort_expiry_prefix, receipt.last_touched_timestamp, receipt.txn_id);
        defer self.alloc.free(expiry_key);
        if (!receipt.prepared_completion_only) try self.putSqlAbortProof(txn, receipt.txn_id);
        try txn.put(receipt_key, encoded);
        try txn.put(expiry_key, &.{});
    }

    fn migrateLegacySqlAbortReceipt(self: *DurableSessionStore, id: db_mod.types.TxnId) !bool {
        const mutation = self.mutationMutex();
        platform_sync.lockYielding(mutation);
        defer mutation.unlock();
        return switch (self.backend) {
            .docstore => |store| result: {
                var txn = try store.beginWriteTxn();
                errdefer txn.abort();
                const changed = try self.migrateLegacySqlAbortReceiptTxn(&txn, id);
                if (changed) try txn.commit() else txn.abort();
                break :result changed;
            },
            .runtime => |store| result: {
                var txn = try store.beginWrite();
                errdefer txn.abort();
                const changed = try self.migrateLegacySqlAbortReceiptTxn(&txn, id);
                if (changed) try txn.commit() else txn.abort();
                break :result changed;
            },
        };
    }

    fn migrateLegacySqlAbortReceiptTxn(self: *DurableSessionStore, txn: anytype, id: db_mod.types.TxnId) !bool {
        var receipt = (try self.loadSessionTxn(txn, id)) orelse return false;
        defer receipt.deinit(self.alloc);
        if (!isLegacySqlAbortReceipt(receipt)) return false;
        if (sql_connection_record.loadTxn(txn, self.alloc, &receipt.connection_id.?)) |loaded| {
            var connection = loaded;
            defer connection.deinit();
            if (std.meta.eql(connection.value.active_txn, @as(?[16]u8, id))) return false;
        } else |err| if (err != error.SqlConnectionNotFound) return err;
        const key = try makeSessionKey(self.alloc, id);
        defer self.alloc.free(key);
        try self.deleteLoadedSessionTxn(txn, key, receipt, Version.of(receipt), null, null);
        try self.putSqlAbortReceipt(txn, receipt);
        return true;
    }

    fn isLegacySqlAbortReceipt(session: Session) bool {
        return session.connection_id != null and session.terminal_abort and !session.graph_receipt and
            session.sql == null and session.staged == null and session.execution_plan == null and session.commit_body_digest == null;
    }

    /// Compact execution admission, published with every connection-owned SQL
    /// session write. Source completion must not decode staged rows or plans.
    pub fn connectionExecutionPendingTxn(_: *DurableSessionStore, txn: anytype, txn_id: db_mod.types.TxnId, connection: [32]u8, principal: []const u8, node: u64) !bool {
        var key: [96]u8 = undefined;
        const raw = txn.get(try connectionExecutionKey(&key, txn_id)) catch |err| return if (err == error.NotFound) error.InvalidTransactionSessionRecord else err;
        if (raw.len != 74 or raw[0] != 1 or raw[1] > 2 or !std.mem.eql(u8, raw[2..34], &connection) or std.mem.readInt(u64, raw[34..42], .big) != node) return error.InvalidTransactionSessionRecord;
        var hash: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(principal, &hash, .{});
        if (!std.mem.eql(u8, raw[42..74], &hash)) return error.InvalidTransactionSessionRecord;
        return raw[1] == 1;
    }

    fn connectionExecutionKey(buffer: []u8, txn_id: db_mod.types.TxnId) ![]const u8 {
        return std.fmt.bufPrint(buffer, "\x00\x00__api_sql_execution__:{s}", .{&std.fmt.bytesToHex(txn_id, .lower)});
    }

    fn putConnectionExecutionTxn(_: *DurableSessionStore, txn: anytype, session: Session) !void {
        const connection = session.connection_id orelse return;
        var key: [96]u8 = undefined;
        const name = try connectionExecutionKey(&key, session.txn_id);
        var encoded: [74]u8 = undefined;
        encoded[0] = 1;
        encoded[1] = if (session.terminal_commit != null or session.terminal_abort or session.external_preparation_prepared) 2 else if (session.commit_execution_started) 1 else 0;
        @memcpy(encoded[2..34], &connection);
        std.mem.writeInt(u64, encoded[34..42], session.owner_node_id, .big);
        std.crypto.hash.Blake3.hash(session.principal orelse "", encoded[42..74], .{});
        if (txn.get(name) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        }) |previous| if (std.mem.eql(u8, previous, &encoded)) return;
        try txn.put(name, &encoded);
    }

    /// Reads the transaction under an existing connection write transaction,
    /// so terminal proof and committed setting values share its detach cut.
    pub fn connectionLifecycle(self: *DurableSessionStore, id: db_mod.types.TxnId) !?ConnectionLifecycle {
        return self.readConnectionLifecycle(id, false);
    }

    pub fn probeConnectionLifecycle(self: *DurableSessionStore, id: db_mod.types.TxnId) !?ConnectionLifecycle {
        return self.readConnectionLifecycle(id, true);
    }

    fn readConnectionLifecycle(self: *DurableSessionStore, id: db_mod.types.TxnId, bounded: bool) !?ConnectionLifecycle {
        switch (self.backend) {
            .docstore => |store| {
                var txn = try store.beginReadTxn();
                defer txn.abort();
                return self.connectionLifecycleTxn(&txn, id, bounded);
            },
            .runtime => |store| {
                var txn = try store.beginRead();
                defer txn.abort();
                return self.connectionLifecycleTxn(&txn, id, bounded);
            },
        }
    }

    fn connectionLifecycleTxn(self: *DurableSessionStore, txn: anytype, id: db_mod.types.TxnId, bounded: bool) !?ConnectionLifecycle {
        const key = try makeSessionKey(self.alloc, id);
        defer self.alloc.free(key);
        const packed_state = prefix: {
            var scope = txn.openReadScope(self.alloc) catch |err| switch (err) {
                error.Unsupported => break :prefix null,
                else => return err,
            };
            defer scope.close();
            const bytes = scope.probePrefix(key, connection_prefix_len) catch |err| switch (err) {
                error.NotFound => return null,
                error.EndOfStream, error.Unsupported => break :prefix null,
                else => return err,
            };
            break :prefix try parseConnectionPrefix(bytes);
        };
        if (packed_state) |p| return .{
            .connection_id = std.fmt.bytesToHex(p[49..65].*, .lower),
            .version = .{ .owner = std.mem.readInt(u64, p[1..9], .little), .touched = std.mem.readInt(u64, p[41..49], .little), .executing = p[0] & 2 != 0 },
            .terminal = p[0] & 4 != 0,
        };
        if (bounded) return error.Unsupported;
        var legacy = (try self.loadSessionTxn(txn, id)) orelse return null;
        defer legacy.deinit(self.alloc);
        return .{ .connection_id = legacy.connection_id, .version = Version.of(legacy), .terminal = legacy.terminal_abort or legacy.terminal_commit != null };
    }

    pub fn loadSessionTxn(self: *DurableSessionStore, txn: anytype, txn_id: db_mod.types.TxnId) !?Session {
        const key = try makeSessionKey(self.alloc, txn_id);
        defer self.alloc.free(key);
        const raw = txn.get(key) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        return try self.decodeDurableSession(self.alloc, txn_id, raw);
    }

    fn requireBeginningConnection(self: *DurableSessionStore, txn: anytype, session: Session) !void {
        const id = session.connection_id orelse return;
        var connection = sql_connection_record.loadTxn(txn, self.alloc, &id) catch |err| switch (err) {
            error.SqlConnectionNotFound => return error.SqlConnectionChanged,
            else => return err,
        };
        defer connection.deinit();
        if (connection.value.state != .beginning or !std.meta.eql(connection.value.active_txn, @as(?[16]u8, session.txn_id)) or
            connection.value.owner_node_id != session.owner_node_id or !std.mem.eql(u8, connection.value.principal, session.principal orelse ""))
            return error.SqlConnectionChanged;
    }

    fn deleteTaggedSqlExpiryKeyTxn(self: *DurableSessionStore, txn: anytype, id: db_mod.types.TxnId, timestamp: u64) !void {
        const key = try makeExpiryKey(self.alloc, tagged_sql_expiry_prefix, timestamp, id);
        defer self.alloc.free(key);
        txn.delete(key) catch |err| if (err != error.NotFound) return err;
    }

    fn clearTaggedSqlExpiryTxn(self: *DurableSessionStore, txn: anytype, session: Session) !void {
        if (session.prepared_completion_only and sessionCompletionSettled(session) and sessionCanExpire(session))
            try self.deleteTaggedSqlExpiryKeyTxn(txn, session.txn_id, session.last_touched_timestamp);
    }

    fn putTaggedSqlExpiryTxn(self: *DurableSessionStore, txn: anytype, session: Session) !void {
        if (!session.prepared_completion_only or !sessionCompletionSettled(session) or !sessionCanExpire(session)) return;
        const key = try makeExpiryKey(self.alloc, tagged_sql_expiry_prefix, session.last_touched_timestamp, session.txn_id);
        defer self.alloc.free(key);
        try txn.put(key, &.{});
    }

    fn putSessionAndExpiryTxn(self: *DurableSessionStore, txn: anytype, key: []const u8, value: []const u8, session: Session, options: SaveOptions) !void {
        if (options.creating) try self.requireBeginningConnection(txn, session);
        if (options.unscoped_savepoint) if (session.connection_id) |id|
            try @import("sql/session/command_producer.zig").requireCommandTxn(self.alloc, txn, id, null, 0);
        var previous_needs_recovery = false;
        if (txn.get(key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        }) |raw| {
            if (options.creating) return error.CypherWriteRequestAlreadyUsed;
            var previous = try self.decodeDurableSession(self.alloc, session.txn_id, raw);
            defer previous.deinit(self.alloc);
            if (options.expected) |version| {
                if (!std.meta.eql(version, Version.of(previous))) return error.SessionLeaseLost;
            } else if (previous.owner_node_id != session.owner_node_id) return error.SessionLeaseLost;
            if ((previous.terminal_abort and !session.terminal_abort) or
                (previous.terminal_commit != null and session.terminal_commit == null)) return error.SqlTransactionOutcomeUnknown;
            // Only the dedicated receipt writers can retire a sealed external
            // plan. Version CAS and the unchanged seal bind the exact inputs
            // those writers verified before recording PREPARE or the decision.
            const retires_external_plan = options.external_progress and options.expected != null and
                previous.external_preparation != null and optionalStringsEqual(previous.external_preparation, session.external_preparation) and
                (session.external_preparation_prepared or session.terminal_abort or session.external_decision != null);
            const retires_graph_plan = previous.graph_receipt and session.graph_receipt and session.terminal_abort;
            const retires_plan = session.execution_plan == null and (retires_external_plan or retires_graph_plan);
            if (previous.commit_execution_started and !session.commit_execution_started or
                (previous.execution_plan != null and !retires_plan and !optionalStringsEqual(previous.execution_plan, session.execution_plan)))
                return error.TransactionCommitSealed;
            previous_needs_recovery = sessionNeedsRecovery(previous);
            try self.clearTaggedSqlExpiryTxn(txn, previous);
            const old_expiry_key = try makeSessionExpiryKey(self.alloc, previous.last_touched_timestamp, session.txn_id);
            defer self.alloc.free(old_expiry_key);
            txn.delete(old_expiry_key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
        } else {
            if (options.expected != null) return error.SessionLeaseLost;
            try updateSessionCount(txn, true, options.capacity);
        }
        if (options.ttl_ns) |ttl| {
            const lease_key = try makeSessionLeaseKey(self.alloc, session.txn_id);
            defer self.alloc.free(lease_key);
            const raw = txn.get(lease_key) catch |err| if (err == error.NotFound) return error.SessionLeaseLost else return err;
            const parsed = try std.json.parseFromSlice(lease_mod.LeaseRecord, self.alloc, raw, .{});
            defer parsed.deinit();
            try requireReaderLease(parsed.value, session.owner_node_id);
            if (options.guard) |admitted| {
                const stamp = leaseStamp(self.alloc, txn, session.txn_id, parsed.value.epoch) catch |err| return if (err == error.LegacyReaderLease) error.SessionLeaseLost else err;
                if (admitted.tenure != stamp) return error.SessionLeaseLost;
            }
            var renewed = parsed.value;
            renewed.expires_at_ms = @max(renewed.expires_at_ms, nextTxnTimestamp() / std.time.ns_per_ms +| @max(1, ttl / std.time.ns_per_ms));
            const encoded = try std.json.Stringify.valueAlloc(self.alloc, renewed, .{});
            defer self.alloc.free(encoded);
            try txn.put(lease_key, encoded);
        } else if (options.guard) |admitted| {
            try admitted.checkLeaseTxn(self.alloc, txn);
        }
        const expiry_key = try makeSessionExpiryKey(self.alloc, session.last_touched_timestamp, session.txn_id);
        defer self.alloc.free(expiry_key);
        try self.rejectReferencedResurrection(txn, session);
        try self.preserveCompletionTxn(txn, session, false);
        try self.putConnectionExecutionTxn(txn, session);
        try txn.put(key, value);
        try txn.put(expiry_key, &.{});
        try self.putTaggedSqlExpiryTxn(txn, session);
        if (sessionNeedsRecovery(session)) {
            try setSessionRecoveryIndexTxn(self, txn, session);
        } else if (previous_needs_recovery) {
            try clearSessionRecoveryIndexTxn(self, txn, session.txn_id);
        }
    }

    fn taggedSqlBackfillPosition(self: *DurableSessionStore) !?[]u8 {
        return switch (self.backend) {
            .docstore => |store| store.get(self.alloc, tagged_sql_backfill_key) catch |err| if (err == error.NotFound) null else return err,
            .runtime => |store| result: {
                var txn = try store.beginRead();
                defer txn.abort();
                const value = txn.get(tagged_sql_backfill_key) catch |err| if (err == error.NotFound) break :result null else return err;
                break :result try self.alloc.dupe(u8, value);
            },
        };
    }

    fn advanceTaggedSqlBackfill(self: *DurableSessionStore, previous: ?[]const u8, next: []const u8) !void {
        const mutation = self.mutationMutex();
        platform_sync.lockYielding(mutation);
        defer mutation.unlock();
        switch (self.backend) {
            .docstore => |store| {
                var txn = try store.beginWriteTxn();
                errdefer txn.abort();
                if (try advanceTaggedSqlBackfillTxn(&txn, previous, next)) try txn.commit() else txn.abort();
            },
            .runtime => |store| {
                var txn = try store.beginWrite();
                errdefer txn.abort();
                if (try advanceTaggedSqlBackfillTxn(&txn, previous, next)) try txn.commit() else txn.abort();
            },
        }
    }

    fn advanceTaggedSqlBackfillTxn(txn: anytype, previous: ?[]const u8, next: []const u8) !bool {
        const current = txn.get(tagged_sql_backfill_key) catch |err| if (err == error.NotFound) null else return err;
        if (!optionalStringsEqual(current, previous)) return false;
        try txn.put(tagged_sql_backfill_key, next);
        return true;
    }

    fn maintainTaggedSqlPage(self: *DurableSessionStore, ids: []const db_mod.types.TxnId, cutoff: ?u64, removed: *std.AutoHashMapUnmanaged(db_mod.types.TxnId, void)) !usize {
        if (ids.len == 0) return 0;
        const mutation = self.mutationMutex();
        platform_sync.lockYielding(mutation);
        defer mutation.unlock();
        const batch = switch (self.backend) {
            .docstore => |store| result: {
                var txn = try store.beginWriteTxn();
                errdefer txn.abort();
                const batch = try self.maintainTaggedSqlPageTxn(&txn, ids, cutoff, removed);
                if (batch.changed) try txn.commit() else txn.abort();
                break :result batch;
            },
            .runtime => |store| result: {
                var txn = try store.beginWrite();
                errdefer txn.abort();
                const batch = try self.maintainTaggedSqlPageTxn(&txn, ids, cutoff, removed);
                if (batch.changed) try txn.commit() else txn.abort();
                break :result batch;
            },
        };
        return batch.consumed;
    }

    const TaggedSqlBatch = struct { consumed: usize = 0, changed: bool = false };

    fn maintainTaggedSqlPageTxn(self: *DurableSessionStore, txn: anytype, ids: []const db_mod.types.TxnId, cutoff: ?u64, removed: *std.AutoHashMapUnmanaged(db_mod.types.TxnId, void)) !TaggedSqlBatch {
        const byte_budget = 8 << 20;
        var observed_bytes: usize = 0;
        var batch: TaggedSqlBatch = .{};
        for (ids) |id| {
            if (observed_bytes >= byte_budget) break;
            batch.consumed += 1;
            const key = try makeSessionKey(self.alloc, id);
            defer self.alloc.free(key);
            const raw = txn.get(key) catch |err| switch (err) {
                error.NotFound => continue,
                else => return err,
            };
            // Charge the retained compare value before decoding. An individual
            // oversized record gets a transaction of its own when first.
            observed_bytes +|= raw.len;
            if (observed_bytes > byte_budget and batch.consumed > 1) {
                batch.consumed -= 1;
                break;
            }
            var session = decodeSessionRecord(self.alloc, id, raw) catch |err| switch (err) {
                error.InvalidTransactionSessionRecord, error.SyntaxError, error.UnexpectedToken, error.UnexpectedEndOfInput => {
                    std.log.warn("tagged SQL terminal row skipped txn_id={x} err={s}", .{ id, @errorName(err) });
                    continue;
                },
                else => return err,
            };
            defer session.deinit(self.alloc);
            if (!session.prepared_completion_only or !sessionCompletionSettled(session) or !sessionCanExpire(session)) continue;
            const index_key = try makeExpiryKey(self.alloc, tagged_sql_expiry_prefix, session.last_touched_timestamp, id);
            defer self.alloc.free(index_key);
            if (cutoff) |expires_before| {
                self.deleteLoadedSessionTxn(txn, key, session, Version.of(session), expires_before, null) catch |err| switch (err) {
                    error.SessionLeaseLost => continue,
                    else => return err,
                };
                try removed.put(self.alloc, id, {});
            } else if (txn.get(index_key)) |_| {
                continue;
            } else |err| {
                if (err != error.NotFound) return err;
                try txn.put(index_key, &.{});
            }
            batch.changed = true;
        }
        return batch;
    }

    fn expireSqlAbortReceiptPage(self: *DurableSessionStore, ids: []const db_mod.types.TxnId, cutoff: u64) !usize {
        if (ids.len == 0) return 0;
        const mutation = self.mutationMutex();
        platform_sync.lockYielding(mutation);
        defer mutation.unlock();
        return switch (self.backend) {
            .docstore => |store| result: {
                var txn = try store.beginWriteTxn();
                errdefer txn.abort();
                const count = try self.expireSqlAbortReceiptPageTxn(&txn, ids, cutoff);
                if (count != 0) try txn.commit() else txn.abort();
                break :result count;
            },
            .runtime => |store| result: {
                var txn = try store.beginWrite();
                errdefer txn.abort();
                const count = try self.expireSqlAbortReceiptPageTxn(&txn, ids, cutoff);
                if (count != 0) try txn.commit() else txn.abort();
                break :result count;
            },
        };
    }

    fn expireSqlAbortReceiptPageTxn(self: *DurableSessionStore, txn: anytype, ids: []const db_mod.types.TxnId, cutoff: u64) !usize {
        var count: usize = 0;
        for (ids) |id| {
            const expired = self.expireSqlAbortReceiptTxn(txn, id, cutoff) catch |err| switch (err) {
                error.InvalidTransactionSessionRecord => {
                    std.log.warn("invalid SQL terminal receipt retained txn_id={x}", .{id});
                    continue;
                },
                else => return err,
            };
            if (expired) count += 1;
        }
        return count;
    }

    fn expireSqlAbortReceiptTxn(self: *DurableSessionStore, txn: anytype, id: db_mod.types.TxnId, cutoff: u64) !bool {
        const key = try makeSqlAbortReceiptKey(self.alloc, id);
        defer self.alloc.free(key);
        const raw = txn.get(key) catch |err| switch (err) {
            error.NotFound => return false,
            else => return err,
        };
        var receipt = try decodeSqlTerminalReceipt(self.alloc, id, raw);
        defer receipt.deinit(self.alloc);
        if (receipt.last_touched_timestamp >= cutoff) return false;
        if (!receipt.prepared_completion_only and !try self.sqlAbortProofExists(txn, id)) try self.putSqlAbortProof(txn, id);
        const expiry_key = try makeExpiryKey(self.alloc, sql_abort_expiry_prefix, receipt.last_touched_timestamp, id);
        defer self.alloc.free(expiry_key);
        try txn.delete(expiry_key);
        try txn.delete(key);
        return true;
    }

    fn deleteSessionAndExpiryTxn(self: *DurableSessionStore, txn: anytype, key: []const u8, txn_id: db_mod.types.TxnId, expected: Version, cutoff: ?u64, guard: ?Mutation, sql_commit: ?bool) !void {
        const raw = txn.get(key) catch |err| if (err == error.NotFound) return error.SessionLeaseLost else return err;
        var previous = try self.decodeDurableSession(self.alloc, txn_id, raw);
        defer previous.deinit(self.alloc);
        try self.deleteLoadedSessionTxn(txn, key, previous, expected, cutoff, guard);
        if (sql_commit) |committed| if (previous.prepared_completion_only) try self.putSqlCommitDecision(txn, previous, committed);
    }

    fn deleteLoadedSessionTxn(self: *DurableSessionStore, txn: anytype, key: []const u8, previous: Session, expected: Version, cutoff: ?u64, guard: ?Mutation) !void {
        const txn_id = previous.txn_id;
        if (!std.meta.eql(expected, Version.of(previous))) return error.SessionLeaseLost;
        if (guard) |admission| {
            _ = try admission.version(previous);
            try admission.checkLeaseTxn(self.alloc, txn);
        }
        if (cutoff) |expires_before| {
            if (previous.last_touched_timestamp >= expires_before) return error.SessionLeaseLost;
            if (previous.connection_id) |id| {
                if (sql_connection_record.loadTxn(txn, self.alloc, &id)) |loaded| {
                    var connection = loaded;
                    defer connection.deinit();
                    if (std.meta.eql(connection.value.active_txn, @as(?[16]u8, txn_id))) return error.SessionLeaseLost;
                } else |err| if (err != error.SqlConnectionNotFound) return err;
            }
        }
        // Deletion and its terminal witness share one storage commit.
        if (previous.commit_execution_started and !sessionCompletionSettled(previous)) return error.SqlTransactionOutcomeUnknown;
        try self.preserveCompletionTxn(txn, previous, true);
        if (!previous.prepared_completion_only and isLegacySqlAbortReceipt(previous)) try self.putSqlAbortProof(txn, txn_id);
        const expiry_key = try makeSessionExpiryKey(self.alloc, previous.last_touched_timestamp, txn_id);
        defer self.alloc.free(expiry_key);
        txn.delete(expiry_key) catch |err| switch (err) {
            error.NotFound => {},
            else => return err,
        };
        try self.clearTaggedSqlExpiryTxn(txn, previous);
        try updateSessionCount(txn, false, null);
        try txn.delete(key);
        try self.clearSessionLeaseTxn(txn, txn_id);
        const recovery_key = try makeSessionRecoveryKey(self.alloc, txn_id);
        defer self.alloc.free(recovery_key);
        txn.delete(recovery_key) catch |err| switch (err) {
            error.NotFound => {},
            else => return err,
        };
    }
    fn clearSessionLeaseTxn(self: *DurableSessionStore, txn: anytype, txn_id: db_mod.types.TxnId) !void {
        const lease_key = try makeSessionLeaseKey(self.alloc, txn_id);
        defer self.alloc.free(lease_key);
        txn.delete(lease_key) catch |err| if (err != error.NotFound) return err;
        const generation_key = try readerGenerationKey(self.alloc, txn_id);
        defer self.alloc.free(generation_key);
        const generation = try readerGeneration(txn, generation_key);
        if (generation != 0) {
            // Earlier issuers could advance a per-ID generation during a rolling upgrade.
            if (generation >= 1 << 63) {
                var buf: [64]u8 = undefined;
                const counter = try readerClockKey(&buf, txn_id);
                if (generation > try readerGeneration(txn, counter)) {
                    var bytes: [8]u8 = undefined;
                    std.mem.writeInt(u64, &bytes, generation, .little);
                    try txn.put(counter, &bytes);
                }
            }
            try txn.delete(generation_key);
        }
    }

    fn setSessionRecoveryIndexTxn(self: *DurableSessionStore, txn: anytype, session: Session) !void {
        const key = try makeSessionRecoveryKey(self.alloc, session.txn_id);
        defer self.alloc.free(key);
        if (sessionNeedsRecovery(session)) {
            try txn.put(key, &.{});
        } else {
            txn.delete(key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
        }
    }

    fn clearSessionRecoveryIndexTxn(self: *DurableSessionStore, txn: anytype, txn_id: db_mod.types.TxnId) !void {
        const key = try makeSessionRecoveryKey(self.alloc, txn_id);
        defer self.alloc.free(key);
        txn.delete(key) catch |err| switch (err) {
            error.NotFound => {},
            else => return err,
        };
    }

    pub const RecoveryIdPage = struct {
        ids: []db_mod.types.TxnId,
        next_after: ?db_mod.types.TxnId,
    };

    /// Reads only compact recovery-index keys. The cursor is exclusive and a
    /// short page at the end resets it, so callers eventually visit every key
    /// without rescanning the beginning on every maintenance tick.
    pub fn scanRecoveryIds(
        self: *DurableSessionStore,
        alloc: std.mem.Allocator,
        after: ?db_mod.types.TxnId,
        limit: usize,
    ) !RecoveryIdPage {
        var ids = std.ArrayListUnmanaged(db_mod.types.TxnId).empty;
        errdefer ids.deinit(alloc);
        if (limit == 0) return .{ .ids = try ids.toOwnedSlice(alloc), .next_after = null };
        const start_key = if (after) |txn_id| try makeSessionRecoveryKey(alloc, txn_id) else null;
        defer if (start_key) |key| alloc.free(key);
        const Scan = struct {
            allocator: std.mem.Allocator,
            limit: usize,
            ids: *std.ArrayListUnmanaged(db_mod.types.TxnId),

            fn visit(raw: *anyopaque, key: []const u8, _: []const u8) anyerror!bool {
                const scan: *@This() = @ptrCast(@alignCast(raw));
                if (key.len <= session_recovery_prefix.len) return true;
                const txn_id = distributed_txn.parseTxnIdHex(key[session_recovery_prefix.len..]) catch return true;
                try scan.ids.append(scan.allocator, txn_id);
                return scan.ids.items.len < scan.limit;
            }
        };
        var scan = Scan{ .allocator = alloc, .limit = limit, .ids = &ids };
        try self.scanPrefixFromWithContext(session_recovery_prefix, start_key, &scan, Scan.visit);
        const next_after = if (ids.items.len == limit) ids.items[ids.items.len - 1] else null;
        return .{ .ids = try ids.toOwnedSlice(alloc), .next_after = next_after };
    }

    /// Bounded compatibility audit for sessions written before the recovery
    /// index existed. Only `scan_limit` full records are decoded per call.
    const LegacyRecoveryPage = struct { ids: []db_mod.types.TxnId, next_after: ?[]u8 };

    fn scanLegacyRecoveryIds(
        self: *DurableSessionStore,
        alloc: std.mem.Allocator,
        after: ?[]const u8,
        scan_limit: usize,
    ) !LegacyRecoveryPage {
        switch (self.backend) {
            .docstore => |store| {
                var read = try store.beginReadTxn();
                defer read.abort();
                var cursor = try read.openPhysicalCursorAdapter();
                defer cursor.close();
                return scanLegacyRecoveryTxn(&read, &cursor, alloc, after, scan_limit);
            },
            .runtime => |store| {
                var read = try store.beginRead();
                defer read.abort();
                var cursor = try read.openCursor();
                defer cursor.close();
                return scanLegacyRecoveryTxn(&read, &cursor, alloc, after, scan_limit);
            },
        }
    }

    fn scanLegacyRecoveryTxn(read: anytype, cursor: *backend_erased.Cursor, alloc: std.mem.Allocator, start: ?[]const u8, limit: usize) !LegacyRecoveryPage {
        var ids = std.ArrayListUnmanaged(db_mod.types.TxnId).empty;
        errdefer ids.deinit(alloc);
        if (limit == 0) return .{ .ids = try ids.toOwnedSlice(alloc), .next_after = null };
        cursor.setKeysOnly();
        var scanned: usize = 0;
        var last: ?[]u8 = null;
        errdefer if (last) |key| alloc.free(key);
        var entry = try cursor.seekAtOrAfter(start orelse session_prefix);
        while (entry) |row| : (entry = if (scanned < limit) try cursor.next() else null) {
            if (!std.mem.startsWith(u8, row.key, session_prefix)) break;
            if (start) |key| if (std.mem.eql(u8, row.key, key)) continue;
            scanned += 1;
            if (scanned == limit) last = try alloc.dupe(u8, row.key);
            const id = distributed_txn.parseTxnIdHex(row.key[session_prefix.len..]) catch continue;
            var scope = read.openReadScope(alloc) catch |err| {
                std.log.warn("legacy transaction recovery read scope unavailable txn_id={x} err={s}", .{ id, @errorName(err) });
                continue;
            };
            defer scope.close();
            const recovery_key = try makeSessionRecoveryKey(alloc, id);
            defer alloc.free(recovery_key);
            if (scope.get(recovery_key)) |_| {
                continue;
            } else |err| switch (err) {
                error.NotFound => {},
                else => {
                    std.log.warn("legacy transaction recovery index unreadable txn_id={x} err={s}", .{ id, @errorName(err) });
                    continue;
                },
            }
            const may_need_work = recoveryPrefixMayNeedWork(&scope, row.key, id) catch |err| {
                std.log.warn("legacy recovery status unreadable txn_id={x} err={s}", .{ id, @errorName(err) });
                continue;
            };
            if (!may_need_work) continue;
            const raw = scope.get(row.key) catch |err| {
                std.log.warn("legacy transaction recovery row unreadable txn_id={x} err={s}", .{ id, @errorName(err) });
                continue;
            };
            var session = decodeSessionRecord(alloc, id, raw) catch |err| {
                std.log.warn("legacy transaction recovery row invalid txn_id={x} err={s}", .{ id, @errorName(err) });
                continue;
            };
            defer session.deinit(alloc);
            if (sessionNeedsRecovery(session)) try ids.append(alloc, id);
        }
        return .{ .ids = try ids.toOwnedSlice(alloc), .next_after = last };
    }

    /// Rechecks the current durable row under a write transaction before
    /// backfilling its index entry, avoiding stale audit results.
    pub fn refreshRecoveryIndex(self: *DurableSessionStore, txn_id: db_mod.types.TxnId) !void {
        const mutation = self.mutationMutex();
        platform_sync.lockYielding(mutation);
        defer mutation.unlock();
        const session_key = try makeSessionKey(self.alloc, txn_id);
        defer self.alloc.free(session_key);
        const recovery_key = try makeSessionRecoveryKey(self.alloc, txn_id);
        defer self.alloc.free(recovery_key);
        switch (self.backend) {
            .docstore => |store| {
                var txn = try store.beginWriteTxn();
                errdefer txn.abort();
                if (try self.refreshRecoveryIndexTxn(&txn, txn_id, session_key, recovery_key)) try txn.commit() else txn.abort();
            },
            .runtime => |store| {
                var txn = try store.beginWrite();
                errdefer txn.abort();
                if (try self.refreshRecoveryIndexTxn(&txn, txn_id, session_key, recovery_key)) try txn.commit() else txn.abort();
            },
        }
    }

    fn refreshRecoveryIndexTxn(self: *DurableSessionStore, txn: anytype, id: db_mod.types.TxnId, session_key: []const u8, recovery_key: []const u8) !bool {
        const indexed = if (txn.get(recovery_key)) |_| true else |err| switch (err) {
            error.NotFound => false,
            else => return err,
        };
        const raw = txn.get(session_key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        const pending = if (raw) |value| pending: {
            var session = try decodeSessionRecord(self.alloc, id, value);
            defer session.deinit(self.alloc);
            break :pending sessionNeedsRecovery(session);
        } else false;
        if (indexed == pending) return false;
        // A replicated prefix read fences the observed receipt by the
        // namespace epoch, so backfill can submit only the marker change.
        _ = try txn.countPrefix(recovery_key);
        if (pending) try txn.put(recovery_key, &.{}) else try txn.delete(recovery_key);
        return true;
    }

    fn scanExpiredIds(self: *DurableSessionStore, alloc: std.mem.Allocator, prefix: []const u8, after: ?ExpiryPosition, cutoff_ns: u64, limit: usize, positions: ?*std.ArrayListUnmanaged(ExpiryPosition)) !struct { ids: []db_mod.types.TxnId, next_after: ?ExpiryPosition } {
        if (limit == 0) return .{ .ids = &.{}, .next_after = null };
        var ids = std.ArrayListUnmanaged(db_mod.types.TxnId).empty;
        errdefer ids.deinit(alloc);
        const Scan = struct {
            allocator: std.mem.Allocator,
            prefix: []const u8,
            cutoff: u64,
            limit: usize,
            ids: *std.ArrayListUnmanaged(db_mod.types.TxnId),
            positions: ?*std.ArrayListUnmanaged(ExpiryPosition),
            next_after: ?ExpiryPosition = null,
            inspected: usize = 0,
            fn visit(raw: *anyopaque, key: []const u8, _: []const u8) anyerror!bool {
                const scan: *@This() = @ptrCast(@alignCast(raw));
                scan.inspected += 1;
                if (parseExpiryKey(scan.prefix, key)) |parsed| {
                    if (parsed.timestamp >= scan.cutoff) return false;
                    try scan.ids.append(scan.allocator, parsed.txn_id);
                    if (scan.positions) |keys| try keys.append(scan.allocator, ExpiryPosition.after(key, scan.prefix.len).?);
                }
                if (scan.inspected < scan.limit) return true;
                scan.next_after = ExpiryPosition.after(key, scan.prefix.len);
                return false;
            }
        };
        var scan = Scan{ .allocator = alloc, .prefix = prefix, .cutoff = cutoff_ns, .limit = limit, .ids = &ids, .positions = positions };
        const start = if (after) |*position| position.bytes[0..position.len] else null;
        try self.scanPrefixFromWithContextMode(prefix, start, if (after) |position| position.exclusive else false, &scan, Scan.visit, true);
        return .{ .ids = try ids.toOwnedSlice(alloc), .next_after = scan.next_after };
    }

    fn listSessionIds(self: *DurableSessionStore, alloc: std.mem.Allocator, principal: ?[]const u8) ![]db_mod.types.TxnId {
        switch (self.backend) {
            .docstore => |store| {
                var txn = try store.beginReadTxn();
                defer txn.abort();
                var cursor = try txn.openPhysicalKeyCursorAdapter();
                defer cursor.close();
                return listPrincipalSessionIds(&txn, &cursor, alloc, principal);
            },
            .runtime => |store| {
                var txn = try store.beginRead();
                defer txn.abort();
                if (self.listSessionIdsPaged(&txn, alloc, principal)) |ids| return ids else |err| {
                    if (err != error.Unsupported) return err;
                }
                var cursor = try txn.openCursor();
                defer cursor.close();
                cursor.setKeysOnly();
                return listPrincipalSessionIds(&txn, &cursor, alloc, principal);
            },
        }
    }

    fn listSessionIdsPaged(_: *DurableSessionStore, txn: *backend_erased.ReadTxn, alloc: std.mem.Allocator, principal: ?[]const u8) ![]db_mod.types.TxnId {
        var ids: std.ArrayList(db_mod.types.TxnId) = .empty;
        errdefer ids.deinit(alloc);
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(principal orelse "", &digest, .{});
        var page = std.heap.ArenaAllocator.init(alloc);
        defer page.deinit();
        var continuation: [512]u8 = undefined;
        var after: ?[]const u8 = null;
        while (true) {
            _ = page.reset(.retain_capacity);
            const rows = try txn.readPrefixPage(page.allocator(), session_prefix, after, graph_status_read_len);
            for (rows) |row| {
                if (!std.mem.startsWith(u8, row.key, session_prefix)) return error.InvalidTransactionSessionRecord;
                const id = distributed_txn.parseTxnIdHex(row.key[session_prefix.len..]) catch continue;
                const matches = (try sessionPrincipalMatchesPrefix(row.value, principal, &digest)) orelse legacy: {
                    var scope = try txn.openReadScope(alloc);
                    defer scope.close();
                    break :legacy try sessionPrincipalCandidate(&scope, alloc, row.key, principal, &digest);
                };
                if (matches) try ids.append(alloc, id);
            }
            if (rows.len < 64) break;
            const last = rows[rows.len - 1].key;
            if (last.len > continuation.len) return error.InvalidTransactionSessionRecord;
            if (after) |previous| if (std.mem.order(u8, last, previous) != .gt) return error.InvalidTransactionSessionRecord;
            @memcpy(continuation[0..last.len], last);
            after = continuation[0..last.len];
        }
        return ids.toOwnedSlice(alloc);
    }

    fn listPrincipalSessionIds(txn: anytype, cursor: *backend_erased.Cursor, alloc: std.mem.Allocator, principal: ?[]const u8) ![]db_mod.types.TxnId {
        var ids: std.ArrayList(db_mod.types.TxnId) = .empty;
        errdefer ids.deinit(alloc);
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(principal orelse "", &digest, .{});
        var entry = try cursor.seekAtOrAfter(session_prefix);
        while (entry) |row| : (entry = try cursor.next()) {
            if (!std.mem.startsWith(u8, row.key, session_prefix)) break;
            const id = distributed_txn.parseTxnIdHex(row.key[session_prefix.len..]) catch continue;
            var scope = try txn.openReadScope(alloc);
            defer scope.close();
            if (try sessionPrincipalCandidate(&scope, alloc, row.key, principal, &digest))
                try ids.append(alloc, id);
        }
        return ids.toOwnedSlice(alloc);
    }

    pub fn scanPrefixWithContext(
        self: *DurableSessionStore,
        prefix: []const u8,
        ctx: *anyopaque,
        callback: *const fn (ctx: *anyopaque, key: []const u8, value: []const u8) anyerror!bool,
    ) !void {
        return try self.scanPrefixFromWithContext(prefix, null, ctx, callback);
    }

    pub fn scanPrefixFromWithContext(
        self: *DurableSessionStore,
        prefix: []const u8,
        start_after: ?[]const u8,
        ctx: *anyopaque,
        callback: *const fn (ctx: *anyopaque, key: []const u8, value: []const u8) anyerror!bool,
    ) !void {
        return self.scanPrefixFromWithContextMode(prefix, start_after, true, ctx, callback, false);
    }

    fn scanPrefixFromWithContextMode(
        self: *DurableSessionStore,
        prefix: []const u8,
        start_after: ?[]const u8,
        exclusive: bool,
        ctx: *anyopaque,
        callback: *const fn (ctx: *anyopaque, key: []const u8, value: []const u8) anyerror!bool,
        keys_only: bool,
    ) !void {
        switch (self.backend) {
            .docstore => |store| {
                if (keys_only) {
                    var read = try store.beginReadTxn();
                    defer read.abort();
                    var cursor = try read.openPhysicalCursorAdapter();
                    defer cursor.close();
                    cursor.setKeysOnly();
                    var entry = try cursor.seekAtOrAfter(start_after orelse prefix);
                    while (entry) |row| : (entry = try cursor.next()) {
                        if (!std.mem.startsWith(u8, row.key, prefix)) break;
                        if (exclusive) if (start_after) |start| {
                            if (std.mem.eql(u8, row.key, start)) continue;
                        };
                        if (!(try callback(ctx, row.key, &.{}))) break;
                    }
                    return;
                }
                const Adapter = struct {
                    context: *anyopaque,
                    prefix: []const u8,
                    start_after: ?[]const u8,
                    visit: *const fn (ctx: *anyopaque, key: []const u8, value: []const u8) anyerror!bool,

                    fn run(raw: ?*anyopaque, key: []const u8, value: []const u8) anyerror!docstore_mod.DocStore.ScanAction {
                        const adapter: *@This() = @ptrCast(@alignCast(raw.?));
                        if (!std.mem.startsWith(u8, key, adapter.prefix)) return .stop;
                        if (adapter.start_after) |start| if (std.mem.eql(u8, key, start)) return .@"continue";
                        return if (try adapter.visit(adapter.context, key, value)) .@"continue" else .stop;
                    }
                };
                var adapter = Adapter{ .context = ctx, .prefix = prefix, .start_after = if (exclusive) start_after else null, .visit = callback };
                try store.scanWithContext(start_after orelse prefix, &.{}, .{}, &adapter, Adapter.run);
            },
            .runtime => |store| {
                var txn = try store.beginCurrentScan();
                defer txn.abort();
                var cursor = try txn.openCursor();
                defer cursor.close();
                if (keys_only) cursor.setKeysOnly();
                var entry = try cursor.seekAtOrAfter(start_after orelse prefix);
                while (entry) |row| : (entry = try cursor.next()) {
                    if (!std.mem.startsWith(u8, row.key, prefix)) break;
                    if (exclusive) if (start_after) |start| {
                        if (std.mem.eql(u8, row.key, start)) continue;
                    };
                    if (!(try callback(ctx, row.key, row.value))) break;
                }
            },
        }
    }

    // Bootstrap on the first capped admission, counting legacy rows on the same
    // serializable cut as the insert. Once present the counter follows every
    // membership change, including writers configured with no limit.
    fn updateSessionCount(txn: anytype, creating: bool, capacity: ?usize) !void {
        const key = session_record.count_key;
        const raw = txn.get(key) catch |err| if (err == error.NotFound) null else return err;
        const count: u64 = if (raw) |bytes| n: {
            if (bytes.len != 8) return error.InvalidTransactionSessionRecord;
            break :n std.mem.readInt(u64, bytes[0..8], .little);
        } else n: {
            if (capacity == null) return;
            const nrows = try txn.countPrefix(session_prefix);
            if (nrows >= capacity.?) {
                try putSessionCount(txn, nrows);
                return error.SessionCountInitializedAtLimit;
            }
            break :n nrows;
        };
        if (capacity) |limit| if (creating and count >= limit) return error.SessionLimitExceeded;
        const next = if (creating) try std.math.add(u64, count, 1) else try std.math.sub(u64, count, 1);
        try putSessionCount(txn, next);
    }

    fn putSessionCount(txn: anytype, count: u64) !void {
        var bytes: [8]u8 = undefined;
        std.mem.writeInt(u64, &bytes, count, .little);
        try txn.put(session_record.count_key, &bytes);
    }
};

pub const OpenedSessionStore = struct {
    alloc: std.mem.Allocator,
    path_z: [:0]u8,
    docstore: *docstore_mod.DocStore,
    durable: DurableSessionStore,
    lease: SessionLeaseStore,

    pub fn open(alloc: std.mem.Allocator, path: []const u8) !OpenedSessionStore {
        const path_z = try alloc.dupeSentinel(u8, path, 0);
        errdefer alloc.free(path_z);
        const docstore = try alloc.create(docstore_mod.DocStore);
        errdefer alloc.destroy(docstore);
        docstore.* = try docstore_mod.DocStore.open(alloc, path_z, .{});
        errdefer docstore.close();
        return .{
            .alloc = alloc,
            .path_z = path_z,
            .docstore = docstore,
            .durable = DurableSessionStore.init(alloc, docstore),
            .lease = SessionLeaseStore.init(alloc, docstore),
        };
    }

    pub fn deinit(self: *OpenedSessionStore) void {
        self.durable.deinitVolatilePayloads();
        self.docstore.close();
        self.alloc.destroy(self.docstore);
        self.alloc.free(self.path_z);
        self.* = undefined;
    }

    pub fn durableStore(self: *OpenedSessionStore) *DurableSessionStore {
        return &self.durable;
    }

    pub fn leaseStore(self: *OpenedSessionStore) *SessionLeaseStore {
        self.lease.mutation_mutex = self.durable.mutationMutex();
        return &self.lease;
    }
};

pub const SessionStoreScope = enum {
    /// Session records are persisted by their owner node. A miss on another
    /// node must still route to the owner encoded in the transaction ID.
    node_local,
    /// Every node observes the same durable keyspace, so a miss is
    /// authoritative and must not resurrect routing to an obsolete owner.
    cluster_shared,
};

pub const SessionLeaseStore = struct {
    alloc: std.mem.Allocator,
    backend: DurableSessionStore.Backend,
    mutation_mutex: ?*std.atomic.Mutex = null,

    pub fn init(alloc: std.mem.Allocator, store: *docstore_mod.DocStore) SessionLeaseStore {
        return .{
            .alloc = alloc,
            .backend = .{ .docstore = store },
            .mutation_mutex = switch (store.writeSerialization()) {
                .gate => |gate| gate,
                .intrinsic, .unsupported => null,
            },
        };
    }

    pub fn initFromDurable(durable: *DurableSessionStore) SessionLeaseStore {
        return .{ .alloc = durable.alloc, .backend = durable.backend, .mutation_mutex = durable.mutationMutex() };
    }

    pub fn load(self: *const SessionLeaseStore, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId) !?lease_mod.LeaseRecord {
        const key = try makeSessionLeaseKey(self.alloc, txn_id);
        defer self.alloc.free(key);
        var lease = switch (self.backend) {
            .docstore => |store| try lease_mod.Lease.init(self.alloc, store, key),
            .runtime => |store| try lease_mod.Lease.init(self.alloc, store, key),
        };
        defer lease.deinit();
        return try lease.load(alloc);
    }

    fn certifyReader(self: *const SessionLeaseStore, a: std.mem.Allocator, id: db_mod.types.TxnId, owner: u64) !void {
        if (self.mutation_mutex) |mutex| platform_sync.lockYielding(mutex);
        defer if (self.mutation_mutex) |mutex| mutex.unlock();
        switch (self.backend) {
            .docstore => |db| {
                var txn = try db.beginWriteTxn();
                errdefer txn.abort();
                if (try promoteReaderLease(a, &txn, id, owner)) try txn.commit() else txn.abort();
            },
            .runtime => |db| {
                var txn = try db.beginWrite();
                errdefer txn.abort();
                if (try promoteReaderLease(a, &txn, id, owner)) try txn.commit() else txn.abort();
            },
        }
    }

    fn promoteReaderLease(a: std.mem.Allocator, txn: anytype, id: db_mod.types.TxnId, owner: u64) !bool {
        const lease_key = try makeSessionLeaseKey(a, id);
        defer a.free(lease_key);
        const raw = txn.get(lease_key) catch |err| if (err == error.NotFound) return error.SessionLeaseLost else return err;
        const lease = try std.json.parseFromSlice(lease_mod.LeaseRecord, a, raw, .{});
        defer lease.deinit();
        try requireReaderLease(lease.value, owner);
        if (lease.value.epoch >= certified_lease_epoch) return false;
        try issueReaderGeneration(a, txn, id);
        var promoted = lease.value;
        promoted.epoch = certified_lease_epoch;
        const value = try std.json.Stringify.valueAlloc(a, promoted, .{});
        defer a.free(value);
        try txn.put(lease_key, value);
        return true;
    }

    pub fn renew(self: *const SessionLeaseStore, txn_id: db_mod.types.TxnId, owner_node_id: u64, now_ms: u64, ttl_ms: u64) !bool {
        return self.renewMatching(txn_id, owner_node_id, now_ms, ttl_ms, false);
    }

    fn renewMatching(self: *const SessionLeaseStore, txn_id: db_mod.types.TxnId, owner_node_id: u64, now_ms: u64, ttl_ms: u64, existing_owner_only: bool) !bool {
        if (self.mutation_mutex) |mutex| platform_sync.lockYielding(mutex);
        defer if (self.mutation_mutex) |mutex| mutex.unlock();
        const key = try makeSessionLeaseKey(self.alloc, txn_id);
        defer self.alloc.free(key);
        var lease = switch (self.backend) {
            .docstore => |store| try lease_mod.Lease.init(self.alloc, store, key),
            .runtime => |store| try lease_mod.Lease.init(self.alloc, store, key),
        };
        defer lease.deinit();
        const owner_id = try ownerLeaseId(self.alloc, owner_node_id);
        defer self.alloc.free(owner_id);
        if (existing_owner_only) return lease.renewExistingOwner(owner_id, now_ms, ttl_ms);
        return lease.renew(owner_id, now_ms, ttl_ms);
    }

    pub fn release(self: *const SessionLeaseStore, txn_id: db_mod.types.TxnId, owner_node_id: u64) !bool {
        if (self.mutation_mutex) |mutex| platform_sync.lockYielding(mutex);
        defer if (self.mutation_mutex) |mutex| mutex.unlock();
        const key = try makeSessionLeaseKey(self.alloc, txn_id);
        defer self.alloc.free(key);
        var lease = switch (self.backend) {
            .docstore => |store| try lease_mod.Lease.init(self.alloc, store, key),
            .runtime => |store| try lease_mod.Lease.init(self.alloc, store, key),
        };
        defer lease.deinit();
        const owner_id = try ownerLeaseId(self.alloc, owner_node_id);
        defer self.alloc.free(owner_id);
        return try lease.release(owner_id);
    }
};

// Typed native rejection outcomes that public stable-COMMIT already reports
// as safe failures. Never infer abort from transport, cancellation, allocation,
// propagation or generic execution errors.
pub const CommitRejection = enum { none, fresh, confirmed, ambiguous };

pub fn abortCause(err: anyerror) ?error{ RowPolicyDenied, StorageReadTemporarilyUnavailable } {
    if (err == error.PreDispatchDecisionAdmissionUnavailable) return error.StorageReadTemporarilyUnavailable;
    return confirmedAbortCause(err);
}

pub fn confirmedAbortCause(err: anyerror) ?error{ RowPolicyDenied, StorageReadTemporarilyUnavailable } {
    return switch (err) {
        error.TransactionPrepareAbortedRowPolicyDenied => error.RowPolicyDenied,
        error.TransactionPrepareAbortedUnavailable => error.StorageReadTemporarilyUnavailable,
        else => null,
    };
}

pub fn classifyCommitRejection(first_attempt: bool, err: anyerror) CommitRejection {
    if (confirmedAbortCause(err) != null) return .confirmed;
    if (err == error.CatalogGenerationChanged) return .ambiguous;
    const rejected = switch (err) {
        error.PreDispatchDecisionAdmissionUnavailable, error.PreDispatchCatalogGenerationChanged, error.TransactionTooLarge, error.InvalidBatchRequest, error.InvalidArgument, error.InvalidGraphEdges, error.UnsupportedTransformOperation, error.TopologyChanged, error.DecisionConflict, error.DocIdentityNamespaceMismatch, error.UnsupportedOperation, error.TableNotFound, error.UnknownGroup => true,
        else => false,
    };
    if (!rejected) return .none;
    return if (first_attempt) .fresh else .ambiguous;
}

pub const Mutation = struct {
    id: db_mod.types.TxnId,
    owner: u64,
    tenure: ?u128,

    authorization: ?struct { begin_timestamp: u64, principal: ?[32]u8 } = null,

    pub fn bindPublicSession(self: *Mutation, begin_timestamp: u64, principal: ?[]const u8) void {
        self.authorization = .{
            .begin_timestamp = begin_timestamp,
            .principal = principalDigest(principal),
        };
    }

    fn principalDigest(principal: ?[]const u8) ?[32]u8 {
        const name = principal orelse return null;
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(name, &digest, .{});
        return digest;
    }

    pub fn checkLeaseTxn(self: Mutation, alloc: std.mem.Allocator, txn: anytype) !void {
        const admitted = self.tenure orelse return;
        const current = readLeaseTenure(alloc, txn, self.id, self.owner) catch |err|
            return if (err == error.LegacyReaderLease) error.SessionLeaseLost else err;
        if (current != admitted) return error.SessionLeaseLost;
    }

    fn version(self: Mutation, session: Session) !DurableSessionStore.Version {
        if (!std.mem.eql(u8, &self.id, &session.txn_id) or self.owner != session.owner_node_id) return error.SessionLeaseLost;
        if (self.authorization) |authorized| {
            if (session.begin_timestamp != authorized.begin_timestamp or session.graph_receipt or session.connection_id != null or
                !std.meta.eql(authorized.principal, principalDigest(session.principal))) return error.SessionLeaseLost;
        }
        return DurableSessionStore.Version.of(session);
    }
};

pub const SessionRegistry = struct {
    const session_lock_count = 64;

    snapshot_exports: @import("sql/execution/snapshot_exports.zig").Directory = .{},
    snapshot_retirement: @import("sql/execution/retained_snapshot.zig").Retirement = .{},
    snapshot_renew_offset: std.atomic.Value(usize) = .init(0),

    mutex: AtomicMutex = .{},
    session_locks: [session_lock_count]AtomicMutex = @as([session_lock_count]AtomicMutex, @splat(.{})),
    // Record locks protect individual durable mutations. Execution ownership
    // spans 2PC and its response handoff, which must not race a local replay.
    commit_locks: [session_lock_count]AtomicMutex = @as([session_lock_count]AtomicMutex, @splat(.{})),
    sessions: std.AutoHashMapUnmanaged(db_mod.types.TxnId, Session) = .empty,
    durable: ?*DurableSessionStore = null,
    lease_store: ?SessionLeaseStore = null,
    owner_lease_ttl_ns: ?u64 = null,
    max_savepoints: ?usize = null,
    max_sessions: ?usize = null,
    max_record_bytes: ?usize = null,
    durable_scope: SessionStoreScope = .node_local,
    connection_sweep_offset: std.atomic.Value(usize) = .init(0),
    reserved_session_count: usize = 0,
    expiry_cursor: ?ExpiryPosition = null,
    tagged_sql_expiry_cursor: ?ExpiryPosition = null,
    sql_abort_expiry_cursor: ?ExpiryPosition = null,
    sql_abort_migration_lock: AtomicMutex = .{},
    sql_abort_migration_after: std.ArrayList(u8) = .empty,
    sql_abort_migration_rescan_ns: u64 = 0,
    recovery_index_cursor: ?db_mod.types.TxnId = null,
    recovery_audit_cursor: ?[]u8 = null,
    memory_recovery_scan_offset: usize = 0,

    pub const CommitExecution = struct {
        lock: *AtomicMutex,
        retirement: *@import("sql/execution/retained_snapshot.zig").Retirement,
        transaction_id: db_mod.types.TxnId,

        pub fn matches(self: CommitExecution, registry: *SessionRegistry, id: db_mod.types.TxnId) bool {
            return self.retirement == &registry.snapshot_retirement and self.lock == registry.commitLock(id) and std.mem.eql(u8, &self.transaction_id, &id);
        }

        pub fn release(self: CommitExecution) void {
            self.lock.unlock();
            self.retirement.drain();
        }
    };

    pub fn acquireCommitExecution(self: *SessionRegistry, txn_id: db_mod.types.TxnId, io: std.Io) std.Io.Cancelable!CommitExecution {
        const lock = self.commitLock(txn_id);
        // Execution can yield during network/storage work. A contending HTTP
        // retry must yield through its caller's Io as well, including under
        // cooperative execution, and must remain cancellable.
        while (!lock.inner.tryLock()) try io.sleep(.fromMilliseconds(1), .awake);
        return .{ .lock = lock, .retirement = &self.snapshot_retirement, .transaction_id = txn_id };
    }

    /// Maintenance skips live requests instead of waiting on their execution.
    /// The durable recovery index remains intact for a later pass or restart.
    pub fn tryAcquireCommitExecution(self: *SessionRegistry, txn_id: db_mod.types.TxnId) ?CommitExecution {
        const lock = self.commitLock(txn_id);
        if (!lock.inner.tryLock()) return null;
        return .{ .lock = lock, .retirement = &self.snapshot_retirement, .transaction_id = txn_id };
    }

    fn commitLock(self: *SessionRegistry, txn_id: db_mod.types.TxnId) *AtomicMutex {
        return &self.commit_locks[std.hash.Wyhash.hash(0, &txn_id) % session_lock_count];
    }

    pub fn init(durable: ?*DurableSessionStore) SessionRegistry {
        return initWithOptions(durable, null, null, null, null, null);
    }

    pub fn initWithLeaseTtl(durable: ?*DurableSessionStore, lease_store: ?SessionLeaseStore, owner_lease_ttl_ns: ?u64) SessionRegistry {
        return initWithOptions(durable, lease_store, owner_lease_ttl_ns, null, null, null);
    }

    pub fn initWithOptions(
        durable: ?*DurableSessionStore,
        lease_store: ?SessionLeaseStore,
        owner_lease_ttl_ns: ?u64,
        max_savepoints: ?usize,
        max_sessions: ?usize,
        max_record_bytes: ?usize,
    ) SessionRegistry {
        return .{
            .durable = durable,
            .lease_store = lease_store,
            .owner_lease_ttl_ns = owner_lease_ttl_ns,
            .max_savepoints = max_savepoints,
            .max_sessions = max_sessions,
            .max_record_bytes = max_record_bytes,
        };
    }

    pub fn deinit(self: *SessionRegistry, alloc: std.mem.Allocator) void {
        var it = self.sessions.iterator();
        while (it.next()) |entry| entry.value_ptr.deinit(alloc);
        self.sessions.deinit(alloc);
        self.snapshot_exports.deinit(alloc);
        self.snapshot_retirement.drain();
        if (self.recovery_audit_cursor) |key| self.durable.?.alloc.free(key);
        if (self.durable) |durable| self.sql_abort_migration_after.deinit(durable.alloc);
        self.* = .{};
    }

    pub fn hasDurableStore(self: *const SessionRegistry) bool {
        return self.durable != null;
    }

    pub fn durableMissIsAuthoritative(self: *const SessionRegistry) bool {
        return self.durable != null and self.durable_scope == .cluster_shared;
    }

    pub fn begin(self: *SessionRegistry, alloc: std.mem.Allocator, req: BeginRequest, owner_node_id: u64) !SessionInfo {
        return try self.beginForPrincipal(alloc, req, owner_node_id, null);
    }

    pub fn beginForPrincipal(
        self: *SessionRegistry,
        alloc: std.mem.Allocator,
        req: BeginRequest,
        owner_node_id: u64,
        principal: ?[]const u8,
    ) !SessionInfo {
        return self.beginForPrincipalWithSettings(alloc, req, owner_node_id, principal, &.{}, null);
    }

    /// An idle HTTP connection's overlay is copied into the new durable
    /// transaction before the transaction becomes visible. The caller holds
    /// the connection's durable beginning fence until it binds this ID.
    pub fn beginForPrincipalWithSettings(
        self: *SessionRegistry,
        alloc: std.mem.Allocator,
        req: BeginRequest,
        owner_node_id: u64,
        principal: ?[]const u8,
        overlay: []const setting_catalog.OverlayEntry,
        connection_id: ?[32]u8,
    ) !SessionInfo {
        return self.beginForPrincipalWithSettingsAndId(alloc, req, owner_node_id, principal, overlay, connection_id, newSessionTxnId(owner_node_id));
    }

    /// The connection owner persists this exact ID in its beginning fence
    /// before constructing the durable transaction. A crash can therefore
    /// reconcile the two records without guessing which transaction to own.
    pub fn beginForPrincipalWithSettingsAndId(
        self: *SessionRegistry,
        alloc: std.mem.Allocator,
        req: BeginRequest,
        owner_node_id: u64,
        principal: ?[]const u8,
        overlay: []const setting_catalog.OverlayEntry,
        connection_id: ?[32]u8,
        txn_id: db_mod.types.TxnId,
    ) !SessionInfo {
        const now = nextTxnTimestamp();
        var session: Session = .{
            .owner_alloc = alloc,
            .txn_id = txn_id,
            .owner_node_id = owner_node_id,
            .principal = if (principal) |value| try alloc.dupe(u8, value) else null,
            .connection_id = connection_id,
            .prepared_completion_only = req.prepared_completion_only,
            .begin_timestamp = now,
            .last_touched_timestamp = now,
            .sync_level = req.sync_level,
        };
        errdefer session.deinit(alloc);
        if (req.sql) |metadata| session.sql = try metadata.clone(alloc);
        if (overlay.len != 0) {
            if (req.sql == null) return error.InvalidTransactionRequest;
            session.setting_active = try cloneSettingEntries(alloc, overlay);
            session.setting_committed = try cloneSettingEntries(alloc, overlay);
        }
        _ = try self.insertNewSession(alloc, &session);
        return session.info();
    }

    /// Publish the receipt, immutable replay input, recovery index and lease
    /// together. No unexecuted graph ID can escape on a preparation failure.
    pub fn beginGraphExecution(self: *SessionRegistry, alloc: std.mem.Allocator, id: db_mod.types.TxnId, owner: u64, principal: ?[]const u8, input: *const OwnedTransactionCommitRequest, tables: []const distributed_txn.TableCommitRequest) !struct { begin_timestamp: u64, plan: ExecutionPlan } {
        const now = nextTxnTimestamp();
        var session: Session = .{
            .owner_alloc = alloc,
            .txn_id = id,
            .owner_node_id = owner,
            .begin_timestamp = now,
            .last_touched_timestamp = now,
            .sync_level = .full_index,
            .graph_receipt = true,
            .commit_execution_started = true,
        };
        errdefer session.deinit(alloc);
        if (principal) |name| session.principal = try alloc.dupe(u8, name);
        session.staged = try input.clone(alloc);
        try session.refreshListingSummary(alloc, self.max_record_bytes);
        session.commit_body_digest = try commitBodyDigest(alloc, input);
        session.execution_plan = try encodeExecutionPlan(alloc, tables);
        var max_name: usize = 0;
        for (tables) |table| max_name = @max(max_name, table.table_name.len);
        // JSON can escape a byte as six characters. Reserve room for the
        // longest coordinator name, terminal fields, and wider numbers.
        session.graph_admission_growth = 256 +| (6 *| max_name);
        var plan = try parseExecutionPlan(alloc, session.execution_plan.?);
        errdefer plan.deinit();
        const guard = try self.insertNewSession(alloc, &session);
        plan.publication = .{ .guard = guard, .version = DurableSessionStore.Version.of(session) };
        return .{ .begin_timestamp = now, .plan = plan };
    }

    /// Duplicate checks only need key presence. A retry can refer to an
    /// arbitrarily large sealed plan, including one owned by another node.
    pub fn hasSession(self: *SessionRegistry, alloc: std.mem.Allocator, id: db_mod.types.TxnId) !bool {
        if (!self.durableMissIsAuthoritative()) {
            self.mutex.lock();
            const cached = self.sessions.contains(id);
            self.mutex.unlock();
            if (cached) return true;
        }
        const durable = self.durable orelse return false;
        const key = try makeSessionKey(alloc, id);
        defer alloc.free(key);
        switch (durable.backend) {
            .docstore => |store| {
                var txn = try store.beginReadTxn();
                defer txn.abort();
                return txn.contains(key);
            },
            .runtime => |store| {
                var txn = try store.beginProbe();
                defer txn.abort();
                return txn.contains(key);
            },
        }
    }

    fn insertNewSession(self: *SessionRegistry, alloc: std.mem.Allocator, session: *Session) !Mutation {
        const txn_id = session.txn_id;
        const owner_node_id = session.owner_node_id;
        const session_lock = self.sessionLock(txn_id);
        session_lock.lock();
        defer session_lock.unlock();
        if (session.connection_id != null and self.durable == null) return error.SqlConnectionUnavailable;
        if (try self.hasSession(alloc, txn_id)) return error.CypherWriteRequestAlreadyUsed;
        if (session.graph_receipt and self.durable != null and self.lease_store != null and
            (!self.hasColocatedLease() or self.owner_lease_ttl_ns == null)) return error.SessionLeaseLost;
        self.mutex.lock();
        self.ensureSessionCapacityLocked() catch |err| {
            self.mutex.unlock();
            return err;
        };
        self.reserveSessionSlotLocked(alloc) catch |err| {
            self.mutex.unlock();
            return err;
        };
        self.reserved_session_count += 1;
        self.mutex.unlock();
        var reservation_active = true;
        defer if (reservation_active) {
            self.mutex.lock();
            self.reserved_session_count -= 1;
            self.mutex.unlock();
        };
        var guard: Mutation = .{ .id = txn_id, .owner = owner_node_id, .tenure = null };
        if (self.durable != null and self.lease_store != null and self.owner_lease_ttl_ns != null) {
            const ttl_ms = @max(@as(u64, 1), self.owner_lease_ttl_ns.? / std.time.ns_per_ms);
            if (!(try self.durable.?.saveWithLease(session.*, null, session.begin_timestamp / std.time.ns_per_ms, ttl_ms, true, self.max_record_bytes, self.max_sessions, if (session.graph_receipt) &guard.tenure else null))) return error.SessionLeaseLost;
        } else {
            if (self.durable) |durable| try durable.saveRecord(session.*, self.max_record_bytes, .{ .creating = true, .capacity = self.max_sessions });
            try self.renewLeaseLocked(txn_id, owner_node_id);
            if (session.graph_receipt and self.lease_store != null) guard = try self.mutation(alloc, txn_id, owner_node_id);
        }
        self.mutex.lock();
        // A shared-store peer may have expired an older cached incarnation.
        var old = self.sessions.fetchPutAssumeCapacity(txn_id, session.*);
        self.reserved_session_count -= 1;
        reservation_active = false;
        self.mutex.unlock();
        if (old) |*entry| entry.value.deinit(alloc);
        return guard;
    }

    pub const PrincipalAccess = enum {
        missing,
        allowed,
        denied,
    };

    /// Checks the immutable authenticated-subject binding without cloning a
    /// potentially large staged transaction. A legacy unbound record therefore
    /// fails closed for every authenticated principal.
    pub fn principalAccess(
        self: *SessionRegistry,
        _: std.mem.Allocator,
        txn_id: db_mod.types.TxnId,
        principal: ?[]const u8,
    ) !PrincipalAccess {
        const session_lock = self.sessionLock(txn_id);
        session_lock.lock();
        defer session_lock.unlock();

        if (self.durableMissIsAuthoritative()) {
            const durable = self.durable.?;
            var current = (try durable.load(txn_id)) orelse return .missing;
            defer current.deinit(durable.alloc);
            return if (principalsEqual(current.principal, principal)) .allowed else .denied;
        }

        self.mutex.lock();
        if (self.sessions.getPtr(txn_id)) |session| {
            const allowed = principalsEqual(session.principal, principal);
            self.mutex.unlock();
            return if (allowed) .allowed else .denied;
        }
        self.mutex.unlock();

        const durable = self.durable orelse return .missing;
        var loaded = (try durable.load(txn_id)) orelse return .missing;
        defer loaded.deinit(durable.alloc);
        return if (principalsEqual(loaded.principal, principal)) .allowed else .denied;
    }

    pub fn getInfo(self: *SessionRegistry, txn_id: db_mod.types.TxnId) ?SessionInfo {
        return self.readInfo(txn_id) catch null;
    }

    pub fn readMutationInfo(self: *SessionRegistry, guard: Mutation) !?SessionInfo {
        const lock = self.sessionLock(guard.id);
        lock.lock();
        defer lock.unlock();
        var view = (try self.readSessionAssumeStripe(guard.id)) orelse return null;
        defer view.deinit();
        _ = try guard.version(view.session);
        return view.session.info();
    }

    pub fn readInfo(self: *SessionRegistry, txn_id: db_mod.types.TxnId) !?SessionInfo {
        const session_lock = self.sessionLock(txn_id);
        session_lock.lock();
        defer session_lock.unlock();
        if (self.durableMissIsAuthoritative()) {
            const durable = self.durable.?;
            var current = (try durable.load(txn_id)) orelse return null;
            defer current.deinit(durable.alloc);
            return current.info();
        }
        self.mutex.lock();
        if (self.sessions.getPtr(txn_id)) |existing| {
            const info = existing.info();
            self.mutex.unlock();
            return info;
        }
        self.mutex.unlock();
        const durable = self.durable orelse return null;
        var loaded = (try durable.load(txn_id)) orelse return null;
        defer loaded.deinit(durable.alloc);
        return loaded.info();
    }

    pub const SqlState = struct {
        volatile_payload_available: bool = true,
        prepared_completion_only: bool = false,
        version: DurableSessionStore.Version,
        metadata: SqlMetadata,
        sequence_epoch: u64 = 1,
        sequence_state: @import("antfly_local_sources").sql_sequence_session.Snapshot = .{},
        begin_timestamp: u64,
        setting_active: SettingEntries = .empty,
        connection_id: ?[32]u8 = null,
        owner_node_id: u64,
        execution_started: bool,
        terminal: ?TerminalCommitStatus,
        terminal_repair_required: bool = false,
        terminal_prepare: bool = false,
        terminal_abort: bool = false,
        terminal_abort_reason: ?CommitConflictReason = null,
        savepoints: usize,
        /// Existing monotone savepoint identity; zero is the transaction root.
        current_subtransaction: u64 = 0,

        fn fromSession(alloc: std.mem.Allocator, session: *const Session) !?SqlState {
            const metadata = session.sql orelse return null;
            var result: SqlState = .{ .prepared_completion_only = session.prepared_completion_only, .version = DurableSessionStore.Version.of(session.*), .metadata = try metadata.clone(alloc), .connection_id = session.connection_id, .owner_node_id = session.owner_node_id, .execution_started = session.commit_execution_started, .terminal = if (session.terminal_commit) |terminal| terminal.status else null, .terminal_abort = session.terminal_abort, .savepoints = session.savepoints.count(), .volatile_payload_available = session.volatile_payload_available, .begin_timestamp = session.begin_timestamp, .terminal_repair_required = if (session.terminal_commit) |terminal| terminal.repair_required else false, .terminal_prepare = session.external_preparation_prepared, .terminal_abort_reason = session.terminal_abort_reason, .current_subtransaction = session.current_subtransaction, .sequence_epoch = session.sequence_epoch };
            errdefer result.deinit(alloc);
            result.setting_active = try cloneSettingEntries(alloc, session.setting_active.items);
            result.sequence_state = try session.sequence_state.clone(alloc);
            return result;
        }

        pub fn deinit(self: *SqlState, alloc: std.mem.Allocator) void {
            self.sequence_state.deinit(alloc);
            self.metadata.deinit(alloc);
            deinitSettingEntries(alloc, &self.setting_active);
        }
    };

    /// A shared store is authoritative even when this node has a cached session.
    pub fn requireActiveSqlReader(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId, principal: []const u8, owner: u64) !void {
        _ = try self.sqlReaderTenure(alloc, txn_id, principal, owner);
    }

    pub fn sqlReaderTenure(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId, principal: []const u8, owner: u64) !?u128 {
        const stripe = self.sessionLock(txn_id);
        stripe.lock();
        defer stripe.unlock();
        return self.readSqlTenureLocked(alloc, txn_id, principal, owner) catch |err| switch (err) {
            error.LegacyReaderLease => blk: {
                try self.lease_store.?.certifyReader(alloc, txn_id, owner);
                break :blk try self.readSqlTenureLocked(alloc, txn_id, principal, owner);
            },
            else => err,
        };
    }

    fn readSqlTenureLocked(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId, principal: []const u8, owner: u64) !?u128 {
        if (!self.durableMissIsAuthoritative()) {
            self.mutex.lock();
            if (self.sessions.getPtr(txn_id)) |session| {
                const checked = checkSqlReader(session, principal, owner);
                self.mutex.unlock();
                try checked;
                return self.leaseTenure(alloc, txn_id, owner);
            }
            self.mutex.unlock();
        }
        const durable = self.durable orelse return error.SqlTransactionNotActive;
        const shared_lease = self.hasColocatedLease();
        const epoch = switch (durable.backend) {
            .docstore => |store| blk: {
                var read = try store.beginReadTxn();
                defer read.abort();
                break :blk try requireDurableReader(alloc, &read, txn_id, principal, owner, shared_lease);
            },
            .runtime => |store| blk: {
                var read = try store.beginRead();
                defer read.abort();
                break :blk try requireDurableReader(alloc, &read, txn_id, principal, owner, shared_lease);
            },
        };
        return if (shared_lease) epoch else self.leaseTenure(alloc, txn_id, owner);
    }

    // The caller holds the session stripe for the lifetime of a cached view.
    const SessionView = struct {
        session: Session,
        owned_by: ?std.mem.Allocator = null,

        fn deinit(self: *SessionView) void {
            if (self.owned_by) |a| self.session.deinit(a);
        }

        fn stagedRequest(self: *SessionView, alloc: std.mem.Allocator) !OwnedTransactionCommitRequest {
            const session = &self.session;
            if (!session.volatile_payload_available) return error.SqlRelationRecoveryIncomplete;
            const metadata = session.sql orelse return error.SqlTransactionNotActive;
            if (metadata.failed or session.terminal_abort) return error.SqlTransactionAborted;
            if (session.commit_body_digest != null or session.commit_execution_started or session.terminal_commit != null)
                return error.TransactionCommitSealed;
            const staged = session.staged orelse return .{ .sync_level = session.sync_level };
            if (self.owned_by) |a| if (std.meta.eql(a, alloc)) {
                session.staged = null;
                return staged;
            };
            return staged.clone(alloc);
        }
    };

    fn readSessionAssumeStripe(self: *SessionRegistry, txn_id: db_mod.types.TxnId) !?SessionView {
        if (!self.durableMissIsAuthoritative()) {
            self.mutex.lock();
            const cached = self.sessions.get(txn_id);
            self.mutex.unlock();
            if (cached) |session| return .{ .session = session };
        }
        const durable = self.durable orelse return null;
        return .{ .session = (try durable.load(txn_id)) orelse return null, .owned_by = durable.alloc };
    }

    pub const SqlAdmission = struct {
        state: SqlState,
        guard: Mutation,
    };

    pub fn admitSql(self: *SessionRegistry, alloc: std.mem.Allocator, id: db_mod.types.TxnId, principal: ?[]const u8, owner: u64, connection: ?[32]u8, prior: ?Mutation) !SqlAdmission {
        const lock = self.sessionLock(id);
        lock.lock();
        defer lock.unlock();
        return self.admitSqlLocked(alloc, id, principal, owner, connection, prior);
    }

    /// Clone staged rows from the very cache entry admitted by the durable
    /// version and lease check. The stripe pins its slices through the copy.
    pub fn admitSqlStatement(self: *SessionRegistry, registry_alloc: std.mem.Allocator, result_alloc: std.mem.Allocator, guard: Mutation, principal: ?[]const u8, connection: ?[32]u8) !struct { state: SqlState, staged: OwnedTransactionCommitRequest } {
        const admission = try self.admitSqlStatementForOwner(registry_alloc, result_alloc, guard.id, principal, guard.owner, connection, guard);
        return .{ .state = admission.state, .staged = admission.staged };
    }

    pub fn admitSqlStatementForOwner(self: *SessionRegistry, registry_alloc: std.mem.Allocator, result_alloc: std.mem.Allocator, id: db_mod.types.TxnId, principal: ?[]const u8, owner: u64, connection: ?[32]u8, prior: ?Mutation) !struct { state: SqlState, staged: OwnedTransactionCommitRequest, guard: Mutation } {
        const lock = self.sessionLock(id);
        lock.lock();
        defer lock.unlock();
        var admission = try self.admitSqlLocked(registry_alloc, id, principal, owner, connection, prior);
        errdefer admission.state.deinit(registry_alloc);
        if (admission.state.terminal_abort or admission.state.terminal != null) return error.SqlTransactionNotActive;
        if (admission.state.execution_started) return error.SqlTransactionOutcomeUnknown;
        self.mutex.lock();
        const cached = self.sessions.get(id).?;
        self.mutex.unlock();
        var view = SessionView{ .session = cached };
        return .{ .state = admission.state, .staged = try view.stagedRequest(result_alloc), .guard = admission.guard };
    }

    fn admitSqlLocked(self: *SessionRegistry, alloc: std.mem.Allocator, id: db_mod.types.TxnId, principal: ?[]const u8, owner: u64, connection: ?[32]u8, prior: ?Mutation) !SqlAdmission {
        self.mutex.lock();
        const cached = self.sessions.get(id);
        self.mutex.unlock();
        if (!self.durableMissIsAuthoritative()) {
            if (cached) |value| {
                return self.admitSqlState(alloc, null, &value, principal, owner, connection, prior);
            }
        }
        const durable = self.durable orelse return error.SqlTransactionNotActive;
        return self.admitDurableSql(alloc, durable, id, principal, owner, connection, prior, cached) catch |err| switch (err) {
            error.LegacyReaderLease => blk: {
                try self.lease_store.?.certifyReader(alloc, id, owner);
                break :blk self.admitDurableSql(alloc, durable, id, principal, owner, connection, prior, cached);
            },
            else => err,
        };
    }

    fn admitDurableSql(self: *SessionRegistry, alloc: std.mem.Allocator, durable: *DurableSessionStore, id: db_mod.types.TxnId, principal: ?[]const u8, owner: u64, connection: ?[32]u8, prior: ?Mutation, cached: ?Session) !SqlAdmission {
        switch (durable.backend) {
            .docstore => |store| {
                var txn = try store.beginReadTxn();
                defer txn.abort();
                return self.admitSqlTxn(alloc, &txn, id, principal, owner, connection, prior, cached);
            },
            .runtime => |store| {
                var txn = try store.beginRead();
                defer txn.abort();
                return self.admitSqlTxn(alloc, &txn, id, principal, owner, connection, prior, cached);
            },
        }
    }

    fn admitSqlState(self: *SessionRegistry, alloc: std.mem.Allocator, txn: anytype, session: *const Session, principal: ?[]const u8, owner: u64, connection: ?[32]u8, prior: ?Mutation) !SqlAdmission {
        if (!principalsEqual(session.principal, principal)) return error.SqlTransactionNotActive;
        if (!std.meta.eql(session.connection_id, connection)) return error.SqlConnectionNotFound;
        if (session.owner_node_id != owner) return error.SessionLeaseLost;
        const guard: Mutation = if (@TypeOf(txn) != @TypeOf(null) and self.hasColocatedLease())
            .{ .id = session.txn_id, .owner = owner, .tenure = try readLeaseTenure(alloc, txn, session.txn_id, owner) }
        else
            try self.mutation(alloc, session.txn_id, owner);
        if (prior) |admitted| if (!std.meta.eql(admitted, guard)) return error.SessionLeaseLost;
        return .{ .state = (try SqlState.fromSession(alloc, session)) orelse return error.SqlTransactionNotActive, .guard = guard };
    }

    fn admitSqlTxn(self: *SessionRegistry, alloc: std.mem.Allocator, txn: anytype, id: db_mod.types.TxnId, principal: ?[]const u8, owner: u64, connection: ?[32]u8, prior: ?Mutation, cached: ?Session) !SqlAdmission {
        const key = try makeSessionKey(alloc, id);
        defer alloc.free(key);
        // The stripe keeps the cached session alive. Validate its canonical
        // version and the current lease on this same read transaction.
        var full: ?[]const u8 = null;
        if (cached) |value| {
            const narrow = narrow: {
                var scope = txn.openReadScope(alloc) catch |err| switch (err) {
                    error.Unsupported => break :narrow null,
                    else => return err,
                };
                defer scope.close();
                const bytes = scope.probePrefix(key, connection_prefix_len) catch |err| switch (err) {
                    error.NotFound => return error.SqlTransactionNotActive,
                    error.Unsupported, error.EndOfStream => break :narrow null,
                    else => return err,
                };
                break :narrow try parseSqlPrefix(bytes, if (value.connection_id != null) connection_field else sql_field);
            };
            // A legacy backend may lack range reads. Compare its borrowed
            // record prefix before paying to decode staged mutations.
            const packed_state = narrow orelse legacy: {
                full = txn.get(key) catch |err| return if (err == error.NotFound) error.SqlTransactionNotActive else err;
                break :legacy try parseSqlPrefix(full.?, if (value.connection_id != null) connection_field else sql_field);
            };
            if (packed_state) |bytes| {
                if (std.mem.eql(u8, &bytes, &try sqlPrefixPayload(&value))) return self.admitSqlState(alloc, txn, &value, principal, owner, connection, prior);
            }
        }
        const durable = self.durable.?;
        const bytes = full orelse txn.get(key) catch |err| return if (err == error.NotFound) error.SqlTransactionNotActive else err;
        var value = try durable.decodeDurableSession(durable.alloc, id, bytes);
        var retain = false;
        defer if (!retain) value.deinit(durable.alloc);
        var admission = try self.admitSqlState(alloc, txn, &value, principal, owner, connection, prior);
        errdefer admission.state.deinit(alloc);
        self.mutex.lock();
        defer self.mutex.unlock();
        if (self.sessions.getPtr(id)) |entry| {
            if (entry.native_sql_cut) |cut| {
                if (sameNativeSnapshotOwner(entry.*, value)) value.native_sql_cut = cut.retain();
            }
            entry.deinitOwned();
            entry.* = value;
        } else {
            try self.reserveSessionSlotLocked(alloc);
            self.sessions.putAssumeCapacity(id, value);
        }
        retain = true;
        return admission;
    }

    pub fn getSqlState(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId) !?SqlState {
        return self.readSqlState(alloc, txn_id, null);
    }

    /// A portal needs metadata and its staged rows from the same durable cut.
    pub fn readSqlState(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId, staged: ?*OwnedTransactionCommitRequest) !?SqlState {
        const stripe = self.sessionLock(txn_id);
        stripe.lock();
        defer stripe.unlock();
        var view = (try self.readSessionAssumeStripe(txn_id)) orelse return null;
        defer view.deinit();
        var state = (try SqlState.fromSession(alloc, &view.session)) orelse return null;
        errdefer state.deinit(alloc);
        if (staged) |out| out.* = try view.stagedRequest(alloc);
        return state;
    }

    pub const SqlGenerationBinding = struct {
        entry: ?generation_intents.Entry,
        connection_id: ?[32]u8,
        subtransaction: u64,
        freeze_eligible: bool,
    };

    /// Clone only the matching declaration. Staged rows, sequence state, and
    /// unrelated declarations are not needed to resolve one relation.
    pub fn sqlGenerationBinding(
        self: *SessionRegistry,
        arena: std.mem.Allocator,
        txn_id: db_mod.types.TxnId,
        principal: ?[]const u8,
        target: @import("antfly_local_sources").system_catalog_domain.Target,
        writable: bool,
    ) !SqlGenerationBinding {
        const stripe = self.sessionLock(txn_id);
        stripe.lock();
        defer stripe.unlock();
        if (!self.durableMissIsAuthoritative()) {
            self.mutex.lock();
            const cached = self.sessions.get(txn_id);
            self.mutex.unlock();
            if (cached) |current| return bindSqlGeneration(arena, current, principal, target, writable);
        }
        const durable = self.durable orelse return error.SqlTransactionNotActive;
        var loaded = (try durable.load(txn_id)) orelse return error.SqlTransactionNotActive;
        defer loaded.deinit(durable.alloc);
        return bindSqlGeneration(arena, loaded, principal, target, writable);
    }

    fn bindSqlGeneration(arena: std.mem.Allocator, current: Session, principal: ?[]const u8, target: @import("antfly_local_sources").system_catalog_domain.Target, writable: bool) !SqlGenerationBinding {
        if (!principalsEqual(current.principal, principal)) return error.Forbidden;
        const metadata = current.sql orelse return error.SqlTransactionNotActive;
        if (metadata.failed) return error.SqlTransactionAborted;
        if (writable and metadata.mode == .read_only) return error.SqlReadOnlyTransaction;
        if (current.commit_body_digest != null or current.external_preparation != null or current.commit_execution_started or
            current.terminal_commit != null or current.terminal_abort) return error.TransactionCommitSealed;
        if (!current.volatile_payload_available) return error.SqlRelationRecoveryIncomplete;
        if (!std.mem.eql(u8, target.database, metadata.database)) return error.InvalidCatalogMutation;
        const pending = &metadata.catalog_generations;
        const entry = pending.latest(target);
        const freeze_eligible = if (entry != null) eligible: {
            _ = pending.requireFreeze(target, current.current_subtransaction, false) catch |err| switch (err) {
                error.SqlCopyFreezeGenerationRequired => break :eligible false,
                else => return err,
            };
            break :eligible true;
        } else false;
        return .{
            .entry = if (entry) |value| try value.clone(arena) else null,
            .freeze_eligible = freeze_eligible,
            .connection_id = current.connection_id,
            .subtransaction = current.current_subtransaction,
        };
    }

    /// Sequence visibility is connection state, not a transactional write set:
    /// savepoints and statement/transaction rollback never rewind these values.
    pub fn setSqlSequenceState(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId, principal: ?[]const u8, owner: u64, value: @import("antfly_local_sources").sql_sequence_session.Snapshot, reset: bool) !void {
        try value.validate();
        const session_lock = self.sessionLock(txn_id);
        session_lock.lock();
        defer session_lock.unlock();
        var candidate = (try self.loadSessionCloneAssumeStripe(alloc, txn_id)) orelse return error.SqlTransactionNotActive;
        errdefer candidate.deinit(alloc);
        const expected = DurableSessionStore.Version.of(candidate);
        if (candidate.sql == null or candidate.owner_node_id != owner or !std.meta.eql(candidate.principal == null, principal == null)) return error.SqlTransactionNotActive;
        if (candidate.principal) |actual| if (!std.mem.eql(u8, actual, principal.?)) return error.Forbidden;
        if (candidate.commit_execution_started) return error.SqlTransactionOutcomeUnknown;
        if (reset) {
            if (candidate.sequence_epoch == std.math.maxInt(u64)) return error.SqlProgramLimitExceeded;
            candidate.sequence_epoch += 1;
        }
        const next = try value.clone(alloc);
        candidate.sequence_state.deinit(alloc);
        candidate.sequence_state = next;
        touchSession(&candidate);
        try self.persistOwnedVersion(candidate, expected);
        self.mutex.lock();
        defer self.mutex.unlock();
        const target = self.sessions.getPtr(txn_id) orelse return error.SessionRemovedDuringMutation;
        self.publishCandidateLocked(alloc, target, &candidate);
    }

    /// Read-only SQL statements must not consume an obsolete cached session
    /// after another node adopts its durable owner lease.
    pub fn validateSqlLease(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId, owner: u64) !void {
        const store = self.lease_store orelse return;
        var record = (try store.load(alloc, txn_id)) orelse return error.SessionLeaseLost;
        defer lease_mod.deinitRecord(alloc, &record);
        try requireReaderLease(record, owner);
    }

    pub fn setSqlTransactionOptions(
        self: *SessionRegistry,
        alloc: std.mem.Allocator,
        guard: Mutation,
        scope: @import("antfly_local_sources").sql_session.Scope,
        isolation: ?@import("antfly_local_sources").sql_session.Isolation,
        mode: ?@import("antfly_local_sources").sql_session.ReadMode,
    ) !void {
        const session_lock = self.sessionLock(guard.id);
        session_lock.lock();
        defer session_lock.unlock();
        var candidate = (try self.loadSessionCloneAssumeStripe(alloc, guard.id)) orelse return error.SqlTransactionNotActive;
        errdefer candidate.deinit(alloc);
        if (!principalsEqual(candidate.principal, scope.principal) or candidate.connection_id != null)
            return error.SqlTransactionNotActive;
        const expected = try guard.version(candidate);
        try self.checkTenure(alloc, guard);
        if (candidate.terminal_commit != null or candidate.terminal_abort) return error.SqlTransactionNotActive;
        const metadata = if (candidate.sql) |*sql| sql else return error.SqlTransactionNotActive;
        if (!std.mem.eql(u8, metadata.database, scope.database) or !std.mem.eql(u8, metadata.namespace, scope.namespace))
            return error.SqlTransactionNotActive;
        if (metadata.failed) return error.InFailedSqlTransaction;
        if (candidate.commit_execution_started or candidate.commit_body_digest != null or candidate.staged != null or
            candidate.next_savepoint_id != 1 or candidate.read_snapshots.count() != 0 or candidate.lake_cuts.count() != 0)
            return error.ActiveSqlTransaction;
        const next_isolation = isolation orelse metadata.isolation;
        const next_mode = mode orelse metadata.mode;
        if (next_isolation == metadata.isolation and next_mode == metadata.mode) {
            candidate.deinit(alloc);
            return;
        }
        metadata.isolation = next_isolation;
        metadata.mode = next_mode;
        touchSession(&candidate);
        try self.persistMutation(candidate, expected, guard);
        self.mutex.lock();
        defer self.mutex.unlock();
        const publish_target = self.sessions.getPtr(guard.id) orelse return error.SessionRemovedDuringMutation;
        self.publishCandidateLocked(alloc, publish_target, &candidate);
    }

    pub fn mutation(self: *SessionRegistry, alloc: std.mem.Allocator, id: db_mod.types.TxnId, owner: u64) !Mutation {
        const tenure = self.leaseTenure(alloc, id, owner) catch |err| switch (err) {
            error.LegacyReaderLease => blk: {
                try self.lease_store.?.certifyReader(alloc, id, owner);
                break :blk try self.leaseTenure(alloc, id, owner);
            },
            else => return err,
        };
        return .{ .id = id, .owner = owner, .tenure = tenure };
    }

    fn checkTenure(self: *SessionRegistry, alloc: std.mem.Allocator, guard: Mutation) !void {
        const current = self.leaseTenure(alloc, guard.id, guard.owner) catch |err|
            return if (err == error.LegacyReaderLease) error.SessionLeaseLost else err;
        if (current != guard.tenure) return error.SessionLeaseLost;
    }

    fn leaseTenure(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId, owner: u64) !?u128 {
        const store = self.lease_store orelse return null;
        switch (store.backend) {
            .docstore => |db| {
                var read_txn = try db.beginReadTxn();
                defer read_txn.abort();
                return try readLeaseTenure(alloc, &read_txn, txn_id, owner);
            },
            .runtime => |db| {
                var read_txn = try db.beginRead();
                defer read_txn.abort();
                return try readLeaseTenure(alloc, &read_txn, txn_id, owner);
            },
        }
    }

    pub fn setSqlFailed(self: *SessionRegistry, alloc: std.mem.Allocator, guard: Mutation, failed: bool) !void {
        const session_lock = self.sessionLock(guard.id);
        session_lock.lock();
        defer session_lock.unlock();
        var candidate = (try self.loadSessionCloneAssumeStripe(alloc, guard.id)) orelse return error.SqlTransactionNotActive;
        errdefer candidate.deinit(alloc);
        const expected = try guard.version(candidate);
        if (candidate.commit_execution_started) return error.SqlTransactionOutcomeUnknown;
        if (candidate.sql) |*metadata| metadata.failed = failed else return error.SqlTransactionNotActive;
        touchSession(&candidate);
        try self.persistMutation(candidate, expected, guard);
        self.mutex.lock();
        defer self.mutex.unlock();
        const publish_target = self.sessions.getPtr(guard.id) orelse return error.SessionRemovedDuringMutation;
        self.publishCandidateLocked(alloc, publish_target, &candidate);
    }

    /// Failure and its native rollback intent are one durable publication.
    /// Keep the failed SQL shell attached until ROLLBACK/ROLLBACK TO; cleanup
    /// never evaluates SQL or invents a commit decision after response loss.
    pub fn failSqlWithControl(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId, plan: @import("sql/session/command_producer.zig").ControlPlan) !void {
        if (!plan.failure) return error.InvalidLockScope;
        const stripe = self.sessionLock(txn_id);
        stripe.lock();
        defer stripe.unlock();
        var candidate = (try self.loadSessionCloneAssumeStripe(alloc, txn_id)) orelse return error.SqlTransactionNotActive;
        errdefer candidate.deinit(alloc);
        const expected = try self.refreshDurableCandidate(alloc, &candidate, true);
        if (candidate.commit_execution_started or candidate.external_preparation != null or candidate.terminal_commit != null or candidate.terminal_abort) return error.TransactionCommitSealed;
        if (candidate.sql) |*metadata| metadata.failed = true else return error.SqlTransactionNotActive;
        touchSession(&candidate);
        try self.renewLeaseLocked(txn_id, candidate.owner_node_id);
        try self.persistSavepoint(candidate, expected, plan, .{ .rollback = .{ .transaction = txn_id, .first_aborted = candidate.current_subtransaction } }, null);
        self.mutex.lock();
        defer self.mutex.unlock();
        const target = self.sessions.getPtr(txn_id) orelse return error.SessionRemovedDuringMutation;
        self.publishCandidateLocked(alloc, target, &candidate);
    }

    /// Called under the SQL execution lease. The first query fixes snapshot
    /// characteristics durably; later queries take an allocation-free fast path.
    pub fn lockSqlCharacteristics(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId) !void {
        return self.markSqlFlag(alloc, txn_id, "characteristics_locked");
    }

    pub fn markSqlTemporaryAccess(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId) !void {
        return self.markSqlFlag(alloc, txn_id, "temporary_relation_access");
    }

    fn markSqlFlag(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId, comptime field: []const u8) !void {
        const stripe = self.sessionLock(txn_id);
        stripe.lock();
        defer stripe.unlock();
        self.mutex.lock();
        const locked = if (self.sessions.getPtr(txn_id)) |session| if (session.sql) |metadata| @field(metadata, field) else false else false;
        self.mutex.unlock();
        if (locked) return;
        var candidate = (try self.loadSessionCloneAssumeStripe(alloc, txn_id)) orelse return error.SqlTransactionNotActive;
        errdefer candidate.deinit(alloc);
        const expected = DurableSessionStore.Version.of(candidate);
        if (candidate.sql) |*metadata| @field(metadata, field) = true else return error.SqlTransactionNotActive;
        touchSession(&candidate);
        try self.persistOwnedVersion(candidate, expected);
        self.mutex.lock();
        defer self.mutex.unlock();
        const target = self.sessions.getPtr(txn_id) orelse return error.SessionRemovedDuringMutation;
        self.publishCandidateLocked(alloc, target, &candidate);
    }

    pub const NativeSqlCut = struct {
        locked: bool,
        cut: ?*@import("sql/execution/retained_snapshot.zig").Shared,
    };

    /// Caller holds execution ownership, but no registry lock while using it.
    pub fn nativeSqlCut(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId) !NativeSqlCut {
        {
            self.mutex.lock();
            defer self.mutex.unlock();
            if (self.sessions.getPtr(txn_id)) |session| {
                const metadata = session.sql orelse return error.SqlTransactionNotActive;
                if (metadata.failed) return error.SqlTransactionAborted;
                if (session.native_sql_cut) |cut| if (cut.lost.load(.acquire)) return error.RetainedReadRestartRequired;
                return .{ .locked = metadata.characteristics_locked, .cut = if (session.native_sql_cut) |cut| cut.retain() else null };
            }
        }
        // Ordinary metadata reads intentionally do not populate this cache.
        // A recovered first-query marker is evidence of a LOST cut, never an
        // absent transaction or permission to capture a replacement snapshot.
        var recovered = (try self.getSqlState(alloc, txn_id)) orelse return error.SqlTransactionNotActive;
        defer recovered.deinit(alloc);
        if (recovered.metadata.failed) return error.SqlTransactionAborted;
        return .{ .locked = recovered.metadata.characteristics_locked, .cut = null };
    }

    /// Publish the live capability before acknowledging the durable first-query
    /// marker. A crash may lose the capability, never authorize a newer cut.
    /// The caller retains ownership of its reference on all paths.
    pub fn installNativeSqlCut(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId, cut: *@import("sql/execution/retained_snapshot.zig").Shared) !void {
        return self.installSqlCut(alloc, txn_id, cut, false);
    }

    pub fn installImportedSqlCut(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId, cut: *@import("sql/execution/retained_snapshot.zig").Shared) !void {
        return self.installSqlCut(alloc, txn_id, cut, true);
    }

    fn installSqlCut(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId, cut: *@import("sql/execution/retained_snapshot.zig").Shared, imported: bool) !void {
        const stripe = self.sessionLock(txn_id);
        stripe.lock();
        defer stripe.unlock();
        var candidate = (try self.loadSessionCloneAssumeStripe(alloc, txn_id)) orelse return error.SqlTransactionNotActive;
        errdefer candidate.deinit(alloc);
        const expected = DurableSessionStore.Version.of(candidate);
        if (!candidate.volatile_payload_available) return error.SqlRelationRecoveryIncomplete;
        const metadata = if (candidate.sql) |*value| value else return error.SqlTransactionNotActive;
        if (metadata.failed) return error.SqlTransactionAborted;
        if (imported) {
            if (metadata.characteristics_locked or candidate.savepoints.count() != 0) return error.SqlTransactionCharacteristicsLocked;
            if (metadata.isolation != .repeatable_read) return error.SqlSnapshotIsolationMismatch;
            if (candidate.commit_body_digest != null or candidate.commit_execution_started or candidate.terminal_abort or candidate.terminal_commit != null)
                return error.TransactionCommitSealed;
        }
        if (metadata.characteristics_locked or candidate.native_sql_cut != null) return error.RetainedReadRestartRequired;
        candidate.native_sql_cut = cut.retain();
        metadata.characteristics_locked = true;
        touchSession(&candidate);
        try self.persistOwnedVersion(candidate, expected);
        self.mutex.lock();
        defer self.mutex.unlock();
        const target = self.sessions.getPtr(txn_id) orelse return error.SessionRemovedDuringMutation;
        self.publishCandidateLocked(alloc, target, &candidate);
    }

    /// Omitted fields retain their values. No partially applied characteristic
    /// can escape failed persistence, a stale lease, or late snapshot admission.
    pub fn setSqlCharacteristics(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId, changes: @import("antfly_local_sources").sql_session.Characteristics) !void {
        const stripe = self.sessionLock(txn_id);
        stripe.lock();
        defer stripe.unlock();
        var candidate = (try self.loadSessionCloneAssumeStripe(alloc, txn_id)) orelse return error.SqlTransactionNotActive;
        errdefer candidate.deinit(alloc);
        const expected = DurableSessionStore.Version.of(candidate);
        const metadata = if (candidate.sql) |*value| value else return error.SqlTransactionNotActive;
        if (metadata.failed) return error.SqlTransactionAborted;
        if (candidate.commit_body_digest != null or candidate.commit_execution_started or candidate.terminal_abort or candidate.terminal_commit != null) return error.TransactionCommitSealed;
        if (changes.deferrable orelse false) return error.UnsupportedSqlShape;
        const deferrable = changes.deferrable orelse metadata.deferrable;
        const effective_isolation = changes.isolation orelse metadata.isolation;
        const effective_mode = changes.mode orelse metadata.mode;
        // DEFERRABLE is inert in read-write transactions, but it remains a
        // characteristic. A later mode change must not silently claim a safe
        // read-only snapshot that this owner cannot admit.
        if (deferrable and effective_isolation == .serializable and effective_mode == .read_only) return error.UnsupportedSqlShape;
        if (changes.isolation) |isolation| {
            if (metadata.characteristics_locked or candidate.savepoints.count() != 0) return error.SqlTransactionCharacteristicsLocked;
            metadata.isolation = isolation;
        }
        if (changes.mode) |mode| {
            if (mode == .read_write and metadata.mode == .read_only and (metadata.characteristics_locked or candidate.savepoints.count() != 0)) return error.SqlTransactionCharacteristicsLocked;
            metadata.mode = mode;
        }
        metadata.deferrable = deferrable;
        touchSession(&candidate);
        try self.persistOwnedVersion(candidate, expected);
        self.mutex.lock();
        defer self.mutex.unlock();
        const target = self.sessions.getPtr(txn_id) orelse return error.SessionRemovedDuringMutation;
        self.publishCandidateLocked(alloc, target, &candidate);
    }

    /// The registry is the HTTP setting owner: it rechecks the current
    /// principal-scoped catalog before durably changing either overlay.
    const SettingInput = union(enum) { typed: setting_catalog.Value, raw: []const u8 };

    pub fn setSqlSearchPath(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId, principal: ?[]const u8, path: ?@import("../pgwire/search_path.zig").Path, local: bool) !void {
        if (path) |value| try value.validate();
        const session_lock = self.sessionLock(txn_id);
        session_lock.lock();
        defer session_lock.unlock();
        var candidate = (try self.loadSessionCloneAssumeStripe(alloc, txn_id)) orelse return error.SqlTransactionNotActive;
        errdefer candidate.deinit(alloc);
        const expected = DurableSessionStore.Version.of(candidate);
        if (!principalsEqual(candidate.principal, principal)) return error.Forbidden;
        const metadata = if (candidate.sql) |*value| value else return error.SqlTransactionNotActive;
        if (metadata.failed) return error.SqlTransactionAborted;
        if (candidate.commit_body_digest != null or candidate.commit_execution_started or candidate.terminal_commit != null or candidate.terminal_abort) return error.TransactionCommitSealed;
        metadata.search_path_active = path;
        if (!local) metadata.search_path_committed = path;
        touchSession(&candidate);
        try self.persistOwnedVersion(candidate, expected);
        self.mutex.lock();
        defer self.mutex.unlock();
        const publish_target = self.sessions.getPtr(txn_id) orelse return error.SessionRemovedDuringMutation;
        self.publishCandidateLocked(alloc, publish_target, &candidate);
    }

    pub fn setSqlSetting(self: *SessionRegistry, alloc: std.mem.Allocator, guard: Mutation, principal: ?[]const u8, owner: setting_catalog.Owner, name: []const u8, value: setting_catalog.Value, local: bool) !void {
        return self.setSqlSettingInput(alloc, guard, principal, owner, name, .{ .typed = value }, local);
    }

    pub fn setSqlSettingRaw(self: *SessionRegistry, alloc: std.mem.Allocator, guard: Mutation, principal: ?[]const u8, owner: setting_catalog.Owner, name: []const u8, raw: []const u8, local: bool) !void {
        return self.setSqlSettingInput(alloc, guard, principal, owner, name, .{ .raw = raw }, local);
    }

    fn setSqlSettingInput(self: *SessionRegistry, alloc: std.mem.Allocator, guard: Mutation, principal: ?[]const u8, owner: setting_catalog.Owner, name: []const u8, input: SettingInput, local: bool) !void {
        const session_lock = self.sessionLock(guard.id);
        session_lock.lock();
        defer session_lock.unlock();
        var candidate = (try self.loadSessionCloneAssumeStripe(alloc, guard.id)) orelse return error.SqlTransactionNotActive;
        errdefer candidate.deinit(alloc);
        const expected = try guard.version(candidate);
        if (!principalsEqual(candidate.principal, principal)) return error.Forbidden;
        const metadata = candidate.sql orelse return error.SqlTransactionNotActive;
        if (metadata.failed) return error.SqlTransactionAborted;
        if (candidate.commit_body_digest != null or candidate.commit_execution_started) return error.TransactionCommitSealed;
        var view = try setting_catalog.View.capture(alloc, owner, owner.scopeFor(principal orelse "", metadata.database), candidate.setting_active.items);
        defer view.deinit();
        const definition = try view.writable(name);
        const value = try setting_catalog.normalizeDefinedValue(definition, switch (input) {
            .typed => |typed| typed,
            .raw => |raw| try setting_catalog.parseValue(definition.kind, raw),
        });
        if (std.meta.activeTag(value) != definition.kind or (value == .string and (value.string.len > 4096 or !std.unicode.utf8ValidateSlice(value.string)))) return error.InvalidSettingValue;
        const entry: setting_catalog.OverlayEntry = .{ .identity = definition.identity, .value = value };
        try putSettingEntry(alloc, &candidate.setting_active, entry);
        if (!local) try putSettingEntry(alloc, &candidate.setting_committed, entry);
        touchSession(&candidate);
        try self.persistMutation(candidate, expected, guard);
        self.mutex.lock();
        defer self.mutex.unlock();
        const publish_target = self.sessions.getPtr(guard.id) orelse return error.SessionRemovedDuringMutation;
        self.publishCandidateLocked(alloc, publish_target, &candidate);
    }

    pub fn resetSqlSetting(self: *SessionRegistry, alloc: std.mem.Allocator, guard: Mutation, principal: ?[]const u8, owner: setting_catalog.Owner, name: []const u8) !void {
        return self.resetSqlSettingMode(alloc, guard, principal, owner, name, false);
    }

    pub fn resetLocalSqlSetting(self: *SessionRegistry, alloc: std.mem.Allocator, guard: Mutation, principal: ?[]const u8, owner: setting_catalog.Owner, name: []const u8) !void {
        return self.resetSqlSettingMode(alloc, guard, principal, owner, name, true);
    }

    fn resetSqlSettingMode(self: *SessionRegistry, alloc: std.mem.Allocator, guard: Mutation, principal: ?[]const u8, owner: setting_catalog.Owner, name: []const u8, local: bool) !void {
        const session_lock = self.sessionLock(guard.id);
        session_lock.lock();
        defer session_lock.unlock();
        var candidate = (try self.loadSessionCloneAssumeStripe(alloc, guard.id)) orelse return error.SqlTransactionNotActive;
        errdefer candidate.deinit(alloc);
        const expected = try guard.version(candidate);
        if (!principalsEqual(candidate.principal, principal)) return error.Forbidden;
        const metadata = candidate.sql orelse return error.SqlTransactionNotActive;
        if (metadata.failed) return error.SqlTransactionAborted;
        if (candidate.commit_body_digest != null or candidate.commit_execution_started) return error.TransactionCommitSealed;
        var view = try setting_catalog.View.capture(alloc, owner, owner.scopeFor(principal orelse "", metadata.database), candidate.setting_active.items);
        defer view.deinit();
        const definition = try view.writable(name);
        removeSettingEntry(alloc, &candidate.setting_active, definition.identity.id);
        if (!local) removeSettingEntry(alloc, &candidate.setting_committed, definition.identity.id);
        touchSession(&candidate);
        try self.persistMutation(candidate, expected, guard);
        self.mutex.lock();
        defer self.mutex.unlock();
        const publish_target = self.sessions.getPtr(guard.id) orelse return error.SessionRemovedDuringMutation;
        self.publishCandidateLocked(alloc, publish_target, &candidate);
    }

    /// RESET ALL removes only this transaction's client-owned overlays. No
    /// catalog read is needed: clearing stale identities is safe even when a
    /// definition has changed, and the next statement captures fresh defaults.
    pub fn resetAllSqlSettings(self: *SessionRegistry, alloc: std.mem.Allocator, guard: Mutation, principal: ?[]const u8) !void {
        const session_lock = self.sessionLock(guard.id);
        session_lock.lock();
        defer session_lock.unlock();
        var candidate = (try self.loadSessionCloneAssumeStripe(alloc, guard.id)) orelse return error.SqlTransactionNotActive;
        errdefer candidate.deinit(alloc);
        const expected = try guard.version(candidate);
        if (!principalsEqual(candidate.principal, principal)) return error.Forbidden;
        const metadata = candidate.sql orelse return error.SqlTransactionNotActive;
        if (metadata.failed) return error.SqlTransactionAborted;
        if (candidate.commit_body_digest != null or candidate.commit_execution_started) return error.TransactionCommitSealed;
        candidate.sql.?.search_path_active = null;
        candidate.sql.?.search_path_committed = null;
        deinitSettingEntries(alloc, &candidate.setting_active);
        deinitSettingEntries(alloc, &candidate.setting_committed);
        touchSession(&candidate);
        try self.persistMutation(candidate, expected, guard);
        self.mutex.lock();
        defer self.mutex.unlock();
        const publish_target = self.sessions.getPtr(guard.id) orelse return error.SessionRemovedDuringMutation;
        self.publishCandidateLocked(alloc, publish_target, &candidate);
    }

    /// Owned notification intents are sealed with the ordinary SQL decision.
    /// The ingress command lease serializes staging with row/control effects.
    pub fn stageSqlNotification(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId, principal: ?[]const u8, command: notification_types.Command) !void {
        const stripe = self.sessionLock(txn_id);
        stripe.lock();
        defer stripe.unlock();
        var candidate = (try self.loadSessionCloneAssumeStripe(alloc, txn_id)) orelse return error.SqlTransactionNotActive;
        errdefer candidate.deinit(alloc);
        const expected = DurableSessionStore.Version.of(candidate);
        if (!principalsEqual(candidate.principal, principal)) return error.Forbidden;
        const metadata = if (candidate.sql) |*value| value else return error.SqlTransactionNotActive;
        if (metadata.failed) return error.SqlTransactionAborted;
        if (candidate.commit_body_digest != null or candidate.commit_execution_started) return error.TransactionCommitSealed;
        try metadata.notifications.append(alloc, command);
        touchSession(&candidate);
        try self.persistOwnedVersion(candidate, expected);
        self.mutex.lock();
        defer self.mutex.unlock();
        const target = self.sessions.getPtr(txn_id) orelse return error.SessionRemovedDuringMutation;
        self.publishCandidateLocked(alloc, target, &candidate);
    }

    /// Retain a transaction-private declaration under the SQL session lease.
    /// The catalog writer still needs a sealed decision before publication.
    pub fn stageSqlGeneration(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId, principal: ?[]const u8, entry: generation_intents.Entry) !void {
        const stripe = self.sessionLock(txn_id);
        stripe.lock();
        defer stripe.unlock();
        var candidate = (try self.loadSessionCloneAssumeStripe(alloc, txn_id)) orelse return error.SqlTransactionNotActive;
        errdefer candidate.deinit(alloc);
        const expected = try self.refreshDurableCandidate(alloc, &candidate, self.durable != null);
        if (!principalsEqual(candidate.principal, principal)) return error.Forbidden;
        const metadata = if (candidate.sql) |*value| value else return error.SqlTransactionNotActive;
        if (metadata.failed) return error.SqlTransactionAborted;
        if (metadata.mode == .read_only) return error.SqlReadOnlyTransaction;
        if (candidate.commit_body_digest != null or candidate.external_preparation != null or candidate.commit_execution_started or
            candidate.terminal_commit != null or candidate.terminal_abort) return error.TransactionCommitSealed;
        if (!candidate.volatile_payload_available) return error.SqlRelationRecoveryIncomplete;
        if (!std.mem.eql(u8, &entry.transaction, &txn_id) or !std.mem.eql(u8, entry.target.database, metadata.database) or
            entry.subtransaction != candidate.current_subtransaction or entry.birth_subtransaction != candidate.current_subtransaction or
            !entry.freeze_eligible) return error.InvalidTransactionSessionRecord;

        const count = metadata.catalog_generations.entries.items.len;
        try metadata.catalog_generations.append(alloc, entry);
        if (metadata.catalog_generations.entries.items.len == count) {
            candidate.deinit(alloc);
            return;
        }

        metadata.characteristics_locked = true;
        touchSession(&candidate);
        try self.renewLeaseLocked(txn_id, candidate.owner_node_id);
        if (self.durable) |durable| {
            try durable.saveScopedSession(candidate, expected, null, null, &entry.birth, self.max_record_bytes, null);
        } else if (candidate.connection_id != null) return error.SqlConnectionUnavailable;
        self.mutex.lock();
        defer self.mutex.unlock();
        const target = self.sessions.getPtr(txn_id) orelse return error.SessionRemovedDuringMutation;
        self.publishCandidateLocked(alloc, target, &candidate);
    }

    pub const StageValidator = struct {
        ptr: *anyopaque,
        validate: *const fn (*anyopaque, std.mem.Allocator, ?*const OwnedTransactionCommitRequest, *OwnedTransactionCommitRequest, *const OwnedTransactionCommitRequest) anyerror!void,
    };

    pub fn stage(self: *SessionRegistry, alloc: std.mem.Allocator, guard: Mutation, req: *const OwnedTransactionCommitRequest) !?SessionInfo {
        return self.stageValidated(alloc, guard, req, null);
    }

    pub fn stageSql(self: *SessionRegistry, alloc: std.mem.Allocator, guard: Mutation, req: *const OwnedTransactionCommitRequest) !?SessionInfo {
        return self.stageValidated(alloc, guard, req, .{ .ptr = self, .validate = normalizeSqlStage });
    }

    pub fn normalizeSqlStage(_: *anyopaque, alloc: std.mem.Allocator, previous: ?*const OwnedTransactionCommitRequest, merged: *OwnedTransactionCommitRequest, extra: *const OwnedTransactionCommitRequest) !void {
        for (extra.tables) |incoming| {
            const physical = extra.physicalName(incoming.table_name);
            const target = for (merged.tables) |*table| {
                if (std.mem.eql(u8, merged.physicalName(table.table_name), physical)) break table;
            } else return error.InvalidTransactionCommitRequest;
            var latest: std.StringArrayHashMapUnmanaged(?db_mod.types.BatchWrite) = .empty;
            defer latest.deinit(alloc);
            if (previous) |old| for (old.tables) |table| {
                if (!std.mem.eql(u8, old.physicalName(table.table_name), physical)) continue;
                if (table.batch.transforms.len != 0) return error.UnsupportedSqlExecution;
                for (table.batch.writes) |write| try latest.put(alloc, write.key, write);
                for (table.batch.deletes) |key| try latest.put(alloc, key, null);
            };
            if (incoming.batch.transforms.len != 0) return error.UnsupportedSqlExecution;
            for (incoming.batch.writes) |write| try latest.put(alloc, write.key, write);
            for (incoming.batch.deletes) |key| try latest.put(alloc, key, null);
            var writes: std.ArrayList(db_mod.types.BatchWrite) = .empty;
            defer writes.deinit(alloc);
            var deletes: std.ArrayList([]const u8) = .empty;
            defer deletes.deinit(alloc);
            for (latest.keys(), latest.values()) |key, value| {
                if (value) |write| try writes.append(alloc, write) else try deletes.append(alloc, key);
            }
            var replacement: batch_api.OwnedBatchRequest = .{};
            errdefer replacement.deinit(alloc);
            try appendBatchWrites(alloc, &replacement, writes.items);
            try appendBatchDeletes(alloc, &replacement, deletes.items);
            syncBatchReq(&replacement);
            replacement.req.commit_requires = target.batch.req.commit_requires;
            target.batch.deinit(alloc);
            target.batch = replacement;
        }
    }

    /// Statement snapshot without sealing COMMIT. Callers serialize the SQL
    /// statement through acquireCommitExecution before reading/staging it.
    pub fn cloneSqlStaged(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId) !OwnedTransactionCommitRequest {
        const session_lock = self.sessionLock(txn_id);
        session_lock.lock();
        defer session_lock.unlock();
        var view = (try self.readSessionAssumeStripe(txn_id)) orelse return error.SqlTransactionNotActive;
        defer view.deinit();
        return view.stagedRequest(alloc);
    }

    pub fn stageValidated(self: *SessionRegistry, alloc: std.mem.Allocator, guard: Mutation, req: *const OwnedTransactionCommitRequest, validator: ?StageValidator) !?SessionInfo {
        const session_lock = self.sessionLock(guard.id);
        session_lock.lock();
        defer session_lock.unlock();

        var candidate = (try self.loadSessionCloneAssumeStripe(alloc, guard.id)) orelse return null;
        errdefer candidate.deinit(alloc);
        if (!candidate.volatile_payload_available) return error.SqlRelationRecoveryIncomplete;
        const expected = try guard.version(candidate);
        if (candidate.sql) |metadata| {
            if (metadata.failed) return error.SqlTransactionAborted;
            if (metadata.mode == .read_only) for (req.tables) |table| {
                if (table.batch.writes.len != 0 or table.batch.deletes.len != 0 or table.batch.transforms.len != 0 or table.batch.graph_writes.len != 0 or table.batch.graph_deletes.len != 0) return error.SqlReadOnlyTransaction;
            };
        }
        if (candidate.commit_body_digest != null) return error.TransactionCommitSealed;
        if (req.frozen_births.rows != 0) {
            const metadata = candidate.sql orelse return error.InvalidTransactionSessionRecord;
            if (validator == null or metadata.mode == .read_only) return error.InvalidTransactionSessionRecord;
            const pending = metadata.catalog_generations;
            try requireFrozenBirths(alloc, req.*, guard.id, pending.entries.items);
            try requireFrozenStatementImages(alloc, req.*, true);
            for (req.frozen_births.groups.items) |group| {
                const declared = for (pending.entries.items) |entry| {
                    if (std.mem.eql(u8, &entry.nonce, &group.origin.nonce)) break entry;
                } else return error.CatalogGenerationChanged;
                const current = try pending.requireFreeze(declared.target, candidate.current_subtransaction, false);
                if (!current.sameDeclaration(declared)) return error.CatalogGenerationChanged;
            }
        }
        var previous = if (validator != null and candidate.staged != null) try candidate.staged.?.clone(alloc) else null;
        defer if (previous) |*value| value.deinit(alloc);
        if (candidate.staged == null) {
            candidate.staged = try req.clone(alloc);
        } else {
            try candidate.staged.?.invalidateFrozenBirths(req);
            try candidate.staged.?.mergeFrom(alloc, req);
        }
        // Keep only this session's stripe while doing bounded remote reads;
        // the registry mutex is free and durable lease CAS still precedes publish.
        if (validator) |value| try value.validate(value.ptr, alloc, if (previous) |*old| old else null, &candidate.staged.?, req);
        try candidate.refreshListingSummary(alloc, self.max_record_bytes);
        touchSession(&candidate);
        try self.persistMutation(candidate, expected, guard);

        self.mutex.lock();
        defer self.mutex.unlock();
        const publish_target = self.sessions.getPtr(guard.id) orelse return error.SessionRemovedDuringMutation;
        self.publishCandidateLocked(alloc, publish_target, &candidate);
        return publish_target.info();
    }

    pub fn getLakeCut(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId, table: []const u8) !?SessionReadSnapshot {
        const session_lock = self.sessionLock(txn_id);
        session_lock.lock();
        defer session_lock.unlock();
        var session = (try self.loadSessionCloneAssumeStripe(alloc, txn_id)) orelse return error.SqlTransactionNotActive;
        defer session.deinit(alloc);
        return cloneReadSnapshotForKey(alloc, &session.lake_cuts, table, "accepted");
    }
    pub fn cloneLakeCuts(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId) ![]SessionReadSnapshot {
        const session_lock = self.sessionLock(txn_id);
        session_lock.lock();
        defer session_lock.unlock();
        var session = (try self.loadSessionCloneAssumeStripe(alloc, txn_id)) orelse return error.SqlTransactionNotActive;
        defer session.deinit(alloc);
        const result = try alloc.alloc(SessionReadSnapshot, session.lake_cuts.count());
        var initialized: usize = 0;
        errdefer {
            for (result[0..initialized]) |*entry| entry.deinit(alloc);
            alloc.free(result);
        }
        for (session.lake_cuts.values(), result) |entry, *out| {
            out.* = try entry.clone(alloc);
            initialized += 1;
        }
        return result;
    }
    pub fn bindLakeCut(self: *SessionRegistry, alloc: std.mem.Allocator, guard: Mutation, table: []const u8, table_id: u64, token_json: []const u8) !void {
        const session_lock = self.sessionLock(guard.id);
        session_lock.lock();
        defer session_lock.unlock();
        var candidate = (try self.loadSessionCloneAssumeStripe(alloc, guard.id)) orelse return error.SqlTransactionNotActive;
        errdefer candidate.deinit(alloc);
        const expected = try guard.version(candidate);
        if (candidate.commit_body_digest != null or candidate.commit_execution_started or candidate.terminal_abort or candidate.terminal_commit != null) return error.TransactionCommitSealed;
        if (try cloneReadSnapshotForKey(alloc, &candidate.lake_cuts, table, "accepted")) |value| {
            var old = value;
            defer old.deinit(alloc);
            if (old.version != table_id or old.document_json == null or !std.mem.eql(u8, old.document_json.?, token_json)) return error.CatalogGenerationChanged;
            candidate.deinit(alloc);
            return;
        }
        if (candidate.lake_cuts.count() >= 64) return error.SqlProgramLimitExceeded;
        if (candidate.sql) |*metadata| metadata.accepted_lake_reads = true;
        try upsertReadSnapshot(alloc, &candidate.lake_cuts, .{ .table_name = table, .key = "accepted", .version = table_id, .document_json = token_json });
        touchSession(&candidate);
        try self.persistMutation(candidate, expected, guard);
        self.mutex.lock();
        defer self.mutex.unlock();
        const target = self.sessions.getPtr(guard.id) orelse return error.SessionRemovedDuringMutation;
        self.publishCandidateLocked(alloc, target, &candidate);
    }

    pub fn getReadSnapshot(
        self: *SessionRegistry,
        alloc: std.mem.Allocator,
        txn_id: db_mod.types.TxnId,
        table_name: []const u8,
        key: []const u8,
    ) !?SessionReadSnapshot {
        return self.getReadSnapshotBound(alloc, txn_id, table_name, key, null);
    }

    pub fn getReadSnapshotForMutation(self: *SessionRegistry, alloc: std.mem.Allocator, guard: Mutation, table_name: []const u8, key: []const u8) !?SessionReadSnapshot {
        return self.getReadSnapshotBound(alloc, guard.id, table_name, key, guard);
    }

    fn getReadSnapshotBound(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId, table_name: []const u8, key: []const u8, guard: ?Mutation) !?SessionReadSnapshot {
        const session_lock = self.sessionLock(txn_id);
        session_lock.lock();
        defer session_lock.unlock();
        var view = (try self.readSessionAssumeStripe(txn_id)) orelse return null;
        defer view.deinit();
        const session = view.session;
        if (guard) |bound| _ = try bound.version(session);
        return try cloneReadSnapshotForKey(alloc, &session.read_snapshots, table_name, key);
    }

    pub fn stageRead(
        self: *SessionRegistry,
        alloc: std.mem.Allocator,
        guard: Mutation,
        req: *const OwnedTransactionCommitRequest,
        snapshot: StageReadSnapshot,
    ) !?SessionInfo {
        const session_lock = self.sessionLock(guard.id);
        session_lock.lock();
        defer session_lock.unlock();

        var candidate = (try self.loadSessionCloneAssumeStripe(alloc, guard.id)) orelse return null;
        errdefer candidate.deinit(alloc);
        if (!candidate.volatile_payload_available) return error.SqlRelationRecoveryIncomplete;
        const expected = try guard.version(candidate);
        if (candidate.commit_body_digest != null) return error.TransactionCommitSealed;
        if (req.frozen_births.rows != 0) return error.InvalidTransactionSessionRecord;
        try upsertReadSnapshot(alloc, &candidate.read_snapshots, snapshot);
        if (candidate.staged == null) {
            candidate.staged = try req.clone(alloc);
        } else {
            try candidate.staged.?.mergeFrom(alloc, req);
        }
        try candidate.refreshListingSummary(alloc, self.max_record_bytes);
        touchSession(&candidate);
        try self.persistMutation(candidate, expected, guard);

        self.mutex.lock();
        defer self.mutex.unlock();
        const publish_target = self.sessions.getPtr(guard.id) orelse return error.SessionRemovedDuringMutation;
        self.publishCandidateLocked(alloc, publish_target, &candidate);
        return publish_target.info();
    }

    pub fn cloneCommitRequest(
        self: *SessionRegistry,
        alloc: std.mem.Allocator,
        txn_id: db_mod.types.TxnId,
        extra_req: ?*const OwnedTransactionCommitRequest,
    ) !?OwnedTransactionCommitRequest {
        return self.cloneCommitRequestWithSqlSyncLevel(alloc, txn_id, extra_req, null);
    }

    /// Freeze the SQL visibility choice with the first immutable commit body,
    /// not in a second write. Retry/recovery retains the original level even
    /// when a connection's settings have subsequently changed.
    pub fn cloneCommitRequestWithSqlSyncLevel(
        self: *SessionRegistry,
        alloc: std.mem.Allocator,
        txn_id: db_mod.types.TxnId,
        extra_req: ?*const OwnedTransactionCommitRequest,
        sql_sync_level: ?db_mod.types.SyncLevel,
    ) !?OwnedTransactionCommitRequest {
        return self.cloneSqlCommitRequestInternal(alloc, txn_id, extra_req, sql_sync_level, null);
    }

    pub fn cloneScopedSqlCommitRequest(self: *SessionRegistry, a: std.mem.Allocator, txn_id: db_mod.types.TxnId, sync_level: ?db_mod.types.SyncLevel, fence: *const @import("antfly_local_sources").storage_pessimistic_lock_authority.CommandFence) !?OwnedTransactionCommitRequest {
        return self.cloneSqlCommitRequestInternal(a, txn_id, null, sync_level, fence);
    }

    pub fn cloneOwnedCommitRequest(
        self: *SessionRegistry,
        alloc: std.mem.Allocator,
        guard: Mutation,
        extra_req: ?*const OwnedTransactionCommitRequest,
    ) !?OwnedTransactionCommitRequest {
        return self.cloneSqlCommitRequestWithOwner(alloc, guard, extra_req, null, null, null);
    }

    fn cloneSqlCommitRequestInternal(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId, extra_req: ?*const OwnedTransactionCommitRequest, sql_sync_level: ?db_mod.types.SyncLevel, catalog_scope: ?*const @import("antfly_local_sources").storage_pessimistic_lock_authority.CommandFence) !?OwnedTransactionCommitRequest {
        const info = (try self.readInfo(txn_id)) orelse return null;
        return self.cloneSqlCommitRequestWithOwner(alloc, try self.mutation(alloc, txn_id, info.version.owner), extra_req, sql_sync_level, catalog_scope, null);
    }

    pub fn inspectOrSealHttpCommit(self: *SessionRegistry, alloc: std.mem.Allocator, guard: Mutation, extra_req: ?*const OwnedTransactionCommitRequest, terminal: *?TerminalCommit) !?OwnedTransactionCommitRequest {
        return self.cloneSqlCommitRequestWithOwner(alloc, guard, extra_req, null, null, terminal);
    }

    fn cloneSqlCommitRequestWithOwner(self: *SessionRegistry, alloc: std.mem.Allocator, guard: Mutation, extra_req: ?*const OwnedTransactionCommitRequest, sql_sync_level: ?db_mod.types.SyncLevel, catalog_scope: ?*const @import("antfly_local_sources").storage_pessimistic_lock_authority.CommandFence, terminal: ?*?TerminalCommit) !?OwnedTransactionCommitRequest {
        const txn_id = guard.id;
        const session_lock = self.sessionLock(txn_id);
        session_lock.lock();
        defer session_lock.unlock();
        var candidate = (try self.loadSessionCloneAssumeStripe(alloc, txn_id)) orelse return null;
        errdefer candidate.deinit(alloc);
        const expected = try guard.version(candidate);
        if (candidate.terminal_abort) return error.TransactionAlreadyAborted;
        if (!candidate.volatile_payload_available) return error.SqlRelationRecoveryIncomplete;
        if (sql_sync_level != null and candidate.sql == null) return error.SqlTransactionNotActive;
        var pending: ?generation_intents.Pending = null;
        if (candidate.sql) |metadata| {
            if (metadata.failed) return error.SqlTransactionAborted;
            // Ordinary row commit cannot publish transaction-owned catalog generations.
            if (metadata.catalog_generations.entries.items.len != 0) {
                const fence = catalog_scope orelse return error.UnsupportedSqlExecution;
                for (metadata.catalog_generations.entries.items) |entry| {
                    if (entry.birth.group != fence.group or entry.birth.database != fence.database or
                        entry.birth.standalone != fence.standalone or !entry.birth.backend.eql(fence.backend) or
                        !std.mem.eql(u8, &entry.birth.principal, &fence.principal)) return error.CatalogGenerationChanged;
                }
                pending = metadata.catalog_generations;
            }
            if (metadata.mode == .read_only and extra_req != null) for (extra_req.?.tables) |table| {
                if (table.batch.writes.len != 0 or table.batch.deletes.len != 0 or table.batch.transforms.len != 0 or table.batch.graph_writes.len != 0 or table.batch.graph_deletes.len != 0) return error.SqlReadOnlyTransaction;
            };
        }
        const body_digest = try commitBodyDigest(alloc, extra_req);
        if (candidate.commit_body_digest) |sealed_digest| {
            if (!std.mem.eql(u8, &sealed_digest, &body_digest)) return error.TransactionCommitRequestMismatch;
            const sealed = candidate.staged orelse return error.InvalidTransactionSessionRecord;
            var out = try sealed.clone(alloc);
            errdefer out.deinit(alloc);
            if (pending) |generations| try out.discardPrivateGenerations(alloc, generations);
            if (terminal) |result| if (candidate.terminal_commit) |commit| {
                result.* = try commit.clone(alloc);
                candidate.deinit(alloc);
                return out;
            };
            touchSession(&candidate);
            out.session_version = DurableSessionStore.Version.of(candidate);
            try self.persistMutation(candidate, expected, guard);
            self.mutex.lock();
            defer self.mutex.unlock();
            const publish_target = self.sessions.getPtr(txn_id) orelse return error.SessionRemovedDuringMutation;
            self.publishCandidateLocked(alloc, publish_target, &candidate);
            return out;
        }
        var out: OwnedTransactionCommitRequest = if (candidate.staged) |staged|
            try staged.clone(alloc)
        else
            .{ .sync_level = candidate.sync_level };
        errdefer out.deinit(alloc);
        if (extra_req) |req| {
            try out.mergeFrom(alloc, req);
        }
        if (out.tables.len == 0 and pending == null) {
            out.deinit(alloc);
            candidate.deinit(alloc);
            return null;
        }
        if (candidate.sql != null) {
            const level = sql_sync_level orelse try setting_catalog.mutationSyncLevel(candidate.setting_active.items);
            candidate.sync_level = if (level == .propose) .write else level;
            out.sync_level = candidate.sync_level;
        }
        const retained = try out.clone(alloc);
        if (candidate.staged) |*staged| staged.deinit(alloc);
        candidate.staged = retained;
        if (pending) |generations| try out.discardPrivateGenerations(alloc, generations);
        if (extra_req != null) try candidate.refreshListingSummary(alloc, self.max_record_bytes);
        candidate.commit_body_digest = body_digest;
        touchSession(&candidate);
        out.session_version = DurableSessionStore.Version.of(candidate);
        try self.persistMutation(candidate, expected, guard);
        self.mutex.lock();
        defer self.mutex.unlock();
        const publish_target = self.sessions.getPtr(txn_id) orelse return error.SessionRemovedDuringMutation;
        self.publishCandidateLocked(alloc, publish_target, &candidate);
        return out;
    }

    /// Atomically persists the terminal API result before the coordinator's
    /// retained self-acknowledgement is sent. Repeated calls may update the
    /// externally visible pending state, but may not redirect the durable
    /// decision acknowledgement to a different coordinator.
    pub fn recordTerminalCommit(
        self: *SessionRegistry,
        alloc: std.mem.Allocator,
        txn_id: db_mod.types.TxnId,
        status: TerminalCommitStatus,
        coordinator_group_id: ?u64,
        coordinator_table_name: ?[]const u8,
    ) !?void {
        return try self.recordTerminalCommitWithRepair(
            alloc,
            txn_id,
            status,
            false,
            coordinator_group_id,
            coordinator_table_name,
        );
    }

    pub fn recordTerminalCommitWithRepair(
        self: *SessionRegistry,
        alloc: std.mem.Allocator,
        txn_id: db_mod.types.TxnId,
        status: TerminalCommitStatus,
        repair_required: bool,
        coordinator_group_id: ?u64,
        coordinator_table_name: ?[]const u8,
    ) !?void {
        return self.recordTerminalCommitBound(alloc, txn_id, status, repair_required, coordinator_group_id, coordinator_table_name, null);
    }

    pub fn recordTerminalCommitForMutation(self: *SessionRegistry, alloc: std.mem.Allocator, guard: Mutation, status: TerminalCommitStatus, repair_required: bool, coordinator_group_id: ?u64, coordinator_table_name: ?[]const u8) !?void {
        return self.recordTerminalCommitBound(alloc, guard.id, status, repair_required, coordinator_group_id, coordinator_table_name, guard);
    }

    fn recordTerminalCommitBound(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId, status: TerminalCommitStatus, repair_required: bool, coordinator_group_id: ?u64, coordinator_table_name: ?[]const u8, guard: ?Mutation) !?void {
        if ((coordinator_group_id == null) != (coordinator_table_name == null)) return error.InvalidTransactionSessionRecord;
        const session_lock = self.sessionLock(txn_id);
        session_lock.lock();
        defer session_lock.unlock();

        var candidate = (try self.loadSessionCloneAssumeStripe(alloc, txn_id)) orelse return null;
        errdefer candidate.deinit(alloc);
        if (candidate.external_preparation != null) return error.TransactionCommitSealed;
        const expected = if (guard) |bound| try bound.version(candidate) else DurableSessionStore.Version.of(candidate);
        if (candidate.terminal_abort) return error.SqlTransactionAborted;
        const coordinator_acknowledged = if (candidate.terminal_commit) |terminal| blk: {
            const fills_provisional_repair_handoff = terminal.status == .committed and
                terminal.repair_required and
                terminal.coordinator_group_id == null and
                status == .committed and
                repair_required and
                coordinator_group_id != null;
            if (!fills_provisional_repair_handoff and
                (terminal.coordinator_group_id != coordinator_group_id or
                    !optionalStringsEqual(terminal.coordinator_table_name, coordinator_table_name)))
            {
                return error.TransactionCoordinatorMismatch;
            }
            break :blk if (fills_provisional_repair_handoff) false else terminal.coordinator_acknowledged;
        } else false;
        const commit_version = if (candidate.terminal_commit) |terminal| terminal.commit_version else null;
        if (candidate.terminal_commit) |*terminal| terminal.deinit(alloc);
        candidate.terminal_commit = null;
        const owned_coordinator_table_name = if (coordinator_table_name) |table_name| try alloc.dupe(u8, table_name) else null;
        candidate.terminal_commit = .{
            .status = status,
            .repair_required = repair_required,
            .coordinator_group_id = coordinator_group_id,
            .coordinator_table_name = owned_coordinator_table_name,
            .coordinator_acknowledged = coordinator_acknowledged,
            .commit_version = commit_version,
        };
        releaseCompletedReadCut(&candidate);
        touchSession(&candidate);
        try self.persistMutation(candidate, expected, guard);

        self.mutex.lock();
        defer self.mutex.unlock();
        const publish_target = self.sessions.getPtr(txn_id) orelse return error.SessionRemovedDuringMutation;
        self.publishCandidateLocked(alloc, publish_target, &candidate);
        return {};
    }

    /// A coordinator's definite conflict is durably distinguishable from a
    /// committed decision and from an unknown result. Connection detachment
    /// may fail after this point; recovery can still prove the abort.
    pub fn recordTerminalAbort(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId) !?void {
        _ = (try self.recordAttemptAbort(alloc, txn_id, null)) orelse return null;
        return {};
    }

    pub fn recordAttemptAbort(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId, publication: ?ExecutionPlan.Publication) !?DurableSessionStore.Version {
        const abort = (try self.persistAttemptAbort(alloc, txn_id, publication, null, .conflict, null)) orelse return null;
        return abort.version;
    }

    pub fn recordSqlAttemptAbort(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId, publication: ?ExecutionPlan.Publication, cause: SqlAbortAcknowledgement) !?DurableSessionStore.Version {
        const abort = (try self.persistAttemptAbort(alloc, txn_id, publication, null, cause, null)) orelse return null;
        return abort.version;
    }

    pub fn recordTerminalAbortWithReason(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId, reason: ?CommitConflictReason) !?void {
        return self.recordAttemptAbortWithReason(alloc, txn_id, null, reason);
    }

    pub fn recordAttemptAbortWithReason(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId, publication: ?ExecutionPlan.Publication, reason: ?CommitConflictReason) !?void {
        _ = (try self.persistAttemptAbort(alloc, txn_id, publication, null, .conflict, reason)) orelse return null;
        return {};
    }

    fn persistAttemptAbort(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId, publication: ?ExecutionPlan.Publication, guard: ?Mutation, cause: SqlAbortAcknowledgement, reason: ?CommitConflictReason) !?struct { version: DurableSessionStore.Version, retain: bool } {
        const session_lock = self.sessionLock(txn_id);
        session_lock.lock();
        defer session_lock.unlock();
        var candidate = (try self.loadSessionCloneAssumeStripe(alloc, txn_id)) orelse return null;
        errdefer candidate.deinit(alloc);
        if (candidate.external_preparation != null) return error.TransactionCommitSealed;
        if (candidate.terminal_commit != null or !candidate.commit_execution_started) return error.SqlTransactionOutcomeUnknown;
        const expected = if (guard) |bound| try bound.version(candidate) else DurableSessionStore.Version.of(candidate);
        if (publication) |first| if (!std.meta.eql(first.version, expected) or !std.meta.eql(first.guard.id, txn_id)) return error.SessionLeaseLost;
        if (candidate.graph_receipt and candidate.staged != null) {
            candidate.graph_abort_tables = try sessionTableDetails(alloc, candidate.staged, true);
            for (candidate.graph_abort_tables) |*table| {
                const physical = candidate.staged.?.physicalName(table.table_name);
                if (!std.mem.eql(u8, physical, table.table_name))
                    table.storage_name = try alloc.dupe(u8, physical);
            }
            if (candidate.staged) |*staged| staged.deinit(alloc);
            candidate.staged = null;
            candidate.clearListingSummary(alloc);
            if (candidate.execution_plan) |bytes| alloc.free(bytes);
            candidate.execution_plan = null;
        }
        if (!candidate.terminal_abort) candidate.sql_abort_acknowledgement = cause;
        if (reason != null and candidate.terminal_abort and candidate.terminal_abort_reason != reason) return error.SqlTransactionOutcomeUnknown;
        candidate.terminal_abort = true;
        if (reason != null) candidate.terminal_abort_reason = reason;
        releaseCompletedReadCut(&candidate);
        touchSession(&candidate);
        try self.persistMutation(candidate, expected, if (publication) |first| first.guard else guard);
        self.mutex.lock();
        defer self.mutex.unlock();
        const publish_target = self.sessions.getPtr(txn_id) orelse return error.SessionRemovedDuringMutation;
        self.publishCandidateLocked(alloc, publish_target, &candidate);
        return .{ .version = DurableSessionStore.Version.of(publish_target.*), .retain = publish_target.graph_receipt or publish_target.connection_id != null };
    }

    pub fn publishAbortAndRetire(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId, publication: ?ExecutionPlan.Publication) !void {
        return self.publishAbortAndRetireBound(alloc, txn_id, publication, null);
    }

    pub fn publishAbortAndRetireForMutation(self: *SessionRegistry, alloc: std.mem.Allocator, guard: Mutation, publication: ?ExecutionPlan.Publication) !void {
        return self.publishAbortAndRetireBound(alloc, guard.id, publication, guard);
    }

    fn publishAbortAndRetireBound(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId, publication: ?ExecutionPlan.Publication, guard: ?Mutation) !void {
        const terminal = (try self.persistAttemptAbort(alloc, txn_id, publication, guard, .conflict, null)) orelse return error.SessionLeaseLost;
        // The abort is durable even when an attached connection or a concurrent
        // operation prevents immediate retirement.
        if (terminal.retain) return;
        _ = self.removeMode(alloc, txn_id, false, false, .active, terminal.version, if (publication) |first| first.guard else guard) catch false;
    }

    pub fn getExecutionPlan(self: *SessionRegistry, alloc: std.mem.Allocator, guard: Mutation) !?ExecutionPlan {
        const txn_id = guard.id;
        const session_lock = self.sessionLock(txn_id);
        session_lock.lock();
        defer session_lock.unlock();
        var candidate = (try self.loadSessionCloneAssumeStripe(alloc, txn_id)) orelse return null;
        defer candidate.deinit(alloc);
        if (!candidate.volatile_payload_available) return error.SqlRelationRecoveryIncomplete;
        _ = try guard.version(candidate);
        if (candidate.terminal_abort) return error.TransactionAlreadyAborted;
        if (candidate.commit_execution_started) try self.checkTenure(alloc, guard);
        if (candidate.execution_plan) |bytes| return try parseExecutionPlan(alloc, bytes);
        // Older executing document sessions already chose their participant
        // request. Preserve it rather than introduce fresh planning on retry.
        if (!candidate.commit_execution_started) return null;
        const staged = if (candidate.staged) |*value| value else return error.InvalidTransactionSessionRecord;
        const tables_view = try staged.distributedTables(alloc);
        defer tables_view.deinit();
        const tables = tables_view.tables;
        const bytes = try encodeExecutionPlan(alloc, tables);
        defer alloc.free(bytes);
        return try parseExecutionPlan(alloc, bytes);
    }

    pub fn externalPreparationSeal(self: *SessionRegistry, a: std.mem.Allocator, txn_id: db_mod.types.TxnId) !?[]u8 {
        const lock = self.sessionLock(txn_id);
        lock.lock();
        defer lock.unlock();
        self.mutex.lock();
        if (self.sessions.getPtr(txn_id)) |cached| {
            defer self.mutex.unlock();
            if (cached.external_preparation_prepared or cached.terminal_abort or cached.terminal_commit != null or cached.external_receipt_acknowledged) return error.TransactionCommitSealed;
            // Admission needs only the compact purpose seal. Do not clone the
            // staged rows and encoded execution plan on this normal path.
            return if (cached.external_preparation) |bytes| try a.dupe(u8, bytes) else null;
        }
        self.mutex.unlock();
        var candidate = (try self.loadSessionCloneAssumeStripe(a, txn_id)) orelse return null;
        defer candidate.deinit(a);
        if (candidate.external_preparation_prepared or candidate.terminal_abort or candidate.terminal_commit != null or candidate.external_receipt_acknowledged) return error.TransactionCommitSealed;
        return if (candidate.external_preparation) |bytes| try a.dupe(u8, bytes) else null;
    }

    /// Atomically binds external decision purpose, the immutable owner cohort,
    /// and the exact signed plan. Ordinary commit recovery must never consume
    /// this plan. Retries return the first durable publication unchanged.
    pub fn sealExternalExecutionPlan(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId, seal: ExternalPreparationSeal, tables: []const distributed_txn.TableCommitRequest) !?ExecutionPlan {
        const bytes = try encodeExecutionPlan(alloc, tables);
        defer alloc.free(bytes);
        return self.sealExternalEncodedExecutionPlan(alloc, txn_id, seal, bytes);
    }

    pub fn sealExternalEncodedExecutionPlan(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId, seal: ExternalPreparationSeal, encoded_plan: []const u8) !?ExecutionPlan {
        const session_lock = self.sessionLock(txn_id);
        session_lock.lock();
        defer session_lock.unlock();
        var candidate = (try self.loadSessionCloneAssumeStripe(alloc, txn_id)) orelse return null;
        errdefer candidate.deinit(alloc);
        const expected = DurableSessionStore.Version.of(candidate);
        if (!candidate.volatile_payload_available) return error.SqlRelationRecoveryIncomplete;
        if (candidate.external_preparation) |bytes| {
            var region = std.heap.ArenaAllocator.init(alloc);
            defer region.deinit();
            const existing = try std.json.parseFromSliceLeaky(ExternalPreparationSeal, region.allocator(), bytes, .{});
            if (!(try existing.identity.owner(alloc)).eql(try seal.identity.owner(alloc))) return error.ExternalTransactionDecisionChanged;
            const plan = try parseExecutionPlan(alloc, candidate.execution_plan orelse return error.InvalidTransactionSessionRecord);
            candidate.deinit(alloc);
            return plan;
        }
        if (candidate.commit_execution_started or candidate.execution_plan != null or candidate.terminal_commit != null or candidate.terminal_abort) return error.TransactionCommitSealed;
        if (candidate.sql == null or seal.identity.session == null or seal.identity.session.?.owner_node_id != candidate.owner_node_id) return error.InvalidTransactionSessionRecord;
        const plan_bytes = try alloc.dupe(u8, encoded_plan);
        var plan_owned = true;
        errdefer if (plan_owned) alloc.free(plan_bytes);
        try seal.validate(alloc, candidate, plan_bytes);
        const sealed = try std.json.Stringify.valueAlloc(alloc, seal, .{});
        var seal_owned = true;
        errdefer if (seal_owned) alloc.free(sealed);
        if (sealed.len > @import("../system_catalog/prepared_transactions.zig").max_bytes) return error.TransactionSessionTooLarge;
        var result = try parseExecutionPlan(alloc, plan_bytes);
        errdefer result.deinit();
        if (candidate.commit_body_digest == null) candidate.commit_body_digest = try commitBodyDigest(alloc, null);
        candidate.execution_plan = plan_bytes;
        candidate.external_preparation = sealed;
        candidate.commit_execution_started = true;
        plan_owned = false;
        seal_owned = false;
        // Candidate now owns both allocations, including failed persistence.
        try self.persistExternalCandidate(alloc, txn_id, &candidate, expected);
        return result;
    }

    fn persistExternalCandidate(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId, candidate: *Session, expected: DurableSessionStore.Version) !void {
        touchSession(candidate);
        if (self.durable) |durable| try durable.saveRecord(candidate.*, self.max_record_bytes, .{
            .expected = expected,
            .ttl_ns = if (self.lease_store != null) self.owner_lease_ttl_ns else null,
            .external_progress = true,
        });
        self.mutex.lock();
        defer self.mutex.unlock();
        const target = self.sessions.getPtr(txn_id) orelse return error.SessionRemovedDuringMutation;
        self.publishCandidateLocked(alloc, target, candidate);
    }

    /// A known terminal decision no longer needs the transaction's physical
    /// read cut. Keep its sealed plan and acknowledgement debt for recovery.
    /// This is a candidate reference: failed persistence leaves the published
    /// session's own reference intact. Successful publication retires its last
    /// reference; CommitExecution.release closes it outside execution/registry
    /// locks through the existing snapshot retirement queue.
    fn releaseCompletedReadCut(candidate: *Session) void {
        if (candidate.native_sql_cut) |cut| cut.release();
        candidate.native_sql_cut = null;
    }

    /// Only a durable metadata delivery proof may finish the originating SQL
    /// transaction. The later decision on its GID cannot change this result.
    fn releaseExternalExecutionInputs(candidate: *Session, a: std.mem.Allocator) void {
        if (candidate.execution_plan) |bytes| a.free(bytes);
        candidate.execution_plan = null;
        if (candidate.staged) |*request| request.deinit(a);
        candidate.staged = null;
        if (candidate.native_sql_cut) |cut| cut.release();
        candidate.native_sql_cut = null;
        deinitReadSnapshotMap(a, &candidate.lake_cuts);
        candidate.lake_cuts = .empty;
        deinitReadSnapshotMap(a, &candidate.read_snapshots);
        candidate.read_snapshots = .empty;
        var points = candidate.savepoints.valueIterator();
        while (points.next()) |point| point.deinit(a);
        candidate.savepoints.deinit(a);
        candidate.savepoints = .empty;
        candidate.current_subtransaction = 0;
    }

    pub fn recordExternalPreparationReceipt(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId, receipt: @import("../system_catalog/prepared_transactions.zig").Receipt) !?void {
        defer self.snapshot_retirement.drain();
        const session_lock = self.sessionLock(txn_id);
        session_lock.lock();
        defer session_lock.unlock();
        var candidate = (try self.loadSessionCloneAssumeStripe(alloc, txn_id)) orelse return null;
        errdefer candidate.deinit(alloc);
        const expected = DurableSessionStore.Version.of(candidate);
        var region = std.heap.ArenaAllocator.init(alloc);
        defer region.deinit();
        const seal = try std.json.parseFromSliceLeaky(ExternalPreparationSeal, region.allocator(), candidate.external_preparation orelse return error.InvalidTransactionSessionRecord, .{});
        if (receipt.kind != .named or seal.identity.kind != .named) return error.ExternalTransactionDecisionChanged;
        receipt.validate() catch return error.ExternalTransactionDecisionChanged;
        try seal.validate(region.allocator(), candidate, candidate.execution_plan);
        if (receipt.status == .pending or !receipt.owner.eql(try seal.identity.owner(alloc)) or
            !std.mem.eql(u8, &receipt.txn_id, &txn_id) or !std.mem.eql(u8, receipt.gid, seal.identity.gid) or
            !std.meta.eql(receipt.session, seal.identity.session.?) or !std.meta.eql(receipt.lock_scope, seal.identity.lock_scope)) return error.ExternalTransactionDecisionChanged;
        if (candidate.external_preparation_prepared and receipt.status != .prepared or candidate.terminal_abort and receipt.status != .aborted) return error.ExternalTransactionDecisionChanged;
        // Exact delivery replay has already crossed local durability. Avoid
        // another session write or lease renewal before retrying metadata ACK.
        if (candidate.external_preparation_prepared or candidate.terminal_abort) {
            candidate.deinit(alloc);
            return {};
        }
        candidate.external_preparation_prepared = receipt.status == .prepared;
        candidate.terminal_abort = receipt.status == .aborted;
        releaseExternalExecutionInputs(&candidate, alloc);
        try self.persistExternalCandidate(alloc, txn_id, &candidate, expected);
        return {};
    }

    /// Only a resolved private decision can finish this local purpose. Save
    /// its exact proof before releasing inputs or acknowledging metadata.
    pub fn recordExternalCommitReceipt(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId, receipt: @import("../system_catalog/prepared_transactions.zig").Receipt) !?void {
        defer self.snapshot_retirement.drain();
        const lock = self.sessionLock(txn_id);
        lock.lock();
        defer lock.unlock();
        var candidate = (try self.loadSessionCloneAssumeStripe(alloc, txn_id)) orelse return null;
        errdefer candidate.deinit(alloc);
        const expected = DurableSessionStore.Version.of(candidate);
        var scratch = std.heap.ArenaAllocator.init(alloc);
        defer scratch.deinit();
        const a = scratch.allocator();
        const seal = try std.json.parseFromSliceLeaky(ExternalPreparationSeal, a, candidate.external_preparation orelse return error.InvalidTransactionSessionRecord, .{});
        try seal.validate(a, candidate, candidate.execution_plan);
        receipt.validate() catch return error.ExternalTransactionDecisionChanged;
        if (receipt.kind != .session_commit or seal.identity.kind != .session_commit or receipt.status == .pending or receipt.status == .prepared or
            !receipt.owner.eql(try seal.identity.owner(a)) or !std.mem.eql(u8, &receipt.txn_id, &txn_id) or
            !std.meta.eql(receipt.session, seal.identity.session.?) or !std.meta.eql(receipt.lock_scope, seal.identity.lock_scope) or !std.mem.eql(u8, receipt.gid, seal.identity.gid)) return error.ExternalTransactionDecisionChanged;
        const decision = receipt.decision orelse return error.ExternalTransactionDecisionChanged;
        if (decision.version <= seal.identity.begin_timestamp or (decision.status == .committed and decision.sync_level != seal.identity.commit_sync_level)) return error.ExternalTransactionDecisionChanged;
        const status: TerminalCommitStatus = switch (receipt.progress) {
            .resolving => .committed_recovery_pending,
            .indexing => .committed_visibility_pending,
            .ready, .repair_required => .committed,
        };
        const repair = receipt.progress == .repair_required;
        if (candidate.external_decision) |prior| {
            if (!std.meta.eql(prior, decision)) return error.ExternalTransactionDecisionChanged;
            if (candidate.terminal_commit) |terminal| {
                const rank = struct {
                    fn of(value: TerminalCommitStatus) u8 {
                        return switch (value) {
                            .committed_recovery_pending => 0,
                            .committed_visibility_pending => 1,
                            .committed => 2,
                        };
                    }
                }.of;
                // Delayed earlier snapshots cannot downgrade a later proof.
                if (rank(status) < rank(terminal.status)) {
                    candidate.deinit(alloc);
                    return {};
                }
                if (rank(status) == rank(terminal.status)) {
                    if (terminal.repair_required != repair) return error.ExternalTransactionDecisionChanged;
                    candidate.deinit(alloc);
                    return {};
                }
            } else {
                candidate.deinit(alloc);
                return {};
            }
        } else if (candidate.terminal_abort or candidate.terminal_commit != null) return error.ExternalTransactionDecisionChanged;
        candidate.external_decision = decision;
        if (receipt.status == .committed) {
            candidate.terminal_commit = .{ .status = status, .repair_required = repair, .coordinator_acknowledged = true, .commit_version = decision.version };
        } else candidate.terminal_abort = true;
        releaseExternalExecutionInputs(&candidate, alloc);
        try seal.validate(a, candidate, null);
        try self.persistExternalCandidate(alloc, txn_id, &candidate, expected);
        return {};
    }

    /// The authority rejected or permanently fenced the exact original
    /// admission and observed no job/receipt in one metadata read cut. No
    /// native BEGIN was authorized, so there is no native recovery handoff.
    pub fn recordExternalAdmissionRejection(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId, owner: @import("../system_catalog/prepared_transactions.zig").Owner) !?void {
        defer self.snapshot_retirement.drain();
        const lock = self.sessionLock(txn_id);
        lock.lock();
        defer lock.unlock();
        var candidate = (try self.loadSessionCloneAssumeStripe(alloc, txn_id)) orelse return null;
        errdefer candidate.deinit(alloc);
        const expected = DurableSessionStore.Version.of(candidate);
        var region = std.heap.ArenaAllocator.init(alloc);
        defer region.deinit();
        const seal = try std.json.parseFromSliceLeaky(ExternalPreparationSeal, region.allocator(), candidate.external_preparation orelse return error.InvalidTransactionSessionRecord, .{});
        if (candidate.external_preparation_prepared or candidate.external_decision != null or candidate.terminal_commit != null or !(try seal.identity.owner(alloc)).eql(owner)) return error.ExternalTransactionDecisionChanged;
        candidate.terminal_abort = true;
        candidate.external_receipt_acknowledged = true;
        releaseExternalExecutionInputs(&candidate, alloc);
        try self.persistExternalCandidate(alloc, txn_id, &candidate, expected);
        return {};
    }

    pub fn markExternalPreparationReceiptAcknowledged(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId) !?void {
        const session_lock = self.sessionLock(txn_id);
        session_lock.lock();
        defer session_lock.unlock();
        var candidate = (try self.loadSessionCloneAssumeStripe(alloc, txn_id)) orelse return null;
        errdefer candidate.deinit(alloc);
        const expected = DurableSessionStore.Version.of(candidate);
        if (candidate.external_preparation == null or (!candidate.external_preparation_prepared and !candidate.terminal_abort and candidate.terminal_commit == null)) return error.InvalidTransactionSessionRecord;
        if (candidate.terminal_commit) |terminal| if (terminal.status != .committed) return error.InvalidTransactionSessionRecord;
        if (candidate.external_receipt_acknowledged) {
            candidate.deinit(alloc);
            return {};
        }
        candidate.external_receipt_acknowledged = true;
        try self.persistExternalCandidate(alloc, txn_id, &candidate, expected);
        return {};
    }

    /// First successful lease-fenced publisher chooses the immutable plan.
    /// Every concurrent HTTP retry receives that same owned plan before 2PC.
    pub fn sealExecutionPlan(self: *SessionRegistry, alloc: std.mem.Allocator, guard: Mutation, tables: []const distributed_txn.TableCommitRequest) !?ExecutionPlan {
        const txn_id = guard.id;
        const session_lock = self.sessionLock(txn_id);
        session_lock.lock();
        defer session_lock.unlock();
        var candidate = (try self.loadSessionCloneAssumeStripe(alloc, txn_id)) orelse return null;
        errdefer candidate.deinit(alloc);
        if (!candidate.volatile_payload_available) return error.SqlRelationRecoveryIncomplete;
        if (candidate.external_preparation != null) return error.TransactionCommitSealed;
        const expected = try guard.version(candidate);
        if (candidate.terminal_abort) return error.TransactionAlreadyAborted;
        if (candidate.commit_body_digest == null or candidate.staged == null) return error.InvalidTransactionSessionRecord;
        if (candidate.execution_plan) |bytes| {
            try self.checkTenure(alloc, guard);
            const result = try parseExecutionPlan(alloc, bytes);
            candidate.deinit(alloc);
            return result;
        }
        // An old executing record cannot be upgraded to a new plan.
        const selected = if (candidate.commit_execution_started) try candidate.staged.?.distributedTables(alloc) else null;
        defer if (selected) |value| value.deinit();
        candidate.execution_plan = try encodeExecutionPlan(alloc, if (selected) |value| value.tables else tables);
        var result = try parseExecutionPlan(alloc, candidate.execution_plan.?);
        errdefer result.deinit();
        candidate.commit_execution_started = true;
        touchSession(&candidate);
        if (!expected.executing) result.publication = .{ .version = DurableSessionStore.Version.of(candidate), .guard = guard };
        // The storage transaction observes both the original unsealed row and
        // its lease. Only its successful publisher may take first-attempt paths.
        try self.persistMutation(candidate, expected, guard);
        self.mutex.lock();
        defer self.mutex.unlock();
        const target = self.sessions.getPtr(txn_id) orelse return error.SessionRemovedDuringMutation;
        self.publishCandidateLocked(alloc, target, &candidate);
        return result;
    }

    pub fn markCommitExecutionStarted(
        self: *SessionRegistry,
        alloc: std.mem.Allocator,
        txn_id: db_mod.types.TxnId,
    ) !?void {
        const session_lock = self.sessionLock(txn_id);
        session_lock.lock();
        defer session_lock.unlock();
        var candidate = (try self.loadSessionCloneAssumeStripe(alloc, txn_id)) orelse return null;
        const expected = DurableSessionStore.Version.of(candidate);
        errdefer candidate.deinit(alloc);
        if (!candidate.volatile_payload_available) return error.SqlRelationRecoveryIncomplete;
        if (candidate.commit_body_digest == null or candidate.staged == null) return error.InvalidTransactionSessionRecord;
        if (candidate.commit_execution_started) {
            candidate.deinit(alloc);
            return {};
        }
        candidate.commit_execution_started = true;
        touchSession(&candidate);
        try self.persistOwnedVersion(candidate, expected);
        self.mutex.lock();
        defer self.mutex.unlock();
        const publish_target = self.sessions.getPtr(txn_id) orelse return error.SessionRemovedDuringMutation;
        self.publishCandidateLocked(alloc, publish_target, &candidate);
        return {};
    }

    /// Records the durable acknowledgement receipt after the replicated
    /// coordinator command succeeds. This prevents later API retries from
    /// consulting a coordinator route that topology is now free to retire.
    pub fn markTerminalCoordinatorAcknowledged(
        self: *SessionRegistry,
        alloc: std.mem.Allocator,
        txn_id: db_mod.types.TxnId,
    ) !?void {
        return self.markTerminalCoordinatorAcknowledgedBound(alloc, txn_id, null);
    }

    pub fn markTerminalCoordinatorAcknowledgedForMutation(self: *SessionRegistry, alloc: std.mem.Allocator, guard: Mutation) !?void {
        return self.markTerminalCoordinatorAcknowledgedBound(alloc, guard.id, guard);
    }

    fn markTerminalCoordinatorAcknowledgedBound(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId, guard: ?Mutation) !?void {
        const session_lock = self.sessionLock(txn_id);
        session_lock.lock();
        defer session_lock.unlock();

        var candidate = (try self.loadSessionCloneAssumeStripe(alloc, txn_id)) orelse return null;
        errdefer candidate.deinit(alloc);
        const expected = if (guard) |bound| try bound.version(candidate) else DurableSessionStore.Version.of(candidate);
        const terminal = if (candidate.terminal_commit) |*value| value else return error.InvalidTransactionSessionRecord;
        if (terminal.coordinator_group_id == null) return error.InvalidTransactionSessionRecord;
        if (terminal.coordinator_acknowledged) {
            candidate.deinit(alloc);
            return {};
        }
        terminal.coordinator_acknowledged = true;
        touchSession(&candidate);
        try self.persistMutation(candidate, expected, guard);

        self.mutex.lock();
        defer self.mutex.unlock();
        const publish_target = self.sessions.getPtr(txn_id) orelse return error.SessionRemovedDuringMutation;
        self.publishCandidateLocked(alloc, publish_target, &candidate);
        return {};
    }

    pub fn getTerminalCommit(
        self: *SessionRegistry,
        alloc: std.mem.Allocator,
        txn_id: db_mod.types.TxnId,
    ) !?TerminalCommit {
        return self.getTerminalCommitBound(alloc, txn_id, null);
    }

    pub fn getTerminalCommitForMutation(self: *SessionRegistry, alloc: std.mem.Allocator, guard: Mutation) !?TerminalCommit {
        return self.getTerminalCommitBound(alloc, guard.id, guard);
    }

    fn getTerminalCommitBound(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId, guard: ?Mutation) !?TerminalCommit {
        const session_lock = self.sessionLock(txn_id);
        session_lock.lock();
        defer session_lock.unlock();
        var view = (try self.readSessionAssumeStripe(txn_id)) orelse return null;
        defer view.deinit();
        const session = view.session;
        if (guard) |bound| _ = try bound.version(session);
        const terminal = session.terminal_commit orelse return null;
        return try terminal.clone(alloc);
    }

    /// Returns a bounded, rotating batch of stable transactions that require
    /// either idempotent commit replay or a coordinator acknowledgement.
    pub fn listPendingRecoveryIds(
        self: *SessionRegistry,
        alloc: std.mem.Allocator,
        limit: usize,
    ) ![]db_mod.types.TxnId {
        var pending = std.ArrayListUnmanaged(db_mod.types.TxnId).empty;
        errdefer pending.deinit(alloc);
        if (limit == 0) return try pending.toOwnedSlice(alloc);

        if (self.durable) |durable| {
            self.mutex.lock();
            const index_after = self.recovery_index_cursor;
            const audit_after: ?(std.mem.Allocator.Error![]u8) = if (self.recovery_audit_cursor) |key| alloc.dupe(u8, key) else null;
            self.mutex.unlock();
            const audit_start = if (audit_after) |copy| copy catch |err| {
                std.log.warn("legacy recovery continuation unavailable err={s}", .{@errorName(err)});
                return self.indexedRecoveryOnly(alloc, index_after, limit);
            } else null;
            defer if (audit_start) |key| alloc.free(key);

            const indexed = try durable.scanRecoveryIds(alloc, index_after, limit);
            defer alloc.free(indexed.ids);
            try pending.appendSlice(alloc, indexed.ids);

            // Continuously audit a small bounded page. This backfills records
            // written by a previous binary and self-heals a missing index key.
            const audit_budget = @min(limit, 8);
            // Publish indexed progress even if legacy storage is unreadable.
            self.mutex.lock();
            self.recovery_index_cursor = indexed.next_after;
            self.mutex.unlock();

            const audited = durable.scanLegacyRecoveryIds(durable.alloc, audit_start, audit_budget) catch |err| {
                std.log.warn("legacy transaction recovery audit deferred err={s}", .{@errorName(err)});
                return pending.toOwnedSlice(alloc);
            };
            defer durable.alloc.free(audited.ids);
            var cursor_transferred = false;
            defer if (!cursor_transferred) {
                if (audited.next_after) |key| durable.alloc.free(key);
            };
            for (audited.ids) |txn_id| {
                durable.refreshRecoveryIndex(txn_id) catch |err| {
                    std.log.warn("legacy transaction recovery index backfill deferred txn_id={x} err={s}", .{ txn_id, @errorName(err) });
                };
                if (pending.items.len >= limit) continue;
                var duplicate = false;
                for (pending.items) |existing| {
                    if (std.mem.eql(u8, &existing, &txn_id)) {
                        duplicate = true;
                        break;
                    }
                }
                if (!duplicate) try pending.append(alloc, txn_id);
            }

            self.mutex.lock();
            if (self.recovery_audit_cursor) |key| durable.alloc.free(key);
            self.recovery_audit_cursor = audited.next_after;
            cursor_transferred = true;
            self.mutex.unlock();
        } else {
            self.mutex.lock();
            defer self.mutex.unlock();
            var it = self.sessions.iterator();
            var scan_offset = self.memory_recovery_scan_offset;
            var skipped: usize = 0;
            while (skipped < scan_offset and it.next() != null) skipped += 1;
            // Hash-map deletions can shrink the collection between maintenance
            // passes. Restart immediately instead of returning an avoidable
            // empty page when the ordinal cursor is now beyond the end.
            if (skipped < scan_offset) {
                scan_offset = 0;
                it = self.sessions.iterator();
            }
            var scanned: usize = 0;
            const scan_limit = @max(limit, 1) *| 4;
            var exhausted = true;
            while (it.next()) |entry| {
                scanned += 1;
                if (sessionNeedsRecovery(entry.value_ptr.*)) try pending.append(alloc, entry.key_ptr.*);
                if (pending.items.len >= limit or scanned >= scan_limit) {
                    exhausted = false;
                    break;
                }
            }
            self.memory_recovery_scan_offset = if (exhausted) 0 else scan_offset +| scanned;
        }
        return try pending.toOwnedSlice(alloc);
    }

    fn indexedRecoveryOnly(self: *SessionRegistry, alloc: std.mem.Allocator, after: ?db_mod.types.TxnId, limit: usize) ![]db_mod.types.TxnId {
        const indexed = try self.durable.?.scanRecoveryIds(alloc, after, limit);
        self.mutex.lock();
        self.recovery_index_cursor = indexed.next_after;
        self.mutex.unlock();
        return indexed.ids;
    }

    /// Claims indexed work under the session lease and returns a stable clone
    /// of the exact action to execute. A foreign owner is transferred only
    /// after its durable lease expires.
    pub fn claimPendingRecovery(
        self: *SessionRegistry,
        alloc: std.mem.Allocator,
        txn_id: db_mod.types.TxnId,
        owner_node_id: u64,
        now_ns: u64,
    ) !?PendingSessionRecovery {
        const session_lock = self.sessionLock(txn_id);
        session_lock.lock();
        defer session_lock.unlock();

        var candidate = if (self.durable) |durable|
            (try durable.loadWithRecoveryOwner(txn_id, owner_node_id)) orelse {
                try durable.refreshRecoveryIndex(txn_id);
                return null;
            }
        else blk: {
            self.mutex.lock();
            defer self.mutex.unlock();
            const session = self.sessions.getPtr(txn_id) orelse return null;
            break :blk try session.clone(alloc);
        };
        defer candidate.deinit(if (self.durable) |durable| durable.alloc else alloc);

        // The scan is advisory. A foreground attempt may have settled this ID
        // before we acquired the execution guard.
        if (!sessionNeedsRecovery(candidate)) {
            if (self.durable) |durable| try durable.refreshRecoveryIndex(txn_id);
            return null;
        }
        if (candidate.owner_node_id != owner_node_id) {
            const durable = self.durable orelse return null;
            if (self.durable_scope != .cluster_shared or self.lease_store == null or self.owner_lease_ttl_ns == null or owner_node_id == 0) return null;
            const expected = DurableSessionStore.Version.of(candidate);
            candidate.owner_node_id = owner_node_id;
            touchSession(&candidate);
            var adopted = try candidate.clone(alloc);
            var adopted_owned = true;
            defer if (adopted_owned) adopted.deinit(alloc);
            const ttl_ms = @max(@as(u64, 1), self.owner_lease_ttl_ns.? / std.time.ns_per_ms);
            if (!(try durable.saveWithLease(adopted, expected, now_ns / std.time.ns_per_ms, ttl_ms, true, self.max_record_bytes, null, null))) return null;
            try self.publishAdoptedCandidateAssumeStripe(alloc, txn_id, &adopted);
            adopted_owned = false;
        } else if (self.lease_store != null and self.owner_lease_ttl_ns != null) {
            try self.renewLeaseLockedAt(txn_id, owner_node_id, now_ns);
        }

        if (candidate.external_preparation) |seal| {
            if (candidate.external_receipt_acknowledged) return null;
            return .{ .external_preparation = .{ .txn_id = txn_id, .seal = try alloc.dupe(u8, seal), .prepared = candidate.external_preparation_prepared, .aborted = candidate.terminal_abort, .committed = candidate.terminal_commit != null, .volatile_payload_available = candidate.volatile_payload_available } };
        }

        if (!candidate.volatile_payload_available) return error.SqlRelationRecoveryIncomplete;

        if (candidate.terminal_commit) |terminal| {
            // A pending terminal response is not the API/storage handoff. Keep
            // replaying the exact sealed request under the original ID until
            // every phase-two delivery and requested visibility barrier has
            // completed. In particular, do this before consulting the legacy
            // acknowledgement bit so records written by an older binary can
            // self-heal instead of remaining pending forever.
            const repair_handoff_needs_coordinator = terminal.status == .committed and
                terminal.repair_required and terminal.coordinator_group_id == null;
            if (terminal.status != .committed or repair_handoff_needs_coordinator) {
                if (candidate.commit_body_digest == null or !candidate.commit_execution_started)
                    return error.InvalidTransactionSessionRecord;
                const cloned_plan = try takeRecoveryPlan(&candidate, alloc);
                const cloned_request = if (cloned_plan == null) try (candidate.staged orelse return error.InvalidTransactionSessionRecord).clone(alloc) else null;
                return .{
                    .commit = .{
                        .txn_id = txn_id,
                        .begin_timestamp = candidate.begin_timestamp,
                        // Repair debt for one participant does not weaken another
                        // participant's still-live visibility contract. Exact
                        // failure markers keep the repaired request from invoking
                        // its provider again while recovery retains the caller's
                        // original barrier for every remaining participant. A
                        // source that returned only a thrown terminal error may
                        // use write durability once to recover missing coordinator
                        // metadata; maintenance selects that fallback explicitly.
                        .sync_level = candidate.sync_level,
                        .repair_required = terminal.repair_required,
                        .repair_handoff_needs_coordinator = repair_handoff_needs_coordinator,
                        .request = cloned_request,
                        .execution_plan = cloned_plan,
                    },
                };
            }
            if (terminal.coordinator_acknowledged) return null;
            const group_id = terminal.coordinator_group_id orelse return null;
            const table_name = terminal.coordinator_table_name orelse return error.InvalidTransactionSessionRecord;
            return .{ .acknowledge = .{
                .txn_id = txn_id,
                .owner_node_id = owner_node_id,
                .coordinator_group_id = group_id,
                .coordinator_table_name = try alloc.dupe(u8, table_name),
            } };
        }
        if (candidate.commit_body_digest == null or !candidate.commit_execution_started) return null;
        const cloned_plan = try takeRecoveryPlan(&candidate, alloc);
        const cloned_request = if (cloned_plan == null) try (candidate.staged orelse return error.InvalidTransactionSessionRecord).clone(alloc) else null;
        return .{ .commit = .{
            .txn_id = txn_id,
            .begin_timestamp = candidate.begin_timestamp,
            .sync_level = candidate.sync_level,
            .repair_required = false,
            .repair_handoff_needs_coordinator = false,
            .request = cloned_request,
            .execution_plan = cloned_plan,
        } };
    }

    fn takeRecoveryPlan(candidate: *Session, alloc: std.mem.Allocator) !?[]u8 {
        const plan = candidate.execution_plan orelse return null;
        if (candidate.owner_alloc) |owner| if (std.meta.eql(owner, alloc)) {
            candidate.execution_plan = null;
            return plan;
        };
        return try alloc.dupe(u8, plan);
    }

    pub fn createSavepoint(self: *SessionRegistry, alloc: std.mem.Allocator, guard: Mutation) !?SavepointInfo {
        return self.createSavepointNamed(alloc, guard, null, null);
    }

    pub fn createNamedSavepoint(self: *SessionRegistry, alloc: std.mem.Allocator, guard: Mutation, name: []const u8) !?SavepointInfo {
        if (name.len == 0 or name.len > 63) return error.InvalidSavepointName;
        return self.createSavepointNamed(alloc, guard, name, null);
    }

    pub const SqlSavepointAction = enum { create, rollback, release };
    pub fn changeSqlSavepoint(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId, name: []const u8, action: SqlSavepointAction, plan: @import("sql/session/command_producer.zig").ControlPlan) !?SavepointInfo {
        if (name.len == 0 or name.len > 63) return error.InvalidSavepointName;
        const info = (try self.readInfo(txn_id)) orelse return null;
        const guard = try self.mutation(alloc, txn_id, info.version.owner);
        return switch (action) {
            .create => self.createSavepointNamed(alloc, guard, name, plan),
            .rollback => self.changeSavepoint(alloc, guard, .{ .name = name }, false, plan),
            .release => self.changeSavepoint(alloc, guard, .{ .name = name }, true, plan),
        };
    }
    fn sameNativeSnapshotOwner(resident: Session, durable: Session) bool {
        if (durable.terminal_commit != null or durable.terminal_abort or durable.external_preparation_prepared)
            return false;
        const old = resident.sql orelse return false;
        const current = durable.sql orelse return false;
        return std.mem.eql(u8, &resident.txn_id, &durable.txn_id) and
            resident.owner_node_id == durable.owner_node_id and resident.begin_timestamp == durable.begin_timestamp and
            std.meta.eql(resident.connection_id, durable.connection_id) and
            std.mem.eql(u8, resident.principal orelse "", durable.principal orelse "") and
            old.characteristics_locked and current.characteristics_locked and old.isolation == current.isolation and
            old.mode == current.mode and old.accepted_lake_reads == current.accepted_lake_reads and
            old.temporary_relation_access == current.temporary_relation_access and
            std.mem.eql(u8, old.database, current.database) and std.mem.eql(u8, old.namespace, current.namespace);
    }

    fn refreshDurableCandidate(self: *SessionRegistry, alloc: std.mem.Allocator, candidate: *Session, refresh: bool) !?[32]u8 {
        if (!refresh) return null;
        const durable = self.durable orelse return error.SqlConnectionUnavailable;
        var latest = (try durable.savepointSnapshot(alloc, candidate.txn_id)) orelse return error.SqlTransactionNotActive;
        errdefer latest.value.deinit(alloc);
        if (!latest.value.volatile_payload_available) return error.SqlRelationRecoveryIncomplete;
        // Durable metadata never carries a live retained-read capability. A
        // matching live facade must preserve its already-chosen cut, rather
        // than silently losing it or recapturing after a savepoint mutation.
        if (candidate.native_sql_cut) |cut| {
            if (!sameNativeSnapshotOwner(candidate.*, latest.value)) return error.SqlWriteConflict;
            latest.value.native_sql_cut = cut.retain();
        }
        candidate.deinit(alloc);
        candidate.* = latest.value;
        return latest.digest;
    }
    fn persistSavepoint(self: *SessionRegistry, session: Session, expected: ?[32]u8, plan: ?@import("sql/session/command_producer.zig").ControlPlan, control: ?@import("antfly_local_sources").storage_pessimistic_lock_authority.ledger.Control, guard: ?Mutation) !void {
        if (self.durable) |durable| try durable.saveScopedSession(session, expected, plan, control, null, self.max_record_bytes, guard) else if (plan != null) return error.SqlConnectionUnavailable;
    }
    fn createSavepointNamed(self: *SessionRegistry, alloc: std.mem.Allocator, guard: Mutation, name: ?[]const u8, plan: ?@import("sql/session/command_producer.zig").ControlPlan) !?SavepointInfo {
        const session_lock = self.sessionLock(guard.id);
        session_lock.lock();
        defer session_lock.unlock();
        var candidate = (try self.loadSessionCloneAssumeStripe(alloc, guard.id)) orelse return null;
        errdefer candidate.deinit(alloc);
        if (!candidate.volatile_payload_available) return error.SqlRelationRecoveryIncomplete;
        const digest = try self.refreshDurableCandidate(alloc, &candidate, plan != null);
        const expected = try guard.version(candidate);
        if (candidate.commit_body_digest != null or candidate.external_preparation != null or candidate.terminal_commit != null or candidate.terminal_abort) return error.TransactionCommitSealed;
        if (self.max_savepoints) |limit| {
            if (candidate.savepoints.count() >= limit) return error.SavepointLimitExceeded;
        }
        const savepoint_id = candidate.next_savepoint_id;
        candidate.next_savepoint_id = std.math.add(u64, savepoint_id, 1) catch return error.SavepointLimitExceeded;
        const snapshot: OwnedTransactionCommitRequest = if (candidate.staged) |staged|
            try staged.clone(alloc)
        else
            .{ .sync_level = candidate.sync_level };
        var new_savepoint: Savepoint = .{
            .id = savepoint_id,
            .notification_count = if (candidate.sql) |metadata| metadata.notifications.commands.items.len else 0,
            .generation_count = if (candidate.sql) |metadata| metadata.catalog_generations.entries.items.len else 0,
            .snapshot = snapshot,
            .transaction_mode = if (candidate.sql) |metadata| metadata.mode else null,
            .transaction_deferrable = if (candidate.sql) |metadata| metadata.deferrable else null,
            .search_path_active = if (candidate.sql) |metadata| metadata.search_path_active else null,
            .search_path_committed = if (candidate.sql) |metadata| metadata.search_path_committed else null,
        };
        var savepoint_inserted = false;
        errdefer if (!savepoint_inserted) new_savepoint.deinit(alloc);
        new_savepoint.name = if (name) |value| try alloc.dupe(u8, value) else null;
        new_savepoint.read_snapshots = try cloneReadSnapshotMap(alloc, candidate.read_snapshots);
        new_savepoint.setting_active = try cloneSettingEntries(alloc, candidate.setting_active.items);
        new_savepoint.setting_committed = try cloneSettingEntries(alloc, candidate.setting_committed.items);
        try candidate.savepoints.put(alloc, savepoint_id, new_savepoint);
        candidate.current_subtransaction = savepoint_id;
        savepoint_inserted = true;
        touchSession(&candidate);

        if (plan != null) try self.persistSavepoint(candidate, digest, plan, null, guard) else try self.persistVersionedSession(candidate, expected, guard, true);
        self.mutex.lock();
        defer self.mutex.unlock();
        const publish_target = self.sessions.getPtr(guard.id) orelse return error.SessionRemovedDuringMutation;
        self.publishCandidateLocked(alloc, publish_target, &candidate);
        return .{ .txn_id = guard.id, .savepoint_id = savepoint_id };
    }

    pub fn rollbackToSavepoint(self: *SessionRegistry, alloc: std.mem.Allocator, guard: Mutation, savepoint_id: u64) !?SavepointInfo {
        return self.changeSavepoint(alloc, guard, .{ .id = savepoint_id }, false, null);
    }

    pub fn releaseSavepoint(self: *SessionRegistry, alloc: std.mem.Allocator, guard: Mutation, savepoint_id: u64) !?SavepointInfo {
        return self.changeSavepoint(alloc, guard, .{ .id = savepoint_id }, true, null);
    }

    pub fn rollbackToNamedSavepoint(self: *SessionRegistry, alloc: std.mem.Allocator, guard: Mutation, name: []const u8) !?SavepointInfo {
        return self.changeSavepoint(alloc, guard, .{ .name = name }, false, null);
    }

    pub fn releaseNamedSavepoint(self: *SessionRegistry, alloc: std.mem.Allocator, guard: Mutation, name: []const u8) !?SavepointInfo {
        return self.changeSavepoint(alloc, guard, .{ .name = name }, true, null);
    }

    fn changeSavepoint(self: *SessionRegistry, alloc: std.mem.Allocator, guard: Mutation, target: union(enum) { id: u64, name: []const u8 }, release: bool, plan: ?@import("sql/session/command_producer.zig").ControlPlan) !?SavepointInfo {
        const session_lock = self.sessionLock(guard.id);
        session_lock.lock();
        defer session_lock.unlock();
        var candidate = (try self.loadSessionCloneAssumeStripe(alloc, guard.id)) orelse return null;
        errdefer candidate.deinit(alloc);
        if (!candidate.volatile_payload_available) return error.SqlRelationRecoveryIncomplete;
        const digest = try self.refreshDurableCandidate(alloc, &candidate, plan != null);
        const expected = try guard.version(candidate);
        if (candidate.commit_body_digest != null or candidate.external_preparation != null or candidate.terminal_commit != null or candidate.terminal_abort) return error.TransactionCommitSealed;
        const savepoint_id = switch (target) {
            .id => |id| id,
            .name => |name| blk: {
                var newest: u64 = 0;
                var points = candidate.savepoints.iterator();
                while (points.next()) |point| if (point.value_ptr.name) |stored| {
                    if (std.mem.eql(u8, name, stored)) newest = @max(newest, point.key_ptr.*);
                };
                break :blk newest;
            },
        };
        if (!candidate.savepoints.contains(savepoint_id)) {
            candidate.deinit(alloc);
            return null;
        }
        const savepoint = candidate.savepoints.getPtr(savepoint_id).?;
        if (!release) {
            var staged = try savepoint.snapshot.clone(alloc);
            errdefer staged.deinit(alloc);
            if (candidate.staged) |*old| try staged.retainRangeGuards(alloc, old);
            if (candidate.staged) |*old| old.deinit(alloc);
            candidate.staged = staged;
            // Ownership moved into candidate, whose error path releases it.
            staged = .{};
            try candidate.refreshListingSummary(alloc, self.max_record_bytes);
            const read_snapshots = try cloneReadSnapshotMap(alloc, savepoint.read_snapshots);
            deinitReadSnapshotMap(alloc, &candidate.read_snapshots);
            candidate.read_snapshots = read_snapshots;
            const active = try cloneSettingEntries(alloc, savepoint.setting_active.items);
            const committed = cloneSettingEntries(alloc, savepoint.setting_committed.items) catch |err| {
                var disposable = active;
                deinitSettingEntries(alloc, &disposable);
                return err;
            };
            deinitSettingEntries(alloc, &candidate.setting_active);
            deinitSettingEntries(alloc, &candidate.setting_committed);
            candidate.setting_active = active;
            candidate.setting_committed = committed;
            if (candidate.sql) |*metadata| {
                metadata.failed = false;
                if (savepoint.notification_count > metadata.notifications.commands.items.len) return error.InvalidTransactionSessionRecord;
                metadata.notifications.truncate(alloc, savepoint.notification_count);
                try metadata.catalog_generations.rollback(alloc, savepoint.generation_count);
                if (savepoint.transaction_mode) |mode| metadata.mode = mode;
                if (savepoint.transaction_deferrable) |deferrable| metadata.deferrable = deferrable;
                metadata.search_path_active = savepoint.search_path_active;
                metadata.search_path_committed = savepoint.search_path_committed;
            }
        }
        // Numeric ids define nesting even when SQL names shadow older names.
        // Gather ids before mutation so hash-map iteration cannot be invalidated.
        var removed_ids = std.ArrayList(u64).empty;
        defer removed_ids.deinit(alloc);
        candidate.current_subtransaction = 0;
        var points = candidate.savepoints.keyIterator();
        while (points.next()) |id| {
            if (id.* > savepoint_id or (release and id.* == savepoint_id)) {
                try removed_ids.append(alloc, id.*);
            } else candidate.current_subtransaction = @max(candidate.current_subtransaction, id.*);
        }
        for (removed_ids.items) |id| {
            var removed = candidate.savepoints.fetchRemove(id).?.value;
            removed.deinit(alloc);
        }
        touchSession(&candidate);

        if (release) if (candidate.sql) |*metadata| try metadata.catalog_generations.release(savepoint_id, candidate.current_subtransaction);
        if (plan != null) try self.persistSavepoint(candidate, digest, plan, if (release)
            .{ .release_savepoint = .{ .transaction = guard.id, .first_child = savepoint_id, .parent = candidate.current_subtransaction } }
        else
            .{ .rollback = .{ .transaction = guard.id, .first_aborted = savepoint_id } }, guard) else try self.persistVersionedSession(candidate, expected, guard, true);
        self.mutex.lock();
        defer self.mutex.unlock();
        const publish_target = self.sessions.getPtr(guard.id) orelse return error.SessionRemovedDuringMutation;
        self.publishCandidateLocked(alloc, publish_target, &candidate);
        return .{ .txn_id = guard.id, .savepoint_id = savepoint_id };
    }

    pub fn getStatus(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId) !?SessionStatus {
        return self.getStatusFiltered(alloc, txn_id, .all);
    }

    pub fn getStatusForPrincipal(self: *SessionRegistry, alloc: std.mem.Allocator, id: db_mod.types.TxnId, principal: ?[]const u8) !?SessionStatus {
        return self.getStatusFiltered(alloc, id, .{ .principal = principal });
    }

    fn getStatusFiltered(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId, filter: StatusPrincipalFilter) !?SessionStatus {
        const session_lock = self.sessionLock(txn_id);
        session_lock.lock();
        defer session_lock.unlock();
        if (self.durableMissIsAuthoritative() or self.hasColocatedLease()) {
            if (try self.durable.?.readStatus(txn_id, filter, self.hasColocatedLease())) |projection| switch (projection) {
                .absent => return null,
                .found => |status| {
                    var result = status;
                    if (!self.hasColocatedLease()) result.lease_expires_at = try self.loadLeaseExpiryLocked(alloc, txn_id);
                    result.savepoint_limit = self.max_savepoints;
                    result.remaining_savepoints = if (self.max_savepoints) |limit| limit - @min(limit, result.savepoint_count) else null;
                    return result;
                },
            };
        }
        var view = (try self.readSessionAssumeStripe(txn_id)) orelse {
            const durable = self.durable orelse return null;
            if (try durable.readStatus(txn_id, filter, false)) |projection| switch (projection) {
                .found => |status| {
                    var result = status;
                    applySavepointLimits(self, &result);
                    return result;
                },
                .absent => {},
            };
            return null;
        };
        defer view.deinit();
        const session = view.session;
        switch (filter) {
            .all => {},
            .principal => |principal| if (!principalsEqual(session.principal, principal)) return null,
        }
        return try sessionStatusFromSession(self, alloc, &session);
    }

    pub fn getDetails(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId) !?SessionDetails {
        return self.getDetailsFiltered(alloc, txn_id, .all, .details);
    }

    pub fn getDetailsForPrincipal(self: *SessionRegistry, alloc: std.mem.Allocator, id: db_mod.types.TxnId, principal: ?[]const u8) !?SessionDetails {
        return self.getDetailsFiltered(alloc, id, .{ .principal = principal }, .details);
    }

    pub fn getAuthorizationDetailsForPrincipal(self: *SessionRegistry, alloc: std.mem.Allocator, id: db_mod.types.TxnId, principal: ?[]const u8) !?SessionDetails {
        return self.getDetailsFiltered(alloc, id, .{ .principal = principal }, .authorization);
    }

    pub fn getListingDetailsForPrincipal(self: *SessionRegistry, alloc: std.mem.Allocator, id: db_mod.types.TxnId, principal: ?[]const u8) !?SessionDetails {
        return self.getDetailsFiltered(alloc, id, .{ .principal = principal }, .listing);
    }

    const DetailsProjection = enum { details, listing, authorization };

    fn getDetailsFiltered(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId, filter: StatusPrincipalFilter, projection: DetailsProjection) !?SessionDetails {
        const session_lock = self.sessionLock(txn_id);
        session_lock.lock();
        defer session_lock.unlock();
        const colocated_lease = self.hasColocatedLease();
        if ((projection != .authorization and colocated_lease) or
            (projection == .listing and self.durableMissIsAuthoritative()))
        {
            const key = try makeSessionKey(alloc, txn_id);
            defer alloc.free(key);
            var snapshot = (switch (self.durable.?.backend) {
                .docstore => |store| read: {
                    var txn = try store.beginReadTxn();
                    defer txn.abort();
                    break :read try self.detailsFromTxn(&txn, alloc, key, txn_id, filter, projection, colocated_lease);
                },
                .runtime => |store| read: {
                    var txn = try store.beginRead();
                    defer txn.abort();
                    break :read try self.detailsFromTxn(&txn, alloc, key, txn_id, filter, projection, colocated_lease);
                },
            }) orelse return null;
            defer snapshot.session.deinit(alloc);
            defer if (snapshot.summary) |summary| summary.deinit();
            if (!colocated_lease) snapshot.status.lease_expires_at = try self.loadLeaseExpiryLocked(alloc, txn_id);
            return try projectDetails(alloc, &snapshot.session, snapshot.status, projection, if (snapshot.summary) |summary| summary.value else null);
        }
        var view = (try self.readSessionAssumeStripe(txn_id)) orelse return null;
        defer view.deinit();
        const session = view.session;
        if (!detailsPrincipalMatches(&session, filter)) return null;
        const status = if (projection != .authorization) try sessionStatusFromSession(self, alloc, &session) else sessionStatusSnapshot(&session, self.durable != null);
        return try projectDetails(alloc, &session, status, projection, null);
    }

    const DetailsSnapshot = struct {
        session: Session,
        status: SessionStatus,
        summary: ?std.json.Parsed(SessionListingSummary) = null,
    };

    fn detailsFromTxn(self: *SessionRegistry, txn: anytype, alloc: std.mem.Allocator, key: []const u8, id: db_mod.types.TxnId, filter: StatusPrincipalFilter, projection: DetailsProjection, colocated_lease: bool) !?DetailsSnapshot {
        const raw = txn.get(key) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        var savepoint_count: usize = 0;
        var summary: ?std.json.Parsed(SessionListingSummary) = null;
        errdefer if (summary) |value| value.deinit();
        var session = if (projection == .listing)
            try decodeSessionForListing(alloc, id, raw, &savepoint_count, &summary)
        else
            try decodeSessionRecord(alloc, id, raw);
        errdefer session.deinit(alloc);
        if (!detailsPrincipalMatches(&session, filter)) {
            session.deinit(alloc);
            if (summary) |value| value.deinit();
            return null;
        }
        var status = sessionStatusSnapshot(&session, true);
        if (projection == .listing) status.savepoint_count = savepoint_count;
        if (colocated_lease) status.lease_expires_at = try matchingLeaseExpiry(txn, alloc, id, session.owner_node_id);
        if (summary) |value| {
            status.staged_table_count = value.value.counts.tables;
            status.staged_read_count = value.value.counts.reads;
            status.staged_write_count = value.value.counts.writes;
            status.staged_delete_count = value.value.counts.deletes;
        }
        applySavepointLimits(self, &status);
        return .{ .session = session, .status = status, .summary = summary };
    }

    fn detailsPrincipalMatches(session: *const Session, filter: StatusPrincipalFilter) bool {
        return switch (filter) {
            .all => true,
            .principal => |principal| principalsEqual(session.principal, principal),
        };
    }

    fn projectDetails(alloc: std.mem.Allocator, session: *const Session, status: SessionStatus, projection: DetailsProjection, summary: ?SessionListingSummary) !SessionDetails {
        var details: SessionDetails = .{
            .graph_receipt = session.graph_receipt,
            .prepared_completion_only = session.prepared_completion_only,
            .status = status,
            .connection_id = session.connection_id,
            .tables = &.{},
            .read_snapshots = &.{},
            .savepoint_ids = &.{},
        };
        errdefer details.deinit(alloc);
        details.tables = if (summary) |value| try cloneTableDetails(alloc, value.tables) else if (session.graph_receipt and session.terminal_abort and session.staged == null) try cloneTableDetails(alloc, session.graph_abort_tables) else try sessionTableDetails(alloc, session.staged, projection == .details);
        details.read_snapshots = try sessionReadSnapshots(alloc, session, projection == .details);
        if (projection == .details) details.savepoint_ids = try sessionSavepointIds(alloc, session);
        if (summary) |value| {
            try cloneDetailBindings(alloc, &details, value.bindings);
        } else if (session.staged) |*staged| {
            try cloneDetailBindings(alloc, &details, staged.catalog_bindings.items);
        }
        return details;
    }

    fn cloneDetailBindings(alloc: std.mem.Allocator, details: *SessionDetails, source: []const CatalogBinding) !void {
        var bindings = std.ArrayList(CatalogBinding).empty;
        errdefer {
            for (bindings.items) |binding| {
                alloc.free(binding.logical);
                alloc.free(binding.physical);
            }
            bindings.deinit(alloc);
        }
        for (source) |binding| {
            const logical = try alloc.dupe(u8, binding.logical);
            errdefer alloc.free(logical);
            const physical = try alloc.dupe(u8, binding.physical);
            errdefer alloc.free(physical);
            try bindings.append(alloc, .{ .logical = logical, .physical = physical });
        }
        details.catalog_bindings = try bindings.toOwnedSlice(alloc);
    }

    pub fn listSessionIds(self: *SessionRegistry, alloc: std.mem.Allocator, principal: ?[]const u8) ![]db_mod.types.TxnId {
        if (self.durable) |durable| return durable.listSessionIds(alloc, principal);
        self.mutex.lock();
        defer self.mutex.unlock();
        var ids: std.ArrayList(db_mod.types.TxnId) = .empty;
        errdefer ids.deinit(alloc);
        var entries = self.sessions.iterator();
        while (entries.next()) |entry| {
            if (principalsEqual(entry.value_ptr.principal, principal)) try ids.append(alloc, entry.key_ptr.*);
        }
        return ids.toOwnedSlice(alloc);
    }

    pub fn listStatuses(self: *SessionRegistry, alloc: std.mem.Allocator) ![]SessionStatus {
        return try self.listStatusesFiltered(alloc, .all);
    }

    pub fn listStatusesForPrincipal(
        self: *SessionRegistry,
        alloc: std.mem.Allocator,
        principal: ?[]const u8,
    ) ![]SessionStatus {
        return try self.listStatusesFiltered(alloc, .{ .principal = principal });
    }

    const StatusPrincipalFilter = union(enum) {
        all,
        principal: ?[]const u8,
    };

    fn listStatusesFiltered(
        self: *SessionRegistry,
        alloc: std.mem.Allocator,
        principal_filter: StatusPrincipalFilter,
    ) ![]SessionStatus {
        var statuses = std.ArrayListUnmanaged(SessionStatus).empty;
        errdefer statuses.deinit(alloc);

        if (self.durable) |durable| {
            // Decode one row at a time so listing does not duplicate every
            // potentially large staged transaction record in memory.
            const Scan = struct {
                registry: *SessionRegistry,
                allocator: std.mem.Allocator,
                statuses: *std.ArrayListUnmanaged(SessionStatus),
                principal_filter: StatusPrincipalFilter,

                fn visit(raw: *anyopaque, key: []const u8, value: []const u8) anyerror!bool {
                    const scan: *@This() = @ptrCast(@alignCast(raw));
                    if (key.len <= session_prefix.len) return true;
                    const txn_id = distributed_txn.parseTxnIdHex(key[session_prefix.len..]) catch return true;
                    var session = decodeSessionRecord(scan.allocator, txn_id, value) catch return true;
                    defer session.deinit(scan.allocator);
                    switch (scan.principal_filter) {
                        .all => {},
                        .principal => |principal| if (!principalsEqual(session.principal, principal)) return true,
                    }
                    const counts = stagedCounts(&session);
                    const savepoint_count = session.savepoints.count();
                    try scan.statuses.append(scan.allocator, .{
                        .txn_id = session.txn_id,
                        .owner_node_id = session.owner_node_id,
                        .begin_timestamp = session.begin_timestamp,
                        .last_touched_timestamp = session.last_touched_timestamp,
                        .lease_expires_at = 0,
                        .sync_level = session.sync_level,
                        .staged_table_count = counts.tables,
                        .staged_read_count = counts.reads,
                        .staged_write_count = counts.writes,
                        .staged_delete_count = counts.deletes,
                        .read_snapshot_count = session.read_snapshots.count(),
                        .savepoint_count = savepoint_count,
                        .savepoint_limit = scan.registry.max_savepoints,
                        .remaining_savepoints = if (scan.registry.max_savepoints) |limit| limit - @min(limit, savepoint_count) else null,
                        .durable = true,
                    });
                    return true;
                }
            };
            var scan = Scan{
                .registry = self,
                .allocator = alloc,
                .statuses = &statuses,
                .principal_filter = principal_filter,
            };
            try durable.scanPrefixWithContext(session_prefix, &scan, Scan.visit);
            // Avoid nested backend reads by loading lease metadata only after
            // the scan transaction has closed.
            for (statuses.items) |*status| status.lease_expires_at = try self.loadLeaseExpiryLocked(alloc, status.txn_id);
            return try statuses.toOwnedSlice(alloc);
        }

        self.mutex.lock();
        defer self.mutex.unlock();
        var it = self.sessions.iterator();
        while (it.next()) |entry| {
            const session = entry.value_ptr.*;
            switch (principal_filter) {
                .all => {},
                .principal => |principal| if (!principalsEqual(session.principal, principal)) continue,
            }
            try statuses.append(alloc, try sessionStatusFromSession(self, alloc, &session));
        }
        return try statuses.toOwnedSlice(alloc);
    }

    /// Returns Unsupported when a legacy record needs an admitted full read.
    pub fn probeOwnerNodeId(self: *SessionRegistry, txn_id: db_mod.types.TxnId, capacity: *@import("antfly_local_sources").common_request_admission.RequestAdmission, context: RoutingContext) !?u64 {
        const stripe = self.sessionLock(txn_id);
        stripe.lock();
        defer stripe.unlock();
        try context.ensureActive();
        if (!self.durableMissIsAuthoritative()) {
            self.mutex.lock();
            const cached = if (self.sessions.getPtr(txn_id)) |session| session.owner_node_id else null;
            self.mutex.unlock();
            if (cached) |owner| return owner;
        }
        const durable = self.durable orelse return null;
        return switch (durable.backend) {
            .docstore => |store| result: {
                var read = try store.beginReadTxn();
                defer read.abort();
                break :result try probeOwnerWithFallback(durable, &read, txn_id, capacity, context);
            },
            .runtime => |store| result: {
                var read = try durable.beginRoutingRead(store, context);
                defer read.abort();
                break :result try probeOwnerWithFallback(durable, &read, txn_id, capacity, context);
            },
        };
    }

    fn probeOwnerWithFallback(durable: *DurableSessionStore, txn: anytype, id: db_mod.types.TxnId, capacity: *@import("antfly_local_sources").common_request_admission.RequestAdmission, context: RoutingContext) !?u64 {
        return probeDurableOwner(txn, durable.alloc, id) catch |err| switch (err) {
            error.Unsupported => fallback: {
                try context.ensureActive();
                var lease = capacity.tryAcquireLease() orelse return error.SqlWriteCapacityUnavailable;
                defer lease.release();
                var session = (try durable.loadSessionTxn(txn, id)) orelse break :fallback null;
                defer session.deinit(durable.alloc);
                break :fallback session.owner_node_id;
            },
            else => return err,
        };
    }

    fn probeDurableOwner(read: anytype, alloc: std.mem.Allocator, id: db_mod.types.TxnId) !?u64 {
        const key = try makeSessionKey(alloc, id);
        defer alloc.free(key);
        var scope = try read.openReadScope(alloc);
        defer scope.close();
        const bytes = scope.probePrefix(key, reader_prefix_len) catch |err| switch (err) {
            error.NotFound => return null,
            error.EndOfStream => return error.Unsupported,
            else => return err,
        };
        if (try parseConnectionPrefix(bytes)) |payload| return std.mem.readInt(u64, payload[1..9], .little);
        if (try parseSqlPrefix(bytes, sql_field)) |payload| return std.mem.readInt(u64, payload[1..9], .little);
        const payload = (try parseReaderPrefix(bytes)) orelse return error.Unsupported;
        return std.mem.readInt(u64, payload[1..9], .little);
    }

    pub fn getOwnerNodeId(self: *SessionRegistry, _: std.mem.Allocator, txn_id: db_mod.types.TxnId) !?u64 {
        const session_lock = self.sessionLock(txn_id);
        session_lock.lock();
        defer session_lock.unlock();
        if (self.durableMissIsAuthoritative()) {
            const durable = self.durable.?;
            var current = (try durable.load(txn_id)) orelse return null;
            defer current.deinit(durable.alloc);
            return current.owner_node_id;
        }
        self.mutex.lock();
        if (self.sessions.getPtr(txn_id)) |session| {
            const owner_node_id = session.owner_node_id;
            self.mutex.unlock();
            return owner_node_id;
        }
        self.mutex.unlock();
        if (self.durable) |durable| {
            var session = (try durable.load(txn_id)) orelse return null;
            defer session.deinit(durable.alloc);
            return session.owner_node_id;
        }
        return null;
    }

    pub fn adopt(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId, owner_node_id: u64) !bool {
        const session_lock = self.sessionLock(txn_id);
        session_lock.lock();
        defer session_lock.unlock();
        const durable = self.durable orelse return false;
        var persisted = (try durable.load(txn_id)) orelse return false;
        defer persisted.deinit(durable.alloc);
        var candidate = try persisted.clone(alloc);
        errdefer candidate.deinit(alloc);
        if (candidate.owner_node_id == owner_node_id) {
            try self.publishAdoptedCandidateAssumeStripe(alloc, txn_id, &candidate);
            return true;
        }
        const expected = DurableSessionStore.Version.of(candidate);
        candidate.owner_node_id = owner_node_id;
        touchSession(&candidate);
        if (self.lease_store != null and self.owner_lease_ttl_ns != null) {
            const now_ns = nextTxnTimestamp();
            const ttl_ms = @max(@as(u64, 1), self.owner_lease_ttl_ns.? / std.time.ns_per_ms);
            if (!(try durable.saveWithLease(candidate, expected, now_ns / std.time.ns_per_ms, ttl_ms, false, self.max_record_bytes, null, null))) {
                candidate.deinit(alloc);
                return false;
            }
        } else try durable.saveRecord(candidate, self.max_record_bytes, .{ .expected = expected });
        try self.publishAdoptedCandidateAssumeStripe(alloc, txn_id, &candidate);
        return true;
    }

    pub fn adoptIfLeaseExpired(
        self: *SessionRegistry,
        alloc: std.mem.Allocator,
        txn_id: db_mod.types.TxnId,
        owner_node_id: u64,
        now_ns: ?u64,
    ) !bool {
        return self.adoptIfLeaseExpiredControlled(alloc, txn_id, owner_node_id, now_ns, .{});
    }

    pub fn adoptIfLeaseExpiredControlled(
        self: *SessionRegistry,
        alloc: std.mem.Allocator,
        txn_id: db_mod.types.TxnId,
        owner_node_id: u64,
        now_ns: ?u64,
        context: RoutingContext,
    ) !bool {
        const session_lock = self.sessionLock(txn_id);
        while (!session_lock.inner.tryLock()) {
            try context.ensureActive();
            platform_time.yieldBriefly();
        }
        defer session_lock.unlock();
        try context.ensureActive();
        const durable = self.durable orelse return false;
        if (self.lease_store == null or self.owner_lease_ttl_ns == null) {
            return false;
        }
        // A routing/principal lookup may have cached an older non-owner copy.
        // Adoption must always fence and transfer the latest durable record or
        // it can overwrite staged work that the previous owner published.
        var persisted = (try durable.loadForRouting(txn_id, context)) orelse return false;
        defer persisted.deinit(durable.alloc);
        var candidate = try persisted.clone(alloc);
        errdefer candidate.deinit(alloc);
        if (candidate.owner_node_id == owner_node_id) {
            try self.publishAdoptedCandidateAssumeStripe(alloc, txn_id, &candidate);
            return true;
        }
        const expected = DurableSessionStore.Version.of(candidate);
        const effective_now = now_ns orelse nextTxnTimestamp();
        candidate.owner_node_id = owner_node_id;
        touchSession(&candidate);
        const ttl_ms = @max(@as(u64, 1), self.owner_lease_ttl_ns.? / std.time.ns_per_ms);
        if (!(try durable.saveWithLeaseControlled(candidate, expected, effective_now / std.time.ns_per_ms, ttl_ms, true, self.max_record_bytes, null, null, context))) {
            candidate.deinit(alloc);
            return false;
        }
        try self.publishAdoptedCandidateAssumeStripe(alloc, txn_id, &candidate);
        return true;
    }

    pub fn migrateLegacySqlAbortReceipts(self: *SessionRegistry, alloc: std.mem.Allocator) !void {
        const durable = self.durable orelse return;
        // Shared stores audit the terminal index without moving legacy aborts.
        const index_only = self.durable_scope == .cluster_shared;
        const now_ns = platform_time.monotonicNs();
        if (!self.sql_abort_migration_lock.inner.tryLock()) return;
        defer self.sql_abort_migration_lock.unlock();
        if (now_ns < self.sql_abort_migration_rescan_ns) return;
        const persisted_position = if (index_only) try durable.taggedSqlBackfillPosition() else null;
        defer if (persisted_position) |position| durable.alloc.free(position);
        if (persisted_position) |position| if (position.len == 0) {
            self.sql_abort_migration_rescan_ns = std.math.maxInt(u64);
            return;
        };
        const start = if (index_only) persisted_position else if (self.sql_abort_migration_after.items.len > 0) self.sql_abort_migration_after.items else null;
        var page: LegacySqlAbortPage = .{ .index_only = index_only };
        defer page.last.deinit(durable.alloc);
        switch (durable.backend) {
            .docstore => |store| {
                var txn = try store.beginReadTxn();
                defer txn.abort();
                var cursor = try txn.openPhysicalKeyCursorAdapter();
                defer cursor.close();
                try page.scan(&txn, &cursor, durable.alloc, start);
            },
            .runtime => |store| {
                var txn = try store.beginRead();
                defer txn.abort();
                var arena = std.heap.ArenaAllocator.init(alloc);
                defer arena.deinit();
                if (txn.readPrefixPage(arena.allocator(), session_prefix, start, connection_prefix_len)) |rows| {
                    for (rows) |row| {
                        if (!std.mem.startsWith(u8, row.key, session_prefix)) return error.InvalidTransactionSessionRecord;
                        try page.recordPosition(durable.alloc, row.key);
                        if (row.key.len != session_prefix.len + 32) continue;
                        const prefix = if (row.value.len == 0) null else page.isTerminalPrefix(row.value) catch null;
                        if (prefix orelse try page.terminalConnectionPrefix(&txn, alloc, row.key)) page.addCandidate(row.key);
                    }
                    page.more = rows.len == 64;
                } else |err| {
                    if (err != error.Unsupported and err != error.InvalidTransactionSessionRecord) return err;
                    var cursor = try txn.openCursor();
                    defer cursor.close();
                    try page.scan(&txn, &cursor, durable.alloc, start);
                }
            },
        }
        if (index_only) {
            var removed: std.AutoHashMapUnmanaged(db_mod.types.TxnId, void) = .empty;
            defer removed.deinit(durable.alloc);
            const consumed = try durable.maintainTaggedSqlPage(page.ids[0..page.candidates], null, &removed);
            if (consumed < page.candidates) {
                const key = try makeSessionKey(durable.alloc, page.ids[consumed - 1]);
                defer durable.alloc.free(key);
                try durable.advanceTaggedSqlBackfill(persisted_position, key);
            } else try durable.advanceTaggedSqlBackfill(persisted_position, if (page.more) page.last.items else "");
            return;
        } else for (page.ids[0..page.candidates]) |id| {
            const stripe = self.sessionLock(id);
            stripe.lock();
            defer stripe.unlock();
            const changed = durable.migrateLegacySqlAbortReceipt(id) catch |err| switch (err) {
                error.SessionLeaseLost => continue,
                error.InvalidTransactionSessionRecord, error.InvalidSqlConnection, error.SyntaxError, error.UnexpectedEndOfInput, error.UnexpectedToken => {
                    std.log.warn("legacy SQL abort row skipped txn_id={x} err={s}", .{ id, @errorName(err) });
                    continue;
                },
                else => return err,
            };
            if (changed) {
                self.mutex.lock();
                const removed = self.sessions.fetchRemove(id);
                self.mutex.unlock();
                if (removed) |entry| {
                    var value = entry.value;
                    value.deinit(alloc);
                }
            }
        }
        self.sql_abort_migration_after.clearRetainingCapacity();
        if (page.more) {
            std.mem.swap(std.ArrayList(u8), &self.sql_abort_migration_after, &page.last);
        } else {
            self.sql_abort_migration_rescan_ns = now_ns +| std.time.ns_per_hour;
        }
    }

    const LegacySqlAbortPage = struct {
        index_only: bool = false,
        ids: [128]db_mod.types.TxnId = undefined,
        candidates: usize = 0,
        scanned: usize = 0,
        last: std.ArrayList(u8) = .empty,
        more: bool = false,

        fn recordPosition(page: *LegacySqlAbortPage, alloc: std.mem.Allocator, key: []const u8) !void {
            page.last.clearRetainingCapacity();
            try page.last.appendSlice(alloc, key);
            page.scanned += 1;
        }

        fn addCandidate(page: *LegacySqlAbortPage, key: []const u8) void {
            page.ids[page.candidates] = distributed_txn.parseTxnIdHex(key[session_prefix.len..]) catch return;
            page.candidates += 1;
        }

        fn isTerminalPrefix(page: *const LegacySqlAbortPage, bytes: []const u8) !bool {
            if (page.index_only and !std.mem.startsWith(u8, bytes, prepared_session_header)) return false;
            const header = (try parseConnectionPrefix(bytes)) orelse
                if (page.index_only) (try parseSqlPrefix(bytes, sql_field)) orelse return false else return false;
            return header[0] & 4 != 0;
        }

        fn scan(page: *LegacySqlAbortPage, read: anytype, cursor: *backend_erased.Cursor, alloc: std.mem.Allocator, start: ?[]const u8) !void {
            cursor.setKeysOnly();
            var entry = try cursor.seekAtOrAfter(start orelse session_prefix);
            while (entry) |row| : (entry = try cursor.next()) {
                if (!std.mem.startsWith(u8, row.key, session_prefix)) break;
                if (start) |key| if (std.mem.eql(u8, row.key, key)) continue;
                try page.recordPosition(alloc, row.key);
                if (row.key.len == session_prefix.len + 32 and try page.terminalConnectionPrefix(read, alloc, row.key)) page.addCandidate(row.key);
                if (page.scanned == page.ids.len) {
                    page.more = true;
                    break;
                }
            }
        }

        fn terminalConnectionPrefix(page: *const LegacySqlAbortPage, read: anytype, alloc: std.mem.Allocator, key: []const u8) !bool {
            const narrow = bounded: {
                var scope = read.openReadScope(alloc) catch |err| switch (err) {
                    error.Unsupported => break :bounded null,
                    else => return err,
                };
                defer scope.close();
                const bytes = scope.probePrefix(key, connection_prefix_len) catch |err| switch (err) {
                    error.NotFound => return false,
                    error.Unsupported, error.EndOfStream, error.InvalidTransactionSessionRecord => break :bounded null,
                    else => return err,
                };
                break :bounded page.isTerminalPrefix(bytes) catch break :bounded null;
            };
            if (narrow) |terminal| return terminal;
            const bytes = read.get(key) catch |err| if (err == error.NotFound) return false else return err;
            return page.isTerminalPrefix(bytes) catch false;
        }
    };

    pub fn cleanupExpiredSqlAbortReceipts(self: *SessionRegistry, alloc: std.mem.Allocator, cutoff_ns: u64) !usize {
        const durable = self.durable orelse return 0;
        self.mutex.lock();
        const after = self.sql_abort_expiry_cursor;
        self.mutex.unlock();
        const page = try durable.scanExpiredIds(alloc, sql_abort_expiry_prefix, after, cutoff_ns, 128, null);
        defer alloc.free(page.ids);
        const removed = try durable.expireSqlAbortReceiptPage(page.ids, cutoff_ns);
        self.mutex.lock();
        defer self.mutex.unlock();
        if (std.meta.eql(after, self.sql_abort_expiry_cursor)) self.sql_abort_expiry_cursor = page.next_after;
        return removed;
    }

    pub fn hasSqlReceiptCleanupBacklog(self: *SessionRegistry) bool {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.sql_abort_expiry_cursor != null;
    }

    pub fn cleanupExpired(self: *SessionRegistry, alloc: std.mem.Allocator, cutoff_ns: u64) !usize {
        return self.cleanupExpiredMode(alloc, cutoff_ns);
    }

    pub fn cleanupExpiredTaggedSqlDecisions(self: *SessionRegistry, alloc: std.mem.Allocator, cutoff_ns: u64) !usize {
        const durable = self.durable orelse return 0;
        self.mutex.lock();
        const after = self.tagged_sql_expiry_cursor;
        self.mutex.unlock();
        var positions: std.ArrayListUnmanaged(ExpiryPosition) = .empty;
        defer positions.deinit(alloc);
        const page = try durable.scanExpiredIds(alloc, tagged_sql_expiry_prefix, after, cutoff_ns, 128, &positions);
        defer alloc.free(page.ids);
        var removed: std.AutoHashMapUnmanaged(db_mod.types.TxnId, void) = .empty;
        defer removed.deinit(durable.alloc);
        var batch = page.ids;
        const consumed = retry: while (true) {
            break :retry durable.maintainTaggedSqlPage(batch, cutoff_ns, &removed) catch |err| {
                if (!isCorruptExpiredSession(err)) return err;
                // The failed transaction aborted. Find a committable prefix
                // or isolate one corrupt row without committing partial edits.
                removed.clearRetainingCapacity();
                if (batch.len == 1) {
                    _ = skipCorruptExpiredSession(batch[0], err);
                    break :retry 1;
                }
                batch = batch[0 .. batch.len / 2];
                continue :retry;
            };
        };
        const next_after = if (consumed < page.ids.len) positions.items[consumed - 1] else page.next_after;
        var it = removed.keyIterator();
        while (it.next()) |id| self.evictDetachedSqlSession(id.*);
        self.mutex.lock();
        defer self.mutex.unlock();
        if (std.meta.eql(after, self.tagged_sql_expiry_cursor)) self.tagged_sql_expiry_cursor = next_after;
        return removed.count();
    }

    pub fn hasSessionCleanupBacklog(self: *SessionRegistry, tagged_only: bool) bool {
        self.mutex.lock();
        defer self.mutex.unlock();
        return if (tagged_only) self.tagged_sql_expiry_cursor != null else self.expiry_cursor != null;
    }

    fn cleanupExpiredMode(self: *SessionRegistry, alloc: std.mem.Allocator, cutoff_ns: u64) !usize {
        defer self.snapshot_retirement.drain();
        var expired_ids = std.ArrayListUnmanaged(db_mod.types.TxnId).empty;
        defer expired_ids.deinit(alloc);
        var positions: std.ArrayListUnmanaged(ExpiryPosition) = .empty;
        defer positions.deinit(alloc);
        var after: ?ExpiryPosition = null;
        var next: ?ExpiryPosition = null;
        defer if (self.durable != null) {
            self.mutex.lock();
            defer self.mutex.unlock();
            if (std.meta.eql(after, self.expiry_cursor)) self.expiry_cursor = next;
        };
        if (self.durable) |durable| {
            self.mutex.lock();
            after = self.expiry_cursor;
            next = after;
            self.mutex.unlock();
            const page = try durable.scanExpiredIds(alloc, session_expiry_prefix, after, cutoff_ns, 1024, &positions);
            expired_ids = .fromOwnedSlice(page.ids);
            next = page.next_after;
        } else {
            self.mutex.lock();
            var loaded_it = self.sessions.iterator();
            while (loaded_it.next()) |entry| {
                if (entry.value_ptr.last_touched_timestamp < cutoff_ns and sessionCanExpire(entry.value_ptr.*)) expired_ids.append(alloc, entry.key_ptr.*) catch |err| {
                    self.mutex.unlock();
                    return err;
                };
                if (expired_ids.items.len >= 1024) break;
            }
            self.mutex.unlock();
        }

        var removed_count: usize = 0;
        const page_end = next;
        for (expired_ids.items, 0..) |txn_id, index| {
            if (self.durable != null) {
                // Keep the failing row and its unprocessed suffix eligible.
                next = positions.items[index];
                next.?.exclusive = false;
            }
            const execution = self.tryAcquireCommitExecution(txn_id) orelse continue;
            defer execution.release();
            const session_lock = self.sessionLock(txn_id);
            session_lock.lock();
            defer session_lock.unlock();
            // Scanning pinned receipts must not populate the foreground cache.
            var view = (self.readSessionAssumeStripe(txn_id) catch |err| {
                if (skipCorruptExpiredSession(txn_id, err)) continue;
                return err;
            }) orelse continue;
            defer view.deinit();
            const current = view.session;
            if (current.last_touched_timestamp >= cutoff_ns) continue;
            if (current.external_preparation != null and !current.external_receipt_acknowledged) continue;
            // The HTTP connection is the durable owner of this transaction.
            // Deleting its terminal record before an exact detach succeeds
            // would turn an uncertain connection into unrecoverable quota
            // debt. Keep both active and terminal records while referenced.
            if (!sessionCanExpire(current)) continue;
            self.deletePersistent(txn_id, DurableSessionStore.Version.of(current), cutoff_ns, null) catch |err| {
                if (err == error.SessionLeaseLost or skipCorruptExpiredSession(txn_id, err)) continue;
                return err;
            };
            self.releaseLease(txn_id, current.owner_node_id) catch {};
            self.mutex.lock();
            var removed = self.sessions.fetchRemove(txn_id);
            self.mutex.unlock();
            if (removed) |*entry| entry.value.deinit(alloc);
            removed_count += 1;
        }
        next = page_end;
        return removed_count;
    }

    fn isCorruptExpiredSession(err: anyerror) bool {
        return switch (err) {
            error.InvalidTransactionSessionRecord, error.InvalidSqlConnection, error.SyntaxError, error.UnexpectedToken, error.UnexpectedEndOfInput => true,
            else => false,
        };
    }

    fn skipCorruptExpiredSession(id: db_mod.types.TxnId, err: anyerror) bool {
        if (!isCorruptExpiredSession(err)) return false;
        std.log.warn("expired session record skipped txn_id={x} err={s}", .{ id, @errorName(err) });
        return true;
    }

    pub fn remove(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId) bool {
        return self.removeMode(alloc, txn_id, false, false, .active, null, null) catch false;
    }

    /// A no-participant COMMIT still publishes non-LOCAL setting mutations.
    /// For an attached connection, that publication and session retirement
    /// share one durable transaction; failed preflight/rollback use the abort
    /// variant above and leave the idle overlay unchanged.
    pub fn removeBeforeExecution(self: *SessionRegistry, alloc: std.mem.Allocator, id: db_mod.types.TxnId) bool {
        defer self.snapshot_retirement.drain();
        return self.removeMode(alloc, id, true, false, .active, null, null) catch false;
    }

    pub fn commitBeforeExecution(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId) bool {
        defer self.snapshot_retirement.drain();
        return self.removeMode(alloc, txn_id, true, true, .commit, null, null) catch false;
    }

    pub fn retireBeforeExecution(self: *SessionRegistry, alloc: std.mem.Allocator, guard: Mutation, expected: DurableSessionStore.Version, commit_settings: bool) !bool {
        return self.removeMode(alloc, guard.id, true, commit_settings, .active, expected, guard);
    }

    pub fn finishSqlCommitBeforeExecution(self: *SessionRegistry, alloc: std.mem.Allocator, guard: Mutation, expected: DurableSessionStore.Version, commit_settings: bool) !bool {
        return self.removeMode(alloc, guard.id, true, commit_settings, .commit, expected, guard);
    }

    pub fn evictDetachedSqlSession(self: *SessionRegistry, id: db_mod.types.TxnId) void {
        const stripe = self.sessionLock(id);
        stripe.lock();
        defer stripe.unlock();
        self.mutex.lock();
        defer self.mutex.unlock();
        if (self.sessions.fetchRemove(id)) |old| {
            var session = old.value;
            session.deinitOwned();
        }
    }

    pub fn rollbackConnectionBeforeExecution(self: *SessionRegistry, txn_id: db_mod.types.TxnId, connection_id: [32]u8, expected: DurableSessionStore.Version, condition: DurableSessionStore.DetachCondition) !void {
        defer self.snapshot_retirement.drain();
        const session_lock = self.sessionLock(txn_id);
        session_lock.lock();
        defer session_lock.unlock();
        const durable = self.durable orelse return error.SqlConnectionUnavailable;
        try durable.deleteAndDetachConnection(txn_id, connection_id, false, expected, condition, null, self.durable_scope != .cluster_shared);
        if (!self.hasColocatedLease()) self.releaseLease(txn_id, expected.owner) catch {};
        self.mutex.lock();
        defer self.mutex.unlock();
        if (self.sessions.fetchRemove(txn_id)) |removed| {
            var session = removed.value;
            session.deinitOwned();
        }
    }

    fn removeMode(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId, before_execution_only: bool, commit_settings: bool, condition: DurableSessionStore.DetachCondition, observed: ?DurableSessionStore.Version, guard: ?Mutation) !bool {
        const session_lock = self.sessionLock(txn_id);
        session_lock.lock();
        defer session_lock.unlock();
        var current = (try self.loadSessionCloneAssumeStripe(alloc, txn_id)) orelse return false;
        defer current.deinit(alloc);
        if (commit_settings) if (current.sql) |metadata| {
            if (metadata.catalog_generations.entries.items.len != 0) return false;
        };
        const expected = DurableSessionStore.Version.of(current);
        if (guard) |admission| _ = try admission.version(current);
        if (observed) |version| if (!std.meta.eql(expected, version)) return false;
        if (current.graph_receipt) return false; // Public reconciliation owns this ID until expiry.
        if (current.connection_id) |connection_id| {
            // No generic caller may erase a connection-owned decision. For an
            // unexecuted transaction the exact detach and session deletion
            // must share one native commit, regardless of which caller asked.
            if (current.commit_execution_started or current.terminal_commit != null or current.terminal_abort) return false;
            const durable = self.durable orelse return false;
            try durable.deleteAndDetachConnection(txn_id, connection_id, commit_settings, expected, condition, guard, false);
        } else {
            if (before_execution_only and (current.commit_execution_started or current.terminal_commit != null or current.terminal_abort)) return false;
            if (condition == .commit and current.prepared_completion_only and self.durable != null) {
                try self.durable.?.delete(txn_id, expected, null, guard, commit_settings);
            } else try self.deletePersistent(txn_id, expected, null, guard);
        }
        self.releaseLease(txn_id, current.owner_node_id) catch {};
        self.mutex.lock();
        defer self.mutex.unlock();
        const removed = self.sessions.fetchRemove(txn_id) orelse return false;
        var session = removed.value;
        session.deinit(alloc);
        return true;
    }

    fn sessionLock(self: *SessionRegistry, txn_id: db_mod.types.TxnId) *AtomicMutex {
        const hash = std.hash.Wyhash.hash(0, &txn_id);
        return &self.session_locks[hash % session_lock_count];
    }

    fn lockAllSessions(self: *SessionRegistry) void {
        for (&self.session_locks) |*lock| lock.lock();
    }

    fn unlockAllSessions(self: *SessionRegistry) void {
        var index = self.session_locks.len;
        while (index > 0) {
            index -= 1;
            self.session_locks[index].unlock();
        }
    }

    fn publishCandidateLocked(self: *SessionRegistry, alloc: std.mem.Allocator, current: *Session, candidate: *Session) void {
        _ = self;
        var previous = current.*;
        current.* = candidate.*;
        previous.deinit(alloc);
    }

    /// Publishes an authoritative durable ownership transition into this
    /// registry. The transaction stripe is held by the caller.
    fn publishAdoptedCandidateAssumeStripe(
        self: *SessionRegistry,
        alloc: std.mem.Allocator,
        txn_id: db_mod.types.TxnId,
        candidate: *Session,
    ) !void {
        self.mutex.lock();
        if (self.sessions.getPtr(txn_id)) |current| {
            self.publishCandidateLocked(alloc, current, candidate);
            self.mutex.unlock();
            return;
        }
        self.reserveSessionSlotLocked(alloc) catch |err| {
            self.mutex.unlock();
            return err;
        };
        self.sessions.putAssumeCapacity(txn_id, candidate.*);
        candidate.* = undefined;
        self.mutex.unlock();
    }

    fn hasColocatedLease(self: *SessionRegistry) bool {
        const durable = self.durable orelse return false;
        const lease = self.lease_store orelse return false;
        return std.meta.eql(lease.backend, durable.backend);
    }

    fn persistMutation(self: *SessionRegistry, session: Session, expected: DurableSessionStore.Version, guard: ?Mutation) !void {
        try self.persistVersionedSession(session, expected, guard, false);
    }

    fn persistVersionedSession(self: *SessionRegistry, session: Session, expected: DurableSessionStore.Version, guard: ?Mutation, unscoped_savepoint: bool) !void {
        if (self.durable) |durable| {
            try durable.saveRecord(session, self.max_record_bytes, .{
                .expected = expected,
                .ttl_ns = if (self.lease_store != null) self.owner_lease_ttl_ns else null,
                .guard = guard,
                .unscoped_savepoint = unscoped_savepoint,
            });
        } else try self.renewLeaseLocked(session.txn_id, session.owner_node_id);
    }

    fn persistOwnedVersion(self: *SessionRegistry, session: Session, expected: DurableSessionStore.Version) !void {
        return self.persistMutation(session, expected, null);
    }

    fn deletePersistent(self: *SessionRegistry, txn_id: db_mod.types.TxnId, expected: DurableSessionStore.Version, cutoff: ?u64, guard: ?Mutation) !void {
        if (self.durable) |durable| {
            try durable.delete(txn_id, expected, cutoff, guard, null);
        }
    }

    // BEGIN holds its slot across durable I/O. Every insertion must leave room
    // for all outstanding publications, including loads on other stripes.
    fn reserveSessionSlotLocked(self: *SessionRegistry, alloc: std.mem.Allocator) !void {
        const pending = std.math.cast(u32, self.reserved_session_count + 1) orelse return error.SessionLimitExceeded;
        try self.sessions.ensureUnusedCapacity(alloc, pending);
    }

    fn ensureSessionCapacityLocked(self: *SessionRegistry) !void {
        const limit = self.max_sessions orelse return;
        if (self.durable != null) return;
        const count = self.sessions.count();
        if (count + self.reserved_session_count >= limit) return error.SessionLimitExceeded;
    }

    /// The caller holds the txn stripe. Durable reads happen without the global
    /// registry mutex; publication is a short double-checked map operation.
    fn loadSessionCloneAssumeStripe(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId) !?Session {
        if (self.durableMissIsAuthoritative()) {
            const durable = self.durable.?;
            var loaded = (try durable.load(txn_id)) orelse {
                self.mutex.lock();
                defer self.mutex.unlock();
                if (self.sessions.fetchRemove(txn_id)) |old| {
                    var value = old.value;
                    value.deinit(alloc);
                }
                return null;
            };
            defer loaded.deinit(durable.alloc);
            var cloned = try loaded.clone(alloc);
            errdefer cloned.deinit(alloc);
            var cached = try loaded.clone(alloc);
            var cached_owned = true;
            defer if (cached_owned) cached.deinit(alloc);
            self.mutex.lock();
            defer self.mutex.unlock();
            try self.reserveSessionSlotLocked(alloc);
            // Decode supplies the latest durable state. The already chosen
            // snapshot is a process-local capability, so retain it only for
            // the same active transaction and the same locked SQL settings.
            if (self.sessions.getPtr(txn_id)) |resident| {
                if (resident.native_sql_cut) |cut| {
                    if (sameNativeSnapshotOwner(resident.*, loaded)) {
                        cloned.native_sql_cut = cut.retain();
                        cached.native_sql_cut = cut.retain();
                    }
                }
            }
            if (self.sessions.fetchRemove(txn_id)) |old| {
                var value = old.value;
                value.deinit(alloc);
            }
            self.sessions.putAssumeCapacity(txn_id, cached);
            cached_owned = false;
            return cloned;
        }
        self.mutex.lock();
        if (self.sessions.getPtr(txn_id)) |session| {
            const cloned = session.clone(alloc) catch |err| {
                self.mutex.unlock();
                return err;
            };
            self.mutex.unlock();
            return cloned;
        }
        self.mutex.unlock();
        const durable = self.durable orelse return null;
        var loaded = (try durable.loadInto(alloc, txn_id)) orelse return null;
        var loaded_owned = true;
        errdefer if (loaded_owned) loaded.deinit(alloc);
        self.mutex.lock();
        defer self.mutex.unlock();
        if (self.sessions.getPtr(txn_id)) |session| {
            loaded.deinit(alloc);
            loaded_owned = false;
            return try session.clone(alloc);
        }
        try self.reserveSessionSlotLocked(alloc);
        self.sessions.putAssumeCapacity(txn_id, loaded);
        loaded_owned = false;
        return try self.sessions.getPtr(txn_id).?.clone(alloc);
    }

    fn renewLeaseLocked(self: *SessionRegistry, txn_id: db_mod.types.TxnId, owner_node_id: u64) !void {
        const now_ns = nextTxnTimestamp();
        try self.renewLeaseLockedAt(txn_id, owner_node_id, now_ns);
    }

    pub fn renewOwnedLeases(self: *SessionRegistry, owner_node_id: u64, now_ns: u64) !usize {
        const durable = self.durable orelse return 0;
        var ids = std.ArrayListUnmanaged(db_mod.types.TxnId).empty;
        defer ids.deinit(durable.alloc);
        self.mutex.lock();
        if (self.lease_store == null or self.owner_lease_ttl_ns == null) {
            self.mutex.unlock();
            return 0;
        }
        var it = self.sessions.iterator();
        while (it.next()) |entry| {
            if (!shouldRenewLease(entry.value_ptr.*, owner_node_id)) continue;
            ids.append(durable.alloc, entry.key_ptr.*) catch |err| {
                self.mutex.unlock();
                return err;
            };
        }
        self.mutex.unlock();

        var renewed: usize = 0;
        for (ids.items) |txn_id| {
            const session_lock = self.sessionLock(txn_id);
            session_lock.lock();
            defer session_lock.unlock();
            self.mutex.lock();
            const still_owned = if (self.sessions.getPtr(txn_id)) |session| shouldRenewLease(session.*, owner_node_id) else false;
            self.mutex.unlock();
            if (!still_owned) continue;
            self.renewLeaseLockedAt(txn_id, owner_node_id, now_ns) catch |err| switch (err) {
                error.SessionLeaseLost => continue,
                else => return err,
            };
            renewed += 1;
        }
        // Renew durable coordinator leases first. Retained token maintenance
        // gets a separate bounded pass, with rotation for fairness under load.
        const deadline = @import("antfly_platform").time.monotonicNs() +| 5 * std.time.ns_per_s;
        const first_index = if (ids.items.len == 0) 0 else self.snapshot_renew_offset.load(.monotonic) % ids.items.len;
        for (0..ids.items.len) |offset| {
            if (@import("antfly_platform").time.monotonicNs() >= deadline) break;
            const index = (first_index + offset) % ids.items.len;
            self.snapshot_renew_offset.store(index + 1, .monotonic);
            const txn_id = ids.items[index];
            const execution = self.tryAcquireCommitExecution(txn_id) orelse continue;
            defer execution.release();
            const state = self.nativeSqlCut(durable.alloc, txn_id) catch continue;
            if (state.cut) |cut| {
                defer cut.release();
                // Loss is local to this cut and never recaptures newer data.
                cut.renew(deadline) catch {};
            }
        }
        return renewed;
    }

    fn shouldRenewLease(session: Session, owner: u64) bool {
        return session.owner_node_id == owner and !(session.graph_receipt and sessionCanExpire(session));
    }

    fn renewLeaseLockedAt(self: *SessionRegistry, txn_id: db_mod.types.TxnId, owner_node_id: u64, now_ns: u64) !void {
        const lease_store = self.lease_store orelse return;
        const ttl_ns = self.owner_lease_ttl_ns orelse return;
        const now_ms = now_ns / std.time.ns_per_ms;
        const ttl_ms = @max(@as(u64, 1), ttl_ns / std.time.ns_per_ms);
        if (!(try lease_store.renewMatching(txn_id, owner_node_id, now_ms, ttl_ms, self.hasColocatedLease()))) return error.SessionLeaseLost;
    }

    fn releaseLease(self: *SessionRegistry, txn_id: db_mod.types.TxnId, owner_node_id: u64) !void {
        const lease_store = self.lease_store orelse return;
        // A co-located lease was deleted in the session's version-checked transaction.
        if (self.hasColocatedLease()) return;
        _ = try lease_store.release(txn_id, owner_node_id);
    }

    fn loadLeaseExpiryLocked(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId) !u64 {
        const lease_store = self.lease_store orelse return 0;
        var record = (try lease_store.load(alloc, txn_id)) orelse return 0;
        defer lease_mod.deinitRecord(alloc, &record);
        // Durable lease records are external state. A corrupt or future-format
        // millisecond value must not panic a request handler during conversion.
        return leaseExpiryNs(record.expires_at_ms);
    }
};

pub fn sessionOwnerNodeId(txn_id: db_mod.types.TxnId) u64 {
    return std.mem.readInt(u64, txn_id[0..8], .big);
}

pub fn parseBeginRequest(alloc: std.mem.Allocator, body: []const u8) !BeginRequest {
    if (body.len == 0 or std.mem.eql(u8, body, "{}")) return .{};
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{});
    defer parsed.deinit();
    const root = switch (parsed.value) {
        .object => |obj| obj,
        else => return error.InvalidTransactionBeginRequest,
    };
    var req: BeginRequest = .{};
    if (root.get("sync_level")) |sync_level_value| {
        req.sync_level = parseSyncLevel(sync_level_value) orelse return error.InvalidTransactionBeginRequest;
    }
    return req;
}

pub fn parseStageReadRequest(alloc: std.mem.Allocator, body: []const u8) !OwnedTransactionCommitRequest {
    var read_req = try parseStageReadPayload(alloc, body);
    defer read_req.deinit(alloc);
    return try ownedRequestFromStageRead(alloc, read_req);
}

pub fn parseStageReadPayload(alloc: std.mem.Allocator, body: []const u8) !StageReadRequest {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{});
    defer parsed.deinit();
    const obj = switch (parsed.value) {
        .object => |obj| obj,
        else => return error.InvalidTransactionStageRequest,
    };

    return .{
        .table_name = try alloc.dupe(u8, requireString(obj, "table")),
        .key = try alloc.dupe(u8, requireString(obj, "key")),
        .version = try parseVersionString(requireString(obj, "version")),
    };
}

pub fn ownedRequestFromStageReadRequest(alloc: std.mem.Allocator, req: StageReadRequest) !OwnedTransactionCommitRequest {
    return try ownedRequestFromStageRead(alloc, req);
}

pub fn parseStageWriteRequest(alloc: std.mem.Allocator, body: []const u8) !OwnedTransactionCommitRequest {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{ .parse_numbers = false });
    defer parsed.deinit();
    const obj = switch (parsed.value) {
        .object => |obj| obj,
        else => return error.InvalidTransactionStageRequest,
    };
    const document = obj.get("document") orelse return error.InvalidTransactionStageRequest;
    if (document != .object) return error.InvalidTransactionStageRequest;

    var write_req = StageWriteRequest{
        .table_name = try alloc.dupe(u8, requireString(obj, "table")),
        .key = try alloc.dupe(u8, requireString(obj, "key")),
        .value_json = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(document, .{})}),
    };
    defer write_req.deinit(alloc);

    return try ownedRequestFromStageWrite(alloc, write_req);
}

pub fn parseStageDeleteRequest(alloc: std.mem.Allocator, body: []const u8) !OwnedTransactionCommitRequest {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{});
    defer parsed.deinit();
    const obj = switch (parsed.value) {
        .object => |obj| obj,
        else => return error.InvalidTransactionStageRequest,
    };

    var delete_req = StageDeleteRequest{
        .table_name = try alloc.dupe(u8, requireString(obj, "table")),
        .key = try alloc.dupe(u8, requireString(obj, "key")),
    };
    defer delete_req.deinit(alloc);

    return try ownedRequestFromStageDelete(alloc, delete_req);
}

pub fn buildBeginResponse(alloc: std.mem.Allocator, session: SessionInfo) !BeginResponse {
    const txn_hex = distributed_txn.encodeTxnIdHex(session.txn_id);
    return .{
        .transaction_id = try alloc.dupe(u8, &txn_hex),
        .begin_timestamp = session.begin_timestamp,
        .sync_level = syncLevelText(session.sync_level),
    };
}

pub fn buildTransactionStatusResponse(
    alloc: std.mem.Allocator,
    txn_id: db_mod.types.TxnId,
    status: []const u8,
) !TransactionStatusResponse {
    const txn_hex = distributed_txn.encodeTxnIdHex(txn_id);
    return .{
        .status = status,
        .transaction_id = try alloc.dupe(u8, &txn_hex),
    };
}

pub fn buildAbortResponse(alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId) !TransactionStatusResponse {
    return try buildTransactionStatusResponse(alloc, txn_id, "aborted");
}

pub fn buildStageResponse(alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId) !TransactionStatusResponse {
    return try buildTransactionStatusResponse(alloc, txn_id, "staged");
}

pub fn buildStageReadResponse(
    alloc: std.mem.Allocator,
    txn_id: db_mod.types.TxnId,
    snapshot: StageReadSnapshot,
) !StageReadResponse {
    const txn_hex = distributed_txn.encodeTxnIdHex(txn_id);
    const version_text = try std.fmt.allocPrint(alloc, "{d}", .{snapshot.version});
    const document = if (snapshot.document_json) |document_json|
        (try std.json.parseFromSlice(std.json.Value, alloc, document_json, .{ .parse_numbers = false })).value
    else
        .null;
    return .{
        .status = "staged",
        .transaction_id = try alloc.dupe(u8, &txn_hex),
        .snapshot = .{
            .table = snapshot.table_name,
            .key = snapshot.key,
            .version = version_text,
            .document = document,
        },
    };
}

pub fn buildSavepointStatusResponse(
    alloc: std.mem.Allocator,
    info: SavepointInfo,
    status: []const u8,
) !SavepointStatusResponse {
    const txn_hex = distributed_txn.encodeTxnIdHex(info.txn_id);
    return .{
        .status = status,
        .transaction_id = try alloc.dupe(u8, &txn_hex),
        .savepoint_id = info.savepoint_id,
    };
}

pub fn buildSavepointResponse(alloc: std.mem.Allocator, info: SavepointInfo) !SavepointStatusResponse {
    return try buildSavepointStatusResponse(alloc, info, "savepoint_created");
}

pub fn buildRollbackResponse(alloc: std.mem.Allocator, info: SavepointInfo) !SavepointStatusResponse {
    return try buildSavepointStatusResponse(alloc, info, "rolled_back");
}

pub fn buildSessionStatusResponse(alloc: std.mem.Allocator, status: SessionStatus) !SessionStatusResponse {
    const txn_hex = distributed_txn.encodeTxnIdHex(status.txn_id);
    const now_ns = nextTxnTimestamp();
    return .{
        .transaction_id = try alloc.dupe(u8, &txn_hex),
        .owner_node_id = status.owner_node_id,
        .begin_timestamp = status.begin_timestamp,
        .last_touched_timestamp = status.last_touched_timestamp,
        .lease_expires_at = status.lease_expires_at,
        .lease_state = @tagName(sessionLeaseState(status.lease_expires_at, now_ns)),
        .sync_level = syncLevelText(status.sync_level),
        .disposition = @tagName(status.disposition),
        .staged_table_count = status.staged_table_count,
        .staged_read_count = status.staged_read_count,
        .staged_write_count = status.staged_write_count,
        .staged_delete_count = status.staged_delete_count,
        .read_snapshot_count = status.read_snapshot_count,
        .savepoint_count = status.savepoint_count,
        .savepoint_limit = status.savepoint_limit,
        .remaining_savepoints = status.remaining_savepoints,
        .durable = status.durable,
    };
}

fn buildSessionReadSnapshotResponse(
    alloc: std.mem.Allocator,
    snapshot: SessionReadSnapshot,
) !SessionReadSnapshotResponse {
    return .{
        .table = snapshot.table_name,
        .key = snapshot.key,
        .version = snapshot.version,
        .document = if (snapshot.document_json) |document_json|
            (try std.json.parseFromSlice(std.json.Value, alloc, document_json, .{ .parse_numbers = false })).value
        else
            null,
    };
}

pub fn buildSessionDetailsResponse(alloc: std.mem.Allocator, details: SessionDetails) !SessionDetailsResponse {
    const status = try buildSessionStatusResponse(alloc, details.status);
    const tables = try alloc.alloc(SessionTableDetailResponse, details.tables.len);
    for (details.tables, 0..) |table, i| {
        tables[i] = .{
            .table = table.table_name,
            .staged_read_count = table.staged_read_count,
            .staged_write_count = table.staged_write_count,
            .staged_delete_count = table.staged_delete_count,
            .staged_predicate_count = table.staged_predicate_count,
        };
    }

    const read_snapshots = try alloc.alloc(SessionReadSnapshotResponse, details.read_snapshots.len);
    for (details.read_snapshots, 0..) |snapshot, i| {
        read_snapshots[i] = try buildSessionReadSnapshotResponse(alloc, snapshot);
    }

    const savepoint_ids = try alloc.alloc(u64, details.savepoint_ids.len);
    @memcpy(savepoint_ids, details.savepoint_ids);

    return .{
        .transaction_id = status.transaction_id,
        .owner_node_id = status.owner_node_id,
        .begin_timestamp = status.begin_timestamp,
        .last_touched_timestamp = status.last_touched_timestamp,
        .lease_expires_at = status.lease_expires_at,
        .lease_state = status.lease_state,
        .sync_level = status.sync_level,
        .disposition = status.disposition,
        .staged_table_count = status.staged_table_count,
        .staged_read_count = status.staged_read_count,
        .staged_write_count = status.staged_write_count,
        .staged_delete_count = status.staged_delete_count,
        .read_snapshot_count = status.read_snapshot_count,
        .savepoint_count = status.savepoint_count,
        .savepoint_limit = status.savepoint_limit,
        .remaining_savepoints = status.remaining_savepoints,
        .durable = status.durable,
        .tables = tables,
        .read_snapshots = read_snapshots,
        .savepoint_ids = savepoint_ids,
    };
}

pub fn buildSessionListResponse(alloc: std.mem.Allocator, sessions: []const SessionStatus) !SessionListResponse {
    const now_ns = nextTxnTimestamp();
    var lease_held_count: usize = 0;
    var lease_expired_count: usize = 0;
    for (sessions) |session| {
        switch (sessionLeaseState(session.lease_expires_at, now_ns)) {
            .held => lease_held_count += 1,
            .expired => lease_expired_count += 1,
            .none => {},
        }
    }

    const generated = try alloc.alloc(SessionStatusResponse, sessions.len);
    for (sessions, 0..) |session, i| {
        generated[i] = try buildSessionStatusResponse(alloc, session);
    }

    return .{
        .session_count = sessions.len,
        .lease_held_count = lease_held_count,
        .lease_expired_count = lease_expired_count,
        .sessions = generated,
    };
}

pub fn buildSessionCleanupResponse(removed: usize, cutoff_ns: u64) SessionCleanupResponse {
    return .{
        .removed = removed,
        .cutoff_ns = cutoff_ns,
    };
}

pub fn encodeSessionStatusResponse(alloc: std.mem.Allocator, status: SessionStatus) ![]u8 {
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();
    const response = try buildSessionStatusResponse(arena, status);
    return try std.json.Stringify.valueAlloc(alloc, response, .{});
}

pub fn encodeSessionDetailsResponse(alloc: std.mem.Allocator, details: SessionDetails) ![]u8 {
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();
    const response = try buildSessionDetailsResponse(arena, details);
    return try std.json.Stringify.valueAlloc(alloc, response, .{});
}

pub fn encodeSessionListResponse(alloc: std.mem.Allocator, sessions: []const SessionStatus) ![]u8 {
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();
    const response = try buildSessionListResponse(arena, sessions);
    return try std.json.Stringify.valueAlloc(alloc, response, .{});
}

pub fn encodeSessionCleanupResponse(alloc: std.mem.Allocator, removed: usize, cutoff_ns: u64) ![]u8 {
    return try std.json.Stringify.valueAlloc(alloc, buildSessionCleanupResponse(removed, cutoff_ns), .{});
}

fn buildCommitConflictResponse(info: CommitConflict) CommitConflictResponse {
    return .{
        .table = info.table_name,
        .key = info.key,
        .message = info.message,
        .kind = conflictKindText(info.kind),
        .reason = info.reason,
        .retryable = info.retryable,
        .retry_after_ms = info.retry_after_ms,
        .retry_scope = info.retry_scope,
        .expected_version = info.expected_version,
        .current_version = info.current_version,
        .participant = if (info.group_id != null or info.phase != null) .{
            .group_id = info.group_id,
            .phase = if (info.phase) |phase| participantPhaseText(phase) else null,
        } else null,
    };
}

fn buildCommitTablesResponse(
    alloc: std.mem.Allocator,
    tables: []const TableCommitRequest,
) !CommitTablesResponse {
    var out = CommitTablesResponse{};
    errdefer out.deinit(alloc);
    for (tables) |table| {
        try out.map.put(alloc, table.table_name, table.result());
    }
    return out;
}

pub fn buildCommitResponse(
    alloc: std.mem.Allocator,
    status: []const u8,
    conflict: ?CommitConflict,
    tables: ?[]const TableCommitRequest,
) !CommitResponse {
    return .{
        .status = status,
        .conflict = if (conflict) |info| buildCommitConflictResponse(info) else null,
        .tables = if (tables) |table_entries| try buildCommitTablesResponse(alloc, table_entries) else null,
    };
}

pub fn buildMultiBatchResponse(
    alloc: std.mem.Allocator,
    status: []const u8,
    tables: []const TableCommitRequest,
) !MultiBatchResponse {
    return .{ .status = status, .tables = try buildCommitTablesResponse(alloc, tables) };
}

pub fn buildSessionCommitResponse(
    alloc: std.mem.Allocator,
    txn_id: db_mod.types.TxnId,
    status: []const u8,
    conflict: ?CommitConflict,
    tables: ?[]const TableCommitRequest,
) !SessionCommitResponse {
    const txn_hex = distributed_txn.encodeTxnIdHex(txn_id);
    return .{
        .status = status,
        .transaction_id = try alloc.dupe(u8, &txn_hex),
        .conflict = if (conflict) |info| buildCommitConflictResponse(info) else null,
        .tables = if (tables) |table_entries| try buildCommitTablesResponse(alloc, table_entries) else null,
    };
}

pub fn encodeBeginResponse(alloc: std.mem.Allocator, session: SessionInfo) ![]u8 {
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const response = try buildBeginResponse(arena_impl.allocator(), session);
    return try std.json.Stringify.valueAlloc(alloc, response, .{});
}

pub fn encodeAbortResponse(alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId) ![]u8 {
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const response = try buildAbortResponse(arena_impl.allocator(), txn_id);
    return try std.json.Stringify.valueAlloc(alloc, response, .{});
}

pub fn encodeStageResponse(alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId) ![]u8 {
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const response = try buildStageResponse(arena_impl.allocator(), txn_id);
    return try std.json.Stringify.valueAlloc(alloc, response, .{});
}

pub fn encodeStageReadResponse(alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId, snapshot: StageReadSnapshot) ![]u8 {
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const response = try buildStageReadResponse(arena_impl.allocator(), txn_id, snapshot);
    return try std.json.Stringify.valueAlloc(alloc, response, .{});
}

pub fn encodeSavepointResponse(alloc: std.mem.Allocator, info: SavepointInfo) ![]u8 {
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const response = try buildSavepointResponse(arena_impl.allocator(), info);
    return try std.json.Stringify.valueAlloc(alloc, response, .{});
}

pub fn encodeRollbackResponse(alloc: std.mem.Allocator, info: SavepointInfo) ![]u8 {
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const response = try buildRollbackResponse(arena_impl.allocator(), info);
    return try std.json.Stringify.valueAlloc(alloc, response, .{});
}

pub fn parseCommitRequest(alloc: std.mem.Allocator, body: []const u8) !OwnedTransactionCommitRequest {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{ .parse_numbers = false });
    defer parsed.deinit();
    return try parseCommitValue(alloc, parsed.value);
}

pub fn parseMultiBatchRequest(alloc: std.mem.Allocator, body: []const u8) !OwnedTransactionCommitRequest {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{ .parse_numbers = false });
    defer parsed.deinit();

    const root = switch (parsed.value) {
        .object => |obj| obj,
        else => return error.InvalidTransactionCommitRequest,
    };
    var fields = root.iterator();
    while (fields.next()) |field| {
        if (!std.mem.eql(u8, field.key_ptr.*, "tables") and
            !std.mem.eql(u8, field.key_ptr.*, "sync_level") and
            !std.mem.eql(u8, field.key_ptr.*, "commit_requires"))
        {
            return error.InvalidTransactionCommitRequest;
        }
    }

    var req: OwnedTransactionCommitRequest = .{};
    errdefer req.deinit(alloc);
    req.tables = try parseTables(alloc, root.get("tables") orelse return error.InvalidTransactionCommitRequest);
    const requirement = if (root.get("commit_requires")) |value| try db_mod.types.parseCommitRequires(value) else null;
    req.commit_requires = requirement;
    req.retainCommitRequires();
    if (req.tables.len == 0) return error.InvalidTransactionCommitRequest;
    if (root.get("sync_level")) |sync_level_value| {
        req.sync_level = parseSyncLevel(sync_level_value) orelse return error.InvalidTransactionCommitRequest;
    } else {
        for (req.tables) |table| {
            if (@backingInt(table.batch.req.sync_level) > @backingInt(req.sync_level)) {
                req.sync_level = table.batch.req.sync_level;
            }
        }
    }
    var operation_count: usize = 0;
    for (req.tables) |table| operation_count += table.batch.writes.len + table.batch.deletes.len + table.batch.transforms.len + table.batch.graph_writes.len + table.batch.graph_deletes.len;
    if (operation_count == 0) return error.InvalidTransactionCommitRequest;
    return req;
}

pub fn encodeCommitRequest(alloc: std.mem.Allocator, req: OwnedTransactionCommitRequest) ![]u8 {
    return encodeCommitRequestMode(alloc, req, false);
}
fn encodeCommitRequestMode(alloc: std.mem.Allocator, req: OwnedTransactionCommitRequest, trusted: bool) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    const versioned = trusted and (req.bound_catalog_incarnation != null or req.commit_requires != null or for (req.tables) |table| {
        if (table.commit_requires_floor != null or table.batch.req.commit_requires != null) break true;
    } else false);
    if (versioned) try out.appendSlice(alloc, if (req.bound_catalog_incarnation != null) "[\"bound-catalog-request-v1\"," else "[\"commit-requirements-request-v1\",");
    try out.appendSlice(alloc, "{\"read_set\":[");
    for (req.read_set, 0..) |item, i| {
        if (i > 0) try out.append(alloc, ',');
        try out.appendSlice(alloc, "{\"table\":");
        try appendJsonString(alloc, &out, item.table_name);
        try out.appendSlice(alloc, ",\"key\":");
        try appendJsonString(alloc, &out, item.key);
        try out.appendSlice(alloc, ",\"version\":");
        const version_text = try std.fmt.allocPrint(alloc, "{d}", .{item.expected_version});
        defer alloc.free(version_text);
        try appendJsonString(alloc, &out, version_text);
        try out.append(alloc, '}');
    }
    try out.appendSlice(alloc, "],\"tables\":{");
    for (req.tables, 0..) |table, i| {
        if (i > 0) try out.append(alloc, ',');
        try appendJsonString(alloc, &out, table.table_name);
        try out.append(alloc, ':');
        var encoded_table = table;
        // Public round trips retain an irrevocable direct floor. Trusted
        // persistence stores provenance separately instead of flattening it.
        if (!trusted and table.commit_requires_floor == .direct_enrichments)
            encoded_table.batch.req.commit_requires = .direct_enrichments;
        const batch_json = try encodeTableBatchRequest(alloc, encoded_table);
        defer alloc.free(batch_json);
        try out.appendSlice(alloc, batch_json);
    }
    try out.append(alloc, '}');
    if (req.commit_requires) |requirement| try out.print(alloc, ",\"commit_requires\":\"{s}\"", .{@tagName(requirement)});
    try out.appendSlice(alloc, ",\"sync_level\":");
    try appendJsonString(alloc, &out, syncLevelText(req.sync_level));
    if (versioned) {
        try out.appendSlice(alloc, ",\"commit_requires_floors\":{");
        var first_floor = true;
        for (req.tables) |table| if (table.commit_requires_floor) |requirement| {
            if (!first_floor) try out.append(alloc, ',');
            first_floor = false;
            try appendJsonString(alloc, &out, table.table_name);
            try out.print(alloc, ":\"{s}\"", .{@tagName(requirement)});
        };
        try out.append(alloc, '}');
    }
    if (trusted and req.frozen_births.rows != 0) {
        try out.appendSlice(alloc, ",\"frozen_births\":");
        const births = try std.json.Stringify.valueAlloc(alloc, req.frozen_births, .{});
        defer alloc.free(births);
        try out.appendSlice(alloc, births);
    }
    if (trusted and req.constraint_timing.items.len != 0) {
        try out.appendSlice(alloc, ",\"constraint_timing\":");
        const timing = try std.json.Stringify.valueAlloc(alloc, req.constraint_timing.items, .{});
        defer alloc.free(timing);
        try out.appendSlice(alloc, timing);
    }
    if (trusted) if (req.bound_catalog_incarnation) |incarnation| {
        try out.appendSlice(alloc, ",\"bound_catalog_incarnation\":\"");
        try out.appendSlice(alloc, &std.fmt.bytesToHex(incarnation, .lower));
        try out.append(alloc, '"');
    };
    if (trusted and req.catalog_bindings.items.len != 0) {
        try out.appendSlice(alloc, ",\"catalog_bindings\":");
        const bindings = try std.json.Stringify.valueAlloc(alloc, req.catalog_bindings.items, .{});
        defer alloc.free(bindings);
        try out.appendSlice(alloc, bindings);
    }
    if (trusted) {
        try out.appendSlice(alloc, ",\"prepared_domain_rows\":{");
        var first_domain_table = true;
        for (req.tables) |table| if (batch_api.DomainHandoff.present(table.batch.writes)) {
            try batch_api.DomainHandoff.requireUniqueKeys(alloc, table.batch.writes);
            if (!first_domain_table) try out.append(alloc, ',');
            first_domain_table = false;
            try appendJsonString(alloc, &out, table.table_name);
            var encoded: std.Io.Writer.Allocating = .init(alloc);
            defer encoded.deinit();
            try batch_api.DomainHandoff.writeWire(&encoded.writer, table.batch.writes);
            try out.append(alloc, ':');
            try out.appendSlice(alloc, encoded.written());
        };
        try out.append(alloc, '}');
        try out.appendSlice(alloc, ",\"json_null_fields\":{");
        var first_null_table = true;
        for (req.tables) |table| {
            var first_null_write = true;
            for (table.batch.writes) |write| if (write.json_null_fields.len != 0) {
                if (first_null_write) {
                    if (!first_null_table) try out.append(alloc, ',');
                    first_null_table = false;
                    try appendJsonString(alloc, &out, table.table_name);
                    try out.appendSlice(alloc, ":{");
                } else try out.append(alloc, ',');
                first_null_write = false;
                try appendJsonString(alloc, &out, write.key);
                try out.append(alloc, ':');
                const fields = try std.json.Stringify.valueAlloc(alloc, write.json_null_fields, .{});
                defer alloc.free(fields);
                try out.appendSlice(alloc, fields);
            };
            if (!first_null_write) try out.append(alloc, '}');
        }
        try out.append(alloc, '}');
        try out.appendSlice(alloc, ",\"sql_authorization\":{");
        var first_authority = true;
        for (req.tables) |table| if (table.sql_authorization) |authority| {
            try authority.validate();
            if (!first_authority) try out.append(alloc, ',');
            first_authority = false;
            try appendJsonString(alloc, &out, table.table_name);
            try out.append(alloc, ':');
            const bytes = try std.json.Stringify.valueAlloc(alloc, authority, .{});
            defer alloc.free(bytes);
            try out.appendSlice(alloc, bytes);
        };
        try out.append(alloc, '}');
        try out.appendSlice(alloc, ",\"schema_versions\":{");
        var first_epoch = true;
        for (req.tables) |table| if (table.schema_version) |version| {
            if (!first_epoch) try out.append(alloc, ',');
            first_epoch = false;
            try appendJsonString(alloc, &out, table.table_name);
            try out.print(alloc, ":{d}", .{version});
        };
        try out.append(alloc, '}');
        try out.appendSlice(alloc, ",\"relational_schema_versions\":{");
        var first_schema = true;
        for (req.tables) |table| if (table.relational_schema_version) |version| {
            if (!first_schema) try out.append(alloc, ',');
            first_schema = false;
            try appendJsonString(alloc, &out, table.table_name);
            try out.print(alloc, ":{d}", .{version});
        };
        try out.append(alloc, '}');
        // Private durable dependencies are distinct from the public read set.
        // Savepoints and recovery use this same encoding; public bodies cannot
        // supply these observations or receive their physical content digests.
        if (for (req.tables) |table| {
            if (table.document_revisions != null) break true;
        } else false) {
            try out.appendSlice(alloc, ",\"document_revision_handoffs\":{");
            var first_revision_table = true;
            for (req.tables) |table| if (table.document_revisions) |revisions| {
                if (!first_revision_table) try out.append(alloc, ',');
                first_revision_table = false;
                try appendJsonString(alloc, &out, table.table_name);
                try out.append(alloc, ':');
                const bytes = try std.json.Stringify.valueAlloc(alloc, revisions.value, .{});
                defer alloc.free(bytes);
                try out.appendSlice(alloc, bytes);
            };
            try out.append(alloc, '}');
        }
        try out.appendSlice(alloc, ",\"trigger_receipts\":{");
        var first_trigger = true;
        for (req.tables) |table| if (table.trigger_receipts) |receipts| {
            if (!first_trigger) try out.append(alloc, ',');
            first_trigger = false;
            try appendJsonString(alloc, &out, table.table_name);
            try out.append(alloc, ':');
            const bytes = try @import("antfly_local_sources").sql_catalog.trigger_handoff.encodeReceipts(alloc, receipts.value);
            defer alloc.free(bytes);
            try out.appendSlice(alloc, bytes);
        };
        try out.append(alloc, '}');
        try out.appendSlice(alloc, ",\"conflict_guards\":{");
        var first_guard = true;
        for (req.tables) |table| if (table.conflict_guards) |guards| {
            if (!first_guard) try out.append(alloc, ',');
            first_guard = false;
            try appendJsonString(alloc, &out, table.table_name);
            try out.append(alloc, ':');
            const bytes = try std.json.Stringify.valueAlloc(alloc, guards.value, .{});
            defer alloc.free(bytes);
            try out.appendSlice(alloc, bytes);
        };
        try out.append(alloc, '}');
        try out.appendSlice(alloc, ",\"range_guards\":{");
        var first_range_guard = true;
        for (req.tables) |table| if (table.range_guards) |guards| {
            if (!first_range_guard) try out.append(alloc, ',');
            first_range_guard = false;
            try appendJsonString(alloc, &out, table.table_name);
            try out.append(alloc, ':');
            const bytes = try std.json.Stringify.valueAlloc(alloc, guards.value, .{});
            defer alloc.free(bytes);
            try out.appendSlice(alloc, bytes);
        };
        try out.append(alloc, '}');
        try out.appendSlice(alloc, ",\"observed_predicates\":{");
        for (req.tables, 0..) |table, i| {
            if (i != 0) try out.append(alloc, ',');
            try appendJsonString(alloc, &out, table.table_name);
            try out.appendSlice(alloc, ":[");
            for (table.predicates.items, 0..) |predicate, j| {
                if (j != 0) try out.append(alloc, ',');
                var version_buf: [20]u8 = undefined;
                var revision_buf: [20]u8 = undefined;
                const encoded = try std.json.Stringify.valueAlloc(alloc, .{
                    .key = predicate.key,
                    .version = try std.fmt.bufPrint(&version_buf, "{d}", .{predicate.expected_version}),
                    .digest = predicate.expected_content_digest,
                    .mutation_revision = if (predicate.expected_mutation_revision) |revision| try std.fmt.bufPrint(&revision_buf, "{d}", .{revision}) else null,
                    .unique_absence = predicate.unique_absence,
                    .row_policy_requirement = predicate.row_policy_requirement,
                }, .{});
                defer alloc.free(encoded);
                try out.appendSlice(alloc, encoded);
            }
            try out.append(alloc, ']');
        }
        try out.append(alloc, '}');
    }
    try out.append(alloc, '}');
    if (versioned) try out.append(alloc, ']');
    return try out.toOwnedSlice(alloc);
}

pub fn encodeCommitResponse(
    alloc: std.mem.Allocator,
    status: []const u8,
    conflict: ?CommitConflict,
    tables: ?[]const TableCommitRequest,
) ![]u8 {
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const response = try buildCommitResponse(arena_impl.allocator(), status, conflict, tables);
    return try std.json.Stringify.valueAlloc(alloc, response, .{ .emit_null_optional_fields = false });
}

pub fn encodeSessionCommitResponse(
    alloc: std.mem.Allocator,
    txn_id: db_mod.types.TxnId,
    status: []const u8,
    conflict: ?CommitConflict,
    tables: ?[]const TableCommitRequest,
) ![]u8 {
    var arena_impl = std.heap.ArenaAllocator.init(alloc);
    defer arena_impl.deinit();
    const response = try buildSessionCommitResponse(arena_impl.allocator(), txn_id, status, conflict, tables);
    return try std.json.Stringify.valueAlloc(alloc, response, .{ .emit_null_optional_fields = false });
}

pub fn encodeSessionStageConflictResponse(
    alloc: std.mem.Allocator,
    txn_id: db_mod.types.TxnId,
    conflict: CommitConflict,
) ![]u8 {
    return try encodeSessionCommitResponse(alloc, txn_id, "conflict", conflict, null);
}

pub fn conflictFromOutcome(outcome: distributed_txn.CommitConflict) CommitConflict {
    return .{
        .table_name = outcome.table_name,
        .key = outcome.key,
        .message = outcome.message,
        .group_id = outcome.group_id,
        .phase = outcome.phase,
        .reason = outcome.reason,
        .kind = classifyConflictKind(outcome.message),
        .retryable = outcome.reason == null and (outcome.retryable or isRetryableConflict(outcome.message)),
        .retry_after_ms = retryAfterMsForKind(classifyConflictKind(outcome.message)),
        .retry_scope = retryScopeForKind(classifyConflictKind(outcome.message)),
    };
}

pub fn topologyChangedConflict(table_name: []const u8) CommitConflict {
    return .{
        .table_name = table_name,
        .key = "",
        .message = "topology changed",
        .kind = .topology_changed,
        .retryable = true,
        .retry_after_ms = 100,
        .retry_scope = "topology",
    };
}

pub fn isTopologyChangedConflictMessage(message: []const u8) bool {
    return std.mem.eql(u8, message, "topology changed");
}

pub fn versionConflict(table_name: []const u8, key: []const u8, expected_version: ?u64, current_version: ?u64) CommitConflict {
    return .{
        .table_name = table_name,
        .key = key,
        .message = "version conflict",
        .kind = .version_conflict,
        .retryable = false,
        .expected_version = expected_version,
        .current_version = current_version,
    };
}

/// Preflight must preserve the same create-only cause as native prepare.
/// Inspect only the first failed observation: this is one bounded request
/// walk, not a per-row secondary lookup or a new source of write authority.
pub fn readSetConflict(req: OwnedTransactionCommitRequest, item: TransactionReadItem, current_version: u64) CommitConflict {
    var result = versionConflict(item.table_name, item.key, item.expected_version, current_version);
    if (item.expected_version != 0 or current_version == 0) return result;
    const physical = req.physicalName(item.table_name);
    for (req.tables) |table| {
        if (!std.mem.eql(u8, physical, req.physicalName(table.table_name))) continue;
        for (table.predicates.items) |predicate| {
            if (!predicate.unique_absence or predicate.expected_version != 0 or !std.mem.eql(u8, predicate.key, item.key)) continue;
            result.kind = .transaction_conflict;
            result.reason = .unique_constraint_violation;
            result.message = "unique constraint violation";
            return result;
        }
    }
    return result;
}

test "transaction read preflight preserves physical unique absence without relabeling optimistic conflicts" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var predicates = [_]db_mod.types.TransactionVersionPredicate{.{ .key = "doc:a", .expected_version = 0, .unique_absence = true }};
    var tables = [_]TableCommitRequest{.{ .table_name = @constCast("logical"), .predicates = .{ .items = &predicates, .capacity = predicates.len, .pointer_stability = .{} } }};
    var request: OwnedTransactionCommitRequest = .{ .tables = &tables };
    try request.bind(arena.allocator(), "logical", "physical");
    const item: TransactionReadItem = .{ .table_name = @constCast("physical"), .key = @constCast("doc:a"), .expected_version = 0 };
    try std.testing.expectEqual(CommitConflictReason.unique_constraint_violation, readSetConflict(request, item, 42).reason.?);
    try std.testing.expect(readSetConflict(request, item, 0).reason == null);
    var other = item;
    other.key = @constCast("doc:b");
    try std.testing.expect(readSetConflict(request, other, 42).reason == null);
    other = item;
    other.table_name = @constCast("unrelated");
    try std.testing.expect(readSetConflict(request, other, 42).reason == null);
    other = item;
    other.expected_version = 7;
    try std.testing.expect(readSetConflict(request, other, 42).reason == null);
    predicates[0].unique_absence = false;
    try std.testing.expect(readSetConflict(request, item, 42).reason == null);
}

pub fn participantUnavailableConflict(table_name: []const u8) CommitConflict {
    return .{
        .table_name = table_name,
        .key = "",
        .message = "participant unavailable",
        .kind = .participant_unavailable,
        .retryable = true,
        .retry_after_ms = 50,
        .retry_scope = "participant",
    };
}

pub fn docIdentityUnavailableConflict(table_name: []const u8) CommitConflict {
    return .{
        .table_name = table_name,
        .key = "",
        .message = "doc identity unavailable",
        .kind = .doc_identity_unavailable,
        .retryable = true,
        .retry_after_ms = 100,
        .retry_scope = "doc_identity",
    };
}

pub fn decisionConflict(table_name: []const u8) CommitConflict {
    return .{
        .table_name = table_name,
        .key = "",
        .message = "decision conflict",
        .kind = .transaction_conflict,
        .retryable = false,
    };
}

pub fn tornStateConflict(table_name: []const u8) CommitConflict {
    return .{
        .table_name = table_name,
        .key = "",
        .message = "torn transaction state",
        .kind = .torn_state,
        .retryable = false,
    };
}

pub fn sessionLeaseLostConflict(table_name: []const u8) CommitConflict {
    return .{
        .table_name = table_name,
        .key = "",
        .message = "session lease lost",
        .kind = .session_lease_lost,
        .retryable = true,
        .retry_after_ms = 25,
        .retry_scope = "session",
    };
}

fn parseReadSet(alloc: std.mem.Allocator, value: std.json.Value) ![]TransactionReadItem {
    const arr = switch (value) {
        .array => |arr| arr,
        else => return error.InvalidTransactionCommitRequest,
    };
    var out = try alloc.alloc(TransactionReadItem, arr.items.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*item| item.deinit(alloc);
        alloc.free(out);
    }
    for (arr.items) |item| {
        const obj = switch (item) {
            .object => |obj| obj,
            else => return error.InvalidTransactionCommitRequest,
        };
        out[initialized] = .{
            .table_name = try alloc.dupe(u8, requireString(obj, "table")),
            .key = try alloc.dupe(u8, requireString(obj, "key")),
            .expected_version = try parseVersionString(requireString(obj, "version")),
        };
        initialized += 1;
    }
    return out;
}

fn parseStoredCommitValue(alloc: std.mem.Allocator, encoded: std.json.Value) !OwnedTransactionCommitRequest {
    const value = if (encoded == .array) blk: {
        const fields = encoded.array.items;
        if (fields.len != 2 or fields[0] != .string or fields[1] != .object) return error.InvalidTransactionSessionRecord;
        const bound = std.mem.eql(u8, fields[0].string, "bound-catalog-request-v1");
        if (!bound and !std.mem.eql(u8, fields[0].string, "commit-requirements-request-v1")) return error.InvalidTransactionSessionRecord;
        if (bound != fields[1].object.contains("bound_catalog_incarnation")) return error.InvalidTransactionSessionRecord;
        break :blk fields[1];
    } else encoded;
    if (encoded == .array and !value.object.contains("commit_requires_floors")) return error.InvalidTransactionSessionRecord;
    var request = try parseCommitValue(alloc, value);
    errdefer request.deinit(alloc);
    if (value.object.get("commit_requires_floors")) |floors| {
        if (floors != .object) return error.InvalidTransactionSessionRecord;
        var entries = floors.object.iterator();
        while (entries.next()) |entry| {
            const table = for (request.tables) |*table| {
                if (std.mem.eql(u8, table.table_name, entry.key_ptr.*)) break table;
            } else return error.InvalidTransactionSessionRecord;
            const floor = (try db_mod.types.parseCommitRequires(entry.value_ptr.*)) orelse return error.InvalidTransactionSessionRecord;
            if (table.batch.req.commit_requires == .direct_enrichments and floor != .direct_enrichments)
                return error.InvalidTransactionSessionRecord;
            table.commit_requires_floor = floor;
        }
    }
    if (value.object.get("frozen_births")) |births| request.frozen_births = try @import("sql/copy/frozen_births.zig").Ledger.parse(alloc, births);
    if (value.object.get("constraint_timing")) |timing| {
        if (timing != .array or timing.array.items.len > 4096) return error.InvalidTransactionSessionRecord;
        var parsed = try std.json.parseFromValue([]const @import("antfly_local_sources").storage_relational_index.ConstraintTiming, alloc, timing, .{});
        defer parsed.deinit();
        for (parsed.value) |mode| try request.setConstraintTiming(alloc, mode);
    }
    // This function reads owned durable records only. Public request parsing
    // intentionally ignores the private preparation authority envelope.
    if (value.object.get("prepared_domain_rows")) |tables| {
        if (tables != .object) return error.InvalidTransactionSessionRecord;
        var entries = tables.object.iterator();
        while (entries.next()) |entry| {
            const table = for (request.tables) |*table| {
                if (std.mem.eql(u8, table.table_name, entry.key_ptr.*)) break table;
            } else return error.InvalidTransactionSessionRecord;
            var parsed = try std.json.parseFromValue(batch_api.DomainHandoff.Wire, alloc, entry.value_ptr.*, .{});
            defer parsed.deinit();
            table.batch.prepared_domain_rows = try batch_api.DomainHandoff.attach(alloc, table.batch.writes, parsed.value);
        }
    }
    if (value.object.get("json_null_fields")) |tables| {
        if (tables != .object) return error.InvalidTransactionSessionRecord;
        var entries = tables.object.iterator();
        while (entries.next()) |entry| {
            const table = for (request.tables) |*table| {
                if (std.mem.eql(u8, table.table_name, entry.key_ptr.*)) break table;
            } else return error.InvalidTransactionSessionRecord;
            if (entry.value_ptr.* != .object) return error.InvalidTransactionSessionRecord;
            var rows = entry.value_ptr.object.iterator();
            while (rows.next()) |row| {
                const write = for (table.batch.writes) |*write| {
                    if (std.mem.eql(u8, write.key, row.key_ptr.*)) break write;
                } else return error.InvalidTransactionSessionRecord;
                var parsed = try std.json.parseFromValue([]const []const u8, alloc, row.value_ptr.*, .{});
                defer parsed.deinit();
                if (parsed.value.len > 256) return error.InvalidTransactionSessionRecord;
                write.json_null_fields = try cloneJsonNullFields(alloc, parsed.value);
            }
        }
    }
    if (value.object.get("bound_catalog_incarnation")) |field| {
        if (encoded != .array) return error.InvalidTransactionSessionRecord;
        if (field != .string or field.string.len != 32) return error.InvalidTransactionSessionRecord;
        var incarnation: [16]u8 = undefined;
        _ = std.fmt.hexToBytes(&incarnation, field.string) catch return error.InvalidTransactionSessionRecord;
        request.bound_catalog_incarnation = incarnation;
    }
    if (value.object.get("catalog_bindings")) |bindings| {
        if (bindings != .array) return error.InvalidTransactionSessionRecord;
        for (bindings.array.items) |binding| {
            if (binding != .object) return error.InvalidTransactionSessionRecord;
            const logical = requireString(binding.object, "logical");
            const physical = requireString(binding.object, "physical");
            if (logical.len == 0 or physical.len == 0) return error.InvalidTransactionSessionRecord;
            var persistence: ?@import("antfly_local_sources").common_topology_records.RelationPersistence.Descriptor = null;
            if (binding.object.get("sql_persistence")) |raw| {
                const decoded = try std.json.parseFromValue(@import("antfly_local_sources").common_topology_records.RelationPersistence.Descriptor, alloc, raw, .{});
                defer decoded.deinit();
                try decoded.value.validate();
                persistence = decoded.value;
            }
            try request.bindWithPersistence(alloc, logical, physical, persistence);
        }
    }
    if (value.object.get("sql_authorization")) |authorities| {
        if (authorities != .object) return error.InvalidTransactionSessionRecord;
        var entries = authorities.object.iterator();
        while (entries.next()) |entry| {
            const table = for (request.tables) |*table| {
                if (std.mem.eql(u8, table.table_name, entry.key_ptr.*)) break table;
            } else return error.InvalidTransactionSessionRecord;
            var parsed = try std.json.parseFromValue(TableCommitRequest.SqlAuthorization, alloc, entry.value_ptr.*, .{});
            defer parsed.deinit();
            try parsed.value.validate();
            table.sql_authorization = parsed.value;
        }
    }
    if (value.object.get("schema_versions")) |versions| {
        if (versions != .object) return error.InvalidTransactionSessionRecord;
        var entries = versions.object.iterator();
        while (entries.next()) |entry| {
            const table = for (request.tables) |*table| {
                if (std.mem.eql(u8, table.table_name, entry.key_ptr.*)) break table;
            } else return error.InvalidTransactionSessionRecord;
            const version: u64 = switch (entry.value_ptr.*) {
                .integer => |number| try nonNegativeRecordInteger(number),
                .number_string => |number| try recordNumber(number),
                else => return error.InvalidTransactionSessionRecord,
            };
            table.schema_version = std.math.cast(u32, version) orelse return error.InvalidTransactionSessionRecord;
        }
    }
    if (value.object.get("relational_schema_versions")) |versions| {
        if (versions != .object) return error.InvalidTransactionSessionRecord;
        var entries = versions.object.iterator();
        while (entries.next()) |entry| {
            const table = for (request.tables) |*table| {
                if (std.mem.eql(u8, table.table_name, entry.key_ptr.*)) break table;
            } else return error.InvalidTransactionSessionRecord;
            const version: u64 = switch (entry.value_ptr.*) {
                .integer => |number| try nonNegativeRecordInteger(number),
                .number_string => |number| try recordNumber(number),
                else => return error.InvalidTransactionSessionRecord,
            };
            table.relational_schema_version = std.math.cast(u32, version) orelse return error.InvalidTransactionSessionRecord;
        }
    }
    if (value.object.get("document_revision_handoffs")) |revisions| {
        if (revisions != .object) return error.InvalidTransactionSessionRecord;
        var entries = revisions.object.iterator();
        while (entries.next()) |entry| {
            const table = for (request.tables) |*table| {
                if (std.mem.eql(u8, table.table_name, entry.key_ptr.*)) break table;
            } else return error.InvalidTransactionSessionRecord;
            var parsed = try std.json.parseFromValue([]const @import("sql/execution/document_revision_custody.zig").Handoff, alloc, entry.value_ptr.*, .{ .allocate = .alloc_always });
            defer parsed.deinit();
            try table.mergeDocumentRevisions(alloc, parsed.value);
        }
    }
    if (value.object.get("trigger_receipts")) |receipts| {
        if (receipts != .object) return error.InvalidTransactionSessionRecord;
        var entries = receipts.object.iterator();
        while (entries.next()) |entry| {
            const table = for (request.tables) |*table| {
                if (std.mem.eql(u8, table.table_name, entry.key_ptr.*)) break table;
            } else return error.InvalidTransactionSessionRecord;
            var parsed = try std.json.parseFromValue([]const @import("antfly_local_sources").sql_catalog.trigger_handoff.Receipt, alloc, entry.value_ptr.*, .{ .allocate = .alloc_always });
            defer parsed.deinit();
            try table.mergeTriggerReceipts(alloc, parsed.value);
        }
    }
    if (value.object.get("conflict_guards")) |guards| {
        if (guards != .object) return error.InvalidTransactionSessionRecord;
        var entries = guards.object.iterator();
        while (entries.next()) |entry| {
            const table = for (request.tables) |*table| {
                if (std.mem.eql(u8, table.table_name, entry.key_ptr.*)) break table;
            } else return error.InvalidTransactionSessionRecord;
            var parsed = try std.json.parseFromValue(TableCommitRequest.ConflictGuards, alloc, entry.value_ptr.*, .{ .allocate = .alloc_always });
            defer parsed.deinit();
            try table.mergeConflictGuards(alloc, parsed.value);
        }
    }
    if (value.object.get("range_guards")) |guards| {
        if (guards != .object) return error.InvalidTransactionSessionRecord;
        var entries = guards.object.iterator();
        while (entries.next()) |entry| {
            const table = for (request.tables) |*table| {
                if (std.mem.eql(u8, table.table_name, entry.key_ptr.*)) break table;
            } else return error.InvalidTransactionSessionRecord;
            var parsed = try std.json.parseFromValue([]const @import("antfly_local_sources").api_range_read_guards.OwnerRangeProof, alloc, entry.value_ptr.*, .{ .allocate = .alloc_always });
            defer parsed.deinit();
            try table.mergeRangeGuards(alloc, parsed.value);
        }
    }
    if (value.object.get("observed_predicates")) |observations| {
        if (observations != .object) return error.InvalidTransactionSessionRecord;
        var entries = observations.object.iterator();
        while (entries.next()) |entry| {
            const table = for (request.tables) |*table| {
                if (std.mem.eql(u8, table.table_name, entry.key_ptr.*)) break table;
            } else return error.InvalidTransactionSessionRecord;
            const Stored = struct { key: []const u8, version: []const u8, digest: ?[32]u8, mutation_revision: ?[]const u8 = null, unique_absence: bool = false, row_policy_requirement: @import("../usermgr/mod.zig").row_policy_authority.MutationRequirement = .native };
            var parsed = try std.json.parseFromValue([]const Stored, alloc, entry.value_ptr.*, .{});
            defer parsed.deinit();
            const predicates = try alloc.alloc(db_mod.types.TransactionVersionPredicate, parsed.value.len);
            defer alloc.free(predicates);
            for (predicates, parsed.value) |*predicate, stored| predicate.* = .{
                .key = stored.key,
                .expected_version = try parseVersionString(stored.version),
                .expected_content_digest = stored.digest,
                .expected_mutation_revision = if (stored.mutation_revision) |revision| try parseVersionString(revision) else null,
                .unique_absence = stored.unique_absence,
                .row_policy_requirement = stored.row_policy_requirement,
            };
            try appendPredicates(alloc, &table.predicates, predicates);
        }
    }
    return request;
}

fn parseCommitValue(alloc: std.mem.Allocator, value: std.json.Value) !OwnedTransactionCommitRequest {
    const root = switch (value) {
        .object => |obj| obj,
        else => return error.InvalidTransactionCommitRequest,
    };

    const default_requirement = if (root.get("commit_requires")) |requirement_value| try db_mod.types.parseCommitRequires(requirement_value) else null;

    var req: OwnedTransactionCommitRequest = .{};
    errdefer req.deinit(alloc);

    const read_set_value = root.get("read_set") orelse return error.InvalidTransactionCommitRequest;
    req.read_set = try parseReadSet(alloc, read_set_value);

    const tables_value = root.get("tables") orelse return error.InvalidTransactionCommitRequest;
    req.tables = try parseTables(alloc, tables_value);
    req.commit_requires = default_requirement;
    req.retainCommitRequires();

    if (root.get("sync_level")) |sync_level_value| {
        req.sync_level = parseSyncLevel(sync_level_value) orelse return error.InvalidTransactionCommitRequest;
    }

    try applyReadSetPredicates(alloc, &req);
    return req;
}

fn parseTables(alloc: std.mem.Allocator, value: std.json.Value) ![]TableCommitRequest {
    const obj = switch (value) {
        .object => |obj| obj,
        else => return error.InvalidTransactionCommitRequest,
    };
    var out = try alloc.alloc(TableCommitRequest, obj.count());
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*item| item.deinit(alloc);
        alloc.free(out);
    }
    var it = obj.iterator();
    while (it.next()) |entry| {
        const table_name = try alloc.dupe(u8, entry.key_ptr.*);
        errdefer alloc.free(table_name);
        out[initialized] = .{
            .table_name = table_name,
            .batch = try parseTableBatch(alloc, entry.value_ptr.*),
        };
        initialized += 1;
    }
    return out;
}

fn encodeTableBatchRequest(alloc: std.mem.Allocator, table: TableCommitRequest) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.append(alloc, '{');
    var first = true;
    if (table.batch.writes.len > 0) {
        first = false;
        try out.appendSlice(alloc, "\"inserts\":{");
        for (table.batch.writes, 0..) |write, i| {
            if (i > 0) try out.append(alloc, ',');
            try appendJsonString(alloc, &out, write.key);
            try out.append(alloc, ':');
            try out.appendSlice(alloc, write.value);
        }
        try out.append(alloc, '}');
    }
    if (table.batch.deletes.len > 0) {
        if (!first) try out.append(alloc, ',');
        first = false;
        try out.appendSlice(alloc, "\"deletes\":[");
        for (table.batch.deletes, 0..) |key, i| {
            if (i > 0) try out.append(alloc, ',');
            try appendJsonString(alloc, &out, key);
        }
        try out.append(alloc, ']');
    }
    if (table.batch.transforms.len > 0) {
        if (!first) try out.append(alloc, ',');
        first = false;
        try out.appendSlice(alloc, "\"transforms\":[");
        for (table.batch.transforms, 0..) |transform, i| {
            if (i > 0) try out.append(alloc, ',');
            try out.appendSlice(alloc, "{\"key\":");
            try appendJsonString(alloc, &out, transform.key);
            try out.appendSlice(alloc, ",\"operations\":[");
            for (transform.operations, 0..) |op, op_index| {
                if (op_index > 0) try out.append(alloc, ',');
                try out.appendSlice(alloc, "{\"op\":");
                try appendJsonString(alloc, &out, db_mod.types.transformOpText(op.op));
                try out.appendSlice(alloc, ",\"path\":");
                try appendJsonString(alloc, &out, op.path);
                if (op.value_json) |value_json| {
                    try out.appendSlice(alloc, ",\"value\":");
                    try out.appendSlice(alloc, value_json);
                }
                try out.append(alloc, '}');
            }
            try out.append(alloc, ']');
            if (transform.upsert) try out.appendSlice(alloc, ",\"upsert\":true");
            try out.append(alloc, '}');
        }
        try out.append(alloc, ']');
    }
    inline for (.{ "graph_writes", "graph_deletes" }) |field| {
        const effects = @field(table.batch, field);
        if (effects.len != 0) {
            if (!first) try out.append(alloc, ',');
            first = false;
            try appendJsonString(alloc, &out, field);
            try out.append(alloc, ':');
            const Wire = struct {
                items: @TypeOf(effects),
                pub fn jsonStringify(self: @This(), stream: anytype) !void {
                    try stream.beginArray();
                    for (self.items) |edge| {
                        try stream.beginObject();
                        inline for (@typeInfo(@TypeOf(edge)).@"struct".field_names) |name| {
                            const omitted = if (comptime std.mem.eql(u8, name, "edge_id") or std.mem.eql(u8, name, "owner_document") or std.mem.eql(u8, name, "owner")) @field(edge, name).len == 0 else false;
                            if (!omitted) {
                                try stream.objectField(name);
                                try stream.write(@field(edge, name));
                            }
                        }
                        try stream.endObject();
                    }
                    try stream.endArray();
                }
            };
            const encoded = try std.json.Stringify.valueAlloc(alloc, Wire{ .items = effects }, .{});
            defer alloc.free(encoded);
            try out.appendSlice(alloc, encoded);
        }
    }
    if (table.batch.req.commit_requires) |requirement| {
        if (!first) try out.append(alloc, ',');
        try out.print(alloc, "\"commit_requires\":\"{s}\"", .{@tagName(requirement)});
    }
    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

fn ownedRequestFromStageRead(alloc: std.mem.Allocator, req: StageReadRequest) !OwnedTransactionCommitRequest {
    var out: OwnedTransactionCommitRequest = .{};
    errdefer out.deinit(alloc);
    out.read_set = try alloc.alloc(TransactionReadItem, 1);
    out.read_set[0] = .{
        .table_name = try alloc.dupe(u8, req.table_name),
        .key = try alloc.dupe(u8, req.key),
        .expected_version = req.version,
    };
    out.tables = try alloc.alloc(TableCommitRequest, 1);
    out.tables[0] = .{
        .table_name = try alloc.dupe(u8, req.table_name),
    };
    try applyReadSetPredicates(alloc, &out);
    return out;
}

fn ownedRequestFromStageWrite(alloc: std.mem.Allocator, req: StageWriteRequest) !OwnedTransactionCommitRequest {
    var out: OwnedTransactionCommitRequest = .{};
    errdefer out.deinit(alloc);
    out.tables = try alloc.alloc(TableCommitRequest, 1);
    out.tables[0] = .{
        .table_name = try alloc.dupe(u8, req.table_name),
        .batch = .{
            .writes = try alloc.alloc(db_mod.types.BatchWrite, 1),
        },
    };
    out.tables[0].batch.writes[0] = .{
        .key = try alloc.dupe(u8, req.key),
        .value = try alloc.dupe(u8, req.value_json),
    };
    syncBatchReq(&out.tables[0].batch);
    return out;
}

fn ownedRequestFromStageDelete(alloc: std.mem.Allocator, req: StageDeleteRequest) !OwnedTransactionCommitRequest {
    var out: OwnedTransactionCommitRequest = .{};
    errdefer out.deinit(alloc);
    out.tables = try alloc.alloc(TableCommitRequest, 1);
    out.tables[0] = .{
        .table_name = try alloc.dupe(u8, req.table_name),
        .batch = .{
            .deletes = try alloc.alloc([]const u8, 1),
        },
    };
    out.tables[0].batch.deletes[0] = try alloc.dupe(u8, req.key);
    syncBatchReq(&out.tables[0].batch);
    return out;
}

fn classifyConflictKind(message: []const u8) CommitConflictKind {
    if (std.mem.eql(u8, message, "version conflict")) return .version_conflict;
    if (std.mem.eql(u8, message, "intent conflict")) return .intent_conflict;
    if (isTopologyChangedConflictMessage(message)) return .topology_changed;
    if (std.mem.eql(u8, message, "participant unavailable")) return .participant_unavailable;
    if (std.mem.eql(u8, message, "doc identity unavailable")) return .doc_identity_unavailable;
    if (std.mem.eql(u8, message, "session lease lost")) return .session_lease_lost;
    if (std.mem.eql(u8, message, "torn transaction state")) return .torn_state;
    return .transaction_conflict;
}

fn isRetryableConflict(message: []const u8) bool {
    return isTopologyChangedConflictMessage(message) or
        std.mem.eql(u8, message, "participant unavailable") or
        std.mem.eql(u8, message, "doc identity unavailable") or
        std.mem.eql(u8, message, "session lease lost");
}

fn retryAfterMsForKind(kind: CommitConflictKind) ?u32 {
    return switch (kind) {
        .topology_changed => 100,
        .participant_unavailable => 50,
        .doc_identity_unavailable => 100,
        .session_lease_lost => 25,
        else => null,
    };
}

fn retryScopeForKind(kind: CommitConflictKind) ?[]const u8 {
    return switch (kind) {
        .topology_changed => "topology",
        .participant_unavailable => "participant",
        .doc_identity_unavailable => "doc_identity",
        .session_lease_lost => "session",
        else => null,
    };
}

fn conflictKindText(kind: CommitConflictKind) []const u8 {
    return switch (kind) {
        .version_conflict => "version_conflict",
        .intent_conflict => "intent_conflict",
        .topology_changed => "topology_changed",
        .participant_unavailable => "participant_unavailable",
        .doc_identity_unavailable => "doc_identity_unavailable",
        .session_lease_lost => "session_lease_lost",
        .transaction_conflict => "transaction_conflict",
        .torn_state => "torn_state",
    };
}

fn participantPhaseText(phase: distributed_txn.ParticipantPhase) []const u8 {
    return switch (phase) {
        .begin => "begin",
        .prepare => "prepare",
        .resolve => "resolve",
    };
}

fn cloneBatchRequest(alloc: std.mem.Allocator, batch: batch_api.OwnedBatchRequest) !batch_api.OwnedBatchRequest {
    var out: batch_api.OwnedBatchRequest = .{};
    out.req.commit_requires = batch.req.commit_requires;
    out.req.sync_level = batch.req.sync_level;
    errdefer out.deinit(alloc);
    try appendBatchWrites(alloc, &out, batch.writes);
    try appendBatchDeletes(alloc, &out, batch.deletes);
    try appendBatchTransforms(alloc, &out, batch.transforms);
    try @import("transaction_graph_effects.zig").merge(alloc, &out, batch.graph_writes, batch.graph_deletes);
    syncBatchReq(&out);
    return out;
}

fn syncBatchReq(batch: *batch_api.OwnedBatchRequest) void {
    const requirement = batch.req.commit_requires;
    const sync_level = batch.req.sync_level;
    batch.req = .{
        .commit_requires = requirement,
        .sync_level = sync_level,
        .writes = batch.writes,
        .deletes = batch.deletes,
        .transforms = batch.transforms,
        .graph_writes = batch.graph_writes,
        .graph_deletes = batch.graph_deletes,
    };
}

fn clonePredicatesInto(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(db_mod.types.TransactionVersionPredicate),
    predicates: []const db_mod.types.TransactionVersionPredicate,
) !void {
    try out.ensureTotalCapacity(alloc, predicates.len);
    for (predicates) |predicate| {
        out.appendAssumeCapacity(.{
            .key = try alloc.dupe(u8, predicate.key),
            .expected_version = predicate.expected_version,
            .expected_content_digest = predicate.expected_content_digest,
            .expected_mutation_revision = predicate.expected_mutation_revision,
            .unique_absence = predicate.unique_absence,
            .row_policy_requirement = predicate.row_policy_requirement,
        });
    }
}

fn appendReadSet(
    alloc: std.mem.Allocator,
    req: *OwnedTransactionCommitRequest,
    items: []const TransactionReadItem,
) !void {
    if (items.len == 0) return;
    var extra: usize = 0;
    for (items) |item| {
        if (findReadSetIndex(req.read_set, item.table_name, item.key) == null) extra += 1;
    }
    const old_len = req.read_set.len;
    var next = try alloc.alloc(TransactionReadItem, old_len + extra);
    var copied: usize = 0;
    errdefer {
        for (next[0..copied]) |*item| item.deinit(alloc);
        alloc.free(next);
    }
    for (req.read_set) |item| {
        next[copied] = try item.clone(alloc);
        copied += 1;
    }
    for (items) |item| {
        if (findReadSetIndex(next[0..copied], item.table_name, item.key)) |idx| {
            next[idx].expected_version = item.expected_version;
        } else {
            next[copied] = try item.clone(alloc);
            copied += 1;
        }
    }
    for (req.read_set) |*item| item.deinit(alloc);
    if (req.read_set.len > 0) alloc.free(req.read_set);
    req.read_set = next;
}

fn findReadSetIndex(items: []const TransactionReadItem, table_name: []const u8, key: []const u8) ?usize {
    for (items, 0..) |item, i| {
        if (std.mem.eql(u8, item.table_name, table_name) and std.mem.eql(u8, item.key, key)) return i;
    }
    return null;
}

fn readSnapshotMapKey(alloc: std.mem.Allocator, table_name: []const u8, key: []const u8) ![]u8 {
    return try tupleMapKeyAlloc(alloc, &.{ table_name, key });
}

fn tupleMapKeyAlloc(alloc: std.mem.Allocator, components: []const []const u8) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);

    for (components) |component| {
        if (component.len > std.math.maxInt(u32)) return error.KeyComponentTooLarge;
        var len_buf: [@sizeOf(u32)]u8 = undefined;
        std.mem.writeInt(u32, &len_buf, @intCast(component.len), .big);
        try out.appendSlice(alloc, &len_buf);
        try out.appendSlice(alloc, component);
    }

    return try out.toOwnedSlice(alloc);
}

test "transaction read snapshot map keys preserve embedded delimiters" {
    const alloc = std.testing.allocator;

    const left = try readSnapshotMapKey(alloc, "docs\x00a", "key");
    defer alloc.free(left);
    const right = try readSnapshotMapKey(alloc, "docs", "a\x00key");
    defer alloc.free(right);

    try std.testing.expect(!std.mem.eql(u8, left, right));
}

fn deinitReadSnapshotMap(alloc: std.mem.Allocator, map: *std.StringArrayHashMapUnmanaged(SessionReadSnapshot)) void {
    var it = map.iterator();
    while (it.next()) |entry| {
        alloc.free(@constCast(entry.key_ptr.*));
        entry.value_ptr.deinit(alloc);
    }
    map.deinit(alloc);
    map.* = .empty;
}

fn cloneReadSnapshotMap(
    alloc: std.mem.Allocator,
    map: std.StringArrayHashMapUnmanaged(SessionReadSnapshot),
) !std.StringArrayHashMapUnmanaged(SessionReadSnapshot) {
    var out: std.StringArrayHashMapUnmanaged(SessionReadSnapshot) = .empty;
    errdefer deinitReadSnapshotMap(alloc, &out);
    var it = map.iterator();
    while (it.next()) |entry| {
        const owned_key = try alloc.dupe(u8, entry.key_ptr.*);
        errdefer alloc.free(owned_key);
        const snapshot = try entry.value_ptr.clone(alloc);
        try out.put(alloc, owned_key, snapshot);
    }
    return out;
}

fn cloneReadSnapshotForKey(
    alloc: std.mem.Allocator,
    map: *const std.StringArrayHashMapUnmanaged(SessionReadSnapshot),
    table_name: []const u8,
    key: []const u8,
) !?SessionReadSnapshot {
    const map_key = try readSnapshotMapKey(alloc, table_name, key);
    defer alloc.free(map_key);
    const snapshot = map.get(map_key) orelse return null;
    return try snapshot.clone(alloc);
}

fn upsertReadSnapshot(
    alloc: std.mem.Allocator,
    map: *std.StringArrayHashMapUnmanaged(SessionReadSnapshot),
    snapshot: StageReadSnapshot,
) !void {
    const map_key = try readSnapshotMapKey(alloc, snapshot.table_name, snapshot.key);
    if (map.getPtr(map_key)) |existing| {
        defer alloc.free(map_key);
        if (existing.version == snapshot.version) return;
        const replacement = try ownReadSnapshot(alloc, snapshot);
        const previous = existing.*;
        existing.* = replacement;
        var old = previous;
        old.deinit(alloc);
        return;
    }

    errdefer alloc.free(map_key);
    var owned = try ownReadSnapshot(alloc, snapshot);
    errdefer owned.deinit(alloc);
    try map.putNoClobber(alloc, map_key, owned);
}

fn ownReadSnapshot(alloc: std.mem.Allocator, snapshot: StageReadSnapshot) !SessionReadSnapshot {
    const table_name = try alloc.dupe(u8, snapshot.table_name);
    errdefer alloc.free(table_name);
    const key = try alloc.dupe(u8, snapshot.key);
    errdefer alloc.free(key);
    const document_json = if (snapshot.document_json) |json| try alloc.dupe(u8, json) else null;
    return .{
        .table_name = table_name,
        .key = key,
        .version = snapshot.version,
        .document_json = document_json,
    };
}

fn appendReadSnapshotJson(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    snapshot: SessionReadSnapshot,
) !void {
    try out.appendSlice(alloc, "{\"table\":");
    try appendJsonString(alloc, out, snapshot.table_name);
    try out.appendSlice(alloc, ",\"key\":");
    try appendJsonString(alloc, out, snapshot.key);
    try out.appendSlice(alloc, ",\"version\":");
    try out.print(alloc, "{d}", .{snapshot.version});
    try out.appendSlice(alloc, ",\"document\":");
    if (snapshot.document_json) |document_json| {
        try out.appendSlice(alloc, document_json);
    } else {
        try out.appendSlice(alloc, "null");
    }
    try out.append(alloc, '}');
}

fn decodeReadSnapshotsInto(
    alloc: std.mem.Allocator,
    value: std.json.Value,
    map: *std.StringArrayHashMapUnmanaged(SessionReadSnapshot),
) !void {
    const arr = switch (value) {
        .array => |arr| arr,
        else => return error.InvalidTransactionSessionRecord,
    };
    for (arr.items) |entry| {
        const obj = switch (entry) {
            .object => |obj| obj,
            else => return error.InvalidTransactionSessionRecord,
        };
        const table_name = requireString(obj, "table");
        const key = requireString(obj, "key");
        if (table_name.len == 0 or key.len == 0) return error.InvalidTransactionSessionRecord;
        const version = switch (obj.get("version") orelse return error.InvalidTransactionSessionRecord) {
            .integer => |v| try nonNegativeRecordInteger(v),
            .number_string => |text| try recordNumber(text),
            .string => |s| try parseVersionString(s),
            else => return error.InvalidTransactionSessionRecord,
        };
        const document_json = if (obj.get("document")) |document| switch (document) {
            .null => null,
            else => try std.json.Stringify.valueAlloc(alloc, document, .{}),
        } else null;
        defer if (document_json) |json| alloc.free(json);
        try upsertReadSnapshot(alloc, map, .{
            .table_name = table_name,
            .key = key,
            .version = version,
            .document_json = document_json,
        });
    }
}

fn appendTable(
    alloc: std.mem.Allocator,
    req: *OwnedTransactionCommitRequest,
    table: TableCommitRequest,
) !void {
    const old_len = req.tables.len;
    var next = try alloc.alloc(TableCommitRequest, old_len + 1);
    var copied: usize = 0;
    errdefer {
        for (next[0..copied]) |*entry| entry.deinit(alloc);
        alloc.free(next);
    }
    for (req.tables) |entry| {
        next[copied] = try entry.clone(alloc);
        copied += 1;
    }
    next[copied] = try table.clone(alloc);
    copied += 1;
    for (req.tables) |*entry| entry.deinit(alloc);
    if (req.tables.len > 0) alloc.free(req.tables);
    req.tables = next;
}

fn cloneJsonNullFields(alloc: std.mem.Allocator, fields: []const []const u8) ![]const []const u8 {
    if (fields.len == 0) return &.{};
    const copy = try alloc.alloc([]const u8, fields.len);
    var count: usize = 0;
    errdefer {
        for (copy[0..count]) |field| alloc.free(field);
        alloc.free(copy);
    }
    for (fields, copy) |field, *out| {
        out.* = try alloc.dupe(u8, field);
        count += 1;
    }
    return copy;
}

fn freeJsonNullFields(alloc: std.mem.Allocator, fields: []const []const u8) void {
    for (fields) |field| alloc.free(field);
    if (fields.len != 0) alloc.free(fields);
}

fn cloneBatchWrite(alloc: std.mem.Allocator, write: db_mod.types.BatchWrite) !db_mod.types.BatchWrite {
    const key = try alloc.dupe(u8, write.key);
    errdefer alloc.free(key);
    const value = try alloc.dupe(u8, write.value);
    errdefer alloc.free(value);
    return .{ .key = key, .value = value, .json_null_fields = try cloneJsonNullFields(alloc, write.json_null_fields), .prepared_domain = write.prepared_domain };
}

fn appendBatchWrites(alloc: std.mem.Allocator, batch: *batch_api.OwnedBatchRequest, writes: []const db_mod.types.BatchWrite) !void {
    if (writes.len == 0) return;
    // Fold before cloning: bounded staging retains one image per key, and a
    // raw overwrite cannot inherit an older row's preparation authority.
    var latest: std.StringArrayHashMapUnmanaged(db_mod.types.BatchWrite) = .empty;
    defer latest.deinit(alloc);
    for (batch.writes) |write| try latest.put(alloc, write.key, write);
    for (writes) |write| try latest.put(alloc, write.key, write);
    const next = try alloc.alloc(db_mod.types.BatchWrite, latest.count());
    var copied: usize = 0;
    errdefer {
        for (next[0..copied]) |write| {
            alloc.free(write.key);
            alloc.free(write.value);
            freeJsonNullFields(alloc, write.json_null_fields);
        }
        alloc.free(next);
    }
    for (latest.values(), next) |write, *out| {
        out.* = try cloneBatchWrite(alloc, write);
        copied += 1;
    }
    const owner = try batch_api.DomainHandoff.cloneInto(alloc, next, next);
    for (batch.writes) |write| {
        alloc.free(write.key);
        alloc.free(write.value);
        freeJsonNullFields(alloc, write.json_null_fields);
    }
    if (batch.writes.len > 0) alloc.free(batch.writes);
    if (batch.prepared_domain_rows) |*old| old.deinit();
    batch.writes = next;
    batch.prepared_domain_rows = owner;
}

fn appendBatchDeletes(alloc: std.mem.Allocator, batch: *batch_api.OwnedBatchRequest, deletes: []const []const u8) !void {
    if (deletes.len == 0) return;
    const old_len = batch.deletes.len;
    var next = try alloc.alloc([]const u8, old_len + deletes.len);
    var copied: usize = 0;
    errdefer {
        for (next[0..copied]) |key| alloc.free(key);
        alloc.free(next);
    }
    for (batch.deletes) |key| {
        next[copied] = try alloc.dupe(u8, key);
        copied += 1;
    }
    for (deletes) |key| {
        next[copied] = try alloc.dupe(u8, key);
        copied += 1;
    }
    for (batch.deletes) |key| alloc.free(key);
    if (batch.deletes.len > 0) alloc.free(batch.deletes);
    batch.deletes = next;
}

fn cloneTransform(alloc: std.mem.Allocator, transform: db_mod.types.DocumentTransform) !db_mod.types.DocumentTransform {
    const key = try alloc.dupe(u8, transform.key);
    errdefer alloc.free(key);
    const ops = try alloc.alloc(db_mod.types.TransformOp, transform.operations.len);
    var initialized: usize = 0;
    errdefer {
        for (ops[0..initialized]) |op| {
            alloc.free(op.path);
            if (op.value_json) |value| alloc.free(value);
        }
        alloc.free(ops);
    }
    for (transform.operations, 0..) |op, i| {
        const path = try alloc.dupe(u8, op.path);
        errdefer alloc.free(path);
        ops[i] = .{ .op = op.op, .path = path, .value_json = if (op.value_json) |value| try alloc.dupe(u8, value) else null };
        initialized += 1;
    }
    return .{ .key = key, .operations = ops, .upsert = transform.upsert };
}

fn freeTransform(alloc: std.mem.Allocator, transform: db_mod.types.DocumentTransform) void {
    alloc.free(transform.key);
    for (transform.operations) |op| {
        alloc.free(op.path);
        if (op.value_json) |value| alloc.free(value);
    }
    alloc.free(transform.operations);
}

fn appendBatchTransforms(alloc: std.mem.Allocator, batch: *batch_api.OwnedBatchRequest, transforms: []const db_mod.types.DocumentTransform) !void {
    if (transforms.len == 0) return;
    const next = try alloc.alloc(db_mod.types.DocumentTransform, batch.transforms.len + transforms.len);
    var copied: usize = 0;
    errdefer {
        for (next[0..copied]) |transform| freeTransform(alloc, transform);
        alloc.free(next);
    }
    for (batch.transforms) |transform| {
        next[copied] = try cloneTransform(alloc, transform);
        copied += 1;
    }
    for (transforms) |transform| {
        next[copied] = try cloneTransform(alloc, transform);
        copied += 1;
    }
    for (batch.transforms) |transform| freeTransform(alloc, transform);
    alloc.free(batch.transforms);
    batch.transforms = next;
}

fn appendPredicates(
    alloc: std.mem.Allocator,
    predicates: *std.ArrayListUnmanaged(db_mod.types.TransactionVersionPredicate),
    extras: []const db_mod.types.TransactionVersionPredicate,
) !void {
    if (extras.len == 0) return;
    // One dependency per key, independent of statement count. Never replace an
    // earlier observation with a newer one: that would bless a lost update.
    var by_key = std.StringHashMapUnmanaged(usize).empty;
    defer by_key.deinit(alloc);
    try by_key.ensureTotalCapacity(alloc, @intCast(predicates.items.len + extras.len));
    for (predicates.items, 0..) |predicate, i| by_key.putAssumeCapacity(predicate.key, i);
    try predicates.ensureTotalCapacity(alloc, predicates.items.len + extras.len);
    for (extras) |predicate| {
        if (by_key.get(predicate.key)) |index| {
            const previous = &predicates.items[index];
            if (previous.expected_version != predicate.expected_version) return error.VersionConflict;
            if (previous.expected_content_digest) |digest| {
                if (predicate.expected_content_digest) |next| if (!std.mem.eql(u8, &digest, &next)) return error.VersionConflict;
            } else previous.expected_content_digest = predicate.expected_content_digest;
            if (previous.expected_mutation_revision) |revision| {
                if (predicate.expected_mutation_revision) |next| if (revision != next) return error.VersionConflict;
            } else previous.expected_mutation_revision = predicate.expected_mutation_revision;
            previous.unique_absence = previous.unique_absence or predicate.unique_absence;
            if (predicate.row_policy_requirement != .native)
                previous.row_policy_requirement = predicate.row_policy_requirement;
            continue;
        }
        predicates.appendAssumeCapacity(.{
            .key = try alloc.dupe(u8, predicate.key),
            .expected_version = predicate.expected_version,
            .expected_content_digest = predicate.expected_content_digest,
            .expected_mutation_revision = predicate.expected_mutation_revision,
            .unique_absence = predicate.unique_absence,
            .row_policy_requirement = predicate.row_policy_requirement,
        });
        by_key.putAssumeCapacity(predicates.items[predicates.items.len - 1].key, predicates.items.len - 1);
    }
}

fn syncTableBatch(table: *TableCommitRequest) void {
    syncBatchReq(&table.batch);
}

pub fn isEmptySessionCommitBody(body: []const u8) bool {
    const trimmed = std.mem.trim(u8, body, &std.ascii.whitespace);
    return trimmed.len == 0 or std.mem.eql(u8, trimmed, "{}");
}

fn parseTableBatch(alloc: std.mem.Allocator, value: std.json.Value) !batch_api.OwnedBatchRequest {
    var owned = try batch_api.public_codec.parseBatchValue(alloc, value);
    errdefer owned.deinit(alloc);
    return batch_api.takePublic(&owned);
}

fn applyReadSetPredicates(alloc: std.mem.Allocator, req: *OwnedTransactionCommitRequest) !void {
    for (req.read_set) |item| {
        const table = try ensureTableCommit(alloc, &req.tables, item.table_name);
        try table.predicates.append(alloc, .{
            .key = try alloc.dupe(u8, item.key),
            .expected_version = item.expected_version,
        });
    }
}

fn ensureTableCommit(
    alloc: std.mem.Allocator,
    tables: *[]TableCommitRequest,
    table_name: []const u8,
) !*TableCommitRequest {
    for (tables.*) |*table| {
        if (std.mem.eql(u8, table.table_name, table_name)) return table;
    }
    const old = tables.*;
    var next = try alloc.alloc(TableCommitRequest, old.len + 1);
    @memcpy(next[0..old.len], old);
    next[old.len] = .{ .table_name = try alloc.dupe(u8, table_name) };
    if (old.len > 0) alloc.free(old);
    tables.* = next;
    return &tables.*[tables.*.len - 1];
}

fn parseSyncLevel(value: std.json.Value) ?db_mod.types.SyncLevel {
    return db_mod.types.parsePublicSyncLevelJson(value);
}

fn syncLevelText(level: db_mod.types.SyncLevel) []const u8 {
    return db_mod.types.publicSyncLevelText(level);
}

fn nextTxnTimestamp() u64 {
    // Session timestamps are persisted and compared against recovery/cleanup
    // cutoffs, so they must stay on the realtime clock.
    return platform_time.realtimeNs();
}

fn touchSession(session: *Session) void {
    session.last_touched_timestamp = @max(session.last_touched_timestamp +| 1, nextTxnTimestamp());
}

const StagedCounts = struct { tables: usize = 0, reads: usize = 0, writes: usize = 0, deletes: usize = 0 };

fn stagedCounts(session: *const Session) StagedCounts {
    var counts: StagedCounts = .{};
    if (session.staged) |req| {
        counts.tables = req.tables.len;
        counts.reads = req.read_set.len;
        for (req.tables) |*table| {
            counts.writes += table.writeCount();
            counts.deletes += table.deleteCount();
        }
    }
    for (session.graph_abort_tables) |table| {
        counts.tables += 1;
        counts.reads += table.staged_read_count;
        counts.writes += table.staged_write_count;
        counts.deletes += table.staged_delete_count;
    }
    return counts;
}

const SessionLeaseState = enum {
    none,
    held,
    expired,
};

fn sessionLeaseState(lease_expires_at: u64, now_ns: u64) SessionLeaseState {
    if (lease_expires_at == 0) return .none;
    if (lease_expires_at <= now_ns) return .expired;
    return .held;
}

fn leaseExpiryNs(expires_at_ms: u64) u64 {
    return std.math.mul(u64, expires_at_ms, std.time.ns_per_ms) catch std.math.maxInt(u64);
}

fn sessionStatusFromSession(self: *SessionRegistry, alloc: std.mem.Allocator, session: *const Session) !SessionStatus {
    var status = sessionStatusSnapshot(session, self.durable != null);
    status.lease_expires_at = try self.loadLeaseExpiryLocked(alloc, session.txn_id);
    applySavepointLimits(self, &status);
    return status;
}

fn applySavepointLimits(self: *const SessionRegistry, status: *SessionStatus) void {
    status.savepoint_limit = self.max_savepoints;
    status.remaining_savepoints = if (self.max_savepoints) |limit| limit - @min(limit, status.savepoint_count) else null;
}

fn sessionStatusSnapshot(session: *const Session, durable: bool) SessionStatus {
    const counts = stagedCounts(session);
    const savepoint_count = session.savepoints.count();
    return .{
        .txn_id = session.txn_id,
        .owner_node_id = session.owner_node_id,
        .begin_timestamp = session.begin_timestamp,
        .last_touched_timestamp = session.last_touched_timestamp,
        .lease_expires_at = 0,
        .sync_level = session.sync_level,
        .disposition = sessionDisposition(session.*),
        .staged_table_count = counts.tables,
        .staged_read_count = counts.reads,
        .staged_write_count = counts.writes,
        .staged_delete_count = counts.deletes,
        .read_snapshot_count = session.read_snapshots.count(),
        .savepoint_count = savepoint_count,
        .durable = durable,
    };
}

fn sessionDisposition(session: Session) SessionDisposition {
    if (session.external_preparation_prepared) return .prepared;
    if (session.terminal_abort) return .aborted;
    if (session.terminal_commit) |terminal| {
        if (session.graph_receipt and terminal.status == .committed and terminal.coordinator_group_id != null and !terminal.coordinator_acknowledged)
            return .committed_pending;
        if (terminal.repair_required) return .committed_repair_required;
        return if (terminal.status == .committed) .committed else .committed_pending;
    }
    return if (session.commit_execution_started) .outcome_unknown else .active;
}

fn sessionReadSnapshots(alloc: std.mem.Allocator, session: *const Session, sorted: bool) ![]SessionReadSnapshot {
    var out = try alloc.alloc(SessionReadSnapshot, session.read_snapshots.count());
    var i: usize = 0;
    var it = session.read_snapshots.iterator();
    errdefer {
        for (out[0..i]) |*snapshot| snapshot.deinit(alloc);
        if (out.len > 0) alloc.free(out);
    }
    while (it.next()) |entry| {
        out[i] = try entry.value_ptr.clone(alloc);
        i += 1;
    }
    if (sorted) std.sort.pdq(SessionReadSnapshot, out, {}, struct {
        fn lessThan(_: void, a: SessionReadSnapshot, b: SessionReadSnapshot) bool {
            if (std.mem.order(u8, a.table_name, b.table_name) == .lt) return true;
            if (std.mem.eql(u8, a.table_name, b.table_name)) return std.mem.lessThan(u8, a.key, b.key);
            return false;
        }
    }.lessThan);
    return out;
}

fn cloneTableDetails(alloc: std.mem.Allocator, tables: []const SessionTableDetail) ![]SessionTableDetail {
    const result = try alloc.dupe(SessionTableDetail, tables);
    errdefer alloc.free(result);
    var initialized: usize = 0;
    errdefer for (result[0..initialized]) |*table| table.deinit(alloc);
    for (result) |*table| {
        const storage_name = table.storage_name;
        table.table_name = try alloc.dupe(u8, table.table_name);
        table.storage_name = null;
        initialized += 1;
        if (storage_name) |name| table.storage_name = try alloc.dupe(u8, name);
    }
    return result;
}

fn sessionTableDetails(alloc: std.mem.Allocator, staged: ?OwnedTransactionCommitRequest, sorted: bool) ![]SessionTableDetail {
    const req = staged orelse return &.{};
    var map = std.StringArrayHashMapUnmanaged(SessionTableDetail).empty;
    defer map.deinit(alloc);

    for (req.read_set) |read| {
        const gop = try map.getOrPut(alloc, read.table_name);
        if (!gop.found_existing) {
            gop.value_ptr.* = .{
                .table_name = read.table_name,
                .staged_read_count = 0,
                .staged_write_count = 0,
                .staged_delete_count = 0,
                .staged_predicate_count = 0,
            };
        }
        gop.value_ptr.staged_read_count += 1;
    }

    for (req.tables) |table| {
        const gop = try map.getOrPut(alloc, table.table_name);
        if (!gop.found_existing) {
            gop.value_ptr.* = .{
                .table_name = table.table_name,
                .staged_read_count = 0,
                .staged_write_count = 0,
                .staged_delete_count = 0,
                .staged_predicate_count = 0,
            };
        }
        gop.value_ptr.has_range_reads = gop.value_ptr.has_range_reads or table.range_guards != null;
        gop.value_ptr.staged_write_count += table.writeCount();
        gop.value_ptr.staged_delete_count += table.deleteCount();
        gop.value_ptr.staged_predicate_count += table.predicates.items.len;
    }

    const out = try cloneTableDetails(alloc, map.values());
    if (sorted) std.sort.pdq(SessionTableDetail, out, {}, struct {
        fn lessThan(_: void, a: SessionTableDetail, b: SessionTableDetail) bool {
            return std.mem.lessThan(u8, a.table_name, b.table_name);
        }
    }.lessThan);
    return out;
}

fn sessionSavepointIds(alloc: std.mem.Allocator, session: *const Session) ![]u64 {
    var out = try alloc.alloc(u64, session.savepoints.count());
    var i: usize = 0;
    var it = session.savepoints.iterator();
    while (it.next()) |entry| {
        out[i] = entry.key_ptr.*;
        i += 1;
    }
    std.sort.pdq(u64, out, {}, std.sort.asc(u64));
    return out;
}

fn makeSqlAbortReceiptKey(alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId) ![]u8 {
    return std.fmt.allocPrint(alloc, "\x00\x00__api_sql_abort_receipt__:{s}", .{&std.fmt.bytesToHex(txn_id, .lower)});
}

pub fn makeSessionKey(alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId) ![]u8 {
    const txn_hex = distributed_txn.encodeTxnIdHex(txn_id);
    return try std.fmt.allocPrint(alloc, "{s}{s}", .{ session_prefix, &txn_hex });
}

fn makeSessionLeaseKey(alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId) ![]u8 {
    const txn_hex = distributed_txn.encodeTxnIdHex(txn_id);
    return try std.fmt.allocPrint(alloc, "{s}{s}", .{ session_lease_prefix, &txn_hex });
}

fn makeSessionExpiryKey(alloc: std.mem.Allocator, timestamp: u64, txn_id: db_mod.types.TxnId) ![]u8 {
    return makeExpiryKey(alloc, session_expiry_prefix, timestamp, txn_id);
}

fn makeExpiryKey(alloc: std.mem.Allocator, prefix: []const u8, timestamp: u64, txn_id: db_mod.types.TxnId) ![]u8 {
    const txn_hex = distributed_txn.encodeTxnIdHex(txn_id);
    return try std.fmt.allocPrint(alloc, "{s}{x:0>16}:{s}", .{ prefix, timestamp, &txn_hex });
}

fn makeSessionRecoveryKey(alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId) ![]u8 {
    const txn_hex = distributed_txn.encodeTxnIdHex(txn_id);
    return try std.fmt.allocPrint(alloc, "{s}{s}", .{ session_recovery_prefix, &txn_hex });
}

fn sessionNeedsRecovery(session: Session) bool {
    if (session.external_preparation != null) return !session.external_receipt_acknowledged;
    if (session.terminal_abort) return false;
    if (session.terminal_commit) |terminal| {
        return terminal.status != .committed or
            (terminal.repair_required and terminal.coordinator_group_id == null) or
            (terminal.coordinator_group_id != null and !terminal.coordinator_acknowledged);
    }
    return session.commit_body_digest != null and session.commit_execution_started;
}

const ParsedSessionExpiryKey = struct {
    timestamp: u64,
    txn_id: db_mod.types.TxnId,
};

fn IndexPosition(comptime canonical_len: usize) type {
    return struct {
        const Self = @This();
        bytes: [canonical_len + 1]u8 = @splat(0),
        len: usize,
        exclusive: bool,

        fn after(key: []const u8, prefix_len: usize) ?Self {
            var position: Self = .{
                .len = @min(key.len, canonical_len + 1),
                .exclusive = key.len <= canonical_len,
            };
            @memcpy(position.bytes[0..position.len], key[0..position.len]);
            if (position.exclusive) return position;
            // All longer keys sharing this prefix are malformed. Jump past
            // that run without skipping any fixed-width marker.
            var end = position.len;
            while (end > prefix_len) {
                end -= 1;
                if (position.bytes[end] == 255) continue;
                position.bytes[end] += 1;
                position.len = end + 1;
                return position;
            }
            return null;
        }
    };
}

// The supported expiry prefixes fit with their fixed-width timestamp and ID.
const ExpiryPosition = IndexPosition(@as(usize, @max(session_expiry_prefix.len, tagged_sql_expiry_prefix.len, sql_abort_expiry_prefix.len)) + 49);

fn parseExpiryKey(prefix: []const u8, key: []const u8) ?ParsedSessionExpiryKey {
    if (!std.mem.startsWith(u8, key, prefix)) return null;
    const suffix = key[prefix.len..];
    if (suffix.len != 49 or suffix[16] != ':') return null;
    // Cutoff comparisons rely on the canonical byte order used by the writer.
    for (suffix[0..16]) |byte| switch (byte) {
        '0'...'9', 'a'...'f' => {},
        else => return null,
    };
    return .{
        .timestamp = std.fmt.parseUnsigned(u64, suffix[0..16], 16) catch return null,
        .txn_id = distributed_txn.parseTxnIdHex(suffix[17..]) catch return null,
    };
}

fn ownerLeaseId(alloc: std.mem.Allocator, owner_node_id: u64) ![]u8 {
    return try std.fmt.allocPrint(alloc, "node:{d}", .{owner_node_id});
}

fn leaseRecordOwnerNodeId(owner_id: []const u8) ?u64 {
    if (!std.mem.startsWith(u8, owner_id, "node:")) return null;
    return std.fmt.parseUnsigned(u64, owner_id["node:".len..], 10) catch null;
}

fn decodeAbortReason(value: ?std.json.Value, aborted: bool) !?CommitConflictReason {
    const item = value orelse return null;
    if (item == .null) return null;
    if (!aborted or item != .string) return error.InvalidTransactionSessionRecord;
    return std.meta.stringToEnum(CommitConflictReason, item.string) orelse error.InvalidTransactionSessionRecord;
}

test "distributed txn SQL retained cut survives candidates and savepoints but never durable recovery" {
    _ = @import("sql/execution/retained_snapshot.zig");
    const a = std.testing.allocator;
    const Fixture = struct {
        closed: usize = 0,
        fn open(_: *anyopaque, _: std.mem.Allocator, _: @import("antfly_local_sources").api_table_read_source.RelationalStatementScan) !@import("antfly_local_sources").api_table_read_source.RelationalReadView {
            return error.TestUnexpectedResult;
        }
        fn close(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.closed += 1;
        }
    };
    var backend = mem_backend.Backend.init(a, .{});
    defer backend.close();
    var store = try backend.runtimeStore(a, .{ .name = "system/sql-native-cut" });
    defer store.deinit();
    var durable = DurableSessionStore.initRuntime(a, &store);
    var registry = SessionRegistry.init(&durable);
    defer registry.deinit(a);
    var fixture: Fixture = .{};
    const info = try registry.beginForPrincipal(a, .{ .sql = .{ .database = "default", .namespace = "public", .isolation = .repeatable_read, .mode = .read_only } }, 7, "alice");
    const point = (try registry.createSavepoint(a, try registry.mutation(a, info.txn_id, info.version.owner))).?;
    const cut = try @import("sql/execution/retained_snapshot.zig").Shared.create(a, .{ .ptr = &fixture, .vtable = &.{ .open = Fixture.open, .close = Fixture.close } }, &registry.snapshot_retirement);
    defer cut.release();
    defer registry.snapshot_retirement.drain();
    durable.fail_writes_for_test = true;
    try std.testing.expectError(error.InjectedSessionStoreFailure, registry.installNativeSqlCut(a, info.txn_id, cut));
    durable.fail_writes_for_test = false;
    const before = try registry.nativeSqlCut(a, info.txn_id);
    try std.testing.expect(!before.locked and before.cut == null);
    try registry.installNativeSqlCut(a, info.txn_id, cut);
    _ = try registry.rollbackToSavepoint(a, try registry.mutation(a, info.txn_id, info.version.owner), point.savepoint_id);
    const pinned = try registry.nativeSqlCut(a, info.txn_id);
    try std.testing.expect(pinned.locked and pinned.cut.? == cut);
    pinned.cut.?.release();
    var recovered = (try durable.load(info.txn_id)).?;
    defer recovered.deinit(a);
    try std.testing.expect(recovered.sql.?.characteristics_locked);
    try std.testing.expect(recovered.native_sql_cut == null);
    var reopened = SessionRegistry.init(&durable);
    defer reopened.deinit(a);
    const cold = try reopened.nativeSqlCut(a, info.txn_id);
    try std.testing.expect(cold.locked and cold.cut == null);
    try std.testing.expectEqual(@as(usize, 0), reopened.sessions.count());
    try std.testing.expectError(error.RetainedReadRestartRequired, reopened.installNativeSqlCut(a, info.txn_id, cut));
    try std.testing.expectError(error.RetainedReadRestartRequired, registry.installNativeSqlCut(a, info.txn_id, cut));
    try std.testing.expectEqual(0, fixture.closed);
    try std.testing.expect(registry.remove(a, info.txn_id));
    try std.testing.expectEqual(0, fixture.closed);
}

test "distributed txn SQL characteristics survive faults reopen and savepoint rollback" {
    const a = std.testing.allocator;
    const sql = @import("antfly_local_sources").sql_session;
    var backend = mem_backend.Backend.init(a, .{});
    defer backend.close();
    var store = try backend.runtimeStore(a, .{ .name = "system/sql-characteristics" });
    defer store.deinit();
    var durable = DurableSessionStore.initRuntime(a, &store);
    var registry = SessionRegistry.init(&durable);
    defer registry.deinit(a);
    const info = try registry.beginForPrincipal(a, .{ .sql = .{
        .database = "default",
        .namespace = "public",
        .isolation = .read_committed,
        .mode = .read_write,
    } }, 7, "alice");
    durable.fail_writes_for_test = true;
    try std.testing.expectError(error.InjectedSessionStoreFailure, registry.setSqlCharacteristics(a, info.txn_id, .{ .isolation = .repeatable_read, .mode = .read_only }));
    durable.fail_writes_for_test = false;
    {
        var state = (try registry.getSqlState(a, info.txn_id)).?;
        defer state.deinit(a);
        try std.testing.expectEqual(sql.Isolation.read_committed, state.metadata.isolation);
        try std.testing.expectEqual(sql.ReadMode.read_write, state.metadata.mode);
    }
    try registry.setSqlCharacteristics(a, info.txn_id, .{ .isolation = .repeatable_read });
    const point = (try registry.createSavepoint(a, try registry.mutation(a, info.txn_id, info.version.owner))).?;
    try registry.setSqlCharacteristics(a, info.txn_id, .{ .mode = .read_only });
    try std.testing.expectError(error.SqlTransactionCharacteristicsLocked, registry.setSqlCharacteristics(a, info.txn_id, .{ .mode = .read_write }));
    {
        var restored = (try durable.load(info.txn_id)).?;
        defer restored.deinit(a);
        try std.testing.expectEqual(sql.ReadMode.read_write, restored.savepoints.get(point.savepoint_id).?.transaction_mode.?);
        try std.testing.expectEqual(sql.ReadMode.read_only, restored.sql.?.mode);
    }
    _ = try registry.rollbackToSavepoint(a, try registry.mutation(a, info.txn_id, info.version.owner), point.savepoint_id);
    {
        var state = (try registry.getSqlState(a, info.txn_id)).?;
        defer state.deinit(a);
        try std.testing.expectEqual(sql.ReadMode.read_write, state.metadata.mode);
        try std.testing.expectEqual(sql.Isolation.repeatable_read, state.metadata.isolation);
    }
    durable.fail_writes_for_test = true;
    try std.testing.expectError(error.InjectedSessionStoreFailure, registry.lockSqlCharacteristics(a, info.txn_id));
    durable.fail_writes_for_test = false;
    try registry.lockSqlCharacteristics(a, info.txn_id);
    durable.fail_writes_for_test = true;
    // Subsequent reads neither clone the staged transaction nor rewrite storage.
    var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    try registry.lockSqlCharacteristics(failing.allocator(), info.txn_id);
    durable.fail_writes_for_test = false;
    _ = try registry.rollbackToSavepoint(a, try registry.mutation(a, info.txn_id, info.version.owner), point.savepoint_id);
    _ = try registry.releaseSavepoint(a, try registry.mutation(a, info.txn_id, info.version.owner), point.savepoint_id);
    try std.testing.expectError(error.SqlTransactionCharacteristicsLocked, registry.setSqlCharacteristics(a, info.txn_id, .{ .isolation = .serializable }));
    var restored = (try durable.load(info.txn_id)).?;
    defer restored.deinit(a);
    try std.testing.expect(restored.sql.?.characteristics_locked);
    try std.testing.expectEqual(sql.Isolation.repeatable_read, restored.sql.?.isolation);
    const deferred = try registry.beginForPrincipal(a, .{ .sql = .{
        .database = "default",
        .namespace = "public",
        .isolation = .serializable,
        .mode = .read_write,
        .deferrable = true,
    } }, 7, "alice");
    const point_with_deferrable = (try registry.createSavepoint(a, try registry.mutation(a, deferred.txn_id, deferred.version.owner))).?;
    try registry.setSqlCharacteristics(a, deferred.txn_id, .{ .deferrable = false, .mode = .read_only });
    _ = try registry.rollbackToSavepoint(a, try registry.mutation(a, deferred.txn_id, deferred.version.owner), point_with_deferrable.savepoint_id);
    try std.testing.expectError(error.UnsupportedSqlShape, registry.setSqlCharacteristics(a, deferred.txn_id, .{ .mode = .read_only }));
    var reopened_deferred = (try durable.load(deferred.txn_id)).?;
    defer reopened_deferred.deinit(a);
    try std.testing.expect(reopened_deferred.sql.?.deferrable);
    try std.testing.expectEqual(sql.ReadMode.read_write, reopened_deferred.sql.?.mode);
}

test "distributed txn SQL recovery decodes into the registry allocator without adopting store memory" {
    const a = std.testing.allocator;
    var backend = mem_backend.Backend.init(a, .{});
    defer backend.close();
    var store = try backend.runtimeStore(a, .{ .name = "system/sql-allocator-recovery" });
    defer store.deinit();
    var durable = DurableSessionStore.initRuntime(std.heap.page_allocator, &store);
    var id: db_mod.types.TxnId = undefined;
    {
        var writer = SessionRegistry.init(&durable);
        defer writer.deinit(a);
        id = (try writer.beginForPrincipal(a, .{ .sql = .{
            .database = "default",
            .namespace = "public",
            .isolation = .read_committed,
            .mode = .read_write,
        } }, 7, "alice")).txn_id;
    }
    var reader = SessionRegistry.init(&durable);
    defer reader.deinit(a);
    try reader.lockSqlCharacteristics(a, id);
    try reader.setSqlCharacteristics(a, id, .{ .mode = .read_only });
    var state = (try reader.getSqlState(a, id)).?;
    defer state.deinit(a);
    try std.testing.expect(state.metadata.characteristics_locked);
    try std.testing.expectEqual(@import("antfly_local_sources").sql_session.ReadMode.read_only, state.metadata.mode);
}

test "distributed txn terminal constraint abort survives durable reopen and failed persistence" {
    try typedAbortRecovery();
}

fn typedAbortRecovery() !void {
    const a = std.testing.allocator;
    var backend = mem_backend.Backend.init(a, .{});
    defer backend.close();
    var store = try backend.runtimeStore(a, .{ .name = "system/api-transaction-sessions" });
    defer store.deinit();
    var durable = DurableSessionStore.initRuntime(a, &store);
    var registry = SessionRegistry.init(&durable);
    defer registry.deinit(a);
    for (std.enums.values(CommitConflictReason)) |reason| {
        const options: BeginRequest = .{ .sql = .{
            .database = "default",
            .namespace = "public",
            .isolation = .read_committed,
            .mode = .read_write,
        } };
        const begun = try registry.beginForPrincipal(a, options, 7, "alice");
        const body = try std.json.Stringify.valueAlloc(a, .{ .read_set = [_]u32{}, .tables = .{ .docs = .{ .inserts = .{ .a = .{ .id = 1 } } } } }, .{});
        defer a.free(body);
        var request = try parseCommitRequest(a, body);
        defer request.deinit(a);
        var sealed = (try registry.cloneCommitRequest(a, begun.txn_id, &request)).?;
        defer sealed.deinit(a);
        const tables_view = try sealed.distributedTables(a);
        defer tables_view.deinit();
        const tables = tables_view.tables;
        var plan = (try registry.sealExecutionPlan(a, try registry.mutation(a, begun.txn_id, begun.version.owner), tables)).?;
        defer plan.deinit();
        durable.fail_writes_for_test = true;
        const failure = registry.recordTerminalAbortWithReason(a, begun.txn_id, reason);
        try std.testing.expectError(error.InjectedSessionStoreFailure, failure);
        durable.fail_writes_for_test = false;
        var before = (try durable.load(begun.txn_id)).?;
        defer before.deinit(a);
        try std.testing.expect(!before.terminal_abort);
        try std.testing.expect(before.terminal_abort_reason == null);
        _ = try registry.recordTerminalAbortWithReason(a, begun.txn_id, reason);
        _ = try registry.recordTerminalAbort(a, begun.txn_id);
        var restored = (try durable.load(begun.txn_id)).?;
        defer restored.deinit(a);
        try std.testing.expect(restored.terminal_abort);
        try std.testing.expectEqual(reason, restored.terminal_abort_reason.?);
        var reader = SessionRegistry.init(&durable);
        defer reader.deinit(a);
        var state = (try reader.getSqlState(a, begun.txn_id)).?;
        defer state.deinit(a);
        try std.testing.expect(state.terminal_abort);
        try std.testing.expectEqual(reason, state.terminal_abort_reason.?);
    }
}

fn decodeSearchPath(a: std.mem.Allocator, input: ?std.json.Value) !?@import("../pgwire/search_path.zig").Path {
    const value = input orelse return null;
    if (value == .null) return null;
    var parsed = std.json.parseFromValue(@import("../pgwire/search_path.zig").Path, a, value, .{}) catch |err| return if (err == error.OutOfMemory) err else error.InvalidTransactionSessionRecord;
    defer parsed.deinit();
    return parsed.value;
}

fn sessionContainsNativePayload(session: Session) bool {
    if (session.staged) |staged| for (staged.catalog_bindings.items) |binding| {
        if (binding.sql_persistence != null) return true;
    };
    var savepoints = session.savepoints.valueIterator();
    while (savepoints.next()) |point| for (point.snapshot.catalog_bindings.items) |binding| {
        if (binding.sql_persistence != null) return true;
    };
    return false;
}

fn nativePayloadGenerations(session: Session, alloc: std.mem.Allocator) ![32]u8 {
    var fingerprints: std.ArrayList([32]u8) = .empty;
    defer fingerprints.deinit(alloc);
    if (session.staged) |staged| for (staged.catalog_bindings.items) |binding| {
        if (binding.sql_persistence) |descriptor| try fingerprints.append(alloc, try descriptor.fingerprint());
    };
    var points = session.savepoints.valueIterator();
    while (points.next()) |point| for (point.snapshot.catalog_bindings.items) |binding| {
        if (binding.sql_persistence) |descriptor| try fingerprints.append(alloc, try descriptor.fingerprint());
    };
    const Order = struct {
        fn less(_: void, left: [32]u8, right: [32]u8) bool {
            return std.mem.order(u8, &left, &right) == .lt;
        }
    };
    std.mem.sort([32]u8, fingerprints.items, {}, Order.less);
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly/native-session-generations/v1\x00");
    hash.update(&session.txn_id);
    for (fingerprints.items) |fingerprint| hash.update(&fingerprint);
    var result: [32]u8 = undefined;
    hash.final(&result);
    return result;
}

fn encodeSessionRecord(alloc: std.mem.Allocator, session: Session) ![]u8 {
    // Every production durable writer uses encodeDurableSession. This guard
    // catches accidental future direct serialization of native row payloads.
    if (sessionContainsNativePayload(session)) return error.SqlRelationRecoveryIncomplete;
    return encodeSessionRecordUnchecked(alloc, session);
}

fn requireReaderLease(record: lease_mod.LeaseRecord, owner: u64) !void {
    var buffer: [32]u8 = undefined;
    const expected = try std.fmt.bufPrint(&buffer, "node:{d}", .{owner});
    if (!std.mem.eql(u8, expected, record.owner_id) or record.expires_at_ms <= nextTxnTimestamp() / std.time.ns_per_ms)
        return error.SessionLeaseLost;
}

fn readerActive(session: *const Session) bool {
    const sql = session.sql orelse return false;
    return session.principal != null and !sql.failed and !session.terminal_abort and session.terminal_commit == null and !session.commit_execution_started and !session.external_preparation_prepared;
}

fn checkSqlReader(session: *const Session, principal: []const u8, owner: u64) !void {
    if (!readerActive(session) or !principalsEqual(session.principal, principal)) return error.SqlTransactionNotActive;
    if (session.owner_node_id != owner) return error.SessionLeaseLost;
}

// A prefix of the session itself shares its CAS, deletion, and mixed-version
// rewrite semantics. An older writer drops the field and selects legacy decode.
const reader_field = "\"reader_v1\":\"";
const reader_payload_len = 41;
const prepared_session_header = session_record.prepared_session_header;

fn sessionRecordHeader(bytes: []const u8) []const u8 {
    if (std.mem.startsWith(u8, bytes, prepared_session_header)) return prepared_session_header;
    if (std.mem.startsWith(u8, bytes, session_record.bound_session_header)) return session_record.bound_session_header;
    return "{";
}

const reader_prefix_len = session_record.bound_session_header.len + reader_field.len + 2 * reader_payload_len + 2;

comptime {
    std.debug.assert(reader_prefix_len <= session_record.cached_prefix_bytes);
}

fn readerPayload(session: *const Session) [reader_payload_len]u8 {
    var payload: [reader_payload_len]u8 = undefined;
    payload[0] = @intFromBool(readerActive(session));
    std.mem.writeInt(u64, payload[1..9], session.owner_node_id, .little);
    std.crypto.hash.sha2.Sha256.hash(session.principal orelse "", payload[9..41], .{});
    return payload;
}

// Attached sessions pack recovery state and reader identity into a single
// bounded prefix. Older records remain readable on the same snapshot.
const connection_field = "\"c1\":\"";
const sql_field = "\"s1\":\"";
const payload_len = 65;
const encoded_len = std.base64.standard.Encoder.calcSize(payload_len);
const connection_prefix_len = session_record.bound_session_header.len + connection_field.len + encoded_len + 2;
comptime {
    std.debug.assert(connection_prefix_len <= session_record.cached_prefix_bytes);
}

pub const ConnectionLifecycle = struct {
    connection_id: ?[32]u8,
    version: DurableSessionStore.Version,
    terminal: bool,
};

fn parseConnectionPrefix(bytes: []const u8) !?[payload_len]u8 {
    return parseSqlPrefix(bytes, connection_field);
}

fn sqlPrefixPayload(session: *const Session) ![payload_len]u8 {
    var bytes: [payload_len]u8 = @splat(0);
    bytes[0..41].* = readerPayload(session);
    if (session.commit_execution_started) bytes[0] |= 2;
    if (session.terminal_commit != null or session.terminal_abort) bytes[0] |= 4;
    std.mem.writeInt(u64, bytes[41..49], session.last_touched_timestamp, .little);
    if (session.connection_id) |id| _ = std.fmt.hexToBytes(bytes[49..65], &id) catch return error.InvalidTransactionSessionRecord;
    return bytes;
}

fn parseSqlPrefix(bytes: []const u8, name: []const u8) !?[payload_len]u8 {
    const header = sessionRecordHeader(bytes);
    if (!std.mem.startsWith(u8, bytes, header) or !std.mem.startsWith(u8, bytes[header.len..], name)) return null;
    const field = bytes[header.len + name.len ..];
    if (field.len < encoded_len + 2 or !std.mem.startsWith(u8, field[encoded_len..], "\",")) return error.InvalidTransactionSessionRecord;
    var result: [payload_len]u8 = undefined;
    std.base64.standard.Decoder.decode(&result, field[0..encoded_len]) catch return error.InvalidTransactionSessionRecord;
    if (result[0] & ~@as(u8, 7) != 0) return error.InvalidTransactionSessionRecord;
    return result;
}

fn checkReaderPrefix(bytes: []const u8, principal: []const u8, owner: u64) !bool {
    if (try parseConnectionPrefix(bytes)) |payload| return checkReaderIdentity(payload[0..41], principal, owner);
    if (try parseSqlPrefix(bytes, sql_field)) |payload| return checkReaderIdentity(payload[0..41], principal, owner);
    const payload = (try parseReaderPrefix(bytes)) orelse return false;
    return checkReaderIdentity(&payload, principal, owner);
}

fn parseReaderPrefix(bytes: []const u8) !?[reader_payload_len]u8 {
    const header: []const u8 = sessionRecordHeader(bytes);
    if (!std.mem.startsWith(u8, bytes, header) or !std.mem.startsWith(u8, bytes[header.len..], reader_field)) return null;
    const encoded = bytes[header.len + reader_field.len ..];
    if (encoded.len < 2 * reader_payload_len + 2 or !std.mem.startsWith(u8, encoded[2 * reader_payload_len ..], "\",")) return error.InvalidTransactionSessionRecord;
    var payload: [reader_payload_len]u8 = undefined;
    _ = std.fmt.hexToBytes(&payload, encoded[0 .. 2 * reader_payload_len]) catch return error.InvalidTransactionSessionRecord;
    if (payload[0] > 1) return error.InvalidTransactionSessionRecord;
    return payload;
}

fn checkReaderIdentity(payload: *const [41]u8, principal: []const u8, owner: u64) !bool {
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(principal, &digest, .{});
    if (payload[0] & 1 == 0 or !std.mem.eql(u8, &digest, payload[9..41])) return error.SqlTransactionNotActive;
    if (std.mem.readInt(u64, payload[1..9], .little) != owner) return error.SessionLeaseLost;
    return true;
}

fn requireDurableReader(alloc: std.mem.Allocator, read: anytype, id: db_mod.types.TxnId, principal: []const u8, owner: u64, check_lease: bool) !?u128 {
    const key = try makeSessionKey(alloc, id);
    defer alloc.free(key);
    checkDurableReaderSession(alloc, read, key, id, principal, owner) catch |err| switch (err) {
        error.NotFound => return error.SqlTransactionNotActive,
        else => return err,
    };
    return if (check_lease) try readLeaseTenure(alloc, read, id, owner) else null;
}

// Legacy adoption resets the epoch to zero. Reserve the high half for
// certified leases and rotate a separate durable generation on each promotion.
// Old writers can renew a certified tenure, but cannot restore its generation
// after resetting the lease.
const certified_lease_epoch: u64 = 1 << 63;

fn readerGenerationKey(a: std.mem.Allocator, id: db_mod.types.TxnId) ![]u8 {
    return std.fmt.allocPrint(a, "\x00\x00__api_txn_reader_generation__:{s}", .{std.fmt.bytesToHex(id, .lower)});
}

fn readerGeneration(txn: anytype, key: []const u8) !u64 {
    const raw = txn.get(key) catch |err| if (err == error.NotFound) return 0 else return err;
    if (raw.len != 8) return error.InvalidTransactionSessionRecord;
    return std.mem.readInt(u64, raw[0..8], .little);
}

// A bounded set of durable high-water marks allows per-session generation
// rows to be retired. Each ID always hashes to the same counter; unrelated
// sessions need no common counter. The high bit separates these generations
// from the original per-session promotion sequence.
fn readerClockKey(buf: []u8, id: db_mod.types.TxnId) ![]const u8 {
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(&id, &digest, .{});
    const bucket = digest[0];
    return std.fmt.bufPrint(buf, "\x00\x00__api_txn_reader_clock__:{d}", .{bucket});
}

fn issueReaderGeneration(a: std.mem.Allocator, txn: anytype, id: db_mod.types.TxnId) !void {
    var counter_buf: [64]u8 = undefined;
    const counter = try readerClockKey(&counter_buf, id);
    const key = try readerGenerationKey(a, id);
    defer a.free(key);
    const prior = @max(try readerGeneration(txn, counter), try readerGeneration(txn, key));
    const generation = std.math.add(u64, @max(prior, 1 << 63), 1) catch return error.LeaseEpochOverflow;
    var bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &bytes, generation, .little);
    try txn.put(counter, &bytes);
    try txn.put(key, &bytes);
}

fn readLeaseTenure(alloc: std.mem.Allocator, read: anytype, id: db_mod.types.TxnId, owner: u64) !u128 {
    const lease_key = try makeSessionLeaseKey(alloc, id);
    defer alloc.free(lease_key);
    const raw = read.get(lease_key) catch |err| switch (err) {
        error.NotFound => return error.SessionLeaseLost,
        else => return err,
    };
    const lease = try std.json.parseFromSlice(lease_mod.LeaseRecord, alloc, raw, .{});
    defer lease.deinit();
    try requireReaderLease(lease.value, owner);
    return leaseStamp(alloc, read, id, lease.value.epoch);
}

fn leaseStamp(alloc: std.mem.Allocator, read: anytype, id: db_mod.types.TxnId, epoch: u64) !u128 {
    if (epoch < certified_lease_epoch) return error.LegacyReaderLease;
    const key = try readerGenerationKey(alloc, id);
    defer alloc.free(key);
    const generation = try readerGeneration(read, key);
    if (generation == 0) return error.InvalidTransactionSessionRecord;
    return (@as(u128, generation) << 64) | epoch;
}

fn checkDurableReaderSession(alloc: std.mem.Allocator, read: anytype, key: []const u8, id: db_mod.types.TxnId, principal: []const u8, owner: u64) !void {
    var scope = try read.openReadScope(alloc);
    defer scope.close();
    const prefix = scope.probePrefix(key, @max(reader_prefix_len, connection_prefix_len)) catch |err| switch (err) {
        error.Unsupported, error.EndOfStream => null,
        else => return err,
    };
    if (prefix) |bytes| if (try checkReaderPrefix(bytes, principal, owner)) return;
    const full = try read.get(key);
    if (prefix == null and try checkReaderPrefix(full, principal, owner)) return;
    var legacy = try decodeSessionRecord(alloc, id, full);
    defer legacy.deinit(alloc);
    try checkSqlReader(&legacy, principal, owner);
}

const graph_status_field = "\"g1\":\"";
const graph_status_bytes = 32 + 3 + 3 * 8 + 4 * 4;
const graph_status_encoded_len = std.base64.standard.Encoder.calcSize(graph_status_bytes);
const graph_status_read_len = session_record.bound_session_header.len + graph_status_field.len + graph_status_encoded_len + 2;
comptime {
    std.debug.assert(graph_status_read_len <= session_record.cached_prefix_bytes);
}
const GraphStatusLookup = union(enum) { absent, found: SessionStatus };

fn sessionPrincipalCandidate(scope: *backend_erased.ReadScope, alloc: std.mem.Allocator, key: []const u8, principal: ?[]const u8, digest: *const [32]u8) !bool {
    const narrow = scope.probePrefix(key, graph_status_read_len) catch |err| switch (err) {
        error.NotFound => return false,
        error.Unsupported, error.EndOfStream => null,
        else => return err,
    };
    if (narrow) |raw| if (try sessionPrincipalMatchesPrefix(raw, principal, digest)) |matches| return matches;
    const raw = scope.get(key) catch |err| switch (err) {
        error.NotFound => return false,
        else => return err,
    };
    const parsed = std.json.parseFromSlice(struct { principal: ?[]const u8 = null }, alloc, raw, .{ .ignore_unknown_fields = true }) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return false,
    };
    defer parsed.deinit();
    return principalsEqual(parsed.value.principal, principal);
}

fn sessionPrincipalMatchesPrefix(raw: []const u8, principal: ?[]const u8, digest: *const [32]u8) !?bool {
    if (raw.len < graph_status_read_len) return null;
    const header: []const u8 = sessionRecordHeader(raw);
    const fields = raw[header.len..];
    if (std.mem.startsWith(u8, fields, graph_status_field)) {
        var bits: [graph_status_bytes]u8 = undefined;
        std.base64.standard.Decoder.decode(&bits, fields[graph_status_field.len..][0..graph_status_encoded_len]) catch return error.InvalidTransactionSessionRecord;
        return (principal != null) == (bits[32] != 0) and std.mem.eql(u8, digest, bits[0..32]);
    }
    for ([_][]const u8{ connection_field, sql_field }) |field| {
        if (try parseSqlPrefix(raw, field)) |bytes| return std.mem.eql(u8, digest, bytes[9..41]);
    }
    if (std.mem.startsWith(u8, fields, reader_field)) {
        var bytes: [reader_payload_len]u8 = undefined;
        _ = std.fmt.hexToBytes(&bytes, fields[reader_field.len..][0 .. 2 * reader_payload_len]) catch return error.InvalidTransactionSessionRecord;
        return std.mem.eql(u8, digest, bytes[9..41]);
    }
    return null;
}

fn encodeGraphStatus(out: *std.ArrayList(u8), alloc: std.mem.Allocator, session: *const Session) !void {
    var bits: [graph_status_bytes]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(session.principal orelse "", bits[0..32], .{});
    bits[32] = @intFromBool(session.principal != null);
    bits[33] = @backingInt(sessionDisposition(session.*));
    bits[34] = @intCast(@backingInt(session.sync_level));
    const counts = stagedCounts(session);
    const stamps = [_]u64{ session.owner_node_id, session.begin_timestamp, session.last_touched_timestamp };
    for (stamps, 0..) |value, index| std.mem.writeInt(u64, bits[35 + index * 8 ..][0..8], value, .little);
    const sizes = [_]usize{ counts.tables, counts.reads, counts.writes, counts.deletes };
    for (sizes, 0..) |value, index| std.mem.writeInt(u32, bits[59 + index * 4 ..][0..4], std.math.cast(u32, value) orelse return error.SessionRecordTooLarge, .little);
    var encoded: [graph_status_encoded_len]u8 = undefined;
    try out.appendSlice(alloc, graph_status_field);
    try out.appendSlice(alloc, std.base64.standard.Encoder.encode(&encoded, &bits));
    try out.appendSlice(alloc, "\",");
}

fn readStatusTxn(txn: anytype, alloc: std.mem.Allocator, key: []const u8, id: db_mod.types.TxnId, filter: SessionRegistry.StatusPrincipalFilter, colocated: bool) !?GraphStatusLookup {
    const projected = try readGraphStatus(txn, alloc, key, id, filter);
    var result: GraphStatusLookup = if (projected) |value| switch (value) {
        .absent => try readSqlAbortStatus(txn, alloc, id, filter),
        .found => value,
    } else legacy: {
        const raw = txn.get(key) catch |err| switch (err) {
            error.NotFound => return try readSqlAbortStatus(txn, alloc, id, filter),
            else => return err,
        };
        var session = try decodeSessionRecord(alloc, id, raw);
        defer session.deinit(alloc);
        switch (filter) {
            .all => {},
            .principal => |principal| if (!principalsEqual(session.principal, principal)) return .absent,
        }
        break :legacy .{ .found = sessionStatusSnapshot(&session, true) };
    };
    if (!colocated or result == .absent) return result;
    result.found.lease_expires_at = try matchingLeaseExpiry(txn, alloc, id, result.found.owner_node_id);
    return result;
}

fn decodeSqlTerminalReceipt(alloc: std.mem.Allocator, id: db_mod.types.TxnId, raw: []const u8) !Session {
    var receipt = decodeSessionRecord(alloc, id, raw) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.InvalidTransactionSessionRecord,
    };
    errdefer receipt.deinit(alloc);
    if (receipt.terminal_abort and receipt.terminal_commit == null and DurableSessionStore.isLegacySqlAbortReceipt(receipt)) return receipt;
    if (!receipt.prepared_completion_only or receipt.sql != null or receipt.staged != null or !sessionCompletionSettled(receipt))
        return error.InvalidTransactionSessionRecord;
    if (receipt.terminal_commit) |commit| {
        if (receipt.terminal_abort or commit.coordinator_group_id != null) return error.InvalidTransactionSessionRecord;
    }
    return receipt;
}

fn readSqlAbortStatus(txn: anytype, alloc: std.mem.Allocator, id: db_mod.types.TxnId, filter: SessionRegistry.StatusPrincipalFilter) !GraphStatusLookup {
    const key = try makeSqlAbortReceiptKey(alloc, id);
    defer alloc.free(key);
    const raw = txn.get(key) catch |err| switch (err) {
        error.NotFound => return .absent,
        else => return err,
    };
    var receipt = try decodeSqlTerminalReceipt(alloc, id, raw);
    defer receipt.deinit(alloc);
    switch (filter) {
        .all => {},
        .principal => |principal| if (!principalsEqual(receipt.principal, principal)) return .absent,
    }
    return .{ .found = sessionStatusSnapshot(&receipt, true) };
}

fn matchingLeaseExpiry(txn: anytype, alloc: std.mem.Allocator, id: db_mod.types.TxnId, owner_node_id: u64) !u64 {
    const lease_key = try makeSessionLeaseKey(alloc, id);
    defer alloc.free(lease_key);
    const raw = txn.get(lease_key) catch |err| switch (err) {
        error.NotFound => return 0,
        else => return err,
    };
    const lease = try std.json.parseFromSlice(lease_mod.LeaseRecord, alloc, raw, .{});
    defer lease.deinit();
    return if (leaseRecordOwnerNodeId(lease.value.owner_id) == owner_node_id) leaseExpiryNs(lease.value.expires_at_ms) else 0;
}

fn readGraphStatus(txn: anytype, alloc: std.mem.Allocator, key: []const u8, id: db_mod.types.TxnId, filter: SessionRegistry.StatusPrincipalFilter) !?GraphStatusLookup {
    var scope = txn.openReadScope(alloc) catch |err| switch (err) {
        error.Unsupported => return null,
        else => return err,
    };
    defer scope.close();
    const raw = scope.probePrefix(key, graph_status_read_len) catch |err| switch (err) {
        error.NotFound => return .absent,
        error.Unsupported, error.EndOfStream => return null,
        else => return err,
    };
    return graphStatusFromPrefix(raw, id, filter);
}

fn recoveryPrefixMayNeedWork(scope: *backend_erased.ReadScope, key: []const u8, id: db_mod.types.TxnId) !bool {
    const raw = scope.probePrefix(key, graph_status_read_len) catch |err| switch (err) {
        error.NotFound => return false,
        error.Unsupported, error.EndOfStream => return true,
        else => return err,
    };
    if (try graphStatusFromPrefix(raw, id, .all)) |projection| {
        return switch (projection) {
            .absent => false,
            .found => |status| switch (status.disposition) {
                .active, .aborted, .committed => false,
                else => true,
            },
        };
    }
    for ([_][]const u8{ connection_field, sql_field }) |field| {
        if (try parseSqlPrefix(raw, field)) |payload| return payload[0] & 6 != 0;
    }
    return true;
}

fn graphStatusFromPrefix(raw: []const u8, id: db_mod.types.TxnId, filter: SessionRegistry.StatusPrincipalFilter) !?GraphStatusLookup {
    const header: []const u8 = sessionRecordHeader(raw);
    if (raw.len < graph_status_read_len or !std.mem.startsWith(u8, raw[header.len..], graph_status_field)) return null;
    const start = header.len + graph_status_field.len;
    var bits: [graph_status_bytes]u8 = undefined;
    std.base64.standard.Decoder.decode(&bits, raw[start..][0..graph_status_encoded_len]) catch return error.InvalidTransactionSessionRecord;
    switch (filter) {
        .all => {},
        .principal => |principal| {
            if ((principal != null) != (bits[32] != 0)) return .absent;
            var digest: [32]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash(principal orelse "", &digest, .{});
            if (!std.mem.eql(u8, &digest, bits[0..32])) return .absent;
        },
    }
    var stamps: [3]u64 = undefined;
    for (&stamps, 0..) |*value, index| value.* = std.mem.readInt(u64, bits[35 + index * 8 ..][0..8], .little);
    var sizes: [4]u32 = undefined;
    for (&sizes, 0..) |*value, index| value.* = std.mem.readInt(u32, bits[59 + index * 4 ..][0..4], .little);
    const disposition = std.enums.fromInt(SessionDisposition, bits[33]) orelse return error.InvalidTransactionSessionRecord;
    const sync_level = std.enums.fromInt(db_mod.types.SyncLevel, bits[34]) orelse return error.InvalidTransactionSessionRecord;
    return .{ .found = .{
        .txn_id = id,
        .owner_node_id = stamps[0],
        .begin_timestamp = stamps[1],
        .last_touched_timestamp = stamps[2],
        .lease_expires_at = 0,
        .sync_level = sync_level,
        .disposition = disposition,
        .staged_table_count = @intCast(sizes[0]),
        .staged_read_count = @intCast(sizes[1]),
        .staged_write_count = @intCast(sizes[2]),
        .staged_delete_count = @intCast(sizes[3]),
        .read_snapshot_count = 0,
        .savepoint_count = 0,
        .durable = true,
    } };
}

const SessionListingSummary = struct {
    counts: StagedCounts,
    tables: []const SessionTableDetail,
    bindings: []const CatalogBinding,
};

fn encodeSessionRecordUnchecked(alloc: std.mem.Allocator, session: Session) ![]u8 {
    return encodeSessionRecordWithListingLimit(alloc, session, std.math.maxInt(usize));
}

fn encodeSessionRecordWithListingLimit(alloc: std.mem.Allocator, session: Session, listing_limit: usize) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    const bound = if (session.staged != null and session.staged.?.bound_catalog_incarnation != null) true else blk: {
        var saves = session.savepoints.valueIterator();
        while (saves.next()) |save| if (save.snapshot.bound_catalog_incarnation != null) break :blk true;
        break :blk false;
    };
    try out.appendSlice(alloc, if (session.prepared_completion_only) prepared_session_header else if (bound) session_record.bound_session_header else "{");
    if (session.graph_receipt) {
        try encodeGraphStatus(&out, alloc, &session);
    } else if (session.connection_id != null or session.sql != null) {
        const bytes = try sqlPrefixPayload(&session);
        var encoded: [encoded_len]u8 = undefined;
        try out.appendSlice(alloc, if (session.connection_id != null) connection_field else sql_field);
        try out.appendSlice(alloc, std.base64.standard.Encoder.encode(&encoded, &bytes));
    } else {
        try out.appendSlice(alloc, reader_field);
        try out.appendSlice(alloc, &std.fmt.bytesToHex(readerPayload(&session), .lower));
    }
    if (!session.graph_receipt) try out.appendSlice(alloc, "\",");
    if (session.graph_receipt) {
        try out.appendSlice(alloc, "\"graph_receipt\":true,");
        if (session.terminal_abort) {
            try out.appendSlice(alloc, "\"graph_abort_tables\":");
            try out.append(alloc, '[');
            for (session.graph_abort_tables, 0..) |table, index| {
                if (index != 0) try out.append(alloc, ',');
                var public_fields = table;
                public_fields.storage_name = null;
                try out.print(alloc, "{f}", .{std.json.fmt(public_fields, .{ .emit_null_optional_fields = false })});
            }
            try out.appendSlice(alloc, "],\"graph_abort_storage_names\":[");
            for (session.graph_abort_tables, 0..) |table, index| {
                if (index != 0) try out.append(alloc, ',');
                if (table.storage_name) |name| try appendJsonString(alloc, &out, name) else try out.appendSlice(alloc, "null");
            }
            try out.appendSlice(alloc, "],");
        }
    }
    try out.appendSlice(alloc, "\"owner_node_id\":");
    try out.print(alloc, "{d}", .{session.owner_node_id});
    try out.appendSlice(alloc, ",\"terminal_abort_reason\":");
    if (session.terminal_abort_reason) |reason| {
        try appendJsonString(alloc, &out, @tagName(reason));
    } else try out.appendSlice(alloc, "null");
    if (session.volatile_payload) |reference| {
        try out.appendSlice(alloc, ",\"volatile_payload\":");
        const encoded_reference = try std.json.Stringify.valueAlloc(alloc, reference, .{});
        defer alloc.free(encoded_reference);
        try out.appendSlice(alloc, encoded_reference);
    }
    try out.appendSlice(alloc, ",\"principal\":");
    if (session.principal) |principal| {
        try appendJsonString(alloc, &out, principal);
    } else {
        try out.appendSlice(alloc, "null");
    }
    try out.appendSlice(alloc, ",\"connection_id\":");
    if (session.connection_id) |connection_id|
        try appendJsonString(alloc, &out, &connection_id)
    else
        try out.appendSlice(alloc, "null");
    try out.appendSlice(alloc, ",\"sql\":");
    if (session.sql) |metadata| {
        const encoded = try std.json.Stringify.valueAlloc(alloc, metadata, .{});
        defer alloc.free(encoded);
        try out.appendSlice(alloc, encoded);
    } else try out.appendSlice(alloc, "null");
    try out.appendSlice(alloc, ",\"begin_timestamp\":");
    try out.print(alloc, "{d}", .{session.begin_timestamp});
    try out.appendSlice(alloc, ",\"last_touched_timestamp\":");
    try out.print(alloc, "{d}", .{session.last_touched_timestamp});
    try out.appendSlice(alloc, ",\"sync_level\":");
    try appendJsonString(alloc, &out, syncLevelText(session.sync_level));
    try out.appendSlice(alloc, ",\"next_savepoint_id\":");
    try out.print(alloc, "{d}", .{session.next_savepoint_id});
    try out.appendSlice(alloc, ",\"staged\":");
    if (session.staged) |staged| {
        const encoded = try encodeCommitRequestMode(alloc, staged, true);
        defer alloc.free(encoded);
        try out.appendSlice(alloc, encoded);
    } else {
        try out.appendSlice(alloc, "null");
    }
    try out.appendSlice(alloc, ",\"commit_body_digest\":");
    if (session.commit_body_digest) |digest| {
        const hex = std.fmt.bytesToHex(digest, .lower);
        try appendJsonString(alloc, &out, &hex);
    } else {
        try out.appendSlice(alloc, "null");
    }
    try out.appendSlice(alloc, ",\"commit_execution_started\":");
    try out.appendSlice(alloc, if (session.commit_execution_started) "true" else "false");
    try out.appendSlice(alloc, ",\"terminal_abort\":");
    try out.appendSlice(alloc, if (session.terminal_abort) "true" else "false");
    if (session.terminal_abort and session.sql_abort_acknowledgement != .conflict) {
        try out.appendSlice(alloc, ",\"sql_abort_acknowledgement\":");
        try appendJsonString(alloc, &out, @tagName(session.sql_abort_acknowledgement));
    }
    try out.appendSlice(alloc, ",\"execution_plan\":");
    if (session.execution_plan) |bytes| try appendJsonString(alloc, &out, bytes) else try out.appendSlice(alloc, "null");
    try out.appendSlice(alloc, ",\"external_preparation\":");
    if (session.external_preparation) |bytes| try appendJsonString(alloc, &out, bytes) else try out.appendSlice(alloc, "null");
    try out.appendSlice(alloc, ",\"external_preparation_prepared\":");
    try out.appendSlice(alloc, if (session.external_preparation_prepared) "true" else "false");
    try out.appendSlice(alloc, ",\"external_receipt_acknowledged\":");
    try out.appendSlice(alloc, if (session.external_receipt_acknowledged) "true" else "false");
    if (session.external_decision) |decision| {
        try out.appendSlice(alloc, ",\"external_decision\":");
        const encoded = try std.json.Stringify.valueAlloc(alloc, decision, .{});
        defer alloc.free(encoded);
        try out.appendSlice(alloc, encoded);
    }
    try out.appendSlice(alloc, ",\"terminal_commit\":");
    if (session.terminal_commit) |terminal| {
        try out.appendSlice(alloc, "{\"status\":");
        try appendJsonString(alloc, &out, terminal.status.text());
        try out.appendSlice(alloc, ",\"coordinator_group_id\":");
        if (terminal.coordinator_group_id) |group_id| {
            try out.print(alloc, "{d}", .{group_id});
        } else {
            try out.appendSlice(alloc, "null");
        }
        try out.appendSlice(alloc, ",\"coordinator_table_name\":");
        if (terminal.coordinator_table_name) |table_name| {
            try appendJsonString(alloc, &out, table_name);
        } else {
            try out.appendSlice(alloc, "null");
        }
        try out.appendSlice(alloc, ",\"coordinator_acknowledged\":");
        try out.appendSlice(alloc, if (terminal.coordinator_acknowledged) "true" else "false");
        try out.appendSlice(alloc, ",\"repair_required\":");
        try out.appendSlice(alloc, if (terminal.repair_required) "true" else "false");
        if (terminal.commit_version) |version| try out.print(alloc, ",\"commit_version\":{d}", .{version});
        try out.append(alloc, '}');
    } else {
        try out.appendSlice(alloc, "null");
    }
    try out.appendSlice(alloc, ",\"lake_cuts\":[");
    var lake_cuts_it = session.lake_cuts.iterator();
    var first_lake_cut = true;
    while (lake_cuts_it.next()) |entry| {
        if (!first_lake_cut) try out.append(alloc, ',');
        first_lake_cut = false;
        try appendReadSnapshotJson(alloc, &out, entry.value_ptr.*);
    }
    try out.append(alloc, ']');
    try out.appendSlice(alloc, ",\"read_snapshots\":[");
    var snapshots_it = session.read_snapshots.iterator();
    var first_snapshot = true;
    while (snapshots_it.next()) |entry| {
        if (!first_snapshot) try out.append(alloc, ',');
        first_snapshot = false;
        try appendReadSnapshotJson(alloc, &out, entry.value_ptr.*);
    }
    try out.append(alloc, ']');
    try out.print(alloc, ",\"sequence_epoch\":{d}", .{session.sequence_epoch});
    try out.appendSlice(alloc, ",\"sequence_state\":");
    const sequence_state = try std.json.Stringify.valueAlloc(alloc, session.sequence_state, .{});
    defer alloc.free(sequence_state);
    try out.appendSlice(alloc, sequence_state);
    try out.appendSlice(alloc, ",\"setting_active\":");
    const active_settings = try std.json.Stringify.valueAlloc(alloc, session.setting_active.items, .{});
    defer alloc.free(active_settings);
    try out.appendSlice(alloc, active_settings);
    try out.appendSlice(alloc, ",\"setting_committed\":");
    const committed_settings = try std.json.Stringify.valueAlloc(alloc, session.setting_committed.items, .{});
    defer alloc.free(committed_settings);
    try out.appendSlice(alloc, committed_settings);
    try out.appendSlice(alloc, ",\"savepoints\":[");
    var it = session.savepoints.iterator();
    var first = true;
    while (it.next()) |entry| {
        if (!first) try out.append(alloc, ',');
        first = false;
        try out.appendSlice(alloc, "{\"id\":");
        try out.print(alloc, "{d}", .{entry.key_ptr.*});
        if (entry.value_ptr.notification_count != 0) try out.print(alloc, ",\"notification_count\":{d}", .{entry.value_ptr.notification_count});
        if (entry.value_ptr.generation_count != 0) try out.print(alloc, ",\"generation_count\":{d}", .{entry.value_ptr.generation_count});
        try out.appendSlice(alloc, ",\"name\":");
        if (entry.value_ptr.name) |name| try appendJsonString(alloc, &out, name) else try out.appendSlice(alloc, "null");
        const mode = try std.json.Stringify.valueAlloc(alloc, entry.value_ptr.transaction_mode, .{});
        defer alloc.free(mode);
        try out.appendSlice(alloc, ",\"transaction_mode\":");
        try out.appendSlice(alloc, mode);
        try out.appendSlice(alloc, ",\"transaction_deferrable\":");
        if (entry.value_ptr.transaction_deferrable) |deferrable| try out.appendSlice(alloc, if (deferrable) "true" else "false") else try out.appendSlice(alloc, "null");
        const active_path = try std.json.Stringify.valueAlloc(alloc, entry.value_ptr.search_path_active, .{});
        defer alloc.free(active_path);
        try out.appendSlice(alloc, ",\"search_path_active\":");
        try out.appendSlice(alloc, active_path);
        const committed_path = try std.json.Stringify.valueAlloc(alloc, entry.value_ptr.search_path_committed, .{});
        defer alloc.free(committed_path);
        try out.appendSlice(alloc, ",\"search_path_committed\":");
        try out.appendSlice(alloc, committed_path);
        try out.appendSlice(alloc, ",\"snapshot\":");
        const encoded = try encodeCommitRequestMode(alloc, entry.value_ptr.snapshot, true);
        defer alloc.free(encoded);
        try out.appendSlice(alloc, encoded);
        try out.appendSlice(alloc, ",\"read_snapshots\":[");
        var savepoint_snapshots_it = entry.value_ptr.read_snapshots.iterator();
        var first_savepoint_snapshot = true;
        while (savepoint_snapshots_it.next()) |snapshot_entry| {
            if (!first_savepoint_snapshot) try out.append(alloc, ',');
            first_savepoint_snapshot = false;
            try appendReadSnapshotJson(alloc, &out, snapshot_entry.value_ptr.*);
        }
        try out.append(alloc, ']');
        try out.appendSlice(alloc, ",\"setting_active\":");
        const point_active = try std.json.Stringify.valueAlloc(alloc, entry.value_ptr.setting_active.items, .{});
        defer alloc.free(point_active);
        try out.appendSlice(alloc, point_active);
        try out.appendSlice(alloc, ",\"setting_committed\":");
        const point_committed = try std.json.Stringify.valueAlloc(alloc, entry.value_ptr.setting_committed.items, .{});
        defer alloc.free(point_committed);
        try out.appendSlice(alloc, point_committed);
        try out.append(alloc, '}');
    }
    try out.append(alloc, ']');
    if (session.listing_summary_tail) |summary| {
        if (out.items.len +| listing_tail_field.len +| summary.len +| 2 <= listing_limit) {
            try out.appendSlice(alloc, listing_tail_field);
            try out.appendSlice(alloc, summary);
            try out.append(alloc, '"');
        }
    }
    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

pub fn decodeSessionRecord(alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId, body: []const u8) !Session {
    return decodeSessionRecordForOwner(alloc, txn_id, body, null);
}

/// Decode only fields needed to replay sealed work already held by this
/// owner. Adoption and legacy replay still load the complete durable record.
fn decodeOwnedSealedRecovery(alloc: std.mem.Allocator, id: db_mod.types.TxnId, body: []const u8, owner: u64) !?Session {
    const Header = struct {
        session_format: u16 = 2,
        owner_node_id: ?u64 = null,
        begin_timestamp: u64,
        sync_level: []const u8,
        commit_body_digest: ?[]const u8 = null,
        commit_execution_started: bool = false,
        terminal_abort: bool = false,
        execution_plan: ?[]const u8 = null,
        external_preparation: ?[]const u8 = null,
        terminal_commit: ?struct {
            status: TerminalCommitStatus,
            coordinator_group_id: ?u64,
            coordinator_table_name: ?[]const u8,
            coordinator_acknowledged: bool = false,
            repair_required: bool = false,
        } = null,
    };
    const decoded = std.json.parseFromSlice(Header, alloc, body, .{ .ignore_unknown_fields = true }) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        else => return null,
    };
    defer decoded.deinit();
    const header = decoded.value;
    if (header.session_format != 2 and header.session_format != 3) return error.InvalidTransactionSessionRecord;
    if ((header.owner_node_id orelse sessionOwnerNodeId(id)) != owner or header.external_preparation != null) return null;
    const plan = header.execution_plan orelse return null;
    if (!header.commit_execution_started) return error.InvalidTransactionSessionRecord;
    const hex = header.commit_body_digest orelse return error.InvalidTransactionSessionRecord;
    var digest: [32]u8 = undefined;
    _ = std.fmt.hexToBytes(&digest, hex) catch return error.InvalidTransactionSessionRecord;
    var session: Session = .{
        .owner_alloc = alloc,
        .txn_id = id,
        .prepared_completion_only = header.session_format == 3,
        .owner_node_id = owner,
        .begin_timestamp = header.begin_timestamp,
        .last_touched_timestamp = header.begin_timestamp,
        .sync_level = parseSyncLevel(.{ .string = header.sync_level }) orelse return error.InvalidTransactionSessionRecord,
        .commit_body_digest = digest,
        .commit_execution_started = true,
        .terminal_abort = header.terminal_abort,
        .execution_plan = try alloc.dupe(u8, plan),
    };
    errdefer session.deinit(alloc);
    if (header.terminal_commit) |terminal| {
        if (header.terminal_abort or (terminal.coordinator_group_id == null) != (terminal.coordinator_table_name == null))
            return error.InvalidTransactionSessionRecord;
        session.terminal_commit = .{
            .status = terminal.status,
            .coordinator_group_id = terminal.coordinator_group_id,
            .coordinator_table_name = if (terminal.coordinator_table_name) |name| try alloc.dupe(u8, name) else null,
            .coordinator_acknowledged = terminal.coordinator_acknowledged,
            .repair_required = terminal.repair_required,
        };
    }
    return session;
}

fn decodeSessionRecordForOwner(alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId, body: []const u8, recovery_owner: ?u64) !Session {
    if (recovery_owner) |owner|
        if (try decodeOwnedSealedRecovery(alloc, txn_id, body, owner)) |session| return session;
    // Staged rows, read snapshots, and savepoints share this durable envelope.
    // Do not convert any user number until its schema is available.
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{ .parse_numbers = false });
    defer parsed.deinit();
    const obj = switch (parsed.value) {
        .object => |obj| obj,
        else => return error.InvalidTransactionSessionRecord,
    };
    return decodeSessionObject(alloc, txn_id, obj);
}

const listing_tail_field = ",\"listing_v2\":\"";

/// The newest writer puts this base64 string last. A legacy record ends
/// in the savepoint array, and nested values cannot end the root object.
fn listingTailValue(raw: []const u8) ?[]const u8 {
    if (!std.mem.endsWith(u8, raw, "\"}")) return null;
    const end = raw.len - 2;
    const start_quote = std.mem.lastIndexOfScalar(u8, raw[0..end], '"') orelse return null;
    const start = start_quote + 1;
    if (start < listing_tail_field.len or !std.mem.eql(u8, raw[start - listing_tail_field.len .. start], listing_tail_field)) return null;
    return raw[start..end];
}

fn encodeListingSummaryBytes(alloc: std.mem.Allocator, json: []const u8) ![]u8 {
    const encoder = std.base64.standard.Encoder;
    const encoded = try alloc.alloc(u8, encoder.calcSize(json.len));
    _ = encoder.encode(encoded, json);
    return encoded;
}

fn decodeListingSummaryBytes(alloc: std.mem.Allocator, encoded: []const u8) ![]u8 {
    const decoder = std.base64.standard.Decoder;
    const size = decoder.calcSizeForSlice(encoded) catch return error.InvalidTransactionSessionRecord;
    const bytes = try alloc.alloc(u8, size);
    errdefer alloc.free(bytes);
    decoder.decode(bytes, encoded) catch return error.InvalidTransactionSessionRecord;
    return bytes;
}

fn decodeSessionForListing(alloc: std.mem.Allocator, id: db_mod.types.TxnId, raw: []const u8, savepoint_count: *usize, summary: *?std.json.Parsed(SessionListingSummary)) !Session {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const scratch = arena.allocator();
    var scanner = std.json.Scanner.initCompleteInput(scratch, raw);
    defer scanner.deinit();
    if (try scanner.next() != .object_begin) return error.InvalidTransactionSessionRecord;
    const listing_tail = listingTailValue(raw);
    if (listing_tail) |encoded| decode_summary: {
        const bytes = decodeListingSummaryBytes(scratch, encoded) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => break :decode_summary,
        };
        summary.* = std.json.parseFromSlice(SessionListingSummary, alloc, bytes, .{ .allocate = .alloc_always }) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => null,
        };
    }
    var object: std.json.ObjectMap = .empty;
    var has_current_kind = listing_tail != null;
    while (true) {
        const key = switch (try scanner.nextAlloc(scratch, .alloc_if_needed)) {
            .object_end => break,
            .string => |key| key,
            .allocated_string => |key| key,
            else => return error.InvalidTransactionSessionRecord,
        };
        if (std.mem.eql(u8, key, "graph_receipt") or
            std.mem.eql(u8, key, "s1") or
            std.mem.eql(u8, key, "c1")) has_current_kind = true;
        const entry = try object.getOrPut(scratch, key);
        if (entry.found_existing) return error.InvalidTransactionSessionRecord;
        if (std.mem.eql(u8, key, "listing_v2")) {
            try scanner.skipValue();
            entry.value_ptr.* = .null;
        } else if (std.mem.eql(u8, key, "listing_v1")) {
            if (summary.* != null) return error.InvalidTransactionSessionRecord;
            const value = try std.json.innerParse(std.json.Value, scratch, &scanner, .{
                .allocate = .alloc_if_needed,
                .max_value_len = raw.len,
            });
            summary.* = std.json.parseFromValue(SessionListingSummary, alloc, value, .{ .allocate = .alloc_always }) catch |err| switch (err) {
                error.OutOfMemory => return err,
                else => null,
            };
            entry.value_ptr.* = .null;
        } else if (summary.* != null and std.mem.eql(u8, key, "staged")) {
            try scanner.skipValue();
            entry.value_ptr.* = .null;
        } else if (std.mem.eql(u8, key, "savepoints")) {
            if (try scanner.next() != .array_begin) return error.InvalidTransactionSessionRecord;
            while (try scanner.peekNextTokenType() == .object_begin) {
                try scanner.skipValue();
                savepoint_count.* += 1;
            }
            if (try scanner.next() != .array_end) return error.InvalidTransactionSessionRecord;
            entry.value_ptr.* = .{ .array = std.json.Array.init(scratch) };
        } else if ((has_current_kind and std.mem.eql(u8, key, "execution_plan")) or
            std.mem.eql(u8, key, "lake_cuts") or std.mem.eql(u8, key, "setting_active") or
            std.mem.eql(u8, key, "setting_committed"))
        {
            try scanner.skipValue();
            entry.value_ptr.* = .null;
            _ = object.orderedRemove(key);
        } else {
            entry.value_ptr.* = try std.json.innerParse(std.json.Value, scratch, &scanner, .{
                .allocate = .alloc_if_needed,
                .max_value_len = raw.len,
                .parse_numbers = false,
            });
        }
    }
    if (try scanner.next() != .end_of_document) return error.InvalidTransactionSessionRecord;
    _ = object.orderedRemove("listing_v2");
    _ = object.orderedRemove("listing_v1");
    return decodeSessionObject(alloc, id, object);
}

fn decodeSessionObject(alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId, obj: std.json.ObjectMap) !Session {
    var prepared_completion_only = false;
    if (obj.get("session_format")) |version| {
        if (version != .number_string) return error.InvalidTransactionSessionRecord;
        prepared_completion_only = std.mem.eql(u8, version.number_string, "3");
        if (!prepared_completion_only and !std.mem.eql(u8, version.number_string, "2")) return error.InvalidTransactionSessionRecord;
    }
    var session: Session = .{
        .owner_alloc = alloc,
        .txn_id = txn_id,
        .prepared_completion_only = prepared_completion_only,
        .owner_node_id = if (obj.get("owner_node_id")) |value|
            switch (value) {
                .integer => |v| try nonNegativeRecordInteger(v),
                .number_string => |text| try recordNumber(text),
                else => return error.InvalidTransactionSessionRecord,
            }
        else
            sessionOwnerNodeId(txn_id),
        .principal = null,
        .connection_id = if (obj.get("connection_id")) |value|
            switch (value) {
                .string => |encoded| blk: {
                    const parsed_id = distributed_txn.parseTxnIdHex(encoded) catch return error.InvalidTransactionSessionRecord;
                    break :blk std.fmt.bytesToHex(parsed_id, .lower);
                },
                .null => null,
                else => return error.InvalidTransactionSessionRecord,
            }
        else
            null,
        .begin_timestamp = switch (obj.get("begin_timestamp") orelse return error.InvalidTransactionSessionRecord) {
            .integer => |v| try nonNegativeRecordInteger(v),
            .number_string => |text| try recordNumber(text),
            else => return error.InvalidTransactionSessionRecord,
        },
        .last_touched_timestamp = 0,
        .sync_level = parseSyncLevel(obj.get("sync_level") orelse return error.InvalidTransactionSessionRecord) orelse return error.InvalidTransactionSessionRecord,
        .next_savepoint_id = switch (obj.get("next_savepoint_id") orelse return error.InvalidTransactionSessionRecord) {
            .integer => |v| try nonNegativeRecordInteger(v),
            .number_string => |text| try recordNumber(text),
            else => return error.InvalidTransactionSessionRecord,
        },
    };
    errdefer session.deinit(alloc);
    if (obj.get("volatile_payload")) |raw| {
        const reference = try std.json.parseFromValue(volatile_capsules.Reference, alloc, raw, .{});
        defer reference.deinit();
        try reference.value.validate();
        session.volatile_payload = reference.value;
        session.volatile_payload_available = false;
    }
    if (obj.get("sql_abort_acknowledgement")) |ack| {
        if (ack != .string) return error.InvalidTransactionSessionRecord;
        session.sql_abort_acknowledgement = std.meta.stringToEnum(SqlAbortAcknowledgement, ack.string) orelse return error.InvalidTransactionSessionRecord;
    }

    session.principal = if (obj.get("principal")) |value|
        switch (value) {
            .string => |principal| try alloc.dupe(u8, principal),
            .null => null,
            else => return error.InvalidTransactionSessionRecord,
        }
    else
        null;
    if (obj.get("sql")) |metadata| {
        if (metadata != .null) {
            const decoded = std.json.parseFromValue(SqlMetadata, alloc, metadata, .{}) catch return error.InvalidTransactionSessionRecord;
            defer decoded.deinit();
            for (decoded.value.catalog_generations.entries.items) |entry| {
                if (!std.mem.eql(u8, &entry.transaction, &session.txn_id)) return error.InvalidTransactionSessionRecord;
            }
            session.sql = try decoded.value.clone(alloc);
        }
    }
    session.last_touched_timestamp = if (obj.get("last_touched_timestamp")) |value|
        switch (value) {
            .integer => |v| try nonNegativeRecordInteger(v),
            .number_string => |text| try recordNumber(text),
            else => return error.InvalidTransactionSessionRecord,
        }
    else
        session.begin_timestamp;
    if (obj.get("staged")) |staged_value| {
        if (staged_value != .null) session.staged = try parseStoredCommitValue(alloc, staged_value);
    }
    if (session.staged != null) {
        if (obj.get("listing_v2")) |summary| {
            if (summary == .string) session.listing_summary_tail = try alloc.dupe(u8, summary.string);
        } else if (obj.get("listing_v1")) |summary| {
            if (summary == .object) {
                const json = try std.json.Stringify.valueAlloc(alloc, summary, .{});
                defer alloc.free(json);
                session.listing_summary_tail = try encodeListingSummaryBytes(alloc, json);
            }
        }
    }
    if (obj.get("commit_body_digest")) |digest_value| {
        switch (digest_value) {
            .string => |encoded| {
                if (encoded.len != 64) return error.InvalidTransactionSessionRecord;
                var digest: [32]u8 = undefined;
                _ = std.fmt.hexToBytes(&digest, encoded) catch return error.InvalidTransactionSessionRecord;
                session.commit_body_digest = digest;
            },
            .null => {},
            else => return error.InvalidTransactionSessionRecord,
        }
    }
    session.commit_execution_started = if (obj.get("commit_execution_started")) |value| switch (value) {
        .bool => |started| started,
        else => return error.InvalidTransactionSessionRecord,
    } else false;
    session.terminal_abort = if (obj.get("terminal_abort")) |value| switch (value) {
        .bool => |aborted| aborted,
        else => return error.InvalidTransactionSessionRecord,
    } else false;
    if (session.terminal_abort and !session.commit_execution_started) return error.InvalidTransactionSessionRecord;
    session.terminal_abort_reason = try decodeAbortReason(obj.get("terminal_abort_reason"), session.terminal_abort);
    session.external_preparation_prepared = if (obj.get("external_preparation_prepared")) |value| switch (value) {
        .bool => |v| v,
        else => return error.InvalidTransactionSessionRecord,
    } else false;
    session.external_receipt_acknowledged = if (obj.get("external_receipt_acknowledged")) |value| switch (value) {
        .bool => |v| v,
        else => return error.InvalidTransactionSessionRecord,
    } else false;
    if (obj.get("graph_receipt")) |value| {
        if (value != .bool) return error.InvalidTransactionSessionRecord;
        session.graph_receipt = value.bool;
    }
    if (obj.get("graph_abort_tables")) |value| {
        if (!session.graph_receipt or !session.terminal_abort) return error.InvalidTransactionSessionRecord;
        var tables = try std.json.parseFromValue([]SessionTableDetail, alloc, value, .{});
        defer tables.deinit();
        session.graph_abort_tables = try cloneTableDetails(alloc, tables.value);
    }
    if (obj.get("graph_abort_storage_names")) |value| {
        if (!session.graph_receipt or !session.terminal_abort or value != .array or value.array.items.len != session.graph_abort_tables.len)
            return error.InvalidTransactionSessionRecord;
        for (session.graph_abort_tables, value.array.items) |*table, name| switch (name) {
            .null => {},
            .string => |physical| {
                if (table.storage_name != null) return error.InvalidTransactionSessionRecord;
                table.storage_name = try alloc.dupe(u8, physical);
            },
            else => return error.InvalidTransactionSessionRecord,
        };
    }
    if (obj.get("execution_plan")) |value| switch (value) {
        .null => {},
        .string => |bytes| {
            if (!session.commit_execution_started or session.commit_body_digest == null) return error.InvalidTransactionSessionRecord;
            if (obj.get("graph_receipt") == null) {
                var plan = try parseExecutionPlan(alloc, bytes);
                defer plan.deinit();
                // Graph receipts written before the explicit marker still carry
                // the server-only graph normalization flag in their sealed plan.
                for (plan.value) |table| if (table.normalized_graph_rows) {
                    session.graph_receipt = true;
                    break;
                };
            }
            session.execution_plan = try alloc.dupe(u8, bytes);
        },
        else => return error.InvalidTransactionSessionRecord,
    };
    if (obj.get("external_preparation")) |value| switch (value) {
        .null => {},
        .string => |bytes| {
            session.external_preparation = try alloc.dupe(u8, bytes);
        },
        else => return error.InvalidTransactionSessionRecord,
    };
    if (obj.get("terminal_commit")) |terminal_value| {
        if (terminal_value != .null) {
            const terminal_obj = switch (terminal_value) {
                .object => |value| value,
                else => return error.InvalidTransactionSessionRecord,
            };
            const status_text = switch (terminal_obj.get("status") orelse return error.InvalidTransactionSessionRecord) {
                .string => |value| value,
                else => return error.InvalidTransactionSessionRecord,
            };
            const coordinator_group_id: ?u64 = switch (terminal_obj.get("coordinator_group_id") orelse return error.InvalidTransactionSessionRecord) {
                .integer => |value| try nonNegativeRecordInteger(value),
                .number_string => |text| try recordNumber(text),
                .null => null,
                else => return error.InvalidTransactionSessionRecord,
            };
            const coordinator_table_name_text: ?[]const u8 = switch (terminal_obj.get("coordinator_table_name") orelse return error.InvalidTransactionSessionRecord) {
                .string => |value| value,
                .null => null,
                else => return error.InvalidTransactionSessionRecord,
            };
            if ((coordinator_group_id == null) != (coordinator_table_name_text == null)) return error.InvalidTransactionSessionRecord;
            const status = std.meta.stringToEnum(TerminalCommitStatus, status_text) orelse return error.InvalidTransactionSessionRecord;
            const coordinator_acknowledged = if (terminal_obj.get("coordinator_acknowledged")) |value| switch (value) {
                .bool => |acknowledged| acknowledged,
                else => return error.InvalidTransactionSessionRecord,
            } else false;
            const repair_required = if (terminal_obj.get("repair_required")) |value| switch (value) {
                .bool => |required| required,
                else => return error.InvalidTransactionSessionRecord,
            } else false;
            const coordinator_table_name = if (coordinator_table_name_text) |value| try alloc.dupe(u8, value) else null;
            session.terminal_commit = .{
                .status = status,
                .repair_required = repair_required,
                .coordinator_group_id = coordinator_group_id,
                .coordinator_table_name = coordinator_table_name,
                .coordinator_acknowledged = coordinator_acknowledged,
                .commit_version = if (terminal_obj.get("commit_version")) |value| switch (value) {
                    .null => null,
                    .integer => |number| try nonNegativeRecordInteger(number),
                    .number_string => |text| try recordNumber(text),
                    else => return error.InvalidTransactionSessionRecord,
                } else null,
            };
            if (session.terminal_commit.?.commit_version == 0) return error.InvalidTransactionSessionRecord;
        }
    }
    if (obj.get("external_decision")) |value| if (value != .null) {
        var parsed_decision = std.json.parseFromValue(@import("../system_catalog/prepared_transactions.zig").Decision, alloc, value, .{}) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => return error.InvalidTransactionSessionRecord,
        };
        defer parsed_decision.deinit();
        session.external_decision = parsed_decision.value;
    };
    if ((session.external_preparation_prepared or session.external_receipt_acknowledged or session.external_decision != null) and session.external_preparation == null or
        session.external_preparation_prepared and (session.terminal_abort or session.terminal_commit != null) or
        session.external_receipt_acknowledged and !session.external_preparation_prepared and !session.terminal_abort and session.terminal_commit == null or
        session.terminal_abort and session.terminal_commit != null) return error.InvalidTransactionSessionRecord;
    if (obj.get("lake_cuts")) |cuts| try decodeReadSnapshotsInto(alloc, cuts, &session.lake_cuts);
    if (obj.get("read_snapshots")) |snapshots_value| {
        try decodeReadSnapshotsInto(alloc, snapshots_value, &session.read_snapshots);
    }
    if (obj.get("sequence_epoch")) |value| {
        session.sequence_epoch = switch (value) {
            .integer => |number| try nonNegativeRecordInteger(number),
            .number_string => |text| try recordNumber(text),
            else => return error.InvalidTransactionSessionRecord,
        };
        if (session.sequence_epoch == 0) return error.InvalidTransactionSessionRecord;
    }
    if (obj.get("sequence_state")) |value| {
        const bytes = try std.json.Stringify.valueAlloc(alloc, value, .{});
        defer alloc.free(bytes);
        var decoded = try std.json.parseFromSlice(@import("antfly_local_sources").sql_sequence_session.Snapshot, alloc, bytes, .{});
        defer decoded.deinit();
        session.sequence_state = try decoded.value.clone(alloc);
    }
    if (obj.get("setting_active")) |value| session.setting_active = try decodeSettingEntries(alloc, value);
    if (obj.get("setting_committed")) |value| session.setting_committed = try decodeSettingEntries(alloc, value);
    if (session.next_savepoint_id == 0) return error.InvalidTransactionSessionRecord;
    const savepoints_value = obj.get("savepoints") orelse return error.InvalidTransactionSessionRecord;
    const savepoints = switch (savepoints_value) {
        .array => |arr| arr,
        else => return error.InvalidTransactionSessionRecord,
    };
    for (savepoints.items) |entry| {
        const entry_obj = switch (entry) {
            .object => |value| value,
            else => return error.InvalidTransactionSessionRecord,
        };
        const id: u64 = switch (entry_obj.get("id") orelse return error.InvalidTransactionSessionRecord) {
            .integer => |v| try nonNegativeRecordInteger(v),
            .number_string => |text| try recordNumber(text),
            else => return error.InvalidTransactionSessionRecord,
        };
        if (id == 0 or id >= session.next_savepoint_id or session.savepoints.contains(id)) return error.InvalidTransactionSessionRecord;
        session.current_subtransaction = @max(session.current_subtransaction, id);
        const snapshot = try parseStoredCommitValue(alloc, entry_obj.get("snapshot") orelse return error.InvalidTransactionSessionRecord);
        var owned_snapshot = snapshot;
        var transferred = false;
        errdefer if (!transferred) owned_snapshot.deinit(alloc);
        const name: ?[]u8 = if (entry_obj.get("name")) |value| switch (value) {
            .null => null,
            .string => |text| if (text.len > 0 and text.len <= 63) try alloc.dupe(u8, text) else return error.InvalidTransactionSessionRecord,
            else => return error.InvalidTransactionSessionRecord,
        } else null;
        errdefer if (!transferred) if (name) |text| alloc.free(text);
        var read_snapshots: std.StringArrayHashMapUnmanaged(SessionReadSnapshot) = .empty;
        errdefer if (!transferred) deinitReadSnapshotMap(alloc, &read_snapshots);
        if (entry_obj.get("read_snapshots")) |read_snapshots_value| {
            try decodeReadSnapshotsInto(alloc, read_snapshots_value, &read_snapshots);
        }
        var setting_active = if (entry_obj.get("setting_active")) |value| try decodeSettingEntries(alloc, value) else SettingEntries.empty;
        errdefer if (!transferred) deinitSettingEntries(alloc, &setting_active);
        var setting_committed = if (entry_obj.get("setting_committed")) |value| try decodeSettingEntries(alloc, value) else SettingEntries.empty;
        errdefer if (!transferred) deinitSettingEntries(alloc, &setting_committed);
        const notification_count: usize = if (entry_obj.get("notification_count")) |value| std.math.cast(usize, switch (value) {
            .integer => |count| try nonNegativeRecordInteger(count),
            .number_string => |text| try recordNumber(text),
            else => return error.InvalidTransactionSessionRecord,
        }) orelse return error.InvalidTransactionSessionRecord else 0;
        if (notification_count > if (session.sql) |metadata| metadata.notifications.commands.items.len else @as(usize, 0)) return error.InvalidTransactionSessionRecord;
        const generation_count: usize = if (entry_obj.get("generation_count")) |value| std.math.cast(usize, switch (value) {
            .integer => |count| try nonNegativeRecordInteger(count),
            .number_string => |encoded| try recordNumber(encoded),
            else => return error.InvalidTransactionSessionRecord,
        }) orelse return error.InvalidTransactionSessionRecord else 0;
        if (generation_count > if (session.sql) |metadata| metadata.catalog_generations.entries.items.len else @as(usize, 0)) return error.InvalidTransactionSessionRecord;
        try session.savepoints.put(alloc, id, .{
            .id = id,
            .notification_count = notification_count,
            .generation_count = generation_count,
            .name = name,
            .snapshot = snapshot,
            .search_path_active = try decodeSearchPath(alloc, entry_obj.get("search_path_active")),
            .search_path_committed = try decodeSearchPath(alloc, entry_obj.get("search_path_committed")),
            .transaction_mode = if (entry_obj.get("transaction_mode")) |mode| switch (mode) {
                .null => null,
                .string => |text| std.meta.stringToEnum(@import("antfly_local_sources").sql_session.ReadMode, text) orelse return error.InvalidTransactionSessionRecord,
                else => return error.InvalidTransactionSessionRecord,
            } else null,
            .transaction_deferrable = if (entry_obj.get("transaction_deferrable")) |value| switch (value) {
                .null => null,
                .bool => |deferrable| deferrable,
                else => return error.InvalidTransactionSessionRecord,
            } else null,
            .read_snapshots = read_snapshots,
            .setting_active = setting_active,
            .setting_committed = setting_committed,
        });
        transferred = true;
    }
    // A private terminal proof is checked only after the complete marker,
    // decision and released input sets have been decoded.
    if (session.external_preparation) |bytes| {
        var scratch = std.heap.ArenaAllocator.init(alloc);
        defer scratch.deinit();
        const seal = try std.json.parseFromSliceLeaky(ExternalPreparationSeal, scratch.allocator(), bytes, .{});
        try seal.validate(scratch.allocator(), session, session.execution_plan);
    }
    const generation_entries = if (session.sql) |metadata| metadata.catalog_generations.entries.items else &.{};
    if (session.staged) |staged| try requireFrozenBirths(alloc, staged, txn_id, generation_entries);
    var points = session.savepoints.valueIterator();
    while (points.next()) |point| try requireFrozenBirths(alloc, point.snapshot, txn_id, generation_entries[0..point.generation_count]);
    return session;
}

/// Bind live births to the producer's statement postimages before merging it
/// into the session. A folded journal may also contain superseded births,
/// whose postimages were already replaced by later operations in the journal.
pub fn requireFrozenStatementImages(a: std.mem.Allocator, request: OwnedTransactionCommitRequest, folded: bool) !void {
    for (request.frozen_births.groups.items) |group| {
        const physical = "table:".* ++ std.fmt.bytesToHex(group.origin.physical, .lower);
        const table = for (request.tables) |table| {
            if (std.mem.eql(u8, request.physicalName(table.table_name), &physical)) break table;
        } else return error.InvalidTransactionSessionRecord;
        var writes: std.StringHashMapUnmanaged(*const db_mod.types.BatchWrite) = .empty;
        defer writes.deinit(a);
        for (table.batch.writes) |*write| {
            const slot = try writes.getOrPut(a, write.key);
            if (slot.found_existing) return error.InvalidTransactionSessionRecord;
            slot.value_ptr.* = write;
        }
        // Statement folding applies deletes after writes.
        for (table.batch.deletes) |key| _ = writes.remove(key);
        for (group.rows.items) |row| {
            if (row.superseded) {
                if (!folded) return error.InvalidTransactionSessionRecord;
                continue;
            }
            const write = writes.get(row.key) orelse return error.InvalidTransactionSessionRecord;
            if (!std.mem.eql(u8, row.document, write.value) or row.json_null_fields.len != write.json_null_fields.len)
                return error.InvalidTransactionSessionRecord;
            for (row.json_null_fields) |field| {
                for (write.json_null_fields) |written| {
                    if (std.mem.eql(u8, field, written)) break;
                } else return error.InvalidTransactionSessionRecord;
            }
        }
    }
}

fn requireFrozenBirths(backing: std.mem.Allocator, request: OwnedTransactionCommitRequest, transaction: [16]u8, entries: []const generation_intents.Entry) !void {
    if (request.frozen_births.rows == 0) return;
    var arena = std.heap.ArenaAllocator.init(backing);
    defer arena.deinit();
    const a = arena.allocator();
    for (request.frozen_births.groups.items) |group| {
        try group.origin.validate();
        if (!std.mem.eql(u8, &transaction, &group.origin.transaction)) return error.InvalidTransactionSessionRecord;
        const entry = for (entries) |entry| {
            if (std.mem.eql(u8, &entry.nonce, &group.origin.nonce) and std.mem.eql(u8, &entry.physical, &group.origin.physical)) break entry;
        } else return error.CatalogGenerationChanged;
        const prepared = try entry.prepared(a);
        const header = try std.json.parseFromSliceLeaky(struct { version: u32 = 0 }, a, prepared.child.schema_json, .{ .ignore_unknown_fields = true });
        if (header.version != group.origin.schema_version) return error.CatalogGenerationChanged;
        const binding = for (request.catalog_bindings.items) |binding| {
            if (std.mem.eql(u8, binding.physical, prepared.child.name)) break binding;
        } else return error.InvalidTransactionSessionRecord;
        // Retain normal lifetime custody: UNLOGGED/temporary history belongs
        // in the same process-local capsule as its current staged rows.
        if (!std.meta.eql(binding.sql_persistence, prepared.child.sql_persistence)) return error.CatalogGenerationChanged;
    }
}

fn principalsEqual(left: ?[]const u8, right: ?[]const u8) bool {
    if (left == null or right == null) return left == null and right == null;
    return std.mem.eql(u8, left.?, right.?);
}

fn optionalStringsEqual(left: ?[]const u8, right: ?[]const u8) bool {
    if (left == null or right == null) return left == null and right == null;
    return std.mem.eql(u8, left.?, right.?);
}

fn commitBodyDigest(
    alloc: std.mem.Allocator,
    req: ?*const OwnedTransactionCommitRequest,
) ![32]u8 {
    const encoded = if (req) |value| try encodeCommitRequest(alloc, value.*) else try alloc.dupe(u8, "null");
    defer alloc.free(encoded);
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(encoded, &digest, .{});
    return digest;
}

pub fn newSessionTxnId(owner_node_id: u64) db_mod.types.TxnId {
    var txn_id: db_mod.types.TxnId = undefined;
    const nonce = txn_id_nonce.fetchAdd(1, .monotonic);
    std.mem.writeInt(u64, txn_id[0..8], nonce, .big);
    std.mem.writeInt(u64, txn_id[8..16], nextTxnTimestamp(), .big);
    std.mem.writeInt(u64, txn_id[0..8], owner_node_id, .big);
    return txn_id;
}

fn parseVersionString(text: []const u8) !u64 {
    return try std.fmt.parseUnsigned(u64, text, 10);
}

fn nonNegativeRecordInteger(value: i64) !u64 {
    if (value < 0) return error.InvalidTransactionSessionRecord;
    return @intCast(value);
}

fn recordNumber(text: []const u8) !u64 {
    return std.fmt.parseUnsigned(u64, text, 10) catch error.InvalidTransactionSessionRecord;
}

fn requireString(obj: std.json.ObjectMap, key: []const u8) []const u8 {
    const value = obj.get(key) orelse return "";
    return switch (value) {
        .string => |s| s,
        else => "",
    };
}

fn appendJsonString(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), value: []const u8) !void {
    const encoded = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})});
    defer alloc.free(encoded);
    try out.appendSlice(alloc, encoded);
}

test "transaction commit parser keeps read set and table batches" {
    var req = try parseCommitRequest(std.testing.allocator,
        \\{
        \\  "read_set":[{"table":"docs","key":"doc:a","version":"7"}],
        \\  "tables":{"docs":{"inserts":{"doc:a":{"title":"alpha"}}}}
        \\}
    );
    defer req.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), req.read_set.len);
    try std.testing.expectEqual(@as(usize, 1), req.tables.len);
    try std.testing.expectEqual(@as(usize, 1), req.tables[0].predicates.items.len);
    try std.testing.expectEqualStrings("docs", req.tables[0].table_name);
}

test "transaction commit parser keeps table transforms" {
    var req = try parseCommitRequest(std.testing.allocator,
        \\{
        \\  "read_set":[],
        \\  "tables":{"docs":{"transforms":[{"key":"doc:a","operations":[{"op":"$set","path":"status","value":"updated"},{"op":"$min","path":"priority","value":2},{"op":"$max","path":"version","value":3}],"upsert":true}]}}
        \\}
    );
    defer req.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), req.tables.len);
    try std.testing.expectEqual(@as(usize, 1), req.tables[0].batch.transforms.len);
    try std.testing.expect(req.tables[0].batch.transforms[0].upsert);
    try std.testing.expectEqual(db_mod.types.TransformOpType.min, req.tables[0].batch.transforms[0].operations[1].op);
    try std.testing.expectEqual(db_mod.types.TransformOpType.max, req.tables[0].batch.transforms[0].operations[2].op);
}

test "multi batch parser accepts the public batch envelope without a read set" {
    var req = try parseMultiBatchRequest(std.testing.allocator,
        \\{
        \\  "tables":{
        \\    "docs":{"inserts":{"doc:a":{"title":"alpha"}}},
        \\    "audit":{"deletes":["event:old"]}
        \\  },
        \\  "sync_level":"write"
        \\}
    );
    defer req.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 0), req.read_set.len);
    try std.testing.expectEqual(@as(usize, 2), req.tables.len);
    try std.testing.expectEqual(db_mod.types.SyncLevel.write, req.sync_level);
    try std.testing.expectEqualStrings("docs", req.tables[0].table_name);
    try std.testing.expectEqual(@as(usize, 1), req.tables[0].batch.writes.len);
    try std.testing.expectEqualStrings("audit", req.tables[1].table_name);
    try std.testing.expectEqual(@as(usize, 1), req.tables[1].batch.deletes.len);
}

test "multi batch commit requirements survive transaction planning" {
    const a = std.testing.allocator;
    var gated = try parseMultiBatchRequest(a, "{\"tables\":{\"docs\":{\"inserts\":{\"a\":{}},\"commit_requires\":\"direct_enrichments\"}}}");
    defer gated.deinit(a);
    var distributed = try gated.distributedTables(a);
    defer distributed.deinit();
    try std.testing.expectEqual(db_mod.types.CommitRequires.direct_enrichments, distributed.tables[0].commit_requires.?);
    var no_gate = try parseCommitRequest(a, "{\"read_set\":[],\"tables\":{\"docs\":{\"commit_requires\":\"none\"}}}");
    defer no_gate.deinit(a);
}

test "multi batch parser derives strongest per-table sync level" {
    var req = try parseMultiBatchRequest(std.testing.allocator,
        \\{
        \\  "tables":{
        \\    "docs":{"inserts":{"doc:a":{"title":"alpha"}},"sync_level":"write"},
        \\    "audit":{"deletes":["event:old"],"sync_level":"full_index"}
        \\  }
        \\}
    );
    defer req.deinit(std.testing.allocator);
    try std.testing.expectEqual(db_mod.types.SyncLevel.full_index, req.sync_level);
}

test "multi batch parser rejects empty operation sets" {
    try std.testing.expectError(
        error.InvalidTransactionCommitRequest,
        parseMultiBatchRequest(std.testing.allocator, "{\"tables\":{\"docs\":{}}}"),
    );
}

test "multi batch parser rejects transaction-only read sets" {
    try std.testing.expectError(
        error.InvalidTransactionCommitRequest,
        parseMultiBatchRequest(std.testing.allocator,
            \\{"read_set":[],"tables":{"docs":{"deletes":["doc:a"]}}}
        ),
    );
}

test "transaction session registry begins and removes sessions" {
    var registry = SessionRegistry.init(null);
    defer registry.deinit(std.testing.allocator);
    const session = try registry.begin(std.testing.allocator, .{ .sync_level = .full_index }, 7);
    try std.testing.expect(registry.getInfo(session.txn_id) != null);
    try std.testing.expectEqual(db_mod.types.SyncLevel.full_index, registry.getInfo(session.txn_id).?.sync_level);
    try std.testing.expectEqual(@as(u64, 7), session.version.owner);
    try std.testing.expect(registry.remove(std.testing.allocator, session.txn_id));
    try std.testing.expect(registry.getInfo(session.txn_id) == null);
}

test "durable transaction sessions preserve and enforce principal bindings" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/txn-session-principal", .{tmp.sub_path});
    defer std.testing.allocator.free(path);
    const path_z = try std.testing.allocator.dupeSentinel(u8, path, 0);
    defer std.testing.allocator.free(path_z);

    var store = try docstore_mod.DocStore.open(std.testing.allocator, path_z, .{});
    defer store.close();
    var durable = DurableSessionStore.init(std.testing.allocator, &store);

    var alice_txn_id: db_mod.types.TxnId = undefined;
    {
        var writer = SessionRegistry.init(&durable);
        defer writer.deinit(std.testing.allocator);
        const alice = try writer.beginForPrincipal(
            std.testing.allocator,
            .{ .sync_level = .write },
            7,
            "user:alice",
        );
        alice_txn_id = alice.txn_id;
        _ = try writer.beginForPrincipal(
            std.testing.allocator,
            .{ .sync_level = .write },
            7,
            "user:bob",
        );
        _ = try writer.begin(std.testing.allocator, .{ .sync_level = .write }, 7);
    }

    var reader = SessionRegistry.init(&durable);
    defer reader.deinit(std.testing.allocator);
    try std.testing.expectEqual(
        SessionRegistry.PrincipalAccess.allowed,
        try reader.principalAccess(std.testing.allocator, alice_txn_id, "user:alice"),
    );
    try std.testing.expectEqual(
        SessionRegistry.PrincipalAccess.denied,
        try reader.principalAccess(std.testing.allocator, alice_txn_id, "user:bob"),
    );
    try std.testing.expectEqual(
        SessionRegistry.PrincipalAccess.denied,
        try reader.principalAccess(std.testing.allocator, alice_txn_id, null),
    );

    const alice_sessions = try reader.listStatusesForPrincipal(std.testing.allocator, "user:alice");
    defer std.testing.allocator.free(alice_sessions);
    try std.testing.expectEqual(@as(usize, 1), alice_sessions.len);
    try std.testing.expectEqualSlices(u8, &alice_txn_id, &alice_sessions[0].txn_id);

    const anonymous_sessions = try reader.listStatusesForPrincipal(std.testing.allocator, null);
    defer std.testing.allocator.free(anonymous_sessions);
    try std.testing.expectEqual(@as(usize, 1), anonymous_sessions.len);
}

test "durable session mutations publish only after persistence succeeds: HTTP SQL settings survive restart and savepoint rollback" {
    const alloc = std.testing.allocator;
    var backend = mem_backend.Backend.init(alloc, .{});
    defer backend.close();
    var store = try backend.runtimeStore(alloc, .{ .name = "system/api-sql-setting-sessions" });
    defer store.deinit();
    var durable = DurableSessionStore.initRuntime(alloc, &store);
    const Fixture = struct {
        generation: u64 = 1,
        const definitions = [_]setting_catalog.Definition{
            .{ .identity = .{ .id = 1, .generation = 1 }, .name = "app.limit", .kind = .integer, .session_writable = true, .default = .{ .integer = 3 } },
            .{ .identity = .{ .id = 2, .generation = 1 }, .name = "app.secret", .kind = .string, .policy_sensitive = true, .default = .{ .string = "hidden" } },
        };
        fn load(ptr: *anyopaque, allocator: std.mem.Allocator, scope: setting_catalog.Scope) !setting_catalog.RawSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (!std.mem.eql(u8, scope.principal, "alice") or !std.mem.eql(u8, scope.database, "main")) return error.Forbidden;
            var values = definitions;
            values[0].identity.generation = self.generation;
            // The snapshot is borrowed only until View.capture returns.
            return .{ .scope = scope, .epoch = self.generation, .definitions = try allocator.dupe(setting_catalog.Definition, &values) };
        }
    };
    var fixture: Fixture = .{};
    const owner: setting_catalog.Owner = .{ .ptr = &fixture, .load = Fixture.load };
    var id: db_mod.types.TxnId = undefined;
    {
        var registry = SessionRegistry.init(&durable);
        defer registry.deinit(alloc);
        const begun = try registry.beginForPrincipal(alloc, .{ .sql = .{ .database = "main", .namespace = "public", .isolation = .read_committed, .mode = .read_write } }, 7, "alice");
        id = begun.txn_id;
        const guard = try registry.mutation(alloc, id, 7);
        try std.testing.expectError(error.Forbidden, registry.setSqlSetting(alloc, guard, "bob", owner, "app.limit", .{ .integer = 5 }, false));
        try std.testing.expectError(error.SettingWriteForbidden, registry.setSqlSetting(alloc, guard, "alice", owner, "app.secret", .{ .string = "leak" }, false));
        try registry.setSqlSetting(alloc, guard, "alice", owner, "app.limit", .{ .integer = 5 }, false);
        const point = (try registry.createNamedSavepoint(alloc, guard, "before_local")).?;
        try registry.setSqlSetting(alloc, guard, "alice", owner, "app.limit", .{ .integer = 7 }, true);
        durable.fail_writes_for_test = true;
        try std.testing.expectError(error.InjectedSessionStoreFailure, registry.setSqlSetting(alloc, guard, "alice", owner, "app.limit", .{ .integer = 9 }, false));
        durable.fail_writes_for_test = false;
        var after_failure = (try registry.getSqlState(alloc, id)).?;
        defer after_failure.deinit(alloc);
        try std.testing.expectEqual(@as(i64, 7), after_failure.setting_active.items[0].value.integer);
        _ = try registry.rollbackToSavepoint(alloc, guard, point.savepoint_id);
    }
    {
        var registry = SessionRegistry.init(&durable);
        defer registry.deinit(alloc);
        const guard = try registry.mutation(alloc, id, 7);
        var resumed = (try registry.getSqlState(alloc, id)).?;
        defer resumed.deinit(alloc);
        try std.testing.expectEqual(@as(i64, 5), resumed.setting_active.items[0].value.integer);
        var view = try setting_catalog.View.capture(alloc, owner, .{ .principal = "alice", .database = "main" }, resumed.setting_active.items);
        defer view.deinit();
        try std.testing.expectEqual(@as(i64, 5), (try view.resolve("app.limit")).value.integer);
        try registry.setSqlSetting(alloc, guard, "alice", owner, "app.limit", .{ .integer = 8 }, false);
        try registry.resetSqlSetting(alloc, guard, "alice", owner, "app.limit");
        var reset_state = (try registry.getSqlState(alloc, id)).?;
        defer reset_state.deinit(alloc);
        try std.testing.expectEqual(@as(usize, 0), reset_state.setting_active.items.len);
        try registry.setSqlSetting(alloc, guard, "alice", owner, "app.limit", .{ .integer = 8 }, false);
        const before_reset_all = (try registry.createNamedSavepoint(alloc, guard, "before_reset_all")).?;
        try std.testing.expectError(error.Forbidden, registry.resetAllSqlSettings(alloc, guard, "bob"));
        try registry.resetAllSqlSettings(alloc, guard, "alice");
        var cleared_at_savepoint = (try registry.getSqlState(alloc, id)).?;
        defer cleared_at_savepoint.deinit(alloc);
        try std.testing.expectEqual(@as(usize, 0), cleared_at_savepoint.setting_active.items.len);
        var persisted_clear = (try durable.load(id)).?;
        defer persisted_clear.deinit(durable.alloc);
        try std.testing.expectEqual(@as(usize, 0), persisted_clear.setting_committed.items.len);
        _ = try registry.rollbackToSavepoint(alloc, guard, before_reset_all.savepoint_id);
        durable.fail_writes_for_test = true;
        try std.testing.expectError(error.InjectedSessionStoreFailure, registry.resetAllSqlSettings(alloc, guard, "alice"));
        durable.fail_writes_for_test = false;
        var after_reset_failure = (try registry.getSqlState(alloc, id)).?;
        defer after_reset_failure.deinit(alloc);
        try std.testing.expectEqual(@as(i64, 8), after_reset_failure.setting_active.items[0].value.integer);
        fixture.generation = 2;
        try std.testing.expectError(error.SettingCatalogChanged, setting_catalog.View.capture(alloc, owner, .{ .principal = "alice", .database = "main" }, resumed.setting_active.items));
        try std.testing.expectError(error.SettingCatalogChanged, registry.setSqlSetting(alloc, guard, "alice", owner, "app.limit", .{ .integer = 9 }, false));
        try std.testing.expectError(error.SettingCatalogChanged, registry.resetSqlSetting(alloc, guard, "alice", owner, "app.limit"));
        // Removing stale client overlays does not need a catalog round trip.
        try registry.resetAllSqlSettings(alloc, guard, "alice");
    }
    {
        var registry = SessionRegistry.init(&durable);
        defer registry.deinit(alloc);
        var resumed = (try registry.getSqlState(alloc, id)).?;
        defer resumed.deinit(alloc);
        try std.testing.expectEqual(@as(usize, 0), resumed.setting_active.items.len);
        var persisted_clear = (try durable.load(id)).?;
        defer persisted_clear.deinit(durable.alloc);
        try std.testing.expectEqual(@as(usize, 0), persisted_clear.setting_committed.items.len);
        var view = try setting_catalog.View.capture(alloc, owner, .{ .principal = "alice", .database = "main" }, resumed.setting_active.items);
        defer view.deinit();
        try std.testing.expectEqual(@as(i64, 3), (try view.resolve("app.limit")).value.integer);
    }
}

test "durable SQL LOCAL DEFAULT preserves committed setting across restart and savepoint" {
    const alloc = std.testing.allocator;
    var backend = mem_backend.Backend.init(alloc, .{});
    defer backend.close();
    var store = try backend.runtimeStore(alloc, .{ .name = "system/api-sql-local-default-sessions" });
    defer store.deinit();
    var durable = DurableSessionStore.initRuntime(alloc, &store);
    const Fixture = struct {
        const definition: setting_catalog.Definition = .{ .identity = .{ .id = 1, .generation = 1 }, .name = "app.limit", .kind = .integer, .session_writable = true, .default = .{ .integer = 3 } };
        fn load(_: *anyopaque, _: std.mem.Allocator, scope: setting_catalog.Scope) !setting_catalog.RawSnapshot {
            return .{ .scope = scope, .epoch = 1, .definitions = &.{definition} };
        }
    };
    var marker: u8 = 0;
    const owner: setting_catalog.Owner = .{ .ptr = &marker, .load = Fixture.load };
    var id: db_mod.types.TxnId = undefined;
    var point_id: u64 = undefined;
    {
        var registry = SessionRegistry.init(&durable);
        defer registry.deinit(alloc);
        const begun = try registry.beginForPrincipal(alloc, .{ .sql = .{ .database = "main", .namespace = "public", .isolation = .read_committed, .mode = .read_write } }, 7, "alice");
        id = begun.txn_id;
        const guard = try registry.mutation(alloc, id, 7);
        try registry.setSqlSetting(alloc, guard, "alice", owner, "app.limit", .{ .integer = 5 }, false);
        point_id = (try registry.createNamedSavepoint(alloc, guard, "before_default")).?.savepoint_id;
        durable.fail_writes_for_test = true;
        try std.testing.expectError(error.InjectedSessionStoreFailure, registry.resetLocalSqlSetting(alloc, guard, "alice", owner, "app.limit"));
        durable.fail_writes_for_test = false;
        var before = (try registry.getSqlState(alloc, id)).?;
        defer before.deinit(alloc);
        try std.testing.expectEqual(@as(i64, 5), before.setting_active.items[0].value.integer);
        try registry.resetLocalSqlSetting(alloc, guard, "alice", owner, "app.limit");
        var state = (try registry.getSqlState(alloc, id)).?;
        defer state.deinit(alloc);
        try std.testing.expectEqual(@as(usize, 0), state.setting_active.items.len);
        var persisted = (try durable.load(id)).?;
        defer persisted.deinit(durable.alloc);
        try std.testing.expectEqual(@as(i64, 5), persisted.setting_committed.items[0].value.integer);
    }
    {
        var registry = SessionRegistry.init(&durable);
        defer registry.deinit(alloc);
        const guard = try registry.mutation(alloc, id, 7);
        var state = (try registry.getSqlState(alloc, id)).?;
        defer state.deinit(alloc);
        try std.testing.expectEqual(@as(usize, 0), state.setting_active.items.len);
        _ = try registry.rollbackToSavepoint(alloc, guard, point_id);
        var restored = (try registry.getSqlState(alloc, id)).?;
        defer restored.deinit(alloc);
        try std.testing.expectEqual(@as(i64, 5), restored.setting_active.items[0].value.integer);
        var persisted = (try durable.load(id)).?;
        defer persisted.deinit(durable.alloc);
        try std.testing.expectEqual(@as(i64, 5), persisted.setting_committed.items[0].value.integer);
    }
}

test "durable session mutations publish only after persistence succeeds" {
    var backend = mem_backend.Backend.init(std.testing.allocator, .{});
    defer backend.close();
    var store = try backend.runtimeStore(std.testing.allocator, .{ .name = "system/api-transaction-sessions" });
    defer store.deinit();
    var durable = DurableSessionStore.initRuntime(std.testing.allocator, &store);
    var registry = SessionRegistry.init(&durable);
    defer registry.deinit(std.testing.allocator);

    durable.fail_writes_for_test = true;
    try std.testing.expectError(
        error.InjectedSessionStoreFailure,
        registry.begin(std.testing.allocator, .{ .sync_level = .write }, 1),
    );
    try std.testing.expectEqual(@as(usize, 0), registry.sessions.count());

    durable.fail_writes_for_test = false;
    const session = try registry.begin(std.testing.allocator, .{ .sync_level = .write }, 1);
    var stage_req = try parseStageWriteRequest(std.testing.allocator, "{\"table\":\"docs\",\"key\":\"doc:a\",\"document\":{\"title\":\"must-not-publish\"}}");
    defer stage_req.deinit(std.testing.allocator);

    durable.fail_writes_for_test = true;
    try std.testing.expectError(
        error.InjectedSessionStoreFailure,
        registry.stage(std.testing.allocator, try registry.mutation(std.testing.allocator, session.txn_id, session.version.owner), &stage_req),
    );
    const details = (try registry.getDetails(std.testing.allocator, session.txn_id)).?;
    defer {
        var owned = details;
        owned.deinit(std.testing.allocator);
    }
    try std.testing.expectEqual(@as(usize, 0), details.status.staged_write_count);
}

test "transaction commit execution yields through caller io and preserves ownership on cancellation" {
    var registry = SessionRegistry.init(null);
    defer registry.deinit(std.testing.allocator);
    const txn_id = try distributed_txn.parseTxnIdHex("00112233445566778899aabbccddeeff");
    const Wait = struct {
        held: ?SessionRegistry.CommitExecution,
        cancel: bool = true,
        calls: usize = 0,

        fn sleep(ptr: ?*anyopaque, _: std.Io.Timeout) std.Io.Cancelable!void {
            const self: *@This() = @ptrCast(@alignCast(ptr.?));
            self.calls += 1;
            if (self.cancel) return error.Canceled;
            self.held.?.release();
            self.held = null;
        }
    };
    var waiter = Wait{ .held = try registry.acquireCommitExecution(txn_id, std.testing.io) };
    defer if (waiter.held) |held| held.release();
    var vtable = std.testing.io.vtable.*;
    vtable.sleep = Wait.sleep;
    const io: std.Io = .{ .userdata = &waiter, .vtable = &vtable };
    try std.testing.expectError(error.Canceled, registry.acquireCommitExecution(txn_id, io));
    try std.testing.expect(registry.tryAcquireCommitExecution(txn_id) == null);
    waiter.cancel = false;
    const next = try registry.acquireCommitExecution(txn_id, io);
    try std.testing.expectEqual(@as(usize, 2), waiter.calls);
    try std.testing.expect(registry.tryAcquireCommitExecution(txn_id) == null);
    next.release();
    const recovery = registry.tryAcquireCommitExecution(txn_id) orelse return error.TestUnexpectedResult;
    recovery.release();
}

test "durable transaction sessions retain terminal commit coordinator handoff" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/txn-session-terminal", .{tmp.sub_path});
    defer std.testing.allocator.free(path);
    const path_z = try std.testing.allocator.dupeSentinel(u8, path, 0);
    defer std.testing.allocator.free(path_z);

    var store = try docstore_mod.DocStore.open(std.testing.allocator, path_z, .{});
    defer store.close();
    var durable = DurableSessionStore.init(std.testing.allocator, &store);

    var txn_id: db_mod.types.TxnId = undefined;
    {
        var writer = SessionRegistry.init(&durable);
        defer writer.deinit(std.testing.allocator);
        const session = try writer.begin(std.testing.allocator, .{ .sync_level = .write }, 7);
        txn_id = session.txn_id;
        try std.testing.expect((try writer.recordTerminalCommitWithRepair(
            std.testing.allocator,
            txn_id,
            .committed_visibility_pending,
            true,
            7001,
            "docs",
        )) != null);
    }

    var reader = SessionRegistry.init(&durable);
    defer reader.deinit(std.testing.allocator);
    var terminal = (try reader.getTerminalCommit(std.testing.allocator, txn_id)).?;
    defer terminal.deinit(std.testing.allocator);
    try std.testing.expectEqual(TerminalCommitStatus.committed_visibility_pending, terminal.status);
    try std.testing.expect(terminal.repair_required);
    try std.testing.expectEqual(@as(?u64, 7001), terminal.coordinator_group_id);
    try std.testing.expectEqualStrings("docs", terminal.coordinator_table_name.?);
    try std.testing.expect(!terminal.coordinator_acknowledged);
    try std.testing.expectError(error.TransactionCoordinatorMismatch, reader.recordTerminalCommit(
        std.testing.allocator,
        txn_id,
        .committed,
        7002,
        "docs",
    ));
    try std.testing.expectEqual(@as(usize, 0), try reader.cleanupExpired(std.testing.allocator, std.math.maxInt(u64)));
    try std.testing.expect((try reader.markTerminalCoordinatorAcknowledged(std.testing.allocator, txn_id)) != null);
    var acknowledged = (try reader.getTerminalCommit(std.testing.allocator, txn_id)).?;
    defer acknowledged.deinit(std.testing.allocator);
    try std.testing.expect(acknowledged.coordinator_acknowledged);
    try std.testing.expectEqual(@as(usize, 1), try reader.cleanupExpired(std.testing.allocator, std.math.maxInt(u64)));
}

test "distributed txn graph admission and recovered abort retain a compact principal-bound receipt" {
    const a = std.testing.allocator;
    var backend = mem_backend.Backend.init(a, .{});
    defer backend.close();
    var store = try backend.runtimeStore(a, .{ .name = "system/api-transaction-sessions" });
    defer store.deinit();
    var durable = DurableSessionStore.initRuntime(a, &store);
    const leases = SessionLeaseStore.initFromDurable(&durable);
    const id: db_mod.types.TxnId = @splat(9);
    var request = try parseCommitRequest(a, "{\"read_set\":[],\"tables\":{\"docs\":{\"inserts\":{\"a\":{}}}}}");
    defer request.deinit(a);
    const tables = [_]distributed_txn.TableCommitRequest{.{ .table_name = "docs", .normalized_graph_rows = true }};
    {
        var registry = SessionRegistry.initWithOptions(&durable, leases, 60 * std.time.ns_per_s, null, 1, 1);
        defer registry.deinit(a);
        try std.testing.expectError(error.SessionRecordTooLarge, registry.beginGraphExecution(a, id, 1, "alice", &request, &tables));
        try std.testing.expect((try durable.load(id)) == null);
        registry.max_record_bytes = null;
        durable.fail_lease_transition_after_session_write_for_test = true;
        try std.testing.expectError(error.InjectedLeaseTransitionFailure, registry.beginGraphExecution(a, id, 1, "alice", &request, &tables));
        durable.fail_lease_transition_after_session_write_for_test = false;
        try std.testing.expect((try leases.load(a, id)) == null);
        try std.testing.expect((try durable.load(id)) == null);
        const result = try registry.beginGraphExecution(a, id, 1, "alice", &request, &tables);
        defer result.plan.deinit();
        try std.testing.expect(result.plan.firstAttempt());
        try std.testing.expectEqualDeep(try registry.mutation(a, id, 1), result.plan.publication.?.guard);
        // Restart from the previous wire format, which identified graph
        // writes only by the normalized rows marker in the execution plan.
        const key = try makeSessionKey(a, id);
        defer a.free(key);
        var txn = try store.beginWrite();
        errdefer txn.abort();
        const raw = try txn.get(key);
        const legacy = try std.mem.replaceOwned(u8, a, raw, "\"graph_receipt\":true,", "");
        defer a.free(legacy);
        try std.testing.expect(legacy.len < raw.len);
        try txn.put(key, legacy);
        try txn.commit();
    }
    {
        var restarted = SessionRegistry.initWithLeaseTtl(&durable, leases, 60 * std.time.ns_per_s);
        defer restarted.deinit(a);
        var work = (try restarted.claimPendingRecovery(a, id, 1, nextTxnTimestamp())).?;
        defer work.deinit(a);
        try std.testing.expect(work.commit.execution_plan != null);
        try restarted.publishAbortAndRetire(a, id, null);
    }
    var reader = SessionRegistry.init(&durable);
    defer reader.deinit(a);
    var saved = (try durable.load(id)).?;
    defer saved.deinit(a);
    try std.testing.expect(saved.terminal_abort and saved.execution_plan == null and saved.staged == null);
    try std.testing.expectEqual(SessionRegistry.PrincipalAccess.denied, try reader.principalAccess(a, id, "bob"));
    try std.testing.expect((try reader.getStatusForPrincipal(a, id, "bob")) == null);
    try std.testing.expect((try reader.getDetailsForPrincipal(a, id, "bob")) == null);
    var details = (try reader.getDetailsForPrincipal(a, id, "alice")).?;
    defer details.deinit(a);
    try std.testing.expect(details.graph_receipt);
    try std.testing.expectEqual(SessionDisposition.aborted, details.status.disposition);
    try std.testing.expectEqual(@as(usize, 1), details.status.staged_write_count);
    try std.testing.expectEqualStrings("docs", details.tables[0].table_name);
    try std.testing.expectEqual(@as(usize, 1), details.tables[0].staged_write_count);
    try std.testing.expectError(error.CypherWriteRequestAlreadyUsed, reader.beginGraphExecution(a, id, 2, "bob", &request, &tables));
    try std.testing.expectEqual(@as(usize, 1), try reader.cleanupExpired(a, std.math.maxInt(u64)));
    try std.testing.expect((try durable.load(id)) == null);
}

test "distributed txn public mutation guards reject a reused id" {
    const a = std.testing.allocator;
    var registry = SessionRegistry.init(null);
    defer registry.deinit(a);
    const id: db_mod.types.TxnId = @splat(7);
    const original = try registry.beginForPrincipalWithSettingsAndId(a, .{}, 1, "alice", &.{}, null, id);
    var guard = try registry.mutation(a, id, 1);
    guard.bindPublicSession(original.begin_timestamp, "alice");
    try std.testing.expect((try registry.readMutationInfo(guard)) != null);
    try std.testing.expectEqual(@as(usize, 1), try registry.cleanupExpired(a, std.math.maxInt(u64)));
    _ = try registry.beginForPrincipalWithSettingsAndId(a, .{}, 1, "bob", &.{}, null, id);
    try std.testing.expectError(error.SessionLeaseLost, registry.readMutationInfo(guard));
    try std.testing.expectError(error.SessionLeaseLost, registry.getTerminalCommitForMutation(a, guard));
}

test "distributed txn shared SQL pin issuer seals remain independent" {
    const a = std.testing.allocator;
    var backend = mem_backend.Backend.init(a, .{});
    defer backend.close();
    var store = try backend.runtimeStore(a, .{ .name = "shared-sql-pin-fence" });
    defer store.deinit();
    var first = DurableSessionStore.initRuntime(a, &store);
    var second = DurableSessionStore.initRuntime(a, &store);
    try first.bindRegistrationIssuer(1);
    try second.bindRegistrationIssuer(2);
    const seed: [8]u8 = @splat(3);
    const missing = "01010101010101010101010101010101";
    const pending = try first.createRegistration(seed, .{ .transaction_id = missing });
    try std.testing.expectError(error.CompletionIssuerNotDrained, first.sealSharedLegacySqlPinIssuer(1));
    try first.removeRegistration(pending);
    try first.sealSharedLegacySqlPinIssuer(1);
    var reopened = DurableSessionStore.initRuntime(a, &store);
    try reopened.bindRegistrationIssuer(1);
    try std.testing.expectError(error.SqlTransactionOutcomeUnknown, reopened.createRegistration(seed, .{ .transaction_id = missing }));
    _ = try second.createRegistration(seed, .{ .transaction_id = missing });
}

test "distributed txn standalone SQL pin fence drains and persists across issuer restart" {
    const a = std.testing.allocator;
    var backend = mem_backend.Backend.init(a, .{});
    defer backend.close();
    var store = try backend.runtimeStore(a, .{ .name = "standalone-sql-pin-fence" });
    defer store.deinit();
    var durable = DurableSessionStore.initRuntime(a, &store);
    var registry = SessionRegistry.init(&durable);
    defer registry.deinit(a);
    const id = (try registry.begin(a, .{}, 0)).txn_id;
    const encoded = std.fmt.bytesToHex(id, .lower);
    const seed: [8]u8 = @splat(3);
    const legacy = try durable.createRegistration(seed, .{ .transaction_id = &encoded });
    try std.testing.expectError(error.CompletionIssuerNotDrained, durable.sealStandaloneLegacySqlPins());
    try durable.removeRegistration(legacy);
    try durable.reserveRegistrations(1);
    try std.testing.expectError(error.CompletionIssuerNotDrained, durable.sealStandaloneLegacySqlPins());
    durable.releaseRegistrations(1);
    const old_marker: db_mod.types.TxnId = @splat(8);
    {
        var write = try store.beginWrite();
        errdefer write.abort();
        try durable.putSqlAbortProof(&write, old_marker);
        try write.commit();
    }
    try std.testing.expectEqual(@as(usize, 0), try durable.reclaimStandaloneSqlAbortProofs());
    try durable.sealStandaloneLegacySqlPins();

    var reopened = DurableSessionStore.initRuntime(a, &store);
    try std.testing.expectEqual(@as(usize, 1), try reopened.reclaimStandaloneSqlAbortProofs());
    try std.testing.expect(!try reopened.completionSettled(old_marker));
    const missing: db_mod.types.TxnId = @splat(7);
    {
        var write = try store.beginWrite();
        errdefer write.abort();
        try reopened.putSqlAbortProof(&write, missing);
        try write.commit();
    }
    try std.testing.expect(!try reopened.completionSettled(missing));
    try std.testing.expectError(error.CompletionUnavailable, reopened.pinCompletion(id, @splat(4)));
    const missing_hex = std.fmt.bytesToHex(missing, .lower);
    try std.testing.expectError(error.SqlTransactionOutcomeUnknown, reopened.createRegistration(seed, .{ .transaction_id = &missing_hex }));
    const atomic = try reopened.createRegistration(seed, .{ .transaction_id = &encoded });
    try std.testing.expectEqual(@as(u64, 2), @import("completion_retirement.zig").sequence(atomic));
    try reopened.sealStandaloneLegacySqlPins();
    var registration = (try reopened.loadRegistration(a, atomic)).?;
    defer registration.deinit();
    try std.testing.expect(registration.value.phase == .registered);
    try std.testing.expect(try reopened.completionReferenceSettled(id, atomic));
    try reopened.pinCompletion(id, atomic);
}

test "distributed txn backfills and expires tagged terminal sessions" {
    const a = std.testing.allocator;
    var backend = mem_backend.Backend.init(a, .{});
    defer backend.close();
    var store = try backend.runtimeStore(a, .{ .name = "tagged-terminal-index" });
    defer store.deinit();
    var durable = DurableSessionStore.initRuntime(a, &store);
    var registry = SessionRegistry.init(&durable);
    registry.durable_scope = .cluster_shared;
    defer registry.deinit(a);
    const id = (try registry.begin(a, .{ .prepared_completion_only = true, .sql = .{ .database = "main", .namespace = "public", .isolation = .read_committed, .mode = .read_write } }, 0)).txn_id;
    var session = (try durable.load(id)).?;
    defer session.deinit(a);
    session.terminal_abort = true;
    session.commit_execution_started = true;
    session.sql_abort_acknowledgement = .row_policy_denied;
    try durable.saveForTest(session, null);
    {
        var write = try store.beginWrite();
        errdefer write.abort();
        try durable.deleteTaggedSqlExpiryKeyTxn(&write, id, session.last_touched_timestamp);
        try write.commit();
    }
    const decision = (try durable.sqlCommitDecisionStatus(a, id, null, "")).?;
    try std.testing.expectEqual(SqlAbortAcknowledgement.row_policy_denied, decision.abort_acknowledgement);
    try std.testing.expectEqual(@as(usize, 0), try registry.cleanupExpiredTaggedSqlDecisions(a, std.math.maxInt(u64)));
    try registry.migrateLegacySqlAbortReceipts(a);
    try std.testing.expectEqual(@as(usize, 1), try registry.cleanupExpiredTaggedSqlDecisions(a, std.math.maxInt(u64)));
}

test "distributed txn tagged expiry advances past a corrupt completion directory" {
    const a = std.testing.allocator;
    var backend = mem_backend.Backend.init(a, .{});
    defer backend.close();
    var store = try backend.runtimeStore(a, .{ .name = "tagged-terminal-corrupt-dependency" });
    defer store.deinit();
    var durable = DurableSessionStore.initRuntime(a, &store);
    var registry = SessionRegistry.init(&durable);
    defer registry.deinit(a);
    var ids: [3]db_mod.types.TxnId = undefined;
    for (&ids, 0..) |*id, index| {
        id.* = (try registry.begin(a, .{ .prepared_completion_only = true, .sql = .{
            .database = "main",
            .namespace = "public",
            .isolation = .read_committed,
            .mode = .read_write,
        } }, 0)).txn_id;
        var session = (try durable.load(id.*)).?;
        defer session.deinit(a);
        session.last_touched_timestamp = @intCast(index + 1);
        session.terminal_abort = true;
        session.commit_execution_started = true;
        try durable.saveForTest(session, null);
    }
    const bad = try completionDirectoryKey(a, ids[1]);
    defer a.free(bad);
    {
        var write = try store.beginWrite();
        errdefer write.abort();
        try write.put(bad, "[");
        try write.commit();
    }
    try std.testing.expectEqual(@as(usize, 1), try registry.cleanupExpiredTaggedSqlDecisions(a, 4));
    try std.testing.expectEqual(@as(usize, 0), try registry.cleanupExpiredTaggedSqlDecisions(a, 4));
    try std.testing.expectEqual(@as(usize, 1), try registry.cleanupExpiredTaggedSqlDecisions(a, 4));
    try std.testing.expect((try durable.load(ids[0])) == null);
    var retained = (try durable.load(ids[1])).?;
    defer retained.deinit(a);
    try std.testing.expect(retained.terminal_abort);
    try std.testing.expect((try durable.load(ids[2])) == null);
}

test "distributed txn migrates legacy detached SQL aborts out of live quota" {
    const a = std.testing.allocator;
    var backend = mem_backend.Backend.init(a, .{});
    defer backend.close();
    var store = try backend.runtimeStore(a, .{ .name = "legacy-sql-aborts" });
    defer store.deinit();
    var durable = DurableSessionStore.initRuntime(a, &store);
    var registry = SessionRegistry.init(&durable);
    registry.max_sessions = 1;
    defer registry.deinit(a);
    const id: db_mod.types.TxnId = @splat(5);
    _ = try registry.beginForPrincipalWithSettingsAndId(a, .{}, 1, "alice", &.{}, null, id);
    var old = (try durable.load(id)).?;
    defer old.deinit(a);
    old.connection_id = @splat('a');
    old.terminal_abort = true;
    old.commit_execution_started = true;
    try durable.saveForTest(old, null);
    try std.testing.expectError(error.SessionLimitExceeded, registry.begin(a, .{}, 1));
    registry.durable_scope = .cluster_shared;
    try registry.migrateLegacySqlAbortReceipts(a);
    try std.testing.expectEqual(SessionDisposition.aborted, (try registry.getStatusForPrincipal(a, id, "alice")).?.disposition);
    registry.durable_scope = .node_local;
    registry.sql_abort_migration_rescan_ns = 0;

    const bad_key = try makeSessionKey(a, @splat(4));
    defer a.free(bad_key);
    {
        var write = try store.beginWrite();
        errdefer write.abort();
        var oversized_key: [1024]u8 = @splat('0');
        @memcpy(oversized_key[0..session_prefix.len], session_prefix);
        try write.put(&oversized_key, "{}");
        try write.put(bad_key, "{\"c1\":\"bad\"}");
        try write.commit();
    }
    try registry.migrateLegacySqlAbortReceipts(a);
    try std.testing.expect(registry.sql_abort_migration_rescan_ns != 0);
    try std.testing.expect((try durable.load(id)) == null);
    try std.testing.expectEqual(SessionDisposition.aborted, (try registry.getStatusForPrincipal(a, id, "alice")).?.disposition);
    _ = try registry.begin(a, .{}, 1);

    const invalid_id: db_mod.types.TxnId = @splat(0);
    const invalid_receipt_key = try makeSqlAbortReceiptKey(a, invalid_id);
    defer a.free(invalid_receipt_key);
    const invalid_expiry_key = try makeExpiryKey(a, sql_abort_expiry_prefix, 1, invalid_id);
    defer a.free(invalid_expiry_key);
    {
        var write = try store.beginWrite();
        errdefer write.abort();
        try write.put(invalid_receipt_key, "{}");
        try write.put(invalid_expiry_key, &.{});
        try write.commit();
    }
    try std.testing.expectEqual(@as(usize, 1), try registry.cleanupExpiredSqlAbortReceipts(a, std.math.maxInt(u64)));
    try std.testing.expect((try registry.getStatusForPrincipal(a, id, "alice")) == null);
    try std.testing.expect(!try durable.completionSettled(invalid_id));
}

test "distributed txn listing summary preserves status, table permissions and a matching co-located lease" {
    const a = std.testing.allocator;
    var backend = mem_backend.Backend.init(a, .{});
    defer backend.close();
    var store = try backend.runtimeStore(a, .{ .name = "system/api-transaction-sessions" });
    defer store.deinit();
    var durable = DurableSessionStore.initRuntime(a, &store);
    const leases = SessionLeaseStore.initFromDurable(&durable);
    var registry = SessionRegistry.initWithLeaseTtl(&durable, leases, 60 * std.time.ns_per_s);
    defer registry.deinit(a);
    const id: db_mod.types.TxnId = @splat(6);
    _ = try registry.beginForPrincipalWithSettingsAndId(a, .{}, 1, "alice", &.{}, null, id);
    var staged = try parseCommitRequest(a, "{\"read_set\":[],\"tables\":{\"docs\":{\"inserts\":{\"one\":{\"value\":1}}}}}");
    defer staged.deinit(a);
    _ = (try registry.stage(a, try registry.mutation(a, id, 1), &staged)).?;
    _ = (try registry.createSavepoint(a, try registry.mutation(a, id, 1))).?;
    const key = try makeSessionLeaseKey(a, id);
    defer a.free(key);
    for ([_]u64{ 2, 1 }) |owner| {
        const owner_id = try ownerLeaseId(a, owner);
        defer a.free(owner_id);
        const value = try std.json.Stringify.valueAlloc(a, lease_mod.LeaseRecord{ .owner_id = owner_id, .epoch = 1, .expires_at_ms = 7 }, .{});
        defer a.free(value);
        {
            var write = try store.beginWrite();
            errdefer write.abort();
            try write.put(key, value);
            try write.commit();
        }
        const status = (try registry.getStatusForPrincipal(a, id, "alice")).?;
        try std.testing.expectEqual(@as(u64, 1), status.owner_node_id);
        try std.testing.expectEqual(if (owner == 1) leaseExpiryNs(7) else 0, status.lease_expires_at);
        var details = (try registry.getDetailsForPrincipal(a, id, "alice")).?;
        defer details.deinit(a);
        try std.testing.expectEqual(status, details.status);
        try std.testing.expectEqual(@as(usize, 1), details.savepoint_ids.len);
        var listing = (try registry.getListingDetailsForPrincipal(a, id, "alice")).?;
        defer listing.deinit(a);
        try std.testing.expectEqual(status, listing.status);
        try std.testing.expectEqual(@as(usize, 1), listing.tables.len);
        try std.testing.expectEqualStrings("docs", listing.tables[0].table_name);
        try std.testing.expectEqual(@as(usize, 1), listing.tables[0].staged_write_count);
        try std.testing.expectEqual(@as(usize, 0), listing.savepoint_ids.len);
        var authorization = (try registry.getAuthorizationDetailsForPrincipal(a, id, "alice")).?;
        defer authorization.deinit(a);
        try std.testing.expectEqual(status.begin_timestamp, authorization.status.begin_timestamp);
        try std.testing.expectEqual(@as(u64, 0), authorization.status.lease_expires_at);
    }
    try std.testing.expect((try registry.getStatusForPrincipal(a, id, "bob")) == null);
    try std.testing.expect((try registry.getDetailsForPrincipal(a, id, "bob")) == null);
    try std.testing.expect((try registry.getAuthorizationDetailsForPrincipal(a, id, "bob")) == null);
}

test "distributed txn legacy recovery counts malformed cursor rows" {
    const a = std.testing.allocator;
    var backend = mem_backend.Backend.init(a, .{});
    defer backend.close();
    var store = try backend.runtimeStore(a, .{ .name = "system/api-transaction-sessions" });
    defer store.deinit();
    var durable = DurableSessionStore.initRuntime(a, &store);
    {
        var write = try store.beginWrite();
        errdefer write.abort();
        try write.put(session_prefix ++ "!", "{}");
        try write.put(session_prefix ++ "!!", "{}");
        try write.commit();
    }
    const first = try durable.scanLegacyRecoveryIds(a, null, 1);
    defer a.free(first.ids);
    defer a.free(first.next_after.?);
    try std.testing.expectEqualStrings(session_prefix ++ "!", first.next_after.?);
    const second = try durable.scanLegacyRecoveryIds(a, first.next_after, 1);
    defer a.free(second.ids);
    defer a.free(second.next_after.?);
    try std.testing.expectEqualStrings(session_prefix ++ "!!", second.next_after.?);
}

test "distributed txn indexed recovery prunes a missing receipt marker" {
    const a = std.testing.allocator;
    var backend = mem_backend.Backend.init(a, .{});
    defer backend.close();
    var store = try backend.runtimeStore(a, .{ .name = "system/api-transaction-sessions" });
    defer store.deinit();
    var durable = DurableSessionStore.initRuntime(a, &store);
    var registry = SessionRegistry.init(&durable);
    defer registry.deinit(a);
    const id: db_mod.types.TxnId = @splat(8);
    const key = try makeSessionRecoveryKey(a, id);
    defer a.free(key);
    {
        var txn = try store.beginWrite();
        errdefer txn.abort();
        try txn.put(key, &.{});
        try txn.commit();
    }
    try std.testing.expect((try registry.claimPendingRecovery(a, id, 1, nextTxnTimestamp())) == null);
    const page = try durable.scanRecoveryIds(a, null, 1);
    defer a.free(page.ids);
    try std.testing.expectEqual(@as(usize, 0), page.ids.len);
}

test "distributed txn sealed recovery rejects unsupported durable formats" {
    const a = std.testing.allocator;
    const id: db_mod.types.TxnId = @splat(0);
    const body =
        \\{"session_format":999,"owner_node_id":1,"begin_timestamp":1,"sync_level":"write","commit_execution_started":true,"commit_body_digest":"0000000000000000000000000000000000000000000000000000000000000000","execution_plan":"[]"}
    ;
    try std.testing.expectError(error.InvalidTransactionSessionRecord, decodeOwnedSealedRecovery(a, id, body, 1));
}

test "distributed txn graph admission reserves terminal and acknowledgement capacity" {
    const a = std.testing.allocator;
    var backend = mem_backend.Backend.init(a, .{});
    defer backend.close();
    var store = try backend.runtimeStore(a, .{ .name = "system/api-transaction-sessions" });
    defer store.deinit();
    var durable = DurableSessionStore.initRuntime(a, &store);
    const leases = SessionLeaseStore.initFromDurable(&durable);
    var registry = SessionRegistry.initWithLeaseTtl(&durable, leases, 60 * std.time.ns_per_s);
    defer registry.deinit(a);
    var request = try parseCommitRequest(a, "{\"read_set\":[],\"tables\":{\"docs\":{\"inserts\":{\"a\":{}}}}}");
    defer request.deinit(a);
    const tables = [_]distributed_txn.TableCommitRequest{.{ .table_name = "docs", .normalized_graph_rows = true }};
    const id: db_mod.types.TxnId = @splat(9);
    const first = try registry.beginGraphExecution(a, id, 1, "alice", &request, &tables);
    defer first.plan.deinit();
    var persisted = (try durable.load(id)).?;
    defer persisted.deinit(a);
    const mandatory = try encodeSessionRecordWithListingLimit(a, persisted, 0);
    defer a.free(mandatory);
    registry.max_record_bytes = mandatory.len;
    try std.testing.expectError(error.SessionRecordTooLarge, registry.beginGraphExecution(a, @splat(10), 1, "alice", &request, &tables));
    registry.max_record_bytes.? += 256 + 6 * "docs".len;
    const second = try registry.beginGraphExecution(a, @splat(10), 1, "alice", &request, &tables);
    defer second.plan.deinit();
    try std.testing.expect((try registry.recordTerminalCommitWithRepair(a, @splat(10), .committed, false, 42, "docs")) != null);
    try std.testing.expectEqual(SessionDisposition.committed_pending, (try registry.getStatusForPrincipal(a, @splat(10), "alice")).?.disposition);
    try std.testing.expect((try registry.getStatusForPrincipal(a, @splat(10), "bob")) == null);
    try std.testing.expect((try registry.markTerminalCoordinatorAcknowledged(a, @splat(10))) != null);
    try std.testing.expectEqual(SessionDisposition.committed, (try registry.getStatusForPrincipal(a, @splat(10), "alice")).?.disposition);
    try std.testing.expectEqual(@as(usize, 1), try registry.cleanupExpired(a, std.math.maxInt(u64)));
}

test "distributed txn graph admission distinguishes lost commit acknowledgments from staging failures" {
    const LostAck = struct {
        attempted: bool = false,
        failure: anyerror = error.Unavailable,
        fn commit(self: *@This()) !void {
            self.attempted = true;
            return self.failure;
        }
        fn abort(_: *@This()) void {}
    };
    var txn: LostAck = .{};
    try std.testing.expectError(error.TransactionTooLarge, DurableSessionStore.finishSessionWrite(&txn, error.TransactionTooLarge, true));
    try std.testing.expect(!txn.attempted);
    try std.testing.expectError(error.GraphReceiptAdmissionUnknown, DurableSessionStore.finishSessionWrite(&txn, true, true));
    try std.testing.expect(txn.attempted);
    txn.failure = error.SessionWriteOutcomeUnknown;
    try std.testing.expectError(error.GraphReceiptAdmissionUnknown, DurableSessionStore.finishSessionWrite(&txn, true, true));
    txn.failure = error.SessionWriteUnavailableBeforeProposal;
    try std.testing.expectError(error.StorageReadTemporarilyUnavailable, DurableSessionStore.finishSessionWrite(&txn, true, true));
    txn.failure = error.SessionWriteNotLeaderBeforeProposal;
    try std.testing.expectError(error.NotLeader, DurableSessionStore.finishSessionWrite(&txn, true, true));
    txn.failure = error.NotLeader;
    try std.testing.expectError(error.GraphReceiptAdmissionUnknown, DurableSessionStore.finishSessionWrite(&txn, true, true));
    txn.failure = error.TransactionTooLarge;
    try std.testing.expectError(error.TransactionTooLarge, DurableSessionStore.finishSessionWrite(&txn, true, true));
}

test "distributed txn expiry advances past a full page of unresolved decisions" {
    const a = std.testing.allocator;
    var backend = mem_backend.Backend.init(a, .{});
    defer backend.close();
    var store = try backend.runtimeStore(a, .{ .name = "system/api-transaction-sessions" });
    defer store.deinit();
    var durable = DurableSessionStore.initRuntime(a, &store);
    var registry = SessionRegistry.init(&durable);
    defer registry.deinit(a);
    var session: Session = .{ .txn_id = @splat(0), .owner_node_id = 1, .begin_timestamp = 1, .last_touched_timestamp = 1, .sync_level = .write, .commit_execution_started = true };
    const pending = try encodeSessionRecord(a, session);
    defer a.free(pending);
    {
        var txn = try store.beginWrite();
        errdefer txn.abort();
        for (0..1025) |i| {
            std.mem.writeInt(u64, session.txn_id[8..16], i, .big);
            const key = try makeSessionKey(a, session.txn_id);
            defer a.free(key);
            const expiry = try makeSessionExpiryKey(a, 1, session.txn_id);
            defer a.free(expiry);
            if (i == 1024) {
                session.commit_execution_started = false;
                const idle = try encodeSessionRecord(a, session);
                defer a.free(idle);
                try txn.put(key, idle);
            } else try txn.put(key, pending);
            try txn.put(expiry, "");
        }
        try txn.commit();
    }
    try std.testing.expectEqual(@as(usize, 0), try registry.cleanupExpired(a, 2));
    try std.testing.expectEqual(@as(usize, 0), registry.sessions.count());
    try std.testing.expectEqual(@as(usize, 1), try registry.cleanupExpired(a, 2));
    try std.testing.expect(registry.expiry_cursor == null);
    try std.testing.expect((try durable.load(session.txn_id)) == null);
}

test "distributed txn renewed same owner rejects stale seals and retirement" {
    const a = std.testing.allocator;
    var backend = mem_backend.Backend.init(a, .{});
    defer backend.close();
    var store = try backend.runtimeStore(a, .{ .name = "system/api-transaction-sessions" });
    defer store.deinit();
    var durable = DurableSessionStore.initRuntime(a, &store);
    const leases = SessionLeaseStore.initFromDurable(&durable);
    var registry = SessionRegistry.initWithLeaseTtl(&durable, leases, std.time.ns_per_s);
    defer registry.deinit(a);
    const info = try registry.begin(a, .{}, 9);
    // A new native session already has its generation before the first stage.
    const original = try registry.leaseTenure(a, info.txn_id, 9);
    const guard = try registry.mutation(a, info.txn_id, 9);
    try std.testing.expect(original != null and original == guard.tenure);
    var request = try parseCommitRequest(a, "{\"read_set\":[],\"tables\":{\"docs\":{\"inserts\":{\"a\":{}}}}}");
    defer request.deinit(a);
    var staged = (try registry.cloneOwnedCommitRequest(a, guard, &request)).?;
    defer staged.deinit(a);
    const version = staged.session_version.?;
    _ = try registry.renewOwnedLeases(9, nextTxnTimestamp() + 2 * std.time.ns_per_s);
    const current = try registry.mutation(a, info.txn_id, 9);
    try std.testing.expect(current.tenure != guard.tenure);
    try std.testing.expectError(error.SessionLeaseLost, registry.cloneOwnedCommitRequest(a, guard, &request));
    try std.testing.expectError(error.SessionLeaseLost, registry.sealExecutionPlan(a, guard, &.{.{ .table_name = "docs" }}));
    for ([_]bool{ true, false }) |commit_settings| {
        try std.testing.expectError(error.SessionLeaseLost, registry.retireBeforeExecution(a, guard, version, commit_settings));
    }
    try std.testing.expectEqualDeep(version, registry.getInfo(info.txn_id).?.version);
    try std.testing.expect(try registry.retireBeforeExecution(a, current, version, false));
}

test "distributed txn expired publisher cannot overwrite an adopted execution plan" {
    const a = std.testing.allocator;
    var backend = mem_backend.Backend.init(a, .{});
    defer backend.close();
    var store = try backend.runtimeStore(a, .{ .name = "system/api-transaction-sessions" });
    defer store.deinit();
    var durable = DurableSessionStore.initRuntime(a, &store);
    const leases = SessionLeaseStore.initFromDurable(&durable);
    var old = SessionRegistry.initWithLeaseTtl(&durable, leases, std.time.ns_per_s);
    defer old.deinit(a);
    var next = SessionRegistry.initWithLeaseTtl(&durable, leases, std.time.ns_per_s);
    defer next.deinit(a);
    const id = (try old.begin(a, .{ .sync_level = .write }, 9)).txn_id;
    const old_guard = try old.mutation(a, id, 9);
    var request = try parseCommitRequest(a, "{\"read_set\":[],\"tables\":{\"docs\":{\"inserts\":{\"a\":{}}}}}");
    defer request.deinit(a);
    var staged = (try old.cloneCommitRequest(a, id, &request)).?;
    defer staged.deinit(a);
    // Old's registry still has the pre-adoption clone. Advance the lease
    // clock so the second owner can publish before old resumes its save.
    try std.testing.expect(!try next.adoptIfLeaseExpired(a, id, 10, nextTxnTimestamp()));
    try std.testing.expect(try next.adoptIfLeaseExpired(a, id, 10, nextTxnTimestamp() + 2 * std.time.ns_per_s));
    try std.testing.expect(!(old.retireBeforeExecution(a, old_guard, staged.session_version.?, false) catch false));
    try std.testing.expectError(error.SessionLeaseLost, durable.delete(id, staged.session_version.?, std.math.maxInt(u64), null, null));
    const adopted = next.getInfo(id).?;
    try std.testing.expectError(error.SessionLeaseLost, durable.delete(id, adopted.version, adopted.version.touched, null, null));
    var winner = (try next.sealExecutionPlan(a, try next.mutation(a, id, 10), &.{.{ .table_name = "docs" }})).?;
    defer winner.deinit();
    try std.testing.expect(winner.firstAttempt());
    try std.testing.expectError(error.SessionLeaseLost, old.sealExecutionPlan(a, old_guard, &.{.{ .table_name = "stale" }}));
    var saved = (try durable.load(id)).?;
    defer saved.deinit(a);
    try std.testing.expectEqual(@as(u64, 10), saved.owner_node_id);
    var chosen = try parseExecutionPlan(a, saved.execution_plan.?);
    defer chosen.deinit();
    try std.testing.expectEqualStrings("docs", chosen.value[0].table_name);
    _ = try next.renewOwnedLeases(10, nextTxnTimestamp() + 4 * std.time.ns_per_s);
    try std.testing.expectError(error.SessionLeaseLost, next.recordAttemptAbort(a, id, winner.publication));
    try std.testing.expect(!(try next.readInfo(id)).?.terminal_abort);
    try std.testing.expect(try old.adoptIfLeaseExpired(a, id, 9, nextTxnTimestamp() + 7 * std.time.ns_per_s));
    try std.testing.expectError(error.SessionLeaseLost, next.recordAttemptAbort(a, id, winner.publication));
}

test "distributed txn sessions durably seal exact binary integrity plans before recovery" {
    const alloc = std.testing.allocator;
    var backend = mem_backend.Backend.init(alloc, .{});
    defer backend.close();
    var store = try backend.runtimeStore(alloc, .{ .name = "system/api-transaction-sessions" });
    defer store.deinit();
    var durable = DurableSessionStore.initRuntime(alloc, &store);
    var writer = SessionRegistry.init(&durable);
    defer writer.deinit(alloc);
    const session = try writer.begin(alloc, .{ .sync_level = .write }, 9);
    var request = try parseCommitRequest(alloc,
        \\{"read_set":[{"table":"docs","key":"a","version":"7"}],"tables":{"docs":{"inserts":{"a":{"id":1}}}}}
    );
    defer request.deinit(alloc);
    var sealed = (try writer.cloneCommitRequest(alloc, session.txn_id, &request)).?;
    defer sealed.deinit(alloc);
    const commands = [_]@import("antfly_local_sources").storage_db_relational_integrity_contract.Command{.{
        .address = try @import("antfly_local_sources").storage_db_relational_integrity_contract.Address.init(@splat(2), "\x00\xfftuple"),
        .operation = .{ .check_owner = .{ .parent_table = "docs", .parent_key = "\x00\xffa" } },
    }};
    const tables = [_]distributed_txn.TableCommitRequest{.{ .table_name = "docs", .relational_schema_version = 3, .relational_integrity_generation_set = @splat(0xff), .predicates = &.{.{ .key = "a", .expected_version = 7 }}, .integrity_commands = &commands, .row_policy_principal_proof = "signed-owner-admission", .row_policy_database = "default", .row_policy_admitted_at_seconds = 1_800_000_000 }};
    durable.fail_writes_for_test = true;
    try std.testing.expectError(error.InjectedSessionStoreFailure, writer.sealExecutionPlan(alloc, try writer.mutation(alloc, session.txn_id, 9), &tables));
    try std.testing.expect((try writer.getExecutionPlan(alloc, try writer.mutation(alloc, session.txn_id, 9))) == null);
    durable.fail_writes_for_test = false;
    var first = (try writer.sealExecutionPlan(alloc, try writer.mutation(alloc, session.txn_id, 9), &tables)).?;
    defer first.deinit();
    // A retry that read different metadata cannot redirect the chosen plan.
    var retry = (try writer.sealExecutionPlan(alloc, try writer.mutation(alloc, session.txn_id, 9), &.{.{ .table_name = "wrong" }})).?;
    defer retry.deinit();
    try std.testing.expect(first.firstAttempt() and !retry.firstAttempt());
    try std.testing.expectEqualStrings("docs", retry.value[0].table_name);
    // A stale preflight on a concurrent HTTP retry may not erase the plan.
    try std.testing.expect(!try writer.retireBeforeExecution(alloc, try writer.mutation(alloc, session.txn_id, 9), session.version, false));
    var reopened = SessionRegistry.init(&durable);
    defer reopened.deinit(alloc);
    var recovery = (try reopened.claimPendingRecovery(alloc, session.txn_id, 9, nextTxnTimestamp())).?;
    defer recovery.deinit(alloc);
    var decoded = try parseExecutionPlan(alloc, recovery.commit.execution_plan.?);
    defer decoded.deinit();
    try std.testing.expectEqual(@as(?u32, 3), decoded.value[0].relational_schema_version);
    try std.testing.expectEqualStrings("signed-owner-admission", decoded.value[0].row_policy_principal_proof);
    try std.testing.expectEqualStrings("default", decoded.value[0].row_policy_database);
    try std.testing.expectEqual(@as(i64, 1_800_000_000), decoded.value[0].row_policy_admitted_at_seconds);
    try std.testing.expectEqual(@as(u64, 7), decoded.value[0].predicates[0].expected_version);
    try std.testing.expectEqualStrings("\x00\xffa", decoded.value[0].integrity_commands[0].operation.check_owner.parent_key);
    try std.testing.expectEqualSlices(u8, &commands[0].address.routing, &decoded.value[0].integrity_commands[0].address.routing);
    // Public retry identity and original read dependencies remain separate.
    try std.testing.expect(recovery.commit.request == null);
}

test "distributed txn sealed request cloning is allocation failure safe" {
    const Fixture = struct {
        fn run(alloc: std.mem.Allocator) !void {
            var reads = [_]TransactionReadItem{.{ .table_name = @constCast("docs"), .key = @constCast("a"), .expected_version = 7 }};
            const ops = [_]db_mod.types.TransformOp{.{ .op = .set, .path = "title", .value_json = "\"new\"" }};
            var transforms = [_]db_mod.types.DocumentTransform{.{ .key = "b", .operations = &ops }};
            var writes = [_]db_mod.types.BatchWrite{.{ .key = "a", .value = "{\"id\":1}" }};
            var deletes = [_][]const u8{"c"};
            var tables = [_]TableCommitRequest{.{
                .table_name = @constCast("docs"),
                .batch = .{ .writes = &writes, .deletes = &deletes, .transforms = &transforms },
            }};
            const input: OwnedTransactionCommitRequest = .{ .read_set = &reads, .tables = &tables };
            var clone = try input.clone(alloc);
            defer clone.deinit(alloc);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "distributed txn public commit JSON cannot supply internal execution authority" {
    const alloc = std.testing.allocator;
    try std.testing.expectError(error.InvalidBatchRequest, parseCommitRequest(alloc,
        \\{"read_set":[],"execution_plan":"forged","commit_execution_started":true,"tables":{"docs":{"inserts":{"a":{"id":1}},"relational_integrity_generation_set":[255],"integrity_commands":[]}}}
    ));
    var request = try parseCommitRequest(alloc,
        \\{"read_set":[],"execution_plan":"forged","commit_execution_started":true,"tables":{"docs":{"inserts":{"a":{"id":1}}}}}
    );
    defer request.deinit(alloc);
    const tables_view = try request.distributedTables(alloc);
    defer tables_view.deinit();
    const tables = tables_view.tables;
    try std.testing.expect(tables[0].relational_integrity_generation_set == null);
    try std.testing.expectEqual(@as(usize, 0), tables[0].integrity_commands.len);
    var registry = SessionRegistry.init(null);
    defer registry.deinit(alloc);
    const session = try registry.begin(alloc, .{}, 9);
    var sealed = (try registry.cloneCommitRequest(alloc, session.txn_id, &request)).?;
    defer sealed.deinit(alloc);
    try std.testing.expect((try registry.getExecutionPlan(alloc, try registry.mutation(alloc, session.txn_id, 9))) == null);
}

test "distributed txn stage validation rejects atomically and preserves prior savepoints" {
    const alloc = std.testing.allocator;
    var registry = SessionRegistry.init(null);
    defer registry.deinit(alloc);
    const session = try registry.begin(alloc, .{}, 9);
    var request = try parseStageWriteRequest(alloc, "{\"table\":\"docs\",\"key\":\"a\",\"document\":{\"id\":1}}");
    defer request.deinit(alloc);
    const session_mutation = try registry.mutation(alloc, session.txn_id, session.version.owner);
    _ = try registry.stage(alloc, session_mutation, &request);
    _ = try registry.createSavepoint(alloc, session_mutation);
    var calls: usize = 0;
    const Validator = struct {
        pub fn validate(ptr: *anyopaque, _: std.mem.Allocator, previous: ?*const OwnedTransactionCommitRequest, candidate: *OwnedTransactionCommitRequest, _: *const OwnedTransactionCommitRequest) !void {
            const count: *usize = @ptrCast(@alignCast(ptr));
            count.* += 1;
            try std.testing.expectEqual(@as(usize, 1), previous.?.tables[0].batch.writes.len);
            try std.testing.expectEqual(@as(usize, 2), candidate.tables[0].batch.writes.len);
            return error.ForeignKeyParentMissing;
        }
    };
    var rejected = try parseStageWriteRequest(alloc,
        \\{"table":"docs","key":"b","document":{"id":2}}
    );
    defer rejected.deinit(alloc);
    try std.testing.expectError(error.ForeignKeyParentMissing, registry.stageValidated(alloc, session_mutation, &rejected, .{ .ptr = &calls, .validate = Validator.validate }));
    try std.testing.expectEqual(@as(usize, 1), calls);
    var committed = (try registry.cloneCommitRequest(alloc, session.txn_id, null)).?;
    defer committed.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), committed.tables[0].batch.writes.len);
}

test "repair-required transaction sessions replay propagation once then release coordination" {
    const alloc = std.testing.allocator;
    var registry = SessionRegistry.init(null);
    defer registry.deinit(alloc);
    const session = try registry.begin(alloc, .{ .sync_level = .full_index }, 9);
    var request = try parseCommitRequest(alloc,
        \\{"read_set":[],"tables":{"docs":{"inserts":{"doc:a":{"value":1}}}}}
    );
    defer request.deinit(alloc);
    var sealed = (try registry.cloneCommitRequest(alloc, session.txn_id, &request)) orelse return error.TestExpectedEqual;
    sealed.deinit(alloc);
    _ = (try registry.markCommitExecutionStarted(alloc, session.txn_id)) orelse return error.TestExpectedEqual;
    _ = (try registry.recordTerminalCommitWithRepair(
        alloc,
        session.txn_id,
        .committed_visibility_pending,
        true,
        7001,
        "docs",
    )) orelse return error.TestExpectedEqual;

    var replay = (try registry.claimPendingRecovery(alloc, session.txn_id, 9, nextTxnTimestamp())) orelse return error.TestExpectedEqual;
    defer replay.deinit(alloc);
    try std.testing.expect(replay == .commit);
    try std.testing.expectEqual(db_mod.types.SyncLevel.full_index, replay.commit.sync_level);
    try std.testing.expect(replay.commit.repair_required);

    _ = (try registry.recordTerminalCommitWithRepair(
        alloc,
        session.txn_id,
        terminalCommitStatusForOutcome(false, false, false, false),
        true,
        7001,
        "docs",
    )) orelse return error.TestExpectedEqual;
    var acknowledgement = (try registry.claimPendingRecovery(alloc, session.txn_id, 9, nextTxnTimestamp())) orelse return error.TestExpectedEqual;
    defer acknowledgement.deinit(alloc);
    try std.testing.expect(acknowledgement == .acknowledge);
    _ = (try registry.markTerminalCoordinatorAcknowledged(alloc, session.txn_id)) orelse return error.TestExpectedEqual;
    const pending = try registry.listPendingRecoveryIds(alloc, 8);
    defer alloc.free(pending);
    try std.testing.expectEqual(@as(usize, 0), pending.len);
}

test "committed repair session without coordinator replays a write handoff" {
    const alloc = std.testing.allocator;
    var registry = SessionRegistry.init(null);
    defer registry.deinit(alloc);
    const session = try registry.begin(alloc, .{ .sync_level = .full_index }, 9);
    var request = try parseCommitRequest(alloc,
        \\{"read_set":[],"tables":{"docs":{"inserts":{"doc:a":{"value":1}}}}}
    );
    defer request.deinit(alloc);
    var sealed = (try registry.cloneCommitRequest(alloc, session.txn_id, &request)) orelse return error.TestExpectedEqual;
    sealed.deinit(alloc);
    _ = (try registry.markCommitExecutionStarted(alloc, session.txn_id)) orelse return error.TestExpectedEqual;

    // This is the crash-safe provisional state written when storage committed
    // but the source surfaced only a terminal enrichment error, before it
    // returned coordinator metadata to the session handler.
    _ = (try registry.recordTerminalCommitWithRepair(
        alloc,
        session.txn_id,
        .committed,
        true,
        null,
        null,
    )) orelse return error.TestExpectedEqual;

    var replay = (try registry.claimPendingRecovery(alloc, session.txn_id, 9, nextTxnTimestamp())) orelse return error.TestExpectedEqual;
    defer replay.deinit(alloc);
    try std.testing.expect(replay == .commit);
    try std.testing.expectEqual(db_mod.types.SyncLevel.full_index, replay.commit.sync_level);
    try std.testing.expect(replay.commit.repair_required);
    try std.testing.expect(replay.commit.repair_handoff_needs_coordinator);

    // The provisional record may be promoted exactly once when the write-only
    // recovery call returns the missing coordinator identity.
    _ = (try registry.recordTerminalCommitWithRepair(
        alloc,
        session.txn_id,
        .committed,
        true,
        7001,
        "docs",
    )) orelse return error.TestExpectedEqual;
    var acknowledgement = (try registry.claimPendingRecovery(alloc, session.txn_id, 9, nextTxnTimestamp())) orelse return error.TestExpectedEqual;
    defer acknowledgement.deinit(alloc);
    try std.testing.expect(acknowledgement == .acknowledge);
}

test "terminal commit response preserves live debt ahead of repair" {
    try std.testing.expectEqual(
        TerminalCommitStatus.committed_recovery_pending,
        terminalCommitStatusForOutcome(true, true, true, false),
    );
    try std.testing.expectEqualStrings(
        "committed_recovery_pending",
        terminalCommitResponseStatus(.committed_recovery_pending, true),
    );
    try std.testing.expectEqual(
        TerminalCommitStatus.committed_visibility_pending,
        terminalCommitStatusForOutcome(false, true, true, false),
    );
    try std.testing.expectEqualStrings(
        "committed_visibility_pending",
        terminalCommitResponseStatus(.committed_visibility_pending, true),
    );
    try std.testing.expectEqualStrings(
        "committed_repair_required",
        terminalCommitResponseStatus(.committed, true),
    );
    // Legacy/private adapters may only populate the established parent bit.
    // Preserve their unmet visibility contract conservatively.
    try std.testing.expectEqual(
        TerminalCommitStatus.committed_visibility_pending,
        terminalCommitStatusForOutcome(false, true, false, false),
    );
    // A classified terminal repair is not live retry work.
    try std.testing.expectEqual(
        TerminalCommitStatus.committed,
        terminalCommitStatusForOutcome(false, true, false, true),
    );
}

test "durable session limits bound count and encoded record size" {
    var backend = mem_backend.Backend.init(std.testing.allocator, .{});
    defer backend.close();
    var store = try backend.runtimeStore(std.testing.allocator, .{ .name = "system/api-transaction-sessions" });
    defer store.deinit();
    var durable = DurableSessionStore.initRuntime(std.testing.allocator, &store);
    var registry = SessionRegistry.initWithOptions(&durable, null, null, null, 1, 4096);
    defer registry.deinit(std.testing.allocator);
    const first = try registry.begin(std.testing.allocator, .{ .sync_level = .write }, 1);
    try std.testing.expectError(
        error.SessionLimitExceeded,
        registry.begin(std.testing.allocator, .{ .sync_level = .write }, 1),
    );

    var other = SessionRegistry.initWithOptions(&durable, null, null, null, 1, null);
    defer other.deinit(std.testing.allocator);
    registry.durable_scope = .cluster_shared;
    other.durable_scope = .cluster_shared;
    try std.testing.expectError(error.SessionLimitExceeded, other.begin(std.testing.allocator, .{}, 2));
    try std.testing.expect(other.remove(std.testing.allocator, first.txn_id));
    _ = try registry.begin(std.testing.allocator, .{}, 1);
    try std.testing.expectError(error.SessionLimitExceeded, other.begin(std.testing.allocator, .{}, 2));

    var small_registry = SessionRegistry.initWithOptions(&durable, null, null, null, null, 8);
    defer small_registry.deinit(std.testing.allocator);
    try std.testing.expectError(
        error.SessionRecordTooLarge,
        small_registry.begin(std.testing.allocator, .{ .sync_level = .write }, 2),
    );
    try std.testing.expectEqual(@as(usize, 0), small_registry.sessions.count());
}

test "durable session limits bootstrap exact occupancy even when the first begin is denied" {
    const a = std.testing.allocator;
    var backend = mem_backend.Backend.init(a, .{});
    defer backend.close();
    var native = try backend.runtimeStore(a, .{ .name = "system/api-transaction-sessions" });
    defer native.deinit();
    var durable = DurableSessionStore.initRuntime(a, &native);
    var registry = SessionRegistry.init(&durable);
    defer registry.deinit(a);
    const first = try registry.begin(a, .{}, 1);
    const second = try registry.begin(a, .{}, 1);
    _ = try registry.begin(a, .{}, 1);
    registry.max_sessions = 2;
    for (0..2) |_| {
        try std.testing.expectError(error.SessionLimitExceeded, registry.begin(a, .{}, 1));
        var txn = try native.beginRead();
        defer txn.abort();
        const bytes = try txn.get(session_record.count_key);
        try std.testing.expectEqual(@as(u64, 3), std.mem.readInt(u64, bytes[0..8], .little));
    }
    try std.testing.expect(registry.remove(a, first.txn_id));
    try std.testing.expectError(error.SessionLimitExceeded, registry.begin(a, .{}, 1));
    try std.testing.expect(registry.remove(a, second.txn_id));
    _ = try registry.begin(a, .{}, 1);
}

test "transaction session registry adopts durable session ownership" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/txn-session-adopt-store", .{tmp.sub_path});
    defer std.testing.allocator.free(path);
    const path_z = try std.testing.allocator.dupeSentinel(u8, path, 0);
    defer std.testing.allocator.free(path_z);

    var store = try docstore_mod.DocStore.open(std.testing.allocator, path_z, .{});
    defer store.close();
    var durable = DurableSessionStore.init(std.testing.allocator, &store);

    var writer = SessionRegistry.init(&durable);
    defer writer.deinit(std.testing.allocator);
    const session = try writer.begin(std.testing.allocator, .{ .sync_level = .write }, 9);

    var adopter = SessionRegistry.init(&durable);
    defer adopter.deinit(std.testing.allocator);
    try std.testing.expect(try adopter.adopt(std.testing.allocator, session.txn_id, 12));
    const status = (try adopter.getStatus(std.testing.allocator, session.txn_id)).?;
    try std.testing.expectEqual(@as(u64, 12), status.owner_node_id);
}

test "transaction session commit request is sealed across retries" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/txn-session-commit-seal-store", .{tmp.sub_path});
    defer alloc.free(path);
    const path_z = try alloc.dupeSentinel(u8, path, 0);
    defer alloc.free(path_z);

    var store = try docstore_mod.DocStore.open(alloc, path_z, .{});
    defer store.close();
    var durable = DurableSessionStore.init(alloc, &store);

    var writer = SessionRegistry.init(&durable);
    defer writer.deinit(alloc);
    const session = try writer.begin(alloc, .{ .sync_level = .write }, 9);
    const txn_id = session.txn_id;

    var first_body = try parseCommitRequest(alloc,
        \\{"read_set":[],"tables":{"docs":{"inserts":{"doc:a":{"value":1}}}}}
    );
    defer first_body.deinit(alloc);
    var changed_body = try parseCommitRequest(alloc,
        \\{"read_set":[],"tables":{"docs":{"inserts":{"doc:a":{"value":2}}}}}
    );
    defer changed_body.deinit(alloc);

    var first = (try writer.cloneCommitRequest(alloc, txn_id, &first_body)) orelse return error.TestExpectedEqual;
    defer first.deinit(alloc);

    // Re-open through an empty registry to prove that both the sealed request
    // and its digest survive process-local cache loss.
    var reader = SessionRegistry.init(&durable);
    defer reader.deinit(alloc);
    var retry = (try reader.cloneCommitRequest(alloc, txn_id, &first_body)) orelse return error.TestExpectedEqual;
    defer retry.deinit(alloc);
    try std.testing.expectEqualStrings(first.tables[0].batch.writes[0].value, retry.tables[0].batch.writes[0].value);
    try std.testing.expectError(
        error.TransactionCommitRequestMismatch,
        reader.cloneCommitRequest(alloc, txn_id, &changed_body),
    );
    try std.testing.expectError(
        error.TransactionCommitSealed,
        reader.stage(alloc, try reader.mutation(alloc, txn_id, sessionOwnerNodeId(txn_id)), &changed_body),
    );
}

test "durable recovery index tracks only validated commit execution and terminal handoff" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/txn-session-recovery-index", .{tmp.sub_path});
    defer alloc.free(path);
    const path_z = try alloc.dupeSentinel(u8, path, 0);
    defer alloc.free(path_z);
    var store = try docstore_mod.DocStore.open(alloc, path_z, .{});
    defer store.close();
    var durable = DurableSessionStore.init(alloc, &store);
    var registry = SessionRegistry.init(&durable);
    defer registry.deinit(alloc);

    const session = try registry.begin(alloc, .{ .sync_level = .write }, 9);
    var request = try parseCommitRequest(alloc,
        \\{"read_set":[],"tables":{"docs":{"inserts":{"doc:a":{"value":1}}}}}
    );
    defer request.deinit(alloc);
    var sealed = (try registry.cloneCommitRequest(alloc, session.txn_id, &request)) orelse return error.TestExpectedEqual;
    sealed.deinit(alloc);

    var pending = try registry.listPendingRecoveryIds(alloc, 32);
    defer alloc.free(pending);
    try std.testing.expectEqual(@as(usize, 0), pending.len);

    _ = (try registry.markCommitExecutionStarted(alloc, session.txn_id)) orelse return error.TestExpectedEqual;
    alloc.free(pending);
    pending = try registry.listPendingRecoveryIds(alloc, 32);
    try std.testing.expectEqual(@as(usize, 1), pending.len);
    try std.testing.expectEqualSlices(u8, &session.txn_id, &pending[0]);

    var work = (try registry.claimPendingRecovery(alloc, session.txn_id, 9, nextTxnTimestamp())) orelse return error.TestExpectedEqual;
    defer work.deinit(alloc);
    try std.testing.expect(work == .commit);

    _ = (try registry.recordTerminalCommit(alloc, session.txn_id, .committed_recovery_pending, 7001, "docs")) orelse return error.TestExpectedEqual;
    var terminal_registry = SessionRegistry.init(&durable);
    defer terminal_registry.deinit(alloc);
    const terminal_pending = try terminal_registry.listPendingRecoveryIds(alloc, 32);
    defer alloc.free(terminal_pending);
    try std.testing.expectEqual(@as(usize, 1), terminal_pending.len);

    var pending_terminal_work = (try terminal_registry.claimPendingRecovery(
        alloc,
        session.txn_id,
        9,
        nextTxnTimestamp(),
    )) orelse return error.TestExpectedEqual;
    defer pending_terminal_work.deinit(alloc);
    try std.testing.expect(pending_terminal_work == .commit);

    _ = (try terminal_registry.recordTerminalCommit(alloc, session.txn_id, .committed, 7001, "docs")) orelse return error.TestExpectedEqual;
    var acknowledgement_work = (try terminal_registry.claimPendingRecovery(
        alloc,
        session.txn_id,
        9,
        nextTxnTimestamp(),
    )) orelse return error.TestExpectedEqual;
    defer acknowledgement_work.deinit(alloc);
    try std.testing.expect(acknowledgement_work == .acknowledge);
    _ = (try terminal_registry.markTerminalCoordinatorAcknowledged(alloc, session.txn_id)) orelse return error.TestExpectedEqual;

    var completed_registry = SessionRegistry.init(&durable);
    defer completed_registry.deinit(alloc);
    const completed = try completed_registry.listPendingRecoveryIds(alloc, 32);
    defer alloc.free(completed);
    try std.testing.expectEqual(@as(usize, 0), completed.len);
}

test "durable recovery scan rotates fairly beyond one maintenance batch" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/txn-session-recovery-fairness", .{tmp.sub_path});
    defer alloc.free(path);
    const path_z = try alloc.dupeSentinel(u8, path, 0);
    defer alloc.free(path_z);
    var store = try docstore_mod.DocStore.open(alloc, path_z, .{});
    defer store.close();
    var durable = DurableSessionStore.init(alloc, &store);
    var registry = SessionRegistry.init(&durable);
    defer registry.deinit(alloc);
    var request = try parseCommitRequest(alloc,
        \\{"read_set":[],"tables":{"docs":{"inserts":{"doc:a":{"value":1}}}}}
    );
    defer request.deinit(alloc);

    var target: db_mod.types.TxnId = undefined;
    for (0..33) |i| {
        const session = try registry.begin(alloc, .{ .sync_level = .write }, if (i < 32) 1 else 2);
        var sealed = (try registry.cloneCommitRequest(alloc, session.txn_id, &request)) orelse return error.TestExpectedEqual;
        sealed.deinit(alloc);
        _ = (try registry.markCommitExecutionStarted(alloc, session.txn_id)) orelse return error.TestExpectedEqual;
        if (i == 32) target = session.txn_id;
    }

    const first = try registry.listPendingRecoveryIds(alloc, 32);
    defer alloc.free(first);
    try std.testing.expectEqual(@as(usize, 32), first.len);
    const second = try registry.listPendingRecoveryIds(alloc, 32);
    defer alloc.free(second);
    var found_target = false;
    for (second) |txn_id| if (std.mem.eql(u8, &txn_id, &target)) {
        found_target = true;
        break;
    };
    try std.testing.expect(found_target);
}

test "in-memory recovery scan rotates fairly when the first page remains pending" {
    const alloc = std.testing.allocator;
    var registry = SessionRegistry.init(null);
    defer registry.deinit(alloc);
    var request = try parseCommitRequest(alloc,
        \\{"read_set":[],"tables":{"docs":{"inserts":{"doc:a":{"value":1}}}}}
    );
    defer request.deinit(alloc);

    for (0..33) |_| {
        const session = try registry.begin(alloc, .{ .sync_level = .write }, 1);
        var sealed = (try registry.cloneCommitRequest(alloc, session.txn_id, &request)) orelse return error.TestExpectedEqual;
        sealed.deinit(alloc);
        _ = (try registry.markCommitExecutionStarted(alloc, session.txn_id)) orelse return error.TestExpectedEqual;
    }

    const first = try registry.listPendingRecoveryIds(alloc, 32);
    defer alloc.free(first);
    try std.testing.expectEqual(@as(usize, 32), first.len);

    const second = try registry.listPendingRecoveryIds(alloc, 32);
    defer alloc.free(second);
    try std.testing.expectEqual(@as(usize, 1), second.len);
    for (first) |txn_id| {
        try std.testing.expect(!std.mem.eql(u8, &txn_id, &second[0]));
    }
}

test "SQL session metadata admits warm statement rows and settings without decoding durable savepoints" {
    const a = std.testing.allocator;
    var backend = mem_backend.Backend.init(a, .{});
    defer backend.close();
    var native = try backend.runtimeStore(a, .{ .name = "session-statement-test" });
    defer native.deinit();
    var durable = DurableSessionStore.initRuntime(a, &native);
    const leases = SessionLeaseStore.initFromDurable(&durable);
    var registry = SessionRegistry.initWithLeaseTtl(&durable, leases, 60 * std.time.ns_per_s);
    defer registry.deinit(a);
    registry.durable_scope = .cluster_shared;
    const info = try registry.beginForPrincipal(a, .{ .sql = .{ .database = "main", .namespace = "public", .isolation = .read_committed, .mode = .read_write } }, 1, "alice");
    const guard = try registry.mutation(a, info.txn_id, 1);
    var request = try parseCommitRequest(a, "{\"read_set\":[],\"tables\":{\"docs\":{\"inserts\":{\"a\":{}}}}}");
    defer request.deinit(a);
    _ = try registry.stage(a, guard, &request);
    _ = try registry.createNamedSavepoint(a, guard, "before_read");
    // Version and tenure reads use the caller allocator. A full record
    // decode (including the savepoint's staged rows) would fail here.
    var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    durable.alloc = failing.allocator();
    defer durable.alloc = a;
    var statement = try registry.admitSqlStatement(a, a, guard, "alice", null);
    defer statement.state.deinit(a);
    defer statement.staged.deinit(a);
    try std.testing.expectEqual(@as(usize, 1), statement.state.savepoints);
    try std.testing.expectEqualStrings("a", statement.staged.tables[0].batch.writes[0].key);
}

test "SQL session metadata mutation requires the admitted owner and exact tenure after adoption" {
    const a = std.testing.allocator;
    var backend = mem_backend.Backend.init(a, .{});
    defer backend.close();
    var native = try backend.runtimeStore(a, .{ .name = "session-fence-test" });
    defer native.deinit();
    // Cold admission transfers decoded values from the store while the
    // registry retains ownership of its map allocation.
    var storage_arena = std.heap.ArenaAllocator.init(a);
    defer storage_arena.deinit();
    var durable = DurableSessionStore.initRuntime(storage_arena.allocator(), &native);
    const leases = SessionLeaseStore.initFromDurable(&durable);
    var former = SessionRegistry.initWithLeaseTtl(&durable, leases, 60 * std.time.ns_per_s);
    defer former.deinit(a);
    former.durable_scope = .cluster_shared;
    var next = SessionRegistry.initWithLeaseTtl(&durable, leases, 60 * std.time.ns_per_s);
    defer next.deinit(a);
    next.durable_scope = .cluster_shared;
    const begun = try former.beginForPrincipal(a, .{ .sql = .{ .database = "main", .namespace = "public", .isolation = .read_committed, .mode = .read_write } }, 1, "alice");
    const guard = try former.mutation(a, begun.txn_id, 1);
    var cold = try next.admitSql(a, begun.txn_id, "alice", 1, null, null);
    defer cold.state.deinit(a);
    try std.testing.expectEqualDeep(guard, cold.guard);
    try former.setSqlFailed(a, guard, true);
    var refreshed = try next.admitSql(a, begun.txn_id, "alice", 1, null, guard);
    defer refreshed.state.deinit(a);
    try std.testing.expect(refreshed.state.metadata.failed);
    try former.setSqlFailed(a, guard, false);
    try std.testing.expectError(error.SqlTransactionNotActive, next.admitSql(a, begun.txn_id, "bob", 1, null, guard));
    try std.testing.expect(try next.adopt(a, begun.txn_id, 2));
    try std.testing.expectError(error.SessionLeaseLost, former.createNamedSavepoint(a, guard, "stale"));
    try std.testing.expect(try former.adopt(a, begun.txn_id, 1));
    try std.testing.expectError(error.SessionLeaseLost, former.setSqlFailed(a, guard, true));
    try std.testing.expectError(error.SessionLeaseLost, next.admitSql(a, begun.txn_id, "alice", 1, null, guard));
    var current = (try next.getSqlState(a, begun.txn_id)).?;
    defer current.deinit(a);
    try std.testing.expect(!current.metadata.failed);
    try std.testing.expectEqual(@as(usize, 0), current.savepoints);
}

test "SQL session metadata for a retained reader checks the shared session and live lease on one snapshot" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/reader-lease", .{tmp.sub_path}, 0);
    defer a.free(path);
    var store = try docstore_mod.DocStore.open(a, path, .{});
    defer store.close();
    var durable = DurableSessionStore.init(a, &store);
    const leases = SessionLeaseStore.initFromDurable(&durable);
    var registry = SessionRegistry.initWithLeaseTtl(&durable, leases, 60 * std.time.ns_per_s);
    defer registry.deinit(a);
    registry.durable_scope = .cluster_shared;
    const info = try registry.beginForPrincipal(a, .{ .sql = .{ .database = "app", .namespace = "public", .isolation = .read_committed, .mode = .read_only } }, 7, "alice");
    const id = info.txn_id;
    const generation_key = try readerGenerationKey(a, id);
    defer a.free(generation_key);
    const issued = try store.get(a, generation_key);
    defer a.free(issued);
    var initial_lease = (try leases.load(a, id)).?;
    defer lease_mod.deinitRecord(a, &initial_lease);
    try std.testing.expectEqual(certified_lease_epoch, initial_lease.epoch);
    try registry.requireActiveSqlReader(a, id, "alice", 7);
    try std.testing.expectError(error.SessionLeaseLost, registry.requireActiveSqlReader(a, id, "alice", 8));
    try std.testing.expectError(error.SqlTransactionNotActive, registry.requireActiveSqlReader(a, id, "bob", 7));

    const key = try makeSessionKey(a, id);
    defer a.free(key);
    const raw = try store.get(a, key);
    defer a.free(raw);
    // Only the prefix is available to the fast path, independent of staged
    // data, settings, and savepoints that may follow in the same value.
    const PrefixOnly = struct {
        prefix: []const u8,
        fn openReadScope(self: *@This(), _: std.mem.Allocator) anyerror!*@This() {
            return self;
        }
        fn probePrefix(self: *@This(), _: []const u8, length: usize) anyerror![]const u8 {
            try std.testing.expectEqual(self.prefix.len, length);
            return self.prefix;
        }
        fn get(_: *@This(), _: []const u8) anyerror![]const u8 {
            return error.UnexpectedFullRead;
        }
        fn close(_: *@This()) void {}
    };
    var bounded = PrefixOnly{ .prefix = raw[0..reader_prefix_len] };
    try checkDurableReaderSession(a, &bounded, key, id, "alice", 7);
    var attached = (try durable.load(id)).?;
    defer attached.deinit(a);
    attached.connection_id = @splat('a');
    const packed_session = try encodeSessionRecord(a, attached);
    defer a.free(packed_session);
    var lifecycle_prefix = PrefixOnly{ .prefix = packed_session[0..connection_prefix_len] };
    const lifecycle = (try durable.connectionLifecycleTxn(&lifecycle_prefix, id, false)).?;
    try std.testing.expectEqualDeep(DurableSessionStore.Version.of(attached), lifecycle.version);
    try std.testing.expectEqual(attached.connection_id, lifecycle.connection_id);
    try std.testing.expect(!lifecycle.terminal);
    bounded.prefix = packed_session[0..@max(connection_prefix_len, reader_prefix_len)];
    try checkDurableReaderSession(a, &bounded, key, id, "alice", 7);
    try std.testing.expectEqual(@as(?u64, 7), try SessionRegistry.probeDurableOwner(&bounded, a, id));
    bounded.prefix = raw[0..reader_prefix_len];
    try std.testing.expectEqual(@as(?u64, 7), try SessionRegistry.probeDurableOwner(&bounded, a, id));
    const old_fields = std.mem.indexOf(u8, raw, "\"owner_node_id\"").?;
    const legacy = try std.mem.concat(a, u8, &.{ "{", raw[old_fields..] });
    defer a.free(legacy);
    try store.put(key, legacy);
    try registry.requireActiveSqlReader(a, id, "alice", 7);
    try std.testing.expectError(error.SqlTransactionNotActive, registry.requireActiveSqlReader(a, id, "bob", 7));
    try store.put(key, raw);
    const first = (try registry.sqlReaderTenure(a, id, "alice", 7)).?;
    const now = nextTxnTimestamp() / std.time.ns_per_ms;
    try std.testing.expect(try leases.renew(id, 7, now, 60_000));
    try std.testing.expectEqual(first, (try registry.sqlReaderTenure(a, id, "alice", 7)).?);
    try std.testing.expect(try leases.renew(id, 7, now + 120_000, 60_000));
    try std.testing.expectEqual(first + 1, (try registry.sqlReaderTenure(a, id, "alice", 7)).?);
    try std.testing.expect(try leases.renew(id, 8, now + 240_000, 60_000));
    try std.testing.expectError(error.SessionLeaseLost, registry.sqlReaderTenure(a, id, "alice", 7));
    try std.testing.expect(try leases.renew(id, 7, now + 360_000, 60_000));
    try std.testing.expectEqual(first + 3, (try registry.sqlReaderTenure(a, id, "alice", 7)).?);
    const lease_key = try makeSessionLeaseKey(a, id);
    defer a.free(lease_key);
    // A base-version adopter rewrites the canonical lease with epoch zero.
    // Its heartbeat may promote that back to one before this server observes it.
    const live_legacy = try std.json.Stringify.valueAlloc(a, lease_mod.LeaseRecord{ .owner_id = "node:7", .expires_at_ms = now + 600_000, .epoch = 1 }, .{});
    defer a.free(live_legacy);
    try store.put(lease_key, live_legacy);
    const reacquired = (try registry.sqlReaderTenure(a, id, "alice", 7)).?;
    try std.testing.expect(reacquired != first);
    try std.testing.expectEqual(reacquired, (try registry.sqlReaderTenure(a, id, "alice", 7)).?);
    try store.put(lease_key, "{\"owner_id\":\"node:7\",\"expires_at_ms\":1}");
    try std.testing.expectError(error.SessionLeaseLost, registry.requireActiveSqlReader(a, id, "alice", 7));
    try std.testing.expect(try leases.renew(id, 8, nextTxnTimestamp() / std.time.ns_per_ms, 60_000));
    try std.testing.expectError(error.SessionLeaseLost, registry.requireActiveSqlReader(a, id, "alice", 7));
    try std.testing.expect(registry.remove(a, id));
    try std.testing.expectError(error.NotFound, store.get(a, generation_key));
    try std.testing.expectError(error.NotFound, store.get(a, lease_key));
    try std.testing.expectError(error.SqlTransactionNotActive, registry.requireActiveSqlReader(a, id, "alice", 7));
    _ = try registry.beginForPrincipalWithSettingsAndId(a, .{ .sql = .{ .database = "app", .namespace = "public", .isolation = .read_committed, .mode = .read_only } }, 7, "alice", &.{}, null, id);
    const reused = (try registry.sqlReaderTenure(a, id, "alice", 7)).?;
    try std.testing.expect(reused != first and reused != reacquired);
}

test "background recovery adopts an expired shared-store owner lease" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/txn-session-recovery-adopt", .{tmp.sub_path});
    defer alloc.free(path);
    const path_z = try alloc.dupeSentinel(u8, path, 0);
    defer alloc.free(path_z);
    var store = try docstore_mod.DocStore.open(alloc, path_z, .{});
    defer store.close();
    var durable = DurableSessionStore.init(alloc, &store);
    const leases = SessionLeaseStore.init(alloc, &store);
    var owner = SessionRegistry.initWithLeaseTtl(&durable, leases, std.time.ns_per_s);
    defer owner.deinit(alloc);
    owner.durable_scope = .cluster_shared;
    const session = try owner.begin(alloc, .{ .sync_level = .write }, 7);
    var request = try parseCommitRequest(alloc,
        \\{"read_set":[],"tables":{"docs":{"inserts":{"doc:a":{"value":1}}}}}
    );
    defer request.deinit(alloc);
    var sealed = (try owner.cloneCommitRequest(alloc, session.txn_id, &request)) orelse return error.TestExpectedEqual;
    sealed.deinit(alloc);
    _ = (try owner.markCommitExecutionStarted(alloc, session.txn_id)) orelse return error.TestExpectedEqual;

    var adopter = SessionRegistry.initWithLeaseTtl(&durable, leases, std.time.ns_per_s);
    defer adopter.deinit(alloc);
    adopter.durable_scope = .cluster_shared;
    try std.testing.expect((try adopter.claimPendingRecovery(alloc, session.txn_id, 12, session.begin_timestamp)) == null);
    var lease = (try leases.load(alloc, session.txn_id)).?;
    defer lease_mod.deinitRecord(alloc, &lease);
    var failing = std.testing.FailingAllocator.init(alloc, .{ .fail_index = 0 });
    try std.testing.expectError(error.OutOfMemory, adopter.claimPendingRecovery(failing.allocator(), session.txn_id, 12, lease.expires_at_ms * std.time.ns_per_ms + 1));
    var before_retry = (try durable.load(session.txn_id)).?;
    try std.testing.expectEqual(@as(u64, 7), before_retry.owner_node_id);
    before_retry.deinit(durable.alloc);
    var claimed = (try adopter.claimPendingRecovery(alloc, session.txn_id, 12, lease.expires_at_ms * std.time.ns_per_ms + 1)) orelse return error.TestExpectedEqual;
    defer claimed.deinit(alloc);
    try std.testing.expect(claimed == .commit);
    const status = (try adopter.getStatus(alloc, session.txn_id)).?;
    try std.testing.expectEqual(@as(u64, 12), status.owner_node_id);
}

test "transaction session registry only adopts durable sessions after lease expiry" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/txn-session-adopt-timeout-store", .{tmp.sub_path});
    defer std.testing.allocator.free(path);
    const path_z = try std.testing.allocator.dupeSentinel(u8, path, 0);
    defer std.testing.allocator.free(path_z);

    var store = try docstore_mod.DocStore.open(std.testing.allocator, path_z, .{});
    defer store.close();
    var durable = DurableSessionStore.init(std.testing.allocator, &store);

    const lease_store = SessionLeaseStore.init(std.testing.allocator, &store);
    var writer = SessionRegistry.initWithLeaseTtl(&durable, lease_store, std.time.ns_per_s);
    defer writer.deinit(std.testing.allocator);
    const session = try writer.begin(std.testing.allocator, .{ .sync_level = .write }, 9);

    var adopter = SessionRegistry.initWithLeaseTtl(&durable, lease_store, std.time.ns_per_s);
    defer adopter.deinit(std.testing.allocator);
    try std.testing.expect(!(try adopter.adoptIfLeaseExpired(std.testing.allocator, session.txn_id, 12, session.begin_timestamp)));

    var lease_record = (try lease_store.load(std.testing.allocator, session.txn_id)).?;
    defer lease_mod.deinitRecord(std.testing.allocator, &lease_record);
    const expired_now = lease_record.expires_at_ms * std.time.ns_per_ms + 1;

    try std.testing.expect(try adopter.adoptIfLeaseExpired(std.testing.allocator, session.txn_id, 12, expired_now));
    const status = (try adopter.getStatus(std.testing.allocator, session.txn_id)).?;
    try std.testing.expectEqual(@as(u64, 12), status.owner_node_id);
    try std.testing.expect(status.lease_expires_at > session.begin_timestamp);
}

test "transaction session adoption preserves newer durable state than a local cache" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/txn-session-adopt-fresh-state", .{tmp.sub_path});
    defer std.testing.allocator.free(path);
    const path_z = try std.testing.allocator.dupeSentinel(u8, path, 0);
    defer std.testing.allocator.free(path_z);

    var store = try docstore_mod.DocStore.open(std.testing.allocator, path_z, .{});
    defer store.close();
    var durable = DurableSessionStore.init(std.testing.allocator, &store);
    const lease_store = SessionLeaseStore.init(std.testing.allocator, &store);

    var writer = SessionRegistry.initWithLeaseTtl(&durable, lease_store, std.time.ns_per_s);
    defer writer.deinit(std.testing.allocator);
    const session = try writer.begin(std.testing.allocator, .{ .sync_level = .write }, 9);

    var adopter = SessionRegistry.initWithLeaseTtl(&durable, lease_store, std.time.ns_per_s);
    defer adopter.deinit(std.testing.allocator);
    // Model a non-owner status request that populated an initial local copy.
    _ = (try adopter.getStatus(std.testing.allocator, session.txn_id)) orelse return error.TestExpectedEqual;

    var stage_req = try parseStageWriteRequest(std.testing.allocator, "{\"table\":\"docs\",\"key\":\"doc:a\",\"document\":{\"title\":\"newer durable state\"}}");
    defer stage_req.deinit(std.testing.allocator);
    const session_mutation = try writer.mutation(std.testing.allocator, session.txn_id, session.version.owner);
    _ = try writer.stage(std.testing.allocator, session_mutation, &stage_req);

    var renewed_lease = (try lease_store.load(std.testing.allocator, session.txn_id)).?;
    defer lease_mod.deinitRecord(std.testing.allocator, &renewed_lease);
    const expired_now = renewed_lease.expires_at_ms * std.time.ns_per_ms + 1;
    try std.testing.expect(try adopter.adoptIfLeaseExpired(std.testing.allocator, session.txn_id, 12, expired_now));

    var adopted = (try adopter.cloneCommitRequest(std.testing.allocator, session.txn_id, null)) orelse return error.TestExpectedEqual;
    defer adopted.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), adopted.tables.len);
    try std.testing.expectEqual(@as(usize, 1), adopted.tables[0].batch.writes.len);
    try std.testing.expect(std.mem.indexOf(u8, adopted.tables[0].batch.writes[0].value, "newer durable state") != null);
}

test "transaction session ownership and lease transition atomically on failure" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/txn-session-atomic-owner", .{tmp.sub_path});
    defer std.testing.allocator.free(path);
    const path_z = try std.testing.allocator.dupeSentinel(u8, path, 0);
    defer std.testing.allocator.free(path_z);

    var store = try docstore_mod.DocStore.open(std.testing.allocator, path_z, .{});
    defer store.close();
    var durable = DurableSessionStore.init(std.testing.allocator, &store);
    const lease_store = SessionLeaseStore.init(std.testing.allocator, &store);
    var writer = SessionRegistry.initWithLeaseTtl(&durable, lease_store, std.time.ns_per_s);
    defer writer.deinit(std.testing.allocator);
    const session = try writer.begin(std.testing.allocator, .{ .sync_level = .write }, 9);
    var lease_before = (try lease_store.load(std.testing.allocator, session.txn_id)).?;
    defer lease_mod.deinitRecord(std.testing.allocator, &lease_before);

    var adopter = SessionRegistry.initWithLeaseTtl(&durable, lease_store, std.time.ns_per_s);
    defer adopter.deinit(std.testing.allocator);
    durable.fail_lease_transition_after_session_write_for_test = true;
    try std.testing.expectError(
        error.InjectedLeaseTransitionFailure,
        adopter.adoptIfLeaseExpired(std.testing.allocator, session.txn_id, 12, lease_before.expires_at_ms * std.time.ns_per_ms + 1),
    );
    durable.fail_lease_transition_after_session_write_for_test = false;

    var persisted = (try durable.load(session.txn_id)).?;
    defer persisted.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u64, 9), persisted.owner_node_id);
    var lease_after = (try lease_store.load(std.testing.allocator, session.txn_id)).?;
    defer lease_mod.deinitRecord(std.testing.allocator, &lease_after);
    try std.testing.expectEqualStrings(lease_before.owner_id, lease_after.owner_id);
    try std.testing.expectEqual(lease_before.expires_at_ms, lease_after.expires_at_ms);
}

test "transaction session registry renews and releases separate lease records" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/txn-session-lease-renew-store", .{tmp.sub_path});
    defer std.testing.allocator.free(path);
    const path_z = try std.testing.allocator.dupeSentinel(u8, path, 0);
    defer std.testing.allocator.free(path_z);

    var store = try docstore_mod.DocStore.open(std.testing.allocator, path_z, .{});
    defer store.close();
    var durable = DurableSessionStore.init(std.testing.allocator, &store);

    const lease_store = SessionLeaseStore.init(std.testing.allocator, &store);
    var registry = SessionRegistry.initWithLeaseTtl(&durable, lease_store, 10 * std.time.ns_per_ms);
    defer registry.deinit(std.testing.allocator);
    const session = try registry.begin(std.testing.allocator, .{ .sync_level = .write }, 15);

    const initial_status = (try registry.getStatus(std.testing.allocator, session.txn_id)) orelse return error.TestExpectedEqual;
    try std.testing.expect(initial_status.lease_expires_at > 0);

    std.testing.io.sleep(.fromNanoseconds(1), .awake) catch {};
    const session_mutation = try registry.mutation(std.testing.allocator, session.txn_id, session.version.owner);
    _ = (try registry.createSavepoint(std.testing.allocator, session_mutation)) orelse return error.TestExpectedEqual;

    const renewed_status = (try registry.getStatus(std.testing.allocator, session.txn_id)) orelse return error.TestExpectedEqual;
    try std.testing.expect(renewed_status.lease_expires_at > initial_status.lease_expires_at);

    try std.testing.expect(registry.remove(std.testing.allocator, session.txn_id));
    try std.testing.expect((try lease_store.load(std.testing.allocator, session.txn_id)) == null);
}

test "transaction session registry reloads durable sessions from kv store" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/txn-session-store", .{tmp.sub_path});
    defer std.testing.allocator.free(path);
    const path_z = try std.testing.allocator.dupeSentinel(u8, path, 0);
    defer std.testing.allocator.free(path_z);

    var store = try docstore_mod.DocStore.open(std.testing.allocator, path_z, .{});
    defer store.close();
    var durable = DurableSessionStore.init(std.testing.allocator, &store);

    var writer = SessionRegistry.init(&durable);
    defer writer.deinit(std.testing.allocator);
    const session = try writer.begin(std.testing.allocator, .{ .sync_level = .write }, 9);

    var stage_req = try parseStageWriteRequest(std.testing.allocator, "{\"table\":\"docs\",\"key\":\"doc:a\",\"document\":{\"title\":\"persisted\"}}");
    defer stage_req.deinit(std.testing.allocator);
    const session_mutation = try writer.mutation(std.testing.allocator, session.txn_id, session.version.owner);
    _ = try writer.stage(std.testing.allocator, session_mutation, &stage_req);

    var reader = SessionRegistry.init(&durable);
    defer reader.deinit(std.testing.allocator);
    const loaded = reader.getInfo(session.txn_id) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u64, 9), sessionOwnerNodeId(loaded.txn_id));
    const status = (try reader.getStatus(std.testing.allocator, session.txn_id)).?;
    try std.testing.expectEqual(@as(u64, 0), status.lease_expires_at);

    var merged = (try reader.cloneCommitRequest(std.testing.allocator, session.txn_id, null)) orelse return error.TestExpectedEqual;
    defer merged.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), merged.tables.len);
    try std.testing.expectEqual(@as(usize, 1), merged.tables[0].batch.writes.len);
    try std.testing.expect(std.mem.indexOf(u8, merged.tables[0].batch.writes[0].value, "\"persisted\"") != null);
}

test "transaction session registry reports status and cleans expired durable sessions" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/txn-session-cleanup-store", .{tmp.sub_path});
    defer std.testing.allocator.free(path);
    const path_z = try std.testing.allocator.dupeSentinel(u8, path, 0);
    defer std.testing.allocator.free(path_z);

    var store = try docstore_mod.DocStore.open(std.testing.allocator, path_z, .{});
    defer store.close();
    var durable = DurableSessionStore.init(std.testing.allocator, &store);

    var registry = SessionRegistry.init(&durable);
    defer registry.deinit(std.testing.allocator);
    const session = try registry.begin(std.testing.allocator, .{ .sync_level = .write }, 11);

    var read_req = try parseStageReadRequest(std.testing.allocator, "{\"table\":\"docs\",\"key\":\"doc:a\",\"version\":\"7\"}");
    defer read_req.deinit(std.testing.allocator);
    const session_mutation = try registry.mutation(std.testing.allocator, session.txn_id, session.version.owner);
    _ = try registry.stage(std.testing.allocator, session_mutation, &read_req);
    var write_req = try parseStageWriteRequest(std.testing.allocator, "{\"table\":\"docs\",\"key\":\"doc:a\",\"document\":{\"title\":\"status\"}}");
    defer write_req.deinit(std.testing.allocator);
    _ = try registry.stage(std.testing.allocator, session_mutation, &write_req);

    _ = (try registry.createSavepoint(std.testing.allocator, session_mutation)) orelse return error.TestExpectedEqual;

    const status = (try registry.getStatus(std.testing.allocator, session.txn_id)) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u64, 11), status.owner_node_id);
    try std.testing.expectEqual(@as(u64, 0), status.lease_expires_at);
    try std.testing.expectEqual(@as(usize, 1), status.staged_table_count);
    try std.testing.expectEqual(@as(usize, 1), status.staged_read_count);
    try std.testing.expectEqual(@as(usize, 1), status.staged_write_count);
    try std.testing.expectEqual(@as(usize, 0), status.staged_delete_count);
    try std.testing.expectEqual(@as(usize, 1), status.savepoint_count);
    try std.testing.expect(status.savepoint_limit == null);
    try std.testing.expect(status.remaining_savepoints == null);
    try std.testing.expect(status.durable);

    registry.sessions.getPtr(session.txn_id).?.last_touched_timestamp = 1;
    try durable.saveForTest(registry.sessions.get(session.txn_id).?, null);
    const removed = try registry.cleanupExpired(std.testing.allocator, 2);
    try std.testing.expectEqual(@as(usize, 1), removed);
    try std.testing.expect(registry.getInfo(session.txn_id) == null);
    try std.testing.expect((try durable.load(session.txn_id)) == null);
}

test "distributed txn constraint timing stage rollback and durable reload are atomic" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrintSentinel(alloc, ".zig-cache/tmp/{s}/timing", .{tmp.sub_path}, 0);
    defer alloc.free(path);
    var store = try docstore_mod.DocStore.open(alloc, path, .{});
    defer store.close();
    var durable = DurableSessionStore.init(alloc, &store);
    var writer = SessionRegistry.init(&durable);
    defer writer.deinit(alloc);
    const session = try writer.begin(alloc, .{}, 21);
    var deferred: OwnedTransactionCommitRequest = .{};
    defer deferred.deinit(alloc);
    try deferred.setConstraintTiming(alloc, .{ .deferred = true });
    const session_mutation = try writer.mutation(alloc, session.txn_id, session.version.owner);
    _ = try writer.stage(alloc, session_mutation, &deferred);
    const point = (try writer.createNamedSavepoint(alloc, session_mutation, "deferred")).?;
    var immediate: OwnedTransactionCommitRequest = .{};
    defer immediate.deinit(alloc);
    try immediate.setConstraintTiming(alloc, .{ .generation = @splat(4), .deferred = false });
    const Reject = struct {
        pub fn validate(_: *anyopaque, _: std.mem.Allocator, _: ?*const OwnedTransactionCommitRequest, _: *OwnedTransactionCommitRequest, _: *const OwnedTransactionCommitRequest) !void {
            return error.UniqueConstraintViolation;
        }
    };
    var context: u8 = 0;
    try std.testing.expectError(error.UniqueConstraintViolation, writer.stageValidated(alloc, session_mutation, &immediate, .{ .ptr = &context, .validate = Reject.validate }));
    {
        var pending = try writer.sessions.get(session.txn_id).?.staged.?.clone(alloc);
        defer pending.deinit(alloc);
        try std.testing.expectEqual(@as(usize, 1), pending.constraint_timing.items.len);
        try std.testing.expect(pending.constraint_timing.items[0].deferred);
    }
    _ = try writer.stage(alloc, session_mutation, &immediate);
    _ = try writer.rollbackToSavepoint(alloc, session_mutation, point.savepoint_id);
    var recovered = (try durable.load(session.txn_id)).?;
    defer recovered.deinit(alloc);
    var pending = try recovered.staged.?.clone(alloc);
    defer pending.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), pending.constraint_timing.items.len);
    try std.testing.expect(pending.constraint_timing.items[0].deferred);
    var public = try parseCommitRequest(alloc, "{\"read_set\":[],\"tables\":{},\"constraint_timing\":[{\"deferred\":true}]}");
    defer public.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), public.constraint_timing.items.len);
}

test "transaction session named savepoints shadow release and roll back nested state" {
    const alloc = std.testing.allocator;
    var registry = SessionRegistry.init(null);
    defer registry.deinit(alloc);
    const session = try registry.begin(alloc, .{}, 21);
    const session_mutation = try registry.mutation(alloc, session.txn_id, session.version.owner);
    const first = (try registry.createNamedSavepoint(alloc, session_mutation, "point")).?;
    _ = (try registry.createNamedSavepoint(alloc, session_mutation, "nested")).?;
    const shadow = (try registry.createNamedSavepoint(alloc, session_mutation, "point")).?;
    _ = (try registry.createNamedSavepoint(alloc, session_mutation, "after")).?;
    try std.testing.expectEqual(shadow.savepoint_id, (try registry.rollbackToNamedSavepoint(alloc, session_mutation, "point")).?.savepoint_id);
    try std.testing.expectEqual(@as(usize, 3), (try registry.getStatus(alloc, session.txn_id)).?.savepoint_count);
    try std.testing.expect((try registry.releaseNamedSavepoint(alloc, session_mutation, "after")) == null);
    _ = (try registry.releaseNamedSavepoint(alloc, session_mutation, "point")).?;
    try std.testing.expectEqual(first.savepoint_id, (try registry.rollbackToNamedSavepoint(alloc, session_mutation, "point")).?.savepoint_id);
    try std.testing.expectEqual(@as(usize, 1), (try registry.getStatus(alloc, session.txn_id)).?.savepoint_count);
    _ = (try registry.releaseNamedSavepoint(alloc, session_mutation, "point")).?;
    try std.testing.expectEqual(@as(usize, 0), (try registry.getStatus(alloc, session.txn_id)).?.savepoint_count);
}

test "SQL transaction options update only the untouched owner session" {
    const alloc = std.testing.allocator;
    var registry = SessionRegistry.init(null);
    defer registry.deinit(alloc);
    const session = try registry.beginForPrincipal(alloc, .{ .sql = .{
        .database = "app",
        .namespace = "public",
        .isolation = .read_committed,
        .mode = .read_write,
    } }, 9, "alice");
    const scope: @import("antfly_local_sources").sql_session.Scope = .{ .database = "app", .principal = "alice" };
    const guard = try registry.mutation(alloc, session.txn_id, 9);
    var invalid_guard = guard;
    invalid_guard.owner = 10;
    var wrong_scope = scope;
    wrong_scope.principal = "bob";
    try std.testing.expectError(error.SqlTransactionNotActive, registry.setSqlTransactionOptions(alloc, guard, wrong_scope, .repeatable_read, .read_only));
    try std.testing.expectError(error.SessionLeaseLost, registry.setSqlTransactionOptions(alloc, invalid_guard, scope, .repeatable_read, .read_only));
    try registry.setSqlTransactionOptions(alloc, guard, scope, .repeatable_read, .read_only);
    try registry.setSqlTransactionOptions(alloc, guard, scope, null, .read_only);
    var state = (try registry.getSqlState(alloc, session.txn_id)).?;
    defer state.deinit(alloc);
    try std.testing.expect(state.metadata.isolation == .repeatable_read);
    try std.testing.expect(state.metadata.mode == .read_only);
    _ = (try registry.createNamedSavepoint(alloc, guard, "started")).?;
    try std.testing.expectError(error.ActiveSqlTransaction, registry.setSqlTransactionOptions(alloc, guard, scope, null, .read_write));
}

test "durable SQL session rejects duplicate savepoint ids without double freeing overlays" {
    const alloc = std.testing.allocator;
    var registry = SessionRegistry.init(null);
    defer registry.deinit(alloc);
    const info = try registry.beginForPrincipal(alloc, .{ .sql = .{ .database = "app", .namespace = "public", .isolation = .read_committed, .mode = .read_write } }, 9, "alice");
    const info_mutation = try registry.mutation(alloc, info.txn_id, info.version.owner);
    _ = (try registry.createNamedSavepoint(alloc, info_mutation, "first")).?;
    _ = (try registry.createNamedSavepoint(alloc, info_mutation, "second")).?;
    const bytes = try encodeSessionRecord(alloc, registry.sessions.get(info.txn_id).?);
    defer alloc.free(bytes);
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, bytes, .{});
    defer parsed.deinit();
    const points = parsed.value.object.getPtr("savepoints").?.array.items;
    try std.testing.expectEqual(@as(usize, 2), points.len);
    try points[1].object.put(parsed.arena.allocator(), "id", points[0].object.get("id").?);
    const invalid = try std.json.Stringify.valueAlloc(alloc, parsed.value, .{});
    defer alloc.free(invalid);
    try std.testing.expectError(error.InvalidTransactionSessionRecord, decodeSessionRecord(alloc, info.txn_id, invalid));
    for ([_]std.json.Value{ .{ .integer = 0 }, .{ .integer = 3 }, .{ .number_string = "18446744073709551615" } }) |bad_id| {
        try points[1].object.put(parsed.arena.allocator(), "id", bad_id);
        const malformed = try std.json.Stringify.valueAlloc(alloc, parsed.value, .{});
        defer alloc.free(malformed);
        try std.testing.expectError(error.InvalidTransactionSessionRecord, decodeSessionRecord(alloc, info.txn_id, malformed));
    }
    try points[1].object.put(parsed.arena.allocator(), "id", .{ .integer = 2 });
    for ([_]i64{ 0, 2 }) |bad_next| {
        try parsed.value.object.put(parsed.arena.allocator(), "next_savepoint_id", .{ .integer = bad_next });
        const malformed = try std.json.Stringify.valueAlloc(alloc, parsed.value, .{});
        defer alloc.free(malformed);
        try std.testing.expectError(error.InvalidTransactionSessionRecord, decodeSessionRecord(alloc, info.txn_id, malformed));
    }
}

test "durable SQL session rejects malformed connection id without leaking principal" {
    const alloc = std.testing.allocator;
    var registry = SessionRegistry.init(null);
    defer registry.deinit(alloc);
    const info = try registry.beginForPrincipal(alloc, .{}, 9, "alice");
    const bytes = try encodeSessionRecord(alloc, registry.sessions.get(info.txn_id).?);
    defer alloc.free(bytes);
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, bytes, .{});
    defer parsed.deinit();
    try parsed.value.object.put(parsed.arena.allocator(), "connection_id", .{ .string = "gg" });
    const invalid = try std.json.Stringify.valueAlloc(alloc, parsed.value, .{});
    defer alloc.free(invalid);
    try std.testing.expectError(error.InvalidTransactionSessionRecord, decodeSessionRecord(alloc, info.txn_id, invalid));
}

test "SQL statement clock retains the authoritative transaction start across registry reload" {
    const alloc = std.testing.allocator;
    var backend = mem_backend.Backend.init(alloc, .{});
    defer backend.close();
    var store = try backend.runtimeStore(alloc, .{ .name = "system/sql-statement-clock" });
    defer store.deinit();
    var durable = DurableSessionStore.initRuntime(alloc, &store);
    var id: db_mod.types.TxnId = undefined;
    var started: u64 = undefined;
    {
        var registry = SessionRegistry.init(&durable);
        defer registry.deinit(alloc);
        const info = try registry.beginForPrincipal(alloc, .{ .sql = .{ .database = "app", .namespace = "public", .isolation = .read_committed, .mode = .read_only } }, 9, "alice");
        id = info.txn_id;
        started = info.begin_timestamp;
        var state = (try registry.getSqlState(alloc, id)).?;
        defer state.deinit(alloc);
        try std.testing.expectEqual(started, state.begin_timestamp);
    }
    var registry = SessionRegistry.init(&durable);
    defer registry.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), registry.sessions.count());
    var loaded = (try registry.getSqlState(alloc, id)).?;
    defer loaded.deinit(alloc);
    try std.testing.expectEqual(started, loaded.begin_timestamp);
    try std.testing.expectEqualStrings("app", loaded.metadata.database);
    try std.testing.expectEqual(@as(u64, 9), loaded.owner_node_id);
}

test "SQL session metadata and schema fences survive durable records" {
    const alloc = std.testing.allocator;
    var registry = SessionRegistry.init(null);
    defer registry.deinit(alloc);
    const info = try registry.beginForPrincipal(alloc, .{ .sql = .{ .database = "app", .namespace = "public", .isolation = .read_committed, .mode = .read_only } }, 9, "alice");
    try registry.requireActiveSqlReader(alloc, info.txn_id, "alice", 9);
    try std.testing.expectError(error.SessionLeaseLost, registry.requireActiveSqlReader(alloc, info.txn_id, "alice", 10));
    try std.testing.expectError(error.SqlTransactionNotActive, registry.requireActiveSqlReader(alloc, info.txn_id, "bob", 9));
    const info_mutation = try registry.mutation(alloc, info.txn_id, info.version.owner);
    try registry.setSqlFailed(alloc, info_mutation, true);
    try std.testing.expectError(error.SqlTransactionNotActive, registry.requireActiveSqlReader(alloc, info.txn_id, "alice", 9));
    const session = registry.sessions.get(info.txn_id).?;
    const bytes = try encodeSessionRecord(alloc, session);
    defer alloc.free(bytes);
    var decoded = try decodeSessionRecord(alloc, info.txn_id, bytes);
    defer decoded.deinit(alloc);
    try std.testing.expectEqualStrings("app", decoded.sql.?.database);
    try std.testing.expectEqual(@import("antfly_local_sources").sql_session.Isolation.read_committed, decoded.sql.?.isolation);
    try std.testing.expect(decoded.sql.?.failed);
    var request: OwnedTransactionCommitRequest = .{};
    defer request.deinit(alloc);
    request.tables = try alloc.alloc(TableCommitRequest, 1);
    request.tables[0] = .{ .table_name = try alloc.dupe(u8, "table"), .relational_schema_version = 7, .schema_version = 7 };
    const stored = try encodeCommitRequestMode(alloc, request, true);
    defer alloc.free(stored);
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, stored, .{ .parse_numbers = false });
    defer parsed.deinit();
    var restored = try parseStoredCommitValue(alloc, parsed.value);
    defer restored.deinit(alloc);
    try std.testing.expectEqual(@as(?u32, 7), restored.tables[0].relational_schema_version);
    try std.testing.expectEqual(@as(?u32, 7), restored.tables[0].schema_version);
    const tables_view = try restored.distributedTables(alloc);
    defer tables_view.deinit();
    const tables = tables_view.tables;
    try std.testing.expectEqual(@as(?u32, 7), tables[0].relational_schema_version);
    try std.testing.expectEqual(@as(?u32, 7), tables[0].schema_version);
}

test "distributed txn SQL range guards survive durability and savepoint rollback without restoring writes" {
    const Case = struct {
        fn run(alloc: std.mem.Allocator) !void {
            const ranges = @import("antfly_local_sources").api_range_read_guards;
            const observation = ranges.OwnerRangeProof{ .fence = .{ .metadata_group_id = 1, .metadata_incarnation = @splat('1'), .catalog_revision = 2, .table_id = 3, .topology_epoch = 4, .route = .{ .group_id = 5, .range_id = 6, .identity_namespace = .{ .table_id = 3, .shard_id = 5, .range_id = 6 } } }, .proofs = &.{.{ .bucket = 98, .generation = std.math.maxInt(u64) }} };
            const source_observation = ranges.OwnerRangeProof{ .fence = .{ .metadata_group_id = 1, .metadata_incarnation = @splat('1'), .catalog_revision = 2, .table_id = 4, .topology_epoch = 4, .route = .{ .group_id = 7, .range_id = 8, .identity_namespace = .{ .table_id = 4, .shard_id = 7, .range_id = 8 } } }, .proofs = &.{.{ .bucket = 99, .generation = 43 }} };
            var registry = SessionRegistry.init(null);
            defer registry.deinit(alloc);
            const session = try registry.begin(alloc, .{ .sql = .{ .database = "default", .namespace = "public", .isolation = .serializable, .mode = .read_write } }, 1);
            const session_mutation = try registry.mutation(alloc, session.txn_id, session.version.owner);
            _ = try registry.createNamedSavepoint(alloc, session_mutation, "before");
            var request = try parseCommitRequest(alloc, "{\"read_set\":[],\"tables\":{\"docs\":{\"inserts\":{\"a\":{\"v\":1}}},\"source\":{}}}");
            defer request.deinit(alloc);
            try request.bind(alloc, "docs", "physical:3");
            try request.bind(alloc, "source", "physical:4");
            request.tables[0].schema_version = 7;
            request.tables[1].schema_version = 8;
            try request.tables[0].mergeRangeGuards(alloc, &.{observation});
            try request.tables[1].mergeRangeGuards(alloc, &.{source_observation});
            _ = try registry.stage(alloc, session_mutation, &request);
            _ = try registry.rollbackToNamedSavepoint(alloc, session_mutation, "before");
            var restored = try registry.cloneSqlStaged(alloc, session.txn_id);
            defer restored.deinit(alloc);
            try std.testing.expectEqual(@as(usize, 2), restored.tables.len);
            try std.testing.expectEqual(@as(usize, 0), restored.tables[0].batch.writes.len);
            try std.testing.expectEqual(@as(usize, 0), restored.tables[1].batch.writes.len);
            try std.testing.expectEqualStrings("physical:3", restored.physicalName("docs"));
            try std.testing.expectEqualStrings("physical:4", restored.physicalName("source"));
            try std.testing.expectEqual(@as(?u64, std.math.maxInt(u64)), restored.tables[0].range_guards.?.value[0].proofs[0].generation);
            try std.testing.expectEqual(@as(?u64, 43), restored.tables[1].range_guards.?.value[0].proofs[0].generation);
            const encoded = try encodeCommitRequestMode(alloc, restored, true);
            defer alloc.free(encoded);
            var parsed = try std.json.parseFromSlice(std.json.Value, alloc, encoded, .{ .parse_numbers = false });
            defer parsed.deinit();
            var durable = try parseStoredCommitValue(alloc, parsed.value);
            defer durable.deinit(alloc);
            const routed_view = try durable.distributedTables(alloc);
            defer routed_view.deinit();
            const routed = routed_view.tables;
            try std.testing.expectEqual(@as(u64, 5), routed[0].range_guards[0].fence.route.group_id);
            try std.testing.expectEqual(@as(?u64, std.math.maxInt(u64)), routed[0].range_guards[0].proofs[0].generation);
            try std.testing.expectEqual(@as(u64, 7), routed[1].range_guards[0].fence.route.group_id);
            try std.testing.expectEqual(@as(u64, 8), routed[1].range_guards[0].fence.route.range_id);
            try std.testing.expectEqual(@as(u64, 4), routed[1].range_guards[0].fence.route.identity_namespace.table_id);
            try std.testing.expectEqual(@as(?u64, 43), routed[1].range_guards[0].proofs[0].generation);
            const public = try encodeCommitRequestMode(alloc, durable, false);
            defer alloc.free(public);
            try std.testing.expect(std.mem.indexOf(u8, public, "range_guards") == null);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}

test "distributed txn table views isolate statement scratch from request ownership" {
    const alloc = std.testing.allocator;
    var budget: @import("../pgwire/budget.zig").Budget = .{ .child = alloc, .limit = 1024 * 1024 };
    {
        var request = try parseCommitRequest(budget.allocator(),
            \\{"read_set":[],"tables":{"a":{"inserts":{"k1":{"n":1},"k2":{"n":2}}},"b":{"inserts":{"k3":{"n":3}}}}}
        );
        defer request.deinit(budget.allocator());
        request.tables[0].batch.writes[0].json_null_fields = try cloneJsonNullFields(budget.allocator(), &.{"nullable"});
        const owned_bytes = budget.used;
        // The SQL executor's conflict lookup receives an arena allocator even
        // though the staged request belongs to the connection budget. Destroy
        // that arena between lookups, then clean up the request via its owner.
        for (0..2) |_| {
            var scratch = std.heap.ArenaAllocator.init(alloc);
            defer scratch.deinit();
            const view = try request.distributedTables(scratch.allocator());
            defer view.deinit();
            try std.testing.expectEqual(owned_bytes, budget.used);
            try std.testing.expectEqual(@as(usize, 2), view.tables.len);
            for (request.tables, view.tables) |source, table| {
                try std.testing.expectEqual(source.batch.writes.len, table.writes.len);
                for (source.batch.writes, table.writes) |write, converted| {
                    try std.testing.expectEqual(write.key.ptr, converted.key.ptr);
                    try std.testing.expectEqual(write.value.ptr, converted.value.ptr);
                    try std.testing.expectEqualSlices([]const u8, write.json_null_fields, converted.json_null_fields);
                }
            }
        }
        const Harness = struct {
            fn run(a: std.mem.Allocator, value: *const OwnedTransactionCommitRequest) !void {
                const view = try value.distributedTables(a);
                defer view.deinit();
                try std.testing.expectEqual(@as(usize, 2), view.tables.len);
            }
        };
        try std.testing.checkAllAllocationFailures(alloc, Harness.run, .{&request});
        try std.testing.expectEqual(owned_bytes, budget.used);
    }
    try std.testing.expectEqual(@as(usize, 0), budget.used);
    const empty: OwnedTransactionCommitRequest = .{};
    const view = try empty.distributedTables(alloc);
    defer view.deinit();
    try std.testing.expectEqual(@as(usize, 0), view.tables.len);
}

test "distributed txn SQL conflict guards retain first native observation through clone and durable round trip" {
    const Harness = struct {
        fn run(alloc: std.mem.Allocator) !void {
            const native = @import("antfly_local_sources").storage_db_relational_integrity_contract;
            const address = try native.Address.init(@splat(3), "\x00\xfftuple");
            const first = [_]native.Command{.{ .address = address, .operation = .{ .compare_claim = null } }};
            const later = [_]native.Command{.{ .address = address, .operation = .{ .compare_claim = .{ .tuple = "\x00\xfftuple", .parent_table = "table", .parent_key = "staged", .schema_version = 7 } } }};
            var original: TableCommitRequest = .{ .table_name = try alloc.dupe(u8, "table"), .relational_schema_version = 7 };
            defer original.deinit(alloc);
            try original.mergeConflictGuards(alloc, .{ .generation_set = @splat(255), .commands = &first });
            var request: OwnedTransactionCommitRequest = .{};
            defer request.deinit(alloc);
            const table = try original.clone(alloc);
            request.tables = alloc.alloc(TableCommitRequest, 1) catch |err| {
                var owned = table;
                owned.deinit(alloc);
                return err;
            };
            request.tables[0] = table;
            try request.tables[0].mergeConflictGuards(alloc, .{ .generation_set = @splat(255), .commands = &later });
            try std.testing.expectEqual(@as(usize, 1), request.tables[0].conflict_guards.?.value.commands.len);
            try std.testing.expect(request.tables[0].conflict_guards.?.value.commands[0].operation.compare_claim == null);
            const encoded = try encodeCommitRequestMode(alloc, request, true);
            defer alloc.free(encoded);
            var parsed = try std.json.parseFromSlice(std.json.Value, alloc, encoded, .{ .parse_numbers = false });
            defer parsed.deinit();
            var restored = try parseStoredCommitValue(alloc, parsed.value);
            defer restored.deinit(alloc);
            const tables_view = try restored.distributedTables(alloc);
            defer tables_view.deinit();
            const tables = tables_view.tables;
            try std.testing.expectEqualSlices(u8, &(@as([32]u8, @splat(255))), &tables[0].relational_integrity_generation_set.?);
            try std.testing.expectEqualDeep(address, tables[0].integrity_commands[0].address);
            try std.testing.expect(tables[0].integrity_commands[0].operation.compare_claim == null);
            var occupied: TableCommitRequest = .{ .table_name = try alloc.dupe(u8, "table") };
            defer occupied.deinit(alloc);
            try occupied.mergeConflictGuards(alloc, .{ .generation_set = @splat(255), .commands = &later });
            var occupied_copy = try occupied.clone(alloc);
            defer occupied_copy.deinit(alloc);
            try std.testing.expectEqualStrings("\x00\xfftuple", occupied_copy.conflict_guards.?.value.commands[0].operation.compare_claim.?.tuple);
            try std.testing.expectError(error.CatalogGenerationChanged, occupied.mergeConflictGuards(alloc, .{ .generation_set = @splat(1), .commands = &first }));
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{});
}

test "SQL staged statements coalesce writes deletes and retain first version" {
    const alloc = std.testing.allocator;
    var registry = SessionRegistry.init(null);
    defer registry.deinit(alloc);
    const info = try registry.beginForPrincipal(alloc, .{ .sql = .{ .database = "default", .namespace = "public", .isolation = .read_committed, .mode = .read_write } }, 1, "alice");
    var first = try parseCommitRequest(alloc, "{\"read_set\":[{\"table\":\"t\",\"key\":\"a\",\"version\":\"0\"}],\"tables\":{\"t\":{\"inserts\":{\"a\":{\"n\":1}}}}}");
    defer first.deinit(alloc);
    const info_mutation = try registry.mutation(alloc, info.txn_id, info.version.owner);
    _ = try registry.stageSql(alloc, info_mutation, &first);
    _ = try registry.createNamedSavepoint(alloc, info_mutation, "inserted");
    var deletion = try parseCommitRequest(alloc, "{\"read_set\":[],\"tables\":{\"t\":{\"deletes\":[\"a\"]}}}");
    defer deletion.deinit(alloc);
    _ = try registry.stageSql(alloc, info_mutation, &deletion);
    var deleted = try registry.cloneSqlStaged(alloc, info.txn_id);
    defer deleted.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), deleted.tables[0].batch.writes.len);
    try std.testing.expectEqual(@as(usize, 1), deleted.tables[0].batch.deletes.len);
    _ = try registry.stageSql(alloc, info_mutation, &first);
    var restored = try registry.cloneSqlStaged(alloc, info.txn_id);
    defer restored.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), restored.tables[0].batch.writes.len);
    try std.testing.expectEqual(@as(usize, 0), restored.tables[0].batch.deletes.len);
    try std.testing.expectEqual(@as(usize, 1), restored.tables[0].predicates.items.len);
    try std.testing.expectEqual(@as(u64, 0), restored.tables[0].predicates.items[0].expected_version);
}

test "transaction session registry enforces savepoint limits and reports remaining capacity" {
    var registry = SessionRegistry.initWithOptions(null, null, null, 1, null, null);
    defer registry.deinit(std.testing.allocator);
    const session = try registry.begin(std.testing.allocator, .{ .sync_level = .write }, 21);

    const session_mutation = try registry.mutation(std.testing.allocator, session.txn_id, session.version.owner);
    _ = (try registry.createSavepoint(std.testing.allocator, session_mutation)) orelse return error.TestExpectedEqual;
    try std.testing.expectError(error.SavepointLimitExceeded, registry.createSavepoint(std.testing.allocator, session_mutation));

    const status = (try registry.getStatus(std.testing.allocator, session.txn_id)) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(usize, 1), status.savepoint_count);
    try std.testing.expectEqual(@as(usize, 1), status.savepoint_limit.?);
    try std.testing.expectEqual(@as(usize, 0), status.remaining_savepoints.?);
}

test "transaction session responses summarize lease state" {
    const now_ns = nextTxnTimestamp();
    const held = SessionStatus{
        .txn_id = newSessionTxnId(1),
        .owner_node_id = 1,
        .begin_timestamp = now_ns,
        .last_touched_timestamp = now_ns,
        .lease_expires_at = now_ns + std.time.ns_per_s,
        .sync_level = .write,
        .staged_table_count = 0,
        .staged_read_count = 0,
        .staged_write_count = 0,
        .staged_delete_count = 0,
        .read_snapshot_count = 0,
        .savepoint_count = 0,
        .durable = true,
    };
    const expired = SessionStatus{
        .txn_id = newSessionTxnId(2),
        .owner_node_id = 2,
        .begin_timestamp = now_ns,
        .last_touched_timestamp = now_ns,
        .lease_expires_at = now_ns -| std.time.ns_per_s,
        .sync_level = .write,
        .staged_table_count = 0,
        .staged_read_count = 0,
        .staged_write_count = 0,
        .staged_delete_count = 0,
        .read_snapshot_count = 0,
        .savepoint_count = 0,
        .durable = true,
    };
    const none = SessionStatus{
        .txn_id = newSessionTxnId(3),
        .owner_node_id = 3,
        .begin_timestamp = now_ns,
        .last_touched_timestamp = now_ns,
        .lease_expires_at = 0,
        .sync_level = .write,
        .staged_table_count = 0,
        .staged_read_count = 0,
        .staged_write_count = 0,
        .staged_delete_count = 0,
        .read_snapshot_count = 0,
        .savepoint_count = 0,
        .durable = true,
    };

    const encoded = try encodeSessionListResponse(std.testing.allocator, &.{ held, expired, none });
    defer std.testing.allocator.free(encoded);
    var parsed = try std.json.parseFromSlice(SessionListResponse, std.testing.allocator, encoded, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(usize, 3), parsed.value.session_count);
    try std.testing.expectEqual(@as(usize, 1), parsed.value.lease_held_count);
    try std.testing.expectEqual(@as(usize, 1), parsed.value.lease_expired_count);
    try std.testing.expectEqualStrings("held", parsed.value.sessions[0].lease_state);
    try std.testing.expectEqualStrings("expired", parsed.value.sessions[1].lease_state);
    try std.testing.expectEqualStrings("none", parsed.value.sessions[2].lease_state);
}

test "session cleanup response encodes removed count and cutoff" {
    const encoded = try encodeSessionCleanupResponse(std.testing.allocator, 3, 99);
    defer std.testing.allocator.free(encoded);

    var parsed = try std.json.parseFromSlice(SessionCleanupResponse, std.testing.allocator, encoded, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(usize, 3), parsed.value.removed);
    try std.testing.expectEqual(@as(u64, 99), parsed.value.cutoff_ns);
}

test "transaction session lease expiry conversion saturates corrupt values" {
    try std.testing.expectEqual(
        std.math.maxInt(u64),
        leaseExpiryNs(std.math.maxInt(u64)),
    );
    try std.testing.expectEqual(
        @as(u64, 42 * std.time.ns_per_ms),
        leaseExpiryNs(42),
    );
}

test "transaction session conflict responses include version details" {
    const encoded = try encodeSessionCommitResponse(
        std.testing.allocator,
        newSessionTxnId(4),
        "aborted",
        versionConflict("docs", "doc:a", 7, 8),
        null,
    );
    defer std.testing.allocator.free(encoded);
    var parsed = try std.json.parseFromSlice(SessionCommitResponse, std.testing.allocator, encoded, .{});
    defer parsed.deinit();
    const conflict = parsed.value.conflict.?;
    try std.testing.expectEqualStrings("version_conflict", conflict.kind);
    try std.testing.expectEqual(@as(?u64, 7), conflict.expected_version);
    try std.testing.expectEqual(@as(?u64, 8), conflict.current_version);
}

test "transaction session registry can renew owned leases opportunistically" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/txn-session-opportunistic-renew-store", .{tmp.sub_path});
    defer std.testing.allocator.free(path);
    const path_z = try std.testing.allocator.dupeSentinel(u8, path, 0);
    defer std.testing.allocator.free(path_z);

    var store = try docstore_mod.DocStore.open(std.testing.allocator, path_z, .{});
    defer store.close();
    var durable = DurableSessionStore.init(std.testing.allocator, &store);
    const lease_store = SessionLeaseStore.init(std.testing.allocator, &store);

    var registry = SessionRegistry.initWithLeaseTtl(&durable, lease_store, 10 * std.time.ns_per_ms);
    defer registry.deinit(std.testing.allocator);
    const session = try registry.begin(std.testing.allocator, .{ .sync_level = .write }, 21);

    const initial = (try registry.getStatus(std.testing.allocator, session.txn_id)) orelse return error.TestExpectedEqual;
    const renewed_now = initial.lease_expires_at + 2 * std.time.ns_per_ms;
    try std.testing.expectEqual(@as(usize, 1), try registry.renewOwnedLeases(std.testing.allocator, 21, renewed_now));
    const renewed = (try registry.getStatus(std.testing.allocator, session.txn_id)) orelse return error.TestExpectedEqual;
    try std.testing.expect(renewed.lease_expires_at > initial.lease_expires_at);
}

test "transaction session commit response includes retry hints for topology conflicts" {
    const txn_id = newSessionTxnId(13);
    const encoded = try encodeSessionCommitResponse(
        std.testing.allocator,
        txn_id,
        "aborted",
        topologyChangedConflict("docs"),
        null,
    );
    defer std.testing.allocator.free(encoded);

    var parsed = try std.json.parseFromSlice(SessionCommitResponse, std.testing.allocator, encoded, .{});
    defer parsed.deinit();
    const conflict = parsed.value.conflict.?;
    try std.testing.expectEqualStrings("topology_changed", conflict.kind);
    try std.testing.expectEqual(true, conflict.retryable);
    try std.testing.expectEqual(@as(?u32, 100), conflict.retry_after_ms);
    try std.testing.expectEqualStrings("topology", conflict.retry_scope.?);
}

test "transaction session commit response includes retry hints for session lease conflicts" {
    const txn_id = newSessionTxnId(14);
    const encoded = try encodeSessionCommitResponse(
        std.testing.allocator,
        txn_id,
        "aborted",
        sessionLeaseLostConflict("docs"),
        null,
    );
    defer std.testing.allocator.free(encoded);

    var parsed = try std.json.parseFromSlice(SessionCommitResponse, std.testing.allocator, encoded, .{});
    defer parsed.deinit();
    const conflict = parsed.value.conflict.?;
    try std.testing.expectEqualStrings("session_lease_lost", conflict.kind);
    try std.testing.expectEqual(true, conflict.retryable);
    try std.testing.expectEqual(@as(?u32, 25), conflict.retry_after_ms);
    try std.testing.expectEqualStrings("session", conflict.retry_scope.?);
}

test "transaction session commit response includes retry hints for participant availability conflicts" {
    const txn_id = newSessionTxnId(15);
    const encoded = try encodeSessionCommitResponse(
        std.testing.allocator,
        txn_id,
        "aborted",
        participantUnavailableConflict("docs"),
        null,
    );
    defer std.testing.allocator.free(encoded);

    var parsed = try std.json.parseFromSlice(SessionCommitResponse, std.testing.allocator, encoded, .{});
    defer parsed.deinit();
    const conflict = parsed.value.conflict.?;
    try std.testing.expectEqualStrings("participant_unavailable", conflict.kind);
    try std.testing.expectEqual(true, conflict.retryable);
    try std.testing.expectEqual(@as(?u32, 50), conflict.retry_after_ms);
    try std.testing.expectEqualStrings("participant", conflict.retry_scope.?);
    try std.testing.expect(conflict.participant == null);
}

test "transaction session commit response includes retry hints for doc identity availability conflicts" {
    const txn_id = newSessionTxnId(16);
    const encoded = try encodeSessionCommitResponse(
        std.testing.allocator,
        txn_id,
        "aborted",
        docIdentityUnavailableConflict("docs"),
        null,
    );
    defer std.testing.allocator.free(encoded);

    var parsed = try std.json.parseFromSlice(SessionCommitResponse, std.testing.allocator, encoded, .{});
    defer parsed.deinit();
    const conflict = parsed.value.conflict.?;
    try std.testing.expectEqualStrings("doc_identity_unavailable", conflict.kind);
    try std.testing.expectEqual(true, conflict.retryable);
    try std.testing.expectEqual(@as(?u32, 100), conflict.retry_after_ms);
    try std.testing.expectEqualStrings("doc_identity", conflict.retry_scope.?);
    try std.testing.expect(conflict.participant == null);
}

test "transaction commit response includes participant group diagnostics" {
    const encoded = try encodeCommitResponse(std.testing.allocator, "aborted", .{
        .table_name = "docs",
        .key = "",
        .message = "participant unavailable",
        .group_id = 7001,
        .phase = .prepare,
        .kind = .participant_unavailable,
        .retryable = true,
        .retry_after_ms = 50,
        .retry_scope = "participant",
    }, null);
    defer std.testing.allocator.free(encoded);

    var parsed = try std.json.parseFromSlice(CommitResponse, std.testing.allocator, encoded, .{});
    defer parsed.deinit();
    const conflict = parsed.value.conflict.?;
    try std.testing.expectEqualStrings("participant_unavailable", conflict.kind);
    const participant = conflict.participant.?;
    try std.testing.expectEqual(@as(?u64, 7001), participant.group_id);
    try std.testing.expectEqualStrings("prepare", participant.phase.?);
}

test "SQL pgwire bound staged and savepoint-only sessions require the bound-session decoder" {
    const a = std.testing.allocator;
    for (0..3) |mode| {
        var session: Session = .{ .txn_id = @splat(1), .owner_node_id = 1, .begin_timestamp = 1, .last_touched_timestamp = 1, .sync_level = .write };
        defer session.deinit(a);
        const bound: OwnedTransactionCommitRequest = .{ .bound_catalog_incarnation = @splat(17) };
        if (mode == 1) session.staged = bound;
        if (mode == 2) try session.savepoints.put(a, 1, .{ .id = 1, .snapshot = bound });
        const bytes = try encodeSessionRecord(a, session);
        defer a.free(bytes);
        var recovered = try decodeSessionRecord(a, session.txn_id, bytes);
        defer recovered.deinit(a);
        if (mode == 1) try std.testing.expectEqual(bound.bound_catalog_incarnation, recovered.staged.?.bound_catalog_incarnation);
        if (mode == 2) try std.testing.expectEqual(bound.bound_catalog_incarnation, recovered.savepoints.get(1).?.snapshot.bound_catalog_incarnation);
        const frame = try session_record.encode(a, .{ .changes = &.{.{ .key = session_record.session_prefix ++ "id", .expected = null, .value = bytes }} });
        defer a.free(frame);
        try std.testing.expectEqual(@as(u8, if (mode == 0) 1 else 2), frame[0]);
        frame[0] = if (mode == 0) 2 else 1;
        try std.testing.expectError(error.InvalidTransactionSessionRecord, session_record.decode(a, frame));
    }
}

test "distributed txn session preserves numeric tokens across staging savepoints and durable recovery" {
    const alloc = std.testing.allocator;
    const row = "{\"id\":9007199254740993.0,\"min\":-9223372036854775808e0}";
    var session: Session = .{
        .txn_id = @splat(1),
        .owner_node_id = std.math.maxInt(u64),
        .begin_timestamp = 42,
        .last_touched_timestamp = 43,
        .sync_level = .write,
        .staged = try parseStageWriteRequest(alloc, "{\"table\":\"docs\",\"key\":\"a\",\"document\":" ++ row ++ "}"),
    };
    defer session.deinit(alloc);
    try std.testing.expectEqualStrings(row, session.staged.?.tables[0].batch.writes[0].value);
    const commit = "{\"read_set\":[],\"tables\":{\"docs\":{\"inserts\":{\"a\":" ++ row ++ "}}}}";
    var staged = try parseCommitRequest(alloc, commit);
    defer staged.deinit(alloc);
    try staged.setConstraintTiming(alloc, .{ .deferred = true });
    try session.staged.?.setConstraintTiming(alloc, .{ .generation = @splat(7), .deferred = false });
    try std.testing.expectEqualStrings(row, staged.tables[0].batch.writes[0].value);
    try upsertReadSnapshot(alloc, &session.read_snapshots, .{ .table_name = "docs", .key = "a", .version = 44, .document_json = row });
    try session.savepoints.put(alloc, 1, .{ .id = 1, .name = try alloc.dupe(u8, "before_update"), .snapshot = try staged.clone(alloc), .read_snapshots = try cloneReadSnapshotMap(alloc, session.read_snapshots) });
    session.next_savepoint_id = 2;
    const bytes = try encodeSessionRecord(alloc, session);
    defer alloc.free(bytes);
    var restored = try decodeSessionRecord(alloc, session.txn_id, bytes);
    defer restored.deinit(alloc);
    try std.testing.expectEqual(session.owner_node_id, restored.owner_node_id);
    try std.testing.expectEqualStrings(row, restored.staged.?.tables[0].batch.writes[0].value);
    try std.testing.expectEqualStrings(row, restored.read_snapshots.values()[0].document_json.?);
    try std.testing.expectEqualStrings(row, restored.savepoints.get(1).?.snapshot.tables[0].batch.writes[0].value);
    try std.testing.expectEqualStrings(row, restored.savepoints.get(1).?.read_snapshots.values()[0].document_json.?);
    try std.testing.expectEqualStrings("before_update", restored.savepoints.get(1).?.name.?);
    try std.testing.expect(restored.savepoints.get(1).?.snapshot.constraint_timing.items[0].deferred);
    try std.testing.expectEqualDeep(@as(?[16]u8, @splat(7)), restored.staged.?.constraint_timing.items[0].generation);
    try std.testing.expect(!restored.staged.?.constraint_timing.items[0].deferred);
}

test "transaction catalog bindings persist privately and cannot be injected publicly" {
    const alloc = std.testing.allocator;
    const body = "{\"read_set\":[],\"tables\":{\"docs\":{\"inserts\":{\"a\":{}}}},\"bound_catalog_incarnation\":\"ffffffffffffffffffffffffffffffff\",\"catalog_bindings\":[{\"logical\":\"docs\",\"physical\":\"table:untrusted\"}]}";
    var request = try parseCommitRequest(alloc, body);
    defer request.deinit(alloc);
    try std.testing.expectEqualStrings("docs", request.physicalName("docs"));
    try std.testing.expect(request.bound_catalog_incarnation == null);
    request.bound_catalog_incarnation = @splat(7);
    try request.bind(alloc, "docs", "table:original");
    try std.testing.expectError(error.CatalogGenerationChanged, request.bind(alloc, "docs", "table:replacement"));
    const public = try encodeCommitRequest(alloc, request);
    defer alloc.free(public);
    try std.testing.expect(std.mem.indexOf(u8, public, "catalog_bindings") == null);
    try std.testing.expect(std.mem.indexOf(u8, public, "bound_catalog_incarnation") == null);
    const stored = try encodeCommitRequestMode(alloc, request, true);
    defer alloc.free(stored);
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, stored, .{});
    defer parsed.deinit();
    var restored = try parseStoredCommitValue(alloc, parsed.value);
    defer restored.deinit(alloc);
    try std.testing.expectEqualStrings("table:original", restored.physicalName("docs"));
    try std.testing.expectEqualStrings("docs", restored.logicalName("table:original"));
    var copied = try restored.clone(alloc);
    defer copied.deinit(alloc);
    try std.testing.expectEqual(request.bound_catalog_incarnation, copied.bound_catalog_incarnation);
    const different: OwnedTransactionCommitRequest = .{ .bound_catalog_incarnation = @splat(8) };
    try std.testing.expectError(error.CatalogGenerationChanged, copied.mergeFrom(alloc, &different));
    try std.testing.expectEqualStrings("table:original", copied.physicalName("docs"));
}

fn checkCatalogBoundRequestClone(alloc: std.mem.Allocator, request: OwnedTransactionCommitRequest) !void {
    var copied = try request.clone(alloc);
    defer copied.deinit(alloc);
}

test "SQL JSON null metadata survives stage clone savepoint persistence and recovery" {
    const alloc = std.testing.allocator;
    var request = try parseCommitRequest(alloc,
        \\{"read_set":[],"tables":{"docs":{"inserts":{"a":{"j":null}}}}}
    );
    defer request.deinit(alloc);
    request.tables[0].batch.writes[0].json_null_fields = try cloneJsonNullFields(alloc, &.{"j"});
    try std.testing.checkAllAllocationFailures(alloc, checkCatalogBoundRequestClone, .{request});
    const stored = try encodeCommitRequestMode(alloc, request, true);
    defer alloc.free(stored);
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, stored, .{});
    defer parsed.deinit();
    var restored = try parseStoredCommitValue(alloc, parsed.value);
    defer restored.deinit(alloc);
    try std.testing.expectEqualStrings("j", restored.tables[0].batch.writes[0].json_null_fields[0]);
    const view = try restored.distributedTables(alloc);
    defer view.deinit();
    try std.testing.expectEqualStrings("j", view.tables[0].writes[0].json_null_fields[0]);
    // Public JSON cannot forge the private persisted typing envelope.
    var public = try parseCommitRequest(alloc, stored);
    defer public.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), public.tables[0].batch.writes[0].json_null_fields.len);
}

test "transaction catalog binding clone releases partial allocations" {
    const alloc = std.testing.allocator;
    var request = try parseCommitRequest(alloc,
        \\{"read_set":[{"table":"docs","key":"a","version":"1"}],"tables":{"docs":{"inserts":{"a":{}},"deletes":["b"],"transforms":[{"key":"c","operations":[{"op":"$set","path":"status","value":"ready"}]}]}}}
    );
    defer request.deinit(alloc);
    try request.bind(alloc, "docs", "table:original");
    try std.testing.checkAllAllocationFailures(alloc, checkCatalogBoundRequestClone, .{request});
}

test "distributed txn session observed predicates survive private persistence without public injection or duplicate growth" {
    const alloc = std.testing.allocator;
    var request = try parseCommitRequest(alloc,
        \\{"read_set":[{"table":"docs","key":"a","version":"18446744073709551615"}],"tables":{"docs":{}},"observed_predicates":{"docs":[{"key":"forged","version":"1","digest":null}]}}
    );
    defer request.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), request.tables[0].predicates.items.len);
    const observations = [_]db_mod.types.TransactionVersionPredicate{
        .{ .key = "a", .expected_version = std.math.maxInt(u64), .expected_content_digest = @splat(255), .expected_mutation_revision = std.math.maxInt(u64) },
        .{ .key = "b", .expected_version = 0 },
    };
    try appendPredicates(alloc, &request.tables[0].predicates, &observations);
    const public = try encodeCommitRequest(alloc, request);
    defer alloc.free(public);
    try std.testing.expect(std.mem.indexOf(u8, public, "observed_predicates") == null);
    const stored = try encodeCommitRequestMode(alloc, request, true);
    defer alloc.free(stored);
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, stored, .{});
    defer parsed.deinit();
    var restored = try parseStoredCommitValue(alloc, parsed.value);
    defer restored.deinit(alloc);
    try restored.mergeFrom(alloc, &request);
    const predicates = restored.tables[0].predicates.items;
    try std.testing.expectEqual(@as(usize, 2), predicates.len);
    try std.testing.expectEqual(std.math.maxInt(u64), predicates[0].expected_version);
    try std.testing.expectEqualSlices(u8, &@as([32]u8, @splat(255)), &predicates[0].expected_content_digest.?);
    try std.testing.expectEqual(@as(?u64, std.math.maxInt(u64)), predicates[0].expected_mutation_revision);
    try std.testing.expectEqual(@as(u64, 0), predicates[1].expected_version);
}

test "lake SQL session cuts survive savepoint rollback and durable record recovery" {
    const a = std.testing.allocator;
    var registry = SessionRegistry.init(null);
    defer registry.deinit(a);
    const begun = try registry.beginForPrincipal(a, .{ .sql = .{ .database = "main", .namespace = "public", .isolation = .read_committed, .mode = .read_only } }, 7, "alice");
    const begun_mutation = try registry.mutation(a, begun.txn_id, begun.version.owner);
    const point = (try registry.createSavepoint(a, begun_mutation)).?;
    try registry.bindLakeCut(a, begun_mutation, "events", 19, "\"sql-lake1:original\"");
    _ = try registry.rollbackToSavepoint(a, begun_mutation, point.savepoint_id);
    var held = (try registry.getLakeCut(a, begun.txn_id, "events")).?;
    defer held.deinit(a);
    try std.testing.expectEqualStrings("\"sql-lake1:original\"", held.document_json.?);
    try std.testing.expectError(error.CatalogGenerationChanged, registry.bindLakeCut(a, begun_mutation, "events", 19, "\"sql-lake1:new\""));
    var session = (try registry.loadSessionCloneAssumeStripe(a, begun.txn_id)).?;
    defer session.deinit(a);
    const bytes = try encodeSessionRecord(a, session);
    defer a.free(bytes);
    var restored = try decodeSessionRecord(a, begun.txn_id, bytes);
    defer restored.deinit(a);
    try std.testing.expect(restored.sql.?.accepted_lake_reads);
    var recovered = (try cloneReadSnapshotForKey(a, &restored.lake_cuts, "events", "accepted")).?;
    defer recovered.deinit(a);
    try std.testing.expectEqualStrings(held.document_json.?, recovered.document_json.?);
}

test "SQL sequence durable session records retain issued values identities and full width cache epochs" {
    const a = std.testing.allocator;
    const sequence_state = @import("antfly_local_sources").sql_sequence_session;
    const identity: sequence_state.Identity = .{ .incarnation = @splat(2), .id = std.math.maxInt(u64) };
    const values: sequence_state.Snapshot = .{ .values = &.{.{ .identity = identity, .value = std.math.minInt(i64) }}, .last = identity };
    var registry = SessionRegistry.init(null);
    defer registry.deinit(a);
    const info = try registry.beginForPrincipal(a, .{ .sql = .{ .database = "app", .namespace = "public", .isolation = .read_committed, .mode = .read_write } }, 9, "alice");
    try registry.setSqlSequenceState(a, info.txn_id, "alice", 9, values, false);
    var original = try registry.sessions.get(info.txn_id).?.clone(a);
    defer original.deinit(a);
    original.sequence_epoch = std.math.maxInt(u64);
    const bytes = try encodeSessionRecord(a, original);
    defer a.free(bytes);
    var decoded = try decodeSessionRecord(a, info.txn_id, bytes);
    defer decoded.deinit(a);
    try std.testing.expectEqual(std.math.maxInt(u64), decoded.sequence_epoch);
    try std.testing.expectEqualDeep(values, decoded.sequence_state);
    var cloned = try decoded.clone(a);
    defer cloned.deinit(a);
    try std.testing.expectEqual(decoded.sequence_epoch, cloned.sequence_epoch);
    try std.testing.expectEqualDeep(values, cloned.sequence_state);
    try registry.setSqlSequenceState(a, info.txn_id, "alice", 9, .{}, true);
    var reset = (try registry.getSqlState(a, info.txn_id)) orelse return error.MissingSqlState;
    defer reset.deinit(a);
    try std.testing.expectEqual(@as(u64, 2), reset.sequence_epoch);
    try std.testing.expectEqual(@as(usize, 0), reset.sequence_state.values.len);
    try std.testing.expect(reset.sequence_state.last == null);
}

test "distributed txn SQL search paths retain local committed savepoint and recovery state" {
    const a = std.testing.allocator;
    const path_api = @import("../pgwire/search_path.zig");
    var first: path_api.Path = .{};
    try first.append(try path_api.Namespace.init("tenant"));
    try first.append(try path_api.Namespace.init("public"));
    var second: path_api.Path = .{};
    try second.append(try path_api.Namespace.init("public"));
    var registry = SessionRegistry.init(null);
    defer registry.deinit(a);
    const begun = try registry.beginForPrincipal(a, .{ .sql = .{ .database = "default", .namespace = "public", .isolation = .read_committed, .mode = .read_write } }, 7, "alice");
    try registry.setSqlSearchPath(a, begun.txn_id, "alice", first, false);
    const point = (try registry.createNamedSavepoint(a, try registry.mutation(a, begun.txn_id, begun.version.owner), "path_guard")).?;
    try registry.setSqlSearchPath(a, begun.txn_id, "alice", second, true);
    var candidate = (try registry.loadSessionCloneAssumeStripe(a, begun.txn_id)).?;
    defer candidate.deinit(a);
    const encoded = try encodeSessionRecord(a, candidate);
    defer a.free(encoded);
    var decoded = try decodeSessionRecord(a, begun.txn_id, encoded);
    defer decoded.deinit(a);
    try std.testing.expectEqualDeep(second, decoded.sql.?.search_path_active.?);
    try std.testing.expectEqualDeep(first, decoded.sql.?.search_path_committed.?);
    try std.testing.expectEqualDeep(first, decoded.savepoints.get(point.savepoint_id).?.search_path_active.?);
    try std.testing.expectError(error.Forbidden, registry.setSqlSearchPath(a, begun.txn_id, "mallory", null, false));
    // Rollback uses the same durable savepoint fields as ordinary settings.
    _ = try registry.rollbackToSavepoint(a, try registry.mutation(a, begun.txn_id, begun.version.owner), point.savepoint_id);
    var state = (try registry.getSqlState(a, begun.txn_id)).?;
    defer state.deinit(a);
    try std.testing.expectEqualDeep(first, state.metadata.search_path_active.?);
    try std.testing.expectEqualDeep(first, state.metadata.search_path_committed.?);
    try std.testing.expect(!state.metadata.characteristics_locked);
}

test "SQL privileges staged intent survives private recovery without public injection or native authority loss" {
    const a = std.testing.allocator;
    const Access = @import("antfly_local_sources").system_catalog_privileges.Access;
    const body = "{\"read_set\":[],\"tables\":{\"docs\":{\"inserts\":{\"a\":{}}}},\"sql_authorization\":{\"docs\":{\"principal_instance\":[1],\"table_id\":9,\"required\":255,\"native_rights\":255}}}";
    var request = try parseCommitRequest(a, body);
    defer request.deinit(a);
    try std.testing.expect(request.tables[0].sql_authorization == null);
    request.tables[0].schema_version = 3;
    request.tables[0].sql_authorization = .{ .row_filter_digest = @splat(0), .principal_instance = @splat(7), .table_id = 9, .required = Access.bit(.insert), .native_rights = 0 };
    try request.bind(a, "docs", "table:9");
    const public = try encodeCommitRequest(a, request);
    defer a.free(public);
    try std.testing.expect(std.mem.indexOf(u8, public, "sql_authorization") == null);
    const stored = try encodeCommitRequestMode(a, request, true);
    defer a.free(stored);
    var parsed = try std.json.parseFromSlice(std.json.Value, a, stored, .{});
    defer parsed.deinit();
    var restored = try parseStoredCommitValue(a, parsed.value);
    defer restored.deinit(a);
    try std.testing.expectEqualDeep(request.tables[0].sql_authorization, restored.tables[0].sql_authorization);
    var copied = try restored.clone(a);
    defer copied.deinit(a);
    const update: TableCommitRequest = .{ .table_name = @constCast("docs"), .sql_authorization = .{ .row_filter_digest = @splat(0), .principal_instance = @splat(7), .table_id = 9, .required = Access.bit(.update), .native_rights = 0 } };
    try copied.tables[0].mergeFrom(a, update);
    try std.testing.expectEqual(Access.bit(.insert) | Access.bit(.update), copied.tables[0].sql_authorization.?.required);
    var foreign = update;
    foreign.sql_authorization.?.principal_instance = @splat(8);
    try std.testing.expectError(error.CatalogGenerationChanged, copied.tables[0].mergeFrom(a, foreign));
    foreign = update;
    foreign.sql_authorization.?.table_id = 10;
    try std.testing.expectError(error.CatalogGenerationChanged, copied.tables[0].mergeFrom(a, foreign));
    foreign = update;
    foreign.sql_authorization.?.row_filter_digest = @splat(9);
    try std.testing.expectError(error.Forbidden, copied.tables[0].mergeFrom(a, foreign));
    var native = try parseStageDeleteRequest(a, "{\"table\":\"docs\",\"key\":\"b\"}");
    defer native.deinit(a);
    try copied.tables[0].mergeFrom(a, native.tables[0]);
    try std.testing.expect(copied.tables[0].sql_authorization.?.native_write);
    // Combining native writes with a later SQL statement must preserve the
    // native requirement regardless of statement order.
    try native.tables[0].mergeFrom(a, request.tables[0]);
    try std.testing.expect(native.tables[0].sql_authorization.?.native_write);
    var authority = copied.tables[0].sql_authorization.?;
    authority.required = 0;
    try std.testing.expectError(error.InvalidTransactionSessionRecord, authority.validate());
    authority.required = 256;
    try std.testing.expectError(error.InvalidTransactionSessionRecord, authority.validate());
}

test "SQL domain session handoffs survive clone recovery and raw overwrite without public authority" {
    const a = std.testing.allocator;
    const handoff = batch_api.DomainHandoff;
    var request = try parseCommitRequest(a,
        \\{"read_set":[],"tables":{"docs":{"inserts":{"a":{"v":null},"b":{"v":"raw"}}}}}
    );
    defer request.deinit(a);
    const contract: handoff.Contract = .{ .schema_version = 7, .domain_digest = @splat(9) };
    request.tables[0].batch.prepared_domain_rows = try handoff.attach(a, request.tables[0].batch.writes, .{ .contract = contract, .row_indices = &.{0} });
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, checkCatalogBoundRequestClone, .{request});
    const stored = try encodeCommitRequestMode(a, request, true);
    defer a.free(stored);
    var parsed = try std.json.parseFromSlice(std.json.Value, a, stored, .{});
    defer parsed.deinit();
    var restored = try parseStoredCommitValue(a, parsed.value);
    defer restored.deinit(a);
    try std.testing.expect(contract.eql(restored.tables[0].batch.writes[0].prepared_domain.?.*));
    try std.testing.expect(restored.tables[0].batch.writes[1].prepared_domain == null);
    try std.testing.expect(restored.tables[0].batch.writes[0].prepared_domain != request.tables[0].batch.writes[0].prepared_domain);
    const distributed = try restored.distributedTables(a);
    defer distributed.deinit();
    try std.testing.expect(contract.eql(distributed.tables[0].writes[0].prepared_domain.?.*));
    var public = try parseCommitRequest(a, stored);
    defer public.deinit(a);
    try std.testing.expect(!handoff.present(public.tables[0].batch.writes));
    const ordinary = try encodeCommitRequest(a, request);
    defer a.free(ordinary);
    try std.testing.expect(std.mem.indexOf(u8, ordinary, "prepared_domain") == null);
    try appendBatchWrites(a, &restored.tables[0].batch, &.{.{ .key = "a", .value = "{\"v\":\"replacement\"}" }});
    try std.testing.expectEqual(@as(usize, 2), restored.tables[0].batch.writes.len);
    try std.testing.expect(!handoff.present(restored.tables[0].batch.writes));
    try std.testing.expect(restored.tables[0].batch.prepared_domain_rows == null);
    try appendBatchWrites(a, &restored.tables[0].batch, request.tables[0].batch.writes[0..1]);
    try std.testing.expect(contract.eql(restored.tables[0].batch.writes[0].prepared_domain.?.*));
}

test "distributed txn external session purpose and delivery receipts survive cold recovery without ordinary commit" {
    const a = std.testing.allocator;
    var backend = mem_backend.Backend.init(a, .{});
    defer backend.close();
    var store = try backend.runtimeStore(a, .{ .name = "system/external-transaction-sessions" });
    defer store.deinit();
    var durable = DurableSessionStore.initRuntime(a, &store);
    const types = @import("../system_catalog/prepared_transactions.zig");
    const tables = [_]distributed_txn.TableCommitRequest{.{ .table_name = "docs", .row_policy_principal_proof = "exact-policy", .row_policy_database = "default", .row_policy_admitted_at_seconds = 1_800_000_000, .writes = &.{.{ .key = "key", .value = "{\"value\":1}" }} }};
    const encoded = try encodeExecutionPlan(a, &tables);
    defer a.free(encoded);
    var txn_id: db_mod.types.TxnId = undefined;
    var receipt: types.Receipt = undefined;
    {
        var writer = SessionRegistry.init(&durable);
        defer writer.deinit(a);
        const session = try writer.begin(a, .{ .sql = .{ .database = "default", .namespace = "public", .isolation = .read_committed, .mode = .read_write } }, 7);
        txn_id = session.txn_id;
        const identity: types.Identity = .{ .session = .{ .owner_node_id = 7 }, .gid = "session-cut", .database_id = 1, .principal = .bootstrap, .txn_id = txn_id, .operation_id = @splat(3), .metadata_incarnation = @splat('1'), .begin_timestamp = session.begin_timestamp, .expires_at_unix_ns = 100, .plan_digest = try types.planDigest(encoded), .manifest_digest = try types.manifestDigest(a, &.{}, @splat('1')) };
        const seal: ExternalPreparationSeal = .{ .admission_head = 0, .admitted_at_ns = 1, .standalone = false, .identity = identity, .actor = .{ .session_user = .bootstrap, .credential = "" }, .participants = &.{} };
        durable.fail_writes_for_test = true;
        try std.testing.expectError(error.InjectedSessionStoreFailure, writer.sealExternalExecutionPlan(a, txn_id, seal, &tables));
        try std.testing.expect((try writer.externalPreparationSeal(a, txn_id)) == null);
        try std.testing.expect((try writer.getExecutionPlan(a, try writer.mutation(a, txn_id, 7))) == null);
        durable.fail_writes_for_test = false;
        var plan = (try writer.sealExternalExecutionPlan(a, txn_id, seal, &tables)).?;
        defer plan.deinit();
        try std.testing.expectEqualStrings("exact-policy", plan.value[0].row_policy_principal_proof);
        var retry = (try writer.sealExternalExecutionPlan(a, txn_id, seal, &.{.{ .table_name = "different" }})).?;
        defer retry.deinit();
        try std.testing.expectEqualStrings("docs", retry.value[0].table_name);
        try std.testing.expectError(error.TransactionCommitSealed, writer.sealExecutionPlan(a, try writer.mutation(a, txn_id, 7), &tables));
        try std.testing.expect(!writer.removeBeforeExecution(a, txn_id));
        receipt = .{ .owner = try identity.owner(a), .txn_id = txn_id, .gid = "session-cut", .session = identity.session.?, .status = .pending };
        try std.testing.expectError(error.ExternalTransactionDecisionChanged, writer.recordExternalPreparationReceipt(a, txn_id, receipt));
    }
    {
        var recovered = SessionRegistry.init(&durable);
        defer recovered.deinit(a);
        var work = (try recovered.claimPendingRecovery(a, txn_id, 7, nextTxnTimestamp())).?;
        defer work.deinit(a);
        try std.testing.expect(work == .external_preparation);
        try std.testing.expect(!work.external_preparation.prepared and !work.external_preparation.aborted);
        try std.testing.expectEqual(@as(usize, 0), try recovered.cleanupExpired(a, std.math.maxInt(u64)));
        receipt.status = .prepared;
        var wrong_kind = receipt;
        wrong_kind.kind = .session_commit;
        wrong_kind.status = .committed;
        try std.testing.expectError(error.ExternalTransactionDecisionChanged, recovered.recordExternalPreparationReceipt(a, txn_id, wrong_kind));
        var wrong = receipt;
        wrong.owner.operation_id = @splat(9);
        try std.testing.expectError(error.ExternalTransactionDecisionChanged, recovered.recordExternalPreparationReceipt(a, txn_id, wrong));
        _ = (try recovered.recordExternalPreparationReceipt(a, txn_id, receipt)).?;
        var state = (try recovered.getSqlState(a, txn_id)).?;
        defer state.deinit(a);
        try std.testing.expect(state.terminal_prepare);
        try std.testing.expect(state.terminal == null and !state.terminal_abort);
        var persisted = (try durable.load(txn_id)).?;
        defer persisted.deinit(a);
        try std.testing.expect(persisted.execution_plan == null and persisted.staged == null);
        try std.testing.expect(persisted.external_preparation != null);
        try std.testing.expectEqual(@as(usize, 0), persisted.savepoints.count());
        try std.testing.expectEqual(@as(usize, 0), try recovered.cleanupExpired(a, std.math.maxInt(u64)));
    }
    {
        var recovered = SessionRegistry.init(&durable);
        defer recovered.deinit(a);
        var work = (try recovered.claimPendingRecovery(a, txn_id, 7, nextTxnTimestamp())).?;
        defer work.deinit(a);
        try std.testing.expect(work == .external_preparation and work.external_preparation.prepared);
        var wrong = receipt;
        wrong.status = .aborted;
        try std.testing.expectError(error.ExternalTransactionDecisionChanged, recovered.recordExternalPreparationReceipt(a, txn_id, wrong));
        _ = (try recovered.markExternalPreparationReceiptAcknowledged(a, txn_id)).?;
        try std.testing.expect((try recovered.claimPendingRecovery(a, txn_id, 7, nextTxnTimestamp())) == null);
        try std.testing.expectEqual(@as(usize, 1), try recovered.cleanupExpired(a, std.math.maxInt(u64)));
        try std.testing.expect(try durable.load(txn_id) == null);
    }
}

test "distributed txn terminal decision version codec is lossless and leaves absent-version records unchanged" {
    const a = std.testing.allocator;
    const id: db_mod.types.TxnId = @splat(8);
    var session: Session = .{ .txn_id = id, .owner_node_id = 7, .begin_timestamp = 1, .last_touched_timestamp = 2, .sync_level = .write, .commit_execution_started = true, .terminal_commit = .{ .status = .committed, .coordinator_acknowledged = true } };
    defer session.deinit(a);
    const old_bytes = try encodeSessionRecord(a, session);
    defer a.free(old_bytes);
    try std.testing.expect(std.mem.indexOf(u8, old_bytes, "\"commit_version\"") == null);
    var old = try decodeSessionRecord(a, id, old_bytes);
    defer old.deinit(a);
    try std.testing.expect(old.terminal_commit.?.commit_version == null);
    for ([_]u64{ 1, 9_007_199_254_740_993, std.math.maxInt(u64) }) |version| {
        session.terminal_commit.?.commit_version = version;
        const bytes = try encodeSessionRecord(a, session);
        defer a.free(bytes);
        var decoded = try decodeSessionRecord(a, id, bytes);
        defer decoded.deinit(a);
        try std.testing.expectEqual(@as(?u64, version), decoded.terminal_commit.?.commit_version);
        var cloned = try decoded.terminal_commit.?.clone(a);
        defer cloned.deinit(a);
        try std.testing.expectEqual(@as(?u64, version), cloned.commit_version);
    }
    session.terminal_commit.?.commit_version = 1;
    const valid = try encodeSessionRecord(a, session);
    defer a.free(valid);
    for ([_][]const u8{ "0", "-1", "1.5", "18446744073709551616", "true", "\"1\"" }) |invalid| {
        const replacement = try std.fmt.allocPrint(a, "\"commit_version\":{s}", .{invalid});
        defer a.free(replacement);
        const malformed = try std.mem.replaceOwned(u8, a, valid, "\"commit_version\":1", replacement);
        defer a.free(malformed);
        try std.testing.expectError(error.InvalidTransactionSessionRecord, decodeSessionRecord(a, id, malformed));
    }
}

test "distributed txn terminal decision version survives native LSM reopen failed persistence and visibility updates" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    for ([_]?u64{ null, 9_007_199_254_740_993, std.math.maxInt(u64) }, 0..) |version, number| {
        const path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/terminal-version-{d}", .{ tmp.sub_path, number }, 0);
        defer a.free(path);
        const id: db_mod.types.TxnId = @splat(@as(u8, @intCast(number + 1)));
        for (0..3) |phase| {
            var store = try docstore_mod.DocStore.open(a, path, .{});
            defer store.close();
            var durable = DurableSessionStore.init(a, &store);
            if (phase == 0) {
                const now = nextTxnTimestamp();
                var session: Session = .{ .txn_id = id, .owner_node_id = 7, .begin_timestamp = now, .last_touched_timestamp = now, .sync_level = .write, .commit_execution_started = true, .terminal_commit = .{ .status = .committed_visibility_pending, .repair_required = true, .commit_version = version } };
                defer session.deinit(a);
                try durable.saveForTest(session, null);
            } else {
                var sessions = SessionRegistry.init(&durable);
                defer sessions.deinit(a);
                var before = (try sessions.getTerminalCommit(a, id)).?;
                defer before.deinit(a);
                try std.testing.expectEqual(version, before.commit_version);
                if (phase == 1) {
                    durable.fail_writes_for_test = true;
                    try std.testing.expectError(error.InjectedSessionStoreFailure, sessions.recordTerminalCommitWithRepair(a, id, .committed, false, null, null));
                    durable.fail_writes_for_test = false;
                    var unchanged = (try durable.load(id)).?;
                    defer unchanged.deinit(a);
                    try std.testing.expectEqual(.committed_visibility_pending, unchanged.terminal_commit.?.status);
                    try std.testing.expectEqual(version, unchanged.terminal_commit.?.commit_version);
                    _ = (try sessions.recordTerminalCommitWithRepair(a, id, .committed, false, null, null)).?;
                    var after = (try sessions.getTerminalCommit(a, id)).?;
                    defer after.deinit(a);
                    try std.testing.expectEqual(version, after.commit_version);
                } else {
                    try std.testing.expectEqual(.committed, before.status);
                    try std.testing.expect(!before.repair_required);
                    _ = (try sessions.recordTerminalCommit(a, id, .committed, null, null)).?;
                    var repeated = (try sessions.getTerminalCommit(a, id)).?;
                    defer repeated.deinit(a);
                    try std.testing.expectEqual(version, repeated.commit_version);
                }
            }
        }
    }
}

test "distributed txn named delivery validates authority and survives every local durability boundary cold" {
    const a = std.testing.allocator;
    const P = @import("../system_catalog/prepared_transactions.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const tables = [_]distributed_txn.TableCommitRequest{.{ .table_name = "docs", .writes = &.{.{ .key = "key", .value = "{\"value\":1}" }} }};
    const encoded = try encodeExecutionPlan(a, &tables);
    defer a.free(encoded);
    for ([_]bool{ false, true }, 0..) |aborted, number| {
        const path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/named-delivery-{d}", .{ tmp.sub_path, number }, 0);
        defer a.free(path);
        var txn_id: db_mod.types.TxnId = undefined;
        var receipt: P.Receipt = undefined;
        for (0..4) |phase| {
            var store = try docstore_mod.DocStore.open(a, path, .{});
            defer store.close();
            var durable = DurableSessionStore.init(a, &store);
            var sessions = SessionRegistry.init(&durable);
            defer sessions.deinit(a);
            if (phase == 0) {
                const session = try sessions.begin(a, .{ .sql = .{ .database = "default", .namespace = "public", .isolation = .read_committed, .mode = .read_write } }, 7);
                txn_id = session.txn_id;
                const identity: P.Identity = .{ .session = .{ .owner_node_id = 7 }, .gid = "cold-named", .database_id = 1, .principal = .bootstrap, .txn_id = txn_id, .operation_id = @splat(3), .metadata_incarnation = @splat('1'), .begin_timestamp = session.begin_timestamp, .expires_at_unix_ns = 100, .plan_digest = try P.planDigest(encoded), .manifest_digest = try P.manifestDigest(a, &.{}, @splat('1')) };
                const seal: ExternalPreparationSeal = .{ .admission_head = 0, .admitted_at_ns = 1, .standalone = false, .identity = identity, .actor = .{ .session_user = .bootstrap, .credential = "" }, .participants = &.{} };
                var plan = (try sessions.sealExternalExecutionPlan(a, txn_id, seal, &tables)).?;
                defer plan.deinit();
                receipt = .{ .owner = try identity.owner(a), .txn_id = txn_id, .gid = identity.gid, .session = identity.session.?, .status = if (aborted) .aborted else .prepared };
            } else if (phase == 1) {
                // Each malformed authority must leave the immutable plan and
                // external recovery purpose intact before any persistence.
                for (0..8) |fault| {
                    var wrong = receipt;
                    switch (fault) {
                        0 => wrong.status = .committed,
                        1 => wrong.status = .pending,
                        2 => wrong.decision = .{ .status = .committed, .version = 1 },
                        3 => wrong.owner.operation_id = @splat(9),
                        4 => wrong.txn_id = @splat(9),
                        5 => wrong.gid = "other-gid",
                        6 => wrong.session.owner_node_id = 8,
                        7 => wrong.kind = .session_commit,
                        else => unreachable,
                    }
                    try std.testing.expectError(error.ExternalTransactionDecisionChanged, sessions.recordExternalPreparationReceipt(a, txn_id, wrong));
                }
                try std.testing.expectError(error.TransactionCommitSealed, sessions.recordTerminalAbort(a, txn_id));
                try std.testing.expectError(error.TransactionCommitSealed, sessions.recordTerminalCommit(a, txn_id, .committed, null, null));
                var plan = (try sessions.getExecutionPlan(a, try sessions.mutation(a, txn_id, 7))).?;
                defer plan.deinit();
                try std.testing.expectEqualStrings("docs", plan.value[0].table_name);
                durable.fail_writes_for_test = true;
                try std.testing.expectError(error.InjectedSessionStoreFailure, sessions.recordExternalPreparationReceipt(a, txn_id, receipt));
                durable.fail_writes_for_test = false;
                var before = (try durable.load(txn_id)).?;
                defer before.deinit(a);
                try std.testing.expect(before.execution_plan != null and !before.external_preparation_prepared and !before.terminal_abort);
                _ = (try sessions.recordExternalPreparationReceipt(a, txn_id, receipt)).?;
            } else {
                var persisted = (try durable.load(txn_id)).?;
                defer persisted.deinit(a);
                try std.testing.expectEqual(aborted, persisted.terminal_abort);
                try std.testing.expectEqual(!aborted, persisted.external_preparation_prepared);
                try std.testing.expect(persisted.execution_plan == null and persisted.staged == null);
                try std.testing.expect(persisted.external_preparation != null and persisted.terminal_commit == null);
                // Lost delivery replies retry without a second durable write,
                // including when the local store is temporarily unavailable.
                durable.fail_writes_for_test = true;
                _ = (try sessions.recordExternalPreparationReceipt(a, txn_id, receipt)).?;
                if (phase == 2) {
                    try std.testing.expectError(error.InjectedSessionStoreFailure, sessions.markExternalPreparationReceiptAcknowledged(a, txn_id));
                    durable.fail_writes_for_test = false;
                    var debt = (try sessions.claimPendingRecovery(a, txn_id, 7, nextTxnTimestamp())).?;
                    defer debt.deinit(a);
                    try std.testing.expect(debt == .external_preparation);
                    try std.testing.expectEqual(aborted, debt.external_preparation.aborted);
                    try std.testing.expectEqual(!aborted, debt.external_preparation.prepared);
                    _ = (try sessions.markExternalPreparationReceiptAcknowledged(a, txn_id)).?;
                } else {
                    durable.fail_writes_for_test = false;
                    try std.testing.expect(persisted.external_receipt_acknowledged);
                    try std.testing.expect((try sessions.claimPendingRecovery(a, txn_id, 7, nextTxnTimestamp())) == null);
                }
            }
        }
    }
}

test "distributed txn SQL sync choice seals atomically and survives cold retries with changed caller settings" {
    const a = std.testing.allocator;
    const Fixture = struct {
        fn load(_: *anyopaque, _: std.mem.Allocator, scope: setting_catalog.Scope) !setting_catalog.RawSnapshot {
            return .{ .scope = scope, .epoch = 1, .definitions = &.{} };
        }
    };
    var marker: u8 = 0;
    const owner: setting_catalog.Owner = .{ .ptr = &marker, .load = Fixture.load };
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/sql-sync-seal", .{tmp.sub_path}, 0);
    defer a.free(path);
    var txn_id: db_mod.types.TxnId = undefined;
    for (0..3) |phase| {
        var store = try docstore_mod.DocStore.open(a, path, .{});
        defer store.close();
        var durable = DurableSessionStore.init(a, &store);
        var sessions = SessionRegistry.init(&durable);
        defer sessions.deinit(a);
        if (phase == 0) {
            const info = try sessions.beginForPrincipal(a, .{ .sql = .{ .database = "main", .namespace = "public", .isolation = .read_committed, .mode = .read_write } }, 7, "alice");
            txn_id = info.txn_id;
            try sessions.setSqlSettingRaw(a, try sessions.mutation(a, txn_id, 7), "alice", owner, setting_catalog.native_sync_name, "full_text", false);
            var stale = (try sessions.getSqlState(a, txn_id)).?;
            defer stale.deinit(a);
            // A SET may finish after HTTP attachment but before COMMIT takes
            // its execution lease. Sealing must use the current durable cut.
            try sessions.setSqlSettingRaw(a, try sessions.mutation(a, txn_id, 7), "alice", owner, setting_catalog.native_sync_name, "full_index", true);
            try std.testing.expectEqualStrings("full_text", stale.setting_active.items[0].value.string);
            var request = try parseCommitRequest(a, "{\"read_set\":[],\"tables\":{\"docs\":{\"inserts\":{\"k\":{\"value\":1}}}}}");
            defer request.deinit(a);
            _ = (try sessions.stage(a, try sessions.mutation(a, txn_id, 7), &request)).?;
            durable.fail_writes_for_test = true;
            try std.testing.expectError(error.InjectedSessionStoreFailure, sessions.cloneCommitRequestWithSqlSyncLevel(a, txn_id, null, null));
            durable.fail_writes_for_test = false;
            var before = (try durable.load(txn_id)).?;
            defer before.deinit(a);
            try std.testing.expect(before.commit_body_digest == null);
            try std.testing.expectEqual(info.sync_level, before.sync_level);
            try std.testing.expectEqual(info.sync_level, sessions.getInfo(txn_id).?.sync_level);
            var sealed = (try sessions.cloneCommitRequestWithSqlSyncLevel(a, txn_id, null, null)).?;
            defer sealed.deinit(a);
            try std.testing.expectEqual(.full_index, sealed.sync_level);
        } else {
            var sealed = (try sessions.cloneCommitRequestWithSqlSyncLevel(a, txn_id, null, .write)).?;
            defer sealed.deinit(a);
            try std.testing.expectEqual(.full_index, sealed.sync_level);
            try std.testing.expectEqual(.full_index, sessions.getInfo(txn_id).?.sync_level);
            var persisted = (try durable.load(txn_id)).?;
            defer persisted.deinit(a);
            try std.testing.expectEqual(.full_index, persisted.sync_level);
            try std.testing.expect(persisted.commit_body_digest != null);
            try std.testing.expectError(error.TransactionCommitSealed, sessions.setSqlSettingRaw(a, try sessions.mutation(a, txn_id, 7), "alice", owner, setting_catalog.native_sync_name, "write", true));
        }
    }
}

fn beginPrivateCommitFixture(registry: *SessionRegistry, alloc: std.mem.Allocator, sync_level: db_mod.types.SyncLevel) !SessionInfo {
    const connections = @import("sql/session/connections.zig");
    const durable = registry.durable.?;
    const connection: [32]u8 = @splat('a');
    const node: u64 = 7;
    const now = nextTxnTimestamp() / std.time.ns_per_ms;
    const id = newSessionTxnId(node);
    try connections.create(durable, registry, .{
        .id = connection,
        .principal = "",
        .owner_node_id = node,
        .expires_at_ms = now + sql_connection_record.ttl_ms,
        .database = "default",
        .namespace = "public",
    }, now);
    _ = try connections.mutate(durable, .{ .begin = .{ .id = connection, .expected_revision = 1, .txn_id = id } }, "", node, now);
    const session = try registry.beginForPrincipalWithSettingsAndId(alloc, .{
        .sync_level = sync_level,
        .sql = .{ .database = "default", .namespace = "public", .isolation = .read_committed, .mode = .read_write },
    }, node, null, &.{}, connection, id);
    _ = try connections.mutate(durable, .{ .bind = .{ .id = connection, .txn_id = id } }, "", node, now);
    return session;
}

test "distributed txn private COMMIT delivery retains exact cold decisions through failed adoption and ACK replay" {
    const a = std.testing.allocator;
    const P = @import("../system_catalog/prepared_transactions.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const tables = [_]distributed_txn.TableCommitRequest{.{ .table_name = "docs", .writes = &.{} }};
    const encoded = try encodeExecutionPlan(a, &tables);
    defer a.free(encoded);
    for ([_]bool{ false, true }, 0..) |aborted, number| {
        const path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/private-delivery-{d}", .{ tmp.sub_path, number }, 0);
        defer a.free(path);
        var txn_id: db_mod.types.TxnId = undefined;
        var receipt: P.Receipt = undefined;
        var expected_seal: ?[]u8 = null;
        defer if (expected_seal) |bytes| a.free(bytes);
        for (0..4) |phase| {
            var store = try docstore_mod.DocStore.open(a, path, .{});
            defer store.close();
            var durable = DurableSessionStore.init(a, &store);
            var sessions = SessionRegistry.init(&durable);
            defer sessions.deinit(a);
            if (phase == 0) {
                const session = try beginPrivateCommitFixture(&sessions, a, if (aborted) .propose else .write);
                txn_id = session.txn_id;
                _ = (try sessions.createNamedSavepoint(a, try sessions.mutation(a, txn_id, ((try sessions.readInfo(txn_id)).?).version.owner), "before_commit")).?;
                const identity: P.Identity = .{ .kind = .session_commit, .session = .{ .owner_node_id = 7, .connection_id = @splat('a') }, .gid = "", .database_id = 1, .principal = .bootstrap, .txn_id = txn_id, .operation_id = @splat(3), .metadata_incarnation = @splat('1'), .begin_timestamp = session.begin_timestamp, .expires_at_unix_ns = session.begin_timestamp + 60 * std.time.ns_per_s, .plan_digest = try P.planDigest(encoded), .manifest_digest = try P.manifestDigest(a, &.{}, @splat('1')) };
                const seal: ExternalPreparationSeal = .{ .admission_head = 0, .admitted_at_ns = session.begin_timestamp, .standalone = false, .identity = identity, .actor = .{ .session_user = .bootstrap, .credential = "" }, .participants = &.{} };
                var plan = (try sessions.sealExternalExecutionPlan(a, txn_id, seal, &tables)).?;
                defer plan.deinit();
                expected_seal = (try sessions.externalPreparationSeal(a, txn_id)).?;
                var pending = (try durable.loadInto(a, txn_id)).?;
                defer pending.deinit(a);
                try std.testing.expectEqual(@import("../sql/session_completion.zig").SealedOutcome.unknown, @import("../sql/session_completion.zig").sealedTerminalOutcome(pending, expected_seal.?));
                receipt = .{ .kind = .session_commit, .owner = try identity.owner(a), .txn_id = txn_id, .gid = "", .session = identity.session.?, .status = if (aborted) .aborted else .committed, .decision = .{ .status = if (aborted) .aborted else .committed, .version = std.math.maxInt(u64), .sync_level = .write } };
            } else if (phase == 1) {
                for (0..13) |fault| {
                    var wrong = receipt;
                    switch (fault) {
                        0 => wrong.kind = .named,
                        1 => {
                            wrong.status = .pending;
                            wrong.decision = null;
                        },
                        2 => wrong.decision = null,
                        3 => wrong.decision.?.version = 1,
                        4 => wrong.owner.operation_id = @splat(9),
                        5 => wrong.owner.admission_digest = @splat(9),
                        6 => wrong.txn_id = @splat(9),
                        7 => wrong.gid = "other-purpose",
                        8 => wrong.session.owner_node_id = 8,
                        9 => wrong.session.connection_id = @splat('b'),
                        10 => wrong.decision.?.sync_level = .propose,
                        11 => wrong.decision.?.status = if (aborted) .committed else .aborted,
                        12 => {
                            var incarnation: [16]u8 = undefined;
                            _ = try std.fmt.hexToBytes(&incarnation, &receipt.owner.metadata_incarnation);
                            wrong.lock_scope = .{ .backend = .{ .incarnation = incarnation, .id = @splat(8), .fence = 1 }, .command = .{ .sequence = 1, .nonce = @splat(7), .executor = @splat(6) } };
                            try wrong.validate(); // Structurally valid, wrong sealed identity.
                        },
                        else => unreachable,
                    }
                    // Aborted outcomes do not carry a visibility requirement;
                    // propose is invalid only for a committed decision.
                    if (fault == 10 and aborted) continue;
                    try std.testing.expectError(error.ExternalTransactionDecisionChanged, sessions.recordExternalCommitReceipt(a, txn_id, wrong));
                }
                try std.testing.expectError(error.ExternalTransactionDecisionChanged, sessions.recordExternalPreparationReceipt(a, txn_id, receipt));
                try std.testing.expectError(error.TransactionCommitSealed, sessions.recordTerminalAbort(a, txn_id));
                try std.testing.expectError(error.TransactionCommitSealed, sessions.recordTerminalCommit(a, txn_id, .committed, null, null));
                durable.fail_writes_for_test = true;
                try std.testing.expectError(error.InjectedSessionStoreFailure, sessions.recordExternalCommitReceipt(a, txn_id, receipt));
                durable.fail_writes_for_test = false;
                var before = (try durable.load(txn_id)).?;
                defer before.deinit(a);
                try std.testing.expect(before.execution_plan != null and before.external_decision == null and before.savepoints.count() == 1);
                _ = (try sessions.recordExternalCommitReceipt(a, txn_id, receipt)).?;
            } else {
                var persisted = (try durable.load(txn_id)).?;
                defer persisted.deinit(a);
                // This is the actual public completion observation: terminal
                // journal and exact seal survive cold reopen and ACK replay.
                // The admission-only getter must remain sealed, while the
                // recovery observer accepts the same real terminal Session.
                try std.testing.expectError(error.TransactionCommitSealed, sessions.externalPreparationSeal(a, txn_id));
                var terminal_cut = (try durable.loadInto(a, txn_id)).?;
                defer terminal_cut.deinit(a);
                const completion = @import("../sql/session_completion.zig");
                try std.testing.expectEqual(if (aborted) completion.SealedOutcome.aborted else completion.SealedOutcome.committed, completion.sealedTerminalOutcome(terminal_cut, expected_seal.?));
                try std.testing.expectEqual(completion.SealedOutcome.unknown, completion.sealedTerminalOutcome(terminal_cut, "different-purpose"));
                var absent_seal = terminal_cut;
                absent_seal.external_preparation = null;
                try std.testing.expectEqual(completion.SealedOutcome.unknown, completion.sealedTerminalOutcome(absent_seal, expected_seal.?));
                try std.testing.expectEqualDeep(receipt.decision.?, persisted.external_decision.?);
                try std.testing.expectEqual(aborted, persisted.terminal_abort);
                try std.testing.expectEqual(!aborted, persisted.terminal_commit != null);
                if (persisted.terminal_commit) |terminal| try std.testing.expectEqual(std.math.maxInt(u64), terminal.commit_version.?);
                try std.testing.expect(!persisted.external_preparation_prepared and persisted.execution_plan == null and persisted.staged == null and persisted.native_sql_cut == null and persisted.savepoints.count() == 0);
                durable.fail_writes_for_test = true;
                _ = (try sessions.recordExternalCommitReceipt(a, txn_id, receipt)).?;
                var changed = receipt;
                changed.decision.?.version -= 1;
                try std.testing.expectError(error.ExternalTransactionDecisionChanged, sessions.recordExternalCommitReceipt(a, txn_id, changed));
                if (phase == 2) {
                    try std.testing.expectError(error.InjectedSessionStoreFailure, sessions.markExternalPreparationReceiptAcknowledged(a, txn_id));
                    durable.fail_writes_for_test = false;
                    var debt = (try sessions.claimPendingRecovery(a, txn_id, 7, nextTxnTimestamp())).?;
                    defer debt.deinit(a);
                    try std.testing.expect(debt == .external_preparation);
                    try std.testing.expectEqual(!aborted, debt.external_preparation.committed);
                    try std.testing.expectEqual(aborted, debt.external_preparation.aborted);
                    try std.testing.expect(!debt.external_preparation.prepared);
                    _ = (try sessions.markExternalPreparationReceiptAcknowledged(a, txn_id)).?;
                } else {
                    try std.testing.expect(persisted.external_receipt_acknowledged);
                    _ = (try sessions.markExternalPreparationReceiptAcknowledged(a, txn_id)).?;
                    durable.fail_writes_for_test = false;
                    try std.testing.expect((try sessions.claimPendingRecovery(a, txn_id, 7, nextTxnTimestamp())) == null);
                }
            }
        }
    }
}

test "transaction graph effects preserve clone merge and durable encoding" {
    const a = std.testing.allocator;
    var original = try parseCommitRequest(a,
        \\{"read_set":[],"tables":{"docs":{"graph_writes":[{"index_name":"g","source":"a","target":"b","edge_type":"R","edge_id":"edge-1","metadata_json":"{\"value\":9223372036854775807}"}]}}}
    );
    defer original.deinit(a);
    var copy = try original.clone(a);
    defer copy.deinit(a);
    const tables = try copy.distributedTables(a);
    defer tables.deinit();
    try std.testing.expectEqual(@as(usize, 1), tables.tables[0].graph_writes.len);
    const encoded = try encodeCommitRequest(a, copy);
    defer a.free(encoded);
    var restored = try parseCommitRequest(a, encoded);
    defer restored.deinit(a);
    try std.testing.expectEqualStrings("{\"value\":9223372036854775807}", restored.tables[0].batch.graph_writes[0].metadata_json);
    var deleting = try parseCommitRequest(a,
        \\{"read_set":[],"tables":{"docs":{"graph_deletes":[{"index_name":"g","source":"a","target":"b","edge_type":"R","edge_id":"edge-1"}]}}}
    );
    defer deleting.deinit(a);
    try restored.mergeFrom(a, &deleting);
    try std.testing.expectEqual(@as(usize, 0), restored.tables[0].batch.graph_writes.len);
    try std.testing.expectEqual(@as(usize, 1), restored.tables[0].batch.graph_deletes.len);
}

test "transaction graph effects client IDs never overwrite an admitted principal" {
    const a = std.testing.allocator;
    var registry = SessionRegistry.init(null);
    defer registry.deinit(a);
    const id: db_mod.types.TxnId = @splat(7);
    _ = try registry.beginForPrincipalWithSettingsAndId(a, .{}, 17, "user:a", &.{}, null, id);
    try std.testing.expectError(error.CypherWriteRequestAlreadyUsed, registry.beginForPrincipalWithSettingsAndId(a, .{}, 42, "user:b", &.{}, null, id));
    try std.testing.expectEqual(@as(u64, 17), registry.sessions.get(id).?.owner_node_id);
    try std.testing.expectEqual(SessionRegistry.PrincipalAccess.allowed, try registry.principalAccess(a, id, "user:a"));
    try std.testing.expectEqual(SessionRegistry.PrincipalAccess.denied, try registry.principalAccess(a, id, "user:b"));
}

fn sessionCanExpire(session: Session) bool {
    if (session.commit_execution_started and !sessionCompletionSettled(session)) return false;
    if (session.terminal_commit) |terminal| return terminal.coordinator_group_id == null or terminal.coordinator_acknowledged;
    return true;
}

fn sessionCompletionSettled(session: Session) bool {
    if (session.external_preparation != null and !session.external_receipt_acknowledged) return false;
    if (session.terminal_abort) return true;
    if (session.terminal_commit) |terminal| return terminal.status == .committed and !terminal.repair_required;
    // A named PREPARE settles the source session after the receiving native
    // registry takes custody and its receipt is durably acknowledged.
    return session.external_preparation_prepared and session.external_receipt_acknowledged;
}
fn makeSessionCompletionKey(alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId) ![]u8 {
    const encoded = std.fmt.bytesToHex(txn_id, .lower);
    return std.fmt.allocPrint(alloc, "{s}{s}", .{ sql_abort_proof_prefix, &encoded });
}

fn completionDirectoryKey(alloc: std.mem.Allocator, id: db_mod.types.TxnId) ![]u8 {
    return std.fmt.allocPrint(alloc, "\x00\x00__api_completion_consumers__:{s}", .{&std.fmt.bytesToHex(id, .lower)});
}
fn completionReferenceKey(alloc: std.mem.Allocator, id: db_mod.types.TxnId, reference: [16]u8) ![]u8 {
    return std.fmt.allocPrint(alloc, "\x00\x00__api_completion_reference__:{s}:{s}", .{ &std.fmt.bytesToHex(id, .lower), &std.fmt.bytesToHex(reference, .lower) });
}

test "durable session mutations publish only after persistence succeeds: concurrent completion consumers and retirement checkpoints" {
    const a = std.testing.allocator;
    var backend = mem_backend.Backend.init(a, .{});
    defer backend.close();
    var store = try backend.runtimeStore(a, .{ .name = "system/concurrent-completion-consumers" });
    defer store.deinit();
    var durable = DurableSessionStore.initRuntime(a, &store);
    var registry = SessionRegistry.init(&durable);
    defer registry.deinit(a);
    const session = try registry.begin(a, .{ .sync_level = .write }, 0);
    var request = try parseCommitRequest(a, "{\"read_set\":[],\"tables\":{\"docs\":{\"inserts\":{\"a\":{\"value\":1}}}}}");
    defer request.deinit(a);
    var sealed = (try registry.cloneCommitRequest(a, session.txn_id, &request)).?;
    sealed.deinit(a);
    _ = (try registry.markCommitExecutionStarted(a, session.txn_id)).?;
    const Job = struct {
        fn run(storage: *DurableSessionStore, id: [16]u8, start: u8) !void {
            for (0..16) |offset| try storage.pinCompletion(id, @splat(start + @as(u8, @intCast(offset))));
        }
        fn finish(sessions: *SessionRegistry, id: [16]u8) !void {
            _ = try sessions.recordTerminalAbort(std.testing.allocator, id);
        }
    };
    var one = try std.testing.io.concurrent(Job.run, .{ &durable, session.txn_id, @as(u8, 1) });
    var two = try std.testing.io.concurrent(Job.run, .{ &durable, session.txn_id, @as(u8, 33) });
    var terminal = try std.testing.io.concurrent(Job.finish, .{ &registry, session.txn_id });
    try one.await(std.testing.io);
    try two.await(std.testing.io);
    try terminal.await(std.testing.io);
    _ = try registry.cleanupExpired(a, std.math.maxInt(u64));
    for ([_]u8{ 1, 33 }) |start| for (0..16) |offset| {
        const reference: [16]u8 = @splat(start + @as(u8, @intCast(offset)));
        try std.testing.expect(try durable.completionReferenceSettled(session.txn_id, reference));
        try std.testing.expect(try durable.acknowledgeCompletion(session.txn_id, reference));
    };
}

test "durable session mutations publish only after persistence succeeds: durable registration outbox and bounded retirement" {
    const a = std.testing.allocator;
    var backend = mem_backend.Backend.init(a, .{});
    defer backend.close();
    var store = try backend.runtimeStore(a, .{ .name = "system/completion-registration-outbox" });
    defer store.deinit();
    var durable = DurableSessionStore.initRuntime(a, &store);
    var registry = SessionRegistry.init(&durable);
    defer registry.deinit(a);
    const session = try registry.begin(a, .{ .sync_level = .write }, 0);
    const id = std.fmt.bytesToHex(session.txn_id, .lower);
    const first = try durable.createRegistration(@splat(3), .{ .transaction_id = &id });
    const second = try durable.createRegistration(@splat(9), .{ .transaction_id = &id });
    try std.testing.expectEqualSlices(u8, first[0..8], second[0..8]);
    _ = try durable.managedCompletion(session.txn_id, first, false, 0);
    _ = try durable.managedCompletion(session.txn_id, second, false, 0);
    try std.testing.expect(try durable.managedCompletion(session.txn_id, second, true, 0));
    try durable.removeRegistration(second);
    try std.testing.expectEqual(@as(u64, 0), try durable.registrationFloor());
    try std.testing.expectError(error.CompletionAcknowledged, durable.managedCompletion(session.txn_id, second, false, 0));
    try std.testing.expect(try durable.managedCompletion(session.txn_id, first, true, 0));
    try durable.removeRegistration(first);
    try std.testing.expectEqual(@as(u64, 2), try durable.registrationFloor());
    // Re-open the producer facade: sequence and pending state are durable.
    var restarted = DurableSessionStore.initRuntime(a, &store);
    const third = try restarted.createRegistration(@splat(7), .{ .transaction_id = &id });
    try std.testing.expectEqual(@as(u64, 3), @import("completion_retirement.zig").sequence(third));
    _ = try restarted.managedCompletion(session.txn_id, third, false, try restarted.registrationFloor());
    // Retired markers have been collected, but stale registrations stay rejected.
    try std.testing.expectError(error.CompletionAcknowledged, restarted.managedCompletion(session.txn_id, first, false, 0));
    try std.testing.expectError(error.CompletionAcknowledged, restarted.managedCompletion(session.txn_id, second, false, 0));
    var pending = (try restarted.loadRegistration(a, third)).?;
    defer pending.deinit();
    try std.testing.expectEqualStrings(&id, pending.value.outcome.transaction_id.?);
    try std.testing.expect(pending.value.phase == .registering);
}

test "SQL pgwire native commit rejection requires a fresh execution" {
    for ([_]anyerror{ error.TableNotFound, error.TopologyChanged, error.TransactionTooLarge, error.UnknownGroup, error.PreDispatchDecisionAdmissionUnavailable }) |err| {
        try std.testing.expectEqual(CommitRejection.fresh, classifyCommitRejection(true, err));
        try std.testing.expectEqual(CommitRejection.ambiguous, classifyCommitRejection(false, err));
    }
    inline for (.{ true, false }) |fresh| try std.testing.expectEqual(CommitRejection.ambiguous, classifyCommitRejection(fresh, error.CatalogGenerationChanged));
    inline for (.{ true, false }) |first| {
        inline for (.{ error.TransactionPrepareAbortedRowPolicyDenied, error.TransactionPrepareAbortedUnavailable }) |err|
            try std.testing.expectEqual(CommitRejection.confirmed, classifyCommitRejection(first, err));
        try std.testing.expectEqual(CommitRejection.none, classifyCommitRejection(first, error.AbortDecisionNotDurable));
    }
    try std.testing.expectEqual(CommitRejection.none, classifyCommitRejection(true, error.CommitDecisionUnknown));
    const session: Session = .{ .txn_id = @splat(1), .owner_node_id = 0, .begin_timestamp = 1, .last_touched_timestamp = 1, .sync_level = .write, .commit_execution_started = true };
    try std.testing.expect(session.info().commit_execution_started);
}

test "durable session mutations publish only after persistence succeeds: shared runtime facades retain both consumers after terminal cleanup" {
    const regression_allocator = std.testing.allocator;
    const FixtureRun = struct {
        fn run(a: std.mem.Allocator, memory: bool, configured_gate: bool, document_facades: bool) !void {
            var tmp = std.testing.tmpDir(.{});
            defer tmp.cleanup();
            const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/shared-runtime-review", .{tmp.sub_path});
            defer a.free(path);
            var mem = mem_backend.Backend.init(a, .{});
            defer mem.close();
            var lsm = if (!memory) try @import("antfly_local_sources").storage_lsm_backend.Backend.open(a, path, .{}) else null;
            defer if (lsm) |*backend| backend.close();
            var runtime = if (lsm) |*backend| try backend.runtimeStore(a, .{ .name = "system/shared-completion" }) else try mem.runtimeStore(a, .{ .name = "system/shared-completion" });
            var second_runtime = if (lsm) |*backend| try backend.runtimeStore(a, .{ .name = "system/shared-completion" }) else try mem.runtimeStore(a, .{ .name = "system/shared-completion" });
            defer second_runtime.deinit();
            if (configured_gate) second_runtime.write_gate = second_runtime.writeSerialization().gate;
            // An erased wrapper must preserve whether its underlying callbacks already
            // hold the gate; acquiring that gate twice would deadlock this case.
            var wrapped_runtime = try backend_erased.storeFrom(a, second_runtime);
            defer wrapped_runtime.deinit();
            defer runtime.deinit();
            var original_docs = if (document_facades) try docstore_mod.DocStore.openRuntime(a, &runtime) else null;
            defer if (original_docs) |*docs| docs.close();
            var original = if (original_docs) |*docs| DurableSessionStore.init(a, docs) else DurableSessionStore.initRuntime(a, &runtime);
            var registry = SessionRegistry.init(&original);
            defer registry.deinit(a);
            const session = try registry.begin(a, .{}, 1);
            var request = try parseCommitRequest(a, "{\"read_set\":[],\"tables\":{\"docs\":{\"inserts\":{\"a\":{\"n\":1}}}}}");
            defer request.deinit(a);
            var staged = (try registry.cloneCommitRequest(a, session.txn_id, &request)).?;
            staged.deinit(a);
            _ = (try registry.markCommitExecutionStarted(a, session.txn_id)).?;
            const Fixture = struct {
                store: *backend_erased.Store,
                first_read: std.Io.Event = .unset,
                release_first: std.Io.Event = .unset,
                second_started: std.Io.Event = .unset,
                second_done: std.Io.Event = .unset,
                const Outer = @This();
                const Wrapped = struct {
                    original: backend_erased.WriteTxn,
                    fixture: *Outer,
                    fn get(ptr: *anyopaque, key: []const u8) ![]const u8 {
                        const self: *@This() = @ptrCast(@alignCast(ptr));
                        const result = self.original.get(key);
                        if (std.mem.indexOf(u8, key, "\x00\x00__api_completion_consumers__:") != null) {
                            self.fixture.first_read.set(std.testing.io);
                            self.fixture.release_first.waitUncancelable(std.testing.io);
                        }
                        return result;
                    }
                    fn openCursor(_: std.mem.Allocator, ptr: *anyopaque) !backend_erased.Cursor {
                        return @as(*@This(), @ptrCast(@alignCast(ptr))).original.openCursor();
                    }
                    fn put(ptr: *anyopaque, key: []const u8, value: []const u8) !void {
                        return @as(*@This(), @ptrCast(@alignCast(ptr))).original.put(key, value);
                    }
                    fn delete(ptr: *anyopaque, key: []const u8) !void {
                        return @as(*@This(), @ptrCast(@alignCast(ptr))).original.delete(key);
                    }
                    fn commit(alloc: std.mem.Allocator, ptr: *anyopaque) !void {
                        const self: *@This() = @ptrCast(@alignCast(ptr));
                        try self.original.commit();
                        alloc.destroy(self);
                    }
                    fn abort(alloc: std.mem.Allocator, ptr: *anyopaque) void {
                        const self: *@This() = @ptrCast(@alignCast(ptr));
                        self.original.abort();
                        alloc.destroy(self);
                    }
                };
                fn capabilities(ptr: *anyopaque) @import("antfly_local_sources").storage_backend_types.Capabilities {
                    return @as(*@This(), @ptrCast(@alignCast(ptr))).store.capabilities();
                }
                fn beginRead(_: std.mem.Allocator, ptr: *anyopaque) !backend_erased.ReadTxn {
                    return @as(*@This(), @ptrCast(@alignCast(ptr))).store.beginRead();
                }
                fn beginBatch(_: std.mem.Allocator, ptr: *anyopaque) !backend_erased.Batch {
                    return @as(*@This(), @ptrCast(@alignCast(ptr))).store.beginBatch();
                }
                fn deinit(_: std.mem.Allocator, _: *anyopaque) void {}
                fn beginWrite(alloc: std.mem.Allocator, ptr: *anyopaque) !backend_erased.WriteTxn {
                    const self: *@This() = @ptrCast(@alignCast(ptr));
                    const wrapped = try alloc.create(Wrapped);
                    errdefer alloc.destroy(wrapped);
                    wrapped.* = .{ .original = try self.store.beginWrite(), .fixture = self };
                    return .{ .allocator = alloc, .ptr = wrapped, .vtable = &.{ .get = Wrapped.get, .put = Wrapped.put, .delete = Wrapped.delete, .commit = Wrapped.commit, .abort = Wrapped.abort, .open_cursor = Wrapped.openCursor } };
                }
                fn pin(store: *DurableSessionStore, id: [16]u8, reference: [16]u8, fctx: ?*@This()) !void {
                    if (fctx) |f| f.second_started.set(std.testing.io);
                    defer if (fctx) |f| f.second_done.set(std.testing.io);
                    try store.pinCompletion(id, reference);
                }
            };
            var fixture: Fixture = .{ .store = &runtime };
            // Every erased callback must receive the context it was built for.
            // Delegate serialization to the wrapped store, including its gate.
            var intercepted: backend_erased.Store = .{
                .allocator = a,
                .ptr = &fixture,
                .write_serialization = runtime.writeSerialization(),
                .vtable = &.{ .deinit = Fixture.deinit, .capabilities = Fixture.capabilities, .begin_read = Fixture.beginRead, .begin_write = Fixture.beginWrite, .begin_batch = Fixture.beginBatch },
            };
            var first_docs = if (document_facades) try docstore_mod.DocStore.openRuntime(a, &intercepted) else null;
            defer if (first_docs) |*docs| docs.close();
            var second_docs = if (document_facades) try docstore_mod.DocStore.openRuntime(a, &wrapped_runtime) else null;
            defer if (second_docs) |*docs| docs.close();
            // Cross the document adapter and erasure boundary for the second
            // facade, so coordination must survive every wrapper in the stack.
            var erased_docs = if (second_docs) |*docs| try backend_erased.storeFrom(a, docs.backendStore()) else null;
            defer if (erased_docs) |*store| store.deinit();
            var first = if (first_docs) |*docs| DurableSessionStore.init(a, docs) else DurableSessionStore.initRuntime(a, &intercepted);
            var second = if (erased_docs) |*store| DurableSessionStore.initRuntime(a, store) else DurableSessionStore.initRuntime(a, &wrapped_runtime);
            try first.bindRegistrationIssuer(1);
            try second.bindRegistrationIssuer(2);
            const ref_a: [16]u8 = @splat(0x41);
            const ref_b: [16]u8 = @splat(0x42);
            var one = try std.testing.io.concurrent(Fixture.pin, .{ &first, session.txn_id, ref_a, @as(?*Fixture, null) });
            defer {
                fixture.release_first.set(std.testing.io);
                one.cancel(std.testing.io) catch {};
            }
            fixture.first_read.waitUncancelable(std.testing.io);
            var two = try std.testing.io.concurrent(Fixture.pin, .{ &second, session.txn_id, ref_b, @as(?*Fixture, &fixture) });
            defer two.cancel(std.testing.io) catch {};
            fixture.second_started.waitUncancelable(std.testing.io);
            std.testing.io.sleep(.fromMilliseconds(50), .awake) catch {};
            const escaped = fixture.second_done.isSet();
            fixture.release_first.set(std.testing.io);
            try one.await(std.testing.io);
            try two.await(std.testing.io);
            try std.testing.expect(!escaped);
            const listed = try registry.listPendingRecoveryIds(a, 16);
            defer a.free(listed);
            try std.testing.expect(for (listed) |id| {
                if (std.meta.eql(id, session.txn_id)) break true;
            } else false);
            _ = (try registry.recordTerminalAbort(a, session.txn_id)).?;
            try std.testing.expect((try registry.claimPendingRecovery(a, session.txn_id, session.version.owner, nextTxnTimestamp())) == null);
            _ = try registry.cleanupExpired(a, std.math.maxInt(u64));
            const settled_a = try original.completionReferenceSettled(session.txn_id, ref_a);
            const settled_b = try original.completionReferenceSettled(session.txn_id, ref_b);
            try std.testing.expect((try original.load(session.txn_id)) == null);
            try std.testing.expect(settled_a and settled_b);
            try std.testing.expect(try original.acknowledgeCompletion(session.txn_id, ref_a));
            try std.testing.expect(try original.acknowledgeCompletion(session.txn_id, ref_b));
        }
    };
    for ([_]bool{ false, true }) |document_facades| {
        try FixtureRun.run(regression_allocator, true, false, document_facades);
        try FixtureRun.run(regression_allocator, false, false, document_facades);
        try FixtureRun.run(regression_allocator, false, true, document_facades);
    }
}

test "durable session mutations publish only after persistence succeeds: shared completion rejects uncoordinated runtime providers" {
    const a = std.testing.allocator;
    var backend = mem_backend.Backend.init(a, .{});
    defer backend.close();
    var runtime = try backend.runtimeStore(a, .{ .name = "system/uncoordinated" });
    defer runtime.deinit();
    runtime.write_serialization = .unsupported;
    var durable = DurableSessionStore.initRuntime(a, &runtime);
    try std.testing.expectError(error.CompletionOwnerUnavailable, durable.bindRegistrationIssuer(1));
    // Custom intrinsically serializable providers can explicitly declare that
    // contract, and callers can supply Store.write_gate for other providers.
    runtime.write_serialization = .intrinsic;
    try durable.bindRegistrationIssuer(1);
}

test "distributed txn private COMMIT visibility adoption is monotone across cold reopen and retains repair proof" {
    const a = std.testing.allocator;
    const P = @import("../system_catalog/prepared_transactions.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const tables = [_]distributed_txn.TableCommitRequest{.{ .table_name = "docs", .writes = &.{.{ .key = "key", .value = "{}" }} }};
    const encoded = try encodeExecutionPlan(a, &tables);
    defer a.free(encoded);
    const manifest = [_]P.Participant{.{ .table_name = "docs", .route = .{ .metadata_group_id = 1, .metadata_incarnation = @splat('1'), .catalog_revision = 2, .table_id = 7, .topology_epoch = 3, .route = .{ .group_id = 9, .range_id = 9, .identity_namespace = .{ .table_id = 7, .shard_id = 9, .range_id = 9 } } } }};
    for ([_]bool{ false, true }) |repair| {
        const path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/private-visibility-{any}", .{ tmp.sub_path, repair }, 0);
        defer a.free(path);
        var txn_id: [16]u8 = undefined;
        var receipt: P.Receipt = undefined;
        for (0..5) |phase| {
            var store = try docstore_mod.DocStore.open(a, path, .{});
            defer store.close();
            var durable = DurableSessionStore.init(a, &store);
            var sessions = SessionRegistry.init(&durable);
            defer sessions.deinit(a);
            if (phase == 0) {
                const session = try beginPrivateCommitFixture(&sessions, a, .full_index);
                txn_id = session.txn_id;
                const identity: P.Identity = .{ .kind = .session_commit, .commit_sync_level = .full_index, .session = .{ .owner_node_id = 7, .connection_id = @splat('a') }, .gid = "", .database_id = 1, .principal = .bootstrap, .txn_id = txn_id, .operation_id = @splat(3), .metadata_incarnation = @splat('1'), .begin_timestamp = session.begin_timestamp, .expires_at_unix_ns = session.begin_timestamp + 60 * std.time.ns_per_s, .plan_digest = try P.planDigest(encoded), .manifest_digest = try P.manifestDigest(a, &manifest, @splat('1')) };
                const seal: ExternalPreparationSeal = .{ .admission_head = 0, .admitted_at_ns = session.begin_timestamp, .standalone = false, .identity = identity, .actor = .{ .session_user = .bootstrap, .credential = "" }, .participants = &manifest };
                var plan = (try sessions.sealExternalExecutionPlan(a, txn_id, seal, &tables)).?;
                defer plan.deinit();
                receipt = .{ .kind = .session_commit, .owner = try identity.owner(a), .txn_id = txn_id, .gid = "", .session = identity.session.?, .status = .committed, .progress = .resolving, .decision = .{ .status = .committed, .version = std.math.maxInt(u64), .sync_level = .full_index } };
                _ = (try sessions.recordExternalCommitReceipt(a, txn_id, receipt)).?;
                var state = (try sessions.getSqlState(a, txn_id)).?;
                defer state.deinit(a);
                try std.testing.expectEqual(.committed_recovery_pending, state.terminal.?);
                try std.testing.expectError(error.InvalidTransactionSessionRecord, sessions.markExternalPreparationReceiptAcknowledged(a, txn_id));
            } else if (phase == 1) {
                receipt.progress = .indexing;
                _ = (try sessions.recordExternalCommitReceipt(a, txn_id, receipt)).?;
                var stale = receipt;
                stale.progress = .resolving;
                _ = (try sessions.recordExternalCommitReceipt(a, txn_id, stale)).?;
                var state = (try sessions.getSqlState(a, txn_id)).?;
                defer state.deinit(a);
                try std.testing.expectEqual(.committed_visibility_pending, state.terminal.?);
                try std.testing.expect(!state.terminal_repair_required);
                try std.testing.expectError(error.InvalidTransactionSessionRecord, sessions.markExternalPreparationReceiptAcknowledged(a, txn_id));
            } else if (phase == 2) {
                receipt.progress = if (repair) .repair_required else .ready;
                _ = (try sessions.recordExternalCommitReceipt(a, txn_id, receipt)).?;
                var stale = receipt;
                stale.progress = .indexing;
                _ = (try sessions.recordExternalCommitReceipt(a, txn_id, stale)).?;
                var changed = receipt;
                changed.progress = if (repair) .ready else .repair_required;
                try std.testing.expectError(error.ExternalTransactionDecisionChanged, sessions.recordExternalCommitReceipt(a, txn_id, changed));
                _ = (try sessions.markExternalPreparationReceiptAcknowledged(a, txn_id)).?;
            } else {
                var state = (try sessions.getSqlState(a, txn_id)).?;
                defer state.deinit(a);
                try std.testing.expectEqual(.committed, state.terminal.?);
                try std.testing.expectEqual(repair, state.terminal_repair_required);
                var terminal = (try durable.load(txn_id)).?;
                defer terminal.deinit(a);
                try std.testing.expect(terminal.external_receipt_acknowledged);
                try std.testing.expect(terminal.execution_plan == null);
                try std.testing.expectEqual(receipt.decision.?.version, terminal.terminal_commit.?.commit_version.?);
                _ = (try sessions.recordExternalCommitReceipt(a, txn_id, receipt)).?;
            }
        }
    }
}

test "durable SQL connection execution projection bounds reads and avoids unchanged writes" {
    const Probe = struct {
        bytes: [74]u8 = undefined,
        present: bool = false,
        reads: usize = 0,
        writes: usize = 0,
        fn get(self: *@This(), key: []const u8) ![]const u8 {
            try std.testing.expect(std.mem.startsWith(u8, key, "\x00\x00__api_sql_execution__:"));
            self.reads += 1;
            if (!self.present) return error.NotFound;
            return &self.bytes;
        }
        fn put(self: *@This(), _: []const u8, bytes: []const u8) !void {
            try std.testing.expectEqual(self.bytes.len, bytes.len);
            @memcpy(&self.bytes, bytes);
            self.present = true;
            self.writes += 1;
        }
    };
    // The unopened backend cannot supply a session payload. All work must use
    // the explicit transaction probe, bounded stack scratch and no allocator.
    var backend: docstore_mod.DocStore = undefined;
    var store = DurableSessionStore.init(std.testing.allocator, &backend);
    var probe: Probe = .{};
    const connection: [32]u8 = @splat('a');
    var session: Session = .{ .txn_id = @splat(3), .owner_node_id = 7, .principal = @constCast("alice"), .connection_id = connection, .begin_timestamp = 1, .last_touched_timestamp = 1, .sync_level = .write };
    try store.putConnectionExecutionTxn(&probe, session);
    try std.testing.expectEqual(@as(usize, 1), probe.writes);
    probe.reads = 0;
    try std.testing.expect(!try store.connectionExecutionPendingTxn(&probe, session.txn_id, connection, "alice", 7));
    try std.testing.expectEqual(@as(usize, 1), probe.reads);
    for (0..100) |_| {
        session.last_touched_timestamp += 1;
        session.sequence_epoch += 1;
        try store.putConnectionExecutionTxn(&probe, session);
    }
    try std.testing.expectEqual(@as(usize, 1), probe.writes);
    session.commit_execution_started = true;
    try store.putConnectionExecutionTxn(&probe, session);
    try std.testing.expect(try store.connectionExecutionPendingTxn(&probe, session.txn_id, connection, "alice", 7));
    try std.testing.expectEqual(@as(usize, 2), probe.writes);
    try std.testing.expectError(error.InvalidTransactionSessionRecord, store.connectionExecutionPendingTxn(&probe, session.txn_id, @splat('b'), "alice", 7));
    try std.testing.expectError(error.InvalidTransactionSessionRecord, store.connectionExecutionPendingTxn(&probe, session.txn_id, connection, "bob", 7));
    try std.testing.expectError(error.InvalidTransactionSessionRecord, store.connectionExecutionPendingTxn(&probe, session.txn_id, connection, "alice", 8));
    session.terminal_abort = true;
    try store.putConnectionExecutionTxn(&probe, session);
    try std.testing.expect(!try store.connectionExecutionPendingTxn(&probe, session.txn_id, connection, "alice", 7));
    try std.testing.expectEqual(@as(usize, 3), probe.writes);
    probe.bytes[1] = 3;
    try std.testing.expectError(error.InvalidTransactionSessionRecord, store.connectionExecutionPendingTxn(&probe, session.txn_id, connection, "alice", 7));
    probe.present = false;
    try std.testing.expectError(error.InvalidTransactionSessionRecord, store.connectionExecutionPendingTxn(&probe, session.txn_id, connection, "alice", 7));
    probe = .{};
    session.connection_id = null;
    try store.putConnectionExecutionTxn(&probe, session);
    try std.testing.expectEqual(@as(usize, 0), probe.reads);
    try std.testing.expectEqual(@as(usize, 0), probe.writes);
}

test "SQL execution lease borrowing rejects another registry and colliding transaction identities" {
    const a = std.testing.allocator;
    var first_registry = SessionRegistry.init(null);
    defer first_registry.deinit(a);
    var other_registry = SessionRegistry.init(null);
    defer other_registry.deinit(a);
    const id: db_mod.types.TxnId = @splat(1);
    const held = first_registry.tryAcquireCommitExecution(id) orelse return error.TestExpectedExecutionLease;
    defer held.release();
    try std.testing.expect(held.matches(&first_registry, id));
    try std.testing.expect(!held.matches(&other_registry, id));
    var colliding: db_mod.types.TxnId = @splat(2);
    var found = false;
    for (0..65536) |serial| {
        std.mem.writeInt(u64, colliding[0..8], serial, .little);
        if (first_registry.commitLock(colliding) == held.lock) {
            found = true;
            break;
        }
    }
    try std.testing.expect(found);
    try std.testing.expect(!held.matches(&first_registry, colliding));
    try std.testing.expect(first_registry.tryAcquireCommitExecution(colliding) == null);
}

test "pgwire notifications retain owned intents and savepoint frontiers in durable session records" {
    const a = std.testing.allocator;
    var registry = SessionRegistry.init(null);
    defer registry.deinit(a);
    const info = try registry.beginForPrincipal(a, .{ .sql = .{ .database = "app", .namespace = "public", .isolation = .read_committed, .mode = .read_write } }, 9, "alice");
    try registry.stageSqlNotification(a, info.txn_id, "alice", .{ .notify = .{ .channel = "events", .payload = "first" } });
    const point = (try registry.createNamedSavepoint(a, try registry.mutation(a, info.txn_id, info.version.owner), "child")).?;
    const bytes = try encodeSessionRecord(a, registry.sessions.get(info.txn_id).?);
    defer a.free(bytes);
    var decoded = try decodeSessionRecord(a, info.txn_id, bytes);
    defer decoded.deinit(a);
    try std.testing.expectEqual(@as(usize, 1), decoded.sql.?.notifications.commands.items.len);
    try std.testing.expectEqual(@as(usize, 1), decoded.savepoints.get(point.savepoint_id).?.notification_count);
    const recovered = try decoded.clone(a);
    const original = registry.sessions.getPtr(info.txn_id).?;
    original.deinit(a);
    original.* = recovered;
    try registry.stageSqlNotification(a, info.txn_id, "alice", .{ .notify = .{ .channel = "events", .payload = "removed" } });
    _ = try registry.rollbackToNamedSavepoint(a, try registry.mutation(a, info.txn_id, info.version.owner), "child");
    var state = (try registry.getSqlState(a, info.txn_id)).?;
    defer state.deinit(a);
    try std.testing.expectEqual(@as(usize, 1), state.metadata.notifications.commands.items.len);
    try std.testing.expectEqualStrings("first", state.metadata.notifications.commands.items[0].notify.payload);
}

test "SQL relation lifetime native session capsules keep rows and savepoints outside durable envelopes and refuse cold payload replay" {
    const a = std.testing.allocator;
    var durable = DurableSessionStore.init(a, undefined);
    defer durable.deinitVolatilePayloads();
    var request = try parseCommitRequest(a, "{\"read_set\":[],\"tables\":{\"scratch\":{\"inserts\":{\"secret-key\":{\"text\":\"private-native-row-sentinel\"}}}}}");
    defer request.deinit(a);
    const persistence: @import("antfly_local_sources").common_topology_records.RelationPersistence.Descriptor = .{ .kind = .unlogged, .owner_node_id = 7, .owner_store_id = 8, .generation_instance = @splat(9), .data_generation = 10 };
    try request.bindWithPersistence(a, "scratch", "table:17", persistence);
    try std.testing.expectError(error.CatalogGenerationChanged, request.bind(a, "scratch", "table:17"));
    var changed = persistence;
    changed.data_generation += 1;
    try std.testing.expectError(error.CatalogGenerationChanged, request.bindWithPersistence(a, "scratch", "table:17", changed));
    const tables = try request.distributedTables(a);
    defer tables.deinit();
    var session: Session = .{ .txn_id = @splat(3), .owner_node_id = 7, .begin_timestamp = 11, .last_touched_timestamp = 12, .sync_level = .write, .staged = try request.clone(a), .commit_execution_started = true, .commit_body_digest = @splat(4), .execution_plan = try encodeExecutionPlan(a, tables.tables) };
    defer session.deinit(a);
    session.next_savepoint_id = 2;
    session.current_subtransaction = 1;
    try session.savepoints.put(a, 1, .{ .id = 1, .snapshot = try request.clone(a) });
    try upsertReadSnapshot(a, &session.read_snapshots, .{ .table_name = "scratch", .key = "secret-key", .version = 1, .document_json = "{\"text\":\"private-native-row-sentinel\"}" });
    // The standalone codec is not an alternate durable row writer.
    try std.testing.expectError(error.SqlRelationRecoveryIncomplete, encodeSessionRecord(a, session));
    const envelope = try durable.encodeDurableSession(session, null);
    defer a.free(envelope);
    try std.testing.expect(std.mem.indexOf(u8, envelope, "private-native-row-sentinel") == null);
    try std.testing.expect(std.mem.indexOf(u8, envelope, "secret-key") == null);
    durable.publishVolatileSession(session);
    var hydrated = try durable.decodeDurableSession(a, session.txn_id, envelope);
    defer hydrated.deinit(a);
    try std.testing.expect(hydrated.volatile_payload_available);
    var plan = try parseExecutionPlan(a, hydrated.execution_plan.?);
    defer plan.deinit();
    try std.testing.expect(std.mem.indexOf(u8, plan.value[0].writes[0].value, "private-native-row-sentinel") != null);
    try std.testing.expect(plan.value[0].sql_persistence.?.eql(persistence));
    try std.testing.expectEqual(@as(usize, 1), hydrated.savepoints.count());
    try std.testing.expectEqual(@as(usize, 1), hydrated.read_snapshots.count());
    try std.testing.expect(std.mem.indexOf(u8, hydrated.read_snapshots.values()[0].document_json.?, "private-native-row-sentinel") != null);

    try std.testing.expect(hydrated.staged.?.catalog_bindings.items[0].sql_persistence.?.eql(persistence));
    var cold = DurableSessionStore.init(a, undefined);
    defer cold.deinitVolatilePayloads();
    var absent = try cold.decodeDurableSession(a, session.txn_id, envelope);
    defer absent.deinit(a);
    try std.testing.expect(!absent.volatile_payload_available);
    try std.testing.expect(absent.staged == null and absent.execution_plan == null and absent.savepoints.count() == 0);
    const second_envelope = try cold.encodeDurableSession(absent, null);
    defer a.free(second_envelope);
    try std.testing.expectEqualStrings(envelope, second_envelope);
    try std.testing.expectError(error.SessionRecordTooLarge, durable.encodeDurableSession(session, envelope.len));
}

test "SQL relation lifetime ordinary execution plans omit native owner extension" {
    const a = std.testing.allocator;
    const Stub = struct {
        fn sign(_: *anyopaque, _: std.mem.Allocator, _: @import("antfly_local_sources").common_topology_records.RelationPersistence.Owner, _: @import("antfly_local_sources").common_relation_owner_authority.Scope) ![]u8 {
            return error.TestUnexpectedOwnerSigning;
        }
    };
    var marker: u8 = 0;
    const minter: @import("antfly_local_sources").common_relation_owner_authority.Minter = .{ .ptr = &marker, .sign_fn = Stub.sign, .owner = .{ .issuer_node_id = 1, .connection_id = @splat('a'), .wire_epoch = @splat(2), .execution_instance = @splat(3), .connection_generation = 4, .principal_digest = @splat(5) } };
    const body = try encodeExecutionPlan(a, &.{.{ .table_name = "ordinary", .relation_owner_minter = minter }});
    defer a.free(body);
    try std.testing.expect(std.mem.indexOf(u8, body, "sql_persistence") == null);
    try std.testing.expect(std.mem.indexOf(u8, body, "relation_owner_minter") == null);
    const http = try std.json.Stringify.valueAlloc(a, distributed_txn.TableCommitRequest{ .table_name = "ordinary", .relation_owner_minter = minter }, .{});
    defer a.free(http);
    try std.testing.expect(std.mem.indexOf(u8, http, "relation_owner_minter") == null);

    var plan = try parseExecutionPlan(a, body);
    defer plan.deinit();
    try std.testing.expect(plan.value[0].sql_persistence == null);
}

test "distributed txn fresh public execution plans assign ordinary origin without changing older sealed plans" {
    const a = std.testing.allocator;
    var request = try parseCommitRequest(a, "{\"read_set\":[],\"tables\":{\"docs\":{\"inserts\":{\"row\":{}}}},\"source_producer\":\"legacy_cdc\"}");
    defer request.deinit(a);
    const view = try request.distributedTables(a);
    defer view.deinit();
    try std.testing.expectEqual(@as(usize, 1), view.tables.len);
    try std.testing.expectEqual(@FieldType(distributed_txn.TableCommitRequest, "source_producer").ordinary, view.tables[0].source_producer);
    var old = try parseExecutionPlan(a, "[{\"table_name\":\"docs\"}]");
    defer old.deinit();
    try std.testing.expectEqual(@FieldType(distributed_txn.TableCommitRequest, "source_producer").unlabelled, old.value[0].source_producer);
}

test "commit requirements survive transaction session cloning persistence and statement merging" {
    const a = std.testing.allocator;
    var request = try parseCommitRequest(a, "{\"read_set\":[],\"commit_requires\":\"direct_enrichments\",\"tables\":{\"docs\":{\"inserts\":{\"a\":{}}},\"audit\":{\"inserts\":{\"b\":{}},\"commit_requires\":\"none\"}}}");
    defer request.deinit(a);
    const encoded = try encodeCommitRequest(a, request);
    defer a.free(encoded);
    var recovered = try parseCommitRequest(a, encoded);
    defer recovered.deinit(a);
    var view = try recovered.distributedTables(a);
    defer view.deinit();
    for (view.tables) |table| try std.testing.expectEqual(if (std.mem.eql(u8, table.table_name, "docs")) db_mod.types.CommitRequires.direct_enrichments else .none, table.commit_requires.?);
    try std.testing.expectEqual(db_mod.types.CommitRequires.direct_enrichments, recovered.commit_requires.?);
    for (view.tables) |table| try std.testing.expectEqual(db_mod.types.CommitRequires.direct_enrichments, table.default_commit_requires.?);
    var cloned = try recovered.clone(a);
    defer cloned.deinit(a);
    var weaker = try parseMultiBatchRequest(a, "{\"commit_requires\":\"none\",\"tables\":{\"later\":{\"inserts\":{\"c\":{}}}}}");
    defer weaker.deinit(a);
    try cloned.mergeFrom(a, &weaker);
    try std.testing.expectEqual(db_mod.types.CommitRequires.direct_enrichments, cloned.commit_requires.?);
    const stored = try encodeCommitRequestMode(a, cloned, true);
    defer a.free(stored);
    var parsed_stored = try std.json.parseFromSlice(std.json.Value, a, stored, .{});
    defer parsed_stored.deinit();
    var restored = try parseStoredCommitValue(a, parsed_stored.value);
    defer restored.deinit(a);
    try std.testing.expectEqual(db_mod.types.CommitRequires.direct_enrichments, restored.commit_requires.?);
    var staged = try request.tables[0].clone(a);
    defer staged.deinit(a);
    var next = try parseMultiBatchRequest(a, "{\"commit_requires\":\"none\",\"tables\":{\"docs\":{\"inserts\":{\"c\":{}}}}}");
    defer next.deinit(a);
    try staged.mergeFrom(a, next.tables[0]);
    try std.testing.expectEqual(db_mod.types.CommitRequires.direct_enrichments, staged.commit_requires_floor.?);
    try std.testing.expectEqual(@as(usize, 2), staged.batch.writes.len);
    try cloned.mergeFrom(a, &next);
    try SessionRegistry.normalizeSqlStage(undefined, a, &recovered, &cloned, &next);
    for (cloned.tables) |table| if (std.mem.eql(u8, table.table_name, "docs")) {
        try std.testing.expectEqual(db_mod.types.CommitRequires.direct_enrichments, table.batch.req.commit_requires.?);
        try std.testing.expectEqual(@as(usize, 2), table.batch.writes.len);
    };
}

test "commit requirements inherited defaults strengthen without becoming explicit overrides" {
    const a = std.testing.allocator;
    var staged = try parseCommitRequest(a, "{\"read_set\":[],\"commit_requires\":\"none\",\"tables\":{\"docs\":{\"inserts\":{\"a\":{}}},\"audit\":{\"inserts\":{\"b\":{}},\"commit_requires\":\"none\"}}}");
    defer staged.deinit(a);
    var overridden = try staged.clone(a);
    defer overridden.deinit(a);
    var explicit_stage = try parseCommitRequest(a, "{\"read_set\":[],\"commit_requires\":\"direct_enrichments\",\"tables\":{\"docs\":{\"commit_requires\":\"none\"}}}");
    defer explicit_stage.deinit(a);
    try overridden.mergeFrom(a, &explicit_stage);
    var overridden_view = try overridden.distributedTables(a);
    defer overridden_view.deinit();
    for (overridden_view.tables) |table| try std.testing.expectEqual(db_mod.types.CommitRequires.none, table.commit_requires.?);
    // Exercise the exact representation persisted by a staged session.
    const encoded = try encodeCommitRequestMode(a, staged, true);
    defer a.free(encoded);
    try std.testing.expectError(error.InvalidTransactionCommitRequest, parseCommitRequest(a, encoded));
    var json = try std.json.parseFromSlice(std.json.Value, a, encoded, .{});
    defer json.deinit();
    var restored = try parseStoredCommitValue(a, json.value);
    defer restored.deinit(a);
    var stronger = try parseCommitRequest(a, "{\"read_set\":[],\"commit_requires\":\"direct_enrichments\",\"tables\":{}}");
    defer stronger.deinit(a);
    var registry = SessionRegistry.init(null);
    defer registry.deinit(a);
    const info = try registry.begin(a, .{}, 1);
    const info_mutation = try registry.mutation(a, info.txn_id, info.version.owner);
    _ = (try registry.stage(a, info_mutation, &staged)).?;
    var commit = (try registry.cloneCommitRequest(a, info.txn_id, &stronger)).?;
    defer commit.deinit(a);
    const session_bytes = try encodeSessionRecord(a, registry.sessions.get(info.txn_id).?);
    defer a.free(session_bytes);
    var recovered_session = try decodeSessionRecord(a, info.txn_id, session_bytes);
    defer recovered_session.deinit(a);
    var session_view = try recovered_session.staged.?.distributedTables(a);
    defer session_view.deinit();
    for (session_view.tables) |table| try std.testing.expectEqual(if (std.mem.eql(u8, table.table_name, "docs")) db_mod.types.CommitRequires.direct_enrichments else .none, table.commit_requires.?);
    try restored.mergeFrom(a, &stronger);
    var view = try restored.distributedTables(a);
    defer view.deinit();
    for (view.tables) |table| try std.testing.expectEqual(if (std.mem.eql(u8, table.table_name, "docs")) db_mod.types.CommitRequires.direct_enrichments else .none, table.commit_requires.?);
    // A later explicit none cannot weaken a direct prerequisite already
    // selected for the retained source postimage.
    var weaker = try parseCommitRequest(a, "{\"read_set\":[],\"tables\":{\"docs\":{\"inserts\":{\"c\":{}},\"commit_requires\":\"none\"}}}");
    defer weaker.deinit(a);
    try restored.mergeFrom(a, &weaker);
    const persisted = try encodeCommitRequestMode(a, restored, true);
    defer a.free(persisted);
    var persisted_json = try std.json.parseFromSlice(std.json.Value, a, persisted, .{});
    defer persisted_json.deinit();
    var final = try parseStoredCommitValue(a, persisted_json.value);
    defer final.deinit(a);
    var final_view = try final.distributedTables(a);
    defer final_view.deinit();
    for (final_view.tables) |table| try std.testing.expectEqual(if (std.mem.eql(u8, table.table_name, "docs")) db_mod.types.CommitRequires.direct_enrichments else .none, table.commit_requires.?);
}

test "distributed txn observation retirement advances beyond malformed index rows within the scan budget" {
    const alloc = std.testing.allocator;
    var backend = mem_backend.Backend.init(alloc, .{});
    defer backend.close();
    var store = try backend.runtimeStore(alloc, .{ .name = "system/observation-retirement" });
    defer store.deinit();
    var durable = DurableSessionStore.initRuntime(alloc, &store);

    const id: [16]u8 = @splat(1);
    const reference: [16]u8 = @splat(2);
    var valid_buf: [128]u8 = undefined;
    const valid = try durable.observationRegistrationKey(id, &valid_buf);
    var prefix_buf: [128]u8 = undefined;
    const prefix = try durable.observationRegistrationPrefix(&prefix_buf);
    const invalid = try std.fmt.allocPrint(alloc, "{s}!", .{prefix});
    defer alloc.free(invalid);
    {
        var write = try store.beginWrite();
        errdefer write.abort();
        try write.put(invalid, "invalid");
        try write.put(valid, &reference);
        try write.commit();
    }

    const first = try durable.observationRetirementPage(alloc, null, 1);
    defer alloc.free(first.entries);
    defer if (first.next_after) |key| alloc.free(key);
    try std.testing.expectEqual(@as(usize, 0), first.entries.len);
    try std.testing.expectEqualStrings(invalid, first.next_after.?);

    const next = try durable.observationRetirementPage(alloc, first.next_after, 2);
    defer alloc.free(next.entries);
    defer if (next.next_after) |key| alloc.free(key);
    try std.testing.expectEqual(@as(usize, 1), next.entries.len);
    try std.testing.expectEqual(id, next.entries[0].transaction_id);
    try std.testing.expectEqual(reference, next.entries[0].reference);
    try std.testing.expect(next.next_after == null);
}

test "distributed txn observation capacity follows live records rather than the sequence window" {
    const alloc = std.testing.allocator;
    var backend = mem_backend.Backend.init(alloc, .{});
    defer backend.close();
    var store = try backend.runtimeStore(alloc, .{ .name = "system/observation-capacity" });
    defer store.deinit();
    const outbox: DurableSessionStore.Outbox.Namespace = .{ .observation = true };
    const producer: DurableSessionStore.Outbox.Producer = .{ .id = @splat(1), .next = 65537, .active = 1 };
    const state = try std.json.Stringify.valueAlloc(alloc, producer, .{});
    defer alloc.free(state);
    var write = try store.beginWrite();
    errdefer write.abort();
    try write.put("\x00\x00__sql_observation_producer__:0000000000000000", state);
    const reference = try outbox.create(&write, alloc, @splat(1), .{}, true);
    try std.testing.expectEqual(@as(usize, 65534), try outbox.available(&write, alloc));
    try outbox.remove(&write, alloc, reference);
    try std.testing.expect((try outbox.load(&write, alloc, reference)) == null);
    try write.commit();
}

test "distributed txn expiry scan counts malformed keys and resumes before a canonical key" {
    const alloc = std.testing.allocator;
    var backend = mem_backend.Backend.init(alloc, .{});
    defer backend.close();
    var store = try backend.runtimeStore(alloc, .{ .name = "system/expiry-scan-budget" });
    defer store.deinit();
    var durable = DurableSessionStore.initRuntime(alloc, &store);
    const id: [16]u8 = @splat(1);
    const valid = try makeExpiryKey(alloc, sql_abort_expiry_prefix, 1, id);
    defer alloc.free(valid);
    {
        var write = try store.beginWrite();
        errdefer write.abort();
        try write.put(sql_abort_expiry_prefix ++ "!", "");
        try write.put(sql_abort_expiry_prefix ++ "!begin-overlong-run" ++ "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", "");
        try write.put(sql_abort_expiry_prefix ++ "!begin-overlong-run" ++ "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx" ++ "extra", "");
        try write.put(valid, "");
        try write.commit();
    }
    const first = try durable.scanExpiredIds(alloc, sql_abort_expiry_prefix, null, 2, 1, null);
    defer alloc.free(first.ids);
    try std.testing.expectEqual(@as(usize, 0), first.ids.len);
    const second = try durable.scanExpiredIds(alloc, sql_abort_expiry_prefix, first.next_after.?, 2, 1, null);
    defer alloc.free(second.ids);
    try std.testing.expectEqual(@as(usize, 0), second.ids.len);
    const third = try durable.scanExpiredIds(alloc, sql_abort_expiry_prefix, second.next_after.?, 2, 2, null);
    defer alloc.free(third.ids);
    try std.testing.expectEqualSlices([16]u8, &.{id}, third.ids);
    try std.testing.expect(third.next_after == null);
}

test "distributed txn shared acknowledgement sweep reclaims retired observation issuers only" {
    const alloc = std.testing.allocator;
    var backend = mem_backend.Backend.init(alloc, .{});
    defer backend.close();
    var store = try backend.runtimeStore(alloc, .{ .name = "system/shared-observation-ack-sweep" });
    defer store.deinit();
    var durable = DurableSessionStore.initRuntime(alloc, &store);
    const retirement = @import("completion_retirement.zig");
    const id: [16]u8 = @splat(3);
    const observed: [16]u8 = .{ 0, 0, 0, 0, 0, 0, 0, 7, 128, 0, 0, 0, 0, 0, 0, 1 };
    var ordinary = observed;
    ordinary[8] = 0;
    const old_key = try completionReferenceKey(alloc, id, observed);
    defer alloc.free(old_key);
    const ordinary_key = try completionReferenceKey(alloc, id, ordinary);
    defer alloc.free(ordinary_key);
    {
        var write = try store.beginWrite();
        errdefer write.abort();
        try retirement.acknowledge(&write, alloc, DurableSessionStore.completion_retirement_root, observed, old_key);
        try retirement.acknowledge(&write, alloc, DurableSessionStore.completion_retirement_root, ordinary, ordinary_key);
        try write.commit();
    }
    // The old issuer has no local facade. The sweep does not need its outbox.
    try std.testing.expect(try durable.reclaimLegacyObservationAcks(true));
    try std.testing.expect(!try durable.reclaimLegacyObservationAcks(true));
    var read = try store.beginRead();
    defer read.abort();
    try std.testing.expectError(error.NotFound, read.get(old_key));
    try std.testing.expectEqualStrings(retirement.marker, try read.get(ordinary_key));
}

test "distributed txn ordinary expiry retries an unprocessed page after a storage error" {
    const alloc = std.testing.allocator;
    var backend = mem_backend.Backend.init(alloc, .{});
    defer backend.close();
    var store = try backend.runtimeStore(alloc, .{ .name = "system/expiry-retry" });
    defer store.deinit();
    var durable = DurableSessionStore.initRuntime(alloc, &store);
    var registry = SessionRegistry.init(&durable);
    defer registry.deinit(alloc);
    _ = try registry.begin(alloc, .{}, 0);
    _ = try registry.begin(alloc, .{}, 0);
    const cutoff = std.math.maxInt(u64);
    durable.fail_writes_for_test = true;
    try std.testing.expectError(error.InjectedSessionStoreFailure, registry.cleanupExpired(alloc, cutoff));
    try std.testing.expect(registry.hasSessionCleanupBacklog(false));
    durable.fail_writes_for_test = false;
    try std.testing.expectEqual(@as(usize, 2), try registry.cleanupExpired(alloc, cutoff));
    try std.testing.expect(!registry.hasSessionCleanupBacklog(false));
}

test "distributed txn observation reuse rejects a different transaction on both lookup cuts" {
    const alloc = std.testing.allocator;
    var backend = mem_backend.Backend.init(alloc, .{});
    defer backend.close();
    var store = try backend.runtimeStore(alloc, .{ .name = "system/observation-identity" });
    defer store.deinit();
    var durable = DurableSessionStore.initRuntime(alloc, &store);
    var registry = SessionRegistry.init(&durable);
    defer registry.deinit(alloc);
    const original = try registry.begin(alloc, .{}, 0);
    const other = try registry.begin(alloc, .{}, 0);
    const original_hex = std.fmt.bytesToHex(original.txn_id, .lower);
    const other_hex = std.fmt.bytesToHex(other.txn_id, .lower);
    const reference = try durable.createLocalObservedRegistration(@splat(1), .{ .transaction_id = &other_hex }, other.txn_id);
    {
        var key_buf: [128]u8 = undefined;
        const key = try durable.observationRegistrationKey(original.txn_id, &key_buf);
        var write = try store.beginWrite();
        errdefer write.abort();
        try write.put(key, &reference);
        try write.commit();
    }
    try std.testing.expectError(error.InvalidTransactionSessionRecord, durable.existingObservationReference(original.txn_id));
    try std.testing.expectError(error.InvalidTransactionSessionRecord, durable.registrationMutation(.{ .create = .{
        .seed = @splat(2),
        .outcome = .{ .transaction_id = &original_hex },
        .prepared = true,
        .local_transaction_id = original.txn_id,
        .observe_only = true,
    } }));
    try std.testing.expectEqual(@as(?[16]u8, reference), try durable.existingObservationReference(other.txn_id));
}

test "distributed txn document revision sessions preserve assignments through clone savepoints and durable recovery" {
    const a = std.testing.allocator;
    var backend = mem_backend.Backend.init(a, .{});
    defer backend.close();
    var store = try backend.runtimeStore(a, .{ .name = "system/revision-sessions" });
    defer store.deinit();
    var durable = DurableSessionStore.initRuntime(a, &store);
    var id: db_mod.types.TxnId = undefined;
    var point: u64 = undefined;
    {
        var registry = SessionRegistry.init(&durable);
        defer registry.deinit(a);
        id = (try registry.begin(a, .{}, 7)).txn_id;
        var request = try parseCommitRequest(a, "{\"read_set\":[],\"tables\":{\"docs\":{\"inserts\":{\"a\":{\"n\":1}}}}}");
        defer request.deinit(a);
        const revisions = @import("sql/execution/document_revision_custody.zig");
        const scope: revisions.Scope = .{ .metadata_incarnation = @splat('1'), .transaction_id = id, .principal_digest = @splat(3), .begin_timestamp = 4, .table_id = 5, .shard_id = 6, .range_id = 7 };
        try request.tables[0].mergeDocumentRevisions(a, &.{.{ .scope = scope, .references = &.{.{ .key = "a", .version = 9007199254740993 }} }});
        _ = (try registry.stage(a, try registry.mutation(a, id, 7), &request)).?;
        point = (try registry.createSavepoint(a, try registry.mutation(a, id, ((try registry.readInfo(id)).?).version.owner))).?.savepoint_id;
        var later = try parseCommitRequest(a, "{\"read_set\":[],\"tables\":{\"docs\":{\"inserts\":{\"b\":{\"n\":2}}}}}");
        defer later.deinit(a);
        try later.tables[0].mergeDocumentRevisions(a, &.{.{ .scope = scope, .references = &.{.{ .key = "b", .version = 9007199254740994 }} }});
        _ = (try registry.stage(a, try registry.mutation(a, id, 7), &later)).?;
        var clone = try registry.sessions.get(id).?.staged.?.clone(a);
        defer clone.deinit(a);
        const views = try clone.distributedTables(a);
        defer views.deinit();
        try std.testing.expectEqual(@as(usize, 2), views.tables[0].document_revision_handoffs[0].references.len);
    }
    {
        var registry = SessionRegistry.init(&durable);
        defer registry.deinit(a);
        _ = (try registry.rollbackToSavepoint(a, try registry.mutation(a, id, ((try registry.readInfo(id)).?).version.owner), point)).?;
        var clone = (try registry.cloneCommitRequest(a, id, null)).?;
        defer clone.deinit(a);
        const held = clone.tables[0].document_revisions.?.value;
        try std.testing.expectEqual(@as(usize, 1), held[0].references.len);
        try std.testing.expectEqualStrings("a", held[0].references[0].key);
        try std.testing.expectEqual(@as(u64, 9007199254740993), held[0].references[0].version);
    }
}

test "durable SQL session restores generation after child rollback and retains freeze invalidation" {
    const a = std.testing.allocator;
    var backend = mem_backend.Backend.init(a, .{});
    defer backend.close();
    var store = try backend.runtimeStore(a, .{ .name = "system/sql-generation-sessions" });
    defer store.deinit();
    var durable = DurableSessionStore.initRuntime(a, &store);
    var registry = SessionRegistry.init(&durable);
    defer registry.deinit(a);
    const info = try registry.beginForPrincipal(a, .{ .sql = .{
        .database = "app",
        .namespace = "public",
        .isolation = .read_committed,
        .mode = .read_write,
    } }, 9, "alice");
    var arena_state = std.heap.ArenaAllocator.init(a);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    var first = try generation_intents.testingEntry(arena, info.txn_id, 2, 0);
    var prepared = try first.prepared(arena);
    prepared.creation_guard = null;
    prepared.previous = .{
        .resource = .{ .kind = .table, .id = 40, .sql_relation_id = first.logical_relation_id, .parent_id = prepared.namespace_id, .name = first.target.table, .storage_name = "published" },
        .table = .{ .table_id = 40, .name = "published", .schema_json = "{}" },
        .ranges = &.{.{ .table_id = 40, .group_id = 606, .start_key = "" }},
    };
    first.preparation = try std.json.Stringify.valueAlloc(arena, prepared, .{});
    try std.testing.expectError(error.Forbidden, registry.sqlGenerationBinding(arena, info.txn_id, "bob", first.target, true));
    try std.testing.expect((try registry.sqlGenerationBinding(arena, info.txn_id, "alice", first.target, true)).entry == null);
    try std.testing.expectError(error.Forbidden, registry.stageSqlGeneration(a, info.txn_id, "bob", first));
    durable.fail_writes_for_test = true;
    try std.testing.expectError(error.InjectedSessionStoreFailure, registry.stageSqlGeneration(a, info.txn_id, "alice", first));
    try std.testing.expectEqual(@as(usize, 0), registry.sessions.get(info.txn_id).?.sql.?.catalog_generations.entries.items.len);
    durable.fail_writes_for_test = false;
    try registry.stageSqlGeneration(a, info.txn_id, "alice", first);
    try registry.stageSqlGeneration(a, info.txn_id, "alice", first);
    try std.testing.expect(registry.sessions.get(info.txn_id).?.sql.?.characteristics_locked);
    try std.testing.expectError(error.UnsupportedSqlExecution, registry.cloneCommitRequest(a, info.txn_id, null));
    try std.testing.expect(!registry.commitBeforeExecution(a, info.txn_id));
    const point = (try registry.createNamedSavepoint(a, try registry.mutation(a, info.txn_id, info.version.owner), "child")).?;
    var second = try generation_intents.testingEntry(arena, info.txn_id, 3, point.savepoint_id);
    var child = try second.prepared(arena);
    child.creation_guard = null;
    child.previous = prepared.previous;
    second.preparation = try std.json.Stringify.valueAlloc(arena, child, .{});
    try std.testing.expectError(error.InvalidTransactionSessionRecord, registry.stageSqlGeneration(a, info.txn_id, "alice", first));
    var stale = (try durable.savepointSnapshot(a, info.txn_id)).?;
    defer stale.value.deinit(a);
    try registry.stageSqlGeneration(a, info.txn_id, "alice", second);
    const child_binding = try registry.sqlGenerationBinding(arena, info.txn_id, "alice", first.target, true);
    try std.testing.expectEqual(point.savepoint_id, child_binding.subtransaction);
    try std.testing.expect(child_binding.entry.?.sameDeclaration(second));
    try std.testing.expectError(error.SqlWriteConflict, durable.saveScopedSession(stale.value, stale.digest, null, null, &first.birth, null, null));

    {
        var decoded = (try durable.load(info.txn_id)).?;
        defer decoded.deinit(a);
        try std.testing.expectEqual(@as(usize, 1), decoded.savepoints.get(point.savepoint_id).?.generation_count);
        try std.testing.expectEqual(second.generation, (try decoded.sql.?.catalog_generations.requireFreeze(first.target, point.savepoint_id, false)).generation);
    }

    var reopened = SessionRegistry.init(&durable);
    defer reopened.deinit(a);
    _ = (try reopened.rollbackToNamedSavepoint(a, try reopened.mutation(a, info.txn_id, info.version.owner), "child")).?;
    const restored_binding = try reopened.sqlGenerationBinding(arena, info.txn_id, "alice", first.target, true);
    try std.testing.expect(restored_binding.entry.?.sameDeclaration(first));
    try std.testing.expect(child_binding.entry.?.sameDeclaration(second));
    {
        var reloaded = (try durable.load(info.txn_id)).?;
        defer reloaded.deinit(a);
        try std.testing.expectEqual(first.generation, reloaded.sql.?.catalog_generations.latest(first.target).?.generation);
        try std.testing.expectError(error.SqlCopyFreezeGenerationRequired, reloaded.sql.?.catalog_generations.requireFreeze(first.target, 0, false));
    }

    var successor = try generation_intents.testingEntry(arena, info.txn_id, 4, point.savepoint_id);
    var successor_definition = try successor.prepared(arena);
    successor_definition.creation_guard = null;
    successor_definition.previous = prepared.previous;
    successor.preparation = try std.json.Stringify.valueAlloc(arena, successor_definition, .{});
    try reopened.stageSqlGeneration(a, info.txn_id, "alice", successor);
    _ = (try reopened.releaseNamedSavepoint(a, try reopened.mutation(a, info.txn_id, info.version.owner), "child")).?;
    var released = (try durable.load(info.txn_id)).?;
    defer released.deinit(a);
    try std.testing.expectEqual(successor.generation, (try released.sql.?.catalog_generations.requireFreeze(first.target, 0, false)).generation);

    const intents = released.sql.?.catalog_generations;
    const binding: @import("../system_catalog/prepared_transactions.zig").CatalogBinding = .{
        .fence = successor.birth,
        .transaction = info.txn_id,
        .operation = @splat(22),
        .intent_digest = intents.digest(),
    };
    const local = @import("antfly_local_sources");
    const preview = @import("generation_handoff_preview.zig");
    const staging = @import("../metadata/restore_staging.zig");
    const Witness = struct {
        fn get(_: *const anyopaque, _: std.mem.Allocator, id: staging.Id, name: []const u8, old: local.common_topology_records.RangeRecord, next: u64) !preview.Preview {
            try std.testing.expectEqualStrings("published", name);
            try std.testing.expectEqual(@as(u64, 606), old.group_id);
            const fence: local.storage_db_relational_integrity_topology_contract.Fence = .{
                .transition_id = std.mem.readInt(u64, id[0..8], .little),
                .attempt = std.mem.readInt(u64, id[8..16], .little),
                .admission_epoch = 2,
                .owner_group_id = old.group_id,
                .peer_group_id = next,
                .role = .rewrite_source,
                .namespace = .{ .table_id = old.table_id, .shard_id = 606, .range_id = 606 },
                .catalog_digest = @splat(7),
            };
            return .{ .fence = fence, .handoff = .{
                .source_group_id = old.group_id,
                .target_group_id = next,
                .source_namespace = fence.namespace,
                .admissions = &.{},
                .admissions_digest = try local.storage_portable_backup.sourceGenerationAdmissionSummaryDigest(fence.namespace, &.{}),
                .retired_digest = @splat(8),
                .retired_count = 1,
            } };
        }
    };
    const capture: local.storage_db_relational_population_contract.Capture = .{
        .capture_id = @splat(1),
        .definition = @splat(2),
        .source_definition = @splat(3),
        .target_schema = @splat(4),
        .principal_instance = @splat(5),
        .catalog_revision = 7,
        .write_version = 9,
        .expires_at_ms = 100,
    };
    const Cohort = @import("sql/copy/generation_rows.zig").Cohort;
    const staging_id = try staging.idForAttempt(23, 1);
    try std.testing.expectError(error.SqlRelationRecoveryIncomplete, Cohort.init(a, &released, staging_id, binding, capture));
    var cohort = try Cohort.initWithPredecessors(a, &released, staging_id, binding, capture, .{ .ptr = &binding, .get = Witness.get });
    defer cohort.deinit();
    try std.testing.expectEqual(@as(usize, 1), cohort.plan.targets.len);
    var target = cohort.plan.targets[0];
    try std.testing.expectEqual(successor.generation, target.table.table_id);
    try std.testing.expectEqual(first.logical_relation_id, target.catalog_binding.?.effectiveLogicalRelationId());
    try std.testing.expectEqual(@as(u64, 40), target.replace.?.table.table_id);
    try std.testing.expectEqual(@as(u64, 1), target.replace.?.fences[0].attempt);
    try std.testing.expectEqual(target.ranges[0].group_id, target.replace.?.fences[0].peer_group_id);
    try std.testing.expectEqual(@as(u64, 1), target.generation_handoffs[0].retired_count);
    const ordinary_tables: []const distributed_txn.TableCommitRequest = &.{.{ .table_name = "published_elsewhere" }};
    const encoded = try encodeCatalogExecutionPlan(a, .{ .tables = ordinary_tables, .catalog = cohort.plan });
    defer a.free(encoded);
    var ordinary_view = try parseExecutionPlan(a, encoded);
    defer ordinary_view.deinit();
    try std.testing.expectEqualStrings("published_elsewhere", ordinary_view.value[0].table_name);
    var recovery_view = try parseCatalogExecutionPlan(a, encoded);
    defer recovery_view.deinit();
    try std.testing.expectEqual(cohort.participant.plan_digest, try recovery_view.value.catalog.digest(a));
    try std.testing.expectEqual(cohort.plan.catalog_binding.?.transaction, recovery_view.value.catalog.catalog_binding.?.transaction);
    var tampered = cohort.plan;
    var handoff = target.generation_handoffs[0];
    handoff.retired_count += 1;
    var changed = target;
    changed.generation_handoffs = &.{handoff};
    tampered.targets = &.{changed};
    try std.testing.expect(!std.mem.eql(u8, &cohort.participant.plan_digest, &try tampered.digest(arena)));
    target.replace.?.table.name = "unrelated";
    try std.testing.expectError(error.InvalidCatalogMutation, intents.requireTargets(a, &.{target}));

    var created: generation_intents.Pending = .{};
    defer created.deinit(a);
    const birth = try generation_intents.testingEntry(arena, info.txn_id, 1, 0);
    try created.append(a, birth);
    const replacement = try generation_intents.testingEntry(arena, info.txn_id, 2, 1);
    try created.append(a, replacement);
    try created.rollback(a, 1);
    try std.testing.expectEqual(birth.generation, (try created.requireFreeze(birth.target, 0, false)).generation);
}

test "SQL generation intents frozen births survive key reuse cold reopen and savepoint rollback" {
    const a = std.testing.allocator;
    const generations = generation_intents;
    const births = @import("sql/copy/frozen_births.zig");
    const Helper = struct {
        fn entry(a_: std.mem.Allocator, transaction: [16]u8) !generations.Entry {
            var entry_ = try generations.testingEntry(a_, transaction, 1, 0);
            entry_.birth.principal = try @import("../system_catalog/lock_authority.zig").principalDigest(a_, .bootstrap);
            errdefer a_.free(entry_.preparation);
            var arena = std.heap.ArenaAllocator.init(a_);
            defer arena.deinit();
            var prepared = try entry_.prepared(arena.allocator());
            const local = @import("antfly_local_sources");
            prepared.child.schema_json = try local.sql_ddl_runtime.createSchemaAlloc(arena.allocator(), .{
                .table = .{ .table = "generation_test" },
                .columns = &.{.{ .name = "v", .type = .string, .nullable = true }},
            });
            prepared.child.min_ranges = 2;
            const ranges = [_]@import("antfly_local_sources").common_topology_records.RangeRecord{
                .{ .table_id = prepared.child.table_id, .group_id = 801, .range_id = 801, .doc_identity_shard_id = 801, .doc_identity_range_id = 801, .start_key = "", .end_key = "m" },
                .{ .table_id = prepared.child.table_id, .group_id = 802, .range_id = 802, .doc_identity_shard_id = 802, .doc_identity_range_id = 802, .start_key = "m" },
            };
            prepared.child_ranges = &ranges;
            const replacement = try std.json.Stringify.valueAlloc(a_, prepared, .{});
            a_.free(entry_.preparation);
            entry_.preparation = replacement;
            return entry_;
        }
        fn statement(a_: std.mem.Allocator, entry_: generations.Entry, json: []const u8, rows: []const births.Row) !OwnedTransactionCommitRequest {
            var request = try parseCommitRequest(a_, json);
            errdefer request.deinit(a_);
            const name = try std.fmt.allocPrint(a_, "table:{s}", .{std.fmt.bytesToHex(entry_.physical, .lower)});
            defer a_.free(name);
            try request.bind(a_, "rows", name);
            var arena = std.heap.ArenaAllocator.init(a_);
            defer arena.deinit();
            const scratch = arena.allocator();
            const prepared = try entry_.prepared(scratch);
            const header = try std.json.parseFromSliceLeaky(struct { version: u32 = 0 }, scratch, prepared.child.schema_json, .{ .ignore_unknown_fields = true });
            try request.frozen_births.append(a_, .{ .transaction = entry_.transaction, .nonce = entry_.nonce, .physical = entry_.physical, .schema_version = header.version }, rows);
            return request;
        }
    };
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/frozen-birth-session", .{tmp.sub_path}, 0);
    defer a.free(path);
    var transaction: [16]u8 = undefined;
    {
        var physical = try docstore_mod.DocStore.open(a, path, .{});
        defer physical.close();
        var store = DurableSessionStore.init(a, &physical);
        defer store.deinitVolatilePayloads();
        var registry = SessionRegistry.init(&store);
        defer registry.deinit(a);
        const begun = try registry.beginForPrincipal(a, .{ .sql = .{ .database = "app", .namespace = "public", .isolation = .repeatable_read, .mode = .read_write } }, 1, "alice");
        transaction = begun.txn_id;
        const entry = try Helper.entry(a, transaction);
        defer a.free(entry.preparation);
        try registry.stageSqlGeneration(a, transaction, "alice", entry);
        {
            var forged = try Helper.statement(a, entry,
                \\{"read_set":[],"tables":{"rows":{"inserts":{"a":{"n":1}}}}}
            , &.{.{ .key = "a", .document = "{\"n\":99}" }});
            defer forged.deinit(a);
            try std.testing.expectError(error.InvalidTransactionSessionRecord, registry.stageSql(a, transaction, &forged));
            var journal = @import("sql/session/command_mutations.zig").Journal.init(a);
            defer journal.deinit();
            try std.testing.expectError(error.InvalidTransactionSessionRecord, journal.append(&forged));
            var missing = try Helper.statement(a, entry,
                \\{"read_set":[],"tables":{"rows":{"deletes":["a"]}}}
            , &.{.{ .key = "a", .document = "{\"n\":1}" }});
            defer missing.deinit(a);
            try std.testing.expectError(error.InvalidTransactionSessionRecord, registry.stageSql(a, transaction, &missing));
        }
        var first = try Helper.statement(a, entry,
            \\{"read_set":[],"tables":{"rows":{"inserts":{"z":{"n":0},"a":{"n":1}}}}}
        , &.{ .{ .key = "z", .document = "{\"n\":0}" }, .{ .key = "a", .document = "{\"n\":1}" } });
        defer first.deinit(a);
        _ = (try registry.stageSql(a, transaction, &first)).?;
        _ = (try registry.createNamedSavepoint(a, transaction, "before_reinsert")).?;
        var retirement = try Helper.statement(a, entry,
            \\{"read_set":[],"tables":{"rows":{"deletes":["a"]}}}
        , &.{});
        defer retirement.deinit(a);
        _ = (try registry.stageSql(a, transaction, &retirement)).?;
        var second = try Helper.statement(a, entry,
            \\{"read_set":[],"tables":{"rows":{"inserts":{"a":{"n":2}}}}}
        , &.{.{ .key = "a", .document = "{\"n\":2}" }});
        defer second.deinit(a);
        // A generation born in the parent cannot FREEZE in this child.
        try std.testing.expectError(error.SqlCopyFreezeGenerationRequired, registry.stageSql(a, transaction, &second));
        _ = (try registry.releaseNamedSavepoint(a, transaction, "before_reinsert")).?;
        _ = (try registry.stageSql(a, transaction, &second)).?;
        _ = (try registry.createNamedSavepoint(a, transaction, "keep_two")).?;
        var ordinary = try Helper.statement(a, entry,
            \\{"read_set":[],"tables":{"rows":{"inserts":{"a":{"n":3}}}}}
        , &.{});
        defer ordinary.deinit(a);
        _ = (try registry.stageSql(a, transaction, &ordinary)).?;
        // Public encoding/decoding can neither disclose nor mint private births.
        const public = try encodeCommitRequest(a, second);
        defer a.free(public);
        try std.testing.expect(std.mem.indexOf(u8, public, "frozen_births") == null);
        var spoofed = try parseCommitRequest(a,
            \\{"read_set":[],"tables":{"rows":{"inserts":{"a":{}}}},"frozen_births":"untrusted"}
        );
        defer spoofed.deinit(a);
        try std.testing.expectEqual(@as(usize, 0), spoofed.frozen_births.rows);
        // The ordinary journal folds current rows but retains all COPY events.
        var journal = @import("sql/session/command_mutations.zig").Journal.init(a);
        defer journal.deinit();
        try journal.append(&first);
        try journal.append(&retirement);
        try journal.append(&second);
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const folded = try journal.folded(arena.allocator());
        try requireFrozenStatementImages(a, folded, true);
        try std.testing.expectEqual(@as(usize, 3), folded.frozen_births.rows);
        try std.testing.expectEqual(@as(usize, 2), folded.tables[0].batch.writes.len);
    }
    {
        var physical = try docstore_mod.DocStore.open(a, path, .{});
        defer physical.close();
        var store = DurableSessionStore.init(a, &physical);
        defer store.deinitVolatilePayloads();
        var registry = SessionRegistry.init(&store);
        defer registry.deinit(a);
        var cold = (try store.loadInto(a, transaction)).?;
        defer cold.deinit(a);
        try std.testing.expectEqual(@as(usize, 3), cold.staged.?.frozen_births.rows);
        const historical = cold.staged.?.frozen_births.groups.items[0].rows.items;
        try std.testing.expect(!historical[0].superseded);
        try std.testing.expect(historical[1].superseded);
        try std.testing.expect(historical[2].superseded);
        const latest = for (cold.staged.?.tables[0].batch.writes) |write| {
            if (std.mem.eql(u8, write.key, "a")) break write.value;
        } else return error.TestExpectedEqual;
        try std.testing.expectEqualStrings("{\"n\":3}", latest);
        const streams = @import("sql/copy/generation_rows.zig");
        const contract = @import("antfly_local_sources").storage_db_relational_population_contract;
        const entry = cold.sql.?.catalog_generations.entries.items[0];
        var current = try streams.Stream.init(a, &cold, entry, .current);
        defer current.deinit();
        var frozen = try streams.Stream.init(a, &cold, entry, .frozen);
        defer frozen.deinit();
        try std.testing.expectEqual(@as(u64, 2), current.manifest.source_rows);
        try std.testing.expectEqual(@as(u64, 3), frozen.manifest.source_rows);
        try std.testing.expectEqualStrings("{\"n\":3}", current.rows[0].value);
        try std.testing.expectEqualStrings("{\"n\":1}", frozen.rows[0].value);
        try std.testing.expectEqualStrings("{\"n\":2}", frozen.rows[1].value);
        try std.testing.expectEqualStrings("{\"n\":0}", frozen.rows[2].value);
        const former = try contract.frozen_key.decode(frozen.rows[0].key);
        const later = try contract.frozen_key.decode(frozen.rows[1].key);
        try std.testing.expectEqual(@as(u64, 1), former.ordinal);
        try std.testing.expectEqual(@as(u64, 2), later.ordinal);
        try std.testing.expectEqual(contract.frozen_key.Lineage.superseded, former.lineage);
        try std.testing.expectEqual(contract.frozen_key.Lineage.superseded, later.lineage);
        try std.testing.expectEqual(contract.frozen_key.Lineage.surviving, (try contract.frozen_key.decode(frozen.rows[2].key)).lineage);
        const frozen_boundary = try contract.frozen_key.lower(a, "m");
        defer a.free(frozen_boundary);
        for ([_]*streams.Stream{ &current, &frozen }) |stream| {
            var cursors: [2]contract.Cursor = undefined;
            for (&cursors, [_]u64{ 801, 802 }) |*cursor, partition| {
                cursor.* = try contract.Cursor.init(.{
                    .capture_id = entry.nonce,
                    .definition = stream.definition,
                    .source_definition = stream.definition,
                    .target_schema = @splat(4),
                    .principal_instance = @splat(5),
                    .catalog_revision = 7,
                    .write_version = 9,
                    .partition = partition,
                    .transaction_rows = stream.manifest.forPartition(partition),
                });
            }
            const boundary = if (stream.manifest.image == .frozen) frozen_boundary else "m";
            while (try stream.next(1)) |page| {
                const cursor = &cursors[if (std.mem.order(u8, page[0].key.?, boundary) == .lt) @as(usize, 0) else 1];
                cursor.* = (try cursor.admit(.{ .binding = cursor.binding, .sequence = cursor.pages, .first_row = cursor.rows, .previous = cursor.chain, .rows = page })).apply;
            }
            var sealed_rows: u64 = 0;
            for (cursors) |cursor| {
                const sealed = try cursor.seal(cursor.completionForSource(stream.manifest.source_rows));
                try std.testing.expect(sealed.rows != 0);
                sealed_rows += sealed.rows;
            }
            try std.testing.expectEqual(stream.manifest.source_rows, sealed_rows);
        }
        const pending = cold.sql.?.catalog_generations;
        const binding: @import("../system_catalog/prepared_transactions.zig").CatalogBinding = .{
            .fence = entry.birth,
            .transaction = transaction,
            .operation = @splat(21),
            .intent_digest = pending.digest(),
        };
        var cohort = try streams.Cohort.init(a, &cold, @splat(23), binding, .{
            .capture_id = @splat(1),
            .definition = @splat(2),
            .source_definition = @splat(3),
            .target_schema = @splat(4),
            .principal_instance = @splat(5),
            .catalog_revision = 7,
            .write_version = 9,
            .expires_at_ms = 100,
        });
        defer cohort.deinit();
        try std.testing.expectEqual(@as(usize, 1), cohort.plan.targets.len);
        const sealed = cohort.plan.targets[0];
        try std.testing.expectEqual(@as(u64, 2), sealed.population.?.transaction_rows.?.source_rows);
        try std.testing.expectEqual(@as(u64, 3), sealed.frozen_population.?.capture.transaction_rows.?.source_rows);
        try std.testing.expectEqualDeep(cohort.participant.plan_digest, try cohort.plan.digest(a));
        const stages = @import("../metadata/restore_staging.zig");
        const scope = try stages.ownerScope(a, cohort.plan, cohort.participant.plan_digest, sealed, sealed.ranges[0]);
        try scope.validate();
        try std.testing.expectEqualDeep(binding.intent_digest, scope.catalog_intent_digest.?);
        try std.testing.expectError(error.InvalidRestoreStaging, cohort.plan.validate(a));
        const retained_job: stages.Job = .{ .plan = cohort.plan, .plan_digest = cohort.participant.plan_digest };
        try retained_job.validateRetained(a);
        var changed_job = retained_job;
        changed_job.plan_digest[0] ^= 1;
        try std.testing.expectError(error.InvalidRestoreStaging, changed_job.validateRetained(a));
        try cohort.plan.validateCatalog(a, binding.fence.group, &pending);
        const execution_wire = try encodeCatalogExecutionPlan(a, .{ .tables = &.{}, .catalog = cohort.plan });
        defer a.free(execution_wire);
        var retained = try parseCatalogExecutionPlan(a, execution_wire);
        defer retained.deinit();
        var resumed = try streams.Cohort.rejoin(a, &cold, retained.value.catalog, cohort.participant);
        defer resumed.deinit();
        try std.testing.expectEqual(cohort.participant.plan_digest, resumed.participant.plan_digest);
        var wrong = cohort.participant;
        wrong.plan_digest[0] ^= 1;
        try std.testing.expectError(error.ExternalTransactionDecisionChanged, streams.Cohort.rejoin(a, &cold, retained.value.catalog, wrong));
        var incomplete = retained.value.catalog;
        var live_only = incomplete.targets[0];
        live_only.frozen_population = null;
        incomplete.targets = &.{live_only};
        incomplete.cohort_digest = try @import("../metadata/relation_create.zig").fingerprint(a, incomplete.targets);
        var incomplete_participant = cohort.participant;
        incomplete_participant.plan_digest = try incomplete.digest(a);
        try std.testing.expectError(error.RelationPopulationChanged, streams.Cohort.rejoin(a, &cold, incomplete, incomplete_participant));
        const authority: stages.Command.CatalogReservation = .{
            .group = binding.fence.group,
            .pending = &pending,
            .plan_digest = cohort.participant.plan_digest,
        };
        var reserve: stages.Command = .{ .id = cohort.plan.id, .action = .reserve, .plan = cohort.plan };
        const wire = try std.json.Stringify.valueAlloc(a, reserve, .{});
        defer a.free(wire);
        try std.testing.expectError(error.InvalidRestoreStaging, stages.parseCommand(a, wire));
        var decoded = try stages.parseCatalogReservation(a, wire, authority);
        defer decoded.deinit();
        try std.testing.expectEqualDeep(cohort.participant.plan_digest, try decoded.value.plan.?.digest(a));
        var bootstrap_scratch = std.heap.ArenaAllocator.init(a);
        defer bootstrap_scratch.deinit();
        const scratch = bootstrap_scratch.allocator();
        const recovered = decoded.value.plan.?;
        const target = recovered.targets[0];
        const historical_bootstrap = try stages.frozenOwnerBootstrap(scratch, recovered, authority.plan_digest, target, 0);
        try std.testing.expectEqual(.frozen, historical_bootstrap.scope.population.?.transaction_rows.?.image);
        try std.testing.expectEqualDeep(scope.target_namespace, historical_bootstrap.scope.target_namespace);
        try std.testing.expectEqualDeep(binding.intent_digest, historical_bootstrap.scope.catalog_intent_digest.?);
        const historical_digest = historical_bootstrap.scope.digest();
        try std.testing.expect(!std.mem.eql(u8, &scope.digest(), &historical_digest));
        try std.testing.expectEqualStrings(target.frozen_population.?.schema_json, historical_bootstrap.schema_json);
        try std.testing.expectEqualStrings(frozen_boundary, historical_bootstrap.byte_range.end);
        const rejoined = try stages.ownerBootstrapForScope(scratch, recovered, authority.plan_digest, target, 0, historical_digest);
        try std.testing.expectEqualDeep(historical_bootstrap, rejoined);
        const live_bootstrap = try stages.ownerBootstrapForScope(scratch, recovered, authority.plan_digest, target, 0, scope.digest());
        try std.testing.expectEqual(.current, live_bootstrap.scope.population.?.transaction_rows.?.image);
        try std.testing.expectError(error.RestoreStagingScopeChanged, stages.ownerBootstrapForScope(scratch, recovered, authority.plan_digest, target, 1, historical_digest));
        const archive = @import("antfly_local_sources").storage_db_relational_frozen_contract;
        const historical_scope = historical_bootstrap.scope;
        const population = historical_scope.population.?;
        var archive_cursor = try contract.Cursor.init(population);
        var progress: @import("antfly_local_sources").storage_db_restore_staging_contract.Progress = .{ .scope = historical_scope };
        try std.testing.expectError(error.RelationPopulationChanged, archive.Binding.fromProgress(progress));
        const stream = &cohort.sources[0].frozen.?;
        while (try stream.next(1)) |page| {
            if (std.mem.order(u8, page[0].key.?, historical_bootstrap.byte_range.end) != .lt) continue;
            archive_cursor = (try archive_cursor.admit(.{
                .binding = archive_cursor.binding,
                .sequence = archive_cursor.pages,
                .first_row = archive_cursor.rows,
                .previous = archive_cursor.chain,
                .rows = page,
            })).apply;
        }
        archive_cursor = try archive_cursor.seal(archive_cursor.completionForSource(stream.manifest.source_rows));
        progress.phase = .imported;
        progress.population = archive_cursor;
        progress.rows = archive_cursor.rows;
        progress.logical_digest = archive_cursor.chain;
        progress.rows_complete = true;
        progress.artifacts_complete = true;
        progress.source_generation_proofs_complete = true;
        const archive_binding = try archive.Binding.fromProgress(progress);
        try std.testing.expectEqualDeep(try archive.Binding.fromScope(historical_scope), archive_binding);
        try std.testing.expectError(error.InvalidRelationPopulation, archive.Binding.fromScope(scope));
        progress.population.?.content = @splat(0x13);
        try std.testing.expectError(error.InvalidRelationPopulation, archive.Binding.fromProgress(progress));
        const live_binding = live_bootstrap.scope.population.?;
        var live_cursor = try contract.Cursor.init(live_binding);
        const live_stream = &cohort.sources[0].current;
        while (try live_stream.next(1)) |page| {
            if (std.mem.order(u8, page[0].key.?, live_bootstrap.byte_range.end) != .lt) continue;
            live_cursor = (try live_cursor.admit(.{
                .binding = live_cursor.binding,
                .sequence = live_cursor.pages,
                .first_row = live_cursor.rows,
                .previous = live_cursor.chain,
                .rows = page,
            })).apply;
        }
        live_cursor = try live_cursor.seal(live_cursor.completionForSource(live_stream.manifest.source_rows));
        const OwnerProgress = @import("antfly_local_sources").storage_db_restore_staging_contract.Progress;
        var live_proof: OwnerProgress = .{ .scope = scope, .phase = .imported, .rows = live_cursor.rows, .logical_digest = live_cursor.chain, .population = live_cursor };
        var receipt: stages.OwnerReceipt = .{
            .group_id = target.ranges[0].group_id,
            .range_id = scope.target_namespace.range_id,
            .plan_digest = authority.plan_digest,
            .completion_digest = live_proof.receipt(),
            .population = live_cursor,
        };
        try std.testing.expectError(error.InvalidRestoreStaging, receipt.requirePopulation(scope));
        receipt.frozen_population = archive_binding;
        try receipt.requirePopulation(scope);
        receipt.frozen_population.?.content[0] ^= 1;
        try std.testing.expectError(error.InvalidRestoreStaging, receipt.requirePopulation(scope));
        receipt.frozen_population = archive_binding;
        receipt.population = archive_cursor;
        try std.testing.expectError(error.InvalidRestoreStaging, receipt.requirePopulation(scope));
        const tally: stages.PopulationProgress = .{ .source_rows = live_stream.manifest.source_rows };
        receipt.population = live_cursor;
        const partial = try tally.accept(receipt, 2, stream.manifest.source_rows);
        try std.testing.expectEqual(archive_binding.rows, partial.frozen.?.rows);
        try std.testing.expectEqual(live_cursor.rows, partial.rows);
        try std.testing.expectError(error.InvalidRestoreStaging, tally.accept(receipt, 1, stream.manifest.source_rows));
        try std.testing.expectError(error.InvalidRestoreStaging, partial.accept(receipt, 2, stream.manifest.source_rows + 1));
        const metadata = @import("../metadata/storage/raft_apply_store.zig");
        const protocol = @import("../metadata/topology_protocol.zig");
        var metadata_dir = std.testing.tmpDir(.{});
        defer metadata_dir.cleanup();
        const metadata_path = try std.fmt.allocPrint(scratch, ".zig-cache/tmp/{s}/catalog-receipt", .{metadata_dir.sub_path});
        var metadata_store = try metadata.RaftApplyStore.init(a, .{ .root_dir = metadata_path });
        defer metadata_store.deinit();
        const metadata_group = binding.fence.group;
        try metadata_store.applyStandaloneCommand(metadata_group, .{ .initialize_metadata_incarnation = recovered.expected_incarnation.? });
        try metadata_store.applyStandaloneCommand(metadata_group, .{ .activate_topology_protocol = try std.json.Stringify.valueAlloc(scratch, protocol.Activation{
            .version = protocol.sql_catalog_progress_version,
            .incarnation = recovered.expected_incarnation.?,
            .member_count = 1,
            .membership_fingerprint = @splat(19),
        }, .{}) });
        var key_buffer: [256]u8 = undefined;
        try metadata_store.store.put(try stages.jobKey(&key_buffer, metadata_group, recovered.id), try std.json.Stringify.valueAlloc(scratch, stages.Job{ .plan = recovered, .plan_digest = authority.plan_digest }, .{}));
        try metadata_store.store.put(try stages.progressKey(&key_buffer, metadata_group, recovered.id), try std.json.Stringify.valueAlloc(scratch, stages.Progress{}, .{}));
        const command: stages.Command = .{ .id = recovered.id, .action = .imported, .expected_revision = 1, .record_receipt_identity = true, .receipt = receipt };
        const encoded_receipt = try std.json.Stringify.valueAlloc(scratch, command, .{});
        try metadata_store.applyStandaloneCommand(metadata_group, .{ .apply_restore_staging = encoded_receipt });
        try std.testing.expectEqual(null, try metadata_store.loadRestoreStagingReceiptIdentity(scratch, metadata_group, recovered.id, .importing, receipt.group_id));
        try metadata_store.applyStandaloneCommand(metadata_group, .{ .accept_sql_catalog_progress = encoded_receipt });
        try std.testing.expectEqual(null, try metadata_store.loadRestoreStagingReceiptIdentity(scratch, metadata_group, recovered.id, .importing, receipt.group_id));
        const registry_types = @import("../system_catalog/prepared_transactions.zig");
        const admitted: registry_types.Record = .{
            .identity = .{
                .kind = .session_commit,
                .catalog_participant = cohort.participant,
                .lock_scope = .{ .backend = binding.fence.backend, .command = binding.fence.command },
                .session = .{ .owner_node_id = 0, .connection_id = @splat('a'), .standalone = binding.fence.standalone },
                .gid = "",
                .database_id = binding.fence.database,
                .principal = .bootstrap,
                .txn_id = binding.transaction,
                .operation_id = binding.operation,
                .metadata_incarnation = recovered.expected_incarnation.?,
                .begin_timestamp = 5,
                .expires_at_unix_ns = 100,
                .plan_digest = try registry_types.planDigest(execution_wire),
                .manifest_digest = try registry_types.manifestDigest(scratch, &.{}, recovered.expected_incarnation.?),
            },
            .participants = &.{},
        };
        try admitted.validate(scratch);
        try metadata_store.store.put(try registry_types.operationKey(scratch, metadata_group, binding.operation), try std.json.Stringify.valueAlloc(scratch, admitted, .{}));
        try metadata_store.store.put(try registry_types.workKey(scratch, metadata_group, binding.operation), "");
        var wrong_archive = command;
        wrong_archive.receipt.?.frozen_population.?.content[0] ^= 1;
        try metadata_store.applyStandaloneCommand(metadata_group, .{ .accept_sql_catalog_progress = try std.json.Stringify.valueAlloc(scratch, wrong_archive, .{}) });
        try std.testing.expectEqual(null, try metadata_store.loadRestoreStagingReceiptIdentity(scratch, metadata_group, recovered.id, .importing, receipt.group_id));
        for (0..2) |_| try metadata_store.applyStandaloneCommand(metadata_group, .{ .accept_sql_catalog_progress = encoded_receipt });
        const recorded = (try metadata_store.loadRestoreStagingReceiptIdentity(scratch, metadata_group, recovered.id, .importing, receipt.group_id)).?;
        try std.testing.expectEqualSlices(u8, &try command.receiptIdentity(scratch), recorded);
        const saved_progress = (try metadata_store.loadRestoreStagingProgress(scratch, metadata_group, recovered.id)).?;
        try std.testing.expectEqual(@as(u32, 1), saved_progress.completed_owners);
        var validation = command;
        validation.action = .validated;
        validation.receipt.?.population = null;
        validation.receipt.?.frozen_population = null;
        const too_early = try std.json.Stringify.valueAlloc(scratch, validation, .{});
        try metadata_store.applyStandaloneCommand(metadata_group, .{ .accept_sql_catalog_progress = too_early });
        try std.testing.expectEqual(null, try metadata_store.loadRestoreStagingReceiptIdentity(scratch, metadata_group, recovered.id, .validating, receipt.group_id));

        // The import barrier has advanced after the other owner completed.
        try metadata_store.store.put(try stages.progressKey(&key_buffer, metadata_group, recovered.id), try std.json.Stringify.valueAlloc(scratch, stages.Progress{ .state = .validating, .revision = 2 }, .{}));
        validation.expected_revision = 2;
        const validated = try std.json.Stringify.valueAlloc(scratch, validation, .{});
        try metadata_store.applyStandaloneCommand(metadata_group, .{ .apply_restore_staging = validated });
        try std.testing.expectEqual(null, try metadata_store.loadRestoreStagingReceiptIdentity(scratch, metadata_group, recovered.id, .validating, receipt.group_id));
        for (0..2) |_| try metadata_store.applyStandaloneCommand(metadata_group, .{ .accept_sql_catalog_progress = validated });
        const validation_identity = (try metadata_store.loadRestoreStagingReceiptIdentity(scratch, metadata_group, recovered.id, .validating, receipt.group_id)).?;
        try std.testing.expectEqualSlices(u8, &try validation.receiptIdentity(scratch), validation_identity);
        const validated_progress = (try metadata_store.loadRestoreStagingProgress(scratch, metadata_group, recovered.id)).?;
        try std.testing.expectEqual(stages.State.validating, validated_progress.state);
        try std.testing.expectEqual(@as(u32, 1), validated_progress.completed_owners);
        const premature_cancel: stages.Command = .{ .id = recovered.id, .action = .begin_cancel, .expected_revision = 2 };
        try metadata_store.applyStandaloneCommand(metadata_group, .{ .accept_sql_catalog_progress = try std.json.Stringify.valueAlloc(scratch, premature_cancel, .{}) });
        try std.testing.expectEqualDeep(validated_progress, (try metadata_store.loadRestoreStagingProgress(scratch, metadata_group, recovered.id)).?);
        const owner = try admitted.identity.owner(scratch);
        var committed = try registry_types.advance(scratch, admitted, admitted.revision, owner, .prepare);
        if (committed.phase == .locking) committed = try registry_types.advance(scratch, committed, committed.revision, owner, .prepare);
        committed = try registry_types.advance(scratch, committed, committed.revision, owner, .{ .commit = .{ .version = 20 } });
        committed = try registry_types.completeCatalog(scratch, metadata_group, committed, committed.revision, owner, binding, cohort.participant, .published);
        try metadata_store.store.put(try registry_types.operationKey(scratch, metadata_group, binding.operation), try std.json.Stringify.valueAlloc(scratch, committed, .{}));
        try metadata_store.store.put(try stages.progressKey(&key_buffer, metadata_group, recovered.id), try std.json.Stringify.valueAlloc(scratch, stages.Progress{ .state = .published, .revision = 3, .completed_owners = 2 }, .{}));
        live_proof.phase = .published;
        var published_ack = command;
        published_ack.action = .catalog_owner_published;
        published_ack.expected_revision = 3;
        published_ack.receipt.?.frozen_population = null;
        published_ack.receipt.?.completion_digest = live_proof.receipt();
        const ack_wire = try std.json.Stringify.valueAlloc(scratch, published_ack, .{});
        try metadata_store.applyStandaloneCommand(metadata_group, .{ .apply_restore_staging = ack_wire });
        var altered_ack = published_ack;
        altered_ack.receipt.?.completion_digest[0] ^= 1;
        try metadata_store.applyStandaloneCommand(metadata_group, .{ .accept_sql_catalog_progress = try std.json.Stringify.valueAlloc(scratch, altered_ack, .{}) });
        try std.testing.expectEqual(null, try metadata_store.loadRestoreStagingReceiptIdentity(scratch, metadata_group, recovered.id, .published, receipt.group_id));
        for (0..2) |_| try metadata_store.applyStandaloneCommand(metadata_group, .{ .accept_sql_catalog_progress = ack_wire });
        const ack = (try metadata_store.loadRestoreStagingReceiptIdentity(scratch, metadata_group, recovered.id, .published, receipt.group_id)).?;
        try std.testing.expectEqualSlices(u8, &try published_ack.receiptIdentity(scratch), ack);
        const ack_progress = (try metadata_store.loadRestoreStagingProgress(scratch, metadata_group, recovered.id)).?;
        try std.testing.expectEqual(@as(u32, 2), ack_progress.completed_owners);
        try std.testing.expectEqual(@as(u32, 1), ack_progress.published_owners);
        try std.testing.expectEqual(@as(u64, 4), ack_progress.revision);

        const restore = @import("restore_owner.zig");
        const Probe = struct {
            expected: [32]u8,
            calls: usize = 0,
            fn execute(raw: *anyopaque, _: std.mem.Allocator, table: []const u8, group: u64, request: restore.Request, _: @import("antfly_local_sources").api_operation.RequestContext) !restore.Response {
                const self: *@This() = @ptrCast(@alignCast(raw));
                self.calls += 1;
                try std.testing.expectEqual(@as(u64, 801), group);
                try std.testing.expect(table.len != 0);
                try std.testing.expectEqualDeep(self.expected, request.scope.digest());
                try std.testing.expect(request.action == .status);
                return error.CatalogPublicationProofPending;
            }
        };
        var probe: Probe = .{ .expected = historical_digest };
        const port: restore.Port = .{ .ptr = &probe, .execute_fn = Probe.execute };
        try std.testing.expectError(error.CatalogPublicationProofPending, cohort.deliverImage(a, 0, .frozen, port, .{}, .{}));
        probe.expected = scope.digest();
        try std.testing.expectError(error.CatalogPublicationProofPending, cohort.deliverImage(a, 0, .current, port, .{}, .{}));
        try std.testing.expectEqual(@as(usize, 2), probe.calls);
        try std.testing.expectError(error.CatalogPublicationProofPending, cohort.deliverTarget(a, 0, port, .{}, .{}));
        const CancelProbe = struct {
            archive: [32]u8,
            primary: [32]u8,
            first_pending: bool = true,
            sent: usize = 0,

            fn execute(raw: *anyopaque, _: std.mem.Allocator, _: []const u8, owner_group: u64, request: restore.Request, _: @import("antfly_local_sources").api_operation.RequestContext) !restore.Response {
                const self: *@This() = @ptrCast(@alignCast(raw));
                try std.testing.expectEqual(@as(u64, 801), owner_group);
                try std.testing.expectEqual(.cancel, request.action);
                defer self.sent += 1;
                if (self.sent < 2 or self.sent == 3) {
                    try std.testing.expectEqualDeep(self.archive, request.scope.digest());
                    return .{ .phase = if (self.first_pending) .imported else .canceled, .rows = 0, .receipt = @splat(31) };
                }
                try std.testing.expectEqualDeep(self.primary, request.scope.digest());
                return .{ .phase = .canceled, .rows = 0, .receipt = @splat(32) };
            }
        };
        var cancellation: CancelProbe = .{ .archive = historical_digest, .primary = scope.digest() };
        const cancellation_port: restore.Port = .{ .ptr = &cancellation, .execute_fn = CancelProbe.execute };
        const abort_job: stages.Job = .{ .plan = recovered, .plan_digest = authority.plan_digest, .state = .canceling };
        const first_cancel = abort_job.plan.catalogCancelTarget(0).?;
        const next_cancel = abort_job.plan.catalogCancelTarget(1).?;
        try std.testing.expectEqual(@as(u64, 801), first_cancel.target.ranges[first_cancel.range_index].group_id);
        try std.testing.expectEqual(@as(u64, 802), next_cancel.target.ranges[next_cancel.range_index].group_id);
        try std.testing.expect(abort_job.plan.catalogCancelTarget(2) == null);
        const fence: @import("antfly_local_sources").storage_db_relational_integrity_topology_contract.Fence = .{
            .transition_id = 8,
            .attempt = 1,
            .owner_group_id = 800,
            .peer_group_id = 801,
            .namespace = .{ .table_id = 44, .shard_id = 800, .range_id = 800 },
            .role = .child_generation_source,
            .catalog_digest = @splat(21),
        };
        var predecessor_target = target;
        predecessor_target.replace = .{
            .table = .{ .table_id = 44, .name = "previous" },
            .ranges = &.{.{ .table_id = 44, .group_id = 800, .range_id = 800, .start_key = "" }},
            .fences = &.{fence},
        };
        var predecessor_plan = abort_job.plan;
        predecessor_plan.targets = &.{predecessor_target};
        try std.testing.expect((try predecessor_plan.catalogCancelPredecessor(1)) == null);
        const previous = (try predecessor_plan.catalogCancelPredecessor(2)).?;
        try std.testing.expectEqualDeep(fence, previous.fence);
        const cutover_predecessor = (try predecessor_plan.catalogPredecessorAt(0)).?;
        try std.testing.expectEqualDeep(previous.fence, cutover_predecessor.fence);
        try std.testing.expect((try predecessor_plan.catalogPredecessorAt(1)) == null);
        try std.testing.expect((try predecessor_plan.catalogCancelPredecessor(3)) == null);
        var stale_fence = fence;
        stale_fence.admission_epoch += 1;
        try std.testing.expect(!std.mem.eql(u8, &try stages.predecessorCancelDigest(fence), &try stages.predecessorCancelDigest(stale_fence)));
        predecessor_target.ranges = target.ranges[0..1];
        predecessor_target.generation_handoffs = &.{.{
            .source_group_id = fence.owner_group_id,
            .target_group_id = fence.peer_group_id,
            .source_namespace = fence.namespace,
            .admissions = &.{},
            .admissions_digest = @splat(22),
            .retired_digest = @splat(23),
            .retired_count = 0,
        }};
        predecessor_plan.targets = &.{predecessor_target};

        const activating_digest = try predecessor_plan.digest(scratch);
        const activation = predecessor_plan.catalogActivationOwnerAt(0).?;
        try std.testing.expect(predecessor_plan.catalogActivationOwnerAt(1) == null);
        const live_replacement = try stages.ownerBootstrapForRangeIndex(scratch, predecessor_plan, activating_digest, activation.target, 0);
        const activation_fence = try stages.emptyGenerationDestinationFence(predecessor_plan, activation.target, activation.range, activation.handoff, live_replacement.scope);
        try std.testing.expectEqual(fence.owner_group_id, activation_fence.peer_group_id);
        const archived_replacement = try stages.frozenOwnerBootstrap(scratch, predecessor_plan, activating_digest, activation.target, 0);
        try std.testing.expectError(error.InvalidRestoreStaging, stages.emptyGenerationDestinationFence(predecessor_plan, activation.target, activation.range, activation.handoff, archived_replacement.scope));
        try std.testing.expectError(error.CatalogPublicationProofPending, streams.cancelOwner(a, abort_job, target, 0, cancellation_port, .{}));
        try std.testing.expectEqual(@as(usize, 1), cancellation.sent);
        cancellation.first_pending = false;
        const canceled_once = try streams.cancelOwner(a, abort_job, target, 0, cancellation_port, .{});
        const canceled_retry = try streams.cancelOwner(a, abort_job, target, 0, cancellation_port, .{});
        try std.testing.expectEqualDeep(canceled_once, canceled_retry);
        try std.testing.expectEqual(@as(usize, 5), cancellation.sent);

        var omitted = sealed;
        omitted.frozen_population = null;
        reserve.plan.?.targets = &.{omitted};
        reserve.plan.?.cohort_digest = try @import("../metadata/relation_create.zig").fingerprint(a, reserve.plan.?.targets);
        const altered_wire = try std.json.Stringify.valueAlloc(a, reserve, .{});
        defer a.free(altered_wire);
        try std.testing.expectError(error.InvalidRestoreStaging, stages.parseCatalogReservation(a, altered_wire, authority));
        try std.testing.expectError(error.InvalidCatalogMutation, cohort.plan.validateCatalog(a, binding.fence.group + 1, &pending));
        var invalid_plan = cohort.plan;
        invalid_plan.catalog_binding.?.transaction = @splat(40);
        try std.testing.expectError(error.InvalidRestoreStaging, invalid_plan.validateCatalog(a, binding.fence.group, &pending));
        invalid_plan = cohort.plan;
        invalid_plan.cohort_digest = @splat(40);
        try std.testing.expectError(error.InvalidRestoreStaging, invalid_plan.validateCatalog(a, binding.fence.group, &pending));
        invalid_plan = cohort.plan;
        invalid_plan.expected_incarnation = @splat('f');
        try std.testing.expectError(error.InvalidRestoreStaging, invalid_plan.validateCatalog(a, binding.fence.group, &pending));
        var altered = sealed;
        altered.population.?.transaction_rows.?.generation_nonce = @splat(30);
        try std.testing.expectError(error.InvalidCatalogMutation, pending.requireTargets(a, &.{altered}));
        altered = sealed;
        altered.frozen_population.?.schema_json = "{}";
        try std.testing.expectError(error.InvalidCatalogMutation, pending.requireTargets(a, &.{altered}));
        _ = (try registry.rollbackToNamedSavepoint(a, transaction, "keep_two")).?;
        var rolled_back = try registry.cloneSqlStaged(a, transaction);
        defer rolled_back.deinit(a);
        try std.testing.expectEqual(@as(usize, 3), rolled_back.frozen_births.rows);
        try std.testing.expectEqual(@as(usize, 2), rolled_back.tables[0].batch.writes.len);
        const row_a = for (rolled_back.tables[0].batch.writes) |write| {
            if (std.mem.eql(u8, write.key, "a")) break write.value;
        } else return error.TestExpectedEqual;
        try std.testing.expectEqualStrings("{\"n\":2}", row_a);
        var ordinary = try parseCommitRequest(a, "{\"read_set\":[],\"tables\":{\"ordinary\":{\"inserts\":{\"b\":{\"n\":4}}}}}");
        defer ordinary.deinit(a);
        _ = (try registry.stageSql(a, transaction, &ordinary)).?;
        var wrong_scope = entry.birth;
        wrong_scope.backend.fence += 1;
        try std.testing.expectError(error.CatalogGenerationChanged, registry.cloneScopedSqlCommitRequest(a, transaction, .write, &wrong_scope));
        var public = (try registry.cloneScopedSqlCommitRequest(a, transaction, .write, &entry.birth)).?;
        defer public.deinit(a);
        try std.testing.expectEqual(@as(usize, 1), public.tables.len);
        try std.testing.expectEqualStrings("ordinary", public.tables[0].table_name);
        try std.testing.expectEqual(@as(usize, 0), public.frozen_births.rows);
        try std.testing.expectEqual(@as(usize, 0), public.catalog_bindings.items.len);
        var retained_request = (try store.loadInto(a, transaction)).?;
        defer retained_request.deinit(a);
        try std.testing.expectEqual(@as(usize, 2), retained_request.staged.?.tables.len);
        try std.testing.expectEqual(@as(usize, 3), retained_request.staged.?.frozen_births.rows);
    }
}
