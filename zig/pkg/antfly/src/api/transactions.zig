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
const platform_sync = @import("antfly_platform").sync;
const batch_api = @import("batch.zig");
const db_mod = @import("../storage/db/selected_root.zig").db;
const distributed_txn = @import("distributed_txn.zig");
const backend_erased = @import("../storage/backend_erased.zig");
const docstore_mod = @import("../storage/docstore.zig");
const mem_backend = @import("../storage/mem_backend.zig");
const lease_mod = @import("../storage/db/lease.zig");
const platform_time = @import("antfly_platform").time;

const session_prefix = "\x00\x00__api_txn_sessions__:";
const session_lease_prefix = "\x00\x00__api_txn_session_leases__:";
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
        commands: []const @import("../storage/db/relational_integrity_contract.zig").Command,
        pub fn jsonStringify(self: @This(), stream: anytype) @TypeOf(stream.*).Error!void {
            return @import("../storage/db/relational_integrity_json.zig").write(self, stream);
        }
    };
    /// Server-authored arbiter observations survive statement merging,
    /// savepoints, restart and final native dependency expansion.
    conflict_guards: ?std.json.Parsed(ConflictGuards) = null,
    range_guards: ?@import("range_read_guards.zig").Owned = null,
    schema_version: ?u32 = null,
    table_name: []u8,
    relational_schema_version: ?u32 = null,
    batch: batch_api.OwnedBatchRequest = .{},
    predicates: std.ArrayListUnmanaged(db_mod.types.TransactionVersionPredicate) = .empty,
    txn_writes: []db_mod.types.TransactionWrite = &.{},

    pub fn deinit(self: *TableCommitRequest, alloc: std.mem.Allocator) void {
        if (self.conflict_guards) |*guards| guards.deinit();
        if (self.range_guards) |*guards| guards.deinit();
        alloc.free(self.table_name);
        if (self.txn_writes.len > 0) alloc.free(self.txn_writes);
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
        };
        errdefer out.deinit(alloc);
        out.batch = try cloneBatchRequest(alloc, self.batch);
        if (self.conflict_guards) |guards| try out.mergeConflictGuards(alloc, guards.value);
        if (self.range_guards) |guards| try out.mergeRangeGuards(alloc, guards.value);
        try clonePredicatesInto(alloc, &out.predicates, self.predicates.items);
        return out;
    }

    pub fn mergeFrom(self: *TableCommitRequest, alloc: std.mem.Allocator, other: TableCommitRequest) !void {
        if (other.range_guards) |guards| try self.mergeRangeGuards(alloc, guards.value);
        if (other.conflict_guards) |guards| try self.mergeConflictGuards(alloc, guards.value);
        if (self.schema_version != null and other.schema_version != null and self.schema_version != other.schema_version) return error.CatalogGenerationChanged;
        if (self.schema_version == null) self.schema_version = other.schema_version;
        if (self.relational_schema_version != null and other.relational_schema_version != null and self.relational_schema_version != other.relational_schema_version) return error.CatalogGenerationChanged;
        if (self.relational_schema_version == null) self.relational_schema_version = other.relational_schema_version;
        try appendBatchWrites(alloc, &self.batch, other.batch.writes);
        try appendBatchDeletes(alloc, &self.batch, other.batch.deletes);
        try appendBatchTransforms(alloc, &self.batch, other.batch.transforms);
        try appendPredicates(alloc, &self.predicates, other.predicates.items);
        syncAndClear(self, alloc);
    }

    pub fn mergeConflictGuards(self: *TableCommitRequest, alloc: std.mem.Allocator, incoming: ConflictGuards) !void {
        const native = @import("../storage/db/relational_integrity_contract.zig");
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

    pub fn mergeRangeGuards(self: *TableCommitRequest, alloc: std.mem.Allocator, incoming: []const @import("range_read_guards.zig").OwnerRangeProof) !void {
        const merged = try @import("range_read_guards.zig").merge(alloc, if (self.range_guards) |guards| guards.value else &.{}, incoming);
        if (self.range_guards) |*guards| guards.deinit();
        self.range_guards = merged;
    }

    pub fn prepareWrites(self: *TableCommitRequest, alloc: std.mem.Allocator) !void {
        if (self.txn_writes.len > 0) return;
        self.txn_writes = try alloc.alloc(db_mod.types.TransactionWrite, self.batch.writes.len);
        for (self.batch.writes, 0..) |write, i| {
            self.txn_writes[i] = .{
                .key = write.key,
                .value = write.value,
                .json_null_fields = write.json_null_fields,
            };
        }
    }

    pub fn result(self: TableCommitRequest) batch_api.BatchResult {
        return self.batch.result();
    }
};

pub const CatalogBinding = struct { logical: []const u8, physical: []const u8 };

pub const OwnedTransactionCommitRequest = struct {
    constraint_timing: std.ArrayListUnmanaged(@import("../storage/relational_index.zig").ConstraintTiming) = .empty,

    // Server-authored identity bindings are persisted with staged operations.
    // Public JSON cannot supply them. Labels remain stable across renames.
    catalog_bindings: std.ArrayListUnmanaged(CatalogBinding) = .empty,

    read_set: []TransactionReadItem = &.{},
    tables: []TableCommitRequest = &.{},
    sync_level: db_mod.types.SyncLevel = .propose,

    pub fn setConstraintTiming(self: *@This(), alloc: std.mem.Allocator, mode: @import("../storage/relational_index.zig").ConstraintTiming) !void {
        if (mode.generation == null) self.constraint_timing.clearRetainingCapacity();
        for (self.constraint_timing.items) |*previous| if (std.meta.eql(previous.generation, mode.generation)) {
            previous.* = mode;
            return;
        };
        if (self.constraint_timing.items.len >= 4096) return error.TransactionTooLarge;
        try self.constraint_timing.append(alloc, mode);
    }

    pub fn bind(self: *OwnedTransactionCommitRequest, alloc: std.mem.Allocator, logical: []const u8, physical: []const u8) !void {
        for (self.catalog_bindings.items) |binding| if (std.mem.eql(u8, binding.logical, logical)) {
            if (!std.mem.eql(u8, binding.physical, physical)) return error.CatalogGenerationChanged;
            return;
        };
        const owned_logical = try alloc.dupe(u8, logical);
        errdefer alloc.free(owned_logical);
        const owned_physical = try alloc.dupe(u8, physical);
        errdefer alloc.free(owned_physical);
        try self.catalog_bindings.append(alloc, .{ .logical = owned_logical, .physical = owned_physical });
    }
    pub fn physicalName(self: OwnedTransactionCommitRequest, logical: []const u8) []const u8 {
        for (self.catalog_bindings.items) |binding| if (std.mem.eql(u8, binding.logical, logical)) return binding.physical;
        return logical;
    }
    pub fn observeRanges(self: *OwnedTransactionCommitRequest, alloc: std.mem.Allocator, logical: []const u8, physical: []const u8, schema_version: u32, observations: []const @import("range_read_guards.zig").OwnerRangeProof) !void {
        try self.bind(alloc, logical, physical);
        for (self.tables) |*table| if (std.mem.eql(u8, self.physicalName(table.table_name), physical)) {
            if (table.schema_version != null and table.schema_version != schema_version) return error.CatalogGenerationChanged;
            try table.mergeRangeGuards(alloc, observations);
            table.schema_version = schema_version;
            return;
        };
        var guards = try @import("range_read_guards.zig").merge(alloc, &.{}, observations);
        defer guards.deinit();
        try appendTable(alloc, self, .{ .table_name = @constCast(logical), .schema_version = schema_version, .range_guards = guards });
    }
    pub fn logicalName(self: OwnedTransactionCommitRequest, physical: []const u8) []const u8 {
        for (self.catalog_bindings.items) |binding| if (std.mem.eql(u8, binding.physical, physical)) return binding.logical;
        return physical;
    }

    pub fn deinit(self: *OwnedTransactionCommitRequest, alloc: std.mem.Allocator) void {
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
            .sync_level = self.sync_level,
        };
        errdefer out.deinit(alloc);
        try out.constraint_timing.appendSlice(alloc, self.constraint_timing.items);
        for (self.catalog_bindings.items) |binding| try out.bind(alloc, binding.logical, binding.physical);

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

    pub fn mergeFrom(self: *OwnedTransactionCommitRequest, alloc: std.mem.Allocator, other: *const OwnedTransactionCommitRequest) !void {
        for (other.constraint_timing.items) |mode| try self.setConstraintTiming(alloc, mode);
        for (other.catalog_bindings.items) |binding| try self.bind(alloc, binding.logical, binding.physical);
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
    }

    pub fn distributedTables(self: *OwnedTransactionCommitRequest, alloc: std.mem.Allocator) ![]distributed_txn.TableCommitRequest {
        for (self.tables) |*table| try table.prepareWrites(alloc);
        var out = try alloc.alloc(distributed_txn.TableCommitRequest, self.tables.len);
        for (self.tables, 0..) |*table, i| {
            out[i] = .{
                .table_name = self.physicalName(table.table_name),
                .schema_version = table.schema_version,
                .relational_schema_version = table.relational_schema_version,
                .relational_integrity_generation_set = if (table.conflict_guards) |guards| guards.value.generation_set else null,
                .integrity_commands = if (table.conflict_guards) |guards| guards.value.commands else &.{},
                .range_guards = if (table.range_guards) |guards| guards.value else &.{},
                .writes = table.txn_writes,
                .deletes = table.batch.deletes,
                .transforms = table.batch.transforms,
                .predicates = table.predicates.items,
            };
        }
        return out;
    }

    /// Rollback undoes staged mutations, not observations already exposed to
    /// the client. Retain range dependencies across savepoint rollback so a
    /// later write cannot evade serializable validation using an undone read.
    pub fn retainRangeGuards(self: *OwnedTransactionCommitRequest, alloc: std.mem.Allocator, other: *const OwnedTransactionCommitRequest) !void {
        for (other.tables) |table| if (table.range_guards) |guards| {
            const physical = other.physicalName(table.table_name);
            try self.bind(alloc, table.table_name, physical);
            const existing = for (self.tables) |*entry| {
                if (std.mem.eql(u8, self.physicalName(entry.table_name), physical)) break entry;
            } else null;
            if (existing) |entry| {
                if (entry.schema_version != null and table.schema_version != null and entry.schema_version != table.schema_version) return error.CatalogGenerationChanged;
                try entry.mergeRangeGuards(alloc, guards.value);
                if (entry.schema_version == null) entry.schema_version = table.schema_version;
            } else try appendTable(alloc, self, .{ .table_name = table.table_name, .schema_version = table.schema_version, .relational_schema_version = table.relational_schema_version, .range_guards = guards });
        };
    }
};

pub const ExecutionPlan = std.json.Parsed([]const distributed_txn.TableCommitRequest);

pub fn parseExecutionPlan(alloc: std.mem.Allocator, bytes: []const u8) !ExecutionPlan {
    return std.json.parseFromSlice([]const distributed_txn.TableCommitRequest, alloc, bytes, .{ .allocate = .alloc_always });
}

fn encodeExecutionPlan(alloc: std.mem.Allocator, tables: []const distributed_txn.TableCommitRequest) ![]u8 {
    const Wire = struct {
        tables: []const distributed_txn.TableCommitRequest,
        pub fn jsonStringify(self: @This(), stream: anytype) !void {
            try @import("../storage/db/relational_integrity_json.zig").write(self.tables, stream);
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
pub const CommitConflictReason = @import("distributed_txn_contract.zig").CommitConflictReason;

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
    sync_level: db_mod.types.SyncLevel = .propose,
    sql: ?SqlMetadata = null,
};

pub const SqlMetadata = struct {
    database: []const u8,
    namespace: []const u8,
    isolation: @import("../sql/session.zig").Isolation,
    mode: @import("../sql/session.zig").ReadMode,
    failed: bool = false,

    pub fn clone(self: SqlMetadata, alloc: std.mem.Allocator) !SqlMetadata {
        const database = try alloc.dupe(u8, self.database);
        errdefer alloc.free(database);
        var out = self;
        out.database = database;
        out.namespace = try alloc.dupe(u8, self.namespace);
        return out;
    }

    pub fn deinit(self: *SqlMetadata, alloc: std.mem.Allocator) void {
        alloc.free(self.database);
        alloc.free(self.namespace);
        self.* = undefined;
    }
};

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
pub const PendingSessionRecovery = union(enum) {
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
        request: OwnedTransactionCommitRequest,
        execution_plan: ?[]u8 = null,
    },
    acknowledge: PendingTerminalAcknowledgement,

    pub fn deinit(self: *PendingSessionRecovery, alloc: std.mem.Allocator) void {
        switch (self.*) {
            .commit => |*value| {
                value.request.deinit(alloc);
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
    table_name: []u8,
    staged_read_count: usize,
    staged_write_count: usize,
    staged_delete_count: usize,
    staged_predicate_count: usize,

    pub fn deinit(self: *SessionTableDetail, alloc: std.mem.Allocator) void {
        alloc.free(self.table_name);
        self.* = undefined;
    }
};

pub const SessionDetails = struct {
    status: SessionStatus,
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
    name: ?[]u8 = null,
    snapshot: OwnedTransactionCommitRequest,
    read_snapshots: std.StringArrayHashMapUnmanaged(SessionReadSnapshot) = .empty,

    pub fn deinit(self: *Savepoint, alloc: std.mem.Allocator) void {
        if (self.name) |name| alloc.free(name);
        self.snapshot.deinit(alloc);
        deinitReadSnapshotMap(alloc, &self.read_snapshots);
        self.* = undefined;
    }

    pub fn clone(self: Savepoint, alloc: std.mem.Allocator) !Savepoint {
        const name = if (self.name) |name| try alloc.dupe(u8, name) else null;
        errdefer if (name) |value| alloc.free(value);
        var out: Savepoint = .{
            .id = self.id,
            .name = name,
            .snapshot = try self.snapshot.clone(alloc),
        };
        errdefer out.snapshot.deinit(alloc);
        out.read_snapshots = try cloneReadSnapshotMap(alloc, self.read_snapshots);
        return out;
    }
};

pub const Session = struct {
    sql: ?SqlMetadata = null,
    txn_id: db_mod.types.TxnId,
    owner_node_id: u64,
    /// Stable authenticated subject that created this session. `null` is the
    /// anonymous principal used only when authentication is disabled. The
    /// binding is immutable across node-owner lease transfers.
    principal: ?[]u8 = null,
    begin_timestamp: u64,
    last_touched_timestamp: u64,
    sync_level: db_mod.types.SyncLevel,
    staged: ?OwnedTransactionCommitRequest = null,
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
    /// Persisted before releasing the retained coordinator's topology fence.
    terminal_commit: ?TerminalCommit = null,
    read_snapshots: std.StringArrayHashMapUnmanaged(SessionReadSnapshot) = .empty,
    next_savepoint_id: u64 = 1,
    savepoints: std.AutoHashMapUnmanaged(u64, Savepoint) = .empty,

    pub fn info(self: Session) SessionInfo {
        return .{
            .txn_id = self.txn_id,
            .begin_timestamp = self.begin_timestamp,
            .sync_level = self.sync_level,
        };
    }

    pub fn deinit(self: *Session, alloc: std.mem.Allocator) void {
        if (self.sql) |*metadata| metadata.deinit(alloc);
        if (self.principal) |principal| alloc.free(principal);
        if (self.staged) |*staged| staged.deinit(alloc);
        if (self.execution_plan) |bytes| alloc.free(bytes);
        if (self.terminal_commit) |*terminal| terminal.deinit(alloc);
        deinitReadSnapshotMap(alloc, &self.read_snapshots);
        var it = self.savepoints.iterator();
        while (it.next()) |entry| entry.value_ptr.deinit(alloc);
        self.savepoints.deinit(alloc);
        self.* = undefined;
    }

    pub fn clone(self: Session, alloc: std.mem.Allocator) !Session {
        var out: Session = .{
            .txn_id = self.txn_id,
            .owner_node_id = self.owner_node_id,
            .principal = if (self.principal) |principal| try alloc.dupe(u8, principal) else null,
            .begin_timestamp = self.begin_timestamp,
            .last_touched_timestamp = self.last_touched_timestamp,
            .sync_level = self.sync_level,
            .next_savepoint_id = self.next_savepoint_id,
            .commit_body_digest = self.commit_body_digest,
            .commit_execution_started = self.commit_execution_started,
        };
        errdefer out.deinit(alloc);
        if (self.sql) |metadata| out.sql = try metadata.clone(alloc);
        if (self.staged) |staged| out.staged = try staged.clone(alloc);
        if (self.execution_plan) |bytes| out.execution_plan = try alloc.dupe(u8, bytes);
        if (self.terminal_commit) |terminal| out.terminal_commit = try terminal.clone(alloc);
        out.read_snapshots = try cloneReadSnapshotMap(alloc, self.read_snapshots);
        try out.savepoints.ensureUnusedCapacity(alloc, self.savepoints.count());
        var it = self.savepoints.iterator();
        while (it.next()) |entry| {
            out.savepoints.putAssumeCapacity(entry.key_ptr.*, try entry.value_ptr.clone(alloc));
        }
        return out;
    }
};

pub const DurableSessionStore = struct {
    alloc: std.mem.Allocator,
    backend: Backend,
    fail_writes_for_test: bool = false,
    fail_lease_transition_after_session_write_for_test: bool = false,

    const Backend = union(enum) {
        docstore: *docstore_mod.DocStore,
        runtime: *backend_erased.Store,
    };

    pub fn init(alloc: std.mem.Allocator, store: *docstore_mod.DocStore) DurableSessionStore {
        return .{
            .alloc = alloc,
            .backend = .{ .docstore = store },
        };
    }

    /// Binds session durability to an existing storage-engine namespace. The
    /// runtime store remains owned by the engine and must outlive this value.
    pub fn initRuntime(alloc: std.mem.Allocator, store: *backend_erased.Store) DurableSessionStore {
        return .{ .alloc = alloc, .backend = .{ .runtime = store } };
    }

    pub fn save(self: *DurableSessionStore, session: Session, max_record_bytes: ?usize) !void {
        if (self.fail_writes_for_test) return error.InjectedSessionStoreFailure;
        const key = try makeSessionKey(self.alloc, session.txn_id);
        defer self.alloc.free(key);
        const value = try encodeSessionRecord(self.alloc, session);
        defer self.alloc.free(value);
        if (max_record_bytes) |limit| {
            if (value.len > limit) return error.SessionRecordTooLarge;
        }
        switch (self.backend) {
            .docstore => |store| {
                var txn = try store.beginWriteTxn();
                errdefer txn.abort();
                try putSessionAndExpiryTxn(self, &txn, key, value, session);
                try txn.commit();
            },
            .runtime => |store| {
                var txn = try store.beginWrite();
                errdefer txn.abort();
                try putSessionAndExpiryTxn(self, &txn, key, value, session);
                try txn.commit();
            },
        }
    }

    /// Atomically publishes a session owner and its fencing lease in the same
    /// storage transaction. `expected_owner` is null for a new session; when it
    /// is present the durable session must still name that owner. Expired-only
    /// transitions reject an unexpired lease owned by another node.
    pub fn saveWithLease(
        self: *DurableSessionStore,
        session: Session,
        expected_owner: ?u64,
        now_ms: u64,
        ttl_ms: u64,
        require_expired: bool,
        max_record_bytes: ?usize,
    ) !bool {
        if (self.fail_writes_for_test) return error.InjectedSessionStoreFailure;
        return switch (self.backend) {
            .docstore => |store| blk: {
                var txn = try store.beginWriteTxn();
                errdefer txn.abort();
                const changed = try saveSessionWithLeaseTxn(self, &txn, session, expected_owner, now_ms, ttl_ms, require_expired, max_record_bytes);
                if (!changed) {
                    txn.abort();
                    break :blk false;
                }
                try txn.commit();
                break :blk true;
            },
            .runtime => |store| blk: {
                var txn = try store.beginWrite();
                errdefer txn.abort();
                const changed = try saveSessionWithLeaseTxn(self, &txn, session, expected_owner, now_ms, ttl_ms, require_expired, max_record_bytes);
                if (!changed) {
                    txn.abort();
                    break :blk false;
                }
                try txn.commit();
                break :blk true;
            },
        };
    }

    fn saveSessionWithLeaseTxn(
        self: *DurableSessionStore,
        txn: anytype,
        session: Session,
        expected_owner: ?u64,
        now_ms: u64,
        ttl_ms: u64,
        require_expired: bool,
        max_record_bytes: ?usize,
    ) !bool {
        const session_key = try makeSessionKey(self.alloc, session.txn_id);
        defer self.alloc.free(session_key);
        const lease_key = try makeSessionLeaseKey(self.alloc, session.txn_id);
        defer self.alloc.free(lease_key);

        const current_raw = txn.get(session_key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        if (expected_owner) |owner| {
            const raw = current_raw orelse return false;
            var current = try decodeSessionRecord(self.alloc, session.txn_id, raw);
            defer current.deinit(self.alloc);
            if (current.owner_node_id != owner) return false;
        } else if (current_raw != null) return false;

        const owner_id = try ownerLeaseId(self.alloc, session.owner_node_id);
        defer self.alloc.free(owner_id);
        const lease_raw = txn.get(lease_key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        if (lease_raw) |raw| {
            const parsed = try std.json.parseFromSlice(lease_mod.LeaseRecord, self.alloc, raw, .{ .allocate = .alloc_always });
            defer parsed.deinit();
            if (require_expired and parsed.value.expires_at_ms > now_ms and !std.mem.eql(u8, parsed.value.owner_id, owner_id)) return false;
        }

        const session_value = try encodeSessionRecord(self.alloc, session);
        defer self.alloc.free(session_value);
        if (max_record_bytes) |limit| if (session_value.len > limit) return error.SessionRecordTooLarge;
        const lease_value = try std.json.Stringify.valueAlloc(self.alloc, lease_mod.LeaseRecord{
            .owner_id = owner_id,
            .expires_at_ms = now_ms + ttl_ms,
        }, .{});
        defer self.alloc.free(lease_value);
        var previous_needs_recovery = false;
        if (current_raw) |raw| {
            var previous = try decodeSessionRecord(self.alloc, session.txn_id, raw);
            defer previous.deinit(self.alloc);
            previous_needs_recovery = sessionNeedsRecovery(previous);
            const old_expiry_key = try makeSessionExpiryKey(self.alloc, previous.last_touched_timestamp, session.txn_id);
            defer self.alloc.free(old_expiry_key);
            txn.delete(old_expiry_key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
        }
        const expiry_key = try makeSessionExpiryKey(self.alloc, session.last_touched_timestamp, session.txn_id);
        defer self.alloc.free(expiry_key);
        try txn.put(session_key, session_value);
        try txn.put(expiry_key, &.{});
        if (sessionNeedsRecovery(session)) {
            try setSessionRecoveryIndexTxn(self, txn, session);
        } else if (previous_needs_recovery) {
            try clearSessionRecoveryIndexTxn(self, txn, session.txn_id);
        }
        if (self.fail_lease_transition_after_session_write_for_test) return error.InjectedLeaseTransitionFailure;
        try txn.put(lease_key, lease_value);
        return true;
    }

    pub fn load(self: *DurableSessionStore, txn_id: db_mod.types.TxnId) !?Session {
        const key = try makeSessionKey(self.alloc, txn_id);
        defer self.alloc.free(key);
        const value = switch (self.backend) {
            .docstore => |store| store.get(self.alloc, key) catch |err| switch (err) {
                error.NotFound => return null,
                else => return err,
            },
            .runtime => |store| blk: {
                var txn = try store.beginRead();
                defer txn.abort();
                const raw = txn.get(key) catch |err| switch (err) {
                    error.NotFound => return null,
                    else => return err,
                };
                break :blk try self.alloc.dupe(u8, raw);
            },
        };
        defer self.alloc.free(value);
        return try decodeSessionRecord(self.alloc, txn_id, value);
    }

    pub fn delete(self: *DurableSessionStore, txn_id: db_mod.types.TxnId) !void {
        if (self.fail_writes_for_test) return error.InjectedSessionStoreFailure;
        const key = try makeSessionKey(self.alloc, txn_id);
        defer self.alloc.free(key);
        switch (self.backend) {
            .docstore => |store| {
                var txn = try store.beginWriteTxn();
                errdefer txn.abort();
                try deleteSessionAndExpiryTxn(self, &txn, key, txn_id);
                try txn.commit();
            },
            .runtime => |store| {
                var txn = try store.beginWrite();
                errdefer txn.abort();
                try deleteSessionAndExpiryTxn(self, &txn, key, txn_id);
                try txn.commit();
            },
        }
    }

    fn putSessionAndExpiryTxn(self: *DurableSessionStore, txn: anytype, key: []const u8, value: []const u8, session: Session) !void {
        var previous_needs_recovery = false;
        if (txn.get(key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        }) |raw| {
            var previous = try decodeSessionRecord(self.alloc, session.txn_id, raw);
            defer previous.deinit(self.alloc);
            previous_needs_recovery = sessionNeedsRecovery(previous);
            const old_expiry_key = try makeSessionExpiryKey(self.alloc, previous.last_touched_timestamp, session.txn_id);
            defer self.alloc.free(old_expiry_key);
            txn.delete(old_expiry_key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
        }
        const expiry_key = try makeSessionExpiryKey(self.alloc, session.last_touched_timestamp, session.txn_id);
        defer self.alloc.free(expiry_key);
        try txn.put(key, value);
        try txn.put(expiry_key, &.{});
        if (sessionNeedsRecovery(session)) {
            try setSessionRecoveryIndexTxn(self, txn, session);
        } else if (previous_needs_recovery) {
            try clearSessionRecoveryIndexTxn(self, txn, session.txn_id);
        }
    }

    fn deleteSessionAndExpiryTxn(self: *DurableSessionStore, txn: anytype, key: []const u8, txn_id: db_mod.types.TxnId) !void {
        if (txn.get(key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        }) |raw| {
            var previous = try decodeSessionRecord(self.alloc, txn_id, raw);
            defer previous.deinit(self.alloc);
            const expiry_key = try makeSessionExpiryKey(self.alloc, previous.last_touched_timestamp, txn_id);
            defer self.alloc.free(expiry_key);
            txn.delete(expiry_key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
        }
        txn.delete(key) catch |err| switch (err) {
            error.NotFound => {},
            else => return err,
        };
        const recovery_key = try makeSessionRecoveryKey(self.alloc, txn_id);
        defer self.alloc.free(recovery_key);
        txn.delete(recovery_key) catch |err| switch (err) {
            error.NotFound => {},
            else => return err,
        };
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
    pub fn scanLegacyRecoveryIds(
        self: *DurableSessionStore,
        alloc: std.mem.Allocator,
        after: ?db_mod.types.TxnId,
        scan_limit: usize,
    ) !RecoveryIdPage {
        var ids = std.ArrayListUnmanaged(db_mod.types.TxnId).empty;
        errdefer ids.deinit(alloc);
        if (scan_limit == 0) return .{ .ids = try ids.toOwnedSlice(alloc), .next_after = null };
        const start_key = if (after) |txn_id| try makeSessionKey(alloc, txn_id) else null;
        defer if (start_key) |key| alloc.free(key);
        const Scan = struct {
            allocator: std.mem.Allocator,
            scan_limit: usize,
            scanned: usize = 0,
            last_txn_id: ?db_mod.types.TxnId = null,
            ids: *std.ArrayListUnmanaged(db_mod.types.TxnId),

            fn visit(raw: *anyopaque, key: []const u8, value: []const u8) anyerror!bool {
                const scan: *@This() = @ptrCast(@alignCast(raw));
                if (key.len <= session_prefix.len) return true;
                const txn_id = distributed_txn.parseTxnIdHex(key[session_prefix.len..]) catch return true;
                scan.scanned += 1;
                scan.last_txn_id = txn_id;
                var session = decodeSessionRecord(scan.allocator, txn_id, value) catch return scan.scanned < scan.scan_limit;
                defer session.deinit(scan.allocator);
                if (sessionNeedsRecovery(session)) try scan.ids.append(scan.allocator, txn_id);
                return scan.scanned < scan.scan_limit;
            }
        };
        var scan = Scan{ .allocator = alloc, .scan_limit = scan_limit, .ids = &ids };
        try self.scanPrefixFromWithContext(session_prefix, start_key, &scan, Scan.visit);
        const next_after = if (scan.scanned == scan_limit) scan.last_txn_id else null;
        return .{ .ids = try ids.toOwnedSlice(alloc), .next_after = next_after };
    }

    /// Rechecks the current durable row under a write transaction before
    /// backfilling its index entry, avoiding stale audit results.
    pub fn refreshRecoveryIndex(self: *DurableSessionStore, txn_id: db_mod.types.TxnId) !void {
        const session_key = try makeSessionKey(self.alloc, txn_id);
        defer self.alloc.free(session_key);
        const recovery_key = try makeSessionRecoveryKey(self.alloc, txn_id);
        defer self.alloc.free(recovery_key);
        switch (self.backend) {
            .docstore => |store| {
                var txn = try store.beginWriteTxn();
                errdefer txn.abort();
                if (txn.get(session_key) catch |err| switch (err) {
                    error.NotFound => null,
                    else => return err,
                }) |raw| {
                    var session = try decodeSessionRecord(self.alloc, txn_id, raw);
                    defer session.deinit(self.alloc);
                    try self.setSessionRecoveryIndexTxn(&txn, session);
                } else {
                    txn.delete(recovery_key) catch |err| switch (err) {
                        error.NotFound => {},
                        else => return err,
                    };
                }
                try txn.commit();
            },
            .runtime => |store| {
                var txn = try store.beginWrite();
                errdefer txn.abort();
                if (txn.get(session_key) catch |err| switch (err) {
                    error.NotFound => null,
                    else => return err,
                }) |raw| {
                    var session = try decodeSessionRecord(self.alloc, txn_id, raw);
                    defer session.deinit(self.alloc);
                    try self.setSessionRecoveryIndexTxn(&txn, session);
                } else {
                    txn.delete(recovery_key) catch |err| switch (err) {
                        error.NotFound => {},
                        else => return err,
                    };
                }
                try txn.commit();
            },
        }
    }

    pub fn scanExpiredIds(self: *DurableSessionStore, alloc: std.mem.Allocator, cutoff_ns: u64, limit: usize) ![]db_mod.types.TxnId {
        var ids = std.ArrayListUnmanaged(db_mod.types.TxnId).empty;
        errdefer ids.deinit(alloc);
        const Scan = struct {
            allocator: std.mem.Allocator,
            cutoff: u64,
            limit: usize,
            ids: *std.ArrayListUnmanaged(db_mod.types.TxnId),
            fn visit(raw: *anyopaque, key: []const u8, _: []const u8) anyerror!bool {
                const scan: *@This() = @ptrCast(@alignCast(raw));
                const parsed = parseSessionExpiryKey(key) orelse return true;
                if (parsed.timestamp >= scan.cutoff or scan.ids.items.len >= scan.limit) return false;
                try scan.ids.append(scan.allocator, parsed.txn_id);
                return scan.ids.items.len < scan.limit;
            }
        };
        var scan = Scan{ .allocator = alloc, .cutoff = cutoff_ns, .limit = limit, .ids = &ids };
        try self.scanPrefixWithContext(session_expiry_prefix, &scan, Scan.visit);
        return try ids.toOwnedSlice(alloc);
    }

    pub fn scanPrefixWithContext(
        self: *DurableSessionStore,
        prefix: []const u8,
        ctx: *anyopaque,
        callback: *const fn (ctx: *anyopaque, key: []const u8, value: []const u8) anyerror!bool,
    ) !void {
        return try self.scanPrefixFromWithContext(prefix, null, ctx, callback);
    }

    fn scanPrefixFromWithContext(
        self: *DurableSessionStore,
        prefix: []const u8,
        start_after: ?[]const u8,
        ctx: *anyopaque,
        callback: *const fn (ctx: *anyopaque, key: []const u8, value: []const u8) anyerror!bool,
    ) !void {
        switch (self.backend) {
            .docstore => |store| {
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
                var adapter = Adapter{ .context = ctx, .prefix = prefix, .start_after = start_after, .visit = callback };
                try store.scanWithContext(start_after orelse prefix, &.{}, .{}, &adapter, Adapter.run);
            },
            .runtime => |store| {
                var txn = try store.beginCurrentScan();
                defer txn.abort();
                var cursor = try txn.openCursor();
                defer cursor.close();
                var entry = try cursor.seekAtOrAfter(start_after orelse prefix);
                while (entry) |row| : (entry = try cursor.next()) {
                    if (!std.mem.startsWith(u8, row.key, prefix)) break;
                    if (start_after) |start| if (std.mem.eql(u8, row.key, start)) continue;
                    if (!(try callback(ctx, row.key, row.value))) break;
                }
            },
        }
    }

    pub fn sessionCount(self: *DurableSessionStore) !usize {
        return switch (self.backend) {
            .docstore => |store| blk: {
                const Counter = struct {
                    count: usize = 0,
                    fn visit(ctx: ?*anyopaque, key: []const u8, _: []const u8) anyerror!docstore_mod.DocStore.ScanAction {
                        const counter: *@This() = @ptrCast(@alignCast(ctx.?));
                        if (!std.mem.startsWith(u8, key, session_prefix)) return .stop;
                        counter.count += 1;
                        return .@"continue";
                    }
                };
                var counter = Counter{};
                try store.scanWithContext(session_prefix, &.{}, .{}, &counter, Counter.visit);
                break :blk counter.count;
            },
            .runtime => |store| blk: {
                var txn = try store.beginCurrentScan();
                defer txn.abort();
                var cursor = try txn.openCursor();
                defer cursor.close();
                var count: usize = 0;
                var entry = try cursor.seekAtOrAfter(session_prefix);
                while (entry) |row| : (entry = try cursor.next()) {
                    if (!std.mem.startsWith(u8, row.key, session_prefix)) break;
                    count += 1;
                }
                break :blk count;
            },
        };
    }
};

pub const OpenedSessionStore = struct {
    alloc: std.mem.Allocator,
    path_z: [:0]u8,
    docstore: *docstore_mod.DocStore,
    durable: DurableSessionStore,
    lease: SessionLeaseStore,

    pub fn open(alloc: std.mem.Allocator, path: []const u8) !OpenedSessionStore {
        const path_z = try alloc.dupeZ(u8, path);
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
        self.docstore.close();
        self.alloc.destroy(self.docstore);
        self.alloc.free(self.path_z);
        self.* = undefined;
    }

    pub fn durableStore(self: *OpenedSessionStore) *DurableSessionStore {
        return &self.durable;
    }

    pub fn leaseStore(self: *OpenedSessionStore) *SessionLeaseStore {
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

    pub fn init(alloc: std.mem.Allocator, store: *docstore_mod.DocStore) SessionLeaseStore {
        return .{
            .alloc = alloc,
            .backend = .{ .docstore = store },
        };
    }

    pub fn initFromDurable(durable: *DurableSessionStore) SessionLeaseStore {
        return .{ .alloc = durable.alloc, .backend = durable.backend };
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

    pub fn renew(self: *const SessionLeaseStore, txn_id: db_mod.types.TxnId, owner_node_id: u64, now_ms: u64, ttl_ms: u64) !bool {
        const key = try makeSessionLeaseKey(self.alloc, txn_id);
        defer self.alloc.free(key);
        var lease = switch (self.backend) {
            .docstore => |store| try lease_mod.Lease.init(self.alloc, store, key),
            .runtime => |store| try lease_mod.Lease.init(self.alloc, store, key),
        };
        defer lease.deinit();
        const owner_id = try ownerLeaseId(self.alloc, owner_node_id);
        defer self.alloc.free(owner_id);
        return try lease.renew(owner_id, now_ms, ttl_ms);
    }

    pub fn release(self: *const SessionLeaseStore, txn_id: db_mod.types.TxnId, owner_node_id: u64) !bool {
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

pub const SessionRegistry = struct {
    const session_lock_count = 64;

    mutex: AtomicMutex = .{},
    session_locks: [session_lock_count]AtomicMutex = [_]AtomicMutex{.{}} ** session_lock_count,
    // Record locks protect individual durable mutations. Execution ownership
    // spans 2PC and its response handoff, which must not race a local replay.
    commit_locks: [session_lock_count]AtomicMutex = [_]AtomicMutex{.{}} ** session_lock_count,
    sessions: std.AutoHashMapUnmanaged(db_mod.types.TxnId, Session) = .empty,
    durable: ?*DurableSessionStore = null,
    lease_store: ?SessionLeaseStore = null,
    owner_lease_ttl_ns: ?u64 = null,
    max_savepoints: ?usize = null,
    max_sessions: ?usize = null,
    max_record_bytes: ?usize = null,
    durable_scope: SessionStoreScope = .node_local,
    known_durable_session_count: ?usize = null,
    reserved_session_count: usize = 0,
    recovery_index_cursor: ?db_mod.types.TxnId = null,
    recovery_audit_cursor: ?db_mod.types.TxnId = null,
    memory_recovery_scan_offset: usize = 0,

    pub const CommitExecution = struct {
        lock: *AtomicMutex,

        pub fn release(self: CommitExecution) void {
            self.lock.unlock();
        }
    };

    pub fn acquireCommitExecution(self: *SessionRegistry, txn_id: db_mod.types.TxnId, io: std.Io) std.Io.Cancelable!CommitExecution {
        const lock = self.commitLock(txn_id);
        // Execution can yield during network/storage work. A contending HTTP
        // retry must yield through its caller's Io as well, including under
        // cooperative execution, and must remain cancellable.
        while (!lock.inner.tryLock()) try io.sleep(.fromMilliseconds(1), .awake);
        return .{ .lock = lock };
    }

    /// Maintenance skips live requests instead of waiting on their execution.
    /// The durable recovery index remains intact for a later pass or restart.
    pub fn tryAcquireCommitExecution(self: *SessionRegistry, txn_id: db_mod.types.TxnId) ?CommitExecution {
        const lock = self.commitLock(txn_id);
        if (!lock.inner.tryLock()) return null;
        return .{ .lock = lock };
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
        const txn_id = newSessionTxnId(owner_node_id);
        const now = nextTxnTimestamp();
        var session: Session = .{
            .txn_id = txn_id,
            .owner_node_id = owner_node_id,
            .principal = if (principal) |value| try alloc.dupe(u8, value) else null,
            .begin_timestamp = now,
            .last_touched_timestamp = now,
            .sync_level = req.sync_level,
        };
        var session_owned = true;
        errdefer if (session_owned) session.deinit(alloc);
        if (req.sql) |metadata| session.sql = try metadata.clone(alloc);
        try self.initializeDurableSessionCount();
        self.mutex.lock();
        self.ensureSessionCapacityLocked() catch |err| {
            self.mutex.unlock();
            return err;
        };
        self.sessions.ensureUnusedCapacity(alloc, 1) catch |err| {
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
        if (self.durable != null and self.lease_store != null and self.owner_lease_ttl_ns != null) {
            const ttl_ms = @max(@as(u64, 1), self.owner_lease_ttl_ns.? / std.time.ns_per_ms);
            if (!(try self.durable.?.saveWithLease(session, null, now / std.time.ns_per_ms, ttl_ms, true, self.max_record_bytes))) return error.SessionLeaseLost;
        } else {
            try self.persistLocked(session);
            try self.renewLeaseLocked(txn_id, owner_node_id);
        }
        self.mutex.lock();
        defer self.mutex.unlock();
        self.sessions.putAssumeCapacity(txn_id, session);
        session_owned = false;
        self.reserved_session_count -= 1;
        reservation_active = false;
        if (self.known_durable_session_count) |count| self.known_durable_session_count = count + 1;
        return session.info();
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
        const session_lock = self.sessionLock(txn_id);
        session_lock.lock();
        defer session_lock.unlock();
        self.mutex.lock();
        if (self.sessions.getPtr(txn_id)) |existing| {
            const info = existing.info();
            self.mutex.unlock();
            return info;
        }
        self.mutex.unlock();
        const durable = self.durable orelse return null;
        var loaded = (durable.load(txn_id) catch return null) orelse return null;
        defer loaded.deinit(durable.alloc);
        return loaded.info();
    }

    pub const SqlState = struct {
        metadata: SqlMetadata,
        owner_node_id: u64,
        execution_started: bool,
        terminal: ?TerminalCommitStatus,
        savepoints: usize,

        pub fn deinit(self: *SqlState, alloc: std.mem.Allocator) void {
            self.metadata.deinit(alloc);
        }
    };

    pub fn getSqlState(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId) !?SqlState {
        const session_lock = self.sessionLock(txn_id);
        session_lock.lock();
        defer session_lock.unlock();
        self.mutex.lock();
        if (self.sessions.getPtr(txn_id)) |existing| {
            defer self.mutex.unlock();
            const metadata = existing.sql orelse return null;
            return .{ .metadata = try metadata.clone(alloc), .owner_node_id = existing.owner_node_id, .execution_started = existing.commit_execution_started, .terminal = if (existing.terminal_commit) |terminal| terminal.status else null, .savepoints = existing.savepoints.count() };
        }
        self.mutex.unlock();
        const durable = self.durable orelse return null;
        var loaded = (try durable.load(txn_id)) orelse return null;
        defer loaded.deinit(durable.alloc);
        const metadata = loaded.sql orelse return null;
        return .{ .metadata = try metadata.clone(alloc), .owner_node_id = loaded.owner_node_id, .execution_started = loaded.commit_execution_started, .terminal = if (loaded.terminal_commit) |terminal| terminal.status else null, .savepoints = loaded.savepoints.count() };
    }

    /// Read-only SQL statements must not consume an obsolete cached session
    /// after another node adopts its durable owner lease.
    pub fn validateSqlLease(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId, owner: u64) !void {
        const store = self.lease_store orelse return;
        var record = (try store.load(alloc, txn_id)) orelse return error.SessionLeaseLost;
        defer lease_mod.deinitRecord(alloc, &record);
        const expected = try ownerLeaseId(alloc, owner);
        defer alloc.free(expected);
        if (!std.mem.eql(u8, record.owner_id, expected) or record.expires_at_ms <= nextTxnTimestamp() / std.time.ns_per_ms) return error.SessionLeaseLost;
    }

    pub fn setSqlFailed(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId, failed: bool) !void {
        const session_lock = self.sessionLock(txn_id);
        session_lock.lock();
        defer session_lock.unlock();
        var candidate = (try self.loadSessionCloneAssumeStripe(alloc, txn_id)) orelse return error.SqlTransactionNotActive;
        errdefer candidate.deinit(alloc);
        if (candidate.commit_execution_started) return error.SqlTransactionOutcomeUnknown;
        if (candidate.sql) |*metadata| metadata.failed = failed else return error.SqlTransactionNotActive;
        touchSession(&candidate);
        try self.renewLeaseLocked(txn_id, candidate.owner_node_id);
        try self.persistLocked(candidate);
        self.mutex.lock();
        defer self.mutex.unlock();
        const publish_target = self.sessions.getPtr(txn_id) orelse return error.SessionRemovedDuringMutation;
        self.publishCandidateLocked(alloc, publish_target, &candidate);
    }

    pub const StageValidator = struct {
        ptr: *anyopaque,
        validate: *const fn (*anyopaque, std.mem.Allocator, ?*const OwnedTransactionCommitRequest, *OwnedTransactionCommitRequest, *const OwnedTransactionCommitRequest) anyerror!void,
    };

    pub fn stage(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId, req: *const OwnedTransactionCommitRequest) !?SessionInfo {
        return self.stageValidated(alloc, txn_id, req, null);
    }

    pub fn stageSql(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId, req: *const OwnedTransactionCommitRequest) !?SessionInfo {
        return self.stageValidated(alloc, txn_id, req, .{ .ptr = self, .validate = normalizeSqlStage });
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
            target.batch.deinit(alloc);
            target.batch = replacement;
            clearPreparedWrites(target, alloc);
        }
    }

    /// Statement snapshot without sealing COMMIT. Callers serialize the SQL
    /// statement through acquireCommitExecution before reading/staging it.
    pub fn cloneSqlStaged(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId) !OwnedTransactionCommitRequest {
        const session_lock = self.sessionLock(txn_id);
        session_lock.lock();
        defer session_lock.unlock();
        self.mutex.lock();
        const cached = self.sessions.get(txn_id);
        self.mutex.unlock();
        // The per-session stripe protects these owned slices while the map
        // mutex remains free. Savepoint snapshots are not cloned for a SELECT.
        var loaded: ?Session = null;
        defer if (loaded) |*value| value.deinit(self.durable.?.alloc);
        const current = cached orelse blk: {
            const durable = self.durable orelse return error.SqlTransactionNotActive;
            loaded = (try durable.load(txn_id)) orelse return error.SqlTransactionNotActive;
            break :blk loaded.?;
        };
        const metadata = current.sql orelse return error.SqlTransactionNotActive;
        if (metadata.failed) return error.SqlTransactionAborted;
        if (current.commit_body_digest != null) return error.TransactionCommitSealed;
        if (current.staged) |staged| return staged.clone(alloc);
        return .{ .sync_level = current.sync_level };
    }

    pub fn stageValidated(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId, req: *const OwnedTransactionCommitRequest, validator: ?StageValidator) !?SessionInfo {
        const session_lock = self.sessionLock(txn_id);
        session_lock.lock();
        defer session_lock.unlock();

        var candidate = (try self.loadSessionCloneAssumeStripe(alloc, txn_id)) orelse return null;
        errdefer candidate.deinit(alloc);
        if (candidate.sql) |metadata| {
            if (metadata.failed) return error.SqlTransactionAborted;
            if (metadata.mode == .read_only) for (req.tables) |table| {
                if (table.batch.writes.len != 0 or table.batch.deletes.len != 0 or table.batch.transforms.len != 0) return error.SqlReadOnlyTransaction;
            };
        }
        if (candidate.commit_body_digest != null) return error.TransactionCommitSealed;
        var previous = if (validator != null and candidate.staged != null) try candidate.staged.?.clone(alloc) else null;
        defer if (previous) |*value| value.deinit(alloc);
        if (candidate.staged == null) {
            candidate.staged = try req.clone(alloc);
        } else {
            try candidate.staged.?.mergeFrom(alloc, req);
        }
        // Keep only this session's stripe while doing bounded remote reads;
        // the registry mutex is free and durable lease CAS still precedes publish.
        if (validator) |value| try value.validate(value.ptr, alloc, if (previous) |*old| old else null, &candidate.staged.?, req);
        touchSession(&candidate);
        try self.renewLeaseLocked(txn_id, candidate.owner_node_id);
        try self.persistLocked(candidate);

        self.mutex.lock();
        defer self.mutex.unlock();
        const publish_target = self.sessions.getPtr(txn_id) orelse return error.SessionRemovedDuringMutation;
        self.publishCandidateLocked(alloc, publish_target, &candidate);
        return publish_target.info();
    }

    pub fn getReadSnapshot(
        self: *SessionRegistry,
        alloc: std.mem.Allocator,
        txn_id: db_mod.types.TxnId,
        table_name: []const u8,
        key: []const u8,
    ) !?SessionReadSnapshot {
        const session_lock = self.sessionLock(txn_id);
        session_lock.lock();
        defer session_lock.unlock();
        var session = (try self.loadSessionCloneAssumeStripe(alloc, txn_id)) orelse return null;
        defer session.deinit(alloc);
        return try cloneReadSnapshotForKey(alloc, &session.read_snapshots, table_name, key);
    }

    pub fn stageRead(
        self: *SessionRegistry,
        alloc: std.mem.Allocator,
        txn_id: db_mod.types.TxnId,
        req: *const OwnedTransactionCommitRequest,
        snapshot: StageReadSnapshot,
    ) !?SessionInfo {
        const session_lock = self.sessionLock(txn_id);
        session_lock.lock();
        defer session_lock.unlock();

        var candidate = (try self.loadSessionCloneAssumeStripe(alloc, txn_id)) orelse return null;
        errdefer candidate.deinit(alloc);
        if (candidate.commit_body_digest != null) return error.TransactionCommitSealed;
        try upsertReadSnapshot(alloc, &candidate.read_snapshots, snapshot);
        if (candidate.staged == null) {
            candidate.staged = try req.clone(alloc);
        } else {
            try candidate.staged.?.mergeFrom(alloc, req);
        }
        touchSession(&candidate);
        try self.renewLeaseLocked(txn_id, candidate.owner_node_id);
        try self.persistLocked(candidate);

        self.mutex.lock();
        defer self.mutex.unlock();
        const publish_target = self.sessions.getPtr(txn_id) orelse return error.SessionRemovedDuringMutation;
        self.publishCandidateLocked(alloc, publish_target, &candidate);
        return publish_target.info();
    }

    pub fn cloneCommitRequest(
        self: *SessionRegistry,
        alloc: std.mem.Allocator,
        txn_id: db_mod.types.TxnId,
        extra_req: ?*const OwnedTransactionCommitRequest,
    ) !?OwnedTransactionCommitRequest {
        const session_lock = self.sessionLock(txn_id);
        session_lock.lock();
        defer session_lock.unlock();
        var candidate = (try self.loadSessionCloneAssumeStripe(alloc, txn_id)) orelse return null;
        errdefer candidate.deinit(alloc);
        if (candidate.sql) |metadata| {
            if (metadata.failed) return error.SqlTransactionAborted;
            if (metadata.mode == .read_only and extra_req != null) for (extra_req.?.tables) |table| {
                if (table.batch.writes.len != 0 or table.batch.deletes.len != 0 or table.batch.transforms.len != 0) return error.SqlReadOnlyTransaction;
            };
        }
        const body_digest = try commitBodyDigest(alloc, extra_req);
        if (candidate.commit_body_digest) |sealed_digest| {
            if (!std.mem.eql(u8, &sealed_digest, &body_digest)) return error.TransactionCommitRequestMismatch;
            const sealed = candidate.staged orelse return error.InvalidTransactionSessionRecord;
            var out = try sealed.clone(alloc);
            errdefer out.deinit(alloc);
            touchSession(&candidate);
            try self.renewLeaseLocked(txn_id, candidate.owner_node_id);
            try self.persistLocked(candidate);
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
        if (out.tables.len == 0) {
            out.deinit(alloc);
            candidate.deinit(alloc);
            return null;
        }
        if (candidate.staged) |*staged| staged.deinit(alloc);
        candidate.staged = try out.clone(alloc);
        candidate.commit_body_digest = body_digest;
        touchSession(&candidate);
        try self.renewLeaseLocked(txn_id, candidate.owner_node_id);
        try self.persistLocked(candidate);
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
        if ((coordinator_group_id == null) != (coordinator_table_name == null)) return error.InvalidTransactionSessionRecord;
        const session_lock = self.sessionLock(txn_id);
        session_lock.lock();
        defer session_lock.unlock();

        var candidate = (try self.loadSessionCloneAssumeStripe(alloc, txn_id)) orelse return null;
        errdefer candidate.deinit(alloc);
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
        if (candidate.terminal_commit) |*terminal| terminal.deinit(alloc);
        candidate.terminal_commit = null;
        const owned_coordinator_table_name = if (coordinator_table_name) |table_name| try alloc.dupe(u8, table_name) else null;
        candidate.terminal_commit = .{
            .status = status,
            .repair_required = repair_required,
            .coordinator_group_id = coordinator_group_id,
            .coordinator_table_name = owned_coordinator_table_name,
            .coordinator_acknowledged = coordinator_acknowledged,
        };
        touchSession(&candidate);
        try self.renewLeaseLocked(txn_id, candidate.owner_node_id);
        try self.persistLocked(candidate);

        self.mutex.lock();
        defer self.mutex.unlock();
        const publish_target = self.sessions.getPtr(txn_id) orelse return error.SessionRemovedDuringMutation;
        self.publishCandidateLocked(alloc, publish_target, &candidate);
        return {};
    }

    pub fn getExecutionPlan(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId) !?ExecutionPlan {
        const session_lock = self.sessionLock(txn_id);
        session_lock.lock();
        defer session_lock.unlock();
        var candidate = (try self.loadSessionCloneAssumeStripe(alloc, txn_id)) orelse return null;
        defer candidate.deinit(alloc);
        if (candidate.execution_plan) |bytes| return try parseExecutionPlan(alloc, bytes);
        // Older executing document sessions already chose their participant
        // request. Preserve it rather than introduce fresh planning on retry.
        if (!candidate.commit_execution_started) return null;
        var staged = if (candidate.staged) |*value| value else return error.InvalidTransactionSessionRecord;
        const tables = try staged.distributedTables(alloc);
        defer alloc.free(tables);
        const bytes = try encodeExecutionPlan(alloc, tables);
        defer alloc.free(bytes);
        return try parseExecutionPlan(alloc, bytes);
    }

    /// First successful lease-fenced publisher chooses the immutable plan.
    /// Every concurrent HTTP retry receives that same owned plan before 2PC.
    pub fn sealExecutionPlan(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId, tables: []const distributed_txn.TableCommitRequest) !?ExecutionPlan {
        const session_lock = self.sessionLock(txn_id);
        session_lock.lock();
        defer session_lock.unlock();
        var candidate = (try self.loadSessionCloneAssumeStripe(alloc, txn_id)) orelse return null;
        errdefer candidate.deinit(alloc);
        if (candidate.commit_body_digest == null or candidate.staged == null) return error.InvalidTransactionSessionRecord;
        if (candidate.execution_plan) |bytes| {
            const result = try parseExecutionPlan(alloc, bytes);
            candidate.deinit(alloc);
            return result;
        }
        // An old executing record cannot be upgraded to a new plan.
        const selected = if (candidate.commit_execution_started) try candidate.staged.?.distributedTables(alloc) else null;
        defer if (selected) |value| alloc.free(value);
        candidate.execution_plan = try encodeExecutionPlan(alloc, selected orelse tables);
        var result = try parseExecutionPlan(alloc, candidate.execution_plan.?);
        errdefer result.deinit();
        candidate.commit_execution_started = true;
        touchSession(&candidate);
        try self.renewLeaseLocked(txn_id, candidate.owner_node_id);
        try self.persistLocked(candidate);
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
        errdefer candidate.deinit(alloc);
        if (candidate.commit_body_digest == null or candidate.staged == null) return error.InvalidTransactionSessionRecord;
        if (candidate.commit_execution_started) {
            candidate.deinit(alloc);
            return {};
        }
        candidate.commit_execution_started = true;
        touchSession(&candidate);
        try self.renewLeaseLocked(txn_id, candidate.owner_node_id);
        try self.persistLocked(candidate);
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
        const session_lock = self.sessionLock(txn_id);
        session_lock.lock();
        defer session_lock.unlock();

        var candidate = (try self.loadSessionCloneAssumeStripe(alloc, txn_id)) orelse return null;
        errdefer candidate.deinit(alloc);
        const terminal = if (candidate.terminal_commit) |*value| value else return error.InvalidTransactionSessionRecord;
        if (terminal.coordinator_group_id == null) return error.InvalidTransactionSessionRecord;
        if (terminal.coordinator_acknowledged) return {};
        terminal.coordinator_acknowledged = true;
        touchSession(&candidate);
        try self.renewLeaseLocked(txn_id, candidate.owner_node_id);
        try self.persistLocked(candidate);

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
        const session_lock = self.sessionLock(txn_id);
        session_lock.lock();
        defer session_lock.unlock();
        var session = (try self.loadSessionCloneAssumeStripe(alloc, txn_id)) orelse return null;
        defer session.deinit(alloc);
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
            const audit_after = self.recovery_audit_cursor;
            self.mutex.unlock();

            const indexed = try durable.scanRecoveryIds(alloc, index_after, limit);
            defer alloc.free(indexed.ids);
            try pending.appendSlice(alloc, indexed.ids);

            // Continuously audit a small bounded page. This backfills records
            // written by a previous binary and self-heals a missing index key.
            const audit_budget = @min(limit, 8);
            const audited = try durable.scanLegacyRecoveryIds(alloc, audit_after, audit_budget);
            defer alloc.free(audited.ids);
            for (audited.ids) |txn_id| {
                try durable.refreshRecoveryIndex(txn_id);
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
            self.recovery_index_cursor = indexed.next_after;
            self.recovery_audit_cursor = audited.next_after;
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
            (try durable.load(txn_id)) orelse return null
        else blk: {
            self.mutex.lock();
            defer self.mutex.unlock();
            const session = self.sessions.getPtr(txn_id) orelse return null;
            break :blk try session.clone(alloc);
        };
        defer candidate.deinit(if (self.durable) |durable| durable.alloc else alloc);

        if (candidate.owner_node_id != owner_node_id) {
            const durable = self.durable orelse return null;
            if (self.durable_scope != .cluster_shared or self.lease_store == null or self.owner_lease_ttl_ns == null or owner_node_id == 0) return null;
            var adopted = try candidate.clone(alloc);
            var adopted_owned = true;
            defer if (adopted_owned) adopted.deinit(alloc);
            const expected_owner = adopted.owner_node_id;
            adopted.owner_node_id = owner_node_id;
            touchSession(&adopted);
            const ttl_ms = @max(@as(u64, 1), self.owner_lease_ttl_ns.? / std.time.ns_per_ms);
            if (!(try durable.saveWithLease(adopted, expected_owner, now_ns / std.time.ns_per_ms, ttl_ms, true, self.max_record_bytes))) return null;
            candidate.deinit(durable.alloc);
            candidate = try adopted.clone(durable.alloc);
            try self.publishAdoptedCandidateAssumeStripe(alloc, txn_id, &adopted);
            adopted_owned = false;
        } else if (self.lease_store != null and self.owner_lease_ttl_ns != null) {
            try self.renewLeaseLockedAt(txn_id, owner_node_id, now_ns);
        }

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
                const request = candidate.staged orelse return error.InvalidTransactionSessionRecord;
                var cloned_request = try request.clone(alloc);
                errdefer cloned_request.deinit(alloc);
                const cloned_plan = if (candidate.execution_plan) |bytes| try alloc.dupe(u8, bytes) else null;
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
        const request = candidate.staged orelse return error.InvalidTransactionSessionRecord;
        var cloned_request = try request.clone(alloc);
        errdefer cloned_request.deinit(alloc);
        const cloned_plan = if (candidate.execution_plan) |bytes| try alloc.dupe(u8, bytes) else null;
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

    pub fn createSavepoint(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId) !?SavepointInfo {
        return self.createSavepointNamed(alloc, txn_id, null);
    }

    pub fn createNamedSavepoint(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId, name: []const u8) !?SavepointInfo {
        if (name.len == 0 or name.len > 63) return error.InvalidSavepointName;
        return self.createSavepointNamed(alloc, txn_id, name);
    }

    fn createSavepointNamed(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId, name: ?[]const u8) !?SavepointInfo {
        const session_lock = self.sessionLock(txn_id);
        session_lock.lock();
        defer session_lock.unlock();
        var candidate = (try self.loadSessionCloneAssumeStripe(alloc, txn_id)) orelse return null;
        errdefer candidate.deinit(alloc);
        if (candidate.commit_body_digest != null) return error.TransactionCommitSealed;
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
            .snapshot = snapshot,
        };
        var savepoint_inserted = false;
        errdefer if (!savepoint_inserted) new_savepoint.deinit(alloc);
        new_savepoint.name = if (name) |value| try alloc.dupe(u8, value) else null;
        new_savepoint.read_snapshots = try cloneReadSnapshotMap(alloc, candidate.read_snapshots);
        try candidate.savepoints.put(alloc, savepoint_id, new_savepoint);
        savepoint_inserted = true;
        touchSession(&candidate);
        try self.renewLeaseLocked(txn_id, candidate.owner_node_id);
        try self.persistLocked(candidate);
        self.mutex.lock();
        defer self.mutex.unlock();
        const publish_target = self.sessions.getPtr(txn_id) orelse return error.SessionRemovedDuringMutation;
        self.publishCandidateLocked(alloc, publish_target, &candidate);
        return .{ .txn_id = txn_id, .savepoint_id = savepoint_id };
    }

    pub fn rollbackToSavepoint(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId, savepoint_id: u64) !?SavepointInfo {
        return self.changeSavepoint(alloc, txn_id, .{ .id = savepoint_id }, false);
    }

    pub fn releaseSavepoint(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId, savepoint_id: u64) !?SavepointInfo {
        return self.changeSavepoint(alloc, txn_id, .{ .id = savepoint_id }, true);
    }

    pub fn rollbackToNamedSavepoint(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId, name: []const u8) !?SavepointInfo {
        return self.changeSavepoint(alloc, txn_id, .{ .name = name }, false);
    }

    pub fn releaseNamedSavepoint(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId, name: []const u8) !?SavepointInfo {
        return self.changeSavepoint(alloc, txn_id, .{ .name = name }, true);
    }

    fn changeSavepoint(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId, target: union(enum) { id: u64, name: []const u8 }, release: bool) !?SavepointInfo {
        const session_lock = self.sessionLock(txn_id);
        session_lock.lock();
        defer session_lock.unlock();
        var candidate = (try self.loadSessionCloneAssumeStripe(alloc, txn_id)) orelse return null;
        errdefer candidate.deinit(alloc);
        if (candidate.commit_body_digest != null) return error.TransactionCommitSealed;
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
            const read_snapshots = try cloneReadSnapshotMap(alloc, savepoint.read_snapshots);
            deinitReadSnapshotMap(alloc, &candidate.read_snapshots);
            candidate.read_snapshots = read_snapshots;
            if (candidate.sql) |*metadata| metadata.failed = false;
        }
        // Numeric ids define nesting even when SQL names shadow older names.
        // Gather ids before mutation so hash-map iteration cannot be invalidated.
        var removed_ids = std.ArrayList(u64).empty;
        defer removed_ids.deinit(alloc);
        var points = candidate.savepoints.keyIterator();
        while (points.next()) |id| if (id.* > savepoint_id or (release and id.* == savepoint_id)) try removed_ids.append(alloc, id.*);
        for (removed_ids.items) |id| {
            var removed = candidate.savepoints.fetchRemove(id).?.value;
            removed.deinit(alloc);
        }
        touchSession(&candidate);
        try self.renewLeaseLocked(txn_id, candidate.owner_node_id);
        try self.persistLocked(candidate);
        self.mutex.lock();
        defer self.mutex.unlock();
        const publish_target = self.sessions.getPtr(txn_id) orelse return error.SessionRemovedDuringMutation;
        self.publishCandidateLocked(alloc, publish_target, &candidate);
        return .{ .txn_id = txn_id, .savepoint_id = savepoint_id };
    }

    pub fn getStatus(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId) !?SessionStatus {
        const session_lock = self.sessionLock(txn_id);
        session_lock.lock();
        defer session_lock.unlock();
        var session = (try self.loadSessionCloneAssumeStripe(alloc, txn_id)) orelse return null;
        defer session.deinit(alloc);
        return try sessionStatusFromSession(self, alloc, &session);
    }

    pub fn getDetails(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId) !?SessionDetails {
        const session_lock = self.sessionLock(txn_id);
        session_lock.lock();
        defer session_lock.unlock();
        var session = (try self.loadSessionCloneAssumeStripe(alloc, txn_id)) orelse return null;
        defer session.deinit(alloc);
        var details: SessionDetails = .{
            .status = try sessionStatusFromSession(self, alloc, &session),
            .tables = &.{},
            .read_snapshots = &.{},
            .savepoint_ids = &.{},
        };
        errdefer details.deinit(alloc);
        details.tables = try sessionTableDetails(alloc, session.staged);
        details.read_snapshots = try sessionReadSnapshots(alloc, &session);
        details.savepoint_ids = try sessionSavepointIds(alloc, &session);
        if (session.staged) |*staged| {
            var bindings = std.ArrayList(CatalogBinding).empty;
            errdefer {
                for (bindings.items) |binding| {
                    alloc.free(binding.logical);
                    alloc.free(binding.physical);
                }
                bindings.deinit(alloc);
            }
            for (staged.catalog_bindings.items) |binding| {
                const logical = try alloc.dupe(u8, binding.logical);
                errdefer alloc.free(logical);
                const physical = try alloc.dupe(u8, binding.physical);
                errdefer alloc.free(physical);
                try bindings.append(alloc, .{ .logical = logical, .physical = physical });
            }
            details.catalog_bindings = try bindings.toOwnedSlice(alloc);
        }
        return details;
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
                    const counts = stagedCounts(session.staged);
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

    pub fn getOwnerNodeId(self: *SessionRegistry, _: std.mem.Allocator, txn_id: db_mod.types.TxnId) !?u64 {
        const session_lock = self.sessionLock(txn_id);
        session_lock.lock();
        defer session_lock.unlock();
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
        const expected_owner = candidate.owner_node_id;
        candidate.owner_node_id = owner_node_id;
        touchSession(&candidate);
        if (self.lease_store != null and self.owner_lease_ttl_ns != null) {
            const now_ns = nextTxnTimestamp();
            const ttl_ms = @max(@as(u64, 1), self.owner_lease_ttl_ns.? / std.time.ns_per_ms);
            if (!(try durable.saveWithLease(candidate, expected_owner, now_ns / std.time.ns_per_ms, ttl_ms, false, self.max_record_bytes))) return false;
        } else try self.persistLocked(candidate);
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
        const session_lock = self.sessionLock(txn_id);
        session_lock.lock();
        defer session_lock.unlock();
        const durable = self.durable orelse return false;
        if (self.lease_store == null or self.owner_lease_ttl_ns == null) {
            return false;
        }
        // A routing/principal lookup may have cached an older non-owner copy.
        // Adoption must always fence and transfer the latest durable record or
        // it can overwrite staged work that the previous owner published.
        var persisted = (try durable.load(txn_id)) orelse return false;
        defer persisted.deinit(durable.alloc);
        var candidate = try persisted.clone(alloc);
        errdefer candidate.deinit(alloc);
        if (candidate.owner_node_id == owner_node_id) {
            try self.publishAdoptedCandidateAssumeStripe(alloc, txn_id, &candidate);
            return true;
        }
        const expected_owner = candidate.owner_node_id;
        const effective_now = now_ns orelse nextTxnTimestamp();
        candidate.owner_node_id = owner_node_id;
        touchSession(&candidate);
        const ttl_ms = @max(@as(u64, 1), self.owner_lease_ttl_ns.? / std.time.ns_per_ms);
        if (!(try durable.saveWithLease(candidate, expected_owner, effective_now / std.time.ns_per_ms, ttl_ms, true, self.max_record_bytes))) return false;
        try self.publishAdoptedCandidateAssumeStripe(alloc, txn_id, &candidate);
        return true;
    }

    pub fn cleanupExpired(self: *SessionRegistry, alloc: std.mem.Allocator, cutoff_ns: u64) !usize {
        var expired_ids = std.ArrayListUnmanaged(db_mod.types.TxnId).empty;
        defer expired_ids.deinit(alloc);
        if (self.durable) |durable| {
            const indexed = try durable.scanExpiredIds(alloc, cutoff_ns, 1024);
            defer alloc.free(indexed);
            try expired_ids.appendSlice(alloc, indexed);
        } else {
            self.mutex.lock();
            var loaded_it = self.sessions.iterator();
            while (loaded_it.next()) |entry| {
                if (entry.value_ptr.last_touched_timestamp < cutoff_ns) expired_ids.append(alloc, entry.key_ptr.*) catch |err| {
                    self.mutex.unlock();
                    return err;
                };
                if (expired_ids.items.len >= 1024) break;
            }
            self.mutex.unlock();
        }

        var removed_count: usize = 0;
        for (expired_ids.items) |txn_id| {
            const session_lock = self.sessionLock(txn_id);
            session_lock.lock();
            defer session_lock.unlock();
            var current = (try self.loadSessionCloneAssumeStripe(alloc, txn_id)) orelse continue;
            defer current.deinit(alloc);
            if (current.last_touched_timestamp >= cutoff_ns) continue;
            if (current.terminal_commit) |terminal| {
                if (terminal.coordinator_group_id != null and !terminal.coordinator_acknowledged) continue;
            }
            try self.deletePersistent(txn_id);
            self.releaseLease(txn_id, current.owner_node_id) catch {};
            self.mutex.lock();
            if (self.sessions.fetchRemove(txn_id)) |removed| {
                var session = removed.value;
                session.deinit(alloc);
                removed_count += 1;
            }
            self.mutex.unlock();
        }
        return removed_count;
    }

    pub fn remove(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId) bool {
        return self.removeMode(alloc, txn_id, false);
    }

    /// Preflight failure may race another commit retry. Never delete its
    /// durable decision/recovery handoff based on an earlier missing-plan read.
    pub fn removeBeforeExecution(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId) bool {
        return self.removeMode(alloc, txn_id, true);
    }

    fn removeMode(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId, before_execution_only: bool) bool {
        const session_lock = self.sessionLock(txn_id);
        session_lock.lock();
        defer session_lock.unlock();
        var current = (self.loadSessionCloneAssumeStripe(alloc, txn_id) catch return false) orelse return false;
        defer current.deinit(alloc);
        if (before_execution_only and (current.commit_execution_started or current.terminal_commit != null)) return false;
        self.deletePersistent(txn_id) catch return false;
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
        self.sessions.put(alloc, txn_id, candidate.*) catch |err| {
            self.mutex.unlock();
            return err;
        };
        candidate.* = undefined;
        self.mutex.unlock();
    }

    fn persistLocked(self: *SessionRegistry, session: Session) !void {
        if (self.durable) |durable| try durable.save(session, self.max_record_bytes);
    }

    fn deletePersistent(self: *SessionRegistry, txn_id: db_mod.types.TxnId) !void {
        if (self.durable) |durable| {
            try durable.delete(txn_id);
            self.mutex.lock();
            defer self.mutex.unlock();
            if (self.known_durable_session_count) |count| self.known_durable_session_count = count -| 1;
        }
    }

    fn ensureSessionCapacityLocked(self: *SessionRegistry) !void {
        const limit = self.max_sessions orelse return;
        const count = if (self.durable != null) self.known_durable_session_count orelse return error.SessionCapacityUnavailable else self.sessions.count();
        if (count + self.reserved_session_count >= limit) return error.SessionLimitExceeded;
    }

    fn initializeDurableSessionCount(self: *SessionRegistry) !void {
        const durable = self.durable orelse return;
        self.mutex.lock();
        if (self.known_durable_session_count != null) {
            self.mutex.unlock();
            return;
        }
        self.mutex.unlock();
        const count = try durable.sessionCount();
        self.mutex.lock();
        if (self.known_durable_session_count == null) self.known_durable_session_count = count;
        self.mutex.unlock();
    }

    /// The caller holds the txn stripe. Durable reads happen without the global
    /// registry mutex; publication is a short double-checked map operation.
    fn loadSessionCloneAssumeStripe(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId) !?Session {
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
        var loaded = (try durable.load(txn_id)) orelse return null;
        var loaded_owned = true;
        errdefer if (loaded_owned) loaded.deinit(durable.alloc);
        self.mutex.lock();
        defer self.mutex.unlock();
        if (self.sessions.getPtr(txn_id)) |session| {
            loaded.deinit(durable.alloc);
            loaded_owned = false;
            return try session.clone(alloc);
        }
        try self.sessions.put(alloc, txn_id, loaded);
        loaded_owned = false;
        return try self.sessions.getPtr(txn_id).?.clone(alloc);
    }

    fn renewLeaseLocked(self: *SessionRegistry, txn_id: db_mod.types.TxnId, owner_node_id: u64) !void {
        const now_ns = nextTxnTimestamp();
        try self.renewLeaseLockedAt(txn_id, owner_node_id, now_ns);
    }

    pub fn renewOwnedLeases(self: *SessionRegistry, owner_node_id: u64, now_ns: u64) !usize {
        var ids = std.ArrayListUnmanaged(db_mod.types.TxnId).empty;
        defer ids.deinit(self.durable.?.alloc);
        self.mutex.lock();
        if (self.lease_store == null or self.owner_lease_ttl_ns == null or self.durable == null) {
            self.mutex.unlock();
            return 0;
        }
        var it = self.sessions.iterator();
        while (it.next()) |entry| {
            if (entry.value_ptr.owner_node_id != owner_node_id) continue;
            ids.append(self.durable.?.alloc, entry.key_ptr.*) catch |err| {
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
            const still_owned = if (self.sessions.getPtr(txn_id)) |session| session.owner_node_id == owner_node_id else false;
            self.mutex.unlock();
            if (!still_owned) continue;
            try self.renewLeaseLockedAt(txn_id, owner_node_id, now_ns);
            renewed += 1;
        }
        return renewed;
    }

    fn renewLeaseLockedAt(self: *SessionRegistry, txn_id: db_mod.types.TxnId, owner_node_id: u64, now_ns: u64) !void {
        const lease_store = self.lease_store orelse return;
        const ttl_ns = self.owner_lease_ttl_ns orelse return;
        const now_ms = now_ns / std.time.ns_per_ms;
        const ttl_ms = @max(@as(u64, 1), ttl_ns / std.time.ns_per_ms);
        if (!(try lease_store.renew(txn_id, owner_node_id, now_ms, ttl_ms))) return error.SessionLeaseLost;
    }

    fn releaseLease(self: *SessionRegistry, txn_id: db_mod.types.TxnId, owner_node_id: u64) !void {
        const lease_store = self.lease_store orelse return;
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
            !std.mem.eql(u8, field.key_ptr.*, "sync_level"))
        {
            return error.InvalidTransactionCommitRequest;
        }
    }

    var req: OwnedTransactionCommitRequest = .{};
    errdefer req.deinit(alloc);
    req.tables = try parseTables(alloc, root.get("tables") orelse return error.InvalidTransactionCommitRequest);
    if (req.tables.len == 0) return error.InvalidTransactionCommitRequest;
    if (root.get("sync_level")) |sync_level_value| {
        req.sync_level = parseSyncLevel(sync_level_value) orelse return error.InvalidTransactionCommitRequest;
    } else {
        for (req.tables) |table| {
            if (@intFromEnum(table.batch.req.sync_level) > @intFromEnum(req.sync_level)) {
                req.sync_level = table.batch.req.sync_level;
            }
        }
    }
    var operation_count: usize = 0;
    for (req.tables) |table| operation_count += table.batch.writes.len + table.batch.deletes.len + table.batch.transforms.len;
    if (operation_count == 0) return error.InvalidTransactionCommitRequest;
    return req;
}

pub fn encodeCommitRequest(alloc: std.mem.Allocator, req: OwnedTransactionCommitRequest) ![]u8 {
    return encodeCommitRequestMode(alloc, req, false);
}
fn encodeCommitRequestMode(alloc: std.mem.Allocator, req: OwnedTransactionCommitRequest, trusted: bool) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
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
        const batch_json = try encodeTableBatchRequest(alloc, table);
        defer alloc.free(batch_json);
        try out.appendSlice(alloc, batch_json);
    }
    try out.append(alloc, '}');
    try out.appendSlice(alloc, ",\"sync_level\":");
    try appendJsonString(alloc, &out, syncLevelText(req.sync_level));
    if (trusted and req.constraint_timing.items.len != 0) {
        try out.appendSlice(alloc, ",\"constraint_timing\":");
        const timing = try std.json.Stringify.valueAlloc(alloc, req.constraint_timing.items, .{});
        defer alloc.free(timing);
        try out.appendSlice(alloc, timing);
    }
    if (trusted and req.catalog_bindings.items.len != 0) {
        try out.appendSlice(alloc, ",\"catalog_bindings\":");
        const bindings = try std.json.Stringify.valueAlloc(alloc, req.catalog_bindings.items, .{});
        defer alloc.free(bindings);
        try out.appendSlice(alloc, bindings);
    }
    if (trusted) {
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
                const encoded = try std.json.Stringify.valueAlloc(alloc, .{
                    .key = predicate.key,
                    .version = try std.fmt.bufPrint(&version_buf, "{d}", .{predicate.expected_version}),
                    .digest = predicate.expected_content_digest,
                }, .{});
                defer alloc.free(encoded);
                try out.appendSlice(alloc, encoded);
            }
            try out.append(alloc, ']');
        }
        try out.append(alloc, '}');
    }
    try out.append(alloc, '}');
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

fn parseStoredCommitValue(alloc: std.mem.Allocator, value: std.json.Value) !OwnedTransactionCommitRequest {
    var request = try parseCommitValue(alloc, value);
    errdefer request.deinit(alloc);
    if (value.object.get("constraint_timing")) |timing| {
        if (timing != .array or timing.array.items.len > 4096) return error.InvalidTransactionSessionRecord;
        var parsed = try std.json.parseFromValue([]const @import("../storage/relational_index.zig").ConstraintTiming, alloc, timing, .{});
        defer parsed.deinit();
        for (parsed.value) |mode| try request.setConstraintTiming(alloc, mode);
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
    if (value.object.get("catalog_bindings")) |bindings| {
        if (bindings != .array) return error.InvalidTransactionSessionRecord;
        for (bindings.array.items) |binding| {
            if (binding != .object) return error.InvalidTransactionSessionRecord;
            const logical = requireString(binding.object, "logical");
            const physical = requireString(binding.object, "physical");
            if (logical.len == 0 or physical.len == 0) return error.InvalidTransactionSessionRecord;
            try request.bind(alloc, logical, physical);
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
            var parsed = try std.json.parseFromValue([]const @import("range_read_guards.zig").OwnerRangeProof, alloc, entry.value_ptr.*, .{ .allocate = .alloc_always });
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
            const Stored = struct { key: []const u8, version: []const u8, digest: ?[32]u8 };
            var parsed = try std.json.parseFromValue([]const Stored, alloc, entry.value_ptr.*, .{});
            defer parsed.deinit();
            const predicates = try alloc.alloc(db_mod.types.TransactionVersionPredicate, parsed.value.len);
            defer alloc.free(predicates);
            for (predicates, parsed.value) |*predicate, stored| predicate.* = .{
                .key = stored.key,
                .expected_version = try parseVersionString(stored.version),
                .expected_content_digest = stored.digest,
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

    var req: OwnedTransactionCommitRequest = .{};
    errdefer req.deinit(alloc);

    const read_set_value = root.get("read_set") orelse return error.InvalidTransactionCommitRequest;
    req.read_set = try parseReadSet(alloc, read_set_value);

    const tables_value = root.get("tables") orelse return error.InvalidTransactionCommitRequest;
    req.tables = try parseTables(alloc, tables_value);

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
    errdefer out.deinit(alloc);
    try appendBatchWrites(alloc, &out, batch.writes);
    try appendBatchDeletes(alloc, &out, batch.deletes);
    try appendBatchTransforms(alloc, &out, batch.transforms);
    syncBatchReq(&out);
    return out;
}

fn syncBatchReq(batch: *batch_api.OwnedBatchRequest) void {
    batch.req = .{
        .writes = batch.writes,
        .deletes = batch.deletes,
        .transforms = batch.transforms,
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

fn clearPreparedWrites(table: *TableCommitRequest, alloc: std.mem.Allocator) void {
    if (table.txn_writes.len > 0) {
        alloc.free(table.txn_writes);
        table.txn_writes = &.{};
    }
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
    return .{ .key = key, .value = value, .json_null_fields = try cloneJsonNullFields(alloc, write.json_null_fields) };
}

fn appendBatchWrites(alloc: std.mem.Allocator, batch: *batch_api.OwnedBatchRequest, writes: []const db_mod.types.BatchWrite) !void {
    if (writes.len == 0) return;
    const old_len = batch.writes.len;
    var next = try alloc.alloc(db_mod.types.BatchWrite, old_len + writes.len);
    var copied: usize = 0;
    errdefer {
        for (next[0..copied]) |write| {
            alloc.free(@constCast(write.key));
            alloc.free(@constCast(write.value));
            freeJsonNullFields(alloc, write.json_null_fields);
        }
        alloc.free(next);
    }
    for (batch.writes) |write| {
        next[copied] = try cloneBatchWrite(alloc, write);
        copied += 1;
    }
    for (writes) |write| {
        next[copied] = try cloneBatchWrite(alloc, write);
        copied += 1;
    }
    for (batch.writes) |write| {
        alloc.free(@constCast(write.key));
        alloc.free(@constCast(write.value));
        freeJsonNullFields(alloc, write.json_null_fields);
    }
    if (batch.writes.len > 0) alloc.free(batch.writes);
    batch.writes = next;
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
            continue;
        }
        predicates.appendAssumeCapacity(.{
            .key = try alloc.dupe(u8, predicate.key),
            .expected_version = predicate.expected_version,
            .expected_content_digest = predicate.expected_content_digest,
        });
        by_key.putAssumeCapacity(predicates.items[predicates.items.len - 1].key, predicates.items.len - 1);
    }
}

fn syncTableBatch(table: *TableCommitRequest) void {
    syncBatchReq(&table.batch);
}

fn syncAndClear(table: *TableCommitRequest, alloc: std.mem.Allocator) void {
    clearPreparedWrites(table, alloc);
    syncTableBatch(table);
}

pub fn isEmptySessionCommitBody(body: []const u8) bool {
    const trimmed = std.mem.trim(u8, body, &std.ascii.whitespace);
    return trimmed.len == 0 or std.mem.eql(u8, trimmed, "{}");
}

fn parseTableBatch(alloc: std.mem.Allocator, value: std.json.Value) !batch_api.OwnedBatchRequest {
    const encoded = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})});
    defer alloc.free(encoded);
    return try batch_api.parseBatchRequest(alloc, encoded);
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
    session.last_touched_timestamp = nextTxnTimestamp();
}

fn stagedCounts(staged: ?OwnedTransactionCommitRequest) struct { tables: usize, reads: usize, writes: usize, deletes: usize } {
    if (staged) |req| {
        var write_count: usize = 0;
        var delete_count: usize = 0;
        for (req.tables) |table| {
            write_count += table.batch.writes.len;
            delete_count += table.batch.deletes.len;
        }
        return .{
            .tables = req.tables.len,
            .reads = req.read_set.len,
            .writes = write_count,
            .deletes = delete_count,
        };
    }
    return .{ .tables = 0, .reads = 0, .writes = 0, .deletes = 0 };
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
    const counts = stagedCounts(session.staged);
    const savepoint_count = session.savepoints.count();
    return .{
        .txn_id = session.txn_id,
        .owner_node_id = session.owner_node_id,
        .begin_timestamp = session.begin_timestamp,
        .last_touched_timestamp = session.last_touched_timestamp,
        .lease_expires_at = try self.loadLeaseExpiryLocked(alloc, session.txn_id),
        .sync_level = session.sync_level,
        .staged_table_count = counts.tables,
        .staged_read_count = counts.reads,
        .staged_write_count = counts.writes,
        .staged_delete_count = counts.deletes,
        .read_snapshot_count = session.read_snapshots.count(),
        .savepoint_count = savepoint_count,
        .savepoint_limit = self.max_savepoints,
        .remaining_savepoints = if (self.max_savepoints) |limit| limit - @min(limit, savepoint_count) else null,
        .durable = self.durable != null,
    };
}

fn sessionReadSnapshots(alloc: std.mem.Allocator, session: *const Session) ![]SessionReadSnapshot {
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
    std.sort.pdq(SessionReadSnapshot, out, {}, struct {
        fn lessThan(_: void, a: SessionReadSnapshot, b: SessionReadSnapshot) bool {
            if (std.mem.order(u8, a.table_name, b.table_name) == .lt) return true;
            if (std.mem.eql(u8, a.table_name, b.table_name)) return std.mem.lessThan(u8, a.key, b.key);
            return false;
        }
    }.lessThan);
    return out;
}

fn sessionTableDetails(alloc: std.mem.Allocator, staged: ?OwnedTransactionCommitRequest) ![]SessionTableDetail {
    const req = staged orelse return &.{};
    var map = std.StringArrayHashMapUnmanaged(SessionTableDetail).empty;
    errdefer {
        var it = map.iterator();
        while (it.next()) |entry| {
            alloc.free(@constCast(entry.key_ptr.*));
            entry.value_ptr.deinit(alloc);
        }
        map.deinit(alloc);
    }

    for (req.read_set) |read| {
        const gop = try map.getOrPut(alloc, read.table_name);
        if (!gop.found_existing) {
            gop.key_ptr.* = try alloc.dupe(u8, read.table_name);
            gop.value_ptr.* = .{
                .table_name = try alloc.dupe(u8, read.table_name),
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
            gop.key_ptr.* = try alloc.dupe(u8, table.table_name);
            gop.value_ptr.* = .{
                .table_name = try alloc.dupe(u8, table.table_name),
                .staged_read_count = 0,
                .staged_write_count = 0,
                .staged_delete_count = 0,
                .staged_predicate_count = 0,
            };
        }
        gop.value_ptr.staged_write_count += table.batch.writes.len;
        gop.value_ptr.staged_delete_count += table.batch.deletes.len;
        gop.value_ptr.staged_predicate_count += table.predicates.items.len;
    }

    var out = try alloc.alloc(SessionTableDetail, map.count());
    var i: usize = 0;
    var it = map.iterator();
    while (it.next()) |entry| {
        out[i] = entry.value_ptr.*;
        alloc.free(@constCast(entry.key_ptr.*));
        i += 1;
    }
    map.deinit(alloc);
    std.sort.pdq(SessionTableDetail, out, {}, struct {
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

fn makeSessionKey(alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId) ![]u8 {
    const txn_hex = distributed_txn.encodeTxnIdHex(txn_id);
    return try std.fmt.allocPrint(alloc, "{s}{s}", .{ session_prefix, &txn_hex });
}

fn makeSessionLeaseKey(alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId) ![]u8 {
    const txn_hex = distributed_txn.encodeTxnIdHex(txn_id);
    return try std.fmt.allocPrint(alloc, "{s}{s}", .{ session_lease_prefix, &txn_hex });
}

fn makeSessionExpiryKey(alloc: std.mem.Allocator, timestamp: u64, txn_id: db_mod.types.TxnId) ![]u8 {
    const txn_hex = distributed_txn.encodeTxnIdHex(txn_id);
    return try std.fmt.allocPrint(alloc, "{s}{x:0>16}:{s}", .{ session_expiry_prefix, timestamp, &txn_hex });
}

fn makeSessionRecoveryKey(alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId) ![]u8 {
    const txn_hex = distributed_txn.encodeTxnIdHex(txn_id);
    return try std.fmt.allocPrint(alloc, "{s}{s}", .{ session_recovery_prefix, &txn_hex });
}

fn sessionNeedsRecovery(session: Session) bool {
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

fn parseSessionExpiryKey(key: []const u8) ?ParsedSessionExpiryKey {
    if (!std.mem.startsWith(u8, key, session_expiry_prefix)) return null;
    const suffix = key[session_expiry_prefix.len..];
    if (suffix.len < 18 or suffix[16] != ':') return null;
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

fn encodeSessionRecord(alloc: std.mem.Allocator, session: Session) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.appendSlice(alloc, "{\"owner_node_id\":");
    try out.print(alloc, "{d}", .{session.owner_node_id});
    try out.appendSlice(alloc, ",\"principal\":");
    if (session.principal) |principal| {
        try appendJsonString(alloc, &out, principal);
    } else {
        try out.appendSlice(alloc, "null");
    }
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
    try out.appendSlice(alloc, ",\"execution_plan\":");
    if (session.execution_plan) |bytes| try appendJsonString(alloc, &out, bytes) else try out.appendSlice(alloc, "null");
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
        try out.append(alloc, '}');
    } else {
        try out.appendSlice(alloc, "null");
    }
    try out.appendSlice(alloc, ",\"read_snapshots\":[");
    var snapshots_it = session.read_snapshots.iterator();
    var first_snapshot = true;
    while (snapshots_it.next()) |entry| {
        if (!first_snapshot) try out.append(alloc, ',');
        first_snapshot = false;
        try appendReadSnapshotJson(alloc, &out, entry.value_ptr.*);
    }
    try out.append(alloc, ']');
    try out.appendSlice(alloc, ",\"savepoints\":[");
    var it = session.savepoints.iterator();
    var first = true;
    while (it.next()) |entry| {
        if (!first) try out.append(alloc, ',');
        first = false;
        try out.appendSlice(alloc, "{\"id\":");
        try out.print(alloc, "{d}", .{entry.key_ptr.*});
        try out.appendSlice(alloc, ",\"name\":");
        if (entry.value_ptr.name) |name| try appendJsonString(alloc, &out, name) else try out.appendSlice(alloc, "null");
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
        try out.append(alloc, '}');
    }
    try out.appendSlice(alloc, "]}");
    return try out.toOwnedSlice(alloc);
}

fn decodeSessionRecord(alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId, body: []const u8) !Session {
    // Staged rows, read snapshots, and savepoints share this durable envelope.
    // Do not convert any user number until its schema is available.
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{ .parse_numbers = false });
    defer parsed.deinit();
    const obj = switch (parsed.value) {
        .object => |obj| obj,
        else => return error.InvalidTransactionSessionRecord,
    };
    var session: Session = .{
        .txn_id = txn_id,
        .owner_node_id = if (obj.get("owner_node_id")) |value|
            switch (value) {
                .integer => |v| try nonNegativeRecordInteger(v),
                .number_string => |text| try recordNumber(text),
                else => return error.InvalidTransactionSessionRecord,
            }
        else
            sessionOwnerNodeId(txn_id),
        .principal = if (obj.get("principal")) |value|
            switch (value) {
                .string => |principal| try alloc.dupe(u8, principal),
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
    if (obj.get("sql")) |metadata| {
        if (metadata != .null) {
            const decoded = std.json.parseFromValue(SqlMetadata, alloc, metadata, .{}) catch return error.InvalidTransactionSessionRecord;
            defer decoded.deinit();
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
    if (obj.get("execution_plan")) |value| switch (value) {
        .null => {},
        .string => |bytes| {
            if (!session.commit_execution_started or session.commit_body_digest == null) return error.InvalidTransactionSessionRecord;
            var plan = try parseExecutionPlan(alloc, bytes);
            defer plan.deinit();
            session.execution_plan = try alloc.dupe(u8, bytes);
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
            };
        }
    }
    if (obj.get("read_snapshots")) |snapshots_value| {
        try decodeReadSnapshotsInto(alloc, snapshots_value, &session.read_snapshots);
    }
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
        const snapshot = try parseStoredCommitValue(alloc, entry_obj.get("snapshot") orelse return error.InvalidTransactionSessionRecord);
        var owned_snapshot = snapshot;
        errdefer owned_snapshot.deinit(alloc);
        const name: ?[]u8 = if (entry_obj.get("name")) |value| switch (value) {
            .null => null,
            .string => |text| if (text.len > 0 and text.len <= 63) try alloc.dupe(u8, text) else return error.InvalidTransactionSessionRecord,
            else => return error.InvalidTransactionSessionRecord,
        } else null;
        errdefer if (name) |text| alloc.free(text);
        var read_snapshots: std.StringArrayHashMapUnmanaged(SessionReadSnapshot) = .empty;
        errdefer deinitReadSnapshotMap(alloc, &read_snapshots);
        if (entry_obj.get("read_snapshots")) |read_snapshots_value| {
            try decodeReadSnapshotsInto(alloc, read_snapshots_value, &read_snapshots);
        }
        try session.savepoints.put(alloc, id, .{
            .id = id,
            .name = name,
            .snapshot = snapshot,
            .read_snapshots = read_snapshots,
        });
    }
    return session;
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

fn newSessionTxnId(owner_node_id: u64) db_mod.types.TxnId {
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
    try std.testing.expectEqual(@as(u64, 7), sessionOwnerNodeId(session.txn_id));
    try std.testing.expect(registry.remove(std.testing.allocator, session.txn_id));
    try std.testing.expect(registry.getInfo(session.txn_id) == null);
}

test "durable transaction sessions preserve and enforce principal bindings" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/txn-session-principal", .{tmp.sub_path});
    defer std.testing.allocator.free(path);
    const path_z = try std.testing.allocator.dupeZ(u8, path);
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
        registry.stage(std.testing.allocator, session.txn_id, &stage_req),
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
    const path_z = try std.testing.allocator.dupeZ(u8, path);
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
    const commands = [_]@import("../storage/db/relational_integrity_contract.zig").Command{.{
        .address = try @import("../storage/db/relational_integrity_contract.zig").Address.init(@splat(2), "\x00\xfftuple"),
        .operation = .{ .check_owner = .{ .parent_table = "docs", .parent_key = "\x00\xffa" } },
    }};
    const tables = [_]distributed_txn.TableCommitRequest{.{ .table_name = "docs", .relational_schema_version = 3, .relational_integrity_generation_set = @splat(0xff), .predicates = &.{.{ .key = "a", .expected_version = 7 }}, .integrity_commands = &commands }};
    durable.fail_writes_for_test = true;
    try std.testing.expectError(error.InjectedSessionStoreFailure, writer.sealExecutionPlan(alloc, session.txn_id, &tables));
    try std.testing.expect((try writer.getExecutionPlan(alloc, session.txn_id)) == null);
    durable.fail_writes_for_test = false;
    var first = (try writer.sealExecutionPlan(alloc, session.txn_id, &tables)).?;
    defer first.deinit();
    // A retry that read different metadata cannot redirect the chosen plan.
    var retry = (try writer.sealExecutionPlan(alloc, session.txn_id, &.{.{ .table_name = "wrong" }})).?;
    defer retry.deinit();
    try std.testing.expectEqualStrings("docs", retry.value[0].table_name);
    // A stale preflight on a concurrent HTTP retry may not erase the plan.
    try std.testing.expect(!writer.removeBeforeExecution(alloc, session.txn_id));
    var reopened = SessionRegistry.init(&durable);
    defer reopened.deinit(alloc);
    var recovery = (try reopened.claimPendingRecovery(alloc, session.txn_id, 9, nextTxnTimestamp())).?;
    defer recovery.deinit(alloc);
    var decoded = try parseExecutionPlan(alloc, recovery.commit.execution_plan.?);
    defer decoded.deinit();
    try std.testing.expectEqual(@as(?u32, 3), decoded.value[0].relational_schema_version);
    try std.testing.expectEqual(@as(u64, 7), decoded.value[0].predicates[0].expected_version);
    try std.testing.expectEqualStrings("\x00\xffa", decoded.value[0].integrity_commands[0].operation.check_owner.parent_key);
    try std.testing.expectEqualSlices(u8, &commands[0].address.routing, &decoded.value[0].integrity_commands[0].address.routing);
    // Public retry identity and original read dependencies remain separate.
    try std.testing.expectEqual(@as(u64, 7), recovery.commit.request.read_set[0].expected_version);
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
    var request = try parseCommitRequest(alloc,
        \\{"read_set":[],"execution_plan":"forged","commit_execution_started":true,"tables":{"docs":{"inserts":{"a":{"id":1}},"relational_integrity_generation_set":[255],"integrity_commands":[]}}}
    );
    defer request.deinit(alloc);
    const tables = try request.distributedTables(alloc);
    defer alloc.free(tables);
    try std.testing.expect(tables[0].relational_integrity_generation_set == null);
    try std.testing.expectEqual(@as(usize, 0), tables[0].integrity_commands.len);
    var registry = SessionRegistry.init(null);
    defer registry.deinit(alloc);
    const session = try registry.begin(alloc, .{}, 9);
    var sealed = (try registry.cloneCommitRequest(alloc, session.txn_id, &request)).?;
    defer sealed.deinit(alloc);
    try std.testing.expect((try registry.getExecutionPlan(alloc, session.txn_id)) == null);
}

test "distributed txn stage validation rejects atomically and preserves prior savepoints" {
    const alloc = std.testing.allocator;
    var registry = SessionRegistry.init(null);
    defer registry.deinit(alloc);
    const session = try registry.begin(alloc, .{}, 9);
    var request = try parseStageWriteRequest(alloc, "{\"table\":\"docs\",\"key\":\"a\",\"document\":{\"id\":1}}");
    defer request.deinit(alloc);
    _ = try registry.stage(alloc, session.txn_id, &request);
    _ = try registry.createSavepoint(alloc, session.txn_id);
    var calls: usize = 0;
    const Validator = struct {
        fn validate(ptr: *anyopaque, _: std.mem.Allocator, previous: ?*const OwnedTransactionCommitRequest, candidate: *OwnedTransactionCommitRequest, _: *const OwnedTransactionCommitRequest) !void {
            const count: *usize = @ptrCast(@alignCast(ptr));
            count.* += 1;
            try std.testing.expectEqual(@as(usize, 1), previous.?.tables[0].batch.writes.len);
            try std.testing.expectEqual(@as(usize, 2), candidate.tables[0].batch.writes.len);
            return error.ForeignKeyParentMissing;
        }
    };
    try std.testing.expectError(error.ForeignKeyParentMissing, registry.stageValidated(alloc, session.txn_id, &request, .{ .ptr = &calls, .validate = Validator.validate }));
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
    _ = try registry.begin(std.testing.allocator, .{ .sync_level = .write }, 1);
    try std.testing.expectError(
        error.SessionLimitExceeded,
        registry.begin(std.testing.allocator, .{ .sync_level = .write }, 1),
    );

    var small_registry = SessionRegistry.initWithOptions(&durable, null, null, null, null, 8);
    defer small_registry.deinit(std.testing.allocator);
    try std.testing.expectError(
        error.SessionRecordTooLarge,
        small_registry.begin(std.testing.allocator, .{ .sync_level = .write }, 2),
    );
    try std.testing.expectEqual(@as(usize, 0), small_registry.sessions.count());
}

test "transaction session registry adopts durable session ownership" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/txn-session-adopt-store", .{tmp.sub_path});
    defer std.testing.allocator.free(path);
    const path_z = try std.testing.allocator.dupeZ(u8, path);
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
    const path_z = try alloc.dupeZ(u8, path);
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
        reader.stage(alloc, txn_id, &changed_body),
    );
}

test "durable recovery index tracks only validated commit execution and terminal handoff" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/txn-session-recovery-index", .{tmp.sub_path});
    defer alloc.free(path);
    const path_z = try alloc.dupeZ(u8, path);
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
    const path_z = try alloc.dupeZ(u8, path);
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

test "background recovery adopts an expired shared-store owner lease" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/txn-session-recovery-adopt", .{tmp.sub_path});
    defer alloc.free(path);
    const path_z = try alloc.dupeZ(u8, path);
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
    const path_z = try std.testing.allocator.dupeZ(u8, path);
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
    const path_z = try std.testing.allocator.dupeZ(u8, path);
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
    _ = try writer.stage(std.testing.allocator, session.txn_id, &stage_req);

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
    const path_z = try std.testing.allocator.dupeZ(u8, path);
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
    const path_z = try std.testing.allocator.dupeZ(u8, path);
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
    _ = (try registry.createSavepoint(std.testing.allocator, session.txn_id)) orelse return error.TestExpectedEqual;

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
    const path_z = try std.testing.allocator.dupeZ(u8, path);
    defer std.testing.allocator.free(path_z);

    var store = try docstore_mod.DocStore.open(std.testing.allocator, path_z, .{});
    defer store.close();
    var durable = DurableSessionStore.init(std.testing.allocator, &store);

    var writer = SessionRegistry.init(&durable);
    defer writer.deinit(std.testing.allocator);
    const session = try writer.begin(std.testing.allocator, .{ .sync_level = .write }, 9);

    var stage_req = try parseStageWriteRequest(std.testing.allocator, "{\"table\":\"docs\",\"key\":\"doc:a\",\"document\":{\"title\":\"persisted\"}}");
    defer stage_req.deinit(std.testing.allocator);
    _ = try writer.stage(std.testing.allocator, session.txn_id, &stage_req);

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
    const path_z = try std.testing.allocator.dupeZ(u8, path);
    defer std.testing.allocator.free(path_z);

    var store = try docstore_mod.DocStore.open(std.testing.allocator, path_z, .{});
    defer store.close();
    var durable = DurableSessionStore.init(std.testing.allocator, &store);

    var registry = SessionRegistry.init(&durable);
    defer registry.deinit(std.testing.allocator);
    const session = try registry.begin(std.testing.allocator, .{ .sync_level = .write }, 11);

    var read_req = try parseStageReadRequest(std.testing.allocator, "{\"table\":\"docs\",\"key\":\"doc:a\",\"version\":\"7\"}");
    defer read_req.deinit(std.testing.allocator);
    _ = try registry.stage(std.testing.allocator, session.txn_id, &read_req);
    var write_req = try parseStageWriteRequest(std.testing.allocator, "{\"table\":\"docs\",\"key\":\"doc:a\",\"document\":{\"title\":\"status\"}}");
    defer write_req.deinit(std.testing.allocator);
    _ = try registry.stage(std.testing.allocator, session.txn_id, &write_req);

    _ = (try registry.createSavepoint(std.testing.allocator, session.txn_id)) orelse return error.TestExpectedEqual;

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
    try durable.save(registry.sessions.get(session.txn_id).?, null);
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
    _ = try writer.stage(alloc, session.txn_id, &deferred);
    const point = (try writer.createNamedSavepoint(alloc, session.txn_id, "deferred")).?;
    var immediate: OwnedTransactionCommitRequest = .{};
    defer immediate.deinit(alloc);
    try immediate.setConstraintTiming(alloc, .{ .generation = @splat(4), .deferred = false });
    const Reject = struct {
        fn validate(_: *anyopaque, _: std.mem.Allocator, _: ?*const OwnedTransactionCommitRequest, _: *OwnedTransactionCommitRequest, _: *const OwnedTransactionCommitRequest) !void {
            return error.UniqueConstraintViolation;
        }
    };
    var context: u8 = 0;
    try std.testing.expectError(error.UniqueConstraintViolation, writer.stageValidated(alloc, session.txn_id, &immediate, .{ .ptr = &context, .validate = Reject.validate }));
    {
        var pending = try writer.sessions.get(session.txn_id).?.staged.?.clone(alloc);
        defer pending.deinit(alloc);
        try std.testing.expectEqual(@as(usize, 1), pending.constraint_timing.items.len);
        try std.testing.expect(pending.constraint_timing.items[0].deferred);
    }
    _ = try writer.stage(alloc, session.txn_id, &immediate);
    _ = try writer.rollbackToSavepoint(alloc, session.txn_id, point.savepoint_id);
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
    const first = (try registry.createNamedSavepoint(alloc, session.txn_id, "point")).?;
    _ = (try registry.createNamedSavepoint(alloc, session.txn_id, "nested")).?;
    const shadow = (try registry.createNamedSavepoint(alloc, session.txn_id, "point")).?;
    _ = (try registry.createNamedSavepoint(alloc, session.txn_id, "after")).?;
    try std.testing.expectEqual(shadow.savepoint_id, (try registry.rollbackToNamedSavepoint(alloc, session.txn_id, "point")).?.savepoint_id);
    try std.testing.expectEqual(@as(usize, 3), (try registry.getStatus(alloc, session.txn_id)).?.savepoint_count);
    try std.testing.expect((try registry.releaseNamedSavepoint(alloc, session.txn_id, "after")) == null);
    _ = (try registry.releaseNamedSavepoint(alloc, session.txn_id, "point")).?;
    try std.testing.expectEqual(first.savepoint_id, (try registry.rollbackToNamedSavepoint(alloc, session.txn_id, "point")).?.savepoint_id);
    try std.testing.expectEqual(@as(usize, 1), (try registry.getStatus(alloc, session.txn_id)).?.savepoint_count);
    _ = (try registry.releaseNamedSavepoint(alloc, session.txn_id, "point")).?;
    try std.testing.expectEqual(@as(usize, 0), (try registry.getStatus(alloc, session.txn_id)).?.savepoint_count);
}

test "SQL session metadata and schema fences survive durable records" {
    const alloc = std.testing.allocator;
    var registry = SessionRegistry.init(null);
    defer registry.deinit(alloc);
    const info = try registry.beginForPrincipal(alloc, .{ .sql = .{ .database = "app", .namespace = "public", .isolation = .read_committed, .mode = .read_only } }, 9, "alice");
    try registry.setSqlFailed(alloc, info.txn_id, true);
    const session = registry.sessions.get(info.txn_id).?;
    const bytes = try encodeSessionRecord(alloc, session);
    defer alloc.free(bytes);
    var decoded = try decodeSessionRecord(alloc, info.txn_id, bytes);
    defer decoded.deinit(alloc);
    try std.testing.expectEqualStrings("app", decoded.sql.?.database);
    try std.testing.expectEqual(@import("../sql/session.zig").Isolation.read_committed, decoded.sql.?.isolation);
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
    const tables = try restored.distributedTables(alloc);
    defer alloc.free(tables);
    try std.testing.expectEqual(@as(?u32, 7), tables[0].relational_schema_version);
    try std.testing.expectEqual(@as(?u32, 7), tables[0].schema_version);
}

test "distributed txn SQL range guards survive durability and savepoint rollback without restoring writes" {
    const Case = struct {
        fn run(alloc: std.mem.Allocator) !void {
            const ranges = @import("range_read_guards.zig");
            const observation = ranges.OwnerRangeProof{ .fence = .{ .metadata_group_id = 1, .metadata_incarnation = @splat('1'), .catalog_revision = 2, .table_id = 3, .topology_epoch = 4, .route = .{ .group_id = 5, .range_id = 6, .identity_namespace = .{ .table_id = 3, .shard_id = 5, .range_id = 6 } } }, .proofs = &.{.{ .bucket = 98, .generation = std.math.maxInt(u64) }} };
            const source_observation = ranges.OwnerRangeProof{ .fence = .{ .metadata_group_id = 1, .metadata_incarnation = @splat('1'), .catalog_revision = 2, .table_id = 4, .topology_epoch = 4, .route = .{ .group_id = 7, .range_id = 8, .identity_namespace = .{ .table_id = 4, .shard_id = 7, .range_id = 8 } } }, .proofs = &.{.{ .bucket = 99, .generation = 43 }} };
            var registry = SessionRegistry.init(null);
            defer registry.deinit(alloc);
            const session = try registry.begin(alloc, .{ .sql = .{ .database = "default", .namespace = "public", .isolation = .serializable, .mode = .read_write } }, 1);
            _ = try registry.createNamedSavepoint(alloc, session.txn_id, "before");
            var request = try parseCommitRequest(alloc, "{\"read_set\":[],\"tables\":{\"docs\":{\"inserts\":{\"a\":{\"v\":1}}},\"source\":{}}}");
            defer request.deinit(alloc);
            try request.bind(alloc, "docs", "physical:3");
            try request.bind(alloc, "source", "physical:4");
            request.tables[0].schema_version = 7;
            request.tables[1].schema_version = 8;
            try request.tables[0].mergeRangeGuards(alloc, &.{observation});
            try request.tables[1].mergeRangeGuards(alloc, &.{source_observation});
            _ = try registry.stage(alloc, session.txn_id, &request);
            _ = try registry.rollbackToNamedSavepoint(alloc, session.txn_id, "before");
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
            const routed = try durable.distributedTables(alloc);
            defer alloc.free(routed);
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
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}

test "distributed txn SQL conflict guards retain first native observation through clone and durable round trip" {
    const Harness = struct {
        fn run(alloc: std.mem.Allocator) !void {
            const native = @import("../storage/db/relational_integrity_contract.zig");
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
            const tables = try restored.distributedTables(alloc);
            defer alloc.free(tables);
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
    _ = try registry.stageSql(alloc, info.txn_id, &first);
    _ = try registry.createNamedSavepoint(alloc, info.txn_id, "inserted");
    var deletion = try parseCommitRequest(alloc, "{\"read_set\":[],\"tables\":{\"t\":{\"deletes\":[\"a\"]}}}");
    defer deletion.deinit(alloc);
    _ = try registry.stageSql(alloc, info.txn_id, &deletion);
    var deleted = try registry.cloneSqlStaged(alloc, info.txn_id);
    defer deleted.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), deleted.tables[0].batch.writes.len);
    try std.testing.expectEqual(@as(usize, 1), deleted.tables[0].batch.deletes.len);
    _ = try registry.stageSql(alloc, info.txn_id, &first);
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

    _ = (try registry.createSavepoint(std.testing.allocator, session.txn_id)) orelse return error.TestExpectedEqual;
    try std.testing.expectError(error.SavepointLimitExceeded, registry.createSavepoint(std.testing.allocator, session.txn_id));

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
    const path_z = try std.testing.allocator.dupeZ(u8, path);
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
    try std.testing.expectEqual(@as(usize, 1), try registry.renewOwnedLeases(21, renewed_now));
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
    const body = "{\"read_set\":[],\"tables\":{\"docs\":{\"inserts\":{\"a\":{}}}},\"catalog_bindings\":[{\"logical\":\"docs\",\"physical\":\"table:untrusted\"}]}";
    var request = try parseCommitRequest(alloc, body);
    defer request.deinit(alloc);
    try std.testing.expectEqualStrings("docs", request.physicalName("docs"));
    try request.bind(alloc, "docs", "table:original");
    try std.testing.expectError(error.CatalogGenerationChanged, request.bind(alloc, "docs", "table:replacement"));
    const public = try encodeCommitRequest(alloc, request);
    defer alloc.free(public);
    try std.testing.expect(std.mem.indexOf(u8, public, "catalog_bindings") == null);
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
    try restored.tables[0].prepareWrites(alloc);
    try std.testing.expectEqualStrings("j", restored.tables[0].txn_writes[0].json_null_fields[0]);
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
        .{ .key = "a", .expected_version = std.math.maxInt(u64), .expected_content_digest = @splat(255) },
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
    try std.testing.expectEqual(@as(u64, 0), predicates[1].expected_version);
}
