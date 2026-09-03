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
const db_mod = @import("../storage/db/mod.zig");
const distributed_txn = @import("distributed_txn.zig");
const backend_erased = @import("../storage/backend_erased.zig");
const docstore_mod = @import("../storage/docstore.zig");
const lease_mod = @import("../storage/db/lease.zig");
const platform_process = @import("antfly_platform").process;
const platform_time = @import("antfly_platform").time;

const session_prefix = "\x00\x00__api_txn_sessions__:";
const session_lease_prefix = "\x00\x00__api_txn_session_leases__:";
const session_expiry_prefix = "\x00\x00__api_txn_session_expiry__:";
const session_recovery_prefix = "\x00\x00__api_txn_session_recovery__:";
// Keyed receipts use a versioned namespace that pre-feature binaries do not
// scan. Older binaries therefore cannot discover, adopt, rewrite, or expire
// them while a cluster is rolling through the feature boundary.
const receipt_session_prefix = "\x00\x00__api_idempotent_receipts_v1__:";
const receipt_lease_prefix = "\x00\x00__api_idempotent_receipt_leases_v1__:";
const receipt_expiry_prefix = "\x00\x00__api_idempotent_receipt_expiry_v1__:";
const receipt_recovery_prefix = "\x00\x00__api_idempotent_receipt_recovery_v1__:";
var txn_id_nonce: std.atomic.Value(u64) = .init(0);
var owner_incarnation_nonce: std.atomic.Value(u64) = .init(1);

const SessionExpiryCursor = struct {
    timestamp: u64,
    txn_id: db_mod.types.TxnId,
};

fn newOwnerIncarnation() u64 {
    const material = [3]u64{
        platform_time.realtimeNs(),
        platform_process.currentId() orelse 0,
        owner_incarnation_nonce.fetchAdd(1, .monotonic),
    };
    return @max(@as(u64, 1), std.hash.Wyhash.hash(0, std.mem.asBytes(&material)) & std.math.maxInt(i64));
}

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
        return .{
            .table_name = try alloc.dupe(u8, self.table_name),
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
    table_name: []u8,
    batch: batch_api.OwnedBatchRequest = .{},
    predicates: std.ArrayListUnmanaged(db_mod.types.TransactionVersionPredicate) = .empty,
    txn_writes: []db_mod.types.TransactionWrite = &.{},

    pub fn deinit(self: *TableCommitRequest, alloc: std.mem.Allocator) void {
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
        };
        errdefer out.deinit(alloc);
        out.batch = try cloneBatchRequest(alloc, self.batch);
        try clonePredicatesInto(alloc, &out.predicates, self.predicates.items);
        return out;
    }

    pub fn mergeFrom(self: *TableCommitRequest, alloc: std.mem.Allocator, other: TableCommitRequest) !void {
        try appendBatchWrites(alloc, &self.batch, other.batch.writes);
        try appendBatchDeletes(alloc, &self.batch, other.batch.deletes);
        try appendBatchTransforms(alloc, &self.batch, other.batch.transforms);
        try appendPredicates(alloc, &self.predicates, other.predicates.items);
        syncAndClear(self, alloc);
    }

    pub fn prepareWrites(self: *TableCommitRequest, alloc: std.mem.Allocator) !void {
        if (self.txn_writes.len > 0) return;
        self.txn_writes = try alloc.alloc(db_mod.types.TransactionWrite, self.batch.writes.len);
        for (self.batch.writes, 0..) |write, i| {
            self.txn_writes[i] = .{
                .key = write.key,
                .value = write.value,
            };
        }
    }

    pub fn result(self: TableCommitRequest) batch_api.BatchResult {
        return self.batch.result();
    }
};

pub const OwnedTransactionCommitRequest = struct {
    read_set: []TransactionReadItem = &.{},
    tables: []TableCommitRequest = &.{},
    sync_level: db_mod.types.SyncLevel = .propose,

    pub fn deinit(self: *OwnedTransactionCommitRequest, alloc: std.mem.Allocator) void {
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

        out.read_set = try alloc.alloc(TransactionReadItem, self.read_set.len);
        var read_count: usize = 0;
        errdefer {
            for (out.read_set[0..read_count]) |*item| item.deinit(alloc);
            if (out.read_set.len > 0) alloc.free(out.read_set);
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
        }
        for (self.tables) |table| {
            out.tables[table_count] = try table.clone(alloc);
            table_count += 1;
        }
        return out;
    }

    pub fn mergeFrom(self: *OwnedTransactionCommitRequest, alloc: std.mem.Allocator, other: *const OwnedTransactionCommitRequest) !void {
        try appendReadSet(alloc, self, other.read_set);
        for (other.tables) |table| {
            const existing = findTableIndex(self.tables, table.table_name);
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
                .table_name = table.table_name,
                .writes = table.txn_writes,
                .deletes = table.batch.deletes,
                .transforms = table.batch.transforms,
                .predicates = table.predicates.items,
            };
        }
        return out;
    }
};

pub const CommitConflict = struct {
    table_name: []const u8,
    key: []const u8,
    message: []const u8,
    group_id: ?u64 = null,
    phase: ?distributed_txn.ParticipantPhase = null,
    kind: CommitConflictKind = .transaction_conflict,
    retryable: bool = false,
    retry_after_ms: ?u32 = null,
    retry_scope: ?[]const u8 = null,
    expected_version: ?u64 = null,
    current_version: ?u64 = null,
};

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
    kind: SessionKind = .interactive,
};

/// Session purpose is a capability boundary, not merely reporting metadata.
/// Idempotency receipts are owned by the keyed-batch replay/recovery path and
/// must never be mutated or deleted through interactive transaction APIs.
pub const SessionKind = enum {
    interactive,
    idempotent_receipt,
};

pub const TerminalCommitStatus = enum {
    committed,
    committed_visibility_pending,
    committed_recovery_pending,

    pub fn text(self: TerminalCommitStatus) []const u8 {
        return @tagName(self);
    }
};

fn terminalCommitProgress(status: TerminalCommitStatus) u2 {
    return switch (status) {
        .committed_recovery_pending => 0,
        .committed_visibility_pending => 1,
        .committed => 2,
    };
}

fn laterTerminalCommitStatus(existing: TerminalCommitStatus, incoming: TerminalCommitStatus) TerminalCommitStatus {
    return if (terminalCommitProgress(incoming) > terminalCommitProgress(existing)) incoming else existing;
}

/// Non-commit receipt state remains distinct from committed visibility debt.
/// The versioned receipt keyspace, rather than permissive JSON decoding, is the
/// compatibility boundary that prevents rollback binaries mutating it.
pub const IdempotentReceiptOutcome = enum {
    not_applied,
    aborted,

    pub fn text(self: IdempotentReceiptOutcome) []const u8 {
        return @tagName(self);
    }
};

/// The complete terminal API result is part of the durable idempotency
/// contract. Replays must return the same machine-readable reason as the
/// request that established the terminal receipt, not merely the same broad
/// applied/not-applied classification.
pub const IdempotentTerminalReceipt = struct {
    outcome: IdempotentReceiptOutcome,
    code: []u8,
    message: []u8,
    retryable: bool = false,

    pub fn deinit(self: *IdempotentTerminalReceipt, alloc: std.mem.Allocator) void {
        alloc.free(self.code);
        alloc.free(self.message);
        self.* = undefined;
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

/// Coordinator acknowledgement is part of the durable API/storage handoff.
/// Until it is persisted, replay and reconciliation must continue to expose
/// recovery debt even though the commit decision itself is durable.
pub fn effectiveTerminalCommitStatus(terminal: TerminalCommit) TerminalCommitStatus {
    if (terminal.status == .committed and
        terminal.coordinator_group_id != null and
        !terminal.coordinator_acknowledged) return .committed_recovery_pending;
    return terminal.status;
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

/// An immutable, ownership-independent view of a committed idempotent
/// operation. Terminal receipt replay must remain available after process
/// restart even while the prior writer lease is still fencing mutable work.
pub const IdempotentTerminalCommitSnapshot = struct {
    request: OwnedTransactionCommitRequest,
    terminal: TerminalCommit,
    owns_mutation_lease: bool,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        self.request.deinit(alloc);
        self.terminal.deinit(alloc);
        self.* = undefined;
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
        /// Stable keyed batches retain a terminal non-application receipt
        /// instead of deleting their session when recovery proves an abort.
        idempotent_receipt: bool = false,
        /// A prior visibility attempt reached durable repair debt. Recovery
        /// should finish participant propagation at write durability without
        /// polling the failed provider again.
        repair_required: bool = false,
        /// The source returned only a terminal error, so coordinator metadata
        /// must be recovered without re-entering the failed visibility barrier.
        repair_handoff_needs_coordinator: bool = false,
        request: OwnedTransactionCommitRequest,
    },
    acknowledge: PendingTerminalAcknowledgement,

    pub fn deinit(self: *PendingSessionRecovery, alloc: std.mem.Allocator) void {
        switch (self.*) {
            .commit => |*value| value.request.deinit(alloc),
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
    /// Durable application outcome for an idempotent commit. Null means the
    /// transaction has not reached a terminal API receipt yet.
    outcome: ?[]const u8 = null,
    repair_required: bool = false,
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

    pub fn deinit(self: *SessionDetails, alloc: std.mem.Allocator) void {
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
    outcome: ?[]const u8 = null,
    repair_required: bool = false,
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
    outcome: ?[]const u8 = null,
    repair_required: bool = false,
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
    snapshot: OwnedTransactionCommitRequest,
    read_snapshots: std.StringArrayHashMapUnmanaged(SessionReadSnapshot) = .empty,

    pub fn deinit(self: *Savepoint, alloc: std.mem.Allocator) void {
        self.snapshot.deinit(alloc);
        deinitReadSnapshotMap(alloc, &self.read_snapshots);
        self.* = undefined;
    }

    pub fn clone(self: Savepoint, alloc: std.mem.Allocator) !Savepoint {
        var out: Savepoint = .{
            .id = self.id,
            .snapshot = try self.snapshot.clone(alloc),
        };
        errdefer out.snapshot.deinit(alloc);
        out.read_snapshots = try cloneReadSnapshotMap(alloc, self.read_snapshots);
        return out;
    }
};

pub const Session = struct {
    txn_id: db_mod.types.TxnId,
    owner_node_id: u64,
    /// Process-incarnation token. Node IDs are stable across restarts, so they
    /// cannot fence an overlapped old process by themselves.
    owner_incarnation: u64 = 0,
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
    /// Marks sessions whose identity is derived from an external idempotency
    /// key. Recovery must preserve their terminal non-application receipt.
    idempotent_receipt: bool = false,
    /// Terminal rejection is deliberately distinct from commit visibility and
    /// recovery status.
    idempotent_outcome: ?IdempotentReceiptOutcome = null,
    /// Additive fields preserve the exact terminal HTTP error envelope. Older
    /// records that contain only `idempotent_outcome` remain readable and use
    /// the legacy generic replay response.
    idempotent_error_code: ?[]u8 = null,
    idempotent_error_message: ?[]u8 = null,
    idempotent_error_retryable: bool = false,
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
            .kind = if (self.idempotent_receipt) .idempotent_receipt else .interactive,
        };
    }

    pub fn deinit(self: *Session, alloc: std.mem.Allocator) void {
        if (self.principal) |principal| alloc.free(principal);
        if (self.staged) |*staged| staged.deinit(alloc);
        if (self.idempotent_error_code) |code| alloc.free(code);
        if (self.idempotent_error_message) |message| alloc.free(message);
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
            .owner_incarnation = self.owner_incarnation,
            .principal = if (self.principal) |principal| try alloc.dupe(u8, principal) else null,
            .begin_timestamp = self.begin_timestamp,
            .last_touched_timestamp = self.last_touched_timestamp,
            .sync_level = self.sync_level,
            .next_savepoint_id = self.next_savepoint_id,
            .commit_body_digest = self.commit_body_digest,
            .commit_execution_started = self.commit_execution_started,
            .idempotent_receipt = self.idempotent_receipt,
            .idempotent_outcome = self.idempotent_outcome,
            .idempotent_error_retryable = self.idempotent_error_retryable,
        };
        errdefer out.deinit(alloc);
        if (self.idempotent_error_code) |code| out.idempotent_error_code = try alloc.dupe(u8, code);
        if (self.idempotent_error_message) |message| out.idempotent_error_message = try alloc.dupe(u8, message);
        if (self.staged) |staged| out.staged = try staged.clone(alloc);
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
        const key = try makeSessionKeyForKind(self.alloc, session.txn_id, sessionKind(session));
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
        expected_incarnation: ?u64,
        now_ms: u64,
        ttl_ms: u64,
        require_expired: bool,
        max_record_bytes: ?usize,
        max_sessions: ?usize,
    ) !bool {
        if (self.fail_writes_for_test) return error.InjectedSessionStoreFailure;
        return switch (self.backend) {
            .docstore => |store| blk: {
                var txn = try store.beginWriteTxn();
                errdefer txn.abort();
                const changed = try saveSessionWithLeaseTxn(self, &txn, session, expected_owner, expected_incarnation, now_ms, ttl_ms, require_expired, max_record_bytes, max_sessions);
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
                const changed = try saveSessionWithLeaseTxn(self, &txn, session, expected_owner, expected_incarnation, now_ms, ttl_ms, require_expired, max_record_bytes, max_sessions);
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
        expected_incarnation: ?u64,
        now_ms: u64,
        ttl_ms: u64,
        require_expired: bool,
        max_record_bytes: ?usize,
        max_sessions: ?usize,
    ) !bool {
        const kind = sessionKind(session);
        const session_key = try makeSessionKeyForKind(self.alloc, session.txn_id, kind);
        defer self.alloc.free(session_key);
        const lease_key = try makeSessionLeaseKeyForKind(self.alloc, session.txn_id, kind);
        defer self.alloc.free(lease_key);
        const alternate_kind: SessionKind = if (kind == .interactive) .idempotent_receipt else .interactive;
        const alternate_key = try makeSessionKeyForKind(self.alloc, session.txn_id, alternate_kind);
        defer self.alloc.free(alternate_key);
        if (txn.get(alternate_key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        } != null) return false;

        const current_raw = txn.get(session_key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        if (expected_owner) |owner| {
            const raw = current_raw orelse return false;
            var current = try decodeSessionRecord(self.alloc, session.txn_id, raw);
            defer current.deinit(self.alloc);
            if (current.owner_node_id != owner or
                current.owner_incarnation != (expected_incarnation orelse return false)) return false;
        } else {
            if (current_raw != null) return false;
        }

        // Cluster-shared admission cannot use a process-local cached count.
        // Count and create under the backend write transaction so every API
        // process observes one serial capacity boundary. This scan is also
        // compatible with rolling binaries that do not maintain a counter.
        if (expected_owner == null) if (max_sessions) |limit| {
            if (try countSessionsTxn(txn, limit) >= limit) return error.SessionLimitExceeded;
        };

        const owner_id = try ownerLeaseId(self.alloc, session.owner_node_id, session.owner_incarnation);
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
            if (sessionKind(previous) != kind) return error.InvalidTransactionSessionRecord;
            const old_expiry_key = try makeSessionExpiryKeyForKind(self.alloc, previous.last_touched_timestamp, session.txn_id, kind);
            defer self.alloc.free(old_expiry_key);
            txn.delete(old_expiry_key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
        }
        const expiry_key = try makeSessionExpiryKeyForKind(self.alloc, session.last_touched_timestamp, session.txn_id, kind);
        defer self.alloc.free(expiry_key);
        try txn.put(session_key, session_value);
        try txn.put(expiry_key, &.{});
        if (sessionNeedsRecovery(session)) {
            try setSessionRecoveryIndexTxn(self, txn, session);
        } else if (previous_needs_recovery) {
            try clearSessionRecoveryIndexTxn(self, txn, session.txn_id, kind);
        }
        if (self.fail_lease_transition_after_session_write_for_test) return error.InjectedLeaseTransitionFailure;
        try txn.put(lease_key, lease_value);
        return true;
    }

    fn countSessionsTxn(txn: anytype, limit: usize) !usize {
        if (limit == 0) return 0;
        var cursor = try txn.openCursor();
        defer cursor.close();
        var count: usize = 0;
        for ([_]SessionKind{ .interactive, .idempotent_receipt }) |kind| {
            const prefix = sessionPrefix(kind);
            var entry = try cursor.seekAtOrAfter(prefix);
            while (entry) |row| : (entry = try cursor.next()) {
                if (!std.mem.startsWith(u8, row.key, prefix)) break;
                count += 1;
                if (count >= limit) return count;
            }
        }
        return count;
    }

    pub fn load(self: *DurableSessionStore, txn_id: db_mod.types.TxnId) !?Session {
        if (try self.loadForKind(txn_id, .idempotent_receipt)) |session| return session;
        return try self.loadForKind(txn_id, .interactive);
    }

    fn loadForKind(self: *DurableSessionStore, txn_id: db_mod.types.TxnId, kind: SessionKind) !?Session {
        const key = try makeSessionKeyForKind(self.alloc, txn_id, kind);
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
        var session = try decodeSessionRecord(self.alloc, txn_id, value);
        errdefer session.deinit(self.alloc);
        if (sessionKind(session) != kind) return error.InvalidTransactionSessionRecord;
        return session;
    }

    pub fn delete(self: *DurableSessionStore, txn_id: db_mod.types.TxnId) !void {
        if (self.fail_writes_for_test) return error.InjectedSessionStoreFailure;
        switch (self.backend) {
            .docstore => |store| {
                var txn = try store.beginWriteTxn();
                errdefer txn.abort();
                try deleteLocatedSessionAndExpiryTxn(self, &txn, txn_id);
                try txn.commit();
            },
            .runtime => |store| {
                var txn = try store.beginWrite();
                errdefer txn.abort();
                try deleteLocatedSessionAndExpiryTxn(self, &txn, txn_id);
                try txn.commit();
            },
        }
    }

    /// Deletes a session only while the caller's exact process incarnation
    /// still owns it. The session, indexes, and lease disappear atomically.
    pub fn deleteWithLease(
        self: *DurableSessionStore,
        txn_id: db_mod.types.TxnId,
        owner_node_id: u64,
        owner_incarnation: u64,
    ) !bool {
        if (self.fail_writes_for_test) return error.InjectedSessionStoreFailure;
        return switch (self.backend) {
            .docstore => |store| blk: {
                var txn = try store.beginWriteTxn();
                errdefer txn.abort();
                if (!(try deleteSessionWithLeaseTxn(self, &txn, txn_id, owner_node_id, owner_incarnation))) {
                    txn.abort();
                    break :blk false;
                }
                try txn.commit();
                break :blk true;
            },
            .runtime => |store| blk: {
                var txn = try store.beginWrite();
                errdefer txn.abort();
                if (!(try deleteSessionWithLeaseTxn(self, &txn, txn_id, owner_node_id, owner_incarnation))) {
                    txn.abort();
                    break :blk false;
                }
                try txn.commit();
                break :blk true;
            },
        };
    }

    /// Reclaims a receipt left by a dead process without first publishing a
    /// transient new owner. The immutable state observation, retention test,
    /// expired lease, indexes, and deletion are one storage transaction.
    pub fn deleteExpiredWithLease(
        self: *DurableSessionStore,
        txn_id: db_mod.types.TxnId,
        expected_owner_node_id: u64,
        expected_owner_incarnation: u64,
        expected_last_touched_timestamp: u64,
        cutoff_ns: u64,
        now_ms: u64,
    ) !bool {
        if (self.fail_writes_for_test) return error.InjectedSessionStoreFailure;
        return switch (self.backend) {
            .docstore => |store| blk: {
                var txn = try store.beginWriteTxn();
                errdefer txn.abort();
                if (!(try deleteExpiredSessionWithLeaseTxn(
                    self,
                    &txn,
                    txn_id,
                    expected_owner_node_id,
                    expected_owner_incarnation,
                    expected_last_touched_timestamp,
                    cutoff_ns,
                    now_ms,
                ))) {
                    txn.abort();
                    break :blk false;
                }
                try txn.commit();
                break :blk true;
            },
            .runtime => |store| blk: {
                var txn = try store.beginWrite();
                errdefer txn.abort();
                if (!(try deleteExpiredSessionWithLeaseTxn(
                    self,
                    &txn,
                    txn_id,
                    expected_owner_node_id,
                    expected_owner_incarnation,
                    expected_last_touched_timestamp,
                    cutoff_ns,
                    now_ms,
                ))) {
                    txn.abort();
                    break :blk false;
                }
                try txn.commit();
                break :blk true;
            },
        };
    }

    fn deleteSessionWithLeaseTxn(
        self: *DurableSessionStore,
        txn: anytype,
        txn_id: db_mod.types.TxnId,
        owner_node_id: u64,
        owner_incarnation: u64,
    ) !bool {
        var located = (try loadLocatedSessionTxn(self, txn, txn_id)) orelse return false;
        defer located.deinit(self.alloc);
        const current = &located.session;
        if (current.owner_node_id != owner_node_id or
            current.owner_incarnation != owner_incarnation) return false;

        const kind = sessionKind(current.*);
        const lease_key = try makeSessionLeaseKeyForKind(self.alloc, txn_id, kind);
        defer self.alloc.free(lease_key);
        const lease_raw = txn.get(lease_key) catch |err| switch (err) {
            error.NotFound => return false,
            else => return err,
        };
        const parsed = try std.json.parseFromSlice(lease_mod.LeaseRecord, self.alloc, lease_raw, .{ .allocate = .alloc_always });
        defer parsed.deinit();
        const owner_id = try ownerLeaseId(self.alloc, owner_node_id, owner_incarnation);
        defer self.alloc.free(owner_id);
        if (!std.mem.eql(u8, parsed.value.owner_id, owner_id)) return false;

        try deleteSessionAndExpiryTxn(self, txn, located.key, txn_id, kind);
        try txn.delete(lease_key);
        return true;
    }

    fn deleteExpiredSessionWithLeaseTxn(
        self: *DurableSessionStore,
        txn: anytype,
        txn_id: db_mod.types.TxnId,
        expected_owner_node_id: u64,
        expected_owner_incarnation: u64,
        expected_last_touched_timestamp: u64,
        cutoff_ns: u64,
        now_ms: u64,
    ) !bool {
        var located = (try loadLocatedSessionTxn(self, txn, txn_id)) orelse return false;
        defer located.deinit(self.alloc);
        const current = &located.session;
        if (current.owner_node_id != expected_owner_node_id or
            current.owner_incarnation != expected_owner_incarnation or
            current.last_touched_timestamp != expected_last_touched_timestamp or
            current.last_touched_timestamp >= cutoff_ns) return false;
        // Retention applies to completed receipts, never to an operation whose
        // durable decision or coordinator handoff is still being recovered.
        // Keep this check in the storage transaction as the final authority.
        if (sessionRetentionPinned(current.*)) return false;
        if (current.terminal_commit) |terminal| {
            if (terminal.coordinator_group_id != null and !terminal.coordinator_acknowledged) return false;
        }

        const kind = sessionKind(current.*);
        const lease_key = try makeSessionLeaseKeyForKind(self.alloc, txn_id, kind);
        defer self.alloc.free(lease_key);
        const lease_raw = txn.get(lease_key) catch |err| switch (err) {
            error.NotFound => return false,
            else => return err,
        };
        const parsed = try std.json.parseFromSlice(lease_mod.LeaseRecord, self.alloc, lease_raw, .{ .allocate = .alloc_always });
        defer parsed.deinit();
        const expected_owner_id = try ownerLeaseId(self.alloc, expected_owner_node_id, expected_owner_incarnation);
        defer self.alloc.free(expected_owner_id);
        if (!std.mem.eql(u8, parsed.value.owner_id, expected_owner_id) or
            parsed.value.expires_at_ms > now_ms) return false;

        try deleteSessionAndExpiryTxn(self, txn, located.key, txn_id, kind);
        try txn.delete(lease_key);
        return true;
    }

    fn putSessionAndExpiryTxn(self: *DurableSessionStore, txn: anytype, key: []const u8, value: []const u8, session: Session) !void {
        const kind = sessionKind(session);
        const alternate_kind: SessionKind = if (kind == .interactive) .idempotent_receipt else .interactive;
        const alternate_key = try makeSessionKeyForKind(self.alloc, session.txn_id, alternate_kind);
        defer self.alloc.free(alternate_key);
        if (txn.get(alternate_key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        } != null) return error.SessionKindMismatch;
        var previous_needs_recovery = false;
        if (txn.get(key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        }) |raw| {
            var previous = try decodeSessionRecord(self.alloc, session.txn_id, raw);
            defer previous.deinit(self.alloc);
            if (sessionKind(previous) != kind) return error.InvalidTransactionSessionRecord;
            previous_needs_recovery = sessionNeedsRecovery(previous);
            const old_expiry_key = try makeSessionExpiryKeyForKind(self.alloc, previous.last_touched_timestamp, session.txn_id, kind);
            defer self.alloc.free(old_expiry_key);
            txn.delete(old_expiry_key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
        }
        const expiry_key = try makeSessionExpiryKeyForKind(self.alloc, session.last_touched_timestamp, session.txn_id, kind);
        defer self.alloc.free(expiry_key);
        try txn.put(key, value);
        try txn.put(expiry_key, &.{});
        if (sessionNeedsRecovery(session)) {
            try setSessionRecoveryIndexTxn(self, txn, session);
        } else if (previous_needs_recovery) {
            try clearSessionRecoveryIndexTxn(self, txn, session.txn_id, kind);
        }
    }

    const LocatedSession = struct {
        key: []u8,
        session: Session,

        fn deinit(self: *LocatedSession, alloc: std.mem.Allocator) void {
            alloc.free(self.key);
            self.session.deinit(alloc);
            self.* = undefined;
        }
    };

    fn loadLocatedSessionTxn(self: *DurableSessionStore, txn: anytype, txn_id: db_mod.types.TxnId) !?LocatedSession {
        for ([_]SessionKind{ .idempotent_receipt, .interactive }) |kind| {
            const key = try makeSessionKeyForKind(self.alloc, txn_id, kind);
            errdefer self.alloc.free(key);
            const raw = txn.get(key) catch |err| switch (err) {
                error.NotFound => {
                    self.alloc.free(key);
                    continue;
                },
                else => return err,
            };
            var session = try decodeSessionRecord(self.alloc, txn_id, raw);
            errdefer session.deinit(self.alloc);
            if (sessionKind(session) != kind) return error.InvalidTransactionSessionRecord;
            return .{ .key = key, .session = session };
        }
        return null;
    }

    fn deleteLocatedSessionAndExpiryTxn(self: *DurableSessionStore, txn: anytype, txn_id: db_mod.types.TxnId) !void {
        var located = (try loadLocatedSessionTxn(self, txn, txn_id)) orelse return;
        defer located.deinit(self.alloc);
        try deleteSessionAndExpiryTxn(self, txn, located.key, txn_id, sessionKind(located.session));
    }

    fn deleteSessionAndExpiryTxn(self: *DurableSessionStore, txn: anytype, key: []const u8, txn_id: db_mod.types.TxnId, kind: SessionKind) !void {
        if (txn.get(key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        }) |raw| {
            var previous = try decodeSessionRecord(self.alloc, txn_id, raw);
            defer previous.deinit(self.alloc);
            if (sessionKind(previous) != kind) return error.InvalidTransactionSessionRecord;
            const expiry_key = try makeSessionExpiryKeyForKind(self.alloc, previous.last_touched_timestamp, txn_id, kind);
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
        const recovery_key = try makeSessionRecoveryKeyForKind(self.alloc, txn_id, kind);
        defer self.alloc.free(recovery_key);
        txn.delete(recovery_key) catch |err| switch (err) {
            error.NotFound => {},
            else => return err,
        };
    }

    fn setSessionRecoveryIndexTxn(self: *DurableSessionStore, txn: anytype, session: Session) !void {
        const key = try makeSessionRecoveryKeyForKind(self.alloc, session.txn_id, sessionKind(session));
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

    fn clearSessionRecoveryIndexTxn(self: *DurableSessionStore, txn: anytype, txn_id: db_mod.types.TxnId, kind: SessionKind) !void {
        const key = try makeSessionRecoveryKeyForKind(self.alloc, txn_id, kind);
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
        kind: SessionKind,
        after: ?db_mod.types.TxnId,
        limit: usize,
    ) !RecoveryIdPage {
        var ids = std.ArrayListUnmanaged(db_mod.types.TxnId).empty;
        errdefer ids.deinit(alloc);
        if (limit == 0) return .{ .ids = try ids.toOwnedSlice(alloc), .next_after = null };
        const prefix = sessionRecoveryPrefix(kind);
        const start_key = if (after) |txn_id| try makeSessionRecoveryKeyForKind(alloc, txn_id, kind) else null;
        defer if (start_key) |key| alloc.free(key);
        const Scan = struct {
            allocator: std.mem.Allocator,
            prefix: []const u8,
            limit: usize,
            ids: *std.ArrayListUnmanaged(db_mod.types.TxnId),

            fn visit(raw: *anyopaque, key: []const u8, _: []const u8) anyerror!bool {
                const scan: *@This() = @ptrCast(@alignCast(raw));
                if (key.len <= scan.prefix.len) return true;
                const txn_id = distributed_txn.parseTxnIdHex(key[scan.prefix.len..]) catch return true;
                try scan.ids.append(scan.allocator, txn_id);
                return scan.ids.items.len < scan.limit;
            }
        };
        var scan = Scan{ .allocator = alloc, .prefix = prefix, .limit = limit, .ids = &ids };
        try self.scanPrefixFromWithContext(prefix, start_key, &scan, Scan.visit);
        const next_after = if (ids.items.len == limit) ids.items[ids.items.len - 1] else null;
        return .{ .ids = try ids.toOwnedSlice(alloc), .next_after = next_after };
    }

    /// Bounded compatibility audit for sessions written before the recovery
    /// index existed. Only `scan_limit` full records are decoded per call.
    pub fn scanLegacyRecoveryIds(
        self: *DurableSessionStore,
        alloc: std.mem.Allocator,
        kind: SessionKind,
        after: ?db_mod.types.TxnId,
        scan_limit: usize,
    ) !RecoveryIdPage {
        var ids = std.ArrayListUnmanaged(db_mod.types.TxnId).empty;
        errdefer ids.deinit(alloc);
        if (scan_limit == 0) return .{ .ids = try ids.toOwnedSlice(alloc), .next_after = null };
        const prefix = sessionPrefix(kind);
        const start_key = if (after) |txn_id| try makeSessionKeyForKind(alloc, txn_id, kind) else null;
        defer if (start_key) |key| alloc.free(key);
        const Scan = struct {
            allocator: std.mem.Allocator,
            kind: SessionKind,
            prefix: []const u8,
            scan_limit: usize,
            scanned: usize = 0,
            last_txn_id: ?db_mod.types.TxnId = null,
            ids: *std.ArrayListUnmanaged(db_mod.types.TxnId),

            fn visit(raw: *anyopaque, key: []const u8, value: []const u8) anyerror!bool {
                const scan: *@This() = @ptrCast(@alignCast(raw));
                if (key.len <= scan.prefix.len) return true;
                const txn_id = distributed_txn.parseTxnIdHex(key[scan.prefix.len..]) catch return true;
                scan.scanned += 1;
                scan.last_txn_id = txn_id;
                var session = decodeSessionRecord(scan.allocator, txn_id, value) catch return scan.scanned < scan.scan_limit;
                defer session.deinit(scan.allocator);
                if (sessionKind(session) != scan.kind) return scan.scanned < scan.scan_limit;
                if (sessionNeedsRecovery(session)) try scan.ids.append(scan.allocator, txn_id);
                return scan.scanned < scan.scan_limit;
            }
        };
        var scan = Scan{ .allocator = alloc, .kind = kind, .prefix = prefix, .scan_limit = scan_limit, .ids = &ids };
        try self.scanPrefixFromWithContext(prefix, start_key, &scan, Scan.visit);
        const next_after = if (scan.scanned == scan_limit) scan.last_txn_id else null;
        return .{ .ids = try ids.toOwnedSlice(alloc), .next_after = next_after };
    }

    /// Rechecks the current durable row under a write transaction before
    /// backfilling its index entry, avoiding stale audit results.
    pub fn refreshRecoveryIndex(self: *DurableSessionStore, txn_id: db_mod.types.TxnId, kind: SessionKind) !void {
        const session_key = try makeSessionKeyForKind(self.alloc, txn_id, kind);
        defer self.alloc.free(session_key);
        const recovery_key = try makeSessionRecoveryKeyForKind(self.alloc, txn_id, kind);
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
                    if (sessionKind(session) != kind) return error.InvalidTransactionSessionRecord;
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
                    if (sessionKind(session) != kind) return error.InvalidTransactionSessionRecord;
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

    pub const ExpiredIdPage = struct {
        ids: []db_mod.types.TxnId,
        next_after: ?SessionExpiryCursor,
    };

    /// Rotates through expired keys so retention-pinned recovery records at
    /// the head of the timestamp index cannot starve completed receipts.
    pub fn scanExpiredIds(
        self: *DurableSessionStore,
        alloc: std.mem.Allocator,
        kind: SessionKind,
        cutoff_ns: u64,
        after: ?SessionExpiryCursor,
        limit: usize,
    ) !ExpiredIdPage {
        var ids = std.ArrayListUnmanaged(db_mod.types.TxnId).empty;
        errdefer ids.deinit(alloc);
        if (limit == 0) return .{ .ids = try ids.toOwnedSlice(alloc), .next_after = null };
        const prefix = sessionExpiryPrefix(kind);
        const start_key = if (after) |cursor| try makeSessionExpiryKeyForKind(alloc, cursor.timestamp, cursor.txn_id, kind) else null;
        defer if (start_key) |key| alloc.free(key);
        const Scan = struct {
            allocator: std.mem.Allocator,
            kind: SessionKind,
            cutoff: u64,
            limit: usize,
            ids: *std.ArrayListUnmanaged(db_mod.types.TxnId),
            last: ?SessionExpiryCursor = null,
            fn visit(raw: *anyopaque, key: []const u8, _: []const u8) anyerror!bool {
                const scan: *@This() = @ptrCast(@alignCast(raw));
                const parsed = parseSessionExpiryKey(key, scan.kind) orelse return true;
                if (parsed.timestamp >= scan.cutoff or scan.ids.items.len >= scan.limit) return false;
                try scan.ids.append(scan.allocator, parsed.txn_id);
                scan.last = parsed;
                return scan.ids.items.len < scan.limit;
            }
        };
        var scan = Scan{ .allocator = alloc, .kind = kind, .cutoff = cutoff_ns, .limit = limit, .ids = &ids };
        try self.scanPrefixFromWithContext(prefix, start_key, &scan, Scan.visit);
        const next_after = if (ids.items.len == limit) scan.last else null;
        return .{
            .ids = try ids.toOwnedSlice(alloc),
            .next_after = next_after,
        };
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
                    prefix: []const u8 = "",
                    fn visit(ctx: ?*anyopaque, key: []const u8, _: []const u8) anyerror!docstore_mod.DocStore.ScanAction {
                        const counter: *@This() = @ptrCast(@alignCast(ctx.?));
                        if (!std.mem.startsWith(u8, key, counter.prefix)) return .stop;
                        counter.count += 1;
                        return .@"continue";
                    }
                };
                var counter = Counter{};
                for ([_]SessionKind{ .interactive, .idempotent_receipt }) |kind| {
                    counter.prefix = sessionPrefix(kind);
                    try store.scanWithContext(sessionPrefix(kind), &.{}, .{}, &counter, Counter.visit);
                }
                break :blk counter.count;
            },
            .runtime => |store| blk: {
                var txn = try store.beginCurrentScan();
                defer txn.abort();
                var cursor = try txn.openCursor();
                defer cursor.close();
                var count: usize = 0;
                for ([_]SessionKind{ .interactive, .idempotent_receipt }) |kind| {
                    const prefix = sessionPrefix(kind);
                    var entry = try cursor.seekAtOrAfter(prefix);
                    while (entry) |row| : (entry = try cursor.next()) {
                        if (!std.mem.startsWith(u8, row.key, prefix)) break;
                        count += 1;
                    }
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

/// Lease records store an absolute millisecond deadline while callers measure
/// time in nanoseconds. Round the configured duration up and retain one extra
/// millisecond so flooring `now_ns` cannot make the durable lease shorter than
/// the configured lifetime. The bounded skew is preferable to an ownership
/// gap in which another process can adopt before the first heartbeat.
pub fn sessionLeaseStorageTtlMs(ttl_ns: u64) u64 {
    const rounded_ms = @max(@as(u64, 1), (ttl_ns +| (std.time.ns_per_ms - 1)) / std.time.ns_per_ms);
    return rounded_ms +| 1;
}

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
        if (try self.loadForKind(alloc, txn_id, .idempotent_receipt)) |record| return record;
        return try self.loadForKind(alloc, txn_id, .interactive);
    }

    fn loadForKind(self: *const SessionLeaseStore, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId, kind: SessionKind) !?lease_mod.LeaseRecord {
        const key = try makeSessionLeaseKeyForKind(self.alloc, txn_id, kind);
        defer self.alloc.free(key);
        var lease = switch (self.backend) {
            .docstore => |store| try lease_mod.Lease.init(self.alloc, store, key),
            .runtime => |store| try lease_mod.Lease.init(self.alloc, store, key),
        };
        defer lease.deinit();
        return try lease.load(alloc);
    }

    pub fn renew(self: *const SessionLeaseStore, txn_id: db_mod.types.TxnId, kind: SessionKind, owner_node_id: u64, owner_incarnation: u64, now_ms: u64, ttl_ms: u64) !bool {
        const owner_id = try ownerLeaseId(self.alloc, owner_node_id, owner_incarnation);
        defer self.alloc.free(owner_id);
        const key = try makeSessionLeaseKeyForKind(self.alloc, txn_id, kind);
        defer self.alloc.free(key);
        var lease = switch (self.backend) {
            .docstore => |store| try lease_mod.Lease.init(self.alloc, store, key),
            .runtime => |store| try lease_mod.Lease.init(self.alloc, store, key),
        };
        defer lease.deinit();
        return try lease.renew(owner_id, now_ms, ttl_ms);
    }

    pub fn release(self: *const SessionLeaseStore, txn_id: db_mod.types.TxnId, owner_node_id: u64, owner_incarnation: u64) !bool {
        const owner_id = try ownerLeaseId(self.alloc, owner_node_id, owner_incarnation);
        defer self.alloc.free(owner_id);
        for ([_]SessionKind{ .idempotent_receipt, .interactive }) |kind| {
            const key = try makeSessionLeaseKeyForKind(self.alloc, txn_id, kind);
            defer self.alloc.free(key);
            var lease = switch (self.backend) {
                .docstore => |store| try lease_mod.Lease.init(self.alloc, store, key),
                .runtime => |store| try lease_mod.Lease.init(self.alloc, store, key),
            };
            defer lease.deinit();
            if (try lease.release(owner_id)) return true;
        }
        return false;
    }
};

pub const SessionRegistry = struct {
    const session_lock_count = 64;

    mutex: AtomicMutex = .{},
    session_locks: [session_lock_count]AtomicMutex = [_]AtomicMutex{.{}} ** session_lock_count,
    sessions: std.AutoHashMapUnmanaged(db_mod.types.TxnId, Session) = .empty,
    lease_renewal_candidates: std.AutoHashMapUnmanaged(db_mod.types.TxnId, SessionKind) = .empty,
    durable: ?*DurableSessionStore = null,
    lease_store: ?SessionLeaseStore = null,
    owner_lease_ttl_ns: ?u64 = null,
    max_savepoints: ?usize = null,
    max_sessions: ?usize = null,
    max_record_bytes: ?usize = null,
    durable_scope: SessionStoreScope = .node_local,
    owner_incarnation: u64 = 0,
    known_durable_session_count: ?usize = null,
    reserved_session_count: usize = 0,
    recovery_index_cursors: [2]?db_mod.types.TxnId = .{ null, null },
    recovery_audit_cursors: [2]?db_mod.types.TxnId = .{ null, null },
    recovery_namespace_cursor: SessionKind = .interactive,
    expiry_cleanup_cursors: [2]?SessionExpiryCursor = .{ null, null },
    expiry_namespace_cursor: SessionKind = .interactive,
    memory_recovery_scan_offset: usize = 0,
    lease_renewal_scan_offset: usize = 0,
    /// Test-only fault injection for deadline-aware heartbeat coverage. The
    /// transaction stripe serializes access, just like the lease mutation it
    /// precedes.
    lease_renewal_failures_for_test: usize = 0,
    lease_renewal_delay_ns_for_test: u64 = 0,
    lease_renewal_io_for_test: ?std.Io = null,
    lease_renewal_entered_for_test: ?*std.Io.Event = null,
    lease_renewal_release_for_test: ?*std.Io.Event = null,

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
            .owner_incarnation = newOwnerIncarnation(),
        };
    }

    pub fn deinit(self: *SessionRegistry, alloc: std.mem.Allocator) void {
        var it = self.sessions.iterator();
        while (it.next()) |entry| entry.value_ptr.deinit(alloc);
        self.sessions.deinit(alloc);
        self.lease_renewal_candidates.deinit(alloc);
        self.* = .{};
    }

    pub fn hasDurableStore(self: *const SessionRegistry) bool {
        return self.durable != null;
    }

    pub fn hasAtomicDurableStore(self: *const SessionRegistry) bool {
        return self.durable != null and
            self.lease_store != null and
            self.owner_lease_ttl_ns != null and
            self.owner_incarnation != 0;
    }

    /// A scope declaration alone is not a concurrency primitive. Distributed
    /// idempotency is safe only when receipt creation and ownership transfer
    /// are fenced atomically in the shared keyspace.
    pub fn hasAtomicClusterSharedStore(self: *const SessionRegistry) bool {
        return self.durable_scope == .cluster_shared and self.hasAtomicDurableStore();
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
            .owner_incarnation = self.owner_incarnation,
            .principal = if (principal) |value| try alloc.dupe(u8, value) else null,
            .begin_timestamp = now,
            .last_touched_timestamp = now,
            .sync_level = req.sync_level,
        };
        var session_owned = true;
        errdefer if (session_owned) session.deinit(alloc);
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
        if (self.shouldTrackLeaseRenewal(session)) self.lease_renewal_candidates.ensureUnusedCapacity(alloc, 1) catch |err| {
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
            const ttl_ms = sessionLeaseStorageTtlMs(self.owner_lease_ttl_ns.?);
            if (!(try self.durable.?.saveWithLease(session, null, null, now / std.time.ns_per_ms, ttl_ms, true, self.max_record_bytes, self.max_sessions))) return error.SessionLeaseLost;
        } else {
            try self.persistLocked(session);
        }
        self.mutex.lock();
        defer self.mutex.unlock();
        self.sessions.putAssumeCapacity(txn_id, session);
        if (self.shouldTrackLeaseRenewal(session))
            self.lease_renewal_candidates.putAssumeCapacity(txn_id, sessionKind(session));
        session_owned = false;
        self.reserved_session_count -= 1;
        reservation_active = false;
        if (self.known_durable_session_count) |count| self.known_durable_session_count = count + 1;
        return session.info();
    }

    /// Creates or returns the durable transaction session assigned to an
    /// external idempotency key. The caller derives `txn_id` from the
    /// authenticated principal, resource, and key. Creation and payload
    /// sealing share one durable write, so a crash cannot leave a reusable
    /// key that was accepted without its request binding.
    pub fn beginIdempotentForPrincipal(
        self: *SessionRegistry,
        alloc: std.mem.Allocator,
        txn_id: db_mod.types.TxnId,
        req: BeginRequest,
        owner_node_id: u64,
        principal: ?[]const u8,
        sealed_request: *const OwnedTransactionCommitRequest,
    ) !SessionInfo {
        const session_lock = self.sessionLock(txn_id);
        session_lock.lock();
        defer session_lock.unlock();

        const body_digest = try commitBodyDigest(alloc, sealed_request);

        if (try self.loadSessionCloneAssumeStripe(alloc, txn_id)) |existing_value| {
            var existing = existing_value;
            defer existing.deinit(alloc);
            if (!existing.idempotent_receipt or
                !principalsEqual(existing.principal, principal) or
                existing.sync_level != req.sync_level)
                return error.IdempotencyConflict;
            if (existing.commit_body_digest) |sealed_digest| {
                if (!std.mem.eql(u8, &sealed_digest, &body_digest))
                    return error.TransactionCommitRequestMismatch;
            }
            return existing.info();
        }

        const now = nextTxnTimestamp();
        var session: Session = .{
            .txn_id = txn_id,
            .owner_node_id = owner_node_id,
            .owner_incarnation = self.owner_incarnation,
            .principal = if (principal) |value| try alloc.dupe(u8, value) else null,
            .begin_timestamp = now,
            .last_touched_timestamp = now,
            .sync_level = req.sync_level,
            .commit_body_digest = body_digest,
            .idempotent_receipt = true,
        };
        var session_owned = true;
        errdefer if (session_owned) session.deinit(alloc);
        session.staged = try sealed_request.clone(alloc);

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
        if (self.shouldTrackLeaseRenewal(session)) self.lease_renewal_candidates.ensureUnusedCapacity(alloc, 1) catch |err| {
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
            const ttl_ms = sessionLeaseStorageTtlMs(self.owner_lease_ttl_ns.?);
            if (!(try self.durable.?.saveWithLease(session, null, null, now / std.time.ns_per_ms, ttl_ms, true, self.max_record_bytes, self.max_sessions))) {
                // Another API node won the create race. Load the immutable
                // binding and return it only when it is the same operation.
                var existing = (try self.durable.?.load(txn_id)) orelse return error.SessionLeaseLost;
                defer existing.deinit(self.durable.?.alloc);
                if (!existing.idempotent_receipt or
                    !principalsEqual(existing.principal, principal) or
                    existing.sync_level != req.sync_level)
                    return error.IdempotencyConflict;
                if (existing.commit_body_digest) |sealed_digest| {
                    if (!std.mem.eql(u8, &sealed_digest, &body_digest))
                        return error.TransactionCommitRequestMismatch;
                }
                session.deinit(alloc);
                session_owned = false;
                return existing.info();
            }
        } else {
            try self.persistLocked(session);
        }

        self.mutex.lock();
        self.sessions.putAssumeCapacity(txn_id, session);
        if (self.shouldTrackLeaseRenewal(session))
            self.lease_renewal_candidates.putAssumeCapacity(txn_id, sessionKind(session));
        self.reserved_session_count -= 1;
        if (self.known_durable_session_count) |count| self.known_durable_session_count = count + 1;
        self.mutex.unlock();
        reservation_active = false;
        session_owned = false;
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

    pub fn stage(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId, req: *const OwnedTransactionCommitRequest) !?SessionInfo {
        const session_lock = self.sessionLock(txn_id);
        session_lock.lock();
        defer session_lock.unlock();

        var candidate = (try self.loadSessionCloneAssumeStripe(alloc, txn_id)) orelse return null;
        errdefer candidate.deinit(alloc);
        if (candidate.idempotent_receipt) return error.IdempotentReceiptImmutable;
        if (candidate.commit_body_digest != null) return error.TransactionCommitSealed;
        if (candidate.staged == null) {
            candidate.staged = try req.clone(alloc);
        } else {
            try candidate.staged.?.mergeFrom(alloc, req);
        }
        touchSession(&candidate);
        try self.persistOwnedLocked(candidate);

        self.mutex.lock();
        defer self.mutex.unlock();
        const publish_target = self.sessions.getPtr(txn_id) orelse return error.SessionRemovedDuringMutation;
        try self.publishCandidateLocked(alloc, publish_target, &candidate);
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
        if (candidate.idempotent_receipt) return error.IdempotentReceiptImmutable;
        if (candidate.commit_body_digest != null) return error.TransactionCommitSealed;
        try upsertReadSnapshot(alloc, &candidate.read_snapshots, snapshot);
        if (candidate.staged == null) {
            candidate.staged = try req.clone(alloc);
        } else {
            try candidate.staged.?.mergeFrom(alloc, req);
        }
        touchSession(&candidate);
        try self.persistOwnedLocked(candidate);

        self.mutex.lock();
        defer self.mutex.unlock();
        const publish_target = self.sessions.getPtr(txn_id) orelse return error.SessionRemovedDuringMutation;
        try self.publishCandidateLocked(alloc, publish_target, &candidate);
        return publish_target.info();
    }

    pub fn cloneCommitRequest(
        self: *SessionRegistry,
        alloc: std.mem.Allocator,
        txn_id: db_mod.types.TxnId,
        extra_req: ?*const OwnedTransactionCommitRequest,
    ) !?OwnedTransactionCommitRequest {
        return try self.cloneCommitRequestForKind(alloc, txn_id, extra_req, .interactive);
    }

    pub fn cloneIdempotentCommitRequest(
        self: *SessionRegistry,
        alloc: std.mem.Allocator,
        txn_id: db_mod.types.TxnId,
        extra_req: ?*const OwnedTransactionCommitRequest,
    ) !?OwnedTransactionCommitRequest {
        return try self.cloneCommitRequestForKind(alloc, txn_id, extra_req, .idempotent_receipt);
    }

    fn cloneCommitRequestForKind(
        self: *SessionRegistry,
        alloc: std.mem.Allocator,
        txn_id: db_mod.types.TxnId,
        extra_req: ?*const OwnedTransactionCommitRequest,
        expected_kind: SessionKind,
    ) !?OwnedTransactionCommitRequest {
        const session_lock = self.sessionLock(txn_id);
        session_lock.lock();
        defer session_lock.unlock();
        var candidate = (try self.loadSessionCloneAssumeStripe(alloc, txn_id)) orelse return null;
        var candidate_owned = true;
        errdefer if (candidate_owned) candidate.deinit(alloc);
        const actual_kind: SessionKind = if (candidate.idempotent_receipt) .idempotent_receipt else .interactive;
        if (actual_kind != expected_kind) return error.SessionKindMismatch;
        // Builds predating atomic idempotent create-and-seal may have crashed
        // after publishing the key but before persisting its payload. No 2PC
        // execution can have started without a digest, so terminalize that
        // compatibility state as definitely not applied. Never let a later
        // request choose the payload for an already accepted orphan key.
        if (candidate.idempotent_receipt and candidate.commit_body_digest == null) {
            if (candidate.commit_execution_started or candidate.terminal_commit != null)
                return error.InvalidTransactionSessionRecord;
            try setIdempotentTerminalReceipt(
                alloc,
                &candidate,
                .not_applied,
                "idempotency_receipt_incomplete",
                "the key was created by an older server before its payload was sealed; use a new Idempotency-Key",
                false,
            );
            touchSession(&candidate);
            try self.persistOwnedLocked(candidate);
            self.mutex.lock();
            const publish_target = self.sessions.getPtr(txn_id) orelse {
                self.mutex.unlock();
                return error.SessionRemovedDuringMutation;
            };
            try self.publishCandidateLocked(alloc, publish_target, &candidate);
            self.mutex.unlock();
            candidate_owned = false;
            return error.UnsealedIdempotencyReceipt;
        }
        const body_digest = try commitBodyDigest(alloc, extra_req);
        if (candidate.commit_body_digest) |sealed_digest| {
            if (!std.mem.eql(u8, &sealed_digest, &body_digest)) return error.TransactionCommitRequestMismatch;
            const sealed = candidate.staged orelse return error.InvalidTransactionSessionRecord;
            var out = try sealed.clone(alloc);
            errdefer out.deinit(alloc);
            touchSession(&candidate);
            try self.persistOwnedLocked(candidate);
            self.mutex.lock();
            defer self.mutex.unlock();
            const publish_target = self.sessions.getPtr(txn_id) orelse return error.SessionRemovedDuringMutation;
            try self.publishCandidateLocked(alloc, publish_target, &candidate);
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
            return null;
        }
        if (candidate.staged) |*staged| staged.deinit(alloc);
        candidate.staged = try out.clone(alloc);
        candidate.commit_body_digest = body_digest;
        touchSession(&candidate);
        try self.persistOwnedLocked(candidate);
        self.mutex.lock();
        defer self.mutex.unlock();
        const publish_target = self.sessions.getPtr(txn_id) orelse return error.SessionRemovedDuringMutation;
        try self.publishCandidateLocked(alloc, publish_target, &candidate);
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

    pub const PreExecutionRejectionResult = enum {
        recorded,
        execution_started,
        terminal,
    };

    /// Atomically terminalizes a keyed request only while it is still before
    /// the durable execution boundary. A concurrent replay that observes a
    /// started or terminal request must reconcile that state instead of
    /// publishing a false non-application receipt.
    pub fn recordIdempotentPreExecutionRejection(
        self: *SessionRegistry,
        alloc: std.mem.Allocator,
        txn_id: db_mod.types.TxnId,
        code: []const u8,
        message: []const u8,
    ) !?PreExecutionRejectionResult {
        const session_lock = self.sessionLock(txn_id);
        session_lock.lock();
        defer session_lock.unlock();

        var candidate = (try self.loadSessionCloneAssumeStripe(alloc, txn_id)) orelse return null;
        errdefer candidate.deinit(alloc);
        if (!candidate.idempotent_receipt) return error.SessionKindMismatch;
        if (candidate.terminal_commit != null or candidate.idempotent_outcome != null) {
            candidate.deinit(alloc);
            return .terminal;
        }
        if (candidate.commit_execution_started) {
            candidate.deinit(alloc);
            return .execution_started;
        }
        try setIdempotentTerminalReceipt(alloc, &candidate, .not_applied, code, message, false);
        touchSession(&candidate);
        try self.persistOwnedLocked(candidate);

        self.mutex.lock();
        defer self.mutex.unlock();
        const publish_target = self.sessions.getPtr(txn_id) orelse return error.SessionRemovedDuringMutation;
        try self.publishCandidateLocked(alloc, publish_target, &candidate);
        return .recorded;
    }

    /// Persists the only non-commit terminal outcome permitted after execution
    /// starts. The caller must already hold durable coordinator evidence that
    /// the transaction chose abort.
    pub fn recordIdempotentDurableAbort(
        self: *SessionRegistry,
        alloc: std.mem.Allocator,
        txn_id: db_mod.types.TxnId,
        code: []const u8,
        message: []const u8,
    ) !?void {
        const session_lock = self.sessionLock(txn_id);
        session_lock.lock();
        defer session_lock.unlock();

        var candidate = (try self.loadSessionCloneAssumeStripe(alloc, txn_id)) orelse return null;
        errdefer candidate.deinit(alloc);
        if (!candidate.idempotent_receipt) return error.SessionKindMismatch;
        if (!candidate.commit_execution_started) return error.TransactionExecutionNotStarted;
        if (candidate.terminal_commit != null) return error.TransactionOutcomeMismatch;
        if (candidate.idempotent_outcome) |existing| {
            if (existing != .aborted) return error.TransactionOutcomeMismatch;
            candidate.deinit(alloc);
            return {};
        }
        try setIdempotentTerminalReceipt(alloc, &candidate, .aborted, code, message, false);
        touchSession(&candidate);
        try self.persistOwnedLocked(candidate);

        self.mutex.lock();
        defer self.mutex.unlock();
        const publish_target = self.sessions.getPtr(txn_id) orelse return error.SessionRemovedDuringMutation;
        try self.publishCandidateLocked(alloc, publish_target, &candidate);
        return {};
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
        if (candidate.idempotent_outcome != null) return error.TransactionOutcomeMismatch;
        if (candidate.terminal_commit) |*terminal| {
            // Replays of one transaction can complete out of order. Treat the
            // durable result as a monotonic join: phase-two debt may advance to
            // visibility debt and then committed, while repair and coordinator
            // acknowledgement facts are sticky. A stale attempt must never
            // turn a completed receipt back into retryable work.
            if (terminal.coordinator_group_id != null and coordinator_group_id != null and
                (terminal.coordinator_group_id != coordinator_group_id or
                    !optionalStringsEqual(terminal.coordinator_table_name, coordinator_table_name)))
            {
                return error.TransactionCoordinatorMismatch;
            }
            terminal.status = laterTerminalCommitStatus(terminal.status, status);
            terminal.repair_required = terminal.repair_required or repair_required;
            // Error-only outcomes cannot report coordinator metadata. Preserve
            // an established identity when such an older attempt arrives, and
            // allow recovery to fill an identity that was previously absent.
            if (terminal.coordinator_group_id == null) if (coordinator_group_id) |group_id| {
                terminal.coordinator_group_id = group_id;
                terminal.coordinator_table_name = try alloc.dupe(u8, coordinator_table_name.?);
            };
        } else {
            candidate.terminal_commit = .{
                .status = status,
                .repair_required = repair_required,
                .coordinator_group_id = coordinator_group_id,
                .coordinator_table_name = if (coordinator_table_name) |table_name| try alloc.dupe(u8, table_name) else null,
                .coordinator_acknowledged = false,
            };
        }
        touchSession(&candidate);
        try self.persistOwnedLocked(candidate);

        self.mutex.lock();
        defer self.mutex.unlock();
        const publish_target = self.sessions.getPtr(txn_id) orelse return error.SessionRemovedDuringMutation;
        try self.publishCandidateLocked(alloc, publish_target, &candidate);
        return {};
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
        if (candidate.idempotent_outcome != null) return error.TransactionOutcomeMismatch;
        if (candidate.commit_execution_started) {
            candidate.deinit(alloc);
            return {};
        }
        candidate.commit_execution_started = true;
        touchSession(&candidate);
        try self.persistOwnedLocked(candidate);
        self.mutex.lock();
        defer self.mutex.unlock();
        const publish_target = self.sessions.getPtr(txn_id) orelse return error.SessionRemovedDuringMutation;
        try self.publishCandidateLocked(alloc, publish_target, &candidate);
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
        try self.persistOwnedLocked(candidate);

        self.mutex.lock();
        defer self.mutex.unlock();
        const publish_target = self.sessions.getPtr(txn_id) orelse return error.SessionRemovedDuringMutation;
        try self.publishCandidateLocked(alloc, publish_target, &candidate);
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

    /// Returns a self-consistent terminal receipt without renewing or taking
    /// the session lease. The supplied request is revalidated under the same
    /// stripe lock, so ownership-independent replay cannot disclose a receipt
    /// for a colliding body.
    pub fn getIdempotentTerminalCommitSnapshot(
        self: *SessionRegistry,
        alloc: std.mem.Allocator,
        txn_id: db_mod.types.TxnId,
        supplied_request: *const OwnedTransactionCommitRequest,
    ) !?IdempotentTerminalCommitSnapshot {
        const session_lock = self.sessionLock(txn_id);
        session_lock.lock();
        defer session_lock.unlock();
        var session = (try self.loadSessionCloneAssumeStripe(alloc, txn_id)) orelse return null;
        defer session.deinit(alloc);
        if (!session.idempotent_receipt) return error.SessionKindMismatch;
        const terminal = session.terminal_commit orelse return null;
        const sealed_digest = session.commit_body_digest orelse return error.InvalidTransactionSessionRecord;
        const supplied_digest = try commitBodyDigest(alloc, supplied_request);
        if (!std.mem.eql(u8, &sealed_digest, &supplied_digest))
            return error.TransactionCommitRequestMismatch;
        const sealed_request = session.staged orelse return error.InvalidTransactionSessionRecord;
        var snapshot: IdempotentTerminalCommitSnapshot = .{
            .request = try sealed_request.clone(alloc),
            .terminal = undefined,
            .owns_mutation_lease = session.owner_incarnation == self.owner_incarnation,
        };
        errdefer snapshot.request.deinit(alloc);
        snapshot.terminal = try terminal.clone(alloc);
        return snapshot;
    }

    pub fn getIdempotentOutcome(
        self: *SessionRegistry,
        alloc: std.mem.Allocator,
        txn_id: db_mod.types.TxnId,
    ) !?IdempotentReceiptOutcome {
        const session_lock = self.sessionLock(txn_id);
        session_lock.lock();
        defer session_lock.unlock();
        var session = (try self.loadSessionCloneAssumeStripe(alloc, txn_id)) orelse return null;
        defer session.deinit(alloc);
        return session.idempotent_outcome;
    }

    pub fn getIdempotentTerminalReceipt(
        self: *SessionRegistry,
        alloc: std.mem.Allocator,
        txn_id: db_mod.types.TxnId,
    ) !?IdempotentTerminalReceipt {
        const session_lock = self.sessionLock(txn_id);
        session_lock.lock();
        defer session_lock.unlock();
        var session = (try self.loadSessionCloneAssumeStripe(alloc, txn_id)) orelse return null;
        defer session.deinit(alloc);
        const outcome = session.idempotent_outcome orelse return null;
        const code = session.idempotent_error_code orelse "transaction_not_applied";
        const message = session.idempotent_error_message orelse "the durable batch transaction was not applied";
        var receipt: IdempotentTerminalReceipt = .{
            .outcome = outcome,
            .code = try alloc.dupe(u8, code),
            .message = undefined,
            .retryable = session.idempotent_error_retryable,
        };
        errdefer alloc.free(receipt.code);
        receipt.message = try alloc.dupe(u8, message);
        return receipt;
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
            const index_after = self.recovery_index_cursors;
            const audit_after = self.recovery_audit_cursors;
            const first_kind = self.recovery_namespace_cursor;
            self.mutex.unlock();

            var next_index_after = index_after;
            var next_audit_after = audit_after;
            var kind = first_kind;
            for (0..2) |_| {
                const kind_index = sessionKindIndex(kind);
                if (pending.items.len < limit) {
                    const indexed = try durable.scanRecoveryIds(alloc, kind, index_after[kind_index], limit - pending.items.len);
                    defer alloc.free(indexed.ids);
                    try pending.appendSlice(alloc, indexed.ids);
                    next_index_after[kind_index] = indexed.next_after;
                }

                // Continuously audit a small bounded page per namespace. This
                // backfills records written before the compact index and
                // self-heals a missing index key even while the index is full.
                const audit_budget = @min(limit, 8);
                const audited = try durable.scanLegacyRecoveryIds(alloc, kind, audit_after[kind_index], audit_budget);
                defer alloc.free(audited.ids);
                next_audit_after[kind_index] = audited.next_after;
                for (audited.ids) |txn_id| {
                    try durable.refreshRecoveryIndex(txn_id, kind);
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
                kind = nextSessionKind(kind);
            }

            self.mutex.lock();
            self.recovery_index_cursors = next_index_after;
            self.recovery_audit_cursors = next_audit_after;
            self.recovery_namespace_cursor = nextSessionKind(first_kind);
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

        if (candidate.owner_node_id != owner_node_id or
            candidate.owner_incarnation != self.owner_incarnation)
        {
            const durable = self.durable orelse return null;
            // Scope controls whether a miss is authoritative; it does not
            // control whether an existing row can be safely transferred. The
            // atomic row+lease CAS fences restart recovery for node-local and
            // cluster-shared stores alike, including standalone owner node 0.
            if (self.lease_store == null or self.owner_lease_ttl_ns == null) return null;
            var adopted = try candidate.clone(alloc);
            var adopted_owned = true;
            defer if (adopted_owned) adopted.deinit(alloc);
            const expected_owner = adopted.owner_node_id;
            const expected_incarnation = adopted.owner_incarnation;
            adopted.owner_node_id = owner_node_id;
            adopted.owner_incarnation = self.owner_incarnation;
            touchSession(&adopted);
            const ttl_ms = sessionLeaseStorageTtlMs(self.owner_lease_ttl_ns.?);
            if (!(try durable.saveWithLease(adopted, expected_owner, expected_incarnation, now_ns / std.time.ns_per_ms, ttl_ms, true, self.max_record_bytes, null))) return null;
            candidate.deinit(durable.alloc);
            candidate = try adopted.clone(durable.alloc);
            try self.publishAdoptedCandidateAssumeStripe(alloc, txn_id, &adopted);
            adopted_owned = false;
        } else if (self.lease_store != null and self.owner_lease_ttl_ns != null) {
            try self.persistOwnedLockedAt(candidate, now_ns);
        }

        if (candidate.idempotent_outcome != null) return null;
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
                        .idempotent_receipt = candidate.idempotent_receipt,
                        .repair_required = terminal.repair_required,
                        .repair_handoff_needs_coordinator = repair_handoff_needs_coordinator,
                        .request = try request.clone(alloc),
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
        return .{ .commit = .{
            .txn_id = txn_id,
            .begin_timestamp = candidate.begin_timestamp,
            .sync_level = candidate.sync_level,
            .idempotent_receipt = candidate.idempotent_receipt,
            .repair_required = false,
            .repair_handoff_needs_coordinator = false,
            .request = try request.clone(alloc),
        } };
    }

    pub fn createSavepoint(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId) !?SavepointInfo {
        const session_lock = self.sessionLock(txn_id);
        session_lock.lock();
        defer session_lock.unlock();
        var candidate = (try self.loadSessionCloneAssumeStripe(alloc, txn_id)) orelse return null;
        errdefer candidate.deinit(alloc);
        if (candidate.idempotent_receipt) return error.IdempotentReceiptImmutable;
        if (candidate.commit_body_digest != null) return error.TransactionCommitSealed;
        if (self.max_savepoints) |limit| {
            if (candidate.savepoints.count() >= limit) return error.SavepointLimitExceeded;
        }
        const savepoint_id = candidate.next_savepoint_id;
        candidate.next_savepoint_id += 1;
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
        new_savepoint.read_snapshots = try cloneReadSnapshotMap(alloc, candidate.read_snapshots);
        try candidate.savepoints.put(alloc, savepoint_id, new_savepoint);
        savepoint_inserted = true;
        touchSession(&candidate);
        try self.persistOwnedLocked(candidate);
        self.mutex.lock();
        defer self.mutex.unlock();
        const publish_target = self.sessions.getPtr(txn_id) orelse return error.SessionRemovedDuringMutation;
        try self.publishCandidateLocked(alloc, publish_target, &candidate);
        return .{ .txn_id = txn_id, .savepoint_id = savepoint_id };
    }

    pub fn rollbackToSavepoint(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId, savepoint_id: u64) !?SavepointInfo {
        const session_lock = self.sessionLock(txn_id);
        session_lock.lock();
        defer session_lock.unlock();
        var candidate = (try self.loadSessionCloneAssumeStripe(alloc, txn_id)) orelse return null;
        errdefer candidate.deinit(alloc);
        if (candidate.idempotent_receipt) return error.IdempotentReceiptImmutable;
        if (candidate.commit_body_digest != null) return error.TransactionCommitSealed;
        if (!candidate.savepoints.contains(savepoint_id)) return null;
        const savepoint = candidate.savepoints.getPtr(savepoint_id).?;
        if (candidate.staged) |*staged| staged.deinit(alloc);
        candidate.staged = try savepoint.snapshot.clone(alloc);
        deinitReadSnapshotMap(alloc, &candidate.read_snapshots);
        candidate.read_snapshots = try cloneReadSnapshotMap(alloc, savepoint.read_snapshots);
        touchSession(&candidate);
        try self.persistOwnedLocked(candidate);
        self.mutex.lock();
        defer self.mutex.unlock();
        const publish_target = self.sessions.getPtr(txn_id) orelse return error.SessionRemovedDuringMutation;
        try self.publishCandidateLocked(alloc, publish_target, &candidate);
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
        return .{
            .status = try sessionStatusFromSession(self, alloc, &session),
            .tables = try sessionTableDetails(alloc, session.staged),
            .read_snapshots = try sessionReadSnapshots(alloc, &session),
            .savepoint_ids = try sessionSavepointIds(alloc, &session),
        };
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
                kind: SessionKind = .interactive,
                prefix: []const u8 = session_prefix,

                fn visit(raw: *anyopaque, key: []const u8, value: []const u8) anyerror!bool {
                    const scan: *@This() = @ptrCast(@alignCast(raw));
                    if (key.len <= scan.prefix.len) return true;
                    const txn_id = distributed_txn.parseTxnIdHex(key[scan.prefix.len..]) catch return true;
                    var session = decodeSessionRecord(scan.allocator, txn_id, value) catch return true;
                    defer session.deinit(scan.allocator);
                    if (sessionKind(session) != scan.kind) return true;
                    switch (scan.principal_filter) {
                        .all => {},
                        .principal => |principal| if (!principalsEqual(session.principal, principal)) return true,
                    }
                    try scan.statuses.append(scan.allocator, sessionStatusProjection(
                        &session,
                        scan.registry.max_savepoints,
                        true,
                        0,
                    ));
                    return true;
                }
            };
            var scan = Scan{
                .registry = self,
                .allocator = alloc,
                .statuses = &statuses,
                .principal_filter = principal_filter,
            };
            for ([_]SessionKind{ .interactive, .idempotent_receipt }) |kind| {
                scan.kind = kind;
                scan.prefix = sessionPrefix(kind);
                try durable.scanPrefixWithContext(scan.prefix, &scan, Scan.visit);
            }
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
        if (self.durable_scope == .cluster_shared) if (self.durable) |durable| {
            var session = (try durable.load(txn_id)) orelse return null;
            defer session.deinit(durable.alloc);
            return session.owner_node_id;
        };
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
        if (candidate.owner_node_id == owner_node_id and
            candidate.owner_incarnation == self.owner_incarnation)
        {
            try self.publishAdoptedCandidateAssumeStripe(alloc, txn_id, &candidate);
            return true;
        }
        const expected_owner = candidate.owner_node_id;
        const expected_incarnation = candidate.owner_incarnation;
        candidate.owner_node_id = owner_node_id;
        candidate.owner_incarnation = self.owner_incarnation;
        touchSession(&candidate);
        if (self.lease_store != null and self.owner_lease_ttl_ns != null) {
            const now_ns = nextTxnTimestamp();
            const ttl_ms = sessionLeaseStorageTtlMs(self.owner_lease_ttl_ns.?);
            if (!(try durable.saveWithLease(candidate, expected_owner, expected_incarnation, now_ns / std.time.ns_per_ms, ttl_ms, false, self.max_record_bytes, null))) return false;
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
        if (candidate.owner_node_id == owner_node_id and
            candidate.owner_incarnation == self.owner_incarnation)
        {
            try self.publishAdoptedCandidateAssumeStripe(alloc, txn_id, &candidate);
            return true;
        }
        const expected_owner = candidate.owner_node_id;
        const expected_incarnation = candidate.owner_incarnation;
        const effective_now = now_ns orelse nextTxnTimestamp();
        candidate.owner_node_id = owner_node_id;
        candidate.owner_incarnation = self.owner_incarnation;
        touchSession(&candidate);
        const ttl_ms = sessionLeaseStorageTtlMs(self.owner_lease_ttl_ns.?);
        if (!(try durable.saveWithLease(candidate, expected_owner, expected_incarnation, effective_now / std.time.ns_per_ms, ttl_ms, true, self.max_record_bytes, null))) return false;
        try self.publishAdoptedCandidateAssumeStripe(alloc, txn_id, &candidate);
        return true;
    }

    pub fn cleanupExpired(self: *SessionRegistry, alloc: std.mem.Allocator, cutoff_ns: u64) !usize {
        return try self.cleanupExpiredAt(alloc, cutoff_ns, nextTxnTimestamp());
    }

    fn cleanupExpiredAt(self: *SessionRegistry, alloc: std.mem.Allocator, cutoff_ns: u64, now_ns: u64) !usize {
        return try self.cleanupExpiredAtLimit(alloc, cutoff_ns, now_ns, 1024);
    }

    fn cleanupExpiredAtLimit(
        self: *SessionRegistry,
        alloc: std.mem.Allocator,
        cutoff_ns: u64,
        now_ns: u64,
        scan_limit: usize,
    ) !usize {
        var expired_ids = std.ArrayListUnmanaged(db_mod.types.TxnId).empty;
        defer expired_ids.deinit(alloc);
        if (self.durable) |durable| {
            self.mutex.lock();
            const cleanup_after = self.expiry_cleanup_cursors;
            const first_kind = self.expiry_namespace_cursor;
            self.mutex.unlock();
            var next_cleanup_after = cleanup_after;
            var kind = first_kind;
            for (0..2) |_| {
                if (expired_ids.items.len < scan_limit) {
                    const kind_index = sessionKindIndex(kind);
                    const indexed = try durable.scanExpiredIds(alloc, kind, cutoff_ns, cleanup_after[kind_index], scan_limit - expired_ids.items.len);
                    defer alloc.free(indexed.ids);
                    try expired_ids.appendSlice(alloc, indexed.ids);
                    next_cleanup_after[kind_index] = indexed.next_after;
                }
                kind = nextSessionKind(kind);
            }
            self.mutex.lock();
            self.expiry_cleanup_cursors = next_cleanup_after;
            self.expiry_namespace_cursor = nextSessionKind(first_kind);
            self.mutex.unlock();
        } else {
            self.mutex.lock();
            var loaded_it = self.sessions.iterator();
            while (loaded_it.next()) |entry| {
                if (entry.value_ptr.last_touched_timestamp < cutoff_ns) expired_ids.append(alloc, entry.key_ptr.*) catch |err| {
                    self.mutex.unlock();
                    return err;
                };
                if (expired_ids.items.len >= scan_limit) break;
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
            if (sessionRetentionPinned(current)) continue;
            if (current.terminal_commit) |terminal| {
                if (terminal.coordinator_group_id != null and !terminal.coordinator_acknowledged) continue;
            }
            if (!(try self.deleteExpiredPersistent(current, cutoff_ns, now_ns))) continue;
            self.mutex.lock();
            if (self.sessions.fetchRemove(txn_id)) |removed| {
                var session = removed.value;
                session.deinit(alloc);
                _ = self.lease_renewal_candidates.remove(txn_id);
                removed_count += 1;
            }
            self.mutex.unlock();
        }
        return removed_count;
    }

    pub const AbortInteractiveResult = enum {
        removed,
        missing,
        idempotent_receipt,
        execution_started,
        terminal_commit,
    };

    /// Atomically aborts only a still-mutable interactive session. Keeping the
    /// classification and deletion under the same stripe lock prevents an
    /// abort racing execution-start persistence from erasing recovery work.
    pub fn abortInteractive(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId) !AbortInteractiveResult {
        const session_lock = self.sessionLock(txn_id);
        session_lock.lock();
        defer session_lock.unlock();
        var current = (try self.loadSessionCloneAssumeStripe(alloc, txn_id)) orelse return .missing;
        defer current.deinit(alloc);
        if (current.idempotent_receipt) return .idempotent_receipt;
        if (current.terminal_commit != null) return .terminal_commit;
        if (current.commit_execution_started) return .execution_started;
        if (!(try self.deleteOwnedPersistent(current))) return error.SessionLeaseLost;
        self.removePublishedLocked(alloc, txn_id);
        return .removed;
    }

    /// Deletes an interactive session after the caller has durable evidence
    /// that it no longer requires recovery. Receipt records are categorically
    /// protected even if a future caller passes their ID here by mistake.
    pub fn removeInteractiveAfterDecision(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId) bool {
        const session_lock = self.sessionLock(txn_id);
        session_lock.lock();
        defer session_lock.unlock();
        var current = (self.loadSessionCloneAssumeStripe(alloc, txn_id) catch return false) orelse return false;
        defer current.deinit(alloc);
        if (current.idempotent_receipt) return false;
        if (!(self.deleteOwnedPersistent(current) catch return false)) return false;
        self.removePublishedLocked(alloc, txn_id);
        return true;
    }

    /// Compatibility alias for interactive transaction completion. The
    /// registry still enforces receipt protection at this boundary.
    pub fn remove(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId) bool {
        return self.removeInteractiveAfterDecision(alloc, txn_id);
    }

    fn removePublishedLocked(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        const removed = self.sessions.fetchRemove(txn_id) orelse return;
        _ = self.lease_renewal_candidates.remove(txn_id);
        var session = removed.value;
        session.deinit(alloc);
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

    fn publishCandidateLocked(self: *SessionRegistry, alloc: std.mem.Allocator, current: *Session, candidate: *Session) !void {
        try self.syncLeaseRenewalCandidateLocked(alloc, candidate.*);
        var previous = current.*;
        current.* = candidate.*;
        previous.deinit(alloc);
    }

    fn shouldTrackLeaseRenewal(self: *const SessionRegistry, session: Session) bool {
        return self.durable != null and
            self.lease_store != null and
            self.owner_lease_ttl_ns != null and
            session.owner_incarnation == self.owner_incarnation and
            sessionNeedsLeaseRenewal(session);
    }

    fn syncLeaseRenewalCandidateLocked(self: *SessionRegistry, alloc: std.mem.Allocator, session: Session) !void {
        if (self.shouldTrackLeaseRenewal(session)) {
            try self.lease_renewal_candidates.put(alloc, session.txn_id, sessionKind(session));
        } else {
            _ = self.lease_renewal_candidates.remove(session.txn_id);
        }
    }

    /// Drops a renewal projection only when it still describes the stale
    /// snapshot that lost the durable lease. The transaction stripe held by
    /// the caller prevents a concurrent local ownership mutation, while the
    /// conditional check keeps this safe if the projection was refreshed.
    fn forgetLostLeaseRenewalCandidateAssumeStripe(
        self: *SessionRegistry,
        txn_id: db_mod.types.TxnId,
        kind: SessionKind,
        owner_node_id: u64,
        owner_incarnation: u64,
    ) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        const session = self.sessions.getPtr(txn_id) orelse return;
        if (session.owner_node_id != owner_node_id or
            session.owner_incarnation != owner_incarnation or
            sessionKind(session.*) != kind) return;
        if (self.lease_renewal_candidates.remove(txn_id)) {
            // Hash-map mutation invalidates an ordinal iterator position.
            self.lease_renewal_scan_offset = 0;
        }
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
            self.publishCandidateLocked(alloc, current, candidate) catch |err| {
                self.mutex.unlock();
                return err;
            };
            self.mutex.unlock();
            return;
        }
        self.syncLeaseRenewalCandidateLocked(alloc, candidate.*) catch |err| {
            self.mutex.unlock();
            return err;
        };
        self.sessions.put(alloc, txn_id, candidate.*) catch |err| {
            _ = self.lease_renewal_candidates.remove(txn_id);
            self.mutex.unlock();
            return err;
        };
        candidate.* = undefined;
        self.mutex.unlock();
    }

    fn persistLocked(self: *SessionRegistry, session: Session) !void {
        if (self.durable) |durable| try durable.save(session, self.max_record_bytes);
    }

    /// Publishes the session mutation and renews its incarnation-specific lease
    /// in one storage transaction. A process paused past lease expiry can no
    /// longer overwrite a newer owner's record when it resumes.
    fn persistOwnedLocked(self: *SessionRegistry, session: Session) !void {
        return try self.persistOwnedLockedAt(session, nextTxnTimestamp());
    }

    fn persistOwnedLockedAt(self: *SessionRegistry, session: Session, now_ns: u64) !void {
        const durable = self.durable orelse return;
        if (self.lease_store == null or self.owner_lease_ttl_ns == null) {
            return try durable.save(session, self.max_record_bytes);
        }
        if (session.owner_incarnation != self.owner_incarnation) return error.SessionLeaseLost;
        const ttl_ms = sessionLeaseStorageTtlMs(self.owner_lease_ttl_ns.?);
        if (!(try durable.saveWithLease(
            session,
            session.owner_node_id,
            session.owner_incarnation,
            now_ns / std.time.ns_per_ms,
            ttl_ms,
            true,
            self.max_record_bytes,
            null,
        ))) return error.SessionLeaseLost;
    }

    fn deletePersistent(self: *SessionRegistry, txn_id: db_mod.types.TxnId) !void {
        if (self.durable) |durable| {
            try durable.delete(txn_id);
            self.mutex.lock();
            defer self.mutex.unlock();
            if (self.known_durable_session_count) |count| self.known_durable_session_count = count -| 1;
        }
    }

    fn deleteOwnedPersistent(self: *SessionRegistry, session: Session) !bool {
        const durable = self.durable orelse return true;
        if (self.lease_store != null and self.owner_lease_ttl_ns != null) {
            if (session.owner_incarnation != self.owner_incarnation) return false;
            if (!(try durable.deleteWithLease(
                session.txn_id,
                session.owner_node_id,
                session.owner_incarnation,
            ))) return false;
            self.mutex.lock();
            defer self.mutex.unlock();
            if (self.known_durable_session_count) |count| self.known_durable_session_count = count -| 1;
            return true;
        }
        try self.deletePersistent(session.txn_id);
        return true;
    }

    fn deleteExpiredPersistent(self: *SessionRegistry, session: Session, cutoff_ns: u64, now_ns: u64) !bool {
        const durable = self.durable orelse return true;
        if (self.lease_store != null and self.owner_lease_ttl_ns != null) {
            const deleted = if (session.owner_incarnation == self.owner_incarnation)
                try durable.deleteWithLease(
                    session.txn_id,
                    session.owner_node_id,
                    session.owner_incarnation,
                )
            else
                try durable.deleteExpiredWithLease(
                    session.txn_id,
                    session.owner_node_id,
                    session.owner_incarnation,
                    session.last_touched_timestamp,
                    cutoff_ns,
                    now_ns / std.time.ns_per_ms,
                );
            if (!deleted) return false;
            self.mutex.lock();
            defer self.mutex.unlock();
            if (self.known_durable_session_count) |count| self.known_durable_session_count = count -| 1;
            return true;
        }
        try self.deletePersistent(session.txn_id);
        return true;
    }

    fn ensureSessionCapacityLocked(self: *SessionRegistry) !void {
        const limit = self.max_sessions orelse return;
        // Cluster-shared stores enforce capacity in the same backend write
        // transaction that creates the durable session. A cached process-local
        // count cannot safely admit work across API nodes.
        if (self.durable != null and self.durable_scope == .cluster_shared) return;
        const count = if (self.durable != null) self.known_durable_session_count orelse return error.SessionCapacityUnavailable else self.sessions.count();
        if (count + self.reserved_session_count >= limit) return error.SessionLimitExceeded;
    }

    fn initializeDurableSessionCount(self: *SessionRegistry) !void {
        const durable = self.durable orelse return;
        if (self.durable_scope == .cluster_shared) return;
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
        if (self.durable_scope == .cluster_shared) if (self.durable) |durable| {
            var loaded = (try durable.load(txn_id)) orelse return null;
            defer loaded.deinit(durable.alloc);
            var cached = try loaded.clone(alloc);
            var cached_owned = true;
            defer if (cached_owned) cached.deinit(alloc);
            var result = try loaded.clone(alloc);
            errdefer result.deinit(alloc);

            // Shared storage is authoritative. Refresh stale process-local
            // ownership before the caller attempts a fenced mutation.
            self.mutex.lock();
            if (self.sessions.getPtr(txn_id)) |current| {
                self.publishCandidateLocked(alloc, current, &cached) catch |err| {
                    self.mutex.unlock();
                    return err;
                };
            } else {
                self.syncLeaseRenewalCandidateLocked(alloc, cached) catch |err| {
                    self.mutex.unlock();
                    return err;
                };
                self.sessions.put(alloc, txn_id, cached) catch |err| {
                    _ = self.lease_renewal_candidates.remove(txn_id);
                    self.mutex.unlock();
                    return err;
                };
            }
            cached_owned = false;
            self.mutex.unlock();
            return result;
        };
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
        try self.syncLeaseRenewalCandidateLocked(alloc, loaded);
        self.sessions.put(alloc, txn_id, loaded) catch |err| {
            _ = self.lease_renewal_candidates.remove(txn_id);
            return err;
        };
        loaded_owned = false;
        return try self.sessions.getPtr(txn_id).?.clone(alloc);
    }

    pub const LeaseRenewalBatch = struct {
        renewed: usize,
        has_more: bool,
    };

    pub fn renewOwnedLeaseBatch(
        self: *SessionRegistry,
        owner_node_id: u64,
        now_ns: u64,
        scan_limit: usize,
    ) !LeaseRenewalBatch {
        const Renewal = struct {
            txn_id: db_mod.types.TxnId,
            kind: SessionKind,
        };
        const durable = self.durable orelse return .{ .renewed = 0, .has_more = false };
        var renewals = std.ArrayListUnmanaged(Renewal).empty;
        defer renewals.deinit(durable.alloc);
        self.mutex.lock();
        if (self.lease_store == null or self.owner_lease_ttl_ns == null) {
            self.mutex.unlock();
            return .{ .renewed = 0, .has_more = false };
        }
        var it = self.lease_renewal_candidates.iterator();
        var scan_offset = self.lease_renewal_scan_offset;
        var skipped: usize = 0;
        while (skipped < scan_offset and it.next() != null) skipped += 1;
        if (skipped < scan_offset) {
            scan_offset = 0;
            it = self.lease_renewal_candidates.iterator();
        }
        var scanned: usize = 0;
        while (scanned < @max(scan_limit, 1)) : (scanned += 1) {
            const entry = it.next() orelse break;
            renewals.append(durable.alloc, .{
                .txn_id = entry.key_ptr.*,
                .kind = entry.value_ptr.*,
            }) catch |err| {
                self.mutex.unlock();
                return err;
            };
        }
        const has_more = scanned == @max(scan_limit, 1) and it.next() != null;
        self.lease_renewal_scan_offset = if (has_more) scan_offset +| scanned else 0;
        self.mutex.unlock();

        var renewed: usize = 0;
        var deferred_error: ?anyerror = null;
        for (renewals.items) |renewal| {
            const session_lock = self.sessionLock(renewal.txn_id);
            session_lock.lock();
            defer session_lock.unlock();
            self.mutex.lock();
            const incarnation = if (self.sessions.getPtr(renewal.txn_id)) |session| blk: {
                if (session.owner_node_id != owner_node_id or
                    session.owner_incarnation != self.owner_incarnation or
                    sessionKind(session.*) != renewal.kind) break :blk null;
                break :blk session.owner_incarnation;
            } else null;
            self.mutex.unlock();
            if (incarnation == null) continue;
            // Each durable write receives a fresh wall-clock timestamp. One
            // slow early renewal must not consume the lease lifetime of every
            // later item in this bounded batch.
            const renewal_now_ns = @max(now_ns, platform_time.realtimeNs());
            _ = self.renewLeaseLockedAt(renewal.txn_id, renewal.kind, owner_node_id, incarnation.?, renewal_now_ns) catch |err| {
                // A stale cached owner is local work to forget, not a reason to
                // suppress renewal for the rest of the batch. Other storage
                // failures are reported after every independent lease had its
                // chance to advance.
                if (err == error.SessionLeaseLost) {
                    self.forgetLostLeaseRenewalCandidateAssumeStripe(
                        renewal.txn_id,
                        renewal.kind,
                        owner_node_id,
                        incarnation.?,
                    );
                } else if (deferred_error == null) {
                    deferred_error = err;
                }
                continue;
            };
            renewed += 1;
        }
        if (deferred_error) |err| return err;
        return .{ .renewed = renewed, .has_more = has_more };
    }

    pub fn renewOwnedLeases(self: *SessionRegistry, owner_node_id: u64, now_ns: u64) !usize {
        var renewed: usize = 0;
        while (true) {
            const batch = try self.renewOwnedLeaseBatch(owner_node_id, now_ns, 256);
            renewed +|= batch.renewed;
            if (!batch.has_more) return renewed;
        }
    }

    /// Renews one claimed session using the exact process incarnation. Long
    /// recovery operations use this independently of the periodic registry
    /// scan so their ownership cannot expire while storage work is in flight.
    pub fn renewOwnedLease(
        self: *SessionRegistry,
        txn_id: db_mod.types.TxnId,
        owner_node_id: u64,
        now_ns: u64,
    ) !u64 {
        const session_lock = self.sessionLock(txn_id);
        session_lock.lock();
        defer session_lock.unlock();
        const durable = self.durable orelse return std.math.maxInt(u64);
        var session = (try self.loadSessionCloneAssumeStripe(durable.alloc, txn_id)) orelse return error.SessionLeaseLost;
        defer session.deinit(durable.alloc);
        if (session.owner_node_id != owner_node_id or session.owner_incarnation != self.owner_incarnation)
            return error.SessionLeaseLost;
        if (self.lease_renewal_failures_for_test > 0) {
            self.lease_renewal_failures_for_test -= 1;
            return error.InjectedSessionLeaseRenewalFailure;
        }
        return try self.renewLeaseLockedAt(txn_id, sessionKind(session), owner_node_id, self.owner_incarnation, now_ns);
    }

    /// Establishes a fresh lease window before external work starts. A slow
    /// storage call may return with too little of the requested window left;
    /// retrying is safe here because the caller has not begun that work yet.
    pub fn confirmOwnedLeaseRunway(
        self: *SessionRegistry,
        txn_id: db_mod.types.TxnId,
        owner_node_id: u64,
        minimum_runway_ns: u64,
    ) !u64 {
        for (0..3) |_| {
            const expires_at_ns = self.renewOwnedLease(
                txn_id,
                owner_node_id,
                platform_time.realtimeNs(),
            ) catch |err| switch (err) {
                // This path runs before external work. If a just-persisted
                // claim expired during slow I/O, atomically re-establish it by
                // CASing the durable session incarnation and lease together.
                error.SessionLeaseLost => try self.reacquireOwnedLeaseBeforeWork(txn_id, owner_node_id),
                else => return err,
            };
            const observed_at_ns = platform_time.realtimeNs();
            if (observed_at_ns < expires_at_ns and
                expires_at_ns - observed_at_ns >= minimum_runway_ns) return expires_at_ns;
        }
        return error.SessionLeaseRunwayUnavailable;
    }

    fn reacquireOwnedLeaseBeforeWork(
        self: *SessionRegistry,
        txn_id: db_mod.types.TxnId,
        owner_node_id: u64,
    ) !u64 {
        const session_lock = self.sessionLock(txn_id);
        session_lock.lock();
        defer session_lock.unlock();
        const durable = self.durable orelse return std.math.maxInt(u64);
        var session = (try self.loadSessionCloneAssumeStripe(durable.alloc, txn_id)) orelse return error.SessionLeaseLost;
        defer session.deinit(durable.alloc);
        if (session.owner_node_id != owner_node_id or
            session.owner_incarnation != self.owner_incarnation) return error.SessionLeaseLost;
        const ttl_ns = self.owner_lease_ttl_ns orelse return std.math.maxInt(u64);
        const ttl_ms = sessionLeaseStorageTtlMs(ttl_ns);
        const now_ns = platform_time.realtimeNs();
        try self.delayLeaseMutationForTest();
        if (!(try durable.saveWithLease(
            session,
            owner_node_id,
            self.owner_incarnation,
            now_ns / std.time.ns_per_ms,
            ttl_ms,
            true,
            self.max_record_bytes,
            null,
        ))) return error.SessionLeaseLost;
        return leaseExpiryNs(now_ns / std.time.ns_per_ms +| ttl_ms);
    }

    fn delayLeaseMutationForTest(self: *SessionRegistry) !void {
        const io = self.lease_renewal_io_for_test orelse return;
        if (self.lease_renewal_entered_for_test) |entered| entered.set(io);
        if (self.lease_renewal_release_for_test) |release| while (!release.isSet()) {
            release.waitTimeout(io, .{
                .duration = .{ .raw = std.Io.Duration.fromMilliseconds(100), .clock = .awake },
            }) catch |err| switch (err) {
                error.Timeout => {},
                error.Canceled => return err,
            };
        };
        if (self.lease_renewal_delay_ns_for_test > 0) try io.sleep(
            std.Io.Duration.fromNanoseconds(self.lease_renewal_delay_ns_for_test),
            .awake,
        );
    }

    fn renewLeaseLockedAt(self: *SessionRegistry, txn_id: db_mod.types.TxnId, kind: SessionKind, owner_node_id: u64, owner_incarnation: u64, now_ns: u64) !u64 {
        const lease_store = self.lease_store orelse return std.math.maxInt(u64);
        const ttl_ns = self.owner_lease_ttl_ns orelse return std.math.maxInt(u64);
        const effective_now_ns = @max(now_ns, platform_time.realtimeNs());
        const now_ms = effective_now_ns / std.time.ns_per_ms;
        const ttl_ms = sessionLeaseStorageTtlMs(ttl_ns);
        if (owner_incarnation != self.owner_incarnation) return error.SessionLeaseLost;
        try self.delayLeaseMutationForTest();
        if (!(try lease_store.renew(txn_id, kind, owner_node_id, owner_incarnation, now_ms, ttl_ms))) return error.SessionLeaseLost;
        return leaseExpiryNs(now_ms +| ttl_ms);
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

/// Keeps one active session fenced while external work is in flight. A
/// transient storage failure is retried while the last confirmed lease is
/// still valid; only an explicit ownership mismatch or exhaustion of that
/// deadline publishes `lost`.
pub const OwnedSessionLeaseHeartbeat = struct {
    registry: *SessionRegistry,
    io: std.Io,
    txn_id: db_mod.types.TxnId,
    owner_node_id: u64,
    interval_ns: u64,
    stop_event: std.Io.Event = .unset,
    lost: std.atomic.Value(bool) = .init(false),
    expires_at_ns: std.atomic.Value(u64),

    pub fn init(
        registry: *SessionRegistry,
        io: std.Io,
        txn_id: db_mod.types.TxnId,
        owner_node_id: u64,
        interval_ns: u64,
        confirmed_expires_at_ns: u64,
    ) OwnedSessionLeaseHeartbeat {
        return .{
            .registry = registry,
            .io = io,
            .txn_id = txn_id,
            .owner_node_id = owner_node_id,
            .interval_ns = interval_ns,
            .expires_at_ns = .init(confirmed_expires_at_ns),
        };
    }

    pub fn run(self: *@This()) void {
        while (!self.stop_event.isSet()) {
            const before_wait_ns = platform_time.realtimeNs();
            const confirmed_expiry_ns = self.expires_at_ns.load(.acquire);
            if (before_wait_ns >= confirmed_expiry_ns) {
                if (self.stop_event.isSet()) return;
                self.lost.store(true, .release);
                return;
            }
            // After a slow renewal or transient failure, retry inside the
            // remaining confirmed window instead of sleeping a full interval
            // past it. Half the remaining window retains one retry opportunity.
            const remaining_ns = confirmed_expiry_ns - before_wait_ns;
            const wait_ns = @min(self.interval_ns, @max(@as(u64, 1), remaining_ns / 2));
            self.stop_event.waitTimeout(self.io, .{
                .duration = .{ .raw = std.Io.Duration.fromNanoseconds(wait_ns), .clock = .awake },
            }) catch |err| switch (err) {
                error.Timeout => {},
                error.Canceled => {
                    if (self.stop_event.isSet()) return;
                    self.lost.store(true, .release);
                    return;
                },
            };
            if (self.stop_event.isSet()) return;
            const renewal_started_ns = platform_time.realtimeNs();
            const renewed_expiry_ns = self.registry.renewOwnedLease(self.txn_id, self.owner_node_id, renewal_started_ns) catch |err| {
                const observed_at_ns = platform_time.realtimeNs();
                if (err == error.SessionLeaseLost or
                    observed_at_ns >= self.expires_at_ns.load(.acquire))
                {
                    self.lost.store(true, .release);
                    return;
                }
                std.log.warn("session lease heartbeat renewal deferred txn_id={x} err={s}", .{ self.txn_id, @errorName(err) });
                continue;
            };
            const observed_at_ns = platform_time.realtimeNs();
            // Strict renewal proves that the durable owner still held a live
            // lease at the storage boundary. If the newly written window was
            // consumed before the call returned, recovery must take over under
            // the same transaction ID.
            if (observed_at_ns >= renewed_expiry_ns) {
                self.lost.store(true, .release);
                return;
            }
            self.expires_at_ns.store(renewed_expiry_ns, .release);
        }
    }

    pub fn stop(self: *@This()) void {
        self.stop_event.set(self.io);
    }

    pub fn isLost(self: *const @This()) bool {
        // The renewal task may itself be blocked in storage. Ownership expiry
        // must therefore be observable directly from the confirmed deadline,
        // without waiting for that task to return and publish `lost`.
        return self.lost.load(.acquire) or
            platform_time.realtimeNs() >= self.expires_at_ns.load(.acquire);
    }

    pub fn isConfirmedAt(self: *const @This(), now_ns: u64) bool {
        return !self.lost.load(.acquire) and now_ns < self.expires_at_ns.load(.acquire);
    }
};

/// Combines request cancellation with the ownership fence used by retained
/// transaction execution. Implementations that support cancellation can stop
/// visibility waits promptly when the lease heartbeat loses authority.
pub const OwnedSessionLeaseCancellation = struct {
    upstream: db_mod.types.CancellationToken = .none,
    heartbeat: *const OwnedSessionLeaseHeartbeat,

    pub fn token(self: *const @This()) db_mod.types.CancellationToken {
        return .{
            .ptr = self,
            .is_cancelled_fn = isCancelled,
        };
    }

    fn isCancelled(ptr: *const anyopaque) bool {
        const self: *const @This() = @ptrCast(@alignCast(ptr));
        return self.upstream.isCancelled() or self.heartbeat.isLost();
    }
};

pub fn sessionLeaseExpirationNs(ttl_ns: u64, now_ns: u64) u64 {
    const duration_ns = sessionLeaseStorageTtlMs(ttl_ns) *| std.time.ns_per_ms;
    return (now_ns / std.time.ns_per_ms) *| std.time.ns_per_ms +| duration_ns;
}

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
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{});
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
        (try std.json.parseFromSlice(std.json.Value, alloc, document_json, .{})).value
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
        .outcome = status.outcome,
        .repair_required = status.repair_required,
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
            (try std.json.parseFromSlice(std.json.Value, alloc, document_json, .{})).value
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
        .outcome = status.outcome,
        .repair_required = status.repair_required,
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
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{});
    defer parsed.deinit();
    return try parseCommitValue(alloc, parsed.value);
}

pub fn parseMultiBatchRequest(alloc: std.mem.Allocator, body: []const u8) !OwnedTransactionCommitRequest {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{});
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

/// Promotes the legacy one-table batch contract into the durable transaction
/// request used by idempotent batch execution. All bytes are cloned because
/// the session store seals and persists the request beyond the HTTP body's
/// lifetime.
pub fn ownedRequestFromBatch(
    alloc: std.mem.Allocator,
    table_name: []const u8,
    batch: batch_api.OwnedBatchRequest,
) !OwnedTransactionCommitRequest {
    var request: OwnedTransactionCommitRequest = .{ .sync_level = batch.req.sync_level };
    errdefer request.deinit(alloc);
    const owned_table_name = try alloc.dupe(u8, table_name);
    errdefer alloc.free(owned_table_name);
    var owned_batch = try cloneBatchRequest(alloc, batch);
    errdefer owned_batch.deinit(alloc);
    request.tables = try alloc.alloc(TableCommitRequest, 1);
    request.tables[0] = .{
        .table_name = owned_table_name,
        .batch = owned_batch,
    };
    return request;
}

pub fn encodeCommitRequest(alloc: std.mem.Allocator, req: OwnedTransactionCommitRequest) ![]u8 {
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
        .kind = classifyConflictKind(outcome.message),
        .retryable = isRetryableConflict(outcome.message),
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
                try appendJsonString(alloc, &out, db_mod.transform.transformOpText(op.op));
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
    out.writes = try alloc.alloc(db_mod.types.BatchWrite, batch.writes.len);
    var write_count: usize = 0;
    errdefer {
        for (out.writes[0..write_count]) |write| {
            alloc.free(@constCast(write.key));
            alloc.free(@constCast(write.value));
        }
        if (out.writes.len > 0) alloc.free(out.writes);
    }
    for (batch.writes) |write| {
        out.writes[write_count] = .{
            .key = try alloc.dupe(u8, write.key),
            .value = try alloc.dupe(u8, write.value),
        };
        write_count += 1;
    }
    out.deletes = try alloc.alloc([]const u8, batch.deletes.len);
    var delete_count: usize = 0;
    errdefer {
        for (out.deletes[0..delete_count]) |key| alloc.free(key);
        if (out.deletes.len > 0) alloc.free(out.deletes);
    }
    for (batch.deletes) |key| {
        out.deletes[delete_count] = try alloc.dupe(u8, key);
        delete_count += 1;
    }
    out.transforms = try alloc.alloc(db_mod.types.DocumentTransform, batch.transforms.len);
    var transform_count: usize = 0;
    errdefer {
        for (out.transforms[0..transform_count]) |transform| {
            alloc.free(@constCast(transform.key));
            for (transform.operations) |op| {
                alloc.free(@constCast(op.path));
                if (op.value_json) |value_json| alloc.free(@constCast(value_json));
            }
            if (transform.operations.len > 0) alloc.free(transform.operations);
        }
        if (out.transforms.len > 0) alloc.free(out.transforms);
    }
    for (batch.transforms) |transform| {
        const ops = try alloc.alloc(db_mod.types.TransformOp, transform.operations.len);
        errdefer alloc.free(ops);
        for (transform.operations, 0..) |op, i| {
            ops[i] = .{
                .op = op.op,
                .path = try alloc.dupe(u8, op.path),
                .value_json = if (op.value_json) |value_json| try alloc.dupe(u8, value_json) else null,
            };
        }
        out.transforms[transform_count] = .{
            .key = try alloc.dupe(u8, transform.key),
            .operations = ops,
            .upsert = transform.upsert,
        };
        transform_count += 1;
    }
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

fn findTableIndex(tables: []const TableCommitRequest, table_name: []const u8) ?usize {
    for (tables, 0..) |table, i| {
        if (std.mem.eql(u8, table.table_name, table_name)) return i;
    }
    return null;
}

fn clearPreparedWrites(table: *TableCommitRequest, alloc: std.mem.Allocator) void {
    if (table.txn_writes.len > 0) {
        alloc.free(table.txn_writes);
        table.txn_writes = &.{};
    }
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
        }
        alloc.free(next);
    }
    for (batch.writes) |write| {
        next[copied] = .{
            .key = try alloc.dupe(u8, write.key),
            .value = try alloc.dupe(u8, write.value),
        };
        copied += 1;
    }
    for (writes) |write| {
        next[copied] = .{
            .key = try alloc.dupe(u8, write.key),
            .value = try alloc.dupe(u8, write.value),
        };
        copied += 1;
    }
    for (batch.writes) |write| {
        alloc.free(@constCast(write.key));
        alloc.free(@constCast(write.value));
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

fn appendBatchTransforms(
    alloc: std.mem.Allocator,
    batch: *batch_api.OwnedBatchRequest,
    transforms: []const db_mod.types.DocumentTransform,
) !void {
    if (transforms.len == 0) return;
    const old_len = batch.transforms.len;
    var next = try alloc.alloc(db_mod.types.DocumentTransform, old_len + transforms.len);
    var copied: usize = 0;
    errdefer {
        for (next[0..copied]) |transform| {
            alloc.free(@constCast(transform.key));
            for (transform.operations) |op| {
                alloc.free(@constCast(op.path));
                if (op.value_json) |value_json| alloc.free(@constCast(value_json));
            }
            if (transform.operations.len > 0) alloc.free(transform.operations);
        }
        alloc.free(next);
    }
    for (batch.transforms) |transform| {
        const ops = try alloc.alloc(db_mod.types.TransformOp, transform.operations.len);
        errdefer alloc.free(ops);
        for (transform.operations, 0..) |op, i| {
            ops[i] = .{
                .op = op.op,
                .path = try alloc.dupe(u8, op.path),
                .value_json = if (op.value_json) |value_json| try alloc.dupe(u8, value_json) else null,
            };
        }
        next[copied] = .{
            .key = try alloc.dupe(u8, transform.key),
            .operations = ops,
            .upsert = transform.upsert,
        };
        copied += 1;
    }
    for (transforms) |transform| {
        const ops = try alloc.alloc(db_mod.types.TransformOp, transform.operations.len);
        errdefer alloc.free(ops);
        for (transform.operations, 0..) |op, i| {
            ops[i] = .{
                .op = op.op,
                .path = try alloc.dupe(u8, op.path),
                .value_json = if (op.value_json) |value_json| try alloc.dupe(u8, value_json) else null,
            };
        }
        next[copied] = .{
            .key = try alloc.dupe(u8, transform.key),
            .operations = ops,
            .upsert = transform.upsert,
        };
        copied += 1;
    }
    for (batch.transforms) |transform| {
        alloc.free(@constCast(transform.key));
        for (transform.operations) |op| {
            alloc.free(@constCast(op.path));
            if (op.value_json) |value_json| alloc.free(@constCast(value_json));
        }
        if (transform.operations.len > 0) alloc.free(transform.operations);
    }
    if (batch.transforms.len > 0) alloc.free(batch.transforms);
    batch.transforms = next;
}

fn appendPredicates(
    alloc: std.mem.Allocator,
    predicates: *std.ArrayListUnmanaged(db_mod.types.TransactionVersionPredicate),
    extras: []const db_mod.types.TransactionVersionPredicate,
) !void {
    if (extras.len == 0) return;
    try predicates.ensureTotalCapacity(alloc, predicates.items.len + extras.len);
    for (extras) |predicate| {
        predicates.appendAssumeCapacity(.{
            .key = try alloc.dupe(u8, predicate.key),
            .expected_version = predicate.expected_version,
        });
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

fn setIdempotentTerminalReceipt(
    alloc: std.mem.Allocator,
    session: *Session,
    outcome: IdempotentReceiptOutcome,
    code: []const u8,
    message: []const u8,
    retryable: bool,
) !void {
    std.debug.assert(session.idempotent_outcome == null);
    std.debug.assert(session.idempotent_error_code == null);
    std.debug.assert(session.idempotent_error_message == null);
    session.idempotent_outcome = outcome;
    session.idempotent_error_code = try alloc.dupe(u8, code);
    session.idempotent_error_message = try alloc.dupe(u8, message);
    session.idempotent_error_retryable = retryable;
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

fn sessionStatusProjection(
    session: *const Session,
    max_savepoints: ?usize,
    durable: bool,
    lease_expires_at: u64,
) SessionStatus {
    const counts = stagedCounts(session.staged);
    const savepoint_count = session.savepoints.count();
    return .{
        .txn_id = session.txn_id,
        .owner_node_id = session.owner_node_id,
        .begin_timestamp = session.begin_timestamp,
        .last_touched_timestamp = session.last_touched_timestamp,
        .lease_expires_at = lease_expires_at,
        .sync_level = session.sync_level,
        .staged_table_count = counts.tables,
        .staged_read_count = counts.reads,
        .staged_write_count = counts.writes,
        .staged_delete_count = counts.deletes,
        .read_snapshot_count = session.read_snapshots.count(),
        .savepoint_count = savepoint_count,
        .savepoint_limit = max_savepoints,
        .remaining_savepoints = if (max_savepoints) |limit| limit - @min(limit, savepoint_count) else null,
        .durable = durable,
        .outcome = if (session.idempotent_outcome) |outcome|
            outcome.text()
        else if (session.terminal_commit) |terminal|
            terminalCommitResponseStatus(effectiveTerminalCommitStatus(terminal), terminal.repair_required)
        else if (session.commit_execution_started)
            "unknown"
        else
            "not_applied",
        .repair_required = if (session.terminal_commit) |terminal| terminal.repair_required else false,
    };
}

fn sessionStatusFromSession(self: *SessionRegistry, alloc: std.mem.Allocator, session: *const Session) !SessionStatus {
    return sessionStatusProjection(
        session,
        self.max_savepoints,
        self.durable != null,
        try self.loadLeaseExpiryLocked(alloc, session.txn_id),
    );
}

/// Stable, non-secret transaction identity for a public idempotency scope.
/// Length-prefixing prevents ambiguous concatenations. Payload bytes are not
/// included: the durable session's sealed request detects key reuse with a
/// different mutation.
pub fn idempotentTransactionId(
    principal: ?[]const u8,
    table_name: []const u8,
    idempotency_key: []const u8,
) db_mod.types.TxnId {
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    hasher.update("antfly:batch-idempotency:v1");
    hashIdentityComponent(&hasher, principal orelse "");
    hashIdentityComponent(&hasher, table_name);
    hashIdentityComponent(&hasher, idempotency_key);
    var digest: [32]u8 = undefined;
    hasher.final(&digest);
    var txn_id: db_mod.types.TxnId = undefined;
    @memcpy(&txn_id, digest[0..txn_id.len]);
    return txn_id;
}

fn hashIdentityComponent(hasher: anytype, value: []const u8) void {
    var len: [8]u8 = undefined;
    std.mem.writeInt(u64, &len, value.len, .big);
    hasher.update(&len);
    hasher.update(value);
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
    return try makeSessionKeyForKind(alloc, txn_id, .interactive);
}

fn makeSessionKeyForKind(alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId, kind: SessionKind) ![]u8 {
    const txn_hex = distributed_txn.encodeTxnIdHex(txn_id);
    return try std.fmt.allocPrint(alloc, "{s}{s}", .{ sessionPrefix(kind), &txn_hex });
}

fn makeSessionLeaseKey(alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId) ![]u8 {
    return try makeSessionLeaseKeyForKind(alloc, txn_id, .interactive);
}

fn makeSessionLeaseKeyForKind(alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId, kind: SessionKind) ![]u8 {
    const txn_hex = distributed_txn.encodeTxnIdHex(txn_id);
    return try std.fmt.allocPrint(alloc, "{s}{s}", .{ sessionLeasePrefix(kind), &txn_hex });
}

fn makeSessionExpiryKey(alloc: std.mem.Allocator, timestamp: u64, txn_id: db_mod.types.TxnId) ![]u8 {
    return try makeSessionExpiryKeyForKind(alloc, timestamp, txn_id, .interactive);
}

fn makeSessionExpiryKeyForKind(alloc: std.mem.Allocator, timestamp: u64, txn_id: db_mod.types.TxnId, kind: SessionKind) ![]u8 {
    const txn_hex = distributed_txn.encodeTxnIdHex(txn_id);
    return try std.fmt.allocPrint(alloc, "{s}{x:0>16}:{s}", .{ sessionExpiryPrefix(kind), timestamp, &txn_hex });
}

fn makeSessionRecoveryKey(alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId) ![]u8 {
    return try makeSessionRecoveryKeyForKind(alloc, txn_id, .interactive);
}

fn makeSessionRecoveryKeyForKind(alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId, kind: SessionKind) ![]u8 {
    const txn_hex = distributed_txn.encodeTxnIdHex(txn_id);
    return try std.fmt.allocPrint(alloc, "{s}{s}", .{ sessionRecoveryPrefix(kind), &txn_hex });
}

fn sessionKind(session: Session) SessionKind {
    return if (session.idempotent_receipt) .idempotent_receipt else .interactive;
}

fn sessionKindIndex(kind: SessionKind) usize {
    return switch (kind) {
        .interactive => 0,
        .idempotent_receipt => 1,
    };
}

fn nextSessionKind(kind: SessionKind) SessionKind {
    return switch (kind) {
        .interactive => .idempotent_receipt,
        .idempotent_receipt => .interactive,
    };
}

fn sessionPrefix(kind: SessionKind) []const u8 {
    return switch (kind) {
        .interactive => session_prefix,
        .idempotent_receipt => receipt_session_prefix,
    };
}

fn sessionLeasePrefix(kind: SessionKind) []const u8 {
    return switch (kind) {
        .interactive => session_lease_prefix,
        .idempotent_receipt => receipt_lease_prefix,
    };
}

fn sessionExpiryPrefix(kind: SessionKind) []const u8 {
    return switch (kind) {
        .interactive => session_expiry_prefix,
        .idempotent_receipt => receipt_expiry_prefix,
    };
}

fn sessionRecoveryPrefix(kind: SessionKind) []const u8 {
    return switch (kind) {
        .interactive => session_recovery_prefix,
        .idempotent_receipt => receipt_recovery_prefix,
    };
}

fn sessionNeedsRecovery(session: Session) bool {
    if (session.idempotent_outcome != null) return false;
    if (session.terminal_commit) |terminal| {
        return terminal.status != .committed or
            (terminal.repair_required and terminal.coordinator_group_id == null) or
            (terminal.coordinator_group_id != null and !terminal.coordinator_acknowledged);
    }
    return session.commit_body_digest != null and session.commit_execution_started;
}

fn sessionNeedsLeaseRenewal(session: Session) bool {
    if (session.idempotent_outcome != null) return false;
    if (session.terminal_commit != null) return sessionNeedsRecovery(session);
    return true;
}

/// A retention deadline must not delete work while its outcome or required
/// phase-two handoff is still live. Terminal repair debt is the exception: it
/// is an operator-visible final result, so an acknowledged legacy
/// `committed_visibility_pending + repair_required` record may expire just as
/// the canonical `committed + repair_required` representation does.
fn sessionRetentionPinned(session: Session) bool {
    if (session.idempotent_outcome != null) return false;
    if (session.terminal_commit) |terminal| {
        if (terminal.coordinator_group_id != null and !terminal.coordinator_acknowledged) return true;
        return switch (terminal.status) {
            .committed => terminal.repair_required and terminal.coordinator_group_id == null,
            .committed_visibility_pending => !terminal.repair_required,
            .committed_recovery_pending => true,
        };
    }
    return session.commit_body_digest != null and session.commit_execution_started;
}

fn parseSessionExpiryKey(key: []const u8, kind: SessionKind) ?SessionExpiryCursor {
    const prefix = sessionExpiryPrefix(kind);
    if (!std.mem.startsWith(u8, key, prefix)) return null;
    const suffix = key[prefix.len..];
    if (suffix.len < 18 or suffix[16] != ':') return null;
    return .{
        .timestamp = std.fmt.parseUnsigned(u64, suffix[0..16], 16) catch return null,
        .txn_id = distributed_txn.parseTxnIdHex(suffix[17..]) catch return null,
    };
}

fn ownerLeaseId(alloc: std.mem.Allocator, owner_node_id: u64, owner_incarnation: u64) ![]u8 {
    if (owner_incarnation == 0)
        return try std.fmt.allocPrint(alloc, "node:{d}", .{owner_node_id});
    return try std.fmt.allocPrint(alloc, "node:{d}:incarnation:{d}", .{ owner_node_id, owner_incarnation });
}

fn leaseRecordOwnerNodeId(owner_id: []const u8) ?u64 {
    if (!std.mem.startsWith(u8, owner_id, "node:")) return null;
    const suffix = owner_id["node:".len..];
    const end = std.mem.indexOfScalar(u8, suffix, ':') orelse suffix.len;
    return std.fmt.parseUnsigned(u64, suffix[0..end], 10) catch null;
}

fn encodeSessionRecord(alloc: std.mem.Allocator, session: Session) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.appendSlice(alloc, "{\"owner_node_id\":");
    try out.print(alloc, "{d}", .{session.owner_node_id});
    try out.appendSlice(alloc, ",\"owner_incarnation\":");
    try out.print(alloc, "{d}", .{session.owner_incarnation});
    try out.appendSlice(alloc, ",\"principal\":");
    if (session.principal) |principal| {
        try appendJsonString(alloc, &out, principal);
    } else {
        try out.appendSlice(alloc, "null");
    }
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
        const encoded = try encodeCommitRequest(alloc, staged);
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
    try out.appendSlice(alloc, ",\"idempotent_receipt\":");
    try out.appendSlice(alloc, if (session.idempotent_receipt) "true" else "false");
    try out.appendSlice(alloc, ",\"idempotent_outcome\":");
    if (session.idempotent_outcome) |outcome| {
        try appendJsonString(alloc, &out, outcome.text());
    } else {
        try out.appendSlice(alloc, "null");
    }
    try out.appendSlice(alloc, ",\"idempotent_error_code\":");
    if (session.idempotent_error_code) |code| {
        try appendJsonString(alloc, &out, code);
    } else {
        try out.appendSlice(alloc, "null");
    }
    try out.appendSlice(alloc, ",\"idempotent_error_message\":");
    if (session.idempotent_error_message) |message| {
        try appendJsonString(alloc, &out, message);
    } else {
        try out.appendSlice(alloc, "null");
    }
    try out.appendSlice(alloc, ",\"idempotent_error_retryable\":");
    try out.appendSlice(alloc, if (session.idempotent_error_retryable) "true" else "false");
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
        try out.appendSlice(alloc, ",\"snapshot\":");
        const encoded = try encodeCommitRequest(alloc, entry.value_ptr.snapshot);
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
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{});
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
                else => return error.InvalidTransactionSessionRecord,
            }
        else
            sessionOwnerNodeId(txn_id),
        .owner_incarnation = if (obj.get("owner_incarnation")) |value|
            switch (value) {
                .integer => |v| try nonNegativeRecordInteger(v),
                else => return error.InvalidTransactionSessionRecord,
            }
        else
            0,
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
            else => return error.InvalidTransactionSessionRecord,
        },
        .last_touched_timestamp = 0,
        .sync_level = parseSyncLevel(obj.get("sync_level") orelse return error.InvalidTransactionSessionRecord) orelse return error.InvalidTransactionSessionRecord,
        .next_savepoint_id = switch (obj.get("next_savepoint_id") orelse return error.InvalidTransactionSessionRecord) {
            .integer => |v| try nonNegativeRecordInteger(v),
            else => return error.InvalidTransactionSessionRecord,
        },
    };
    errdefer session.deinit(alloc);
    session.last_touched_timestamp = if (obj.get("last_touched_timestamp")) |value|
        switch (value) {
            .integer => |v| try nonNegativeRecordInteger(v),
            else => return error.InvalidTransactionSessionRecord,
        }
    else
        session.begin_timestamp;
    if (obj.get("staged")) |staged_value| {
        if (staged_value != .null) session.staged = try parseCommitValue(alloc, staged_value);
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
    session.idempotent_receipt = if (obj.get("idempotent_receipt")) |value| switch (value) {
        .bool => |enabled| enabled,
        else => return error.InvalidTransactionSessionRecord,
    } else false;
    if (obj.get("idempotent_outcome")) |value| switch (value) {
        .string => |text| session.idempotent_outcome = std.meta.stringToEnum(IdempotentReceiptOutcome, text) orelse
            return error.InvalidTransactionSessionRecord,
        .null => {},
        else => return error.InvalidTransactionSessionRecord,
    };
    if (obj.get("idempotent_error_code")) |value| switch (value) {
        .string => |code| session.idempotent_error_code = try alloc.dupe(u8, code),
        .null => {},
        else => return error.InvalidTransactionSessionRecord,
    };
    if (obj.get("idempotent_error_message")) |value| switch (value) {
        .string => |message| session.idempotent_error_message = try alloc.dupe(u8, message),
        .null => {},
        else => return error.InvalidTransactionSessionRecord,
    };
    session.idempotent_error_retryable = if (obj.get("idempotent_error_retryable")) |value| switch (value) {
        .bool => |retryable| retryable,
        else => return error.InvalidTransactionSessionRecord,
    } else false;
    if ((session.idempotent_error_code == null) != (session.idempotent_error_message == null))
        return error.InvalidTransactionSessionRecord;
    if (session.idempotent_outcome == null and
        (session.idempotent_error_code != null or session.idempotent_error_retryable))
        return error.InvalidTransactionSessionRecord;
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
            else => return error.InvalidTransactionSessionRecord,
        };
        const snapshot = try parseCommitValue(alloc, entry_obj.get("snapshot") orelse return error.InvalidTransactionSessionRecord);
        var read_snapshots: std.StringArrayHashMapUnmanaged(SessionReadSnapshot) = .empty;
        errdefer deinitReadSnapshotMap(alloc, &read_snapshots);
        if (entry_obj.get("read_snapshots")) |read_snapshots_value| {
            try decodeReadSnapshotsInto(alloc, read_snapshots_value, &read_snapshots);
        }
        try session.savepoints.put(alloc, id, .{
            .id = id,
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
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/txn-session-failure-atomic", .{tmp.sub_path});
    defer std.testing.allocator.free(path);
    const path_z = try std.testing.allocator.dupeZ(u8, path);
    defer std.testing.allocator.free(path_z);

    var store = try docstore_mod.DocStore.open(std.testing.allocator, path_z, .{});
    defer store.close();
    var durable = DurableSessionStore.init(std.testing.allocator, &store);
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

test "terminal commit receipts join concurrent replay results monotonically" {
    const alloc = std.testing.allocator;
    var registry = SessionRegistry.init(null);
    defer registry.deinit(alloc);
    const session = try registry.begin(alloc, .{ .sync_level = .full_index }, 9);
    var request = try parseCommitRequest(alloc,
        \\{"read_set":[],"tables":{"docs":{"inserts":{"doc:a":{"value":1}}}}}
    );
    defer request.deinit(alloc);
    var sealed = (try registry.cloneCommitRequest(alloc, session.txn_id, &request)).?;
    sealed.deinit(alloc);
    _ = (try registry.markCommitExecutionStarted(alloc, session.txn_id)).?;

    // An error-only attempt can establish debt without coordinator metadata.
    _ = (try registry.recordTerminalCommitWithRepair(
        alloc,
        session.txn_id,
        .committed_recovery_pending,
        false,
        null,
        null,
    )).?;
    // Recovery fills the immutable coordinator identity and advances the debt.
    _ = (try registry.recordTerminalCommitWithRepair(
        alloc,
        session.txn_id,
        .committed_visibility_pending,
        false,
        7001,
        "docs",
    )).?;
    // A slower original attempt must not regress status or erase identity.
    _ = (try registry.recordTerminalCommitWithRepair(
        alloc,
        session.txn_id,
        .committed_recovery_pending,
        false,
        null,
        null,
    )).?;

    var pending = (try registry.getTerminalCommit(alloc, session.txn_id)).?;
    try std.testing.expectEqual(TerminalCommitStatus.committed_visibility_pending, pending.status);
    try std.testing.expectEqual(@as(?u64, 7001), pending.coordinator_group_id);
    try std.testing.expectEqualStrings("docs", pending.coordinator_table_name.?);
    pending.deinit(alloc);

    _ = (try registry.recordTerminalCommitWithRepair(
        alloc,
        session.txn_id,
        .committed,
        false,
        7001,
        "docs",
    )).?;
    _ = (try registry.markTerminalCoordinatorAcknowledged(alloc, session.txn_id)).?;
    // Repair evidence is sticky, but its stale visibility status is not.
    _ = (try registry.recordTerminalCommitWithRepair(
        alloc,
        session.txn_id,
        .committed_visibility_pending,
        true,
        null,
        null,
    )).?;

    var terminal = (try registry.getTerminalCommit(alloc, session.txn_id)).?;
    defer terminal.deinit(alloc);
    try std.testing.expectEqual(TerminalCommitStatus.committed, terminal.status);
    try std.testing.expect(terminal.repair_required);
    try std.testing.expect(terminal.coordinator_acknowledged);
    try std.testing.expectEqual(@as(?u64, 7001), terminal.coordinator_group_id);
    try std.testing.expectEqualStrings("docs", terminal.coordinator_table_name.?);
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
        effectiveTerminalCommitStatus(.{
            .status = .committed,
            .coordinator_group_id = 7001,
            .coordinator_table_name = null,
            .coordinator_acknowledged = false,
        }),
    );
    try std.testing.expectEqual(
        TerminalCommitStatus.committed,
        effectiveTerminalCommitStatus(.{
            .status = .committed,
            .coordinator_group_id = 7001,
            .coordinator_table_name = null,
            .coordinator_acknowledged = true,
        }),
    );
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
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/txn-session-limits", .{tmp.sub_path});
    defer std.testing.allocator.free(path);
    const path_z = try std.testing.allocator.dupeZ(u8, path);
    defer std.testing.allocator.free(path_z);

    var store = try docstore_mod.DocStore.open(std.testing.allocator, path_z, .{});
    defer store.close();
    var durable = DurableSessionStore.init(std.testing.allocator, &store);
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

test "cluster shared session capacity is enforced by the durable create transaction" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/txn-session-shared-capacity", .{tmp.sub_path});
    defer alloc.free(path);
    const path_z = try alloc.dupeZ(u8, path);
    defer alloc.free(path_z);

    var store = try docstore_mod.DocStore.open(alloc, path_z, .{});
    defer store.close();
    var durable = DurableSessionStore.init(alloc, &store);
    const leases = SessionLeaseStore.init(alloc, &store);
    var first = SessionRegistry.initWithOptions(&durable, leases, std.time.ns_per_s, null, 1, null);
    defer first.deinit(alloc);
    first.durable_scope = .cluster_shared;
    var second = SessionRegistry.initWithOptions(&durable, leases, std.time.ns_per_s, null, 1, null);
    defer second.deinit(alloc);
    second.durable_scope = .cluster_shared;

    // Model two API processes that both observed zero sessions before either
    // create. The backend write transaction, not these stale caches, owns the
    // cluster-wide capacity invariant.
    first.known_durable_session_count = 0;
    second.known_durable_session_count = 0;
    _ = try first.begin(alloc, .{ .sync_level = .write }, 1);
    try std.testing.expectError(
        error.SessionLimitExceeded,
        second.begin(alloc, .{ .sync_level = .write }, 2),
    );
    try std.testing.expectEqual(@as(usize, 1), try durable.sessionCount());
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
    _ = (try reader.markCommitExecutionStarted(alloc, txn_id)).?;
    try std.testing.expectEqual(
        SessionRegistry.AbortInteractiveResult.execution_started,
        try reader.abortInteractive(alloc, txn_id),
    );
    try std.testing.expect(reader.getInfo(txn_id) != null);
}

test "cluster-shared idempotency requires atomic owner fencing" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/txn-session-shared-capability", .{tmp.sub_path});
    defer alloc.free(path);
    const path_z = try alloc.dupeZ(u8, path);
    defer alloc.free(path_z);
    var store = try docstore_mod.DocStore.open(alloc, path_z, .{});
    defer store.close();
    var durable = DurableSessionStore.init(alloc, &store);

    var unfenced = SessionRegistry.init(&durable);
    defer unfenced.deinit(alloc);
    unfenced.durable_scope = .cluster_shared;
    try std.testing.expect(!unfenced.hasAtomicClusterSharedStore());

    var fenced = SessionRegistry.initWithLeaseTtl(&durable, SessionLeaseStore.init(alloc, &store), std.time.ns_per_s);
    defer fenced.deinit(alloc);
    fenced.durable_scope = .cluster_shared;
    try std.testing.expect(fenced.hasAtomicClusterSharedStore());
}

test "shared session mutations and cleanup reject an adopted owner incarnation" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/txn-session-incarnation-fence", .{tmp.sub_path});
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
    const txn_id = idempotentTransactionId("alice", "docs", "fenced-operation");
    var request = try parseCommitRequest(alloc,
        \\{"read_set":[],"tables":{"docs":{"inserts":{"doc:a":{"value":1}}}},"sync_level":"write"}
    );
    defer request.deinit(alloc);
    _ = try owner.beginIdempotentForPrincipal(alloc, txn_id, .{ .sync_level = .write }, 7, "alice", &request);
    var sealed = (try owner.cloneIdempotentCommitRequest(alloc, txn_id, &request)).?;
    sealed.deinit(alloc);

    var lease = (try leases.load(alloc, txn_id)).?;
    defer lease_mod.deinitRecord(alloc, &lease);
    var adopter = SessionRegistry.initWithLeaseTtl(&durable, leases, std.time.ns_per_s);
    defer adopter.deinit(alloc);
    adopter.durable_scope = .cluster_shared;
    try std.testing.expect(try adopter.adoptIfLeaseExpired(
        alloc,
        txn_id,
        7,
        lease.expires_at_ms * std.time.ns_per_ms + 1,
    ));
    try std.testing.expect(owner.owner_incarnation != adopter.owner_incarnation);
    try std.testing.expectEqual(@as(usize, 1), owner.lease_renewal_candidates.count());
    try std.testing.expectEqual(
        @as(usize, 0),
        try owner.renewOwnedLeases(7, lease.expires_at_ms * std.time.ns_per_ms + 2),
    );
    // Losing durable ownership evicts the stale process-local projection. It
    // must not consume a bounded renewal slot on every future supervisor pass.
    try std.testing.expectEqual(@as(usize, 0), owner.lease_renewal_candidates.count());

    try std.testing.expectError(
        error.SessionLeaseLost,
        owner.cloneIdempotentCommitRequest(alloc, txn_id, &request),
    );

    // Interactive abort uses the same incarnation fence. A stale HTTP owner
    // must surface lease loss instead of reporting success or deleting the
    // adopter's record.
    const interactive = try owner.begin(alloc, .{ .sync_level = .write }, 7);
    var interactive_lease = (try leases.load(alloc, interactive.txn_id)).?;
    defer lease_mod.deinitRecord(alloc, &interactive_lease);
    try std.testing.expect(try adopter.adoptIfLeaseExpired(
        alloc,
        interactive.txn_id,
        7,
        interactive_lease.expires_at_ms * std.time.ns_per_ms + 1,
    ));
    try std.testing.expectError(error.SessionLeaseLost, owner.abortInteractive(alloc, interactive.txn_id));

    try std.testing.expectEqual(@as(usize, 0), try owner.cleanupExpired(alloc, std.math.maxInt(u64)));
    var persisted = (try durable.load(txn_id)).?;
    try std.testing.expectEqual(adopter.owner_incarnation, persisted.owner_incarnation);
    persisted.deinit(alloc);

    var adopted_lease = (try leases.load(alloc, txn_id)).?;
    defer lease_mod.deinitRecord(alloc, &adopted_lease);
    var reaper = SessionRegistry.initWithLeaseTtl(&durable, leases, std.time.ns_per_s);
    defer reaper.deinit(alloc);
    reaper.durable_scope = .cluster_shared;
    try std.testing.expectEqual(
        @as(usize, 1),
        try reaper.cleanupExpiredAt(
            alloc,
            std.math.maxInt(u64),
            adopted_lease.expires_at_ms * std.time.ns_per_ms + 1,
        ),
    );
    try std.testing.expect((try durable.load(txn_id)) == null);
}

test "terminal idempotent receipt replay does not require mutation lease ownership" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/txn-terminal-receipt-replay", .{tmp.sub_path});
    defer alloc.free(path);
    const path_z = try alloc.dupeZ(u8, path);
    defer alloc.free(path_z);
    var store = try docstore_mod.DocStore.open(alloc, path_z, .{});
    defer store.close();
    var durable = DurableSessionStore.init(alloc, &store);
    const leases = SessionLeaseStore.init(alloc, &store);

    var owner = SessionRegistry.initWithLeaseTtl(&durable, leases, std.time.ns_per_s);
    defer owner.deinit(alloc);
    const txn_id = idempotentTransactionId("alice", "docs", "completed-operation");
    var request = try parseCommitRequest(alloc,
        \\{"read_set":[],"tables":{"docs":{"inserts":{"doc:a":{"value":1}}}},"sync_level":"write"}
    );
    defer request.deinit(alloc);
    _ = try owner.beginIdempotentForPrincipal(alloc, txn_id, .{ .sync_level = .write }, 7, "alice", &request);
    _ = (try owner.markCommitExecutionStarted(alloc, txn_id)).?;
    _ = (try owner.recordTerminalCommit(alloc, txn_id, .committed, 7001, "docs")).?;
    _ = (try owner.markTerminalCoordinatorAcknowledged(alloc, txn_id)).?;

    // A restarted process has a new incarnation and cannot mutate while the
    // old lease is live, but immutable replay remains immediately available.
    var restarted = SessionRegistry.initWithLeaseTtl(&durable, leases, std.time.ns_per_s);
    defer restarted.deinit(alloc);
    var snapshot = (try restarted.getIdempotentTerminalCommitSnapshot(alloc, txn_id, &request)).?;
    defer snapshot.deinit(alloc);
    try std.testing.expect(snapshot.terminal.coordinator_acknowledged);
    try std.testing.expect(!snapshot.owns_mutation_lease);
    try std.testing.expectEqual(@as(usize, 1), snapshot.request.tables.len);
    try std.testing.expectError(
        error.SessionLeaseLost,
        restarted.cloneIdempotentCommitRequest(alloc, txn_id, &request),
    );

    var different = try parseCommitRequest(alloc,
        \\{"read_set":[],"tables":{"docs":{"inserts":{"doc:a":{"value":2}}}},"sync_level":"write"}
    );
    defer different.deinit(alloc);
    try std.testing.expectError(
        error.TransactionCommitRequestMismatch,
        restarted.getIdempotentTerminalCommitSnapshot(alloc, txn_id, &different),
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

    var pending_terminal_work = (try registry.claimPendingRecovery(
        alloc,
        session.txn_id,
        9,
        nextTxnTimestamp(),
    )) orelse return error.TestExpectedEqual;
    defer pending_terminal_work.deinit(alloc);
    try std.testing.expect(pending_terminal_work == .commit);

    _ = (try registry.recordTerminalCommit(alloc, session.txn_id, .committed, 7001, "docs")) orelse return error.TestExpectedEqual;
    var acknowledgement_work = (try registry.claimPendingRecovery(
        alloc,
        session.txn_id,
        9,
        nextTxnTimestamp(),
    )) orelse return error.TestExpectedEqual;
    defer acknowledgement_work.deinit(alloc);
    try std.testing.expect(acknowledgement_work == .acknowledge);
    _ = (try registry.markTerminalCoordinatorAcknowledged(alloc, session.txn_id)) orelse return error.TestExpectedEqual;

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
    var registry = SessionRegistry.initWithLeaseTtl(&durable, lease_store, std.time.ns_per_s);
    defer registry.deinit(std.testing.allocator);
    const session = try registry.begin(std.testing.allocator, .{ .sync_level = .write }, 15);

    const initial_status = (try registry.getStatus(std.testing.allocator, session.txn_id)) orelse return error.TestExpectedEqual;
    try std.testing.expect(initial_status.lease_expires_at > 0);

    std.Thread.yield() catch {};
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
    const renewed_now = initial.lease_expires_at - 500 * std.time.ns_per_ms;
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

test "idempotent batch identities are scoped and session bodies are sealed" {
    const alloc = std.testing.allocator;
    const alice_docs = idempotentTransactionId("alice", "docs", "load-42");
    const alice_docs_replay = idempotentTransactionId("alice", "docs", "load-42");
    const bob_docs = idempotentTransactionId("bob", "docs", "load-42");
    const alice_other = idempotentTransactionId("alice", "other", "load-42");
    try std.testing.expectEqualSlices(u8, &alice_docs, &alice_docs_replay);
    try std.testing.expect(!std.mem.eql(u8, &alice_docs, &bob_docs));
    try std.testing.expect(!std.mem.eql(u8, &alice_docs, &alice_other));

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/idempotent-receipt-store", .{tmp.sub_path});
    defer alloc.free(path);
    const path_z = try alloc.dupeZ(u8, path);
    defer alloc.free(path_z);
    var store = try docstore_mod.DocStore.open(alloc, path_z, .{});
    defer store.close();
    var durable = DurableSessionStore.init(alloc, &store);
    var registry = SessionRegistry.init(&durable);
    defer registry.deinit(alloc);
    var first_batch = try batch_api.parseBatchRequest(alloc,
        \\{"inserts":{"one":{"value":1}},"sync_level":"write"}
    );
    defer first_batch.deinit(alloc);
    var first_request = try ownedRequestFromBatch(alloc, "docs", first_batch);
    defer first_request.deinit(alloc);
    const first = try registry.beginIdempotentForPrincipal(alloc, alice_docs, .{ .sync_level = .write }, 7, "alice", &first_request);
    try std.testing.expectEqual(SessionKind.idempotent_receipt, first.kind);
    var created = (try durable.load(alice_docs)).?;
    defer created.deinit(alloc);
    try std.testing.expect(created.commit_body_digest != null);
    try std.testing.expect(created.staged != null);
    const replay = try registry.beginIdempotentForPrincipal(alloc, alice_docs, .{ .sync_level = .write }, 7, "alice", &first_request);
    try std.testing.expectEqual(first.begin_timestamp, replay.begin_timestamp);
    try std.testing.expectError(
        error.IdempotencyConflict,
        registry.beginIdempotentForPrincipal(alloc, alice_docs, .{ .sync_level = .full_index }, 7, "alice", &first_request),
    );
    try std.testing.expectError(
        error.SessionKindMismatch,
        registry.cloneCommitRequest(alloc, alice_docs, &first_request),
    );
    try std.testing.expectError(
        error.IdempotentReceiptImmutable,
        registry.stage(alloc, alice_docs, &first_request),
    );
    try std.testing.expectError(
        error.IdempotentReceiptImmutable,
        registry.createSavepoint(alloc, alice_docs),
    );
    try std.testing.expectEqual(SessionRegistry.AbortInteractiveResult.idempotent_receipt, try registry.abortInteractive(alloc, alice_docs));
    try std.testing.expect(!registry.remove(alloc, alice_docs));
    var sealed = (try registry.cloneIdempotentCommitRequest(alloc, alice_docs, &first_request)).?;
    sealed.deinit(alloc);

    var changed_batch = try batch_api.parseBatchRequest(alloc,
        \\{"inserts":{"one":{"value":2}},"sync_level":"write"}
    );
    defer changed_batch.deinit(alloc);
    var changed_request = try ownedRequestFromBatch(alloc, "docs", changed_batch);
    defer changed_request.deinit(alloc);
    try std.testing.expectError(
        error.TransactionCommitRequestMismatch,
        registry.beginIdempotentForPrincipal(alloc, alice_docs, .{ .sync_level = .write }, 7, "alice", &changed_request),
    );
    try std.testing.expectError(
        error.TransactionCommitRequestMismatch,
        registry.cloneIdempotentCommitRequest(alloc, alice_docs, &changed_request),
    );
    _ = (try registry.markCommitExecutionStarted(alloc, alice_docs)).?;
    var recovery = (try registry.claimPendingRecovery(alloc, alice_docs, 7, nextTxnTimestamp())).?;
    defer recovery.deinit(alloc);
    try std.testing.expect(recovery.commit.idempotent_receipt);
    _ = (try registry.recordIdempotentDurableAbort(alloc, alice_docs, "transaction_conflict", "conflict")).?;
    const status = (try registry.getStatus(alloc, alice_docs)).?;
    try std.testing.expectEqualStrings("aborted", status.outcome.?);

    // Rejections are additive receipt state, not new values in the established
    // terminal-commit enum. Verify the durable representation is authoritative
    // after all process-local state is gone.
    var reloaded = SessionRegistry.init(&durable);
    defer reloaded.deinit(alloc);
    const reloaded_status = (try reloaded.getStatus(alloc, alice_docs)).?;
    try std.testing.expectEqualStrings("aborted", reloaded_status.outcome.?);
    try std.testing.expectEqual(IdempotentReceiptOutcome.aborted, (try reloaded.getIdempotentOutcome(alloc, alice_docs)).?);
    var reloaded_receipt = (try reloaded.getIdempotentTerminalReceipt(alloc, alice_docs)).?;
    defer reloaded_receipt.deinit(alloc);
    try std.testing.expectEqual(IdempotentReceiptOutcome.aborted, reloaded_receipt.outcome);
    try std.testing.expectEqualStrings("transaction_conflict", reloaded_receipt.code);
    try std.testing.expectEqualStrings("conflict", reloaded_receipt.message);
    try std.testing.expect(!reloaded_receipt.retryable);
    const listed = try reloaded.listStatusesForPrincipal(alloc, "alice");
    defer alloc.free(listed);
    try std.testing.expectEqual(@as(usize, 1), listed.len);
    try std.testing.expectEqualStrings("aborted", listed[0].outcome.?);
    try std.testing.expect(!listed[0].repair_required);
    const pending = try registry.listPendingRecoveryIds(alloc, 8);
    defer alloc.free(pending);
    try std.testing.expectEqual(@as(usize, 0), pending.len);
}

test "idempotent receipt creation does not publish an unsealed oversized key" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/idempotent-receipt-oversized", .{tmp.sub_path});
    defer alloc.free(path);
    const path_z = try alloc.dupeZ(u8, path);
    defer alloc.free(path_z);
    var store = try docstore_mod.DocStore.open(alloc, path_z, .{});
    defer store.close();
    var durable = DurableSessionStore.init(alloc, &store);
    var registry = SessionRegistry.initWithOptions(&durable, null, null, null, 8, 256);
    defer registry.deinit(alloc);

    var request = try parseCommitRequest(alloc,
        \\{"read_set":[],"tables":{"docs":{"inserts":{"doc:a":{"value":"this payload is deliberately long enough that its durable session encoding cannot fit inside the configured two hundred and fifty six byte receipt limit"}}}},"sync_level":"write"}
    );
    defer request.deinit(alloc);
    const txn_id = idempotentTransactionId("alice", "docs", "oversized");
    try std.testing.expectError(
        error.SessionRecordTooLarge,
        registry.beginIdempotentForPrincipal(alloc, txn_id, .{ .sync_level = .write }, 7, "alice", &request),
    );
    try std.testing.expect((try durable.load(txn_id)) == null);
    try std.testing.expectEqual(@as(usize, 0), registry.sessions.count());
}

test "legacy unsealed idempotent receipt is terminalized without binding a retry body" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/idempotent-receipt-unsealed-compat", .{tmp.sub_path});
    defer alloc.free(path);
    const path_z = try alloc.dupeZ(u8, path);
    defer alloc.free(path_z);
    var store = try docstore_mod.DocStore.open(alloc, path_z, .{});
    defer store.close();
    var durable = DurableSessionStore.init(alloc, &store);
    const txn_id = idempotentTransactionId("alice", "docs", "legacy-unsealed");
    var orphan: Session = .{
        .txn_id = txn_id,
        .owner_node_id = 7,
        .principal = try alloc.dupe(u8, "alice"),
        .begin_timestamp = nextTxnTimestamp(),
        .last_touched_timestamp = nextTxnTimestamp(),
        .sync_level = .write,
        .idempotent_receipt = true,
    };
    defer orphan.deinit(alloc);
    try durable.save(orphan, null);

    var registry = SessionRegistry.init(&durable);
    defer registry.deinit(alloc);
    var request = try parseCommitRequest(alloc,
        \\{"read_set":[],"tables":{"docs":{"inserts":{"doc:a":{"value":1}}}},"sync_level":"write"}
    );
    defer request.deinit(alloc);
    try std.testing.expectError(
        error.UnsealedIdempotencyReceipt,
        registry.cloneIdempotentCommitRequest(alloc, txn_id, &request),
    );
    try std.testing.expectEqual(IdempotentReceiptOutcome.not_applied, (try registry.getIdempotentOutcome(alloc, txn_id)).?);
    const pending = try registry.listPendingRecoveryIds(alloc, 8);
    defer alloc.free(pending);
    try std.testing.expectEqual(@as(usize, 0), pending.len);
}

test "retention never removes an idempotent receipt with live recovery" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/idempotent-receipt-live-retention", .{tmp.sub_path});
    defer alloc.free(path);
    const path_z = try alloc.dupeZ(u8, path);
    defer alloc.free(path_z);
    var store = try docstore_mod.DocStore.open(alloc, path_z, .{});
    defer store.close();
    var durable = DurableSessionStore.init(alloc, &store);
    var registry = SessionRegistry.init(&durable);
    defer registry.deinit(alloc);

    var request = try parseCommitRequest(alloc,
        \\{"read_set":[],"tables":{"docs":{"inserts":{"doc:a":{"value":1}}}},"sync_level":"write"}
    );
    defer request.deinit(alloc);
    const txn_id = idempotentTransactionId("alice", "docs", "live-retention");
    _ = try registry.beginIdempotentForPrincipal(alloc, txn_id, .{ .sync_level = .write }, 7, "alice", &request);
    _ = (try registry.markCommitExecutionStarted(alloc, txn_id)).?;

    try std.testing.expectEqual(@as(usize, 0), try registry.cleanupExpired(alloc, std.math.maxInt(u64)));
    var retained = (try durable.load(txn_id)).?;
    retained.deinit(alloc);

    _ = (try registry.recordIdempotentDurableAbort(alloc, txn_id, "transaction_conflict", "conflict")).?;
    try std.testing.expectEqual(@as(usize, 1), try registry.cleanupExpired(alloc, std.math.maxInt(u64)));
    try std.testing.expect((try durable.load(txn_id)) == null);
}

test "isolated retention namespaces cannot starve completed sessions" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/idempotent-retention-fairness", .{tmp.sub_path});
    defer alloc.free(path);
    const path_z = try alloc.dupeZ(u8, path);
    defer alloc.free(path_z);
    var store = try docstore_mod.DocStore.open(alloc, path_z, .{});
    defer store.close();
    var durable = DurableSessionStore.init(alloc, &store);
    var registry = SessionRegistry.init(&durable);
    defer registry.deinit(alloc);

    var request = try parseCommitRequest(alloc,
        \\{"read_set":[],"tables":{"docs":{"inserts":{"doc:a":{"value":1}}}},"sync_level":"write"}
    );
    defer request.deinit(alloc);
    const pinned_id = idempotentTransactionId("alice", "docs", "pinned-first");
    _ = try registry.beginIdempotentForPrincipal(alloc, pinned_id, .{ .sync_level = .write }, 7, "alice", &request);
    _ = (try registry.markCommitExecutionStarted(alloc, pinned_id)).?;
    const completed = try registry.begin(alloc, .{ .sync_level = .write }, 7);

    try std.testing.expectEqual(
        @as(usize, 1),
        try registry.cleanupExpiredAtLimit(alloc, std.math.maxInt(u64), std.math.maxInt(u64), 1),
    );
    try std.testing.expectEqual(
        @as(usize, 0),
        try registry.cleanupExpiredAtLimit(alloc, std.math.maxInt(u64), std.math.maxInt(u64), 1),
    );
    try std.testing.expect((try durable.load(completed.txn_id)) == null);
    var pinned = (try durable.load(pinned_id)).?;
    pinned.deinit(alloc);
}

test "idempotent receipt transitions serialize rejection against execution start" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/idempotent-receipt-transition", .{tmp.sub_path});
    defer alloc.free(path);
    const path_z = try alloc.dupeZ(u8, path);
    defer alloc.free(path_z);
    var store = try docstore_mod.DocStore.open(alloc, path_z, .{});
    defer store.close();
    var durable = DurableSessionStore.init(alloc, &store);
    var registry = SessionRegistry.init(&durable);
    defer registry.deinit(alloc);
    var request = try parseCommitRequest(alloc,
        \\{"read_set":[],"tables":{"docs":{"inserts":{"doc:a":{"value":1}}}},"sync_level":"write"}
    );
    defer request.deinit(alloc);

    const rejected_id = idempotentTransactionId("alice", "docs", "rejection-wins");
    _ = try registry.beginIdempotentForPrincipal(alloc, rejected_id, .{ .sync_level = .write }, 7, "alice", &request);
    try std.testing.expectError(error.TransactionExecutionNotStarted, registry.recordIdempotentDurableAbort(alloc, rejected_id, "transaction_conflict", "conflict"));
    try std.testing.expectEqual(
        SessionRegistry.PreExecutionRejectionResult.recorded,
        (try registry.recordIdempotentPreExecutionRejection(alloc, rejected_id, "table_not_found", "table not found")).?,
    );
    try std.testing.expectError(error.TransactionOutcomeMismatch, registry.markCommitExecutionStarted(alloc, rejected_id));
    try std.testing.expectEqual(IdempotentReceiptOutcome.not_applied, (try registry.getIdempotentOutcome(alloc, rejected_id)).?);

    const started_id = idempotentTransactionId("alice", "docs", "execution-wins");
    _ = try registry.beginIdempotentForPrincipal(alloc, started_id, .{ .sync_level = .write }, 7, "alice", &request);
    _ = (try registry.markCommitExecutionStarted(alloc, started_id)).?;
    try std.testing.expectEqual(
        SessionRegistry.PreExecutionRejectionResult.execution_started,
        (try registry.recordIdempotentPreExecutionRejection(alloc, started_id, "table_not_found", "table not found")).?,
    );
    try std.testing.expect((try registry.getIdempotentOutcome(alloc, started_id)) == null);
    _ = (try registry.recordIdempotentDurableAbort(alloc, started_id, "transaction_conflict", "conflict")).?;
    try std.testing.expectEqual(IdempotentReceiptOutcome.aborted, (try registry.getIdempotentOutcome(alloc, started_id)).?);
}

test "versioned idempotent receipt storage is invisible to legacy session scans" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/idempotent-receipt-versioned-namespace", .{tmp.sub_path});
    defer alloc.free(path);
    const path_z = try alloc.dupeZ(u8, path);
    defer alloc.free(path_z);
    var store = try docstore_mod.DocStore.open(alloc, path_z, .{});
    defer store.close();
    var durable = DurableSessionStore.init(alloc, &store);
    const lease_store = SessionLeaseStore.init(alloc, &store);
    var registry = SessionRegistry.initWithOptions(&durable, lease_store, std.time.ns_per_s, null, 1, null);
    registry.durable_scope = .cluster_shared;
    defer registry.deinit(alloc);
    var request = try parseCommitRequest(alloc,
        \\{"read_set":[],"tables":{"docs":{"inserts":{"doc:a":{"value":1}}}},"sync_level":"write"}
    );
    defer request.deinit(alloc);
    const txn_id = idempotentTransactionId("alice", "docs", "versioned-storage");
    _ = try registry.beginIdempotentForPrincipal(alloc, txn_id, .{ .sync_level = .write }, 7, "alice", &request);
    _ = (try registry.markCommitExecutionStarted(alloc, txn_id)).?;

    const legacy_key = try makeSessionKey(alloc, txn_id);
    defer alloc.free(legacy_key);
    const legacy_value = store.get(alloc, legacy_key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
    defer if (legacy_value) |value| alloc.free(value);
    try std.testing.expect(legacy_value == null);

    const receipt_key = try makeSessionKeyForKind(alloc, txn_id, .idempotent_receipt);
    defer alloc.free(receipt_key);
    const receipt_value = try store.get(alloc, receipt_key);
    defer alloc.free(receipt_value);
    try std.testing.expect(receipt_value.len > 0);

    const legacy_lease_key = try makeSessionLeaseKey(alloc, txn_id);
    defer alloc.free(legacy_lease_key);
    const legacy_lease_value = store.get(alloc, legacy_lease_key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
    defer if (legacy_lease_value) |value| alloc.free(value);
    try std.testing.expect(legacy_lease_value == null);
    const receipt_lease_key = try makeSessionLeaseKeyForKind(alloc, txn_id, .idempotent_receipt);
    defer alloc.free(receipt_lease_key);
    const receipt_lease_value = try store.get(alloc, receipt_lease_key);
    defer alloc.free(receipt_lease_value);
    try std.testing.expect(receipt_lease_value.len > 0);
    try std.testing.expectEqual(@as(usize, 1), try registry.renewOwnedLeases(7, nextTxnTimestamp()));
    const legacy_lease_after_renew = store.get(alloc, legacy_lease_key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
    defer if (legacy_lease_after_renew) |value| alloc.free(value);
    try std.testing.expect(legacy_lease_after_renew == null);

    const legacy_recovery = try durable.scanRecoveryIds(alloc, .interactive, null, 8);
    defer alloc.free(legacy_recovery.ids);
    try std.testing.expectEqual(@as(usize, 0), legacy_recovery.ids.len);
    const receipt_recovery = try durable.scanRecoveryIds(alloc, .idempotent_receipt, null, 8);
    defer alloc.free(receipt_recovery.ids);
    try std.testing.expectEqual(@as(usize, 1), receipt_recovery.ids.len);
    try std.testing.expectEqualSlices(u8, &txn_id, &receipt_recovery.ids[0]);
    const legacy_expiry = try durable.scanExpiredIds(alloc, .interactive, std.math.maxInt(u64), null, 8);
    defer alloc.free(legacy_expiry.ids);
    try std.testing.expectEqual(@as(usize, 0), legacy_expiry.ids.len);
    const receipt_expiry = try durable.scanExpiredIds(alloc, .idempotent_receipt, std.math.maxInt(u64), null, 8);
    defer alloc.free(receipt_expiry.ids);
    try std.testing.expectEqual(@as(usize, 1), receipt_expiry.ids.len);
    try std.testing.expectEqualSlices(u8, &txn_id, &receipt_expiry.ids[0]);
    try std.testing.expectEqual(@as(usize, 1), try durable.sessionCount());
    try std.testing.expectError(error.SessionLimitExceeded, registry.begin(alloc, .{ .sync_level = .write }, 7));
}

test "lease renewal retries transient failure and excludes terminal receipts" {
    const alloc = std.testing.allocator;
    const path = "/tmp/antfly-session-lease-supervisor";
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), path) catch {};
    const path_z = try alloc.dupeZ(u8, path);
    defer alloc.free(path_z);
    var store = try docstore_mod.DocStore.open(alloc, path_z, .{});
    defer store.close();
    var durable = DurableSessionStore.init(alloc, &store);
    const lease_store = SessionLeaseStore.init(alloc, &store);
    const ttl_ns = 40 * std.time.ns_per_ms;
    var registry = SessionRegistry.initWithLeaseTtl(&durable, lease_store, ttl_ns);
    defer registry.deinit(alloc);

    var request = try parseCommitRequest(alloc,
        \\{"read_set":[],"tables":{"docs":{"inserts":{"doc:a":{"value":1}}}},"sync_level":"write"}
    );
    defer request.deinit(alloc);
    const txn_id = idempotentTransactionId(null, "docs", "heartbeat-retry");
    _ = try registry.beginIdempotentForPrincipal(alloc, txn_id, .{ .sync_level = .write }, 0, null, &request);
    _ = (try registry.markCommitExecutionStarted(alloc, txn_id)).?;
    const before = (try registry.getStatus(alloc, txn_id)).?.lease_expires_at;

    registry.lease_renewal_failures_for_test = 1;
    const now_ns = platform_time.realtimeNs();
    var heartbeat = OwnedSessionLeaseHeartbeat.init(
        &registry,
        io_impl.io(),
        txn_id,
        0,
        5 * std.time.ns_per_ms,
        sessionLeaseExpirationNs(ttl_ns, now_ns),
    );
    var future = try io_impl.io().concurrent(OwnedSessionLeaseHeartbeat.run, .{&heartbeat});
    io_impl.io().sleep(std.Io.Duration.fromMilliseconds(18), .awake) catch {};
    heartbeat.stop();
    future.await(io_impl.io());
    try std.testing.expect(!heartbeat.isLost());
    try std.testing.expect((try registry.getStatus(alloc, txn_id)).?.lease_expires_at > before);
    try std.testing.expectEqual(@as(usize, 1), try registry.renewOwnedLeases(0, platform_time.realtimeNs()));

    _ = (try registry.recordIdempotentDurableAbort(alloc, txn_id, "transaction_conflict", "conflict")).?;
    try std.testing.expectEqual(@as(usize, 0), try registry.renewOwnedLeases(0, platform_time.realtimeNs()));
    try std.testing.expectEqual(@as(usize, 0), registry.lease_renewal_candidates.count());

    _ = try registry.begin(alloc, .{}, 0);
    _ = try registry.begin(alloc, .{}, 0);
    try std.testing.expectEqual(@as(usize, 2), registry.lease_renewal_candidates.count());
    var total_renewed: usize = 0;
    var rounds: usize = 0;
    while (true) {
        const batch = try registry.renewOwnedLeaseBatch(0, platform_time.realtimeNs(), 1);
        try std.testing.expect(batch.renewed <= 1);
        total_renewed += batch.renewed;
        rounds += 1;
        if (!batch.has_more) break;
        try std.testing.expect(rounds <= 3);
    }
    try std.testing.expectEqual(@as(usize, 2), total_renewed);
}

test "lease confirmation rejects windows consumed by storage latency" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/txn-session-lease-runway", .{tmp.sub_path});
    defer alloc.free(path);
    const path_z = try alloc.dupeZ(u8, path);
    defer alloc.free(path_z);
    var store = try docstore_mod.DocStore.open(alloc, path_z, .{});
    defer store.close();
    var durable = DurableSessionStore.init(alloc, &store);
    const leases = SessionLeaseStore.init(alloc, &store);
    const ttl_ns = 2 * std.time.ns_per_ms;
    var registry = SessionRegistry.initWithLeaseTtl(&durable, leases, ttl_ns);
    defer registry.deinit(alloc);
    const session = try registry.begin(alloc, .{ .sync_level = .write }, 0);

    registry.lease_renewal_delay_ns_for_test = 5 * std.time.ns_per_ms;
    registry.lease_renewal_io_for_test = io_impl.io();
    try std.testing.expectError(
        error.SessionLeaseRunwayUnavailable,
        registry.confirmOwnedLeaseRunway(session.txn_id, 0, std.time.ns_per_ms),
    );
}

test "lease cancellation observes deadline while renewal IO is blocked" {
    const alloc = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/txn-session-lease-deadline-cancel", .{tmp.sub_path});
    defer alloc.free(path);
    const path_z = try alloc.dupeZ(u8, path);
    defer alloc.free(path_z);
    var store = try docstore_mod.DocStore.open(alloc, path_z, .{});
    defer store.close();
    var durable = DurableSessionStore.init(alloc, &store);
    const leases = SessionLeaseStore.init(alloc, &store);
    const ttl_ns = 30 * std.time.ns_per_ms;
    var registry = SessionRegistry.initWithLeaseTtl(&durable, leases, ttl_ns);
    defer registry.deinit(alloc);
    const session = try registry.begin(alloc, .{ .sync_level = .write }, 0);
    const confirmed_expiry_ns = try registry.confirmOwnedLeaseRunway(
        session.txn_id,
        0,
        10 * std.time.ns_per_ms,
    );

    registry.lease_renewal_io_for_test = io_impl.io();
    var renewal_entered: std.Io.Event = .unset;
    var renewal_release: std.Io.Event = .unset;
    registry.lease_renewal_entered_for_test = &renewal_entered;
    registry.lease_renewal_release_for_test = &renewal_release;
    var heartbeat = OwnedSessionLeaseHeartbeat.init(
        &registry,
        io_impl.io(),
        session.txn_id,
        0,
        std.time.ns_per_ms,
        confirmed_expiry_ns,
    );
    const lease_cancellation = OwnedSessionLeaseCancellation{ .heartbeat = &heartbeat };
    const token = lease_cancellation.token();
    var future = try io_impl.io().concurrent(OwnedSessionLeaseHeartbeat.run, .{&heartbeat});
    defer {
        renewal_release.set(io_impl.io());
        heartbeat.stop();
        future.await(io_impl.io());
    }

    const entered_deadline_ns = platform_time.monotonicNs() +| 5 * std.time.ns_per_s;
    while (!renewal_entered.isSet()) {
        if (platform_time.monotonicNs() >= entered_deadline_ns) return error.TestUnexpectedResult;
        try io_impl.io().sleep(std.Io.Duration.fromMilliseconds(1), .awake);
    }
    const now_ns = platform_time.realtimeNs();
    if (now_ns < confirmed_expiry_ns) try io_impl.io().sleep(
        std.Io.Duration.fromNanoseconds(confirmed_expiry_ns - now_ns +| std.time.ns_per_ms),
        .awake,
    );
    // The renewal task remains blocked on the explicit std.Io event. The
    // absolute deadline, not completion of that task, cancels fenced work.
    try std.testing.expect(!heartbeat.lost.load(.acquire));
    try std.testing.expect(token.isCancelled());
}

test "session lease storage duration covers nanosecond to millisecond rounding" {
    try std.testing.expectEqual(@as(u64, 2), sessionLeaseStorageTtlMs(1));
    try std.testing.expectEqual(@as(u64, 2), sessionLeaseStorageTtlMs(std.time.ns_per_ms));
    try std.testing.expectEqual(@as(u64, 3), sessionLeaseStorageTtlMs(std.time.ns_per_ms + 1));
    try std.testing.expectEqual(@as(u64, 21), sessionLeaseStorageTtlMs(20 * std.time.ns_per_ms));
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
