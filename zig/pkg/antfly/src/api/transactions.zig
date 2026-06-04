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
const batch_api = @import("batch.zig");
const db_mod = @import("../storage/db/mod.zig");
const distributed_txn = @import("distributed_txn.zig");
const docstore_mod = @import("../storage/docstore.zig");
const lease_mod = @import("../storage/db/lease.zig");
const platform_time = @import("../platform/time.zig");

const session_prefix = "\x00\x00__api_txn_sessions__:";
const session_lease_prefix = "\x00\x00__api_txn_session_leases__:";
var txn_id_nonce: std.atomic.Value(u64) = .init(0);

const AtomicMutex = struct {
    inner: std.atomic.Mutex = .unlocked,

    fn lock(self: *AtomicMutex) void {
        while (!self.inner.tryLock()) std.atomic.spinLoopHint();
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
        return .{
            .table_name = try alloc.dupe(u8, self.table_name),
            .key = try alloc.dupe(u8, self.key),
            .version = self.version,
            .document_json = if (self.document_json) |document_json| try alloc.dupe(u8, document_json) else null,
        };
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
};

pub const Session = struct {
    txn_id: db_mod.types.TxnId,
    owner_node_id: u64,
    begin_timestamp: u64,
    last_touched_timestamp: u64,
    sync_level: db_mod.types.SyncLevel,
    staged: ?OwnedTransactionCommitRequest = null,
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
        if (self.staged) |*staged| staged.deinit(alloc);
        deinitReadSnapshotMap(alloc, &self.read_snapshots);
        var it = self.savepoints.iterator();
        while (it.next()) |entry| entry.value_ptr.deinit(alloc);
        self.savepoints.deinit(alloc);
        self.* = undefined;
    }
};

pub const DurableSessionStore = struct {
    alloc: std.mem.Allocator,
    store: *docstore_mod.DocStore,

    pub fn init(alloc: std.mem.Allocator, store: *docstore_mod.DocStore) DurableSessionStore {
        return .{
            .alloc = alloc,
            .store = store,
        };
    }

    pub fn save(self: *DurableSessionStore, session: Session) !void {
        const key = try makeSessionKey(self.alloc, session.txn_id);
        defer self.alloc.free(key);
        const value = try encodeSessionRecord(self.alloc, session);
        defer self.alloc.free(value);
        try self.store.put(key, value);
    }

    pub fn load(self: *DurableSessionStore, txn_id: db_mod.types.TxnId) !?Session {
        const key = try makeSessionKey(self.alloc, txn_id);
        defer self.alloc.free(key);
        const value = self.store.get(self.alloc, key) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        defer self.alloc.free(value);
        return try decodeSessionRecord(self.alloc, txn_id, value);
    }

    pub fn delete(self: *DurableSessionStore, txn_id: db_mod.types.TxnId) !void {
        const key = try makeSessionKey(self.alloc, txn_id);
        defer self.alloc.free(key);
        self.store.delete(key) catch |err| switch (err) {
            error.NotFound => {},
            else => return err,
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

pub const SessionLeaseStore = struct {
    alloc: std.mem.Allocator,
    store: *docstore_mod.DocStore,

    pub fn init(alloc: std.mem.Allocator, store: *docstore_mod.DocStore) SessionLeaseStore {
        return .{
            .alloc = alloc,
            .store = store,
        };
    }

    pub fn load(self: *const SessionLeaseStore, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId) !?lease_mod.LeaseRecord {
        const key = try makeSessionLeaseKey(self.alloc, txn_id);
        defer self.alloc.free(key);
        var lease = try lease_mod.Lease.init(self.alloc, self.store, key);
        defer lease.deinit();
        return try lease.load(alloc);
    }

    pub fn renew(self: *const SessionLeaseStore, txn_id: db_mod.types.TxnId, owner_node_id: u64, now_ms: u64, ttl_ms: u64) !bool {
        const key = try makeSessionLeaseKey(self.alloc, txn_id);
        defer self.alloc.free(key);
        var lease = try lease_mod.Lease.init(self.alloc, self.store, key);
        defer lease.deinit();
        const owner_id = try ownerLeaseId(self.alloc, owner_node_id);
        defer self.alloc.free(owner_id);
        return try lease.renew(owner_id, now_ms, ttl_ms);
    }

    pub fn release(self: *const SessionLeaseStore, txn_id: db_mod.types.TxnId, owner_node_id: u64) !bool {
        const key = try makeSessionLeaseKey(self.alloc, txn_id);
        defer self.alloc.free(key);
        var lease = try lease_mod.Lease.init(self.alloc, self.store, key);
        defer lease.deinit();
        const owner_id = try ownerLeaseId(self.alloc, owner_node_id);
        defer self.alloc.free(owner_id);
        return try lease.release(owner_id);
    }
};

pub const SessionRegistry = struct {
    mutex: AtomicMutex = .{},
    sessions: std.AutoHashMapUnmanaged(db_mod.types.TxnId, Session) = .empty,
    durable: ?*DurableSessionStore = null,
    lease_store: ?SessionLeaseStore = null,
    owner_lease_ttl_ns: ?u64 = null,
    max_savepoints: ?usize = null,

    pub fn init(durable: ?*DurableSessionStore) SessionRegistry {
        return initWithOptions(durable, null, null, null);
    }

    pub fn initWithLeaseTtl(durable: ?*DurableSessionStore, lease_store: ?SessionLeaseStore, owner_lease_ttl_ns: ?u64) SessionRegistry {
        return initWithOptions(durable, lease_store, owner_lease_ttl_ns, null);
    }

    pub fn initWithOptions(
        durable: ?*DurableSessionStore,
        lease_store: ?SessionLeaseStore,
        owner_lease_ttl_ns: ?u64,
        max_savepoints: ?usize,
    ) SessionRegistry {
        return .{
            .durable = durable,
            .lease_store = lease_store,
            .owner_lease_ttl_ns = owner_lease_ttl_ns,
            .max_savepoints = max_savepoints,
        };
    }

    pub fn deinit(self: *SessionRegistry, alloc: std.mem.Allocator) void {
        var it = self.sessions.iterator();
        while (it.next()) |entry| entry.value_ptr.deinit(alloc);
        self.sessions.deinit(alloc);
        self.* = .{};
    }

    pub fn begin(self: *SessionRegistry, alloc: std.mem.Allocator, req: BeginRequest, owner_node_id: u64) !SessionInfo {
        const txn_id = newSessionTxnId(owner_node_id);
        const now = nextTxnTimestamp();
        const session: Session = .{
            .txn_id = txn_id,
            .owner_node_id = owner_node_id,
            .begin_timestamp = now,
            .last_touched_timestamp = now,
            .sync_level = req.sync_level,
        };
        self.mutex.lock();
        defer self.mutex.unlock();
        try self.renewLeaseLocked(txn_id, owner_node_id);
        try self.sessions.put(alloc, txn_id, session);
        try self.persistLocked(session);
        return session.info();
    }

    pub fn getInfo(self: *SessionRegistry, txn_id: db_mod.types.TxnId) ?SessionInfo {
        self.mutex.lock();
        defer self.mutex.unlock();
        const session = if (self.sessions.getPtr(txn_id)) |existing| blk: {
            break :blk existing.*;
        } else if (self.durable) |durable| blk: {
            const loaded = (self.loadIntoCacheLocked(durable.alloc, txn_id) catch return null) orelse return null;
            break :blk loaded.*;
        } else return null;
        return session.info();
    }

    pub fn stage(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId, req: *const OwnedTransactionCommitRequest) !?SessionInfo {
        self.mutex.lock();
        defer self.mutex.unlock();
        const session = (try self.loadIntoCacheLocked(alloc, txn_id)) orelse return null;
        if (session.staged == null) {
            session.staged = try req.clone(alloc);
        } else {
            try session.staged.?.mergeFrom(alloc, req);
        }
        touchSession(session);
        try self.renewLeaseLocked(txn_id, session.owner_node_id);
        try self.persistLocked(session.*);
        return session.info();
    }

    pub fn getReadSnapshot(
        self: *SessionRegistry,
        alloc: std.mem.Allocator,
        txn_id: db_mod.types.TxnId,
        table_name: []const u8,
        key: []const u8,
    ) !?SessionReadSnapshot {
        self.mutex.lock();
        defer self.mutex.unlock();
        const session = (try self.loadIntoCacheLocked(alloc, txn_id)) orelse return null;
        return try cloneReadSnapshotForKey(alloc, &session.read_snapshots, table_name, key);
    }

    pub fn stageRead(
        self: *SessionRegistry,
        alloc: std.mem.Allocator,
        txn_id: db_mod.types.TxnId,
        req: *const OwnedTransactionCommitRequest,
        snapshot: StageReadSnapshot,
    ) !?SessionInfo {
        self.mutex.lock();
        defer self.mutex.unlock();
        const session = (try self.loadIntoCacheLocked(alloc, txn_id)) orelse return null;
        try upsertReadSnapshot(alloc, &session.read_snapshots, snapshot);
        if (session.staged == null) {
            session.staged = try req.clone(alloc);
        } else {
            try session.staged.?.mergeFrom(alloc, req);
        }
        touchSession(session);
        try self.renewLeaseLocked(txn_id, session.owner_node_id);
        try self.persistLocked(session.*);
        return session.info();
    }

    pub fn cloneCommitRequest(
        self: *SessionRegistry,
        alloc: std.mem.Allocator,
        txn_id: db_mod.types.TxnId,
        extra_req: ?*const OwnedTransactionCommitRequest,
    ) !?OwnedTransactionCommitRequest {
        self.mutex.lock();
        defer self.mutex.unlock();
        const session = (try self.loadIntoCacheLocked(alloc, txn_id)) orelse return null;
        var out: OwnedTransactionCommitRequest = if (session.staged) |staged|
            try staged.clone(alloc)
        else
            .{ .sync_level = session.sync_level };
        errdefer out.deinit(alloc);
        if (extra_req) |req| {
            try out.mergeFrom(alloc, req);
        }
        if (out.tables.len == 0) {
            out.deinit(alloc);
            return null;
        }
        touchSession(session);
        try self.renewLeaseLocked(txn_id, session.owner_node_id);
        try self.persistLocked(session.*);
        return out;
    }

    pub fn createSavepoint(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId) !?SavepointInfo {
        self.mutex.lock();
        defer self.mutex.unlock();
        const session = (try self.loadIntoCacheLocked(alloc, txn_id)) orelse return null;
        if (self.max_savepoints) |limit| {
            if (session.savepoints.count() >= limit) return error.SavepointLimitExceeded;
        }
        const savepoint_id = session.next_savepoint_id;
        session.next_savepoint_id += 1;
        const snapshot: OwnedTransactionCommitRequest = if (session.staged) |staged|
            try staged.clone(alloc)
        else
            .{ .sync_level = session.sync_level };
        try session.savepoints.put(alloc, savepoint_id, .{
            .id = savepoint_id,
            .snapshot = snapshot,
            .read_snapshots = try cloneReadSnapshotMap(alloc, session.read_snapshots),
        });
        touchSession(session);
        try self.renewLeaseLocked(txn_id, session.owner_node_id);
        try self.persistLocked(session.*);
        return .{ .txn_id = txn_id, .savepoint_id = savepoint_id };
    }

    pub fn rollbackToSavepoint(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId, savepoint_id: u64) !?SavepointInfo {
        self.mutex.lock();
        defer self.mutex.unlock();
        const session = (try self.loadIntoCacheLocked(alloc, txn_id)) orelse return null;
        const savepoint = session.savepoints.getPtr(savepoint_id) orelse return null;
        if (session.staged) |*staged| staged.deinit(alloc);
        session.staged = try savepoint.snapshot.clone(alloc);
        deinitReadSnapshotMap(alloc, &session.read_snapshots);
        session.read_snapshots = try cloneReadSnapshotMap(alloc, savepoint.read_snapshots);
        touchSession(session);
        try self.renewLeaseLocked(txn_id, session.owner_node_id);
        try self.persistLocked(session.*);
        return .{ .txn_id = txn_id, .savepoint_id = savepoint_id };
    }

    pub fn getStatus(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId) !?SessionStatus {
        self.mutex.lock();
        defer self.mutex.unlock();
        const session = (try self.loadIntoCacheLocked(alloc, txn_id)) orelse return null;
        return try sessionStatusFromSession(self, alloc, session);
    }

    pub fn getDetails(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId) !?SessionDetails {
        self.mutex.lock();
        defer self.mutex.unlock();
        const session = (try self.loadIntoCacheLocked(alloc, txn_id)) orelse return null;
        return .{
            .status = try sessionStatusFromSession(self, alloc, session),
            .tables = try sessionTableDetails(alloc, session.staged),
            .read_snapshots = try sessionReadSnapshots(alloc, session),
            .savepoint_ids = try sessionSavepointIds(alloc, session),
        };
    }

    pub fn listStatuses(self: *SessionRegistry, alloc: std.mem.Allocator) ![]SessionStatus {
        self.mutex.lock();
        defer self.mutex.unlock();

        var statuses = std.ArrayListUnmanaged(SessionStatus).empty;
        errdefer statuses.deinit(alloc);

        if (self.durable) |durable| {
            const rows = try durable.store.scanPrefix(alloc, session_prefix);
            defer docstore_mod.DocStore.freeResults(alloc, rows);
            for (rows) |row| {
                if (row.key.len <= session_prefix.len) continue;
                const txn_id = distributed_txn.parseTxnIdHex(row.key[session_prefix.len..]) catch continue;
                var session = decodeSessionRecord(alloc, txn_id, row.value) catch continue;
                defer session.deinit(alloc);
                try statuses.append(alloc, try sessionStatusFromSession(self, alloc, &session));
            }
            return try statuses.toOwnedSlice(alloc);
        }

        var it = self.sessions.iterator();
        while (it.next()) |entry| {
            const session = entry.value_ptr.*;
            try statuses.append(alloc, try sessionStatusFromSession(self, alloc, &session));
        }
        return try statuses.toOwnedSlice(alloc);
    }

    pub fn getOwnerNodeId(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId) !?u64 {
        self.mutex.lock();
        defer self.mutex.unlock();
        const session = (try self.loadIntoCacheLocked(alloc, txn_id)) orelse return null;
        return session.owner_node_id;
    }

    pub fn adopt(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId, owner_node_id: u64) !bool {
        self.mutex.lock();
        defer self.mutex.unlock();
        if (self.durable == null) return false;
        const session = (try self.loadIntoCacheLocked(alloc, txn_id)) orelse return false;
        if (session.owner_node_id == owner_node_id) return true;
        session.owner_node_id = owner_node_id;
        touchSession(session);
        if (self.lease_store != null and self.owner_lease_ttl_ns != null) {
            const now_ns = nextTxnTimestamp();
            try self.forceRenewLeaseLockedAt(txn_id, owner_node_id, now_ns);
        }
        try self.persistLocked(session.*);
        return true;
    }

    pub fn adoptIfLeaseExpired(
        self: *SessionRegistry,
        alloc: std.mem.Allocator,
        txn_id: db_mod.types.TxnId,
        owner_node_id: u64,
        now_ns: ?u64,
    ) !bool {
        self.mutex.lock();
        defer self.mutex.unlock();
        if (self.durable == null) return false;
        if (self.lease_store == null or self.owner_lease_ttl_ns == null) return false;
        const session = (try self.loadIntoCacheLocked(alloc, txn_id)) orelse return false;
        if (session.owner_node_id == owner_node_id) return true;
        const effective_now = now_ns orelse nextTxnTimestamp();
        const lease_expires_at = try self.loadLeaseExpiryLocked(alloc, txn_id);
        if (lease_expires_at != 0 and effective_now < lease_expires_at) return false;
        self.renewLeaseLockedAt(txn_id, owner_node_id, effective_now) catch |err| switch (err) {
            error.SessionLeaseLost => return false,
            else => return err,
        };
        session.owner_node_id = owner_node_id;
        touchSession(session);
        try self.persistLocked(session.*);
        return true;
    }

    fn forceRenewLeaseLockedAt(self: *SessionRegistry, txn_id: db_mod.types.TxnId, owner_node_id: u64, now_ns: u64) !void {
        const lease_store = self.lease_store orelse return;
        const ttl_ns = self.owner_lease_ttl_ns orelse return;
        const ttl_ms = @max(@as(u64, 1), ttl_ns / std.time.ns_per_ms);
        const now_ms = now_ns / std.time.ns_per_ms;
        const renewed = try lease_store.renew(txn_id, owner_node_id, now_ms, ttl_ms);
        if (!renewed) {
            if (try lease_store.load(self.durable.?.alloc, txn_id)) |loaded_lease_record| {
                var lease_record = loaded_lease_record;
                defer lease_mod.deinitRecord(self.durable.?.alloc, &lease_record);
                const previous_owner = leaseRecordOwnerNodeId(lease_record.owner_id);
                if (previous_owner != null) {
                    _ = try lease_store.release(txn_id, previous_owner.?);
                    _ = try lease_store.renew(txn_id, owner_node_id, now_ms, ttl_ms);
                }
            }
        }
    }

    pub fn cleanupExpired(self: *SessionRegistry, alloc: std.mem.Allocator, cutoff_ns: u64) !usize {
        self.mutex.lock();
        defer self.mutex.unlock();

        var expired_ids = std.ArrayListUnmanaged(db_mod.types.TxnId).empty;
        defer expired_ids.deinit(alloc);

        var loaded_it = self.sessions.iterator();
        while (loaded_it.next()) |entry| {
            if (entry.value_ptr.last_touched_timestamp < cutoff_ns) {
                try expired_ids.append(alloc, entry.key_ptr.*);
            }
        }

        if (self.durable) |durable| {
            const rows = try durable.store.scanPrefix(alloc, session_prefix);
            defer docstore_mod.DocStore.freeResults(alloc, rows);
            for (rows) |row| {
                if (row.key.len <= session_prefix.len) continue;
                const txn_id = distributed_txn.parseTxnIdHex(row.key[session_prefix.len..]) catch continue;
                if (containsTxnId(expired_ids.items, txn_id)) continue;
                var session = decodeSessionRecord(alloc, txn_id, row.value) catch continue;
                defer session.deinit(alloc);
                if (session.last_touched_timestamp < cutoff_ns) {
                    try expired_ids.append(alloc, txn_id);
                }
            }
        }

        for (expired_ids.items) |txn_id| {
            if (self.sessions.fetchRemove(txn_id)) |removed| {
                var session = removed.value;
                self.releaseLeaseLocked(txn_id, session.owner_node_id) catch {};
                session.deinit(alloc);
            }
            try self.deletePersistentLocked(txn_id);
        }
        return expired_ids.items.len;
    }

    pub fn remove(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId) bool {
        self.mutex.lock();
        defer self.mutex.unlock();
        _ = self.loadIntoCacheLocked(alloc, txn_id) catch return false;
        const removed = self.sessions.fetchRemove(txn_id) orelse return false;
        var session = removed.value;
        self.releaseLeaseLocked(txn_id, session.owner_node_id) catch return false;
        session.deinit(alloc);
        self.deletePersistentLocked(txn_id) catch return false;
        return true;
    }

    fn persistLocked(self: *SessionRegistry, session: Session) !void {
        if (self.durable) |durable| try durable.save(session);
    }

    fn deletePersistentLocked(self: *SessionRegistry, txn_id: db_mod.types.TxnId) !void {
        if (self.durable) |durable| try durable.delete(txn_id);
    }

    fn loadIntoCacheLocked(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId) !?*Session {
        if (self.sessions.getPtr(txn_id)) |session| return session;
        const durable = self.durable orelse return null;
        const loaded = (try durable.load(txn_id)) orelse return null;
        try self.sessions.put(alloc, txn_id, loaded);
        return self.sessions.getPtr(txn_id).?;
    }

    fn renewLeaseLocked(self: *SessionRegistry, txn_id: db_mod.types.TxnId, owner_node_id: u64) !void {
        const now_ns = nextTxnTimestamp();
        try self.renewLeaseLockedAt(txn_id, owner_node_id, now_ns);
    }

    pub fn renewOwnedLeases(self: *SessionRegistry, owner_node_id: u64, now_ns: u64) !usize {
        self.mutex.lock();
        defer self.mutex.unlock();
        if (self.lease_store == null or self.owner_lease_ttl_ns == null) return 0;
        var renewed: usize = 0;
        var it = self.sessions.iterator();
        while (it.next()) |entry| {
            if (entry.value_ptr.owner_node_id != owner_node_id) continue;
            try self.renewLeaseLockedAt(entry.key_ptr.*, owner_node_id, now_ns);
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

    fn releaseLeaseLocked(self: *SessionRegistry, txn_id: db_mod.types.TxnId, owner_node_id: u64) !void {
        const lease_store = self.lease_store orelse return;
        _ = try lease_store.release(txn_id, owner_node_id);
    }

    fn loadLeaseExpiryLocked(self: *SessionRegistry, alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId) !u64 {
        const lease_store = self.lease_store orelse return 0;
        var record = (try lease_store.load(alloc, txn_id)) orelse return 0;
        defer lease_mod.deinitRecord(alloc, &record);
        return record.expires_at_ms * std.time.ns_per_ms;
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
    errdefer alloc.free(map_key);
    const gop = try map.getOrPut(alloc, map_key);
    if (gop.found_existing) {
        alloc.free(map_key);
        if (gop.value_ptr.version == snapshot.version) return;
        gop.value_ptr.deinit(alloc);
    }
    gop.key_ptr.* = map_key;
    gop.value_ptr.* = .{
        .table_name = try alloc.dupe(u8, snapshot.table_name),
        .key = try alloc.dupe(u8, snapshot.key),
        .version = snapshot.version,
        .document_json = if (snapshot.document_json) |document_json| try alloc.dupe(u8, document_json) else null,
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
            .integer => |v| @as(u64, @intCast(v)),
            .string => |s| try parseVersionString(s),
            else => return error.InvalidTransactionSessionRecord,
        };
        const document_json = if (obj.get("document")) |document| switch (document) {
            .null => null,
            else => try std.json.Stringify.valueAlloc(alloc, document, .{}),
        } else null;
        errdefer if (document_json) |json| alloc.free(json);
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

fn containsTxnId(ids: []const db_mod.types.TxnId, txn_id: db_mod.types.TxnId) bool {
    for (ids) |existing| {
        if (std.mem.eql(u8, &existing, &txn_id)) return true;
    }
    return false;
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
                .integer => |v| @intCast(v),
                else => return error.InvalidTransactionSessionRecord,
            }
        else
            sessionOwnerNodeId(txn_id),
        .begin_timestamp = switch (obj.get("begin_timestamp") orelse return error.InvalidTransactionSessionRecord) {
            .integer => |v| @intCast(v),
            else => return error.InvalidTransactionSessionRecord,
        },
        .last_touched_timestamp = 0,
        .sync_level = parseSyncLevel(obj.get("sync_level") orelse return error.InvalidTransactionSessionRecord) orelse return error.InvalidTransactionSessionRecord,
        .next_savepoint_id = switch (obj.get("next_savepoint_id") orelse return error.InvalidTransactionSessionRecord) {
            .integer => |v| @intCast(v),
            else => return error.InvalidTransactionSessionRecord,
        },
    };
    errdefer session.deinit(alloc);
    session.last_touched_timestamp = if (obj.get("last_touched_timestamp")) |value|
        switch (value) {
            .integer => |v| @intCast(v),
            else => return error.InvalidTransactionSessionRecord,
        }
    else
        session.begin_timestamp;
    if (obj.get("staged")) |staged_value| {
        if (staged_value != .null) session.staged = try parseCommitValue(alloc, staged_value);
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
            .integer => |v| @intCast(v),
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
        \\  "tables":{"docs":{"transforms":[{"key":"doc:a","operations":[{"op":"$set","path":"status","value":"updated"},{"op":"$max","path":"version","value":3}],"upsert":true}]}}
        \\}
    );
    defer req.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), req.tables.len);
    try std.testing.expectEqual(@as(usize, 1), req.tables[0].batch.transforms.len);
    try std.testing.expect(req.tables[0].batch.transforms[0].upsert);
    try std.testing.expectEqual(db_mod.types.TransformOpType.max, req.tables[0].batch.transforms[0].operations[1].op);
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
    try durable.save(registry.sessions.get(session.txn_id).?);
    const removed = try registry.cleanupExpired(std.testing.allocator, 2);
    try std.testing.expectEqual(@as(usize, 1), removed);
    try std.testing.expect(registry.getInfo(session.txn_id) == null);
    try std.testing.expect((try durable.load(session.txn_id)) == null);
}

test "transaction session registry enforces savepoint limits and reports remaining capacity" {
    var registry = SessionRegistry.initWithOptions(null, null, null, 1);
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
